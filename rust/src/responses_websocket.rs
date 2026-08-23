use std::{
    collections::HashMap,
    env, io,
    pin::Pin,
    sync::{Arc, LazyLock, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::Body,
    extract::ws::{Message, WebSocket},
    http::HeaderMap,
};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use futures_util::{SinkExt, StreamExt};
use percent_encoding::percent_decode_str;
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, ServerName},
};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf},
    net::TcpStream,
    sync::Semaphore,
};
use tokio_rustls::{TlsConnector, client::TlsStream};
use tokio_tungstenite::{
    client_async_tls,
    tungstenite::{
        Message as UpstreamMessage, client::IntoClientRequest, http::HeaderValue as WsHeaderValue,
    },
};
use url::Url;

use super::{
    ApiError, AppState, MAX_REQUEST_BODY_BYTES, MAX_UPSTREAM_BODY_BYTES, NATIVE_UPSTREAM_TIMEOUT,
    acquire_native_codex_lease, codex_sse_data, native_codex_responses_payload,
    project_codex_response_event, responses_with_timeout, sse_delimiter,
    validate_codex_response_event,
};

const MAX_CONNECTIONS: usize = 64;
const MAX_LIFETIME: Duration = Duration::from_secs(60 * 60);
const MAX_TRANSCRIPT_BYTES: usize = 16 * 1024 * 1024;
const MAX_TOTAL_TRANSCRIPT_BYTES: usize = 128 * 1024 * 1024;
const CODEX_RESPONSES_WEBSOCKET_BETA: &str = "responses_websockets=2026-02-06";
const CODEX_RESPONSES_WEBSOCKET_USER_AGENT: &str =
    "codex-tui/0.146.0 (Mac OS 26.5.0; arm64) iTerm.app/3.6.10 (codex-tui; 0.146.0)";
const CODEX_RESPONSES_WEBSOCKET_PATH: &str = "/backend-api/codex/responses";
const CONNECT_RETRY_DELAYS: [Duration; 2] =
    [Duration::from_millis(200), Duration::from_millis(400)];
const IDLE_CONNECTION_POLL_TIMEOUT: Duration = Duration::from_millis(10);
const MAX_IDLE_CONTROL_FRAMES: usize = 32;
const MAX_PROXY_AUTH_HEADER_BYTES: usize = 16 * 1024;
const MAX_SOCKS5_AUTH_FIELD_BYTES: usize = 255;
const REUSE_PROPERTY_FIELDS: &[&str] = &[
    "model",
    "instructions",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "reasoning",
    "store",
    "stream",
    "include",
    "service_tier",
    "prompt_cache_key",
    "text",
    "context_management",
];

static CONNECTIONS: LazyLock<Semaphore> = LazyLock::new(|| Semaphore::new(MAX_CONNECTIONS));
static TRANSCRIPT_BUDGET: LazyLock<Arc<TranscriptBudget>> =
    LazyLock::new(|| Arc::new(TranscriptBudget::new(MAX_TOTAL_TRANSCRIPT_BYTES)));

pub(super) async fn run(mut socket: WebSocket, state: AppState, headers: HeaderMap) {
    let permit = match CONNECTIONS.try_acquire() {
        Ok(permit) => permit,
        Err(_) => {
            let _ = send_error(
                &mut socket,
                "server_error",
                "websocket_connection_capacity_reached",
                "Responses websocket connection capacity reached; try again later.",
                Some(429),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
    };
    let _permit = permit;
    let mut session = Session::default();
    let mut transport = NativeCodexWebSocketTransport::default();
    let deadline = tokio::time::Instant::now() + MAX_LIFETIME;

    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            let _ = send_error(
                &mut socket,
                "invalid_request_error",
                "websocket_connection_limit_reached",
                "Responses websocket connection limit reached (60 minutes). Create a new websocket connection to continue.",
                Some(400),
            )
            .await;
            let _ = socket.close().await;
            return;
        }
        let inbound = match tokio::time::timeout(remaining, socket.recv()).await {
            Ok(Some(Ok(Message::Text(text)))) => text.to_string(),
            Ok(Some(Ok(Message::Binary(_)))) => {
                let _ = send_error(
                    &mut socket,
                    "invalid_request_error",
                    "invalid_json",
                    "Request body is not valid JSON.",
                    None,
                )
                .await;
                continue;
            }
            Ok(Some(Ok(Message::Ping(payload)))) => {
                let _ = socket.send(Message::Pong(payload)).await;
                continue;
            }
            Ok(Some(Ok(Message::Pong(_)))) => continue,
            Ok(Some(Ok(Message::Close(_)))) | Ok(None) => return,
            Ok(Some(Err(_))) | Err(_) => return,
        };

        let value = match serde_json::from_str::<Value>(&inbound) {
            Ok(value) => value,
            Err(_) => {
                let _ = send_error(
                    &mut socket,
                    "invalid_request_error",
                    "invalid_json",
                    "Request body is not valid JSON.",
                    None,
                )
                .await;
                continue;
            }
        };
        let turn = match session.prepare(value) {
            Ok(turn) => turn,
            Err(error) => {
                let _ = send_api_error(&mut socket, error).await;
                continue;
            }
        };
        session.last_request = Some(turn.replay_body.clone());
        session.pending_request_properties = Some(turn.request_properties.clone());

        let turn_deadline = tokio::time::Instant::now() + NATIVE_UPSTREAM_TIMEOUT;
        let result = transport
            .forward(&state, &turn, &mut socket, turn_deadline)
            .await;
        match result {
            Ok(ForwardResult::Completed(response)) => {
                session.commit(response);
            }
            Ok(ForwardResult::Failed) => session.fail(&turn),
            Err(TransportError::HandshakeUnavailable) if !turn.warmup => {
                transport.close();
                let bytes = match serde_json::to_vec(&turn.replay_body) {
                    Ok(bytes) if bytes.len() <= MAX_REQUEST_BODY_BYTES => bytes,
                    _ => {
                        let _ = send_error(
                            &mut socket,
                            "invalid_request_error",
                            "websocket_session_too_large",
                            "websocket session state is too large",
                            None,
                        )
                        .await;
                        session.fail(&turn);
                        continue;
                    }
                };
                let fallback = responses_with_timeout(
                    axum::extract::State(state.clone()),
                    headers.clone(),
                    Body::from(bytes),
                    NATIVE_UPSTREAM_TIMEOUT,
                )
                .await;
                match fallback {
                    Ok(response) => match forward_http_response(&mut socket, response).await {
                        Ok(ForwardResult::Completed(response)) => {
                            session.commit(response);
                        }
                        Ok(ForwardResult::Failed) => {
                            session.fail(&turn);
                        }
                        Err(()) => {
                            session.fail(&turn);
                            let _ = send_error(
                                &mut socket,
                                "server_error",
                                "upstream_error",
                                "The upstream request failed. Please try again later.",
                                None,
                            )
                            .await;
                        }
                    },
                    Err(error) => {
                        session.fail(&turn);
                        let _ = send_api_error(&mut socket, error).await;
                    }
                }
            }
            Err(error) => {
                if matches!(error, TransportError::DownstreamClosed) {
                    return;
                }
                session.fail(&turn);
                let _ = send_error(
                    &mut socket,
                    "server_error",
                    "upstream_error",
                    transport_error_message(error),
                    None,
                )
                .await;
            }
        }
    }
}

fn transport_error_message(_error: TransportError) -> &'static str {
    "The upstream request failed. Please try again later."
}

struct Session {
    last_response_id: Option<String>,
    transcript: Vec<Value>,
    unavailable: Option<(String, &'static str, &'static str)>,
    last_request: Option<Map<String, Value>>,
    pending_request_properties: Option<Value>,
    last_request_properties: Option<Value>,
    budget: Arc<TranscriptBudget>,
    reservation: usize,
}

impl Default for Session {
    fn default() -> Self {
        Self::with_budget(TRANSCRIPT_BUDGET.clone())
    }
}

impl Session {
    fn with_budget(budget: Arc<TranscriptBudget>) -> Self {
        Self {
            last_response_id: None,
            transcript: Vec::new(),
            unavailable: None,
            last_request: None,
            pending_request_properties: None,
            last_request_properties: None,
            budget,
            reservation: 0,
        }
    }

    fn prepare(&self, value: Value) -> Result<PreparedTurn, ApiError> {
        let Value::Object(mut object) = value else {
            return Err(unsupported_event());
        };
        if object.remove("type").as_ref().and_then(Value::as_str) != Some("response.create") {
            return Err(unsupported_event());
        }
        if object
            .get("background")
            .is_some_and(|value| !value.is_null())
        {
            return Err(ApiError::websocket_custom(
                "invalid_request_error",
                "invalid_request_error",
                "background is not supported in Responses WebSocket mode",
            ));
        }
        let warmup = match object.get("generate") {
            None => false,
            Some(Value::Bool(false)) => true,
            Some(_) => {
                return Err(ApiError::websocket_custom(
                    "invalid_request_error",
                    "invalid_request_error",
                    "generate must be false in Responses WebSocket mode",
                ));
            }
        };
        object.remove("background");
        object.remove("stream");
        let previous_response_id = match object.remove("previous_response_id") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => {
                let value = value.trim().to_owned();
                (!value.is_empty()).then_some(value)
            }
            Some(_) => {
                return Err(ApiError::websocket_custom(
                    "invalid_request_error",
                    "invalid_request_error",
                    "previous_response_id must be a string",
                ));
            }
        };
        if let Some(previous) = previous_response_id.as_deref() {
            if let Some((id, code, message)) = &self.unavailable
                && id == previous
            {
                return Err(ApiError::websocket_custom(
                    "invalid_request_error",
                    code,
                    message,
                ));
            }
            if self.last_response_id.as_deref() != Some(previous) {
                return Err(ApiError::websocket_custom(
                    "invalid_request_error",
                    "previous_response_not_found",
                    "previous response is not available on this websocket connection",
                ));
            }
        }

        let mut incremental_body = object.clone();
        if let Some(previous) = previous_response_id.as_deref() {
            incremental_body.insert(
                "previous_response_id".to_owned(),
                Value::String(previous.to_owned()),
            );
        }
        incremental_body.insert("stream".to_owned(), Value::Bool(true));

        let mut replay_body = object;
        if previous_response_id.is_some() {
            let current = input_items(replay_body.remove("input"))?;
            let mut input = self.transcript.clone();
            input.extend(current);
            replay_body.insert("input".to_owned(), Value::Array(input));
        }
        replay_body.insert("stream".to_owned(), Value::Bool(true));

        let request_properties = reuse_properties(&incremental_body)?;
        if previous_response_id.is_some()
            && self.last_request_properties.as_ref() != Some(&request_properties)
        {
            incremental_body = replay_body.clone();
        }
        self.check_size(&incremental_body)?;
        self.check_size(&replay_body)?;
        Ok(PreparedTurn {
            incremental_body,
            replay_body,
            previous_response_id,
            request_properties,
            warmup,
        })
    }

    fn commit(&mut self, response: Value) -> bool {
        let request = self.last_request.take();
        let request_properties = self.pending_request_properties.take();
        let Some(id) = response.get("id").and_then(Value::as_str) else {
            return false;
        };
        let Some(input) = request
            .as_ref()
            .and_then(|request| request.get("input"))
            .cloned()
        else {
            return false;
        };
        let mut transcript = match input_items(Some(input)) {
            Ok(items) => items,
            Err(_) => return false,
        };
        if let Some(output) = response.get("output").and_then(Value::as_array) {
            transcript.extend(output.iter().filter(|item| item.is_object()).cloned());
        }
        let next_size = serde_json::to_vec(&transcript)
            .map(|bytes| bytes.len())
            .unwrap_or(usize::MAX);
        if next_size > MAX_TRANSCRIPT_BYTES {
            self.invalidate(
                id.to_owned(),
                "websocket_session_too_large",
                "websocket session state is too large",
            );
            return false;
        }
        if !self.budget.replace(self.reservation, next_size) {
            self.invalidate(
                id.to_owned(),
                "websocket_server_capacity_reached",
                "websocket session capacity reached; start a new response without previous_response_id",
            );
            return false;
        }
        self.reservation = next_size;
        self.transcript = transcript;
        self.last_response_id = Some(id.to_owned());
        self.last_request_properties = request_properties;
        self.unavailable = None;
        true
    }

    fn fail(&mut self, turn: &PreparedTurn) {
        self.last_request = None;
        self.pending_request_properties = None;
        if turn.previous_response_id.as_deref() == self.last_response_id.as_deref() {
            self.clear_transcript();
        }
    }

    fn invalidate(&mut self, id: String, code: &'static str, message: &'static str) {
        self.clear_transcript();
        self.last_request = None;
        self.pending_request_properties = None;
        self.unavailable = Some((id, code, message));
    }

    fn clear_transcript(&mut self) {
        self.budget.release(self.reservation);
        self.reservation = 0;
        self.transcript.clear();
        self.last_response_id = None;
        self.last_request_properties = None;
    }

    fn check_size(&self, value: &Map<String, Value>) -> Result<(), ApiError> {
        if serde_json::to_vec(value)
            .map(|bytes| bytes.len() > MAX_TRANSCRIPT_BYTES)
            .unwrap_or(true)
        {
            return Err(ApiError::websocket_custom(
                "invalid_request_error",
                "websocket_session_too_large",
                "websocket session state is too large",
            ));
        }
        Ok(())
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.budget.release(self.reservation);
        self.reservation = 0;
    }
}

fn unsupported_event() -> ApiError {
    ApiError::websocket_custom(
        "invalid_request_error",
        "unsupported_event",
        "unsupported websocket event",
    )
}

fn input_items(value: Option<Value>) -> Result<Vec<Value>, ApiError> {
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) if items.iter().all(Value::is_object) => Ok(items),
        Some(Value::Object(item)) => Ok(vec![Value::Object(item)]),
        Some(Value::String(text)) => Ok(vec![json!({
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": text}],
        })]),
        Some(_) => Err(ApiError::validation()),
    }
}

fn request_without_transport_controls(body: &Map<String, Value>) -> Map<String, Value> {
    let mut body = body.clone();
    body.remove("previous_response_id");
    body.remove("generate");
    body
}

fn normalized_payload(body: &Map<String, Value>) -> Result<Value, ApiError> {
    let mut body = request_without_transport_controls(body);
    body.insert("stream".to_owned(), Value::Bool(true));
    native_codex_responses_payload(&body)
}

fn wire_payload(body: &Map<String, Value>) -> Result<Value, TransportError> {
    let previous_response_id = body
        .get("previous_response_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let mut payload = normalized_payload(body).map_err(|_| TransportError::Protocol)?;
    let object = payload.as_object_mut().ok_or(TransportError::Protocol)?;
    if body.get("generate") == Some(&Value::Bool(false)) {
        object.insert("generate".to_owned(), Value::Bool(false));
    }
    object.insert(
        "type".to_owned(),
        Value::String("response.create".to_owned()),
    );
    if let Some(previous_response_id) = previous_response_id {
        object.insert(
            "previous_response_id".to_owned(),
            Value::String(previous_response_id),
        );
    }
    Ok(payload)
}

fn reuse_properties(body: &Map<String, Value>) -> Result<Value, ApiError> {
    let payload = normalized_payload(body)?;
    let object = payload.as_object().ok_or_else(ApiError::invalid_request)?;
    let mut properties = Map::new();
    for field in REUSE_PROPERTY_FIELDS {
        properties.insert(
            (*field).to_owned(),
            object.get(*field).cloned().unwrap_or(Value::Null),
        );
    }
    Ok(Value::Object(properties))
}

#[derive(Debug)]
struct PreparedTurn {
    incremental_body: Map<String, Value>,
    replay_body: Map<String, Value>,
    previous_response_id: Option<String>,
    request_properties: Value,
    warmup: bool,
}

#[derive(Debug)]
enum TransportError {
    HandshakeUnavailable,
    Protocol,
    Upstream,
    Timeout,
    DownstreamClosed,
}

enum ForwardInput {
    Upstream(Result<Option<UpstreamMessage>, ()>),
    Downstream(Option<Message>),
}

enum ForwardResult {
    Completed(Value),
    Failed,
}

struct CredentialKey {
    token: String,
    account_id: String,
    proxy_url: String,
}

impl PartialEq for CredentialKey {
    fn eq(&self, other: &Self) -> bool {
        self.token == other.token
            && self.account_id == other.account_id
            && self.proxy_url == other.proxy_url
    }
}

type UpstreamWebSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<ProxyStream>>;

enum ProxyStream {
    Tcp(TcpStream),
    HttpsProxy(Box<TlsStream<TcpStream>>),
}

impl AsyncRead for ProxyStream {
    fn poll_read(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_read(context, buffer),
            Self::HttpsProxy(stream) => Pin::new(stream.as_mut()).poll_read(context, buffer),
        }
    }
}

impl AsyncWrite for ProxyStream {
    fn poll_write(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_write(context, buffer),
            Self::HttpsProxy(stream) => Pin::new(stream.as_mut()).poll_write(context, buffer),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_flush(context),
            Self::HttpsProxy(stream) => Pin::new(stream.as_mut()).poll_flush(context),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(stream) => Pin::new(stream).poll_shutdown(context),
            Self::HttpsProxy(stream) => Pin::new(stream.as_mut()).poll_shutdown(context),
        }
    }
}

#[derive(Default)]
struct NativeCodexWebSocketTransport {
    connection: Option<UpstreamWebSocket>,
    credential_key: Option<CredentialKey>,
    lease: Option<super::AccountLease>,
    disabled: bool,
}

impl NativeCodexWebSocketTransport {
    async fn forward(
        &mut self,
        state: &AppState,
        turn: &PreparedTurn,
        downstream: &mut WebSocket,
        deadline: tokio::time::Instant,
    ) -> Result<ForwardResult, TransportError> {
        let result = self.forward_inner(state, turn, downstream, deadline).await;
        self.finish_forward(result)
    }

    async fn forward_inner(
        &mut self,
        state: &AppState,
        turn: &PreparedTurn,
        downstream: &mut WebSocket,
        deadline: tokio::time::Instant,
    ) -> Result<ForwardResult, TransportError> {
        let reused = self.ensure_connection(state, turn).await?;
        let body = if reused {
            &turn.incremental_body
        } else {
            &turn.replay_body
        };
        let payload = wire_payload(body)?;
        let serialized = serde_json::to_string(&payload).map_err(|_| TransportError::Protocol)?;
        let connection = self
            .connection
            .as_mut()
            .ok_or(TransportError::HandshakeUnavailable)?;
        connection
            .send(UpstreamMessage::Text(serialized.into()))
            .await
            .map_err(|_| TransportError::Upstream)?;

        let mut completed_items = HashMap::<usize, Map<String, Value>>::new();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                self.close();
                return Err(TransportError::Timeout);
            }
            let upstream = async {
                let Some(connection) = self.connection.as_mut() else {
                    return Err(());
                };
                match connection.next().await {
                    Some(Ok(message)) => Ok(Some(message)),
                    Some(Err(_)) => Err(()),
                    None => Ok(None),
                }
            };
            let selected = tokio::time::timeout(remaining, async {
                tokio::select! {
                    message = upstream => ForwardInput::Upstream(message),
                    inbound = downstream.recv() => ForwardInput::Downstream(inbound.and_then(Result::ok)),
                }
            })
            .await
            .map_err(|_| {
                self.close();
                TransportError::Timeout
            })?;
            let message = match selected {
                ForwardInput::Upstream(Ok(Some(message))) => message,
                ForwardInput::Upstream(Ok(None)) | ForwardInput::Upstream(Err(())) => {
                    self.close();
                    return Err(TransportError::Upstream);
                }
                ForwardInput::Downstream(Some(Message::Text(text))) => {
                    let value = match serde_json::from_str::<Value>(&text) {
                        Ok(value) => value,
                        Err(_) => {
                            send_error(
                                downstream,
                                "invalid_request_error",
                                "invalid_json",
                                "Request body is not valid JSON.",
                                None,
                            )
                            .await
                            .map_err(|_| TransportError::DownstreamClosed)?;
                            continue;
                        }
                    };
                    let Some(object) = value.as_object() else {
                        send_error(
                            downstream,
                            "invalid_request_error",
                            "unsupported_event",
                            "unsupported websocket event",
                            None,
                        )
                        .await
                        .map_err(|_| TransportError::DownstreamClosed)?;
                        continue;
                    };
                    if object.get("type").and_then(Value::as_str) != Some("response.cancel") {
                        send_error(
                            downstream,
                            "invalid_request_error",
                            "unsupported_event",
                            "unsupported websocket event",
                            None,
                        )
                        .await
                        .map_err(|_| TransportError::DownstreamClosed)?;
                        continue;
                    }
                    let cancel = serde_json::to_string(&json!({"type": "response.cancel"}))
                        .map_err(|_| TransportError::Protocol)?;
                    let connection = self.connection.as_mut().ok_or(TransportError::Upstream)?;
                    connection
                        .send(UpstreamMessage::Text(cancel.into()))
                        .await
                        .map_err(|_| TransportError::Upstream)?;
                    continue;
                }
                ForwardInput::Downstream(Some(Message::Ping(payload))) => {
                    downstream
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|_| TransportError::DownstreamClosed)?;
                    continue;
                }
                ForwardInput::Downstream(Some(Message::Pong(_))) => continue,
                ForwardInput::Downstream(Some(Message::Binary(_))) => {
                    send_error(
                        downstream,
                        "invalid_request_error",
                        "invalid_json",
                        "Request body is not valid JSON.",
                        None,
                    )
                    .await
                    .map_err(|_| TransportError::DownstreamClosed)?;
                    continue;
                }
                ForwardInput::Downstream(Some(Message::Close(_)))
                | ForwardInput::Downstream(None) => {
                    self.close();
                    return Err(TransportError::DownstreamClosed);
                }
            };
            let raw = match message {
                UpstreamMessage::Text(text) => text.to_string(),
                UpstreamMessage::Binary(_) => {
                    self.close();
                    return Err(TransportError::Protocol);
                }
                UpstreamMessage::Ping(payload) => {
                    let connection = self.connection.as_mut().ok_or(TransportError::Upstream)?;
                    connection
                        .send(UpstreamMessage::Pong(payload))
                        .await
                        .map_err(|_| TransportError::Upstream)?;
                    continue;
                }
                UpstreamMessage::Pong(_) => continue,
                _ => {
                    self.close();
                    return Err(TransportError::Upstream);
                }
            };
            if raw.len() > MAX_UPSTREAM_BODY_BYTES {
                self.close();
                return Err(TransportError::Protocol);
            }
            let value: Value = serde_json::from_str(&raw).map_err(|_| TransportError::Protocol)?;
            let event_type =
                validate_codex_response_event(&value).map_err(|_| TransportError::Protocol)?;
            let mut public =
                project_codex_response_event(&value).map_err(|_| TransportError::Protocol)?;
            let public = public
                .as_mut()
                .and_then(Value::as_object_mut)
                .ok_or(TransportError::Protocol)?;
            normalize_active_public_event(public, event_type)?;
            if event_type == "response.output_item.done"
                && let (Some(index), Some(item)) = (
                    public.get("output_index").and_then(Value::as_u64),
                    public.get("item").and_then(Value::as_object),
                )
                && let Ok(index) = usize::try_from(index)
            {
                completed_items.insert(index, item.clone());
            }
            if matches!(event_type, "response.completed" | "response.incomplete") {
                reconcile_completed_output(public, &completed_items)?;
            }
            let serialized = serde_json::to_string(public).map_err(|_| TransportError::Protocol)?;
            downstream
                .send(Message::Text(serialized.into()))
                .await
                .map_err(|_| TransportError::Upstream)?;
            if matches!(event_type, "response.completed" | "response.incomplete") {
                let response = public
                    .get("response")
                    .cloned()
                    .ok_or(TransportError::Protocol)?;
                if let Some(lease) = self.lease.as_ref()
                    && !state.account_store.mark_text_used(lease.token())
                {
                    super::AccountStore::note_usage_mark_failure();
                }
                return Ok(ForwardResult::Completed(response));
            }
            if matches!(event_type, "response.failed" | "error") {
                self.close();
                return Ok(ForwardResult::Failed);
            }
        }
    }

    fn finish_forward(
        &mut self,
        result: Result<ForwardResult, TransportError>,
    ) -> Result<ForwardResult, TransportError> {
        if !matches!(&result, Ok(ForwardResult::Completed(_))) {
            self.close();
        }
        result
    }

    async fn ensure_connection(
        &mut self,
        state: &AppState,
        turn: &PreparedTurn,
    ) -> Result<bool, TransportError> {
        if self.disabled {
            return Err(TransportError::HandshakeUnavailable);
        }
        if state.config.upstream_protocol != super::UpstreamProtocol::ChatGpt {
            return Err(TransportError::HandshakeUnavailable);
        }
        let model = turn
            .replay_body
            .get("model")
            .and_then(Value::as_str)
            .ok_or(TransportError::Protocol)?;
        let current_token = self.lease.as_ref().map(|lease| lease.token().to_owned());
        let lease = if let Some(token) = current_token.as_deref() {
            let groups = state.account_type_catalog.supported_types_for(model);
            state
                .account_store
                .acquire_exact_with_type_and_source_filter(model, token, groups.as_ref())
                .await
        } else {
            None
        };
        let lease = match lease {
            Some(lease) => lease,
            None => acquire_native_codex_lease(state, model, &std::collections::HashSet::new())
                .await
                .ok_or(TransportError::HandshakeUnavailable)?,
        };
        let key = credential_key(&lease);
        let reused = self.connection.is_some()
            && self
                .credential_key
                .as_ref()
                .is_some_and(|current| current == &key);
        if reused {
            if !self.connection_is_ready_before_send().await {
                self.close();
            } else {
                let previous = self.lease.replace(lease);
                drop(previous);
                return Ok(true);
            }
        }
        self.close();
        let base_url = state
            .config
            .upstream_base_url
            .as_deref()
            .unwrap_or("https://chatgpt.com");
        self.lease = Some(lease);
        self.credential_key = Some(key);
        if self.connect(base_url).await.is_err() {
            self.close();
            self.disabled = true;
            return Err(TransportError::HandshakeUnavailable);
        }
        Ok(false)
    }

    async fn connection_is_ready_before_send(&mut self) -> bool {
        for _ in 0..MAX_IDLE_CONTROL_FRAMES {
            let message = {
                let Some(connection) = self.connection.as_mut() else {
                    return false;
                };
                match tokio::time::timeout(IDLE_CONNECTION_POLL_TIMEOUT, connection.next()).await {
                    Err(_) => return true,
                    Ok(Some(Ok(message))) => message,
                    Ok(Some(Err(_))) | Ok(None) => return false,
                }
            };
            match message {
                UpstreamMessage::Ping(payload) => {
                    let Some(connection) = self.connection.as_mut() else {
                        return false;
                    };
                    if connection
                        .send(UpstreamMessage::Pong(payload))
                        .await
                        .is_err()
                    {
                        return false;
                    }
                }
                UpstreamMessage::Pong(_) => {}
                UpstreamMessage::Close(_) => return false,
                UpstreamMessage::Text(_) | UpstreamMessage::Binary(_) => return false,
                _ => return false,
            }
        }
        true
    }

    async fn connect(&mut self, base_url: &str) -> Result<(), ()> {
        let key = self.credential_key.as_ref().ok_or(())?;
        let url = websocket_url(base_url).ok_or(())?;
        let proxy = (!key.proxy_url.is_empty()).then_some(key.proxy_url.as_str());
        for (attempt, delay) in CONNECT_RETRY_DELAYS
            .iter()
            .copied()
            .chain(std::iter::once(Duration::ZERO))
            .enumerate()
        {
            let request = websocket_request(&url, key).map_err(|_| ())?;
            let result = connect_request(request, &url, proxy).await;
            match result {
                Ok(connection) => {
                    self.connection = Some(connection);
                    return Ok(());
                }
                Err(()) if attempt < CONNECT_RETRY_DELAYS.len() => {
                    tokio::time::sleep(delay).await;
                }
                Err(()) => return Err(()),
            }
        }
        Err(())
    }

    fn close(&mut self) {
        self.connection = None;
        self.credential_key = None;
        self.lease = None;
    }
}

fn credential_key(lease: &super::AccountLease) -> CredentialKey {
    let raw_proxy = lease
        .proxy_url()
        .map(ToOwned::to_owned)
        .or_else(|| env::var("RUST_UPSTREAM_PROXY").ok())
        .unwrap_or_default();
    CredentialKey {
        token: lease.token().to_owned(),
        account_id: lease.chatgpt_account_id().unwrap_or_default().to_owned(),
        proxy_url: normalize_proxy_url(&raw_proxy)
            .map(|proxy| proxy.to_string())
            .unwrap_or(raw_proxy),
    }
}

fn websocket_url(base_url: &str) -> Option<Url> {
    let mut url = Url::parse(&format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        CODEX_RESPONSES_WEBSOCKET_PATH.trim_start_matches('/')
    ))
    .ok()?;
    let scheme = match url.scheme() {
        "http" => "ws",
        "https" => "wss",
        _ => return None,
    };
    url.set_scheme(scheme).ok()?;
    Some(url)
}

fn websocket_request(
    url: &Url,
    key: &CredentialKey,
) -> Result<tokio_tungstenite::tungstenite::http::Request<()>, ()> {
    let mut request = url.as_str().into_client_request().map_err(|_| ())?;
    let headers = request.headers_mut();
    headers.insert(
        "Authorization",
        WsHeaderValue::from_str(&format!("Bearer {}", key.token)).map_err(|_| ())?,
    );
    headers.insert(
        "User-Agent",
        WsHeaderValue::from_static(CODEX_RESPONSES_WEBSOCKET_USER_AGENT),
    );
    headers.insert("Originator", WsHeaderValue::from_static("codex-tui"));
    headers.insert(
        "OpenAI-Beta",
        WsHeaderValue::from_static(CODEX_RESPONSES_WEBSOCKET_BETA),
    );
    headers.insert(
        "session-id",
        WsHeaderValue::from_str(&super::native_message_id()).map_err(|_| ())?,
    );
    headers.insert(
        "thread-id",
        WsHeaderValue::from_str(&super::native_message_id()).map_err(|_| ())?,
    );
    if !key.account_id.is_empty() {
        headers.insert(
            "ChatGPT-Account-ID",
            WsHeaderValue::from_str(&key.account_id).map_err(|_| ())?,
        );
    }
    Ok(request)
}

async fn connect_request(
    request: tokio_tungstenite::tungstenite::http::Request<()>,
    target: &Url,
    proxy: Option<&str>,
) -> Result<UpstreamWebSocket, ()> {
    ensure_rustls_crypto_provider();
    let stream = tokio::time::timeout(Duration::from_secs(20), async {
        if let Some(proxy) = proxy {
            let proxy = normalize_proxy_url(proxy).ok_or(())?;
            connect_via_proxy(&proxy, target).await
        } else {
            Ok(ProxyStream::Tcp(connect_target(target).await?))
        }
    })
    .await
    .map_err(|_| ())??;
    let (socket, _) =
        tokio::time::timeout(Duration::from_secs(20), client_async_tls(request, stream))
            .await
            .map_err(|_| ())?
            .map_err(|_| ())?;
    Ok(socket)
}

async fn connect_target(target: &Url) -> Result<TcpStream, ()> {
    let host = target.host_str().ok_or(())?;
    let port = target.port_or_known_default().ok_or(())?;
    tokio::time::timeout(Duration::from_secs(20), TcpStream::connect((host, port)))
        .await
        .map_err(|_| ())?
        .map_err(|_| ())
}

fn normalize_proxy_url(raw: &str) -> Option<Url> {
    let mut candidate = raw.trim().to_owned();
    if candidate.is_empty() {
        return None;
    }
    if !candidate.contains("://")
        && let Some(colon_url) = colon_proxy_to_url(&candidate)
    {
        candidate = colon_url;
    }
    let mut proxy = Url::parse(&candidate).ok()?;
    let scheme = proxy.scheme().to_ascii_lowercase();
    match scheme.as_str() {
        "socks" | "socks5" => proxy.set_scheme("socks5h").ok()?,
        "http" | "https" | "socks5h" => {}
        _ => return None,
    }
    Some(proxy)
}

fn colon_proxy_to_url(candidate: &str) -> Option<String> {
    let parts = candidate.splitn(4, ':').collect::<Vec<_>>();
    if parts.len() == 4 && parts[1].chars().all(|character| character.is_ascii_digit()) {
        let mut proxy = Url::parse(&format!("http://{}:{}", parts[0], parts[1])).ok()?;
        proxy.set_username(parts[2]).ok()?;
        proxy.set_password(Some(parts[3])).ok()?;
        return Some(proxy.to_string());
    }
    if parts.len() == 2
        && parts[1].chars().all(|character| character.is_ascii_digit())
        && parts[1].parse::<u16>().is_ok()
    {
        return Some(format!("http://{candidate}"));
    }
    None
}

struct ProxyCredentials {
    username: String,
    password: String,
}

fn proxy_credentials(proxy: &Url) -> Result<Option<ProxyCredentials>, ()> {
    let raw_username = proxy.username();
    let raw_password = proxy.password();
    if raw_username.is_empty() && raw_password.is_none() {
        return Ok(None);
    }
    let credentials = ProxyCredentials {
        username: decode_proxy_auth_component(raw_username)?,
        password: decode_proxy_auth_component(raw_password.unwrap_or_default())?,
    };
    if credentials
        .username
        .len()
        .checked_add(credentials.password.len())
        .and_then(|length| length.checked_add(1))
        .is_none_or(|length| length > MAX_PROXY_AUTH_HEADER_BYTES)
    {
        return Err(());
    }
    Ok(Some(credentials))
}

fn decode_proxy_auth_component(raw: &str) -> Result<String, ()> {
    let bytes = raw.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len()
                || !bytes[index + 1].is_ascii_hexdigit()
                || !bytes[index + 2].is_ascii_hexdigit()
            {
                return Err(());
            }
            index += 3;
        } else {
            index += 1;
        }
    }
    let decoded = percent_decode_str(raw)
        .decode_utf8()
        .map_err(|_| ())?
        .into_owned();
    if decoded.chars().any(char::is_control) {
        return Err(());
    }
    Ok(decoded)
}

fn socks5_auth_fields(credentials: &ProxyCredentials) -> Result<(&[u8], &[u8]), ()> {
    let username = credentials.username.as_bytes();
    let password = credentials.password.as_bytes();
    if username.len() > MAX_SOCKS5_AUTH_FIELD_BYTES || password.len() > MAX_SOCKS5_AUTH_FIELD_BYTES
    {
        return Err(());
    }
    Ok((username, password))
}

async fn connect_via_proxy(proxy: &Url, target: &Url) -> Result<ProxyStream, ()> {
    connect_via_proxy_with_tls_connector(proxy, target, None).await
}

async fn connect_via_proxy_with_tls_connector(
    proxy: &Url,
    target: &Url,
    tls_connector: Option<TlsConnector>,
) -> Result<ProxyStream, ()> {
    match proxy.scheme() {
        "http" => Ok(ProxyStream::Tcp(
            connect_http_proxy(proxy, target, connect_proxy_tcp(proxy).await?).await?,
        )),
        "https" => {
            let stream = connect_proxy_tcp(proxy).await?;
            let connector = match tls_connector {
                Some(connector) => connector,
                None => proxy_tls_connector()?,
            };
            let host = proxy.host_str().ok_or(())?.to_owned();
            let server_name = ServerName::try_from(host).map_err(|_| ())?;
            let stream = connector
                .connect(server_name, stream)
                .await
                .map_err(|_| ())?;
            Ok(ProxyStream::HttpsProxy(Box::new(
                connect_http_proxy(proxy, target, stream).await?,
            )))
        }
        "socks5h" => Ok(ProxyStream::Tcp(connect_socks5_proxy(proxy, target).await?)),
        _ => Err(()),
    }
}

async fn connect_proxy_tcp(proxy: &Url) -> Result<TcpStream, ()> {
    let proxy_host = proxy.host_str().ok_or(())?;
    let proxy_port = proxy.port_or_known_default().ok_or(())?;
    tokio::time::timeout(
        Duration::from_secs(20),
        TcpStream::connect((proxy_host, proxy_port)),
    )
    .await
    .map_err(|_| ())?
    .map_err(|_| ())
}

async fn connect_http_proxy<S>(proxy: &Url, target: &Url, mut stream: S) -> Result<S, ()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let credentials = proxy_credentials(proxy)?;
    let target_host = target.host_str().ok_or(())?;
    let target_port = target.port_or_known_default().ok_or(())?;
    let authority = if target_host.contains(':') {
        format!("[{target_host}]:{target_port}")
    } else {
        format!("{target_host}:{target_port}")
    };
    let mut request =
        format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nConnection: keep-alive\r\n");
    if let Some(credentials) = credentials {
        let encoded = BASE64.encode(format!("{}:{}", credentials.username, credentials.password));
        request.push_str(&format!("Proxy-Authorization: Basic {encoded}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await.map_err(|_| ())?;
    let response = read_proxy_headers(&mut stream).await?;
    let line = std::str::from_utf8(&response)
        .ok()
        .and_then(|value| value.lines().next())
        .ok_or(())?;
    let mut parts = line.split_whitespace();
    let _http = parts.next().ok_or(())?;
    let status = parts.next().ok_or(())?;
    if status != "200" {
        return Err(());
    }
    Ok(stream)
}

async fn read_proxy_headers<S>(stream: &mut S) -> Result<Vec<u8>, ()>
where
    S: AsyncRead + Unpin,
{
    let mut response = Vec::new();
    let mut chunk = [0_u8; 4096];
    loop {
        let count = stream.read(&mut chunk).await.map_err(|_| ())?;
        if count == 0 {
            return Err(());
        }
        response.extend_from_slice(&chunk[..count]);
        if response.windows(4).any(|window| window == b"\r\n\r\n") {
            return Ok(response);
        }
        if response.len() > 64 * 1024 {
            return Err(());
        }
    }
}

fn proxy_tls_connector() -> Result<TlsConnector, ()> {
    proxy_tls_connector_with_extra_roots(std::iter::empty())
}

fn ensure_rustls_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn proxy_tls_connector_with_extra_roots<I>(extra_roots: I) -> Result<TlsConnector, ()>
where
    I: IntoIterator<Item = CertificateDer<'static>>,
{
    ensure_rustls_crypto_provider();
    let mut roots = RootCertStore::empty();
    let result = rustls_native_certs::load_native_certs();
    let (added, _) = roots.add_parsable_certificates(result.certs);
    for certificate in extra_roots {
        roots.add(certificate).map_err(|_| ())?;
    }
    if added == 0 && roots.is_empty() {
        return Err(());
    }
    let config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(config)))
}

async fn connect_socks5_proxy(proxy: &Url, target: &Url) -> Result<TcpStream, ()> {
    let credentials = proxy_credentials(proxy)?;
    let auth_fields = match credentials.as_ref() {
        Some(credentials) => Some(socks5_auth_fields(credentials)?),
        None => None,
    };
    let mut stream = connect_proxy_tcp(proxy).await?;
    let methods = if credentials.is_none() {
        vec![0_u8]
    } else {
        vec![0_u8, 2_u8]
    };
    stream
        .write_all(&[5, methods.len() as u8])
        .await
        .map_err(|_| ())?;
    stream.write_all(&methods).await.map_err(|_| ())?;
    let version = stream.read_u8().await.map_err(|_| ())?;
    let method = stream.read_u8().await.map_err(|_| ())?;
    if version != 5 || method == 0xff {
        return Err(());
    }
    if method == 2 {
        let (username, password) = auth_fields.ok_or(())?;
        stream
            .write_all(&[1, username.len() as u8])
            .await
            .map_err(|_| ())?;
        stream.write_all(username).await.map_err(|_| ())?;
        stream
            .write_all(&[password.len() as u8])
            .await
            .map_err(|_| ())?;
        stream.write_all(password).await.map_err(|_| ())?;
        if stream.read_u8().await.map_err(|_| ())? != 1
            || stream.read_u8().await.map_err(|_| ())? != 0
        {
            return Err(());
        }
    } else if method != 0 {
        return Err(());
    }
    let host = target.host_str().ok_or(())?;
    if host.len() > 255 {
        return Err(());
    }
    let port = target.port_or_known_default().ok_or(())?;
    let mut request = vec![5, 1, 0, 3, host.len() as u8];
    request.extend_from_slice(host.as_bytes());
    request.extend_from_slice(&port.to_be_bytes());
    stream.write_all(&request).await.map_err(|_| ())?;
    if stream.read_u8().await.map_err(|_| ())? != 5
        || stream.read_u8().await.map_err(|_| ())? != 0
        || stream.read_u8().await.map_err(|_| ())? != 0
    {
        return Err(());
    }
    match stream.read_u8().await.map_err(|_| ())? {
        1 => {
            let mut address = [0_u8; 4];
            stream.read_exact(&mut address).await.map_err(|_| ())?;
        }
        3 => {
            let length = stream.read_u8().await.map_err(|_| ())? as usize;
            let mut address = vec![0_u8; length];
            stream.read_exact(&mut address).await.map_err(|_| ())?;
        }
        4 => {
            let mut address = [0_u8; 16];
            stream.read_exact(&mut address).await.map_err(|_| ())?;
        }
        _ => return Err(()),
    }
    let mut bound_port = [0_u8; 2];
    stream.read_exact(&mut bound_port).await.map_err(|_| ())?;
    Ok(stream)
}

fn normalize_active_public_event(
    public: &mut Map<String, Value>,
    event_type: &str,
) -> Result<(), TransportError> {
    if !matches!(event_type, "response.created" | "response.in_progress") {
        return Ok(());
    }
    let response = public
        .get_mut("response")
        .and_then(Value::as_object_mut)
        .ok_or(TransportError::Protocol)?;
    if response.get("status").is_none() {
        response.insert("status".to_owned(), Value::String("in_progress".to_owned()));
    }
    Ok(())
}

fn reconcile_completed_output(
    public: &mut Map<String, Value>,
    completed_items: &HashMap<usize, Map<String, Value>>,
) -> Result<(), TransportError> {
    let response = public
        .get_mut("response")
        .and_then(Value::as_object_mut)
        .ok_or(TransportError::Protocol)?;
    let Some(output) = response.get_mut("output").and_then(Value::as_array_mut) else {
        return Ok(());
    };
    for (index, item) in output.iter_mut().enumerate() {
        let Some(completed) = completed_items.get(&index) else {
            continue;
        };
        let Some(item) = item.as_object_mut() else {
            continue;
        };
        let mut merged = completed.clone();
        for (key, value) in item.iter() {
            merged.insert(key.clone(), value.clone());
        }
        *item = merged;
    }
    Ok(())
}

async fn forward_http_response(
    socket: &mut WebSocket,
    response: axum::response::Response,
) -> Result<ForwardResult, ()> {
    let mut input = response.into_body().into_data_stream();
    let mut buffer = Vec::new();
    let mut total = 0usize;
    let mut completed_items = HashMap::<usize, Map<String, Value>>::new();
    while let Some(chunk) = input.next().await {
        let chunk = chunk.map_err(|_| ())?;
        total = total.checked_add(chunk.len()).ok_or(())?;
        if total > MAX_REQUEST_BODY_BYTES * 4 {
            return Err(());
        }
        buffer.extend_from_slice(&chunk);
        while let Some((position, delimiter_length)) = sse_delimiter(&buffer) {
            let event = buffer.drain(..position).collect::<Vec<_>>();
            buffer.drain(..delimiter_length);
            let Some(data) = codex_sse_data(&event).map_err(|_| ())? else {
                continue;
            };
            if data == "[DONE]" {
                continue;
            }
            let value: Value = serde_json::from_str(&data).map_err(|_| ())?;
            let event_type = validate_codex_response_event(&value).map_err(|_| ())?;
            let mut public = project_codex_response_event(&value).map_err(|_| ())?;
            let public = public.as_mut().and_then(Value::as_object_mut).ok_or(())?;
            normalize_active_public_event(public, event_type).map_err(|_| ())?;
            if event_type == "response.output_item.done"
                && let (Some(index), Some(item)) = (
                    public.get("output_index").and_then(Value::as_u64),
                    public.get("item").and_then(Value::as_object),
                )
                && let Ok(index) = usize::try_from(index)
            {
                completed_items.insert(index, item.clone());
            }
            if matches!(event_type, "response.completed" | "response.incomplete") {
                reconcile_completed_output(public, &completed_items).map_err(|_| ())?;
            }
            let serialized = serde_json::to_string(public).map_err(|_| ())?;
            socket
                .send(Message::Text(serialized.into()))
                .await
                .map_err(|_| ())?;
            if matches!(event_type, "response.completed" | "response.incomplete") {
                return Ok(ForwardResult::Completed(
                    public.get("response").cloned().ok_or(())?,
                ));
            }
            if matches!(event_type, "response.failed" | "error") {
                return Ok(ForwardResult::Failed);
            }
        }
    }
    Err(())
}

async fn send_api_error(socket: &mut WebSocket, error: ApiError) -> Result<(), ()> {
    let (kind, code, message) = error.websocket_parts();
    send_error(socket, kind, code, message, None).await
}

async fn send_error(
    socket: &mut WebSocket,
    kind: &str,
    code: &str,
    message: &str,
    status: Option<u16>,
) -> Result<(), ()> {
    let mut error = json!({
        "type": "error",
        "error": {"type": kind, "code": code, "message": message},
    });
    if let Some(status) = status {
        error["status"] = json!(status);
    }
    let text = serde_json::to_string(&error).map_err(|_| ())?;
    socket
        .send(Message::Text(text.into()))
        .await
        .map_err(|_| ())
}

struct TranscriptBudget {
    capacity: usize,
    used: Mutex<usize>,
}

impl TranscriptBudget {
    const fn new(capacity: usize) -> Self {
        Self {
            capacity,
            used: Mutex::new(0),
        }
    }

    fn replace(&self, current: usize, next: usize) -> bool {
        let Ok(mut used) = self.used.lock() else {
            return false;
        };
        let Some(without_current) = used.checked_sub(current) else {
            return false;
        };
        let Some(next_used) = without_current.checked_add(next) else {
            return false;
        };
        if next_used > self.capacity {
            return false;
        }
        *used = next_used;
        true
    }

    fn release(&self, amount: usize) {
        if let Ok(mut used) = self.used.lock() {
            *used = used.saturating_sub(amount);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::PathBuf,
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    use axum::{Json, Router, extract::ws::WebSocketUpgrade, routing::get};
    use futures_util::StreamExt;
    use rustls::{ServerConfig, pki_types::PrivateKeyDer};
    use tokio_rustls::TlsAcceptor;
    use tokio_tungstenite::{connect_async, tungstenite::client::IntoClientRequest};

    static TEST_FILE_COUNTER: AtomicUsize = AtomicUsize::new(0);

    struct ProjectFileCleanup(PathBuf);

    impl Drop for ProjectFileCleanup {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    fn project_test_file(stem: &str) -> (PathBuf, ProjectFileCleanup) {
        let root = env::var_os("CHATGPT2API_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(".local/codex/tmp/rust"));
        fs::create_dir_all(&root).expect("project test temp directory");
        let sequence = TEST_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = root.join(format!("{stem}-{}-{sequence}.json", std::process::id()));
        (path.clone(), ProjectFileCleanup(path))
    }

    #[derive(Default)]
    struct FakeKeepaliveState {
        sessions: AtomicUsize,
        first_idle_sent: tokio::sync::Notify,
        second_payload: StdMutex<Option<Value>>,
    }

    async fn fake_keepalive_upstream(mut socket: WebSocket, state: Arc<FakeKeepaliveState>) {
        let session = state.sessions.fetch_add(1, Ordering::SeqCst);
        let payload = loop {
            match socket.recv().await {
                Some(Ok(Message::Text(text))) => {
                    break serde_json::from_str::<Value>(text.as_str()).expect("upstream payload");
                }
                Some(Ok(Message::Ping(payload))) => {
                    let _ = socket.send(Message::Pong(payload)).await;
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return,
            }
        };
        assert_eq!(payload["type"], "response.create");
        if session == 0 {
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "sequence_number": 1,
                        "response": {
                            "id": "resp-one",
                            "model": "gpt-test",
                            "status": "completed",
                            "output": []
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("first terminal");
            socket
                .send(Message::Ping(b"idle".to_vec().into()))
                .await
                .expect("idle ping");
            let _ = socket.send(Message::Close(None)).await;
            state.first_idle_sent.notify_one();
        } else {
            *state.second_payload.lock().expect("payload lock") = Some(payload);
            socket
                .send(Message::Text(
                    json!({
                        "type": "response.completed",
                        "sequence_number": 1,
                        "response": {
                            "id": "resp-two",
                            "model": "gpt-test",
                            "status": "completed",
                            "output": []
                        }
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .expect("second terminal");
        }
    }

    #[test]
    fn session_replays_only_when_the_upstream_socket_cannot_reuse_properties() {
        let mut session = Session::default();
        let first = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "input": "hello"
            }))
            .expect("first response.create");
        assert_eq!(first.incremental_body["stream"], true);
        session.last_request = Some(first.replay_body.clone());
        session.pending_request_properties = Some(first.request_properties.clone());
        assert!(session.commit(json!({
            "id": "resp-1",
            "output": [{
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": "reply"}]
            }]
        })));

        let continuation = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "previous_response_id": "resp-1",
                "input": "next"
            }))
            .expect("continuation response.create");
        assert_eq!(
            continuation.incremental_body["previous_response_id"],
            "resp-1"
        );
        assert_eq!(
            continuation.incremental_body["input"].as_str(),
            Some("next")
        );
        assert_eq!(
            continuation.replay_body["input"].as_array().map(Vec::len),
            Some(3)
        );

        let error = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "previous_response_id": "foreign",
                "input": "next"
            }))
            .expect_err("foreign response id must fail closed");
        assert_eq!(error.websocket_parts().1, "previous_response_not_found");
    }

    #[test]
    fn warmup_keeps_generate_false_for_the_native_wire_payload() {
        let session = Session::default();
        let turn = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "input": [],
                "generate": false,
            }))
            .expect("warmup");
        assert!(turn.warmup);
        assert_eq!(turn.replay_body["generate"], false);
        let payload = wire_payload(&turn.replay_body).expect("native warmup wire payload");
        assert_eq!(payload["generate"], false);
    }

    #[test]
    fn proxy_normalization_supports_colon_credentials_and_remote_dns_schemes() {
        let colon = normalize_proxy_url("proxy.example.test:8080:user@example:p:a:ss")
            .expect("colon proxy");
        assert_eq!(colon.scheme(), "http");
        assert_eq!(colon.host_str(), Some("proxy.example.test"));
        assert_eq!(colon.port(), Some(8080));
        assert_eq!(colon.username(), "user%40example");
        assert_eq!(colon.password(), Some("p%3Aa%3Ass"));
        let credentials = proxy_credentials(&colon).expect("decoded credentials");
        let credentials = credentials.expect("credentials present");
        assert_eq!(credentials.username, "user@example");
        assert_eq!(credentials.password, "p:a:ss");

        assert_eq!(
            normalize_proxy_url("socks://proxy.example.test:1080")
                .expect("socks normalization")
                .scheme(),
            "socks5h"
        );
        assert_eq!(
            normalize_proxy_url("socks5://proxy.example.test:1080")
                .expect("socks5 normalization")
                .scheme(),
            "socks5h"
        );
        assert_eq!(
            normalize_proxy_url("http://proxy.example.test:8080")
                .expect("http proxy")
                .scheme(),
            "http"
        );
        assert_eq!(
            normalize_proxy_url("https://proxy.example.test:8443")
                .expect("https proxy")
                .scheme(),
            "https"
        );
    }

    #[test]
    fn proxy_auth_is_strictly_decoded_and_bounded() {
        let over_socks_limit = "u".repeat(MAX_SOCKS5_AUTH_FIELD_BYTES + 1);
        let proxy = Url::parse(&format!(
            "http://{over_socks_limit}:password@proxy.example.test:8080"
        ))
        .expect("long URL");
        let credentials = proxy_credentials(&proxy)
            .expect("HTTP credentials within the total header budget")
            .expect("HTTP credentials present");
        assert_eq!(credentials.username.len(), MAX_SOCKS5_AUTH_FIELD_BYTES + 1);
        assert!(socks5_auth_fields(&credentials).is_err());

        let over_header_limit = "u".repeat(MAX_PROXY_AUTH_HEADER_BYTES);
        let proxy = Url::parse(&format!(
            "http://{over_header_limit}:password@proxy.example.test:8080"
        ))
        .expect("oversized URL");
        assert!(proxy_credentials(&proxy).is_err());

        let control =
            Url::parse("http://user%0Aname:password@proxy.example.test:8080").expect("control URL");
        assert!(proxy_credentials(&control).is_err());

        let malformed = Url::parse("http://user%ZZ:password@proxy.example.test:8080")
            .expect("malformed escape remains representable");
        assert!(proxy_credentials(&malformed).is_err());
    }

    #[tokio::test]
    async fn http_proxy_connect_sends_decoded_auth_and_accepts_no_auth() {
        async fn run_case(proxy_url: String, expected_auth: Option<String>) {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("HTTP proxy listener");
            let address = listener.local_addr().expect("HTTP proxy address");
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("HTTP proxy accept");
                let request = read_proxy_headers(&mut stream)
                    .await
                    .expect("HTTP CONNECT request");
                let request = String::from_utf8(request).expect("HTTP request UTF-8");
                assert!(request.starts_with("CONNECT remote.example.test:443 HTTP/1.1\r\n"));
                match expected_auth {
                    Some(expected) => assert!(
                        request.contains(&format!("Proxy-Authorization: Basic {expected}\r\n"))
                    ),
                    None => assert!(!request.contains("Proxy-Authorization:")),
                }
                stream
                    .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    .await
                    .expect("HTTP proxy response");
            });
            let proxy =
                normalize_proxy_url(&proxy_url.replace("PROXY_PORT", &address.port().to_string()))
                    .expect("HTTP proxy URL");
            let target = Url::parse("wss://remote.example.test:443").expect("target URL");
            let stream = connect_via_proxy(&proxy, &target)
                .await
                .expect("HTTP CONNECT");
            assert!(matches!(stream, ProxyStream::Tcp(_)));
            server.await.expect("HTTP proxy server");
        }

        run_case(
            "http://user%40name:p%3Ass@127.0.0.1:PROXY_PORT".to_owned(),
            Some(BASE64.encode("user@name:p:ss")),
        )
        .await;
        let long_username = "u".repeat(MAX_SOCKS5_AUTH_FIELD_BYTES + 1);
        run_case(
            format!("http://{long_username}:password@127.0.0.1:PROXY_PORT"),
            Some(BASE64.encode(format!("{long_username}:password"))),
        )
        .await;
        run_case("http://127.0.0.1:PROXY_PORT".to_owned(), None).await;
    }

    #[tokio::test]
    async fn http_proxy_non_200_reply_fails_closed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTP proxy listener");
        let address = listener.local_addr().expect("HTTP proxy address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("HTTP proxy accept");
            let _ = read_proxy_headers(&mut stream).await.expect("HTTP request");
            stream
                .write_all(b"HTTP/1.1 407 Proxy Authentication Required\r\n\r\n")
                .await
                .expect("HTTP failure response");
        });
        let proxy = normalize_proxy_url(&format!("http://127.0.0.1:{}", address.port()))
            .expect("HTTP proxy URL");
        let target = Url::parse("wss://remote.example.test:443").expect("target URL");
        assert!(connect_via_proxy(&proxy, &target).await.is_err());
        server.await.expect("HTTP proxy server");
    }

    #[tokio::test]
    async fn https_proxy_connect_uses_tls_and_decoded_auth() {
        ensure_rustls_crypto_provider();
        let certificate = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("proxy certificate");
        let certificate_der = CertificateDer::from(certificate.cert.der().to_vec());
        let private_key = PrivateKeyDer::try_from(certificate.key_pair.serialize_der())
            .expect("proxy private key");
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![certificate_der.clone()], private_key)
            .expect("proxy TLS config");
        let acceptor = TlsAcceptor::from(Arc::new(server_config));
        let client_connector =
            proxy_tls_connector_with_extra_roots(std::iter::once(certificate_der))
                .expect("proxy client TLS config");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("HTTPS proxy listener");
        let address = listener.local_addr().expect("HTTPS proxy address");
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("HTTPS proxy accept");
            let mut stream = acceptor.accept(stream).await.expect("HTTPS proxy TLS");
            let request = read_proxy_headers(&mut stream)
                .await
                .expect("HTTPS CONNECT request");
            let request = String::from_utf8(request).expect("HTTPS request UTF-8");
            assert!(request.starts_with("CONNECT remote.example.test:443 HTTP/1.1\r\n"));
            let expected = BASE64.encode("https-user:https-pass");
            assert!(request.contains(&format!("Proxy-Authorization: Basic {expected}\r\n")));
            stream
                .write_all(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                .await
                .expect("HTTPS proxy response");
        });
        let proxy = normalize_proxy_url(&format!(
            "https://https-user:https-pass@localhost:{}",
            address.port()
        ))
        .expect("HTTPS proxy URL");
        let target = Url::parse("wss://remote.example.test:443").expect("target URL");
        let stream = connect_via_proxy_with_tls_connector(&proxy, &target, Some(client_connector))
            .await
            .expect("HTTPS CONNECT");
        assert!(matches!(stream, ProxyStream::HttpsProxy(_)));
        server.await.expect("HTTPS proxy server");
    }

    #[tokio::test]
    async fn socks5h_proxy_uses_remote_dns_and_decoded_auth() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("SOCKS proxy listener");
        let address = listener.local_addr().expect("SOCKS proxy address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("SOCKS proxy accept");
            let mut greeting = [0_u8; 2];
            stream
                .read_exact(&mut greeting)
                .await
                .expect("SOCKS greeting");
            assert_eq!(greeting, [5, 2]);
            let mut methods = [0_u8; 2];
            stream
                .read_exact(&mut methods)
                .await
                .expect("SOCKS methods");
            assert_eq!(methods, [0, 2]);
            stream
                .write_all(&[5, 2])
                .await
                .expect("SOCKS method choice");

            let mut auth_header = [0_u8; 2];
            stream
                .read_exact(&mut auth_header)
                .await
                .expect("SOCKS auth header");
            assert_eq!(auth_header[0], 1);
            let mut username = vec![0_u8; auth_header[1] as usize];
            stream
                .read_exact(&mut username)
                .await
                .expect("SOCKS username");
            let password_length = stream.read_u8().await.expect("SOCKS password length");
            let mut password = vec![0_u8; password_length as usize];
            stream
                .read_exact(&mut password)
                .await
                .expect("SOCKS password");
            assert_eq!(username, b"socks@user");
            assert_eq!(password, b"socks:pass");
            stream.write_all(&[1, 0]).await.expect("SOCKS auth result");

            let mut request_header = [0_u8; 5];
            stream
                .read_exact(&mut request_header)
                .await
                .expect("SOCKS CONNECT header");
            assert_eq!(request_header[..4], [5, 1, 0, 3]);
            let mut host = vec![0_u8; request_header[4] as usize];
            stream.read_exact(&mut host).await.expect("SOCKS host");
            let mut port = [0_u8; 2];
            stream.read_exact(&mut port).await.expect("SOCKS port");
            assert_eq!(host, b"remote.example.test");
            assert_eq!(u16::from_be_bytes(port), 443);
            stream
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 80])
                .await
                .expect("SOCKS success");
        });
        let proxy = normalize_proxy_url(&format!(
            "socks5h://socks%40user:socks%3Apass@127.0.0.1:{}",
            address.port()
        ))
        .expect("SOCKS proxy URL");
        let target = Url::parse("wss://remote.example.test:443").expect("target URL");
        let stream = connect_via_proxy(&proxy, &target)
            .await
            .expect("SOCKS CONNECT");
        assert!(matches!(stream, ProxyStream::Tcp(_)));
        server.await.expect("SOCKS proxy server");
    }

    #[tokio::test]
    async fn socks5h_non_success_reply_fails_closed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("SOCKS proxy listener");
        let address = listener.local_addr().expect("SOCKS proxy address");
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("SOCKS proxy accept");
            let mut greeting = [0_u8; 3];
            stream
                .read_exact(&mut greeting)
                .await
                .expect("SOCKS greeting");
            stream
                .write_all(&[5, 0])
                .await
                .expect("SOCKS method choice");
            let mut request = [0_u8; 5];
            stream
                .read_exact(&mut request)
                .await
                .expect("SOCKS request");
            let host_length = request[4] as usize;
            let mut rest = vec![0_u8; host_length + 2];
            stream
                .read_exact(&mut rest)
                .await
                .expect("SOCKS request body");
            stream
                .write_all(&[5, 5, 0, 1, 0, 0, 0, 0, 0, 0])
                .await
                .expect("SOCKS failure");
        });
        let proxy = normalize_proxy_url(&format!("socks5h://127.0.0.1:{}", address.port()))
            .expect("SOCKS proxy URL");
        let target = Url::parse("wss://remote.example.test:443").expect("target URL");
        assert!(connect_via_proxy(&proxy, &target).await.is_err());
        server.await.expect("SOCKS proxy server");
    }

    #[test]
    fn property_change_wire_replay_does_not_readd_previous_response_id() {
        let mut session = Session::default();
        let first = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "input": "first",
                "instructions": "one"
            }))
            .expect("first turn");
        session.last_request = Some(first.replay_body.clone());
        session.pending_request_properties = Some(first.request_properties.clone());
        assert!(session.commit(json!({"id":"resp-1","output":[]})));

        let changed = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "previous_response_id": "resp-1",
                "input": "second",
                "instructions": "two"
            }))
            .expect("property-changing continuation");
        assert!(
            !changed
                .incremental_body
                .contains_key("previous_response_id")
        );
        assert_eq!(
            changed.incremental_body["input"].as_array().map(Vec::len),
            Some(2)
        );
        let payload = wire_payload(&changed.incremental_body).expect("replay wire payload");
        assert!(
            !payload
                .as_object()
                .unwrap()
                .contains_key("previous_response_id")
        );
        assert_eq!(payload["input"].as_array().map(Vec::len), Some(2));
    }

    #[test]
    fn forward_error_and_failed_terminal_close_the_transport_but_success_reuses_it() {
        let key = || CredentialKey {
            token: "token".to_owned(),
            account_id: String::new(),
            proxy_url: String::new(),
        };

        let mut failed = NativeCodexWebSocketTransport {
            credential_key: Some(key()),
            ..Default::default()
        };
        let result = failed.finish_forward(Err(TransportError::Protocol));
        assert!(result.is_err());
        assert!(failed.connection.is_none());
        assert!(failed.credential_key.is_none());
        assert!(failed.lease.is_none());

        let mut terminal_failure = NativeCodexWebSocketTransport {
            credential_key: Some(key()),
            ..Default::default()
        };
        let _ = terminal_failure.finish_forward(Ok(ForwardResult::Failed));
        assert!(terminal_failure.credential_key.is_none());

        let mut success = NativeCodexWebSocketTransport {
            credential_key: Some(key()),
            ..Default::default()
        };
        let result = success.finish_forward(Ok(ForwardResult::Completed(json!({
            "id": "resp-1"
        }))));
        assert!(result.is_ok());
        assert!(success.credential_key.is_some());
    }

    #[tokio::test]
    async fn idle_ping_close_forces_pre_send_reconnect_and_full_replay() {
        let (account_path, _cleanup) = project_test_file("responses-ws-keepalive-account");
        fs::write(
            &account_path,
            r#"{"items":[{"access_token":"codex-token","source_type":"codex","status":"正常","models":["gpt-test"]}]}"#,
        )
        .expect("account snapshot");

        let fake_state = Arc::new(FakeKeepaliveState::default());
        let fake_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("fake listener");
        let fake_address = fake_listener.local_addr().expect("fake address");
        let fake_route_state = fake_state.clone();
        let fake_server = tokio::spawn(async move {
            let websocket = get(move |upgrade: WebSocketUpgrade| {
                let fake_route_state = fake_route_state.clone();
                async move {
                    upgrade
                        .on_upgrade(move |socket| fake_keepalive_upstream(socket, fake_route_state))
                }
            });
            let app = Router::new()
                .route("/", get(|| async { "<html></html>" }))
                .route(
                    "/backend-api/models",
                    get(|| async { Json(json!({"models":[{"slug":"gpt-test"}]})) }),
                )
                .route("/backend-api/codex/responses", websocket);
            axum::serve(fake_listener, app).await.expect("fake server")
        });

        let state = AppState::new(super::super::AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{fake_address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path),
            upstream_protocol: super::super::UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        assert!(state.account_store.poison_usage_marker_lock("codex-token"));
        let downstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("downstream listener");
        let downstream_address = downstream_listener
            .local_addr()
            .expect("downstream address");
        let downstream_server = tokio::spawn({
            let router = state.router();
            async move {
                axum::serve(downstream_listener, router)
                    .await
                    .expect("downstream server")
            }
        });

        let mut request = format!("ws://{downstream_address}/v1/responses")
            .into_client_request()
            .expect("downstream request");
        request
            .headers_mut()
            .insert("Authorization", WsHeaderValue::from_static("Bearer client"));
        let (mut client, _) = connect_async(request).await.expect("downstream websocket");
        client
            .send(UpstreamMessage::Text(
                json!({
                    "type": "response.create",
                    "model": "gpt-test",
                    "input": "first"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("first request");
        let first_event = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("first event timeout")
            .expect("first event")
            .expect("first event frame");
        assert!(matches!(first_event, UpstreamMessage::Text(_)));
        tokio::time::timeout(
            Duration::from_secs(5),
            fake_state.first_idle_sent.notified(),
        )
        .await
        .expect("upstream idle close timeout");

        client
            .send(UpstreamMessage::Text(
                json!({
                    "type": "response.create",
                    "model": "gpt-test",
                    "previous_response_id": "resp-one",
                    "input": "second"
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("continuation request");
        let second_event = tokio::time::timeout(Duration::from_secs(5), client.next())
            .await
            .expect("second event timeout")
            .expect("second event")
            .expect("second event frame");
        assert!(matches!(second_event, UpstreamMessage::Text(_)));

        let payload = fake_state
            .second_payload
            .lock()
            .expect("payload lock")
            .clone()
            .expect("reconnected payload");
        assert_eq!(fake_state.sessions.load(Ordering::SeqCst), 2);
        assert!(
            !payload
                .as_object()
                .unwrap()
                .contains_key("previous_response_id")
        );
        assert_eq!(payload["input"].as_array().map(Vec::len), Some(2));
        assert_eq!(payload["input"][0]["content"][0]["text"], "first");
        assert_eq!(payload["input"][1]["content"][0]["text"], "second");

        let _ = client.close(None).await;
        downstream_server.abort();
        fake_server.abort();
    }

    #[test]
    fn independent_failure_preserves_a_but_continuation_failure_evicts_a() {
        let mut session = Session::default();
        let first = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "input": "a"
            }))
            .expect("first");
        session.last_request = Some(first.replay_body.clone());
        session.pending_request_properties = Some(first.request_properties.clone());
        assert!(session.commit(json!({"id":"resp-a","output":[]})));

        let independent = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "input": "b"
            }))
            .expect("independent");
        session.fail(&independent);
        assert_eq!(session.last_response_id.as_deref(), Some("resp-a"));

        let continuation = session
            .prepare(json!({
                "type": "response.create",
                "model": "gpt-test",
                "previous_response_id": "resp-a",
                "input": "c"
            }))
            .expect("continuation");
        session.fail(&continuation);
        assert!(session.last_response_id.is_none());
    }

    #[test]
    fn unsupported_event_uses_public_contract_code() {
        let error = Session::default()
            .prepare(json!({"type":"response.completed"}))
            .expect_err("unsupported event");
        assert_eq!(error.websocket_parts().1, "unsupported_event");
    }
}
