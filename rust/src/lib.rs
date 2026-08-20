#![forbid(unsafe_code)]

mod account_pool;
mod codex_sse;
mod codex_upstream;
mod config;
mod errors;
mod model_pool;
mod native_pow;
mod protocol_anthropic;
mod protocol_chat;
mod protocol_codex_payload;
mod protocol_responses;
mod shutdown;
use account_pool::{
    AccountLease, AccountModelGroup, AccountRecord, AccountStore, CatalogAccountCandidate,
};
#[cfg(test)]
use codex_sse::native_codex_text;
use codex_sse::{
    codex_sse_data, native_codex_delta_frame, native_codex_response_to_chat,
    native_codex_responses_json,
};
#[cfg(test)]
use codex_upstream::parse_codex_client_version;
use codex_upstream::{
    NativeRequestContext, codex_client_version, codex_request_headers, native_browser_headers,
    native_codex_response_payload,
};
pub use config::{AppConfig, AppInitError, UpstreamProtocol};
use errors::ApiError;
use model_pool::{ModelCatalog, ModelStore, PublicModel, project_remote_model_list};
#[cfg(test)]
use native_pow::{NativePowConfigInputs, native_pow_config_from_inputs};
use native_pow::{NativePowResources, native_pow_config, parse_native_pow_resources};
use protocol_anthropic::{
    anthropic_stream_responses_response, from_chat_response, from_responses_response,
    stream_body_response as anthropic_stream_body_response,
    stream_response as anthropic_stream_response, stream_responses_body_response, to_chat_payload,
    to_responses_payload, validate_message_request,
};
pub(crate) use protocol_chat::validate_chat_payload;
use protocol_chat::{
    native_completion_text, native_conversation_payload, native_finish_frame, native_frame,
    native_role_frame, native_usage, native_usage_for_prompt_tokens, native_usage_frame,
};
use protocol_codex_payload::native_codex_responses_payload;
use protocol_responses::validate_responses_payload;
pub use shutdown::run;
#[cfg(test)]
use shutdown::{serve_state_with_bounded_shutdown, serve_with_bounded_shutdown};

use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, LazyLock, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Json, Router,
    body::{Body, Bytes, to_bytes},
    extract::{DefaultBodyLimit, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine as _;
use futures_util::{Stream, StreamExt, stream, stream::FuturesUnordered};
use reqwest::Client;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sha3::Sha3_512;
use tokio::sync::{Mutex, Semaphore};
use unicode_properties::{GeneralCategory, UnicodeGeneralCategory};

#[cfg(test)]
use tokio::sync::Notify;

pub const MAX_REQUEST_BODY_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_UPSTREAM_BODY_BYTES: usize = 16 * 1024 * 1024;
const MAX_AUTH_KEYS_BYTES: u64 = 1024 * 1024;
const MAX_AUTH_KEYS: usize = 10_000;
const MAX_ACCOUNT_SNAPSHOT_BYTES: u64 = 4 * 1024 * 1024;
const MAX_ACCOUNTS: usize = 10_000;
const MAX_ACCOUNT_TOKEN_LENGTH: usize = 16 * 1024;
const MAX_MODEL_SNAPSHOT_BYTES: u64 = 1024 * 1024;
const MAX_MODELS: usize = 10_000;
const MAX_MODEL_TEXT_LENGTH: usize = 256;
const MAX_MODEL_CREATED: i64 = i64::MAX;
const PUBLIC_SERVER_ERROR: &str = "The upstream request failed. Please try again later.";
const INVALID_AUTH: &str = "密钥无效或已失效，请重新登录";
const NATIVE_UPSTREAM_TIMEOUT: Duration = Duration::from_secs(30);
const CODEX_RESPONSES_MODEL: &str = "gpt-5.5";
const NATIVE_USER_AGENT: &str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36";
const NATIVE_ORIGIN: &str = "https://chatgpt.com";
const NATIVE_CLIENT_VERSION: &str = "prod-a194cd50d4416d3c0b47c740f206b12ce60f5887";
const NATIVE_CLIENT_BUILD_NUMBER: &str = "6708908";
const NATIVE_SEC_CH_UA: &str =
    "\"Microsoft Edge\";v=\"143\", \"Chromium\";v=\"143\", \"Not A(Brand\";v=\"24\"";
const DEFAULT_POW_SCRIPT: &str = "https://chatgpt.com/backend-api/sentinel/sdk.js";
const MAX_POW_ITERATIONS: u32 = 500_000;
const MAX_POW_TEXT_LENGTH: usize = 256;
const MAX_POW_SCRIPT_SOURCES: usize = 64;
const NATIVE_POW_MAX_CONCURRENCY: usize = 4;
const MAX_TURNSTILE_DX_CHARS: usize = 2 * 1024 * 1024;
const MAX_TURNSTILE_INSTRUCTIONS: usize = 100_000;
const MAX_TURNSTILE_VALUE_STRING_CHARS: usize = 64 * 1024;
static NATIVE_POW_SEMAPHORE: LazyLock<Arc<Semaphore>> =
    LazyLock::new(|| Arc::new(Semaphore::new(NATIVE_POW_MAX_CONCURRENCY)));

#[cfg(test)]
static NATIVE_POW_ACTIVE_WORKERS: AtomicUsize = AtomicUsize::new(0);

#[cfg(test)]
struct NativePowWorkerGuard;

#[cfg(test)]
impl NativePowWorkerGuard {
    fn for_value(value: Option<&Value>) -> Option<Self> {
        let is_probe = value
            .and_then(|value| value.get("seed"))
            .and_then(Value::as_str)
            .is_some_and(|seed| seed == "permit-release-test-seed");
        if is_probe {
            NATIVE_POW_ACTIVE_WORKERS.fetch_add(1, Ordering::AcqRel);
            Some(Self)
        } else {
            None
        }
    }
}

#[cfg(test)]
impl Drop for NativePowWorkerGuard {
    fn drop(&mut self) {
        NATIVE_POW_ACTIVE_WORKERS.fetch_sub(1, Ordering::AcqRel);
    }
}

#[derive(Clone, Debug)]
struct AuthRecord {
    key_hash: String,
    enabled: bool,
}

#[derive(Clone)]
struct AuthSnapshot {
    generation: u64,
    fingerprint: [u8; 32],
    valid: bool,
    records: Vec<AuthRecord>,
}

#[cfg(test)]
#[derive(Clone)]
struct AuthReloadTestHook {
    pause_before_publish: Arc<AtomicBool>,
    read_complete: Arc<Notify>,
    release_publish: Arc<Notify>,
}

#[derive(Clone)]
struct AuthStore {
    path: Option<Arc<PathBuf>>,
    snapshot: Arc<RwLock<AuthSnapshot>>,
    reload_gate: Arc<Mutex<()>>,
    #[cfg(test)]
    test_hook: Arc<RwLock<Option<AuthReloadTestHook>>>,
}

impl AuthStore {
    fn load(path: Option<&Path>) -> Result<Self, AppInitError> {
        let (records, fingerprint) = if let Some(path) = path {
            read_auth_snapshot(path)?
        } else {
            (Vec::new(), [0; 32])
        };
        Ok(Self {
            path: path.map(|path| Arc::new(path.to_owned())),
            snapshot: Arc::new(RwLock::new(AuthSnapshot {
                generation: 0,
                fingerprint,
                valid: true,
                records,
            })),
            reload_gate: Arc::new(Mutex::new(())),
            #[cfg(test)]
            test_hook: Arc::new(RwLock::new(None)),
        })
    }

    async fn reload(&self) -> bool {
        let Some(path) = self.path.clone() else {
            return true;
        };
        let _reload_guard = self.reload_gate.lock().await;
        let result = tokio::task::spawn_blocking(move || read_auth_snapshot(&path)).await;

        #[cfg(test)]
        self.pause_before_publish().await;

        let mut snapshot = self.snapshot.write().expect("auth snapshot lock");
        match result {
            Ok(Ok((records, fingerprint))) => {
                if !snapshot.valid || snapshot.fingerprint != fingerprint {
                    snapshot.generation = snapshot.generation.saturating_add(1);
                    snapshot.fingerprint = fingerprint;
                    snapshot.records = records;
                    snapshot.valid = true;
                }
                true
            }
            _ => {
                snapshot.generation = snapshot.generation.saturating_add(1);
                snapshot.valid = false;
                snapshot.records.clear();
                false
            }
        }
    }

    #[cfg(test)]
    fn set_test_hook(&self, hook: Option<AuthReloadTestHook>) {
        *self.test_hook.write().expect("auth test hook lock") = hook;
    }

    #[cfg(test)]
    async fn pause_before_publish(&self) {
        let hook = self.test_hook.read().expect("auth test hook lock").clone();
        let Some(hook) = hook else {
            return;
        };
        if hook.pause_before_publish.swap(false, Ordering::SeqCst) {
            hook.read_complete.notify_one();
            hook.release_publish.notified().await;
        }
    }

    fn accepts(&self, token: &str) -> bool {
        let snapshot = self.snapshot.read().expect("auth snapshot lock");
        if !snapshot.valid {
            return false;
        }
        let candidate = Sha256::digest(token.as_bytes());
        let candidate = candidate
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        snapshot.records.iter().any(|record| {
            record.enabled && constant_time_equal(candidate.as_bytes(), record.key_hash.as_bytes())
        })
    }
}

fn read_auth_snapshot(path: &Path) -> Result<(Vec<AuthRecord>, [u8; 32]), AppInitError> {
    let file = fs::File::open(path).map_err(|_| AppInitError::AuthSnapshot)?;
    let mut bytes = Vec::new();
    file.take(MAX_AUTH_KEYS_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| AppInitError::AuthSnapshot)?;
    if bytes.len() as u64 > MAX_AUTH_KEYS_BYTES {
        return Err(AppInitError::AuthSnapshot);
    }
    let fingerprint = Sha256::digest(&bytes).into();
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| AppInitError::AuthSnapshot)?;
    let items = value
        .as_object()
        .and_then(|object| object.get("items"))
        .and_then(Value::as_array)
        .ok_or(AppInitError::AuthSnapshot)?;
    if items.len() > MAX_AUTH_KEYS {
        return Err(AppInitError::AuthSnapshot);
    }
    let mut records = Vec::with_capacity(items.len());
    let mut hashes = std::collections::HashSet::with_capacity(items.len());
    for item in items {
        let object = item.as_object().ok_or(AppInitError::AuthSnapshot)?;
        let key_hash = object
            .get("key_hash")
            .and_then(Value::as_str)
            .ok_or(AppInitError::AuthSnapshot)?
            .trim();
        if key_hash.len() != 64
            || !key_hash.bytes().all(|byte| byte.is_ascii_hexdigit())
            || !hashes.insert(key_hash.to_ascii_lowercase())
        {
            return Err(AppInitError::AuthSnapshot);
        }
        let enabled = object
            .get("enabled")
            .and_then(Value::as_bool)
            .ok_or(AppInitError::AuthSnapshot)?;
        records.push(AuthRecord {
            key_hash: key_hash.to_ascii_lowercase(),
            enabled,
        });
    }
    Ok((records, fingerprint))
}

fn read_account_snapshot(path: &Path) -> Result<(Vec<AccountRecord>, [u8; 32]), AppInitError> {
    let file = fs::File::open(path).map_err(|_| AppInitError::AccountSnapshot)?;
    let mut bytes = Vec::new();
    file.take(MAX_ACCOUNT_SNAPSHOT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| AppInitError::AccountSnapshot)?;
    if bytes.len() as u64 > MAX_ACCOUNT_SNAPSHOT_BYTES {
        return Err(AppInitError::AccountSnapshot);
    }
    let fingerprint = Sha256::digest(&bytes).into();
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| AppInitError::AccountSnapshot)?;
    let items = match &value {
        Value::Array(items) => items,
        Value::Object(object) => object
            .get("items")
            .and_then(Value::as_array)
            .ok_or(AppInitError::AccountSnapshot)?,
        _ => return Err(AppInitError::AccountSnapshot),
    };
    if items.len() > MAX_ACCOUNTS {
        return Err(AppInitError::AccountSnapshot);
    }
    let mut records = Vec::with_capacity(items.len());
    let mut tokens = std::collections::HashSet::with_capacity(items.len());
    for item in items {
        let object = item.as_object().ok_or(AppInitError::AccountSnapshot)?;
        let token = object
            .get("access_token")
            .or_else(|| object.get("token"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|token| !token.is_empty() && token.len() <= MAX_ACCOUNT_TOKEN_LENGTH)
            .ok_or(AppInitError::AccountSnapshot)?
            .to_owned();
        if !tokens.insert(token.clone()) {
            return Err(AppInitError::AccountSnapshot);
        }
        let status = match object.get("status") {
            None => "正常".to_owned(),
            Some(Value::String(value)) => {
                let value = value.trim();
                if !matches!(value, "正常" | "限流" | "异常" | "禁用") {
                    return Err(AppInitError::AccountSnapshot);
                }
                value.to_owned()
            }
            Some(_) => return Err(AppInitError::AccountSnapshot),
        };
        if status.chars().count() > 64 {
            return Err(AppInitError::AccountSnapshot);
        }
        let account_type = normalize_account_type(object.get("type"))?;
        let source_type = normalize_source_type(object.get("source_type"))?;
        let chatgpt_account_id = optional_bounded_text(
            object
                .get("chatgpt_account_id")
                .or_else(|| object.get("account_id")),
            MAX_ACCOUNT_TOKEN_LENGTH,
        )?;
        let models = match object.get("models") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(items)) => {
                let mut models = Vec::new();
                for model in items {
                    let model = model
                        .as_str()
                        .map(str::trim)
                        .filter(|model| {
                            !model.is_empty() && model.chars().count() <= MAX_MODEL_TEXT_LENGTH
                        })
                        .ok_or(AppInitError::AccountSnapshot)?
                        .to_owned();
                    if !models.contains(&model) {
                        models.push(model);
                    }
                }
                models
            }
            _ => return Err(AppInitError::AccountSnapshot),
        };
        records.push(AccountRecord {
            token,
            status,
            source_type,
            chatgpt_account_id,
            account_type,
            models,
        });
    }
    Ok((records, fingerprint))
}

fn normalize_source_type(value: Option<&Value>) -> Result<String, AppInitError> {
    let Some(value) = value else {
        return Ok("web".to_owned());
    };
    let source = value
        .as_str()
        .map(str::trim)
        .filter(|source| !source.is_empty() && source.chars().count() <= 64)
        .ok_or(AppInitError::AccountSnapshot)?
        .to_ascii_lowercase();
    Ok(match source.as_str() {
        // Historical OAuth login records carry an API/Codex token.  Keep the
        // persisted login provenance elsewhere; the routing capability is
        // Codex, not the Web conversation backend.
        "oauth_login" => "codex".to_owned(),
        _ => source,
    })
}

fn optional_bounded_text(
    value: Option<&Value>,
    max_length: usize,
) -> Result<Option<String>, AppInitError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => {
            let value = value.trim();
            if value.is_empty() || value.chars().count() > max_length {
                Err(AppInitError::AccountSnapshot)
            } else {
                Ok(Some(value.to_owned()))
            }
        }
        Some(_) => Err(AppInitError::AccountSnapshot),
    }
}

fn is_semver(value: &str) -> bool {
    let mut parts = value.split('.');
    let valid = (0..3).all(|_| {
        parts.next().is_some_and(|part| {
            !part.is_empty() && part.chars().all(|character| character.is_ascii_digit())
        })
    });
    valid && parts.next().is_none()
}

fn normalize_account_type(value: Option<&Value>) -> Result<String, AppInitError> {
    let Some(value) = value else {
        return Ok("free".to_owned());
    };
    let raw = value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.chars().count() <= 64)
        .ok_or(AppInitError::AccountSnapshot)?
        .to_owned();
    let compact = raw
        .to_ascii_lowercase()
        .chars()
        .filter(|character| !matches!(character, '-' | '_' | ' '))
        .collect::<String>();
    Ok(match compact.as_str() {
        "free" => "free",
        "codex" => "free",
        "plus" => "plus",
        "pro" => "pro",
        "prolite" => "prolite",
        "team" | "business" => "team",
        "enterprise" => "enterprise",
        _ => raw.as_str(),
    }
    .to_owned())
}

#[derive(Clone)]
pub struct AppState {
    config: Arc<AppConfig>,
    auth_store: Arc<AuthStore>,
    account_store: Arc<AccountStore>,
    models: Arc<ModelStore>,
    account_type_catalog: Arc<AccountTypeCatalog>,
    client: Client,
}

impl AppState {
    pub fn new(config: AppConfig) -> Result<Self, AppInitError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(AppInitError::Client)?;
        let auth_store = AuthStore::load(config.auth_keys_path.as_deref())?;
        let account_store = AccountStore::load(config.accounts_path.as_deref())?;
        let models = ModelStore::load(config.models_path.as_deref(), &config.models)?;
        let account_type_catalog = AccountTypeCatalog::new(
            Arc::new(account_store.clone()),
            client.clone(),
            config.upstream_base_url.clone(),
            config.accounts_path.is_some(),
            config.upstream_protocol,
            codex_client_version(),
        );
        Ok(Self {
            config: Arc::new(config),
            auth_store: Arc::new(auth_store),
            account_store: Arc::new(account_store),
            models: Arc::new(models),
            account_type_catalog: Arc::new(account_type_catalog),
            client,
        })
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/health", get(health))
            .route("/v1/models", get(models))
            .route("/v1/chat/completions", post(chat_completions))
            .route("/v1/responses", post(responses))
            .route("/v1/messages", post(messages))
            .layer(DefaultBodyLimit::max(MAX_REQUEST_BODY_BYTES))
            .with_state(self.clone())
    }
}

fn health_payload(state: &AppState) -> Value {
    let accounts = state.account_store.health_summary();
    json!({
        "status": "degraded",
        "healthy": false,
        "version": state.config.version,
        "storage": {
            "backend": "unknown",
            "health": {
                "status": "unhealthy",
                "error": "storage health unavailable",
            },
        },
        "proxy_runtime": {
            "enabled": false,
            "clearance_enabled": false,
        },
        "accounts": {
            "total": accounts.total,
            "cumulative_total": 0,
            "active": accounts.active,
            "limited": accounts.limited,
            "abnormal": accounts.abnormal,
            "disabled": accounts.disabled,
            "total_quota": 0,
            "total_success": 0,
            "total_fail": 0,
            "by_type": {},
        },
    })
}

#[derive(Debug, Deserialize)]
struct HealthQuery {
    format: Option<String>,
}

async fn health(State(state): State<AppState>, Query(query): Query<HealthQuery>) -> Response {
    let _ = state.account_store.reload().await;
    let payload = health_payload(&state);
    if query.format.as_deref() == Some("json") {
        return Json(payload).into_response();
    }
    let status = payload["status"].as_str().unwrap_or("degraded");
    Html(format!(
        "<!DOCTYPE html><html lang=\"zh\"><head><meta charset=\"utf-8\"><title>号池健康监控</title></head><body><h1>号池健康监控</h1><p>status: {status}</p></body></html>"
    ))
    .into_response()
}

const ACCOUNT_TYPE_MODEL_TTL: Duration = Duration::from_secs(300);
const ACCOUNT_TYPE_MODEL_RETRY_BACKOFF: Duration = Duration::from_secs(5);
const ACCOUNT_TYPE_MODEL_REFRESH_DEADLINE: Duration = Duration::from_secs(90);
const ACCOUNT_TYPE_REFRESH_CONCURRENCY: usize = 4;
// Model discovery is published by normalized account type, but each source
// capability owns its own representative and endpoint.  A credential is
// never sent to an endpoint that belongs to another source capability.
#[derive(Clone)]
struct CatalogOwners {
    candidates: Vec<CatalogAccountCandidate>,
    model_sources: HashMap<String, HashSet<String>>,
}

impl CatalogOwners {
    fn first(candidates: &[CatalogAccountCandidate]) -> Self {
        Self {
            candidates: candidates.first().cloned().into_iter().collect(),
            model_sources: HashMap::new(),
        }
    }

    fn with_model_sources(
        candidates: Vec<CatalogAccountCandidate>,
        model_sources: HashMap<String, HashSet<String>>,
    ) -> Self {
        Self {
            candidates,
            model_sources,
        }
    }

    fn is_current(&self, candidates: &[CatalogAccountCandidate]) -> bool {
        !self.candidates.is_empty()
            && self
                .candidates
                .iter()
                .all(|owner| candidates.iter().any(|candidate| candidate == owner))
    }

    fn any_current(&self, candidates: &[CatalogAccountCandidate]) -> bool {
        self.candidates
            .iter()
            .any(|owner| candidates.iter().any(|candidate| candidate == owner))
    }
}

#[derive(Clone)]
struct AccountTypeCatalogEntry {
    models: Arc<Vec<PublicModel>>,
    model_sources: HashMap<String, HashSet<String>>,
    ready: bool,
    tokens: Vec<String>,
    owners: CatalogOwners,
    expires_at: Instant,
    retry_at: Instant,
}

#[derive(Clone)]
struct AccountTypeCatalogSnapshot {
    generation: u64,
    anonymous_models: Arc<Vec<PublicModel>>,
    anonymous_ready: bool,
    anonymous_expires_at: Instant,
    anonymous_retry_at: Instant,
    entries: HashMap<AccountModelGroup, AccountTypeCatalogEntry>,
    live_tokens: HashMap<AccountModelGroup, Vec<String>>,
    live_candidates: HashMap<AccountModelGroup, Vec<CatalogAccountCandidate>>,
}

#[derive(Clone, Eq, Hash, PartialEq)]
enum CatalogFetchKey {
    Anonymous,
    AccountType {
        account_group: AccountModelGroup,
        expected_candidates: Vec<CatalogAccountCandidate>,
    },
}

enum CatalogFetchJob {
    Anonymous,
    AccountType {
        account_group: AccountModelGroup,
        expected_candidates: Vec<CatalogAccountCandidate>,
        candidate_accounts: Vec<CatalogAccountCandidate>,
    },
}

struct CatalogFetchResult {
    job: CatalogFetchJob,
    models: Option<Vec<PublicModel>>,
    owners: Option<CatalogOwners>,
    complete: bool,
}

impl CatalogFetchJob {
    fn key(&self) -> CatalogFetchKey {
        match self {
            Self::Anonymous => CatalogFetchKey::Anonymous,
            Self::AccountType {
                account_group,
                expected_candidates,
                ..
            } => CatalogFetchKey::AccountType {
                account_group: account_group.clone(),
                expected_candidates: expected_candidates.clone(),
            },
        }
    }
}

impl Default for AccountTypeCatalogSnapshot {
    fn default() -> Self {
        let now = Instant::now();
        Self {
            generation: 0,
            anonymous_models: Arc::new(Vec::new()),
            anonymous_ready: false,
            anonymous_expires_at: now,
            anonymous_retry_at: now,
            entries: HashMap::new(),
            live_tokens: HashMap::new(),
            live_candidates: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct AccountTypeCatalog {
    enabled: bool,
    protocol: UpstreamProtocol,
    base_url: Option<String>,
    codex_client_version: Arc<RwLock<Option<String>>>,
    client: Client,
    account_store: Arc<AccountStore>,
    snapshot: Arc<RwLock<AccountTypeCatalogSnapshot>>,
    refresh_gate: Arc<Mutex<()>>,
    refresh_running: Arc<AtomicBool>,
    shutdown_requested: Arc<AtomicBool>,
    refresh_task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    #[cfg(test)]
    shutdown_taken: Arc<Notify>,
    #[cfg(test)]
    live_membership_after_tokens: Arc<Notify>,
    #[cfg(test)]
    live_membership_release: Arc<Notify>,
    #[cfg(test)]
    live_membership_barrier_enabled: Arc<AtomicBool>,
}

impl AccountTypeCatalog {
    fn new(
        account_store: Arc<AccountStore>,
        client: Client,
        base_url: Option<String>,
        enabled: bool,
        protocol: UpstreamProtocol,
        codex_client_version: Option<String>,
    ) -> Self {
        Self {
            enabled,
            protocol,
            base_url,
            codex_client_version: Arc::new(RwLock::new(codex_client_version)),
            client,
            account_store,
            snapshot: Arc::new(RwLock::new(AccountTypeCatalogSnapshot::default())),
            refresh_gate: Arc::new(Mutex::new(())),
            refresh_running: Arc::new(AtomicBool::new(false)),
            shutdown_requested: Arc::new(AtomicBool::new(false)),
            refresh_task: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            shutdown_taken: Arc::new(Notify::new()),
            #[cfg(test)]
            live_membership_after_tokens: Arc::new(Notify::new()),
            #[cfg(test)]
            live_membership_release: Arc::new(Notify::new()),
            #[cfg(test)]
            live_membership_barrier_enabled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn enabled(&self) -> bool {
        self.enabled && self.base_url.is_some()
    }

    fn codex_client_version(&self) -> Option<String> {
        self.codex_client_version
            .read()
            .expect("codex client version lock")
            .clone()
    }

    #[cfg(test)]
    fn set_codex_client_version_for_test(&self, version: Option<String>) {
        *self
            .codex_client_version
            .write()
            .expect("codex client version lock") = version;
    }

    fn clear_live_tokens(&self) {
        let mut snapshot = self.snapshot.write().expect("account type catalog lock");
        snapshot.live_tokens.clear();
        snapshot.live_candidates.clear();
    }

    async fn refresh_for_public(&self) {
        self.refresh_with_cold_wait(None).await;
    }

    async fn refresh_for_model(&self, model: &str) {
        self.refresh_with_cold_wait(Some(model)).await;
    }

    async fn refresh_with_cold_wait(&self, requested_model: Option<&str>) {
        if !self.enabled() || self.shutdown_requested.load(Ordering::Acquire) {
            return;
        }
        let Some((_, candidate_groups)) = self.account_store.active_type_candidates().await else {
            self.clear_live_tokens();
            return;
        };
        let groups = candidate_groups
            .iter()
            .map(|(group, candidates)| {
                (
                    group.clone(),
                    candidates
                        .iter()
                        .map(|candidate| candidate.token.clone())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        {
            let mut snapshot = self.snapshot.write().expect("account type catalog lock");
            snapshot.live_tokens = groups;
            snapshot.live_candidates = candidate_groups.clone();
        }
        #[cfg(test)]
        if self
            .live_membership_barrier_enabled
            .swap(false, Ordering::AcqRel)
        {
            self.live_membership_after_tokens.notify_one();
            self.live_membership_release.notified().await;
        }
        let has_ready_snapshot = {
            let snapshot = self.snapshot.read().expect("account type catalog lock");
            match requested_model.filter(|model| *model != "auto") {
                Some(model) => {
                    let anonymous_ready = snapshot.anonymous_ready
                        && snapshot
                            .anonymous_models
                            .iter()
                            .any(|candidate| candidate.id == model);
                    anonymous_ready
                        || candidate_groups.iter().any(|(account_group, candidates)| {
                            snapshot.entries.get(account_group).is_some_and(|entry| {
                                entry.owners.is_current(candidates)
                                    && entry.ready
                                    && entry.models.iter().any(|candidate| candidate.id == model)
                            })
                        })
                }
                None => {
                    snapshot.anonymous_ready
                        || candidate_groups.iter().any(|(account_group, candidates)| {
                            snapshot.entries.get(account_group).is_some_and(|entry| {
                                entry.ready && entry.owners.is_current(candidates)
                            })
                        })
                }
            }
        };
        if requested_model.is_none() || has_ready_snapshot {
            let mut refresh_task = self.refresh_task.lock().await;
            if self.shutdown_requested.load(Ordering::Acquire) {
                return;
            }
            if self
                .refresh_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                let worker = self.clone();
                let task = tokio::spawn(async move {
                    worker.refresh_inner().await;
                    worker.refresh_running.store(false, Ordering::Release);
                });
                *refresh_task = Some(task);
            }
            return;
        }
        self.refresh_inner().await;
    }

    async fn shutdown(&self) {
        self.shutdown_requested.store(true, Ordering::Release);
        let task = self.refresh_task.lock().await.take();
        #[cfg(test)]
        self.shutdown_taken.notify_one();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.refresh_running.store(false, Ordering::Release);
    }

    async fn refresh_inner(&self) {
        self.refresh_inner_with_budget(ACCOUNT_TYPE_MODEL_REFRESH_DEADLINE)
            .await;
    }

    async fn refresh_inner_with_budget(&self, budget: Duration) {
        if !self.enabled() || self.shutdown_requested.load(Ordering::Acquire) {
            return;
        }
        let deadline = Instant::now() + budget;
        let _refresh_guard = self.refresh_gate.lock().await;
        let Some((_, candidate_groups)) = self.account_store.active_type_candidates().await else {
            self.clear_live_tokens();
            return;
        };
        let groups = candidate_groups
            .iter()
            .map(|(group, candidates)| {
                (
                    group.clone(),
                    candidates
                        .iter()
                        .map(|candidate| candidate.token.clone())
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<HashMap<_, _>>();
        {
            let mut snapshot = self.snapshot.write().expect("account type catalog lock");
            snapshot.live_tokens = groups.clone();
            snapshot.live_candidates = candidate_groups.clone();
        }

        let now = Instant::now();
        let (anonymous_pending, pending) = {
            let snapshot = self.snapshot.read().expect("account type catalog lock");
            (
                (!snapshot.anonymous_ready || now >= snapshot.anonymous_expires_at)
                    && now >= snapshot.anonymous_retry_at,
                candidate_groups
                    .iter()
                    .filter_map(|(account_group, candidates)| {
                        let needs_refresh =
                            snapshot.entries.get(account_group).is_none_or(|entry| {
                                let owner_invalid = !entry.owners.is_current(candidates);
                                owner_invalid
                                    || ((!entry.ready || now >= entry.expires_at)
                                        && now >= entry.retry_at)
                            });
                        needs_refresh.then(|| CatalogFetchJob::AccountType {
                            account_group: account_group.clone(),
                            expected_candidates: candidates.clone(),
                            candidate_accounts: candidates.clone(),
                        })
                    })
                    .collect::<Vec<_>>(),
            )
        };

        let mut pending_jobs = VecDeque::new();
        if anonymous_pending {
            pending_jobs.push_back(CatalogFetchJob::Anonymous);
        }
        pending_jobs.extend(pending);

        let mut active = FuturesUnordered::new();
        let mut in_flight = HashSet::new();
        for _ in 0..ACCOUNT_TYPE_REFRESH_CONCURRENCY {
            let Some(job) = pending_jobs.pop_front() else {
                break;
            };
            in_flight.insert(job.key());
            active.push(self.fetch_catalog_job(job, deadline));
        }

        while !active.is_empty() {
            let result =
                tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), active.next())
                    .await
                    .ok()
                    .flatten();
            let Some(CatalogFetchResult {
                job,
                models,
                owners,
                complete,
            }) = result
            else {
                break;
            };
            in_flight.remove(&job.key());

            let current = self.account_store.active_type_candidates().await;
            let mut snapshot = self.snapshot.write().expect("account type catalog lock");
            let now = Instant::now();
            match job {
                CatalogFetchJob::Anonymous => {
                    snapshot.generation = snapshot.generation.saturating_add(1);
                    match models {
                        Some(models) => {
                            snapshot.anonymous_models = Arc::new(models);
                            snapshot.anonymous_ready = true;
                            snapshot.anonymous_expires_at = now + ACCOUNT_TYPE_MODEL_TTL;
                            snapshot.anonymous_retry_at = now;
                        }
                        None => {
                            snapshot.anonymous_retry_at = now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF;
                        }
                    }
                }
                CatalogFetchJob::AccountType {
                    account_group,
                    expected_candidates,
                    ..
                } => {
                    let owners_is_current = current.as_ref().and_then(|(_, current_groups)| {
                        let candidates = current_groups.get(&account_group)?;
                        let owners = owners.as_ref()?;
                        let valid = if complete {
                            owners.is_current(candidates)
                        } else {
                            owners.any_current(candidates)
                        };
                        valid.then(|| owners.clone())
                    });
                    if let Some(owners) = owners_is_current {
                        snapshot.generation = snapshot.generation.saturating_add(1);
                        let current_tokens = current
                            .as_ref()
                            .and_then(|(_, groups)| groups.get(&account_group))
                            .unwrap_or(&expected_candidates)
                            .iter()
                            .map(|candidate| candidate.token.clone())
                            .collect();
                        match models {
                            Some(models) if complete => {
                                snapshot.entries.insert(
                                    account_group,
                                    AccountTypeCatalogEntry {
                                        models: Arc::new(models),
                                        model_sources: owners.model_sources.clone(),
                                        ready: true,
                                        tokens: current_tokens,
                                        owners,
                                        expires_at: now + ACCOUNT_TYPE_MODEL_TTL,
                                        retry_at: now,
                                    },
                                );
                            }
                            Some(models) => {
                                if let Some(entry) = snapshot.entries.get_mut(&account_group) {
                                    entry.tokens = current_tokens;
                                    let model_sources = owners.model_sources.clone();
                                    entry.owners = owners;
                                    entry.model_sources = model_sources;
                                    entry.retry_at = now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF;
                                    if entry.models.is_empty() {
                                        entry.models = Arc::new(models);
                                        entry.ready = false;
                                    }
                                } else {
                                    snapshot.entries.insert(
                                        account_group,
                                        AccountTypeCatalogEntry {
                                            models: Arc::new(models),
                                            model_sources: owners.model_sources.clone(),
                                            ready: false,
                                            tokens: current_tokens,
                                            owners,
                                            expires_at: now,
                                            retry_at: now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF,
                                        },
                                    );
                                }
                            }
                            None => {
                                if let Some(entry) = snapshot.entries.get_mut(&account_group) {
                                    entry.tokens = current_tokens;
                                    let model_sources = owners.model_sources.clone();
                                    entry.owners = owners;
                                    entry.model_sources = model_sources;
                                    entry.retry_at = now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF;
                                } else {
                                    snapshot.entries.insert(
                                        account_group,
                                        AccountTypeCatalogEntry {
                                            models: Arc::new(Vec::new()),
                                            model_sources: owners.model_sources.clone(),
                                            ready: false,
                                            tokens: current_tokens,
                                            owners,
                                            expires_at: now,
                                            retry_at: now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF,
                                        },
                                    );
                                }
                            }
                        }
                    } else if let Some((_, current_groups)) = current.as_ref() {
                        if let Some(current_candidates) = current_groups.get(&account_group) {
                            let current_tokens = current_candidates
                                .iter()
                                .map(|candidate| candidate.token.clone())
                                .collect();
                            let retry_at = if owners.is_some() {
                                now
                            } else {
                                now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF
                            };
                            let previous_owner_is_current = snapshot
                                .entries
                                .get(&account_group)
                                .is_some_and(|entry| entry.owners.any_current(current_candidates));
                            if previous_owner_is_current {
                                if let Some(entry) = snapshot.entries.get_mut(&account_group) {
                                    entry.tokens = current_tokens;
                                    entry.retry_at = now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF;
                                }
                            } else {
                                snapshot.entries.insert(
                                    account_group,
                                    AccountTypeCatalogEntry {
                                        models: Arc::new(Vec::new()),
                                        model_sources: HashMap::new(),
                                        ready: false,
                                        tokens: current_tokens,
                                        owners: CatalogOwners::first(current_candidates),
                                        expires_at: now,
                                        retry_at,
                                    },
                                );
                            }
                        } else {
                            snapshot.entries.remove(&account_group);
                        }
                    }
                }
            }

            if Instant::now() < deadline
                && let Some(job) = pending_jobs.pop_front()
            {
                in_flight.insert(job.key());
                active.push(self.fetch_catalog_job(job, deadline));
            }
        }

        if !in_flight.is_empty() || !pending_jobs.is_empty() {
            let now = Instant::now();
            let mut snapshot = self.snapshot.write().expect("account type catalog lock");
            let mut unfinished = in_flight;
            unfinished.extend(pending_jobs.into_iter().map(|job| job.key()));
            for key in unfinished {
                match key {
                    CatalogFetchKey::Anonymous => {
                        snapshot.anonymous_retry_at = now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF;
                    }
                    CatalogFetchKey::AccountType {
                        account_group,
                        expected_candidates,
                    } => {
                        let expected_tokens = expected_candidates
                            .iter()
                            .map(|candidate| candidate.token.clone())
                            .collect();
                        if let Some(entry) = snapshot.entries.get_mut(&account_group) {
                            entry.tokens = expected_tokens;
                            entry.owners = CatalogOwners::first(&expected_candidates);
                            entry.retry_at = now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF;
                        } else {
                            snapshot.entries.insert(
                                account_group,
                                AccountTypeCatalogEntry {
                                    models: Arc::new(Vec::new()),
                                    model_sources: HashMap::new(),
                                    ready: false,
                                    tokens: expected_tokens,
                                    owners: CatalogOwners::first(&expected_candidates),
                                    expires_at: now,
                                    retry_at: now + ACCOUNT_TYPE_MODEL_RETRY_BACKOFF,
                                },
                            );
                        }
                    }
                }
            }
        }

        match self.account_store.active_type_candidates().await {
            Some((_, current_groups)) => {
                let current_tokens = current_groups
                    .iter()
                    .map(|(group, candidates)| {
                        (
                            group.clone(),
                            candidates
                                .iter()
                                .map(|candidate| candidate.token.clone())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect();
                let mut snapshot = self.snapshot.write().expect("account type catalog lock");
                snapshot.live_tokens = current_tokens;
                snapshot.live_candidates = current_groups;
            }
            None => self.clear_live_tokens(),
        }
    }

    async fn fetch_catalog_job(
        &self,
        job: CatalogFetchJob,
        deadline: Instant,
    ) -> CatalogFetchResult {
        let (models, owners, complete) = match &job {
            CatalogFetchJob::Anonymous => {
                let models = self.fetch_anonymous_models(deadline).await;
                let complete = models.is_some();
                (models, None, complete)
            }
            CatalogFetchJob::AccountType {
                account_group,
                candidate_accounts,
                ..
            } => {
                match self
                    .fetch_account_type_models(account_group, candidate_accounts, deadline)
                    .await
                {
                    Some((models, owners, complete)) => (Some(models), Some(owners), complete),
                    None => (None, None, false),
                }
            }
        };
        CatalogFetchResult {
            job,
            models,
            owners,
            complete,
        }
    }

    async fn fetch_account_type_models(
        &self,
        account_group: &AccountModelGroup,
        candidate_accounts: &[CatalogAccountCandidate],
        deadline: Instant,
    ) -> Option<(Vec<PublicModel>, CatalogOwners, bool)> {
        if self.protocol == UpstreamProtocol::ChatGpt {
            let mut source_groups = Vec::<(String, Vec<CatalogAccountCandidate>)>::new();
            for candidate in candidate_accounts {
                if let Some((_, candidates)) = source_groups
                    .iter_mut()
                    .find(|(source, _)| source == &candidate.source_type)
                {
                    candidates.push(candidate.clone());
                } else {
                    source_groups.push((candidate.source_type.clone(), vec![candidate.clone()]));
                }
            }

            // Each source capability gets one independent representative
            // search.  In particular, a Web credential is never probed via
            // the Codex endpoint and vice versa.  The futures are still
            // concurrent so one slow capability cannot starve another.
            let mut source_fetches = FuturesUnordered::new();
            let mut expected_source_count = 0usize;
            let mut complete = !source_groups.is_empty();
            for (source_type, candidates) in source_groups {
                let source_supported = matches!(
                    source_type.as_str(),
                    "web" | "password" | "password-oauth" | "codex"
                );
                if !source_supported {
                    complete = false;
                    continue;
                }
                expected_source_count += 1;
                let account_group = account_group.clone();
                source_fetches.push(async move {
                    match source_type.as_str() {
                        "web" | "password" | "password-oauth" => {
                            let mut result = None;
                            for candidate in &candidates {
                                if let Some(models) = self
                                    .fetch_native_models(
                                        &candidate.token,
                                        Some(&account_group),
                                        candidate.chatgpt_account_id.as_deref(),
                                        deadline,
                                    )
                                    .await
                                {
                                    result = Some((models, candidate.clone()));
                                    break;
                                }
                            }
                            result
                        }
                        "codex" => {
                            let mut result = None;
                            for candidate in &candidates {
                                if let Some(models) = self
                                    .fetch_codex_models(
                                        &candidate.token,
                                        Some(&account_group),
                                        candidate.chatgpt_account_id.as_deref(),
                                        deadline,
                                    )
                                    .await
                                {
                                    result = Some((models, candidate.clone()));
                                    break;
                                }
                            }
                            result
                        }
                        _ => None,
                    }
                });
            }

            let mut models = Vec::new();
            let mut owners = Vec::new();
            let mut model_sources = HashMap::<String, HashSet<String>>::new();
            while let Some(result) = source_fetches.next().await {
                match result {
                    Some((source_models, owner)) => {
                        for model in &source_models {
                            model_sources
                                .entry(model.id.clone())
                                .or_default()
                                .insert(owner.source_type.clone());
                        }
                        models = Self::merge_catalog_models(models, source_models);
                        owners.push(owner);
                    }
                    None => complete = false,
                }
            }
            complete &= owners.len() == expected_source_count;
            if !models.is_empty() {
                return Some((
                    models,
                    CatalogOwners::with_model_sources(owners, model_sources),
                    complete,
                ));
            }
            return None;
        }
        for candidate in candidate_accounts {
            let base_url = self.base_url.as_deref()?;
            let url = format!("{}/v1/models", base_url.trim_end_matches('/'));
            let request = self
                .client
                .get(url)
                .header(header::AUTHORIZATION, format!("Bearer {}", candidate.token));
            let response = match tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                request.send(),
            )
            .await
            {
                Ok(Ok(response)) => response,
                _ => continue,
            };
            if !response.status().is_success() {
                continue;
            }
            let body = match tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                bounded_response_body(response),
            )
            .await
            {
                Ok(Ok(body)) => body,
                _ => continue,
            };
            let Ok(value) = serde_json::from_slice::<Value>(&body) else {
                continue;
            };
            let Some(_items) = value.get("data").and_then(Value::as_array) else {
                continue;
            };
            return project_remote_model_list(
                &value,
                "data",
                false,
                Some(account_group),
                false,
                false,
            )
            .map(|models| {
                let model_sources = models
                    .iter()
                    .map(|model| {
                        (
                            model.id.clone(),
                            HashSet::from([candidate.source_type.clone()]),
                        )
                    })
                    .collect();
                (
                    models,
                    CatalogOwners::with_model_sources(vec![candidate.clone()], model_sources),
                    true,
                )
            });
        }
        None
    }

    fn merge_catalog_models(
        mut first: Vec<PublicModel>,
        second: Vec<PublicModel>,
    ) -> Vec<PublicModel> {
        let mut merged = HashMap::<String, PublicModel>::new();
        for model in first.drain(..).chain(second) {
            if let Some(existing) = merged.get_mut(&model.id) {
                existing.allow_anonymous |= model.allow_anonymous;
                for account_type in model.supported_account_types {
                    if !existing.supported_account_types.contains(&account_type) {
                        existing.supported_account_types.push(account_type);
                    }
                }
                for effort in model.supported_reasoning_efforts {
                    if !existing.supported_reasoning_efforts.contains(&effort) {
                        existing.supported_reasoning_efforts.push(effort);
                    }
                }
            } else {
                merged.insert(model.id.clone(), model);
            }
        }
        let mut models = merged.into_values().collect::<Vec<_>>();
        models.sort_by(|left, right| left.id.cmp(&right.id));
        models
    }

    async fn fetch_native_models(
        &self,
        token: &str,
        account_type: Option<&str>,
        chatgpt_account_id: Option<&str>,
        deadline: Instant,
    ) -> Option<Vec<PublicModel>> {
        let base_url = self.base_url.as_deref()?;
        let authenticated = !token.is_empty();
        let context = NativeRequestContext::new();
        if authenticated {
            tokio::time::timeout_at(
                tokio::time::Instant::from_std(deadline),
                native_bootstrap(&self.client, base_url, token, &context),
            )
            .await
            .ok()?
            .ok()?;
        }
        let url = if authenticated {
            format!(
                "{}/backend-api/models?history_and_training_disabled=false",
                base_url.trim_end_matches('/')
            )
        } else {
            format!(
                "{}/backend-anon/models?iim=false&is_gizmo=false",
                base_url.trim_end_matches('/')
            )
        };
        let mut request = native_browser_headers(self.client.get(url), &context)
            .header(
                "X-OpenAI-Target-Path",
                if authenticated {
                    "/backend-api/models"
                } else {
                    "/backend-anon/models"
                },
            )
            .header(
                "X-OpenAI-Target-Route",
                if authenticated {
                    "/backend-api/models"
                } else {
                    "/backend-anon/models"
                },
            );
        if authenticated {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
            if let Some(account_id) = chatgpt_account_id.filter(|value| !value.is_empty()) {
                request = request.header("ChatGPT-Account-ID", account_id);
            }
        }
        let response =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), request.send())
                .await
                .ok()?
                .ok()?;
        if !response.status().is_success() || upstream_declares_oversize(&response) {
            return None;
        }
        let body = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            bounded_response_body(response),
        )
        .await
        .ok()?
        .ok()?;
        let value = serde_json::from_slice::<Value>(&body).ok()?;
        project_remote_model_list(&value, "models", !authenticated, account_type, true, false)
    }

    async fn fetch_codex_models(
        &self,
        token: &str,
        account_type: Option<&str>,
        chatgpt_account_id: Option<&str>,
        deadline: Instant,
    ) -> Option<Vec<PublicModel>> {
        let base_url = self.base_url.as_deref()?;
        let version = self.codex_client_version()?;
        let url = format!(
            "{}/backend-api/codex/models?client_version={version}",
            base_url.trim_end_matches('/')
        );
        let mut request = self
            .client
            .get(url)
            .header(header::AUTHORIZATION, format!("Bearer {token}"))
            .header(header::ACCEPT, "application/json")
            .header("originator", "codex_cli_rs")
            .header("User-Agent", format!("codex_cli_rs/{version}"))
            .header("version", version);
        if let Some(account_id) = chatgpt_account_id.filter(|value| !value.is_empty()) {
            request = request.header("ChatGPT-Account-ID", account_id);
        }
        let response =
            tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), request.send())
                .await
                .ok()?
                .ok()?;
        if !response.status().is_success() || upstream_declares_oversize(&response) {
            return None;
        }
        let body = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            bounded_response_body(response),
        )
        .await
        .ok()?
        .ok()?;
        let value = serde_json::from_slice::<Value>(&body).ok()?;
        project_remote_model_list(&value, "models", false, account_type, true, true)
    }

    async fn fetch_anonymous_models(&self, deadline: Instant) -> Option<Vec<PublicModel>> {
        if self.protocol == UpstreamProtocol::ChatGpt {
            return self.fetch_native_models("", None, None, deadline).await;
        }
        let base_url = self.base_url.as_deref()?;
        let url = format!("{}/v1/models", base_url.trim_end_matches('/'));
        let response = match tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.client.get(url).send(),
        )
        .await
        {
            Ok(Ok(response)) => response,
            _ => return None,
        };
        if !response.status().is_success() {
            return None;
        }
        let body = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            bounded_response_body(response),
        )
        .await
        .ok()?
        .ok()?;
        let value = serde_json::from_slice::<Value>(&body).ok()?;
        let items = value.get("data").and_then(Value::as_array)?;
        let mut seen = HashSet::new();
        let mut models = Vec::new();
        for item in items {
            let Some(mut model) = ModelCatalog::project(item) else {
                continue;
            };
            if !seen.insert(model.id.clone()) {
                continue;
            }
            model.allow_anonymous = true;
            model.supported_account_types.clear();
            models.push(model);
        }
        Some(models)
    }

    fn public_models(&self, anonymous: Arc<Vec<PublicModel>>) -> Arc<Vec<PublicModel>> {
        let snapshot = self.snapshot.read().expect("account type catalog lock");
        let mut models = anonymous.as_ref().clone();
        for model in &mut models {
            model.allow_anonymous = false;
            model.supported_account_types.clear();
        }
        let mut indexes = models
            .iter()
            .enumerate()
            .map(|(index, model)| (model.id.clone(), index))
            .collect::<HashMap<_, _>>();
        if snapshot.anonymous_ready {
            for model in snapshot.anonymous_models.iter() {
                if let Some(index) = indexes.get(&model.id).copied() {
                    models[index].allow_anonymous = true;
                } else {
                    indexes.insert(model.id.clone(), models.len());
                    models.push(model.clone());
                }
            }
        }
        for (account_group, entry) in &snapshot.entries {
            if !entry.ready
                || !snapshot.live_tokens.contains_key(account_group)
                || !snapshot
                    .live_candidates
                    .get(account_group)
                    .is_some_and(|candidates| entry.owners.is_current(candidates))
            {
                continue;
            }
            let account_type = account_group;
            for model in entry.models.iter() {
                if let Some(index) = indexes.get(&model.id).copied() {
                    let supported = &mut models[index].supported_account_types;
                    let public_account_type = account_type.to_ascii_lowercase();
                    if !supported.contains(&public_account_type) {
                        supported.push(public_account_type);
                        supported.sort();
                    }
                } else {
                    let mut model = model.clone();
                    model.supported_account_types = vec![account_type.to_ascii_lowercase()];
                    indexes.insert(model.id.clone(), models.len());
                    models.push(model);
                }
            }
        }
        models.sort_by(|left, right| left.id.cmp(&right.id));
        Arc::new(models)
    }

    fn supported_types_for(&self, model: &str) -> Option<HashSet<AccountModelGroup>> {
        if !self.enabled() {
            return None;
        }
        let model_is_auto = model == "auto";
        let snapshot = self.snapshot.read().expect("account type catalog lock");
        let mut types = HashSet::new();
        for (account_group, entry) in &snapshot.entries {
            if entry.ready
                && snapshot.live_tokens.contains_key(account_group)
                && snapshot
                    .live_candidates
                    .get(account_group)
                    .is_some_and(|candidates| entry.owners.is_current(candidates))
                && (!entry.models.is_empty()
                    && (model_is_auto
                        || entry.models.iter().any(|candidate| candidate.id == model)))
            {
                types.insert(account_group.clone());
            }
        }
        Some(types)
    }

    fn source_types_for(
        &self,
        model: &str,
        allowed_groups: Option<&HashSet<AccountModelGroup>>,
    ) -> HashSet<String> {
        let snapshot = self.snapshot.read().expect("account type catalog lock");
        let mut sources = HashSet::new();
        for (group, entry) in &snapshot.entries {
            if allowed_groups.is_some_and(|groups| !groups.contains(group)) {
                continue;
            }
            if !entry.ready {
                continue;
            }
            if let Some(model_sources) = entry.model_sources.get(model) {
                sources.extend(model_sources.iter().cloned());
            }
        }
        sources
    }

    fn allows_anonymous_model(&self, model: &str) -> bool {
        let snapshot = self.snapshot.read().expect("account type catalog lock");
        snapshot.anonymous_ready
            && ((model == "auto" && !snapshot.anonymous_models.is_empty())
                || snapshot
                    .anonymous_models
                    .iter()
                    .any(|candidate| candidate.id == model))
    }

    fn model_catalog_pending(&self, model: &str) -> bool {
        let snapshot = self.snapshot.read().expect("account type catalog lock");
        if model == "auto" {
            if snapshot.anonymous_ready && !snapshot.anonymous_models.is_empty() {
                return false;
            }
            if snapshot.entries.iter().any(|(account_group, entry)| {
                snapshot.live_tokens.contains_key(account_group)
                    && entry.ready
                    && !entry.models.is_empty()
                    && snapshot
                        .live_candidates
                        .get(account_group)
                        .is_some_and(|candidates| entry.owners.is_current(candidates))
            }) {
                return false;
            }
            return snapshot.live_tokens.keys().any(|account_group| {
                !snapshot.entries.get(account_group).is_some_and(|entry| {
                    entry.ready
                        && !entry.models.is_empty()
                        && snapshot
                            .live_candidates
                            .get(account_group)
                            .is_some_and(|candidates| entry.owners.is_current(candidates))
                })
            }) || !snapshot.anonymous_ready;
        }
        if snapshot.anonymous_ready
            && snapshot
                .anonymous_models
                .iter()
                .any(|candidate| candidate.id == model)
        {
            return false;
        }
        if snapshot.entries.iter().any(|(account_group, entry)| {
            snapshot.live_tokens.contains_key(account_group)
                && snapshot
                    .live_candidates
                    .get(account_group)
                    .is_some_and(|candidates| entry.owners.is_current(candidates))
                && entry.ready
                && entry.models.iter().any(|candidate| candidate.id == model)
        }) {
            return false;
        }
        !snapshot.anonymous_ready
            || snapshot.live_tokens.keys().any(|account_group| {
                !snapshot.entries.get(account_group).is_some_and(|entry| {
                    entry.ready
                        && snapshot
                            .live_candidates
                            .get(account_group)
                            .is_some_and(|candidates| entry.owners.is_current(candidates))
                })
            })
    }

    fn live_tokens_for(&self, model: &str) -> Option<HashSet<String>> {
        let account_types = self.supported_types_for(model)?;
        let snapshot = self.snapshot.read().expect("account type catalog lock");
        let mut tokens = HashSet::new();
        for account_group in account_types {
            if let Some(live_tokens) = snapshot.live_tokens.get(&account_group) {
                tokens.extend(live_tokens.iter().cloned());
            }
        }
        Some(tokens)
    }
}

fn bounded_text(value: Option<&Value>, max_length: usize) -> Option<String> {
    let text = value?.as_str()?.trim();
    if text.is_empty() || text.chars().count() > max_length {
        None
    } else {
        Some(text.to_owned())
    }
}

fn normalized_reasoning_efforts(object: &Map<String, Value>) -> Vec<String> {
    const KEYS: &[&str] = &[
        "supported_reasoning_efforts",
        "supported_reasoning_levels",
        "supported_thinking_efforts",
        "reasoning_efforts",
        "reasoning_levels",
        "thinking_efforts",
    ];
    const ENTRY_KEYS: &[&str] = &["reasoning_effort", "thinking_effort", "effort", "value"];
    let mut containers = vec![object];
    if let Some(capabilities) = object.get("capabilities").and_then(Value::as_object) {
        containers.push(capabilities);
    }

    let Some(raw) = containers
        .iter()
        .find_map(|container| KEYS.iter().find_map(|key| container.get(*key)))
    else {
        return Vec::new();
    };
    let Some(items) = raw.as_array() else {
        return Vec::new();
    };
    let mut result = Vec::new();
    for item in items {
        let value = item
            .as_object()
            .and_then(|entry| {
                ENTRY_KEYS
                    .iter()
                    .find_map(|key| entry.get(*key).filter(|value| !value.is_null()))
            })
            .unwrap_or(item);
        let Some(text) = bounded_text(Some(value), 64) else {
            continue;
        };
        if !result.contains(&text.to_lowercase()) {
            result.push(text.to_lowercase());
        }
    }
    result
}

fn normalized_list(value: Option<&Value>, max_length: usize) -> Vec<String> {
    let mut result = Vec::new();
    let mut seen = std::collections::HashSet::new();
    if let Some(Value::Array(items)) = value {
        for item in items {
            let Some(text) = bounded_text(Some(item), max_length) else {
                continue;
            };
            let text = text.to_lowercase();
            if seen.insert(text.clone()) {
                result.push(text);
            }
        }
    }
    result
}

fn parse_created(value: Option<&Value>) -> i64 {
    let candidate = match value {
        Some(Value::Number(number)) => number.as_i64(),
        Some(Value::String(text)) => {
            let text = text.trim();
            if text.is_empty()
                || text.len() > 19
                || !text.is_ascii()
                || !text.bytes().all(|byte| byte.is_ascii_digit())
            {
                None
            } else {
                text.parse::<i64>().ok()
            }
        }
        _ => None,
    };
    candidate
        .filter(|value| (0..=MAX_MODEL_CREATED).contains(value))
        .unwrap_or(0)
}

async fn models(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    authenticated(&headers, &state).await?;
    if !state.models.reload().await {
        return Err(ApiError::unavailable());
    }
    state.account_type_catalog.refresh_for_public().await;
    let models = state
        .account_type_catalog
        .public_models(state.models.current());
    Ok(Json(json!({ "object": "list", "data": models.as_ref() })))
}

async fn authenticated(headers: &HeaderMap, state: &AppState) -> Result<(), ApiError> {
    let Some(value) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return Err(ApiError::unauthorized());
    };
    let Some((scheme, raw_token)) = value.split_once(' ') else {
        return Err(ApiError::unauthorized());
    };
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(ApiError::unauthorized());
    }
    let token = raw_token.trim();
    if token.is_empty() {
        return Err(ApiError::unauthorized());
    }
    if state
        .config
        .auth_key
        .as_deref()
        .is_some_and(|expected| constant_time_equal(token.as_bytes(), expected.trim().as_bytes()))
    {
        return Ok(());
    }
    if !state.auth_store.reload().await {
        return Err(ApiError::unauthorized());
    }
    if state.auth_store.accepts(token) {
        Ok(())
    } else {
        Err(ApiError::unauthorized())
    }
}

fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}

static NATIVE_MESSAGE_ID: AtomicUsize = AtomicUsize::new(1);

fn native_message_id() -> String {
    let id = NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed);
    format!("00000000-0000-4000-8000-{id:012x}")
}

fn native_completion_id() -> String {
    format!("chatcmpl-{}", native_message_id())
}

fn native_created() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

pub(crate) fn sse_delimiter(buffer: &[u8]) -> Option<(usize, usize)> {
    let lf = buffer.windows(2).position(|window| window == b"\n\n");
    let crlf = buffer.windows(4).position(|window| window == b"\r\n\r\n");
    match (lf, crlf) {
        (Some(lf), Some(crlf)) if lf <= crlf => Some((lf, 2)),
        (Some(_), Some(crlf)) => Some((crlf, 4)),
        (Some(lf), None) => Some((lf, 2)),
        (None, Some(crlf)) => Some((crlf, 4)),
        (None, None) => None,
    }
}

struct NativeRequirements {
    token: String,
    so_token: Option<String>,
    proof_token: Option<String>,
    turnstile_token: Option<String>,
}

fn native_requirements_token(
    user_agent: &str,
    resources: &NativePowResources,
) -> Result<String, ApiError> {
    let config = native_pow_config(user_agent, resources);
    let payload = serde_json::to_vec(&config).map_err(|_| ApiError::upstream())?;
    Ok(format!(
        "gAAAAAC{}",
        base64::engine::general_purpose::STANDARD.encode(payload)
    ))
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
    if value.is_empty() || !value.len().is_multiple_of(2) || value.len() > MAX_POW_TEXT_LENGTH {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = (pair[0] as char).to_digit(16)? as u8;
        let low = (pair[1] as char).to_digit(16)? as u8;
        bytes.push((high << 4) | low);
    }
    Some(bytes)
}

fn native_proof_token_sync_for_config(
    value: Option<&Value>,
    config: &[Value],
    cancel: &AtomicBool,
    deadline: Instant,
) -> Result<String, ApiError> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let Some(object) = value.as_object() else {
        return Err(ApiError::upstream());
    };
    if object.get("required") != Some(&Value::Bool(true)) {
        return Ok(String::new());
    }
    let seed = object
        .get("seed")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.chars().count() <= MAX_POW_TEXT_LENGTH)
        .ok_or_else(ApiError::upstream)?;
    let difficulty = object
        .get("difficulty")
        .and_then(Value::as_str)
        .filter(|value| value.len() <= MAX_POW_TEXT_LENGTH)
        .and_then(decode_hex)
        .ok_or_else(ApiError::upstream)?;
    if difficulty.len() > 64 {
        return Err(ApiError::upstream());
    }
    let prefix = serde_json::to_string(&config[..3])
        .map_err(|_| ApiError::upstream())?
        .trim_end_matches(']')
        .to_owned()
        + ",";
    let middle_json = serde_json::to_string(&config[4..9]).map_err(|_| ApiError::upstream())?;
    let middle = format!(",{},", &middle_json[1..middle_json.len() - 1]);
    let suffix_json = serde_json::to_string(&config[10..]).map_err(|_| ApiError::upstream())?;
    let suffix = format!(",{}", &suffix_json[1..]);
    for counter in 0..MAX_POW_ITERATIONS {
        if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(ApiError::upstream());
        }
        let candidate = format!("{prefix}{counter}{middle}{}{suffix}", counter / 2);
        let encoded = base64::engine::general_purpose::STANDARD.encode(candidate.as_bytes());
        let mut hasher = Sha3_512::new();
        hasher.update(seed.as_bytes());
        hasher.update(encoded.as_bytes());
        let digest = hasher.finalize();
        if digest[..difficulty.len()].cmp(&difficulty) != std::cmp::Ordering::Greater {
            return Ok(format!("gAAAAAB{encoded}"));
        }
    }
    Err(ApiError::upstream())
}

fn native_proof_token_sync(
    value: Option<&Value>,
    user_agent: &str,
    resources: &NativePowResources,
    cancel: &AtomicBool,
    deadline: Instant,
) -> Result<String, ApiError> {
    let config = native_pow_config(user_agent, resources);
    native_proof_token_sync_for_config(value, &config, cancel, deadline)
}

struct NativePowCancelGuard(Arc<AtomicBool>);

impl Drop for NativePowCancelGuard {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

async fn native_proof_token(
    value: Option<&Value>,
    user_agent: &str,
    resources: &NativePowResources,
    deadline: Instant,
) -> Result<String, ApiError> {
    if !native_optional_challenge_required(value)? {
        return Ok(String::new());
    }
    let permit = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        NATIVE_POW_SEMAPHORE.clone().acquire_owned(),
    )
    .await
    .map_err(|_| ApiError::upstream())?
    .map_err(|_| ApiError::upstream())?;
    let value = value.cloned();
    let user_agent = user_agent.to_owned();
    let resources = resources.clone();
    let cancel = Arc::new(AtomicBool::new(false));
    let worker_cancel = cancel.clone();
    let _cancel_guard = NativePowCancelGuard(cancel.clone());
    let mut worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        #[cfg(test)]
        let _worker_guard = NativePowWorkerGuard::for_value(value.as_ref());
        native_proof_token_sync(
            value.as_ref(),
            &user_agent,
            &resources,
            &worker_cancel,
            deadline,
        )
    });
    let result =
        tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), &mut worker).await;
    match result {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(ApiError::upstream()),
        Err(_) => {
            cancel.store(true, Ordering::Release);
            let _ = worker.await;
            Err(ApiError::upstream())
        }
    }
}

fn native_turnstile_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => Some("undefined".to_owned()),
        Value::Bool(value) => Some(if *value {
            "True".to_owned()
        } else {
            "False".to_owned()
        }),
        Value::Number(value) => Some(native_turnstile_json_number(value)),
        Value::String(value) => Some(match value.as_str() {
            "window.Math" => "[object Math]".to_owned(),
            "window.Reflect" => "[object Reflect]".to_owned(),
            "window.performance" => "[object Performance]".to_owned(),
            "window.localStorage" => "[object Storage]".to_owned(),
            "window.Object" => "function Object() { [native code] }".to_owned(),
            "window.Reflect.set" => "function set() { [native code] }".to_owned(),
            "window.performance.now" => "function () { [native code] }".to_owned(),
            "window.Object.create" => "function create() { [native code] }".to_owned(),
            "window.Object.keys" => "function keys() { [native code] }".to_owned(),
            "window.Math.random" => "function random() { [native code] }".to_owned(),
            value => value.to_owned(),
        }),
        Value::Array(values) if values.iter().all(Value::is_string) => Some(
            values
                .iter()
                .map(|value| value.as_str().unwrap_or_default())
                .collect::<Vec<_>>()
                .join(","),
        ),
        Value::Array(_) | Value::Object(_) => native_turnstile_python_repr(value),
    }
}

fn native_turnstile_json_quote(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\b"),
            '\u{0c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character <= '\u{1f}' => {
                output.push_str(&format!("\\u{:04x}", character as u32));
            }
            character if character.is_ascii() => output.push(character),
            character => {
                let mut units = [0_u16; 2];
                for unit in character.encode_utf16(&mut units) {
                    output.push_str(&format!("\\u{:04x}", unit));
                }
            }
        }
    }
    output.push('"');
    output
}

fn native_turnstile_python_quote(value: &str) -> String {
    let mut output = String::with_capacity(value.len() + 2);
    let quote = if value.contains('\'') && !value.contains('"') {
        '"'
    } else {
        '\''
    };
    output.push(quote);
    for character in value.chars() {
        match character {
            character if character == quote => {
                output.push('\\');
                output.push(character);
            }
            '\\' => output.push_str("\\\\"),
            '\u{08}' => output.push_str("\\x08"),
            '\u{0c}' => output.push_str("\\x0c"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '\u{2028}' => output.push_str("\\u2028"),
            '\u{2029}' => output.push_str("\\u2029"),
            character if native_turnstile_python_needs_escape(character) => {
                let code = character as u32;
                if code <= 0xff {
                    output.push_str(&format!("\\x{code:02x}"));
                } else if code <= 0xffff {
                    output.push_str(&format!("\\u{code:04x}"));
                } else {
                    output.push_str(&format!("\\U{code:08x}"));
                }
            }
            character => output.push(character),
        }
    }
    output.push(quote);
    output
}

fn native_turnstile_python_needs_escape(character: char) -> bool {
    if character == ' ' {
        return false;
    }
    matches!(
        character.general_category(),
        GeneralCategory::Control
            | GeneralCategory::Format
            | GeneralCategory::Surrogate
            | GeneralCategory::Unassigned
            | GeneralCategory::PrivateUse
            | GeneralCategory::SpaceSeparator
            | GeneralCategory::LineSeparator
            | GeneralCategory::ParagraphSeparator
    )
}

fn native_turnstile_python_repr(value: &Value) -> Option<String> {
    match value {
        Value::Null => Some("None".to_owned()),
        Value::Bool(value) => Some(if *value { "True" } else { "False" }.to_owned()),
        Value::Number(value) => Some(native_turnstile_json_number(value)),
        Value::String(value) => Some(native_turnstile_python_quote(value)),
        Value::Array(values) => {
            let values = values
                .iter()
                .map(native_turnstile_python_repr)
                .collect::<Option<Vec<_>>>()?;
            Some(format!("[{}]", values.join(", ")))
        }
        Value::Object(values) => {
            let fields = values
                .iter()
                .map(|(key, value)| {
                    Some(format!(
                        "{}: {}",
                        native_turnstile_python_quote(key),
                        native_turnstile_python_repr(value)?
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(format!("{{{}}}", fields.join(", ")))
        }
    }
}

fn native_turnstile_json_number(value: &serde_json::Number) -> String {
    let text = value.to_string();
    let Some(exponent_index) = text.find(['e', 'E']) else {
        return text;
    };
    let (mantissa, exponent) = text.split_at(exponent_index);
    let exponent = &exponent[1..];
    let (sign, digits) = match exponent.as_bytes().first() {
        Some(b'+' | b'-') => (&exponent[..1], &exponent[1..]),
        Some(_) => ("", exponent),
        None => return text,
    };
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return text;
    }
    let padded = if digits.len() == 1 {
        format!("0{digits}")
    } else {
        digits.to_owned()
    };
    format!("{mantissa}e{sign}{padded}")
}

fn native_turnstile_json_dumps(value: &Value) -> Option<String> {
    match value {
        Value::Null => Some("null".to_owned()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(native_turnstile_json_number(value)),
        Value::String(value) => Some(native_turnstile_json_quote(value)),
        Value::Array(values) => {
            let values = values
                .iter()
                .map(native_turnstile_json_dumps)
                .collect::<Option<Vec<_>>>()?;
            Some(format!("[{}]", values.join(", ")))
        }
        Value::Object(values) => {
            let fields = values
                .iter()
                .map(|(key, value)| {
                    Some(format!(
                        "{}: {}",
                        native_turnstile_json_quote(key),
                        native_turnstile_json_dumps(value)?
                    ))
                })
                .collect::<Option<Vec<_>>>()?;
            Some(format!("{{{}}}", fields.join(", ")))
        }
    }
}

fn native_turnstile_key(value: &Value) -> Option<i64> {
    value.as_i64().filter(|value| *value >= 0)
}

fn native_turnstile_validate_value(value: &Value, depth: usize) -> bool {
    if depth > 32 {
        return false;
    }
    match value {
        Value::String(value) => value.chars().count() <= MAX_TURNSTILE_VALUE_STRING_CHARS,
        Value::Array(values) => {
            values.len() <= MAX_TURNSTILE_INSTRUCTIONS
                && values
                    .iter()
                    .all(|value| native_turnstile_validate_value(value, depth + 1))
        }
        Value::Object(values) => values
            .values()
            .all(|value| native_turnstile_validate_value(value, depth + 1)),
        _ => true,
    }
}

fn native_turnstile_token_sync(
    value: Option<&Value>,
    source_p: &str,
    cancel: &AtomicBool,
    deadline: Instant,
) -> Result<String, ApiError> {
    let Some(value) = value else {
        return Ok(String::new());
    };
    let Some(object) = value.as_object() else {
        return Err(ApiError::upstream());
    };
    if object.get("required") != Some(&Value::Bool(true)) {
        return Ok(String::new());
    }
    let dx = object
        .get("dx")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.chars().count() <= MAX_TURNSTILE_DX_CHARS)
        .ok_or_else(ApiError::upstream)?;
    if source_p.is_empty() || source_p.chars().count() > MAX_ACCOUNT_TOKEN_LENGTH {
        return Err(ApiError::upstream());
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(dx)
        .map_err(|_| ApiError::upstream())?;
    let mut unmasked = Vec::with_capacity(decoded.len());
    let key = source_p.as_bytes();
    if key.is_empty() {
        return Err(ApiError::upstream());
    }
    for (index, byte) in decoded.into_iter().enumerate() {
        unmasked.push(byte ^ key[index % key.len()]);
    }
    let program_text = String::from_utf8(unmasked).map_err(|_| ApiError::upstream())?;
    let program: Value = serde_json::from_str(&program_text).map_err(|_| ApiError::upstream())?;
    let instructions = program.as_array().ok_or_else(ApiError::upstream)?;
    if instructions.len() > MAX_TURNSTILE_INSTRUCTIONS
        || !native_turnstile_validate_value(&program, 0)
    {
        return Err(ApiError::upstream());
    }

    let mut process: HashMap<i64, Value> = HashMap::new();
    let function_slots = [1_i64, 2, 3, 5, 6, 7, 8, 14, 15, 17, 18, 19, 20, 21, 23, 24]
        .into_iter()
        .collect::<HashSet<_>>();
    process.insert(9, Value::Array(instructions.to_vec()));
    process.insert(10, Value::String("window".to_owned()));
    process.insert(16, Value::String(source_p.to_owned()));
    let mut ordered_maps: HashMap<i64, Vec<(String, Value)>> = HashMap::new();
    let mut ordered_map_ids: HashMap<i64, u64> = HashMap::new();
    let mut next_ordered_map_id = 0_u64;
    let mut result = None;
    for instruction in instructions {
        if cancel.load(Ordering::Acquire) || Instant::now() >= deadline {
            return Err(ApiError::upstream());
        }
        let tokens = instruction.as_array().ok_or_else(ApiError::upstream)?;
        let opcode = tokens
            .first()
            .and_then(Value::as_u64)
            .filter(|value| (1..=24).contains(value))
            .ok_or_else(ApiError::upstream)?;
        match opcode {
            2 => {
                if tokens.len() != 3 {
                    return Err(ApiError::upstream());
                }
                let key = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                ordered_maps.remove(&key);
                ordered_map_ids.remove(&key);
                process.insert(key, tokens[2].clone());
            }
            3 => {
                if tokens.len() != 2 {
                    return Err(ApiError::upstream());
                }
                // Python func_3 receives the raw instruction argument. It
                // does not dereference numeric arguments through process_map;
                // malformed callback shapes therefore fail closed here.
                let text = tokens[1].as_str().ok_or_else(ApiError::upstream)?;
                result = Some(base64::engine::general_purpose::STANDARD.encode(text.as_bytes()));
            }
            1 => {
                if tokens.len() != 3 {
                    return Err(ApiError::upstream());
                }
                let left_key = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let right_key = native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                let left = process
                    .get(&left_key)
                    .and_then(native_turnstile_string)
                    .ok_or_else(ApiError::upstream)?;
                let right = process
                    .get(&right_key)
                    .and_then(native_turnstile_string)
                    .ok_or_else(ApiError::upstream)?;
                let right_chars = right.chars().collect::<Vec<_>>();
                if right_chars.is_empty() {
                    return Err(ApiError::upstream());
                }
                let value = left
                    .chars()
                    .enumerate()
                    .map(|(index, ch)| {
                        char::from_u32(ch as u32 ^ right_chars[index % right_chars.len()] as u32)
                    })
                    .collect::<Option<String>>()
                    .ok_or_else(ApiError::upstream)?;
                process.insert(left_key, Value::String(value));
            }
            5 => {
                if tokens.len() != 3 {
                    return Err(ApiError::upstream());
                }
                let key = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let current = process.get(&key).cloned().ok_or_else(ApiError::upstream)?;
                let incoming_key =
                    native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                let incoming = process
                    .get(&incoming_key)
                    .cloned()
                    .ok_or_else(ApiError::upstream)?;
                let merged = match (current, incoming) {
                    (Value::Array(mut current), incoming) => {
                        current.push(incoming);
                        Value::Array(current)
                    }
                    (current, incoming)
                        if matches!(current, Value::String(_))
                            || matches!(incoming, Value::String(_))
                            || matches!(&current, Value::Number(number) if number.is_f64())
                            || matches!(&incoming, Value::Number(number) if number.is_f64()) =>
                    {
                        Value::String(
                            native_turnstile_concat_string(&current)
                                .ok_or_else(ApiError::upstream)?
                                + &native_turnstile_concat_string(&incoming)
                                    .ok_or_else(ApiError::upstream)?,
                        )
                    }
                    _ => Value::String("NaN".to_owned()),
                };
                process.insert(key, merged);
            }
            6 | 24 => {
                if tokens.len() != 4 {
                    return Err(ApiError::upstream());
                }
                let target = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let left = process
                    .get(&native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?)
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .ok_or_else(ApiError::upstream)?;
                let right = process
                    .get(&native_turnstile_key(&tokens[3]).ok_or_else(ApiError::upstream)?)
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .ok_or_else(ApiError::upstream)?;
                let joined = if opcode == 6 && left == "window.document" && right == "location" {
                    "https://chatgpt.com/".to_owned()
                } else {
                    format!("{left}.{right}")
                };
                process.insert(target, Value::String(joined));
            }
            8 => {
                if tokens.len() != 3 {
                    return Err(ApiError::upstream());
                }
                let target = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let source = native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                process.insert(
                    target,
                    process
                        .get(&source)
                        .cloned()
                        .ok_or_else(ApiError::upstream)?,
                );
                if let Some(identity) = ordered_map_ids.get(&source).copied() {
                    ordered_map_ids.insert(target, identity);
                } else {
                    ordered_map_ids.remove(&target);
                }
            }
            14 => {
                if tokens.len() != 3 {
                    return Err(ApiError::upstream());
                }
                let target = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let source = native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                let text = process
                    .get(&source)
                    .and_then(native_turnstile_string)
                    .ok_or_else(ApiError::upstream)?;
                process.insert(
                    target,
                    serde_json::from_str(&text).map_err(|_| ApiError::upstream())?,
                );
            }
            15 => {
                if tokens.len() != 3 {
                    return Err(ApiError::upstream());
                }
                let target = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let source = native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                // Python's OrderedMap has no JSON encoder support. Its
                // json.dumps call raises TypeError and the Python VM skips
                // that instruction. Rust does not implement that continuation
                // model, so reject this unproven extension instead of
                // manufacturing a different token.
                if ordered_maps.contains_key(&source) {
                    return Err(ApiError::upstream());
                }
                let value = process.get(&source).ok_or_else(ApiError::upstream)?;
                let value = native_turnstile_json_dumps(value).ok_or_else(ApiError::upstream)?;
                process.insert(target, Value::String(value));
            }
            18 | 19 => {
                if tokens.len() != 2 {
                    return Err(ApiError::upstream());
                }
                let key = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let value = process
                    .get(&key)
                    .and_then(native_turnstile_string)
                    .ok_or_else(ApiError::upstream)?;
                let converted = if opcode == 18 {
                    String::from_utf8(
                        base64::engine::general_purpose::STANDARD
                            .decode(value)
                            .map_err(|_| ApiError::upstream())?,
                    )
                    .map_err(|_| ApiError::upstream())?
                } else {
                    base64::engine::general_purpose::STANDARD.encode(value.as_bytes())
                };
                process.insert(key, Value::String(converted));
            }
            17 => {
                if tokens.len() < 3 {
                    return Err(ApiError::upstream());
                }
                let target = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let function_key =
                    native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                let function = process
                    .get(&function_key)
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::upstream)?;
                let value = match function {
                    "window.Object.create" => {
                        ordered_maps.insert(target, Vec::new());
                        let identity = next_ordered_map_id;
                        next_ordered_map_id = next_ordered_map_id.saturating_add(1);
                        ordered_map_ids.insert(target, identity);
                        Value::Object(Map::new())
                    }
                    "window.performance.now" => return Err(ApiError::upstream()),
                    // Python uses process-global randomness here. A fixed
                    // value would manufacture a token that is not Python
                    // parity, so the isolated canary rejects this branch.
                    "window.Math.random" => return Err(ApiError::upstream()),
                    "window.Object.keys" => {
                        let argument_key = tokens
                            .get(3)
                            .and_then(native_turnstile_key)
                            .ok_or_else(ApiError::upstream)?;
                        if process.get(&argument_key).and_then(Value::as_str)
                            != Some("window.localStorage")
                        {
                            return Err(ApiError::upstream());
                        }
                        json!([
                            "STATSIG_LOCAL_STORAGE_INTERNAL_STORE_V4",
                            "STATSIG_LOCAL_STORAGE_STABLE_ID",
                            "client-correlated-secret",
                            "oai/apps/capExpiresAt",
                            "oai-did",
                            "STATSIG_LOCAL_STORAGE_LOGGING_REQUEST",
                            "UiState.isNavigationCollapsed.1"
                        ])
                    }
                    _ => return Err(ApiError::upstream()),
                };
                process.insert(target, value);
            }
            7 => {
                if tokens.len() < 3 {
                    return Err(ApiError::upstream());
                }
                let function_key =
                    native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let function = process
                    .get(&function_key)
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::upstream)?;
                if function != "window.Reflect.set" || tokens.len() != 5 {
                    return Err(ApiError::upstream());
                }
                let object_key = native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                let key_key = native_turnstile_key(&tokens[3]).ok_or_else(ApiError::upstream)?;
                let value_key = native_turnstile_key(&tokens[4]).ok_or_else(ApiError::upstream)?;
                let key = process
                    .get(&key_key)
                    .and_then(native_turnstile_string)
                    .ok_or_else(ApiError::upstream)?;
                let value = process
                    .get(&value_key)
                    .cloned()
                    .ok_or_else(ApiError::upstream)?;
                if !ordered_maps.contains_key(&object_key) {
                    // Python's Reflect.set path calls OrderedMap.add(); a
                    // normal JSON object raises and the instruction is
                    // skipped. Do not mutate it and manufacture a token.
                    return Err(ApiError::upstream());
                }
                let object = process
                    .get_mut(&object_key)
                    .ok_or_else(ApiError::upstream)?;
                object
                    .as_object_mut()
                    .ok_or_else(ApiError::upstream)?
                    .insert(key.clone(), value.clone());
                if let Some(values) = ordered_maps.get_mut(&object_key) {
                    if let Some((_, old_value)) =
                        values.iter_mut().find(|(old_key, _)| old_key == &key)
                    {
                        *old_value = value;
                    } else {
                        values.push((key, value));
                    }
                }
            }
            20 => {
                if tokens.len() < 4 {
                    return Err(ApiError::upstream());
                }
                let left = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let right = native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                let equal = match (process.get(&left), process.get(&right)) {
                    (Some(left_value), Some(right_value)) => {
                        match (ordered_map_ids.get(&left), ordered_map_ids.get(&right)) {
                            (Some(left), Some(right)) => left == right,
                            (Some(_), None) | (None, Some(_)) => false,
                            (None, None) => left_value == right_value,
                        }
                    }
                    _ => false,
                };
                if equal {
                    let function =
                        native_turnstile_key(&tokens[3]).ok_or_else(ApiError::upstream)?;
                    if !function_slots.contains(&function) {
                        return Err(ApiError::upstream());
                    }
                    let args = tokens[4..]
                        .iter()
                        .map(|key| {
                            let key = native_turnstile_key(key).ok_or_else(ApiError::upstream)?;
                            process.get(&key).cloned().ok_or_else(ApiError::upstream)
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    match function {
                        2 if args.len() == 2 => {
                            let target =
                                native_turnstile_key(&args[0]).ok_or_else(ApiError::upstream)?;
                            ordered_maps.remove(&target);
                            ordered_map_ids.remove(&target);
                            process.insert(target, args[1].clone());
                            if let Some(value_source) = tokens.get(5).and_then(native_turnstile_key)
                                && let Some(identity) = ordered_map_ids.get(&value_source).copied()
                            {
                                ordered_map_ids.insert(target, identity);
                            }
                        }
                        3 if args.len() == 1 => {
                            let text = args[0].as_str().ok_or_else(ApiError::upstream)?;
                            result = Some(
                                base64::engine::general_purpose::STANDARD.encode(text.as_bytes()),
                            );
                        }
                        21 => {}
                        _ => return Err(ApiError::upstream()),
                    }
                }
            }
            23 => {
                if tokens.len() < 3 {
                    return Err(ApiError::upstream());
                }
                let guard = native_turnstile_key(&tokens[1]).ok_or_else(ApiError::upstream)?;
                let function = native_turnstile_key(&tokens[2]).ok_or_else(ApiError::upstream)?;
                if process.get(&guard).is_some_and(|value| !value.is_null()) {
                    if !function_slots.contains(&function) {
                        return Err(ApiError::upstream());
                    }
                    // Python func_23 invokes the target with raw instruction
                    // arguments. This intentionally differs from opcode 20,
                    // whose function arguments are dereferenced through
                    // process_map.
                    let args = tokens[3..].to_vec();
                    match function {
                        2 if args.len() == 2 => {
                            let target =
                                native_turnstile_key(&args[0]).ok_or_else(ApiError::upstream)?;
                            ordered_maps.remove(&target);
                            ordered_map_ids.remove(&target);
                            process.insert(target, args[1].clone());
                        }
                        3 if args.len() == 1 => {
                            let text = args[0].as_str().ok_or_else(ApiError::upstream)?;
                            result = Some(
                                base64::engine::general_purpose::STANDARD.encode(text.as_bytes()),
                            );
                        }
                        21 => {}
                        _ => return Err(ApiError::upstream()),
                    }
                }
            }
            21 => {}
            _ => return Err(ApiError::upstream()),
        }
    }
    result
        .filter(|value| !value.is_empty())
        .ok_or_else(ApiError::upstream)
}

async fn native_turnstile_token(
    value: Option<&Value>,
    source_p: &str,
    deadline: Instant,
) -> Result<String, ApiError> {
    if !native_optional_challenge_required(value)? {
        return Ok(String::new());
    }
    let permit = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        NATIVE_POW_SEMAPHORE.clone().acquire_owned(),
    )
    .await
    .map_err(|_| ApiError::upstream())?
    .map_err(|_| ApiError::upstream())?;
    let value = value.cloned();
    let source_p = source_p.to_owned();
    let cancel = Arc::new(AtomicBool::new(false));
    let worker_cancel = cancel.clone();
    let _cancel_guard = NativePowCancelGuard(cancel.clone());
    let mut worker = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        native_turnstile_token_sync(value.as_ref(), &source_p, &worker_cancel, deadline)
    });
    match tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), &mut worker).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(ApiError::upstream()),
        Err(_) => {
            cancel.store(true, Ordering::Release);
            let _ = worker.await;
            Err(ApiError::upstream())
        }
    }
}

fn native_challenge_required(value: &Value, key: &str) -> Result<bool, ApiError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Object(object)) => Ok(object.get("required") == Some(&Value::Bool(true))),
        Some(_) => Err(ApiError::upstream()),
    }
}

fn native_optional_challenge_required(value: Option<&Value>) -> Result<bool, ApiError> {
    match value {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Object(object)) => Ok(object.get("required") == Some(&Value::Bool(true))),
        Some(_) => Err(ApiError::upstream()),
    }
}

fn native_stage_retryable(status: StatusCode, allow_rate_limit: bool) -> bool {
    status.is_server_error() || (allow_rate_limit && status == StatusCode::TOO_MANY_REQUESTS)
}

#[cfg(test)]
async fn native_bootstrap_with_timeout(
    client: &Client,
    base_url: &str,
    token: &str,
    timeout: Duration,
) -> Result<NativePowResources, (ApiError, bool)> {
    let context = NativeRequestContext::new();
    native_bootstrap_with_timeout_context(client, base_url, token, timeout, &context).await
}

async fn native_bootstrap_with_timeout_context(
    client: &Client,
    base_url: &str,
    token: &str,
    timeout: Duration,
    context: &NativeRequestContext,
) -> Result<NativePowResources, (ApiError, bool)> {
    let resources = tokio::time::timeout(timeout, async {
        let mut request = native_browser_headers(
            client.get(format!("{}/", base_url.trim_end_matches('/'))),
            context,
        )
        .header(header::ACCEPT, "text/html,application/xhtml+xml");
        if !token.is_empty() {
            request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        let response = request
            .send()
            .await
            .map_err(|_| (ApiError::upstream(), false))?;
        let status = response.status();
        if !status.is_success() {
            return Err((ApiError::upstream(), native_stage_retryable(status, false)));
        }
        let body = bounded_response_body(response)
            .await
            .map_err(|error| (error, false))?;
        Ok::<NativePowResources, (ApiError, bool)>(parse_native_pow_resources(&body))
    })
    .await
    .map_err(|_| (ApiError::upstream(), false))??;
    Ok(resources)
}

async fn native_bootstrap(
    client: &Client,
    base_url: &str,
    token: &str,
    context: &NativeRequestContext,
) -> Result<NativePowResources, (ApiError, bool)> {
    native_bootstrap_with_timeout_context(client, base_url, token, NATIVE_UPSTREAM_TIMEOUT, context)
        .await
}

#[cfg(test)]
async fn native_chat_requirements_with_timeout(
    client: &Client,
    base_url: &str,
    token: &str,
    timeout: Duration,
) -> Result<NativeRequirements, (ApiError, bool)> {
    native_chat_requirements_with_resources(
        client,
        base_url,
        token,
        &NativePowResources::default(),
        timeout,
    )
    .await
}

#[cfg(test)]
async fn native_chat_requirements_with_resources(
    client: &Client,
    base_url: &str,
    token: &str,
    resources: &NativePowResources,
    timeout: Duration,
) -> Result<NativeRequirements, (ApiError, bool)> {
    native_chat_requirements_with_resources_for_route(
        client, base_url, token, resources, timeout, true,
    )
    .await
}

#[cfg(test)]
async fn native_chat_requirements_with_resources_for_route(
    client: &Client,
    base_url: &str,
    token: &str,
    resources: &NativePowResources,
    timeout: Duration,
    authenticated: bool,
) -> Result<NativeRequirements, (ApiError, bool)> {
    let context = NativeRequestContext::new();
    native_chat_requirements_with_resources_for_route_context(
        client,
        base_url,
        token,
        resources,
        timeout,
        authenticated,
        &context,
    )
    .await
}

async fn native_chat_requirements_with_resources_for_route_context(
    client: &Client,
    base_url: &str,
    token: &str,
    resources: &NativePowResources,
    timeout: Duration,
    authenticated: bool,
    context: &NativeRequestContext,
) -> Result<NativeRequirements, (ApiError, bool)> {
    let deadline = Instant::now() + timeout;
    let base_url = base_url.trim_end_matches('/');
    let route_base = if authenticated {
        "/backend-api"
    } else {
        "/backend-anon"
    };
    let p_token =
        native_requirements_token(NATIVE_USER_AGENT, resources).map_err(|error| (error, false))?;
    let prepare_path = format!("{route_base}/sentinel/chat-requirements/prepare");
    let mut prepare_request =
        native_browser_headers(client.post(format!("{base_url}{prepare_path}")), context)
            .header("X-OpenAI-Target-Path", &prepare_path)
            .header("X-OpenAI-Target-Route", &prepare_path)
            .json(&json!({"p": p_token}));
    if authenticated {
        prepare_request = prepare_request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let prepare = prepare_request.send();
    let prepare_body = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
        let prepare = prepare.await.map_err(|_| (ApiError::upstream(), false))?;
        let status = prepare.status();
        if !status.is_success() {
            return Err((ApiError::upstream(), native_stage_retryable(status, true)));
        }
        bounded_response_body(prepare)
            .await
            .map_err(|error| (error, false))
    })
    .await
    .map_err(|_| (ApiError::upstream(), false))??;
    let prepare_value: Value =
        serde_json::from_slice(&prepare_body).map_err(|_| (ApiError::upstream(), false))?;
    if native_challenge_required(&prepare_value, "arkose").map_err(|error| (error, false))? {
        return Err((ApiError::upstream(), false));
    }
    let prepare_token = prepare_value
        .get("prepare_token")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| (ApiError::upstream(), false))?;
    let proof_token = native_proof_token(
        prepare_value.get("proofofwork"),
        NATIVE_USER_AGENT,
        resources,
        deadline,
    )
    .await
    .map_err(|error| (error, false))?;
    let turnstile_token =
        native_turnstile_token(prepare_value.get("turnstile"), &p_token, deadline)
            .await
            .map_err(|error| (error, false))?;
    let finalize_path = format!("{route_base}/sentinel/chat-requirements/finalize");
    let mut finalize_request =
        native_browser_headers(client.post(format!("{base_url}{finalize_path}")), context)
            .header("X-OpenAI-Target-Path", &finalize_path)
            .header("X-OpenAI-Target-Route", &finalize_path)
            .json(&json!({
                "prepare_token": prepare_token,
                "proof_token": proof_token.clone(),
                "turnstile_token": turnstile_token.clone(),
            }));
    if authenticated {
        finalize_request =
            finalize_request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    let finalize = finalize_request.send();
    let finalize_body = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async {
        let finalize = finalize.await.map_err(|_| (ApiError::upstream(), false))?;
        let status = finalize.status();
        if !status.is_success() {
            return Err((ApiError::upstream(), native_stage_retryable(status, false)));
        }
        bounded_response_body(finalize)
            .await
            .map_err(|error| (error, false))
    })
    .await
    .map_err(|_| (ApiError::upstream(), false))??;
    let finalize_value: Value =
        serde_json::from_slice(&finalize_body).map_err(|_| (ApiError::upstream(), false))?;
    if native_challenge_required(&finalize_value, "arkose").map_err(|error| (error, false))? {
        return Err((ApiError::upstream(), false));
    }
    if finalize_value
        .get("proof_token")
        .is_some_and(|value| !value.is_null())
        || finalize_value
            .get("turnstile_token")
            .is_some_and(|value| !value.is_null())
    {
        return Err((ApiError::upstream(), false));
    }
    let token = finalize_value
        .get("token")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| (ApiError::upstream(), false))?;
    let optional_text = |key: &str| -> Result<Option<String>, (ApiError, bool)> {
        match finalize_value.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
            _ => Err((ApiError::upstream(), false)),
        }
    };
    Ok(NativeRequirements {
        token,
        so_token: optional_text("so_token")?,
        proof_token: (!proof_token.is_empty()).then_some(proof_token),
        turnstile_token: (!turnstile_token.is_empty()).then_some(turnstile_token),
    })
}

fn native_stream_response(
    response: reqwest::Response,
    lease: Option<AccountLease>,
    model: String,
    include_usage: bool,
    prompt_tokens: usize,
) -> Response {
    type UpstreamStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;
    let input: UpstreamStream = Box::pin(response.bytes_stream());
    let completion_id = native_completion_id();
    let created = native_created();
    let stream = stream::unfold(
        (
            input,
            VecDeque::<Vec<u8>>::new(),
            Vec::new(),
            String::new(),
            0usize,
            false,
            false,
            Some(lease),
            completion_id,
            model,
            created,
            include_usage,
            prompt_tokens,
        ),
        |(
            mut input,
            mut pending,
            mut buffer,
            mut text,
            mut total,
            mut finished,
            mut role_sent,
            mut lease,
            completion_id,
            model,
            created,
            include_usage,
            prompt_tokens,
        )| async move {
            loop {
                if let Some(frame) = pending.pop_front() {
                    return Some((
                        Ok(Bytes::from(frame)),
                        (
                            input,
                            pending,
                            buffer,
                            text,
                            total,
                            finished,
                            role_sent,
                            lease,
                            completion_id,
                            model,
                            created,
                            include_usage,
                            prompt_tokens,
                        ),
                    ));
                }
                if finished {
                    drop(lease.take());
                    return None;
                }
                let next = tokio::time::timeout(NATIVE_UPSTREAM_TIMEOUT, input.next()).await;
                let chunk = match next {
                    Ok(Some(Ok(chunk))) => chunk,
                    Ok(Some(Err(_))) | Err(_) => {
                        drop(lease.take());
                        return Some((
                            Err(io::Error::other("upstream stream failed")),
                            (
                                input,
                                pending,
                                buffer,
                                text,
                                total,
                                true,
                                role_sent,
                                lease,
                                completion_id,
                                model,
                                created,
                                include_usage,
                                prompt_tokens,
                            ),
                        ));
                    }
                    Ok(None) => {
                        drop(lease.take());
                        return Some((
                            Err(io::Error::other(
                                "upstream stream ended without terminal event",
                            )),
                            (
                                input,
                                pending,
                                buffer,
                                text,
                                total,
                                true,
                                role_sent,
                                lease,
                                completion_id,
                                model,
                                created,
                                include_usage,
                                prompt_tokens,
                            ),
                        ));
                    }
                };
                total = match total.checked_add(chunk.len()) {
                    Some(total) if total <= MAX_UPSTREAM_BODY_BYTES => total,
                    _ => {
                        drop(lease.take());
                        return Some((
                            Err(io::Error::other("upstream body exceeded limit")),
                            (
                                input,
                                pending,
                                buffer,
                                text,
                                total,
                                true,
                                role_sent,
                                lease,
                                completion_id,
                                model,
                                created,
                                include_usage,
                                prompt_tokens,
                            ),
                        ));
                    }
                };
                buffer.extend_from_slice(&chunk);
                while let Some((position, delimiter_length)) = sse_delimiter(&buffer) {
                    let event = buffer.drain(..position).collect::<Vec<_>>();
                    buffer.drain(..delimiter_length);
                    match native_frame(
                        &event,
                        &mut text,
                        &completion_id,
                        &model,
                        created,
                        include_usage,
                    ) {
                        Ok(Some(frame)) => {
                            if frame == b"data: [DONE]\n\n" {
                                finished = true;
                                if !role_sent {
                                    pending.push_back(native_role_frame(
                                        &completion_id,
                                        &model,
                                        created,
                                        include_usage,
                                    ));
                                    role_sent = true;
                                }
                                pending.push_back(native_finish_frame(
                                    &completion_id,
                                    &model,
                                    created,
                                    include_usage,
                                ));
                                if include_usage {
                                    match native_usage_for_prompt_tokens(
                                        &model,
                                        prompt_tokens,
                                        &text,
                                    ) {
                                        Ok(usage) => pending.push_back(native_usage_frame(
                                            &completion_id,
                                            &model,
                                            created,
                                            usage,
                                        )),
                                        Err(_) => {
                                            drop(lease.take());
                                            return Some((
                                                Err(io::Error::other(
                                                    "upstream usage calculation failed",
                                                )),
                                                (
                                                    input,
                                                    pending,
                                                    buffer,
                                                    text,
                                                    total,
                                                    true,
                                                    role_sent,
                                                    lease,
                                                    completion_id,
                                                    model,
                                                    created,
                                                    include_usage,
                                                    prompt_tokens,
                                                ),
                                            ));
                                        }
                                    }
                                }
                            } else if !role_sent {
                                pending.push_back(native_role_frame(
                                    &completion_id,
                                    &model,
                                    created,
                                    include_usage,
                                ));
                                role_sent = true;
                            }
                            pending.push_back(frame);
                            if finished {
                                break;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            drop(lease.take());
                            return Some((
                                Err(error),
                                (
                                    input,
                                    pending,
                                    buffer,
                                    text,
                                    total,
                                    true,
                                    role_sent,
                                    lease,
                                    completion_id,
                                    model,
                                    created,
                                    include_usage,
                                    prompt_tokens,
                                ),
                            ));
                        }
                    }
                }
                if finished {
                    continue;
                }
            }
        },
    );
    let mut output = Response::new(Body::from_stream(stream));
    output.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    output
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    output
}

fn native_turnstile_concat_string(value: &Value) -> Option<String> {
    match value {
        Value::Array(values) if !values.iter().all(Value::is_string) => {
            native_turnstile_python_repr(value)
        }
        Value::Object(_) => native_turnstile_python_repr(value),
        _ => native_turnstile_string(value),
    }
}

fn native_codex_stream_response(
    response: reqwest::Response,
    lease: Option<AccountLease>,
    model: String,
    include_usage: bool,
    prompt_tokens: usize,
) -> Response {
    type UpstreamStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;
    let input: UpstreamStream = Box::pin(response.bytes_stream());
    let completion_id = native_completion_id();
    let created = native_created();
    let stream = stream::unfold(
        (
            input,
            VecDeque::<Vec<u8>>::new(),
            Vec::new(),
            0usize,
            false,
            false,
            Some(lease),
            completion_id,
            model,
            created,
            include_usage,
            prompt_tokens,
        ),
        |(
            mut input,
            mut pending,
            mut buffer,
            mut total,
            mut finished,
            mut role_sent,
            mut lease,
            completion_id,
            model,
            created,
            include_usage,
            prompt_tokens,
        )| async move {
            loop {
                if let Some(frame) = pending.pop_front() {
                    return Some((
                        Ok(Bytes::from(frame)),
                        (
                            input,
                            pending,
                            buffer,
                            total,
                            finished,
                            role_sent,
                            lease,
                            completion_id,
                            model,
                            created,
                            include_usage,
                            prompt_tokens,
                        ),
                    ));
                }
                if finished {
                    drop(lease.take());
                    return None;
                }
                let next = tokio::time::timeout(NATIVE_UPSTREAM_TIMEOUT, input.next()).await;
                let chunk = match next {
                    Ok(Some(Ok(chunk))) => chunk,
                    Ok(Some(Err(_))) | Err(_) => {
                        drop(lease.take());
                        return Some((
                            Err(io::Error::other("Codex upstream stream failed")),
                            (
                                input,
                                pending,
                                buffer,
                                total,
                                true,
                                role_sent,
                                lease,
                                completion_id,
                                model,
                                created,
                                include_usage,
                                prompt_tokens,
                            ),
                        ));
                    }
                    Ok(None) => {
                        drop(lease.take());
                        return Some((
                            Err(io::Error::other(
                                "Codex stream ended without terminal event",
                            )),
                            (
                                input,
                                pending,
                                buffer,
                                total,
                                true,
                                role_sent,
                                lease,
                                completion_id,
                                model,
                                created,
                                include_usage,
                                prompt_tokens,
                            ),
                        ));
                    }
                };
                total = match total.checked_add(chunk.len()) {
                    Some(total) if total <= MAX_UPSTREAM_BODY_BYTES => total,
                    _ => {
                        drop(lease.take());
                        return Some((
                            Err(io::Error::other("Codex upstream body exceeded limit")),
                            (
                                input,
                                pending,
                                buffer,
                                total,
                                true,
                                role_sent,
                                lease,
                                completion_id,
                                model,
                                created,
                                include_usage,
                                prompt_tokens,
                            ),
                        ));
                    }
                };
                buffer.extend_from_slice(&chunk);
                while let Some((position, delimiter_length)) = sse_delimiter(&buffer) {
                    let event = buffer.drain(..position).collect::<Vec<_>>();
                    buffer.drain(..delimiter_length);
                    let data = match codex_sse_data(&event) {
                        Ok(Some(data)) => data,
                        Ok(None) => continue,
                        Err(error) => {
                            drop(lease.take());
                            return Some((
                                Err(error),
                                (
                                    input,
                                    pending,
                                    buffer,
                                    total,
                                    true,
                                    role_sent,
                                    lease,
                                    completion_id,
                                    model,
                                    created,
                                    include_usage,
                                    prompt_tokens,
                                ),
                            ));
                        }
                    };
                    if data == "[DONE]" {
                        continue;
                    }
                    let value: Value = match serde_json::from_str(&data) {
                        Ok(value) => value,
                        Err(_) => {
                            drop(lease.take());
                            return Some((
                                Err(io::Error::other("malformed Codex event")),
                                (
                                    input,
                                    pending,
                                    buffer,
                                    total,
                                    true,
                                    role_sent,
                                    lease,
                                    completion_id,
                                    model,
                                    created,
                                    include_usage,
                                    prompt_tokens,
                                ),
                            ));
                        }
                    };
                    match value.get("type").and_then(Value::as_str) {
                        Some("response.output_text.delta") => {
                            let Some(delta) = value.get("delta").and_then(Value::as_str) else {
                                drop(lease.take());
                                return Some((
                                    Err(io::Error::other("malformed Codex delta")),
                                    (
                                        input,
                                        pending,
                                        buffer,
                                        total,
                                        true,
                                        role_sent,
                                        lease,
                                        completion_id,
                                        model,
                                        created,
                                        include_usage,
                                        prompt_tokens,
                                    ),
                                ));
                            };
                            pending.push_back(native_codex_delta_frame(
                                &completion_id,
                                &model,
                                created,
                                delta,
                                include_usage,
                                !role_sent,
                            ));
                            role_sent = true;
                        }
                        Some("response.completed") => {
                            if !role_sent {
                                pending.push_back(native_role_frame(
                                    &completion_id,
                                    &model,
                                    created,
                                    include_usage,
                                ));
                                role_sent = true;
                            }
                            pending.push_back(native_finish_frame(
                                &completion_id,
                                &model,
                                created,
                                include_usage,
                            ));
                            if include_usage {
                                match native_usage_for_prompt_tokens(&model, prompt_tokens, "") {
                                    Ok(usage) => pending.push_back(native_usage_frame(
                                        &completion_id,
                                        &model,
                                        created,
                                        usage,
                                    )),
                                    Err(_) => {
                                        drop(lease.take());
                                        return Some((
                                            Err(io::Error::other("Codex usage calculation failed")),
                                            (
                                                input,
                                                pending,
                                                buffer,
                                                total,
                                                true,
                                                role_sent,
                                                lease,
                                                completion_id,
                                                model,
                                                created,
                                                include_usage,
                                                prompt_tokens,
                                            ),
                                        ));
                                    }
                                }
                            }
                            pending.push_back(b"data: [DONE]\n\n".to_vec());
                            finished = true;
                            break;
                        }
                        Some("response.failed") | Some("response.incomplete") => {
                            drop(lease.take());
                            return Some((
                                Err(io::Error::other("Codex response failed")),
                                (
                                    input,
                                    pending,
                                    buffer,
                                    total,
                                    true,
                                    role_sent,
                                    lease,
                                    completion_id,
                                    model,
                                    created,
                                    include_usage,
                                    prompt_tokens,
                                ),
                            ));
                        }
                        _ => {}
                    }
                    if finished {
                        break;
                    }
                }
                if !pending.is_empty() || finished {
                    continue;
                }
            }
        },
    );
    let mut output = Response::new(Body::from_stream(stream));
    output.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    output
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    output
}

async fn native_codex_response_attempt(
    client: &Client,
    lease: &AccountLease,
    base_url: &str,
    payload: &Value,
    version: Option<&str>,
    timeout: Duration,
) -> Result<reqwest::Response, (ApiError, bool)> {
    let url = format!(
        "{}/backend-api/codex/responses",
        base_url.trim_end_matches('/')
    );
    let request = codex_request_headers(
        client.post(url),
        lease.token(),
        lease.chatgpt_account_id().map(ToOwned::to_owned),
        version,
    )
    .ok_or_else(|| (ApiError::upstream(), false))?
    .json(payload);
    let response = tokio::time::timeout(timeout, request.send())
        .await
        .map_err(|_| (ApiError::upstream(), false))?
        .map_err(|_| (ApiError::upstream(), false))?;
    if !response.status().is_success() {
        let retryable = matches!(
            response.status(),
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        );
        return Err((ApiError::upstream(), retryable));
    }
    if upstream_declares_oversize(&response) {
        return Err((ApiError::upstream(), false));
    }
    Ok(response)
}

async fn native_conversation_attempt(
    client: &Client,
    base_url: &str,
    token: &str,
    payload: &Value,
) -> Result<reqwest::Response, (ApiError, bool)> {
    let authenticated = !token.is_empty();
    let context = NativeRequestContext::new();
    let pow_resources = native_bootstrap(client, base_url, token, &context).await?;
    let requirements = native_chat_requirements_with_resources_for_route_context(
        client,
        base_url,
        token,
        &pow_resources,
        NATIVE_UPSTREAM_TIMEOUT,
        authenticated,
        &context,
    )
    .await?;
    let route_base = if authenticated {
        "/backend-api/conversation"
    } else {
        "/backend-anon/conversation"
    };
    let url = format!("{}{route_base}", base_url.trim_end_matches('/'));
    let mut request = native_browser_headers(client.post(url), &context)
        .header(header::ACCEPT, "text/event-stream")
        .header("X-OpenAI-Target-Path", route_base)
        .header("X-OpenAI-Target-Route", route_base)
        .header(
            "OpenAI-Sentinel-Chat-Requirements-Token",
            requirements.token,
        )
        .json(payload);
    if authenticated {
        request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(value) = requirements.so_token {
        request = request.header("OpenAI-Sentinel-SO-Token", value);
    }
    if let Some(value) = requirements.proof_token {
        request = request.header("OpenAI-Sentinel-Proof-Token", value);
    }
    if let Some(value) = requirements.turnstile_token {
        request = request.header("OpenAI-Sentinel-Turnstile-Token", value);
    }
    let upstream = tokio::time::timeout(NATIVE_UPSTREAM_TIMEOUT, request.send())
        .await
        .map_err(|_| (ApiError::upstream(), false))?
        .map_err(|_| (ApiError::upstream(), false))?;
    if !upstream.status().is_success() {
        let retryable = matches!(
            upstream.status(),
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        );
        return Err((ApiError::upstream(), retryable));
    }
    if upstream_declares_oversize(&upstream) {
        return Err((ApiError::upstream(), false));
    }
    Ok(upstream)
}

async fn chat_completions(
    state: State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    chat_completions_with_timeout(state, headers, body, NATIVE_UPSTREAM_TIMEOUT).await
}

async fn responses(
    state: State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    responses_with_timeout(state, headers, body, NATIVE_UPSTREAM_TIMEOUT).await
}

async fn messages(State(state): State<AppState>, headers: HeaderMap, body: Body) -> Response {
    match messages_inner(State(state), headers, body).await {
        Ok(response) => response,
        Err(error) => error.into_anthropic_response(),
    }
}

async fn messages_inner(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    let api_key = headers
        .get("x-api-key")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(ApiError::unauthorized)?;
    if headers
        .get("anthropic-version")
        .and_then(|value| value.to_str().ok())
        .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ApiError::invalid_request());
    }
    let bearer = format!("Bearer {api_key}");
    let mut auth_headers = headers.clone();
    auth_headers.insert(
        header::AUTHORIZATION,
        HeaderValue::from_str(&bearer).map_err(|_| ApiError::unauthorized())?,
    );
    authenticated(&auth_headers, &state).await?;
    let bytes = to_bytes(body, MAX_REQUEST_BODY_BYTES)
        .await
        .map_err(|_| ApiError::validation())?;
    let request = validate_message_request(
        serde_json::from_slice(&bytes).map_err(|_| ApiError::validation())?,
    )?;
    let model = request
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?
        .to_owned();
    let wants_stream = request
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let requires_responses = anthropic_request_requires_responses(&request);
    if state.config.upstream_protocol == UpstreamProtocol::ChatGpt {
        if requires_responses {
            let responses_payload = to_responses_payload(&request)?;
            let internal_body =
                serde_json::to_vec(&responses_payload).map_err(|_| ApiError::invalid_request())?;
            let routed = responses_with_timeout(
                State(state.clone()),
                auth_headers,
                Body::from(internal_body),
                NATIVE_UPSTREAM_TIMEOUT,
            )
            .await?;
            if wants_stream {
                return Ok(stream_responses_body_response(
                    routed.into_body(),
                    model,
                    Instant::now() + NATIVE_UPSTREAM_TIMEOUT,
                ));
            }
            let body = to_bytes(routed.into_body(), MAX_REQUEST_BODY_BYTES)
                .await
                .map_err(|_| ApiError::upstream())?;
            return Ok((
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                Json(from_responses_response(&body, &model)?),
            )
                .into_response());
        }
        let payload = to_chat_payload(&request)?;
        let internal_body =
            serde_json::to_vec(&payload).map_err(|_| ApiError::invalid_request())?;
        let routed = chat_completions_with_timeout(
            State(state.clone()),
            auth_headers,
            Body::from(internal_body),
            NATIVE_UPSTREAM_TIMEOUT,
        )
        .await?;
        if wants_stream {
            return Ok(anthropic_stream_body_response(
                routed.into_body(),
                model,
                Instant::now() + NATIVE_UPSTREAM_TIMEOUT,
            ));
        }
        let body = to_bytes(routed.into_body(), MAX_REQUEST_BODY_BYTES)
            .await
            .map_err(|_| ApiError::upstream())?;
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Json(from_chat_response(&body, &model)?),
        )
            .into_response());
    }
    if state.config.upstream_protocol != UpstreamProtocol::OpenAi {
        return Err(ApiError::unavailable());
    }
    if requires_responses {
        let payload = to_responses_payload(&request)?;
        let base_url = state
            .config
            .upstream_base_url
            .as_deref()
            .ok_or_else(ApiError::unavailable)?;
        let url = format!("{}/v1/responses", base_url.trim_end_matches('/'));
        let mut upstream_request = state.client.post(url).json(&payload);
        if let Some(auth) = state.config.upstream_auth.as_deref() {
            upstream_request = upstream_request.header(header::AUTHORIZATION, auth);
        }
        let deadline = Instant::now() + NATIVE_UPSTREAM_TIMEOUT;
        let upstream = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            upstream_request.send(),
        )
        .await
        .map_err(|_| ApiError::upstream())?
        .map_err(|_| ApiError::upstream())?;
        if !upstream.status().is_success() || upstream_declares_oversize(&upstream) {
            return Err(ApiError::upstream());
        }
        if wants_stream {
            return Ok(anthropic_stream_responses_response(
                upstream, model, deadline,
            ));
        }
        let body = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            bounded_response_body(upstream),
        )
        .await
        .map_err(|_| ApiError::upstream())??;
        return Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            Json(from_responses_response(&body, &model)?),
        )
            .into_response());
    }
    let payload = to_chat_payload(&request)?;
    let base_url = state
        .config
        .upstream_base_url
        .as_deref()
        .ok_or_else(ApiError::unavailable)?;
    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));
    let mut upstream_request = state.client.post(url).json(&payload);
    if let Some(auth) = state.config.upstream_auth.as_deref() {
        upstream_request = upstream_request.header(header::AUTHORIZATION, auth);
    }
    let deadline = Instant::now() + NATIVE_UPSTREAM_TIMEOUT;
    let upstream = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        upstream_request.send(),
    )
    .await
    .map_err(|_| ApiError::upstream())?
    .map_err(|_| ApiError::upstream())?;
    if !upstream.status().is_success() || upstream_declares_oversize(&upstream) {
        return Err(ApiError::upstream());
    }
    if wants_stream {
        return Ok(anthropic_stream_response(upstream, model, deadline));
    }
    let body = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        bounded_response_body(upstream),
    )
    .await
    .map_err(|_| ApiError::upstream())??;
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        Json(from_chat_response(&body, &model)?),
    )
        .into_response())
}

fn anthropic_request_requires_responses(request: &Map<String, Value>) -> bool {
    request
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| {
            tools.iter().any(|tool| {
                matches!(
                    tool.get("type").and_then(Value::as_str),
                    Some("web_search_20250305")
                )
            })
        })
        || request
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| {
                messages.iter().any(|message| {
                    message
                        .get("content")
                        .and_then(Value::as_array)
                        .is_some_and(|items| {
                            items.iter().any(|item| {
                                matches!(
                                    item.get("type").and_then(Value::as_str),
                                    Some("image" | "image_url" | "input_image")
                                )
                            })
                        })
                })
            })
}

fn native_codex_responses_stream_response(
    response: reqwest::Response,
    lease: AccountLease,
    deadline: Instant,
) -> Response {
    type UpstreamStream = Pin<Box<dyn Stream<Item = Result<Bytes, reqwest::Error>> + Send>>;
    let input: UpstreamStream = Box::pin(response.bytes_stream());
    let stream = stream::unfold(
        (input, Vec::new(), 0usize, false, Some(lease)),
        move |(mut input, mut buffer, total, terminal, mut lease)| async move {
            if terminal {
                drop(lease.take());
                return None;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                drop(lease.take());
                return Some((
                    Err(io::Error::other("Codex Responses stream timed out")),
                    (input, buffer, total, true, lease),
                ));
            }
            let next = tokio::time::timeout(remaining, input.next()).await;
            let chunk = match next {
                Ok(Some(Ok(chunk))) => chunk,
                Ok(Some(Err(_))) | Err(_) => {
                    drop(lease.take());
                    return Some((
                        Err(io::Error::other("Codex Responses stream failed")),
                        (input, buffer, total, true, lease),
                    ));
                }
                Ok(None) => {
                    drop(lease.take());
                    return if terminal {
                        None
                    } else {
                        Some((
                            Err(io::Error::other(
                                "Codex Responses stream ended without completion",
                            )),
                            (input, buffer, total, true, lease),
                        ))
                    };
                }
            };
            let Some(next_total) = total.checked_add(chunk.len()) else {
                drop(lease.take());
                return Some((
                    Err(io::Error::other("Codex Responses body exceeded limit")),
                    (input, buffer, total, true, lease),
                ));
            };
            if next_total > MAX_UPSTREAM_BODY_BYTES {
                drop(lease.take());
                return Some((
                    Err(io::Error::other("Codex Responses body exceeded limit")),
                    (input, buffer, total, true, lease),
                ));
            }
            buffer.extend_from_slice(&chunk);
            let mut saw_terminal = false;
            while let Some((position, delimiter_length)) = sse_delimiter(&buffer) {
                let event = buffer[..position].to_vec();
                buffer.drain(..position + delimiter_length);
                if let Ok(Some(data)) = codex_sse_data(&event) {
                    match serde_json::from_str::<Value>(&data)
                        .ok()
                        .and_then(|value| {
                            value.get("type").and_then(Value::as_str).map(str::to_owned)
                        })
                        .as_deref()
                    {
                        Some("response.completed") => saw_terminal = true,
                        Some("response.failed") | Some("response.incomplete") => {
                            saw_terminal = true
                        }
                        _ => {}
                    }
                }
            }
            Some((Ok(chunk), (input, buffer, next_total, saw_terminal, lease)))
        },
    );
    let mut output = Response::new(Body::from_stream(stream));
    output.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    output
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    output
}

async fn acquire_native_codex_lease(
    state: &AppState,
    model: &str,
    excluded_tokens: &HashSet<String>,
) -> Option<AccountLease> {
    if state.account_type_catalog.enabled() {
        state.account_type_catalog.refresh_for_model(model).await;
        let groups = state.account_type_catalog.supported_types_for(model);
        let sources = state
            .account_type_catalog
            .source_types_for(model, groups.as_ref());
        if !sources.is_empty() && !sources.contains("codex") {
            return None;
        }
    }
    let allowed_groups = state.account_type_catalog.supported_types_for(model);
    state
        .account_store
        .acquire_excluding_with_type_and_source_filter(
            model,
            excluded_tokens,
            allowed_groups.as_ref(),
            Some("codex"),
        )
        .await
}

async fn responses_with_timeout(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
    upstream_timeout: Duration,
) -> Result<Response, ApiError> {
    authenticated(&headers, &state).await?;
    let bytes = to_bytes(body, MAX_REQUEST_BODY_BYTES)
        .await
        .map_err(|_| ApiError::validation())?;
    let payload: Value = serde_json::from_slice(&bytes).map_err(|_| ApiError::validation())?;
    let object = validate_responses_payload(payload)?;
    if state.config.upstream_protocol == UpstreamProtocol::ChatGpt {
        return native_responses_with_timeout(state, object, upstream_timeout).await;
    }
    if state.config.upstream_protocol != UpstreamProtocol::OpenAi {
        return Err(ApiError::unavailable());
    }
    let Some(base_url) = state.config.upstream_base_url.as_deref() else {
        return Err(ApiError::unavailable());
    };
    let route_deadline = Instant::now() + upstream_timeout;
    let url = format!("{}/v1/responses", base_url.trim_end_matches('/'));
    let mut request = state.client.post(url).json(&object);
    if let Some(auth) = state.config.upstream_auth.as_deref() {
        request = request.header(header::AUTHORIZATION, auth);
    }
    let upstream = tokio::time::timeout_at(
        tokio::time::Instant::from_std(route_deadline),
        request.send(),
    )
    .await
    .map_err(|_| ApiError::upstream())?
    .map_err(|_| ApiError::upstream())?;
    if !upstream.status().is_success() || upstream_declares_oversize(&upstream) {
        return Err(ApiError::upstream());
    }
    if object
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(stream_response_with_timeout(upstream, upstream_timeout));
    }
    let body = tokio::time::timeout_at(
        tokio::time::Instant::from_std(route_deadline),
        bounded_response_body(upstream),
    )
    .await
    .map_err(|_| ApiError::upstream())??;
    let value: Value = serde_json::from_slice(&body).map_err(|_| ApiError::upstream())?;
    if !value.is_object() {
        return Err(ApiError::upstream());
    }
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        Json(value),
    )
        .into_response())
}

async fn native_responses_with_timeout(
    state: AppState,
    object: Map<String, Value>,
    upstream_timeout: Duration,
) -> Result<Response, ApiError> {
    let payload = native_codex_responses_payload(&object)?;
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?
        .to_owned();
    let base_url = state
        .config
        .upstream_base_url
        .as_deref()
        .ok_or_else(ApiError::unavailable)?;
    let route_deadline = Instant::now() + upstream_timeout;
    let mut attempted_tokens = HashSet::new();
    let mut lease = tokio::time::timeout_at(
        tokio::time::Instant::from_std(route_deadline),
        acquire_native_codex_lease(&state, &model, &attempted_tokens),
    )
    .await
    .map_err(|_| ApiError::upstream())?
    .ok_or_else(ApiError::unavailable)?;
    let codex_client_version = state.account_type_catalog.codex_client_version();
    let upstream = loop {
        let remaining = route_deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            drop(lease);
            return Err(ApiError::upstream());
        }
        attempted_tokens.insert(lease.token().to_owned());
        match native_codex_response_attempt(
            &state.client,
            &lease,
            base_url,
            &payload,
            codex_client_version.as_deref(),
            remaining,
        )
        .await
        {
            Ok(response) => break response,
            Err((error, retryable)) if retryable => {
                drop(lease);
                let acquired = match tokio::time::timeout_at(
                    tokio::time::Instant::from_std(route_deadline),
                    acquire_native_codex_lease(&state, &model, &attempted_tokens),
                )
                .await
                {
                    Ok(acquired) => acquired,
                    Err(_) => return Err(error),
                };
                lease = match acquired {
                    Some(lease) => lease,
                    None => return Err(error),
                };
            }
            Err((error, _)) => {
                drop(lease);
                return Err(error);
            }
        }
    };
    if object
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Ok(native_codex_responses_stream_response(
            upstream,
            lease,
            route_deadline,
        ));
    }
    let body = tokio::time::timeout_at(
        tokio::time::Instant::from_std(route_deadline),
        bounded_response_body(upstream),
    )
    .await
    .map_err(|_| ApiError::upstream())??;
    let response = native_codex_responses_json(&body, &model)?;
    drop(lease);
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        Json(response),
    )
        .into_response())
}

async fn chat_completions_with_timeout(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Body,
    upstream_timeout: Duration,
) -> Result<Response, ApiError> {
    authenticated(&headers, &state).await?;
    let bytes = to_bytes(body, MAX_REQUEST_BODY_BYTES)
        .await
        .map_err(|_| ApiError::validation())?;
    let payload: Value = serde_json::from_slice(&bytes).map_err(|_| ApiError::validation())?;
    let object = validate_chat_payload(payload)?;
    if state.config.upstream_protocol == UpstreamProtocol::ChatGpt {
        if native_conversation_payload(&object).is_err()
            && native_codex_response_payload(&object).is_err()
        {
            return Err(ApiError::unavailable());
        }
        let model = object
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("auto");
        let base_url = state
            .config
            .upstream_base_url
            .as_deref()
            .ok_or_else(ApiError::unavailable)?;
        let has_static_model = state
            .models
            .current()
            .iter()
            .any(|candidate| candidate.id == model);
        let catalog_enabled = state.account_type_catalog.enabled();
        if catalog_enabled {
            state.account_type_catalog.refresh_for_model(model).await;
        }
        let catalog_groups = catalog_enabled
            .then(|| state.account_type_catalog.supported_types_for(model))
            .flatten();
        let use_catalog_for_route = catalog_groups
            .as_ref()
            .is_some_and(|groups| !groups.is_empty());
        let allows_anonymous = state.account_type_catalog.allows_anonymous_model(model);
        let allowed_account_types = if !catalog_enabled {
            None
        } else if use_catalog_for_route {
            catalog_groups.clone()
        } else if allows_anonymous {
            Some(HashSet::new())
        } else if has_static_model && !state.account_type_catalog.model_catalog_pending(model) {
            None
        } else {
            return Err(if state.account_type_catalog.model_catalog_pending(model) {
                ApiError::catalog_pending()
            } else {
                ApiError::model_unavailable()
            });
        };
        if use_catalog_for_route
            && state
                .account_type_catalog
                .live_tokens_for(model)
                .as_ref()
                .is_some_and(|tokens| tokens.is_empty())
            && !allows_anonymous
        {
            return Err(if state.account_type_catalog.model_catalog_pending(model) {
                ApiError::catalog_pending()
            } else {
                ApiError::model_unavailable()
            });
        }
        let mut attempted_tokens = HashSet::new();
        let source_types = if use_catalog_for_route {
            state
                .account_type_catalog
                .source_types_for(model, catalog_groups.as_ref())
        } else {
            HashSet::new()
        };
        let required_source_type = (source_types.len() == 1)
            .then(|| source_types.into_iter().next().expect("source type"));
        let mut lease = state
            .account_store
            .acquire_excluding_with_type_and_source_filter(
                model,
                &HashSet::new(),
                allowed_account_types.as_ref(),
                required_source_type.as_deref(),
            )
            .await;
        if lease.is_none() && !allows_anonymous {
            return Err(ApiError::unavailable());
        }
        let mut transient_failover_used = false;
        let codex_client_version = state.account_type_catalog.codex_client_version();
        let upstream = loop {
            let token = lease.as_ref().map_or("", AccountLease::token);
            if !token.is_empty() {
                attempted_tokens.insert(token.to_owned());
            }
            let is_codex = lease
                .as_ref()
                .is_some_and(|current| current.source_type() == "codex");
            let attempt = if is_codex {
                let current = lease.as_ref().expect("Codex route lease");
                debug_assert!(!current.account_type().is_empty());
                let payload = native_codex_response_payload(&object)?;
                native_codex_response_attempt(
                    &state.client,
                    current,
                    base_url,
                    &payload,
                    codex_client_version.as_deref(),
                    NATIVE_UPSTREAM_TIMEOUT,
                )
                .await
            } else {
                let payload = native_conversation_payload(&object)?;
                native_conversation_attempt(&state.client, base_url, token, &payload).await
            };
            match attempt {
                Ok(upstream) => break upstream,
                Err((error, retryable)) if retryable && !transient_failover_used => {
                    transient_failover_used = true;
                    drop(lease);
                    lease = state
                        .account_store
                        .acquire_excluding_with_type_and_source_filter(
                            model,
                            &attempted_tokens,
                            allowed_account_types.as_ref(),
                            required_source_type.as_deref(),
                        )
                        .await;
                    if lease.is_none() {
                        return Err(error);
                    }
                }
                Err((error, _)) => {
                    drop(lease);
                    return Err(error);
                }
            }
        };
        let is_codex = lease
            .as_ref()
            .is_some_and(|current| current.source_type() == "codex");
        if object
            .get("stream")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            let include_usage = object
                .get("stream_options")
                .and_then(Value::as_object)
                .and_then(|options| options.get("include_usage"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let prompt_tokens = if include_usage {
                native_usage(&object, "")?
                    .get("prompt_tokens")
                    .and_then(Value::as_u64)
                    .and_then(|value| usize::try_from(value).ok())
                    .ok_or_else(ApiError::upstream)?
            } else {
                0
            };
            return Ok(if is_codex {
                native_codex_stream_response(
                    upstream,
                    lease,
                    model.to_owned(),
                    include_usage,
                    prompt_tokens,
                )
            } else {
                native_stream_response(
                    upstream,
                    lease,
                    model.to_owned(),
                    include_usage,
                    prompt_tokens,
                )
            });
        }
        let body = tokio::time::timeout(NATIVE_UPSTREAM_TIMEOUT, bounded_response_body(upstream))
            .await
            .map_err(|_| ApiError::upstream())??;
        if is_codex {
            let response = native_codex_responses_json(&body, model)?;
            let chat = native_codex_response_to_chat(&response, model)?;
            drop(lease);
            return Ok(Json(chat).into_response());
        }
        let text = native_completion_text(&body)?;
        let usage = native_usage(&object, &text)?;
        drop(lease);
        return Ok(Json(json!({
            "id": native_completion_id(),
            "object": "chat.completion",
            "created": native_created(),
            "model": model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text}, "finish_reason": "stop"}],
            "usage": usage,
        }))
        .into_response());
    }
    let payload = Value::Object(object.clone());
    let Some(base_url) = state.config.upstream_base_url.as_deref() else {
        return Err(ApiError::unavailable());
    };
    let url = format!("{}/v1/chat/completions", base_url.trim_end_matches('/'));
    let mut request = state.client.post(url).json(&payload);
    if let Some(auth) = state.config.upstream_auth.as_deref() {
        request = request.header(header::AUTHORIZATION, auth);
    }
    let route_deadline = Instant::now() + upstream_timeout;
    let upstream = tokio::time::timeout_at(
        tokio::time::Instant::from_std(route_deadline),
        request.send(),
    )
    .await
    .map_err(|_| ApiError::upstream())?
    .map_err(|_| ApiError::upstream())?;
    if !upstream.status().is_success() {
        return Err(ApiError::upstream());
    }
    if upstream_declares_oversize(&upstream) {
        return Err(ApiError::upstream());
    }
    let wants_stream = object
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if wants_stream {
        return Ok(stream_response_with_timeout(upstream, upstream_timeout));
    }
    let body = tokio::time::timeout_at(
        tokio::time::Instant::from_std(route_deadline),
        bounded_response_body(upstream),
    )
    .await
    .map_err(|_| ApiError::upstream())??;
    let value: Value = serde_json::from_slice(&body).map_err(|_| ApiError::upstream())?;
    if !value.is_object() {
        return Err(ApiError::upstream());
    }
    Ok((
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        Json(value),
    )
        .into_response())
}

async fn bounded_response_body(response: reqwest::Response) -> Result<Vec<u8>, ApiError> {
    if upstream_declares_oversize(&response) {
        return Err(ApiError::upstream());
    }
    let mut total = 0usize;
    let mut body = Vec::new();
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|_| ApiError::upstream())?;
        total = total
            .checked_add(chunk.len())
            .ok_or_else(ApiError::upstream)?;
        if total > MAX_UPSTREAM_BODY_BYTES {
            return Err(ApiError::upstream());
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn upstream_declares_oversize(response: &reqwest::Response) -> bool {
    response
        .content_length()
        .is_some_and(|length| length > MAX_UPSTREAM_BODY_BYTES as u64)
}

fn stream_response_with_timeout(response: reqwest::Response, read_timeout: Duration) -> Response {
    let stream = bounded_stream_with_timeout(response.bytes_stream(), read_timeout);
    let mut output = Response::new(Body::from_stream(stream));
    output.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    output
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    output
}

fn bounded_stream_with_timeout<S, E>(
    input: S,
    read_timeout: Duration,
) -> impl Stream<Item = Result<Bytes, io::Error>>
where
    S: Stream<Item = Result<axum::body::Bytes, E>> + Send + 'static,
    E: Send + 'static,
{
    stream::unfold(
        (Box::pin(input), 0usize, false),
        move |(mut input, total, terminated)| async move {
            if terminated {
                return None;
            }
            match tokio::time::timeout(read_timeout, input.next()).await {
                Err(_) => Some((
                    Err(io::Error::other("upstream stream timed out")),
                    (input, total, true),
                )),
                Ok(None) => None,
                Ok(Some(Err(_))) => Some((
                    Err(io::Error::other("upstream stream failed")),
                    (input, total, true),
                )),
                Ok(Some(Ok(chunk))) => {
                    let Some(next_total) = total.checked_add(chunk.len()) else {
                        return Some((
                            Err(io::Error::other("upstream body exceeded limit")),
                            (input, total, true),
                        ));
                    };
                    if next_total > MAX_UPSTREAM_BODY_BYTES {
                        Some((
                            Err(io::Error::other("upstream body exceeded limit")),
                            (input, total, true),
                        ))
                    } else {
                        Some((Ok(chunk), (input, next_total, false)))
                    }
                }
            }
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct CatalogRequestGuard {
        completed_normally: Arc<AtomicBool>,
        canceled: Arc<Notify>,
    }

    impl Drop for CatalogRequestGuard {
        fn drop(&mut self) {
            if !self.completed_normally.load(Ordering::Acquire) {
                self.canceled.notify_one();
            }
        }
    }

    fn native_test_turnstile_dx(program: &Value, source_p: &str) -> String {
        let plain = serde_json::to_string(program).expect("turnstile program");
        let key = source_p.as_bytes();
        assert!(!key.is_empty());
        let masked = plain
            .as_bytes()
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ key[index % key.len()])
            .collect::<Vec<_>>();
        base64::engine::general_purpose::STANDARD.encode(masked)
    }
    use axum::body::Body;
    use http_body_util::BodyExt;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tower::ServiceExt;

    fn state(auth_key: Option<&str>) -> AppState {
        AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: auth_key.map(ToOwned::to_owned),
            models: vec!["auto".to_owned(), "gpt-test".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("client")
    }

    #[tokio::test]
    async fn anthropic_messages_route_converts_standard_request_and_response() {
        let upstream = post(|Json(payload): Json<Value>| async move {
            assert_eq!(payload["model"], "gpt-test");
            assert_eq!(payload["messages"][0]["role"], "system");
            assert_eq!(payload["messages"][1]["role"], "user");
            assert_eq!(payload["messages"][1]["content"], "hello");
            assert_eq!(payload["max_tokens"], 8);
            Json(json!({
                "id":"chatcmpl-anthropic",
                "choices":[{"message":{"role":"assistant","content":"world"},"finish_reason":"stop"}],
                "usage":{"prompt_tokens":3,"completion_tokens":2}
            }))
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(upstream)),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "secret")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","max_tokens":8,"system":"be brief","messages":[{"role":"user","content":"hello"}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("message response");
        assert_eq!(value["type"], "message");
        assert_eq!(value["role"], "assistant");
        assert_eq!(value["content"][0], json!({"type":"text","text":"world"}));
        assert_eq!(value["stop_reason"], "end_turn");
        upstream_task.abort();
    }

    #[tokio::test]
    async fn anthropic_messages_route_preserves_tool_use_and_tool_result_ids() {
        let upstream = post(|Json(payload): Json<Value>| async move {
            assert_eq!(payload["messages"][0]["tool_calls"][0]["id"], "toolu_1");
            assert_eq!(payload["messages"][1]["role"], "user");
            assert_eq!(payload["messages"][1]["content"], "before");
            assert_eq!(payload["messages"][2]["role"], "tool");
            assert_eq!(payload["messages"][2]["tool_call_id"], "toolu_1");
            assert_eq!(payload["messages"][2]["content"], "done");
            assert_eq!(payload["messages"][3]["role"], "user");
            assert_eq!(payload["messages"][3]["content"], "between");
            assert_eq!(payload["messages"][4]["role"], "tool");
            assert_eq!(payload["messages"][4]["tool_call_id"], "toolu_2");
            assert_eq!(payload["messages"][4]["content"], "tool_error: tool failed");
            assert_eq!(payload["tools"][0]["function"]["name"], "lookup");
            assert_eq!(
                payload["tools"][0]["function"]["parameters"],
                json!({"type":"object"})
            );
            assert_eq!(
                payload["tool_choice"],
                json!({"type":"function","function":{"name":"lookup"}})
            );
            assert_eq!(payload["parallel_tool_calls"], false);
            Json(json!({
                "id":"chatcmpl-tool",
                "choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"toolu_2","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"next\"}"}}]},"finish_reason":"tool_calls"}],
                "usage":{"prompt_tokens":5,"completion_tokens":4}
            }))
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(upstream)),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "secret")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","max_tokens":8,"tools":[{"name":"lookup","description":"look up","input_schema":{"type":"object"}}],"tool_choice":{"type":"tool","name":"lookup","disable_parallel_tool_use":true},"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"q":"first"}}]},{"role":"user","content":[{"type":"text","text":"before"},{"type":"tool_result","tool_use_id":"toolu_1","content":"done"},{"type":"text","text":"between"},{"type":"tool_result","tool_use_id":"toolu_2","content":[{"type":"text","text":"tool failed"}],"is_error":true}]}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("message response");
        assert_eq!(value["stop_reason"], "tool_use");
        assert_eq!(value["content"][0]["type"], "tool_use");
        assert_eq!(value["content"][0]["id"], "toolu_2");
        upstream_task.abort();
    }

    #[tokio::test]
    async fn anthropic_messages_route_projects_stream_events() {
        let upstream = post(|| async {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                Body::from(
                    "data: {\"choices\":[{\"delta\":{\"role\":\"assistant\",\"content\":\"hi\"},\"finish_reason\":null}]}\n\n"
                        .to_owned()
                        + "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                ),
            )
                .into_response()
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(upstream)),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "secret")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hello"}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("sse");
        let start = text.find("event: message_start").expect("message start");
        let block_start = text
            .find("event: content_block_start")
            .expect("block start");
        let delta = text.find("event: content_block_delta").expect("text delta");
        let block_stop = text.find("event: content_block_stop").expect("block stop");
        let message_delta = text.find("event: message_delta").expect("message delta");
        let message_stop = text.find("event: message_stop").expect("message stop");
        assert!(start < block_start && block_start < delta && delta < block_stop);
        assert!(block_stop < message_delta && message_delta < message_stop);
        assert!(text.contains("text_delta"));
        upstream_task.abort();
    }

    #[tokio::test]
    async fn anthropic_messages_route_projects_tool_stream_and_rejects_early_done() {
        let upstream = post(|| async {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                Body::from(
                    r##"data: {"choices":[{"delta":{"role":"assistant","content":"lead","tool_calls":[{"index":0,"id":"toolu_stream","type":"function","function":{"name":"lookup","arguments":"{\"q\":\""}}]},"finish_reason":null}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"rust\"}"}},{"index":1,"id":"toolu_second","type":"function","function":{"name":"lookup2","arguments":"{\"x\":\""}}]},"finish_reason":null}]}

data: {"choices":[{"delta":{"tool_calls":[{"index":1,"function":{"arguments":"y\"}"}}]},"finish_reason":null}]}

data: {"choices":[{"delta":{},"finish_reason":"tool_calls"}]}

data: [DONE]

"##,
                ),
            )
                .into_response()
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(upstream)),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "secret")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","max_tokens":8,"stream":true,"tools":[{"name":"lookup","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto"},"messages":[{"role":"user","content":"lookup"}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("tool stream body")
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("sse");
        let mut started = HashSet::new();
        let mut stopped = HashSet::new();
        let mut saw_text = false;
        let mut saw_tools = HashSet::new();
        let mut saw_message_delta = false;
        for frame in text.split("\n\n").filter(|frame| !frame.is_empty()) {
            let (event, data) = frame.split_once("\ndata: ").expect("event/data frame");
            let value: Value = serde_json::from_str(data).expect("event json");
            match event.strip_prefix("event: ").expect("event name") {
                "message_start" => assert!(started.is_empty()),
                "content_block_start" => {
                    let index = value["index"].as_u64().expect("block index");
                    assert!(started.insert(index));
                    assert!(!stopped.contains(&index));
                    match value["content_block"]["type"].as_str() {
                        Some("text") => saw_text = true,
                        Some("tool_use") => {
                            saw_tools.insert(index);
                        }
                        other => panic!("unexpected content block: {other:?}"),
                    }
                }
                "content_block_delta" => {
                    let index = value["index"].as_u64().expect("delta index");
                    assert!(started.contains(&index));
                    assert!(!stopped.contains(&index));
                }
                "content_block_stop" => {
                    let index = value["index"].as_u64().expect("stop index");
                    assert!(started.contains(&index));
                    assert!(stopped.insert(index));
                }
                "message_delta" => {
                    assert_eq!(started, stopped);
                    assert_eq!(value["delta"]["stop_reason"], "tool_use");
                    saw_message_delta = true;
                }
                "message_stop" => {
                    assert!(saw_message_delta);
                    assert_eq!(started, stopped);
                }
                other => panic!("unexpected event: {other}"),
            }
        }
        assert!(saw_text);
        assert_eq!(saw_tools.len(), 2);
        assert_eq!(started.len(), 3);
        assert!(text.contains("text_delta"));
        upstream_task.abort();

        let early_done = post(|| async {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                Body::from("data: [DONE]\n\n"),
            )
                .into_response()
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/v1/chat/completions", post(early_done)),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "secret")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"lookup"}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(response.into_body().collect().await.is_err());
        upstream_task.abort();
    }

    #[tokio::test]
    async fn anthropic_stream_fails_closed_on_malformed_frame_and_incomplete_tool_json() {
        let malformed = anthropic_stream_body_response(
            Body::from(
                "data: {\"choices\":[{\"delta\":{\"content\":\"ok\"},\"finish_reason\":null}]}\n\n\
                 data: {not-json}\n\n\
                 data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            ),
            "gpt-test".to_owned(),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(malformed.into_body().collect().await.is_err());

        let incomplete_tool = anthropic_stream_body_response(
            Body::from(
                "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"toolu_bad\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{\\\\\"q\\\\\":\"}}]},\"finish_reason\":null}]}\n\n\
                 data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            ),
            "gpt-test".to_owned(),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(incomplete_tool.into_body().collect().await.is_err());

        let unknown = stream_responses_body_response(
            Body::from(
                "data: {\"type\":\"response.created\"}\n\n\
                 data: {\"type\":\"response.future_event\"}\n\n",
            ),
            "gpt-test".to_owned(),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(unknown.into_body().collect().await.is_err());
    }

    #[tokio::test]
    async fn anthropic_stream_requires_finish_reason_to_match_tool_blocks() {
        let text_then_tool_finish = anthropic_stream_body_response(
            Body::from(
                "data: {\"choices\":[{\"delta\":{\"content\":\"text\"},\"finish_reason\":null}]}\n\n\
                 data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"tool_calls\"}]}\n\n",
            ),
            "gpt-test".to_owned(),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(text_then_tool_finish.into_body().collect().await.is_err());

        let tool_then_stop = anthropic_stream_body_response(
            Body::from(
                "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"id\":\"toolu_1\",\"type\":\"function\",\"function\":{\"name\":\"lookup\",\"arguments\":\"{}\"}}]},\"finish_reason\":null}]}\n\n\
                 data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
            ),
            "gpt-test".to_owned(),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(tool_then_stop.into_body().collect().await.is_err());
    }

    #[test]
    fn anthropic_conversion_preserves_mixed_text_and_tool_calls_and_validates_finish() {
        let request = validate_message_request(json!({
            "model":"gpt-test",
            "max_tokens":8,
            "messages":[
                {"role":"assistant","content":[
                    {"type":"text","text":"before"},
                    {"type":"tool_use","id":"toolu_1","name":"lookup","input":{"q":"rust"}}
                ]}
            ]
        }))
        .expect("request");
        let payload = to_chat_payload(&request).expect("payload");
        assert_eq!(payload["messages"][0]["content"], "before");
        assert_eq!(payload["messages"][0]["tool_calls"][0]["id"], "toolu_1");

        let valid = from_chat_response(
            br#"{"choices":[{"message":{"role":"assistant","content":"before","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"q\":\"rust\"}"}}]},"finish_reason":"tool_calls"}]}"#,
            "gpt-test",
        )
        .expect("valid mixed response");
        assert_eq!(valid["content"][0]["text"], "before");
        assert_eq!(valid["content"][1]["input"]["q"], "rust");
        assert_eq!(valid["stop_reason"], "tool_use");

        let no_tool_finish = br#"{"choices":[{"message":{"role":"assistant","content":"text"},"finish_reason":"tool_calls"}]}"#;
        assert!(from_chat_response(no_tool_finish, "gpt-test").is_err());
        let wrong_finish = br#"{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{}"}}]},"finish_reason":"stop"}]}"#;
        assert!(from_chat_response(wrong_finish, "gpt-test").is_err());
        let non_object = br#"{"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"[]"}}]},"finish_reason":"tool_calls"}]}"#;
        assert!(from_chat_response(non_object, "gpt-test").is_err());
    }

    #[test]
    fn anthropic_standard_images_and_web_search_convert_to_responses_payload() {
        let request = validate_message_request(json!({
            "model":"gpt-test",
            "max_tokens":8,
            "tools":[{"type":"web_search_20250305","name":"web_search"}],
            "messages":[{"role":"user","content":[
                {"type":"text","text":"find this"},
                {"type":"image","source":{"type":"base64","media_type":"image/png","data":"AQI="}}
            ]}]
        }))
        .expect("standard image/search request");
        let payload = to_responses_payload(&request).expect("responses payload");
        assert_eq!(payload["max_output_tokens"], 8);
        assert_eq!(payload["tools"][0]["type"], "web_search_preview");
        assert_eq!(payload["input"][0]["type"], "message");
        assert_eq!(payload["input"][0]["content"][0]["type"], "input_text");
        assert_eq!(payload["input"][0]["content"][1]["type"], "input_image");
        assert_eq!(
            payload["input"][0]["content"][1]["image_url"],
            "data:image/png;base64,AQI="
        );

        let unsupported_scope = validate_message_request(json!({
            "model":"gpt-test",
            "max_tokens":8,
            "tools":[{"type":"web_search_20250305","name":"web_search","max_uses":1}],
            "messages":[{"role":"user","content":"find this"}]
        }))
        .expect_err("max_uses must not be silently dropped");
        assert_eq!(unsupported_scope.code(), "unsupported_capability");

        for (field, value) in [
            ("temperature", json!(0.2)),
            ("top_p", json!(0.8)),
            ("top_k", json!(20)),
            ("stop_sequences", json!(["END"])),
            ("metadata", json!({"request_id":"test"})),
        ] {
            let mut payload = json!({
                "model":"gpt-test",
                "max_tokens":8,
                "messages":[{"role":"user","content":"hello"}]
            });
            payload[field] = value;
            let error =
                validate_message_request(payload).expect_err("unsupported field must fail closed");
            assert_eq!(error.status, StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), "unsupported_capability", "field={field}");
        }
    }

    #[test]
    fn anthropic_responses_projection_preserves_tool_error_semantics() {
        let request = validate_message_request(json!({
            "model":"gpt-test",
            "max_tokens":8,
            "messages":[{
                "role":"user",
                "content":[{
                    "type":"tool_result",
                    "tool_use_id":"toolu-error",
                    "content":"permission denied",
                    "is_error":true
                }]
            }]
        }))
        .expect("valid Anthropic request");
        let payload = to_responses_payload(&request).expect("Responses payload");
        assert_eq!(
            payload["input"][0]["output"],
            "tool_error: permission denied"
        );
    }

    #[test]
    fn anthropic_responses_search_preserves_standard_results_citations_and_usage() {
        let body = br#"{
            "id":"resp-search",
            "usage":{"input_tokens":11,"output_tokens":7},
            "output":[
                {"type":"web_search_call","id":"ws-1","action":{"type":"search","query":"latest news"}},
                {"type":"message","role":"assistant","content":[
                    {"type":"output_text","text":"Answer from Example","annotations":[
                        {"type":"url_citation","url":"https://example.com","title":"Example","start_index":0,"end_index":6,"encrypted_index":"opaque-index"}
                    ]}
                ]}
            ]
        }"#;
        let response = from_responses_response(body, "gpt-test").expect("Anthropic response");
        assert_eq!(response["content"][0]["type"], "server_tool_use");
        assert_eq!(response["content"][1]["type"], "web_search_tool_result");
        assert_eq!(response["content"][1]["tool_use_id"], "ws-1");
        assert_eq!(
            response["content"][1]["content"].as_array().unwrap().len(),
            1
        );
        assert!(
            response["content"][1]["content"][0]["encrypted_content"]
                .as_str()
                .unwrap()
                .starts_with("chatgpt2api-search-v1:")
        );
        assert_eq!(response["content"][2]["type"], "text");
        assert_eq!(
            response["content"][2]["citations"][0]["type"],
            "web_search_result_location"
        );
        assert_eq!(
            response["content"][2]["citations"][0]["cited_text"],
            "Answer"
        );
        assert!(
            response["content"][2]["citations"][0]["encrypted_index"]
                .as_str()
                .unwrap()
                .starts_with("chatgpt2api-search-v1:")
        );
        assert_eq!(response["usage"]["input_tokens"], 11);
        assert_eq!(response["usage"]["output_tokens"], 7);
        assert_eq!(
            response["stop_reason"], "end_turn",
            "a completed server web search is not an assistant tool call"
        );

        let replay = validate_message_request(json!({
            "model":"gpt-test",
            "max_tokens":8,
            "messages":[{"role":"assistant","content":response["content"].clone()}]
        }))
        .expect("generated search response must be replayable");
        let replay_payload = to_responses_payload(&replay).expect("replay payload");
        let replay_text = serde_json::to_string(&replay_payload).expect("replay JSON");
        assert!(!replay_text.contains("<tool_") && !replay_text.contains("chatgpt2api-search-v1:"));
        assert!(
            replay_payload["input"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| {
                    item["content"].as_array().unwrap().iter().any(|block| {
                        block["type"] == "input_text"
                            && block["text"].as_str().unwrap().contains("Web search")
                    })
                })
        );
    }

    #[tokio::test]
    async fn anthropic_messages_route_returns_anthropic_error_envelope() {
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","max_tokens":8,"messages":[{"role":"user","content":"hello"}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(value["type"], "error");
        assert_eq!(value["error"]["type"], "invalid_request_error");
        assert!(value["error"]["message"].is_string());
    }

    #[test]
    fn anthropic_nonstream_rejects_unknown_upstream_finish_reason() {
        let body = br#"{"choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"vendor_new_reason"}]}"#;
        assert!(from_chat_response(body, "gpt-test").is_err());
    }

    #[test]
    fn anthropic_tool_choice_uses_standard_object_shapes() {
        let mut none = validate_message_request(json!({
            "model":"gpt-test",
            "max_tokens":4,
            "tools":[{"name":"lookup","input_schema":{"type":"object"}}],
            "tool_choice":{"type":"none"},
            "messages":[{"role":"user","content":"hello"}]
        }))
        .expect("none choice");
        assert_eq!(
            to_chat_payload(&none).expect("none payload")["tool_choice"],
            "none"
        );

        let invalid = validate_message_request(json!({
            "model":"gpt-test",
            "max_tokens":4,
            "tool_choice":"auto",
            "messages":[{"role":"user","content":"hello"}]
        }));
        assert!(invalid.is_err());

        none["tool_choice"] = json!({"type":"auto","disable_parallel_tool_use":true});
        assert_eq!(
            to_chat_payload(&none).expect("parallel payload")["parallel_tool_calls"],
            false
        );
    }

    #[test]
    fn anthropic_capability_split_keeps_function_tools_on_chat_path() {
        let function_request = validate_message_request(json!({
            "model": "gpt-test",
            "max_tokens": 8,
            "tools": [{"name": "lookup", "input_schema": {"type": "object"}}],
            "tool_choice": {"type": "auto"},
            "messages": [{"role": "user", "content": "lookup"}]
        }))
        .expect("function request");
        assert!(!anthropic_request_requires_responses(&function_request));

        let search_request = validate_message_request(json!({
            "model": "gpt-test",
            "max_tokens": 8,
            "tools": [{"type": "web_search_20250305", "name": "web_search"}],
            "messages": [{"role": "user", "content": "search"}]
        }))
        .expect("search request");
        assert!(anthropic_request_requires_responses(&search_request));
    }

    #[tokio::test]
    async fn shutdown_forces_a_pending_connection_after_bounded_drain() {
        let request_started = Arc::new(Notify::new());
        let handler_notify = request_started.clone();
        let router = Router::new().route(
            "/hold",
            get(move || {
                handler_notify.notify_one();
                async { std::future::pending::<Response>().await }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(serve_with_bounded_shutdown(
            listener,
            router,
            async move {
                let _ = shutdown_rx.await;
            },
            Duration::from_millis(20),
        ));

        let mut connection = tokio::net::TcpStream::connect(address)
            .await
            .expect("connection");
        connection
            .write_all(b"GET /hold HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .expect("request");
        request_started.notified().await;
        shutdown_tx.send(()).expect("shutdown");

        let mut server = server;
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut server)
                .await
                .is_err()
        );
        let result = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("bounded drain")
            .expect("server task");
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn shutdown_admission_fence_blocks_a_refresh_started_after_shutdown() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-catalog-shutdown-fence-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&account_path, b"[]").expect("account snapshot");
        let app_state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["static-model".to_owned()],
            upstream_base_url: Some("http://127.0.0.1:1".to_owned()),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let catalog = app_state.account_type_catalog.clone();
        let refresh_slot = catalog.refresh_task.lock().await;
        let shutdown_catalog = catalog.clone();
        let shutdown = tokio::spawn(async move {
            shutdown_catalog.shutdown().await;
        });
        tokio::task::yield_now().await;
        drop(refresh_slot);
        catalog.shutdown_taken.notified().await;

        let refresh_catalog = catalog.clone();
        let refresh = tokio::spawn(async move {
            refresh_catalog.refresh_with_cold_wait(None).await;
        });

        shutdown.await.expect("shutdown task");
        refresh.await.expect("refresh task");
        assert!(!catalog.refresh_running.load(Ordering::Acquire));
        assert!(catalog.refresh_task.lock().await.is_none());
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn shutdown_cancels_background_catalog_refresh_owner() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-catalog-shutdown-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"catalog-token","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");

        let upstream_started = Arc::new(Notify::new());
        let upstream_release = Arc::new(Notify::new());
        let upstream_canceled = Arc::new(Notify::new());
        let completed_normally = Arc::new(AtomicBool::new(false));
        let started_for_upstream = upstream_started.clone();
        let release_for_upstream = upstream_release.clone();
        let canceled_for_upstream = upstream_canceled.clone();
        let completed_for_upstream = completed_normally.clone();
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("upstream listener");
        let upstream_address = upstream_listener.local_addr().expect("upstream address");
        let upstream = tokio::spawn(async move {
            let root = get(move || {
                let started = started_for_upstream.clone();
                let release = release_for_upstream.clone();
                let canceled = canceled_for_upstream.clone();
                let completed = completed_for_upstream.clone();
                async move {
                    let _guard = CatalogRequestGuard {
                        completed_normally: completed.clone(),
                        canceled,
                    };
                    started.notify_one();
                    release.notified().await;
                    completed.store(true, Ordering::Release);
                    Html("<html></html>")
                }
            });
            axum::serve(upstream_listener, Router::new().route("/", root))
                .await
                .expect("upstream server");
        });

        let app_state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["static-model".to_owned()],
            upstream_base_url: Some(format!("http://{upstream_address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let app_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("app listener");
        let app_address = app_listener.local_addr().expect("app address");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let app = tokio::spawn(serve_state_with_bounded_shutdown(
            app_listener,
            app_state,
            async move {
                let _ = shutdown_rx.await;
            },
            Duration::from_millis(20),
        ));

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()
            .expect("client");
        let response = client
            .get(format!("http://{app_address}/v1/models"))
            .header(header::AUTHORIZATION, "Bearer client")
            .send()
            .await
            .expect("models response");
        assert_eq!(response.status(), StatusCode::OK);
        upstream_started.notified().await;

        shutdown_tx.send(()).expect("shutdown");
        tokio::time::timeout(Duration::from_secs(1), app)
            .await
            .expect("app shutdown")
            .expect("app task")
            .expect("server result");

        let canceled_before_release =
            tokio::time::timeout(Duration::from_millis(100), upstream_canceled.notified())
                .await
                .is_ok();
        upstream_release.notify_waiters();
        assert!(
            canceled_before_release,
            "catalog refresh must be canceled before shutdown returns"
        );

        upstream.abort();
        let _ = upstream.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn generic_chat_request_timeout_covers_response_headers() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("connection");
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            std::future::pending::<()>().await;
        });
        let app_state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer client"),
        );
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            chat_completions_with_timeout(
                State(app_state),
                headers,
                Body::from(r#"{"model":"gpt-test","prompt":"hello"}"#),
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("request timeout must be observed by handler");
        assert!(result.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn generic_chat_request_timeout_covers_response_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("connection");
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 128\r\n\r\n")
                .await
                .expect("headers");
            std::future::pending::<()>().await;
        });
        let app_state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer client"),
        );
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            chat_completions_with_timeout(
                State(app_state),
                headers,
                Body::from(r#"{"model":"gpt-test","prompt":"hello"}"#),
                Duration::from_millis(10),
            ),
        )
        .await
        .expect("body timeout must be observed by handler");
        assert!(result.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn generic_chat_timeout_is_shared_across_headers_and_body() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("connection");
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            tokio::time::sleep(Duration::from_millis(25)).await;
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 7\r\n\r\n{\"x\":",
                )
                .await
                .expect("first body chunk");
            tokio::time::sleep(Duration::from_millis(25)).await;
            socket.write_all(b"1}").await.expect("last body chunk");
        });
        let app_state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer client"),
        );
        let result = tokio::time::timeout(
            Duration::from_millis(250),
            chat_completions_with_timeout(
                State(app_state),
                headers,
                Body::from(r#"{"model":"gpt-test","prompt":"hello"}"#),
                Duration::from_millis(40),
            ),
        )
        .await
        .expect("shared route timeout must return");
        assert!(result.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn generic_chat_stream_rejects_declared_oversize_before_publishing() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("connection");
            let mut request = [0u8; 4096];
            let _ = socket.read(&mut request).await;
            socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 16777217\r\n\r\n",
                )
                .await
                .expect("headers");
            std::future::pending::<()>().await;
        });
        let app_state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer client"),
        );
        let result = chat_completions_with_timeout(
            State(app_state),
            headers,
            Body::from(r#"{"model":"gpt-test","stream":true,"prompt":"hello"}"#),
            Duration::from_millis(100),
        )
        .await;
        assert!(result.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn health_matches_public_shape_without_auth() {
        let response = state(Some("secret"))
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health?format=json")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value["status"], "degraded");
        assert_eq!(value["accounts"]["total"], 0);
        assert!(value["proxy_runtime"]["enabled"].is_boolean());

        let html = state(Some("secret"))
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            html.headers()[header::CONTENT_TYPE],
            "text/html; charset=utf-8"
        );
    }

    #[tokio::test]
    async fn health_reports_loaded_account_status_counts() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-health-accounts-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &path,
            r#"{"items":[
                {"access_token":"normal","status":"正常"},
                {"access_token":"limited","status":"限流"},
                {"access_token":"abnormal","status":"异常"},
                {"access_token":"disabled","status":"禁用"}
            ]}"#
            .as_bytes(),
        )
        .expect("accounts snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: vec!["auto".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health?format=json")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value["accounts"]["total"], 4);
        assert_eq!(value["accounts"]["active"], 1);
        assert_eq!(value["accounts"]["limited"], 1);
        assert_eq!(value["accounts"]["abnormal"], 1);
        assert_eq!(value["accounts"]["disabled"], 1);
        fs::remove_file(path).expect("cleanup");
    }

    #[tokio::test]
    async fn models_and_chat_require_bearer() {
        let app = state(Some("secret")).router();
        let models = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(models.status(), StatusCode::UNAUTHORIZED);

        let authorized_models = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(authorized_models.status(), StatusCode::OK);

        let padded_authorization = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer    secret  ")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(padded_authorization.status(), StatusCode::OK);

        let unauthorized = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        let body = unauthorized
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(value["error"]["code"], "invalid_api_key");
    }

    #[tokio::test]
    async fn malformed_chat_types_fail_before_upstream() {
        let app = state(Some("secret")).router();
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":[],"stream":"true"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn pydantic_type_errors_use_unprocessable_entity() {
        for payload in [
            r#"{"model":[],"prompt":"x"}"#,
            r#"{"prompt":{"text":"x"}}"#,
            r#"{"prompt":"x","stream":"true"}"#,
            r#"{"prompt":"x","messages":{}}"#,
            r#"{"prompt":"x","messages":["not-an-object"]}"#,
            r#"{"prompt":"x","n":"1"}"#,
            r#"{"prompt":"x","modalities":"text"}"#,
            r#"{"prompt":"x","modalities":["text",7]}"#,
        ] {
            let response = state(Some("secret"))
                .router()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/v1/chat/completions")
                        .header(header::AUTHORIZATION, "Bearer secret")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(payload))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::UNPROCESSABLE_ENTITY,
                "{payload}"
            );
        }
    }

    #[tokio::test]
    async fn malformed_chat_messages_fail_before_upstream() {
        for payload in [
            r#"{"model":"auto","messages":[{"role":"unknown","content":"x"}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":["x"]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user"}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"text","text":{"canary":"secret"}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":{},"arguments":"{}"}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"assistant","tool_calls":[{"id":"","type":"function","function":{"name":"lookup","arguments":"{}"}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":" ","arguments":"{}"}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.test/a.png","detail":{}}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"not-base64","format":"wav"}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"assistant","content":[{"type":"image_url","image_url":"https://example.test/a.png"}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"developer","content":[{"type":"input_audio","input_audio":{"data":"aGVsbG8=","format":"wav"}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"text","text":"hello","prompt_cache_breakpoint":"bad"}]}]}"#,
            r#"{"model":"auto"}"#,
            r#"{"model":"auto","messages":[]}"#,
            r#"{"model":"auto","prompt":"x","parallel_tool_calls":"true"}"#,
            r#"{"model":"auto","prompt":"x","tool_choice":"auto"}"#,
            r#"{"model":"auto","prompt":"x","store":true}"#,
            r#"{"model":"auto","prompt":"x","stream_options":{"include_usage":true}}"#,
            r#"{"model":"auto","prompt":"x","stream":true,"stream_options":{"include_usage":"yes"}}"#,
            r#"{"model":"auto","prompt":"x","unsupported_canary":"secret"}"#,
            r#"{"model":"auto","prompt":"x","reasoning":[]}"#,
            r#"{"model":"auto","prompt":"x","reasoning":{"effort":5}}"#,
            r#"{"model":"auto","prompt":"x","reasoning_effort":"high","thinking_effort":"low"}"#,
            r#"{"model":"auto","prompt":"x","tools":{}}"#,
            r#"{"model":"auto","prompt":"x","tools":["function"]}"#,
            r#"{"model":"auto","prompt":"x","tools":[{}]}"#,
        ] {
            let response = state(Some("secret"))
                .router()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/v1/chat/completions")
                        .header(header::AUTHORIZATION, "Bearer secret")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(payload))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{payload}");
        }

        let valid_assistant_tool_call = state(Some("secret"))
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"auto","messages":[{"role":"assistant","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            valid_assistant_tool_call.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        for payload in [
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.test/a.png","detail":"auto"}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"aGVsbG8=","format":"wav"}}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"text","text":"hello","prompt_cache_breakpoint":null}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"https://example.test/a.png","detail":"auto"},"prompt_cache_breakpoint":null}]}]}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"aGVsbG8=","format":"wav"},"prompt_cache_breakpoint":null}]}]}"#,
            r#"{"model":"auto","prompt":"x","stream":true,"stream_options":{"include_usage":true}}"#,
            r#"{"model":"auto","prompt":"x","reasoning":{"effort":"high"}}"#,
            r#"{"model":"auto","prompt":"x","tools":[{"type":"function","function":{"name":"lookup","parameters":{}}}]}"#,
            r#"{"model":null,"prompt":"x","messages":null,"stream":null}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":"hello","extra":null}]}"#,
            r#"{"model":"auto","messages":[{"role":"assistant","content":null,"tool_calls":null}]}"#,
        ] {
            let response = state(Some("secret"))
                .router()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/v1/chat/completions")
                        .header(header::AUTHORIZATION, "Bearer secret")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(payload))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{payload}"
            );
        }
    }

    #[tokio::test]
    async fn malformed_function_tools_fail_before_upstream() {
        for payload in [
            r#"{"model":"auto","prompt":"x","tools":[{"type":"function"}]}"#,
            r#"{"model":"auto","prompt":"x","tools":[{"type":"function","function":{"name":""}}]}"#,
            r#"{"model":"auto","prompt":"x","tools":[{"type":"function","function":{"name":"lookup","parameters":"{}"}}]}"#,
            r#"{"model":"auto","prompt":"x","tools":[{"type":"function","function":{"name":"lookup","strict":"true"}}]}"#,
            r#"{"model":"auto","prompt":"x","tools":[{"type":"function","function":{"name":"lookup","extra":true}}]}"#,
        ] {
            let response = state(Some("secret"))
                .router()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/v1/chat/completions")
                        .header(header::AUTHORIZATION, "Bearer secret")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(payload))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{payload}");
        }
    }

    #[tokio::test]
    async fn unknown_function_tool_types_fail_before_upstream() {
        for payload in [
            r#"{"model":"auto","prompt":"x","tools":[{"type":"unknown_canary"}]}"#,
            r#"{"model":"auto","prompt":"x","tools":[{"type":"   "}]}"#,
        ] {
            let response = state(Some("secret"))
                .router()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/v1/chat/completions")
                        .header(header::AUTHORIZATION, "Bearer secret")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(payload))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{payload}");
        }
    }

    #[tokio::test]
    async fn function_tool_choice_auto_reaches_upstream_boundary() {
        let response = state(Some("secret"))
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"auto","prompt":"x","tools":[{"type":"function","function":{"name":"lookup","parameters":{}}}],"tool_choice":"auto"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert!(matches!(
            response.status(),
            StatusCode::BAD_REQUEST | StatusCode::SERVICE_UNAVAILABLE
        ));
    }

    #[tokio::test]
    async fn function_tool_choice_none_fails_before_upstream() {
        let response = state(Some("secret"))
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"auto","prompt":"x","tools":[{"type":"function","function":{"name":"lookup","parameters":{}}}],"tool_choice":"none"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn native_message_features_allow_auto_tool_choice_to_reach_upstream() {
        for payload in [
            r#"{"model":"auto","messages":[{"role":"assistant","content":null,"tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{}"}}] }],"tool_choice":"auto"}"#,
            r#"{"model":"auto","messages":[{"role":"developer","content":"Answer concisely."}],"tool_choice":"auto"}"#,
            r#"{"model":"auto","messages":[{"role":"user","content":[{"type":"input_audio","input_audio":{"data":"aGVsbG8=","format":"wav"}}]}],"tool_choice":"auto"}"#,
        ] {
            let response = state(Some("secret"))
                .router()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/v1/chat/completions")
                        .header(header::AUTHORIZATION, "Bearer secret")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(payload))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(
                response.status(),
                StatusCode::SERVICE_UNAVAILABLE,
                "{payload}"
            );
        }
    }

    #[tokio::test]
    async fn native_message_features_reject_none_tool_choice() {
        let response = state(Some("secret"))
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"auto","messages":[{"role":"developer","content":"Answer concisely."}],"tool_choice":"none"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn auth_snapshot_controls_the_real_chat_route() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-auth-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let hash = Sha256::digest(b"file-secret");
        let hash = hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let document = json!({
            "items": [
                {"id": "enabled", "role": "user", "key_hash": hash, "enabled": true},
                {"id": "disabled", "role": "user", "key_hash": "0".repeat(64), "enabled": false}
            ]
        });
        fs::write(&path, serde_json::to_vec(&document).expect("json")).expect("write");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: vec!["auto".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: Some(path.clone()),
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        assert!(state.auth_store.accepts("file-secret"));
        assert!(!state.auth_store.accepts("disabled-secret"));
        let accepted = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer file-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(accepted.status(), StatusCode::SERVICE_UNAVAILABLE);

        fs::write(
            &path,
            serde_json::to_vec(&json!({
                "items": [
                    {"id": "enabled", "role": "user", "key_hash": hash, "enabled": false}
                ]
            }))
            .expect("disabled json"),
        )
        .expect("disable key");
        let disabled_after_reload = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer file-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(disabled_after_reload.status(), StatusCode::UNAUTHORIZED);

        fs::write(&path, br#"{"items":[{"key_hash":{"canary":"secret"}}]}"#)
            .expect("corrupt snapshot");
        let rejected_corrupt_snapshot = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer file-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(rejected_corrupt_snapshot.status(), StatusCode::UNAUTHORIZED);

        fs::write(&path, serde_json::to_vec(&document).expect("valid json"))
            .expect("restore snapshot");
        let accepted_after_recovery = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer file-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(
            accepted_after_recovery.status(),
            StatusCode::SERVICE_UNAVAILABLE
        );

        let rejected = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer disabled-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED);
        fs::remove_file(path).expect("cleanup");
    }

    #[tokio::test]
    async fn legacy_auth_key_survives_invalid_isolated_snapshot() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-legacy-auth-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        fs::write(&path, br#"{"items":[]}"#).expect("write snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("legacy-secret".to_owned()),
            models: vec!["auto".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: Some(path.clone()),
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        fs::write(&path, br#"{"items":[{"key_hash":{"canary":"secret"}}]}"#)
            .expect("corrupt snapshot");

        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer legacy-secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        fs::remove_file(path).expect("cleanup");
    }

    #[tokio::test]
    async fn legacy_auth_key_trims_config_value_like_python() {
        let response = state(Some(" secret "))
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer secret")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn auth_reload_singleflight_fences_old_publish_and_old_auth() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-auth-race-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let hash = Sha256::digest(b"file-secret");
        let hash = hash
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        let enabled_document = json!({
            "items": [
                {"id": "enabled", "role": "user", "key_hash": hash, "enabled": true}
            ]
        });
        let disabled_document = json!({
            "items": [
                {"id": "enabled", "role": "user", "key_hash": hash, "enabled": false}
            ]
        });
        fs::write(
            &path,
            serde_json::to_vec(&enabled_document).expect("enabled json"),
        )
        .expect("write enabled snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: vec!["auto".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: Some(path.clone()),
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        fs::write(
            &path,
            serde_json::to_vec(&disabled_document).expect("disabled json"),
        )
        .expect("disable key");

        let hook = AuthReloadTestHook {
            pause_before_publish: Arc::new(AtomicBool::new(true)),
            read_complete: Arc::new(Notify::new()),
            release_publish: Arc::new(Notify::new()),
        };
        let read_complete = hook.read_complete.notified();
        state.auth_store.set_test_hook(Some(hook.clone()));
        let leader = tokio::spawn(route_chat_request(state.clone(), "file-secret"));
        read_complete.await;

        let mut follower = tokio::spawn(route_chat_request(state.clone(), "file-secret"));
        let follower_before_publish =
            tokio::time::timeout(Duration::from_millis(100), &mut follower).await;
        fs::write(
            &path,
            serde_json::to_vec(&enabled_document).expect("enabled json"),
        )
        .expect("restore newer snapshot");
        hook.release_publish.notify_one();

        let leader = leader.await.expect("leader task");
        let follower_was_pending = follower_before_publish.is_err();
        let follower = match follower_before_publish {
            Ok(result) => result.expect("follower task"),
            Err(_) => follower.await.expect("follower task"),
        };
        assert!(
            follower_was_pending,
            "follower used the old generation before leader publish"
        );
        assert_eq!(leader.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(follower.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(state.auth_store.accepts("file-secret"));
        state.auth_store.set_test_hook(None);
        fs::remove_file(path).expect("cleanup");
    }

    async fn route_chat_request(state: AppState, token: &str) -> Response {
        state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, format!("Bearer {token}"))
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"x"}"#))
                    .expect("request"),
            )
            .await
            .expect("response")
    }

    #[test]
    fn malformed_auth_snapshot_fails_closed_at_startup() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-auth-invalid-{}.json",
            std::process::id()
        ));
        fs::write(&path, br#"{"items":[{"key_hash":{"secret":"canary"}}]}"#).expect("write");
        let result = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: vec!["auto".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: Some(path.clone()),
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        });
        assert!(matches!(result, Err(AppInitError::AuthSnapshot)));
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn model_snapshot_uses_the_public_allowlist_and_bounds() {
        let canary = "model-internal-canary";
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-models-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let document = json!({
            "internal": {"secret": canary},
            "data": [{
                "id": " gpt-public ",
                "created": "not-a-timestamp",
                "owned_by": {"secret": canary},
                "root": [canary],
                "permission": [canary],
                "supported_account_types": [" Pro ", {"secret": canary}, "pro", "Ä", "ä"],
                "supported_reasoning_efforts": [" low ", " high ", 9],
                "access_token": canary,
                "allow_anonymous": true
            }]
        });
        fs::write(&path, serde_json::to_vec(&document).expect("json")).expect("write");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["unused".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: Some(path.clone()),
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let models = state.models.current();
        let body = serde_json::to_string(models.as_ref()).expect("json");
        assert!(!body.contains(canary));
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id, "gpt-public");
        assert_eq!(models[0].created, 0);
        assert_eq!(models[0].owned_by, "chatgpt");
        assert_eq!(models[0].supported_account_types, vec!["pro", "ä"]);
        assert_eq!(models[0].supported_reasoning_efforts, vec!["low", "high"]);
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn model_catalog_accepts_python_reasoning_levels_alias() {
        let model = ModelCatalog::project(&json!({
            "id": "reasoning-levels-model",
            "reasoning_levels": [
                {"value": "low"},
                {"value": "high"}
            ]
        }))
        .expect("model");
        assert_eq!(model.supported_reasoning_efforts, vec!["low", "high"]);
    }

    #[test]
    fn public_models_recompute_stale_static_account_type_metadata() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-models-stale-types-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let document = json!({
            "data": [{
                "id": "shared-model",
                "allow_anonymous": false,
                "supported_account_types": ["old-type"]
            }]
        });
        fs::write(&path, serde_json::to_vec(&document).expect("json")).expect("write");
        let account_path = path.with_extension("accounts.json");
        fs::write(&account_path, b"[]").expect("accounts");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some("http://127.0.0.1:1".to_owned()),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: Some(path.clone()),
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");

        let models = state
            .account_type_catalog
            .public_models(state.models.current());
        assert_eq!(models.len(), 1);
        assert!(models[0].supported_account_types.is_empty());
        assert!(!models[0].allow_anonymous);

        fs::remove_file(path).expect("cleanup models");
        fs::remove_file(account_path).expect("cleanup accounts");
    }

    #[tokio::test]
    async fn public_models_does_not_advertise_static_anonymous_without_ready_catalog() {
        let models_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-models-static-anonymous-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &models_path,
            br#"{"data":[{"id":"static-model","allow_anonymous":true}]}"#,
        )
        .expect("models snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: Some(models_path.clone()),
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        assert_eq!(value["data"][0]["id"], "static-model");
        assert_eq!(value["data"][0]["allow_anonymous"], false);
        fs::remove_file(models_path).expect("cleanup models");
    }

    #[test]
    fn model_catalog_error_projection_distinguishes_pending_and_unsupported() {
        let pending = ApiError::catalog_pending().into_response();
        assert_eq!(pending.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(pending.headers()[header::RETRY_AFTER], "5");

        let unsupported = ApiError::model_unavailable().into_response();
        assert_eq!(unsupported.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn codex_models_require_an_explicit_semver_client_version() {
        assert_eq!(parse_codex_client_version(None), None);
        assert_eq!(parse_codex_client_version(Some("prod-web")), None);
        assert_eq!(parse_codex_client_version(Some("0.147")), None);
        assert_eq!(
            parse_codex_client_version(Some(" 0.147.0 ")),
            Some("0.147.0".to_owned())
        );
    }

    #[test]
    fn model_text_limits_count_unicode_characters() {
        let id = "测".repeat(MAX_MODEL_TEXT_LENGTH);
        let too_long = "超".repeat(MAX_MODEL_TEXT_LENGTH + 1);
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-models-unicode-{}-{}.json",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let document = json!({
            "data": [
                {"id": id, "owned_by": "所有者"},
                {"id": too_long, "owned_by": "所有者"}
            ]
        });
        fs::write(&path, serde_json::to_vec(&document).expect("json")).expect("write");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: vec![],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: Some(path.clone()),
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let models = state.models.current();
        assert_eq!(models.len(), 1);
        assert_eq!(models[0].id.chars().count(), MAX_MODEL_TEXT_LENGTH);
        fs::remove_file(path).expect("cleanup");
    }

    #[tokio::test]
    async fn models_route_reloads_generation_and_fails_closed_on_bad_snapshot() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-models-reload-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&path, br#"{"data":[{"id":"gpt-old"}]}"#).expect("initial model snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["unused".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: Some(path.clone()),
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");

        async fn model_ids(state: &AppState) -> (StatusCode, Vec<String>) {
            let response = state
                .router()
                .oneshot(
                    axum::http::Request::builder()
                        .uri("/v1/models")
                        .header(header::AUTHORIZATION, "Bearer secret")
                        .body(Body::empty())
                        .expect("request"),
                )
                .await
                .expect("response");
            let status = response.status();
            let body = response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes();
            let value: Value = serde_json::from_slice(&body).expect("json");
            let ids = value["data"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|item| item["id"].as_str().map(ToOwned::to_owned))
                .collect();
            (status, ids)
        }

        assert_eq!(
            model_ids(&state).await,
            (StatusCode::OK, vec!["gpt-old".to_owned()])
        );
        fs::write(&path, br#"{"data":[{"id":"gpt-new"}]}"#).expect("new model snapshot");
        assert_eq!(
            model_ids(&state).await,
            (StatusCode::OK, vec!["gpt-new".to_owned()])
        );
        fs::write(&path, br#"{"data":{"secret":"model-canary"}}"#).expect("bad model snapshot");
        let (status, ids) = model_ids(&state).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(ids.is_empty());
        fs::write(&path, br#"{"data":[{"id":"gpt-recovered"}]}"#)
            .expect("recovered model snapshot");
        assert_eq!(
            model_ids(&state).await,
            (StatusCode::OK, vec!["gpt-recovered".to_owned()])
        );
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn malformed_model_snapshot_fails_closed_at_startup() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-models-invalid-{}.json",
            std::process::id()
        ));
        fs::write(&path, br#"{"data":{"id":"not-an-array"}}"#).expect("write");
        let result = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: vec!["auto".to_owned()],
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: Some(path.clone()),
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        });
        assert!(matches!(result, Err(AppInitError::ModelSnapshot)));
        fs::remove_file(path).expect("cleanup");
    }

    #[tokio::test]
    async fn chat_forwards_to_configured_upstream_for_both_modes() {
        async fn upstream(headers: HeaderMap, Json(payload): Json<Value>) -> Response {
            assert_eq!(
                headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok()),
                Some("Bearer upstream-secret")
            );
            assert_eq!(payload.get("model").and_then(Value::as_str), Some("auto"));
            if payload.get("reasoning_effort").is_some() {
                assert_eq!(
                    payload.get("reasoning_effort").and_then(Value::as_str),
                    Some("auto")
                );
            }
            if payload.get("stream").and_then(Value::as_bool) == Some(true) {
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from("data: {\"ok\":true}\n\n"),
                )
                    .into_response()
            } else {
                Json(json!({
                    "id": "chatcmpl-test",
                    "object": "chat.completion",
                    "choices": [],
                }))
                .into_response()
            }
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route("/v1/chat/completions", post(upstream)),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("secret".to_owned()),
            models: vec!["auto".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: Some("Bearer upstream-secret".to_owned()),
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("client");

        for (stream, expected_content_type) in
            [(false, "application/json"), (true, "text/event-stream")]
        {
            let request_body = if stream {
                r#"{"model":null,"prompt":"x","stream":true}"#
            } else {
                r#"{"model":"  ","prompt":"x","reasoning_effort":" NONE "}"#
            };
            let response = state
                .router()
                .oneshot(
                    axum::http::Request::builder()
                        .method("POST")
                        .uri("/v1/chat/completions")
                        .header(header::AUTHORIZATION, "bEaReR secret")
                        .header(header::CONTENT_TYPE, "application/json")
                        .body(Body::from(request_body))
                        .expect("request"),
                )
                .await
                .expect("response");
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                expected_content_type
            );
            let body = response
                .into_body()
                .collect()
                .await
                .expect("body")
                .to_bytes();
            assert!(!body.is_empty());
        }
        upstream_task.abort();
    }

    #[tokio::test]
    async fn account_snapshot_filters_status_and_model_and_lease_releases() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-accounts-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &path,
            r#"{"items":[
                {"access_token":"disabled-token","status":"禁用","models":["gpt-test"]},
                {"access_token":"wrong-model-token","status":"正常","models":["other"]},
                {"access_token":"account-token","status":"正常","models":["gpt-test"]}
            ]}"#
            .as_bytes(),
        )
        .expect("accounts snapshot");
        let store = AccountStore::load(Some(&path)).expect("account store");
        assert_eq!(store.inflight(), 0);
        let lease = store.acquire("gpt-test").await.expect("eligible account");
        assert_eq!(lease.token(), "account-token");
        assert_eq!(store.inflight(), 1);
        drop(lease);
        assert_eq!(store.inflight(), 0);
        assert!(store.acquire("missing-model").await.is_none());

        let old_lease = store.acquire("gpt-test").await.expect("old lease");
        assert_eq!(old_lease.token(), "account-token");
        fs::write(
            &path,
            r#"{"items":[{"access_token":"rotated-token","status":"正常","models":["gpt-test"]}]}"#
                .as_bytes(),
        )
        .expect("rotated snapshot");
        let rotated_lease = store.acquire("gpt-test").await.expect("rotated lease");
        assert_eq!(rotated_lease.token(), "rotated-token");
        assert_eq!(store.inflight(), 1);
        drop(old_lease);
        assert_eq!(store.inflight(), 1);
        drop(rotated_lease);
        fs::write(
            &path,
            r#"{"items":[{"access_token":"rotated-token","status":"禁用","models":["gpt-test"]}]}"#
                .as_bytes(),
        )
        .expect("disabled snapshot");
        assert!(store.acquire("gpt-test").await.is_none());
        fs::write(&path, br#"{"items":[{"access_token":{"secret":true}}]}"#)
            .expect("invalid snapshot");
        assert!(store.acquire("gpt-test").await.is_none());
        fs::write(
            &path,
            r#"{"items":[{"access_token":"recovered-token","status":"正常","models":["gpt-test"]}]}"#
                .as_bytes(),
        )
        .expect("recovered snapshot");
        let recovered = store.acquire("gpt-test").await.expect("recovered lease");
        assert_eq!(recovered.token(), "recovered-token");
        drop(recovered);
        fs::remove_file(path).expect("cleanup");
    }

    #[tokio::test]
    async fn account_snapshot_rejects_explicit_invalid_statuses_and_defaults_missing_status() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-accounts-status-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &path,
            r#"{"items":[{"access_token":"account-token","status":"正常","models":["gpt-test"]}]}"#,
        )
        .expect("initial snapshot");
        let store = AccountStore::load(Some(&path)).expect("account store");
        for status in [r#""unknown""#, r#"""#, r#"7"#] {
            fs::write(
                &path,
                format!(
                    r#"{{"items":[{{"access_token":"account-token","status":{status},"models":["gpt-test"]}}]}}"#
                ),
            )
            .expect("invalid status snapshot");
            assert!(store.acquire("gpt-test").await.is_none(), "status={status}");
        }
        fs::write(
            &path,
            r#"{"items":[{"access_token":"account-token","models":["gpt-test"]}]}"#,
        )
        .expect("missing status snapshot");
        let lease = store
            .acquire("gpt-test")
            .await
            .expect("missing status defaults to normal");
        assert_eq!(lease.token(), "account-token");
        drop(lease);
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn account_snapshot_rejects_duplicate_tokens_even_with_conflicting_status() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-accounts-duplicate-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &path,
            r#"{"items":[
                {"access_token":"same-token","status":"禁用"},
                {"access_token":"same-token","status":"正常"}
            ]}"#,
        )
        .expect("duplicate snapshot");
        assert!(matches!(
            AccountStore::load(Some(&path)),
            Err(AppInitError::AccountSnapshot)
        ));
        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn legacy_oauth_login_snapshot_is_normalized_to_codex_capability() {
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-accounts-oauth-source-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &path,
            r#"[{"access_token":"oauth-token","status":"正常","type":"Pro","source_type":"oauth_login"}]"#,
        )
        .expect("oauth snapshot");
        let store = AccountStore::load(Some(&path)).expect("account store");
        let snapshot = store.snapshot.read().expect("account snapshot lock");
        assert_eq!(snapshot.accounts[0].record.source_type, "codex");
        fs::remove_file(path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_requirements_solves_pow_and_submits_proof_to_finalize() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let finalize = post(|Json(payload): Json<Value>| async move {
            assert_eq!(payload["prepare_token"], "prepare-token");
            assert!(
                payload["proof_token"]
                    .as_str()
                    .is_some_and(|value| value.starts_with("gAAAAAB"))
            );
            assert_eq!(payload["turnstile_token"], "");
            Json(json!({"token":"requirements-token"})).into_response()
        });
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        post(|| async {
                            Json(json!({
                                "prepare_token":"prepare-token",
                                "proofofwork":{"required":true,"seed":"seed","difficulty":"ff"},
                                "turnstile":{"required":false},
                                "arkose":{"required":false}
                            }))
                        }),
                    )
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize),
            )
            .await
            .expect("server");
        });

        let client = Client::builder().build().expect("client");
        let requirements = native_chat_requirements_with_timeout(
            &client,
            &format!("http://{address}"),
            "account-token",
            Duration::from_secs(1),
        )
        .await
        .expect("pow requirements");
        assert_eq!(requirements.token, "requirements-token");

        server.abort();
    }

    #[tokio::test]
    async fn native_requirements_fail_closed_for_unimplemented_challenges() {
        for challenge in ["turnstile", "arkose"] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let address = listener.local_addr().expect("address");
            let challenge_name = challenge.to_owned();
            let server = tokio::spawn(async move {
                let prepare = post(move || {
                    let challenge_name = challenge_name.clone();
                    async move {
                        let mut payload = json!({"prepare_token":"prepare-token"});
                        payload[&challenge_name] = json!({"required":true});
                        Json(payload).into_response()
                    }
                });
                let finalize = post(|| async { Json(json!({"token":"unused"})).into_response() });
                axum::serve(
                    listener,
                    Router::new()
                        .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                        .route("/backend-api/sentinel/chat-requirements/finalize", finalize),
                )
                .await
                .expect("server");
            });
            let client = Client::builder().build().expect("client");
            let result = native_chat_requirements_with_timeout(
                &client,
                &format!("http://{address}"),
                "account-token",
                Duration::from_secs(1),
            )
            .await;
            assert!(result.is_err(), "challenge={challenge}");
            server.abort();
        }
    }

    #[test]
    fn native_pow_config_matches_python_index_shape() {
        let resources = NativePowResources {
            script_sources: vec!["https://chatgpt.com/c/test/_sdk.js".to_owned()],
            data_build: "c/test/_build".to_owned(),
        };
        let config = native_pow_config("UA", &resources);
        assert_eq!(
            Value::Array(config),
            json!([
                3000,
                "Mon Jan 01 2024 00:00:00 GMT-0500 (Eastern Standard Time)",
                4294705152u64,
                1,
                "UA",
                "https://chatgpt.com/c/test/_sdk.js",
                "c/test/_build",
                "en-US",
                "en-US,es-US,en,es",
                0.5,
                "hardwareConcurrency−32",
                "location",
                "navigator",
                1234.0,
                "00000000-0000-4000-8000-000000000001",
                "",
                32,
                1_700_000_000_000.0,
                0,
                0,
                0,
                0,
                0,
                0,
                0
            ])
        );
    }

    #[test]
    fn native_pow_matches_python_golden_vector_and_target() {
        let resources = NativePowResources {
            script_sources: vec!["https://chatgpt.com/c/test/_sdk.js".to_owned()],
            data_build: "c/test/_build".to_owned(),
        };
        let inputs = NativePowConfigInputs {
            screen_sum: 3000,
            legacy_time: "Mon Jan 01 2024 00:00:00 GMT-0500 (Eastern Standard Time)".to_owned(),
            random_value: 0.5,
            navigator_key: "hardwareConcurrency−32".to_owned(),
            document_key: "location".to_owned(),
            window_key: "navigator".to_owned(),
            performance_ms: 1234.0,
            uuid: "00000000-0000-4000-8000-000000000001".to_owned(),
            cores: 32,
            epoch_minus_performance_ms: 1_700_000_000_000.0,
            edge_flag: 0,
        };
        let config = native_pow_config_from_inputs("UA", &resources, &inputs);
        let challenge = json!({
            "required": true,
            "seed": "golden-seed",
            "difficulty": "7f"
        });
        let token = native_proof_token_sync_for_config(
            Some(&challenge),
            &config,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("golden PoW vector");
        assert_eq!(
            token,
            "gAAAAABWzMwMDAsIk1vbiBKYW4gMDEgMjAyNCAwMDowMDowMCBHTVQtMDUwMCAoRWFzdGVybiBTdGFuZGFyZCBUaW1lKSIsNDI5NDcwNTE1MiwyLCJVQSIsImh0dHBzOi8vY2hhdGdwdC5jb20vYy90ZXN0L19zZGsuanMiLCJjL3Rlc3QvX2J1aWxkIiwiZW4tVVMiLCJlbi1VUyxlcy1VUyxlbixlcyIsMSwiaGFyZHdhcmVDb25jdXJyZW5jeeKIkjMyIiwibG9jYXRpb24iLCJuYXZpZ2F0b3IiLDEyMzQuMCwiMDAwMDAwMDAtMDAwMC00MDAwLTgwMDAtMDAwMDAwMDAwMDAxIiwiIiwzMiwxNzAwMDAwMDAwMDAwLjAsMCwwLDAsMCwwLDAsMF0="
        );

        let encoded = &token["gAAAAAB".len()..];
        let candidate = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .expect("base64 proof");
        let mut hasher = Sha3_512::new();
        hasher.update(b"golden-seed");
        hasher.update(encoded.as_bytes());
        let digest = hasher.finalize();
        assert!(digest[0] <= 0x7f);
        assert!(candidate.windows(2).any(|pair| pair == b",2"));
    }

    #[test]
    fn native_pow_seed_limit_matches_python_unicode_character_contract() {
        let resources = NativePowResources {
            script_sources: vec![],
            data_build: String::new(),
        };
        let config = native_pow_config("UA", &resources);
        let challenge = json!({
            "required": true,
            "seed": "é".repeat(MAX_POW_TEXT_LENGTH),
            "difficulty": "ff"
        });
        let token = native_proof_token_sync_for_config(
            Some(&challenge),
            &config,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("256 Unicode characters are valid under Python's seed limit");
        assert!(token.starts_with("gAAAAAB"));
    }

    #[test]
    fn native_turnstile_executes_valid_program_and_enforces_bounds() {
        let source_p = "source-p";
        let challenge = json!({
            "required": true,
            "dx": native_test_turnstile_dx(&json!([[3, source_p]]), source_p)
        });
        let token = native_turnstile_token_sync(
            Some(&challenge),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("turnstile program");
        assert_eq!(
            token,
            base64::engine::general_purpose::STANDARD.encode(source_p.as_bytes())
        );

        let oversized_program = vec![json!([3, "x"]); MAX_TURNSTILE_INSTRUCTIONS + 1];
        let oversized = json!({
            "required": true,
            "dx": native_test_turnstile_dx(&Value::Array(oversized_program), source_p)
        });
        assert!(
            native_turnstile_token_sync(
                Some(&oversized),
                source_p,
                &AtomicBool::new(false),
                Instant::now() + Duration::from_secs(1),
            )
            .is_err()
        );
        assert!(
            native_turnstile_token_sync(
                Some(&json!({"required":true,"dx":"x".repeat(MAX_TURNSTILE_DX_CHARS + 1)})),
                source_p,
                &AtomicBool::new(false),
                Instant::now() + Duration::from_secs(1),
            )
            .is_err()
        );
        assert!(native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":native_test_turnstile_dx(&json!([[99]]), source_p)})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .is_err());
    }

    #[test]
    fn native_turnstile_bool_stringification_matches_python() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAQQBGhAvTz4fXEBeWVAbR3BcKF5ZQVNJHkEuQy5AU0keQF9cRV5QSR5ALjI=";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("bool conversion fixture");
        // Generated by utils.turnstile.solve_turnstile_token for the same
        // fixed dx/source-p fixture.
        assert_eq!(token, "LAoNHQ==");
    }

    #[test]
    fn native_turnstile_float_stringification_matches_python() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAUEWREdCPkl2Ql9cRF5BHQ8tXzREXlBVAUNCMlkpUVUBQ0NDRkJPVgFDQzIo";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("float conversion fixture");
        // Generated by utils.turnstile.solve_turnstile_token for the same
        // fixed dx/source-p fixture (Python uses 1e+20 here).
        assert_eq!(token, "SR1TSkg=");
    }

    #[test]
    fn native_turnstile_json_dumps_matches_python_default_format() {
        let source_p = "source-p";
        let dx = "KDRHXlZJVlISTU9QAUcBUi8aQRdRAQ9KUTMARFZdGlIOMlkpUlABRl9aKF44Vx1cSkNMXlBJGy0u";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("json.dumps conversion fixture");
        // Generated by utils.turnstile.solve_turnstile_token for the same
        // fixed dx/source-p fixture. Python json.dumps uses its default
        // separators and ensure_ascii=True.
        assert_eq!(token, "eyJhIjogImIiLCAiXHU0ZTJkIjogIlx1NjU4NyJ9");
    }

    #[test]
    fn native_turnstile_json_dumps_preserves_object_insertion_order() {
        let source_p = "source-p";
        let dx = "KDRHXlZJVlIJTU9DT0dMUkldCC9PPhxFX1lZRz5JdkJDQ0xeWkkeXEUyKA==";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("object order conversion fixture");
        // Python json.dumps keeps insertion order (z before a). This vector
        // also prevents the default serde_json BTreeMap ordering from being
        // mistaken for the Turnstile VM contract.
        assert_eq!(token, "eyJ6IjogMSwgImEiOiAyfQ==");
    }

    #[test]
    fn native_turnstile_json_dumps_matches_python_float_exponent_format() {
        let source_p = "source-p";
        let dx = "KDRHXlZJVlILTU9DBk4fQA4yWSlSUAFGX1ooXjhXHVxKQ0xeUEkbLS4=";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("float JSON conversion fixture");
        // Python json.dumps formats this value as 1e+20.
        assert_eq!(token, "eyJ4IjogMWUrMjB9");
    }

    #[test]
    fn native_turnstile_json_dumps_preserves_python_negative_exponent_width() {
        let source_p = "source-p";
        let dx = "KDRHXlJJHBVeX0IvTz4cRV9dWUM+SXZCQ0NEXlJJHlxBMig=";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("negative exponent JSON conversion fixture");
        // Python json.dumps(1e-7) emits 1e-07; serde_json commonly emits
        // 1e-7, so this is a direct cross-language golden vector.
        assert_eq!(token, "MWUtMDc=");
    }

    #[test]
    fn native_turnstile_string_matches_python_float_exponent_width() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAUEWQkVFPkl2Ql9cRF5TOAErQkNGQk9WHC1fNEdCT1YdXEBfWUFPVh0tLg==";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("float string conversion fixture");
        // Python str(1e-7) also emits 1e-07 before opcode 1 string XOR.
        assert_eq!(token, "AVUdAAc=");
    }

    #[test]
    fn native_turnstile_opcode_five_preserves_python_integer_nan_branch() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAUEuQy5AT1YcXEEyWSlWSR5AX1xEL08+H0BfXEVeUFUBQ19cRS8+";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("integer opcode five fixture");
        // Python func_5 excludes JSON integers from its string/float branch
        // and stores the literal NaN marker.
        assert_eq!(token, "TmFO");
    }

    #[test]
    fn native_turnstile_opcode_five_matches_python_object_repr_branch() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAVIDHRAUCh0PLV80R15QVAELUQ5XSFJJDxJRVS4GERBIXB0aGR5PR1VSLhIoXjhQAUNDQ0ZDPkl2QkNDRkJPVh1cQENGQj44";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("object repr opcode five fixture");
        // Python _turnstile_to_str(dict) uses Python's repr, including
        // single quotes and True/None in nested JSON values.
        assert_eq!(
            token,
            "cHJlZml4eydhJzogMSwgJ2InOiBbVHJ1ZSwgTm9uZSwgJ3gnXX0="
        );
    }

    #[test]
    fn native_turnstile_does_not_structurally_equal_distinct_ordered_maps() {
        let source_p = "source-p";
        let program = json!([
            [2, 40, "window.Object.create"],
            [17, 30, 40],
            [17, 31, 40],
            [20, 30, 31, 3, 16]
        ]);
        let dx = native_test_turnstile_dx(&program, source_p);
        // Python OrderedMap uses identity equality. The two separately
        // created objects are therefore unequal and the opcode-20 callback
        // is not invoked; Rust must not turn them into equal JSON objects.
        assert!(
            native_turnstile_token_sync(
                Some(&json!({"required": true, "dx": dx})),
                source_p,
                &AtomicBool::new(false),
                Instant::now() + Duration::from_secs(1),
            )
            .is_err()
        );
    }

    #[test]
    fn native_turnstile_does_not_equal_missing_process_slots() {
        let source_p = "source-p";
        let program = json!([[20, 30, 31, 3, 16]]);
        let dx = native_test_turnstile_dx(&program, source_p);
        // Python process_map[30] raises KeyError before the callback can run,
        // so this instruction is skipped and solve_turnstile_token returns
        // None. Rust must not compare two missing Option values as equal.
        assert!(
            native_turnstile_token_sync(
                Some(&json!({"required": true, "dx": dx})),
                source_p,
                &AtomicBool::new(false),
                Instant::now() + Duration::from_secs(1),
            )
            .is_err()
        );
    }

    #[test]
    fn native_turnstile_opcode_one_matches_python_container_stringification() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAQtRDldIUhhwXChdWUFSSXZSEk1ZQz44AStCQ0ZCT1YcLV80R0JPVh1cQF9ZQU9WHS0u";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("container stringification fixture");
        // Python _turnstile_to_str(dict/list) uses their finite repr in
        // opcode 1 before XOR; the vector is generated by solve_turnstile_token.
        assert_eq!(token, "IAAAABYAACA=");
    }

    #[test]
    fn native_turnstile_rejects_unseeded_math_random_instead_of_faking_parity() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAVIDHRAUCh0PLV80R15QVAFSBAYbFgwSAz0SGx1cEQRDFBwCVy9PPhxHX1xHXlBUcFwoWllBU0keQi5DLkBTSR5AX1xFXlBJHkAuMg==";
        let result = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        );
        // Python's solver uses fresh random.random() here. The vector's
        // expected token is only deterministic because its Python fixture
        // patches random.random to 0.5; Rust must not hard-code that value.
        assert!(result.is_err());
    }

    #[test]
    fn native_turnstile_rejects_runtime_performance_now_instead_of_faking_parity() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAVIDHRAUCh0PLV80R15QVAFSBAYbFgwSAwAWHRMdEQhMHhAKWxwMEg8tXzRERU9WH1xAXiheOFABQ0NDRkA+SXZCQ0NGQk9WHVxAQ0ZCPjg=";
        let result = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        );
        // Python combines wall-clock elapsed time with random.random(). Rust
        // has no equivalent browser/runtime clock contract in this canary.
        assert!(result.is_err());
    }

    #[test]
    fn native_turnstile_rejects_reflect_set_on_plain_json_object() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAQsOMlkpUUkeQV9NAhsNAUIHXT0QFA8ATgRdHBAGQTgBK0FDRkBPR0YVCk0oXjhXAUNAQ1cEAglYFVEyWSlUSR5BX1xFXlBXAUNAMlkpUlABQ0dDRkI+SXZCQ0NGRk9WGVxAQ0ZGPjg=";
        let result = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        );
        // Python's dict has no OrderedMap.add(), so Reflect.set is skipped
        // and the subsequent dump is the empty object (base64: e30=). Rust
        // must not mutate a plain JSON object and emit a different token.
        assert!(result.is_err());
    }

    #[test]
    fn native_turnstile_rejects_object_keys_for_non_local_storage() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAVIEBhsWDBIDPxEFEBEXS0YVChxXL08+H1xAXllQFAxDFBwYWz8CEUVSLkMuQ1RJHkJfXEVeUFRwXChdWUFQSQ8AAQoTGxtHcFwoWllBUEkeQi5DLkBTSR5DX1xGXlBJHkMuMg==";
        let result = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        );
        // Python leaves the result slot untouched unless the argument is
        // exactly window.localStorage; Rust must not inject a fixed list.
        assert!(result.is_err());
    }

    #[test]
    fn native_turnstile_object_keys_local_storage_keeps_python_path() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAVIEBhsWDBIDPxEFEBEXS0YVChxXL08+H1xAXllQFAxDFBwYWx4MBkwcIBsaAAICSFIuQy5DVEkeQl9cRV5QVHBcKF5MXlBXcFwoXUVeUFcBQ0FDRl5QV3At";
        let token = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        )
        .expect("localStorage Object.keys fixture");
        assert_eq!(
            token,
            "VTFSQlZGTkpSMTlNVDBOQlRGOVRWRTlTUVVkRlgwbE9WRVZTVGtGTVgxTlVUMUpGWDFZMExGTlVRVlJUU1VkZlRFOURRVXhmVTFSUFVrRkhSVjlUVkVGQ1RFVmZTVVFzWTJ4cFpXNTBMV052Y25KbGJHRjBaV1F0YzJWamNtVjBMRzloYVM5aGNIQnpMMk5oY0VWNGNHbHlaWE5CZEN4dllXa3RaR2xrTEZOVVFWUlRTVWRmVEU5RFFVeGZVMVJQVWtGSFJWOU1UMGRIU1U1SFgxSkZVVlZGVTFRc1ZXbFRkR0YwWlM1cGMwNWhkbWxuWVhScGIyNURiMnhzWVhCelpXUXVNUT09"
        );
    }

    #[test]
    fn native_turnstile_python_repr_uses_python_control_escapes() {
        assert_eq!(
            native_turnstile_python_repr(&json!({"value": "\u{08}\u{0c}"})),
            Some("{'value': '\\x08\\x0c'}".to_owned())
        );
        assert_eq!(
            native_turnstile_python_repr(&json!({"value": "a'b"})),
            Some("{'value': \"a'b\"}".to_owned())
        );
        assert_eq!(
            native_turnstile_python_repr(&json!({"value": "\u{0378}"})),
            Some(r"{'value': '\u0378'}".to_owned())
        );
        assert_eq!(
            native_turnstile_python_quote("''\""),
            String::from("'") + "\\'" + "\\'" + "\"" + "'"
        );
        assert_eq!(
            native_turnstile_python_quote("'\""),
            String::from("'") + "\\'" + "\"" + "'"
        );
        assert_eq!(
            native_turnstile_python_quote("\""),
            String::from("'") + "\"" + "'"
        );
        assert_eq!(
            native_turnstile_python_quote("\u{00a0}\u{00ad}\u{200b}\u{feff}"),
            r"'\xa0\xad\u200b\ufeff'"
        );
    }

    #[test]
    fn native_turnstile_python_repr_matches_del_c1_and_unicode_separator_escapes() {
        assert_eq!(
            native_turnstile_python_repr(&json!({
                "value": "\u{7f}\u{80}\u{85}\u{2028}\u{2029}"
            })),
            Some(r"{'value': '\x7f\x80\x85\u2028\u2029'}".to_owned())
        );
    }

    #[test]
    fn native_turnstile_python_repr_escapes_python_nonprintable_categories() {
        assert_eq!(
            native_turnstile_python_repr(&json!({
                "unassigned": "\u{0378}",
                "separator": "\u{1680}",
                "private_use": "\u{e000}",
            })),
            Some(
                r"{'unassigned': '\u0378', 'separator': '\u1680', 'private_use': '\ue000'}"
                    .to_owned()
            )
        );
    }

    #[test]
    fn native_turnstile_opcode_23_skips_a_null_guard_like_python() {
        let source_p = "source-p";
        let dx = "KDRHXlBVAR4GAxkvTz4fQ19cRV5QSQ8DHBoHEQZIXVIuMg==";
        let result = native_turnstile_token_sync(
            Some(&json!({"required":true,"dx":dx})),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        );
        // Python solve_turnstile_token returns None because process[30] is
        // null and func_23 requires a non-None guard.
        assert!(result.is_err());
    }

    #[test]
    fn native_pow_resource_parser_accepts_html_attribute_variants() {
        let html =
            br#"<html data-build="build-canary"><SCRIPT defer SRC = '/c/abc/_build.js'></SCRIPT>"#;
        let resources = parse_native_pow_resources(html);
        assert_eq!(resources.script_sources, vec!["/c/abc/_build.js"]);
        assert_eq!(resources.data_build, "c/abc/_");
    }

    #[test]
    fn native_pow_resource_parser_requires_a_real_script_tag_name() {
        let resources = parse_native_pow_resources(
            br#"<scripture src='/wrong.js'></scripture><script src='/right.js'></script>"#,
        );
        assert_eq!(resources.script_sources, vec!["/right.js"]);
    }

    #[test]
    fn native_turnstile_matches_python_fixture_and_fails_closed_on_unsupported_ordered_map() {
        let source_p = "source-p";
        // This is the exact dx consumed by the Python solver in
        // test_turnstile_python_fixture_with_ordered_map_and_callbacks. The
        // authoritative result is None: Python's json.dumps(OrderedMap)
        // raises TypeError and its raw-argument opcode 23 callback raises as
        // well. Rust must not manufacture a token for this unproven VM shape.
        let dx = "KDRNXkNVAVBCXyheQz4fXFNeWVJBN0gWHwoWBkE4AVAoWVlSUUkNQF9PRC9PRXZCX09GXkNHXhUHTSheQz4fRF9PQV5DVwFQQDJZUjhXAVBGQ1VQFAxDFBwYWz0BD0gTB0EWAAYEWRVRMllSOFQaXFNZWVJWOAFQKF1ZUlRJDVISTSheQz4fXFNXWVJBBw8tX08uRU9FGVxTWVlSVEkNSC5DVSlSUAFQSkNVRD5JDStLQ1VDUkkNQUUyWVI4VwFQQl1ZUkEWTB0WTSheQz4fXFNeRl5DR14RHgpXL09FdkJfT0RGT0UcRS5DVSlRSQ1BRkNVUBcERBxRMllSOFcdXFNeR15DVB5cU11ZUlJRAVBCWiheQz4fQ19PREBPRR5cU1YoLw==";
        let challenge = json!({"required": true, "dx": dx});
        let result = native_turnstile_token_sync(
            Some(&challenge),
            source_p,
            &AtomicBool::new(false),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(
            result.is_err(),
            "Rust must match Python None as fail-closed"
        );
    }

    #[tokio::test]
    async fn native_missing_pow_does_not_wait_for_blocking_permit() {
        let mut permits = Vec::new();
        for _ in 0..NATIVE_POW_MAX_CONCURRENCY {
            permits.push(
                NATIVE_POW_SEMAPHORE
                    .clone()
                    .acquire_owned()
                    .await
                    .expect("pow permit"),
            );
        }
        let result = native_proof_token(
            None,
            NATIVE_USER_AGENT,
            &NativePowResources::default(),
            Instant::now() + Duration::from_millis(50),
        )
        .await;
        assert_eq!(result.expect("missing proof is an empty token"), "");
        drop(permits);
    }

    #[tokio::test]
    async fn native_requirements_finalize_sends_empty_unimplemented_tokens() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let prepare =
                post(|| async { Json(json!({"prepare_token":"prepare-token"})).into_response() });
            let finalize = post(|Json(payload): Json<Value>| async move {
                assert_eq!(payload["proof_token"], "");
                assert_eq!(payload["turnstile_token"], "");
                Json(json!({"token":"requirements-token"})).into_response()
            });
            axum::serve(
                listener,
                Router::new()
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize),
            )
            .await
            .expect("server");
        });
        let client = Client::builder().build().expect("client");
        let result = native_chat_requirements_with_timeout(
            &client,
            &format!("http://{address}"),
            "account-token",
            Duration::from_secs(1),
        )
        .await;
        assert_eq!(result.expect("requirements").token, "requirements-token");
        server.abort();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn native_pow_difficulty_does_not_block_the_async_runtime() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let difficulty = "00".repeat(64);
        let server = tokio::spawn(async move {
            let prepare = post(move || {
                let difficulty = difficulty.clone();
                async move {
                    Json(json!({
                        "prepare_token":"prepare-token",
                        "proofofwork":{"required":true,"seed":"runtime-nonblocking-test-seed","difficulty":difficulty}
                    }))
                    .into_response()
                }
            });
            let finalize = post(|| async { Json(json!({"token":"unused"})).into_response() });
            axum::serve(
                listener,
                Router::new()
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize),
            )
            .await
            .expect("server");
        });
        let client = Client::builder().build().expect("client");
        let result = tokio::time::timeout(
            Duration::from_millis(50),
            native_chat_requirements_with_timeout(
                &client,
                &format!("http://{address}"),
                "account-token",
                Duration::from_secs(5),
            ),
        )
        .await;
        assert!(result.is_err(), "PoW must not block the async runtime");
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn native_pow_timeout_waits_for_worker_and_releases_permit() {
        let initial_active = NATIVE_POW_ACTIVE_WORKERS.load(Ordering::Acquire);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let prepare_count = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn(async move {
            let prepare_count_for_handler = prepare_count.clone();
            let prepare = post(move || {
                let prepare_count = prepare_count_for_handler.clone();
                async move {
                    let index = prepare_count.fetch_add(1, Ordering::AcqRel);
                    let difficulty = if index < NATIVE_POW_MAX_CONCURRENCY {
                        "00".repeat(64)
                    } else {
                        "ff".to_owned()
                    };
                    Json(json!({
                        "prepare_token":"prepare-token",
                        "proofofwork":{"required":true,"seed":"seed","difficulty":difficulty}
                    }))
                    .into_response()
                }
            });
            let finalize =
                post(|| async { Json(json!({"token":"requirements-token"})).into_response() });
            axum::serve(
                listener,
                Router::new()
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize),
            )
            .await
            .expect("server");
        });
        let client = Client::builder().build().expect("client");
        let base_url = format!("http://{address}");
        let mut timed_out = Vec::new();
        for _ in 0..NATIVE_POW_MAX_CONCURRENCY {
            let client = client.clone();
            let base_url = base_url.clone();
            timed_out.push(tokio::spawn(async move {
                native_chat_requirements_with_timeout(
                    &client,
                    &base_url,
                    "account-token",
                    Duration::from_millis(75),
                )
                .await
            }));
        }
        for task in timed_out {
            assert!(task.await.expect("worker join").is_err());
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while NATIVE_POW_ACTIVE_WORKERS.load(Ordering::Acquire) != initial_active {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("timed-out workers must drain before the caller returns");

        let recovered = tokio::time::timeout(
            Duration::from_secs(1),
            native_chat_requirements_with_timeout(
                &client,
                &base_url,
                "account-token",
                Duration::from_secs(1),
            ),
        )
        .await
        .expect("a released permit must admit the next solve")
        .expect("the next low-cost solve must succeed");
        assert_eq!(recovered.token, "requirements-token");
        assert_eq!(
            NATIVE_POW_ACTIVE_WORKERS.load(Ordering::Acquire),
            initial_active
        );
        server.abort();
    }

    #[tokio::test]
    async fn native_chat_slice_selects_account_translates_sse_and_releases_on_drop() {
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let upstream_address = upstream_listener.local_addr().expect("address");
        let call_log = Arc::new(Mutex::new(Vec::<&'static str>::new()));
        let parent_ids = Arc::new(Mutex::new(Vec::<String>::new()));
        let browser_ids = Arc::new(Mutex::new(None::<(String, String)>));
        let bootstrap_log = call_log.clone();
        let bootstrap_browser_ids = browser_ids.clone();
        let bootstrap = get(move |headers: HeaderMap| {
            let bootstrap_log = bootstrap_log.clone();
            let bootstrap_browser_ids = bootstrap_browser_ids.clone();
            async move {
                if headers.get(header::AUTHORIZATION).is_none() {
                    return (
                        StatusCode::OK,
                        Body::from(
                            r#"<html data-build="c/test/_build"><script src="https://chatgpt.com/c/test/_sdk.js"></script></html>"#,
                        ),
                    )
                        .into_response();
                }
                assert_eq!(
                    headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer account-token")
                );
                for name in [
                    "Origin",
                    "Referer",
                    "Accept-Language",
                    "Sec-Ch-Ua",
                    "OAI-Device-Id",
                    "OAI-Session-Id",
                    "OAI-Client-Version",
                    "OAI-Client-Build-Number",
                ] {
                    assert!(headers.get(name).is_some(), "missing browser header {name}");
                }
                let ids = (
                    headers
                        .get("OAI-Device-Id")
                        .and_then(|value| value.to_str().ok())
                        .expect("device id")
                        .to_owned(),
                    headers
                        .get("OAI-Session-Id")
                        .and_then(|value| value.to_str().ok())
                        .expect("session id")
                        .to_owned(),
                );
                bootstrap_browser_ids.lock().await.replace(ids);
                bootstrap_log.lock().await.push("bootstrap");
                (
                    StatusCode::OK,
                    Body::from(
                        r#"<html data-build="c/test/_build"><script src="https://chatgpt.com/c/test/_sdk.js"></script></html>"#,
                    ),
                )
                    .into_response()
            }
        });
        let prepare_log = call_log.clone();
        let prepare_browser_ids = browser_ids.clone();
        let prepare = post(move |headers: HeaderMap, Json(payload): Json<Value>| {
            let prepare_log = prepare_log.clone();
            let prepare_browser_ids = prepare_browser_ids.clone();
            async move {
                assert_eq!(
                    headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer account-token")
                );
                assert_eq!(
                    headers
                        .get("X-OpenAI-Target-Path")
                        .and_then(|value| value.to_str().ok()),
                    Some("/backend-api/sentinel/chat-requirements/prepare")
                );
                assert_eq!(
                    headers
                        .get("X-OpenAI-Target-Route")
                        .and_then(|value| value.to_str().ok()),
                    Some("/backend-api/sentinel/chat-requirements/prepare")
                );
                let ids = prepare_browser_ids
                    .lock()
                    .await
                    .clone()
                    .expect("bootstrap browser ids");
                assert_eq!(
                    headers
                        .get("OAI-Device-Id")
                        .and_then(|value| value.to_str().ok()),
                    Some(ids.0.as_str())
                );
                assert_eq!(
                    headers
                        .get("OAI-Session-Id")
                        .and_then(|value| value.to_str().ok()),
                    Some(ids.1.as_str())
                );
                assert!(
                    payload["p"]
                        .as_str()
                        .is_some_and(|value| value.starts_with("gAAAAAC"))
                );
                let encoded = payload["p"]
                    .as_str()
                    .and_then(|value| value.strip_prefix("gAAAAAC"))
                    .expect("legacy p");
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .expect("legacy p base64");
                let config: Value = serde_json::from_slice(&decoded).expect("legacy p config");
                assert_eq!(config[5], "https://chatgpt.com/c/test/_sdk.js");
                // Python's ScriptSrcParser gives the script-derived build
                // precedence over the html data-build attribute.
                assert_eq!(config[6], "c/test/_");
                let p = payload["p"].as_str().expect("source p");
                let turnstile_dx = native_test_turnstile_dx(&json!([[3, p]]), p);
                prepare_log.lock().await.push("prepare");
                Json(json!({
                    "prepare_token": "prepare-token",
                    "arkose": {"required": false},
                    "proofofwork": {"required": true, "seed": "seed", "difficulty": "ff"},
                    "turnstile": {"required": true, "dx": turnstile_dx}
                }))
                .into_response()
            }
        });
        let finalize_log = call_log.clone();
        let finalize_browser_ids = browser_ids.clone();
        let finalize = post(move |headers: HeaderMap, Json(payload): Json<Value>| {
            let finalize_log = finalize_log.clone();
            let finalize_browser_ids = finalize_browser_ids.clone();
            async move {
                assert_eq!(
                    headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer account-token")
                );
                assert_eq!(
                    headers
                        .get("X-OpenAI-Target-Path")
                        .and_then(|value| value.to_str().ok()),
                    Some("/backend-api/sentinel/chat-requirements/finalize")
                );
                assert_eq!(
                    headers
                        .get("X-OpenAI-Target-Route")
                        .and_then(|value| value.to_str().ok()),
                    Some("/backend-api/sentinel/chat-requirements/finalize")
                );
                let ids = finalize_browser_ids
                    .lock()
                    .await
                    .clone()
                    .expect("bootstrap browser ids");
                assert_eq!(
                    headers
                        .get("OAI-Device-Id")
                        .and_then(|value| value.to_str().ok()),
                    Some(ids.0.as_str())
                );
                assert_eq!(
                    headers
                        .get("OAI-Session-Id")
                        .and_then(|value| value.to_str().ok()),
                    Some(ids.1.as_str())
                );
                assert_eq!(payload["prepare_token"], "prepare-token");
                assert!(
                    payload["proof_token"]
                        .as_str()
                        .is_some_and(|value| value.starts_with("gAAAAAB"))
                );
                let expected_turnstile = base64::engine::general_purpose::STANDARD.encode(
                    native_requirements_token(
                        NATIVE_USER_AGENT,
                        &NativePowResources {
                            script_sources: vec!["https://chatgpt.com/c/test/_sdk.js".to_owned()],
                            data_build: "c/test/_".to_owned(),
                        },
                    )
                    .expect("legacy p")
                    .as_bytes(),
                );
                assert_eq!(payload["turnstile_token"], expected_turnstile);
                finalize_log.lock().await.push("finalize");
                Json(json!({"token":"requirements-token","so_token":"so-token"})).into_response()
            }
        });
        let conversation_log = call_log.clone();
        let conversation_parent_ids = parent_ids.clone();
        let conversation_browser_ids = browser_ids.clone();
        let conversation = post(move |headers: HeaderMap, Json(payload): Json<Value>| {
            let conversation_log = conversation_log.clone();
            let conversation_parent_ids = conversation_parent_ids.clone();
            let conversation_browser_ids = conversation_browser_ids.clone();
            async move {
                assert_eq!(
                    headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer account-token")
                );
                assert_eq!(
                    headers
                        .get("OpenAI-Sentinel-Chat-Requirements-Token")
                        .and_then(|value| value.to_str().ok()),
                    Some("requirements-token")
                );
                assert_eq!(
                    headers
                        .get("OpenAI-Sentinel-SO-Token")
                        .and_then(|value| value.to_str().ok()),
                    Some("so-token")
                );
                let ids = conversation_browser_ids
                    .lock()
                    .await
                    .clone()
                    .expect("bootstrap browser ids");
                assert_eq!(
                    headers
                        .get("OAI-Device-Id")
                        .and_then(|value| value.to_str().ok()),
                    Some(ids.0.as_str())
                );
                assert_eq!(
                    headers
                        .get("OAI-Session-Id")
                        .and_then(|value| value.to_str().ok()),
                    Some(ids.1.as_str())
                );
                assert_eq!(
                    headers
                        .get("X-OpenAI-Target-Path")
                        .and_then(|value| value.to_str().ok()),
                    Some("/backend-api/conversation")
                );
                assert_eq!(
                    headers
                        .get("X-OpenAI-Target-Route")
                        .and_then(|value| value.to_str().ok()),
                    Some("/backend-api/conversation")
                );
                assert!(
                    headers
                        .get("OpenAI-Sentinel-Proof-Token")
                        .and_then(|value| value.to_str().ok())
                        .is_some_and(|value| value.starts_with("gAAAAAB"))
                );
                let expected_turnstile = base64::engine::general_purpose::STANDARD.encode(
                    native_requirements_token(
                        NATIVE_USER_AGENT,
                        &NativePowResources {
                            script_sources: vec!["https://chatgpt.com/c/test/_sdk.js".to_owned()],
                            data_build: "c/test/_".to_owned(),
                        },
                    )
                    .expect("legacy p")
                    .as_bytes(),
                );
                assert_eq!(
                    headers
                        .get("OpenAI-Sentinel-Turnstile-Token")
                        .and_then(|value| value.to_str().ok()),
                    Some(expected_turnstile.as_str())
                );
                assert_eq!(payload["action"], "next");
                assert_eq!(payload["messages"][0]["author"]["role"], "user");
                let parent_id = payload["parent_message_id"]
                    .as_str()
                    .expect("parent id")
                    .to_owned();
                conversation_parent_ids.lock().await.push(parent_id);
                conversation_log.lock().await.push("conversation");
                let prompt = payload["messages"][0]["content"]["parts"][0]
                    .as_str()
                    .unwrap_or_default();
                if prompt == "hold" {
                    let first = stream::once(async {
                        Ok::<Bytes, std::convert::Infallible>(Bytes::from(
                            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"first\"]}}}\n\n",
                        ))
                    });
                    let rest = stream::pending::<Result<Bytes, std::convert::Infallible>>();
                    return (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        Body::from_stream(first.chain(rest)),
                    )
                        .into_response();
                }
                let body = match prompt {
                    "empty" => "data: [DONE]\n\n",
                    "no-terminal" => {
                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"partial\"]}}}\n\n"
                    }
                    "partial" => "data: {\"message\":",
                    "patch" => concat!(
                        "data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"hello\"}\n\n",
                        "data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\" world\"}\n\n",
                        "data: [DONE]\n\n",
                    ),
                    "annotated" => concat!(
                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"Repo: ",
                        "\u{e200}url\u{e202}chatgpt2api\u{e202}https://example.test/repo\u{e201} done ",
                        "\u{e200}cite\u{e202}turn0search0\u{e201}.\"]}}}\n\n",
                        "data: [DONE]\n\n",
                    ),
                    _ => {
                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"hello\"]}}}\n\n\
                         data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"hello world\"]}}}\n\n\
                         data: [DONE]\n\n"
                    }
                };
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(body),
                )
                    .into_response()
            }
        });
        let anonymous_prepare =
            post(|| async { Json(json!({"prepare_token":"anonymous-prepare-token"})) });
        let anonymous_finalize =
            post(|| async { Json(json!({"token":"anonymous-requirements-token"})) });
        let anonymous_models =
            get(|| async { Json(json!({"models": [{"slug":"catalog-model"}]})) });
        let authenticated_models =
            get(|| async { Json(json!({"models": [{"slug":"catalog-model"}]})) });
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                upstream_listener,
                axum::Router::new()
                    .route("/", bootstrap)
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize)
                    .route("/backend-api/models", authenticated_models)
                    .route(
                        "/backend-api/codex/models",
                        get(|| async { Json(json!({"models": [{"slug": "catalog-model"}]})) }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/prepare",
                        anonymous_prepare,
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        anonymous_finalize,
                    )
                    .route("/backend-anon/models", anonymous_models)
                    .route("/backend-api/conversation", conversation),
            )
            .await
            .expect("native server");
        });
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-native-accounts-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"{"items":[{"access_token":"account-token","status":"正常","models":["gpt-test"]}]}"#
                .as_bytes(),
        )
        .expect("accounts snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{upstream_address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));

        let dropped_tool_call = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","messages":[{"role":"assistant","content":"visible","tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{}"}}]}]}"#,
                    ))
                    .expect("tool-call request"),
            )
            .await
            .expect("tool-call response");
        assert_eq!(dropped_tool_call.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert!(call_log.lock().await.is_empty());
        assert_eq!(state.account_store.inflight(), 0);

        let response = state
            .router()
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","messages":[{"role":"user","content":"hi"}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("completion json");
        assert_eq!(value["model"], "gpt-test");
        assert!(value["created"].as_i64().is_some());
        assert!(
            value["id"]
                .as_str()
                .is_some_and(|id| id.starts_with("chatcmpl-"))
        );
        assert_eq!(value["usage"]["prompt_tokens"], 8);
        assert_eq!(value["usage"]["completion_tokens"], 2);
        assert_eq!(value["usage"]["total_tokens"], 10);
        assert_eq!(value["choices"][0]["message"]["content"], "hello world");
        assert_eq!(state.account_store.inflight(), 0);
        assert_eq!(
            *call_log.lock().await,
            vec![
                "bootstrap",
                "bootstrap",
                "prepare",
                "finalize",
                "conversation"
            ]
        );

        let patch_response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","messages":[{"role":"user","content":"patch"}]}"#,
                    ))
                    .expect("patch request"),
            )
            .await
            .expect("patch response");
        assert_eq!(patch_response.status(), StatusCode::OK);
        let patch_body = patch_response
            .into_body()
            .collect()
            .await
            .expect("patch body")
            .to_bytes();
        let patch_value: Value = serde_json::from_slice(&patch_body).expect("patch completion");
        assert_eq!(
            patch_value["choices"][0]["message"]["content"],
            "hello world"
        );

        let annotated_response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","messages":[{"role":"user","content":"annotated"}]}"#,
                    ))
                    .expect("annotated request"),
            )
            .await
            .expect("annotated response");
        assert_eq!(annotated_response.status(), StatusCode::OK);
        let annotated_body = annotated_response
            .into_body()
            .collect()
            .await
            .expect("annotated body")
            .to_bytes();
        let annotated_text = String::from_utf8(annotated_body.to_vec()).expect("annotated json");
        let annotated_value: Value =
            serde_json::from_str(&annotated_text).expect("annotated completion");
        assert_eq!(
            annotated_value["choices"][0]["message"]["content"],
            "Repo: chatgpt2api (https://example.test/repo) done."
        );
        assert!(!annotated_text.contains('\u{e200}'));

        let empty_messages_fallback = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","messages":[],"prompt":"hi"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(empty_messages_fallback.status(), StatusCode::OK);

        let stream_request = || {
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer client")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"model":"gpt-test","stream":true,"prompt":"hi"}"#,
                ))
                .expect("request")
        };
        let response = state
            .router()
            .oneshot(stream_request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let stream_body = response
            .into_body()
            .collect()
            .await
            .expect("stream body")
            .to_bytes();
        let stream_text = String::from_utf8(stream_body.to_vec()).expect("stream text");
        assert!(
            stream_text.contains("hello") && stream_text.contains(" world"),
            "stream body: {stream_text:?}"
        );
        let stream_frames = stream_text
            .split("\n\n")
            .filter_map(|frame| frame.strip_prefix("data: "))
            .filter_map(|frame| serde_json::from_str::<Value>(frame).ok())
            .collect::<Vec<_>>();
        let role_indexes = stream_frames
            .iter()
            .enumerate()
            .filter(|(_, frame)| frame["choices"][0]["delta"]["role"] == "assistant")
            .map(|(index, _)| index)
            .collect::<Vec<_>>();
        assert_eq!(role_indexes.len(), 1);
        let first_content_index = stream_frames
            .iter()
            .position(|frame| {
                frame["choices"][0]["delta"]["content"]
                    .as_str()
                    .is_some_and(|content| !content.is_empty())
            })
            .expect("content frame");
        assert!(role_indexes[0] < first_content_index);
        assert!(stream_text.contains("\"model\":\"gpt-test\""));
        assert!(stream_text.contains("\"created\":"));
        assert!(stream_text.contains("\"id\":\"chatcmpl-"));
        assert!(stream_text.contains("\"finish_reason\":\"stop\""));
        assert!(stream_text.ends_with("data: [DONE]\n\n"));
        assert_eq!(state.account_store.inflight(), 0);

        let usage_response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","stream":true,"prompt":"hi","stream_options":{"include_usage":true}}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(usage_response.status(), StatusCode::OK);
        let usage_text = String::from_utf8(
            usage_response
                .into_body()
                .collect()
                .await
                .expect("usage stream body")
                .to_bytes()
                .to_vec(),
        )
        .expect("usage stream text");
        let usage_frame = usage_text
            .split("\n\n")
            .filter_map(|frame| frame.strip_prefix("data: "))
            .filter_map(|frame| serde_json::from_str::<Value>(frame).ok())
            .find(|frame| frame.get("choices") == Some(&json!([])))
            .expect("usage frame");
        let usage_frames = usage_text
            .split("\n\n")
            .filter_map(|frame| frame.strip_prefix("data: "))
            .filter_map(|frame| serde_json::from_str::<Value>(frame).ok())
            .collect::<Vec<_>>();
        for frame in usage_frames
            .iter()
            .filter(|frame| frame.get("choices") != Some(&json!([])))
        {
            assert_eq!(frame.get("usage"), Some(&Value::Null));
        }
        assert_eq!(usage_frame["usage"]["prompt_tokens"], 8);
        assert_eq!(usage_frame["usage"]["completion_tokens"], 2);
        assert_eq!(usage_frame["usage"]["total_tokens"], 10);

        let empty_response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","stream":true,"prompt":"empty"}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        let empty_text = String::from_utf8(
            empty_response
                .into_body()
                .collect()
                .await
                .expect("empty stream body")
                .to_bytes()
                .to_vec(),
        )
        .expect("empty stream text");
        let first_empty_frame = empty_text
            .strip_prefix("data: ")
            .and_then(|value| value.split("\n\n").next())
            .and_then(|value| serde_json::from_str::<Value>(value).ok())
            .expect("empty role frame");
        assert_eq!(
            first_empty_frame["choices"][0]["delta"]["role"],
            "assistant"
        );
        assert_eq!(first_empty_frame["choices"][0]["delta"]["content"], "");
        let empty_role_count = empty_text
            .split("\n\n")
            .filter_map(|frame| frame.strip_prefix("data: "))
            .filter_map(|frame| serde_json::from_str::<Value>(frame).ok())
            .filter(|frame| frame["choices"][0]["delta"]["role"] == "assistant")
            .count();
        assert_eq!(empty_role_count, 1);
        let parent_ids = parent_ids.lock().await.clone();
        assert!(parent_ids.len() >= 2);
        assert_ne!(parent_ids[0], parent_ids[1]);
        assert!(parent_ids.iter().all(|id| id != "client-created-root"));

        let hold_request = || {
            axum::http::Request::builder()
                .method("POST")
                .uri("/v1/chat/completions")
                .header(header::AUTHORIZATION, "Bearer client")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"model":"gpt-test","stream":true,"prompt":"hold"}"#,
                ))
                .expect("request")
        };
        let response = state
            .router()
            .oneshot(hold_request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let mut body = response.into_body().into_data_stream();
        let first = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .expect("first chunk timeout")
            .expect("first chunk")
            .expect("first chunk error");
        let first_frame = first
            .strip_prefix(b"data: ")
            .and_then(|value| value.strip_suffix(b"\n\n"))
            .and_then(|value| serde_json::from_slice::<Value>(value).ok())
            .expect("first role frame");
        assert_eq!(first_frame["choices"][0]["delta"]["role"], "assistant");
        let second = tokio::time::timeout(Duration::from_secs(1), body.next())
            .await
            .expect("second chunk timeout")
            .expect("second chunk")
            .expect("second chunk error");
        assert!(second.windows(5).any(|window| window == b"first"));
        drop(body);
        assert_eq!(state.account_store.inflight(), 0);

        let response = state
            .router()
            .oneshot(stream_request())
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(state.account_store.inflight(), 1);
        drop(response);
        assert_eq!(state.account_store.inflight(), 0);

        fs::remove_file(account_path).expect("cleanup");
        upstream_task.abort();
    }

    #[tokio::test]
    async fn anthropic_messages_uses_native_dispatcher_and_releases_lease() {
        let request_log = Arc::new(Mutex::new(Vec::<String>::new()));
        let log = request_log.clone();
        let bootstrap = get(move || {
            let log = log.clone();
            async move {
                log.lock().await.push("bootstrap".to_owned());
                (
                    StatusCode::OK,
                    Body::from(
                        r#"<html data-build="c/test/_build"><script src="https://chatgpt.com/c/test/_sdk.js"></script></html>"#,
                    ),
                )
                    .into_response()
            }
        });
        let models = get(|| async { Json(json!({"models":[{"slug":"gpt-test"}]})) });
        let prepare = post(|| async {
            Json(json!({"prepare_token":"prepare-token","proofofwork":{"required":false}}))
        });
        let finalize = post(|| async { Json(json!({"token":"requirements-token"})) });
        let log = request_log.clone();
        let conversation = post(move |Json(payload): Json<Value>| {
            let log = log.clone();
            async move {
                log.lock().await.push("conversation".to_owned());
                assert_eq!(
                    payload["messages"][0]["content"]["parts"][0],
                    "anthropic native"
                );
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(
                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"native answer\"]}}}\n\n\
                         data: [DONE]\n\n",
                    ),
                )
                    .into_response()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", bootstrap)
                    .route("/backend-api/models", models)
                    .route(
                        "/backend-api/codex/models",
                        get(|| async { Json(json!({"models": [{"slug": "gpt-test"}]})) }),
                    )
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize)
                    .route("/backend-api/conversation", conversation),
            )
            .await
            .expect("native server");
        });
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-anthropic-native-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"{"items":[{"access_token":"account-token","status":"正常","type":"free","source_type":"web","models":["gpt-test"]}]}"#,
        )
        .expect("accounts");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "client")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r##"{"model":"gpt-test","max_tokens":8,"messages":[{"role":"user","content":"anthropic native"}]}"##,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(status, StatusCode::OK);
        let value: Value = serde_json::from_slice(&body).expect("message");
        assert_eq!(value["content"][0]["text"], "native answer");
        assert!(
            request_log
                .lock()
                .await
                .iter()
                .any(|entry| entry == "conversation")
        );
        assert_eq!(state.account_store.inflight(), 0);
        state.account_type_catalog.shutdown().await;
        fs::remove_file(account_path).expect("cleanup");
        upstream_task.abort();
    }

    #[test]
    fn native_sse_requires_terminal_and_filters_non_visible_messages() {
        let visible =
            br#"data: {"message":{"author":{"role":"assistant"},"content":{"parts":["hello"]}}}

data: [DONE]

"#;
        assert_eq!(native_completion_text(visible).expect("terminal"), "hello");
        let crlf_with_tail = b"data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"crlf\"]}}}\r\n\r\ndata: [DONE]\r\n\r\ndata: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"must-not-leak\"]}}}\r\n\r\n";
        assert_eq!(
            native_completion_text(crlf_with_tail).expect("crlf terminal"),
            "crlf"
        );

        let missing_terminal =
            br#"data: {"message":{"author":{"role":"assistant"},"content":{"parts":["secret"]}}}

"#;
        assert!(native_completion_text(missing_terminal).is_err());
        assert!(native_completion_text(b"data: {\"message\":").is_err());

        let invisible = br#"data: {"message":{"author":{"role":"user"},"content":{"parts":["user text"]}}}

data: {"message":{"author":{"role":"assistant"},"recipient":"web","content":{"parts":["tool text"]}}}

data: {"message":{"author":{"role":"assistant"},"metadata":{"is_visually_hidden_from_conversation":true},"content":{"parts":["hidden text"]}}}

data: [DONE]

"#;
        assert_eq!(native_completion_text(invisible).expect("terminal"), "");
    }

    #[test]
    fn native_sse_uses_text_when_empty_parts_match_python_fallback() {
        let body = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[],\"text\":\"fallback text\"}}}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(body.as_bytes()).expect("terminal fallback"),
            "fallback text"
        );
    }

    #[test]
    fn native_sse_applies_python_conversation_text_patches() {
        let patches = concat!(
            "data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"hello\"}\n\n",
            "data: {\"o\":\"patch\",\"v\":[{\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\" \"},{\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"world\"}]}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(patches.as_bytes()).expect("patch terminal"),
            "hello world"
        );
        let no_path_append = concat!(
            "data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"hello\"}\n\n",
            "data: {\"v\":\" world\"}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(no_path_append.as_bytes()).expect("implicit patch terminal"),
            "hello world"
        );
        let nullable_message_content = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":null},\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"x\"}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(nullable_message_content.as_bytes())
                .expect("nullable content patch"),
            "x"
        );
        let malformed_message_content = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":\"canary\"},\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":\"x\"}\n\n",
            "data: [DONE]\n\n",
        );
        assert!(native_completion_text(malformed_message_content.as_bytes()).is_err());
        let replacement = concat!(
            "data: {\"p\":\"/message/content/parts/0\",\"o\":\"replace\",\"v\":\"replaced\"}\n\n",
            "data: [DONE]\n\n",
        );
        assert!(native_completion_text(replacement.as_bytes()).is_err());
        let malformed = concat!(
            "data: {\"p\":\"/message/content/parts/0\",\"o\":\"append\",\"v\":{\"canary\":true}}\n\n",
            "data: [DONE]\n\n",
        );
        assert!(native_completion_text(malformed.as_bytes()).is_err());
    }

    #[test]
    fn native_sse_sanitizes_python_annotation_markers() {
        let annotated = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"Repo: ",
            "\u{e200}url\u{e202}chatgpt2api\u{e202}https://example.test/repo\u{e201} done ",
            "\u{e200}cite\u{e202}turn0search0\u{e201}.\"]}}}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(annotated.as_bytes()).expect("annotation terminal"),
            "Repo: chatgpt2api (https://example.test/repo) done."
        );

        let internal_prefixes = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"x ",
            "\u{e200}cite\u{e202}turnfoo\u{e202}Readable\u{e201}. y ",
            "\u{e200}cite\u{e202}turntable\u{e202}Other\u{e201}.\"]}}}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(internal_prefixes.as_bytes()).expect("prefix terminal"),
            "x Readable. y Other."
        );

        let entity = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"The ",
            "\u{e200}entity\u{e202}Invincible\u{e201}.\"]}}}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(entity.as_bytes()).expect("entity terminal"),
            "The Invincible."
        );

        let unclosed = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"partial ",
            "\u{e200}cite\u{e202}turn0search0\"]}}}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(unclosed.as_bytes()).expect("unclosed terminal"),
            "partial "
        );

        let empty_citation = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"done ",
            "\u{e200}cite\u{e202}turn0search0\u{e201}.\"]}}}\n\n",
            "data: [DONE]\n\n",
        );
        assert_eq!(
            native_completion_text(empty_citation.as_bytes()).expect("empty cite terminal"),
            "done."
        );
    }

    #[tokio::test]
    async fn generic_stream_read_timeout_bounds_a_body_that_stops_after_first_chunk() {
        let input = stream::iter(vec![Ok::<Bytes, io::Error>(Bytes::from_static(b"first"))])
            .chain(stream::pending());
        let mut body = Box::pin(bounded_stream_with_timeout(
            input,
            Duration::from_millis(10),
        ));

        assert_eq!(
            body.next()
                .await
                .expect("first chunk")
                .expect("first chunk ok"),
            Bytes::from_static(b"first")
        );
        let next = tokio::time::timeout(Duration::from_millis(30), body.next())
            .await
            .expect("stream read must be bounded");
        assert!(matches!(next, Some(Err(_))));
    }

    #[tokio::test]
    async fn generic_stream_has_no_fixed_total_deadline() {
        let input = stream::unfold(0usize, |index| async move {
            if index == 5 {
                None
            } else {
                tokio::time::sleep(Duration::from_millis(5)).await;
                Some((Ok::<Bytes, io::Error>(Bytes::from_static(b"x")), index + 1))
            }
        });
        let mut body = Box::pin(bounded_stream_with_timeout(
            input,
            Duration::from_millis(20),
        ));
        let mut chunks = Vec::new();
        while let Some(item) = body.next().await {
            chunks.push(item.expect("chunk"));
        }
        assert_eq!(chunks.len(), 5);
    }

    #[test]
    fn native_payload_rejects_unimplemented_function_tools() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "prompt": "lookup",
            "tools": [{
                "type": "function",
                "function": {"name": "lookup", "parameters": {}}
            }],
            "tool_choice": "auto"
        }))
        .expect("validated chat payload");
        assert!(native_conversation_payload(&payload).is_err());
    }

    #[test]
    fn native_payload_rejects_tool_calls_instead_of_dropping_them() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "messages": [{
                "role": "assistant",
                "content": "visible text",
                "tool_calls": [{
                    "id": "call-1",
                    "type": "function",
                    "function": {"name": "lookup", "arguments": "{}"}
                }]
            }]
        }))
        .expect("validated chat payload");
        assert!(native_conversation_payload(&payload).is_err());
    }

    #[test]
    fn native_payload_rejects_developer_messages_without_codex_route() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "messages": [{"role": "developer", "content": "internal instruction"}]
        }))
        .expect("validated chat payload");
        // Python routes developer messages to Codex Responses. The Rust
        // canary has no Codex implementation, so it must not send them to the
        // ChatGPT conversation endpoint as if the contracts were equivalent.
        assert!(native_conversation_payload(&payload).is_err());
    }

    #[test]
    fn native_payload_preserves_normalized_reasoning_effort() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "prompt": "reason",
            "reasoning_effort": " HIGH "
        }))
        .expect("validated chat payload");
        let conversation = native_conversation_payload(&payload).expect("conversation payload");
        assert_eq!(conversation["thinking_effort"], "high");
    }

    #[test]
    fn native_payload_matches_python_conversation_control_fields() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "prompt": "hello"
        }))
        .expect("validated prompt");
        let conversation = native_conversation_payload(&payload).expect("conversation payload");

        assert_eq!(conversation["conversation_origin"], Value::Null);
        assert_eq!(conversation["force_paragen"], false);
        assert_eq!(conversation["force_paragen_model_slug"], "");
        assert_eq!(conversation["force_rate_limit"], false);
        assert_eq!(conversation["history_and_training_disabled"], true);
        assert_eq!(conversation["reset_rate_limits"], false);
        assert_eq!(conversation["suggestions"], json!([]));
        assert_eq!(conversation["system_hints"], json!([]));
        assert_eq!(conversation["timezone"], "Asia/Shanghai");
        assert_eq!(conversation["timezone_offset_min"], -480);
        assert_eq!(conversation["variant_purpose"], "comparison_implicit");
        assert!(
            conversation["websocket_request_id"]
                .as_str()
                .is_some_and(|value| value.len() == 36)
        );
        assert_eq!(
            conversation["client_contextual_info"],
            json!({
                "is_dark_mode": false,
                "time_since_loaded": 120,
                "page_height": 900,
                "page_width": 1400,
                "pixel_ratio": 2,
                "screen_height": 1440,
                "screen_width": 2560
            })
        );
    }

    #[test]
    fn native_payload_matches_python_empty_content_part() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "messages": [{"role": "user", "content": []}]
        }))
        .expect("validated empty content");
        let conversation = native_conversation_payload(&payload).expect("conversation payload");
        assert_eq!(conversation["messages"][0]["content"]["parts"], json!([""]));
    }

    #[test]
    fn native_payload_strips_prompt_like_python_chat_path() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "prompt": "  hello  "
        }))
        .expect("validated prompt");
        let conversation = native_conversation_payload(&payload).expect("conversation payload");
        assert_eq!(
            conversation["messages"][0]["content"]["parts"],
            json!(["hello"])
        );
    }

    #[test]
    fn native_payload_normalizes_nullable_assistant_content_like_python() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "messages": [{"role": "assistant", "content": null}]
        }))
        .expect("validated nullable assistant content");
        let conversation = native_conversation_payload(&payload).expect("conversation payload");
        assert_eq!(conversation["messages"][0]["content"]["parts"], json!([""]));
    }

    #[test]
    fn native_usage_trims_prompt_like_python_chat_path() {
        let payload = validate_chat_payload(json!({
            "model": "gpt-test",
            "prompt": "  hello  "
        }))
        .expect("validated prompt");
        let usage = native_usage(&payload, "").expect("usage");
        assert_eq!(usage["prompt_tokens"], 8);
    }

    #[test]
    fn native_usage_accepts_nullable_assistant_content_like_python() {
        let payload = validate_chat_payload(json!({
            "model": "gpt-test",
            "messages": [{"role": "assistant", "content": null}]
        }))
        .expect("validated nullable assistant content");
        let usage = native_usage(&payload, "").expect("usage");
        assert_eq!(usage["prompt_tokens"], 7);
    }

    #[test]
    fn native_payload_accepts_stream_usage_for_native_route() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "prompt": "usage",
            "stream": true,
            "stream_options": {"include_usage": true}
        }))
        .expect("validated chat payload");
        assert!(native_conversation_payload(&payload).is_ok());
    }

    #[test]
    fn native_nonstream_sse_uses_delimited_events_and_requires_complete_done() {
        let multiline = concat!(
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\r\n",
            "data:  \"content\":{\"parts\":[\"hello\"]}}}\r\n\r\n",
            "data: [DONE]\r\n\r\n",
        );
        assert_eq!(
            native_completion_text(multiline.as_bytes()).expect("multiline SSE"),
            "hello"
        );

        let done_with_tail = concat!(
            "data: [DONE]\n\n",
            "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"leak\"]}}}\n\n",
        );
        assert_eq!(
            native_completion_text(done_with_tail.as_bytes()).expect("terminal SSE"),
            ""
        );
        assert!(native_completion_text(b"data: [DONE]\n").is_err());
        assert!(native_completion_text(b"data: {\"message\":").is_err());
    }

    #[test]
    fn native_codex_sse_ignores_done_sentinel_before_explicit_completion() {
        let body = concat!(
            "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n",
            "data: [DONE]\n\n",
            "data: {\"type\":\"response.completed\"}\n\n",
        );
        assert_eq!(
            native_codex_text(body.as_bytes()).expect("Codex stream"),
            "ok"
        );
    }

    #[tokio::test]
    async fn native_codex_stream_done_without_completed_fails_closed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/",
                    get(|| async {
                        (
                            [(header::CONTENT_TYPE, "text/event-stream")],
                            Body::from(
                                "data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n\
                                 data: [DONE]\n\n",
                            ),
                        )
                    }),
                ),
            )
            .await
            .expect("server");
        });
        let response = reqwest::Client::new()
            .get(format!("http://{address}/"))
            .send()
            .await
            .expect("upstream response");
        let response =
            native_codex_stream_response(response, None, "test-model".to_owned(), false, 0);
        assert!(response.into_body().collect().await.is_err());
        server.abort();
        let _ = server.await;
    }

    #[tokio::test]
    async fn native_bootstrap_body_timeout_releases_account_lease() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                axum::Router::new().route(
                    "/",
                    get(|| async {
                        let first = stream::once(async {
                            Ok::<Bytes, std::convert::Infallible>(Bytes::from("<html>"))
                        });
                        let rest = stream::pending::<Result<Bytes, std::convert::Infallible>>();
                        (StatusCode::OK, Body::from_stream(first.chain(rest))).into_response()
                    }),
                ),
            )
            .await
            .expect("server");
        });
        let path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-timeout-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &path,
            r#"{"items":[{"access_token":"account-token","status":"正常"}]}"#.as_bytes(),
        )
        .expect("accounts snapshot");
        let store = AccountStore::load(Some(&path)).expect("account store");
        let lease = store.acquire("auto").await.expect("lease");
        let client = Client::builder().build().expect("client");
        let result = tokio::spawn(async move {
            let _lease = lease;
            native_bootstrap_with_timeout(
                &client,
                &format!("http://{address}"),
                "account-token",
                Duration::from_millis(20),
            )
            .await
        })
        .await
        .expect("task");
        assert!(result.is_err());
        assert_eq!(store.inflight(), 0);
        fs::remove_file(path).expect("cleanup");
        server.abort();
    }

    #[tokio::test]
    async fn native_requirements_body_timeout_releases_account_lease() {
        for hang_prepare in [true, false] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener");
            let address = listener.local_addr().expect("address");
            let server = tokio::spawn(async move {
                let prepare = post(move || async move {
                    if hang_prepare {
                        let first = stream::once(async {
                            Ok::<Bytes, std::convert::Infallible>(Bytes::from("{"))
                        });
                        let rest = stream::pending::<Result<Bytes, std::convert::Infallible>>();
                        (StatusCode::OK, Body::from_stream(first.chain(rest))).into_response()
                    } else {
                        Json(json!({"prepare_token": "prepare-token"})).into_response()
                    }
                });
                let finalize = post(|| async {
                    let first = stream::once(async {
                        Ok::<Bytes, std::convert::Infallible>(Bytes::from("{"))
                    });
                    let rest = stream::pending::<Result<Bytes, std::convert::Infallible>>();
                    (StatusCode::OK, Body::from_stream(first.chain(rest))).into_response()
                });
                axum::serve(
                    listener,
                    Router::new()
                        .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                        .route("/backend-api/sentinel/chat-requirements/finalize", finalize),
                )
                .await
                .expect("server");
            });

            let path = std::env::temp_dir().join(format!(
                "chatgpt2api-rust-requirements-timeout-{}-{}.json",
                std::process::id(),
                NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
            ));
            fs::write(
                &path,
                r#"{"items":[{"access_token":"account-token","status":"正常"}]}"#,
            )
            .expect("accounts snapshot");
            let store = AccountStore::load(Some(&path)).expect("account store");
            let lease = store.acquire("auto").await.expect("lease");
            let client = Client::builder().build().expect("client");
            let result = tokio::spawn(async move {
                let _lease = lease;
                native_chat_requirements_with_timeout(
                    &client,
                    &format!("http://{address}"),
                    "account-token",
                    Duration::from_millis(20),
                )
                .await
            })
            .await
            .expect("task");
            assert!(result.is_err());
            assert_eq!(store.inflight(), 0);
            fs::remove_file(path).expect("cleanup");
            server.abort();
        }
    }

    #[tokio::test]
    async fn native_requirements_send_timeout_uses_stage_budget() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let server = tokio::spawn(async move {
            let (_socket, _) = listener.accept().await.expect("accept");
            std::future::pending::<()>().await;
        });
        let client = Client::builder().build().expect("client");
        let result = tokio::time::timeout(
            Duration::from_millis(100),
            native_chat_requirements_with_timeout(
                &client,
                &format!("http://{address}"),
                "account-token",
                Duration::from_millis(20),
            ),
        )
        .await;
        assert!(matches!(result, Ok(Err(_))));
        server.abort();
    }

    async fn native_chat_failover_case(
        statuses: Vec<StatusCode>,
        second_account_model: &str,
    ) -> (StatusCode, usize, Vec<String>, usize) {
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let upstream_address = upstream_listener.local_addr().expect("address");
        let conversation_count = Arc::new(AtomicUsize::new(0));
        let conversation_tokens = Arc::new(Mutex::new(Vec::<String>::new()));
        let statuses_for_handler = statuses.clone();

        let bootstrap = get(|headers: HeaderMap| async move {
            if headers.get(header::AUTHORIZATION).is_none() {
                return (StatusCode::OK, Body::from("<html></html>")).into_response();
            }
            assert!(
                headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(|value| value.starts_with("Bearer "))
            );
            (StatusCode::OK, Body::from("<html></html>")).into_response()
        });
        let prepare = post(
            |headers: HeaderMap, Json(_payload): Json<Value>| async move {
                assert!(
                    headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .is_some_and(|value| value.starts_with("Bearer "))
                );
                Json(json!({"prepare_token":"prepare-token"})).into_response()
            },
        );
        let finalize = post(
            |headers: HeaderMap, Json(_payload): Json<Value>| async move {
                assert!(
                    headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .is_some_and(|value| value.starts_with("Bearer "))
                );
                Json(json!({"token":"requirements-token"})).into_response()
            },
        );
        let anonymous_prepare =
            post(|| async { Json(json!({"prepare_token":"anonymous-prepare-token"})) });
        let anonymous_finalize =
            post(|| async { Json(json!({"token":"anonymous-requirements-token"})) });
        let anonymous_models =
            get(|| async { Json(json!({"models": [{"slug":"catalog-model"}]})) });
        let authenticated_models =
            get(|| async { Json(json!({"models": [{"slug":"catalog-model"}]})) });
        let count_for_handler = conversation_count.clone();
        let tokens_for_handler = conversation_tokens.clone();
        let conversation = post(move |headers: HeaderMap, Json(_payload): Json<Value>| {
            let count_for_handler = count_for_handler.clone();
            let tokens_for_handler = tokens_for_handler.clone();
            let statuses_for_handler = statuses_for_handler.clone();
            async move {
                let token = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                tokens_for_handler.lock().await.push(token);
                let index = count_for_handler.fetch_add(1, Ordering::AcqRel);
                let status = statuses_for_handler
                    .get(index)
                    .copied()
                    .unwrap_or(StatusCode::BAD_GATEWAY);
                if !status.is_success() {
                    return status.into_response();
                }
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(
                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"ok\"]}}}\n\n\
                         data: [DONE]\n\n",
                    ),
                )
                    .into_response()
            }
        });
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                upstream_listener,
                Router::new()
                    .route("/", bootstrap)
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize)
                    .route("/backend-api/models", authenticated_models)
                    .route(
                        "/backend-api/codex/models",
                        get(|| async { Json(json!({"models": [{"slug": "catalog-model"}]})) }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/prepare",
                        anonymous_prepare,
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        anonymous_finalize,
                    )
                    .route("/backend-anon/models", anonymous_models)
                    .route("/backend-api/conversation", conversation),
            )
            .await
            .expect("native server");
        });

        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-native-failover-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            serde_json::to_vec(&json!({
                "items": [
                    {"access_token": "first-token", "status": "正常", "models": ["gpt-test"]},
                    {"access_token": "second-token", "status": "正常", "models": [second_account_model]}
                ]
            }))
            .expect("accounts json"),
        )
        .expect("accounts snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{upstream_address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));

        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"gpt-test","prompt":"hello"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        let result = (
            response.status(),
            conversation_count.load(Ordering::Acquire),
            conversation_tokens.lock().await.clone(),
            state.account_store.inflight(),
        );

        fs::remove_file(account_path).expect("cleanup");
        upstream_task.abort();
        result
    }

    #[derive(Clone, Copy)]
    enum NativeFailureStage {
        Bootstrap,
        Prepare,
        Finalize,
    }

    async fn native_chat_stage_failover_case(
        stage: NativeFailureStage,
        failure_status: StatusCode,
        failure_body: Option<&'static str>,
    ) -> (StatusCode, usize, Vec<String>, usize) {
        let upstream_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let upstream_address = upstream_listener.local_addr().expect("address");
        let conversation_count = Arc::new(AtomicUsize::new(0));
        let conversation_tokens = Arc::new(Mutex::new(Vec::<String>::new()));

        let bootstrap = get(move |headers: HeaderMap| async move {
            let is_first = headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                == Some("Bearer first-token");
            if is_first && matches!(stage, NativeFailureStage::Bootstrap) {
                if let Some(body) = failure_body {
                    return (StatusCode::OK, Body::from(body)).into_response();
                }
                return failure_status.into_response();
            }
            (StatusCode::OK, Body::from("<html></html>")).into_response()
        });
        let prepare = post(
            move |headers: HeaderMap, Json(_payload): Json<Value>| async move {
                let is_first = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    == Some("Bearer first-token");
                if is_first && matches!(stage, NativeFailureStage::Prepare) {
                    if let Some(body) = failure_body {
                        return (StatusCode::OK, Body::from(body)).into_response();
                    }
                    return failure_status.into_response();
                }
                Json(json!({"prepare_token":"prepare-token"})).into_response()
            },
        );
        let finalize = post(
            move |headers: HeaderMap, Json(_payload): Json<Value>| async move {
                let is_first = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    == Some("Bearer first-token");
                if is_first && matches!(stage, NativeFailureStage::Finalize) {
                    if let Some(body) = failure_body {
                        return (StatusCode::OK, Body::from(body)).into_response();
                    }
                    return failure_status.into_response();
                }
                Json(json!({"token":"requirements-token"})).into_response()
            },
        );
        let count_for_handler = conversation_count.clone();
        let tokens_for_handler = conversation_tokens.clone();
        let conversation = post(move |headers: HeaderMap, Json(_payload): Json<Value>| {
            let count_for_handler = count_for_handler.clone();
            let tokens_for_handler = tokens_for_handler.clone();
            async move {
                let token = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                tokens_for_handler.lock().await.push(token);
                count_for_handler.fetch_add(1, Ordering::AcqRel);
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(
                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"ok\"]}}}\n\n\
                         data: [DONE]\n\n",
                    ),
                )
                    .into_response()
            }
        });
        let authenticated_models =
            get(|| async { Json(json!({"models": [{"slug":"catalog-model"}]})) });
        let anonymous_models =
            get(|| async { Json(json!({"models": [{"slug":"catalog-model"}]})) });
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                upstream_listener,
                Router::new()
                    .route("/", bootstrap)
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize)
                    .route("/backend-api/models", authenticated_models)
                    .route(
                        "/backend-api/codex/models",
                        get(|| async { Json(json!({"models": [{"slug": "catalog-model"}]})) }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/prepare",
                        post(|| async { Json(json!({"prepare_token":"anonymous-prepare-token"})) }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        post(|| async { Json(json!({"token":"anonymous-requirements-token"})) }),
                    )
                    .route("/backend-anon/models", anonymous_models)
                    .route("/backend-api/conversation", conversation),
            )
            .await
            .expect("native server");
        });

        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-native-stage-failover-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            serde_json::to_vec(&json!({
                "items": [
                    {"access_token": "first-token", "status": "正常", "models": ["gpt-test"]},
                    {"access_token": "second-token", "status": "正常", "models": ["gpt-test"]}
                ]
            }))
            .expect("accounts json"),
        )
        .expect("accounts snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{upstream_address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"gpt-test","prompt":"hello"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        let result = (
            response.status(),
            conversation_count.load(Ordering::Acquire),
            conversation_tokens.lock().await.clone(),
            state.account_store.inflight(),
        );

        fs::remove_file(account_path).expect("cleanup");
        upstream_task.abort();
        result
    }

    #[tokio::test]
    async fn native_chat_rejects_unsupported_model_after_catalog_is_ready() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-model-error-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"catalog-token","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let bootstrap = get(|| async { Html("<html></html>") });
        let prepare = post(|| async { Json(json!({"prepare_token":"prepare-token"})) });
        let finalize = post(|| async { Json(json!({"token":"requirements-token"})) });
        let models = get(|| async {
            Json(json!({
                "models": [{"slug":"known-model"}]
            }))
        });
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", bootstrap)
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        prepare.clone(),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/finalize",
                        finalize.clone(),
                    )
                    .route("/backend-api/models", models.clone())
                    .route(
                        "/backend-api/codex/models",
                        get(|| async { Json(json!({"models": [{"slug": "known-model"}]})) }),
                    )
                    .route("/backend-anon/sentinel/chat-requirements/prepare", prepare)
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        finalize,
                    )
                    .route("/backend-anon/models", models),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"unknown-model","prompt":"hello"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(value["error"]["code"], "model_not_found");

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_chat_reports_pending_model_catalog_as_retryable() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-model-pending-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"catalog-token","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/",
                    get(|| async { StatusCode::BAD_GATEWAY.into_response() }),
                ),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            state.router().oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"warming-model","prompt":"hello"}"#))
                    .expect("request"),
            ),
        )
        .await
        .expect("pending route must remain bounded")
        .expect("response");
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[header::RETRY_AFTER], "5");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("error json");
        assert_eq!(value["error"]["code"], "model_catalog_pending");

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_chat_uses_ready_anonymous_model_without_waiting_for_refresh() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-anonymous-ready-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"account-token","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");

        let model_fetch_started = Arc::new(Notify::new());
        let model_fetch_release = Arc::new(Notify::new());
        let prepare_calls = Arc::new(AtomicUsize::new(0));
        let finalize_calls = Arc::new(AtomicUsize::new(0));
        let conversation_calls = Arc::new(AtomicUsize::new(0));
        let upstream_ready = Arc::new(Notify::new());
        let upstream_ready_for_task = upstream_ready.clone();
        let started_for_upstream = model_fetch_started.clone();
        let release_for_upstream = model_fetch_release.clone();
        let started_for_authenticated = model_fetch_started.clone();
        let release_for_authenticated = model_fetch_release.clone();
        let prepare_calls_for_upstream = prepare_calls.clone();
        let finalize_calls_for_upstream = finalize_calls.clone();
        let prepare_calls_for_authenticated = prepare_calls.clone();
        let finalize_calls_for_authenticated = finalize_calls.clone();
        let conversation_calls_for_upstream = conversation_calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream = tokio::spawn(async move {
            upstream_ready_for_task.notify_one();
            axum::serve(
                listener,
                Router::new()
                    .route("/", get(|| async { Html("<html></html>") }))
                    .route(
                        "/backend-anon/models",
                        get(move || {
                            let started = started_for_upstream.clone();
                            let release = release_for_upstream.clone();
                            async move {
                                started.notify_one();
                                release.notified().await;
                                Json(json!({"models": [{"slug": "new-anonymous-model"}]}))
                            }
                        }),
                    )
                    .route(
                        "/backend-api/models",
                        get(move || {
                            let started = started_for_authenticated.clone();
                            let release = release_for_authenticated.clone();
                            async move {
                                started.notify_one();
                                release.notified().await;
                                Json(json!({"models": [{"slug": "account-model"}]}))
                            }
                        }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/prepare",
                        post(move || {
                            let calls = prepare_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                Json(json!({"prepare_token": "prepare-token"}))
                            }
                        }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        post(move || {
                            let calls = finalize_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                Json(json!({"token": "requirements-token"}))
                            }
                        }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        post(move || {
                            let calls = prepare_calls_for_authenticated.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                Json(json!({"prepare_token": "prepare-token"}))
                            }
                        }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/finalize",
                        post(move || {
                            let calls = finalize_calls_for_authenticated.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                Json(json!({"token": "requirements-token"}))
                            }
                        }),
                    )
                    .route(
                        "/backend-anon/conversation",
                        post(move || {
                            let calls = conversation_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                (
                                    [(header::CONTENT_TYPE, "text/event-stream")],
                                    Body::from(
                                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"ready anonymous\"]}}}\n\n\
                                         data: [DONE]\n\n",
                                    ),
                                )
                                    .into_response()
                            }
                        }),
                    ),
            )
            .await
            .expect("upstream server");
        });
        upstream_ready.notified().await;

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        {
            let mut snapshot = state
                .account_type_catalog
                .snapshot
                .write()
                .expect("catalog snapshot lock");
            snapshot.anonymous_models = Arc::new(vec![PublicModel {
                id: "anonymous-model".to_owned(),
                object: "model",
                created: 0,
                owned_by: "chatgpt".to_owned(),
                permission: Vec::new(),
                root: "anonymous-model".to_owned(),
                parent: None,
                allow_anonymous: true,
                supported_account_types: Vec::new(),
                supported_reasoning_efforts: Vec::new(),
            }]);
            snapshot.anonymous_ready = true;
            snapshot.anonymous_expires_at = Instant::now() + Duration::from_secs(60);
            snapshot.anonymous_retry_at = Instant::now();
        }

        let response = tokio::time::timeout(
            Duration::from_millis(500),
            state.router().oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"anonymous-model","prompt":"hello"}"#,
                    ))
                    .expect("request"),
            ),
        )
        .await
        .expect("ready anonymous route must not wait for refresh")
        .expect("response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(status, StatusCode::OK, "body: {body:?}");
        assert!(String::from_utf8_lossy(&body).contains("ready anonymous"));
        assert_eq!(conversation_calls.load(Ordering::SeqCst), 1);
        tokio::time::timeout(Duration::from_millis(500), model_fetch_started.notified())
            .await
            .expect("refresh owner");

        state.account_type_catalog.shutdown().await;
        model_fetch_release.notify_waiters();
        upstream.abort();
        let _ = upstream.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_chat_routes_anonymous_catalog_models_without_an_account() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-anonymous-chat-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&account_path, "[]").expect("empty account snapshot");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let bootstrap = get(|| async { Html("<html></html>") });
        let prepare = post(|| async { Json(json!({"prepare_token":"prepare-token"})) });
        let finalize = post(|| async { Json(json!({"token":"requirements-token"})) });
        let models = get(|| async {
            Json(json!({
                "models": [{"slug":"anonymous-model"}]
            }))
        });
        let conversation = post(|| async {
            (
                [(header::CONTENT_TYPE, "text/event-stream")],
                Body::from(
                    "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"anonymous ok\"]}}}\n\n\
                     data: [DONE]\n\n",
                ),
            )
                .into_response()
        });
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", bootstrap)
                    .route("/backend-anon/sentinel/chat-requirements/prepare", prepare)
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        finalize,
                    )
                    .route("/backend-anon/models", models)
                    .route("/backend-anon/conversation", conversation),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            state.router().oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"anonymous-model","prompt":"hello"}"#,
                    ))
                    .expect("request"),
            ),
        )
        .await
        .expect("anonymous route must remain bounded")
        .expect("response");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        assert_eq!(status, StatusCode::OK, "anonymous route body: {body:?}");
        let value: Value = serde_json::from_slice(&body).expect("completion json");
        assert_eq!(value["choices"][0]["message"]["content"], "anonymous ok");

        let auto_response = tokio::time::timeout(
            Duration::from_secs(2),
            state.router().oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"hello"}"#))
                    .expect("auto request"),
            ),
        )
        .await
        .expect("anonymous auto route must remain bounded")
        .expect("auto response");
        assert_eq!(auto_response.status(), StatusCode::OK);
        let auto_body = auto_response
            .into_body()
            .collect()
            .await
            .expect("auto body")
            .to_bytes();
        let auto_value: Value = serde_json::from_slice(&auto_body).expect("auto completion json");
        assert_eq!(
            auto_value["choices"][0]["message"]["content"],
            "anonymous ok"
        );

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_chat_rejects_auto_without_catalog_ownership_before_upstream() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-auto-catalog-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&account_path, "[]").expect("empty account snapshot");

        let conversation_calls = Arc::new(AtomicUsize::new(0));
        let conversation_calls_for_upstream = conversation_calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let bootstrap = get(|| async { Html("<html></html>") });
        let prepare = post(|| async { Json(json!({"prepare_token":"prepare-token"})) });
        let finalize = post(|| async { Json(json!({"token":"requirements-token"})) });
        let models = get(|| async { Json(json!({"models": []})) });
        let conversation = post(move || {
            let calls = conversation_calls_for_upstream.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(
                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"unexpected\"]}}}\n\n\
                         data: [DONE]\n\n",
                    ),
                )
                    .into_response()
            }
        });
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", bootstrap)
                    .route("/backend-anon/sentinel/chat-requirements/prepare", prepare)
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        finalize,
                    )
                    .route("/backend-anon/models", models)
                    .route("/backend-anon/conversation", conversation),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            state.router().oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"auto","prompt":"hello"}"#))
                    .expect("request"),
            ),
        )
        .await
        .expect("auto route must remain bounded")
        .expect("response");
        assert!(matches!(
            response.status(),
            StatusCode::BAD_REQUEST | StatusCode::SERVICE_UNAVAILABLE
        ));
        assert_eq!(conversation_calls.load(Ordering::SeqCst), 0);

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_chat_retries_transient_bootstrap_failure_with_next_account() {
        let result = native_chat_stage_failover_case(
            NativeFailureStage::Bootstrap,
            StatusCode::BAD_GATEWAY,
            None,
        )
        .await;
        assert_eq!(result.0, StatusCode::OK);
        assert_eq!(result.1, 1);
        assert_eq!(result.2, ["Bearer second-token"]);
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_chat_retries_transient_prepare_failure_with_next_account() {
        let result = native_chat_stage_failover_case(
            NativeFailureStage::Prepare,
            StatusCode::TOO_MANY_REQUESTS,
            None,
        )
        .await;
        assert_eq!(result.0, StatusCode::OK);
        assert_eq!(result.1, 1);
        assert_eq!(result.2, ["Bearer second-token"]);
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_chat_retries_transient_finalize_failure_with_next_account() {
        let result = native_chat_stage_failover_case(
            NativeFailureStage::Finalize,
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
        )
        .await;
        assert_eq!(result.0, StatusCode::OK);
        assert_eq!(result.1, 1);
        assert_eq!(result.2, ["Bearer second-token"]);
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_chat_does_not_fail_over_bad_request_in_requirements_stage() {
        let result = native_chat_stage_failover_case(
            NativeFailureStage::Prepare,
            StatusCode::BAD_REQUEST,
            None,
        )
        .await;
        assert_eq!(result.0, StatusCode::BAD_GATEWAY);
        assert_eq!(result.1, 0);
        assert!(result.2.is_empty());
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_chat_does_not_fail_over_malformed_requirements_body() {
        let result =
            native_chat_stage_failover_case(NativeFailureStage::Prepare, StatusCode::OK, Some("{"))
                .await;
        assert_eq!(result.0, StatusCode::BAD_GATEWAY);
        assert_eq!(result.1, 0);
        assert!(result.2.is_empty());
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_models_catalog_uses_one_representative_per_source_capability() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-native-models-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"web-token","status":"正常","type":"Pro","source_type":"web"},
                {"access_token":"codex-token","status":"正常","type":"Pro","source_type":"codex","chatgpt_account_id":"codex-account"},
                {"access_token":"codex-no-id","status":"正常","type":"Plus","source_type":"codex"}
            ]"#,
        )
        .expect("account snapshot");

        let paths = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");

        let root_paths = paths.clone();
        let prepare_paths = paths.clone();
        let finalize_paths = paths.clone();
        let auth_model_paths = paths.clone();
        let anon_prepare_paths = paths.clone();
        let anon_finalize_paths = paths.clone();
        let anon_model_paths = paths.clone();
        let auth_model_authorizations = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let auth_model_authorizations_for_upstream = auth_model_authorizations.clone();
        let codex_model_paths = paths.clone();
        let codex_model_calls = Arc::new(AtomicUsize::new(0));
        let codex_model_calls_for_upstream = codex_model_calls.clone();
        let codex_chat_calls = Arc::new(AtomicUsize::new(0));
        let codex_chat_calls_for_upstream = codex_chat_calls.clone();
        let conversation_calls = Arc::new(AtomicUsize::new(0));
        let conversation_calls_for_upstream = conversation_calls.clone();
        let upstream_ready = Arc::new(Notify::new());
        let upstream_ready_for_task = upstream_ready.clone();
        let upstream_task = tokio::spawn(async move {
            upstream_ready_for_task.notify_one();
            async fn record(
                request: axum::extract::Request,
                paths: Arc<std::sync::Mutex<Vec<String>>>,
            ) {
                paths
                    .lock()
                    .expect("path lock")
                    .push(request.uri().to_string());
            }

            axum::serve(
                listener,
                Router::new()
                    .route(
                        "/",
                        get(move |request: axum::extract::Request| {
                            let paths = root_paths.clone();
                            async move {
                                record(request, paths).await;
                                Html("<html></html>")
                            }
                        }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        post(move |request: axum::extract::Request| {
                            let paths = prepare_paths.clone();
                            async move {
                                record(request, paths).await;
                                Json(json!({
                                    "prepare_token": "prepare-token"
                                }))
                                    .into_response()
                            }
                        }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/finalize",
                        post(move |request: axum::extract::Request| {
                            let paths = finalize_paths.clone();
                            async move {
                                record(request, paths).await;
                                Json(json!({"token":"requirements-token"}))
                            }
                        }),
                    )
                    .route(
                        "/backend-api/models",
                        get(move |request: axum::extract::Request| {
                            let paths = auth_model_paths.clone();
                            async move {
                                assert_eq!(
                                    request
                                        .headers()
                                        .get("X-OpenAI-Target-Path")
                                        .and_then(|value| value.to_str().ok()),
                                    Some("/backend-api/models")
                                );
                                assert_eq!(
                                    request
                                        .headers()
                                        .get("X-OpenAI-Target-Route")
                                        .and_then(|value| value.to_str().ok()),
                                    Some("/backend-api/models")
                                );
                                let authorization = request
                                    .headers()
                                    .get(header::AUTHORIZATION)
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_owned);
                                assert!(authorization
                                    .as_deref()
                                    .is_some_and(|value| value.starts_with("Bearer ")));
                                let account_id = request
                                    .headers()
                                    .get("ChatGPT-Account-ID")
                                    .and_then(|value| value.to_str().ok());
                                if authorization.as_deref() == Some("Bearer codex-token") {
                                    assert_eq!(account_id, Some("codex-account"));
                                } else {
                                    assert!(account_id.is_none());
                                }
                                auth_model_authorizations_for_upstream
                                    .lock()
                                    .expect("auth model calls lock")
                                    .push(authorization.clone().expect("authorization"));
                                record(request, paths).await;
                                let models = if authorization.as_deref() == Some("Bearer web-token") {
                                    json!({"models":[
                                        {"slug":"web-only-model","owned_by":"chatgpt"},
                                        {"slug":"collision-model","owned_by":"chatgpt"}
                                    ]})
                                } else {
                                    json!({"models":[{"slug":"plus-auth-model","owned_by":"chatgpt"}]})
                                };
                                Json(models).into_response()
                            }
                        }),
                    )
                    .route(
                        "/backend-api/codex/models",
                        get(move |request: axum::extract::Request| {
                            let paths = codex_model_paths.clone();
                            let calls = codex_model_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                let authorization = request
                                    .headers()
                                    .get(header::AUTHORIZATION)
                                    .and_then(|value| value.to_str().ok())
                                    .map(str::to_owned);
                                let Some(authorization) = authorization.as_deref() else {
                                    return StatusCode::BAD_REQUEST.into_response();
                                };
                                assert_eq!(
                                    request
                                        .headers()
                                        .get(header::ACCEPT)
                                        .and_then(|value| value.to_str().ok()),
                                    Some("application/json")
                                );
                                assert_eq!(
                                    request
                                        .headers()
                                        .get("originator")
                                        .and_then(|value| value.to_str().ok()),
                                    Some("codex_cli_rs")
                                );
                                if authorization == "Bearer codex-token" {
                                    assert_eq!(
                                        request
                                            .headers()
                                            .get("ChatGPT-Account-ID")
                                            .and_then(|value| value.to_str().ok()),
                                        Some("codex-account")
                                    );
                                } else if authorization == "Bearer web-token" {
                                    assert!(request.headers().get("ChatGPT-Account-ID").is_none());
                                } else {
                                    assert_eq!(authorization, "Bearer codex-no-id");
                                    assert!(request.headers().get("ChatGPT-Account-ID").is_none());
                                }
                                assert!(request
                                    .uri()
                                    .query()
                                    .is_some_and(|query| query.starts_with("client_version=")));
                                record(request, paths).await;
                                let slug = if authorization == "Bearer web-token" {
                                    "codex-only-model"
                                } else if authorization == "Bearer codex-token" {
                                    "collision-model"
                                } else {
                                    "plus-only-model"
                                };
                                Json(json!({
                                    "models":[{
                                        "slug":slug,
                                        "visibility":"list",
                                        "supported_in_api":true
                                    }]
                                }))
                                    .into_response()
                            }
                        }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/prepare",
                        post(move |request: axum::extract::Request| {
                            let paths = anon_prepare_paths.clone();
                            async move {
                                record(request, paths).await;
                                Json(json!({
                                    "prepare_token": "anon-prepare-token"
                                }))
                            }
                        }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        post(move |request: axum::extract::Request| {
                            let paths = anon_finalize_paths.clone();
                            async move {
                                record(request, paths).await;
                                Json(json!({"token":"anon-requirements-token"}))
                            }
                        }),
                    )
                    .route(
                        "/backend-anon/models",
                        get(move |request: axum::extract::Request| {
                            let paths = anon_model_paths.clone();
                            async move {
                                assert_eq!(
                                    request
                                        .headers()
                                        .get("X-OpenAI-Target-Path")
                                        .and_then(|value| value.to_str().ok()),
                                    Some("/backend-anon/models")
                                );
                                assert_eq!(
                                    request
                                        .headers()
                                        .get("X-OpenAI-Target-Route")
                                        .and_then(|value| value.to_str().ok()),
                                    Some("/backend-anon/models")
                                );
                                record(request, paths).await;
                                Json(json!({"models":[{"slug":"native-anon-model"}]}))
                            }
                        }),
                    )
                    .route(
                        "/backend-api/conversation",
                        post(move |_request: axum::extract::Request| {
                            let calls = conversation_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                StatusCode::BAD_GATEWAY
                            }
                        }),
                    )
                    .route(
                        "/backend-api/codex/responses",
                        post(move |request: axum::extract::Request| {
                            let calls = codex_chat_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                assert_eq!(
                                    request
                                        .headers()
                                        .get(header::AUTHORIZATION)
                                        .and_then(|value| value.to_str().ok()),
                                    Some("Bearer codex-token")
                                );
                                assert_eq!(
                                    request
                                        .headers()
                                        .get("ChatGPT-Account-ID")
                                        .and_then(|value| value.to_str().ok()),
                                    Some("codex-account")
                                );
                                (
                                    [(header::CONTENT_TYPE, "text/event-stream")],
                                    Body::from(
                                        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"codex ok\"}\n\n\
                                         data: [DONE]\n\n\
                                         data: {\"type\":\"response.completed\"}\n\n",
                                    ),
                                )
                                    .into_response()
                            }
                        }),
                    ),
            )
            .await
            .expect("upstream server");
        });
        upstream_ready.notified().await;

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["collision-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        state.account_store.cursor.store(1, Ordering::Release);
        let cold_chat = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"collision-model","messages":[{"role":"user","content":"hi"}]}"#,
                    ))
                    .expect("cold chat request"),
            )
            .await
            .expect("cold chat response");
        let cold_status = cold_chat.status();
        assert_eq!(cold_status, StatusCode::OK);
        assert_eq!(codex_chat_calls.load(Ordering::SeqCst), 1);
        assert_eq!(conversation_calls.load(Ordering::SeqCst), 0);
        let auth_model_calls = auth_model_authorizations
            .lock()
            .expect("auth model calls lock")
            .clone();
        let mut auth_model_calls = auth_model_calls;
        auth_model_calls.sort();
        assert_eq!(auth_model_calls, vec!["Bearer web-token".to_owned()]);
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_secs(1))
            .await;
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        let ids = value["data"]
            .as_array()
            .expect("model data")
            .iter()
            .filter_map(|item| item["id"].as_str())
            .collect::<HashSet<_>>();
        assert!(ids.contains("native-anon-model"));
        assert!(ids.contains("web-only-model"));
        assert!(ids.contains("collision-model"));
        assert!(ids.contains("plus-only-model"));
        assert!(ids.contains("collision-model"));
        assert_eq!(
            state
                .account_type_catalog
                .supported_types_for("web-only-model")
                .expect("web group"),
            HashSet::from(["pro".to_owned()])
        );
        assert_eq!(
            state
                .account_type_catalog
                .supported_types_for("collision-model")
                .expect("codex group"),
            HashSet::from(["pro".to_owned()])
        );
        assert_eq!(
            state
                .account_type_catalog
                .supported_types_for("plus-only-model")
                .expect("plus group"),
            HashSet::from(["plus".to_owned()])
        );
        assert_eq!(codex_model_calls.load(Ordering::SeqCst), 2);
        let codex_paths = paths
            .lock()
            .expect("path lock")
            .iter()
            .filter(|path| path.starts_with("/backend-api/codex/models?"))
            .count();
        assert_eq!(codex_paths, 2);
        assert_eq!(
            state
                .account_type_catalog
                .live_tokens_for("web-only-model")
                .expect("web live tokens"),
            HashSet::from(["web-token".to_owned(), "codex-token".to_owned()])
        );
        assert_eq!(
            state
                .account_type_catalog
                .live_tokens_for("collision-model")
                .expect("shared type live tokens"),
            HashSet::from(["web-token".to_owned(), "codex-token".to_owned(),])
        );
        state.account_store.cursor.store(1, Ordering::Release);
        let chat = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"collision-model","messages":[{"role":"user","content":"hi"}]}"#,
                    ))
                    .expect("chat request"),
            )
            .await
            .expect("chat response");
        assert_eq!(chat.status(), StatusCode::OK);
        let chat_body = chat
            .into_body()
            .collect()
            .await
            .expect("chat body")
            .to_bytes();
        let chat_value: Value = serde_json::from_slice(&chat_body).expect("chat json");
        assert_eq!(chat_value["choices"][0]["message"]["content"], "codex ok");
        assert_eq!(codex_chat_calls.load(Ordering::SeqCst), 2);
        assert_eq!(conversation_calls.load(Ordering::SeqCst), 0);
        state.account_store.cursor.store(1, Ordering::Release);
        let codex_groups = state
            .account_type_catalog
            .supported_types_for("collision-model")
            .expect("codex route groups");
        let codex_lease = state
            .account_store
            .acquire_excluding_with_type_filter(
                "collision-model",
                &HashSet::new(),
                Some(&codex_groups),
            )
            .await
            .expect("codex route lease");
        assert_eq!(codex_lease.token(), "codex-token");
        assert_eq!(codex_lease.source_type(), "codex");
        assert_eq!(codex_lease.account_type(), "pro");
        assert_eq!(codex_lease.chatgpt_account_id(), Some("codex-account"));
        drop(codex_lease);

        state.account_store.cursor.store(1, Ordering::Release);
        let chat_stream = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"collision-model","stream":true,"messages":[{"role":"user","content":"hi"}]}"#,
                    ))
                    .expect("stream request"),
            )
            .await
            .expect("stream response");
        assert_eq!(chat_stream.status(), StatusCode::OK);
        let stream_body = chat_stream
            .into_body()
            .collect()
            .await
            .expect("stream body")
            .to_bytes();
        let stream_text = String::from_utf8(stream_body.to_vec()).expect("stream utf8");
        assert!(stream_text.contains("\"role\":\"assistant\""));
        assert!(stream_text.contains("codex ok"));
        assert!(stream_text.ends_with("data: [DONE]\n\n"));
        assert_eq!(stream_text.matches("data: [DONE]\n\n").count(), 1);
        assert_eq!(codex_chat_calls.load(Ordering::SeqCst), 3);
        assert_eq!(conversation_calls.load(Ordering::SeqCst), 0);
        assert_eq!(state.account_store.inflight(), 0);

        let unknown = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"unknown-dynamic-model","messages":[{"role":"user","content":"hi"}]}"#,
                    ))
                    .expect("unknown request"),
            )
            .await
            .expect("unknown response");
        assert!(matches!(
            unknown.status(),
            StatusCode::BAD_REQUEST | StatusCode::SERVICE_UNAVAILABLE
        ));
        assert_eq!(codex_chat_calls.load(Ordering::SeqCst), 3);
        assert_eq!(conversation_calls.load(Ordering::SeqCst), 0);

        let paths = paths.lock().expect("path lock").clone();
        assert!(
            paths.contains(&"/backend-api/models?history_and_training_disabled=false".to_owned())
        );
        assert_eq!(
            paths
                .iter()
                .filter(|path| path.starts_with("/backend-api/codex/models?client_version="))
                .count(),
            2
        );
        assert!(paths.contains(&"/backend-anon/models?iim=false&is_gizmo=false".to_owned()));
        assert!(
            !paths
                .iter()
                .any(|path| path.starts_with("/backend-anon/sentinel/"))
        );

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    async fn assert_web_catalog_uses_only_web_endpoint() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-web-source-catalog-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"representative","status":"正常","type":"Pro","source_type":"web"}]"#,
        )
        .expect("account snapshot");

        let web_calls = Arc::new(AtomicUsize::new(0));
        let codex_calls = Arc::new(AtomicUsize::new(0));
        let web_calls_for_upstream = web_calls.clone();
        let codex_calls_for_upstream = codex_calls.clone();
        let upstream_ready = Arc::new(Notify::new());
        let upstream_ready_for_task = upstream_ready.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            upstream_ready_for_task.notify_one();
            axum::serve(
                listener,
                Router::new()
                    .route("/", get(|| async { Html("<html></html>") }))
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        post(|| async { Json(json!({"prepare_token":"prepare-token"})) }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/finalize",
                        post(|| async { Json(json!({"token":"requirements-token"})) }),
                    )
                    .route(
                        "/backend-api/models",
                        get(move || {
                            let calls = web_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                Json(json!({"models":[{"slug":"web-model"}]}))
                            }
                        }),
                    )
                    .route(
                        "/backend-api/codex/models",
                        get(move || {
                            let calls = codex_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                Json(json!({"models":[{"slug":"must-not-be-requested","supported_in_api":true}]}))
                            }
                        }),
                    ),
            )
            .await
            .expect("upstream server");
        });
        upstream_ready.notified().await;

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let candidates = vec![CatalogAccountCandidate {
            token: "representative".to_owned(),
            source_type: "web".to_owned(),
            chatgpt_account_id: None,
        }];
        let catalog = state.account_type_catalog.clone();
        let fetch = tokio::spawn(async move {
            catalog
                .fetch_account_type_models(
                    &"pro".to_owned(),
                    &candidates,
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        });
        let result = fetch.await.expect("fetch task").expect("catalog result");
        assert_eq!(
            result
                .0
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            vec!["web-model"]
        );
        assert!(result.2);
        assert_eq!(web_calls.load(Ordering::SeqCst), 1);
        assert_eq!(codex_calls.load(Ordering::SeqCst), 0);

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_catalog_web_representative_uses_only_web_endpoint() {
        assert_web_catalog_uses_only_web_endpoint().await;
    }

    #[tokio::test]
    async fn native_catalog_codex_representative_uses_only_codex_endpoint() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-codex-source-catalog-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"representative","status":"正常","type":"Pro","source_type":"codex"}]"#,
        )
        .expect("account snapshot");
        let web_finished = Arc::new(Notify::new());
        let codex_started = Arc::new(Notify::new());
        let release_codex = Arc::new(Notify::new());
        let web_finished_for_upstream = web_finished.clone();
        let codex_started_for_upstream = codex_started.clone();
        let release_codex_for_upstream = release_codex.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", get(|| async { Html("<html></html>") }))
                    .route("/backend-api/models", get(move || {
                        let finished = web_finished_for_upstream.clone();
                        async move {
                            finished.notify_one();
                            Json(json!({"models":[{"slug":"web-fast-model"}]}))
                        }
                    }))
                    .route("/backend-api/codex/models", get(move || {
                        let started = codex_started_for_upstream.clone();
                        let release = release_codex_for_upstream.clone();
                        async move {
                            started.notify_one();
                            release.notified().await;
                            Json(json!({"models":[{"slug":"codex-slow-model","supported_in_api":true}]}))
                        }
                    })),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let candidates = vec![CatalogAccountCandidate {
            token: "representative".to_owned(),
            source_type: "codex".to_owned(),
            chatgpt_account_id: None,
        }];
        let catalog = state.account_type_catalog.clone();
        let fetch = tokio::spawn(async move {
            catalog
                .fetch_account_type_models(
                    &"pro".to_owned(),
                    &candidates,
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        });
        codex_started.notified().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), web_finished.notified())
                .await
                .is_err(),
            "Codex representatives must not call the Web endpoint"
        );
        assert!(!fetch.is_finished());
        release_codex.notify_one();
        let result = fetch.await.expect("fetch task").expect("catalog result");
        assert_eq!(
            result
                .0
                .into_iter()
                .map(|model| model.id)
                .collect::<HashSet<_>>(),
            HashSet::from(["codex-slow-model".to_owned()])
        );
        assert!(result.2);
        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_catalog_unknown_source_fails_closed_without_endpoint() {
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: None,
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let catalog = state.account_type_catalog.clone();
        let candidates = vec![CatalogAccountCandidate {
            token: "unknown-source-token".to_owned(),
            source_type: "future-incompatible".to_owned(),
            chatgpt_account_id: None,
        }];
        assert!(
            catalog
                .fetch_account_type_models(
                    &"pro".to_owned(),
                    &candidates,
                    Instant::now() + Duration::from_secs(1),
                )
                .await
                .is_none()
        );
        catalog.shutdown().await;
    }

    #[tokio::test]
    async fn native_catalog_codex_timeout_does_not_probe_web_endpoint() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-codex-source-timeout-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"representative","status":"正常","type":"Pro","source_type":"codex"}]"#,
        )
        .expect("account snapshot");
        let codex_started = Arc::new(Notify::new());
        let codex_started_for_upstream = codex_started.clone();
        let web_finished = Arc::new(Notify::new());
        let web_finished_for_upstream = web_finished.clone();
        let upstream_ready = Arc::new(Notify::new());
        let upstream_ready_for_task = upstream_ready.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            upstream_ready_for_task.notify_one();
            axum::serve(
                listener,
                Router::new()
                    .route("/", get(|| async { Html("<html></html>") }))
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        post(|| async { Json(json!({"prepare_token":"prepare-token"})) }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/finalize",
                        post(|| async { Json(json!({"token":"requirements-token"})) }),
                    )
                    .route(
                        "/backend-api/models",
                        get(move || {
                            let finished = web_finished_for_upstream.clone();
                            async move {
                                finished.notify_one();
                                Json(json!({"models":[{"slug":"web-survives-timeout"}]}))
                            }
                        }),
                    )
                    .route(
                        "/backend-api/codex/models",
                        get(move || {
                            let started = codex_started_for_upstream.clone();
                            async move {
                                started.notify_one();
                                let first = stream::once(async {
                                    Ok::<Bytes, std::convert::Infallible>(Bytes::from("{"))
                                });
                                let rest =
                                    stream::pending::<Result<Bytes, std::convert::Infallible>>();
                                (StatusCode::OK, Body::from_stream(first.chain(rest)))
                                    .into_response()
                            }
                        }),
                    ),
            )
            .await
            .expect("upstream server");
        });
        upstream_ready.notified().await;

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let candidates = vec![CatalogAccountCandidate {
            token: "representative".to_owned(),
            source_type: "codex".to_owned(),
            chatgpt_account_id: None,
        }];
        let catalog = state.account_type_catalog.clone();
        let fetch = tokio::spawn(async move {
            catalog
                .fetch_account_type_models(
                    &"pro".to_owned(),
                    &candidates,
                    Instant::now() + Duration::from_millis(100),
                )
                .await
        });
        codex_started.notified().await;
        let result = tokio::time::timeout(Duration::from_secs(1), fetch)
            .await
            .expect("catalog deadline")
            .expect("fetch task");
        assert!(
            result.is_none(),
            "a failed Codex source has no usable catalog"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(100), web_finished.notified())
                .await
                .is_err(),
            "Codex representatives must not probe Web"
        );

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    async fn catalog_endpoint_status_case(
        web_status: StatusCode,
        codex_status: StatusCode,
    ) -> (Option<(HashSet<String>, bool)>, usize, usize) {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-web-source-status-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"representative","status":"正常","type":"Pro"}]"#,
        )
        .expect("account snapshot");
        let web_calls = Arc::new(AtomicUsize::new(0));
        let codex_calls = Arc::new(AtomicUsize::new(0));
        let web_calls_for_upstream = web_calls.clone();
        let codex_calls_for_upstream = codex_calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", get(|| async { Html("<html></html>") }))
                    .route(
                        "/backend-api/models",
                        get(move || {
                            let calls = web_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                if !web_status.is_success() {
                                    return web_status.into_response();
                                }
                                Json(json!({"models":[{"slug":"web-model"}]})).into_response()
                            }
                        }),
                    )
                    .route(
                        "/backend-api/codex/models",
                        get(move || {
                            let calls = codex_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                if !codex_status.is_success() {
                                    return codex_status.into_response();
                                }
                                Json(json!({"models":[{"slug":"codex-model","supported_in_api":true}]})).into_response()
                            }
                        }),
                    ),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let candidates = vec![CatalogAccountCandidate {
            token: "representative".to_owned(),
            source_type: "web".to_owned(),
            chatgpt_account_id: None,
        }];
        let result = state
            .account_type_catalog
            .fetch_account_type_models(
                &"pro".to_owned(),
                &candidates,
                Instant::now() + Duration::from_millis(500),
            )
            .await
            .map(|(models, _, complete)| {
                (models.into_iter().map(|model| model.id).collect(), complete)
            });
        let counts = (
            web_calls.load(Ordering::SeqCst),
            codex_calls.load(Ordering::SeqCst),
        );
        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
        (result, counts.0, counts.1)
    }

    #[tokio::test]
    async fn native_catalog_web_failure_does_not_fallback_across_transport() {
        let (result, web_calls, codex_calls) =
            catalog_endpoint_status_case(StatusCode::BAD_GATEWAY, StatusCode::OK).await;
        assert!(result.is_none());
        assert_eq!((web_calls, codex_calls), (1, 0));
    }

    #[tokio::test]
    async fn native_catalog_web_failure_is_not_hidden_by_codex_success() {
        let (result, web_calls, codex_calls) =
            catalog_endpoint_status_case(StatusCode::BAD_GATEWAY, StatusCode::UNAUTHORIZED).await;
        assert!(result.is_none());
        assert_eq!((web_calls, codex_calls), (1, 0));
    }

    #[tokio::test]
    async fn native_catalog_web_source_falls_back_to_next_same_type_candidate() {
        let web_calls = Arc::new(AtomicUsize::new(0));
        let codex_calls = Arc::new(AtomicUsize::new(0));
        let web_calls_for_upstream = web_calls.clone();
        let codex_calls_for_upstream = codex_calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", get(|| async { Html("<html></html>") }))
                    .route(
                        "/backend-api/models",
                        get(move |headers: HeaderMap| {
                            let calls = web_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                let token = headers
                                    .get(header::AUTHORIZATION)
                                    .and_then(|value| value.to_str().ok())
                                    .unwrap_or_default();
                                if token.ends_with("first") {
                                    return StatusCode::BAD_GATEWAY.into_response();
                                }
                                Json(json!({
                                    "models": [{"slug": "web-second-candidate"}]
                                }))
                                .into_response()
                            }
                        }),
                    )
                    .route(
                        "/backend-api/codex/models",
                        get(move |headers: HeaderMap| {
                            let calls = codex_calls_for_upstream.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                let _ = headers;
                                StatusCode::INTERNAL_SERVER_ERROR.into_response()
                            }
                        }),
                    ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let candidates = vec![
            CatalogAccountCandidate {
                token: "first".to_owned(),
                source_type: "web".to_owned(),
                chatgpt_account_id: None,
            },
            CatalogAccountCandidate {
                token: "second".to_owned(),
                source_type: "web".to_owned(),
                chatgpt_account_id: None,
            },
        ];
        let result = state
            .account_type_catalog
            .fetch_account_type_models(
                &"pro".to_owned(),
                &candidates,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .expect("partial endpoint fallback remains usable");
        let owners = result.1.clone();
        assert_eq!(owners.candidates[0].token, "second");
        assert_eq!(
            result
                .0
                .into_iter()
                .map(|model| model.id)
                .collect::<HashSet<_>>(),
            HashSet::from(["web-second-candidate".to_owned()])
        );
        assert!(result.2, "the second same-source representative succeeds");
        assert_eq!(
            (
                web_calls.load(Ordering::SeqCst),
                codex_calls.load(Ordering::SeqCst)
            ),
            (2, 0),
            "a source group stops after its first successful candidate and never crosses transport"
        );

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
    }

    #[tokio::test]
    async fn native_catalog_codex_source_falls_back_to_next_same_type_candidate() {
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/backend-api/codex/models",
                    get(move |headers: HeaderMap| {
                        let calls = calls_for_upstream.clone();
                        async move {
                            let call = calls.fetch_add(1, Ordering::SeqCst);
                            let token = headers
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default();
                            if call == 0 {
                                assert!(token.ends_with("first"));
                                return StatusCode::BAD_GATEWAY.into_response();
                            }
                            assert!(token.ends_with("second"));
                            Json(json!({
                                "models": [{"slug":"codex-second-candidate","supported_in_api":true}]
                            }))
                            .into_response()
                        }
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let candidates = vec![
            CatalogAccountCandidate {
                token: "first".to_owned(),
                source_type: "codex".to_owned(),
                chatgpt_account_id: None,
            },
            CatalogAccountCandidate {
                token: "second".to_owned(),
                source_type: "codex".to_owned(),
                chatgpt_account_id: None,
            },
        ];
        let result = state
            .account_type_catalog
            .fetch_account_type_models(
                &"pro".to_owned(),
                &candidates,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .expect("same-source fallback");
        assert_eq!(result.0[0].id, "codex-second-candidate");
        assert!(result.2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(result.1.candidates[0].token, "second");
        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
    }

    #[tokio::test]
    async fn native_catalog_web_source_does_not_require_chat_sentinel_handshake() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-model-catalog-no-sentinel-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"representative","status":"正常","type":"Pro"}]"#,
        )
        .expect("account snapshot");

        let sentinel_calls = Arc::new(AtomicUsize::new(0));
        let sentinel_calls_for_prepare = sentinel_calls.clone();
        let sentinel_calls_for_finalize = sentinel_calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route("/", get(|| async { Html("<html></html>") }))
                    .route(
                        "/backend-api/models",
                        get(|| async {
                            Json(json!({
                                "models": [{"slug": "web-catalog-without-sentinel"}]
                            }))
                        }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        post(move || {
                            let calls = sentinel_calls_for_prepare.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                StatusCode::INTERNAL_SERVER_ERROR
                            }
                        }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/finalize",
                        post(move || {
                            let calls = sentinel_calls_for_finalize.clone();
                            async move {
                                calls.fetch_add(1, Ordering::SeqCst);
                                StatusCode::INTERNAL_SERVER_ERROR
                            }
                        }),
                    ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: None,
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let candidates = vec![CatalogAccountCandidate {
            token: "representative".to_owned(),
            source_type: "web".to_owned(),
            chatgpt_account_id: None,
        }];
        let result = state
            .account_type_catalog
            .fetch_account_type_models(
                &"pro".to_owned(),
                &candidates,
                Instant::now() + Duration::from_secs(1),
            )
            .await
            .expect("web catalog should not depend on chat sentinel");
        assert_eq!(
            result
                .0
                .iter()
                .map(|model| model.id.as_str())
                .collect::<Vec<_>>(),
            vec!["web-catalog-without-sentinel"]
        );
        assert_eq!(sentinel_calls.load(Ordering::SeqCst), 0);

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn public_models_native_cold_refresh_is_nonblocking_and_singleflight() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-native-public-cold-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"plus-a","status":"正常","type":"plus"},
                {"access_token":"team-a","status":"正常","type":"team"}
            ]"#,
        )
        .expect("account snapshot");

        let model_fetches = Arc::new(AtomicUsize::new(0));
        let model_fetch_started = Arc::new(Notify::new());
        let model_fetches_for_auth = model_fetches.clone();
        let model_fetches_for_anon = model_fetches.clone();
        let started_for_auth = model_fetch_started.clone();
        let started_for_anon = model_fetch_started.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            let pending_models = |counter: Arc<AtomicUsize>, started: Arc<Notify>| {
                get(move || {
                    let counter = counter.clone();
                    let started = started.clone();
                    async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        started.notify_waiters();
                        let first = stream::once(async {
                            Ok::<Bytes, std::convert::Infallible>(Bytes::from("{"))
                        });
                        let rest = stream::pending::<Result<Bytes, std::convert::Infallible>>();
                        (StatusCode::OK, Body::from_stream(first.chain(rest))).into_response()
                    }
                })
            };
            axum::serve(
                listener,
                Router::new()
                    .route("/", get(|| async { Html("<html></html>") }))
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        post(|| async { Json(json!({"prepare_token":"prepare-token"})) }),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/finalize",
                        post(|| async { Json(json!({"token":"requirements-token"})) }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/prepare",
                        post(|| async { Json(json!({"prepare_token":"anon-prepare-token"})) }),
                    )
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        post(|| async { Json(json!({"token":"anon-requirements-token"})) }),
                    )
                    .route(
                        "/backend-api/models",
                        pending_models(model_fetches_for_auth, started_for_auth),
                    )
                    .route(
                        "/backend-anon/models",
                        pending_models(model_fetches_for_anon, started_for_anon),
                    ),
            )
            .await
            .expect("native upstream");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["static-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        let request = || {
            axum::http::Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, "Bearer client")
                .body(Body::empty())
                .expect("request")
        };
        let first_router = state.router();
        let second_router = state.router();
        let first =
            tokio::time::timeout(Duration::from_millis(250), first_router.oneshot(request()))
                .await
                .expect("first public native route must not wait for catalog")
                .expect("first response");
        let second =
            tokio::time::timeout(Duration::from_millis(250), second_router.oneshot(request()))
                .await
                .expect("second public native route must not wait for catalog")
                .expect("second response");
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if model_fetches.load(Ordering::Acquire) == 3 {
                    break;
                }
                model_fetch_started.notified().await;
            }
        })
        .await
        .expect("one native background owner should fetch anonymous plus two types");
        assert_eq!(model_fetches.load(Ordering::SeqCst), 3);

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_chat_waits_for_requested_type_when_another_type_is_ready() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-native-model-wait-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"free-token","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");

        let plus_started = Arc::new(Notify::new());
        let plus_release = Arc::new(Notify::new());
        let plus_started_for_upstream = plus_started.clone();
        let plus_release_for_upstream = plus_release.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            let bootstrap = get(|| async { Html("<html></html>") });
            let prepare = post(|| async { Json(json!({"prepare_token":"prepare-token"})) });
            let finalize = post(|| async { Json(json!({"token":"requirements-token"})) });
            let conversation = post(|| async {
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(
                        "data: {\"message\":{\"author\":{\"role\":\"assistant\"},\"content\":{\"parts\":[\"ok\"]}}}\n\n\
                         data: [DONE]\n\n",
                    ),
                )
                    .into_response()
            });
            let plus_models = get(move |headers: HeaderMap| {
                let plus_started = plus_started_for_upstream.clone();
                let plus_release = plus_release_for_upstream.clone();
                async move {
                    let token = headers
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    if token.contains("plus-token") {
                        plus_started.notify_one();
                        plus_release.notified().await;
                        return Json(json!({
                            "models":[{"slug":"plus-model"}]
                        }));
                    }
                    Json(json!({"models":[{"slug":"plus-model"}]}))
                }
            });
            let anon_models = get(|| async { Json(json!({"models":[{"slug":"anon-model"}]})) });
            axum::serve(
                listener,
                Router::new()
                    .route("/", bootstrap)
                    .route(
                        "/backend-api/sentinel/chat-requirements/prepare",
                        prepare.clone(),
                    )
                    .route(
                        "/backend-api/sentinel/chat-requirements/finalize",
                        finalize.clone(),
                    )
                    .route("/backend-api/models", plus_models)
                    .route(
                        "/backend-api/codex/models",
                        get(|| async { Json(json!({"models": [{"slug":"plus-model"}]})) }),
                    )
                    .route("/backend-anon/sentinel/chat-requirements/prepare", prepare)
                    .route(
                        "/backend-anon/sentinel/chat-requirements/finalize",
                        finalize,
                    )
                    .route("/backend-anon/models", anon_models)
                    .route("/backend-api/conversation", conversation),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;

        fs::write(
            &account_path,
            r#"[
                {"access_token":"plus-token","status":"正常","type":"plus"}
            ]"#,
        )
        .expect("new account type");
        let router = state.router();
        let request = axum::http::Request::builder()
            .method("POST")
            .uri("/v1/chat/completions")
            .header(header::AUTHORIZATION, "Bearer client")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"model":"plus-model","prompt":"hello"}"#))
            .expect("request");
        let mut request_task = tokio::spawn(async move { router.oneshot(request).await });
        tokio::time::timeout(Duration::from_millis(250), plus_started.notified())
            .await
            .expect("requested type entered catalog refresh");
        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut request_task)
                .await
                .is_err(),
            "requested model must wait for its pending type"
        );
        plus_release.notify_waiters();
        let response = tokio::time::timeout(Duration::from_secs(1), request_task)
            .await
            .expect("chat route deadline")
            .expect("chat route task")
            .expect("chat response");
        assert_eq!(response.status(), StatusCode::OK);
        let _ = response.into_body().collect().await.expect("response body");

        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn codex_legacy_type_uses_the_python_free_catalog_group() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-codex-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"codex-token","status":"正常","type":"codex"},
                {"access_token":"free-token","status":"正常","type":"free"}
            ]"#,
        )
        .expect("account snapshot");

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| {
                        let calls = calls_for_upstream.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            let token = headers
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default();
                            let id = if token.is_empty() {
                                "anonymous-model"
                            } else {
                                "free-model"
                            };
                            Json(json!({
                                "object": "list",
                                "data": [{"id": id}]
                            }))
                        }
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "legacy codex and free accounts share Python's free catalog"
        );
        assert_eq!(
            state
                .account_type_catalog
                .live_tokens_for("free-model")
                .expect("free route"),
            HashSet::from(["codex-token".to_owned(), "free-token".to_owned()])
        );

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn model_catalog_preserves_python_reasoning_effort_aliases() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-model-reasoning-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"free-token","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(|headers: HeaderMap| async move {
                        if headers.get(header::AUTHORIZATION).is_some() {
                            Json(json!({
                                "object": "list",
                                "data": [{
                                    "id": "reasoning-model",
                                    "capabilities": {
                                        "reasoning_efforts": [
                                            " low ",
                                            {"effort": "high"},
                                            "low"
                                        ]
                                    }
                                }]
                            }))
                        } else {
                            Json(json!({
                                "object": "list",
                                "data": [{"id": "anonymous-model"}]
                            }))
                        }
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");

        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        let models = state
            .account_type_catalog
            .public_models(Arc::new(Vec::new()));
        let model = models
            .iter()
            .find(|model| model.id == "reasoning-model")
            .expect("reasoning model");
        assert_eq!(model.supported_reasoning_efforts, vec!["low", "high"]);

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn unknown_account_type_case_variants_remain_distinct_catalog_types() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-unknown-case-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"custom-upper","status":"正常","type":"Custom"},
                {"access_token":"custom-lower","status":"正常","type":"custom"}
            ]"#,
        )
        .expect("account snapshot");

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| {
                        let calls = calls_for_upstream.clone();
                        async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            let token = headers
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default();
                            let id = if token.contains("custom-upper") {
                                "custom-upper-model"
                            } else if token.contains("custom-lower") {
                                "custom-lower-model"
                            } else {
                                "anonymous-model"
                            };
                            Json(json!({
                                "object": "list",
                                "data": [{"id": id}]
                            }))
                        }
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;

        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "anonymous plus each unknown case-sensitive type needs one representative fetch"
        );
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        let ids = value["data"]
            .as_array()
            .expect("model data")
            .iter()
            .filter_map(|item| item["id"].as_str())
            .collect::<HashSet<_>>();
        assert!(ids.contains("custom-upper-model"));
        assert!(ids.contains("custom-lower-model"));
        let upper = value["data"]
            .as_array()
            .expect("model data")
            .iter()
            .find(|item| item["id"] == "custom-upper-model")
            .expect("upper model");
        assert_eq!(upper["supported_account_types"], json!(["custom"]));

        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn models_route_uses_one_representative_per_account_type() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-types-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"free-a","status":"正常","type":"free"},
                {"access_token":"free-b","status":"正常","type":"free"},
                {"access_token":"plus-a","status":"正常","type":"plus"}
            ]"#,
        )
        .expect("account snapshot");

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        async fn upstream(headers: HeaderMap, calls: Arc<AtomicUsize>) -> Json<Value> {
            calls.fetch_add(1, Ordering::SeqCst);
            let token = headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default();
            let model = if token.contains("plus") {
                "plus-model"
            } else {
                "free-model"
            };
            Json(json!({
                "object": "list",
                "data": [{"id": model, "owned_by": "upstream"}]
            }))
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| upstream(headers, calls_for_upstream.clone())),
                ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["z-model".to_owned(), "a-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        let ordered_ids = value["data"]
            .as_array()
            .expect("model data")
            .iter()
            .filter_map(|item| item["id"].as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            ordered_ids,
            vec!["a-model", "free-model", "plus-model", "z-model"]
        );
        let mut ids = ordered_ids;
        ids.sort_unstable();
        assert_eq!(ids, vec!["a-model", "free-model", "plus-model", "z-model"]);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        fs::write(
            &account_path,
            r#"[
                {"access_token":"free-a","status":"正常","type":"free"},
                {"access_token":"free-b","status":"正常","type":"free"},
                {"access_token":"free-c","status":"正常","type":"free"},
                {"access_token":"plus-a","status":"正常","type":"plus"}
            ]"#,
        )
        .expect("same-type account addition");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "same-type membership changes must not refetch a ready directory"
        );

        let second_response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("second response");
        assert_eq!(second_response.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 3);

        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn models_route_retries_until_success_within_a_type() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-failover-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"free-a","status":"正常","type":"free"},
                {"access_token":"free-b","status":"正常","type":"free"},
                {"access_token":"free-c","status":"正常","type":"free"},
                {"access_token":"plus-a","status":"正常","type":"plus"}
            ]"#,
        )
        .expect("account snapshot");
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_upstream = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(headers: HeaderMap, calls: Arc<AtomicUsize>) -> Response {
                calls.fetch_add(1, Ordering::SeqCst);
                let token = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                if token.contains("free-a") || token.contains("free-b") {
                    return StatusCode::BAD_GATEWAY.into_response();
                }
                let model = if token.contains("plus") {
                    "plus-model"
                } else {
                    "free-model"
                };
                Json(json!({
                    "object": "list",
                    "data": [{"id": model}]
                }))
                .into_response()
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| upstream(headers, calls_for_upstream.clone())),
                ),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["anon-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        let ids = value["data"]
            .as_array()
            .expect("model data")
            .iter()
            .filter_map(|item| item["id"].as_str())
            .collect::<HashSet<_>>();
        assert!(ids.contains("free-model"));
        assert!(ids.contains("plus-model"));
        assert_eq!(calls.load(Ordering::SeqCst), 5);
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn failed_type_refresh_keeps_last_good_and_respects_backoff() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-backoff-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"free-a","status":"正常","type":"free"},
                {"access_token":"free-b","status":"正常","type":"free"}
            ]"#,
        )
        .expect("account snapshot");
        let calls = Arc::new(AtomicUsize::new(0));
        let fail = Arc::new(AtomicBool::new(false));
        let failed = Arc::new(Notify::new());
        let calls_for_upstream = calls.clone();
        let fail_for_upstream = fail.clone();
        let failed_for_upstream = failed.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(
                headers: HeaderMap,
                calls: Arc<AtomicUsize>,
                fail: Arc<AtomicBool>,
                failed: Arc<Notify>,
            ) -> Response {
                let call_number = calls.fetch_add(1, Ordering::SeqCst) + 1;
                let token = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                if token.is_empty() {
                    return Json(json!({
                        "object": "list",
                        "data": [{"id": "anonymous-model"}]
                    }))
                    .into_response();
                }
                if fail.load(Ordering::SeqCst) {
                    if call_number == 4 {
                        failed.notify_one();
                    }
                    return StatusCode::BAD_GATEWAY.into_response();
                }
                Json(json!({
                    "object": "list",
                    "data": [{"id": "free-model"}]
                }))
                .into_response()
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| {
                        upstream(
                            headers,
                            calls_for_upstream.clone(),
                            fail_for_upstream.clone(),
                            failed_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["anon-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        let request = || {
            axum::http::Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, "Bearer client")
                .body(Body::empty())
                .expect("request")
        };
        let first = state.router().oneshot(request()).await.expect("response");
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        fail.store(true, Ordering::SeqCst);
        {
            let mut snapshot = state
                .account_type_catalog
                .snapshot
                .write()
                .expect("catalog lock");
            for entry in snapshot.entries.values_mut() {
                entry.expires_at = Instant::now() - Duration::from_secs(1);
                entry.retry_at = Instant::now() - Duration::from_secs(1);
            }
        }
        let second = state.router().oneshot(request()).await.expect("response");
        assert_eq!(second.status(), StatusCode::OK);
        let body = second.into_body().collect().await.expect("body").to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        assert!(
            value["data"]
                .as_array()
                .is_some_and(|items| { items.iter().any(|item| item["id"] == "free-model") })
        );
        failed.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        let third = state.router().oneshot(request()).await.expect("response");
        assert_eq!(third.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn account_removal_updates_live_membership_without_erasing_catalog() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-membership-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"free-a","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream() -> Json<Value> {
                Json(json!({"object":"list","data":[{"id":"free-model"}]}))
            }
            axum::serve(listener, Router::new().route("/v1/models", get(upstream)))
                .await
                .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["anon-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            state
                .account_type_catalog
                .live_tokens_for("free-model")
                .expect("dynamic route"),
            HashSet::from(["free-a".to_owned()])
        );
        fs::write(&account_path, "[]").expect("removed account snapshot");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        assert!(
            !state
                .account_type_catalog
                .public_models(Arc::new(Vec::new()))
                .iter()
                .any(|model| {
                    model.id == "free-model"
                        && model
                            .supported_account_types
                            .iter()
                            .any(|account_type| account_type == "free")
                }),
            "a removed account type must not remain in the public catalog"
        );
        fs::write(
            &account_path,
            r#"[{"access_token":"free-b","status":"正常","type":"free"}]"#,
        )
        .expect("rotated account snapshot");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        assert_eq!(
            state
                .account_type_catalog
                .live_tokens_for("free-model")
                .expect("dynamic route"),
            HashSet::from(["free-b".to_owned()])
        );
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn bad_account_snapshot_clears_catalog_live_membership() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-invalid-membership-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"free-a","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(headers: HeaderMap) -> Json<Value> {
                let model = headers
                    .get(header::AUTHORIZATION)
                    .map(|_| "free-model")
                    .unwrap_or("anon-upstream-model");
                Json(json!({
                    "object": "list",
                    "data": [{"id": model}]
                }))
            }
            axum::serve(listener, Router::new().route("/v1/models", get(upstream)))
                .await
                .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["anon-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_secs(1))
            .await;
        assert_eq!(
            state
                .account_type_catalog
                .live_tokens_for("free-model")
                .expect("initial route"),
            HashSet::from(["free-a".to_owned()])
        );

        fs::write(&account_path, b"{").expect("broken account snapshot");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/v1/models")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .body(Body::empty())
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        assert!(
            state
                .account_type_catalog
                .live_tokens_for("free-model")
                .is_some_and(|tokens| tokens.is_empty()),
            "a broken account generation must not leave old tokens routable"
        );
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        assert!(
            !value["data"]
                .as_array()
                .is_some_and(|items| { items.iter().any(|item| item["id"] == "free-model") })
        );

        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn late_catalog_result_cannot_publish_for_a_replaced_token_same_type() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-token-fence-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"free-a","status":"正常","type":"free"}]"#,
        )
        .expect("account snapshot");
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let started_for_upstream = started.clone();
        let release_for_upstream = release.clone();
        let calls_for_upstream = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(
                headers: HeaderMap,
                started: Arc<Notify>,
                release: Arc<Notify>,
                calls: Arc<AtomicUsize>,
            ) -> Json<Value> {
                let token = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                calls.fetch_add(1, Ordering::SeqCst);
                if token == "Bearer free-a" {
                    started.notify_one();
                    release.notified().await;
                    return Json(json!({
                        "object": "list",
                        "data": [{"id": "old-model"}]
                    }));
                }
                Json(json!({
                    "object": "list",
                    "data": [{"id": "new-model"}]
                }))
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| {
                        upstream(
                            headers,
                            started_for_upstream.clone(),
                            release_for_upstream.clone(),
                            calls_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let catalog = state.account_type_catalog.clone();
        let refresh = tokio::spawn(async move {
            catalog
                .refresh_inner_with_budget(Duration::from_secs(1))
                .await;
        });
        started.notified().await;
        fs::write(
            &account_path,
            r#"[{"access_token":"free-b","status":"正常","type":"free"}]"#,
        )
        .expect("replaced account snapshot");
        release.notify_one();
        refresh.await.expect("refresh task");

        assert!(
            state
                .account_type_catalog
                .supported_types_for("old-model")
                .is_some_and(|types| types.is_empty()),
            "a late result from the replaced token must not become ready"
        );
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_secs(1))
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            state
                .account_type_catalog
                .live_tokens_for("new-model")
                .expect("new model route"),
            HashSet::from(["free-b".to_owned()])
        );

        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn late_catalog_result_cannot_publish_for_a_replaced_account_id_same_token() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-account-id-fence-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"codex-token","status":"正常","type":"pro","source_type":"codex","chatgpt_account_id":"old-account"}]"#,
        )
        .expect("account snapshot");
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls = Arc::new(AtomicUsize::new(0));
        let upstream_ready = Arc::new(Notify::new());
        let upstream_ready_for_task = upstream_ready.clone();
        let started_for_upstream = started.clone();
        let release_for_upstream = release.clone();
        let calls_for_upstream = calls.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            upstream_ready_for_task.notify_one();
            async fn upstream(
                request: axum::extract::Request,
                started: Arc<Notify>,
                release: Arc<Notify>,
                calls: Arc<AtomicUsize>,
            ) -> Json<Value> {
                if request.headers().get(header::AUTHORIZATION).is_none() {
                    return Json(json!({
                        "data": [{"id": "anonymous-model", "object": "model"}]
                    }));
                }
                let call = calls.fetch_add(1, Ordering::SeqCst);
                if call == 0 {
                    started.notify_one();
                    release.notified().await;
                    return Json(json!({
                        "data": [{
                            "id": "old-account-model",
                            "object": "model"
                        }]
                    }));
                }
                Json(json!({
                    "data": [{
                        "id": "new-account-model",
                        "object": "model"
                    }]
                }))
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |request: axum::extract::Request| {
                        upstream(
                            request,
                            started_for_upstream.clone(),
                            release_for_upstream.clone(),
                            calls_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });
        upstream_ready.notified().await;

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let catalog = state.account_type_catalog.clone();
        let refresh = tokio::spawn(async move {
            catalog
                .refresh_inner_with_budget(Duration::from_secs(1))
                .await;
        });
        started.notified().await;
        fs::write(
            &account_path,
            r#"[{"access_token":"codex-token","status":"正常","type":"pro","source_type":"codex","chatgpt_account_id":"new-account"}]"#,
        )
        .expect("replaced account identity");
        release.notify_one();
        refresh.await.expect("refresh task");

        assert!(
            state
                .account_type_catalog
                .supported_types_for("old-account-model")
                .is_some_and(|types| types.is_empty()),
            "a late result from a replaced account identity must not become ready"
        );
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_secs(1))
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            state
                .account_type_catalog
                .supported_types_for("new-account-model")
                .is_some_and(|types| types.contains("pro"))
        );

        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn concurrent_cold_model_reads_share_one_type_refresh() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-singleflight-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"free-a","status":"正常","type":"free"},
                {"access_token":"free-b","status":"正常","type":"free"},
                {"access_token":"free-c","status":"正常","type":"free"}
            ]"#,
        )
        .expect("account snapshot");
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let calls_for_upstream = calls.clone();
        let started_for_upstream = started.clone();
        let release_for_upstream = release.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(
                headers: HeaderMap,
                calls: Arc<AtomicUsize>,
                started: Arc<Notify>,
                release: Arc<Notify>,
            ) -> Json<Value> {
                calls.fetch_add(1, Ordering::SeqCst);
                let token = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                if token.is_empty() {
                    return Json(json!({
                        "object": "list",
                        "data": [{"id":"anonymous-model"}]
                    }));
                }
                started.notify_one();
                release.notified().await;
                Json(json!({"object":"list","data":[{"id":"free-model"}]}))
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| {
                        upstream(
                            headers,
                            calls_for_upstream.clone(),
                            started_for_upstream.clone(),
                            release_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["anon-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let request = || {
            axum::http::Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, "Bearer client")
                .body(Body::empty())
                .expect("request")
        };
        let first_router = state.router();
        let second_router = state.router();
        let first = tokio::spawn(async move { first_router.oneshot(request()).await });
        started.notified().await;
        let second = tokio::spawn(async move { second_router.oneshot(request()).await });
        release.notify_one();
        assert_eq!(
            first
                .await
                .expect("first task")
                .expect("first response")
                .status(),
            StatusCode::OK
        );
        assert_eq!(
            second
                .await
                .expect("second task")
                .expect("second response")
                .status(),
            StatusCode::OK
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn public_models_cold_refresh_is_nonblocking_and_singleflight() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-public-cold-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"plus-a","status":"正常","type":"plus"},
                {"access_token":"team-a","status":"正常","type":"team"}
            ]"#,
        )
        .expect("account snapshot");

        let started_count = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let started_count_for_upstream = started_count.clone();
        let started_for_upstream = started.clone();
        let release_for_upstream = release.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(
                started_count: Arc<AtomicUsize>,
                started: Arc<Notify>,
                release: Arc<Notify>,
            ) -> Response {
                started_count.fetch_add(1, Ordering::SeqCst);
                started.notify_one();
                release.notified().await;
                Json(json!({
                    "object": "list",
                    "data": [{"id":"blocked-model"}]
                }))
                .into_response()
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move || {
                        upstream(
                            started_count_for_upstream.clone(),
                            started_for_upstream.clone(),
                            release_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["static-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let request = || {
            axum::http::Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, "Bearer client")
                .body(Body::empty())
                .expect("request")
        };
        let first_router = state.router();
        let second_router = state.router();
        let first_task = tokio::spawn(async move { first_router.oneshot(request()).await });
        let second_task = tokio::spawn(async move { second_router.oneshot(request()).await });
        let first = tokio::time::timeout(Duration::from_millis(250), first_task)
            .await
            .expect("first public route must not wait for catalog deadline")
            .expect("first route task")
            .expect("first response");
        let second = tokio::time::timeout(Duration::from_millis(250), second_task)
            .await
            .expect("second public route must not wait for catalog deadline")
            .expect("second route task")
            .expect("second response");
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if started_count.load(Ordering::Acquire) == 3 {
                    break;
                }
                started.notified().await;
            }
        })
        .await
        .expect("one background owner should fetch anonymous plus two types");
        assert_eq!(
            started_count.load(Ordering::SeqCst),
            3,
            "concurrent public reads must share one bounded refresh owner"
        );
        let body = first.into_body().collect().await.expect("body").to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        assert!(
            value["data"]
                .as_array()
                .is_some_and(|items| { items.iter().any(|item| item["id"] == "static-model") })
        );

        release.notify_waiters();
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn public_models_empty_cold_snapshot_returns_empty_without_waiting() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-public-empty-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"plus-a","status":"正常","type":"plus"},
                {"access_token":"team-a","status":"正常","type":"team"}
            ]"#,
        )
        .expect("account snapshot");

        let started_count = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let started_count_for_upstream = started_count.clone();
        let started_for_upstream = started.clone();
        let release_for_upstream = release.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(
                started_count: Arc<AtomicUsize>,
                started: Arc<Notify>,
                release: Arc<Notify>,
            ) -> Response {
                started_count.fetch_add(1, Ordering::SeqCst);
                started.notify_waiters();
                release.notified().await;
                Json(json!({
                    "object": "list",
                    "data": [{"id":"late-model"}]
                }))
                .into_response()
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move || {
                        upstream(
                            started_count_for_upstream.clone(),
                            started_for_upstream.clone(),
                            release_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let request = || {
            axum::http::Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, "Bearer client")
                .body(Body::empty())
                .expect("request")
        };
        let response = tokio::time::timeout(
            Duration::from_millis(250),
            state.router().oneshot(request()),
        )
        .await
        .expect("public route must not wait for a cold catalog")
        .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        assert_eq!(value["data"], json!([]));

        tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if started_count.load(Ordering::Acquire) >= 1 {
                    break;
                }
                started.notified().await;
            }
        })
        .await
        .expect("background catalog owner should be admitted");
        release.notify_waiters();
        state.account_type_catalog.shutdown().await;
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn anonymous_last_good_does_not_block_a_new_type_refresh() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-anonymous-ready-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(&account_path, "[]").expect("empty account snapshot");

        let calls = Arc::new(AtomicUsize::new(0));
        let plus_started = Arc::new(Notify::new());
        let plus_release = Arc::new(Notify::new());
        let calls_for_upstream = calls.clone();
        let plus_started_for_upstream = plus_started.clone();
        let plus_release_for_upstream = plus_release.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(
                headers: HeaderMap,
                calls: Arc<AtomicUsize>,
                plus_started: Arc<Notify>,
                plus_release: Arc<Notify>,
            ) -> Response {
                calls.fetch_add(1, Ordering::SeqCst);
                let token = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                if token.contains("plus-a") {
                    plus_started.notify_one();
                    plus_release.notified().await;
                    return Json(json!({
                        "object": "list",
                        "data": [{"id":"plus-model"}]
                    }))
                    .into_response();
                }
                Json(json!({
                    "object": "list",
                    "data": [{"id":"anonymous-model"}]
                }))
                .into_response()
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| {
                        upstream(
                            headers,
                            calls_for_upstream.clone(),
                            plus_started_for_upstream.clone(),
                            plus_release_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        let request = || {
            axum::http::Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, "Bearer client")
                .body(Body::empty())
                .expect("request")
        };
        let initial = state.router().oneshot(request()).await.expect("response");
        assert_eq!(initial.status(), StatusCode::OK);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        fs::write(
            &account_path,
            r#"[{"access_token":"plus-a","status":"正常","type":"plus"}]"#,
        )
        .expect("new account type");
        let response = tokio::time::timeout(
            Duration::from_millis(250),
            state.router().oneshot(request()),
        )
        .await
        .expect("anonymous last-good must make route nonblocking")
        .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        plus_started.notified().await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        let ids = value["data"]
            .as_array()
            .expect("model data")
            .iter()
            .filter_map(|item| item["id"].as_str())
            .collect::<HashSet<_>>();
        assert_eq!(ids, HashSet::from(["anonymous-model"]));

        plus_release.notify_waiters();
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn cold_wait_never_publishes_torn_live_membership() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-live-membership-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"old-token","status":"正常","type":"free"}]"#,
        )
        .expect("initial account snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["static-model".to_owned()],
            upstream_base_url: Some("http://127.0.0.1:9".to_owned()),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let group = "free".to_owned();
        {
            let mut snapshot = state
                .account_type_catalog
                .snapshot
                .write()
                .expect("catalog lock");
            snapshot
                .live_tokens
                .insert(group.clone(), vec!["old-token".to_owned()]);
            snapshot.live_candidates.insert(
                group.clone(),
                vec![CatalogAccountCandidate {
                    token: "old-token".to_owned(),
                    source_type: "web".to_owned(),
                    chatgpt_account_id: None,
                }],
            );
        }
        fs::write(
            &account_path,
            r#"[{"access_token":"new-token","status":"正常","type":"free"}]"#,
        )
        .expect("replaced account snapshot");

        state
            .account_type_catalog
            .live_membership_barrier_enabled
            .store(true, Ordering::Release);
        let published = state
            .account_type_catalog
            .live_membership_after_tokens
            .notified();
        let catalog = state.account_type_catalog.clone();
        let refresh = tokio::spawn(async move {
            catalog.refresh_with_cold_wait(None).await;
        });
        published.await;

        {
            let snapshot = state
                .account_type_catalog
                .snapshot
                .read()
                .expect("catalog lock");
            assert_eq!(snapshot.live_tokens[&group], vec!["new-token"]);
            assert_eq!(snapshot.live_candidates[&group][0].token, "new-token");
        }

        state
            .account_type_catalog
            .live_membership_release
            .notify_one();
        state.account_type_catalog.shutdown().await;
        refresh.abort();
        let _ = refresh.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn cold_catalog_refresh_has_one_deadline_and_bounded_type_concurrency() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-deadline-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"free-a","status":"正常","type":"free"},
                {"access_token":"plus-a","status":"正常","type":"plus"},
                {"access_token":"team-a","status":"正常","type":"team"},
                {"access_token":"pro-a","status":"正常","type":"pro"},
                {"access_token":"enterprise-a","status":"正常","type":"enterprise"}
            ]"#,
        )
        .expect("account snapshot");

        let started_count = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let retry_started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let started_for_upstream = started_count.clone();
        let notify_for_upstream = started.clone();
        let retry_for_upstream = retry_started.clone();
        let release_for_upstream = release.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(
                started_count: Arc<AtomicUsize>,
                started: Arc<Notify>,
                retry_started: Arc<Notify>,
                release: Arc<Notify>,
            ) -> Response {
                let call_number = started_count.fetch_add(1, Ordering::SeqCst) + 1;
                if call_number > 4 {
                    retry_started.notify_one();
                }
                started.notify_one();
                release.notified().await;
                Json(json!({"object":"list","data":[{"id":"blocked-model"}]})).into_response()
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move || {
                        upstream(
                            started_for_upstream.clone(),
                            notify_for_upstream.clone(),
                            retry_for_upstream.clone(),
                            release_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: Vec::new(),
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let catalog = state.account_type_catalog.clone();
        let refresh_catalog = catalog.clone();
        let refresh_task = tokio::spawn(async move {
            refresh_catalog
                .refresh_inner_with_budget(Duration::from_millis(80))
                .await;
        });

        tokio::time::timeout(Duration::from_millis(250), async {
            loop {
                if started_count.load(Ordering::Acquire) >= 4 {
                    break;
                }
                started.notified().await;
            }
        })
        .await
        .expect("bounded refresh admitted its fixed number of jobs");

        tokio::time::timeout(Duration::from_secs(2), refresh_task)
            .await
            .expect("catalog-level deadline")
            .expect("refresh task");
        assert_eq!(
            started_count.load(Ordering::Acquire),
            4,
            "no queued type may start after the shared deadline"
        );

        release.notify_waiters();
        catalog
            .refresh_inner_with_budget(Duration::from_millis(80))
            .await;
        assert!(
            tokio::time::timeout(Duration::from_millis(250), retry_started.notified())
                .await
                .is_err(),
            "queued types must inherit the catalog deadline backoff"
        );
        assert_eq!(
            started_count.load(Ordering::Acquire),
            4,
            "the backoff must suppress an immediate second refresh"
        );

        release.notify_waiters();
        fs::write(
            &account_path,
            r#"[
                {"access_token":"free-b","status":"正常","type":"free"},
                {"access_token":"plus-a","status":"正常","type":"plus"},
                {"access_token":"team-a","status":"正常","type":"team"},
                {"access_token":"pro-a","status":"正常","type":"pro"},
                {"access_token":"enterprise-a","status":"正常","type":"enterprise"}
            ]"#,
        )
        .expect("rotated account snapshot");
        catalog
            .refresh_inner_with_budget(Duration::from_millis(80))
            .await;
        assert_eq!(
            started_count.load(Ordering::Acquire),
            5,
            "a new account identity must not inherit the old identity backoff"
        );
        catalog
            .refresh_inner_with_budget(Duration::from_millis(80))
            .await;
        assert_eq!(
            started_count.load(Ordering::Acquire),
            5,
            "a failed new identity must still respect its retry backoff"
        );

        release.notify_waiters();
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn models_route_keeps_last_good_during_type_deadline_backoff_without_retry_storm() {
        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-account-type-route-backoff-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"free-a","status":"正常","type":"free"}]"#,
        )
        .expect("initial account snapshot");

        let calls = Arc::new(AtomicUsize::new(0));
        let plus_started = Arc::new(Notify::new());
        let plus_release = Arc::new(Notify::new());
        let calls_for_upstream = calls.clone();
        let plus_started_for_upstream = plus_started.clone();
        let plus_release_for_upstream = plus_release.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let upstream_task = tokio::spawn(async move {
            async fn upstream(
                headers: HeaderMap,
                calls: Arc<AtomicUsize>,
                plus_started: Arc<Notify>,
                plus_release: Arc<Notify>,
            ) -> Response {
                calls.fetch_add(1, Ordering::SeqCst);
                let token = headers
                    .get(header::AUTHORIZATION)
                    .and_then(|value| value.to_str().ok())
                    .unwrap_or_default();
                if token.is_empty() {
                    return Json(json!({
                        "object": "list",
                        "data": [{"id":"anonymous-model"}]
                    }))
                    .into_response();
                }
                if token.contains("plus-a") {
                    plus_started.notify_one();
                    plus_release.notified().await;
                    return Json(json!({
                        "object": "list",
                        "data": [{"id":"plus-model"}]
                    }))
                    .into_response();
                }
                Json(json!({
                    "object": "list",
                    "data": [{"id":"free-model"}]
                }))
                .into_response()
            }
            axum::serve(
                listener,
                Router::new().route(
                    "/v1/models",
                    get(move |headers: HeaderMap| {
                        upstream(
                            headers,
                            calls_for_upstream.clone(),
                            plus_started_for_upstream.clone(),
                            plus_release_for_upstream.clone(),
                        )
                    }),
                ),
            )
            .await
            .expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["anon-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        state
            .account_type_catalog
            .refresh_inner_with_budget(Duration::from_millis(500))
            .await;
        let request = || {
            axum::http::Request::builder()
                .uri("/v1/models")
                .header(header::AUTHORIZATION, "Bearer client")
                .body(Body::empty())
                .expect("request")
        };

        assert_eq!(calls.load(Ordering::SeqCst), 2);

        fs::write(
            &account_path,
            r#"[
                {"access_token":"free-a","status":"正常","type":"free"},
                {"access_token":"plus-a","status":"正常","type":"plus"}
            ]"#,
        )
        .expect("new account type");
        let refresh = state.account_type_catalog.clone();
        let refresh_task = tokio::spawn(async move {
            refresh
                .refresh_inner_with_budget(Duration::from_millis(80))
                .await;
        });
        tokio::time::timeout(Duration::from_millis(250), plus_started.notified())
            .await
            .expect("new type entered the refresh");
        tokio::time::timeout(Duration::from_millis(500), refresh_task)
            .await
            .expect("catalog deadline")
            .expect("refresh task");

        let first_router = state.router();
        let second_router = state.router();
        let first = tokio::spawn(async move { first_router.oneshot(request()).await });
        let second = tokio::spawn(async move { second_router.oneshot(request()).await });
        let first = first
            .await
            .expect("first route task")
            .expect("first response");
        let second = second
            .await
            .expect("second route task")
            .expect("second response");
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(second.status(), StatusCode::OK);
        assert_eq!(
            calls.load(Ordering::SeqCst),
            3,
            "last-good anonymous/free directories must be served without retrying the timed-out type"
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(250), plus_started.notified())
                .await
                .is_err(),
            "the backoff window must suppress a retry from the concurrent routes"
        );
        let body = first.into_body().collect().await.expect("body").to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("models json");
        let ids = value["data"]
            .as_array()
            .expect("model data")
            .iter()
            .filter_map(|item| item["id"].as_str())
            .collect::<HashSet<_>>();
        assert!(ids.contains("anonymous-model"));
        assert!(ids.contains("free-model"));
        assert!(!ids.contains("plus-model"));

        plus_release.notify_waiters();
        upstream_task.abort();
        let _ = upstream_task.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_chat_retries_one_transient_upstream_failure_with_next_account() {
        let result =
            native_chat_failover_case(vec![StatusCode::BAD_GATEWAY, StatusCode::OK], "gpt-test")
                .await;
        assert_eq!(result.0, StatusCode::OK);
        assert_eq!(result.1, 2);
        assert_eq!(result.2, ["Bearer first-token", "Bearer second-token"]);
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_chat_stops_after_one_transient_failover() {
        let result = native_chat_failover_case(
            vec![
                StatusCode::BAD_GATEWAY,
                StatusCode::BAD_GATEWAY,
                StatusCode::OK,
            ],
            "gpt-test",
        )
        .await;
        assert_eq!(result.0, StatusCode::BAD_GATEWAY);
        assert_eq!(result.1, 2);
        assert_eq!(result.2, ["Bearer first-token", "Bearer second-token"]);
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_chat_does_not_fail_over_client_errors() {
        let result =
            native_chat_failover_case(vec![StatusCode::BAD_REQUEST, StatusCode::OK], "gpt-test")
                .await;
        assert_eq!(result.0, StatusCode::BAD_GATEWAY);
        assert_eq!(result.1, 1);
        assert_eq!(result.2, ["Bearer first-token"]);
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_chat_failover_keeps_the_requested_model_filter() {
        let result =
            native_chat_failover_case(vec![StatusCode::BAD_GATEWAY, StatusCode::OK], "other-model")
                .await;
        assert_eq!(result.0, StatusCode::BAD_GATEWAY);
        assert_eq!(result.1, 1);
        assert_eq!(result.2, ["Bearer first-token"]);
        assert_eq!(result.3, 0);
    }

    #[tokio::test]
    async fn native_chat_binds_codex_lease_to_codex_endpoint_for_static_model_collision() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let codex_calls = Arc::new(AtomicUsize::new(0));
        let codex_calls_for_upstream = codex_calls.clone();
        let upstream_ready = Arc::new(Notify::new());
        let upstream_ready_for_task = upstream_ready.clone();
        let codex = post(move |request: axum::extract::Request| {
            let codex_calls = codex_calls_for_upstream.clone();
            async move {
                codex_calls.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    request
                        .headers()
                        .get(header::AUTHORIZATION)
                        .and_then(|value| value.to_str().ok()),
                    Some("Bearer codex-token")
                );
                assert_eq!(
                    request
                        .headers()
                        .get("ChatGPT-Account-ID")
                        .and_then(|value| value.to_str().ok()),
                    Some("codex-account")
                );
                (
                    [(header::CONTENT_TYPE, "text/event-stream")],
                    Body::from(
                        "data: {\"type\":\"response.output_text.delta\",\"delta\":\"ok\"}\n\n\
                     data: {\"type\":\"response.completed\"}\n\n",
                    ),
                )
                    .into_response()
            }
        });
        let web = get(|| async { StatusCode::INTERNAL_SERVER_ERROR });
        let upstream = tokio::spawn(async move {
            upstream_ready_for_task.notify_one();
            axum::serve(
                listener,
                Router::new()
                    .route("/backend-api/codex/responses", codex)
                    .route("/", web),
            )
            .await
            .expect("upstream server");
        });
        upstream_ready.notified().await;

        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-codex-chat-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[
                {"access_token":"web-token","status":"正常","source_type":"web","type":"pro"},
                {"access_token":"codex-token","status":"正常","source_type":"codex","chatgpt_account_id":"codex-account","type":"pro"}
            ]"#,
        )
        .expect("account snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["collision-model".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));
        let catalog_group = "pro".to_owned();
        let catalog_model = PublicModel {
            id: "collision-model".to_owned(),
            object: "model",
            created: 0,
            owned_by: "codex".to_owned(),
            permission: Vec::new(),
            root: "collision-model".to_owned(),
            parent: None,
            allow_anonymous: false,
            supported_account_types: vec!["pro".to_owned()],
            supported_reasoning_efforts: Vec::new(),
        };
        {
            let mut catalog = state
                .account_type_catalog
                .snapshot
                .write()
                .expect("catalog snapshot lock");
            catalog.entries.insert(
                catalog_group.clone(),
                AccountTypeCatalogEntry {
                    models: Arc::new(vec![catalog_model]),
                    model_sources: HashMap::from([(
                        "collision-model".to_owned(),
                        HashSet::from(["codex".to_owned()]),
                    )]),
                    ready: true,
                    tokens: vec!["codex-token".to_owned()],
                    owners: CatalogOwners::with_model_sources(
                        vec![CatalogAccountCandidate {
                            token: "codex-token".to_owned(),
                            source_type: "codex".to_owned(),
                            chatgpt_account_id: Some("codex-account".to_owned()),
                        }],
                        HashMap::from([(
                            "collision-model".to_owned(),
                            HashSet::from(["codex".to_owned()]),
                        )]),
                    ),
                    expires_at: Instant::now() + Duration::from_secs(60),
                    retry_at: Instant::now(),
                },
            );
            catalog
                .live_tokens
                .insert(catalog_group, vec!["codex-token".to_owned()]);
            catalog.live_candidates.insert(
                "pro".to_owned(),
                vec![CatalogAccountCandidate {
                    token: "codex-token".to_owned(),
                    source_type: "codex".to_owned(),
                    chatgpt_account_id: Some("codex-account".to_owned()),
                }],
            );
        }
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"collision-model","messages":[{"role":"user","content":"hi"}]}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("completion json");
        assert_eq!(value["choices"][0]["message"]["content"], "ok");
        assert_eq!(state.account_store.inflight(), 0);
        assert_eq!(codex_calls.load(Ordering::SeqCst), 1);

        let unknown = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/chat/completions")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"unknown-dynamic-model","messages":[{"role":"user","content":"hi"}]}"#,
                    ))
                    .expect("unknown request"),
            )
            .await
            .expect("unknown response");
        assert!(matches!(
            unknown.status(),
            StatusCode::BAD_REQUEST | StatusCode::SERVICE_UNAVAILABLE
        ));
        assert_eq!(codex_calls.load(Ordering::SeqCst), 1);

        state.account_type_catalog.shutdown().await;
        upstream.abort();
        let _ = upstream.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn responses_route_forwards_openai_compatible_payload() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let calls = Arc::new(AtomicUsize::new(0));
        let upstream_calls = calls.clone();
        let upstream = tokio::spawn(async move {
            let app = Router::new().route(
                "/v1/responses",
                post(move || {
                    let calls = upstream_calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Json(json!({
                            "id": "resp-test",
                            "object": "response",
                            "output": [],
                        }))
                    }
                }),
            );
            axum::serve(listener, app).await.expect("upstream server");
        });

        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: None,
            upstream_protocol: UpstreamProtocol::OpenAi,
        })
        .expect("state");
        let response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"gpt-test","input":"hello"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::OK);
        let body = response
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("response json");
        assert_eq!(value["id"], "resp-test");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        upstream.abort();
        let _ = upstream.await;
    }

    #[tokio::test]
    async fn native_responses_codex_text_supports_nonstream_and_stream() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let response_calls = Arc::new(AtomicUsize::new(0));
        let response_calls_for_upstream = response_calls.clone();
        let captured_payloads = Arc::new(Mutex::new(Vec::<Value>::new()));
        let captured_payloads_for_upstream = captured_payloads.clone();
        let hang_next = Arc::new(AtomicBool::new(false));
        let hang_next_for_upstream = hang_next.clone();
        let error_next = Arc::new(AtomicBool::new(false));
        let error_next_for_upstream = error_next.clone();
        let upstream = tokio::spawn(async move {
            let bootstrap = get(|| async {
                Html(
                    r#"<html data-build="c/test/_build"><script src="https://chatgpt.com/c/test/_sdk.js"></script></html>"#,
                )
            });
            let prepare = post(|| async { Json(json!({"prepare_token":"prepare-token"})) });
            let finalize = post(|| async { Json(json!({"token":"requirements-token"})) });
            let models = get(|| async {
                Json(json!({
                    "models": [{"slug":"gpt-test","supported_in_api":true}]
                }))
            });
            let responses = post(move |headers: HeaderMap, Json(payload): Json<Value>| {
                let response_calls = response_calls_for_upstream.clone();
                let captured_payloads = captured_payloads_for_upstream.clone();
                let hang_next = hang_next_for_upstream.clone();
                let error_next = error_next_for_upstream.clone();
                async move {
                    assert_eq!(
                        headers
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok()),
                        Some("Bearer codex-token")
                    );
                    response_calls.fetch_add(1, Ordering::SeqCst);
                    captured_payloads.lock().await.push(payload);
                    if error_next.swap(false, Ordering::SeqCst) {
                        let chunks = stream::iter(vec![
                            Ok::<Bytes, std::io::Error>(Bytes::from(
                                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-error\"}}\n\n",
                            )),
                            Err(std::io::Error::other("upstream body error")),
                        ]);
                        return (
                            [(header::CONTENT_TYPE, "text/event-stream")],
                            Body::from_stream(chunks),
                        )
                            .into_response();
                    }
                    if hang_next.swap(false, Ordering::SeqCst) {
                        let first = stream::once(async {
                            Ok::<Bytes, std::convert::Infallible>(Bytes::from(
                                "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-pending\"}}\n\n",
                            ))
                        });
                        let rest = stream::pending::<Result<Bytes, std::convert::Infallible>>();
                        return (
                            [(header::CONTENT_TYPE, "text/event-stream")],
                            Body::from_stream(first.chain(rest)),
                        )
                            .into_response();
                    }
                    (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        Body::from(
                            "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-test\",\"model\":\"gpt-test\",\"status\":\"in_progress\"}}\n\n\
                             data: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n\
                             data: [DONE]\n\n\
                             data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"model\":\"gpt-test\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"hello\",\"annotations\":[]}]}]}}\n\n",
                        ),
                    )
                        .into_response()
                }
            });
            axum::serve(
                listener,
                Router::new()
                    .route("/", bootstrap)
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize)
                    .route(
                        "/backend-api/models",
                        get(|| async {
                            Json(json!({
                                "models": [{"slug":"gpt-test","supported_in_api":true}]
                            }))
                        }),
                    )
                    .route("/backend-api/codex/models", models)
                    .route("/backend-api/codex/responses", responses),
            )
            .await
            .expect("upstream server");
        });

        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-native-responses-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"codex-token","status":"正常","source_type":"codex","type":"free","models":["gpt-test"]}]"#,
        )
        .expect("account snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));

        let nonstream = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(r#"{"model":"gpt-test","input":"hello"}"#))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(nonstream.status(), StatusCode::OK);
        let body = nonstream
            .into_body()
            .collect()
            .await
            .expect("body")
            .to_bytes();
        let value: Value = serde_json::from_slice(&body).expect("response json");
        assert_eq!(value["id"], "resp-test");
        assert_eq!(value["output"][0]["content"][0]["text"], "hello");
        assert_eq!(state.account_store.inflight(), 0);

        error_next.store(true, Ordering::SeqCst);
        let response_error = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","input":"error","stream":true}"#,
                    ))
                    .expect("error request"),
            )
            .await
            .expect("error response");
        assert_eq!(response_error.status(), StatusCode::BAD_GATEWAY);
        let response_error_body = response_error
            .into_body()
            .collect()
            .await
            .expect("Responses error envelope")
            .to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&response_error_body).expect("Responses error JSON")["error"]
                ["type"],
            "server_error"
        );
        assert_eq!(state.account_store.inflight(), 0);

        hang_next.store(true, Ordering::SeqCst);
        let pending = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","input":"cancel","stream":true}"#,
                    ))
                    .expect("pending request"),
            )
            .await
            .expect("pending response");
        assert_eq!(pending.status(), StatusCode::OK);
        drop(pending);
        assert_eq!(state.account_store.inflight(), 0);

        let stream = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","input":"hello","stream":true}"#,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(stream.status(), StatusCode::OK);
        let body = stream
            .into_body()
            .collect()
            .await
            .expect("stream body")
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("utf8 stream");
        assert!(text.contains("response.created"));
        assert!(text.contains("response.output_text.delta"));
        assert!(text.contains("response.completed"));
        assert_eq!(state.account_store.inflight(), 0);
        assert_eq!(response_calls.load(Ordering::SeqCst), 4);
        let payloads = captured_payloads.lock().await;
        assert_eq!(payloads.len(), 4);
        assert_eq!(payloads[0]["model"], "gpt-test");
        assert_eq!(payloads[0]["input"], "hello");
        assert_eq!(payloads[1]["input"], "error");
        assert_eq!(payloads[2]["input"], "cancel");
        assert_eq!(payloads[3]["input"], "hello");
        for payload in payloads.iter().skip(1) {
            assert_eq!(payload["stream"], true);
        }
        drop(payloads);

        error_next.store(true, Ordering::SeqCst);
        let anthropic_error = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "client")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r##"{"model":"gpt-test","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"error"}]}"##,
                    ))
                    .expect("Anthropic error request"),
        )
            .await
            .expect("Anthropic error response");
        assert_eq!(anthropic_error.status(), StatusCode::BAD_GATEWAY);
        let anthropic_error_body = anthropic_error
            .into_body()
            .collect()
            .await
            .expect("Anthropic error envelope")
            .to_bytes();
        assert_eq!(
            serde_json::from_slice::<Value>(&anthropic_error_body).expect("Anthropic error JSON")["error"]
                ["type"],
            "server_error"
        );
        assert_eq!(state.account_store.inflight(), 0);

        hang_next.store(true, Ordering::SeqCst);
        let anthropic_pending = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "client")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r##"{"model":"gpt-test","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"cancel"}]}"##,
                    ))
                    .expect("pending Anthropic request"),
            )
            .await
            .expect("pending Anthropic response");
        assert_eq!(anthropic_pending.status(), StatusCode::OK);
        drop(anthropic_pending);
        assert_eq!(state.account_store.inflight(), 0);

        let anthropic = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "client")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r##"{"model":"gpt-test","max_tokens":8,"stream":true,"messages":[{"role":"user","content":"hello"}]}"##,
                    ))
                    .expect("request"),
            )
            .await
            .expect("response");
        assert_eq!(anthropic.status(), StatusCode::OK);
        let body = anthropic
            .into_body()
            .collect()
            .await
            .expect("Anthropic body")
            .to_bytes();
        let text = String::from_utf8(body.to_vec()).expect("Anthropic SSE");
        assert!(text.contains("content_block_start"));
        assert!(text.contains("text_delta"));
        assert!(text.contains("message_stop"));
        assert_eq!(state.account_store.inflight(), 0);
        assert_eq!(response_calls.load(Ordering::SeqCst), 7);
        let payloads = captured_payloads.lock().await;
        assert_eq!(payloads.len(), 7);
        assert_eq!(payloads[4]["input"][0]["content"][0]["text"], "error");
        assert_eq!(payloads[5]["input"][0]["content"][0]["text"], "cancel");
        assert_eq!(payloads[6]["input"][0]["content"][0]["text"], "hello");

        state.account_type_catalog.shutdown().await;
        upstream.abort();
        let _ = upstream.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[tokio::test]
    async fn native_responses_tools_route_through_codex_and_release_leases() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let captured = Arc::new(Mutex::new(Vec::<Value>::new()));
        let captured_for_upstream = captured.clone();
        let upstream = tokio::spawn(async move {
            let bootstrap = get(|| async {
                Html(
                    r#"<html data-build="c/test/_build"><script src="https://chatgpt.com/c/test/_sdk.js"></script></html>"#,
                )
            });
            let prepare = post(|| async { Json(json!({"prepare_token":"prepare-token"})) });
            let finalize = post(|| async { Json(json!({"token":"requirements-token"})) });
            let models = get(|| async {
                Json(json!({
                    "models": [{"slug":"gpt-test","supported_in_api":true}]
                }))
            });
            let responses = post(move |Json(payload): Json<Value>| {
                let captured = captured_for_upstream.clone();
                async move {
                    captured.lock().await.push(payload.clone());
                    let is_search = payload["tools"][0]["type"] == "web_search";
                    let body = if is_search {
                        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-search\"}}\n\n\
                         data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"web_search_call\",\"id\":\"ws-1\",\"status\":\"completed\",\"action\":{\"type\":\"search\",\"query\":\"rust\"}}}\n\n\
                         data: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg-1\",\"delta\":\"Rust answer\"}\n\n\
                         data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-search\",\"usage\":{\"output_tokens\":4},\"output\":[{\"type\":\"web_search_call\",\"id\":\"ws-1\",\"status\":\"completed\",\"action\":{\"type\":\"search\",\"query\":\"rust\"}},{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"Rust answer\",\"annotations\":[{\"type\":\"url_citation\",\"url\":\"https://example.com\",\"title\":\"Example\",\"start_index\":0,\"end_index\":4}]}]}]}}\n\n"
                    } else {
                        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-function\"}}\n\n\
                         data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-function\",\"output\":[{\"type\":\"function_call\",\"id\":\"fc-1\",\"call_id\":\"call-1\",\"name\":\"lookup\",\"arguments\":\"{\\\"q\\\":\\\"rust\\\"}\"}]}}\n\n"
                    };
                    (
                        [(header::CONTENT_TYPE, "text/event-stream")],
                        Body::from(body),
                    )
                        .into_response()
                }
            });
            axum::serve(
                listener,
                Router::new()
                    .route("/", bootstrap)
                    .route("/backend-api/sentinel/chat-requirements/prepare", prepare)
                    .route("/backend-api/sentinel/chat-requirements/finalize", finalize)
                    .route(
                        "/backend-api/models",
                        get(|| async {
                            Json(json!({
                                "models": [{"slug":"gpt-test","supported_in_api":true}]
                            }))
                        }),
                    )
                    .route("/backend-api/codex/models", models)
                    .route("/backend-api/codex/responses", responses),
            )
            .await
            .expect("upstream server");
        });

        let account_path = std::env::temp_dir().join(format!(
            "chatgpt2api-rust-native-responses-tools-{}-{}.json",
            std::process::id(),
            NATIVE_MESSAGE_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::write(
            &account_path,
            r#"[{"access_token":"codex-token","status":"正常","source_type":"codex","type":"free","models":["gpt-test"]}]"#,
        )
        .expect("account snapshot");
        let state = AppState::new(AppConfig {
            version: "test".to_owned(),
            auth_key: Some("client".to_owned()),
            models: vec!["gpt-test".to_owned()],
            upstream_base_url: Some(format!("http://{address}")),
            upstream_auth: None,
            auth_keys_path: None,
            models_path: None,
            accounts_path: Some(account_path.clone()),
            upstream_protocol: UpstreamProtocol::ChatGpt,
        })
        .expect("state");
        state
            .account_type_catalog
            .set_codex_client_version_for_test(Some("0.147.0".to_owned()));

        let anthropic_request = validate_message_request(json!({
            "model": "gpt-test",
            "max_tokens": 8,
            "messages": [{"role":"user","content":"lookup rust"}],
            "tools": [{"name":"lookup","input_schema":{"type":"object"}}],
            "tool_choice": {"type":"auto"}
        }))
        .expect("Anthropic tool request fixture");
        let chat_payload = to_chat_payload(&anthropic_request).expect("Chat tool payload");
        assert!(
            native_codex_response_payload(chat_payload.as_object().expect("object")).is_ok(),
            "Codex payload rejected Chat projection: {chat_payload}"
        );

        let function_response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","input":"lookup rust","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}"#,
                    ))
                    .expect("function request"),
            )
            .await
            .expect("function response");
        assert_eq!(function_response.status(), StatusCode::OK);
        let function_body = function_response
            .into_body()
            .collect()
            .await
            .expect("function body")
            .to_bytes();
        let function_value: Value = serde_json::from_slice(&function_body).expect("function json");
        assert_eq!(function_value["output"][0]["id"], "fc-1");
        assert_eq!(function_value["output"][0]["call_id"], "call-1");
        assert_eq!(state.account_store.inflight(), 0);

        let anthropic_function = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "client")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","max_tokens":8,"messages":[{"role":"user","content":"lookup rust"}],"tools":[{"name":"lookup","input_schema":{"type":"object"}}],"tool_choice":{"type":"auto"}}"#,
                    ))
                    .expect("Anthropic function request"),
            )
            .await
            .expect("Anthropic function response");
        let anthropic_status = anthropic_function.status();
        let body = anthropic_function
            .into_body()
            .collect()
            .await
            .expect("Anthropic function body")
            .to_bytes();
        assert_eq!(
            anthropic_status,
            StatusCode::OK,
            "Anthropic function body: {}; captured={}",
            String::from_utf8_lossy(&body),
            serde_json::to_string(&*captured.lock().await).unwrap_or_default()
        );
        let value: Value = serde_json::from_slice(&body).expect("Anthropic function json");
        assert_eq!(value["content"][0]["type"], "tool_use");
        assert_eq!(value["content"][0]["name"], "lookup");
        assert_eq!(value["stop_reason"], "tool_use");
        assert_eq!(state.account_store.inflight(), 0);

        let search_response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/responses")
                    .header(header::AUTHORIZATION, "Bearer client")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","input":"rust news","stream":true,"tools":[{"type":"web_search_preview","search_context_size":"low"}]}"#,
                    ))
                    .expect("search request"),
            )
            .await
            .expect("search response");
        assert_eq!(search_response.status(), StatusCode::OK);
        let search_body = search_response
            .into_body()
            .collect()
            .await
            .expect("search body")
            .to_bytes();
        let search_text = String::from_utf8(search_body.to_vec()).expect("search sse");
        assert!(search_text.contains("response.output_item.done"));
        assert!(search_text.contains("web_search_call"));
        assert!(search_text.contains("url_citation"));
        assert_eq!(state.account_store.inflight(), 0);

        let anthropic_search = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "client")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(
                        r#"{"model":"gpt-test","max_tokens":8,"stream":true,"tools":[{"type":"web_search_20250305","name":"web_search"}],"messages":[{"role":"user","content":"rust news"}]}"#,
                    ))
                    .expect("Anthropic search request"),
            )
            .await
            .expect("Anthropic search response");
        assert_eq!(anthropic_search.status(), StatusCode::OK);
        let anthropic_search_body = anthropic_search
            .into_body()
            .collect()
            .await
            .expect("Anthropic search body")
            .to_bytes();
        let anthropic_search_text =
            String::from_utf8(anthropic_search_body.to_vec()).expect("Anthropic search SSE");
        assert!(anthropic_search_text.contains("server_tool_use"));
        assert!(anthropic_search_text.contains("web_search_tool_result"));
        assert!(anthropic_search_text.contains("citations_delta"));
        assert!(anthropic_search_text.contains("web_search_requests"));
        assert!(anthropic_search_text.contains(r#""output_tokens":4"#));
        assert!(anthropic_search_text.contains(r#""stop_reason":"end_turn""#));
        let mut replay_blocks = Vec::<Option<Value>>::new();
        for frame in anthropic_search_text
            .split("\n\n")
            .filter(|frame| !frame.is_empty())
        {
            let Some((event, data)) = frame.split_once("\ndata: ") else {
                continue;
            };
            let event = event.strip_prefix("event: ").unwrap_or_default();
            let value: Value = serde_json::from_str(data).expect("Anthropic SSE JSON");
            if event == "event: content_block_start" || event == "content_block_start" {
                let index = value["index"].as_u64().expect("content block index") as usize;
                if replay_blocks.len() <= index {
                    replay_blocks.resize_with(index + 1, || None);
                }
                replay_blocks[index] = Some(value["content_block"].clone());
            } else if event == "content_block_delta" {
                let index = value["index"].as_u64().expect("delta index") as usize;
                let block = replay_blocks[index].as_mut().expect("started block");
                match value["delta"]["type"].as_str() {
                    Some("text_delta") => {
                        let text = block["text"].as_str().unwrap_or_default().to_owned();
                        block["text"] = json!(format!("{text}{}", value["delta"]["text"]));
                    }
                    Some("citations_delta") => {
                        let mut citations =
                            block["citations"].as_array().cloned().unwrap_or_default();
                        citations.push(value["delta"]["citation"].clone());
                        block["citations"] = Value::Array(citations);
                    }
                    Some("input_json_delta") => {}
                    other => panic!("unexpected Anthropic delta: {other:?}"),
                }
            }
        }
        let replay_content = replay_blocks
            .into_iter()
            .map(|block| block.expect("every replay block started"))
            .collect::<Vec<_>>();
        assert_eq!(replay_content[0]["type"], "server_tool_use");
        assert_eq!(replay_content[1]["type"], "web_search_tool_result");
        assert!(!replay_content[1]["content"].as_array().unwrap().is_empty());
        assert_eq!(replay_content[2]["type"], "text");
        assert!(
            !replay_content[2]["citations"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        let replay_request = serde_json::to_vec(&json!({
            "model":"gpt-test",
            "max_tokens":8,
            "stream":true,
            "tools":[{"type":"web_search_20250305","name":"web_search"}],
            "messages":[{"role":"assistant","content":replay_content}]
        }))
        .expect("replay request");
        let replay_response = state
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", "client")
                    .header("anthropic-version", "2023-06-01")
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from(replay_request))
                    .expect("replay request builder"),
            )
            .await
            .expect("replay response");
        assert_eq!(replay_response.status(), StatusCode::OK);
        let _ = replay_response
            .into_body()
            .collect()
            .await
            .expect("replay body");
        let replay_payload = captured
            .lock()
            .await
            .last()
            .cloned()
            .expect("replay payload");
        let replay_payload_text =
            serde_json::to_string(&replay_payload).expect("replay payload JSON");
        assert!(!replay_payload_text.contains("chatgpt2api-search-v1:"));
        assert!(!replay_payload_text.contains("<tool_"));
        assert_eq!(state.account_store.inflight(), 0);

        let payloads = captured.lock().await;
        assert_eq!(payloads.len(), 5);
        assert_eq!(payloads[0]["tools"][0]["type"], "function");
        assert_eq!(payloads[0]["tools"][0]["name"], "lookup");
        assert_eq!(payloads[1]["tools"][0]["type"], "function");
        assert_eq!(payloads[1]["max_output_tokens"], 8);
        assert_eq!(payloads[2]["tools"][0]["type"], "web_search");
        assert_eq!(payloads[3]["tools"][0]["type"], "web_search");
        assert!(payloads[4]["input"].as_array().unwrap().iter().any(|item| {
            item["content"].as_array().unwrap().iter().any(|block| {
                block["type"] == "input_text"
                    && block["text"]
                        .as_str()
                        .unwrap_or_default()
                        .contains("Web search")
            })
        }));
        state.account_type_catalog.shutdown().await;
        upstream.abort();
        let _ = upstream.await;
        fs::remove_file(account_path).expect("cleanup");
    }

    #[test]
    fn native_responses_tool_payload_and_output_round_trip() {
        let function = json!({
            "type": "function",
            "name": "lookup",
            "description": "Find a value",
            "parameters": {"type": "object", "properties": {"q": {"type": "string"}}},
            "strict": true
        });
        let tool_object = validate_responses_payload(json!({
            "model": "gpt-test",
            "input": "hello",
            "tools": [function]
        }))
        .expect("request object");
        let payload = native_codex_responses_payload(&tool_object).expect("tool payload");
        assert_eq!(payload["tool_choice"], "auto");
        assert_eq!(payload["tools"][0]["type"], "function");
        assert_eq!(payload["tools"][0]["name"], "lookup");
        assert_eq!(payload["tools"][0]["strict"], true);

        let completed = br#"data: {"type":"response.created","response":{"id":"resp-1"}}

data: {"type":"response.output_item.done","output_index":0,"item":{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{\"q\":\"rust\"}"}}

data: {"type":"response.completed","response":{"id":"resp-1","output":[{"type":"function_call","id":"fc-1","call_id":"call-1","name":"lookup","arguments":"{\"q\":\"rust\"}"}]}}

"#;
        let response = native_codex_responses_json(completed, "gpt-test").expect("response");
        assert_eq!(response["output"][0]["id"], "fc-1");
        assert_eq!(response["output"][0]["call_id"], "call-1");
        assert_eq!(response["output"][0]["arguments"], "{\"q\":\"rust\"}");
    }

    #[test]
    fn native_responses_web_search_payload_and_citations_round_trip() {
        let object = validate_responses_payload(json!({
            "model": "gpt-test",
            "input": "latest news",
            "tools": [{
                "type": "web_search_preview",
                "search_context_size": "high",
                "user_location": {"type": "approximate", "country": "CN"}
            }]
        }))
        .expect("request object");
        let payload = native_codex_responses_payload(&object).expect("search payload");
        assert_eq!(payload["tools"][0]["type"], "web_search");
        assert_eq!(payload["tools"][0]["search_context_size"], "high");
        let completed = br#"data: {"type":"response.created","response":{"id":"resp-search"}}

data: {"type":"response.completed","response":{"id":"resp-search","output":[{"type":"web_search_call","id":"ws-1","status":"completed","action":{"type":"search","query":"latest news"}},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Answer","annotations":[{"type":"url_citation","url":"https://example.com","title":"Example","start_index":0,"end_index":6}]}]}]}}

"#;
        let response = native_codex_responses_json(completed, "gpt-test").expect("response");
        assert_eq!(response["output"][0]["id"], "ws-1");
        assert_eq!(
            response["output"][1]["content"][0]["annotations"][0]["url"],
            "https://example.com"
        );
    }

    #[test]
    fn native_responses_tool_history_preserves_call_id_and_rejects_unknown_items() {
        let object = validate_responses_payload(json!({
            "model": "gpt-test",
            "input": [
                {
                    "type": "function_call",
                    "id": "fc-1",
                    "call_id": "call-1",
                    "name": "lookup",
                    "arguments": "{\"q\":\"rust\"}"
                },
                {
                    "type": "function_call_output",
                    "call_id": "call-1",
                    "output": "sunny"
                }
            ]
        }))
        .expect("request object");
        let payload = native_codex_responses_payload(&object).expect("payload");
        assert_eq!(payload["input"][0]["id"], "fc-1");
        assert_eq!(payload["input"][0]["call_id"], "call-1");
        assert_eq!(payload["input"][1]["call_id"], "call-1");

        let invalid = validate_responses_payload(json!({
            "model": "gpt-test",
            "input": [{"type": "function_call_output", "call_id": "call-1", "future": true}]
        }))
        .expect("request object");
        assert_eq!(
            native_codex_responses_payload(&invalid)
                .expect_err("unknown history field must fail closed")
                .status,
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn native_responses_tools_reject_unknown_fields_and_missing_terminal() {
        let tool_object = validate_responses_payload(json!({
            "model": "gpt-test",
            "input": "hello",
            "tools": [{"type": "function", "name": "lookup", "future": true}]
        }))
        .expect("request object");
        assert_eq!(
            native_codex_responses_payload(&tool_object)
                .expect_err("unknown tool field must fail closed")
                .status,
            StatusCode::BAD_REQUEST
        );
        let incomplete = b"data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp-1\"}}\n\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n";
        assert!(native_codex_responses_json(incomplete, "gpt-test").is_err());
    }

    #[test]
    fn native_responses_unknown_event_fails_closed() {
        let body = br#"data: {"type":"response.created","response":{"id":"resp-1"}}

data: {"type":"response.future_event","future":true}

data: {"type":"response.completed","response":{"id":"resp-1","output":[]}}

"#;
        assert!(
            native_codex_responses_json(body, "gpt-test").is_err(),
            "unknown upstream event must not be silently discarded"
        );
    }

    #[test]
    fn chat_effort_aliases_match_python_contract() {
        let mut object = json!({
            "reasoning": {"effort": " NONE "},
            "thinking_effort": " HIGH ",
        })
        .as_object()
        .cloned()
        .expect("object");
        super::protocol_chat::normalize_chat_effort(&mut object);
        assert_eq!(object["reasoning"]["effort"], "auto");
        assert_eq!(object["thinking_effort"], "high");
    }

    #[test]
    fn constant_time_comparison_does_not_accept_prefixes() {
        assert!(constant_time_equal(b"secret", b"secret"));
        assert!(!constant_time_equal(b"secret", b"secret2"));
    }
}
