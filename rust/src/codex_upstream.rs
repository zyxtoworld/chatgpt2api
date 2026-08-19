use axum::http::header;
use reqwest::RequestBuilder;
use serde_json::{Map, Value, json};
use std::env;

use super::{ApiError, CODEX_RESPONSES_MODEL, is_semver};

pub(super) fn codex_client_version() -> Option<String> {
    env::var("CODEX_MODELS_CLIENT_VERSION")
        .ok()
        .and_then(|value| parse_codex_client_version(Some(&value)))
}

pub(super) fn parse_codex_client_version(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    is_semver(value).then(|| value.to_owned())
}

pub(super) fn codex_request_headers(
    request: RequestBuilder,
    token: &str,
    account_id: Option<String>,
    version: Option<&str>,
) -> Option<RequestBuilder> {
    let version = version?.to_owned();
    let request = request
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .header(header::ACCEPT, "text/event-stream")
        .header(header::CONTENT_TYPE, "application/json")
        .header("originator", "codex_cli_rs")
        .header("User-Agent", format!("codex_cli_rs/{version}"))
        .header("version", version);
    Some(match account_id {
        Some(account_id) => request.header("ChatGPT-Account-ID", account_id),
        None => request,
    })
}

pub(super) fn native_codex_message(message: &Value) -> Result<Value, ApiError> {
    let object = message.as_object().ok_or_else(ApiError::invalid_request)?;
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let role = if role == "system" { "developer" } else { role };
    if !matches!(role, "developer" | "user" | "assistant")
        || object
            .get("tool_calls")
            .is_some_and(|value| !value.is_null())
    {
        return Err(ApiError::unavailable());
    }
    let content = object
        .get("content")
        .and_then(|value| match value {
            Value::String(text) => Some(vec![json!({"type":"input_text","text":text})]),
            Value::Array(parts) => {
                let mut result = Vec::new();
                for part in parts {
                    let part = part.as_object()?;
                    let kind = part.get("type")?.as_str()?;
                    if !matches!(kind, "text" | "input_text" | "output_text") {
                        return None;
                    }
                    let text = part.get("text")?.as_str()?;
                    result.push(json!({"type":"input_text","text":text}));
                }
                Some(result)
            }
            Value::Null if role == "assistant" => Some(Vec::new()),
            _ => None,
        })
        .ok_or_else(ApiError::unavailable)?;
    Ok(json!({"type":"message","role":role,"content":content}))
}

pub(super) fn native_codex_response_payload(
    object: &Map<String, Value>,
) -> Result<Value, ApiError> {
    if object
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
        || object
            .get("tool_choice")
            .is_some_and(|choice| !choice.is_null())
    {
        return Err(ApiError::unavailable());
    }
    let mut input = Vec::new();
    if let Some(messages) = object.get("messages").and_then(Value::as_array) {
        for message in messages {
            input.push(native_codex_message(message)?);
        }
    } else if let Some(prompt) = object.get("prompt").and_then(Value::as_str) {
        input.push(json!({
            "type":"message",
            "role":"user",
            "content":[{"type":"input_text","text":prompt.trim()}]
        }));
    }
    if input.is_empty() {
        return Err(ApiError::invalid_request());
    }
    let mut payload = json!({
        "model": object
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| *model != "auto")
            .map_or_else(|| json!(CODEX_RESPONSES_MODEL), |model| json!(model)),
        "input": input,
        "store": false,
        "stream": object.get("stream").and_then(Value::as_bool).unwrap_or(false),
    });
    if let Some(effort) = object
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .or_else(|| object.get("thinking_effort").and_then(Value::as_str))
    {
        payload["reasoning"] = json!({"effort": effort});
    }
    Ok(payload)
}
