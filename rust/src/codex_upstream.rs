use axum::http::header;
use reqwest::RequestBuilder;
use serde_json::{Map, Value, json};
use std::collections::HashSet;
use std::env;

use super::protocol_codex_payload::native_codex_tool;
use super::{
    ApiError, CODEX_RESPONSES_MODEL, NATIVE_CLIENT_BUILD_NUMBER, NATIVE_CLIENT_VERSION,
    NATIVE_ORIGIN, NATIVE_SEC_CH_UA, NATIVE_USER_AGENT, is_semver, native_message_id,
};

#[derive(Clone)]
pub(crate) struct NativeRequestContext {
    pub(crate) device_id: String,
    pub(crate) session_id: String,
}

impl NativeRequestContext {
    pub(crate) fn new() -> Self {
        Self {
            device_id: native_message_id(),
            session_id: native_message_id(),
        }
    }
}

pub(crate) fn native_browser_headers(
    request: RequestBuilder,
    context: &NativeRequestContext,
) -> RequestBuilder {
    request
        .header(header::USER_AGENT, NATIVE_USER_AGENT)
        .header("Origin", NATIVE_ORIGIN)
        .header("Referer", "https://chatgpt.com/")
        .header("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8,en-US;q=0.7")
        .header("Cache-Control", "no-cache")
        .header("Pragma", "no-cache")
        .header("Priority", "u=1, i")
        .header("Sec-Ch-Ua", NATIVE_SEC_CH_UA)
        .header("Sec-Ch-Ua-Arch", "\"x86\"")
        .header("Sec-Ch-Ua-Bitness", "\"64\"")
        .header("Sec-Ch-Ua-Full-Version", "\"143.0.3650.96\"")
        .header(
            "Sec-Ch-Ua-Full-Version-List",
            "\"Microsoft Edge\";v=\"143.0.3650.96\", \"Chromium\";v=\"143.0.7499.147\", \"Not A(Brand\";v=\"24.0.0.0\"",
        )
        .header("Sec-Ch-Ua-Mobile", "?0")
        .header("Sec-Ch-Ua-Model", "\"\"")
        .header("Sec-Ch-Ua-Platform", "\"Windows\"")
        .header("Sec-Ch-Ua-Platform-Version", "\"19.0.0\"")
        .header("OAI-Device-Id", &context.device_id)
        .header("OAI-Session-Id", &context.session_id)
        .header("OAI-Language", "zh-CN")
        .header("OAI-Client-Version", NATIVE_CLIENT_VERSION)
        .header("OAI-Client-Build-Number", NATIVE_CLIENT_BUILD_NUMBER)
}

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

pub(super) fn native_codex_message(message: &Value) -> Result<Vec<Value>, ApiError> {
    let object = message.as_object().ok_or_else(ApiError::invalid_request)?;
    let original_role = object
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    if original_role == "tool" {
        let call_id = object
            .get("tool_call_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(ApiError::invalid_request)?;
        let output = native_tool_output(object.get("content"))?;
        return Ok(vec![json!({
            "type": "function_call_output",
            "call_id": call_id,
            "output": output,
        })]);
    }
    let role = if original_role == "system" {
        "developer"
    } else {
        original_role
    };
    if !matches!(role, "developer" | "user" | "assistant") {
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
    let tool_calls = object
        .get("tool_calls")
        .filter(|value| !value.is_null())
        .map(|value| value.as_array().ok_or_else(ApiError::invalid_request))
        .transpose()?;
    let mut result = Vec::new();
    if tool_calls.as_ref().is_none_or(|calls| calls.is_empty()) || !content.is_empty() {
        result.push(json!({"type":"message","role":role,"content":content}));
    }
    if let Some(tool_calls) = tool_calls {
        for call in tool_calls {
            let call = call.as_object().ok_or_else(ApiError::invalid_request)?;
            if call.get("type").and_then(Value::as_str) != Some("function") {
                return Err(ApiError::invalid_request());
            }
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            let function = call
                .get("function")
                .and_then(Value::as_object)
                .ok_or_else(ApiError::invalid_request)?;
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            let arguments = function
                .get("arguments")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            let parsed: Value =
                serde_json::from_str(arguments).map_err(|_| ApiError::invalid_request())?;
            if !parsed.is_object() {
                return Err(ApiError::invalid_request());
            }
            result.push(json!({
                "type":"function_call",
                "id":id,
                "call_id":id,
                "name":name,
                "arguments":arguments
            }));
        }
    }
    if result.is_empty() {
        return Err(ApiError::invalid_request());
    }
    Ok(result)
}

fn native_tool_output(content: Option<&Value>) -> Result<String, ApiError> {
    match content {
        Some(Value::String(text)) => Ok(text.clone()),
        Some(Value::Array(parts)) => {
            let mut output = String::new();
            for part in parts {
                let part = part.as_object().ok_or_else(ApiError::invalid_request)?;
                if !matches!(
                    part.get("type").and_then(Value::as_str),
                    Some("text") | Some("input_text") | Some("output_text")
                ) {
                    return Err(ApiError::invalid_request());
                }
                output.push_str(
                    part.get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(ApiError::invalid_request)?,
                );
            }
            Ok(output)
        }
        Some(Value::Null) | None => Ok(String::new()),
        Some(_) => Err(ApiError::invalid_request()),
    }
}

pub(super) fn native_codex_response_payload(
    object: &Map<String, Value>,
) -> Result<Value, ApiError> {
    let mut input = Vec::new();
    let mut function_call_ids = HashSet::new();
    if let Some(messages) = object.get("messages").and_then(Value::as_array) {
        for message in messages {
            for item in native_codex_message(message)? {
                match item.get("type").and_then(Value::as_str) {
                    Some("function_call") => {
                        let call_id = item
                            .get("call_id")
                            .and_then(Value::as_str)
                            .ok_or_else(ApiError::invalid_request)?;
                        function_call_ids.insert(call_id.to_owned());
                    }
                    Some("function_call_output") => {
                        let call_id = item
                            .get("call_id")
                            .and_then(Value::as_str)
                            .ok_or_else(ApiError::invalid_request)?;
                        if !function_call_ids.contains(call_id) {
                            return Err(ApiError::invalid_request());
                        }
                    }
                    _ => {}
                }
                input.push(item);
            }
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
        "tool_choice": "auto",
        "parallel_tool_calls": object
            .get("parallel_tool_calls")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    });
    if let Some(max_tokens) = object.get("max_tokens").filter(|value| !value.is_null()) {
        payload["max_output_tokens"] = max_tokens.clone();
    }
    if let Some(tools) = object.get("tools").filter(|value| !value.is_null()) {
        let tools = tools.as_array().ok_or_else(ApiError::invalid_request)?;
        payload["tools"] = Value::Array(
            tools
                .iter()
                .map(native_codex_tool)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    if let Some(choice) = object.get("tool_choice").filter(|value| !value.is_null()) {
        let valid_choice = matches!(choice.as_str(), Some("auto"))
            || matches!(choice.as_object(), Some(choice)
                if choice.len() == 1
                    && choice.get("type").and_then(Value::as_str) == Some("auto"));
        if !valid_choice {
            return Err(ApiError::invalid_request());
        }
    }
    if let Some(effort) = object
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .or_else(|| object.get("thinking_effort").and_then(Value::as_str))
    {
        payload["reasoning"] = json!({"effort": effort});
    }
    Ok(payload)
}
