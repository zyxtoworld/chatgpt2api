use axum::http::header;
use base64::Engine;
use reqwest::RequestBuilder;
use serde_json::{Map, Value, json};
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
    native_browser_headers_with_referer(request, context, "https://chatgpt.com/")
}

pub(crate) fn native_browser_headers_with_referer(
    request: RequestBuilder,
    context: &NativeRequestContext,
    referer: &str,
) -> RequestBuilder {
    request
        .header(header::USER_AGENT, NATIVE_USER_AGENT)
        .header("Origin", NATIVE_ORIGIN)
        .header(header::REFERER, referer)
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
    env::var("CODEX_CLIENT_VERSION")
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
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .ok_or_else(ApiError::invalid_request)?;
    if original_role == "tool" {
        let call_id = object
            .get("tool_call_id")
            .and_then(Value::as_str)
            .map(str::trim)
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
        return Err(ApiError::invalid_request());
    }
    let content = native_codex_content_parts(object.get("content"), role)?;
    let tool_calls = object
        .get("tool_calls")
        .filter(|value| !value.is_null())
        .map(|value| value.as_array().ok_or_else(ApiError::invalid_request))
        .transpose()?;
    let mut result = Vec::new();
    if !content.is_empty() {
        result.push(json!({"type":"message","role":role,"content":content}));
    }
    if let Some(tool_calls) = tool_calls {
        for call in tool_calls {
            let call = call.as_object().ok_or_else(ApiError::invalid_request)?;
            if call.len() != 3 || call.get("type").and_then(Value::as_str) != Some("function") {
                return Err(ApiError::invalid_request());
            }
            let id = call
                .get("id")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            let function = call
                .get("function")
                .and_then(Value::as_object)
                .ok_or_else(ApiError::invalid_request)?;
            if function.len() != 2
                || !function.contains_key("name")
                || !function.contains_key("arguments")
            {
                return Err(ApiError::invalid_request());
            }
            let name = function
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            let arguments = function
                .get("arguments")
                .and_then(Value::as_str)
                .ok_or_else(ApiError::invalid_request)?;
            result.push(json!({
                "type":"function_call",
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

fn native_codex_content_parts(content: Option<&Value>, role: &str) -> Result<Vec<Value>, ApiError> {
    let Some(content) = content else {
        return Ok(Vec::new());
    };
    match content {
        Value::String(text) => Ok(vec![json!({
            "type": if role == "assistant" { "output_text" } else { "input_text" },
            "text": text,
        })]),
        Value::Null => Ok(Vec::new()),
        Value::Array(parts) => parts
            .iter()
            .map(|part| {
                let part = part.as_object().ok_or_else(ApiError::invalid_request)?;
                let kind = part
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::invalid_request)?;
                let mapped = match kind {
                    "text" | "input_text" | "output_text" => {
                        if part.keys().any(|key| {
                            !matches!(key.as_str(), "type" | "text" | "prompt_cache_breakpoint")
                        }) || part
                            .get("prompt_cache_breakpoint")
                            .is_some_and(|value| !value.is_null())
                        {
                            return Err(ApiError::invalid_request());
                        }
                        let text = part
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or_else(ApiError::invalid_request)?;
                        json!({
                            "type": if role == "assistant" { "output_text" } else { "input_text" },
                            "text": text,
                        })
                    }
                    "image_url" | "input_image" if role == "user" => {
                        if part.keys().any(|key| {
                            !matches!(
                                key.as_str(),
                                "type" | "image_url" | "prompt_cache_breakpoint"
                            )
                        }) || part
                            .get("prompt_cache_breakpoint")
                            .is_some_and(|value| !value.is_null())
                        {
                            return Err(ApiError::invalid_request());
                        }
                        let image = part
                            .get("image_url")
                            .ok_or_else(ApiError::invalid_request)?;
                        let (url, detail) = if let Some(url) = image.as_str() {
                            (url.to_owned(), None)
                        } else {
                            let image = image.as_object().ok_or_else(ApiError::invalid_request)?;
                            if image.keys().any(|key| key != "url" && key != "detail") {
                                return Err(ApiError::invalid_request());
                            }
                            let url = image
                                .get("url")
                                .and_then(Value::as_str)
                                .filter(|url| !url.trim().is_empty())
                                .ok_or_else(ApiError::invalid_request)?
                                .to_owned();
                            let detail = image.get("detail").cloned();
                            if detail.as_ref().is_some_and(|value| {
                                !matches!(value.as_str(), Some("auto" | "low" | "high"))
                            }) {
                                return Err(ApiError::invalid_request());
                            }
                            (url, detail)
                        };
                        if url.trim().is_empty() {
                            return Err(ApiError::invalid_request());
                        }
                        let mut value = json!({"type":"input_image","image_url":url});
                        if let Some(detail) = detail.filter(|value| !value.is_null()) {
                            value["detail"] = detail;
                        }
                        value
                    }
                    "input_audio" if role == "user" => {
                        if part.keys().any(|key| {
                            !matches!(
                                key.as_str(),
                                "type" | "input_audio" | "prompt_cache_breakpoint"
                            )
                        }) || part
                            .get("prompt_cache_breakpoint")
                            .is_some_and(|value| !value.is_null())
                        {
                            return Err(ApiError::invalid_request());
                        }
                        let audio = part
                            .get("input_audio")
                            .and_then(Value::as_object)
                            .ok_or_else(ApiError::invalid_request)?;
                        if audio.len() != 2 {
                            return Err(ApiError::invalid_request());
                        }
                        let data = audio
                            .get("data")
                            .and_then(Value::as_str)
                            .filter(|data| !data.is_empty())
                            .ok_or_else(ApiError::invalid_request)?;
                        let format = audio
                            .get("format")
                            .and_then(Value::as_str)
                            .filter(|format| matches!(*format, "wav" | "mp3"))
                            .ok_or_else(ApiError::invalid_request)?;
                        let decoded = base64::engine::general_purpose::STANDARD
                            .decode(data)
                            .map_err(|_| ApiError::invalid_request())?;
                        let _ = decoded;
                        json!({
                            "type":"input_audio",
                            "audio_url":format!(
                                "data:audio/{};base64,{data}",
                                if format == "wav" { "wav" } else { "mpeg" }
                            ),
                        })
                    }
                    _ => return Err(ApiError::invalid_request()),
                };
                Ok(mapped)
            })
            .collect(),
        _ => Err(ApiError::invalid_request()),
    }
}

fn native_tool_output(content: Option<&Value>) -> Result<String, ApiError> {
    match content {
        Some(Value::String(text)) => Ok(text.clone()),
        Some(Value::Array(_)) => {
            let parts = native_codex_content_parts(content, "tool")?;
            let mut output = String::new();
            for part in parts {
                output.push_str(
                    part.get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(ApiError::invalid_request)?,
                );
            }
            Ok(output)
        }
        Some(Value::Null) | None => Ok(String::new()),
        Some(value) => serde_json::to_string(value).map_err(|_| ApiError::invalid_request()),
    }
}

fn native_chat_web_search_tool(options: &Value) -> Result<Value, ApiError> {
    let options = options.as_object().ok_or_else(ApiError::invalid_request)?;
    if options
        .keys()
        .any(|key| !matches!(key.as_str(), "search_context_size" | "user_location"))
    {
        return Err(ApiError::invalid_request());
    }
    let mut tool = Map::new();
    tool.insert("type".to_owned(), json!("web_search"));
    if let Some(value) = options.get("search_context_size") {
        tool.insert("search_context_size".to_owned(), value.clone());
    }
    if let Some(location) = options
        .get("user_location")
        .filter(|value| !value.is_null())
    {
        let location = location.as_object().ok_or_else(ApiError::invalid_request)?;
        let approximate = location
            .get("approximate")
            .and_then(Value::as_object)
            .ok_or_else(ApiError::invalid_request)?;
        let mut normalized = Map::new();
        normalized.insert("type".to_owned(), json!("approximate"));
        for field in ["city", "country", "region", "timezone"] {
            if let Some(value) = approximate.get(field) {
                normalized.insert(field.to_owned(), value.clone());
            }
        }
        tool.insert("user_location".to_owned(), Value::Object(normalized));
    }
    native_codex_tool(&Value::Object(tool))
}

fn native_chat_response_text(value: &Value) -> Result<Option<Value>, ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    match kind {
        "text" if object.len() == 1 => Ok(None),
        "json_schema" => {
            let schema = object
                .get("json_schema")
                .and_then(Value::as_object)
                .ok_or_else(ApiError::invalid_request)?;
            if object
                .keys()
                .any(|key| key != "type" && key != "json_schema")
                || schema
                    .keys()
                    .any(|key| !matches!(key.as_str(), "name" | "schema" | "strict"))
                || schema
                    .get("name")
                    .and_then(Value::as_str)
                    .is_none_or(|name| name.trim().is_empty())
                || !schema.get("schema").is_some_and(Value::is_object)
                || schema
                    .get("strict")
                    .is_some_and(|value| !value.is_boolean())
            {
                return Err(ApiError::invalid_request());
            }
            let mut format = schema.clone();
            format.insert("type".to_owned(), json!("json_schema"));
            Ok(Some(json!({"format": format})))
        }
        _ => Err(ApiError::invalid_request()),
    }
}

pub(super) fn native_codex_response_payload(
    object: &Map<String, Value>,
) -> Result<Value, ApiError> {
    let mut input = Vec::new();
    if let Some(messages) = object.get("messages").and_then(Value::as_array) {
        for message in messages {
            for item in native_codex_message(message)? {
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
    let mut tools = object
        .get("tools")
        .filter(|value| !value.is_null())
        .map(|value| value.as_array().ok_or_else(ApiError::invalid_request))
        .transpose()?
        .map(|tools| {
            tools
                .iter()
                .map(native_codex_tool)
                .collect::<Result<Vec<_>, _>>()
        })
        .transpose()?
        .unwrap_or_default();
    if let Some(options) = object
        .get("web_search_options")
        .filter(|value| !value.is_null())
    {
        let web_tool = native_chat_web_search_tool(options)?;
        if let Some(existing) = tools
            .iter_mut()
            .find(|tool| tool.get("type").and_then(Value::as_str) == Some("web_search"))
        {
            if let (Some(existing), Some(web_tool)) =
                (existing.as_object_mut(), web_tool.as_object())
            {
                for (key, value) in web_tool {
                    existing.insert(key.clone(), value.clone());
                }
            }
        } else {
            tools.push(web_tool);
        }
    }
    if !tools.is_empty() {
        payload["tools"] = Value::Array(tools);
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
    if let Some(reasoning) = object.get("reasoning").filter(|value| !value.is_null()) {
        payload["reasoning"] = reasoning.clone();
    }
    if let Some(prompt_cache_key) = object
        .get("prompt_cache_key")
        .filter(|value| !value.is_null())
    {
        payload["prompt_cache_key"] = prompt_cache_key.clone();
    }
    if let Some(service_tier) = object.get("service_tier").filter(|value| !value.is_null()) {
        payload["service_tier"] = match service_tier.as_str() {
            Some("fast") => json!("priority"),
            Some("default") => Value::Null,
            Some("priority" | "flex") => service_tier.clone(),
            _ => return Err(ApiError::invalid_request()),
        };
        if payload["service_tier"].is_null() {
            payload
                .as_object_mut()
                .expect("payload object")
                .remove("service_tier");
        }
    }
    if let Some(verbosity) = object.get("verbosity").filter(|value| !value.is_null()) {
        payload["text"] = json!({"verbosity": verbosity});
    }
    if let Some(response_format) = object
        .get("response_format")
        .filter(|value| !value.is_null())
        && let Some(text) = native_chat_response_text(response_format)?
    {
        let mut merged = payload
            .get("text")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if let Some(format) = text.get("format") {
            merged.insert("format".to_owned(), format.clone());
        }
        payload["text"] = Value::Object(merged);
    }
    Ok(payload)
}
