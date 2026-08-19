use super::{
    ApiError, DEFAULT_POW_SCRIPT, MAX_POW_SCRIPT_SOURCES, Map, NATIVE_CLIENT_BUILD_NUMBER,
    NATIVE_CLIENT_VERSION, NATIVE_ORIGIN, NATIVE_SEC_CH_UA, NATIVE_USER_AGENT, Value,
    native_message_id, sse_delimiter,
};
use axum::http::header;
use base64::Engine;
use reqwest::RequestBuilder;
use serde_json::json;
use std::io;

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

#[derive(Clone, Default)]
pub(crate) struct NativePowResources {
    pub(crate) script_sources: Vec<String>,
    pub(crate) data_build: String,
}

fn html_attribute(tag: &str, name: &str) -> Option<String> {
    let bytes = tag.as_bytes();
    let mut index = 0usize;
    while index < bytes.len() {
        while index < bytes.len()
            && (bytes[index].is_ascii_whitespace() || matches!(bytes[index], b'<' | b'/' | b'>'))
        {
            index += 1;
        }
        let name_start = index;
        while index < bytes.len()
            && !bytes[index].is_ascii_whitespace()
            && !matches!(bytes[index], b'=' | b'/' | b'>')
        {
            index += 1;
        }
        if name_start == index {
            index = index.saturating_add(1);
            continue;
        }
        let attribute_name = &tag[name_start..index];
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index >= bytes.len() || bytes[index] != b'=' {
            continue;
        }
        index += 1;
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let value = if index < bytes.len() && matches!(bytes[index], b'"' | b'\'') {
            let quote = bytes[index];
            index += 1;
            let value_start = index;
            while index < bytes.len() && bytes[index] != quote {
                index += 1;
            }
            let value = tag[value_start..index].to_owned();
            if index < bytes.len() {
                index += 1;
            }
            value
        } else {
            let value_start = index;
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() && bytes[index] != b'>'
            {
                index += 1;
            }
            tag[value_start..index].to_owned()
        };
        if attribute_name.eq_ignore_ascii_case(name) {
            return Some(value);
        }
    }
    None
}

fn pow_data_build_from_source(source: &str) -> Option<String> {
    let start = source.find("c/")?;
    let suffix = &source[start + 2..];
    let component_end = suffix.find("/_")?;
    Some(source[start..start + 2 + component_end + 2].to_owned())
}

pub(crate) fn parse_native_pow_resources(body: &[u8]) -> NativePowResources {
    let html = String::from_utf8_lossy(body);
    let lowercase_html = html.to_ascii_lowercase();
    let mut script_sources = Vec::new();
    let mut cursor = 0usize;
    while script_sources.len() < MAX_POW_SCRIPT_SOURCES {
        let Some(relative_start) = lowercase_html[cursor..].find("<script") else {
            break;
        };
        let start = cursor + relative_start;
        let has_script_tag_boundary = lowercase_html
            .as_bytes()
            .get(start + "<script".len())
            .is_none_or(|byte| byte.is_ascii_whitespace() || matches!(*byte, b'/' | b'>'));
        if !has_script_tag_boundary {
            cursor = start + "<script".len();
            continue;
        }
        let Some(relative_end) = html[start..].find('>') else {
            break;
        };
        let end = start + relative_end;
        if let Some(source) = html_attribute(&html[start..=end], "src")
            && !source.is_empty()
            && source.chars().count() <= 4096
        {
            script_sources.push(source);
        }
        cursor = end + 1;
    }
    if script_sources.is_empty() {
        script_sources.push(DEFAULT_POW_SCRIPT.to_owned());
    }
    let data_build = script_sources
        .iter()
        .find_map(|source| pow_data_build_from_source(source))
        .or_else(|| {
            html.find("<html")
                .or_else(|| lowercase_html.find("<html"))
                .and_then(|start| html[start..].find('>').map(|end| &html[start..start + end]))
                .and_then(|tag| html_attribute(tag, "data-build"))
                .filter(|value| !value.is_empty() && value.chars().count() <= 4096)
        })
        .unwrap_or_default();
    NativePowResources {
        script_sources,
        data_build,
    }
}

pub(crate) fn native_message(message: &Value) -> Result<Value, ApiError> {
    let object = message.as_object().ok_or_else(ApiError::invalid_request)?;
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    if matches!(role, "tool" | "developer")
        || object
            .get("tool_calls")
            .is_some_and(|value| !value.is_null())
    {
        // Python routes developer messages and tool history to Codex
        // Responses. The native canary has no Codex translation, so reject
        // them instead of sending a different backend contract to ChatGPT.
        return Err(ApiError::unavailable());
    }
    let content = object
        .get("content")
        .and_then(|value| match value {
            Value::Null if role == "assistant" => Some(vec![String::new()]),
            Value::String(text) => Some(vec![text.clone()]),
            Value::Array(parts) => {
                let mut text_parts = Vec::new();
                for part in parts {
                    let part = part.as_object()?;
                    let kind = part.get("type")?.as_str()?;
                    if !matches!(kind, "text" | "input_text" | "output_text") {
                        return None;
                    }
                    text_parts.push(part.get("text")?.as_str()?.to_owned());
                }
                if text_parts.is_empty() {
                    text_parts.push(String::new());
                }
                Some(text_parts)
            }
            _ => None,
        })
        .ok_or_else(ApiError::unavailable)?;
    Ok(json!({
        "id": native_message_id(),
        "author": {"role": role},
        "content": {"content_type": "text", "parts": content},
    }))
}

pub(crate) fn native_conversation_payload(object: &Map<String, Value>) -> Result<Value, ApiError> {
    if object
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
    {
        return Err(ApiError::unavailable());
    }
    let mut messages = Vec::new();
    if let Some(Value::Array(items)) = object.get("messages")
        && !items.is_empty()
    {
        for item in items {
            messages.push(native_message(item)?);
        }
    } else if let Some(prompt) = object.get("prompt").and_then(Value::as_str) {
        messages.push(json!({
            "id": native_message_id(),
            "author": {"role": "user"},
            "content": {"content_type": "text", "parts": [prompt.trim()]},
        }));
    }
    if messages.is_empty() {
        return Err(ApiError::invalid_request());
    }
    let thinking_effort = object
        .get("reasoning_effort")
        .and_then(Value::as_str)
        .or_else(|| object.get("thinking_effort").and_then(Value::as_str))
        .or_else(|| {
            object
                .get("reasoning")
                .and_then(Value::as_object)
                .and_then(|reasoning| reasoning.get("effort"))
                .and_then(Value::as_str)
        })
        .filter(|effort| !effort.is_empty());
    let mut payload = json!({
        "action": "next",
        "messages": messages,
        "model": object.get("model").cloned().unwrap_or_else(|| json!("auto")),
        "parent_message_id": native_message_id(),
        "conversation_mode": {"kind": "primary_assistant"},
        "conversation_origin": Value::Null,
        "force_paragen": false,
        "force_paragen_model_slug": "",
        "force_rate_limit": false,
        "force_use_sse": true,
        "history_and_training_disabled": true,
        "reset_rate_limits": false,
        "suggestions": [],
        "supported_encodings": [],
        "system_hints": [],
        "timezone": "Asia/Shanghai",
        "timezone_offset_min": -480,
        "variant_purpose": "comparison_implicit",
        "websocket_request_id": native_message_id(),
        "client_contextual_info": {
            "is_dark_mode": false,
            "time_since_loaded": 120,
            "page_height": 900,
            "page_width": 1400,
            "pixel_ratio": 2,
            "screen_height": 1440,
            "screen_width": 2560,
        },
    });
    if let Some(effort) = thinking_effort {
        payload["thinking_effort"] = Value::String(effort.to_owned());
    }
    Ok(payload)
}

pub(crate) fn native_message_text(message: &Value) -> Result<String, ApiError> {
    let object = message.as_object().ok_or_else(ApiError::invalid_request)?;
    let content = object
        .get("content")
        .ok_or_else(ApiError::invalid_request)?;
    let role = object.get("role").and_then(Value::as_str);
    match content {
        Value::Null if role == Some("assistant") => Ok(String::new()),
        Value::String(text) => Ok(text.clone()),
        Value::Array(parts) => {
            let mut text = String::new();
            for part in parts {
                let part = part.as_object().ok_or_else(ApiError::invalid_request)?;
                text.push_str(
                    part.get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(ApiError::invalid_request)?,
                );
            }
            Ok(text)
        }
        _ => Err(ApiError::invalid_request()),
    }
}

pub(crate) fn native_token_count(
    bpe: &tiktoken_rs::CoreBPE,
    text: &str,
) -> Result<usize, ApiError> {
    bpe.count(text, &std::collections::HashSet::new())
        .map_err(|_| ApiError::upstream())
}

pub(crate) fn native_usage(object: &Map<String, Value>, output: &str) -> Result<Value, ApiError> {
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("auto");
    let bpe =
        tiktoken_rs::bpe_for_model(model).unwrap_or_else(|_| tiktoken_rs::o200k_base_singleton());
    let mut prompt_tokens = 0usize;
    if let Some(Value::Array(messages)) = object.get("messages")
        && !messages.is_empty()
    {
        for message in messages {
            let object = message.as_object().ok_or_else(ApiError::invalid_request)?;
            prompt_tokens = prompt_tokens
                .checked_add(3)
                .ok_or_else(ApiError::upstream)?;
            if let Some(role) = object.get("role").and_then(Value::as_str) {
                prompt_tokens = prompt_tokens
                    .checked_add(native_token_count(bpe, role)?)
                    .ok_or_else(ApiError::upstream)?;
            }
            prompt_tokens = prompt_tokens
                .checked_add(native_token_count(bpe, &native_message_text(message)?)?)
                .ok_or_else(ApiError::upstream)?;
        }
    } else if let Some(prompt) = object.get("prompt").and_then(Value::as_str) {
        prompt_tokens = 3usize
            .checked_add(native_token_count(bpe, "user")?)
            .and_then(|value| value.checked_add(native_token_count(bpe, prompt.trim()).ok()?))
            .ok_or_else(ApiError::upstream)?;
    } else {
        return Err(ApiError::invalid_request());
    }
    prompt_tokens = prompt_tokens
        .checked_add(3)
        .ok_or_else(ApiError::upstream)?;
    native_usage_for_prompt_tokens(model, prompt_tokens, output)
}

pub(crate) fn native_usage_for_prompt_tokens(
    model: &str,
    prompt_tokens: usize,
    output: &str,
) -> Result<Value, ApiError> {
    let bpe =
        tiktoken_rs::bpe_for_model(model).unwrap_or_else(|_| tiktoken_rs::o200k_base_singleton());
    let completion_tokens = native_token_count(bpe, output)?;
    let total_tokens = prompt_tokens
        .checked_add(completion_tokens)
        .ok_or_else(ApiError::upstream)?;
    Ok(json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "prompt_tokens_details": {
            "text_tokens": prompt_tokens,
            "image_tokens": 0,
            "cached_tokens": 0,
        },
        "completion_tokens_details": {
            "text_tokens": completion_tokens,
            "image_tokens": 0,
            "reasoning_tokens": 0,
        },
    }))
}

const MAX_NATIVE_PATCH_DEPTH: usize = 32;

fn native_text_candidate(value: &Value) -> Result<Option<String>, io::Error> {
    let message = value
        .get("message")
        .or_else(|| value.get("v").and_then(|value| value.get("message")));
    let Some(message) = message else {
        return Ok(None);
    };
    let Some(message) = message.as_object() else {
        return Err(io::Error::other("malformed upstream event"));
    };
    let role = message
        .get("author")
        .and_then(Value::as_object)
        .and_then(|author| author.get("role"))
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::other("malformed upstream event"))?;
    let metadata = match message.get("metadata") {
        None | Some(Value::Null) => None,
        Some(Value::Object(metadata)) => Some(metadata),
        _ => return Err(io::Error::other("malformed upstream event")),
    };
    let hidden = metadata.and_then(|metadata| metadata.get("is_visually_hidden_from_conversation"));
    if hidden.is_some_and(|value| !value.is_boolean()) {
        return Err(io::Error::other("malformed upstream event"));
    }
    let optional_text = |key: &str| -> Result<Option<&str>, io::Error> {
        match message.get(key) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.as_str())),
            _ => Err(io::Error::other("malformed upstream event")),
        }
    };
    let recipient = optional_text("recipient")?;
    let channel = optional_text("channel")?;
    let visible = role.trim().eq_ignore_ascii_case("assistant")
        && hidden.is_none_or(|hidden| hidden.as_bool() != Some(true))
        && recipient
            .is_none_or(|recipient| recipient.trim().is_empty() || recipient.trim() == "all")
        && channel.is_none_or(|channel| channel.trim().is_empty() || channel.trim() == "final");
    if !visible {
        return Ok(None);
    }
    let content = match message.get("content") {
        None | Some(Value::Null) => return Ok(None),
        Some(Value::Object(content)) => content,
        Some(_) => return Err(io::Error::other("malformed upstream event")),
    };
    if let Some(parts) = content.get("parts") {
        let parts = parts
            .as_array()
            .ok_or_else(|| io::Error::other("malformed upstream event"))?;
        let mut text = String::new();
        for part in parts {
            text.push_str(
                part.as_str()
                    .ok_or_else(|| io::Error::other("malformed upstream event"))?,
            );
        }
        if !text.is_empty() {
            return Ok(Some(text));
        }
    }
    if let Some(text) = content.get("text") {
        let text = text
            .as_str()
            .ok_or_else(|| io::Error::other("malformed upstream event"))?;
        return Ok((!text.is_empty()).then(|| text.to_owned()));
    }
    Err(io::Error::other("malformed upstream event"))
}

fn native_patch_candidate(
    value: &Value,
    current_text: &str,
    depth: usize,
) -> Result<Option<String>, io::Error> {
    if depth > MAX_NATIVE_PATCH_DEPTH {
        return Err(io::Error::other("malformed upstream event"));
    }
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    let path = object.get("p").and_then(Value::as_str);
    let operation = object.get("o").and_then(Value::as_str);
    let raw_value = object.get("v");

    if path == Some("/message/content/parts/0") {
        let text = raw_value
            .and_then(Value::as_str)
            .ok_or_else(|| io::Error::other("malformed upstream event"))?;
        return match operation {
            Some("append") => Ok(Some(format!("{current_text}{text}"))),
            Some("replace") => Err(io::Error::other("unsupported upstream text replacement")),
            Some(_) | None => Ok(None),
        };
    }

    if path.is_none() && operation.is_none() {
        if current_text.is_empty() {
            return Ok(None);
        }
        if let Some(text) = raw_value.and_then(Value::as_str) {
            return Ok(Some(format!("{current_text}{text}")));
        }
    }

    let Some(items) = raw_value.and_then(Value::as_array) else {
        return Ok(None);
    };
    let mut next_text = current_text.to_owned();
    for item in items {
        if let Some(candidate) = native_patch_candidate(item, &next_text, depth + 1)? {
            next_text = candidate;
        }
    }
    if next_text == current_text {
        Ok(None)
    } else {
        Ok(Some(next_text))
    }
}

fn native_is_internal_annotation_part(part: &str) -> bool {
    let value = part.trim();
    if value.is_empty() {
        return true;
    }
    let lower = value.to_ascii_lowercase();
    if lower.starts_with("source") {
        return true;
    }
    lower.starts_with("turn")
}

fn native_annotation_text(payload: &str) -> String {
    let mut parts = payload.split('\u{e202}').map(str::trim);
    let kind = parts.next().unwrap_or_default().to_ascii_lowercase();
    let data = parts.collect::<Vec<_>>();
    if kind == "url" {
        let label = data.first().copied().unwrap_or_default();
        let url = data.get(1).copied().unwrap_or_default();
        if !label.is_empty() && (url.starts_with("http://") || url.starts_with("https://")) {
            return format!("{label} ({url})");
        }
        return if !label.is_empty() {
            label.to_owned()
        } else {
            url.to_owned()
        };
    }
    data.into_iter()
        .find(|part| !native_is_internal_annotation_part(part))
        .unwrap_or_default()
        .to_owned()
}

pub(crate) fn native_sanitize_text(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut remainder = text;
    loop {
        let Some(start) = remainder.find('\u{e200}') else {
            output.push_str(remainder);
            break;
        };
        output.push_str(&remainder[..start]);
        let after_start = &remainder[start + '\u{e200}'.len_utf8()..];
        let Some(end) = after_start.find('\u{e201}') else {
            break;
        };
        let payload = &after_start[..end];
        let replacement = native_annotation_text(payload);
        let after_end = &after_start[end + '\u{e201}'.len_utf8()..];
        if replacement.is_empty()
            && after_end
                .chars()
                .next()
                .is_some_and(|character| ".,;:!?".contains(character))
        {
            while output
                .chars()
                .last()
                .is_some_and(|character| matches!(character, ' ' | '\t'))
            {
                output.pop();
            }
        }
        output.push_str(&replacement);
        remainder = after_end;
    }
    output
}

pub(crate) fn native_frame(
    payload: &[u8],
    current_text: &mut String,
    completion_id: &str,
    model: &str,
    created: i64,
    include_usage: bool,
) -> Result<Option<Vec<u8>>, io::Error> {
    let text =
        std::str::from_utf8(payload).map_err(|_| io::Error::other("malformed upstream event"))?;
    let data = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>()
        .join("\n");
    if data.is_empty() {
        return Ok(None);
    }
    if data == "[DONE]" {
        return Ok(Some(b"data: [DONE]\n\n".to_vec()));
    }
    let value: Value =
        serde_json::from_str(&data).map_err(|_| io::Error::other("malformed upstream event"))?;
    let candidate = native_text_candidate(&value)?;
    let candidate = candidate.or(native_patch_candidate(&value, current_text, 0)?);
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let current_visible = native_sanitize_text(current_text);
    let candidate_visible = native_sanitize_text(&candidate);
    let delta = if candidate_visible.starts_with(current_visible.as_str()) {
        candidate_visible[current_visible.len()..].to_owned()
    } else if current_visible.is_empty() {
        candidate_visible.clone()
    } else {
        return Err(io::Error::other("malformed upstream event"));
    };
    *current_text = candidate;
    if delta.is_empty() {
        return Ok(None);
    }
    let mut frame = json!({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {"content": delta}, "finish_reason": null}],
    });
    if include_usage {
        frame["usage"] = Value::Null;
    }
    let mut output = serde_json::to_vec(&frame).map_err(|_| io::Error::other("upstream error"))?;
    output.extend_from_slice(b"\n\n");
    let mut framed = b"data: ".to_vec();
    framed.extend(output);
    Ok(Some(framed))
}

pub(crate) fn native_finish_frame(
    completion_id: &str,
    model: &str,
    created: i64,
    include_usage: bool,
) -> Vec<u8> {
    let mut frame = json!({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
    });
    if include_usage {
        frame["usage"] = Value::Null;
    }
    let mut framed = b"data: ".to_vec();
    framed.extend(serde_json::to_vec(&frame).expect("static completion frame"));
    framed.extend_from_slice(b"\n\n");
    framed
}

pub(crate) fn native_role_frame(
    completion_id: &str,
    model: &str,
    created: i64,
    include_usage: bool,
) -> Vec<u8> {
    let mut frame = json!({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{
            "index": 0,
            "delta": {"role": "assistant", "content": ""},
            "finish_reason": null,
        }],
    });
    if include_usage {
        frame["usage"] = Value::Null;
    }
    let mut framed = b"data: ".to_vec();
    framed.extend(serde_json::to_vec(&frame).expect("static role frame"));
    framed.extend_from_slice(b"\n\n");
    framed
}

pub(crate) fn native_usage_frame(
    completion_id: &str,
    model: &str,
    created: i64,
    usage: Value,
) -> Vec<u8> {
    let frame = json!({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [],
        "usage": usage,
    });
    let mut framed = b"data: ".to_vec();
    framed.extend(serde_json::to_vec(&frame).expect("static usage frame"));
    framed.extend_from_slice(b"\n\n");
    framed
}

pub(crate) fn native_completion_text(body: &[u8]) -> Result<String, ApiError> {
    let mut text = String::new();
    let mut terminated = false;
    let mut buffer = body.to_vec();
    while let Some((position, delimiter_length)) = sse_delimiter(&buffer) {
        let event = buffer.drain(..position).collect::<Vec<_>>();
        buffer.drain(..delimiter_length);
        if let Some(frame) =
            native_frame(&event, &mut text, "chatcmpl-rust-canary", "auto", 0, false)
                .map_err(|_| ApiError::upstream())?
            && frame == b"data: [DONE]\n\n"
        {
            terminated = true;
            break;
        }
    }
    if !terminated {
        return Err(ApiError::upstream());
    }
    Ok(native_sanitize_text(&text))
}

fn valid_chat_content(value: &Value, role: &str) -> bool {
    match value {
        Value::String(_) => true,
        Value::Array(parts) => parts.iter().all(|part| valid_chat_content_part(part, role)),
        _ => false,
    }
}

fn valid_chat_content_part(part: &Value, role: &str) -> bool {
    let Some(object) = part.as_object() else {
        return false;
    };
    let Some(kind) = object.get("type").and_then(Value::as_str) else {
        return false;
    };
    let optional_breakpoint_is_null = object
        .get("prompt_cache_breakpoint")
        .is_none_or(Value::is_null);
    match kind {
        "text" | "input_text" | "output_text" => {
            optional_breakpoint_is_null
                && object
                    .keys()
                    .all(|key| key == "type" || key == "text" || key == "prompt_cache_breakpoint")
                && object.get("text").is_some_and(Value::is_string)
                && object.get("type").is_some()
        }
        "image_url" | "input_image" if role == "user" => {
            let Some(image_url) = object.get("image_url") else {
                return false;
            };
            let valid_url = image_url
                .as_str()
                .is_some_and(|value| !value.trim().is_empty())
                || image_url.as_object().is_some_and(|image| {
                    image.keys().all(|key| key == "url" || key == "detail")
                        && image
                            .get("url")
                            .and_then(Value::as_str)
                            .is_some_and(|value| !value.trim().is_empty())
                        && image.get("detail").is_none_or(|detail| {
                            detail
                                .as_str()
                                .is_some_and(|value| matches!(value, "auto" | "low" | "high"))
                        })
                });
            valid_url
                && optional_breakpoint_is_null
                && object.keys().all(|key| {
                    key == "type" || key == "image_url" || key == "prompt_cache_breakpoint"
                })
        }
        "input_audio" if role == "user" => {
            let Some(audio) = object.get("input_audio").and_then(Value::as_object) else {
                return false;
            };
            let Some(data) = audio.get("data").and_then(Value::as_str) else {
                return false;
            };
            let Some(format) = audio.get("format").and_then(Value::as_str) else {
                return false;
            };
            audio.len() == 2
                && optional_breakpoint_is_null
                && object.keys().all(|key| {
                    key == "type" || key == "input_audio" || key == "prompt_cache_breakpoint"
                })
                && !data.is_empty()
                && matches!(format, "wav" | "mp3")
                && base64::engine::general_purpose::STANDARD
                    .decode(data)
                    .is_ok()
        }
        _ => false,
    }
}

fn valid_tool_calls(value: &Value) -> bool {
    let Some(calls) = value.as_array() else {
        return false;
    };
    calls.iter().all(|call| {
        let Some(object) = call.as_object() else {
            return false;
        };
        let Some(function) = object.get("function").and_then(Value::as_object) else {
            return false;
        };
        object.len() == 3
            && object
                .get("id")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
            && object.get("type").and_then(Value::as_str) == Some("function")
            && function.len() == 2
            && function
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|value| !value.trim().is_empty())
            && function.get("arguments").is_some_and(Value::is_string)
    })
}

fn valid_chat_tools(value: &Value) -> bool {
    let Some(tools) = value.as_array() else {
        return false;
    };
    tools.iter().all(|tool| {
        let Some(tool) = tool.as_object() else {
            return false;
        };
        let Some(tool_type) = tool.get("type").and_then(Value::as_str) else {
            return false;
        };
        if tool_type.trim().is_empty() {
            return false;
        }
        if tool_type != "function" {
            // The Python Chat contract only has a validated function-tool
            // path here.  Unknown tool types must not be forwarded to an
            // upstream boundary that the canary does not implement.
            return false;
        }
        if tool.keys().any(|key| key != "type" && key != "function") {
            return false;
        }
        let Some(function) = tool.get("function").and_then(Value::as_object) else {
            return false;
        };
        if function.keys().any(|key| {
            !matches!(
                key.as_str(),
                "name" | "description" | "parameters" | "strict"
            )
        }) {
            return false;
        }
        if function
            .get("name")
            .and_then(Value::as_str)
            .is_none_or(|name| name.trim().is_empty())
        {
            return false;
        }
        if function
            .get("description")
            .is_some_and(|description| !description.is_null() && !description.is_string())
        {
            return false;
        }
        if function
            .get("parameters")
            .is_some_and(|parameters| !parameters.is_null() && !parameters.is_object())
        {
            return false;
        }
        !function
            .get("strict")
            .is_some_and(|strict| !strict.is_null() && !strict.is_boolean())
    })
}

fn has_function_tool(value: Option<&Value>) -> bool {
    value.and_then(Value::as_array).is_some_and(|tools| {
        tools.iter().any(|tool| {
            tool.as_object()
                .and_then(|tool| tool.get("type"))
                .and_then(Value::as_str)
                == Some("function")
        })
    })
}

fn has_native_chat_feature(value: Option<&Value>) -> bool {
    value.and_then(Value::as_array).is_some_and(|messages| {
        messages.iter().any(|message| {
            let Some(message) = message.as_object() else {
                return false;
            };
            if matches!(
                message.get("role").and_then(Value::as_str),
                Some("tool" | "developer")
            ) {
                return true;
            }
            if message
                .get("tool_calls")
                .is_some_and(|tool_calls| !tool_calls.is_null())
            {
                return true;
            }
            message
                .get("content")
                .and_then(Value::as_array)
                .is_some_and(|parts| {
                    parts.iter().any(|part| {
                        part.as_object()
                            .and_then(|part| part.get("type"))
                            .and_then(Value::as_str)
                            == Some("input_audio")
                    })
                })
        })
    })
}

fn validate_chat_message(message: &Value) -> bool {
    let Some(object) = message.as_object() else {
        return false;
    };
    let Some(role) = object.get("role").and_then(Value::as_str) else {
        return false;
    };
    let allowed = match role {
        "system" | "developer" | "user" => ["role", "content"].as_slice(),
        "assistant" => ["role", "content", "tool_calls"].as_slice(),
        "tool" => ["role", "content", "tool_call_id"].as_slice(),
        _ => return false,
    };
    if object
        .iter()
        .any(|(key, value)| !value.is_null() && !allowed.contains(&key.as_str()))
    {
        return false;
    }
    if role == "tool"
        && object
            .get("tool_call_id")
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
    {
        return false;
    }
    if let Some(tool_calls) = object.get("tool_calls")
        && !tool_calls.is_null()
        && (role != "assistant" || !valid_tool_calls(tool_calls))
    {
        return false;
    }
    match object.get("content") {
        Some(Value::Null) if role == "assistant" => true,
        Some(content) => valid_chat_content(content, role),
        None => role == "assistant" && object.contains_key("tool_calls"),
    }
}

fn validate_chat_options(object: &Map<String, Value>) -> Result<(), ApiError> {
    const ALLOWED_FIELDS: &[&str] = &[
        "messages",
        "modalities",
        "model",
        "n",
        "parallel_tool_calls",
        "prompt",
        "reasoning",
        "reasoning_effort",
        "store",
        "stream",
        "stream_options",
        "thinking_effort",
        "tool_choice",
        "tools",
    ];
    if object
        .iter()
        .any(|(key, value)| !value.is_null() && !ALLOWED_FIELDS.contains(&key.as_str()))
    {
        return Err(ApiError::invalid_request());
    }
    if let Some(value) = object.get("n")
        && !value.is_null()
    {
        if value.as_i64().is_none() {
            return Err(ApiError::validation());
        }
        if value.as_i64() != Some(1) {
            return Err(ApiError::invalid_request());
        }
    }
    if let Some(value) = object.get("modalities")
        && !value.is_null()
    {
        let Value::Array(items) = value else {
            return Err(ApiError::validation());
        };
        if items.iter().any(|item| !item.is_string()) {
            return Err(ApiError::validation());
        }
        if !(items.len() == 1 && items[0].as_str() == Some("text")) {
            return Err(ApiError::invalid_request());
        }
    }
    if let Some(value) = object.get("store")
        && !value.is_null()
        && value.as_bool() != Some(false)
    {
        return Err(ApiError::invalid_request());
    }
    if let Some(value) = object.get("parallel_tool_calls")
        && !value.is_null()
        && !value.is_boolean()
    {
        return Err(ApiError::invalid_request());
    }
    let contains_function_tool = has_function_tool(object.get("tools"));
    let has_native_feature = has_native_chat_feature(object.get("messages"));
    if let Some(value) = object.get("tool_choice")
        && !value.is_null()
        && (((contains_function_tool || has_native_feature) && value.as_str() != Some("auto"))
            || (!contains_function_tool && !has_native_feature && value.as_str() != Some("none")))
    {
        return Err(ApiError::invalid_request());
    }
    if let Some(value) = object.get("tools")
        && !value.is_null()
    {
        let Value::Array(_) = value else {
            return Err(ApiError::invalid_request());
        };
        if !valid_chat_tools(value) {
            return Err(ApiError::invalid_request());
        }
    }
    let mut effort_sources = 0usize;
    if let Some(value) = object.get("reasoning")
        && !value.is_null()
    {
        let Value::Object(reasoning) = value else {
            return Err(ApiError::invalid_request());
        };
        if reasoning.keys().any(|key| key != "effort") {
            return Err(ApiError::invalid_request());
        }
        if let Some(effort) = reasoning.get("effort")
            && !effort.is_null()
        {
            if !effort.is_string() {
                return Err(ApiError::invalid_request());
            }
            effort_sources += 1;
        }
    }
    for key in ["reasoning_effort", "thinking_effort"] {
        if let Some(value) = object.get(key)
            && !value.is_null()
        {
            if !value.is_string() {
                return Err(ApiError::invalid_request());
            }
            effort_sources += 1;
        }
    }
    if effort_sources > 1 {
        return Err(ApiError::invalid_request());
    }
    if let Some(value) = object.get("stream_options")
        && !value.is_null()
    {
        let Value::Object(options) = value else {
            return Err(ApiError::invalid_request());
        };
        if object.get("stream").and_then(Value::as_bool) != Some(true)
            || options
                .keys()
                .any(|key| !matches!(key.as_str(), "include_usage" | "include_obfuscation"))
            || options.values().any(|option| !option.is_boolean())
            || options.get("include_obfuscation").and_then(Value::as_bool) == Some(true)
        {
            return Err(ApiError::invalid_request());
        }
    }
    Ok(())
}

fn canonical_chat_effort(value: &str) -> String {
    match value.trim().to_lowercase().as_str() {
        "none" | "auto" => "auto".to_owned(),
        "low" => "low".to_owned(),
        "medium" => "medium".to_owned(),
        "high" => "high".to_owned(),
        "xhigh" => "xhigh".to_owned(),
        "extended" => "extended".to_owned(),
        "standard" => "standard".to_owned(),
        "max" => "max".to_owned(),
        normalized => normalized.to_owned(),
    }
}

pub(crate) fn normalize_chat_effort(object: &mut Map<String, Value>) {
    if let Some(Value::Object(reasoning)) = object.get_mut("reasoning")
        && let Some(Value::String(effort)) = reasoning.get_mut("effort")
    {
        *effort = canonical_chat_effort(effort);
    }
    for key in ["reasoning_effort", "thinking_effort"] {
        if let Some(Value::String(effort)) = object.get_mut(key) {
            *effort = canonical_chat_effort(effort);
        }
    }
}

pub(crate) fn validate_chat_payload(payload: Value) -> Result<Map<String, Value>, ApiError> {
    let Value::Object(mut object) = payload else {
        return Err(ApiError::validation());
    };
    if let Some(model) = object.get("model")
        && !model.is_null()
        && !model.is_string()
    {
        return Err(ApiError::validation());
    }
    if let Some(prompt) = object.get("prompt")
        && !prompt.is_null()
        && !prompt.is_string()
    {
        return Err(ApiError::validation());
    }
    if let Some(stream) = object.get("stream")
        && !stream.is_null()
        && !stream.is_boolean()
    {
        return Err(ApiError::validation());
    }
    let has_messages = if let Some(messages) = object.get("messages") {
        if messages.is_null() {
            false
        } else {
            let Value::Array(messages) = messages else {
                return Err(ApiError::validation());
            };
            for message in messages {
                if !message.is_object() {
                    return Err(ApiError::validation());
                }
                if !validate_chat_message(message) {
                    return Err(ApiError::invalid_request());
                }
            }
            !messages.is_empty()
        }
    } else {
        false
    };
    let has_prompt = object
        .get("prompt")
        .filter(|prompt| !prompt.is_null())
        .and_then(Value::as_str)
        .is_some_and(|prompt| !prompt.trim().is_empty());
    if !has_messages && !has_prompt {
        return Err(ApiError::invalid_request());
    }
    validate_chat_options(&object)?;
    normalize_chat_effort(&mut object);
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
        .to_owned();
    object.insert("model".to_owned(), Value::String(model));
    Ok(object)
}
