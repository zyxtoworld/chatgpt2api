use axum::{
    body::{Body, Bytes},
    http::{HeaderValue, header},
    response::Response,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{Stream, StreamExt, stream};
use serde_json::{Map, Value, json};
use std::{
    collections::{BTreeMap, HashMap},
    io,
    pin::Pin,
    time::Instant,
};

use super::{ApiError, native_message_id, sse_delimiter};

const SEARCH_OPAQUE_PREFIX: &str = "chatgpt2api-search-v1:";

fn encode_search_opaque(kind: &str, value: Value) -> String {
    let raw = serde_json::to_vec(&json!({"kind": kind, "value": value}))
        .expect("search continuation is JSON");
    format!("{SEARCH_OPAQUE_PREFIX}{}", URL_SAFE_NO_PAD.encode(raw))
}

fn decode_search_opaque(value: &str, expected_kind: &str) -> Result<Value, ApiError> {
    let encoded = value
        .strip_prefix(SEARCH_OPAQUE_PREFIX)
        .ok_or_else(ApiError::invalid_request)?;
    let raw = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| ApiError::invalid_request())?;
    let envelope: Value = serde_json::from_slice(&raw).map_err(|_| ApiError::invalid_request())?;
    if envelope.get("kind").and_then(Value::as_str) != Some(expected_kind) {
        return Err(ApiError::invalid_request());
    }
    envelope
        .get("value")
        .cloned()
        .ok_or_else(ApiError::invalid_request)
}

fn search_result_blocks_from_annotations(
    annotations: &[&Value],
    text: &str,
) -> Result<Vec<Value>, ApiError> {
    let chars = text.chars().collect::<Vec<_>>();
    let mut blocks = Vec::new();
    for annotation in annotations {
        let annotation = annotation.as_object().ok_or_else(ApiError::upstream)?;
        if annotation.get("type").and_then(Value::as_str) != Some("url_citation") {
            return Err(ApiError::upstream());
        }
        let url = annotation
            .get("url")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(ApiError::upstream)?;
        let title = annotation
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or(url);
        let start = annotation
            .get("start_index")
            .and_then(Value::as_u64)
            .ok_or_else(ApiError::upstream)? as usize;
        let end = annotation
            .get("end_index")
            .and_then(Value::as_u64)
            .ok_or_else(ApiError::upstream)? as usize;
        if start > end || end > chars.len() {
            return Err(ApiError::upstream());
        }
        let cited_text = chars[start..end].iter().collect::<String>();
        let source = json!({
            "url": url,
            "title": title,
            "snippet": cited_text,
        });
        blocks.push(json!({
            "type": "web_search_result",
            "url": url,
            "title": title,
            "encrypted_content": encode_search_opaque("search-result", source),
        }));
    }
    Ok(blocks)
}

fn citations_from_annotations(annotations: &[&Value], text: &str) -> Result<Vec<Value>, ApiError> {
    let chars = text.chars().collect::<Vec<_>>();
    let mut citations = Vec::new();
    for annotation in annotations {
        let annotation = annotation.as_object().ok_or_else(ApiError::upstream)?;
        if annotation.get("type").and_then(Value::as_str) != Some("url_citation") {
            return Err(ApiError::upstream());
        }
        let url = annotation
            .get("url")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .ok_or_else(ApiError::upstream)?;
        let title = annotation
            .get("title")
            .and_then(Value::as_str)
            .unwrap_or(url);
        let start = annotation
            .get("start_index")
            .and_then(Value::as_u64)
            .ok_or_else(ApiError::upstream)? as usize;
        let end = annotation
            .get("end_index")
            .and_then(Value::as_u64)
            .ok_or_else(ApiError::upstream)? as usize;
        if start > end || end > chars.len() {
            return Err(ApiError::upstream());
        }
        let cited_text = chars[start..end].iter().collect::<String>();
        citations.push(json!({
            "type": "web_search_result_location",
            "url": url,
            "title": title,
            "cited_text": cited_text,
            "encrypted_index": encode_search_opaque("search-index", json!({"url": url})),
        }));
    }
    Ok(citations)
}

pub(super) fn validate_message_request(payload: Value) -> Result<Map<String, Value>, ApiError> {
    let Value::Object(mut object) = payload else {
        return Err(ApiError::validation());
    };
    const UNSUPPORTED_FIELDS: &[&str] = &[
        "temperature",
        "top_p",
        "top_k",
        "stop_sequences",
        "metadata",
        "thinking",
        "service_tier",
        "cache_control",
    ];
    if object
        .keys()
        .any(|key| UNSUPPORTED_FIELDS.contains(&key.as_str()))
    {
        return Err(ApiError::unsupported_capability());
    }
    if object.keys().any(|key| {
        !matches!(
            key.as_str(),
            "model" | "messages" | "system" | "max_tokens" | "stream" | "tools" | "tool_choice"
        )
    }) {
        return Err(ApiError::invalid_request());
    }
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(ApiError::invalid_request)?
        .to_owned();
    let max_tokens = object
        .get("max_tokens")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0 && *value <= 1_000_000)
        .ok_or_else(ApiError::invalid_request)?;
    let messages = object
        .get("messages")
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty())
        .ok_or_else(ApiError::invalid_request)?;
    for message in messages {
        validate_message(message)?;
    }
    if let Some(system) = object.get("system")
        && !system.is_string()
        && !(system.is_array()
            && system
                .as_array()
                .is_some_and(|items| items.iter().all(valid_text_block)))
    {
        return Err(ApiError::invalid_request());
    }
    if let Some(tools) = object.get("tools") {
        let tools = tools.as_array().ok_or_else(ApiError::invalid_request)?;
        for tool in tools {
            let tool = tool.as_object().ok_or_else(ApiError::invalid_request)?;
            match tool.get("type").and_then(Value::as_str) {
                Some("web_search_20250305") => {
                    if tool.keys().any(|key| {
                        !matches!(
                            key.as_str(),
                            "type"
                                | "name"
                                | "max_uses"
                                | "allowed_domains"
                                | "blocked_domains"
                                | "user_location"
                        )
                    }) || tool.get("name").and_then(Value::as_str) != Some("web_search")
                    {
                        return Err(ApiError::invalid_request());
                    }
                    if tool.get("max_uses").is_some_and(|value| {
                        value.as_u64().is_none_or(|uses| uses == 0 || uses > 10)
                    }) {
                        return Err(ApiError::invalid_request());
                    }
                    if tool.get("max_uses").is_some() {
                        return Err(ApiError::unsupported_capability());
                    }
                    if tool.get("allowed_domains").is_some()
                        || tool.get("blocked_domains").is_some()
                        || tool.get("user_location").is_some()
                    {
                        return Err(ApiError::unavailable());
                    }
                }
                None | Some("function") => {
                    if tool
                        .keys()
                        .any(|key| !matches!(key.as_str(), "name" | "description" | "input_schema"))
                        || tool.get("name").and_then(Value::as_str).is_none()
                        || tool
                            .get("input_schema")
                            .and_then(Value::as_object)
                            .is_none()
                    {
                        return Err(ApiError::invalid_request());
                    }
                }
                _ => return Err(ApiError::invalid_request()),
            }
        }
    }
    if let Some(choice) = object.get("tool_choice") {
        validate_tool_choice(choice)?;
    }
    if object
        .get("stream")
        .is_some_and(|value| !value.is_boolean() && !value.is_null())
    {
        return Err(ApiError::validation());
    }
    object.insert("model".to_owned(), Value::String(model));
    object.insert("max_tokens".to_owned(), Value::Number(max_tokens.into()));
    Ok(object)
}

fn valid_text_block(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.keys().all(|key| key == "type" || key == "text")
        && object.get("type").and_then(Value::as_str) == Some("text")
        && object.get("text").and_then(Value::as_str).is_some()
}

fn validate_message(value: &Value) -> Result<(), ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "role" | "content"))
    {
        return Err(ApiError::invalid_request());
    }
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    if !matches!(role, "user" | "assistant") {
        return Err(ApiError::invalid_request());
    }
    let content = object
        .get("content")
        .ok_or_else(ApiError::invalid_request)?;
    match content {
        Value::String(_) => Ok(()),
        Value::Array(items) => {
            for item in items {
                let item = item.as_object().ok_or_else(ApiError::invalid_request)?;
                let kind = item
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::invalid_request)?;
                match kind {
                    "text" => {
                        if item
                            .keys()
                            .any(|key| !matches!(key.as_str(), "type" | "text" | "citations"))
                        {
                            return Err(ApiError::invalid_request());
                        }
                        if item.get("text").and_then(Value::as_str).is_none() {
                            return Err(ApiError::invalid_request());
                        }
                        if let Some(citations) = item.get("citations") {
                            for citation in
                                citations.as_array().ok_or_else(ApiError::invalid_request)?
                            {
                                let citation =
                                    citation.as_object().ok_or_else(ApiError::invalid_request)?;
                                if citation.keys().any(|key| {
                                    !matches!(
                                        key.as_str(),
                                        "type" | "url" | "title" | "cited_text" | "encrypted_index"
                                    )
                                }) || citation.get("type").and_then(Value::as_str)
                                    != Some("web_search_result_location")
                                    || citation.get("url").and_then(Value::as_str).is_none()
                                    || citation.get("title").and_then(Value::as_str).is_none()
                                    || citation.get("cited_text").and_then(Value::as_str).is_none()
                                {
                                    return Err(ApiError::invalid_request());
                                }
                                if let Some(index) = citation.get("encrypted_index") {
                                    let index =
                                        index.as_str().ok_or_else(ApiError::invalid_request)?;
                                    decode_search_opaque(index, "search-index")?;
                                }
                            }
                        }
                    }
                    "tool_use" => {
                        if item
                            .keys()
                            .any(|key| !matches!(key.as_str(), "type" | "id" | "name" | "input"))
                            || role != "assistant"
                            || item.get("id").and_then(Value::as_str).is_none()
                            || item.get("name").and_then(Value::as_str).is_none()
                            || item.get("input").and_then(Value::as_object).is_none()
                        {
                            return Err(ApiError::invalid_request());
                        }
                    }
                    "tool_result" => {
                        if item.keys().any(|key| {
                            !matches!(
                                key.as_str(),
                                "type" | "tool_use_id" | "content" | "is_error"
                            )
                        }) || item
                            .get("is_error")
                            .is_some_and(|value| !value.is_boolean())
                            || role != "user"
                            || item.get("tool_use_id").and_then(Value::as_str).is_none()
                        {
                            return Err(ApiError::invalid_request());
                        }
                    }
                    "server_tool_use" => {
                        if role != "assistant"
                            || item.keys().any(|key| {
                                !matches!(key.as_str(), "type" | "id" | "name" | "input")
                            })
                            || item.get("id").and_then(Value::as_str).is_none()
                            || item.get("name").and_then(Value::as_str) != Some("web_search")
                            || item.get("input").and_then(Value::as_object).is_none()
                        {
                            return Err(ApiError::invalid_request());
                        }
                    }
                    "web_search_tool_result" => {
                        if role != "assistant"
                            || item.keys().any(|key| {
                                !matches!(key.as_str(), "type" | "tool_use_id" | "content")
                            })
                            || item.get("tool_use_id").and_then(Value::as_str).is_none()
                            || item.get("content").and_then(Value::as_array).is_none()
                        {
                            return Err(ApiError::invalid_request());
                        }
                        for result in item
                            .get("content")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                        {
                            let result =
                                result.as_object().ok_or_else(ApiError::invalid_request)?;
                            if result.keys().any(|key| {
                                !matches!(
                                    key.as_str(),
                                    "type" | "url" | "title" | "encrypted_content" | "page_age"
                                )
                            }) || result.get("type").and_then(Value::as_str)
                                != Some("web_search_result")
                            {
                                return Err(ApiError::invalid_request());
                            }
                        }
                    }
                    "image" => {
                        if role != "user"
                            || item
                                .keys()
                                .any(|key| !matches!(key.as_str(), "type" | "source"))
                        {
                            return Err(ApiError::invalid_request());
                        }
                        let source = item
                            .get("source")
                            .and_then(Value::as_object)
                            .ok_or_else(ApiError::invalid_request)?;
                        if source
                            .keys()
                            .any(|key| !matches!(key.as_str(), "type" | "media_type" | "data"))
                            || source.get("type").and_then(Value::as_str) != Some("base64")
                            || source.get("media_type").and_then(Value::as_str).is_none()
                            || source.get("data").and_then(Value::as_str).is_none()
                        {
                            return Err(ApiError::invalid_request());
                        }
                    }
                    "image_url" | "input_image" => {
                        if role != "user" {
                            return Err(ApiError::invalid_request());
                        }
                        if item.keys().any(|key| {
                            !matches!(
                                key.as_str(),
                                "type" | "image_url" | "url" | "source" | "b64_json" | "base64"
                            )
                        }) {
                            return Err(ApiError::invalid_request());
                        }
                        let has_reference = item
                            .get("url")
                            .and_then(Value::as_str)
                            .is_some_and(|value| !value.trim().is_empty())
                            || item
                                .get("image_url")
                                .and_then(Value::as_str)
                                .is_some_and(|value| !value.trim().is_empty())
                            || item.get("source").is_some()
                            || item.get("b64_json").is_some()
                            || item.get("base64").is_some();
                        if !has_reference {
                            return Err(ApiError::invalid_request());
                        }
                    }
                    _ => return Err(ApiError::invalid_request()),
                }
            }
            Ok(())
        }
        _ => Err(ApiError::invalid_request()),
    }
}

fn validate_tool_choice(value: &Value) -> Result<(), ApiError> {
    match value {
        Value::Object(object) => {
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "type" | "name" | "disable_parallel_tool_use"))
                || object
                    .get("disable_parallel_tool_use")
                    .is_some_and(|value| !value.is_boolean())
            {
                return Err(ApiError::invalid_request());
            }
            match object.get("type").and_then(Value::as_str) {
                Some("none" | "auto" | "any") if object.get("name").is_none() => Ok(()),
                Some("tool") if object.get("name").and_then(Value::as_str).is_some() => Ok(()),
                _ => Err(ApiError::invalid_request()),
            }
        }
        _ => Err(ApiError::invalid_request()),
    }
}

fn tool_result_text(object: &Map<String, Value>) -> Result<String, ApiError> {
    let content = object.get("content").cloned().unwrap_or_else(|| json!(""));
    match content {
        Value::String(text) => Ok(text),
        Value::Array(items) => {
            let mut text = String::new();
            for item in items {
                let item = item.as_object().ok_or_else(ApiError::invalid_request)?;
                if item.keys().any(|key| key != "type" && key != "text")
                    || item.get("type").and_then(Value::as_str) != Some("text")
                {
                    return Err(ApiError::invalid_request());
                }
                text.push_str(
                    item.get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(ApiError::invalid_request)?,
                );
            }
            Ok(text)
        }
        _ => Err(ApiError::invalid_request()),
    }
}

fn web_search_replay_text(object: &Map<String, Value>) -> Result<String, ApiError> {
    match object.get("type").and_then(Value::as_str) {
        Some("server_tool_use") => {
            let id = object
                .get("id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            let query = object
                .get("input")
                .and_then(Value::as_object)
                .and_then(|input| input.get("query"))
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            Ok(format!("Web search call {id}: {query}"))
        }
        Some("web_search_tool_result") => {
            let id = object
                .get("tool_use_id")
                .and_then(Value::as_str)
                .filter(|value| !value.is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            let results = object
                .get("content")
                .and_then(Value::as_array)
                .ok_or_else(ApiError::invalid_request)?;
            let mut text = format!("Web search results for {id}:");
            for result in results {
                let result = result.as_object().ok_or_else(ApiError::invalid_request)?;
                if result.get("type").and_then(Value::as_str) != Some("web_search_result") {
                    return Err(ApiError::invalid_request());
                }
                let title = result
                    .get("title")
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::invalid_request)?;
                let url = result
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(ApiError::invalid_request)?;
                let encrypted = result
                    .get("encrypted_content")
                    .and_then(Value::as_str)
                    .filter(|value| value.starts_with(SEARCH_OPAQUE_PREFIX))
                    .ok_or_else(ApiError::invalid_request)?;
                let opaque = decode_search_opaque(encrypted, "search-result")?;
                if opaque.get("url").and_then(Value::as_str) != Some(url)
                    || opaque.get("title").and_then(Value::as_str) != Some(title)
                {
                    return Err(ApiError::invalid_request());
                }
                let snippet = opaque
                    .get("snippet")
                    .and_then(Value::as_str)
                    .unwrap_or_default();
                text.push_str(&format!("\n- {title}: {url} {snippet}"));
            }
            Ok(text)
        }
        _ => Err(ApiError::invalid_request()),
    }
}

fn content_to_openai(content: &Value) -> Result<(Vec<Value>, Vec<Value>), ApiError> {
    let items = match content {
        Value::String(text) => vec![json!({"type":"text","text":text})],
        Value::Array(items) => items.clone(),
        _ => return Err(ApiError::invalid_request()),
    };
    let mut text = Vec::new();
    let mut tool_calls = Vec::new();
    for item in items {
        let object = item.as_object().ok_or_else(ApiError::invalid_request)?;
        match object.get("type").and_then(Value::as_str) {
            Some("text") => text.push(json!({"type":"text","text":object.get("text").and_then(Value::as_str).ok_or_else(ApiError::invalid_request)?})),
            Some("tool_use") => tool_calls.push(json!({
                "id": object.get("id").and_then(Value::as_str).ok_or_else(ApiError::invalid_request)?,
                "type": "function",
                "function": {
                    "name": object.get("name").and_then(Value::as_str).ok_or_else(ApiError::invalid_request)?,
                    "arguments": serde_json::to_string(object.get("input").ok_or_else(ApiError::invalid_request)?).map_err(|_| ApiError::invalid_request())?,
                }
            })),
            Some("tool_result") => {
                let is_error = object
                    .get("is_error")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                let text_value = tool_result_text(object)?;
                let text_value = if is_error {
                    format!("tool_error: {text_value}")
                } else {
                    text_value
                };
                text.push(json!({"type":"text","text":text_value}));
            }
            Some("server_tool_use") | Some("web_search_tool_result") => {
                text.push(json!({
                    "type":"text",
                    "text":web_search_replay_text(object)?
                }));
            }
            _ => return Err(ApiError::invalid_request()),
        }
    }
    Ok((text, tool_calls))
}

pub(super) fn to_chat_payload(object: &Map<String, Value>) -> Result<Value, ApiError> {
    let mut messages = Vec::new();
    if let Some(system) = object.get("system") {
        let (content, _) = content_to_openai(system)?;
        messages.push(json!({"role":"system","content":content}));
    }
    for message in object
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(ApiError::invalid_request)?
    {
        let source = message.as_object().ok_or_else(ApiError::invalid_request)?;
        let role = source
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(ApiError::invalid_request)?;
        if role == "user"
            && let Some(items) = source.get("content").and_then(Value::as_array)
        {
            let mut text_parts = Vec::new();
            for item in items {
                let item = item.as_object().ok_or_else(ApiError::invalid_request)?;
                match item.get("type").and_then(Value::as_str) {
                    Some("text") => text_parts.push(
                        item.get("text")
                            .and_then(Value::as_str)
                            .ok_or_else(ApiError::invalid_request)?
                            .to_owned(),
                    ),
                    Some("tool_result") => {
                        if !text_parts.is_empty() {
                            messages.push(json!({"role":"user","content":text_parts.join("")}));
                            text_parts.clear();
                        }
                        let is_error = item
                            .get("is_error")
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                        let content = tool_result_text(item)?;
                        let content = if is_error {
                            format!("tool_error: {content}")
                        } else {
                            content
                        };
                        messages.push(json!({
                            "role":"tool",
                            "tool_call_id":item.get("tool_use_id").cloned().ok_or_else(ApiError::invalid_request)?,
                            "content":content,
                        }));
                    }
                    _ => return Err(ApiError::invalid_request()),
                }
            }
            if !text_parts.is_empty() {
                messages.push(json!({"role":"user","content":text_parts.join("")}));
            }
            continue;
        }
        let (content, tool_calls) = content_to_openai(
            source
                .get("content")
                .ok_or_else(ApiError::invalid_request)?,
        )?;
        let mut converted = json!({"role":role,"content": if content.len() == 1 { content[0].get("text").cloned().unwrap_or(Value::Array(content.clone())) } else { Value::Array(content) }});
        if !tool_calls.is_empty() {
            converted["tool_calls"] = Value::Array(tool_calls);
        }
        messages.push(converted);
    }
    let mut payload = json!({
        "model": object.get("model").cloned().ok_or_else(ApiError::invalid_request)?,
        "messages": messages,
        "max_tokens": object.get("max_tokens").cloned().ok_or_else(ApiError::invalid_request)?,
        "stream": object.get("stream").and_then(Value::as_bool).unwrap_or(false),
    });
    if let Some(tools) = object.get("tools") {
        payload["tools"] = Value::Array(
            tools
                .as_array()
                .ok_or_else(ApiError::invalid_request)?
                .iter()
                .map(|tool| {
                    let tool = tool.as_object().ok_or_else(ApiError::invalid_request)?;
                    Ok(json!({
                        "type":"function",
                        "function": {
                            "name": tool.get("name").cloned().ok_or_else(ApiError::invalid_request)?,
                            "description": tool.get("description").cloned().unwrap_or_else(|| json!("")),
                            "parameters": tool.get("input_schema").cloned().ok_or_else(ApiError::invalid_request)?,
                        }
                    }))
                })
                .collect::<Result<Vec<_>, ApiError>>()?
        );
    }
    if let Some(choice) = object.get("tool_choice") {
        payload["tool_choice"] = match choice {
            Value::Object(object) => match object.get("type").and_then(Value::as_str) {
                Some("none") => Value::String("none".to_owned()),
                Some("auto") => Value::String("auto".to_owned()),
                Some("any") => Value::String("required".to_owned()),
                Some("tool") => json!({
                    "type":"function",
                    "function":{"name":object.get("name").and_then(Value::as_str).ok_or_else(ApiError::invalid_request)?}
                }),
                _ => return Err(ApiError::invalid_request()),
            },
            _ => return Err(ApiError::invalid_request()),
        };
        if choice
            .get("disable_parallel_tool_use")
            .and_then(Value::as_bool)
            .is_some_and(|disabled| disabled)
        {
            payload["parallel_tool_calls"] = Value::Bool(false);
        }
    }
    Ok(payload)
}

fn responses_content_block(item: &Map<String, Value>) -> Result<Vec<Value>, ApiError> {
    match item.get("type").and_then(Value::as_str) {
        Some("text") => Ok(vec![json!({
            "type": "input_text",
            "text": item.get("text").and_then(Value::as_str).ok_or_else(ApiError::invalid_request)?,
        })]),
        Some("image") => {
            let source = item
                .get("source")
                .and_then(Value::as_object)
                .ok_or_else(ApiError::invalid_request)?;
            if source.get("type").and_then(Value::as_str) != Some("base64") {
                return Err(ApiError::invalid_request());
            }
            let media_type = source
                .get("media_type")
                .and_then(Value::as_str)
                .ok_or_else(ApiError::invalid_request)?;
            let data = source
                .get("data")
                .and_then(Value::as_str)
                .ok_or_else(ApiError::invalid_request)?;
            Ok(vec![json!({
                "type": "input_image",
                "image_url": format!("data:{media_type};base64,{data}"),
            })])
        }
        Some("image_url") | Some("input_image") => {
            let url = item
                .get("url")
                .or_else(|| item.get("image_url"))
                .and_then(|value| {
                    value
                        .as_str()
                        .or_else(|| value.get("url").and_then(Value::as_str))
                })
                .ok_or_else(ApiError::invalid_request)?;
            Ok(vec![json!({"type":"input_image","image_url":url})])
        }
        Some("server_tool_use") | Some("web_search_tool_result") => Ok(vec![json!({
            "type":"input_text",
            "text":web_search_replay_text(item)?
        })]),
        _ => Err(ApiError::invalid_request()),
    }
}

pub(super) fn to_responses_payload(object: &Map<String, Value>) -> Result<Value, ApiError> {
    let mut input = Vec::new();
    for message in object
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(ApiError::invalid_request)?
    {
        let message = message.as_object().ok_or_else(ApiError::invalid_request)?;
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(ApiError::invalid_request)?;
        let content = message
            .get("content")
            .ok_or_else(ApiError::invalid_request)?;
        let items = match content {
            Value::String(text) => vec![json!({"type":"input_text","text":text})],
            Value::Array(items) => {
                let mut converted = Vec::new();
                for item in items {
                    let item = item.as_object().ok_or_else(ApiError::invalid_request)?;
                    match item.get("type").and_then(Value::as_str) {
                        Some("tool_use") => {
                            let input_value =
                                item.get("input").ok_or_else(ApiError::invalid_request)?;
                            input.push(json!({
                                "type":"function_call",
                                "id":item.get("id").cloned().ok_or_else(ApiError::invalid_request)?,
                                "call_id":item.get("id").cloned().ok_or_else(ApiError::invalid_request)?,
                                "name":item.get("name").cloned().ok_or_else(ApiError::invalid_request)?,
                                "arguments":serde_json::to_string(input_value).map_err(|_| ApiError::invalid_request())?,
                            }));
                        }
                        Some("tool_result") => {
                            let output = tool_result_text(item)?;
                            let output = if item
                                .get("is_error")
                                .and_then(Value::as_bool)
                                .unwrap_or(false)
                            {
                                format!("tool_error: {output}")
                            } else {
                                output
                            };
                            input.push(json!({
                                "type":"function_call_output",
                                "call_id":item.get("tool_use_id").cloned().ok_or_else(ApiError::invalid_request)?,
                                "output":output,
                            }));
                        }
                        _ => converted.extend(responses_content_block(item)?),
                    }
                }
                converted
            }
            _ => return Err(ApiError::invalid_request()),
        };
        if !items.is_empty() {
            input.push(json!({"type":"message","role":role,"content":items}));
        }
    }
    if input.is_empty() {
        return Err(ApiError::invalid_request());
    }
    let mut payload = json!({
        "model": object.get("model").cloned().ok_or_else(ApiError::invalid_request)?,
        "input": input,
        "stream": object.get("stream").and_then(Value::as_bool).unwrap_or(false),
        "max_output_tokens": object.get("max_tokens").cloned().ok_or_else(ApiError::invalid_request)?,
    });
    if let Some(system) = object.get("system") {
        payload["instructions"] = match system {
            Value::String(value) => Value::String(value.clone()),
            Value::Array(items) => Value::String(
                items
                    .iter()
                    .map(|item| item.get("text").and_then(Value::as_str).unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join(""),
            ),
            _ => return Err(ApiError::invalid_request()),
        };
    }
    if let Some(tools) = object.get("tools") {
        let mut converted = Vec::new();
        for tool in tools.as_array().ok_or_else(ApiError::invalid_request)? {
            let tool = tool.as_object().ok_or_else(ApiError::invalid_request)?;
            match tool.get("type").and_then(Value::as_str) {
                Some("web_search_20250305") => converted.push(json!({
                    "type":"web_search_preview",
                    "search_context_size":"medium"
                })),
                Some("function") | None => converted.push(json!({
                    "type":"function",
                    "name":tool.get("name").cloned().ok_or_else(ApiError::invalid_request)?,
                    "description":tool.get("description").cloned().unwrap_or_else(|| json!("")),
                    "parameters":tool.get("input_schema").cloned().ok_or_else(ApiError::invalid_request)?,
                })),
                _ => return Err(ApiError::invalid_request()),
            }
        }
        payload["tools"] = Value::Array(converted);
    }
    if object
        .get("tool_choice")
        .is_some_and(|choice| choice != &json!({"type":"auto"}))
    {
        return Err(ApiError::invalid_request());
    }
    Ok(payload)
}

pub(super) fn from_responses_response(body: &[u8], model: &str) -> Result<Value, ApiError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| ApiError::upstream())?;
    let output = value
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(ApiError::upstream)?;
    let mut content = Vec::new();
    let mut searches = Vec::<(Value, String)>::new();
    for item in output {
        let item = item.as_object().ok_or_else(ApiError::upstream)?;
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                for part in item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(ApiError::upstream)?
                {
                    let part = part.as_object().ok_or_else(ApiError::upstream)?;
                    if part.get("type").and_then(Value::as_str) == Some("output_text") {
                        let text = part
                            .get("text")
                            .and_then(Value::as_str)
                            .ok_or_else(ApiError::upstream)?;
                        let mut block = json!({"type":"text","text":text});
                        let citations = part
                            .get("annotations")
                            .map(|annotations| {
                                let annotations =
                                    annotations.as_array().ok_or_else(ApiError::upstream)?;
                                citations_from_annotations(
                                    &annotations.iter().collect::<Vec<_>>(),
                                    text,
                                )
                            })
                            .transpose()?
                            .unwrap_or_default();
                        if !citations.is_empty() {
                            block["citations"] = Value::Array(citations);
                        }
                        content.push(block);
                    }
                }
            }
            Some("function_call") => {
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::upstream)?;
                let input: Value =
                    serde_json::from_str(arguments).map_err(|_| ApiError::upstream())?;
                if !input.is_object() {
                    return Err(ApiError::upstream());
                }
                content.push(json!({"type":"tool_use","id":item.get("call_id").or_else(|| item.get("id")).cloned().ok_or_else(ApiError::upstream)?,"name":item.get("name").cloned().ok_or_else(ApiError::upstream)?,"input":input}));
            }
            Some("web_search_call") => {
                let id = item
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(ApiError::upstream)?
                    .to_owned();
                let query = item
                    .get("action")
                    .and_then(Value::as_object)
                    .and_then(|action| action.get("query"))
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::upstream)?;
                searches.push((json!(id), query.to_owned()));
            }
            _ => return Err(ApiError::upstream()),
        }
    }
    let text = content
        .iter()
        .find_map(|item| (item["type"] == "text").then(|| item["text"].as_str()))
        .flatten()
        .unwrap_or_default();
    if !searches.is_empty() {
        let annotations = output
            .iter()
            .filter_map(Value::as_object)
            .filter(|item| item.get("type").and_then(Value::as_str) == Some("message"))
            .flat_map(|item| item.get("content").and_then(Value::as_array))
            .flatten()
            .filter_map(Value::as_object)
            .filter(|part| part.get("type").and_then(Value::as_str) == Some("output_text"))
            .flat_map(|part| part.get("annotations").and_then(Value::as_array))
            .flatten()
            .collect::<Vec<_>>();
        let mut result_blocks = Vec::new();
        let mut seen_sources = std::collections::HashSet::new();
        for block in search_result_blocks_from_annotations(&annotations, text)? {
            let key = (
                block["url"].as_str().unwrap_or_default().to_owned(),
                block["title"].as_str().unwrap_or_default().to_owned(),
            );
            if seen_sources.insert(key) {
                result_blocks.push(block);
            }
        }
        if result_blocks.is_empty() {
            return Err(ApiError::upstream());
        }
        let mut searched_content = Vec::new();
        for (id, query) in searches {
            searched_content.push(json!({
                "type":"server_tool_use",
                "id":id,
                "name":"web_search",
                "input":{"query":query}
            }));
            searched_content.push(json!({
                "type":"web_search_tool_result",
                "tool_use_id":id,
                "content":result_blocks
            }));
        }
        let mut merged = searched_content;
        merged.extend(content);
        content = merged;
    }
    // `server_tool_use` is the completed web-search side channel.  It is
    // reported as content, but it is not a client tool call that asks the
    // caller for a tool result; Python's Anthropic adapter therefore ends a
    // successful search turn normally.
    let stop_reason = if content.iter().any(|item| item["type"] == "tool_use") {
        "tool_use"
    } else {
        "end_turn"
    };
    let mut usage = value.get("usage").cloned().unwrap_or_else(|| json!({}));
    let search_requests = content
        .iter()
        .filter(|item| item["type"] == "server_tool_use")
        .count();
    if search_requests > 0 {
        usage["server_tool_use"] = json!({"web_search_requests": search_requests});
    }
    Ok(json!({
        "id":value.get("id").cloned().unwrap_or_else(|| json!(format!("msg_{}",native_message_id()))),
        "type":"message",
        "role":"assistant",
        "model":model,
        "content":content,
        "stop_reason":stop_reason,
        "stop_sequence":null,
        "usage":{
            "input_tokens":usage.get("input_tokens").and_then(Value::as_u64).unwrap_or(0),
            "output_tokens":usage.get("output_tokens").and_then(Value::as_u64).unwrap_or(0),
            "server_tool_use":usage.get("server_tool_use").cloned().unwrap_or(Value::Null),
        }
    }))
}

pub(super) fn from_chat_response(body: &[u8], model: &str) -> Result<Value, ApiError> {
    let value: Value = serde_json::from_slice(body).map_err(|_| ApiError::upstream())?;
    let choice = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|items| items.first())
        .ok_or_else(ApiError::upstream)?;
    let message = choice
        .get("message")
        .and_then(Value::as_object)
        .ok_or_else(ApiError::upstream)?;
    let mut content = Vec::new();
    if let Some(text) = message.get("content").and_then(Value::as_str) {
        content.push(json!({"type":"text","text":text}));
    }
    let tool_calls = message
        .get("tool_calls")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for call in &tool_calls {
        let function = call
            .get("function")
            .and_then(Value::as_object)
            .ok_or_else(ApiError::upstream)?;
        let arguments = function
            .get("arguments")
            .and_then(Value::as_str)
            .ok_or_else(ApiError::upstream)?;
        let input = serde_json::from_str::<Value>(arguments).map_err(|_| ApiError::upstream())?;
        if !input.is_object() {
            return Err(ApiError::upstream());
        }
        content.push(json!({"type":"tool_use","id":call.get("id").cloned().ok_or_else(ApiError::upstream)?,"name":function.get("name").cloned().ok_or_else(ApiError::upstream)?,"input":input}));
    }
    let finish_reason = choice
        .get("finish_reason")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::upstream)?;
    if finish_reason == "tool_calls" && tool_calls.is_empty() {
        return Err(ApiError::upstream());
    }
    if !tool_calls.is_empty() && finish_reason != "tool_calls" {
        return Err(ApiError::upstream());
    }
    let stop_reason = match finish_reason {
        "stop" => "end_turn",
        "tool_calls" => "tool_use",
        "length" => "max_tokens",
        _ => return Err(ApiError::upstream()),
    };
    let usage = value
        .get("usage")
        .cloned()
        .unwrap_or_else(|| json!({"prompt_tokens":0,"completion_tokens":0}));
    Ok(
        json!({"id":format!("msg_{}",native_message_id()),"type":"message","role":"assistant","model":model,"content":content,"stop_reason":stop_reason,"stop_sequence":Value::Null,"usage":{"input_tokens":usage.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0),"output_tokens":usage.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0)}}),
    )
}

pub(super) fn stream_response(
    response: reqwest::Response,
    model: String,
    deadline: Instant,
) -> Response {
    stream_body_response(
        Body::from_stream(
            response
                .bytes_stream()
                .map(|result| result.map_err(|error| io::Error::other(error.to_string()))),
        ),
        model,
        deadline,
    )
}

struct ResponsesStreamTool {
    index: usize,
    arguments: String,
    stopped: bool,
}

struct ResponsesStreamSearch {
    id: String,
    query: Option<String>,
    server_index: Option<usize>,
    result_index: Option<usize>,
    stopped: bool,
}

struct ResponsesStreamState {
    input: Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>,
    buffer: Vec<u8>,
    model: String,
    started: bool,
    text_block: Option<usize>,
    text_buffer: String,
    tools: BTreeMap<usize, ResponsesStreamTool>,
    searches: BTreeMap<usize, ResponsesStreamSearch>,
    next_block: usize,
    terminal: bool,
}

fn responses_message_start(output: &mut Vec<u8>, model: &str) {
    output.extend_from_slice(
        format!(
            "event: message_start\ndata: {}\n\n",
            json!({
                "type":"message_start",
                "message":{
                    "id":format!("msg_{}",native_message_id()),
                    "type":"message",
                    "role":"assistant",
                    "model":model,
                    "content":[],
                    "stop_reason":null,
                    "stop_sequence":null,
                    "usage":{"input_tokens":0,"output_tokens":0}
                }
            })
        )
        .as_bytes(),
    );
}

fn anthropic_sse(output: &mut Vec<u8>, event: &str, value: Value) {
    output.extend_from_slice(format!("event: {event}\ndata: {value}\n\n").as_bytes());
}

fn response_stream_error(
    state: ResponsesStreamState,
    message: &'static str,
) -> (Result<Bytes, io::Error>, ResponsesStreamState) {
    (
        Err(io::Error::other(message)),
        ResponsesStreamState {
            terminal: true,
            ..state
        },
    )
}

pub(super) fn stream_responses_body_response(
    body: Body,
    model: String,
    deadline: Instant,
) -> Response {
    let input = Box::pin(
        body.into_data_stream()
            .map(|result| result.map_err(|error| io::Error::other(error.to_string()))),
    );
    let state = ResponsesStreamState {
        input,
        buffer: Vec::new(),
        model,
        started: false,
        text_block: None,
        text_buffer: String::new(),
        tools: BTreeMap::new(),
        searches: BTreeMap::new(),
        next_block: 0,
        terminal: false,
    };
    let stream = stream::unfold(state, move |mut state| async move {
        if state.terminal {
            return None;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Some(response_stream_error(
                state,
                "Anthropic Responses stream timed out",
            ));
        }
        let chunk = match tokio::time::timeout(remaining, state.input.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(Some(Err(_))) | Err(_) => {
                return Some(response_stream_error(
                    state,
                    "Anthropic Responses stream failed",
                ));
            }
            Ok(None) => {
                return Some(response_stream_error(
                    state,
                    "Anthropic Responses stream ended before completion",
                ));
            }
        };
        state.buffer.extend_from_slice(&chunk);
        let mut output = Vec::new();
        while let Some((position, delimiter)) = sse_delimiter(&state.buffer) {
            let event = state.buffer.drain(..position).collect::<Vec<_>>();
            state.buffer.drain(..delimiter);
            let Some(data) = event.strip_prefix(b"data: ") else {
                continue;
            };
            if data == b"[DONE]" {
                if !state.terminal {
                    return Some(response_stream_error(
                        state,
                        "Anthropic Responses stream ended before completion",
                    ));
                }
                continue;
            }
            let value: Value = match serde_json::from_slice(data) {
                Ok(value) => value,
                Err(_) => {
                    return Some(response_stream_error(
                        state,
                        "Anthropic Responses stream contained malformed JSON",
                    ));
                }
            };
            match value.get("type").and_then(Value::as_str) {
                Some("response.created") if !state.started => {
                    responses_message_start(&mut output, &state.model);
                    state.started = true;
                }
                Some("response.created") => {}
                Some("response.output_text.delta") => {
                    let text = value.get("delta").and_then(Value::as_str).ok_or_else(|| {
                        io::Error::other("Anthropic Responses text delta is malformed")
                    });
                    let text = match text {
                        Ok(text) => text,
                        Err(error) => {
                            return Some((
                                Err(error),
                                ResponsesStreamState {
                                    terminal: true,
                                    ..state
                                },
                            ));
                        }
                    };
                    if !state.searches.is_empty() {
                        state.text_buffer.push_str(text);
                        continue;
                    }
                    if !state.started {
                        responses_message_start(&mut output, &state.model);
                        state.started = true;
                    }
                    let index = state.text_block.unwrap_or_else(|| {
                        let index = state.next_block;
                        state.next_block += 1;
                        anthropic_sse(
                            &mut output,
                            "content_block_start",
                            json!({"type":"content_block_start","index":index,"content_block":{"type":"text","text":""}}),
                        );
                        state.text_block = Some(index);
                        index
                    });
                    anthropic_sse(
                        &mut output,
                        "content_block_delta",
                        json!({"type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":text}}),
                    );
                }
                Some("response.output_item.added") | Some("response.output_item.done") => {
                    let output_index = value
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .and_then(|value| usize::try_from(value).ok())
                        .ok_or_else(|| io::Error::other("Responses item missing output_index"));
                    let item = value.get("item").and_then(Value::as_object);
                    let (output_index, item) = match (output_index, item) {
                        (Ok(index), Some(item)) => (index, item),
                        _ => {
                            return Some(response_stream_error(
                                state,
                                "Responses item is malformed",
                            ));
                        }
                    };
                    let item_type = item.get("type").and_then(Value::as_str);
                    if item_type == Some("web_search_call") {
                        let id = item
                            .get("id")
                            .and_then(Value::as_str)
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned);
                        let query = item
                            .get("action")
                            .and_then(Value::as_object)
                            .and_then(|action| action.get("query"))
                            .and_then(Value::as_str)
                            .filter(|value| !value.is_empty())
                            .map(str::to_owned);
                        let Some(id) = id else {
                            return Some(response_stream_error(
                                state,
                                "Responses web search item is malformed",
                            ));
                        };
                        let is_done = value.get("type").and_then(Value::as_str)
                            == Some("response.output_item.done");
                        let entry = state.searches.entry(output_index).or_insert_with(|| {
                            ResponsesStreamSearch {
                                id: id.clone(),
                                query: None,
                                server_index: None,
                                result_index: None,
                                stopped: false,
                            }
                        });
                        if entry.id != id {
                            return Some(response_stream_error(
                                state,
                                "Responses web search id changed",
                            ));
                        }
                        if query.is_some() {
                            entry.query = query;
                        }
                        if !is_done {
                            continue;
                        }
                        let Some(query) = entry.query.clone() else {
                            return Some(response_stream_error(
                                state,
                                "Responses web search item has no query",
                            ));
                        };
                        if entry.server_index.is_some() {
                            continue;
                        }
                        if !state.started {
                            responses_message_start(&mut output, &state.model);
                            state.started = true;
                        }
                        let block_index = state.next_block;
                        state.next_block += 1;
                        anthropic_sse(
                            &mut output,
                            "content_block_start",
                            json!({
                                "type":"content_block_start",
                                "index":block_index,
                                "content_block":{"type":"server_tool_use","id":id,"name":"web_search","input":{"query":query}}
                            }),
                        );
                        anthropic_sse(
                            &mut output,
                            "content_block_stop",
                            json!({"type":"content_block_stop","index":block_index}),
                        );
                        entry.server_index = Some(block_index);
                        continue;
                    }
                    if item_type != Some("function_call") {
                        continue;
                    }
                    if let Some(tool) = state.tools.get_mut(&output_index) {
                        if let Some(arguments) = item.get("arguments").and_then(Value::as_str) {
                            tool.arguments = arguments.to_owned();
                        }
                        continue;
                    }
                    let id = item
                        .get("call_id")
                        .or_else(|| item.get("id"))
                        .and_then(Value::as_str);
                    let name = item.get("name").and_then(Value::as_str);
                    let (Some(id), Some(name)) = (id, name) else {
                        return Some(response_stream_error(
                            state,
                            "Responses function item is malformed",
                        ));
                    };
                    if !state.started {
                        responses_message_start(&mut output, &state.model);
                        state.started = true;
                    }
                    let block_index = state.next_block;
                    state.next_block += 1;
                    anthropic_sse(
                        &mut output,
                        "content_block_start",
                        json!({"type":"content_block_start","index":block_index,"content_block":{"type":"tool_use","id":id,"name":name,"input":{}}}),
                    );
                    state.tools.insert(
                        output_index,
                        ResponsesStreamTool {
                            index: block_index,
                            arguments: String::new(),
                            stopped: false,
                        },
                    );
                }
                Some("response.function_call_arguments.delta") => {
                    let output_index = value
                        .get("output_index")
                        .and_then(Value::as_u64)
                        .and_then(|value| usize::try_from(value).ok());
                    let delta = value.get("delta").and_then(Value::as_str);
                    let (Some(output_index), Some(delta)) = (output_index, delta) else {
                        return Some(response_stream_error(
                            state,
                            "Responses function delta is malformed",
                        ));
                    };
                    let Some(tool) = state.tools.get_mut(&output_index) else {
                        return Some(response_stream_error(
                            state,
                            "Responses function delta has no block",
                        ));
                    };
                    tool.arguments.push_str(delta);
                    anthropic_sse(
                        &mut output,
                        "content_block_delta",
                        json!({"type":"content_block_delta","index":tool.index,"delta":{"type":"input_json_delta","partial_json":delta}}),
                    );
                }
                Some("response.completed") => {
                    if !state.started {
                        return Some(response_stream_error(
                            state,
                            "Responses completed without content",
                        ));
                    }
                    if state.tools.values().any(|tool| {
                        serde_json::from_str::<Value>(&tool.arguments)
                            .ok()
                            .is_none_or(|value| !value.is_object())
                    }) {
                        return Some(response_stream_error(
                            state,
                            "Responses function arguments are incomplete",
                        ));
                    }
                    let completed = value.get("response").and_then(Value::as_object);
                    let annotations = completed
                        .and_then(|response| response.get("output"))
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .filter(|item| item.get("type").and_then(Value::as_str) == Some("message"))
                        .flat_map(|item| {
                            item.get("content")
                                .and_then(Value::as_array)
                                .into_iter()
                                .flatten()
                        })
                        .filter(|block| {
                            block.get("type").and_then(Value::as_str) == Some("output_text")
                        })
                        .flat_map(|block| {
                            block
                                .get("annotations")
                                .and_then(Value::as_array)
                                .into_iter()
                                .flatten()
                        })
                        .filter(|annotation| {
                            annotation.get("type").and_then(Value::as_str) == Some("url_citation")
                        })
                        .collect::<Vec<_>>();
                    if !state.searches.is_empty() {
                        let text = state.text_buffer.clone();
                        let result_content =
                            match search_result_blocks_from_annotations(&annotations, &text) {
                                Ok(content) if !content.is_empty() => content,
                                _ => {
                                    return Some(response_stream_error(
                                        state,
                                        "Responses search has no replayable sources",
                                    ));
                                }
                            };
                        let search_ids = state
                            .searches
                            .values()
                            .map(|search| (search.id.clone(), search.query.clone()))
                            .collect::<Vec<_>>();
                        for (id, query) in search_ids {
                            let Some(_query) = query else {
                                return Some(response_stream_error(
                                    state,
                                    "Responses search completed without a query",
                                ));
                            };
                            let result_index = state.next_block;
                            state.next_block += 1;
                            anthropic_sse(
                                &mut output,
                                "content_block_start",
                                json!({
                                    "type":"content_block_start",
                                    "index":result_index,
                                    "content_block":{"type":"web_search_tool_result","tool_use_id":id,"content":result_content}
                                }),
                            );
                            anthropic_sse(
                                &mut output,
                                "content_block_stop",
                                json!({"type":"content_block_stop","index":result_index}),
                            );
                            if let Some(search) =
                                state.searches.values_mut().find(|search| search.id == id)
                            {
                                search.result_index = Some(result_index);
                                search.stopped = true;
                            }
                        }
                        if state.text_block.is_none() && !state.text_buffer.is_empty() {
                            let index = state.next_block;
                            state.next_block += 1;
                            state.text_block = Some(index);
                            anthropic_sse(
                                &mut output,
                                "content_block_start",
                                json!({"type":"content_block_start","index":index,"content_block":{"type":"text","text":"","citations":[]}}),
                            );
                            anthropic_sse(
                                &mut output,
                                "content_block_delta",
                                json!({"type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":state.text_buffer}}),
                            );
                        }
                    }
                    if let Some(index) = state.text_block {
                        let citations =
                            match citations_from_annotations(&annotations, &state.text_buffer) {
                                Ok(citations) => citations,
                                Err(_) => {
                                    return Some(response_stream_error(
                                        state,
                                        "Responses citation is malformed",
                                    ));
                                }
                            };
                        for citation in citations {
                            anthropic_sse(
                                &mut output,
                                "content_block_delta",
                                json!({"type":"content_block_delta","index":index,"delta":{"type":"citations_delta","citation":citation}}),
                            );
                        }
                    }
                    if let Some(index) = state.text_block {
                        anthropic_sse(
                            &mut output,
                            "content_block_stop",
                            json!({"type":"content_block_stop","index":index}),
                        );
                    }
                    for tool in state.tools.values_mut() {
                        if !tool.stopped {
                            anthropic_sse(
                                &mut output,
                                "content_block_stop",
                                json!({"type":"content_block_stop","index":tool.index}),
                            );
                            tool.stopped = true;
                        }
                    }
                    for search in state.searches.values_mut() {
                        search.stopped = true;
                    }
                    let stop_reason = if state.tools.is_empty() {
                        "end_turn"
                    } else {
                        "tool_use"
                    };
                    let output_tokens = completed
                        .and_then(|response| response.get("usage"))
                        .and_then(Value::as_object)
                        .and_then(|usage| usage.get("output_tokens"))
                        .and_then(Value::as_u64)
                        .unwrap_or(0);
                    let mut stream_usage = json!({"output_tokens":output_tokens});
                    if !state.searches.is_empty() {
                        stream_usage["server_tool_use"] =
                            json!({"web_search_requests": state.searches.len()});
                    }
                    anthropic_sse(
                        &mut output,
                        "message_delta",
                        json!({"type":"message_delta","delta":{"stop_reason":stop_reason},"usage":stream_usage}),
                    );
                    anthropic_sse(&mut output, "message_stop", json!({"type":"message_stop"}));
                    state.terminal = true;
                }
                Some("response.failed") | Some("response.incomplete") => {
                    return Some(response_stream_error(state, "Responses stream failed"));
                }
                _ => {
                    return Some(response_stream_error(
                        state,
                        "Responses stream contained an unsupported event",
                    ));
                }
            }
        }
        if output.is_empty() && state.terminal {
            return None;
        }
        Some((Ok(Bytes::from(output)), state))
    });
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
}

pub(super) fn anthropic_stream_responses_response(
    response: reqwest::Response,
    model: String,
    deadline: Instant,
) -> Response {
    stream_responses_body_response(
        Body::from_stream(
            response
                .bytes_stream()
                .map(|result| result.map_err(|error| io::Error::other(error.to_string()))),
        ),
        model,
        deadline,
    )
}

#[derive(Clone, Copy)]
struct StreamBlock {
    stopped: bool,
}

#[derive(Default)]
struct StreamBlocks {
    text_block: Option<usize>,
    blocks: Vec<StreamBlock>,
    tool_blocks: HashMap<usize, usize>,
    tool_arguments: HashMap<usize, String>,
}

fn append_message_start(output: &mut Vec<u8>, model: &str) {
    output.extend_from_slice(
        format!(
            "event: message_start\ndata: {}\n\n",
            json!({"type":"message_start","message":{"id":format!("msg_{}",native_message_id()),"type":"message","role":"assistant","model":model,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":0,"output_tokens":0}}})
        )
        .as_bytes(),
    );
}

pub(super) fn stream_body_response(body: Body, model: String, deadline: Instant) -> Response {
    type Input = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>;
    let input: Input = Box::pin(
        body.into_data_stream()
            .map(|result| result.map_err(|error| io::Error::other(error.to_string()))),
    );
    let stream = stream::unfold(
        (
            input,
            Vec::new(),
            false,
            false,
            StreamBlocks::default(),
            false,
            model,
        ),
        move |(
            mut input,
            mut buffer,
            mut done,
            mut started,
            mut block_state,
            mut terminal_seen,
            model,
        )| async move {
            if done {
                return None;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Some((
                    Err(io::Error::other("Anthropic stream timed out")),
                    (
                        input,
                        buffer,
                        true,
                        started,
                        block_state,
                        terminal_seen,
                        model,
                    ),
                ));
            }
            let chunk = match tokio::time::timeout(remaining, input.next()).await {
                Ok(Some(Ok(chunk))) => chunk,
                Ok(Some(Err(_))) | Err(_) => {
                    return Some((
                        Err(io::Error::other("Anthropic stream failed")),
                        (
                            input,
                            buffer,
                            true,
                            started,
                            block_state,
                            terminal_seen,
                            model,
                        ),
                    ));
                }
                Ok(None) => {
                    if terminal_seen {
                        return None;
                    }
                    return Some((
                        Err(io::Error::other("Anthropic stream ended")),
                        (
                            input,
                            buffer,
                            true,
                            started,
                            block_state,
                            terminal_seen,
                            model,
                        ),
                    ));
                }
            };
            buffer.extend_from_slice(&chunk);
            let mut output = Vec::new();
            while let Some((position, delimiter)) = sse_delimiter(&buffer) {
                let event = buffer.drain(..position).collect::<Vec<_>>();
                buffer.drain(..delimiter);
                let Some(data) = event.strip_prefix(b"data: ") else {
                    continue;
                };
                if data == b"[DONE]" {
                    if !terminal_seen {
                        return Some((
                            Err(io::Error::other("Anthropic stream ended before completion")),
                            (
                                input,
                                buffer,
                                true,
                                started,
                                block_state,
                                terminal_seen,
                                model,
                            ),
                        ));
                    }
                    done = true;
                    break;
                }
                let value: Value = match serde_json::from_slice(data) {
                    Ok(value) => value,
                    Err(_) => {
                        return Some((
                            Err(io::Error::other(
                                "Anthropic stream contained malformed JSON",
                            )),
                            (
                                input,
                                buffer,
                                true,
                                started,
                                block_state,
                                terminal_seen,
                                model,
                            ),
                        ));
                    }
                };
                let delta = value.pointer("/choices/0/delta");
                if let Some(text) = delta
                    .and_then(|delta| delta.get("content"))
                    .and_then(Value::as_str)
                {
                    let index = if let Some(index) = block_state.text_block {
                        index
                    } else {
                        if !started {
                            append_message_start(&mut output, &model);
                            started = true;
                        }
                        let index = block_state.blocks.len();
                        output.extend_from_slice(
                            format!(
                                "event: content_block_start\ndata: {}\n\n",
                                json!({"type":"content_block_start","index":index,"content_block":{"type":"text","text":""}})
                            )
                            .as_bytes(),
                        );
                        block_state.text_block = Some(index);
                        block_state.blocks.push(StreamBlock { stopped: false });
                        index
                    };
                    output.extend_from_slice(
                        format!(
                            "event: content_block_delta\ndata: {}\n\n",
                            json!({"type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":text}})
                        )
                        .as_bytes(),
                    );
                }
                if let Some(tool_calls) = delta
                    .and_then(|delta| delta.get("tool_calls"))
                    .and_then(Value::as_array)
                {
                    for call in tool_calls {
                        let tool_index = call
                            .get("index")
                            .and_then(Value::as_u64)
                            .and_then(|value| usize::try_from(value).ok())
                            .ok_or_else(|| io::Error::other("Anthropic tool stream missing index"));
                        let tool_index = match tool_index {
                            Ok(index) => index,
                            Err(error) => {
                                return Some((
                                    Err(error),
                                    (
                                        input,
                                        buffer,
                                        true,
                                        started,
                                        block_state,
                                        terminal_seen,
                                        model,
                                    ),
                                ));
                            }
                        };
                        let function = call.get("function").and_then(Value::as_object);
                        let id = call.get("id").and_then(Value::as_str);
                        let name = function
                            .and_then(|function| function.get("name"))
                            .and_then(Value::as_str);
                        let block_index = if let Some(index) =
                            block_state.tool_blocks.get(&tool_index).copied()
                        {
                            index
                        } else {
                            let Some(id) = id else {
                                return Some((
                                    Err(io::Error::other("Anthropic tool stream missing id")),
                                    (
                                        input,
                                        buffer,
                                        true,
                                        started,
                                        block_state,
                                        terminal_seen,
                                        model,
                                    ),
                                ));
                            };
                            let Some(name) = name else {
                                return Some((
                                    Err(io::Error::other("Anthropic tool stream missing name")),
                                    (
                                        input,
                                        buffer,
                                        true,
                                        started,
                                        block_state,
                                        terminal_seen,
                                        model,
                                    ),
                                ));
                            };
                            if !started {
                                append_message_start(&mut output, &model);
                                started = true;
                            }
                            let index = block_state.blocks.len();
                            output.extend_from_slice(
                                format!(
                                    "event: content_block_start\ndata: {}\n\n",
                                    json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":id,"name":name,"input":{}}})
                                )
                                .as_bytes(),
                            );
                            block_state.tool_blocks.insert(tool_index, index);
                            block_state.blocks.push(StreamBlock { stopped: false });
                            index
                        };
                        if let Some(arguments) = function
                            .and_then(|function| function.get("arguments"))
                            .and_then(Value::as_str)
                            && !arguments.is_empty()
                        {
                            block_state
                                .tool_arguments
                                .entry(tool_index)
                                .or_default()
                                .push_str(arguments);
                            output.extend_from_slice(
                                format!(
                                    "event: content_block_delta\ndata: {}\n\n",
                                    json!({"type":"content_block_delta","index":block_index,"delta":{"type":"input_json_delta","partial_json":arguments}})
                                )
                                .as_bytes(),
                            );
                        }
                    }
                }
                if let Some(reason) = value.pointer("/choices/0/finish_reason").and_then(|value| {
                    if value.is_null() {
                        None
                    } else {
                        value.as_str()
                    }
                }) {
                    let stop_reason = match reason {
                        "stop" => "end_turn",
                        "length" => "max_tokens",
                        "tool_calls" => "tool_use",
                        _ => {
                            return Some((
                                Err(io::Error::other(
                                    "Anthropic stream has unknown finish reason",
                                )),
                                (
                                    input,
                                    buffer,
                                    true,
                                    started,
                                    block_state,
                                    terminal_seen,
                                    model,
                                ),
                            ));
                        }
                    };
                    if !started {
                        return Some((
                            Err(io::Error::other(
                                "Anthropic stream completed without content",
                            )),
                            (
                                input,
                                buffer,
                                true,
                                started,
                                block_state,
                                terminal_seen,
                                model,
                            ),
                        ));
                    }
                    let has_tool_blocks = !block_state.tool_blocks.is_empty();
                    if (reason == "tool_calls" && !has_tool_blocks)
                        || (reason != "tool_calls" && has_tool_blocks)
                    {
                        return Some((
                            Err(io::Error::other(
                                "Anthropic stream finish reason does not match content blocks",
                            )),
                            (
                                input,
                                buffer,
                                true,
                                started,
                                block_state,
                                terminal_seen,
                                model,
                            ),
                        ));
                    }
                    if reason == "tool_calls"
                        && block_state.tool_blocks.keys().any(|tool_index| {
                            let arguments = block_state
                                .tool_arguments
                                .get(tool_index)
                                .map(String::as_str)
                                .unwrap_or("{}");
                            serde_json::from_str::<Value>(arguments)
                                .ok()
                                .is_none_or(|value| !value.is_object())
                        })
                    {
                        return Some((
                            Err(io::Error::other(
                                "Anthropic tool stream contained incomplete arguments",
                            )),
                            (
                                input,
                                buffer,
                                true,
                                started,
                                block_state,
                                terminal_seen,
                                model,
                            ),
                        ));
                    }
                    for (index, block) in block_state.blocks.iter_mut().enumerate() {
                        if !block.stopped {
                            output.extend_from_slice(
                                format!(
                                    "event: content_block_stop\ndata: {}\n\n",
                                    json!({"type":"content_block_stop","index":index})
                                )
                                .as_bytes(),
                            );
                            block.stopped = true;
                        }
                    }
                    output.extend_from_slice(format!("event: message_delta\ndata: {}\n\n", json!({"type":"message_delta","delta":{"stop_reason":stop_reason},"usage":{"output_tokens":0}})).as_bytes());
                    output.extend_from_slice(
                        b"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n",
                    );
                    terminal_seen = true;
                    done = true;
                }
            }
            Some((
                Ok(Bytes::from(output)),
                (
                    input,
                    buffer,
                    done,
                    started,
                    block_state,
                    terminal_seen,
                    model,
                ),
            ))
        },
    );
    let mut response = Response::new(Body::from_stream(stream));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
}
