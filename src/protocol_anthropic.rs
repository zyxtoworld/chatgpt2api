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
    sync::LazyLock,
    time::Instant,
};

use regex::Regex;

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
        return Err(ApiError::validation_message(
            "Input should be a valid dictionary or object to extract fields from",
        ));
    };
    if object
        .get("model")
        .is_some_and(|value| !value.is_null() && !value.is_string())
    {
        return Err(ApiError::validation_message(
            "model: Input should be a valid string",
        ));
    }
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("auto")
        .to_owned();
    if let Some(messages) = object.get("messages") {
        if !messages.is_null() && !messages.is_array() {
            return Err(ApiError::validation_message(
                "messages: Input should be a valid list",
            ));
        }
        if let Some((index, _)) = messages
            .as_array()
            .into_iter()
            .flatten()
            .enumerate()
            .find(|(_, message)| !message.is_object())
        {
            return Err(ApiError::validation_message(format!(
                "messages.{index}: Input should be a valid dictionary"
            )));
        }
    }
    if let Some(value) = object.get("stream").filter(|value| !value.is_null()) {
        let stream = match value {
            Value::Bool(value) => *value,
            Value::Number(value) if value.as_i64() == Some(0) => false,
            Value::Number(value) if value.as_i64() == Some(1) => true,
            Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "t" | "yes" | "y" | "on" => true,
                "0" | "false" | "f" | "no" | "n" | "off" => false,
                _ => {
                    return Err(ApiError::validation_message(
                        "stream: Input should be a valid boolean",
                    ));
                }
            },
            _ => {
                return Err(ApiError::validation_message(
                    "stream: Input should be a valid boolean",
                ));
            }
        };
        object.insert("stream".to_owned(), Value::Bool(stream));
    }
    object.insert("model".to_owned(), Value::String(model));
    Ok(object)
}

fn python_string_repr(value: &str) -> String {
    let quote = if !value.contains('\'') {
        '\''
    } else if !value.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut output = String::with_capacity(value.len() + 2);
    output.push(quote);
    for character in value.chars() {
        match character {
            '\\' => output.push_str("\\\\"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            character if character == quote => {
                output.push('\\');
                output.push(character);
            }
            character => output.push(character),
        }
    }
    output.push(quote);
    output
}

fn python_repr(value: &Value) -> String {
    match value {
        Value::Null => "None".to_owned(),
        Value::Bool(true) => "True".to_owned(),
        Value::Bool(false) => "False".to_owned(),
        Value::Number(value) => super::native_turnstile_json_number(value),
        Value::String(value) => python_string_repr(value),
        Value::Array(items) => format!(
            "[{}]",
            items.iter().map(python_repr).collect::<Vec<_>>().join(", ")
        ),
        Value::Object(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(|(key, value)| format!("{}: {}", python_string_repr(key), python_repr(value)))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

pub(super) fn python_str(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        value => python_repr(value),
    }
}

pub(super) fn python_text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null | Value::Bool(false)) => String::new(),
        Some(Value::String(value)) => value.clone(),
        Some(Value::Bool(true)) => "True".to_owned(),
        Some(Value::Number(value)) if value.as_f64() == Some(0.0) => String::new(),
        Some(Value::Array(values)) if values.is_empty() => String::new(),
        Some(Value::Object(values)) if values.is_empty() => String::new(),
        Some(value) => python_repr(value),
    }
}

fn anthropic_message_text(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Array(items) => {
            let mut output = String::new();
            for item in items {
                match item {
                    Value::String(text) => output.push_str(text),
                    Value::Object(object)
                        if matches!(
                            object.get("type").and_then(Value::as_str),
                            Some("text" | "input_text" | "output_text")
                        ) =>
                    {
                        output.push_str(&python_text(object.get("text")));
                    }
                    _ => {}
                }
            }
            output
        }
        _ => String::new(),
    }
}

fn python_truthy(value: Option<&Value>) -> bool {
    match value {
        None | Some(Value::Null | Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        Some(Value::Number(value)) => value.as_f64().is_some_and(|value| value != 0.0),
        Some(Value::String(value)) => !value.is_empty(),
        Some(Value::Array(value)) => !value.is_empty(),
        Some(Value::Object(value)) => !value.is_empty(),
    }
}

fn python_json_dumps(value: &Value) -> String {
    match value {
        Value::Array(items) => format!(
            "[{}]",
            items
                .iter()
                .map(python_json_dumps)
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Object(items) => format!(
            "{{{}}}",
            items
                .iter()
                .map(|(key, value)| format!(
                    "{}: {}",
                    serde_json::to_string(key).expect("JSON string key"),
                    python_json_dumps(value)
                ))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        Value::Number(value) => super::native_turnstile_json_number(value),
        _ => serde_json::to_string(value).expect("JSON value"),
    }
}

fn tool_prompt(tools: Option<&Value>, system: Option<&Value>) -> String {
    static XML_RULE: &str = "Tool output adapter: when calling tools, output ONLY this XML and no prose/markdown:\n<tool_calls><tool_call><tool_name>TOOL_NAME</tool_name><parameters><PARAM><![CDATA[value]]></PARAM></parameters></tool_call></tool_calls>";
    static CLAUDE_CODE: &str = "You are Claude Code";

    let has_claude_code_system = match system {
        Some(Value::String(text)) => text.contains(CLAUDE_CODE),
        Some(Value::Array(items)) => items.iter().any(|item| {
            item.as_object().is_some_and(|object| {
                object
                    .get("text")
                    .filter(|value| python_truthy(Some(value)))
                    .map(python_str)
                    .is_some_and(|text| text.contains(CLAUDE_CODE))
            })
        }),
        _ => false,
    };
    if has_claude_code_system {
        return XML_RULE.to_owned();
    }

    let Some(Value::Array(tools)) = tools else {
        return String::new();
    };
    let mut blocks = Vec::new();
    for tool in tools {
        let Some(tool) = tool.as_object() else {
            continue;
        };
        let function = tool.get("function").and_then(Value::as_object);
        let name = tool
            .get("name")
            .filter(|value| python_truthy(Some(value)))
            .or_else(|| {
                function
                    .and_then(|function| function.get("name"))
                    .filter(|value| python_truthy(Some(value)))
            })
            .map(python_str)
            .unwrap_or_default()
            .trim()
            .to_owned();
        if name.is_empty() {
            continue;
        }
        let description = tool
            .get("description")
            .filter(|value| python_truthy(Some(value)))
            .or_else(|| {
                function
                    .and_then(|function| function.get("description"))
                    .filter(|value| python_truthy(Some(value)))
            })
            .map(python_str)
            .unwrap_or_default()
            .trim()
            .to_owned();
        let schema = [
            tool.get("input_schema"),
            tool.get("parameters"),
            function.and_then(|function| function.get("input_schema")),
            function.and_then(|function| function.get("parameters")),
        ]
        .into_iter()
        .flatten()
        .find(|value| python_truthy(Some(value)))
        .cloned()
        .unwrap_or_else(|| json!({}));
        blocks.push(format!(
            "Tool: {name}\nDescription: {description}\nParameters: {}",
            python_json_dumps(&schema)
        ));
    }
    if blocks.is_empty() {
        return String::new();
    }
    format!(
        "Available tools:\n{}\n\nTool use rules:\n- If the user asks to list/read/search files, inspect project state, run a command, or answer from local code, you MUST call a suitable tool first. Do not say you cannot access files.\n- To call tools, output ONLY XML and no prose/markdown:\n<tool_calls><tool_call><tool_name>TOOL_NAME</tool_name><parameters><PARAM><![CDATA[value]]></PARAM></parameters></tool_call></tool_calls>\n- Put parameters under <parameters> using the exact schema names.",
        blocks.join("\n")
    )
}

fn compact_system(system: Option<&Value>) -> Value {
    match system {
        Some(Value::String(text)) => json!(text),
        Some(Value::Array(items)) => Value::Array(
            items
                .iter()
                .map(|item| {
                    let Some(object) = item.as_object() else {
                        return item.clone();
                    };
                    if object.get("type").and_then(Value::as_str) != Some("text") {
                        return item.clone();
                    }
                    let mut copied = object.clone();
                    let text = copied
                        .get("text")
                        .filter(|value| python_truthy(Some(value)))
                        .map(python_str)
                        .unwrap_or_default();
                    copied.insert("text".to_owned(), Value::String(text));
                    Value::Object(copied)
                })
                .collect(),
        ),
        Some(value) => value.clone(),
        None => Value::Null,
    }
}

fn merged_system(system: Option<&Value>, tools: Option<&Value>) -> Value {
    let mut system = compact_system(system);
    let extra = tool_prompt(tools, Some(&system));
    if extra.is_empty() {
        return system;
    }
    match &mut system {
        Value::String(text) if !text.trim().is_empty() => {
            *text = format!("{}\n\n{extra}", text.trim());
        }
        Value::Array(items) => items.push(json!({"type":"text","text":extra})),
        _ => system = Value::String(extra),
    }
    system
}

fn preprocess_message_content(content: &Value) -> Value {
    let Value::Array(items) = content else {
        return content.clone();
    };
    Value::Array(
        items
            .iter()
            .map(|item| {
                let Some(block) = item.as_object() else {
                    return item.clone();
                };
                match block.get("type").and_then(Value::as_str) {
                    Some("text") => {
                        let mut copied = block.clone();
                        let text = copied
                            .get("text")
                            .filter(|value| python_truthy(Some(value)))
                            .map(python_str)
                            .unwrap_or_default();
                        copied.insert("text".to_owned(), Value::String(text));
                        Value::Object(copied)
                    }
                    Some("tool_use") => {
                        let name = block
                            .get("name")
                            .filter(|value| python_truthy(Some(value)))
                            .map(python_str)
                            .unwrap_or_default();
                        let input = block
                            .get("input")
                            .filter(|value| python_truthy(Some(value)))
                            .cloned()
                            .unwrap_or_else(|| json!({}));
                        json!({"type":"text","text":format!("<tool_calls><tool_call><tool_name>{name}</tool_name><parameters>{}</parameters></tool_call></tool_calls>", python_json_dumps(&input))})
                    }
                    Some("tool_result") => {
                        let id = block
                            .get("tool_use_id")
                            .filter(|value| python_truthy(Some(value)))
                            .map(python_str)
                            .unwrap_or_default();
                        let content = block
                            .get("content")
                            .filter(|value| python_truthy(Some(value)))
                            .map(python_str)
                            .unwrap_or_default();
                        json!({"type":"text","text":format!("Tool result {id}: {content}")})
                    }
                    _ => item.clone(),
                }
            })
            .collect(),
    )
}

fn nonempty_image_reference(value: Option<&Value>) -> Option<String> {
    let value = value?;
    value
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| {
            ["url", "image_url"].into_iter().find_map(|key| {
                value
                    .get(key)
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(ToOwned::to_owned)
            })
        })
}

fn chat_image_url(block: &Map<String, Value>) -> Option<String> {
    let block_type = block.get("type").and_then(Value::as_str).map(str::trim)?;
    if !matches!(block_type, "image" | "image_url" | "input_image") {
        return None;
    }
    if let Some(url) = ["image_url", "url"]
        .into_iter()
        .find_map(|key| nonempty_image_reference(block.get(key)))
    {
        return Some(url);
    }
    if block_type == "image_url" {
        return None;
    }
    let source = block.get("source").and_then(Value::as_object);
    let source_is_base64 = source
        .and_then(|source| source.get("type"))
        .and_then(Value::as_str)
        == Some("base64");
    let encoded = ["b64_json", "base64"]
        .into_iter()
        .filter_map(|key| block.get(key))
        .find(|value| python_truthy(Some(value)))
        .or_else(|| {
            source
                .filter(|_| block_type != "image" || source_is_base64)
                .and_then(|source| source.get("data"))
                .filter(|value| python_truthy(Some(value)))
        })
        .map(python_str)
        .filter(|value| !value.is_empty())?;
    let mime = ["media_type", "mime_type"]
        .into_iter()
        .filter_map(|key| source.and_then(|source| source.get(key)))
        .chain(
            ["media_type", "mime_type", "mimeType"]
                .into_iter()
                .filter_map(|key| block.get(key)),
        )
        .find(|value| python_truthy(Some(value)))
        .map(python_str)
        .map(|value| value.to_ascii_lowercase())
        .unwrap_or_else(|| "image/png".to_owned());
    let mime = if mime == "image/jpg" {
        "image/jpeg".to_owned()
    } else {
        mime
    };
    Some(format!("data:{mime};base64,{encoded}"))
}

fn chat_content(content: &Value, role: &str) -> Value {
    let Value::Array(items) = content else {
        return Value::String(anthropic_message_text(content));
    };
    let text = anthropic_message_text(content);
    if role != "user" {
        return Value::String(text);
    }
    let images = items
        .iter()
        .filter_map(Value::as_object)
        .filter_map(chat_image_url)
        .map(|url| json!({"type":"image_url","image_url":{"url":url}}))
        .collect::<Vec<_>>();
    if images.is_empty() {
        Value::String(text)
    } else {
        let mut parts = Vec::with_capacity(images.len() + usize::from(!text.is_empty()));
        if !text.is_empty() {
            parts.push(json!({"type":"text","text":text}));
        }
        parts.extend(images);
        Value::Array(parts)
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

pub(super) fn to_chat_payload(object: &Map<String, Value>) -> Result<Value, ApiError> {
    let mut messages = Vec::new();
    let system = merged_system(object.get("system"), object.get("tools"));
    let system_text = anthropic_message_text(&system);
    if !system_text.is_empty() {
        messages.push(json!({"role":"system","content":system_text}));
    }
    if let Some(source_messages) = object.get("messages").and_then(Value::as_array) {
        for source in source_messages {
            let Some(source) = source.as_object() else {
                continue;
            };
            let role = source.get("role").cloned().unwrap_or_else(|| json!("user"));
            let role_text = match &role {
                Value::String(value) => value.clone(),
                value => python_str(value),
            };
            let content = source
                .get("content")
                .map(preprocess_message_content)
                .unwrap_or_else(|| json!(""));
            messages.push(json!({
                "role":role,
                "content":chat_content(&content, &role_text),
            }));
        }
    }
    Ok(json!({
        "model": object.get("model").cloned().ok_or_else(ApiError::invalid_request)?,
        "messages": messages,
        "stream": object.get("stream").and_then(Value::as_bool).unwrap_or(false),
    }))
}

pub(super) fn to_responses_payload(object: &Map<String, Value>) -> Result<Value, ApiError> {
    let mut input = Vec::new();
    let chat_payload = to_chat_payload(object)?;
    for message in chat_payload["messages"]
        .as_array()
        .ok_or_else(ApiError::invalid_request)?
    {
        let message = message.as_object().ok_or_else(ApiError::invalid_request)?;
        let role = message
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(ApiError::invalid_request)?;
        if role == "system" {
            continue;
        }
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
                        Some("text") => converted.push(json!({
                            "type":"input_text",
                            "text":item.get("text").and_then(Value::as_str).unwrap_or_default(),
                        })),
                        Some("image_url") => {
                            let image_url = item
                                .get("image_url")
                                .and_then(|value| {
                                    value
                                        .as_str()
                                        .or_else(|| value.get("url").and_then(Value::as_str))
                                })
                                .ok_or_else(ApiError::invalid_request)?;
                            converted.push(json!({"type":"input_image","image_url":image_url}));
                        }
                        _ => {}
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
    let mut payload = json!({
        "model": object.get("model").cloned().ok_or_else(ApiError::invalid_request)?,
        "input": input,
        "stream": object.get("stream").and_then(Value::as_bool).unwrap_or(false),
    });
    if let Some(system) = chat_payload["messages"]
        .as_array()
        .and_then(|messages| messages.first())
        .filter(|message| message["role"] == "system")
        .and_then(|message| message.get("content"))
    {
        let instructions = anthropic_message_text(system);
        if !instructions.is_empty() {
            payload["instructions"] = Value::String(instructions);
        }
    }
    Ok(payload)
}

pub(super) fn from_responses_response(
    body: &[u8],
    model: &str,
    tools_enabled: bool,
    input_tokens: usize,
) -> Result<Value, ApiError> {
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
        .filter(|item| item["type"] == "text")
        .filter_map(|item| item["text"].as_str())
        .collect::<String>();
    if tools_enabled {
        let (content, stop_reason) = content_blocks(&text, true);
        return Ok(json!({
            "id":format!("msg_{}",native_message_id()),
            "type":"message",
            "role":"assistant",
            "model":model,
            "content":content,
            "stop_reason":stop_reason,
            "stop_sequence":null,
            "usage":{
                "input_tokens":input_tokens,
                "output_tokens":model_token_count(model, &text),
            }
        }));
    }
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
        for block in search_result_blocks_from_annotations(&annotations, &text)? {
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
    usage["input_tokens"] = json!(input_tokens);
    usage["output_tokens"] = json!(model_token_count(model, &text));
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

pub(super) fn has_tools(object: &Map<String, Value>) -> bool {
    object
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|tools| !tools.is_empty())
}

fn model_token_count(model: &str, text: &str) -> usize {
    let bpe =
        tiktoken_rs::bpe_for_model(model).unwrap_or_else(|_| tiktoken_rs::o200k_base_singleton());
    bpe.count(text, &std::collections::HashSet::new())
        .unwrap_or_default()
}

pub(super) fn input_token_count(
    object: &Map<String, Value>,
    global_system_prompt: Option<&str>,
) -> Result<usize, ApiError> {
    let payload = to_chat_payload(object)?;
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("auto");
    let messages = payload
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(ApiError::invalid_request)?;
    let mut total = 0usize;
    let mut image_tokens = 0usize;
    if let Some(prompt) = global_system_prompt.filter(|prompt| !prompt.is_empty()) {
        total = total.saturating_add(3 + model_token_count(model, "system"));
        total = total.saturating_add(model_token_count(model, prompt));
    }
    for message in messages {
        let Some(message) = message.as_object() else {
            continue;
        };
        total = total.saturating_add(3);
        if let Some(role) = message.get("role").and_then(Value::as_str) {
            total = total.saturating_add(model_token_count(model, role));
        }
        if let Some(content) = message.get("content") {
            let text = match content {
                Value::String(text) => text.clone(),
                Value::Array(parts) => {
                    if message.get("role").and_then(Value::as_str) == Some("user") {
                        for part in parts {
                            let Some(part) = part.as_object() else {
                                continue;
                            };
                            let Some(image_url) = chat_image_url(part) else {
                                continue;
                            };
                            if let Ok((_, _, width, height)) = super::native_image_input(&image_url)
                            {
                                image_tokens = image_tokens.saturating_add(
                                    usize::try_from(super::native_image_patch_tokens(
                                        width, height, "auto",
                                    ))
                                    .unwrap_or(usize::MAX),
                                );
                            }
                        }
                    }
                    anthropic_message_text(content)
                }
                value if python_truthy(Some(value)) => python_str(value),
                _ => String::new(),
            };
            total = total.saturating_add(model_token_count(model, &text));
        }
        if let Some(name) = message.get("name").and_then(Value::as_str) {
            total = total.saturating_add(model_token_count(model, name) + 1);
        }
    }
    Ok(total.saturating_add(image_tokens).saturating_add(3))
}

fn decode_html_entities(value: &str) -> String {
    html_escape::decode_html_entities(value).into_owned()
}

fn xml_value(text: &str, tag: &str) -> Option<String> {
    let pattern = Regex::new(&format!(
        r"(?is)<{}\b[^>]*>(.*?)</{}>",
        regex::escape(tag),
        regex::escape(tag)
    ))
    .ok()?;
    let value = pattern.captures(text)?.get(1)?.as_str().trim();
    let value = if value.starts_with("<![CDATA[") && value.ends_with("]]>") {
        &value[9..value.len() - 3]
    } else {
        value
    };
    Some(decode_html_entities(value).trim().to_owned())
}

fn parse_tool_value(raw: &str) -> Value {
    let wrapped = format!("<x>{raw}</x>");
    let value = xml_value(&wrapped, "x").unwrap_or_default();
    serde_json::from_str(&value).unwrap_or_else(|_| Value::String(value))
}

fn parse_tool_params(raw: &str) -> Value {
    if let Ok(Value::Object(value)) = serde_json::from_str::<Value>(raw.trim()) {
        return Value::Object(value);
    }
    static PARAMS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?is)<([\w.-]+)\b[^>]*>(.*?)</([\w.-]+)>").expect("tool parameter regex")
    });
    let mut value = Map::new();
    for captures in PARAMS.captures_iter(raw) {
        let Some(name) = captures.get(1).map(|value| value.as_str()) else {
            continue;
        };
        if !captures
            .get(3)
            .is_some_and(|closing| closing.as_str().eq_ignore_ascii_case(name))
        {
            continue;
        }
        let Some(raw_value) = captures.get(2).map(|value| value.as_str()) else {
            continue;
        };
        value.insert(name.to_owned(), parse_tool_value(raw_value));
    }
    Value::Object(value)
}

fn parse_tool_calls(text: &str) -> Vec<(String, Value)> {
    static CODE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?is)```.*?```").expect("code fence regex"));
    static CALLS: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(
            r"(?is)<tool_call\b[^>]*>(.*?)</tool_call>|<function_call\b[^>]*>(.*?)</function_call>|<invoke\b[^>]*>(.*?)</invoke>",
        )
        .expect("tool call regex")
    });
    let text = CODE.replace_all(text, "");
    let mut calls = Vec::new();
    for captures in CALLS.captures_iter(text.trim()) {
        let block = (1..=3)
            .find_map(|index| captures.get(index).map(|value| value.as_str()))
            .unwrap_or_default();
        let name = ["tool_name", "name", "function"]
            .into_iter()
            .find_map(|tag| xml_value(block, tag))
            .filter(|name| !name.is_empty());
        let Some(name) = name else {
            continue;
        };
        let params = ["parameters", "input", "arguments"]
            .into_iter()
            .find_map(|tag| xml_value(block, tag))
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| "{}".to_owned());
        calls.push((name, parse_tool_params(&params)));
    }
    calls
}

fn strip_tool_markup(text: &str) -> String {
    static MARKUP: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?is)<tool_calls\b[^>]*>.*?</tool_calls>|<tool_call\b[^>]*>.*?</tool_call>|<function_call\b[^>]*>.*?</function_call>|<invoke\b[^>]*>.*?</invoke>")
            .expect("tool markup regex")
    });
    MARKUP.replace_all(text, "").trim().to_owned()
}

fn content_blocks(text: &str, tools_enabled: bool) -> (Vec<Value>, &'static str) {
    let calls = if tools_enabled {
        parse_tool_calls(text)
    } else {
        Vec::new()
    };
    let text = strip_tool_markup(text);
    let mut content = Vec::new();
    if calls.is_empty() || !text.is_empty() {
        content.push(json!({"type":"text","text":text}));
    }
    for (name, input) in calls {
        content.push(json!({
            "type":"tool_use",
            "id":format!("toolu_{}",native_message_id()),
            "name":name,
            "input":input,
        }));
    }
    let stop_reason = if content.iter().any(|item| item["type"] == "tool_use") {
        "tool_use"
    } else {
        "end_turn"
    };
    (content, stop_reason)
}

pub(super) fn from_chat_response(
    body: &[u8],
    model: &str,
    tools_enabled: bool,
    input_tokens: usize,
) -> Result<Value, ApiError> {
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
    let text = message
        .get("content")
        .filter(|value| python_truthy(Some(value)))
        .map(python_str)
        .unwrap_or_default();
    let (content, stop_reason) = content_blocks(&text, tools_enabled);
    Ok(
        json!({"id":format!("msg_{}",native_message_id()),"type":"message","role":"assistant","model":model,"content":content,"stop_reason":stop_reason,"stop_sequence":Value::Null,"usage":{"input_tokens":input_tokens,"output_tokens":model_token_count(model, &text)}}),
    )
}

pub(super) fn stream_response(
    response: reqwest::Response,
    model: String,
    deadline: Instant,
) -> Response {
    stream_response_with_options(response, model, deadline, false, 0)
}

pub(super) fn stream_response_with_options(
    response: reqwest::Response,
    model: String,
    deadline: Instant,
    tools_enabled: bool,
    input_tokens: usize,
) -> Response {
    stream_body_response_with_options(
        Body::from_stream(
            response
                .bytes_stream()
                .map(|result| result.map_err(|error| io::Error::other(error.to_string()))),
        ),
        model,
        deadline,
        tools_enabled,
        input_tokens,
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

struct XmlToolStreamState {
    input: Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>,
    buffer: Vec<u8>,
    done: bool,
    started: bool,
    terminal_seen: bool,
    model: String,
    tools_enabled: bool,
    input_tokens: usize,
    created: i64,
    text_open: bool,
    streamed_text: String,
    current_text: String,
    tool_started: bool,
}

fn xml_tool_stream_event(output: &mut Vec<u8>, event: &str, value: Value) {
    output.extend_from_slice(format!("event: {event}\ndata: {value}\n\n").as_bytes());
}

fn append_xml_tool_message_start(output: &mut Vec<u8>, model: &str, input_tokens: usize) {
    xml_tool_stream_event(
        output,
        "message_start",
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
                "usage":{"input_tokens":input_tokens,"output_tokens":0}
            }
        }),
    );
}

fn append_xml_tool_text_start(output: &mut Vec<u8>) {
    xml_tool_stream_event(
        output,
        "content_block_start",
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
    );
}

fn streamable_tool_text(text: &str) -> &str {
    static MARKER: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"(?is)<tool_calls\b|<tool_call\b|<function_call\b|<invoke\b")
            .expect("stream tool marker regex")
    });
    MARKER
        .find(text)
        .map_or(text, |matched| &text[..matched.start()])
        .trim_end()
}

fn xml_stream_error(
    state: XmlToolStreamState,
    message: &'static str,
) -> (Result<Bytes, io::Error>, XmlToolStreamState) {
    let mut output = Vec::new();
    xml_tool_stream_event(
        &mut output,
        "error",
        json!({
            "type": "error",
            "error": {"type": "RuntimeError", "message": message},
        }),
    );
    (
        Ok(Bytes::from(output)),
        XmlToolStreamState {
            done: true,
            ..state
        },
    )
}

pub(super) fn stream_body_response(body: Body, model: String, deadline: Instant) -> Response {
    stream_body_response_with_options(body, model, deadline, false, 0)
}

pub(super) fn stream_body_response_with_options(
    body: Body,
    model: String,
    deadline: Instant,
    tools_enabled: bool,
    input_tokens: usize,
) -> Response {
    type Input = Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>;
    let input: Input = Box::pin(
        body.into_data_stream()
            .map(|result| result.map_err(|error| io::Error::other(error.to_string()))),
    );
    let state = XmlToolStreamState {
        input,
        buffer: Vec::new(),
        done: false,
        started: false,
        terminal_seen: false,
        model,
        tools_enabled,
        input_tokens,
        created: super::native_created(),
        text_open: false,
        streamed_text: String::new(),
        current_text: String::new(),
        tool_started: false,
    };
    let stream = stream::unfold(state, move |mut state| async move {
        if state.done {
            return None;
        }
        if !state.started {
            let mut output = Vec::new();
            append_xml_tool_message_start(&mut output, &state.model, state.input_tokens);
            state.started = true;
            if !state.tools_enabled {
                append_xml_tool_text_start(&mut output);
                state.text_open = true;
            }
            return Some((Ok(Bytes::from(output)), state));
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Some(xml_stream_error(state, "Anthropic stream timed out"));
        }
        let chunk = match tokio::time::timeout(remaining, state.input.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(Some(Err(_))) | Err(_) => {
                return Some(xml_stream_error(state, "Anthropic stream failed"));
            }
            Ok(None) if state.terminal_seen => return None,
            Ok(None) => {
                state.buffer.extend_from_slice(
                    br#"data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n"#,
                );
                Bytes::new()
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
                xml_tool_stream_event(
                    &mut output,
                    "message_stop",
                    json!({"type":"message_stop","created":state.created}),
                );
                state.done = true;
                break;
            }
            let value: Value = match serde_json::from_slice(data) {
                Ok(value) => value,
                Err(_) => {
                    return Some(xml_stream_error(
                        state,
                        "Anthropic stream contained malformed JSON",
                    ));
                }
            };
            let delta = value.pointer("/choices/0/delta");
            if let Some(text) = delta
                .and_then(|delta| delta.get("content"))
                .and_then(Value::as_str)
                .filter(|text| !text.is_empty())
            {
                state.current_text.push_str(text);
                if !state.tool_started {
                    let visible = if state.tools_enabled {
                        streamable_tool_text(&state.current_text)
                    } else {
                        &state.current_text
                    };
                    if visible.starts_with(&state.streamed_text) {
                        let next = &visible[state.streamed_text.len()..];
                        if !next.is_empty() {
                            if !state.text_open {
                                append_xml_tool_text_start(&mut output);
                                state.text_open = true;
                            }
                            xml_tool_stream_event(
                                &mut output,
                                "content_block_delta",
                                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":next}}),
                            );
                            state.streamed_text = visible.to_owned();
                        }
                    }
                    state.tool_started = state.tools_enabled && visible != state.current_text;
                }
            }
            if value
                .pointer("/choices/0/finish_reason")
                .is_some_and(|value| python_truthy(Some(value)))
            {
                let (content, stop_reason) =
                    content_blocks(&state.current_text, state.tools_enabled);
                let had_text_block = state.text_open;
                if had_text_block {
                    xml_tool_stream_event(
                        &mut output,
                        "content_block_stop",
                        json!({"type":"content_block_stop","index":0}),
                    );
                }
                if stop_reason == "tool_use" {
                    let mut blocks = content;
                    let mut start_index = usize::from(had_text_block);
                    if blocks.first().is_some_and(|item| item["type"] == "text") {
                        let text = blocks[0]["text"].as_str().unwrap_or_default();
                        let remaining = text.strip_prefix(&state.streamed_text).unwrap_or(text);
                        if !remaining.is_empty() {
                            if !had_text_block {
                                append_xml_tool_text_start(&mut output);
                            }
                            xml_tool_stream_event(
                                &mut output,
                                "content_block_delta",
                                json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":remaining}}),
                            );
                            if !had_text_block {
                                xml_tool_stream_event(
                                    &mut output,
                                    "content_block_stop",
                                    json!({"type":"content_block_stop","index":0}),
                                );
                            }
                        }
                        start_index = 1;
                        blocks.remove(0);
                    }
                    for (offset, block) in blocks.iter().enumerate() {
                        let index = start_index + offset;
                        xml_tool_stream_event(
                            &mut output,
                            "content_block_start",
                            json!({
                                "type":"content_block_start",
                                "index":index,
                                "content_block":{
                                    "type":"tool_use",
                                    "id":block["id"],
                                    "name":block["name"],
                                    "input":{}
                                }
                            }),
                        );
                        xml_tool_stream_event(
                            &mut output,
                            "content_block_delta",
                            json!({
                                "type":"content_block_delta",
                                "index":index,
                                "delta":{
                                    "type":"input_json_delta",
                                    "partial_json":python_json_dumps(&block["input"])
                                }
                            }),
                        );
                        xml_tool_stream_event(
                            &mut output,
                            "content_block_stop",
                            json!({"type":"content_block_stop","index":index}),
                        );
                    }
                }
                xml_tool_stream_event(
                    &mut output,
                    "message_delta",
                    json!({
                        "type":"message_delta",
                        "delta":{"stop_reason":stop_reason,"stop_sequence":null},
                        "usage":{"output_tokens":model_token_count(&state.model, &state.current_text)}
                    }),
                );
                xml_tool_stream_event(
                    &mut output,
                    "message_stop",
                    json!({"type":"message_stop","created":state.created}),
                );
                state.terminal_seen = true;
                state.done = true;
                break;
            }
        }
        if output.is_empty() && state.done {
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

struct ResponsesXmlToolStreamState {
    input: Pin<Box<dyn Stream<Item = Result<Bytes, io::Error>> + Send>>,
    buffer: Vec<u8>,
    done: bool,
    text_seen: bool,
}

fn chat_stream_chunk(text: Option<&str>, finish: bool) -> String {
    let delta = text.map_or_else(|| json!({}), |text| json!({"content":text}));
    let finish_reason = finish.then_some("stop");
    format!(
        "data: {}\n\n",
        json!({"choices":[{"delta":delta,"finish_reason":finish_reason}]})
    )
}

fn responses_final_text(response: &Value) -> String {
    response
        .get("output")
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
        .filter(|item| item.get("type").and_then(Value::as_str) == Some("output_text"))
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect()
}

fn responses_text_as_chat_stream(body: Body, deadline: Instant) -> Body {
    let input = Box::pin(
        body.into_data_stream()
            .map(|result| result.map_err(|error| io::Error::other(error.to_string()))),
    );
    let state = ResponsesXmlToolStreamState {
        input,
        buffer: Vec::new(),
        done: false,
        text_seen: false,
    };
    let stream = stream::unfold(state, move |mut state| async move {
        if state.done {
            return None;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            state.done = true;
            return Some((Err(io::Error::other("Responses stream timed out")), state));
        }
        let chunk = match tokio::time::timeout(remaining, state.input.next()).await {
            Ok(Some(Ok(chunk))) => chunk,
            Ok(Some(Err(_))) | Err(_) => {
                state.done = true;
                return Some((Err(io::Error::other("Responses stream failed")), state));
            }
            Ok(None) => {
                state.done = true;
                return Some((Err(io::Error::other("Responses stream ended early")), state));
            }
        };
        state.buffer.extend_from_slice(&chunk);
        let mut output = String::new();
        while let Some((position, delimiter)) = sse_delimiter(&state.buffer) {
            let event = state.buffer.drain(..position).collect::<Vec<_>>();
            state.buffer.drain(..delimiter);
            let Some(data) = event.strip_prefix(b"data: ") else {
                continue;
            };
            if data == b"[DONE]" {
                if state.done {
                    break;
                }
                state.done = true;
                return Some((Err(io::Error::other("Responses stream ended early")), state));
            }
            let value: Value = match serde_json::from_slice(data) {
                Ok(value) => value,
                Err(_) => {
                    state.done = true;
                    return Some((
                        Err(io::Error::other("Responses stream JSON is invalid")),
                        state,
                    ));
                }
            };
            match value.get("type").and_then(Value::as_str) {
                Some("response.output_text.delta") => {
                    if let Some(delta) = value.get("delta").and_then(Value::as_str)
                        && !delta.is_empty()
                    {
                        output.push_str(&chat_stream_chunk(Some(delta), false));
                        state.text_seen = true;
                    }
                }
                Some("response.completed") => {
                    let response = value.get("response").unwrap_or(&value);
                    if !state.text_seen {
                        let text = responses_final_text(response);
                        if !text.is_empty() {
                            output.push_str(&chat_stream_chunk(Some(&text), false));
                        }
                    }
                    output.push_str(&chat_stream_chunk(None, true));
                    output.push_str("data: [DONE]\n\n");
                    state.done = true;
                    break;
                }
                Some("response.failed" | "response.incomplete") => {
                    state.done = true;
                    return Some((Err(io::Error::other("Responses stream failed")), state));
                }
                _ => {}
            }
        }
        if output.is_empty() && state.done {
            return None;
        }
        Some((Ok(Bytes::from(output)), state))
    });
    Body::from_stream(stream)
}

pub(super) fn stream_responses_xml_tools_body_response(
    body: Body,
    model: String,
    deadline: Instant,
    input_tokens: usize,
) -> Response {
    stream_body_response_with_options(
        responses_text_as_chat_stream(body, deadline),
        model,
        deadline,
        true,
        input_tokens,
    )
}

pub(super) fn stream_responses_xml_tools_response(
    response: reqwest::Response,
    model: String,
    deadline: Instant,
    input_tokens: usize,
) -> Response {
    stream_responses_xml_tools_body_response(
        Body::from_stream(
            response
                .bytes_stream()
                .map(|result| result.map_err(|error| io::Error::other(error.to_string()))),
        ),
        model,
        deadline,
        input_tokens,
    )
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

#[allow(dead_code)]
fn stream_body_response_legacy(body: Body, model: String, deadline: Instant) -> Response {
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
