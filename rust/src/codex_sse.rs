use std::io;

use serde_json::{Value, json};

use super::{ApiError, native_completion_id, native_message_id, sse_delimiter};

pub(super) fn native_codex_responses_json(
    body: &[u8],
    requested_model: &str,
) -> Result<Value, ApiError> {
    let mut buffer = body.to_vec();
    let mut text = String::new();
    let mut response_id = None;
    let mut completed_response = None;
    let mut terminal = false;
    while let Some((position, delimiter_length)) = sse_delimiter(&buffer) {
        let event = buffer.drain(..position).collect::<Vec<_>>();
        buffer.drain(..delimiter_length);
        let Some(data) = codex_sse_data(&event).map_err(|_| ApiError::upstream())? else {
            continue;
        };
        if data == "[DONE]" {
            continue;
        }
        let value: Value = serde_json::from_str(&data).map_err(|_| ApiError::upstream())?;
        match value.get("type").and_then(Value::as_str) {
            Some("response.created") | Some("response.in_progress") => {
                let response = value
                    .get("response")
                    .and_then(Value::as_object)
                    .ok_or_else(ApiError::upstream)?;
                if let Some(id) = response.get("id").and_then(Value::as_str) {
                    response_id = Some(id.to_owned());
                }
            }
            Some("response.output_text.delta") => {
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::upstream)?;
                text.push_str(delta);
            }
            Some("response.completed") => {
                terminal = true;
                if let Some(response) = value.get("response") {
                    if !response.is_object() {
                        return Err(ApiError::upstream());
                    }
                    completed_response = Some(response.clone());
                }
                break;
            }
            Some("response.failed") | Some("response.incomplete") => {
                return Err(ApiError::upstream());
            }
            _ => {}
        }
    }
    if !terminal || !buffer.iter().all(u8::is_ascii_whitespace) {
        return Err(ApiError::upstream());
    }

    let mut response = completed_response
        .and_then(|value| value.as_object().cloned())
        .unwrap_or_default();
    let response_id = response
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or(response_id)
        .unwrap_or_else(native_completion_id);
    response.insert("id".to_owned(), Value::String(response_id));
    response.insert("object".to_owned(), json!("response"));
    response.insert("status".to_owned(), json!("completed"));
    if response.get("model").and_then(Value::as_str).is_none() {
        response.insert(
            "model".to_owned(),
            Value::String(requested_model.to_owned()),
        );
    }
    if !response.get("output").is_some_and(Value::is_array) {
        response.insert(
            "output".to_owned(),
            json!([{
                "type": "message",
                "id": format!("msg_{}", native_message_id()),
                "status": "completed",
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": text,
                    "annotations": []
                }]
            }]),
        );
    }
    Ok(Value::Object(response))
}

pub(super) fn native_codex_response_to_chat(
    response: &Value,
    requested_model: &str,
) -> Result<Value, ApiError> {
    let output = response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(ApiError::upstream)?;
    let mut text = String::new();
    let mut tool_calls = Vec::new();
    for item in output {
        match item.get("type").and_then(Value::as_str) {
            Some("message") => {
                if let Some(content) = item.get("content").and_then(Value::as_array) {
                    for block in content {
                        if block.get("type").and_then(Value::as_str) == Some("output_text") {
                            text.push_str(
                                block
                                    .get("text")
                                    .and_then(Value::as_str)
                                    .ok_or_else(ApiError::upstream)?,
                            );
                        }
                    }
                }
            }
            Some("function_call") => {
                let id = item
                    .get("call_id")
                    .or_else(|| item.get("id"))
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(ApiError::upstream)?;
                let name = item
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .ok_or_else(ApiError::upstream)?;
                let arguments = item
                    .get("arguments")
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::upstream)?;
                let parsed: Value =
                    serde_json::from_str(arguments).map_err(|_| ApiError::upstream())?;
                if !parsed.is_object() {
                    return Err(ApiError::upstream());
                }
                tool_calls.push(json!({
                    "id": id,
                    "type": "function",
                    "function": {"name": name, "arguments": arguments}
                }));
            }
            Some(_) | None => {}
        }
    }
    let finish_reason = if tool_calls.is_empty() {
        "stop"
    } else {
        "tool_calls"
    };
    let message = json!({
        "role": "assistant",
        "content": if text.is_empty() { Value::Null } else { Value::String(text) },
        "tool_calls": if tool_calls.is_empty() { Value::Null } else { Value::Array(tool_calls) },
    });
    Ok(json!({
        "id": response.get("id").cloned().unwrap_or_else(|| json!(native_completion_id())),
        "object": "chat.completion",
        "created": std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |duration| i64::try_from(duration.as_secs()).unwrap_or(i64::MAX)),
        "model": response.get("model").and_then(Value::as_str).unwrap_or(requested_model),
        "choices": [{"index":0,"message":message,"finish_reason":finish_reason}],
        "usage": response.get("usage").cloned().unwrap_or_else(|| json!({"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}))
    }))
}

pub(super) fn codex_sse_data(event: &[u8]) -> Result<Option<String>, io::Error> {
    let text = std::str::from_utf8(event).map_err(|_| io::Error::other("malformed Codex event"))?;
    let data = text
        .lines()
        .filter_map(|line| line.strip_prefix("data:"))
        .map(str::trim_start)
        .collect::<Vec<_>>()
        .join("\n");
    if data.is_empty() {
        return Ok(None);
    }
    Ok(Some(data))
}

#[cfg(test)]
pub(super) fn native_codex_text(body: &[u8]) -> Result<String, ApiError> {
    let mut buffer = body.to_vec();
    let mut text = String::new();
    let mut terminal = false;
    while let Some((position, delimiter_length)) = sse_delimiter(&buffer) {
        let event = buffer.drain(..position).collect::<Vec<_>>();
        buffer.drain(..delimiter_length);
        let Some(data) = codex_sse_data(&event).map_err(|_| ApiError::upstream())? else {
            continue;
        };
        if data == "[DONE]" {
            continue;
        }
        let value: Value = serde_json::from_str(&data).map_err(|_| ApiError::upstream())?;
        match value.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                let delta = value
                    .get("delta")
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::upstream)?;
                text.push_str(delta);
            }
            Some("response.completed") => {
                terminal = true;
                break;
            }
            Some("response.failed") | Some("response.incomplete") => {
                return Err(ApiError::upstream());
            }
            _ => {}
        }
    }
    if terminal && buffer.iter().all(u8::is_ascii_whitespace) {
        Ok(text)
    } else {
        Err(ApiError::upstream())
    }
}

pub(super) fn native_codex_delta_frame(
    completion_id: &str,
    model: &str,
    created: i64,
    delta: &str,
    include_usage: bool,
    role: bool,
) -> Vec<u8> {
    let mut value = json!({
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [{"index":0,"delta":{"content":delta},"finish_reason":null}],
    });
    if role {
        value["choices"][0]["delta"]["role"] = json!("assistant");
    }
    if include_usage {
        value["usage"] = Value::Null;
    }
    let mut output = b"data: ".to_vec();
    output.extend(serde_json::to_vec(&value).expect("static Codex chat frame"));
    output.extend_from_slice(b"\n\n");
    output
}
