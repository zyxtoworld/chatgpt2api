use std::io;

use serde_json::{Value, json};

use super::{ApiError, sse_delimiter};

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
