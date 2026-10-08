use super::protocol_codex_payload::native_codex_tool;
use super::{ApiError, Map, Value, native_message_id, sse_delimiter};
use base64::Engine;
use serde_json::json;
use std::io::{self, Cursor};
use std::sync::atomic::{AtomicUsize, Ordering};

pub(crate) fn native_message(message: &Value) -> Result<Value, ApiError> {
    let object = message.as_object().ok_or_else(ApiError::invalid_request)?;
    let role = object
        .get("role")
        .cloned()
        .unwrap_or_else(|| Value::String("user".to_owned()));
    let content = match object.get("content") {
        Some(Value::String(text)) => text.clone(),
        Some(Value::Array(parts)) => {
            let mut text = String::new();
            for part in parts {
                match part {
                    Value::String(value) => text.push_str(value),
                    Value::Object(part)
                        if matches!(
                            part.get("type").and_then(Value::as_str),
                            Some("text" | "input_text" | "output_text")
                        ) =>
                    {
                        if let Some(value) = part
                            .get("text")
                            .filter(|value| super::account_pool::account_value_truthy(Some(value)))
                        {
                            text.push_str(&super::protocol_anthropic::python_text(Some(value)));
                        }
                    }
                    _ => {}
                }
            }
            text
        }
        _ => String::new(),
    };
    Ok(json!({
        "id": native_message_id(),
        "author": {"role": role},
        "content": {"content_type": "text", "parts": [content]},
    }))
}

pub(crate) fn native_conversation_payload(object: &Map<String, Value>) -> Result<Value, ApiError> {
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
    let thinking_effort = python_thinking_effort(object);
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
    if !thinking_effort.is_empty() {
        payload["thinking_effort"] = Value::String(thinking_effort.clone());
    }
    Ok(payload)
}
pub(crate) fn python_thinking_effort(object: &Map<String, Value>) -> String {
    let value = if object.contains_key("thinking_effort") {
        object.get("thinking_effort")
    } else if object.contains_key("reasoning_effort") {
        object.get("reasoning_effort")
    } else {
        object
            .get("reasoning")
            .and_then(Value::as_object)
            .and_then(|reasoning| reasoning.get("effort"))
    };
    let text = value
        .filter(|value| super::account_pool::account_value_truthy(Some(value)))
        .map(|value| super::protocol_anthropic::python_text(Some(value)))
        .unwrap_or_default()
        .trim()
        .to_lowercase();
    match text.as_str() {
        "" | "none" => String::new(),
        "low" | "medium" | "high" => text,
        "xhigh" | "extended" => "extended".to_owned(),
        _ => String::new(),
    }
}
pub(crate) fn native_message_text(message: &Value) -> Result<String, ApiError> {
    let object = message.as_object().ok_or_else(ApiError::invalid_request)?;
    let content = object
        .get("content")
        .ok_or_else(ApiError::invalid_request)?;
    match content {
        Value::Null => Ok(String::new()),
        Value::String(text) => Ok(text.clone()),
        Value::Array(parts) => {
            let mut text = String::new();
            for part in parts {
                match part {
                    Value::String(value) => text.push_str(value),
                    Value::Object(part)
                        if matches!(
                            part.get("type").and_then(Value::as_str),
                            Some("text" | "input_text" | "output_text")
                        ) =>
                    {
                        if let Some(value) = part.get("text") {
                            text.push_str(&super::protocol_anthropic::python_text(Some(value)));
                        }
                    }
                    _ => {}
                }
            }
            Ok(text)
        }
        _ => Ok(String::new()),
    }
}

pub(crate) fn native_assistant_history(object: &Map<String, Value>) -> String {
    object
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
        .filter_map(|message| native_message_text(message).ok())
        .collect()
}
pub(crate) fn native_assistant_history_messages(object: &Map<String, Value>) -> Vec<String> {
    object
        .get("messages")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter(|message| message.get("role").and_then(Value::as_str) == Some("assistant"))
        .filter_map(|message| native_message_text(message).ok())
        .filter(|text| !text.is_empty())
        .collect()
}

fn strip_history(text: &str, history_text: &str) -> String {
    let mut text = text.to_owned();
    while !history_text.is_empty() && text.starts_with(history_text) {
        text.drain(..history_text.len());
    }
    text
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
    let mut text_prompt_tokens = 0usize;
    let mut image_tokens = 0usize;
    if let Some(Value::Array(messages)) = object.get("messages")
        && !messages.is_empty()
    {
        for message in messages {
            let message_object = message.as_object().ok_or_else(ApiError::invalid_request)?;
            text_prompt_tokens = text_prompt_tokens
                .checked_add(3)
                .ok_or_else(ApiError::upstream)?;
            if let Some(role) = message_object.get("role").and_then(Value::as_str) {
                text_prompt_tokens = text_prompt_tokens
                    .checked_add(native_token_count(bpe, role)?)
                    .ok_or_else(ApiError::upstream)?;
            }
            text_prompt_tokens = text_prompt_tokens
                .checked_add(native_token_count(bpe, &native_message_text(message)?)?)
                .ok_or_else(ApiError::upstream)?;
            for (key, value) in message_object {
                if matches!(key.as_str(), "role" | "content") {
                    continue;
                }
                if let Some(text) = value.as_str() {
                    text_prompt_tokens = text_prompt_tokens
                        .checked_add(native_token_count(bpe, text)?)
                        .ok_or_else(ApiError::upstream)?;
                    if key == "name" {
                        text_prompt_tokens = text_prompt_tokens
                            .checked_add(1)
                            .ok_or_else(ApiError::upstream)?;
                    }
                }
            }
            image_tokens = image_tokens
                .checked_add(native_message_image_tokens(message))
                .ok_or_else(ApiError::upstream)?;
        }
    } else if let Some(prompt) = object.get("prompt").and_then(Value::as_str) {
        text_prompt_tokens = 3usize
            .checked_add(native_token_count(bpe, "user")?)
            .and_then(|value| value.checked_add(native_token_count(bpe, prompt.trim()).ok()?))
            .ok_or_else(ApiError::upstream)?;
    } else {
        return Err(ApiError::invalid_request());
    }
    text_prompt_tokens = text_prompt_tokens
        .checked_add(3)
        .ok_or_else(ApiError::upstream)?;
    native_usage_for_prompt_tokens_with_images(model, text_prompt_tokens, image_tokens, output)
}

pub(crate) fn native_usage_for_prompt_tokens(
    model: &str,
    prompt_tokens: usize,
    output: &str,
) -> Result<Value, ApiError> {
    native_usage_for_prompt_tokens_with_images(model, prompt_tokens, 0, output)
}

pub(crate) fn native_usage_for_prompt_tokens_with_images(
    model: &str,
    text_prompt_tokens: usize,
    image_tokens: usize,
    output: &str,
) -> Result<Value, ApiError> {
    let bpe =
        tiktoken_rs::bpe_for_model(model).unwrap_or_else(|_| tiktoken_rs::o200k_base_singleton());
    let completion_tokens = native_token_count(bpe, output)?;
    let prompt_tokens = text_prompt_tokens
        .checked_add(image_tokens)
        .ok_or_else(ApiError::upstream)?;
    let total_tokens = prompt_tokens
        .checked_add(completion_tokens)
        .ok_or_else(ApiError::upstream)?;
    Ok(json!({
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "prompt_tokens_details": {
            "text_tokens": text_prompt_tokens,
            "image_tokens": image_tokens,
            "cached_tokens": 0,
        },
        "completion_tokens_details": {
            "text_tokens": completion_tokens,
            "image_tokens": 0,
            "reasoning_tokens": 0,
        },
    }))
}

fn native_message_image_tokens(message: &Value) -> usize {
    message
        .get("content")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .filter_map(|part| {
            let part_type = part
                .get("type")
                .and_then(Value::as_str)
                .map(str::trim)
                .unwrap_or_default();
            let has_source = part
                .get("source")
                .is_some_and(|value| super::account_pool::account_value_truthy(Some(value)));
            if !matches!(part_type, "image" | "image_url" | "input_image") && !has_source {
                return None;
            }
            let (width, height) = native_image_dimensions(part)?;
            Some(native_image_input_tokens(width, height, "auto"))
        })
        .sum()
}

fn native_image_dimensions(part: &Map<String, Value>) -> Option<(u32, u32)> {
    fn dimension(value: Option<&Value>) -> Option<u32> {
        match value? {
            Value::Number(number) => number
                .as_u64()
                .or_else(|| number.as_f64().map(|value| value.trunc() as u64))
                .and_then(|value| u32::try_from(value).ok())
                .filter(|value| *value > 0),
            Value::String(value) => value.trim().parse::<u32>().ok().filter(|value| *value > 0),
            _ => None,
        }
    }
    if let (Some(width), Some(height)) =
        (dimension(part.get("width")), dimension(part.get("height")))
    {
        return Some((width, height));
    }
    let image_url = part.get("image_url");
    let image_url = image_url.and_then(Value::as_str).or_else(|| {
        image_url
            .and_then(Value::as_object)
            .and_then(|image| {
                image
                    .get("url")
                    .filter(|value| super::account_pool::account_value_truthy(Some(value)))
                    .or_else(|| {
                        image
                            .get("image_url")
                            .filter(|value| super::account_pool::account_value_truthy(Some(value)))
                    })
            })
            .and_then(Value::as_str)
    });
    if let Some(image_url) = image_url.filter(|value| value.starts_with("data:")) {
        let payload = image_url.split_once(',')?.1;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(payload)
            .ok()?;
        return image::ImageReader::new(Cursor::new(bytes))
            .with_guessed_format()
            .ok()?
            .into_dimensions()
            .ok();
    }
    let source = part.get("source")?.as_object()?;
    if source.get("type").and_then(Value::as_str) != Some("base64") {
        return None;
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(source.get("data")?.as_str()?)
        .ok()?;
    image::ImageReader::new(Cursor::new(bytes))
        .with_guessed_format()
        .ok()?
        .into_dimensions()
        .ok()
}

fn native_image_input_tokens(width: u32, height: u32, detail: &str) -> usize {
    const PATCH_SIZE: f64 = 32.0;
    const PATCH_BUDGET: f64 = 1536.0;
    const MAX_DIMENSION: f64 = 2048.0;
    const PATCH_MULTIPLIER: f64 = 1.62;
    if width == 0 || height == 0 {
        return 0;
    }
    if detail.trim().eq_ignore_ascii_case("low") {
        return (256.0 * PATCH_MULTIPLIER).ceil() as usize;
    }
    let mut resized_width = f64::from(width);
    let mut resized_height = f64::from(height);
    let scale = (MAX_DIMENSION / resized_width.max(resized_height)).min(1.0);
    resized_width *= scale;
    resized_height *= scale;
    let patch_count =
        |width: f64, height: f64| (width / PATCH_SIZE).ceil() * (height / PATCH_SIZE).ceil();
    if patch_count(resized_width, resized_height) > PATCH_BUDGET {
        let shrink =
            ((PATCH_SIZE * PATCH_SIZE * PATCH_BUDGET) / (resized_width * resized_height)).sqrt();
        let width_units = resized_width * shrink / PATCH_SIZE;
        let height_units = resized_height * shrink / PATCH_SIZE;
        let width_adjust = if width_units > 0.0 {
            width_units.floor() / width_units
        } else {
            1.0
        };
        let height_adjust = if height_units > 0.0 {
            height_units.floor() / height_units
        } else {
            1.0
        };
        let adjusted = shrink * width_adjust.min(height_adjust);
        resized_width *= adjusted;
        resized_height *= adjusted;
    }
    patch_count(resized_width.max(1.0), resized_height.max(1.0))
        .min(PATCH_BUDGET)
        .mul_add(PATCH_MULTIPLIER, 0.0)
        .ceil() as usize
}

const MAX_NATIVE_PATCH_DEPTH: usize = 32;

fn native_text_candidate(value: &Value) -> Result<Option<String>, io::Error> {
    for candidate in [Some(value), value.get("v")].into_iter().flatten() {
        let Some(message) = candidate.get("message").and_then(Value::as_object) else {
            continue;
        };
        let role = message
            .get("author")
            .and_then(Value::as_object)
            .and_then(|author| author.get("role"))
            .map(|value| super::protocol_anthropic::python_text(Some(value)))
            .unwrap_or_default();
        if !role.trim().eq_ignore_ascii_case("assistant") {
            continue;
        }
        let content = match message.get("content") {
            None | Some(Value::Null) => continue,
            Some(value) if !super::account_pool::account_value_truthy(Some(value)) => continue,
            Some(Value::Object(content)) => content,
            Some(_) => return Err(io::Error::other("malformed upstream event")),
        };
        if let Some(parts) = content
            .get("parts")
            .filter(|value| super::account_pool::account_value_truthy(Some(value)))
            .and_then(Value::as_array)
        {
            let text = parts.iter().filter_map(Value::as_str).collect::<String>();
            if !text.is_empty() {
                return Ok(Some(text));
            }
            if let Some(text) = content.get("text") {
                let text = super::protocol_anthropic::python_text(Some(text));
                return Ok((!text.is_empty()).then_some(text));
            }
            continue;
        }
        if let Some(text) = content.get("text") {
            let text = super::protocol_anthropic::python_text(Some(text));
            return Ok((!text.is_empty()).then_some(text));
        }
    }
    Ok(None)
}

fn native_history_text_candidate(value: &Value) -> Result<Option<String>, io::Error> {
    for candidate in [Some(value), value.get("v")].into_iter().flatten() {
        let role = candidate
            .get("message")
            .and_then(|message| message.get("author"))
            .and_then(|author| author.get("role"))
            .and_then(Value::as_str);
        if role == Some("assistant") {
            return native_text_candidate(candidate);
        }
    }
    Ok(None)
}

fn native_patch_candidate(
    value: &Value,
    current_text: &str,
    history_text: &str,
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
            .map(|value| super::protocol_anthropic::python_text(Some(value)))
            .unwrap_or_default();
        return match operation {
            Some("append") => Ok(Some(format!("{current_text}{text}"))),
            Some("replace") => Ok(Some(strip_history(&text, history_text))),
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
        if let Some(candidate) = native_patch_candidate(item, &next_text, history_text, depth + 1)?
        {
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
    if lower.starts_with("turn") {
        return true;
    }
    !lower.is_empty() && lower.chars().all(|character| character.is_ascii_digit())
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
    native_strip_space_before_punctuation(&output)
}

fn native_strip_space_before_punctuation(text: &str) -> String {
    let characters = text.chars().collect::<Vec<_>>();
    let mut cleaned = String::with_capacity(text.len());
    let mut index = 0;
    while index < characters.len() {
        if !characters[index].is_whitespace() {
            cleaned.push(characters[index]);
            index += 1;
            continue;
        }
        let start = index;
        while index < characters.len() && characters[index].is_whitespace() {
            index += 1;
        }
        if index == characters.len() || !".,;:!?".contains(characters[index]) {
            cleaned.extend(characters[start..index].iter());
        }
    }
    cleaned
}
pub(crate) fn native_clean_search_text(text: &str) -> String {
    let sanitized = native_sanitize_text(text);
    let characters = sanitized.chars().collect::<Vec<_>>();
    let mut cleaned = String::with_capacity(sanitized.len());
    let mut index = 0;
    while index < characters.len() {
        if !characters[index].is_whitespace() {
            cleaned.push(characters[index]);
            index += 1;
            continue;
        }
        let start = index;
        while index < characters.len() && characters[index].is_whitespace() {
            index += 1;
        }
        if index == characters.len() || !".,;:!?".contains(characters[index]) {
            cleaned.extend(characters[start..index].iter());
        }
    }
    cleaned.trim().to_owned()
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn native_frame_with_history_messages(
    payload: &[u8],
    current_text: &mut String,
    completion_id: &str,
    model: &str,
    created: i64,
    include_usage: bool,
    history_text: &str,
    history_messages: &[String],
    history_index: &AtomicUsize,
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
    let Ok(value) = serde_json::from_str::<Value>(&data) else {
        return Ok(None);
    };
    // ChatGPT emits lifecycle metadata before and between visible messages
    // (for example resume_conversation_token). Python skips those frames;
    // they are not malformed assistant content.
    if value.get("type").and_then(Value::as_str).is_some()
        && value.get("message").is_none()
        && value.get("p").is_none()
        && value.get("v").is_none()
    {
        return Ok(None);
    }
    let history_candidate = native_history_text_candidate(&value)?;
    if let Some(candidate) = history_candidate.as_deref() {
        let index = history_index.load(Ordering::Acquire);
        if index < history_messages.len()
            && strip_history(candidate, history_text) == history_messages[index]
        {
            history_index.store(index + 1, Ordering::Release);
            current_text.clear();
            return Ok(None);
        }
    }
    let text_candidate = native_text_candidate(&value)?;
    let candidate = if let Some(candidate) = text_candidate {
        Some(strip_history(&candidate, history_text))
    } else {
        native_patch_candidate(&value, current_text, history_text, 0)?
    };
    let Some(candidate) = candidate else {
        return Ok(None);
    };
    let current_visible = native_sanitize_text(current_text);
    let candidate_visible = native_sanitize_text(&candidate);
    let delta = if candidate_visible.starts_with(current_visible.as_str()) {
        candidate_visible[current_visible.len()..].to_owned()
    } else {
        candidate_visible.clone()
    };
    *current_text = candidate;
    if delta.is_empty() {
        return Ok(None);
    }
    let mut frame = if current_visible.is_empty() {
        json!({
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"role": "assistant", "content": delta}, "finish_reason": null}],
        })
    } else {
        json!({
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{"index": 0, "delta": {"content": delta}, "finish_reason": null}],
        })
    };
    if include_usage {
        frame["usage"] = Value::Null;
    }
    let mut output = serde_json::to_vec(&frame).map_err(|_| io::Error::other("upstream error"))?;
    output.extend_from_slice(b"\n\n");
    let mut framed = b"data: ".to_vec();
    framed.extend(output);
    Ok(Some(framed))
}

pub(crate) fn native_frame_with_history(
    payload: &[u8],
    current_text: &mut String,
    completion_id: &str,
    model: &str,
    created: i64,
    include_usage: bool,
    history_text: &str,
) -> Result<Option<Vec<u8>>, io::Error> {
    let history_index = AtomicUsize::new(0);
    native_frame_with_history_messages(
        payload,
        current_text,
        completion_id,
        model,
        created,
        include_usage,
        history_text,
        &[],
        &history_index,
    )
}

pub(crate) fn native_frame(
    payload: &[u8],
    current_text: &mut String,
    completion_id: &str,
    model: &str,
    created: i64,
    include_usage: bool,
) -> Result<Option<Vec<u8>>, io::Error> {
    native_frame_with_history(
        payload,
        current_text,
        completion_id,
        model,
        created,
        include_usage,
        "",
    )
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

fn append_completion_frame_text(frame: &[u8], output: &mut String) -> Result<(), ApiError> {
    let data = frame
        .strip_prefix(b"data: ")
        .and_then(|frame| frame.strip_suffix(b"\n\n"))
        .ok_or_else(ApiError::upstream)?;
    if data == b"[DONE]" {
        return Ok(());
    }
    let value: Value = serde_json::from_slice(data).map_err(|_| ApiError::upstream())?;
    if let Some(content) = value
        .get("choices")
        .and_then(Value::as_array)
        .and_then(|choices| choices.first())
        .and_then(|choice| choice.get("delta"))
        .and_then(Value::as_object)
        .and_then(|delta| delta.get("content"))
        .and_then(Value::as_str)
    {
        output.push_str(content);
    }
    Ok(())
}

pub(crate) fn native_completion_text_with_history(
    body: &[u8],
    history_text: &str,
) -> Result<String, ApiError> {
    let mut current_text = String::new();
    let mut output_text = String::new();
    let mut buffer = body.to_vec();
    let mut terminated = false;
    while let Some((position, delimiter_length)) = sse_delimiter(&buffer) {
        let event = buffer.drain(..position).collect::<Vec<_>>();
        buffer.drain(..delimiter_length);
        if let Some(frame) = native_frame_with_history(
            &event,
            &mut current_text,
            "chatcmpl-rust-canary",
            "auto",
            0,
            false,
            history_text,
        )
        .map_err(|_| ApiError::upstream())?
        {
            if frame == b"data: [DONE]\n\n" {
                terminated = true;
                break;
            }
            append_completion_frame_text(&frame, &mut output_text)?;
        }
    }
    if !terminated
        && !buffer.is_empty()
        && let Some(frame) = native_frame_with_history(
            &buffer,
            &mut current_text,
            "chatcmpl-rust-canary",
            "auto",
            0,
            false,
            history_text,
        )
        .map_err(|_| ApiError::upstream())?
    {
        if frame == b"data: [DONE]\n\n" {
            terminated = true;
        } else {
            append_completion_frame_text(&frame, &mut output_text)?;
        }
    }
    let _ = terminated;
    // Python accepts a clean EOF after valid SSE content; transport/body
    // failures are reported by the caller before this parser is invoked.
    Ok(native_sanitize_text(&output_text))
}

pub(crate) fn native_completion_text(body: &[u8]) -> Result<String, ApiError> {
    native_completion_text_with_history(body, "")
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
    tools.iter().all(|tool| native_codex_tool(tool).is_ok())
}

fn has_native_tool(value: Option<&Value>) -> bool {
    value.and_then(Value::as_array).is_some_and(|tools| {
        tools.iter().any(|tool| {
            tool.as_object()
                .and_then(|tool| tool.get("type"))
                .and_then(Value::as_str)
                .is_some_and(|kind| {
                    matches!(
                        kind,
                        "function"
                            | "web_search"
                            | "web_search_preview"
                            | "web_search_preview_2025_03_11"
                            | "web_search_2025_08_26"
                    )
                })
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
    message.is_object()
}

#[allow(dead_code)]
fn validate_chat_options_post_v17(object: &Map<String, Value>) -> Result<(), ApiError> {
    const ALLOWED_FIELDS: &[&str] = &[
        "messages",
        "modalities",
        "max_tokens",
        "model",
        "n",
        "parallel_tool_calls",
        "prompt",
        "prompt_cache_key",
        "reasoning",
        "reasoning_effort",
        "response_format",
        "service_tier",
        "store",
        "stream",
        "stream_options",
        "thinking_effort",
        "tool_choice",
        "tools",
        "verbosity",
        "web_search_options",
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
        if items.is_empty()
            || items
                .iter()
                .any(|item| !matches!(item.as_str(), Some("text" | "image")))
        {
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
    let contains_native_tool = has_native_tool(object.get("tools"))
        || object
            .get("web_search_options")
            .is_some_and(|value| !value.is_null());
    let has_native_feature = has_native_chat_feature(object.get("messages"));
    if let Some(value) = object.get("tool_choice")
        && !value.is_null()
        && (((contains_native_tool || has_native_feature) && value.as_str() != Some("auto"))
            || (!contains_native_tool && !has_native_feature && value.as_str() != Some("none")))
    {
        return Err(ApiError::invalid_request());
    }
    if let Some(Value::Array(_)) = object.get("tools").filter(|value| !value.is_null())
        && !valid_chat_tools(object.get("tools").expect("tools array present"))
    {
        return Err(ApiError::invalid_request());
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
    if object
        .get("prompt_cache_key")
        .is_some_and(|value| !value.is_null() && !value.is_string())
        || object.get("service_tier").is_some_and(|value| {
            !value.is_null()
                && !matches!(
                    value.as_str(),
                    Some("default" | "fast" | "priority" | "flex")
                )
        })
        || object.get("verbosity").is_some_and(|value| {
            !value.is_null() && !matches!(value.as_str(), Some("low" | "medium" | "high"))
        })
    {
        return Err(ApiError::invalid_request());
    }
    if let Some(response_format) = object
        .get("response_format")
        .filter(|value| !value.is_null())
    {
        let response_format = response_format
            .as_object()
            .ok_or_else(ApiError::invalid_request)?;
        let format_type = response_format
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(ApiError::invalid_request)?;
        match format_type {
            "text" if response_format.len() == 1 => {}
            "json_schema" => {
                let schema = response_format
                    .get("json_schema")
                    .and_then(Value::as_object)
                    .ok_or_else(ApiError::invalid_request)?;
                if response_format
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
            }
            _ => return Err(ApiError::invalid_request()),
        }
    }
    if let Some(options) = object
        .get("web_search_options")
        .filter(|value| !value.is_null())
    {
        let options = options.as_object().ok_or_else(ApiError::invalid_request)?;
        if options
            .keys()
            .any(|key| !matches!(key.as_str(), "search_context_size" | "user_location"))
            || options
                .get("search_context_size")
                .is_some_and(|value| !matches!(value.as_str(), Some("low" | "medium" | "high")))
        {
            return Err(ApiError::invalid_request());
        }
        if let Some(location) = options
            .get("user_location")
            .filter(|value| !value.is_null())
        {
            let location = location.as_object().ok_or_else(ApiError::invalid_request)?;
            if location
                .keys()
                .any(|key| !matches!(key.as_str(), "type" | "approximate"))
                || location.get("type").and_then(Value::as_str) != Some("approximate")
            {
                return Err(ApiError::invalid_request());
            }
            let approximate = location
                .get("approximate")
                .and_then(Value::as_object)
                .ok_or_else(ApiError::invalid_request)?;
            if approximate
                .keys()
                .any(|key| !matches!(key.as_str(), "city" | "country" | "region" | "timezone"))
                || approximate.values().any(|value| !value.is_string())
            {
                return Err(ApiError::invalid_request());
            }
        }
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

fn python_bool_value(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(value) => match value.as_i64()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        },
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "t" | "yes" | "y" | "on" => Some(true),
            "0" | "false" | "f" | "no" | "n" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn pydantic_integer_value(value: &Value) -> Option<Value> {
    let integer = match value {
        Value::Bool(value) => i64::from(*value),
        Value::Number(value) => {
            if let Some(integer) = value.as_i64() {
                integer
            } else if let Some(unsigned) = value.as_u64() {
                return Some(Value::from(unsigned));
            } else {
                let float = value.as_f64()?;
                if float.fract() != 0.0 || float < i64::MIN as f64 || float > i64::MAX as f64 {
                    return None;
                }
                float as i64
            }
        }
        Value::String(value) => value.trim().parse::<i64>().ok()?,
        _ => return None,
    };
    Some(Value::from(integer))
}

fn validate_chat_options(object: &mut Map<String, Value>) -> Result<(), ApiError> {
    if let Some(value) = object.get("n").filter(|value| !value.is_null()) {
        let integer = pydantic_integer_value(value).ok_or_else(ApiError::validation)?;
        object.insert("n".to_owned(), integer);
    }
    if let Some(value) = object.get("stream").filter(|value| !value.is_null()) {
        let boolean = python_bool_value(value).ok_or_else(ApiError::validation)?;
        object.insert("stream".to_owned(), Value::Bool(boolean));
    }
    if let Some(value) = object.get("modalities").filter(|value| !value.is_null()) {
        let Value::Array(items) = value else {
            return Err(ApiError::validation());
        };
        if items.iter().any(|item| !item.is_string()) {
            return Err(ApiError::validation());
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
        return Err(ApiError::validation_message(
            "model: Input should be a valid string",
        ));
    }
    if let Some(prompt) = object.get("prompt")
        && !prompt.is_null()
        && !prompt.is_string()
    {
        return Err(ApiError::validation_message(
            "prompt: Input should be a valid string",
        ));
    }
    let has_messages = if let Some(messages) = object.get("messages") {
        if messages.is_null() {
            false
        } else {
            let Value::Array(messages) = messages else {
                return Err(ApiError::validation_message(
                    "messages: Input should be a valid list",
                ));
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
        return Err(ApiError::invalid_request_message(
            "messages or prompt is required",
        ));
    }
    validate_chat_options(&mut object)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scalar_tools_are_accepted_like_python_extra_fields() {
        let payload = validate_chat_payload(json!({
            "model": "auto",
            "prompt": "hello",
            "tools": "bad"
        }))
        .expect("scalar tools are ignored by the v1.7 adapter");
        assert_eq!(payload["tools"], "bad");
    }

    #[test]
    fn chat_text_n_values_are_normalized_without_singleton_restriction() {
        for raw in [0, 2] {
            let payload = validate_chat_payload(json!({
                "model": "auto",
                "prompt": "text",
                "n": raw,
            }))
            .expect("text Chat accepts any parsed n like Python");
            assert_eq!(payload["n"], raw);
        }
    }

    #[test]
    fn native_sanitize_text_removes_space_before_punctuation_for_plain_text() {
        assert_eq!(native_sanitize_text("hello , world !"), "hello, world!");
    }
    #[test]
    fn assistant_history_uses_python_role_selection() {
        let payload = |value: Value| {
            format!(
                "data: {}\n\n",
                serde_json::to_string(&value).expect("event JSON")
            )
            .into_bytes()
        };
        let mut current_text = String::new();
        let history_index = AtomicUsize::new(0);
        let history = vec!["old".to_owned()];
        let frame = native_frame_with_history_messages(
            &payload(json!({
                "message": {
                    "author": {"role": " Assistant "},
                    "content": {"parts": ["old"]}
                }
            })),
            &mut current_text,
            "id",
            "auto",
            0,
            false,
            "",
            &history,
            &history_index,
        )
        .expect("assistant frame");
        assert!(frame.is_some(), "non-exact role must not consume history");

        let mut current_text = String::new();
        let history_index = AtomicUsize::new(0);
        let frame = native_frame_with_history_messages(
            &payload(json!({
                "message": {
                    "author": {"role": "user"},
                    "content": {"parts": ["user"]}
                },
                "v": {
                    "message": {
                        "author": {"role": "assistant"},
                        "content": {"parts": ["assistant"]}
                    }
                }
            })),
            &mut current_text,
            "id",
            "auto",
            0,
            false,
            "",
            &[],
            &history_index,
        )
        .expect("assistant frame in v");
        assert!(frame.is_some(), "assistant text in v must be considered");
    }
}
