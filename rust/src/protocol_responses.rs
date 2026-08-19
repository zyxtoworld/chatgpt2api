use serde_json::{Map, Value, json};

use super::errors::ApiError;

pub(super) fn validate_responses_payload(payload: Value) -> Result<Map<String, Value>, ApiError> {
    let Value::Object(object) = payload else {
        return Err(ApiError::validation());
    };
    if object
        .get("model")
        .is_some_and(|value| !value.is_null() && value.as_str().is_none())
        || object
            .get("instructions")
            .is_some_and(|value| !value.is_null() && value.as_str().is_none())
        || object
            .get("stream")
            .is_some_and(|value| !value.is_null() && !value.is_boolean())
    {
        return Err(ApiError::validation());
    }
    if let Some(input) = object.get("input")
        && !input.is_null()
        && !input.is_string()
        && !input.is_object()
        && !(input.is_array()
            && input
                .as_array()
                .is_some_and(|items| items.iter().all(Value::is_object)))
    {
        return Err(ApiError::validation());
    }
    let model = object
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(ApiError::invalid_request)?
        .to_owned();
    if object.get("input").is_none_or(|value| value.is_null()) {
        return Err(ApiError::invalid_request());
    }
    let mut object = object;
    object.insert("model".to_owned(), Value::String(model));
    Ok(object)
}

pub(super) fn native_responses_text_input(value: &Value) -> Result<Value, ApiError> {
    match value {
        Value::String(text) => Ok(Value::String(text.clone())),
        Value::Array(items) => {
            let mut normalized = Vec::with_capacity(items.len());
            for item in items {
                let Some(object) = item.as_object() else {
                    return Err(ApiError::invalid_request());
                };
                let role = object
                    .get("role")
                    .and_then(Value::as_str)
                    .ok_or_else(ApiError::invalid_request)?;
                if !matches!(role, "user" | "assistant" | "developer") {
                    return Err(ApiError::invalid_request());
                }
                let content = object
                    .get("content")
                    .ok_or_else(ApiError::invalid_request)?;
                let content = match content {
                    Value::String(text) => Value::String(text.clone()),
                    Value::Array(parts) => {
                        let mut normalized_parts = Vec::with_capacity(parts.len());
                        for part in parts {
                            let Some(part) = part.as_object() else {
                                return Err(ApiError::invalid_request());
                            };
                            if part.keys().any(|key| key != "type" && key != "text")
                                || !matches!(
                                    part.get("type").and_then(Value::as_str),
                                    Some("input_text" | "output_text" | "text")
                                )
                            {
                                return Err(ApiError::invalid_request());
                            }
                            let text = part
                                .get("text")
                                .and_then(Value::as_str)
                                .ok_or_else(ApiError::invalid_request)?;
                            normalized_parts.push(json!({
                                "type": "input_text",
                                "text": text,
                            }));
                        }
                        Value::Array(normalized_parts)
                    }
                    _ => return Err(ApiError::invalid_request()),
                };
                if object.keys().any(|key| {
                    key != "type"
                        && key != "id"
                        && key != "role"
                        && key != "content"
                        && key != "status"
                }) {
                    return Err(ApiError::invalid_request());
                }
                normalized.push(json!({
                    "type": "message",
                    "role": role,
                    "content": content,
                }));
            }
            Ok(Value::Array(normalized))
        }
        _ => Err(ApiError::invalid_request()),
    }
}
