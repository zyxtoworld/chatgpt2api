use serde_json::{Map, Value, json};

use super::errors::ApiError;

const RESPONSE_MESSAGE_FIELDS: &[&str] = &["type", "id", "role", "content", "phase", "status"];
const RESPONSE_CONTENT_TYPES: &[&str] =
    &["input_text", "output_text", "input_image", "input_audio"];

pub(super) fn validate_responses_payload(payload: Value) -> Result<Map<String, Value>, ApiError> {
    let Value::Object(mut object) = payload else {
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
        || object
            .get("parallel_tool_calls")
            .is_some_and(|value| !value.is_null() && !value.is_boolean())
        || object
            .get("store")
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
    if let Some(input) = object.get("input").filter(|value| !value.is_null()) {
        validate_response_input(input)?;
    }
    if let Some(value) = object.get("tools").filter(|value| !value.is_null())
        && (!value.is_array()
            || value
                .as_array()
                .is_some_and(|items| items.iter().any(|item| !item.is_object())))
    {
        return Err(ApiError::validation());
    }
    if let Some(value) = object.get("include").filter(|value| !value.is_null())
        && (!value.is_array()
            || value
                .as_array()
                .is_some_and(|items| items.iter().any(|item| !item.is_string())))
    {
        return Err(ApiError::validation());
    }
    if let Some(value) = object
        .get("stream_options")
        .filter(|value| !value.is_null())
        && !value.is_object()
    {
        return Err(ApiError::validation());
    }
    if let Some(value) = object
        .get("context_management")
        .filter(|value| !value.is_null())
        && !value.is_array()
    {
        return Err(ApiError::validation());
    }
    if let Some(value) = object.get("text").filter(|value| !value.is_null())
        && !value.is_object()
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
    if object.get("input").is_none_or(Value::is_null) {
        return Err(ApiError::invalid_request());
    }
    object.insert("model".to_owned(), Value::String(model));
    Ok(object)
}

fn validate_response_input(input: &Value) -> Result<(), ApiError> {
    let items: Vec<Value> = match input {
        Value::String(_) => return Ok(()),
        Value::Object(object) => vec![Value::Object(object.clone())],
        Value::Array(items) => items.to_vec(),
        _ => return Err(ApiError::validation()),
    };
    for item in &items {
        let object = item.as_object().ok_or_else(ApiError::invalid_request)?;
        if object
            .get("type")
            .and_then(Value::as_str)
            .is_some_and(|kind| RESPONSE_CONTENT_TYPES.contains(&kind))
        {
            validate_response_content_part(item)?;
            continue;
        }
        let is_message = object.get("type").and_then(Value::as_str) == Some("message")
            || (object.get("type").is_none()
                && object.contains_key("role")
                && object.contains_key("content"));
        if is_message {
            validate_response_message(item)?;
            continue;
        }
        let kind = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(ApiError::invalid_request)?;
        validate_response_history_item(kind, object)?;
    }
    Ok(())
}

fn validate_response_message(value: &Value) -> Result<(), ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    if object
        .keys()
        .any(|key| !RESPONSE_MESSAGE_FIELDS.contains(&key.as_str()))
    {
        return Err(ApiError::invalid_request());
    }
    if object.get("type").is_some_and(|value| value != "message") {
        return Err(ApiError::invalid_request());
    }
    let role = object
        .get("role")
        .and_then(Value::as_str)
        .filter(|role| matches!(*role, "user" | "assistant" | "system" | "developer"))
        .ok_or_else(ApiError::invalid_request)?;
    if object.get("id").is_some_and(|value| {
        !value.is_null() && value.as_str().is_none_or(|id| id.trim().is_empty())
    }) || object.get("phase").is_some_and(|value| {
        !value.is_null()
            && (role != "assistant"
                || !matches!(value.as_str(), Some("commentary" | "final_answer")))
    }) || object.get("status").is_some_and(|value| {
        !value.is_null()
            && !matches!(
                value.as_str(),
                Some("in_progress" | "completed" | "incomplete")
            )
    }) {
        return Err(ApiError::invalid_request());
    }
    let content = object
        .get("content")
        .ok_or_else(ApiError::invalid_request)?;
    match content {
        Value::String(_) => Ok(()),
        Value::Array(parts) => parts.iter().try_for_each(validate_response_content_part),
        _ => Err(ApiError::invalid_request()),
    }
}

fn validate_response_content_part(value: &Value) -> Result<(), ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .filter(|kind| RESPONSE_CONTENT_TYPES.contains(kind))
        .ok_or_else(ApiError::invalid_request)?;
    match kind {
        "input_text" => {
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "type" | "text"))
                || !object.get("text").is_some_and(Value::is_string)
            {
                return Err(ApiError::invalid_request());
            }
        }
        "output_text" => {
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "type" | "text" | "annotations" | "logprobs"))
                || !object.get("text").is_some_and(Value::is_string)
                || object
                    .get("annotations")
                    .is_some_and(|value| !value.is_array())
                || object
                    .get("logprobs")
                    .is_some_and(|value| !value.is_array())
            {
                return Err(ApiError::invalid_request());
            }
        }
        "input_image" => {
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "type" | "image_url" | "detail"))
                || object
                    .get("image_url")
                    .and_then(Value::as_str)
                    .is_none_or(|url| url.trim().is_empty())
                || object.get("detail").is_some_and(|value| {
                    !matches!(value.as_str(), Some("auto" | "low" | "high" | "original"))
                })
            {
                return Err(ApiError::invalid_request());
            }
        }
        "input_audio" => {
            if object
                .keys()
                .any(|key| !matches!(key.as_str(), "type" | "audio_url"))
                || object
                    .get("audio_url")
                    .and_then(Value::as_str)
                    .is_none_or(|url| url.trim().is_empty())
            {
                return Err(ApiError::invalid_request());
            }
        }
        _ => unreachable!(),
    }
    Ok(())
}

fn validate_response_history_item(kind: &str, object: &Map<String, Value>) -> Result<(), ApiError> {
    let allowed: &[&str] = match kind {
        "function_call" => &[
            "type",
            "id",
            "call_id",
            "name",
            "description",
            "namespace",
            "arguments",
            "encrypted_function_args",
            "status",
        ],
        "function_call_output" => &["type", "id", "call_id", "output", "status"],
        "custom_tool_call" => &[
            "type",
            "id",
            "call_id",
            "name",
            "namespace",
            "input",
            "status",
        ],
        "custom_tool_call_output" => &["type", "id", "call_id", "name", "output"],
        "reasoning" => &[
            "type",
            "id",
            "summary",
            "content",
            "encrypted_content",
            "status",
        ],
        "compaction" | "compaction_summary" | "context_compaction" => {
            &["type", "id", "encrypted_content"]
        }
        "image_generation_call" => &["type", "id", "status", "revised_prompt", "result"],
        "web_search_call" => &["type", "id", "status", "action"],
        "tool_search_call" => &["type", "id", "call_id", "status", "execution", "arguments"],
        "tool_search_output" => &["type", "id", "call_id", "status", "execution", "tools"],
        "mcp_tool_call_output" => &["type", "call_id", "output"],
        _ => return Err(ApiError::invalid_request()),
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(ApiError::invalid_request());
    }
    match kind {
        "function_call"
            if object
                .get("call_id")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
                || object
                    .get("name")
                    .and_then(Value::as_str)
                    .is_none_or(|value| value.trim().is_empty())
                || !object.get("arguments").is_some_and(Value::is_string) =>
        {
            return Err(ApiError::invalid_request());
        }
        "function_call_output"
            if object
                .get("call_id")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
                || !object.contains_key("output") =>
        {
            return Err(ApiError::invalid_request());
        }
        "custom_tool_call"
            if object
                .get("call_id")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
                || object
                    .get("name")
                    .and_then(Value::as_str)
                    .is_none_or(|value| value.trim().is_empty())
                || !object.get("input").is_some_and(Value::is_string) =>
        {
            return Err(ApiError::invalid_request());
        }
        "custom_tool_call_output"
            if object
                .get("call_id")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
                || !object.contains_key("output") =>
        {
            return Err(ApiError::invalid_request());
        }
        "image_generation_call"
            if !matches!(
                object.get("status").and_then(Value::as_str),
                Some("in_progress" | "completed" | "generating" | "failed")
            ) || !object.get("result").is_some_and(Value::is_string) =>
        {
            return Err(ApiError::invalid_request());
        }
        "tool_search_call" | "tool_search_output"
            if !matches!(
                object.get("execution").and_then(Value::as_str),
                Some("server" | "client")
            ) =>
        {
            return Err(ApiError::invalid_request());
        }
        "mcp_tool_call_output"
            if object
                .get("call_id")
                .and_then(Value::as_str)
                .is_none_or(|value| value.trim().is_empty())
                || !object.get("output").is_some_and(Value::is_object) =>
        {
            return Err(ApiError::invalid_request());
        }
        _ => {}
    }
    Ok(())
}

pub(super) fn normalize_response_content_part(
    value: &Value,
    role: &str,
) -> Result<Value, ApiError> {
    validate_response_content_part(value)?;
    let mut part = value
        .as_object()
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    if role == "assistant" && part.get("type").and_then(Value::as_str) == Some("input_text") {
        part.insert("type".to_owned(), json!("output_text"));
    }
    Ok(Value::Object(part))
}

pub(super) fn normalize_response_message(value: &Value) -> Result<Value, ApiError> {
    validate_response_message(value)?;
    let mut object = value
        .as_object()
        .cloned()
        .ok_or_else(ApiError::invalid_request)?;
    if object.get("type").is_none() {
        object.insert("type".to_owned(), json!("message"));
    }
    if object.get("role").and_then(Value::as_str) == Some("system") {
        object.insert("role".to_owned(), json!("developer"));
    }
    object.remove("status");
    if let Some(Value::Array(parts)) = object.get("content").cloned() {
        let role = object.get("role").and_then(Value::as_str).unwrap_or("user");
        object.insert(
            "content".to_owned(),
            Value::Array(
                parts
                    .iter()
                    .map(|part| normalize_response_content_part(part, role))
                    .collect::<Result<Vec<_>, _>>()?,
            ),
        );
    }
    Ok(Value::Object(object))
}

pub(super) fn native_responses_text_input(value: &Value) -> Result<Value, ApiError> {
    match value {
        Value::String(text) => Ok(json!([{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": text}],
        }])),
        Value::Object(_) => Ok(Value::Array(vec![normalize_response_message(value)?])),
        Value::Array(items) => {
            if items.is_empty() {
                return Err(ApiError::invalid_request());
            }
            if items.iter().all(|item| {
                item.as_object()
                    .is_some_and(|object| object.get("role").is_some())
            }) {
                return items
                    .iter()
                    .map(normalize_response_message)
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::Array);
            }
            Err(ApiError::invalid_request())
        }
        _ => Err(ApiError::invalid_request()),
    }
}

pub(super) fn response_content_part_type(value: &Value) -> Option<&str> {
    value
        .as_object()
        .and_then(|object| object.get("type"))
        .and_then(Value::as_str)
        .filter(|kind| RESPONSE_CONTENT_TYPES.contains(kind))
}
