use serde_json::{Map, Value, json};

use super::errors::ApiError;

const RESPONSE_MESSAGE_FIELDS: &[&str] = &["type", "id", "role", "content", "phase", "status"];
const RESPONSE_CONTENT_TYPES: &[&str] = &[
    "text",
    "input_text",
    "output_text",
    "input_image",
    "input_audio",
    "image_url",
    "image",
];

pub(super) fn validate_responses_payload(payload: Value) -> Result<Map<String, Value>, ApiError> {
    let Value::Object(mut object) = payload else {
        return Err(ApiError::validation());
    };
    if object
        .get("model")
        .is_some_and(|value| !value.is_null() && value.as_str().is_none())
    {
        return Err(ApiError::validation_message(
            "model: Input should be a valid string",
        ));
    }
    if let Some(stream) = object.get("stream").filter(|value| !value.is_null()) {
        let normalized = pydantic_bool(stream).ok_or_else(|| {
            ApiError::validation_message("stream: Input should be a valid boolean")
        })?;
        object.insert("stream".to_owned(), Value::Bool(normalized));
    }
    if let Some(value) = object.get("tools").filter(|value| !value.is_null())
        && (!value.is_array()
            || value
                .as_array()
                .is_some_and(|items| items.iter().any(|item| !item.is_object())))
    {
        return Err(ApiError::validation());
    }
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

fn pydantic_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::Number(value) if value.as_i64() == Some(0) => Some(false),
        Value::Number(value) if value.as_i64() == Some(1) => Some(true),
        Value::String(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "t" | "yes" | "y" | "on" => Some(true),
            "0" | "false" | "f" | "no" | "n" | "off" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn validate_response_input(_input: &Value) -> Result<(), ApiError> {
    Ok(())
}

fn validate_response_message(value: &Value) -> Result<(), ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    if let Some(Value::Array(parts)) = object.get("content") {
        for part in parts {
            if part.as_object().is_some_and(|part| {
                part.get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| RESPONSE_CONTENT_TYPES.contains(&kind.trim()))
            }) {
                validate_response_content_part(part)?;
            }
        }
    }
    Ok(())
}

fn validate_response_content_part(value: &Value) -> Result<(), ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
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
        "text" => {
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
        "input_image" | "image_url" | "image" => {
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

pub(super) fn python_responses_thinking_effort(object: &Map<String, Value>) -> String {
    let value = if let Some(reasoning) = object.get("reasoning").and_then(Value::as_object) {
        reasoning.get("effort")
    } else if object.contains_key("thinking_effort") {
        object.get("thinking_effort")
    } else if object.contains_key("reasoning_effort") {
        object.get("reasoning_effort")
    } else {
        None
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
pub(super) fn native_responses_text_input(value: &Value) -> Result<Value, ApiError> {
    match value {
        Value::String(text) => Ok(if text.trim().is_empty() {
            Value::Array(Vec::new())
        } else {
            json!([{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": text.trim()}],
            }])
        }),
        Value::Object(_) if is_response_content_part(value) => Ok(json!([{
            "type":"message",
            "role":"user",
            "content":[value]
        }])),
        Value::Object(_) => Ok(Value::Array(
            response_input_message(value).into_iter().collect(),
        )),
        Value::Array(items) if items.iter().all(is_response_content_part) => {
            let parts = items
                .iter()
                .filter(|item| item.is_object())
                .cloned()
                .collect::<Vec<_>>();
            Ok(if parts.is_empty() {
                Value::Array(Vec::new())
            } else {
                json!([{"type":"message","role":"user","content":parts}])
            })
        }
        Value::Array(items) => {
            let mut messages = Vec::new();
            let mut pending_parts = Vec::new();
            for item in items {
                if is_response_content_part(item) {
                    if item.is_object() {
                        pending_parts.push(item.clone());
                    }
                    continue;
                }
                if !pending_parts.is_empty() {
                    messages.push(json!({
                        "type":"message",
                        "role":"user",
                        "content":std::mem::take(&mut pending_parts)
                    }));
                }
                if let Some(message) = response_input_message(item) {
                    messages.push(message);
                }
            }
            if !pending_parts.is_empty() {
                messages.push(json!({
                    "type":"message",
                    "role":"user",
                    "content":pending_parts
                }));
            }
            Ok(Value::Array(messages))
        }
        _ => Ok(Value::Array(Vec::new())),
    }
}

fn is_response_content_part(value: &Value) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or_default();
    RESPONSE_CONTENT_TYPES.contains(&kind)
        || (object.contains_key("image_url") && kind != "message")
}

fn response_input_message(value: &Value) -> Option<Value> {
    let object = value.as_object()?;
    let role = object
        .get("role")
        .filter(|value| super::account_pool::account_value_truthy(Some(value)))
        .map(|value| Value::String(super::account_pool::python_account_value_string(value)))
        .unwrap_or_else(|| json!("user"));
    let content = object.get("content")?;
    let content = match content {
        Value::String(text) if !text.trim().is_empty() => Value::String(text.trim().to_owned()),
        Value::Array(parts) if !parts.is_empty() => Value::Array(parts.clone()),
        _ => return None,
    };
    Some(json!({"type":"message","role":role,"content":content}))
}

pub(super) fn response_content_part_type(value: &Value) -> Option<&str> {
    value
        .as_object()
        .and_then(|object| object.get("type"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|kind| RESPONSE_CONTENT_TYPES.contains(kind))
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn responses_stream_validation_matches_pydantic_boolean_coercion() {
        for (raw, expected) in [("true", true), (" YES ", true), ("off", false)] {
            let payload = validate_responses_payload(json!({"stream":raw}))
                .expect("Pydantic-compatible stream value");
            assert_eq!(payload["stream"], expected);
        }
        assert_eq!(
            validate_responses_payload(json!({"stream":"sometimes"}))
                .expect_err("unknown boolean string is rejected")
                .status,
            axum::http::StatusCode::UNPROCESSABLE_ENTITY
        );
    }
    #[test]
    fn responses_roles_use_python_string_coercion() {
        let value = native_responses_text_input(&json!([{"role":7,"content":"x"}]))
            .expect("numeric response role");
        assert_eq!(value[0]["role"], "7");
        assert_eq!(value[0]["role"], "7");
    }

    #[test]
    fn responses_trim_content_type_but_preserve_role_text() {
        let value = native_responses_text_input(&json!([{
            "role":" assistant ",
            "content":[{"type":" input_text ","text":"hello"}]
        }]))
        .expect("trimmed response content type");
        assert_eq!(value[0]["role"], " assistant ");
        assert_eq!(value[0]["content"][0]["type"], " input_text ");
    }

    #[test]
    fn responses_empty_input_becomes_empty_user_message_at_route_boundary() {
        let value = native_responses_text_input(&Value::Null).expect("empty input");
        assert!(value.as_array().expect("messages").is_empty());
    }
    #[test]
    fn responses_empty_reasoning_does_not_fallback_to_other_efforts() {
        let value = json!({
            "reasoning": {},
            "thinking_effort": "high",
            "reasoning_effort": "low"
        });
        assert_eq!(
            python_responses_thinking_effort(value.as_object().unwrap()),
            ""
        );
    }
}
