use super::protocol_responses::native_responses_text_input;
use super::{ApiError, CODEX_RESPONSES_MODEL};
use serde_json::{Map, Value, json};

pub(crate) fn native_codex_tool(value: &Value) -> Result<Value, ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    let tool_type = object
        .get("type")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(ApiError::invalid_request)?;
    match tool_type {
        "function" => {
            const ALLOWED: &[&str] = &[
                "type",
                "name",
                "description",
                "parameters",
                "strict",
                "defer_loading",
            ];
            if object.keys().any(|key| !ALLOWED.contains(&key.as_str())) {
                return Err(ApiError::invalid_request());
            }
            let name = object
                .get("name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(ApiError::invalid_request)?;
            if object
                .get("description")
                .is_some_and(|value| !value.is_null() && !value.is_string())
                || object
                    .get("parameters")
                    .is_some_and(|value| !value.is_null() && !value.is_object())
                || object
                    .get("strict")
                    .is_some_and(|value| !value.is_null() && !value.is_boolean())
                || object
                    .get("defer_loading")
                    .is_some_and(|value| !value.is_null() && !value.is_boolean())
            {
                return Err(ApiError::invalid_request());
            }
            let mut normalized = Map::new();
            normalized.insert("type".to_owned(), json!("function"));
            normalized.insert("name".to_owned(), Value::String(name.to_owned()));
            normalized.insert(
                "description".to_owned(),
                object
                    .get("description")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .unwrap_or_else(|| json!("")),
            );
            normalized.insert(
                "parameters".to_owned(),
                object
                    .get("parameters")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .unwrap_or_else(|| json!({})),
            );
            normalized.insert(
                "strict".to_owned(),
                object
                    .get("strict")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .unwrap_or_else(|| json!(false)),
            );
            if object
                .get("defer_loading")
                .and_then(Value::as_bool)
                .is_some_and(|value| value)
            {
                normalized.insert("defer_loading".to_owned(), json!(true));
            }
            Ok(Value::Object(normalized))
        }
        "web_search"
        | "web_search_preview"
        | "web_search_preview_2025_03_11"
        | "web_search_2025_08_26" => {
            const ALLOWED: &[&str] = &[
                "type",
                "search_context_size",
                "search_content_types",
                "user_location",
                "external_web_access",
                "filters",
            ];
            if object.keys().any(|key| !ALLOWED.contains(&key.as_str())) {
                return Err(ApiError::invalid_request());
            }
            if object.get("search_context_size").is_some_and(|value| {
                !value.is_null() && !matches!(value.as_str(), Some("low" | "medium" | "high"))
            }) {
                return Err(ApiError::invalid_request());
            }
            if object.get("search_content_types").is_some_and(|value| {
                !value.is_null()
                    && (!value.is_array()
                        || value.as_array().is_some_and(|items| {
                            items
                                .iter()
                                .any(|item| !matches!(item.as_str(), Some("text" | "image")))
                        }))
            }) {
                return Err(ApiError::invalid_request());
            }
            if object
                .get("external_web_access")
                .is_some_and(|value| !value.is_null() && !value.is_boolean())
                || object.get("filters").is_some_and(|value| {
                    !value.is_null()
                        && (!value.is_object()
                            || value.as_object().is_some_and(|filters| {
                                filters.keys().any(|key| key != "allowed_domains")
                                    || filters.get("allowed_domains").is_some_and(|domains| {
                                        !domains.is_array()
                                            || domains.as_array().is_some_and(|items| {
                                                items.len() > 100
                                                    || items.iter().any(|item| {
                                                        !item.as_str().is_some_and(|domain| {
                                                            !domain.is_empty()
                                                                && domain == domain.trim()
                                                                && !domain.contains("://")
                                                                && !domain.chars().any(|ch| {
                                                                    matches!(
                                                                        ch,
                                                                        '/' | '?' | '#' | '@'
                                                                    )
                                                                })
                                                        })
                                                    })
                                            })
                                    })
                            }))
                })
            {
                return Err(ApiError::invalid_request());
            }
            if object.get("user_location").is_some_and(|value| {
                if value.is_null() {
                    return false;
                }
                let Some(location) = value.as_object() else {
                    return true;
                };
                if location.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "type" | "city" | "country" | "region" | "timezone"
                    )
                }) || location
                    .get("type")
                    .is_some_and(|value| value != "approximate")
                    || location
                        .iter()
                        .any(|(key, value)| key != "type" && !value.is_string())
                {
                    return true;
                }
                location.get("country").is_some_and(|value| {
                    !value.as_str().is_some_and(|country| {
                        country.len() == 2
                            && country.is_ascii()
                            && country
                                .chars()
                                .all(|character| character.is_ascii_alphabetic())
                    })
                })
            }) {
                return Err(ApiError::invalid_request());
            }
            let mut normalized = Map::new();
            normalized.insert("type".to_owned(), json!("web_search"));
            for key in [
                "search_context_size",
                "search_content_types",
                "user_location",
                "external_web_access",
                "filters",
            ] {
                if let Some(value) = object.get(key).filter(|value| !value.is_null()) {
                    normalized.insert(key.to_owned(), value.clone());
                }
            }
            Ok(Value::Object(normalized))
        }
        _ => Err(ApiError::invalid_request()),
    }
}

pub(crate) fn native_codex_input(value: &Value) -> Result<Value, ApiError> {
    let Value::Array(items) = value else {
        return native_responses_text_input(value);
    };
    if items.iter().all(|item| {
        item.as_object()
            .and_then(|object| object.get("role"))
            .is_some()
    }) {
        return native_responses_text_input(value);
    }
    let mut normalized = Vec::with_capacity(items.len());
    for item in items {
        let object = item.as_object().ok_or_else(ApiError::invalid_request)?;
        let item_type = object
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(ApiError::invalid_request)?;
        let allowed: &[&str] = match item_type {
            "function_call" => &["type", "id", "call_id", "name", "arguments", "status"],
            "function_call_output" => &["type", "call_id", "output", "status"],
            "web_search_call" => &["type", "id", "status", "action"],
            "message" => &["type", "id", "role", "content", "status"],
            _ => return Err(ApiError::invalid_request()),
        };
        if object.keys().any(|key| !allowed.contains(&key.as_str())) {
            return Err(ApiError::invalid_request());
        }
        if matches!(item_type, "function_call" | "function_call_output")
            && object
                .get("call_id")
                .and_then(Value::as_str)
                .is_none_or(|value| value.is_empty())
        {
            return Err(ApiError::invalid_request());
        }
        normalized.push(item.clone());
    }
    Ok(Value::Array(normalized))
}

pub(crate) fn native_codex_responses_payload(
    object: &Map<String, Value>,
) -> Result<Value, ApiError> {
    const ALLOWED_FIELDS: &[&str] = &[
        "model",
        "input",
        "instructions",
        "stream",
        "store",
        "parallel_tool_calls",
        "tool_choice",
        "reasoning",
        "tools",
    ];
    if object
        .iter()
        .any(|(key, value)| !value.is_null() && !ALLOWED_FIELDS.contains(&key.as_str()))
    {
        return Err(ApiError::invalid_request());
    }
    if object
        .get("store")
        .is_some_and(|value| !value.is_null() && value != &Value::Bool(false))
        || object
            .get("parallel_tool_calls")
            .is_some_and(|value| !value.is_null() && !value.is_boolean())
    {
        return Err(ApiError::invalid_request());
    }
    if object
        .get("tool_choice")
        .is_some_and(|value| !value.is_null() && value.as_str() != Some("auto"))
    {
        return Err(ApiError::invalid_request());
    }
    if object.get("reasoning").is_some_and(|value| {
        !value.is_null()
            && (!value.is_object()
                || value.as_object().is_some_and(|reasoning| {
                    reasoning.keys().any(|key| key != "effort")
                        || reasoning
                            .get("effort")
                            .is_some_and(|effort| !effort.is_null() && !effort.is_string())
                }))
    }) {
        return Err(ApiError::invalid_request());
    }
    let input = native_codex_input(object.get("input").ok_or_else(ApiError::invalid_request)?)?;
    let mut payload = json!({
        "model": object
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| *model != "auto")
            .map_or_else(|| json!(CODEX_RESPONSES_MODEL), |model| json!(model)),
        "input": input,
        "instructions": object.get("instructions").cloned().unwrap_or(Value::Null),
        "stream": object.get("stream").and_then(Value::as_bool).unwrap_or(false),
        "store": false,
        "tool_choice": "auto",
        "parallel_tool_calls": object
            .get("parallel_tool_calls")
            .and_then(Value::as_bool)
            .unwrap_or(true),
    });
    if let Some(reasoning) = object.get("reasoning").filter(|value| !value.is_null()) {
        payload["reasoning"] = reasoning.clone();
    }
    if let Some(tools) = object.get("tools").filter(|value| !value.is_null()) {
        let tools = tools.as_array().ok_or_else(ApiError::invalid_request)?;
        payload["tools"] = Value::Array(
            tools
                .iter()
                .map(native_codex_tool)
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    Ok(payload)
}
