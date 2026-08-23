use super::protocol_responses::{
    native_responses_text_input, normalize_response_content_part, normalize_response_message,
    response_content_part_type,
};
use super::{ApiError, CODEX_RESPONSES_MODEL};
use serde_json::{Map, Value, json};

pub(crate) fn native_codex_tool(value: &Value) -> Result<Value, ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    let wrapper = object.get("function").is_some();
    let normalized_wrapper = if wrapper {
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "type" | "function"))
            || object.get("type").and_then(Value::as_str) != Some("function")
        {
            return Err(ApiError::invalid_request());
        }
        let function = object
            .get("function")
            .and_then(Value::as_object)
            .ok_or_else(ApiError::invalid_request)?;
        let mut normalized = function.clone();
        normalized.insert("type".to_owned(), json!("function"));
        Some(normalized)
    } else {
        None
    };
    let object = normalized_wrapper.as_ref().unwrap_or(object);
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
        "image_generation" => {
            const ALLOWED: &[&str] = &[
                "type",
                "action",
                "background",
                "input_fidelity",
                "input_image_mask",
                "model",
                "moderation",
                "output_compression",
                "output_format",
                "partial_images",
                "quality",
                "size",
            ];
            if object.keys().any(|key| !ALLOWED.contains(&key.as_str())) {
                return Err(ApiError::invalid_request());
            }
            if object.get("action").is_some_and(|value| {
                !value.is_null() && !matches!(value.as_str(), Some("auto" | "generate" | "edit"))
            }) || object
                .get("input_fidelity")
                .is_some_and(|value| !value.is_null())
                || object
                    .get("input_image_mask")
                    .is_some_and(|value| !value.is_null())
            {
                return Err(ApiError::invalid_request());
            }
            for field in [
                "background",
                "moderation",
                "output_format",
                "quality",
                "size",
            ] {
                if object
                    .get(field)
                    .is_some_and(|value| !value.is_null() && !value.is_string())
                {
                    return Err(ApiError::invalid_request());
                }
            }
            if object.get("model").is_some_and(|value| {
                !value.is_null()
                    && (!value.is_string()
                        || value.as_str().is_some_and(|model| model.trim().is_empty()))
            }) || object.get("output_compression").is_some_and(|value| {
                !value.is_null() && (value.as_u64().is_none_or(|compression| compression > 100))
            }) || object.get("partial_images").is_some_and(|value| {
                !value.is_null() && value.as_u64().is_none_or(|count| count > 3)
            }) {
                return Err(ApiError::invalid_request());
            }
            let mut normalized = Map::new();
            normalized.insert("type".to_owned(), json!("image_generation"));
            normalized.insert(
                "action".to_owned(),
                object
                    .get("action")
                    .filter(|value| !value.is_null())
                    .cloned()
                    .unwrap_or_else(|| json!("auto")),
            );
            for field in [
                "background",
                "model",
                "moderation",
                "output_compression",
                "output_format",
                "partial_images",
                "quality",
                "size",
            ] {
                if let Some(value) = object.get(field).filter(|value| !value.is_null()) {
                    normalized.insert(field.to_owned(), value.clone());
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
    if items.is_empty() {
        return Ok(Value::Array(Vec::new()));
    }
    if items.iter().all(|item| {
        item.as_object()
            .is_some_and(|object| object.get("role").is_some())
    }) {
        return native_responses_text_input(value);
    }
    let mut normalized = Vec::with_capacity(items.len());
    let mut pending_parts = Vec::new();
    for item in items {
        if response_content_part_type(item).is_some() {
            pending_parts.push(normalize_response_content_part(item, "user")?);
            continue;
        }
        if !pending_parts.is_empty() {
            normalized.push(json!({
                "type": "message",
                "role": "user",
                "content": std::mem::take(&mut pending_parts),
            }));
        }
        let object = item.as_object().ok_or_else(ApiError::invalid_request)?;
        if object.get("role").is_some()
            || object.get("type").and_then(Value::as_str) == Some("message")
        {
            normalized.push(normalize_response_message(item)?);
            continue;
        }
        normalized.push(normalize_native_history_item(item)?);
    }
    if !pending_parts.is_empty() {
        normalized.push(json!({
            "type": "message",
            "role": "user",
            "content": pending_parts,
        }));
    }
    if normalized.is_empty() {
        return Err(ApiError::invalid_request());
    }
    Ok(Value::Array(normalized))
}

fn normalize_native_history_item(value: &Value) -> Result<Value, ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    let item_type = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::invalid_request)?;
    let allowed: &[&str] = match item_type {
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
        "web_search_call" => &["type", "id", "status", "action"],
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
        "tool_search_call" => &["type", "id", "call_id", "status", "execution", "arguments"],
        "tool_search_output" => &["type", "id", "call_id", "status", "execution", "tools"],
        "mcp_tool_call_output" => &["type", "call_id", "output"],
        _ => return Err(ApiError::invalid_request()),
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(ApiError::invalid_request());
    }
    if matches!(item_type, "function_call" | "function_call_output")
        && object
            .get("call_id")
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ApiError::invalid_request());
    }
    if item_type == "function_call"
        && (object
            .get("name")
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
            || !object.get("arguments").is_some_and(Value::is_string))
    {
        return Err(ApiError::invalid_request());
    }
    if item_type == "function_call_output" && !object.contains_key("output") {
        return Err(ApiError::invalid_request());
    }
    let mut normalized = object.clone();
    if matches!(
        item_type,
        "function_call" | "function_call_output" | "reasoning" | "message"
    ) {
        normalized.remove("status");
    }
    Ok(Value::Object(normalized))
}

pub(crate) fn native_codex_responses_payload(
    object: &Map<String, Value>,
) -> Result<Value, ApiError> {
    const ALLOWED_FIELDS: &[&str] = &[
        "include",
        "model",
        "input",
        "instructions",
        "context_management",
        "stream",
        "stream_options",
        "max_output_tokens",
        "prompt_cache_key",
        "service_tier",
        "store",
        "parallel_tool_calls",
        "tool_choice",
        "reasoning",
        "text",
        "tools",
    ];
    if object
        .iter()
        .any(|(key, value)| !value.is_null() && !ALLOWED_FIELDS.contains(&key.as_str()))
    {
        return Err(ApiError::invalid_request());
    }
    if object.get("max_output_tokens").is_some_and(|value| {
        value
            .as_u64()
            .is_none_or(|tokens| tokens == 0 || tokens > 1_000_000)
    }) {
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
    if object.get("tool_choice").is_some_and(|value| {
        !value.is_null()
            && !matches!(
                value,
                Value::String(choice) if choice == "auto"
            )
            && !matches!(
                value,
                Value::Object(choice)
                    if choice.len() == 1
                        && matches!(
                            choice.get("type").and_then(Value::as_str),
                            Some("auto" | "image_generation")
                        )
            )
    }) {
        return Err(ApiError::invalid_request());
    }
    if let Some(reasoning) = object.get("reasoning").filter(|value| !value.is_null()) {
        let reasoning = reasoning
            .as_object()
            .ok_or_else(ApiError::invalid_request)?;
        if reasoning
            .keys()
            .any(|key| !matches!(key.as_str(), "effort" | "summary" | "context"))
            || reasoning.values().any(|value| !value.is_string())
        {
            return Err(ApiError::invalid_request());
        }
        if reasoning.get("summary").is_some_and(|value| {
            !matches!(
                value.as_str(),
                Some("auto" | "concise" | "detailed" | "none")
            )
        }) || reasoning.get("context").is_some_and(|value| {
            !matches!(value.as_str(), Some("auto" | "current_turn" | "all_turns"))
        }) {
            return Err(ApiError::invalid_request());
        }
    }
    if object
        .get("prompt_cache_key")
        .is_some_and(|value| !value.is_null() && !value.is_string())
        || object.get("service_tier").is_some_and(|value| {
            !value.is_null() && !matches!(value.as_str(), Some("default" | "priority" | "flex"))
        })
    {
        return Err(ApiError::invalid_request());
    }
    if let Some(options) = object
        .get("stream_options")
        .filter(|value| !value.is_null())
    {
        let options = options.as_object().ok_or_else(ApiError::invalid_request)?;
        if object.get("stream").and_then(Value::as_bool) != Some(true)
            || options.keys().any(|key| {
                !matches!(
                    key.as_str(),
                    "include_usage" | "include_obfuscation" | "reasoning_summary_delivery"
                )
            })
            || options
                .get("include_usage")
                .is_some_and(|value| !value.is_boolean())
            || options
                .get("include_obfuscation")
                .is_some_and(|value| value != &Value::Bool(false))
            || options
                .get("reasoning_summary_delivery")
                .is_some_and(|value| value.as_str() != Some("sequential_cutoff"))
        {
            return Err(ApiError::invalid_request());
        }
    }
    let input = native_codex_input(object.get("input").ok_or_else(ApiError::invalid_request)?)?;
    let mut payload = json!({
        "model": object
            .get("model")
            .and_then(Value::as_str)
            .filter(|model| *model != "auto")
            .map_or_else(|| json!(CODEX_RESPONSES_MODEL), |model| json!(model)),
        "input": input,
        "instructions": object
            .get("instructions")
            .cloned()
            .unwrap_or_else(|| json!("")),
        "stream": true,
        "max_output_tokens": object
            .get("max_output_tokens")
            .cloned()
            .unwrap_or(Value::Null),
        "store": false,
        "tool_choice": object
            .get("tool_choice")
            .filter(|value| !value.is_null())
            .cloned()
            .unwrap_or_else(|| json!("auto")),
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
    payload["include"] = normalize_include(object.get("include"))?;
    if let Some(context_management) = object
        .get("context_management")
        .filter(|value| !value.is_null())
    {
        payload["context_management"] = normalize_context_management(context_management)?;
    }
    if let Some(prompt_cache_key) = object
        .get("prompt_cache_key")
        .filter(|value| !value.is_null())
    {
        payload["prompt_cache_key"] = prompt_cache_key.clone();
    }
    if let Some(service_tier) = object.get("service_tier").filter(|value| !value.is_null()) {
        payload["service_tier"] = service_tier.clone();
    }
    if let Some(text) = object.get("text").filter(|value| !value.is_null()) {
        payload["text"] = normalize_text_controls(text)?;
    }
    if let Some(stream_options) = object
        .get("stream_options")
        .filter(|value| !value.is_null())
    {
        payload["stream_options"] = stream_options.clone();
    }
    Ok(payload)
}

fn normalize_include(value: Option<&Value>) -> Result<Value, ApiError> {
    let mut include = vec!["reasoning.encrypted_content".to_owned()];
    let Some(value) = value.filter(|value| !value.is_null()) else {
        return Ok(Value::Array(
            include.into_iter().map(Value::String).collect(),
        ));
    };
    let values = value.as_array().ok_or_else(ApiError::invalid_request)?;
    for item in values {
        let item = item.as_str().ok_or_else(ApiError::invalid_request)?;
        if !matches!(
            item,
            "reasoning.encrypted_content"
                | "web_search_call.action.sources"
                | "web_search_call.results"
        ) {
            return Err(ApiError::invalid_request());
        }
        if !include.iter().any(|existing| existing == item) {
            include.push(item.to_owned());
        }
    }
    Ok(Value::Array(
        include.into_iter().map(Value::String).collect(),
    ))
}

fn normalize_context_management(value: &Value) -> Result<Value, ApiError> {
    let entries = value.as_array().ok_or_else(ApiError::invalid_request)?;
    let mut normalized = Vec::with_capacity(entries.len());
    for entry in entries {
        let object = entry.as_object().ok_or_else(ApiError::invalid_request)?;
        if object
            .keys()
            .any(|key| !matches!(key.as_str(), "type" | "compact_threshold"))
            || object.get("type").and_then(Value::as_str) != Some("compaction")
        {
            return Err(ApiError::invalid_request());
        }
        if object.get("compact_threshold").is_some_and(|threshold| {
            threshold
                .as_f64()
                .is_none_or(|value| !value.is_finite() || value < 1000.0)
        }) {
            return Err(ApiError::invalid_request());
        }
        normalized.push(Value::Object(object.clone()));
    }
    Ok(Value::Array(normalized))
}

fn normalize_text_controls(value: &Value) -> Result<Value, ApiError> {
    let object = value.as_object().ok_or_else(ApiError::invalid_request)?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "verbosity" | "format"))
    {
        return Err(ApiError::invalid_request());
    }
    if object
        .get("verbosity")
        .is_some_and(|verbosity| !matches!(verbosity.as_str(), Some("low" | "medium" | "high")))
    {
        return Err(ApiError::invalid_request());
    }
    let mut normalized = Map::new();
    if let Some(verbosity) = object.get("verbosity") {
        normalized.insert("verbosity".to_owned(), verbosity.clone());
    }
    if let Some(format) = object.get("format").filter(|value| !value.is_null()) {
        let format = format.as_object().ok_or_else(ApiError::invalid_request)?;
        match format.get("type").and_then(Value::as_str) {
            Some("text") => {
                if format.len() != 1 {
                    return Err(ApiError::invalid_request());
                }
                normalized.insert("format".to_owned(), json!({"type":"text"}));
            }
            Some("json_schema") => {
                if format
                    .keys()
                    .any(|key| !matches!(key.as_str(), "type" | "name" | "schema" | "strict"))
                    || !format
                        .get("name")
                        .and_then(Value::as_str)
                        .is_some_and(|name| !name.is_empty() && name.len() <= 64)
                    || !format.get("schema").is_some_and(Value::is_object)
                    || format
                        .get("strict")
                        .is_some_and(|value| !value.is_boolean())
                {
                    return Err(ApiError::invalid_request());
                }
                normalized.insert("format".to_owned(), Value::Object(format.clone()));
            }
            _ => return Err(ApiError::invalid_request()),
        }
    }
    Ok(Value::Object(normalized))
}
