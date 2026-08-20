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
        let event_type = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(ApiError::upstream)?;
        if terminal {
            return Err(ApiError::upstream());
        }
        let rule = response_event_rule(event_type).ok_or_else(ApiError::upstream)?;
        validate_response_event(&value, event_type, rule)?;
        match event_type {
            "response.created" | "response.in_progress" => {
                let response = value
                    .get("response")
                    .and_then(Value::as_object)
                    .ok_or_else(ApiError::upstream)?;
                if let Some(id) = response.get("id").and_then(Value::as_str) {
                    response_id = Some(id.to_owned());
                }
            }
            "response.output_text.delta" => {
                text.push_str(
                    value
                        .get("delta")
                        .and_then(Value::as_str)
                        .ok_or_else(ApiError::upstream)?,
                );
            }
            "response.completed" => {
                let response = value
                    .get("response")
                    .and_then(Value::as_object)
                    .ok_or_else(ApiError::upstream)?;
                if let Some(output) = response.get("output") {
                    for item in output.as_array().ok_or_else(ApiError::upstream)? {
                        validate_output_item(item)?;
                    }
                }
                terminal = true;
                completed_response = Some(Value::Object(response.clone()));
            }
            "response.failed" | "response.incomplete" | "error" => {
                return Err(ApiError::upstream());
            }
            "response.output_item.added" | "response.output_item.done" => {
                validate_output_item(value.get("item").ok_or_else(ApiError::upstream)?)?;
            }
            "response.metadata"
            | "codex.response.metadata"
            | "responsesapi.websocket_timing"
            | "response.output_text.done"
            | "response.content_part.added"
            | "response.content_part.done"
            | "response.refusal.delta"
            | "response.refusal.done"
            | "response.function_call_arguments.delta"
            | "response.function_call_arguments.done"
            | "response.queued"
            | "response.file_search_call.in_progress"
            | "response.file_search_call.searching"
            | "response.file_search_call.completed"
            | "response.web_search_call.in_progress"
            | "response.web_search_call.searching"
            | "response.web_search_call.completed"
            | "response.reasoning_summary_part.added"
            | "response.reasoning_summary_part.done"
            | "response.reasoning_summary_text.delta"
            | "response.reasoning_summary_text.done"
            | "response.reasoning_text.delta"
            | "response.reasoning_text.done"
            | "response.image_generation_call.in_progress"
            | "response.image_generation_call.generating"
            | "response.image_generation_call.completed"
            | "response.image_generation_call.partial_image"
            | "response.mcp_call_arguments.delta"
            | "response.mcp_call_arguments.done"
            | "response.mcp_call.in_progress"
            | "response.mcp_call.completed"
            | "response.mcp_call.failed"
            | "response.mcp_list_tools.in_progress"
            | "response.mcp_list_tools.completed"
            | "response.mcp_list_tools.failed"
            | "response.code_interpreter_call.in_progress"
            | "response.code_interpreter_call.interpreting"
            | "response.code_interpreter_call.completed"
            | "response.code_interpreter_call_code.delta"
            | "response.code_interpreter_call_code.done"
            | "response.output_text.annotation.added"
            | "response.custom_tool_call_input.delta"
            | "response.custom_tool_call_input.done"
            | "response.audio.delta"
            | "response.audio.done"
            | "response.audio.transcript.delta"
            | "response.audio.transcript.done"
            | "response.shell_call_command.added"
            | "response.shell_call_command.delta"
            | "response.shell_call_command.done"
            | "response.shell_call_output_content.delta"
            | "response.shell_call_output_content.done" => {}
            _ => unreachable!("response event table and side effects diverged"),
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

#[derive(Clone, Copy)]
enum FieldKind {
    Any,
    String,
    Integer,
    Bool,
    Object,
    Array,
}

#[derive(Clone, Copy)]
struct FieldRule {
    name: &'static str,
    kind: FieldKind,
}

const fn field(name: &'static str, kind: FieldKind) -> FieldRule {
    FieldRule { name, kind }
}

struct EventRule {
    name: &'static str,
    fields: &'static [FieldRule],
}

const RESPONSE_FIELDS: &[FieldRule] = &[
    field("response", FieldKind::Object),
    field("sequence_number", FieldKind::Integer),
];
const OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("item", FieldKind::Object),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const CONTENT_PART_FIELDS: &[FieldRule] = &[
    field("content_index", FieldKind::Integer),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("part", FieldKind::Object),
    field("sequence_number", FieldKind::Integer),
];
const OUTPUT_TEXT_DELTA_FIELDS: &[FieldRule] = &[
    field("content_index", FieldKind::Integer),
    field("delta", FieldKind::String),
    field("item_id", FieldKind::String),
    field("logprobs", FieldKind::Array),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const OUTPUT_TEXT_DONE_FIELDS: &[FieldRule] = &[
    field("content_index", FieldKind::Integer),
    field("item_id", FieldKind::String),
    field("logprobs", FieldKind::Array),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
    field("text", FieldKind::String),
];
const REFUSAL_DELTA_FIELDS: &[FieldRule] = &[
    field("content_index", FieldKind::Integer),
    field("delta", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const REFUSAL_DONE_FIELDS: &[FieldRule] = &[
    field("content_index", FieldKind::Integer),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("refusal", FieldKind::String),
    field("sequence_number", FieldKind::Integer),
];
const FUNCTION_ARGUMENTS_DELTA_FIELDS: &[FieldRule] = &[
    field("delta", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const FUNCTION_ARGUMENTS_DONE_FIELDS: &[FieldRule] = &[
    field("arguments", FieldKind::String),
    field("item_id", FieldKind::String),
    field("name", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const ITEM_LIFECYCLE_FIELDS: &[FieldRule] = &[
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const REASONING_SUMMARY_PART_FIELDS: &[FieldRule] = &[
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("part", FieldKind::Object),
    field("sequence_number", FieldKind::Integer),
    field("summary_index", FieldKind::Integer),
];
const REASONING_SUMMARY_TEXT_DELTA_FIELDS: &[FieldRule] = &[
    field("delta", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
    field("summary_index", FieldKind::Integer),
];
const REASONING_SUMMARY_TEXT_DONE_FIELDS: &[FieldRule] = &[
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
    field("summary_index", FieldKind::Integer),
    field("text", FieldKind::String),
];
const REASONING_TEXT_DELTA_FIELDS: &[FieldRule] = &[
    field("content_index", FieldKind::Integer),
    field("delta", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const REASONING_TEXT_DONE_FIELDS: &[FieldRule] = &[
    field("content_index", FieldKind::Integer),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
    field("text", FieldKind::String),
];
const PARTIAL_IMAGE_FIELDS: &[FieldRule] = &[
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("partial_image_b64", FieldKind::String),
    field("partial_image_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const CODE_DELTA_FIELDS: &[FieldRule] = &[
    field("delta", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const CODE_DONE_FIELDS: &[FieldRule] = &[
    field("code", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const ANNOTATION_FIELDS: &[FieldRule] = &[
    field("annotation", FieldKind::Object),
    field("annotation_index", FieldKind::Integer),
    field("content_index", FieldKind::Integer),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const MCP_ARGUMENTS_DELTA_FIELDS: &[FieldRule] = &[
    field("delta", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const MCP_ARGUMENTS_DONE_FIELDS: &[FieldRule] = &[
    field("arguments", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const CUSTOM_TOOL_INPUT_DONE_FIELDS: &[FieldRule] = &[
    field("input", FieldKind::String),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const SHELL_COMMAND_ADDED_FIELDS: &[FieldRule] = &[
    field("command", FieldKind::String),
    field("command_index", FieldKind::Integer),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const SHELL_COMMAND_DELTA_FIELDS: &[FieldRule] = &[
    field("command_index", FieldKind::Integer),
    field("delta", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const SHELL_COMMAND_DONE_FIELDS: &[FieldRule] = &[
    field("command", FieldKind::String),
    field("command_index", FieldKind::Integer),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const SHELL_OUTPUT_DELTA_FIELDS: &[FieldRule] = &[
    field("command_index", FieldKind::Integer),
    field("delta", FieldKind::Object),
    field("item_id", FieldKind::String),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const SHELL_OUTPUT_DONE_FIELDS: &[FieldRule] = &[
    field("command_index", FieldKind::Integer),
    field("item_id", FieldKind::String),
    field("output", FieldKind::Array),
    field("output_index", FieldKind::Integer),
    field("sequence_number", FieldKind::Integer),
];
const ERROR_FIELDS: &[FieldRule] = &[
    field("code", FieldKind::String),
    field("message", FieldKind::String),
    field("param", FieldKind::String),
    field("sequence_number", FieldKind::Integer),
];

const EVENT_RULES: &[EventRule] = &[
    EventRule {
        name: "response.created",
        fields: RESPONSE_FIELDS,
    },
    EventRule {
        name: "response.in_progress",
        fields: RESPONSE_FIELDS,
    },
    EventRule {
        name: "response.completed",
        fields: RESPONSE_FIELDS,
    },
    EventRule {
        name: "response.failed",
        fields: RESPONSE_FIELDS,
    },
    EventRule {
        name: "response.incomplete",
        fields: RESPONSE_FIELDS,
    },
    EventRule {
        name: "response.queued",
        fields: RESPONSE_FIELDS,
    },
    EventRule {
        name: "response.output_item.added",
        fields: OUTPUT_ITEM_FIELDS,
    },
    EventRule {
        name: "response.output_item.done",
        fields: OUTPUT_ITEM_FIELDS,
    },
    EventRule {
        name: "response.content_part.added",
        fields: CONTENT_PART_FIELDS,
    },
    EventRule {
        name: "response.content_part.done",
        fields: CONTENT_PART_FIELDS,
    },
    EventRule {
        name: "response.output_text.delta",
        fields: OUTPUT_TEXT_DELTA_FIELDS,
    },
    EventRule {
        name: "response.output_text.done",
        fields: OUTPUT_TEXT_DONE_FIELDS,
    },
    EventRule {
        name: "response.refusal.delta",
        fields: REFUSAL_DELTA_FIELDS,
    },
    EventRule {
        name: "response.refusal.done",
        fields: REFUSAL_DONE_FIELDS,
    },
    EventRule {
        name: "response.function_call_arguments.delta",
        fields: FUNCTION_ARGUMENTS_DELTA_FIELDS,
    },
    EventRule {
        name: "response.function_call_arguments.done",
        fields: FUNCTION_ARGUMENTS_DONE_FIELDS,
    },
    EventRule {
        name: "response.file_search_call.in_progress",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.file_search_call.searching",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.file_search_call.completed",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.web_search_call.in_progress",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.web_search_call.searching",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.web_search_call.completed",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.reasoning_summary_part.added",
        fields: REASONING_SUMMARY_PART_FIELDS,
    },
    EventRule {
        name: "response.reasoning_summary_part.done",
        fields: REASONING_SUMMARY_PART_FIELDS,
    },
    EventRule {
        name: "response.reasoning_summary_text.delta",
        fields: REASONING_SUMMARY_TEXT_DELTA_FIELDS,
    },
    EventRule {
        name: "response.reasoning_summary_text.done",
        fields: REASONING_SUMMARY_TEXT_DONE_FIELDS,
    },
    EventRule {
        name: "response.reasoning_text.delta",
        fields: REASONING_TEXT_DELTA_FIELDS,
    },
    EventRule {
        name: "response.reasoning_text.done",
        fields: REASONING_TEXT_DONE_FIELDS,
    },
    EventRule {
        name: "response.image_generation_call.in_progress",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.image_generation_call.generating",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.image_generation_call.completed",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.image_generation_call.partial_image",
        fields: PARTIAL_IMAGE_FIELDS,
    },
    EventRule {
        name: "response.mcp_call_arguments.delta",
        fields: MCP_ARGUMENTS_DELTA_FIELDS,
    },
    EventRule {
        name: "response.mcp_call_arguments.done",
        fields: MCP_ARGUMENTS_DONE_FIELDS,
    },
    EventRule {
        name: "response.mcp_call.in_progress",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.mcp_call.completed",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.mcp_call.failed",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.mcp_list_tools.in_progress",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.mcp_list_tools.completed",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.mcp_list_tools.failed",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.code_interpreter_call.in_progress",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.code_interpreter_call.interpreting",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.code_interpreter_call.completed",
        fields: ITEM_LIFECYCLE_FIELDS,
    },
    EventRule {
        name: "response.code_interpreter_call_code.delta",
        fields: CODE_DELTA_FIELDS,
    },
    EventRule {
        name: "response.code_interpreter_call_code.done",
        fields: CODE_DONE_FIELDS,
    },
    EventRule {
        name: "response.output_text.annotation.added",
        fields: ANNOTATION_FIELDS,
    },
    EventRule {
        name: "response.custom_tool_call_input.delta",
        fields: FUNCTION_ARGUMENTS_DELTA_FIELDS,
    },
    EventRule {
        name: "response.custom_tool_call_input.done",
        fields: CUSTOM_TOOL_INPUT_DONE_FIELDS,
    },
    EventRule {
        name: "response.audio.delta",
        fields: &[
            field("delta", FieldKind::String),
            field("sequence_number", FieldKind::Integer),
        ],
    },
    EventRule {
        name: "response.audio.done",
        fields: &[field("sequence_number", FieldKind::Integer)],
    },
    EventRule {
        name: "response.audio.transcript.delta",
        fields: &[
            field("delta", FieldKind::String),
            field("sequence_number", FieldKind::Integer),
        ],
    },
    EventRule {
        name: "response.audio.transcript.done",
        fields: &[field("sequence_number", FieldKind::Integer)],
    },
    EventRule {
        name: "response.shell_call_command.added",
        fields: SHELL_COMMAND_ADDED_FIELDS,
    },
    EventRule {
        name: "response.shell_call_command.delta",
        fields: SHELL_COMMAND_DELTA_FIELDS,
    },
    EventRule {
        name: "response.shell_call_command.done",
        fields: SHELL_COMMAND_DONE_FIELDS,
    },
    EventRule {
        name: "response.shell_call_output_content.delta",
        fields: SHELL_OUTPUT_DELTA_FIELDS,
    },
    EventRule {
        name: "response.shell_call_output_content.done",
        fields: SHELL_OUTPUT_DONE_FIELDS,
    },
    EventRule {
        name: "error",
        fields: ERROR_FIELDS,
    },
    EventRule {
        name: "response.metadata",
        fields: &[],
    },
    EventRule {
        name: "codex.response.metadata",
        fields: &[],
    },
    EventRule {
        name: "responsesapi.websocket_timing",
        fields: &[],
    },
];

struct OutputItemRule {
    name: &'static str,
    fields: &'static [FieldRule],
}

const MESSAGE_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("content", FieldKind::Array),
    field("role", FieldKind::String),
    field("status", FieldKind::String),
];
const FILE_SEARCH_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("queries", FieldKind::Array),
    field("status", FieldKind::String),
];
const FUNCTION_CALL_ITEM_FIELDS: &[FieldRule] = &[
    field("arguments", FieldKind::String),
    field("call_id", FieldKind::String),
    field("name", FieldKind::String),
];
const FUNCTION_CALL_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("output", FieldKind::Any),
    field("status", FieldKind::String),
];
const WEB_SEARCH_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("action", FieldKind::Object),
    field("status", FieldKind::String),
];
const COMPUTER_CALL_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("pending_safety_checks", FieldKind::Array),
    field("status", FieldKind::String),
];
const COMPUTER_CALL_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("output", FieldKind::Object),
    field("status", FieldKind::String),
];
const REASONING_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("summary", FieldKind::Array),
];
const PROGRAM_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("code", FieldKind::String),
    field("fingerprint", FieldKind::String),
];
const PROGRAM_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("result", FieldKind::String),
    field("status", FieldKind::String),
];
const TOOL_SEARCH_CALL_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("arguments", FieldKind::Any),
    field("call_id", FieldKind::String),
    field("execution", FieldKind::String),
    field("status", FieldKind::String),
];
const TOOL_SEARCH_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("execution", FieldKind::String),
    field("status", FieldKind::String),
    field("tools", FieldKind::Array),
];
const ADDITIONAL_TOOLS_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("role", FieldKind::String),
    field("tools", FieldKind::Array),
];
const COMPACTION_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("encrypted_content", FieldKind::String),
];
const IMAGE_GENERATION_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("result", FieldKind::String),
    field("status", FieldKind::String),
];
const CODE_INTERPRETER_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("code", FieldKind::String),
    field("container_id", FieldKind::String),
    field("outputs", FieldKind::Array),
    field("status", FieldKind::String),
];
const LOCAL_SHELL_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("action", FieldKind::Object),
    field("call_id", FieldKind::String),
    field("status", FieldKind::String),
];
const LOCAL_SHELL_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("output", FieldKind::String),
];
const SHELL_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("action", FieldKind::Object),
    field("call_id", FieldKind::String),
    field("environment", FieldKind::Any),
    field("status", FieldKind::String),
];
const SHELL_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("max_output_length", FieldKind::Integer),
    field("output", FieldKind::Array),
    field("status", FieldKind::String),
];
const APPLY_PATCH_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("operation", FieldKind::Any),
    field("status", FieldKind::String),
];
const APPLY_PATCH_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("status", FieldKind::String),
];
const MCP_CALL_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("arguments", FieldKind::String),
    field("name", FieldKind::String),
    field("server_label", FieldKind::String),
];
const MCP_LIST_TOOLS_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("server_label", FieldKind::String),
    field("tools", FieldKind::Array),
];
const MCP_APPROVAL_REQUEST_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("arguments", FieldKind::String),
    field("name", FieldKind::String),
    field("server_label", FieldKind::String),
];
const MCP_APPROVAL_RESPONSE_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("approval_request_id", FieldKind::String),
    field("approve", FieldKind::Bool),
];
const CUSTOM_TOOL_CALL_ITEM_FIELDS: &[FieldRule] = &[
    field("call_id", FieldKind::String),
    field("input", FieldKind::String),
    field("name", FieldKind::String),
];
const CUSTOM_TOOL_CALL_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("id", FieldKind::String),
    field("call_id", FieldKind::String),
    field("output", FieldKind::Any),
    field("status", FieldKind::String),
];

const OUTPUT_ITEM_RULES: &[OutputItemRule] = &[
    OutputItemRule {
        name: "message",
        fields: MESSAGE_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "file_search_call",
        fields: FILE_SEARCH_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "function_call",
        fields: FUNCTION_CALL_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "function_call_output",
        fields: FUNCTION_CALL_OUTPUT_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "web_search_call",
        fields: WEB_SEARCH_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "computer_call",
        fields: COMPUTER_CALL_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "computer_call_output",
        fields: COMPUTER_CALL_OUTPUT_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "reasoning",
        fields: REASONING_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "program",
        fields: PROGRAM_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "program_output",
        fields: PROGRAM_OUTPUT_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "tool_search_call",
        fields: TOOL_SEARCH_CALL_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "tool_search_output",
        fields: TOOL_SEARCH_OUTPUT_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "additional_tools",
        fields: ADDITIONAL_TOOLS_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "compaction",
        fields: COMPACTION_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "image_generation_call",
        fields: IMAGE_GENERATION_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "code_interpreter_call",
        fields: CODE_INTERPRETER_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "local_shell_call",
        fields: LOCAL_SHELL_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "local_shell_call_output",
        fields: LOCAL_SHELL_OUTPUT_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "shell_call",
        fields: SHELL_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "shell_call_output",
        fields: SHELL_OUTPUT_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "apply_patch_call",
        fields: APPLY_PATCH_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "apply_patch_call_output",
        fields: APPLY_PATCH_OUTPUT_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "mcp_call",
        fields: MCP_CALL_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "mcp_list_tools",
        fields: MCP_LIST_TOOLS_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "mcp_approval_request",
        fields: MCP_APPROVAL_REQUEST_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "mcp_approval_response",
        fields: MCP_APPROVAL_RESPONSE_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "custom_tool_call",
        fields: CUSTOM_TOOL_CALL_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "custom_tool_call_output",
        fields: CUSTOM_TOOL_CALL_OUTPUT_ITEM_FIELDS,
    },
];

fn response_event_rule(name: &str) -> Option<&'static EventRule> {
    EVENT_RULES.iter().find(|rule| rule.name == name)
}

fn validate_response_event(
    value: &Value,
    event_type: &str,
    rule: &EventRule,
) -> Result<(), ApiError> {
    if value.get("type").and_then(Value::as_str) != Some(event_type) {
        return Err(ApiError::upstream());
    }
    for field_rule in rule.fields {
        validate_field(value, *field_rule)?;
    }
    Ok(())
}

fn validate_field(value: &Value, rule: FieldRule) -> Result<(), ApiError> {
    let field = value.get(rule.name).ok_or_else(ApiError::upstream)?;
    let valid = match rule.kind {
        FieldKind::Any => !field.is_null(),
        FieldKind::String => field.is_string(),
        FieldKind::Integer => field.as_u64().is_some(),
        FieldKind::Bool => field.is_boolean(),
        FieldKind::Object => field.is_object(),
        FieldKind::Array => field.is_array(),
    };
    valid.then_some(()).ok_or_else(ApiError::upstream)
}

fn validate_output_item(value: &Value) -> Result<(), ApiError> {
    let object = value.as_object().ok_or_else(ApiError::upstream)?;
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(ApiError::upstream)?;
    let rule = OUTPUT_ITEM_RULES
        .iter()
        .find(|rule| rule.name == kind)
        .ok_or_else(ApiError::upstream)?;
    for field_rule in rule.fields {
        validate_field(value, *field_rule)?;
    }
    Ok(())
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
    let response = native_codex_responses_json(body, "gpt-test")?;
    let mut text = String::new();
    for item in response
        .get("output")
        .and_then(Value::as_array)
        .ok_or_else(ApiError::upstream)?
    {
        if item.get("type").and_then(Value::as_str) != Some("message") {
            continue;
        }
        for content in item
            .get("content")
            .and_then(Value::as_array)
            .ok_or_else(ApiError::upstream)?
        {
            if content.get("type").and_then(Value::as_str) == Some("output_text") {
                text.push_str(
                    content
                        .get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(ApiError::upstream)?,
                );
            }
        }
    }
    Ok(text)
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
