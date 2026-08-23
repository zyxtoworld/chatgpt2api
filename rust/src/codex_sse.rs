use std::io;

use serde_json::{Map, Value, json};

use super::{ApiError, native_completion_id, native_message_id, sse_delimiter};

pub(super) fn native_codex_responses_json(
    body: &[u8],
    requested_model: &str,
) -> Result<Value, ApiError> {
    let mut buffer = body.to_vec();
    let mut text = String::new();
    let mut response_id = None;
    let mut completed_response = None;
    let mut terminal_status = None;
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
        validate_codex_response_event(&value).map_err(|_| ApiError::upstream())?;
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
                terminal_status = Some("completed");
            }
            "response.incomplete" => {
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
                terminal_status = Some("incomplete");
            }
            "response.failed" | "error" => {
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
    response.insert(
        "status".to_owned(),
        json!(terminal_status.unwrap_or("completed")),
    );
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
                "status": terminal_status.unwrap_or("completed"),
                "role": "assistant",
                "content": [{
                    "type": "output_text",
                    "text": text,
                    "annotations": []
                }]
            }]),
        );
    }
    let response = Value::Object(response);
    normalize_terminal_response(&response, terminal_status.unwrap_or("completed"))
        .map_err(|_| ApiError::upstream())
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
const MCP_CALL_OUTPUT_ITEM_FIELDS: &[FieldRule] = &[
    field("call_id", FieldKind::String),
    field("output", FieldKind::Object),
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
        name: "mcp_call_output",
        fields: MCP_CALL_OUTPUT_ITEM_FIELDS,
    },
    OutputItemRule {
        name: "mcp_tool_call_output",
        fields: MCP_CALL_OUTPUT_ITEM_FIELDS,
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
    if kind == "message" {
        let content = object
            .get("content")
            .and_then(Value::as_array)
            .ok_or_else(ApiError::upstream)?;
        for part in content {
            let part = part.as_object().ok_or_else(ApiError::upstream)?;
            match part.get("type").and_then(Value::as_str) {
                Some("output_text") => {
                    if !part.get("text").is_some_and(Value::is_string)
                        || part
                            .get("annotations")
                            .is_some_and(|value| !value.is_array())
                        || part.get("logprobs").is_some_and(|value| !value.is_array())
                    {
                        return Err(ApiError::upstream());
                    }
                    if let Some(annotations) = part.get("annotations").and_then(Value::as_array) {
                        for annotation in annotations {
                            let annotation =
                                annotation.as_object().ok_or_else(ApiError::upstream)?;
                            if annotation.get("type").and_then(Value::as_str).is_none()
                                || annotation
                                    .get("url")
                                    .is_some_and(|value| !value.is_string())
                                || annotation
                                    .get("title")
                                    .is_some_and(|value| !value.is_string())
                                || annotation
                                    .get("start_index")
                                    .is_some_and(|value| value.as_u64().is_none())
                                || annotation
                                    .get("end_index")
                                    .is_some_and(|value| value.as_u64().is_none())
                            {
                                return Err(ApiError::upstream());
                            }
                        }
                    }
                }
                Some("refusal") => {
                    if !part.get("refusal").is_some_and(Value::is_string) {
                        return Err(ApiError::upstream());
                    }
                }
                _ => return Err(ApiError::upstream()),
            }
        }
    }
    if matches!(kind, "mcp_call_output" | "mcp_tool_call_output")
        && object
            .get("call_id")
            .and_then(Value::as_str)
            .is_none_or(|value| value.trim().is_empty())
    {
        return Err(ApiError::upstream());
    }
    Ok(())
}

fn validate_codex_usage(value: &Value) -> Result<(), ApiError> {
    let object = value.as_object().ok_or_else(ApiError::upstream)?;
    for field in ["input_tokens", "output_tokens", "total_tokens"] {
        if object
            .get(field)
            .is_none_or(|value| value.as_u64().is_none())
        {
            return Err(ApiError::upstream());
        }
    }
    for (field, required, optional) in [
        (
            "input_tokens_details",
            "cached_tokens",
            ["cache_write_tokens", "text_tokens", "image_tokens"].as_slice(),
        ),
        (
            "output_tokens_details",
            "reasoning_tokens",
            ["text_tokens", "image_tokens"].as_slice(),
        ),
    ] {
        let Some(details) = object.get(field).filter(|value| !value.is_null()) else {
            continue;
        };
        let details = details.as_object().ok_or_else(ApiError::upstream)?;
        if details
            .get(required)
            .is_none_or(|value| value.as_u64().is_none())
            || optional.iter().any(|name| {
                details
                    .get(*name)
                    .is_some_and(|value| value.as_u64().is_none())
            })
        {
            return Err(ApiError::upstream());
        }
    }
    Ok(())
}

fn validate_response_envelope(
    value: &Value,
    expected_status: Option<&str>,
) -> Result<(), ApiError> {
    let response = value
        .get("response")
        .and_then(Value::as_object)
        .ok_or_else(ApiError::upstream)?;
    if response
        .get("id")
        .and_then(Value::as_str)
        .is_none_or(|value| value.trim().is_empty())
        || response.get("model").is_some_and(|value| {
            !value.is_null() && value.as_str().is_none_or(|v| v.trim().is_empty())
        })
        || response
            .get("created_at")
            .is_some_and(|value| !value.is_null() && value.as_u64().is_none())
        || response
            .get("status")
            .is_some_and(|value| !value.is_null() && !value.is_string())
        || response
            .get("output")
            .is_some_and(|value| !value.is_array())
        || response
            .get("error")
            .is_some_and(|value| !value.is_null() && !value.is_object())
    {
        return Err(ApiError::upstream());
    }
    if let Some(status) = expected_status {
        if response.get("error").is_some_and(|value| !value.is_null()) {
            return Err(ApiError::upstream());
        }
        if response
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|value| value != status)
        {
            return Err(ApiError::upstream());
        }
        if status == "completed" {
            if response
                .get("incomplete_details")
                .is_some_and(|value| !value.is_null())
            {
                return Err(ApiError::upstream());
            }
        } else if let Some(details) = response
            .get("incomplete_details")
            .filter(|value| !value.is_null())
        {
            let details = details.as_object().ok_or_else(ApiError::upstream)?;
            if details.get("reason").is_some_and(|value| {
                !value.is_null()
                    && !matches!(value.as_str(), Some("max_output_tokens" | "content_filter"))
            }) {
                return Err(ApiError::upstream());
            }
        }
    }
    if let Some(output) = response.get("output").and_then(Value::as_array) {
        for item in output {
            validate_output_item(item)?;
        }
    }
    if let Some(usage) = response.get("usage").filter(|value| !value.is_null()) {
        validate_codex_usage(usage)?;
    }
    Ok(())
}

fn validate_active_response_envelope(value: &Value) -> Result<(), ApiError> {
    validate_response_envelope(value, Some("in_progress"))?;
    let response = value
        .get("response")
        .and_then(Value::as_object)
        .ok_or_else(ApiError::upstream)?;
    if response.get("error").is_some_and(|value| !value.is_null())
        || response
            .get("incomplete_details")
            .is_some_and(|value| !value.is_null())
    {
        return Err(ApiError::upstream());
    }
    Ok(())
}

pub(super) fn validate_codex_response_event(value: &Value) -> Result<&str, io::Error> {
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::other("malformed Codex event type"))?;
    let rule = response_event_rule(event_type)
        .ok_or_else(|| io::Error::other("unknown Codex response event"))?;
    validate_response_event(value, event_type, rule)
        .map_err(|_| io::Error::other("malformed Codex response event"))?;
    if matches!(
        event_type,
        "response.output_item.added" | "response.output_item.done"
    ) {
        validate_output_item(
            value
                .get("item")
                .ok_or_else(|| io::Error::other("malformed Codex output item"))?,
        )
        .map_err(|_| io::Error::other("malformed Codex output item"))?;
    }
    match event_type {
        "response.created" | "response.in_progress" => validate_active_response_envelope(value),
        "response.completed" => validate_response_envelope(value, Some("completed")),
        "response.incomplete" => validate_response_envelope(value, Some("incomplete")),
        _ => Ok(()),
    }
    .map_err(|_| io::Error::other("malformed Codex response envelope"))?;
    Ok(event_type)
}

const PUBLIC_EVENT_FIELDS: &[&str] = &[
    "type",
    "sequence_number",
    "response",
    "item",
    "output_index",
    "content_index",
    "item_id",
    "delta",
    "text",
    "arguments",
    "part",
    "code",
    "message",
    "param",
    "error",
];

const PUBLIC_RESPONSE_FIELDS: &[&str] = &[
    "id",
    "object",
    "created_at",
    "status",
    "error",
    "incomplete_details",
    "model",
    "output",
    "parallel_tool_calls",
    "usage",
];

const PUBLIC_MESSAGE_ITEM_FIELDS: &[&str] = &["type", "id", "role", "content", "phase", "status"];
const PUBLIC_FILE_SEARCH_ITEM_FIELDS: &[&str] = &["type", "id", "queries", "status"];
const PUBLIC_FUNCTION_CALL_ITEM_FIELDS: &[&str] = &[
    "type",
    "id",
    "call_id",
    "name",
    "description",
    "namespace",
    "arguments",
    "encrypted_function_args",
    "status",
];
const PUBLIC_FUNCTION_CALL_OUTPUT_ITEM_FIELDS: &[&str] =
    &["type", "id", "call_id", "output", "status"];
const PUBLIC_WEB_SEARCH_ITEM_FIELDS: &[&str] = &["type", "id", "action", "status"];
const PUBLIC_COMPUTER_CALL_ITEM_FIELDS: &[&str] =
    &["type", "id", "call_id", "pending_safety_checks", "status"];
const PUBLIC_COMPUTER_CALL_OUTPUT_ITEM_FIELDS: &[&str] =
    &["type", "id", "call_id", "output", "status"];
const PUBLIC_REASONING_ITEM_FIELDS: &[&str] = &[
    "type",
    "id",
    "summary",
    "content",
    "encrypted_content",
    "status",
];
const PUBLIC_PROGRAM_ITEM_FIELDS: &[&str] = &["type", "id", "call_id", "code", "fingerprint"];
const PUBLIC_PROGRAM_OUTPUT_ITEM_FIELDS: &[&str] = &["type", "id", "call_id", "result", "status"];
const PUBLIC_TOOL_SEARCH_CALL_ITEM_FIELDS: &[&str] =
    &["type", "id", "call_id", "status", "execution", "arguments"];
const PUBLIC_TOOL_SEARCH_OUTPUT_ITEM_FIELDS: &[&str] =
    &["type", "id", "call_id", "status", "execution", "tools"];
const PUBLIC_ADDITIONAL_TOOLS_ITEM_FIELDS: &[&str] = &["type", "id", "role", "tools"];
const PUBLIC_COMPACTION_ITEM_FIELDS: &[&str] = &["type", "id", "encrypted_content"];
const PUBLIC_IMAGE_GENERATION_ITEM_FIELDS: &[&str] =
    &["type", "id", "status", "revised_prompt", "result"];
const PUBLIC_CODE_INTERPRETER_ITEM_FIELDS: &[&str] =
    &["type", "id", "code", "container_id", "outputs", "status"];
const PUBLIC_LOCAL_SHELL_ITEM_FIELDS: &[&str] = &["type", "id", "action", "call_id", "status"];
const PUBLIC_LOCAL_SHELL_OUTPUT_ITEM_FIELDS: &[&str] = &["type", "id", "output"];
const PUBLIC_SHELL_ITEM_FIELDS: &[&str] =
    &["type", "id", "action", "call_id", "environment", "status"];
const PUBLIC_SHELL_OUTPUT_ITEM_FIELDS: &[&str] = &[
    "type",
    "id",
    "call_id",
    "max_output_length",
    "output",
    "status",
];
const PUBLIC_APPLY_PATCH_ITEM_FIELDS: &[&str] = &["type", "id", "call_id", "operation", "status"];
const PUBLIC_APPLY_PATCH_OUTPUT_ITEM_FIELDS: &[&str] = &["type", "id", "call_id", "status"];
const PUBLIC_MCP_CALL_ITEM_FIELDS: &[&str] = &["type", "id", "arguments", "name", "server_label"];
const PUBLIC_MCP_LIST_TOOLS_ITEM_FIELDS: &[&str] = &["type", "id", "server_label", "tools"];
const PUBLIC_MCP_APPROVAL_REQUEST_ITEM_FIELDS: &[&str] =
    &["type", "id", "arguments", "name", "server_label"];
const PUBLIC_MCP_APPROVAL_RESPONSE_ITEM_FIELDS: &[&str] =
    &["type", "id", "approval_request_id", "approve"];
const PUBLIC_CUSTOM_TOOL_CALL_ITEM_FIELDS: &[&str] = &[
    "type",
    "id",
    "call_id",
    "name",
    "namespace",
    "input",
    "status",
];
const PUBLIC_CUSTOM_TOOL_CALL_OUTPUT_ITEM_FIELDS: &[&str] =
    &["type", "id", "call_id", "name", "output", "status"];
const PUBLIC_MCP_CALL_OUTPUT_ITEM_FIELDS: &[&str] = &["type", "call_id", "output"];

const PUBLIC_CONTENT_FIELDS: &[&str] = &[
    "type",
    "text",
    "annotations",
    "logprobs",
    "image_url",
    "detail",
    "audio_url",
    "encrypted_content",
];
const PUBLIC_ANNOTATION_FIELDS: &[&str] = &["type", "url", "title", "start_index", "end_index"];
const PUBLIC_LOGPROB_FIELDS: &[&str] = &["token", "logprob", "bytes", "top_logprobs"];
const PUBLIC_ERROR_FIELDS: &[&str] = &["type", "code", "message", "param"];
const PUBLIC_USAGE_FIELDS: &[&str] = &[
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "input_tokens_details",
    "output_tokens_details",
];
const PUBLIC_USAGE_DETAIL_FIELDS: &[&str] = &[
    "cached_tokens",
    "cache_write_tokens",
    "text_tokens",
    "image_tokens",
    "reasoning_tokens",
];
const PUBLIC_ACTION_FIELDS: &[&str] = &["type", "query", "queries", "url", "pattern"];
const PUBLIC_ENVIRONMENT_FIELDS: &[&str] = &[
    "type",
    "id",
    "name",
    "container_id",
    "shell",
    "working_directory",
    "env",
    "network",
    "timeout",
    "description",
    "text",
];
const PUBLIC_OPERATION_FIELDS: &[&str] = &[
    "type",
    "id",
    "operation",
    "path",
    "patch",
    "status",
    "command",
    "description",
    "text",
];
const PUBLIC_SAFETY_CHECK_FIELDS: &[&str] = &["type", "id", "code", "message", "reason"];
const PUBLIC_OUTPUT_FIELDS: &[&str] = &[
    "type",
    "id",
    "text",
    "logs",
    "files",
    "stdout",
    "stderr",
    "command",
    "exit_code",
    "status",
    "mime_type",
    "filename",
    "url",
    "data",
    "content",
];
const PUBLIC_TOOL_DEFINITION_FIELDS: &[&str] = &[
    "type",
    "name",
    "description",
    "parameters",
    "strict",
    "defer_loading",
];
const PUBLIC_TOOL_SCHEMA_FIELDS: &[&str] = &[
    "type",
    "properties",
    "required",
    "items",
    "additionalProperties",
    "description",
    "title",
    "pattern",
    "enum",
    "const",
    "oneOf",
    "anyOf",
    "allOf",
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
];
const PUBLIC_TOOL_SCHEMA_STRING_FIELDS: &[&str] = &["description", "title", "pattern"];
const PUBLIC_TOOL_SCHEMA_NUMBER_FIELDS: &[&str] = &["minLength", "maxLength", "minimum", "maximum"];
const PUBLIC_STRING_FIELDS: &[&str] = &[
    "type",
    "id",
    "object",
    "status",
    "model",
    "role",
    "phase",
    "item_id",
    "call_id",
    "name",
    "namespace",
    "delta",
    "text",
    "arguments",
    "encrypted_content",
    "result",
    "revised_prompt",
    "execution",
    "detail",
    "image_url",
    "audio_url",
    "code",
    "message",
    "param",
    "url",
    "title",
    "query",
    "pattern",
    "description",
    "path",
    "patch",
    "command",
    "stdout",
    "stderr",
    "logs",
    "mime_type",
    "filename",
    "reason",
];
const PUBLIC_INTEGER_FIELDS: &[&str] = &[
    "sequence_number",
    "output_index",
    "content_index",
    "created_at",
    "start_index",
    "end_index",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "cached_tokens",
    "cache_write_tokens",
    "text_tokens",
    "image_tokens",
    "reasoning_tokens",
    "max_output_length",
    "exit_code",
    "timeout",
];

fn has_field(fields: &[&str], field: &str) -> bool {
    fields.contains(&field)
}

fn project_public_integer(value: &Value) -> Result<Value, ()> {
    value.as_u64().map(Value::from).ok_or(())
}

fn project_public_number(value: &Value) -> Result<Value, ()> {
    value.is_number().then(|| value.clone()).ok_or(())
}

fn project_public_array(value: &Value) -> Result<&Vec<Value>, ()> {
    value.as_array().ok_or(())
}

fn project_public_annotation(value: &Value) -> Result<Value, ()> {
    project_public_object(value, PUBLIC_ANNOTATION_FIELDS)
}

fn project_public_environment(value: &Value) -> Result<Value, ()> {
    project_public_object(value, PUBLIC_ENVIRONMENT_FIELDS)
}

fn project_public_operation(value: &Value) -> Result<Value, ()> {
    project_public_object(value, PUBLIC_OPERATION_FIELDS)
}

fn project_public_safety_checks(value: &Value) -> Result<Value, ()> {
    let values = project_public_array(value)?;
    Ok(Value::Array(
        values
            .iter()
            .map(|value| project_public_object(value, PUBLIC_SAFETY_CHECK_FIELDS))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn project_public_outputs(value: &Value) -> Result<Value, ()> {
    let values = project_public_array(value)?;
    Ok(Value::Array(
        values
            .iter()
            .map(|value| project_public_object(value, PUBLIC_OUTPUT_FIELDS))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn project_public_output_object(value: &Value) -> Result<Value, ()> {
    project_public_object(value, PUBLIC_OUTPUT_FIELDS)
}

fn project_public_item_output(value: &Value) -> Result<Value, ()> {
    if value.is_null() || value.is_string() || value.is_number() || value.is_boolean() {
        return Ok(value.clone());
    }
    if value.is_object() {
        return project_public_output_object(value);
    }
    project_public_outputs(value)
}

fn public_item_fields(item_type: &str) -> Option<&'static [&'static str]> {
    Some(match item_type {
        "message" => PUBLIC_MESSAGE_ITEM_FIELDS,
        "file_search_call" => PUBLIC_FILE_SEARCH_ITEM_FIELDS,
        "function_call" => PUBLIC_FUNCTION_CALL_ITEM_FIELDS,
        "function_call_output" => PUBLIC_FUNCTION_CALL_OUTPUT_ITEM_FIELDS,
        "web_search_call" => PUBLIC_WEB_SEARCH_ITEM_FIELDS,
        "computer_call" => PUBLIC_COMPUTER_CALL_ITEM_FIELDS,
        "computer_call_output" => PUBLIC_COMPUTER_CALL_OUTPUT_ITEM_FIELDS,
        "reasoning" => PUBLIC_REASONING_ITEM_FIELDS,
        "program" => PUBLIC_PROGRAM_ITEM_FIELDS,
        "program_output" => PUBLIC_PROGRAM_OUTPUT_ITEM_FIELDS,
        "tool_search_call" => PUBLIC_TOOL_SEARCH_CALL_ITEM_FIELDS,
        "tool_search_output" => PUBLIC_TOOL_SEARCH_OUTPUT_ITEM_FIELDS,
        "additional_tools" => PUBLIC_ADDITIONAL_TOOLS_ITEM_FIELDS,
        "compaction" => PUBLIC_COMPACTION_ITEM_FIELDS,
        "image_generation_call" => PUBLIC_IMAGE_GENERATION_ITEM_FIELDS,
        "code_interpreter_call" => PUBLIC_CODE_INTERPRETER_ITEM_FIELDS,
        "local_shell_call" => PUBLIC_LOCAL_SHELL_ITEM_FIELDS,
        "local_shell_call_output" => PUBLIC_LOCAL_SHELL_OUTPUT_ITEM_FIELDS,
        "shell_call" => PUBLIC_SHELL_ITEM_FIELDS,
        "shell_call_output" => PUBLIC_SHELL_OUTPUT_ITEM_FIELDS,
        "apply_patch_call" => PUBLIC_APPLY_PATCH_ITEM_FIELDS,
        "apply_patch_call_output" => PUBLIC_APPLY_PATCH_OUTPUT_ITEM_FIELDS,
        "mcp_call" => PUBLIC_MCP_CALL_ITEM_FIELDS,
        "mcp_call_output" | "mcp_tool_call_output" => PUBLIC_MCP_CALL_OUTPUT_ITEM_FIELDS,
        "mcp_list_tools" => PUBLIC_MCP_LIST_TOOLS_ITEM_FIELDS,
        "mcp_approval_request" => PUBLIC_MCP_APPROVAL_REQUEST_ITEM_FIELDS,
        "mcp_approval_response" => PUBLIC_MCP_APPROVAL_RESPONSE_ITEM_FIELDS,
        "custom_tool_call" => PUBLIC_CUSTOM_TOOL_CALL_ITEM_FIELDS,
        "custom_tool_call_output" => PUBLIC_CUSTOM_TOOL_CALL_OUTPUT_ITEM_FIELDS,
        _ => return None,
    })
}

fn project_public_logprob(value: &Value) -> Result<Value, ()> {
    let object = value.as_object().ok_or(())?;
    let mut projected = Map::new();
    for field in PUBLIC_LOGPROB_FIELDS {
        let Some(child) = object.get(*field) else {
            continue;
        };
        let value = match *field {
            "token" => child.is_string().then(|| child.clone()).ok_or(())?,
            "logprob" => project_public_number(child)?,
            "bytes" => {
                let bytes = project_public_array(child)?;
                Value::Array(
                    bytes
                        .iter()
                        .map(project_public_integer)
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            "top_logprobs" => {
                let values = project_public_array(child)?;
                Value::Array(
                    values
                        .iter()
                        .map(project_public_logprob)
                        .collect::<Result<Vec<_>, _>>()?,
                )
            }
            _ => unreachable!(),
        };
        projected.insert((*field).to_owned(), value);
    }
    Ok(Value::Object(projected))
}

fn project_public_tool_schema(value: &Value) -> Result<Value, ()> {
    let object = value.as_object().ok_or(())?;
    let mut projected = Map::new();
    for (field, child) in object {
        if !has_field(PUBLIC_TOOL_SCHEMA_FIELDS, field) {
            continue;
        }
        let projected_child = if has_field(PUBLIC_TOOL_SCHEMA_STRING_FIELDS, field) {
            child
                .as_str()
                .map(|value| Value::String(value.to_owned()))
                .ok_or(())?
        } else if field == "type" {
            match child {
                Value::String(_) => child.clone(),
                Value::Array(values) if values.iter().all(Value::is_string) => child.clone(),
                _ => return Err(()),
            }
        } else if field == "properties" {
            let properties = child.as_object().ok_or(())?;
            let mut projected_properties = Map::new();
            for (name, schema) in properties {
                if !schema.is_object() {
                    continue;
                }
                projected_properties.insert(name.clone(), project_public_tool_schema(schema)?);
            }
            Value::Object(projected_properties)
        } else if field == "required" {
            let required = project_public_array(child)?;
            Value::Array(
                required
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(|value| Value::String(value.to_owned()))
                            .ok_or(())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else if field == "items" {
            project_public_tool_schema(child)?
        } else if field == "additionalProperties" {
            if child.is_boolean() {
                child.clone()
            } else {
                project_public_tool_schema(child)?
            }
        } else if matches!(field.as_str(), "oneOf" | "anyOf" | "allOf") {
            let values = project_public_array(child)?;
            Value::Array(
                values
                    .iter()
                    .map(project_public_tool_schema)
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else if field == "enum" {
            let values = project_public_array(child)?;
            if values
                .iter()
                .any(|value| value.is_object() || value.is_array())
            {
                return Err(());
            }
            child.clone()
        } else if field == "const" {
            if child.is_object() || child.is_array() {
                return Err(());
            }
            child.clone()
        } else if has_field(PUBLIC_TOOL_SCHEMA_NUMBER_FIELDS, field) {
            project_public_number(child)?
        } else {
            unreachable!()
        };
        projected.insert(field.clone(), projected_child);
    }
    Ok(Value::Object(projected))
}

fn project_public_object(value: &Value, fields: &[&str]) -> Result<Value, ()> {
    let object = value.as_object().ok_or(())?;
    let mut projected = Map::new();
    for field in fields {
        let Some(child) = object.get(*field) else {
            continue;
        };
        projected.insert(
            (*field).to_owned(),
            project_public_value(child, Some(field))?,
        );
    }
    Ok(Value::Object(projected))
}

fn project_public_value(value: &Value, field: Option<&str>) -> Result<Value, ()> {
    let Some(field) = field else {
        return Ok(value.clone());
    };
    if matches!(field, "error" | "item" | "response" | "usage")
        && !value.is_null()
        && !value.is_object()
    {
        return Err(());
    }
    if value.is_null()
        && matches!(
            field,
            "error"
                | "item"
                | "response"
                | "usage"
                | "action"
                | "tool_definition"
                | "input_tokens_details"
                | "output_tokens_details"
                | "part"
                | "annotation"
        )
    {
        return Ok(Value::Null);
    }
    match field {
        "output" => {
            if value.is_null() {
                return Ok(Value::Null);
            }
            let values = project_public_array(value)?;
            Ok(Value::Array(
                values
                    .iter()
                    .map(project_public_item)
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        "tools" => {
            if value.is_null() {
                return Ok(Value::Null);
            }
            let values = project_public_array(value)?;
            Ok(Value::Array(
                values
                    .iter()
                    .map(|value| project_public_object(value, PUBLIC_TOOL_DEFINITION_FIELDS))
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        "content" | "annotations" => {
            if value.is_null() {
                return Ok(Value::Null);
            }
            let values = project_public_array(value)?;
            Ok(Value::Array(
                values
                    .iter()
                    .map(|value| {
                        if field == "content" {
                            project_public_content(value)
                        } else {
                            project_public_annotation(value)
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        "queries" => {
            if value.is_null() {
                return Ok(Value::Null);
            }
            let values = project_public_array(value)?;
            Ok(Value::Array(
                values
                    .iter()
                    .map(|value| {
                        value
                            .as_str()
                            .map(|value| Value::String(value.to_owned()))
                            .ok_or(())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        "logprobs" => {
            if value.is_null() {
                return Ok(Value::Null);
            }
            let values = project_public_array(value)?;
            Ok(Value::Array(
                values
                    .iter()
                    .map(project_public_logprob)
                    .collect::<Result<Vec<_>, _>>()?,
            ))
        }
        "incomplete_details" => {
            if value.is_null() {
                return Ok(Value::Null);
            }
            let object = value.as_object().ok_or(())?;
            match object.get("reason") {
                None | Some(Value::Null) => Ok(json!({})),
                Some(Value::String(reason))
                    if matches!(reason.as_str(), "max_output_tokens" | "content_filter") =>
                {
                    Ok(json!({"reason": reason}))
                }
                _ => Err(()),
            }
        }
        "parameters" => project_public_tool_schema(value),
        "environment" => project_public_environment(value),
        "operation" => project_public_operation(value),
        "pending_safety_checks" => project_public_safety_checks(value),
        "outputs" | "files" => project_public_outputs(value),
        "parallel_tool_calls" | "strict" | "defer_loading" => {
            value.is_boolean().then(|| value.clone()).ok_or(())
        }
        _ if has_field(PUBLIC_STRING_FIELDS, field) => {
            value.is_string().then(|| value.clone()).ok_or(())
        }
        _ if has_field(PUBLIC_INTEGER_FIELDS, field) => project_public_integer(value),
        _ if field == "error" => project_public_object(value, PUBLIC_ERROR_FIELDS),
        _ if field == "usage" => project_public_object(value, PUBLIC_USAGE_FIELDS),
        _ if matches!(field, "input_tokens_details" | "output_tokens_details") => {
            project_public_object(value, PUBLIC_USAGE_DETAIL_FIELDS)
        }
        _ if field == "action" => project_public_object(value, PUBLIC_ACTION_FIELDS),
        _ if field == "tool_definition" => {
            project_public_object(value, PUBLIC_TOOL_DEFINITION_FIELDS)
        }
        _ if field == "item" => project_public_item(value),
        _ if field == "response" => project_public_object(value, PUBLIC_RESPONSE_FIELDS),
        _ if matches!(field, "part" | "annotation") => {
            let fields = if field == "part" {
                PUBLIC_CONTENT_FIELDS
            } else {
                PUBLIC_ANNOTATION_FIELDS
            };
            project_public_object(value, fields)
        }
        _ if value.is_object() => project_public_object(value, PUBLIC_CONTENT_FIELDS),
        _ if value.is_array() => Ok(Value::Array(
            value
                .as_array()
                .expect("array checked")
                .iter()
                .map(|value| project_public_value(value, Some(field)))
                .collect::<Result<Vec<_>, _>>()?,
        )),
        _ => Ok(value.clone()),
    }
}

fn project_public_content(value: &Value) -> Result<Value, ()> {
    project_public_object(value, PUBLIC_CONTENT_FIELDS)
}

fn project_public_item(value: &Value) -> Result<Value, ()> {
    let object = value.as_object().ok_or(())?;
    let item_type = object.get("type").and_then(Value::as_str).ok_or(())?;
    let fields = public_item_fields(item_type).ok_or(())?;
    let mut projected = Map::new();
    for field in fields {
        let Some(child) = object.get(*field) else {
            continue;
        };
        let child = match (item_type, *field) {
            ("shell_call", "environment") => project_public_environment(child)?,
            ("apply_patch_call", "operation") => project_public_operation(child)?,
            ("computer_call", "pending_safety_checks") => project_public_safety_checks(child)?,
            ("code_interpreter_call", "outputs") | ("shell_call_output", "output") => {
                project_public_outputs(child)?
            }
            ("computer_call_output", "output") => project_public_output_object(child)?,
            ("function_call_output", "output")
            | ("custom_tool_call_output", "output")
            | ("mcp_call_output", "output")
            | ("mcp_tool_call_output", "output")
            | ("local_shell_call_output", "output") => project_public_item_output(child)?,
            _ => project_public_value(child, Some(field))?,
        };
        projected.insert((*field).to_owned(), child);
    }
    Ok(Value::Object(projected))
}

fn project_public_response(value: &Value) -> Result<Value, ()> {
    project_public_object(value, PUBLIC_RESPONSE_FIELDS)
}

fn normalize_terminal_response(value: &Value, expected_status: &str) -> Result<Value, ()> {
    let raw = value.as_object().ok_or(())?;
    if raw.get("error").is_some_and(|value| !value.is_null())
        || raw
            .get("status")
            .and_then(Value::as_str)
            .is_some_and(|status| status != expected_status)
        || (expected_status == "completed"
            && raw
                .get("incomplete_details")
                .is_some_and(|value| !value.is_null()))
    {
        return Err(());
    }
    let mut response = project_public_response(value)?;
    let object = response.as_object_mut().ok_or(())?;
    object.insert(
        "status".to_owned(),
        Value::String(expected_status.to_owned()),
    );
    if expected_status == "incomplete"
        && let Some(details) = value.get("incomplete_details")
        && !details.is_null()
    {
        object.insert(
            "incomplete_details".to_owned(),
            project_public_value(details, Some("incomplete_details"))?,
        );
    }
    Ok(response)
}

pub(super) fn project_codex_response_event(value: &Value) -> Result<Option<Value>, io::Error> {
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::other("malformed Codex response event"))?;
    if matches!(
        event_type,
        "response.metadata" | "codex.response.metadata" | "responsesapi.websocket_timing"
    ) {
        return Ok(None);
    }
    let object = value
        .as_object()
        .ok_or_else(|| io::Error::other("malformed Codex response event"))?;
    let mut projected = Map::new();
    for field in PUBLIC_EVENT_FIELDS {
        let Some(child) = object.get(*field) else {
            continue;
        };
        let child = if *field == "response"
            && matches!(event_type, "response.completed" | "response.incomplete")
        {
            normalize_terminal_response(
                child,
                if event_type == "response.completed" {
                    "completed"
                } else {
                    "incomplete"
                },
            )
            .map_err(|_| io::Error::other("malformed Codex response projection"))?
        } else {
            project_public_value(child, Some(field))
                .map_err(|_| io::Error::other("malformed Codex response projection"))?
        };
        projected.insert((*field).to_owned(), child);
    }
    Ok(Some(Value::Object(projected)))
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
