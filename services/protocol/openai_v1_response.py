from __future__ import annotations

import copy
import math
import re
import time
import uuid
from typing import Any, Iterable, Iterator

from fastapi import HTTPException

from services.account_service import AccountService, account_service
from services.model_service import ModelUnavailableError, model_catalog_service
from services.openai_backend_api import CODEX_RESPONSES_MODEL, OpenAIBackendAPI
from services.protocol.chat_completion_cache import (
    cache_key,
    chat_completion_cache,
    normalize_text_messages,
    resolve_access_token_cache_scope,
)
from services.protocol.conversation import (
    ConversationRequest,
    ImageOutput,
    count_message_image_tokens,
    count_message_text_tokens,
    count_text_tokens,
    encode_images,
    normalize_messages,
    stream_image_outputs_with_pool,
    stream_text_deltas_without_closing as stream_text_deltas,
    text_backend,
)
from services.protocol.reasoning_effort import conversation_effort_from_body
from services.protocol.error_response import PublicSafeValueError
from services.protocol.image_options import (
    normalize_image_output_compression,
    normalize_image_output_format,
    normalize_image_quality,
    normalize_image_size,
    normalize_supported_image_background,
    normalize_supported_image_moderation,
    normalize_supported_partial_images,
)
from services.protocol.web_search_tool import (
    WEB_SEARCH_TOOL_TYPES,
    has_unsupported_tools,
    has_web_search_tool,
)
from utils.helper import (
    UpstreamHTTPError,
    extract_image_from_message_content,
    extract_response_prompt,
    is_codex_image_model,
    has_response_image_generation_tool,
    is_supported_image_model,
)
from utils.image_tokens import (
    count_image_content_tokens,
    count_image_output_items_tokens,
    image_usage,
    token_usage,
)

RESPONSE_CONTENT_PART_TYPES = frozenset({
    "input_text",
    "output_text",
    "input_image",
    "input_audio",
})
SUPPORTED_RESPONSE_MESSAGE_CONTENT_PART_TYPES = RESPONSE_CONTENT_PART_TYPES
CODEX_RESPONSE_CONTENT_PART_TYPES = RESPONSE_CONTENT_PART_TYPES
SUPPORTED_RESPONSE_IMAGE_DETAILS = {"auto", "low", "high", "original"}
SUPPORTED_RESPONSE_MESSAGE_FIELDS = {"type", "id", "role", "content", "phase", "status"}
SUPPORTED_RESPONSE_MESSAGE_ROLES = {"user", "assistant", "system", "developer"}
SUPPORTED_RESPONSE_MESSAGE_PHASES = {"commentary", "final_answer"}
SUPPORTED_RESPONSE_MESSAGE_STATUSES = {"in_progress", "completed", "incomplete"}
SUPPORTED_FUNCTION_CALL_OUTPUT_FIELDS = {"type", "id", "call_id", "output", "status"}
_CODEX_TEXT_FAILOVER_STATUSES = frozenset({429, 500, 502, 503, 504})
_CODEX_TEXT_FAILOVER_DEADLINE_SECONDS = 30.0
SUPPORTED_FUNCTION_CALL_OUTPUT_STATUSES = {"in_progress", "completed", "incomplete"}
SUPPORTED_CUSTOM_TOOL_CALL_OUTPUT_FIELDS = {"type", "id", "call_id", "name", "output"}
SUPPORTED_FUNCTION_CALL_FIELDS = {
    "type",
    "id",
    "call_id",
    "name",
    "description",
    "namespace",
    "arguments",
    "encrypted_function_args",
    "status",
}
SUPPORTED_CUSTOM_TOOL_CALL_FIELDS = {
    "type",
    "id",
    "call_id",
    "name",
    "namespace",
    "input",
    "status",
}


def _is_codex_account_token(access_token: str) -> bool:
    return AccountService.is_codex_backend_compatible(
        account_service.get_account(access_token)
    )


SUPPORTED_TOOL_CALL_STATUSES = {"in_progress", "completed", "incomplete"}
SUPPORTED_REASONING_FIELDS = {
    "type",
    "id",
    "summary",
    "content",
    "encrypted_content",
    "status",
}
SUPPORTED_COMPACTION_FIELDS = {"type", "id", "encrypted_content"}
SUPPORTED_IMAGE_GENERATION_CALL_FIELDS = {
    "type",
    "id",
    "status",
    "revised_prompt",
    "result",
}
SUPPORTED_IMAGE_GENERATION_CALL_STATUSES = {
    "in_progress",
    "completed",
    "generating",
    "failed",
}
SUPPORTED_WEB_SEARCH_CALL_FIELDS = {"type", "id", "status", "action"}
SUPPORTED_WEB_SEARCH_CALL_STATUSES = {"in_progress", "searching", "completed", "failed"}
SUPPORTED_TOOL_SEARCH_CALL_FIELDS = {
    "type",
    "id",
    "call_id",
    "status",
    "execution",
    "arguments",
}
SUPPORTED_TOOL_SEARCH_OUTPUT_FIELDS = {
    "type",
    "id",
    "call_id",
    "status",
    "execution",
    "tools",
}
SUPPORTED_TOOL_SEARCH_EXECUTIONS = {"server", "client"}
SUPPORTED_MCP_TOOL_CALL_OUTPUT_FIELDS = {"type", "call_id", "output"}
SUPPORTED_TOOL_CALL_OUTPUT_PART_TYPES = {
    "input_text",
    "input_image",
    "input_audio",
    "encrypted_content",
}
CODEX_DEFAULT_INCLUDE = "reasoning.encrypted_content"
CODEX_INTERNAL_RESPONSE_EVENT_TYPES = frozenset({
    "codex.response.metadata",
    "response.metadata",
    "responsesapi.websocket_timing",
})
CODEX_INTERNAL_RESPONSE_EVENT_FIELDS = frozenset({
    "headers",
    "metadata",
    "safety_buffering",
})
CODEX_INTERNAL_RESPONSE_ITEM_FIELDS = frozenset({
    "internal_chat_message_metadata_passthrough",
})
_PUBLIC_CODEX_EVENT_FIELDS = frozenset({
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
})
_PUBLIC_CODEX_RESPONSE_FIELDS = frozenset({
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
})
_PUBLIC_CODEX_ITEM_FIELDS_BY_TYPE = {
    "message": SUPPORTED_RESPONSE_MESSAGE_FIELDS,
    "file_search_call": {"type", "id", "queries", "status"},
    "function_call_output": SUPPORTED_FUNCTION_CALL_OUTPUT_FIELDS,
    "custom_tool_call_output": SUPPORTED_CUSTOM_TOOL_CALL_OUTPUT_FIELDS | {"status"},
    "function_call": SUPPORTED_FUNCTION_CALL_FIELDS,
    "custom_tool_call": SUPPORTED_CUSTOM_TOOL_CALL_FIELDS,
    "reasoning": SUPPORTED_REASONING_FIELDS,
    "program": {"type", "id", "call_id", "code", "fingerprint"},
    "program_output": {"type", "id", "call_id", "result", "status"},
    "compaction": SUPPORTED_COMPACTION_FIELDS,
    "image_generation_call": SUPPORTED_IMAGE_GENERATION_CALL_FIELDS,
    "web_search_call": SUPPORTED_WEB_SEARCH_CALL_FIELDS,
    "tool_search_call": SUPPORTED_TOOL_SEARCH_CALL_FIELDS,
    "tool_search_output": SUPPORTED_TOOL_SEARCH_OUTPUT_FIELDS,
    "additional_tools": {"type", "id", "role", "tools"},
    "mcp_call_output": SUPPORTED_MCP_TOOL_CALL_OUTPUT_FIELDS,
    "mcp_tool_call_output": SUPPORTED_MCP_TOOL_CALL_OUTPUT_FIELDS,
    "computer_call": {"type", "id", "call_id", "pending_safety_checks", "status"},
    "computer_call_output": {"type", "id", "call_id", "output", "status"},
    "shell_call": {"type", "id", "action", "call_id", "environment", "status"},
    "shell_call_output": {"type", "id", "call_id", "max_output_length", "output", "status"},
    "apply_patch_call": {"type", "id", "call_id", "operation", "status"},
    "apply_patch_call_output": {"type", "id", "call_id", "status"},
    "code_interpreter_call": {"type", "id", "code", "container_id", "outputs", "status"},
    "local_shell_call": {"type", "id", "action", "call_id", "status"},
    "local_shell_call_output": {"type", "id", "output"},
    "mcp_call": {"type", "id", "arguments", "name", "server_label"},
    "mcp_list_tools": {"type", "id", "server_label", "tools"},
    "mcp_approval_request": {"type", "id", "arguments", "name", "server_label"},
    "mcp_approval_response": {"type", "id", "approval_request_id", "approve"},
}
_PUBLIC_CODEX_CONTENT_FIELDS = frozenset({
    "type",
    "text",
    "annotations",
    "logprobs",
    "image_url",
    "detail",
    "audio_url",
    "encrypted_content",
})
_PUBLIC_CODEX_LOGPROB_FIELDS = frozenset({"token", "logprob", "bytes", "top_logprobs"})
_PUBLIC_CODEX_ERROR_FIELDS = frozenset({"type", "code", "message", "param"})
_PUBLIC_CODEX_USAGE_FIELDS = frozenset({
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "input_tokens_details",
    "output_tokens_details",
})
_PUBLIC_CODEX_USAGE_DETAIL_FIELDS = frozenset({
    "cached_tokens",
    "cache_write_tokens",
    "text_tokens",
    "image_tokens",
    "reasoning_tokens",
})
_PUBLIC_CODEX_ANNOTATION_FIELDS = frozenset({
    "type",
    "url",
    "title",
    "start_index",
    "end_index",
})
_PUBLIC_CODEX_INCOMPLETE_REASONS = frozenset({
    "max_output_tokens",
    "content_filter",
})
_PUBLIC_CODEX_ACTION_FIELDS = frozenset({
    "type",
    "query",
    "queries",
    "url",
    "pattern",
})
_PUBLIC_CODEX_ENVIRONMENT_FIELDS = frozenset({
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
})
_PUBLIC_CODEX_OPERATION_FIELDS = frozenset({
    "type",
    "id",
    "operation",
    "path",
    "patch",
    "status",
    "command",
    "description",
    "text",
})
_PUBLIC_CODEX_SAFETY_CHECK_FIELDS = frozenset({"type", "id", "code", "message", "reason"})
_PUBLIC_CODEX_OUTPUT_FIELDS = frozenset({
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
})
_PUBLIC_CODEX_TOOL_DEFINITION_FIELDS = frozenset({
    "type",
    "name",
    "description",
    "parameters",
    "strict",
    "defer_loading",
})
_PUBLIC_CODEX_TOOL_SCHEMA_FIELDS = frozenset({
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
})
_PUBLIC_CODEX_TOOL_SCHEMA_STRING_FIELDS = frozenset({
    "description",
    "title",
    "pattern",
})
_PUBLIC_CODEX_TOOL_SCHEMA_NUMBER_FIELDS = frozenset({
    "minLength",
    "maxLength",
    "minimum",
    "maximum",
})
_PUBLIC_CODEX_SCALAR_FIELDS = frozenset({
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
    "parallel_tool_calls",
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
})
_PUBLIC_CODEX_STRING_FIELDS = frozenset(_PUBLIC_CODEX_SCALAR_FIELDS - {
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
    "parallel_tool_calls",
})
_PUBLIC_CODEX_INTEGER_FIELDS = frozenset({
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
    "exit_code",
    "timeout",
})
CODEX_SUPPORTED_INCLUDE_VALUES = {
    CODEX_DEFAULT_INCLUDE,
    "web_search_call.action.sources",
    "web_search_call.results",
}
CODEX_SUPPORTED_RESPONSE_FIELDS = {
    "include",
    "input",
    "instructions",
    "context_management",
    "model",
    "parallel_tool_calls",
    "prompt_cache_key",
    "reasoning",
    "service_tier",
    "store",
    "stream",
    "stream_options",
    "text",
    "tool_choice",
    "tools",
}
CODEX_NATIVE_RESPONSE_ROUTING_FIELDS = {
    "context_management",
    "include",
    "prompt_cache_key",
    "service_tier",
    "text",
}
CODEX_NATIVE_INPUT_ITEM_TYPES = {
    "compaction",
    "compaction_summary",
    "context_compaction",
    "custom_tool_call",
    "custom_tool_call_output",
    "function_call",
    "function_call_output",
    "image_generation_call",
    "mcp_tool_call_output",
    "reasoning",
    "tool_search_call",
    "tool_search_output",
    "web_search_call",
}
SUPPORTED_RESPONSE_INPUT_ITEM_TYPES = {
    *RESPONSE_CONTENT_PART_TYPES,
    *CODEX_NATIVE_INPUT_ITEM_TYPES,
    "message",
}
CODEX_UNSUPPORTED_RESPONSE_FIELDS = {
    "background",
    "conversation",
    "generate",
    "max_completion_tokens",
    "max_output_tokens",
    "max_tokens",
    "max_tool_calls",
    "metadata",
    "previous_response_id",
    "prompt_cache_retention",
    "safety_identifier",
    "temperature",
    "top_p",
    "truncation",
    "user",
}
PLAIN_TEXT_RESPONSE_FIELDS = {
    "input",
    "instructions",
    "model",
    "parallel_tool_calls",
    "reasoning",
    "reasoning_effort",
    "store",
    "stream",
    "stream_options",
    "thinking_effort",
    "tool_choice",
    "tools",
}
IMAGE_RESPONSE_FIELDS = {
    "input",
    "model",
    "parallel_tool_calls",
    "store",
    "stream",
    "stream_options",
    "tool_choice",
    "tools",
}
IMAGE_TOOL_FIELDS = {
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
    "type",
}


def thinking_effort_from_body(body: dict[str, Any]) -> str:
    try:
        return conversation_effort_from_body(body)
    except ValueError as exc:
        raise _codex_request_error(str(exc)) from exc


def _normalize_response_stream_options(
        body: dict[str, Any],
        *,
        native: bool = False,
        websocket: bool = False,
) -> dict[str, str] | None:
    options = body.get("stream_options")
    if options is None:
        return None
    if not isinstance(options, dict):
        raise _codex_request_error("stream_options must be an object")
    allowed = {"include_obfuscation"}
    if native:
        allowed.add("reasoning_summary_delivery")
    if set(options) - allowed:
        raise _codex_request_error("stream_options is not supported by the configured backend")
    if not websocket and body.get("stream") is not True:
        raise _codex_request_error("stream_options requires stream=true")
    if "include_obfuscation" in options:
        include_obfuscation = options["include_obfuscation"]
        if type(include_obfuscation) is not bool:
            raise _codex_request_error("stream_options.include_obfuscation must be a boolean")
        if include_obfuscation:
            raise _codex_request_error("stream obfuscation is not supported by the configured backend")

    upstream_options: dict[str, str] = {}
    if "reasoning_summary_delivery" in options:
        delivery = options["reasoning_summary_delivery"]
        if delivery != "sequential_cutoff":
            raise _codex_request_error("stream_options is not supported by Codex Responses")
        upstream_options["reasoning_summary_delivery"] = delivery
    return upstream_options or None


def is_text_response_request(body: dict[str, Any]) -> bool:
    return not has_response_image_generation_tool(body)


def has_unsupported_response_tools(body: dict[str, Any]) -> bool:
    return has_unsupported_tools(body, {"image_generation", *WEB_SEARCH_TOOL_TYPES})


def has_function_tool(body: dict[str, Any]) -> bool:
    tools = body.get("tools")
    return isinstance(tools, list) and any(
        isinstance(tool, dict) and str(tool.get("type") or "").strip() == "function"
        for tool in tools
    )


def has_native_codex_tool(body: dict[str, Any]) -> bool:
    return has_function_tool(body) or has_web_search_tool(body)


def _is_response_message_input(item: dict[str, Any]) -> bool:
    item_type = item.get("type")
    return item_type == "message" or (
        item_type is None
        and "role" in item
        and "content" in item
    )


def has_native_codex_input_history(body: dict[str, Any]) -> bool:
    input_value = body.get("input")
    items = input_value if isinstance(input_value, list) else [input_value]
    for item in items:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "input_audio":
            return True
        if item.get("type") == "input_image" and item.get("detail") is not None:
            return True
        if item.get("type") in CODEX_NATIVE_INPUT_ITEM_TYPES:
            return True
        if not _is_response_message_input(item):
            continue
        if item.get("role") == "developer":
            return True
        if item.get("phase") is not None:
            return True
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") == "input_audio":
                return True
            if part.get("type") == "input_image" and part.get("detail") is not None:
                return True
    return False


def has_native_codex_response_controls(body: dict[str, Any]) -> bool:
    if any(
        body.get(field) is not None
        for field in CODEX_NATIVE_RESPONSE_ROUTING_FIELDS
    ):
        return True
    reasoning = body.get("reasoning")
    if isinstance(reasoning, dict) and any(
        field in reasoning
        for field in ("summary", "context")
    ):
        return True
    stream_options = body.get("stream_options")
    if (
        isinstance(stream_options, dict)
        and "reasoning_summary_delivery" in stream_options
    ):
        return True
    return body.get("tool_choice") == "auto"


def uses_native_codex_responses(body: dict[str, Any]) -> bool:
    return has_native_codex_tool(body) or has_native_codex_input_history(body) or (
        is_text_response_request(body)
        and has_native_codex_response_controls(body)
    )


def _codex_request_error(message: str) -> HTTPException:
    return HTTPException(status_code=400, detail={"error": message})


def _is_supported_string_enum(value: object, supported: set[str] | frozenset[str]) -> bool:
    return isinstance(value, str) and value in supported


def _validate_response_content_part(part: dict[str, Any]) -> None:
    part_type = part.get("type")
    if part_type == "input_text":
        if set(part) - {"type", "text"}:
            raise _codex_request_error("message content part fields are not supported")
        if not isinstance(part.get("text"), str):
            raise _codex_request_error("message text must be a string")
        return
    if part_type == "output_text":
        if set(part) - {"type", "text", "annotations", "logprobs"}:
            raise _codex_request_error("message content part fields are not supported")
        if not isinstance(part.get("text"), str):
            raise _codex_request_error("message text must be a string")
        for field in ("annotations", "logprobs"):
            value = part.get(field)
            if value is not None and not isinstance(value, list):
                raise _codex_request_error(f"message output_text {field} must be an array")
        return
    if part_type == "input_image":
        if set(part) - {"type", "image_url", "detail"}:
            raise _codex_request_error("message image fields are not supported")
        image_url = part.get("image_url")
        if not isinstance(image_url, str) or not image_url.strip():
            raise _codex_request_error("message image_url must be a non-empty string")
        detail = part.get("detail")
        if detail is not None and not _is_supported_string_enum(detail, SUPPORTED_RESPONSE_IMAGE_DETAILS):
            raise _codex_request_error("message image detail is not supported")
        return
    if part_type == "input_audio":
        if set(part) - {"type", "audio_url"}:
            raise _codex_request_error("message audio fields are not supported")
        audio_url = part.get("audio_url")
        if not isinstance(audio_url, str) or not audio_url.strip():
            raise _codex_request_error("message audio_url must be a non-empty string")
        return
    raise _codex_request_error("message content part type is not supported")


def _validate_tool_call_output_body(output: object) -> None:
    if isinstance(output, str):
        return
    if not isinstance(output, list):
        raise _codex_request_error("tool call output must be a string or array")

    for part in output:
        if not isinstance(part, dict):
            raise _codex_request_error("tool call output must contain objects")
        part_type = part.get("type")
        if not _is_supported_string_enum(part_type, SUPPORTED_TOOL_CALL_OUTPUT_PART_TYPES):
            raise _codex_request_error("tool call output content type is not supported")
        if part_type == "encrypted_content":
            if set(part) != {"type", "encrypted_content"} or not isinstance(
                part.get("encrypted_content"), str
            ):
                raise _codex_request_error("function call encrypted content is invalid")
            continue
        _validate_response_content_part(part)


def _validate_output_item_identity(item: dict[str, Any], item_name: str) -> None:
    item_id = item.get("id")
    if item_id is not None and (
        not isinstance(item_id, str) or not item_id.strip()
    ):
        raise _codex_request_error(f"{item_name} id must be a non-empty string")

    call_id = item.get("call_id")
    if not isinstance(call_id, str) or not call_id.strip():
        raise _codex_request_error(f"{item_name} call_id must be a non-empty string")


def _validate_function_call_output(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_FUNCTION_CALL_OUTPUT_FIELDS:
        raise _codex_request_error("function call output fields are not supported")
    _validate_output_item_identity(item, "function call output")

    status = item.get("status")
    if status is not None and not _is_supported_string_enum(status, SUPPORTED_FUNCTION_CALL_OUTPUT_STATUSES):
        raise _codex_request_error("function call output status is not supported")
    _validate_tool_call_output_body(item.get("output"))


def _validate_custom_tool_call_output(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_CUSTOM_TOOL_CALL_OUTPUT_FIELDS:
        raise _codex_request_error("custom tool call output fields are not supported")
    _validate_output_item_identity(item, "custom tool call output")

    name = item.get("name")
    if name is not None and (not isinstance(name, str) or not name.strip()):
        raise _codex_request_error("custom tool call output name must be a non-empty string")
    _validate_tool_call_output_body(item.get("output"))


def _validate_tool_call_identity(item: dict[str, Any], item_name: str) -> None:
    _validate_output_item_identity(item, item_name)
    name = item.get("name")
    if not isinstance(name, str) or not name.strip():
        raise _codex_request_error(f"{item_name} name must be a non-empty string")
    namespace = item.get("namespace")
    if namespace is not None and (
        not isinstance(namespace, str) or not namespace.strip()
    ):
        raise _codex_request_error(f"{item_name} namespace must be a non-empty string")
    status = item.get("status")
    if status is not None and not _is_supported_string_enum(status, SUPPORTED_TOOL_CALL_STATUSES):
        raise _codex_request_error(f"{item_name} status is not supported")


def _validate_function_call(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_FUNCTION_CALL_FIELDS:
        raise _codex_request_error("function call fields are not supported")
    _validate_tool_call_identity(item, "function call")
    if not isinstance(item.get("arguments"), str):
        raise _codex_request_error("function call arguments must be a string")
    encrypted_args = item.get("encrypted_function_args")
    if encrypted_args is not None and (
        not isinstance(encrypted_args, list)
        or any(not isinstance(value, str) for value in encrypted_args)
    ):
        raise _codex_request_error("function call encrypted arguments must be an array of strings")


def _validate_custom_tool_call(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_CUSTOM_TOOL_CALL_FIELDS:
        raise _codex_request_error("custom tool call fields are not supported")
    _validate_tool_call_identity(item, "custom tool call")
    if not isinstance(item.get("input"), str):
        raise _codex_request_error("custom tool call input must be a string")


def _validate_optional_history_id(item: dict[str, Any], item_name: str) -> None:
    item_id = item.get("id")
    if item_id is not None and (
        not isinstance(item_id, str) or not item_id.strip()
    ):
        raise _codex_request_error(f"{item_name} id must be a non-empty string")


def _validate_reasoning_history(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_REASONING_FIELDS:
        raise _codex_request_error("reasoning fields are not supported")
    _validate_optional_history_id(item, "reasoning")
    status = item.get("status")
    if status is not None and not _is_supported_string_enum(status, SUPPORTED_TOOL_CALL_STATUSES):
        raise _codex_request_error("reasoning status is not supported")

    summary = item.get("summary")
    if not isinstance(summary, list):
        raise _codex_request_error("reasoning summary must be an array")
    for part in summary:
        if (
            not isinstance(part, dict)
            or set(part) != {"type", "text"}
            or part.get("type") != "summary_text"
            or not isinstance(part.get("text"), str)
        ):
            raise _codex_request_error("reasoning summary content is invalid")

    content = item.get("content")
    if content is not None:
        if not isinstance(content, list):
            raise _codex_request_error("reasoning content must be an array")
        for part in content:
            if (
                not isinstance(part, dict)
                or set(part) != {"type", "text"}
                or not _is_supported_string_enum(part.get("type"), {"reasoning_text", "text"})
                or not isinstance(part.get("text"), str)
            ):
                raise _codex_request_error("reasoning content is invalid")

    encrypted_content = item.get("encrypted_content")
    if encrypted_content is not None and not isinstance(encrypted_content, str):
        raise _codex_request_error("reasoning encrypted_content must be a string")


def _validate_compaction_history(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_COMPACTION_FIELDS:
        raise _codex_request_error("compaction fields are not supported")
    _validate_optional_history_id(item, "compaction")
    encrypted_content = item.get("encrypted_content")
    if item.get("type") in {"compaction", "compaction_summary"}:
        if not isinstance(encrypted_content, str):
            raise _codex_request_error("compaction encrypted_content must be a string")
        return
    if encrypted_content is not None and not isinstance(encrypted_content, str):
        raise _codex_request_error("context compaction encrypted_content must be a string")


def _validate_image_generation_history(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_IMAGE_GENERATION_CALL_FIELDS:
        raise _codex_request_error("image generation call fields are not supported")
    _validate_optional_history_id(item, "image generation call")
    if not _is_supported_string_enum(item.get("status"), SUPPORTED_IMAGE_GENERATION_CALL_STATUSES):
        raise _codex_request_error("image generation call status is not supported")
    if not isinstance(item.get("result"), str):
        raise _codex_request_error("image generation call result must be a string")
    revised_prompt = item.get("revised_prompt")
    if revised_prompt is not None and not isinstance(revised_prompt, str):
        raise _codex_request_error("image generation revised_prompt must be a string")


def _validate_web_search_action(action: object) -> None:
    if not isinstance(action, dict):
        raise _codex_request_error("web search action must be an object")
    action_type = action.get("type")
    fields_by_type = {
        "search": {"type", "query", "queries"},
        "open_page": {"type", "url"},
        "find_in_page": {"type", "url", "pattern"},
    }
    allowed_fields = fields_by_type.get(action_type) if isinstance(action_type, str) else None
    if allowed_fields is None or set(action) - allowed_fields:
        raise _codex_request_error("web search action is not supported by Codex Responses")
    query = action.get("query")
    if query is not None and not isinstance(query, str):
        raise _codex_request_error("web search query must be a string")
    queries = action.get("queries")
    if queries is not None and (
        not isinstance(queries, list)
        or any(not isinstance(value, str) for value in queries)
    ):
        raise _codex_request_error("web search queries must be an array of strings")
    for field in ("url", "pattern"):
        value = action.get(field)
        if value is not None and not isinstance(value, str):
            raise _codex_request_error(f"web search {field} must be a string")


def _validate_web_search_history(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_WEB_SEARCH_CALL_FIELDS:
        raise _codex_request_error("web search call fields are not supported")
    _validate_optional_history_id(item, "web search call")
    status = item.get("status")
    if status is not None and not _is_supported_string_enum(status, SUPPORTED_WEB_SEARCH_CALL_STATUSES):
        raise _codex_request_error("web search call status is not supported")
    action = item.get("action")
    if action is not None:
        _validate_web_search_action(action)


def _validate_optional_call_id(item: dict[str, Any], item_name: str) -> None:
    call_id = item.get("call_id")
    if call_id is not None and (
        not isinstance(call_id, str) or not call_id.strip()
    ):
        raise _codex_request_error(f"{item_name} call_id must be a non-empty string")


def _validate_tool_search_common(item: dict[str, Any], item_name: str) -> None:
    _validate_optional_history_id(item, item_name)
    _validate_optional_call_id(item, item_name)
    status = item.get("status")
    if status is not None and not _is_supported_string_enum(status, SUPPORTED_TOOL_CALL_STATUSES):
        raise _codex_request_error(f"{item_name} status is not supported")
    if not _is_supported_string_enum(item.get("execution"), SUPPORTED_TOOL_SEARCH_EXECUTIONS):
        raise _codex_request_error(f"{item_name} execution is not supported")


def _validate_tool_search_call(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_TOOL_SEARCH_CALL_FIELDS:
        raise _codex_request_error("tool search call fields are not supported")
    _validate_tool_search_common(item, "tool search call")
    if "arguments" not in item:
        raise _codex_request_error("tool search call arguments are required")


def _validate_tool_search_output(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_TOOL_SEARCH_OUTPUT_FIELDS:
        raise _codex_request_error("tool search output fields are not supported")
    _validate_tool_search_common(item, "tool search output")
    if item.get("status") is None:
        raise _codex_request_error("tool search output status is required")
    tools = item.get("tools")
    if not isinstance(tools, list) or any(not isinstance(tool, dict) for tool in tools):
        raise _codex_request_error("tool search output tools must be an array of objects")


def _validate_mcp_tool_call_output(item: dict[str, Any]) -> None:
    if set(item) - SUPPORTED_MCP_TOOL_CALL_OUTPUT_FIELDS:
        raise _codex_request_error("MCP tool call output fields are not supported")
    call_id = item.get("call_id")
    if not isinstance(call_id, str) or not call_id.strip():
        raise _codex_request_error("MCP tool call output call_id must be a non-empty string")
    if not isinstance(item.get("output"), dict):
        raise _codex_request_error("MCP tool call output must be an object")


def _normalize_codex_reasoning(value: object) -> dict[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) - {"effort", "summary", "context"}:
        raise _codex_request_error("reasoning contains unsupported fields")
    normalized: dict[str, str] = {}
    effort = value.get("effort")
    if effort is not None:
        if not isinstance(effort, str) or not effort.strip():
            raise _codex_request_error("reasoning effort must be a non-empty string")
        normalized["effort"] = effort.strip().lower()
    summary = value.get("summary")
    if summary is not None:
        if not isinstance(summary, str) or summary not in {"auto", "concise", "detailed", "none"}:
            raise _codex_request_error("reasoning summary is not supported by Codex Responses")
        normalized["summary"] = summary
    context = value.get("context")
    if context is not None:
        if not isinstance(context, str) or context not in {"auto", "current_turn", "all_turns"}:
            raise _codex_request_error("reasoning context is not supported by Codex Responses")
        normalized["context"] = context
    return normalized


_TEXT_FORMAT_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _normalize_codex_text(value: object) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) - {"verbosity", "format"}:
        raise _codex_request_error("text must contain only verbosity and format")

    normalized: dict[str, Any] = {}
    verbosity = value.get("verbosity")
    if verbosity is not None:
        if not isinstance(verbosity, str) or verbosity not in {"low", "medium", "high"}:
            raise _codex_request_error("text verbosity is not supported by Codex Responses")
        normalized["verbosity"] = verbosity

    text_format = value.get("format")
    if text_format is None:
        return normalized or None
    if not isinstance(text_format, dict):
        raise _codex_request_error("text format must be an object")

    format_type = text_format.get("type")
    if format_type == "text":
        if set(text_format) != {"type"}:
            raise _codex_request_error("plain text format does not accept additional fields")
        return normalized or None
    if format_type != "json_schema":
        raise _codex_request_error("text format is not supported by Codex Responses")
    if set(text_format) - {"type", "name", "schema", "strict"}:
        raise _codex_request_error("JSON schema format contains unsupported fields")

    name = text_format.get("name")
    schema = text_format.get("schema")
    strict = text_format.get("strict")
    if not isinstance(name, str) or _TEXT_FORMAT_NAME_RE.fullmatch(name) is None:
        raise _codex_request_error("JSON schema format name is invalid")
    if not isinstance(schema, dict):
        raise _codex_request_error("JSON schema format schema must be an object")
    if strict is not None and type(strict) is not bool:
        raise _codex_request_error("JSON schema format strict must be a boolean")
    normalized["format"] = {
        "type": "json_schema",
        "name": name,
        "schema": copy.deepcopy(schema),
        "strict": bool(strict),
    }
    return normalized


def validate_tool_container(body: dict[str, Any]) -> None:
    tools = body.get("tools")
    if tools is None:
        return
    if not isinstance(tools, list):
        raise _codex_request_error("tools must be an array")
    for tool in tools:
        if not isinstance(tool, dict):
            raise _codex_request_error("tools must contain objects")
        tool_type = tool.get("type")
        if not isinstance(tool_type, str) or not tool_type.strip():
            raise _codex_request_error("tool type is required")


def validate_response_core_parameters(body: dict[str, Any]) -> None:
    model = body.get("model")
    if model is not None and not isinstance(model, str):
        raise _codex_request_error("model must be a string")
    instructions = body.get("instructions")
    if instructions is not None and not isinstance(instructions, str):
        raise _codex_request_error("instructions must be a string")
    input_value = body.get("input")
    if input_value is not None and (
        not isinstance(input_value, (str, dict, list))
        or isinstance(input_value, list)
        and any(not isinstance(item, dict) for item in input_value)
    ):
        raise _codex_request_error("input must be a string, object, or array of objects")
    input_items = input_value if isinstance(input_value, list) else [input_value]
    for item in input_items:
        if not isinstance(item, dict):
            continue
        if "type" not in item and not _is_response_message_input(item):
            raise _codex_request_error("input item type is required")
        if "type" in item:
            item_type = item.get("type")
            if not isinstance(item_type, str) or item_type not in SUPPORTED_RESPONSE_INPUT_ITEM_TYPES:
                raise _codex_request_error("input item type is not supported")
            if item_type in SUPPORTED_RESPONSE_MESSAGE_CONTENT_PART_TYPES:
                _validate_response_content_part(item)
            if item_type == "function_call_output":
                _validate_function_call_output(item)
            if item_type == "custom_tool_call_output":
                _validate_custom_tool_call_output(item)
            if item_type == "function_call":
                _validate_function_call(item)
            if item_type == "custom_tool_call":
                _validate_custom_tool_call(item)
            if item_type == "reasoning":
                _validate_reasoning_history(item)
            if item_type in {"compaction", "compaction_summary", "context_compaction"}:
                _validate_compaction_history(item)
            if item_type == "image_generation_call":
                _validate_image_generation_history(item)
            if item_type == "web_search_call":
                _validate_web_search_history(item)
            if item_type == "tool_search_call":
                _validate_tool_search_call(item)
            if item_type == "tool_search_output":
                _validate_tool_search_output(item)
            if item_type == "mcp_tool_call_output":
                _validate_mcp_tool_call_output(item)
        if not _is_response_message_input(item):
            continue
        if set(item) - SUPPORTED_RESPONSE_MESSAGE_FIELDS:
            raise _codex_request_error("message fields are not supported")
        role = item.get("role")
        if not isinstance(role, str) or role not in SUPPORTED_RESPONSE_MESSAGE_ROLES:
            raise _codex_request_error("message role is not supported")
        message_id = item.get("id")
        if message_id is not None and (
            not isinstance(message_id, str) or not message_id.strip()
        ):
            raise _codex_request_error("message id must be a non-empty string")
        phase = item.get("phase")
        if phase is not None and (
            role != "assistant" or not _is_supported_string_enum(phase, SUPPORTED_RESPONSE_MESSAGE_PHASES)
        ):
            raise _codex_request_error("message phase is not supported")
        status = item.get("status")
        if status is not None and not _is_supported_string_enum(status, SUPPORTED_RESPONSE_MESSAGE_STATUSES):
            raise _codex_request_error("message status is not supported")
        content = item.get("content")
        if isinstance(content, str):
            continue
        if not isinstance(content, list):
            raise _codex_request_error("message content must be a string or array")
        for part in content:
            if not isinstance(part, dict):
                raise _codex_request_error("message content must contain objects")
            part_type = part.get("type")
            if (
                not isinstance(part_type, str)
                or part_type not in SUPPORTED_RESPONSE_MESSAGE_CONTENT_PART_TYPES
            ):
                raise _codex_request_error("message content part type is not supported")
            _validate_response_content_part(part)
    stream = body.get("stream")
    if stream is not None and type(stream) is not bool:
        raise _codex_request_error("stream must be a boolean")


def validate_plain_text_parameters(body: dict[str, Any]) -> None:
    if any(
        value is not None and key not in PLAIN_TEXT_RESPONSE_FIELDS
        for key, value in body.items()
    ):
        raise _codex_request_error("parameter is not supported by the upstream text backend")
    store = body.get("store")
    if store is not None and store is not False:
        raise _codex_request_error("store=true is not supported by the upstream text backend")
    parallel_tool_calls = body.get("parallel_tool_calls")
    if parallel_tool_calls is not None and type(parallel_tool_calls) is not bool:
        raise _codex_request_error("parallel_tool_calls must be a boolean")
    tool_choice = body.get("tool_choice")
    if tool_choice is not None and tool_choice != "none":
        raise _codex_request_error("tool_choice requires a supported tool")
    thinking_effort_from_body(body)
    _normalize_response_stream_options(body)


def validate_image_response_parameters(body: dict[str, Any]) -> None:
    if any(
        value is not None and key not in IMAGE_RESPONSE_FIELDS
        for key, value in body.items()
    ):
        raise _codex_request_error("parameter is not supported by the upstream image backend")
    store = body.get("store")
    if store is not None and store is not False:
        raise _codex_request_error("store=true is not supported by the upstream image backend")
    parallel_tool_calls = body.get("parallel_tool_calls")
    if parallel_tool_calls is not None and type(parallel_tool_calls) is not bool:
        raise _codex_request_error("parallel_tool_calls must be a boolean")
    _normalize_response_stream_options(body)

    tools = body.get("tools")
    if not isinstance(tools, list) or len(tools) != 1:
        raise _codex_request_error("exactly one image_generation tool is required")
    tool = tools[0]
    if not isinstance(tool, dict) or tool.get("type") != "image_generation":
        raise _codex_request_error("exactly one image_generation tool is required")
    if any(value is not None and key not in IMAGE_TOOL_FIELDS for key, value in tool.items()):
        raise _codex_request_error("image_generation tool parameter is not supported")
    try:
        tool["quality"] = normalize_image_quality(tool.get("quality"))
        tool["size"] = normalize_image_size(tool.get("size"))
        tool["output_format"] = normalize_image_output_format(tool.get("output_format"))
        tool["output_compression"] = normalize_image_output_compression(
            tool.get("output_compression"),
            tool["output_format"],
        )
        tool["background"] = normalize_supported_image_background(
            tool.get("background"),
            allow_non_auto=is_codex_image_model(tool.get("model")),
            output_format=tool["output_format"],
        )
        tool["moderation"] = normalize_supported_image_moderation(tool.get("moderation"))
        tool["partial_images"] = normalize_supported_partial_images(tool.get("partial_images"))
    except PublicSafeValueError as exc:
        raise _codex_request_error(exc.public_safe_message()) from exc
    action = tool.get("action")
    if action is None:
        action = "auto"
    if not isinstance(action, str) or action not in {"auto", "generate", "edit"}:
        raise _codex_request_error("image_generation action must be auto, generate, or edit")
    tool["action"] = action
    if tool.get("input_fidelity") is not None:
        raise _codex_request_error("image_generation input_fidelity is not supported")
    if tool.get("input_image_mask") is not None:
        raise _codex_request_error("image_generation input_image_mask is not supported")
    image_model = tool.get("model")
    if image_model is not None:
        if not isinstance(image_model, str) or not is_supported_image_model(image_model.strip()):
            raise _codex_request_error("image_generation model is not supported")
        tool["model"] = image_model.strip()

    tool_choice = body.get("tool_choice")
    if tool_choice is None or tool_choice == "required":
        return
    if (
        isinstance(tool_choice, dict)
        and tool_choice.get("type") == "image_generation"
        and set(tool_choice) == {"type"}
    ):
        return
    raise _codex_request_error("tool_choice must select image_generation")


def _normalize_codex_tool(tool: object) -> dict[str, Any]:
    if not isinstance(tool, dict):
        raise _codex_request_error("tools must contain objects")
    normalized = copy.deepcopy(tool)
    raw_type = normalized.get("type")
    if not isinstance(raw_type, str) or raw_type != raw_type.strip():
        raise _codex_request_error("tool type is not supported by Codex Responses")
    tool_type = raw_type
    if tool_type in WEB_SEARCH_TOOL_TYPES:
        return _normalize_codex_web_search_tool(normalized, tool_type)
    if tool_type not in {"function", "image_generation", "web_search"}:
        raise _codex_request_error("tool type is not supported by Codex Responses")
    if tool_type == "function":
        allowed_fields = {"type", "name", "description", "parameters", "strict", "defer_loading"}
        if set(normalized) - allowed_fields:
            raise _codex_request_error("function tool contains unsupported fields")
        name = normalized.get("name")
        if not isinstance(name, str) or not name.strip():
            raise _codex_request_error("function tool name is required")
        normalized["name"] = name.strip()
        description = normalized.get("description")
        if description is not None and not isinstance(description, str):
            raise _codex_request_error("function tool description must be a string")
        normalized["description"] = description or ""
        parameters = normalized.get("parameters")
        if parameters is not None and not isinstance(parameters, dict):
            raise _codex_request_error("function tool parameters must be an object")
        normalized["parameters"] = parameters or {}
        strict = normalized.get("strict")
        if strict is not None and type(strict) is not bool:
            raise _codex_request_error("function tool strict must be a boolean")
        normalized["strict"] = False if strict is None else strict
        defer_loading = normalized.get("defer_loading")
        if defer_loading is not None and type(defer_loading) is not bool:
            raise _codex_request_error("function tool defer_loading must be a boolean")
        if defer_loading is not True:
            normalized.pop("defer_loading", None)
    return normalized


def _normalize_codex_web_search_tool(tool: dict[str, Any], tool_type: str) -> dict[str, Any]:
    is_preview = tool_type in {"web_search_preview", "web_search_preview_2025_03_11"}
    allowed_fields = {"type", "search_context_size", "search_content_types", "user_location"}
    if not is_preview:
        allowed_fields.update({"external_web_access", "filters"})
    if set(tool) - allowed_fields:
        raise _codex_request_error("web search tool contains unsupported fields")

    normalized: dict[str, Any] = {"type": "web_search"}
    if "search_context_size" in tool:
        context_size = tool.get("search_context_size")
        if not isinstance(context_size, str) or context_size not in {"low", "medium", "high"}:
            raise _codex_request_error("web search context size is invalid")
        normalized["search_context_size"] = context_size

    if "search_content_types" in tool:
        content_types = tool.get("search_content_types")
        if (
            not isinstance(content_types, list)
            or any(not isinstance(value, str) or value not in {"text", "image"} for value in content_types)
        ):
            raise _codex_request_error("web search content types are invalid")
        normalized["search_content_types"] = list(content_types)

    if not is_preview and tool.get("filters") is not None:
        filters = tool.get("filters")
        if not isinstance(filters, dict) or set(filters) - {"allowed_domains"}:
            raise _codex_request_error("web search filters are invalid")
        normalized_filters: dict[str, Any] = {}
        allowed_domains = filters.get("allowed_domains")
        if allowed_domains is not None:
            if (
                not isinstance(allowed_domains, list)
                or len(allowed_domains) > 100
                or any(
                    not isinstance(domain, str)
                    or not domain
                    or domain != domain.strip()
                    or "://" in domain
                    or any(separator in domain for separator in "/?#@")
                    for domain in allowed_domains
                )
            ):
                raise _codex_request_error("web search allowed domains are invalid")
            normalized_filters["allowed_domains"] = list(allowed_domains)
        normalized["filters"] = normalized_filters

    if not is_preview and "external_web_access" in tool:
        external_web_access = tool.get("external_web_access")
        if type(external_web_access) is not bool:
            raise _codex_request_error("web search external web access must be a boolean")
        normalized["external_web_access"] = external_web_access

    if tool.get("user_location") is not None:
        location = tool.get("user_location")
        location_fields = {"type", "city", "country", "region", "timezone"}
        if not isinstance(location, dict) or set(location) - location_fields:
            raise _codex_request_error("web search user location is invalid")
        location_type = location.get("type", "approximate")
        if location_type != "approximate":
            raise _codex_request_error("web search user location type is invalid")
        normalized_location: dict[str, Any] = {"type": "approximate"}
        for field in ("city", "country", "region", "timezone"):
            value = location.get(field)
            if value is None:
                continue
            if not isinstance(value, str):
                raise _codex_request_error("web search user location values must be strings")
            if field == "country" and (
                len(value) != 2 or not value.isascii() or not value.isalpha()
            ):
                raise _codex_request_error("web search user location country is invalid")
            normalized_location[field] = value
        normalized["user_location"] = normalized_location
    return normalized


def _normalize_codex_tool_choice(value: object) -> str:
    if value != "auto":
        raise _codex_request_error("tool_choice is not supported by Codex Responses")
    return "auto"


def _normalize_codex_include(value: object) -> list[str]:
    if value is None:
        return [CODEX_DEFAULT_INCLUDE]
    if not isinstance(value, (list, tuple)):
        raise _codex_request_error("include must be an array")
    normalized = [CODEX_DEFAULT_INCLUDE]
    for item in value:
        if not isinstance(item, str) or item not in CODEX_SUPPORTED_INCLUDE_VALUES:
            raise _codex_request_error("include is not supported by Codex Responses")
        if item not in normalized:
            normalized.append(item)
    return normalized


def _normalize_context_management(value: object) -> list[dict[str, Any]] | None:
    if value is None:
        return None
    if not isinstance(value, list):
        raise _codex_request_error("context_management must be an array")

    normalized: list[dict[str, Any]] = []
    for entry in value:
        if not isinstance(entry, dict) or set(entry) - {"type", "compact_threshold"}:
            raise _codex_request_error(
                "context_management entries must contain only type and compact_threshold"
            )
        if entry.get("type") != "compaction":
            raise _codex_request_error("context_management type must be compaction")

        item: dict[str, Any] = {"type": "compaction"}
        if "compact_threshold" in entry:
            threshold = entry.get("compact_threshold")
            if threshold is not None and (
                isinstance(threshold, bool)
                or not isinstance(threshold, (int, float))
                or not math.isfinite(threshold)
                or threshold < 1000
            ):
                raise _codex_request_error(
                    "compact_threshold must be a finite number greater than or equal to 1000"
                )
            item["compact_threshold"] = threshold
        normalized.append(item)
    return normalized


def _codex_input(value: object) -> list[dict[str, Any]]:
    if isinstance(value, str):
        return [{
            "type": "message",
            "role": "user",
            "content": [{"type": "input_text", "text": value}],
        }]
    if isinstance(value, dict):
        result = [copy.deepcopy(value)]
    elif isinstance(value, list):
        result = copy.deepcopy(value)
    else:
        raise _codex_request_error("input must be a string, object, or array")

    grouped: list[dict[str, Any]] = []
    pending_parts: list[dict[str, Any]] = []
    for item in result:
        if (
            isinstance(item, dict)
            and item.get("type") in CODEX_RESPONSE_CONTENT_PART_TYPES
        ):
            pending_parts.append(item)
            continue
        if pending_parts:
            grouped.append({
                "type": "message",
                "role": "user",
                "content": pending_parts,
            })
            pending_parts = []
        if isinstance(item, dict):
            grouped.append(item)
    if pending_parts:
        grouped.append({
            "type": "message",
            "role": "user",
            "content": pending_parts,
        })
    result = grouped

    for item in result:
        if not isinstance(item, dict):
            continue
        if _is_response_message_input(item):
            item.setdefault("type", "message")
            item.pop("status", None)
            content = item.get("content")
            if isinstance(content, list):
                item["content"] = [
                    {"type": "output_text", "text": part["text"]}
                    if isinstance(part, dict) and part.get("type") == "output_text"
                    else part
                    for part in content
                ]
        if item.get("type") == "function_call_output":
            item.pop("status", None)
        if item.get("type") == "function_call":
            item.pop("status", None)
        if item.get("type") == "reasoning":
            item.pop("status", None)
        if item.get("role") == "system":
            item["role"] = "developer"
    return result


def codex_response_payload(body: dict[str, Any], *, websocket: bool = False) -> dict[str, Any]:
    validate_response_core_parameters(body)
    for field in CODEX_UNSUPPORTED_RESPONSE_FIELDS:
        if websocket and field == "generate":
            continue
        if body.get(field) is not None:
            raise _codex_request_error("parameter is not supported by Codex Responses")
    unknown_fields = {
        key
        for key, value in body.items()
        if value is not None
        and key not in CODEX_SUPPORTED_RESPONSE_FIELDS
        and key not in CODEX_UNSUPPORTED_RESPONSE_FIELDS
    }
    if unknown_fields:
        raise _codex_request_error("parameter is not supported by Codex Responses")
    model_value = body.get("model")
    instructions = body.get("instructions")
    store = body.get("store")
    if store is not None and type(store) is not bool:
        raise _codex_request_error("store must be a boolean")
    if store is True:
        raise _codex_request_error("store=true is not supported by Codex Responses")
    parallel_tool_calls = body.get("parallel_tool_calls")
    if parallel_tool_calls is not None and type(parallel_tool_calls) is not bool:
        raise _codex_request_error("parallel_tool_calls must be a boolean")
    prompt_cache_key = body.get("prompt_cache_key")
    if prompt_cache_key is not None and not isinstance(prompt_cache_key, str):
        raise _codex_request_error("prompt_cache_key must be a string")
    service_tier = body.get("service_tier")
    if service_tier is not None and not _is_supported_string_enum(
        service_tier,
        {"default", "priority", "flex"},
    ):
        raise _codex_request_error("service_tier is not supported by Codex Responses")
    include = _normalize_codex_include(body.get("include"))
    generate = body.get("generate")
    if websocket and "generate" in body and generate is not False:
        raise _codex_request_error("generate must be false in Responses WebSocket mode")

    tools = body.get("tools")
    if tools is not None and not isinstance(tools, list):
        raise _codex_request_error("tools must be an array")
    normalized_tools = (
        [_normalize_codex_tool(tool) for tool in tools]
        if isinstance(tools, list)
        else None
    )
    stream_options = _normalize_response_stream_options(
        body,
        native=True,
        websocket=websocket,
    )
    reasoning = _normalize_codex_reasoning(body.get("reasoning"))
    text_controls = _normalize_codex_text(body.get("text"))
    context_management = _normalize_context_management(body.get("context_management"))

    requested_model = str(model_value or "auto").strip() or "auto"
    payload: dict[str, Any] = {
        "model": CODEX_RESPONSES_MODEL if requested_model == "auto" else requested_model,
        "instructions": instructions if instructions is not None else "",
        "input": _codex_input(body.get("input")),
        "stream": True,
        "store": False,
        "tool_choice": "auto",
        "parallel_tool_calls": True if parallel_tool_calls is None else parallel_tool_calls,
        "include": include,
    }
    if normalized_tools is not None:
        payload["tools"] = normalized_tools
    if stream_options is not None:
        payload["stream_options"] = copy.deepcopy(stream_options)
    for field in ("prompt_cache_key",):
        if body.get(field) is not None:
            payload[field] = copy.deepcopy(body[field])
    if reasoning is not None:
        payload["reasoning"] = reasoning
    if text_controls is not None:
        payload["text"] = text_controls
    if context_management is not None:
        payload["context_management"] = context_management
    if service_tier in {"priority", "flex"}:
        payload["service_tier"] = service_tier
    if websocket and generate is False:
        payload["generate"] = False
    if body.get("tool_choice") is not None:
        payload["tool_choice"] = _normalize_codex_tool_choice(body.get("tool_choice"))
    return payload


def resolve_codex_reasoning_effort(
        payload: dict[str, Any],
        *,
        access_token: str,
) -> None:
    reasoning = payload.get("reasoning")
    if not isinstance(reasoning, dict) or "effort" not in reasoning:
        return
    normalized = model_catalog_service.normalize_reasoning_effort(
        str(payload.get("model") or "").strip(),
        reasoning["effort"],
        access_token=access_token,
    )
    if normalized:
        reasoning["effort"] = normalized
        return
    reasoning.pop("effort", None)
    if not reasoning:
        payload.pop("reasoning", None)


def _codex_usage_count(source: dict[str, Any], field: str, *, required: bool = False) -> int:
    if field not in source:
        if required:
            raise RuntimeError("codex returned malformed codex usage")
        return 0
    value = source[field]
    if type(value) is not int or value < 0:
        raise RuntimeError("codex returned malformed codex usage")
    return value


def _project_codex_usage_details(
        usage: dict[str, Any],
        field: str,
        *,
        required_field: str,
        optional_fields: tuple[str, ...],
) -> dict[str, int] | None:
    value = usage.get(field)
    if value is None:
        return None
    if not isinstance(value, dict):
        raise RuntimeError("codex returned malformed codex usage")
    projected = {required_field: _codex_usage_count(value, required_field, required=True)}
    for optional_field in optional_fields:
        if optional_field in value:
            projected[optional_field] = _codex_usage_count(value, optional_field)
    return projected


def validated_codex_usage(usage: object) -> dict[str, Any] | None:
    if usage is None:
        return None
    if not isinstance(usage, dict):
        raise RuntimeError("codex returned malformed codex usage")

    projected: dict[str, Any] = {
        "input_tokens": _codex_usage_count(usage, "input_tokens", required=True),
        "output_tokens": _codex_usage_count(usage, "output_tokens", required=True),
        "total_tokens": _codex_usage_count(usage, "total_tokens", required=True),
    }
    input_details = _project_codex_usage_details(
        usage,
        "input_tokens_details",
        required_field="cached_tokens",
        optional_fields=("cache_write_tokens", "text_tokens", "image_tokens"),
    )
    if input_details is not None:
        projected["input_tokens_details"] = input_details
    output_details = _project_codex_usage_details(
        usage,
        "output_tokens_details",
        required_field="reasoning_tokens",
        optional_fields=("text_tokens", "image_tokens"),
    )
    if output_details is not None:
        projected["output_tokens_details"] = output_details
    return projected


def _terminal_has_malformed_citation(response: dict[str, Any]) -> bool:
    output = response.get("output")
    if not isinstance(output, list):
        return False
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            annotations = part.get("annotations")
            if not isinstance(annotations, list):
                continue
            for annotation in annotations:
                if not isinstance(annotation, dict) or annotation.get("type") != "url_citation":
                    continue
                if (
                    not isinstance(annotation.get("url"), str)
                    or not isinstance(annotation.get("title"), str)
                    or type(annotation.get("start_index")) is not int
                    or annotation.get("start_index") < 0
                    or type(annotation.get("end_index")) is not int
                    or annotation.get("end_index") < 0
                ):
                    return True
    return False


def terminal_response_from_event(event: dict[str, Any]) -> dict[str, Any] | None:
    event_type = event.get("type")
    expected_status = {
        "response.completed": "completed",
        "response.incomplete": "incomplete",
    }.get(event_type)
    if expected_status is None:
        return None

    response = event.get("response")
    if not isinstance(response, dict):
        raise RuntimeError("codex returned a malformed terminal response")
    response_id = response.get("id")
    if not isinstance(response_id, str) or not response_id.strip():
        raise RuntimeError("codex returned a malformed terminal response")
    status = response.get("status")
    if status is not None and status != expected_status:
        raise RuntimeError("codex returned a malformed terminal response")
    model = response.get("model")
    if model is not None and (not isinstance(model, str) or not model.strip()):
        raise RuntimeError("codex returned a malformed terminal response")
    created_at = response.get("created_at")
    if created_at is not None and (type(created_at) is not int or created_at < 0):
        raise RuntimeError("codex returned a malformed terminal response")
    if "output" in response and not isinstance(response.get("output"), list):
        raise RuntimeError("codex returned a malformed terminal response")
    if response.get("error") is not None:
        raise RuntimeError("codex returned a malformed terminal response")
    incomplete_details = response.get("incomplete_details")
    projected_incomplete_details: dict[str, str] | None = None
    if expected_status == "completed":
        if incomplete_details is not None:
            raise RuntimeError("codex returned a malformed terminal response")
    elif incomplete_details is not None:
        if not isinstance(incomplete_details, dict):
            raise RuntimeError("codex returned a malformed terminal response")
        reason = incomplete_details.get("reason")
        if reason is not None and (
            not isinstance(reason, str)
            or reason not in {"max_output_tokens", "content_filter"}
        ):
            raise RuntimeError("codex returned a malformed terminal response")
        projected_incomplete_details = {} if reason is None else {"reason": reason}

    validated_usage = (
        validated_codex_usage(response.get("usage"))
        if "usage" in response
        else None
    )
    try:
        normalized = _project_public_codex_value(response, field="response")
    except RuntimeError:
        if _terminal_has_malformed_citation(response):
            raise RuntimeError("codex returned a malformed citation") from None
        raise
    normalized["status"] = expected_status
    if projected_incomplete_details is not None:
        normalized["incomplete_details"] = projected_incomplete_details
    if "usage" in response:
        normalized["usage"] = validated_usage
    return normalized


def _active_response_from_event(event: dict[str, Any], event_type: str) -> dict[str, Any] | None:
    if event.get("type") != event_type:
        return None
    error_message = f"codex returned a malformed {event_type} event"
    response = event.get("response")
    if not isinstance(response, dict):
        raise RuntimeError(error_message)
    response_id = response.get("id")
    if not isinstance(response_id, str) or not response_id.strip():
        raise RuntimeError(error_message)
    status = response.get("status")
    if status is not None and status != "in_progress":
        raise RuntimeError(error_message)
    model = response.get("model")
    if model is not None and (not isinstance(model, str) or not model.strip()):
        raise RuntimeError(error_message)
    created_at = response.get("created_at")
    if created_at is not None and (type(created_at) is not int or created_at < 0):
        raise RuntimeError(error_message)
    if "output" in response and not isinstance(response.get("output"), list):
        raise RuntimeError(error_message)
    for nullable_field in ("error", "incomplete_details"):
        if response.get(nullable_field) is not None:
            raise RuntimeError(error_message)
    return copy.deepcopy(response)


def created_response_from_event(event: dict[str, Any]) -> dict[str, Any] | None:
    return _active_response_from_event(event, "response.created")


def in_progress_response_from_event(event: dict[str, Any]) -> dict[str, Any] | None:
    return _active_response_from_event(event, "response.in_progress")


def _project_public_codex_object(value: Any, allowed: set[str] | frozenset[str]) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError("codex returned malformed public response event")
    return {
        key: _project_public_codex_value(child, field=key)
        for key, child in value.items()
        if key in allowed
    }


def _project_public_codex_environment(value: Any) -> dict[str, Any]:
    return _project_public_codex_object(value, _PUBLIC_CODEX_ENVIRONMENT_FIELDS)


def _project_public_codex_operation(value: Any) -> dict[str, Any]:
    return _project_public_codex_object(value, _PUBLIC_CODEX_OPERATION_FIELDS)


def _project_public_codex_safety_checks(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(child, dict) for child in value):
        raise RuntimeError("codex returned malformed public response event")
    return [
        _project_public_codex_object(child, _PUBLIC_CODEX_SAFETY_CHECK_FIELDS)
        for child in value
    ]


def _project_public_codex_output_object(value: Any) -> dict[str, Any]:
    return _project_public_codex_object(value, _PUBLIC_CODEX_OUTPUT_FIELDS)


def _project_public_codex_outputs(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list) or any(not isinstance(child, dict) for child in value):
        raise RuntimeError("codex returned malformed public response event")
    return [_project_public_codex_output_object(child) for child in value]


def _project_public_codex_item_output(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return _project_public_codex_output_object(value)
    return _project_public_codex_outputs(value)


def _project_public_codex_value(value: Any, *, field: str | None = None) -> Any:
    if field in {"error", "item", "response", "usage"}:
        if value is not None and not isinstance(value, dict):
            raise RuntimeError("codex returned malformed public response event")
    if field == "output":
        if value is None:
            return None
        if not isinstance(value, list) or any(not isinstance(child, dict) for child in value):
            raise RuntimeError("codex returned malformed public response event")
        return [_project_public_codex_response_item(child) for child in value]
    if field == "tools":
        if value is None:
            return None
        if not isinstance(value, list) or any(not isinstance(child, dict) for child in value):
            raise RuntimeError("codex returned malformed public response event")
        return [_project_public_codex_value(child, field="tool_definition") for child in value]
    if field in {"content", "annotations"}:
        if value is None:
            return None
        if not isinstance(value, list) or any(not isinstance(child, dict) for child in value):
            raise RuntimeError("codex returned malformed public response event")
        element_field = "part" if field == "content" else "annotation"
        return [_project_public_codex_value(child, field=element_field) for child in value]
    if field == "queries":
        if value is None:
            return None
        if not isinstance(value, list) or any(not isinstance(query, str) for query in value):
            raise RuntimeError("codex returned malformed public response event")
        return list(value)
    if field == "logprobs":
        if value is None:
            return None
        if not isinstance(value, list):
            raise RuntimeError("codex returned malformed public response event")
        return [_project_public_codex_logprob(item) for item in value]
    if field == "incomplete_details":
        if value is None:
            return None
        if not isinstance(value, dict):
            raise RuntimeError("codex returned malformed public response event")
        reason = value.get("reason")
        if reason is None:
            return {}
        if not isinstance(reason, str) or reason not in _PUBLIC_CODEX_INCOMPLETE_REASONS:
            raise RuntimeError("codex returned malformed public response event")
        return {"reason": reason}
    if field in _PUBLIC_CODEX_STRING_FIELDS:
        if not isinstance(value, str):
            raise RuntimeError("codex returned malformed public response event")
        return value
    if field in _PUBLIC_CODEX_INTEGER_FIELDS:
        if type(value) is not int or value < 0:
            raise RuntimeError("codex returned malformed public response event")
        return value
    if field == "parallel_tool_calls":
        if type(value) is not bool:
            raise RuntimeError("codex returned malformed public response event")
        return value
    if field in {"strict", "defer_loading"}:
        if type(value) is not bool:
            raise RuntimeError("codex returned malformed public response event")
        return value
    if field == "parameters":
        return _project_public_codex_tool_schema(value)
    if field == "environment":
        return _project_public_codex_environment(value)
    if field == "operation":
        return _project_public_codex_operation(value)
    if field == "pending_safety_checks":
        return _project_public_codex_safety_checks(value)
    if field in {"outputs", "files"}:
        return _project_public_codex_outputs(value)
    if field == "item":
        return _project_public_codex_response_item(value)
    if isinstance(value, dict):
        if field == "error":
            allowed = _PUBLIC_CODEX_ERROR_FIELDS
        elif field == "usage":
            allowed = _PUBLIC_CODEX_USAGE_FIELDS
        elif field == "response":
            allowed = _PUBLIC_CODEX_RESPONSE_FIELDS
        elif field in {"input_tokens_details", "output_tokens_details"}:
            allowed = _PUBLIC_CODEX_USAGE_DETAIL_FIELDS
        elif field == "annotation":
            allowed = _PUBLIC_CODEX_ANNOTATION_FIELDS
        elif field == "action":
            allowed = _PUBLIC_CODEX_ACTION_FIELDS
        elif field == "tool_definition":
            allowed = _PUBLIC_CODEX_TOOL_DEFINITION_FIELDS
        elif field in {"part", "content"}:
            allowed = _PUBLIC_CODEX_CONTENT_FIELDS
        else:
            allowed = _PUBLIC_CODEX_CONTENT_FIELDS
        projected: dict[str, Any] = {}
        for key, child in value.items():
            if key not in allowed:
                continue
            projected[key] = _project_public_codex_value(child, field=key)
        return projected
    if isinstance(value, list):
        return [_project_public_codex_value(child, field=field) for child in value]
    return value


def _project_public_codex_logprob(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError("codex returned malformed public response event")
    projected: dict[str, Any] = {}
    for key, child in value.items():
        if key not in _PUBLIC_CODEX_LOGPROB_FIELDS:
            continue
        if key == "token":
            if not isinstance(child, str):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = child
        elif key == "logprob":
            if isinstance(child, bool) or not isinstance(child, (int, float)) or not math.isfinite(child):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = child
        elif key == "bytes":
            if not isinstance(child, list) or any(type(item) is not int or item < 0 for item in child):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = list(child)
        elif key == "top_logprobs":
            if not isinstance(child, list):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = [_project_public_codex_logprob(item) for item in child]
    return projected


def _project_public_codex_tool_schema(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError("codex returned malformed public response event")

    projected: dict[str, Any] = {}
    for key, child in value.items():
        if key not in _PUBLIC_CODEX_TOOL_SCHEMA_FIELDS:
            continue
        if key in _PUBLIC_CODEX_TOOL_SCHEMA_STRING_FIELDS:
            if not isinstance(child, str):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = child
        elif key == "type":
            if isinstance(child, str):
                projected[key] = child
            elif isinstance(child, list) and all(isinstance(item, str) for item in child):
                projected[key] = list(child)
            else:
                raise RuntimeError("codex returned malformed public response event")
        elif key == "properties":
            if not isinstance(child, dict):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = {
                name: _project_public_codex_tool_schema(schema)
                for name, schema in child.items()
                if isinstance(name, str) and isinstance(schema, dict)
            }
        elif key == "required":
            if not isinstance(child, list) or any(not isinstance(item, str) for item in child):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = list(child)
        elif key in {"items", "additionalProperties"}:
            if isinstance(child, bool) and key == "additionalProperties":
                projected[key] = child
            else:
                projected[key] = _project_public_codex_tool_schema(child)
        elif key in {"oneOf", "anyOf", "allOf"}:
            if not isinstance(child, list) or any(not isinstance(item, dict) for item in child):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = [_project_public_codex_tool_schema(item) for item in child]
        elif key == "enum":
            if not isinstance(child, list) or any(isinstance(item, (dict, list)) for item in child):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = list(child)
        elif key == "const":
            if isinstance(child, (dict, list)):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = child
        elif key in _PUBLIC_CODEX_TOOL_SCHEMA_NUMBER_FIELDS:
            if isinstance(child, bool) or not isinstance(child, (int, float)) or not math.isfinite(child):
                raise RuntimeError("codex returned malformed public response event")
            projected[key] = child
    return projected


def _project_public_codex_response_item(item: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise RuntimeError("codex returned malformed public response event")
    item_type = item.get("type")
    if not isinstance(item_type, str):
        raise RuntimeError("codex returned malformed public response event")
    allowed = _PUBLIC_CODEX_ITEM_FIELDS_BY_TYPE.get(item_type)
    if allowed is None:
        raise RuntimeError("codex returned malformed public response event")

    projected: dict[str, Any] = {}
    for key, child in item.items():
        if key not in allowed:
            continue
        if key == "environment":
            projected[key] = _project_public_codex_environment(child)
        elif key == "operation":
            projected[key] = _project_public_codex_operation(child)
        elif key == "pending_safety_checks":
            projected[key] = _project_public_codex_safety_checks(child)
        elif key in {"outputs", "files"}:
            projected[key] = _project_public_codex_outputs(child)
        elif key == "output" and item_type == "computer_call_output":
            projected[key] = _project_public_codex_output_object(child)
        elif key == "output" and item_type in {"shell_call_output", "code_interpreter_call"}:
            projected[key] = _project_public_codex_outputs(child)
        elif key == "output" and item_type in {
            "function_call_output",
            "custom_tool_call_output",
            "mcp_call_output",
            "mcp_tool_call_output",
            "local_shell_call_output",
        }:
            projected[key] = _project_public_codex_item_output(child)
        else:
            projected[key] = _project_public_codex_value(child, field=key)
    return projected


def project_public_codex_response_event(event: dict[str, Any]) -> dict[str, Any] | None:
    event_type = event.get("type")
    if not isinstance(event_type, str) or not event_type.strip():
        raise RuntimeError("codex returned malformed public response event")
    if event_type in CODEX_INTERNAL_RESPONSE_EVENT_TYPES:
        return None

    try:
        projected = {
            key: _project_public_codex_value(value, field=key)
            for key, value in event.items()
            if key in _PUBLIC_CODEX_EVENT_FIELDS
        }
        item = projected.get("item")
        if isinstance(item, dict):
            projected["item"] = _project_public_codex_response_item(item)
        if event_type in {"response.created", "response.in_progress"}:
            active_response = _active_response_from_event(event, event_type)
            if active_response is not None:
                projected["response"] = _project_public_codex_value(
                    active_response,
                    field="response",
                )
        elif event_type in {"response.completed", "response.incomplete"}:
            terminal_response = terminal_response_from_event(event)
            if terminal_response is None:
                raise RuntimeError("codex returned a malformed terminal response")
            projected["response"] = terminal_response
        else:
            response = projected.get("response")
            if isinstance(response, dict):
                projected["response"] = _project_public_codex_value(response, field="response")
        return projected
    except RuntimeError as exc:
        if event_type in {"response.created", "response.in_progress"}:
            raise RuntimeError(f"codex returned malformed {event_type}") from None
        response = event.get("response")
        if (
            event_type in {"response.completed", "response.incomplete"}
            and isinstance(response, dict)
            and "usage" in response
        ):
            raise RuntimeError("codex returned malformed codex usage") from None
        raise RuntimeError("codex returned malformed public response event") from None


def stream_codex_response(
    body: dict[str, Any],
    *,
    access_token: str | None = None,
    deadline: float | None = None,
) -> Iterator[dict[str, Any]]:
    payload = codex_response_payload(body)
    model = str(body.get("model") or "auto").strip() or "auto"
    attempted_tokens: set[str] = set()
    current_token = access_token
    emitted_public_event = False
    last_retryable_error: UpstreamHTTPError | None = None
    failover_deadline = (
        deadline
        if deadline is not None
        else time.monotonic() + _CODEX_TEXT_FAILOVER_DEADLINE_SECONDS
    )
    while True:
        if not current_token:
            if time.monotonic() >= failover_deadline:
                if last_retryable_error is not None:
                    raise last_retryable_error
                raise TimeoutError("codex response failover deadline expired")
            try:
                if attempted_tokens:
                    current_token = account_service.get_text_access_token(
                        model=model,
                        source_type="codex",
                        excluded_tokens=set(attempted_tokens),
                        deadline=failover_deadline,
                    )
                else:
                    current_token = account_service.get_text_access_token(
                        model=model,
                        source_type="codex",
                        deadline=failover_deadline,
                    )
            except TimeoutError:
                if last_retryable_error is not None:
                    raise last_retryable_error
                raise
            except ModelUnavailableError as exc:
                if last_retryable_error is not None:
                    raise last_retryable_error
                raise HTTPException(
                    status_code=503,
                    detail={"error": "native tools require an active Codex OAuth account"},
                ) from exc
            if time.monotonic() >= failover_deadline:
                if last_retryable_error is not None:
                    raise last_retryable_error
                raise TimeoutError("codex response failover deadline expired")
        if not current_token or current_token in attempted_tokens:
            raise ModelUnavailableError("no active Codex OAuth account is available")
        remaining = failover_deadline - time.monotonic()
        if remaining <= 0:
            if last_retryable_error is not None:
                raise last_retryable_error
            raise RuntimeError("codex response failover deadline expired")
        attempted_tokens.add(current_token)
        backend = None
        try:
            attempt_payload = copy.deepcopy(payload)
            resolve_codex_reasoning_effort(attempt_payload, access_token=current_token)
            expected_account = None
            get_account_lease = getattr(account_service, "_get_account_lease", None)
            if callable(get_account_lease):
                _, expected_account = get_account_lease(current_token)
            backend = OpenAIBackendAPI(access_token=current_token)
            events = backend.iter_codex_response_events(
                attempt_payload,
                timeout=max(1.0, remaining),
            )
            for event in events:
                public_event = project_public_codex_response_event(event)
                if public_event is None:
                    continue
                event = public_event
                active = created_response_from_event(event)
                if active is None:
                    active = in_progress_response_from_event(event)
                terminal = None if active is not None else terminal_response_from_event(event)
                if active is not None:
                    event = {**event, "response": active}
                elif terminal is not None:
                    event = {**event, "response": terminal}
                emitted_public_event = True
                yield event
                if terminal is not None:
                    if expected_account is None:
                        account_service.mark_text_used(current_token)
                    else:
                        account_service.mark_text_used(
                            current_token,
                            expected_account=expected_account,
                        )
                    return
                if event.get("type") in {"response.failed", "error"}:
                    return
            raise RuntimeError("codex response ended without a terminal response event")
        except UpstreamHTTPError as exc:
            if exc.status_code == 401:
                account_service.remove_invalid_token(
                    current_token,
                    "codex_responses_unauthorized",
                    expected_account=expected_account,
                )
                if emitted_public_event:
                    raise
                last_retryable_error = exc
                current_token = None
                continue
            if emitted_public_event or exc.status_code not in _CODEX_TEXT_FAILOVER_STATUSES:
                raise
            last_retryable_error = exc
            current_token = None
            continue
        finally:
            if backend is not None:
                backend.close()


def response_image_tool(body: dict[str, Any]) -> dict[str, object]:
    for tool in body.get("tools") or []:
        if isinstance(tool, dict) and tool.get("type") == "image_generation":
            return tool
    return {}


def extract_response_image(input_value: object) -> tuple[bytes, str] | None:
    if isinstance(input_value, dict):
        if str(input_value.get("type") or "").strip() == "input_image":
            images = extract_image_from_message_content([input_value])
            return images[0] if images else None
        images = extract_image_from_message_content(input_value.get("content"))
        return images[0] if images else None
    if not isinstance(input_value, list):
        return None
    for item in reversed(input_value):
        if isinstance(item, dict):
            if str(item.get("type") or "").strip() == "input_image":
                images = extract_image_from_message_content([item])
                if images:
                    return images[0]
            images = extract_image_from_message_content(item.get("content"))
            if images:
                return images[0]
    return None


def _input_image_parts(input_value: object) -> list[dict[str, Any]]:
    parts: list[dict[str, Any]] = []
    if isinstance(input_value, dict):
        if str(input_value.get("type") or "").strip() in {"input_image", "image", "image_url"}:
            return [input_value]
        content = input_value.get("content")
        if isinstance(content, list):
            parts.extend(item for item in content if isinstance(item, dict))
        return parts
    if not isinstance(input_value, list):
        return parts
    if all(isinstance(item, dict) and item.get("type") for item in input_value):
        return [item for item in input_value if isinstance(item, dict)]
    for item in input_value:
        if isinstance(item, dict):
            content = item.get("content")
            if isinstance(content, list):
                parts.extend(part for part in content if isinstance(part, dict))
    return parts


def _has_input_image(parts: Iterable[dict[str, Any]]) -> bool:
    return any(
        str(part.get("type") or "").strip() in {"input_image", "image", "image_url"}
        or "image_url" in part
        for part in parts
    )


def _is_response_content_part(value: object) -> bool:
    if not isinstance(value, dict):
        return False
    part_type = str(value.get("type") or "").strip()
    return part_type in RESPONSE_CONTENT_PART_TYPES or ("image_url" in value and part_type != "message")


def _message_content_from_response_item(item: dict[str, Any]) -> object:
    content = item.get("content")
    if isinstance(content, list):
        return [dict(part) if isinstance(part, dict) else part for part in content]
    if isinstance(content, str):
        return content
    return extract_response_prompt([item]) or content or ""


def _append_response_message(messages: list[dict[str, Any]], role: object, content: object) -> None:
    if isinstance(content, str):
        if content.strip():
            messages.append({"role": str(role or "user"), "content": content.strip()})
        return
    if isinstance(content, list) and content:
        messages.append({"role": str(role or "user"), "content": content})


def messages_from_input(input_value: object, instructions: object = None) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    system_text = str(instructions or "").strip()
    if system_text:
        messages.append({"role": "system", "content": system_text})
    if isinstance(input_value, str):
        if input_value.strip():
            messages.append({"role": "user", "content": input_value.strip()})
        return messages
    if isinstance(input_value, dict):
        if _is_response_content_part(input_value):
            _append_response_message(messages, "user", [dict(input_value)])
            return messages
        _append_response_message(
            messages,
            input_value.get("role") or "user",
            _message_content_from_response_item(input_value),
        )
        return messages
    if isinstance(input_value, list):
        if all(_is_response_content_part(item) for item in input_value):
            _append_response_message(messages, "user", [dict(item) for item in input_value if isinstance(item, dict)])
            return messages
        pending_parts: list[dict[str, Any]] = []
        for item in input_value:
            if _is_response_content_part(item):
                pending_parts.append(dict(item))
                continue
            if pending_parts:
                _append_response_message(messages, "user", pending_parts)
                pending_parts = []
            if not isinstance(item, dict):
                continue
            _append_response_message(
                messages,
                item.get("role") or "user",
                _message_content_from_response_item(item),
            )
        if pending_parts:
            _append_response_message(messages, "user", pending_parts)
    return messages


def output_text_part(
    text: str,
    annotations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {"type": "output_text", "text": text, "annotations": annotations or []}


def text_output_item(
    text: str,
    item_id: str | None = None,
    status: str = "completed",
    annotations: list[dict[str, Any]] | None = None,
    *,
    content_added: bool = True,
) -> dict[str, Any]:
    return {
        "id": item_id or f"msg_{uuid.uuid4().hex}",
        "type": "message",
        "status": status,
        "role": "assistant",
        "content": [output_text_part(text, annotations)] if content_added else [],
    }


def image_output_items(prompt: str, data: list[dict[str, Any]], item_id: str | None = None) -> list[dict[str, Any]]:
    output = []
    for item in data:
        b64_value = item.get("b64_json")
        if not isinstance(b64_value, str):
            continue
        b64_json = b64_value.strip()
        if b64_json:
            output.append({
                "id": item_id or f"ig_{len(output) + 1}",
                "type": "image_generation_call",
                "status": "completed",
                "result": b64_json,
                "revised_prompt": (
                    item["revised_prompt"].strip()
                    if isinstance(item.get("revised_prompt"), str) and item["revised_prompt"].strip()
                    else prompt
                ),
            })
    return output


def response_created(response_id: str, model: str, created: int) -> dict[str, Any]:
    return {
        "type": "response.created",
        "response": {
            "id": response_id,
            "object": "response",
            "created_at": created,
            "status": "in_progress",
            "error": None,
            "incomplete_details": None,
            "model": model,
            "output": [],
            "parallel_tool_calls": False,
        },
    }


def response_completed(
    response_id: str,
    model: str,
    created: int,
    output: list[dict[str, Any]],
    usage: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = {
        "type": "response.completed",
        "response": {
            "id": response_id,
            "object": "response",
            "created_at": created,
            "status": "completed",
            "error": None,
            "incomplete_details": None,
            "model": model,
            "output": output,
            "parallel_tool_calls": False,
        },
    }
    if usage:
        response["response"]["usage"] = usage
    return response


def text_response_parts(body: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    validate_plain_text_parameters(body)
    model = str(body.get("model") or "auto").strip() or "auto"
    messages = normalize_text_messages(normalize_messages(messages_from_input(body.get("input"), body.get("instructions"))))
    if has_unsupported_response_tools(body):
        raise HTTPException(status_code=400, detail={"error": "tool type is not supported"})
    return model, messages


def stream_text_response(backend, body: dict[str, Any], messages: list[dict[str, Any]] | None = None) -> Iterator[dict[str, Any]]:
    try:
        model = str(body.get("model") or "auto").strip() or "auto"
        messages = messages if messages is not None else messages_from_input(body.get("input"), body.get("instructions"))
        thinking_effort = thinking_effort_from_body(body)
        response_id = f"resp_{uuid.uuid4().hex}"
        item_id = f"msg_{uuid.uuid4().hex}"
        created = int(time.time())
        full_text = ""
        yield response_created(response_id, model, created)
        yield {
            "type": "response.output_item.added",
            "output_index": 0,
            "item": text_output_item("", item_id, "in_progress", content_added=False),
        }
        yield {
            "type": "response.content_part.added",
            "item_id": item_id,
            "output_index": 0,
            "content_index": 0,
            "part": output_text_part(""),
        }
        request = ConversationRequest(model=model, messages=messages, thinking_effort=thinking_effort)
        for delta in stream_text_deltas(backend, request):
            full_text += delta
            yield {"type": "response.output_text.delta", "item_id": item_id, "output_index": 0, "content_index": 0, "delta": delta}
        yield {"type": "response.output_text.done", "item_id": item_id, "output_index": 0, "content_index": 0, "text": full_text}
        yield {
            "type": "response.content_part.done",
            "item_id": item_id,
            "output_index": 0,
            "content_index": 0,
            "part": output_text_part(full_text),
        }
        item = text_output_item(full_text, item_id, "completed")
        yield {"type": "response.output_item.done", "output_index": 0, "item": item}
        usage = token_usage(
            input_text_tokens=count_message_text_tokens(messages, model),
            input_image_tokens=count_message_image_tokens(messages, model),
            output_text_tokens=count_text_tokens(full_text, model),
        )
        yield response_completed(response_id, model, created, [item], usage)
    finally:
        close = getattr(backend, "close", None)
        if callable(close):
            close()


def replay_text_response_events(events: list[dict[str, Any]]) -> Iterator[dict[str, Any]]:
    response_id = f"resp_{uuid.uuid4().hex}"
    created = int(time.time())
    item_ids: dict[str, str] = {}

    def remap_item(item: object) -> None:
        if not isinstance(item, dict):
            return
        old_id = item.get("id")
        if isinstance(old_id, str):
            item_ids.setdefault(old_id, f"msg_{uuid.uuid4().hex}")
            item["id"] = item_ids[old_id]

    def remap_item_id(value: object) -> object:
        if not isinstance(value, str):
            return value
        item_ids.setdefault(value, f"msg_{uuid.uuid4().hex}")
        return item_ids[value]

    for event in events:
        if not isinstance(event, dict):
            yield event
            continue
        event_type = event.get("type")
        response = event.get("response")
        if event_type in {"response.created", "response.completed"} and isinstance(response, dict):
            response["id"] = response_id
            response["created_at"] = created
            output = response.get("output")
            if isinstance(output, list):
                for item in output:
                    remap_item(item)
        if "item_id" in event:
            event["item_id"] = remap_item_id(event.get("item_id"))
        if "item" in event:
            remap_item(event.get("item"))
        yield event


def stream_image_response(
    image_outputs: Iterable[ImageOutput],
    prompt: str,
    model: str,
    input_image_tokens: int = 0,
    size: object = None,
    quality: str = "auto",
    usage_model: str | None = None,
) -> Iterator[dict[str, Any]]:
    usage_model = usage_model or model
    response_id = f"resp_{uuid.uuid4().hex}"
    created = int(time.time())
    yield response_created(response_id, model, created)
    for output in image_outputs:
        if output.kind == "message":
            text = output.public_message()
            item_id = f"msg_{uuid.uuid4().hex}"
            yield {
                "type": "response.output_item.added",
                "output_index": 0,
                "item": text_output_item("", item_id, "in_progress", content_added=False),
            }
            yield {
                "type": "response.content_part.added",
                "item_id": item_id,
                "output_index": 0,
                "content_index": 0,
                "part": output_text_part(""),
            }
            usage = token_usage(
                input_text_tokens=count_text_tokens(prompt, usage_model),
                input_image_tokens=input_image_tokens,
                output_text_tokens=count_text_tokens(text, usage_model),
            )
            yield {"type": "response.output_text.delta", "item_id": item_id, "output_index": 0, "content_index": 0, "delta": text}
            yield {"type": "response.output_text.done", "item_id": item_id, "output_index": 0, "content_index": 0, "text": text}
            yield {
                "type": "response.content_part.done",
                "item_id": item_id,
                "output_index": 0,
                "content_index": 0,
                "part": output_text_part(text),
            }
            item = text_output_item(text, item_id)
            yield {"type": "response.output_item.done", "output_index": 0, "item": item}
            yield response_completed(response_id, model, created, [item], usage)
            return
        if output.kind != "result":
            continue
        items = image_output_items(prompt, output.data)
        if items:
            usage = image_usage(
                input_text_tokens=count_text_tokens(prompt, usage_model),
                input_image_tokens=input_image_tokens,
                output_tokens=count_image_output_items_tokens(output.data, size, quality),
            )
            for output_index, item in enumerate(items):
                yield {"type": "response.output_item.done", "output_index": output_index, "item": item}
            yield response_completed(response_id, model, created, items, usage)
            return
    raise RuntimeError("image generation failed")


def collect_response(events: Iterable[dict[str, Any]]) -> dict[str, Any]:
    terminal = {}
    for event in events:
        candidate = terminal_response_from_event(event)
        if candidate is not None:
            terminal = candidate
    if not terminal:
        raise RuntimeError("response generation failed")
    return terminal


def response_events(
    body: dict[str, Any],
    *,
    cache_scope: str = "",
    authenticated: bool = False,
) -> Iterator[dict[str, Any]]:
    validate_response_core_parameters(body)
    codex_deadline = (
        time.monotonic() + _CODEX_TEXT_FAILOVER_DEADLINE_SECONDS
        if authenticated
        else None
    )
    if uses_native_codex_responses(body):
        if codex_deadline is None:
            yield from stream_codex_response(body)
        else:
            yield from stream_codex_response(body, deadline=codex_deadline)
        return
    if is_text_response_request(body):
        model, messages = text_response_parts(body)
        selected_token = (
            account_service.get_text_access_token(
                model=model,
                backend_capability="standard",
                deadline=codex_deadline,
            )
            if authenticated or cache_scope
            else None
        )
        if selected_token and _is_codex_account_token(selected_token):
            yield from stream_codex_response(
                body,
                access_token=selected_token,
                deadline=codex_deadline,
            )
            return
        account_generation = (
            account_service.get_account_cache_scope(selected_token)
            if selected_token
            else ""
        )
        effective_scope = resolve_access_token_cache_scope(
            cache_scope,
            selected_token or "",
            authenticated=authenticated,
            account_generation=account_generation,
        )
        compute = lambda: stream_text_response(
            text_backend(model, access_token=selected_token)
            if selected_token is not None
            else text_backend(model),
            body,
            messages,
        )
        key = cache_key(body, messages, stream=bool(body.get("stream")), cache_scope=effective_scope)
        yield from chat_completion_cache.get_or_compute_stream(
            key,
            compute,
            replay=replay_text_response_events,
        )
        return

    validate_image_response_parameters(body)
    prompt = extract_response_prompt(body.get("input"))
    if not prompt:
        raise HTTPException(status_code=400, detail={"error": "input text is required"})
    tool = response_image_tool(body)
    action = tool.get("action") or "auto"
    input_image_parts = _input_image_parts(body.get("input"))
    has_input_image = _has_input_image(input_image_parts)
    if action == "edit" and not has_input_image:
        raise _codex_request_error("image_generation action=edit requires an input image")
    if action == "generate" and has_input_image:
        raise _codex_request_error("image_generation action=generate does not accept an input image")

    image_info = extract_response_image(body.get("input")) if has_input_image else None
    if image_info:
        image_data, mime_type = image_info
        images = encode_images([(image_data, "image.png", mime_type)])
    else:
        images = None
    image_model = str(tool.get("model") or "gpt-image-2")
    response_model = str(body.get("model") or image_model).strip() or image_model
    if action == "edit" and image_info is None:
        raise _codex_request_error("image_generation action=edit requires an input image")
    input_image_tokens = count_image_content_tokens(input_image_parts, image_model)
    quality = tool.get("quality") or "auto"
    size = tool.get("size")
    image_outputs = stream_image_outputs_with_pool(ConversationRequest(
        prompt=prompt,
        model=image_model,
        size=size,
        quality=quality,
        response_format="b64_json",
        output_format=tool.get("output_format") or "png",
        output_compression=tool.get("output_compression"),
        background=tool.get("background") or "auto",
        images=images,
    ))
    yield from stream_image_response(
        image_outputs,
        prompt,
        response_model,
        input_image_tokens,
        size,
        quality,
        usage_model=image_model,
    )


def handle(
    body: dict[str, Any],
    *,
    cache_scope: str = "",
    authenticated: bool = False,
) -> dict[str, Any] | Iterator[dict[str, Any]]:
    validate_response_core_parameters(body)
    validate_tool_container(body)
    if authenticated:
        events = response_events(
            body,
            cache_scope=cache_scope,
            authenticated=True,
        )
    elif cache_scope:
        events = response_events(body, cache_scope=cache_scope)
    else:
        events = response_events(body)
    if body.get("stream"):
        return events
    return collect_response(events)
