from __future__ import annotations

import base64
import binascii
import copy
import json
import time
import uuid
from typing import Any, Iterable, Iterator

from fastapi import HTTPException

from services.account_service import account_service
from services.protocol.chat_completion_cache import (
    cache_key,
    chat_completion_cache,
    normalize_text_messages,
    resolve_access_token_cache_scope,
)
from services.protocol.conversation import (
    ConversationRequest,
    ImageOutput,
    PUBLIC_IMAGE_PROGRESS_MESSAGE,
    collect_image_outputs,
    collect_text,
    count_message_image_tokens,
    count_message_text_tokens,
    count_text_tokens,
    encode_images,
    normalize_messages,
    stream_image_outputs_with_pool,
    stream_text_deltas,
    text_backend,
)
from services.protocol.reasoning_effort import codex_effort_from_body, conversation_effort_from_body
from services.protocol.web_search_tool import (
    WEB_SEARCH_TOOL_TYPES,
    has_web_search_tool,
    has_unsupported_tools,
    is_web_search_chat_request,
    run_web_search,
    search_query_from_messages,
    text_with_url_citations,
)
from services.protocol import openai_v1_response
from utils.helper import build_chat_image_markdown_content, extract_chat_image, extract_chat_prompt, is_image_chat_request, parse_image_count
from utils.image_tokens import (
    chat_usage_from_image_usage,
    count_image_inputs_tokens,
    count_image_output_items_tokens,
    image_usage,
)

PLAIN_TEXT_CHAT_FIELDS = {
    "messages",
    "modalities",
    "model",
    "n",
    "parallel_tool_calls",
    "prompt",
    "reasoning",
    "reasoning_effort",
    "store",
    "stream",
    "stream_options",
    "thinking_effort",
    "tool_choice",
    "tools",
}
IMAGE_CHAT_FIELDS = {
    "messages",
    "modalities",
    "model",
    "n",
    "prompt",
    "stream",
    "stream_options",
}
CODEX_CHAT_SUPPORTED_FIELDS = {
    "messages",
    "modalities",
    "model",
    "n",
    "parallel_tool_calls",
    "prompt_cache_key",
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
}
CODEX_CHAT_UNSUPPORTED_FIELDS = {
    "audio",
    "frequency_penalty",
    "logit_bias",
    "logprobs",
    "max_completion_tokens",
    "max_tokens",
    "metadata",
    "prediction",
    "presence_penalty",
    "reasoning",
    "seed",
    "stop",
    "temperature",
    "top_logprobs",
    "top_p",
    "user",
}
CODEX_CHAT_NATIVE_ROUTING_FIELDS = {
    "prompt_cache_key",
    "response_format",
    "service_tier",
    "verbosity",
}
CHAT_MESSAGE_FIELDS_BY_ROLE = {
    "system": {"role", "content"},
    "developer": {"role", "content"},
    "user": {"role", "content"},
    "assistant": {"role", "content", "tool_calls"},
    "tool": {"role", "content", "tool_call_id"},
}
CHAT_TOOL_CALL_FIELDS = {"id", "type", "function"}
CHAT_TOOL_CALL_FUNCTION_FIELDS = {"name", "arguments"}

def thinking_effort_from_body(body: dict[str, Any]) -> str:
    try:
        return conversation_effort_from_body(body)
    except ValueError as exc:
        raise _chat_codex_error(str(exc)) from exc


def completion_chunk(
    model: str,
    delta: dict[str, Any],
    finish_reason: str | None = None,
    completion_id: str = "",
    created: int | None = None,
    *,
    include_usage: bool = False,
) -> dict[str, Any]:
    chunk = {
        "id": completion_id or f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion.chunk",
        "created": created or int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    if include_usage:
        chunk["usage"] = None
    return chunk


def completion_usage_chunk(
    model: str,
    usage: dict[str, Any],
    completion_id: str,
    created: int,
) -> dict[str, Any]:
    return {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created,
        "model": model,
        "choices": [],
        "usage": usage,
    }


def include_stream_usage(body: dict[str, Any]) -> bool:
    options = body.get("stream_options")
    if options is None:
        return False
    if not isinstance(options, dict):
        raise HTTPException(status_code=400, detail={"error": "stream_options must be an object"})
    if set(options) - {"include_usage", "include_obfuscation"}:
        raise HTTPException(status_code=400, detail={"error": "stream_options parameter is not supported"})
    if "include_obfuscation" in options:
        include_obfuscation = options["include_obfuscation"]
        if type(include_obfuscation) is not bool:
            raise HTTPException(
                status_code=400,
                detail={"error": "stream_options.include_obfuscation must be a boolean"},
            )
        if include_obfuscation:
            raise HTTPException(
                status_code=400,
                detail={"error": "stream obfuscation is not supported by the configured backend"},
            )
    include_usage = options.get("include_usage", False)
    if type(include_usage) is not bool:
        raise HTTPException(status_code=400, detail={"error": "stream_options.include_usage must be a boolean"})
    return include_usage


def validate_chat_core_parameters(body: dict[str, Any]) -> None:
    model = body.get("model")
    if model is not None and not isinstance(model, str):
        raise _chat_codex_error("model must be a string")
    prompt = body.get("prompt")
    if prompt is not None and not isinstance(prompt, str):
        raise _chat_codex_error("prompt must be a string")
    messages = body.get("messages")
    if messages is not None and (
        not isinstance(messages, list)
        or any(not isinstance(message, dict) for message in messages)
    ):
        raise _chat_codex_error("messages must be an array of objects")
    if isinstance(messages, list):
        for message in messages:
            role = message.get("role")
            if not isinstance(role, str) or role not in CHAT_MESSAGE_FIELDS_BY_ROLE:
                raise _chat_codex_error("message role is not supported by the configured backend")
            allowed_fields = CHAT_MESSAGE_FIELDS_BY_ROLE[role]
            if any(value is not None and key not in allowed_fields for key, value in message.items()):
                raise _chat_codex_error("message field is not supported by the configured backend")
            if "content" not in message:
                if role != "assistant" or "tool_calls" not in message:
                    raise _chat_codex_error("message content is required")
            elif message.get("content") is None and role != "assistant":
                raise _chat_codex_error("message content is required")
            _chat_content_parts(
                message.get("content"),
                "developer" if role == "system" else role,
            )
    count = body.get("n")
    if count is not None and type(count) is not int:
        raise _chat_codex_error("n must be an integer")
    stream = body.get("stream")
    if stream is not None and type(stream) is not bool:
        raise _chat_codex_error("stream must be a boolean")


def validate_single_text_output_parameters(body: dict[str, Any]) -> None:
    count = body.get("n")
    if count is not None and (type(count) is not int or count != 1):
        raise _chat_codex_error("n greater than 1 is not supported by the upstream text backend")
    modalities = body.get("modalities")
    if modalities is not None and modalities != ["text"] and modalities != ("text",):
        raise _chat_codex_error("modality is not supported by the upstream text backend")


def validate_plain_text_parameters(body: dict[str, Any]) -> None:
    if any(
        value is not None and key not in PLAIN_TEXT_CHAT_FIELDS
        for key, value in body.items()
    ):
        raise _chat_codex_error("parameter is not supported by the upstream text backend")
    store = body.get("store")
    if store is not None and store is not False:
        raise _chat_codex_error("store=true is not supported by the upstream text backend")
    parallel_tool_calls = body.get("parallel_tool_calls")
    if parallel_tool_calls is not None and type(parallel_tool_calls) is not bool:
        raise _chat_codex_error("parallel_tool_calls must be a boolean")
    tool_choice = body.get("tool_choice")
    if tool_choice is not None and tool_choice != "none":
        raise _chat_codex_error("tool_choice requires a supported tool")
    validate_single_text_output_parameters(body)
    if body.get("stream_options") is not None:
        if body.get("stream") is not True:
            raise _chat_codex_error("stream_options requires stream=true")
        include_stream_usage(body)
    thinking_effort_from_body(body)


def validate_image_chat_parameters(body: dict[str, Any]) -> None:
    if any(
        value is not None and key not in IMAGE_CHAT_FIELDS
        for key, value in body.items()
    ):
        raise _chat_codex_error("parameter is not supported by the upstream image backend")
    modalities = body.get("modalities")
    if modalities is not None:
        if (
            not isinstance(modalities, (list, tuple))
            or not modalities
            or any(not isinstance(item, str) for item in modalities)
        ):
            raise _chat_codex_error("modalities must be an array of strings")
        normalized = {item.strip().lower() for item in modalities}
        if "image" not in normalized or not normalized.issubset({"text", "image"}):
            raise _chat_codex_error("modality is not supported by the upstream image backend")
    if body.get("stream_options") is not None:
        if body.get("stream") is not True:
            raise _chat_codex_error("stream_options requires stream=true")
        include_stream_usage(body)


def chat_text_usage(model: str, messages: list[dict[str, Any]], content: str) -> dict[str, Any]:
    prompt_text_tokens = count_message_text_tokens(messages, model)
    prompt_image_tokens = count_message_image_tokens(messages, model)
    prompt_tokens = prompt_text_tokens + prompt_image_tokens
    completion_tokens = count_text_tokens(content, model)
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": prompt_tokens + completion_tokens,
        "prompt_tokens_details": {
            "text_tokens": prompt_text_tokens,
            "image_tokens": prompt_image_tokens,
            "cached_tokens": 0,
        },
        "completion_tokens_details": {
            "text_tokens": completion_tokens,
            "image_tokens": 0,
            "reasoning_tokens": 0,
        },
    }


def completion_response(
    model: str,
    content: str,
    created: int | None = None,
    messages: list[dict[str, Any]] | None = None,
    annotations: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    message = {"role": "assistant", "content": content}
    if annotations:
        message["annotations"] = annotations
    return {
        "id": f"chatcmpl-{uuid.uuid4().hex}",
        "object": "chat.completion",
        "created": created or int(time.time()),
        "model": model,
        "choices": [{
            "index": 0,
            "message": message,
            "finish_reason": "stop",
        }],
        "usage": chat_text_usage(model, messages or [], content),
    }


def _fresh_completion_id() -> str:
    return f"chatcmpl-{uuid.uuid4().hex}"


def replay_chat_completion_response(value: dict[str, Any]) -> dict[str, Any]:
    value["id"] = _fresh_completion_id()
    value["created"] = int(time.time())
    return value


def replay_chat_completion_stream(value: list[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    completion_id = _fresh_completion_id()
    created = int(time.time())
    for chunk in value:
        if isinstance(chunk, dict):
            chunk["id"] = completion_id
            chunk["created"] = created
        yield chunk


def stream_text_chat_completion(
    backend,
    messages: list[dict[str, Any]],
    model: str,
    thinking_effort: str = "",
    include_usage: bool = False,
) -> Iterator[dict[str, Any]]:
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())
    sent_role = False
    content_parts: list[str] = []
    request = ConversationRequest(model=model, messages=messages, thinking_effort=thinking_effort)
    for delta_text in stream_text_deltas(backend, request):
        content_parts.append(delta_text)
        if not sent_role:
            sent_role = True
            yield completion_chunk(
                model,
                {"role": "assistant", "content": delta_text},
                None,
                completion_id,
                created,
                include_usage=include_usage,
            )
        else:
            yield completion_chunk(
                model,
                {"content": delta_text},
                None,
                completion_id,
                created,
                include_usage=include_usage,
            )
    if not sent_role:
        yield completion_chunk(
            model,
            {"role": "assistant", "content": ""},
            None,
            completion_id,
            created,
            include_usage=include_usage,
        )
    yield completion_chunk(model, {}, "stop", completion_id, created, include_usage=include_usage)
    if include_usage:
        yield completion_usage_chunk(
            model,
            chat_text_usage(model, messages, "".join(content_parts)),
            completion_id,
            created,
        )


def collect_chat_content(chunks: Iterable[dict[str, Any]]) -> str:
    parts: list[str] = []
    for chunk in chunks:
        if not isinstance(chunk, dict):
            raise RuntimeError("malformed upstream chat chunk")
        choices = chunk.get("choices")
        if choices in (None, []):
            first = {}
        elif not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise RuntimeError("malformed upstream chat chunk")
        else:
            first = choices[0]
        raw_delta = first.get("delta")
        if raw_delta is not None and not isinstance(raw_delta, dict):
            raise RuntimeError("malformed upstream chat chunk")
        content_value = raw_delta.get("content") if isinstance(raw_delta, dict) else None
        if content_value is not None and not isinstance(content_value, str):
            raise RuntimeError("malformed upstream text delta")
        content = content_value or ""
        if content:
            parts.append(content)
    return "".join(parts)


def chat_messages_from_body(body: dict[str, Any]) -> list[dict[str, Any]]:
    messages = body.get("messages")
    if isinstance(messages, list) and messages:
        return [message for message in messages if isinstance(message, dict)]
    prompt = str(body.get("prompt") or "").strip()
    if prompt:
        return [{"role": "user", "content": prompt}]
    raise HTTPException(status_code=400, detail={"error": "messages or prompt is required"})


def has_chat_function_tool(body: dict[str, Any]) -> bool:
    tools = body.get("tools")
    return isinstance(tools, list) and any(
        isinstance(tool, dict)
        and tool.get("type") == "function"
        and isinstance(tool.get("function"), dict)
        for tool in tools
    )


def has_chat_tool_history(body: dict[str, Any]) -> bool:
    messages = body.get("messages")
    if not isinstance(messages, list):
        return False
    return any(
        isinstance(message, dict)
        and (
            message.get("role") == "tool"
            or message.get("tool_calls") is not None
        )
        for message in messages
    )


def has_chat_native_message_features(body: dict[str, Any]) -> bool:
    messages = body.get("messages")
    if not isinstance(messages, list):
        return False
    for message in messages:
        if not isinstance(message, dict):
            continue
        if message.get("role") == "developer":
            return True
        content = message.get("content")
        if isinstance(content, list) and any(
            isinstance(part, dict) and part.get("type") == "input_audio"
            for part in content
        ):
            return True
    return False


def uses_chat_native_codex(body: dict[str, Any]) -> bool:
    return (
        has_chat_function_tool(body)
        or has_chat_tool_history(body)
        or has_chat_native_message_features(body)
        or has_web_search_tool(body)
        or isinstance(body.get("web_search_options"), dict)
        or any(body.get(field) is not None for field in CODEX_CHAT_NATIVE_ROUTING_FIELDS)
    )


def _chat_codex_error(message: str) -> HTTPException:
    return HTTPException(status_code=400, detail={"error": message})


def _is_codex_account_token(access_token: str) -> bool:
    account = account_service.get_account(access_token)
    return (
        isinstance(account, dict)
        and str(account.get("source_type") or "").strip().lower() == "codex"
    )


def _chat_content_parts(content: object, role: str) -> list[dict[str, Any]]:
    text_type = "output_text" if role == "assistant" else "input_text"
    if isinstance(content, str):
        return [{"type": text_type, "text": content}]
    if content is None:
        return []
    if not isinstance(content, list):
        raise _chat_codex_error("message content must be text or an array")
    result: list[dict[str, Any]] = []
    for part in content:
        if not isinstance(part, dict):
            raise _chat_codex_error("message content parts must be objects")
        if part.get("prompt_cache_breakpoint") is not None:
            raise _chat_codex_error("prompt_cache_breakpoint is not supported by Codex Responses")
        part_type = part.get("type")
        if not isinstance(part_type, str):
            raise _chat_codex_error("message content type must be a string")
        if part_type in {"text", "input_text", "output_text"}:
            if set(part) - {"type", "text", "prompt_cache_breakpoint"}:
                raise _chat_codex_error("text content contains unsupported fields")
            text = part.get("text")
            if not isinstance(text, str):
                raise _chat_codex_error("text content must be a string")
            result.append({"type": text_type, "text": text})
            continue
        if part_type in {"image_url", "input_image"} and role == "user":
            if set(part) - {"type", "image_url", "prompt_cache_breakpoint"}:
                raise _chat_codex_error("image content contains unsupported fields")
            image_url = part.get("image_url")
            detail: str | None = None
            if isinstance(image_url, dict):
                if set(image_url) - {"url", "detail"}:
                    raise _chat_codex_error("image_url contains unsupported fields")
                if "detail" in image_url:
                    detail_value = image_url.get("detail")
                    if not isinstance(detail_value, str) or detail_value not in {"auto", "low", "high"}:
                        raise _chat_codex_error("image_url detail must be auto, low, or high")
                    detail = detail_value
                image_url = image_url.get("url")
            if not isinstance(image_url, str) or not image_url.strip():
                raise _chat_codex_error("image_url is required")
            image_part = {"type": "input_image", "image_url": image_url}
            if detail is not None:
                image_part["detail"] = detail
            result.append(image_part)
            continue
        if part_type == "input_audio" and role == "user":
            if set(part) - {"type", "input_audio", "prompt_cache_breakpoint"}:
                raise _chat_codex_error("input_audio contains unsupported fields")
            input_audio = part.get("input_audio")
            if not isinstance(input_audio, dict) or set(input_audio) != {"data", "format"}:
                raise _chat_codex_error("input_audio is malformed")
            data = input_audio.get("data")
            audio_format = input_audio.get("format")
            if not isinstance(data, str) or not data:
                raise _chat_codex_error("input_audio data must be base64")
            if audio_format not in {"wav", "mp3"}:
                raise _chat_codex_error("input_audio format must be wav or mp3")
            try:
                base64.b64decode(data, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise _chat_codex_error("input_audio data must be base64") from exc
            mime = "audio/wav" if audio_format == "wav" else "audio/mpeg"
            result.append({
                "type": "input_audio",
                "audio_url": f"data:{mime};base64,{data}",
            })
            continue
        raise _chat_codex_error("message content type is not supported by Codex Responses")
    return result


def _chat_tool_output(content: object) -> str:
    if isinstance(content, str):
        return content
    if content is None:
        return ""
    if isinstance(content, list):
        parts = _chat_content_parts(content, "tool")
        return "".join(
            str(part.get("text") or "")
            for part in parts
            if part.get("type") == "input_text"
        )
    return json.dumps(content, ensure_ascii=False, separators=(",", ":"))


def _chat_messages_to_codex_input(messages: object) -> list[dict[str, Any]]:
    if not isinstance(messages, list) or not messages:
        raise _chat_codex_error("messages are required")
    result: list[dict[str, Any]] = []
    for message in messages:
        if not isinstance(message, dict):
            raise _chat_codex_error("messages must contain objects")
        role = str(message.get("role") or "").strip()
        if role == "tool":
            call_id = message.get("tool_call_id")
            if not isinstance(call_id, str) or not call_id.strip():
                raise _chat_codex_error("tool_call_id is required")
            result.append({
                "type": "function_call_output",
                "call_id": call_id.strip(),
                "output": _chat_tool_output(message.get("content")),
            })
            continue
        normalized_role = "developer" if role == "system" else role
        if normalized_role not in {"developer", "user", "assistant"}:
            raise _chat_codex_error("message role is not supported by Codex Responses")
        content = _chat_content_parts(message.get("content"), normalized_role)
        if content:
            result.append({"type": "message", "role": normalized_role, "content": content})
        tool_calls = message.get("tool_calls")
        if tool_calls is None:
            continue
        if normalized_role != "assistant" or not isinstance(tool_calls, list):
            raise _chat_codex_error("tool_calls are only valid on assistant messages")
        for tool_call in tool_calls:
            if (
                not isinstance(tool_call, dict)
                or set(tool_call) != CHAT_TOOL_CALL_FIELDS
                or tool_call.get("type") != "function"
            ):
                raise _chat_codex_error("only function tool calls are supported")
            function = tool_call.get("function")
            call_id = tool_call.get("id")
            if (
                not isinstance(function, dict)
                or set(function) != CHAT_TOOL_CALL_FUNCTION_FIELDS
                or not isinstance(call_id, str)
                or not call_id.strip()
            ):
                raise _chat_codex_error("function tool call is malformed")
            name = function.get("name")
            arguments = function.get("arguments")
            if not isinstance(name, str) or not name.strip() or not isinstance(arguments, str):
                raise _chat_codex_error("function tool call is malformed")
            result.append({
                "type": "function_call",
                "call_id": call_id.strip(),
                "name": name.strip(),
                "arguments": arguments,
            })
    return result


def _chat_tools_to_responses(tools: object) -> list[dict[str, Any]]:
    if not isinstance(tools, list) or not tools:
        raise _chat_codex_error("tools are required")
    result: list[dict[str, Any]] = []
    for tool in tools:
        if not isinstance(tool, dict):
            raise _chat_codex_error("tools must contain objects")
        if tool.get("type") == "function":
            if set(tool) != {"type", "function"}:
                raise _chat_codex_error("function tool definition is malformed")
            function = tool.get("function")
            if not isinstance(function, dict):
                raise _chat_codex_error("function tool definition is malformed")
            if set(function) - {"name", "description", "parameters", "strict"}:
                raise _chat_codex_error("function tool definition is malformed")
            result.append(openai_v1_response._normalize_codex_tool({**copy.deepcopy(function), "type": "function"}))
            continue
        result.append(openai_v1_response._normalize_codex_tool(tool))
    return result


def _chat_web_search_options_to_tool(options: dict[str, Any]) -> dict[str, Any]:
    if set(options) - {"search_context_size", "user_location"}:
        raise _chat_codex_error("web_search_options contains unsupported fields")
    tool: dict[str, Any] = {"type": "web_search"}
    if "search_context_size" in options:
        context_size = options.get("search_context_size")
        if not isinstance(context_size, str) or context_size not in {"low", "medium", "high"}:
            raise _chat_codex_error("web_search_options search_context_size is invalid")
        tool["search_context_size"] = context_size
    if "user_location" in options:
        user_location = options.get("user_location")
        if user_location is None:
            return tool
        if not isinstance(user_location, dict) or set(user_location) != {"type", "approximate"}:
            raise _chat_codex_error("web_search_options user_location is malformed")
        if user_location.get("type") != "approximate":
            raise _chat_codex_error("web_search_options user_location type is invalid")
        approximate = user_location.get("approximate")
        if not isinstance(approximate, dict) or set(approximate) - {"city", "country", "region", "timezone"}:
            raise _chat_codex_error("web_search_options approximate location is malformed")
        if any(not isinstance(value, str) for value in approximate.values()):
            raise _chat_codex_error("web_search_options approximate location values must be strings")
        tool["user_location"] = {
            "type": "approximate",
            **copy.deepcopy(approximate),
        }
    return tool


def _chat_codex_tools(body: dict[str, Any]) -> list[dict[str, Any]]:
    tools_value = body.get("tools")
    tools = _chat_tools_to_responses(tools_value) if isinstance(tools_value, list) and tools_value else []
    options = body.get("web_search_options")
    if options is not None and not isinstance(options, dict):
        raise _chat_codex_error("web_search_options must be an object")
    if isinstance(options, dict):
        web_tool = _chat_web_search_options_to_tool(options)
        for index, tool in enumerate(tools):
            if tool.get("type") == "web_search":
                tools[index] = {**tool, **web_tool}
                break
        else:
            tools.append(web_tool)
    if not tools and has_web_search_tool(body):
        tools.append({"type": "web_search"})
    return tools


def _chat_tool_choice_to_responses(choice: object) -> object:
    try:
        return openai_v1_response._normalize_codex_tool_choice(choice)
    except HTTPException as exc:
        raise _chat_codex_error("tool_choice is not supported by the upstream text backend") from exc


def _chat_response_format_to_text(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        raise _chat_codex_error("response_format must be an object")
    format_type = value.get("type")
    if format_type == "text":
        if set(value) != {"type"}:
            raise _chat_codex_error("text response_format is malformed")
        return None
    if format_type != "json_schema" or set(value) != {"type", "json_schema"}:
        raise _chat_codex_error("response_format is not supported by Codex Responses")
    json_schema = value.get("json_schema")
    if not isinstance(json_schema, dict) or set(json_schema) - {"name", "schema", "strict"}:
        raise _chat_codex_error("json_schema response_format is malformed")
    return openai_v1_response._normalize_codex_text({
        "format": {"type": "json_schema", **copy.deepcopy(json_schema)},
    })


def chat_codex_response_body(body: dict[str, Any]) -> dict[str, Any]:
    if any(body.get(field) is not None for field in CODEX_CHAT_UNSUPPORTED_FIELDS):
        raise _chat_codex_error("parameter is not supported by Codex Responses")
    if any(
        value is not None
        and key not in CODEX_CHAT_SUPPORTED_FIELDS
        and key not in CODEX_CHAT_UNSUPPORTED_FIELDS
        for key, value in body.items()
    ):
        raise _chat_codex_error("parameter is not supported by Codex Responses")
    validate_single_text_output_parameters(body)
    store = body.get("store")
    if store is not None and store is not False:
        raise _chat_codex_error("store=true is not supported by Codex Responses")
    parallel_tool_calls = body.get("parallel_tool_calls")
    if parallel_tool_calls is not None and type(parallel_tool_calls) is not bool:
        raise _chat_codex_error("parallel_tool_calls must be a boolean")
    if body.get("stream_options") is not None and body.get("stream") is not True:
        raise _chat_codex_error("stream_options requires stream=true")
    prompt_cache_key = body.get("prompt_cache_key")
    if prompt_cache_key is not None and not isinstance(prompt_cache_key, str):
        raise _chat_codex_error("prompt_cache_key must be a string")
    service_tier = body.get("service_tier")
    if service_tier is not None and service_tier not in {"default", "fast", "priority", "flex"}:
        raise _chat_codex_error("service_tier is not supported by Codex Responses")

    tools = _chat_codex_tools(body)
    result: dict[str, Any] = {
        "model": str(body.get("model") or "auto").strip() or "auto",
        "input": _chat_messages_to_codex_input(body.get("messages")),
        "tool_choice": "auto",
        "parallel_tool_calls": True if parallel_tool_calls is None else parallel_tool_calls,
        "stream": bool(body.get("stream")),
    }
    if tools:
        result["tools"] = tools
    if body.get("tool_choice") is not None:
        result["tool_choice"] = _chat_tool_choice_to_responses(body.get("tool_choice"))
    try:
        effort = codex_effort_from_body(body)
    except ValueError as exc:
        raise _chat_codex_error(str(exc)) from exc
    if effort:
        result["reasoning"] = {"effort": effort}
    if prompt_cache_key is not None:
        result["prompt_cache_key"] = prompt_cache_key
    if service_tier in {"fast", "priority"}:
        result["service_tier"] = "priority"
    elif service_tier == "flex":
        result["service_tier"] = "flex"
    text_controls = (
        openai_v1_response._normalize_codex_text({"verbosity": body.get("verbosity")})
        if body.get("verbosity") is not None
        else None
    )
    if body.get("response_format") is not None:
        response_text_controls = _chat_response_format_to_text(body.get("response_format"))
        if response_text_controls is not None:
            text_controls = {**(text_controls or {}), **response_text_controls}
    if text_controls is not None:
        result["text"] = text_controls
    return result


def _codex_usage_count(source: dict[str, Any], field: str, *, required: bool = False) -> int:
    if field not in source:
        if required:
            raise RuntimeError("codex returned malformed codex usage")
        return 0
    value = source[field]
    if type(value) is not int or value < 0:
        raise RuntimeError("codex returned malformed codex usage")
    return value


def _chat_usage_from_responses(usage: object) -> dict[str, Any]:
    validated = openai_v1_response.validated_codex_usage(usage)
    if validated is None:
        source: dict[str, Any] = {}
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
    else:
        source = validated
        prompt_tokens = _codex_usage_count(source, "input_tokens", required=True)
        completion_tokens = _codex_usage_count(source, "output_tokens", required=True)
        total_tokens = _codex_usage_count(source, "total_tokens", required=True)

    input_details = source.get("input_tokens_details") or {}
    output_details = source.get("output_tokens_details") or {}
    return {
        "prompt_tokens": prompt_tokens,
        "completion_tokens": completion_tokens,
        "total_tokens": total_tokens,
        "prompt_tokens_details": {
            "cached_tokens": _codex_usage_count(input_details, "cached_tokens"),
            "text_tokens": _codex_usage_count(input_details, "text_tokens"),
            "image_tokens": _codex_usage_count(input_details, "image_tokens"),
        },
        "completion_tokens_details": {
            "reasoning_tokens": _codex_usage_count(output_details, "reasoning_tokens"),
            "text_tokens": _codex_usage_count(output_details, "text_tokens"),
            "image_tokens": _codex_usage_count(output_details, "image_tokens"),
        },
    }


def _codex_function_call_fields(item: dict[str, Any]) -> tuple[str, str, str, str]:
    item_id = item.get("id")
    call_id = item.get("call_id")
    name = item.get("name")
    arguments = item.get("arguments")
    if (
        item_id is not None
        and (not isinstance(item_id, str) or not item_id.strip())
    ) or (
        not isinstance(call_id, str)
        or not call_id.strip()
        or not isinstance(name, str)
        or not name.strip()
        or not isinstance(arguments, str)
    ):
        raise RuntimeError("codex returned a malformed function call")
    return (item_id.strip() if isinstance(item_id, str) else call_id.strip(), call_id.strip(), name, arguments)


def _chat_codex_events(
    body: dict[str, Any],
    *,
    access_token: str | None = None,
    deadline: float | None = None,
) -> Iterator[dict[str, Any]]:
    payload = chat_codex_response_body(body)
    if access_token is None:
        yield from openai_v1_response.stream_codex_response(payload, deadline=deadline)
    else:
        yield from openai_v1_response.stream_codex_response(
            payload,
            access_token=access_token,
            deadline=deadline,
        )


def _chat_response_from_codex(
    body: dict[str, Any],
    *,
    access_token: str | None = None,
    deadline: float | None = None,
) -> dict[str, Any]:
    terminal: dict[str, Any] = {}
    terminal_type = ""
    for event in _chat_codex_events(body, access_token=access_token, deadline=deadline):
        candidate = openai_v1_response.terminal_response_from_event(event)
        if candidate is not None:
            terminal = candidate
            terminal_type = event["type"]
    if not terminal:
        raise RuntimeError("codex response ended without a terminal response event")
    tool_calls: list[dict[str, Any]] = []
    text_parts: list[str] = []
    annotations: list[dict[str, Any]] = []
    output = terminal.get("output") or []
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") == "function_call":
            _, call_id, name, arguments = _codex_function_call_fields(item)
            tool_calls.append({
                "id": call_id,
                "type": "function",
                "function": {
                    "name": name,
                    "arguments": arguments,
                },
            })
        elif item.get("type") == "message":
            content = item.get("content")
            if not isinstance(content, list):
                raise RuntimeError("codex returned malformed message content")
            for part in content:
                if not isinstance(part, dict):
                    raise RuntimeError("codex returned malformed message content")
                if part.get("type") == "output_text":
                    text = part.get("text")
                    if not isinstance(text, str):
                        raise RuntimeError("codex returned malformed text output")
                    text_parts.append(text)
                    raw_annotations = part.get("annotations")
                    if raw_annotations is not None and not isinstance(raw_annotations, list):
                        raise RuntimeError("codex returned malformed text annotations")
                    if isinstance(raw_annotations, list):
                        annotations.extend(item for item in raw_annotations if isinstance(item, dict))
    message: dict[str, Any] = {"role": "assistant", "content": "".join(text_parts) or None}
    if tool_calls:
        message["tool_calls"] = tool_calls
    chat_annotations = chat_completion_annotations(annotations)
    if chat_annotations:
        message["annotations"] = chat_annotations
    response_id = terminal["id"]
    created_at = terminal.get("created_at")
    model = terminal.get("model")
    return {
        "id": response_id if response_id.startswith("chatcmpl-") else f"chatcmpl-{response_id}",
        "object": "chat.completion",
        "created": created_at if created_at is not None else int(time.time()),
        "model": model if model is not None else str(body.get("model") or "auto"),
        "choices": [{
            "index": 0,
            "message": message,
            "logprobs": None,
            "finish_reason": (
                "length"
                if terminal_type == "response.incomplete"
                else "tool_calls" if tool_calls else "stop"
            ),
        }],
        "usage": _chat_usage_from_responses(terminal.get("usage")),
    }


def _stream_chat_response_from_codex(
    body: dict[str, Any],
    *,
    access_token: str | None = None,
    deadline: float | None = None,
) -> Iterator[dict[str, Any]]:
    include_usage = include_stream_usage(body)
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())
    model = str(body.get("model") or "auto")
    item_indexes: dict[str, int] = {}
    argument_deltas: set[str] = set()
    saw_tool_call = False
    sent_role = False

    def ensure_role() -> Iterator[dict[str, Any]]:
        nonlocal sent_role
        if not sent_role:
            sent_role = True
            yield completion_chunk(
                model,
                {"role": "assistant", "content": ""},
                completion_id=completion_id,
                created=created,
                include_usage=include_usage,
            )

    for event in _chat_codex_events(body, access_token=access_token, deadline=deadline):
        event_type = event.get("type")
        if event_type == "response.created":
            response = openai_v1_response.created_response_from_event(event)
            if response is None:
                raise RuntimeError("codex returned a malformed response.created event")
            upstream_id = response["id"]
            completion_id = upstream_id if upstream_id.startswith("chatcmpl-") else f"chatcmpl-{upstream_id}"
            upstream_model = response.get("model")
            if upstream_model is not None:
                model = upstream_model
            upstream_created = response.get("created_at")
            if upstream_created is not None:
                created = upstream_created
            yield from ensure_role()
            continue
        if event_type == "response.output_item.added":
            item = event.get("item")
            if not isinstance(item, dict):
                raise RuntimeError("codex returned a malformed output item")
            if item.get("type") != "function_call":
                continue
            yield from ensure_role()
            saw_tool_call = True
            item_id, call_id, name, _ = _codex_function_call_fields(item)
            index = len(item_indexes)
            item_indexes[item_id] = index
            yield completion_chunk(
                model,
                {"tool_calls": [{
                    "index": index,
                    "id": call_id,
                    "type": "function",
                    "function": {"name": name, "arguments": ""},
                }]},
                completion_id=completion_id,
                created=created,
                include_usage=include_usage,
            )
            continue
        if event_type == "response.function_call_arguments.delta":
            yield from ensure_role()
            item_id = event.get("item_id")
            delta = event.get("delta")
            if not isinstance(item_id, str) or not isinstance(delta, str):
                raise RuntimeError("codex returned a malformed function call delta")
            if item_id not in item_indexes:
                continue
            argument_deltas.add(item_id)
            yield completion_chunk(
                model,
                {"tool_calls": [{
                    "index": item_indexes[item_id],
                    "function": {"arguments": delta},
                }]},
                completion_id=completion_id,
                created=created,
                include_usage=include_usage,
            )
            continue
        if event_type == "response.output_item.done":
            item = event.get("item")
            if not isinstance(item, dict):
                raise RuntimeError("codex returned a malformed output item")
            if item.get("type") == "function_call":
                item_id, _, _, arguments = _codex_function_call_fields(item)
            else:
                item_id = ""
            if item.get("type") == "function_call" and item_id in item_indexes and item_id not in argument_deltas:
                if arguments:
                    yield completion_chunk(
                        model,
                        {"tool_calls": [{
                            "index": item_indexes[item_id],
                            "function": {"arguments": arguments},
                        }]},
                        completion_id=completion_id,
                        created=created,
                        include_usage=include_usage,
                    )
            elif item.get("type") == "message":
                annotations: list[dict[str, Any]] = []
                raw_content = item.get("content")
                if not isinstance(raw_content, list):
                    raise RuntimeError("codex returned malformed message content")
                for part in raw_content:
                    if not isinstance(part, dict):
                        raise RuntimeError("codex returned malformed message content")
                    raw_annotations = part.get("annotations")
                    if raw_annotations is not None and not isinstance(raw_annotations, list):
                        raise RuntimeError("codex returned malformed text annotations")
                    if isinstance(raw_annotations, list):
                        annotations.extend(annotation for annotation in raw_annotations if isinstance(annotation, dict))
                chat_annotations = chat_completion_annotations(annotations)
                if chat_annotations:
                    yield from ensure_role()
                    yield completion_chunk(
                        model,
                        {"annotations": chat_annotations},
                        completion_id=completion_id,
                        created=created,
                        include_usage=include_usage,
                    )
            continue
        if event_type == "response.output_text.delta":
            yield from ensure_role()
            delta = event.get("delta")
            if not isinstance(delta, str):
                raise RuntimeError("codex returned a malformed text delta")
            yield completion_chunk(
                model,
                {"content": delta},
                completion_id=completion_id,
                created=created,
                include_usage=include_usage,
            )
            continue
        response = openai_v1_response.terminal_response_from_event(event)
        if response is None:
            continue
        yield from ensure_role()
        yield completion_chunk(
            model,
            {},
            (
                "length"
                if event_type == "response.incomplete"
                else "tool_calls" if saw_tool_call else "stop"
            ),
            completion_id,
            created,
            include_usage=include_usage,
        )
        if include_usage:
            yield completion_usage_chunk(
                model,
                _chat_usage_from_responses(response.get("usage")),
                completion_id,
                created,
            )
        return
    raise RuntimeError("codex response ended without a terminal response event")


def chat_image_args(body: dict[str, Any]) -> tuple[str, str, int, list[tuple[bytes, str, str]]]:
    model = str(body.get("model") or "gpt-image-2").strip() or "gpt-image-2"
    prompt = extract_chat_prompt(body)
    if not prompt:
        raise HTTPException(status_code=400, detail={"error": "prompt is required"})
    images = [
        (data, f"image_{idx}.png", mime)
        for idx, (data, mime) in enumerate(extract_chat_image(body), start=1)
    ]
    return model, prompt, parse_image_count(body.get("n")), images


def text_chat_parts(body: dict[str, Any]) -> tuple[str, list[dict[str, Any]]]:
    model = str(body.get("model") or "auto").strip() or "auto"
    messages = normalize_text_messages(normalize_messages(chat_messages_from_body(body)))
    if has_unsupported_tools(body, WEB_SEARCH_TOOL_TYPES):
        raise HTTPException(status_code=400, detail={"error": "tool type is not supported"})
    return model, messages


def chat_completion_annotations(annotations: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for item in annotations:
        if item.get("type") != "url_citation":
            continue
        url = item.get("url", "")
        title = item.get("title", "")
        start_index = item.get("start_index", 0)
        end_index = item.get("end_index", 0)
        if (
            not isinstance(url, str)
            or not isinstance(title, str)
            or type(start_index) is not int
            or start_index < 0
            or type(end_index) is not int
            or end_index < 0
        ):
            raise RuntimeError("codex returned a malformed citation")
        output.append({
            "type": "url_citation",
            "url_citation": {
                "start_index": start_index,
                "end_index": end_index,
                "url": url,
                "title": title,
            },
        })
    return output


def web_search_chat_response(messages: list[dict[str, Any]], model: str) -> dict[str, Any]:
    query = search_query_from_messages(messages)
    if not query:
        raise HTTPException(status_code=400, detail={"error": "messages or prompt is required for web search"})
    text, annotations = text_with_url_citations(run_web_search(query))
    return completion_response(
        model,
        text,
        messages=messages,
        annotations=chat_completion_annotations(annotations),
    )


def stream_web_search_chat_completion(
    messages: list[dict[str, Any]],
    model: str,
    *,
    include_usage: bool = False,
) -> Iterator[dict[str, Any]]:
    query = search_query_from_messages(messages)
    if not query:
        raise HTTPException(status_code=400, detail={"error": "messages or prompt is required for web search"})
    text, _annotations = text_with_url_citations(run_web_search(query))
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())
    yield completion_chunk(
        model,
        {"role": "assistant", "content": text},
        None,
        completion_id,
        created,
        include_usage=include_usage,
    )
    yield completion_chunk(model, {}, "stop", completion_id, created, include_usage=include_usage)
    if include_usage:
        yield completion_usage_chunk(model, chat_text_usage(model, messages, text), completion_id, created)


def image_result_content(result: dict[str, Any]) -> str:
    data = result.get("data")
    if isinstance(data, list) and data:
        return build_chat_image_markdown_content(result)
    return str(result.get("message") or "Image generation completed.")


def image_chat_response(body: dict[str, Any]) -> dict[str, Any]:
    model, prompt, n, images = chat_image_args(body)
    result = collect_image_outputs(stream_image_outputs_with_pool(ConversationRequest(
        prompt=prompt,
        model=model,
        n=n,
        response_format="b64_json",
        images=encode_images(images) or None,
    )))
    response = completion_response(model, image_result_content(result), int(result.get("created") or 0) or None)
    usage = image_usage(
        input_text_tokens=count_text_tokens(prompt, model),
        input_image_tokens=count_image_inputs_tokens(images, model),
        output_tokens=count_image_output_items_tokens(result.get("data")),
    )
    response["usage"] = chat_usage_from_image_usage(usage)
    return response


def image_chat_events(body: dict[str, Any], *, include_usage: bool = False) -> Iterator[dict[str, Any]]:
    model, prompt, n, images = chat_image_args(body)
    image_outputs = stream_image_outputs_with_pool(ConversationRequest(
        prompt=prompt,
        model=model,
        n=n,
        response_format="b64_json",
        images=encode_images(images) or None,
    ))
    yield from stream_image_chat_completion(
        image_outputs,
        model,
        prompt=prompt,
        images=images,
        include_usage=include_usage,
    )


def stream_image_chat_completion(
    image_outputs: Iterable[ImageOutput],
    model: str,
    *,
    prompt: str = "",
    images: list[tuple[bytes, str, str]] | None = None,
    include_usage: bool = False,
) -> Iterator[dict[str, Any]]:
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    created = int(time.time())
    sent_role = False
    sent_text = ""
    progress_sent = False
    result_items: list[dict[str, Any]] = []
    for output in image_outputs:
        content = ""
        if output.kind == "progress":
            if progress_sent:
                continue
            content = PUBLIC_IMAGE_PROGRESS_MESSAGE
            progress_sent = True
        elif output.kind == "result":
            content = build_chat_image_markdown_content({"data": output.data})
            result_items.extend(item for item in output.data if isinstance(item, dict))
        elif output.kind == "message":
            message = output.public_message()
            content = message[len(sent_text):] if message.startswith(sent_text) else message
        if not content:
            continue
        if not sent_role:
            sent_role = True
            yield completion_chunk(
                model,
                {"role": "assistant", "content": content},
                None,
                completion_id,
                created,
                include_usage=include_usage,
            )
        else:
            yield completion_chunk(
                model,
                {"content": content},
                None,
                completion_id,
                created,
                include_usage=include_usage,
            )
    if not sent_role:
        yield completion_chunk(
            model,
            {"role": "assistant", "content": ""},
            None,
            completion_id,
            created,
            include_usage=include_usage,
        )
    yield completion_chunk(model, {}, "stop", completion_id, created, include_usage=include_usage)
    if include_usage:
        usage = image_usage(
            input_text_tokens=count_text_tokens(prompt, model),
            input_image_tokens=count_image_inputs_tokens(images, model),
            output_tokens=count_image_output_items_tokens(result_items),
        )
        yield completion_usage_chunk(model, chat_usage_from_image_usage(usage), completion_id, created)


def handle(
    body: dict[str, Any],
    *,
    cache_scope: str = "",
    authenticated: bool = False,
) -> dict[str, Any] | Iterator[dict[str, Any]]:
    validate_chat_core_parameters(body)
    openai_v1_response.validate_tool_container(body)
    codex_deadline = (
        time.monotonic() + openai_v1_response._CODEX_TEXT_FAILOVER_DEADLINE_SECONDS
        if authenticated
        else None
    )
    if uses_chat_native_codex(body):
        if body.get("stream"):
            return _stream_chat_response_from_codex(body, deadline=codex_deadline)
        return _chat_response_from_codex(body, deadline=codex_deadline)
    image_request = is_image_chat_request(body)
    if image_request:
        validate_image_chat_parameters(body)
    else:
        validate_plain_text_parameters(body)
    if body.get("stream"):
        include_usage = include_stream_usage(body)
        if image_request:
            return image_chat_events(body, include_usage=include_usage)
        model, messages = text_chat_parts(body)
        if is_web_search_chat_request(body) and not has_unsupported_tools(body, WEB_SEARCH_TOOL_TYPES):
            return stream_web_search_chat_completion(messages, model, include_usage=include_usage)
        thinking_effort = thinking_effort_from_body(body)
        selected_token = (
            account_service.get_text_access_token(
                model=model,
                backend_capability="web",
                deadline=codex_deadline,
            )
            if authenticated or cache_scope
            else None
        )
        if selected_token and _is_codex_account_token(selected_token):
            return _stream_chat_response_from_codex(
                body,
                access_token=selected_token,
                deadline=codex_deadline,
            )
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
        compute = lambda: stream_text_chat_completion(
            text_backend(model, access_token=selected_token) if selected_token is not None else text_backend(model),
            messages,
            model,
            thinking_effort,
            include_usage,
        )
        key = cache_key(body, messages, stream=True, cache_scope=effective_scope)
        return chat_completion_cache.get_or_compute_stream(
            key,
            compute,
            replay=replay_chat_completion_stream,
        )
    if image_request:
        return image_chat_response(body)
    model, messages = text_chat_parts(body)
    if is_web_search_chat_request(body) and not has_unsupported_tools(body, WEB_SEARCH_TOOL_TYPES):
        return web_search_chat_response(messages, model)
    thinking_effort = thinking_effort_from_body(body)
    selected_token = (
        account_service.get_text_access_token(
            model=model,
            backend_capability="web",
            deadline=codex_deadline,
        )
        if authenticated or cache_scope
        else None
    )
    if selected_token and _is_codex_account_token(selected_token):
        return _chat_response_from_codex(
            body,
            access_token=selected_token,
            deadline=codex_deadline,
        )
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
    compute = lambda: completion_response(
        model,
        collect_text(
            text_backend(model, access_token=selected_token)
            if selected_token is not None
            else text_backend(model),
            ConversationRequest(model=model, messages=messages, thinking_effort=thinking_effort),
        ),
        messages=messages,
    )
    key = cache_key(body, messages, stream=False, cache_scope=effective_scope)
    return chat_completion_cache.get_or_compute_response(
        key,
        compute,
        replay=replay_chat_completion_response,
    )
