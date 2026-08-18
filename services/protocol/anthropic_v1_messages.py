from __future__ import annotations

import html
import json
import re
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from services.account_service import account_service
from services.openai_backend_api import OpenAIBackendAPI
from services.protocol.conversation import count_message_tokens, count_text_tokens, normalize_messages
from services.protocol.openai_v1_chat_complete import collect_chat_content, stream_text_chat_completion

XML_TOOL_RULE = """Tool output adapter: when calling tools, output ONLY this XML and no prose/markdown:
<tool_calls><tool_call><tool_name>TOOL_NAME</tool_name><parameters><PARAM><![CDATA[value]]></PARAM></parameters></tool_call></tool_calls>"""

_SUPPORTED_MESSAGE_FIELDS = frozenset({
    "model",
    "messages",
    "system",
    "stream",
    "tools",
})


@dataclass
class MessageRequest:
    backend: OpenAIBackendAPI
    messages: list[dict[str, Any]]
    model: str
    tools: Any = None


def _tool_meta(tool: dict[str, object]) -> tuple[str, str, object]:
    fn = tool.get("function") if isinstance(tool.get("function"), dict) else {}
    raw_name = tool.get("name") if tool.get("name") is not None else fn.get("name")
    if raw_name is not None and not isinstance(raw_name, str):
        raise HTTPException(status_code=400, detail={"error": "tool name must be a string"})
    raw_description = (
        tool.get("description")
        if tool.get("description") is not None
        else fn.get("description")
    )
    if raw_description is not None and not isinstance(raw_description, str):
        raise HTTPException(status_code=400, detail={"error": "tool description must be a string"})
    name = raw_name.strip() if isinstance(raw_name, str) else ""
    if not name:
        raise HTTPException(status_code=400, detail={"error": "tool name is required"})
    desc = raw_description.strip() if isinstance(raw_description, str) else ""
    schema = None
    for candidate in (
        tool.get("input_schema"),
        tool.get("parameters"),
        fn.get("input_schema"),
        fn.get("parameters"),
    ):
        if candidate is not None:
            schema = candidate
            break
    if not isinstance(schema, dict):
        raise HTTPException(status_code=400, detail={"error": "tool input schema must be an object"})
    return name, desc, schema


def build_tool_prompt(tools: object) -> str:
    if tools is None:
        return ""
    if not isinstance(tools, list):
        raise HTTPException(status_code=400, detail={"error": "tools must be an array"})
    blocks = []
    for tool in tools:
        if not isinstance(tool, dict):
            raise HTTPException(status_code=400, detail={"error": "tool must be an object"})
        name, desc, schema = _tool_meta(tool)
        if name:
            blocks.append(f"Tool: {name}\nDescription: {desc}\nParameters: {json.dumps(schema, ensure_ascii=False)}")
    if not blocks:
        return ""
    return "Available tools:\n" + "\n".join(blocks) + """

Tool use rules:
- If the user asks to list/read/search files, inspect project state, run a command, or answer from local code, you MUST call a suitable tool first. Do not say you cannot access files.
- To call tools, output ONLY XML and no prose/markdown:
<tool_calls><tool_call><tool_name>TOOL_NAME</tool_name><parameters><PARAM><![CDATA[value]]></PARAM></parameters></tool_call></tool_calls>
- Put parameters under <parameters> using the exact schema names.
""".strip()


def merge_system(system: object, extra: str) -> object:
    system = compact_system(system)
    if _has_claude_code_system(system):
        extra = XML_TOOL_RULE
    if not extra:
        return system
    if isinstance(system, str) and system.strip():
        return f"{system.strip()}\n\n{extra}"
    if isinstance(system, list):
        return [*system, {"type": "text", "text": extra}]
    return extra


def _has_claude_code_system(system: object) -> bool:
    if isinstance(system, str):
        return "You are Claude Code" in system
    if isinstance(system, list):
        return any(
            isinstance(item, dict)
            and isinstance(item.get("text"), str)
            and "You are Claude Code" in item["text"]
            for item in system
        )
    return False


def compact_system(system: object) -> object:
    if system is None:
        return None
    if isinstance(system, str):
        return _compact_system_text(system)
    if isinstance(system, list):
        result = []
        for item in system:
            if not isinstance(item, dict):
                raise HTTPException(
                    status_code=400,
                    detail={"error": "system block must be an object"},
                )
            if item.get("type") != "text":
                raise HTTPException(
                    status_code=400,
                    detail={"error": "system block type must be text"},
                )
            raw_text = item.get("text")
            if not isinstance(raw_text, str):
                raise HTTPException(
                    status_code=400,
                    detail={"error": "system text block must contain a string"},
                )
            copied = dict(item)
            copied["text"] = _compact_system_text(raw_text)
            result.append(copied)
        return result
    raise HTTPException(status_code=400, detail={"error": "system must be a string or array"})


def _compact_system_text(text: str) -> str:
    return text or ""


def _compact_message_text(text: str) -> str:
    return text or ""


def _validate_base64_image_source(source: object) -> None:
    if not isinstance(source, dict):
        raise HTTPException(
            status_code=400,
            detail={"error": "image source must be an object"},
        )
    if source.get("type") != "base64":
        raise HTTPException(
            status_code=400,
            detail={"error": "image source type is not supported"},
        )
    media_type = source.get("media_type")
    data = source.get("data")
    if (
        not isinstance(media_type, str)
        or not media_type.strip()
        or not isinstance(data, str)
        or not data.strip()
    ):
        raise HTTPException(
            status_code=400,
            detail={"error": "image source must contain string media_type and data"},
        )


def _validate_image_reference(value: object) -> None:
    if isinstance(value, str):
        if value.strip():
            return
    elif isinstance(value, dict):
        url = value.get("url") or value.get("image_url")
        if isinstance(url, str) and url.strip():
            return
    raise HTTPException(
        status_code=400,
        detail={"error": "image reference must be a non-empty string or URL object"},
    )


def _validate_tool_result_content_blocks(content: list[object]) -> None:
    for block in content:
        if not isinstance(block, dict):
            raise HTTPException(
                status_code=400,
                detail={"error": "tool result content blocks must be objects"},
            )
        block_type = block.get("type")
        if block_type == "text":
            if not isinstance(block.get("text"), str):
                raise HTTPException(
                    status_code=400,
                    detail={"error": "tool result text block must contain a string"},
                )
        elif block_type == "image":
            _validate_base64_image_source(block.get("source"))
        else:
            raise HTTPException(
                status_code=400,
                detail={"error": "tool result content block type is not supported"},
            )


def _xml_text(text: str) -> str:
    """Escape untrusted text before placing it inside the adapter's XML envelope."""
    return html.escape(text, quote=False)


def preprocess_payload(payload: dict[str, object], text_mapper: Callable[[str], str] | None = None) -> dict[str, object]:
    payload["messages"] = preprocess_messages(payload.get("messages"), text_mapper)
    payload["system"] = merge_system(payload.get("system"), build_tool_prompt(payload.get("tools")))
    return payload


def message_request(body: dict[str, Any]) -> MessageRequest:
    if set(body) - _SUPPORTED_MESSAGE_FIELDS:
        raise HTTPException(
            status_code=400,
            detail={"error": "message parameter is not supported by the configured backend"},
        )
    payload = preprocess_payload(dict(body))
    model = str(payload.get("model") or "auto").strip() or "auto"
    messages = normalize_messages(payload.get("messages"), payload.get("system"))
    return MessageRequest(
        backend=OpenAIBackendAPI(
            access_token=account_service.get_text_access_token(
                model=model,
                backend_capability="web",
            )
        ),
        messages=messages,
        model=model,
        tools=payload.get("tools"),
    )


def preprocess_messages(messages: object, text_mapper: Callable[[str], str] | None = None) -> object:
    if not isinstance(messages, list):
        raise HTTPException(status_code=400, detail={"error": "messages must be an array"})
    mapper = text_mapper or (lambda text: text)
    result = []
    for message in messages:
        if not isinstance(message, dict):
            raise HTTPException(
                status_code=400,
                detail={"error": "message must be an object"},
            )
        if "role" not in message or "content" not in message:
            raise HTTPException(
                status_code=400,
                detail={"error": "message must contain role and content"},
            )
        item = dict(message)
        role = message.get("role")
        if not isinstance(role, str) or role not in {"user", "assistant"}:
            raise HTTPException(
                status_code=400,
                detail={"error": "message role must be user or assistant"},
            )
        content = item.get("content")
        if isinstance(content, str):
            item["content"] = _compact_message_text(mapper(content))
        elif isinstance(content, list):
            item["content"] = [_preprocess_block(block, mapper) for block in content]
        else:
            raise HTTPException(
                status_code=400,
                detail={"error": "message content must be a string or array"},
            )
        result.append(item)
    return result


def _preprocess_block(block: object, text_mapper: Callable[[str], str]) -> object:
    if not isinstance(block, dict):
        raise HTTPException(
            status_code=400,
            detail={"error": "message content block must be an object"},
        )
    raw_type = block.get("type")
    if not isinstance(raw_type, str):
        raise HTTPException(
            status_code=400,
            detail={"error": "message content block type must be a string"},
        )
    block_type = raw_type
    if block_type not in {"text", "tool_use", "tool_result", "image", "image_url", "input_image"}:
        raise HTTPException(
            status_code=400,
            detail={"error": "message content block type is not supported"},
        )
    if block_type == "text":
        raw_text = block.get("text")
        if not isinstance(raw_text, str):
            raise HTTPException(
                status_code=400,
                detail={"error": "message text block must contain a string"},
            )
        item = dict(block)
        item["text"] = _compact_message_text(text_mapper(raw_text))
        return item
    if block_type == "tool_use":
        name = block.get("name")
        if not isinstance(name, str) or not name.strip():
            raise HTTPException(
                status_code=400,
                detail={"error": "tool use name must be a string"},
            )
        tool_input = block.get("input", {})
        if not isinstance(tool_input, dict):
            raise HTTPException(
                status_code=400,
                detail={"error": "tool use input must be an object"},
            )
        encoded_name = _xml_text(name.strip())
        encoded_input = _xml_text(json.dumps(tool_input, ensure_ascii=False))
        return {"type": "text", "text": f"<tool_calls><tool_call><tool_name>{encoded_name}</tool_name><parameters>{encoded_input}</parameters></tool_call></tool_calls>"}
    if block_type == "tool_result":
        tool_use_id = block.get("tool_use_id")
        if not isinstance(tool_use_id, str) or not tool_use_id.strip():
            raise HTTPException(
                status_code=400,
                detail={"error": "tool result id must be a string"},
            )
        content = block.get("content", "")
        if isinstance(content, str):
            content_text = content
        elif isinstance(content, list):
            _validate_tool_result_content_blocks(content)
            content_text = json.dumps(content, ensure_ascii=False)
        else:
            raise HTTPException(
                status_code=400,
                detail={"error": "tool result content must be text or a list"},
            )
        return {"type": "text", "text": f"Tool result {_xml_text(tool_use_id.strip())}: {_xml_text(content_text)}"}
    if block_type == "image":
        _validate_base64_image_source(block.get("source"))
        return dict(block)
    if block_type == "image_url":
        image_url = block.get("image_url")
        if image_url is None:
            image_url = block.get("url")
        _validate_image_reference(image_url)
        return dict(block)
    if block_type == "input_image":
        source = block.get("source")
        if source is not None:
            _validate_base64_image_source(source)
            return dict(block)
        image_url = block.get("image_url")
        if image_url is None:
            image_url = block.get("url")
        if image_url is None:
            image_url = block.get("b64_json")
        if image_url is None:
            image_url = block.get("base64")
        _validate_image_reference(image_url)
        return dict(block)
    return block


def message_response(model: str, text: str, input_tokens: int, output_tokens: int, tools: object = None) -> dict[str, object]:
    content, stop_reason = content_blocks(text, tools)
    return {
        "id": f"msg_{uuid.uuid4()}",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason,
        "stop_sequence": None,
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
    }


def content_blocks(text: str, tools: object = None) -> tuple[list[dict[str, object]], str]:
    calls = parse_tool_calls(text) if isinstance(tools, list) and tools else []
    text = strip_tool_markup(text)
    if calls:
        content = ([{"type": "text", "text": text}] if text else []) + [{"type": "tool_use", "id": f"toolu_{uuid.uuid4()}", "name": name, "input": args} for name, args in calls]
        return content, "tool_use"
    return [{"type": "text", "text": text}], "end_turn"


def strip_tool_markup(text: str) -> str:
    return re.sub(r"(?is)<tool_calls\b[^>]*>.*?</tool_calls>|<tool_call\b[^>]*>.*?</tool_call>|<function_call\b[^>]*>.*?</function_call>|<invoke\b[^>]*>.*?</invoke>", "", text or "").strip()


def streamable_text(text: str) -> str:
    text = text or ""
    match = re.search(r"(?is)<tool_calls\b|<tool_call\b|<function_call\b|<invoke\b", text)
    return text[:match.start()].rstrip() if match else text


def parse_tool_calls(text: str) -> list[tuple[str, dict[str, object]]]:
    text = re.sub(r"(?is)```.*?```", "", text or "").strip()
    blocks = re.findall(r"(?is)<tool_call\b[^>]*>(.*?)</tool_call>|<function_call\b[^>]*>(.*?)</function_call>|<invoke\b[^>]*>(.*?)</invoke>", text)
    result = []
    for block in (next((part for part in match if part), "") for match in blocks):
        name = xml_value(block, "tool_name") or xml_value(block, "name") or xml_value(block, "function")
        params = xml_value(block, "parameters") or xml_value(block, "input") or xml_value(block, "arguments") or "{}"
        if name:
            result.append((name, parse_tool_params(params)))
    return result


def xml_value(text: str, tag: str) -> str:
    match = re.search(rf"(?is)<{tag}\b[^>]*>(.*?)</{tag}>", text)
    if not match:
        return ""
    value = match.group(1).strip()
    cdata = re.fullmatch(r"(?is)<!\[CDATA\[(.*?)]]>", value)
    return html.unescape(cdata.group(1) if cdata else value).strip()


def parse_tool_params(raw: str) -> dict[str, object]:
    raw = raw.strip()
    try:
        parsed = json.loads(raw)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {m.group(1): parse_tool_value(m.group(2)) for m in re.finditer(r"(?is)<([\w.-]+)\b[^>]*>(.*?)</\1>", raw)}


def parse_tool_value(raw: str) -> object:
    value = xml_value(f"<x>{raw}</x>", "x")
    try:
        return json.loads(value)
    except Exception:
        return value


def _close_iterator(source: object) -> None:
    close = getattr(source, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def stream_events(
    chunks: Iterable[dict[str, object]],
    model: str,
    input_tokens: int,
    output_tokens: Callable[[str], int],
    tools: object = None,
    backend: object = None,
) -> Iterator[dict[str, object]]:
    source = None
    try:
        source = iter(chunks)
        yield from _stream_events(source, model, input_tokens, output_tokens, tools)
    finally:
        try:
            if source is not None:
                _close_iterator(source)
        finally:
            _close_iterator(backend)


def _stream_events(chunks: Iterable[dict[str, object]], model: str, input_tokens: int, output_tokens: Callable[[str], int], tools: object = None) -> Iterator[dict[str, object]]:
    message_id = f"msg_{uuid.uuid4()}"
    created = int(time.time())
    current_text = ""
    streamed_text = ""
    tool_mode = isinstance(tools, list) and bool(tools)
    tool_started = False
    text_open = False
    yield {"type": "message_start", "message": {"id": message_id, "type": "message", "role": "assistant", "model": model, "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": input_tokens, "output_tokens": 0}}}
    if not tool_mode:
        text_open = True
        yield {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}
    for chunk in chunks:
        if not isinstance(chunk, dict):
            raise RuntimeError("malformed upstream chat chunk")
        choices = chunk.get("choices")
        if choices in (None, []):
            choice = {}
        elif not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            raise RuntimeError("malformed upstream chat chunk")
        else:
            choice = choices[0]
        raw_delta = choice.get("delta")
        if raw_delta is not None and not isinstance(raw_delta, dict):
            raise RuntimeError("malformed upstream chat chunk")
        delta = raw_delta or {}
        text_delta = delta.get("content", "") if isinstance(delta, dict) else ""
        if text_delta is not None and not isinstance(text_delta, str):
            raise RuntimeError("malformed upstream text delta")
        text_delta = text_delta or ""
        if text_delta:
            current_text += text_delta
            if not tool_started:
                visible_text = current_text if not tool_mode else streamable_text(current_text)
                if visible_text.startswith(streamed_text):
                    text_delta = visible_text[len(streamed_text):]
                    if text_delta:
                        if not text_open:
                            text_open = True
                            yield {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}
                        streamed_text = visible_text
                        yield {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": text_delta}}
                tool_started = tool_mode and visible_text != current_text
        if choice.get("finish_reason"):
            content, stop_reason = content_blocks(current_text, tools)
            if text_open:
                yield {"type": "content_block_stop", "index": 0}
            if stop_reason == "tool_use":
                start_index = 1 if text_open else 0
                if content and content[0]["type"] == "text":
                    remaining = str(content[0].get("text") or "")[len(streamed_text):]
                    if remaining:
                        if not text_open:
                            yield {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}}
                        yield {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": remaining}}
                        if not text_open:
                            yield {"type": "content_block_stop", "index": 0}
                    start_index = 1
                    content = content[1:]
                yield from _stream_buffered_blocks(content, start_index)
            yield {"type": "message_delta", "delta": {"stop_reason": stop_reason, "stop_sequence": None}, "usage": {"output_tokens": output_tokens(current_text)}}
            break
    yield {"type": "message_stop", "created": created}


def _stream_buffered_blocks(content: list[dict[str, object]], start_index: int = 0) -> Iterator[dict[str, object]]:
    for offset, block in enumerate(content):
        index = start_index + offset
        if block["type"] == "tool_use":
            start = {"type": "tool_use", "id": block["id"], "name": block["name"], "input": {}}
            delta = {"type": "input_json_delta", "partial_json": json.dumps(block.get("input") or {}, ensure_ascii=False)}
        else:
            start = {"type": "text", "text": ""}
            delta = {"type": "text_delta", "text": block.get("text") or ""}
        yield {"type": "content_block_start", "index": index, "content_block": start}
        yield {"type": "content_block_delta", "index": index, "delta": delta}
        yield {"type": "content_block_stop", "index": index}


def handle(body: dict[str, Any]) -> dict[str, Any] | Iterator[dict[str, Any]]:
    request = message_request(body)
    if body.get("stream"):
        return stream_events(
            stream_text_chat_completion(request.backend, request.messages, request.model),
            request.model,
            count_message_tokens(request.messages, request.model),
            lambda text: count_text_tokens(text, request.model),
            request.tools,
            request.backend,
        )
    source = stream_text_chat_completion(request.backend, request.messages, request.model)
    try:
        text = collect_chat_content(source)
    finally:
        try:
            _close_iterator(source)
        finally:
            _close_iterator(request.backend)
    return message_response(
        request.model,
        text,
        count_message_tokens(request.messages, request.model),
        count_text_tokens(text, request.model),
        request.tools,
    )
