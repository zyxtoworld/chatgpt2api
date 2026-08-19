from __future__ import annotations

import html
import json
import re
import time
import uuid
import base64
import binascii
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from services.account_service import account_service
from services.openai_backend_api import OpenAIBackendAPI
from services.protocol.conversation import count_message_tokens, count_text_tokens, encoding_for_model, normalize_messages
from services.protocol.openai_v1_chat_complete import collect_chat_content, stream_text_chat_completion
from services.protocol.web_search_tool import clean_search_text, normalized_sources, run_web_search, search_query_from_messages

XML_TOOL_RULE = """Tool output adapter: when calling tools, output ONLY this XML and no prose/markdown:
<tool_calls><tool_call><tool_name>TOOL_NAME</tool_name><parameters><PARAM><![CDATA[value]]></PARAM></parameters></tool_call></tool_calls>"""

_SUPPORTED_MESSAGE_FIELDS = frozenset({
    "max_tokens",
    "model",
    "messages",
    "system",
    "stream",
    "tools",
})
_UNSUPPORTED_MESSAGE_FIELDS = frozenset({
    "cache_control",
    "container",
    "inference_geo",
    "metadata",
    "output_config",
    "service_tier",
    "stop_sequences",
    "temperature",
    "thinking",
    "tool_choice",
    "top_k",
    "top_p",
    "user_profile_id",
})
_PUBLIC_MESSAGE_FIELDS = _SUPPORTED_MESSAGE_FIELDS | _UNSUPPORTED_MESSAGE_FIELDS
_SEARCH_OPAQUE_PREFIX = "chatgpt2api-search-v1:"
_SEARCH_OPAQUE_MAX_CHARS = 8192


@dataclass
class MessageRequest:
    backend: OpenAIBackendAPI | None
    messages: list[dict[str, Any]]
    model: str
    tools: Any = None
    max_tokens: int | None = None
    search_result: dict[str, Any] | None = None
    search_query: str | None = None
    search_tool_use_id: str | None = None


class _MaxTokensStream:
    """Bound one upstream iterator for both Anthropic stream modes."""

    def __init__(self, source: Iterable[dict[str, object]], model: str, limit: int) -> None:
        self._source = iter(source)
        self._encoding = encoding_for_model(model)
        self._limit = limit
        self._text = ""
        self._at_limit = False
        self._closed = False
        self.hit_max_tokens = False

    def __iter__(self) -> "_MaxTokensStream":
        return self

    def __next__(self) -> dict[str, object]:
        if self._closed:
            raise StopIteration
        chunk = next(self._source)
        if not isinstance(chunk, dict):
            return chunk
        choices = chunk.get("choices")
        if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
            return chunk
        choice = choices[0]
        delta = choice.get("delta")
        content = delta.get("content") if isinstance(delta, dict) else None
        if not isinstance(content, str) or not content:
            return chunk
        candidate = self._text + content
        tokens = self._encoding.encode(candidate)
        if len(tokens) <= self._limit:
            self._text = candidate
            self._at_limit = len(tokens) == self._limit
            return chunk

        prefix = _strict_token_prefix(self._encoding, tokens[:self._limit])
        safe_prefix = _drop_incomplete_tool_markup(prefix)
        if not safe_prefix.startswith(self._text):
            safe_prefix = self._text
        delta_text = safe_prefix[len(self._text):]
        self._text = safe_prefix
        self.hit_max_tokens = True
        self._closed = True
        _close_iterator(self._source)
        return _rewrite_chat_chunk(chunk, delta_text, "max_tokens")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        _close_iterator(self._source)


def _rewrite_chat_chunk(
    chunk: dict[str, object],
    content: str,
    finish_reason: str,
) -> dict[str, object]:
    copied = dict(chunk)
    choices = copied.get("choices")
    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
        return copied
    choice = dict(choices[0])
    delta = choice.get("delta")
    choice["delta"] = {**delta, "content": content} if isinstance(delta, dict) else {"content": content}
    choice["finish_reason"] = finish_reason
    copied["choices"] = [choice, *choices[1:]]
    return copied


def _strict_token_prefix(encoding: object, tokens: list[int]) -> str:
    decode_token = getattr(encoding, "decode_single_token_bytes", None)
    if not callable(decode_token):
        raise RuntimeError("tokenizer cannot provide strict UTF-8 decoding")
    token_bytes = [decode_token(token) for token in tokens]
    for token_count in range(len(token_bytes), -1, -1):
        raw = b"".join(token_bytes[:token_count])
        try:
            decoded = raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        encode = getattr(encoding, "encode", None)
        if callable(encode) and len(encode(decoded)) > len(tokens):
            continue
        return decoded
    return ""


def _drop_incomplete_tool_markup(text: str) -> str:
    lowered = text.lower()
    for opener in ("<tool_calls", "<function_call", "<tool_call", "<invoke"):
        for size in range(1, len(opener)):
            prefix = opener[:size]
            index = lowered.rfind(prefix)
            if index >= 0 and lowered[index:] == prefix:
                return text[:index].rstrip()
    for match in re.finditer(r"(?is)<(tool_calls|tool_call|function_call|invoke)\b", text):
        if not re.search(rf"(?is)</{match.group(1)}>", text[match.start():]):
            return text[:match.start()].rstrip()
    return text


def _tool_meta(tool: dict[str, object]) -> tuple[str, str, object]:
    _reject_unknown_fields(
        tool,
        {"type", "name", "description", "input_schema", "parameters", "function"},
        "tool",
    )
    if "type" in tool and tool.get("type") not in (None, "function"):
        raise HTTPException(status_code=400, detail={"error": "tool type is not supported"})
    if "function" in tool and not isinstance(tool.get("function"), dict):
        raise HTTPException(status_code=400, detail={"error": "tool function must be an object"})
    if isinstance(tool.get("function"), dict):
        _reject_unknown_fields(
            tool["function"],
            {"name", "description", "input_schema", "parameters"},
            "tool function",
        )
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


def _is_web_search_tool(tool: object) -> bool:
    return isinstance(tool, dict) and str(tool.get("type") or "").strip() == "web_search_20250305"


def _is_web_search_tool_type(tool: object) -> bool:
    return isinstance(tool, dict) and str(tool.get("type") or "").strip().startswith("web_search_")


def _web_search_tool(tools: object) -> dict[str, object] | None:
    if not isinstance(tools, list):
        return None
    found: dict[str, object] | None = None
    ordinary_tools = 0
    for tool in tools:
        if _is_web_search_tool_type(tool) and not _is_web_search_tool(tool):
            raise HTTPException(status_code=400, detail={"error": "web search tool version is not supported by the configured backend"})
        if not _is_web_search_tool(tool):
            ordinary_tools += 1
            continue
        _reject_unknown_fields(
            tool,
            {"type", "name", "max_uses", "allowed_domains", "blocked_domains", "user_location"},
            "web search tool",
        )
        if found is not None:
            raise HTTPException(status_code=400, detail={"error": "multiple web search tools are not supported by the configured backend"})
        found = tool
    if found is None:
        return None
    if ordinary_tools:
        raise HTTPException(status_code=400, detail={"error": "web search and function tools cannot be combined by the configured backend"})
    if found.get("name") != "web_search":
        raise HTTPException(status_code=400, detail={"error": "web search tool name must be web_search"})
    max_uses = found.get("max_uses")
    if max_uses is not None and (
        not isinstance(max_uses, int) or isinstance(max_uses, bool) or max_uses <= 0
    ):
        raise HTTPException(status_code=400, detail={"error": "web search max_uses must be a positive integer"})
    if any(found.get(key) is not None for key in ("allowed_domains", "blocked_domains", "user_location")):
        raise HTTPException(status_code=400, detail={"error": "web search filtering and user_location are not supported by the configured backend"})
    return found


def build_tool_prompt(tools: object) -> str:
    if tools is None:
        return ""
    if not isinstance(tools, list):
        raise HTTPException(status_code=400, detail={"error": "tools must be an array"})
    blocks = []
    for tool in tools:
        if not isinstance(tool, dict):
            raise HTTPException(status_code=400, detail={"error": "tool must be an object"})
        if _is_web_search_tool_type(tool):
            continue
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
            _reject_unknown_fields(item, {"type", "text"}, "system block")
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


def _reject_unknown_fields(value: dict[str, object], allowed: set[str], label: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise HTTPException(
            status_code=400,
            detail={"error": f"{label} field '{unknown[0]}' is not supported by the configured backend"},
        )


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


def _encode_search_opaque(kind: str, value: object) -> str:
    raw = json.dumps({"kind": kind, "value": value}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    encoded = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    result = _SEARCH_OPAQUE_PREFIX + encoded
    if len(result) > _SEARCH_OPAQUE_MAX_CHARS:
        raise HTTPException(status_code=400, detail={"error": "web search continuation is too large"})
    return result


def _decode_search_opaque(value: object, expected_kind: str) -> object:
    if not isinstance(value, str) or not value.startswith(_SEARCH_OPAQUE_PREFIX) or len(value) > _SEARCH_OPAQUE_MAX_CHARS:
        raise HTTPException(status_code=400, detail={"error": "web search continuation is invalid"})
    encoded = value[len(_SEARCH_OPAQUE_PREFIX):]
    try:
        padded = encoded + "=" * (-len(encoded) % 4)
        decoded = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    except (ValueError, TypeError, UnicodeDecodeError, binascii.Error, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail={"error": "web search continuation is invalid"}) from exc
    if not isinstance(decoded, dict) or decoded.get("kind") != expected_kind:
        raise HTTPException(status_code=400, detail={"error": "web search continuation is invalid"})
    return decoded.get("value")


def preprocess_payload(payload: dict[str, object], text_mapper: Callable[[str], str] | None = None) -> dict[str, object]:
    payload["messages"] = preprocess_messages(payload.get("messages"), text_mapper)
    payload["system"] = merge_system(payload.get("system"), build_tool_prompt(payload.get("tools")))
    return payload


def message_request(body: dict[str, Any]) -> MessageRequest:
    unknown_fields = set(body) - _PUBLIC_MESSAGE_FIELDS
    if unknown_fields:
        raise HTTPException(
            status_code=400,
            detail={"error": "message parameter is not supported by the configured backend"},
        )
    unsupported_fields = sorted(set(body) & _UNSUPPORTED_MESSAGE_FIELDS)
    if unsupported_fields:
        raise HTTPException(
            status_code=400,
            detail={"error": f"message parameter '{unsupported_fields[0]}' is not supported by the configured backend"},
        )
    raw_max_tokens = body.get("max_tokens")
    if raw_max_tokens is not None and (
        not isinstance(raw_max_tokens, int)
        or isinstance(raw_max_tokens, bool)
        or raw_max_tokens <= 0
    ):
        raise HTTPException(status_code=400, detail={"error": "max_tokens must be a positive integer"})
    payload = preprocess_payload(dict(body))
    model = str(payload.get("model") or "auto").strip() or "auto"
    messages = normalize_messages(payload.get("messages"), payload.get("system"))
    search_tool = _web_search_tool(payload.get("tools"))
    if search_tool is not None:
        query = search_query_from_messages(messages)
        if not query:
            raise HTTPException(status_code=400, detail={"error": "web search requires a user query"})
        return MessageRequest(
            backend=None,
            messages=messages,
            model=model,
            tools=payload.get("tools"),
            max_tokens=raw_max_tokens if isinstance(raw_max_tokens, int) else None,
            search_result=run_web_search(query),
            search_query=query,
            search_tool_use_id=f"srvtoolu_{uuid.uuid4().hex}",
        )
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
        max_tokens=raw_max_tokens if isinstance(raw_max_tokens, int) else None,
    )


def preprocess_messages(messages: object, text_mapper: Callable[[str], str] | None = None) -> object:
    if not isinstance(messages, list):
        raise HTTPException(status_code=400, detail={"error": "messages must be an array"})
    mapper = text_mapper or (lambda text: text)
    result = []
    server_tool_ids: set[str] = set()
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
        _reject_unknown_fields(message, {"role", "content"}, "message")
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
            for block in content:
                if not isinstance(block, dict):
                    continue
                if block.get("type") == "server_tool_use":
                    tool_use_id = block.get("id")
                    if isinstance(tool_use_id, str) and tool_use_id.strip():
                        server_tool_ids.add(tool_use_id.strip())
                elif block.get("type") == "web_search_tool_result":
                    tool_use_id = block.get("tool_use_id")
                    if tool_use_id not in server_tool_ids:
                        raise HTTPException(status_code=400, detail={"error": "web search tool result has no matching tool use"})
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
    if block_type not in {
        "text", "tool_use", "tool_result", "image", "image_url", "input_image",
        "server_tool_use", "web_search_tool_result",
    }:
        raise HTTPException(
            status_code=400,
            detail={"error": "message content block type is not supported"},
        )
    if block_type == "text":
        _reject_unknown_fields(block, {"type", "text", "citations"}, "text block")
        raw_text = block.get("text")
        if not isinstance(raw_text, str):
            raise HTTPException(
                status_code=400,
                detail={"error": "message text block must contain a string"},
            )
        item = dict(block)
        item["text"] = _compact_message_text(text_mapper(raw_text))
        citations = block.get("citations")
        if citations is not None:
            if not isinstance(citations, list):
                raise HTTPException(status_code=400, detail={"error": "citations must be an array"})
            for citation in citations:
                if not isinstance(citation, dict) or citation.get("type") != "web_search_result_location":
                    raise HTTPException(status_code=400, detail={"error": "web search citation is not supported"})
                if not isinstance(citation.get("url"), str) or not citation["url"].strip():
                    raise HTTPException(status_code=400, detail={"error": "web search citation url is required"})
                if not isinstance(citation.get("encrypted_index"), str):
                    raise HTTPException(status_code=400, detail={"error": "web search citation index is required"})
                _decode_search_opaque(citation["encrypted_index"], "search-index")
            item.pop("citations", None)
        return item
    if block_type == "server_tool_use":
        _reject_unknown_fields(block, {"type", "id", "name", "input"}, "server tool use")
        tool_use_id = block.get("id")
        name = block.get("name")
        input_value = block.get("input")
        if not isinstance(tool_use_id, str) or not tool_use_id.strip() or name != "web_search" or not isinstance(input_value, dict) or not isinstance(input_value.get("query"), str) or not input_value["query"].strip():
            raise HTTPException(status_code=400, detail={"error": "web search server tool use is invalid"})
        return {"type": "text", "text": f"Web search call {tool_use_id.strip()}: {input_value['query'].strip()}"}
    if block_type == "web_search_tool_result":
        _reject_unknown_fields(block, {"type", "tool_use_id", "content"}, "web search tool result")
        tool_use_id = block.get("tool_use_id")
        result_content = block.get("content")
        if not isinstance(tool_use_id, str) or not tool_use_id.strip() or not isinstance(result_content, list):
            raise HTTPException(status_code=400, detail={"error": "web search tool result is invalid"})
        lines = [f"Web search results for {tool_use_id.strip()}:"]
        for item in result_content:
            if not isinstance(item, dict) or item.get("type") != "web_search_result":
                raise HTTPException(status_code=400, detail={"error": "web search result block is invalid"})
            url = item.get("url")
            title = item.get("title")
            if not isinstance(url, str) or not url.strip() or not isinstance(title, str):
                raise HTTPException(status_code=400, detail={"error": "web search result fields are invalid"})
            opaque = _decode_search_opaque(item.get("encrypted_content"), "search-result")
            if not isinstance(opaque, dict) or opaque.get("url") != url or opaque.get("title") != title:
                raise HTTPException(status_code=400, detail={"error": "web search result continuation does not match"})
            snippet = opaque.get("snippet") if isinstance(opaque.get("snippet"), str) else ""
            lines.append(f"- {title}: {url} {snippet}".strip())
        return {"type": "text", "text": "\n".join(lines)}
    if block_type == "tool_use":
        _reject_unknown_fields(block, {"type", "id", "name", "input"}, "tool use")
        tool_use_id = block.get("id")
        if not isinstance(tool_use_id, str) or not tool_use_id.strip():
            raise HTTPException(status_code=400, detail={"error": "tool use id must be a string"})
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
        encoded_id = _xml_text(tool_use_id.strip())
        encoded_input = _xml_text(json.dumps(tool_input, ensure_ascii=False))
        return {"type": "text", "text": f"<tool_calls><tool_call><tool_use_id>{encoded_id}</tool_use_id><tool_name>{encoded_name}</tool_name><parameters>{encoded_input}</parameters></tool_call></tool_calls>"}
    if block_type == "tool_result":
        _reject_unknown_fields(block, {"type", "tool_use_id", "content", "is_error"}, "tool result")
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
        is_error = block.get("is_error", False)
        if not isinstance(is_error, bool):
            raise HTTPException(status_code=400, detail={"error": "tool result is_error must be boolean"})
        label = "Tool error" if is_error else "Tool result"
        return {"type": "text", "text": f"{label} {_xml_text(tool_use_id.strip())}: {_xml_text(content_text)}"}
    if block_type == "image":
        _reject_unknown_fields(block, {"type", "source"}, "image block")
        _validate_base64_image_source(block.get("source"))
        return dict(block)
    if block_type == "image_url":
        _reject_unknown_fields(block, {"type", "image_url", "url"}, "image URL block")
        image_url = block.get("image_url")
        if image_url is None:
            image_url = block.get("url")
        _validate_image_reference(image_url)
        return dict(block)
    if block_type == "input_image":
        _reject_unknown_fields(block, {"type", "source", "image_url", "url", "b64_json", "base64"}, "input image block")
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


def message_response(
    model: str,
    text: str,
    input_tokens: int,
    output_tokens: int,
    tools: object = None,
    stop_reason_override: str | None = None,
) -> dict[str, object]:
    content, stop_reason = content_blocks(text, tools)
    return {
        "id": f"msg_{uuid.uuid4()}",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": content,
        "stop_reason": stop_reason_override or stop_reason,
        "stop_sequence": None,
        "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens},
    }


def _search_content(result: dict[str, Any]) -> tuple[str, list[dict[str, object]]]:
    answer = result.get("answer")
    text = clean_search_text(answer if isinstance(answer, str) else "")
    citations = []
    for source in normalized_sources(result):
        citations.append({
            "type": "web_search_result_location",
            "url": source["url"],
            "title": source["title"] or source["url"],
            "encrypted_index": _encode_search_opaque("search-index", {"url": source["url"]}),
            "cited_text": source["snippet"][:150],
        })
    return text, citations


def _bounded_plain_text(text: str, model: str, limit: int | None) -> tuple[str, bool]:
    if limit is None:
        return text, False
    tokens = encoding_for_model(model).encode(text)
    if len(tokens) <= limit:
        return text, False
    return _strict_token_prefix(encoding_for_model(model), tokens[:limit]), True


def _search_result_blocks(result: dict[str, Any]) -> list[dict[str, object]]:
    return [
        {
            "type": "web_search_result",
            "url": source["url"],
            "title": source["title"] or source["url"],
            "encrypted_content": _encode_search_opaque("search-result", source),
        }
        for source in normalized_sources(result)
    ]


def _search_response(
    model: str,
    result: dict[str, Any],
    input_tokens: int,
    max_tokens: int | None,
    query: str,
    tool_use_id: str,
) -> dict[str, object]:
    text, citations = _search_content(result)
    text, hit_max = _bounded_plain_text(text, model, max_tokens)
    return {
        "id": f"msg_{uuid.uuid4()}",
        "type": "message",
        "role": "assistant",
        "model": model,
        "content": [
            {"type": "server_tool_use", "id": tool_use_id, "name": "web_search", "input": {"query": query}},
            {"type": "web_search_tool_result", "tool_use_id": tool_use_id, "content": _search_result_blocks(result)},
            {"type": "text", "text": text, "citations": citations},
        ],
        "stop_reason": "max_tokens" if hit_max else "end_turn",
        "stop_sequence": None,
        "usage": {
            "input_tokens": input_tokens,
            "output_tokens": count_text_tokens(text, model),
            "server_tool_use": {"web_search_requests": 1},
        },
    }


def _stream_search_response(
    model: str,
    result: dict[str, Any],
    input_tokens: int,
    max_tokens: int | None,
    query: str,
    tool_use_id: str,
) -> Iterator[dict[str, object]]:
    text, citations = _search_content(result)
    text, hit_max = _bounded_plain_text(text, model, max_tokens)
    message_id = f"msg_{uuid.uuid4()}"
    yield {"type": "message_start", "message": {"id": message_id, "type": "message", "role": "assistant", "model": model, "content": [], "stop_reason": None, "stop_sequence": None, "usage": {"input_tokens": input_tokens, "output_tokens": 0, "server_tool_use": {"web_search_requests": 1}}}}
    yield {"type": "content_block_start", "index": 0, "content_block": {"type": "server_tool_use", "id": tool_use_id, "name": "web_search", "input": {}}}
    yield {"type": "content_block_delta", "index": 0, "delta": {"type": "input_json_delta", "partial_json": json.dumps({"query": query}, ensure_ascii=False)}}
    yield {"type": "content_block_stop", "index": 0}
    yield {"type": "content_block_start", "index": 1, "content_block": {"type": "web_search_tool_result", "tool_use_id": tool_use_id, "content": _search_result_blocks(result)}}
    yield {"type": "content_block_stop", "index": 1}
    yield {"type": "content_block_start", "index": 2, "content_block": {"type": "text", "text": "", "citations": []}}
    if text:
        yield {"type": "content_block_delta", "index": 2, "delta": {"type": "text_delta", "text": text}}
    for citation in citations:
        yield {"type": "content_block_delta", "index": 2, "delta": {"type": "citations_delta", "citation": citation}}
    yield {"type": "content_block_stop", "index": 2}
    yield {"type": "message_delta", "delta": {"stop_reason": "max_tokens" if hit_max else "end_turn", "stop_sequence": None}, "usage": {"output_tokens": count_text_tokens(text, model)}}
    yield {"type": "message_stop"}


def content_blocks(text: str, tools: object = None) -> tuple[list[dict[str, object]], str]:
    calls = parse_tool_calls_with_ids(text) if isinstance(tools, list) and tools else []
    text = strip_tool_markup(text)
    if calls:
        content = ([{"type": "text", "text": text}] if text else []) + [{"type": "tool_use", "id": call_id or f"toolu_{uuid.uuid4()}", "name": name, "input": args} for call_id, name, args in calls]
        return content, "tool_use"
    return [{"type": "text", "text": text}], "end_turn"


def strip_tool_markup(text: str) -> str:
    return re.sub(r"(?is)<tool_calls\b[^>]*>.*?</tool_calls>|<tool_call\b[^>]*>.*?</tool_call>|<function_call\b[^>]*>.*?</function_call>|<invoke\b[^>]*>.*?</invoke>", "", text or "").strip()


def streamable_text(text: str) -> str:
    text = text or ""
    lowered = text.lower()
    matches = list(re.finditer(r"(?is)<(tool_calls|tool_call|function_call|invoke)\b", text))
    if matches:
        return text[:matches[0].start()].rstrip()
    for opener in ("<tool_calls", "<function_call", "<tool_call", "<invoke"):
        for size in range(1, len(opener)):
            prefix = opener[:size]
            index = lowered.rfind(prefix)
            if index >= 0 and lowered[index:] == prefix:
                return text[:index].rstrip()
    return text


def parse_tool_calls(text: str) -> list[tuple[str, dict[str, object]]]:
    return [(name, args) for _call_id, name, args in parse_tool_calls_with_ids(text)]


def parse_tool_calls_with_ids(text: str) -> list[tuple[str, str, dict[str, object]]]:
    text = re.sub(r"(?is)```.*?```", "", text or "").strip()
    blocks = re.findall(r"(?is)<tool_call\b[^>]*>(.*?)</tool_call>|<function_call\b[^>]*>(.*?)</function_call>|<invoke\b[^>]*>(.*?)</invoke>", text)
    result = []
    for block in (next((part for part in match if part), "") for match in blocks):
        call_id = xml_value(block, "tool_use_id")
        name = xml_value(block, "tool_name") or xml_value(block, "name") or xml_value(block, "function")
        params = xml_value(block, "parameters") or xml_value(block, "input") or xml_value(block, "arguments") or "{}"
        if name:
            result.append((call_id, name, parse_tool_params(params)))
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
    terminal = False
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
        finish_reason = choice.get("finish_reason")
        if finish_reason:
            terminal = True
            if finish_reason == "max_tokens" and not parse_tool_calls_with_ids(current_text):
                current_text = _drop_incomplete_tool_markup(current_text)
            elif tool_mode and not parse_tool_calls_with_ids(current_text):
                current_text = _drop_incomplete_tool_markup(current_text)
            content, stop_reason = content_blocks(current_text, tools)
            if finish_reason == "max_tokens":
                stop_reason = "max_tokens"
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
    if not terminal:
        raise RuntimeError("upstream stream ended without terminal finish reason")
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
    input_tokens = count_message_tokens(request.messages, request.model)
    if request.search_result is not None:
        if body.get("stream"):
            return _stream_search_response(request.model, request.search_result, input_tokens, request.max_tokens, request.search_query or "", request.search_tool_use_id or "")
        return _search_response(request.model, request.search_result, input_tokens, request.max_tokens, request.search_query or "", request.search_tool_use_id or "")
    source = stream_text_chat_completion(request.backend, request.messages, request.model)
    bounded_source = (
        _MaxTokensStream(source, request.model, request.max_tokens)
        if request.max_tokens is not None
        else source
    )
    if body.get("stream"):
        return stream_events(
            bounded_source,
            request.model,
            input_tokens,
            lambda text: count_text_tokens(text, request.model),
            request.tools,
            request.backend,
        )
    try:
        text = collect_chat_content(bounded_source)
    finally:
        try:
            _close_iterator(bounded_source)
        finally:
            _close_iterator(request.backend)
    return message_response(
        request.model,
        text,
        input_tokens,
        count_text_tokens(text, request.model),
        request.tools,
        "max_tokens" if isinstance(bounded_source, _MaxTokensStream) and bounded_source.hit_max_tokens else None,
    )
