from __future__ import annotations

import hashlib
import json
import itertools
import os
import tempfile
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any
from uuid import uuid4

import anyio
from fastapi import HTTPException
from fastapi.responses import JSONResponse, StreamingResponse

from services.config import DATA_DIR
from services.protocol.error_response import (
    PUBLIC_SERVER_ERROR_MESSAGE,
    anthropic_error_response,
    exception_log_message,
    openai_error_response,
    project_public_responses_event,
    public_exception_message,
)
from utils.helper import anthropic_sse_stream, responses_sse_stream, sse_json_stream

LOG_TYPE_CALL = "call"
LOG_TYPE_ACCOUNT = "account"
INTERNAL_RESPONSE_KEYS = {"_account_email", "_conversation_id"}
_AI_THREAD_CAPACITY = 32
_AI_THREAD_STATE = threading.local()
_AI_STREAM_THREAD_CAPACITY = 64
_AI_STREAM_THREAD_STATE = threading.local()
_WS_AI_THREAD_CAPACITY = 64
_WS_AI_THREAD_STATE = threading.local()
_LOG_IO_THREAD_STATE = threading.local()
_LOG_MAX_BYTES = 16 * 1024 * 1024
_LOG_RETAIN_BYTES = 8 * 1024 * 1024
_LOG_READ_CHUNK_BYTES = 64 * 1024
_LOG_SECRET_KEYS = frozenset(
    {
        "access_token",
        "refresh_token",
        "id_token",
        "token",
        "password",
        "api_key",
        "secret_key",
        "authorization",
        "cookie",
        "user_agent",
        "email",
        "account_email",
        "_account_email",
    }
)
_LOG_ERROR_FALLBACK = "request failed"


def _sanitize_log_value(value: object, *, redact_error: bool = False) -> object:
    if isinstance(value, dict):
        sanitized: dict[str, object] = {}
        for key, item in value.items():
            normalized_key = str(key).casefold().replace("-", "_")
            if normalized_key in _LOG_SECRET_KEYS:
                continue
            if redact_error and normalized_key == "error":
                sanitized[key] = _LOG_ERROR_FALLBACK
            else:
                sanitized[key] = _sanitize_log_value(item, redact_error=redact_error)
        return sanitized
    if isinstance(value, list):
        return [_sanitize_log_value(item, redact_error=redact_error) for item in value]
    if isinstance(value, tuple):
        return tuple(_sanitize_log_value(item, redact_error=redact_error) for item in value)
    return value


class LogService:
    def __init__(
        self,
        path: Path,
        *,
        max_bytes: int = _LOG_MAX_BYTES,
        retain_bytes: int = _LOG_RETAIN_BYTES,
    ):
        if type(max_bytes) is not int or max_bytes <= 0:
            raise ValueError("max_bytes must be a positive integer")
        if type(retain_bytes) is not int or retain_bytes < 0 or retain_bytes > max_bytes:
            raise ValueError("retain_bytes must be between zero and max_bytes")
        self.path = path
        self.max_bytes = max_bytes
        self.retain_bytes = retain_bytes
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()

    @staticmethod
    def _legacy_id(raw_line: str, line_number: int) -> str:
        payload = f"{line_number}:{raw_line}".encode("utf-8", errors="ignore")
        return hashlib.sha1(payload).hexdigest()[:24]

    def _parse_line(self, raw_line: str, line_number: int) -> dict[str, Any] | None:
        try:
            item = json.loads(raw_line)
        except Exception:
            return None
        if not isinstance(item, dict):
            return None
        parsed = dict(item)
        if "detail" in parsed:
            parsed["detail"] = _sanitize_log_value(parsed["detail"], redact_error=True)
        parsed["id"] = str(parsed.get("id") or self._legacy_id(raw_line, line_number))
        return parsed

    @staticmethod
    def _serialize_item(item: dict[str, Any]) -> str:
        return json.dumps(item, ensure_ascii=False, separators=(",", ":"))

    def _serialized_item_bytes(self, item: dict[str, Any]) -> bytes:
        encoded = (self._serialize_item(item) + "\n").encode("utf-8")
        if len(encoded) <= self.max_bytes:
            return encoded
        fallback = {
            "id": item.get("id"),
            "time": item.get("time"),
            "type": str(item.get("type") or "")[:128],
            "summary": str(item.get("summary") or "")[:512],
            "detail": {"truncated": True},
        }
        return (self._serialize_item(fallback) + "\n").encode("utf-8")

    def _atomic_replace_bytes_locked(self, payload: bytes) -> None:
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as temp_file:
                temp_path = Path(temp_file.name)
                temp_file.write(payload)
                temp_file.flush()
                os.fsync(temp_file.fileno())
            os.replace(temp_path, self.path)
        except Exception:
            if temp_path is not None:
                try:
                    temp_path.unlink(missing_ok=True)
                except OSError:
                    pass
            raise

    def _compact_for_append_locked(self, incoming_size: int) -> None:
        if not self.path.exists():
            return
        current_size = self.path.stat().st_size
        if current_size + incoming_size <= self.max_bytes:
            return
        target_bytes = min(self.retain_bytes, max(0, self.max_bytes - incoming_size))
        if target_bytes <= 0:
            retained = b""
        else:
            with self.path.open("rb") as source:
                start = max(0, current_size - target_bytes)
                source.seek(start)
                if start:
                    source.readline()
                retained = source.read()
        self._atomic_replace_bytes_locked(retained)

    @staticmethod
    def _iter_lines_reverse(path: Path):
        with path.open("rb") as source:
            source.seek(0, os.SEEK_END)
            position = source.tell()
            remainder = b""
            while position > 0:
                read_size = min(_LOG_READ_CHUNK_BYTES, position)
                position -= read_size
                source.seek(position)
                block = source.read(read_size) + remainder
                parts = block.split(b"\n")
                remainder = parts[0]
                offset = position + len(parts[0]) + 1
                complete: list[tuple[int, bytes]] = []
                for raw_line in parts[1:]:
                    complete.append((offset, raw_line))
                    offset += len(raw_line) + 1
                for line_offset, raw_line in reversed(complete):
                    if raw_line:
                        yield line_offset, raw_line.decode("utf-8", errors="replace").rstrip("\r")
            if remainder:
                yield 0, remainder.decode("utf-8", errors="replace").rstrip("\r")

    @staticmethod
    def _matches_filters(item: dict[str, Any], *, type: str = "", start_date: str = "", end_date: str = "") -> bool:
        t = str(item.get("time") or "")
        day = t[:10]
        if type and item.get("type") != type:
            return False
        if start_date and day < start_date:
            return False
        if end_date and day > end_date:
            return False
        return True

    def add(self, type: str, summary: str = "", detail: dict[str, Any] | None = None, **data: Any) -> None:
        item = {
            "id": uuid4().hex,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "type": type,
            "summary": summary,
            "detail": _sanitize_log_value(detail or data),
        }
        with self._lock:
            encoded = self._serialized_item_bytes(item)
            self._compact_for_append_locked(len(encoded))
            with self.path.open("ab") as file:
                file.write(encoded)

    def list(self, type: str = "", start_date: str = "", end_date: str = "", limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            if not self.path.exists():
                return []
            items: list[dict[str, Any]] = []
            for line_offset, raw_line in self._iter_lines_reverse(self.path):
                item = self._parse_line(raw_line, line_offset)
                if item is None:
                    continue
                if not self._matches_filters(item, type=type, start_date=start_date, end_date=end_date):
                    continue
                items.append(item)
                if len(items) >= limit:
                    break
            return items

    def delete(self, ids: list[str]) -> dict[str, int]:
        target_ids = {str(item or "").strip() for item in ids if str(item or "").strip()}
        with self._lock:
            if not self.path.exists() or not target_ids:
                return {"removed": 0}
            kept_lines: list[str] = []
            removed = 0
            with self.path.open("rb") as source:
                line_offset = 0
                for raw_bytes in source:
                    raw_line = raw_bytes.decode("utf-8", errors="replace").rstrip("\r\n")
                    item = self._parse_line(raw_line, line_offset)
                    line_offset += len(raw_bytes)
                    if item is None:
                        kept_lines.append(raw_line)
                        continue
                    if str(item.get("id") or "") in target_ids:
                        removed += 1
                        continue
                    kept_lines.append(self._serialize_item(item))
            content = "\n".join(kept_lines)
            if content:
                content += "\n"
            self._atomic_replace_bytes_locked(content.encode("utf-8"))
            return {"removed": removed}


log_service = LogService(DATA_DIR / "logs.jsonl")


def _collect_urls(value: object) -> list[str]:
    urls: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "url" and isinstance(item, str):
                urls.append(item)
            elif key == "urls" and isinstance(item, list):
                urls.extend(str(url) for url in item if isinstance(url, str))
            else:
                urls.extend(_collect_urls(item))
    elif isinstance(value, list):
        for item in value:
            urls.extend(_collect_urls(item))
    return urls


def _collect_conversation_ids(value: object) -> list[str]:
    ids: list[str] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if key == "_conversation_id" and isinstance(item, str) and item.strip():
                ids.append(item.strip())
            else:
                ids.extend(_collect_conversation_ids(item))
    elif isinstance(value, list):
        for item in value:
            ids.extend(_collect_conversation_ids(item))
    return ids


def _strip_internal_response_fields(value: object) -> object:
    if isinstance(value, dict):
        return {
            key: _strip_internal_response_fields(item)
            for key, item in value.items()
            if key not in INTERNAL_RESPONSE_KEYS
        }
    if isinstance(value, list):
        return [_strip_internal_response_fields(item) for item in value]
    return value


def _request_excerpt(text: object, limit: int = 1000) -> str:
    value = str(text or "").strip()
    if not value:
        return ""
    normalized = " ".join(value.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "…"


def _image_error_response(exc: Exception) -> JSONResponse:
    from services.protocol.conversation import ImageGenerationError, public_image_error_message

    message = public_image_error_message(exc)
    if "no available image quota" in message.lower():
        return openai_error_response(
            {
                "error": {
                    "message": "no available image quota",
                    "type": "insufficient_quota",
                    "param": None,
                    "code": "insufficient_quota",
                }
            },
            429,
        )
    if isinstance(exc, ImageGenerationError):
        return JSONResponse(status_code=int(exc.status_code), content=exc.to_openai_error())
    return openai_error_response(message, 502)


def _exception_log_message(exc: BaseException) -> str:
    return exception_log_message(exc)


def _protocol_error_response(exc: Exception, status_code: int, sse: str) -> JSONResponse:
    message = public_exception_message(exc, PUBLIC_SERVER_ERROR_MESSAGE)
    if sse == "anthropic":
        return anthropic_error_response(message, status_code)
    return openai_error_response(message, status_code)


def _next_item(items):
    try:
        return True, next(items)
    except StopIteration:
        return False, None


def _close_iterators(*items: object) -> None:
    seen: set[int] = set()
    for item in items:
        marker = id(item)
        if marker in seen:
            continue
        seen.add(marker)
        close = getattr(item, "close", None)
        if not callable(close):
            continue
        try:
            close()
        except Exception:
            pass


def _ai_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_AI_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_AI_THREAD_CAPACITY)
        _AI_THREAD_STATE.limiter = limiter
    return limiter


def _ws_ai_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_WS_AI_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_WS_AI_THREAD_CAPACITY)
        _WS_AI_THREAD_STATE.limiter = limiter
    return limiter


def _ai_stream_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_AI_STREAM_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_AI_STREAM_THREAD_CAPACITY)
        _AI_STREAM_THREAD_STATE.limiter = limiter
    return limiter


async def _run_limited_threadpool(limiter: anyio.CapacityLimiter, func, *args):
    return await anyio.to_thread.run_sync(
        partial(func, *args),
        limiter=limiter,
    )


async def _run_ai_in_threadpool(func, *args):
    return await _run_limited_threadpool(_ai_thread_limiter(), func, *args)


async def _run_ws_ai_in_threadpool(func, *args):
    return await _run_limited_threadpool(_ws_ai_thread_limiter(), func, *args)


async def _run_ai_stream_in_threadpool(func, *args):
    return await _run_limited_threadpool(_ai_stream_thread_limiter(), func, *args)


def _log_io_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_LOG_IO_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(1)
        _LOG_IO_THREAD_STATE.limiter = limiter
    return limiter


async def run_log_in_threadpool(func, *args, **kwargs):
    return await anyio.to_thread.run_sync(
        partial(func, *args, **kwargs),
        limiter=_log_io_thread_limiter(),
    )


async def _iterate_sync_chunks(chunks, *source_iterators: object, runner):
    iterator = iter(chunks)
    try:
        while True:
            has_item, item = await runner(_next_item, iterator)
            if not has_item:
                return
            yield item
    finally:
        # Client disconnects close the async body iterator. Explicitly unwind
        # the synchronous protocol/SSE chain so backend sessions, account
        # slots, and cache owners are released without waiting for GC.
        with anyio.CancelScope(shield=True):
            await runner(
                _close_iterators,
                iterator,
                chunks,
                *source_iterators,
            )


async def _iterate_ai_chunks(chunks, *source_iterators: object):
    async for item in _iterate_sync_chunks(
        chunks,
        *source_iterators,
        runner=_run_ai_stream_in_threadpool,
    ):
        yield item


async def _iterate_ws_ai_chunks(chunks, *source_iterators: object):
    async for item in _iterate_sync_chunks(
        chunks,
        *source_iterators,
        runner=_run_ws_ai_in_threadpool,
    ):
        yield item


# Short AI calls, long-lived HTTP streams, and websocket upstream iterators use
# separate bounded capacities so one workload cannot starve the others.
run_ai_in_threadpool = _run_ai_in_threadpool
iterate_ai_chunks = _iterate_ai_chunks
run_ws_ai_in_threadpool = _run_ws_ai_in_threadpool
iterate_ws_ai_chunks = _iterate_ws_ai_chunks


@dataclass
class LoggedCall:
    identity: dict[str, object]
    endpoint: str
    model: str
    summary: str
    started: float = field(default_factory=time.time)
    request_text: str = ""
    request_shape: dict[str, int] | None = None

    async def run(self, handler, *args, sse: str = "openai"):
        from services.protocol.conversation import ImageGenerationError

        try:
            result = await _run_ai_in_threadpool(handler, *args)
        except ImageGenerationError as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc),
                                 conversation_id=getattr(exc, "conversation_id", ""))
            return _image_error_response(exc)
        except HTTPException as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc))
            raise
        except Exception as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc))
            if self.endpoint.startswith("/v1/images"):
                return _image_error_response(exc)
            return _protocol_error_response(exc, 502, sse)

        if isinstance(result, dict):
            await self.log_async("调用完成", result)
            response = dict(result)
            response.pop("_account_email", None)
            return response

        if sse == "anthropic":
            sender = anthropic_sse_stream
        elif sse in {"responses", "images"}:
            sender = responses_sse_stream
        else:
            sender = sse_json_stream
        try:
            has_first, first = await _run_ai_stream_in_threadpool(_next_item, result)
        except ImageGenerationError as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc),
                                 conversation_id=getattr(exc, "conversation_id", ""))
            return _image_error_response(exc)
        except HTTPException as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc))
            raise
        except Exception as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc))
            if self.endpoint.startswith("/v1/images"):
                return _image_error_response(exc)
            return _protocol_error_response(exc, 502, sse)
        if not has_first:
            await self.log_async("流式调用结束")
            return StreamingResponse(_iterate_ai_chunks(sender(())), media_type="text/event-stream")
        logged_items = self.stream(itertools.chain([first], result))
        chunks = sender(logged_items)
        return StreamingResponse(
            _iterate_ai_chunks(chunks, logged_items, result),
            media_type="text/event-stream",
        )

    def stream(self, items):
        urls: list[str] = []
        conversation_ids: list[str] = []
        failed = False
        try:
            for item in items:
                public_item = _strip_internal_response_fields(item)
                if self.endpoint == "/v1/responses":
                    public_item = project_public_responses_event(public_item, model=self.model)
                urls.extend(_collect_urls(public_item))
                conversation_ids.extend(_collect_conversation_ids(public_item))
                if (
                    self.endpoint == "/v1/responses"
                    and isinstance(public_item, dict)
                    and public_item.get("type") in {"response.failed", "error"}
                ):
                    failed = True
                    self.log(
                        "流式调用失败",
                        status="failed",
                        error="upstream response failed",
                        urls=urls,
                        conversation_id=(conversation_ids[0] if conversation_ids else ""),
                    )
                    yield public_item
                    return
                yield public_item
        except Exception as exc:
            failed = True
            self.log(
                "流式调用失败",
                status="failed",
                error=_exception_log_message(exc),
                urls=urls,
                conversation_id=(conversation_ids[0] if conversation_ids else getattr(exc, "conversation_id", "")),
            )
            if self.endpoint.startswith("/v1/images"):
                from services.protocol.conversation import ImageGenerationError, public_image_error_message

                if not isinstance(exc, ImageGenerationError):
                    raise ImageGenerationError(public_image_error_message(exc)) from exc
            raise
        finally:
            if not failed:
                self.log("流式调用结束", urls=urls,
                         conversation_id=conversation_ids[0] if conversation_ids else "")

    def log(self, suffix: str, result: object = None, status: str = "success", error: str = "",
            urls: list[str] | None = None, conversation_id: str = "") -> None:
        detail = {
            "key_id": self.identity.get("id"),
            "key_name": self.identity.get("name"),
            "role": self.identity.get("role"),
            "endpoint": self.endpoint,
            "model": self.model,
            "started_at": datetime.fromtimestamp(self.started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "duration_ms": int((time.time() - self.started) * 1000),
            "status": status,
        }
        request_excerpt = _request_excerpt(self.request_text)
        if request_excerpt:
            detail["request_text"] = request_excerpt
        if self.request_shape:
            detail["request_shape"] = self.request_shape
        if error:
            detail["error"] = error
        conv_id = str(conversation_id or "").strip()
        if not conv_id:
            conv_ids = _collect_conversation_ids(result)
            conv_id = conv_ids[0] if conv_ids else ""
        if conv_id:
            detail["conversation_id"] = conv_id
        collected_urls = [*(urls or []), *_collect_urls(result)]
        if collected_urls and not self.endpoint.startswith("/v1/search"):
            detail["urls"] = list(dict.fromkeys(collected_urls))
        log_service.add(LOG_TYPE_CALL, f"{self.summary}{suffix}", detail)

    async def log_async(self, suffix: str, result: object = None, status: str = "success", error: str = "",
                        urls: list[str] | None = None, conversation_id: str = "") -> None:
        await run_log_in_threadpool(
            self.log,
            suffix,
            result,
            status,
            error,
            urls,
            conversation_id,
        )
