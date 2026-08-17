from __future__ import annotations

import hashlib
import json
import itertools
import logging
import os
import re
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
import stat
from typing import Any
from uuid import uuid4
from urllib.parse import urlsplit, urlunsplit

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
from services.secure_file import atomic_write_bytes, append_checked_file_bytes, open_checked_file
from services.storage.base import canonical_path_write_lock
from utils.helper import anthropic_sse_stream, responses_sse_stream, sse_json_stream

_LOGGER = logging.getLogger(__name__)

LOG_TYPE_CALL = "call"
LOG_TYPE_ACCOUNT = "account"
INTERNAL_RESPONSE_KEYS = {
    "_account_email",
    "_conversation_id",
    "account_email",
    "access_token",
    "refresh_token",
    "id_token",
    "password",
    "api_key",
    "secret_key",
}


def _assert_log_path_unchanged(path: Path, opened: object) -> None:
    opened_stat = getattr(opened, "stat_result", None)
    if opened_stat is None:
        raise OSError("log file handle metadata unavailable")
    try:
        current_stat = os.stat(path, follow_symlinks=False)
    except OSError as exc:
        raise OSError("log file changed during read") from exc
    if (
        not stat.S_ISREG(current_stat.st_mode)
        or current_stat.st_dev != opened_stat.st_dev
        or current_stat.st_ino != opened_stat.st_ino
    ):
        raise OSError("log file changed during read")
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
        "old_token",
        "new_token",
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
_LOG_DETAIL_FIELDS = frozenset(
    {
        "source",
        "status",
        "rotated",
        "added",
        "skipped",
        "removed",
        "reason",
        "key_id",
        "key_name",
        "role",
        "endpoint",
        "model",
        "started_at",
        "ended_at",
        "duration_ms",
        "request_text",
        "request_shape",
        "error",
        "conversation_id",
        "urls",
    }
)
_LOG_REQUEST_SHAPE_FIELDS = frozenset(
    {
        "response_message_items",
        "input_image_parts",
        "image_url_parts",
        "image_parts",
        "data_url_images",
        "remote_image_urls",
        "literal_image_placeholders",
    }
)
_ANSI_ESCAPE_RE = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07]*(?:\x07|\x1b\\))")
_REQUEST_URL_RE = re.compile(r"https?://[^\s\"'<>]+", re.IGNORECASE)
_REQUEST_DATA_URI_RE = re.compile(r"(?i)\bdata:[^\s,]+,[^\s]+")
_REQUEST_WINDOWS_PATH_RE = re.compile(r"(?<![\w])[A-Za-z]:\\[^\s]+")
_REQUEST_POSIX_PATH_RE = re.compile(r"(?<![\w])/(?:Users|home|tmp|var|opt|etc|private|mnt)/[^\s]+", re.IGNORECASE)
_REQUEST_BEARER_RE = re.compile(r"(?i)(?:\bauthorization\s*:\s*)?\bbearer\s+[^\s,;]+")
_REQUEST_SECRET_RE = re.compile(
    r"(?i)\b(?:access_token|refresh_token|id_token|api_key|secret_key|password|cookie|token)\s*(?:=|:)\s*[^\s,;]+"
)


def _bounded_nonnegative_int(value: object) -> int | None:
    if type(value) is not int or value < 0 or value > 1_000_000_000:
        return None
    return value


def _sanitize_request_shape(value: object) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    return {
        key: parsed
        for key, item in value.items()
        if isinstance(key, str)
        and key in _LOG_REQUEST_SHAPE_FIELDS
        and (parsed := _bounded_nonnegative_int(item)) is not None
    }


def _sanitize_log_url(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    text = value.strip()
    try:
        parsed = urlsplit(text)
        if parsed.scheme:
            if (
                parsed.scheme.lower() not in {"http", "https"}
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
            ):
                return ""
            hostname = parsed.hostname
            if not hostname:
                return ""
            if ":" in hostname:
                hostname = f"[{hostname}]"
            port = parsed.port
            authority = f"{hostname}:{port}" if port is not None else hostname
            return urlunsplit((parsed.scheme.lower(), authority, parsed.path, "", ""))
        if text.startswith("/"):
            return text.split("?", 1)[0].split("#", 1)[0]
    except ValueError:
        return ""
    return ""


def _sanitize_log_detail(value: object) -> dict[str, object]:
    if not isinstance(value, dict):
        return {}
    sanitized: dict[str, object] = {}
    for key, item in value.items():
        if not isinstance(key, str) or key not in _LOG_DETAIL_FIELDS:
            continue
        normalized_key = key.casefold().replace("-", "_")
        if normalized_key in _LOG_SECRET_KEYS:
            continue
        if normalized_key == "error":
            sanitized[key] = _LOG_ERROR_FALLBACK
        elif key == "request_shape":
            shape = _sanitize_request_shape(item)
            if shape:
                sanitized[key] = shape
        elif key == "request_text":
            excerpt = _request_excerpt(item, limit=4096)
            if excerpt:
                sanitized[key] = excerpt
        elif key == "urls":
            if isinstance(item, list):
                urls = [
                    safe_url[:2048]
                    for url in item
                    if (safe_url := _sanitize_log_url(url))
                ]
                if urls:
                    sanitized[key] = urls[:100]
        elif key in {"added", "skipped", "removed", "duration_ms"}:
            parsed = _bounded_nonnegative_int(item)
            if parsed is not None:
                sanitized[key] = parsed
        elif key == "rotated":
            if type(item) is bool:
                sanitized[key] = item
        elif isinstance(item, str):
            sanitized[key] = item[:4096]
    return sanitized


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
        parent_stat = self.path.parent.stat()
        self._parent_identity = (parent_stat.st_dev, parent_stat.st_ino)
        self._path_lock = canonical_path_write_lock(self.path.absolute())
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
        parsed: dict[str, Any] = {}
        for key in ("time", "type", "summary"):
            value = item.get(key)
            if isinstance(value, str):
                parsed[key] = value
        if "detail" in item:
            parsed["detail"] = _sanitize_log_detail(item["detail"])
        raw_id = item.get("id")
        parsed["id"] = (
            raw_id
            if isinstance(raw_id, str) and raw_id.strip() and len(raw_id) <= 256
            else self._legacy_id(raw_line, line_number)
        )
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
        encoded = (self._serialize_item(fallback) + "\n").encode("utf-8")
        if len(encoded) <= self.max_bytes:
            return encoded

        # The configured budget can be smaller than the fixed diagnostic
        # fallback. Keep the hard file-size contract instead of appending an
        # oversized record; the fixed payload also avoids carrying caller text.
        minimal = (self._serialize_item({"type": "log", "summary": "entry truncated"}) + "\n").encode(
            "utf-8"
        )
        if len(minimal) <= self.max_bytes:
            return minimal
        return b"{}\n" if self.max_bytes >= 3 else b""

    def _atomic_replace_bytes_locked(self, payload: bytes) -> None:
        parent_stat = self.path.parent.stat()
        if (parent_stat.st_dev, parent_stat.st_ino) != self._parent_identity:
            raise OSError("log directory changed")
        atomic_write_bytes(
            self.path,
            self.path.parent,
            payload,
            mode=0o600,
            expected_root_identity=self._parent_identity,
        )

    def _open_log_file_locked(
        self,
        expected_file_identity: tuple[int, int] | None = None,
    ):
        opened = open_checked_file(self.path, self.path.parent, self.path.parent)
        try:
            parent_stat = self.path.parent.stat()
        except Exception:
            opened.file.close()
            raise
        if (parent_stat.st_dev, parent_stat.st_ino) != self._parent_identity:
            opened.file.close()
            raise OSError("log directory changed")
        if expected_file_identity is not None and (
            opened.stat_result.st_dev,
            opened.stat_result.st_ino,
        ) != expected_file_identity:
            opened.file.close()
            raise OSError("log file changed")
        return opened

    def _stat_log_file_locked(self):
        current_stat = os.stat(self.path, follow_symlinks=False)
        if not stat.S_ISREG(current_stat.st_mode) or not current_stat.st_ino:
            raise OSError("log file is not a regular file")
        return current_stat

    def _assert_current_log_identity_locked(self, expected_file_identity: tuple[int, int]) -> None:
        try:
            current_stat = os.stat(self.path, follow_symlinks=False)
        except OSError as exc:
            raise OSError("log file changed") from exc
        if (
            not stat.S_ISREG(current_stat.st_mode)
            or (current_stat.st_dev, current_stat.st_ino) != expected_file_identity
        ):
            raise OSError("log file changed")

    def _compact_for_append_locked(self, incoming_size: int) -> bytes | None:
        if not self.path.exists():
            return None
        current_stat = self._stat_log_file_locked()
        current_size = current_stat.st_size
        if current_size + incoming_size <= self.max_bytes:
            return None
        target_bytes = min(self.retain_bytes, max(0, self.max_bytes - incoming_size))
        expected_file_identity = (current_stat.st_dev, current_stat.st_ino)
        opened = self._open_log_file_locked(expected_file_identity)
        if target_bytes <= 0:
            retained = b""
        else:
            try:
                source = opened.file
                start = max(0, current_size - target_bytes)
                source.seek(start)
                if start:
                    source.readline()
                retained = source.read()
            finally:
                opened.file.close()
        if not opened.file.closed:
            opened.file.close()
        _assert_log_path_unchanged(self.path, opened)
        self._assert_current_log_identity_locked(expected_file_identity)
        return retained

    def _iter_lines_reverse(self, expected_file_identity: tuple[int, int]):
        opened = self._open_log_file_locked(expected_file_identity)
        try:
            source = opened.file
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
        finally:
            opened.file.close()

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
            "detail": _sanitize_log_detail(detail or data),
        }
        with self._lock, self._path_lock:
            encoded = self._serialized_item_bytes(item)
            retained = self._compact_for_append_locked(len(encoded))
            if retained is not None:
                self._atomic_replace_bytes_locked(retained + encoded)
            else:
                append_checked_file_bytes(
                    self.path,
                    self.path.parent,
                    encoded,
                    expected_root_identity=self._parent_identity,
                )

    def list(self, type: str = "", start_date: str = "", end_date: str = "", limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            if not self.path.exists():
                return []
            current_stat = self._stat_log_file_locked()
            items: list[dict[str, Any]] = []
            for line_offset, raw_line in self._iter_lines_reverse(
                (current_stat.st_dev, current_stat.st_ino)
            ):
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
        with self._lock, self._path_lock:
            if not self.path.exists() or not target_ids:
                return {"removed": 0}
            kept_lines: list[str] = []
            removed = 0
            current_stat = self._stat_log_file_locked()
            opened = self._open_log_file_locked((current_stat.st_dev, current_stat.st_ino))
            try:
                source = opened.file
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
            finally:
                opened.file.close()
            _assert_log_path_unchanged(self.path, opened)
            self._assert_current_log_identity_locked((current_stat.st_dev, current_stat.st_ino))
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
    if not isinstance(text, str):
        return ""
    value = _ANSI_ESCAPE_RE.sub("", text)
    value = "".join(char if char in "\t\n\r" or ord(char) >= 0x20 else " " for char in value)
    value = _REQUEST_DATA_URI_RE.sub("[image data redacted]", value)
    value = _REQUEST_URL_RE.sub(
        lambda match: _sanitize_log_url(match.group(0).rstrip(".,;:)]")),
        value,
    )
    value = _REQUEST_WINDOWS_PATH_RE.sub("[path redacted]", value)
    value = _REQUEST_POSIX_PATH_RE.sub("[path redacted]", value)
    value = _REQUEST_BEARER_RE.sub("Bearer [redacted]", value)
    value = _REQUEST_SECRET_RE.sub(lambda match: f"{match.group(0).split('=', 1)[0].split(':', 1)[0]}=[redacted]", value)
    value = value.strip()
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


async def _run_limited_threadpool(limiter: anyio.CapacityLimiter, func, *args, **kwargs):
    return await anyio.to_thread.run_sync(
        partial(func, *args, **kwargs),
        limiter=limiter,
    )


async def _run_ai_in_threadpool(func, *args, **kwargs):
    return await _run_limited_threadpool(_ai_thread_limiter(), func, *args, **kwargs)


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
    iterator = None
    try:
        iterator = iter(chunks)
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


class _ClosableAIStream:
    """Async response body that can close prefetched sources before first read."""

    def __init__(self, chunks, *source_iterators: object) -> None:
        self._chunks = chunks
        self._source_iterators = source_iterators
        self._iterator = None
        self._closed = False
        self._finished = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._closed:
            raise StopAsyncIteration
        if self._iterator is None:
            self._iterator = _iterate_ai_chunks(self._chunks, *self._source_iterators)
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            self._finished = True
            self._closed = True
            raise

    async def aclose(self) -> None:
        if self._closed:
            if self._finished or self._iterator is None:
                return
            await self._iterator.aclose()
            return
        self._closed = True
        if self._iterator is not None:
            await self._iterator.aclose()
            return
        with anyio.CancelScope(shield=True):
            await _run_ai_stream_in_threadpool(
                _close_iterators,
                self._chunks,
                *self._source_iterators,
            )


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

    async def run(self, handler, *args, sse: str = "openai", **handler_kwargs):
        from services.protocol.conversation import ImageGenerationError
        from services.model_service import ModelUnavailableError

        try:
            result = await _run_ai_in_threadpool(handler, *args, **handler_kwargs)
        except ImageGenerationError as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc),
                                 conversation_id=getattr(exc, "conversation_id", ""))
            return _image_error_response(exc)
        except ModelUnavailableError as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc))
            return _protocol_error_response(exc, exc.status_code, sse)
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
            return _strip_internal_response_fields(result)

        if sse == "anthropic":
            sender = anthropic_sse_stream
        elif sse in {"responses", "images"}:
            sender = responses_sse_stream
        else:
            sender = sse_json_stream
        stream_transferred = False
        try:
            has_first, first = await _run_ai_stream_in_threadpool(_next_item, result)
            if not has_first:
                await self.log_async("流式调用结束")
                return StreamingResponse(_iterate_ai_chunks(sender(())), media_type="text/event-stream")
            logged_items = self.stream(itertools.chain([first], result))
            chunks = sender(logged_items)
            response = StreamingResponse(
                _ClosableAIStream(chunks, logged_items, result),
                media_type="text/event-stream",
            )
            stream_transferred = True
            return response
        except ImageGenerationError as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc),
                                 conversation_id=getattr(exc, "conversation_id", ""))
            return _image_error_response(exc)
        except ModelUnavailableError as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc))
            return _protocol_error_response(exc, exc.status_code, sse)
        except HTTPException as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc))
            raise
        except Exception as exc:
            await self.log_async("调用失败", status="failed", error=_exception_log_message(exc))
            if self.endpoint.startswith("/v1/images"):
                return _image_error_response(exc)
            return _protocol_error_response(exc, 502, sse)
        finally:
            if not stream_transferred:
                await _run_ai_stream_in_threadpool(_close_iterators, result)

    def stream(self, items):
        urls: list[str] = []
        conversation_ids: list[str] = []
        failed = False
        try:
            for item in items:
                public_item = _strip_internal_response_fields(item)
                if self.endpoint == "/v1/responses" and self.summary == "Responses":
                    from services.protocol.openai_v1_response import project_public_codex_response_event

                    public_item = project_public_codex_response_event(public_item)
                    if public_item is None:
                        continue
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
        try:
            log_service.add(LOG_TYPE_CALL, f"{self.summary}{suffix}", detail)
        except Exception as exc:
            # Logging is an observability side effect. A full disk or a
            # transient log-file error must not turn a completed upstream
            # request into a failed API response or interrupt an active SSE.
            try:
                _LOGGER.error(
                    "log persistence failed",
                    extra={"error_type": type(exc).__name__},
                )
            except Exception:
                # The fallback logger is also a side effect; it must never
                # replace the business result or the original stream error.
                pass

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
