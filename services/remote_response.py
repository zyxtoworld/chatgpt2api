"""Bounded response-body helpers for small third-party JSON APIs."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


DEFAULT_REMOTE_JSON_BYTES = 16 * 1024 * 1024
REMOTE_JSON_CHUNK_BYTES = 64 * 1024


class _RemoteResponseReadError(RuntimeError):
    pass


def close_response(response: object) -> None:
    close = getattr(response, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _validate_max_bytes(max_bytes: int) -> int:
    if type(max_bytes) is not int or max_bytes <= 0:
        raise RuntimeError("response body too large")
    return max_bytes


def _declared_content_length(response: object) -> int | None:
    headers = getattr(response, "headers", None)
    if not isinstance(headers, Mapping):
        return None
    raw = headers.get("content-length")
    if raw is None:
        raw = headers.get("Content-Length")
    if raw is None:
        return None
    if type(raw) is int:
        return raw if raw >= 0 else -1
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("ascii")
        except UnicodeDecodeError:
            return -1
    if not isinstance(raw, str):
        return -1
    text = raw.strip()
    if not text or not text.isascii() or not text.isdecimal():
        return -1
    try:
        return int(text)
    except (ValueError, OverflowError):
        return -1


def _read_response_bytes(response: object, *, max_bytes: int) -> bytes:
    max_bytes = _validate_max_bytes(max_bytes)
    declared_length = _declared_content_length(response)
    if declared_length is not None:
        if declared_length < 0:
            raise _RemoteResponseReadError("invalid response body")
        if declared_length > max_bytes:
            raise _RemoteResponseReadError("response body too large")

    iterator = getattr(response, "iter_content", None)
    if callable(iterator):
        payload = bytearray()
        try:
            chunks = iterator(chunk_size=REMOTE_JSON_CHUNK_BYTES)
            for chunk in chunks:
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise _RemoteResponseReadError("invalid response body")
                chunk_length = len(chunk)
                if len(payload) + chunk_length > max_bytes:
                    raise _RemoteResponseReadError("response body too large")
                payload.extend(chunk)
        except _RemoteResponseReadError:
            raise
        except Exception as exc:
            raise RuntimeError("invalid response body") from exc
        return bytes(payload)

    # A response without a streaming iterator may still expose a bounded
    # bytes/text adapter.
    content = getattr(response, "content", None)
    if isinstance(content, (bytes, bytearray, memoryview)):
        if len(content) > max_bytes:
            raise _RemoteResponseReadError("response body too large")
        return bytes(content)

    text = getattr(response, "text", None)
    if isinstance(text, str):
        try:
            encoded = text.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise _RemoteResponseReadError("invalid response body") from exc
        if len(encoded) > max_bytes:
            raise _RemoteResponseReadError("response body too large")
        return encoded
    raise _RemoteResponseReadError("invalid response body")


def read_bounded_text(
    response: object,
    operation: str,
    *,
    max_bytes: int,
    require_ok: bool = True,
    close: bool = True,
) -> str:
    """Read a small streamed text body and always close its response."""
    try:
        if require_ok and not getattr(response, "ok", False):
            raise RuntimeError(f"HTTP {getattr(response, 'status_code', 0)}")
        try:
            return _read_response_bytes(response, max_bytes=max_bytes).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise RuntimeError("invalid response text") from exc
    finally:
        if close:
            close_response(response)


def parse_json_response(
    response: object,
    operation: str,
    *,
    max_bytes: int = DEFAULT_REMOTE_JSON_BYTES,
    require_ok: bool = True,
    close: bool = True,
) -> Any:
    """Read one remote JSON response with an explicit body budget."""
    try:
        if require_ok and not getattr(response, "ok", False):
            raise RuntimeError(f"HTTP {getattr(response, 'status_code', 0)}")
        try:
            text = _read_response_bytes(response, max_bytes=max_bytes).decode("utf-8")
            return json.loads(text)
        except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, MemoryError) as exc:
            raise RuntimeError("invalid JSON") from exc
    finally:
        if close:
            close_response(response)
