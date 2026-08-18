from __future__ import annotations

import copy
import hashlib
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Iterator

from services.config import config

@dataclass
class CacheEntry:
    expires_at: float
    value: Any


@dataclass
class InflightCall:
    condition: threading.Condition = field(default_factory=lambda: threading.Condition(threading.RLock()))
    done: bool = False
    value: Any = None
    error: BaseException | None = None


class _CacheInvalidatedError(RuntimeError):
    pass


def _json_safe(value: Any) -> Any:
    if isinstance(value, bytes):
        return {"__bytes_sha256__": hashlib.sha256(value).hexdigest(), "length": len(value)}
    if isinstance(value, bytearray):
        data = bytes(value)
        return {"__bytes_sha256__": hashlib.sha256(data).hexdigest(), "length": len(data)}
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


def canonical_body(body: dict[str, Any], messages: list[dict[str, Any]], *, stream: bool) -> dict[str, Any]:
    payload = dict(body)
    payload["messages"] = messages
    payload["stream"] = bool(stream)
    return payload


def cache_key(
    body: dict[str, Any],
    messages: list[dict[str, Any]],
    *,
    stream: bool,
    cache_scope: str = "",
) -> str:
    encoded = json.dumps(
        _json_safe(canonical_body(body, messages, stream=stream)),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if cache_scope:
        encoded = encoded + b"\x00" + cache_scope.encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def resolve_access_token_cache_scope(
    cache_scope: str,
    access_token: str,
    *,
    authenticated: bool = False,
    account_generation: str = "",
) -> str:
    """Bind a request cache to the selected account generation."""
    if not cache_scope and not authenticated:
        return ""
    token_digest = hashlib.sha256(str(access_token or "<anonymous>").encode("utf-8")).hexdigest()
    owner_scope = cache_scope or "authenticated"
    if account_generation:
        owner_scope = f"{owner_scope}\x00{account_generation}"
    return f"{owner_scope}\x00account:{token_digest}"


def _message_signature(message: dict[str, Any]) -> str:
    return json.dumps(_json_safe(message), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def normalize_text_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    settings = config.get_chat_completion_cache_settings()
    if not settings.get("normalize_messages"):
        return messages

    normalized: list[dict[str, Any]] = []
    previous_signature = ""
    for message in messages:
        if settings.get("drop_assistant_history") and str(message.get("role") or "") == "assistant":
            continue
        signature = _message_signature(message)
        if settings.get("drop_adjacent_duplicates") and signature == previous_signature:
            continue
        normalized.append(message)
        previous_signature = signature
    return normalized


class ChatCompletionCache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, CacheEntry] = {}
        self._inflight: dict[str, InflightCall] = {}
        self._generation = 0

    def clear(self) -> None:
        with self._lock:
            self._generation += 1
            self._entries.clear()
            inflight_calls = list(self._inflight.values())
            self._inflight.clear()
        for inflight in inflight_calls:
            with inflight.condition:
                if inflight.done:
                    continue
                inflight.error = _CacheInvalidatedError("cache fill invalidated")
                inflight.done = True
                inflight.condition.notify_all()

    def _settings(self) -> dict[str, object]:
        return config.get_chat_completion_cache_settings()

    def _prune_locked(self, now: float, max_entries: int) -> None:
        expired = [key for key, item in self._entries.items() if item.expires_at <= now]
        for key in expired:
            self._entries.pop(key, None)
        while len(self._entries) > max_entries:
            oldest_key = min(self._entries, key=lambda key: self._entries[key].expires_at)
            self._entries.pop(oldest_key, None)

    @staticmethod
    def _copy(value: Any) -> Any:
        return copy.deepcopy(value)

    @staticmethod
    def _finish_inflight(
        inflight: InflightCall,
        *,
        value: Any = None,
        error: BaseException | None = None,
    ) -> None:
        with inflight.condition:
            if inflight.done:
                return
            inflight.value = value
            inflight.error = error
            inflight.done = True
            inflight.condition.notify_all()

    def get_or_compute_response(
        self,
        key: str,
        compute: Callable[[], dict[str, Any]],
        replay: Callable[[dict[str, Any]], dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        settings = self._settings()
        if not settings.get("enabled") or int(settings.get("ttl_seconds") or 0) <= 0:
            return compute()

        now = time.time()
        max_entries = int(settings.get("max_entries") or 1)
        with self._lock:
            self._prune_locked(now, max_entries)
            entry = self._entries.get(key)
            if entry and entry.expires_at > now:
                value = self._copy(entry.value)
                return replay(value) if replay is not None else value
            inflight = self._inflight.get(key) if settings.get("dedupe_inflight") else None
            if inflight is None:
                inflight = InflightCall()
                if settings.get("dedupe_inflight"):
                    self._inflight[key] = inflight
                owner = True
            else:
                owner = False
            generation = self._generation

        if not owner:
            with inflight.condition:
                while not inflight.done:
                    inflight.condition.wait()
                if inflight.error:
                    raise inflight.error
                value = self._copy(inflight.value)
                return replay(value) if replay is not None else value

        try:
            value = compute()
        except BaseException as exc:
            with self._lock:
                if self._inflight.get(key) is inflight:
                    self._inflight.pop(key, None)
            self._finish_inflight(inflight, error=exc)
            raise

        with self._lock:
            if self._generation == generation:
                expires_at = time.time() + int(settings.get("ttl_seconds") or 0)
                self._entries[key] = CacheEntry(expires_at=expires_at, value=self._copy(value))
                self._prune_locked(time.time(), max_entries)
            if self._inflight.get(key) is inflight:
                self._inflight.pop(key, None)
        self._finish_inflight(inflight, value=self._copy(value))
        return value

    def get_or_compute_stream(
        self,
        key: str,
        compute: Callable[[], Iterable[dict[str, Any]]],
        replay: Callable[[list[dict[str, Any]]], Iterable[dict[str, Any]]] | None = None,
    ) -> Iterator[dict[str, Any]]:
        settings = self._settings()
        if (
            not settings.get("enabled")
            or not settings.get("stream_cache")
            or int(settings.get("ttl_seconds") or 0) <= 0
        ):
            yield from compute()
            return

        now = time.time()
        max_entries = int(settings.get("max_entries") or 1)
        with self._lock:
            self._prune_locked(now, max_entries)
            entry = self._entries.get(key)
            if entry and entry.expires_at > now:
                value = self._copy(entry.value)
                yield from (replay(value) if replay is not None else value)
                return
            inflight = self._inflight.get(key) if settings.get("dedupe_inflight") else None
            if inflight is None:
                inflight = InflightCall()
                if settings.get("dedupe_inflight"):
                    self._inflight[key] = inflight
                owner = True
            else:
                owner = False
            generation = self._generation

        if not owner:
            # A streaming owner must reacquire the bounded AI worker for every
            # chunk. Waiting followers would hold those same workers until the
            # owner completes and can therefore starve it permanently. Let
            # concurrent followers compute independently; only the first
            # request writes this cache entry, and later requests still replay
            # its completed stream.
            yield from compute()
            return

        chunks: list[dict[str, Any]] = []
        source = None
        try:
            source = iter(compute())
            for chunk in source:
                chunks.append(self._copy(chunk))
                yield chunk
        except BaseException as exc:
            with self._lock:
                if self._inflight.get(key) is inflight:
                    self._inflight.pop(key, None)
            self._finish_inflight(inflight, error=exc)
            raise
        finally:
            if source is not None:
                close = getattr(source, "close", None)
                if callable(close):
                    try:
                        close()
                    except Exception:
                        pass

        with self._lock:
            if self._generation == generation:
                expires_at = time.time() + int(settings.get("ttl_seconds") or 0)
                self._entries[key] = CacheEntry(expires_at=expires_at, value=self._copy(chunks))
                self._prune_locked(time.time(), max_entries)
            if self._inflight.get(key) is inflight:
                self._inflight.pop(key, None)
        self._finish_inflight(inflight, value=self._copy(chunks))


chat_completion_cache = ChatCompletionCache()
