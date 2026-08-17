from __future__ import annotations

import time
from collections.abc import Callable
from copy import deepcopy
from concurrent.futures import FIRST_COMPLETED, TimeoutError as FutureTimeoutError
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from functools import partial
from threading import BoundedSemaphore, Event, RLock, local
from typing import Any

import anyio

from services.account_service import AccountService, account_service
from services.model_contract import parse_model_text
from services.openai_backend_api import OpenAIBackendAPI
from services.protocol.error_response import PublicSafeErrorMarker
from services.protocol.reasoning_effort import (
    canonical_conversation_effort,
    fallback_conversation_effort,
    strongest_conversation_effort,
)
from utils.log import logger


_MODEL_IO_THREAD_CAPACITY = 4
_MODEL_IO_THREAD_STATE = local()
MODEL_CATALOG_REFRESH_TIMEOUT_SECS = 90.0
MODEL_CATALOG_REFRESH_WORKERS = 4
_MODEL_CATALOG_REFRESH_EXECUTOR = ThreadPoolExecutor(
    max_workers=MODEL_CATALOG_REFRESH_WORKERS,
    thread_name_prefix="model-catalog",
)
_MODEL_CATALOG_REFRESH_SLOTS = BoundedSemaphore(MODEL_CATALOG_REFRESH_WORKERS)


def _model_io_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_MODEL_IO_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_MODEL_IO_THREAD_CAPACITY)
        _MODEL_IO_THREAD_STATE.limiter = limiter
    return limiter


async def run_model_catalog_in_threadpool(func, *args):
    return await anyio.to_thread.run_sync(
        partial(func, *args),
        limiter=_model_io_thread_limiter(),
    )


@dataclass(frozen=True)
class ModelRoute:
    access_tokens: frozenset[str]
    allow_anonymous: bool = False


class ModelUnavailableError(RuntimeError, PublicSafeErrorMarker):
    status_code = 400

    def public_safe_message(self) -> str:
        return "The requested model is not available."


class ModelCatalogRefreshTimeout(TimeoutError):
    pass


class ModelCatalogService:
    """Caches model catalogs owned by each active access token."""

    def __init__(
        self,
        accounts: AccountService,
        *,
        backend_factory: Callable[..., Any] = OpenAIBackendAPI,
        cache_ttl_seconds: float = 300,
        clock: Callable[[], float] = time.monotonic,
        deadline_clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._accounts = accounts
        self._backend_factory = backend_factory
        self._cache_ttl_seconds = max(1.0, float(cache_ttl_seconds))
        self._clock = clock
        self._deadline_clock = deadline_clock
        self._lock = RLock()
        self._expires_at = 0.0
        self._account_signature: tuple[tuple[str, tuple[str, ...]], ...] = ()
        self._anonymous_models: dict[str, dict[str, Any]] = {}
        self._models_by_access_token: dict[str, dict[str, dict[str, Any]]] = {}
        self._account_type_by_access_token: dict[str, str] = {}
        self._catalog_loaded = False
        self._refresh_in_progress = False
        self._refresh_done = Event()
        self._refresh_done.set()

    @staticmethod
    def _model_map(result: object) -> dict[str, dict[str, Any]]:
        if not isinstance(result, dict) or not isinstance(result.get("data"), list):
            raise TypeError("upstream model response has no data list")
        models: dict[str, dict[str, Any]] = {}
        for item in result["data"]:
            if not isinstance(item, dict):
                continue
            model_id = parse_model_text(item.get("id"))
            if model_id and model_id not in models:
                models[model_id] = deepcopy(item)
        return models

    def _active_accounts_by_type(self) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = {}
        for account in self._accounts.list_accounts():
            if not isinstance(account, dict) or not self._accounts._is_text_account_available(account):
                continue
            access_token = str(account.get("access_token") or "").strip()
            account_type = self._accounts._normalize_account_type(account.get("type"))
            if access_token and account_type:
                groups.setdefault(account_type, []).append(access_token)
        return groups

    @staticmethod
    def _signature(groups: dict[str, list[str]]) -> tuple[tuple[str, tuple[str, ...]], ...]:
        return tuple(
            (account_type, tuple(sorted(tokens)))
            for account_type, tokens in sorted(groups.items())
        )

    def _fetch_models(
        self,
        access_token: str = "",
        *,
        deadline: float | None = None,
    ) -> dict[str, dict[str, Any]]:
        backend = self._backend_factory(access_token=access_token)
        try:
            return self._model_map(backend.list_models(deadline=deadline))
        finally:
            backend.close()

    def _fetch_account_models(
        self,
        account_type: str,
        access_token: str,
        deadline: float,
    ) -> tuple[str, dict[str, dict[str, Any]] | None]:
        resolved_token = access_token
        try:
            resolved_token = self._accounts.refresh_access_token(
                access_token,
                event="list_models",
                deadline=deadline,
            ) or access_token
            if self._deadline_clock() >= deadline:
                raise TimeoutError("model catalog refresh timed out")
            return resolved_token, self._fetch_models(resolved_token, deadline=deadline)
        except TimeoutError:
            raise
        except Exception as exc:  # noqa: BLE001 - retain the last good catalog
            logger.warning({
                "event": "model_catalog_account_failed",
                "account_type": account_type,
                "error_type": type(exc).__name__,
            })
            return resolved_token, None

    def _submit_refresh_future(
        self,
        func: Callable[..., Any],
        *args: Any,
        submit_deadline: float,
        **kwargs: Any,
    ):
        while True:
            remaining = submit_deadline - self._deadline_clock()
            if remaining <= 0:
                raise ModelCatalogRefreshTimeout("model catalog refresh timed out")
            if _MODEL_CATALOG_REFRESH_SLOTS.acquire(timeout=min(remaining, 0.05)):
                break
        try:
            future = _MODEL_CATALOG_REFRESH_EXECUTOR.submit(func, *args, **kwargs)
        except BaseException:
            _MODEL_CATALOG_REFRESH_SLOTS.release()
            raise
        future.add_done_callback(lambda _future: _MODEL_CATALOG_REFRESH_SLOTS.release())
        return future

    def _refresh(
        self,
        groups: dict[str, list[str]],
        previous_anonymous_models: dict[str, dict[str, Any]],
        previous_models_by_access_token: dict[str, dict[str, dict[str, Any]]],
        deadline: float,
    ) -> tuple[
        dict[str, dict[str, Any]],
        dict[str, dict[str, dict[str, Any]]],
        dict[str, str],
    ]:
        models_by_access_token: dict[str, dict[str, dict[str, Any]]] = {}
        account_type_by_access_token: dict[str, str] = {}
        account_items = [
            (account_type, access_token)
            for account_type, access_tokens in groups.items()
            for access_token in dict.fromkeys(access_tokens)
        ]
        futures = []
        try:
            anonymous_future = self._submit_refresh_future(
                self._fetch_models,
                submit_deadline=deadline,
                deadline=deadline,
            )
            futures.append(anonymous_future)
            account_futures = {}
            for account_type, access_token in account_items:
                future = self._submit_refresh_future(
                    self._fetch_account_models,
                    account_type,
                    access_token,
                    deadline,
                    submit_deadline=deadline,
                )
                futures.append(future)
                account_futures[future] = (account_type, access_token)

            def remaining() -> float:
                value = deadline - self._deadline_clock()
                if value <= 0:
                    raise ModelCatalogRefreshTimeout("model catalog refresh timed out")
                return value

            try:
                anonymous_models = anonymous_future.result(timeout=remaining())
            except TimeoutError as exc:
                raise ModelCatalogRefreshTimeout("model catalog refresh timed out") from exc
            except Exception as exc:  # noqa: BLE001 - retain cached models on upstream failure
                logger.warning({
                    "event": "model_catalog_anonymous_failed",
                    "error_type": type(exc).__name__,
                })
                anonymous_models = previous_anonymous_models

            pending = set(account_futures)
            while pending:
                completed, pending = wait(
                    pending,
                    timeout=min(remaining(), 0.05),
                    return_when=FIRST_COMPLETED,
                )
                if self._deadline_clock() >= deadline:
                    raise ModelCatalogRefreshTimeout("model catalog refresh timed out")
                for future in completed:
                    account_type, access_token = account_futures[future]
                    resolved_token, models = future.result()
                    account_type_by_access_token[resolved_token] = account_type
                    if models is not None:
                        models_by_access_token[resolved_token] = models
                    else:
                        previous = previous_models_by_access_token.get(access_token)
                        if previous is None and resolved_token != access_token:
                            previous = previous_models_by_access_token.get(resolved_token)
                        if previous is not None:
                            models_by_access_token[resolved_token] = previous
        except (FutureTimeoutError, ModelCatalogRefreshTimeout) as exc:
            for future in futures:
                future.cancel()
            raise ModelCatalogRefreshTimeout("model catalog refresh timed out") from exc
        except BaseException:
            for future in futures:
                future.cancel()
            raise

        return anonymous_models, models_by_access_token, account_type_by_access_token

    def _prune_inactive_access_tokens_locked(self, groups: dict[str, list[str]]) -> None:
        current_type_by_access_token = {
            access_token: account_type
            for account_type, access_tokens in groups.items()
            for access_token in access_tokens
        }
        stale_tokens = {
            access_token
            for access_token, cached_type in self._account_type_by_access_token.items()
            if current_type_by_access_token.get(access_token) != cached_type
        }
        if not stale_tokens:
            return
        self._models_by_access_token = {
            access_token: models
            for access_token, models in self._models_by_access_token.items()
            if access_token not in stale_tokens
        }
        self._account_type_by_access_token = {
            access_token: account_type
            for access_token, account_type in self._account_type_by_access_token.items()
            if access_token not in stale_tokens
        }

    def _ensure_catalog(self) -> None:
        groups = self._active_accounts_by_type()
        signature = self._signature(groups)
        refresh_event: Event
        refresh_owner = False
        with self._lock:
            self._prune_inactive_access_tokens_locked(groups)
            if signature == self._account_signature and self._clock() < self._expires_at:
                return
            if self._refresh_in_progress:
                if self._catalog_loaded:
                    return
                refresh_event = self._refresh_done
            else:
                refresh_owner = True
                self._refresh_in_progress = True
                refresh_event = Event()
                self._refresh_done = refresh_event
                previous_anonymous_models = dict(self._anonymous_models)
                previous_models_by_access_token = {
                    access_token: dict(models)
                    for access_token, models in self._models_by_access_token.items()
                }

        if not refresh_owner:
            refresh_event.wait()
            with self._lock:
                if self._catalog_loaded:
                    return
            raise ModelUnavailableError("model catalog is unavailable")

        deadline = self._deadline_clock() + MODEL_CATALOG_REFRESH_TIMEOUT_SECS
        try:
            anonymous_models, models_by_access_token, account_type_by_access_token = self._refresh(
                groups,
                previous_anonymous_models,
                previous_models_by_access_token,
                deadline,
            )
        except ModelCatalogRefreshTimeout:
            with self._lock:
                self._refresh_in_progress = False
                refresh_event.set()
                if self._catalog_loaded:
                    self._expires_at = self._clock()
                    return
            raise ModelUnavailableError("model catalog is unavailable")
        except BaseException:
            with self._lock:
                self._refresh_in_progress = False
                refresh_event.set()
            raise

        current_groups = self._active_accounts_by_type()
        current_type_by_access_token = {
            access_token: account_type
            for account_type, access_tokens in current_groups.items()
            for access_token in access_tokens
        }
        models_by_access_token = {
            access_token: models
            for access_token, models in models_by_access_token.items()
            if (
                access_token in current_type_by_access_token
                and account_type_by_access_token.get(access_token)
                == current_type_by_access_token[access_token]
            )
        }
        account_type_by_access_token = {
            access_token: current_type_by_access_token[access_token]
            for access_token in models_by_access_token
        }
        current_signature = self._signature(current_groups)
        # 当前账号的上游目录失败时仍沿用本轮已有/旧快照并遵守 TTL；只有刷新期间
        # 账号集合发生变化，才必须立即让下一次读取重新按新身份拉取目录。
        cache_complete = current_signature == signature

        with self._lock:
            self._anonymous_models = anonymous_models
            self._models_by_access_token = models_by_access_token
            self._account_type_by_access_token = account_type_by_access_token
            self._account_signature = current_signature
            self._expires_at = (
                self._clock() + self._cache_ttl_seconds
                if cache_complete
                else self._clock()
            )
            self._catalog_loaded = True
            self._refresh_in_progress = False
            refresh_event.set()

    def list_models(self) -> dict[str, Any]:
        self._ensure_catalog()
        with self._lock:
            union: dict[str, dict[str, Any]] = {
                model_id: deepcopy(item)
                for model_id, item in self._anonymous_models.items()
            }
            for access_token in sorted(self._models_by_access_token):
                for model_id, item in self._models_by_access_token[access_token].items():
                    union.setdefault(model_id, deepcopy(item))
            data = []
            for model_id in sorted(union):
                item = deepcopy(union[model_id])
                item["allow_anonymous"] = model_id in self._anonymous_models
                item["supported_account_types"] = sorted({
                    account_type.lower()
                    for access_token, models in self._models_by_access_token.items()
                    if model_id in models
                    for account_type in [self._account_type_by_access_token.get(access_token, "")]
                    if account_type
                })
                data.append(item)
        return {
            "object": "list",
            "data": data,
        }

    def route_for_model(self, model: str) -> ModelRoute:
        model = str(model or "").strip()
        self._ensure_catalog()
        with self._lock:
            access_tokens = frozenset(
                access_token
                for access_token, models in self._models_by_access_token.items()
                if model in models
            )
            return ModelRoute(
                access_tokens=access_tokens,
                allow_anonymous=model in self._anonymous_models,
            )

    def supported_reasoning_efforts(
        self,
        model: str,
        *,
        access_token: str = "",
    ) -> tuple[str, ...] | None:
        model = str(model or "").strip()
        if not model:
            return None
        self._ensure_catalog()
        with self._lock:
            models = (
                self._models_by_access_token.get(access_token, {})
                if access_token
                else self._anonymous_models
            )
            item = models.get(model)
            if not isinstance(item, dict) or "supported_reasoning_efforts" not in item:
                return None
            values = item.get("supported_reasoning_efforts")
            if not isinstance(values, list):
                return None
            return tuple(
                value.strip().lower()
                for value in values
                if isinstance(value, str) and value.strip()
            )

    def normalize_reasoning_effort(
        self,
        model: str,
        value: object,
        *,
        access_token: str = "",
    ) -> str:
        requested = canonical_conversation_effort(value)
        if requested in {"", "auto"}:
            return ""
        supported = self.supported_reasoning_efforts(
            model,
            access_token=access_token,
        )
        if supported is None:
            return fallback_conversation_effort(requested)
        normalized_supported = tuple(
            canonical_conversation_effort(item)
            for item in supported
        )
        if requested in normalized_supported:
            return requested
        aliases = {
            "minimal": ("min",),
            "min": ("minimal",),
            "xhigh": ("extended",),
            "extended": ("xhigh",),
        }
        for alias in aliases.get(requested, ()):
            if alias in normalized_supported:
                return alias
        strongest = strongest_conversation_effort(normalized_supported)
        return "" if strongest == "auto" else strongest


model_catalog_service = ModelCatalogService(account_service)
