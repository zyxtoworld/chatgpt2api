from __future__ import annotations

import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from threading import Event, RLock, local
from typing import Any

import anyio

from services.account_service import AccountService, account_service
from services.openai_backend_api import OpenAIBackendAPI
from services.protocol.reasoning_effort import (
    canonical_conversation_effort,
    fallback_conversation_effort,
    strongest_conversation_effort,
)
from utils.log import logger


_MODEL_IO_THREAD_CAPACITY = 4
_MODEL_IO_THREAD_STATE = local()


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
    account_types: frozenset[str]
    allow_anonymous: bool = False


class ModelUnavailableError(RuntimeError):
    pass


class ModelCatalogService:
    """Caches the model catalogs advertised to each active account type."""

    def __init__(
        self,
        accounts: AccountService,
        *,
        backend_factory: Callable[..., Any] = OpenAIBackendAPI,
        cache_ttl_seconds: float = 300,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._accounts = accounts
        self._backend_factory = backend_factory
        self._cache_ttl_seconds = max(1.0, float(cache_ttl_seconds))
        self._clock = clock
        self._lock = RLock()
        self._expires_at = 0.0
        self._account_signature: tuple[tuple[str, int], ...] = ()
        self._anonymous_models: dict[str, dict[str, Any]] = {}
        self._models_by_account_type: dict[str, dict[str, dict[str, Any]]] = {}
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
            model_id = str(item.get("id") or "").strip()
            if model_id and model_id not in models:
                models[model_id] = dict(item)
        return models

    def _active_accounts_by_type(self) -> dict[str, list[str]]:
        groups: dict[str, list[str]] = {}
        for account in self._accounts.list_accounts():
            if not isinstance(account, dict) or account.get("status") in {"禁用", "异常"}:
                continue
            access_token = str(account.get("access_token") or "").strip()
            account_type = self._accounts._normalize_account_type(account.get("type"))
            if access_token and account_type:
                groups.setdefault(account_type, []).append(access_token)
        return groups

    @staticmethod
    def _signature(groups: dict[str, list[str]]) -> tuple[tuple[str, int], ...]:
        return tuple(
            (account_type, len(tokens))
            for account_type, tokens in sorted(groups.items())
        )

    def _fetch_models(self, access_token: str = "") -> dict[str, dict[str, Any]]:
        backend = self._backend_factory(access_token=access_token)
        try:
            return self._model_map(backend.list_models())
        finally:
            backend.close()

    def _fetch_account_type_models(
        self,
        account_type: str,
        access_tokens: list[str],
    ) -> dict[str, dict[str, Any]] | None:
        attempted_tokens: set[str] = set()
        last_error: Exception | None = None
        for access_token in access_tokens:
            try:
                resolved_token = self._accounts.refresh_access_token(
                    access_token,
                    event="list_models",
                ) or access_token
                if resolved_token in attempted_tokens:
                    continue
                attempted_tokens.add(resolved_token)
                return self._fetch_models(resolved_token)
            except Exception as exc:  # noqa: BLE001 - try the next account for any upstream failure
                last_error = exc
        if last_error is not None:
            logger.warning({
                "event": "model_catalog_account_type_failed",
                "account_type": account_type,
                "error_type": type(last_error).__name__,
            })
        return None

    def _refresh(
        self,
        groups: dict[str, list[str]],
        previous_anonymous_models: dict[str, dict[str, Any]],
        previous_models_by_account_type: dict[str, dict[str, dict[str, Any]]],
    ) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, dict[str, Any]]]]:
        models_by_account_type: dict[str, dict[str, dict[str, Any]]] = {}
        with ThreadPoolExecutor(max_workers=min(4, len(groups) + 1)) as executor:
            anonymous_future = executor.submit(self._fetch_models)
            account_futures = {
                account_type: executor.submit(
                    self._fetch_account_type_models,
                    account_type,
                    access_tokens,
                )
                for account_type, access_tokens in groups.items()
            }
            try:
                anonymous_models = anonymous_future.result()
            except Exception as exc:  # noqa: BLE001 - retain cached models on upstream failure
                logger.warning({
                    "event": "model_catalog_anonymous_failed",
                    "error_type": type(exc).__name__,
                })
                anonymous_models = previous_anonymous_models

            for account_type, future in account_futures.items():
                models = future.result()
                if models is not None:
                    models_by_account_type[account_type] = models
                elif account_type in previous_models_by_account_type:
                    models_by_account_type[account_type] = previous_models_by_account_type[account_type]

        return anonymous_models, models_by_account_type

    def _ensure_catalog(self) -> None:
        groups = self._active_accounts_by_type()
        signature = self._signature(groups)
        refresh_event: Event
        refresh_owner = False
        with self._lock:
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
                previous_models_by_account_type = {
                    account_type: dict(models)
                    for account_type, models in self._models_by_account_type.items()
                }

        if not refresh_owner:
            refresh_event.wait()
            with self._lock:
                if self._catalog_loaded:
                    return
            raise ModelUnavailableError("model catalog is unavailable")

        try:
            anonymous_models, models_by_account_type = self._refresh(
                groups,
                previous_anonymous_models,
                previous_models_by_account_type,
            )
        except BaseException:
            with self._lock:
                self._refresh_in_progress = False
                refresh_event.set()
            raise

        with self._lock:
            self._anonymous_models = anonymous_models
            self._models_by_account_type = models_by_account_type
            self._account_signature = signature
            self._expires_at = self._clock() + self._cache_ttl_seconds
            self._catalog_loaded = True
            self._refresh_in_progress = False
            refresh_event.set()

    def list_models(self) -> dict[str, Any]:
        self._ensure_catalog()
        with self._lock:
            union: dict[str, dict[str, Any]] = {
                model_id: dict(item)
                for model_id, item in self._anonymous_models.items()
            }
            for account_type in sorted(self._models_by_account_type):
                for model_id, item in self._models_by_account_type[account_type].items():
                    union.setdefault(model_id, dict(item))
            data = []
            for model_id in sorted(union):
                item = dict(union[model_id])
                item["allow_anonymous"] = model_id in self._anonymous_models
                item["supported_account_types"] = sorted({
                    account_type.lower()
                    for account_type, models in self._models_by_account_type.items()
                    if model_id in models
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
            account_types = frozenset(
                account_type
                for account_type, models in self._models_by_account_type.items()
                if model in models
            )
            return ModelRoute(
                account_types=account_types,
                allow_anonymous=model in self._anonymous_models,
            )

    def _account_type_for_access_token(self, access_token: str) -> str:
        if not access_token:
            return ""
        for account in self._accounts.list_accounts():
            if not isinstance(account, dict):
                continue
            if str(account.get("access_token") or "").strip() != access_token:
                continue
            return self._accounts._normalize_account_type(account.get("type"))
        return ""

    def supported_reasoning_efforts(
        self,
        model: str,
        *,
        access_token: str = "",
    ) -> tuple[str, ...] | None:
        model = str(model or "").strip()
        if not model:
            return None
        account_type = self._account_type_for_access_token(access_token)
        if access_token and not account_type:
            return None
        self._ensure_catalog()
        with self._lock:
            models = (
                self._models_by_account_type.get(account_type, {})
                if account_type
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
