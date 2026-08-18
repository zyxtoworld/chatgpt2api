from __future__ import annotations

import time
from collections.abc import Callable
from copy import deepcopy
from concurrent.futures import FIRST_COMPLETED, TimeoutError as FutureTimeoutError
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass
from functools import partial
from threading import BoundedSemaphore, Condition, Event, RLock, local
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


AccountModelGroup = tuple[str, str]


_MODEL_IO_THREAD_CAPACITY = 4
_MODEL_IO_THREAD_STATE = local()
MODEL_CATALOG_REFRESH_TIMEOUT_SECS = 90.0
MODEL_CATALOG_RETRY_BACKOFF_SECS = 5.0
MODEL_CATALOG_REFRESH_WORKERS = 4
_MODEL_CATALOG_REFRESH_EXECUTOR = ThreadPoolExecutor(
    max_workers=MODEL_CATALOG_REFRESH_WORKERS,
    thread_name_prefix="model-catalog",
)
_MODEL_CATALOG_REFRESH_SLOTS = BoundedSemaphore(MODEL_CATALOG_REFRESH_WORKERS)
_MODEL_CATALOG_OWNER_EXECUTOR = ThreadPoolExecutor(
    max_workers=1,
    thread_name_prefix="model-catalog-owner",
)


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
    catalog_complete: bool = True


class ModelUnavailableError(RuntimeError, PublicSafeErrorMarker):
    status_code = 400

    def public_safe_message(self) -> str:
        return "The requested model is not available."


class ModelCatalogPendingError(ModelUnavailableError):
    status_code = 503
    retry_after_seconds = 5

    def public_safe_message(self) -> str:
        return "The model catalog is still warming up. Please try again shortly."


class ModelCatalogRefreshTimeout(TimeoutError):
    pass


@dataclass(frozen=True)
class _IncrementalAccountTypeCatalogResult:
    account_type: AccountModelGroup
    resolved_token: str
    expected_account: object | None
    models: dict[str, dict[str, Any]] | None


class ModelCatalogService:
    """Caches one upstream model catalog for each active account type."""

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
        self._catalog_condition = Condition(self._lock)
        self._expires_at = 0.0
        self._account_signature: tuple[str, ...] = ()
        self._anonymous_models: dict[str, dict[str, Any]] = {}
        self._anonymous_ready = False
        self._anonymous_retry_not_before = 0.0
        self._models_by_account_group: dict[AccountModelGroup, dict[str, dict[str, Any]]] = {}
        self._ready_account_groups: set[AccountModelGroup] = set()
        self._account_group_retry_not_before: dict[AccountModelGroup, float] = {}
        self._catalog_loaded = False
        self._cold_retry_not_before = 0.0
        self._catalog_complete = False
        self._refresh_in_progress = False
        self._catalog_generation = 0
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

    def _active_accounts_by_group(self) -> dict[AccountModelGroup, list[str]]:
        groups: dict[AccountModelGroup, list[str]] = {}
        source_normalizer = getattr(self._accounts, "_normalize_source_type", None)
        for account in self._accounts.list_accounts():
            if not isinstance(account, dict) or not self._accounts._is_text_account_available(account):
                continue
            access_token = str(account.get("access_token") or "").strip()
            account_type = self._accounts._normalize_account_type(account.get("type"))
            source_type = (
                source_normalizer(account.get("source_type"))
                if callable(source_normalizer)
                else str(account.get("source_type") or "web").strip().lower() or "web"
            )
            if access_token and account_type and source_type:
                groups.setdefault((source_type, account_type), []).append(access_token)
        for access_tokens in groups.values():
            access_tokens[:] = sorted(dict.fromkeys(access_tokens))
        return groups

    @staticmethod
    def _signature(groups: dict[AccountModelGroup, list[str]]) -> tuple[AccountModelGroup, ...]:
        return tuple(sorted(groups))

    def _active_accounts_by_type(self) -> dict[str, list[str]]:
        """Compatibility view used by image/public helpers; routing uses groups."""
        groups: dict[str, list[str]] = {}
        for (_source_type, account_type), access_tokens in self._active_accounts_by_group().items():
            groups.setdefault(account_type, []).extend(access_tokens)
        for access_tokens in groups.values():
            access_tokens[:] = sorted(dict.fromkeys(access_tokens))
        return groups

    @property
    def _models_by_account_type(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Read-only compatibility projection; source-aware state lives by group."""
        result: dict[str, dict[str, dict[str, Any]]] = {}
        for (_source_type, account_type), models in self._models_by_account_group.items():
            target = result.setdefault(account_type, {})
            for model_id, item in models.items():
                target.setdefault(model_id, deepcopy(item))
        return result

    @property
    def _ready_account_types(self) -> set[str]:
        return {account_type for _source_type, account_type in self._ready_account_groups}

    def _fetch_models(
        self,
        access_token: str = "",
        *,
        source_type: str | None = None,
        deadline: float | None = None,
    ) -> dict[str, dict[str, Any]]:
        backend = self._backend_factory(access_token=access_token)
        if source_type:
            # Keep the discovery owner explicit even when a backend factory
            # obtains its account metadata from a different singleton.
            setattr(backend, "_catalog_source_type", source_type)
        try:
            models = self._model_map(backend.list_models(deadline=deadline))
            if not models:
                # A successful HTTP response with no usable model is not a
                # ready catalog.  Publishing it would make the account group
                # look healthy, suppress retry backoff, and can turn a cold
                # /v1/models response into a misleading synthetic-only list.
                raise RuntimeError("upstream model catalog is empty")
            return models
        finally:
            backend.close()

    def _get_account_lease(self, access_token: str) -> tuple[str, object | None]:
        getter = getattr(self._accounts, "_get_account_lease", None)
        if callable(getter):
            resolved_token, account = getter(access_token)
            return str(resolved_token or access_token), account
        getter = getattr(self._accounts, "get_account", None)
        if callable(getter):
            return access_token, getter(access_token)
        return access_token, None

    def _fetch_account_type_models(
        self,
        account_type: AccountModelGroup,
        candidate_tokens: tuple[str, ...],
        deadline: float,
    ) -> _IncrementalAccountTypeCatalogResult:
        last_error: Exception | None = None
        resolved_token = candidate_tokens[0] if candidate_tokens else ""
        expected_account: object | None = None
        # A type is represented by one deterministic account.  Try each
        # distinct live candidate until one succeeds or the shared deadline is
        # exhausted; never truncate a type's candidate set arbitrarily.
        attempted_tokens: set[str] = set()
        for access_token in candidate_tokens:
            try:
                resolved_token = (
                    self._accounts.refresh_access_token(
                        access_token,
                        event="list_models",
                        deadline=deadline,
                    )
                    or access_token
                )
                if resolved_token in attempted_tokens:
                    continue
                attempted_tokens.add(resolved_token)
                if self._deadline_clock() >= deadline:
                    raise TimeoutError("model catalog refresh timed out")
                resolved_token, expected_account = self._get_account_lease(resolved_token)
                if expected_account is None:
                    raise RuntimeError("representative account disappeared")
                models = self._fetch_models(
                    resolved_token,
                    source_type=account_type[0],
                    deadline=deadline,
                )
                return _IncrementalAccountTypeCatalogResult(
                    account_type,
                    resolved_token,
                    expected_account,
                    models,
                )
            except Exception as exc:  # noqa: BLE001 - try the bounded fallback
                last_error = exc
                logger.warning({
                    "event": "model_catalog_account_failed",
                    "account_type": account_type,
                    "error_type": type(exc).__name__,
                })
                if self._deadline_clock() >= deadline:
                    break
        if last_error is not None:
            logger.warning({
                "event": "model_catalog_type_failed",
                "account_type": account_type,
                "error_type": type(last_error).__name__,
            })
        return _IncrementalAccountTypeCatalogResult(
            account_type,
            resolved_token,
            expected_account,
            None,
        )

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
        previous_models_by_account_type: dict[AccountModelGroup, dict[str, dict[str, Any]]],
        deadline: float,
    ) -> tuple[
        dict[str, dict[str, Any]],
        dict[str, dict[str, dict[str, Any]]],
        set[str],
        bool,
    ]:
        models_by_account_type: dict[AccountModelGroup, dict[str, dict[str, Any]]] = {}
        successful_account_types: set[AccountModelGroup] = set()
        anonymous_succeeded = False
        futures = []
        try:
            anonymous_future = self._submit_refresh_future(
                self._fetch_models,
                submit_deadline=deadline,
                deadline=deadline,
            )
            futures.append(anonymous_future)
            account_futures = {}
            for account_type, access_tokens in sorted(groups.items()):
                future = self._submit_refresh_future(
                    self._fetch_account_type_models,
                    account_type,
                    tuple(access_tokens),
                    deadline,
                    submit_deadline=deadline,
                )
                futures.append(future)
                account_futures[future] = account_type

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
            else:
                anonymous_succeeded = isinstance(anonymous_models, dict)

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
                    account_type = account_futures[future]
                    result = future.result()
                    if result.models is not None:
                        models_by_account_type[account_type] = result.models
                        successful_account_types.add(account_type)
                    elif account_type in previous_models_by_account_type:
                        models_by_account_type[account_type] = deepcopy(
                            previous_models_by_account_type[account_type]
                        )
        except (FutureTimeoutError, ModelCatalogRefreshTimeout) as exc:
            for future in futures:
                future.cancel()
            raise ModelCatalogRefreshTimeout("model catalog refresh timed out") from exc
        except BaseException:
            for future in futures:
                future.cancel()
            raise

        return (
            anonymous_models,
            models_by_account_type,
            successful_account_types,
            anonymous_succeeded,
        )

    def _prune_inactive_account_types_locked(
        self,
        groups: dict[AccountModelGroup, list[str]],
    ) -> None:
        active_groups = set(groups)
        for account_group in set(self._models_by_account_group) - active_groups:
            self._models_by_account_group.pop(account_group, None)
            self._ready_account_groups.discard(account_group)
        for account_group in set(self._account_group_retry_not_before) - active_groups:
            self._account_group_retry_not_before.pop(account_group, None)

    def _has_cold_ready_snapshot_locked(
        self,
        groups: dict[AccountModelGroup, list[str]],
    ) -> bool:
        return self._anonymous_ready and set(groups) <= self._ready_account_groups

    def _refresh_scope_locked(
        self,
        groups: dict[AccountModelGroup, list[str]],
    ) -> tuple[dict[AccountModelGroup, list[str]], bool]:
        """Choose only types whose snapshot needs work at this instant."""
        now = self._clock()
        signature_changed = self._signature(groups) != self._account_signature
        refresh_types: set[AccountModelGroup] = set()

        if not self._catalog_loaded:
            refresh_types = set(groups)
            refresh_anonymous = True
        elif signature_changed:
            refresh_types = {
                account_type
                for account_type in groups
                if account_type not in self._ready_account_groups
                or (
                    account_type in self._account_group_retry_not_before
                    and now >= self._account_group_retry_not_before[account_type]
                )
            }
            refresh_anonymous = (
                not self._anonymous_ready
                or now >= self._anonymous_retry_not_before
            )
        elif self._catalog_complete and now >= self._expires_at:
            refresh_types = {
                account_type
                for account_type in groups
                if now >= self._account_group_retry_not_before.get(account_type, 0.0)
            }
            refresh_anonymous = now >= self._anonymous_retry_not_before
        else:
            refresh_types = {
                account_type
                for account_type in groups
                if (
                    account_type not in self._ready_account_groups
                    or account_type in self._account_group_retry_not_before
                )
                and now >= self._account_group_retry_not_before.get(account_type, 0.0)
            }
            refresh_anonymous = (
                not self._anonymous_ready
                and now >= self._anonymous_retry_not_before
            )

        return (
            {
                account_type: list(groups[account_type])
                for account_type in sorted(refresh_types)
            },
            refresh_anonymous,
        )

    def _publish_incremental_account_type_result(
        self,
        result: _IncrementalAccountTypeCatalogResult,
        previous_models_by_account_type: dict[AccountModelGroup, dict[str, dict[str, Any]]],
        generation: int,
    ) -> None:
        with self._lock:
            if generation != self._catalog_generation or not self._refresh_in_progress:
                return
            groups = self._active_accounts_by_group()
            if result.account_type not in groups:
                return
            resolved_token, current_account = self._get_account_lease(result.resolved_token)
            if (
                resolved_token != result.resolved_token
                or current_account is not result.expected_account
                or resolved_token not in groups[result.account_type]
            ):
                if result.models is None:
                    self._account_group_retry_not_before[result.account_type] = (
                        self._clock() + MODEL_CATALOG_RETRY_BACKOFF_SECS
                    )
                return

            if result.models is not None:
                self._models_by_account_group[result.account_type] = deepcopy(result.models)
                self._ready_account_groups.add(result.account_type)
                self._account_group_retry_not_before.pop(result.account_type, None)
            elif result.account_type not in previous_models_by_account_type:
                self._ready_account_groups.discard(result.account_type)
                self._account_group_retry_not_before[result.account_type] = (
                    self._clock() + MODEL_CATALOG_RETRY_BACKOFF_SECS
                )
            else:
                self._account_group_retry_not_before[result.account_type] = (
                    self._clock() + MODEL_CATALOG_RETRY_BACKOFF_SECS
                )
            self._catalog_loaded = True
            if self._has_cold_ready_snapshot_locked(groups):
                self._cold_retry_not_before = 0.0
            self._catalog_condition.notify_all()

    def _publish_incremental_anonymous_models(
        self,
        models: dict[str, dict[str, Any]] | None,
        generation: int,
    ) -> None:
        if models is None:
            with self._lock:
                if generation == self._catalog_generation and self._refresh_in_progress:
                    self._anonymous_retry_not_before = (
                        self._clock() + MODEL_CATALOG_RETRY_BACKOFF_SECS
                    )
            return
        with self._lock:
            if generation != self._catalog_generation or not self._refresh_in_progress:
                return
            self._anonymous_models = deepcopy(models)
            self._anonymous_ready = True
            self._anonymous_retry_not_before = 0.0
            self._catalog_loaded = True
            self._catalog_condition.notify_all()

    def _run_incremental_refresh(
        self,
        groups: dict[AccountModelGroup, list[str]],
        previous_models_by_account_type: dict[AccountModelGroup, dict[str, dict[str, Any]]],
        generation: int,
        owner_deadline: float,
        refresh_account_types: set[AccountModelGroup] | None = None,
        refresh_anonymous: bool = True,
    ) -> None:
        started_signature = self._signature(groups)
        account_types = set(groups) if refresh_account_types is None else set(refresh_account_types)
        pending = [
            (account_type, groups[account_type])
            for account_type in sorted(account_types & set(groups))
        ]
        futures: dict[object, tuple[str, str | None]] = {}
        anonymous_finished = not refresh_anonymous
        anonymous_succeeded = not refresh_anonymous
        admission_blocked = False
        try:
            while (pending or futures or not anonymous_finished) and not admission_blocked:
                while len(futures) < MODEL_CATALOG_REFRESH_WORKERS:
                    if not anonymous_finished and not any(
                        kind == "anonymous" for kind, _ in futures.values()
                    ):
                        try:
                            future = self._submit_refresh_future(
                                self._fetch_models,
                                submit_deadline=owner_deadline,
                                deadline=owner_deadline,
                            )
                        except ModelCatalogRefreshTimeout:
                            admission_blocked = True
                            break
                        futures[future] = ("anonymous", None)
                        continue
                    if not pending:
                        break
                    account_type, access_tokens = pending.pop(0)
                    try:
                        future = self._submit_refresh_future(
                            self._fetch_account_type_models,
                            account_type,
                            tuple(access_tokens),
                            owner_deadline,
                            submit_deadline=owner_deadline,
                        )
                    except ModelCatalogRefreshTimeout:
                        pending.insert(0, (account_type, access_tokens))
                        admission_blocked = True
                        break
                    futures[future] = ("account", account_type)

                if not futures:
                    break
                remaining = owner_deadline - self._deadline_clock()
                if remaining <= 0:
                    break
                completed, _ = wait(
                    futures,
                    timeout=min(0.25, remaining),
                    return_when=FIRST_COMPLETED,
                )
                if not completed:
                    continue
                for future in completed:
                    kind, _access_token = futures.pop(future)
                    try:
                        value = future.result()
                    except Exception as exc:  # noqa: BLE001 - continue other owners
                        logger.warning({
                            "event": "model_catalog_incremental_failed",
                            "error_type": type(exc).__name__,
                        })
                        value = None
                    if kind == "anonymous":
                        anonymous_finished = True
                        anonymous_succeeded = isinstance(value, dict)
                        self._publish_incremental_anonymous_models(
                            value if isinstance(value, dict) else None,
                            generation,
                        )
                    elif isinstance(value, _IncrementalAccountTypeCatalogResult):
                        self._publish_incremental_account_type_result(
                            value,
                            previous_models_by_account_type,
                            generation,
                        )
        finally:
            for future in futures:
                future.cancel()
            with self._lock:
                if generation != self._catalog_generation:
                    return
                current_groups = self._active_accounts_by_group()
                self._prune_inactive_account_types_locked(current_groups)
                current_signature = self._signature(current_groups)
                work_complete = (
                    not pending
                    and not futures
                    and anonymous_finished
                    and not admission_blocked
                )
                self._refresh_in_progress = False
                self._refresh_done.set()
                self._catalog_condition.notify_all()
                if not self._catalog_loaded:
                    self._catalog_loaded = True
                retry_not_before = self._clock() + MODEL_CATALOG_RETRY_BACKOFF_SECS
                unfinished_account_types = {
                    account_type
                    for account_type, _access_tokens in pending
                }
                unfinished_anonymous = not anonymous_finished
                for kind, account_type in futures.values():
                    if kind == "anonymous":
                        unfinished_anonymous = True
                    elif account_type is not None:
                        unfinished_account_types.add(account_type)
                for account_type in unfinished_account_types:
                    if account_type in current_groups:
                        self._account_group_retry_not_before[account_type] = retry_not_before
                if unfinished_anonymous:
                    self._anonymous_retry_not_before = retry_not_before
                self._account_signature = started_signature
                self._catalog_complete = (
                    current_signature == started_signature
                    and work_complete
                    and account_types <= self._ready_account_groups
                    and (anonymous_succeeded or self._anonymous_ready)
                )
                if self._catalog_complete:
                    self._expires_at = self._clock() + self._cache_ttl_seconds
                else:
                    self._expires_at = self._clock()

    def _ensure_catalog_nonblocking(self) -> None:
        groups = self._active_accounts_by_group()
        with self._lock:
            self._prune_inactive_account_types_locked(groups)
            if self._refresh_in_progress:
                return

            refresh_groups, refresh_anonymous = self._refresh_scope_locked(groups)
            if not refresh_groups and not refresh_anonymous:
                return

            self._refresh_in_progress = True
            self._catalog_generation += 1
            generation = self._catalog_generation
            self._refresh_done = Event()
            previous_models_by_account_type = {
                account_group: deepcopy(models)
                for account_group, models in self._models_by_account_group.items()
            }
            try:
                _MODEL_CATALOG_OWNER_EXECUTOR.submit(
                    self._run_incremental_refresh,
                    dict(groups),
                    previous_models_by_account_type,
                    generation,
                    self._deadline_clock() + MODEL_CATALOG_REFRESH_TIMEOUT_SECS,
                    set(refresh_groups),
                    refresh_anonymous,
                )
            except BaseException:
                self._refresh_in_progress = False
                self._refresh_done.set()
                self._catalog_condition.notify_all()
                raise

    def _ensure_catalog(self) -> None:
        groups = self._active_accounts_by_group()
        signature = self._signature(groups)
        refresh_event: Event
        refresh_owner = False
        cold_start = False
        with self._lock:
            self._prune_inactive_account_types_locked(groups)
            if signature == self._account_signature and self._clock() < self._expires_at:
                return
            if self._refresh_in_progress:
                if self._catalog_loaded and self._has_cold_ready_snapshot_locked(groups):
                    return
                refresh_event = self._refresh_done
            else:
                refresh_owner = True
                cold_start = not self._has_cold_ready_snapshot_locked(groups)
                self._refresh_in_progress = True
                self._catalog_generation += 1
                refresh_event = Event()
                self._refresh_done = refresh_event
                previous_anonymous_models = dict(self._anonymous_models)
                previous_models_by_account_type = {
                    account_group: dict(models)
                    for account_group, models in self._models_by_account_group.items()
                }
                previous_ready_account_types = set(self._ready_account_groups)
                previous_anonymous_ready = self._anonymous_ready

        if not refresh_owner:
            refresh_event.wait()
            with self._lock:
                current_groups = self._active_accounts_by_group()
                self._prune_inactive_account_types_locked(current_groups)
                if self._has_cold_ready_snapshot_locked(current_groups):
                    return
            raise ModelCatalogPendingError("model catalog is still loading")

        deadline = self._deadline_clock() + MODEL_CATALOG_REFRESH_TIMEOUT_SECS
        try:
            (
                anonymous_models,
                models_by_account_type,
                successful_account_types,
                anonymous_succeeded,
            ) = self._refresh(
                groups,
                previous_anonymous_models,
                previous_models_by_account_type,
                deadline,
            )
        except ModelCatalogRefreshTimeout:
            with self._lock:
                self._refresh_in_progress = False
                refresh_event.set()
                current_groups = self._active_accounts_by_group()
                self._prune_inactive_account_types_locked(current_groups)
                if self._catalog_loaded and self._has_cold_ready_snapshot_locked(current_groups):
                    self._expires_at = self._clock()
                    return
            raise ModelCatalogPendingError("model catalog is still loading")
        except BaseException:
            with self._lock:
                self._refresh_in_progress = False
                refresh_event.set()
            raise

        current_groups = self._active_accounts_by_group()
        current_signature = self._signature(current_groups)
        # 当前账号的上游目录失败时仍沿用本轮已有/旧快照并遵守 TTL；只有刷新期间
        # 账号集合发生变化，才必须立即让下一次读取重新按新身份拉取目录。
        cache_complete = current_signature == signature

        with self._lock:
            self._anonymous_models = anonymous_models
            self._anonymous_ready = anonymous_succeeded or previous_anonymous_ready
            if anonymous_succeeded:
                self._anonymous_retry_not_before = 0.0
            else:
                self._anonymous_retry_not_before = (
                    self._clock() + MODEL_CATALOG_RETRY_BACKOFF_SECS
                )
            self._models_by_account_group = {
                account_group: models
                for account_group, models in models_by_account_type.items()
                if account_group in current_groups
            }
            self._ready_account_groups = {
                account_group
                for account_group in current_groups
                if account_group in successful_account_types
                or account_group in previous_ready_account_types
            }
            for account_group in current_groups:
                if account_group in successful_account_types:
                    self._account_group_retry_not_before.pop(account_group, None)
                else:
                    self._account_group_retry_not_before[account_group] = (
                        self._clock() + MODEL_CATALOG_RETRY_BACKOFF_SECS
                    )
            self._account_signature = current_signature
            cache_complete = (
                cache_complete
                and self._anonymous_ready
                and set(current_groups) <= self._ready_account_groups
            )
            self._catalog_complete = cache_complete
            self._expires_at = (
                self._clock() + self._cache_ttl_seconds
                if cache_complete
                else self._clock()
            )
            self._catalog_loaded = True
            self._refresh_in_progress = False
            refresh_event.set()
            self._catalog_condition.notify_all()

            cold_ready = self._has_cold_ready_snapshot_locked(current_groups)
            self._cold_retry_not_before = (
                0.0
                if cold_ready
                else self._clock() + MODEL_CATALOG_RETRY_BACKOFF_SECS
            )

        if cold_start and not cold_ready:
            raise ModelCatalogPendingError("model catalog is still loading")

    def _model_is_ready(self, model: str) -> bool:
        groups = self._active_accounts_by_group()
        with self._lock:
            if model == "auto":
                return self._anonymous_ready or any(
                    account_group in self._ready_account_groups
                    for account_group in groups
                )
            if model in self._anonymous_models:
                return True
            return any(
                account_group in self._ready_account_groups
                and model in self._models_by_account_group.get(account_group, {})
                for account_group in groups
            )

    def _ensure_catalog_for_model(self, model: str) -> None:
        self._ensure_catalog_nonblocking()
        deadline = self._deadline_clock() + MODEL_CATALOG_REFRESH_TIMEOUT_SECS
        while True:
            if self._model_is_ready(model):
                return
            with self._catalog_condition:
                if not self._refresh_in_progress:
                    return
                remaining = deadline - self._deadline_clock()
                if remaining <= 0:
                    return
                self._catalog_condition.wait(timeout=min(0.05, remaining))

    def _ensure_catalog_for_read(self, *, wait_for_cold: bool) -> None:
        if not wait_for_cold:
            self._ensure_catalog_nonblocking()
            return
        groups = self._active_accounts_by_group()
        with self._lock:
            self._prune_inactive_account_types_locked(groups)
            cold = not self._has_cold_ready_snapshot_locked(groups)
            if cold and self._catalog_loaded and self._clock() < self._cold_retry_not_before:
                raise ModelCatalogPendingError("model catalog is still loading")
        if cold:
            self._ensure_catalog()
        else:
            self._ensure_catalog_nonblocking()

    def list_models(self, *, wait_for_cold: bool = True) -> dict[str, Any]:
        self._ensure_catalog_for_read(wait_for_cold=wait_for_cold)
        with self._lock:
            union: dict[str, dict[str, Any]] = {
                model_id: deepcopy(item)
                for model_id, item in self._anonymous_models.items()
            }
            for account_group in sorted(self._models_by_account_group):
                for model_id, item in self._models_by_account_group[account_group].items():
                    union.setdefault(model_id, deepcopy(item))
            data = []
            for model_id in sorted(union):
                item = deepcopy(union[model_id])
                item["allow_anonymous"] = model_id in self._anonymous_models
                item["supported_account_types"] = sorted({
                    account_type.lower()
                    for (_source_type, account_type), models in self._models_by_account_group.items()
                    if model_id in models
                })
                data.append(item)
        return {
            "object": "list",
            "data": data,
        }

    def route_for_model(self, model: str, *, wait_for_cold: bool = True) -> ModelRoute:
        model = str(model or "").strip()
        with self._lock:
            cold = not self._catalog_loaded
        if wait_for_cold and cold:
            self._ensure_catalog_for_model(model)
        else:
            self._ensure_catalog_for_read(wait_for_cold=False)
        groups = self._active_accounts_by_group()
        with self._lock:
            access_tokens = frozenset(
                access_token
                for account_group, access_tokens_for_type in groups.items()
                if account_group in self._ready_account_groups
                and model in self._models_by_account_group.get(account_group, {})
                for access_token in access_tokens_for_type
            )
            pending_types = set(groups) - self._ready_account_groups
            return ModelRoute(
                access_tokens=access_tokens,
                allow_anonymous=model in self._anonymous_models,
                catalog_complete=(
                    (self._anonymous_ready and not pending_types)
                    or model in self._anonymous_models
                    or bool(access_tokens)
                ),
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
        self._ensure_catalog_for_read(wait_for_cold=True)
        with self._lock:
            if access_token:
                account = self._accounts.get_account(access_token)
                account_type = self._accounts._normalize_account_type(
                    account.get("type") if isinstance(account, dict) else None
                )
                source_normalizer = getattr(self._accounts, "_normalize_source_type", None)
                source_type = (
                    source_normalizer(account.get("source_type") if isinstance(account, dict) else None)
                    if callable(source_normalizer)
                    else str((account or {}).get("source_type") or "web").strip().lower()
                    if isinstance(account, dict)
                    else "web"
                )
                models = self._models_by_account_group.get((source_type, account_type), {})
            else:
                models = self._anonymous_models
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
