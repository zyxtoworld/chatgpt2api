from __future__ import annotations

import base64
import json
import logging
import secrets
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Condition, Lock
from typing import Any, Callable, Iterator
from urllib.parse import urlencode

from services.config import config
from services.log_service import (
    LOG_TYPE_ACCOUNT,
    log_service,
)
from services.protocol.error_response import exception_log_message
from services.remote_response import close_response, parse_json_response
from services.secure_file import atomic_write_bytes, read_checked_file_bytes
from services.storage.base import (
    StorageBackend,
    StorageDataError,
    StorageSnapshot,
    make_storage_snapshot,
)
from services.task_executor import BackgroundTaskQueueFullError, reserve_background_task
from utils.helper import anonymize_token


_ACCOUNT_LOGGER = logging.getLogger(__name__)


def _account_log(summary: str, detail: dict[str, Any]) -> None:
    try:
        log_service.add(LOG_TYPE_ACCOUNT, summary, detail)
    except Exception:
        _ACCOUNT_LOGGER.error("account log persistence failed")


ACCOUNT_REFRESH_WORKERS = 10
_ACCOUNT_REFRESH_EXECUTOR = ThreadPoolExecutor(
    max_workers=ACCOUNT_REFRESH_WORKERS,
    thread_name_prefix="account-refresh",
)
TOKEN_REFRESH_ERROR_FALLBACK = "refresh_failed"
_MAX_TOKEN_RESPONSE_BYTES = 1 * 1024 * 1024
TOKEN_REFRESH_ERROR_CODES = frozenset(
    {
        TOKEN_REFRESH_ERROR_FALLBACK,
        "app_session_terminated",
        "network_error",
        "invalid_response",
        "http_4xx",
        "http_5xx",
    }
)
INVALID_TOKEN_ERROR_MESSAGE = "账号访问令牌无效"
RELOGIN_ERROR_FALLBACK = "relogin_failed"
RELOGIN_ERROR_CODES = frozenset(
    {
        "password_verify_failed_403",
        "rate_limit_exceeded",
        "unsupported_country_region_territory",
        "invalid_state",
        "invalid_password",
        "password_verify_invalid_response",
        "need_verification_code",
        "no_auth_code",
        "token_exchange_failed",
        "authorize_redirect_error",
    }
)
_PROGRESS_RETENTION_SECONDS = 15 * 60
_PROGRESS_UNFINISHED_RETENTION_SECONDS = 60 * 60
_PROGRESS_MAX_COMPLETED_ENTRIES = 128
_INVALID_ACCOUNT_TIMESTAMP = object()
_INVALID_LIMITS_PROGRESS = object()
_USE_ACCOUNT_NOW = object()


def _normalize_token_refresh_error_code(value: object) -> str | None:
    if value is None:
        return None
    code = value if isinstance(value, str) else ""
    code = code.strip()
    if not code:
        return None
    return code if code in TOKEN_REFRESH_ERROR_CODES else TOKEN_REFRESH_ERROR_FALLBACK


def _normalize_relogin_error_code(value: object) -> str:
    if not isinstance(value, str):
        return RELOGIN_ERROR_FALLBACK
    code = value.strip()
    return code if code in RELOGIN_ERROR_CODES else RELOGIN_ERROR_FALLBACK


def _remaining_timeout(deadline: float | None, maximum: float) -> float:
    if deadline is None:
        return maximum
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise FuturesTimeoutError("account refresh timed out")
    return min(maximum, remaining)


def _strict_oauth_token_fields(value: object, *, fallback_refresh_token: str = "") -> dict[str, str] | None:
    if not isinstance(value, dict):
        return None
    access_token = value.get("access_token")
    refresh_token = value.get("refresh_token")
    id_token = value.get("id_token")
    if (
        not isinstance(access_token, str)
        or not access_token.strip()
        or (refresh_token is not None and not isinstance(refresh_token, str))
        or (id_token is not None and not isinstance(id_token, str))
    ):
        return None
    return {
        "access_token": access_token.strip(),
        "refresh_token": (refresh_token or fallback_refresh_token).strip(),
        "id_token": (id_token or "").strip(),
    }


def _prune_progress_locked(progress_store: dict[str, dict], *, now: float | None = None) -> None:
    current = time.monotonic() if now is None else now
    expired: list[str] = []
    completed: list[tuple[float, str]] = []
    for progress_id, progress in progress_store.items():
        if not isinstance(progress, dict):
            expired.append(progress_id)
            continue
        created_at = progress.get("_created_ts")
        if not isinstance(created_at, (int, float)) or isinstance(created_at, bool):
            created_at = current
            progress["_created_ts"] = created_at
        last_activity_at = progress.get("_last_activity_ts")
        if not isinstance(last_activity_at, (int, float)) or isinstance(last_activity_at, bool):
            last_activity_at = current
            progress["_last_activity_ts"] = last_activity_at
        finished_at = progress.get("_finished_ts")
        if isinstance(finished_at, (int, float)) and not isinstance(finished_at, bool):
            if current - finished_at >= _PROGRESS_RETENTION_SECONDS:
                expired.append(progress_id)
            else:
                completed.append((float(finished_at), progress_id))
        elif progress.get("done") is True:
            # Old in-memory entries had no finish timestamp. Treat the first
            # observation as completion time so they do not become immortal.
            finished_at = current
            progress["_finished_ts"] = finished_at
            completed.append((float(finished_at), progress_id))
        elif current - last_activity_at >= _PROGRESS_UNFINISHED_RETENTION_SECONDS:
            expired.append(progress_id)
    for progress_id in expired:
        progress_store.pop(progress_id, None)
    completed.sort()
    for _, progress_id in completed[: max(0, len(completed) - _PROGRESS_MAX_COMPLETED_ENTRIES)]:
        progress_store.pop(progress_id, None)


def _public_progress(progress: dict) -> dict:
    return {key: value for key, value in progress.items() if not str(key).startswith("_")}


class TokenRefreshError(RuntimeError):
    """Refresh failure with a bounded code and no upstream message."""

    def __init__(self, code: str) -> None:
        self.code = _normalize_token_refresh_error_code(code) or TOKEN_REFRESH_ERROR_FALLBACK
        super().__init__(self.code)


@dataclass(frozen=True)
class _WatcherRefreshOwner:
    generation: int | None
    operation_generation: int | None = None


@dataclass
class _RefreshOperationState:
    valid: bool = True
    references: int = 1


def _payload_has_error_code(payload: object, expected: str) -> bool:
    if not isinstance(payload, dict):
        return False
    candidates: list[object] = [
        payload.get("code"),
        payload.get("error_code"),
        payload.get("error_description"),
        payload.get("error"),
    ]
    nested_error = payload.get("error")
    if isinstance(nested_error, dict):
        candidates.extend(
            [nested_error.get("code"), nested_error.get("error_code"), nested_error.get("type")]
        )
    return any(isinstance(candidate, str) and candidate.strip().lower() == expected for candidate in candidates)


class AccountService:
    """账号池服务，使用 token -> account 的 dict 保存账号。"""

    _ACCOUNT_STATUSES = frozenset({"正常", "限流", "异常", "禁用"})
    _NEW_ACCOUNT_INVALID_GRACE_SECONDS = 10 * 60
    _INVALID_CONFIRM_SECONDS = 30
    _ACCESS_TOKEN_REFRESH_SKEW_SECONDS = 24 * 60 * 60
    _REFRESH_TOKEN_KEEPALIVE_SECONDS = 3 * 24 * 60 * 60
    _REFRESH_TOKEN_KEEPALIVE_ERROR_BACKOFF_SECONDS = 6 * 60 * 60
    _REFRESH_TOKEN_KEEPALIVE_BATCH_SIZE = 3
    _TOKEN_REFRESH_ERROR_BACKOFF_SECONDS = 5 * 60
    _OAUTH_TOKEN_URL = "https://auth.openai.com/oauth/token"
    _OAUTH_CLIENT_ID = "app_2SKx67EdpoN0G6j64rFvigXD"
    _OAUTH_USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    )

    # 刷新进度追踪
    _refresh_progress: dict[str, dict] = {}
    _refresh_progress_lock = Lock()
    # 重新登录进度追踪
    _relogin_progress: dict[str, dict] = {}
    _relogin_progress_lock = Lock()

    def __init__(self, storage_backend: StorageBackend):
        self.storage = storage_backend
        self._lock = Lock()
        # Progress belongs to one service lifecycle.  Keep it off the class so
        # separate API/service instances cannot overwrite the same progress id.
        self._refresh_progress = {}
        self._refresh_progress_lock = Lock()
        self._refresh_progress_generation = 0
        self._relogin_progress = {}
        self._relogin_progress_lock = Lock()
        self._relogin_progress_generation = 0
        self._relogin_progress_owners: dict[str, _WatcherRefreshOwner] = {}
        self._token_refresh_condition = Condition(self._lock)
        self._active_token_refreshes: set[str] = set()
        self._token_refresh_leases: dict[str, set[str]] = {}
        self._image_slot_condition = Condition(self._lock)
        self._watcher_generation = 0
        self._watcher_operation_generation = 0
        self._refresh_operations: dict[int, _RefreshOperationState] = {}
        self._index = 0
        self._accounts = self._load_accounts()
        self._image_inflight: dict[str, int] = {}
        self._token_aliases: dict[str, str] = {}
        self._cumulative_total = self._load_cumulative_total()

    def begin_watcher_refresh(self) -> _WatcherRefreshOwner:
        with getattr(self, "_lock", nullcontext()):
            self._watcher_generation += 1
            return _WatcherRefreshOwner(self._watcher_generation)

    def _begin_refresh_batch_owner(
        self,
        parent: _WatcherRefreshOwner | None,
    ) -> _WatcherRefreshOwner:
        with getattr(self, "_lock", nullcontext()):
            self._watcher_operation_generation = getattr(self, "_watcher_operation_generation", 0) + 1
            generation = parent.generation if parent is not None else getattr(self, "_watcher_generation", 0)
            operations = getattr(self, "_refresh_operations", None)
            if not isinstance(operations, dict):
                operations = {}
                self._refresh_operations = operations
            operations[self._watcher_operation_generation] = _RefreshOperationState()
            return _WatcherRefreshOwner(
                generation,
                self._watcher_operation_generation,
            )

    def _begin_relogin_batch_owner(self) -> _WatcherRefreshOwner:
        with getattr(self, "_lock", nullcontext()):
            self._watcher_operation_generation = getattr(self, "_watcher_operation_generation", 0) + 1
            operations = getattr(self, "_refresh_operations", None)
            if not isinstance(operations, dict):
                operations = {}
                self._refresh_operations = operations
            operations[self._watcher_operation_generation] = _RefreshOperationState()
            return _WatcherRefreshOwner(None, self._watcher_operation_generation)

    def _invalidate_refresh_batch_owner(self, owner: _WatcherRefreshOwner) -> None:
        operation_generation = owner.operation_generation
        if operation_generation is None:
            return
        with getattr(self, "_lock", nullcontext()):
            operations = getattr(self, "_refresh_operations", {})
            state = operations.get(operation_generation) if isinstance(operations, dict) else None
            if state is not None:
                state.valid = False
                if state.references <= 0:
                    operations.pop(operation_generation, None)

    def _retain_refresh_batch_owner(self, owner: _WatcherRefreshOwner) -> None:
        operation_generation = owner.operation_generation
        if operation_generation is None:
            return
        with getattr(self, "_lock", nullcontext()):
            operations = getattr(self, "_refresh_operations", {})
            state = operations.get(operation_generation) if isinstance(operations, dict) else None
            if state is not None:
                state.references += 1

    def _release_refresh_batch_owner(self, owner: _WatcherRefreshOwner) -> None:
        operation_generation = owner.operation_generation
        if operation_generation is None:
            return
        with getattr(self, "_lock", nullcontext()):
            operations = getattr(self, "_refresh_operations", {})
            state = operations.get(operation_generation) if isinstance(operations, dict) else None
            if state is None:
                return
            state.references -= 1
            if state.references <= 0:
                operations.pop(operation_generation, None)

    def _track_refresh_future(self, owner: _WatcherRefreshOwner, future: object) -> None:
        add_done_callback = getattr(future, "add_done_callback", None)
        if not callable(add_done_callback):
            return
        self._retain_refresh_batch_owner(owner)
        try:
            add_done_callback(lambda _future: self._release_refresh_batch_owner(owner))
        except Exception:
            self._release_refresh_batch_owner(owner)
            raise

    def invalidate_watcher_refresh(self, owner: _WatcherRefreshOwner) -> None:
        with getattr(self, "_lock", nullcontext()):
            if self._watcher_generation == owner.generation:
                self._watcher_generation += 1

    def invalidate_all_watcher_refreshes(self) -> None:
        with self._lock:
            self._watcher_generation += 1

    def _watcher_refresh_is_current(self, owner: _WatcherRefreshOwner | None) -> bool:
        if owner is None:
            return True
        with self._lock:
            return self._watcher_refresh_is_current_locked(owner)

    def _watcher_refresh_is_current_locked(self, owner: _WatcherRefreshOwner | None) -> bool:
        if owner is None:
            return True
        if owner.generation is not None and getattr(self, "_watcher_generation", 0) != owner.generation:
            return False
        operation_generation = owner.operation_generation
        if operation_generation is None:
            return True
        operations = getattr(self, "_refresh_operations", {})
        state = operations.get(operation_generation) if isinstance(operations, dict) else None
        return state is not None and state.valid

    @contextmanager
    def _token_refresh_slot(self, token: str, *, deadline: float | None = None) -> Iterator[bool]:
        waited = False
        with self._token_refresh_condition:
            while token in self._active_token_refreshes:
                waited = True
                if deadline is None:
                    self._token_refresh_condition.wait()
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not self._token_refresh_condition.wait(timeout=remaining):
                    if token in self._active_token_refreshes:
                        raise TimeoutError("token refresh deadline exceeded")
            self._active_token_refreshes.add(token)
            leases = getattr(self, "_token_refresh_leases", None)
            if not isinstance(leases, dict):
                leases = {}
                self._token_refresh_leases = leases
            leases[token] = {token}
        try:
            yield not waited
        finally:
            with self._token_refresh_condition:
                leases = getattr(self, "_token_refresh_leases", {})
                lease_tokens = leases.pop(token, {token}) if isinstance(leases, dict) else {token}
                for lease_token in lease_tokens:
                    self._active_token_refreshes.discard(lease_token)
                self._token_refresh_condition.notify_all()

    def _get_cumulative_file(self) -> Path:
        from services.config import DATA_DIR
        return DATA_DIR / ".cumulative_total"

    def _load_cumulative_total(self) -> int:
        current_count = len(self._accounts)
        load_persisted_total = getattr(self.storage, "load_cumulative_total", None)
        supports_cumulative_snapshot = bool(
            getattr(self.storage, "supports_cumulative_snapshot", False)
        )
        if supports_cumulative_snapshot and callable(load_persisted_total):
            try:
                persisted_total = load_persisted_total()
                if persisted_total is not None:
                    return max(current_count, int(persisted_total), 0)
            except Exception:
                # A malformed/unreadable unified metadata value must not be
                # masked by an unrelated legacy sidecar.
                return current_count
        try:
            f = self._get_cumulative_file()
            if f.exists():
                stored_total = int(read_checked_file_bytes(f, f.parent).decode("utf-8").strip())
                return max(current_count, stored_total, 0)
        except Exception:
            pass
        return current_count

    def _save_cumulative_total(self) -> None:
        try:
            path = self._get_cumulative_file()
            atomic_write_bytes(path, path.parent, str(self._cumulative_total).encode("utf-8"))
        except Exception:
            pass

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _decode_jwt_payload(token: str) -> dict:
        try:
            payload = str(token or "").split(".")[1]
            payload += "=" * ((4 - len(payload) % 4) % 4)
            import base64
            import json
            data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    @staticmethod
    def _parse_time(value: object) -> datetime | None:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except Exception:
            try:
                parsed = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except Exception:
                return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _timestamp_to_iso(value: object) -> str:
        try:
            ts = int(value)
        except (TypeError, ValueError):
            return ""
        tz = timezone(timedelta(hours=8))
        return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(tz).isoformat()

    def _load_accounts(self) -> dict[str, dict]:
        load_snapshot = getattr(self.storage, "load_accounts_snapshot", None)
        if callable(load_snapshot):
            snapshot = load_snapshot()
            if not isinstance(snapshot, StorageSnapshot):
                raise StorageDataError()
            accounts = snapshot.records
        else:
            accounts = self.storage.load_accounts()
            snapshot = make_storage_snapshot(accounts)
        if not isinstance(accounts, list):
            raise StorageDataError()
        normalized_accounts: dict[str, dict] = {}
        migrated_items: list[dict] = []
        seen_access_tokens: set[str] = set()
        migration_needed = False
        for item in accounts:
            if not isinstance(item, dict):
                raise StorageDataError()
            try:
                normalized = self._normalize_account(item, created_at_default=None)
            except Exception as exc:
                raise StorageDataError() from exc
            if normalized is None:
                raise StorageDataError()
            access_token = normalized.get("access_token")
            if not isinstance(access_token, str) or not access_token.strip():
                raise StorageDataError()
            if access_token in seen_access_tokens:
                raise StorageDataError()
            seen_access_tokens.add(access_token)
            migrated_item = dict(item)
            safe_invalid_error = normalized.get("last_refresh_error")
            if migrated_item.get("last_refresh_error") != safe_invalid_error:
                migrated_item["last_refresh_error"] = safe_invalid_error
                migration_needed = True
            safe_error = normalized.get("last_token_refresh_error")
            if migrated_item.get("last_token_refresh_error") != safe_error:
                migrated_item["last_token_refresh_error"] = safe_error
                migration_needed = True
            migrated_items.append(migrated_item)
            normalized_accounts[normalized["access_token"]] = normalized
        if migration_needed:
            save_if_revision = getattr(self.storage, "save_accounts_if_revision", None)
            if callable(save_if_revision) and isinstance(snapshot, StorageSnapshot):
                saved_snapshot = save_if_revision(snapshot, migrated_items)
                self._accounts_snapshot = (
                    saved_snapshot
                    if isinstance(saved_snapshot, StorageSnapshot)
                    else make_storage_snapshot(migrated_items)
                )
            else:
                self.storage.save_accounts(migrated_items)
        else:
            self._accounts_snapshot = snapshot
        return normalized_accounts

    def _save_accounts(self) -> None:
        accounts = list(self._accounts.values())
        expected_accounts = getattr(self, "_accounts_snapshot", None)
        save_if_revision = getattr(self.storage, "save_accounts_if_revision", None)
        if callable(save_if_revision) and isinstance(expected_accounts, StorageSnapshot):
            saved_snapshot = save_if_revision(expected_accounts, accounts)
            self._accounts_snapshot = (
                saved_snapshot
                if isinstance(saved_snapshot, StorageSnapshot)
                else make_storage_snapshot(accounts)
            )
        else:
            # Structural fakes used by integrations may not implement the CAS helper.
            self.storage.load_accounts()
            self.storage.save_accounts(accounts)
            self._accounts_snapshot = make_storage_snapshot(accounts)

    def _capture_account_state_locked(
        self,
    ) -> tuple[dict[str, dict], dict[str, str], dict[str, int], int, object]:
        return (
            {token: dict(account) for token, account in self._accounts.items()},
            dict(self._token_aliases),
            dict(self._image_inflight),
            self._index,
            getattr(self, "_accounts_snapshot", None),
        )

    def _restore_account_state_locked(
        self,
        state: tuple[dict[str, dict], dict[str, str], dict[str, int], int, object],
    ) -> None:
        (
            self._accounts,
            self._token_aliases,
            self._image_inflight,
            self._index,
            self._accounts_snapshot,
        ) = state

    def _save_accounts_with_rollback_locked(
        self,
        state: tuple[dict[str, dict], dict[str, str], dict[str, int], int, object],
    ) -> None:
        try:
            self._save_accounts()
        except BaseException:
            self._restore_account_state_locked(state)
            raise

    @staticmethod
    def _is_image_account_available(account: dict) -> bool:
        if not isinstance(account, dict):
            return False
        status = account.get("status")
        if not isinstance(status, str) or status.strip() != "正常":
            return False
        quota = account.get("quota")
        return type(quota) is int and quota > 0

    @classmethod
    def _account_matches_plan_type(cls, account: dict, plan_type: str | None = None) -> bool:
        if not plan_type:
            return True
        normalized_plan = cls._normalize_account_type(plan_type)
        normalized_account = cls._normalize_account_type(account.get("type"))
        if not normalized_plan or not normalized_account:
            return False
        return normalized_plan.lower() == normalized_account.lower()

    @classmethod
    def _account_matches_source_type(cls, account: dict, source_type: str | None = None) -> bool:
        if not source_type:
            return True
        return cls._normalize_source_type(account.get("source_type")) == cls._normalize_source_type(source_type)

    @classmethod
    def _account_matches_any_plan_type(cls, account: dict, plan_types: set[str] | tuple[str, ...] | None = None) -> bool:
        if not plan_types:
            return True
        normalized_account = cls._normalize_account_type(account.get("type"))
        normalized_plans = {
            normalized
            for plan_type in plan_types
            if (normalized := cls._normalize_account_type(plan_type))
        }
        return bool(normalized_account and normalized_account in normalized_plans)

    @staticmethod
    def _normalize_source_type(value: object) -> str:
        return str(value or "web").strip().lower() or "web"

    @staticmethod
    def _normalize_counter(item: dict, field: str) -> int | None:
        if field not in item:
            return 0
        value = item.get(field)
        if type(value) is not int or value < 0:
            return None
        return value

    @staticmethod
    def _normalize_account_type(value: object) -> str | None:
        if not isinstance(value, str):
            return None
        raw = value.strip()
        if not raw:
            return None
        key = raw.lower().replace("-", "_").replace(" ", "_")
        compact = key.replace("_", "")
        aliases = {
            "free": "free",
            "plus": "Plus",
            "pro": "Pro",
            "prolite": "ProLite",
            "team": "Team",
            "business": "Team",
            "enterprise": "Enterprise",
        }
        return aliases.get(compact) or aliases.get(key) or raw

    @classmethod
    def _account_type_from_token_claims(cls, item: dict) -> str | None:
        for token_field in ("access_token", "id_token"):
            token = item.get(token_field)
            if not isinstance(token, str) or not token.strip():
                continue
            payload = cls._decode_jwt_payload(token)
            auth_claim = payload.get("https://api.openai.com/auth")
            if not isinstance(auth_claim, dict):
                continue
            raw_plan_type = auth_claim.get("chatgpt_plan_type")
            if not isinstance(raw_plan_type, str):
                continue
            plan_type = cls._normalize_account_type(raw_plan_type)
            if plan_type:
                return plan_type
        return None

    def _search_account_type(self, payload: object) -> str | None:
        if isinstance(payload, dict):
            for key in ("plan_type", "account_plan", "account_type", "subscription_type", "type"):
                plan = self._normalize_account_type(payload.get(key))
                if plan:
                    return plan
            for value in payload.values():
                plan = self._search_account_type(value)
                if plan:
                    return plan
        elif isinstance(payload, list):
            for value in payload:
                plan = self._search_account_type(value)
                if plan:
                    return plan
        return None

    def _normalize_account(
        self,
        item: dict,
        *,
        created_at_default: str | None | object = _USE_ACCOUNT_NOW,
    ) -> dict | None:
        if not isinstance(item, dict):
            return None
        access_token = self._account_payload_token(item)
        if not access_token:
            return None
        normalized = dict(item)
        normalized.pop("accessToken", None)
        normalized["access_token"] = access_token

        if "type" not in item:
            account_type = "free"
        else:
            raw_type = item.get("type")
            if not isinstance(raw_type, str) or not raw_type.strip():
                return None
            account_type = raw_type.strip()
        normalized["type"] = account_type

        if "status" not in item:
            status = "正常"
        else:
            raw_status = item.get("status")
            if not isinstance(raw_status, str):
                return None
            status = raw_status.strip()
            if status not in self._ACCOUNT_STATUSES:
                return None
        normalized["status"] = status

        for field in ("quota", "success", "fail", "invalid_count"):
            counter = self._normalize_counter(item, field)
            if counter is None:
                return None
            normalized[field] = counter

        for field in (
            "email",
            "user_id",
            "default_model_slug",
            "restore_at",
            "source_type",
            "export_type",
            "password",
            "refresh_token",
            "id_token",
        ):
            value = item.get(field)
            if value is not None and not isinstance(value, str):
                return None
            normalized[field] = value.strip() if isinstance(value, str) and value.strip() else None
        if account_type.lower() == "codex":
            # `type=codex` is a legacy export marker, not a plan type.  Apply
            # this derived marker after copying optional persisted fields so a
            # missing raw `export_type` cannot erase it.
            normalized["export_type"] = "codex"
            normalized["type"] = "free"
        raw_proxy = item.get("proxy")
        if raw_proxy is not None and not isinstance(raw_proxy, str):
            return None
        normalized["proxy"] = raw_proxy.strip() if isinstance(raw_proxy, str) else ""
        source_type = normalized.get("source_type")
        if not source_type and str(normalized.get("export_type") or "").strip().lower() == "codex":
            source_type = "codex"
        normalized["source_type"] = self._normalize_source_type(source_type)
        limits_progress = self._normalize_limits_progress_snapshot(normalized.get("limits_progress"))
        if limits_progress is _INVALID_LIMITS_PROGRESS:
            return None
        normalized["limits_progress"] = limits_progress
        for field in (
            "last_used_at",
            "last_invalid_at",
            "last_refresh_error_at",
            "last_token_refresh_at",
            "last_token_refresh_error_at",
        ):
            timestamp = self._normalize_account_timestamp(item, field)
            if timestamp is _INVALID_ACCOUNT_TIMESTAMP:
                return None
            normalized[field] = timestamp
        normalized["last_refresh_error"] = (
            INVALID_TOKEN_ERROR_MESSAGE
            if normalized.get("last_refresh_error")
            else None
        )
        normalized["last_token_refresh_error"] = _normalize_token_refresh_error_code(
            normalized.get("last_token_refresh_error")
        )
        default_created_at = (
            AccountService._now()
            if created_at_default is _USE_ACCOUNT_NOW
            else created_at_default
        )
        created_at = self._normalize_account_timestamp(
            item,
            "created_at",
            default=default_created_at,
        )
        if created_at is _INVALID_ACCOUNT_TIMESTAMP:
            return None
        normalized["created_at"] = created_at
        return normalized

    @classmethod
    def _normalize_account_timestamp(
        cls,
        item: dict,
        field: str,
        *,
        default: str | None = None,
    ) -> str | None | object:
        if field not in item or item.get(field) is None:
            return default
        value = item.get(field)
        if not isinstance(value, str):
            return _INVALID_ACCOUNT_TIMESTAMP
        value = value.strip()
        if not value:
            return default
        if cls._parse_time(value) is None:
            return _INVALID_ACCOUNT_TIMESTAMP
        return value

    @staticmethod
    def _normalize_limits_progress_snapshot(value: object) -> list[dict[str, Any]] | object:
        if value is None:
            return []
        if not isinstance(value, list) or len(value) > 100:
            return _INVALID_LIMITS_PROGRESS
        normalized: list[dict[str, Any]] = []
        allowed_fields = {"feature_name", "remaining", "reset_after"}
        for item in value:
            if not isinstance(item, dict) or not item or set(item) - allowed_fields:
                return _INVALID_LIMITS_PROGRESS
            entry: dict[str, Any] = {}
            if "feature_name" in item:
                feature_name = item["feature_name"]
                if (
                    not isinstance(feature_name, str)
                    or not feature_name.strip()
                    or len(feature_name.strip()) > 256
                ):
                    return _INVALID_LIMITS_PROGRESS
                entry["feature_name"] = feature_name.strip()
            if "remaining" in item:
                remaining = item["remaining"]
                if type(remaining) is not int or remaining < 0:
                    return _INVALID_LIMITS_PROGRESS
                entry["remaining"] = remaining
            if "reset_after" in item:
                reset_after = item["reset_after"]
                if not isinstance(reset_after, str) or len(reset_after.strip()) > 256:
                    return _INVALID_LIMITS_PROGRESS
                entry["reset_after"] = reset_after.strip()
            if not entry:
                return _INVALID_LIMITS_PROGRESS
            normalized.append(entry)
        return normalized

    @staticmethod
    def _jwt_exp(access_token: str) -> int:
        try:
            return int(AccountService._decode_jwt_payload(access_token).get("exp") or 0)
        except (TypeError, ValueError):
            return 0

    @classmethod
    def _token_expires_in(cls, access_token: str) -> int | None:
        exp = cls._jwt_exp(access_token)
        if exp <= 0:
            return None
        return exp - int(time.time())

    @classmethod
    def _token_needs_refresh(cls, access_token: str, *, force: bool = False) -> bool:
        if force:
            return True
        remaining = cls._token_expires_in(access_token)
        return remaining is not None and remaining <= cls._ACCESS_TOKEN_REFRESH_SKEW_SECONDS

    @classmethod
    def _token_issued_at(cls, access_token: str) -> datetime | None:
        try:
            iat = int(cls._decode_jwt_payload(access_token).get("iat") or 0)
        except (TypeError, ValueError):
            return None
        if iat <= 0:
            return None
        return datetime.fromtimestamp(iat, tz=timezone.utc)

    @staticmethod
    def _safe_response_text(response: object, limit: int = 300) -> str:
        try:
            return str(getattr(response, "text", "") or "")[:limit]
        except Exception:
            return ""

    def _resolve_access_token_locked(self, access_token: str) -> str:
        token = str(access_token or "").strip()
        seen: set[str] = set()
        while token and token not in self._accounts and token in self._token_aliases and token not in seen:
            seen.add(token)
            token = self._token_aliases.get(token, token)
        return token

    def resolve_access_token(self, access_token: str) -> str:
        if not access_token:
            return ""
        with self._lock:
            return self._resolve_access_token_locked(access_token)

    def _get_account_for_token(self, access_token: str) -> tuple[str, dict | None]:
        with self._lock:
            resolved = self._resolve_access_token_locked(access_token)
            account = self._accounts.get(resolved)
            return resolved, dict(account) if account else None

    def _get_account_lease(self, access_token: str) -> tuple[str, dict | None]:
        """Return the live account object held by a request owner.

        Callers must not use the returned object outside an owner check.  A
        later account update replaces the dict, so identity comparison makes
        stale network results fail closed even when token aliases still exist.
        """
        with self._lock:
            resolved = self._resolve_access_token_locked(access_token)
            return resolved, self._accounts.get(resolved)

    def _account_lease_matches_locked(
        self,
        access_token: str,
        expected_account: dict | None,
    ) -> bool:
        if expected_account is None:
            return True
        token = str(access_token or "").strip()
        resolved = self._resolve_access_token_locked(token)
        return resolved == token and self._accounts.get(resolved) is expected_account

    def _canonical_account_lease_matches_locked(
        self,
        canonical_token: str,
        expected_account: dict | None,
    ) -> bool:
        if expected_account is None:
            return True
        token = str(canonical_token or "").strip()
        return bool(token) and self._accounts.get(token) is expected_account

    def _canonical_account_lease_matches(
        self,
        canonical_token: str,
        expected_account: dict | None,
    ) -> bool:
        with self._lock:
            return self._canonical_account_lease_matches_locked(canonical_token, expected_account)

    def _account_lease_matches(
        self,
        access_token: str,
        expected_account: dict | None,
    ) -> bool:
        with self._lock:
            return self._account_lease_matches_locked(access_token, expected_account)

    def _record_token_refresh_error(
        self,
        access_token: str,
        event: str,
        error_code: str,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        expected_account: dict | None = None,
    ) -> None:
        safe_code = _normalize_token_refresh_error_code(error_code) or TOKEN_REFRESH_ERROR_FALLBACK
        now = datetime.now(timezone.utc).isoformat()
        with getattr(self, "_lock", nullcontext()):
            if not self._watcher_refresh_is_current_locked(watcher_owner):
                return
            if not self._account_lease_matches_locked(access_token, expected_account):
                return
            resolved = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(resolved)
            if current is None:
                return
            state = self._capture_account_state_locked()
            next_item = dict(current)
            next_item["last_token_refresh_error"] = safe_code
            next_item["last_token_refresh_error_at"] = now
            account = self._normalize_account(next_item)
            if account is not None:
                self._accounts[resolved] = account
                self._save_accounts_with_rollback_locked(state)
        _account_log(
            "refresh_token 刷新 access_token 失败",
            {"source": event, "token": anonymize_token(access_token), "error": safe_code},
        )

    def _recent_token_refresh_error(self, account: dict) -> bool:
        last_error_at = self._parse_time(account.get("last_token_refresh_error_at"))
        if last_error_at is None:
            return False
        return (datetime.now(timezone.utc) - last_error_at).total_seconds() < self._TOKEN_REFRESH_ERROR_BACKOFF_SECONDS

    def _recent_refresh_token_keepalive_error(self, account: dict, now: datetime) -> bool:
        last_error_at = self._parse_time(account.get("last_token_refresh_error_at"))
        if last_error_at is None:
            return False
        return (now - last_error_at).total_seconds() < self._REFRESH_TOKEN_KEEPALIVE_ERROR_BACKOFF_SECONDS

    def _refresh_token_keepalive_anchor(self, account: dict) -> datetime | None:
        return (
            self._parse_time(account.get("last_token_refresh_at"))
            or self._token_issued_at(str(account.get("access_token") or ""))
            or self._parse_time(account.get("created_at"))
        )

    def _refresh_token_keepalive_due_at(self, account: dict, now: datetime) -> datetime | None:
        if not str(account.get("refresh_token") or "").strip():
            return None
        if account.get("status") == "禁用":
            return None
        if self._recent_refresh_token_keepalive_error(account, now):
            return None
        anchor = self._refresh_token_keepalive_anchor(account)
        if anchor is None:
            return now
        due_at = anchor + timedelta(seconds=self._REFRESH_TOKEN_KEEPALIVE_SECONDS)
        return due_at if due_at <= now else None

    def _request_access_token_refresh(
        self,
        refresh_token: str,
        account: dict | None = None,
        *,
        deadline: float | None = None,
    ) -> dict[str, str]:
        from curl_cffi import requests
        from services.proxy_service import proxy_settings

        session = None
        try:
            try:
                session = requests.Session(
                    **proxy_settings.build_session_kwargs(account=account, impersonate="chrome110", verify=True)
                )
            except Exception as exc:
                raise TokenRefreshError("network_error") from exc
            if deadline is not None:
                request_timeout = min(60.0, deadline - time.monotonic())
                if request_timeout <= 0:
                    raise TimeoutError("token refresh deadline exceeded")
            else:
                request_timeout = 60.0
            try:
                response = session.post(
                    self._OAUTH_TOKEN_URL,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/x-www-form-urlencoded",
                        "User-Agent": self._OAUTH_USER_AGENT,
                    },
                    data={
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                        "client_id": self._OAUTH_CLIENT_ID,
                    },
                    timeout=request_timeout,
                    stream=True,
                )
            except TimeoutError:
                raise
            except Exception as exc:
                raise TokenRefreshError("network_error") from exc
            status_code = response.status_code
            try:
                data = parse_json_response(
                    response,
                    "token refresh response",
                    max_bytes=_MAX_TOKEN_RESPONSE_BYTES,
                    require_ok=False,
                )
            except Exception as exc:
                raise TokenRefreshError("invalid_response") from exc
            if status_code != 200 or not isinstance(data, dict) or not data.get("access_token"):
                if _payload_has_error_code(data, "app_session_terminated"):
                    raise TokenRefreshError("app_session_terminated")
                if isinstance(status_code, int) and status_code >= 500:
                    raise TokenRefreshError("http_5xx")
                if isinstance(status_code, int) and 400 <= status_code < 500:
                    raise TokenRefreshError("http_4xx")
                raise TokenRefreshError("invalid_response")
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("token refresh deadline exceeded")
            token_fields = _strict_oauth_token_fields(data, fallback_refresh_token=refresh_token)
            if token_fields is None:
                raise TokenRefreshError("invalid_response")
            return token_fields
        except TimeoutError:
            raise
        except TokenRefreshError:
            raise
        except Exception as exc:
            raise TokenRefreshError(TOKEN_REFRESH_ERROR_FALLBACK) from exc
        finally:
            if session is not None:
                try:
                    session.close()
                except Exception:
                    pass

    def _apply_refreshed_tokens(
        self,
        old_access_token: str,
        token_data: dict,
        event: str,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        expected_account: dict | None = None,
        extra_updates: dict | None = None,
        result_state: dict[str, bool] | None = None,
    ) -> str:
        if result_state is not None:
            result_state["applied"] = False
        now = datetime.now(timezone.utc).isoformat()
        with self._image_slot_condition:
            if not self._watcher_refresh_is_current_locked(watcher_owner):
                return old_access_token
            if not self._account_lease_matches_locked(old_access_token, expected_account):
                return old_access_token
            old_token = self._resolve_access_token_locked(old_access_token)
            current = self._accounts.get(old_token)
            if current is None:
                return old_token
            state = self._capture_account_state_locked()
            if not isinstance(token_data, dict):
                return old_token
            raw_new_token = token_data.get("access_token")
            raw_refresh_token = token_data.get("refresh_token")
            raw_id_token = token_data.get("id_token")
            if (
                (raw_new_token is not None and not isinstance(raw_new_token, str))
                or (raw_refresh_token is not None and not isinstance(raw_refresh_token, str))
                or (raw_id_token is not None and not isinstance(raw_id_token, str))
            ):
                return old_token
            new_token = (raw_new_token or old_token).strip()
            if not new_token:
                return old_token

            next_item = dict(current)
            next_item["access_token"] = new_token
            if raw_refresh_token:
                next_item["refresh_token"] = raw_refresh_token.strip()
            if raw_id_token:
                next_item["id_token"] = raw_id_token.strip()
            next_item["last_token_refresh_at"] = now
            next_item["last_token_refresh_error"] = None
            next_item["last_token_refresh_error_at"] = None
            next_item["invalid_count"] = 0
            next_item["last_invalid_at"] = None
            next_item["last_refresh_error"] = None
            next_item["last_refresh_error_at"] = None
            if isinstance(extra_updates, dict):
                next_item.update(extra_updates)

            account = self._normalize_account(next_item)
            if account is None:
                return old_token

            rotated = new_token != old_token
            if rotated:
                self._accounts.pop(old_token, None)
                self._token_aliases[old_token] = new_token
                if old_token in self._active_token_refreshes:
                    self._active_token_refreshes.add(new_token)
                    leases = getattr(self, "_token_refresh_leases", {})
                    if isinstance(leases, dict):
                        for lease_tokens in leases.values():
                            if old_token in lease_tokens:
                                lease_tokens.add(new_token)
                                break
                old_inflight = int(self._image_inflight.pop(old_token, 0))
                if old_inflight:
                    self._image_inflight[new_token] = int(self._image_inflight.get(new_token, 0)) + old_inflight
            self._accounts[new_token] = account
            self._save_accounts_with_rollback_locked(state)
            if result_state is not None:
                result_state["applied"] = True
            self._image_slot_condition.notify_all()

        _account_log(
            "refresh_token 已刷新 access_token",
            {"source": event, "token": anonymize_token(new_token), "rotated": rotated},
        )
        return new_token

    def refresh_access_token(
        self,
        access_token: str,
        *,
        force: bool = False,
        event: str = "refresh_access_token",
        deadline: float | None = None,
        watcher_owner: _WatcherRefreshOwner | None = None,
        expected_account: dict | None = None,
    ) -> str:
        if not access_token:
            return ""
        resolved_token, lease_account = self._get_account_lease(access_token)
        lease_token = resolved_token or str(access_token).strip()
        if expected_account is not None:
            if not self._canonical_account_lease_matches(lease_token, expected_account):
                return ""
            lease_account = expected_account
        with self._token_refresh_slot(lease_token, deadline=deadline) as slot_owner:
            if watcher_owner is not None and not self._watcher_refresh_is_current(watcher_owner):
                return access_token
            if deadline is not None and time.monotonic() >= deadline:
                raise TimeoutError("token refresh deadline exceeded")
            if not slot_owner:
                resolved_after, account_after = self._get_account_for_token(access_token)
                if expected_account is not None and not self._canonical_account_lease_matches(
                    resolved_after,
                    expected_account,
                ):
                    return ""
                if account_after is None:
                    return access_token
                return str(account_after.get("access_token") or resolved_after or access_token)
            resolved_token, account = self._get_account_for_token(access_token)
            if lease_account is not None and not self._canonical_account_lease_matches(
                resolved_token,
                lease_account,
            ):
                return access_token
            if expected_account is not None:
                resolved_token, account_ref = self._get_account_lease(access_token)
                if not self._canonical_account_lease_matches(resolved_token, expected_account):
                    return ""
                account = dict(account_ref) if account_ref is not None else None
            if not account:
                return access_token
            active_token = str(account.get("access_token") or resolved_token or access_token)
            if not self._token_needs_refresh(active_token, force=force):
                return active_token
            refresh_token = str(account.get("refresh_token") or "").strip()
            if not refresh_token:
                return active_token
            if not force and self._recent_token_refresh_error(account):
                return active_token
            try:
                token_data = self._request_access_token_refresh(
                    refresh_token,
                    account,
                    deadline=deadline,
                )
            except TimeoutError:
                raise
            except TokenRefreshError as exc:
                self._record_token_refresh_error(
                    active_token,
                    event,
                    exc.code,
                    watcher_owner=watcher_owner,
                    expected_account=lease_account,
                )
                # 如果是 app_session_terminated 错误，尝试密码重新登录
                if (
                    exc.code == "app_session_terminated"
                    and self._watcher_refresh_is_current(watcher_owner)
                    and (deadline is None or time.monotonic() < deadline)
                ):
                    # 获取账号信息（email, password）
                    email = str(account.get("email") or "").strip()
                    password = str(account.get("password") or "").strip()
                    if email and password:
                        self._schedule_password_relogin(
                            active_token,
                            email,
                            password,
                            event,
                            watcher_owner=watcher_owner,
                            deadline=deadline,
                        )
                return active_token
            except Exception:
                self._record_token_refresh_error(
                    active_token,
                    event,
                    TOKEN_REFRESH_ERROR_FALLBACK,
                    watcher_owner=watcher_owner,
                    expected_account=lease_account,
                )
                return active_token
            if deadline is not None:
                _remaining_timeout(deadline, float("inf"))
            return self._apply_refreshed_tokens(
                active_token,
                token_data,
                event,
                watcher_owner=watcher_owner,
                expected_account=lease_account,
            )

    def _schedule_password_relogin(
        self,
        access_token: str,
        email: str,
        password: str,
        event: str,
        progress_id: str | None = None,
        watcher_owner: _WatcherRefreshOwner | None = None,
        deadline: float | None = None,
        expected_account: dict | None = None,
        progress_generation: int | None = None,
    ) -> bool:
        if deadline is not None:
            _remaining_timeout(deadline, float("inf"))
        if watcher_owner is not None and not self._watcher_refresh_is_current(watcher_owner):
            return False
        try:
            reservation = reserve_background_task()
        except BackgroundTaskQueueFullError:
            _account_log("重新登录任务未启动", {"source": event, "status": "queue_full"})
            if progress_id:
                self.update_relogin_progress(
                    progress_id,
                    access_token,
                    "跳过",
                    "relogin_queue_full",
                    watcher_owner=watcher_owner,
                    progress_generation=progress_generation,
                )
            return False
        try:
            if deadline is not None:
                _remaining_timeout(deadline, float("inf"))
            submit_args = (access_token, email, password, event, progress_id)
            submit_kwargs = {"deadline": deadline} if deadline is not None else {}
            if watcher_owner is not None:
                submit_kwargs["watcher_owner"] = watcher_owner
            if expected_account is not None:
                submit_kwargs["expected_account"] = expected_account
            if progress_generation is not None:
                submit_kwargs["progress_generation"] = progress_generation
            future = reservation.submit(self._password_re_login_thread, *submit_args, **submit_kwargs)
            if watcher_owner is not None:
                self._track_refresh_future(watcher_owner, future)
        except TimeoutError:
            reservation.cancel()
            raise
        except Exception:
            try:
                reservation.cancel()
            except BaseException:
                pass
            _account_log("重新登录任务未启动", {"source": event, "status": "submit_failed"})
            if progress_id:
                self.update_relogin_progress(
                    progress_id,
                    access_token,
                    "异常",
                    RELOGIN_ERROR_FALLBACK,
                    watcher_owner=watcher_owner,
                    progress_generation=progress_generation,
                )
            return False
        return True

    def _password_re_login_thread(
        self,
        access_token: str,
        email: str,
        password: str,
        event: str,
        progress_id: str | None = None,
        watcher_owner: _WatcherRefreshOwner | None = None,
        *,
        deadline: float | None = None,
        expected_account: dict | None = None,
        progress_generation: int | None = None,
    ) -> None:
        """密码重新登录线程入口"""
        def watcher_is_current() -> bool:
            return (
                (watcher_owner is None or self._watcher_refresh_is_current(watcher_owner))
                and (deadline is None or time.monotonic() < deadline)
            )

        def update_progress(status: str, error: str | None = None) -> None:
            if progress_id and watcher_is_current():
                self.update_relogin_progress(
                    progress_id,
                    access_token,
                    status,
                    error,
                    watcher_owner=watcher_owner,
                    progress_generation=progress_generation,
                )

        try:
            if not watcher_is_current():
                return
            result = self._login_with_password(email, password)
            if result.get("ok"):
                if not watcher_is_current():
                    return
                # 登录成功，更新账号
                new_access_token = result.get("access_token", "")
                new_refresh_token = result.get("refresh_token", "")
                new_id_token = result.get("id_token", "")
                new_expires_at = result.get("expires_at")

                # 构建 token_data 供 _apply_refreshed_tokens 使用
                token_data = {
                    "access_token": new_access_token,
                    "refresh_token": new_refresh_token,
                    "id_token": new_id_token,
                }
                apply_state: dict[str, bool] = {}

                # 使用 _apply_refreshed_tokens 更新账号（处理 token 别名）
                new_token = self._apply_refreshed_tokens(
                    access_token,
                    token_data,
                    f"{event}:password_relogin",
                    watcher_owner=watcher_owner,
                    expected_account=expected_account,
                    extra_updates={
                        "source_type": result.get("source_type", "password"),
                        "status": "正常",
                    },
                    result_state=apply_state,
                )

                if not apply_state.get("applied"):
                    return

                if watcher_is_current():
                    _account_log(
                        "更新账号",
                        {
                            "source": event,
                            "old_token": anonymize_token(access_token),
                            "new_token": anonymize_token(new_access_token),
                            "status": "成功",
                        },
                    )
                    update_progress("成功")
            else:
                # 登录失败
                error_type = _normalize_relogin_error_code(result.get("error"))
                if error_type == "password_verify_failed_403" and isinstance(result.get("detail"), dict):
                    detail_error = result["detail"].get("error", {})
                    if isinstance(detail_error, dict) and detail_error.get("code") == "account_deactivated":
                        # 账号已删除/停用 → 标记为禁用
                        if not watcher_is_current():
                            return
                        self.update_account(
                            access_token,
                            {"status": "禁用", "quota": 0},
                            quiet=True,
                            watcher_owner=watcher_owner,
                            expected_account=expected_account,
                        )
                        if watcher_is_current():
                            _account_log(
                                "账号已停用-标记禁用",
                                {
                                    "source": event,
                                    "token": anonymize_token(access_token),
                                },
                            )
                            update_progress("禁用")
                    else:
                        # 永久故障：将账号标记为异常（或自动移除）
                        if not watcher_is_current():
                            return
                        self.remove_invalid_token(
                            access_token,
                            f"{event}:password_relogin_failed",
                            quiet=True,
                            watcher_owner=watcher_owner,
                            expected_account=expected_account,
                        )
                        if watcher_is_current():
                            _account_log(
                                "更新账号",
                                {
                                    "source": event,
                                    "token": anonymize_token(access_token),
                                    "status": "失败",
                                    "error": error_type,
                                },
                            )
                            update_progress("异常", error_type)
                else:
                    # 永久故障：将账号标记为异常（或自动移除）
                    if not watcher_is_current():
                        return
                    self.remove_invalid_token(
                        access_token,
                        f"{event}:password_relogin_failed",
                        quiet=True,
                        watcher_owner=watcher_owner,
                        expected_account=expected_account,
                    )
                    if watcher_is_current():
                        _account_log(
                            "更新账号",
                            {
                                "source": event,
                                "token": anonymize_token(access_token),
                                "status": "失败",
                                "error": error_type,
                            },
                        )
                        update_progress("异常", error_type)
        except Exception as exc:
            # 将账号标记为异常（或自动移除）
            if not watcher_is_current():
                return
            self.remove_invalid_token(
                access_token,
                f"{event}:password_relogin_exception",
                quiet=True,
                watcher_owner=watcher_owner,
                expected_account=expected_account,
            )
            if watcher_is_current():
                error_message = exception_log_message(exc)
                _account_log(
                    "更新账号",
                    {
                        "source": event,
                        "token": anonymize_token(access_token),
                        "status": "异常",
                        "error": error_message,
                    },
                )
                update_progress("异常", error_message)

    def _login_with_password(self, email: str, password: str) -> dict:
        """通过邮箱+密码登录，返回 {access_token, refresh_token, id_token, ...}"""
        from curl_cffi import requests
        
        # 常量
        auth_base = "https://auth.openai.com"
        platform_oauth_audience = "https://api.openai.com/v1"
        platform_auth0_client = "eyJuYW1lIjoiYXV0aDAtc3BhLWpzIiwidmVyc2lvbiI6IjEuMjEuMCJ9"
        platform_oauth_client_id = self._OAUTH_CLIENT_ID
        platform_oauth_redirect_uri = "https://platform.openai.com/auth/callback"
        user_agent = self._OAUTH_USER_AGENT
        
        # 创建 session
        session_kwargs = {"impersonate": "chrome110", "verify": True}
        proxy = config.get_proxy_settings()
        if proxy:
            session_kwargs["proxy"] = proxy
        session = requests.Session(**session_kwargs)
        
        try:
            device_id = str(uuid.uuid4())
            
            # ─── 方式2: OAuth authorize 流程 ──────────────────────────
            # 使用 Platform Client + PKCE
            
            from utils.pkce import generate_pkce
            code_verifier, code_challenge = generate_pkce()
            
            # ② 发起 OAuth authorize 请求 (使用 Platform Client + PKCE)
            session.cookies.set("oai-did", device_id, domain=".auth.openai.com")
            session.cookies.set("oai-did", device_id, domain="auth.openai.com")
            params = {
                "issuer": auth_base,
                "client_id": platform_oauth_client_id,
                "audience": platform_oauth_audience,
                "redirect_uri": platform_oauth_redirect_uri,
                "device_id": device_id,
                "screen_hint": "login_or_signup",
                "max_age": "0",
                "login_hint": email,
                "scope": "openid profile email offline_access",
                "response_type": "code",
                "response_mode": "query",
                "state": secrets.token_urlsafe(32),
                "nonce": secrets.token_urlsafe(32),
                "code_challenge": code_challenge,
                "code_challenge_method": "S256",
                "auth0Client": platform_auth0_client,
            }
            authorize_url = f"{auth_base}/api/accounts/authorize?{urlencode(params)}"
            resp = session.get(
                authorize_url,
                headers={
                    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
                    "user-agent": user_agent,
                    "sec-ch-ua": '"Chromium";v="145", "Google Chrome";v="145", "Not/A)Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "document",
                    "sec-fetch-mode": "navigate",
                    "sec-fetch-site": "cross-site",
                    "sec-fetch-user": "?1",
                    "upgrade-insecure-requests": "1",
                    "referer": "https://platform.openai.com/",
                },
                allow_redirects=True,
                timeout=30,
                stream=True,
            )
            try:
                status_code = resp.status_code if isinstance(resp.status_code, int) else 0
                final_url = str(resp.url)
            finally:
                close_response(resp)

            if status_code not in (200, 302):
                return {
                    "ok": False,
                    "error": f"authorize_failed_{status_code}",
                    "detail": {"status": status_code},
                }

            # 检测最终 URL 是否指向错误页面
            if "/error" in final_url and "payload=" in final_url:
                from urllib.parse import parse_qs, urlparse
                try:
                    parsed_query = parse_qs(urlparse(final_url).query)
                    error_payload_b64 = parsed_query.get("payload", [""])[0]
                    if len(error_payload_b64) > _MAX_TOKEN_RESPONSE_BYTES * 2:
                        raise ValueError("authorize error payload is too large")
                    error_payload_b64 += "=" * ((4 - len(error_payload_b64) % 4) % 4)
                    error_payload = json.loads(base64.b64decode(error_payload_b64))
                    if not isinstance(error_payload, dict):
                        raise ValueError("authorize error payload is invalid")
                    error_code = error_payload.get("errorCode")
                    error_code = error_code if isinstance(error_code, str) else ""
                    if error_code == "rate_limit_exceeded":
                        return {"ok": False, "error": "rate_limit_exceeded", "detail": {}}
                    return {"ok": False, "error": "authorize_redirect_error", "detail": {}}
                except Exception:
                    return {
                        "ok": False,
                        "error": "authorize_redirect_error",
                        "detail": {"parse_error": "invalid authorize redirect payload"},
                    }
            
            # ③ 提交密码验证
            login_headers = {
                "accept": "application/json",
                "accept-language": "zh-CN,zh;q=0.9",
                "content-type": "application/json",
                "origin": auth_base,
                "priority": "u=1, i",
                "user-agent": user_agent,
                "sec-ch-ua": '"Chromium";v="145", "Google Chrome";v="145", "Not/A)Brand";v="99"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "referer": f"{auth_base}/email-verification",
                "oai-device-id": device_id,
            }
            
            # 添加 sentinel token
            try:
                from utils.sentinel import build_sentinel_token
                sentinel_val, oai_sc_val = build_sentinel_token(session, device_id, "password_verify")
                login_headers["openai-sentinel-token"] = sentinel_val
                if oai_sc_val:
                    session.cookies.set("oai-sc", oai_sc_val, domain=".openai.com")
            except Exception:
                pass
            
            login_resp = session.post(
                f"{auth_base}/api/accounts/password/verify",
                headers=login_headers,
                json={"password": password},
                timeout=30,
                stream=True,
            )
            login_status_code = login_resp.status_code
            try:
                login_data = parse_json_response(
                    login_resp,
                    "password verification response",
                    max_bytes=_MAX_TOKEN_RESPONSE_BYTES,
                    require_ok=False,
                )
            except Exception:
                login_data = {}

            if login_status_code != 200:
                error_payload = (
                    login_data.get("error")
                    if isinstance(login_data, dict)
                    else None
                )
                if not isinstance(error_payload, dict):
                    return {
                        "ok": False,
                        "error": f"password_verify_failed_{login_status_code}",
                        "detail": {},
                    }
                error_code = error_payload.get("code")
                error_code = error_code.strip() if isinstance(error_code, str) else ""
                error_msg = error_payload.get("message")
                error_msg = error_msg if isinstance(error_msg, str) else ""
                safe_detail = {"error": {"code": error_code}} if error_code else {}
                if error_code == "unsupported_country_region_territory":
                    return {"ok": False, "error": "unsupported_country_region_territory", "detail": safe_detail}
                elif error_code == "invalid_state":
                    return {"ok": False, "error": "invalid_state", "detail": safe_detail}
                elif "Invalid credentials" in error_msg or "wrong password" in error_msg.lower():
                    return {"ok": False, "error": "invalid_password", "detail": safe_detail}
                return {"ok": False, "error": f"password_verify_failed_{login_status_code}", "detail": safe_detail}
            
            if not isinstance(login_data, dict):
                return {
                    "ok": False,
                    "error": "password_verify_invalid_response",
                    "detail": {},
                }

            # 获取 authorization code
            continue_url = str(login_data.get("continue_url") or "").strip()
            auth_code = ""
            if continue_url:
                from urllib.parse import parse_qs, urlparse
                parsed_params = parse_qs(urlparse(continue_url).query)
                auth_code = str((parsed_params.get("code") or [""])[0]).strip()
            
            # ─── 处理邮箱 OTP 验证 ──────────────────────────
            if not auth_code:
                page_type = ""
                page_info = login_data.get("page")
                if isinstance(page_info, dict):
                    page_type = str(page_info.get("type") or "")
                
                if page_type == "email_otp_verification":
                    # 需要验证码才能登录，直接标记为账号异常
                    return {"ok": False, "error": "need_verification_code", "detail": {}}
                else:
                    return {"ok": False, "error": "no_auth_code", "detail": {}}
            
            # ④ 用 code 换 token (使用 Platform Client + code_verifier)
            platform_base = "https://platform.openai.com"
            token_resp = session.post(
                f"{auth_base}/api/accounts/oauth/token",
                headers={
                    "accept": "*/*",
                    "accept-language": "zh-CN,zh;q=0.9",
                    "auth0-client": platform_auth0_client,
                    "cache-control": "no-cache",
                    "content-type": "application/json",
                    "origin": platform_base,
                    "pragma": "no-cache",
                    "priority": "u=1, i",
                    "referer": f"{platform_base}/",
                    "sec-ch-ua": '"Chromium";v="145", "Google Chrome";v="145", "Not/A)Brand";v="99"',
                    "sec-ch-ua-mobile": "?0",
                    "sec-ch-ua-platform": '"Windows"',
                    "sec-fetch-dest": "empty",
                    "sec-fetch-mode": "cors",
                    "sec-fetch-site": "same-site",
                    "user-agent": user_agent,
                },
                json={
                    "client_id": platform_oauth_client_id,
                    "code_verifier": code_verifier,
                    "grant_type": "authorization_code",
                    "code": auth_code,
                    "redirect_uri": platform_oauth_redirect_uri,
                },
                verify=True,
                timeout=60,
                stream=True,
            )
            token_status_code = token_resp.status_code
            try:
                token_data = parse_json_response(
                    token_resp,
                    "password token exchange response",
                    max_bytes=_MAX_TOKEN_RESPONSE_BYTES,
                    require_ok=False,
                )
            except Exception:
                token_data = {}

            token_fields = _strict_oauth_token_fields(token_data)
            if token_status_code != 200 or token_fields is None:
                return {"ok": False, "error": "token_exchange_failed", "detail": {}}

            access_token = token_fields["access_token"]
            refresh_token = token_fields["refresh_token"]
            id_token = token_fields["id_token"]
            
            # ⑤ 用 access_token 获取用户信息
            user_info = {}
            try:
                me_resp = session.get(
                    "https://chatgpt.com/backend-api/me",
                    headers={
                        "accept": "application/json",
                        "authorization": f"Bearer {access_token}",
                        "user-agent": user_agent,
                    },
                    timeout=30,
                    stream=True,
                )
                me_status_code = me_resp.status_code
                if me_status_code == 200:
                    user_info = parse_json_response(
                        me_resp,
                        "account profile response",
                        max_bytes=_MAX_TOKEN_RESPONSE_BYTES,
                        require_ok=False,
                    )
                else:
                    close_response(me_resp)
            except Exception:
                close_response(locals().get("me_resp"))
            
            # 解析 JWT payload
            jwt_payload = self._decode_jwt_payload(access_token)
            if not isinstance(jwt_payload, dict):
                jwt_payload = {}

            profile_claim = jwt_payload.get("https://api.openai.com/profile")
            profile_claim = profile_claim if isinstance(profile_claim, dict) else {}
            auth_claim = jwt_payload.get("https://api.openai.com/auth")
            auth_claim = auth_claim if isinstance(auth_claim, dict) else {}
            email_from_jwt = profile_claim.get("email")
            email_from_jwt = email_from_jwt.strip() if isinstance(email_from_jwt, str) else ""
            account_id_from_jwt = auth_claim.get("chatgpt_account_id")
            account_id_from_jwt = account_id_from_jwt.strip() if isinstance(account_id_from_jwt, str) else ""

            account_info = user_info.get("account") if isinstance(user_info, dict) else {}
            account_info = account_info if isinstance(account_info, dict) else {}
            result = {
                "ok": True,
                "email": email_from_jwt or email,
                "account_id": account_id_from_jwt or account_info.get("account_id", ""),
                "access_token": access_token,
                "refresh_token": refresh_token,
                "id_token": id_token,
                "expires_at": jwt_payload.get("exp"),
                "source_type": "password",
            }
            
            return result
        
        finally:
            try:
                session.close()
            except Exception:
                # Session cleanup must not replace a completed login result or
                # the original authentication failure.
                pass

    def list_expiring_access_tokens(self) -> list[str]:
        with self._lock:
            return [
                token
                for account in self._accounts.values()
                if str(account.get("refresh_token") or "").strip()
                and (token := str(account.get("access_token") or "").strip())
                and self._token_needs_refresh(token)
            ]

    def list_refresh_token_keepalive_tokens(self) -> list[str]:
        now = datetime.now(timezone.utc)
        due_items: list[tuple[datetime, str]] = []
        with self._lock:
            for account in self._accounts.values():
                due_at = self._refresh_token_keepalive_due_at(account, now)
                token = str(account.get("access_token") or "").strip()
                if due_at is not None and token:
                    due_items.append((due_at, token))
        due_items.sort(key=lambda item: item[0])
        return [token for _, token in due_items[: self._REFRESH_TOKEN_KEEPALIVE_BATCH_SIZE]]

    def keepalive_refresh_tokens(
        self,
        access_tokens: list[str],
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
    ) -> dict[str, Any]:
        access_tokens = list(dict.fromkeys(token for token in access_tokens if token))
        if not access_tokens:
            return {"refreshed": 0, "errors": [], "items": self.list_accounts()}

        refreshed = 0
        errors = []
        for access_token in access_tokens:
            before = self.resolve_access_token(access_token)
            after = self.refresh_access_token(
                before,
                force=True,
                event="refresh_token_keepalive",
                watcher_owner=watcher_owner,
            )
            account = self.get_account(after)
            error_code = _normalize_token_refresh_error_code(account.get("last_token_refresh_error")) if account else None
            if error_code:
                errors.append({
                    "token": anonymize_token(before),
                    "error": error_code,
                })
                continue
            if account:
                refreshed += 1

        return {
            "refreshed": refreshed,
            "errors": errors,
            "items": self.list_accounts(),
            "relogined": 0,
        }

    def list_tokens(self) -> list[str]:
        with self._lock:
            return list(self._accounts)

    def _list_ready_candidate_tokens(
            self,
            excluded_tokens: set[str] | None = None,
            plan_type: str | None = None,
            source_type: str | None = None,
            plan_types: set[str] | tuple[str, ...] | None = None,
    ) -> list[str]:
        excluded = set(excluded_tokens or set())
        return [
            token
            for item in self._accounts.values()
            if self._is_image_account_available(item)
               and self._account_matches_plan_type(item, plan_type)
               and self._account_matches_any_plan_type(item, plan_types)
               and self._account_matches_source_type(item, source_type)
               and (token := item.get("access_token") or "")
               and token not in excluded
        ]

    def _list_available_candidate_tokens(
            self,
            excluded_tokens: set[str] | None = None,
            plan_type: str | None = None,
            source_type: str | None = None,
            plan_types: set[str] | tuple[str, ...] | None = None,
    ) -> list[str]:
        max_concurrency = max(1, int(config.image_account_concurrency or 1))
        return [
            token
            for token in self._list_ready_candidate_tokens(excluded_tokens, plan_type, source_type, plan_types)
            if int(self._image_inflight.get(token, 0)) < max_concurrency
        ]

    def _acquire_next_candidate_token(
            self,
            excluded_tokens: set[str] | None = None,
            plan_type: str | None = None,
            source_type: str | None = None,
            plan_types: set[str] | tuple[str, ...] | None = None,
    ) -> str:
        with self._image_slot_condition:
            while True:
                if not self._list_ready_candidate_tokens(excluded_tokens, plan_type, source_type, plan_types):
                    raise RuntimeError(
                        f"no available {plan_type or source_type or ''} image quota".replace("  ", " ").strip()
                        if plan_type or source_type else "no available image quota"
                    )
                tokens = self._list_available_candidate_tokens(excluded_tokens, plan_type, source_type, plan_types)
                if tokens:
                    access_token = tokens[self._index % len(tokens)]
                    self._index += 1
                    self._image_inflight[access_token] = int(self._image_inflight.get(access_token, 0)) + 1
                    return access_token
                self._image_slot_condition.wait(timeout=1.0)

    def release_image_slot(self, access_token: str) -> None:
        if not access_token:
            return
        with self._image_slot_condition:
            access_token = self._resolve_access_token_locked(access_token)
            current_inflight = int(self._image_inflight.get(access_token, 0))
            if current_inflight <= 1:
                self._image_inflight.pop(access_token, None)
            else:
                self._image_inflight[access_token] = current_inflight - 1
            self._image_slot_condition.notify_all()

    @staticmethod
    def _is_text_account_available(account: object) -> bool:
        return (
            isinstance(account, dict)
            and account.get("status") not in {"禁用", "限流", "异常"}
        )

    def get_available_access_token(
            self,
            plan_type: str | None = None,
            source_type: str | None = None,
            plan_types: set[str] | tuple[str, ...] | None = None,
    ) -> str:
        """从候选池中获取一个可用的图片生图 token。

        基于本地缓存做初筛，然后通过 fetch_remote_info 做远程验证（token 有效性、配额等）。
        限制最大尝试次数防止 token rotation 导致无限循环。
        """
        max_attempts = 20  # 防止无限循环
        attempted_tokens: set[str] = set()
        for _attempt in range(max_attempts):
            access_token = self._acquire_next_candidate_token(
                excluded_tokens=attempted_tokens,
                plan_type=plan_type,
                source_type=source_type,
                plan_types=plan_types,
            )
            attempted_tokens.add(access_token)
            try:
                account = self.fetch_remote_info(access_token, "get_available_access_token")
            except Exception:
                self.release_image_slot(access_token)
                continue
            # fetch_remote_info 内部可能因 token rotation 导致 access_token 变化，
            # 把新 token 也加入排除列表，防止重复尝试
            resolved = str((account or {}).get("access_token") or "")
            if resolved and resolved != access_token:
                attempted_tokens.add(resolved)
            if (
                    self._is_image_account_available(account or {})
                    and self._account_matches_plan_type(account or {}, plan_type)
                    and self._account_matches_any_plan_type(account or {}, plan_types)
                    and self._account_matches_source_type(account or {}, source_type)
            ):
                return str((account or {}).get("access_token") or access_token)
            self.release_image_slot(access_token)
        raise RuntimeError(
            f"no available {plan_type or source_type or ''} image quota (tried {len(attempted_tokens)} tokens)".replace("  ", " ").strip()
            if plan_type or source_type else f"no available image quota (tried {len(attempted_tokens)} tokens)"
        )

    def get_text_access_token(
            self,
            excluded_tokens: set[str] | None = None,
            model: str = "auto",
            source_type: str | None = None,
            plan_types: set[str] | tuple[str, ...] | None = None,
    ) -> str:
        excluded = set(excluded_tokens or set())
        requested_model = str(model or "auto").strip() or "auto"
        requested_source = self._normalize_source_type(source_type) if source_type else None
        route = None
        if requested_model != "auto":
            from services.model_service import model_catalog_service

            route = model_catalog_service.route_for_model(requested_model)
        with self._lock:
            candidates = [
                (token, account)
                for account in self._accounts.values()
                if self._is_text_account_available(account)
                   and (token := account.get("access_token") or "")
                   and (route is None or token in route.access_tokens)
                   and self._account_matches_source_type(account, requested_source)
                   and self._account_matches_any_plan_type(account, plan_types)
                   and token not in excluded
            ]
            if not candidates:
                if requested_source:
                    from services.model_service import ModelUnavailableError

                    raise ModelUnavailableError("no active account is available for the requested source")
                if plan_types:
                    from services.model_service import ModelUnavailableError

                    raise ModelUnavailableError("no active account matches the requested plan")
                if route is None or route.allow_anonymous:
                    return ""
                from services.model_service import ModelUnavailableError

                raise ModelUnavailableError(
                    f"model {requested_model!r} is not available to any active account"
                )
            access_token, expected_account = candidates[self._index % len(candidates)]
            self._index += 1
        resolved_token = (
            self.refresh_access_token(
                access_token,
                event="get_text_access_token",
                expected_account=expected_account,
            )
            or access_token
        )
        resolved_account = self.get_account(resolved_token)
        if (
            not self._is_text_account_available(resolved_account)
            or not self._account_matches_any_plan_type(resolved_account or {}, plan_types)
        ):
            from services.model_service import ModelUnavailableError

            raise ModelUnavailableError("selected account is no longer available")
        # A stale refresh may return the requested alias after its owner was
        # replaced.  Continue with the live account identity resolved above;
        # never hand the stale alias to the backend.
        resolved_token = str((resolved_account or {}).get("access_token") or resolved_token)
        if requested_model != "auto":
            refreshed_route = model_catalog_service.route_for_model(requested_model)
            if resolved_token not in refreshed_route.access_tokens:
                from services.model_service import ModelUnavailableError

                raise ModelUnavailableError("refreshed account does not advertise the requested model")
        if requested_source:
            if not self._account_matches_source_type(resolved_account or {}, requested_source):
                from services.model_service import ModelUnavailableError

                raise ModelUnavailableError("refreshed account does not match the requested source")
        return resolved_token

    def mark_text_used(
        self,
        access_token: str,
        *,
        expected_account: dict | None = None,
    ) -> None:
        if not access_token:
            return
        with self._lock:
            if not self._account_lease_matches_locked(access_token, expected_account):
                return
            access_token = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(access_token)
            if current is None:
                return
            next_item = dict(current)
            next_item["last_used_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            account = self._normalize_account(next_item)
            if account is None:
                return
            self._accounts[access_token] = account
            # This is hot-path telemetry, not an account mutation. Persisting
            # the full snapshot here serializes every completed text request
            # behind the account lock and storage backend. The in-memory value
            # is included by the next real account mutation.

    def remove_invalid_token(
        self,
        access_token: str,
        event: str,
        quiet: bool = False,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        expected_account: dict | None = None,
    ) -> bool:
        if watcher_owner is not None and not self._watcher_refresh_is_current(watcher_owner):
            return False
        if not config.auto_remove_invalid_accounts:
            update_kwargs = {"quiet": quiet, "watcher_owner": watcher_owner}
            if expected_account is not None:
                update_kwargs["expected_account"] = expected_account
            self.update_account(access_token, {"status": "异常", "quota": 0}, **update_kwargs)
            return False
        delete_kwargs = {"watcher_owner": watcher_owner}
        if expected_account is not None:
            delete_kwargs["expected_account"] = expected_account
        removed = bool(
            self.delete_accounts([access_token], **delete_kwargs)["removed"]
        )
        if removed and watcher_owner is None:
            _account_log(
                "自动移除异常账号",
                {"source": event, "token": anonymize_token(access_token)},
            )
        elif access_token:
            update_kwargs = {"quiet": quiet, "watcher_owner": watcher_owner}
            if expected_account is not None:
                update_kwargs["expected_account"] = expected_account
            self.update_account(access_token, {"status": "异常", "quota": 0}, **update_kwargs)
        return removed

    def get_account(self, access_token: str) -> dict | None:
        if not access_token:
            return None
        with self._lock:
            access_token = self._resolve_access_token_locked(access_token)
            account = self._accounts.get(access_token)
            return dict(account) if account else None

    def list_accounts(self) -> list[dict]:
        """返回所有账号的副本，并为每个账号附加当前图片在途数 image_inflight。

        image_inflight 为内存态并发计数(账号正在生成、尚未结束的图片数)。号池空闲时
        若某账号该值持续 > 0，说明其并发槽位泄漏、已被静默排除出调度，可借此在 UI 上诊断。
        """
        with self._lock:
            result = []
            for item in self._accounts.values():
                account = dict(item)
                token = account.get("access_token") or ""
                account["image_inflight"] = int(self._image_inflight.get(token, 0))
                result.append(account)
            return result

    def list_limited_tokens(self) -> list[str]:
        with self._lock:
            return [
                token
                for item in self._accounts.values()
                if item.get("status") == "限流"
                   and (token := item.get("access_token") or "")
            ]

    def list_normal_tokens(self) -> list[str]:
        with self._lock:
            return [
                token
                for item in self._accounts.values()
                if item.get("status") == "正常"
                   and (token := item.get("access_token") or "")
            ]

    @staticmethod
    def _account_payload_token(item: dict) -> str:
        value = item.get("access_token")
        if not isinstance(value, str):
            value = item.get("accessToken")
        return value.strip() if isinstance(value, str) else ""

    @classmethod
    def _account_identity_key(cls, item: dict) -> tuple[str, str] | None:
        if not isinstance(item, dict):
            return None

        access_payload = cls._decode_jwt_payload(cls._account_payload_token(item))
        id_payload = cls._decode_jwt_payload(str(item.get("id_token") or "").strip())
        access_auth = access_payload.get("https://api.openai.com/auth")
        access_auth = access_auth if isinstance(access_auth, dict) else {}
        id_auth = id_payload.get("https://api.openai.com/auth")
        id_auth = id_auth if isinstance(id_auth, dict) else {}
        access_profile = access_payload.get("https://api.openai.com/profile")
        access_profile = access_profile if isinstance(access_profile, dict) else {}
        id_profile = id_payload.get("https://api.openai.com/profile")
        id_profile = id_profile if isinstance(id_profile, dict) else {}

        # Account IDs identify workspaces; subject and email are fallbacks only.
        account_id = next(
            (
                value
                for value in (
                    str(access_auth.get("chatgpt_account_id") or "").strip(),
                    str(id_auth.get("chatgpt_account_id") or "").strip(),
                    str(item.get("account_id") or "").strip(),
                    str(item.get("chatgpt_account_id") or "").strip(),
                )
                if value
            ),
            "",
        )
        if account_id:
            return "account_id", account_id

        subject = next(
            (
                value
                for value in (
                    str(access_payload.get("sub") or "").strip(),
                    str(id_payload.get("sub") or "").strip(),
                    str(access_auth.get("user_id") or "").strip(),
                    str(id_auth.get("user_id") or "").strip(),
                    str(item.get("user_id") or "").strip(),
                )
                if value
            ),
            "",
        )
        if subject:
            return "subject", subject

        email = next(
            (
                value
                for value in (
                    str(access_profile.get("email") or "").strip(),
                    str(id_profile.get("email") or "").strip(),
                    str(access_payload.get("email") or "").strip(),
                    str(id_payload.get("email") or "").strip(),
                    str(item.get("email") or "").strip(),
                )
                if value
            ),
            "",
        )
        return ("email", email.casefold()) if email else None

    @classmethod
    def _access_token_rank(cls, token: str) -> tuple[int, int]:
        payload = cls._decode_jwt_payload(token)

        def as_int(value: object) -> int:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        return as_int(payload.get("exp")), as_int(payload.get("iat"))

    @staticmethod
    def _has_import_value(value: object) -> bool:
        return value is not None and (not isinstance(value, str) or bool(value.strip()))

    @classmethod
    def _merge_duplicate_payloads(cls, current: dict, incoming: dict) -> dict:
        current_token = cls._account_payload_token(current)
        incoming_token = cls._account_payload_token(incoming)
        incoming_wins = (
            incoming_token == current_token
            or cls._access_token_rank(incoming_token) >= cls._access_token_rank(current_token)
        )
        preferred, fallback = (incoming, current) if incoming_wins else (current, incoming)
        merged = dict(fallback)
        for key, value in preferred.items():
            if key not in merged or cls._has_import_value(value):
                merged[key] = value

        preferred_token = cls._account_payload_token(preferred)
        merged["access_token"] = preferred_token
        merged.pop("accessToken", None)

        dated_values = [
            (parsed, str(item.get("created_at") or "").strip())
            for item in (current, incoming)
            if (parsed := cls._parse_time(item.get("created_at"))) is not None
        ]
        if dated_values:
            merged["created_at"] = min(dated_values, key=lambda value: value[0])[1]
        return merged

    @classmethod
    def _prepare_account_payload(cls, item: dict) -> dict | None:
        if not isinstance(item, dict):
            return None
        access_token = AccountService._account_payload_token(item)
        if not access_token:
            return None
        payload = dict(item)
        payload.pop("accessToken", None)
        payload["access_token"] = access_token
        # CPA/Codex 导出文件里的 `type=codex` 是导出格式，不是号池套餐类型。
        if str(payload.get("type") or "").strip().lower() == "codex":
            payload["export_type"] = "codex"
            payload["source_type"] = "codex"
            payload.pop("type", None)
        if str(payload.get("export_type") or "").strip().lower() == "codex":
            payload["source_type"] = "codex"
        if not payload.get("type"):
            raw_plan_type = payload.get("plan_type")
            if isinstance(raw_plan_type, str):
                payload["type"] = cls._normalize_account_type(raw_plan_type)
        if not payload.get("type"):
            claimed_plan_type = cls._account_type_from_token_claims(payload)
            if claimed_plan_type:
                payload["type"] = claimed_plan_type
        return payload

    def add_account_items(self, items: list[dict]) -> dict:
        payloads = [
            payload
            for item in items
            if (payload := self._prepare_account_payload(item)) is not None
        ]
        return self._add_account_payloads(payloads)

    def add_accounts(self, tokens: list[str], source_type: str = "web") -> dict:
        tokens = list(dict.fromkeys(token for token in tokens if token))
        if not tokens:
            return {"added": 0, "skipped": 0, "items": self.list_accounts()}
        payloads = [
            payload
            for token in tokens
            if (
                payload := self._prepare_account_payload(
                    {"access_token": token, "source_type": self._normalize_source_type(source_type)}
                )
            ) is not None
        ]
        return self._add_account_payloads(payloads)

    def _add_account_payloads(self, payloads: list[dict]) -> dict:
        deduped: dict[tuple[str, str], dict] = {}
        batch_tokens: dict[tuple[str, str], set[str]] = {}
        valid_payloads = 0
        for payload in payloads:
            if not isinstance(payload, dict):
                continue
            access_token = self._account_payload_token(payload)
            if not access_token:
                continue
            valid_payloads += 1
            prepared = {**payload, "access_token": access_token}
            identity_key = self._account_identity_key(prepared)
            dedupe_key = identity_key or ("access_token", access_token)
            batch_tokens.setdefault(dedupe_key, set()).add(access_token)
            current = deduped.get(dedupe_key)
            if current is None:
                deduped[dedupe_key] = prepared
                continue

            merged = self._merge_duplicate_payloads(current, prepared)
            deduped[dedupe_key] = merged

        if not deduped:
            return {"added": 0, "skipped": 0, "items": self.list_accounts()}

        with self._lock:
            original_accounts = {token: dict(account) for token, account in self._accounts.items()}
            original_aliases = dict(self._token_aliases)
            original_image_inflight = dict(self._image_inflight)
            original_cumulative_total = self._cumulative_total
            original_accounts_snapshot = getattr(self, "_accounts_snapshot", None)
            added = 0
            skipped = max(0, valid_payloads - len(deduped))
            identity_index: dict[tuple[str, str], str] = {}
            for existing_token, existing_account in self._accounts.items():
                identity_key = self._account_identity_key(existing_account)
                if identity_key is None:
                    continue
                indexed_token = identity_index.get(identity_key)
                if (
                    indexed_token is None
                    or self._access_token_rank(existing_token) >= self._access_token_rank(indexed_token)
                ):
                    identity_index[identity_key] = existing_token

            for dedupe_key, payload in deduped.items():
                incoming_token = self._account_payload_token(payload)
                identity_key = self._account_identity_key(payload)
                current_token = incoming_token if incoming_token in self._accounts else None
                if current_token is None and identity_key is not None:
                    current_token = identity_index.get(identity_key)
                current = self._accounts.get(current_token) if current_token else None
                is_new_account = current is None
                if current is None:
                    current = {"created_at": self._now()}
                else:
                    skipped += 1

                incoming = self._merge_duplicate_payloads(current, payload)
                access_token = self._account_payload_token(incoming)
                if not incoming.get("created_at"):
                    incoming.pop("created_at", None)
                raw_type = incoming.get("type")
                account_type = (
                    "free"
                    if raw_type is None or (isinstance(raw_type, str) and not raw_type)
                    else raw_type
                )
                account = self._normalize_account(
                    {
                        **incoming,
                        "access_token": access_token,
                        "type": account_type,
                    }
                )
                if account is not None:
                    if is_new_account:
                        added += 1
                    if current_token and current_token != access_token:
                        self._accounts.pop(current_token, None)
                        self._token_aliases[current_token] = access_token
                        old_inflight = int(self._image_inflight.pop(current_token, 0))
                        if old_inflight:
                            current_inflight = int(self._image_inflight.get(access_token, 0))
                            self._image_inflight[access_token] = current_inflight + old_inflight
                    self._accounts[access_token] = account
                    if incoming_token != access_token:
                        self._token_aliases[incoming_token] = access_token
                    for duplicate_token in batch_tokens[dedupe_key]:
                        if duplicate_token != access_token:
                            self._token_aliases[duplicate_token] = access_token
                    if identity_key is not None:
                        identity_index[identity_key] = access_token
            next_accounts = list(self._accounts.values())
            save_accounts_with_total = getattr(self.storage, "save_accounts_with_cumulative_total", None)
            try:
                if (
                    added
                    and getattr(self.storage, "supports_cumulative_snapshot", False)
                    and callable(save_accounts_with_total)
                    and isinstance(
                        original_accounts_snapshot, StorageSnapshot
                    )
                ):
                    next_total = self._cumulative_total + added
                    saved_snapshot = save_accounts_with_total(
                        original_accounts_snapshot,
                        next_accounts,
                        next_total,
                    )
                    self._accounts_snapshot = (
                        saved_snapshot
                        if isinstance(saved_snapshot, StorageSnapshot)
                        else make_storage_snapshot(next_accounts)
                    )
                    self._cumulative_total = next_total
                else:
                    self._save_accounts()
                    if added:
                        self._cumulative_total += added
                        self._save_cumulative_total()
            except BaseException:
                self._accounts = original_accounts
                self._token_aliases = original_aliases
                self._image_inflight = original_image_inflight
                self._cumulative_total = original_cumulative_total
                self._accounts_snapshot = original_accounts_snapshot
                raise
            items = [dict(item) for item in self._accounts.values()]
            _account_log(
                f"新增 {added} 个账号，跳过 {skipped} 个",
                {"added": added, "skipped": skipped},
            )
        return {"added": added, "skipped": skipped, "items": items}

    def delete_accounts(
        self,
        tokens: list[str],
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        expected_account: dict | None = None,
    ) -> dict:
        target_set = set(token for token in tokens if token)
        if not target_set:
            return {"removed": 0, "items": self.list_accounts()}
        with self._lock:
            if not self._watcher_refresh_is_current_locked(watcher_owner):
                return {"removed": 0, "items": [dict(item) for item in self._accounts.values()]}
            if not self._account_lease_matches_locked(next(iter(target_set)), expected_account):
                return {"removed": 0, "items": [dict(item) for item in self._accounts.values()]}
            state = self._capture_account_state_locked()
            target_set = {self._resolve_access_token_locked(token) for token in target_set if token}
            removed = sum(self._accounts.pop(token, None) is not None for token in target_set)
            for token in target_set:
                self._image_inflight.pop(token, None)
            self._token_aliases = {
                old: new
                for old, new in self._token_aliases.items()
                if old not in target_set and new not in target_set
            }
            if removed:
                if self._accounts:
                    self._index %= len(self._accounts)
                else:
                    self._index = 0
                self._save_accounts_with_rollback_locked(state)
                _account_log(f"删除 {removed} 个账号", {"removed": removed})
            items = [dict(item) for item in self._accounts.values()]
        return {"removed": removed, "items": items}

    def update_account(
        self,
        access_token: str,
        updates: dict,
        quiet: bool = False,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        deadline: float | None = None,
        reset_refresh_state: bool = False,
        expected_account: dict | None = None,
    ) -> dict | None:
        if not access_token:
            return None
        with self._lock:
            if not self._watcher_refresh_is_current_locked(watcher_owner):
                return None
            if not self._account_lease_matches_locked(access_token, expected_account):
                return None
            if deadline is not None:
                _remaining_timeout(deadline, float("inf"))
            access_token = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(access_token)
            if current is None:
                return None
            state = self._capture_account_state_locked()
            merged = {**current, **updates, "access_token": access_token}
            if reset_refresh_state:
                if "invalid_count" not in updates:
                    merged["invalid_count"] = 0
                if "last_invalid_at" not in updates:
                    merged["last_invalid_at"] = None
                if "last_refresh_error" not in updates:
                    merged["last_refresh_error"] = None
                if "last_refresh_error_at" not in updates:
                    merged["last_refresh_error_at"] = None
            account = self._normalize_account(merged)
            if account is None:
                return None
            if account.get("status") == "限流" and config.auto_remove_rate_limited_accounts:
                self._accounts.pop(access_token, None)
                self._save_accounts_with_rollback_locked(state)
                _account_log("自动移除限流账号", {"token": anonymize_token(access_token)})
                return None
            self._accounts[access_token] = account
            self._save_accounts_with_rollback_locked(state)
            if not quiet:
                _account_log(
                    "更新账号",
                    {"token": anonymize_token(access_token), "status": account.get("status")},
                )
            return dict(account)
        return None

    def _should_defer_invalid_token(self, account: dict | None, now: datetime) -> bool:
        if not isinstance(account, dict):
            return False
        created_at = self._parse_time(account.get("created_at"))
        if created_at is not None and (now - created_at).total_seconds() < self._NEW_ACCOUNT_INVALID_GRACE_SECONDS:
            return True
        last_invalid_at = self._parse_time(account.get("last_invalid_at"))
        invalid_count = int(account.get("invalid_count") or 0)
        if invalid_count <= 1:
            return True
        if last_invalid_at is not None and (now - last_invalid_at).total_seconds() < self._INVALID_CONFIRM_SECONDS:
            return True
        return False

    def _record_invalid_token_seen(
        self,
        access_token: str,
        event: str,
        error: str,
        defer_invalid_removal: bool = True,
        watcher_owner: _WatcherRefreshOwner | None = None,
        expected_account: dict | None = None,
    ) -> dict | None:
        now = datetime.now(timezone.utc)
        with self._lock:
            if not self._watcher_refresh_is_current_locked(watcher_owner):
                return None
            if not self._account_lease_matches_locked(access_token, expected_account):
                return None
            access_token = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(access_token)
            if current is None:
                return None
            state = self._capture_account_state_locked()
            should_defer = defer_invalid_removal and self._should_defer_invalid_token(current, now)
            next_item = dict(current)
            next_item["invalid_count"] = int(next_item.get("invalid_count") or 0) + 1
            next_item["last_invalid_at"] = now.isoformat()
            next_item["last_refresh_error"] = INVALID_TOKEN_ERROR_MESSAGE
            next_item["last_refresh_error_at"] = now.isoformat()
            account = self._normalize_account(next_item)
            if account is None:
                return None
            self._accounts[access_token] = account
            self._save_accounts_with_rollback_locked(state)
            if should_defer:
                _account_log(
                    "暂缓标记异常账号",
                    {
                        "source": event,
                        "token": anonymize_token(access_token),
                        "reason": "invalid_access_token",
                    },
                )
                return None
            return account

    def mark_image_result(
        self,
        access_token: str,
        success: bool,
        *,
        expected_account: dict | None = None,
    ) -> dict | None:
        if not access_token:
            return None
        with self._lock:
            if not self._account_lease_matches_locked(access_token, expected_account):
                return None
            access_token = self._resolve_access_token_locked(access_token)
            current = self._accounts.get(access_token)
            if current is None:
                return None
            state = self._capture_account_state_locked()
            next_item = dict(current)
            next_item["last_used_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if success:
                next_item["success"] = int(next_item.get("success") or 0) + 1
                next_item["quota"] = max(0, int(next_item.get("quota") or 0) - 1)
                if next_item["quota"] == 0:
                    next_item["status"] = "限流"
                    next_item["restore_at"] = next_item.get("restore_at") or None
                elif next_item.get("status") == "限流":
                    next_item["status"] = "正常"
            else:
                next_item["fail"] = int(next_item.get("fail") or 0) + 1
            account = self._normalize_account(next_item)
            if account is None:
                return None
            if account.get("status") == "限流" and config.auto_remove_rate_limited_accounts:
                self._accounts.pop(access_token, None)
                self._save_accounts_with_rollback_locked(state)
                _account_log("自动移除限流账号", {"token": anonymize_token(access_token)})
                return None
            self._accounts[access_token] = account
            self._save_accounts_with_rollback_locked(state)
            return dict(account)
        return None

    def fetch_remote_info(
        self,
        access_token: str,
        event: str = "fetch_remote_info",
        defer_invalid_removal: bool = True,
        *,
        deadline: float | None = None,
        watcher_owner: _WatcherRefreshOwner | None = None,
        expected_account: dict | None = None,
    ) -> dict[str, Any] | None:
        if not access_token:
            raise ValueError("access_token is required")
        if watcher_owner is not None and not self._watcher_refresh_is_current(watcher_owner):
            return None

        active_token, active_account = self._get_account_lease(access_token)
        if active_account is None:
            return None
        if expected_account is not None and not self._canonical_account_lease_matches(
            active_token,
            expected_account,
        ):
            return None
        refresh_kwargs = {
            "event": f"{event}:preflight",
            "expected_account": active_account,
        }
        if deadline is not None:
            refresh_kwargs["deadline"] = deadline
        refresh_kwargs["watcher_owner"] = watcher_owner
        refreshed_token = self.refresh_access_token(access_token, **refresh_kwargs) or access_token
        resolved_after_refresh, refreshed_account = self._get_account_lease(refreshed_token)
        if resolved_after_refresh == active_token:
            if refreshed_account is not active_account:
                return None
        active_token = resolved_after_refresh
        active_account = refreshed_account
        if active_account is None:
            return None
        try:
            from services.openai_backend_api import InvalidAccessTokenError, OpenAIBackendAPI
            backend = OpenAIBackendAPI(active_token)
            try:
                result = backend.get_user_info(deadline=deadline) if deadline is not None else backend.get_user_info()
            finally:
                backend.close()
        except InvalidAccessTokenError as exc:
            refresh_kwargs = {"force": True, "event": f"{event}:invalid_access_token"}
            if deadline is not None:
                refresh_kwargs["deadline"] = deadline
            refresh_kwargs["watcher_owner"] = watcher_owner
            refresh_kwargs["expected_account"] = active_account
            refreshed_token = self.refresh_access_token(active_token, **refresh_kwargs)
            if refreshed_token and refreshed_token != active_token:
                active_token, active_account = self._get_account_lease(refreshed_token)
                if active_account is None:
                    raise exc
                try:
                    backend = OpenAIBackendAPI(refreshed_token)
                    try:
                        result = backend.get_user_info(deadline=deadline) if deadline is not None else backend.get_user_info()
                    finally:
                        backend.close()
                except InvalidAccessTokenError as retry_exc:
                    marked_account = self._record_invalid_token_seen(
                        refreshed_token,
                        event,
                        exception_log_message(retry_exc),
                        defer_invalid_removal=defer_invalid_removal,
                        watcher_owner=watcher_owner,
                        expected_account=active_account,
                    )
                    if marked_account is not None:
                        self.remove_invalid_token(
                            refreshed_token,
                            event,
                            watcher_owner=watcher_owner,
                            expected_account=marked_account,
                        )
                    raise
                active_token = refreshed_token
            else:
                marked_account = self._record_invalid_token_seen(
                    active_token,
                    event,
                    exception_log_message(exc),
                    defer_invalid_removal=defer_invalid_removal,
                    watcher_owner=watcher_owner,
                    expected_account=active_account,
                )
                if marked_account is not None:
                    self.remove_invalid_token(
                        active_token,
                        event,
                        watcher_owner=watcher_owner,
                        expected_account=marked_account,
                    )
                raise
        if deadline is not None:
            _remaining_timeout(deadline, float("inf"))
        claimed_plan_type = self._account_type_from_token_claims(
            {
                "access_token": active_token,
                "id_token": (self.get_account(active_token) or {}).get("id_token"),
            }
        )
        if claimed_plan_type:
            result = {**result, "type": claimed_plan_type}
        return self.update_account(
            active_token,
            result,
            watcher_owner=watcher_owner,
            deadline=deadline,
            reset_refresh_state=True,
            expected_account=active_account,
        )

    # ---- 刷新进度追踪 ----

    def init_refresh_progress(self, progress_id: str, total: int) -> int:
        """初始化刷新进度记录。"""
        with self._refresh_progress_lock:
            now = time.monotonic()
            _prune_progress_locked(self._refresh_progress, now=now)
            self._refresh_progress_generation = getattr(self, "_refresh_progress_generation", 0) + 1
            generation = self._refresh_progress_generation
            self._refresh_progress[progress_id] = {
                "total": total,
                "processed": 0,
                "done": False,
                "error": None,
                "status_counts": {"正常": 0, "限流": 0, "异常": 0, "禁用": 0},
                "total_quota": 0,
                "_created_ts": now,
                "_last_activity_ts": now,
                "_generation": generation,
            }
            return generation

    def update_refresh_progress(
        self,
        progress_id: str,
        token: str,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        progress_generation: int | None = None,
        expected_account: dict | None = None,
        account_snapshot: dict | None = None,
    ) -> None:
        """刷新单个账号后，更新进度计数。"""
        with getattr(self, "_lock", nullcontext()):
            if not self._watcher_refresh_is_current_locked(watcher_owner):
                return
            if account_snapshot is not None:
                account = account_snapshot
                account_replaced = False
            else:
                resolved = self._resolve_access_token_locked(token)
                account = self._accounts.get(resolved)
                account_replaced = expected_account is not None and account is not expected_account
            status = str(account.get("status") or "正常").strip() if account else "正常"
            quota = max(0, int(account.get("quota") or 0)) if account else 0

            with self._refresh_progress_lock:
                now = time.monotonic()
                _prune_progress_locked(self._refresh_progress, now=now)
                progress = self._refresh_progress.get(progress_id)
                if progress is None:
                    return
                if (
                    progress_generation is not None
                    and progress.get("_generation") != progress_generation
                ):
                    return
                progress["_last_activity_ts"] = now
                progress["processed"] += 1
                if account_replaced:
                    return
                progress["status_counts"][status] = progress["status_counts"].get(status, 0) + 1
                progress["total_quota"] += quota

    def finish_refresh_progress(
        self,
        progress_id: str,
        result: dict | None = None,
        error: str | None = None,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        progress_generation: int | None = None,
    ) -> None:
        """标记刷新完成。"""
        with getattr(self, "_lock", nullcontext()):
            if not self._watcher_refresh_is_current_locked(watcher_owner):
                return
            with self._refresh_progress_lock:
                now = time.monotonic()
                _prune_progress_locked(self._refresh_progress, now=now)
                progress = self._refresh_progress.get(progress_id)
                if progress is None:
                    return
                if (
                    progress_generation is not None
                    and progress.get("_generation") != progress_generation
                ):
                    return
                progress["done"] = True
                progress["result"] = result
                progress["_finished_ts"] = now
                progress["_last_activity_ts"] = now
                if error:
                    progress["error"] = error

    def get_refresh_progress(self, progress_id: str) -> dict | None:
        """查询刷新进度。"""
        with self._refresh_progress_lock:
            _prune_progress_locked(self._refresh_progress, now=time.monotonic())
            progress = self._refresh_progress.get(progress_id)
            return _public_progress(progress) if progress else None

    def clean_refresh_progress(self, progress_id: str) -> None:
        """清理过期进度记录。"""
        with self._refresh_progress_lock:
            self._refresh_progress.pop(progress_id, None)

    # ---- 重新登录进度追踪 ----

    def init_relogin_progress(
        self,
        progress_id: str,
        total: int,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
    ) -> int:
        """初始化重新登录进度记录。"""
        previous_owner = None
        with self._relogin_progress_lock:
            owners = getattr(self, "_relogin_progress_owners", {})
            if isinstance(owners, dict):
                previous_owner = owners.get(progress_id)
        if previous_owner is not None and previous_owner is not watcher_owner:
            self._invalidate_refresh_batch_owner(previous_owner)

        with self._relogin_progress_lock:
            now = time.monotonic()
            _prune_progress_locked(self._relogin_progress, now=now)
            self._relogin_progress_generation = getattr(self, "_relogin_progress_generation", 0) + 1
            generation = self._relogin_progress_generation
            owners = getattr(self, "_relogin_progress_owners", None)
            if not isinstance(owners, dict):
                owners = {}
                self._relogin_progress_owners = owners
            if watcher_owner is not None:
                owners[progress_id] = watcher_owner
            else:
                owners.pop(progress_id, None)
            self._relogin_progress[progress_id] = {
                "total": total,
                "processed": 0,
                "done": False,
                "error": None,
                "results": [],
                "_created_ts": now,
                "_last_activity_ts": now,
                "_generation": generation,
            }
            return generation

    def update_relogin_progress(
        self,
        progress_id: str,
        token: str,
        status: str,
        error: str | None = None,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        progress_generation: int | None = None,
    ) -> None:
        """更新单个重新登录进度。当所有账号处理完毕时自动标记完成。"""
        lock_context = self._lock if watcher_owner is not None else nullcontext()
        with lock_context:
            if watcher_owner is not None and not self._watcher_refresh_is_current_locked(watcher_owner):
                return
            with self._relogin_progress_lock:
                now = time.monotonic()
                _prune_progress_locked(self._relogin_progress, now=now)
                progress = self._relogin_progress.get(progress_id)
                if progress is None:
                    return
                if (
                    progress_generation is not None
                    and progress.get("_generation") != progress_generation
                ):
                    return
                progress["_last_activity_ts"] = now
                progress["processed"] += 1
                progress["results"].append({
                    "token": anonymize_token(token),
                    "status": status,
                    "error": error,
                })
                if progress["processed"] >= progress["total"]:
                    progress["done"] = True
                    progress["_finished_ts"] = now
                    owners = getattr(self, "_relogin_progress_owners", {})
                    if isinstance(owners, dict) and owners.get(progress_id) is watcher_owner:
                        owners.pop(progress_id, None)

    def finish_relogin_progress(
        self,
        progress_id: str,
        result: dict | None = None,
        error: str | None = None,
        *,
        watcher_owner: _WatcherRefreshOwner | None = None,
        progress_generation: int | None = None,
    ) -> None:
        """标记重新登录完成。"""
        owner_to_invalidate = watcher_owner
        with self._relogin_progress_lock:
            owners = getattr(self, "_relogin_progress_owners", {})
            if error and owner_to_invalidate is None and isinstance(owners, dict):
                owner_to_invalidate = owners.get(progress_id)
        if owner_to_invalidate is not None and error:
            self._invalidate_refresh_batch_owner(owner_to_invalidate)

        lock_context = self._lock if watcher_owner is not None else nullcontext()
        with lock_context:
            if watcher_owner is not None and not self._watcher_refresh_is_current_locked(watcher_owner):
                return
            with self._relogin_progress_lock:
                now = time.monotonic()
                _prune_progress_locked(self._relogin_progress, now=now)
                progress = self._relogin_progress.get(progress_id)
                if progress is None:
                    return
                if (
                    progress_generation is not None
                    and progress.get("_generation") != progress_generation
                ):
                    return
                progress["done"] = True
                progress["result"] = result
                progress["_finished_ts"] = now
                progress["_last_activity_ts"] = now
                if error:
                    progress["error"] = error
                if isinstance(owners, dict) and owners.get(progress_id) is owner_to_invalidate:
                    owners.pop(progress_id, None)

    def cancel_relogin_progress(self, progress_id: str, error: str = "重新登录任务已取消") -> None:
        self.finish_relogin_progress(progress_id, error=error)

    def get_relogin_progress(self, progress_id: str) -> dict | None:
        """查询重新登录进度。"""
        with self._relogin_progress_lock:
            now = time.monotonic()
            _prune_progress_locked(self._relogin_progress, now=now)
            progress = self._relogin_progress.get(progress_id)
            return _public_progress(progress) if progress else None

    def clean_relogin_progress(self, progress_id: str) -> None:
        """清理过期进度记录。"""
        owner = None
        with self._relogin_progress_lock:
            self._relogin_progress.pop(progress_id, None)
            owners = getattr(self, "_relogin_progress_owners", {})
            if isinstance(owners, dict):
                owner = owners.pop(progress_id, None)
        if owner is not None:
            self._invalidate_refresh_batch_owner(owner)

    def refresh_accounts(
        self,
        access_tokens: list[str],
        progress_id: str | None = None,
        defer_invalid_removal: bool = True,
        *,
        on_progress: Callable[[int], None] | None = None,
        deadline: float | None = None,
        watcher_owner: _WatcherRefreshOwner | None = None,
    ) -> dict[str, Any]:
        refresh_owner = self._begin_refresh_batch_owner(watcher_owner)
        owner_released = False
        owner_invalidated = False
        progress_generation: int | None = None
        current_futures: dict[object, tuple[str, dict | None]] = {}

        def invalidate_refresh_owner() -> None:
            nonlocal owner_invalidated
            if owner_invalidated:
                return
            self._invalidate_refresh_batch_owner(refresh_owner)
            owner_invalidated = True

        def release_refresh_owner_once() -> None:
            nonlocal owner_released
            if owner_released:
                return
            self._release_refresh_batch_owner(refresh_owner)
            owner_released = True

        def cancel_current_futures() -> None:
            for future in current_futures:
                try:
                    future.cancel()
                except BaseException:
                    pass

        def finish_refresh_failure(error: str) -> BaseException | None:
            cleanup_error: BaseException | None = None
            try:
                if progress_id and progress_generation is not None:
                    self.finish_refresh_progress(
                        progress_id,
                        error=error,
                        watcher_owner=refresh_owner,
                        progress_generation=progress_generation,
                    )
            except BaseException as exc:
                cleanup_error = exc
            finally:
                invalidate_refresh_owner()
            return cleanup_error

        try:
            with self._lock:
                access_tokens = list(dict.fromkeys(
                    self._resolve_access_token_locked(token)
                    for token in access_tokens
                    if token
                ))
            progress_generation = (
                self.init_refresh_progress(progress_id, len(access_tokens))
                if progress_id
                else None
            )

            if not access_tokens:
                items = self.list_accounts()
                result = {"refreshed": 0, "errors": [], "items": items, "relogined": 0}
                if progress_id and progress_generation is not None:
                    self.finish_refresh_progress(
                        progress_id,
                        result,
                        watcher_owner=refresh_owner,
                        progress_generation=progress_generation,
                    )
                return result

            refreshed = 0
            errors = []

            for offset in range(0, len(access_tokens), ACCOUNT_REFRESH_WORKERS):
                batch = access_tokens[offset:offset + ACCOUNT_REFRESH_WORKERS]
                current_futures = {}
                for token in batch:
                    _, expected_account = self._get_account_lease(token)
                    future = _ACCOUNT_REFRESH_EXECUTOR.submit(
                        self.fetch_remote_info,
                        token,
                        "refresh_accounts",
                        defer_invalid_removal,
                        **({"deadline": deadline} if deadline is not None else {}),
                        watcher_owner=refresh_owner,
                        expected_account=expected_account,
                    )
                    self._track_refresh_future(refresh_owner, future)
                    current_futures[future] = (token, expected_account)
                completed_futures = (
                    as_completed(current_futures)
                    if deadline is None
                    else as_completed(
                        current_futures,
                        # The import caller owns the single absolute deadline.
                        # Each network operation has its own 60/20 second cap;
                        # executor collection must consume the whole remaining
                        # budget instead of introducing a second 30-second cap.
                        timeout=_remaining_timeout(deadline, float("inf")),
                    )
                )
                for future in completed_futures:
                    token, expected_account = current_futures[future]
                    account: dict[str, Any] | None = None
                    try:
                        account = future.result()
                    except (KeyboardInterrupt, SystemExit):
                        raise
                    except Exception as exc:
                        error_str = str(exc)
                        # TLS/代理连接错误是网络问题，不计入账号失败
                        from services.protocol.conversation import is_tls_connection_error
                        if not is_tls_connection_error(error_str):
                            errors.append({"token": anonymize_token(token), "error": exception_log_message(exc)})
                    else:
                        if account is not None:
                            refreshed += 1

                    if progress_id and self._watcher_refresh_is_current(refresh_owner):
                        progress_kwargs = {
                            "watcher_owner": refresh_owner,
                            "progress_generation": progress_generation,
                        }
                        if account is None and expected_account is not None:
                            progress_kwargs["expected_account"] = expected_account
                        elif account is not None:
                            progress_kwargs["account_snapshot"] = account
                        self.update_refresh_progress(
                            progress_id,
                            token,
                            **progress_kwargs,
                        )
                    if on_progress is not None and self._watcher_refresh_is_current(refresh_owner):
                        on_progress(refreshed)

            # 自动重新登录异常账号（仅当配置开启时）。这段必须和批量刷新
            # 共用同一个 operation owner；其中任一 deadline/调度/结果写入
            # 异常都不能让 progress 或 owner 遗留在 running/valid。
            relogined = 0
            if deadline is not None:
                _remaining_timeout(deadline, float("inf"))
            if config.auto_relogin_after_refresh:
                for token in access_tokens:
                    if not self._watcher_refresh_is_current(refresh_owner):
                        break
                    if deadline is not None:
                        _remaining_timeout(deadline, float("inf"))
                    account = self.get_account(token)
                    if not account:
                        continue
                    if deadline is not None:
                        _remaining_timeout(deadline, float("inf"))
                    status = str(account.get("status") or "").strip()
                    if status != "异常":
                        continue
                    email = str(account.get("email") or "").strip()
                    password = str(account.get("password") or "").strip()
                    if not email or not password:
                        continue
                    if self._schedule_password_relogin(
                        token,
                        email,
                        password,
                        "auto_relogin_after_refresh",
                        watcher_owner=refresh_owner,
                        deadline=deadline,
                    ):
                        relogined += 1

            if deadline is not None:
                _remaining_timeout(deadline, float("inf"))

            result = {
                "refreshed": refreshed,
                "errors": errors,
                "items": self.list_accounts(),
                "relogined": relogined,
            }

            if progress_id and self._watcher_refresh_is_current(refresh_owner):
                self.finish_refresh_progress(
                    progress_id,
                    result,
                    watcher_owner=refresh_owner,
                    progress_generation=progress_generation,
                )
            return result
        except FuturesTimeoutError as exc:
            cancel_current_futures()
            cleanup_error = finish_refresh_failure("refresh timed out")
            timeout_error = TimeoutError("account refresh timed out")
            if cleanup_error is not None:
                raise timeout_error from cleanup_error
            raise timeout_error from exc
        except (KeyboardInterrupt, SystemExit) as exc:
            cancel_current_futures()
            cleanup_error = finish_refresh_failure("cancelled")
            if cleanup_error is not None:
                raise exc from cleanup_error
            raise
        except Exception as exc:
            cancel_current_futures()
            cleanup_error = finish_refresh_failure("refresh failed")
            if cleanup_error is not None:
                raise exc from cleanup_error
            raise
        except BaseException as exc:
            cancel_current_futures()
            cleanup_error = finish_refresh_failure("refresh failed")
            if cleanup_error is not None:
                raise exc from cleanup_error
            raise
        finally:
            release_refresh_owner_once()

    def re_login_accounts(self, access_tokens: list[str], progress_id: str | None = None) -> dict[str, Any]:
        """对选中账号执行密码重新登录流程。

        仅对包含 email + password 的账号有效。
        登录成功后自动将状态设为"正常"。
        """
        access_tokens = list(dict.fromkeys(token for token in access_tokens if token))
        if not access_tokens:
            result = {"relogined": 0, "skipped": 0, "errors": [], "items": self.list_accounts()}
            if progress_id:
                self.finish_relogin_progress(progress_id, result)
            return result

        relogin_owner = self._begin_relogin_batch_owner()
        progress_generation = (
            self.init_relogin_progress(
                progress_id,
                len(access_tokens),
                watcher_owner=relogin_owner,
            )
            if progress_id
            else None
        )
        try:
            relogined = 0
            skipped = 0
            errors = []

            for token in access_tokens:
                resolved_token, live_account = self._get_account_lease(token)
                if not live_account:
                    errors.append({"token": anonymize_token(token), "error": "账号不存在"})
                    if progress_id:
                        self.update_relogin_progress(
                            progress_id,
                            token,
                            "跳过",
                            "账号不存在",
                            watcher_owner=relogin_owner,
                            progress_generation=progress_generation,
                        )
                    continue

                account = dict(live_account)
                email = str(account.get("email") or "").strip()
                password = str(account.get("password") or "").strip()
                if not email or not password:
                    skipped += 1
                    if progress_id:
                        self.update_relogin_progress(
                            progress_id,
                            resolved_token,
                            "跳过",
                            "无邮箱密码",
                            watcher_owner=relogin_owner,
                            progress_generation=progress_generation,
                        )
                    continue

                if self._schedule_password_relogin(
                    resolved_token,
                    email,
                    password,
                    "manual_relogin",
                    progress_id,
                    watcher_owner=relogin_owner,
                    expected_account=live_account,
                    progress_generation=progress_generation,
                ):
                    relogined += 1
                else:
                    skipped += 1

            result = {
                "relogined": relogined,
                "skipped": skipped,
                "errors": errors,
                "items": self.list_accounts(),
            }
            if progress_id and relogined == 0:
                self.finish_relogin_progress(
                    progress_id,
                    result,
                    watcher_owner=relogin_owner,
                    progress_generation=progress_generation,
                )
            return result
        except BaseException as exc:
            if progress_id:
                try:
                    self.finish_relogin_progress(
                        progress_id,
                        error="relogin failed",
                        watcher_owner=relogin_owner,
                        progress_generation=progress_generation,
                    )
                except BaseException:
                    pass
            self._invalidate_refresh_batch_owner(relogin_owner)
            raise exc
        finally:
            self._release_refresh_batch_owner(relogin_owner)

    def build_export_items(self, access_tokens: list[str] | None = None) -> list[dict[str, str]]:
        target_tokens = set(token for token in (access_tokens or []) if token)
        with self._lock:
            accounts = [
                dict(item)
                for item in self._accounts.values()
                if not target_tokens or str(item.get("access_token") or "") in target_tokens
            ]

        items: list[dict[str, str]] = []
        for account in accounts:
            access_token = str(account.get("access_token") or "").strip()
            refresh_token = str(account.get("refresh_token") or "").strip()
            id_token = str(account.get("id_token") or "").strip()
            if not access_token or not refresh_token or not id_token:
                continue

            access_payload = self._decode_jwt_payload(access_token)
            id_payload = self._decode_jwt_payload(id_token)
            auth_claim = access_payload.get("https://api.openai.com/auth")
            auth_claim = auth_claim if isinstance(auth_claim, dict) else {}
            profile_claim = access_payload.get("https://api.openai.com/profile")
            profile_claim = profile_claim if isinstance(profile_claim, dict) else {}

            email = (
                str(account.get("email") or "").strip()
                or str(profile_claim.get("email") or "").strip()
                or str(id_payload.get("email") or "").strip()
            )
            account_id = (
                str(account.get("account_id") or "").strip()
                or str(auth_claim.get("chatgpt_account_id") or "").strip()
                or str(account.get("user_id") or "").strip()
            )
            item = {
                "type": str(account.get("export_type") or "codex"),
                "email": email,
                "account_id": account_id,
                "access_token": access_token,
                "refresh_token": refresh_token,
                "id_token": id_token,
                "expired": self._timestamp_to_iso(access_payload.get("exp")),
                "last_refresh": self._timestamp_to_iso(access_payload.get("iat")),
            }
            password = str(account.get("password") or "").strip()
            if password:
                item["password"] = password
            items.append(item)
        return items

    def get_stats(self) -> dict:
        with self._lock:
            items = list(self._accounts.values())
        total = len(items)
        active = sum(1 for a in items if a.get("status") == "正常")
        limited = sum(1 for a in items if a.get("status") == "限流")
        abnormal = sum(1 for a in items if a.get("status") == "异常")
        disabled = sum(1 for a in items if a.get("status") == "禁用")
        total_quota = sum(max(0, int(a.get("quota") or 0)) for a in items if a.get("status") == "正常")
        total_success = sum(int(a.get("success") or 0) for a in items)
        total_fail = sum(int(a.get("fail") or 0) for a in items)
        by_type = {}
        for a in items:
            t = a.get("type", "unknown")
            by_type[t] = by_type.get(t, 0) + 1
        return {
            "total": total,
            "cumulative_total": self._cumulative_total,
            "active": active,
            "limited": limited,
            "abnormal": abnormal,
            "disabled": disabled,
            "total_quota": total_quota,
            "total_success": total_success,
            "total_fail": total_fail,
            "by_type": by_type,
        }

    def account_health(self) -> dict:
        stats = self.get_stats()
        return {
            "healthy": stats["active"] > 0,
            "status": "ok" if stats["active"] > 0 else "degraded",
            **stats,
        }


account_service = AccountService(config.get_storage_backend())
