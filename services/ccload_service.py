"""ccLoad integration for browsing and importing Codex OAuth channels."""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import BoundedSemaphore, Lock
from typing import Callable, Iterator

from curl_cffi.requests import Session

from services.account_service import account_service
from services.config import DATA_DIR, parse_public_url
from services.model_contract import parse_model_text
from services.openai_backend_api import OpenAIBackendAPI
from services.remote_response import parse_json_response
from services.protocol.error_response import (
    ImportJobActiveError,
    PublicSafeValueError,
    canonicalize_import_job_errors,
    exception_log_message,
    validate_import_job_errors,
)
from services.secure_file import atomic_write_bytes, read_checked_file_bytes
from services.storage.base import (
    StorageConflictError,
    StorageDataError,
    canonical_path_write_lock,
    canonical_scoped_path_write_lock,
)
from services.task_executor import reserve_background_task, run_with_timeout
from utils.log import logger


CCLOAD_CONFIG_FILE = DATA_DIR / "ccload_config.json"
CCLOAD_CHANNEL_BROWSE_TIMEOUT_SECS = 90.0
CCLOAD_IMPORT_TIMEOUT_SECS = 30 * 60.0
CCLOAD_MAX_CHANNELS = 5000
CCLOAD_MAX_CHANNEL_PAGES = 25
CCLOAD_FETCH_WORKERS = 16
CCLOAD_MODEL_CATALOG_WORKERS = 8
CCLOAD_MODEL_BATCH_LIMIT = 50
_CCLOAD_FETCH_EXECUTOR = ThreadPoolExecutor(
    max_workers=CCLOAD_FETCH_WORKERS,
    thread_name_prefix="ccload-fetch",
)
_CCLOAD_MODEL_EXECUTOR = ThreadPoolExecutor(
    max_workers=CCLOAD_MODEL_CATALOG_WORKERS,
    thread_name_prefix="ccload-models",
)
_CCLOAD_MODEL_SLOTS = BoundedSemaphore(CCLOAD_MODEL_CATALOG_WORKERS)


class CCLoadError(RuntimeError):
    pass


class CCLoadSelectionError(CCLoadError, PublicSafeValueError):
    """The channel-model request itself is invalid, not an upstream failure."""


_MAX_CHANNEL_ID_LENGTH = 64
_CCLOAD_PUBLIC_TEXT_MAX_LENGTH = 256


def _remaining_timeout(deadline: float | None, maximum: float) -> float:
    if deadline is None:
        return maximum
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CCLoadError("ccLoad channel browse timed out")
    return min(maximum, remaining)


def _clean(value: object) -> str:
    return str(value or "").strip()


def _clean_text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _clean_public_text(value: object) -> str:
    text = _clean_text(value)
    return text if len(text) <= _CCLOAD_PUBLIC_TEXT_MAX_LENGTH else ""


def _clean_channel_id(value: object) -> str:
    if type(value) is int:
        text = str(value)
        return text if value > 0 and len(text) <= _MAX_CHANNEL_ID_LENGTH else ""
    if isinstance(value, str):
        value = value.strip()
        return (
            value
            if len(value) <= _MAX_CHANNEL_ID_LENGTH
            and all("0" <= char <= "9" for char in value)
            and not value.startswith("0")
            and value.lstrip("0")
            else ""
        )
    return ""


def _clean_channel_ids(values: object) -> list[str]:
    if not isinstance(values, list) or not values:
        return []
    selected: list[str] = []
    seen: set[str] = set()
    for value in values:
        channel_id = _clean_channel_id(value)
        if not channel_id:
            return []
        if channel_id not in seen:
            seen.add(channel_id)
            selected.append(channel_id)
    return selected


def _require_base_url(value: object) -> str:
    normalized = parse_public_url(value)
    if not normalized:
        raise PublicSafeValueError(
            "ccLoad base URL must use http or https without credentials, query, or fragment"
        )
    return normalized


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


def _parse_nonnegative_int(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("invalid non-negative integer")
    return value


def _normalize_import_job(raw: object, *, fail_unfinished: bool) -> dict | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise StorageDataError()
    status = raw.get("status")
    if not isinstance(status, str) or status not in {"pending", "running", "completed", "failed"}:
        raise StorageDataError()
    if fail_unfinished and status in {"pending", "running"}:
        status = "failed"
    counters: dict[str, int] = {}
    for name in ("total", "completed", "added", "skipped", "refreshed", "failed"):
        value = raw.get(name, 0)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise StorageDataError()
        counters[name] = value
    job_id = raw.get("job_id")
    created_at = raw.get("created_at")
    updated_at = raw.get("updated_at")
    if not all(isinstance(value, str) and value.strip() for value in (job_id, created_at, updated_at)):
        raise StorageDataError()
    if "errors" not in raw:
        raise StorageDataError()
    errors = validate_import_job_errors(raw["errors"])
    if (
        counters["completed"] > counters["total"]
        or counters["failed"] > counters["completed"]
        or counters["added"] + counters["skipped"] > counters["total"]
        or counters["refreshed"] > counters["total"]
    ):
        raise StorageDataError()
    return {
        "job_id": job_id.strip(),
        "status": status,
        "created_at": created_at.strip(),
        "updated_at": updated_at.strip(),
        **counters,
        "errors": errors,
    }


def _normalize_server(raw: object, *, fail_unfinished: bool) -> dict:
    if not isinstance(raw, dict):
        raise StorageDataError()
    values: dict[str, str] = {}
    for name in ("id", "name", "base_url", "password"):
        value = raw.get(name)
        if not isinstance(value, str):
            raise StorageDataError()
        values[name] = value.strip()
    values["base_url"] = parse_public_url(values["base_url"])
    if not values["id"] or not values["base_url"] or not values["password"]:
        raise StorageDataError()
    return {
        **values,
        "import_job": _normalize_import_job(raw.get("import_job"), fail_unfinished=fail_unfinished),
    }


def _file_revision(path: Path) -> str | None:
    try:
        payload = read_checked_file_bytes(path, path.parent)
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise StorageDataError() from exc
    return hashlib.sha256(payload).hexdigest()


class CCLoadConfig:
    """Small fail-closed store for ccLoad connection definitions and import progress."""

    def __init__(self, store_file: Path):
        self._store_file = Path(store_file)
        self._lock = Lock()
        self._path_write_lock = canonical_path_write_lock(self._store_file)
        self._servers, self._snapshot_revision = self._load(fail_unfinished=False)
        recovered_servers: list[dict] = []
        recovered = False
        active_server_ids: list[str] = []
        for server in self._servers:
            next_server = dict(server)
            import_job = server.get("import_job")
            if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                active_server_ids.append(server["id"])
                next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=True)
                recovered = True
            recovered_servers.append(next_server)
        if recovered:
            with ExitStack() as locks:
                for server_id in sorted(active_server_ids):
                    locks.enter_context(self.import_job_lock(server_id))
                self._servers, self._snapshot_revision = self._load(fail_unfinished=False)
                recovered_servers = []
                recovered_after_lock = False
                for server in self._servers:
                    next_server = dict(server)
                    import_job = server.get("import_job")
                    if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                        next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=True)
                        recovered_after_lock = True
                    recovered_servers.append(next_server)
                if recovered_after_lock:
                    self._commit_locked(recovered_servers)

    def import_job_lock(self, server_id: str):
        return canonical_scoped_path_write_lock(self._store_file, f"ccload-import:{server_id}")

    def _load(self, *, fail_unfinished: bool) -> tuple[list[dict], str | None]:
        try:
            payload = read_checked_file_bytes(self._store_file, self._store_file.parent)
        except FileNotFoundError:
            return [], None
        try:
            revision = hashlib.sha256(payload).hexdigest()
            raw = json.loads(payload.decode("utf-8"))
            if not isinstance(raw, list):
                raise StorageDataError()
            servers: list[dict] = []
            seen_ids: set[str] = set()
            for item in raw:
                server = _normalize_server(item, fail_unfinished=fail_unfinished)
                if server["id"] in seen_ids:
                    raise StorageDataError()
                seen_ids.add(server["id"])
                servers.append(server)
            return servers, revision
        except StorageDataError:
            raise
        except Exception as exc:
            raise StorageDataError() from exc

    def _reload_locked(self) -> None:
        self._servers, self._snapshot_revision = self._load(fail_unfinished=False)

    def _commit_locked(self, servers: list[dict]) -> None:
        parent = self._store_file.parent
        try:
            parent_stat = parent.stat()
            expected_root_identity = (parent_stat.st_dev, parent_stat.st_ino)
        except FileNotFoundError:
            expected_root_identity = None
        with self._path_write_lock:
            if _file_revision(self._store_file) != self._snapshot_revision:
                raise StorageConflictError()
            payload = (json.dumps(servers, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
            parent.mkdir(parents=True, exist_ok=True)
            if expected_root_identity is None:
                parent_stat = parent.stat()
                expected_root_identity = (parent_stat.st_dev, parent_stat.st_ino)
            atomic_write_bytes(
                self._store_file,
                parent,
                payload,
                expected_root_identity=expected_root_identity,
            )
            self._servers = servers
            self._snapshot_revision = hashlib.sha256(payload).hexdigest()

    def list_servers(self) -> list[dict]:
        with self._lock:
            self._reload_locked()
            return [dict(server) for server in self._servers]

    def get_server(self, server_id: str) -> dict | None:
        with self._lock:
            self._reload_locked()
            for server in self._servers:
                if server["id"] == server_id:
                    return dict(server)
        return None

    def add_server(self, *, name: str, base_url: str, password: str) -> dict:
        clean_base_url = _require_base_url(base_url)
        clean_password = _clean(password)
        if not clean_password:
            raise PublicSafeValueError("base URL and admin password are required")
        server = {
            "id": _new_id(),
            "name": _clean(name),
            "base_url": clean_base_url,
            "password": clean_password,
            "import_job": None,
        }
        with self._lock:
            self._reload_locked()
            self._commit_locked([*self._servers, server])
        return dict(server)

    def update_server(self, server_id: str, updates: dict) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                active = isinstance(server.get("import_job"), dict) and server["import_job"].get("status") in {"pending", "running"}
                break
            else:
                return None
        if active:
            with self.import_job_lock(server_id), self._lock:
                self._reload_locked()
                for index, server in enumerate(self._servers):
                    if server["id"] != server_id:
                        continue
                    merged = dict(server)
                    for name in ("name", "base_url", "password"):
                        if name in updates and updates[name] is not None:
                            merged[name] = (
                                _require_base_url(updates[name])
                                if name == "base_url"
                                else _clean(updates[name])
                            )
                    if not merged["password"]:
                        raise PublicSafeValueError("base URL and admin password are required")
                    normalized = _normalize_server(merged, fail_unfinished=False)
                    next_servers = list(self._servers)
                    next_servers[index] = normalized
                    self._commit_locked(next_servers)
                    return dict(normalized)
            return None
        with self._lock:
            self._reload_locked()
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                merged = dict(server)
                for name in ("name", "base_url", "password"):
                    if name in updates and updates[name] is not None:
                        merged[name] = (
                            _require_base_url(updates[name])
                            if name == "base_url"
                            else _clean(updates[name])
                        )
                if not merged["password"]:
                    raise PublicSafeValueError("base URL and admin password are required")
                normalized = _normalize_server(merged, fail_unfinished=False)
                next_servers = list(self._servers)
                next_servers[index] = normalized
                self._commit_locked(next_servers)
                return dict(normalized)
        return None

    def delete_server(self, server_id: str) -> bool:
        with self._lock:
            self._reload_locked()
            server = next((item for item in self._servers if item["id"] == server_id), None)
            active = isinstance(server.get("import_job"), dict) and server["import_job"].get("status") in {"pending", "running"} if server else False
        if active:
            with self.import_job_lock(server_id), self._lock:
                self._reload_locked()
                for server in self._servers:
                    if server["id"] == server_id:
                        import_job = server.get("import_job")
                        if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                            raise ImportJobActiveError("import is already running")
                        break
                before = len(self._servers)
                next_servers = [server for server in self._servers if server["id"] != server_id]
                if len(next_servers) < before:
                    self._commit_locked(next_servers)
                    return True
            return False
        with self._lock:
            self._reload_locked()
            next_servers = [server for server in self._servers if server["id"] != server_id]
            if len(next_servers) == len(self._servers):
                return False
            self._commit_locked(next_servers)
            return True

    def set_import_job(
        self,
        server_id: str,
        import_job: dict | None,
        *,
        expected_job_id: str | None = None,
    ) -> dict | None:
        with self._lock:
            self._reload_locked()
            current = next((server for server in self._servers if server["id"] == server_id), None)
            active = isinstance(current.get("import_job"), dict) and current["import_job"].get("status") in {"pending", "running"} if current else False
        lock = self.import_job_lock(server_id) if active else None
        if lock is not None:
            with lock:
                return self._set_import_job_locked(server_id, import_job, expected_job_id=expected_job_id)
        return self._set_import_job_locked(server_id, import_job, expected_job_id=expected_job_id)

    def _set_import_job_locked(
        self,
        server_id: str,
        import_job: dict | None,
        *,
        expected_job_id: str | None,
    ) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                current_job = server.get("import_job")
                if expected_job_id is not None and (
                    not isinstance(current_job, dict)
                    or current_job.get("job_id") != expected_job_id
                ):
                    return None
                next_server = dict(server)
                next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=False)
                next_servers = list(self._servers)
                next_servers[index] = next_server
                self._commit_locked(next_servers)
                return dict(next_server)
        return None

    def begin_import_job(self, server_id: str, import_job: dict) -> dict | None:
        with self._lock:
            self._reload_locked()
            current = next((server for server in self._servers if server["id"] == server_id), None)
            active = isinstance(current.get("import_job"), dict) and current["import_job"].get("status") in {"pending", "running"} if current else False
        lock = self.import_job_lock(server_id) if active else None
        if lock is not None:
            with lock:
                return self._begin_import_job_locked(server_id, import_job)
        return self._begin_import_job_locked(server_id, import_job)

    def _begin_import_job_locked(self, server_id: str, import_job: dict) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                current_job = server.get("import_job")
                if isinstance(current_job, dict) and current_job.get("status") in {"pending", "running"}:
                    raise ImportJobActiveError("import is already running")
                next_server = dict(server)
                next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=False)
                next_servers = list(self._servers)
                next_servers[index] = next_server
                self._commit_locked(next_servers)
                return dict(next_server)
        return None

    def get_import_job(self, server_id: str) -> dict | None:
        server = self.get_server(server_id)
        job = server.get("import_job") if server else None
        return dict(job) if isinstance(job, dict) else None


def _response_payload(response, operation: str) -> dict:
    try:
        payload = parse_json_response(response, f"ccLoad {operation}")
    except Exception as exc:
        if isinstance(exc, CCLoadError):
            raise
        raise CCLoadError(f"ccLoad {operation} failed") from exc
    if not isinstance(payload, dict) or payload.get("success") is not True or "data" not in payload:
        raise CCLoadError(f"ccLoad {operation} failed")
    return payload


@contextmanager
def _admin_session(
        server: dict,
        *,
        deadline: float | None = None,
) -> Iterator[tuple[Session, str, dict[str, str]]]:
    base_url = _clean(server.get("base_url")).rstrip("/")
    password = _clean(server.get("password"))
    if not base_url or not password:
        raise CCLoadError("ccLoad connection is incomplete")

    session = Session(verify=True)
    token = ""
    try:
        try:
            response = session.post(
                f"{base_url}/login",
                json={"mode": "admin", "password": password},
                headers={"Accept": "application/json", "Content-Type": "application/json"},
                timeout=_remaining_timeout(deadline, 30.0),
                stream=True,
            )
        except Exception as exc:
            raise CCLoadError("ccLoad login failed") from exc
        payload = _response_payload(response, "login")
        data = payload.get("data")
        token = _clean_text(data.get("token")) if isinstance(data, dict) else ""
        role = _clean_text(data.get("role")) if isinstance(data, dict) else ""
        if not token or role != "admin":
            raise CCLoadError("ccLoad login failed")
        yield session, base_url, {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
    finally:
        if token:
            try:
                logout_timeout = _remaining_timeout(deadline, 10.0) if deadline is not None else 10.0
                logout_response = session.post(
                    f"{base_url}/logout",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=logout_timeout,
                    stream=True,
                )
                close = getattr(logout_response, "close", None)
                if callable(close):
                    close()
            except Exception:
                pass
        session.close()


def list_remote_channels(server: dict) -> list[dict]:
    """List public metadata for ccLoad Codex OAuth channels."""
    channels: list[dict] = []
    limit = 200
    offset = 0
    page_count = 0
    expected_count: int | None = None
    count_seen = False
    deadline = time.monotonic() + CCLOAD_CHANNEL_BROWSE_TIMEOUT_SECS
    with _admin_session(server, deadline=deadline) as (session, base_url, headers):
        while True:
            page_count += 1
            if page_count > CCLOAD_MAX_CHANNEL_PAGES:
                raise CCLoadError("ccLoad channel list limit exceeded")
            try:
                response = session.get(
                    f"{base_url}/admin/channels",
                    headers=headers,
                    params={"auth_type": "codex_oauth", "limit": limit, "offset": offset},
                    timeout=_remaining_timeout(deadline, 30.0),
                    stream=True,
                )
            except Exception as exc:
                raise CCLoadError("ccLoad channel list failed") from exc
            payload = _response_payload(response, "channel list")
            data = payload.get("data")
            if not isinstance(data, list):
                raise CCLoadError("ccLoad channel list failed")
            count = payload.get("count")
            if count_seen:
                if type(count) is not int or count != expected_count:
                    raise CCLoadError("ccLoad channel list failed")
            elif count is not None:
                if page_count != 1 or type(count) is not int:
                    raise CCLoadError("ccLoad channel list failed")
                expected_count = count
                count_seen = True
            if count is not None:
                if type(count) is not int or count < offset:
                    raise CCLoadError("ccLoad channel list failed")
                if count > CCLOAD_MAX_CHANNELS:
                    raise CCLoadError("ccLoad channel list limit exceeded")
            next_offset = offset + len(data)
            if next_offset > CCLOAD_MAX_CHANNELS:
                raise CCLoadError("ccLoad channel list limit exceeded")
            if count is not None and next_offset > count:
                raise CCLoadError("ccLoad channel list failed")
            for item in data:
                if not isinstance(item, dict) or _clean_text(item.get("auth_type")) != "codex_oauth":
                    continue
                channel_id = _clean_channel_id(item.get("id"))
                enabled = item.get("enabled")
                if not channel_id or not isinstance(enabled, bool):
                    raise CCLoadError("ccLoad channel list failed")
                channels.append({
                    "id": channel_id,
                    "name": _clean_public_text(item.get("name")),
                    "enabled": enabled,
                    "plan_type": _clean_public_text(item.get("codex_plan_type")),
                    "subscription_active_until": _clean_public_text(item.get("codex_subscription_active_until")),
                    "models": [],
                    "models_loaded": not enabled,
                })

            offset = next_offset
            if count is None:
                # Some ccLoad versions omit the total. A full page is not
                # evidence of completion; continue until the first short
                # or empty page.
                if not data or len(data) < limit:
                    break
                if page_count >= CCLOAD_MAX_CHANNEL_PAGES:
                    raise CCLoadError("ccLoad channel list limit exceeded")
            elif type(count) is int and count >= offset:
                if offset >= count:
                    break
                if not data:
                    raise CCLoadError("ccLoad channel list failed")
            else:
                raise CCLoadError("ccLoad channel list failed")
    return channels


def list_remote_channel_models(server: dict, channel_ids: list[str]) -> list[dict]:
    selected = _clean_channel_ids(channel_ids)
    if not selected:
        raise CCLoadSelectionError("ccLoad channel selection is invalid")
    if len(selected) > CCLOAD_MODEL_BATCH_LIMIT:
        raise CCLoadSelectionError("ccLoad model batch supports at most 50 channels")

    deadline = time.monotonic() + CCLOAD_CHANNEL_BROWSE_TIMEOUT_SECS
    catalogs: list[dict] = []
    model_tokens: dict[int, str] = {}
    with _admin_session(server, deadline=deadline) as (session, base_url, headers):
        for channel_id in selected:
            try:
                credential = _fetch_remote_credential(
                    session,
                    base_url,
                    headers,
                    channel_id,
                    deadline=deadline,
                )
                plan_type = _clean_public_text(credential.get("plan_type"))
                catalog_index = len(catalogs)
                catalogs.append({
                    "id": channel_id,
                    "plan_type": plan_type,
                    "models": [],
                    "models_loaded": False,
                })
                model_tokens[catalog_index] = credential["access_token"]
            except Exception as exc:
                if time.monotonic() >= deadline:
                    raise CCLoadError("ccLoad channel model list timed out") from exc
                catalogs.append({
                    "id": channel_id,
                    "plan_type": "",
                    "models": [],
                    "models_loaded": False,
                })
                logger.warning({
                    "event": "ccload_channel_model_catalog_failed",
                    "channel_id": channel_id,
                    "error": exception_log_message(exc),
                })
    if not model_tokens:
        return catalogs

    futures = {}
    try:
        for catalog_index, access_token in model_tokens.items():
            futures[_submit_channel_model_ids(
                access_token,
                deadline=deadline,
            )] = catalog_index
        for future in as_completed(futures, timeout=_remaining_timeout(deadline, CCLOAD_CHANNEL_BROWSE_TIMEOUT_SECS)):
            catalog_index = futures[future]
            try:
                catalogs[catalog_index]["models"] = future.result()
                catalogs[catalog_index]["models_loaded"] = True
            except Exception as exc:
                catalogs[catalog_index]["models_loaded"] = False
                if time.monotonic() >= deadline:
                    raise CCLoadError("ccLoad channel model list timed out") from exc
                logger.warning({
                    "event": "ccload_channel_model_catalog_failed",
                    "channel_id": catalogs[catalog_index]["id"],
                    "error": exception_log_message(exc),
                })
    except FuturesTimeoutError as exc:
        for future in futures:
            future.cancel()
        raise CCLoadError("ccLoad channel model list timed out") from exc
    except BaseException:
        for future in futures:
            future.cancel()
        raise
    return catalogs


def _normalized_codex_credential(raw: object) -> dict | None:
    if not isinstance(raw, dict):
        return None
    credential: dict[str, str] = {}
    for name in (
        "id_token",
        "access_token",
        "refresh_token",
        "account_id",
        "email",
        "type",
        "expired",
        "plan_type",
    ):
        value = raw.get(name, "")
        if not isinstance(value, str):
            return None
        credential[name] = value.strip()
    credential["type"] = credential["type"].lower() or "codex"
    expired = credential["expired"]
    if not re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",
        expired,
    ):
        return None
    try:
        parsed_expiry = datetime.fromisoformat(expired[:-1] + "+00:00" if expired.endswith("Z") else expired)
    except ValueError:
        return None
    if parsed_expiry.tzinfo is None:
        return None
    if (
        credential["type"] != "codex"
        or not credential["access_token"]
        or not credential["refresh_token"]
        or not credential["expired"]
    ):
        return None
    return credential


def _fetch_remote_credential(
        session: Session,
        base_url: str,
        headers: dict[str, str],
        channel_id: str,
        *,
        deadline: float | None = None,
) -> dict:
    response = session.get(
        f"{base_url}/admin/channels/{channel_id}/editor",
        headers=headers,
        timeout=_remaining_timeout(deadline, 30.0),
        stream=True,
    )
    payload = _response_payload(response, "credential fetch")
    data = payload.get("data")
    channel = data.get("channel") if isinstance(data, dict) else None
    raw_credential = data.get("oauth_credential") if isinstance(data, dict) else None
    credential = _normalized_codex_credential(raw_credential)
    if (
        not isinstance(channel, dict)
        or _clean_channel_id(channel.get("id")) != channel_id
        or _clean_text(channel.get("auth_type")) != "codex_oauth"
        or credential is None
    ):
        raise CCLoadError("ccLoad credential fetch failed")
    return credential


def _channel_model_ids(
        access_token: str,
        deadline: float | None = None,
) -> list[str]:
    backend = OpenAIBackendAPI(access_token=access_token)
    try:
        payload = backend.list_models(timeout_secs=30.0, deadline=deadline)
    except Exception as exc:
        raise CCLoadError("ccLoad channel model list failed") from exc
    finally:
        backend.close()
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        raise CCLoadError("ccLoad channel model list failed")
    return list(dict.fromkeys(
        model_id
        for item in data
        if isinstance(item, dict) and (model_id := parse_model_text(item.get("id")))
    ))


def _submit_channel_model_ids(
        access_token: str,
        *,
        deadline: float,
):
    remaining = _remaining_timeout(deadline, float("inf"))
    if not _CCLOAD_MODEL_SLOTS.acquire(timeout=remaining):
        raise CCLoadError("ccLoad channel model list timed out")
    try:
        future = _CCLOAD_MODEL_EXECUTOR.submit(
            _channel_model_ids,
            access_token,
            deadline=deadline,
        )
    except BaseException:
        _CCLOAD_MODEL_SLOTS.release()
        raise
    future.add_done_callback(lambda _future: _CCLOAD_MODEL_SLOTS.release())
    return future


def _fetch_remote_credential_for_import(
        base_url: str,
        headers: dict[str, str],
        channel_id: str,
        *,
        deadline: float | None = None,
) -> dict:
    session = Session(verify=True)
    try:
        return _fetch_remote_credential(
            session,
            base_url,
            headers,
            channel_id,
            deadline=deadline,
        )
    finally:
        session.close()


def fetch_remote_credentials(
        server: dict,
        channel_ids: list[str],
        *,
        on_progress: Callable[[str, str | None], None] | None = None,
        deadline: float | None = None,
) -> tuple[list[dict], list[dict]]:
    selected = _clean_channel_ids(channel_ids)
    if not selected:
        raise CCLoadError("ccLoad channel selection is invalid")

    credentials: list[dict] = []
    errors: list[dict] = []
    if deadline is None:
        deadline = time.monotonic() + CCLOAD_IMPORT_TIMEOUT_SECS
    with _admin_session(server, deadline=deadline) as (_session, base_url, headers):
        for offset in range(0, len(selected), CCLOAD_FETCH_WORKERS):
            batch = selected[offset:offset + CCLOAD_FETCH_WORKERS]
            future_map = {}
            try:
                for channel_id in batch:
                    future_map[_CCLOAD_FETCH_EXECUTOR.submit(
                        _fetch_remote_credential_for_import,
                        base_url,
                        headers,
                        channel_id,
                        deadline=deadline,
                    )] = channel_id
                futures = as_completed(
                    future_map,
                    timeout=_remaining_timeout(deadline, CCLOAD_IMPORT_TIMEOUT_SECS),
                )
                for future in futures:
                    channel_id = future_map[future]
                    error: str | None = None
                    try:
                        credentials.append(future.result())
                    except Exception:
                        error = "credential unavailable"
                        errors.append({"name": channel_id, "error": error})
                    if on_progress is not None:
                        on_progress(channel_id, error)
            except FuturesTimeoutError as exc:
                for future in future_map:
                    future.cancel()
                raise CCLoadError("ccLoad import timed out") from exc
            except BaseException:
                for future in future_map:
                    future.cancel()
                raise
    return credentials, errors


class CCLoadImportService:
    def __init__(self, config: CCLoadConfig):
        self._config = config

    def start_import(self, server: dict, channel_ids: list[str]) -> dict:
        selected = _clean_channel_ids(channel_ids)
        if not selected:
            raise PublicSafeValueError("channel ids are required")
        if len(selected) > CCLOAD_MAX_CHANNELS:
            raise PublicSafeValueError("channel ids limit exceeded")
        server_id = _clean(server.get("id"))
        reservation = reserve_background_task()
        now = _now_iso()
        job = {
            "job_id": uuid.uuid4().hex,
            "status": "pending",
            "created_at": now,
            "updated_at": now,
            "total": len(selected),
            "completed": 0,
            "added": 0,
            "skipped": 0,
            "refreshed": 0,
            "failed": 0,
            "errors": [],
        }
        try:
            saved = self._config.begin_import_job(server_id, job)
        except Exception:
            reservation.cancel()
            raise
        if saved is None:
            reservation.cancel()
            raise PublicSafeValueError("server not found")
        try:
            reservation.submit(self._run_import, server_id, dict(saved), selected)
        except Exception:
            self._config.set_import_job(
                server_id,
                {
                    **job,
                    "status": "failed",
                    "completed": len(selected),
                    "failed": len(selected),
                    "errors": [],
                },
            )
            raise
        return dict(saved.get("import_job") or job)

    def _update_job(
        self,
        server_id: str,
        *,
        expected_job_id: str | None = None,
        **updates: object,
    ) -> bool:
        attempts = 2 if expected_job_id is not None else 1
        for attempt in range(attempts):
            current = self._config.get_import_job(server_id)
            if current is None:
                return False
            if expected_job_id is not None and current.get("job_id") != expected_job_id:
                return False
            next_job = {**current, **updates, "updated_at": _now_iso()}
            try:
                if expected_job_id is None:
                    saved = self._config.set_import_job(server_id, next_job)
                else:
                    saved = self._config.set_import_job(
                        server_id,
                        next_job,
                        expected_job_id=expected_job_id,
                    )
            except StorageConflictError:
                if expected_job_id is None:
                    raise
                latest = self._config.get_import_job(server_id)
                if latest is None or latest.get("job_id") != expected_job_id:
                    return False
                if attempt + 1 >= attempts:
                    return False
                continue
            return saved is not None
        return False

    def _job_is_current(self, server_id: str, expected_job_id: str) -> bool:
        try:
            current = self._config.get_import_job(server_id)
        except (OSError, StorageDataError):
            # A concurrent atomic replacement or invalid snapshot means the
            # worker cannot prove ownership; cancel it without touching data.
            return False
        return current is not None and current.get("job_id") == expected_job_id

    def _run_import(self, server_id: str, server: dict, channel_ids: list[str]) -> None:
        initial_job = server.get("import_job")
        expected_job_id = initial_job.get("job_id") if isinstance(initial_job, dict) else None
        if expected_job_id is None:
            current_job = self._config.get_import_job(server_id)
            expected_job_id = current_job.get("job_id") if current_job else None
        if not isinstance(expected_job_id, str) or not expected_job_id:
            return
        try:
            if not self._update_job(server_id, expected_job_id=expected_job_id, status="running"):
                return
        except Exception:
            try:
                self._update_job(
                    server_id,
                    expected_job_id=expected_job_id,
                    status="failed",
                    completed=len(channel_ids),
                    failed=len(channel_ids),
                    errors=canonicalize_import_job_errors([
                        {"name": "ccLoad", "error": "import failed"},
                    ]),
                )
            except Exception:
                logger.error({
                    "event": "ccload_import_initial_state_persist_failed",
                    "stage": "running",
                })
            return
        deadline = time.monotonic() + CCLOAD_IMPORT_TIMEOUT_SECS
        completed_count = 0
        failed_count = 0

        def record_progress(channel_id: str, error: str | None) -> None:
            nonlocal completed_count, failed_count
            completed_count += 1
            current = self._config.get_import_job(server_id) or {}
            updates: dict[str, object] = {"completed": completed_count}
            if error is not None:
                failed_count += 1
                updates["failed"] = failed_count
                updates["errors"] = canonicalize_import_job_errors([
                    *(current.get("errors") or []),
                    {"name": channel_id, "error": error},
                ])
            self._update_job(server_id, expected_job_id=expected_job_id, **updates)

        try:
            credentials, errors = fetch_remote_credentials(
                server,
                channel_ids,
                on_progress=record_progress,
                deadline=deadline,
            )
        except Exception:
            credentials = []
            errors = [{"name": channel_id, "error": "credential unavailable"} for channel_id in channel_ids]

        # Error details are a bounded diagnostic sample, not the result count.
        # The selected set and the returned credential results are the stable
        # accounting boundary; keep progress-derived failures as a lower-level
        # signal for real-time fetch callbacks.
        fetch_failure_count = max(0, len(channel_ids) - len(credentials))
        failure_count = min(len(channel_ids), max(failed_count, fetch_failure_count))
        safe_errors = canonicalize_import_job_errors(errors)
        if not credentials:
            safe_errors = safe_errors or canonicalize_import_job_errors(
                [{"name": "ccLoad", "error": "credential unavailable"}],
            )
            self._update_job(
                server_id,
                expected_job_id=expected_job_id,
                status="failed",
                completed=len(channel_ids),
                failed=len(channel_ids),
                errors=safe_errors,
            )
            return

        try:
            _remaining_timeout(deadline, float("inf"))
        except CCLoadError:
            self._update_job(
                server_id,
                expected_job_id=expected_job_id,
                status="failed",
                completed=len(channel_ids),
                failed=len(channel_ids),
                errors=canonicalize_import_job_errors(
                    [*safe_errors, {"name": "ccLoad", "error": "import failed"}],
                ),
            )
            return

        with self._config.import_job_lock(server_id):
            if not self._job_is_current(server_id, expected_job_id):
                return

            try:
                add_result = account_service.add_account_items(credentials)
                if not isinstance(add_result, dict) or not add_result:
                    raise ValueError("invalid account import result")
                added = _parse_nonnegative_int(add_result["added"])
                skipped = _parse_nonnegative_int(add_result["skipped"])
                if added + skipped != len(credentials):
                    raise ValueError("invalid account import count")
            except Exception:
                self._update_job(
                    server_id,
                    expected_job_id=expected_job_id,
                    status="failed",
                    completed=len(channel_ids),
                    added=0,
                    skipped=0,
                    refreshed=0,
                    failed=len(channel_ids),
                    errors=canonicalize_import_job_errors(
                        [*safe_errors, {"name": "ccLoad", "error": "account import failed"}],
                    ),
                )
                return
            if not self._update_job(
                server_id,
                expected_job_id=expected_job_id,
                added=added,
                skipped=skipped,
            ):
                return

        if not self._job_is_current(server_id, expected_job_id):
            return

        try:
            _remaining_timeout(deadline, float("inf"))
        except CCLoadError:
            self._update_job(
                server_id,
                expected_job_id=expected_job_id,
                status="failed",
                completed=len(channel_ids),
                added=added,
                skipped=skipped,
                refreshed=0,
                failed=len(channel_ids),
                errors=canonicalize_import_job_errors(
                    [*safe_errors, {"name": "ccLoad", "error": "import failed"}],
                ),
            )
            return

        refresh_active = True
        refresh_state_lock = Lock()

        def record_refresh_progress(refreshed: int) -> None:
            with refresh_state_lock:
                if refresh_active and time.monotonic() < deadline:
                    self._update_job(
                        server_id,
                        expected_job_id=expected_job_id,
                        refreshed=refreshed,
                    )

        access_tokens = [credential["access_token"] for credential in credentials]
        try:
            refresh_result = run_with_timeout(
                account_service.refresh_accounts,
                access_tokens,
                timeout=_remaining_timeout(deadline, float("inf")),
                timeout_message="ccLoad account refresh timed out",
                on_progress=record_refresh_progress,
                deadline=deadline,
            )
            _remaining_timeout(deadline, float("inf"))
            if not isinstance(refresh_result, dict) or not refresh_result:
                raise ValueError("invalid account refresh result")
            refreshed = _parse_nonnegative_int(refresh_result["refreshed"])
            if refreshed > len(access_tokens):
                raise ValueError("invalid account refresh count")
            refresh_errors = refresh_result.get("errors", [])
            if not isinstance(refresh_errors, list):
                raise ValueError("invalid account refresh errors")
            # Error details are a bounded diagnostic sample.  The number of
            # refresh failures comes from the selected token set and the
            # trusted refreshed count, never from len(refresh_errors).
            refresh_failure_count = len(access_tokens) - refreshed
        except (TimeoutError, CCLoadError):
            with refresh_state_lock:
                refresh_active = False
            current = self._config.get_import_job(server_id) or {}
            self._update_job(
                server_id,
                expected_job_id=expected_job_id,
                status="failed",
                completed=len(channel_ids),
                added=added,
                skipped=skipped,
                refreshed=int(current.get("refreshed") or 0),
                failed=len(channel_ids),
                errors=canonicalize_import_job_errors(
                    [*safe_errors, {"name": "ccLoad", "error": "import failed"}],
                ),
            )
            return
        except Exception:
            with refresh_state_lock:
                refresh_active = False
            current = self._config.get_import_job(server_id) or {}
            self._update_job(
                server_id,
                expected_job_id=expected_job_id,
                status="failed",
                completed=len(channel_ids),
                added=added,
                skipped=skipped,
                refreshed=int(current.get("refreshed") or 0),
                failed=failure_count,
                errors=canonicalize_import_job_errors(
                    [*safe_errors, {"name": "ccLoad", "error": "account import failed"}],
                ),
            )
            return

        with refresh_state_lock:
            refresh_active = False
        if refresh_failure_count > 0 or refresh_errors:
            self._update_job(
                server_id,
                expected_job_id=expected_job_id,
                status="failed",
                completed=len(channel_ids),
                added=added,
                skipped=skipped,
                refreshed=refreshed,
                failed=min(len(channel_ids), failure_count + refresh_failure_count),
                errors=canonicalize_import_job_errors([*safe_errors, *refresh_errors]),
            )
            return

        self._update_job(
            server_id,
            expected_job_id=expected_job_id,
            status="completed",
            completed=len(channel_ids),
            added=added,
            skipped=skipped,
            refreshed=refreshed,
            failed=failure_count,
            errors=safe_errors,
        )


ccload_config = CCLoadConfig(CCLOAD_CONFIG_FILE)
ccload_import_service = CCLoadImportService(ccload_config)
