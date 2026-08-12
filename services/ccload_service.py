"""ccLoad integration for browsing and importing Codex OAuth channels."""

from __future__ import annotations

import hashlib
import json
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Iterator

from curl_cffi.requests import Session

from services.account_service import account_service
from services.config import DATA_DIR, parse_public_url
from services.openai_backend_api import OpenAIBackendAPI
from services.protocol.error_response import (
    PublicSafeValueError,
    canonicalize_import_job_errors,
    exception_log_message,
    validate_import_job_errors,
)
from services.secure_file import atomic_write_bytes
from services.storage.base import StorageConflictError, StorageDataError
from services.task_executor import reserve_background_task
from utils.log import logger


CCLOAD_CONFIG_FILE = DATA_DIR / "ccload_config.json"
CCLOAD_CHANNEL_BROWSE_TIMEOUT_SECS = 90.0
CCLOAD_MODEL_CATALOG_WORKERS = 8


class CCLoadError(RuntimeError):
    pass


def _remaining_timeout(deadline: float | None, maximum: float) -> float:
    if deadline is None:
        return maximum
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise CCLoadError("ccLoad channel browse timed out")
    return min(maximum, remaining)


def _clean(value: object) -> str:
    return str(value or "").strip()


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
    if counters["completed"] > counters["total"] or counters["failed"] > counters["completed"]:
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
        payload = path.read_bytes()
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
        self._servers, self._snapshot_revision = self._load(fail_unfinished=False)
        recovered_servers: list[dict] = []
        recovered = False
        for server in self._servers:
            next_server = dict(server)
            import_job = server.get("import_job")
            if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                next_server["import_job"] = _normalize_import_job(import_job, fail_unfinished=True)
                recovered = True
            recovered_servers.append(next_server)
        if recovered:
            self._commit_locked(recovered_servers)

    def _load(self, *, fail_unfinished: bool) -> tuple[list[dict], str | None]:
        revision = _file_revision(self._store_file)
        if revision is None:
            return [], None
        try:
            raw = json.loads(self._store_file.read_text(encoding="utf-8"))
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
        if _file_revision(self._store_file) != self._snapshot_revision:
            raise StorageConflictError()
        payload = (json.dumps(servers, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        self._store_file.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_bytes(self._store_file, self._store_file.parent, payload)
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
            next_servers = [server for server in self._servers if server["id"] != server_id]
            if len(next_servers) == len(self._servers):
                return False
            self._commit_locked(next_servers)
            return True

    def set_import_job(self, server_id: str, import_job: dict | None) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
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
            for index, server in enumerate(self._servers):
                if server["id"] != server_id:
                    continue
                current_job = server.get("import_job")
                if isinstance(current_job, dict) and current_job.get("status") in {"pending", "running"}:
                    raise PublicSafeValueError("import is already running")
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
    if not getattr(response, "ok", False):
        raise CCLoadError(f"ccLoad {operation} failed")
    try:
        payload = response.json()
    except Exception as exc:
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
            )
        except Exception as exc:
            raise CCLoadError("ccLoad login failed") from exc
        payload = _response_payload(response, "login")
        data = payload.get("data")
        token = _clean(data.get("token")) if isinstance(data, dict) else ""
        role = _clean(data.get("role")) if isinstance(data, dict) else ""
        if not token or role != "admin":
            raise CCLoadError("ccLoad login failed")
        yield session, base_url, {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }
    finally:
        if token:
            try:
                session.post(
                    f"{base_url}/logout",
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
                    timeout=10,
                )
            except Exception:
                pass
        session.close()


def list_remote_channels(server: dict) -> list[dict]:
    """List public metadata for ccLoad Codex OAuth channels."""
    channels: list[dict] = []
    model_tokens: dict[int, str] = {}
    limit = 200
    offset = 0
    deadline = time.monotonic() + CCLOAD_CHANNEL_BROWSE_TIMEOUT_SECS
    with _admin_session(server, deadline=deadline) as (session, base_url, headers):
        while True:
            try:
                response = session.get(
                    f"{base_url}/admin/channels",
                    headers=headers,
                    params={"auth_type": "codex_oauth", "limit": limit, "offset": offset},
                    timeout=_remaining_timeout(deadline, 30.0),
                )
            except Exception as exc:
                raise CCLoadError("ccLoad channel list failed") from exc
            payload = _response_payload(response, "channel list")
            data = payload.get("data")
            if not isinstance(data, list):
                raise CCLoadError("ccLoad channel list failed")
            for item in data:
                if not isinstance(item, dict) or _clean(item.get("auth_type")) != "codex_oauth":
                    continue
                channel_id = _clean(item.get("id"))
                enabled = item.get("enabled")
                if not channel_id.isdecimal() or int(channel_id) <= 0 or not isinstance(enabled, bool):
                    raise CCLoadError("ccLoad channel list failed")
                channels.append({
                    "id": channel_id,
                    "name": _clean(item.get("name")),
                    "enabled": enabled,
                    "plan_type": _clean(item.get("codex_plan_type")),
                    "subscription_active_until": _clean(item.get("codex_subscription_active_until")),
                    "models": [],
                })

            count = payload.get("count")
            total = count if isinstance(count, int) and count >= 0 else offset + len(data)
            offset += len(data)
            if not data or offset >= total or len(data) < limit:
                break
        for index, channel in enumerate(channels):
            if not channel["enabled"]:
                continue
            try:
                credential = _fetch_remote_credential(
                    session,
                    base_url,
                    headers,
                    channel["id"],
                    deadline=deadline,
                )
                model_tokens[index] = credential["access_token"]
            except Exception as exc:
                if time.monotonic() >= deadline:
                    raise CCLoadError("ccLoad channel browse timed out") from exc
                logger.warning({
                    "event": "ccload_channel_model_catalog_failed",
                    "channel_id": channel["id"],
                    "error": exception_log_message(exc),
                })
    if not model_tokens:
        return channels

    executor = ThreadPoolExecutor(
        max_workers=min(CCLOAD_MODEL_CATALOG_WORKERS, len(model_tokens)),
        thread_name_prefix="ccload-models",
    )
    futures = {
        executor.submit(_channel_model_ids, access_token, deadline=deadline): index
        for index, access_token in model_tokens.items()
    }
    try:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise FuturesTimeoutError
        for future in as_completed(futures, timeout=remaining):
            index = futures[future]
            try:
                channels[index]["models"] = future.result()
            except Exception as exc:
                if time.monotonic() >= deadline:
                    raise CCLoadError("ccLoad channel model list timed out") from exc
                logger.warning({
                    "event": "ccload_channel_model_catalog_failed",
                    "channel_id": channels[index]["id"],
                    "error": exception_log_message(exc),
                })
    except FuturesTimeoutError as exc:
        raise CCLoadError("ccLoad channel model list timed out") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    return channels


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
    )
    payload = _response_payload(response, "credential fetch")
    data = payload.get("data")
    channel = data.get("channel") if isinstance(data, dict) else None
    raw_credential = data.get("oauth_credential") if isinstance(data, dict) else None
    credential = _normalized_codex_credential(raw_credential)
    if (
        not isinstance(channel, dict)
        or _clean(channel.get("id")) != channel_id
        or _clean(channel.get("auth_type")) != "codex_oauth"
        or credential is None
    ):
        raise CCLoadError("ccLoad credential fetch failed")
    return credential


def _channel_model_ids(access_token: str, *, deadline: float | None = None) -> list[str]:
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
        if isinstance(item, dict) and (model_id := _clean(item.get("id")))
    ))


def fetch_remote_credentials(server: dict, channel_ids: list[str]) -> tuple[list[dict], list[dict]]:
    selected = list(dict.fromkeys(_clean(value) for value in channel_ids if _clean(value)))
    if not selected or any(not value.isdecimal() or int(value) <= 0 for value in selected):
        raise CCLoadError("ccLoad channel selection is invalid")

    credentials: list[dict] = []
    errors: list[dict] = []
    with _admin_session(server) as (session, base_url, headers):
        for channel_id in selected:
            try:
                credentials.append(_fetch_remote_credential(session, base_url, headers, channel_id))
            except Exception:
                errors.append({"name": channel_id, "error": "credential unavailable"})
    return credentials, errors


class CCLoadImportService:
    def __init__(self, config: CCLoadConfig):
        self._config = config

    def start_import(self, server: dict, channel_ids: list[str]) -> dict:
        selected = list(dict.fromkeys(_clean(value) for value in channel_ids if _clean(value)))
        if not selected or any(not value.isdecimal() or int(value) <= 0 for value in selected):
            raise PublicSafeValueError("channel ids are required")
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
            reservation.submit(self._run_import, server_id, dict(server), selected)
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

    def _update_job(self, server_id: str, **updates: object) -> None:
        current = self._config.get_import_job(server_id)
        if current is None:
            return
        self._config.set_import_job(server_id, {**current, **updates, "updated_at": _now_iso()})

    def _run_import(self, server_id: str, server: dict, channel_ids: list[str]) -> None:
        self._update_job(server_id, status="running")
        try:
            credentials, errors = fetch_remote_credentials(server, channel_ids)
        except Exception:
            credentials = []
            errors = [{"name": channel_id, "error": "credential unavailable"} for channel_id in channel_ids]

        failure_count = min(len(channel_ids), len(errors))
        safe_errors = canonicalize_import_job_errors(errors)
        if not credentials:
            safe_errors = safe_errors or canonicalize_import_job_errors(
                [{"name": "ccLoad", "error": "credential unavailable"}],
            )
            self._update_job(
                server_id,
                status="failed",
                completed=len(channel_ids),
                failed=len(channel_ids),
                errors=safe_errors,
            )
            return

        try:
            add_result = account_service.add_account_items(credentials)
        except Exception:
            self._update_job(
                server_id,
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
        if not isinstance(add_result, dict) or not add_result:
            self._update_job(
                server_id,
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
        access_tokens = [credential["access_token"] for credential in credentials]
        try:
            refresh_result = account_service.refresh_accounts(access_tokens)
        except Exception:
            self._update_job(
                server_id,
                status="failed",
                completed=len(channel_ids),
                added=int(add_result.get("added") or 0),
                skipped=int(add_result.get("skipped") or 0),
                refreshed=0,
                failed=failure_count,
                errors=canonicalize_import_job_errors(
                    [*safe_errors, {"name": "ccLoad", "error": "account import failed"}],
                ),
            )
            return

        self._update_job(
            server_id,
            status="completed",
            completed=len(channel_ids),
            added=int(add_result.get("added") or 0),
            skipped=int(add_result.get("skipped") or 0),
            refreshed=int(refresh_result.get("refreshed") or 0),
            failed=failure_count,
            errors=safe_errors,
        )


ccload_config = CCLoadConfig(CCLOAD_CONFIG_FILE)
ccload_import_service = CCLoadImportService(ccload_config)
