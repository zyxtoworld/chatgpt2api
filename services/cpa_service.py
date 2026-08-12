"""CLIProxyAPI integration for browsing remote auth files and importing selected tokens."""

from __future__ import annotations

import json
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from curl_cffi.requests import Session

from services.account_service import account_service
from services.config import DATA_DIR
from services.protocol.error_response import (
    PublicSafeValueError,
    canonicalize_import_job_errors,
    exception_log_message,
    validate_import_job_errors,
)
from services.proxy_service import proxy_settings
from services.secure_file import atomic_write_bytes
from services.storage.base import StorageConflictError, StorageDataError, make_storage_snapshot
from services.task_executor import reserve_background_task


CPA_CONFIG_FILE = DATA_DIR / "cpa_config.json"
CPA_FETCH_WORKERS = 16
_CPA_FETCH_EXECUTOR = ThreadPoolExecutor(
    max_workers=CPA_FETCH_WORKERS,
    thread_name_prefix="cpa-fetch",
)


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_nonnegative_int(value: object) -> int:
    if type(value) is not int or value < 0:
        raise ValueError("invalid non-negative integer")
    return value


def _normalize_import_job(raw: object, *, fail_unfinished: bool) -> dict | None:
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise StorageDataError()

    raw_status = raw.get("status")
    if not isinstance(raw_status, str) or not raw_status.strip():
        raise StorageDataError()
    status = raw_status.strip()
    if status not in {"pending", "running", "completed", "failed"}:
        raise StorageDataError()
    if fail_unfinished and status in {"pending", "running"}:
        status = "failed"

    counters: dict[str, int] = {}
    for name in ("total", "completed", "added", "skipped", "refreshed", "failed"):
        value = raw.get(name, 0)
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise StorageDataError()
        counters[name] = value

    text_fields: dict[str, str] = {}
    for name in ("job_id", "created_at", "updated_at"):
        value = raw.get(name)
        if not isinstance(value, str) or not value.strip():
            raise StorageDataError()
        text_fields[name] = value.strip()

    if "errors" not in raw:
        raise StorageDataError()
    raw_errors = validate_import_job_errors(raw["errors"])
    if counters["completed"] > counters["total"] or counters["failed"] > counters["completed"]:
        raise StorageDataError()
    return {
        "job_id": text_fields["job_id"],
        "status": status,
        "created_at": text_fields["created_at"],
        "updated_at": text_fields["updated_at"],
        **counters,
        "errors": raw_errors,
    }


def _normalize_pool(raw: dict, *, fail_unfinished: bool = True) -> dict:
    if not isinstance(raw, dict):
        raise StorageDataError()

    values: dict[str, str] = {}
    for name in ("id", "name", "base_url", "secret_key"):
        if name not in raw:
            values[name] = ""
            continue
        value = raw[name]
        if not isinstance(value, str):
            raise StorageDataError()
        values[name] = value.strip()

    if not values["id"]:
        raise StorageDataError()

    return {
        "id": values["id"],
        "name": values["name"],
        "base_url": values["base_url"],
        "secret_key": values["secret_key"],
        "import_job": _normalize_import_job(raw.get("import_job"), fail_unfinished=fail_unfinished),
    }


def _management_headers(secret_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {secret_key}",
        "Accept": "application/json",
    }


class CPAConfig:
    def __init__(self, store_file: Path):
        self._store_file = store_file
        self._lock = Lock()
        self._pools, migrated = self._load(fail_unfinished=False)
        self._snapshot_revision = make_storage_snapshot(self._pools).revision
        recovered_pools: list[dict] = []
        recovered = migrated
        for pool in self._pools:
            next_pool = dict(pool)
            import_job = pool.get("import_job")
            if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                next_pool["import_job"] = _normalize_import_job(import_job, fail_unfinished=True)
                recovered = True
            recovered_pools.append(next_pool)
        if recovered:
            self._commit_locked(recovered_pools)

    def _load(self, *, fail_unfinished: bool = True) -> tuple[list[dict], bool]:
        if not self._store_file.exists():
            return [], False
        try:
            raw = json.loads(self._store_file.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and "base_url" in raw:
                legacy = dict(raw)
                legacy_id = legacy.get("id")
                if legacy_id is None or (isinstance(legacy_id, str) and not legacy_id.strip()):
                    legacy["id"] = "legacy-cpa"
                elif not isinstance(legacy_id, str):
                    raise StorageDataError()
                pool = _normalize_pool(legacy, fail_unfinished=fail_unfinished)
                if not pool["base_url"]:
                    raise StorageDataError()
                return [pool], True
            if not isinstance(raw, list):
                raise StorageDataError()
            pools: list[dict] = []
            seen_ids: set[str] = set()
            for item in raw:
                if not isinstance(item, dict):
                    raise StorageDataError()
                pool = _normalize_pool(item, fail_unfinished=fail_unfinished)
                if not pool["base_url"] or pool["id"] in seen_ids:
                    raise StorageDataError()
                seen_ids.add(pool["id"])
                pools.append(pool)
            return pools, False
        except StorageDataError:
            raise
        except Exception as exc:
            raise StorageDataError() from exc

    def _reload_locked(self) -> None:
        pools, _ = self._load(fail_unfinished=False)
        self._pools = pools
        self._snapshot_revision = make_storage_snapshot(pools).revision

    def _commit_locked(self, pools: list[dict]) -> None:
        previous = self._pools
        self._pools = pools
        try:
            self._save()
        except Exception:
            self._pools = previous
            raise

    def _save(self) -> None:
        self._store_file.parent.mkdir(parents=True, exist_ok=True)
        current_pools, _ = self._load(fail_unfinished=False)
        current_revision = make_storage_snapshot(current_pools).revision
        if current_revision != self._snapshot_revision:
            raise StorageConflictError()
        payload = (json.dumps(self._pools, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        atomic_write_bytes(self._store_file, self._store_file.parent, payload)
        self._snapshot_revision = make_storage_snapshot(self._pools).revision

    def list_pools(self) -> list[dict]:
        with self._lock:
            self._reload_locked()
            return [dict(pool) for pool in self._pools]

    def get_pool(self, pool_id: str) -> dict | None:
        with self._lock:
            self._reload_locked()
            for pool in self._pools:
                if pool["id"] == pool_id:
                    return dict(pool)
        return None

    def add_pool(self, name: str, base_url: str, secret_key: str) -> dict:
        pool = _normalize_pool({"id": _new_id(), "name": name, "base_url": base_url, "secret_key": secret_key})
        with self._lock:
            self._reload_locked()
            self._commit_locked([*self._pools, pool])
        return dict(pool)

    def update_pool(self, pool_id: str, updates: dict) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, pool in enumerate(self._pools):
                if pool["id"] != pool_id:
                    continue
                merged = {**pool, **{key: value for key, value in updates.items() if value is not None}, "id": pool_id}
                next_pools = list(self._pools)
                next_pools[index] = _normalize_pool(merged)
                self._commit_locked(next_pools)
                return dict(next_pools[index])
        return None

    def delete_pool(self, pool_id: str) -> bool:
        with self._lock:
            self._reload_locked()
            before = len(self._pools)
            next_pools = [pool for pool in self._pools if pool["id"] != pool_id]
            if len(next_pools) < before:
                self._commit_locked(next_pools)
                return True
        return False

    def set_import_job(self, pool_id: str, import_job: dict | None) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, pool in enumerate(self._pools):
                if pool["id"] != pool_id:
                    continue
                next_pool = dict(pool)
                next_pool["import_job"] = _normalize_import_job(import_job, fail_unfinished=False)
                next_pools = list(self._pools)
                next_pools[index] = next_pool
                self._commit_locked(next_pools)
                return dict(next_pool)
        return None

    def begin_import_job(self, pool_id: str, import_job: dict) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, pool in enumerate(self._pools):
                if pool["id"] != pool_id:
                    continue
                current_job = pool.get("import_job")
                if isinstance(current_job, dict) and current_job.get("status") in {"pending", "running"}:
                    raise PublicSafeValueError("import is already running")
                next_pool = dict(pool)
                next_pool["import_job"] = _normalize_import_job(import_job, fail_unfinished=False)
                next_pools = list(self._pools)
                next_pools[index] = next_pool
                self._commit_locked(next_pools)
                return dict(next_pool)
        return None

    def get_import_job(self, pool_id: str) -> dict | None:
        with self._lock:
            self._reload_locked()
            for pool in self._pools:
                if pool["id"] == pool_id:
                    job = pool.get("import_job")
                    return dict(job) if isinstance(job, dict) else None
        return None


def list_remote_files(pool: dict) -> list[dict]:
    base_url = str(pool.get("base_url") or "").strip()
    secret_key = str(pool.get("secret_key") or "").strip()
    if not base_url or not secret_key:
        return []

    url = f"{base_url.rstrip('/')}/v0/management/auth-files"
    session = Session(**proxy_settings.build_session_kwargs(verify=True))
    try:
        response = session.get(url, headers=_management_headers(secret_key), timeout=30)
        if not response.ok:
            raise RuntimeError(f"remote list failed: HTTP {response.status_code}")
        payload = response.json()
    finally:
        session.close()

    files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(files, list):
        raise RuntimeError("remote list payload is invalid")

    items: list[dict] = []
    for item in files:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        email = str(item.get("email") or item.get("account") or "").strip()
        if not name:
            continue
        items.append({"name": name, "email": email})
    return items


def fetch_remote_access_token(pool: dict, file_name: str) -> tuple[str | None, str | None]:
    base_url = str(pool.get("base_url") or "").strip()
    secret_key = str(pool.get("secret_key") or "").strip()
    file_name = str(file_name or "").strip()
    if not base_url or not secret_key or not file_name:
        return None, "invalid request"

    url = f"{base_url.rstrip('/')}/v0/management/auth-files/download"
    session = Session(**proxy_settings.build_session_kwargs(verify=True))
    try:
        response = session.get(url, headers=_management_headers(secret_key), params={"name": file_name}, timeout=30)
        if not response.ok:
            return None, f"HTTP {response.status_code}"
        payload = response.json()
    except Exception as exc:
        return None, exception_log_message(exc)
    finally:
        session.close()

    if not isinstance(payload, dict):
        return None, "invalid payload"

    access_token = str(payload.get("access_token") or "").strip()
    if not access_token:
        return None, "missing access_token"
    return access_token, None


class CPAImportService:
    def __init__(self, cpa_config: CPAConfig):
        self._config = cpa_config

    def start_import(self, pool: dict, selected_files: list[str]) -> dict:
        names = [str(name or "").strip() for name in selected_files if str(name or "").strip()]
        if not names:
            raise PublicSafeValueError("selected files is required")

        pool_id = str(pool.get("id") or "").strip()
        reservation = reserve_background_task()
        job = {
            "job_id": uuid.uuid4().hex,
            "status": "pending",
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
            "total": len(names),
            "completed": 0,
            "added": 0,
            "skipped": 0,
            "refreshed": 0,
            "failed": 0,
            "errors": [],
        }
        try:
            saved_pool = self._config.begin_import_job(pool_id, job)
        except Exception:
            reservation.cancel()
            raise
        if saved_pool is None:
            reservation.cancel()
            raise PublicSafeValueError("pool not found")
        try:
            reservation.submit(self._run_import, pool_id, pool, names)
        except Exception:
            self._config.set_import_job(
                pool_id,
                {
                    **job,
                    "status": "failed",
                    "completed": len(names),
                    "failed": len(names),
                    "errors": [],
                },
            )
            raise
        return dict(saved_pool.get("import_job") or job)

    def _update_job(self, pool_id: str, **updates) -> dict | None:
        current = self._config.get_import_job(pool_id)
        if current is None:
            return None
        next_job = {**current, **updates, "updated_at": _now_iso()}
        pool = self._config.set_import_job(pool_id, next_job)
        if pool is None:
            return None
        job = pool.get("import_job")
        return dict(job) if isinstance(job, dict) else None

    def _append_error(self, pool_id: str, file_name: str, message: str) -> None:
        current = self._config.get_import_job(pool_id)
        if current is None:
            return
        errors = list(current.get("errors") or [])
        errors.append({"name": file_name, "error": message})
        self._update_job(pool_id, errors=canonicalize_import_job_errors(errors))

    def _run_import(self, pool_id: str, pool: dict, names: list[str]) -> None:
        self._update_job(pool_id, status="running")

        tokens: list[str] = []
        current = self._config.get_import_job(pool_id) or {}
        failed_count = int(current.get("failed") or 0)
        for offset in range(0, len(names), CPA_FETCH_WORKERS):
            batch = names[offset:offset + CPA_FETCH_WORKERS]
            future_map = {
                _CPA_FETCH_EXECUTOR.submit(fetch_remote_access_token, pool, name): name
                for name in batch
            }
            for future in as_completed(future_map):
                file_name = future_map[future]
                try:
                    token, error = future.result()
                except Exception as exc:
                    token, error = None, exception_log_message(exc)

                if token:
                    tokens.append(token)
                else:
                    self._append_error(pool_id, file_name, error or "unknown error")
                    failed_count += 1

                current = self._config.get_import_job(pool_id) or {}
                self._update_job(
                    pool_id,
                    completed=int(current.get("completed") or 0) + 1,
                    failed=failed_count,
                )

        if not tokens:
            current = self._config.get_import_job(pool_id) or {}
            self._update_job(
                pool_id,
                status="failed",
                completed=int(current.get("total") or 0),
                failed=failed_count,
            )
            return

        current = self._config.get_import_job(pool_id) or {}
        total = int(current.get("total") or len(names))
        try:
            add_result = account_service.add_accounts(tokens, source_type="codex")
            if not isinstance(add_result, dict) or not add_result:
                raise ValueError("invalid account import result")
            added = _parse_nonnegative_int(add_result["added"])
            skipped = _parse_nonnegative_int(add_result["skipped"])
        except Exception:
            self._update_job(
                pool_id,
                status="failed",
                completed=total,
                added=0,
                skipped=0,
                refreshed=0,
                failed=total,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "CPA", "error": "import failed"}],
                ),
            )
            return
        try:
            refresh_result = account_service.refresh_accounts(tokens)
            if not isinstance(refresh_result, dict) or not refresh_result:
                raise ValueError("invalid account refresh result")
            refreshed = _parse_nonnegative_int(refresh_result["refreshed"])
        except Exception:
            self._update_job(
                pool_id,
                status="failed",
                completed=total,
                added=added,
                skipped=skipped,
                refreshed=0,
                failed=failed_count,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "CPA", "error": "import failed"}],
                ),
            )
            return
        current = self._config.get_import_job(pool_id) or {}
        self._update_job(
            pool_id,
            status="completed",
            completed=total,
            added=added,
            skipped=skipped,
            refreshed=refreshed,
            failed=failed_count,
        )


cpa_config = CPAConfig(CPA_CONFIG_FILE)
cpa_import_service = CPAImportService(cpa_config)
