"""CLIProxyAPI integration for browsing remote auth files and importing selected tokens."""

from __future__ import annotations

import json
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError, as_completed
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock

from curl_cffi.requests import Session

from services.account_service import account_service
from services.config import DATA_DIR, parse_public_url
from services.protocol.error_response import (
    ImportJobActiveError,
    PublicSafeValueError,
    canonicalize_import_job_errors,
    exception_log_message,
    validate_import_job_errors,
)
from services.proxy_service import proxy_settings
from services.remote_response import parse_json_response
from services.secure_file import atomic_write_bytes, read_checked_file_bytes
from services.storage.base import (
    StorageConflictError,
    StorageDataError,
    canonical_path_write_lock,
    canonical_scoped_path_write_lock,
    make_storage_snapshot,
)
from services.task_executor import reserve_background_task, run_with_timeout
from utils.log import logger


CPA_CONFIG_FILE = DATA_DIR / "cpa_config.json"
CPA_FETCH_WORKERS = 16
CPA_MAX_REMOTE_FILES = 5000
CPA_IMPORT_TIMEOUT_SECS = 30 * 60.0
_CPA_PUBLIC_TEXT_MAX_LENGTH = 256
_CPA_FETCH_EXECUTOR = ThreadPoolExecutor(
    max_workers=CPA_FETCH_WORKERS,
    thread_name_prefix="cpa-fetch",
)
def _new_id() -> str:
    return uuid.uuid4().hex[:12]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_remote_text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _clean_public_text(value: object) -> str:
    text = _clean_remote_text(value)
    return text if len(text) <= _CPA_PUBLIC_TEXT_MAX_LENGTH else ""


def _require_base_url(value: object) -> str:
    normalized = parse_public_url(value)
    if not normalized:
        raise PublicSafeValueError(
            "CPA base URL must use http or https without credentials, query, or fragment"
        )
    return normalized


def _response_json(response, operation: str) -> object:
    return parse_json_response(response, operation)


def _remaining_timeout(deadline: float | None, maximum: float) -> float:
    if deadline is None:
        return maximum
    remaining = deadline - time.monotonic()
    if remaining <= 0:
        raise FuturesTimeoutError("CPA import timed out")
    return min(maximum, remaining)


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
    if (
        counters["completed"] > counters["total"]
        or counters["failed"] > counters["completed"]
        or counters["added"] + counters["skipped"] > counters["total"]
        or counters["refreshed"] > counters["total"]
    ):
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
        self._path_write_lock = canonical_path_write_lock(store_file)
        self._pools, migrated = self._load(fail_unfinished=False)
        self._snapshot_revision = make_storage_snapshot(self._pools).revision
        recovered_pools: list[dict] = []
        recovered = migrated
        active_pool_ids: list[str] = []
        for pool in self._pools:
            next_pool = dict(pool)
            import_job = pool.get("import_job")
            if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                active_pool_ids.append(pool["id"])
                next_pool["import_job"] = _normalize_import_job(import_job, fail_unfinished=True)
                recovered = True
            recovered_pools.append(next_pool)
        if recovered:
            with ExitStack() as locks:
                for pool_id in sorted(active_pool_ids):
                    locks.enter_context(self.import_job_lock(pool_id))
                self._pools, migrated_after_lock = self._load(fail_unfinished=False)
                recovered_pools = []
                recovered_after_lock = migrated_after_lock
                for pool in self._pools:
                    next_pool = dict(pool)
                    import_job = pool.get("import_job")
                    if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                        next_pool["import_job"] = _normalize_import_job(import_job, fail_unfinished=True)
                        recovered_after_lock = True
                    recovered_pools.append(next_pool)
                self._snapshot_revision = make_storage_snapshot(self._pools).revision
                if recovered_after_lock:
                    self._commit_locked(recovered_pools)

    def import_job_lock(self, pool_id: str):
        return canonical_scoped_path_write_lock(self._store_file, f"cpa-import:{pool_id}")

    def _load(self, *, fail_unfinished: bool = True) -> tuple[list[dict], bool]:
        if not self._store_file.exists():
            return [], False
        try:
            raw = json.loads(read_checked_file_bytes(self._store_file, self._store_file.parent).decode("utf-8"))
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
                if not pool["base_url"] or not parse_public_url(pool["base_url"]) or pool["id"] in seen_ids:
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
        parent = self._store_file.parent
        try:
            parent_stat = parent.stat()
            expected_root_identity = (parent_stat.st_dev, parent_stat.st_ino)
        except FileNotFoundError:
            expected_root_identity = None
        with self._path_write_lock:
            parent.mkdir(parents=True, exist_ok=True)
            if expected_root_identity is None:
                parent_stat = parent.stat()
                expected_root_identity = (parent_stat.st_dev, parent_stat.st_ino)
            current_pools, _ = self._load(fail_unfinished=False)
            current_revision = make_storage_snapshot(current_pools).revision
            if current_revision != self._snapshot_revision:
                raise StorageConflictError()
            payload = (json.dumps(self._pools, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
            atomic_write_bytes(
                self._store_file,
                parent,
                payload,
                expected_root_identity=expected_root_identity,
            )
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
        pool = _normalize_pool({
            "id": _new_id(),
            "name": name,
            "base_url": _require_base_url(base_url),
            "secret_key": secret_key,
        })
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
                active = isinstance(pool.get("import_job"), dict) and pool["import_job"].get("status") in {"pending", "running"}
                break
            else:
                return None
        if active:
            with self.import_job_lock(pool_id), self._lock:
                self._reload_locked()
                for index, pool in enumerate(self._pools):
                    if pool["id"] != pool_id:
                        continue
                    merged = {**pool, **{key: value for key, value in updates.items() if value is not None}, "id": pool_id}
                    if "base_url" in updates and updates["base_url"] is not None:
                        merged["base_url"] = _require_base_url(updates["base_url"])
                    next_pools = list(self._pools)
                    next_pools[index] = _normalize_pool(merged)
                    self._commit_locked(next_pools)
                    return dict(next_pools[index])
            return None
        with self._lock:
            self._reload_locked()
            for index, pool in enumerate(self._pools):
                if pool["id"] != pool_id:
                    continue
                merged = {**pool, **{key: value for key, value in updates.items() if value is not None}, "id": pool_id}
                if "base_url" in updates and updates["base_url"] is not None:
                    merged["base_url"] = _require_base_url(updates["base_url"])
                next_pools = list(self._pools)
                next_pools[index] = _normalize_pool(merged)
                self._commit_locked(next_pools)
                return dict(next_pools[index])
        return None

    def delete_pool(self, pool_id: str) -> bool:
        with self._lock:
            self._reload_locked()
            pool = next((item for item in self._pools if item["id"] == pool_id), None)
            active = isinstance(pool.get("import_job"), dict) and pool["import_job"].get("status") in {"pending", "running"} if pool else False
        if active:
            with self.import_job_lock(pool_id), self._lock:
                self._reload_locked()
                for pool in self._pools:
                    if pool["id"] == pool_id:
                        import_job = pool.get("import_job")
                        if isinstance(import_job, dict) and import_job.get("status") in {"pending", "running"}:
                            raise ImportJobActiveError("import is already running")
                        break
                before = len(self._pools)
                next_pools = [pool for pool in self._pools if pool["id"] != pool_id]
                if len(next_pools) < before:
                    self._commit_locked(next_pools)
                    return True
            return False
        with self._lock:
            self._reload_locked()
            before = len(self._pools)
            next_pools = [pool for pool in self._pools if pool["id"] != pool_id]
            if len(next_pools) < before:
                self._commit_locked(next_pools)
                return True
        return False

    def set_import_job(
        self,
        pool_id: str,
        import_job: dict | None,
        *,
        expected_job_id: str | None = None,
    ) -> dict | None:
        with self._lock:
            self._reload_locked()
            current = next((pool for pool in self._pools if pool["id"] == pool_id), None)
            active = isinstance(current.get("import_job"), dict) and current["import_job"].get("status") in {"pending", "running"} if current else False
        lock = self.import_job_lock(pool_id) if active else None
        if lock is not None:
            with lock:
                return self._set_import_job_locked(pool_id, import_job, expected_job_id=expected_job_id)
        return self._set_import_job_locked(pool_id, import_job, expected_job_id=expected_job_id)

    def _set_import_job_locked(
        self,
        pool_id: str,
        import_job: dict | None,
        *,
        expected_job_id: str | None,
    ) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, pool in enumerate(self._pools):
                if pool["id"] != pool_id:
                    continue
                current_job = pool.get("import_job")
                if expected_job_id is not None:
                    if not isinstance(current_job, dict) or current_job.get("job_id") != expected_job_id:
                        return None
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
            current = next((pool for pool in self._pools if pool["id"] == pool_id), None)
            active = isinstance(current.get("import_job"), dict) and current["import_job"].get("status") in {"pending", "running"} if current else False
        lock = self.import_job_lock(pool_id) if active else None
        if lock is not None:
            with lock:
                return self._begin_import_job_locked(pool_id, import_job)
        return self._begin_import_job_locked(pool_id, import_job)

    def _begin_import_job_locked(self, pool_id: str, import_job: dict) -> dict | None:
        with self._lock:
            self._reload_locked()
            for index, pool in enumerate(self._pools):
                if pool["id"] != pool_id:
                    continue
                current_job = pool.get("import_job")
                if isinstance(current_job, dict) and current_job.get("status") in {"pending", "running"}:
                    raise ImportJobActiveError("import is already running")
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
        response = session.get(url, headers=_management_headers(secret_key), timeout=30, stream=True)
        payload = _response_json(response, "remote list failed")
    finally:
        session.close()

    files = payload.get("files") if isinstance(payload, dict) else None
    if not isinstance(files, list):
        raise RuntimeError("remote list payload is invalid")
    if len(files) > CPA_MAX_REMOTE_FILES:
        raise RuntimeError("remote file limit exceeded")

    items: list[dict] = []
    for item in files:
        if not isinstance(item, dict):
            continue
        name = _clean_public_text(item.get("name"))
        email = _clean_public_text(item.get("email")) or _clean_public_text(item.get("account"))
        if not name:
            continue
        items.append({"name": name, "email": email})
    return items


def fetch_remote_access_token(
    pool: dict,
    file_name: str,
    *,
    deadline: float | None = None,
) -> tuple[str | None, str | None]:
    base_url = str(pool.get("base_url") or "").strip()
    secret_key = str(pool.get("secret_key") or "").strip()
    file_name = file_name.strip() if isinstance(file_name, str) else ""
    if not base_url or not secret_key or not file_name:
        return None, "invalid request"

    url = f"{base_url.rstrip('/')}/v0/management/auth-files/download"
    session = Session(**proxy_settings.build_session_kwargs(verify=True))
    try:
        response = session.get(
            url,
            headers=_management_headers(secret_key),
            params={"name": file_name},
            timeout=_remaining_timeout(deadline, 30.0),
            stream=True,
        )
        try:
            payload = _response_json(response, "remote file download failed")
        except Exception as exc:
            if not response.ok:
                return None, f"HTTP {response.status_code}"
            return None, exception_log_message(exc)
    except Exception as exc:
        return None, exception_log_message(exc)
    finally:
        session.close()

    if not isinstance(payload, dict):
        return None, "invalid payload"

    access_token = _clean_remote_text(payload.get("access_token"))
    if not access_token:
        return None, "missing access_token"
    return access_token, None


class CPAImportService:
    def __init__(self, cpa_config: CPAConfig):
        self._config = cpa_config

    def start_import(self, pool: dict, selected_files: list[str]) -> dict:
        if not isinstance(selected_files, list) or any(not isinstance(name, str) for name in selected_files):
            raise PublicSafeValueError("selected files is required")
        names = list(dict.fromkeys(name.strip() for name in selected_files if name.strip()))
        if not names:
            raise PublicSafeValueError("selected files is required")
        if len(names) > CPA_MAX_REMOTE_FILES:
            raise PublicSafeValueError("selected files limit exceeded")

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
            reservation.submit(self._run_import, pool_id, dict(saved_pool), names)
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
                expected_job_id=job["job_id"],
            )
            raise
        return dict(saved_pool.get("import_job") or job)

    def _update_job(self, pool_id: str, *, expected_job_id: str | None = None, **updates) -> dict | None:
        current = self._config.get_import_job(pool_id)
        if current is None:
            return None
        if expected_job_id is not None and current.get("job_id") != expected_job_id:
            return None
        next_job = {**current, **updates, "updated_at": _now_iso()}
        pool = self._config.set_import_job(pool_id, next_job, expected_job_id=expected_job_id)
        if pool is None:
            return None
        job = pool.get("import_job")
        return dict(job) if isinstance(job, dict) else None

    def _append_error(
        self,
        pool_id: str,
        file_name: str,
        message: str,
        *,
        expected_job_id: str | None = None,
    ) -> bool:
        current = self._config.get_import_job(pool_id)
        if current is None:
            return False
        if expected_job_id is not None and current.get("job_id") != expected_job_id:
            return False
        errors = list(current.get("errors") or [])
        errors.append({"name": file_name, "error": message})
        return self._update_job(
            pool_id,
            expected_job_id=expected_job_id,
            errors=canonicalize_import_job_errors(errors),
        ) is not None

    def _run_import(self, pool_id: str, pool: dict, names: list[str]) -> None:
        import_job = pool.get("import_job")
        job_id = import_job.get("job_id") if isinstance(import_job, dict) else None
        if not isinstance(job_id, str) or not job_id:
            return
        try:
            if self._update_job(pool_id, expected_job_id=job_id, status="running") is None:
                return
        except Exception:
            try:
                self._update_job(
                    pool_id,
                    expected_job_id=job_id,
                    status="failed",
                    completed=len(names),
                    failed=len(names),
                    errors=canonicalize_import_job_errors([
                        {"name": "CPA", "error": "import failed"},
                    ]),
                )
            except Exception:
                logger.error({
                    "event": "cpa_import_initial_state_persist_failed",
                    "stage": "running",
                })
            return
        deadline = time.monotonic() + CPA_IMPORT_TIMEOUT_SECS

        tokens: list[str] = []
        current = self._config.get_import_job(pool_id) or {}
        if current.get("job_id") != job_id:
            return
        failed_count = int(current.get("failed") or 0)
        for offset in range(0, len(names), CPA_FETCH_WORKERS):
            batch = names[offset:offset + CPA_FETCH_WORKERS]
            future_map = {}
            try:
                for name in batch:
                    future_map[_CPA_FETCH_EXECUTOR.submit(
                        fetch_remote_access_token,
                        pool,
                        name,
                        deadline=deadline,
                    )] = name
                completed_futures = as_completed(
                    future_map,
                    timeout=_remaining_timeout(deadline, CPA_IMPORT_TIMEOUT_SECS),
                )
                for future in completed_futures:
                    file_name = future_map[future]
                    try:
                        token, error = future.result()
                    except Exception as exc:
                        token, error = None, exception_log_message(exc)

                    if token:
                        tokens.append(token)
                    else:
                        if not self._append_error(
                            pool_id,
                            file_name,
                            error or "unknown error",
                            expected_job_id=job_id,
                        ):
                            return
                        failed_count += 1

                    current = self._config.get_import_job(pool_id) or {}
                    if current.get("job_id") != job_id:
                        return
                    if self._update_job(
                        pool_id,
                        expected_job_id=job_id,
                        completed=int(current.get("completed") or 0) + 1,
                        failed=failed_count,
                    ) is None:
                        return
            except FuturesTimeoutError:
                for future in future_map:
                    future.cancel()
                current = self._config.get_import_job(pool_id) or {}
                if current.get("job_id") != job_id:
                    return
                self._update_job(
                    pool_id,
                    expected_job_id=job_id,
                    status="failed",
                    completed=len(names),
                    failed=len(names),
                    errors=canonicalize_import_job_errors(
                        [*(current.get("errors") or []), {"name": "CPA", "error": "import failed"}],
                    ),
                )
                return
            except Exception:
                for future in future_map:
                    future.cancel()
                current = self._config.get_import_job(pool_id) or {}
                if current.get("job_id") != job_id:
                    return
                self._update_job(
                    pool_id,
                    expected_job_id=job_id,
                    status="failed",
                    completed=len(names),
                    failed=len(names),
                    errors=canonicalize_import_job_errors(
                        [*(current.get("errors") or []), {"name": "CPA", "error": "import failed"}],
                    ),
                )
                return
            except BaseException:
                for future in future_map:
                    future.cancel()
                raise

        if not tokens:
            current = self._config.get_import_job(pool_id) or {}
            if current.get("job_id") != job_id:
                return
            self._update_job(
                pool_id,
                expected_job_id=job_id,
                status="failed",
                completed=int(current.get("total") or 0),
                failed=failed_count,
            )
            return

        with self._config.import_job_lock(pool_id):
            current = self._config.get_import_job(pool_id) or {}
            if current.get("job_id") != job_id:
                return
            total = int(current.get("total") or len(names))
            if time.monotonic() >= deadline:
                self._update_job(
                    pool_id,
                    expected_job_id=job_id,
                    status="failed",
                    completed=total,
                    failed=total,
                    errors=canonicalize_import_job_errors(
                        [*(current.get("errors") or []), {"name": "CPA", "error": "import failed"}],
                    ),
                )
                return
            try:
                add_result = account_service.add_accounts(tokens, source_type="codex")
                if not isinstance(add_result, dict) or not add_result:
                    raise ValueError("invalid account import result")
                added = _parse_nonnegative_int(add_result["added"])
                skipped = _parse_nonnegative_int(add_result["skipped"])
                if added + skipped > len(tokens):
                    raise ValueError("invalid account import count")
            except Exception:
                self._update_job(
                    pool_id,
                    expected_job_id=job_id,
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
            current = self._config.get_import_job(pool_id) or {}
            if current.get("job_id") != job_id:
                return
            if time.monotonic() >= deadline:
                self._update_job(
                    pool_id,
                    expected_job_id=job_id,
                    status="failed",
                    completed=total,
                    added=added,
                    skipped=skipped,
                    refreshed=0,
                    failed=total,
                    errors=canonicalize_import_job_errors(
                        [*(current.get("errors") or []), {"name": "CPA", "error": "import failed"}],
                    ),
                )
                return
        try:
            refresh_result = run_with_timeout(
                account_service.refresh_accounts,
                tokens,
                timeout=_remaining_timeout(deadline, float("inf")),
                timeout_message="CPA account refresh timed out",
                deadline=deadline,
            )
            if not isinstance(refresh_result, dict) or not refresh_result:
                raise ValueError("invalid account refresh result")
            refreshed = _parse_nonnegative_int(refresh_result["refreshed"])
            if refreshed > len(tokens):
                raise ValueError("invalid account refresh count")
            refresh_errors = refresh_result.get("errors", [])
            if not isinstance(refresh_errors, list):
                raise ValueError("invalid account refresh errors")
        except TimeoutError:
            self._update_job(
                pool_id,
                expected_job_id=job_id,
                status="failed",
                completed=total,
                added=added,
                skipped=skipped,
                refreshed=0,
                failed=total,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "CPA", "error": "import failed"}],
                ),
            )
            return
        except Exception:
            self._update_job(
                pool_id,
                expected_job_id=job_id,
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
        if time.monotonic() >= deadline:
            self._update_job(
                pool_id,
                expected_job_id=job_id,
                status="failed",
                completed=total,
                added=added,
                skipped=skipped,
                refreshed=refreshed,
                failed=total,
                errors=canonicalize_import_job_errors(
                    [*(current.get("errors") or []), {"name": "CPA", "error": "import failed"}],
                ),
            )
            return
        if refresh_errors:
            self._update_job(
                pool_id,
                expected_job_id=job_id,
                status="failed",
                completed=total,
                added=added,
                skipped=skipped,
                refreshed=refreshed,
                failed=min(total, failed_count + len(refresh_errors)),
                errors=canonicalize_import_job_errors([*(current.get("errors") or []), *refresh_errors]),
            )
            return
        current = self._config.get_import_job(pool_id) or {}
        if current.get("job_id") != job_id:
            return
        self._update_job(
            pool_id,
            expected_job_id=job_id,
            status="completed",
            completed=total,
            added=added,
            skipped=skipped,
            refreshed=refreshed,
            failed=failed_count,
        )


cpa_config = CPAConfig(CPA_CONFIG_FILE)
cpa_import_service = CPAImportService(cpa_config)
