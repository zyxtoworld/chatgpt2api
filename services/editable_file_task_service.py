from __future__ import annotations

import hashlib
import hmac
import json
import logging
import math
import os
import secrets
import shutil
import stat
import threading
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO
from urllib.parse import quote, urlsplit

from services.account_service import account_service
from services.config import DATA_DIR
from services.content_filter import request_text
from services.log_service import LOG_TYPE_CALL, log_service
from services.openai_backend_api import EDITABLE_FILE_MODEL, OpenAIBackendAPI
from services.protocol.error_response import PublicSafeError, PublicSafeValueError, public_exception_message
from services.secure_file import (
    OpenedFile as OpenedEditableFile,
    atomic_write_bytes,
    authorized_root,
    has_link as _has_symlink,
    normalize_windows_handle_path as _normalize_windows_handle_path,
    open_no_follow_file as _open_no_follow_file,
    read_checked_file_bytes,
    validate_windows_handle_path as _validate_windows_handle_path,
)
from services.storage.base import (
    StorageConflictError,
    StorageDataError,
    canonical_path_write_lock,
    make_storage_snapshot,
)
from services.task_executor import reserve_background_task
from services.task_contract import canonical_task_timestamp

_EDITABLE_TASK_LOGGER = logging.getLogger(__name__)
from utils.helper import new_uuid

TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_ERROR = "error"
UNFINISHED_STATUSES = {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING}
EDITABLE_FILE_PLAN_TYPES = ("Plus", "Team", "Pro", "Enterprise")
EDITABLE_FILE_ROOT = DATA_DIR / "files"
EDITABLE_FILE_TASKS_PATH = DATA_DIR / "editable_file_tasks.json"
_PERSISTED_EDITABLE_ERRORS = frozenset({
    "editable file task failed",
    "PSD 任务需要至少一张图片",
    "服务已重启，未完成的任务已中断",
})
_MAX_CLIENT_TASK_ID_LENGTH = 256


def _persisted_editable_error(value: object) -> str:
    return value if isinstance(value, str) and value in _PERSISTED_EDITABLE_ERRORS else "editable file task failed"


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _clean(value: object, default: str = "") -> str:
    return str(value or default).strip()


def _owner_id(identity: dict[str, object]) -> str:
    return _clean(identity.get("id")) or "anonymous"


def _task_key(owner_id: str, task_id: str) -> str:
    return f"{owner_id}:{task_id}"


def _owner_storage_segment(owner_id: str) -> str:
    return hashlib.sha256(owner_id.encode("utf-8")).hexdigest()


def _validate_persisted_file_url(value: str, owner_id: str, kind: str, task_id: str) -> bool:
    if len(value) > 4096 or any(ord(character) < 32 or ord(character) == 127 for character in value):
        return False
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        return False
    if parsed.netloc and not parsed.scheme:
        return False
    if parsed.scheme and parsed.scheme not in {"http", "https"}:
        return False
    if parsed.scheme and not parsed.netloc:
        return False
    path = parsed.path
    marker = "/files/"
    if marker not in path:
        return False
    parts = path.split(marker, 1)[1].split("/")
    if any(not part or part in {".", ".."} for part in parts):
        return False
    if len(parts) == 3:
        expected = (kind, task_id)
        if tuple(parts[:2]) != expected:
            return False
    elif len(parts) >= 5:
        if parts[1] != _owner_storage_segment(owner_id) or parts[2] != kind or parts[3] != task_id:
            return False
    else:
        return False
    return all("\\" not in part and ":" not in part for part in parts)


def _new_download_capability() -> str:
    return secrets.token_urlsafe(32)


def _download_capability_digest(
    capability: str,
    owner_id: str,
    kind: str,
    task_id: str,
    relative_path: str,
) -> str:
    payload = "\0".join((capability, owner_id, kind, task_id, relative_path))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _is_download_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _normalize_relative_file_path(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("download file path is invalid")
    raw = value.replace("\\", "/")
    parts = raw.split("/")
    if (
        not raw
        or raw.startswith("/")
        or any(
            not part
            or part in {".", ".."}
            or ":" in part
            or any(ord(character) < 32 or ord(character) == 127 for character in part)
            for part in parts
        )
    ):
        raise ValueError("download file path is invalid")
    return "/".join(parts)


def _open_posix_file(path: Path) -> BinaryIO:
    return _open_no_follow_file(path, EDITABLE_FILE_ROOT, EDITABLE_FILE_ROOT)


def _open_windows_file(path: Path, output_dir: Path) -> BinaryIO:
    return _open_no_follow_file(path, EDITABLE_FILE_ROOT, output_dir)


def _open_verified_file(path: Path, output_dir: Path) -> BinaryIO:
    return _open_no_follow_file(path, EDITABLE_FILE_ROOT, output_dir)


def _validate_task_id(task_id: str) -> None:
    if not task_id or len(task_id) > _MAX_CLIENT_TASK_ID_LENGTH:
        raise PublicSafeValueError("client_task_id length exceeded")
    if (
        task_id in {".", ".."}
        or "/" in task_id
        or "\\" in task_id
        or ":" in task_id
        or "," in task_id
        or any(ord(character) < 32 or ord(character) == 127 for character in task_id)
    ):
        raise PublicSafeValueError("client_task_id must be a safe path segment")


def _editable_output_dir(owner_id: str, kind: str, task_id: str) -> Path:
    _validate_task_id(task_id)
    root = authorized_root(EDITABLE_FILE_ROOT)
    owner_segment = _owner_storage_segment(owner_id)
    unresolved = root / owner_segment / kind / task_id
    if _has_symlink(root, unresolved.relative_to(root).parts):
        raise ValueError("editable output directory must not contain symlinks")
    candidate = unresolved
    try:
        candidate.relative_to(root)
    except ValueError as exc:
        raise ValueError("editable output directory must stay inside the file root") from exc
    return candidate


def _elapsed_seconds(task: dict[str, Any]) -> int:
    start = float(task.get("started_ts") or task.get("created_ts") or 0)
    end = float(task.get("ended_ts") or time.time())
    return max(0, int(end - start)) if start else 0


def _output_relative_path(path: Path, owner_id: str, kind: str, task_id: str) -> str:
    output_dir = _editable_output_dir(owner_id, kind, task_id)
    unresolved = Path(path)
    try:
        relative = unresolved.relative_to(output_dir)
    except ValueError as exc:
        raise ValueError("editable output file must stay inside its task directory") from exc
    relative_path = _normalize_relative_file_path(relative.as_posix())
    if _has_symlink(output_dir, tuple(relative_path.split("/"))):
        raise ValueError("editable output file must not contain symlinks")
    resolved = unresolved.resolve()
    try:
        resolved.relative_to(output_dir)
    except ValueError as exc:
        raise ValueError("editable output file must stay inside its task directory") from exc
    if not resolved.is_file():
        raise FileNotFoundError(relative_path)
    return relative_path


def _file_url(
    path: Path,
    base_url: str,
    owner_id: str,
    kind: str,
    task_id: str,
    download_capability: str,
) -> str:
    if not download_capability or "/" in download_capability or "\\" in download_capability:
        raise ValueError("download capability is invalid")
    rel = _output_relative_path(path, owner_id, kind, task_id)
    prefix = str(base_url or "").strip().rstrip("/")
    owner_scope = _owner_storage_segment(owner_id)
    download_path = "/".join(
        (
            quote(download_capability, safe=""),
            owner_scope,
            quote(kind, safe=""),
            quote(task_id, safe=""),
            quote(rel, safe="/"),
        ),
    )
    return f"{prefix}/files/{download_path}" if prefix else f"/files/{download_path}"


def _editable_access_token() -> str:
    try:
        return account_service.get_text_access_token(plan_types=EDITABLE_FILE_PLAN_TYPES)
    except Exception as exc:
        from services.model_service import ModelUnavailableError

        if isinstance(exc, ModelUnavailableError):
            raise RuntimeError("no available plus/team/pro account") from exc
        raise


def _public_task(task: dict[str, Any]) -> dict[str, Any]:
    item = {
        "id": task.get("id"),
        "taskId": task.get("id"),
        "status": task.get("status"),
        "kind": task.get("kind"),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
        "elapsed_seconds": _elapsed_seconds(task),
    }
    for key in ("result", "error"):
        if task.get(key):
            item[key] = task[key]
    return item


class EditableFileTaskService:
    def __init__(self, path: Path = EDITABLE_FILE_TASKS_PATH) -> None:
        self.path = path
        self._lock = threading.RLock()
        self._path_lock = canonical_path_write_lock(path)
        self._tasks: dict[str, dict[str, Any]] = {}
        self._snapshot_tasks: dict[str, dict[str, Any]] = {}
        self._snapshot_revision = make_storage_snapshot([]).revision
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock:
            self._tasks = self._load_locked()
            if self._recover_unfinished_locked():
                self._save_locked()

    def submit_ppt(self, identity: dict[str, object], *, client_task_id: str = "", prompt: str = "", base64_images: list[str] | None = None, base_url: str = "") -> dict[str, Any]:
        return self._submit(identity, client_task_id=client_task_id, kind="ppt", prompt=prompt, base64_images=base64_images or [], base_url=base_url)

    def submit_psd(self, identity: dict[str, object], *, client_task_id: str = "", prompt: str = "", base64_images: list[str] | None = None, base_url: str = "") -> dict[str, Any]:
        return self._submit(identity, client_task_id=client_task_id, kind="psd", prompt=prompt, base64_images=base64_images or [], base_url=base_url)

    def list_tasks(self, identity: dict[str, object], task_ids: list[str]) -> dict[str, Any]:
        owner = _owner_id(identity)
        requested = [_clean(item) for item in task_ids if _clean(item)]
        with self._lock:
            if requested:
                items = [task for task_id in requested if (task := self._tasks.get(_task_key(owner, task_id)))]
                return {"items": [_public_task(item) for item in items], "missing_ids": [task_id for task_id in requested if _task_key(owner, task_id) not in self._tasks]}
            items = [task for task in self._tasks.values() if task.get("owner_id") == owner]
        items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return {"items": [_public_task(item) for item in items], "missing_ids": []}

    def _submit(self, identity: dict[str, object], *, client_task_id: str, kind: str, prompt: str, base64_images: list[str], base_url: str) -> dict[str, Any]:
        task_id = _clean(client_task_id) or new_uuid()
        _validate_task_id(task_id)
        owner = _owner_id(identity)
        key = _task_key(owner, task_id)
        now = _now_iso()
        with self._lock:
            if key in self._tasks:
                return _public_task(self._tasks[key])
            reservation = reserve_background_task()
            try:
                download_capability = _new_download_capability()
            except BaseException:
                reservation.cancel()
                raise
            ts = time.time()
            self._tasks[key] = {
                "id": task_id,
                "owner_id": owner,
                "status": TASK_STATUS_QUEUED,
                "kind": kind,
                "model": EDITABLE_FILE_MODEL,
                "created_at": now,
                "updated_at": now,
                "created_ts": ts,
                "updated_ts": ts,
            }
            task = dict(self._tasks[key])
            try:
                self._save_locked()
            except StorageConflictError:
                self._tasks.pop(key, None)
                try:
                    with self._path_lock:
                        latest_tasks = self._load_locked()
                        self._tasks = latest_tasks
                    existing = latest_tasks.get(key)
                except Exception:
                    reservation.cancel()
                    raise
                reservation.cancel()
                if existing is not None:
                    return _public_task(existing)
                raise
            except Exception:
                self._tasks.pop(key, None)
                reservation.cancel()
                raise
            try:
                reservation.submit(
                    self._run_task,
                    key,
                    kind,
                    prompt,
                    base64_images,
                    dict(identity),
                    base_url,
                    download_capability,
                )
            except Exception:
                self._tasks.pop(key, None)
                try:
                    self._save_locked()
                except Exception:
                    _EDITABLE_TASK_LOGGER.error("editable task submission rollback persistence failed")
                raise
        return _public_task(task)

    def _run_task(
        self,
        key: str,
        kind: str,
        prompt: str,
        base64_images: list[str],
        identity: dict[str, object],
        base_url: str,
        download_capability: str,
    ) -> None:
        started = time.time()
        token = ""
        expected_account = None
        backend = None
        output_dir: Path | None = None
        output_dir_was_present = False
        try:
            self._update_task(key, status=TASK_STATUS_RUNNING, error="", started_ts=started)
            if kind == "psd" and not base64_images:
                raise PublicSafeError("PSD 任务需要至少一张图片")
            token = _editable_access_token()
            get_account_lease = getattr(account_service, "_get_account_lease", None)
            if callable(get_account_lease):
                _, expected_account = get_account_lease(token)
            backend = OpenAIBackendAPI(token)
            owner = _owner_id(identity)
            task_id = key.rsplit(":", 1)[-1]
            output_dir = _editable_output_dir(owner, kind, task_id)
            output_dir_was_present = output_dir.exists()
            result = backend.export_psd_zip(base64_images, prompt, output_dir) if kind == "psd" else backend.export_ppt_zip(base64_images, prompt, output_dir)
            if expected_account is None:
                account_service.mark_text_used(token)
            else:
                account_service.mark_text_used(token, expected_account=expected_account)
            primary_relative_path = _output_relative_path(result.primary_path, owner, kind, task_id)
            zip_relative_path = _output_relative_path(result.zip_path, owner, kind, task_id)
            capability_hashes = {
                primary_relative_path: _download_capability_digest(
                    download_capability,
                    owner,
                    kind,
                    task_id,
                    primary_relative_path,
                ),
                zip_relative_path: _download_capability_digest(
                    download_capability,
                    owner,
                    kind,
                    task_id,
                    zip_relative_path,
                ),
            }
            data = {
                "conversation_id": result.conversation_id,
                "primary_url": _file_url(
                    result.primary_path,
                    base_url,
                    owner,
                    kind,
                    task_id,
                    download_capability,
                ),
                "zip_url": _file_url(
                    result.zip_path,
                    base_url,
                    owner,
                    kind,
                    task_id,
                    download_capability,
                ),
            }
            self._update_task(
                key,
                status=TASK_STATUS_SUCCESS,
                result=data,
                download_capability_hashes=capability_hashes,
                error="",
                ended_ts=time.time(),
            )
            self._log_call(identity, kind, started, request_text(prompt))
        except StorageConflictError:
            return
        except Exception as exc:
            error = _persisted_editable_error(public_exception_message(exc, "editable file task failed"))
            try:
                self._update_task(key, status=TASK_STATUS_ERROR, error=error, ended_ts=time.time())
            except Exception:
                with self._lock:
                    task = self._tasks.get(key)
                    if task is not None:
                        task.update({
                            "status": TASK_STATUS_ERROR,
                            "error": error,
                            "ended_ts": time.time(),
                            "updated_at": _now_iso(),
                            "updated_ts": time.time(),
                        })
                _EDITABLE_TASK_LOGGER.error("editable task terminal state persistence failed")
            if output_dir is not None and not output_dir_was_present:
                try:
                    root = authorized_root(EDITABLE_FILE_ROOT)
                    relative_parts = output_dir.relative_to(root).parts
                    if relative_parts and not _has_symlink(root, relative_parts) and not output_dir.is_symlink():
                        shutil.rmtree(output_dir)
                except Exception:
                    _EDITABLE_TASK_LOGGER.error("editable task artifact cleanup failed")
            self._log_call(identity, kind, started, request_text(prompt), status="failed", error=error)
        finally:
            if backend is not None:
                backend.close()

    def _resolve_download_file(self, relative_path: str) -> tuple[Path, Path, tuple[str, ...]]:
        raw = str(relative_path or "").replace("\\", "/").lstrip("/")
        parts = raw.split("/")
        if len(parts) < 5:
            raise FileNotFoundError(raw)
        capability, owner_scope, kind, task_id = parts[:4]
        if (
            not capability
            or len(capability) > 256
            or len(owner_scope) != 64
            or any(character not in "0123456789abcdef" for character in owner_scope)
            or kind not in {"ppt", "psd"}
        ):
            raise FileNotFoundError(raw)
        try:
            _validate_task_id(task_id)
            normalized_path = _normalize_relative_file_path("/".join(parts[4:]))
        except ValueError:
            raise FileNotFoundError(raw) from None

        task = None
        with self._lock:
            for candidate in self._tasks.values():
                candidate_owner = _clean(candidate.get("owner_id"))
                if (
                    candidate.get("id") != task_id
                    or candidate.get("kind") != kind
                    or candidate.get("status") != TASK_STATUS_SUCCESS
                    or _owner_storage_segment(candidate_owner) != owner_scope
                ):
                    continue
                raw_hashes = candidate.get("download_capability_hashes")
                expected_digest = raw_hashes.get(normalized_path) if isinstance(raw_hashes, dict) else None
                actual_digest = _download_capability_digest(
                    capability,
                    candidate_owner,
                    kind,
                    task_id,
                    normalized_path,
                )
                if isinstance(expected_digest, str) and hmac.compare_digest(expected_digest, actual_digest):
                    task = candidate
                    break
        if task is None:
            raise FileNotFoundError(raw)

        try:
            output_dir = _editable_output_dir(_clean(task.get("owner_id")), kind, task_id)
        except (OSError, ValueError):
            raise FileNotFoundError(raw) from None
        relative_parts = tuple(normalized_path.split("/"))
        if _has_symlink(output_dir, relative_parts):
            raise FileNotFoundError(raw)
        unresolved = output_dir.joinpath(*relative_parts)
        path = unresolved.resolve()
        try:
            path.relative_to(output_dir)
        except ValueError:
            raise FileNotFoundError(raw) from None
        if not path.is_file():
            raise FileNotFoundError(raw)
        return path, output_dir, relative_parts

    def open_public_file(self, relative_path: str) -> OpenedEditableFile:
        path, output_dir, relative_parts = self._resolve_download_file(relative_path)
        try:
            if _has_symlink(output_dir, relative_parts):
                raise FileNotFoundError(str(relative_path))
            validated_stat = os.stat(path)
            if _has_symlink(output_dir, relative_parts):
                raise FileNotFoundError(str(relative_path))
            file = _open_verified_file(path, output_dir)
        except (OSError, ValueError) as exc:
            raise FileNotFoundError(str(relative_path)) from exc
        try:
            opened_stat = os.fstat(file.fileno())
            if not stat.S_ISREG(opened_stat.st_mode):
                raise FileNotFoundError(str(relative_path))
            if not validated_stat.st_ino or not opened_stat.st_ino:
                raise FileNotFoundError(str(relative_path))
            if (
                validated_stat.st_dev != opened_stat.st_dev
                or validated_stat.st_ino != opened_stat.st_ino
            ):
                raise FileNotFoundError(str(relative_path))
            return OpenedEditableFile(file=file, filename=path.name, stat_result=opened_stat)
        except Exception:
            file.close()
            raise

    def _update_task(self, key: str, **updates: Any) -> None:
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                return
            previous = dict(task)
            task.update(updates)
            task["updated_at"] = _now_iso()
            task["updated_ts"] = time.time()
            try:
                self._save_locked()
            except Exception:
                task.clear()
                task.update(previous)
                raise

    def _read_items_locked(self) -> list[dict[str, Any]]:
        if not self.path.exists():
            return []
        try:
            raw = json.loads(read_checked_file_bytes(self.path, self.path.parent).decode("utf-8"))
            raw_items = raw.get("tasks") if isinstance(raw, dict) else raw
            return make_storage_snapshot(raw_items).records
        except StorageDataError:
            raise
        except Exception as exc:
            raise StorageDataError() from exc

    @staticmethod
    def _timestamp_value(value: object) -> float:
        if value is None or value == "":
            return 0.0
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise StorageDataError()
        result = float(value)
        if not math.isfinite(result):
            raise StorageDataError()
        return result

    def _load_locked(self) -> dict[str, dict[str, Any]]:
        items = self._read_items_locked()
        self._snapshot_revision = make_storage_snapshot(items).revision
        tasks: dict[str, dict[str, Any]] = {}
        for item in items:
            task_id_value = item.get("id")
            owner_value = item.get("owner_id")
            if not isinstance(task_id_value, str) or not isinstance(owner_value, str):
                raise StorageDataError()
            task_id = task_id_value.strip()
            owner = owner_value.strip()
            if not task_id or not owner:
                raise StorageDataError()
            try:
                _validate_task_id(task_id)
            except ValueError as exc:
                raise StorageDataError() from exc

            status_value = item.get("status")
            if status_value in (None, ""):
                status = TASK_STATUS_ERROR
            elif isinstance(status_value, str) and status_value in {
                TASK_STATUS_QUEUED,
                TASK_STATUS_RUNNING,
                TASK_STATUS_SUCCESS,
                TASK_STATUS_ERROR,
            }:
                status = status_value
            else:
                raise StorageDataError()

            kind_value = item.get("kind")
            if kind_value in (None, ""):
                kind = "ppt"
            elif isinstance(kind_value, str) and kind_value in {"ppt", "psd"}:
                kind = kind_value
            else:
                raise StorageDataError()

            model_value = item.get("model")
            if model_value is not None and not isinstance(model_value, str):
                raise StorageDataError()

            created_at_value = item.get("created_at")
            updated_at_value = item.get("updated_at")
            created_at = canonical_task_timestamp(created_at_value, "")
            updated_at = canonical_task_timestamp(updated_at_value, "")
            if not created_at or not updated_at:
                raise StorageDataError()
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": status,
                "kind": kind,
                "model": model_value or EDITABLE_FILE_MODEL,
                "created_at": created_at,
                "updated_at": updated_at,
                "created_ts": self._timestamp_value(item.get("created_ts")),
                "updated_ts": self._timestamp_value(item.get("updated_ts")),
            }
            raw_result = item.get("result")
            if raw_result is not None:
                if not isinstance(raw_result, dict) or set(raw_result) - {"conversation_id", "primary_url", "zip_url"}:
                    raise StorageDataError()
                result: dict[str, str] = {}
                for field in ("conversation_id", "primary_url", "zip_url"):
                    field_value = raw_result.get(field)
                    if field_value is not None and not isinstance(field_value, str):
                        raise StorageDataError()
                    if isinstance(field_value, str):
                        if field in {"primary_url", "zip_url"} and not _validate_persisted_file_url(
                            field_value,
                            owner,
                            kind,
                            task_id,
                        ):
                            raise StorageDataError()
                        result[field] = field_value
                if result:
                    task["result"] = result
            raw_error = item.get("error")
            if raw_error is not None and not isinstance(raw_error, str):
                raise StorageDataError()
            if isinstance(raw_error, str) and raw_error and raw_error not in _PERSISTED_EDITABLE_ERRORS:
                raise StorageDataError()
            if isinstance(raw_error, str) and raw_error:
                task["error"] = raw_error
            else:
                task["error"] = ""
            started_ts = self._timestamp_value(item.get("started_ts"))
            ended_ts = self._timestamp_value(item.get("ended_ts"))
            if started_ts:
                task["started_ts"] = started_ts
            if ended_ts:
                task["ended_ts"] = ended_ts
            raw_capability_hashes = item.get("download_capability_hashes")
            if raw_capability_hashes is not None:
                if not isinstance(raw_capability_hashes, dict):
                    raise StorageDataError()
                capability_hashes: dict[str, str] = {}
                for relative_path, digest in raw_capability_hashes.items():
                    if not isinstance(relative_path, str):
                        raise StorageDataError()
                    try:
                        normalized_path = _normalize_relative_file_path(relative_path)
                    except ValueError as exc:
                        raise StorageDataError() from exc
                    if normalized_path != relative_path or not _is_download_digest(digest):
                        raise StorageDataError()
                    capability_hashes[normalized_path] = digest
                if capability_hashes:
                    task["download_capability_hashes"] = capability_hashes
            key = _task_key(owner, task_id)
            if key in tasks:
                raise StorageDataError()
            tasks[key] = task
        self._snapshot_tasks = deepcopy(tasks)
        return tasks

    def _save_locked(self) -> None:
        with self._path_lock:
            local_tasks = deepcopy(self._tasks)
            baseline_tasks = deepcopy(self._snapshot_tasks)
            baseline_revision = self._snapshot_revision
            current_tasks = self._load_locked()
            current_revision = self._snapshot_revision
            if current_revision != baseline_revision:
                merged_tasks = deepcopy(current_tasks)
                dirty_keys = {
                    key
                    for key in set(local_tasks) | set(baseline_tasks)
                    if local_tasks.get(key) != baseline_tasks.get(key)
                }
                for key in dirty_keys:
                    baseline = baseline_tasks.get(key)
                    current = current_tasks.get(key)
                    local = local_tasks.get(key)
                    if current != baseline and current != local:
                        self._tasks = local_tasks
                        self._snapshot_tasks = baseline_tasks
                        self._snapshot_revision = baseline_revision
                        raise StorageConflictError()
                    if local is None:
                        merged_tasks.pop(key, None)
                    else:
                        merged_tasks[key] = local
            else:
                merged_tasks = local_tasks

            items = sorted(merged_tasks.values(), key=lambda item: str(item.get("updated_at") or ""), reverse=True)
            next_snapshot = make_storage_snapshot(items)
            payload = (json.dumps({"tasks": items}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
            atomic_write_bytes(self.path, self.path.parent, payload)
            self._tasks = merged_tasks
            self._snapshot_tasks = deepcopy(merged_tasks)
            self._snapshot_revision = next_snapshot.revision

    def _recover_unfinished_locked(self) -> bool:
        changed = False
        for task in self._tasks.values():
            if task.get("status") in UNFINISHED_STATUSES:
                task["status"] = TASK_STATUS_ERROR
                task["error"] = "服务已重启，未完成的任务已中断"
                task["ended_ts"] = time.time()
                task["updated_at"] = _now_iso()
                task["updated_ts"] = time.time()
                changed = True
        return changed

    def _log_call(
            self,
            identity: dict[str, object],
            kind: str,
            started: float,
            request_preview: str,
            *,
            status: str = "success",
            error: str = "",
    ) -> None:
        detail = {
            "key_id": identity.get("id"),
            "key_name": identity.get("name"),
            "role": identity.get("role"),
            "endpoint": f"/v1/{kind}/generations",
            "model": EDITABLE_FILE_MODEL,
            "started_at": datetime.fromtimestamp(started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": _now_iso(),
            "duration_ms": int((time.time() - started) * 1000),
            "status": status,
        }
        if request_preview:
            detail["request_text"] = request_preview
        if error:
            detail["error"] = error
        try:
            log_service.add(LOG_TYPE_CALL, f"{kind.upper()}生成任务{'失败' if status == 'failed' else '完成'}", detail)
        except Exception:
            _EDITABLE_TASK_LOGGER.error("editable task log persistence failed")


editable_file_task_service = EditableFileTaskService()
