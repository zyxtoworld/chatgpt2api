from __future__ import annotations

import json
import logging
import math
import os
import re
import threading
import time
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from services.config import DATA_DIR, config
from services.content_filter import request_text
from services.log_service import LOG_TYPE_CALL, log_service
from services.account_service import account_service
from services.openai_backend_api import ImagePollTimeoutError
from services.protocol import openai_v1_image_edit, openai_v1_image_generations
from services.protocol.error_response import PublicSafeError, public_exception_message
from services.protocol.image_options import normalize_image_quality, normalize_image_size
from services.secure_file import atomic_write_bytes, read_checked_file_bytes
from services.storage.base import (
    StorageConflictError,
    StorageDataError,
    canonical_path_write_lock,
    make_storage_snapshot,
)
from services.task_executor import reserve_background_task
from services.task_contract import canonical_task_timestamp

TASK_STATUS_QUEUED = "queued"
TASK_STATUS_RUNNING = "running"
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_ERROR = "error"
TERMINAL_STATUSES = {TASK_STATUS_SUCCESS, TASK_STATUS_ERROR}
UNFINISHED_STATUSES = {TASK_STATUS_QUEUED, TASK_STATUS_RUNNING}
_KEEP_RESUME_CREDENTIAL = object()
_IMAGE_POLL_TIMEOUT_CODE = "image_poll_timeout"
_IMAGE_RESTART_ERROR_CODE = "image_task_interrupted_on_restart"
_IMAGE_RESTART_ERROR_MESSAGE = "服务已重启，未完成的图片任务已中断"
_MAX_CLIENT_TASK_ID_LENGTH = 256
_PERSISTED_IMAGE_PROGRESS = frozenset({
    "getting_account",
    "image_stream_resolve_start",
    "receiving_image",
})
_PERSISTED_IMAGE_ERRORS = frozenset({
    "image task failed",
    _IMAGE_RESTART_ERROR_MESSAGE,
    "图片任务未生成图片，请稍后重试",
    "The image generation request failed. Please try again later.",
    "The image generation request was invalid.",
    "Image generation was rejected by upstream policy.",
    "The upstream service did not generate an image.",
    "Image generation failed: the upstream model returned a text description instead of an image. Please try again later.",
})
_SAFE_CONVERSATION_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}\Z")
_IMAGE_TASK_LOGGER = logging.getLogger(__name__)


def _task_file_lock(path: Path):
    return canonical_path_write_lock(path)


class ImageTaskNotFoundError(ValueError):
    pass


class ImageTaskResumeConflictError(ValueError):
    pass


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _timestamp(value: object) -> float:
    if not isinstance(value, str) or not value.strip():
        return 0.0
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(value[:26], fmt).timestamp()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return 0.0


def _clean(value: object, default: str = "") -> str:
    return str(value or default).strip()


def _persisted_image_error(value: object) -> str:
    return value if isinstance(value, str) and value in _PERSISTED_IMAGE_ERRORS else "image task failed"


def _public_image_error(task: dict[str, Any]) -> str:
    if task.get("error_code") == _IMAGE_RESTART_ERROR_CODE:
        return _IMAGE_RESTART_ERROR_MESSAGE
    value = task.get("error")
    return value if isinstance(value, str) and value in _PERSISTED_IMAGE_ERRORS else "image task failed"


def _finite_nonnegative_number(value: object, *, default: float = 0.0, maximum: float = 1_000_000_000_000_000.0) -> float:
    if value is None or value == "":
        return default
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise StorageDataError()
    number = float(value)
    if not math.isfinite(number) or number < 0 or number > maximum:
        raise StorageDataError()
    return number


def _owner_id(identity: dict[str, object]) -> str:
    return _clean(identity.get("id")) or "anonymous"


def _task_key(owner_id: str, task_id: str) -> str:
    return f"{owner_id}:{task_id}"


def _validate_client_task_id(task_id: str) -> None:
    if not task_id or len(task_id) > _MAX_CLIENT_TASK_ID_LENGTH:
        raise ValueError("client_task_id length exceeded")
    if "," in task_id:
        raise ValueError("client_task_id must not contain comma")


def _collect_image_urls(data: list[Any]) -> list[str]:
    urls: list[str] = []
    for item in data:
        if isinstance(item, dict):
            url = item.get("url")
            if isinstance(url, str) and url:
                urls.append(url)
    return urls


def _safe_log_image_url(value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        return ""
    text = value.strip()
    try:
        parsed = urlsplit(text)
        if parsed.scheme in {"http", "https"} and parsed.hostname:
            host = parsed.hostname
            if ":" in host:
                host = f"[{host}]"
            port = parsed.port
            authority = f"{host}:{port}" if port is not None else host
            return urlunsplit((parsed.scheme, authority, parsed.path, "", ""))
        if text.startswith("/"):
            return text.split("?", 1)[0].split("#", 1)[0]
    except ValueError:
        return "[redacted-image-url]"
    return "[redacted-image-url]"


def _is_public_image_url(value: object) -> bool:
    if not isinstance(value, str) or not value or len(value) > 4096:
        return False
    if any(character == "\\" or ord(character) < 32 or ord(character) == 127 for character in value):
        return False
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    if parsed.username or parsed.password or parsed.fragment:
        return False
    if parsed.netloc and not parsed.scheme:
        return False
    if parsed.scheme:
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    return parsed.path.startswith("/") and not parsed.path.startswith("//")


def _public_image_data(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    allowed_fields = ("b64_json", "url", "revised_prompt")
    projected: list[dict[str, str]] = []
    for raw_item in value:
        if not isinstance(raw_item, dict):
            continue
        item: dict[str, str] = {}
        for field in allowed_fields:
            field_value = raw_item.get(field)
            if isinstance(field_value, str) and (field != "url" or _is_public_image_url(field_value)):
                item[field] = field_value
        if item:
            projected.append(item)
    return projected


def _public_usage(value: object) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    usage: dict[str, Any] = {}
    for field in ("input_tokens", "output_tokens", "total_tokens"):
        field_value = value.get(field)
        if type(field_value) is int and field_value >= 0:
            usage[field] = field_value

    detail_fields = {
        "input_tokens_details": ("text_tokens", "image_tokens", "cached_tokens"),
        "output_tokens_details": ("text_tokens", "image_tokens", "reasoning_tokens"),
    }
    for detail_name, fields in detail_fields.items():
        raw_details = value.get(detail_name)
        if not isinstance(raw_details, dict):
            continue
        details: dict[str, int] = {}
        for field in fields:
            field_value = raw_details.get(field)
            if type(field_value) is int and field_value >= 0:
                details[field] = field_value
        if details:
            usage[detail_name] = details
    return usage or None


def _public_task(task: dict[str, Any]) -> dict[str, Any]:
    item = {
        "id": task.get("id"),
        "status": task.get("status"),
        "mode": task.get("mode"),
        "model": task.get("model"),
        "size": task.get("size"),
        "quality": task.get("quality"),
        "created_at": task.get("created_at"),
        "updated_at": task.get("updated_at"),
    }
    if task.get("conversation_id"):
        item["conversation_id"] = task.get("conversation_id")
    if task.get("data") is not None:
        item["data"] = _public_image_data(task.get("data"))
    usage = _public_usage(task.get("usage"))
    if usage is not None:
        item["usage"] = usage
    if task.get("error"):
        item["error"] = _public_image_error(task)
    if task.get("progress"):
        item["progress"] = task.get("progress")
    if task.get("duration_ms") is not None:
        item["duration_ms"] = task.get("duration_ms")
    if task.get("status") in (TASK_STATUS_RUNNING, TASK_STATUS_QUEUED):
        if task.get("status") == TASK_STATUS_RUNNING:
            # RUNNING 状态仅在 started_ts 被设置后（image_stream_resolve_start）才计时
            base_ts = task.get("started_ts")
        else:
            # QUEUED 状态从 created_ts 开始计时（排队等待中）
            base_ts = task.get("created_ts") or task.get("updated_ts")
        if base_ts:
            item["elapsed_secs"] = round(time.time() - base_ts, 1)
    return item


class ImageTaskService:
    def __init__(
        self,
        path: Path,
        *,
        generation_handler: Callable[[dict[str, Any]], dict[str, Any]] = openai_v1_image_generations.handle,
        edit_handler: Callable[[dict[str, Any]], dict[str, Any]] = openai_v1_image_edit.handle,
        retention_days_getter: Callable[[], int] | None = None,
    ):
        self.path = path
        self.generation_handler = generation_handler
        self.edit_handler = edit_handler
        self.retention_days_getter = retention_days_getter or (lambda: config.image_retention_days)
        self._lock = threading.RLock()
        self._path_lock = _task_file_lock(path)
        self._tasks: dict[str, dict[str, Any]] = {}
        self._snapshot_revision = make_storage_snapshot([]).revision
        # 续轮询凭据仅存活于当前进程，按 owner/task 隔离，绝不写入 image_tasks.json。
        self._resume_credentials: dict[str, str] = {}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._lock, self._path_lock:
            self._tasks = self._load_locked()
            changed = self._recover_unfinished_locked()
            changed = self._cleanup_locked() or changed
            if changed:
                self._save_locked()

    def submit_generation(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        size: str | None,
        quality: str = "auto",
        base_url: str = "",
    ) -> dict[str, Any]:
        size = normalize_image_size(size)
        quality = normalize_image_quality(quality)
        payload = {
            "prompt": prompt,
            "model": model,
            "n": 1,
            "size": size,
            "quality": quality,
            "response_format": "url",
            "base_url": base_url,
        }
        return self._submit(identity, client_task_id=client_task_id, mode="generate", payload=payload)

    def submit_edit(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        prompt: str,
        model: str,
        size: str | None,
        quality: str = "auto",
        base_url: str = "",
        images: list[tuple[bytes, str, str]] | None = None,
        masks: list[tuple[bytes, str, str]] | None = None,
    ) -> dict[str, Any]:
        size = normalize_image_size(size, editing=True)
        quality = normalize_image_quality(quality)
        payload = {
            "prompt": prompt,
            "images": images or [],
            "mask": masks or [],
            "model": model,
            "n": 1,
            "size": size,
            "quality": quality,
            "response_format": "url",
            "base_url": base_url,
        }
        return self._submit(identity, client_task_id=client_task_id, mode="edit", payload=payload)

    def list_tasks(self, identity: dict[str, object], task_ids: list[str]) -> dict[str, Any]:
        owner = _owner_id(identity)
        requested_ids = [_clean(task_id) for task_id in task_ids if _clean(task_id)]
        with self._lock:
            if self._cleanup_locked():
                self._save_locked()
            items = []
            missing_ids = []
            for task_id in requested_ids:
                task = self._tasks.get(_task_key(owner, task_id))
                if task is None:
                    missing_ids.append(task_id)
                else:
                    items.append(_public_task(task))
            if not requested_ids:
                items = [
                    _public_task(task)
                    for task in self._tasks.values()
                    if task.get("owner_id") == owner
                ]
                items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
                missing_ids = []
            return {"items": items, "missing_ids": missing_ids}

    def _submit(
        self,
        identity: dict[str, object],
        *,
        client_task_id: str,
        mode: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        task_id = _clean(client_task_id)
        if not task_id:
            raise ValueError("client_task_id is required")
        _validate_client_task_id(task_id)
        owner = _owner_id(identity)
        key = _task_key(owner, task_id)
        now = _now_iso()
        with self._lock:
            cleaned = self._cleanup_locked()
            task = self._tasks.get(key)
            if task is not None:
                if cleaned:
                    self._save_locked()
                return _public_task(task)
            reservation = reserve_background_task()
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": TASK_STATUS_QUEUED,
                "mode": mode,
                "model": _clean(payload.get("model"), "gpt-image-2"),
                "size": _clean(payload.get("size")),
                "quality": _clean(payload.get("quality"), "auto"),
                "created_at": now,
                "updated_at": now,
                "created_ts": time.time(),
            }
            self._tasks[key] = task
            try:
                self._save_locked()
            except Exception:
                self._tasks.pop(key, None)
                reservation.cancel()
                raise
            try:
                reservation.submit(
                    self._run_task,
                    key,
                    mode,
                    payload,
                    dict(identity),
                    _clean(payload.get("model"), "gpt-image-2"),
                )
            except Exception:
                self._tasks.pop(key, None)
                try:
                    self._save_locked()
                except Exception:
                    _IMAGE_TASK_LOGGER.error("image task submission rollback persistence failed")
                raise
        return _public_task(task)

    def _run_task(
        self,
        key: str,
        mode: str,
        payload: dict[str, Any],
        identity: dict[str, object],
        model: str,
    ) -> None:
        started = time.time()
        try:
            self._update_task(key, status=TASK_STATUS_RUNNING, error="")
        except StorageConflictError:
            return
        except Exception as exc:
            # The worker was accepted, but its first durable transition
            # failed. Do not leave the task looking queued forever while the
            # worker has already exited; keep a safe terminal projection in
            # memory until the next durable recovery opportunity.
            with self._lock:
                task = self._tasks.get(key)
                if task is not None and task.get("status") not in TERMINAL_STATUSES:
                    task.update({
                        "status": TASK_STATUS_ERROR,
                        "error": _persisted_image_error(public_exception_message(exc, "image task failed")),
                        "error_code": "",
                        "data": [],
                        "duration_ms": 0,
                        "updated_at": _now_iso(),
                        "updated_ts": time.time(),
                    })
            _IMAGE_TASK_LOGGER.error("image task initial state persistence failed")
            return
        # 创建进度回调，每个步骤完成后更新任务状态
        def progress_callback(step: str) -> None:
            if step == "image_stream_resolve_start":
                self._update_task(key, started_ts=time.time())
            self._update_task(key, progress=step)
        # 将进度回调添加到 payload 中（handler 会提取并传递给 ConversationRequest）
        payload_with_progress = {**payload, "progress_callback": progress_callback}
        try:
            handler = self.edit_handler if mode == "edit" else self.generation_handler
            result = handler(payload_with_progress)
            if not isinstance(result, dict):
                raise RuntimeError("image task returned streaming result unexpectedly")
            data = result.get("data")
            if not isinstance(data, list) or not data:
                raise PublicSafeError("图片任务未生成图片，请稍后重试")
            safe_data = _public_image_data(data)
            if not safe_data:
                raise PublicSafeError("图片任务未生成图片，请稍后重试")
            usage = _public_usage(result.get("usage"))
            duration_ms = int((time.time() - started) * 1000)
            self._transition_task(
                key,
                resume_credential=None,
                status=TASK_STATUS_SUCCESS,
                data=safe_data,
                usage=usage,
                error="",
                error_code="",
                duration_ms=duration_ms,
            )
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用完成",
                request_preview=request_text(payload.get("prompt")),
                urls=_collect_image_urls(safe_data),
            )
        except StorageConflictError:
            return
        except Exception as exc:
            error_message = public_exception_message(exc, "image task failed")
            persisted_error = _persisted_image_error(error_message)
            error_code = _IMAGE_POLL_TIMEOUT_CODE if isinstance(exc, ImagePollTimeoutError) else ""
            conversation_id = _clean(getattr(exc, "conversation_id", ""))
            access_token = _clean(getattr(exc, "access_token", ""))
            duration_ms = int((time.time() - started) * 1000)
            resume_credential = (
                access_token
                if isinstance(exc, ImagePollTimeoutError) and access_token and conversation_id
                else None
            )
            try:
                self._transition_task(
                    key,
                    resume_credential=resume_credential,
                    status=TASK_STATUS_ERROR,
                    error=persisted_error,
                    error_code=error_code,
                    data=[],
                    duration_ms=duration_ms,
                    **({"conversation_id": conversation_id} if conversation_id else {}),
                )
            except StorageConflictError:
                return
            except Exception:
                with self._lock:
                    task = self._tasks.get(key)
                    if task is not None and task.get("status") not in TERMINAL_STATUSES:
                        task.update({
                            "status": TASK_STATUS_ERROR,
                            "error": persisted_error,
                            "error_code": error_code,
                            "data": [],
                            "duration_ms": duration_ms,
                            "updated_at": _now_iso(),
                            "updated_ts": time.time(),
                        })
                _IMAGE_TASK_LOGGER.error("image task terminal state persistence failed")
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用失败",
                request_preview=request_text(payload.get("prompt")),
                status="failed",
                error=persisted_error,
            )

    def _log_call(
        self,
        identity: dict[str, object],
        mode: str,
        model: str,
        started: float,
        suffix: str,
        *,
        request_preview: str = "",
        status: str = "success",
        error: str = "",
        urls: list[str] | None = None,
    ) -> None:
        endpoint = "/v1/images/edits" if mode == "edit" else "/v1/images/generations"
        summary_prefix = "图生图" if mode == "edit" else "文生图"
        detail = {
            "key_id": identity.get("id"),
            "key_name": identity.get("name"),
            "role": identity.get("role"),
            "endpoint": endpoint,
            "model": model,
            "started_at": datetime.fromtimestamp(started).strftime("%Y-%m-%d %H:%M:%S"),
            "ended_at": _now_iso(),
            "duration_ms": int((time.time() - started) * 1000),
            "status": status,
        }
        if request_preview:
            detail["request_text"] = request_preview
        if error:
            detail["error"] = error
        if urls:
            detail["urls"] = list(
                dict.fromkeys(
                    safe_url for url in urls if (safe_url := _safe_log_image_url(url))
                )
            )
        try:
            log_service.add(LOG_TYPE_CALL, f"{summary_prefix}{suffix}", detail)
        except Exception:
            _IMAGE_TASK_LOGGER.error("image task log persistence failed")

    def _update_task(self, key: str, **updates: Any) -> None:
        self._transition_task(key, **updates)

    def _set_resume_credential_locked(self, key: str, value: object) -> None:
        if isinstance(value, str) and value:
            self._resume_credentials[key] = value
        else:
            self._resume_credentials.pop(key, None)

    def _restore_resume_credential_locked(self, key: str, previous: object) -> None:
        if previous is _KEEP_RESUME_CREDENTIAL:
            self._resume_credentials.pop(key, None)
        else:
            self._resume_credentials[key] = previous  # type: ignore[assignment]

    @staticmethod
    def _same_persisted_task(left: dict[str, Any], right: dict[str, Any]) -> bool:
        return all(
            left.get(field) == right.get(field)
            for field in ("id", "owner_id", "status", "updated_at", "updated_ts")
        )

    def _merge_task_transition_locked(
        self,
        key: str,
        expected_task: dict[str, Any],
        resume_credential: object,
        updates: dict[str, Any],
    ) -> None:
        # A different request may have persisted an unrelated task. Reload and
        # merge only when this exact task version is unchanged; never overwrite
        # a concurrent transition of the same task.
        with self._path_lock:
            latest_tasks = self._load_locked()
            latest_task = latest_tasks.get(key)
            self._tasks = latest_tasks
            if latest_task is None or not self._same_persisted_task(expected_task, latest_task):
                if latest_task is None or latest_task.get("status") in TERMINAL_STATUSES:
                    self._resume_credentials.pop(key, None)
                raise StorageConflictError()

            previous_task = dict(latest_task)
            previous_credential = self._resume_credentials.get(key, _KEEP_RESUME_CREDENTIAL)
            if resume_credential is not _KEEP_RESUME_CREDENTIAL:
                self._set_resume_credential_locked(key, resume_credential)
            latest_task.update(updates)
            latest_task["updated_at"] = _now_iso()
            latest_task["updated_ts"] = time.time()
            try:
                self._save_locked()
            except Exception:
                latest_task.clear()
                latest_task.update(previous_task)
                self._restore_resume_credential_locked(key, previous_credential)
                raise

    def _transition_task(
        self,
        key: str,
        *,
        resume_credential: object = _KEEP_RESUME_CREDENTIAL,
        **updates: Any,
    ) -> None:
        """Publish task state and its in-memory resume credential atomically."""
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                return
            previous = dict(task)
            previous_credential = self._resume_credentials.get(key, _KEEP_RESUME_CREDENTIAL)
            if resume_credential is not _KEEP_RESUME_CREDENTIAL:
                self._set_resume_credential_locked(key, resume_credential)
            task.update(updates)
            task["updated_at"] = _now_iso()
            task["updated_ts"] = time.time()
            try:
                self._save_locked()
            except StorageConflictError:
                task.clear()
                task.update(previous)
                self._restore_resume_credential_locked(key, previous_credential)
                self._merge_task_transition_locked(key, previous, resume_credential, updates)
            except Exception:
                task.clear()
                task.update(previous)
                self._restore_resume_credential_locked(key, previous_credential)
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

    def _load_locked(self) -> dict[str, dict[str, Any]]:
        raw_items = self._read_items_locked()
        snapshot = make_storage_snapshot(raw_items)
        tasks: dict[str, dict[str, Any]] = {}
        for item in snapshot.records:
            task_id_value = item.get("id")
            owner_value = item.get("owner_id")
            if not isinstance(task_id_value, str) or not isinstance(owner_value, str):
                raise StorageDataError()
            task_id = task_id_value.strip()
            owner = owner_value.strip()
            if not task_id or not owner:
                raise StorageDataError()
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
            mode_value = item.get("mode")
            if mode_value in (None, ""):
                mode = "generate"
            elif isinstance(mode_value, str) and mode_value in {"edit", "generate"}:
                mode = mode_value
            else:
                raise StorageDataError()
            model_value = item.get("model")
            if model_value is not None and not isinstance(model_value, str):
                raise StorageDataError()
            size_value = item.get("size")
            if size_value is not None and not isinstance(size_value, str):
                raise StorageDataError()
            quality_value = item.get("quality")
            if quality_value is not None and not isinstance(quality_value, str):
                raise StorageDataError()
            created_at_value = item.get("created_at")
            if not isinstance(created_at_value, str) or not created_at_value.strip():
                raise StorageDataError()
            updated_at_value = item.get("updated_at")
            if not isinstance(updated_at_value, str) or not updated_at_value.strip():
                raise StorageDataError()
            created_ts = _finite_nonnegative_number(item.get("created_ts"))
            updated_ts = _finite_nonnegative_number(item.get("updated_ts"))
            started_ts = _finite_nonnegative_number(item.get("started_ts"))
            duration_ms = _finite_nonnegative_number(item.get("duration_ms"), maximum=31_536_000_000.0)
            conversation_id = item.get("conversation_id")
            if conversation_id is not None:
                if not isinstance(conversation_id, str) or not _SAFE_CONVERSATION_ID.fullmatch(conversation_id):
                    raise StorageDataError()
            created_at = canonical_task_timestamp(created_at_value, "")
            updated_at = canonical_task_timestamp(updated_at_value, created_at)
            task = {
                "id": task_id,
                "owner_id": owner,
                "status": status,
                "mode": mode,
                "model": _clean(model_value, "gpt-image-2"),
                "size": _clean(size_value),
                "quality": _clean(quality_value, "auto"),
                "created_at": created_at,
                "updated_at": updated_at,
                "created_ts": created_ts,
                "updated_ts": updated_ts,
                "started_ts": started_ts,
            }
            if conversation_id:
                task["conversation_id"] = conversation_id
            if duration_ms:
                task["duration_ms"] = int(duration_ms) if duration_ms.is_integer() else duration_ms
            raw_progress = item.get("progress")
            if raw_progress is not None:
                if not isinstance(raw_progress, str) or raw_progress not in _PERSISTED_IMAGE_PROGRESS:
                    raise StorageDataError()
                task["progress"] = raw_progress
            data = item.get("data")
            if data is not None and not isinstance(data, list):
                raise StorageDataError()
            if data is not None:
                safe_data = _public_image_data(data)
                if safe_data != data:
                    raise StorageDataError()
                task["data"] = safe_data
            usage = item.get("usage")
            if usage is not None and not isinstance(usage, dict):
                raise StorageDataError()
            if usage is not None:
                safe_usage = _public_usage(usage)
                if safe_usage is None or safe_usage != usage:
                    raise StorageDataError()
                task["usage"] = safe_usage
            raw_error = item.get("error")
            if raw_error is not None and not isinstance(raw_error, str):
                raise StorageDataError()
            error = _clean(raw_error)
            if error:
                task["error"] = _persisted_image_error(error)
            error_code = item.get("error_code")
            if error_code is not None and not isinstance(error_code, str):
                raise StorageDataError()
            if error_code not in (None, "", _IMAGE_POLL_TIMEOUT_CODE):
                if error_code != _IMAGE_RESTART_ERROR_CODE:
                    raise StorageDataError()
            if error_code == _IMAGE_RESTART_ERROR_CODE and error != _IMAGE_RESTART_ERROR_MESSAGE:
                raise StorageDataError()
            if error_code:
                task["error_code"] = error_code
            key = _task_key(owner, task_id)
            if key in tasks:
                raise StorageDataError()
            tasks[key] = task
        self._snapshot_revision = snapshot.revision
        return tasks

    def _save_locked(self) -> None:
        with self._path_lock:
            items = sorted(self._tasks.values(), key=lambda item: str(item.get("updated_at") or ""), reverse=True)
            current_revision = make_storage_snapshot(self._read_items_locked()).revision
            if current_revision != self._snapshot_revision:
                raise StorageConflictError()
            next_snapshot = make_storage_snapshot(items)
            payload = (json.dumps({"tasks": items}, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
            atomic_write_bytes(self.path, self.path.parent, payload)
            self._snapshot_revision = next_snapshot.revision

    def _recover_unfinished_locked(self) -> bool:
        changed = False
        for task in self._tasks.values():
            if task.get("status") in UNFINISHED_STATUSES:
                task["status"] = TASK_STATUS_ERROR
                task["error"] = _IMAGE_RESTART_ERROR_MESSAGE
                task["error_code"] = _IMAGE_RESTART_ERROR_CODE
                task["updated_at"] = _now_iso()
                changed = True
        return changed

    def _cleanup_locked(self) -> bool:
        try:
            retention_days = max(1, int(self.retention_days_getter()))
        except Exception:
            retention_days = 30
        cutoff = time.time() - retention_days * 86400
        removed_keys = [
            key
            for key, task in self._tasks.items()
            if task.get("status") in TERMINAL_STATUSES and _timestamp(task.get("updated_at")) < cutoff
        ]
        for key in removed_keys:
            self._tasks.pop(key, None)
            self._resume_credentials.pop(key, None)
        return bool(removed_keys)

    def resume_poll(
        self,
        identity: dict[str, object],
        task_id: str,
        extra_timeout_secs: float = 30.0,
    ) -> dict[str, Any]:
        """恢复对已超时任务的轮询，额外等待 extra_timeout_secs 秒。"""
        owner = _owner_id(identity)
        key = _task_key(owner, _clean(task_id))
        with self._lock:
            task = self._tasks.get(key)
            if task is None:
                raise ImageTaskNotFoundError("task not found")
            if task.get("status") != TASK_STATUS_ERROR:
                raise ImageTaskResumeConflictError("task is not in error state")
            error_msg = _clean(task.get("error"))
            if task.get("error_code") != _IMAGE_POLL_TIMEOUT_CODE:
                raise ImageTaskResumeConflictError("task error is not a timeout error")
            conversation_id = _clean(task.get("conversation_id"))
            if not conversation_id:
                raise ImageTaskResumeConflictError("task has no conversation_id")
            access_token = _clean(self._resume_credentials.get(key))
            if not access_token:
                raise ImageTaskResumeConflictError("task resume credentials unavailable; task must be retried")
            mode = task.get("mode", "generate")
            model = task.get("model", "gpt-image-2")
            reservation = reserve_background_task()
            # 将任务状态重置为 running
            try:
                self._update_task(key, status=TASK_STATUS_RUNNING, error="", error_code="")
            except Exception:
                reservation.cancel()
                raise

        try:
            reservation.submit(
                self._run_resume_poll,
                key,
                conversation_id,
                extra_timeout_secs,
                access_token,
                dict(identity),
                mode,
                model,
            )
        except Exception:
            self._transition_task(key, status=TASK_STATUS_ERROR, error=error_msg, error_code=_IMAGE_POLL_TIMEOUT_CODE)
            raise
        return _public_task(task)

    def _run_resume_poll(
        self,
        key: str,
        conversation_id: str,
        extra_timeout_secs: float,
        access_token: str,
        identity: dict[str, object],
        mode: str,
        model: str,
    ) -> None:
        """后台线程：继续轮询已有 conversation_id 的图片结果。"""
        started = time.time()
        backend = None
        try:
            from services.openai_backend_api import OpenAIBackendAPI
            from services.protocol.conversation import format_image_result

            access_token = account_service.refresh_access_token(
                access_token,
                event="image_resume_poll",
            ) or access_token
            with self._lock:
                if key in self._resume_credentials:
                    self._resume_credentials[key] = access_token
            backend = OpenAIBackendAPI(access_token=access_token)
            file_ids, sediment_ids = backend._poll_image_results(
                conversation_id,
                extra_timeout_secs,
            )
            if not file_ids and not sediment_ids:
                raise RuntimeError(
                    f"继续等待 {extra_timeout_secs} 秒后仍未找到图片结果。"
                )

            image_urls = backend.resolve_conversation_image_urls(
                conversation_id, file_ids, sediment_ids, poll=False,
            )
            if not image_urls:
                raise RuntimeError("图片 URL 解析失败")

            image_items = [
                {"b64_json": __import__("base64").b64encode(image_data).decode("ascii")}
                for image_data in backend.download_image_bytes(image_urls)
            ]
            # 获取 task 的原始 prompt（从 _public_task 的 mode 判断）
            with self._lock:
                task = self._tasks.get(key)
                quality = _clean(task.get("quality"), "auto") if task else "auto"
                size = _clean(task.get("size")) if task else None
            data = format_image_result(
                image_items,
                "",  # prompt 已不重要，结果已经拿到了
                "b64_json",
                "",
                int(time.time()),
            )["data"]
            self._transition_task(
                key,
                resume_credential=None,
                status=TASK_STATUS_SUCCESS,
                data=data,
                error="",
                error_code="",
                duration_ms=int((time.time() - started) * 1000),
            )
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用完成（续轮询）",
                status="success",
                urls=_collect_image_urls(data),
            )
        except StorageConflictError:
            return
        except Exception as exc:
            error_message = public_exception_message(exc, "resume poll failed")
            persisted_error = _persisted_image_error(error_message)
            duration_ms = int((time.time() - started) * 1000)
            try:
                self._transition_task(
                    key,
                    resume_credential=access_token if isinstance(exc, ImagePollTimeoutError) else None,
                    status=TASK_STATUS_ERROR,
                    error=persisted_error,
                    error_code=_IMAGE_POLL_TIMEOUT_CODE if isinstance(exc, ImagePollTimeoutError) else "",
                    data=[],
                    duration_ms=duration_ms,
                )
            except StorageConflictError:
                return
            except Exception:
                with self._lock:
                    task = self._tasks.get(key)
                    if task is not None and task.get("status") not in TERMINAL_STATUSES:
                        task.update({
                            "status": TASK_STATUS_ERROR,
                            "error": persisted_error,
                            "error_code": _IMAGE_POLL_TIMEOUT_CODE if isinstance(exc, ImagePollTimeoutError) else "",
                            "data": [],
                            "duration_ms": duration_ms,
                            "updated_at": _now_iso(),
                            "updated_ts": time.time(),
                        })
                _IMAGE_TASK_LOGGER.error("image task terminal state persistence failed")
            self._log_call(
                identity,
                mode,
                model,
                started,
                "调用失败（续轮询）",
                status="failed",
                error=error_message,
            )
        finally:
            if backend is not None:
                backend.close()


image_task_service = ImageTaskService(DATA_DIR / "image_tasks.json")
