from __future__ import annotations

import asyncio
from concurrent.futures import Future
import html
import logging
from queue import Empty, Full, Queue
import threading
import weakref
from functools import partial
from urllib.parse import quote

import anyio
from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from starlette.concurrency import iterate_in_threadpool
from pydantic import BaseModel, ConfigDict, StrictBool
from starlette.background import BackgroundTask

from api.support import require_admin_async, require_identity_async, resolve_image_base_url
from services.backup_service import BackupError, backup_service
from services.config import config
from services.image_service import (
    compress_images,
    delete_images,
    delete_to_target,
    download_images_zip,
    get_image_download_response,
    get_image_response,
    get_thumbnail_response,
    list_images,
    storage_stats,
    _MAX_IMAGE_ZIP_ITEMS,
)
from services.image_storage_service import ImageStorageError, image_storage_service
from services.image_tags_service import delete_tag, get_all_tags, set_tags
from services.log_service import log_service, run_log_in_threadpool
from services.proxy_service import proxy_settings, test_clearance, test_proxy
from services.protocol.error_response import PublicSafeValueError, public_exception_message
from services.storage.base import STORAGE_HEALTH_ERROR_MESSAGE


logger = logging.getLogger(__name__)
_STORAGE_HEALTH_THREAD_STATE = threading.local()
_ACCOUNT_STATS_THREAD_STATE = threading.local()
_IMAGE_IO_THREAD_CAPACITY = 8
_IMAGE_IO_THREAD_STATE = threading.local()
_SYSTEM_MANAGEMENT_IO_THREAD_CAPACITY = 4
_SYSTEM_MANAGEMENT_IO_THREAD_STATE = threading.local()
_HEALTH_OVERALL_TIMEOUT_SECONDS = 0.5
_HEALTH_SUBCHECK_TIMEOUT_SECONDS = 0.25
_HEALTH_SHUTDOWN_TIMEOUT_SECONDS = 0.5
_HEALTH_PROBE_STAGE_NAMES = (
    "account_stats",
    "storage_health",
    "storage_factory",
    "backend_info",
    "proxy_status",
)
_HEALTH_PROBE_GLOBAL_GUARDS = {
    stage: threading.Lock() for stage in _HEALTH_PROBE_STAGE_NAMES
}


class _DaemonHealthStage:
    """One bounded daemon worker for one health-probe stage."""

    def __init__(self, stage: str) -> None:
        self._stage = stage
        self._jobs: Queue[tuple[Future, object, tuple[object, ...]]] = Queue(maxsize=1)
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def submit(self, func, *args) -> Future:
        future: Future = Future()
        with self._lock:
            if self._thread is None or not self._thread.is_alive():
                self._thread = threading.Thread(
                    target=self._run,
                    name=f"health-probe-{self._stage}",
                    daemon=True,
                )
                self._thread.start()
            try:
                self._jobs.put_nowait((future, func, args))
            except Full:
                raise RuntimeError(f"health probe stage {self._stage} queue is full") from None
        return future

    def _run(self) -> None:
        while True:
            try:
                future, func, args = self._jobs.get(timeout=0.5)
            except Empty:
                with self._lock:
                    if self._jobs.empty():
                        self._thread = None
                        return
                continue
            try:
                if future.set_running_or_notify_cancel():
                    try:
                        future.set_result(func(*args))
                    except BaseException as exc:
                        future.set_exception(exc)
            finally:
                self._jobs.task_done()


_HEALTH_PROBE_STAGES = {
    stage: _DaemonHealthStage(stage) for stage in _HEALTH_PROBE_STAGE_NAMES
}


class _HealthProbeOwner:
    def __init__(self) -> None:
        self.tasks: dict[str, Future] = {}
        self.tasks_lock = threading.Lock()


_HEALTH_PROBE_OWNERS: weakref.WeakKeyDictionary[
    asyncio.AbstractEventLoop,
    weakref.ReferenceType[_HealthProbeOwner],
] = (
    weakref.WeakKeyDictionary()
)
_HEALTH_PROBE_OWNERS_LOCK = threading.Lock()


def _health_probe_owner() -> _HealthProbeOwner:
    loop = asyncio.get_running_loop()
    with _HEALTH_PROBE_OWNERS_LOCK:
        owner_ref = _HEALTH_PROBE_OWNERS.get(loop)
        owner = owner_ref() if owner_ref is not None else None
        if owner is None:
            owner = _HealthProbeOwner()
            _HEALTH_PROBE_OWNERS[loop] = weakref.ref(owner)
        return owner


def _consume_health_task_result(owner: _HealthProbeOwner, stage: str, task: Future) -> None:
    with owner.tasks_lock:
        if owner.tasks.get(stage) is task:
            del owner.tasks[stage]
    try:
        task.result()
    except BaseException:
        pass


async def wait_for_health_probe_tasks(timeout: float = _HEALTH_SHUTDOWN_TIMEOUT_SECONDS) -> bool:
    """Wait briefly for this event loop's workers without abandoning them."""
    owner = _health_probe_owner()
    deadline = anyio.current_time() + timeout
    while True:
        with owner.tasks_lock:
            tasks = tuple(owner.tasks.items())
        if not tasks:
            break
        for stage, task in tasks:
            if task.done():
                _consume_health_task_result(owner, stage, task)
        with owner.tasks_lock:
            pending_stages = tuple(owner.tasks)
        if not pending_stages:
            break
        remaining = max(0.0, deadline - anyio.current_time())
        if remaining <= 0:
            logger.warning("health probe shutdown timed out", extra={"pending_stages": pending_stages})
            return False
        await asyncio.sleep(min(0.01, remaining))
    return True


async def _bounded_health_io(
    stage: str,
    limiter: anyio.CapacityLimiter,
    func,
    *args,
    deadline: float | None = None,
):
    timeout = _HEALTH_SUBCHECK_TIMEOUT_SECONDS
    if deadline is not None:
        timeout = min(timeout, max(0.0, deadline - anyio.current_time()))
    if timeout <= 0:
        return None

    owner = _health_probe_owner()
    # The worker may outlive the request and even its event loop.  A loop-local
    # lock alone would let a new loop submit another copy of the same stuck
    # probe, so admission is guarded by the process-wide stage owner too.
    guard = _HEALTH_PROBE_GLOBAL_GUARDS[stage]
    if not guard.acquire(blocking=False):
        return None

    # Do not bind a late synchronous worker to the request's event loop.  The
    # public liveness route has a hard deadline, while the synchronous probe
    # may be inside an uninterruptible storage/proxy call.  A stage guard
    # keeps one such late call from multiplying on every health request; the
    # bounded stage worker keeps the total number of late calls finite.  The
    # concurrent future remains in the loop-scoped owner's registry until the
    # worker really finishes, so loop shutdown never has to drain it.
    try:
        future = _HEALTH_PROBE_STAGES[stage].submit(func, *args)
    except BaseException:
        guard.release()
        raise
    future.add_done_callback(lambda _future: guard.release())
    with owner.tasks_lock:
        owner.tasks[stage] = future
    future.add_done_callback(partial(_consume_health_task_result, owner, stage))
    try:
        end = anyio.current_time() + timeout
        while not future.done():
            remaining = end - anyio.current_time()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(0.01, remaining))
        return future.result()
    except BaseException:
        if future.done():
            _consume_health_task_result(owner, stage, future)
        raise


async def _storage_health_async(storage, *, deadline: float | None = None) -> dict | None:
    limiter = getattr(_STORAGE_HEALTH_THREAD_STATE, "limiter", None)
    if limiter is None:
        # Git health checks may pull a repository. Serialize them outside the
        # event loop so concurrent probes cannot freeze unrelated API traffic.
        limiter = anyio.CapacityLimiter(1)
        _STORAGE_HEALTH_THREAD_STATE.limiter = limiter
    return await _bounded_health_io("storage_health", limiter, storage.health_check, deadline=deadline)


async def _account_stats_async(account_service, *, deadline: float | None = None) -> dict | None:
    limiter = getattr(_ACCOUNT_STATS_THREAD_STATE, "limiter", None)
    if limiter is None:
        # Account mutations can hold the service lock while a storage snapshot
        # is committed. A public health probe must never wait on that lock in
        # the single ASGI event-loop thread.
        limiter = anyio.CapacityLimiter(1)
        _ACCOUNT_STATS_THREAD_STATE.limiter = limiter
    return await _bounded_health_io("account_stats", limiter, account_service.get_stats, deadline=deadline)


def _image_io_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_IMAGE_IO_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_IMAGE_IO_THREAD_CAPACITY)
        _IMAGE_IO_THREAD_STATE.limiter = limiter
    return limiter


async def run_image_io(func, *args, **kwargs):
    return await anyio.to_thread.run_sync(
        partial(func, *args, **kwargs),
        limiter=_image_io_thread_limiter(),
    )


def _system_management_io_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_SYSTEM_MANAGEMENT_IO_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_SYSTEM_MANAGEMENT_IO_THREAD_CAPACITY)
        _SYSTEM_MANAGEMENT_IO_THREAD_STATE.limiter = limiter
    return limiter


async def run_system_management_io(func, *args, **kwargs):
    return await anyio.to_thread.run_sync(
        partial(func, *args, **kwargs),
        limiter=_system_management_io_thread_limiter(),
    )


async def _bounded_system_management_health_io(
    stage: str,
    func,
    *args,
    deadline: float | None = None,
):
    return await _bounded_health_io(
        stage,
        _system_management_io_thread_limiter(),
        func,
        *args,
        deadline=deadline,
    )


class SettingsUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")


_SETTINGS_UPDATE_FIELDS = frozenset(
    {
        "proxy",
        "base_url",
        "global_system_prompt",
        "default_upstream_model_name",
        "default_thinking_effort",
        "sensitive_words",
        "ai_review",
        "refresh_account_interval_minute",
        "image_retention_days",
        "image_poll_timeout_secs",
        "image_poll_interval_secs",
        "image_poll_initial_wait_secs",
        "image_account_concurrency",
        "image_parallel_generation",
        "image_settle_enabled",
        "image_check_before_hit_enabled",
        "image_remove_conversation_after_result",
        "image_remove_conversation_always",
        "image_settle_secs",
        "image_timeout_retry_secs",
        "auto_remove_invalid_accounts",
        "auto_remove_rate_limited_accounts",
        "auto_relogin_after_refresh",
        "log_levels",
        "image_storage",
        "proxy_runtime",
        "third_party_apps",
        "backup",
    }
)


def _validate_settings_update_fields(payload: dict[str, object]) -> None:
    if set(payload).difference(_SETTINGS_UPDATE_FIELDS):
        raise HTTPException(status_code=400, detail={"error": "配置更新包含不支持的字段"})


class ProxyTestRequest(BaseModel):
    url: str = ""


class ClearanceTestRequest(BaseModel):
    target_url: str = "https://chatgpt.com"


class ImageDeleteRequest(BaseModel):
    paths: list[str] = []
    start_date: str = ""
    end_date: str = ""
    all_matching: StrictBool = False

class ImageDownloadRequest(BaseModel):
    paths: list[str]


class _ClosableSyncBody:
    def __init__(self, source: object) -> None:
        self._source = source
        self._iterator = None
        self._closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._closed:
            raise StopAsyncIteration
        if self._iterator is None:
            self._iterator = iterate_in_threadpool(self._source)
        try:
            return await self._iterator.__anext__()
        except StopAsyncIteration:
            await self.aclose()
            raise
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            if self._iterator is not None:
                await self._iterator.aclose()
        finally:
            close = getattr(self._source, "close", None)
            if callable(close):
                close()

class ImageTagsRequest(BaseModel):
    path: str
    tags: list[str]

class LogDeleteRequest(BaseModel):
    ids: list[str] = []
class BackupDeleteRequest(BaseModel):
    key: str = ""


_PUBLIC_STORAGE_BACKEND_TYPES = frozenset({"json", "database", "git"})
_PUBLIC_ACCOUNT_TYPES = frozenset({"free", "Plus", "Pro", "ProLite", "Team", "Enterprise"})
_ACCOUNT_STAT_FIELDS = (
    "total",
    "cumulative_total",
    "active",
    "limited",
    "abnormal",
    "disabled",
    "total_quota",
    "total_success",
    "total_fail",
)


def _empty_account_stats() -> dict[str, object]:
    return {
        "total": 0,
        "cumulative_total": 0,
        "active": 0,
        "limited": 0,
        "abnormal": 0,
        "disabled": 0,
        "total_quota": 0,
        "total_success": 0,
        "total_fail": 0,
        "by_type": {},
    }


def _public_account_stats(value: object) -> dict[str, object]:
    """Project health counters and bucket unknown account metadata."""
    source = value if isinstance(value, dict) else {}
    projected: dict[str, object] = {}
    for field in _ACCOUNT_STAT_FIELDS:
        item = source.get(field)
        projected[field] = item if type(item) is int and item >= 0 else 0

    by_type: dict[str, int] = {}
    raw_by_type = source.get("by_type")
    if isinstance(raw_by_type, dict):
        for raw_type, raw_count in raw_by_type.items():
            if type(raw_count) is not int or raw_count < 0:
                continue
            bucket = raw_type if isinstance(raw_type, str) and raw_type in _PUBLIC_ACCOUNT_TYPES else "other"
            by_type[bucket] = by_type.get(bucket, 0) + raw_count
    projected["by_type"] = by_type
    return projected


def _public_storage_snapshot(storage_info: object, storage_health: object) -> tuple[dict[str, object], bool]:
    info = storage_info if isinstance(storage_info, dict) else {}
    backend_type = info.get("type") or info.get("backend")
    info_healthy = isinstance(backend_type, str) and backend_type in _PUBLIC_STORAGE_BACKEND_TYPES
    if not info_healthy:
        backend_type = "unknown"
    storage_healthy = (
        info_healthy
        and isinstance(storage_health, dict)
        and storage_health.get("status") == "healthy"
    )
    health: dict[str, object] = {
        "status": "healthy" if storage_healthy else "unhealthy",
    }
    if not storage_healthy:
        health["error"] = STORAGE_HEALTH_ERROR_MESSAGE
    return {"backend": backend_type, "health": health}, storage_healthy


def _public_storage_info(storage_info: object, storage_health: object) -> dict[str, object]:
    """Project storage diagnostics without exposing paths or backend internals."""
    info = storage_info if isinstance(storage_info, dict) else {}
    backend_type = info.get("type") or info.get("backend")
    if not isinstance(backend_type, str) or backend_type not in _PUBLIC_STORAGE_BACKEND_TYPES:
        backend_type = "unknown"
    backend: dict[str, object] = {"type": backend_type}
    db_type = info.get("db_type")
    if (
        backend_type == "database"
        and isinstance(db_type, str)
        and db_type in {"sqlite", "postgresql", "mysql", "unknown"}
    ):
        backend["db_type"] = db_type

    healthy = isinstance(storage_health, dict) and storage_health.get("status") == "healthy"
    health: dict[str, object] = {"status": "healthy" if healthy else "unhealthy"}
    if not healthy:
        health["error"] = STORAGE_HEALTH_ERROR_MESSAGE
    return {"backend": backend, "health": health}


def _public_proxy_runtime_snapshot(runtime_status: object) -> dict[str, bool]:
    status = runtime_status if isinstance(runtime_status, dict) else {}
    return {
        "enabled": status.get("enabled") is True,
        "clearance_enabled": status.get("clearance_enabled") is True,
    }


def _public_proxy_runtime_status(value: object) -> dict[str, object]:
    """Expose proxy state without cached hosts or operational internals."""
    source = value if isinstance(value, dict) else {}
    egress_mode = source.get("egress_mode")
    if not isinstance(egress_mode, str) or egress_mode not in {"direct", "single_proxy"}:
        egress_mode = "unknown"
    proxy_source = source.get("proxy_source")
    if not isinstance(proxy_source, str) or proxy_source not in {"direct", "account", "runtime", "runtime_resource", "proxy_runtime", "explicit", "global"}:
        proxy_source = "unknown"
    clearance_mode = source.get("clearance_mode")
    if not isinstance(clearance_mode, str) or clearance_mode not in {"none", "manual", "flaresolverr"}:
        clearance_mode = "unknown"
    return {
        "enabled": source.get("enabled") is True,
        "egress_mode": egress_mode,
        "proxy_source": proxy_source,
        "has_proxy": source.get("has_proxy") is True,
        "clearance_enabled": source.get("clearance_enabled") is True,
        "clearance_mode": clearance_mode,
        "has_clearance_bundle": source.get("has_clearance_bundle") is True,
        "cached_clearance_hosts": [],
    }


_PUBLIC_CLEARANCE_STATUSES = frozenset({"disabled", "error", "failed", "ok"})
_PUBLIC_CLEARANCE_ERRORS = frozenset({
    "clearance is disabled",
    "通关测试失败，请稍后重试",
    "clearance refresh returned no bundle",
})


def _public_clearance_test_result(value: object) -> dict[str, object]:
    """Project clearance diagnostics without returning runtime internals."""
    source = value if isinstance(value, dict) else {}
    status = source.get("status")
    if not isinstance(status, str) or status not in _PUBLIC_CLEARANCE_STATUSES:
        status = "error"
    latency_ms = source.get("latency_ms")
    if type(latency_ms) is not int or latency_ms < 0:
        latency_ms = 0
    raw_error = source.get("error")
    if raw_error is None:
        error = None
    elif isinstance(raw_error, str) and raw_error in _PUBLIC_CLEARANCE_ERRORS:
        error = raw_error
    else:
        error = "通关测试失败，请稍后重试"
    return {
        "ok": source.get("ok") is True,
        "status": status,
        "latency_ms": latency_ms,
        "has_cookies": source.get("has_cookies") is True,
        "user_agent": "",
        "error": error,
        "runtime": _public_proxy_runtime_status(source.get("runtime")),
    }


def _html_text(value: object) -> str:
    return html.escape(str(value), quote=True)


def create_router(app_version: str) -> APIRouter:
    router = APIRouter()

    @router.post("/auth/login")
    async def login(authorization: str | None = Header(default=None)):
        identity = await require_identity_async(authorization)
        return {
            "ok": True,
            "version": app_version,
            "role": identity.get("role"),
            "subject_id": identity.get("id"),
            "name": identity.get("name"),
        }

    @router.get("/version")
    async def get_version():
        return {"version": app_version}

    @router.get("/api/settings")
    async def get_settings(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"config": await run_system_management_io(config.get)}

    @router.get("/api/third-party-apps")
    async def get_third_party_apps(authorization: str | None = Header(default=None)):
        await require_identity_async(authorization)
        return {"third_party_apps": config.get_third_party_apps_settings()}

    @router.post("/api/settings")
    async def save_settings(body: SettingsUpdateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        payload = body.model_dump(mode="python")
        _validate_settings_update_fields(payload)
        try:
            return {
                "config": await run_system_management_io(
                    config.update,
                    payload,
                )
            }
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "配置更新失败，请稍后重试")},
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "配置更新失败，请稍后重试"}) from exc

    @router.get("/api/images")
    async def get_images(request: Request, start_date: str = "", end_date: str = "", authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return await run_image_io(
            list_images,
            resolve_image_base_url(request),
            start_date=start_date.strip(),
            end_date=end_date.strip(),
        )

    @router.get("/images/{image_path:path}", include_in_schema=False)
    async def get_image(image_path: str):
        return await run_image_io(get_image_response, image_path)

    @router.get("/image-thumbnails/{image_path:path}", include_in_schema=False)
    async def get_image_thumbnail(image_path: str):
        return await run_image_io(get_thumbnail_response, image_path)

    @router.post("/api/images/delete")
    async def delete_images_endpoint(body: ImageDeleteRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return await run_image_io(
            delete_images,
            body.paths,
            start_date=body.start_date.strip(),
            end_date=body.end_date.strip(),
            all_matching=body.all_matching,
        )

    @router.post("/api/images/download")
    async def download_images_endpoint(body: ImageDownloadRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        if len(body.paths) > _MAX_IMAGE_ZIP_ITEMS:
            raise HTTPException(status_code=413, detail="too many images in archive request")
        buf = await run_image_io(download_images_zip, body.paths)
        try:
            return StreamingResponse(
                _ClosableSyncBody(buf),
                media_type="application/zip",
                headers={"Content-Disposition": 'attachment; filename="images.zip"'},
                background=BackgroundTask(buf.close),
            )
        except Exception:
            buf.close()
            raise

    @router.get("/api/images/download/{image_path:path}")
    async def download_single_image_endpoint(image_path: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return await run_image_io(get_image_download_response, image_path)

    @router.get("/api/logs")
    async def get_logs(type: str = "", start_date: str = "", end_date: str = "", authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        items = await run_log_in_threadpool(
            log_service.list,
            type=type.strip(),
            start_date=start_date.strip(),
            end_date=end_date.strip(),
        )
        return {"items": items}

    @router.post("/api/logs/delete")
    async def delete_logs(body: LogDeleteRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return await run_log_in_threadpool(log_service.delete, body.ids)

    @router.post("/api/proxy/test")
    async def test_proxy_endpoint(body: ProxyTestRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"result": await run_system_management_io(test_proxy, (body.url or "").strip())}

    @router.get("/api/proxy/runtime")
    async def get_proxy_runtime_endpoint(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {
            "runtime": config.get_public_proxy_runtime_settings(),
            "status": _public_proxy_runtime_status(proxy_settings.get_runtime_status()),
        }

    @router.post("/api/proxy/runtime")
    async def save_proxy_runtime_endpoint(body: SettingsUpdateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            await run_system_management_io(
                config.update,
                {"proxy_runtime": body.model_dump(mode="python")},
            )
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "代理运行时配置更新失败，请稍后重试")},
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "代理运行时配置更新失败，请稍后重试"}) from exc
        return {
            "runtime": config.get_public_proxy_runtime_settings(),
            "status": _public_proxy_runtime_status(proxy_settings.get_runtime_status()),
        }

    @router.post("/api/proxy/clearance/test")
    async def test_proxy_clearance_endpoint(body: ClearanceTestRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {
            "result": _public_clearance_test_result(
                await run_system_management_io(test_clearance, body.target_url)
            )
        }

    @router.get("/api/storage/info")
    async def get_storage_info(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        storage = await run_system_management_io(config.get_storage_backend)
        return _public_storage_info(
            await run_system_management_io(storage.get_backend_info),
            await _storage_health_async(storage),
        )

    @router.post("/api/backup/test")
    async def test_backup_connection(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            return {"result": await run_system_management_io(backup_service.test_connection)}
        except BackupError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "备份操作失败，请稍后重试")},
            ) from exc

    @router.post("/api/image-storage/test")
    async def test_image_storage_endpoint(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"result": await run_image_io(image_storage_service.test_webdav)}

    @router.post("/api/image-storage/sync")
    async def sync_image_storage_endpoint(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            return {"result": await run_image_io(image_storage_service.sync_all)}
        except ImageStorageError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "图片存储操作失败，请稍后重试")},
            ) from exc

    @router.get("/api/backups")
    async def get_backups(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            return {
                "items": await run_system_management_io(backup_service.list_backups),
                "state": await run_system_management_io(backup_service.get_status),
                "settings": await run_system_management_io(backup_service.get_settings),
            }
        except BackupError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "备份操作失败，请稍后重试")},
            ) from exc

    @router.post("/api/backups/run")
    async def run_backup_endpoint(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            return {"result": await run_system_management_io(backup_service.run_backup)}
        except BackupError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "备份操作失败，请稍后重试")},
            ) from exc

    @router.post("/api/backups/delete")
    async def delete_backup_endpoint(body: BackupDeleteRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            await run_system_management_io(backup_service.delete_backup, body.key)
            return {"ok": True}
        except BackupError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "备份操作失败，请稍后重试")},
            ) from exc

    @router.get("/api/backups/detail")
    async def get_backup_detail(key: str = "", authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            return {"item": await run_system_management_io(backup_service.get_backup_detail, key)}
        except BackupError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "备份操作失败，请稍后重试")},
            ) from exc

    @router.get("/api/backups/download")
    async def download_backup_endpoint(key: str = "", authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            item = await run_system_management_io(backup_service.download_backup, key)
        except BackupError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "备份操作失败，请稍后重试")},
            ) from exc
        filename = str(item.get("name") or "backup.bin")
        quoted = quote(filename)
        headers = {
            "Content-Disposition": f"attachment; filename*=UTF-8''{quoted}",
            "Content-Length": str(int(item.get("size") or 0)),
        }
        return Response(
            content=bytes(item.get("payload") or b""),
            media_type=str(item.get("content_type") or "application/octet-stream"),
            headers=headers,
        )


    @router.get("/api/images/tags")
    async def list_image_tags(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"tags": await run_image_io(get_all_tags)}

    @router.post("/api/images/tags")
    async def update_image_tags(body: ImageTagsRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        rel = body.path.strip().lstrip("/")
        if not rel:
            raise HTTPException(status_code=400, detail={"error": "path is required"})
        try:
            tags = await run_image_io(set_tags, rel, body.tags)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "标签数量或长度超出限制"}) from exc
        return {"ok": True, "tags": tags}

    @router.delete("/api/images/tags/{tag}")
    async def delete_image_tag(tag: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        count = await run_image_io(delete_tag, tag)
        return {"ok": True, "removed_from": count}

    @router.get("/api/images/storage")
    async def get_image_storage(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return await run_image_io(storage_stats)

    @router.post("/api/images/storage/compress")
    async def compress_all_images(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return await run_image_io(compress_images)

    @router.post("/api/images/storage/cleanup-to-target")
    async def cleanup_to_target(
        target_free_mb: int = 500,
        dry_run: bool = False,
        authorization: str | None = Header(default=None),
    ):
        await require_admin_async(authorization)
        return await run_image_io(delete_to_target, target_free_mb, dry_run)

    @router.get("/health", response_model=None)
    async def health_dashboard(format: str = Query(default="html")):
        from services.account_service import account_service as acct_svc
        stages: dict[str, object] = {}
        deadline = anyio.current_time() + _HEALTH_OVERALL_TIMEOUT_SECONDS

        async def run_stage(name: str, operation) -> None:
            try:
                stages[name] = await operation()
            except Exception:
                stages[name] = None

        async with anyio.create_task_group() as task_group:
            task_group.start_soon(
                run_stage,
                "stats",
                lambda: _account_stats_async(acct_svc, deadline=deadline),
            )
            task_group.start_soon(
                run_stage,
                "storage",
                lambda: _bounded_system_management_health_io(
                    "storage_factory",
                    config.get_storage_backend,
                    deadline=deadline,
                ),
            )
            task_group.start_soon(
                run_stage,
                "proxy",
                lambda: _bounded_system_management_health_io(
                    "proxy_status",
                    proxy_settings.get_runtime_status,
                    deadline=deadline,
                ),
            )

        raw_stats = stages.get("stats") or _empty_account_stats()
        stats = _public_account_stats(raw_stats)
        storage_health: object = {}
        backend_info: object = {}
        storage = stages.get("storage")
        if storage is not None:
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(
                    run_stage,
                    "storage_health",
                    lambda: _storage_health_async(storage, deadline=deadline),
                )
                task_group.start_soon(
                    run_stage,
                    "backend_info",
                    lambda: _bounded_system_management_health_io(
                        "backend_info",
                        storage.get_backend_info,
                        deadline=deadline,
                    ),
                )
            storage_health = stages.get("storage_health") or {}
            backend_info = stages.get("backend_info") or {}
            if stages.get("storage_health") is None:
                backend_info = {}
        public_storage, storage_healthy = _public_storage_snapshot(
            backend_info,
            storage_health,
        )
        healthy = stats["active"] > 0 and storage_healthy
        raw_proxy_runtime = stages.get("proxy") or {}

        stats_json = {
            "status": "ok" if healthy else "degraded",
            "healthy": healthy,
            "version": app_version,
            "storage": public_storage,
            "proxy_runtime": _public_proxy_runtime_snapshot(raw_proxy_runtime),
            "accounts": stats,
        }
        if format == "json":
            return stats_json
        return HTMLResponse(f"""<!DOCTYPE html>
<html lang="zh">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>号池健康监控 - chatgpt2api</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:system-ui,-apple-system,sans-serif;background:#0f1117;color:#e2e8f0;min-height:100vh}}
.header{{background:#1a1d27;border-bottom:1px solid #2a2d3a;padding:16px 24px;display:flex;justify-content:space-between;align-items:center}}
.header h1{{font-size:20px}}
.status-dot{{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:8px}}
.status-ok{{background:#22c55e;box-shadow:0 0 8px #22c55e88}}
.status-degraded{{background:#f59e0b;box-shadow:0 0 8px #f59e0b88}}
.container{{max-width:960px;margin:0 auto;padding:24px}}
.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin-bottom:24px}}
.card{{background:#1a1d27;border:1px solid #2a2d3a;border-radius:10px;padding:16px}}
.card .value{{font-size:28px;font-weight:700;margin:4px 0}}
.card .label{{font-size:13px;color:#94a3b8}}
.green{{color:#22c55e}}.yellow{{color:#f59e0b}}.red{{color:#ef4444}}.blue{{color:#6c63ff}}
table{{width:100%;border-collapse:collapse;background:#1a1d27;border:1px solid #2a2d3a;border-radius:10px;overflow:hidden}}
th{{background:#242836;font-weight:600;text-align:left;padding:10px 12px;font-size:12px;color:#94a3b8;text-transform:uppercase}}
td{{padding:8px 12px;border-top:1px solid #2a2d3a;font-size:14px}}tr:hover td{{background:rgba(108,99,255,.05)}}
.api-url{{font-family:monospace;font-size:12px;color:#6c63ff}}
.refresh{{font-size:12px;color:#64748b;text-align:center;margin-top:24px}}
</style>
<meta http-equiv="refresh" content="30">
</head>
<body>
<div class="header">
<h1><span class="status-dot {'status-ok' if healthy else 'status-degraded'}"></span>号池健康监控</h1>
<div style="font-size:13px;color:#94a3b8">v{_html_text(app_version)} · 30s 自动刷新</div>
</div>
<div class="container">
<div class="cards">
<div class="card"><div class="label">号池状态</div><div class="value {'green' if healthy else 'yellow'}">{'正常' if healthy else '异常'}</div></div>
<div class="card"><div class="label">当前账号</div><div class="value blue">{_html_text(stats['total'])}</div></div>
<div class="card"><div class="label">累计入库</div><div class="value">{_html_text(stats['cumulative_total'])}</div></div>
<div class="card"><div class="label">可用账号</div><div class="value green">{_html_text(stats['active'])}</div></div>
<div class="card"><div class="label">剩余额度</div><div class="value">{_html_text(stats['total_quota'])}</div></div>
<div class="card"><div class="label">限流</div><div class="value yellow">{_html_text(stats['limited'])}</div></div>
<div class="card"><div class="label">异常</div><div class="value red">{_html_text(stats['abnormal'])}</div></div>
<div class="card"><div class="label">禁用</div><div class="value">{_html_text(stats['disabled'])}</div></div>
<div class="card"><div class="label">成功/失败</div><div class="value">{_html_text(stats['total_success'])}<span style="font-size:18px;color:#94a3b8">/</span><span class="red">{_html_text(stats['total_fail'])}</span></div></div>
</div>
<h2 style="margin-bottom:12px;font-size:16px">账号类型分布</h2>
<table>
<tr><th>类型</th><th>数量</th></tr>
{''.join(f'<tr><td>{_html_text(t)}</td><td>{_html_text(c)}</td></tr>' for t,c in sorted(stats['by_type'].items()))}
</table>
<div class="refresh">JSON: <span class="api-url">/health?format=json</span></div>
</div></body></html>""")

    return router
