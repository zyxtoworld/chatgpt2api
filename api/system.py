from __future__ import annotations

import html
import threading
from functools import partial
from urllib.parse import quote

import anyio
from fastapi import APIRouter, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel, ConfigDict

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
)
from services.image_storage_service import ImageStorageError, image_storage_service
from services.image_tags_service import delete_tag, get_all_tags, set_tags
from services.log_service import log_service, run_log_in_threadpool
from services.proxy_service import proxy_settings, test_clearance, test_proxy
from services.protocol.error_response import PublicSafeValueError, public_exception_message
from services.storage.base import STORAGE_HEALTH_ERROR_MESSAGE


_STORAGE_HEALTH_THREAD_STATE = threading.local()
_ACCOUNT_STATS_THREAD_STATE = threading.local()
_IMAGE_IO_THREAD_CAPACITY = 8
_IMAGE_IO_THREAD_STATE = threading.local()
_SYSTEM_MANAGEMENT_IO_THREAD_CAPACITY = 4
_SYSTEM_MANAGEMENT_IO_THREAD_STATE = threading.local()


async def _storage_health_async(storage) -> dict:
    limiter = getattr(_STORAGE_HEALTH_THREAD_STATE, "limiter", None)
    if limiter is None:
        # Git health checks may pull a repository. Serialize them outside the
        # event loop so concurrent probes cannot freeze unrelated API traffic.
        limiter = anyio.CapacityLimiter(1)
        _STORAGE_HEALTH_THREAD_STATE.limiter = limiter
    return await anyio.to_thread.run_sync(storage.health_check, limiter=limiter)


async def _account_stats_async(account_service) -> dict:
    limiter = getattr(_ACCOUNT_STATS_THREAD_STATE, "limiter", None)
    if limiter is None:
        # Account mutations can hold the service lock while a storage snapshot
        # is committed. A public health probe must never wait on that lock in
        # the single ASGI event-loop thread.
        limiter = anyio.CapacityLimiter(1)
        _ACCOUNT_STATS_THREAD_STATE.limiter = limiter
    return await anyio.to_thread.run_sync(account_service.get_stats, limiter=limiter)


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


class SettingsUpdateRequest(BaseModel):
    model_config = ConfigDict(extra="allow")


class ProxyTestRequest(BaseModel):
    url: str = ""


class ClearanceTestRequest(BaseModel):
    target_url: str = "https://chatgpt.com"


class ImageDeleteRequest(BaseModel):
    paths: list[str] = []
    start_date: str = ""
    end_date: str = ""
    all_matching: bool = False

class ImageDownloadRequest(BaseModel):
    paths: list[str]

class ImageTagsRequest(BaseModel):
    path: str
    tags: list[str]

class LogDeleteRequest(BaseModel):
    ids: list[str] = []
class BackupDeleteRequest(BaseModel):
    key: str = ""


_PUBLIC_STORAGE_BACKEND_TYPES = frozenset({"json", "database", "git"})


def _public_storage_snapshot(storage_info: object, storage_health: object) -> tuple[dict[str, object], bool]:
    info = storage_info if isinstance(storage_info, dict) else {}
    backend_type = info.get("type") or info.get("backend")
    if not isinstance(backend_type, str) or backend_type not in _PUBLIC_STORAGE_BACKEND_TYPES:
        backend_type = "unknown"
    storage_healthy = isinstance(storage_health, dict) and storage_health.get("status") == "healthy"
    health: dict[str, object] = {
        "status": "healthy" if storage_healthy else "unhealthy",
    }
    if not storage_healthy:
        health["error"] = STORAGE_HEALTH_ERROR_MESSAGE
    return {"backend": backend_type, "health": health}, storage_healthy


def _public_proxy_runtime_snapshot(runtime_status: object) -> dict[str, bool]:
    status = runtime_status if isinstance(runtime_status, dict) else {}
    return {
        "enabled": status.get("enabled") is True,
        "clearance_enabled": status.get("clearance_enabled") is True,
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
        return {"config": config.get()}

    @router.get("/api/third-party-apps")
    async def get_third_party_apps(authorization: str | None = Header(default=None)):
        await require_identity_async(authorization)
        return {"third_party_apps": config.get_third_party_apps_settings()}

    @router.post("/api/settings")
    async def save_settings(body: SettingsUpdateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            return {
                "config": await run_system_management_io(
                    config.update,
                    body.model_dump(mode="python"),
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
        buf = await run_image_io(download_images_zip, body.paths)
        return StreamingResponse(
            buf,
            media_type="application/zip",
            headers={"Content-Disposition": 'attachment; filename="images.zip"'},
        )

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
            "status": proxy_settings.get_runtime_status(),
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
            "status": proxy_settings.get_runtime_status(),
        }

    @router.post("/api/proxy/clearance/test")
    async def test_proxy_clearance_endpoint(body: ClearanceTestRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"result": await run_system_management_io(test_clearance, body.target_url)}

    @router.get("/api/storage/info")
    async def get_storage_info(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        storage = config.get_storage_backend()
        return {
            "backend": storage.get_backend_info(),
            "health": await _storage_health_async(storage),
        }

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
                "state": backup_service.get_status(),
                "settings": backup_service.get_settings(),
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
        tags = await run_image_io(set_tags, rel, body.tags)
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
        stats = await _account_stats_async(acct_svc)
        storage = config.get_storage_backend()
        storage_health = await _storage_health_async(storage)
        public_storage, storage_healthy = _public_storage_snapshot(
            storage.get_backend_info(),
            storage_health,
        )
        healthy = stats["active"] > 0 and storage_healthy

        stats_json = {
            "status": "ok" if healthy else "degraded",
            "healthy": healthy,
            "version": app_version,
            "storage": public_storage,
            "proxy_runtime": _public_proxy_runtime_snapshot(proxy_settings.get_runtime_status()),
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
