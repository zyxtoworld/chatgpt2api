from __future__ import annotations

import ipaddress
import hmac
import threading
from pathlib import Path
from threading import Event, Thread
from urllib.parse import urlsplit

import anyio
from fastapi import HTTPException, Request

from services.account_service import account_service
from services.auth_service import auth_service
from services.config import config, parse_public_url
from services.model_contract import parse_model_text
from services.protocol.error_response import (
    PUBLIC_SERVER_ERROR_MESSAGE,
    exception_log_message,
    sanitize_import_job_errors,
)
from services.secure_file import OpenedFile, authorized_root, open_checked_file, resolve_under_root
from services.task_executor import BackgroundTaskQueueFullError, reserve_background_task

BASE_DIR = Path(__file__).resolve().parents[1]
WEB_DIST_DIR = BASE_DIR / "web_dist"
_LOCAL_IMAGE_HOSTS = {"localhost", "testserver"}
_AUTH_THREAD_CAPACITY = 4
_AUTH_THREAD_STATE = threading.local()


def extract_bearer_token(authorization: str | None) -> str:
    scheme, _, value = str(authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not value.strip():
        return ""
    return value.strip()


def _legacy_admin_identity(token: str) -> dict[str, object] | None:
    auth_key = str(config.auth_key or "").strip()
    if auth_key and hmac.compare_digest(token, auth_key):
        return {"id": "admin", "name": "管理员", "role": "admin"}
    return None


def require_identity(authorization: str | None) -> dict[str, object]:
    token = extract_bearer_token(authorization)
    identity = _legacy_admin_identity(token) or auth_service.authenticate(token)
    if identity is None:
        raise HTTPException(status_code=401, detail={"error": "密钥无效或已失效，请重新登录"})
    return identity


def require_auth_key(authorization: str | None) -> None:
    require_identity(authorization)


def require_admin(authorization: str | None) -> dict[str, object]:
    identity = require_identity(authorization)
    if identity.get("role") != "admin":
        raise HTTPException(status_code=403, detail={"error": "需要管理员权限才能执行这个操作"})
    return identity


async def require_identity_async(authorization: str | None) -> dict[str, object]:
    token = extract_bearer_token(authorization)
    identity = _legacy_admin_identity(token)
    if identity is None:
        limiter = getattr(_AUTH_THREAD_STATE, "limiter", None)
        if limiter is None:
            limiter = anyio.CapacityLimiter(_AUTH_THREAD_CAPACITY)
            _AUTH_THREAD_STATE.limiter = limiter
        identity = await anyio.to_thread.run_sync(
            auth_service.authenticate,
            token,
            limiter=limiter,
        )
    if identity is None:
        raise HTTPException(status_code=401, detail={"error": "密钥无效或已失效，请重新登录"})
    return identity


async def require_admin_async(authorization: str | None) -> dict[str, object]:
    identity = await require_identity_async(authorization)
    if identity.get("role") != "admin":
        raise HTTPException(status_code=403, detail={"error": "需要管理员权限才能执行这个操作"})
    return identity


def _parse_request_authority(value: object) -> tuple[str, int | None] | None:
    host = str(value or "")
    if not host or host != host.strip() or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in host):
        return None
    try:
        parsed = urlsplit(f"//{host}")
        if parsed.netloc != host or parsed.path or parsed.query or parsed.fragment:
            return None
        if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
            return None
        port = parsed.port
    except (TypeError, ValueError):
        return None
    if host.endswith(":"):
        return None
    if not host.startswith("[") and host.count(":") > 1:
        return None
    hostname = str(parsed.hostname or "").lower()
    if not hostname:
        return None
    return hostname, port


def resolve_image_base_url(request: Request) -> str:
    configured_value = str(config.base_url or "").strip()
    if configured_value:
        return parse_public_url(configured_value)

    host = str(request.headers.get("host") or request.url.netloc or "")
    parsed_authority = _parse_request_authority(host)
    if parsed_authority is None:
        return ""
    hostname, _ = parsed_authority
    is_local = hostname in _LOCAL_IMAGE_HOSTS
    if not is_local:
        try:
            is_local = ipaddress.ip_address(hostname).is_loopback
        except ValueError:
            is_local = False
    if not is_local:
        # 没有受信任的公开基址时返回相对 URL，不能把任意 Host 头写进持久化/响应 URL。
        return ""
    return f"{request.url.scheme}://{host}"


def raise_image_quota_error(exc: Exception) -> None:
    message = str(exc)
    if "no available image quota" in message.lower():
        raise HTTPException(status_code=429, detail={"error": "no available image quota"}) from exc
    raise HTTPException(status_code=502, detail={"error": PUBLIC_SERVER_ERROR_MESSAGE}) from exc


def sanitize_import_job(value: object) -> dict | None:
    if not isinstance(value, dict):
        return None
    sanitized: dict[str, object] = {}
    allowed_statuses = {"pending", "running", "completed", "failed"}
    for key in ("job_id", "status", "created_at", "updated_at"):
        field_value = value.get(key)
        if isinstance(field_value, str):
            normalized = field_value.strip()
            if key == "status":
                sanitized[key] = normalized if normalized in allowed_statuses else "failed"
            elif len(normalized) <= 256:
                sanitized[key] = normalized
    for key in ("total", "completed", "added", "skipped", "refreshed", "failed"):
        field_value = value.get(key)
        if type(field_value) is int and field_value >= 0:
            sanitized[key] = field_value
    sanitized["errors"] = sanitize_import_job_errors(value.get("errors"))
    return sanitized


def sanitize_cpa_pool(pool: dict | None) -> dict | None:
    if not isinstance(pool, dict):
        return None
    sanitized = {
        key: pool[key]
        for key in ("id", "name")
        if isinstance(pool.get(key), str)
    }
    if isinstance(pool.get("base_url"), str):
        safe_base_url = _sanitize_management_base_url(pool["base_url"])
        if safe_base_url:
            sanitized["base_url"] = safe_base_url
    if "import_job" in pool:
        sanitized["import_job"] = sanitize_import_job(pool.get("import_job"))
    return sanitized


def sanitize_cpa_pools(pools: list[dict]) -> list[dict]:
    return [sanitized for pool in pools if (sanitized := sanitize_cpa_pool(pool)) is not None]


def _has_nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _sanitize_management_base_url(value: object) -> str:
    return parse_public_url(value)


def sanitize_sub2api_server(server: dict | None) -> dict | None:
    if not isinstance(server, dict):
        return None
    sanitized = {
        key: server[key]
        for key in ("id", "name", "email", "group_id")
        if isinstance(server.get(key), str)
    }
    if isinstance(server.get("base_url"), str):
        safe_base_url = _sanitize_management_base_url(server["base_url"])
        if safe_base_url:
            sanitized["base_url"] = safe_base_url
    sanitized["has_api_key"] = _has_nonempty_text(server.get("api_key"))
    if "import_job" in server:
        sanitized["import_job"] = sanitize_import_job(server.get("import_job"))
    return sanitized


def sanitize_sub2api_servers(servers: list[dict]) -> list[dict]:
    return [sanitized for server in servers if (sanitized := sanitize_sub2api_server(server)) is not None]


def sanitize_ccload_server(server: dict | None) -> dict | None:
    if not isinstance(server, dict):
        return None
    sanitized = {
        key: server[key]
        for key in ("id", "name")
        if isinstance(server.get(key), str)
    }
    if isinstance(server.get("base_url"), str):
        safe_base_url = _sanitize_management_base_url(server["base_url"])
        if safe_base_url:
            sanitized["base_url"] = safe_base_url
    sanitized["has_password"] = _has_nonempty_text(server.get("password"))
    if "import_job" in server:
        sanitized["import_job"] = sanitize_import_job(server.get("import_job"))
    return sanitized


def sanitize_ccload_servers(servers: list[dict]) -> list[dict]:
    return [sanitized for server in servers if (sanitized := sanitize_ccload_server(server)) is not None]


def sanitize_ccload_channel_catalogs(value: object) -> list[dict]:
    if not isinstance(value, list):
        return []
    sanitized: list[dict] = []
    for catalog in value:
        if not isinstance(catalog, dict):
            continue
        channel_id = catalog.get("id")
        plan_type = catalog.get("plan_type")
        models = catalog.get("models")
        models_loaded = catalog.get("models_loaded")
        if (
            not isinstance(channel_id, str)
            or not channel_id.strip()
            or len(channel_id.strip()) > 64
            or not isinstance(plan_type, str)
            or type(models_loaded) is not bool
            or not isinstance(models, list)
        ):
            continue
        public_models: list[str] = []
        malformed_model = False
        for model in models:
            normalized = parse_model_text(model)
            if not normalized:
                malformed_model = True
                break
            public_models.append(normalized)
        if malformed_model:
            continue
        sanitized.append({
            "id": channel_id.strip(),
            "plan_type": plan_type.strip(),
            "models": public_models,
            "models_loaded": models_loaded,
        })
    return sanitized


def start_limited_account_watcher(stop_event: Event) -> Thread:
    interval_seconds = config.refresh_account_interval_minute * 60
    begin_owner = getattr(account_service, "begin_watcher_refresh", None)
    owner = begin_owner() if callable(begin_owner) else None
    owner_kwargs = {"watcher_owner": owner} if owner is not None else {}

    def run_watcher_operation(function, *args, **kwargs):
        if stop_event.is_set():
            return None
        try:
            reservation = reserve_background_task()
        except BackgroundTaskQueueFullError:
            return None
        try:
            if stop_event.is_set():
                reservation.cancel()
                return None
            future = reservation.submit(function, *args, **kwargs)
        except Exception:
            return None
        while not future.done():
            if stop_event.wait(0.05):
                return None
        return future.result()

    def worker() -> None:
        while not stop_event.is_set():
            try:
                limited_tokens = account_service.list_limited_tokens()
                normal_tokens = account_service.list_normal_tokens()
                expiring_tokens = account_service.list_expiring_access_tokens()
                keepalive_tokens = account_service.list_refresh_token_keepalive_tokens()
                tokens = list(dict.fromkeys([*limited_tokens, *normal_tokens, *expiring_tokens]))
                expiring_token_set = set(expiring_tokens)
                keepalive_tokens = [token for token in keepalive_tokens if token not in expiring_token_set]
                if tokens:
                    print(
                        "[account-watcher] checking "
                        f"{len(limited_tokens)} limited accounts, "
                        f"{len(normal_tokens)} normal accounts, "
                        f"{len(expiring_tokens)} expiring access tokens"
                    )
                    run_watcher_operation(
                        account_service.refresh_accounts,
                        tokens,
                        **owner_kwargs,
                    )
                if stop_event.is_set():
                    return
                if keepalive_tokens:
                    print(f"[account-watcher] keepalive {len(keepalive_tokens)} refresh tokens")
                    result = run_watcher_operation(
                        account_service.keepalive_refresh_tokens,
                        keepalive_tokens,
                        **owner_kwargs,
                    )
                    if isinstance(result, dict) and result.get("errors"):
                        print(f"[account-watcher] keepalive errors: {len(result['errors'])}")
            except Exception as exc:
                print(f"[account-watcher] fail {exception_log_message(exc)}")
            stop_event.wait(interval_seconds)

    thread = Thread(target=worker, name="account-watcher", daemon=True)
    thread.start()
    return thread


def stop_limited_account_watcher(stop_event: Event, thread: Thread | None) -> None:
    """Invalidate watcher writes before stopping its polling thread."""
    invalidate = getattr(account_service, "invalidate_all_watcher_refreshes", None)
    if callable(invalidate):
        invalidate()
    stop_event.set()
    if thread is not None:
        thread.join(timeout=1)


def _web_asset_candidates(requested_path: str) -> list[str]:
    clean_path = str(requested_path or "").strip("/")
    if not clean_path:
        return ["index.html"]
    return [clean_path, f"{clean_path}/index.html", f"{clean_path}.html"]


def open_web_asset(requested_path: str) -> OpenedFile | None:
    """Open one web asset while keeping the authorized root and file handle fixed."""
    try:
        root = authorized_root(WEB_DIST_DIR)
    except (OSError, ValueError):
        return None
    for relative in _web_asset_candidates(requested_path):
        try:
            path = resolve_under_root(root, relative)
            return open_checked_file(path, root, root)
        except (OSError, ValueError):
            continue
    return None
