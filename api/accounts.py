from __future__ import annotations

import asyncio
import io
import json
import re
import threading
import uuid
import zipfile
from datetime import datetime
from functools import partial
from typing import Any, Literal

import anyio
from fastapi import APIRouter, Header, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field, StrictBool, StrictInt

from services.auth_service import auth_service

from api.support import (
    require_admin_async,
    sanitize_ccload_server,
    sanitize_ccload_channel_catalogs,
    sanitize_ccload_servers,
    sanitize_cpa_pool,
    sanitize_cpa_pools,
    sanitize_import_job,
    sanitize_sub2api_server,
    sanitize_sub2api_servers,
)
from services.account_service import account_service
from services.ccload_service import (
    CCLOAD_MAX_CHANNELS,
    ccload_config,
    ccload_import_service,
    list_remote_channel_models as ccload_list_remote_channel_models,
    list_remote_channels as ccload_list_remote_channels,
)
from services.cpa_service import CPA_MAX_REMOTE_FILES, cpa_config, cpa_import_service, list_remote_files
from services.oauth_login_service import OAuthLoginError, oauth_login_service
from services.protocol.error_response import (
    ImportJobActiveError,
    PUBLIC_SERVER_ERROR_MESSAGE,
    PublicSafeValueError,
    exception_log_message,
    public_exception_message,
)
from services.sub2api_service import (
    SUB2API_MAX_REMOTE_ITEMS,
    list_remote_accounts as sub2api_list_remote_accounts,
    list_remote_groups as sub2api_list_remote_groups,
    sub2api_config,
    sub2api_import_service,
)
from services.task_executor import reserve_background_task
from utils.helper import anonymize_token


_MANAGEMENT_THREAD_CAPACITY = 4
_MANAGEMENT_THREAD_STATE = threading.local()
_MANAGEMENT_TASKS: set[asyncio.Task] = set()


def _management_thread_limiter() -> anyio.CapacityLimiter:
    limiter = getattr(_MANAGEMENT_THREAD_STATE, "limiter", None)
    if limiter is None:
        limiter = anyio.CapacityLimiter(_MANAGEMENT_THREAD_CAPACITY)
        _MANAGEMENT_THREAD_STATE.limiter = limiter
    return limiter


async def run_management_io(func, *args, **kwargs):
    return await anyio.to_thread.run_sync(
        partial(func, *args, **kwargs),
        limiter=_management_thread_limiter(),
    )


def _schedule_management_task(task_factory) -> None:
    reservation = reserve_background_task()

    async def _run() -> None:
        await task_factory()

    try:
        task = asyncio.create_task(_run())
        _MANAGEMENT_TASKS.add(task)
        def _finish(done_task: asyncio.Task) -> None:
            # A task can be cancelled before its coroutine gets a first
            # scheduling turn, so _run()'s body is not guaranteed to execute.
            # Release from the task lifecycle itself; Reservation.release is
            # idempotent for the normal completion path as well.
            reservation.release()
            _MANAGEMENT_TASKS.discard(done_task)

        task.add_done_callback(_finish)
    except Exception:
        reservation.release()
        raise


async def wait_for_management_tasks() -> None:
    """Wait for already accepted management writes before application shutdown."""
    while _MANAGEMENT_TASKS:
        await asyncio.gather(*tuple(_MANAGEMENT_TASKS), return_exceptions=True)



class UserKeyCreateRequest(BaseModel):
    name: str = ""


class UserKeyUpdateRequest(BaseModel):
    name: str | None = None
    enabled: StrictBool | None = None
    key: str | None = None


class AccountCreateRequest(BaseModel):
    tokens: list[str] = Field(default_factory=list)
    accounts: list[dict[str, Any]] = Field(default_factory=list)


class AccountDeleteRequest(BaseModel):
    tokens: list[str] = Field(default_factory=list)


class AccountRefreshRequest(BaseModel):
    access_tokens: list[str] = Field(default_factory=list)


class AccountExportRequest(BaseModel):
    access_tokens: list[str] = Field(default_factory=list)
    format: Literal["json", "zip"] = "json"


class AccountUpdateRequest(BaseModel):
    access_token: str = ""
    type: str | None = None
    status: str | None = None
    quota: StrictInt | None = None
    proxy: str | None = None


class CPAPoolCreateRequest(BaseModel):
    name: str = ""
    base_url: str = ""
    secret_key: str = ""


class CPAPoolUpdateRequest(BaseModel):
    name: str | None = None
    base_url: str | None = None
    secret_key: str | None = None


class CPAImportRequest(BaseModel):
    names: list[str] = Field(default_factory=list, max_length=CPA_MAX_REMOTE_FILES)


class Sub2APIServerCreateRequest(BaseModel):
    name: str = ""
    base_url: str = ""
    email: str = ""
    password: str = ""
    api_key: str = ""
    group_id: str = ""


class Sub2APIServerUpdateRequest(BaseModel):
    name: str | None = None
    base_url: str | None = None
    email: str | None = None
    password: str | None = None
    api_key: str | None = None
    group_id: str | None = None


class Sub2APIImportRequest(BaseModel):
    account_ids: list[str] = Field(default_factory=list, max_length=SUB2API_MAX_REMOTE_ITEMS)


class CCLoadServerCreateRequest(BaseModel):
    name: str = ""
    base_url: str = ""
    password: str = ""


class CCLoadServerUpdateRequest(BaseModel):
    name: str | None = None
    base_url: str | None = None
    password: str | None = None


class CCLoadImportRequest(BaseModel):
    channel_ids: list[str] = Field(default_factory=list, max_length=CCLOAD_MAX_CHANNELS)


class CCLoadChannelModelsRequest(BaseModel):
    channel_ids: list[str] = Field(default_factory=list, max_length=50)


class OAuthLoginStartRequest(BaseModel):
    """起始 OAuth 桥。email_hint 可选，仅用于让 OpenAI 登录页预填邮箱。"""
    email_hint: str = ""


class OAuthLoginFinishRequest(BaseModel):
    """提交 callback。callback 既可以是完整 URL 也可以只填 code。"""
    session_id: str = ""
    callback: str = ""


def _account_payload_token(item: dict[str, Any]) -> str:
    value = item.get("access_token")
    if not isinstance(value, str):
        value = item.get("accessToken")
    return value.strip() if isinstance(value, str) else ""


def _unique_tokens(tokens: list[str]) -> list[str]:
    return list(dict.fromkeys(str(token or "").strip() for token in tokens if str(token or "").strip()))


_PUBLIC_ACCOUNT_TEXT_FIELDS = (
    "access_token",
    "proxy",
    "type",
    "source_type",
    "status",
    "email",
    "user_id",
    "limits_progress",
    "default_model_slug",
    "restore_at",
    "last_used_at",
)

_PUBLIC_LIMITS_PROGRESS_MAX = 100

_PUBLIC_ACCOUNT_INT_FIELDS = (
    "quota",
    "success",
    "fail",
    "image_inflight",
)


def _public_account(item: object) -> dict[str, Any] | None:
    if not isinstance(item, dict):
        return None
    projected: dict[str, Any] = {}
    for key in _PUBLIC_ACCOUNT_TEXT_FIELDS:
        value = item.get(key)
        if isinstance(value, str):
            projected[key] = value.strip()
    for key in _PUBLIC_ACCOUNT_INT_FIELDS:
        value = item.get(key)
        if type(value) is int and value >= 0:
            projected[key] = value
    if "limits_progress" in item:
        projected["limits_progress"] = item["limits_progress"] if isinstance(item["limits_progress"], list) else []
    limits_progress = projected.get("limits_progress")
    if isinstance(limits_progress, list):
        safe_limits_progress = []
        for entry in limits_progress[:_PUBLIC_LIMITS_PROGRESS_MAX]:
            if not isinstance(entry, dict):
                continue
            safe_entry: dict[str, Any] = {}
            feature_name = entry.get("feature_name")
            if isinstance(feature_name, str) and feature_name.strip() and len(feature_name.strip()) <= 256:
                safe_entry["feature_name"] = feature_name.strip()
            remaining = entry.get("remaining")
            if type(remaining) is int and remaining >= 0:
                safe_entry["remaining"] = remaining
            reset_after = entry.get("reset_after")
            if isinstance(reset_after, str) and len(reset_after.strip()) <= 256:
                safe_entry["reset_after"] = reset_after.strip()
            if safe_entry:
                safe_limits_progress.append(safe_entry)
        projected["limits_progress"] = safe_limits_progress
    return projected


def _public_accounts(items: object) -> list[dict[str, Any]]:
    if not isinstance(items, list):
        return []
    return [projected for item in items if (projected := _public_account(item)) is not None]


def _public_progress_results(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    projected: list[dict[str, str]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        safe_entry: dict[str, str] = {}
        token = _public_progress_token(entry.get("token"))
        if token is not None:
            safe_entry["token"] = token
        status = entry.get("status")
        if isinstance(status, str):
            safe_entry["status"] = _public_progress_status(status)
        if isinstance(entry.get("error"), str):
            safe_entry["error"] = _public_progress_error(entry.get("error"))
        if safe_entry:
            projected.append(safe_entry)
    return projected


_PUBLIC_ACCOUNT_RESULT_INT_FIELDS = (
    "added",
    "skipped",
    "refreshed",
    "relogined",
    "removed",
    "total",
    "processed",
    "completed",
    "failed",
    "total_quota",
)
_PUBLIC_ACCOUNT_ERROR_MESSAGE = "account operation failed"
_PUBLIC_ACCOUNT_RESULT_ERROR_CODES = frozenset({"relogin_failed"})
_PUBLIC_PROGRESS_STATUSES = frozenset({"成功", "失败", "跳过", "异常", "禁用", "限流", "正常", "处理中"})
_PUBLIC_REFRESH_STATUS_COUNT_KEYS = frozenset({"正常", "限流", "异常", "禁用"})


def _public_progress_status(value: str) -> str:
    return value if value in _PUBLIC_PROGRESS_STATUSES else "失败"


def _public_progress_error(value: object) -> str:
    if isinstance(value, str) and value in _PUBLIC_ACCOUNT_RESULT_ERROR_CODES:
        return value
    return _PUBLIC_ACCOUNT_ERROR_MESSAGE


def _public_progress_token(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > 256:
        return None
    if re.fullmatch(r"token:[0-9a-f]{10}", value):
        return value
    return anonymize_token(value)


def _public_account_errors(value: object) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    projected: list[dict[str, str]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        safe_entry: dict[str, str] = {}
        token = _public_progress_token(entry.get("token"))
        if token is not None:
            safe_entry["token"] = token
        if isinstance(entry.get("error"), str):
            safe_entry["error"] = _PUBLIC_ACCOUNT_ERROR_MESSAGE
        if safe_entry:
            if "error" not in safe_entry:
                safe_entry["error"] = _PUBLIC_ACCOUNT_ERROR_MESSAGE
            projected.append(safe_entry)
    return projected


def _public_status_counts(value: object) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    projected: dict[str, int] = {}
    for key, count in value.items():
        if (
            isinstance(key, str)
            and key in _PUBLIC_REFRESH_STATUS_COUNT_KEYS
            and key
            and len(key) <= 64
            and type(count) is int
            and 0 <= count <= 1_000_000_000
        ):
            projected[key] = count
    return projected


def _public_account_result(result: object) -> dict[str, Any]:
    if not isinstance(result, dict):
        return {}
    allowed_fields = {
        "item",
        "items",
        "added",
        "skipped",
        "refreshed",
        "relogined",
        "removed",
        "errors",
        "total",
        "processed",
        "done",
        "error",
        "status_counts",
        "total_quota",
        "results",
        "result",
    }
    projected: dict[str, Any] = {}
    for key in allowed_fields:
        if key not in result:
            continue
        if key in _PUBLIC_ACCOUNT_RESULT_INT_FIELDS:
            value = result[key]
            if type(value) is int and 0 <= value <= 1_000_000_000_000_000:
                projected[key] = value
        elif key == "done":
            if type(result[key]) is bool:
                projected[key] = result[key]
        elif key == "errors":
            projected[key] = _public_account_errors(result[key])
        elif key == "status_counts":
            projected[key] = _public_status_counts(result[key])
        elif key == "error":
            if isinstance(result[key], str):
                projected[key] = _PUBLIC_ACCOUNT_ERROR_MESSAGE
        elif key == "result":
            if isinstance(result[key], dict):
                projected[key] = _public_account_result(result[key])
        else:
            projected[key] = result[key]
    if "item" in projected:
        projected["item"] = _public_account(projected["item"])
    if "items" in projected:
        projected["items"] = _public_accounts(projected["items"])
    if "results" in projected:
        projected["results"] = _public_progress_results(projected["results"])
    return projected


def _download_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def _safe_export_name(value: str, fallback: str) -> str:
    clean = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip()).strip("-._")
    return (clean or fallback)[:80]


def _account_zip_bytes(items: list[dict[str, str]]) -> bytes:
    buf = io.BytesIO()
    used_names: set[str] = set()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as archive:
        for index, item in enumerate(items, start=1):
            raw_name = item.get("email") or item.get("account_id") or f"account-{index:03d}"
            base_name = _safe_export_name(raw_name, f"account-{index:03d}")
            name = base_name
            suffix = 2
            while name in used_names:
                name = f"{base_name}-{suffix}"
                suffix += 1
            used_names.add(name)
            archive.writestr(
                f"{name}.json",
                json.dumps(item, ensure_ascii=False, indent=2) + "\n",
            )
    return buf.getvalue()


def _build_account_export_file(
    access_tokens: list[str],
    export_format: Literal["json", "zip"],
) -> tuple[bytes, str, str] | None:
    items = account_service.build_export_items(access_tokens)
    if not items:
        return None
    timestamp = _download_timestamp()
    if export_format == "zip":
        return (
            _account_zip_bytes(items),
            "application/zip",
            f"codex-accounts-{timestamp}.zip",
        )
    payload: dict[str, str] | list[dict[str, str]] = items[0] if len(items) == 1 else items
    return (
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        "application/json",
        f"codex-accounts-{timestamp}.json",
    )


def _create_accounts_and_refresh(
    account_payloads: list[dict[str, Any]],
    tokens: list[str],
) -> tuple[dict[str, Any], dict[str, Any]]:
    if account_payloads:
        payload_token_set = set(_unique_tokens([_account_payload_token(item) for item in account_payloads]))
        extra_tokens = [token for token in tokens if token not in payload_token_set]
        combined_payloads = [dict(item) for item in account_payloads]
        combined_payloads.extend(
            {"access_token": token, "source_type": "web"}
            for token in extra_tokens
        )
        result = account_service.add_account_items(combined_payloads)
    else:
        result = account_service.add_accounts(tokens)
    return result, account_service.refresh_accounts(tokens)


def create_router() -> APIRouter:
    router = APIRouter()

    @router.get("/api/auth/users")
    async def list_user_keys(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"items": await run_management_io(auth_service.list_keys, role="user")}

    @router.post("/api/auth/users")
    async def create_user_key(body: UserKeyCreateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            item, raw_key = await run_management_io(
                auth_service.create_key,
                role="user",
                name=body.name,
            )
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "user key request failed")},
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "user key request failed"}) from exc
        return {
            "item": item,
            "key": raw_key,
            "items": await run_management_io(auth_service.list_keys, role="user"),
        }

    @router.post("/api/auth/users/{key_id}")
    async def update_user_key(
            key_id: str,
            body: UserKeyUpdateRequest,
            authorization: str | None = Header(default=None),
    ):
        await require_admin_async(authorization)
        updates = {
            key: value
            for key, value in {
                "name": body.name,
                "enabled": body.enabled,
                "key": body.key,
            }.items()
            if value is not None
        }
        if not updates:
            raise HTTPException(status_code=400, detail={"error": "还没有检测到改动，请修改后再保存"})
        try:
            item = await run_management_io(auth_service.update_key, key_id, updates, role="user")
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "user key request failed")},
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "user key request failed"}) from exc
        if item is None:
            raise HTTPException(status_code=404, detail={"error": "这条用户密钥不存在，可能已经被删除"})
        return {
            "item": item,
            "items": await run_management_io(auth_service.list_keys, role="user"),
        }

    @router.delete("/api/auth/users/{key_id}")
    async def delete_user_key(key_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        if not await run_management_io(auth_service.delete_key, key_id, role="user"):
            raise HTTPException(status_code=404, detail={"error": "这条用户密钥不存在，可能已经被删除"})
        return {"items": await run_management_io(auth_service.list_keys, role="user")}

    @router.get("/api/accounts")
    async def get_accounts(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"items": _public_accounts(await run_management_io(account_service.list_accounts))}

    @router.post("/api/accounts")
    async def create_accounts(body: AccountCreateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        account_payloads = [item for item in body.accounts if isinstance(item, dict)]
        payload_tokens = [_account_payload_token(item) for item in account_payloads]
        tokens = _unique_tokens([*body.tokens, *payload_tokens])
        if not tokens:
            raise HTTPException(status_code=400, detail={"error": "tokens is required"})
        result, refresh_result = await run_management_io(
            _create_accounts_and_refresh,
            account_payloads,
            tokens,
        )
        return _public_account_result({
            **result,
            "refreshed": refresh_result.get("refreshed", 0),
            "errors": refresh_result.get("errors", []),
            "items": refresh_result.get("items", result.get("items", [])),
        })

    @router.delete("/api/accounts")
    async def delete_accounts(body: AccountDeleteRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        tokens = [str(token or "").strip() for token in body.tokens if str(token or "").strip()]
        if not tokens:
            raise HTTPException(status_code=400, detail={"error": "tokens is required"})
        return _public_account_result(await run_management_io(account_service.delete_accounts, tokens))

    @router.post("/api/accounts/refresh")
    async def refresh_accounts(body: AccountRefreshRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        access_tokens = [str(token or "").strip() for token in body.access_tokens if str(token or "").strip()]
        if not access_tokens:
            access_tokens = await run_management_io(account_service.list_tokens)
        if not access_tokens:
            raise HTTPException(status_code=400, detail={"error": "access_tokens is required"})

        progress_id = str(uuid.uuid4())

        async def _do_refresh():
            try:
                await run_management_io(account_service.refresh_accounts, access_tokens, progress_id, False)
            except asyncio.CancelledError:
                try:
                    await run_management_io(
                        account_service.finish_refresh_progress,
                        progress_id,
                        error="刷新任务已取消",
                    )
                except Exception:
                    pass
                raise
            except Exception as e:
                await run_management_io(
                    account_service.finish_refresh_progress,
                    progress_id,
                    error=exception_log_message(e),
                )

        _schedule_management_task(_do_refresh)

        return {"progress_id": progress_id}

    @router.get("/api/accounts/refresh/progress/{progress_id}")
    async def get_refresh_progress(progress_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        progress = await run_management_io(account_service.get_refresh_progress, progress_id)
        if progress is None:
            raise HTTPException(status_code=404, detail={"error": "progress not found"})
        return _public_account_result(progress)

    @router.post("/api/accounts/re-login")
    async def re_login_accounts(body: AccountRefreshRequest, authorization: str | None = Header(default=None)):
        """对选中账号执行密码重新登录流程（密码登录→验证码登录→刷新token）。"""
        await require_admin_async(authorization)
        access_tokens = [str(token or "").strip() for token in body.access_tokens if str(token or "").strip()]
        if not access_tokens:
            raise HTTPException(status_code=400, detail={"error": "access_tokens is required"})

        progress_id = str(uuid.uuid4())

        async def _do_relogin():
            try:
                await run_management_io(account_service.re_login_accounts, access_tokens, progress_id)
            except asyncio.CancelledError:
                try:
                    await run_management_io(
                        account_service.finish_relogin_progress,
                        progress_id,
                        error="重新登录任务已取消",
                    )
                except Exception:
                    pass
                raise
            except Exception as e:
                await run_management_io(
                    account_service.finish_relogin_progress,
                    progress_id,
                    error=exception_log_message(e),
                )

        _schedule_management_task(_do_relogin)

        return {"progress_id": progress_id}

    @router.get("/api/accounts/re-login/progress/{progress_id}")
    async def get_relogin_progress(progress_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        progress = await run_management_io(account_service.get_relogin_progress, progress_id)
        if progress is None:
            raise HTTPException(status_code=404, detail={"error": "progress not found"})
        return _public_account_result(progress)

    @router.post("/api/accounts/export")
    async def export_accounts(body: AccountExportRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        access_tokens = _unique_tokens(body.access_tokens)
        export_file = await run_management_io(
            _build_account_export_file,
            access_tokens,
            body.format,
        )
        if export_file is None:
            raise HTTPException(
                status_code=400,
                detail={"error": "没有可导出的完整账号，需要同时有 access_token、refresh_token 和 id_token"},
            )
        content, media_type, filename = export_file
        return Response(
            content,
            media_type=media_type,
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )

    @router.post("/api/accounts/update")
    async def update_account(body: AccountUpdateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        access_token = str(body.access_token or "").strip()
        if not access_token:
            raise HTTPException(status_code=400, detail={"error": "access_token is required"})
        updates = {key: value for key, value in {"type": body.type, "status": body.status, "quota": body.quota, "proxy": body.proxy}.items() if value is not None}
        if not updates:
            raise HTTPException(status_code=400, detail={"error": "还没有检测到改动，请修改后再保存"})
        account = await run_management_io(account_service.update_account, access_token, updates)
        if account is None:
            raise HTTPException(status_code=404, detail={"error": "account not found"})
        return _public_account_result({
            "item": account,
            "items": await run_management_io(account_service.list_accounts),
        })

    @router.post("/api/accounts/oauth/start")
    async def start_oauth_login(
            body: OAuthLoginStartRequest,
            authorization: str | None = Header(default=None),
    ):
        """登记一次 PKCE 会话，返回可让用户浏览器打开的 authorize URL。"""
        await require_admin_async(authorization)
        try:
            return await run_management_io(oauth_login_service.start, body.email_hint)
        except OAuthLoginError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "OAuth 操作失败，请稍后重试")},
            ) from exc

    @router.post("/api/accounts/oauth/finish")
    async def finish_oauth_login(
            body: OAuthLoginFinishRequest,
            authorization: str | None = Header(default=None),
    ):
        """收用户从浏览器抓回的 callback URL / code，换出 token 三件套并落盘。"""
        await require_admin_async(authorization)
        # callback 可能包含一次性 code/state，不把其内容写入日志。
        print(
            "[oauth-login] finish called: "
            f"session_id_present={bool((body.session_id or '').strip())}, "
            f"callback_present={bool((body.callback or '').strip())}",
            flush=True,
        )
        finish_claim_id = uuid.uuid4().hex
        try:
            tokens = await run_management_io(
                oauth_login_service.finish,
                body.session_id,
                body.callback,
                finish_claim_id,
            )
        except asyncio.CancelledError:
            try:
                oauth_login_service.abort_finish(body.session_id, body.callback, finish_claim_id)
            except Exception:
                pass
            raise
        except OAuthLoginError as exc:
            print(f"[oauth-login] finish rejected: {exception_log_message(exc)}", flush=True)
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "OAuth 操作失败，请稍后重试")},
            ) from exc

        payload = {
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "id_token": tokens["id_token"],
            "source_type": "codex",
            "login_source": "oauth_login",
        }
        try:
            add_result = await run_management_io(account_service.add_account_items, [payload])
        except asyncio.CancelledError:
            try:
                await run_management_io(
                    oauth_login_service.abort_finish,
                    body.session_id,
                    body.callback,
                    getattr(tokens, "claim_id", finish_claim_id),
                )
            except Exception:
                pass
            raise
        except Exception as exc:
            try:
                await run_management_io(
                    oauth_login_service.abort_finish,
                    body.session_id,
                    body.callback,
                    getattr(tokens, "claim_id", finish_claim_id),
                )
            except Exception:
                pass
            raise HTTPException(
                status_code=500,
                detail={"error": "OAuth 凭据保存失败，请稍后重试"},
            ) from exc
        try:
            await run_management_io(
                oauth_login_service.commit_finish,
                body.session_id,
                body.callback,
                getattr(tokens, "claim_id", finish_claim_id),
            )
        except asyncio.CancelledError:
            # add_account_items 已成功；取消只可能打断了等待 commit 的线程。
            # 直接做一次幂等补偿，避免已落盘凭据对应的 PKCE claim 卡到 TTL。
            try:
                oauth_login_service.commit_finish(
                    body.session_id,
                    body.callback,
                    getattr(tokens, "claim_id", finish_claim_id),
                )
            except Exception:
                # 后台 commit 可能已经抢先完成，原始取消仍需保持可见。
                pass
            raise
        refresh_result = await run_management_io(
            account_service.refresh_accounts, [tokens["access_token"]]
        )
        return _public_account_result({
            **add_result,
            "refreshed": refresh_result.get("refreshed", 0),
            "errors": refresh_result.get("errors", []),
            "items": refresh_result.get("items", add_result.get("items", [])),
        })

    @router.get("/api/cpa/pools")
    async def list_cpa_pools(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"pools": sanitize_cpa_pools(await run_management_io(cpa_config.list_pools))}

    @router.post("/api/cpa/pools")
    async def create_cpa_pool(body: CPAPoolCreateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        if not body.base_url.strip():
            raise HTTPException(status_code=400, detail={"error": "base_url is required"})
        if not body.secret_key.strip():
            raise HTTPException(status_code=400, detail={"error": "secret_key is required"})
        try:
            pool = await run_management_io(
                cpa_config.add_pool,
                name=body.name,
                base_url=body.base_url,
                secret_key=body.secret_key,
            )
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "CPA pool request failed")},
            ) from exc
        pools = await run_management_io(cpa_config.list_pools)
        return {"pool": sanitize_cpa_pool(pool), "pools": sanitize_cpa_pools(pools)}

    @router.post("/api/cpa/pools/{pool_id}")
    async def update_cpa_pool(pool_id: str, body: CPAPoolUpdateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            pool = await run_management_io(cpa_config.update_pool, pool_id, body.model_dump(exclude_none=True))
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "CPA pool request failed")},
            ) from exc
        if pool is None:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        pools = await run_management_io(cpa_config.list_pools)
        return {"pool": sanitize_cpa_pool(pool), "pools": sanitize_cpa_pools(pools)}

    @router.delete("/api/cpa/pools/{pool_id}")
    async def delete_cpa_pool(pool_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            deleted = await run_management_io(cpa_config.delete_pool, pool_id)
        except ImportJobActiveError as exc:
            raise HTTPException(status_code=409, detail={"error": public_exception_message(exc, "CPA pool is busy")}) from exc
        if not deleted:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        return {"pools": sanitize_cpa_pools(await run_management_io(cpa_config.list_pools))}

    @router.get("/api/cpa/pools/{pool_id}/files")
    async def cpa_pool_files(pool_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        pool = await run_management_io(cpa_config.get_pool, pool_id)
        if pool is None:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        try:
            files = await run_management_io(list_remote_files, pool)
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": PUBLIC_SERVER_ERROR_MESSAGE}) from exc
        return {"pool_id": pool_id, "files": files}

    @router.post("/api/cpa/pools/{pool_id}/import")
    async def cpa_pool_import(pool_id: str, body: CPAImportRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        pool = await run_management_io(cpa_config.get_pool, pool_id)
        if pool is None:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        try:
            job = await run_management_io(cpa_import_service.start_import, pool, body.names)
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "CPA import request failed")},
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "CPA import request failed"}) from exc
        return {"import_job": sanitize_import_job(job)}

    @router.get("/api/cpa/pools/{pool_id}/import")
    async def cpa_pool_import_progress(pool_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        pool = await run_management_io(cpa_config.get_pool, pool_id)
        if pool is None:
            raise HTTPException(status_code=404, detail={"error": "pool not found"})
        return {"import_job": sanitize_import_job(pool.get("import_job"))}

    @router.get("/api/sub2api/servers")
    async def list_sub2api_servers(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"servers": sanitize_sub2api_servers(await run_management_io(sub2api_config.list_servers))}

    @router.post("/api/sub2api/servers")
    async def create_sub2api_server(body: Sub2APIServerCreateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        if not body.base_url.strip():
            raise HTTPException(status_code=400, detail={"error": "base_url is required"})
        has_login = body.email.strip() and body.password.strip()
        has_api_key = bool(body.api_key.strip())
        if not has_login and not has_api_key:
            raise HTTPException(status_code=400, detail={"error": "email+password or api_key is required"})
        try:
            server = await run_management_io(
                sub2api_config.add_server,
                name=body.name,
                base_url=body.base_url,
                email=body.email,
                password=body.password,
                api_key=body.api_key,
                group_id=body.group_id,
            )
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "Sub2API server request failed")},
            ) from exc
        servers = await run_management_io(sub2api_config.list_servers)
        return {"server": sanitize_sub2api_server(server), "servers": sanitize_sub2api_servers(servers)}

    @router.post("/api/sub2api/servers/{server_id}")
    async def update_sub2api_server(server_id: str, body: Sub2APIServerUpdateRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            server = await run_management_io(
                sub2api_config.update_server,
                server_id,
                body.model_dump(exclude_none=True),
            )
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "Sub2API server request failed")},
            ) from exc
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        servers = await run_management_io(sub2api_config.list_servers)
        return {"server": sanitize_sub2api_server(server), "servers": sanitize_sub2api_servers(servers)}

    @router.delete("/api/sub2api/servers/{server_id}")
    async def delete_sub2api_server(server_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            deleted = await run_management_io(sub2api_config.delete_server, server_id)
        except ImportJobActiveError as exc:
            raise HTTPException(status_code=409, detail={"error": public_exception_message(exc, "Sub2API server is busy")}) from exc
        if not deleted:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        return {"servers": sanitize_sub2api_servers(await run_management_io(sub2api_config.list_servers))}

    @router.get("/api/sub2api/servers/{server_id}/groups")
    async def sub2api_server_groups(server_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        server = await run_management_io(sub2api_config.get_server, server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            groups = await run_management_io(sub2api_list_remote_groups, server)
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": PUBLIC_SERVER_ERROR_MESSAGE}) from exc
        return {"server_id": server_id, "groups": groups}

    @router.get("/api/sub2api/servers/{server_id}/accounts")
    async def sub2api_server_accounts(server_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        server = await run_management_io(sub2api_config.get_server, server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            accounts = await run_management_io(sub2api_list_remote_accounts, server)
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": PUBLIC_SERVER_ERROR_MESSAGE}) from exc
        return {"server_id": server_id, "accounts": accounts}

    @router.post("/api/sub2api/servers/{server_id}/import")
    async def sub2api_server_import(server_id: str, body: Sub2APIImportRequest, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        server = await run_management_io(sub2api_config.get_server, server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            job = await run_management_io(sub2api_import_service.start_import, server, body.account_ids)
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "Sub2API import request failed")},
            ) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail={"error": "Sub2API import request failed"}) from exc
        return {"import_job": sanitize_import_job(job)}

    @router.get("/api/sub2api/servers/{server_id}/import")
    async def sub2api_server_import_progress(server_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        server = await run_management_io(sub2api_config.get_server, server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        return {"import_job": sanitize_import_job(server.get("import_job"))}

    @router.get("/api/ccload/servers")
    async def list_ccload_servers(authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        return {"servers": sanitize_ccload_servers(await run_management_io(ccload_config.list_servers))}

    @router.post("/api/ccload/servers")
    async def create_ccload_server(
        body: CCLoadServerCreateRequest,
        authorization: str | None = Header(default=None),
    ):
        await require_admin_async(authorization)
        try:
            server = await run_management_io(
                ccload_config.add_server,
                name=body.name,
                base_url=body.base_url,
                password=body.password,
            )
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "ccLoad server request failed")},
            ) from exc
        return {
            "server": sanitize_ccload_server(server),
            "servers": sanitize_ccload_servers(await run_management_io(ccload_config.list_servers)),
        }

    @router.post("/api/ccload/servers/{server_id}")
    async def update_ccload_server(
        server_id: str,
        body: CCLoadServerUpdateRequest,
        authorization: str | None = Header(default=None),
    ):
        await require_admin_async(authorization)
        try:
            server = await run_management_io(
                ccload_config.update_server,
                server_id,
                body.model_dump(exclude_none=True),
            )
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "ccLoad server request failed")},
            ) from exc
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        return {
            "server": sanitize_ccload_server(server),
            "servers": sanitize_ccload_servers(await run_management_io(ccload_config.list_servers)),
        }

    @router.delete("/api/ccload/servers/{server_id}")
    async def delete_ccload_server(server_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        try:
            deleted = await run_management_io(ccload_config.delete_server, server_id)
        except ImportJobActiveError as exc:
            raise HTTPException(status_code=409, detail={"error": public_exception_message(exc, "ccLoad server is busy")}) from exc
        if not deleted:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        return {"servers": sanitize_ccload_servers(await run_management_io(ccload_config.list_servers))}

    @router.get("/api/ccload/servers/{server_id}/channels")
    async def ccload_server_channels(server_id: str, authorization: str | None = Header(default=None)):
        await require_admin_async(authorization)
        server = await run_management_io(ccload_config.get_server, server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            channels = await run_management_io(ccload_list_remote_channels, server)
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": PUBLIC_SERVER_ERROR_MESSAGE}) from exc
        return {"server_id": server_id, "channels": channels}

    @router.post("/api/ccload/servers/{server_id}/channel-models")
    async def ccload_server_channel_models(
        server_id: str,
        body: CCLoadChannelModelsRequest,
        authorization: str | None = Header(default=None),
    ):
        await require_admin_async(authorization)
        server = await run_management_io(ccload_config.get_server, server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            channels = await run_management_io(
                ccload_list_remote_channel_models,
                server,
                body.channel_ids,
            )
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "ccLoad channel model request failed")},
            ) from exc
        except Exception as exc:
            raise HTTPException(status_code=502, detail={"error": PUBLIC_SERVER_ERROR_MESSAGE}) from exc
        return {"server_id": server_id, "channels": sanitize_ccload_channel_catalogs(channels)}

    @router.post("/api/ccload/servers/{server_id}/import")
    async def ccload_server_import(
        server_id: str,
        body: CCLoadImportRequest,
        authorization: str | None = Header(default=None),
    ):
        await require_admin_async(authorization)
        server = await run_management_io(ccload_config.get_server, server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        try:
            job = await run_management_io(ccload_import_service.start_import, server, body.channel_ids)
        except PublicSafeValueError as exc:
            raise HTTPException(
                status_code=400,
                detail={"error": public_exception_message(exc, "ccLoad import request failed")},
            ) from exc
        return {"import_job": sanitize_import_job(job)}

    @router.get("/api/ccload/servers/{server_id}/import")
    async def ccload_server_import_progress(
        server_id: str,
        authorization: str | None = Header(default=None),
    ):
        await require_admin_async(authorization)
        server = await run_management_io(ccload_config.get_server, server_id)
        if server is None:
            raise HTTPException(status_code=404, detail={"error": "server not found"})
        return {"import_job": sanitize_import_job(server.get("import_job"))}

    return router
