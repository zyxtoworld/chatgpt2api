from __future__ import annotations

import copy
from dataclasses import dataclass
import errno
import hashlib
import json
import math
import os
import stat
import sys
from pathlib import Path
from threading import RLock
import time
from urllib.parse import urlsplit

from services.storage.base import (
    StorageBackend,
    StorageConflictError,
    StorageDataError,
    canonical_path_write_lock,
)
from services.image_rel_lock import image_rel_lock
from services.secure_file import (
    atomic_write_bytes,
    authorized_root,
    delete_checked_file,
    open_checked_file,
    read_checked_file_bytes,
    resolve_under_root,
)


_IMAGE_FILE_EXTENSIONS = frozenset({".png", ".jpg", ".jpeg", ".webp"})
from services.protocol.error_response import PublicSafeValueError
from services.url_utils import (
    normalize_public_http_url,
    redact_url_credentials as _redact_url_credentials,
)

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
_CONFIG_FILE_ENV = os.getenv("CHATGPT2API_CONFIG_FILE", "").strip()
CONFIG_FILE = Path(_CONFIG_FILE_ENV) if _CONFIG_FILE_ENV else BASE_DIR / "config.json"
VERSION_FILE = BASE_DIR / "VERSION"
BACKUP_STATE_FILE = DATA_DIR / "backup_state.json"


def _hot_overwrite_bind_mounted_file(
    path: Path,
    payload: bytes,
    *,
    expected_identity: tuple[int, int] | None = None,
) -> None:
    flags = os.O_RDWR
    no_follow = getattr(os, "O_NOFOLLOW", 0)
    if no_follow:
        flags |= no_follow
    binary = getattr(os, "O_BINARY", 0)
    if binary:
        flags |= binary
    fd = os.open(path, flags)
    try:
        file_stat = os.fstat(fd)
        if expected_identity is not None and (file_stat.st_dev, file_stat.st_ino) != expected_identity:
            raise StorageConflictError()
        if not stat.S_ISREG(file_stat.st_mode):
            raise OSError("config bind mount is not a regular file")
        if getattr(file_stat, "st_nlink", 1) != 1:
            raise OSError("config bind mount target has multiple links")

        os.lseek(fd, 0, os.SEEK_SET)
        original_payload = os.read(fd, file_stat.st_size)
        if len(original_payload) != file_stat.st_size:
            raise OSError("short config bind mount snapshot")

        def write_all(value: bytes | memoryview) -> None:
            view = memoryview(value)
            while view:
                written = os.write(fd, view)
                if not written:
                    raise OSError("short config bind mount write")
                view = view[written:]

        os.lseek(fd, 0, os.SEEK_SET)
        try:
            write_all(payload)
            os.ftruncate(fd, len(payload))
            os.fsync(fd)
        except Exception as exc:
            try:
                os.lseek(fd, 0, os.SEEK_SET)
                write_all(original_payload)
                os.ftruncate(fd, len(original_payload))
                os.fsync(fd)
            except Exception:
                raise OSError("config bind mount rollback failed") from exc
            raise
    finally:
        os.close(fd)


DEFAULT_BACKUP_INCLUDE = {
    "config": True,
    "cpa": True,
    "sub2api": True,
    "ccload": True,
    "logs": True,
    "image_tasks": True,
    "accounts_snapshot": True,
    "auth_keys_snapshot": True,
    "images": False,
}

DEFAULT_IMAGE_STORAGE = {
    "enabled": False,
    "mode": "local",
    "webdav_url": "",
    "webdav_username": "",
    "webdav_password": "",
    "webdav_root_path": "chatgpt2api/images",
    "public_base_url": "",
}

DEFAULT_CHAT_COMPLETION_CACHE = {
    "enabled": True,
    "ttl_seconds": 60,
    "max_entries": 256,
    "dedupe_inflight": True,
    "stream_cache": True,
    "normalize_messages": True,
    "drop_adjacent_duplicates": True,
    "drop_assistant_history": False,
}

DEFAULT_PROXY_RUNTIME_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/145.0.0.0 Safari/537.36"
)

DEFAULT_PROXY_RUNTIME = {
    "enabled": False,
    "egress_mode": "direct",
    "proxy_url": "",
    "resource_proxy_url": "",
    "skip_ssl_verify": False,
    "reset_session_status_codes": [403],
    "clearance": {
        "enabled": False,
        "mode": "none",
        "cf_cookies": "",
        "cf_clearance": "",
        "user_agent": DEFAULT_PROXY_RUNTIME_USER_AGENT,
        "browser": "chrome",
        "flaresolverr_url": "",
        "timeout_sec": 60,
        "refresh_interval": 3600,
        "warm_up_on_start": False,
    },
}

DEFAULT_THIRD_PARTY_APPS = {
    "infinite_canvas": {
        "enabled": False,
        "url": "https://canvas.best",
    },
}

_PUBLIC_CONFIG_RAW_KEYS = frozenset({"proxy", "base_url", "image_timeout_retry_secs"})

PUBLIC_SECRET_MASK = "********"
BACKUP_STATE_ERROR_FALLBACK = "备份执行失败，请稍后重试"
_BACKUP_STATE_FIXED_MESSAGES = {
    "backup_failed": BACKUP_STATE_ERROR_FALLBACK,
    "r2_config_incomplete": "R2 配置不完整",
    "backup_encrypt_unavailable": "当前环境缺少 openssl，无法执行加密备份",
    "backup_encrypt_failed": "加密备份失败：openssl 执行失败",
    "backup_decrypt_unavailable": "当前环境缺少 openssl，无法解密备份内容",
    "backup_decrypt_failed": "解密备份失败：openssl 执行失败",
    "backup_key_required": "备份对象 key 不能为空",
    "backup_download_passphrase_missing": "当前未配置加密口令，无法下载并解密已加密备份",
    "backup_detail_passphrase_missing": "当前未配置加密口令，无法查看已加密备份",
    "backup_busy": "当前已有备份任务正在执行",
    "backup_encrypt_passphrase_missing": "已启用备份加密，但未设置加密口令",
    "backup_archive_invalid": "解析备份压缩包失败，备份可能已损坏",
}
_BACKUP_STATE_STATUS_MESSAGES = {
    "r2_connection_failed": "连接 R2 失败：HTTP {}",
    "r2_upload_failed": "上传备份失败：HTTP {}",
    "r2_delete_failed": "删除备份失败：HTTP {}",
    "r2_read_failed": "读取备份失败：HTTP {}",
    "r2_list_failed": "获取备份列表失败：HTTP {}",
}
_BACKUP_STATE_STATUSES = frozenset({"idle", "running", "success", "error"})


def parse_public_url(value: object) -> str:
    """Return a safe absolute HTTP(S) base URL, or empty on invalid input."""
    if not isinstance(value, str):
        return ""
    text = value.strip().rstrip("/")
    if not text:
        return ""
    try:
        parsed = urlsplit(text)
        if parsed.query or parsed.fragment:
            return ""
    except (TypeError, ValueError):
        return ""
    normalized = normalize_public_http_url(text)
    return normalized.rstrip("/") if normalized else ""


def _restore_redacted_url(value: object, existing: object) -> object:
    candidate = str(value or "").strip()
    previous = str(existing or "").strip()
    redacted_previous = _redact_url_credentials(previous)
    public_previous = _public_config_url(previous)
    if previous and (
        (redacted_previous and candidate == redacted_previous)
        or (public_previous and candidate == public_previous)
    ):
        return existing
    return value


def _public_config_url(value: object) -> str:
    """Project a configured URL without userinfo, query credentials, or fragments."""
    if not isinstance(value, str):
        return ""
    try:
        parsed = urlsplit(value.strip())
        if not parsed.scheme or not parsed.netloc:
            return ""
        return _redact_url_credentials(parsed._replace(query="", fragment="").geturl())
    except (TypeError, ValueError):
        return ""


def _public_secret(value: object) -> str:
    return PUBLIC_SECRET_MASK if isinstance(value, str) and bool(value.strip()) else ""


def _text_or_default(value: object, default: str = "") -> str:
    return value.strip() if isinstance(value, str) else default


def _normalize_sensitive_words(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item.strip() for item in value if isinstance(item, str) and item.strip()]


_CONFIG_LOG_LEVELS = frozenset({"debug", "info", "warning", "error"})
_CONFIG_THINKING_EFFORTS = frozenset({"auto", "standard", "extended", "max"})


def _normalize_log_levels(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        level
        for item in value
        if isinstance(item, str) and (level := item.strip().lower()) in _CONFIG_LOG_LEVELS
    ]


def _normalize_thinking_effort(value: object) -> str:
    normalized = _text_or_default(value, "auto").lower()
    return normalized if normalized in _CONFIG_THINKING_EFFORTS else "auto"


def _normalize_config_update_values(data: dict[str, object]) -> dict[str, object]:
    normalized = dict(data)
    integer_fields = {
        "refresh_account_interval_minute": (5, 0),
        "image_retention_days": (30, 1),
        "image_poll_timeout_secs": (120, 1),
        "image_account_concurrency": (3, 1),
        "image_timeout_retry_secs": (30, 1),
    }
    for field, (default, minimum) in integer_fields.items():
        if field in normalized:
            normalized[field] = _normalize_positive_int(normalized[field], default, minimum)

    float_fields = {
        "image_poll_interval_secs": (10.0, 0.5),
        "image_poll_initial_wait_secs": (10.0, 0.0),
        "image_settle_secs": (2.0, 0.5),
    }
    for field, (default, minimum) in float_fields.items():
        if field in normalized:
            normalized[field] = _normalize_nonnegative_float(normalized[field], default, minimum)

    boolean_defaults = {
        "image_parallel_generation": True,
        "image_settle_enabled": True,
        "image_check_before_hit_enabled": True,
        "image_remove_conversation_after_result": False,
        "image_remove_conversation_always": False,
        "auto_remove_invalid_accounts": False,
        "auto_remove_rate_limited_accounts": False,
        "auto_relogin_after_refresh": False,
    }
    for field, default in boolean_defaults.items():
        if field in normalized:
            normalized[field] = _normalize_bool(normalized[field], default)
    return normalized


def _restore_masked_fields(
    value: object,
    existing: object,
    fields: set[str],
) -> dict[str, object]:
    incoming = dict(value) if isinstance(value, dict) else {}
    previous = existing if isinstance(existing, dict) else {}
    for field in fields:
        if str(incoming.get(field) or "").strip() == PUBLIC_SECRET_MASK and previous.get(field):
            incoming[field] = previous[field]
    return incoming


def _normalize_bool(value: object, default: bool = False) -> bool:
    if type(value) is bool:
        return value
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        return default
    if value is None:
        return default
    return default


def _normalize_positive_int(value: object, default: int, minimum: int = 0) -> int:
    if type(value) is int:
        normalized = value
    elif isinstance(value, str):
        text = value.strip()
        if not text or (text[0] in {"+", "-"} and not text[1:].isdigit()) or (
            text[0] not in {"+", "-"} and not text.isdigit()
        ):
            normalized = default
        else:
            try:
                normalized = int(text)
            except (OverflowError, ValueError):
                normalized = default
    else:
        normalized = default
    return max(minimum, normalized)


def _normalize_nonnegative_float(value: object, default: float, minimum: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, (int, float)):
        normalized = float(value)
    elif isinstance(value, str):
        try:
            normalized = float(value.strip())
        except (TypeError, ValueError):
            return default
    else:
        return default
    if not math.isfinite(normalized):
        return default
    return max(minimum, normalized)


def _normalize_backup_include(value: object) -> dict[str, bool]:
    source = value if isinstance(value, dict) else {}
    normalized = dict(DEFAULT_BACKUP_INCLUDE)
    for key in normalized:
        normalized[key] = _normalize_bool(source.get(key), normalized[key])
    return normalized


def _normalize_backup_settings(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    return {
        "enabled": _normalize_bool(source.get("enabled"), False),
        "provider": "cloudflare_r2",
        "account_id": _text_or_default(source.get("account_id")),
        "access_key_id": _text_or_default(source.get("access_key_id")),
        "secret_access_key": _text_or_default(source.get("secret_access_key")),
        "bucket": _text_or_default(source.get("bucket")),
        "prefix": _text_or_default(source.get("prefix"), "backups").strip("/") or "backups",
        "interval_minutes": _normalize_positive_int(source.get("interval_minutes"), 360, 1),
        "rotation_keep": _normalize_positive_int(source.get("rotation_keep"), 10, 0),
        "encrypt": _normalize_bool(source.get("encrypt"), False),
        "passphrase": _text_or_default(source.get("passphrase")),
        "include": _normalize_backup_include(source.get("include")),
    }


def _normalize_backup_state(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    raw_error = _text_or_default(source.get("last_error"))
    error_code = _text_or_default(source.get("last_error_code"))
    raw_status = source.get("last_error_status")
    status = raw_status if isinstance(raw_status, int) and not isinstance(raw_status, bool) and 400 <= raw_status <= 599 else None
    invalid_status = raw_status is not None and status is None
    last_error = None
    if invalid_status:
        last_error = BACKUP_STATE_ERROR_FALLBACK
    elif error_code in _BACKUP_STATE_STATUS_MESSAGES:
        last_error = (
            _BACKUP_STATE_STATUS_MESSAGES[error_code].format(status)
            if status is not None
            else BACKUP_STATE_ERROR_FALLBACK
        )
    elif error_code in _BACKUP_STATE_FIXED_MESSAGES:
        last_error = _BACKUP_STATE_FIXED_MESSAGES[error_code]
    elif raw_error or error_code or raw_status is not None or source.get("_last_error_public") is True:
        last_error = BACKUP_STATE_ERROR_FALLBACK
    started_at = _text_or_default(source.get("last_started_at"))
    finished_at = _text_or_default(source.get("last_finished_at"))
    raw_last_status = _text_or_default(source.get("last_status"), "idle")
    last_status = raw_last_status if raw_last_status in _BACKUP_STATE_STATUSES else "idle"
    object_key = _text_or_default(source.get("last_object_key"))
    pending_object_key = _text_or_default(source.get("pending_object_key"))
    normalized = {
        "last_started_at": started_at[:128] or None,
        "last_finished_at": finished_at[:128] or None,
        "last_status": last_status,
        "last_error": last_error,
        "last_object_key": object_key[:2048] or None,
        "pending_object_key": pending_object_key[:2048] or None,
    }
    target_fingerprint = source.get("pending_target_fingerprint")
    if isinstance(target_fingerprint, str) and len(target_fingerprint) == 64 and all(
        char in "0123456789abcdef" for char in target_fingerprint
    ):
        normalized["pending_target_fingerprint"] = target_fingerprint
    return normalized


def _normalize_image_storage_settings(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    mode = str(source.get("mode") or "local").strip().lower()
    if mode not in {"local", "webdav", "both"}:
        mode = "local"
    enabled = _normalize_bool(source.get("enabled"), False)
    if not enabled:
        mode = "local"
    root_path = _text_or_default(
        source.get("webdav_root_path"),
        str(DEFAULT_IMAGE_STORAGE["webdav_root_path"]),
    ).strip().strip("/")
    return {
        "enabled": enabled,
        "mode": mode,
        "webdav_url": _text_or_default(source.get("webdav_url")).rstrip("/"),
        "webdav_username": _text_or_default(source.get("webdav_username")),
        "webdav_password": _text_or_default(source.get("webdav_password")),
        "webdav_root_path": root_path or str(DEFAULT_IMAGE_STORAGE["webdav_root_path"]),
        "public_base_url": parse_public_url(_text_or_default(source.get("public_base_url"))),
    }


def _normalize_chat_completion_cache_settings(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    return {
        "enabled": _normalize_bool(source.get("enabled"), DEFAULT_CHAT_COMPLETION_CACHE["enabled"]),
        "ttl_seconds": _normalize_positive_int(
            source.get("ttl_seconds"),
            int(DEFAULT_CHAT_COMPLETION_CACHE["ttl_seconds"]),
            0,
        ),
        "max_entries": _normalize_positive_int(
            source.get("max_entries"),
            int(DEFAULT_CHAT_COMPLETION_CACHE["max_entries"]),
            1,
        ),
        "dedupe_inflight": _normalize_bool(
            source.get("dedupe_inflight"),
            bool(DEFAULT_CHAT_COMPLETION_CACHE["dedupe_inflight"]),
        ),
        "stream_cache": _normalize_bool(
            source.get("stream_cache"),
            bool(DEFAULT_CHAT_COMPLETION_CACHE["stream_cache"]),
        ),
        "normalize_messages": _normalize_bool(
            source.get("normalize_messages"),
            bool(DEFAULT_CHAT_COMPLETION_CACHE["normalize_messages"]),
        ),
        "drop_adjacent_duplicates": _normalize_bool(
            source.get("drop_adjacent_duplicates"),
            bool(DEFAULT_CHAT_COMPLETION_CACHE["drop_adjacent_duplicates"]),
        ),
        "drop_assistant_history": _normalize_bool(
            source.get("drop_assistant_history"),
            bool(DEFAULT_CHAT_COMPLETION_CACHE["drop_assistant_history"]),
        ),
    }


def _normalize_status_codes(value: object) -> list[int]:
    items = value if isinstance(value, list) else DEFAULT_PROXY_RUNTIME["reset_session_status_codes"]
    normalized: list[int] = []
    for item in items:
        if isinstance(item, bool):
            continue
        try:
            status = int(item)
        except (OverflowError, TypeError, ValueError):
            continue
        if 100 <= status <= 599 and status not in normalized:
            normalized.append(status)
    if not normalized:
        return list(DEFAULT_PROXY_RUNTIME["reset_session_status_codes"])
    return normalized


def _normalize_proxy_runtime_settings(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    default_clearance = DEFAULT_PROXY_RUNTIME["clearance"]
    clearance_source = source.get("clearance") if isinstance(source.get("clearance"), dict) else {}

    egress_mode = _text_or_default(
        source.get("egress_mode"),
        str(DEFAULT_PROXY_RUNTIME["egress_mode"]),
    ).lower()
    if egress_mode not in {"direct", "single_proxy"}:
        egress_mode = str(DEFAULT_PROXY_RUNTIME["egress_mode"])

    clearance_mode = _text_or_default(
        clearance_source.get("mode"),
        str(default_clearance["mode"]),
    ).lower()
    if clearance_mode not in {"none", "manual", "flaresolverr"}:
        clearance_mode = str(default_clearance["mode"])

    user_agent = _text_or_default(
        clearance_source.get("user_agent"),
        str(default_clearance["user_agent"]),
    )
    browser = _text_or_default(clearance_source.get("browser"), str(default_clearance["browser"]))

    existing_clearance_cookies = _text_or_default(source.get("_existing_cf_cookies"))
    existing_cf_clearance = _text_or_default(source.get("_existing_cf_clearance"))
    cf_cookies = _text_or_default(clearance_source.get("cf_cookies"))
    cf_clearance = _text_or_default(clearance_source.get("cf_clearance"))
    if not cf_cookies and _normalize_bool(clearance_source.get("has_cf_cookies"), False):
        cf_cookies = existing_clearance_cookies
    if not cf_clearance and _normalize_bool(clearance_source.get("has_cf_clearance"), False):
        cf_clearance = existing_cf_clearance

    return {
        "enabled": _normalize_bool(source.get("enabled"), bool(DEFAULT_PROXY_RUNTIME["enabled"])),
        "egress_mode": egress_mode,
        "proxy_url": _text_or_default(source.get("proxy_url")),
        "resource_proxy_url": _text_or_default(source.get("resource_proxy_url")),
        "skip_ssl_verify": _normalize_bool(
            source.get("skip_ssl_verify"),
            bool(DEFAULT_PROXY_RUNTIME["skip_ssl_verify"]),
        ),
        "reset_session_status_codes": _normalize_status_codes(source.get("reset_session_status_codes")),
        "clearance": {
            "enabled": _normalize_bool(clearance_source.get("enabled"), bool(default_clearance["enabled"])),
            "mode": clearance_mode,
            "cf_cookies": cf_cookies,
            "cf_clearance": cf_clearance,
            "user_agent": user_agent or str(default_clearance["user_agent"]),
            "browser": browser or str(default_clearance["browser"]),
            "flaresolverr_url": _text_or_default(clearance_source.get("flaresolverr_url")),
            "timeout_sec": _normalize_positive_int(
                clearance_source.get("timeout_sec"),
                int(default_clearance["timeout_sec"]),
                1,
            ),
            "refresh_interval": _normalize_positive_int(
                clearance_source.get("refresh_interval"),
                int(default_clearance["refresh_interval"]),
                60,
            ),
            "warm_up_on_start": _normalize_bool(
                clearance_source.get("warm_up_on_start"),
                bool(default_clearance["warm_up_on_start"]),
            ),
        },
    }


def _normalize_third_party_apps_settings(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    canvas_source = source.get("infinite_canvas") if isinstance(source.get("infinite_canvas"), dict) else {}
    raw_url = canvas_source.get("url")
    if raw_url is not None and not isinstance(raw_url, str):
        raw_url = ""
    url = parse_public_url(
        raw_url if raw_url is not None else DEFAULT_THIRD_PARTY_APPS["infinite_canvas"]["url"]
    )
    enabled = _normalize_bool(canvas_source.get("enabled"), False) and bool(url)
    return {
        "infinite_canvas": {
            "enabled": enabled,
            "url": url,
        },
    }


def _normalize_ai_review_settings(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    return {
        "enabled": _normalize_bool(source.get("enabled"), False),
        "base_url": _text_or_default(source.get("base_url")),
        "api_key": _text_or_default(source.get("api_key")),
        "model": _text_or_default(source.get("model")),
        "prompt": _text_or_default(source.get("prompt")),
    }


def _validate_image_storage_settings(settings: dict[str, object]) -> None:
    if not _normalize_bool(settings.get("enabled"), False):
        return
    if not str(settings.get("webdav_url") or "").strip():
        raise PublicSafeValueError("启用 WebDAV 图片存储后必须填写 WebDAV URL")
    if not str(settings.get("webdav_password") or "").strip():
        raise PublicSafeValueError("启用 WebDAV 图片存储后必须填写 WebDAV 密码")


@dataclass(frozen=True)
class LoadedSettings:
    auth_key: str
    refresh_account_interval_minute: int


def _normalize_auth_key(value: object) -> str:
    return str(value or "").strip()


def _is_invalid_auth_key(value: object) -> bool:
    return _normalize_auth_key(value) == ""


def _read_json_object(
    path: Path,
    *,
    name: str,
    fail_closed: bool = False,
) -> dict[str, object]:
    if not path.exists():
        return {}
    if path.is_dir():
        if fail_closed:
            raise StorageDataError()
        print(
            f"Warning: {name} at '{path}' is a directory, ignoring it and falling back to other configuration sources.",
            file=sys.stderr,
        )
        return {}
    try:
        data = json.loads(read_checked_file_bytes(path, path.parent).decode("utf-8"))
    except Exception as exc:
        if fail_closed:
            raise StorageDataError() from exc
        return {}
    if isinstance(data, dict):
        return data
    if fail_closed:
        raise StorageDataError()
    return {}


def _read_config_snapshot(path: Path) -> tuple[dict[str, object], str | None, tuple[int, int] | None]:
    """Read config bytes once and derive both data and revision from that snapshot."""
    try:
        opened = open_checked_file(path, path.parent, path.parent)
    except FileNotFoundError:
        return {}, None, None
    except OSError as exc:
        raise StorageDataError() from exc
    try:
        payload = opened.file.read()
        identity = (opened.stat_result.st_dev, opened.stat_result.st_ino)
    finally:
        opened.file.close()
    try:
        data = json.loads(payload.decode("utf-8"))
    except Exception as exc:
        raise StorageDataError() from exc
    if not isinstance(data, dict):
        raise StorageDataError()
    return data, hashlib.sha256(payload).hexdigest(), identity


def _load_settings() -> LoadedSettings:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_config = _read_json_object(CONFIG_FILE, name="config.json")
    auth_key = _normalize_auth_key(os.getenv("CHATGPT2API_AUTH_KEY") or raw_config.get("auth-key"))
    if _is_invalid_auth_key(auth_key):
        raise ValueError(
            "❌ auth-key 未设置！\n"
            "请在环境变量 CHATGPT2API_AUTH_KEY 中设置，或者在 config.json 中填写 auth-key。"
        )

    refresh_interval = _normalize_positive_int(
        raw_config.get("refresh_account_interval_minute", 5),
        5,
        0,
    )

    return LoadedSettings(
        auth_key=auth_key,
        refresh_account_interval_minute=refresh_interval,
    )


class ConfigStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = RLock()
        self._path_lock = canonical_path_write_lock(path)
        self._snapshot_revision: str | None = None
        self._snapshot_identity: tuple[int, int] | None = None
        self._snapshot_parent_identity: tuple[int, int] | None = None
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.data = self._load()
        self._storage_backend: StorageBackend | None = None
        if _is_invalid_auth_key(self.auth_key):
            raise ValueError(
                "❌ auth-key 未设置！\n"
                "请按以下任意一种方式解决：\n"
                "1. 在 Render 的 Environment 变量中添加：\n"
                "   CHATGPT2API_AUTH_KEY = your_real_auth_key\n"
                "2. 或者在 config.json 中填写：\n"
                '   "auth-key": "your_real_auth_key"'
            )

    def _load(self) -> dict[str, object]:
        data, self._snapshot_revision, self._snapshot_identity = _read_config_snapshot(self.path)
        try:
            parent_stat = self.path.parent.stat()
        except OSError as exc:
            raise StorageDataError() from exc
        self._snapshot_parent_identity = (parent_stat.st_dev, parent_stat.st_ino)
        return data

    def _save(self, data: dict[str, object] | None = None) -> None:
        with self._path_lock:
            self._save_locked(data)

    def _save_locked(self, data: dict[str, object] | None = None) -> None:
        _, current_revision, current_identity = _read_config_snapshot(self.path)
        if current_revision != self._snapshot_revision:
            raise StorageConflictError()
        if current_identity != self._snapshot_identity:
            raise StorageConflictError()
        try:
            parent_stat = self.path.parent.stat()
        except OSError as exc:
            raise StorageDataError() from exc
        current_parent_identity = (parent_stat.st_dev, parent_stat.st_ino)
        if current_parent_identity != self._snapshot_parent_identity:
            raise StorageConflictError()
        if current_revision is None:
            if self.path.exists():
                raise StorageConflictError()
            original_stat = None
        else:
            try:
                original_stat = self.path.stat()
            except OSError as exc:
                raise StorageDataError() from exc
            if (original_stat.st_dev, original_stat.st_ino) != current_identity:
                raise StorageConflictError()
        target_mode = stat.S_IMODE(original_stat.st_mode) if original_stat is not None else stat.S_IRUSR | stat.S_IWUSR
        target_owner = None
        if original_stat is not None and os.name != "nt":
            effective_uid = getattr(os, "geteuid", lambda: None)()
            effective_gid = getattr(os, "getegid", lambda: None)()
            if (effective_uid, effective_gid) != (original_stat.st_uid, original_stat.st_gid):
                target_owner = (original_stat.st_uid, original_stat.st_gid)
        payload = (
            json.dumps(self.data if data is None else data, ensure_ascii=False, indent=2) + "\n"
        ).encode("utf-8")
        try:
            atomic_write_bytes(
                self.path,
                self.path.parent,
                payload,
                mode=target_mode,
                owner=target_owner,
                expected_root_identity=self._snapshot_parent_identity,
            )
        except OSError as exc:
            if exc.errno != errno.EBUSY or current_revision is None:
                raise
            _hot_overwrite_bind_mounted_file(
                self.path,
                payload,
                expected_identity=current_identity,
            )
        self._snapshot_revision = hashlib.sha256(payload).hexdigest()
        try:
            saved_stat = self.path.stat()
            self._snapshot_identity = (saved_stat.st_dev, saved_stat.st_ino)
        except OSError as exc:
            raise StorageDataError() from exc

    @property
    def auth_key(self) -> str:
        return _normalize_auth_key(os.getenv("CHATGPT2API_AUTH_KEY") or self.data.get("auth-key"))

    @property
    def accounts_file(self) -> Path:
        return DATA_DIR / "accounts.json"

    @property
    def refresh_account_interval_minute(self) -> int:
        return _normalize_positive_int(self.data.get("refresh_account_interval_minute", 5), 5, 0)

    @property
    def image_retention_days(self) -> int:
        return _normalize_positive_int(self.data.get("image_retention_days", 30), 30, 1)

    @property
    def image_poll_timeout_secs(self) -> int:
        return _normalize_positive_int(self.data.get("image_poll_timeout_secs", 120), 120, 1)

    @property
    def image_poll_interval_secs(self) -> float:
        return _normalize_nonnegative_float(self.data.get("image_poll_interval_secs", 10.0), 10.0, 0.5)

    @property
    def image_poll_initial_wait_secs(self) -> float:
        """Image generation upstream takes ~30s; polling immediately wastes requests
        and trips a transient 429. Default 10s gives the conversation document time
        to commit before the first poll."""
        return _normalize_nonnegative_float(self.data.get("image_poll_initial_wait_secs", 10.0), 10.0, 0.0)

    @property
    def image_account_concurrency(self) -> int:
        return _normalize_positive_int(self.data.get("image_account_concurrency", 3), 3, 1)

    @property
    def image_parallel_generation(self) -> bool:
        return _normalize_bool(self.data.get("image_parallel_generation"), True)

    @property
    def image_settle_enabled(self) -> bool:
        """图片二次确认机制：找到 file_ids 后等待一段时间再次确认。"""
        return _normalize_bool(self.data.get("image_settle_enabled"), True)

    @property
    def image_check_before_hit_enabled(self) -> bool:
        """先check再hit：通过轮询确认 file_ids 存在后再返回，而非仅依赖 SSE 事件。"""
        return _normalize_bool(self.data.get("image_check_before_hit_enabled"), True)

    @property
    def image_remove_conversation_after_result(self) -> bool:
        """出图成功后异步隐藏 ChatGPT 本地对话记录。"""
        return _normalize_bool(self.data.get("image_remove_conversation_after_result"), False)

    @property
    def image_remove_conversation_always(self) -> bool:
        """无论是否出图，画图请求结束后都异步隐藏 ChatGPT 本地对话记录。"""
        return _normalize_bool(self.data.get("image_remove_conversation_always"), False)

    @property
    def image_settle_secs(self) -> float:
        """二次确认等待时间（秒）。"""
        return _normalize_nonnegative_float(self.data.get("image_settle_secs", 2.0), 2.0, 0.5)

    @property
    def image_timeout_retry_secs(self) -> int:
        return _normalize_positive_int(self.data.get("image_timeout_retry_secs", 30), 30, 1)

    @property
    def auto_remove_invalid_accounts(self) -> bool:
        return _normalize_bool(self.data.get("auto_remove_invalid_accounts"), False)

    @property
    def auto_remove_rate_limited_accounts(self) -> bool:
        return _normalize_bool(self.data.get("auto_remove_rate_limited_accounts"), False)

    @property
    def auto_relogin_after_refresh(self) -> bool:
        return _normalize_bool(self.data.get("auto_relogin_after_refresh"), False)

    @property
    def log_levels(self) -> list[str]:
        return _normalize_log_levels(self.data.get("log_levels"))

    @property
    def sensitive_words(self) -> list[str]:
        return _normalize_sensitive_words(self.data.get("sensitive_words"))

    @property
    def ai_review(self) -> dict[str, object]:
        value = self.data.get("ai_review")
        return value if isinstance(value, dict) else {}

    @property
    def global_system_prompt(self) -> str:
        return _text_or_default(self.data.get("global_system_prompt"))

    @property
    def default_upstream_model_name(self) -> str:
        return _text_or_default(self.data.get("default_upstream_model_name"), "gpt-5-5") or "gpt-5-5"

    @property
    def default_thinking_effort(self) -> str:
        return _normalize_thinking_effort(self.data.get("default_thinking_effort"))

    @property
    def images_dir(self) -> Path:
        path = DATA_DIR / "images"
        path.mkdir(parents=True, exist_ok=True)
        return path

    @property
    def image_thumbnails_dir(self) -> Path:
        path = DATA_DIR / "image_thumbnails"
        path.mkdir(parents=True, exist_ok=True)
        return path

    def cleanup_old_images(self) -> int:
        cutoff = time.time() - self.image_retention_days * 86400
        removed = 0
        try:
            root = authorized_root(self.images_dir)
        except OSError:
            return 0
        for candidate in root.rglob("*"):
            try:
                rel = candidate.relative_to(root).as_posix()
            except (OSError, ValueError):
                continue
            if Path(rel).suffix.lower() not in _IMAGE_FILE_EXTENSIONS:
                continue
            try:
                path = resolve_under_root(root, rel)
                opened = open_checked_file(path, root, root)
            except OSError:
                continue
            candidate_stat = opened.stat_result
            try:
                old = candidate_stat.st_mtime < cutoff
            finally:
                opened.file.close()
            if not old:
                continue
            index_file = DATA_DIR / "image_index.json"
            with image_rel_lock(index_file, rel):
                try:
                    current = open_checked_file(path, root, root)
                except OSError:
                    continue
                try:
                    current_stat = current.stat_result
                    same_candidate = (
                        current_stat.st_dev == candidate_stat.st_dev
                        and current_stat.st_ino == candidate_stat.st_ino
                        and current_stat.st_mtime_ns == candidate_stat.st_mtime_ns
                        and current_stat.st_size == candidate_stat.st_size
                        and current_stat.st_mtime < cutoff
                    )
                finally:
                    current.file.close()
                if not same_candidate:
                    continue
                try:
                    if delete_checked_file(path, root):
                        removed += 1
                except OSError:
                    continue
        return removed

    @property
    def base_url(self) -> str:
        return parse_public_url(
            _text_or_default(
                os.getenv("CHATGPT2API_BASE_URL")
                or self.data.get("base_url")
            )
        )

    @property
    def app_version(self) -> str:
        try:
            value = VERSION_FILE.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return "0.0.0"
        return value or "0.0.0"

    def get(self) -> dict[str, object]:
        with self._lock:
            return self._get_locked()

    def _get_locked(self) -> dict[str, object]:
        data = {
            key: copy.deepcopy(self.data[key])
            for key in _PUBLIC_CONFIG_RAW_KEYS
            if key in self.data
        }
        data["refresh_account_interval_minute"] = self.refresh_account_interval_minute
        data["image_retention_days"] = self.image_retention_days
        data["image_poll_timeout_secs"] = self.image_poll_timeout_secs
        data["image_poll_interval_secs"] = self.image_poll_interval_secs
        data["image_poll_initial_wait_secs"] = self.image_poll_initial_wait_secs
        data["image_account_concurrency"] = self.image_account_concurrency
        data["image_timeout_retry_secs"] = self.image_timeout_retry_secs
        data["image_parallel_generation"] = self.image_parallel_generation
        data["image_settle_enabled"] = self.image_settle_enabled
        data["image_check_before_hit_enabled"] = self.image_check_before_hit_enabled
        data["image_settle_secs"] = self.image_settle_secs
        data["image_remove_conversation_after_result"] = self.image_remove_conversation_after_result
        data["image_remove_conversation_always"] = self.image_remove_conversation_always
        data["auto_remove_invalid_accounts"] = self.auto_remove_invalid_accounts
        data["auto_remove_rate_limited_accounts"] = self.auto_remove_rate_limited_accounts
        data["auto_relogin_after_refresh"] = self.auto_relogin_after_refresh
        data["log_levels"] = self.log_levels
        data["sensitive_words"] = self.sensitive_words
        ai_review = self.ai_review
        data["ai_review"] = {
            "enabled": _normalize_bool(ai_review.get("enabled"), False),
            "base_url": _public_config_url(ai_review.get("base_url")),
            "api_key": _public_secret(ai_review.get("api_key")),
            "model": _text_or_default(ai_review.get("model")),
            "prompt": _text_or_default(ai_review.get("prompt")),
        }
        data["global_system_prompt"] = self.global_system_prompt
        data["default_upstream_model_name"] = self.default_upstream_model_name
        data["default_thinking_effort"] = self.default_thinking_effort
        backup = self.get_backup_settings()
        backup["secret_access_key"] = _public_secret(backup.get("secret_access_key"))
        backup["passphrase"] = _public_secret(backup.get("passphrase"))
        data["backup"] = backup
        image_storage = self.get_image_storage_settings()
        image_storage["webdav_password"] = _public_secret(image_storage.get("webdav_password"))
        image_storage["webdav_url"] = _public_config_url(image_storage.get("webdav_url"))
        data["image_storage"] = image_storage
        data["chat_completion_cache"] = self.get_chat_completion_cache_settings()
        data["proxy_runtime"] = self.get_public_proxy_runtime_settings()
        if "proxy" in data:
            data["proxy"] = _public_config_url(data.get("proxy"))
        if "base_url" in data:
            data["base_url"] = parse_public_url(_text_or_default(data.get("base_url")))
        data["third_party_apps"] = self.get_third_party_apps_settings()
        data.pop("auth-key", None)
        return data

    def get_proxy_settings(self) -> str:
        return str(self.data.get("proxy") or "").strip()

    def get_proxy_runtime_settings(self) -> dict[str, object]:
        return _normalize_proxy_runtime_settings(self.data.get("proxy_runtime"))

    def get_public_proxy_runtime_settings(self) -> dict[str, object]:
        runtime = copy.deepcopy(self.get_proxy_runtime_settings())
        for field in ("proxy_url", "resource_proxy_url"):
            runtime[field] = _public_config_url(runtime.get(field))
        clearance = runtime.get("clearance") if isinstance(runtime.get("clearance"), dict) else {}
        if isinstance(clearance, dict):
            cf_cookies = str(clearance.get("cf_cookies") or "").strip()
            cf_clearance = str(clearance.get("cf_clearance") or "").strip()
            clearance["cf_cookies"] = ""
            clearance["cf_clearance"] = ""
            clearance["has_cf_cookies"] = bool(cf_cookies)
            clearance["has_cf_clearance"] = bool(cf_clearance)
            clearance["flaresolverr_url"] = _public_config_url(clearance.get("flaresolverr_url"))
        return runtime

    def get_third_party_apps_settings(self) -> dict[str, object]:
        return _normalize_third_party_apps_settings(self.data.get("third_party_apps"))

    def update(self, data: dict[str, object]) -> dict[str, object]:
        with self._lock:
            return self._update_locked(data)

    def _update_locked(self, data: dict[str, object]) -> dict[str, object]:
        next_data = copy.deepcopy(self.data)
        next_data.update(dict(data or {}))
        next_data = _normalize_config_update_values(next_data)
        next_data["proxy"] = _restore_redacted_url(next_data.get("proxy"), self.data.get("proxy"))
        next_data["base_url"] = parse_public_url(_text_or_default(next_data.get("base_url")))
        if "sensitive_words" in next_data:
            next_data["sensitive_words"] = _normalize_sensitive_words(next_data.get("sensitive_words"))
        if "global_system_prompt" in next_data:
            next_data["global_system_prompt"] = _text_or_default(next_data.get("global_system_prompt"))
        if "default_upstream_model_name" in next_data:
            next_data["default_upstream_model_name"] = (
                _text_or_default(next_data.get("default_upstream_model_name"), "gpt-5-5")
                or "gpt-5-5"
            )
        if "default_thinking_effort" in next_data:
            next_data["default_thinking_effort"] = _normalize_thinking_effort(
                next_data.get("default_thinking_effort")
            )
        if "log_levels" in next_data:
            next_data["log_levels"] = _normalize_log_levels(next_data.get("log_levels"))
        if "ai_review" in next_data:
            next_data["ai_review"] = _restore_masked_fields(
                next_data.get("ai_review"),
                self.data.get("ai_review"),
                {"api_key"},
            )
            if isinstance(next_data["ai_review"], dict):
                next_data["ai_review"]["base_url"] = _restore_redacted_url(
                    next_data["ai_review"].get("base_url"),
                    (self.data.get("ai_review") or {}).get("base_url") if isinstance(self.data.get("ai_review"), dict) else "",
                )
            next_data["ai_review"] = _normalize_ai_review_settings(next_data["ai_review"])
        if "backup" in next_data:
            next_data["backup"] = _normalize_backup_settings(
                _restore_masked_fields(next_data.get("backup"), self.data.get("backup"), {"secret_access_key", "passphrase"})
            )
        if "image_storage" in next_data:
            image_storage = _restore_masked_fields(
                next_data.get("image_storage"),
                self.data.get("image_storage"),
                {"webdav_password"},
            )
            image_storage["webdav_url"] = _restore_redacted_url(
                image_storage.get("webdav_url"),
                (self.data.get("image_storage") or {}).get("webdav_url") if isinstance(self.data.get("image_storage"), dict) else "",
            )
            next_data["image_storage"] = _normalize_image_storage_settings(image_storage)
            _validate_image_storage_settings(next_data["image_storage"])
        if "chat_completion_cache" in next_data:
            next_data["chat_completion_cache"] = _normalize_chat_completion_cache_settings(
                next_data.get("chat_completion_cache")
            )
        if "third_party_apps" in next_data:
            next_data["third_party_apps"] = _normalize_third_party_apps_settings(next_data.get("third_party_apps"))
        if "proxy_runtime" in next_data:
            incoming_runtime = next_data.get("proxy_runtime")
            if isinstance(incoming_runtime, dict):
                previous_runtime = self.get_proxy_runtime_settings()
                incoming_runtime = dict(incoming_runtime)
                for field in ("proxy_url", "resource_proxy_url"):
                    incoming_runtime[field] = _restore_redacted_url(
                        incoming_runtime.get(field),
                        previous_runtime.get(field),
                    )
                previous_clearance = self.get_proxy_runtime_settings().get("clearance")
                if isinstance(previous_clearance, dict):
                    incoming_clearance = incoming_runtime.get("clearance")
                    if isinstance(incoming_clearance, dict):
                        incoming_clearance = dict(incoming_clearance)
                        incoming_clearance["flaresolverr_url"] = _restore_redacted_url(
                            incoming_clearance.get("flaresolverr_url"),
                            previous_clearance.get("flaresolverr_url"),
                        )
                        incoming_runtime["clearance"] = incoming_clearance
                    incoming_runtime["_existing_cf_cookies"] = previous_clearance.get("cf_cookies")
                    incoming_runtime["_existing_cf_clearance"] = previous_clearance.get("cf_clearance")
                    incoming_clearance = incoming_runtime.get("clearance")
                    if isinstance(incoming_clearance, dict):
                        incoming_clearance = dict(incoming_clearance)
                        incoming_clearance["flaresolverr_url"] = _restore_redacted_url(
                            incoming_clearance.get("flaresolverr_url"),
                            previous_clearance.get("flaresolverr_url"),
                        )
                        incoming_runtime["clearance"] = incoming_clearance
            next_data["proxy_runtime"] = _normalize_proxy_runtime_settings(incoming_runtime)
        next_data.pop("backup_state", None)
        self._save(next_data)
        self.data = next_data
        return self.get()

    def get_backup_settings(self) -> dict[str, object]:
        return _normalize_backup_settings(self.data.get("backup"))

    def get_image_storage_settings(self) -> dict[str, object]:
        return _normalize_image_storage_settings(self.data.get("image_storage"))

    def get_chat_completion_cache_settings(self) -> dict[str, object]:
        return _normalize_chat_completion_cache_settings(self.data.get("chat_completion_cache"))

    def get_storage_backend(self) -> StorageBackend:
        """获取存储后端实例（单例）"""
        with self._lock:
            if self._storage_backend is None:
                from services.storage.factory import create_storage_backend
                self._storage_backend = create_storage_backend(DATA_DIR)
            return self._storage_backend


def load_backup_state() -> dict[str, object]:
    return _normalize_backup_state(_read_json_object(BACKUP_STATE_FILE, name="backup_state.json"))


def save_backup_state(state: dict[str, object]) -> dict[str, object]:
    normalized = _normalize_backup_state(state)
    persisted = {key: value for key, value in normalized.items() if key != "last_error"}
    raw_code = _text_or_default(state.get("last_error_code"))
    raw_error = _text_or_default(state.get("last_error"))
    raw_status = state.get("last_error_status")
    status = raw_status if isinstance(raw_status, int) and not isinstance(raw_status, bool) and 400 <= raw_status <= 599 else None
    if raw_code or raw_error or raw_status is not None:
        code = raw_code if raw_code in (_BACKUP_STATE_FIXED_MESSAGES | _BACKUP_STATE_STATUS_MESSAGES) else "backup_failed"
        if (raw_status is not None and status is None) or (code in _BACKUP_STATE_STATUS_MESSAGES and status is None):
            code = "backup_failed"
            status = None
        persisted["last_error_code"] = code
        if status is not None and code in _BACKUP_STATE_STATUS_MESSAGES:
            persisted["last_error_status"] = status
    BACKUP_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    parent_stat = BACKUP_STATE_FILE.parent.stat()
    expected_parent_identity = (parent_stat.st_dev, parent_stat.st_ino)
    payload = (json.dumps(persisted, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    atomic_write_bytes(
        BACKUP_STATE_FILE,
        BACKUP_STATE_FILE.parent,
        payload,
        mode=0o600,
        expected_root_identity=expected_parent_identity,
    )
    return normalized


config = ConfigStore(CONFIG_FILE)
