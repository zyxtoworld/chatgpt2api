from __future__ import annotations

import copy
from dataclasses import dataclass
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from threading import RLock
import time
from urllib.parse import urlsplit

from services.storage.base import StorageBackend, StorageConflictError, StorageDataError
from services.secure_file import authorized_root, delete_checked_file, open_checked_file, resolve_under_root
from services.protocol.error_response import PublicSafeValueError
from services.url_utils import redact_url_credentials as _redact_url_credentials

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
_CONFIG_FILE_ENV = os.getenv("CHATGPT2API_CONFIG_FILE", "").strip()
CONFIG_FILE = Path(_CONFIG_FILE_ENV) if _CONFIG_FILE_ENV else BASE_DIR / "config.json"
VERSION_FILE = BASE_DIR / "VERSION"
BACKUP_STATE_FILE = DATA_DIR / "backup_state.json"

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


def parse_public_url(value: object) -> str:
    """Return a safe absolute HTTP(S) base URL, or empty on invalid input."""
    text = str(value or "").strip().rstrip("/")
    if not text or any(character.isspace() or ord(character) < 32 or ord(character) == 127 for character in text):
        return ""
    try:
        parsed = urlsplit(text)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            return ""
        if parsed.username is not None or parsed.password is not None or "@" in parsed.netloc:
            return ""
        if parsed.query or parsed.fragment:
            return ""
        _ = parsed.port
        authority = parsed.netloc
        if authority.endswith(":"):
            return ""
        if authority.startswith("["):
            if authority.count("[") != 1 or authority.count("]") != 1:
                return ""
            closing = authority.find("]")
            suffix = authority[closing + 1:]
            if suffix and (not suffix.startswith(":") or suffix == ":"):
                return ""
        elif authority.count(":") > 1:
            return ""
        if not parsed.hostname:
            return ""
    except (TypeError, ValueError):
        return ""
    return text


def _restore_redacted_url(value: object, existing: object) -> object:
    candidate = str(value or "").strip()
    previous = str(existing or "").strip()
    redacted_previous = _redact_url_credentials(previous)
    if previous and redacted_previous and candidate == redacted_previous:
        return existing
    return value


def _public_secret(value: object) -> str:
    return PUBLIC_SECRET_MASK if str(value or "").strip() else ""


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
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "on"}:
            return True
        if lowered in {"0", "false", "no", "off"}:
            return False
        return default
    if value is None:
        return default
    return bool(value)


def _normalize_positive_int(value: object, default: int, minimum: int = 0) -> int:
    try:
        normalized = int(value)
    except (OverflowError, TypeError, ValueError):
        normalized = default
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
        "account_id": str(source.get("account_id") or "").strip(),
        "access_key_id": str(source.get("access_key_id") or "").strip(),
        "secret_access_key": str(source.get("secret_access_key") or "").strip(),
        "bucket": str(source.get("bucket") or "").strip(),
        "prefix": str(source.get("prefix") or "backups").strip().strip("/") or "backups",
        "interval_minutes": _normalize_positive_int(source.get("interval_minutes"), 360, 1),
        "rotation_keep": _normalize_positive_int(source.get("rotation_keep"), 10, 0),
        "encrypt": _normalize_bool(source.get("encrypt"), False),
        "passphrase": str(source.get("passphrase") or "").strip(),
        "include": _normalize_backup_include(source.get("include")),
    }


def _normalize_backup_state(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    raw_error = str(source.get("last_error") or "").strip()
    error_code = str(source.get("last_error_code") or "").strip()
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
    return {
        "last_started_at": str(source.get("last_started_at") or "").strip() or None,
        "last_finished_at": str(source.get("last_finished_at") or "").strip() or None,
        "last_status": str(source.get("last_status") or "idle").strip() or "idle",
        "last_error": last_error,
        "last_object_key": str(source.get("last_object_key") or "").strip() or None,
    }


def _normalize_image_storage_settings(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else {}
    mode = str(source.get("mode") or "local").strip().lower()
    if mode not in {"local", "webdav", "both"}:
        mode = "local"
    enabled = _normalize_bool(source.get("enabled"), False)
    if not enabled:
        mode = "local"
    root_path = str(source.get("webdav_root_path") or DEFAULT_IMAGE_STORAGE["webdav_root_path"]).strip().strip("/")
    return {
        "enabled": enabled,
        "mode": mode,
        "webdav_url": str(source.get("webdav_url") or "").strip().rstrip("/"),
        "webdav_username": str(source.get("webdav_username") or "").strip(),
        "webdav_password": str(source.get("webdav_password") or "").strip(),
        "webdav_root_path": root_path or str(DEFAULT_IMAGE_STORAGE["webdav_root_path"]),
        "public_base_url": parse_public_url(source.get("public_base_url")),
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

    egress_mode = str(source.get("egress_mode") or DEFAULT_PROXY_RUNTIME["egress_mode"]).strip().lower()
    if egress_mode not in {"direct", "single_proxy"}:
        egress_mode = str(DEFAULT_PROXY_RUNTIME["egress_mode"])

    clearance_mode = str(clearance_source.get("mode") or default_clearance["mode"]).strip().lower()
    if clearance_mode not in {"none", "manual", "flaresolverr"}:
        clearance_mode = str(default_clearance["mode"])

    user_agent = str(clearance_source.get("user_agent") or default_clearance["user_agent"]).strip()
    browser = str(clearance_source.get("browser") or default_clearance["browser"]).strip()

    existing_clearance_cookies = str(source.get("_existing_cf_cookies") or "").strip()
    existing_cf_clearance = str(source.get("_existing_cf_clearance") or "").strip()
    cf_cookies = str(clearance_source.get("cf_cookies") or "").strip()
    cf_clearance = str(clearance_source.get("cf_clearance") or "").strip()
    if not cf_cookies and _normalize_bool(clearance_source.get("has_cf_cookies"), False):
        cf_cookies = existing_clearance_cookies
    if not cf_clearance and _normalize_bool(clearance_source.get("has_cf_clearance"), False):
        cf_clearance = existing_cf_clearance

    return {
        "enabled": _normalize_bool(source.get("enabled"), bool(DEFAULT_PROXY_RUNTIME["enabled"])),
        "egress_mode": egress_mode,
        "proxy_url": str(source.get("proxy_url") or "").strip(),
        "resource_proxy_url": str(source.get("resource_proxy_url") or "").strip(),
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
            "flaresolverr_url": str(clearance_source.get("flaresolverr_url") or "").strip(),
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
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        if fail_closed:
            raise StorageDataError() from exc
        return {}
    if isinstance(data, dict):
        return data
    if fail_closed:
        raise StorageDataError()
    return {}


def _config_file_revision(path: Path) -> str | None:
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        return None
    except Exception as exc:
        raise StorageDataError() from exc
    return hashlib.sha256(payload).hexdigest()


def _load_settings() -> LoadedSettings:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    raw_config = _read_json_object(CONFIG_FILE, name="config.json")
    auth_key = _normalize_auth_key(os.getenv("CHATGPT2API_AUTH_KEY") or raw_config.get("auth-key"))
    if _is_invalid_auth_key(auth_key):
        raise ValueError(
            "❌ auth-key 未设置！\n"
            "请在环境变量 CHATGPT2API_AUTH_KEY 中设置，或者在 config.json 中填写 auth-key。"
        )

    try:
        refresh_interval = int(raw_config.get("refresh_account_interval_minute", 5))
    except (TypeError, ValueError):
        refresh_interval = 5

    return LoadedSettings(
        auth_key=auth_key,
        refresh_account_interval_minute=refresh_interval,
    )


class ConfigStore:
    def __init__(self, path: Path):
        self.path = path
        self._lock = RLock()
        self._snapshot_revision: str | None = None
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
        data = _read_json_object(self.path, name="config.json", fail_closed=True)
        self._snapshot_revision = _config_file_revision(self.path)
        return data

    def _save(self, data: dict[str, object] | None = None) -> None:
        _read_json_object(self.path, name="config.json", fail_closed=True)
        current_revision = _config_file_revision(self.path)
        if current_revision != self._snapshot_revision:
            raise StorageConflictError()
        payload = (
            json.dumps(self.data if data is None else data, ensure_ascii=False, indent=2) + "\n"
        ).encode("utf-8")
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            tmp_path.write_bytes(payload)
            tmp_path.replace(self.path)
        except Exception:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise
        self._snapshot_revision = hashlib.sha256(payload).hexdigest()

    @property
    def auth_key(self) -> str:
        return _normalize_auth_key(os.getenv("CHATGPT2API_AUTH_KEY") or self.data.get("auth-key"))

    @property
    def accounts_file(self) -> Path:
        return DATA_DIR / "accounts.json"

    @property
    def refresh_account_interval_minute(self) -> int:
        try:
            return int(self.data.get("refresh_account_interval_minute", 5))
        except (TypeError, ValueError):
            return 5

    @property
    def image_retention_days(self) -> int:
        try:
            return max(1, int(self.data.get("image_retention_days", 30)))
        except (TypeError, ValueError):
            return 30

    @property
    def image_poll_timeout_secs(self) -> int:
        try:
            return max(1, int(self.data.get("image_poll_timeout_secs", 120)))
        except (TypeError, ValueError):
            return 120

    @property
    def image_poll_interval_secs(self) -> float:
        try:
            return max(0.5, float(self.data.get("image_poll_interval_secs", 10.0)))
        except (TypeError, ValueError):
            return 10.0

    @property
    def image_poll_initial_wait_secs(self) -> float:
        """Image generation upstream takes ~30s; polling immediately wastes requests
        and trips a transient 429. Default 10s gives the conversation document time
        to commit before the first poll."""
        try:
            return max(0.0, float(self.data.get("image_poll_initial_wait_secs", 10.0)))
        except (TypeError, ValueError):
            return 10.0

    @property
    def image_account_concurrency(self) -> int:
        try:
            return max(1, int(self.data.get("image_account_concurrency", 3)))
        except (TypeError, ValueError):
            return 3

    @property
    def image_parallel_generation(self) -> bool:
        value = self.data.get("image_parallel_generation", True)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @property
    def image_settle_enabled(self) -> bool:
        """图片二次确认机制：找到 file_ids 后等待一段时间再次确认。"""
        value = self.data.get("image_settle_enabled", True)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @property
    def image_check_before_hit_enabled(self) -> bool:
        """先check再hit：通过轮询确认 file_ids 存在后再返回，而非仅依赖 SSE 事件。"""
        value = self.data.get("image_check_before_hit_enabled", True)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @property
    def image_remove_conversation_after_result(self) -> bool:
        """出图成功后异步隐藏 ChatGPT 本地对话记录。"""
        value = self.data.get("image_remove_conversation_after_result", False)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @property
    def image_remove_conversation_always(self) -> bool:
        """无论是否出图，画图请求结束后都异步隐藏 ChatGPT 本地对话记录。"""
        return _normalize_bool(self.data.get("image_remove_conversation_always"), False)

    @property
    def image_settle_secs(self) -> float:
        """二次确认等待时间（秒）。"""
        try:
            return max(0.5, float(self.data.get("image_settle_secs", 2.0)))
        except (TypeError, ValueError):
            return 2.0

    @property
    def image_timeout_retry_secs(self) -> int:
        try:
            return max(1, int(self.data.get("image_timeout_retry_secs", 30)))
        except (TypeError, ValueError):
            return 30

    @property
    def auto_remove_invalid_accounts(self) -> bool:
        value = self.data.get("auto_remove_invalid_accounts", False)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @property
    def auto_remove_rate_limited_accounts(self) -> bool:
        value = self.data.get("auto_remove_rate_limited_accounts", False)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @property
    def auto_relogin_after_refresh(self) -> bool:
        value = self.data.get("auto_relogin_after_refresh", False)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    @property
    def log_levels(self) -> list[str]:
        levels = self.data.get("log_levels")
        if not isinstance(levels, list):
            return []
        allowed = {"debug", "info", "warning", "error"}
        return [level for item in levels if (level := str(item or "").strip().lower()) in allowed]

    @property
    def sensitive_words(self) -> list[str]:
        words = self.data.get("sensitive_words")
        return [word for item in words if (word := str(item or "").strip())] if isinstance(words, list) else []

    @property
    def ai_review(self) -> dict[str, object]:
        value = self.data.get("ai_review")
        return value if isinstance(value, dict) else {}

    @property
    def global_system_prompt(self) -> str:
        return str(self.data.get("global_system_prompt") or "").strip()

    @property
    def default_upstream_model_name(self) -> str:
        return str(self.data.get("default_upstream_model_name") or "gpt-5-5").strip()

    @property
    def default_thinking_effort(self) -> str:
        value = str(self.data.get("default_thinking_effort") or "auto").strip().lower()
        return value if value in {"auto", "standard", "extended", "max"} else "auto"

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
            try:
                path = resolve_under_root(root, rel)
                opened = open_checked_file(path, root, root)
            except OSError:
                continue
            try:
                old = opened.stat_result.st_mtime < cutoff
            finally:
                opened.file.close()
            if not old:
                continue
            try:
                if delete_checked_file(path, root):
                    removed += 1
            except OSError:
                continue
        return removed

    @property
    def base_url(self) -> str:
        return str(
            os.getenv("CHATGPT2API_BASE_URL")
            or self.data.get("base_url")
            or ""
        ).strip().rstrip("/")

    @property
    def app_version(self) -> str:
        try:
            value = VERSION_FILE.read_text(encoding="utf-8").strip()
        except FileNotFoundError:
            return "0.0.0"
        return value or "0.0.0"

    def get(self) -> dict[str, object]:
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
        data["image_remove_conversation_after_result"] = self.image_remove_conversation_after_result
        data["image_remove_conversation_always"] = self.image_remove_conversation_always
        data["auto_remove_invalid_accounts"] = self.auto_remove_invalid_accounts
        data["auto_remove_rate_limited_accounts"] = self.auto_remove_rate_limited_accounts
        data["auto_relogin_after_refresh"] = self.auto_relogin_after_refresh
        data["log_levels"] = self.log_levels
        data["sensitive_words"] = self.sensitive_words
        ai_review = copy.deepcopy(self.ai_review)
        if isinstance(ai_review, dict):
            ai_review["api_key"] = _public_secret(ai_review.get("api_key"))
            if "base_url" in ai_review:
                ai_review["base_url"] = _redact_url_credentials(ai_review.get("base_url"))
        data["ai_review"] = ai_review
        data["global_system_prompt"] = self.global_system_prompt
        data["default_upstream_model_name"] = self.default_upstream_model_name
        data["default_thinking_effort"] = self.default_thinking_effort
        backup = self.get_backup_settings()
        backup["secret_access_key"] = _public_secret(backup.get("secret_access_key"))
        backup["passphrase"] = _public_secret(backup.get("passphrase"))
        data["backup"] = backup
        image_storage = self.get_image_storage_settings()
        image_storage["webdav_password"] = _public_secret(image_storage.get("webdav_password"))
        image_storage["webdav_url"] = _redact_url_credentials(image_storage.get("webdav_url"))
        data["image_storage"] = image_storage
        data["chat_completion_cache"] = self.get_chat_completion_cache_settings()
        data["proxy_runtime"] = self.get_public_proxy_runtime_settings()
        if "proxy" in data:
            data["proxy"] = _redact_url_credentials(data.get("proxy"))
        if "base_url" in data:
            data["base_url"] = _redact_url_credentials(data.get("base_url"))
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
            runtime[field] = _redact_url_credentials(runtime.get(field))
        clearance = runtime.get("clearance") if isinstance(runtime.get("clearance"), dict) else {}
        if isinstance(clearance, dict):
            cf_cookies = str(clearance.get("cf_cookies") or "").strip()
            cf_clearance = str(clearance.get("cf_clearance") or "").strip()
            clearance["cf_cookies"] = ""
            clearance["cf_clearance"] = ""
            clearance["has_cf_cookies"] = bool(cf_cookies)
            clearance["has_cf_clearance"] = bool(cf_clearance)
            clearance["flaresolverr_url"] = _redact_url_credentials(clearance.get("flaresolverr_url"))
        return runtime

    def get_third_party_apps_settings(self) -> dict[str, object]:
        return _normalize_third_party_apps_settings(self.data.get("third_party_apps"))

    def update(self, data: dict[str, object]) -> dict[str, object]:
        with self._lock:
            return self._update_locked(data)

    def _update_locked(self, data: dict[str, object]) -> dict[str, object]:
        next_data = copy.deepcopy(self.data)
        next_data.update(dict(data or {}))
        next_data["proxy"] = _restore_redacted_url(next_data.get("proxy"), self.data.get("proxy"))
        next_data["base_url"] = _restore_redacted_url(next_data.get("base_url"), self.data.get("base_url"))
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
        if self._storage_backend is None:
            from services.storage.factory import create_storage_backend
            self._storage_backend = create_storage_backend(DATA_DIR)
        return self._storage_backend


def load_backup_state() -> dict[str, object]:
    return _normalize_backup_state(_read_json_object(BACKUP_STATE_FILE, name="backup_state.json"))


def save_backup_state(state: dict[str, object]) -> dict[str, object]:
    normalized = _normalize_backup_state(state)
    persisted = {key: value for key, value in normalized.items() if key != "last_error"}
    raw_code = str(state.get("last_error_code") or "").strip()
    raw_error = str(state.get("last_error") or "").strip()
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
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=BACKUP_STATE_FILE.parent,
            prefix=f".{BACKUP_STATE_FILE.name}.",
            suffix=".tmp",
            delete=False,
        ) as temp_file:
            temp_path = Path(temp_file.name)
            json.dump(persisted, temp_file, ensure_ascii=False, indent=2)
            temp_file.write("\n")
            temp_file.flush()
            os.fsync(temp_file.fileno())
        os.replace(temp_path, BACKUP_STATE_FILE)
    except Exception:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise
    return normalized


config = ConfigStore(CONFIG_FILE)
