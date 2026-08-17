from __future__ import annotations

import hashlib
import hmac
import io
import json
import os
import subprocess
import tarfile
import threading
import uuid
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import quote, urlencode

from curl_cffi import requests

import services.config as config_module
from services.config import (
    BASE_DIR,
    CONFIG_FILE,
    DATA_DIR,
    config,
    load_backup_state,
    save_backup_state,
)
from services.image_storage_service import IMAGE_INDEX_FILE
from services.image_tags_service import TAGS_FILE
from services.protocol.error_response import PublicSafeError
from services.secure_file import authorized_root, open_checked_file, resolve_under_root
from services.storage.base import canonical_path_write_lock
from services.task_executor import BackgroundTaskQueueFullError, reserve_background_task
from utils.log import logger


# Backup download/detail still returns bytes to the API layer, so keep an
# explicit bounded contract instead of allowing an arbitrary R2 object into
# memory.  This accommodates the repository's current large data snapshots.
_MAX_R2_DOWNLOAD_BYTES = 512 * 1024 * 1024
# Listing must not advertise an object that the download path will reject.
_MAX_BACKUP_OBJECT_SIZE = _MAX_R2_DOWNLOAD_BYTES
_MAX_R2_LIST_RESPONSE_BYTES = 4 * 1024 * 1024
_MAX_R2_LIST_OBJECTS = 5000
_MAX_R2_LIST_PAGES = 25
_MAX_BACKUP_DETAIL_MEMBER_BYTES = 64 * 1024 * 1024
_MAX_BACKUP_DETAIL_MEMBERS = 5000
_R2_STREAM_CHUNK_BYTES = 64 * 1024
_ARCHIVE_READ_CHUNK_BYTES = 1024 * 1024


class _CappedBytesIO(io.BytesIO):
    """Bytes buffer that fails before an archive exceeds the download budget."""

    def __init__(self, limit: int) -> None:
        super().__init__()
        self._limit = limit

    def write(self, value: bytes | bytearray | memoryview) -> int:
        if self.tell() + len(value) > self._limit:
            raise BackupError("备份归档过大", code="backup_archive_too_large")
        return super().write(value)


def _ensure_backup_payload_size(payload: bytes) -> bytes:
    if len(payload) > _MAX_R2_DOWNLOAD_BYTES:
        raise BackupError("备份响应过大", code="r2_read_payload_invalid")
    return payload


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso_now() -> str:
    return _utc_now().replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _new_backup_object_key(settings: dict[str, object]) -> str:
    prefix = _clean(settings.get("prefix")).rstrip("/") or "backups"
    suffix = ".tar.gz.enc" if bool(settings.get("encrypt")) else ".tar.gz"
    timestamp = _utc_now().strftime("%Y%m%dT%H%M%SZ")
    operation_tag = uuid.uuid4().hex
    return f"{prefix}/backup-{timestamp}-{operation_tag}{suffix}"


def _clean(value: object) -> str:
    return str(value or "").strip()


def _backup_target_fingerprint(settings: dict[str, object]) -> str:
    """Return a stable hash of the destination and encoding identity."""
    target = {
        "account_id": _clean(settings.get("account_id")),
        "access_key_id": _clean(settings.get("access_key_id")),
        "secret_access_key": _clean(settings.get("secret_access_key")),
        "bucket": _clean(settings.get("bucket")),
        "prefix": _clean(settings.get("prefix")).strip("/") or "backups",
        "encrypt": bool(settings.get("encrypt")),
        "passphrase": _clean(settings.get("passphrase")),
    }
    encoded = json.dumps(target, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_hex(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _is_backup_object(key: object) -> bool:
    if not isinstance(key, str):
        return False
    name = key.rsplit("/", 1)[-1]
    return name.startswith("backup-") and (name.endswith(".tar.gz") or name.endswith(".tar.gz.enc"))


def _validate_backup_key(key: object, settings: dict[str, object]) -> str:
    if not isinstance(key, str):
        raise BackupError("备份对象 key 无效", code="backup_key_invalid")
    candidate = key.strip()
    if not candidate:
        raise BackupError("备份对象 key 不能为空", code="backup_key_required")
    prefix_value = settings.get("prefix")
    prefix = prefix_value.strip() if isinstance(prefix_value, str) else ""
    prefix = prefix.rstrip("/") or "backups"
    prefix_with_separator = f"{prefix}/"
    name = candidate.removeprefix(prefix_with_separator)
    if (
        not candidate.startswith(prefix_with_separator)
        or not name
        or "/" in name
        or not _is_backup_object(name)
    ):
        raise BackupError("备份对象 key 无效", code="backup_key_invalid")
    return candidate


def _parse_backup_size(value: object) -> int:
    text = _clean(value)
    if not text:
        return 0
    if len(text) > 19 or not text.isascii() or not text.isdecimal():
        raise BackupError("备份列表格式无效", code="r2_list_payload_invalid")
    try:
        size = int(text)
    except ValueError as exc:
        raise BackupError("备份列表格式无效", code="r2_list_payload_invalid") from exc
    if size > _MAX_BACKUP_OBJECT_SIZE:
        raise BackupError("备份列表格式无效", code="r2_list_payload_invalid")
    return size


def _hmac_sha256(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode("utf-8"), hashlib.sha256).digest()


def _openssl_encrypt(data: bytes, passphrase: str) -> bytes:
    env = dict(os.environ)
    env["CHATGPT2API_BACKUP_PASSPHRASE"] = passphrase
    try:
        result = subprocess.run(
            [
                "openssl",
                "enc",
                "-aes-256-cbc",
                "-pbkdf2",
                "-salt",
                "-md",
                "sha256",
                "-pass",
                "env:CHATGPT2API_BACKUP_PASSPHRASE",
            ],
            input=data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            env=env,
        )
    except FileNotFoundError as exc:
        raise BackupError("当前环境缺少 openssl，无法执行加密备份", code="backup_encrypt_unavailable") from exc
    except subprocess.CalledProcessError as exc:
        raise BackupError("加密备份失败：openssl 执行失败", code="backup_encrypt_failed") from exc
    return result.stdout


def _openssl_decrypt(data: bytes, passphrase: str) -> bytes:
    env = dict(os.environ)
    env["CHATGPT2API_BACKUP_PASSPHRASE"] = passphrase
    try:
        result = subprocess.run(
            [
                "openssl",
                "enc",
                "-d",
                "-aes-256-cbc",
                "-pbkdf2",
                "-md",
                "sha256",
                "-pass",
                "env:CHATGPT2API_BACKUP_PASSPHRASE",
            ],
            input=data,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
            env=env,
        )
    except FileNotFoundError as exc:
        raise BackupError("当前环境缺少 openssl，无法解密备份内容", code="backup_decrypt_unavailable") from exc
    except subprocess.CalledProcessError as exc:
        raise BackupError("解密备份失败：openssl 执行失败", code="backup_decrypt_failed") from exc
    return result.stdout


def _guess_content_type(name: str) -> str:
    if name.endswith(".json"):
        return "application/json"
    if name.endswith(".jsonl"):
        return "application/x-ndjson"
    if name.endswith(".tar.gz"):
        return "application/gzip"
    if name.endswith(".gz"):
        return "application/gzip"
    return "application/octet-stream"


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")


def _count_items(value: object) -> int:
    if isinstance(value, list):
        return len(value)
    raise ValueError("invalid snapshot")


def _public_backup_created_at(value: object) -> str | None:
    if not isinstance(value, str) or not value or len(value) > 64:
        return None
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return value


def _public_backup_trigger(value: object) -> str | None:
    return value if isinstance(value, str) and value in {"manual", "schedule"} else None


def _public_backup_app_version(value: object) -> str | None:
    if not isinstance(value, str) or not value or len(value) > 64 or not value.isascii():
        return None
    if any(not (char.isalnum() or char in ".-_+") for char in value):
        return None
    return value


def _public_backup_storage_backend(value: object) -> dict[str, str]:
    if not isinstance(value, dict):
        return {}
    backend = value.get("type")
    if not isinstance(backend, str):
        backend = value.get("backend")
    if backend not in {"json", "database", "git"}:
        return {}
    return {"type": backend}


def _public_archive_member_name(value: object) -> str | None:
    """Project only archive paths emitted by the local backup writer."""
    if not isinstance(value, str) or not value or len(value) > 256 or not value.isascii():
        return None
    if any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._/-" for char in value):
        return None
    parts = value.split("/")
    if any(not part or part in {".", ".."} for part in parts):
        return None
    if value == "config.json":
        return value
    if parts == ["snapshots", "accounts.json"] or parts == ["snapshots", "auth_keys.json"]:
        return value
    if len(parts) >= 2 and parts[0] == "data":
        return value
    return None


def _read_archive_member_bounded(extracted: object) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = extracted.read(_ARCHIVE_READ_CHUNK_BYTES)
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            raise BackupError("解析备份压缩包失败，备份可能已损坏", code="backup_archive_invalid")
        total += len(chunk)
        if total > _MAX_BACKUP_DETAIL_MEMBER_BYTES:
            raise BackupError("解析备份压缩包失败，备份可能已损坏", code="backup_archive_invalid")
        chunks.append(chunk)
    return b"".join(chunks)


def _hash_archive_member(extracted: object, declared_size: int) -> tuple[int, str]:
    if declared_size > _MAX_BACKUP_DETAIL_MEMBER_BYTES:
        raise BackupError("解析备份压缩包失败，备份可能已损坏", code="backup_archive_invalid")
    digest = hashlib.sha256()
    total = 0
    while True:
        chunk = extracted.read(_ARCHIVE_READ_CHUNK_BYTES)
        if not chunk:
            break
        if not isinstance(chunk, bytes):
            raise BackupError("解析备份压缩包失败，备份可能已损坏", code="backup_archive_invalid")
        total += len(chunk)
        if total > _MAX_BACKUP_DETAIL_MEMBER_BYTES:
            raise BackupError("解析备份压缩包失败，备份可能已损坏", code="backup_archive_invalid")
        digest.update(chunk)
    return total, digest.hexdigest()


class BackupError(PublicSafeError):
    """Explicitly reviewed, user-actionable backup error."""

    def __init__(self, public_message: str, *, code: str = "backup_failed", status_code: int | None = None) -> None:
        super().__init__(public_message)
        self.code = str(code or "backup_failed").strip() or "backup_failed"
        self.status_code = status_code


class CloudflareR2Client:
    def __init__(self, settings: dict[str, object]) -> None:
        self.account_id = _clean(settings.get("account_id"))
        self.access_key_id = _clean(settings.get("access_key_id"))
        self.secret_access_key = _clean(settings.get("secret_access_key"))
        self.bucket = _clean(settings.get("bucket"))
        self.prefix = _clean(settings.get("prefix")) or "backups"
        self.session = requests.Session(impersonate="chrome", verify=True)

    def validate(self) -> None:
        missing = []
        if not self.account_id:
            missing.append("Account ID")
        if not self.access_key_id:
            missing.append("Access Key ID")
        if not self.secret_access_key:
            missing.append("Secret Access Key")
        if not self.bucket:
            missing.append("Bucket")
        if missing:
            raise BackupError(
                f"R2 配置不完整：缺少 {'、'.join(missing)}",
                code="r2_config_incomplete",
            )

    @property
    def endpoint(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"

    def _aws_v4_headers(
        self,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        body: bytes = b"",
        extra_headers: dict[str, str] | None = None,
    ) -> tuple[str, dict[str, str]]:
        now = _utc_now()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        encoded_query = urlencode(sorted((query or {}).items()))
        payload_hash = _sha256_hex(body)
        host = f"{self.account_id}.r2.cloudflarestorage.com"
        headers = {
            "host": host,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if extra_headers:
            for key, value in extra_headers.items():
                headers[key.lower()] = value.strip()
        sorted_items = sorted((key.lower(), " ".join(str(value).strip().split())) for key, value in headers.items())
        canonical_headers = "".join(f"{key}:{value}\n" for key, value in sorted_items)
        signed_headers = ";".join(key for key, _ in sorted_items)
        canonical_request = "\n".join([
            method.upper(),
            path,
            encoded_query,
            canonical_headers,
            signed_headers,
            payload_hash,
        ])
        credential_scope = f"{date_stamp}/auto/s3/aws4_request"
        string_to_sign = "\n".join([
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            _sha256_hex(canonical_request.encode("utf-8")),
        ])
        k_date = _hmac_sha256(("AWS4" + self.secret_access_key).encode("utf-8"), date_stamp)
        k_region = hmac.new(k_date, b"auto", hashlib.sha256).digest()
        k_service = hmac.new(k_region, b"s3", hashlib.sha256).digest()
        k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
        signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self.access_key_id}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        request_headers = {key: value for key, value in headers.items()}
        request_headers["authorization"] = authorization
        return encoded_query, request_headers

    def _request(
        self,
        method: str,
        key: str = "",
        *,
        query: dict[str, str] | None = None,
        body: bytes = b"",
        extra_headers: dict[str, str] | None = None,
        timeout: float = 60.0,
        stream: bool | None = None,
    ):
        object_path = f"/{self.bucket}"
        if key:
            object_path += f"/{quote(key.lstrip('/'), safe='/')}"
        encoded_query, headers = self._aws_v4_headers(method, object_path, query=query, body=body, extra_headers=extra_headers)
        url = f"{self.endpoint}{object_path}"
        if encoded_query:
            url += f"?{encoded_query}"
        request_kwargs = {
            "headers": headers,
            "data": body,
            "timeout": timeout,
        }
        if stream is not None:
            request_kwargs["stream"] = stream
        response = self.session.request(method.upper(), url, **request_kwargs)
        return response

    @staticmethod
    def _close_response(response: object) -> None:
        try:
            response.close()
        except Exception:
            pass

    @staticmethod
    def _content_length(response: object, limit: int, *, error_code: str) -> int | None:
        headers = getattr(response, "headers", None)
        if headers is None or not hasattr(headers, "get"):
            return None
        value = headers.get("content-length")
        if value is None:
            value = headers.get("Content-Length")
        if value is None:
            return None
        if not isinstance(value, str):
            raise BackupError("备份响应大小无效", code=error_code)
        text = value.strip()
        if not text or len(text) > 19 or not text.isascii() or not text.isdecimal():
            raise BackupError("备份响应大小无效", code=error_code)
        try:
            size = int(text)
        except ValueError as exc:
            raise BackupError("备份响应大小无效", code=error_code) from exc
        if size > limit:
            raise BackupError("备份响应过大", code=error_code)
        return size

    @classmethod
    def _read_bounded_response(cls, response: object, limit: int, *, error_code: str) -> bytes:
        cls._content_length(response, limit, error_code=error_code)
        chunks: list[bytes] = []
        total = 0
        try:
            for chunk in response.iter_content(chunk_size=_R2_STREAM_CHUNK_BYTES):
                if not isinstance(chunk, (bytes, bytearray, memoryview)):
                    raise BackupError("备份响应内容无效", code=error_code)
                part = bytes(chunk)
                total += len(part)
                if total > limit:
                    raise BackupError("备份响应过大", code=error_code)
                chunks.append(part)
        except BackupError:
            raise
        except Exception as exc:
            raise BackupError("读取备份响应失败", code=error_code) from exc
        return b"".join(chunks)

    def test_connection(self) -> dict[str, object]:
        self.validate()
        response = self._request(
            "GET",
            query={"list-type": "2", "max-keys": "1"},
            timeout=30.0,
            stream=True,
        )
        try:
            if response.status_code >= 400:
                raise BackupError(
                    f"连接 R2 失败：HTTP {response.status_code}",
                    code="r2_connection_failed",
                    status_code=response.status_code,
                )
            return {"ok": True, "status": int(response.status_code)}
        finally:
            self._close_response(response)

    def upload_bytes(self, key: str, payload: bytes, *, content_type: str, metadata: dict[str, str] | None = None) -> dict[str, object]:
        headers = {"content-type": content_type}
        if metadata:
            for item_key, item_value in metadata.items():
                headers[f"x-amz-meta-{item_key}"] = str(item_value)
        response = self._request("PUT", key, body=payload, extra_headers=headers, stream=True)
        try:
            if response.status_code >= 400:
                raise BackupError(
                    f"上传备份失败：HTTP {response.status_code}",
                    code="r2_upload_failed",
                    status_code=response.status_code,
                )
            return {"key": key, "etag": str(response.headers.get("etag") or "").strip('"')}
        finally:
            self._close_response(response)

    def delete_object(self, key: str) -> None:
        response = self._request("DELETE", key, timeout=30.0, stream=True)
        try:
            if response.status_code >= 400 and response.status_code != 404:
                raise BackupError(
                    f"删除备份失败：HTTP {response.status_code}",
                    code="r2_delete_failed",
                    status_code=response.status_code,
                )
        finally:
            self._close_response(response)

    def download_bytes(self, key: str) -> bytes:
        response = self._request("GET", key, timeout=60.0, stream=True)
        try:
            if response.status_code >= 400:
                raise BackupError(
                    f"读取备份失败：HTTP {response.status_code}",
                    code="r2_read_failed",
                    status_code=response.status_code,
                )
            return self._read_bounded_response(
                response,
                _MAX_R2_DOWNLOAD_BYTES,
                error_code="r2_read_payload_invalid",
            )
        finally:
            self._close_response(response)

    def list_objects(self) -> list[dict[str, object]]:
        items: list[dict[str, object]] = []
        continuation = ""
        page_count = 0
        while True:
            page_count += 1
            if page_count > _MAX_R2_LIST_PAGES:
                raise BackupError("备份列表过大", code="r2_list_limit_exceeded")
            query = {"list-type": "2", "prefix": f"{self.prefix.rstrip('/')}/", "max-keys": "1000"}
            if continuation:
                query["continuation-token"] = continuation
            response = self._request("GET", query=query, timeout=30.0, stream=True)
            try:
                if response.status_code >= 400:
                    raise BackupError(
                        f"获取备份列表失败：HTTP {response.status_code}",
                        code="r2_list_failed",
                        status_code=response.status_code,
                    )
                try:
                    text = self._read_bounded_response(
                        response,
                        _MAX_R2_LIST_RESPONSE_BYTES,
                        error_code="r2_list_payload_invalid",
                    ).decode("utf-8")
                except UnicodeDecodeError as exc:
                    raise BackupError("备份列表格式无效", code="r2_list_payload_invalid") from exc
                page_items: list[dict[str, object]] = []
                for block in text.split("<Contents>")[1:]:
                    key = _clean(block.split("<Key>", 1)[1].split("</Key>", 1)[0]) if "<Key>" in block else ""
                    if not key:
                        continue
                    size_text = _clean(block.split("<Size>", 1)[1].split("</Size>", 1)[0]) if "<Size>" in block else "0"
                    updated = _clean(block.split("<LastModified>", 1)[1].split("</LastModified>", 1)[0]) if "<LastModified>" in block else ""
                    page_items.append({
                        "key": key,
                        "size": _parse_backup_size(size_text),
                        "updated_at": updated,
                    })
                if len(items) + len(page_items) > _MAX_R2_LIST_OBJECTS:
                    raise BackupError("备份列表过大", code="r2_list_limit_exceeded")
                items.extend(page_items)
                truncated = "<IsTruncated>true</IsTruncated>" in text
                if not truncated:
                    break
                if "<NextContinuationToken>" not in text:
                    break
                continuation = _clean(text.split("<NextContinuationToken>", 1)[1].split("</NextContinuationToken>", 1)[0])
                if not continuation:
                    break
            finally:
                self._close_response(response)
        items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
        return items

    def close(self) -> None:
        self.session.close()


class BackupService:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._stop_timed_out = False
        self._restart_pending = False
        self._running = False
        self._running_object_key: str | None = None
        self._running_settings: dict[str, object] | None = None
        self._deleting_object_keys: set[str] = set()
        # A process that observes a previous running owner may recover it with
        # a fresh key.  Keep the abandoned object protected for that recovery
        # rotation only; the next successful rotation may reclaim it.
        self._recovered_stale_object_keys: set[str] = set()

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                if self._stop_timed_out:
                    self._restart_pending = True
                return
            self._stop_event = threading.Event()
            self._stop_timed_out = False
            self._restart_pending = False
            self._thread = threading.Thread(
                target=self._run,
                args=(self._stop_event,),
                daemon=True,
                name="r2-backup-scheduler",
            )
            self._thread.start()

    def stop(self) -> None:
        with self._lock:
            self._restart_pending = False
            self._stop_event.set()
            thread = self._thread
        if thread and thread.is_alive():
            thread.join(timeout=2)
        with self._lock:
            if self._thread is thread and thread is not None and thread.is_alive():
                self._stop_timed_out = True
            elif self._thread is thread:
                self._thread = None
                self._stop_timed_out = False

    def _run_scheduled_backup_once(self) -> None:
        try:
            self.run_scheduled_backup_if_needed()
        except Exception as exc:
            logger.warning({
                "event": "scheduled_backup_failed",
                "error_type": type(exc).__name__,
            })

    def _run(self, stop_event: threading.Event) -> None:
        try:
            while not stop_event.is_set():
                try:
                    reservation = reserve_background_task()
                except BackgroundTaskQueueFullError:
                    if stop_event.wait(30):
                        return
                    continue
                try:
                    with self._lock:
                        if stop_event.is_set():
                            reservation.cancel()
                            return
                        future = reservation.submit(self._run_scheduled_backup_once)
                except Exception:
                    if stop_event.wait(30):
                        return
                    continue
                while not future.done():
                    if stop_event.wait(0.05):
                        return
                if stop_event.wait(30):
                    return
        finally:
            current_thread = threading.current_thread()
            with self._lock:
                if self._thread is not current_thread:
                    return
                if self._restart_pending:
                    self._restart_pending = False
                    self._stop_timed_out = False
                    self._stop_event = threading.Event()
                    self._thread = threading.Thread(
                        target=self._run,
                        args=(self._stop_event,),
                        daemon=True,
                        name="r2-backup-scheduler",
                    )
                    self._thread.start()
                else:
                    self._thread = None
                    self._stop_timed_out = False
    def run_scheduled_backup_if_needed(self) -> None:
        settings = config.get_backup_settings()
        if not settings.get("enabled"):
            return
        state = self.get_status()
        if state.get("running"):
            return
        interval_minutes = int(settings.get("interval_minutes") or 360)
        last_finished_raw = _clean(state.get("last_finished_at"))
        if last_finished_raw:
            try:
                last_finished = datetime.fromisoformat(last_finished_raw.replace("Z", "+00:00"))
                elapsed = (_utc_now() - last_finished.astimezone(UTC)).total_seconds()
                if elapsed < interval_minutes * 60:
                    return
            except Exception:
                pass
        self.run_backup(trigger="schedule")

    def get_status(self) -> dict[str, object]:
        state = load_backup_state()
        state.pop("pending_target_fingerprint", None)
        return {
            **state,
            "running": self._running or state.get("last_status") == "running",
        }

    def is_configured(self) -> bool:
        settings = config.get_backup_settings()
        return all([
            _clean(settings.get("account_id")),
            _clean(settings.get("access_key_id")),
            _clean(settings.get("secret_access_key")),
            _clean(settings.get("bucket")),
        ])

    def get_settings(self) -> dict[str, object]:
        settings = dict(config.get_backup_settings())
        settings["secret_access_key"] = "********" if _clean(settings.get("secret_access_key")) else ""
        settings["passphrase"] = "********" if _clean(settings.get("passphrase")) else ""
        return settings

    def update_settings(self, payload: dict[str, object]) -> dict[str, object]:
        current = config.get_backup_settings()
        merged = dict(current)
        merged.update(dict(payload or {}))
        if "include" in payload and isinstance(payload.get("include"), dict):
            include = dict(current.get("include") or {})
            include.update(payload.get("include") or {})
            merged["include"] = include
        if payload.get("secret_access_key") == "********":
            merged["secret_access_key"] = current.get("secret_access_key")
        if payload.get("passphrase") == "********":
            merged["passphrase"] = current.get("passphrase")
        updated = config.update({"backup": merged})
        return dict(updated.get("backup") or {})

    def test_connection(self) -> dict[str, object]:
        client = CloudflareR2Client(config.get_backup_settings())
        try:
            return client.test_connection()
        finally:
            client.close()

    def list_backups(self) -> list[dict[str, object]]:
        if not self.is_configured():
            return []
        settings = config.get_backup_settings()
        client = CloudflareR2Client(settings)
        try:
            items = client.list_objects()
        finally:
            client.close()
        parsed: list[dict[str, object]] = []
        for item in items:
            try:
                key = _validate_backup_key(item.get("key"), settings)
            except BackupError:
                continue
            name = key.rsplit("/", 1)[-1]
            encrypted = name.endswith(".enc")
            parsed.append({
                "key": key,
                "name": name,
                "size": int(item.get("size") or 0),
                "updated_at": _public_backup_created_at(item.get("updated_at")),
                "encrypted": encrypted,
            })
        return parsed

    def delete_backup(self, key: str) -> None:
        settings = config.get_backup_settings()
        candidate = _validate_backup_key(key, settings)
        with self._lock:
            with canonical_path_write_lock(config_module.BACKUP_STATE_FILE):
                state = load_backup_state()
                if (
                    self._running_object_key == candidate
                    or candidate in self._deleting_object_keys
                    or (
                        state.get("last_status") == "running"
                        and state.get("pending_object_key") == candidate
                    )
                ):
                    raise BackupError(
                        "当前备份正在写入该对象，请稍后再删除",
                        code="backup_busy",
                    )
                self._deleting_object_keys.add(candidate)
        try:
            client = CloudflareR2Client(settings)
            try:
                client.validate()
                client.delete_object(candidate)
            finally:
                client.close()
        finally:
            with self._lock:
                self._deleting_object_keys.discard(candidate)

    def download_backup(self, key: str) -> dict[str, object]:
        settings = config.get_backup_settings()
        candidate = _validate_backup_key(key, settings)
        client = CloudflareR2Client(settings)
        try:
            client.validate()
            payload = client.download_bytes(candidate)
        finally:
            client.close()
        name = candidate.rsplit("/", 1)[-1] or "backup.bin"
        if candidate.endswith(".enc"):
            passphrase = _clean(settings.get("passphrase"))
            if not passphrase:
                raise BackupError(
                    "当前未配置加密口令，无法下载并解密已加密备份",
                    code="backup_download_passphrase_missing",
                )
            payload = _ensure_backup_payload_size(_openssl_decrypt(payload, passphrase))
            if name.endswith(".enc"):
                name = name[:-4] or "backup.tar.gz"
        return {
            "key": candidate,
            "name": name,
            "content_type": _guess_content_type(name),
            "payload": payload,
            "size": len(payload),
        }

    def get_backup_detail(self, key: str) -> dict[str, object]:
        settings = config.get_backup_settings()
        candidate = _validate_backup_key(key, settings)
        client = CloudflareR2Client(settings)
        try:
            client.validate()
            payload = client.download_bytes(candidate)
        finally:
            client.close()
        detail = self._decode_backup_payload(candidate, payload)
        detail["key"] = candidate
        detail["name"] = candidate.rsplit("/", 1)[-1]
        detail["encrypted"] = candidate.endswith(".enc")
        return detail

    def _save_state_if_owner(self, object_key: str, payload: dict[str, object]) -> bool:
        with canonical_path_write_lock(config_module.BACKUP_STATE_FILE):
            current = load_backup_state()
            if current.get("pending_object_key") != object_key:
                return False
            save_backup_state(payload)
            return True

    def run_backup(self, *, trigger: str = "manual") -> dict[str, object]:
        with self._lock:
            current: dict[str, object]
            recovered_stale_key: str | None = None
            if self._running:
                raise BackupError("当前已有备份任务正在执行", code="backup_busy")
            settings = config.get_backup_settings()
            target_fingerprint = _backup_target_fingerprint(settings)
            with canonical_path_write_lock(config_module.BACKUP_STATE_FILE):
                current = load_backup_state()
                pending_raw = current.get("pending_object_key")
                if pending_raw and current.get("last_status") != "running":
                    try:
                        object_key = _validate_backup_key(pending_raw, settings)
                    except BackupError as exc:
                        raise BackupError(
                            "上一次备份状态无效，已停止重试",
                            code="backup_state_invalid",
                        ) from exc
                    if object_key in self._deleting_object_keys:
                        raise BackupError(
                            "当前备份对象正在删除，请稍后再试",
                            code="backup_busy",
                        )
                    if current.get("pending_target_fingerprint") != target_fingerprint:
                        raise BackupError(
                            "上一次备份目标配置已变化，已停止重试",
                            code="backup_state_invalid",
                        )
                    pending_is_encrypted = object_key.endswith(".tar.gz.enc")
                    if pending_is_encrypted != bool(settings.get("encrypt")):
                        raise BackupError(
                            "上一次备份的加密设置与当前配置不一致，已停止重试",
                            code="backup_state_invalid",
                        )
                    started_at = str(current.get("last_started_at") or _iso_now())
                else:
                    if pending_raw and current.get("last_status") == "running":
                        try:
                            recovered_stale_key = _validate_backup_key(pending_raw, settings)
                        except BackupError:
                            recovered_stale_key = None
                        if recovered_stale_key and current.get("pending_target_fingerprint") == target_fingerprint:
                            self._recovered_stale_object_keys.add(recovered_stale_key)
                    object_key = _new_backup_object_key(settings)
                    started_at = _iso_now()
                if object_key in self._deleting_object_keys:
                    raise BackupError(
                        "当前备份对象正在删除，请稍后再试",
                        code="backup_busy",
                    )
                self._running = True
                self._running_object_key = object_key
                self._running_settings = dict(settings)
                try:
                    save_backup_state({
                        "last_started_at": started_at,
                        "last_finished_at": current.get("last_finished_at"),
                        "last_status": "running",
                        "last_error": None,
                        "last_object_key": current.get("last_object_key"),
                        "pending_object_key": object_key,
                        "pending_target_fingerprint": target_fingerprint,
                    })
                except BaseException:
                    self._running = False
                    self._running_object_key = None
                    self._running_settings = None
                    raise
        try:
            result = self._run_backup_once(trigger=trigger, object_key=object_key)
            state_saved = self._save_state_if_owner(object_key, {
                "last_started_at": started_at,
                "last_finished_at": _iso_now(),
                "last_status": "success",
                "last_error": None,
                "last_object_key": result["key"],
                "pending_object_key": None,
                "pending_target_fingerprint": None,
            })
            if state_saved and recovered_stale_key:
                with self._lock:
                    self._recovered_stale_object_keys.discard(recovered_stale_key)
            return result
        except Exception as exc:
            error_fields = {
                "last_error_code": getattr(exc, "code", "backup_failed") if isinstance(exc, BackupError) else "backup_failed",
                "last_error_status": getattr(exc, "status_code", None) if isinstance(exc, BackupError) else None,
            }
            try:
                self._save_state_if_owner(object_key, {
                    "last_started_at": started_at,
                    "last_finished_at": _iso_now(),
                    "last_status": "error",
                    **error_fields,
                    "last_object_key": current.get("last_object_key"),
                    "pending_object_key": object_key,
                    "pending_target_fingerprint": target_fingerprint,
                })
            except Exception:
                # State persistence is diagnostic bookkeeping.  It must not
                # replace the original backup failure or turn a controlled
                # BackupError into an unrelated storage exception.
                pass
            raise
        finally:
            with self._lock:
                self._running = False
                if self._running_object_key == object_key:
                    self._running_object_key = None
                    self._running_settings = None

    def _run_backup_once(
        self,
        *,
        trigger: str,
        object_key: str | None = None,
    ) -> dict[str, object]:
        with self._lock:
            settings = (
                dict(self._running_settings)
                if self._running_object_key == object_key and self._running_settings is not None
                else None
            )
        if settings is None:
            settings = config.get_backup_settings()
        client = CloudflareR2Client(settings)
        try:
            client.validate()
            payload_raw = self._build_backup_archive(settings, trigger=trigger)
            encrypted = bool(settings.get("encrypt"))
            if encrypted:
                passphrase = _clean(settings.get("passphrase"))
                if not passphrase:
                    raise BackupError(
                        "已启用备份加密，但未设置加密口令",
                        code="backup_encrypt_passphrase_missing",
                    )
                payload = _openssl_encrypt(payload_raw, passphrase)
                suffix = ".tar.gz.enc"
            else:
                payload = payload_raw
                suffix = ".tar.gz"
            if len(payload) > _MAX_R2_DOWNLOAD_BYTES:
                raise BackupError("备份归档过大", code="backup_archive_too_large")
            if object_key is None:
                object_key = _new_backup_object_key(settings)
            metadata = {
                "created-at": _iso_now(),
                "encrypted": "true" if encrypted else "false",
                "trigger": trigger,
            }
            result = client.upload_bytes(object_key, payload, content_type="application/octet-stream", metadata=metadata)
            self._apply_rotation(
                client,
                int(settings.get("rotation_keep") or 0),
                settings=settings,
                current_object_key=object_key,
            )
            return {
                "key": result["key"],
                "size": len(payload),
                "encrypted": encrypted,
            }
        finally:
            client.close()

    def _decode_backup_payload(self, key: str, payload: bytes) -> dict[str, object]:
        decoded = payload
        if key.endswith(".enc"):
            passphrase = _clean(config.get_backup_settings().get("passphrase"))
            if not passphrase:
                raise BackupError(
                    "当前未配置加密口令，无法查看已加密备份",
                    code="backup_detail_passphrase_missing",
                )
            decoded = _ensure_backup_payload_size(_openssl_decrypt(decoded, passphrase))
        return self._decode_archive_detail(decoded)

    def _apply_rotation(
        self,
        client: CloudflareR2Client,
        keep: int,
        *,
        settings: dict[str, object],
        current_object_key: str,
    ) -> None:
        if keep <= 0:
            return
        items = []
        for item in client.list_objects():
            try:
                _validate_backup_key(item.get("key"), settings)
            except BackupError:
                continue
            items.append(item)
        if len(items) <= keep:
            return
        protected_keys = {current_object_key}
        protected_keys.update(self._recovered_stale_object_keys)
        state = load_backup_state()
        for protected_field in ("pending_object_key", "last_object_key"):
            protected = _clean(state.get(protected_field))
            if protected:
                protected_keys.add(protected)
        eligible = [
            item for item in items
            if _clean(item.get("key")) not in protected_keys
        ]
        delete_from = max(0, keep - len(protected_keys))
        for item in eligible[delete_from:]:
            key = _clean(item.get("key"))
            if key:
                client.delete_object(key)

    def _decode_archive_detail(self, payload: bytes) -> dict[str, object]:
        files: list[dict[str, object]] = []
        snapshots: list[dict[str, object]] = []
        metadata: dict[str, object] = {}
        try:
            with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
                member_count = 0
                for member in archive:
                    member_count += 1
                    if member_count > _MAX_BACKUP_DETAIL_MEMBERS:
                        raise BackupError(
                            "解析备份压缩包失败，备份成员过多",
                            code="backup_archive_invalid",
                        )
                    if not member.isfile():
                        continue
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        continue
                    name = member.name
                    if name == "backup-metadata.json":
                        try:
                            raw = _read_archive_member_bounded(extracted)
                            parsed = json.loads(raw.decode("utf-8"))
                            if not isinstance(parsed, dict):
                                raise ValueError("backup metadata must be an object")
                            metadata = parsed
                        except BackupError:
                            raise
                        except Exception as exc:
                            raise BackupError(
                                "解析备份压缩包失败，备份可能已损坏",
                                code="backup_archive_invalid",
                            ) from exc
                        continue
                    public_name = _public_archive_member_name(name)
                    if public_name is None:
                        continue
                    if public_name.startswith("snapshots/") and public_name.endswith(".json"):
                        count = 0
                        try:
                            raw = _read_archive_member_bounded(extracted)
                            parsed_snapshot = json.loads(raw.decode("utf-8"))
                            if not isinstance(parsed_snapshot, list):
                                raise ValueError("backup snapshot must be a list")
                            count = _count_items(parsed_snapshot)
                        except BackupError:
                            raise
                        except Exception as exc:
                            raise BackupError(
                                "解析备份压缩包失败，备份可能已损坏",
                                code="backup_archive_invalid",
                            ) from exc
                        snapshots.append({
                            "name": public_name.removeprefix("snapshots/").removesuffix(".json"),
                            "count": count,
                        })
                        continue
                    size, sha256 = _hash_archive_member(extracted, int(member.size))
                    files.append({
                        "name": public_name,
                        "exists": True,
                        "content_type": _guess_content_type(name),
                        "size": size,
                        "sha256": sha256,
                    })
        except tarfile.TarError as exc:
            raise BackupError(
                "解析备份压缩包失败，备份可能已损坏",
                code="backup_archive_invalid",
            ) from exc
        snapshot_manifest = metadata.get("snapshot_manifest")
        account_cumulative_total: int | None = None
        if snapshot_manifest is not None:
            if not isinstance(snapshot_manifest, dict) or snapshot_manifest.get("version") != 1:
                raise BackupError(
                    "解析备份压缩包失败，快照 metadata 无效",
                    code="backup_archive_invalid",
                )
            accounts_manifest = snapshot_manifest.get("accounts")
            if not isinstance(accounts_manifest, dict):
                raise BackupError(
                    "解析备份压缩包失败，快照 metadata 无效",
                    code="backup_archive_invalid",
                )
            raw_total = accounts_manifest.get("cumulative_total")
            if type(raw_total) is not int or raw_total < 0:
                raise BackupError(
                    "解析备份压缩包失败，快照 metadata 无效",
                    code="backup_archive_invalid",
                )
            account_cumulative_total = raw_total
        if account_cumulative_total is not None:
            for snapshot in snapshots:
                if snapshot.get("name") == "accounts":
                    snapshot["cumulative_total"] = account_cumulative_total
        files.sort(key=lambda item: str(item.get("name") or ""))
        snapshots.sort(key=lambda item: str(item.get("name") or ""))
        return {
            "created_at": _public_backup_created_at(metadata.get("created_at")),
            "trigger": _public_backup_trigger(metadata.get("trigger")),
            "app_version": _public_backup_app_version(metadata.get("app_version")),
            "storage_backend": _public_backup_storage_backend(metadata.get("storage_backend")),
            "files": files,
            "snapshots": snapshots,
        }

    def _build_backup_archive(self, settings: dict[str, object], *, trigger: str) -> bytes:
        include = settings.get("include") if isinstance(settings.get("include"), dict) else {}
        metadata = {
            "version": 2,
            "created_at": _iso_now(),
            "trigger": trigger,
            "app_version": config.app_version,
            "storage_backend": config.get_storage_backend().get_backend_info(),
        }
        account_records: list[dict[str, object]] | None = None
        if include.get("accounts_snapshot"):
            storage = config.get_storage_backend()
            account_records = storage.load_accounts()
            load_cumulative_total = getattr(storage, "load_cumulative_total", None)
            cumulative_total = (
                load_cumulative_total() if callable(load_cumulative_total) else None
            )
            if cumulative_total is not None and (
                type(cumulative_total) is not int or cumulative_total < 0
            ):
                raise BackupError(
                    "账号快照累计值无效",
                    code="backup_snapshot_invalid",
                )
            if cumulative_total is not None:
                metadata["snapshot_manifest"] = {
                    "version": 1,
                    "accounts": {"cumulative_total": cumulative_total},
                }
        buffer = _CappedBytesIO(_MAX_R2_DOWNLOAD_BYTES)
        with tarfile.open(fileobj=buffer, mode="w:gz") as archive:
            self._add_bytes_to_archive(archive, "backup-metadata.json", _json_bytes(metadata))
            if include.get("config"):
                self._add_file_to_archive(archive, CONFIG_FILE, "config.json")
            if include.get("cpa"):
                self._add_file_to_archive(archive, DATA_DIR / "cpa_config.json", "data/cpa_config.json")
            if include.get("sub2api"):
                self._add_file_to_archive(archive, DATA_DIR / "sub2api_config.json", "data/sub2api_config.json")
            if include.get("ccload"):
                self._add_file_to_archive(archive, DATA_DIR / "ccload_config.json", "data/ccload_config.json")
            if include.get("logs"):
                self._add_file_to_archive(archive, DATA_DIR / "logs.jsonl", "data/logs.jsonl")
            if include.get("image_tasks"):
                self._add_file_to_archive(archive, DATA_DIR / "image_tasks.json", "data/image_tasks.json")
                self._add_file_to_archive(archive, IMAGE_INDEX_FILE, "data/image_index.json")
            if account_records is not None:
                self._add_bytes_to_archive(
                    archive,
                    "snapshots/accounts.json",
                    _json_bytes(account_records),
                )
            if include.get("auth_keys_snapshot"):
                self._add_bytes_to_archive(
                    archive,
                    "snapshots/auth_keys.json",
                    _json_bytes(config.get_storage_backend().load_auth_keys()),
                )
            if include.get("images"):
                self._add_file_to_archive(archive, TAGS_FILE, "data/image_tags.json")
                self._add_directory_to_archive(archive, config.images_dir, "data/images")
        return buffer.getvalue()

    def _add_bytes_to_archive(self, archive: tarfile.TarFile, name: str, payload: bytes) -> None:
        info = tarfile.TarInfo(name=name)
        info.size = len(payload)
        info.mtime = int(_utc_now().timestamp())
        archive.addfile(info, io.BytesIO(payload))

    def _add_file_to_archive(self, archive: tarfile.TarFile, source: Path, arcname: str) -> None:
        source = Path(source)
        try:
            root = authorized_root(source.parent)
            path = resolve_under_root(root, source.name)
            opened = open_checked_file(path, root, root)
        except (OSError, ValueError):
            return
        try:
            info = tarfile.TarInfo(name=arcname)
            info.size = opened.stat_result.st_size
            info.mtime = int(opened.stat_result.st_mtime)
            archive.addfile(info, opened.file)
        finally:
            opened.file.close()

    def _add_directory_to_archive(self, archive: tarfile.TarFile, source_dir: Path, arcname_root: str) -> None:
        try:
            root = authorized_root(Path(source_dir))
        except OSError:
            return
        if not root.exists() or not root.is_dir():
            return
        for candidate in sorted(root.rglob("*")):
            try:
                relative = candidate.relative_to(root).as_posix()
                path = resolve_under_root(root, relative)
                opened = open_checked_file(path, root, root)
            except (OSError, ValueError):
                continue
            try:
                info = tarfile.TarInfo(name=f"{arcname_root}/{relative}")
                info.size = opened.stat_result.st_size
                info.mtime = int(opened.stat_result.st_mtime)
                archive.addfile(info, opened.file)
            finally:
                opened.file.close()


backup_service = BackupService()
