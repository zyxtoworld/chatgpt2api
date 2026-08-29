from __future__ import annotations

import hashlib
import io
import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urlparse

from curl_cffi import requests
from fastapi import HTTPException
from PIL import Image

from services.config import DATA_DIR, config, parse_public_url
from services.image_rel_lock import image_rel_lock as _image_rel_lock
from services.protocol.error_response import PublicSafeErrorMarker, public_exception_message
from services.secure_file import (
    OpenedFile,
    atomic_write_bytes,
    authorized_root,
    delete_checked_file,
    open_checked_file,
    resolve_under_root,
)
from services.storage.base import StorageDataError, canonical_path_write_lock
from services.url_utils import normalize_public_http_url, redact_url_credentials

IMAGE_INDEX_FILE = DATA_DIR / "image_index.json"
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
_MAX_WEBDAV_IMAGE_BYTES = 50 * 1024 * 1024
_MAX_LOCAL_IMAGE_BYTES = 50 * 1024 * 1024
_IMAGE_READ_CHUNK_BYTES = 1024 * 1024
_MAX_IMAGE_INDEX_BYTES = 16 * 1024 * 1024
_IMAGE_PUBLIC_STRING_FIELDS = ("name", "date", "created_at")
_IMAGE_PUBLIC_STORAGE_VALUES = frozenset({"local", "webdav", "both"})


def _public_remote_url(value: object) -> str:
    normalized = normalize_public_http_url(value)
    if not normalized:
        return ""
    parsed = urlparse(normalized)
    return normalized if not parsed.query and not parsed.fragment else ""


def _read_local_image_bytes(opened: OpenedFile) -> bytes:
    """Read a verified local image with the same budget as remote images."""
    declared_size = opened.stat_result.st_size
    if declared_size < 0 or declared_size > _MAX_LOCAL_IMAGE_BYTES:
        raise HTTPException(status_code=413, detail="image exceeds size limit")
    payload = bytearray()
    while True:
        chunk = opened.file.read(_IMAGE_READ_CHUNK_BYTES)
        if not isinstance(chunk, (bytes, bytearray, memoryview)):
            raise HTTPException(status_code=422, detail="image data is invalid")
        if not chunk:
            break
        payload.extend(chunk)
        if len(payload) > _MAX_LOCAL_IMAGE_BYTES:
            raise HTTPException(status_code=413, detail="image exceeds size limit")
    return bytes(payload)


class ImageStorageError(RuntimeError):
    """Internal image-storage failure; its message is never public by default."""

    pass


class PublicImageStorageError(ImageStorageError, PublicSafeErrorMarker):
    """Image-storage error with an explicitly reviewed public message."""

    def __init__(self, public_message: str) -> None:
        message = str(public_message or "").strip()
        if not message:
            raise ValueError("public_message is required")
        self._public_safe_message = message
        super().__init__(message)

    def public_safe_message(self) -> str:
        return self._public_safe_message


WEBDAV_TEST_ERROR_MESSAGE = "WebDAV 测试失败，请稍后重试"


@dataclass(frozen=True)
class StoredImage:
    rel: str
    url: str
    storage: str
    size: int


def _clean(value: object) -> str:
    return str(value or "").strip()


def _now_iso() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _safe_relative_path(path: str) -> str:
    value = str(path or "").strip().replace("\\", "/").lstrip("/")
    if not value:
        raise HTTPException(status_code=404, detail="image not found")
    parts = Path(value).parts
    if any(part in {"", ".", ".."} for part in parts):
        raise HTTPException(status_code=404, detail="image not found")
    return Path(*parts).as_posix()


def _image_info(payload: bytes) -> tuple[tuple[int, int] | None, str, str]:
    try:
        with Image.open(io.BytesIO(payload)) as image:
            image_format = str(image.format or "").upper()
            extension, content_type = {
                "JPEG": ("jpg", "image/jpeg"),
                "PNG": ("png", "image/png"),
                "WEBP": ("webp", "image/webp"),
            }.get(image_format, ("png", "image/png"))
            return image.size, extension, content_type
    except Exception:
        return None, "png", "image/png"


def _image_dimensions(payload: bytes) -> tuple[int, int] | None:
    return _image_info(payload)[0]


def _is_image_rel(path: str) -> bool:
    try:
        safe_rel = _safe_relative_path(path)
    except HTTPException:
        return False
    return Path(safe_rel).suffix.lower() in IMAGE_EXTENSIONS


def _public_image_item(item: dict[str, object], rel: str, public_url: str) -> dict[str, object]:
    """Project an index record without trusting unknown persisted fields."""
    projected: dict[str, object] = {
        "rel": rel,
        "path": rel,
        "url": public_url,
    }
    for field in _IMAGE_PUBLIC_STRING_FIELDS:
        value = item.get(field)
        if isinstance(value, str) and len(value) <= 256:
            projected[field] = value
    size = item.get("size")
    if type(size) is int and size >= 0:
        projected["size"] = size
    storage = item.get("storage")
    if isinstance(storage, str) and storage in _IMAGE_PUBLIC_STORAGE_VALUES:
        projected["storage"] = storage
    for field in ("local", "webdav"):
        value = item.get(field)
        if type(value) is bool:
            projected[field] = value
    for field in ("width", "height"):
        value = item.get(field)
        if type(value) is int and value >= 0:
            projected[field] = value
    remote_url = item.get("remote_url")
    safe_remote_url = _public_remote_url(remote_url)
    if safe_remote_url:
        projected["remote_url"] = safe_remote_url
    return projected


def _storage_flag(item: dict[str, object] | None, field: str) -> bool:
    """Treat only canonical JSON booleans as storage ownership flags."""
    return isinstance(item, dict) and item.get(field) is True


def _local_image_path(relative_path: str) -> Path:
    rel = _safe_relative_path(relative_path)
    try:
        return resolve_under_root(config.images_dir, rel)
    except OSError as exc:
        raise HTTPException(status_code=404, detail="image not found") from exc


def _read_json_object(path: Path) -> dict[str, object]:
    path = Path(path).absolute()
    root = authorized_root(path.parent)
    try:
        opened = open_checked_file(path, root, root)
    except FileNotFoundError:
        return {}
    except OSError as exc:
        raise StorageDataError() from exc
    try:
        if opened.stat_result.st_size < 0 or opened.stat_result.st_size > _MAX_IMAGE_INDEX_BYTES:
            raise StorageDataError()
        raw = opened.file.read(_MAX_IMAGE_INDEX_BYTES + 1)
        if not isinstance(raw, bytes) or len(raw) > _MAX_IMAGE_INDEX_BYTES:
            raise StorageDataError()
        data = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise StorageDataError() from exc
    finally:
        opened.file.close()
    if not isinstance(data, dict):
        raise StorageDataError()
    return data


def _write_json_object(path: Path, data: dict[str, object]) -> None:
    path = Path(path).absolute()
    root = authorized_root(path.parent)
    payload = (json.dumps(data, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    if len(payload) > _MAX_IMAGE_INDEX_BYTES:
        raise StorageDataError()
    atomic_write_bytes(path, root, payload)


class WebDAVClient:
    def __init__(self, settings: dict[str, object]):
        self.url = _clean(settings.get("webdav_url")).rstrip("/")
        self.username = _clean(settings.get("webdav_username"))
        self.password = _clean(settings.get("webdav_password"))
        self.root_path = _clean(settings.get("webdav_root_path")).strip("/")
        self.session = requests.Session()

    def _auth_kwargs(self) -> dict[str, object]:
        return {"auth": (self.username, self.password)} if self.username or self.password else {}

    @staticmethod
    def _close_response(response: object) -> None:
        close = getattr(response, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass

    def _request(self, method: str, url: str, **kwargs):
        kwargs.setdefault("stream", True)
        response = self.session.request(method, url, timeout=30, **self._auth_kwargs(), **kwargs)
        # A redirect is not evidence that the WebDAV operation reached the
        # configured object.  Treat every non-2xx response as a failed
        # operation; MKCOL's idempotent 405 is handled by ensure_dirs().
        if not 200 <= response.status_code < 300:
            self._close_response(response)
            raise PublicImageStorageError(f"WebDAV {method} failed: HTTP {response.status_code}")
        return response

    def remote_url(self, rel: str = "") -> str:
        parts = [part for part in [self.root_path, _safe_relative_path(rel) if rel else ""] if part]
        encoded = "/".join(quote(part, safe="") for item in parts for part in item.split("/") if part)
        return f"{self.url}/{encoded}" if encoded else self.url

    def ensure_dirs(self, rel: str) -> None:
        parts = [part for part in [self.root_path, Path(_safe_relative_path(rel)).parent.as_posix()] if part and part != "."]
        current = self.url
        for item in "/".join(parts).split("/"):
            if not item:
                continue
            current = f"{current}/{quote(item, safe='')}"
            response = self.session.request("MKCOL", current, timeout=30, stream=True, **self._auth_kwargs())
            try:
                if response.status_code not in {201, 405}:
                    raise PublicImageStorageError(f"WebDAV MKCOL failed: HTTP {response.status_code}")
            finally:
                self._close_response(response)

    def put(self, rel: str, payload: bytes, content_type: str = "image/png") -> str:
        self.ensure_dirs(rel)
        url = self.remote_url(rel)
        response = self._request("PUT", url, data=payload, headers={"Content-Type": content_type})
        try:
            return url
        finally:
            self._close_response(response)

    def get(self, rel: str) -> bytes:
        response = self._request("GET", self.remote_url(rel), stream=True)
        try:
            declared_length = response.headers.get("Content-Length") or response.headers.get("content-length")
            if declared_length is not None:
                if not isinstance(declared_length, str) or not declared_length.isascii() or not declared_length.isdecimal() or len(declared_length) > 20:
                    raise PublicImageStorageError("WebDAV image response is invalid")
                if int(declared_length) > _MAX_WEBDAV_IMAGE_BYTES:
                    raise PublicImageStorageError("WebDAV image response is too large")
            chunks: list[bytes] = []
            total = 0
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                if not isinstance(chunk, bytes):
                    raise PublicImageStorageError("WebDAV image response is invalid")
                total += len(chunk)
                if total > _MAX_WEBDAV_IMAGE_BYTES:
                    raise PublicImageStorageError("WebDAV image response is too large")
                chunks.append(chunk)
            return b"".join(chunks)
        finally:
            self._close_response(response)

    def delete(self, rel: str) -> bool:
        response = self.session.request(
            "DELETE",
            self.remote_url(rel),
            timeout=30,
            stream=True,
            **self._auth_kwargs(),
        )
        try:
            if response.status_code in {200, 202, 204, 404}:
                return response.status_code != 404
            raise PublicImageStorageError(f"WebDAV DELETE failed: HTTP {response.status_code}")
        finally:
            self._close_response(response)

    def remote_exists(self, rel: str) -> bool:
        """Return remote ownership state without trusting the local index."""
        response = self.session.request(
            "HEAD",
            self.remote_url(rel),
            timeout=30,
            stream=True,
            **self._auth_kwargs(),
        )
        try:
            if 200 <= response.status_code < 300:
                return True
            if response.status_code == 404:
                return False
            raise PublicImageStorageError("WebDAV 对象存在性检查失败")
        finally:
            self._close_response(response)

    def test(self) -> dict[str, object]:
        try:
            if not self.url:
                return {"ok": False, "status": 0, "error": "WebDAV URL is required"}
            if urlparse(self.url).scheme not in {"http", "https"}:
                return {"ok": False, "status": 0, "error": "invalid WebDAV URL"}
            test_rel = ".chatgpt2api_webdav_test.txt"
            self.put(test_rel, b"chatgpt2api webdav test\n", content_type="text/plain")
            self.delete(test_rel)
            return {"ok": True, "status": 200, "error": None}
        except Exception as exc:
            return {
                "ok": False,
                "status": 0,
                "error": public_exception_message(exc, WEBDAV_TEST_ERROR_MESSAGE),
            }
        finally:
            self.session.close()


class ImageStorageService:
    def __init__(self, index_file: Path = IMAGE_INDEX_FILE):
        self.index_file = index_file
        self._index_path_lock = canonical_path_write_lock(self.index_file.absolute())

    @contextmanager
    def _index_guard(self):
        """Serialize one index read-modify-write across processes and threads."""
        with self._index_path_lock:
            yield

    def settings(self) -> dict[str, object]:
        return config.get_image_storage_settings()

    def mode(self) -> str:
        return _clean(self.settings().get("mode")) or "local"

    def _load_index(self) -> dict[str, dict[str, object]]:
        raw = _read_json_object(self.index_file)
        items = raw.get("items")
        if not isinstance(items, dict):
            if items is None:
                return {}
            raise StorageDataError()
        normalized: dict[str, dict[str, object]] = {}
        for key, value in items.items():
            if not isinstance(key, str) or not isinstance(value, dict):
                raise StorageDataError()
            normalized[key] = value
        return normalized

    def _load_clean_index(self) -> dict[str, dict[str, object]]:
        return self._clean_index_items(self._load_index())

    def _rollback_index_state(self, rel: str) -> tuple[bool, dict[str, object] | None]:
        """Read current ownership evidence without trusting a stale snapshot."""
        try:
            with self._index_guard():
                return True, self._load_clean_index().get(rel)
        except Exception:
            return False, None

    @staticmethod
    def _clean_index_items(items: dict[str, dict[str, object]]) -> dict[str, dict[str, object]]:
        cleaned: dict[str, dict[str, object]] = {}
        for rel, item in items.items():
            if not _is_image_rel(rel):
                continue
            normalized = dict(item)
            if "remote_url" in normalized:
                normalized["remote_url"] = redact_url_credentials(normalized.get("remote_url"))
            cleaned[rel] = normalized
        return cleaned

    def _save_index(self, items: dict[str, dict[str, object]]) -> None:
        sanitized_items: dict[str, dict[str, object]] = {}
        for rel, item in items.items():
            sanitized = dict(item)
            if "remote_url" in sanitized:
                sanitized["remote_url"] = redact_url_credentials(sanitized.get("remote_url"))
            sanitized_items[rel] = sanitized
        _write_json_object(self.index_file, {"items": sanitized_items})

    def _public_url(self, rel: str, base_url: str | None = None) -> str:
        settings = self.settings()
        public_base_url = parse_public_url(settings.get("public_base_url"))
        if public_base_url:
            return f"{public_base_url.rstrip('/')}/{_safe_relative_path(rel)}"
        safe_rel = _safe_relative_path(rel)
        explicit_base_url = parse_public_url(base_url)
        if explicit_base_url:
            return f"{explicit_base_url.rstrip('/')}/images/{safe_rel}"
        return f"/images/{safe_rel}"

    def make_relative_path(self, image_data: bytes, extension: str | None = None) -> str:
        file_hash = hashlib.md5(image_data).hexdigest()
        detected_extension = extension or _image_info(image_data)[1]
        filename = f"{int(time.time())}_{file_hash}.{detected_extension}"
        relative_dir = Path(time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"))
        return f"{relative_dir.as_posix()}/{filename}"

    @contextmanager
    def rel_lock(self, rel: str):
        safe_rel = _safe_relative_path(rel)
        if not _is_image_rel(safe_rel):
            raise HTTPException(status_code=404, detail="image not found")
        with _image_rel_lock(self.index_file, safe_rel):
            yield safe_rel

    def save(self, image_data: bytes, base_url: str | None = None) -> StoredImage:
        dimensions, extension, content_type = _image_info(image_data)
        rel = self.make_relative_path(image_data, extension)
        # Validate before cleanup and before taking this rel lock. Cleanup
        # acquires rel locks for candidates; running it while holding the
        # current rel lock lets concurrent saves deadlock on two rels.
        with self._index_guard():
            self._load_clean_index()
        config.cleanup_old_images()
        with _image_rel_lock(self.index_file, rel):
            return self._save_locked(
                image_data,
                base_url,
                dimensions,
                content_type,
                rel,
            )

    def _save_locked(
        self,
        image_data: bytes,
        base_url: str | None,
        dimensions: tuple[int, int] | None,
        content_type: str,
        rel: str,
    ) -> StoredImage:
        # Validate the existing index before creating a local/remote object. A
        # corrupt index must not be treated as empty after a successful upload.
        with self._index_guard():
            initial_index = self._load_clean_index()
        mode = self.mode()
        if mode not in {"local", "webdav", "both"}:
            mode = "local"
        stored_local = False
        stored_webdav = False
        remote_url = ""
        local_path: Path | None = None
        local_was_present = False
        remote_was_present = _storage_flag(initial_index.get(rel), "webdav")
        client: WebDAVClient | None = None

        try:
            if mode in {"local", "both"}:
                local_path = _local_image_path(rel)
                existing = self.open_local(rel)
                if existing is not None:
                    local_was_present = True
                    existing.file.close()
                atomic_write_bytes(local_path, config.images_dir, image_data)
                stored_local = True

            if mode in {"webdav", "both"}:
                client = WebDAVClient(self.settings())
                # The local index is not authoritative for remote ownership:
                # a prior upload may exist after an index write failure. Probe
                # the remote object immediately before PUT so rollback cannot
                # delete an object this save did not create.
                remote_was_present = client.remote_exists(rel)
                remote_url = redact_url_credentials(client.put(rel, image_data, content_type=content_type))
                stored_webdav = True

            item = {
                "rel": rel,
                "path": rel,
                "name": Path(rel).name,
                "date": "-".join(rel.split("/")[:3]),
                "size": len(image_data),
                "created_at": _now_iso(),
                "storage": "both" if stored_local and stored_webdav else ("webdav" if stored_webdav else "local"),
                "local": stored_local,
                "webdav": stored_webdav,
                "remote_url": redact_url_credentials(remote_url),
            }
            if dimensions:
                item["width"], item["height"] = dimensions
            with self._index_guard():
                items = self._load_clean_index()
                items[rel] = item
                self._save_index(items)
            return StoredImage(rel=rel, url=self._public_url(rel, base_url), storage=str(item["storage"]), size=len(image_data))
        except Exception:
            index_readable, current_item = self._rollback_index_state(rel)
            current_local = _storage_flag(current_item, "local")
            current_webdav = _storage_flag(current_item, "webdav")
            if stored_webdav and not remote_was_present and index_readable and not current_webdav and client is not None:
                try:
                    client.delete(rel)
                except Exception:
                    pass
            if stored_local and not local_was_present and index_readable and not current_local and local_path is not None:
                try:
                    delete_checked_file(local_path, config.images_dir)
                except OSError:
                    pass
            raise
        finally:
            if client is not None:
                client.close()

    def get_bytes(self, rel: str) -> bytes:
        safe_rel = _safe_relative_path(rel)
        if not _is_image_rel(safe_rel):
            raise HTTPException(status_code=404, detail="image not found")
        opened = self.open_local(safe_rel)
        if opened is not None:
            try:
                return _read_local_image_bytes(opened)
            finally:
                opened.file.close()
        item = self._load_clean_index().get(safe_rel, {})
        if _storage_flag(item, "webdav"):
            client = WebDAVClient(self.settings())
            try:
                return client.get(safe_rel)
            finally:
                client.close()
        raise HTTPException(status_code=404, detail="image not found")

    def exists(self, rel: str) -> bool:
        safe_rel = _safe_relative_path(rel)
        if not _is_image_rel(safe_rel):
            return False
        opened = self.open_local(safe_rel)
        if opened is not None:
            opened.file.close()
            return True
        item = self._load_clean_index().get(safe_rel, {})
        return _storage_flag(item, "webdav")

    def has_local(self, rel: str) -> bool:
        safe_rel = _safe_relative_path(rel)
        if not _is_image_rel(safe_rel):
            return False
        opened = self.open_local(safe_rel)
        if opened is None:
            return False
        opened.file.close()
        return True

    def open_local(self, rel: str) -> OpenedFile | None:
        safe_rel = _safe_relative_path(rel)
        if not _is_image_rel(safe_rel):
            return None
        path = _local_image_path(safe_rel)
        try:
            return open_checked_file(path, config.images_dir, config.images_dir)
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise HTTPException(status_code=404, detail="image not found") from exc

    def list_items(self, base_url: str, start_date: str = "", end_date: str = "") -> list[dict[str, object]]:
        with self._index_guard():
            raw_indexed = self._load_index()
            indexed = self._clean_index_items(raw_indexed)
            if indexed != raw_indexed:
                self._save_index(indexed)

        root = config.images_dir
        try:
            root = authorized_root(root)
        except OSError:
            root = None

        for path in root.rglob("*") if root is not None else ():
            if not _is_image_rel(path.name):
                continue
            try:
                rel = path.relative_to(root).as_posix()
            except ValueError:
                continue
            with _image_rel_lock(self.index_file, rel):
                with self._index_guard():
                    current = self._load_clean_index()
                if rel in current:
                    indexed[rel] = current[rel]
                    continue
                try:
                    opened = self.open_local(rel)
                except HTTPException:
                    continue
                if opened is None:
                    continue
                try:
                    payload = _read_local_image_bytes(opened)
                    stat_result = opened.stat_result
                except HTTPException:
                    continue
                finally:
                    opened.file.close()
                dimensions = _image_dimensions(payload)
                item = {
                    "rel": rel,
                    "path": rel,
                    "name": Path(rel).name,
                    "date": "-".join(rel.split("/")[:3]) if len(rel.split("/")) >= 4 else datetime.fromtimestamp(stat_result.st_mtime).strftime("%Y-%m-%d"),
                    "size": len(payload),
                    "created_at": datetime.fromtimestamp(stat_result.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                    "storage": "local",
                    "local": True,
                    "webdav": False,
                    **({"width": dimensions[0], "height": dimensions[1]} if dimensions else {}),
                }
                with self._index_guard():
                    current = self._load_clean_index()
                    if rel not in current:
                        current[rel] = item
                        self._save_index(current)
                    indexed = current

        items: list[dict[str, object]] = []
        for rel in list(indexed):
            if not _is_image_rel(rel):
                continue
            with _image_rel_lock(self.index_file, rel):
                with self._index_guard():
                    current = self._load_clean_index()
                item = current.get(rel)
                if item is None:
                    continue
                try:
                    opened = self.open_local(rel)
                except HTTPException:
                    opened = None
                    local = False
                else:
                    local = opened is not None
                    if opened is not None:
                        opened.file.close()
                webdav = _storage_flag(item, "webdav")
                if not local and not webdav:
                    with self._index_guard():
                        latest = self._load_clean_index()
                        if latest.get(rel) == item:
                            latest.pop(rel, None)
                            self._save_index(latest)
                    continue
                storage = "both" if local and webdav else ("webdav" if webdav else "local")
                if _storage_flag(item, "local") != local or item.get("storage") != storage:
                    updated = {**item, "local": local, "storage": storage}
                    with self._index_guard():
                        latest = self._load_clean_index()
                        if latest.get(rel) == item:
                            latest[rel] = updated
                            self._save_index(latest)
                            item = updated
                        else:
                            item = latest.get(rel)
                    if item is None:
                        continue
                day = str(item.get("date") or "")
                if start_date and day < start_date:
                    continue
                if end_date and day > end_date:
                    continue
                items.append(_public_image_item(item, rel, self._public_url(rel, base_url)))
        items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return items

    def delete(self, rel: str) -> bool:
        safe_rel = _safe_relative_path(rel)
        if not _is_image_rel(safe_rel):
            raise HTTPException(status_code=404, detail="image not found")
        with _image_rel_lock(self.index_file, safe_rel):
            return self._delete_locked(safe_rel)

    def mark_local_deleted(self, rel: str) -> None:
        """Remove only the local side while preserving any WebDAV object."""
        safe_rel = _safe_relative_path(rel)
        if not _is_image_rel(safe_rel):
            raise HTTPException(status_code=404, detail="image not found")
        with _image_rel_lock(self.index_file, safe_rel):
            with self._index_guard():
                items = self._load_clean_index()
                item = items.get(safe_rel)
                if item is None:
                    return
                if _storage_flag(item, "webdav"):
                    items[safe_rel] = {
                        **item,
                        "local": False,
                        "webdav": True,
                        "storage": "webdav",
                    }
                else:
                    items.pop(safe_rel, None)
                self._save_index(items)

    def _delete_locked(self, safe_rel: str) -> bool:
        removed = False
        path = _local_image_path(safe_rel)
        try:
            removed = delete_checked_file(path, config.images_dir)
        except FileNotFoundError:
            removed = False
        except OSError as exc:
            raise HTTPException(status_code=404, detail="image not found") from exc
        with self._index_guard():
            items = self._load_clean_index()
            item = items.get(safe_rel, {})
        if _storage_flag(item, "webdav"):
            client = WebDAVClient(self.settings())
            try:
                removed = client.delete(safe_rel) or removed
            except ImageStorageError:
                if removed:
                    # The local side is already gone, but the remote side is
                    # still owned by this index entry. Preserve that ownership
                    # so a later retry can delete the remote object.
                    try:
                        with self._index_guard():
                            current_items = self._load_clean_index()
                            current_item = current_items.get(safe_rel)
                            if current_item is not None:
                                current_items[safe_rel] = {
                                    **current_item,
                                    "local": False,
                                    "webdav": True,
                                    "storage": "webdav",
                                }
                                self._save_index(current_items)
                    except Exception:
                        # Keep the original remote failure as the public
                        # operation result; an unreadable index is fail-closed
                        # and the existing entry remains retryable.
                        pass
                raise
            finally:
                client.close()
        with self._index_guard():
            items = self._load_clean_index()
            if safe_rel in items:
                items.pop(safe_rel, None)
                self._save_index(items)
        return removed

    def sync_all(self) -> dict[str, int]:
        settings = self.settings()
        if self.mode() not in {"webdav", "both"}:
            raise PublicImageStorageError("WebDAV 图片存储未启用")
        uploaded = 0
        skipped = 0
        failed = 0
        root = config.images_dir
        try:
            root = authorized_root(root)
        except OSError:
            root = None
        candidates: list[str] = []
        for path in sorted(root.rglob("*")) if root is not None else ():
            if not _is_image_rel(path.name):
                continue
            try:
                candidates.append(path.relative_to(root).as_posix())
            except ValueError:
                continue

        client = WebDAVClient(settings)
        try:
            for rel in candidates:
                with _image_rel_lock(self.index_file, rel):
                    remote_uploaded = False
                    try:
                        with self._index_guard():
                            items = self._load_clean_index()
                        item = items.get(rel, {})
                        if _storage_flag(item, "webdav"):
                            skipped += 1
                            continue
                        opened = self.open_local(rel)
                        if opened is None:
                            continue
                        try:
                            payload = _read_local_image_bytes(opened)
                            stat_result = opened.stat_result
                        finally:
                            opened.file.close()
                        remote_was_present = client.remote_exists(rel)
                        remote_url = redact_url_credentials(
                            client.put(rel, payload, content_type=_image_info(payload)[2])
                        )
                        remote_uploaded = True
                        dimensions = _image_dimensions(payload)
                        next_item = {
                            **item,
                            "rel": rel,
                            "path": rel,
                            "name": Path(rel).name,
                            "date": "-".join(rel.split("/")[:3]) if len(rel.split("/")) >= 4 else datetime.fromtimestamp(stat_result.st_mtime).strftime("%Y-%m-%d"),
                            "size": len(payload),
                            "created_at": str(item.get("created_at") or datetime.fromtimestamp(stat_result.st_mtime).strftime("%Y-%m-%d %H:%M:%S")),
                            "storage": "both",
                            "local": True,
                            "webdav": True,
                            "remote_url": redact_url_credentials(remote_url),
                            **({"width": dimensions[0], "height": dimensions[1]} if dimensions else {}),
                        }
                        with self._index_guard():
                            current_items = self._load_clean_index()
                            current_items[rel] = next_item
                            self._save_index(current_items)
                        uploaded += 1
                    except Exception:
                        if remote_uploaded and not remote_was_present:
                            try:
                                client.delete(rel)
                            except Exception as rollback_exc:
                                raise PublicImageStorageError(
                                    "WebDAV 同步回滚失败"
                                ) from rollback_exc
                        failed += 1
        finally:
            client.close()
        return {"uploaded": uploaded, "skipped": skipped, "failed": failed}

    def test_webdav(self) -> dict[str, object]:
        return WebDAVClient(self.settings()).test()


image_storage_service = ImageStorageService()
