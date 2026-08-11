from __future__ import annotations

import hashlib
import io
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock
from urllib.parse import quote, urlparse

from curl_cffi import requests
from fastapi import HTTPException
from PIL import Image

from services.config import DATA_DIR, config, parse_public_url
from services.protocol.error_response import PublicSafeErrorMarker, public_exception_message
from services.secure_file import (
    OpenedFile,
    atomic_write_bytes,
    authorized_root,
    delete_checked_file,
    open_checked_file,
    resolve_under_root,
)
from services.storage.base import StorageDataError
from services.url_utils import redact_url_credentials

IMAGE_INDEX_FILE = DATA_DIR / "image_index.json"
IMAGE_INDEX_LOCK = Lock()
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}


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
        data = json.loads(opened.file.read().decode("utf-8"))
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

    def _request(self, method: str, url: str, **kwargs):
        response = self.session.request(method, url, timeout=30, **self._auth_kwargs(), **kwargs)
        if response.status_code >= 400 and not (method == "MKCOL" and response.status_code in {405}):
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
            response = self.session.request("MKCOL", current, timeout=30, **self._auth_kwargs())
            if response.status_code in {201, 405}:
                continue
            if response.status_code >= 400:
                raise PublicImageStorageError(f"WebDAV MKCOL failed: HTTP {response.status_code}")

    def put(self, rel: str, payload: bytes, content_type: str = "image/png") -> str:
        self.ensure_dirs(rel)
        url = self.remote_url(rel)
        self._request("PUT", url, data=payload, headers={"Content-Type": content_type})
        return url

    def get(self, rel: str) -> bytes:
        response = self._request("GET", self.remote_url(rel))
        return bytes(response.content)

    def delete(self, rel: str) -> bool:
        response = self.session.request("DELETE", self.remote_url(rel), timeout=30, **self._auth_kwargs())
        if response.status_code in {200, 202, 204, 404}:
            return response.status_code != 404
        raise PublicImageStorageError(f"WebDAV DELETE failed: HTTP {response.status_code}")

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
        self._index_lock = IMAGE_INDEX_LOCK

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

    def save(self, image_data: bytes, base_url: str | None = None) -> StoredImage:
        config.cleanup_old_images()
        # Validate the existing index before creating a local/remote object. A
        # corrupt index must not be treated as empty after a successful upload.
        with self._index_lock:
            self._load_clean_index()
        dimensions, extension, content_type = _image_info(image_data)
        rel = self.make_relative_path(image_data, extension)
        mode = self.mode()
        if mode not in {"local", "webdav", "both"}:
            mode = "local"
        stored_local = False
        stored_webdav = False
        remote_url = ""

        if mode in {"local", "both"}:
            path = _local_image_path(rel)
            atomic_write_bytes(path, config.images_dir, image_data)
            stored_local = True

        if mode in {"webdav", "both"}:
            client = WebDAVClient(self.settings())
            try:
                remote_url = redact_url_credentials(client.put(rel, image_data, content_type=content_type))
            finally:
                client.close()
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
        with self._index_lock:
            items = self._load_clean_index()
            items[rel] = item
            self._save_index(items)
        return StoredImage(rel=rel, url=self._public_url(rel, base_url), storage=str(item["storage"]), size=len(image_data))

    def get_bytes(self, rel: str) -> bytes:
        safe_rel = _safe_relative_path(rel)
        if not _is_image_rel(safe_rel):
            raise HTTPException(status_code=404, detail="image not found")
        opened = self.open_local(safe_rel)
        if opened is not None:
            try:
                return opened.file.read()
            finally:
                opened.file.close()
        item = self._load_clean_index().get(safe_rel, {})
        if item.get("webdav"):
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
        return bool(item.get("webdav"))

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
        with self._index_lock:
            raw_indexed = self._load_index()
            indexed = self._clean_index_items(raw_indexed)
            root = config.images_dir
            try:
                root = authorized_root(root)
            except OSError:
                root = None
            changed = indexed != raw_indexed
            for path in root.rglob("*") if root is not None else ():
                if not _is_image_rel(path.name):
                    continue
                try:
                    rel = path.relative_to(root).as_posix()
                except ValueError:
                    continue
                if rel in indexed:
                    continue
                try:
                    opened = self.open_local(rel)
                except HTTPException:
                    continue
                if opened is None:
                    continue
                try:
                    payload = opened.file.read()
                    stat_result = opened.stat_result
                finally:
                    opened.file.close()
                dimensions = _image_dimensions(payload)
                indexed[rel] = {
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
                changed = True

            items: list[dict[str, object]] = []
            for rel, item in list(indexed.items()):
                if not _is_image_rel(rel):
                    indexed.pop(rel, None)
                    changed = True
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
                webdav = bool(item.get("webdav"))
                if not local and not webdav:
                    indexed.pop(rel, None)
                    changed = True
                    continue
                storage = "both" if local and webdav else ("webdav" if webdav else "local")
                if item.get("local") != local or item.get("storage") != storage:
                    item = {
                        **item,
                        "local": local,
                        "storage": storage,
                    }
                    indexed[rel] = item
                    changed = True
                day = str(item.get("date") or "")
                if start_date and day < start_date:
                    continue
                if end_date and day > end_date:
                    continue
                items.append({
                    **item,
                    "rel": rel,
                    "path": rel,
                    "remote_url": redact_url_credentials(item.get("remote_url")),
                    "url": self._public_url(rel, base_url),
                })
            if changed:
                self._save_index(indexed)
        items.sort(key=lambda item: str(item.get("created_at") or ""), reverse=True)
        return items

    def delete(self, rel: str) -> bool:
        safe_rel = _safe_relative_path(rel)
        removed = False
        path = _local_image_path(safe_rel)
        try:
            removed = delete_checked_file(path, config.images_dir)
        except FileNotFoundError:
            removed = False
        except OSError as exc:
            raise HTTPException(status_code=404, detail="image not found") from exc
        with self._index_lock:
            items = self._load_clean_index()
            item = items.get(safe_rel, {})
            if item.get("webdav"):
                client = WebDAVClient(self.settings())
                try:
                    removed = client.delete(safe_rel) or removed
                except ImageStorageError:
                    if not removed:
                        raise
                finally:
                    client.close()
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
        with self._index_lock:
            items = self._load_clean_index()
            client = WebDAVClient(settings)
            try:
                root = config.images_dir
                try:
                    root = authorized_root(root)
                except OSError:
                    root = None
                for path in sorted(root.rglob("*")) if root is not None else ():
                    if not _is_image_rel(path.name):
                        continue
                    try:
                        rel = path.relative_to(root).as_posix()
                    except ValueError:
                        continue
                    item = items.get(rel, {})
                    if item.get("webdav"):
                        skipped += 1
                        continue
                    try:
                        opened = self.open_local(rel)
                        if opened is None:
                            continue
                        try:
                            payload = opened.file.read()
                            stat_result = opened.stat_result
                        finally:
                            opened.file.close()
                        remote_url = redact_url_credentials(
                            client.put(rel, payload, content_type=_image_info(payload)[2])
                        )
                        dimensions = _image_dimensions(payload)
                        items[rel] = {
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
                        uploaded += 1
                    except Exception:
                        failed += 1
                self._save_index(items)
            finally:
                client.close()
        return {"uploaded": uploaded, "skipped": skipped, "failed": failed}

    def test_webdav(self) -> dict[str, object]:
        return WebDAVClient(self.settings()).test()


image_storage_service = ImageStorageService()
