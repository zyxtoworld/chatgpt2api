from __future__ import annotations

import io
import shutil
import threading
import time
import zipfile
from pathlib import Path

from fastapi import HTTPException
from fastapi.responses import Response
from PIL import Image, ImageOps

from services.config import config
from services.image_storage_service import image_storage_service
from services.image_tags_service import load_tags, remove_tags
from services.opened_file_response import OpenedFileResponse
from services.secure_file import (
    OpenedFile,
    atomic_write_bytes,
    authorized_root,
    delete_checked_file,
    open_checked_file,
    resolve_under_root,
)
from utils.log import logger

THUMBNAIL_SIZE = (320, 320)


def _safe_relative_path(path: str) -> str:
    value = str(path or "").strip().replace("\\", "/").lstrip("/")
    if not value:
        raise HTTPException(status_code=404, detail="image not found")
    parts = Path(value).parts
    if any(part in {"", ".", ".."} for part in parts):
        raise HTTPException(status_code=404, detail="image not found")
    return Path(*parts).as_posix()


def _opened_file_response(opened: OpenedFile, *, headers: dict[str, str], include_filename: bool) -> OpenedFileResponse:
    try:
        return OpenedFileResponse(opened, headers=headers, include_filename=include_filename)
    except Exception:
        opened.file.close()
        raise


def get_image_response(relative_path: str) -> OpenedFileResponse | Response:
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    }
    opened = image_storage_service.open_local(relative_path)
    if opened is not None:
        return _opened_file_response(opened, headers=headers, include_filename=False)
    return Response(content=image_storage_service.get_bytes(relative_path), media_type="image/png", headers=headers)


def _thumbnail_path(relative_path: str) -> Path:
    rel = _safe_relative_path(relative_path)
    try:
        return resolve_under_root(config.image_thumbnails_dir, f"{rel}.png")
    except OSError as exc:
        raise HTTPException(status_code=404, detail="image not found") from exc


def thumbnail_url(base_url: str, relative_path: str) -> str:
    return f"{base_url.rstrip('/')}/image-thumbnails/{_safe_relative_path(relative_path)}"


def ensure_thumbnail(relative_path: str) -> OpenedFile:
    target = _thumbnail_path(relative_path)
    source_opened = image_storage_service.open_local(relative_path)
    source_mtime = source_opened.stat_result.st_mtime if source_opened is not None else 0.0
    thumbnail_root = config.image_thumbnails_dir
    existing_target: OpenedFile | None = None
    try:
        try:
            existing_target = open_checked_file(target, thumbnail_root, thumbnail_root)
        except OSError:
            existing_target = None
        if existing_target is not None and (not source_mtime or existing_target.stat_result.st_mtime >= source_mtime):
            result = existing_target
            existing_target = None
            return result

        if existing_target is not None:
            existing_target.file.close()
            existing_target = None

        image_source = source_opened.file if source_opened is not None else io.BytesIO(image_storage_service.get_bytes(relative_path))
        with Image.open(image_source) as image:
            image = ImageOps.exif_transpose(image)
            if image.mode not in {"RGB", "RGBA"}:
                image = image.convert("RGBA" if "A" in image.getbands() else "RGB")
            image.thumbnail(THUMBNAIL_SIZE, Image.Resampling.LANCZOS)
            output = io.BytesIO()
            image.save(output, format="PNG", optimize=True)
            atomic_write_bytes(target, thumbnail_root, output.getvalue())
        return open_checked_file(target, thumbnail_root, thumbnail_root)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=422, detail="failed to create thumbnail") from exc
    finally:
        if existing_target is not None:
            existing_target.file.close()
        if source_opened is not None:
            source_opened.file.close()


def get_thumbnail_response(relative_path: str) -> OpenedFileResponse:
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    }
    opened = ensure_thumbnail(relative_path)
    return _opened_file_response(opened, headers=headers, include_filename=False)


def get_image_download_response(relative_path: str) -> OpenedFileResponse | Response:
    cors_headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, OPTIONS",
        "Access-Control-Allow-Headers": "*",
    }
    opened = image_storage_service.open_local(relative_path)
    if opened is not None:
        headers = {**cors_headers, "Content-Disposition": f'attachment; filename="{opened.filename}"'}
        return _opened_file_response(opened, headers=headers, include_filename=True)
    rel = _safe_relative_path(relative_path)
    headers = {
        **cors_headers,
        "Content-Disposition": f'attachment; filename="{Path(rel).name}"',
    }
    return Response(
        content=image_storage_service.get_bytes(rel),
        media_type="image/png",
        headers=headers,
    )


def cleanup_image_thumbnails() -> int:
    thumbnails_root = config.image_thumbnails_dir
    removed = 0
    for path in thumbnails_root.rglob("*"):
        try:
            rel = path.relative_to(thumbnails_root).as_posix()
        except ValueError:
            continue
        if not rel.endswith(".png") or not image_storage_service.exists(rel[:-4]):
            try:
                removed += int(delete_checked_file(resolve_under_root(thumbnails_root, rel), thumbnails_root))
            except OSError:
                pass
    return removed

def list_images(base_url: str, start_date: str = "", end_date: str = "") -> dict[str, object]:
    config.cleanup_old_images()
    cleanup_image_thumbnails()
    all_tags = load_tags()
    items = [
        {
            **item,
            "url": str(item.get("url") or f"{base_url.rstrip('/')}/images/{item['path']}"),
            "thumbnail_url": thumbnail_url(base_url, str(item["path"])),
            "tags": all_tags.get(str(item["path"]), []),
        }
        for item in image_storage_service.list_items(base_url, start_date, end_date)
    ]
    groups: dict[str, list[dict[str, object]]] = {}
    for item in items:
        groups.setdefault(str(item["date"]), []).append(item)
    return {"items": items, "groups": [{"date": key, "items": value} for key, value in groups.items()]}


def delete_images(paths: list[str] | None = None, start_date: str = "", end_date: str = "", all_matching: bool = False) -> dict[str, int]:
    root = config.images_dir
    targets = [
        str(item["path"])
        for item in image_storage_service.list_items("", start_date=start_date, end_date=end_date)
    ] if all_matching else (paths or [])
    removed = 0
    for item in targets:
        try:
            rel = _safe_relative_path(item)
            path = resolve_under_root(root, rel)
        except (HTTPException, OSError):
            continue
        if image_storage_service.delete(rel):
            removed += 1
        thumbnails_root = config.image_thumbnails_dir
        try:
            thumbnail = resolve_under_root(thumbnails_root, f"{rel}.png")
            delete_checked_file(thumbnail, thumbnails_root)
        except OSError:
            pass
        remove_tags(rel)
    return {"removed": removed}


def download_images_zip(paths: list[str]) -> io.BytesIO:
    buf = io.BytesIO()
    added = 0
    used_names: set[str] = set()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for item in paths:
            rel = _safe_relative_path(item)
            payload: bytes | None = None
            opened = None
            try:
                opened = image_storage_service.open_local(rel)
                if opened is None:
                    payload = image_storage_service.get_bytes(rel)
            except Exception:
                continue
            name = opened.filename if opened is not None else Path(rel).name
            if name in used_names:
                path = Path(name)
                stem = path.stem
                suffix = path.suffix
                counter = 2
                while f"{stem}_{counter}{suffix}" in used_names:
                    counter += 1
                name = f"{stem}_{counter}{suffix}"
            used_names.add(name)
            try:
                if opened is not None:
                    with zf.open(name, "w") as destination:
                        shutil.copyfileobj(opened.file, destination, length=1024 * 1024)
                else:
                    zf.writestr(name, payload)
                added += 1
            finally:
                if opened is not None:
                    opened.file.close()
    if added == 0:
        raise HTTPException(status_code=404, detail="no images found")
    buf.seek(0)
    return buf
def storage_stats() -> dict:
    import shutil
    try:
        root = authorized_root(config.images_dir)
    except OSError:
        return {
            "disk_total_mb": 0,
            "disk_used_mb": 0,
            "disk_free_mb": 0,
            "image_count": 0,
            "image_size_mb": 0,
            "image_size_bytes": 0,
        }
    usage = shutil.disk_usage(root)
    total_mb = usage.total // (1024 * 1024)
    used_mb = usage.used // (1024 * 1024)
    free_mb = usage.free // (1024 * 1024)

    image_count = 0
    image_size = 0
    for p in root.rglob("*"):
        try:
            rel = p.relative_to(root).as_posix()
            if Path(rel).suffix.lower() not in {".png", ".jpg", ".jpeg", ".webp"}:
                continue
            opened = image_storage_service.open_local(rel)
        except (HTTPException, OSError):
            continue
        if opened is None:
            continue
        try:
            image_count += 1
            image_size += opened.stat_result.st_size
        finally:
            opened.file.close()

    return {
        "disk_total_mb": total_mb,
        "disk_used_mb": used_mb,
        "disk_free_mb": free_mb,
        "image_count": image_count,
        "image_size_mb": image_size // (1024 * 1024),
        "image_size_bytes": image_size,
    }


def compress_images(quality: int = 60) -> dict:
    """重新压缩所有图片，返回节省的空间"""
    saved = 0
    count = 0
    try:
        root = authorized_root(config.images_dir)
    except OSError:
        return {"compressed": 0, "saved_bytes": 0, "saved_mb": 0}
    for candidate in sorted(root.rglob("*.png")):
        try:
            rel = candidate.relative_to(root).as_posix()
            path = resolve_under_root(root, rel)
            opened = open_checked_file(path, root, root)
        except OSError:
            continue
        payload = None
        try:
            orig = opened.stat_result.st_size
            with Image.open(opened.file) as img:
                img = ImageOps.exif_transpose(img)
                output = io.BytesIO()
                img.save(output, format="PNG", optimize=True)
                payload = output.getvalue()
        except Exception:
            payload = None
        finally:
            opened.file.close()
        if payload is not None and len(payload) < orig:
            try:
                atomic_write_bytes(path, root, payload)
                saved += orig - len(payload)
                count += 1
            except OSError:
                pass
    return {"compressed": count, "saved_bytes": saved, "saved_mb": saved // (1024 * 1024)}


def delete_to_target(target_free_mb: int, dry_run: bool = False) -> dict:
    """删除最旧的图片直到剩余空间达到 target_free_mb"""
    import shutil
    try:
        root = authorized_root(config.images_dir)
    except OSError:
        return {
            "removed": 0,
            "freed_mb": 0,
            "target_free_mb": 0,
            "current_free_mb": 0,
            "done": False,
            "dry_run": dry_run,
        }
    usage = shutil.disk_usage(root)
    current_free = usage.free // (1024 * 1024)
    if current_free >= target_free_mb and not dry_run:
        return {"removed": 0, "current_free_mb": current_free, "target_free_mb": target_free_mb, "done": True}

    files: list[tuple[str, Path, float, int]] = []
    for candidate in root.rglob("*.png"):
        try:
            rel = candidate.relative_to(root).as_posix()
            path = resolve_under_root(root, rel)
            opened = open_checked_file(path, root, root)
        except OSError:
            continue
        try:
            files.append((rel, path, opened.stat_result.st_mtime, opened.stat_result.st_size))
        finally:
            opened.file.close()
    files.sort(key=lambda item: item[2])
    removed = 0
    freed = 0
    for rel, path, _mtime, size in files:
        if current_free + freed // (1024 * 1024) >= target_free_mb:
            break
        if not dry_run:
            try:
                if not delete_checked_file(path, root):
                    continue
            except OSError:
                continue
            thumbnails_root = config.image_thumbnails_dir
            try:
                thumbnail = resolve_under_root(thumbnails_root, f"{rel}.png")
                delete_checked_file(thumbnail, thumbnails_root)
            except OSError:
                pass
            remove_tags(rel)
        freed += size
        removed += 1

    return {
        "removed": removed,
        "freed_mb": freed // (1024 * 1024),
        "target_free_mb": target_free_mb,
        "current_free_mb": current_free + (freed // (1024 * 1024)),
        "done": (current_free + freed // (1024 * 1024)) >= target_free_mb,
        "dry_run": dry_run,
    }


def _auto_cleanup_worker(stop_event: threading.Event) -> None:
    """后台线程：每30分钟检查存储，空间低于阈值自动清理最旧图片"""
    import shutil
    min_free_mb = getattr(config, "image_min_free_mb", None)
    if min_free_mb is None:
        min_free_mb = 500

    while not stop_event.wait(1800):  # 每30分钟
        try:
            config.cleanup_old_images()
            cleanup_image_thumbnails()
            usage = shutil.disk_usage(config.images_dir)
            free_mb = usage.free // (1024 * 1024)
            if free_mb < min_free_mb:
                logger.info({"event": "image_auto_cleanup", "free_mb": free_mb, "min_free_mb": min_free_mb})
                result = delete_to_target(min_free_mb)
                logger.info({"event": "image_auto_cleanup_done", **result})
        except Exception:
            pass


def start_image_cleanup_scheduler(stop_event: threading.Event) -> threading.Thread:
    t = threading.Thread(target=_auto_cleanup_worker, args=(stop_event,), daemon=True, name="image-cleanup")
    t.start()
    return t
