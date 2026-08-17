from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from threading import RLock

from services.config import DATA_DIR
from services.image_storage_service import image_storage_service
from services.secure_file import atomic_write_bytes, open_checked_file
from services.storage.base import StorageConflictError, StorageDataError, canonical_path_write_lock

TAGS_FILE = DATA_DIR / "image_tags.json"
_TAGS_PARENT_IDENTITIES: dict[str, tuple[int, int]] = {}
try:
    _data_dir_stat = os.stat(DATA_DIR, follow_symlinks=False)
    if stat.S_ISDIR(_data_dir_stat.st_mode):
        _TAGS_PARENT_IDENTITIES[os.path.normcase(str(Path(DATA_DIR).absolute()))] = (
            _data_dir_stat.st_dev,
            _data_dir_stat.st_ino,
        )
except OSError:
    pass
_TAGS_LOCK = RLock()
_MAX_TAG_FILE_BYTES = 8 * 1024 * 1024
_MAX_TAGGED_IMAGES = 10_000
_MAX_TAGS_PER_IMAGE = 64
_MAX_TAG_LENGTH = 256
_MAX_IMAGE_REL_LENGTH = 1024


def _tags_path_lock():
    return canonical_path_write_lock(Path(TAGS_FILE).absolute())


def _atomic_write_locked(payload: bytes) -> None:
    path = Path(TAGS_FILE).absolute()
    path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_bytes(
        path,
        path.parent,
        payload,
        expected_root_identity=_parent_identity(path),
    )


def _parent_identity(path: Path) -> tuple[int, int]:
    parent = Path(path).absolute().parent
    key = os.path.normcase(str(parent))
    identity = _TAGS_PARENT_IDENTITIES.get(key)
    if identity is not None:
        return identity
    try:
        parent_stat = os.stat(parent, follow_symlinks=False)
    except OSError as exc:
        raise StorageDataError() from exc
    if not stat.S_ISDIR(parent_stat.st_mode):
        raise StorageDataError()
    identity = (parent_stat.st_dev, parent_stat.st_ino)
    _TAGS_PARENT_IDENTITIES[key] = identity
    return identity


def _open_tags_file_locked(path: Path):
    opened = open_checked_file(path, path.parent, path.parent)
    try:
        parent_stat = os.stat(path.parent, follow_symlinks=False)
    except OSError:
        opened.file.close()
        raise
    if (parent_stat.st_dev, parent_stat.st_ino) != _parent_identity(path):
        opened.file.close()
        raise OSError("tags directory changed")
    return opened


def _read_locked() -> tuple[bytes, dict[str, list[str]]]:
    opened = None
    try:
        path = Path(TAGS_FILE).absolute()
        path.parent.mkdir(parents=True, exist_ok=True)
        _parent_identity(path)
        if not path.exists():
            with _tags_path_lock():
                if not path.exists():
                    _atomic_write_locked(b"{}\n")
        opened = _open_tags_file_locked(path)
        if opened.stat_result.st_size > _MAX_TAG_FILE_BYTES:
            raise StorageDataError()
        raw = opened.file.read()
        value = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise StorageDataError() from exc
    finally:
        if opened is not None:
            opened.file.close()
    return raw, _validate_data(value)


def _validate_data(value: object) -> dict[str, list[str]]:
    if not isinstance(value, dict) or len(value) > _MAX_TAGGED_IMAGES:
        raise StorageDataError()
    data: dict[str, list[str]] = {}
    for image_rel, tags in value.items():
        if (
            not isinstance(image_rel, str)
            or not image_rel.strip()
            or image_rel != image_rel.strip()
            or len(image_rel) > _MAX_IMAGE_REL_LENGTH
            or not isinstance(tags, list)
            or len(tags) > _MAX_TAGS_PER_IMAGE
            or any(
                not isinstance(tag, str)
                or not tag.strip()
                or tag != tag.strip()
                or len(tag) > _MAX_TAG_LENGTH
                for tag in tags
            )
        ):
            raise StorageDataError()
        data[image_rel] = list(tags)
    return data


def _save_locked(data: dict[str, list[str]], expected_raw: bytes) -> None:
    validated = _validate_data(data)
    current_raw, _ = _read_locked()
    if current_raw != expected_raw:
        raise StorageConflictError()
    payload = (json.dumps(validated, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    if len(payload) > _MAX_TAG_FILE_BYTES:
        raise StorageDataError()
    _atomic_write_locked(payload)


def load_tags() -> dict[str, list[str]]:
    with _TAGS_LOCK:
        _, data = _read_locked()
        return data


def save_tags(data: dict[str, list[str]]) -> None:
    with _TAGS_LOCK, _tags_path_lock():
        raw, _ = _read_locked()
        _save_locked(data, raw)


def get_tags(image_rel: str) -> list[str]:
    with image_storage_service.rel_lock(image_rel) as safe_rel:
        with _TAGS_LOCK:
            _, data = _read_locked()
            return list(data.get(safe_rel, []))


def set_tags(image_rel: str, tags: list[str]) -> list[str]:
    if (
        not isinstance(image_rel, str)
        or not image_rel.strip()
        or image_rel != image_rel.strip()
        or len(image_rel) > _MAX_IMAGE_REL_LENGTH
    ):
        raise ValueError("image path limit exceeded")
    if not isinstance(tags, list) or len(tags) > _MAX_TAGS_PER_IMAGE:
        raise ValueError("tag limit exceeded")
    with image_storage_service.rel_lock(image_rel) as safe_rel:
        with _TAGS_LOCK, _tags_path_lock():
            raw, data = _read_locked()
            cleaned = list(dict.fromkeys(t.strip() for t in tags if isinstance(t, str) and t.strip()))
            if len(cleaned) > _MAX_TAGS_PER_IMAGE or any(len(tag) > _MAX_TAG_LENGTH for tag in cleaned):
                raise ValueError("tag limit exceeded")
            if cleaned:
                data[safe_rel] = cleaned
            else:
                data.pop(safe_rel, None)
            _save_locked(data, raw)
            return cleaned


def remove_tags(image_rel: str) -> None:
    with image_storage_service.rel_lock(image_rel) as safe_rel:
        with _TAGS_LOCK, _tags_path_lock():
            raw, data = _read_locked()
            if data.pop(safe_rel, None) is not None:
                _save_locked(data, raw)


def delete_tag(tag: str) -> int:
    """从所有图片中删除指定标签，返回受影响的图片数。"""
    with _TAGS_LOCK, _tags_path_lock():
        raw, data = _read_locked()
        count = 0
        for rel in list(data):
            if tag in data[rel]:
                data[rel] = [t for t in data[rel] if t != tag]
                if not data[rel]:
                    del data[rel]
                count += 1
        if count > 0:
            _save_locked(data, raw)
        return count


def get_all_tags() -> list[str]:
    with _TAGS_LOCK:
        _, data = _read_locked()
        seen: set[str] = set()
        result: list[str] = []
        for tags in data.values():
            for tag in tags:
                if tag not in seen:
                    seen.add(tag)
                    result.append(tag)
        return result
