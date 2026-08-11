from __future__ import annotations

import json
from pathlib import Path
from threading import RLock

from services.config import DATA_DIR
from services.storage.base import StorageConflictError, StorageDataError

TAGS_FILE = DATA_DIR / "image_tags.json"
_TAGS_LOCK = RLock()


def _atomic_write_locked(payload: bytes) -> None:
    TAGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    temp_path = TAGS_FILE.with_suffix(TAGS_FILE.suffix + ".tmp")
    try:
        temp_path.write_bytes(payload)
        temp_path.replace(TAGS_FILE)
    except Exception:
        try:
            temp_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise


def _read_locked() -> tuple[bytes, dict[str, list[str]]]:
    TAGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not TAGS_FILE.exists():
        _atomic_write_locked(b"{}\n")
    try:
        raw = TAGS_FILE.read_bytes()
        value = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise StorageDataError() from exc
    return raw, _validate_data(value)


def _validate_data(value: object) -> dict[str, list[str]]:
    if not isinstance(value, dict):
        raise StorageDataError()
    data: dict[str, list[str]] = {}
    for image_rel, tags in value.items():
        if not isinstance(image_rel, str) or not isinstance(tags, list) or any(not isinstance(tag, str) for tag in tags):
            raise StorageDataError()
        data[image_rel] = list(tags)
    return data


def _save_locked(data: dict[str, list[str]], expected_raw: bytes) -> None:
    validated = _validate_data(data)
    current_raw, _ = _read_locked()
    if current_raw != expected_raw:
        raise StorageConflictError()
    payload = (json.dumps(validated, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
    _atomic_write_locked(payload)


def load_tags() -> dict[str, list[str]]:
    with _TAGS_LOCK:
        _, data = _read_locked()
        return data


def save_tags(data: dict[str, list[str]]) -> None:
    with _TAGS_LOCK:
        raw, _ = _read_locked()
        _save_locked(data, raw)


def get_tags(image_rel: str) -> list[str]:
    with _TAGS_LOCK:
        _, data = _read_locked()
        return list(data.get(image_rel, []))


def set_tags(image_rel: str, tags: list[str]) -> list[str]:
    with _TAGS_LOCK:
        raw, data = _read_locked()
        cleaned = list(dict.fromkeys(t.strip() for t in tags if isinstance(t, str) and t.strip()))
        if cleaned:
            data[image_rel] = cleaned
        else:
            data.pop(image_rel, None)
        _save_locked(data, raw)
        return cleaned


def remove_tags(image_rel: str) -> None:
    with _TAGS_LOCK:
        raw, data = _read_locked()
        if data.pop(image_rel, None) is not None:
            _save_locked(data, raw)


def delete_tag(tag: str) -> int:
    """从所有图片中删除指定标签，返回受影响的图片数。"""
    with _TAGS_LOCK:
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
