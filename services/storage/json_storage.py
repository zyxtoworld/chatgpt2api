from __future__ import annotations

import json
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from services.secure_file import atomic_write_bytes, read_checked_file_bytes
from services.storage.base import (
    STORAGE_HEALTH_ERROR_MESSAGE,
    StorageConflictError,
    StorageBackend,
    StorageDataError,
    StorageSnapshot,
    canonical_path_write_lock,
    make_storage_snapshot,
    validate_storage_records,
)

_CUMULATIVE_TOTAL_FIELD = "cumulative_total"

class JSONStorageBackend(StorageBackend):
    """本地 JSON 文件存储后端"""

    supports_cumulative_snapshot = True

    def __init__(self, file_path: Path, auth_keys_path: Path | None = None):
        self.file_path = file_path
        self.auth_keys_path = auth_keys_path or file_path.with_name("auth_keys.json")
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.auth_keys_path.parent.mkdir(parents=True, exist_ok=True)
        self._scope_path_locks = {
            "accounts": canonical_path_write_lock(self.file_path),
            "auth_keys": canonical_path_write_lock(self.auth_keys_path),
        }

    @contextmanager
    def _mutation_lock(self, scope: str) -> Iterator[None]:
        path_lock = self._scope_path_locks.get(scope)
        if path_lock is None:
            with super()._mutation_lock(scope):
                yield
            return
        with path_lock:
            with super()._mutation_lock(scope):
                yield

    def _mutation_identity(self) -> str:
        return f"json:{self.file_path.resolve()}:{self.auth_keys_path.resolve()}"

    @staticmethod
    def _is_dangling_reparse_path(file_path: Path) -> bool:
        """Distinguish a genuinely missing file from an unresolved link."""
        try:
            if file_path.is_symlink():
                return True
            is_junction = getattr(file_path, "is_junction", None)
            if callable(is_junction) and is_junction():
                return True
            return os.path.lexists(file_path)
        except OSError as exc:
            raise StorageDataError() from exc

    @staticmethod
    def _load_json_document(file_path: Path) -> tuple[list[dict[str, Any]], int | None]:
        if not file_path.exists():
            if JSONStorageBackend._is_dangling_reparse_path(file_path):
                raise StorageDataError()
            return [], None
        try:
            data = json.loads(read_checked_file_bytes(file_path, file_path.parent).decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StorageDataError() from exc
        cumulative_total = None
        if isinstance(data, dict):
            raw_total = data.get(_CUMULATIVE_TOTAL_FIELD)
            if type(raw_total) is not int or raw_total < 0:
                raise StorageDataError()
            cumulative_total = raw_total
            data = data.get("items")
        return validate_storage_records(data), cumulative_total

    @staticmethod
    def _load_json_list(file_path: Path) -> list[dict[str, Any]]:
        records, _cumulative_total = JSONStorageBackend._load_json_document(file_path)
        return records

    def load_cumulative_total(self) -> int | None:
        _records, cumulative_total = self._load_json_document(self.file_path)
        return cumulative_total

    @staticmethod
    def _save_json_value(file_path: Path, value: object) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            parent_stat = file_path.parent.stat()
        except OSError as exc:
            raise StorageDataError() from exc
        try:
            payload = (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode(
                "utf-8"
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise StorageDataError() from exc
        atomic_write_bytes(
            file_path,
            file_path.parent,
            payload,
            expected_root_identity=(parent_stat.st_dev, parent_stat.st_ino),
        )

    def load_accounts(self) -> list[dict[str, Any]]:
        """从 JSON 文件加载账号数据"""
        return self._load_json_list(self.file_path)

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存账号数据到 JSON 文件"""
        records = validate_storage_records(accounts)
        with self._mutation_lock("accounts"):
            _current_records, cumulative_total = self._load_json_document(self.file_path)
            value: object = records
            if cumulative_total is not None:
                value = {"items": records, _CUMULATIVE_TOTAL_FIELD: cumulative_total}
            self._save_json_value(self.file_path, value)

    def save_accounts_with_cumulative_total(
        self,
        expected: StorageSnapshot,
        accounts: list[dict[str, Any]],
        cumulative_total: int,
    ) -> StorageSnapshot:
        records = validate_storage_records(accounts)
        if type(cumulative_total) is not int or cumulative_total < 0:
            raise StorageDataError()
        next_snapshot = make_storage_snapshot(records)
        with self._mutation_lock("accounts"):
            current = self.load_accounts_snapshot()
            if current.revision != expected.revision:
                raise StorageConflictError()
            self._save_json_value(
                self.file_path,
                {"items": records, _CUMULATIVE_TOTAL_FIELD: cumulative_total},
            )
            return next_snapshot

    def load_auth_keys(self) -> list[dict[str, Any]]:
        """从 JSON 文件加载鉴权密钥数据"""
        if not self.auth_keys_path.exists():
            if self._is_dangling_reparse_path(self.auth_keys_path):
                raise StorageDataError()
            return []
        try:
            data = json.loads(
                read_checked_file_bytes(self.auth_keys_path, self.auth_keys_path.parent).decode("utf-8")
            )
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise StorageDataError() from exc
        if isinstance(data, dict):
            data = data.get("items")
        return validate_storage_records(data)

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存鉴权密钥数据到 JSON 文件"""
        records = validate_storage_records(auth_keys)
        with self._mutation_lock("auth_keys"):
            self._save_json_value(self.auth_keys_path, {"items": records})

    def health_check(self) -> dict[str, Any]:
        """健康检查"""
        try:
            self._load_json_list(self.file_path)
            self.load_auth_keys()
            return {
                "status": "healthy",
                "backend": "json",
                "file_exists": self.file_path.exists(),
                "file_path": str(self.file_path),
                "auth_keys_file_exists": self.auth_keys_path.exists(),
                "auth_keys_file_path": str(self.auth_keys_path),
            }
        except Exception:
            return {
                "status": "unhealthy",
                "backend": "json",
                "error": STORAGE_HEALTH_ERROR_MESSAGE,
            }

    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        return {
            "type": "json",
            "description": "本地 JSON 文件存储",
            "file_path": str(self.file_path),
            "file_exists": self.file_path.exists(),
            "auth_keys_file_path": str(self.auth_keys_path),
            "auth_keys_file_exists": self.auth_keys_path.exists(),
        }
