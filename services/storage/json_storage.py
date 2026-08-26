from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Iterator

from services.account_snapshot import (
    ACCOUNT_SNAPSHOT_MAX_BYTES,
    AccountSnapshotLimitError,
    validate_account_records,
    validate_account_snapshot_bytes,
)
from services.secure_file import atomic_write_bytes, open_checked_file
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
_AUTH_KEYS_MAX_BYTES = 1024 * 1024


@dataclass(frozen=True)
class _FileIdentity:
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int


@dataclass(frozen=True)
class _ValidatedFileState:
    valid: bool
    identity: _FileIdentity | None
    cumulative_total: int | None = None


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
        self._health_state_lock = RLock()
        self._validated_files: dict[Path, _ValidatedFileState] = {}

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
    def _state_key(file_path: Path) -> Path:
        return Path(os.path.abspath(os.fspath(file_path)))

    @staticmethod
    def _identity_from_stat(stat_result: os.stat_result) -> _FileIdentity:
        return _FileIdentity(
            device=int(stat_result.st_dev),
            inode=int(stat_result.st_ino),
            size=int(stat_result.st_size),
            modified_ns=int(getattr(stat_result, "st_mtime_ns", int(stat_result.st_mtime * 1e9))),
            changed_ns=int(getattr(stat_result, "st_ctime_ns", int(stat_result.st_ctime * 1e9))),
        )

    def _publish_valid(
        self,
        file_path: Path,
        identity: _FileIdentity | None,
        *,
        cumulative_total: int | None = None,
    ) -> None:
        with self._health_state_lock:
            validated_files = dict(self._validated_files)
            validated_files[self._state_key(file_path)] = _ValidatedFileState(
                valid=True,
                identity=identity,
                cumulative_total=cumulative_total,
            )
            self._validated_files = validated_files

    def _publish_invalid(self, file_path: Path) -> None:
        with self._health_state_lock:
            validated_files = dict(self._validated_files)
            validated_files[self._state_key(file_path)] = _ValidatedFileState(
                valid=False,
                identity=None,
            )
            self._validated_files = validated_files

    def _read_payload(
        self,
        file_path: Path,
        *,
        max_bytes: int,
    ) -> tuple[bytes, _FileIdentity] | None:
        try:
            opened = open_checked_file(file_path, file_path.parent, file_path.parent)
        except FileNotFoundError:
            if self._is_dangling_reparse_path(file_path):
                raise StorageDataError()
            return None
        try:
            identity = self._identity_from_stat(opened.stat_result)
            if identity.size > max_bytes:
                raise StorageDataError()
            payload = opened.file.read(max_bytes + 1)
            if len(payload) > max_bytes:
                raise StorageDataError()
            final_identity = self._identity_from_stat(os.fstat(opened.file.fileno()))
            if final_identity != identity:
                raise StorageDataError()
            return payload, identity
        finally:
            opened.file.close()

    def _current_identity(self, file_path: Path) -> _FileIdentity | None:
        try:
            opened = open_checked_file(file_path, file_path.parent, file_path.parent)
        except FileNotFoundError:
            if self._is_dangling_reparse_path(file_path):
                raise StorageDataError()
            return None
        try:
            return self._identity_from_stat(opened.stat_result)
        finally:
            opened.file.close()

    def _load_json_document(self, file_path: Path) -> tuple[list[dict[str, Any]], int | None]:
        try:
            opened = self._read_payload(file_path, max_bytes=ACCOUNT_SNAPSHOT_MAX_BYTES)
            if opened is None:
                self._publish_valid(file_path, None)
                return [], None
            payload, identity = opened
            validate_account_snapshot_bytes(payload)
            data = json.loads(payload.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError, AccountSnapshotLimitError) as exc:
            self._publish_invalid(file_path)
            raise StorageDataError() from exc
        cumulative_total = None
        if isinstance(data, dict):
            raw_total = data.get(_CUMULATIVE_TOTAL_FIELD)
            if type(raw_total) is not int or raw_total < 0:
                self._publish_invalid(file_path)
                raise StorageDataError()
            cumulative_total = raw_total
            data = data.get("items")
        try:
            records = validate_account_records(data)
        except AccountSnapshotLimitError as exc:
            self._publish_invalid(file_path)
            raise StorageDataError() from exc
        self._publish_valid(file_path, identity, cumulative_total=cumulative_total)
        return records, cumulative_total

    def _load_json_list(self, file_path: Path) -> list[dict[str, Any]]:
        records, _cumulative_total = self._load_json_document(file_path)
        return records

    def load_cumulative_total(self) -> int | None:
        with self._health_state_lock:
            state = self._validated_files.get(self._state_key(self.file_path))
            if state is not None and state.valid:
                try:
                    if self._current_identity(self.file_path) == state.identity:
                        return state.cumulative_total
                except (OSError, StorageDataError):
                    pass
        _records, cumulative_total = self._load_json_document(self.file_path)
        return cumulative_total

    def _save_json_value(
        self,
        file_path: Path,
        value: object,
        *,
        max_bytes: int,
        cumulative_total: int | None = None,
    ) -> None:
        try:
            try:
                file_path.parent.mkdir(parents=True, exist_ok=True)
                parent_stat = file_path.parent.stat()
            except OSError as exc:
                raise StorageDataError() from exc
            try:
                payload = (
                    json.dumps(
                        value,
                        ensure_ascii=False,
                        allow_nan=False,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8")
            except (TypeError, ValueError, OverflowError) as exc:
                raise StorageDataError() from exc
            try:
                if len(payload) > max_bytes:
                    raise StorageDataError()
                validate_account_snapshot_bytes(payload)
            except AccountSnapshotLimitError as exc:
                raise StorageDataError() from exc
            with self._health_state_lock:
                atomic_write_bytes(
                    file_path,
                    file_path.parent,
                    payload,
                    expected_root_identity=(parent_stat.st_dev, parent_stat.st_ino),
                )
                committed = self._read_payload(file_path, max_bytes=max_bytes)
                if committed is None or committed[0] != payload:
                    raise StorageDataError()
                self._publish_valid(
                    file_path,
                    committed[1],
                    cumulative_total=cumulative_total,
                )
        except BaseException:
            self._publish_invalid(file_path)
            raise

    def load_accounts(self) -> list[dict[str, Any]]:
        """从 JSON 文件加载账号数据"""
        return self._load_json_list(self.file_path)

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存账号数据到 JSON 文件"""
        try:
            records = validate_account_records(accounts)
        except AccountSnapshotLimitError as exc:
            raise StorageDataError() from exc
        with self._mutation_lock("accounts"):
            _current_records, cumulative_total = self._load_json_document(self.file_path)
            value: object = records
            if cumulative_total is not None:
                value = {"items": records, _CUMULATIVE_TOTAL_FIELD: cumulative_total}
            self._save_json_value(
                self.file_path,
                value,
                max_bytes=ACCOUNT_SNAPSHOT_MAX_BYTES,
                cumulative_total=cumulative_total,
            )

    def save_accounts_with_cumulative_total(
        self,
        expected: StorageSnapshot,
        accounts: list[dict[str, Any]],
        cumulative_total: int,
    ) -> StorageSnapshot:
        try:
            records = validate_account_records(accounts)
        except AccountSnapshotLimitError as exc:
            raise StorageDataError() from exc
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
                max_bytes=ACCOUNT_SNAPSHOT_MAX_BYTES,
                cumulative_total=cumulative_total,
            )
            return next_snapshot

    def load_auth_keys(self) -> list[dict[str, Any]]:
        """从 JSON 文件加载鉴权密钥数据"""
        try:
            opened = self._read_payload(self.auth_keys_path, max_bytes=_AUTH_KEYS_MAX_BYTES)
            if opened is None:
                self._publish_valid(self.auth_keys_path, None)
                return []
            payload, identity = opened
            validate_account_snapshot_bytes(payload)
            data = json.loads(payload.decode("utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError, AccountSnapshotLimitError) as exc:
            self._publish_invalid(self.auth_keys_path)
            raise StorageDataError() from exc
        if isinstance(data, dict):
            data = data.get("items")
        try:
            records = validate_storage_records(data)
        except StorageDataError:
            self._publish_invalid(self.auth_keys_path)
            raise
        self._publish_valid(self.auth_keys_path, identity)
        return records

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存鉴权密钥数据到 JSON 文件"""
        records = validate_storage_records(auth_keys)
        with self._mutation_lock("auth_keys"):
            self._save_json_value(
                self.auth_keys_path,
                {"items": records},
                max_bytes=_AUTH_KEYS_MAX_BYTES,
            )

    def _health_identity_matches(
        self,
        file_path: Path,
        validated_files: dict[Path, _ValidatedFileState],
    ) -> bool | None:
        state = validated_files.get(self._state_key(file_path))
        if state is None:
            return None
        if not state.valid:
            return False
        try:
            return self._current_identity(file_path) == state.identity
        except (OSError, StorageDataError):
            return False

    def health_check(self) -> dict[str, Any]:
        """健康检查"""
        try:
            validated_files = self._validated_files
            accounts_healthy = self._health_identity_matches(self.file_path, validated_files)
            auth_healthy = self._health_identity_matches(self.auth_keys_path, validated_files)
            healthy = accounts_healthy is True and auth_healthy is True
            needs_cold_validation = accounts_healthy is None or auth_healthy is None
            if not healthy and needs_cold_validation:
                acquired = self._health_state_lock.acquire(blocking=False)
                if not acquired:
                    raise StorageDataError()
                try:
                    validated_files = self._validated_files
                    accounts_healthy = self._health_identity_matches(
                        self.file_path,
                        validated_files,
                    )
                    if accounts_healthy is None:
                        self._load_json_document(self.file_path)
                    validated_files = self._validated_files
                    auth_healthy = self._health_identity_matches(
                        self.auth_keys_path,
                        validated_files,
                    )
                    if auth_healthy is None:
                        self.load_auth_keys()
                    validated_files = self._validated_files
                    accounts_healthy = self._health_identity_matches(
                        self.file_path,
                        validated_files,
                    )
                    auth_healthy = self._health_identity_matches(
                        self.auth_keys_path,
                        validated_files,
                    )
                    healthy = accounts_healthy is True and auth_healthy is True
                finally:
                    self._health_state_lock.release()
            if not healthy:
                raise StorageDataError()
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
