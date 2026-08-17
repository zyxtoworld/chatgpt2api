from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from threading import RLock, local
from typing import Any, cast


STORAGE_HEALTH_ERROR_MESSAGE = "存储后端健康检查失败"
STORAGE_DATA_ERROR_MESSAGE = "存储快照无效"
STORAGE_CONFLICT_ERROR_MESSAGE = "存储快照已被其他请求更新"

_STORAGE_MUTATION_LOCKS_GUARD = RLock()
_STORAGE_MUTATION_LOCKS: dict[tuple[str, str], RLock] = {}
_PATH_WRITE_LOCKS_GUARD = RLock()
_PATH_WRITE_LOCKS: dict[str, "_CrossProcessPathWriteLock"] = {}


class _CrossProcessPathWriteLock:
    """Re-entrant process lock backed by an OS lock on a sidecar file."""

    def __init__(self, path: Path) -> None:
        canonical_path = Path(path).resolve(strict=False)
        self._lock_path = canonical_path.with_name(f".{canonical_path.name}.lock")
        self._process_lock = RLock()
        self._state = local()
        self._handle = None
        self._os_lock_state = None

    def __enter__(self) -> "_CrossProcessPathWriteLock":
        self._process_lock.acquire()
        depth = getattr(self._state, "depth", 0)
        if depth == 0:
            handle = None
            os_lock_state = None
            try:
                self._lock_path.parent.mkdir(parents=True, exist_ok=True)
                handle = open(self._lock_path, "a+b")
                if os.fstat(handle.fileno()).st_size == 0:
                    handle.seek(0)
                    handle.write(b"\0")
                    handle.flush()
                handle.seek(0)
                os_lock_state = self._acquire_os_lock(handle)
                self._handle = handle
                self._os_lock_state = os_lock_state
            except BaseException:
                try:
                    if handle is not None:
                        try:
                            if os_lock_state is not None:
                                self._release_os_lock(handle, os_lock_state)
                        finally:
                            handle.close()
                finally:
                    self._process_lock.release()
                raise
        self._state.depth = depth + 1
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        depth = getattr(self._state, "depth", 0)
        if depth <= 0:
            raise RuntimeError("path write lock released without acquisition")
        if depth == 1:
            handle = self._handle
            os_lock_state = self._os_lock_state
            self._handle = None
            self._os_lock_state = None
            try:
                if handle is not None:
                    try:
                        self._release_os_lock(handle, os_lock_state)
                    finally:
                        handle.close()
            finally:
                del self._state.depth
                self._process_lock.release()
        else:
            self._state.depth = depth - 1
            self._process_lock.release()

    @staticmethod
    def _acquire_os_lock(handle):
        if os.name == "nt":
            import ctypes
            import msvcrt
            from ctypes import wintypes

            class Overlapped(ctypes.Structure):
                _fields_ = [
                    ("Internal", ctypes.c_void_p),
                    ("InternalHigh", ctypes.c_void_p),
                    ("Offset", wintypes.DWORD),
                    ("OffsetHigh", wintypes.DWORD),
                    ("hEvent", ctypes.c_void_p),
                ]

            kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
            kernel32.LockFileEx.argtypes = [
                wintypes.HANDLE,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.POINTER(Overlapped),
            ]
            kernel32.LockFileEx.restype = wintypes.BOOL
            kernel32.UnlockFileEx.argtypes = [
                wintypes.HANDLE,
                wintypes.DWORD,
                wintypes.DWORD,
                wintypes.DWORD,
                ctypes.POINTER(Overlapped),
            ]
            kernel32.UnlockFileEx.restype = wintypes.BOOL
            overlapped = Overlapped()
            file_handle = wintypes.HANDLE(msvcrt.get_osfhandle(handle.fileno()))
            if not kernel32.LockFileEx(file_handle, 0x00000002, 0, 1, 0, ctypes.byref(overlapped)):
                raise ctypes.WinError(ctypes.get_last_error())
            return kernel32, file_handle, overlapped, Overlapped
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        return None

    @staticmethod
    def _release_os_lock(handle, os_lock_state) -> None:
        if os.name == "nt":
            if os_lock_state is None:
                return
            kernel32, file_handle, overlapped, _ = os_lock_state
            if not kernel32.UnlockFileEx(file_handle, 0, 1, 0, overlapped):
                import ctypes

                raise ctypes.WinError(ctypes.get_last_error())
            return
        import fcntl

        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def canonical_path_write_lock(path: Path) -> _CrossProcessPathWriteLock:
    """Return a re-entrant lock shared by processes for one canonical path."""

    key = str(Path(path).resolve(strict=False)).casefold()
    with _PATH_WRITE_LOCKS_GUARD:
        lock = _PATH_WRITE_LOCKS.get(key)
        if lock is None:
            lock = _CrossProcessPathWriteLock(Path(path))
            _PATH_WRITE_LOCKS[key] = lock
        return lock


def canonical_scoped_path_write_lock(path: Path, scope: str) -> _CrossProcessPathWriteLock:
    """Return a cross-process lock for one stable scope below a storage path."""

    digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()
    canonical_path = Path(path).resolve(strict=False)
    scoped_path = canonical_path.with_name(f".{canonical_path.name}.{digest}.scope")
    return canonical_path_write_lock(scoped_path)


class StorageDataError(ValueError):
    """Persisted storage data is malformed and must not be treated as empty."""

    def __init__(self) -> None:
        super().__init__(STORAGE_DATA_ERROR_MESSAGE)


class StorageConflictError(ValueError):
    """The persisted snapshot changed since the caller loaded it."""

    def __init__(self) -> None:
        super().__init__(STORAGE_CONFLICT_ERROR_MESSAGE)


@dataclass(frozen=True)
class StorageSnapshot:
    """A validated persisted collection plus its content revision."""

    records: list[dict[str, Any]]
    revision: str


def validate_storage_records(value: object) -> list[dict[str, Any]]:
    """Validate one persisted snapshot without silently dropping records."""

    if not isinstance(value, list) or any(not isinstance(item, dict) for item in value):
        raise StorageDataError()
    try:
        # Every backend persists JSON (the database stores one JSON document per
        # row).  Reject non-finite numbers here instead of letting json.dump/
        # json.dumps emit the non-standard NaN/Infinity literals.
        json.dumps(value, ensure_ascii=False, allow_nan=False)
    except (TypeError, ValueError, OverflowError) as exc:
        raise StorageDataError() from exc
    return cast(list[dict[str, Any]], value)


def make_storage_snapshot(value: object) -> StorageSnapshot:
    records = validate_storage_records(value)
    try:
        encoded = json.dumps(
            records,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as exc:
        raise StorageDataError() from exc
    return StorageSnapshot(
        records=deepcopy(records),
        revision=hashlib.sha256(encoded).hexdigest(),
    )


class StorageBackend(ABC):
    """抽象存储后端基类"""

    # Backends that persist the historical account counter in the same
    # snapshot as accounts opt into the capability explicitly.  Structural
    # test/integration backends keep the legacy sidecar fallback.
    supports_cumulative_snapshot = False

    def close(self) -> None:
        """Release backend-owned resources after all accepted work drains."""
        return None

    def _mutation_identity(self) -> str:
        return f"{type(self).__module__}.{type(self).__qualname__}:{id(self)}"

    def _mutation_lock(self, scope: str) -> RLock:
        key = (self._mutation_identity(), scope)
        with _STORAGE_MUTATION_LOCKS_GUARD:
            lock = _STORAGE_MUTATION_LOCKS.get(key)
            if lock is None:
                lock = RLock()
                _STORAGE_MUTATION_LOCKS[key] = lock
            return lock

    def save_accounts_if_unchanged(
        self,
        expected_accounts: list[dict[str, Any]],
        accounts: list[dict[str, Any]],
    ) -> None:
        self.save_accounts_if_revision(make_storage_snapshot(expected_accounts), accounts)

    def load_accounts_snapshot(self) -> StorageSnapshot:
        return make_storage_snapshot(self.load_accounts())

    def load_cumulative_total(self) -> int | None:
        """Return the durable historical account total, if this backend owns it."""
        return None

    def save_accounts_with_cumulative_total(
        self,
        expected: StorageSnapshot,
        accounts: list[dict[str, Any]],
        cumulative_total: int,
    ) -> StorageSnapshot:
        raise NotImplementedError

    def save_accounts_if_revision(
        self,
        expected: StorageSnapshot,
        accounts: list[dict[str, Any]],
    ) -> StorageSnapshot:
        next_snapshot = make_storage_snapshot(accounts)
        with self._mutation_lock("accounts"):
            current = self.load_accounts_snapshot()
            if current.revision != expected.revision:
                raise StorageConflictError()
            self.save_accounts(next_snapshot.records)
            return next_snapshot

    def save_auth_keys_if_unchanged(
        self,
        expected_auth_keys: list[dict[str, Any]],
        auth_keys: list[dict[str, Any]],
    ) -> None:
        self.save_auth_keys_if_revision(make_storage_snapshot(expected_auth_keys), auth_keys)

    def load_auth_keys_snapshot(self) -> StorageSnapshot:
        return make_storage_snapshot(self.load_auth_keys())

    def save_auth_keys_if_revision(
        self,
        expected: StorageSnapshot,
        auth_keys: list[dict[str, Any]],
    ) -> StorageSnapshot:
        next_snapshot = make_storage_snapshot(auth_keys)
        with self._mutation_lock("auth_keys"):
            current = self.load_auth_keys_snapshot()
            if current.revision != expected.revision:
                raise StorageConflictError()
            self.save_auth_keys(next_snapshot.records)
            return next_snapshot

    @abstractmethod
    def load_accounts(self) -> list[dict[str, Any]]:
        """加载所有账号数据"""
        pass

    @abstractmethod
    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存所有账号数据"""
        pass

    @abstractmethod
    def load_auth_keys(self) -> list[dict[str, Any]]:
        """加载所有鉴权密钥数据"""
        pass

    @abstractmethod
    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存所有鉴权密钥数据"""
        pass

    @abstractmethod
    def health_check(self) -> dict[str, Any]:
        """健康检查，返回存储后端状态"""
        pass

    @abstractmethod
    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        pass
