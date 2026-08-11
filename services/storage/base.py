from __future__ import annotations

from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from threading import RLock
from typing import Any, cast


STORAGE_HEALTH_ERROR_MESSAGE = "存储后端健康检查失败"
STORAGE_DATA_ERROR_MESSAGE = "存储快照无效"
STORAGE_CONFLICT_ERROR_MESSAGE = "存储快照已被其他请求更新"

_STORAGE_MUTATION_LOCKS_GUARD = RLock()
_STORAGE_MUTATION_LOCKS: dict[tuple[str, str], RLock] = {}


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

    def save_accounts_if_revision(
        self,
        expected: StorageSnapshot,
        accounts: list[dict[str, Any]],
    ) -> StorageSnapshot:
        with self._mutation_lock("accounts"):
            current = self.load_accounts_snapshot()
            if current.revision != expected.revision:
                raise StorageConflictError()
            self.save_accounts(accounts)
            return make_storage_snapshot(accounts)

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
        with self._mutation_lock("auth_keys"):
            current = self.load_auth_keys_snapshot()
            if current.revision != expected.revision:
                raise StorageConflictError()
            self.save_auth_keys(auth_keys)
            return make_storage_snapshot(auth_keys)

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
