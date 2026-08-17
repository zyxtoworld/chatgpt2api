from __future__ import annotations

import json
from typing import Any

from sqlalchemy import Column, String, Text, create_engine, Integer, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from services.storage.base import (
    STORAGE_HEALTH_ERROR_MESSAGE,
    StorageConflictError,
    StorageBackend,
    StorageDataError,
    StorageSnapshot,
    make_storage_snapshot,
    validate_storage_records,
)
from services.url_utils import redact_url_credentials

Base = declarative_base()


class AccountModel(Base):
    """账号数据模型"""
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    access_token = Column(String(2048), unique=True, nullable=False, index=True)
    data = Column(Text, nullable=False)  # JSON 格式存储完整账号数据


class AuthKeyModel(Base):
    """鉴权密钥数据模型"""
    __tablename__ = "auth_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    key_id = Column(String(255), unique=True, nullable=False, index=True)
    data = Column(Text, nullable=False)


class StorageMutationLockModel(Base):
    """One database row per collection used to serialize cross-process CAS."""

    __tablename__ = "storage_mutation_locks"

    scope = Column(String(64), primary_key=True)


class StorageMetadataModel(Base):
    """Small durable metadata values that share the owning storage contract."""

    __tablename__ = "storage_metadata"

    key = Column(String(64), primary_key=True)
    int_value = Column(Integer, nullable=False)


class DatabaseStorageBackend(StorageBackend):
    """数据库存储后端（支持 SQLite、PostgreSQL、MySQL 等）"""

    supports_cumulative_snapshot = True

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.engine = create_engine(
            database_url,
            pool_pre_ping=True,  # 自动检测连接是否有效
            pool_recycle=3600,   # 1小时回收连接
        )
        try:
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)
            self._ensure_mutation_lock_rows()
        except BaseException:
            try:
                self.engine.dispose()
            except BaseException:
                pass
            raise

    def close(self) -> None:
        """Dispose pooled DB connections after application work has drained."""
        self.engine.dispose()

    def _mutation_identity(self) -> str:
        return f"database:{self.database_url}"

    def load_accounts(self) -> list[dict[str, Any]]:
        """从数据库加载账号数据"""
        return self._load_rows(AccountModel, "access_token", "access_token")

    def save_accounts(self, accounts: list[dict[str, Any]]) -> None:
        """保存账号数据到数据库"""
        self._save_rows(AccountModel, accounts, "access_token", scope="accounts")

    def load_cumulative_total(self) -> int | None:
        session = self.Session()
        try:
            row = session.get(StorageMetadataModel, "cumulative_total")
            if row is None:
                return None
            value = row.int_value
            if type(value) is not int or value < 0:
                raise StorageDataError()
            return value
        finally:
            session.close()

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
        session = self.Session()
        try:
            self._begin_mutation(session, "accounts")
            current = make_storage_snapshot(
                self._load_rows_in_session(session, AccountModel, "access_token", "access_token")
            )
            if current.revision != expected.revision:
                raise StorageConflictError()
            self._save_rows_in_session(
                session, AccountModel, records, "access_token", "access_token"
            )
            session.merge(
                StorageMetadataModel(key="cumulative_total", int_value=cumulative_total)
            )
            session.commit()
            return next_snapshot
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def load_auth_keys(self) -> list[dict[str, Any]]:
        """从数据库加载鉴权密钥数据"""
        return self._load_rows(AuthKeyModel, "id", "key_id")

    def save_auth_keys(self, auth_keys: list[dict[str, Any]]) -> None:
        """保存鉴权密钥数据到数据库"""
        self._save_rows(AuthKeyModel, auth_keys, "id", "key_id", scope="auth_keys")

    def save_accounts_if_revision(
        self,
        expected: StorageSnapshot,
        accounts: list[dict[str, Any]],
    ) -> StorageSnapshot:
        return self._save_if_revision(
            AccountModel,
            "access_token",
            "access_token",
            "accounts",
            expected,
            accounts,
        )

    def save_auth_keys_if_revision(
        self,
        expected: StorageSnapshot,
        auth_keys: list[dict[str, Any]],
    ) -> StorageSnapshot:
        return self._save_if_revision(
            AuthKeyModel,
            "id",
            "key_id",
            "auth_keys",
            expected,
            auth_keys,
        )

    def _ensure_mutation_lock_rows(self) -> None:
        session = self.Session()
        try:
            for scope in ("accounts", "auth_keys"):
                session.merge(StorageMutationLockModel(scope=scope))
            session.commit()
        except IntegrityError:
            session.rollback()
            if any(
                session.get(StorageMutationLockModel, scope) is None
                for scope in ("accounts", "auth_keys")
            ):
                raise
        finally:
            session.close()

    def _begin_mutation(self, session, scope: str) -> None:
        """Start the write transaction and acquire its cross-process scope lock."""
        if self.engine.dialect.name == "sqlite":
            session.execute(text("BEGIN IMMEDIATE"))
        else:
            session.begin()
        session.query(StorageMutationLockModel).filter_by(scope=scope).with_for_update().one()

    def _save_if_revision(
        self,
        model: type[AccountModel] | type[AuthKeyModel],
        item_key: str,
        row_key: str,
        scope: str,
        expected: StorageSnapshot,
        items: list[dict[str, Any]],
    ) -> StorageSnapshot:
        next_snapshot = make_storage_snapshot(items)
        session = self.Session()
        try:
            self._begin_mutation(session, scope)
            current = make_storage_snapshot(self._load_rows_in_session(session, model, item_key, row_key))
            if current.revision != expected.revision:
                raise StorageConflictError()
            self._save_rows_in_session(session, model, items, item_key, row_key)
            session.commit()
            return next_snapshot
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _load_rows(
        self,
        model: type[AccountModel] | type[AuthKeyModel],
        item_key: str,
        row_key: str,
    ) -> list[dict[str, Any]]:
        session = self.Session()
        try:
            return self._load_rows_in_session(session, model, item_key, row_key)
        finally:
            session.close()

    @staticmethod
    def _load_rows_in_session(
        session,
        model: type[AccountModel] | type[AuthKeyModel],
        item_key: str,
        row_key: str,
    ) -> list[dict[str, Any]]:
        items = []
        for row in session.query(model).all():
            try:
                item_data = json.loads(row.data)
            except Exception as exc:
                raise StorageDataError() from exc
            if not isinstance(item_data, dict):
                raise StorageDataError()
            raw_key = item_data.get(item_key)
            if (
                not isinstance(raw_key, str)
                or not raw_key.strip()
                or raw_key.strip() != str(getattr(row, row_key))
            ):
                raise StorageDataError()
            items.append(item_data)
        return items

    def _save_rows(
        self,
        model: type[AccountModel] | type[AuthKeyModel],
        items: list[dict[str, Any]],
        source_key: str,
        target_key: str | None = None,
        *,
        scope: str,
    ) -> None:
        items = validate_storage_records(items)
        session = self.Session()
        try:
            self._begin_mutation(session, scope)
            self._save_rows_in_session(session, model, items, source_key, target_key)
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    @staticmethod
    def _save_rows_in_session(
        session,
        model: type[AccountModel] | type[AuthKeyModel],
        items: list[dict[str, Any]],
        source_key: str,
        target_key: str | None = None,
    ) -> None:
        key_column = target_key or source_key
        existing_rows = {
            str(getattr(row, key_column)): row
            for row in session.query(model).all()
        }
        incoming_keys: set[str] = set()

        for item in items:
            if not isinstance(item, dict):
                continue
            raw_key = item.get(source_key)
            if not isinstance(raw_key, str) or not raw_key.strip():
                raise StorageDataError()
            key_value = raw_key.strip()
            if key_value in incoming_keys:
                raise ValueError(f"Duplicate {source_key} in storage snapshot")

            incoming_keys.add(key_value)
            serialized_data = json.dumps(item, ensure_ascii=False)
            existing_row = existing_rows.get(key_value)
            if existing_row is None:
                session.add(
                    model(
                        **{key_column: key_value},
                        data=serialized_data,
                    )
                )
            elif existing_row.data != serialized_data:
                existing_row.data = serialized_data

        for key_value, row in existing_rows.items():
            if key_value not in incoming_keys:
                session.delete(row)

    def health_check(self) -> dict[str, Any]:
        """健康检查"""
        try:
            session = self.Session()
            try:
                session.execute(text("SELECT 1"))
            finally:
                session.close()
            accounts = self.load_accounts()
            auth_keys = self.load_auth_keys()
            return {
                "status": "healthy",
                "backend": "database",
                "database_url": self._mask_password(self.database_url),
                "account_count": len(accounts),
                "auth_key_count": len(auth_keys),
            }
        except Exception:
            return {
                "status": "unhealthy",
                "backend": "database",
                "error": STORAGE_HEALTH_ERROR_MESSAGE,
            }

    def get_backend_info(self) -> dict[str, Any]:
        """获取存储后端信息"""
        db_type = "unknown"
        if "sqlite" in self.database_url:
            db_type = "sqlite"
        elif "postgresql" in self.database_url or "postgres" in self.database_url:
            db_type = "postgresql"
        elif "mysql" in self.database_url:
            db_type = "mysql"
        
        return {
            "type": "database",
            "db_type": db_type,
            "description": f"数据库存储 ({db_type})",
            "database_url": self._mask_password(self.database_url),
        }

    @staticmethod
    def _mask_password(url: str) -> str:
        """隐藏数据库连接字符串中的完整 userinfo。"""
        return redact_url_credentials(url)
