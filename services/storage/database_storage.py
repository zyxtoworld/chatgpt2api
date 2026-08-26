from __future__ import annotations

import hashlib
import json
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from sqlalchemy import CHAR, Column, Index, String, Text, create_engine, Integer, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

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
from services.url_utils import redact_url_credentials

Base = declarative_base()

_SCHEMA_LOCK_NAMESPACE = b"chatgpt2api-storage-schema-v1"
_SCHEMA_LOCK_WAIT_SECONDS = 10.0
_SCHEMA_LOCK_POLL_SECONDS = 0.05
_MYSQL_SCHEMA_LOCK_PREFIX = "chatgpt2api-schema-v1-"


def _access_token_hash(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _schema_lock_digest(database_name: str) -> bytes:
    return hashlib.sha256(
        _SCHEMA_LOCK_NAMESPACE + b"\0" + database_name.encode("utf-8")
    ).digest()


def _postgres_schema_lock_key(database_name: str) -> int:
    return int.from_bytes(_schema_lock_digest(database_name)[:8], "big", signed=True)


def _mysql_schema_lock_name(database_name: str) -> str:
    digest = _schema_lock_digest(database_name).hex()
    available = 64 - len(_MYSQL_SCHEMA_LOCK_PREFIX)
    return _MYSQL_SCHEMA_LOCK_PREFIX + digest[:available]


class AccountModel(Base):
    """账号数据模型"""
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    # Access-token length is provider-controlled; the storage contract has no
    # 2048-character ceiling.  The fixed-size digest keeps uniqueness in the
    # database without indexing the raw secret.
    access_token = Column(Text, nullable=False)
    access_token_hash = Column(CHAR(64), nullable=False)
    data = Column(Text, nullable=False)  # JSON 格式存储完整账号数据

    __table_args__ = (
        Index("ux_accounts_access_token_hash", "access_token_hash", unique=True),
    )


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
        schema_lock = self._acquire_schema_lock()
        try:
            with schema_lock:
                Base.metadata.create_all(self.engine)
                self.Session = sessionmaker(bind=self.engine)
                self._ensure_account_token_schema()
                self._ensure_mutation_lock_rows()
        except BaseException:
            try:
                self.engine.dispose()
            except BaseException:
                pass
            raise

    @contextmanager
    def _acquire_schema_lock(self):
        """Serialize schema creation and migrations across service processes.

        SQLite has no server-side advisory lock, so a sidecar lock protects the
        database file.  PostgreSQL and MySQL keep their advisory lock on a
        dedicated connection for the entire DDL/migration window.
        """
        dialect = self.engine.dialect.name
        if dialect == "sqlite":
            database = self.engine.url.database
            if database and database != ":memory:" and not database.startswith("file:"):
                lock = canonical_path_write_lock(Path(database))
                with lock:
                    yield
                return
            yield
            return

        if dialect not in {"postgresql", "mysql"}:
            yield
            return

        connection = self.engine.connect()
        if dialect == "postgresql":
            transaction = None
            try:
                transaction = connection.begin()
                database_name = connection.execute(
                    text("SELECT current_database()")
                ).scalar()
                if not isinstance(database_name, str) or not database_name:
                    raise StorageDataError()
                lock_key = _postgres_schema_lock_key(database_name)
                connection.execute(text("SET LOCAL statement_timeout = 10000"))
                deadline = time.monotonic() + _SCHEMA_LOCK_WAIT_SECONDS
                while True:
                    acquired = connection.execute(
                        text("SELECT pg_try_advisory_xact_lock(:lock_key)"),
                        {"lock_key": lock_key},
                    ).scalar()
                    if acquired is True:
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise StorageDataError()
                    time.sleep(min(_SCHEMA_LOCK_POLL_SECONDS, remaining))
                yield
            except BaseException:
                if transaction is not None:
                    try:
                        transaction.rollback()
                    except BaseException as rollback_error:
                        connection.invalidate(rollback_error)
                raise
            else:
                try:
                    transaction.commit()
                except BaseException as commit_error:
                    connection.invalidate(commit_error)
                    raise
            finally:
                connection.close()
            return

        acquired = False
        body_failed = False
        release_error = None
        try:
            database_name = connection.execute(text("SELECT DATABASE()")).scalar()
            if not isinstance(database_name, str) or not database_name:
                raise StorageDataError()
            lock_name = _mysql_schema_lock_name(database_name)
            try:
                result = connection.execute(
                    text("SELECT GET_LOCK(:lock_name, 10)"),
                    {"lock_name": lock_name},
                ).scalar()
            except BaseException as acquire_error:
                connection.invalidate(acquire_error)
                raise StorageDataError() from None
            if result != 1:
                raise StorageDataError()
            acquired = True
            connection.detach()
            try:
                yield
            except BaseException:
                body_failed = True
                raise
        finally:
            if acquired:
                try:
                    released = connection.execute(
                        text("SELECT RELEASE_LOCK(:lock_name)"),
                        {"lock_name": lock_name},
                    ).scalar()
                    if released != 1:
                        release_error = StorageDataError()
                except BaseException as error:
                    release_error = error
                if release_error is not None:
                    connection.invalidate(release_error)
            connection.close()
            if release_error is not None and not body_failed:
                raise StorageDataError() from None

    def _ensure_account_token_schema(self) -> None:
        """Migrate legacy token columns and install the database identity index."""
        dialect = self.engine.dialect.name
        inspector = inspect(self.engine)
        columns = {column["name"]: column for column in inspector.get_columns("accounts")}
        token_column = columns.get("access_token")
        if token_column is None:
            raise StorageDataError()

        if dialect not in {"postgresql", "mysql", "sqlite"}:
            raise StorageDataError()

        indexes = inspector.get_indexes("accounts")
        raw_token_indexes = [
            index
            for index in indexes
            if index.get("unique") and index.get("column_names") == ["access_token"]
        ]
        unique_constraints = inspector.get_unique_constraints("accounts")
        raw_token_constraints = [
            constraint
            for constraint in unique_constraints
            if constraint.get("column_names") == ["access_token"]
        ]
        index_names = {
            index.get("name") for index in raw_token_indexes if index.get("name")
        }
        constraint_names = {
            constraint.get("name")
            for constraint in raw_token_constraints
            if constraint.get("name")
        }
        if dialect == "postgresql":
            raw_token_indexes = [
                index for index in raw_token_indexes
                if index.get("name") not in constraint_names
            ]
        elif dialect == "mysql":
            raw_token_constraints = [
                constraint for constraint in raw_token_constraints
                if constraint.get("name") not in index_names
            ]
        hash_column = columns.get("access_token_hash")
        hash_index_name = "ux_accounts_access_token_hash"
        hash_needs_not_null = hash_column is None or bool(hash_column.get("nullable"))
        hash_indexes = [
            index
            for index in indexes
            if index.get("unique")
            and index.get("column_names") == ["access_token_hash"]
        ]
        sqlite_needs_rebuild = dialect == "sqlite" and (
            hash_needs_not_null
            or bool(raw_token_constraints)
            or not isinstance(token_column.get("type"), Text)
        )

        with self.engine.begin() as connection:
            if hash_column is None:
                connection.execute(
                    text("ALTER TABLE accounts ADD COLUMN access_token_hash CHAR(64)")
                )

            rows = connection.execute(
                text("SELECT id, access_token, access_token_hash FROM accounts")
            ).mappings()
            updates: list[dict[str, Any]] = []
            seen_hashes: set[str] = set()
            for row in rows:
                raw_token = row.get("access_token")
                if not isinstance(raw_token, str) or not raw_token.strip():
                    raise StorageDataError()
                token = raw_token.strip()
                token_hash = _access_token_hash(token)
                if token_hash in seen_hashes:
                    raise StorageDataError()
                seen_hashes.add(token_hash)
                stored_hash = row.get("access_token_hash")
                if stored_hash not in (None, token_hash):
                    raise StorageDataError()
                if stored_hash != token_hash:
                    updates.append({"id": row["id"], "token_hash": token_hash})

            if updates:
                connection.execute(
                    text(
                        "UPDATE accounts "
                        "SET access_token_hash = :token_hash "
                        "WHERE id = :id"
                    ),
                    updates,
                )

            if sqlite_needs_rebuild:
                migration_table = "accounts__chatgpt2api_token_migration"
                connection.execute(text(f"DROP TABLE IF EXISTS {migration_table}"))
                connection.execute(
                    text(
                        f"CREATE TABLE {migration_table} ("
                        "id INTEGER PRIMARY KEY, "
                        "access_token TEXT NOT NULL, "
                        "access_token_hash CHAR(64) NOT NULL, "
                        "data TEXT NOT NULL"
                        ")"
                    )
                )
                connection.execute(
                    text(
                        f"INSERT INTO {migration_table} "
                        "(id, access_token, access_token_hash, data) "
                        "SELECT id, access_token, access_token_hash, data FROM accounts"
                    )
                )
                connection.execute(text("DROP TABLE accounts"))
                connection.execute(
                    text(f"ALTER TABLE {migration_table} RENAME TO accounts")
                )
                raw_token_indexes = []
                raw_token_constraints = []
                hash_indexes = []

            if not hash_indexes:
                connection.execute(
                    text(
                        f"CREATE UNIQUE INDEX {hash_index_name} "
                        "ON accounts (access_token_hash)"
                    )
                )

            if dialect == "postgresql" and hash_needs_not_null:
                connection.execute(
                    text(
                        "ALTER TABLE accounts "
                        "ALTER COLUMN access_token_hash SET NOT NULL"
                    )
                )
            elif dialect == "mysql" and hash_needs_not_null:
                connection.execute(
                    text(
                        "ALTER TABLE accounts MODIFY access_token_hash CHAR(64) NOT NULL"
                    )
                )

            # A recovered SQLite midstate may already have the digest column
            # and unique index while the legacy raw-token index remains. The
            # normal SQLite rebuild removes it, but this partial state does
            # not need a rebuild; drop the redundant index only after the
            # digest identity is verified and unique.
            if dialect == "sqlite" and raw_token_indexes:
                self._drop_raw_token_indexes(connection, raw_token_indexes, dialect)

            # MySQL cannot convert an indexed VARCHAR column to TEXT while the
            # old raw-token index still exists.  Drop it only after the digest
            # index is ready and non-null, so a failed backfill/index build
            # leaves the legacy uniqueness constraint intact.
            if dialect == "mysql":
                if raw_token_indexes:
                    self._drop_raw_token_indexes(connection, raw_token_indexes, dialect)
                if raw_token_constraints:
                    self._drop_raw_token_constraints(
                        connection, raw_token_constraints, dialect
                    )

            if dialect == "postgresql" and not isinstance(
                token_column.get("type"), Text
            ):
                connection.execute(
                    text("ALTER TABLE accounts ALTER COLUMN access_token TYPE TEXT")
                )
            elif dialect == "mysql" and not isinstance(
                token_column.get("type"), Text
            ):
                connection.execute(
                    text("ALTER TABLE accounts MODIFY access_token TEXT NOT NULL")
                )

            if dialect == "postgresql":
                if raw_token_indexes:
                    self._drop_raw_token_indexes(connection, raw_token_indexes, dialect)
                if raw_token_constraints:
                    self._drop_raw_token_constraints(
                        connection, raw_token_constraints, dialect
                    )

    @staticmethod
    def _drop_raw_token_indexes(connection, indexes: list[dict[str, Any]], dialect: str) -> None:
        for index in indexes:
            name = index.get("name")
            if not isinstance(name, str) or not name.replace("_", "").isalnum():
                raise StorageDataError()
            if dialect == "mysql":
                connection.execute(text(f"DROP INDEX `{name}` ON accounts"))
            else:
                connection.execute(text(f'DROP INDEX IF EXISTS "{name}"'))

    @staticmethod
    def _drop_raw_token_constraints(
        connection, constraints: list[dict[str, Any]], dialect: str
    ) -> None:
        for constraint in constraints:
            name = constraint.get("name")
            if not isinstance(name, str) or not name.replace("_", "").isalnum():
                raise StorageDataError()
            if dialect == "mysql":
                connection.execute(text(f"ALTER TABLE accounts DROP INDEX `{name}`"))
            else:
                connection.execute(
                    text(f'ALTER TABLE accounts DROP CONSTRAINT "{name}"')
                )

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
            if model is AccountModel:
                if getattr(row, "access_token_hash", None) != _access_token_hash(raw_key.strip()):
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
                values = {
                    key_column: key_value,
                    "data": serialized_data,
                }
                if model is AccountModel:
                    values["access_token_hash"] = _access_token_hash(key_value)
                session.add(model(**values))
            else:
                if model is AccountModel:
                    existing_row.access_token_hash = _access_token_hash(key_value)
                if existing_row.data != serialized_data:
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
