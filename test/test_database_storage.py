import hashlib
import json
import multiprocessing
from types import SimpleNamespace
from unittest import mock

import pytest
from sqlalchemy import create_engine, event, inspect, String, Text, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from services.storage.database_storage import (
    AccountModel,
    AuthKeyModel,
    DatabaseStorageBackend,
)
from services.storage.base import StorageConflictError, StorageDataError
from services.account_service import AccountService
from services.auth_service import AuthService


def _concurrent_database_startup_worker(database_url, barrier, result_queue):
    backend = None
    try:
        barrier.wait(timeout=15)
        backend = DatabaseStorageBackend(database_url)
        backend.load_accounts()
    except BaseException as exc:
        result_queue.put(f"{type(exc).__name__}:{str(exc)[:120]}")
    else:
        result_queue.put("ok")
    finally:
        if backend is not None:
            backend.close()


def test_database_account_token_schema_has_no_artificial_2048_ceiling() -> None:
    ddl = str(CreateTable(AccountModel.__table__).compile(dialect=postgresql.dialect()))

    assert isinstance(AccountModel.__table__.c.access_token.type, Text)
    assert "VARCHAR(2048)" not in ddl


def test_database_account_identity_hash_is_database_unique(tmp_path) -> None:
    token = "token-identity"
    token_hash = hashlib.sha256(token.encode()).hexdigest()
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'identity.db'}")
    first = backend.Session()
    second = backend.Session()
    try:
        first.add(
            AccountModel(
                access_token=token,
                access_token_hash=token_hash,
                data=json.dumps({"access_token": token}),
            )
        )
        first.commit()

        second.add(
            AccountModel(
                access_token=token,
                access_token_hash=token_hash,
                data=json.dumps({"access_token": token}),
            )
        )
        with pytest.raises(IntegrityError):
            second.commit()
    finally:
        first.close()
        second.rollback()
        second.close()


def test_database_storage_migrates_legacy_account_token_schema(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'legacy-accounts.db'}"
    token = "legacy-token-" + ("x" * 2050)
    legacy_engine = create_engine(database_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE accounts ("
                "id INTEGER PRIMARY KEY, "
                "access_token VARCHAR(2048) NOT NULL, "
                "data TEXT NOT NULL"
                ")"
            )
        )
        connection.execute(text("CREATE UNIQUE INDEX ix_accounts_access_token "
                                "ON accounts (access_token)"))
        connection.execute(
            text("INSERT INTO accounts (access_token, data) VALUES (:token, :data)"),
            {"token": token, "data": json.dumps({"access_token": token})},
        )
    legacy_engine.dispose()

    backend = DatabaseStorageBackend(database_url)
    indexes = inspect(backend.engine).get_indexes("accounts")
    columns = {
        column["name"]: column
        for column in inspect(backend.engine).get_columns("accounts")
    }

    assert backend.load_accounts() == [{"access_token": token}]
    assert columns["access_token_hash"]["nullable"] is False
    assert any(
        index.get("unique")
        and index.get("column_names") == ["access_token_hash"]
        for index in indexes
    )
    assert not any(index.get("column_names") == ["access_token"] for index in indexes)


def test_database_storage_legacy_migration_is_idempotent(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'legacy-idempotent.db'}"
    token = "legacy-token"
    legacy_engine = create_engine(database_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE accounts ("
                "id INTEGER PRIMARY KEY, "
                "access_token VARCHAR(2048) NOT NULL, "
                "data TEXT NOT NULL"
                ")"
            )
        )
        connection.execute(
            text("CREATE UNIQUE INDEX ix_accounts_access_token "
                 "ON accounts (access_token)")
        )
        connection.execute(
            text("INSERT INTO accounts (access_token, data) VALUES (:token, :data)"),
            {"token": token, "data": json.dumps({"access_token": token})},
        )
    legacy_engine.dispose()

    first = DatabaseStorageBackend(database_url)
    first_rows = {
        row.access_token: (row.id, row.access_token_hash, row.data)
        for row in _account_rows(first).values()
    }
    first_indexes = inspect(first.engine).get_indexes("accounts")
    first.close()

    second = DatabaseStorageBackend(database_url)
    second_rows = {
        row.access_token: (row.id, row.access_token_hash, row.data)
        for row in _account_rows(second).values()
    }
    second_indexes = inspect(second.engine).get_indexes("accounts")

    assert second_rows == first_rows
    assert second_indexes == first_indexes


def test_database_storage_sqlite_midstate_removes_legacy_raw_token_index(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'midstate.db'}"
    engine = create_engine(database_url)
    token = "midstate-token"
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE accounts ("
                "id INTEGER PRIMARY KEY, "
                "access_token TEXT NOT NULL, "
                "access_token_hash CHAR(64) NOT NULL, "
                "data TEXT NOT NULL)"
            )
        )
        connection.execute(
            text("CREATE UNIQUE INDEX ix_accounts_access_token ON accounts (access_token)")
        )
        connection.execute(
            text(
                "CREATE UNIQUE INDEX ux_accounts_access_token_hash "
                "ON accounts (access_token_hash)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO accounts (access_token, access_token_hash, data) "
                "VALUES (:token, :token_hash, :data)"
            ),
            {
                "token": token,
                "token_hash": hashlib.sha256(token.encode()).hexdigest(),
                "data": json.dumps({"access_token": token}),
            },
        )
    engine.dispose()

    backend = DatabaseStorageBackend(database_url)
    try:
        indexes = inspect(backend.engine).get_indexes("accounts")
        assert not any(
            index.get("unique") and index.get("column_names") == ["access_token"]
            for index in indexes
        )
        assert any(
            index.get("unique")
            and index.get("column_names") == ["access_token_hash"]
            for index in indexes
        )
    finally:
        backend.close()


def test_database_storage_sqlite_midstate_rebuilds_legacy_raw_token_type(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'midstate-varchar.db'}"
    engine = create_engine(database_url)
    token = "midstate-varchar-token"
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE accounts ("
                "id INTEGER PRIMARY KEY, "
                "access_token VARCHAR(2048) NOT NULL, "
                "access_token_hash CHAR(64) NOT NULL, "
                "data TEXT NOT NULL)"
            )
        )
        connection.execute(
            text("CREATE UNIQUE INDEX ix_accounts_access_token ON accounts (access_token)")
        )
        connection.execute(
            text(
                "CREATE UNIQUE INDEX ux_accounts_access_token_hash "
                "ON accounts (access_token_hash)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO accounts (access_token, access_token_hash, data) "
                "VALUES (:token, :token_hash, :data)"
            ),
            {
                "token": token,
                "token_hash": hashlib.sha256(token.encode()).hexdigest(),
                "data": json.dumps({"access_token": token}),
            },
        )
    engine.dispose()

    backend = DatabaseStorageBackend(database_url)
    try:
        columns = {
            column["name"]: column
            for column in inspect(backend.engine).get_columns("accounts")
        }
        indexes = inspect(backend.engine).get_indexes("accounts")
        assert isinstance(columns["access_token"]["type"], Text)
        assert not any(index.get("column_names") == ["access_token"] for index in indexes)
        assert any(
            index.get("unique")
            and index.get("column_names") == ["access_token_hash"]
            for index in indexes
        )
        assert backend.load_accounts() == [{"access_token": token}]
    finally:
        backend.close()


def test_database_storage_sqlite_midstate_removes_legacy_raw_token_constraint(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'midstate-constraint.db'}"
    engine = create_engine(database_url)
    token = "midstate-constraint-token"
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE accounts ("
                "id INTEGER PRIMARY KEY, "
                "access_token TEXT NOT NULL UNIQUE, "
                "access_token_hash CHAR(64) NOT NULL, "
                "data TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO accounts (access_token, access_token_hash, data) "
                "VALUES (:token, :token_hash, :data)"
            ),
            {
                "token": token,
                "token_hash": hashlib.sha256(token.encode()).hexdigest(),
                "data": json.dumps({"access_token": token}),
            },
        )
    engine.dispose()

    backend = DatabaseStorageBackend(database_url)
    try:
        inspector = inspect(backend.engine)
        constraints = inspector.get_unique_constraints("accounts")
        indexes = inspector.get_indexes("accounts")
        assert not any(
            constraint.get("column_names") == ["access_token"]
            for constraint in constraints
        )
        assert any(
            index.get("unique")
            and index.get("column_names") == ["access_token_hash"]
            for index in indexes
        )
    finally:
        backend.close()


def test_database_storage_concurrent_legacy_startup_is_serialized(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'concurrent-legacy.db'}"
    legacy_engine = create_engine(database_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE accounts ("
                "id INTEGER PRIMARY KEY, "
                "access_token VARCHAR(2048) NOT NULL, "
                "data TEXT NOT NULL)"
            )
        )
        connection.execute(
            text("CREATE UNIQUE INDEX ix_accounts_access_token ON accounts (access_token)")
        )
        connection.execute(
            text(
                "INSERT INTO accounts (access_token, data) "
                "VALUES ('legacy', '{\"access_token\":\"legacy\"}')"
            )
        )
    legacy_engine.dispose()

    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_concurrent_database_startup_worker,
            args=(database_url, barrier, result_queue),
        )
        for _ in range(2)
    ]
    try:
        for process in processes:
            process.start()
        for process in processes:
            process.join(30)
        assert [process.exitcode for process in processes] == [0, 0]
        results = sorted(result_queue.get(timeout=10) for _ in processes)
    finally:
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(10)
    assert results == ["ok", "ok"]


@pytest.mark.parametrize("dialect", ("postgresql", "mysql"))
def test_database_schema_lock_uses_server_advisory_lock(dialect) -> None:
    statements = []

    class Result:
        def scalar(self):
            return 1

    class Connection:
        def execute(self, statement, parameters=None):
            statements.append(str(statement))
            return Result()

        def close(self):
            statements.append("CLOSE")

    backend = DatabaseStorageBackend.__new__(DatabaseStorageBackend)
    backend.engine = SimpleNamespace(
        dialect=SimpleNamespace(name=dialect),
        connect=Connection,
    )
    backend.database_url = f"{dialect}://local/test"

    with backend._acquire_schema_lock():
        pass

    assert any("pg_advisory_lock" in statement or "GET_LOCK" in statement for statement in statements)
    assert any("pg_advisory_unlock" in statement or "RELEASE_LOCK" in statement for statement in statements)
    assert statements[-1] == "CLOSE"


def test_database_storage_rejects_corrupt_account_hash_without_rewriting_row(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'corrupt-hash.db'}"
    backend = DatabaseStorageBackend(database_url)
    backend.save_accounts([{"access_token": "token-a", "name": "A"}])

    session = backend.Session()
    try:
        row = session.query(AccountModel).one()
        row.access_token_hash = "0" * 64
        session.commit()
    finally:
        session.close()

    with pytest.raises(StorageDataError):
        DatabaseStorageBackend(database_url)

    check = create_engine(database_url)
    with check.connect() as connection:
        row = connection.execute(
            text("SELECT access_token, access_token_hash FROM accounts")
        ).one()
    check.dispose()
    assert row.access_token == "token-a"
    assert row.access_token_hash == "0" * 64


def test_database_storage_rejects_legacy_normalized_duplicate_without_rewriting(tmp_path) -> None:
    database_url = f"sqlite:///{tmp_path / 'duplicate-legacy.db'}"
    legacy_engine = create_engine(database_url)
    with legacy_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE accounts ("
                "id INTEGER PRIMARY KEY, "
                "access_token VARCHAR(2048) NOT NULL, "
                "data TEXT NOT NULL"
                ")"
            )
        )
        connection.execute(
            text("CREATE UNIQUE INDEX ix_accounts_access_token "
                 "ON accounts (access_token)")
        )
        connection.execute(
            text(
                "INSERT INTO accounts (access_token, data) VALUES "
                "(' token ', '{\"access_token\": \" token \"}'), "
                "('token', '{\"access_token\": \"token\"}')"
            )
        )
    legacy_engine.dispose()

    with pytest.raises(StorageDataError):
        DatabaseStorageBackend(database_url)

    check = create_engine(database_url)
    with check.connect() as connection:
        rows = connection.execute(
            text("SELECT access_token FROM accounts ORDER BY id")
        ).scalars().all()
    check.dispose()
    assert rows == [" token ", "token"]


class _MigrationResult:
    def mappings(self):
        return []


class _MigrationConnection:
    def __init__(self, statements: list[str]):
        self.statements = statements

    def execute(self, statement, parameters=None):
        self.statements.append(str(statement))
        return _MigrationResult()


class _MigrationContext:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self.connection

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class _MigrationEngine:
    def __init__(self, dialect: str, statements: list[str]):
        self.dialect = SimpleNamespace(name=dialect)
        self.connection = _MigrationConnection(statements)

    def begin(self):
        return _MigrationContext(self.connection)


class _MigrationInspector:
    def __init__(self, indexes=None, constraints=None):
        self.indexes = indexes or []
        self.constraints = constraints or []

    def get_columns(self, table):
        return [{"name": "access_token", "type": String(2048), "nullable": False}]

    def get_indexes(self, table):
        return self.indexes

    def get_unique_constraints(self, table):
        return self.constraints


@pytest.mark.parametrize(
    ("dialect", "raw_drop", "raw_type", "drop_before_type"),
    (
        (
            "postgresql",
            'DROP INDEX IF EXISTS "ix_accounts_access_token"',
            "ALTER TABLE accounts ALTER COLUMN access_token TYPE TEXT",
            False,
        ),
        (
            "mysql",
            "DROP INDEX `ix_accounts_access_token` ON accounts",
            "ALTER TABLE accounts MODIFY access_token TEXT NOT NULL",
            True,
        ),
    ),
)
def test_database_token_migration_orders_hash_before_raw_drop(
    dialect, raw_drop, raw_type, drop_before_type
):
    statements = []
    backend = DatabaseStorageBackend.__new__(DatabaseStorageBackend)
    backend.engine = _MigrationEngine(dialect, statements)
    inspector = _MigrationInspector(
        indexes=[
            {
                "name": "ix_accounts_access_token",
                "unique": True,
                "column_names": ["access_token"],
            }
        ]
    )

    with mock.patch("services.storage.database_storage.inspect", return_value=inspector):
        backend._ensure_account_token_schema()

    create_position = next(
        index for index, statement in enumerate(statements)
        if "CREATE UNIQUE INDEX ux_accounts_access_token_hash" in statement
    )
    not_null_position = next(
        index for index, statement in enumerate(statements)
        if "access_token_hash" in statement and "NOT NULL" in statement
    )
    drop_position = statements.index(raw_drop)
    raw_type_position = statements.index(raw_type)
    assert create_position < not_null_position
    if drop_before_type:
        assert drop_position < raw_type_position
    else:
        assert raw_type_position < drop_position


def test_database_token_migration_handles_reflected_unique_constraint():
    statements = []
    backend = DatabaseStorageBackend.__new__(DatabaseStorageBackend)
    backend.engine = _MigrationEngine("postgresql", statements)
    inspector = _MigrationInspector(
        constraints=[
            {
                "name": "uq_accounts_access_token",
                "column_names": ["access_token"],
            }
        ]
    )

    with mock.patch("services.storage.database_storage.inspect", return_value=inspector):
        backend._ensure_account_token_schema()

    assert 'ALTER TABLE accounts DROP CONSTRAINT "uq_accounts_access_token"' in statements


def test_database_storage_round_trips_access_token_over_2048_characters(tmp_path) -> None:
    token = "token-" + ("x" * 2050)
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'long-token.db'}")

    backend.save_accounts([{"access_token": token, "name": "long"}])

    assert backend.load_accounts() == [{"access_token": token, "name": "long"}]


def _database_revision_writer(
    database_url: str,
    marker: str,
    barrier,
    result_queue,
) -> None:
    backend = DatabaseStorageBackend(database_url)
    try:
        expected = backend.load_accounts_snapshot()
        barrier.wait(timeout=10)
        backend.save_accounts_if_revision(
            expected,
            [{"access_token": "shared-token", "name": marker}],
        )
    except StorageConflictError:
        result_queue.put((marker, "conflict"))
    except BaseException as exc:
        result_queue.put((marker, type(exc).__name__))
    else:
        result_queue.put((marker, "saved"))
    finally:
        backend.close()


def test_database_revision_cas_rejects_second_process_writer(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'concurrent-cas.db'}"
    backend = DatabaseStorageBackend(database_url)
    backend.save_accounts([{"access_token": "shared-token", "name": "original"}])
    backend.close()

    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_database_revision_writer,
            args=(database_url, marker, barrier, result_queue),
        )
        for marker in ("writer-a", "writer-b")
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
    for process in processes:
        assert process.exitcode == 0

    results = [result_queue.get(timeout=2) for _ in processes]
    assert sorted(status for _, status in results) == ["conflict", "saved"]


def _database_auth_revision_writer(
    database_url: str,
    marker: str,
    barrier,
    result_queue,
) -> None:
    backend = DatabaseStorageBackend(database_url)
    try:
        expected = backend.load_auth_keys_snapshot()
        barrier.wait(timeout=10)
        backend.save_auth_keys_if_revision(
            expected,
            [{"id": "shared-key", "role": "user", "key_hash": marker, "enabled": True}],
        )
    except StorageConflictError:
        result_queue.put((marker, "conflict"))
    except BaseException as exc:
        result_queue.put((marker, type(exc).__name__))
    else:
        result_queue.put((marker, "saved"))
    finally:
        backend.close()


def test_database_auth_revision_cas_rejects_second_process_writer(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'concurrent-auth-cas.db'}"
    backend = DatabaseStorageBackend(database_url)
    backend.save_auth_keys([{"id": "shared-key", "role": "user", "key_hash": "original", "enabled": True}])
    backend.close()

    context = multiprocessing.get_context("spawn")
    barrier = context.Barrier(2)
    result_queue = context.Queue()
    processes = [
        context.Process(
            target=_database_auth_revision_writer,
            args=(database_url, marker, barrier, result_queue),
        )
        for marker in ("hash-a", "hash-b")
    ]
    for process in processes:
        process.start()
    for process in processes:
        process.join(timeout=15)
    for process in processes:
        assert process.exitcode == 0

    results = [result_queue.get(timeout=2) for _ in processes]
    assert sorted(status for _, status in results) == ["conflict", "saved"]


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_database_direct_save_locks_scope_before_collection_write(tmp_path, kind):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / f'{kind}-write-order.db'}")
    statements = []

    def record_statement(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement.lower())

    event.listen(backend.engine, "before_cursor_execute", record_statement)
    try:
        if kind == "accounts":
            backend.save_accounts([{"access_token": "token-a", "name": "A"}])
            collection_marker = "accounts"
        else:
            backend.save_auth_keys([{"id": "key-a", "name": "A"}])
            collection_marker = "auth_keys"
    finally:
        event.remove(backend.engine, "before_cursor_execute", record_statement)
        backend.close()

    lock_index = next(
        index for index, statement in enumerate(statements)
        if "storage_mutation_locks" in statement
    )
    collection_index = next(
        index for index, statement in enumerate(statements)
        if collection_marker in statement and "storage_mutation_locks" not in statement
    )
    assert lock_index < collection_index


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_database_direct_and_revision_save_share_scope_mutation_helper(tmp_path, kind):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / f'{kind}-shared-helper.db'}")
    if kind == "accounts":
        save = backend.save_accounts
        load_snapshot = backend.load_accounts_snapshot
        save_if_revision = backend.save_accounts_if_revision
        record = {"access_token": "token-a", "name": "A"}
        updated = {"access_token": "token-a", "name": "B"}
        scope = "accounts"
    else:
        save = backend.save_auth_keys
        load_snapshot = backend.load_auth_keys_snapshot
        save_if_revision = backend.save_auth_keys_if_revision
        record = {"id": "key-a", "name": "A"}
        updated = {"id": "key-a", "name": "B"}
        scope = "auth_keys"

    with mock.patch.object(backend, "_begin_mutation", wraps=backend._begin_mutation) as begin:
        save([record])
        save_if_revision(load_snapshot(), [updated])

    assert [call.args[1] for call in begin.call_args_list] == [scope, scope]


def test_close_disposes_sqlalchemy_engine_connections(tmp_path):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")
    backend.save_accounts([{"access_token": "token-a", "name": "A"}])
    original_dispose = backend.engine.dispose

    with mock.patch.object(backend.engine, "dispose", wraps=original_dispose) as dispose:
        backend.close()
        backend.close()

    assert dispose.call_count == 2
    dispose.assert_has_calls([mock.call(), mock.call()])
    assert "Connections in pool: 0" in backend.engine.pool.status()


def test_database_constructor_disposes_engine_when_initialization_fails(tmp_path):
    created_engine = create_engine(f"sqlite:///{tmp_path / 'constructor-failure.db'}")
    with (
        mock.patch("services.storage.database_storage.create_engine", return_value=created_engine),
        mock.patch.object(
            DatabaseStorageBackend,
            "_ensure_mutation_lock_rows",
            side_effect=RuntimeError("initialization failed"),
        ),
        mock.patch.object(created_engine, "dispose", wraps=created_engine.dispose) as dispose,
    ):
        with pytest.raises(RuntimeError, match="initialization failed"):
            DatabaseStorageBackend("sqlite:///:memory:")

    dispose.assert_called_once_with()
    assert "Connections in pool: 0" in created_engine.pool.status()


def _account_rows(backend: DatabaseStorageBackend) -> dict[str, AccountModel]:
    session = backend.Session()
    try:
        return {
            row.access_token: row
            for row in session.query(AccountModel).order_by(AccountModel.id).all()
        }
    finally:
        session.close()


def test_save_accounts_preserves_existing_rows_and_updates_only_changed_data(tmp_path):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")
    backend.save_accounts(
        [
            {"access_token": "token-a", "name": "A"},
            {"access_token": "token-b", "name": "B"},
        ]
    )

    before = _account_rows(backend)
    before_ids = {token: row.id for token, row in before.items()}
    before_b_data = before["token-b"].data

    backend.save_accounts(
        [
            {"access_token": "token-b", "name": "B"},
            {"access_token": "token-a", "name": "A updated"},
            {"access_token": "token-c", "name": "C"},
        ]
    )

    after = _account_rows(backend)
    assert set(after) == {"token-a", "token-b", "token-c"}
    assert after["token-a"].id == before_ids["token-a"]
    assert after["token-b"].id == before_ids["token-b"]
    assert after["token-b"].data == before_b_data
    assert json.loads(after["token-a"].data)["name"] == "A updated"


def test_save_accounts_deletes_only_rows_missing_from_new_snapshot(tmp_path):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")
    backend.save_accounts(
        [
            {"access_token": "token-a", "name": "A"},
            {"access_token": "token-b", "name": "B"},
        ]
    )
    before = _account_rows(backend)
    token_b_id = before["token-b"].id

    backend.save_accounts([{"access_token": "token-b", "name": "B"}])

    after = _account_rows(backend)
    assert set(after) == {"token-b"}
    assert after["token-b"].id == token_b_id


def test_save_accounts_rejects_duplicate_tokens_and_rolls_back(tmp_path):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")
    original = {"access_token": "token-a", "name": "A"}
    backend.save_accounts([original])

    with pytest.raises(ValueError, match="Duplicate access_token") as exc_info:
        backend.save_accounts(
            [
                {"access_token": "token-a", "name": "first update"},
                {"access_token": "token-a", "name": "second update"},
            ]
        )

    assert "token-a" not in str(exc_info.value)
    assert backend.load_accounts() == [original]


def test_save_accounts_rejects_duplicate_new_tokens_and_rolls_back(tmp_path):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")
    original = {"access_token": "token-a", "name": "A"}
    backend.save_accounts([original])

    with pytest.raises(ValueError, match="Duplicate access_token") as exc_info:
        backend.save_accounts(
            [
                original,
                {"access_token": "token-b", "name": "first new"},
                {"access_token": "token-b", "name": "second new"},
            ]
        )

    assert "token-b" not in str(exc_info.value)
    assert backend.load_accounts() == [original]


def test_save_auth_keys_preserves_ids_with_target_key_mapping(tmp_path):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'auth-keys.db'}")
    backend.save_auth_keys(
        [
            {"id": "key-a", "name": "A"},
            {"id": "key-b", "name": "B"},
        ]
    )

    session = backend.Session()
    try:
        before_ids = {
            row.key_id: row.id
            for row in session.query(AuthKeyModel).order_by(AuthKeyModel.id).all()
        }
    finally:
        session.close()

    backend.save_auth_keys(
        [
            {"id": "key-b", "name": "B"},
            {"id": "key-a", "name": "A updated"},
            {"id": "key-c", "name": "C"},
        ]
    )

    session = backend.Session()
    try:
        after = {
            row.key_id: row
            for row in session.query(AuthKeyModel).order_by(AuthKeyModel.id).all()
        }
        assert set(after) == {"key-a", "key-b", "key-c"}
        assert after["key-a"].id == before_ids["key-a"]
        assert after["key-b"].id == before_ids["key-b"]
        assert json.loads(after["key-a"].data)["name"] == "A updated"
    finally:
        session.close()


def test_save_auth_keys_rejects_duplicate_ids_and_rolls_back(tmp_path):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'auth-keys.db'}")
    original = {"id": "key-a", "name": "A"}
    backend.save_auth_keys([original])

    with pytest.raises(ValueError, match="Duplicate id") as exc_info:
        backend.save_auth_keys(
            [
                {"id": "key-a", "name": "first update"},
                {"id": "key-a", "name": "second update"},
            ]
        )

    assert "key-a" not in str(exc_info.value)
    assert backend.load_auth_keys() == [original]


@pytest.mark.parametrize(
    ("kind", "record"),
    (
        ("accounts", {"access_token": {"storage-key-container-canary": "secret"}}),
        ("auth_keys", {"id": ["storage-key-container-canary"]}),
    ),
)
def test_database_storage_rejects_container_primary_keys(tmp_path, kind, record):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / f'{kind}.db'}")
    save = backend.save_accounts if kind == "accounts" else backend.save_auth_keys

    with pytest.raises(StorageDataError):
        save([record])

    assert (backend.load_accounts if kind == "accounts" else backend.load_auth_keys)() == []


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_database_storage_rejects_row_key_mismatch(tmp_path, kind):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / f'{kind}-mismatch.db'}")
    session = backend.Session()
    try:
        if kind == "accounts":
            session.add(
                AccountModel(
                    access_token="row-key",
                    access_token_hash=hashlib.sha256(b"row-key").hexdigest(),
                    data=json.dumps({"access_token": "payload-key"}),
                )
            )
        else:
            session.add(AuthKeyModel(key_id="row-key", data=json.dumps({"id": "payload-key"})))
        session.commit()
    finally:
        session.close()

    with pytest.raises(StorageDataError):
        (backend.load_accounts if kind == "accounts" else backend.load_auth_keys)()


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_database_storage_trimmed_identity_has_stable_round_trip_and_service_identity(tmp_path, kind):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / f'{kind}-trimmed.db'}")
    try:
        if kind == "accounts":
            backend.save_accounts([{"access_token": " token ", "name": "A"}])
            first = backend.load_accounts_snapshot()
            backend.save_accounts(first.records)
            second = backend.load_accounts_snapshot()
            assert first.revision == second.revision
            assert AccountService(backend).get_account("token")["access_token"] == "token"
            backend.save_accounts([])
            assert backend.load_accounts() == []
        else:
            raw_key = "raw-auth-key"
            backend.save_auth_keys(
                [{
                    "id": " id ",
                    "role": "user",
                    "key_hash": hashlib.sha256(raw_key.encode()).hexdigest(),
                    "enabled": True,
                }]
            )
            first = backend.load_auth_keys_snapshot()
            backend.save_auth_keys(first.records)
            second = backend.load_auth_keys_snapshot()
            assert first.revision == second.revision
            assert AuthService(backend).authenticate(raw_key)["id"] == "id"
            backend.save_auth_keys([])
            assert backend.load_auth_keys() == []
    finally:
        backend.close()


@pytest.mark.parametrize(
    ("kind", "first", "aliases"),
    (
        (
            "accounts",
            {"access_token": " token ", "name": "A"},
            [{"access_token": " token ", "name": "A"}, {"access_token": "token", "name": "B"}],
        ),
        (
            "auth_keys",
            {"id": " id ", "role": "user", "key_hash": "hash-a", "enabled": True},
            [{"id": " id ", "role": "user", "key_hash": "hash-a", "enabled": True}, {"id": "id", "role": "user", "key_hash": "hash-b", "enabled": True}],
        ),
    ),
)
def test_database_storage_rejects_trimmed_identity_alias_without_mutating_snapshot(tmp_path, kind, first, aliases):
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / f'{kind}-aliases.db'}")
    try:
        save = backend.save_accounts if kind == "accounts" else backend.save_auth_keys
        load = backend.load_accounts if kind == "accounts" else backend.load_auth_keys
        save([first])
        with pytest.raises(ValueError):
            save(aliases)
        assert load() == [first]
    finally:
        backend.close()
