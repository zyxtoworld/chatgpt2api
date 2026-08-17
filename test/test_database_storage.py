import json
import hashlib
import multiprocessing
from unittest import mock

import pytest
from sqlalchemy import create_engine, event

from services.storage.database_storage import (
    AccountModel,
    AuthKeyModel,
    DatabaseStorageBackend,
)
from services.storage.base import StorageConflictError, StorageDataError
from services.account_service import AccountService
from services.auth_service import AuthService


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
            session.add(AccountModel(access_token="row-key", data=json.dumps({"access_token": "payload-key"})))
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
