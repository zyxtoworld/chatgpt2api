from __future__ import annotations

import json
import threading
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock

import pytest
from git.exc import GitCommandError

from services.auth_service import AuthService
from services.account_service import AccountService
from services.storage.base import (
    STORAGE_HEALTH_ERROR_MESSAGE,
    StorageConflictError,
    StorageDataError,
)
from services.storage.database_storage import (
    AccountModel,
    AuthKeyModel,
    DatabaseStorageBackend,
)
from services.storage.git_storage import GitStorageBackend
from services.storage.json_storage import JSONStorageBackend


class TrackingStorage:
    def __init__(self, *, accounts=None, auth_keys=None) -> None:
        self.accounts = deepcopy(accounts or [])
        self.auth_keys = deepcopy(auth_keys or [])
        self.save_accounts_calls = 0
        self.save_auth_keys_calls = 0

    def load_accounts(self):
        return deepcopy(self.accounts)

    def save_accounts(self, accounts) -> None:
        self.save_accounts_calls += 1
        self.accounts = deepcopy(accounts)

    def load_auth_keys(self):
        return deepcopy(self.auth_keys)

    def save_auth_keys(self, auth_keys) -> None:
        self.save_auth_keys_calls += 1
        self.auth_keys = deepcopy(auth_keys)

    def health_check(self):
        return {"status": "healthy"}

    def get_backend_info(self):
        return {"type": "memory"}


@pytest.mark.parametrize(
    ("kind", "payload"),
    (
        ("accounts", "{\"broken\":"),
        ("accounts", json.dumps({"not": "a list"})),
        ("auth_keys", "{\"broken\":"),
        ("auth_keys", json.dumps({"items": {"not": "a list"}})),
    ),
)
def test_invalid_json_snapshot_fails_closed_instead_of_becoming_empty(
    tmp_path, kind: str, payload: str
) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    path = accounts_path if kind == "accounts" else auth_keys_path
    path.write_text(payload, encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)

    loader = backend.load_accounts if kind == "accounts" else backend.load_auth_keys

    with pytest.raises(ValueError):
        loader()


def test_invalid_account_entry_is_not_silently_dropped(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    accounts_path.write_text(json.dumps([{"access_token": "ok"}, "not-an-account"]), encoding="utf-8")
    backend = JSONStorageBackend(accounts_path)

    with pytest.raises(ValueError):
        backend.load_accounts()


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_health_check_does_not_report_corrupt_snapshot_as_healthy(tmp_path, kind: str) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    path = accounts_path if kind == "accounts" else auth_keys_path
    path.write_text("{broken", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)

    assert backend.health_check()["status"] == "unhealthy"


def test_auth_service_does_not_turn_storage_error_into_writable_empty_state() -> None:
    storage = mock.Mock()
    storage.load_auth_keys.side_effect = StorageDataError()

    with pytest.raises(StorageDataError):
        AuthService(storage)

    storage.save_auth_keys.assert_not_called()


@pytest.mark.parametrize(
    "invalid_item",
    (
        {"id": "bad-role", "role": "owner", "key_hash": "hash"},
        {"id": "bad-hash", "role": "user", "key_hash": ""},
    ),
)
def test_auth_service_does_not_write_away_semantically_invalid_key(
    invalid_item: dict[str, str],
) -> None:
    valid_item = {"id": "valid", "role": "user", "key_hash": "valid-hash"}
    storage = TrackingStorage(auth_keys=[valid_item])
    service = AuthService(storage)
    storage.auth_keys.append(invalid_item)
    before = deepcopy(storage.auth_keys)

    with mock.patch("services.auth_service.secrets.token_urlsafe", return_value="generated-key"):
        try:
            service.create_key(role="user", name="new")
        except StorageDataError:
            pass
        else:
            assert storage.auth_keys == before
            pytest.fail("invalid auth key was written away instead of failing closed")

    assert storage.auth_keys == before
    assert storage.save_auth_keys_calls == 0


@pytest.mark.parametrize(
    "duplicate_item",
    (
        {"id": "valid", "role": "user", "key_hash": "other-hash"},
        {"id": "other", "role": "user", "key_hash": "valid-hash"},
    ),
)
def test_auth_service_rejects_duplicate_identity_before_write(
    duplicate_item: dict[str, str],
) -> None:
    valid_item = {"id": "valid", "role": "user", "key_hash": "valid-hash"}
    storage = TrackingStorage(auth_keys=[valid_item])
    service = AuthService(storage)
    storage.auth_keys.append(duplicate_item)
    before = deepcopy(storage.auth_keys)

    with mock.patch("services.auth_service.secrets.token_urlsafe", return_value="generated-key"):
        try:
            service.create_key(role="user", name="new")
        except StorageDataError:
            pass
        else:
            assert storage.auth_keys == before
            pytest.fail("duplicate auth identity was written instead of failing closed")

    assert storage.auth_keys == before
    assert storage.save_auth_keys_calls == 0


@pytest.mark.parametrize(
    "invalid_item",
    (
        {"email": "broken@example.com", "status": "正常"},
        {"access_token": "bad-token", "quota": "not-an-integer"},
    ),
)
def test_account_service_does_not_write_away_semantically_invalid_account(
    invalid_item: dict[str, object],
) -> None:
    valid_item = {"access_token": "valid-token", "type": "free"}
    storage = TrackingStorage(accounts=[valid_item, invalid_item])
    before = deepcopy(storage.accounts)

    try:
        service = AccountService(storage)
        with mock.patch.object(service, "_save_cumulative_total"):
            service.add_accounts(["new-token"])
    except StorageDataError:
        pass
    else:
        assert storage.accounts == before
        pytest.fail("invalid account was written away instead of failing closed")

    assert storage.accounts == before
    assert storage.save_accounts_calls == 0


@pytest.mark.parametrize(
    "duplicate_item",
    (
        {"access_token": "same-token", "type": "free"},
        {"accessToken": "same-token", "type": "codex"},
    ),
)
def test_account_service_rejects_duplicate_normalized_token_before_write(
    duplicate_item: dict[str, str],
) -> None:
    first_item = {"access_token": "same-token", "type": "free"}
    storage = TrackingStorage(accounts=[first_item, duplicate_item])
    before = deepcopy(storage.accounts)

    try:
        service = AccountService(storage)
        with mock.patch.object(service, "_save_cumulative_total"):
            service.add_accounts(["new-token"])
    except StorageDataError:
        pass
    else:
        assert storage.accounts == before
        pytest.fail("duplicate normalized account token was written instead of failing closed")

    assert storage.accounts == before
    assert storage.save_accounts_calls == 0


def test_account_service_keeps_explicit_legacy_account_fields_usable() -> None:
    storage = TrackingStorage(accounts=[{"accessToken": "legacy-token", "type": "codex"}])

    with mock.patch.object(AccountService, "_save_cumulative_total"):
        service = AccountService(storage)
        result = service.add_accounts(["new-token"])

    assert result["added"] == 1
    stored = {item["access_token"]: item for item in storage.accounts}
    assert stored["legacy-token"]["source_type"] == "codex"
    assert stored["legacy-token"]["export_type"] == "codex"


def _complete_account_snapshot_item(**overrides: object) -> dict[str, object]:
    item: dict[str, object] = {
        "access_token": "snapshot-token",
        "type": "free",
        "status": "正常",
        "quota": 3,
        "success": 0,
        "fail": 0,
        "invalid_count": 0,
        "last_used_at": None,
        "last_invalid_at": None,
        "last_refresh_error": None,
        "last_refresh_error_at": None,
        "last_token_refresh_at": None,
        "last_token_refresh_error": None,
        "last_token_refresh_error_at": None,
        "restore_at": None,
        "created_at": "2026-01-01 00:00:00",
    }
    item.update(overrides)
    return item


@pytest.mark.parametrize(
    "status",
    (None, "", "未知", 0, {"value": "禁用"}, ["禁用"]),
)
def test_account_service_rejects_invalid_status_snapshot_without_write(status: object) -> None:
    storage = TrackingStorage(accounts=[_complete_account_snapshot_item(status=status)])
    before = deepcopy(storage.accounts)

    with pytest.raises(StorageDataError):
        AccountService(storage)

    assert storage.accounts == before
    assert storage.save_accounts_calls == 0


@pytest.mark.parametrize(
    ("raw_status", "expected_status", "available"),
    (
        ("正常 ", "正常", True),
        ("禁用 ", "禁用", False),
        ("限流 ", "限流", False),
        ("异常 ", "异常", False),
    ),
)
def test_account_service_canonicalizes_known_status_before_selection(
    raw_status: str, expected_status: str, available: bool
) -> None:
    storage = TrackingStorage(accounts=[_complete_account_snapshot_item(status=raw_status)])
    service = AccountService(storage)

    account = service.get_account("snapshot-token")
    assert account is not None
    assert account["status"] == expected_status
    assert ("snapshot-token" in service._list_ready_candidate_tokens()) is available
    assert service.get_stats()["active"] == (1 if available else 0)
    assert storage.save_accounts_calls == 0


@pytest.mark.parametrize("account_type", (None, "", 0, False, [], {"plan": "Pro"}))
def test_account_service_rejects_invalid_type_snapshot_without_write(account_type: object) -> None:
    storage = TrackingStorage(accounts=[_complete_account_snapshot_item(type=account_type)])
    before = deepcopy(storage.accounts)

    with pytest.raises(StorageDataError):
        AccountService(storage)

    assert storage.accounts == before
    assert storage.save_accounts_calls == 0


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("quota", -1),
        ("quota", "3"),
        ("quota", 1.5),
        ("quota", None),
        ("quota", True),
        ("success", -1),
        ("success", "1"),
        ("success", 1.5),
        ("success", None),
        ("success", True),
        ("fail", -1),
        ("fail", "1"),
        ("fail", 1.5),
        ("fail", None),
        ("fail", True),
        ("invalid_count", -1),
        ("invalid_count", "1"),
        ("invalid_count", 1.5),
        ("invalid_count", None),
        ("invalid_count", True),
    ),
)
def test_account_service_rejects_invalid_counter_snapshot_without_write(field: str, value: object) -> None:
    storage = TrackingStorage(accounts=[_complete_account_snapshot_item(**{field: value})])
    before = deepcopy(storage.accounts)

    with pytest.raises(StorageDataError):
        AccountService(storage)

    assert storage.accounts == before
    assert storage.save_accounts_calls == 0


def test_account_migration_write_uses_revision_cas_without_overwriting_concurrent_update(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    legacy_item = _complete_account_snapshot_item(last_refresh_error="legacy error")
    accounts_path.write_text(json.dumps([legacy_item]), encoding="utf-8")
    auth_keys_path.write_text("[]", encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    concurrent_item = _complete_account_snapshot_item(quota=99, last_refresh_error=None)
    original_save_if_revision = backend.save_accounts_if_revision

    def race_with_concurrent_update(expected, accounts):
        backend.save_accounts([concurrent_item])
        return original_save_if_revision(expected, accounts)

    with mock.patch.object(backend, "save_accounts_if_revision", side_effect=race_with_concurrent_update):
        with pytest.raises(StorageConflictError):
            AccountService(backend)

    assert backend.load_accounts() == [concurrent_item]


def _git_backend_and_repo(tmp_path):
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    repo = SimpleNamespace(
        working_dir=str(repo_path),
        head=SimpleNamespace(commit=SimpleNamespace(hexsha="a" * 40)),
    )
    backend = GitStorageBackend(
        "https://git.example/repo.git",
        "git-token",
        local_cache_dir=tmp_path / "cache",
    )
    return backend, repo, repo_path


@pytest.mark.parametrize(
    ("kind", "payload"),
    (
        ("accounts", "{"),
        ("accounts", json.dumps({"not": "a list"})),
        ("accounts", json.dumps([{}, "not-an-account"])),
        ("auth_keys", "{"),
        ("auth_keys", json.dumps({"items": {"not": "a list"}})),
        ("auth_keys", json.dumps({"items": [{}, "not-a-key"]})),
    ),
)
def test_git_storage_rejects_invalid_present_snapshots(tmp_path, kind: str, payload: str) -> None:
    backend, repo, repo_path = _git_backend_and_repo(tmp_path)
    path = repo_path / ("accounts.json" if kind == "accounts" else "auth_keys.json")
    path.write_text(payload, encoding="utf-8")
    loader = backend.load_accounts if kind == "accounts" else backend.load_auth_keys

    with mock.patch.object(backend, "_clone_or_pull", return_value=repo):
        with pytest.raises(StorageDataError):
            loader()


def test_git_storage_missing_snapshots_are_empty(tmp_path) -> None:
    backend, repo, _ = _git_backend_and_repo(tmp_path)

    with mock.patch.object(backend, "_clone_or_pull", return_value=repo):
        assert backend.load_accounts() == []
        assert backend.load_auth_keys() == []


def test_git_storage_serializes_shared_repository_operations(tmp_path) -> None:
    repo_path = tmp_path / "repo"
    repo_path.mkdir()
    (repo_path / "accounts.json").write_text("[]", encoding="utf-8")
    (repo_path / "auth_keys.json").write_text('{"items": []}', encoding="utf-8")
    repo = SimpleNamespace(working_dir=str(repo_path))
    cache_dir = tmp_path / "cache"
    backends = [
        GitStorageBackend(
            "https://git.example/repo.git",
            "git-token",
            local_cache_dir=cache_dir,
        )
        for _ in range(2)
    ]
    barrier = threading.Barrier(2)
    active = 0
    overlap = False
    active_lock = threading.Lock()

    def clone_or_pull():
        nonlocal active, overlap
        with active_lock:
            active += 1
            overlap = overlap or active > 1
        try:
            try:
                barrier.wait(timeout=0.2)
            except threading.BrokenBarrierError:
                pass
            return repo
        finally:
            with active_lock:
                active -= 1

    with (
        mock.patch.object(backends[0], "_clone_or_pull", side_effect=clone_or_pull),
        mock.patch.object(backends[1], "_clone_or_pull", side_effect=clone_or_pull),
        ThreadPoolExecutor(max_workers=2) as executor,
    ):
        futures = (
            executor.submit(backends[0].load_accounts),
            executor.submit(backends[1].load_auth_keys),
        )
        assert futures[0].result(timeout=2) == []
        assert futures[1].result(timeout=2) == []

    assert overlap is False


def test_git_pull_failure_preserves_last_valid_cache(tmp_path) -> None:
    backend = GitStorageBackend(
        "https://git.example/repo.git",
        "git-token",
        local_cache_dir=tmp_path / "cache",
    )
    repo_path = backend.local_cache_dir / "repo"
    (repo_path / ".git").mkdir(parents=True)
    sentinel = repo_path / "accounts.json"
    sentinel.write_text('[{"access_token":"cached-token"}]', encoding="utf-8")
    repo = mock.Mock()
    repo.remote.return_value.pull.side_effect = GitCommandError("git pull", 128)

    with mock.patch("services.storage.git_storage.Repo") as repo_type:
        repo_type.return_value = repo
        repo_type.clone_from.side_effect = AssertionError("pull failure must not trigger destructive re-clone")
        with pytest.raises(GitCommandError):
            backend._clone_or_pull()

    assert sentinel.read_text(encoding="utf-8") == '[{"access_token":"cached-token"}]'
    repo_type.clone_from.assert_not_called()


def test_git_clone_failure_does_not_leave_partial_cache(tmp_path) -> None:
    backend = GitStorageBackend(
        "https://git.example/repo.git",
        "git-token",
        local_cache_dir=tmp_path / "cache",
    )
    repo_path = backend.local_cache_dir / "repo"

    def fail_clone(_url, target, **_kwargs):
        target_path = target if hasattr(target, "mkdir") else type(repo_path)(target)
        target_path.mkdir(parents=True, exist_ok=True)
        (target_path / "partial").write_text("partial", encoding="utf-8")
        raise GitCommandError("git clone", 128)

    with mock.patch("services.storage.git_storage.Repo.clone_from", side_effect=fail_clone):
        with pytest.raises(GitCommandError):
            backend._clone_or_pull()

    assert not repo_path.exists()
    assert list(backend.local_cache_dir.glob(".repo.clone.*.tmp")) == []


def test_git_health_check_rejects_invalid_snapshot_without_raw_error(tmp_path) -> None:
    backend, repo, repo_path = _git_backend_and_repo(tmp_path)
    (repo_path / "accounts.json").write_text("{opaque-storage-secret", encoding="utf-8")

    with mock.patch.object(backend, "_clone_or_pull", return_value=repo):
        result = backend.health_check()

    assert result == {
        "status": "unhealthy",
        "backend": "git",
        "error": STORAGE_HEALTH_ERROR_MESSAGE,
    }


def _insert_database_row(backend: DatabaseStorageBackend, kind: str, payload: str) -> None:
    session = backend.Session()
    try:
        row = (
            AccountModel(access_token="bad-row", data=payload)
            if kind == "accounts"
            else AuthKeyModel(key_id="bad-row", data=payload)
        )
        session.add(row)
        session.commit()
    finally:
        session.close()


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
@pytest.mark.parametrize("payload", ("{", "[]", json.dumps("not-a-record")))
def test_database_storage_rejects_any_invalid_present_row(tmp_path, kind: str, payload: str) -> None:
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")
    _insert_database_row(backend, kind, payload)
    loader = backend.load_accounts if kind == "accounts" else backend.load_auth_keys

    with pytest.raises(StorageDataError):
        loader()


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_database_health_check_rejects_invalid_row_without_raw_error(tmp_path, kind: str) -> None:
    backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")
    _insert_database_row(backend, kind, "opaque-storage-secret")

    result = backend.health_check()

    assert result == {
        "status": "unhealthy",
        "backend": "database",
        "error": STORAGE_HEALTH_ERROR_MESSAGE,
    }
    backend.engine.dispose()


def test_json_replace_failure_preserves_previous_snapshot(tmp_path) -> None:
    path = tmp_path / "accounts.json"
    original = [{"access_token": "old-token"}]
    path.write_text(json.dumps(original), encoding="utf-8")
    backend = JSONStorageBackend(path)

    with mock.patch("os.replace", side_effect=OSError("replace failed")):
        with pytest.raises(OSError):
            backend.save_accounts([{"access_token": "new-token"}])

    assert json.loads(path.read_text(encoding="utf-8")) == original
    assert not list(tmp_path.glob(".accounts.json.*.tmp"))


def test_json_snapshots_round_trip_through_atomic_writer(tmp_path) -> None:
    backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    accounts = [{"access_token": "account-token", "name": "A"}]
    auth_keys = [{"id": "key-1", "role": "user", "key_hash": "hash"}]

    backend.save_accounts(accounts)
    backend.save_auth_keys(auth_keys)

    assert backend.load_accounts() == accounts
    assert backend.load_auth_keys() == auth_keys


def test_json_auth_keys_replace_failure_preserves_previous_envelope(tmp_path) -> None:
    path = tmp_path / "auth_keys.json"
    original_items = [{"id": "old-key", "role": "admin", "key_hash": "old-hash"}]
    path.write_text(json.dumps({"items": original_items}), encoding="utf-8")
    backend = JSONStorageBackend(tmp_path / "accounts.json", path)

    with mock.patch("os.replace", side_effect=OSError("replace failed")):
        with pytest.raises(OSError):
            backend.save_auth_keys([{"id": "new-key", "role": "user", "key_hash": "new-hash"}])

    assert json.loads(path.read_text(encoding="utf-8")) == {"items": original_items}
    assert not list(tmp_path.glob(".auth_keys.json.*.tmp"))
