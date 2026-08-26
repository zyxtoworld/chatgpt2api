from __future__ import annotations

import hashlib
import json
import math
import os
import threading
from copy import deepcopy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pytest
from git.exc import GitCommandError

import services.secure_file as secure_file
import services.config as config_module
import services.storage.json_storage as json_storage_module
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


@pytest.mark.parametrize("backend_kind", ("json", "database"))
@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_direct_storage_save_rejects_nonfinite_json_values(tmp_path, backend_kind: str, kind: str) -> None:
    if backend_kind == "json":
        backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    else:
        backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")

    try:
        if kind == "accounts":
            original = [{"access_token": "token-a", "quota": 1}]
            invalid = [{"access_token": "token-a", "quota": math.nan}]
            save = backend.save_accounts
            load = backend.load_accounts
        else:
            original = [{"id": "key-a", "enabled": True}]
            invalid = [{"id": "key-a", "enabled": True, "metadata": math.inf}]
            save = backend.save_auth_keys
            load = backend.load_auth_keys

        save(original)
        with pytest.raises(StorageDataError):
            save(invalid)
        assert load() == original
    finally:
        backend.close()


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_revision_save_rejects_nonfinite_record_before_overwrite(tmp_path, kind: str) -> None:
    backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    if kind == "accounts":
        original = [{"access_token": "token-a", "quota": 1}]
        invalid = [{"access_token": "token-a", "quota": math.nan}]
        load = backend.load_accounts
        save_if_unchanged = backend.save_accounts_if_unchanged
    else:
        original = [{"id": "key-a", "enabled": True}]
        invalid = [{"id": "key-a", "enabled": True, "metadata": math.inf}]
        load = backend.load_auth_keys
        save_if_unchanged = backend.save_auth_keys_if_unchanged

    if kind == "accounts":
        backend.save_accounts(original)
    else:
        backend.save_auth_keys(original)

    with pytest.raises(StorageDataError):
        save_if_unchanged(original, invalid)

    assert load() == original


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_revision_save_persists_frozen_records_not_mutated_input(tmp_path, kind: str) -> None:
    backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    if kind == "accounts":
        records = [{"access_token": "token-a", "metadata": {"labels": ["initial"]}}]
        save = backend.save_accounts
        load_snapshot = backend.load_accounts_snapshot
        save_if_revision = backend.save_accounts_if_revision
        load = backend.load_accounts
    else:
        records = [{"id": "key-a", "metadata": {"labels": ["initial"]}}]
        save = backend.save_auth_keys
        load_snapshot = backend.load_auth_keys_snapshot
        save_if_revision = backend.save_auth_keys_if_revision
        load = backend.load_auth_keys

    save(records)
    expected = load_snapshot()
    snapshot_ready = threading.Event()
    release_snapshot = threading.Event()
    original_load_snapshot = load_snapshot

    def gated_load_snapshot():
        snapshot_ready.set()
        assert release_snapshot.wait(timeout=2)
        return original_load_snapshot()

    if kind == "accounts":
        backend.load_accounts_snapshot = gated_load_snapshot
    else:
        backend.load_auth_keys_snapshot = gated_load_snapshot

    result: list[object] = []
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            result.append(save_if_revision(expected, records))
        except BaseException as exc:  # pragma: no cover - assertion below reports failures
            errors.append(exc)

    thread = threading.Thread(target=writer)
    thread.start()
    assert snapshot_ready.wait(timeout=2)
    records[0]["metadata"]["labels"].append("mutated-after-revision")
    release_snapshot.set()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert errors == []
    assert result
    assert load() == expected.records
    assert result[0].revision == load_snapshot().revision


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_load_does_not_read_replaced_snapshot_path(tmp_path, kind: str) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    target = accounts_path if kind == "accounts" else auth_keys_path
    replacement = tmp_path / "replacement.json"
    original = [{"id": "original-record"}]
    replaced = [{"id": "replaced-record"}]
    target.write_text(json.dumps(original if kind == "accounts" else {"items": original}), encoding="utf-8")
    replacement.write_text(json.dumps(replaced if kind == "accounts" else {"items": replaced}), encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    original_read_text = Path.read_text

    def replace_before_read(path_obj, *args, **kwargs):
        if path_obj == target:
            target.write_bytes(replacement.read_bytes())
        return original_read_text(path_obj, *args, **kwargs)

    loader = backend.load_accounts if kind == "accounts" else backend.load_auth_keys
    with mock.patch.object(Path, "read_text", autospec=True, side_effect=replace_before_read):
        loaded = loader()

    assert loaded == original


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_load_reads_fixed_handle_after_path_replacement(tmp_path, kind: str) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    target = accounts_path if kind == "accounts" else auth_keys_path
    replacement = tmp_path / "replacement.json"
    displaced = tmp_path / "displaced.json"
    original = [{"id": "original-record"}]
    replaced = [{"id": "replaced-record"}]
    target.write_text(json.dumps(original if kind == "accounts" else {"items": original}), encoding="utf-8")
    replacement.write_text(json.dumps(replaced if kind == "accounts" else {"items": replaced}), encoding="utf-8")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    original_open = secure_file.open_no_follow_file

    def replace_after_open(path_obj, *args, **kwargs):
        opened = original_open(path_obj, *args, **kwargs)
        if path_obj == target:
            target.replace(displaced)
            replacement.replace(target)
        return opened

    loader = backend.load_accounts if kind == "accounts" else backend.load_auth_keys
    with mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_after_open):
        loaded = loader()

    assert loaded == original


@pytest.mark.skipif(os.name != "posix", reason="requires POSIX directory rebind")
@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_save_does_not_write_foreign_parent_after_rebind_before_temp_creation(
    tmp_path, kind: str
) -> None:
    root = tmp_path / "store"
    root.mkdir()
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    displaced = tmp_path / "store-displaced"
    accounts_path = root / "accounts.json"
    auth_keys_path = root / "auth_keys.json"
    backend = JSONStorageBackend(accounts_path, auth_keys_path)

    if kind == "accounts":
        target = accounts_path
        save = backend.save_accounts
        old_value = [{"access_token": "old-token"}]
        new_value = [{"access_token": "new-token"}]
        foreign_value = [{"access_token": "foreign-sentinel"}]
        old_disk_value = old_value
        target.write_text(json.dumps(old_value), encoding="utf-8")
        foreign_target = foreign / target.name
        foreign_target.write_text(json.dumps(foreign_value), encoding="utf-8")
    else:
        target = auth_keys_path
        save = backend.save_auth_keys
        old_value = [{"id": "old-key"}]
        new_value = [{"id": "new-key"}]
        foreign_value = {"items": [{"id": "foreign-sentinel"}]}
        old_disk_value = {"items": old_value}
        target.write_text(json.dumps({"items": old_value}), encoding="utf-8")
        foreign_target = foreign / target.name
        foreign_target.write_text(json.dumps(foreign_value), encoding="utf-8")

    original_mkdir = Path.mkdir
    rebound = False

    def rebind_after_mkdir(path_obj, *args, **kwargs):
        nonlocal rebound
        result = original_mkdir(path_obj, *args, **kwargs)
        if path_obj == root and not rebound:
            rebound = True
            root.rename(displaced)
            root.symlink_to(foreign, target_is_directory=True)
        return result

    path_lock = backend._scope_path_locks[kind]
    with path_lock:
        with (
            mock.patch.object(Path, "mkdir", autospec=True, side_effect=rebind_after_mkdir),
            pytest.raises(OSError),
        ):
            save(new_value)

    assert rebound
    assert root.is_symlink()
    assert foreign_target.read_text(encoding="utf-8") == json.dumps(foreign_value)
    assert json.loads((displaced / target.name).read_text(encoding="utf-8")) == old_disk_value
    assert not list(displaced.glob(f".{target.name}.*.tmp"))
    assert not list(foreign.glob(f".{target.name}.*.tmp"))


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_save_rejects_ordinary_parent_rebind_after_identity_capture(
    tmp_path, kind: str
) -> None:
    root = tmp_path / "store"
    root.mkdir()
    displaced = tmp_path / "store-displaced"
    accounts_path = root / "accounts.json"
    auth_keys_path = root / "auth_keys.json"
    backend = JSONStorageBackend(accounts_path, auth_keys_path)

    if kind == "accounts":
        target = accounts_path
        save = backend.save_accounts
        old_value = [{"access_token": "old-token"}]
        new_value = [{"access_token": "new-token"}]
        foreign_value = [{"access_token": "foreign-sentinel"}]
        target.write_text(json.dumps(old_value), encoding="utf-8")
    else:
        target = auth_keys_path
        save = backend.save_auth_keys
        old_value = [{"id": "old-key"}]
        new_value = [{"id": "new-key"}]
        foreign_value = {"items": [{"id": "foreign-sentinel"}]}
        target.write_text(json.dumps({"items": old_value}), encoding="utf-8")

    original_atomic_write = json_storage_module.atomic_write_bytes
    rebound = False

    def rebind_after_identity_capture(path, root_path, payload, **kwargs):
        nonlocal rebound
        if not rebound:
            root.rename(displaced)
            root.mkdir()
            (root / target.name).write_text(json.dumps(foreign_value), encoding="utf-8")
            rebound = True
        return original_atomic_write(path, root_path, payload, **kwargs)

    with (
        mock.patch.object(json_storage_module, "atomic_write_bytes", side_effect=rebind_after_identity_capture),
        pytest.raises(OSError),
    ):
        save(new_value)

    if rebound:
        assert json.loads((displaced / target.name).read_text(encoding="utf-8")) == (
            old_value if kind == "accounts" else {"items": old_value}
        )
        assert json.loads((root / target.name).read_text(encoding="utf-8")) == foreign_value
        assert not list(displaced.glob(f".{target.name}.*.tmp"))
        assert not list(root.glob(f".{target.name}.*.tmp"))
    else:
        # Windows holds the sidecar handle without DELETE sharing, so the
        # attempted directory rename itself is rejected before any rebind.
        assert json.loads(target.read_text(encoding="utf-8")) == (
            old_value if kind == "accounts" else {"items": old_value}
        )
        assert not list(root.glob(f".{target.name}.*.tmp"))


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_save_passes_parent_identity_to_atomic_writer(tmp_path, kind: str) -> None:
    backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    save = backend.save_accounts if kind == "accounts" else backend.save_auth_keys
    observed: dict[str, object] = {}
    original_atomic_write = json_storage_module.atomic_write_bytes

    def observe_atomic_write(path, root, payload, **kwargs):
        observed["expected_root_identity"] = kwargs.get("expected_root_identity")
        return original_atomic_write(path, root, payload, **kwargs)

    with mock.patch.object(json_storage_module, "atomic_write_bytes", side_effect=observe_atomic_write):
        save([{"access_token": "token-a"}] if kind == "accounts" else [{"id": "key-a"}])

    parent_stat = backend.file_path.parent.stat()
    assert observed["expected_root_identity"] == (parent_stat.st_dev, parent_stat.st_ino)


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


def test_account_cache_generation_bumps_only_after_persistence(tmp_path) -> None:
    storage = TrackingStorage(accounts=[])
    with mock.patch.object(config_module, "DATA_DIR", tmp_path):
        service = AccountService(storage)
        service.add_account_items([{"access_token": "generation-token", "quota": 1}])
        before_update = service.get_account_cache_scope("generation-token")

        service.update_account("generation-token", {"quota": 2})
        after_success = service.get_account_cache_scope("generation-token")
        assert after_success != before_update
        assert storage.load_accounts()[0]["quota"] == 2

        with mock.patch.object(service, "_save_accounts", side_effect=StorageDataError()):
            with pytest.raises(StorageDataError):
                service.update_account("generation-token", {"quota": 3})

        assert service.get_account_cache_scope("generation-token") == after_success
        assert service.list_accounts()[0]["quota"] == 2
        assert storage.load_accounts()[0]["quota"] == 2


def test_account_add_does_not_publish_cumulative_state_before_account_snapshot(
    tmp_path,
) -> None:
    storage = TrackingStorage(accounts=[])
    with mock.patch.object(config_module, "DATA_DIR", tmp_path):
        service = AccountService(storage)
        with (
            mock.patch.object(service, "_save_accounts", side_effect=StorageDataError()),
            mock.patch.object(service, "_save_cumulative_total") as save_cumulative,
            pytest.raises(StorageDataError),
        ):
            service.add_accounts(["new-token"])

    assert service.list_accounts() == []
    assert service._cumulative_total == 0
    save_cumulative.assert_not_called()


def test_account_add_persists_account_snapshot_before_cumulative_counter(tmp_path) -> None:
    storage = TrackingStorage(accounts=[])
    events: list[str] = []
    with mock.patch.object(config_module, "DATA_DIR", tmp_path):
        service = AccountService(storage)
        original_save_accounts = service._save_accounts
        original_save_cumulative = service._save_cumulative_total

        def save_accounts() -> None:
            events.append("accounts")
            original_save_accounts()

        def save_cumulative() -> None:
            events.append("cumulative")
            original_save_cumulative()

        with (
            mock.patch.object(service, "_save_accounts", side_effect=save_accounts),
            mock.patch.object(service, "_save_cumulative_total", side_effect=save_cumulative),
        ):
            result = service.add_accounts(["new-token"])

    assert result["added"] == 1
    assert events == ["accounts", "cumulative"]


def test_json_account_and_cumulative_total_share_one_atomic_snapshot(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    storage = JSONStorageBackend(accounts_path, auth_keys_path)

    with (
        mock.patch.object(config_module, "DATA_DIR", tmp_path),
        mock.patch.object(json_storage_module, "atomic_write_bytes", side_effect=OSError("snapshot disk full")),
    ):
        service = AccountService(storage)
        with pytest.raises(OSError, match="snapshot disk full"):
            service.add_accounts(["historical-token"])

    assert storage.load_accounts() == []
    assert not (tmp_path / ".cumulative_total").exists()

    with mock.patch.object(config_module, "DATA_DIR", tmp_path):
        restarted_service = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))

    assert restarted_service.get_stats()["total"] == 0
    assert restarted_service.get_stats()["cumulative_total"] == 0


def test_json_cumulative_snapshot_survives_delete_and_restart(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    with mock.patch.object(config_module, "DATA_DIR", tmp_path):
        service = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))
        assert service.add_accounts(["historical-token"])["added"] == 1
        deleted_service = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))
        assert deleted_service.delete_accounts(["historical-token"])["removed"] == 1
        restarted_service = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))

    assert restarted_service.get_stats()["total"] == 0
    assert restarted_service.get_stats()["cumulative_total"] == 1


def test_json_stale_account_service_cannot_overwrite_newer_cumulative_snapshot(tmp_path) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    with mock.patch.object(config_module, "DATA_DIR", tmp_path):
        service_one = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))
        service_two = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))
        assert service_one.add_accounts(["newer-token"])["added"] == 1
        with pytest.raises(StorageConflictError):
            service_two.add_accounts(["stale-token"])
        restarted_service = AccountService(JSONStorageBackend(accounts_path, auth_keys_path))

    assert restarted_service.get_stats()["cumulative_total"] == 1
    assert restarted_service.list_tokens() == ["newer-token"]


def test_account_update_rolls_back_memory_when_account_snapshot_save_fails() -> None:
    storage = TrackingStorage(accounts=[{"access_token": "token-1", "type": "free", "name": "old"}])
    service = AccountService(storage)
    before = service.get_account("token-1")

    with mock.patch.object(service, "_save_accounts", side_effect=StorageDataError()), pytest.raises(StorageDataError):
        service.update_account("token-1", {"name": "new"})

    assert service.get_account("token-1") == before
    assert storage.accounts == [{"access_token": "token-1", "type": "free", "name": "old"}]


@pytest.mark.parametrize("mutation", ("add", "update", "delete"))
def test_account_mutation_log_failure_does_not_retract_committed_result(
    mutation: str,
) -> None:
    storage = TrackingStorage(accounts=[{"access_token": "token-1", "type": "free", "name": "old"}])
    service = AccountService(storage)

    with (
        mock.patch("services.account_service.log_service.add", side_effect=OSError("log unavailable")),
        mock.patch("services.account_service._ACCOUNT_LOGGER.error") as fallback_logger,
    ):
        if mutation == "add":
            result = service.add_accounts(["token-2"])
        elif mutation == "update":
            result = service.update_account("token-1", {"name": "new"})
        else:
            result = service.delete_accounts(["token-1"])

    if mutation == "add":
        assert result["added"] == 1
        assert any(item.get("access_token") == "token-2" for item in storage.accounts)
    elif mutation == "update":
        assert result is not None
        assert storage.accounts[0]["name"] == "new"
    else:
        assert result["removed"] == 1
        assert storage.accounts == []
    fallback_logger.assert_called_once_with("account log persistence failed")


@pytest.mark.parametrize("mutation", ("delete", "image_result"))
def test_account_mutations_roll_back_memory_when_snapshot_save_fails(mutation: str) -> None:
    stored = {"access_token": "token-1", "type": "free", "quota": 3, "name": "old"}
    storage = TrackingStorage(accounts=[stored])
    service = AccountService(storage)
    before = service.list_accounts()

    with mock.patch.object(service, "_save_accounts", side_effect=StorageDataError()), pytest.raises(StorageDataError):
        if mutation == "delete":
            service.delete_accounts(["token-1"])
        else:
            service.mark_image_result("token-1", True)

    assert service.list_accounts() == before
    assert storage.accounts == [stored]


def test_refresh_success_and_account_update_share_one_rollback_boundary() -> None:
    stored = {
        "access_token": "token-1",
        "type": "free",
        "status": "异常",
        "invalid_count": 2,
        "last_invalid_at": "2026-08-15T00:00:00+00:00",
        "last_refresh_error": "账号访问令牌无效",
        "last_refresh_error_at": "2026-08-15T00:01:00+00:00",
        "name": "old",
    }
    storage = TrackingStorage(accounts=[stored])
    service = AccountService(storage)
    before = service.get_account("token-1")

    with mock.patch.object(service, "_save_accounts", side_effect=StorageDataError()), pytest.raises(StorageDataError):
        service.update_account("token-1", {"name": "new"}, reset_refresh_state=True)

    assert service.get_account("token-1") == before
    assert storage.accounts == [stored]


def test_refresh_success_clears_all_refresh_state_fields_in_one_commit() -> None:
    stored = {
        "access_token": "token-1",
        "type": "free",
        "status": "异常",
        "invalid_count": 2,
        "last_invalid_at": "2026-08-15T00:00:00+00:00",
        "last_refresh_error": "账号访问令牌无效",
        "last_refresh_error_at": "2026-08-15T00:01:00+00:00",
    }
    storage = TrackingStorage(accounts=[stored])
    service = AccountService(storage)

    result = service.update_account(
        "token-1",
        {"name": "refreshed"},
        reset_refresh_state=True,
    )

    assert result is not None
    assert result["invalid_count"] == 0
    assert result["last_invalid_at"] is None
    assert result["last_refresh_error"] is None
    assert result["last_refresh_error_at"] is None
    persisted = storage.accounts[0]
    assert persisted["invalid_count"] == 0
    assert persisted["last_invalid_at"] is None
    assert persisted["last_refresh_error"] is None
    assert persisted["last_refresh_error_at"] is None


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


@pytest.mark.parametrize("backend_kind", ("json", "database"))
@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_storage_save_rejects_non_object_records_before_mutation(tmp_path, backend_kind: str, kind: str) -> None:
    if backend_kind == "json":
        backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    else:
        backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")

    valid = (
        {"access_token": "existing-account"}
        if kind == "accounts"
        else {"id": "existing-key", "role": "user", "key_hash": "hash"}
    )
    invalid_snapshot = [valid, "not-an-object"]
    save = backend.save_accounts if kind == "accounts" else backend.save_auth_keys
    load = backend.load_accounts if kind == "accounts" else backend.load_auth_keys
    save([valid])

    with pytest.raises(StorageDataError):
        save(invalid_snapshot)

    assert load() == [valid]


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


def test_account_load_missing_created_at_is_stable_across_restarts() -> None:
    storage = TrackingStorage(
        accounts=[
            {
                "access_token": "legacy-token",
                "type": "free",
                "status": "正常",
                "quota": 1,
            }
        ]
    )

    with mock.patch.object(
        AccountService,
        "_now",
        side_effect=("2026-08-16 10:00:00", "2026-08-16 11:00:00"),
    ):
        first = AccountService(storage)
        first_account = first.list_accounts()[0]
        second = AccountService(storage)
        second_account = second.list_accounts()[0]

    assert first_account == second_account
    assert first_account["created_at"] is None
    assert storage.save_accounts_calls == 0


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


@pytest.mark.parametrize(
    "field",
    (
        "last_used_at",
        "last_invalid_at",
        "last_refresh_error_at",
        "last_token_refresh_at",
        "last_token_refresh_error_at",
        "created_at",
    ),
)
def test_account_service_rejects_malformed_persisted_time_fields_without_write(field: str) -> None:
    storage = TrackingStorage(
        accounts=[_complete_account_snapshot_item(**{field: {"timestamp": "canary"}})]
    )
    before = deepcopy(storage.accounts)

    with pytest.raises(StorageDataError):
        AccountService(storage)

    assert storage.accounts == before
    assert storage.save_accounts_calls == 0


@pytest.mark.parametrize(
    "limits_progress",
    (
        {"feature_name": "image_gen"},
        [{"feature_name": "image_gen", "remaining": {"canary": "opaque"}}],
        [{"feature_name": "image_gen", "unknown": "field"}],
        [{"feature_name": "image_gen"}] * 101,
    ),
)
def test_account_service_rejects_noncanonical_limits_progress_snapshot_without_write(
    limits_progress: object,
) -> None:
    storage = TrackingStorage(
        accounts=[_complete_account_snapshot_item(limits_progress=limits_progress)]
    )
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


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_direct_save_shares_cas_mutation_lock(tmp_path, kind: str) -> None:
    backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    if kind == "accounts":
        save = backend.save_accounts
        load_snapshot = backend.load_accounts_snapshot
        save_if_revision = backend.save_accounts_if_revision
        initial = [{"access_token": "token-old", "name": "old"}]
        cas_value = [{"access_token": "token-cas", "name": "cas"}]
        direct_value = [{"access_token": "token-direct", "name": "direct"}]
        final_load = backend.load_accounts
    else:
        save = backend.save_auth_keys
        load_snapshot = backend.load_auth_keys_snapshot
        save_if_revision = backend.save_auth_keys_if_revision
        initial = [{"id": "key-old", "role": "user", "key_hash": "hash-old", "enabled": True}]
        cas_value = [{"id": "key-cas", "role": "user", "key_hash": "hash-cas", "enabled": True}]
        direct_value = [{"id": "key-direct", "role": "user", "key_hash": "hash-direct", "enabled": True}]
        final_load = backend.load_auth_keys

    save(initial)
    expected = load_snapshot()
    read_ready = threading.Event()
    release_read = threading.Event()
    progress = threading.Event()
    direct_lock_attempted = threading.Event()
    direct_write_attempted = threading.Event()
    cas_done = threading.Event()
    release_direct = threading.Event()
    errors: list[BaseException] = []
    thread_ids: dict[str, int] = {}

    original_load_snapshot = load_snapshot

    def gated_load_snapshot():
        snapshot = original_load_snapshot()
        read_ready.set()
        if not release_read.wait(2):
            raise AssertionError("CAS read was not released")
        return snapshot

    if kind == "accounts":
        backend.load_accounts_snapshot = gated_load_snapshot
    else:
        backend.load_auth_keys_snapshot = gated_load_snapshot

    original_mutation_lock = backend._mutation_lock

    def tracked_mutation_lock(requested_scope):
        real_lock = original_mutation_lock(requested_scope)

        class TrackingLock:
            def __enter__(self):
                if threading.get_ident() == thread_ids.get("direct"):
                    direct_lock_attempted.set()
                    progress.set()
                    if not release_direct.wait(2):
                        raise AssertionError("direct save lock was not released")
                real_lock.__enter__()
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                return real_lock.__exit__(exc_type, exc_value, traceback)

        return TrackingLock()

    backend._mutation_lock = tracked_mutation_lock
    original_write = backend._save_json_value

    expected_path = backend.file_path if kind == "accounts" else backend.auth_keys_path
    expected_max_bytes = (
        json_storage_module.ACCOUNT_SNAPSHOT_MAX_BYTES
        if kind == "accounts"
        else json_storage_module._AUTH_KEYS_MAX_BYTES
    )

    def observed_write(
        path,
        value,
        *,
        max_bytes: int,
        cumulative_total: int | None = None,
    ):
        assert path == expected_path
        assert max_bytes == expected_max_bytes
        assert cumulative_total is None
        if threading.get_ident() == thread_ids.get("direct"):
            direct_write_attempted.set()
            progress.set()
        return original_write(
            path,
            value,
            max_bytes=max_bytes,
            cumulative_total=cumulative_total,
        )

    backend._save_json_value = observed_write

    def cas_writer() -> None:
        thread_ids["cas"] = threading.get_ident()
        try:
            save_if_revision(expected, cas_value)
        except BaseException as exc:
            errors.append(exc)
        finally:
            cas_done.set()

    def direct_writer() -> None:
        thread_ids["direct"] = threading.get_ident()
        try:
            save(direct_value)
        except BaseException as exc:
            errors.append(exc)

    cas_thread = threading.Thread(target=cas_writer)
    direct_thread = threading.Thread(target=direct_writer)
    cas_thread.start()
    assert read_ready.wait(2)
    direct_thread.start()
    try:
        assert progress.wait(2)
        assert direct_lock_attempted.is_set(), "direct save bypassed the CAS mutation lock"
        assert not direct_write_attempted.is_set()
    finally:
        release_read.set()
        assert cas_done.wait(2)
        release_direct.set()
        cas_thread.join(2)
        direct_thread.join(2)

    assert not cas_thread.is_alive()
    assert not direct_thread.is_alive()
    assert errors == []
    assert final_load() == direct_value


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
            AccountModel(
                access_token="bad-row",
                access_token_hash=hashlib.sha256(b"bad-row").hexdigest(),
                data=payload,
            )
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


def test_account_cumulative_counter_reads_fixed_handle_after_path_replacement(tmp_path) -> None:
    cumulative_path = tmp_path / ".cumulative_total"
    replacement = tmp_path / "replacement"
    cumulative_path.write_text("7", encoding="utf-8")
    replacement.write_text("99", encoding="utf-8")
    original_read_text = Path.read_text

    def replace_before_read(path_obj, *args, **kwargs):
        if path_obj == cumulative_path:
            cumulative_path.replace(tmp_path / "displaced-total")
            replacement.replace(cumulative_path)
        return original_read_text(path_obj, *args, **kwargs)

    with (
        mock.patch.object(config_module, "DATA_DIR", tmp_path),
        mock.patch.object(Path, "read_text", autospec=True, side_effect=replace_before_read),
    ):
        service = AccountService(TrackingStorage(accounts=[]))

    assert service._cumulative_total == 7


def test_account_cumulative_counter_uses_fixed_handle_after_path_replacement(tmp_path) -> None:
    cumulative_path = tmp_path / ".cumulative_total"
    replacement = tmp_path / "replacement"
    displaced = tmp_path / "displaced-total"
    cumulative_path.write_text("7", encoding="utf-8")
    replacement.write_text("99", encoding="utf-8")
    original_open = secure_file.open_no_follow_file

    def replace_after_open(path_obj, *args, **kwargs):
        opened = original_open(path_obj, *args, **kwargs)
        if path_obj == cumulative_path:
            cumulative_path.replace(displaced)
            replacement.replace(cumulative_path)
        return opened

    with (
        mock.patch.object(config_module, "DATA_DIR", tmp_path),
        mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_after_open),
    ):
        service = AccountService(TrackingStorage(accounts=[]))

    assert service._cumulative_total == 7


@pytest.mark.parametrize("stored_value", ("0", "-3"))
def test_account_cumulative_counter_never_loads_below_current_account_count(
    tmp_path,
    stored_value: str,
) -> None:
    (tmp_path / ".cumulative_total").write_text(stored_value, encoding="utf-8")
    storage = TrackingStorage(accounts=[{"access_token": "token-1"}])

    with mock.patch.object(config_module, "DATA_DIR", tmp_path):
        service = AccountService(storage)

    assert service._cumulative_total == 1


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

    with mock.patch.object(json_storage_module, "atomic_write_bytes", side_effect=OSError("replace failed")):
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

    with mock.patch.object(json_storage_module, "atomic_write_bytes", side_effect=OSError("replace failed")):
        with pytest.raises(OSError):
            backend.save_auth_keys([{"id": "new-key", "role": "user", "key_hash": "new-hash"}])

    assert json.loads(path.read_text(encoding="utf-8")) == {"items": original_items}
    assert not list(tmp_path.glob(".auth_keys.json.*.tmp"))


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_storage_rejects_dangling_snapshot_symlink_instead_of_treating_it_as_empty(tmp_path, kind) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    target = accounts_path if kind == "accounts" else auth_keys_path
    try:
        target.symlink_to(tmp_path / "missing-snapshot.json")
    except OSError as exc:
        pytest.skip(f"dangling symlink unavailable: {exc}")
    backend = JSONStorageBackend(accounts_path, auth_keys_path)

    loader = backend.load_accounts if kind == "accounts" else backend.load_auth_keys
    with pytest.raises(StorageDataError):
        loader()


@pytest.mark.parametrize("kind", ("accounts", "auth_keys"))
def test_json_storage_dangling_path_branch_fails_closed_without_symlink_support(tmp_path, kind) -> None:
    accounts_path = tmp_path / "accounts.json"
    auth_keys_path = tmp_path / "auth_keys.json"
    backend = JSONStorageBackend(accounts_path, auth_keys_path)
    loader = backend.load_accounts if kind == "accounts" else backend.load_auth_keys

    with (
        mock.patch.object(Path, "exists", return_value=False),
        mock.patch.object(Path, "is_symlink", return_value=True),
    ):
        with pytest.raises(StorageDataError):
            loader()
