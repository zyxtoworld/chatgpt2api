from __future__ import annotations

import json
import io
import tarfile
from pathlib import Path
from unittest import mock

import pytest
from git import Repo

import services.config as config_module
import services.account_service as account_service_module
import services.backup_service as backup_module
from services.account_service import AccountService
from services.backup_service import BackupService
from services.backup_service import BackupError
from services.storage.database_storage import DatabaseStorageBackend
from services.storage.base import StorageConflictError
from services.storage.git_storage import GitStorageBackend


def _seed_local_git(root: Path) -> tuple[Path, Repo]:
    remote_path = root / "remote.git"
    seed_path = root / "seed"
    remote = Repo.init(remote_path, bare=True)
    seed = Repo.init(seed_path)
    with seed.config_writer() as writer:
        writer.set_value("user", "name", "local-test")
        writer.set_value("user", "email", "local-test@example.test")
    seed.git.checkout("-b", "main")
    (seed_path / "accounts.json").write_text("[]\n", encoding="utf-8")
    (seed_path / "auth_keys.json").write_text('{"items": []}\n', encoding="utf-8")
    seed.index.add(["accounts.json", "auth_keys.json"])
    seed.index.commit("initial snapshot")
    seed.create_remote("origin", str(remote_path)).push("main")
    remote.git.symbolic_ref("HEAD", "refs/heads/main")
    remote.close()
    return remote_path, seed


@pytest.mark.parametrize("backend_kind", ("sqlite", "git"))
def test_cumulative_total_does_not_revert_after_committed_add_counter_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend_kind: str,
) -> None:
    monkeypatch.setattr(config_module, "DATA_DIR", tmp_path)

    seed = None
    if backend_kind == "sqlite":
        database_url = f"sqlite:///{tmp_path / 'accounts.db'}"
        backend = DatabaseStorageBackend(database_url)
        restart_backend = DatabaseStorageBackend(database_url)
    else:
        remote_path, seed = _seed_local_git(tmp_path)
        backend = GitStorageBackend(str(remote_path), "", local_cache_dir=tmp_path / "cache-1")
        restart_backend = GitStorageBackend(
            str(remote_path), "", local_cache_dir=tmp_path / "cache-2"
        )

    try:
        service = AccountService(backend)
        with mock.patch.object(
            account_service_module,
            "atomic_write_bytes",
            side_effect=OSError("counter write failed"),
        ):
            service.add_accounts(["historical-token"])

        # The accounts collection was already committed before the legacy
        # counter write failed.  Deleting it must not erase history on restart.
        AccountService(restart_backend).delete_accounts(["historical-token"])
        restarted = AccountService(restart_backend)
        assert restarted.get_stats()["cumulative_total"] == 1
    finally:
        backend.close()
        restart_backend.close()
        if seed is not None:
            seed.close()


@pytest.mark.parametrize("backend_kind", ("sqlite", "git"))
def test_stale_account_service_cannot_overwrite_newer_cumulative_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    backend_kind: str,
) -> None:
    monkeypatch.setattr(config_module, "DATA_DIR", tmp_path)

    seed = None
    if backend_kind == "sqlite":
        database_url = f"sqlite:///{tmp_path / 'accounts.db'}"
        backend_one = DatabaseStorageBackend(database_url)
        backend_two = DatabaseStorageBackend(database_url)
        backend_verify = DatabaseStorageBackend(database_url)
    else:
        remote_path, seed = _seed_local_git(tmp_path)
        backend_one = GitStorageBackend(str(remote_path), "", local_cache_dir=tmp_path / "cache-1")
        backend_two = GitStorageBackend(str(remote_path), "", local_cache_dir=tmp_path / "cache-2")
        backend_verify = GitStorageBackend(
            str(remote_path), "", local_cache_dir=tmp_path / "cache-3"
        )

    try:
        service_one = AccountService(backend_one)
        service_two = AccountService(backend_two)
        assert service_one.add_accounts(["newer-token"])["added"] == 1
        with pytest.raises(StorageConflictError):
            service_two.add_accounts(["stale-token"])

        verified = AccountService(backend_verify)
        assert verified.get_stats()["cumulative_total"] == 1
        assert [item["access_token"] for item in verified.list_accounts()] == ["newer-token"]
    finally:
        backend_one.close()
        backend_two.close()
        backend_verify.close()
        if seed is not None:
            seed.close()


@pytest.mark.parametrize("backend_kind", ("json", "sqlite", "git"))
def test_backup_account_snapshot_preserves_cumulative_metadata(
    tmp_path: Path,
    backend_kind: str,
) -> None:
    seed = None
    if backend_kind == "json":
        from services.storage.json_storage import JSONStorageBackend

        backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    elif backend_kind == "sqlite":
        backend = DatabaseStorageBackend(f"sqlite:///{tmp_path / 'accounts.db'}")
    else:
        remote_path, seed = _seed_local_git(tmp_path)
        backend = GitStorageBackend(str(remote_path), "", local_cache_dir=tmp_path / "cache")

    try:
        records = [{"access_token": "historical-token", "type": "free"}]
        backend.save_accounts_with_cumulative_total(
            backend.load_accounts_snapshot(), records, 5
        )
        with mock.patch.object(backup_module.config, "get_storage_backend", return_value=backend):
            payload = BackupService()._build_backup_archive(
                {"include": {"accounts_snapshot": True}},
                trigger="test",
            )
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            snapshot = json.loads(archive.extractfile("snapshots/accounts.json").read())
        assert snapshot == records
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            manifest = json.loads(archive.extractfile("backup-metadata.json").read())
        assert manifest["snapshot_manifest"] == {
            "version": 1,
            "accounts": {"cumulative_total": 5},
        }
        detail = BackupService()._decode_archive_detail(payload)
        assert detail["snapshots"] == [
            {"name": "accounts", "count": 1, "cumulative_total": 5}
        ]
    finally:
        backend.close()
        if seed is not None:
            seed.close()


def test_backup_detail_keeps_legacy_list_snapshot_compatibility() -> None:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        snapshot = json.dumps([{"access_token": "legacy-token"}]).encode("utf-8")
        info = tarfile.TarInfo("snapshots/accounts.json")
        info.size = len(snapshot)
        archive.addfile(info, io.BytesIO(snapshot))

    detail = BackupService()._decode_archive_detail(payload.getvalue())
    assert detail["snapshots"] == [{"name": "accounts", "count": 1}]


def test_backup_detail_rejects_malformed_cumulative_snapshot_metadata() -> None:
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        snapshot = json.dumps({"items": [], "cumulative_total": "bad"}).encode("utf-8")
        info = tarfile.TarInfo("snapshots/accounts.json")
        info.size = len(snapshot)
        archive.addfile(info, io.BytesIO(snapshot))

    with pytest.raises(BackupError) as raised:
        BackupService()._decode_archive_detail(payload.getvalue())
    assert raised.value.code == "backup_archive_invalid"
