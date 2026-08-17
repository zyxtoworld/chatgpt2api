from __future__ import annotations

import json
import gc
import os
import tempfile
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from git import Repo
from git.remote import PushInfo, Remote

from services.storage.base import StorageConflictError, StorageDataError, make_storage_snapshot
import services.storage.git_storage as git_storage_module
from services.storage.git_storage import GitStorageBackend


EXPECTED_GIT_OPERATION_TIMEOUT_SECS = 30.0


class GitStorageRecoveryContractTests(unittest.TestCase):
    @staticmethod
    def _configure_git(repo: Repo) -> None:
        with repo.config_writer() as writer:
            writer.set_value("user", "name", "local-test")
            writer.set_value("user", "email", "local-test@example.test")

    def _new_backend(self, root: Path) -> tuple[GitStorageBackend, Repo, Path]:
        remote_path = root / "remote.git"
        seed_path = root / "seed"
        remote = Repo.init(remote_path, bare=True)
        seed = Repo.init(seed_path)
        self._configure_git(seed)
        seed.git.checkout("-b", "main")
        (seed_path / "accounts.json").write_text("[]\n", encoding="utf-8")
        (seed_path / "auth_keys.json").write_text('{"items": []}\n', encoding="utf-8")
        seed.index.add(["accounts.json", "auth_keys.json"])
        seed.index.commit("initial snapshot")
        origin = seed.create_remote("origin", str(remote_path))
        origin.push("main")
        remote.git.symbolic_ref("HEAD", "refs/heads/main")
        remote.close()
        cache_path = root / "cache"
        backend = GitStorageBackend(
            str(remote_path),
            "",
            branch="main",
            local_cache_dir=cache_path,
        )
        return backend, seed, remote_path

    def test_git_secret_document_write_passes_worktree_identity_to_secure_writer(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, _remote_path = self._new_backend(root)
            original_atomic_write = git_storage_module.atomic_write_bytes
            observed: list[tuple[Path, Path, tuple[int, int] | None]] = []

            def observe_atomic_write(path: Path, owner_root: Path, payload: bytes, **kwargs: object) -> None:
                if path.name == "auth_keys.json":
                    observed.append((path, owner_root, kwargs.get("expected_root_identity")))
                original_atomic_write(path, owner_root, payload, **kwargs)

            try:
                with mock.patch.object(git_storage_module, "atomic_write_bytes", side_effect=observe_atomic_write):
                    backend.save_auth_keys([{"id": "key-1", "role": "user", "key_hash": "hash"}])
                self.assertEqual(len(observed), 1)
                worktree_root = observed[0][1]
                self.assertEqual(observed[0][2], (worktree_root.stat().st_dev, worktree_root.stat().st_ino))
            finally:
                seed.close()

    def test_git_auth_write_rejects_worktree_rebind_before_secure_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, _remote_path = self._new_backend(root)
            displaced = root / "displaced-worktree"
            foreign = root / "foreign-worktree"
            foreign.mkdir()
            foreign_auth = {"items": [{"id": "foreign-key"}]}
            (foreign / "auth_keys.json").write_text(
                json.dumps(foreign_auth) + "\n",
                encoding="utf-8",
            )
            original_atomic_write = git_storage_module.atomic_write_bytes
            observed_foreign: list[object] = []
            observed_displaced: list[object] = []

            def rebind_before_write(path: Path, owner_root: Path, payload: bytes, **kwargs: object) -> None:
                if path.name != "auth_keys.json":
                    original_atomic_write(path, owner_root, payload, **kwargs)
                    return
                os.replace(owner_root, displaced)
                os.replace(foreign, owner_root)
                observed_displaced.append(
                    json.loads((displaced / "auth_keys.json").read_text(encoding="utf-8"))
                )
                try:
                    original_atomic_write(path, owner_root, payload, **kwargs)
                finally:
                    observed_foreign.append(
                        json.loads((owner_root / "auth_keys.json").read_text(encoding="utf-8"))
                    )

            try:
                with mock.patch.object(git_storage_module, "atomic_write_bytes", side_effect=rebind_before_write):
                    with self.assertRaises(StorageDataError):
                        backend.save_auth_keys([{"id": "new-key", "role": "user", "key_hash": "hash"}])
            finally:
                seed.close()

            self.assertEqual(observed_foreign, [foreign_auth])
            self.assertEqual(observed_displaced, [{"items": []}])

    def test_cached_git_pull_has_a_hard_process_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            repo_path = root / "cache" / "repo"
            (repo_path / ".git").mkdir(parents=True)
            origin = mock.Mock()
            fake_repo = mock.Mock()
            fake_repo.remote.return_value = origin
            with (
                mock.patch.object(git_storage_module.sys, "platform", "linux"),
                mock.patch.object(git_storage_module, "Repo", return_value=fake_repo),
            ):
                backend = GitStorageBackend(
                    "https://example.test/repo.git",
                    "",
                    branch="main",
                    local_cache_dir=root / "cache",
                )
                backend._clone_or_pull()

            origin.pull.assert_called_once_with(
                "main",
                kill_after_timeout=EXPECTED_GIT_OPERATION_TIMEOUT_SECS,
            )

    def test_initial_git_clone_has_a_hard_process_timeout(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            clone_path = root / "cache" / ".repo.clone.expected.tmp"
            fake_repo = mock.Mock()
            with (
                mock.patch.object(git_storage_module.uuid, "uuid4", return_value=mock.Mock(hex="expected")),
                mock.patch.object(git_storage_module.sys, "platform", "linux"),
                mock.patch.object(git_storage_module, "Repo", fake_repo),
            ):
                def fake_clone_from(*_args, **_kwargs):
                    clone_path.mkdir(parents=True)
                    return mock.Mock()

                fake_repo.clone_from.side_effect = fake_clone_from
                backend = GitStorageBackend(
                    "https://example.test/repo.git",
                    "",
                    branch="main",
                    local_cache_dir=root / "cache",
                )
                backend._clone_or_pull()

            fake_repo.clone_from.assert_called_once_with(
                backend.auth_repo_url,
                clone_path,
                branch="main",
                kill_after_timeout=EXPECTED_GIT_OPERATION_TIMEOUT_SECS,
            )

    def test_repo_snapshot_paths_must_stay_inside_worktree(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            for field in ("file_path", "auth_keys_file_path"):
                with self.subTest(field=field):
                    with self.assertRaises(ValueError):
                        GitStorageBackend(
                            str(root / "remote.git"),
                            "",
                            local_cache_dir=root / "cache",
                            **{field: "../outside.json"},
                        )

    def _assert_git_direct_save_cannot_enter_between_cas_read_and_commit(self, kind: str) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend = GitStorageBackend(
                str(root / "remote.git"),
                "",
                branch="main",
                local_cache_dir=root / "cache",
            )
            if kind == "accounts":
                initial = [{"access_token": "token-old", "name": "old"}]
                cas_value = [{"access_token": "token-cas", "name": "cas"}]
                direct_value = [{"access_token": "token-direct", "name": "direct"}]
                save = backend.save_accounts
                save_if_revision = backend.save_accounts_if_revision
            else:
                initial = [{"id": "key-old", "role": "user", "key_hash": "hash-old", "enabled": True}]
                cas_value = [{"id": "key-cas", "role": "user", "key_hash": "hash-cas", "enabled": True}]
                direct_value = [{"id": "key-direct", "role": "user", "key_hash": "hash-direct", "enabled": True}]
                save = backend.save_auth_keys
                save_if_revision = backend.save_auth_keys_if_revision
            expected = make_storage_snapshot(initial)
            read_ready = threading.Event()
            release_read = threading.Event()
            direct_called = threading.Event()
            direct_entered_repository = threading.Event()
            release_direct = threading.Event()
            errors: list[BaseException] = []
            direct_thread_id: list[int] = []

            worktree = root / "fake-worktree"
            worktree.mkdir()
            (worktree / "accounts.json").write_text(
                json.dumps(initial) + "\n",
                encoding="utf-8",
            )
            (worktree / "auth_keys.json").write_text(
                json.dumps({"items": initial}) + "\n",
                encoding="utf-8",
            )

            class FakeIndex:
                def add(self, _paths):
                    return None

                def commit(self, _message):
                    return None

            class FakeRepo:
                working_dir = str(worktree)
                index = FakeIndex()

                @staticmethod
                def is_dirty() -> bool:
                    return False

                @staticmethod
                def close() -> None:
                    return None

            def fake_clone():
                if threading.get_ident() == direct_thread_id[0]:
                    direct_entered_repository.set()
                    if not release_direct.wait(2):
                        raise AssertionError("direct save was not released")
                return FakeRepo()

            def gated_snapshot():
                read_ready.set()
                if not release_read.wait(2):
                    raise AssertionError("CAS read was not released")
                return expected

            if kind == "accounts":
                def gated_accounts_document():
                    read_ready.set()
                    if not release_read.wait(2):
                        raise AssertionError("CAS read was not released")
                    return expected.records, None

                backend._load_accounts_document = gated_accounts_document
            else:
                backend.load_auth_keys_snapshot = gated_snapshot

            def cas_writer() -> None:
                try:
                    save_if_revision(expected, cas_value)
                except BaseException as exc:
                    errors.append(exc)

            def direct_writer() -> None:
                direct_thread_id.append(threading.get_ident())
                direct_called.set()
                try:
                    save(direct_value)
                except BaseException as exc:
                    errors.append(exc)

            with mock.patch.object(backend, "_clone_or_pull", side_effect=fake_clone):
                cas_thread = threading.Thread(target=cas_writer)
                direct_thread = threading.Thread(target=direct_writer)
                cas_thread.start()
                self.assertTrue(read_ready.wait(2))
                direct_thread.start()
                try:
                    self.assertTrue(direct_called.wait(2))
                    self.assertFalse(
                        direct_entered_repository.wait(0.2),
                        "direct Git save bypassed the CAS scope lock",
                    )
                finally:
                    release_read.set()
                    release_direct.set()
                    cas_thread.join(2)
                    direct_thread.join(2)

            self.assertFalse(cas_thread.is_alive())
            self.assertFalse(direct_thread.is_alive())
            self.assertEqual(errors, [])

    def test_git_direct_account_save_cannot_enter_between_cas_read_and_commit(self) -> None:
        self._assert_git_direct_save_cannot_enter_between_cas_read_and_commit("accounts")

    def test_git_direct_auth_key_save_cannot_enter_between_cas_read_and_commit(self) -> None:
        self._assert_git_direct_save_cannot_enter_between_cas_read_and_commit("auth_keys")

    @unittest.skipUnless(os.name == "posix", "requires POSIX symlink support")
    def test_existing_local_cache_symlink_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            foreign = root / "foreign"
            foreign.mkdir()
            cache = root / "cache"
            cache.symlink_to(foreign, target_is_directory=True)
            with self.assertRaises(OSError):
                GitStorageBackend(
                    str(root / "remote.git"),
                    "",
                    local_cache_dir=cache,
                )

    @unittest.skipUnless(os.name == "posix", "requires POSIX symlink support")
    def test_existing_backend_rejects_cache_root_rebound_before_load(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, _remote_path = self._new_backend(root)
            try:
                self.assertEqual(backend.load_accounts(), [])
                cache = root / "cache"
                displaced = root / "cache-displaced"
                foreign = root / "foreign"
                foreign.mkdir()
                cache.rename(displaced)
                cache.symlink_to(foreign, target_is_directory=True)

                with self.assertRaises(OSError):
                    backend.load_accounts()

                self.assertFalse((foreign / "repo").exists())
                self.assertTrue((displaced / "repo").exists())
            finally:
                seed.close()

    def _assert_failed_push_recovers_remote_state(
        self,
        *,
        save_name: str,
        load_name: str,
        file_name: str,
        payload: list[dict[str, object]],
        remote_payload: object,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with ExitStack() as resources:
                resources.callback(gc.collect)
                root = Path(temp_dir)
                backend, seed, remote_path = self._new_backend(root)
                resources.callback(seed.close)
                self.assertEqual(getattr(backend, load_name)(), [])

                remote_advance = {"done": False}

                def advance_remote() -> None:
                    seed_path = Path(seed.working_tree_dir or "")
                    (seed_path / "remote-marker.txt").write_text(
                        "remote advance\n",
                        encoding="utf-8",
                    )
                    seed.index.add(["remote-marker.txt"])
                    seed.index.commit("remote concurrent advance")
                    seed.remote("origin").push("main")
                    remote_advance["done"] = True

                backend_repo_path = (root / "cache" / "repo").resolve()
                original_push = Remote.push

                def reject_after_remote_advance(remote: Remote, *args, **kwargs):
                    if (
                        Path(remote.repo.working_dir).resolve() == backend_repo_path
                        and not remote_advance["done"]
                    ):
                        advance_remote()
                    return original_push(remote, *args, **kwargs)

                with (
                    mock.patch.object(Remote, "push", new=reject_after_remote_advance),
                    self.assertRaises(StorageDataError),
                ):
                    getattr(backend, save_name)(payload)

                self.assertTrue(remote_advance["done"])
                self.assertFalse((root / "cache" / ".pending-push").exists())
                self.assertEqual(getattr(backend, load_name)(), [])
                self.assertEqual(backend.health_check()["status"], "healthy")

                rebuilt = GitStorageBackend(
                    str(remote_path),
                    "",
                    branch="main",
                    local_cache_dir=root / "cache",
                )
                self.assertEqual(getattr(rebuilt, load_name)(), [])

                getattr(rebuilt, save_name)(payload)
                verify_path = root / "verify"
                verify = Repo.clone_from(str(remote_path), verify_path, branch="main")
                resources.callback(verify.close)
                stored = json.loads((verify_path / file_name).read_text(encoding="utf-8"))
                self.assertEqual(stored, remote_payload)
                self.assertEqual((verify_path / "remote-marker.txt").read_text(encoding="utf-8"), "remote advance\n")

    def test_accounts_push_rejection_never_becomes_local_only_success(self) -> None:
        payload = [{"access_token": "local-only-account-token"}]
        self._assert_failed_push_recovers_remote_state(
            save_name="save_accounts",
            load_name="load_accounts",
            file_name="accounts.json",
            payload=payload,
            remote_payload=payload,
        )

    def test_auth_keys_push_rejection_never_becomes_local_only_success(self) -> None:
        payload = [{"id": "key-1", "key_hash": "hash-1", "role": "user"}]
        self._assert_failed_push_recovers_remote_state(
            save_name="save_auth_keys",
            load_name="load_auth_keys",
            file_name="auth_keys.json",
            payload=payload,
            remote_payload={"items": payload},
        )

    def test_cumulative_cas_rechecks_remote_after_advance_before_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, remote_path = self._new_backend(root)
            try:
                expected = backend.load_accounts_snapshot()
                entered_save = threading.Event()
                release_save = threading.Event()
                save_error: list[BaseException] = []
                original_save_json = backend._save_json_file

                def gated_save_json(
                    file_path: str,
                    value: object,
                    message: str,
                    **kwargs: object,
                ) -> None:
                    entered_save.set()
                    if not release_save.wait(timeout=10):
                        raise AssertionError("timed out waiting for remote advance")
                    original_save_json(file_path, value, message, **kwargs)

                def run_save() -> None:
                    try:
                        backend.save_accounts_with_cumulative_total(
                            expected,
                            [{"access_token": "stale-writer"}],
                            1,
                        )
                    except BaseException as exc:
                        save_error.append(exc)

                with mock.patch.object(backend, "_save_json_file", side_effect=gated_save_json):
                    worker = threading.Thread(target=run_save)
                    worker.start()
                    self.assertTrue(entered_save.wait(timeout=10))

                    seed_path = Path(seed.working_tree_dir or "")
                    (seed_path / "accounts.json").write_text(
                        json.dumps(
                            {
                                "items": [{"access_token": "newer-writer"}],
                                "cumulative_total": 1,
                            }
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    seed.index.add(["accounts.json"])
                    seed.index.commit("concurrent remote writer")
                    seed.remote("origin").push("main")
                    release_save.set()
                    worker.join(timeout=10)

                self.assertFalse(worker.is_alive())
                self.assertEqual(len(save_error), 1)
                self.assertIsInstance(save_error[0], StorageConflictError)

                verify_path = root / "verify-cumulative-cas"
                verify = Repo.clone_from(str(remote_path), verify_path, branch="main")
                try:
                    self.assertEqual(
                        json.loads((verify_path / "accounts.json").read_text(encoding="utf-8")),
                        {
                            "items": [{"access_token": "newer-writer"}],
                            "cumulative_total": 1,
                        },
                    )
                finally:
                    verify.close()
            finally:
                backend.close()
                seed.close()

    def test_cumulative_cas_rejects_remote_total_advance_with_same_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, remote_path = self._new_backend(root)
            try:
                expected = backend.load_accounts_snapshot()
                entered_save = threading.Event()
                release_save = threading.Event()
                save_error: list[BaseException] = []
                original_save_json = backend._save_json_file

                def gated_save_json(
                    file_path: str,
                    value: object,
                    message: str,
                    **kwargs: object,
                ) -> None:
                    entered_save.set()
                    if not release_save.wait(timeout=10):
                        raise AssertionError("timed out waiting for remote advance")
                    original_save_json(file_path, value, message, **kwargs)

                def run_save() -> None:
                    try:
                        backend.save_accounts_with_cumulative_total(
                            expected,
                            [{"access_token": "late-account"}],
                            1,
                        )
                    except BaseException as exc:
                        save_error.append(exc)

                with mock.patch.object(backend, "_save_json_file", side_effect=gated_save_json):
                    worker = threading.Thread(target=run_save)
                    worker.start()
                    self.assertTrue(entered_save.wait(timeout=10))

                    seed_path = Path(seed.working_tree_dir or "")
                    (seed_path / "accounts.json").write_text(
                        json.dumps({"items": [], "cumulative_total": 2}) + "\n",
                        encoding="utf-8",
                    )
                    seed.index.add(["accounts.json"])
                    seed.index.commit("concurrent cumulative-only writer")
                    seed.remote("origin").push("main")
                    release_save.set()
                    worker.join(timeout=10)

                self.assertFalse(worker.is_alive())
                self.assertEqual(len(save_error), 1)
                self.assertIsInstance(save_error[0], StorageConflictError)

                verify_path = root / "verify-cumulative-only-cas"
                verify = Repo.clone_from(str(remote_path), verify_path, branch="main")
                try:
                    self.assertEqual(
                        json.loads((verify_path / "accounts.json").read_text(encoding="utf-8")),
                        {"items": [], "cumulative_total": 2},
                    )
                finally:
                    verify.close()
            finally:
                backend.close()
                seed.close()

    def test_auth_keys_cas_rechecks_remote_after_advance_before_publish(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, remote_path = self._new_backend(root)
            try:
                expected = backend.load_auth_keys_snapshot()
                entered_save = threading.Event()
                release_save = threading.Event()
                save_error: list[BaseException] = []
                original_save_json = backend._save_json_file

                def gated_save_json(
                    file_path: str,
                    value: object,
                    message: str,
                    **kwargs: object,
                ) -> None:
                    entered_save.set()
                    if not release_save.wait(timeout=10):
                        raise AssertionError("timed out waiting for remote advance")
                    original_save_json(file_path, value, message, **kwargs)

                def run_save() -> None:
                    try:
                        backend.save_auth_keys_if_revision(
                            expected,
                            [{"id": "stale-key", "key_hash": "stale", "role": "user"}],
                        )
                    except BaseException as exc:
                        save_error.append(exc)

                with mock.patch.object(backend, "_save_json_file", side_effect=gated_save_json):
                    worker = threading.Thread(target=run_save)
                    worker.start()
                    self.assertTrue(entered_save.wait(timeout=10))

                    seed_path = Path(seed.working_tree_dir or "")
                    (seed_path / "auth_keys.json").write_text(
                        json.dumps(
                            {
                                "items": [
                                    {"id": "remote-key", "key_hash": "remote", "role": "admin"}
                                ]
                            }
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    seed.index.add(["auth_keys.json"])
                    seed.index.commit("concurrent remote auth-key writer")
                    seed.remote("origin").push("main")
                    release_save.set()
                    worker.join(timeout=10)

                self.assertFalse(worker.is_alive())
                self.assertEqual(len(save_error), 1)
                self.assertIsInstance(save_error[0], StorageConflictError)

                verify_path = root / "verify-auth-cas"
                verify = Repo.clone_from(str(remote_path), verify_path, branch="main")
                try:
                    self.assertEqual(
                        json.loads((verify_path / "auth_keys.json").read_text(encoding="utf-8")),
                        {
                            "items": [
                                {"id": "remote-key", "key_hash": "remote", "role": "admin"}
                            ]
                        },
                    )
                finally:
                    verify.close()
            finally:
                backend.close()
                seed.close()

    def test_cumulative_push_rejection_reconciles_before_restart_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, remote_path = self._new_backend(root)
            try:
                expected = backend.load_accounts_snapshot()
                remote_advance = {"done": False}

                def advance_remote() -> None:
                    seed_path = Path(seed.working_tree_dir or "")
                    (seed_path / "accounts.json").write_text(
                        json.dumps(
                            {
                                "items": [{"access_token": "remote-writer"}],
                                "cumulative_total": 1,
                            }
                        )
                        + "\n",
                        encoding="utf-8",
                    )
                    seed.index.add(["accounts.json"])
                    seed.index.commit("concurrent remote cumulative writer")
                    seed.remote("origin").push("main")
                    remote_advance["done"] = True

                backend_repo_path = (root / "cache" / "repo").resolve()
                original_push = Remote.push

                def reject_after_remote_advance(remote: Remote, *args, **kwargs):
                    if (
                        Path(remote.repo.working_dir).resolve() == backend_repo_path
                        and not remote_advance["done"]
                    ):
                        advance_remote()
                    return original_push(remote, *args, **kwargs)

                with (
                    mock.patch.object(Remote, "push", new=reject_after_remote_advance),
                    self.assertRaises(StorageDataError),
                ):
                    backend.save_accounts_with_cumulative_total(
                        expected,
                        [{"access_token": "local-writer"}],
                        1,
                    )

                self.assertTrue(remote_advance["done"])
                self.assertEqual(backend.load_accounts(), [{"access_token": "remote-writer"}])
                self.assertEqual(backend.load_cumulative_total(), 1)

                rebuilt = GitStorageBackend(
                    str(remote_path),
                    "",
                    branch="main",
                    local_cache_dir=root / "cache",
                )
                self.assertEqual(rebuilt.load_accounts(), [{"access_token": "remote-writer"}])
                self.assertEqual(rebuilt.load_cumulative_total(), 1)

                rebuilt.save_accounts_with_cumulative_total(
                    rebuilt.load_accounts_snapshot(),
                    [{"access_token": "retry-writer"}],
                    2,
                )
                verify_path = root / "verify-cumulative-retry"
                verify = Repo.clone_from(str(remote_path), verify_path, branch="main")
                try:
                    self.assertEqual(
                        json.loads((verify_path / "accounts.json").read_text(encoding="utf-8")),
                        {
                            "items": [{"access_token": "retry-writer"}],
                            "cumulative_total": 2,
                        },
                    )
                finally:
                    verify.close()
            finally:
                backend.close()
                seed.close()

    def test_save_rejects_non_object_records_before_git_commit(self) -> None:
        for kind in ("accounts", "auth_keys"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                backend, seed, _remote_path = self._new_backend(root)
                try:
                    valid = (
                        {"access_token": "existing-account"}
                        if kind == "accounts"
                        else {"id": "existing-key", "role": "user", "key_hash": "hash"}
                    )
                    save = backend.save_accounts if kind == "accounts" else backend.save_auth_keys
                    load = backend.load_accounts if kind == "accounts" else backend.load_auth_keys
                    save([valid])

                    with self.assertRaises(StorageDataError):
                        save([valid, "not-an-object"])

                    self.assertEqual(load(), [valid])
                finally:
                    seed.close()

    def test_save_rejects_nonfinite_json_values_before_git_commit(self) -> None:
        import math

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, _remote_path = self._new_backend(root)
            try:
                cases = (
                    (backend.save_accounts, backend.load_accounts,
                     [{"access_token": "existing-account", "quota": 1}],
                     [{"access_token": "existing-account", "quota": math.nan}]),
                    (backend.save_auth_keys, backend.load_auth_keys,
                     [{"id": "existing-key", "role": "user", "key_hash": "hash"}],
                     [{"id": "existing-key", "role": "user", "key_hash": "hash", "metadata": math.inf}]),
                )
                for save, load, valid, invalid in cases:
                    save(valid)
                    with self.assertRaises(StorageDataError):
                        save(invalid)
                    self.assertEqual(load(), valid)
            finally:
                seed.close()

    def test_pending_push_marker_fails_closed_until_remote_recovery(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with ExitStack() as resources:
                resources.callback(gc.collect)
                root = Path(temp_dir)
                backend, seed, remote_path = self._new_backend(root)
                resources.callback(seed.close)
                self.assertEqual(backend.load_accounts(), [])
                payload = [{"access_token": "pending-only-account-token"}]
                remote_advance = {"done": False}

                def advance_remote() -> None:
                    seed_path = Path(seed.working_tree_dir or "")
                    (seed_path / "remote-marker.txt").write_text("remote advance\n", encoding="utf-8")
                    seed.index.add(["remote-marker.txt"])
                    seed.index.commit("remote concurrent advance")
                    seed.remote("origin").push("main")
                    remote_advance["done"] = True

                backend_repo_path = (root / "cache" / "repo").resolve()
                original_push = Remote.push

                def reject_after_remote_advance(remote: Remote, *args, **kwargs):
                    if (
                        Path(remote.repo.working_dir).resolve() == backend_repo_path
                        and not remote_advance["done"]
                    ):
                        advance_remote()
                    return original_push(remote, *args, **kwargs)

                rebuilt = GitStorageBackend(
                    str(remote_path),
                    "",
                    branch="main",
                    local_cache_dir=root / "cache",
                )
                with (
                    mock.patch.object(Remote, "push", new=reject_after_remote_advance),
                    mock.patch.object(
                        GitStorageBackend,
                        "_restore_repo_from_remote",
                        side_effect=RuntimeError("remote recovery unavailable"),
                    ),
                    self.assertRaises(StorageDataError),
                ):
                    backend.save_accounts(payload)

                marker = root / "cache" / ".pending-push"
                self.assertTrue(remote_advance["done"])
                self.assertTrue(marker.exists())
                self.assertEqual(
                    json.loads((root / "cache" / "repo" / "accounts.json").read_text(encoding="utf-8")),
                    payload,
                )

                with mock.patch.object(
                    GitStorageBackend,
                    "_restore_repo_from_remote",
                    side_effect=RuntimeError("remote recovery unavailable"),
                ):
                    for candidate in (backend, rebuilt):
                        for loader in (candidate.load_accounts, candidate.load_auth_keys):
                            with self.assertRaises(StorageDataError):
                                loader()
                        for saver, value in (
                            (candidate.save_accounts, payload),
                            (candidate.save_auth_keys, [{"id": "key-1", "key_hash": "hash-1", "role": "user"}]),
                        ):
                            with self.assertRaises(StorageDataError):
                                saver(value)
                        self.assertEqual(candidate.health_check()["status"], "unhealthy")

                self.assertEqual(backend.load_accounts(), [])
                self.assertFalse(marker.exists())

                push_calls: list[Remote] = []

                def record_push(remote: Remote, *args, **kwargs):
                    push_calls.append(remote)
                    return original_push(remote, *args, **kwargs)

                with mock.patch.object(Remote, "push", new=record_push):
                    backend.save_accounts(payload)
                self.assertEqual(len(push_calls), 1)

                verify_path = root / "verify-pending"
                verify = Repo.clone_from(str(remote_path), verify_path, branch="main")
                resources.callback(verify.close)
                self.assertEqual(
                    json.loads((verify_path / "accounts.json").read_text(encoding="utf-8")),
                    payload,
                )

    def test_empty_push_result_recovers_and_never_reports_success(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with ExitStack() as resources:
                resources.callback(gc.collect)
                root = Path(temp_dir)
                backend, seed, remote_path = self._new_backend(root)
                resources.callback(seed.close)
                self.assertEqual(backend.load_accounts(), [])
                payload = [{"access_token": "empty-push-result-token"}]

                with (
                    mock.patch.object(Remote, "push", return_value=[]),
                    self.assertRaises(StorageDataError),
                ):
                    backend.save_accounts(payload)

                self.assertFalse((root / "cache" / ".pending-push").exists())
                self.assertEqual(backend.load_accounts(), [])
                verify_path = root / "verify-empty-push"
                verify = Repo.clone_from(str(remote_path), verify_path, branch="main")
                resources.callback(verify.close)
                self.assertEqual(
                    json.loads((verify_path / "accounts.json").read_text(encoding="utf-8")),
                    [],
                )

    def test_pending_push_marker_without_repo_never_reclones(self) -> None:
        for repo_state in ("missing", "corrupt"):
            with self.subTest(repo_state=repo_state), tempfile.TemporaryDirectory() as temp_dir:
                with ExitStack() as resources:
                    resources.callback(gc.collect)
                    root = Path(temp_dir)
                    backend, seed, _remote_path = self._new_backend(root)
                    resources.callback(seed.close)
                    repo_path = root / "cache" / "repo"
                    marker = root / "cache" / ".pending-push"
                    if repo_state == "corrupt":
                        repo_path.mkdir(parents=True, exist_ok=True)
                        (repo_path / "not-a-git-repository").write_text("sentinel", encoding="utf-8")
                    else:
                        self.assertFalse(repo_path.exists())
                    marker.write_text("pending\n", encoding="utf-8")
                    with (
                        mock.patch.object(
                            Repo,
                            "clone_from",
                            side_effect=AssertionError("pending cache must not be re-cloned"),
                        ),
                        self.assertRaises(StorageDataError),
                    ):
                        backend.load_accounts()
                self.assertTrue(marker.exists())

    @unittest.skipUnless(os.name == "posix", "requires POSIX symlink support")
    def test_pending_push_marker_symlink_fails_closed_without_foreign_write(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            backend, seed, _remote_path = self._new_backend(root)
            try:
                self.assertEqual(backend.load_accounts(), [])
                marker = root / "cache" / ".pending-push"
                foreign = root / "foreign-marker"
                foreign.write_text("foreign-sentinel\n", encoding="utf-8")
                original_clone = backend._clone_or_pull

                def clone_then_rebind_marker():
                    repo = original_clone()
                    marker.symlink_to(foreign)
                    return repo

                with (
                    mock.patch.object(backend, "_clone_or_pull", side_effect=clone_then_rebind_marker),
                    self.assertRaises(StorageDataError),
                ):
                    backend.save_accounts([{"access_token": "new-token"}])

                self.assertEqual(foreign.read_text(encoding="utf-8"), "foreign-sentinel\n")
                self.assertFalse(marker.exists())
            finally:
                seed.close()

    def test_push_result_failure_flags_are_fail_closed(self) -> None:
        for flag in (
            PushInfo.ERROR,
            PushInfo.REJECTED,
            PushInfo.REMOTE_REJECTED,
            PushInfo.REMOTE_FAILURE,
        ):
            with self.subTest(flag=flag):
                self.assertTrue(
                    GitStorageBackend._push_result_failed([SimpleNamespace(flags=flag)])
                )
        for flag in (PushInfo.NEW_HEAD, PushInfo.FAST_FORWARD, PushInfo.UP_TO_DATE):
            with self.subTest(flag=flag):
                self.assertFalse(
                    GitStorageBackend._push_result_failed([SimpleNamespace(flags=flag)])
                )
        with self.assertRaises(StorageDataError):
            GitStorageBackend._push_result_failed([])
        with self.assertRaises(StorageDataError):
            GitStorageBackend._push_result_failed(None)


if __name__ == "__main__":
    unittest.main()
