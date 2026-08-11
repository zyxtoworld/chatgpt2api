from __future__ import annotations

import json
import gc
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from git import Repo
from git.remote import PushInfo, Remote

from services.storage.base import StorageDataError
from services.storage.git_storage import GitStorageBackend


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
