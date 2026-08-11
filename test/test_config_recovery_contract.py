from __future__ import annotations

import json
import os
import stat
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

import services.config as config_module
import services.secure_file as secure_file
from services.config import ConfigStore
from services.storage.base import StorageConflictError, StorageDataError


class ConfigRecoveryContractTests(unittest.TestCase):
    def test_concurrent_config_updates_preserve_unrelated_fields(self) -> None:
        copy_barrier = threading.Barrier(2)
        save_barrier = threading.Barrier(2)

        class BarrierValue:
            def __deepcopy__(self, _memo):
                try:
                    copy_barrier.wait(timeout=0.25)
                except threading.BrokenBarrierError:
                    pass
                return "stable"

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                store = ConfigStore(path)
                store.data["barrier"] = BarrierValue()
                original_save = store._save

                def save_with_barrier(*args, **kwargs):
                    try:
                        save_barrier.wait(timeout=0.25)
                    except threading.BrokenBarrierError:
                        pass
                    return original_save(*args, **kwargs)

                with (
                    mock.patch.object(store, "_save", side_effect=save_with_barrier),
                    ThreadPoolExecutor(max_workers=2) as executor,
                ):
                    first = executor.submit(store.update, {"alpha": 1})
                    second = executor.submit(store.update, {"beta": 2})
                    first.result(timeout=5)
                    second.result(timeout=5)

            persisted = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["alpha"], 1)
            self.assertEqual(persisted["beta"], 2)
            self.assertEqual(store.data["alpha"], 1)
            self.assertEqual(store.data["beta"], 2)

    def test_existing_corrupt_config_is_not_loaded_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text("{broken", encoding="utf-8")

            with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                with self.assertRaises(StorageDataError):
                    ConfigStore(path)

    def test_missing_config_with_environment_auth_key_is_created_safely(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                store = ConfigStore(path)
                store.update({"proxy": "http://proxy.example"})
                self.assertEqual(store.auth_key, "env-auth")

            persisted = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["proxy"], "http://proxy.example")
            if os.name == "posix":
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
            self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))

    def test_config_mutation_does_not_overwrite_corrupt_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                store = ConfigStore(path)
                path.write_text("{opaque-corruption", encoding="utf-8")
                original = path.read_bytes()

                with self.assertRaises(StorageDataError):
                    store.update({"proxy": "http://proxy.example"})

            self.assertEqual(path.read_bytes(), original)
            self.assertNotIn("proxy", store.data)

    def test_config_replace_failure_preserves_snapshot_and_cleans_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                store = ConfigStore(path)
                original = path.read_bytes()

                failure_patch = (
                    mock.patch.object(secure_file.os, "replace", side_effect=OSError("replace failed"))
                    if os.name == "posix"
                    else mock.patch.object(
                        secure_file,
                        "_atomic_write_windows",
                        side_effect=OSError("replace failed"),
                    )
                )
                with failure_patch:
                    with self.assertRaises(OSError):
                        store.update({"proxy": "http://proxy.example"})

            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))
            self.assertNotIn("proxy", store.data)

    @unittest.skipUnless(os.name == "posix", "requires POSIX directory fsync")
    def test_config_save_fsyncs_parent_directory_after_replace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            events: list[tuple[str, str | None]] = []
            original_fsync = os.fsync
            original_replace = os.replace

            def record_fsync(fd: int) -> None:
                kind = "directory" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file"
                events.append(("fsync", kind))
                original_fsync(fd)

            def record_replace(source: str, target: str, *args, **kwargs) -> None:
                events.append(("replace", None))
                return original_replace(source, target, *args, **kwargs)

            with (
                mock.patch.object(secure_file.os, "fsync", side_effect=record_fsync),
                mock.patch.object(secure_file.os, "replace", side_effect=record_replace),
                mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False),
            ):
                ConfigStore(path).update({"proxy": "http://proxy.example"})

            replace_index = events.index(("replace", None))
            self.assertTrue(
                any(index > replace_index and event == ("fsync", "directory") for index, event in enumerate(events))
            )

    @unittest.skipUnless(os.name == "posix", "requires POSIX directory fsync")
    def test_config_parent_fsync_failure_is_reported_and_enters_conflict_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                store = ConfigStore(path)
                original_revision = store._snapshot_revision
                original_fsync = os.fsync

                def fail_directory_fsync(fd: int) -> None:
                    if stat.S_ISDIR(os.fstat(fd).st_mode):
                        raise OSError("parent directory fsync failed")
                    original_fsync(fd)

                with mock.patch.object(secure_file.os, "fsync", side_effect=fail_directory_fsync):
                    with self.assertRaises(OSError):
                        store.update({"proxy": "http://proxy.example"})

            persisted = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["proxy"], "http://proxy.example")
            self.assertNotIn("proxy", store.data)
            self.assertEqual(store._snapshot_revision, original_revision)
            with self.assertRaises(StorageConflictError):
                store.update({"next": True})
            self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))

    def test_config_save_preserves_file_mode_and_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            path.chmod(0o600)
            before = path.stat()
            with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                ConfigStore(path).update({"proxy": "http://proxy.example"})

            after = path.stat()
            self.assertEqual(after.st_mode & 0o7777, before.st_mode & 0o7777)
            self.assertEqual(after.st_uid, before.st_uid)
            self.assertEqual(after.st_gid, before.st_gid)

    @unittest.skipUnless(os.name == "posix", "requires POSIX dir-fd replacement")
    def test_config_metadata_is_restored_before_replace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            path.chmod(0o640)
            before = path.stat()
            events: list[str] = []
            original_write = os.write
            original_fsync = os.fsync
            original_replace = os.replace

            def record_write(fd: int, payload: bytes) -> int:
                events.append("write")
                return original_write(fd, payload)

            def record_replace(source: str, target: str, *args, **kwargs) -> None:
                events.append("replace")
                self.assertTrue(Path(source).name.startswith(f".{path.name}."))
                self.assertEqual(Path(source).suffix, ".tmp")
                self.assertEqual(target, path.name)
                return original_replace(source, target, *args, **kwargs)

            with (
                mock.patch.object(config_module.os, "write", side_effect=record_write),
                mock.patch.object(config_module.os, "fchmod", side_effect=lambda *_args: events.append("fchmod"), create=True),
                mock.patch.object(config_module.os, "fchown", side_effect=lambda *_args: events.append("fchown"), create=True),
                mock.patch.object(config_module.os, "geteuid", return_value=before.st_uid + 1, create=True),
                mock.patch.object(config_module.os, "getegid", return_value=before.st_gid + 1, create=True),
                mock.patch.object(config_module.os, "fsync", side_effect=lambda fd: (events.append("fsync"), original_fsync(fd))[1]),
                mock.patch.object(secure_file.os, "replace", side_effect=record_replace),
                mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False),
            ):
                ConfigStore(path).update({"proxy": "http://proxy.example"})

            self.assertLess(events.index("write"), events.index("fchmod"))
            self.assertLess(events.index("fchmod"), events.index("fchown"))
            self.assertLess(events.index("fchown"), events.index("fsync"))
            self.assertLess(events.index("fsync"), events.index("replace"))
            self.assertEqual(path.stat().st_mode & 0o7777, stat.S_IMODE(before.st_mode))
            self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))

    @unittest.skipUnless(os.name == "posix", "requires POSIX metadata operations")
    def test_config_metadata_restore_failure_preserves_snapshot(self) -> None:
        for failure in ("write", "fchmod", "fchown", "fsync"):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as tmp_dir:
                path = Path(tmp_dir) / "config.json"
                path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
                path.chmod(0o600)
                store = ConfigStore(path)
                original = path.read_bytes()
                original_revision = store._snapshot_revision
                replace_called = False

                def record_replace(_source: Path, _target: Path) -> None:
                    nonlocal replace_called
                    replace_called = True
                    raise AssertionError("replace must not run after temp failure")

                patchers = [mock.patch.object(secure_file.os, "replace", side_effect=record_replace)]
                if failure == "write":
                    patchers.append(mock.patch.object(config_module.os, "write", side_effect=OSError("write failed")))
                elif failure == "fchmod":
                    patchers.append(mock.patch.object(config_module.os, "fchmod", side_effect=OSError("fchmod failed"), create=True))
                elif failure == "fchown":
                    before = path.stat()
                    patchers.extend(
                        [
                            mock.patch.object(config_module.os, "fchmod", wraps=getattr(os, "fchmod", os.chmod), create=True),
                            mock.patch.object(config_module.os, "fchown", side_effect=OSError("fchown failed"), create=True),
                            mock.patch.object(config_module.os, "geteuid", return_value=before.st_uid + 1, create=True),
                            mock.patch.object(config_module.os, "getegid", return_value=before.st_gid + 1, create=True),
                        ]
                    )
                else:
                    patchers.append(mock.patch.object(config_module.os, "fsync", side_effect=OSError("fsync failed")))

                with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                    with ExitStack() as stack:
                        for patcher in patchers:
                            stack.enter_context(patcher)
                        with self.assertRaises(OSError):
                            store.update({"proxy": "http://proxy.example"})

                self.assertFalse(replace_called)
                self.assertEqual(path.read_bytes(), original)
                self.assertEqual(store._snapshot_revision, original_revision)
                self.assertNotIn("proxy", store.data)
                self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))


if __name__ == "__main__":
    unittest.main()
