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
from services.config import ConfigStore
from services.storage.base import StorageDataError


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

                with mock.patch.object(Path, "replace", side_effect=OSError("replace failed")):
                    with self.assertRaises(OSError):
                        store.update({"proxy": "http://proxy.example"})

            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))
            self.assertNotIn("proxy", store.data)

    def test_config_save_preserves_file_mode_and_owner(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            path.chmod(0o600)
            before = path.stat()
            created_modes: list[int] = []
            events: list[str] = []
            original_mkstemp = config_module.tempfile.mkstemp
            original_write = config_module.os.write

            def record_mkstemp(*args, **kwargs):
                temp_fd, temp_name = original_mkstemp(*args, **kwargs)
                events.append("create")
                created_modes.append(stat.S_IMODE(os.fstat(temp_fd).st_mode))
                return temp_fd, temp_name

            def record_write(fd: int, payload: bytes) -> int:
                events.append("write")
                return original_write(fd, payload)

            with (
                mock.patch.object(config_module.tempfile, "mkstemp", side_effect=record_mkstemp),
                mock.patch.object(config_module.os, "write", side_effect=record_write),
            ):
                with mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False):
                    ConfigStore(path).update({"proxy": "http://proxy.example"})

            self.assertTrue(created_modes)
            self.assertLess(events.index("create"), events.index("write"))
            if os.name == "posix":
                self.assertTrue(all(mode & 0o177 == 0 for mode in created_modes))
            after = path.stat()
            self.assertEqual(after.st_mode & 0o7777, before.st_mode & 0o7777)
            self.assertEqual(after.st_uid, before.st_uid)
            self.assertEqual(after.st_gid, before.st_gid)

    def test_config_metadata_restore_uses_chown_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            path.chmod(0o640)
            before = path.stat()
            events: list[str] = []
            original_chmod = os.chmod
            original_chown = getattr(os, "chown", None)
            original_fsync = os.fsync
            original_replace = Path.replace

            def record_chmod(target: Path, mode: int) -> None:
                events.append("chmod")
                original_chmod(target, mode)

            def record_chown(target: Path, uid: int, gid: int) -> None:
                events.append("chown")
                if original_chown is not None:
                    original_chown(target, uid, gid)

            def record_replace(source: Path, target: Path) -> Path:
                events.append("replace")
                return original_replace(source, target)

            with (
                mock.patch.object(config_module.os, "fchmod", None, create=True),
                mock.patch.object(config_module.os, "fchown", None, create=True),
                mock.patch.object(config_module.os, "chmod", side_effect=record_chmod),
                mock.patch.object(config_module.os, "chown", side_effect=record_chown, create=True),
                mock.patch.object(config_module.os, "geteuid", return_value=before.st_uid + 1, create=True),
                mock.patch.object(config_module.os, "getegid", return_value=before.st_gid + 1, create=True),
                mock.patch.object(config_module.os, "fsync", side_effect=lambda fd: (events.append("fsync"), original_fsync(fd))[1]),
                mock.patch.object(Path, "replace", autospec=True, side_effect=record_replace),
                mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False),
            ):
                ConfigStore(path).update({"proxy": "http://proxy.example"})

            self.assertLess(events.index("chmod"), events.index("chown"))
            self.assertLess(events.index("chown"), events.index("fsync"))
            self.assertLess(events.index("fsync"), events.index("replace"))
            self.assertEqual(path.stat().st_mode & 0o7777, stat.S_IMODE(before.st_mode))
            self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))

    def test_config_metadata_is_restored_before_replace(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(json.dumps({"auth-key": "file-auth"}), encoding="utf-8")
            path.chmod(0o640)
            before = path.stat()
            events: list[str] = []
            original_write = os.write
            original_fsync = os.fsync
            original_replace = Path.replace

            def record_write(fd: int, payload: bytes) -> int:
                events.append("write")
                return original_write(fd, payload)

            def record_replace(source: Path, target: Path) -> Path:
                events.append("replace")
                self.assertEqual(source.parent, path.parent)
                self.assertEqual(source.name.startswith(f".{path.name}."), True)
                self.assertEqual(source.suffix, ".tmp")
                self.assertFalse(source == path)
                return original_replace(source, target)

            with (
                mock.patch.object(config_module.os, "write", side_effect=record_write),
                mock.patch.object(config_module.os, "fchmod", side_effect=lambda *_args: events.append("fchmod"), create=True),
                mock.patch.object(config_module.os, "fchown", side_effect=lambda *_args: events.append("fchown"), create=True),
                mock.patch.object(config_module.os, "geteuid", return_value=before.st_uid + 1, create=True),
                mock.patch.object(config_module.os, "getegid", return_value=before.st_gid + 1, create=True),
                mock.patch.object(config_module.os, "fsync", side_effect=lambda fd: (events.append("fsync"), original_fsync(fd))[1]),
                mock.patch.object(Path, "replace", autospec=True, side_effect=record_replace),
                mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False),
            ):
                ConfigStore(path).update({"proxy": "http://proxy.example"})

            self.assertLess(events.index("write"), events.index("fchmod"))
            self.assertLess(events.index("fchmod"), events.index("fchown"))
            self.assertLess(events.index("fchown"), events.index("fsync"))
            self.assertLess(events.index("fsync"), events.index("replace"))
            self.assertEqual(path.stat().st_mode & 0o7777, stat.S_IMODE(before.st_mode))
            self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))

    def test_config_metadata_restore_failure_preserves_snapshot(self) -> None:
        for failure in ("write", "fchmod", "fchown", "chown", "fsync"):
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

                patchers = [mock.patch.object(Path, "replace", autospec=True, side_effect=record_replace)]
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
                elif failure == "chown":
                    before = path.stat()
                    patchers.extend(
                        [
                            mock.patch.object(config_module.os, "fchown", None, create=True),
                            mock.patch.object(config_module.os, "chown", side_effect=OSError("chown failed"), create=True),
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
