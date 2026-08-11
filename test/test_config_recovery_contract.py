from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

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
            self.assertFalse(path.with_suffix(path.suffix + ".tmp").exists())
            self.assertNotIn("proxy", store.data)


if __name__ == "__main__":
    unittest.main()
