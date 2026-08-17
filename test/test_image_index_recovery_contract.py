from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import services.image_storage_service as image_storage_module
import services.secure_file as secure_file
from services.image_storage_service import ImageStorageService
from services.storage.base import StorageDataError
from test.fixtures.image_inputs import image_fixture_bytes


class ImageIndexRecoveryContractTests(unittest.TestCase):
    @unittest.skipUnless(os.name == "posix", "requires POSIX dir-fd atomic replace")
    def test_list_items_persists_new_image_index_when_replace_is_not_advertised(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir)
            images_dir = data_dir / "images"
            image_path = images_dir / "2026/08/11/image.png"
            image_path.parent.mkdir(parents=True)
            image_path.write_bytes(image_fixture_bytes("image.png"))
            service = ImageStorageService(data_dir / "image_index.json")
            config = mock.Mock()
            config.images_dir = images_dir
            config.base_url = ""
            config.cleanup_old_images.return_value = 0
            config.get_image_storage_settings.return_value = {"public_base_url": ""}

            with mock.patch.object(image_storage_module, "config", config):
                items = service.list_items("")

            self.assertEqual(len(items), 1)
            self.assertEqual(items[0]["path"], "2026/08/11/image.png")
            self.assertEqual(json.loads((data_dir / "image_index.json").read_text()), {"items": mock.ANY})

    @unittest.skipUnless(os.name == "posix", "requires POSIX dir-fd atomic replace")
    def test_image_index_replace_failure_preserves_previous_snapshot_and_temp_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_index.json"
            original = b'{"items": {}}\n'
            path.write_bytes(original)
            service = ImageStorageService(path)

            with mock.patch.object(secure_file.os, "replace", side_effect=OSError("replace failed")):
                with self.assertRaises(OSError):
                    service._save_index({"2026/08/11/image.png": {"path": "2026/08/11/image.png"}})

            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(list(path.parent.glob(f".{path.name}.*.tmp")))

    def test_corrupt_image_index_is_not_loaded_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_index.json"
            path.write_text("{broken", encoding="utf-8")
            service = ImageStorageService(path)

            with self.assertRaises(StorageDataError):
                service._load_clean_index()

    def test_oversized_image_index_is_rejected_before_unbounded_json_read(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_index.json"
            path.write_bytes(b"x" * 5)
            service = ImageStorageService(path)

            with mock.patch.object(image_storage_module, "_MAX_IMAGE_INDEX_BYTES", 4):
                with self.assertRaises(StorageDataError):
                    service._load_clean_index()

    def test_image_index_write_rejects_payload_over_the_read_budget(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_index.json"
            path.write_bytes(b'{"items": {}}\n')
            service = ImageStorageService(path)
            original = path.read_bytes()

            with mock.patch.object(image_storage_module, "_MAX_IMAGE_INDEX_BYTES", 4):
                with self.assertRaises(StorageDataError):
                    service._save_index({"2026/08/15/image.png": {"rel": "image"}})

            self.assertEqual(path.read_bytes(), original)

    def test_image_save_does_not_overwrite_corrupt_index(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_index.json"
            path.write_text("{opaque-corruption", encoding="utf-8")
            service = ImageStorageService(path)
            original = path.read_bytes()
            put_calls: list[str] = []

            class FakeWebDAVClient:
                def __init__(self, _settings: dict[str, object]) -> None:
                    pass

                def put(self, _rel: str, _payload: bytes) -> str:
                    put_calls.append(_rel)
                    return "https://webdav.example/image.png"

                def close(self) -> None:
                    pass

            with (
                mock.patch.object(service, "mode", return_value="webdav"),
                mock.patch.object(
                    service,
                    "settings",
                    return_value={"mode": "webdav", "webdav_url": "https://webdav.example"},
                ),
                mock.patch.object(image_storage_module, "WebDAVClient", FakeWebDAVClient),
                mock.patch.object(image_storage_module.config, "cleanup_old_images", return_value=0),
            ):
                with self.assertRaises(StorageDataError):
                    service.save(b"not-an-image")

            self.assertEqual(path.read_bytes(), original)
            self.assertEqual(put_calls, [])

    def test_image_index_items_must_be_a_mapping_of_records(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_index.json"
            path.write_text('{"items": ["not-a-record"]}', encoding="utf-8")
            service = ImageStorageService(path)

            with self.assertRaises(StorageDataError):
                service._load_clean_index()


if __name__ == "__main__":
    unittest.main()
