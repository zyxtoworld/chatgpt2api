from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import services.image_storage_service as image_storage_module
from services.image_storage_service import ImageStorageService
from services.storage.base import StorageDataError


class ImageIndexRecoveryContractTests(unittest.TestCase):
    def test_corrupt_image_index_is_not_loaded_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_index.json"
            path.write_text("{broken", encoding="utf-8")
            service = ImageStorageService(path)

            with self.assertRaises(StorageDataError):
                service._load_clean_index()

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
