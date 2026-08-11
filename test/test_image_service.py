from __future__ import annotations

import io
import os
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image

from services import image_service
from services import secure_file
from services.image_storage_service import ImageStorageService
from services.image_service import download_images_zip


def _png_bytes(color: tuple[int, int, int]) -> bytes:
    payload = io.BytesIO()
    Image.new("RGB", (4, 4), color).save(payload, format="PNG")
    return payload.getvalue()


class ImageServiceTests(unittest.TestCase):
    def test_download_images_zip_reads_images_from_storage_when_not_local(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            relative_path = "2026/08/08/remote.png"
            with (
                mock.patch("services.image_service.config") as config,
                mock.patch("services.image_service.image_storage_service") as storage,
            ):
                config.images_dir = root
                storage.open_local.return_value = None
                storage.get_bytes.return_value = b"remote-image"

                archive = download_images_zip([relative_path])

            with zipfile.ZipFile(io.BytesIO(archive.read())) as zipped:
                self.assertEqual(zipped.namelist(), ["remote.png"])
                self.assertEqual(zipped.read("remote.png"), b"remote-image")
            storage.get_bytes.assert_called_once_with(relative_path)

    def test_local_image_response_rejects_replacement_before_secure_open(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            relative_path = "2026/08/08/image.png"
            target = root / relative_path
            foreign = Path(tmp_dir) / "outside" / "secret.png"
            target.parent.mkdir(parents=True, exist_ok=True)
            foreign.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"owner-visible-image")
            foreign.write_bytes(b"private-file-secret")

            app = FastAPI()

            @app.get("/images/{image_path:path}")
            def get_image(image_path: str):
                return image_service.get_image_response(image_path)

            storage_service = ImageStorageService(Path(tmp_dir) / "index.json")
            real_open = secure_file.open_no_follow_file

            def replace_before_open(path, root_path, expected_dir):
                target.unlink()
                foreign.replace(target)
                return real_open(path, root_path, expected_dir)

            with (
                mock.patch("services.image_storage_service.config") as storage_config,
                mock.patch.object(image_service, "image_storage_service", storage_service),
                mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_before_open),
            ):
                storage_config.images_dir = root
                response = TestClient(app, raise_server_exceptions=False).get(f"/images/{relative_path}")

            self.assertEqual(response.status_code, 404)
            self.assertNotIn(b"private-file-secret", response.content)

    def test_thumbnail_response_does_not_reopen_path_after_validation(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            thumbnails = Path(tmp_dir) / "thumbnails"
            relative_path = "2026/08/08/image.png"
            source = root / relative_path
            target = thumbnails / f"{relative_path}.png"
            foreign = Path(tmp_dir) / "outside" / f"{relative_path}.png"
            source.parent.mkdir(parents=True, exist_ok=True)
            target.parent.mkdir(parents=True, exist_ok=True)
            foreign.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(_png_bytes((0, 255, 0)))
            target.write_bytes(b"owner-thumbnail")
            foreign.write_bytes(b"private-thumbnail")
            now = os.path.getmtime(source)
            os.utime(target, (now + 10, now + 10))

            app = FastAPI()

            @app.get("/image-thumbnails/{image_path:path}")
            def get_thumbnail(image_path: str):
                return image_service.get_thumbnail_response(image_path)

            storage_service = ImageStorageService(Path(tmp_dir) / "index.json")
            real_checked_open = image_service.open_checked_file
            target_checked_open_count = 0

            def replace_before_second_target_check(path, root_path, expected_dir):
                nonlocal target_checked_open_count
                if Path(path) == target:
                    target_checked_open_count += 1
                    if target_checked_open_count == 2:
                        target.unlink()
                        foreign.replace(target)
                return real_checked_open(path, root_path, expected_dir)

            with (
                mock.patch("services.image_service.config") as image_config,
                mock.patch("services.image_storage_service.config") as storage_config,
                mock.patch.object(image_service, "image_storage_service", storage_service),
                mock.patch.object(
                    image_service,
                    "open_checked_file",
                    side_effect=replace_before_second_target_check,
                ),
            ):
                image_config.image_thumbnails_dir = thumbnails
                storage_config.images_dir = root
                response = TestClient(app, raise_server_exceptions=False).get(
                    f"/image-thumbnails/{relative_path}"
                )

            self.assertEqual(target_checked_open_count, 1)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.content, b"owner-thumbnail")
            self.assertNotIn(b"private-thumbnail", response.content)


if __name__ == "__main__":
    unittest.main()
