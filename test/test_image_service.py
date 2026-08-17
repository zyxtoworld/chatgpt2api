from __future__ import annotations

import io
import json
import os
import tempfile
import threading
import time
import unittest
import zipfile
from pathlib import Path
from unittest import mock
from unittest.mock import PropertyMock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from PIL import Image

from services import image_service
from services import image_storage_service as storage_module
from services import image_tags_service as tags_module
from services import secure_file
from services import config as config_module
from services.image_storage_service import ImageStorageService
from services.image_rel_lock import image_rel_lock
from services.image_service import download_images_zip


def _png_bytes(color: tuple[int, int, int]) -> bytes:
    payload = io.BytesIO()
    Image.new("RGB", (4, 4), color).save(payload, format="PNG")
    return payload.getvalue()


class ImageServiceTests(unittest.TestCase):
    def test_auto_cleanup_reports_failure_without_exposing_exception_text(self):
        canary = "cleanup-secret-path-token"
        with (
            mock.patch.object(
                image_service.config,
                "cleanup_old_images",
                side_effect=RuntimeError(canary),
            ),
            mock.patch.object(image_service.logger, "warning") as warning,
        ):
            image_service._run_auto_cleanup_cycle()

        warning.assert_called_once()
        event = warning.call_args.args[0]
        self.assertEqual(event["event"], "image_auto_cleanup_failed")
        self.assertEqual(event["error_type"], "RuntimeError")
        self.assertNotIn(canary, repr(event))

    def test_set_tags_waits_for_delete_composite_transaction(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            images = data_dir / "images"
            relative_path = "2026/08/08/image.png"
            (images / relative_path).parent.mkdir(parents=True, exist_ok=True)
            (images / relative_path).write_bytes(_png_bytes((255, 0, 0)))
            tags_file = data_dir / "image_tags.json"
            tags_file.parent.mkdir(parents=True, exist_ok=True)
            tags_file.write_text(json.dumps({relative_path: ["old"]}) + "\n", encoding="utf-8")
            delete_returned = threading.Event()
            allow_cleanup = threading.Event()
            tag_finished = threading.Event()
            storage = ImageStorageService(data_dir / "image_index.json")

            def delete_source(_rel):
                delete_returned.set()
                allow_cleanup.wait(2)
                return True

            def set_new_tags():
                tags_module.set_tags(relative_path, ["new"])
                tag_finished.set()

            with (
                mock.patch.object(config_module, "DATA_DIR", data_dir),
                mock.patch.object(tags_module, "TAGS_FILE", tags_file),
                mock.patch.object(image_service, "image_storage_service", storage),
                mock.patch.object(tags_module, "image_storage_service", storage),
                mock.patch.object(storage, "delete", side_effect=delete_source),
            ):
                delete_thread = threading.Thread(
                    target=image_service.delete_images,
                    kwargs={"paths": [relative_path]},
                )
                delete_thread.start()
                self.assertTrue(delete_returned.wait(2))
                tag_thread = threading.Thread(target=set_new_tags)
                tag_thread.start()
                self.assertFalse(
                    tag_finished.wait(0.2),
                    "tag mutation must wait for the delete transaction",
                )
                allow_cleanup.set()
                delete_thread.join(2)
                tag_thread.join(2)
                self.assertFalse(delete_thread.is_alive())
                self.assertFalse(tag_thread.is_alive())
                self.assertEqual(tags_module.load_tags()[relative_path], ["new"])

    def test_delete_images_does_not_delete_thumbnail_replacement_waiting_on_rel_lock(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            images = data_dir / "images"
            thumbnails = data_dir / "image_thumbnails"
            relative_path = "2026/08/08/image.png"
            source = images / relative_path
            thumbnail = thumbnails / f"{relative_path}.png"
            source.parent.mkdir(parents=True, exist_ok=True)
            thumbnail.parent.mkdir(parents=True, exist_ok=True)
            source.write_bytes(_png_bytes((255, 0, 255)))
            thumbnail.write_bytes(b"old-thumbnail")
            new_source = _png_bytes((255, 255, 0))
            new_thumbnail = _png_bytes((0, 255, 255))
            writer_done = threading.Event()
            storage = ImageStorageService(Path(tmp_dir) / "image_index.json")
            tags = {relative_path: ["old"]}

            def write_replacement():
                with storage.rel_lock(relative_path):
                    source.write_bytes(new_source)
                    thumbnail.write_bytes(new_thumbnail)
                    tags[relative_path] = ["new"]
                    writer_done.set()

            def delete_source(rel):
                with storage.rel_lock(rel):
                    secure_file.delete_checked_file(source, images)
                    threading.Thread(target=write_replacement, daemon=True).start()
                return True

            def remove_old_tags(rel):
                tags.pop(rel, None)

            with (
                mock.patch.object(config_module, "DATA_DIR", data_dir),
                mock.patch.object(storage, "delete", side_effect=delete_source),
                mock.patch.object(image_service, "image_storage_service", storage),
                mock.patch.object(image_service, "remove_tags", side_effect=remove_old_tags),
            ):
                removed = image_service.delete_images(paths=[relative_path])
            self.assertTrue(writer_done.wait(2))
            self.assertEqual(removed, {"removed": 1})
            self.assertEqual(source.read_bytes(), new_source)
            self.assertEqual(thumbnail.read_bytes(), new_thumbnail)
            self.assertEqual(tags[relative_path], ["new"])

    def test_thumbnail_cleanup_does_not_delete_replacement_waiting_on_rel_lock(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            thumbnails = data_dir / "image_thumbnails"
            relative_path = "2026/08/08/image.png"
            target = thumbnails / f"{relative_path}.png"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(b"old-thumbnail")
            new_payload = _png_bytes((0, 0, 255))
            writer_done = threading.Event()
            storage = mock.Mock()
            index_file = Path(tmp_dir) / "image_index.json"
            storage.index_file = index_file
            writer_started = threading.Event()

            def exists_with_waiting_writer(rel):
                def write_replacement():
                    with image_rel_lock(index_file, rel):
                        target.write_bytes(new_payload)
                        writer_started.set()
                    writer_done.set()

                threading.Thread(target=write_replacement, daemon=True).start()
                return False

            storage.exists.side_effect = exists_with_waiting_writer

            def delete_after_writer(path, root):
                writer_started.wait(0.2)
                return secure_file.delete_checked_file(path, root)

            with (
                mock.patch.object(config_module, "DATA_DIR", data_dir),
                mock.patch.object(image_service, "image_storage_service", storage),
                mock.patch.object(image_service, "delete_checked_file", side_effect=delete_after_writer),
            ):
                removed = image_service.cleanup_image_thumbnails()
            self.assertTrue(writer_done.wait(2))
            self.assertEqual(removed, 1)
            self.assertEqual(target.read_bytes(), new_payload)

    def test_cleanup_does_not_delete_file_replaced_after_old_file_check(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            data_dir = Path(tmp_dir) / "data"
            target = data_dir / "images" / "2026" / "08" / "08" / "image.png"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(_png_bytes((255, 0, 0)))
            old_time = time.time() - 3 * 86400
            os.utime(target, (old_time, old_time))

            relative_path = "2026/08/08/image.png"
            storage_service = ImageStorageService(data_dir / "image_index.json")
            storage_service.make_relative_path = mock.Mock(return_value=relative_path)
            new_payload = _png_bytes((0, 255, 0))
            original_open = config_module.open_checked_file
            replaced = False
            writer_done = threading.Event()
            writer_errors: list[BaseException] = []

            def replace_after_check(path, root, expected_dir):
                nonlocal replaced
                opened = original_open(path, root, expected_dir)
                if Path(path) == target and not replaced:
                    replaced = True
                    # Match cleanup_old_images' post-stat window: the checked
                    # descriptor is no longer needed before the path is replaced.
                    opened.file.close()
                    def save_replacement():
                        try:
                            storage_service.save(new_payload)
                        except BaseException as exc:
                            writer_errors.append(exc)
                        finally:
                            writer_done.set()

                    threading.Thread(target=save_replacement, daemon=True).start()
                    if not writer_done.wait(2):
                        raise AssertionError("replacement save did not complete")
                    if writer_errors:
                        raise writer_errors[0]
                return opened

            with (
                mock.patch.object(config_module, "DATA_DIR", data_dir),
                mock.patch.object(storage_module.config, "get_image_storage_settings", return_value={"mode": "local"}),
                mock.patch.object(
                    config_module.ConfigStore,
                    "image_retention_days",
                    new_callable=PropertyMock,
                    return_value=1,
                ),
                mock.patch.object(config_module, "open_checked_file", side_effect=replace_after_check),
            ):
                removed = config_module.config.cleanup_old_images()

            self.assertTrue(replaced)
            self.assertEqual(removed, 0)
            self.assertEqual(target.read_bytes(), new_payload)

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

    def test_download_images_zip_rejects_archive_over_total_budget(self):
        with (
            mock.patch.object(image_service, "_MAX_IMAGE_ZIP_BYTES", 5),
            mock.patch.object(image_service.image_storage_service, "open_local", return_value=None),
            mock.patch.object(image_service.image_storage_service, "get_bytes", return_value=b"1234"),
        ):
            with self.assertRaises(HTTPException) as error:
                download_images_zip(["2026/08/08/one.png", "2026/08/08/two.png"])

        self.assertEqual(error.exception.status_code, 413)
        self.assertEqual(error.exception.detail, "image archive exceeds size limit")

    def test_download_images_zip_rejects_too_many_paths_before_allocating_archive(self):
        paths = [f"2026/08/16/{index}.png" for index in range(image_service._MAX_IMAGE_ZIP_ITEMS + 1)]
        with mock.patch.object(image_service.io, "BytesIO") as bytes_io:
            with self.assertRaises(HTTPException) as error:
                download_images_zip(paths)

        self.assertEqual(error.exception.status_code, 413)
        self.assertEqual(error.exception.detail, "too many images in archive request")
        bytes_io.assert_not_called()

    def test_download_images_zip_propagates_image_size_failure(self):
        size_error = HTTPException(status_code=413, detail="image exceeds size limit")
        with (
            mock.patch.object(image_service.image_storage_service, "open_local", return_value=None),
            mock.patch.object(image_service.image_storage_service, "get_bytes", side_effect=size_error),
        ):
            with self.assertRaises(HTTPException) as error:
                download_images_zip(["2026/08/08/oversized.png"])

        self.assertEqual(error.exception.status_code, 413)
        self.assertEqual(error.exception.detail, "image exceeds size limit")

    def test_download_images_zip_projects_local_read_failure(self):
        opened = mock.Mock()
        opened.filename = "broken.png"
        opened.file.read.side_effect = OSError("local read failed")
        with mock.patch.object(image_service.image_storage_service, "open_local", return_value=opened):
            with self.assertRaises(HTTPException) as error:
                download_images_zip(["2026/08/08/broken.png"])

        self.assertEqual(error.exception.status_code, 502)
        self.assertEqual(error.exception.detail, "image storage read failed")
        opened.file.close.assert_called_once_with()

    def test_single_image_download_projects_filename_before_header_construction(self):
        with (
            mock.patch.object(image_service.image_storage_service, "open_local", return_value=None),
            mock.patch.object(image_service.image_storage_service, "get_bytes", return_value=b"image"),
        ):
            response = image_service.get_image_download_response("bad\r\nX-Leak: yes\".png")

        content_disposition = response.headers["content-disposition"]
        self.assertEqual(content_disposition, 'attachment; filename="bad_X-Leak_yes_.png"')
        self.assertNotIn("\r", content_disposition)
        self.assertNotIn("\n", content_disposition)

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
