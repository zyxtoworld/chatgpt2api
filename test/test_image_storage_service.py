from __future__ import annotations

import tempfile
import threading
import unittest
import io
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from PIL import Image

import services.image_storage_service as image_storage_module
import services.image_service as image_service_module
from services.image_storage_service import ImageStorageService
from services.storage.base import StorageDataError


def png_bytes() -> bytes:
    path = Path(tempfile.gettempdir()) / "chatgpt2api-test-image.png"
    Image.new("RGB", (2, 2), color=(255, 0, 0)).save(path, format="PNG")
    return path.read_bytes()


def jpeg_bytes() -> bytes:
    path = Path(tempfile.gettempdir()) / "chatgpt2api-test-image.jpg"
    Image.new("RGB", (2, 2), color=(0, 255, 0)).save(path, format="JPEG")
    return path.read_bytes()


class FakeWebDAVClient:
    uploaded: dict[str, bytes] = {}
    deleted: list[str] = []
    closed: int = 0
    content_types: dict[str, str] = {}

    def __init__(self, _settings):
        pass

    def put(self, rel: str, payload: bytes, content_type: str = "image/png") -> str:
        self.uploaded[rel] = payload
        self.content_types[rel] = content_type
        return f"https://dav.example.test/{rel}"

    def get(self, rel: str) -> bytes:
        return self.uploaded[rel]

    def delete(self, rel: str) -> bool:
        self.deleted.append(rel)
        self.uploaded.pop(rel, None)
        return True

    def remote_exists(self, rel: str) -> bool:
        return rel in self.uploaded

    def test(self) -> dict[str, object]:
        self.put(".chatgpt2api_webdav_test.txt", b"chatgpt2api webdav test\n")
        self.delete(".chatgpt2api_webdav_test.txt")
        return {"ok": True, "status": 200, "error": None}

    def close(self) -> None:
        type(self).closed += 1


class WebDAVResponseLifecycleTests(unittest.TestCase):
    def test_get_does_not_replace_download_error_with_close_error(self) -> None:
        class Response:
            status_code = 200
            headers = {}

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, *, chunk_size: int):
                self.chunk_size = chunk_size
                yield b"downloaded"

            def close(self) -> None:
                self.closed = True
                raise OSError("close failed")

        class Session:
            def __init__(self, response: Response) -> None:
                self.response = response

            def request(self, *_args, **_kwargs):
                return self.response

        response = Response()
        client = image_storage_module.WebDAVClient({"webdav_url": "https://dav.example.test"})
        client.session = Session(response)

        self.assertEqual(client.get("2026/08/15/image.png"), b"downloaded")
        self.assertTrue(response.closed)


class ImageStorageServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = Path(self.tmp.name)
        self.images_dir = self.data_dir / "images"
        self.settings = {
            "enabled": False,
            "mode": "local",
            "webdav_url": "",
            "webdav_username": "",
            "webdav_password": "",
            "webdav_root_path": "chatgpt2api/images",
            "public_base_url": "",
        }
        self.config_patcher = mock.patch("services.image_storage_service.config")
        self.mock_config = self.config_patcher.start()
        self.addCleanup(self.config_patcher.stop)
        self.mock_config.images_dir = self.images_dir
        self.mock_config.base_url = "http://app.test"
        self.mock_config.cleanup_old_images.return_value = 0
        self.mock_config.get_image_storage_settings.side_effect = lambda: dict(self.settings)
        FakeWebDAVClient.uploaded = {}
        FakeWebDAVClient.deleted = []
        FakeWebDAVClient.closed = 0
        FakeWebDAVClient.content_types = {}

    def service(self) -> ImageStorageService:
        return ImageStorageService(self.data_dir / "image_index.json")

    def test_local_mode_saves_to_local_directory(self):
        stored = self.service().save(png_bytes(), "http://app.test")

        self.assertEqual(stored.storage, "local")
        self.assertTrue((self.images_dir / stored.rel).is_file())
        self.assertEqual(stored.url, f"http://app.test/images/{stored.rel}")

    def test_local_get_bytes_rejects_oversized_file_before_reading_body(self):
        rel = "2026/05/07/oversized.png"
        path = self.images_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"12345")

        with mock.patch.object(image_storage_module, "_MAX_LOCAL_IMAGE_BYTES", 4):
            with self.assertRaisesRegex(image_storage_module.HTTPException, "image exceeds size limit") as error:
                self.service().get_bytes(rel)

        self.assertEqual(error.exception.status_code, 413)

    def test_local_get_bytes_reads_at_most_the_shared_image_budget(self):
        rel = "2026/05/07/image.png"
        path = self.images_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"1234")

        with mock.patch.object(image_storage_module, "_MAX_LOCAL_IMAGE_BYTES", 4):
            self.assertEqual(self.service().get_bytes(rel), b"1234")

    def test_corrupt_webdav_flag_does_not_trigger_remote_read(self):
        rel = "2026/05/07/image.png"
        service = self.service()
        service._save_index({
            rel: {
                "rel": rel,
                "storage": "webdav",
                "local": False,
                "webdav": "false",
            }
        })

        remote = mock.Mock()
        remote.get.return_value = b"remote-bytes"
        with mock.patch("services.image_storage_service.WebDAVClient", return_value=remote):
            with self.assertRaises(image_storage_module.HTTPException) as error:
                service.get_bytes(rel)

        self.assertEqual(error.exception.status_code, 404)
        remote.get.assert_not_called()
        remote.close.assert_not_called()

    def test_list_items_skips_oversized_local_file(self):
        rel = "2026/05/07/oversized.png"
        path = self.images_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"12345")

        with mock.patch.object(image_storage_module, "_MAX_LOCAL_IMAGE_BYTES", 4):
            self.assertEqual(self.service().list_items("http://app.test"), [])

    def test_sync_all_counts_oversized_local_file_without_uploading(self):
        rel = "2026/05/07/oversized.png"
        path = self.images_dir / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"12345")
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })

        with (
            mock.patch.object(image_storage_module, "_MAX_LOCAL_IMAGE_BYTES", 4),
            mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient),
        ):
            result = self.service().sync_all()

        self.assertEqual(result, {"uploaded": 0, "skipped": 0, "failed": 1})
        self.assertEqual(FakeWebDAVClient.uploaded, {})

    def test_concurrent_saves_do_not_invert_rel_lock_while_cleanup_scans(self):
        service = self.service()
        payload_a = png_bytes()
        payload_b = jpeg_bytes()
        rel_a = "2026/01/01/a.png"
        rel_b = "2026/01/01/b.jpg"
        cleanup_barrier = threading.Barrier(2)
        cleanup_calls = 0
        cleanup_calls_lock = threading.Lock()

        def cleanup_old_images() -> None:
            nonlocal cleanup_calls
            with cleanup_calls_lock:
                cleanup_index = cleanup_calls
                cleanup_calls += 1
            cleanup_barrier.wait(timeout=2)
            other_rel = rel_b if cleanup_index == 0 else rel_a
            with image_storage_module._image_rel_lock(service.index_file, other_rel):
                return

        def save(payload: bytes) -> None:
            service.save(payload, "http://app.test")

        errors: list[BaseException] = []
        def save_checked(payload: bytes) -> None:
            try:
                save(payload)
            except BaseException as exc:
                errors.append(exc)

        threads = [
            threading.Thread(target=save_checked, args=(payload,), daemon=True)
            for payload in (payload_a, payload_b)
        ]
        with (
            mock.patch.object(
                service,
                "make_relative_path",
                side_effect=lambda data, _extension: rel_a if data is payload_a else rel_b,
            ),
            mock.patch.object(self.mock_config, "cleanup_old_images", side_effect=cleanup_old_images),
        ):
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=3)

        self.assertTrue(all(not thread.is_alive() for thread in threads))
        self.assertEqual(errors, [])

    def test_save_uses_detected_jpeg_extension_and_webdav_content_type(self):
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })
        with mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient):
            stored = self.service().save(jpeg_bytes(), "http://app.test")

        self.assertTrue(stored.rel.endswith(".jpg"), stored.rel)
        self.assertEqual(FakeWebDAVClient.content_types[stored.rel], "image/jpeg")

    def test_public_url_without_explicit_base_url_is_relative(self):
        rel = "2026/05/07/image.png"

        self.assertEqual(self.service()._public_url(rel, ""), f"/images/{rel}")

    def test_public_base_url_rejects_credentials_query_fragment_and_malformed_authority(self):
        rel = "2026/05/07/image.png"
        for public_base_url in (
            "https://user:secret@cdn.example.test/images",
            "https://cdn.example.test/images?secret=1",
            "https://cdn.example.test/images#secret",
            "https://[::1/images",
            "https://cdn.example.test:bad/images",
        ):
            with self.subTest(public_base_url=public_base_url):
                self.settings["public_base_url"] = public_base_url
                self.assertEqual(self.service()._public_url(rel, ""), f"/images/{rel}")

    def test_webdav_mode_uploads_without_local_file(self):
        self.settings.update({
            "enabled": True,
            "mode": "webdav",
            "webdav_url": "https://dav.example.test",
            "webdav_password": "secret",
        })
        with mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient):
            stored = self.service().save(png_bytes(), "http://app.test")
            payload = self.service().get_bytes(stored.rel)

        self.assertEqual(stored.storage, "webdav")
        self.assertFalse((self.images_dir / stored.rel).exists())
        self.assertIn(stored.rel, FakeWebDAVClient.uploaded)
        self.assertEqual(payload, FakeWebDAVClient.uploaded[stored.rel])

    def test_webdav_clients_are_closed_after_storage_operations(self):
        self.settings.update({
            "enabled": True,
            "mode": "webdav",
            "webdav_url": "https://dav.example.test",
            "webdav_password": "secret",
        })
        service = self.service()
        with mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient):
            stored = service.save(png_bytes(), "http://app.test")
            service.get_bytes(stored.rel)
            service.delete(stored.rel)

        self.assertEqual(FakeWebDAVClient.closed, 3)

    def test_webdav_sync_closes_client_after_batch(self):
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
            "webdav_password": "secret",
        })
        rel = "2026/05/07/sync.png"
        image_path = self.images_dir / rel
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(png_bytes())

        with mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient):
            result = self.service().sync_all()

        self.assertEqual(result["uploaded"], 1)
        self.assertEqual(FakeWebDAVClient.closed, 1)

    def test_list_items_ignores_non_image_files(self):
        image = png_bytes()
        image_path = self.images_dir / "2026" / "05" / "07" / "sample.png"
        image_path.parent.mkdir(parents=True, exist_ok=True)
        image_path.write_bytes(image)
        (self.images_dir / ".DS_Store").write_text("not an image", encoding="utf-8")
        (self.images_dir / "2026" / ".DS_Store").write_text("not an image", encoding="utf-8")

        items = self.service().list_items("http://app.test")

        self.assertEqual([item["rel"] for item in items], ["2026/05/07/sample.png"])
        self.assertEqual(items[0]["storage"], "local")

    def test_both_mode_saves_to_local_and_webdav(self):
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
            "webdav_password": "secret",
            "public_base_url": "https://cdn.example.test/images",
        })
        with mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient):
            stored = self.service().save(png_bytes(), "http://app.test")

        self.assertEqual(stored.storage, "both")
        self.assertTrue((self.images_dir / stored.rel).is_file())
        self.assertIn(stored.rel, FakeWebDAVClient.uploaded)
        self.assertEqual(stored.url, f"https://cdn.example.test/images/{stored.rel}")

    def test_webdav_delete_failure_preserves_retryable_index_ownership(self):
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })
        service = self.service()
        with mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient):
            stored = service.save(png_bytes(), "http://app.test")

        class FailingDeleteClient(FakeWebDAVClient):
            def delete(self, _rel: str) -> bool:
                raise image_storage_module.PublicImageStorageError("remote delete failed")

        with mock.patch("services.image_storage_service.WebDAVClient", FailingDeleteClient):
            with self.assertRaises(image_storage_module.PublicImageStorageError):
                service.delete(stored.rel)

        self.assertFalse((self.images_dir / stored.rel).exists())
        current = service._load_clean_index()
        self.assertIn(stored.rel, current)
        self.assertFalse(current[stored.rel]["local"])
        self.assertTrue(current[stored.rel]["webdav"])
        self.assertIn(stored.rel, FakeWebDAVClient.uploaded)

    def test_sync_index_failure_rolls_back_new_remote_side(self):
        rel = "2026/08/14/sync-index-failure.png"
        service = self.service()
        with mock.patch.object(service, "make_relative_path", return_value=rel):
            service.save(png_bytes(), "http://app.test")
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })

        with (
            mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient),
            mock.patch.object(service, "_save_index", side_effect=OSError("index write failed")),
        ):
            result = service.sync_all()

        self.assertEqual(result, {"uploaded": 0, "skipped": 0, "failed": 1})
        self.assertNotIn(rel, FakeWebDAVClient.uploaded)
        self.assertEqual(FakeWebDAVClient.deleted, [rel])
        current = service._load_clean_index()
        self.assertTrue(current[rel]["local"])
        self.assertFalse(current[rel]["webdav"])

    def test_sync_index_failure_does_not_delete_preexisting_remote_side(self):
        rel = "2026/08/14/preexisting-remote.png"
        service = self.service()
        with mock.patch.object(service, "make_relative_path", return_value=rel):
            service.save(png_bytes(), "http://app.test")
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })
        old_remote_payload = b"remote-object-owned-before-sync"
        FakeWebDAVClient.uploaded[rel] = old_remote_payload

        with (
            mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient),
            mock.patch.object(service, "_save_index", side_effect=OSError("index write failed")),
        ):
            result = service.sync_all()

        self.assertEqual(result, {"uploaded": 0, "skipped": 0, "failed": 1})
        self.assertEqual(FakeWebDAVClient.uploaded[rel], png_bytes())
        self.assertEqual(FakeWebDAVClient.deleted, [])

    def test_sync_does_not_put_when_remote_ownership_is_uncertain(self):
        rel = "2026/08/14/uncertain-remote.png"
        service = self.service()
        with mock.patch.object(service, "make_relative_path", return_value=rel):
            service.save(png_bytes(), "http://app.test")
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })

        class UncertainRemoteClient(FakeWebDAVClient):
            put_calls = 0

            def remote_exists(self, _rel: str) -> bool:
                raise image_storage_module.PublicImageStorageError("remote probe failed")

            def put(self, *args, **kwargs):
                type(self).put_calls += 1
                return super().put(*args, **kwargs)

        with mock.patch("services.image_storage_service.WebDAVClient", UncertainRemoteClient):
            result = service.sync_all()

        self.assertEqual(result, {"uploaded": 0, "skipped": 0, "failed": 1})
        self.assertEqual(UncertainRemoteClient.put_calls, 0)
        self.assertTrue(service._load_clean_index()[rel]["local"])
        self.assertFalse(service._load_clean_index()[rel]["webdav"])

    def test_both_mode_remote_failure_does_not_leave_local_orphan(self):
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })

        class FailingWebDAVClient(FakeWebDAVClient):
            def put(self, rel: str, payload: bytes, content_type: str = "image/png") -> str:
                raise RuntimeError("remote upload failed")

        service = self.service()
        with mock.patch("services.image_storage_service.WebDAVClient", FailingWebDAVClient):
            with self.assertRaises(RuntimeError):
                service.save(png_bytes(), "http://app.test")

        self.assertEqual(service.list_items("http://app.test"), [])
        self.assertFalse(list(self.images_dir.rglob("*.png")))

    def test_corrupt_index_is_rejected_before_image_cleanup(self):
        index_path = self.data_dir / "image_index.json"
        index_path.write_text('{"items": []}\n', encoding="utf-8")
        sentinel = self.images_dir / "2026" / "01" / "01" / "existing.png"
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        sentinel.write_bytes(png_bytes())

        def destructive_cleanup() -> None:
            sentinel.unlink()

        self.mock_config.cleanup_old_images.side_effect = destructive_cleanup
        with self.assertRaises(StorageDataError):
            self.service().save(png_bytes(), "http://app.test")

        self.assertTrue(sentinel.exists())

    def test_index_failure_rolls_back_new_local_object(self):
        service = self.service()
        with mock.patch.object(service, "_save_index", side_effect=OSError("index write failed")):
            with self.assertRaises(OSError):
                service.save(png_bytes(), "http://app.test")

        self.assertFalse(list(self.images_dir.rglob("*.png")))
        self.assertEqual(service.list_items("http://app.test"), [])

    def test_index_failure_rolls_back_new_webdav_object(self):
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })
        service = self.service()
        with (
            mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient),
            mock.patch.object(service, "_save_index", side_effect=OSError("index write failed")),
        ):
            with self.assertRaises(OSError):
                service.save(png_bytes(), "http://app.test")

        self.assertFalse(list(self.images_dir.rglob("*.png")))
        self.assertEqual(FakeWebDAVClient.uploaded, {})

    def test_save_index_failure_does_not_delete_preexisting_remote_missing_from_index(self):
        self.settings.update({
            "enabled": True,
            "mode": "webdav",
            "webdav_url": "https://dav.example.test",
        })
        rel = "2026/08/14/unindexed-remote.png"
        old_payload = b"remote-object-before-save"
        FakeWebDAVClient.uploaded[rel] = old_payload
        service = self.service()

        with (
            mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient),
            mock.patch.object(service, "make_relative_path", return_value=rel),
            mock.patch.object(service, "_save_index", side_effect=OSError("index write failed")),
        ):
            with self.assertRaises(OSError):
                service.save(png_bytes(), "http://app.test")

        self.assertEqual(FakeWebDAVClient.uploaded[rel], png_bytes())
        self.assertEqual(FakeWebDAVClient.deleted, [])

    def test_failed_same_rel_local_save_preserves_existing_object(self):
        rel = "2026/08/14/same.png"
        service = self.service()
        with mock.patch.object(service, "make_relative_path", return_value=rel):
            service.save(png_bytes(), "http://app.test")
            with mock.patch.object(service, "_save_index", side_effect=OSError("index write failed")):
                with self.assertRaises(OSError):
                    service.save(png_bytes(), "http://app.test")

        self.assertTrue((self.images_dir / rel).is_file())
        self.assertIn(rel, service._load_clean_index())

    def test_failed_both_save_rolls_back_only_new_local_side(self):
        rel = "2026/08/14/remote-only.png"
        self.settings.update({
            "enabled": True,
            "mode": "webdav",
            "webdav_url": "https://dav.example.test",
        })
        service = self.service()
        with (
            mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient),
            mock.patch.object(service, "make_relative_path", return_value=rel),
        ):
            service.save(png_bytes(), "http://app.test")
            self.settings["mode"] = "both"
            with mock.patch.object(service, "_save_index", side_effect=OSError("index write failed")):
                with self.assertRaises(OSError):
                    service.save(png_bytes(), "http://app.test")

        self.assertFalse((self.images_dir / rel).exists())
        self.assertIn(rel, FakeWebDAVClient.uploaded)
        self.assertEqual(FakeWebDAVClient.deleted, [])
        self.assertTrue(service._load_clean_index()[rel]["webdav"])
        self.assertFalse(service._load_clean_index()[rel]["local"])

    def test_failed_both_save_rolls_back_only_new_remote_side(self):
        rel = "2026/08/14/local-only.png"
        service = self.service()
        with mock.patch.object(service, "make_relative_path", return_value=rel):
            service.save(png_bytes(), "http://app.test")
            self.settings.update({
                "enabled": True,
                "mode": "both",
                "webdav_url": "https://dav.example.test",
            })
            with (
                mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient),
                mock.patch.object(service, "_save_index", side_effect=OSError("index write failed")),
            ):
                with self.assertRaises(OSError):
                    service.save(png_bytes(), "http://app.test")

        self.assertTrue((self.images_dir / rel).is_file())
        self.assertNotIn(rel, FakeWebDAVClient.uploaded)
        self.assertEqual(FakeWebDAVClient.deleted, [rel])
        self.assertTrue(service._load_clean_index()[rel]["local"])
        self.assertFalse(service._load_clean_index()[rel]["webdav"])

    def test_concurrent_same_rel_webdav_rollback_does_not_delete_first_commit(self):
        self.settings.update({
            "enabled": True,
            "mode": "webdav",
            "webdav_url": "https://dav.example.test",
        })
        service_a = self.service()
        service_b = ImageStorageService(self.data_dir / "image_index.json")
        rel = "2026/08/14/same.png"
        b_initial_done = threading.Event()
        a_put_started = threading.Event()
        a_committed = threading.Event()
        errors: dict[str, BaseException] = {}

        def make_a(_payload: bytes, _extension: str | None = None) -> str:
            self.assertTrue(b_initial_done.wait(2))
            return rel

        def make_b(_payload: bytes, _extension: str | None = None) -> str:
            b_initial_done.set()
            self.assertTrue(a_put_started.wait(2))
            return rel

        class ConcurrentWebDAVClient(FakeWebDAVClient):
            calls = 0
            calls_lock = threading.Lock()

            def put(self, relative: str, payload: bytes, content_type: str = "image/png") -> str:
                with self.calls_lock:
                    type(self).calls += 1
                    call_number = type(self).calls
                if call_number == 1:
                    a_put_started.set()
                else:
                    if not a_committed.wait(2):
                        raise AssertionError("first save did not commit")
                type(self).uploaded[relative] = payload
                return f"https://dav.example.test/{relative}"

        original_save_a = service_a._save_index

        def save_a(items):
            original_save_a(items)
            a_committed.set()

        def save_b(_items):
            self.assertTrue(a_committed.is_set())
            raise OSError("second index commit failed")

        def run(name: str, service: ImageStorageService) -> None:
            try:
                service.save(png_bytes(), "http://app.test")
            except BaseException as exc:
                errors[name] = exc

        ConcurrentWebDAVClient.calls = 0
        with (
            mock.patch("services.image_storage_service.WebDAVClient", ConcurrentWebDAVClient),
            mock.patch.object(service_a, "make_relative_path", side_effect=make_a),
            mock.patch.object(service_b, "make_relative_path", side_effect=make_b),
            mock.patch.object(service_a, "_save_index", side_effect=save_a),
            mock.patch.object(service_b, "_save_index", side_effect=save_b),
        ):
            thread_a = threading.Thread(target=run, args=("a", service_a))
            thread_a.start()
            thread_b = threading.Thread(target=run, args=("b", service_b))
            thread_b.start()
            thread_a.join(3)
            thread_b.join(3)

        self.assertFalse(thread_a.is_alive())
        self.assertFalse(thread_b.is_alive())
        self.assertNotIn("a", errors)
        self.assertIsInstance(errors.get("b"), OSError)
        self.assertIn(rel, FakeWebDAVClient.uploaded)
        self.assertEqual(FakeWebDAVClient.deleted, [])
        self.assertIn(rel, service_a._load_clean_index())

    def test_delete_waits_for_same_rel_save_commit(self):
        rel = "2026/08/14/delete-race.png"
        service = self.service()
        save_at_index = threading.Event()
        release_save = threading.Event()
        delete_called = threading.Event()
        local_deleted = threading.Event()
        release_delete = threading.Event()
        errors: list[BaseException] = []
        real_save_index = service._save_index
        real_delete = image_storage_module.delete_checked_file

        def save_index(items):
            save_at_index.set()
            if not release_save.wait(2):
                raise AssertionError("save release was not signaled")
            real_save_index(items)

        def delete_file(path, root):
            delete_called.set()
            if not release_delete.wait(2):
                raise AssertionError("delete release was not signaled")
            result = real_delete(path, root)
            local_deleted.set()
            return result

        def run_save() -> None:
            try:
                service.save(png_bytes(), "http://app.test")
            except BaseException as exc:
                errors.append(exc)

        def run_delete() -> None:
            try:
                service.delete(rel)
            except BaseException as exc:
                errors.append(exc)

        with (
            mock.patch.object(service, "make_relative_path", return_value=rel),
            mock.patch.object(service, "_save_index", side_effect=save_index),
            mock.patch("services.image_storage_service.delete_checked_file", side_effect=delete_file),
        ):
            save_thread = threading.Thread(target=run_save)
            save_thread.start()
            self.assertTrue(save_at_index.wait(2))
            delete_thread = threading.Thread(target=run_delete)
            delete_thread.start()
            if delete_called.wait(1):
                # This is the unprotected ordering: let the delete finish its
                # local removal before the save is allowed to publish its index.
                release_delete.set()
                self.assertTrue(local_deleted.wait(2))
                release_save.set()
            else:
                # With the per-rel owner, delete cannot reach the filesystem
                # until save has committed and released the owner.
                release_save.set()
                release_delete.set()
            save_thread.join(3)
            delete_thread.join(3)

        self.assertFalse(save_thread.is_alive())
        self.assertFalse(delete_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertFalse((self.images_dir / rel).exists())
        self.assertNotIn(rel, service._load_clean_index())

    def test_list_items_waits_for_same_rel_save_rollback(self):
        rel = "2026/08/14/list-rollback-race.png"
        service = self.service()
        local_written = threading.Event()
        release_save = threading.Event()
        list_finished = threading.Event()
        save_errors: list[BaseException] = []
        list_result: list[dict[str, object]] = []
        real_atomic_write = image_storage_module.atomic_write_bytes
        real_save_index = service._save_index

        def atomic_write(path, root, payload):
            result = real_atomic_write(path, root, payload)
            if threading.current_thread().name == "save-under-test":
                local_written.set()
                if not release_save.wait(2):
                    raise AssertionError("save was not released")
            return result

        def save_index(items):
            if threading.current_thread().name == "save-under-test":
                raise OSError("index commit failed")
            return real_save_index(items)

        def run_save() -> None:
            try:
                with mock.patch.object(service, "make_relative_path", return_value=rel):
                    service.save(png_bytes(), "http://app.test")
            except BaseException as exc:
                save_errors.append(exc)

        def run_list() -> None:
            list_result.extend(service.list_items("http://app.test"))
            list_finished.set()

        with (
            mock.patch("services.image_storage_service.atomic_write_bytes", side_effect=atomic_write),
            mock.patch.object(service, "_save_index", side_effect=save_index),
        ):
            save_thread = threading.Thread(target=run_save, name="save-under-test")
            save_thread.start()
            self.assertTrue(local_written.wait(2))
            list_thread = threading.Thread(target=run_list, name="list-under-test")
            list_thread.start()
            self.assertFalse(list_finished.wait(0.2))
            release_save.set()
            save_thread.join(3)
            list_thread.join(3)

        self.assertFalse(save_thread.is_alive())
        self.assertFalse(list_thread.is_alive())
        self.assertEqual(len(save_errors), 1)
        self.assertIsInstance(save_errors[0], OSError)
        self.assertEqual(list_result, [])
        self.assertFalse((self.images_dir / rel).exists())
        self.assertNotIn(rel, service._load_clean_index())

    def test_sync_all_waits_for_same_rel_save(self):
        rel = "2026/08/14/sync-race.png"
        old_payload = png_bytes()
        new_payload = jpeg_bytes()
        service = self.service()
        with mock.patch.object(service, "make_relative_path", return_value=rel):
            service.save(old_payload, "http://app.test")
        self.settings.update({
            "enabled": True,
            "mode": "both",
            "webdav_url": "https://dav.example.test",
        })
        sync_put_started = threading.Event()
        release_sync_put = threading.Event()
        errors: list[BaseException] = []

        class SyncWebDAVClient(FakeWebDAVClient):
            calls = 0
            calls_lock = threading.Lock()

            def put(self, relative: str, payload: bytes, content_type: str = "image/png") -> str:
                with self.calls_lock:
                    type(self).calls += 1
                    call_number = type(self).calls
                if call_number == 1:
                    sync_put_started.set()
                    if not release_sync_put.wait(2):
                        raise AssertionError("sync upload was not released")
                type(self).uploaded[relative] = payload
                return f"https://dav.example.test/{relative}"

        def run_sync() -> None:
            try:
                service.sync_all()
            except BaseException as exc:
                errors.append(exc)

        def run_save() -> None:
            try:
                with mock.patch.object(service, "make_relative_path", return_value=rel):
                    service.save(new_payload, "http://app.test")
            except BaseException as exc:
                errors.append(exc)

        SyncWebDAVClient.calls = 0
        with mock.patch("services.image_storage_service.WebDAVClient", SyncWebDAVClient):
            sync_thread = threading.Thread(target=run_sync)
            sync_thread.start()
            self.assertTrue(sync_put_started.wait(2))
            save_thread = threading.Thread(target=run_save)
            save_thread.start()
            release_sync_put.set()
            sync_thread.join(3)
            save_thread.join(3)

        self.assertFalse(sync_thread.is_alive())
        self.assertFalse(save_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(FakeWebDAVClient.uploaded[rel], new_payload)

    def test_delete_to_target_waits_for_same_rel_save(self):
        rel = "2026/08/14/delete-to-target-race.png"
        old_payload = png_bytes()
        new_payload = jpeg_bytes()
        root = self.images_dir
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(old_payload)
        service = self.service()
        save_at_index = threading.Event()
        release_save = threading.Event()
        save_done = threading.Event()
        errors: list[BaseException] = []
        delete_calls = 0
        real_save_index = service._save_index
        real_delete = image_storage_module.delete_checked_file

        def save_index(items):
            save_at_index.set()
            if not release_save.wait(2):
                raise AssertionError("save release was not signaled")
            real_save_index(items)

        def run_save() -> None:
            try:
                service.save(new_payload, "http://app.test")
            except BaseException as exc:
                errors.append(exc)
            finally:
                save_done.set()

        def delete_file(path, root_path):
            nonlocal delete_calls
            delete_calls += 1
            if delete_calls > 1:
                return real_delete(path, root_path)
            save_thread = threading.Thread(target=run_save)
            save_thread.start()
            if save_at_index.wait(1):
                # Without the rel owner, the new save has published its file
                # while delete_to_target is about to remove the old snapshot.
                release_save.set()
                self.assertTrue(save_done.wait(2))
                save_thread.join(2)
            result = real_delete(path, root_path)
            if not save_at_index.is_set():
                release_save.set()
                save_thread.join(2)
            return result

        with (
            mock.patch.object(service, "make_relative_path", return_value=rel),
            mock.patch.object(service, "_save_index", side_effect=save_index),
            mock.patch("services.image_service.config") as image_config,
            mock.patch("services.image_storage_service.config") as storage_config,
            mock.patch.object(image_service_module, "image_storage_service", service),
            mock.patch.object(image_service_module, "delete_checked_file", side_effect=delete_file),
            mock.patch("shutil.disk_usage", return_value=SimpleNamespace(total=100, used=100, free=0)),
        ):
            image_config.images_dir = root
            image_config.image_thumbnails_dir = self.data_dir / "thumbnails"
            storage_config.images_dir = root
            storage_config.get_image_storage_settings.return_value = {"mode": "local"}
            result = image_service_module.delete_to_target(1)

        self.assertTrue(save_done.wait(2))
        self.assertEqual(result["removed"], 1)
        self.assertFalse(errors)
        self.assertTrue(target.exists())
        self.assertEqual(target.read_bytes(), new_payload)

    def test_delete_to_target_removes_local_only_entry_from_index(self):
        rel = "2026/08/14/delete-to-target-index.png"
        target = self.images_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(png_bytes())
        service = self.service()
        service._save_index({
            rel: {
                "rel": rel,
                "path": rel,
                "name": target.name,
                "storage": "local",
                "local": True,
                "webdav": False,
                "size": target.stat().st_size,
            }
        })

        with (
            mock.patch.object(image_service_module, "image_storage_service", service),
            mock.patch.object(image_service_module, "remove_tags"),
            mock.patch.object(image_service_module, "config") as image_config,
            mock.patch("shutil.disk_usage", return_value=SimpleNamespace(total=100, used=100, free=0)),
        ):
            image_config.images_dir = self.images_dir
            image_config.image_thumbnails_dir = self.data_dir / "thumbnails"
            result = image_service_module.delete_to_target(1)

        self.assertEqual(result["removed"], 1)
        self.assertFalse(target.exists())
        self.assertNotIn(rel, service._load_clean_index())

    def test_delete_to_target_preserves_remote_only_entry_in_index(self):
        rel = "2026/08/14/delete-to-target-remote.png"
        target = self.images_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(png_bytes())
        service = self.service()
        service._save_index({
            rel: {
                "rel": rel,
                "path": rel,
                "name": target.name,
                "storage": "both",
                "local": True,
                "webdav": True,
                "size": target.stat().st_size,
            }
        })

        with (
            mock.patch.object(image_service_module, "image_storage_service", service),
            mock.patch.object(image_service_module, "remove_tags"),
            mock.patch.object(image_service_module, "config") as image_config,
            mock.patch("shutil.disk_usage", return_value=SimpleNamespace(total=100, used=100, free=0)),
        ):
            image_config.images_dir = self.images_dir
            image_config.image_thumbnails_dir = self.data_dir / "thumbnails"
            result = image_service_module.delete_to_target(1)

        self.assertEqual(result["removed"], 1)
        self.assertFalse(target.exists())
        self.assertEqual(
            service._load_clean_index()[rel]["storage"],
            "webdav",
        )
        self.assertFalse(service._load_clean_index()[rel]["local"])
        self.assertTrue(service._load_clean_index()[rel]["webdav"])

    def test_compress_waits_for_same_rel_save(self):
        rel = "2026/08/14/compress-race.png"
        old_buffer = io.BytesIO()
        Image.new("RGB", (256, 256), color=(10, 20, 30)).save(old_buffer, format="PNG")
        old_payload = old_buffer.getvalue()
        new_payload = png_bytes()
        target = self.images_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(old_payload)
        service = self.service()
        compress_write_started = threading.Event()
        release_compress = threading.Event()
        save_done = threading.Event()
        errors: list[BaseException] = []
        real_atomic_write = image_service_module.atomic_write_bytes

        def atomic_write(path, root_path, payload):
            compress_write_started.set()
            if not release_compress.wait(2):
                raise AssertionError("compression write was not released")
            return real_atomic_write(path, root_path, payload)

        def run_save() -> None:
            try:
                service.save(new_payload, "http://app.test")
            except BaseException as exc:
                errors.append(exc)
            finally:
                save_done.set()

        with (
            mock.patch.object(service, "make_relative_path", return_value=rel),
            mock.patch("services.image_service.config") as image_config,
            mock.patch("services.image_storage_service.config") as storage_config,
            mock.patch.object(image_service_module, "image_storage_service", service),
            mock.patch.object(image_service_module, "atomic_write_bytes", side_effect=atomic_write),
        ):
            image_config.images_dir = self.images_dir
            image_config.image_thumbnails_dir = self.data_dir / "thumbnails"
            storage_config.images_dir = self.images_dir
            storage_config.get_image_storage_settings.return_value = {"mode": "local"}
            compress_thread = threading.Thread(target=image_service_module.compress_images)
            compress_thread.start()
            self.assertTrue(compress_write_started.wait(2))
            save_thread = threading.Thread(target=run_save)
            save_thread.start()
            if save_done.wait(1):
                # Without the rel owner, save can publish before compression
                # writes its stale snapshot back over the new file.
                release_compress.set()
            else:
                release_compress.set()
            compress_thread.join(3)
            save_thread.join(3)

        self.assertFalse(compress_thread.is_alive())
        self.assertFalse(save_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertTrue(save_done.is_set())
        self.assertEqual(target.read_bytes(), new_payload)

    def test_test_webdav_writes_and_deletes_probe_file(self):
        self.settings.update({
            "enabled": True,
            "mode": "webdav",
            "webdav_url": "https://dav.example.test",
            "webdav_password": "secret",
        })
        with mock.patch("services.image_storage_service.WebDAVClient", FakeWebDAVClient):
            result = self.service().test_webdav()

        self.assertTrue(result["ok"])
        self.assertIn(".chatgpt2api_webdav_test.txt", FakeWebDAVClient.deleted)


if __name__ == "__main__":
    unittest.main()
