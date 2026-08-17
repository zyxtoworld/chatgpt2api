from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.image_storage_service as image_storage_module
import services.image_service as image_service_module
import api.system as system_module
from api.system import create_router
from services.image_storage_service import ImageStorageError, PublicImageStorageError, WebDAVClient


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
WEB_DAV_SECRET = "opaque-webdav-token user:password@webdav.example owner@example.com"


class UntrustedImageStorageError(ImageStorageError):
    pass


class ImageStoragePublicContractTests(unittest.TestCase):
    def _client(self) -> WebDAVClient:
        return WebDAVClient(
            {
                "webdav_url": "https://dav.example.test",
                "webdav_username": "dav-user",
                "webdav_password": "dav-password",
                "webdav_root_path": "images",
            }
        )

    def test_webdav_get_streams_with_size_limit_and_closes_response(self) -> None:
        client = self._client()

        class Response:
            status_code = 200
            headers = {"Content-Length": "11"}

            @property
            def content(self):
                raise AssertionError("WebDAV get must not read response.content")

            def iter_content(self, chunk_size=None):
                self.chunk_size = chunk_size
                yield b"hello"
                yield b" world"

            def close(self):
                self.closed = True

        response = Response()
        with mock.patch.object(client.session, "request", return_value=response) as request:
            self.assertEqual(client.get("2026/01/01/image.png"), b"hello world")
        self.assertEqual(response.chunk_size, 1024 * 1024)
        self.assertTrue(response.closed)
        self.assertIsNotNone(request.call_args)
        self.assertTrue(request.call_args.kwargs["stream"])

    def test_webdav_request_closes_http_error_response_before_raising(self) -> None:
        client = self._client()

        class Response:
            status_code = 502

            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        response = Response()
        with mock.patch.object(client.session, "request", return_value=response):
            with self.assertRaises(PublicImageStorageError):
                client._request("GET", client.remote_url("image.png"), stream=True)

        self.assertTrue(response.closed)

    def test_webdav_put_redirect_is_not_treated_as_success(self) -> None:
        client = self._client()

        class Response:
            status_code = 302

            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        response = Response()
        with (
            mock.patch.object(client, "ensure_dirs"),
            mock.patch.object(client.session, "request", return_value=response),
        ):
            with self.assertRaises(PublicImageStorageError):
                client.put("image.png", b"payload")

        self.assertTrue(response.closed)

    def test_webdav_mkcol_and_delete_close_responses(self) -> None:
        client = self._client()

        class Response:
            def __init__(self, status_code: int) -> None:
                self.status_code = status_code
                self.closed = False

            def close(self) -> None:
                self.closed = True

        mkcol_responses = [Response(201), Response(405), Response(201)]
        with mock.patch.object(client.session, "request", side_effect=mkcol_responses) as request:
            client.ensure_dirs("2026/01/image.png")
        self.assertTrue(all(response.closed for response in mkcol_responses))
        self.assertTrue(all(call.kwargs["stream"] for call in request.call_args_list))

        delete_response = Response(204)
        with mock.patch.object(client.session, "request", return_value=delete_response) as request:
            self.assertTrue(client.delete("2026/01/image.png"))
        self.assertTrue(delete_response.closed)
        self.assertTrue(request.call_args.kwargs["stream"])

        put_response = Response(201)
        with (
            mock.patch.object(client, "ensure_dirs"),
            mock.patch.object(client.session, "request", return_value=put_response) as request,
        ):
            self.assertTrue(client.put("image.png", b"payload").endswith("image.png"))
        self.assertTrue(put_response.closed)
        self.assertTrue(request.call_args.kwargs["stream"])

    def test_webdav_mkcol_redirect_is_not_treated_as_success(self) -> None:
        client = self._client()

        class Response:
            status_code = 302

            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        response = Response()
        with mock.patch.object(client.session, "request", return_value=response):
            with self.assertRaises(PublicImageStorageError):
                client.ensure_dirs("2026/01/image.png")

        self.assertTrue(response.closed)

    def test_webdav_remote_exists_is_streamed_closed_and_fail_closed(self) -> None:
        client = self._client()

        class Response:
            def __init__(self, status_code: int) -> None:
                self.status_code = status_code
                self.closed = False

            def close(self) -> None:
                self.closed = True

        present = Response(200)
        with mock.patch.object(client.session, "request", return_value=present) as request:
            self.assertTrue(client.remote_exists("2026/01/01/image.png"))
        self.assertTrue(present.closed)
        self.assertEqual(request.call_args.args[0], "HEAD")
        self.assertTrue(request.call_args.kwargs["stream"])

        missing = Response(404)
        with mock.patch.object(client.session, "request", return_value=missing):
            self.assertFalse(client.remote_exists("2026/01/01/image.png"))
        self.assertTrue(missing.closed)

        uncertain = Response(503)
        with mock.patch.object(client.session, "request", return_value=uncertain):
            with self.assertRaises(PublicImageStorageError):
                client.remote_exists("2026/01/01/image.png")
        self.assertTrue(uncertain.closed)

        redirected = Response(302)
        with mock.patch.object(client.session, "request", return_value=redirected):
            with self.assertRaises(PublicImageStorageError):
                client.remote_exists("2026/01/01/image.png")
        self.assertTrue(redirected.closed)

    def test_webdav_mkcol_error_closes_response_before_raising(self) -> None:
        client = self._client()

        class Response:
            status_code = 500

            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        response = Response()
        with mock.patch.object(client.session, "request", return_value=response):
            with self.assertRaises(PublicImageStorageError):
                client.ensure_dirs("2026/01/image.png")
        self.assertTrue(response.closed)

    def test_webdav_get_rejects_actual_stream_overflow_and_closes_response(self) -> None:
        client = self._client()

        class Response:
            status_code = 200
            headers = {"Content-Length": "4"}

            def iter_content(self, chunk_size=None):
                yield b"12345"

            def close(self):
                self.closed = True

        response = Response()
        with (
            mock.patch.object(client.session, "request", return_value=response),
            mock.patch.object(image_storage_module, "_MAX_WEBDAV_IMAGE_BYTES", 4),
        ):
            with self.assertRaisesRegex(ImageStorageError, "too large"):
                client.get("2026/01/01/image.png")
        self.assertTrue(response.closed)

    def test_webdav_probe_does_not_return_untrusted_exception_text(self) -> None:
        for exception in (
            UntrustedImageStorageError(WEB_DAV_SECRET),
            RuntimeError(WEB_DAV_SECRET),
        ):
            with self.subTest(exception=type(exception).__name__):
                client = self._client()
                self.addCleanup(client.session.close)
                with mock.patch.object(client, "put", side_effect=exception):
                    result = client.test()

                self.assertFalse(result["ok"])
                self.assertEqual(result["error"], "WebDAV 测试失败，请稍后重试")
                self.assertNotIn(WEB_DAV_SECRET, json.dumps(result, ensure_ascii=False))

    def test_webdav_probe_api_does_not_return_untrusted_exception_text(self) -> None:
        app = FastAPI()
        app.include_router(create_router("test"))
        settings = {
            "webdav_url": "https://dav.example.test",
            "webdav_username": "dav-user",
            "webdav_password": "dav-password",
            "webdav_root_path": "images",
        }
        with (
            mock.patch.object(
                image_storage_module.config,
                "get_image_storage_settings",
                return_value=settings,
            ),
            mock.patch.object(WebDAVClient, "put", side_effect=RuntimeError(WEB_DAV_SECRET)),
        ):
            response = TestClient(app).post("/api/image-storage/test", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(WEB_DAV_SECRET, response.text)
        self.assertEqual(response.json()["result"]["error"], "WebDAV 测试失败，请稍后重试")

    def test_image_list_does_not_return_webdav_url_userinfo(self) -> None:
        secret = "webdav-index-secret"
        service = image_storage_module.ImageStorageService()
        raw_remote_url = f"https://dav-user:{secret}@dav.example.test/images/2026/01/01/image.png"
        indexed = {
            "2026/01/01/image.png": {
                "rel": "2026/01/01/image.png",
                "path": "2026/01/01/image.png",
                "name": "image.png",
                "date": "2026-01-01",
                "storage": "webdav",
                "local": False,
                "webdav": True,
                "remote_url": raw_remote_url,
            }
        }
        with (
            mock.patch.object(service, "_load_index", return_value=indexed),
            mock.patch.object(service, "open_local", return_value=None),
            mock.patch.object(service, "_save_index"),
        ):
            items = service.list_items("https://app.example.test")

        self.assertEqual(len(items), 1)
        self.assertNotIn(secret, json.dumps(items, ensure_ascii=False))
        self.assertNotIn("dav-user:", str(items[0].get("remote_url") or ""))

    def test_image_list_drops_unknown_index_fields(self) -> None:
        canary = "image-index-internal-canary owner@example.com"
        service = image_storage_module.ImageStorageService()
        indexed = {
            "2026/01/01/image.png": {
                "rel": "2026/01/01/image.png",
                "path": "2026/01/01/image.png",
                "name": "image.png",
                "date": "2026-01-01",
                "storage": "webdav",
                "local": False,
                "webdav": True,
                "remote_url": "https://dav.example.test/image.png",
                "internal_metadata": canary,
                "nested_secret": {"value": canary},
            }
        }
        with (
            mock.patch.object(service, "_load_index", return_value=indexed),
            mock.patch.object(service, "open_local", return_value=None),
            mock.patch.object(service, "_save_index"),
        ):
            items = service.list_items("https://app.example.test")

        self.assertEqual(len(items), 1)
        self.assertNotIn(canary, json.dumps(items, ensure_ascii=False))
        self.assertNotIn("internal_metadata", items[0])
        self.assertNotIn("nested_secret", items[0])

    def test_api_image_list_does_not_return_webdav_url_userinfo(self) -> None:
        secret = "webdav-api-index-secret"
        service = image_storage_module.ImageStorageService()
        indexed = {
            "2026/01/01/image.png": {
                "rel": "2026/01/01/image.png",
                "path": "2026/01/01/image.png",
                "name": "image.png",
                "date": "2026-01-01",
                "storage": "webdav",
                "local": False,
                "webdav": True,
                "remote_url": f"https://dav-user:{secret}@dav.example.test/image.png",
            }
        }
        app = FastAPI()
        app.include_router(create_router("test"))
        with (
            mock.patch.object(service, "_load_index", return_value=indexed),
            mock.patch.object(service, "open_local", return_value=None),
            mock.patch.object(service, "_save_index"),
            mock.patch.object(image_service_module, "image_storage_service", service),
            mock.patch.object(image_service_module.config, "cleanup_old_images"),
            mock.patch.object(image_service_module, "cleanup_image_thumbnails", return_value=0),
            mock.patch.object(image_service_module, "load_tags", return_value={}),
            mock.patch.object(system_module, "require_admin_async", return_value={"role": "admin"}),
        ):
            response = TestClient(app).get("/api/images")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(secret, response.text)

    def test_api_image_list_does_not_return_webdav_url_query_or_fragment(self) -> None:
        secret = "webdav-query-secret"
        service = image_storage_module.ImageStorageService()
        indexed = {
            "2026/01/01/image.png": {
                "rel": "2026/01/01/image.png",
                "path": "2026/01/01/image.png",
                "name": "image.png",
                "date": "2026-01-01",
                "storage": "webdav",
                "local": False,
                "webdav": True,
                "remote_url": f"https://dav.example.test/image.png?token={secret}#fragment-secret",
            }
        }
        app = FastAPI()
        app.include_router(create_router("test"))
        with (
            mock.patch.object(service, "_load_index", return_value=indexed),
            mock.patch.object(service, "open_local", return_value=None),
            mock.patch.object(service, "_save_index"),
            mock.patch.object(image_service_module, "image_storage_service", service),
            mock.patch.object(image_service_module.config, "cleanup_old_images"),
            mock.patch.object(image_service_module, "cleanup_image_thumbnails", return_value=0),
            mock.patch.object(image_service_module, "load_tags", return_value={}),
            mock.patch.object(system_module, "require_admin_async", return_value={"role": "admin"}),
        ):
            response = TestClient(app).get("/api/images")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(secret, response.text)
        self.assertNotIn("fragment-secret", response.text)

    def test_api_image_list_drops_invalid_date_without_500(self) -> None:
        app = FastAPI()
        app.include_router(create_router("test"))
        with (
            mock.patch.object(image_service_module.config, "cleanup_old_images"),
            mock.patch.object(image_service_module, "cleanup_image_thumbnails", return_value=0),
            mock.patch.object(image_service_module, "load_tags", return_value={}),
            mock.patch.object(
                image_service_module.image_storage_service,
                "list_items",
                return_value=[
                    {
                        "path": "2026/01/01/image.png",
                        "url": "/images/2026/01/01/image.png",
                        "name": "image.png",
                        "created_at": "2026-01-01 00:00:00",
                    }
                ],
            ),
            mock.patch.object(system_module, "require_admin_async", return_value={"role": "admin"}),
        ):
            response = TestClient(app, raise_server_exceptions=False).get("/api/images")

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["groups"], [{"date": "", "items": response.json()["items"]}])

    def test_image_delete_rejects_non_boolean_all_matching_before_delete(self) -> None:
        app = FastAPI()
        app.include_router(create_router("test"))
        with (
            mock.patch.object(system_module, "require_admin_async", new=mock.AsyncMock()),
            mock.patch.object(system_module, "delete_images") as delete_images,
        ):
            response = TestClient(app).post(
                "/api/images/delete",
                headers=AUTH_HEADERS,
                json={"all_matching": "yes"},
            )

        self.assertEqual(response.status_code, 422, response.text)
        delete_images.assert_not_called()

    def test_delete_rejects_non_image_files_inside_image_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            secret = root / "internal-state.json"
            secret.write_text("do-not-delete", encoding="utf-8")
            service = image_storage_module.ImageStorageService(root / "image_index.json")
            with mock.patch.object(
                type(image_storage_module.config),
                "images_dir",
                new_callable=mock.PropertyMock,
                return_value=root,
            ):
                with self.assertRaises(image_storage_module.HTTPException):
                    service.delete("internal-state.json")
            self.assertEqual(secret.read_text(encoding="utf-8"), "do-not-delete")

    def test_thumbnail_cleanup_does_not_delete_non_png_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "thumbnails"
            root.mkdir()
            protected = root / "thumbnail-cache.json"
            protected.write_text("do-not-delete", encoding="utf-8")
            with mock.patch.object(
                type(image_storage_module.config),
                "image_thumbnails_dir",
                new_callable=mock.PropertyMock,
                return_value=root,
            ):
                image_service_module.cleanup_image_thumbnails()
            self.assertEqual(protected.read_text(encoding="utf-8"), "do-not-delete")


if __name__ == "__main__":
    unittest.main()
