from __future__ import annotations

import json
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.image_storage_service as image_storage_module
import services.image_service as image_service_module
import api.system as system_module
from api.system import create_router
from services.image_storage_service import ImageStorageError, WebDAVClient


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


if __name__ == "__main__":
    unittest.main()
