from __future__ import annotations

import unittest
import json
from tempfile import TemporaryDirectory
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
import services.cpa_service as cpa_module


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class CPAAPIPublicErrorTests(unittest.TestCase):
    def test_cpa_config_rejects_unsafe_base_url_before_persisting(self) -> None:
        unsafe_urls = (
            "file:///private/cpa",
            "https://user:password@cpa.example.test",
            "https://cpa.example.test/root?token=management-secret",
            "https://cpa.example.test/root#fragment",
        )

        for index, base_url in enumerate(unsafe_urls):
            with self.subTest(base_url=base_url), TemporaryDirectory() as temp_dir:
                store_file = cpa_module.Path(temp_dir) / f"cpa-{index}.json"
                config = cpa_module.CPAConfig(store_file)

                with self.assertRaises(cpa_module.PublicSafeValueError):
                    config.add_pool("preview", base_url, "management-secret")

                self.assertEqual(config.list_pools(), [])
                if store_file.exists():
                    self.assertNotIn("management-secret", store_file.read_text(encoding="utf-8"))

    def test_cpa_config_rejects_persisted_unsafe_base_url(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_file = cpa_module.Path(temp_dir) / "cpa.json"
            store_file.write_text(
                json.dumps([{
                    "id": "pool-1",
                    "name": "preview",
                    "base_url": "https://user:password@cpa.example.test",
                    "secret_key": "management-secret",
                    "import_job": None,
                }]),
                encoding="utf-8",
            )

            with self.assertRaises(cpa_module.StorageDataError):
                cpa_module.CPAConfig(store_file)

    def test_cpa_config_does_not_read_replaced_snapshot_path(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = cpa_module.Path(temp_dir)
            store_file = root / "cpa.json"
            outside = root / "outside.json"
            original_payload = [{
                "id": "pool-original",
                "name": "original",
                "base_url": "https://cpa.example.test",
                "secret_key": "secret-original",
                "import_job": None,
            }]
            outside_payload = [{
                "id": "pool-replaced",
                "name": "replaced",
                "base_url": "https://cpa.example.test",
                "secret_key": "secret-replaced",
                "import_job": None,
            }]
            store_file.write_text(json.dumps(original_payload), encoding="utf-8")
            outside.write_text(json.dumps(outside_payload), encoding="utf-8")
            original_read_text = cpa_module.Path.read_text

            def replace_before_path_read(path, *args, **kwargs):
                if path == store_file and store_file.exists():
                    store_file.unlink()
                    outside.replace(store_file)
                return original_read_text(path, *args, **kwargs)

            with mock.patch.object(
                cpa_module.Path,
                "read_text",
                autospec=True,
                side_effect=replace_before_path_read,
            ):
                config = cpa_module.CPAConfig(store_file)

            self.assertEqual([item["id"] for item in config.list_pools()], ["pool-original"])

    def test_cpa_create_invalid_base_url_is_a_public_bad_request(self) -> None:
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        with TemporaryDirectory() as temp_dir:
            config = cpa_module.CPAConfig(cpa_module.Path(temp_dir) / "cpa.json")
            with (
                mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
                mock.patch.object(accounts_module, "cpa_config", config),
            ):
                response = TestClient(app).post(
                    "/api/cpa/pools",
                    headers=AUTH_HEADERS,
                    json={
                        "name": "preview",
                        "base_url": "file:///private/cpa",
                        "secret_key": "management-secret",
                    },
                )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn("management-secret", response.text)
        self.assertIn("CPA base URL", response.text)

    def test_import_rejects_container_file_name_before_scheduling(self) -> None:
        canary = "cpa-import-file-container-canary"
        service = cpa_module.CPAImportService(mock.Mock())

        with self.assertRaises(cpa_module.PublicSafeValueError):
            service.start_import(
                {"id": "pool-1"},
                [{"secret": canary}],
            )

    def test_import_deduplicates_selected_file_names_before_scheduling(self) -> None:
        config = mock.Mock()
        config.begin_import_job.side_effect = lambda _pool_id, job: {
            "id": "pool-1",
            "import_job": job,
        }
        reservation = mock.Mock()
        service = cpa_module.CPAImportService(config)

        with mock.patch.object(cpa_module, "reserve_background_task", return_value=reservation):
            job = service.start_import(
                {"id": "pool-1"},
                ["file-a", "file-a", "file-b", "file-b"],
            )

        assert job["total"] == 2
        reservation.submit.assert_called_once()
        assert reservation.submit.call_args.args[3] == ["file-a", "file-b"]

    def test_remote_file_listing_does_not_stringify_container_fields(self) -> None:
        canary = "cpa-file-container-canary"

        class Session:
            def __init__(self, **_kwargs):
                self.closed = False

            def get(self, *_args, **_kwargs):
                return _Response({
                    "files": [
                        {"name": {"secret": canary}, "email": "skip"},
                        {"name": "safe-file", "email": [canary], "account": {"secret": canary}},
                    ],
                })

            def close(self):
                self.closed = True

        class _Response:
            def __init__(self, payload):
                self.ok = True
                self._payload = payload
                self.content = json.dumps(payload).encode("utf-8")

            def json(self):
                return self._payload

        session = Session()
        with (
            mock.patch.object(cpa_module, "Session", lambda **_kwargs: session),
            mock.patch.object(cpa_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            files = cpa_module.list_remote_files({
                "base_url": "https://cpa.example.test",
                "secret_key": "management-secret",
            })

        self.assertEqual(files, [{"name": "safe-file", "email": ""}])
        self.assertNotIn(canary, repr(files))
        self.assertTrue(session.closed)

    def test_remote_file_listing_bounds_public_text_fields(self) -> None:
        oversized = "x" * 257

        class Session:
            def __init__(self, **_kwargs):
                self.closed = False

            def get(self, *_args, **_kwargs):
                return _Response({
                    "files": [{
                        "name": oversized,
                        "email": oversized,
                        "account": oversized,
                    }],
                })

            def close(self):
                self.closed = True

        class _Response:
            ok = True

            def __init__(self, payload):
                self._payload = payload
                self.content = json.dumps(payload).encode("utf-8")

            def json(self):
                return self._payload

        session = Session()
        with (
            mock.patch.object(cpa_module, "Session", lambda **_kwargs: session),
            mock.patch.object(cpa_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            files = cpa_module.list_remote_files({
                "base_url": "https://cpa.example.test",
                "secret_key": "management-secret",
            })

        self.assertEqual(files, [])
        self.assertNotIn(oversized, repr(files))

    def test_remote_file_download_does_not_stringify_container_token(self) -> None:
        canary = "cpa-token-container-canary"

        class Session:
            def __init__(self, **_kwargs):
                self.closed = False

            def get(self, *_args, **_kwargs):
                return _Response()

            def close(self):
                self.closed = True

        class _Response:
            ok = True

            def __init__(self):
                self.content = json.dumps({"access_token": {"secret": canary}}).encode("utf-8")

            def json(self):
                return {"access_token": {"secret": canary}}

        session = Session()
        with (
            mock.patch.object(cpa_module, "Session", lambda **_kwargs: session),
            mock.patch.object(cpa_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            token, error = cpa_module.fetch_remote_access_token(
                {"base_url": "https://cpa.example.test", "secret_key": "management-secret"},
                "safe-file",
            )

        self.assertIsNone(token)
        self.assertEqual(error, "missing access_token")
        self.assertNotIn(canary, repr((token, error)))
        self.assertTrue(session.closed)

    def test_remote_file_download_closes_http_error_response(self) -> None:
        response = mock.Mock(ok=False, status_code=502, iter_content=None)
        session = mock.Mock()
        session.get.return_value = response

        with (
            mock.patch.object(cpa_module, "Session", return_value=session),
            mock.patch.object(cpa_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            token, error = cpa_module.fetch_remote_access_token(
                {"base_url": "https://cpa.example.test", "secret_key": "management-secret"},
                "safe-file",
            )

        self.assertIsNone(token)
        self.assertEqual(error, "HTTP 502")
        response.close.assert_called_once_with()
        session.get.assert_called_once()
        self.assertTrue(session.get.call_args.kwargs["stream"])

    def test_remote_file_download_rejects_nonstring_file_name_before_network(self) -> None:
        canary = "cpa-selected-file-container-canary"
        session = mock.Mock()
        with (
            mock.patch.object(cpa_module, "Session", side_effect=AssertionError("network must not start")),
            mock.patch.object(cpa_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            token, error = cpa_module.fetch_remote_access_token(
                {"base_url": "https://cpa.example.test", "secret_key": "management-secret"},
                {"secret": canary},
            )

        self.assertIsNone(token)
        self.assertEqual(error, "invalid request")

    def test_remote_file_listing_closes_response_after_json_parse(self) -> None:
        response = mock.Mock(ok=True, iter_content=None)
        response.content = b'{"files":[]}'
        response.json.return_value = {"files": []}
        session = mock.Mock()
        session.get.return_value = response

        with (
            mock.patch.object(cpa_module, "Session", return_value=session),
            mock.patch.object(cpa_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            self.assertEqual(
                cpa_module.list_remote_files({
                    "base_url": "https://cpa.example.test",
                    "secret_key": "management-secret",
                }),
                [],
            )

        response.close.assert_called_once_with()

    def test_remote_file_listing_rejects_more_than_bounded_items(self) -> None:
        class Response:
            ok = True

            @property
            def content(self):
                return json.dumps(self.json()).encode("utf-8")

            def json(self):
                return {
                    "files": [
                        {"name": f"account-{index}.json", "email": ""}
                        for index in range(5001)
                    ],
                }

            def close(self):
                pass

        class Session:
            def get(self, *_args, **_kwargs):
                return Response()

            def close(self):
                pass

        with (
            mock.patch.object(cpa_module, "Session", return_value=Session()),
            mock.patch.object(cpa_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            with self.assertRaisesRegex(RuntimeError, "remote file limit exceeded"):
                cpa_module.list_remote_files({
                    "base_url": "https://cpa.example.test",
                    "secret_key": "management-secret",
                })

    def test_file_listing_projects_upstream_failure_to_fixed_502(self) -> None:
        secret = "opaque-cpa-upstream-secret owner@example.com"
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        pool = {
            "id": "pool-1",
            "name": "CPA",
            "base_url": "https://cpa.example.test",
            "secret_key": "management-secret",
        }

        with (
            mock.patch.object(accounts_module.cpa_config, "get_pool", return_value=pool),
            mock.patch.object(accounts_module, "list_remote_files", side_effect=RuntimeError(secret)),
        ):
            response = TestClient(app, raise_server_exceptions=False).get(
                "/api/cpa/pools/pool-1/files",
                headers=AUTH_HEADERS,
            )

        self.assertEqual(response.status_code, 502, response.text)
        self.assertNotIn(secret, response.text)
        self.assertNotIn("owner@example.com", response.text)

    def test_import_start_response_uses_public_job_projection(self) -> None:
        canary = "cpa-import-start-canary owner@example.com"
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        pool = {
            "id": "pool-1",
            "name": "CPA",
            "base_url": "https://cpa.example.test",
            "secret_key": "management-secret",
        }
        job = {
            "job_id": "job-1",
            "status": canary,
            "total": 1,
            "errors": [{"name": canary, "error": canary}],
            "internal_metadata": {"secret": canary},
        }
        with (
            mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
            mock.patch.object(accounts_module.cpa_config, "get_pool", return_value=pool),
            mock.patch.object(accounts_module.cpa_import_service, "start_import", return_value=job),
        ):
            response = TestClient(app).post(
                "/api/cpa/pools/pool-1/import",
                headers=AUTH_HEADERS,
                json={"names": ["safe-file"]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn(canary, response.text)
        self.assertEqual(response.json()["import_job"]["status"], "failed")


if __name__ == "__main__":
    unittest.main()
