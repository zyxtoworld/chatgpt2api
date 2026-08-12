from __future__ import annotations

import json
import threading
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import services.ccload_service as ccload_module
from services.backup_service import BackupService
from services.storage.base import StorageDataError


class _FakeResponse:
    def __init__(self, payload: object, status_code: int = 200):
        self._payload = payload
        self.status_code = status_code
        self.ok = 200 <= status_code < 300

    def json(self):
        return self._payload


class _FakeCCLoadSession:
    instances: list["_FakeCCLoadSession"] = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.closed = False
        type(self).instances.append(self)

    def post(self, url: str, **kwargs):
        self.calls.append(("POST", url, kwargs))
        if url.endswith("/login"):
            return _FakeResponse(
                {
                    "success": True,
                    "data": {"token": "temporary-session-token", "expiresIn": 3600, "role": "admin"},
                }
            )
        if url.endswith("/logout"):
            return _FakeResponse({"success": True, "data": {}})
        raise AssertionError(f"unexpected POST {url}")

    def get(self, url: str, **kwargs):
        self.calls.append(("GET", url, kwargs))
        if url.endswith("/admin/channels"):
            return _FakeResponse(
                {
                    "success": True,
                    "data": [
                        {
                            "id": 7,
                            "name": "Codex Pro",
                            "auth_type": "codex_oauth",
                            "enabled": True,
                            "codex_plan_type": "pro",
                            "codex_subscription_active_until": "2027-01-02T03:04:05Z",
                            "models": [{"model": "gpt-5.4"}],
                            "oauth_usage": {"primary": {"used_percent": 25.0}},
                        }
                    ],
                    "count": 1,
                }
            )
        if url.endswith("/admin/channels/7/editor"):
            return _FakeResponse(
                {
                    "success": True,
                    "data": {
                        "channel": {"id": 7, "name": "Codex Pro", "auth_type": "codex_oauth"},
                        "oauth_credential": {
                            "id_token": "id-token-secret",
                            "access_token": "access-token-secret",
                            "refresh_token": "refresh-token-secret",
                            "account_id": "account-7",
                            "email": "user@example.test",
                            "type": "codex",
                            "expired": "2026-08-09T12:00:00Z",
                            "plan_type": "pro",
                        },
                    },
                }
            )
        raise AssertionError(f"unexpected GET {url}")

    def close(self):
        self.closed = True


class _FakeChannelModelBackend:
    instances: list["_FakeChannelModelBackend"] = []

    def __init__(self, access_token: str = ""):
        self.access_token = access_token
        self.closed = False
        type(self).instances.append(self)

    def list_models(self, **_kwargs):
        if self.access_token != "access-token-secret":
            raise AssertionError("channel access token was not used")
        return {
            "object": "list",
            "data": [
                {"id": "gpt-5.4"},
                {"id": "gpt-5.4-pro"},
                {"id": "gpt-5.4-pro"},
            ],
        }

    def close(self):
        self.closed = True


class CCLoadServiceContractTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeCCLoadSession.instances.clear()
        _FakeChannelModelBackend.instances.clear()
        self.server = {
            "id": "server-1",
            "base_url": "https://ccload.example.test",
            "password": "admin-password-secret",
        }

    def test_lists_only_public_codex_oauth_channel_metadata_and_revokes_session(self) -> None:
        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(
                ccload_module,
                "OpenAIBackendAPI",
                _FakeChannelModelBackend,
                create=True,
            ),
        ):
            channels = ccload_module.list_remote_channels(self.server)

        self.assertEqual(
            channels,
            [
                {
                    "id": "7",
                    "name": "Codex Pro",
                    "enabled": True,
                    "plan_type": "pro",
                    "subscription_active_until": "2027-01-02T03:04:05Z",
                    "models": [],
                    "models_loaded": False,
                }
            ],
        )
        serialized = repr(channels)
        self.assertNotIn("temporary-session-token", serialized)
        self.assertNotIn("admin-password-secret", serialized)
        session = _FakeCCLoadSession.instances[0]
        self.assertTrue(session.closed)
        self.assertEqual([call[0:2] for call in session.calls], [
            ("POST", "https://ccload.example.test/login"),
            ("GET", "https://ccload.example.test/admin/channels"),
            ("POST", "https://ccload.example.test/logout"),
        ])
        list_kwargs = session.calls[1][2]
        self.assertEqual(list_kwargs["headers"]["Authorization"], "Bearer temporary-session-token")
        self.assertEqual(list_kwargs["params"], {"auth_type": "codex_oauth", "limit": 200, "offset": 0})
        self.assertEqual(_FakeChannelModelBackend.instances, [])

    def test_loads_models_only_for_the_requested_channel_batch(self) -> None:
        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "OpenAIBackendAPI", _FakeChannelModelBackend),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["7"])

        self.assertEqual(channels, [{
            "id": "7",
            "models": ["gpt-5.4", "gpt-5.4-pro"],
            "models_loaded": True,
        }])
        session = _FakeCCLoadSession.instances[0]
        self.assertEqual([call[0:2] for call in session.calls], [
            ("POST", "https://ccload.example.test/login"),
            ("GET", "https://ccload.example.test/admin/channels/7/editor"),
            ("POST", "https://ccload.example.test/logout"),
        ])
        self.assertEqual([backend.access_token for backend in _FakeChannelModelBackend.instances], ["access-token-secret"])
        self.assertTrue(_FakeChannelModelBackend.instances[0].closed)

    def test_model_batch_rejects_more_than_fifty_channels_before_remote_io(self) -> None:
        with mock.patch.object(ccload_module, "Session", _FakeCCLoadSession):
            with self.assertRaisesRegex(ccload_module.CCLoadError, "at most 50"):
                ccload_module.list_remote_channel_models(
                    self.server,
                    [str(index) for index in range(1, 52)],
                )

        self.assertEqual(_FakeCCLoadSession.instances, [])

    def test_each_channel_uses_its_own_authenticated_model_catalog(self) -> None:
        class TwoChannelSession(_FakeCCLoadSession):
            instances: list["TwoChannelSession"] = []

            def get(self, url: str, **kwargs):
                self.calls.append(("GET", url, kwargs))
                if url.endswith("/admin/channels"):
                    return _FakeResponse({
                        "success": True,
                        "data": [
                            {"id": 1, "name": "Free", "auth_type": "codex_oauth", "enabled": True, "codex_plan_type": "free"},
                            {"id": 2, "name": "Pro", "auth_type": "codex_oauth", "enabled": True, "codex_plan_type": "pro"},
                        ],
                        "count": 2,
                    })
                channel_id = "1" if url.endswith("/admin/channels/1/editor") else "2"
                if not url.endswith(f"/admin/channels/{channel_id}/editor"):
                    raise AssertionError(f"unexpected GET {url}")
                return _FakeResponse({
                    "success": True,
                    "data": {
                        "channel": {"id": int(channel_id), "auth_type": "codex_oauth"},
                        "oauth_credential": {
                            "id_token": "",
                            "access_token": f"token-{channel_id}",
                            "refresh_token": f"refresh-{channel_id}",
                            "account_id": f"account-{channel_id}",
                            "email": f"user-{channel_id}@example.test",
                            "type": "codex",
                            "expired": "2027-08-09T12:00:00Z",
                            "plan_type": "free" if channel_id == "1" else "pro",
                        },
                    },
                })

        class PerChannelBackend:
            instances: list["PerChannelBackend"] = []

            def __init__(self, access_token: str = ""):
                self.access_token = access_token
                self.closed = False
                type(self).instances.append(self)

            def list_models(self, **_kwargs):
                models = ["common", "free-model"] if self.access_token == "token-1" else [
                    "common",
                    "free-model",
                    "pro-model",
                ]
                return {"object": "list", "data": [{"id": model} for model in models]}

            def close(self):
                self.closed = True

        with (
            mock.patch.object(ccload_module, "Session", TwoChannelSession),
            mock.patch.object(ccload_module, "OpenAIBackendAPI", PerChannelBackend),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["1", "2"])

        self.assertEqual(channels, [
            {"id": "1", "models": ["common", "free-model"], "models_loaded": True},
            {"id": "2", "models": ["common", "free-model", "pro-model"], "models_loaded": True},
        ])
        self.assertEqual([backend.access_token for backend in PerChannelBackend.instances], ["token-1", "token-2"])
        self.assertTrue(all(backend.closed for backend in PerChannelBackend.instances))
        self.assertEqual(len(TwoChannelSession.instances), 1)
        self.assertEqual(
            [call[0:2] for call in TwoChannelSession.instances[0].calls],
            [
                ("POST", "https://ccload.example.test/login"),
                ("GET", "https://ccload.example.test/admin/channels/1/editor"),
                ("GET", "https://ccload.example.test/admin/channels/2/editor"),
                ("POST", "https://ccload.example.test/logout"),
            ],
        )

    def test_enabled_channel_model_catalogs_are_loaded_concurrently(self) -> None:
        active = 0
        maximum_active = 0
        both_started = threading.Event()
        lock = threading.Lock()

        def model_ids(access_token: str, **_kwargs):
            nonlocal active, maximum_active
            with lock:
                active += 1
                maximum_active = max(maximum_active, active)
                if active == 2:
                    both_started.set()
            both_started.wait(timeout=0.25)
            with lock:
                active -= 1
            return [f"model-{access_token}"]

        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            return {"access_token": f"token-{channel_id}"}

        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(ccload_module, "_channel_model_ids", side_effect=model_ids),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["1", "2"])

        self.assertEqual(maximum_active, 2)
        self.assertEqual([item["models"] for item in channels], [["model-token-1"], ["model-token-2"]])

    def test_channel_browse_deadline_fails_before_a_blocked_model_catalog_finishes(self) -> None:
        started = threading.Event()
        release = threading.Event()

        def blocked_model_ids(_access_token: str, **_kwargs):
            started.set()
            release.wait(timeout=1)
            return ["late-model"]

        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "CCLOAD_CHANNEL_BROWSE_TIMEOUT_SECS", 0.05),
            mock.patch.object(ccload_module, "_channel_model_ids", side_effect=blocked_model_ids),
        ):
            before = time.monotonic()
            try:
                with self.assertRaisesRegex(ccload_module.CCLoadError, "timed out"):
                    ccload_module.list_remote_channel_models(self.server, ["7"])
            finally:
                release.set()

        self.assertTrue(started.is_set())
        self.assertLess(time.monotonic() - before, 0.5)

    def test_one_unavailable_channel_does_not_hide_other_channel_catalogs(self) -> None:
        class MixedChannelSession(_FakeCCLoadSession):
            instances: list["MixedChannelSession"] = []

            def get(self, url: str, **kwargs):
                self.calls.append(("GET", url, kwargs))
                if not url.endswith("/admin/channels"):
                    raise AssertionError(f"unexpected GET {url}")
                return _FakeResponse({
                    "success": True,
                    "data": [
                        {"id": 1, "name": "Disabled", "auth_type": "codex_oauth", "enabled": False, "codex_plan_type": "free"},
                        {"id": 2, "name": "Broken", "auth_type": "codex_oauth", "enabled": True, "codex_plan_type": "free"},
                        {"id": 3, "name": "Pro", "auth_type": "codex_oauth", "enabled": True, "codex_plan_type": "pro"},
                    ],
                    "count": 3,
                })

        model_calls: list[str] = []

        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            if channel_id == "2":
                raise ccload_module.CCLoadError("fixture credential failure")
            return {"access_token": f"token-{channel_id}"}

        def model_ids(access_token: str, **_kwargs):
            model_calls.append(access_token)
            return ["gpt-pro-model"]

        with (
            mock.patch.object(ccload_module, "Session", MixedChannelSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(ccload_module, "_channel_model_ids", side_effect=model_ids),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["2", "3"])

        self.assertEqual(model_calls, ["token-3"])
        self.assertEqual(channels, [
            {"id": "2", "models": [], "models_loaded": True},
            {"id": "3", "models": ["gpt-pro-model"], "models_loaded": True},
        ])

    def test_fetches_selected_complete_codex_credential_and_never_lists_it(self) -> None:
        with mock.patch.object(ccload_module, "Session", _FakeCCLoadSession):
            credentials, errors = ccload_module.fetch_remote_credentials(self.server, ["7"])

        self.assertEqual(errors, [])
        self.assertEqual(
            credentials,
            [
                {
                    "id_token": "id-token-secret",
                    "access_token": "access-token-secret",
                    "refresh_token": "refresh-token-secret",
                    "account_id": "account-7",
                    "email": "user@example.test",
                    "type": "codex",
                    "expired": "2026-08-09T12:00:00Z",
                    "plan_type": "pro",
                }
            ],
        )
        self.assertTrue(_FakeCCLoadSession.instances[0].closed)

    def test_rejects_non_string_or_malformed_codex_credential_fields(self) -> None:
        valid = {
            "id_token": "id-token-secret",
            "access_token": "access-token-secret",
            "refresh_token": "refresh-token-secret",
            "account_id": "account-7",
            "email": "user@example.test",
            "type": "codex",
            "expired": "2026-08-09T12:00:00Z",
            "plan_type": "pro",
        }
        invalid_values = {
            "id_token": {"secret": "id-token-secret"},
            "access_token": {"secret": "access-token-secret"},
            "refresh_token": 123,
            "account_id": ["account-7"],
            "email": {"secret": "user@example.test"},
            "type": ["codex"],
            "expired": "2026-08-09 12:00:00",
            "plan_type": {"secret": "pro"},
        }

        for field, value in invalid_values.items():
            with self.subTest(field=field):
                self.assertIsNone(ccload_module._normalized_codex_credential({**valid, field: value}))

    def test_rejects_missing_required_codex_oauth_tokens(self) -> None:
        valid = {
            "id_token": "id-token-secret",
            "access_token": "access-token-secret",
            "refresh_token": "refresh-token-secret",
            "account_id": "account-7",
            "email": "user@example.test",
            "type": "codex",
            "expired": "2026-08-09T12:00:00Z",
            "plan_type": "pro",
        }

        for field in ("access_token", "refresh_token"):
            with self.subTest(field=field, case="empty"):
                self.assertIsNone(
                    ccload_module._normalized_codex_credential({**valid, field: ""})
                )
            with self.subTest(field=field, case="missing"):
                incomplete = dict(valid)
                incomplete.pop(field)
                self.assertIsNone(ccload_module._normalized_codex_credential(incomplete))

    def test_accepts_current_preview_credential_without_optional_id_token(self) -> None:
        valid = {
            "access_token": "access-token-secret",
            "refresh_token": "refresh-token-secret",
            "account_id": "account-7",
            "email": "user@example.test",
            "type": "codex",
            "expired": "2026-08-09T12:00:00Z",
            "plan_type": "pro",
        }

        missing = ccload_module._normalized_codex_credential(valid)
        empty = ccload_module._normalized_codex_credential({**valid, "id_token": ""})

        self.assertIsNotNone(missing)
        self.assertEqual(missing, empty)
        assert missing is not None
        self.assertEqual(missing["id_token"], "")
        self.assertEqual(missing["access_token"], "access-token-secret")
        self.assertEqual(missing["refresh_token"], "refresh-token-secret")

    def test_current_preview_optional_usage_metadata_does_not_break_import(self) -> None:
        raw = {
            "id_token": "id-token-secret",
            "access_token": "access-token-secret",
            "refresh_token": "refresh-token-secret",
            "account_id": "account-7",
            "email": "user@example.test",
            "type": "codex",
            "expired": "2026-08-09T12:00:00Z",
            "plan_type": "pro",
            "last_refresh": "2026-08-09T11:30:00Z",
            "passive_usage": {
                "sampled_at": "2026-08-09T11:59:00Z",
                "windows": [{"scope": "account", "used_percent": 25.0}],
            },
            "oauth_usage": {"primary": {"used_percent": 25.0}},
        }

        normalized = ccload_module._normalized_codex_credential(raw)

        self.assertIsNotNone(normalized)
        assert normalized is not None
        self.assertEqual(normalized["access_token"], "access-token-secret")
        self.assertEqual(normalized["refresh_token"], "refresh-token-secret")
        self.assertNotIn("passive_usage", normalized)
        self.assertNotIn("oauth_usage", normalized)
        self.assertNotIn("last_refresh", normalized)


class _InlineReservation:
    def submit(self, target, *args, **kwargs) -> None:
        target(*args, **kwargs)

    def cancel(self) -> None:
        pass


class _RecordingAccountService:
    def __init__(self) -> None:
        self.imported: list[dict] = []
        self.refreshed: list[str] = []

    def add_account_items(self, items: list[dict]) -> dict:
        self.imported = [dict(item) for item in items]
        return {"added": len(items), "skipped": 0}

    def refresh_accounts(self, tokens: list[str]) -> dict:
        self.refreshed = list(tokens)
        return {"refreshed": len(tokens)}


class CCLoadPersistenceAndImportContractTests(unittest.TestCase):
    def test_rejects_unsafe_ccload_base_urls_before_persisting(self) -> None:
        unsafe_urls = (
            "ftp://ccload.example.test",
            "https://user:admin-secret@ccload.example.test",
            "https://ccload.example.test/root?next=private",
            "https://ccload.example.test/root#fragment",
            "https://ccload.example.test:bad",
            "https://[::1",
            "//ccload.example.test",
        )

        for index, base_url in enumerate(unsafe_urls):
            with self.subTest(base_url=base_url), TemporaryDirectory() as temp_dir:
                store_file = Path(temp_dir) / f"ccload-{index}.json"
                config = ccload_module.CCLoadConfig(store_file)

                with self.assertRaises(ccload_module.PublicSafeValueError):
                    config.add_server(
                        name="preview",
                        base_url=base_url,
                        password="admin-password-secret",
                    )

                self.assertEqual(config.list_servers(), [])
                if store_file.exists():
                    self.assertNotIn("admin-password-secret", store_file.read_text(encoding="utf-8"))

    def test_invalid_ccload_base_url_update_preserves_existing_snapshot(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            config = ccload_module.CCLoadConfig(store_file)
            server = config.add_server(
                name="preview",
                base_url="http://127.0.0.1:3000/ccload/",
                password="admin-password-secret",
            )
            original = store_file.read_bytes()

            with self.assertRaises(ccload_module.PublicSafeValueError):
                config.update_server(
                    server["id"],
                    {"base_url": "https://user:updated-secret@ccload.example.test"},
                )

            self.assertEqual(store_file.read_bytes(), original)
            self.assertEqual(
                config.get_server(server["id"])["base_url"],
                "http://127.0.0.1:3000/ccload",
            )

    def test_invalid_ccload_base_url_reports_url_error_not_missing_fields(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = ccload_module.CCLoadConfig(Path(temp_dir) / "ccload_config.json")

            with self.assertRaisesRegex(
                ccload_module.PublicSafeValueError,
                "ccLoad base URL must use http or https",
            ):
                config.add_server(
                    name="preview",
                    base_url="ftp://ccload.example.test",
                    password="admin-password-secret",
                )

    def test_invalid_ccload_base_url_update_reports_url_error_not_missing_fields(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = ccload_module.CCLoadConfig(Path(temp_dir) / "ccload_config.json")
            server = config.add_server(
                name="preview",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )

            with self.assertRaisesRegex(
                ccload_module.PublicSafeValueError,
                "ccLoad base URL must use http or https",
            ):
                config.update_server(server["id"], {"base_url": "ftp://ccload.example.test"})

    def test_persisted_unsafe_ccload_base_url_fails_closed(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            store_file.write_text(
                json.dumps(
                    [{
                        "id": "server-1",
                        "name": "preview",
                        "base_url": "file:///private/ccload",
                        "password": "admin-password-secret",
                        "import_job": None,
                    }],
                    ensure_ascii=False,
                ),
                encoding="utf-8",
            )

            with self.assertRaises(StorageDataError):
                ccload_module.CCLoadConfig(store_file)

    def test_restart_persists_unfinished_import_as_failed(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            config = ccload_module.CCLoadConfig(store_file)
            server = config.add_server(
                name="preview",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            config.set_import_job(
                server["id"],
                {
                    "job_id": "interrupted-job",
                    "status": "running",
                    "created_at": "2026-08-09T00:00:00+00:00",
                    "updated_at": "2026-08-09T00:00:01+00:00",
                    "total": 1,
                    "completed": 0,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 0,
                    "errors": [],
                },
            )

            recovered = ccload_module.CCLoadConfig(store_file)

            self.assertEqual(recovered.get_import_job(server["id"])["status"], "failed")
            persisted = ccload_module.CCLoadConfig(store_file)
            self.assertEqual(persisted.get_import_job(server["id"])["status"], "failed")

    def test_second_import_cannot_replace_active_job(self) -> None:
        class DeferredReservation:
            instances: list["DeferredReservation"] = []

            def __init__(self):
                type(self).instances.append(self)

            def submit(self, target, *args, **kwargs) -> None:
                self.target = target
                self.args = args
                self.kwargs = kwargs

            def cancel(self) -> None:
                pass

        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            config = ccload_module.CCLoadConfig(store_file)
            server = config.add_server(
                name="preview",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            service = ccload_module.CCLoadImportService(config)
            with mock.patch.object(
                ccload_module,
                "reserve_background_task",
                side_effect=DeferredReservation,
            ):
                first = service.start_import(server, ["7"])
                with self.assertRaises(ccload_module.PublicSafeValueError):
                    service.start_import(server, ["8"])

            current = config.get_import_job(server["id"])
            self.assertEqual(current["job_id"], first["job_id"])
            self.assertEqual(current["status"], "pending")
            self.assertEqual(len(DeferredReservation.instances), 2)

    def test_backup_archive_includes_ccload_connection_snapshot(self) -> None:
        service = BackupService()
        with mock.patch.object(service, "_add_file_to_archive") as add_file:
            service._build_backup_archive({"include": {"ccload": True}}, trigger="manual")

        self.assertEqual(add_file.call_count, 1)
        _, source, archive_name = add_file.call_args.args
        self.assertEqual(source.name, "ccload_config.json")
        self.assertEqual(archive_name, "data/ccload_config.json")

    def test_config_round_trip_is_atomic_and_corruption_fails_closed(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            config = ccload_module.CCLoadConfig(store_file)
            server = config.add_server(
                name="preview",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )

            reloaded = ccload_module.CCLoadConfig(store_file)
            self.assertEqual(reloaded.get_server(server["id"])["password"], "admin-password-secret")

            original = store_file.read_bytes()
            store_file.write_text('{"broken": true}', encoding="utf-8")
            with self.assertRaises(StorageDataError):
                reloaded.update_server(server["id"], {"name": "must-not-write"})
            self.assertEqual(store_file.read_text(encoding="utf-8"), '{"broken": true}')
            self.assertNotEqual(store_file.read_bytes(), original)
            self.assertEqual(list(store_file.parent.glob(".ccload_config.json.*.tmp")), [])

    def test_config_replace_failure_preserves_previous_snapshot(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            config = ccload_module.CCLoadConfig(store_file)
            config.add_server(
                name="preview",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            original = store_file.read_bytes()

            with mock.patch.object(
                ccload_module,
                "atomic_write_bytes",
                side_effect=OSError("replace failed"),
                create=True,
            ):
                with self.assertRaises(OSError):
                    config.add_server(
                        name="second",
                        base_url="https://second.example.test",
                        password="admin-password-secret",
                    )

            self.assertEqual(store_file.read_bytes(), original)
            self.assertEqual(len(config.list_servers()), 1)
            self.assertEqual(list(store_file.parent.glob(".ccload_config.json.*.tmp")), [])

    def test_import_preserves_complete_credentials_without_persisting_secrets(self) -> None:
        credential = {
            "id_token": "id-token-secret",
            "access_token": "access-token-secret",
            "refresh_token": "refresh-token-secret",
            "account_id": "account-7",
            "email": "user@example.test",
            "type": "codex",
            "expired": "2026-08-09T12:00:00Z",
            "plan_type": "pro",
        }
        accounts = _RecordingAccountService()
        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            config = ccload_module.CCLoadConfig(store_file)
            server = config.add_server(
                name="preview",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            service = ccload_module.CCLoadImportService(config)
            with (
                mock.patch.object(ccload_module, "account_service", accounts),
                mock.patch.object(ccload_module, "fetch_remote_credentials", return_value=([credential], [])),
                mock.patch.object(ccload_module, "reserve_background_task", return_value=_InlineReservation()),
            ):
                service.start_import(server, ["7"])

            job = config.get_import_job(server["id"])
            self.assertEqual(job["status"], "completed")
            self.assertEqual(job["added"], 1)
            self.assertEqual(job["refreshed"], 1)
            self.assertEqual(accounts.imported, [credential])
            self.assertEqual(accounts.refreshed, ["access-token-secret"])
            persisted = store_file.read_text(encoding="utf-8")
            for secret in (
                "id-token-secret",
                "access-token-secret",
                "refresh-token-secret",
                "user@example.test",
            ):
                self.assertNotIn(secret, persisted)

    def test_import_failure_persists_only_fixed_error(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            config = ccload_module.CCLoadConfig(store_file)
            server = config.add_server(
                name="preview",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            service = ccload_module.CCLoadImportService(config)
            with (
                mock.patch.object(
                    ccload_module,
                    "fetch_remote_credentials",
                    side_effect=RuntimeError("opaque-upstream-secret user@example.test"),
                ),
                mock.patch.object(ccload_module, "reserve_background_task", return_value=_InlineReservation()),
            ):
                service.start_import(server, ["7"])

            serialized = repr(config.get_import_job(server["id"])) + store_file.read_text(encoding="utf-8")
            self.assertNotIn("opaque-upstream-secret", serialized)
            self.assertNotIn("user@example.test", serialized)
            self.assertIn("import failed", serialized)


if __name__ == "__main__":
    unittest.main()
