from __future__ import annotations

import json
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


class CCLoadServiceContractTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeCCLoadSession.instances.clear()
        self.server = {
            "id": "server-1",
            "base_url": "https://ccload.example.test",
            "password": "admin-password-secret",
        }

    def test_lists_only_public_codex_oauth_channel_metadata_and_revokes_session(self) -> None:
        with mock.patch.object(ccload_module, "Session", _FakeCCLoadSession):
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
                    "models": ["gpt-5.4"],
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
