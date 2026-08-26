from __future__ import annotations

import json
import threading
import time
import unittest
from contextlib import contextmanager
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

    @property
    def content(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

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
        self.assertTrue(list_kwargs["stream"])
        self.assertEqual(_FakeChannelModelBackend.instances, [])

    def test_response_payload_closes_response_after_successful_parse(self) -> None:
        response = mock.Mock(ok=True, status_code=200, iter_content=None)
        response.content = b'{"success":true,"data":{}}'
        response.json.return_value = {"success": True, "data": {}}

        payload = ccload_module._response_payload(response, "fixture")

        self.assertEqual(payload, {"success": True, "data": {}})
        response.close.assert_called_once_with()

    def test_response_payload_closes_response_when_json_is_invalid(self) -> None:
        response = mock.Mock(ok=True, status_code=200, iter_content=None)
        response.content = b"not-json"
        response.json.side_effect = ValueError("malformed")

        with self.assertRaises(ccload_module.CCLoadError):
            ccload_module._response_payload(response, "fixture")

        response.close.assert_called_once_with()

    def test_channel_metadata_rejects_container_values_instead_of_stringifying_them(self) -> None:
        canary = "ccload-channel-metadata-canary"

        class MalformedMetadataSession(_FakeCCLoadSession):
            instances: list["MalformedMetadataSession"] = []

            def get(self, url: str, **kwargs):
                self.calls.append(("GET", url, kwargs))
                if not url.endswith("/admin/channels"):
                    raise AssertionError(f"unexpected GET {url}")
                return _FakeResponse({
                    "success": True,
                    "data": [{
                        "id": 9,
                        "name": {"secret": canary},
                        "auth_type": "codex_oauth",
                        "enabled": True,
                        "codex_plan_type": [canary],
                        "codex_subscription_active_until": {"secret": canary},
                    }],
                    "count": 1,
                })

        with mock.patch.object(ccload_module, "Session", MalformedMetadataSession):
            channels = ccload_module.list_remote_channels(self.server)

        self.assertEqual(channels, [{
            "id": "9",
            "name": "",
            "enabled": True,
            "plan_type": "",
            "subscription_active_until": "",
            "models": [],
            "models_loaded": False,
        }])
        self.assertNotIn(canary, repr(channels))

    def test_channel_metadata_bounds_public_text_fields(self) -> None:
        oversized = "x" * 257

        class OversizedMetadataSession(_FakeCCLoadSession):
            instances: list["OversizedMetadataSession"] = []

            def get(self, url: str, **kwargs):
                self.calls.append(("GET", url, kwargs))
                if not url.endswith("/admin/channels"):
                    raise AssertionError(f"unexpected GET {url}")
                return _FakeResponse({
                    "success": True,
                    "data": [{
                        "id": 9,
                        "name": oversized,
                        "auth_type": "codex_oauth",
                        "enabled": True,
                        "codex_plan_type": oversized,
                        "codex_subscription_active_until": oversized,
                    }],
                    "count": 1,
                })

        with mock.patch.object(ccload_module, "Session", OversizedMetadataSession):
            channels = ccload_module.list_remote_channels(self.server)

        self.assertEqual(channels, [{
            "id": "9",
            "name": "",
            "enabled": True,
            "plan_type": "",
            "subscription_active_until": "",
            "models": [],
            "models_loaded": False,
        }])

    def test_channel_id_with_excessive_digits_is_rejected_as_ccload_error(self) -> None:
        class OversizedIdSession(_FakeCCLoadSession):
            instances: list["OversizedIdSession"] = []

            def get(self, url: str, **kwargs):
                self.calls.append(("GET", url, kwargs))
                if not url.endswith("/admin/channels"):
                    raise AssertionError(f"unexpected GET {url}")
                return _FakeResponse({
                    "success": True,
                    "data": [{
                        "id": "9" * 5000,
                        "name": "Oversized",
                        "auth_type": "codex_oauth",
                        "enabled": True,
                    }],
                    "count": 1,
                })

        with mock.patch.object(ccload_module, "Session", OversizedIdSession):
            with self.assertRaises(ccload_module.CCLoadError):
                ccload_module.list_remote_channels(self.server)

    def test_channel_id_contract_accepts_only_ascii_nonzero_decimal_text(self) -> None:
        cases = [
            ("٠", ""),
            ("١٢", ""),
            ("１２", ""),
            ("000", ""),
            ("0007", ""),
            ("7", "7"),
            ("9" * 64, "9" * 64),
            ("9" * 65, ""),
            (10**63, "1" + "0" * 63),
            (10**64, ""),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(ccload_module._clean_channel_id(raw), expected)

    def test_channel_id_entrypoints_reject_mixed_valid_and_invalid_ids(self) -> None:
        invalid = "١٢"
        with mock.patch.object(ccload_module, "_admin_session", side_effect=AssertionError("network must not start")):
            with self.assertRaises(ccload_module.CCLoadError):
                ccload_module.list_remote_channel_models(self.server, ["7", invalid])
            with self.assertRaises(ccload_module.CCLoadError):
                ccload_module.fetch_remote_credentials(self.server, ["7", invalid])

        config = mock.Mock()
        with (
            mock.patch.object(ccload_module, "reserve_background_task"),
            self.assertRaises(ccload_module.PublicSafeValueError),
        ):
            ccload_module.CCLoadImportService(config).start_import(
                self.server,
                ["7", invalid],
            )

    def test_loads_models_only_for_the_requested_channel_batch(self) -> None:
        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "OpenAIBackendAPI", _FakeChannelModelBackend),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["7"])

        self.assertEqual(channels, [{
            "id": "7",
            "plan_type": "pro",
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

    def test_channel_model_plan_type_rejects_container_instead_of_stringifying_it(self) -> None:
        canary = "ccload-plan-type-container-canary"

        class MalformedCredentialSession(_FakeCCLoadSession):
            instances: list["MalformedCredentialSession"] = []

            def get(self, url: str, **kwargs):
                response = super().get(url, **kwargs)
                if url.endswith("/admin/channels/7/editor"):
                    response._payload["data"]["oauth_credential"]["plan_type"] = {"secret": canary}
                return response

        with (
            mock.patch.object(ccload_module, "Session", MalformedCredentialSession),
            mock.patch.object(ccload_module, "OpenAIBackendAPI", _FakeChannelModelBackend),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["7"])

        self.assertEqual(channels[0]["plan_type"], "")
        self.assertNotIn(canary, repr(channels))

    def test_channel_model_ids_reject_container_ids_instead_of_stringifying_them(self) -> None:
        canary = "ccload-model-container-canary"

        class MalformedModelBackend:
            def __init__(self, access_token: str = ""):
                self.access_token = access_token

            def list_models(self, **_kwargs):
                return {
                    "object": "list",
                    "data": [
                        {"id": {"secret": canary}},
                        {"id": [canary]},
                        {"id": "valid-channel-model"},
                    ],
                }

            def close(self):
                pass

        with mock.patch.object(ccload_module, "OpenAIBackendAPI", MalformedModelBackend):
            models = ccload_module._channel_model_ids("channel-token")

        self.assertEqual(models, ["valid-channel-model"])
        self.assertNotIn(canary, repr(models))

    def test_channel_model_ids_does_not_bind_source_specific_catalog_identity(self) -> None:
        class CapturingBackend:
            instances: list["CapturingBackend"] = []

            def __init__(self, access_token: str = ""):
                self.access_token = access_token
                self.closed = False
                self.__class__.instances.append(self)

            def list_models(self, **_kwargs):
                return {"object": "list", "data": [{"id": "codex-model"}]}

            def close(self):
                self.closed = True

        with mock.patch.object(ccload_module, "OpenAIBackendAPI", CapturingBackend):
            models = ccload_module._channel_model_ids(
                "channel-token",
            )

        self.assertEqual(models, ["codex-model"])
        self.assertEqual(len(CapturingBackend.instances), 1)
        backend = CapturingBackend.instances[0]
        self.assertEqual(backend.access_token, "channel-token")
        self.assertTrue(backend.closed)

    def test_expired_browse_deadline_does_not_start_unbounded_logout_cleanup(self) -> None:
        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module.time, "monotonic", side_effect=[99.5, 100.5]),
        ):
            with ccload_module._admin_session(self.server, deadline=100.0):
                pass

        session = _FakeCCLoadSession.instances[0]
        self.assertEqual([call[0:2] for call in session.calls], [
            ("POST", "https://ccload.example.test/login"),
        ])

    def test_model_batch_rejects_more_than_fifty_channels_before_remote_io(self) -> None:
        with mock.patch.object(ccload_module, "Session", _FakeCCLoadSession):
            with self.assertRaisesRegex(ccload_module.CCLoadError, "at most 50"):
                ccload_module.list_remote_channel_models(
                    self.server,
                    [str(index) for index in range(1, 52)],
                )

        self.assertEqual(_FakeCCLoadSession.instances, [])

    def test_each_distinct_account_type_uses_its_own_authenticated_model_catalog(self) -> None:
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
            {"id": "1", "plan_type": "free", "models": ["common", "free-model"], "models_loaded": True},
            {"id": "2", "plan_type": "pro", "models": ["common", "free-model", "pro-model"], "models_loaded": True},
        ])
        self.assertCountEqual(
            [backend.access_token for backend in PerChannelBackend.instances],
            ["token-1", "token-2"],
        )
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

    def test_model_batch_fetches_one_catalog_per_channel(self) -> None:
        credentials = {
            "1": {"access_token": "free-token-1", "plan_type": "free"},
            "2": {"access_token": "free-token-2", "plan_type": " Free "},
            "3": {"access_token": "pro-token-1", "plan_type": "pro"},
            "4": {"access_token": "team-token-1", "plan_type": "team"},
        }
        model_calls: list[str] = []

        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            return credentials[channel_id]

        def model_ids(access_token: str, **_kwargs):
            model_calls.append(access_token)
            return [f"model-{access_token}"]

        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(ccload_module, "_channel_model_ids", side_effect=model_ids),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["1", "2", "3", "4"])

        self.assertCountEqual(
            model_calls,
            ["free-token-1", "free-token-2", "pro-token-1", "team-token-1"],
        )
        self.assertEqual(channels, [
            {
                "id": "1",
                "plan_type": "free",
                "models": ["model-free-token-1"],
                "models_loaded": True,
            },
            {
                "id": "2",
                "plan_type": "Free",
                "models": ["model-free-token-2"],
                "models_loaded": True,
            },
            {
                "id": "3",
                "plan_type": "pro",
                "models": ["model-pro-token-1"],
                "models_loaded": True,
            },
            {
                "id": "4",
                "plan_type": "team",
                "models": ["model-team-token-1"],
                "models_loaded": True,
            },
        ])

    def test_model_batch_does_not_reuse_catalog_for_account_type_aliases(self) -> None:
        credentials = {
            "1": {"access_token": "business-token", "plan_type": "business"},
            "2": {"access_token": "team-token", "plan_type": "team"},
            "3": {"access_token": "pro-lite-token", "plan_type": "pro_lite"},
            "4": {"access_token": "prolite-token", "plan_type": "ProLite"},
        }
        model_calls: list[str] = []

        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            return credentials[channel_id]

        def model_ids(access_token: str, **_kwargs):
            model_calls.append(access_token)
            return [f"model-{access_token}"]

        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(ccload_module, "_channel_model_ids", side_effect=model_ids),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["1", "2", "3", "4"])

        self.assertCountEqual(
            model_calls,
            ["business-token", "team-token", "pro-lite-token", "prolite-token"],
        )
        self.assertEqual([channel["id"] for channel in channels], ["1", "2", "3", "4"])

    def test_same_plan_channel_failure_is_not_filled_by_another_channel(self) -> None:
        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            return {"access_token": f"pro-token-{channel_id}", "plan_type": "pro"}

        def model_ids(access_token: str, **_kwargs):
            if access_token == "pro-token-1":
                raise ccload_module.CCLoadError("fixture model catalog failure")
            return ["pro-model-b"]

        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(ccload_module, "_channel_model_ids", side_effect=model_ids),
        ):
            catalogs = ccload_module.list_remote_channel_models(self.server, ["1", "2"])

        self.assertEqual(catalogs, [
            {"id": "1", "plan_type": "pro", "models": [], "models_loaded": False},
            {"id": "2", "plan_type": "pro", "models": ["pro-model-b"], "models_loaded": True},
        ])

    def test_model_batch_keeps_empty_plan_types_per_channel(self) -> None:
        credentials = {
            "1": {"access_token": "empty-token-1", "plan_type": ""},
            "2": {"access_token": "empty-token-2", "plan_type": ""},
        }
        model_calls: list[str] = []

        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            return credentials[channel_id]

        def model_ids(access_token: str, **_kwargs):
            model_calls.append(access_token)
            return [f"model-{access_token}"]

        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(ccload_module, "_channel_model_ids", side_effect=model_ids),
        ):
            channels = ccload_module.list_remote_channel_models(self.server, ["1", "2"])

        self.assertCountEqual(model_calls, ["empty-token-1", "empty-token-2"])
        self.assertEqual([channel["id"] for channel in channels], ["1", "2"])

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

    def test_repeated_model_catalog_timeouts_do_not_accumulate_running_pools(self) -> None:
        active = 0
        maximum_active = 0
        active_lock = threading.Lock()
        release = threading.Event()

        def blocked_model_ids(_access_token: str, **_kwargs):
            nonlocal active, maximum_active
            with active_lock:
                active += 1
                maximum_active = max(maximum_active, active)
            try:
                release.wait(timeout=1)
                return ["late-model"]
            finally:
                with active_lock:
                    active -= 1

        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            return {"access_token": f"token-{channel_id}"}

        try:
            with (
                mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
                mock.patch.object(ccload_module, "CCLOAD_CHANNEL_BROWSE_TIMEOUT_SECS", 0.05),
                mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
                mock.patch.object(ccload_module, "_channel_model_ids", side_effect=blocked_model_ids),
            ):
                channel_ids = [str(index) for index in range(1, ccload_module.CCLOAD_MODEL_CATALOG_WORKERS + 1)]
                for _ in range(2):
                    with self.assertRaisesRegex(ccload_module.CCLoadError, "timed out"):
                        ccload_module.list_remote_channel_models(self.server, channel_ids)

            self.assertLessEqual(maximum_active, ccload_module.CCLOAD_MODEL_CATALOG_WORKERS)
        finally:
            release.set()
            deadline = time.monotonic() + 1
            while time.monotonic() < deadline:
                with active_lock:
                    if active == 0:
                        break
                time.sleep(0.01)
            self.assertEqual(active, 0)

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
            return {"access_token": f"token-{channel_id}", "plan_type": "pro"}

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
            {"id": "2", "plan_type": "", "models": [], "models_loaded": False},
            {"id": "3", "plan_type": "pro", "models": ["gpt-pro-model"], "models_loaded": True},
        ])

    def test_same_plan_does_not_fallback_to_another_channel_when_first_fails(self) -> None:
        model_calls: list[str] = []

        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            return {"access_token": f"token-{channel_id}", "plan_type": "pro"}

        def model_ids(access_token: str, **_kwargs):
            model_calls.append(access_token)
            if access_token == "token-1":
                raise ccload_module.CCLoadError("fixture model catalog failure")
            return ["gpt-pro-model"]

        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(ccload_module, "_channel_model_ids", side_effect=model_ids),
        ):
            catalogs = ccload_module.list_remote_channel_models(self.server, ["1", "2"])

        # Model catalog requests run concurrently; completion order is not a
        # contract.  The channel-indexed result assertions below verify that
        # each token remains bound to its own channel.
        self.assertCountEqual(model_calls, ["token-1", "token-2"])
        self.assertEqual(catalogs, [
            {"id": "1", "plan_type": "pro", "models": [], "models_loaded": False},
            {"id": "2", "plan_type": "pro", "models": ["gpt-pro-model"], "models_loaded": True},
        ])

    def test_model_catalog_failure_does_not_mark_catalog_as_loaded(self) -> None:
        def fetch_credential(_session, _base_url, _headers, _channel_id: str, **_kwargs):
            return {"access_token": "token-1", "plan_type": "pro"}

        with (
            mock.patch.object(ccload_module, "Session", _FakeCCLoadSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(
                ccload_module,
                "_channel_model_ids",
                side_effect=ccload_module.CCLoadError("model catalog unavailable"),
            ),
        ):
            catalogs = ccload_module.list_remote_channel_models(self.server, ["1"])

        self.assertEqual(catalogs, [{
            "id": "1",
            "plan_type": "pro",
            "models": [],
            "models_loaded": False,
        }])

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

    def test_credential_import_uses_one_admin_login_and_worker_sized_batches(self) -> None:
        observations: list[int] = []
        progress: list[tuple[str, str | None]] = []
        admin_sessions = 0

        class FakeFuture:
            def __init__(self, function, args, kwargs) -> None:
                self.function = function
                self.args = args
                self.kwargs = kwargs

            def result(self):
                return self.function(*self.args, **self.kwargs)

        class RecordingExecutor:
            def __init__(self) -> None:
                self.submitted: list[FakeFuture] = []

            def submit(self, function, *args, **kwargs):
                future = FakeFuture(function, args, kwargs)
                self.submitted.append(future)
                return future

        class WorkerSession:
            def __init__(self, **_kwargs) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        @contextmanager
        def admin_session(_server, **_kwargs):
            nonlocal admin_sessions
            admin_sessions += 1
            yield object(), "https://ccload.example.test", {"Authorization": "Bearer admin-session"}

        def fetch_credential(_session, _base_url, _headers, channel_id: str, **_kwargs):
            return {"access_token": f"token-{channel_id}", "plan_type": "free"}

        def recording_as_completed(futures, **_kwargs):
            observations.append(len(futures))
            return iter(futures)

        executor = RecordingExecutor()
        channel_ids = [str(index) for index in range(1, 34)]
        with (
            mock.patch.object(ccload_module, "_admin_session", side_effect=admin_session),
            mock.patch.object(ccload_module, "Session", WorkerSession),
            mock.patch.object(ccload_module, "_fetch_remote_credential", side_effect=fetch_credential),
            mock.patch.object(ccload_module, "_CCLOAD_FETCH_EXECUTOR", executor, create=True),
            mock.patch.object(ccload_module, "as_completed", side_effect=recording_as_completed),
        ):
            credentials, errors = ccload_module.fetch_remote_credentials(
                self.server,
                channel_ids,
                on_progress=lambda channel_id, error: progress.append((channel_id, error)),
            )

        self.assertEqual(admin_sessions, 1)
        self.assertEqual(observations, [16, 16, 1])
        self.assertEqual(len(executor.submitted), 33)
        self.assertEqual(len(credentials), 33)
        self.assertEqual(errors, [])
        self.assertEqual(progress, [(channel_id, None) for channel_id in channel_ids])

    def test_credential_import_passes_one_absolute_deadline_to_admin_and_workers(self) -> None:
        deadlines: list[float | None] = []

        @contextmanager
        def admin_session(_server, **kwargs):
            deadlines.append(kwargs.get("deadline"))
            yield object(), "https://ccload.example.test", {"Authorization": "Bearer admin-session"}

        def fetch_credential(_base_url, _headers, _channel_id: str, **kwargs):
            deadlines.append(kwargs.get("deadline"))
            return {"access_token": f"token-{_channel_id}", "plan_type": "free"}

        with (
            mock.patch.object(ccload_module.time, "monotonic", return_value=1000.0),
            mock.patch.object(ccload_module, "_admin_session", side_effect=admin_session),
            mock.patch.object(ccload_module, "_fetch_remote_credential_for_import", side_effect=fetch_credential),
        ):
            credentials, errors = ccload_module.fetch_remote_credentials(self.server, ["7"])

        self.assertEqual(credentials, [{"access_token": "token-7", "plan_type": "free"}])
        self.assertEqual(errors, [])
        self.assertEqual(deadlines, [1000.0 + ccload_module.CCLOAD_IMPORT_TIMEOUT_SECS] * 2)

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

    def refresh_accounts(self, tokens: list[str], *, on_progress=None, **_kwargs) -> dict:
        self.refreshed = list(tokens)
        if on_progress is not None:
            for refreshed in range(1, len(tokens) + 1):
                on_progress(refreshed)
        return {"refreshed": len(tokens)}


class CCLoadPersistenceAndImportContractTests(unittest.TestCase):
    def test_late_worker_cannot_overwrite_restarted_import_job(self) -> None:
        credential = {
            "access_token": "old-access-token",
            "refresh_token": "old-refresh-token",
            "id_token": "old-id-token",
            "account_id": "old-account",
            "email": "old@example.test",
            "type": "codex",
            "expired": "2027-08-09T12:00:00Z",
            "plan_type": "free",
        }

        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            old_config = ccload_module.CCLoadConfig(store_file)
            old_server = old_config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            old_config.begin_import_job(
                old_server["id"],
                {
                    "job_id": "old-import-job",
                    "status": "pending",
                    "created_at": "2026-08-16T00:00:00+00:00",
                    "updated_at": "2026-08-16T00:00:00+00:00",
                    "total": 1,
                    "completed": 0,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 0,
                    "errors": [],
                },
            )
            old_server = old_config.get_server(old_server["id"])
            self.assertIsNotNone(old_server)
            assert old_server is not None
            old_service = ccload_module.CCLoadImportService(old_config)
            fetch_started = threading.Event()
            release_fetch = threading.Event()
            worker_errors: list[BaseException] = []

            def fetch_credentials(_server, _channel_ids, *, on_progress, **_kwargs):
                fetch_started.set()
                self.assertTrue(release_fetch.wait(2))
                on_progress("1", None)
                return [credential], []

            class DeferredReservation:
                def __init__(self) -> None:
                    self.target = None
                    self.args = ()

                def submit(self, target, *args, **_kwargs) -> None:
                    self.target = target
                    self.args = args

                def cancel(self) -> None:
                    pass

            deferred = DeferredReservation()

            def run_old_worker() -> None:
                try:
                    old_service._run_import(old_server["id"], old_server, ["1"])
                except BaseException as exc:
                    worker_errors.append(exc)

            accounts = _RecordingAccountService()
            with (
                mock.patch.object(ccload_module, "fetch_remote_credentials", side_effect=fetch_credentials),
                mock.patch.object(ccload_module, "account_service", accounts),
                mock.patch.object(ccload_module, "reserve_background_task", return_value=deferred),
            ):
                worker = threading.Thread(target=run_old_worker)
                worker.start()
                self.assertTrue(fetch_started.wait(2))

                restarted_config = ccload_module.CCLoadConfig(store_file)
                restarted_server = restarted_config.get_server(old_server["id"])
                self.assertIsNotNone(restarted_server)
                assert restarted_server is not None
                restarted_service = ccload_module.CCLoadImportService(restarted_config)
                restarted_job = restarted_service.start_import(restarted_server, ["2"])

                release_fetch.set()
                worker.join(2)

            self.assertFalse(worker.is_alive())
            self.assertEqual(worker_errors, [])
            current = restarted_config.get_import_job(old_server["id"])
            self.assertIsNotNone(current)
            assert current is not None
            self.assertEqual(current["job_id"], restarted_job["job_id"])
            self.assertEqual(current["status"], "pending")
            self.assertEqual(accounts.imported, [])
            self.assertEqual(accounts.refreshed, [])

    def test_late_worker_job_check_is_atomic_with_state_commit(self) -> None:
        with TemporaryDirectory() as temp_dir:
            store_file = Path(temp_dir) / "ccload_config.json"
            old_config = ccload_module.CCLoadConfig(store_file)
            server = old_config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            old_config.begin_import_job(server["id"], {
                "job_id": "old-job",
                "status": "running",
                "created_at": "2026-08-16T00:00:00+00:00",
                "updated_at": "2026-08-16T00:00:00+00:00",
                "total": 1,
                "completed": 0,
                "added": 0,
                "skipped": 0,
                "refreshed": 0,
                "failed": 0,
                "errors": [],
            })
            check_passed = threading.Event()
            release_commit = threading.Event()
            original_set = old_config.set_import_job

            def pause_before_commit(server_id: str, job: dict, **kwargs):
                check_passed.set()
                self.assertTrue(release_commit.wait(2))
                return original_set(server_id, job, **kwargs)

            service = ccload_module.CCLoadImportService(old_config)
            old_server = old_config.get_server(server["id"])
            self.assertIsNotNone(old_server)
            assert old_server is not None
            worker_errors: list[BaseException] = []

            def run_old_worker() -> None:
                try:
                    service._run_import(old_server["id"], old_server, ["1"])
                except BaseException as exc:
                    worker_errors.append(exc)

            credential = {"access_token": "old-access-token"}
            with (
                mock.patch.object(
                    ccload_module,
                    "fetch_remote_credentials",
                    return_value=([credential], []),
                ),
                mock.patch.object(
                    ccload_module.account_service,
                    "add_account_items",
                    return_value={"added": 1, "skipped": 0},
                ),
                mock.patch.object(
                    ccload_module.account_service,
                    "refresh_accounts",
                    return_value={"refreshed": 1},
                ),
                mock.patch.object(old_config, "set_import_job", side_effect=pause_before_commit),
            ):
                worker = threading.Thread(target=run_old_worker)
                worker.start()
                self.assertTrue(check_passed.wait(2))

                new_config = ccload_module.CCLoadConfig(store_file)
                new_server = new_config.get_server(server["id"])
                self.assertIsNotNone(new_server)
                assert new_server is not None
                new_job = {
                    "job_id": "new-job",
                    "status": "pending",
                    "created_at": "2026-08-16T00:01:00+00:00",
                    "updated_at": "2026-08-16T00:01:00+00:00",
                    "total": 1,
                    "completed": 0,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 0,
                    "errors": [],
                }
                new_config.set_import_job(server["id"], new_job)
                release_commit.set()
                worker.join(2)

                current = new_config.get_import_job(server["id"])

            self.assertFalse(worker.is_alive())
            self.assertEqual(worker_errors, [])
            self.assertIsNotNone(current)
            assert current is not None
            self.assertEqual(current["job_id"], "new-job")
            self.assertEqual(current["status"], "pending")

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

    def test_import_persists_progress_after_each_ccload_credential_result(self) -> None:
        credential = {
            "id_token": "id-token-secret",
            "access_token": "access-token-secret",
            "refresh_token": "refresh-token-secret",
            "account_id": "account-1",
            "email": "user@example.test",
            "type": "codex",
            "expired": "2027-08-09T12:00:00Z",
            "plan_type": "free",
        }
        snapshots: list[tuple[int, int]] = []
        with TemporaryDirectory() as temp_dir:
            config = ccload_module.CCLoadConfig(Path(temp_dir) / "ccload_config.json")
            server = config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            service = ccload_module.CCLoadImportService(config)
            job = {
                "job_id": "progress-job",
                "status": "pending",
                "created_at": "2026-08-13T00:00:00+00:00",
                "updated_at": "2026-08-13T00:00:00+00:00",
                "total": 3,
                "completed": 0,
                "added": 0,
                "skipped": 0,
                "refreshed": 0,
                "failed": 0,
                "errors": [],
            }
            config.set_import_job(server["id"], job)

            def fetch_credentials(_server, _channel_ids, *, on_progress, **_kwargs):
                on_progress("1", None)
                current = config.get_import_job(server["id"])
                snapshots.append((current["completed"], current["failed"]))
                on_progress("2", "credential unavailable")
                current = config.get_import_job(server["id"])
                snapshots.append((current["completed"], current["failed"]))
                on_progress("3", None)
                current = config.get_import_job(server["id"])
                snapshots.append((current["completed"], current["failed"]))
                return [credential, {**credential, "access_token": "access-token-secret-2"}], [
                    {"name": "2", "error": "credential unavailable"},
                ]

            with (
                mock.patch.object(ccload_module, "fetch_remote_credentials", side_effect=fetch_credentials),
                mock.patch.object(ccload_module, "account_service", _RecordingAccountService()),
            ):
                service._run_import(server["id"], server, ["1", "2", "3"])

            self.assertEqual(snapshots, [(1, 0), (2, 1), (3, 1)])
            final_job = config.get_import_job(server["id"])
            self.assertEqual(final_job["status"], "completed")
            self.assertEqual(final_job["completed"], 3)
            self.assertEqual(final_job["failed"], 1)

    def test_import_worker_initial_running_write_failure_is_terminal(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = ccload_module.CCLoadConfig(Path(temp_dir) / "ccload_config.json")
            server = config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            config.set_import_job(
                server["id"],
                {
                    "job_id": "initial-write-failure-job",
                    "status": "pending",
                    "created_at": "2026-08-13T00:00:00+00:00",
                    "updated_at": "2026-08-13T00:00:00+00:00",
                    "total": 2,
                    "completed": 0,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 0,
                    "errors": [],
                },
            )
            service = ccload_module.CCLoadImportService(config)
            original_set_import_job = config.set_import_job
            calls = 0
            second_error: list[str] = []

            def fail_once(server_id: str, job: dict, **_kwargs) -> dict | None:
                nonlocal calls
                calls += 1
                if calls == 1:
                    raise OSError("transient state write failure")
                try:
                    return original_set_import_job(server_id, job)
                except Exception as exc:
                    second_error.append(f"{type(exc).__name__}: {exc}")
                    raise

            with mock.patch.object(config, "set_import_job", side_effect=fail_once):
                service._run_import(server["id"], server, ["1", "2"])

            final_job = config.get_import_job(server["id"])
            self.assertEqual(second_error, [])
            self.assertIsNotNone(final_job)
            assert final_job is not None
            self.assertEqual(final_job["status"], "failed")
            self.assertEqual(final_job["completed"], 2)
            self.assertEqual(final_job["failed"], 2)

    def test_import_publishes_add_and_refresh_stage_counts_while_running(self) -> None:
        credentials = [
            {
                "access_token": f"access-token-{index}",
                "refresh_token": f"refresh-token-{index}",
                "id_token": f"id-token-{index}",
                "account_id": f"account-{index}",
                "email": f"user-{index}@example.test",
                "type": "codex",
                "expired": "2027-08-09T12:00:00Z",
                "plan_type": "free",
            }
            for index in range(3)
        ]
        snapshots: list[tuple[int, int, int]] = []

        with TemporaryDirectory() as temp_dir:
            config = ccload_module.CCLoadConfig(Path(temp_dir) / "ccload_config.json")
            server = config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            config.set_import_job(
                server["id"],
                {
                    "job_id": "stage-count-job",
                    "status": "pending",
                    "created_at": "2026-08-13T00:00:00+00:00",
                    "updated_at": "2026-08-13T00:00:00+00:00",
                    "total": 3,
                    "completed": 0,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 0,
                    "errors": [],
                },
            )
            service = ccload_module.CCLoadImportService(config)

            def fetch_credentials(_server, channel_ids, *, on_progress, **_kwargs):
                for channel_id in channel_ids:
                    on_progress(channel_id, None)
                return credentials, []

            class StagedAccountService:
                def add_account_items(self, _items: list[dict]) -> dict:
                    return {"added": 2, "skipped": 1}

                def refresh_accounts(self, _tokens: list[str], *, on_progress=None, **_kwargs) -> dict:
                    job = config.get_import_job(server["id"])
                    snapshots.append((job["added"], job["skipped"], job["refreshed"]))
                    if on_progress is not None:
                        for refreshed in (1, 2, 3):
                            on_progress(refreshed)
                            job = config.get_import_job(server["id"])
                            snapshots.append((job["added"], job["skipped"], job["refreshed"]))
                    return {"refreshed": 3}

            with (
                mock.patch.object(ccload_module, "fetch_remote_credentials", side_effect=fetch_credentials),
                mock.patch.object(ccload_module, "account_service", StagedAccountService()),
            ):
                service._run_import(server["id"], server, ["1", "2", "3"])

            self.assertEqual(
                snapshots,
                [(2, 1, 0), (2, 1, 1), (2, 1, 2), (2, 1, 3)],
            )
            final_job = config.get_import_job(server["id"])
            self.assertEqual(final_job["status"], "completed")
            self.assertEqual(final_job["added"], 2)
            self.assertEqual(final_job["skipped"], 1)
            self.assertEqual(final_job["refreshed"], 3)

    def test_import_preserves_published_refresh_count_when_refresh_fails(self) -> None:
        credential = {
            "access_token": "access-token-secret",
            "refresh_token": "refresh-token-secret",
            "id_token": "id-token-secret",
            "account_id": "account-1",
            "email": "user@example.test",
            "type": "codex",
            "expired": "2027-08-09T12:00:00Z",
            "plan_type": "free",
        }

        with TemporaryDirectory() as temp_dir:
            config = ccload_module.CCLoadConfig(Path(temp_dir) / "ccload_config.json")
            server = config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            config.set_import_job(
                server["id"],
                {
                    "job_id": "partial-refresh-job",
                    "status": "pending",
                    "created_at": "2026-08-13T00:00:00+00:00",
                    "updated_at": "2026-08-13T00:00:00+00:00",
                    "total": 1,
                    "completed": 0,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 0,
                    "errors": [],
                },
            )
            service = ccload_module.CCLoadImportService(config)

            def fetch_credentials(_server, _channel_ids, *, on_progress, **_kwargs):
                on_progress("1", None)
                return [credential], []

            class FailingRefreshAccountService:
                def add_account_items(self, _items: list[dict]) -> dict:
                    return {"added": 1, "skipped": 0}

                def refresh_accounts(self, _tokens: list[str], *, on_progress=None, **_kwargs) -> dict:
                    on_progress(1)
                    raise RuntimeError("opaque upstream failure")

            with (
                mock.patch.object(ccload_module, "fetch_remote_credentials", side_effect=fetch_credentials),
                mock.patch.object(ccload_module, "account_service", FailingRefreshAccountService()),
            ):
                service._run_import(server["id"], server, ["1"])

            final_job = config.get_import_job(server["id"])
            self.assertEqual(final_job["status"], "failed")
            self.assertEqual(final_job["added"], 1)
            self.assertEqual(final_job["refreshed"], 1)

    def test_import_rejects_incomplete_add_counts_before_refresh(self) -> None:
        credentials = [
            {
                "access_token": f"access-token-{index}",
                "refresh_token": f"refresh-token-{index}",
                "id_token": f"id-token-{index}",
                "account_id": f"account-{index}",
                "email": f"user-{index}@example.test",
                "type": "codex",
                "expired": "2027-08-09T12:00:00Z",
                "plan_type": "free",
            }
            for index in range(2)
        ]

        with TemporaryDirectory() as temp_dir:
            config = ccload_module.CCLoadConfig(Path(temp_dir) / "ccload_config.json")
            server = config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="admin-password-secret",
            )
            config.set_import_job(
                server["id"],
                {
                    "job_id": "incomplete-add-count-job",
                    "status": "pending",
                    "created_at": "2026-08-13T00:00:00+00:00",
                    "updated_at": "2026-08-13T00:00:00+00:00",
                    "total": 2,
                    "completed": 0,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 0,
                    "errors": [],
                },
            )
            service = ccload_module.CCLoadImportService(config)

            class IncompleteAddAccountService:
                def __init__(self) -> None:
                    self.refresh_called = False

                def add_account_items(self, _items: list[dict]) -> dict:
                    return {"added": 1, "skipped": 0}

                def refresh_accounts(self, _tokens: list[str], **_kwargs) -> dict:
                    self.refresh_called = True
                    return {"refreshed": 2}

            account_service = IncompleteAddAccountService()

            def fetch_credentials(_server, channel_ids, *, on_progress, **_kwargs):
                for channel_id in channel_ids:
                    on_progress(channel_id, None)
                return credentials, []

            with (
                mock.patch.object(ccload_module, "fetch_remote_credentials", side_effect=fetch_credentials),
                mock.patch.object(ccload_module, "account_service", account_service),
            ):
                service._run_import(server["id"], server, ["1", "2"])

            final_job = config.get_import_job(server["id"])
            self.assertIsNotNone(final_job)
            assert final_job is not None
            self.assertEqual(final_job["status"], "failed")
            self.assertEqual(final_job["completed"], 2)
            self.assertEqual(final_job["added"], 0)
            self.assertEqual(final_job["skipped"], 0)
            self.assertEqual(final_job["refreshed"], 0)
            self.assertEqual(final_job["failed"], 2)
            self.assertFalse(account_service.refresh_called)

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
