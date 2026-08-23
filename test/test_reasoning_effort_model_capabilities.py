from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.account_service import AccountService
import services.openai_backend_api as openai_backend_module
from services.model_service import ModelCatalogService
from services.openai_backend_api import OpenAIBackendAPI, SearchTimeoutError
from services.protocol import openai_v1_chat_complete, openai_v1_response
from services.protocol.chat_completion_cache import chat_completion_cache
from services.protocol.reasoning_effort import normalize_conversation_effort
from services.storage.json_storage import JSONStorageBackend


def _stream_response(payload: dict) -> mock.Mock:
    response = mock.Mock(status_code=200)
    response.json.return_value = payload
    response.iter_content.side_effect = lambda **_kwargs: iter([
        json.dumps(payload).encode("utf-8")
    ])
    return response


class _HeaderObservingSession:
    def __init__(self, response: mock.Mock) -> None:
        self.headers = {
            "User-Agent": "Mozilla/5.0 web-fingerprint",
            "Origin": "https://chatgpt.com",
            "Referer": "https://chatgpt.com/",
            "OAI-Client-Version": "prod-web-build",
            "OAI-Client-Build-Number": "123",
            "X-OpenAI-Target-Path": "/web-default",
            "X-OpenAI-Target-Route": "/web-default",
            "Sec-Ch-Ua": '"Chromium"',
            "OAI-Device-Id": "web-device",
            "OAI-Session-Id": "web-session",
        }
        self._response = response
        self.effective_headers: dict[str, str] | None = None
        self.urls: list[str] = []
        self.get_calls = 0
        self.close_calls = 0

    def get(self, _url: str, *, headers: dict[str, str], **_kwargs: object) -> mock.Mock:
        self.get_calls += 1
        self.urls.append(_url)
        self.effective_headers = dict(self.headers)
        self.effective_headers.update(headers)
        return self._response

    def close(self) -> None:
        self.close_calls += 1


class UpstreamModelEffortContractTests(unittest.TestCase):
    def test_web_transport_rejects_non_web_or_missing_live_identity_before_io(self) -> None:
        for source_type, live_account in (
            ("codex", {"access_token": "token", "source_type": "codex"}),
            ("future-incompatible", {"access_token": "token", "source_type": "future-incompatible"}),
            ("missing", None),
        ):
            with self.subTest(source_type=source_type):
                backend = object.__new__(OpenAIBackendAPI)
                backend.access_token = "token"
                backend.base_url = "https://chatgpt.com"
                backend.session = mock.Mock()
                backend.session.get.return_value = mock.Mock(status_code=200)
                backend._read_json_response = mock.Mock(return_value={})
                backend._search_remaining = mock.Mock(return_value=1.0)
                with mock.patch.object(
                    openai_backend_module.account_service,
                    "get_account",
                    return_value=live_account,
                ), self.assertRaisesRegex(RuntimeError, "web backend"):
                    backend._get_me()
                backend.session.get.assert_not_called()

    def test_web_transport_accepts_legacy_sources_and_anonymous_before_io(self) -> None:
        for access_token, live_account in (
            ("web-token", {"access_token": "web-token", "source_type": "web"}),
            ("password-token", {"access_token": "password-token", "source_type": "password"}),
            ("oauth-token", {"access_token": "oauth-token", "source_type": "password-oauth"}),
            ("", None),
        ):
            with self.subTest(access_token=access_token):
                backend = object.__new__(OpenAIBackendAPI)
                backend.access_token = access_token
                backend.base_url = "https://chatgpt.com"
                backend.session = mock.Mock()
                backend.session.headers = {}
                backend.session.get.return_value = mock.Mock(status_code=200)
                backend._read_json_response = mock.Mock(return_value={})
                backend._search_remaining = mock.Mock(return_value=1.0)
                with mock.patch.object(
                    openai_backend_module.account_service,
                    "get_account",
                    return_value=live_account,
                ):
                    backend._get_me()
                backend.session.get.assert_called_once()

    def test_web_bootstrap_rejects_non_web_identity_before_io(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.base_url = "https://chatgpt.com"
        backend.user_agent = "test-agent"
        backend.session = mock.Mock()
        backend.session.headers = {
            "Sec-Ch-Ua": '"Chromium"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": '"Windows"',
        }
        with mock.patch.object(
            openai_backend_module.account_service,
            "get_account",
            return_value={"access_token": "codex-token", "source_type": "codex"},
        ), self.assertRaisesRegex(RuntimeError, "web backend"):
            backend._bootstrap()
        backend.session.get.assert_not_called()

    def test_codex_response_version_missing_environment_fails_closed(self) -> None:
        environment = dict(os.environ)
        environment.pop("CODEX_CLIENT_VERSION", None)
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                (
                    "from services.openai_backend_api import OpenAIBackendAPI; "
                    "OpenAIBackendAPI._codex_client_version("
                    "object.__new__(OpenAIBackendAPI))"
                ),
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("client version", result.stderr.lower())

    def test_codex_responses_headers_use_official_client_defaults(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.codex_client_version = "0.147.0"

        headers = backend._codex_responses_headers()

        self.assertEqual(headers["Authorization"], "Bearer codex-token")
        self.assertEqual(headers["ChatGPT-Account-ID"], "acct-codex")
        self.assertEqual(headers["version"], "0.147.0")
        self.assertEqual(headers["originator"], "codex_cli_rs")
        self.assertTrue(headers["User-Agent"].startswith("codex_cli_rs/0.147.0"))

    def test_authenticated_model_list_uses_web_endpoint_without_codex_filter(self) -> None:
        response = _stream_response({
            "models": [
                {
                    "slug": "codex-listed",
                    "visibility": "list",
                    "supported_in_api": True,
                    "supported_reasoning_levels": [{"effort": "high"}],
                },
                {
                    "slug": "codex-hidden",
                    "visibility": "hide",
                    "supported_in_api": True,
                    "supported_reasoning_levels": [{"effort": "medium"}],
                },
                {
                    "slug": "codex-unlisted",
                    "visibility": "none",
                    "supported_in_api": True,
                    "supported_reasoning_levels": [{"effort": "low"}],
                },
                {
                    "slug": "codex-no-api",
                    "visibility": "list",
                    "supported_in_api": False,
                },
            ]
        })
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.base_url = "https://chatgpt.com"
        web_session = _HeaderObservingSession(response)
        web_session.headers["Authorization"] = "Bearer codex-token"
        backend.session = web_session
        backend._bootstrap = mock.Mock()

        result = backend.list_models()

        self.assertEqual(
            [item["id"] for item in result["data"]],
            ["codex-hidden", "codex-listed", "codex-no-api", "codex-unlisted"],
        )
        self.assertEqual(
            result["data"][1]["supported_reasoning_efforts"],
            ["high"],
        )
        self.assertEqual(web_session.get_calls, 1)
        self.assertEqual(
            web_session.urls,
            ["https://chatgpt.com/backend-api/models?history_and_training_disabled=false"],
        )
        headers = web_session.effective_headers
        self.assertIsNotNone(headers)
        assert headers is not None
        self.assertEqual(headers["Authorization"], "Bearer codex-token")
        self.assertNotIn("ChatGPT-Account-ID", headers)
        self.assertEqual(headers["X-OpenAI-Target-Path"], "/backend-api/models")
        self.assertEqual(headers["X-OpenAI-Target-Route"], "/backend-api/models")
        backend._bootstrap.assert_called_once()

    def test_model_list_uses_only_authenticated_web_endpoint_for_all_sources(self) -> None:
        for source_type in ("web", "password", "password-oauth", "codex", "future"):
            with self.subTest(source_type=source_type):
                backend = object.__new__(OpenAIBackendAPI)
                backend.access_token = "representative-token"
                backend.account = {"source_type": source_type, "account_id": "acct"}
                backend.base_url = "https://chatgpt.com"
                backend.session = mock.Mock()
                backend._bootstrap = mock.Mock()
                backend._headers = mock.Mock(
                    return_value={
                        "Authorization": "Bearer representative-token",
                        "X-OpenAI-Target-Path": "/backend-api/models",
                        "X-OpenAI-Target-Route": "/backend-api/models",
                    }
                )
                backend._codex_session = mock.Mock()
                backend._fetch_model_catalog_endpoint = mock.Mock(
                    return_value={
                        "web-model": {
                            "id": "web-model",
                            "supported_reasoning_efforts": ["high"],
                        }
                    }
                )

                result = backend.list_models()

                self.assertEqual([item["id"] for item in result["data"]], ["web-model"])
                backend._bootstrap.assert_called_once()
                backend._fetch_model_catalog_endpoint.assert_called_once()
                call = backend._fetch_model_catalog_endpoint.call_args
                self.assertIs(call.args[0], backend.session)
                self.assertEqual(
                    call.args[1],
                    "/backend-api/models?history_and_training_disabled=false",
                )
                backend._codex_session.assert_not_called()

    def test_model_list_keeps_web_reasoning_metadata_without_codex_merge(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "representative-token"
        backend.account = {"source_type": "codex"}
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})
        backend._fetch_model_catalog_endpoint = mock.Mock(
            return_value={
                "shared": {
                    "id": "shared",
                    "supported_reasoning_efforts": ["high", "medium"],
                },
                "web-only": {"id": "web-only"},
            }
        )
        backend._codex_session = mock.Mock()

        result = backend.list_models()

        self.assertEqual(
            [item["id"] for item in result["data"]],
            ["shared", "web-only"],
        )
        self.assertEqual(
            result["data"][0]["supported_reasoning_efforts"],
            ["high", "medium"],
        )
        backend._fetch_model_catalog_endpoint.assert_called_once()
        backend._codex_session.assert_not_called()

    def test_model_list_web_failure_does_not_probe_codex(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "representative-token"
        backend.account = {"source_type": "codex"}
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})
        backend._fetch_model_catalog_endpoint = mock.Mock(
            side_effect=SearchTimeoutError("web catalog timed out")
        )
        backend._codex_session = mock.Mock()

        with self.assertRaises(SearchTimeoutError):
            backend.list_models(timeout_secs=0.05)

        backend._fetch_model_catalog_endpoint.assert_called_once()
        backend._codex_session.assert_not_called()

    def test_model_list_does_not_require_codex_client_version(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.session = mock.Mock()
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})
        backend._fetch_model_catalog_endpoint = mock.Mock(
            return_value={"model": {"id": "model"}}
        )

        result = backend.list_models()

        self.assertEqual([item["id"] for item in result["data"]], ["model"])
        backend._bootstrap.assert_called_once()

    def test_model_response_cleanup_does_not_mask_primary_error(self) -> None:
        class Response:
            status_code = 200
            headers = {"content-type": "application/json"}

            def __init__(self) -> None:
                self.close_calls = 0

            def iter_content(self, **_kwargs: object):
                raise RuntimeError("response-secret")

            def close(self) -> None:
                self.close_calls += 1
                raise RuntimeError("response-close-secret")

        class Session:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {}
                self.close_calls = 0

            def get(self, _url: str, **_kwargs: object) -> Response:
                return Response()

            def close(self) -> None:
                self.close_calls += 1
                raise RuntimeError("session-secret")

        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.base_url = "https://chatgpt.com"
        session = Session()
        response = Response()

        def get(_url: str, **_kwargs: object) -> Response:
            return response

        session.get = get  # type: ignore[method-assign]
        backend.session = session
        backend._bootstrap = mock.Mock()

        with (
            mock.patch("services.openai_backend_api.logger.warning") as warning,
        ):
            with self.assertRaisesRegex(RuntimeError, "invalid response body"):
                backend.list_models()

        self.assertEqual(response.close_calls, 1)
        self.assertEqual(session.close_calls, 0)
        logged = str(warning.call_args_list)
        self.assertNotIn("session-secret", logged)

    def test_backend_model_list_preserves_only_normalized_effort_capabilities(self) -> None:
        response = _stream_response({
            "models": [
                {
                    "slug": "gpt-capable",
                    "thinking_efforts": [
                        {"thinking_effort": "min", "description": "private detail"},
                        {"thinking_effort": "standard"},
                        {"thinking_effort": "extended"},
                        {"thinking_effort": "max"},
                    ],
                    "opaque_upstream_field": "must-not-be-forwarded",
                }
            ]
        })
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "account-token"
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend.session.get.return_value = response
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})

        result = backend.list_models()

        self.assertEqual(
            result["data"][0]["supported_reasoning_efforts"],
            ["min", "standard", "extended", "max"],
        )
        headers = backend.session.get.call_args.kwargs["headers"]
        self.assertNotIn("originator", headers)
        self.assertNotIn("version", headers)
        self.assertNotIn("opaque_upstream_field", result["data"][0])

    def test_backend_model_list_downgrades_malformed_created_timestamp(self) -> None:
        response = _stream_response({
            "models": [
                {"slug": "gpt-valid", "created": 1700000000},
                {"slug": "gpt-malformed", "created": "not-a-timestamp"},
            ]
        })
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "account-token"
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend.session.get.return_value = response
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})

        result = backend.list_models()

        created = {item["id"]: item["created"] for item in result["data"]}
        self.assertEqual(created["gpt-valid"], 1700000000)
        self.assertEqual(created["gpt-malformed"], 0)

    def test_backend_model_list_closes_upstream_response(self) -> None:
        response = _stream_response({"models": [{"slug": "gpt-model"}]})
        response.close = mock.Mock()
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "account-token"
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend.session.get.return_value = response
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})

        backend.list_models()

        response.close.assert_called_once_with()

    def test_model_catalog_requests_full_account_catalog_without_changing_conversation_history_mode(self) -> None:
        limited_response = _stream_response({"models": [{"slug": "gpt-basic"}]})
        full_response = _stream_response({
            "models": [
                {"slug": "gpt-basic"},
                {"slug": "gpt-pro-extra"},
            ],
        })
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "account-token"
        backend.base_url = "https://chatgpt.com"
        backend.session = mock.Mock()
        backend.session.get.side_effect = lambda url, **_kwargs: (
            full_response if url.endswith("history_and_training_disabled=false") else limited_response
        )
        backend._bootstrap = mock.Mock()
        backend._headers = mock.Mock(return_value={})

        with mock.patch(
            "services.model_service.model_catalog_service.normalize_reasoning_effort",
            return_value="",
        ):
            payload = backend._conversation_payload(
                [{"role": "user", "content": "hello"}],
                "gpt-capable",
                "Asia/Shanghai",
            )
        result = backend.list_models()

        self.assertIs(payload["history_and_training_disabled"], True)
        self.assertEqual(
            [item["id"] for item in result["data"]],
            ["gpt-basic", "gpt-pro-extra"],
        )
        self.assertEqual(
            backend.session.get.call_args.args[0],
            "https://chatgpt.com/backend-api/models?history_and_training_disabled=false",
        )
        self.assertNotIn(
            "ChatGPT-Account-ID",
            backend.session.get.call_args.kwargs["headers"],
        )

    def test_invalid_or_unsupported_effort_uses_selected_models_strongest_level(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            accounts = AccountService(
                JSONStorageBackend(Path(temp_dir) / "accounts.json")
            )
            accounts.add_account_items(
                [{"access_token": "pro-token", "type": "Pro", "status": "正常"}]
            )
            accounts.refresh_access_token = lambda token, **_kwargs: token

            class Backend:
                def __init__(self, access_token: str = "") -> None:
                    self.access_token = access_token

                def list_models(self, **_kwargs) -> dict:
                    efforts = ["min", "standard"] if not self.access_token else [
                        "min",
                        "standard",
                        "extended",
                        "max",
                        "ultra",
                    ]
                    return {
                        "object": "list",
                        "data": [{
                            "id": "gpt-capable",
                            "supported_reasoning_efforts": efforts,
                        }],
                    }

                def close(self) -> None:
                    pass

            catalog = ModelCatalogService(
                accounts,
                backend_factory=Backend,
                cache_ttl_seconds=300,
            )

            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "not-a-real-effort",
                    access_token="pro-token",
                ),
                "ultra",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "high",
                    access_token="pro-token",
                ),
                "ultra",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "extended",
                    access_token="pro-token",
                ),
                "extended",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "xhigh",
                    access_token="pro-token",
                ),
                "extended",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "minimal",
                    access_token="pro-token",
                ),
                "min",
            )
            self.assertEqual(
                catalog.normalize_reasoning_effort(
                    "gpt-capable",
                    "not-a-real-effort",
                    access_token="",
                ),
                "standard",
            )

            with mock.patch.object(
                catalog,
                "supported_reasoning_efforts",
                side_effect=AssertionError("auto must not refresh the upstream catalog"),
            ):
                self.assertEqual(
                    catalog.normalize_reasoning_effort(
                        "gpt-capable",
                        "auto",
                        access_token="pro-token",
                    ),
                    "",
                )

    def test_unknown_public_effort_survives_until_model_capability_resolution(self) -> None:
        self.assertEqual(
            normalize_conversation_effort("model-defined-effort"),
            "model-defined-effort",
        )

    def test_conversation_payload_resolves_effort_for_selected_model_and_account(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "selected-account-token"
        with mock.patch(
            "services.model_service.model_catalog_service.normalize_reasoning_effort",
            return_value="max",
        ) as normalize:
            payload = backend._conversation_payload(
                [{"role": "user", "content": "hello"}],
                "gpt-capable",
                "Asia/Shanghai",
                thinking_effort="wrong-for-this-model",
            )

        self.assertEqual(payload["thinking_effort"], "max")
        normalize.assert_called_once_with(
            "gpt-capable",
            "wrong-for-this-model",
            access_token="selected-account-token",
        )

    def test_chat_and_responses_defer_unknown_effort_to_selected_backend(self) -> None:
        captured: list[str] = []
        chat_completion_cache.clear()

        def collect_text(_backend, request) -> str:
            captured.append(request.thinking_effort)
            return "ok"

        def stream_text(_backend, request):
            captured.append(request.thinking_effort)
            yield "ok"

        with (
            mock.patch.object(openai_v1_chat_complete, "text_backend", return_value=object()),
            mock.patch.object(openai_v1_chat_complete, "collect_text", side_effect=collect_text),
        ):
            openai_v1_chat_complete.handle({
                "model": "gpt-capable",
                "messages": [{"role": "user", "content": "chat effort fallback"}],
                "reasoning_effort": "wrong-for-this-model",
            })

        with (
            mock.patch.object(openai_v1_response, "text_backend", return_value=object()),
            mock.patch.object(openai_v1_response, "stream_text_deltas", side_effect=stream_text),
        ):
            openai_v1_response.handle({
                "model": "gpt-capable",
                "input": "response effort fallback",
                "reasoning": {"effort": "wrong-for-this-model"},
            })

        self.assertEqual(captured, ["wrong-for-this-model", "wrong-for-this-model"])


if __name__ == "__main__":
    unittest.main()
