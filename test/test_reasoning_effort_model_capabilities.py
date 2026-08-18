from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from services.account_service import AccountService
from services.model_service import ModelCatalogService
from services.openai_backend_api import OpenAIBackendAPI
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
        self.get_calls = 0
        self.close_calls = 0

    def get(self, _url: str, *, headers: dict[str, str], **_kwargs: object) -> mock.Mock:
        self.get_calls += 1
        self.effective_headers = dict(self.headers)
        self.effective_headers.update(headers)
        return self._response

    def close(self) -> None:
        self.close_calls += 1


class UpstreamModelEffortContractTests(unittest.TestCase):
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

    def test_codex_model_list_uses_codex_endpoint_headers_and_api_capability_filter(self) -> None:
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
        backend.codex_client_version = "0.147.0"
        backend.fp = {"impersonate": "chrome110"}
        web_session = _HeaderObservingSession(response)
        codex_session = _HeaderObservingSession(response)
        backend.session = web_session
        backend._bootstrap = mock.Mock()

        with mock.patch(
            "services.openai_backend_api.requests.Session",
            return_value=codex_session,
        ) as session_factory:
            result = backend.list_models()

        self.assertEqual(
            [item["id"] for item in result["data"]],
            ["codex-hidden", "codex-listed", "codex-unlisted"],
        )
        self.assertEqual(
            result["data"][1]["supported_reasoning_efforts"],
            ["high"],
        )
        self.assertEqual(web_session.get_calls, 0)
        self.assertEqual(codex_session.get_calls, 1)
        headers = codex_session.effective_headers
        self.assertIsNotNone(headers)
        assert headers is not None
        self.assertEqual(headers["Authorization"], "Bearer codex-token")
        self.assertEqual(headers["ChatGPT-Account-ID"], "acct-codex")
        self.assertEqual(headers["version"], "0.147.0")
        self.assertEqual(headers["originator"], "codex_cli_rs")
        self.assertTrue(
            headers["User-Agent"].startswith("codex_cli_rs/0.147.0")
        )
        session_kwargs = session_factory.call_args.kwargs
        self.assertFalse(session_kwargs["default_headers"])
        self.assertNotIn("impersonate", session_kwargs)
        for forbidden in (
            "Origin",
            "Referer",
            "OAI-Client-Version",
            "OAI-Client-Build-Number",
            "X-OpenAI-Target-Path",
            "X-OpenAI-Target-Route",
            "Sec-Ch-Ua",
            "OAI-Device-Id",
            "OAI-Session-Id",
        ):
            self.assertNotIn(forbidden, headers)
        self.assertEqual(codex_session.close_calls, 1)
        backend._bootstrap.assert_not_called()

    def test_codex_model_list_rejects_invalid_client_version_before_request(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.base_url = "https://chatgpt.com"
        backend.codex_client_version = "prod-web-build"
        backend.session = mock.Mock()
        backend._bootstrap = mock.Mock()

        with self.assertRaisesRegex(RuntimeError, "client version"):
            backend.list_models()

        backend.session.get.assert_not_called()

    def test_codex_model_session_cleanup_does_not_mask_primary_error(self) -> None:
        class Response:
            status_code = 200
            headers = {"content-type": "application/json"}

            def iter_content(self, **_kwargs: object):
                raise RuntimeError("response-secret")

            def close(self) -> None:
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
        backend.codex_client_version = "0.147.0"
        backend.fp = {"impersonate": "chrome110"}
        backend.session = mock.Mock()
        backend._bootstrap = mock.Mock()
        session = Session()

        with (
            mock.patch("services.openai_backend_api.requests.Session", return_value=session),
            mock.patch("services.openai_backend_api.logger.warning") as warning,
        ):
            with self.assertRaisesRegex(RuntimeError, "invalid response body"):
                backend.list_models()

        self.assertEqual(session.close_calls, 1)
        logged = str(warning.call_args_list)
        self.assertIn("models_session", logged)
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
