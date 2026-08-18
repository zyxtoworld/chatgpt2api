from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest import mock

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

import api.ai as ai_module
import api.accounts as accounts_module
import services.account_service as account_service_module
import services.cpa_service as cpa_service_module
import services.content_filter as content_filter_module
import services.log_service as log_service_module
import services.protocol.conversation as conversation_module
import services.protocol.openai_v1_response as openai_v1_response_module
import services.sub2api_service as sub2api_service_module
from api.errors import install_exception_handlers
from services.log_service import LogService, LoggedCall
from services.openai_backend_api import ImagePollTimeoutError
from services.config import config
from services.model_service import ModelCatalogPendingError
from services.protocol.error_response import PublicSafeError, openai_error_payload, public_exception_message
from utils.helper import UpstreamHTTPError, anthropic_sse_stream, responses_sse_stream, sse_json_stream


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
UPSTREAM_SECRET = "upstream-access-token-secret"


def _app_with_ai_router() -> FastAPI:
    app = FastAPI()
    install_exception_handlers(app)
    app.include_router(ai_module.create_router())
    return app


class PublicErrorContractTests(unittest.TestCase):
    def test_chat_cache_does_not_share_response_between_authenticated_identities(self) -> None:
        old_settings = config.data.get("chat_completion_cache")
        config.data["chat_completion_cache"] = {
            "enabled": True,
            "ttl_seconds": 60,
            "max_entries": 32,
            "dedupe_inflight": True,
            "stream_cache": False,
            "normalize_messages": True,
            "drop_adjacent_duplicates": False,
            "drop_assistant_history": False,
        }
        from services.protocol.chat_completion_cache import chat_completion_cache

        chat_completion_cache.clear()
        try:
            app = _app_with_ai_router()
            body = {
                "model": "auto",
                "messages": [{"role": "user", "content": "identity-sensitive cache probe"}],
            }
            with (
                mock.patch.object(
                    ai_module,
                    "require_identity_async",
                    new=mock.AsyncMock(side_effect=[{"id": "identity-a"}, {"id": "identity-b"}]),
                ),
                mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
                mock.patch.object(ai_module.openai_v1_chat_complete, "text_backend", return_value=object()),
                mock.patch.object(
                    ai_module.openai_v1_chat_complete,
                    "collect_text",
                    side_effect=["response-for-a", "response-for-b"],
                ) as collect_text,
            ):
                client = TestClient(app)
                first = client.post("/v1/chat/completions", json=body, headers={"Authorization": "Bearer a"})
                second = client.post("/v1/chat/completions", json=body, headers={"Authorization": "Bearer b"})

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertEqual(first.json()["choices"][0]["message"]["content"], "response-for-a")
            self.assertEqual(second.json()["choices"][0]["message"]["content"], "response-for-b")
            self.assertEqual(collect_text.call_count, 2)
        finally:
            chat_completion_cache.clear()
            if old_settings is None:
                config.data.pop("chat_completion_cache", None)
            else:
                config.data["chat_completion_cache"] = old_settings

    def test_chat_route_keeps_source_routing_when_identity_cache_scope_is_empty(self) -> None:
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_codex_route",
                "object": "response",
                "status": "completed",
                "model": "gpt-5",
                "output": [{
                    "id": "msg_codex_route",
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": "native-route", "annotations": []}],
                }],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }
        seen: dict[str, object] = {}

        class CodexAccountService:
            def get_text_access_token(self, **_kwargs: object) -> str:
                return "codex-route-token"

            def get_account(self, _token: str) -> dict[str, str]:
                return {"source_type": "codex"}

        async def invoke(_self, handler, payload, **kwargs):
            seen.update(kwargs)
            return handler(payload, **kwargs)

        def native_events(_body: dict[str, object], **kwargs: object):
            seen["native_access_token"] = kwargs.get("access_token")
            yield completed

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
            mock.patch.object(ai_module.openai_v1_chat_complete, "account_service", CodexAccountService()),
            mock.patch.object(
                ai_module.openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                ai_module.openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("empty cache scope must not force Web routing"),
            ),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "auto", "messages": [{"role": "user", "content": "hello"}]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(seen["cache_scope"], "")
        self.assertIs(seen["authenticated"], True)
        self.assertEqual(seen["native_access_token"], "codex-route-token")
        self.assertIn("native-route", response.text)

    def test_chat_route_nonstream_carries_one_deadline_through_initial_codex_selection(self) -> None:
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_chat_deadline_nonstream",
                "object": "response",
                "status": "completed",
                "model": "gpt-5",
                "output": [{
                    "id": "msg_chat_deadline_nonstream",
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{"type": "output_text", "text": "ok", "annotations": []}],
                }],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }
        selector_calls: list[dict[str, object]] = []
        backend_deadlines: list[object] = []

        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                return "codex-chat-deadline"

            def get_account(self, _token: str) -> dict[str, str]:
                return {"source_type": "codex"}

        async def invoke(_self, handler, payload, **kwargs):
            return handler(payload, **kwargs)

        def native_events(_body: dict[str, object], **kwargs: object):
            backend_deadlines.append(kwargs.get("deadline"))
            yield completed

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
            mock.patch.object(ai_module.openai_v1_chat_complete, "account_service", CodexAccountService()),
            mock.patch.object(
                ai_module.openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                ai_module.openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("Codex chat must not use the Web backend"),
            ),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "auto", "messages": [{"role": "user", "content": "hello"}]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(selector_calls), 1)
        self.assertIsInstance(selector_calls[0].get("deadline"), float)
        self.assertEqual(backend_deadlines, [selector_calls[0]["deadline"]])

    def test_chat_route_stream_carries_one_deadline_through_initial_codex_selection(self) -> None:
        created = {
            "type": "response.created",
            "response": {"id": "resp_chat_deadline_stream", "model": "gpt-5"},
        }
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_chat_deadline_stream",
                "object": "response",
                "status": "completed",
                "model": "gpt-5",
                "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }
        selector_calls: list[dict[str, object]] = []
        backend_deadlines: list[object] = []

        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                return "codex-chat-stream-deadline"

            def get_account(self, _token: str) -> dict[str, str]:
                return {"source_type": "codex"}

        async def invoke(_self, handler, payload, **kwargs):
            return handler(payload, **kwargs)

        def native_events(_body: dict[str, object], **kwargs: object):
            backend_deadlines.append(kwargs.get("deadline"))
            yield created
            yield completed

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "run", new=invoke),
            mock.patch.object(ai_module.openai_v1_chat_complete, "account_service", CodexAccountService()),
            mock.patch.object(
                ai_module.openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                ai_module.openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("Codex chat must not use the Web backend"),
            ),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={
                    "model": "auto",
                    "stream": True,
                    "messages": [{"role": "user", "content": "hello"}],
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertIn("resp_chat_deadline_stream", response.text)
        self.assertEqual(len(selector_calls), 1)
        self.assertIsInstance(selector_calls[0].get("deadline"), float)
        self.assertEqual(backend_deadlines, [selector_calls[0]["deadline"]])

    def test_chat_route_initial_selector_timeout_does_not_construct_backend(self) -> None:
        selector_calls: list[dict[str, object]] = []

        class TimeoutAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                raise TimeoutError("refresh deadline expired")

        backend_factory = mock.Mock(side_effect=AssertionError("backend must not be constructed"))
        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "log", return_value=None),
            mock.patch.object(ai_module.openai_v1_chat_complete, "account_service", TimeoutAccountService()),
            mock.patch.object(openai_v1_response_module, "account_service", TimeoutAccountService()),
            mock.patch.object(openai_v1_response_module, "OpenAIBackendAPI", backend_factory),
            mock.patch.object(
                ai_module.openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("selector timeout must not fall through to Web"),
            ),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "auto", "messages": [{"role": "user", "content": "hello"}]},
            )

        self.assertEqual(response.status_code, 502, response.text)
        self.assertEqual(len(selector_calls), 1)
        self.assertIsInstance(selector_calls[0].get("deadline"), float)
        backend_factory.assert_not_called()

    def test_chat_route_failover_preserves_one_deadline_and_last_upstream_error(self) -> None:
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_chat_failover",
                "object": "response",
                "status": "completed",
                "model": "gpt-5",
                "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }
        selector_calls: list[dict[str, object]] = []
        backend_timeouts: list[float] = []

        class FailoverAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                excluded = set(kwargs.get("excluded_tokens") or set())
                if "codex-chat-first" not in excluded:
                    return "codex-chat-first"
                if "codex-chat-second" not in excluded:
                    return "codex-chat-second"
                raise AssertionError("selector must not loop after candidates are exhausted")

            def get_account(self, _token: str) -> dict[str, str]:
                return {"source_type": "codex"}

            def mark_text_used(self, _token: str) -> None:
                pass

        class FailoverBackend:
            instances: list["FailoverBackend"] = []

            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.__class__.instances.append(self)

            def iter_codex_response_events(self, _payload: dict[str, object], *, timeout: float):
                backend_timeouts.append(timeout)
                if self.access_token == "codex-chat-first":
                    raise openai_v1_response_module.UpstreamHTTPError(
                        "codex",
                        502,
                        {"error": "transient"},
                    )
                yield completed

            def close(self) -> None:
                pass

        service = FailoverAccountService()
        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "log", return_value=None),
            mock.patch.object(ai_module.openai_v1_chat_complete, "account_service", service),
            mock.patch.object(openai_v1_response_module, "account_service", service),
            mock.patch.object(openai_v1_response_module, "OpenAIBackendAPI", FailoverBackend),
            mock.patch.object(openai_v1_response_module, "resolve_codex_reasoning_effort"),
            mock.patch.object(
                ai_module.openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("Codex failover must not use Web"),
            ),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "auto", "messages": [{"role": "user", "content": "hello"}]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(selector_calls), 2)
        deadline = selector_calls[0]["deadline"]
        self.assertIsInstance(deadline, float)
        self.assertEqual(selector_calls[1]["deadline"], deadline)
        self.assertEqual(len(backend_timeouts), 2)
        self.assertTrue(all(timeout > 0 for timeout in backend_timeouts))
        self.assertTrue(all(timeout <= 30.0 for timeout in backend_timeouts))

    def test_chat_route_failover_timeout_keeps_last_502(self) -> None:
        selector_calls: list[dict[str, object]] = []
        backend_calls: list[str] = []

        class ExhaustedAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                excluded = set(kwargs.get("excluded_tokens") or set())
                if "codex-chat-first" not in excluded:
                    return "codex-chat-first"
                if "codex-chat-second" not in excluded:
                    return "codex-chat-second"
                raise TimeoutError("failover deadline expired")

            def get_account(self, _token: str) -> dict[str, str]:
                return {"source_type": "codex"}

            def mark_text_used(self, _token: str) -> None:
                pass

        class AlwaysFailingBackend:
            def __init__(self, access_token: str) -> None:
                backend_calls.append(access_token)

            def iter_codex_response_events(self, _payload: dict[str, object], *, timeout: float):
                del timeout
                raise openai_v1_response_module.UpstreamHTTPError(
                    "codex",
                    502,
                    {"error": "transient"},
                )
                yield  # pragma: no cover

            def close(self) -> None:
                pass

        service = ExhaustedAccountService()
        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "", "role": "user"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.LoggedCall, "log", return_value=None),
            mock.patch.object(ai_module.openai_v1_chat_complete, "account_service", service),
            mock.patch.object(openai_v1_response_module, "account_service", service),
            mock.patch.object(openai_v1_response_module, "OpenAIBackendAPI", AlwaysFailingBackend),
            mock.patch.object(openai_v1_response_module, "resolve_codex_reasoning_effort"),
            mock.patch.object(
                ai_module.openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("Codex failover must not use Web"),
            ),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "auto", "messages": [{"role": "user", "content": "hello"}]},
            )

        self.assertEqual(response.status_code, 502, response.text)
        self.assertEqual(backend_calls, ["codex-chat-first", "codex-chat-second"])
        self.assertEqual(len(selector_calls), 3)
        self.assertEqual(selector_calls[0]["deadline"], selector_calls[1]["deadline"])
        self.assertEqual(selector_calls[1]["deadline"], selector_calls[2]["deadline"])

    def test_responses_cache_does_not_share_response_between_authenticated_identities(self) -> None:
        old_settings = config.data.get("chat_completion_cache")
        config.data["chat_completion_cache"] = {
            "enabled": True,
            "ttl_seconds": 60,
            "max_entries": 32,
            "dedupe_inflight": True,
            "stream_cache": True,
            "normalize_messages": True,
            "drop_adjacent_duplicates": False,
            "drop_assistant_history": False,
        }
        from services.protocol.chat_completion_cache import chat_completion_cache

        chat_completion_cache.clear()
        try:
            app = _app_with_ai_router()
            body = {"model": "auto", "input": "identity-sensitive responses cache probe"}
            with (
                mock.patch.object(
                    ai_module,
                    "require_identity_async",
                    new=mock.AsyncMock(side_effect=[{"id": "identity-a"}, {"id": "identity-b"}]),
                ),
                mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
                mock.patch.object(ai_module.openai_v1_response, "text_backend", return_value=object()),
                mock.patch.object(
                    ai_module.openai_v1_response,
                    "stream_text_deltas",
                    side_effect=[iter(["response-for-a"]), iter(["response-for-b"])],
                ) as stream_text_deltas,
            ):
                client = TestClient(app)
                first = client.post("/v1/responses", json=body, headers={"Authorization": "Bearer a"})
                second = client.post("/v1/responses", json=body, headers={"Authorization": "Bearer b"})

            self.assertEqual(first.status_code, 200)
            self.assertEqual(second.status_code, 200)
            self.assertIn("response-for-a", first.text)
            self.assertIn("response-for-b", second.text)
            self.assertEqual(stream_text_deltas.call_count, 2)
        finally:
            chat_completion_cache.clear()
            if old_settings is None:
                config.data.pop("chat_completion_cache", None)
            else:
                config.data["chat_completion_cache"] = old_settings

    def test_logged_call_does_not_turn_success_into_error_when_log_write_fails(self) -> None:
        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/chat/completions",
            "auto",
            "chat",
            request_text="private request token=opaque-request-secret",
        )

        with (
            mock.patch.object(
                log_service_module.log_service,
                "add",
                side_effect=OSError("log disk unavailable private-path"),
            ),
            mock.patch.object(log_service_module._LOGGER, "error") as fallback_logger,
        ):
            result = asyncio.run(call.run(lambda: {"id": "response-1", "object": "response"}))

        self.assertEqual(result, {"id": "response-1", "object": "response"})
        fallback_logger.assert_called_once_with(
            "log persistence failed",
            extra={"error_type": "OSError"},
        )
        self.assertNotIn("opaque-request-secret", repr(fallback_logger.call_args))
        self.assertNotIn("private-path", repr(fallback_logger.call_args))

    def test_logged_call_redacts_request_credentials_and_signed_url_text(self) -> None:
        bearer = "upstream-bearer-secret"
        cookie = "session-cookie-secret"
        query_secret = "signed-query-secret"
        image_data = "A" * 128
        ansi = "\x1b[31m"
        with TemporaryDirectory() as directory:
            old_log_service = log_service_module.log_service
            log_service_module.log_service = LogService(Path(directory) / "logs.jsonl")
            try:
                call = LoggedCall(
                    {"id": "user-1", "name": "user", "role": "user"},
                    "/v1/chat/completions",
                    "auto",
                    "chat",
                    request_text=(
                        f"{ansi}Authorization: Bearer {bearer}\r\n"
                        f"cookie=session={cookie} "
                        f"https://cdn.example.test/image.png?token={query_secret}#fragment "
                        f"data:image/png;base64,{image_data} C:\\Users\\owner\\private.txt"
                    ),
                )
                call.log("调用失败", status="failed", error="upstream error")
                persisted = (Path(directory) / "logs.jsonl").read_text(encoding="utf-8")
            finally:
                log_service_module.log_service = old_log_service

        self.assertNotIn(bearer, persisted)
        self.assertNotIn(cookie, persisted)
        self.assertNotIn(query_secret, persisted)
        self.assertNotIn(image_data, persisted)
        self.assertNotIn("C:\\Users\\owner", persisted)
        self.assertNotIn("Authorization:", persisted)
        self.assertNotIn("?token=", persisted)
        self.assertNotIn("#fragment", persisted)
        self.assertNotIn("\x1b", persisted)
        self.assertEqual(len(persisted.splitlines()), 1)
        self.assertIn("https://cdn.example.test/image.png", persisted)

    def test_logged_stream_keeps_items_when_log_write_fails(self) -> None:
        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/chat/completions",
            "auto",
            "chat",
        )

        with mock.patch.object(log_service_module.log_service, "add", side_effect=OSError("log disk unavailable")):
            items = list(call.stream(iter([{"data": "first"}])))

        self.assertEqual(items, [{"data": "first"}])

    def test_logged_stream_preserves_upstream_exception_when_failure_log_fails(self) -> None:
        class UpstreamFailure(RuntimeError):
            pass

        marker = UpstreamFailure("upstream detail")

        def failing_stream():
            yield {"data": "first"}
            raise marker

        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/chat/completions",
            "auto",
            "chat",
        )

        with (
            mock.patch.object(
                log_service_module.log_service,
                "add",
                side_effect=OSError("log disk unavailable secret-path"),
            ),
            mock.patch.object(log_service_module._LOGGER, "error") as fallback_logger,
            self.assertRaises(UpstreamFailure) as raised,
        ):
            list(call.stream(failing_stream()))

        self.assertIs(raised.exception, marker)
        fallback_logger.assert_called_once_with(
            "log persistence failed",
            extra={"error_type": "OSError"},
        )
        self.assertNotIn("log disk unavailable", repr(fallback_logger.call_args))
        self.assertNotIn("upstream detail", repr(fallback_logger.call_args))

    @staticmethod
    def _persist_logged_result_url(url: str) -> str:
        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/chat/completions",
            "auto",
            "chat",
        )
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "logs.jsonl"
            service = LogService(path)
            with mock.patch("services.log_service.log_service", service):
                response = asyncio.run(call.run(lambda: {"data": [{"url": url}]}))
            if response != {"data": [{"url": url}]}:
                raise AssertionError("LoggedCall changed the handler result")
            return path.read_text(encoding="utf-8")

    def test_logged_call_drops_result_url_with_userinfo(self) -> None:
        persisted = self._persist_logged_result_url(
            "https://user:password@cdn.example.test/private.png"
        )

        self.assertNotIn("user:password", persisted)
        self.assertNotIn("https://cdn.example.test/private.png", persisted)

    def test_logged_call_strips_query_and_fragment_from_result_url(self) -> None:
        secret = "signed-result-query-canary owner@example.com"
        persisted = self._persist_logged_result_url(
            f"https://cdn.example.test/signed.png?token={secret}#fragment"
        )

        self.assertNotIn(secret, persisted)
        self.assertNotIn("token=", persisted)
        self.assertNotIn("#fragment", persisted)
        self.assertIn("https://cdn.example.test/signed.png", persisted)

    def test_logged_call_preserves_safe_result_url(self) -> None:
        persisted = self._persist_logged_result_url("https://cdn.example.test/safe.png")

        self.assertIn("https://cdn.example.test/safe.png", persisted)

    def test_conversation_text_container_fails_closed_instead_of_stringifying(self) -> None:
        canary = "conversation-text-container-canary owner@example.test"
        payload = json.dumps({
            "message": {
                "author": {"role": "assistant"},
                "channel": "final",
                "recipient": "all",
                "content": {"content_type": "code", "text": {"secret": canary}},
            }
        })

        with self.assertRaisesRegex(RuntimeError, "malformed"):
            list(conversation_module.iter_conversation_payloads(iter([payload, "[DONE]"])) )

    def test_conversation_text_patch_container_fails_closed_instead_of_stringifying(self) -> None:
        canary = "conversation-text-patch-container-canary owner@example.test"
        payload = json.dumps({
            "p": "/message/content/parts/0",
            "o": "append",
            "v": {"secret": canary},
        })

        with self.assertRaisesRegex(RuntimeError, "malformed"):
            list(conversation_module.iter_conversation_payloads(iter([payload, "[DONE]"])) )

    def test_conversation_metadata_does_not_stringify_container_fields(self) -> None:
        canary = "conversation-metadata-container-canary owner@example.com"
        payload = json.dumps({
            "type": "server_ste_metadata",
            "metadata": {"turn_use_case": {"secret": canary}},
        })

        events = list(conversation_module.iter_conversation_payloads(iter([payload, "[DONE]"])) )
        self.assertTrue(events)
        self.assertEqual(events[0]["turn_use_case"], "")
        self.assertNotIn(canary, events[0]["turn_use_case"])

    def test_image_progress_does_not_stringify_container_event_type(self) -> None:
        canary = "image-event-type-container-canary owner@example.com"

        class Backend:
            def stream_conversation(self, **kwargs):
                yield json.dumps({"type": {"secret": canary}})
                yield "[DONE]"

            def resolve_conversation_image_urls(self, *args, **kwargs):
                return []

        request = conversation_module.ConversationRequest(
            prompt="draw",
            model="gpt-image-2",
        )
        progress = list(conversation_module.stream_image_outputs(Backend(), request))

        serialized = json.dumps(progress, ensure_ascii=False, default=str)
        self.assertNotIn(canary, serialized)
        self.assertTrue(progress)
        self.assertEqual(progress[0].upstream_event_type, "")

    def test_client_error_payload_does_not_stringify_container_fields(self) -> None:
        canary = "error-payload-container-canary owner@example.test"

        payload = openai_error_payload(
            {
                "error": {
                    "message": {"secret": canary},
                    "type": {"secret": canary},
                    "code": [canary],
                    "param": {"secret": canary},
                }
            },
            400,
        )

        serialized = json.dumps(payload, ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        self.assertEqual(payload["error"]["message"], "request failed")
        self.assertEqual(payload["error"]["type"], "invalid_request_error")
        self.assertEqual(payload["error"]["code"], "bad_request")
        self.assertIsNone(payload["error"]["param"])

    def test_codex_response_event_projection_drops_unknown_nested_fields(self) -> None:
        canary = "codex-event-unknown-field-canary owner@example.test"
        event = {
            "type": "response.completed",
            "sequence_number": 4,
            "future_event_field": canary,
            "response": {
                "id": "resp_public",
                "object": "response",
                "created_at": 1_765_000_000,
                "status": "completed",
                "model": "gpt-test",
                "future_response_field": canary,
                "output": [{
                    "id": "msg_public",
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "future_item_field": canary,
                    "content": [],
                }],
            },
        }

        projected = openai_v1_response_module.project_public_codex_response_event(event)

        self.assertIsNotNone(projected)
        serialized = json.dumps(projected, ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        self.assertNotIn("future_event_field", projected)
        self.assertNotIn("future_response_field", projected["response"])
        self.assertNotIn("future_item_field", projected["response"]["output"][0])

    def test_codex_response_event_projection_preserves_search_citations(self) -> None:
        event = {
            "type": "response.completed",
            "response": {
                "id": "resp_search",
                "status": "completed",
                "output": [{
                    "id": "msg_search",
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "answer",
                        "annotations": [{
                            "type": "url_citation",
                            "url": "https://example.test/source",
                            "title": "Source",
                            "start_index": 0,
                            "end_index": 6,
                            "future_annotation_field": "drop-me",
                        }],
                    }],
                }],
            },
        }

        projected = openai_v1_response_module.project_public_codex_response_event(event)
        citation = projected["response"]["output"][0]["content"][0]["annotations"][0]
        self.assertEqual(citation["url"], "https://example.test/source")
        self.assertEqual(citation["title"], "Source")
        self.assertEqual(citation["start_index"], 0)
        self.assertEqual(citation["end_index"], 6)
        self.assertNotIn("future_annotation_field", citation)

    def test_responses_sse_uses_typed_events_without_chat_done_sentinel(self) -> None:
        output = "".join(responses_sse_stream([
            {"type": "response.created", "response": {"id": "resp_1"}},
            {"type": "response.completed", "response": {"id": "resp_1", "status": "completed"}},
        ]))

        self.assertIn("event: response.created\n", output)
        self.assertIn("event: response.completed\n", output)
        self.assertNotIn("[DONE]", output)

    def test_responses_sse_midstream_failure_is_typed_and_does_not_leak(self) -> None:
        secret = "opaque-responses-stream-secret owner@example.com"

        def failing_items():
            yield {"type": "response.created", "response": {"id": "resp_1"}}
            raise RuntimeError(secret)

        output = "".join(responses_sse_stream(failing_items()))

        self.assertIn("event: error\n", output)
        self.assertNotIn(secret, output)
        self.assertNotIn("[DONE]", output)

    def test_public_sse_rejects_container_event_types_without_serializing_them(self) -> None:
        canary = "sse-event-type-container-secret"
        malformed = [{"type": {"secret": canary}, "payload": "ignored"}]

        responses_output = "".join(responses_sse_stream(malformed))
        anthropic_output = "".join(anthropic_sse_stream(malformed))

        for output in (responses_output, anthropic_output):
            self.assertIn("event: error\n", output)
            self.assertNotIn(canary, output)

    def test_logged_public_stream_drops_explicit_credential_fields(self) -> None:
        canary = "stream-access-token-canary owner@example.com"
        item = {
            "type": "response.completed",
            "response": {"id": "resp_1", "status": "completed"},
            "access_token": canary,
            "refresh_token": canary,
            "id_token": canary,
            "password": canary,
            "account_email": canary,
        }
        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/responses",
            "auto",
            "responses",
        )

        with mock.patch("services.log_service.log_service.add"):
            projected = list(call.stream([item]))
        output = "".join(responses_sse_stream(projected))

        self.assertNotIn(canary, output)
        for key in ("access_token", "refresh_token", "id_token", "password", "account_email"):
            self.assertNotIn(key, output)

    def test_logged_responses_stream_drops_unknown_success_fields(self) -> None:
        canary = "responses-success-metadata-canary owner@example.com"
        item = {
            "type": "response.output_text.delta",
            "item_id": "msg_1",
            "output_index": 0,
            "content_index": 0,
            "delta": "hello",
            "metadata": {"private_debug": canary},
            "nested": {"canary": canary},
        }
        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/responses",
            "auto",
            "Responses",
        )

        with mock.patch("services.log_service.log_service.add"):
            projected = list(call.stream([item]))

        self.assertEqual(projected[0]["delta"], "hello")
        serialized = json.dumps(projected, ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        self.assertNotIn("metadata", projected[0])
        self.assertNotIn("nested", projected[0])

    def test_logged_public_json_drops_explicit_credential_fields(self) -> None:
        canary = "json-access-token-canary owner@example.com"
        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/chat/completions",
            "auto",
            "chat",
        )

        with mock.patch("services.log_service.log_service.add"):
            response = asyncio.run(call.run(lambda: {
                "access_token": canary,
                "refresh_token": canary,
                "id_token": canary,
                "password": canary,
                "account_email": canary,
                "answer": "ok",
            }))

        serialized = json.dumps(response, ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        for key in ("access_token", "refresh_token", "id_token", "password", "account_email"):
            self.assertNotIn(key, response)
        self.assertEqual(response["answer"], "ok")

    def test_responses_upstream_failure_events_are_projected_before_sse_and_logging(self) -> None:
        secret = "opaque-terminal-secret owner@example.test"
        private_url = "https://user:private-password@upstream.invalid/private"
        private_response_id = "resp_privateToken98765"
        cases = [
            (
                "response.failed",
                {
                    "type": "response.failed",
                    "sequence_number": 7,
                    "response": {
                        "id": private_response_id,
                        "object": "response",
                        "created_at": 1_765_000_000,
                        "status": "failed",
                        "model": "gpt-test",
                        "output": [],
                        "error": {
                            "code": "opaque_upstream_code",
                            "message": secret,
                            "url": private_url,
                        },
                    },
                },
            ),
            (
                "error",
                {
                    "type": "error",
                    "sequence_number": 8,
                    "code": "opaque_upstream_code",
                    "message": secret,
                    "param": None,
                    "headers": {"location": private_url},
                    "url": private_url,
                },
            ),
        ]

        for event_type, upstream_event in cases:
            with self.subTest(event_type=event_type):
                with (
                    mock.patch.object(
                        ai_module,
                        "require_identity_async",
                        return_value={"id": "user-1", "name": "user", "role": "user"},
                    ),
                    mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
                    mock.patch.object(
                        ai_module.openai_v1_response,
                        "handle",
                        return_value=iter([upstream_event]),
                    ),
                    mock.patch("services.log_service.log_service.add") as add,
                ):
                    response = TestClient(_app_with_ai_router()).post(
                        "/v1/responses",
                        headers=AUTH_HEADERS,
                        json={"model": "gpt-test", "input": "hello", "stream": True},
                    )

                self.assertEqual(response.status_code, 200, response.text)
                data_line = next(
                    line.removeprefix("data: ")
                    for line in response.text.splitlines()
                    if line.startswith("data: ")
                )
                public_event = json.loads(data_line)
                serialized_log = json.dumps(add.call_args_list, ensure_ascii=False, default=str)

                self.assertEqual(public_event["type"], event_type)
                if event_type == "response.failed":
                    self.assertEqual(
                        public_event["response"]["error"]["message"],
                        ai_module.PUBLIC_SERVER_ERROR_MESSAGE,
                    )
                else:
                    self.assertEqual(public_event["message"], ai_module.PUBLIC_SERVER_ERROR_MESSAGE)
                for private_value in (
                    secret,
                    private_url,
                    private_response_id,
                    "private-password",
                    "owner@example.test",
                ):
                    self.assertNotIn(private_value, response.text)
                    self.assertNotIn(private_value, serialized_log)
                self.assertIn('"status": "failed"', serialized_log)
                self.assertNotIn("流式调用结束", serialized_log)

    def test_internal_codex_metadata_events_are_not_public_or_logged_over_sse(self) -> None:
        secret = "opaque-codex-metadata-secret owner@example.test"
        private_url = "https://user:private-password@upstream.invalid/internal"
        public_item = {
            "id": "msg_public",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "hello", "annotations": []}],
        }
        private_item = {
            **public_item,
            "internal_chat_message_metadata_passthrough": {
                "turn_id": secret,
                "url": private_url,
            },
        }
        upstream_events = [
            {
                "type": "response.metadata",
                "headers": {"location": private_url},
                "metadata": {"turn_state": secret},
            },
            {
                "type": "codex.response.metadata",
                "metadata": {"private_debug": secret, "url": private_url},
            },
            {
                "type": "responsesapi.websocket_timing",
                "timing": {"private_debug": secret, "url": private_url},
            },
            {
                "type": "response.output_text.delta",
                "item_id": "msg_public",
                "output_index": 0,
                "content_index": 0,
                "delta": "hello",
                "headers": {"location": private_url},
                "metadata": {"private_debug": secret},
                "safety_buffering": {"private_debug": secret, "url": private_url},
            },
            {
                "type": "response.output_item.done",
                "output_index": 0,
                "item": private_item,
            },
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_public",
                    "object": "response",
                    "status": "completed",
                    "model": "gpt-test",
                    "output": [public_item],
                    "headers": {"location": private_url, "private_debug": secret},
                },
            },
        ]

        class FakeBackend:
            def __init__(self, *, access_token: str) -> None:
                self.access_token = access_token

            def iter_codex_response_events(self, _payload):
                yield from upstream_events

            def close(self) -> None:
                pass

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                return_value={"id": "user-1", "name": "user", "role": "user"},
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-access-token",
            ),
            mock.patch.object(ai_module.openai_v1_response.account_service, "mark_text_used"),
            mock.patch.object(ai_module.openai_v1_response, "OpenAIBackendAPI", FakeBackend),
            mock.patch("services.log_service.log_service.add") as add,
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/responses",
                headers=AUTH_HEADERS,
                json={
                    "model": "gpt-test",
                    "input": "hello",
                    "tool_choice": "auto",
                    "stream": True,
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        public_events = [
            json.loads(line.removeprefix("data: "))
            for line in response.text.splitlines()
            if line.startswith("data: ")
        ]
        self.assertEqual(
            [event["type"] for event in public_events],
            ["response.output_text.delta", "response.output_item.done", "response.completed"],
        )
        self.assertEqual(public_events[0]["delta"], "hello")
        self.assertNotIn("internal_chat_message_metadata_passthrough", public_events[1]["item"])
        self.assertNotIn(
            "internal_chat_message_metadata_passthrough",
            public_events[2]["response"]["output"][0],
        )
        serialized_log = json.dumps(add.call_args_list, ensure_ascii=False, default=str)
        for private_value in (secret, private_url, "private-password", "owner@example.test"):
            self.assertNotIn(private_value, response.text)
            self.assertNotIn(private_value, serialized_log)

    def test_responses_route_selects_responses_sse_framing(self) -> None:
        seen: dict[str, object] = {}

        async def fake_run(_self, _handler, *_args, **kwargs):
            seen.update(kwargs)
            return {"id": "resp_1", "status": "completed", "output": []}

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(LoggedCall, "run", new=fake_run),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/responses",
                headers=AUTH_HEADERS,
                json={"model": "auto", "input": "hello", "stream": True},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(seen.get("sse"), "responses")

    def test_image_routes_select_typed_image_sse_framing(self) -> None:
        seen: list[object] = []

        async def fake_run(_self, _handler, *_args, **kwargs):
            seen.append(kwargs.get("sse"))
            return {"created": 1, "data": []}

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(LoggedCall, "run", new=fake_run),
            mock.patch.object(ai_module, "read_image_sources", new=mock.AsyncMock(return_value=[(b"png", "a.png", "image/png")])),
        ):
            client = TestClient(_app_with_ai_router())
            generation = client.post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={"model": "gpt-image-2", "prompt": "cat", "stream": True},
            )
            edit = client.post(
                "/v1/images/edits",
                headers={**AUTH_HEADERS, "Content-Type": "application/json"},
                json={"model": "gpt-image-2", "prompt": "cat", "image": "data:image/png;base64,cG5n", "stream": True},
            )

        self.assertEqual(generation.status_code, 200, generation.text)
        self.assertEqual(edit.status_code, 200, edit.text)
        self.assertEqual(seen, ["images", "images"])

    def test_image_generation_stream_emits_official_typed_sse_without_done_sentinel(self) -> None:
        event = {
            "type": "image_generation.completed",
            "b64_json": "ZmFrZQ==",
            "usage": {"total_tokens": 7, "input_tokens": 3, "output_tokens": 4},
        }
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.openai_v1_image_generations, "handle", return_value=iter([event])),
            mock.patch("services.log_service.log_service.add"),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/images/generations",
                headers=AUTH_HEADERS,
                json={"model": "gpt-image-2", "prompt": "cat", "stream": True},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertTrue(response.headers["content-type"].startswith("text/event-stream"))
        self.assertIn("event: image_generation.completed\n", response.text)
        self.assertIn(f"data: {json.dumps(event, ensure_ascii=False)}\n\n", response.text)
        self.assertNotIn("[DONE]", response.text)

    def test_models_does_not_return_upstream_body_to_client(self) -> None:
        error = UpstreamHTTPError(
            "/backend-api/models",
            401,
            {"access_token": UPSTREAM_SECRET, "message": "private upstream detail"},
        )
        with mock.patch.object(ai_module.openai_v1_models, "list_models", side_effect=error):
            response = TestClient(_app_with_ai_router()).get("/v1/models", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 502, response.text)
        self.assertNotIn(UPSTREAM_SECRET, response.text)
        self.assertNotIn("private upstream detail", response.text)

    def test_logged_call_does_not_persist_upstream_body_to_client_or_log(self) -> None:
        error = UpstreamHTTPError(
            "/backend-api/conversation/private",
            502,
            {"refresh_token": UPSTREAM_SECRET, "message": "private upstream detail"},
        )

        def handler() -> None:
            raise error

        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/chat/completions",
            "auto",
            "chat",
        )
        with mock.patch("services.log_service.log_service.add") as add:
            response = asyncio.run(call.run(handler))

        self.assertEqual(response.status_code, 502)
        self.assertNotIn(UPSTREAM_SECRET, response.body.decode("utf-8"))
        self.assertNotIn(UPSTREAM_SECRET, json.dumps(add.call_args, ensure_ascii=False, default=str))

    def test_logged_call_generic_error_is_safe_for_openai_and_anthropic_json(self) -> None:
        secret = "opaque-protocol-secret owner@example.com upstream body"

        def handler() -> None:
            raise RuntimeError(secret)

        for sse in ("openai", "anthropic"):
            with self.subTest(sse=sse):
                call = LoggedCall(
                    {"id": "user-1", "name": "user", "role": "user"},
                    "/v1/chat/completions",
                    "auto",
                    "chat",
                )
                with mock.patch("services.log_service.log_service.add") as add:
                    response = asyncio.run(call.run(handler, sse=sse))

                self.assertEqual(response.status_code, 502)
                self.assertNotIn(secret, response.body.decode("utf-8"))
                self.assertNotIn(secret, json.dumps(add.call_args, ensure_ascii=False, default=str))

    def test_model_unavailable_is_a_client_error_for_non_streaming_calls(self) -> None:
        from services.model_service import ModelUnavailableError

        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/chat/completions",
            "missing-model",
            "chat",
        )
        with mock.patch("services.log_service.log_service.add"):
            response = asyncio.run(call.run(lambda: (_ for _ in ()).throw(ModelUnavailableError("missing-model"))))

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["error"]["type"], "invalid_request_error")
        self.assertNotIn("missing-model", response.body.decode("utf-8"))

    def test_model_catalog_pending_is_retryable_for_chat_and_responses_http(self) -> None:
        for endpoint, module in (
            ("/v1/chat/completions", ai_module.openai_v1_chat_complete),
            ("/v1/responses", ai_module.openai_v1_response),
        ):
            with self.subTest(endpoint=endpoint):
                with (
                    mock.patch.object(
                        ai_module,
                        "require_identity_async",
                        return_value={"id": "user-1", "role": "user"},
                    ),
                    mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
                    mock.patch.object(
                        module.account_service,
                        "get_text_access_token",
                        side_effect=ModelCatalogPendingError("private catalog details"),
                    ),
                    mock.patch("services.log_service.log_service.add"),
                ):
                    body = (
                        {"model": "pending-model", "messages": [{"role": "user", "content": "hello"}]}
                        if endpoint.endswith("chat/completions")
                        else {"model": "pending-model", "input": "hello"}
                    )
                    response = TestClient(_app_with_ai_router()).post(
                        endpoint,
                        headers=AUTH_HEADERS,
                        json=body,
                    )

                self.assertEqual(response.status_code, 503, response.text)
                self.assertEqual(response.headers.get("Retry-After"), "5")
                self.assertIn("warming", response.json()["error"]["message"])
                self.assertNotIn("private catalog details", response.text)

    def test_model_unavailable_remains_a_client_error_for_chat_and_responses_http(self) -> None:
        from services.model_service import ModelUnavailableError

        for endpoint, module in (
            ("/v1/chat/completions", ai_module.openai_v1_chat_complete),
            ("/v1/responses", ai_module.openai_v1_response),
        ):
            with self.subTest(endpoint=endpoint):
                with (
                    mock.patch.object(
                        ai_module,
                        "require_identity_async",
                        return_value={"id": "user-1", "role": "user"},
                    ),
                    mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
                    mock.patch.object(
                        module.account_service,
                        "get_text_access_token",
                        side_effect=ModelUnavailableError("private unavailable details"),
                    ),
                    mock.patch("services.log_service.log_service.add"),
                ):
                    body = (
                        {"model": "missing-model", "messages": [{"role": "user", "content": "hello"}]}
                        if endpoint.endswith("chat/completions")
                        else {"model": "missing-model", "input": "hello"}
                    )
                    response = TestClient(_app_with_ai_router()).post(
                        endpoint,
                        headers=AUTH_HEADERS,
                        json=body,
                    )

                self.assertEqual(response.status_code, 400, response.text)
                self.assertEqual(response.json()["error"]["type"], "invalid_request_error")
                self.assertIsNone(response.headers.get("Retry-After"))
                self.assertNotIn("private unavailable details", response.text)

    def test_model_unavailable_is_a_client_error_when_stream_starts(self) -> None:
        from services.model_service import ModelUnavailableError

        def handler():
            raise ModelUnavailableError("missing-model")
            yield  # pragma: no cover

        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/chat/completions",
            "missing-model",
            "chat",
        )
        with mock.patch("services.log_service.log_service.add"):
            response = asyncio.run(call.run(handler))

        payload = json.loads(response.body)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(payload["error"]["type"], "invalid_request_error")
        self.assertNotIn("missing-model", response.body.decode("utf-8"))

    def test_chat_route_projects_unavailable_model_as_invalid_request(self) -> None:
        from services.model_service import ModelUnavailableError

        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "user-1", "role": "user"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.openai_v1_chat_complete.account_service,
                "get_text_access_token",
                return_value="selected-token",
            ),
            mock.patch.object(
                ai_module.openai_v1_chat_complete,
                "text_backend",
                side_effect=ModelUnavailableError("private-model-name"),
            ),
            mock.patch("services.log_service.log_service.add"),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/chat/completions",
                headers=AUTH_HEADERS,
                json={"model": "private-model-name", "messages": [{"role": "user", "content": "hello"}]},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(response.json()["error"]["type"], "invalid_request_error")
        self.assertNotIn("private-model-name", response.text)

    def test_logged_call_first_iterator_failure_is_safe_for_both_protocols(self) -> None:
        secret = "opaque-first-item-secret owner@example.com upstream body"

        def failing_items():
            raise RuntimeError(secret)
            yield  # pragma: no cover

        def handler():
            return failing_items()

        for sse in ("openai", "anthropic"):
            with self.subTest(sse=sse):
                call = LoggedCall(
                    {"id": "user-1", "name": "user", "role": "user"},
                    "/v1/chat/completions",
                    "auto",
                    "chat",
                )
                with mock.patch("services.log_service.log_service.add") as add:
                    response = asyncio.run(call.run(handler, sse=sse))

                self.assertEqual(response.status_code, 502)
                self.assertNotIn(secret, response.body.decode("utf-8"))
                self.assertNotIn(secret, json.dumps(add.call_args, ensure_ascii=False, default=str))

    def test_sub2api_upstream_failure_does_not_return_body(self) -> None:
        class FakeSub2APIConfig:
            @staticmethod
            def get_server(_server_id):
                return {"id": "server-1", "base_url": "https://sub2api.example"}

        error = UpstreamHTTPError(
            "/api/v1/admin/accounts",
            502,
            {"access_token": UPSTREAM_SECRET, "message": "private upstream detail"},
        )
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        with (
            mock.patch.object(accounts_module, "sub2api_config", FakeSub2APIConfig()),
            mock.patch.object(accounts_module, "sub2api_list_remote_groups", side_effect=error),
        ):
            response = TestClient(app).get(
                "/api/sub2api/servers/server-1/groups",
                headers=AUTH_HEADERS,
            )

        self.assertEqual(response.status_code, 502, response.text)
        self.assertNotIn(UPSTREAM_SECRET, response.text)
        self.assertNotIn("private upstream detail", response.text)

    def test_stream_errors_do_not_return_or_log_upstream_body(self) -> None:
        def failing_items():
            yield {"delta": "partial"}
            raise UpstreamHTTPError(
                "/backend-api/conversation/private",
                502,
                {"access_token": UPSTREAM_SECRET, "message": "private upstream detail"},
            )

        with mock.patch("utils.helper.logger.warning") as warning:
            openai_output = "".join(sse_json_stream(failing_items()))
            anthropic_output = "".join(anthropic_sse_stream(failing_items()))

        for output in (openai_output, anthropic_output):
            self.assertNotIn(UPSTREAM_SECRET, output)
            self.assertNotIn("private upstream detail", output)
            self.assertIn("The upstream request failed. Please try again later.", output)
        self.assertNotIn(UPSTREAM_SECRET, json.dumps(warning.call_args_list, default=str))
        self.assertNotIn("private upstream detail", json.dumps(warning.call_args_list, default=str))

    def test_unknown_image_exception_cannot_duck_type_a_public_error(self) -> None:
        secret = "duck-typed-image-secret owner@example.com"

        class EvilImageError(RuntimeError):
            status_code = 400

            def to_openai_error(self):
                return {"error": {"message": secret, "code": secret}}

        def handler():
            raise EvilImageError(secret)

        call = LoggedCall(
            {"id": "user-1", "name": "user", "role": "user"},
            "/v1/images/generations",
            "gpt-image-2",
            "文生图",
        )
        response = asyncio.run(call.run(handler))
        self.assertEqual(response.status_code, 502)
        self.assertNotIn(secret, response.body.decode("utf-8"))

        def failing_items():
            raise EvilImageError(secret)
            yield  # pragma: no cover

        output = "".join(sse_json_stream(failing_items()))
        self.assertNotIn(secret, output)
        self.assertIn("The upstream request failed. Please try again later.", output)

    def test_remote_import_errors_do_not_persist_upstream_body(self) -> None:
        class FailedResponse:
            ok = False
            status_code = 502
            text = json.dumps({"access_token": UPSTREAM_SECRET, "message": "private upstream detail"})

        class FailedLoginSession:
            def __init__(self, *args, **kwargs):
                pass

            def post(self, *args, **kwargs):
                return FailedResponse()

            def close(self):
                pass

        with mock.patch.object(sub2api_service_module, "Session", FailedLoginSession):
            with self.assertRaises(RuntimeError) as raised:
                sub2api_service_module._login("https://sub2api.example", "user@example.com", "password")
        self.assertNotIn(UPSTREAM_SECRET, str(raised.exception))
        self.assertNotIn("private upstream detail", str(raised.exception))

        class FailedDownloadSession:
            def __init__(self, *args, **kwargs):
                pass

            def get(self, *args, **kwargs):
                raise RuntimeError(f"upstream body: {UPSTREAM_SECRET}")

            def close(self):
                pass

        with mock.patch.object(cpa_service_module, "Session", FailedDownloadSession):
            _token, error = cpa_service_module.fetch_remote_access_token(
                {"base_url": "https://cpa.example", "secret_key": "management-secret"},
                "account.json",
            )
        self.assertNotIn(UPSTREAM_SECRET, str(error))

    def test_password_relogin_failure_does_not_return_upstream_body(self) -> None:
        secret = "password-relogin-upstream-secret owner@example.com"

        class FailedResponse:
            status_code = 500
            url = f"https://auth.openai.com/error?payload={secret}"
            text = secret

        class FailedLoginSession:
            def __init__(self, *args, **kwargs):
                self.cookies = mock.Mock()

            def get(self, *args, **kwargs):
                return FailedResponse()

            def close(self):
                pass

        service = account_service_module.AccountService.__new__(account_service_module.AccountService)
        with (
            mock.patch("curl_cffi.requests.Session", FailedLoginSession),
            mock.patch.object(account_service_module.config, "get_proxy_settings", return_value=""),
        ):
            result = service._login_with_password("owner@example.com", "password")

        self.assertNotIn(secret, json.dumps(result, ensure_ascii=False, default=str))

    def test_image_task_diagnostic_log_does_not_include_upstream_body(self) -> None:
        class FailedBackend:
            @staticmethod
            def _query_backend_tasks(**kwargs):
                raise UpstreamHTTPError(
                    "/backend-api/tasks",
                    502,
                    {"access_token": UPSTREAM_SECRET, "message": "private upstream detail"},
                )

        with mock.patch.object(conversation_module.logger, "warning") as warning:
            conversation_module._get_detailed_error_from_tasks(FailedBackend(), "conversation-1", wait_secs=0)

        logged = json.dumps(warning.call_args_list, default=str)
        self.assertNotIn(UPSTREAM_SECRET, logged)
        self.assertNotIn("private upstream detail", logged)

    def test_structured_image_task_error_is_safe_in_output_and_log(self) -> None:
        class TaskBackend:
            @staticmethod
            def _query_backend_tasks(**kwargs):
                return [{"task": "error"}]

            @staticmethod
            def check_task_error(task):
                return True, UPSTREAM_SECRET, {"message": UPSTREAM_SECRET}

        with mock.patch.object(conversation_module.logger, "info") as info:
            result = conversation_module._get_detailed_error_from_tasks(
                TaskBackend(), "conversation-1", wait_secs=0,
            )

        logged = json.dumps(info.call_args_list, ensure_ascii=False, default=str)
        self.assertEqual(result, "Image generation was rejected by upstream policy.")
        self.assertNotIn(UPSTREAM_SECRET, result)
        self.assertNotIn(UPSTREAM_SECRET, logged)
        self.assertNotIn("error_msg", logged)

    def test_image_text_reply_logs_only_structured_metadata(self) -> None:
        secret = "opaque-image-text-secret owner@example.test"
        message = f'{secret} {{"referenced_image_ids":["image-1"]}}'

        class TextReplyBackend:
            @staticmethod
            def resolve_conversation_image_urls(*args, **kwargs):
                return []

            @staticmethod
            def _poll_image_results(*args, **kwargs):
                return [], []

        request = conversation_module.ConversationRequest(
            model="gpt-image-2",
            prompt="draw an apple",
        )
        event = {
            "type": "conversation.done",
            "conversation_id": "conversation-1",
            "file_ids": [],
            "sediment_ids": [],
            "text": message,
            "turn_use_case": "image gen",
        }

        with (
            mock.patch.object(conversation_module, "conversation_events", return_value=iter([event])),
            mock.patch.object(conversation_module, "_get_detailed_error_from_tasks", return_value=""),
            mock.patch.object(conversation_module.logger, "info") as info,
            mock.patch.object(conversation_module.logger, "warning") as warning,
        ):
            outputs = list(conversation_module.stream_image_outputs(TextReplyBackend(), request))

        self.assertEqual(outputs[-1].kind, "message")
        self.assertEqual(outputs[-1].text, message)
        logged = json.dumps(
            [*info.call_args_list, *warning.call_args_list],
            ensure_ascii=False,
            default=str,
        )
        self.assertNotIn(secret, logged)
        self.assertNotIn("message_preview", logged)
        self.assertIn(f'"message_len": {len(message)}', logged)

    def test_image_stream_resolve_log_does_not_stringify_upstream_metadata(self) -> None:
        canary = "image-resolve-upstream-metadata-canary owner@example.test"
        request = conversation_module.ConversationRequest(
            model="gpt-image-2",
            prompt="draw an apple",
        )
        event = {
            "type": "conversation.done",
            "conversation_id": {"secret": canary},
            "file_ids": [],
            "sediment_ids": [],
            "text": "safe upstream message",
            "blocked": True,
            "tool_invoked": {"secret": canary},
            "turn_use_case": [canary],
        }

        with (
            mock.patch.object(conversation_module, "conversation_events", return_value=iter([event])),
            mock.patch.object(conversation_module, "_get_detailed_error_from_tasks", return_value=""),
            mock.patch.object(conversation_module.logger, "info") as info,
        ):
            outputs = list(conversation_module.stream_image_outputs(object(), request))

        self.assertEqual(outputs[-1].kind, "message")
        self.assertNotIn(canary, repr(outputs))
        logged = json.dumps(info.call_args_list, ensure_ascii=False, default=str)
        self.assertNotIn(canary, logged)

    def test_refresh_token_failure_does_not_include_upstream_body(self) -> None:
        class FailedResponse:
            status_code = 401
            text = json.dumps({"error": "invalid_grant", "error_description": UPSTREAM_SECRET})

            @staticmethod
            def json():
                return {"error": "invalid_grant", "error_description": UPSTREAM_SECRET}

        class FailedRefreshSession:
            def __init__(self, *args, **kwargs):
                pass

            def post(self, *args, **kwargs):
                return FailedResponse()

            def close(self):
                pass

        service = account_service_module.AccountService.__new__(account_service_module.AccountService)
        with mock.patch("curl_cffi.requests.Session", FailedRefreshSession):
            with self.assertRaises(RuntimeError) as raised:
                service._request_access_token_refresh("refresh-secret")

        self.assertNotIn(UPSTREAM_SECRET, str(raised.exception))

    def test_password_relogin_failure_does_not_persist_upstream_detail(self) -> None:
        secret = "password-login-upstream-secret"
        service = account_service_module.AccountService.__new__(account_service_module.AccountService)
        failure = {
            "ok": False,
            "error": "password_verify_failed_403",
            "detail": {
                "error": {"code": "invalid_credentials", "message": secret},
                "access_token": secret,
            },
        }
        with (
            mock.patch.object(service, "_login_with_password", return_value=failure),
            mock.patch.object(service, "remove_invalid_token"),
            mock.patch.object(account_service_module.log_service, "add") as add,
        ):
            service._password_re_login_thread("old-access-token", "owner@example.com", "password", "test")

        logged = json.dumps(add.call_args_list, ensure_ascii=False, default=str)
        self.assertNotIn(secret, logged)
        self.assertNotIn("invalid_credentials", logged)

    def test_relogin_progress_projects_untrusted_error_code(self) -> None:
        secret = "relogin-progress-upstream-secret owner@example.com"
        service = account_service_module.AccountService.__new__(account_service_module.AccountService)
        progress_id = "relogin-progress-public-contract"
        service.init_relogin_progress(progress_id, 1)
        service._login_with_password = mock.Mock(
            return_value={"ok": False, "error": secret, "detail": {"message": secret}}
        )
        service.remove_invalid_token = mock.Mock()
        try:
            with mock.patch.object(account_service_module.log_service, "add") as add:
                service._password_re_login_thread(
                    "old-access-token",
                    "owner@example.com",
                    "password",
                    "manual_relogin",
                    progress_id,
                )

                progress = service.get_relogin_progress(progress_id)
                self.assertIsNotNone(progress)
                self.assertEqual(progress["results"][0]["error"], "relogin_failed")
                self.assertNotIn(secret, json.dumps(progress, ensure_ascii=False, default=str))
                self.assertNotIn(secret, json.dumps(add.call_args_list, ensure_ascii=False, default=str))

                app = FastAPI()
                app.include_router(accounts_module.create_router())
                with (
                    mock.patch.object(accounts_module, "account_service", service),
                    mock.patch.object(accounts_module, "require_admin_async", return_value={"role": "admin"}),
                ):
                    response = TestClient(app).get(
                        f"/api/accounts/re-login/progress/{progress_id}",
                        headers=AUTH_HEADERS,
                    )
                self.assertEqual(response.status_code, 200, response.text)
                self.assertNotIn(secret, response.text)
                self.assertEqual(response.json()["results"][0]["error"], "relogin_failed")
        finally:
            service.clean_relogin_progress(progress_id)

    def test_refresh_failure_does_not_return_upstream_body(self) -> None:
        secret = "refresh-upstream-secret"
        service = account_service_module.AccountService.__new__(account_service_module.AccountService)
        service._lock = threading.RLock()
        service._accounts = {"access-token": {"access_token": "access-token", "status": "正常"}}
        service._token_aliases = {}
        service._image_inflight = {}
        with (
            mock.patch.object(service, "fetch_remote_info", side_effect=RuntimeError(secret)),
            mock.patch.dict(account_service_module.config.data, {"auto_relogin_after_refresh": False}),
        ):
            result = service.refresh_accounts(["access-token"])

        self.assertNotIn(secret, json.dumps(result, ensure_ascii=False, default=str))
        self.assertEqual(result["errors"][0]["error"], "RuntimeError")

    def test_content_filter_does_not_log_upstream_body(self) -> None:
        secret = "review-upstream-secret"

        class FailedResponse:
            status_code = 502
            text = secret

            @staticmethod
            def json():
                raise ValueError(secret)

        review_config = {
            "enabled": True,
            "base_url": "https://review.example",
            "api_key": "review-key",
            "model": "review-model",
            "fail_open": True,
        }
        with (
            mock.patch.object(content_filter_module, "config", mock.Mock(sensitive_words=[], ai_review=review_config)),
            mock.patch.object(content_filter_module.requests, "post", return_value=FailedResponse()),
            mock.patch.object(content_filter_module.logger, "warning") as warning,
        ):
            content_filter_module.check_request("hello")

        logged = json.dumps(warning.call_args_list, ensure_ascii=False, default=str)
        self.assertNotIn(secret, logged)
        self.assertNotIn("body_preview", logged)

    def test_content_filter_preview_bounds_client_controlled_nested_json(self) -> None:
        value: object = "leaf"
        for _ in range(1100):
            value = {"content": value}

        preview = content_filter_module.request_text(value)
        shape = content_filter_module.request_shape(value)

        self.assertIn("structured input truncated", preview)
        self.assertEqual(shape, {})
        self.assertLessEqual(
            len(preview),
            content_filter_module._MAX_REVIEW_TEXT_LEN,
        )

        large_preview = content_filter_module.request_text(["x" * 10_000] * 20)
        self.assertLessEqual(len(large_preview), content_filter_module._MAX_REVIEW_TEXT_LEN)

    def test_responses_route_accepts_deep_input_without_preview_recursion_error(self) -> None:
        value: object = "leaf"
        for _ in range(1100):
            value = {"content": value}
        call = mock.Mock()
        call.run = mock.AsyncMock(return_value={"id": "response-test"})

        with (
            mock.patch.object(
                ai_module,
                "require_identity_async",
                new=mock.AsyncMock(return_value={"id": "identity-test"}),
            ),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module, "LoggedCall", return_value=call),
        ):
            response = TestClient(_app_with_ai_router()).post(
                "/v1/responses",
                headers=AUTH_HEADERS,
                json={"model": "auto", "input": value},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"id": "response-test"})
        call.run.assert_awaited_once()

    def test_content_filter_rejects_container_review_settings_without_stringifying_them(self) -> None:
        secret = "malformed-review-setting-canary owner@example.com"
        review_config = {
            "enabled": True,
            "base_url": {"secret": secret},
            "api_key": [secret],
            "model": {"name": secret},
            "prompt": [secret],
            "fail_open": {"value": secret},
        }
        with (
            mock.patch.object(
                content_filter_module,
                "config",
                mock.Mock(sensitive_words=[], ai_review=review_config),
            ),
            mock.patch.object(content_filter_module.requests, "post") as post,
        ):
            with self.assertRaises(HTTPException) as raised:
                content_filter_module.check_request("hello")

        self.assertEqual(raised.exception.status_code, 400)
        self.assertNotIn(secret, json.dumps(raised.exception.detail, ensure_ascii=False, default=str))
        post.assert_not_called()

    def test_content_filter_does_not_log_ambiguous_upstream_decision(self) -> None:
        secret = "ambiguous-review-secret owner@example.com"

        class AmbiguousResponse:
            status_code = 200

            @staticmethod
            def json():
                return {"choices": [{"message": {"content": secret}}]}

        review_config = {
            "enabled": True,
            "base_url": "https://review.example",
            "api_key": "review-key",
            "model": "review-model",
            "fail_open": True,
        }
        with (
            mock.patch.object(content_filter_module, "config", mock.Mock(sensitive_words=[], ai_review=review_config)),
            mock.patch.object(content_filter_module.requests, "post", return_value=AmbiguousResponse()),
            mock.patch.object(content_filter_module.logger, "warning") as warning,
        ):
            content_filter_module.check_request("hello")

        logged = json.dumps(warning.call_args_list, ensure_ascii=False, default=str)
        self.assertNotIn(secret, logged)
        self.assertNotIn("owner@example.com", logged)

    def test_unknown_exception_is_fail_closed_but_explicit_safe_error_survives(self) -> None:
        secret = "opaque-token-7f3d owner@example.com upstream fragment"
        self.assertEqual(public_exception_message(RuntimeError(secret), "image task failed"), "image task failed")

        class MaliciousRuntimeError(RuntimeError):
            public_safe_message = secret

        self.assertEqual(
            public_exception_message(MaliciousRuntimeError("ignored"), "image task failed"),
            "image task failed",
        )
        self.assertEqual(
            public_exception_message(PublicSafeError("图片结果不存在，请稍后重试"), "image task failed"),
            "图片结果不存在，请稍后重试",
        )

    def test_timeout_message_only_comes_from_controlled_constructor(self) -> None:
        fallback = "image task failed"
        untrusted = ImagePollTimeoutError(conversation_id="conversation-1")
        self.assertEqual(public_exception_message(untrusted, fallback), fallback)

        controlled = ImagePollTimeoutError.from_timeout(30, "conversation-1")
        self.assertIn("ChatGPT 生图超时", public_exception_message(controlled, fallback))

    def test_image_diagnostics_never_log_token_or_account_email(self) -> None:
        access_token = "image-access-token-secret-123456"
        token_prefix = access_token[:12]
        account_email = "owner@example.com"

        class FakeBackend:
            def __init__(self, *, access_token):
                self.access_token = access_token

            def close(self):
                pass

        error = conversation_module.ImageGenerationError(
            f"policy denied: {access_token} {account_email}",
            account_email=account_email,
        )
        payload = error.to_openai_error()
        self.assertNotIn(access_token, json.dumps(payload, ensure_ascii=False))
        self.assertNotIn(token_prefix, json.dumps(payload, ensure_ascii=False))
        self.assertNotIn(account_email, json.dumps(payload, ensure_ascii=False))

        with (
            mock.patch.object(conversation_module.account_service, "get_available_access_token", return_value=access_token),
            mock.patch.object(conversation_module.account_service, "get_account", return_value={"email": account_email}),
            mock.patch.object(conversation_module.account_service, "mark_image_result"),
            mock.patch.object(conversation_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(conversation_module.time, "sleep") as sleep,
            mock.patch.object(
                conversation_module,
                "stream_image_outputs",
                side_effect=conversation_module.ImageContentPolicyError("policy denied"),
            ) as stream,
            mock.patch.object(conversation_module.logger, "debug") as debug,
            mock.patch.object(conversation_module.logger, "warning") as warning,
        ):
            with self.assertRaises(conversation_module.ImageGenerationError):
                list(
                    conversation_module._generate_single_image(
                        conversation_module.ConversationRequest(model="gpt-image-2", prompt="cat"),
                        1,
                        1,
                    )
                )

        stream.assert_called_once()
        sleep.assert_not_called()
        logged = json.dumps(
            [*debug.call_args_list, *warning.call_args_list],
            ensure_ascii=False,
            default=str,
        )
        self.assertNotIn(access_token, logged)
        self.assertNotIn(token_prefix, logged)
        self.assertNotIn(account_email, logged)

        def handler() -> None:
            raise error

        call = LoggedCall(
            {"id": "key", "name": "key", "role": "user"},
            "/v1/images/generations",
            "gpt-image-2",
            "image",
        )
        with mock.patch("services.log_service.log_service.add") as add:
            response = asyncio.run(call.run(handler))

        self.assertEqual(response.status_code, 502)
        persisted = json.dumps(add.call_args_list, ensure_ascii=False, default=str)
        self.assertNotIn(account_email, persisted)
        self.assertNotIn('"account_email"', persisted)
        self.assertNotIn("account_email_hash", persisted)


if __name__ == "__main__":
    unittest.main()
