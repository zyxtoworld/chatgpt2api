from __future__ import annotations

import asyncio
import json
import threading
import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
import api.accounts as accounts_module
import services.account_service as account_service_module
import services.cpa_service as cpa_service_module
import services.content_filter as content_filter_module
import services.protocol.conversation as conversation_module
import services.sub2api_service as sub2api_service_module
from api.errors import install_exception_handlers
from services.log_service import LoggedCall
from services.openai_backend_api import ImagePollTimeoutError
from services.protocol.error_response import PublicSafeError, public_exception_message
from utils.helper import UpstreamHTTPError, anthropic_sse_stream, responses_sse_stream, sse_json_stream


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
UPSTREAM_SECRET = "upstream-access-token-secret"


def _app_with_ai_router() -> FastAPI:
    app = FastAPI()
    install_exception_handlers(app)
    app.include_router(ai_module.create_router())
    return app


class PublicErrorContractTests(unittest.TestCase):
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
