from __future__ import annotations

import json
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

from fastapi import HTTPException

import services.openai_backend_api as backend_module
from services.openai_backend_api import CODEX_RESPONSES_MODEL, OpenAIBackendAPI
from services.account_service import AccountService
from services.protocol import openai_v1_chat_complete, openai_v1_response
from services.storage.json_storage import JSONStorageBackend


FUNCTION_TOOL = {
    "type": "function",
    "name": "get_weather",
    "description": "Get the weather",
    "parameters": {
        "type": "object",
        "properties": {"city": {"type": "string"}},
        "required": ["city"],
        "additionalProperties": False,
    },
    "strict": True,
}


def function_events() -> list[dict[str, object]]:
    call = {
        "id": "fc_1",
        "type": "function_call",
        "status": "completed",
        "call_id": "call_1",
        "name": "get_weather",
        "arguments": '{"city":"Shanghai"}',
    }
    return [
        {"type": "response.created", "response": {"id": "resp_1", "status": "in_progress"}},
        {"type": "response.output_item.added", "output_index": 0, "item": {**call, "status": "in_progress"}},
        {
            "type": "response.function_call_arguments.delta",
            "item_id": "fc_1",
            "output_index": 0,
            "delta": '{"city":',
        },
        {
            "type": "response.function_call_arguments.done",
            "item_id": "fc_1",
            "output_index": 0,
            "arguments": '{"city":"Shanghai"}',
        },
        {"type": "response.output_item.done", "output_index": 0, "item": call},
        {
            "type": "response.completed",
            "response": {
                "id": "resp_1",
                "object": "response",
                "status": "completed",
                "model": CODEX_RESPONSES_MODEL,
                "output": [call],
                "usage": {"input_tokens": 12, "output_tokens": 7, "total_tokens": 19},
            },
        },
    ]


def web_search_events() -> list[dict[str, object]]:
    search_call = {
        "id": "ws_1",
        "type": "web_search_call",
        "status": "completed",
        "action": {"type": "search", "query": "latest news"},
    }
    message = {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "status": "completed",
        "content": [{
            "type": "output_text",
            "text": "Native search answer.",
            "annotations": [{
                "type": "url_citation",
                "url": "https://example.com/native",
                "title": "Example",
                "start_index": 0,
                "end_index": 6,
            }],
        }],
    }
    completed = {
        "id": "resp_search",
        "object": "response",
        "created_at": 123,
        "model": "gpt-5.5",
        "status": "completed",
        "output": [search_call, message],
        "usage": {"input_tokens": 4, "output_tokens": 3, "total_tokens": 7},
    }
    return [
        {"type": "response.created", "response": {"id": "resp_search", "model": "gpt-5.5"}},
        {"type": "response.output_item.done", "output_index": 0, "item": search_call},
        {"type": "response.output_text.delta", "item_id": "msg_1", "delta": "Native search answer."},
        {"type": "response.output_item.done", "output_index": 1, "item": message},
        {"type": "response.completed", "response": completed},
    ]


def incomplete_events() -> list[dict[str, object]]:
    message = {
        "id": "msg_partial",
        "type": "message",
        "role": "assistant",
        "status": "incomplete",
        "content": [{"type": "output_text", "text": "partial", "annotations": []}],
    }
    response = {
        "id": "resp_incomplete",
        "object": "response",
        "created_at": 123,
        "model": "gpt-5.5",
        "status": "incomplete",
        "incomplete_details": {"reason": "max_output_tokens"},
        "output": [message],
        "usage": {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3},
    }
    return [
        {"type": "response.created", "response": {"id": "resp_incomplete", "model": "gpt-5.5"}},
        {"type": "response.output_text.delta", "item_id": "msg_partial", "delta": "partial"},
        {"type": "response.output_item.done", "output_index": 0, "item": message},
        {"type": "response.incomplete", "response": response},
    ]


class _FakeCodexBackend:
    instances: list["_FakeCodexBackend"] = []

    def __init__(self, access_token: str) -> None:
        self.access_token = access_token
        self.payload: dict[str, object] | None = None
        self.closed = False
        self.instances.append(self)

    def iter_codex_response_events(self, payload: dict[str, object], *, timeout: float):
        del timeout
        self.payload = payload
        yield from function_events()

    def close(self) -> None:
        self.closed = True


class _IncrementalSSE:
    status = 200
    headers = {"content-type": "text/event-stream"}

    def __init__(self) -> None:
        self._lines = iter(
            [
                b'data: {"type":"response.created","response":{"id":"resp_1"}}\n',
                b"\n",
                b'data: {"type":"response.completed","response":{"id":"resp_1"}}\n',
                b"\n",
            ]
        )
        self.next_calls = 0
        self.allow_rest = False

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        self.next_calls += 1
        if self.next_calls > 2 and not self.allow_rest:
            raise AssertionError("reader consumed the whole SSE stream before yielding the first event")
        return next(self._lines)

    def read(self) -> bytes:
        raise AssertionError("SSE responses must not be buffered with read()")


class CodexToolCallContractTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeCodexBackend.instances.clear()

    def test_plain_chat_with_codex_account_uses_native_responses(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "hello"}],
        }
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_plain_codex",
                "object": "response",
                "status": "completed",
                "model": CODEX_RESPONSES_MODEL,
                "output": [{
                    "id": "msg_plain_codex",
                    "type": "message",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "native",
                        "annotations": [],
                    }],
                }],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }

        class CodexAccountService:
            def get_text_access_token(self, **_kwargs: object) -> str:
                return "codex-token"

            def get_account(self, _token: str) -> dict[str, str]:
                return {"source_type": "codex"}

        captured: dict[str, object] = {}

        def native_events(_body: dict[str, object], **kwargs: object):
            captured.update(kwargs)
            yield completed

        with (
            mock.patch.object(openai_v1_chat_complete, "account_service", CodexAccountService()),
            mock.patch.object(
                openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("Codex plain chat must not use web conversation"),
            ),
        ):
            response = openai_v1_chat_complete.handle(body, authenticated=True)

        self.assertEqual(response["choices"][0]["message"]["content"], "native")
        self.assertEqual(captured, {"access_token": "codex-token"})

    def test_plain_stream_chat_with_codex_account_uses_native_responses(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
        }
        created = {
            "type": "response.created",
            "response": {"id": "resp_plain_codex", "model": CODEX_RESPONSES_MODEL},
        }
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_plain_codex",
                "object": "response",
                "status": "completed",
                "model": CODEX_RESPONSES_MODEL,
                "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }

        class CodexAccountService:
            def get_text_access_token(self, **_kwargs: object) -> str:
                return "codex-token"

            def get_account(self, _token: str) -> dict[str, str]:
                return {"source_type": "codex"}

        captured: dict[str, object] = {}

        def native_events(_body: dict[str, object], **kwargs: object):
            captured.update(kwargs)
            yield created
            yield completed

        with (
            mock.patch.object(openai_v1_chat_complete, "account_service", CodexAccountService()),
            mock.patch.object(
                openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("Codex plain chat must not use web conversation"),
            ),
        ):
            chunks = list(openai_v1_chat_complete.handle(body, authenticated=True))

        self.assertTrue(chunks)
        self.assertEqual(captured, {"access_token": "codex-token"})

    def test_native_codex_text_failover_reaches_third_route_candidate(self) -> None:
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_failover",
                "object": "response",
                "status": "completed",
                "model": CODEX_RESPONSES_MODEL,
                "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }
        selector_calls: list[dict[str, object]] = []

        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                excluded = set(kwargs.get("excluded_tokens") or set())
                for token in ("codex-1", "codex-2", "codex-3"):
                    if token not in excluded:
                        return token
                raise AssertionError("selector returned an exhausted candidate set")

            def mark_text_used(self, _token: str) -> None:
                pass

        class FailoverBackend:
            instances: list["FailoverBackend"] = []

            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.timeout = 0.0
                self.closed = False
                self.instances.append(self)

            def iter_codex_response_events(self, _payload: dict[str, object], *, timeout: float):
                self.timeout = timeout
                if self.access_token != "codex-3":
                    raise openai_v1_response.UpstreamHTTPError(
                        "codex",
                        502,
                        {"error": "transient"},
                    )
                yield completed

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", FailoverBackend),
            mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
        ):
            events = list(
                openai_v1_response.stream_codex_response(
                    {"model": "auto", "input": "hello"},
                )
            )

        self.assertEqual([event["type"] for event in events], ["response.completed"])
        deadline = selector_calls[0]["deadline"]
        self.assertIsInstance(deadline, float)
        self.assertTrue(all(call["deadline"] == deadline for call in selector_calls))
        for call in selector_calls:
            call.pop("deadline")
        self.assertEqual(
            selector_calls,
            [
                {"model": "auto", "source_type": "codex"},
                {"model": "auto", "source_type": "codex", "excluded_tokens": {"codex-1"}},
                {
                    "model": "auto",
                    "source_type": "codex",
                    "excluded_tokens": {"codex-1", "codex-2"},
                },
            ],
        )
        self.assertEqual([backend.access_token for backend in FailoverBackend.instances], [
            "codex-1",
            "codex-2",
            "codex-3",
        ])
        self.assertTrue(all(backend.timeout > 0 for backend in FailoverBackend.instances))
        self.assertTrue(all(
            earlier.timeout >= later.timeout
            for earlier, later in zip(FailoverBackend.instances, FailoverBackend.instances[1:])
        ))
        self.assertTrue(all(
            backend.timeout <= openai_v1_response._CODEX_TEXT_FAILOVER_DEADLINE_SECONDS
            for backend in FailoverBackend.instances
        ))
        self.assertTrue(all(backend.closed for backend in FailoverBackend.instances))

    def test_native_codex_text_does_not_inspect_event_reader_signature(self) -> None:
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_no_inspect",
                "object": "response",
                "status": "completed",
                "model": CODEX_RESPONSES_MODEL,
                "output": [],
            },
        }

        class CodexAccountService:
            def get_text_access_token(self, **_kwargs: object) -> str:
                return "codex-1"

            def mark_text_used(self, _token: str) -> None:
                pass

        class Backend:
            def __init__(self, access_token: str) -> None:
                del access_token
                self.closed = False

            def iter_codex_response_events(
                self,
                _payload: dict[str, object],
                *,
                timeout: float,
            ):
                self.timeout = timeout
                yield completed

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", Backend),
            mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
            mock.patch("inspect.signature", side_effect=AssertionError("signature reflection is forbidden")),
        ):
            events = list(openai_v1_response.stream_codex_response({"model": "auto", "input": "hello"}))

        self.assertEqual([event["type"] for event in events], ["response.completed"])

    def test_native_codex_text_deadline_preserves_last_retryable_error(self) -> None:
        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                return "codex-1" if not kwargs.get("excluded_tokens") else "codex-2"

        class Backend:
            instances: list["Backend"] = []

            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.closed = False
                self.instances.append(self)

            def iter_codex_response_events(
                self,
                _payload: dict[str, object],
                *,
                timeout: float,
            ):
                self.timeout = timeout
                raise openai_v1_response.UpstreamHTTPError(
                    "codex",
                    502,
                    {"error": "transient"},
                )
                yield  # pragma: no cover

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", Backend),
            mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
            mock.patch.object(openai_v1_response.time, "monotonic", side_effect=[100.0, 100.1, 100.2, 100.3, 131.0]),
            self.assertRaises(openai_v1_response.UpstreamHTTPError) as raised,
        ):
            list(openai_v1_response.stream_codex_response({"model": "auto", "input": "hello"}))

        self.assertEqual(raised.exception.status_code, 502)
        self.assertEqual([backend.access_token for backend in Backend.instances], ["codex-1"])
        self.assertTrue(all(backend.closed for backend in Backend.instances))

    def test_native_codex_text_selector_timeout_preserves_last_retryable_error(self) -> None:
        selector_deadlines: list[float] = []

        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                deadline = kwargs.get("deadline")
                self.assert_deadline(deadline)
                selector_deadlines.append(deadline)
                if len(selector_deadlines) == 1:
                    return "codex-1"
                raise TimeoutError("selector deadline exceeded")

            @staticmethod
            def assert_deadline(value: object) -> None:
                if not isinstance(value, float):
                    raise AssertionError("selector did not receive the absolute deadline")

        class Backend:
            def __init__(self, access_token: str) -> None:
                del access_token
                self.closed = False

            def iter_codex_response_events(
                self,
                _payload: dict[str, object],
                *,
                timeout: float,
            ):
                self.timeout = timeout
                raise openai_v1_response.UpstreamHTTPError(
                    "codex",
                    502,
                    {"error": "transient"},
                )
                yield  # pragma: no cover

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", Backend),
            mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
            mock.patch.object(openai_v1_response.time, "monotonic", side_effect=[0.0, 0.1, 0.2, 0.3, 0.4]),
            self.assertRaises(openai_v1_response.UpstreamHTTPError) as raised,
        ):
            list(openai_v1_response.stream_codex_response({"model": "auto", "input": "hello"}))

        self.assertEqual(raised.exception.status_code, 502)
        self.assertEqual(len(selector_deadlines), 2)
        self.assertEqual(selector_deadlines[0], selector_deadlines[1])

    def test_native_codex_text_does_not_failover_after_created_event(self) -> None:
        selector_calls: list[dict[str, object]] = []

        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                return "codex-1" if not kwargs.get("excluded_tokens") else "codex-2"

        class CreatedThenErrorBackend:
            instances: list["CreatedThenErrorBackend"] = []

            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.closed = False
                self.instances.append(self)

            def iter_codex_response_events(self, _payload: dict[str, object], *, timeout: float):
                del timeout
                yield {
                    "type": "response.created",
                    "response": {"id": "resp_committed", "status": "in_progress"},
                }
                raise openai_v1_response.UpstreamHTTPError("codex", 502, {"error": "late"})

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", CreatedThenErrorBackend),
            mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
            self.assertRaises(openai_v1_response.UpstreamHTTPError),
        ):
            list(openai_v1_response.stream_codex_response({"model": "auto", "input": "hello"}))

        self.assertEqual(len(selector_calls), 1)
        self.assertEqual(selector_calls[0]["model"], "auto")
        self.assertEqual(selector_calls[0]["source_type"], "codex")
        self.assertIsInstance(selector_calls[0]["deadline"], float)
        self.assertEqual(len(CreatedThenErrorBackend.instances), 1)
        self.assertTrue(CreatedThenErrorBackend.instances[0].closed)

    def test_native_codex_text_exhaustion_preserves_last_upstream_status(self) -> None:
        selector_calls: list[dict[str, object]] = []

        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                excluded = set(kwargs.get("excluded_tokens") or set())
                for token in ("codex-1", "codex-2", "codex-3"):
                    if token not in excluded:
                        return token
                raise openai_v1_response.ModelUnavailableError("exhausted")

        class ExhaustedBackend:
            instances: list["ExhaustedBackend"] = []

            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.closed = False
                self.instances.append(self)

            def iter_codex_response_events(self, _payload: dict[str, object], *, timeout: float):
                del timeout
                raise openai_v1_response.UpstreamHTTPError(
                    "codex",
                    429,
                    {"error": "rate_limited"},
                )
                yield  # pragma: no cover

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", ExhaustedBackend),
            mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
            self.assertRaises(openai_v1_response.UpstreamHTTPError) as raised,
        ):
            list(openai_v1_response.stream_codex_response({"model": "auto", "input": "hello"}))

        self.assertEqual(raised.exception.status_code, 429)
        self.assertEqual(len(ExhaustedBackend.instances), 3)
        self.assertTrue(all(backend.closed for backend in ExhaustedBackend.instances))
        self.assertEqual(len(selector_calls), 4)

    def test_native_codex_text_failover_rebuilds_payload_for_each_candidate(self) -> None:
        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                return "codex-1" if not kwargs.get("excluded_tokens") else "codex-2"

            def mark_text_used(self, _token: str) -> None:
                pass

        class PayloadBackend:
            payloads: dict[str, dict[str, object]] = {}

            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.closed = False

            def iter_codex_response_events(self, payload: dict[str, object], *, timeout: float):
                del timeout
                self.payloads[self.access_token] = payload
                if self.access_token == "codex-1":
                    raise openai_v1_response.UpstreamHTTPError("codex", 502, {"error": "transient"})
                yield {
                    "type": "response.completed",
                    "response": {
                        "id": "resp_payload",
                        "object": "response",
                        "status": "completed",
                        "model": CODEX_RESPONSES_MODEL,
                        "output": [],
                        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
                    },
                }

            def close(self) -> None:
                self.closed = True

        def mutate_only_first(payload: dict[str, object], *, access_token: str) -> None:
            if access_token == "codex-1":
                payload["reasoning"]["effort"] = "minimal"  # type: ignore[index]

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", PayloadBackend),
            mock.patch.object(
                openai_v1_response,
                "resolve_codex_reasoning_effort",
                side_effect=mutate_only_first,
            ),
        ):
            events = list(
                openai_v1_response.stream_codex_response(
                    {
                        "model": "auto",
                        "input": "hello",
                        "reasoning": {"effort": "high"},
                    }
                )
            )

        self.assertEqual([event["type"] for event in events], ["response.completed"])
        self.assertEqual(PayloadBackend.payloads["codex-1"]["reasoning"], {"effort": "minimal"})
        self.assertEqual(PayloadBackend.payloads["codex-2"]["reasoning"], {"effort": "high"})

    def test_native_codex_text_does_not_failover_after_delta(self) -> None:
        selector_calls: list[dict[str, object]] = []

        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                return "codex-1"

        class PartialBackend:
            instances: list["PartialBackend"] = []

            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.closed = False
                self.instances.append(self)

            def iter_codex_response_events(self, _payload: dict[str, object], *, timeout: float):
                del timeout
                yield {"type": "response.output_text.delta", "item_id": "msg_1", "delta": "part"}
                raise openai_v1_response.UpstreamHTTPError("codex", 502, {"error": "late"})

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", PartialBackend),
            mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
            self.assertRaises(openai_v1_response.UpstreamHTTPError),
        ):
            list(openai_v1_response.stream_codex_response({"model": "auto", "input": "hello"}))

        self.assertEqual(len(selector_calls), 1)
        self.assertEqual(selector_calls[0]["model"], "auto")
        self.assertEqual(selector_calls[0]["source_type"], "codex")
        self.assertIsInstance(selector_calls[0]["deadline"], float)
        self.assertEqual(len(PartialBackend.instances), 1)
        self.assertTrue(PartialBackend.instances[0].closed)

    def test_native_codex_text_does_not_failover_client_errors(self) -> None:
        selector_calls: list[dict[str, object]] = []

        class CodexAccountService:
            def get_text_access_token(self, **kwargs: object) -> str:
                selector_calls.append(kwargs)
                return "codex-1"

        class ClientErrorBackend:
            instances: list["ClientErrorBackend"] = []

            def __init__(self, access_token: str) -> None:
                self.access_token = access_token
                self.closed = False
                self.instances.append(self)

            def iter_codex_response_events(self, _payload: dict[str, object], *, timeout: float):
                del timeout
                raise openai_v1_response.UpstreamHTTPError("codex", 400, {"error": "invalid"})
                yield  # pragma: no cover

            def close(self) -> None:
                self.closed = True

        with (
            mock.patch.object(openai_v1_response, "account_service", CodexAccountService()),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", ClientErrorBackend),
            mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
            self.assertRaises(openai_v1_response.UpstreamHTTPError),
        ):
            list(openai_v1_response.stream_codex_response({"model": "auto", "input": "hello"}))

        self.assertEqual(len(selector_calls), 1)
        self.assertEqual(selector_calls[0]["model"], "auto")
        self.assertEqual(selector_calls[0]["source_type"], "codex")
        self.assertIsInstance(selector_calls[0]["deadline"], float)
        self.assertEqual(len(ClientErrorBackend.instances), 1)
        self.assertTrue(ClientErrorBackend.instances[0].closed)

    def test_account_text_selector_forwards_absolute_deadline_to_refresh(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            service.add_account_items([{
                "access_token": "codex-deadline",
                "source_type": "codex",
                "type": "Pro",
                "status": "正常",
            }])
            with mock.patch.object(
                service,
                "refresh_access_token",
                return_value="codex-deadline",
            ) as refresh:
                selected = service.get_text_access_token(
                    source_type="codex",
                    deadline=123.5,
                )

        self.assertEqual(selected, "codex-deadline")
        self.assertEqual(refresh.call_args.kwargs["deadline"], 123.5)

    def test_late_responses_usage_does_not_mutate_replaced_same_token_account(self) -> None:
        entered = threading.Event()
        release = threading.Event()

        class BlockingBackend:
            def iter_codex_response_events(self, _payload: dict[str, object], *, timeout: float):
                del timeout
                yield {"type": "response.created", "response": {"id": "resp_1", "status": "in_progress"}}
                entered.set()
                if not release.wait(5):
                    raise AssertionError("responses stream did not receive release")
                yield {
                    "type": "response.completed",
                    "response": {
                        "id": "resp_1",
                        "object": "response",
                        "status": "completed",
                        "model": CODEX_RESPONSES_MODEL,
                        "output": [],
                        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
                    },
                }

            def close(self) -> None:
                pass

        with tempfile.TemporaryDirectory() as temp_dir:
            service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            service.add_account_items([{"access_token": "responses-token", "type": "Pro", "source_type": "codex", "status": "正常"}])
            service.get_text_access_token = lambda **_kwargs: "responses-token"
            with (
                mock.patch.object(openai_v1_response, "account_service", service),
                mock.patch.object(openai_v1_response, "OpenAIBackendAPI", return_value=BlockingBackend()),
                mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
            ):
                stream = openai_v1_response.stream_codex_response(
                    {"model": "auto", "input": "hello", "stream": True}
                )
                first = next(stream)
                self.assertEqual(first["type"], "response.created")
                remaining: list[dict[str, object]] = []
                stream_errors: list[BaseException] = []

                def consume_remaining() -> None:
                    try:
                        remaining.extend(stream)
                    except BaseException as exc:
                        stream_errors.append(exc)

                consumer = threading.Thread(target=consume_remaining)
                consumer.start()
                self.assertTrue(entered.wait(5))
                service.update_account(
                    "responses-token",
                    {"last_used_at": "2000-01-01 00:00:00", "success": 99},
                )
                release.set()
                consumer.join(5)
                self.assertFalse(consumer.is_alive())
                self.assertEqual(stream_errors, [])
                self.assertEqual([event["type"] for event in remaining], ["response.completed"])

            current = service.get_account("responses-token")
            self.assertIsNotNone(current)
            self.assertEqual(current["last_used_at"], "2000-01-01 00:00:00")
            self.assertEqual(current["success"], 99)

    def test_responses_lease_capture_failure_does_not_create_backend(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            service.add_account_items([{"access_token": "responses-token", "type": "Pro", "status": "正常"}])
            service.get_text_access_token = lambda **_kwargs: "responses-token"
            with (
                mock.patch.object(
                    service,
                    "_get_account_lease",
                    side_effect=RuntimeError("lease capture failed"),
                ),
                mock.patch.object(openai_v1_response, "account_service", service),
                mock.patch.object(openai_v1_response, "OpenAIBackendAPI") as backend,
                mock.patch.object(openai_v1_response, "resolve_codex_reasoning_effort"),
                self.assertRaisesRegex(RuntimeError, "lease capture failed"),
            ):
                list(openai_v1_response.stream_codex_response({"model": "auto", "input": "hello"}))

            backend.assert_not_called()

    def test_responses_function_tool_stream_uses_codex_account_and_native_events(self) -> None:
        body = {
            "model": "auto",
            "input": "What is the weather?",
            "tools": [FUNCTION_TOOL],
            "tool_choice": "auto",
            "parallel_tool_calls": True,
            "reasoning": {"effort": "minimal"},
            "stream": True,
        }
        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ) as selector,
            mock.patch.object(openai_v1_response.account_service, "mark_text_used") as mark_used,
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", _FakeCodexBackend),
            mock.patch(
                "services.model_service.model_catalog_service.normalize_reasoning_effort",
                return_value="minimal",
            ),
        ):
            events = list(openai_v1_response.handle(body))

        self.assertEqual(events, function_events())
        selector.assert_called_once_with(model="auto", source_type="codex", deadline=mock.ANY)
        mark_used.assert_called_once_with("codex-token")
        self.assertEqual(len(_FakeCodexBackend.instances), 1)
        backend = _FakeCodexBackend.instances[0]
        self.assertTrue(backend.closed)
        payload = backend.payload or {}
        self.assertEqual(payload["model"], CODEX_RESPONSES_MODEL)
        self.assertEqual(payload["tools"], [FUNCTION_TOOL])
        self.assertEqual(payload["tool_choice"], "auto")
        self.assertEqual(payload["input"][0]["type"], "message")
        self.assertEqual(payload["input"][0]["content"][0]["text"], "What is the weather?")
        self.assertEqual(payload["reasoning"], {"effort": "minimal"})
        self.assertIs(payload["stream"], True)
        self.assertIs(payload["store"], False)
        self.assertIs(payload["parallel_tool_calls"], True)
        self.assertEqual(payload["include"], ["reasoning.encrypted_content"])

    def test_native_http_stops_at_first_terminal_and_rejects_missing_terminal(self) -> None:
        class EventBackend(_FakeCodexBackend):
            emitted_events: list[dict[str, object]] = []

            def iter_codex_response_events(self, payload: dict[str, object], *, timeout: float):
                del timeout
                self.payload = payload
                yield from self.emitted_events

        body = {
            "model": "auto",
            "input": "What is the weather?",
            "tools": [FUNCTION_TOOL],
            "stream": True,
        }
        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used") as mark_used,
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", EventBackend),
            mock.patch(
                "services.model_service.model_catalog_service.normalize_reasoning_effort",
                return_value="minimal",
            ),
        ):
            EventBackend.emitted_events = [
                *function_events(),
                {"type": "response.output_text.delta", "delta": "must-not-be-forwarded"},
            ]
            events = list(openai_v1_response.handle(body))
            self.assertEqual(events, function_events())
            mark_used.assert_called_once_with("codex-token")

            EventBackend.emitted_events = [
                {"type": "response.created", "response": {"id": "resp_missing_terminal"}},
            ]
            with self.assertRaisesRegex(RuntimeError, "without a terminal response event"):
                list(openai_v1_response.handle(body))

        self.assertTrue(all(instance.closed for instance in EventBackend.instances))

    def test_native_http_rejects_malformed_response_created_scalars(self) -> None:
        class EventBackend(_FakeCodexBackend):
            emitted_events: list[dict[str, object]] = []

            def iter_codex_response_events(self, payload: dict[str, object], *, timeout: float):
                del timeout
                self.payload = payload
                yield from self.emitted_events

        body = {
            "model": "auto",
            "input": "What is the weather?",
            "tools": [FUNCTION_TOOL],
            "stream": True,
        }
        terminal = function_events()[-1]
        invalid_fields = (
            ("id", {"coerced": True}),
            ("id", ""),
            ("model", ["coerced"]),
            ("created_at", "123"),
            ("created_at", True),
            ("output", {"coerced": True}),
        )

        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used"),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", EventBackend),
            mock.patch(
                "services.model_service.model_catalog_service.normalize_reasoning_effort",
                return_value="minimal",
            ),
        ):
            for field, value in invalid_fields:
                with self.subTest(field=field, value=value):
                    created = {
                        "type": "response.created",
                        "response": {
                            "id": "resp_created",
                            "status": "in_progress",
                            field: value,
                        },
                    }
                    EventBackend.emitted_events = [created, terminal]

                    with self.assertRaisesRegex(RuntimeError, "malformed response.created"):
                        list(openai_v1_response.handle(body))

    def test_native_http_rejects_malformed_response_in_progress_scalars(self) -> None:
        class EventBackend(_FakeCodexBackend):
            emitted_events: list[dict[str, object]] = []

            def iter_codex_response_events(self, payload: dict[str, object], *, timeout: float):
                del timeout
                self.payload = payload
                yield from self.emitted_events

        body = {
            "model": "auto",
            "input": "What is the weather?",
            "tools": [FUNCTION_TOOL],
            "stream": True,
        }
        terminal = function_events()[-1]
        invalid_fields = (
            ("id", {"coerced": True}),
            ("id", ""),
            ("model", ["coerced"]),
            ("created_at", "123"),
            ("created_at", True),
            ("output", {"coerced": True}),
        )

        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used"),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", EventBackend),
            mock.patch(
                "services.model_service.model_catalog_service.normalize_reasoning_effort",
                return_value="minimal",
            ),
        ):
            for field, value in invalid_fields:
                with self.subTest(field=field, value=value):
                    in_progress = {
                        "type": "response.in_progress",
                        "response": {
                            "id": "resp_in_progress",
                            "status": "in_progress",
                            field: value,
                        },
                    }
                    EventBackend.emitted_events = [in_progress, terminal]

                    with self.assertRaisesRegex(RuntimeError, "malformed response.in_progress"):
                        list(openai_v1_response.handle(body))

    def test_native_responses_rejects_malformed_usage_without_scalar_coercion(self) -> None:
        class EventBackend(_FakeCodexBackend):
            emitted_events: list[dict[str, object]] = []

            def iter_codex_response_events(self, payload: dict[str, object], *, timeout: float):
                del timeout
                self.payload = payload
                yield from self.emitted_events

        invalid_usage_values = (
            {"input_tokens": "12", "output_tokens": 7, "total_tokens": 19},
            {"input_tokens": True, "output_tokens": 7, "total_tokens": 8},
            {"input_tokens": 1.5, "output_tokens": 7, "total_tokens": 8},
            {"input_tokens": -1, "output_tokens": 7, "total_tokens": 6},
            {"input_tokens": 12, "output_tokens": 7},
            {
                "input_tokens": 12,
                "output_tokens": 7,
                "total_tokens": 19,
                "input_tokens_details": "not-an-object",
            },
            {
                "input_tokens": 12,
                "output_tokens": 7,
                "total_tokens": 19,
                "input_tokens_details": {"cached_tokens": "3"},
            },
            {
                "input_tokens": 12,
                "output_tokens": 7,
                "total_tokens": 19,
                "output_tokens_details": {"reasoning_tokens": False},
            },
        )

        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used"),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", EventBackend),
            mock.patch(
                "services.model_service.model_catalog_service.normalize_reasoning_effort",
                return_value="minimal",
            ),
        ):
            for stream in (False, True):
                for usage in invalid_usage_values:
                    with self.subTest(stream=stream, usage=usage):
                        terminal = function_events()[-1]
                        terminal["response"]["usage"] = usage
                        EventBackend.emitted_events = [terminal]
                        body = {
                            "model": "auto",
                            "input": "What is the weather?",
                            "tools": [FUNCTION_TOOL],
                            "stream": stream,
                        }

                        with self.assertRaisesRegex(RuntimeError, "malformed codex usage"):
                            result = openai_v1_response.handle(body)
                            if stream:
                                list(result)

    def test_native_responses_projects_usage_to_known_public_fields(self) -> None:
        class EventBackend(_FakeCodexBackend):
            def iter_codex_response_events(self, payload: dict[str, object], *, timeout: float):
                del timeout
                self.payload = payload
                terminal = function_events()[-1]
                terminal["response"]["usage"] = {
                    "input_tokens": 12,
                    "input_tokens_details": {
                        "cached_tokens": 3,
                        "cache_write_tokens": 2,
                        "opaque_internal_detail": "private-detail",
                    },
                    "output_tokens": 7,
                    "output_tokens_details": {
                        "reasoning_tokens": 4,
                        "opaque_internal_detail": "private-detail",
                    },
                    "total_tokens": 19,
                    "codex_rollout_budget_units": 99,
                    "opaque_internal_usage": "private-usage",
                }
                yield terminal

        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used"),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", EventBackend),
            mock.patch(
                "services.model_service.model_catalog_service.normalize_reasoning_effort",
                return_value="minimal",
            ),
        ):
            response = openai_v1_response.handle(
                {
                    "model": "auto",
                    "input": "What is the weather?",
                    "tools": [FUNCTION_TOOL],
                }
            )

        self.assertEqual(
            response["usage"],
            {
                "input_tokens": 12,
                "input_tokens_details": {"cached_tokens": 3, "cache_write_tokens": 2},
                "output_tokens": 7,
                "output_tokens_details": {"reasoning_tokens": 4},
                "total_tokens": 19,
            },
        )
        self.assertNotIn("private", json.dumps(response, ensure_ascii=False))

    def test_native_http_resolves_reasoning_effort_for_selected_account(self) -> None:
        body = {
            "model": "auto",
            "input": "Use the weather tool",
            "tools": [FUNCTION_TOOL],
            "reasoning": {"effort": "wrong-for-this-model", "summary": "detailed"},
            "stream": True,
        }
        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="selected-codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used"),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", _FakeCodexBackend),
            mock.patch(
                "services.model_service.model_catalog_service.normalize_reasoning_effort",
                return_value="max",
            ) as normalize,
        ):
            list(openai_v1_response.handle(body))

        payload = _FakeCodexBackend.instances[0].payload or {}
        self.assertEqual(payload["reasoning"], {"effort": "max", "summary": "detailed"})
        normalize.assert_called_once_with(
            CODEX_RESPONSES_MODEL,
            "wrong-for-this-model",
            access_token="selected-codex-token",
        )

    def test_responses_function_tool_matches_fixed_codex_request_shape(self) -> None:
        deferred_tool = {**FUNCTION_TOOL, "defer_loading": True}
        payload = openai_v1_response.codex_response_payload({
            "model": "auto",
            "input": "Use the weather tool",
            "tools": [deferred_tool],
        })
        self.assertEqual(payload["tools"], [deferred_tool])

        minimal_payload = openai_v1_response.codex_response_payload({
            "model": "auto",
            "input": "Use the tool",
            "tools": [{"type": "function", "name": "minimal", "defer_loading": False}],
        })
        self.assertEqual(minimal_payload["tools"], [{
            "type": "function",
            "name": "minimal",
            "description": "",
            "strict": False,
            "parameters": {},
        }])

        invalid_tools = (
            {**FUNCTION_TOOL, "future_field": "must-not-reach-codex"},
            {**FUNCTION_TOOL, "description": {"text": "weather"}},
            {**FUNCTION_TOOL, "defer_loading": "true"},
            {**FUNCTION_TOOL, "output_schema": {"type": "object"}},
        )
        for tool in invalid_tools:
            with self.subTest(tool=tool):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": "auto",
                        "input": "Use the weather tool",
                        "tools": [tool],
                    })
                self.assertEqual(raised.exception.status_code, 400)

    def test_responses_rejects_noncanonical_tool_type(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            openai_v1_response.codex_response_payload({
                "model": "auto",
                "input": "Use the weather tool",
                "tools": [{**FUNCTION_TOOL, "type": " function "}],
            })

        self.assertEqual(raised.exception.status_code, 400)

    def test_responses_compaction_stream_preserves_opaque_output_item(self) -> None:
        compaction_item = {
            "id": "cmp_1",
            "type": "compaction",
            "encrypted_content": "opaque-compaction-state",
        }
        events = [
            {"type": "response.created", "response": {"id": "resp_compacted"}},
            {"type": "response.output_item.done", "output_index": 0, "item": compaction_item},
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_compacted",
                    "status": "completed",
                    "output": [compaction_item],
                },
            },
        ]

        class CompactionBackend(_FakeCodexBackend):
            def iter_codex_response_events(self, payload: dict[str, object], *, timeout: float):
                del timeout
                self.payload = payload
                yield from events

        body = {
            "model": "gpt-5.3-codex",
            "input": "continue",
            "store": False,
            "stream": True,
            "context_management": [{"type": "compaction", "compact_threshold": 200000}],
        }
        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used") as mark_used,
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", CompactionBackend),
        ):
            actual = list(openai_v1_response.handle(body))

        self.assertEqual(actual, events)
        self.assertEqual(
            CompactionBackend.instances[-1].payload["context_management"],
            body["context_management"],
        )
        mark_used.assert_called_once_with("codex-token")

    def test_responses_function_call_output_round_trips_without_prompt_conversion(self) -> None:
        function_output = {
            "type": "function_call_output",
            "call_id": "call_1",
            "output": '{"temperature":21}',
        }
        body = {
            "model": CODEX_RESPONSES_MODEL,
            "input": [function_output],
            "tools": [FUNCTION_TOOL],
        }
        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used"),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", _FakeCodexBackend),
        ):
            response = openai_v1_response.handle(body)

        self.assertEqual(response["output"][0]["type"], "function_call")
        self.assertEqual(_FakeCodexBackend.instances[0].payload["input"], [function_output])

    def test_responses_function_output_without_repeated_tools_uses_native_codex(self) -> None:
        function_output = {
            "type": "function_call_output",
            "call_id": "call_1",
            "output": '{"temperature":21}',
        }
        body = {
            "model": CODEX_RESPONSES_MODEL,
            "input": [function_output],
        }
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_tool_output", "status": "completed", "output": []},
        }
        payloads: list[dict[str, object]] = []

        def native_events(native_body: dict[str, object]):
            payloads.append(openai_v1_response.codex_response_payload(native_body))
            yield terminal

        with (
            mock.patch.object(
                openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                openai_v1_response,
                "text_backend",
                side_effect=AssertionError("function output must not use the plain conversation backend"),
            ),
        ):
            events = list(openai_v1_response.response_events(body))

        self.assertEqual(events, [terminal])
        self.assertEqual(payloads[0]["input"], [function_output])
        self.assertNotIn("tools", payloads[0])

    def test_responses_function_output_rejects_unrepresentable_shapes(self) -> None:
        invalid_outputs = (
            {"type": "function_call_output", "call_id": "", "output": "ok"},
            {"type": "function_call_output", "call_id": "call_1", "output": {"coerced": True}},
            {"type": "function_call_output", "call_id": "call_1", "output": ["discarded"]},
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": [{"type": "input_text", "text": {"coerced": True}}],
            },
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": [{"type": "input_file", "file_data": "aGVsbG8="}],
            },
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": [{"type": "input_image", "file_id": "file_unsupported"}],
            },
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "ok",
                "caller": {"type": "direct"},
            },
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": "ok",
                "future_field": "must-not-be-ignored",
            },
        )

        for function_output in invalid_outputs:
            with self.subTest(function_output=function_output):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": [function_output],
                    })

                self.assertEqual(raised.exception.status_code, 400)

    def test_responses_function_output_multimodal_content_round_trips(self) -> None:
        function_output = {
            "type": "function_call_output",
            "id": "fco_history",
            "call_id": "call_1",
            "status": "completed",
            "output": [
                {"type": "input_text", "text": "caption"},
                {
                    "type": "input_image",
                    "image_url": "data:image/png;base64,aW1hZ2U=",
                    "detail": "high",
                },
                {
                    "type": "input_audio",
                    "audio_url": "data:audio/wav;base64,YXVkaW8=",
                },
                {"type": "encrypted_content", "encrypted_content": "enc_opaque"},
            ],
        }

        payload = openai_v1_response.codex_response_payload({
            "model": CODEX_RESPONSES_MODEL,
            "input": [function_output],
        })

        expected = dict(function_output)
        expected.pop("status")
        self.assertEqual(payload["input"], [expected])

    def test_responses_custom_tool_output_rejects_unrepresentable_shapes(self) -> None:
        invalid_outputs = (
            {"type": "custom_tool_call_output", "call_id": "", "output": "ok"},
            {
                "type": "custom_tool_call_output",
                "call_id": "call_1",
                "name": "",
                "output": "ok",
            },
            {
                "type": "custom_tool_call_output",
                "call_id": "call_1",
                "output": {"coerced": True},
            },
            {
                "type": "custom_tool_call_output",
                "call_id": "call_1",
                "output": [{"type": "input_file", "file_data": "aGVsbG8="}],
            },
            {
                "type": "custom_tool_call_output",
                "call_id": "call_1",
                "output": "ok",
                "caller": {"type": "direct"},
            },
            {
                "type": "custom_tool_call_output",
                "call_id": "call_1",
                "output": "ok",
                "status": "completed",
            },
        )

        for custom_output in invalid_outputs:
            with self.subTest(custom_output=custom_output):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": [custom_output],
                    })

                self.assertEqual(raised.exception.status_code, 400)

    def test_responses_custom_tool_output_multimodal_content_round_trips(self) -> None:
        custom_output = {
            "type": "custom_tool_call_output",
            "id": "ctco_history",
            "call_id": "call_1",
            "name": "run_local_tool",
            "output": [
                {"type": "input_text", "text": "caption"},
                {
                    "type": "input_image",
                    "image_url": "data:image/png;base64,aW1hZ2U=",
                    "detail": "high",
                },
                {
                    "type": "input_audio",
                    "audio_url": "data:audio/wav;base64,YXVkaW8=",
                },
                {"type": "encrypted_content", "encrypted_content": "enc_opaque"},
            ],
        }

        payload = openai_v1_response.codex_response_payload({
            "model": CODEX_RESPONSES_MODEL,
            "input": [custom_output],
        })

        self.assertEqual(payload["input"], [custom_output])

    def test_responses_tool_call_history_rejects_unrepresentable_shapes(self) -> None:
        invalid_items = (
            {
                "type": "function_call",
                "call_id": "",
                "name": "get_weather",
                "arguments": "{}",
            },
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "",
                "arguments": "{}",
            },
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "get_weather",
                "arguments": {"coerced": True},
            },
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "get_weather",
                "arguments": "{}",
                "status": "failed",
            },
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "get_weather",
                "arguments": "{}",
                "caller": {"type": "direct"},
            },
            {
                "type": "custom_tool_call",
                "call_id": "custom_1",
                "name": "shell",
                "input": {"coerced": True},
            },
            {
                "type": "custom_tool_call",
                "call_id": "custom_1",
                "name": "shell",
                "input": "pwd",
                "future_field": "must-not-be-ignored",
            },
        )

        for input_item in invalid_items:
            with self.subTest(input_item=input_item):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": [input_item],
                    })

                self.assertEqual(raised.exception.status_code, 400)

    def test_responses_tool_call_history_preserves_supported_codex_fields(self) -> None:
        function_call = {
            "type": "function_call",
            "id": "fc_history",
            "call_id": "call_1",
            "name": "get_weather",
            "namespace": "weather",
            "arguments": '{"city":"Shanghai"}',
            "encrypted_function_args": ["enc_arg_1"],
            "status": "completed",
        }
        custom_tool_call = {
            "type": "custom_tool_call",
            "id": "ctc_history",
            "call_id": "custom_1",
            "name": "shell",
            "namespace": "local",
            "input": "pwd",
            "status": "completed",
        }

        payload = openai_v1_response.codex_response_payload({
            "model": CODEX_RESPONSES_MODEL,
            "input": [function_call, custom_tool_call],
        })

        expected_function_call = dict(function_call)
        expected_function_call.pop("status")
        self.assertEqual(payload["input"], [expected_function_call, custom_tool_call])

    def test_responses_stateful_history_rejects_unrepresentable_shapes(self) -> None:
        invalid_items = (
            {
                "type": "reasoning",
                "id": "rs_1",
                "summary": "discarded",
                "encrypted_content": "opaque",
            },
            {
                "type": "reasoning",
                "id": "rs_1",
                "summary": [{"type": "summary_text", "text": {"coerced": True}}],
            },
            {
                "type": "reasoning",
                "id": "rs_1",
                "summary": [],
                "future_field": "must-not-be-ignored",
            },
            {"type": "compaction", "id": "cmp_1", "encrypted_content": {"bad": True}},
            {
                "type": "context_compaction",
                "id": "cmp_2",
                "encrypted_content": ["bad"],
            },
            {
                "type": "image_generation_call",
                "id": "ig_1",
                "status": "completed",
                "result": None,
            },
            {
                "type": "image_generation_call",
                "id": "ig_1",
                "status": "unknown",
                "result": "aW1hZ2U=",
            },
            {
                "type": "web_search_call",
                "id": "ws_1",
                "status": "completed",
                "action": {
                    "type": "search",
                    "query": "weather",
                    "sources": [{"type": "url", "url": "https://example.test"}],
                },
            },
        )

        for input_item in invalid_items:
            with self.subTest(input_item=input_item):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": [input_item],
                    })

                self.assertEqual(raised.exception.status_code, 400)

    def test_responses_reasoning_history_preserves_state_and_strips_output_status(self) -> None:
        reasoning_item = {
            "type": "reasoning",
            "id": "rs_1",
            "summary": [{"type": "summary_text", "text": "summary"}],
            "content": [
                {"type": "reasoning_text", "text": "reasoning"},
                {"type": "text", "text": "upstream-compatible text"},
            ],
            "encrypted_content": "opaque-reasoning-state",
            "status": "completed",
        }

        payload = openai_v1_response.codex_response_payload({
            "model": CODEX_RESPONSES_MODEL,
            "input": [reasoning_item],
        })

        expected = dict(reasoning_item)
        expected.pop("status")
        self.assertEqual(payload["input"], [expected])

    def test_responses_dynamic_tool_history_rejects_unrepresentable_shapes(self) -> None:
        invalid_items = (
            {
                "type": "tool_search_call",
                "call_id": "search_1",
                "arguments": {"query": "calendar"},
            },
            {
                "type": "tool_search_call",
                "call_id": "search_1",
                "execution": "automatic",
                "arguments": {"query": "calendar"},
            },
            {
                "type": "tool_search_output",
                "call_id": "search_1",
                "status": "completed",
                "execution": "client",
                "tools": {"coerced": True},
            },
            {
                "type": "tool_search_output",
                "call_id": "search_1",
                "status": "completed",
                "execution": "client",
                "tools": [],
                "future_field": "must-not-be-ignored",
            },
            {
                "type": "mcp_tool_call_output",
                "call_id": "mcp_1",
                "output": "discarded",
            },
            {
                "type": "mcp_tool_call_output",
                "call_id": "",
                "output": {"content": []},
            },
        )

        for input_item in invalid_items:
            with self.subTest(input_item=input_item):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": [input_item],
                    })

                self.assertEqual(raised.exception.status_code, 400)

    def test_responses_dynamic_tool_history_round_trips_fixed_codex_shapes(self) -> None:
        input_items = [
            {
                "type": "tool_search_call",
                "id": "tsc_1",
                "call_id": "search_1",
                "status": "completed",
                "execution": "client",
                "arguments": {"query": "calendar"},
            },
            {
                "type": "tool_search_output",
                "id": "tso_1",
                "call_id": "search_1",
                "status": "completed",
                "execution": "client",
                "tools": [{"type": "function", "name": "get_calendar"}],
            },
            {
                "type": "mcp_tool_call_output",
                "call_id": "mcp_1",
                "output": {"content": [{"type": "text", "text": "done"}]},
            },
        ]

        payload = openai_v1_response.codex_response_payload({
            "model": CODEX_RESPONSES_MODEL,
            "input": input_items,
        })

        self.assertEqual(payload["input"], input_items)

    def test_responses_single_input_message_is_wrapped_for_native_codex(self) -> None:
        input_message = {
            "type": "message",
            "role": "assistant",
            "phase": "commentary",
            "content": [
                {"type": "output_text", "text": "look at this"},
                {
                    "type": "input_image",
                    "image_url": "data:image/png;base64,aW1hZ2U=",
                    "detail": "original",
                },
            ],
        }
        body = {
            "model": CODEX_RESPONSES_MODEL,
            "input": input_message,
            "tools": [FUNCTION_TOOL],
        }
        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                return_value="codex-token",
            ),
            mock.patch.object(openai_v1_response.account_service, "mark_text_used"),
            mock.patch.object(openai_v1_response, "OpenAIBackendAPI", _FakeCodexBackend),
        ):
            openai_v1_response.handle(body)

        self.assertEqual(_FakeCodexBackend.instances[0].payload["input"], [input_message])

    def test_responses_native_message_features_route_without_repeated_tools(self) -> None:
        native_messages = (
            {
                "role": "developer",
                "content": [{"type": "input_text", "text": "follow this instruction"}],
            },
            {
                "type": "message",
                "role": "assistant",
                "phase": "commentary",
                "content": [{"type": "output_text", "text": "still working"}],
            },
            {
                "type": "message",
                "role": "user",
                "content": [{
                    "type": "input_audio",
                    "audio_url": "data:audio/wav;base64,YXVkaW8=",
                }],
            },
            {
                "type": "message",
                "role": "user",
                "content": [{
                    "type": "input_image",
                    "image_url": "data:image/png;base64,aW1hZ2U=",
                    "detail": "original",
                }],
            },
        )
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_native_message", "status": "completed", "output": []},
        }

        for input_message in native_messages:
            with self.subTest(input_message=input_message):
                payloads: list[dict[str, object]] = []

                def native_events(native_body: dict[str, object]):
                    payloads.append(openai_v1_response.codex_response_payload(native_body))
                    yield terminal

                with (
                    mock.patch.object(
                        openai_v1_response,
                        "stream_codex_response",
                        side_effect=native_events,
                    ),
                    mock.patch.object(
                        openai_v1_response,
                        "text_backend",
                        side_effect=AssertionError(
                            "Codex-native message features must not use the plain conversation backend"
                        ),
                    ),
                ):
                    events = list(openai_v1_response.response_events({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": input_message,
                    }))

                self.assertEqual(events, [terminal])
                expected_message = dict(input_message)
                expected_message.setdefault("type", "message")
                self.assertEqual(payloads[0]["input"], [expected_message])
                self.assertNotIn("tools", payloads[0])

    def test_responses_explicit_image_detail_routes_native_and_is_preserved(self) -> None:
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_image_detail", "status": "completed", "output": []},
        }

        for detail in ("auto", "low", "high", "original"):
            with self.subTest(detail=detail):
                input_message = {
                    "role": "user",
                    "content": [{
                        "type": "input_image",
                        "image_url": "data:image/png;base64,aW1hZ2U=",
                        "detail": detail,
                    }],
                }
                payloads: list[dict[str, object]] = []

                def native_events(native_body: dict[str, object]):
                    payloads.append(openai_v1_response.codex_response_payload(native_body))
                    yield terminal

                with (
                    mock.patch.object(
                        openai_v1_response,
                        "stream_codex_response",
                        side_effect=native_events,
                    ),
                    mock.patch.object(
                        openai_v1_response,
                        "text_backend",
                        side_effect=AssertionError(
                            "explicit Responses image detail must use Codex Responses"
                        ),
                    ),
                ):
                    events = list(openai_v1_response.response_events({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": input_message,
                    }))

                self.assertEqual(events, [terminal])
                self.assertEqual(payloads[0]["input"], [{"type": "message", **input_message}])

    def test_responses_output_text_metadata_is_validated_and_removed_from_codex_input(self) -> None:
        body = {
            "model": CODEX_RESPONSES_MODEL,
            "input": [{
                "type": "message",
                "id": "msg_history",
                "role": "assistant",
                "status": "completed",
                "content": [{
                    "type": "output_text",
                    "text": "done",
                    "annotations": [{"type": "url_citation", "url": "https://example.test"}],
                    "logprobs": [],
                }],
            }],
            "tools": [FUNCTION_TOOL],
        }

        payload = openai_v1_response.codex_response_payload(body)

        self.assertEqual(payload["input"][0], {
            "type": "message",
            "id": "msg_history",
            "role": "assistant",
            "content": [{"type": "output_text", "text": "done"}],
        })

    def test_responses_top_level_native_content_parts_are_wrapped_in_a_user_message(self) -> None:
        native_parts = (
            {
                "type": "input_image",
                "image_url": "data:image/png;base64,aW1hZ2U=",
                "detail": "high",
            },
            {
                "type": "input_audio",
                "audio_url": "data:audio/wav;base64,YXVkaW8=",
            },
        )
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_content_part", "status": "completed", "output": []},
        }

        for part in native_parts:
            with self.subTest(part=part):
                payloads: list[dict[str, object]] = []

                def native_events(native_body: dict[str, object]):
                    payloads.append(openai_v1_response.codex_response_payload(native_body))
                    yield terminal

                with (
                    mock.patch.object(
                        openai_v1_response,
                        "stream_codex_response",
                        side_effect=native_events,
                    ),
                    mock.patch.object(
                        openai_v1_response,
                        "text_backend",
                        side_effect=AssertionError(
                            "native top-level content must not use the plain backend"
                        ),
                    ),
                ):
                    events = list(openai_v1_response.response_events({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": [part],
                    }))

                self.assertEqual(events, [terminal])
                self.assertEqual(payloads[0]["input"], [{
                    "type": "message",
                    "role": "user",
                    "content": [part],
                }])

    def test_responses_easy_input_message_phase_routes_and_adds_optional_type(self) -> None:
        easy_message = {
            "role": "assistant",
            "phase": "final_answer",
            "content": [{"type": "output_text", "text": "done"}],
        }
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_easy_message", "status": "completed", "output": []},
        }
        payloads: list[dict[str, object]] = []

        def native_events(native_body: dict[str, object]):
            payloads.append(openai_v1_response.codex_response_payload(native_body))
            yield terminal

        with (
            mock.patch.object(
                openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                openai_v1_response,
                "text_backend",
                side_effect=AssertionError(
                    "Responses EasyInputMessage phase must not use the plain conversation backend"
                ),
            ),
        ):
            events = list(openai_v1_response.response_events({
                "model": CODEX_RESPONSES_MODEL,
                "input": easy_message,
            }))

        self.assertEqual(events, [terminal])
        self.assertEqual(payloads[0]["input"], [{"type": "message", **easy_message}])

    def test_responses_native_history_items_route_without_repeated_tools(self) -> None:
        history_items = (
            {
                "type": "reasoning",
                "id": "rs_1",
                "summary": [],
                "encrypted_content": "opaque-reasoning-state",
            },
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "get_weather",
                "arguments": '{"city":"Shanghai"}',
            },
            {
                "type": "custom_tool_call",
                "call_id": "custom_1",
                "name": "shell",
                "input": "pwd",
            },
            {
                "type": "tool_search_call",
                "call_id": "search_1",
                "execution": "client",
                "arguments": {"query": "calendar"},
            },
            {
                "type": "web_search_call",
                "id": "ws_1",
                "status": "completed",
            },
            {
                "type": "image_generation_call",
                "id": "ig_1",
                "status": "completed",
                "result": "aW1hZ2U=",
            },
            {
                "type": "compaction",
                "id": "cmp_1",
                "encrypted_content": "opaque-compaction-state",
            },
            {
                "type": "context_compaction",
                "id": "cmp_2",
                "encrypted_content": "opaque-context-state",
            },
        )
        user_message = {
            "role": "user",
            "content": [{"type": "input_text", "text": "continue"}],
        }
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_reasoning_history", "status": "completed", "output": []},
        }
        for history_item in history_items:
            with self.subTest(history_type=history_item["type"]):
                payloads: list[dict[str, object]] = []

                def native_events(native_body: dict[str, object]):
                    payloads.append(openai_v1_response.codex_response_payload(native_body))
                    yield terminal

                with (
                    mock.patch.object(
                        openai_v1_response,
                        "stream_codex_response",
                        side_effect=native_events,
                    ),
                    mock.patch.object(
                        openai_v1_response,
                        "text_backend",
                        side_effect=AssertionError(
                            "Responses native history must not use the plain conversation backend"
                        ),
                    ),
                ):
                    events = list(openai_v1_response.response_events({
                        "model": CODEX_RESPONSES_MODEL,
                        "input": [history_item, user_message],
                    }))

                self.assertEqual(events, [terminal])
                self.assertEqual(
                    payloads[0]["input"],
                    [history_item, {"type": "message", **user_message}],
                )

    def test_codex_function_path_rejects_parameters_the_upstream_cannot_honor(self) -> None:
        for field, value in (
            ("temperature", 0.2),
            ("top_p", 0.5),
            ("max_output_tokens", 100),
            ("generate", False),
            ("parallel_tool_calls", "false"),
            ("store", True),
        ):
            with self.subTest(field=field):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.handle(
                        {"model": "auto", "input": "hello", "tools": [FUNCTION_TOOL], field: value}
                    )
                self.assertEqual(raised.exception.status_code, 400)

    def test_backend_parses_sse_incrementally_instead_of_buffering_whole_response(self) -> None:
        raw = _IncrementalSSE()
        events = OpenAIBackendAPI._iter_codex_response_events(raw)

        first = next(events)

        self.assertEqual(first["type"], "response.created")
        self.assertEqual(raw.next_calls, 2)
        raw.allow_rest = True
        self.assertEqual([event["type"] for event in events], ["response.completed"])

    def test_generic_codex_backend_posts_exact_payload_to_responses_endpoint(self) -> None:
        payload = {
            "model": CODEX_RESPONSES_MODEL,
            "input": [{"type": "function_call_output", "call_id": "call_1", "output": "sunny"}],
            "tools": [FUNCTION_TOOL],
            "stream": True,
            "store": False,
        }
        response_body = "\n".join(
            f"data: {json.dumps(event, separators=(',', ':'))}\n" for event in function_events()
        ).encode()

        class Raw:
            status = 200
            status_code = 200
            headers = {"content-type": "text/event-stream"}

            def __init__(self) -> None:
                self._lines = iter(response_body.splitlines(keepends=True))

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def __iter__(self):
                return self

            def __next__(self):
                return next(self._lines)

            def close(self) -> None:
                pass

        class Session:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {"User-Agent": "web-default"}
                self.close_calls = 0

            def post(self, url: str, **kwargs: object) -> Raw:
                captured["url"] = url
                captured["kwargs"] = kwargs
                return Raw()

            def close(self) -> None:
                self.close_calls += 1

        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.codex_client_version = "0.147.0"
        backend.fp = {"impersonate": "chrome110"}
        backend.base_url = "https://chatgpt.example"
        backend._ensure_codex_source_account = mock.Mock()
        backend._codex_responses_headers = mock.Mock(
            return_value={"Authorization": "Bearer codex-token", "Content-Type": "application/json"}
        )
        captured: dict[str, object] = {}
        session = Session()

        with mock.patch.object(backend_module.requests, "Session", return_value=session):
            events = list(backend.iter_codex_response_events(payload, timeout=17))

        self.assertEqual(events, function_events())
        self.assertEqual(captured["url"], "https://chatgpt.example/backend-api/codex/responses")
        kwargs = captured["kwargs"]
        self.assertEqual(json.loads(kwargs["data"]), payload)
        self.assertEqual(kwargs["timeout"], 17)
        self.assertTrue(kwargs["stream"])
        self.assertEqual(session.close_calls, 1)
        backend._ensure_codex_source_account.assert_called_once_with()

    def test_codex_responses_uses_configured_proxy_session(self) -> None:
        payload = {
            "model": CODEX_RESPONSES_MODEL,
            "input": [{"type": "input_text", "text": "hello"}],
            "stream": True,
        }
        response_body = b'data: {"type":"response.completed"}\n\n'

        class Raw:
            status = 200
            status_code = 200
            headers = {"content-type": "text/event-stream"}

            def __init__(self) -> None:
                self._lines = iter(response_body.splitlines(keepends=True))

            def __iter__(self):
                return self

            def __next__(self):
                return next(self._lines)

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def close(self) -> None:
                pass

        class Session:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {"User-Agent": "web-default"}
                self.post_calls: list[dict[str, object]] = []
                self.close_calls = 0

            def post(self, url: str, **kwargs: object) -> Raw:
                self.post_calls.append({"url": url, **kwargs})
                return Raw()

            def close(self) -> None:
                self.close_calls += 1

        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.codex_client_version = "0.147.0"
        backend.fp = {"impersonate": "chrome110"}
        backend.base_url = "https://chatgpt.example"
        backend._ensure_codex_source_account = mock.Mock()
        session = Session()

        with (
            mock.patch.object(
                backend_module.proxy_settings,
                "build_session_kwargs",
                side_effect=lambda **kwargs: {
                    "proxy": "http://proxy.example",
                    **kwargs,
                },
            ) as build_session_kwargs,
            mock.patch.object(backend_module.requests, "Session", return_value=session) as session_factory,
        ):
            events = list(backend.iter_codex_response_events(payload, timeout=17))

        self.assertEqual(events, [{"type": "response.completed"}])
        build_session_kwargs.assert_called_once()
        self.assertEqual(
            build_session_kwargs.call_args.kwargs["account"],
            backend.account,
        )
        session_kwargs = session_factory.call_args.kwargs
        self.assertFalse(session_kwargs["default_headers"])
        self.assertNotIn("impersonate", session_kwargs)
        self.assertEqual(session.post_calls[0]["timeout"], 17)
        self.assertTrue(session.post_calls[0]["stream"])
        self.assertEqual(session.headers, {})
        self.assertEqual(session.close_calls, 1)

    def test_codex_http_error_body_is_bounded_closed_and_not_exposed(self) -> None:
        class ErrorBody:
            status_code = 502
            headers = {"content-type": "application/json"}

            def __init__(self) -> None:
                self.closed = False
                self.chunk_sizes: list[int | None] = []
                self.body = b"opaque-codex-secret owner@example.com" * 100

            def iter_content(self, chunk_size: int | None = None):
                self.chunk_sizes.append(chunk_size)
                yield self.body

            def close(self) -> None:
                self.closed = True

        payload = {"model": CODEX_RESPONSES_MODEL, "input": "hello", "stream": True}
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.codex_client_version = "0.147.0"
        backend.fp = {"impersonate": "chrome110"}
        backend.base_url = "https://chatgpt.example"
        backend._ensure_codex_source_account = mock.Mock()
        backend._codex_responses_headers = mock.Mock(return_value={"Authorization": "Bearer codex-token"})
        error_body = ErrorBody()

        class Session:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {}
                self.close_calls = 0

            def post(self, _url: str, **_kwargs: object) -> ErrorBody:
                return error_body

            def close(self) -> None:
                self.close_calls += 1

        session = Session()

        with mock.patch.object(backend_module.requests, "Session", return_value=session) as session_factory:
            with self.assertRaises(backend_module.UpstreamHTTPError) as raised:
                list(backend.iter_codex_response_events(payload, timeout=1))

        self.assertTrue(error_body.closed)
        self.assertTrue(error_body.chunk_sizes)
        self.assertLessEqual(
            error_body.chunk_sizes[0] or 0,
            backend_module.CODEX_HTTP_ERROR_MAX_BODY_BYTES + 1,
        )
        self.assertEqual(session.close_calls, 1)
        self.assertFalse(session_factory.call_args.kwargs["default_headers"])
        self.assertNotIn("impersonate", session_factory.call_args.kwargs)
        self.assertNotIn("opaque-codex-secret", str(raised.exception))

    def test_codex_response_cleanup_attempts_both_without_masking_primary_error(self) -> None:
        class Raw:
            status_code = 200
            headers = {"content-type": "text/event-stream"}

            def __init__(self) -> None:
                self.close_calls = 0

            def __iter__(self):
                return iter([b"data: {not-json}\n\n"])

            def close(self) -> None:
                self.close_calls += 1
                raise RuntimeError("response-secret")

        class Session:
            def __init__(self, raw: Raw) -> None:
                self.headers: dict[str, str] = {}
                self.raw = raw
                self.close_calls = 0

            def post(self, _url: str, **_kwargs: object) -> Raw:
                return self.raw

            def close(self) -> None:
                self.close_calls += 1
                raise RuntimeError("session-secret")

        raw = Raw()
        session = Session(raw)
        backend = object.__new__(OpenAIBackendAPI)
        backend.access_token = "codex-token"
        backend.account = {"source_type": "codex", "account_id": "acct-codex"}
        backend.codex_client_version = "0.147.0"
        backend.fp = {"impersonate": "chrome110"}
        backend.base_url = "https://chatgpt.example"
        backend._ensure_codex_source_account = mock.Mock()
        backend._codex_responses_headers = mock.Mock(return_value={"Authorization": "Bearer codex-token"})

        with (
            mock.patch.object(backend_module.requests, "Session", return_value=session),
            mock.patch.object(backend_module.logger, "warning") as warning,
        ):
            with self.assertRaisesRegex(RuntimeError, "malformed codex response event"):
                list(backend.iter_codex_response_events(
                    {"model": CODEX_RESPONSES_MODEL, "input": "hello", "stream": True},
                    timeout=1,
                ))

        self.assertEqual(raw.close_calls, 1)
        self.assertEqual(session.close_calls, 1)
        logged = str(warning.call_args_list)
        self.assertIn("response", logged)
        self.assertIn("session", logged)
        self.assertNotIn("response-secret", logged)
        self.assertNotIn("session-secret", logged)


class ChatFunctionToolContractTests(unittest.TestCase):
    @staticmethod
    def _chat_body(*, stream: bool = False, include_usage: bool = False) -> dict[str, object]:
        body: dict[str, object] = {
            "model": "auto",
            "messages": [{"role": "user", "content": "What is the weather?"}],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "description": "Get the weather",
                    "parameters": FUNCTION_TOOL["parameters"],
                    "strict": True,
                },
            }],
            "tool_choice": "auto",
            "parallel_tool_calls": True,
            "reasoning_effort": "minimal",
            "stream": stream,
        }
        if include_usage:
            body["stream_options"] = {"include_usage": True}
        return body

    def test_chat_non_stream_function_call_uses_native_codex_and_official_shape(self) -> None:
        captured: dict[str, object] = {}

        def fake_codex(body):
            captured.update(body)
            yield from function_events()

        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            side_effect=fake_codex,
        ):
            response = openai_v1_chat_complete.handle(self._chat_body())

        choice = response["choices"][0]
        self.assertEqual(choice["finish_reason"], "tool_calls")
        self.assertIsNone(choice["message"]["content"])
        self.assertEqual(
            choice["message"]["tool_calls"],
            [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "get_weather", "arguments": '{"city":"Shanghai"}'},
            }],
        )
        self.assertEqual(response["usage"]["prompt_tokens"], 12)
        self.assertEqual(response["usage"]["completion_tokens"], 7)
        self.assertEqual(captured["tools"], [FUNCTION_TOOL])
        self.assertEqual(captured["tool_choice"], "auto")
        self.assertEqual(captured["reasoning"], {"effort": "minimal"})
        self.assertEqual(captured["input"][0]["type"], "message")

    def test_chat_developer_message_without_tools_uses_native_codex(self) -> None:
        body = {
            "model": "auto",
            "messages": [
                {"role": "developer", "content": "Answer concisely."},
                {"role": "user", "content": "Hello"},
            ],
        }
        terminal_message = {
            "id": "msg_developer",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "Hello.", "annotations": []}],
        }
        native_bodies: list[dict[str, object]] = []

        def native_events(converted: dict[str, object]):
            native_bodies.append(converted)
            yield {
                "type": "response.completed",
                "response": {
                    "id": "resp_developer",
                    "object": "response",
                    "status": "completed",
                    "model": CODEX_RESPONSES_MODEL,
                    "output": [terminal_message],
                    "usage": {"input_tokens": 8, "output_tokens": 2, "total_tokens": 10},
                },
            }

        with (
            mock.patch.object(
                openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("developer messages must not use the plain conversation backend"),
            ),
        ):
            response = openai_v1_chat_complete.handle(body)

        self.assertEqual(response["choices"][0]["message"]["content"], "Hello.")
        self.assertEqual(
            native_bodies[0]["input"],
            [
                {
                    "type": "message",
                    "role": "developer",
                    "content": [{"type": "input_text", "text": "Answer concisely."}],
                },
                {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "Hello"}],
                },
            ],
        )

    def test_chat_input_audio_without_tools_uses_native_codex(self) -> None:
        body = {
            "model": "auto",
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "input_audio",
                    "input_audio": {"data": "YXVkaW8=", "format": "wav"},
                }],
            }],
        }
        native_bodies: list[dict[str, object]] = []

        def native_events(converted: dict[str, object]):
            native_bodies.append(converted)
            yield {
                "type": "response.completed",
                "response": {
                    "id": "resp_audio",
                    "object": "response",
                    "status": "completed",
                    "model": CODEX_RESPONSES_MODEL,
                    "output": [],
                    "usage": {"input_tokens": 8, "output_tokens": 0, "total_tokens": 8},
                },
            }

        with (
            mock.patch.object(
                openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("input audio must not use the plain conversation backend"),
            ),
        ):
            openai_v1_chat_complete.handle(body)

        self.assertEqual(
            native_bodies[0]["input"],
            [{
                "type": "message",
                "role": "user",
                "content": [{
                    "type": "input_audio",
                    "audio_url": "data:audio/wav;base64,YXVkaW8=",
                }],
            }],
        )

    def test_chat_input_audio_rejects_malformed_payload_before_backend_selection(self) -> None:
        invalid_audio = (
            {"data": "%%%", "format": "wav"},
            {"data": "YXVkaW8=", "format": "flac"},
            {"data": "YXVkaW8=", "format": "wav", "future_field": "discarded"},
            ["YXVkaW8=", "wav"],
        )
        for input_audio in invalid_audio:
            with self.subTest(input_audio=input_audio):
                body = {
                    "model": "auto",
                    "messages": [{
                        "role": "user",
                        "content": [{"type": "input_audio", "input_audio": input_audio}],
                    }],
                }
                with (
                    mock.patch.object(
                        openai_v1_chat_complete.openai_v1_response,
                        "stream_codex_response",
                        side_effect=AssertionError("invalid audio must not reach Codex"),
                    ),
                    mock.patch.object(
                        openai_v1_chat_complete,
                        "text_backend",
                        side_effect=AssertionError("invalid audio must not reach the plain backend"),
                    ),
                    self.assertRaises(HTTPException) as raised,
                ):
                    openai_v1_chat_complete.handle(body)

                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_unhonored_message_content_is_rejected_before_backend_selection(self) -> None:
        invalid_content = (
            ("user", [{"type": "file", "file": {"file_id": "file_1"}}]),
            ("assistant", [{"type": "refusal", "refusal": "No"}]),
            ("user", [{"type": "text", "text": "hello", "future_field": "discarded"}]),
            ("user", [{"type": "future_part", "text": "discarded"}]),
        )
        for role, content in invalid_content:
            with self.subTest(role=role, content=content):
                body = {"model": "auto", "messages": [{"role": role, "content": content}]}
                with (
                    mock.patch.object(
                        openai_v1_chat_complete.openai_v1_response,
                        "stream_codex_response",
                        side_effect=AssertionError("invalid content must not reach Codex"),
                    ),
                    mock.patch.object(
                        openai_v1_chat_complete,
                        "text_backend",
                        side_effect=AssertionError("invalid content must not reach the plain backend"),
                    ),
                    self.assertRaises(HTTPException) as raised,
                ):
                    openai_v1_chat_complete.handle(body)

                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_image_detail_is_preserved_for_codex_responses(self) -> None:
        body = self._chat_body()
        body["messages"] = [{
            "role": "user",
            "content": [{
                "type": "image_url",
                "image_url": {
                    "url": "data:image/png;base64,iVBORw0KGgo=",
                    "detail": "high",
                },
            }],
        }]

        converted = openai_v1_chat_complete.chat_codex_response_body(body)

        self.assertEqual(
            converted["input"][0]["content"],
            [{
                "type": "input_image",
                "image_url": "data:image/png;base64,iVBORw0KGgo=",
                "detail": "high",
            }],
        )

    def test_chat_rejects_invalid_image_detail_before_codex_request(self) -> None:
        for detail in ("original", "ultra", 1, None, [], {}):
            with self.subTest(detail=detail):
                body = self._chat_body()
                body["messages"] = [{
                    "role": "user",
                    "content": [{
                        "type": "image_url",
                        "image_url": {
                            "url": "data:image/png;base64,iVBORw0KGgo=",
                            "detail": detail,
                        },
                    }],
                }]

                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.chat_codex_response_body(body)

                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_function_tool_rejects_fields_outside_official_shape(self) -> None:
        for location in ("outer", "function"):
            with self.subTest(location=location):
                body = self._chat_body()
                tool = body["tools"][0]
                target = tool if location == "outer" else tool["function"]
                target["future_field"] = "must-not-be-dropped"

                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.chat_codex_response_body(body)

                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_non_auto_tool_choice_is_rejected_before_codex(self) -> None:
        for tool_choice in (
            "none",
            "required",
            {"type": "function", "function": {"name": "get_weather"}},
        ):
            with self.subTest(tool_choice=tool_choice):
                body = self._chat_body()
                body["tool_choice"] = tool_choice

                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.chat_codex_response_body(body)

                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_codex_forwards_supported_prompt_cache_and_service_tier(self) -> None:
        for requested_tier, expected_tier in (
            ("priority", "priority"),
            ("fast", "priority"),
            ("flex", "flex"),
        ):
            with self.subTest(requested_tier=requested_tier):
                body = self._chat_body()
                body["prompt_cache_key"] = "chat-session-42"
                body["service_tier"] = requested_tier

                converted = openai_v1_chat_complete.chat_codex_response_body(body)
                upstream_payload = openai_v1_response.codex_response_payload(converted)

                self.assertEqual(converted["prompt_cache_key"], "chat-session-42")
                self.assertEqual(converted["service_tier"], expected_tier)
                self.assertEqual(upstream_payload["prompt_cache_key"], "chat-session-42")
                self.assertEqual(upstream_payload["service_tier"], expected_tier)

        default_body = self._chat_body()
        default_body["service_tier"] = "default"
        converted_default = openai_v1_chat_complete.chat_codex_response_body(default_body)
        self.assertNotIn("service_tier", converted_default)
        self.assertNotIn(
            "service_tier",
            openai_v1_response.codex_response_payload(converted_default),
        )

    def test_chat_codex_rejects_service_tiers_upstream_cannot_honor(self) -> None:
        for service_tier in ("auto", "scale", "ultra", 1, False):
            with self.subTest(service_tier=service_tier):
                body = self._chat_body()
                body["service_tier"] = service_tier

                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.chat_codex_response_body(body)

                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_codex_rejects_prompt_cache_breakpoint_it_cannot_forward(self) -> None:
        body = self._chat_body()
        body["messages"] = [{
            "role": "user",
            "content": [{
                "type": "text",
                "text": "What is the weather?",
                "prompt_cache_breakpoint": {"mode": "explicit"},
            }],
        }]

        with self.assertRaises(HTTPException) as raised:
            openai_v1_chat_complete.chat_codex_response_body(body)

        self.assertEqual(raised.exception.status_code, 400)

    def test_chat_stream_function_call_emits_incremental_tool_calls_and_terminal_usage(self) -> None:
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(function_events()),
        ):
            chunks = list(openai_v1_chat_complete.handle(self._chat_body(stream=True, include_usage=True)))

        for chunk in chunks[:-1]:
            self.assertIsNone(chunk["usage"])
        self.assertEqual(chunks[-1]["choices"], [])
        self.assertEqual(chunks[-1]["usage"]["total_tokens"], 19)
        self.assertEqual(chunks[0]["choices"][0]["delta"]["role"], "assistant")
        tool_deltas = [
            chunk["choices"][0]["delta"]["tool_calls"][0]
            for chunk in chunks
            if chunk["choices"] and chunk["choices"][0]["delta"].get("tool_calls")
        ]
        self.assertEqual(tool_deltas[0]["id"], "call_1")
        self.assertEqual(tool_deltas[0]["function"]["name"], "get_weather")
        self.assertEqual("".join(item["function"].get("arguments", "") for item in tool_deltas), '{"city":')
        self.assertEqual(chunks[-2]["choices"][0]["finish_reason"], "tool_calls")

    def test_chat_rejects_malformed_codex_usage_without_scalar_coercion(self) -> None:
        invalid_usage_values = (
            {"input_tokens": "12", "output_tokens": 7, "total_tokens": 19},
            {"input_tokens": True, "output_tokens": 7, "total_tokens": 8},
            {"input_tokens": 1.5, "output_tokens": 7, "total_tokens": 8},
            {"input_tokens": -1, "output_tokens": 7, "total_tokens": 6},
            {"input_tokens": 12, "output_tokens": 7},
            {
                "input_tokens": 12,
                "output_tokens": 7,
                "total_tokens": 19,
                "input_tokens_details": "not-an-object",
            },
            {
                "input_tokens": 12,
                "output_tokens": 7,
                "total_tokens": 19,
                "input_tokens_details": {"cached_tokens": "3"},
            },
            {
                "input_tokens": 12,
                "output_tokens": 7,
                "total_tokens": 19,
                "output_tokens_details": {"reasoning_tokens": False},
            },
        )

        for stream in (False, True):
            for usage in invalid_usage_values:
                with self.subTest(stream=stream, usage=usage):
                    terminal = function_events()[-1]
                    terminal["response"]["usage"] = usage
                    body = self._chat_body(stream=stream, include_usage=stream)
                    with mock.patch.object(
                        openai_v1_chat_complete.openai_v1_response,
                        "stream_codex_response",
                        return_value=iter([terminal]),
                    ):
                        with self.assertRaisesRegex(RuntimeError, "malformed codex usage"):
                            result = openai_v1_chat_complete.handle(body)
                            if stream:
                                list(result)

    def test_chat_accepts_absent_usage_and_strict_usage_details(self) -> None:
        terminal_without_usage = function_events()[-1]
        terminal_without_usage["response"]["usage"] = None
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter([terminal_without_usage]),
        ):
            response = openai_v1_chat_complete.handle(self._chat_body())
        self.assertEqual(response["usage"]["total_tokens"], 0)

        terminal_with_details = function_events()[-1]
        terminal_with_details["response"]["usage"] = {
            "input_tokens": 12,
            "input_tokens_details": {"cached_tokens": 3, "cache_write_tokens": 2},
            "output_tokens": 7,
            "output_tokens_details": {"reasoning_tokens": 4},
            "total_tokens": 19,
        }
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter([terminal_with_details]),
        ):
            response = openai_v1_chat_complete.handle(self._chat_body())
        self.assertEqual(response["usage"]["prompt_tokens_details"]["cached_tokens"], 3)
        self.assertEqual(response["usage"]["completion_tokens_details"]["reasoning_tokens"], 4)

    def test_chat_tool_result_history_maps_to_function_call_output(self) -> None:
        body = self._chat_body()
        body["messages"] = [
            {"role": "user", "content": "What is the weather?"},
            {
                "role": "assistant",
                "content": None,
                "tool_calls": [{
                    "id": "call_1",
                    "type": "function",
                    "function": {"name": "get_weather", "arguments": '{"city":"Shanghai"}'},
                }],
            },
            {"role": "tool", "tool_call_id": "call_1", "content": '{"temperature":21}'},
        ]

        converted = openai_v1_chat_complete.chat_codex_response_body(body)

        self.assertIn(
            {
                "type": "function_call",
                "call_id": "call_1",
                "name": "get_weather",
                "arguments": '{"city":"Shanghai"}',
            },
            converted["input"],
        )
        self.assertIn(
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": '{"temperature":21}',
            },
            converted["input"],
        )

    def test_chat_tool_call_history_rejects_unknown_nested_fields(self) -> None:
        invalid_tool_calls = (
            {
                "id": "call_1",
                "type": "function",
                "function": {"name": "get_weather", "arguments": "{}"},
                "index": 0,
            },
            {
                "id": "call_1",
                "type": "function",
                "function": {
                    "name": "get_weather",
                    "arguments": "{}",
                    "future_field": "must-not-be-ignored",
                },
            },
        )

        for tool_call in invalid_tool_calls:
            with self.subTest(tool_call=tool_call):
                body = {
                    "model": "auto",
                    "messages": [
                        {"role": "user", "content": "What is the weather?"},
                        {"role": "assistant", "content": None, "tool_calls": [tool_call]},
                    ],
                }
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.chat_codex_response_body(body)

                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_rejects_malformed_codex_function_call_output_types(self) -> None:
        malformed_terminal = {
            "type": "response.completed",
            "response": {
                "id": "resp_bad_function",
                "model": "auto",
                "output": [{
                    "type": "function_call",
                    "id": "fc_1",
                    "call_id": "call_1",
                    "name": "get_weather",
                    "arguments": {"coerced": True},
                }],
                "usage": {},
            },
        }
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter([malformed_terminal]),
        ):
            with self.assertRaises(RuntimeError):
                openai_v1_chat_complete._chat_response_from_codex(self._chat_body())

        malformed_stream = [
            {
                "type": "response.created",
                "response": {"id": "resp_bad_delta", "model": "auto", "created_at": 1},
            },
            {
                "type": "response.output_item.added",
                "item": {
                    "type": "function_call",
                    "id": "fc_1",
                    "call_id": "call_1",
                    "name": "get_weather",
                    "arguments": "",
                },
            },
            {
                "type": "response.function_call_arguments.delta",
                "item_id": "fc_1",
                "delta": {"coerced": True},
            },
            malformed_terminal,
        ]
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(malformed_stream),
        ):
            with self.assertRaises(RuntimeError):
                list(openai_v1_chat_complete._stream_chat_response_from_codex(self._chat_body()))

    def test_chat_rejects_malformed_codex_text_output_types(self) -> None:
        malformed_terminal = {
            "type": "response.completed",
            "response": {
                "id": "resp_bad_text",
                "model": "auto",
                "output": [{
                    "type": "message",
                    "content": [{"type": "output_text", "text": {"coerced": True}}],
                }],
                "usage": {},
            },
        }
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter([malformed_terminal]),
        ):
            with self.assertRaises(RuntimeError):
                openai_v1_chat_complete._chat_response_from_codex(self._chat_body())

        malformed_stream = [
            {
                "type": "response.created",
                "response": {"id": "resp_bad_text_delta", "model": "auto", "created_at": 1},
            },
            {"type": "response.output_text.delta", "delta": {"coerced": True}},
            malformed_terminal,
        ]
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(malformed_stream),
        ):
            with self.assertRaises(RuntimeError):
                list(openai_v1_chat_complete._stream_chat_response_from_codex(self._chat_body()))

    def test_chat_rejects_missing_required_codex_output_item(self) -> None:
        terminal = {
            "type": "response.completed",
            "response": {
                "id": "resp_missing_item",
                "model": "auto",
                "status": "completed",
                "output": [],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }
        for event_type in ("response.output_item.added", "response.output_item.done"):
            with self.subTest(event_type=event_type):
                events = [
                    {
                        "type": "response.created",
                        "response": {"id": "resp_missing_item", "model": "auto"},
                    },
                    {"type": event_type, "output_index": 0, "item": None},
                    terminal,
                ]
                with mock.patch.object(
                    openai_v1_chat_complete.openai_v1_response,
                    "stream_codex_response",
                    return_value=iter(events),
                ):
                    with self.assertRaisesRegex(RuntimeError, "malformed output item"):
                        list(openai_v1_chat_complete._stream_chat_response_from_codex(self._chat_body(stream=True)))

    def test_chat_stream_rejects_malformed_codex_message_content(self) -> None:
        canary = "message-content-container-canary owner@example.test"
        message = {
            "id": "msg_bad_stream_content",
            "type": "message",
            "content": {"secret": canary},
        }
        events = [
            {"type": "response.output_item.done", "output_index": 0, "item": message},
            {
                "type": "response.completed",
                "response": {
                    "id": "resp_bad_stream_content",
                    "model": "auto",
                    "output": [message],
                    "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
                },
            },
        ]
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(events),
        ):
            with self.assertRaisesRegex(RuntimeError, "malformed message content") as raised:
                list(openai_v1_chat_complete._stream_chat_response_from_codex(self._chat_body(stream=True)))
        self.assertNotIn(canary, str(raised.exception))

    def test_chat_rejects_malformed_codex_citation_scalars(self) -> None:
        canary = "citation-container-canary owner@example.test"
        message = {
            "id": "msg_bad_citation",
            "type": "message",
            "content": [{
                "type": "output_text",
                "text": "answer",
                "annotations": [{
                    "type": "url_citation",
                    "url": {"secret": canary},
                    "title": "Example",
                    "start_index": 0,
                    "end_index": 6,
                }],
            }],
        }
        terminal = {
            "type": "response.completed",
            "response": {
                "id": "resp_bad_citation",
                "model": "auto",
                "output": [message],
                "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2},
            },
        }
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter([terminal]),
        ):
            with self.assertRaisesRegex(RuntimeError, "malformed citation") as raised:
                openai_v1_chat_complete._chat_response_from_codex(self._chat_body())
        self.assertNotIn(canary, str(raised.exception))

        stream_events = [
            {"type": "response.output_item.done", "item": message},
            terminal,
        ]
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(stream_events),
        ):
            with self.assertRaisesRegex(RuntimeError, "malformed citation") as raised:
                list(openai_v1_chat_complete._stream_chat_response_from_codex(self._chat_body(stream=True)))
        self.assertNotIn(canary, str(raised.exception))

    def test_chat_tool_text_parts_map_to_plain_function_output(self) -> None:
        body = {
            "model": "auto",
            "messages": [
                {"role": "user", "content": "What is the weather?"},
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "get_weather", "arguments": "{}"},
                    }],
                },
                {
                    "role": "tool",
                    "tool_call_id": "call_1",
                    "content": [{"type": "text", "text": '{"temperature":21}'}],
                },
            ],
        }
        converted = openai_v1_chat_complete.chat_codex_response_body(body)

        self.assertIn(
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": '{"temperature":21}',
            },
            converted["input"],
        )

    def test_chat_tool_result_history_without_repeated_tools_uses_native_codex(self) -> None:
        body = {
            "model": "auto",
            "messages": [
                {"role": "user", "content": "What is the weather?"},
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [{
                        "id": "call_1",
                        "type": "function",
                        "function": {"name": "get_weather", "arguments": '{"city":"Shanghai"}'},
                    }],
                },
                {"role": "tool", "tool_call_id": "call_1", "content": '{"temperature":21}'},
            ],
        }
        terminal_message = {
            "id": "msg_2",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [{"type": "output_text", "text": "It is 21 degrees.", "annotations": []}],
        }
        native_bodies: list[dict[str, object]] = []

        def native_events(converted: dict[str, object]):
            native_bodies.append(converted)
            yield {
                "type": "response.completed",
                "response": {
                    "id": "resp_2",
                    "object": "response",
                    "status": "completed",
                    "model": CODEX_RESPONSES_MODEL,
                    "output": [terminal_message],
                    "usage": {"input_tokens": 8, "output_tokens": 5, "total_tokens": 13},
                },
            }

        with (
            mock.patch.object(
                openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=native_events,
            ),
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("tool history must not use the plain conversation backend"),
            ),
        ):
            response = openai_v1_chat_complete.handle(body)

        self.assertEqual(response["choices"][0]["message"]["content"], "It is 21 degrees.")
        self.assertNotIn("tools", native_bodies[0])
        self.assertIn(
            {
                "type": "function_call_output",
                "call_id": "call_1",
                "output": '{"temperature":21}',
            },
            native_bodies[0]["input"],
        )

    def test_chat_function_path_rejects_sampling_fields_codex_cannot_honor(self) -> None:
        body = self._chat_body()
        body["temperature"] = 0.2
        with (
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("function tools must not fall back to the web conversation backend"),
            ),
            self.assertRaises(HTTPException) as raised,
        ):
            openai_v1_chat_complete.handle(body)
        self.assertEqual(raised.exception.status_code, 400)

    def test_chat_response_format_maps_to_codex_text_controls(self) -> None:
        schema = {
            "type": "object",
            "properties": {"temperature": {"type": "number"}},
            "required": ["temperature"],
            "additionalProperties": False,
        }
        body = self._chat_body()
        body["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": "weather_result",
                "schema": schema,
                "strict": True,
            },
        }
        converted = openai_v1_chat_complete.chat_codex_response_body(body)
        self.assertEqual(converted["text"], {
            "format": {
                "type": "json_schema",
                "name": "weather_result",
                "schema": schema,
                "strict": True,
            },
        })
        self.assertEqual(
            openai_v1_response.codex_response_payload(converted)["text"],
            converted["text"],
        )

        plain = self._chat_body()
        plain["response_format"] = {"type": "text"}
        self.assertNotIn("text", openai_v1_chat_complete.chat_codex_response_body(plain))

        invalid_formats = (
            "json_schema",
            {"type": "json_object"},
            {"type": "text", "future_field": "x"},
            {"type": "json_schema"},
            {"type": "json_schema", "json_schema": {"name": "weather", "schema": schema, "description": "x"}},
            {"type": "json_schema", "json_schema": {"name": "weather", "schema": schema, "strict": 1}},
        )
        for response_format in invalid_formats:
            with self.subTest(response_format=response_format):
                invalid = self._chat_body()
                invalid["response_format"] = response_format
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.chat_codex_response_body(invalid)
                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_response_format_without_tools_uses_native_codex(self) -> None:
        schema = {
            "type": "object",
            "properties": {"answer": {"type": "string"}},
            "required": ["answer"],
            "additionalProperties": False,
        }
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "Return JSON"}],
            "modalities": ["text"],
            "n": 1,
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": "answer",
                    "schema": schema,
                    "strict": True,
                },
            },
            "verbosity": "high",
        }
        captured: dict[str, object] = {}

        def fake_codex(payload: dict[str, object]):
            captured.update(payload)
            return iter(web_search_events())

        with (
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("structured output must not use the web conversation backend"),
            ),
            mock.patch.object(
                openai_v1_chat_complete.openai_v1_response,
                "stream_codex_response",
                side_effect=fake_codex,
            ),
        ):
            response = openai_v1_chat_complete.handle(body)

        self.assertEqual(response["choices"][0]["message"]["content"], "Native search answer.")
        self.assertNotIn("modalities", captured)
        self.assertNotIn("n", captured)
        self.assertEqual(captured["text"], {
            "verbosity": "high",
            "format": {
                "type": "json_schema",
                "name": "answer",
                "schema": schema,
                "strict": True,
            },
        })

        for field, value in (("n", 2), ("modalities", ["audio"])):
            with self.subTest(field=field, value=value):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.chat_codex_response_body({**body, field: value})
                self.assertEqual(raised.exception.status_code, 400)


class NativeWebSearchContractTests(unittest.TestCase):
    def test_responses_web_search_uses_native_codex_stream(self) -> None:
        body = {
            "model": "auto",
            "input": "latest news",
            "tools": [{"type": "web_search_preview", "search_context_size": "high"}],
            "stream": True,
        }
        with (
            mock.patch.object(
                openai_v1_response,
                "stream_codex_response",
                return_value=iter(web_search_events()),
            ) as native,
        ):
            events = list(openai_v1_response.handle(body))

        native.assert_called_once_with(body)
        self.assertEqual(events[-1]["type"], "response.completed")

    def test_responses_web_search_tool_matches_official_shape(self) -> None:
        payload = openai_v1_response.codex_response_payload({
            "model": "auto",
            "input": "latest news",
            "tools": [{
                "type": "web_search_2025_08_26",
                "filters": {"allowed_domains": ["example.com"]},
                "search_context_size": "high",
                "search_content_types": ["text", "image"],
                "user_location": {"type": "approximate", "country": "CN"},
            }],
        })
        self.assertEqual(payload["tools"], [{
            "type": "web_search",
            "filters": {"allowed_domains": ["example.com"]},
            "search_context_size": "high",
            "search_content_types": ["text", "image"],
            "user_location": {"type": "approximate", "country": "CN"},
        }])

        preview = openai_v1_response.codex_response_payload({
            "model": "auto",
            "input": "latest images",
            "tools": [{
                "type": "web_search_preview",
                "search_content_types": ["text", "image"],
            }],
        })
        self.assertEqual(preview["tools"], [{
            "type": "web_search",
            "search_content_types": ["text", "image"],
        }])

        invalid_tools = (
            {"type": "web_search", "future_field": "must-not-reach-codex"},
            {"type": "web_search", "search_context_size": "ultra"},
            {"type": "web_search", "search_context_size": 1},
            {"type": "web_search", "filters": []},
            {"type": "web_search", "filters": {"future_field": "x"}},
            {"type": "web_search", "filters": {"allowed_domains": "example.com"}},
            {"type": "web_search", "filters": {"allowed_domains": ["example.com", 1]}},
            {"type": "web_search", "filters": {"allowed_domains": ["https://example.com"]}},
            {"type": "web_search", "filters": {"allowed_domains": ["example.com"] * 101}},
            {"type": "web_search", "filters": {"blocked_domains": ["example.com"]}},
            {"type": "web_search", "return_token_budget": "unlimited"},
            {"type": "web_search", "image_settings": {"max_results": 3}},
            {"type": "web_search", "external_web_access": "false"},
            {"type": "web_search", "external_web_access": 0},
            {"type": "web_search", "external_web_access": None},
            {"type": "web_search", "search_content_types": ["video"]},
            {"type": "web_search", "search_content_types": "text"},
            {"type": "web_search", "user_location": {"type": "precise", "country": "CN"}},
            {"type": "web_search", "user_location": {"type": "approximate", "country": 86}},
            {"type": "web_search", "user_location": {"type": "approximate", "country": "China"}},
            {"type": "web_search", "user_location": {"type": "approximate", "future_field": "x"}},
            {"type": "web_search_preview", "filters": {"allowed_domains": ["example.com"]}},
            {"type": "web_search_preview", "external_web_access": False},
            {"type": "web_search_preview", "search_content_types": ["video"]},
            {"type": "web_search_preview", "search_content_types": "text"},
        )
        for tool in invalid_tools:
            with self.subTest(tool=tool):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": "auto",
                        "input": "latest news",
                        "tools": [tool],
                    })
                self.assertEqual(raised.exception.status_code, 400)

    def test_responses_web_search_external_access_is_forwarded_as_a_boolean(self) -> None:
        for external_web_access in (False, True):
            with self.subTest(external_web_access=external_web_access):
                payload = openai_v1_response.codex_response_payload({
                    "model": "auto",
                    "input": "latest news",
                    "tools": [{
                        "type": "web_search",
                        "external_web_access": external_web_access,
                    }],
                })

                self.assertEqual(payload["tools"], [{
                    "type": "web_search",
                    "external_web_access": external_web_access,
                }])

    def test_responses_web_search_include_values_are_forwarded_with_reasoning_state(self) -> None:
        payload = openai_v1_response.codex_response_payload({
            "model": "auto",
            "input": "latest news and images",
            "tools": [{"type": "web_search"}],
            "include": [
                "web_search_call.action.sources",
                "web_search_call.results",
            ],
        })

        self.assertEqual(payload["include"], [
            "reasoning.encrypted_content",
            "web_search_call.action.sources",
            "web_search_call.results",
        ])

        for include in (
            "web_search_call.results",
            ["file_search_call.results"],
            ["web_search_call.results", 1],
        ):
            with self.subTest(include=include):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": "auto",
                        "input": "latest news",
                        "tools": [{"type": "web_search"}],
                        "include": include,
                    })
                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_web_search_options_are_mapped_to_native_codex_tool(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "latest news"}],
            "web_search_options": {
                "search_context_size": "high",
                "user_location": {
                    "type": "approximate",
                    "approximate": {"country": "CN"},
                },
            },
        }
        captured: dict[str, object] = {}

        def fake_native(converted):
            captured.update(converted)
            yield from web_search_events()

        with (
            mock.patch.object(openai_v1_chat_complete.openai_v1_response, "stream_codex_response", side_effect=fake_native),
            mock.patch.object(
                openai_v1_chat_complete,
                "run_web_search",
                side_effect=AssertionError("web_search_options must use Codex Responses"),
            ),
        ):
            response = openai_v1_chat_complete.handle(body)

        self.assertEqual(
            captured["tools"],
            [{
                "type": "web_search",
                "search_context_size": "high",
                "user_location": {
                    "type": "approximate",
                    "country": "CN",
                },
            }],
        )
        message = response["choices"][0]["message"]
        self.assertEqual(message["content"], "Native search answer.")
        self.assertEqual(message["annotations"][0]["url_citation"]["url"], "https://example.com/native")

    def test_chat_native_web_search_stream_preserves_citations_and_usage(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "latest news"}],
            "web_search_options": {"search_context_size": "medium"},
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(web_search_events()),
        ):
            chunks = list(openai_v1_chat_complete.handle(body))

        citation_chunks = [
            chunk
            for chunk in chunks
            if chunk["choices"] and chunk["choices"][0]["delta"].get("annotations")
        ]
        self.assertEqual(
            citation_chunks[0]["choices"][0]["delta"]["annotations"][0]["url_citation"]["url"],
            "https://example.com/native",
        )
        self.assertEqual(chunks[-1]["choices"], [])
        self.assertEqual(chunks[-1]["usage"]["total_tokens"], 7)

    def test_chat_web_search_options_reject_values_outside_official_shape(self) -> None:
        invalid_options = (
            {"future_field": "must-not-reach-codex"},
            {"search_context_size": "ultra"},
            {"search_context_size": 1},
            {"search_context_size": None},
            {"user_location": {"type": "approximate", "country": "CN"}},
            {"user_location": {"type": "precise", "approximate": {"country": "CN"}}},
            {"user_location": {"type": "approximate"}},
            {"user_location": {"type": "approximate", "approximate": {"future_field": "x"}}},
            {"user_location": {"type": "approximate", "approximate": {"country": 86}}},
        )
        for options in invalid_options:
            with self.subTest(options=options):
                body = {
                    "model": "auto",
                    "messages": [{"role": "user", "content": "latest news"}],
                    "web_search_options": options,
                }

                with self.assertRaises(HTTPException) as raised:
                    openai_v1_chat_complete.chat_codex_response_body(body)

                self.assertEqual(raised.exception.status_code, 400)


class IncompleteResponseContractTests(unittest.TestCase):
    @staticmethod
    def _terminal_event(event_type: str = "response.completed") -> dict[str, object]:
        status = "incomplete" if event_type == "response.incomplete" else "completed"
        return {
            "type": event_type,
            "response": {
                "id": "resp_terminal",
                "object": "response",
                "created_at": 123,
                "model": "gpt-5.5",
                "status": status,
                "output": [],
                "usage": {"input_tokens": 2, "output_tokens": 1, "total_tokens": 3},
            },
        }

    def test_responses_non_stream_returns_structured_incomplete_response(self) -> None:
        body = {"model": "auto", "input": "hello", "tools": [FUNCTION_TOOL]}
        with mock.patch.object(
            openai_v1_response,
            "stream_codex_response",
            return_value=iter(incomplete_events()),
        ):
            response = openai_v1_response.handle(body)

        self.assertEqual(response["status"], "incomplete")
        self.assertEqual(response["incomplete_details"]["reason"], "max_output_tokens")

    def test_chat_non_stream_maps_incomplete_to_length_finish_reason(self) -> None:
        body = ChatFunctionToolContractTests._chat_body()
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(incomplete_events()),
        ):
            response = openai_v1_chat_complete.handle(body)

        self.assertEqual(response["choices"][0]["finish_reason"], "length")
        self.assertEqual(response["choices"][0]["message"]["content"], "partial")

    def test_chat_stream_maps_incomplete_to_terminal_length_chunk(self) -> None:
        body = ChatFunctionToolContractTests._chat_body(stream=True, include_usage=True)
        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(incomplete_events()),
        ):
            chunks = list(openai_v1_chat_complete.handle(body))

        self.assertEqual(chunks[-2]["choices"][0]["finish_reason"], "length")
        self.assertEqual(chunks[-1]["usage"]["total_tokens"], 3)

    def test_terminal_event_type_must_match_response_status(self) -> None:
        for event_type, status in (
            ("response.completed", "incomplete"),
            ("response.incomplete", "completed"),
            ("response.completed", "failed"),
        ):
            with self.subTest(event_type=event_type, status=status):
                event = self._terminal_event(event_type)
                event["response"]["status"] = status

                with mock.patch.object(openai_v1_response, "response_events", return_value=iter([event])):
                    with self.assertRaisesRegex(RuntimeError, "malformed terminal response"):
                        openai_v1_response.handle({"model": "auto", "input": "hello"})

                for stream in (False, True):
                    body = ChatFunctionToolContractTests._chat_body(stream=stream, include_usage=stream)
                    with mock.patch.object(
                        openai_v1_chat_complete.openai_v1_response,
                        "stream_codex_response",
                        return_value=iter([event]),
                    ):
                        with self.assertRaisesRegex(RuntimeError, "malformed terminal response"):
                            result = openai_v1_chat_complete.handle(body)
                            if stream:
                                list(result)

    def test_terminal_response_rejects_malformed_public_scalars(self) -> None:
        invalid_fields = (
            ("id", {"coerced": True}),
            ("id", ""),
            ("model", ["coerced"]),
            ("created_at", "123"),
            ("created_at", True),
            ("output", {"coerced": True}),
        )
        for field, value in invalid_fields:
            with self.subTest(field=field, value=value):
                event = self._terminal_event()
                event["response"][field] = value

                with mock.patch.object(openai_v1_response, "response_events", return_value=iter([event])):
                    with self.assertRaisesRegex(RuntimeError, "malformed terminal response"):
                        openai_v1_response.handle({"model": "auto", "input": "hello"})

                with mock.patch.object(
                    openai_v1_chat_complete.openai_v1_response,
                    "stream_codex_response",
                    return_value=iter([event]),
                ):
                    with self.assertRaisesRegex(RuntimeError, "malformed terminal response"):
                        openai_v1_chat_complete.handle(ChatFunctionToolContractTests._chat_body())

    def test_chat_stream_rejects_malformed_response_created_scalars(self) -> None:
        invalid_fields = (
            ("id", {"coerced": True}),
            ("id", ""),
            ("model", ["coerced"]),
            ("created_at", "123"),
            ("created_at", True),
            ("output", {"coerced": True}),
        )
        terminal = self._terminal_event()
        body = ChatFunctionToolContractTests._chat_body(stream=True)

        for field, value in invalid_fields:
            with self.subTest(field=field, value=value):
                created = {
                    "type": "response.created",
                    "response": {
                        "id": "resp_created",
                        "status": "in_progress",
                        field: value,
                    },
                }
                with mock.patch.object(
                    openai_v1_chat_complete.openai_v1_response,
                    "stream_codex_response",
                    return_value=iter([created, terminal]),
                ):
                    with self.assertRaisesRegex(RuntimeError, "malformed response.created"):
                        list(openai_v1_chat_complete.handle(body))

    def test_terminal_status_rejects_inconsistent_error_and_incomplete_details(self) -> None:
        secret = "opaque-terminal-detail-secret"
        invalid_cases = (
            ("response.completed", "error", {"code": "opaque", "message": secret}),
            ("response.completed", "incomplete_details", {"reason": "max_output_tokens"}),
            ("response.incomplete", "error", {"code": "opaque", "message": secret}),
            ("response.incomplete", "incomplete_details", "max_output_tokens"),
            ("response.incomplete", "incomplete_details", {"reason": secret}),
            ("response.incomplete", "incomplete_details", {"reason": True}),
        )

        for event_type, field, value in invalid_cases:
            with self.subTest(event_type=event_type, field=field, value=value):
                event = self._terminal_event(event_type)
                event["response"][field] = value

                with mock.patch.object(openai_v1_response, "response_events", return_value=iter([event])):
                    with self.assertRaisesRegex(RuntimeError, "malformed terminal response") as raised:
                        openai_v1_response.handle({"model": "auto", "input": "hello"})
                self.assertNotIn(secret, str(raised.exception))

                body = ChatFunctionToolContractTests._chat_body()
                with mock.patch.object(
                    openai_v1_chat_complete.openai_v1_response,
                    "stream_codex_response",
                    return_value=iter([event]),
                ):
                    with self.assertRaisesRegex(RuntimeError, "malformed terminal response") as raised:
                        openai_v1_chat_complete.handle(body)
                self.assertNotIn(secret, str(raised.exception))

    def test_incomplete_details_projects_only_the_official_reason(self) -> None:
        event = self._terminal_event("response.incomplete")
        event["response"]["incomplete_details"] = {
            "reason": "content_filter",
            "opaque_internal_detail": "private-detail",
        }

        with mock.patch.object(openai_v1_response, "response_events", return_value=iter([event])):
            response = openai_v1_response.handle({"model": "auto", "input": "hello"})

        self.assertEqual(response["incomplete_details"], {"reason": "content_filter"})
        self.assertNotIn("private-detail", json.dumps(response, ensure_ascii=False))

    def test_non_stream_terminal_response_drops_unknown_public_fields(self) -> None:
        canary = "terminal-response-internal-canary"
        event = self._terminal_event()
        event["response"]["future_response_field"] = canary
        event["response"]["metadata"] = {"nested": canary}

        with mock.patch.object(openai_v1_response, "response_events", return_value=iter([event])):
            response = openai_v1_response.handle({"model": "auto", "input": "hello"})

        self.assertNotIn("future_response_field", response)
        self.assertNotIn("metadata", response)
        self.assertNotIn(canary, json.dumps(response, ensure_ascii=False))


class ToolRequestValidationContractTests(unittest.TestCase):
    def test_chat_stream_honors_official_obfuscation_opt_out(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "hello"}],
            "stream": True,
            "stream_options": {
                "include_usage": True,
                "include_obfuscation": False,
            },
        }
        with (
            mock.patch.object(openai_v1_chat_complete, "text_backend", return_value=object()),
            mock.patch.object(openai_v1_chat_complete, "stream_text_deltas", return_value=iter(["hello"])),
        ):
            chunks = list(openai_v1_chat_complete.handle(body))

        self.assertEqual(chunks[-1]["choices"], [])
        self.assertNotIn("obfuscation", json.dumps(chunks))

    def test_responses_stream_honors_official_obfuscation_opt_out(self) -> None:
        body = {
            "model": "auto",
            "input": "hello",
            "stream": True,
            "stream_options": {"include_obfuscation": False},
        }
        with (
            mock.patch.object(openai_v1_response, "text_backend", return_value=object()),
            mock.patch.object(openai_v1_response, "stream_text_deltas", return_value=iter(["hello"])),
        ):
            events = list(openai_v1_response.handle(body))

        self.assertEqual(events[-1]["type"], "response.completed")
        self.assertNotIn("obfuscation", json.dumps(events))

    def test_codex_responses_consumes_obfuscation_opt_out_without_upstream_leak(self) -> None:
        payload = openai_v1_response.codex_response_payload({
            "model": "auto",
            "input": "hello",
            "stream": True,
            "stream_options": {"include_obfuscation": False},
            "tools": [FUNCTION_TOOL],
        })

        self.assertNotIn("stream_options", payload)

    def test_stream_obfuscation_enablement_fails_before_backend_selection(self) -> None:
        with (
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=AssertionError("unsupported obfuscation must not select a backend"),
            ),
            self.assertRaises(HTTPException),
        ):
            openai_v1_chat_complete.handle({
                "model": "auto",
                "messages": [{"role": "user", "content": "hello"}],
                "stream": True,
                "stream_options": {"include_obfuscation": True},
            })

        with (
            mock.patch.object(
                openai_v1_response,
                "text_backend",
                side_effect=AssertionError("unsupported obfuscation must not select a backend"),
            ),
            self.assertRaises(HTTPException),
        ):
            list(openai_v1_response.handle({
                "model": "auto",
                "input": "hello",
                "stream": True,
                "stream_options": {"include_obfuscation": True},
            }))

    def test_context_management_routes_plain_responses_through_codex_and_preserves_payload(self) -> None:
        context_management = [{"type": "compaction", "compact_threshold": 200000}]
        body = {
            "model": "gpt-5.3-codex",
            "input": "continue the long coding task",
            "store": False,
            "context_management": context_management,
        }
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_compacted", "status": "completed", "output": []},
        }

        with (
            mock.patch.object(
                openai_v1_response,
                "stream_codex_response",
                return_value=iter([terminal]),
            ) as native,
            mock.patch.object(openai_v1_response, "text_backend") as legacy_text,
        ):
            events = list(openai_v1_response.response_events(body))

        self.assertEqual(events, [terminal])
        native.assert_called_once_with(body)
        legacy_text.assert_not_called()
        payload = openai_v1_response.codex_response_payload(body)
        self.assertEqual(payload["context_management"], context_management)
        self.assertIsNot(payload["context_management"], context_management)

    def test_native_response_fields_route_plain_input_through_codex_without_tools(self) -> None:
        body = {
            "model": "gpt-5.3-codex",
            "input": "answer briefly",
            "include": ["reasoning.encrypted_content"],
            "prompt_cache_key": "plain-session-42",
            "service_tier": "priority",
            "text": {"verbosity": "low"},
        }
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_plain_native", "status": "completed", "output": []},
        }

        with (
            mock.patch.object(
                openai_v1_response,
                "stream_codex_response",
                return_value=iter([terminal]),
            ) as native,
            mock.patch.object(
                openai_v1_response,
                "text_backend",
                side_effect=AssertionError("native response fields must not use the plain conversation backend"),
            ),
        ):
            events = list(openai_v1_response.response_events(body))

        self.assertEqual(events, [terminal])
        native.assert_called_once_with(body)
        payload = openai_v1_response.codex_response_payload(body)
        self.assertEqual(payload["include"], ["reasoning.encrypted_content"])
        self.assertEqual(payload["prompt_cache_key"], "plain-session-42")
        self.assertEqual(payload["service_tier"], "priority")
        self.assertEqual(payload["text"], {"verbosity": "low"})

    def test_nested_native_response_controls_route_without_tools(self) -> None:
        terminal = {
            "type": "response.completed",
            "response": {"id": "resp_native_control", "status": "completed", "output": []},
        }
        controls = (
            {"reasoning": {"summary": "auto"}},
            {"reasoning": {"context": "all_turns"}},
            {
                "stream": True,
                "stream_options": {"reasoning_summary_delivery": "sequential_cutoff"},
            },
            {"tool_choice": "auto"},
        )

        for control in controls:
            body = {"model": "gpt-5.3-codex", "input": "answer briefly", **control}
            with self.subTest(control=control):
                with (
                    mock.patch.object(
                        openai_v1_response,
                        "stream_codex_response",
                        return_value=iter([terminal]),
                    ) as native,
                    mock.patch.object(
                        openai_v1_response,
                        "text_backend",
                        side_effect=AssertionError("native controls must not use the plain conversation backend"),
                    ),
                ):
                    events = list(openai_v1_response.response_events(body))

                self.assertEqual(events, [terminal])
                native.assert_called_once_with(body)
                openai_v1_response.codex_response_payload(body)

    def test_context_management_matches_official_compaction_schema(self) -> None:
        base = {"model": "gpt-5.3-codex", "input": "hello", "tools": [FUNCTION_TOOL]}
        valid_values = (
            [],
            [{"type": "compaction"}],
            [{"type": "compaction", "compact_threshold": None}],
            [{"type": "compaction", "compact_threshold": 1000}],
            [{"type": "compaction", "compact_threshold": 1000.5}],
        )
        for context_management in valid_values:
            with self.subTest(valid=context_management):
                payload = openai_v1_response.codex_response_payload({
                    **base,
                    "context_management": context_management,
                })
                self.assertEqual(payload["context_management"], context_management)

        invalid_values = (
            {},
            "compaction",
            [None],
            [{}],
            [{"type": 1}],
            [{"type": "other"}],
            [{"type": "compaction", "unknown": "value"}],
            [{"type": "compaction", "compact_threshold": True}],
            [{"type": "compaction", "compact_threshold": "1000"}],
            [{"type": "compaction", "compact_threshold": 999}],
            [{"type": "compaction", "compact_threshold": float("nan")}],
            [{"type": "compaction", "compact_threshold": float("inf")}],
        )
        for context_management in invalid_values:
            with self.subTest(invalid=context_management):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        **base,
                        "context_management": context_management,
                    })
                self.assertEqual(raised.exception.status_code, 400)

    def test_context_management_does_not_divert_image_requests_to_native_codex(self) -> None:
        body = {
            "model": "gpt-image-2",
            "input": "draw a cat",
            "tools": [{"type": "image_generation"}],
            "context_management": [{"type": "compaction", "compact_threshold": 200000}],
        }

        with mock.patch.object(openai_v1_response, "stream_codex_response") as native:
            with self.assertRaises(HTTPException) as raised:
                list(openai_v1_response.response_events(body))

        self.assertEqual(raised.exception.status_code, 400)
        native.assert_not_called()

    def test_codex_defaults_tool_choice_to_auto_like_official_client(self) -> None:
        response_payload = openai_v1_response.codex_response_payload({
            "model": "auto",
            "input": "hello",
            "tools": [FUNCTION_TOOL],
        })
        chat_body = ChatFunctionToolContractTests._chat_body()
        chat_body.pop("tool_choice")
        converted_chat = openai_v1_chat_complete.chat_codex_response_body(chat_body)
        chat_payload = openai_v1_response.codex_response_payload(converted_chat)

        self.assertEqual(response_payload["tool_choice"], "auto")
        self.assertEqual(converted_chat["tool_choice"], "auto")
        self.assertEqual(chat_payload["tool_choice"], "auto")

    def test_codex_rejects_tool_choice_shapes_not_emitted_by_fixed_upstream(self) -> None:
        unsupported_choices = (
            "none",
            "required",
            {"type": "function", "name": "get_weather"},
            {
                "type": "allowed_tools",
                "mode": "auto",
                "tools": [{"type": "function", "name": "get_weather"}],
            },
        )
        for tool_choice in unsupported_choices:
            with self.subTest(tool_choice=tool_choice):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({
                        "model": "auto",
                        "input": "hello",
                        "tools": [FUNCTION_TOOL],
                        "tool_choice": tool_choice,
                    })
                self.assertEqual(raised.exception.status_code, 400)

    def test_codex_parallel_tool_calls_boolean_is_forwarded(self) -> None:
        for parallel_tool_calls in (False, True):
            with self.subTest(api="responses", parallel_tool_calls=parallel_tool_calls):
                payload = openai_v1_response.codex_response_payload({
                    "model": "auto",
                    "input": "hello",
                    "tools": [FUNCTION_TOOL],
                    "parallel_tool_calls": parallel_tool_calls,
                })
                self.assertIs(payload["parallel_tool_calls"], parallel_tool_calls)

            with self.subTest(api="chat", parallel_tool_calls=parallel_tool_calls):
                chat_body = ChatFunctionToolContractTests._chat_body()
                chat_body["parallel_tool_calls"] = parallel_tool_calls
                converted_chat = openai_v1_chat_complete.chat_codex_response_body(chat_body)
                self.assertIs(converted_chat["parallel_tool_calls"], parallel_tool_calls)
                payload = openai_v1_response.codex_response_payload(converted_chat)
                self.assertIs(payload["parallel_tool_calls"], parallel_tool_calls)

    def test_codex_service_tier_matches_official_fast_flex_and_default_contract(self) -> None:
        base = {"model": "auto", "input": "hello", "tools": [FUNCTION_TOOL]}

        self.assertEqual(
            openai_v1_response.codex_response_payload({**base, "service_tier": "priority"})[
                "service_tier"
            ],
            "priority",
        )
        self.assertEqual(
            openai_v1_response.codex_response_payload({**base, "service_tier": "flex"})[
                "service_tier"
            ],
            "flex",
        )
        self.assertNotIn(
            "service_tier",
            openai_v1_response.codex_response_payload({**base, "service_tier": "default"}),
        )

    def test_native_responses_reasoning_validates_and_preserves_supported_fields(self) -> None:
        base = {
            "model": "auto",
            "input": "hello",
            "tools": [FUNCTION_TOOL],
        }
        payload = openai_v1_response.codex_response_payload({
            **base,
            "reasoning": {
                "effort": "minimal",
                "summary": "detailed",
                "context": "all_turns",
            },
        })
        self.assertEqual(
            payload["reasoning"],
            {"effort": "minimal", "summary": "detailed", "context": "all_turns"},
        )
        for effort in ("ultra", "model-defined-effort"):
            with self.subTest(effort=effort):
                custom_payload = openai_v1_response.codex_response_payload({
                    **base,
                    "reasoning": {"effort": effort},
                })
                self.assertEqual(custom_payload["reasoning"], {"effort": effort})

        invalid_reasoning = (
            "high",
            [],
            {"future_field": "must-not-reach-codex"},
            {"effort": ""},
            {"effort": 1},
            {"summary": "verbose"},
            {"summary": 1},
            {"context": "future_context"},
            {"context": 1},
        )
        for reasoning in invalid_reasoning:
            with self.subTest(reasoning=reasoning):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({**base, "reasoning": reasoning})
                self.assertEqual(raised.exception.status_code, 400)

    def test_codex_rejects_public_metadata_that_upstream_cannot_honor(self) -> None:
        metadata = {"tenant": "test", "trace": "42"}
        response_body = {
            "model": "auto",
            "input": "hello",
            "tools": [FUNCTION_TOOL],
            "metadata": metadata,
        }
        chat_body = ChatFunctionToolContractTests._chat_body()
        chat_body["metadata"] = metadata

        with self.assertRaises(HTTPException) as response_error:
            openai_v1_response.codex_response_payload(response_body)
        with self.assertRaises(HTTPException) as chat_error:
            openai_v1_chat_complete.chat_codex_response_body(chat_body)
        self.assertEqual(response_error.exception.status_code, 400)
        self.assertEqual(chat_error.exception.status_code, 400)

    def test_codex_rejects_max_tool_calls_missing_from_upstream_request(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            openai_v1_response.codex_response_payload({
                "model": "auto",
                "input": "hello",
                "tools": [FUNCTION_TOOL],
                "max_tool_calls": 1,
            })

        self.assertEqual(raised.exception.status_code, 400)

    def test_native_responses_core_fields_reject_json_type_coercion(self) -> None:
        base = {
            "model": "auto",
            "instructions": "answer briefly",
            "input": "hello",
            "tools": [FUNCTION_TOOL],
            "parallel_tool_calls": True,
            "store": False,
            "stream": True,
            "prompt_cache_key": "session-42",
        }
        payload = openai_v1_response.codex_response_payload(base)
        self.assertEqual(payload["model"], CODEX_RESPONSES_MODEL)
        self.assertEqual(payload["instructions"], "answer briefly")
        self.assertEqual(payload["prompt_cache_key"], "session-42")

        invalid_fields = (
            ("model", 42),
            ("instructions", {}),
            ("parallel_tool_calls", 1),
            ("store", 0),
            ("stream", "true"),
            ("prompt_cache_key", 42),
        )
        for field, value in invalid_fields:
            with self.subTest(field=field, value=value):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({**base, field: value})
                self.assertEqual(raised.exception.status_code, 400)

    def test_native_responses_text_controls_match_codex_request_shape(self) -> None:
        base = {
            "model": "auto",
            "input": "return weather JSON",
            "tools": [FUNCTION_TOOL],
        }
        schema = {
            "type": "object",
            "properties": {"temperature": {"type": "number"}},
            "required": ["temperature"],
            "additionalProperties": False,
        }
        payload = openai_v1_response.codex_response_payload({
            **base,
            "text": {
                "verbosity": "low",
                "format": {
                    "type": "json_schema",
                    "name": "weather_result",
                    "schema": schema,
                    "strict": True,
                },
            },
        })
        self.assertEqual(
            payload["text"],
            {
                "verbosity": "low",
                "format": {
                    "type": "json_schema",
                    "name": "weather_result",
                    "schema": schema,
                    "strict": True,
                },
            },
        )
        plain_payload = openai_v1_response.codex_response_payload({
            **base,
            "text": {"format": {"type": "text"}},
        })
        self.assertNotIn("text", plain_payload)

        invalid_text = (
            "low",
            {"future_field": "x"},
            {"verbosity": "verbose"},
            {"verbosity": 1},
            {"format": "json_schema"},
            {"format": {"type": "json_object"}},
            {"format": {"type": "text", "name": "unexpected"}},
            {"format": {"type": "json_schema", "schema": schema}},
            {"format": {"type": "json_schema", "name": "weather_result"}},
            {"format": {"type": "json_schema", "name": "bad name", "schema": schema}},
            {"format": {"type": "json_schema", "name": "x" * 65, "schema": schema}},
            {"format": {"type": "json_schema", "name": "weather", "schema": []}},
            {"format": {"type": "json_schema", "name": "weather", "schema": schema, "strict": 1}},
            {
                "format": {
                    "type": "json_schema",
                    "name": "weather",
                    "schema": schema,
                    "description": "not represented by Codex TextFormat",
                },
            },
        )
        for text in invalid_text:
            with self.subTest(text=text):
                with self.assertRaises(HTTPException) as raised:
                    openai_v1_response.codex_response_payload({**base, "text": text})
                self.assertEqual(raised.exception.status_code, 400)

    def test_reasoning_and_stream_options_are_never_silently_ignored(self) -> None:
        plain_cases = (
            (
                openai_v1_chat_complete,
                {
                    "model": "auto",
                    "messages": [{"role": "user", "content": "hello"}],
                    "stream": False,
                    "stream_options": {"include_usage": True},
                },
            ),
            (
                openai_v1_response,
                {
                    "model": "auto",
                    "input": "hello",
                    "reasoning": {"effort": "high", "future_field": "ignored"},
                },
            ),
        )
        for module, body in plain_cases:
            with self.subTest(module=module.__name__, body=body):
                with (
                    mock.patch.object(
                        module,
                        "text_backend",
                        side_effect=AssertionError("invalid parameters must not select a backend"),
                    ),
                    self.assertRaises(HTTPException) as raised,
                ):
                    module.handle(body)
                self.assertEqual(raised.exception.status_code, 400)

        for effort in ("ultra", "model-defined-effort"):
            with self.subTest(effort=effort):
                native_body = ChatFunctionToolContractTests._chat_body()
                native_body["reasoning_effort"] = effort
                converted = openai_v1_chat_complete.chat_codex_response_body(native_body)
                self.assertEqual(converted["reasoning"], {"effort": effort})

    def test_tool_and_image_paths_reject_parameters_they_would_ignore(self) -> None:
        chat_body = ChatFunctionToolContractTests._chat_body()
        chat_body["future_parameter"] = "ignored"
        image_body = {
            "model": "gpt-image-2",
            "input": "draw a cat",
            "tools": [{"type": "image_generation"}],
            "future_parameter": "ignored",
        }
        image_tool_body = {
            "model": "gpt-image-2",
            "input": "draw a cat",
            "tools": [{"type": "image_generation", "partial_images": 2}],
        }

        with (
            mock.patch.object(
                openai_v1_response,
                "stream_codex_response",
                side_effect=AssertionError("invalid Chat parameters must not reach Codex"),
            ),
            self.assertRaises(HTTPException) as chat_error,
        ):
            openai_v1_chat_complete.handle(chat_body)
        self.assertEqual(chat_error.exception.status_code, 400)

        for body in (image_body, image_tool_body):
            with self.subTest(body=body):
                with (
                    mock.patch.object(
                        openai_v1_response,
                        "stream_image_outputs_with_pool",
                        side_effect=AssertionError("invalid image parameters must not select an account"),
                    ),
                    self.assertRaises(HTTPException) as image_error,
                ):
                    openai_v1_response.handle(body)
                self.assertEqual(image_error.exception.status_code, 400)

    def test_chat_image_path_rejects_parameters_it_would_ignore(self) -> None:
        base = {
            "model": "gpt-image-2",
            "messages": [{"role": "user", "content": "draw a cat"}],
        }
        for extra in (
            {"future_parameter": "ignored"},
            {"quality": "high"},
            {"stream": False, "stream_options": {"include_usage": True}},
        ):
            body = {**base, **extra}
            with self.subTest(extra=extra):
                with (
                    mock.patch.object(
                        openai_v1_chat_complete,
                        "stream_image_outputs_with_pool",
                        side_effect=AssertionError("invalid parameters must not select an image account"),
                    ),
                    self.assertRaises(HTTPException) as raised,
                ):
                    openai_v1_chat_complete.handle(body)
                self.assertEqual(raised.exception.status_code, 400)

    def test_plain_text_paths_reject_parameters_the_backend_would_ignore(self) -> None:
        cases = (
            (
                openai_v1_response,
                {"model": "auto", "input": "hello", "temperature": 0.2},
            ),
            (
                openai_v1_response,
                {"model": "auto", "input": "hello", "previous_response_id": "resp_other"},
            ),
            (
                openai_v1_response,
                {"model": "auto", "input": "hello", "future_parameter": "ignored"},
            ),
            (
                openai_v1_chat_complete,
                {
                    "model": "auto",
                    "messages": [{"role": "user", "content": "hello"}],
                    "temperature": 0.2,
                },
            ),
            (
                openai_v1_chat_complete,
                {
                    "model": "auto",
                    "messages": [{"role": "user", "content": "hello"}],
                    "max_completion_tokens": 32,
                },
            ),
            (
                openai_v1_chat_complete,
                {
                    "model": "auto",
                    "messages": [{"role": "user", "content": "hello"}],
                    "future_parameter": "ignored",
                },
            ),
        )
        for module, body in cases:
            with self.subTest(module=module.__name__, field=next(reversed(body))):
                with (
                    mock.patch.object(
                        module,
                        "text_backend",
                        side_effect=AssertionError("invalid parameters must be rejected before backend selection"),
                    ),
                    self.assertRaises(HTTPException) as raised,
                ):
                    module.handle(body)
                self.assertEqual(raised.exception.status_code, 400)

    def test_malformed_tool_containers_never_fall_back_to_plain_text(self) -> None:
        cases = (
            (openai_v1_response, {"model": "auto", "input": "hello", "tools": "not-an-array"}),
            (openai_v1_response, {"model": "auto", "input": "hello", "tools": ["not-an-object"]}),
            (
                openai_v1_chat_complete,
                {"model": "auto", "messages": [{"role": "user", "content": "hello"}], "tools": "not-an-array"},
            ),
            (
                openai_v1_chat_complete,
                {"model": "auto", "messages": [{"role": "user", "content": "hello"}], "tools": ["not-an-object"]},
            ),
        )
        for module, body in cases:
            for stream in (False, True):
                request = {**body, "stream": stream}
                with self.subTest(module=module.__name__, tools=body["tools"], stream=stream):
                    with (
                        mock.patch.object(
                            module,
                            "text_backend",
                            side_effect=AssertionError("invalid tools must not fall back to plain text"),
                        ),
                        self.assertRaises(HTTPException) as raised,
                    ):
                        module.handle(request)
                    self.assertEqual(raised.exception.status_code, 400)

    def test_responses_null_tools_is_treated_as_omitted(self) -> None:
        completed = {
            "type": "response.completed",
            "response": {
                "id": "resp_text",
                "object": "response",
                "status": "completed",
                "output": [],
            },
        }
        with mock.patch.object(
            openai_v1_response,
            "response_events",
            return_value=iter([completed]),
        ) as events:
            response = openai_v1_response.handle(
                {"model": "auto", "input": "hello", "tools": None}
            )

        self.assertEqual(response["id"], "resp_text")
        events.assert_called_once_with({"model": "auto", "input": "hello", "tools": None})


if __name__ == "__main__":
    unittest.main()
