from __future__ import annotations

import asyncio
import threading
import unittest
from unittest import mock
import json
import base64
import tempfile
from pathlib import Path

from fastapi import HTTPException

from services.config import config
from services.account_service import AccountService
from services.log_service import run_ai_in_threadpool
from services.protocol import openai_v1_chat_complete, openai_v1_response
import services.protocol.chat_completion_cache as cache_module
from services.protocol.chat_completion_cache import cache_key, chat_completion_cache
from services.protocol.conversation import iter_conversation_payloads, sanitize_output_text
from services.storage.json_storage import JSONStorageBackend
from utils.helper import extract_image_from_message_content


PNG_1X1 = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADUlEQVR4nGP8z8BQDwAFgwJ/luzl4wAAAABJRU5ErkJggg=="
)
PNG_1X1_DATA_URL = "data:image/png;base64," + base64.b64encode(PNG_1X1).decode("ascii")


def native_search_events(text: str, url: str) -> list[dict[str, object]]:
    search_item = {
        "id": "ws_1",
        "type": "web_search_call",
        "status": "completed",
        "action": {"type": "search", "query": "query"},
    }
    message = {
        "id": "msg_1",
        "type": "message",
        "role": "assistant",
        "status": "completed",
        "content": [{
            "type": "output_text",
            "text": text,
            "annotations": [{
                "type": "url_citation",
                "url": url,
                "title": "Example",
                "start_index": 0,
                "end_index": len(text),
            }],
        }],
    }
    response = {
        "id": "resp_search",
        "object": "response",
        "created_at": 123,
        "model": "gpt-5.5",
        "status": "completed",
        "output": [search_item, message],
        "usage": {"input_tokens": 3, "output_tokens": 4, "total_tokens": 7},
    }
    return [
        {"type": "response.created", "response": {"id": "resp_search", "model": "gpt-5.5"}},
        {"type": "response.output_item.done", "output_index": 0, "item": search_item},
        {"type": "response.output_text.delta", "item_id": "msg_1", "delta": text},
        {"type": "response.output_item.done", "output_index": 1, "item": message},
        {"type": "response.completed", "response": response},
    ]


class ChatCompletionCacheTests(unittest.TestCase):
    def setUp(self) -> None:
        self.old_cache_settings = config.data.get("chat_completion_cache")
        config.data["chat_completion_cache"] = {
            "enabled": True,
            "ttl_seconds": 60,
            "max_entries": 32,
            "dedupe_inflight": True,
            "stream_cache": True,
            "normalize_messages": True,
            "drop_adjacent_duplicates": True,
            "drop_assistant_history": False,
        }
        chat_completion_cache.clear()

    def tearDown(self) -> None:
        if self.old_cache_settings is None:
            config.data.pop("chat_completion_cache", None)
        else:
            config.data["chat_completion_cache"] = self.old_cache_settings
        chat_completion_cache.clear()

    def test_repeated_non_stream_text_completion_uses_cache(self) -> None:
        calls = 0

        def fake_collect_text(_backend, _request):
            nonlocal calls
            calls += 1
            return f"cached answer {calls}"

        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "cache this exact prompt"}],
        }

        with (
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_chat_complete.collect_text", side_effect=fake_collect_text),
        ):
            first = openai_v1_chat_complete.handle(body)
            second = openai_v1_chat_complete.handle(body)

        self.assertEqual(calls, 1)
        self.assertEqual(
            first["choices"][0]["message"]["content"],
            second["choices"][0]["message"]["content"],
        )

    def test_route_account_rotation_does_not_reuse_old_completion(self) -> None:
        class Backend:
            def __init__(self, answer: str) -> None:
                self.answer = answer

        backends = [Backend("answer-a"), Backend("answer-b")]

        def fake_collect_text(backend, _request):
            return backend.answer

        body = {
            "model": "gpt-test",
            "messages": [{"role": "user", "content": "same prompt"}],
        }
        selected_tokens = iter(("account-a", "account-b"))
        with (
            mock.patch.object(
                openai_v1_chat_complete.account_service,
                "get_text_access_token",
                side_effect=lambda **_kwargs: next(selected_tokens),
            ),
            mock.patch.object(
                openai_v1_chat_complete,
                "text_backend",
                side_effect=lambda _model, **_kwargs: backends.pop(0),
            ),
            mock.patch.object(openai_v1_chat_complete, "collect_text", side_effect=fake_collect_text),
        ):
            first = openai_v1_chat_complete.handle(body, cache_scope="user-key")
            second = openai_v1_chat_complete.handle(body, cache_scope="user-key")

        self.assertEqual(first["choices"][0]["message"]["content"], "answer-a")
        self.assertEqual(second["choices"][0]["message"]["content"], "answer-b")

    def test_same_token_account_replacement_does_not_reuse_old_completion(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            accounts = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            accounts.add_account_items([{
                "access_token": "same-token",
                "type": "free",
                "status": "正常",
                "quota": 1,
            }])

            calls = 0

            def fake_collect_text(_backend, _request):
                nonlocal calls
                calls += 1
                return f"answer-{calls}"

            def cache_scope_for(token: str) -> str:
                resolved, account = accounts._get_account_lease(token)
                return f"{resolved}:owner-{id(account)}"

            body = {
                "model": "gpt-test",
                "messages": [{"role": "user", "content": "same prompt"}],
            }
            with (
                mock.patch.object(
                    openai_v1_chat_complete.account_service,
                    "get_text_access_token",
                    return_value="same-token",
                ),
                mock.patch.object(
                    openai_v1_chat_complete.account_service,
                    "get_account_cache_scope",
                    side_effect=cache_scope_for,
                    create=True,
                ),
                mock.patch.object(
                    openai_v1_chat_complete,
                    "text_backend",
                    return_value=object(),
                ),
                mock.patch.object(
                    openai_v1_chat_complete,
                    "collect_text",
                    side_effect=fake_collect_text,
                ),
            ):
                first = openai_v1_chat_complete.handle(body, cache_scope="user-key")
                accounts.delete_accounts(["same-token"])
                accounts.add_account_items([{
                    "access_token": "same-token",
                    "type": "free",
                    "status": "正常",
                    "quota": 9,
                }])
                second = openai_v1_chat_complete.handle(body, cache_scope="user-key")

            self.assertEqual(first["choices"][0]["message"]["content"], "answer-1")
            self.assertEqual(second["choices"][0]["message"]["content"], "answer-2")
            self.assertEqual(calls, 2)

    def test_account_cache_scope_changes_after_same_token_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            accounts = AccountService(JSONStorageBackend(Path(temp_dir) / "accounts.json"))
            accounts.add_account_items([{
                "access_token": "same-token",
                "type": "free",
                "status": "正常",
                "quota": 1,
            }])
            first_scope = accounts.get_account_cache_scope("same-token")

            accounts.delete_accounts(["same-token"])
            accounts.add_account_items([{
                "access_token": "same-token",
                "type": "free",
                "status": "正常",
                "quota": 9,
            }])

            self.assertTrue(first_scope)
            self.assertNotEqual(first_scope, accounts.get_account_cache_scope("same-token"))

    def test_response_cache_binds_to_selected_account(self) -> None:
        class Backend:
            def __init__(self, answer: str) -> None:
                self.answer = answer

        selected_tokens = iter(("account-a", "account-b"))

        def fake_stream_text_deltas(backend, _request):
            yield backend.answer

        body = {"model": "gpt-test", "input": "same prompt"}
        with (
            mock.patch.object(
                openai_v1_response.account_service,
                "get_text_access_token",
                side_effect=lambda **_kwargs: next(selected_tokens),
            ),
            mock.patch.object(
                openai_v1_response,
                "text_backend",
                side_effect=lambda _model, **kwargs: Backend(kwargs["access_token"]),
            ),
            mock.patch.object(openai_v1_response, "stream_text_deltas", side_effect=fake_stream_text_deltas),
        ):
            first = openai_v1_response.handle(body, cache_scope="user-key")
            second = openai_v1_response.handle(body, cache_scope="user-key")

        self.assertEqual(first["output"][0]["content"][0]["text"], "account-a")
        self.assertEqual(second["output"][0]["content"][0]["text"], "account-b")

    def test_cached_non_stream_completion_gets_a_fresh_response_id(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "fresh response identity"}],
        }

        with (
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_chat_complete.collect_text", return_value="ok"),
        ):
            first = openai_v1_chat_complete.handle(body)
            second = openai_v1_chat_complete.handle(body)

        self.assertNotEqual(first["id"], second["id"])

    def test_cache_key_distinguishes_thinking_effort_inputs(self) -> None:
        messages = [{"role": "user", "content": "same prompt"}]
        base = {"model": "auto", "messages": messages}

        default_key = cache_key(base, messages, stream=False)
        thinking_key = cache_key({**base, "thinking_effort": "high"}, messages, stream=False)
        reasoning_key = cache_key({**base, "reasoning": {"effort": "high"}}, messages, stream=False)

        self.assertNotEqual(default_key, thinking_key)
        self.assertNotEqual(default_key, reasoning_key)
        self.assertNotEqual(thinking_key, reasoning_key)

    def test_cache_key_distinguishes_stream_usage_framing(self) -> None:
        messages = [{"role": "user", "content": "same streamed prompt"}]
        base = {"model": "auto", "messages": messages, "stream": True}

        without_usage = cache_key(base, messages, stream=True)
        with_usage = cache_key(
            {**base, "stream_options": {"include_usage": True}},
            messages,
            stream=True,
        )

        self.assertNotEqual(without_usage, with_usage)

    def test_cache_key_distinguishes_all_added_response_options(self) -> None:
        messages = [{"role": "user", "content": "same prompt"}]
        base = {"model": "auto", "messages": messages}
        base_key = cache_key(base, messages, stream=False)

        for field, value in (
            ("modalities", ["text"]),
            ("n", 1),
            ("parallel_tool_calls", False),
            ("prompt", "alternate prompt"),
            ("prompt_cache_key", "conversation-a"),
            ("service_tier", "flex"),
            ("store", False),
            ("verbosity", "high"),
            ("web_search_options", {"search_context_size": "low"}),
        ):
            with self.subTest(field=field):
                self.assertNotEqual(
                    base_key,
                    cache_key({**base, field: value}, messages, stream=False),
                )

    def test_cache_key_distinguishes_unlisted_business_fields(self) -> None:
        messages = [{"role": "user", "content": "same prompt"}]
        base = {"model": "auto", "messages": messages}

        self.assertNotEqual(
            cache_key(base, messages, stream=False),
            cache_key(
                {**base, "future_response_option": {"enabled": True}},
                messages,
                stream=False,
            ),
        )

    def test_cache_key_uses_normalized_messages_stream_and_stable_json_values(self) -> None:
        normalized_messages = [{"role": "user", "content": "normalized"}]
        first = {
            "z_future": {"b": 2, "a": 1},
            "messages": [{"role": "user", "content": "caller value"}],
            "stream": False,
            "payload": b"stable-bytes",
        }
        second = {
            "payload": bytearray(b"stable-bytes"),
            "stream": False,
            "messages": [{"role": "system", "content": "different caller value"}],
            "z_future": {"a": 1, "b": 2},
        }

        self.assertEqual(
            cache_key(first, normalized_messages, stream=True),
            cache_key(second, normalized_messages, stream=True),
        )
        self.assertNotEqual(
            cache_key(first, normalized_messages, stream=True),
            cache_key(first, normalized_messages, stream=False),
        )
        self.assertNotEqual(
            cache_key(first, normalized_messages, stream=True),
            cache_key(first, [{"role": "user", "content": "other"}], stream=True),
        )

    def test_chat_stream_include_usage_emits_official_terminal_usage_chunk(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "count this prompt"}],
            "stream": True,
            "stream_options": {"include_usage": True},
        }

        with (
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch(
                "services.protocol.openai_v1_chat_complete.stream_text_deltas",
                return_value=iter(["count ", "this answer"]),
            ),
        ):
            chunks = list(openai_v1_chat_complete.handle(body))

        self.assertGreaterEqual(len(chunks), 3)
        for chunk in chunks[:-1]:
            self.assertIsNone(chunk["usage"])
            self.assertTrue(chunk["choices"])
        usage_chunk = chunks[-1]
        self.assertEqual(usage_chunk["choices"], [])
        self.assertGreater(usage_chunk["usage"]["prompt_tokens"], 0)
        self.assertGreater(usage_chunk["usage"]["completion_tokens"], 0)
        self.assertEqual(
            usage_chunk["usage"]["total_tokens"],
            usage_chunk["usage"]["prompt_tokens"] + usage_chunk["usage"]["completion_tokens"],
        )

    def test_chat_completion_reasoning_effort_reaches_conversation_request(self) -> None:
        captured_efforts: list[str] = []

        def fake_collect_text(_backend, request):
            captured_efforts.append(request.thinking_effort)
            return "ok"

        body = {
            "model": "auto",
            "reasoning_effort": "xhigh",
            "messages": [{"role": "user", "content": "use more reasoning"}],
        }

        with (
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_chat_complete.collect_text", side_effect=fake_collect_text),
        ):
            openai_v1_chat_complete.handle(body)

        self.assertEqual(captured_efforts, ["xhigh"])

    def test_supported_reasoning_efforts_are_forwarded_without_default_override(self) -> None:
        captured_chat_efforts: list[str] = []
        captured_response_efforts: list[str] = []

        def fake_collect_text(_backend, request):
            captured_chat_efforts.append(request.thinking_effort)
            return "ok"

        def fake_stream_text_deltas(_backend, request):
            captured_response_efforts.append(request.thinking_effort)
            yield "ok"

        with (
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_chat_complete.collect_text", side_effect=fake_collect_text),
        ):
            openai_v1_chat_complete.handle({
                "model": "auto",
                "reasoning_effort": "max",
                "messages": [{"role": "user", "content": "use maximum reasoning"}],
            })
            openai_v1_chat_complete.handle({
                "model": "auto",
                "reasoning_effort": "none",
                "messages": [{"role": "user", "content": "disable reasoning"}],
            })

        with (
            mock.patch("services.protocol.openai_v1_response.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_response.stream_text_deltas", side_effect=fake_stream_text_deltas),
        ):
            openai_v1_response.handle({
                "model": "auto",
                "input": "use maximum reasoning",
                "reasoning": {"effort": "max"},
            })
            openai_v1_response.handle({
                "model": "auto",
                "input": "disable reasoning",
                "reasoning": {"effort": "none"},
            })

        self.assertEqual(captured_chat_efforts, ["max", "auto"])
        self.assertEqual(captured_response_efforts, ["max", "auto"])

    def test_native_chat_forwards_supported_official_reasoning_effort(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "use maximum reasoning"}],
            "tools": [{
                "type": "function",
                "function": {
                    "name": "lookup",
                    "parameters": {"type": "object", "properties": {}},
                },
            }],
            "reasoning_effort": "max",
        }

        result = openai_v1_chat_complete.chat_codex_response_body(body)

        self.assertEqual(result["reasoning"], {"effort": "max"})

    def test_responses_reasoning_effort_reaches_conversation_request(self) -> None:
        captured_efforts: list[str] = []

        def fake_stream_text_deltas(_backend, request):
            captured_efforts.append(request.thinking_effort)
            yield "ok"

        body = {
            "model": "auto",
            "input": "use more reasoning",
            "reasoning": {"effort": "xhigh"},
        }

        with (
            mock.patch("services.protocol.openai_v1_response.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_response.stream_text_deltas", side_effect=fake_stream_text_deltas),
        ):
            openai_v1_response.handle(body)

        self.assertEqual(captured_efforts, ["xhigh"])

    def test_repeated_stream_text_completion_replays_cached_chunks(self) -> None:
        calls = 0

        def fake_stream_text_deltas(_backend, _request):
            nonlocal calls
            calls += 1
            yield "streamed"
            yield " answer"

        body = {
            "model": "auto",
            "stream": True,
            "messages": [{"role": "user", "content": "stream cache this exact prompt"}],
        }

        with (
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch(
                "services.protocol.openai_v1_chat_complete.stream_text_deltas",
                side_effect=fake_stream_text_deltas,
            ),
        ):
            first = list(openai_v1_chat_complete.handle(body))
            second = list(openai_v1_chat_complete.handle(body))

        self.assertEqual(calls, 1)
        self.assertNotEqual(first[0]["id"], second[0]["id"])
        content = "".join(str(chunk["choices"][0]["delta"].get("content") or "") for chunk in second)
        self.assertEqual(content, "streamed answer")

    def test_cached_response_events_get_fresh_response_and_item_ids(self) -> None:
        body = {"model": "auto", "input": "fresh response event identity"}

        with (
            mock.patch("services.protocol.openai_v1_response.text_backend", return_value=object()),
            mock.patch(
                "services.protocol.openai_v1_response.stream_text_deltas",
                return_value=iter(["ok"]),
            ),
        ):
            first = list(openai_v1_response.response_events(body))
            second = list(openai_v1_response.response_events(body))

        first_response = first[-1]["response"]
        second_response = second[-1]["response"]
        self.assertNotEqual(first_response["id"], second_response["id"])
        self.assertNotEqual(first[1]["item"]["id"], second[1]["item"]["id"])
        self.assertEqual(first[-1]["response"]["output"][0]["content"], second[-1]["response"]["output"][0]["content"])

    def test_stream_cache_followers_do_not_starve_the_owner_ai_worker(self) -> None:
        owner_can_finish = threading.Event()
        followers_entered = threading.Event()
        entered_count = 0
        entered_lock = threading.Lock()
        follower_compute_calls = 0

        def owner_compute():
            yield {"type": "owner.started"}
            if not owner_can_finish.wait(timeout=5):
                raise AssertionError("owner was not released")
            yield {"type": "owner.completed"}

        def follower_compute():
            nonlocal follower_compute_calls
            follower_compute_calls += 1
            yield {"type": "follower.started"}

        owner = chat_completion_cache.get_or_compute_stream("same-stream", owner_compute)
        self.assertEqual(next(owner), {"type": "owner.started"})
        followers = [
            chat_completion_cache.get_or_compute_stream("same-stream", follower_compute)
            for _ in range(32)
        ]

        def next_follower(iterator):
            nonlocal entered_count
            with entered_lock:
                entered_count += 1
                if entered_count == len(followers):
                    followers_entered.set()
            return next(iterator)

        async def run_pressure() -> tuple[dict[str, str], list[dict[str, str]]]:
            follower_tasks = [
                asyncio.create_task(run_ai_in_threadpool(next_follower, iterator))
                for iterator in followers
            ]
            owner_task = None
            try:
                self.assertTrue(
                    await asyncio.to_thread(followers_entered.wait, 3),
                    "followers did not occupy the AI worker baseline",
                )
                owner_task = asyncio.create_task(run_ai_in_threadpool(next, owner))
                owner_can_finish.set()
                owner_result = await asyncio.wait_for(asyncio.shield(owner_task), timeout=0.5)
                follower_results = await asyncio.gather(*follower_tasks)
                self.assertEqual(await run_ai_in_threadpool(lambda: list(owner)), [])
                return owner_result, follower_results
            finally:
                owner_can_finish.set()
                if owner_task is not None and not owner_task.done():
                    owner.close()
                await asyncio.gather(*follower_tasks, return_exceptions=True)
                if owner_task is not None:
                    await asyncio.gather(owner_task, return_exceptions=True)
                for iterator in followers:
                    iterator.close()
                owner.close()

        owner_result, follower_results = asyncio.run(run_pressure())

        self.assertEqual(owner_result, {"type": "owner.completed"})
        self.assertEqual(follower_compute_calls, 32)
        self.assertEqual(follower_results, [{"type": "follower.started"}] * 32)

        def unexpected_late_compute():
            raise AssertionError("completed owner stream was not cached")
            yield

        self.assertEqual(
            list(chat_completion_cache.get_or_compute_stream("same-stream", unexpected_late_compute)),
            [{"type": "owner.started"}, {"type": "owner.completed"}],
        )

    def test_stream_cache_closes_owner_source_when_consumer_stops_early(self) -> None:
        class CloseTrackingIterator:
            def __init__(self) -> None:
                self.closed = False
                self.index = 0

            def __iter__(self):
                return self

            def __next__(self):
                if self.index == 0:
                    self.index += 1
                    return {"type": "owner.started"}
                raise AssertionError("consumer should stop before source completes")

            def close(self) -> None:
                self.closed = True

        source = CloseTrackingIterator()
        owner = chat_completion_cache.get_or_compute_stream(
            "early-stop-stream",
            lambda: source,
        )

        self.assertEqual(next(owner), {"type": "owner.started"})
        owner.close()

        self.assertTrue(source.closed)

    def test_clear_invalidates_inflight_response_before_owner_publishes(self) -> None:
        started = threading.Event()
        release = threading.Event()
        results: list[dict[str, object]] = []
        errors: list[BaseException] = []

        def compute_old() -> dict[str, object]:
            started.set()
            self.assertTrue(release.wait(timeout=2))
            return {"value": "old"}

        def run_owner() -> None:
            try:
                results.append(chat_completion_cache.get_or_compute_response("clear-response", compute_old))
            except BaseException as exc:
                errors.append(exc)

        thread = threading.Thread(target=run_owner)
        thread.start()
        self.assertTrue(started.wait(timeout=2))
        chat_completion_cache.clear()
        release.set()
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(results, [{"value": "old"}])

        calls = 0

        def compute_new() -> dict[str, object]:
            nonlocal calls
            calls += 1
            return {"value": "new"}

        self.assertEqual(
            chat_completion_cache.get_or_compute_response("clear-response", compute_new),
            {"value": "new"},
        )
        self.assertEqual(calls, 1)

    def test_clear_invalidates_inflight_stream_before_owner_publishes(self) -> None:
        release = threading.Event()

        def compute_old():
            yield {"value": "old-started"}
            self.assertTrue(release.wait(timeout=2))
            yield {"value": "old-finished"}

        owner = chat_completion_cache.get_or_compute_stream("clear-stream", compute_old)
        self.assertEqual(next(owner), {"value": "old-started"})
        chat_completion_cache.clear()
        release.set()
        self.assertEqual(list(owner), [{"value": "old-finished"}])

        calls = 0

        def compute_new():
            nonlocal calls
            calls += 1
            yield {"value": "new"}

        self.assertEqual(
            list(chat_completion_cache.get_or_compute_stream("clear-stream", compute_new)),
            [{"value": "new"}],
        )
        self.assertEqual(calls, 1)

    def test_clear_wakes_response_waiter_without_touching_new_generation(self) -> None:
        owner_started = threading.Event()
        release_owner = threading.Event()
        waiter_entered = threading.Event()
        follower_errors: list[BaseException] = []
        original_condition = threading.Condition

        class TrackingCondition(original_condition):
            def wait(self, *args, **kwargs):
                waiter_entered.set()
                return super().wait(*args, **kwargs)

        def owner_compute() -> dict[str, object]:
            owner_started.set()
            self.assertTrue(release_owner.wait(timeout=2))
            return {"value": "old"}

        owner_thread = threading.Thread(
            target=lambda: chat_completion_cache.get_or_compute_response("clear-waiter", owner_compute),
        )
        owner_thread.start()
        self.assertTrue(owner_started.wait(timeout=2))

        def follower() -> None:
            try:
                chat_completion_cache.get_or_compute_response("clear-waiter", lambda: {"value": "unexpected"})
            except BaseException as exc:
                follower_errors.append(exc)

        with mock.patch.object(cache_module.threading, "Condition", TrackingCondition):
            follower_thread = threading.Thread(target=follower)
            follower_thread.start()
            self.assertTrue(waiter_entered.wait(timeout=2))
            chat_completion_cache.clear()
            follower_thread.join(timeout=2)

        release_owner.set()
        owner_thread.join(timeout=2)
        self.assertFalse(owner_thread.is_alive())
        self.assertFalse(follower_thread.is_alive())
        self.assertEqual(len(follower_errors), 1)
        self.assertEqual(str(follower_errors[0]), "cache fill invalidated")

    def test_old_response_error_cannot_remove_new_generation_owner(self) -> None:
        old_started = threading.Event()
        release_old = threading.Event()
        new_started = threading.Event()
        release_new = threading.Event()
        third_started = threading.Event()
        old_errors: list[BaseException] = []
        new_results: list[dict[str, object]] = []
        third_results: list[dict[str, object]] = []

        def old_compute() -> dict[str, object]:
            old_started.set()
            self.assertTrue(release_old.wait(timeout=2))
            raise RuntimeError("old failure")

        old_thread = threading.Thread(
            target=lambda: self._capture_error(
                old_errors,
                lambda: chat_completion_cache.get_or_compute_response("clear-aba", old_compute),
            ),
        )
        old_thread.start()
        self.assertTrue(old_started.wait(timeout=2))
        chat_completion_cache.clear()

        def new_compute() -> dict[str, object]:
            new_started.set()
            self.assertTrue(release_new.wait(timeout=2))
            return {"value": "new"}

        new_thread = threading.Thread(
            target=lambda: new_results.append(
                chat_completion_cache.get_or_compute_response("clear-aba", new_compute),
            ),
        )
        new_thread.start()
        self.assertTrue(new_started.wait(timeout=2))
        release_old.set()
        old_thread.join(timeout=2)
        self.assertFalse(old_thread.is_alive())

        def third_compute() -> dict[str, object]:
            third_started.set()
            return {"value": "third"}

        third_thread = threading.Thread(
            target=lambda: third_results.append(
                chat_completion_cache.get_or_compute_response("clear-aba", third_compute),
            ),
        )
        third_thread.start()
        self.assertFalse(third_started.wait(timeout=0.2))
        release_new.set()
        new_thread.join(timeout=2)
        third_thread.join(timeout=2)
        self.assertFalse(new_thread.is_alive())
        self.assertFalse(third_thread.is_alive())
        self.assertEqual(old_errors[0].args, ("old failure",))
        self.assertEqual(new_results, [{"value": "new"}])
        self.assertEqual(third_results, [{"value": "new"}])

    @staticmethod
    def _capture_error(target: list[BaseException], callback) -> None:
        try:
            callback()
        except BaseException as exc:
            target.append(exc)

    def test_stream_cache_clears_inflight_when_compute_acquisition_fails(self) -> None:
        attempts = 0

        def compute():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("compute acquisition failed")
            return iter([{"type": "recovered"}])

        first = chat_completion_cache.get_or_compute_stream("acquisition-failure", compute)
        with self.assertRaises(RuntimeError):
            next(first)

        self.assertEqual(list(chat_completion_cache.get_or_compute_stream("acquisition-failure", compute)), [{"type": "recovered"}])
        self.assertEqual(attempts, 2)

    def test_stream_cache_clears_inflight_when_iterator_acquisition_fails(self) -> None:
        attempts = 0

        class BrokenIterable:
            def __iter__(self):
                raise RuntimeError("iterator acquisition failed")

        def compute():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                return BrokenIterable()
            return iter([{"type": "recovered"}])

        first = chat_completion_cache.get_or_compute_stream("iterator-acquisition-failure", compute)
        with self.assertRaises(RuntimeError):
            next(first)

        self.assertEqual(
            list(chat_completion_cache.get_or_compute_stream("iterator-acquisition-failure", compute)),
            [{"type": "recovered"}],
        )
        self.assertEqual(attempts, 2)

    def test_adjacent_duplicate_messages_are_removed_before_upstream_call(self) -> None:
        captured_messages = []

        def fake_collect_text(_backend, request):
            captured_messages.extend(request.messages or [])
            return "ok"

        body = {
            "model": "auto",
            "messages": [
                {"role": "user", "content": "repeat me"},
                {"role": "user", "content": "repeat me"},
                {"role": "assistant", "content": "old answer"},
                {"role": "user", "content": "next prompt"},
            ],
        }

        with (
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_chat_complete.collect_text", side_effect=fake_collect_text),
        ):
            openai_v1_chat_complete.handle(body)

        self.assertEqual(
            captured_messages,
            [
                {"role": "user", "content": "repeat me"},
                {"role": "assistant", "content": "old answer"},
                {"role": "user", "content": "next prompt"},
            ],
        )

    def test_chat_completion_usage_includes_cached_tokens(self) -> None:
        with (
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_chat_complete.collect_text", return_value="ok"),
        ):
            response = openai_v1_chat_complete.handle({
                "model": "auto",
                "messages": [{"role": "user", "content": "usage shape"}],
            })

        details = response["usage"]["prompt_tokens_details"]
        self.assertEqual(details["cached_tokens"], 0)
        output_details = response["usage"]["completion_tokens_details"]
        self.assertEqual(output_details["reasoning_tokens"], 0)

    def test_responses_completed_usage_includes_cached_tokens(self) -> None:
        with (
            mock.patch("services.protocol.openai_v1_response.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_response.stream_text_deltas", return_value=iter(["ok"])),
        ):
            response = openai_v1_response.handle({
                "model": "auto",
                "input": "usage shape",
            })

        details = response["usage"]["input_tokens_details"]
        self.assertEqual(details["cached_tokens"], 0)
        output_details = response["usage"]["output_tokens_details"]
        self.assertEqual(output_details["reasoning_tokens"], 0)

    def test_repeated_responses_text_request_uses_cache(self) -> None:
        calls = 0

        def fake_stream_text_deltas(_backend, _request):
            nonlocal calls
            calls += 1
            yield f"response cache {calls}"

        body = {
            "model": "auto",
            "input": "cache this responses prompt",
            "stream": True,
        }

        with (
            mock.patch("services.protocol.openai_v1_response.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_response.stream_text_deltas", side_effect=fake_stream_text_deltas),
        ):
            first = list(openai_v1_response.handle(body))
            second = list(openai_v1_response.handle(body))

        self.assertEqual(calls, 1)
        self.assertNotEqual(first[0]["response"]["id"], second[0]["response"]["id"])
        self.assertEqual(
            first[-1]["response"]["output"][0]["content"],
            second[-1]["response"]["output"][0]["content"],
        )

    def test_output_sanitizer_removes_chatgpt_annotation_markup(self) -> None:
        text = (
            "Repo: \ue200url\ue202basketikun/chatgpt2api"
            "\ue202https://github.com/basketikun/chatgpt2api\ue201 "
            "details \ue200cite\ue202turn0search0\ue201."
        )

        self.assertEqual(
            sanitize_output_text(text),
            "Repo: basketikun/chatgpt2api (https://github.com/basketikun/chatgpt2api) details.",
        )

    def test_output_sanitizer_preserves_annotated_entity_text(self) -> None:
        text = (
            "The character is from \ue200entity\ue202Invincible\ue201, "
            "which is based on the comic series \ue200entity\ue202Invincible\ue201."
        )

        self.assertEqual(
            sanitize_output_text(text),
            "The character is from Invincible, which is based on the comic series Invincible.",
        )

    def test_output_sanitizer_preserves_readable_cite_label(self) -> None:
        text = "The character is \ue200cite\ue202Invincible\ue202turn0search0\ue201."

        self.assertEqual(sanitize_output_text(text), "The character is Invincible.")

    def test_output_sanitizer_preserves_code_spaces_before_punctuation(self) -> None:
        self.assertEqual(sanitize_output_text("find ."), "find .")
        self.assertEqual(sanitize_output_text("if ! test -f x; then"), "if ! test -f x; then")

    def test_output_sanitizer_preserves_newline_before_removed_annotation(self) -> None:
        text = "first line\n \ue200cite\ue202turn0search0\ue201.\nlast line"

        self.assertEqual(sanitize_output_text(text), "first line\n.\nlast line")

    def test_stream_sanitizer_does_not_emit_partial_annotation_or_repeat_prefix(self) -> None:
        events = [
            {"p": "/message/content/parts/0", "o": "append", "v": "Repo: \ue200url\ue202chat"},
            {"p": "/message/content/parts/0", "o": "append", "v": "gpt2api\ue202turn0search0\ue201 done \ue200cite\ue202turn0\ue201."},
            "[DONE]",
        ]
        payloads = [json.dumps(event, ensure_ascii=False) if isinstance(event, dict) else event for event in events]
        deltas = [
            str(event.get("delta") or "")
            for event in iter_conversation_payloads(iter(payloads))
            if event.get("type") == "conversation.delta"
        ]

        self.assertEqual("".join(deltas), "Repo: chatgpt2api done.")
        self.assertFalse(any("\ue200" in delta or "\ue202" in delta or "\ue201" in delta for delta in deltas))

    def test_stream_ignores_internal_tool_recipient_messages(self) -> None:
        events = [
            {
                "message": {
                    "author": {"role": "assistant"},
                    "content": {
                        "content_type": "code",
                        "text": 'search("\\u56fd\\u5bb6\\u6d77\\u6d0b")',
                    },
                    "recipient": "web",
                    "status": "finished_successfully",
                },
            },
            {
                "message": {
                    "author": {"name": "web.run", "role": "tool"},
                    "content": {"content_type": "text", "parts": [""]},
                    "recipient": "all",
                    "status": "finished_successfully",
                },
            },
            {
                "v": {
                    "message": {
                        "author": {"role": "assistant"},
                        "content": {
                            "content_type": "code",
                            "text": '{"aspect_ratio":"16:9","query":["museum"],"num_per_query":2}',
                        },
                        "recipient": "web",
                        "status": "finished_successfully",
                    },
                },
            },
            {
                "message": {
                    "author": {"role": "assistant"},
                    "channel": "final",
                    "content": {"content_type": "text", "parts": ["Final answer."]},
                    "recipient": "all",
                    "status": "finished_successfully",
                },
            },
            "[DONE]",
        ]
        payloads = [json.dumps(event, ensure_ascii=False) if isinstance(event, dict) else event for event in events]
        deltas = [
            str(event.get("delta") or "")
            for event in iter_conversation_payloads(iter(payloads))
            if event.get("type") == "conversation.delta"
        ]

        self.assertEqual("".join(deltas), "Final answer.")

    def test_stream_ignores_hidden_and_non_final_assistant_messages(self) -> None:
        events = [
            {
                "message": {
                    "author": {"role": "assistant"},
                    "channel": "final",
                    "content": {"content_type": "text", "parts": ["Hidden text."]},
                    "metadata": {"is_visually_hidden_from_conversation": True},
                    "recipient": "all",
                },
            },
            {
                "message": {
                    "author": {"role": "assistant"},
                    "channel": "analysis",
                    "content": {"content_type": "text", "parts": ["Private reasoning."]},
                    "recipient": "all",
                },
            },
            {
                "message": {
                    "author": {"role": "assistant"},
                    "channel": "final",
                    "content": {"content_type": "text", "parts": ["Visible text."]},
                    "recipient": "all",
                },
            },
            "[DONE]",
        ]
        payloads = [json.dumps(event, ensure_ascii=False) if isinstance(event, dict) else event for event in events]
        deltas = [
            str(event.get("delta") or "")
            for event in iter_conversation_payloads(iter(payloads))
            if event.get("type") == "conversation.delta"
        ]

        self.assertEqual("".join(deltas), "Visible text.")

    def test_stream_preserves_user_visible_code_messages(self) -> None:
        events = [
            {
                "message": {
                    "author": {"role": "assistant"},
                    "channel": "final",
                    "content": {"content_type": "code", "text": 'print("hello")'},
                    "recipient": "all",
                },
            },
            "[DONE]",
        ]
        payloads = [json.dumps(event, ensure_ascii=False) if isinstance(event, dict) else event for event in events]
        deltas = [
            str(event.get("delta") or "")
            for event in iter_conversation_payloads(iter(payloads))
            if event.get("type") == "conversation.delta"
        ]

        self.assertEqual("".join(deltas), 'print("hello")')

    def test_responses_rejects_tools_that_have_no_execution_path(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            openai_v1_response.text_response_parts({
                "model": "auto",
                "input": "inspect the file",
                "tools": [{"type": "file_search", "vector_store_ids": ["vs_1"]}],
            })

        self.assertEqual(raised.exception.status_code, 400)

    def test_chat_rejects_tools_that_have_no_execution_path(self) -> None:
        with self.assertRaises(HTTPException) as raised:
            openai_v1_chat_complete.text_chat_parts({
                "model": "auto",
                "messages": [{"role": "user", "content": "inspect the file"}],
                "tools": [{"type": "custom", "custom": {"name": "shell"}}],
            })

        self.assertEqual(raised.exception.status_code, 400)

    def test_responses_web_search_tool_returns_search_output(self) -> None:
        body = {
            "model": "auto",
            "input": "latest example news",
            "tools": [{"type": "web_search"}],
        }

        with (
            mock.patch.object(
                openai_v1_response,
                "stream_codex_response",
                return_value=iter(native_search_events("Latest answer.", "https://example.com/news")),
            ) as native,
        ):
            response = openai_v1_response.handle(body)

        native.assert_called_once_with(body)
        self.assertEqual(response["output"][0]["type"], "web_search_call")
        self.assertEqual(response["output"][0]["status"], "completed")
        message = response["output"][1]
        self.assertEqual(message["type"], "message")
        content = message["content"][0]
        self.assertIn("Latest answer.", content["text"])
        self.assertEqual(content["annotations"][0]["type"], "url_citation")
        self.assertEqual(content["annotations"][0]["url"], "https://example.com/news")

    def test_responses_web_search_tool_streams_search_events(self) -> None:
        body = {
            "model": "auto",
            "stream": True,
            "input": "stream search",
            "tools": [{"type": "web_search_preview"}],
        }

        with mock.patch.object(
            openai_v1_response,
            "stream_codex_response",
            return_value=iter(native_search_events("Streamed search answer.", "https://example.com/stream")),
        ):
            events = list(openai_v1_response.handle(body))

        event_types = [event["type"] for event in events]
        self.assertIn("response.output_item.done", event_types)
        completed = events[-1]["response"]
        self.assertEqual(completed["output"][0]["type"], "web_search_call")
        self.assertEqual(completed["output"][1]["type"], "message")

    def test_responses_versioned_web_search_tool_returns_search_output(self) -> None:
        body = {
            "model": "auto",
            "input": "versioned search",
            "tools": [{"type": "web_search_preview_2025_03_11"}],
        }

        payload = openai_v1_response.codex_response_payload(body)

        self.assertEqual(payload["tools"], [{"type": "web_search"}])

    def test_chat_completions_web_search_tool_returns_search_answer(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "search chat"}],
            "tools": [{"type": "web_search"}],
        }

        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(native_search_events("Chat search answer.", "https://example.com/chat")),
        ) as native:
            response = openai_v1_chat_complete.handle(body)

        self.assertEqual(native.call_args.args[0]["tools"], [{"type": "web_search"}])
        message = response["choices"][0]["message"]
        self.assertIn("Chat search answer.", message["content"])
        self.assertEqual(message["annotations"][0]["type"], "url_citation")
        self.assertEqual(message["annotations"][0]["url_citation"]["url"], "https://example.com/chat")

    def test_chat_completions_web_search_options_trigger_search(self) -> None:
        body = {
            "model": "auto",
            "messages": [{"role": "user", "content": "search options"}],
            "web_search_options": {"search_context_size": "low"},
        }

        with mock.patch.object(
            openai_v1_chat_complete.openai_v1_response,
            "stream_codex_response",
            return_value=iter(native_search_events("Options search answer.", "https://example.com/options")),
        ) as native:
            response = openai_v1_chat_complete.handle(body)

        self.assertEqual(
            native.call_args.args[0]["tools"],
            [{"type": "web_search", "search_context_size": "low"}],
        )
        self.assertIn("Options search answer.", response["choices"][0]["message"]["content"])

    def test_chat_completions_search_model_triggers_search(self) -> None:
        search_result = {
            "answer": "Search model answer.",
            "sources": [{"title": "Example", "url": "https://example.com/model", "snippet": ""}],
        }
        body = {
            "model": "gpt-5-search-api-2026-06-01",
            "messages": [{"role": "user", "content": "search model"}],
        }

        with mock.patch("services.protocol.openai_v1_chat_complete.run_web_search", return_value=search_result) as search:
            response = openai_v1_chat_complete.handle(body)

        search.assert_called_once_with("search model")
        self.assertEqual(response["model"], "gpt-5-search-api-2026-06-01")
        self.assertIn("Search model answer.", response["choices"][0]["message"]["content"])

    def test_chat_completions_search_like_model_does_not_trigger_search(self) -> None:
        body = {
            "model": "gpt-5-search-apiary",
            "messages": [{"role": "user", "content": "not actually a search model"}],
        }

        with (
            mock.patch("services.protocol.openai_v1_chat_complete.run_web_search") as search,
            mock.patch("services.protocol.openai_v1_chat_complete.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_chat_complete.collect_text", return_value="plain text answer"),
        ):
            response = openai_v1_chat_complete.handle(body)

        search.assert_not_called()
        self.assertIn("plain text answer", response["choices"][0]["message"]["content"])

    def test_chat_completions_rejects_remote_image_url_without_network(self) -> None:
        with mock.patch("services.remote_image.requests.Session") as session:
            with self.assertRaises(HTTPException) as raised:
                openai_v1_chat_complete.text_chat_parts({
                    "model": "auto",
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "Describe this"},
                            {"type": "image_url", "image_url": {"url": "http://127.0.0.1/image.png"}},
                        ],
                    }],
                })

        self.assertNotIn("127.0.0.1", str(raised.exception.detail))
        session.assert_not_called()

    def _assert_chat_and_response_reject_inline_image(self, image_url: str) -> None:
        chat_body = {
            "model": "auto",
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "Describe this"},
                    {"type": "image_url", "image_url": {"url": image_url}},
                ],
            }],
        }
        response_body = {
            "model": "auto",
            "input": [{
                "type": "message",
                "role": "user",
                "content": [
                    {"type": "input_text", "text": "Describe this"},
                    {"type": "input_image", "image_url": {"url": image_url}},
                ],
            }],
        }
        for parser, body in (
            (openai_v1_chat_complete.text_chat_parts, chat_body),
            (openai_v1_response.text_response_parts, response_body),
        ):
            with self.subTest(parser=parser.__module__, image_url=image_url[:40]):
                with self.assertRaises(HTTPException) as raised:
                    parser(body)
                self.assertEqual(raised.exception.status_code, 400)

    def test_chat_and_responses_reject_malformed_inline_image_without_network(self) -> None:
        self._assert_chat_and_response_reject_inline_image("data:image/png;base64,not-base64")

    def test_chat_and_responses_reject_non_image_inline_mime_without_network(self) -> None:
        encoded = base64.b64encode(b"not an image").decode("ascii")
        self._assert_chat_and_response_reject_inline_image(f"data:text/plain;base64,{encoded}")

    def test_chat_and_responses_reject_oversized_inline_image_without_network(self) -> None:
        oversized = base64.b64encode(b"x" * (10 * 1024 * 1024 + 1)).decode("ascii")
        self._assert_chat_and_response_reject_inline_image(f"data:image/png;base64,{oversized}")

    def test_responses_text_request_preserves_input_image(self) -> None:
        captured = {}

        def fake_stream_text_deltas(_backend, request):
            captured["messages"] = request.messages
            yield "red"

        body = {
            "model": "auto",
            "input": [
                {"type": "input_text", "text": "What color is this image?"},
                {"type": "input_image", "image_url": PNG_1X1_DATA_URL},
            ],
        }

        with (
            mock.patch("services.protocol.openai_v1_response.text_backend", return_value=object()),
            mock.patch("services.protocol.openai_v1_response.stream_text_deltas", side_effect=fake_stream_text_deltas),
        ):
            response = openai_v1_response.handle(body)

        self.assertEqual(response["output"][0]["content"][0]["text"], "red")
        content = captured["messages"][0]["content"]
        self.assertEqual(content[0], {"type": "text", "text": "What color is this image?"})
        self.assertEqual(content[1]["type"], "image")
        self.assertEqual(content[1]["mime"], "image/png")
        self.assertEqual(content[1]["data"], PNG_1X1)
        self.assertGreater(response["usage"]["input_tokens_details"]["image_tokens"], 0)

    def test_responses_rejects_remote_input_image_url_without_network(self) -> None:
        with mock.patch("services.remote_image.requests.Session") as session:
            with self.assertRaises(HTTPException) as raised:
                openai_v1_response.text_response_parts({
                    "model": "auto",
                    "input": [{
                        "type": "message",
                        "role": "user",
                        "content": [
                            {"type": "input_text", "text": "Describe this"},
                            {"type": "input_image", "image_url": {"url": "https://localhost/image.png"}},
                        ],
                    }],
                })

        self.assertNotIn("localhost", str(raised.exception.detail))
        session.assert_not_called()

    def test_image_extractor_supports_extra_image_object_shapes(self) -> None:
        encoded = base64.b64encode(PNG_1X1).decode("ascii")

        images = extract_image_from_message_content([
            {"type": "image", "data": PNG_1X1, "mime": "image/png"},
            {"type": "input_image", "base64": encoded, "mime_type": "image/png"},
            {"type": "input_image", "source": {"type": "base64", "data": encoded, "media_type": "image/png"}},
        ])

        self.assertEqual(len(images), 3)
        self.assertEqual([image[1] for image in images], ["image/png", "image/png", "image/png"])
        self.assertTrue(all(image[0] == PNG_1X1 for image in images))

    def test_image_extractor_rejects_non_string_source_data_without_stringifying(self) -> None:
        class ExplodingValue:
            def __str__(self):
                raise AssertionError("source data must not be stringified")

        self.assertEqual(
            extract_image_from_message_content([
                {"type": "input_image", "source": {"type": "base64", "data": ExplodingValue()}},
                {"type": "image_url", "image_url": {"url": ExplodingValue()}},
            ]),
            [],
        )


if __name__ == "__main__":
    unittest.main()
