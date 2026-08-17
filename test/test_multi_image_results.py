from __future__ import annotations

import base64
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest import mock

import services.protocol.conversation as conversation_module
import services.protocol.openai_v1_chat_complete as chat_complete_module
import services.openai_backend_api as backend_module
from services.config import config
from services.openai_backend_api import ImagePollTimeoutError, OpenAIBackendAPI
from services.protocol.conversation import (
    ConversationRequest,
    ImageGenerationError,
    ImageOutput,
    collect_image_outputs,
    extract_conversation_ids,
    stream_image_events,
)
from services.protocol.openai_v1_response import stream_image_response


def _conversation(file_ids: list[str], sediment_ids: list[str] | None = None) -> dict:
    parts: list[object] = [
        {"content_type": "image_asset_pointer", "asset_pointer": f"file-service://{file_id}"}
        for file_id in file_ids
    ]
    parts.extend(f"sediment://{sediment_id}" for sediment_id in (sediment_ids or []))
    return {
        "mapping": {
            "tool": {
                "message": {
                    "author": {"role": "tool"},
                    "create_time": 1,
                    "metadata": {"async_task_type": "image_gen"},
                    "content": {"content_type": "multimodal_text", "parts": parts},
                }
            }
        }
    }


class FakeBackend(OpenAIBackendAPI):
    def __init__(self, conversations: list[dict] | None = None) -> None:
        self.conversations = conversations or []
        self.calls = 0
        self.file_urls: dict[str, str] = {}
        self.sediment_urls: dict[str, str] = {}

    def _get_conversation(self, conversation_id: str) -> dict:
        self.calls += 1
        index = min(self.calls - 1, len(self.conversations) - 1)
        return self.conversations[index]

    def _get_file_download_url(self, file_id: str) -> str:
        return self.file_urls.get(file_id, "")

    def _get_attachment_download_url(self, conversation_id: str, attachment_id: str) -> str:
        return self.sediment_urls.get(attachment_id, "")


class MultiImageResultTests(unittest.TestCase):
    def test_image_poll_deadline_uses_monotonic_clock(self) -> None:
        backend = FakeBackend([_conversation([], [])])
        clock = {"value": 0.0}

        def get_conversation(_conversation_id: str) -> dict:
            clock["value"] = 2.0
            return _conversation([], [])

        backend._query_backend_tasks = mock.Mock(return_value=[])
        backend._get_conversation = mock.Mock(side_effect=get_conversation)

        with (
            mock.patch.dict(
                config.data,
                {
                    "image_poll_initial_wait_secs": 0,
                    "image_poll_interval_secs": 0,
                    "image_settle_enabled": False,
                    "image_check_before_hit_enabled": True,
                },
            ),
            mock.patch.object(backend_module.time, "monotonic", side_effect=lambda: clock["value"]),
            mock.patch.object(backend_module.time, "time", side_effect=AssertionError("wall clock used")),
            self.assertRaises(ImagePollTimeoutError),
        ):
            backend._poll_image_results("conv-clock", timeout_secs=1)

    def test_untrusted_image_progress_is_not_published_by_chat(self) -> None:
        canary = "opaque-image-progress-secret owner@example.com"
        output = ImageOutput(
            kind="progress",
            model="gpt-image-2",
            index=1,
            total=1,
            text=canary,
        )

        chat_chunks = list(chat_complete_module.stream_image_chat_completion([output], "gpt-image-2"))

        self.assertNotIn(canary, repr(chat_chunks))

    def test_untrusted_image_message_is_not_published_by_chat_or_responses(self) -> None:
        canary = "opaque-upstream-secret owner@example.com"
        output = ImageOutput(
            kind="message",
            model="gpt-image-2",
            index=1,
            total=1,
            text=canary,
        )

        collected = collect_image_outputs([output])
        chat_chunks = list(chat_complete_module.stream_image_chat_completion([output], "gpt-image-2"))
        response_events = list(stream_image_response([output], "draw a cat", "gpt-5"))

        self.assertNotIn(canary, repr(collected))
        self.assertNotIn(canary, repr(chat_chunks))
        self.assertNotIn(canary, repr(response_events))

    def test_image_public_outputs_do_not_publish_account_email(self) -> None:
        output = ImageOutput(
            kind="result",
            model="gpt-image-2",
            index=1,
            total=1,
            account_email="owner@example.test",
            data=[{"url": "/images/result.png"}],
        )

        self.assertNotIn("_account_email", output.to_chunk())
        self.assertNotIn("owner@example.test", repr(collect_image_outputs([output])))

    def test_recent_conversation_lookup_rejects_container_ids(self) -> None:
        backend = object.__new__(OpenAIBackendAPI)
        canary = "recent-conversation-id-canary"
        backend._list_recent_conversations = mock.Mock(return_value=[{
            "id": {"secret": canary},
            "title": "Image",
            "updated_at": 2,
        }])

        result = backend.find_conversation_by_prompt("make an image", started_at=1)

        self.assertEqual(result, "")
        self.assertNotIn(canary, repr(result))

    def test_conversation_state_does_not_stringify_container_ids(self) -> None:
        from services.protocol.conversation import ConversationState, update_conversation_state

        state = ConversationState(conversation_id="conversation-safe")
        update_conversation_state(
            state,
            "{}",
            {"conversation_id": {"secret": "conversation-id-canary"}},
        )
        update_conversation_state(
            state,
            "{}",
            {"v": {"conversation_id": ["nested-conversation-id-canary"]}},
        )
        self.assertEqual(state.conversation_id, "conversation-safe")
        self.assertNotIn("conversation-id-canary", repr(state))
        self.assertNotIn("nested-conversation-id-canary", repr(state))

        payload_state = ConversationState()
        update_conversation_state(
            payload_state,
            '{"conversation_id":"../../conversation-id-canary"}',
        )
        self.assertEqual(payload_state.conversation_id, "")

    def test_concurrent_multi_image_requests_share_a_bounded_worker_pool(self) -> None:
        condition = threading.Condition()
        release = threading.Event()
        active = 0
        max_active = 0

        def generate(_request, index, total):
            nonlocal active, max_active
            with condition:
                active += 1
                max_active = max(max_active, active)
                condition.notify_all()
            try:
                if not release.wait(timeout=5):
                    raise AssertionError("image worker was not released")
                return [
                    ImageOutput(
                        kind="result",
                        model="gpt-image-2",
                        index=index,
                        total=total,
                        data=[{"b64_json": "aW1hZ2U="}],
                    )
                ]
            finally:
                with condition:
                    active -= 1
                    condition.notify_all()

        request = ConversationRequest(model="gpt-image-2", prompt="cat", n=10)
        with (
            mock.patch.object(conversation_module, "_generate_single_image", side_effect=generate),
            mock.patch.object(
                conversation_module,
                "config",
                SimpleNamespace(image_parallel_generation=True),
            ),
            ThreadPoolExecutor(max_workers=4) as callers,
        ):
            futures = [
                callers.submit(lambda: list(conversation_module.stream_image_outputs_with_pool(request)))
                for _ in range(4)
            ]
            try:
                deadline = time.monotonic() + 3
                with condition:
                    while max_active < 32 and time.monotonic() < deadline:
                        condition.wait(timeout=0.05)
                    self.assertGreaterEqual(max_active, 30, "parallel image workers did not reach the test baseline")
                    overflow_deadline = time.monotonic() + 0.5
                    while max_active <= 32 and time.monotonic() < overflow_deadline:
                        condition.wait(timeout=0.02)
                    observed_max = max_active
            finally:
                release.set()
            failures = []
            for future in futures:
                try:
                    future.result(timeout=5)
                except ImageGenerationError as exc:
                    failures.append(exc)

        self.assertLessEqual(observed_max, 32)
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0].code, "image_generation_queue_full")

    def test_parallel_image_generation_rejects_before_submit_when_capacity_is_full(self) -> None:
        class RecordingExecutor:
            def __init__(self) -> None:
                self.submitted = 0

            def submit(self, *_args, **_kwargs):
                self.submitted += 1
                raise AssertionError("image generation must be rejected before submit")

        executor = RecordingExecutor()
        slots = threading.BoundedSemaphore(2)
        request = ConversationRequest(model="gpt-image-2", prompt="cat", n=3)

        with (
            mock.patch.object(conversation_module, "_IMAGE_GENERATION_EXECUTOR", executor),
            mock.patch.object(conversation_module, "_IMAGE_GENERATION_SLOTS", slots),
            mock.patch.object(
                conversation_module,
                "config",
                SimpleNamespace(image_parallel_generation=True),
            ),
        ):
            with self.assertRaises(ImageGenerationError) as raised:
                list(conversation_module.stream_image_outputs_with_pool(request))

        self.assertEqual(raised.exception.code, "image_generation_queue_full")
        self.assertEqual(executor.submitted, 0)

    def test_image_api_stream_emits_only_official_completed_events(self) -> None:
        usage = {
            "input_tokens": 2,
            "output_tokens": 3,
            "total_tokens": 5,
            "input_tokens_details": {"text_tokens": 2, "image_tokens": 0},
        }
        outputs = [
            ImageOutput(kind="progress", model="gpt-image-2", index=1, total=1, text="working"),
            ImageOutput(
                kind="result",
                model="gpt-image-2",
                index=1,
                total=1,
                created=1_700_000_003,
                data=[{"b64_json": "ZmFrZQ==", "revised_prompt": "revised"}],
            ),
        ]

        events = list(stream_image_events(outputs, lambda _items: usage))

        self.assertEqual(events, [{
            "type": "image_generation.completed",
            "b64_json": "ZmFrZQ==",
            "background": "auto",
            "created_at": 1_700_000_003,
            "output_format": "png",
            "quality": "auto",
            "size": "auto",
            "usage": usage,
        }])

    def test_stream_id_extractor_keeps_full_file_ids(self) -> None:
        payload = (
            '{"conversation_id":"conv-1"} '
            'file-service://file-first_123-extra sediment://sed-second_456-extra'
        )

        conversation_id, file_ids, sediment_ids = extract_conversation_ids(payload)

        self.assertEqual(conversation_id, "conv-1")
        self.assertEqual(file_ids, ["file-first_123-extra"])
        self.assertEqual(sediment_ids, ["sed-second_456-extra"])

    def test_conversation_record_extractor_finds_all_generated_assets(self) -> None:
        backend = FakeBackend()
        conversation = {
            "mapping": {
                "user": {
                    "message": {
                        "author": {"role": "user"},
                        "content": {"parts": ["file-service://file-user-input"]},
                    }
                },
                "tool": {
                    "message": {
                        "author": {"role": "tool"},
                        "create_time": 1,
                        "metadata": {
                            "async_task_type": "image_gen",
                            "nested": {"asset": "file-service://file-second"},
                        },
                        "content": {
                            "content_type": "text",
                            "parts": [
                                {"content_type": "image_asset_pointer", "asset_pointer": "file-service://file-first"},
                                "sediment://sed-first",
                            ],
                        },
                    }
                },
                "assistant": {
                    "message": {
                        "author": {"role": "assistant"},
                        "create_time": 2,
                        "metadata": {},
                        "content": {
                            "parts": [
                                {"content_type": "image_asset_pointer", "asset_pointer": "file-service://file-third"}
                            ]
                        },
                    }
                },
            }
        }

        records = backend._extract_image_tool_records(conversation)
        file_ids = [file_id for record in records for file_id in record["file_ids"]]
        sediment_ids = [sediment_id for record in records for sediment_id in record["sediment_ids"]]

        self.assertEqual(file_ids, ["file-first", "file-second", "file-third"])
        self.assertEqual(sediment_ids, ["sed-first"])

    def test_poll_waits_for_generated_asset_ids_to_settle(self) -> None:
        backend = FakeBackend([
            _conversation(["file-one"]),
            _conversation(["file-one", "file-two"], ["sed-one"]),
            _conversation(["file-one", "file-two"], ["sed-one"]),
        ])

        with (
            mock.patch.dict(
                config.data,
                {
                    "image_poll_initial_wait_secs": 0,
                    "image_poll_interval_secs": 0.5,
                    "image_settle_enabled": True,
                    "image_check_before_hit_enabled": True,
                    "image_settle_secs": 0.5,
                },
            ),
            mock.patch("services.openai_backend_api.time.sleep", lambda _seconds: None),
        ):
            file_ids, sediment_ids = backend._poll_image_results("conv-1", timeout_secs=10)

        self.assertEqual(file_ids, ["file-one", "file-two"])
        self.assertEqual(sediment_ids, ["sed-one"])
        self.assertEqual(backend.calls, 3)

    def test_poll_returns_first_hit_when_settle_is_disabled(self) -> None:
        backend = FakeBackend([
            _conversation(["file-one"]),
            _conversation(["file-one", "file-two"], ["sed-one"]),
        ])

        with (
            mock.patch.dict(
                config.data,
                {
                    "image_poll_initial_wait_secs": 0,
                    "image_poll_interval_secs": 0.5,
                    "image_settle_enabled": False,
                    "image_check_before_hit_enabled": True,
                },
            ),
            mock.patch("services.openai_backend_api.time.sleep", lambda _seconds: None),
        ):
            file_ids, sediment_ids = backend._poll_image_results("conv-1", timeout_secs=10)

        self.assertEqual(file_ids, ["file-one"])
        self.assertEqual(sediment_ids, [])
        self.assertEqual(backend.calls, 1)

    def test_resolver_uses_file_and_sediment_urls(self) -> None:
        backend = FakeBackend()
        backend.file_urls = {"file-one": "https://files.test/one.png"}
        backend.sediment_urls = {
            "sed-one": "https://attachments.test/one.png",
            "sed-two": "https://attachments.test/two.png",
        }

        urls = backend._resolve_image_urls("conv-1", ["file-one"], ["sed-one", "sed-two"])

        self.assertEqual(urls, [
            "https://files.test/one.png",
            "https://attachments.test/one.png",
            "https://attachments.test/two.png",
        ])

    def test_resolver_keeps_stream_ids_when_poll_extension_fails(self) -> None:
        backend = FakeBackend()
        backend.file_urls = {"file-one": "https://files.test/one.png"}
        backend._get_conversation = mock.Mock(side_effect=RuntimeError("poll failed"))

        with mock.patch("services.openai_backend_api.time.sleep", lambda _seconds: None):
            urls = backend.resolve_conversation_image_urls("conv-1", ["file-one"], [], poll=True)

        self.assertEqual(urls, ["https://files.test/one.png"])

    def test_responses_stream_emits_all_image_output_items(self) -> None:
        first = base64.b64encode(b"first").decode("ascii")
        second = base64.b64encode(b"second").decode("ascii")
        events = list(stream_image_response(
            [ImageOutput(
                kind="result",
                model="gpt-image-2",
                index=1,
                total=1,
                data=[{"b64_json": first}, {"b64_json": second}],
            )],
            "draw two options",
            "gpt-image-2",
        ))

        done_events = [event for event in events if event.get("type") == "response.output_item.done"]
        completed = next(event["response"] for event in events if event.get("type") == "response.completed")

        self.assertEqual([event["output_index"] for event in done_events], [0, 1])
        self.assertEqual([item["result"] for item in completed["output"]], [first, second])


if __name__ == "__main__":
    unittest.main()
