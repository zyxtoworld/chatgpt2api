import threading
import unittest
from unittest import mock

from services.config import config
from services.protocol import conversation
from services.protocol.conversation import _remove_image_conversation_later


class FakeBackend:
    def __init__(self, access_token: str = "source-token") -> None:
        self.access_token = access_token
        self.called = threading.Event()
        self.closed = threading.Event()

    def delete_conversation(self, conversation_id: str) -> dict:
        self.called.set()
        return {}

    def close(self) -> None:
        self.closed.set()


class RemoveImageConversationGateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = dict(config.data)

    def tearDown(self) -> None:
        config.data = self._saved

    def _removed(self, *, after_result: bool, always: bool, success: bool) -> bool:
        config.data = dict(
            self._saved,
            image_remove_conversation_after_result=after_result,
            image_remove_conversation_always=always,
        )
        source_backend = FakeBackend()
        cleanup_backend = FakeBackend(access_token=source_backend.access_token)
        with mock.patch.object(conversation, "OpenAIBackendAPI", return_value=cleanup_backend):
            _remove_image_conversation_later(source_backend, "conv-1", success=success)
            called = cleanup_backend.called.wait(2.0)
            if called:
                self.assertTrue(cleanup_backend.closed.wait(2.0))
            return called

    def test_both_off_never_removes(self) -> None:
        self.assertFalse(self._removed(after_result=False, always=False, success=True))
        self.assertFalse(self._removed(after_result=False, always=False, success=False))

    def test_after_result_only_removes_on_success(self) -> None:
        self.assertTrue(self._removed(after_result=True, always=False, success=True))
        self.assertFalse(self._removed(after_result=True, always=False, success=False))

    def test_always_removes_regardless_of_success(self) -> None:
        self.assertTrue(self._removed(after_result=False, always=True, success=True))
        self.assertTrue(self._removed(after_result=False, always=True, success=False))

    def test_empty_conversation_id_is_noop(self) -> None:
        config.data = dict(self._saved, image_remove_conversation_always=True)
        backend = FakeBackend()
        _remove_image_conversation_later(backend, "", success=False)
        self.assertFalse(backend.called.wait(0.2))

    def test_cleanup_uses_and_closes_an_independent_backend(self) -> None:
        config.data = dict(self._saved, image_remove_conversation_always=True)
        source_backend = FakeBackend()
        cleanup_backend = FakeBackend(access_token=source_backend.access_token)

        with mock.patch.object(
            conversation,
            "OpenAIBackendAPI",
            return_value=cleanup_backend,
        ) as backend_factory:
            _remove_image_conversation_later(source_backend, "conv-1", success=False)

            self.assertTrue(cleanup_backend.called.wait(2.0))
            self.assertTrue(cleanup_backend.closed.wait(2.0))

        self.assertFalse(source_backend.called.is_set())
        backend_factory.assert_called_once_with(access_token="source-token")

    def test_cleanup_workers_and_pending_queue_are_bounded(self) -> None:
        config.data = dict(self._saved, image_remove_conversation_always=True)
        condition = threading.Condition()
        release = threading.Event()
        active = 0
        max_active = 0
        finished = 0

        class BlockingCleanupBackend:
            def __init__(self, *, access_token: str) -> None:
                self.access_token = access_token

            def delete_conversation(self, _conversation_id: str) -> dict:
                nonlocal active, max_active, finished
                with condition:
                    active += 1
                    max_active = max(max_active, active)
                    condition.notify_all()
                try:
                    if not release.wait(timeout=5):
                        raise AssertionError("cleanup worker was not released")
                    return {}
                finally:
                    with condition:
                        active -= 1
                        finished += 1
                        condition.notify_all()

            def close(self) -> None:
                pass

        source_backend = FakeBackend()
        with mock.patch.object(conversation, "OpenAIBackendAPI", BlockingCleanupBackend):
            try:
                for index in range(24):
                    _remove_image_conversation_later(
                        source_backend,
                        f"bounded-cleanup-{index}",
                        success=False,
                    )

                with condition:
                    self.assertTrue(
                        condition.wait_for(lambda: active >= 4, timeout=2),
                        "cleanup workers did not reach the bounded baseline",
                    )
                    self.assertLessEqual(max_active, 4)
            finally:
                release.set()
                with condition:
                    condition.wait_for(lambda: finished >= 16, timeout=3)

        self.assertEqual(finished, 16)


if __name__ == "__main__":
    unittest.main()
