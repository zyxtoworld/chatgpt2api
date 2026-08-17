from __future__ import annotations

import threading
import time
import unittest
from unittest import mock

from services.openai_backend_api import (
    ImageStreamHardTimeoutError,
    OpenAIBackendAPI,
)


class FakeResponse:
    def __init__(self) -> None:
        self.closed = threading.Event()

    def close(self) -> None:
        self.closed.set()


class ImageStreamHardTimeoutTests(unittest.TestCase):
    def test_payload_arriving_after_deadline_is_not_emitted(self) -> None:
        response = FakeResponse()
        backend = object.__new__(OpenAIBackendAPI)

        def late_payloads(_response):
            time.sleep(0.03)
            yield "late payload"

        with mock.patch(
            "services.openai_backend_api.iter_sse_payloads",
            side_effect=late_payloads,
        ):
            payloads = backend._iter_sse_payloads_capped(response, 0.01)
            try:
                with self.assertRaises(ImageStreamHardTimeoutError):
                    next(payloads)
            finally:
                payloads.close()

        self.assertTrue(response.closed.wait(1.0))

    def test_watchdog_close_exception_is_mapped_before_clock_reaches_deadline(self) -> None:
        response = FakeResponse()
        backend = object.__new__(OpenAIBackendAPI)
        timer_holder: dict[str, object] = {}

        class FakeTimer:
            def __init__(self, _delay, callback):
                self.callback = callback
                timer_holder["timer"] = self

            def start(self) -> None:
                pass

            def cancel(self) -> None:
                pass

        def closed_stream(_response):
            timer_holder["timer"].callback()
            raise RuntimeError("closed by curl")

        with (
            mock.patch("services.openai_backend_api.threading.Timer", FakeTimer),
            mock.patch("services.openai_backend_api.time.monotonic", side_effect=[0.0, 0.9]),
            mock.patch("services.openai_backend_api.iter_sse_payloads", side_effect=closed_stream),
        ):
            with self.assertRaises(ImageStreamHardTimeoutError):
                next(backend._iter_sse_payloads_capped(response, 1.0))

        self.assertTrue(response.closed.is_set())

    def test_watchdog_close_eof_is_still_a_hard_timeout(self) -> None:
        response = FakeResponse()
        backend = object.__new__(OpenAIBackendAPI)
        timer_holder: dict[str, object] = {}

        class FakeTimer:
            def __init__(self, _delay, callback):
                self.callback = callback
                timer_holder["timer"] = self

            def start(self) -> None:
                pass

            def cancel(self) -> None:
                pass

        def closed_stream(_response):
            timer_holder["timer"].callback()
            return iter(())

        with (
            mock.patch("services.openai_backend_api.threading.Timer", FakeTimer),
            mock.patch("services.openai_backend_api.time.monotonic", side_effect=[0.0, 0.9]),
            mock.patch("services.openai_backend_api.iter_sse_payloads", side_effect=closed_stream),
        ):
            with self.assertRaises(ImageStreamHardTimeoutError):
                list(backend._iter_sse_payloads_capped(response, 1.0))

        self.assertTrue(response.closed.is_set())


if __name__ == "__main__":
    unittest.main()
