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


if __name__ == "__main__":
    unittest.main()
