from __future__ import annotations

import unittest
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
from api.errors import install_exception_handlers
from services.protocol.error_response import PublicSafeValueError
from services.task_executor import BackgroundTaskQueueFullError


class EditableFileApiContractTests(unittest.TestCase):
    @staticmethod
    def _app() -> FastAPI:
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(ai_module.create_router())
        return app

    def test_invalid_client_task_id_is_a_bad_request(self) -> None:
        app = self._app()
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "owner-1", "role": "admin"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.editable_file_task_service,
                "submit_ppt",
                side_effect=PublicSafeValueError("client_task_id must be a safe path segment"),
            ),
        ):
            response = TestClient(app, raise_server_exceptions=False).post(
                "/v1/ppt/generations",
                json={"client_task_id": "..\\outside", "prompt": "make a deck"},
                headers={"Authorization": "Bearer key"},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertIn("safe path segment", response.text)

    def test_oversized_client_task_id_is_rejected_before_submission(self) -> None:
        app = self._app()
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "owner-1", "role": "admin"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.editable_file_task_service, "submit_ppt") as submit_ppt,
        ):
            response = TestClient(app, raise_server_exceptions=False).post(
                "/v1/ppt/generations",
                json={"client_task_id": "x" * 257, "prompt": "make a deck"},
                headers={"Authorization": "Bearer key"},
            )

        self.assertEqual(response.status_code, 422, response.text)
        self.assertNotIn("x" * 257, response.text)
        submit_ppt.assert_not_called()

    def test_comma_client_task_id_is_rejected_before_submission(self) -> None:
        app = self._app()
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "owner-1", "role": "admin"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.editable_file_task_service, "submit_ppt") as submit_ppt,
        ):
            response = TestClient(app, raise_server_exceptions=False).post(
                "/v1/ppt/generations",
                json={"client_task_id": "task,one", "prompt": "make a deck"},
                headers={"Authorization": "Bearer key"},
            )

        self.assertEqual(response.status_code, 422, response.text)
        submit_ppt.assert_not_called()

    def test_editable_reference_images_have_the_same_sixteen_item_bound(self) -> None:
        app = self._app()
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "owner-1", "role": "admin"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()) as filter_or_log,
            mock.patch.object(ai_module.editable_file_task_service, "submit_ppt") as submit_ppt,
        ):
            response = TestClient(app, raise_server_exceptions=False).post(
                "/v1/ppt/generations",
                json={
                    "client_task_id": "too-many-reference-images",
                    "prompt": "make a deck",
                    "base64_images": ["data:image/png;base64,AA=="] * 17,
                },
                headers={"Authorization": "Bearer key"},
            )

        self.assertEqual(response.status_code, 422, response.text)
        filter_or_log.assert_not_awaited()
        submit_ppt.assert_not_called()

    def test_ppt_does_not_echo_untrusted_value_error(self) -> None:
        secret = "opaque-ppt-secret owner@example.com"
        app = self._app()
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "owner-1", "role": "admin"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.editable_file_task_service, "submit_ppt", side_effect=ValueError(secret)),
        ):
            response = TestClient(app, raise_server_exceptions=False).post(
                "/v1/ppt/generations",
                json={"client_task_id": "secret-ppt", "prompt": "make a deck"},
                headers={"Authorization": "Bearer key"},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn(secret, response.text)

    def test_psd_does_not_echo_untrusted_value_error(self) -> None:
        secret = "opaque-psd-secret owner@example.com"
        app = self._app()
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "owner-1", "role": "admin"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(ai_module.editable_file_task_service, "submit_psd", side_effect=ValueError(secret)),
        ):
            response = TestClient(app, raise_server_exceptions=False).post(
                "/v1/psd/generations",
                json={"client_task_id": "secret-psd", "prompt": "make a psd", "base64_images": ["data:image/png;base64,AA=="]},
                headers={"Authorization": "Bearer key"},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn(secret, response.text)

    def test_ppt_queue_full_returns_openai_retryable_error(self) -> None:
        app = self._app()
        with (
            mock.patch.object(ai_module, "require_identity_async", return_value={"id": "owner-1", "role": "admin"}),
            mock.patch.object(ai_module, "filter_or_log", new=mock.AsyncMock()),
            mock.patch.object(
                ai_module.editable_file_task_service,
                "submit_ppt",
                side_effect=BackgroundTaskQueueFullError("background task queue is full; try again later"),
            ),
        ):
            response = TestClient(app, raise_server_exceptions=False).post(
                "/v1/ppt/generations",
                json={"client_task_id": "queue-full", "prompt": "make a deck"},
                headers={"Authorization": "Bearer key"},
            )

        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(response.headers.get("Retry-After"), "1")
        self.assertEqual(
            response.json()["error"]["message"],
            "background task queue is full; try again later",
        )


if __name__ == "__main__":
    unittest.main()
