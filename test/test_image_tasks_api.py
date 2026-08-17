from __future__ import annotations

import base64
import unittest
from types import SimpleNamespace
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.image_tasks as image_tasks_module
import api.image_inputs as image_inputs_module
from api.errors import install_exception_handlers
from services.image_task_service import ImageTaskNotFoundError, ImageTaskResumeConflictError
from services.task_executor import BackgroundTaskQueueFullError
from test.fixtures.image_inputs import image_fixture_bytes


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
PNG_BYTES = image_fixture_bytes("image.png")
PNG_SECOND_BYTES = image_fixture_bytes("image_edit.png")
DATA_IMAGE_URL = f"data:image/png;base64,{base64.b64encode(PNG_BYTES).decode('ascii')}"


class FakeImageTaskService:
    def __init__(self):
        self.generation_calls = []
        self.edit_calls = []
        self.resume_calls = []
        self.resume_error = None

    def submit_generation(self, identity, **kwargs):
        self.generation_calls.append((identity, kwargs))
        return {
            "id": kwargs["client_task_id"],
            "status": "success",
            "mode": "generate",
            "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00",
            "data": [{"url": f"{kwargs['base_url']}/images/fake.png"}],
        }

    def submit_edit(self, identity, **kwargs):
        self.edit_calls.append((identity, kwargs))
        return {
            "id": kwargs["client_task_id"],
            "status": "queued",
            "mode": "edit",
            "created_at": "2026-01-01 00:00:00",
            "updated_at": "2026-01-01 00:00:00",
        }

    def list_tasks(self, _identity, ids):
        return {
            "items": [
                {
                    "id": task_id,
                    "status": "success",
                    "mode": "generate",
                    "created_at": "2026-01-01 00:00:00",
                    "updated_at": "2026-01-01 00:00:00",
                    "data": [{"url": "http://testserver/images/fake.png"}],
                }
                for task_id in ids
                if task_id != "missing"
            ],
            "missing_ids": [task_id for task_id in ids if task_id == "missing"],
        }

    def resume_poll(self, identity, task_id, extra_timeout_secs):
        self.resume_calls.append((identity, task_id, extra_timeout_secs))
        if self.resume_error is not None:
            raise self.resume_error
        return {"id": task_id, "status": "running"}


class ImageTasksApiTests(unittest.TestCase):
    def setUp(self):
        self.fake_service = FakeImageTaskService()
        self.service_patcher = mock.patch.object(image_tasks_module, "image_task_service", self.fake_service)
        self.service_patcher.start()
        self.addCleanup(self.service_patcher.stop)
        app = FastAPI()
        install_exception_handlers(app)
        app.include_router(image_tasks_module.create_router())
        self.client = TestClient(app)

    def test_create_generation_task(self):
        response = self.client.post(
            "/api/image-tasks/generations",
            headers=AUTH_HEADERS,
            json={"client_task_id": "task-1", "prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual(payload["id"], "task-1")
        self.assertEqual(payload["status"], "success")
        self.assertEqual(len(self.fake_service.generation_calls), 1)

    def test_generation_rejects_oversized_client_task_id_before_service(self):
        response = self.client.post(
            "/api/image-tasks/generations",
            headers=AUTH_HEADERS,
            json={"client_task_id": "x" * 257, "prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 422, response.text)
        self.assertEqual(self.fake_service.generation_calls, [])

    def test_generation_rejects_comma_client_task_id_before_service(self):
        response = self.client.post(
            "/api/image-tasks/generations",
            headers=AUTH_HEADERS,
            json={"client_task_id": "task,one", "prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 422, response.text)
        self.assertEqual(self.fake_service.generation_calls, [])

    def test_generation_uses_relative_image_url_for_untrusted_host(self):
        with mock.patch("api.support.config", SimpleNamespace(base_url="", auth_key="chatgpt2api")):
            response = self.client.post(
                "/api/image-tasks/generations",
                headers={**AUTH_HEADERS, "Host": "attacker.example"},
                json={"client_task_id": "host-poison", "prompt": "cat", "model": "gpt-image-2"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["data"][0]["url"], "/images/fake.png")

    def test_create_edit_task_accepts_multiple_images(self):
        """测试图片编辑任务接口支持多个上传图片。"""
        response = self.client.post(
            "/api/image-tasks/edits",
            headers=AUTH_HEADERS,
            data={"client_task_id": "edit-1", "prompt": "edit", "model": "gpt-image-2"},
            files=[
                ("image", ("one.png", PNG_BYTES, "image/png")),
                ("image", ("two.png", PNG_SECOND_BYTES, "image/png")),
            ],
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.json()["id"], "edit-1")
        self.assertEqual(len(self.fake_service.edit_calls), 1)
        images = self.fake_service.edit_calls[0][1]["images"]
        self.assertEqual(len(images), 2)

    def test_edit_rejects_oversized_client_task_id_before_review_or_image_read(self):
        with mock.patch.object(image_tasks_module, "filter_or_log", new=mock.AsyncMock()) as filter_or_log, \
             mock.patch.object(image_tasks_module, "read_image_sources", new=mock.AsyncMock()) as read_images:
            response = self.client.post(
                "/api/image-tasks/edits",
                headers=AUTH_HEADERS,
                data={"client_task_id": "x" * 257, "prompt": "edit", "model": "gpt-image-2"},
                files=[("image", ("one.png", PNG_BYTES, "image/png"))],
            )

        self.assertEqual(response.status_code, 400, response.text)
        filter_or_log.assert_not_awaited()
        read_images.assert_not_awaited()
        self.assertEqual(self.fake_service.edit_calls, [])

    def test_edit_rejects_comma_client_task_id_before_review_or_image_read(self):
        with mock.patch.object(image_tasks_module, "filter_or_log", new=mock.AsyncMock()) as filter_or_log, \
             mock.patch.object(image_tasks_module, "read_image_sources", new=mock.AsyncMock()) as read_images:
            response = self.client.post(
                "/api/image-tasks/edits",
                headers=AUTH_HEADERS,
                data={"client_task_id": "task,one", "prompt": "edit", "model": "gpt-image-2"},
                files=[("image", ("one.png", PNG_BYTES, "image/png"))],
            )

        self.assertEqual(response.status_code, 400, response.text)
        filter_or_log.assert_not_awaited()
        read_images.assert_not_awaited()
        self.assertEqual(self.fake_service.edit_calls, [])

    def test_json_edit_rejects_oversized_client_task_id_before_decoding_image_url(self):
        with mock.patch.object(image_tasks_module, "filter_or_log", new=mock.AsyncMock()) as filter_or_log, \
             mock.patch.object(image_inputs_module, "_decode_data_url", wraps=image_inputs_module._decode_data_url) as decode_data_url:
            response = self.client.post(
                "/api/image-tasks/edits",
                headers=AUTH_HEADERS,
                json={
                    "client_task_id": "x" * 257,
                    "prompt": "edit",
                    "model": "gpt-image-2",
                    "image_url": DATA_IMAGE_URL,
                },
            )

        self.assertEqual(response.status_code, 400, response.text)
        filter_or_log.assert_not_awaited()
        decode_data_url.assert_not_called()
        self.assertEqual(self.fake_service.edit_calls, [])

    def test_create_edit_task_accepts_image_url(self):
        """测试图片编辑任务接口支持表单 image_url 引用。"""
        response = self.client.post(
            "/api/image-tasks/edits",
            headers=AUTH_HEADERS,
            data={
                "client_task_id": "edit-url-1",
                "prompt": "edit",
                "model": "gpt-image-2",
                "image_url": DATA_IMAGE_URL,
            },
        )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(len(self.fake_service.edit_calls), 1)
        images = self.fake_service.edit_calls[0][1]["images"]
        self.assertEqual(images, [(PNG_BYTES, "image_url.png", "image/png")])

    def test_list_tasks_reports_missing_ids(self):
        response = self.client.get("/api/image-tasks?ids=task-1,missing", headers=AUTH_HEADERS)

        self.assertEqual(response.status_code, 200, response.text)
        payload = response.json()
        self.assertEqual([item["id"] for item in payload["items"]], ["task-1"])
        self.assertEqual(payload["missing_ids"], ["missing"])

    def test_resume_poll_maps_missing_or_foreign_task_to_not_found(self):
        self.fake_service.resume_error = ImageTaskNotFoundError("task not found")

        response = self.client.post(
            "/api/image-tasks/foreign-task/resume-poll",
            headers=AUTH_HEADERS,
            json={"extra_timeout_secs": 5},
        )

        self.assertEqual(response.status_code, 404, response.text)
        self.assertNotIn("foreign-task", response.text)

    def test_resume_poll_maps_invalid_task_state_to_conflict(self):
        self.fake_service.resume_error = ImageTaskResumeConflictError("task is not in error state")

        response = self.client.post(
            "/api/image-tasks/task-1/resume-poll",
            headers=AUTH_HEADERS,
            json={"extra_timeout_secs": 5},
        )

        self.assertEqual(response.status_code, 409, response.text)

    def test_resume_poll_rejects_string_timeout_before_service_call(self):
        response = self.client.post(
            "/api/image-tasks/task-1/resume-poll",
            headers=AUTH_HEADERS,
            json={"extra_timeout_secs": "30"},
        )

        self.assertEqual(response.status_code, 422, response.text)
        self.assertEqual(self.fake_service.resume_calls, [])

    def test_generation_does_not_echo_untrusted_value_error(self):
        secret = "opaque-image-task-secret owner@example.com"
        self.fake_service.submit_generation = mock.Mock(side_effect=ValueError(secret))

        response = self.client.post(
            "/api/image-tasks/generations",
            headers=AUTH_HEADERS,
            json={"client_task_id": "secret-generation", "prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn(secret, response.text)

    def test_generation_queue_full_returns_retryable_public_error(self):
        self.fake_service.submit_generation = mock.Mock(
            side_effect=BackgroundTaskQueueFullError("background task queue is full; try again later")
        )

        response = self.client.post(
            "/api/image-tasks/generations",
            headers=AUTH_HEADERS,
            json={"client_task_id": "queue-full", "prompt": "cat", "model": "gpt-image-2"},
        )

        self.assertEqual(response.status_code, 429, response.text)
        self.assertEqual(response.headers.get("Retry-After"), "1")
        self.assertEqual(
            response.json(),
            {"detail": {"error": "background task queue is full; try again later"}},
        )

    def test_edit_does_not_echo_untrusted_value_error(self):
        secret = "opaque-edit-task-secret owner@example.com"
        self.fake_service.submit_edit = mock.Mock(side_effect=ValueError(secret))

        response = self.client.post(
            "/api/image-tasks/edits",
            headers=AUTH_HEADERS,
            data={"client_task_id": "secret-edit", "prompt": "edit", "model": "gpt-image-2"},
            files=[("image", ("one.png", PNG_BYTES, "image/png"))],
        )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn(secret, response.text)

    def test_resume_does_not_echo_untrusted_domain_error(self):
        secret = "opaque-resume-secret owner@example.com"
        self.fake_service.resume_error = ImageTaskNotFoundError(secret)

        response = self.client.post(
            "/api/image-tasks/secret-resume/resume-poll",
            headers=AUTH_HEADERS,
            json={"extra_timeout_secs": 5},
        )

        self.assertEqual(response.status_code, 404, response.text)
        self.assertNotIn(secret, response.text)


if __name__ == "__main__":
    unittest.main()
