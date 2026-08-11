from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

import services.editable_file_task_service as editable_module
from services.editable_file_task_service import EditableFileTaskService


OWNER = {"id": "owner-1", "name": "Owner", "role": "admin"}


def wait_for_task(service: EditableFileTaskService, task_id: str, status: str) -> dict[str, object]:
    deadline = time.time() + 2
    while time.time() < deadline:
        result = service.list_tasks(OWNER, [task_id])
        if result["items"] and result["items"][0]["status"] == status:
            return result["items"][0]
        time.sleep(0.01)
    raise AssertionError(f"task {task_id!r} did not reach {status!r}")


class EditableFileTaskErrorContractTests(unittest.TestCase):
    def test_unknown_backend_error_is_not_exposed_or_persisted(self) -> None:
        secret = "opaque-editable-token owner@example.com upstream fragment"
        path = Path(tempfile.mkdtemp()) / "editable_file_tasks.json"

        class FakeBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, _output_dir):
                raise RuntimeError(secret)

            def close(self) -> None:
                pass

        service = EditableFileTaskService(path)
        with (
            mock.patch.object(editable_module, "_editable_access_token", return_value="token-editable"),
            mock.patch.object(editable_module, "OpenAIBackendAPI", FakeBackend),
            mock.patch.object(editable_module.log_service, "add") as add,
        ):
            service.submit_ppt(OWNER, client_task_id="secret-error", prompt="make a deck")
            task = wait_for_task(service, "secret-error", "error")

        self.assertNotIn(secret, json.dumps(task, ensure_ascii=False))
        self.assertNotIn(secret, path.read_text(encoding="utf-8"))
        self.assertNotIn(secret, repr(add.call_args_list))

    def test_empty_psd_input_keeps_a_controlled_public_message(self) -> None:
        path = Path(tempfile.mkdtemp()) / "editable_file_tasks.json"
        service = EditableFileTaskService(path)
        with mock.patch.object(editable_module.log_service, "add"):
            task = service.submit_psd(OWNER, client_task_id="empty-psd", prompt="make a psd", base64_images=[])
            task = wait_for_task(service, "empty-psd", "error")

        self.assertEqual(task["error"], "PSD 任务需要至少一张图片")

    def test_client_task_id_cannot_escape_editable_file_root(self) -> None:
        task_ids = (
            "../outside",
            "..\\outside",
            ".",
            "..",
            "/outside",
            r"C:\outside",
        )
        for task_id in task_ids:
            with self.subTest(task_id=task_id), tempfile.TemporaryDirectory() as tmp_dir:
                service = EditableFileTaskService(Path(tmp_dir) / "editable_file_tasks.json")
                with mock.patch.object(editable_module, "reserve_background_task") as reserve:
                    with self.assertRaises(ValueError):
                        service.submit_ppt(OWNER, client_task_id=task_id, prompt="make a deck")

                    reserve.assert_not_called()

    def test_editable_output_dir_has_root_invariant(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "files"
            with mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root):
                output_dir = editable_module._editable_output_dir("owner-1", "ppt", "safe-task")
                self.assertEqual(
                    output_dir,
                    (root / editable_module._owner_storage_segment("owner-1") / "ppt" / "safe-task").resolve(),
                )
                with self.assertRaises(ValueError):
                    editable_module._editable_output_dir("owner-1", "ppt", "../../outside")


if __name__ == "__main__":
    unittest.main()
