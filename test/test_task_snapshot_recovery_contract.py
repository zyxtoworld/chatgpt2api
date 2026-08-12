from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import services.editable_file_task_service as editable_module
import services.image_task_service as image_module
from services.editable_file_task_service import EditableFileTaskService
from services.image_task_service import ImageTaskService
from services.storage.base import StorageDataError


OWNER = {"id": "owner-1", "name": "Owner", "role": "admin"}


class TaskSnapshotRecoveryContractTests(unittest.TestCase):
    def test_editable_constructor_rejects_corrupt_existing_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            path.write_text("{broken", encoding="utf-8")

            with self.assertRaises(StorageDataError):
                EditableFileTaskService(path)

    def test_editable_mutation_does_not_overwrite_corrupt_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            path.write_text('{"tasks": []}\n', encoding="utf-8")
            service = EditableFileTaskService(path)
            path.write_text("{opaque-corruption", encoding="utf-8")
            original = path.read_bytes()

            with mock.patch.object(editable_module.threading, "Thread") as thread:
                with self.assertRaises(StorageDataError):
                    service.submit_ppt(OWNER, client_task_id="new-task", prompt="make a deck")

            self.assertEqual(path.read_bytes(), original)
            self.assertNotIn("owner-1:new-task", service._tasks)
            thread.assert_not_called()

    def test_editable_replace_failure_preserves_snapshot_and_cleans_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            path.write_text('{"tasks": []}\n', encoding="utf-8")
            service = EditableFileTaskService(path)
            original = path.read_bytes()

            with (
                mock.patch.object(editable_module, "atomic_write_bytes", side_effect=OSError("replace failed")),
                mock.patch.object(editable_module.threading, "Thread") as thread,
            ):
                with self.assertRaises(OSError):
                    service.submit_ppt(OWNER, client_task_id="replace-task", prompt="make a deck")

            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(path.with_suffix(path.suffix + ".tmp").exists())
            self.assertNotIn("owner-1:replace-task", service._tasks)
            thread.assert_not_called()

    def test_editable_save_does_not_clobber_preexisting_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "editable_file_tasks.json"
            stale_temp = path.with_suffix(path.suffix + ".tmp")
            stale_temp.write_text("preexisting temp data", encoding="utf-8")
            path.write_text('{"tasks": []}\n', encoding="utf-8")
            service = EditableFileTaskService(path)

            with mock.patch.object(editable_module, "reserve_background_task") as reserve:
                service.submit_ppt(OWNER, client_task_id="collision-task", prompt="make a deck")
            reserve.return_value.submit.assert_called_once()

            self.assertEqual(stale_temp.read_text(encoding="utf-8"), "preexisting temp data")

    def test_image_constructor_rejects_corrupt_existing_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            path.write_text("{broken", encoding="utf-8")

            with self.assertRaises(StorageDataError):
                ImageTaskService(path)

    def test_image_mutation_does_not_overwrite_corrupt_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            path.write_text('{"tasks": []}\n', encoding="utf-8")
            service = ImageTaskService(
                path,
                generation_handler=lambda _payload: {"data": [{"url": "http://example.test/image.png"}]},
                edit_handler=lambda _payload: {"data": [{"url": "http://example.test/edit.png"}]},
                retention_days_getter=lambda: 30,
            )
            path.write_text("{opaque-corruption", encoding="utf-8")
            original = path.read_bytes()

            with mock.patch.object(image_module.threading, "Thread") as thread:
                with self.assertRaises(StorageDataError):
                    service.submit_generation(
                        OWNER,
                        client_task_id="new-task",
                        prompt="make an image",
                        model="gpt-image-2",
                        size=None,
                    )

            self.assertEqual(path.read_bytes(), original)
            self.assertNotIn("owner-1:new-task", service._tasks)
            thread.assert_not_called()

    def test_image_replace_failure_preserves_snapshot_and_cleans_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            path.write_text('{"tasks": []}\n', encoding="utf-8")
            service = ImageTaskService(
                path,
                generation_handler=lambda _payload: {"data": [{"url": "http://example.test/image.png"}]},
                edit_handler=lambda _payload: {"data": [{"url": "http://example.test/edit.png"}]},
                retention_days_getter=lambda: 30,
            )
            original = path.read_bytes()

            with (
                mock.patch.object(image_module, "atomic_write_bytes", side_effect=OSError("replace failed")),
                mock.patch.object(image_module.threading, "Thread") as thread,
            ):
                with self.assertRaises(OSError):
                    service.submit_generation(
                        OWNER,
                        client_task_id="replace-task",
                        prompt="make an image",
                        model="gpt-image-2",
                        size=None,
                    )

            self.assertEqual(path.read_bytes(), original)
            self.assertFalse(path.with_suffix(path.suffix + ".tmp").exists())
            self.assertNotIn("owner-1:replace-task", service._tasks)
            thread.assert_not_called()

    def test_image_save_does_not_clobber_preexisting_temp_file(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "image_tasks.json"
            stale_temp = path.with_suffix(path.suffix + ".tmp")
            stale_temp.write_text("preexisting temp data", encoding="utf-8")
            path.write_text('{"tasks": []}\n', encoding="utf-8")
            service = ImageTaskService(
                path,
                generation_handler=lambda _payload: {"data": [{"url": "http://example.test/image.png"}]},
                edit_handler=lambda _payload: {"data": [{"url": "http://example.test/edit.png"}]},
                retention_days_getter=lambda: 30,
            )

            with mock.patch.object(image_module, "reserve_background_task") as reserve:
                service.submit_generation(
                    OWNER,
                    client_task_id="collision-task",
                    prompt="make an image",
                    model="gpt-image-2",
                    size=None,
                )
            reserve.return_value.submit.assert_called_once()

            self.assertEqual(stale_temp.read_text(encoding="utf-8"), "preexisting temp data")


if __name__ == "__main__":
    unittest.main()
