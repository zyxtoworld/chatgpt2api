from __future__ import annotations

import hashlib
import io
import json
import os
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from urllib.parse import urlsplit
from unittest import TestCase, mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.ai as ai_module
import services.editable_file_task_service as editable_module
from services.editable_file_task_service import EditableFileTaskService


def _owner_scope(owner_id: str) -> str:
    return hashlib.sha256(owner_id.encode()).hexdigest()


class InlineReservation:
    def submit(self, target, *args, **kwargs) -> None:
        target(*args, **kwargs)

    def cancel(self) -> None:
        pass


class EditableFileDownloadContractTests(TestCase):
    def _replace_directory_with_link(self, directory: Path, foreign_directory: Path) -> None:
        shutil.rmtree(directory)
        if os.name == "nt":
            result = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(directory), str(foreign_directory)],
                capture_output=True,
                text=True,
            )
            if result.returncode:
                self.skipTest(f"junction fixture unavailable: {result.stderr or result.stdout}")
        else:
            os.symlink(foreign_directory, directory, target_is_directory=True)

    def _remove_directory_link(self, directory: Path) -> None:
        if directory.is_symlink():
            directory.unlink()
            return
        is_junction = getattr(directory, "is_junction", None)
        if os.name == "nt" and callable(is_junction) and is_junction():
            directory.unlink()

    def _service_with_owner_file(self, root: Path, task_path: Path, capability: str) -> EditableFileTaskService:
        output_dir = root / _owner_scope("owner-a") / "ppt" / "task-a"
        output_dir.mkdir(parents=True)
        (output_dir / "primary.pptx").write_bytes(b"owner-a-ppt")
        task_path.write_text(
            json.dumps(
                {
                    "tasks": [
                        {
                            "id": "task-a",
                            "owner_id": "owner-a",
                            "status": "success",
                            "kind": "ppt",
                            "created_at": "2026-08-09 10:00:00",
                            "updated_at": "2026-08-09 10:00:01",
                            "created_ts": 1,
                            "updated_ts": 2,
                            "download_capability_hashes": {
                                "primary.pptx": hashlib.sha256(
                                    "\0".join(
                                        (capability, "owner-a", "ppt", "task-a", "primary.pptx"),
                                    ).encode(),
                                ).hexdigest(),
                            },
                            "result": {
                                "primary_url": "/files/ppt/task-a/primary.pptx",
                            },
                        },
                    ],
                },
            ),
            encoding="utf-8",
        )
        return EditableFileTaskService(task_path)

    def test_download_reads_fixed_handle_after_path_replacement_barrier(self) -> None:
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            primary_path = root / "primary.pptx"
            foreign_path = root / "foreign.pptx"
            primary_path.write_bytes(b"owner-a-data")
            foreign_path.write_bytes(b"owner-b-secret")
            opened = False

            class BarrierService:
                def public_file_path(self, _file_path: str) -> Path:
                    return primary_path

                def open_public_file(self, _file_path: str):
                    nonlocal opened
                    opened = True
                    stat_result = os.stat(primary_path)
                    fixed_file = io.BytesIO(b"owner-a-data")
                    foreign_path.replace(primary_path)
                    return SimpleNamespace(
                        file=fixed_file,
                        filename=primary_path.name,
                        stat_result=stat_result,
                    )

            app = FastAPI()
            app.include_router(ai_module.create_router())

            with mock.patch.object(ai_module, "editable_file_task_service", BarrierService()):
                client = TestClient(app, raise_server_exceptions=False)
                response = client.get("/files/replacement-race")

            self.assertTrue(opened)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.content, b"owner-a-data")
            self.assertNotIn(b"owner-b-secret", response.content)

    def test_real_service_rejects_replacement_before_verified_open(self) -> None:
        capability = "capability-owner-a-0123456789"
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            task_path = Path(temp_dir) / "editable_file_tasks.json"
            service = self._service_with_owner_file(root, task_path, capability)
            primary_path = root / _owner_scope("owner-a") / "ppt" / "task-a" / "primary.pptx"
            foreign_path = root / _owner_scope("owner-b") / "ppt" / "foreign-task" / "foreign.pptx"
            foreign_path.parent.mkdir(parents=True)
            foreign_path.write_bytes(b"owner-b-secret")
            capability_path = f"/files/{capability}/{_owner_scope('owner-a')}/ppt/task-a/primary.pptx"

            real_open = getattr(editable_module, "_open_verified_file", None)

            def replace_before_open(path: Path, output_dir: Path):
                foreign_path.replace(primary_path)
                return real_open(path, output_dir)

            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "_open_verified_file", side_effect=replace_before_open),
            ):
                with self.assertRaises(FileNotFoundError):
                    service.open_public_file(capability_path.removeprefix("/files/"))

            self.assertEqual(primary_path.read_bytes(), b"owner-b-secret")

    def test_download_rejects_a_rebound_editable_file_root(self) -> None:
        capability = "capability-owner-a-0123456789"
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            foreign = Path(temp_dir) / "outside"
            task_path = Path(temp_dir) / "editable_file_tasks.json"
            service = self._service_with_owner_file(root, task_path, capability)
            foreign_path = foreign / _owner_scope("owner-a") / "ppt" / "task-a" / "primary.pptx"
            foreign_path.parent.mkdir(parents=True)
            foreign_path.write_bytes(b"owner-b-secret")
            capability_path = f"{capability}/{_owner_scope('owner-a')}/ppt/task-a/primary.pptx"
            try:
                self._replace_directory_with_link(root, foreign)
                with mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root):
                    with self.assertRaises(FileNotFoundError):
                        service.open_public_file(capability_path)
                self.assertEqual(foreign_path.read_bytes(), b"owner-b-secret")
            finally:
                self._remove_directory_link(root)

    def test_windows_handle_path_check_rejects_junction_resolved_outside_task(self) -> None:
        if os.name != "nt":
            self.skipTest("Windows handle contract")
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            owner_dir = root / "owner-a" / "ppt" / "task-a"
            lexical_path = owner_dir / "primary.pptx"
            foreign_path = root / "owner-b" / "ppt" / "foreign-task" / "primary.pptx"

            with mock.patch.object(Path, "resolve", return_value=foreign_path):
                old_expected_path = editable_module._normalize_windows_handle_path(str(lexical_path.resolve()))
            self.assertEqual(
                old_expected_path,
                editable_module._normalize_windows_handle_path(str(foreign_path)),
            )
            with self.assertRaises(OSError):
                editable_module._validate_windows_handle_path(
                    str(foreign_path),
                    lexical_path,
                    owner_dir,
                )

    def test_download_capability_is_not_written_to_call_log(self) -> None:
        class LocalBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, output_dir: Path):
                output_dir.mkdir(parents=True, exist_ok=True)
                primary_path = output_dir / "primary.pptx"
                zip_path = output_dir / "assets.zip"
                primary_path.write_bytes(b"primary")
                zip_path.write_bytes(b"zip")
                return SimpleNamespace(
                    conversation_id="log-contract-conversation",
                    primary_path=primary_path,
                    zip_path=zip_path,
                )

            def close(self) -> None:
                pass

        capability = "capability-secret-must-not-be-logged"
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            task_path = Path(temp_dir) / "editable_file_tasks.json"
            service = EditableFileTaskService(task_path)
            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "reserve_background_task", return_value=InlineReservation()),
                mock.patch.object(editable_module, "_new_download_capability", return_value=capability),
                mock.patch.object(editable_module, "_editable_access_token", return_value="backend-token"),
                mock.patch.object(editable_module, "OpenAIBackendAPI", LocalBackend),
                mock.patch.object(editable_module.account_service, "mark_text_used"),
                mock.patch.object(editable_module.log_service, "add") as log_add,
            ):
                service.submit_ppt(
                    {"id": "owner-a", "role": "user"},
                    client_task_id="log-contract",
                    prompt="A",
                )
                task = service.list_tasks({"id": "owner-a"}, ["log-contract"])["items"][0]

            self.assertIn(capability, json.dumps(task, ensure_ascii=False))
            logged = json.dumps(log_add.call_args_list, ensure_ascii=False, default=str)
            self.assertNotIn(capability, logged)
            self.assertNotIn("/files/", logged)

    def test_file_download_is_bound_to_task_capability_and_owner(self) -> None:
        capability = "capability-owner-a-0123456789"
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            task_path = Path(temp_dir) / "editable_file_tasks.json"
            service = self._service_with_owner_file(root, task_path, capability)
            app = FastAPI()
            app.include_router(ai_module.create_router())

            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(ai_module, "editable_file_task_service", service),
            ):
                client = TestClient(app, raise_server_exceptions=False)
                capability_path = f"/files/{capability}/{_owner_scope('owner-a')}/ppt/task-a/primary.pptx"

                owner_response = client.get(capability_path)
                self.assertEqual(owner_response.status_code, 200, owner_response.text)
                self.assertEqual(owner_response.content, b"owner-a-ppt")

                bare_path_response = client.get("/files/ppt/task-a/primary.pptx")
                self.assertEqual(bare_path_response.status_code, 404, bare_path_response.text)

                self.assertEqual(
                    client.get(
                        capability_path,
                        headers={"Authorization": "Bearer owner-b-key"},
                    ).status_code,
                    200,
                )

    def test_same_task_id_has_owner_scoped_outputs_and_persistent_download_capabilities(self) -> None:
        class FakeBackend:
            calls = 0

            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, output_dir: Path):
                type(self).calls += 1
                label = b"owner-a" if self.calls == 1 else b"owner-b"
                output_dir.mkdir(parents=True, exist_ok=True)
                primary_path = output_dir / "primary.pptx"
                zip_path = output_dir / "assets.zip"
                primary_path.write_bytes(label)
                zip_path.write_bytes(label + b"-zip")
                return SimpleNamespace(
                    conversation_id=f"conversation-{self.calls}",
                    primary_path=primary_path,
                    zip_path=zip_path,
                )

            def close(self) -> None:
                pass

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            task_path = Path(temp_dir) / "editable_file_tasks.json"
            service = EditableFileTaskService(task_path)
            owner_a = {"id": "owner-a", "role": "user"}
            owner_b = {"id": "owner-b", "role": "user"}
            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "reserve_background_task", return_value=InlineReservation()),
                mock.patch.object(editable_module, "_editable_access_token", return_value="backend-token"),
                mock.patch.object(editable_module, "OpenAIBackendAPI", FakeBackend),
                mock.patch.object(editable_module.account_service, "mark_text_used"),
            ):
                service.submit_ppt(owner_a, client_task_id="shared", prompt="A")
                service.submit_ppt(owner_b, client_task_id="shared", prompt="B")

                task_a = service.list_tasks(owner_a, ["shared"])["items"][0]
                task_b = service.list_tasks(owner_b, ["shared"])["items"][0]
                self.assertEqual(task_a["status"], "success")
                self.assertEqual(task_b["status"], "success")
                self.assertNotIn("download_capability", task_a)
                self.assertNotIn("download_capability_hashes", task_a)
                url_a = str(task_a["result"]["primary_url"])
                url_b = str(task_b["result"]["primary_url"])
                self.assertNotEqual(url_a, url_b)

                output_a = root / editable_module._owner_storage_segment("owner-a") / "ppt" / "shared"
                output_b = root / editable_module._owner_storage_segment("owner-b") / "ppt" / "shared"
                self.assertEqual((output_a / "primary.pptx").read_bytes(), b"owner-a")
                self.assertEqual((output_b / "primary.pptx").read_bytes(), b"owner-b")
                self.assertFalse((root / "ppt" / "shared").exists())

                reloaded = EditableFileTaskService(task_path)
                reloaded_a = reloaded.list_tasks(owner_a, ["shared"])["items"][0]
                self.assertEqual(reloaded_a["result"]["primary_url"], url_a)
                self.assertTrue(reloaded._tasks["owner-a:shared"].get("download_capability_hashes"))

                app = FastAPI()
                app.include_router(ai_module.create_router())
                with (
                    mock.patch.object(ai_module, "editable_file_task_service", reloaded),
                ):
                    client = TestClient(app, raise_server_exceptions=False)
                    path_a = urlsplit(url_a).path
                    path_b = urlsplit(url_b).path
                    owner_response = client.get(path_a)
                    self.assertEqual(owner_response.status_code, 200, owner_response.text)
                    self.assertEqual(owner_response.content, b"owner-a")
                    self.assertEqual(owner_response.headers["content-disposition"], 'attachment; filename="primary.pptx"')

                    range_response = client.get(path_a, headers={"Range": "bytes=0-5"})
                    self.assertEqual(range_response.status_code, 206, range_response.text)
                    self.assertEqual(range_response.content, b"owner-")

                    head_response = client.head(path_a)
                    self.assertEqual(head_response.status_code, 200, head_response.text)
                    self.assertEqual(head_response.content, b"")

                    self.assertEqual(client.get(path_b).status_code, 200)
                    self.assertEqual(client.get("/files/ppt/shared/primary.pptx").status_code, 404)
                    self.assertEqual(client.head("/files/ppt/shared/primary.pptx").status_code, 404)
                    self.assertEqual(
                        client.get("/files/ppt/shared/primary.pptx", headers={"Range": "bytes=0-5"}).status_code,
                        404,
                    )

                    path_parts_a = path_a.split("/")
                    path_parts_b = path_b.split("/")
                    swapped_capability = "/".join(path_parts_b[:3] + path_parts_a[3:])
                    swapped_scope = "/".join(path_parts_a[:3] + path_parts_b[3:])
                    swapped_file = "/".join(path_parts_a[:-1] + ["other.pptx"])
                    swapped_task = "/".join(path_parts_a[:5] + ["other-task", "primary.pptx"])
                    self.assertEqual(
                        client.get(swapped_capability).status_code,
                        404,
                    )
                    self.assertEqual(
                        client.get(swapped_scope).status_code,
                        404,
                    )
                    self.assertEqual(
                        client.get(swapped_file).status_code,
                        404,
                    )
                    self.assertEqual(
                        client.get(swapped_task).status_code,
                        404,
                    )
                    for escaped in (
                        f"/files/{path_parts_a[2]}/{path_parts_a[3]}/ppt/shared/%2e%2e/primary.pptx",
                        f"/files/{path_parts_a[2]}/{path_parts_a[3]}/ppt/shared/..%5Cprimary.pptx",
                    ):
                        self.assertEqual(client.get(escaped).status_code, 404)

    def test_download_rejects_replaced_file_symlink(self) -> None:
        capability = "capability-owner-a-0123456789"
        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            task_path = Path(temp_dir) / "editable_file_tasks.json"
            service = self._service_with_owner_file(root, task_path, capability)
            foreign_dir = root / _owner_scope("owner-b") / "ppt" / "foreign-task"
            foreign_dir.mkdir(parents=True)
            foreign_path = foreign_dir / "secret.pptx"
            foreign_path.write_bytes(b"owner-b-secret")
            primary_path = root / _owner_scope("owner-a") / "ppt" / "task-a" / "primary.pptx"
            primary_path.unlink()
            try:
                primary_path.symlink_to(foreign_path)
            except (OSError, NotImplementedError) as exc:
                self.skipTest(f"symlink fixture unavailable: {exc}")

            app = FastAPI()
            app.include_router(ai_module.create_router())
            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(ai_module, "editable_file_task_service", service),
            ):
                client = TestClient(app, raise_server_exceptions=False)
                path = f"/files/{capability}/{_owner_scope('owner-a')}/ppt/task-a/primary.pptx"
                response = client.get(path)

            self.assertEqual(response.status_code, 404, response.text)
            self.assertNotIn("owner-b-secret", response.text)

    def test_backend_foreign_file_is_not_signed_or_persisted(self) -> None:
        class ForeignBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, _output_dir: Path):
                foreign_dir = root / _owner_scope("owner-b") / "ppt" / "foreign-task"
                foreign_dir.mkdir(parents=True, exist_ok=True)
                foreign_path = foreign_dir / "foreign.pptx"
                foreign_path.write_bytes(b"owner-b-data")
                return SimpleNamespace(
                    conversation_id="foreign-conversation",
                    primary_path=foreign_path,
                    zip_path=foreign_path,
                )

            def close(self) -> None:
                pass

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            task_path = Path(temp_dir) / "editable_file_tasks.json"
            service = EditableFileTaskService(task_path)
            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "reserve_background_task", return_value=InlineReservation()),
                mock.patch.object(editable_module, "_editable_access_token", return_value="backend-token"),
                mock.patch.object(editable_module, "OpenAIBackendAPI", ForeignBackend),
                mock.patch.object(editable_module.account_service, "mark_text_used"),
            ):
                service.submit_ppt({"id": "owner-a", "role": "user"}, client_task_id="foreign", prompt="A")
                task = service.list_tasks({"id": "owner-a"}, ["foreign"])["items"][0]

            self.assertEqual(task["status"], "error")
            self.assertNotIn("result", task)
            self.assertNotIn("download_capability_hashes", service._tasks["owner-a:foreign"])
            persisted = task_path.read_text(encoding="utf-8")
            self.assertNotIn("primary_url", persisted)
            self.assertNotIn("download_capability_hashes", persisted)

    def test_result_and_capability_are_atomic_when_completion_persist_fails(self) -> None:
        class LocalBackend:
            def __init__(self, _access_token: str) -> None:
                pass

            def export_ppt_zip(self, _images, _prompt, output_dir: Path):
                output_dir.mkdir(parents=True, exist_ok=True)
                primary_path = output_dir / "primary.pptx"
                zip_path = output_dir / "assets.zip"
                primary_path.write_bytes(b"primary")
                zip_path.write_bytes(b"zip")
                return SimpleNamespace(
                    conversation_id="atomic-conversation",
                    primary_path=primary_path,
                    zip_path=zip_path,
                )

            def close(self) -> None:
                pass

        with TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "files"
            task_path = Path(temp_dir) / "editable_file_tasks.json"
            service = EditableFileTaskService(task_path)
            original_save = service._save_locked
            save_calls = 0

            def fail_completion_save_once() -> None:
                nonlocal save_calls
                save_calls += 1
                if save_calls == 3:
                    raise OSError("completion snapshot write failed")
                original_save()

            with (
                mock.patch.object(editable_module, "EDITABLE_FILE_ROOT", root),
                mock.patch.object(editable_module, "reserve_background_task", return_value=InlineReservation()),
                mock.patch.object(editable_module, "_editable_access_token", return_value="backend-token"),
                mock.patch.object(editable_module, "OpenAIBackendAPI", LocalBackend),
                mock.patch.object(editable_module.account_service, "mark_text_used"),
                mock.patch.object(service, "_save_locked", side_effect=fail_completion_save_once),
            ):
                service.submit_ppt({"id": "owner-a", "role": "user"}, client_task_id="atomic", prompt="A")

            task = service.list_tasks({"id": "owner-a"}, ["atomic"])["items"][0]
            self.assertEqual(task["status"], "error")
            self.assertNotIn("result", task)
            self.assertNotIn("download_capability_hashes", service._tasks["owner-a:atomic"])
            persisted = task_path.read_text(encoding="utf-8")
            self.assertNotIn("primary_url", persisted)
            self.assertNotIn("download_capability_hashes", persisted)


if __name__ == "__main__":
    import unittest

    unittest.main()
