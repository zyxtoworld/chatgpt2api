from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.system as system_module
import services.backup_service as backup_module
import services.config as config_module
from api.system import create_router
from services.backup_service import BackupError, BackupService


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}


class BackupPublicErrorContractTests(unittest.TestCase):
    def test_legacy_backup_state_error_is_not_returned(self) -> None:
        secret = "legacy-upstream-secret owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        state_path.write_text(
            json.dumps({
                "last_status": "error",
                "last_error": secret,
            }),
            encoding="utf-8",
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            state = BackupService().get_status()

        self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")
        self.assertNotIn(secret, json.dumps(state, ensure_ascii=False))

    def test_forged_legacy_public_marker_does_not_trust_error_text(self) -> None:
        secret = "forged-state-secret owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        state_path.write_text(
            json.dumps({
                "last_status": "error",
                "last_error": secret,
                "_last_error_public": True,
            }),
            encoding="utf-8",
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            state = BackupService().get_status()

        self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")
        self.assertNotIn(secret, json.dumps(state, ensure_ascii=False))

    def test_forged_marker_and_error_body_are_not_persisted(self) -> None:
        secret = "forged-persisted-secret owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            config_module.save_backup_state(
                {
                    "last_status": "error",
                    "last_error": secret,
                    "_last_error_public": True,
                }
            )
            raw_state = state_path.read_text(encoding="utf-8")
            state = BackupService().get_status()

        self.assertNotIn(secret, raw_state)
        self.assertNotIn("_last_error_public", raw_state)
        self.assertNotIn('"last_error"', raw_state)
        self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")

    def test_backup_state_rebuilds_error_from_allowlisted_code_and_status(self) -> None:
        secret = "persisted-error-body owner@example.com"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        state_path.write_text(
            json.dumps({
                "last_status": "error",
                "last_error": secret,
                "last_error_code": "r2_connection_failed",
                "last_error_status": 503,
            }),
            encoding="utf-8",
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            state = BackupService().get_status()

        self.assertEqual(state["last_error"], "连接 R2 失败：HTTP 503")
        self.assertNotIn(secret, json.dumps(state, ensure_ascii=False))

    def test_unknown_backup_state_code_or_status_falls_back(self) -> None:
        cases = (
            {"last_error_code": "unknown-code", "last_error_status": 503},
            {"last_error_code": "r2_connection_failed", "last_error_status": 700},
            {"last_error_code": "r2_config_incomplete", "last_error_status": "503"},
        )
        for fields in cases:
            with self.subTest(fields=fields), tempfile.TemporaryDirectory() as tmp_dir:
                state_path = Path(tmp_dir) / "backup_state.json"
                state_path.write_text(json.dumps(fields), encoding="utf-8")
                with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
                    state = BackupService().get_status()
                self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")

    def test_invalid_status_is_canonicalized_to_fallback_when_saved(self) -> None:
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            config_module.save_backup_state(
                {
                    "last_status": "error",
                    "last_error_code": "r2_config_incomplete",
                    "last_error_status": "503",
                }
            )
            raw_state = json.loads(state_path.read_text(encoding="utf-8"))
            state = BackupService().get_status()

        self.assertEqual(raw_state["last_error_code"], "backup_failed")
        self.assertNotIn("last_error_status", raw_state)
        self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")

    def test_backup_state_replace_failure_preserves_previous_snapshot(self) -> None:
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        original = json.dumps({"last_status": "idle", "last_object_key": "old"}, ensure_ascii=False) + "\n"
        state_path.write_text(original, encoding="utf-8")

        with (
            mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path),
            mock.patch("os.replace", side_effect=OSError("replace failed")),
        ):
            with self.assertRaises(OSError):
                config_module.save_backup_state({"last_status": "success", "last_object_key": "new"})

        self.assertEqual(state_path.read_text(encoding="utf-8"), original)
        self.assertEqual(list(state_path.parent.glob("backup_state.json.*.tmp")), [])

    def test_unknown_backup_error_is_not_persisted_or_returned(self) -> None:
        secret = "opaque-backup-token owner@example.com upstream fragment"
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        service = BackupService()
        service._run_backup_once = mock.Mock(side_effect=RuntimeError(secret))

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            with self.assertRaises(RuntimeError):
                service.run_backup()

            state = service.get_status()
            raw_state = state_path.read_text(encoding="utf-8")
            self.assertEqual(state["last_error"], "备份执行失败，请稍后重试")
            self.assertNotIn(secret, raw_state)

            app = FastAPI()
            app.include_router(create_router("test"))
            with (
                mock.patch.object(system_module.backup_service, "list_backups", return_value=[]),
                mock.patch.object(system_module.backup_service, "get_settings", return_value={}),
            ):
                response = TestClient(app).get("/api/backups", headers=AUTH_HEADERS)

            self.assertEqual(response.status_code, 200, response.text)
            self.assertNotIn(secret, json.dumps(response.json(), ensure_ascii=False))

    def test_explicit_backup_error_keeps_its_controlled_message(self) -> None:
        state_path = Path(tempfile.mkdtemp()) / "backup_state.json"
        service = BackupService()
        service._run_backup_once = mock.Mock(
            side_effect=BackupError(
                "连接 R2 失败：HTTP 503",
                code="r2_connection_failed",
                status_code=503,
            )
        )

        with mock.patch.object(config_module, "BACKUP_STATE_FILE", state_path):
            with self.assertRaises(BackupError):
                service.run_backup()
            self.assertEqual(service.get_status()["last_error"], "连接 R2 失败：HTTP 503")


if __name__ == "__main__":
    unittest.main()
