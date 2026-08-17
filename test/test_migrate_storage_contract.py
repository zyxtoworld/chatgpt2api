from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path
from unittest import mock

from services.storage.json_storage import JSONStorageBackend


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "migrate_storage.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("migrate_storage_under_test", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Storage:
    def __init__(self) -> None:
        self.closed = False

    def load_accounts(self):
        return [{"access_token": "export-secret-token"}]

    def close(self) -> None:
        self.closed = True


class _FailingStorage(_Storage):
    def save_accounts(self, _accounts) -> None:
        raise ValueError("primary storage failure")

    def close(self) -> None:
        raise RuntimeError("cleanup failure")


def test_account_export_uses_private_atomic_writer_and_preserves_old_file_on_failure(tmp_path: Path) -> None:
    module = _load_module()
    output_path = tmp_path / "export.json"
    old_bytes = b'{"old":true}\n'
    output_path.write_bytes(old_bytes)

    with (
        mock.patch.object(module, "DATA_DIR", tmp_path),
        mock.patch.object(module, "create_storage_backend", return_value=_Storage()),
        mock.patch.object(
            module,
            "atomic_write_bytes",
            side_effect=OSError("simulated export write failure"),
            create=True,
        ) as atomic_write,
    ):
        try:
            module.export_to_json(str(output_path))
        except OSError:
            pass
        else:
            raise AssertionError("export must surface the write failure")

    assert atomic_write.called
    assert output_path.read_bytes() == old_bytes


def test_account_export_writes_private_file_mode(tmp_path: Path) -> None:
    module = _load_module()
    output_path = tmp_path / "export.json"

    with (
        mock.patch.object(module, "DATA_DIR", tmp_path),
        mock.patch.object(module, "create_storage_backend", return_value=_Storage()),
    ):
        module.export_to_json(str(output_path))

    assert json.loads(output_path.read_text(encoding="utf-8"))[0]["access_token"] == "export-secret-token"
    if os.name == "posix":
        assert output_path.stat().st_mode & 0o777 == 0o600


def test_account_export_closes_storage_backend(tmp_path: Path) -> None:
    module = _load_module()
    output_path = tmp_path / "export.json"
    storage = _Storage()

    with (
        mock.patch.object(module, "DATA_DIR", tmp_path),
        mock.patch.object(module, "create_storage_backend", return_value=storage),
    ):
        module.export_to_json(str(output_path))

    assert storage.closed


def test_import_preserves_primary_storage_error_when_close_also_fails(tmp_path: Path) -> None:
    module = _load_module()
    input_path = tmp_path / "accounts.json"
    input_path.write_text("[]", encoding="utf-8")

    with (
        mock.patch.object(module, "DATA_DIR", tmp_path),
        mock.patch.object(module, "create_storage_backend", return_value=_FailingStorage()),
    ):
        try:
            module.import_from_json(str(input_path))
        except ValueError as exc:
            assert str(exc) == "primary storage failure"
        else:
            raise AssertionError("the primary storage error must be preserved")


def test_export_preserves_cumulative_total_from_real_json_backend(tmp_path: Path) -> None:
    module = _load_module()
    backend = JSONStorageBackend(tmp_path / "accounts.json", tmp_path / "auth_keys.json")
    records = [{"access_token": "token-a", "type": "free"}]
    backend.save_accounts_with_cumulative_total(
        backend.load_accounts_snapshot(),
        records,
        9,
    )
    output_path = tmp_path / "export.json"

    with (
        mock.patch.object(module, "DATA_DIR", tmp_path),
        mock.patch.object(module, "create_storage_backend", return_value=backend),
    ):
        module.export_to_json(str(output_path))

    exported = json.loads(output_path.read_text(encoding="utf-8"))
    assert exported["items"] == records
    assert exported["cumulative_total"] == 9


def test_import_preserves_cumulative_total_into_real_json_backend(tmp_path: Path) -> None:
    module = _load_module()
    source = JSONStorageBackend(tmp_path / "source_accounts.json", tmp_path / "source_auth.json")
    records = [{"access_token": "token-a", "type": "free"}]
    source.save_accounts_with_cumulative_total(
        source.load_accounts_snapshot(),
        records,
        9,
    )
    export_path = tmp_path / "accounts.json"
    target = JSONStorageBackend(tmp_path / "target_accounts.json", tmp_path / "target_auth.json")

    with (
        mock.patch.object(module, "DATA_DIR", tmp_path),
        mock.patch.object(module, "create_storage_backend", return_value=source),
    ):
        module.export_to_json(str(export_path))

    with (
        mock.patch.object(module, "DATA_DIR", tmp_path),
        mock.patch.object(module, "create_storage_backend", return_value=target),
    ):
        module.import_from_json(str(export_path))

    assert target.load_accounts() == records
    assert target.load_cumulative_total() == 9
