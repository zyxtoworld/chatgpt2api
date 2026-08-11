from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from services.cpa_service import CPAConfig
from services.storage.base import StorageDataError
from services.sub2api_service import Sub2APIConfig


@pytest.mark.parametrize(
    ("config_type", "filename"),
    ((CPAConfig, "cpa_config.json"), (Sub2APIConfig, "sub2api_config.json")),
)
def test_existing_corrupt_management_snapshot_fails_closed(tmp_path, config_type, filename) -> None:
    path = tmp_path / filename
    path.write_text("{broken", encoding="utf-8")

    with pytest.raises(StorageDataError):
        config_type(path)


@pytest.mark.parametrize("config_type", (CPAConfig, Sub2APIConfig))
def test_management_mutation_does_not_overwrite_snapshot_corrupted_after_load(tmp_path, config_type) -> None:
    path = tmp_path / ("cpa_config.json" if config_type is CPAConfig else "sub2api_config.json")
    path.write_text(
        json.dumps([{"id": "existing", "base_url": "https://example.test"}], ensure_ascii=False),
        encoding="utf-8",
    )
    config = config_type(path)
    path.write_text("{broken", encoding="utf-8")

    with pytest.raises(StorageDataError):
        if config_type is CPAConfig:
            config.add_pool("new", "https://new.example.test", "secret")
        else:
            config.add_server(
                name="new",
                base_url="https://new.example.test",
                email="",
                password="",
                api_key="key",
            )

    assert path.read_text(encoding="utf-8") == "{broken"


@pytest.mark.parametrize("config_type", (CPAConfig, Sub2APIConfig))
def test_in_process_pending_import_job_survives_management_reload(tmp_path, config_type) -> None:
    path = tmp_path / ("cpa_config.json" if config_type is CPAConfig else "sub2api_config.json")
    path.write_text(
        json.dumps([{"id": "existing", "base_url": "https://example.test"}], ensure_ascii=False),
        encoding="utf-8",
    )
    config = config_type(path)
    job = {"job_id": "job-1", "status": "pending", "total": 1}

    assert config.set_import_job("existing", job) is not None
    current = config.get_import_job("existing")
    assert current is not None
    assert current["status"] == "pending"


@pytest.mark.parametrize("config_type", (CPAConfig, Sub2APIConfig))
def test_management_snapshot_replace_failure_keeps_previous_file(tmp_path, config_type) -> None:
    path = tmp_path / ("cpa_config.json" if config_type is CPAConfig else "sub2api_config.json")
    original = json.dumps([{"id": "existing", "base_url": "https://example.test"}], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")
    config = config_type(path)

    with mock.patch.object(Path, "replace", side_effect=OSError("replace failed")):
        with pytest.raises(OSError):
            if config_type is CPAConfig:
                config.add_pool("new", "https://new.example.test", "secret")
            else:
                config.add_server(
                    name="new",
                    base_url="https://new.example.test",
                    email="",
                    password="",
                    api_key="key",
                )

    assert path.read_text(encoding="utf-8") == original
    assert not path.with_suffix(path.suffix + ".tmp").exists()
