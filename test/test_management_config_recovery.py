from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from services.cpa_service import CPAConfig
import services.cpa_service as cpa_module
import services.sub2api_service as sub2api_module
from services.storage.base import StorageDataError
from services.sub2api_service import Sub2APIConfig


def _valid_import_job(status: str = "completed") -> dict:
    return {
        "job_id": "job-1",
        "status": status,
        "created_at": "2026-08-11T00:00:00+00:00",
        "updated_at": "2026-08-11T00:00:01+00:00",
        "total": 1,
        "completed": 1 if status == "completed" else 0,
        "added": 0,
        "skipped": 0,
        "refreshed": 0,
        "failed": 0,
        "errors": [],
    }


def _management_record(config_type, import_job: dict) -> dict:
    if config_type is CPAConfig:
        return {
            "id": "existing",
            "name": "CPA",
            "base_url": "https://example.test",
            "secret_key": "secret",
            "import_job": import_job,
        }
    return {
        "id": "existing",
        "name": "Sub2API",
        "base_url": "https://example.test",
        "email": "admin@example.test",
        "password": "password",
        "api_key": "",
        "group_id": "",
        "import_job": import_job,
    }


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


@pytest.mark.parametrize(
    ("config_type", "filename"),
    ((CPAConfig, "cpa_config.json"), (Sub2APIConfig, "sub2api_config.json")),
)
@pytest.mark.parametrize("missing", (False, True))
def test_management_snapshot_requires_stable_nonempty_id(tmp_path, config_type, filename, missing) -> None:
    path = tmp_path / filename
    record = _management_record(config_type, _valid_import_job())
    if missing:
        record.pop("id")
    else:
        record["id"] = ""
    original = json.dumps([record], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")

    with pytest.raises(StorageDataError):
        config_type(path)

    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize("legacy_id", (None, ""))
def test_cpa_legacy_single_object_is_migrated_once_with_stable_id(tmp_path, legacy_id) -> None:
    path = tmp_path / "cpa_config.json"
    legacy = {
        "name": "CPA",
        "base_url": "https://example.test",
        "secret_key": "secret",
        "import_job": None,
    }
    if legacy_id is not None:
        legacy["id"] = legacy_id
    path.write_text(json.dumps(legacy, ensure_ascii=False), encoding="utf-8")

    first = CPAConfig(path)
    first_pools = first.list_pools()
    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(persisted, list)
    assert persisted[0]["id"] == "legacy-cpa"
    assert first_pools[0]["id"] == "legacy-cpa"

    second = CPAConfig(path)
    assert second.list_pools() == first_pools
    assert second._snapshot_revision == first._snapshot_revision


@pytest.mark.parametrize("config_type", (CPAConfig, Sub2APIConfig))
def test_in_process_pending_import_job_survives_management_reload(tmp_path, config_type) -> None:
    path = tmp_path / ("cpa_config.json" if config_type is CPAConfig else "sub2api_config.json")
    path.write_text(
        json.dumps([{"id": "existing", "base_url": "https://example.test"}], ensure_ascii=False),
        encoding="utf-8",
    )
    config = config_type(path)
    job = _valid_import_job("pending")
    job["completed"] = 0

    assert config.set_import_job("existing", job) is not None
    current = config.get_import_job("existing")
    assert current is not None
    assert current["status"] == "pending"


@pytest.mark.parametrize(
    ("config_type", "filename"),
    ((CPAConfig, "cpa_config.json"), (Sub2APIConfig, "sub2api_config.json")),
)
@pytest.mark.parametrize("field", ("status", "job_id", "created_at", "updated_at"))
@pytest.mark.parametrize("missing", (False, True))
def test_management_import_job_requires_nonempty_snapshot_fields(
    tmp_path,
    config_type,
    filename,
    field,
    missing,
) -> None:
    path = tmp_path / filename
    job = _valid_import_job()
    if missing:
        job.pop(field)
    else:
        job[field] = ""
    original = json.dumps([_management_record(config_type, job)], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")

    with pytest.raises(StorageDataError):
        config_type(path)

    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    ("config_type", "filename", "collection_method"),
    (
        (CPAConfig, "cpa_config.json", "list_pools"),
        (Sub2APIConfig, "sub2api_config.json", "list_servers"),
    ),
)
@pytest.mark.parametrize("status", ("pending", "running"))
def test_restart_persists_unfinished_import_as_failed(
    tmp_path,
    config_type,
    filename,
    collection_method,
    status,
) -> None:
    path = tmp_path / filename
    original = json.dumps([_management_record(config_type, _valid_import_job(status))], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")

    first = config_type(path)
    first_items = getattr(first, collection_method)()
    assert first_items[0]["import_job"]["status"] == "failed"

    persisted = json.loads(path.read_text(encoding="utf-8"))
    assert persisted[0]["import_job"]["status"] == "failed"

    second = config_type(path)
    second_items = getattr(second, collection_method)()
    assert second_items[0]["import_job"]["status"] == "failed"


@pytest.mark.parametrize("config_type", (CPAConfig, Sub2APIConfig))
def test_management_snapshot_replace_failure_keeps_previous_file(tmp_path, config_type) -> None:
    path = tmp_path / ("cpa_config.json" if config_type is CPAConfig else "sub2api_config.json")
    original = json.dumps([{"id": "existing", "base_url": "https://example.test"}], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")
    config = config_type(path)

    module = cpa_module if config_type is CPAConfig else sub2api_module
    with mock.patch.object(module, "atomic_write_bytes", side_effect=OSError("replace failed")):
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


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("base_url", ["https://example.test"]),
        ("api_key", {"value": "opaque"}),
        ("import_job", "not-a-job"),
        ("import_job", {"status": "unknown"}),
        ("import_job", {"status": "failed", "total": "1"}),
    ),
)
def test_sub2api_rejects_semantically_corrupt_snapshot_without_rewriting(tmp_path, field, value) -> None:
    path = tmp_path / "sub2api.json"
    payload = {
        "id": "existing",
        "base_url": "https://example.test",
        "email": "admin@example.test",
        "password": "password",
        "api_key": "",
    }
    payload[field] = value
    original = json.dumps([payload], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")

    with pytest.raises(StorageDataError):
        Sub2APIConfig(path)

    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("base_url", ["https://example.test"]),
        ("secret_key", {"value": "opaque"}),
        ("import_job", "not-a-job"),
        ("import_job", {"status": "unknown"}),
        ("import_job", {"status": "failed", "total": "1"}),
    ),
)
def test_cpa_rejects_semantically_corrupt_snapshot_without_rewriting(tmp_path, field, value) -> None:
    path = tmp_path / "cpa.json"
    payload = {
        "id": "existing",
        "name": "CPA",
        "base_url": "https://example.test",
        "secret_key": "secret",
    }
    payload[field] = value
    original = json.dumps([payload], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")

    with pytest.raises(StorageDataError):
        CPAConfig(path)

    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(
    ("config_type", "filename"),
    ((CPAConfig, "cpa_config.json"), (Sub2APIConfig, "sub2api_config.json")),
)
def test_management_save_does_not_clobber_preexisting_temp_file(tmp_path, config_type, filename) -> None:
    path = tmp_path / filename
    stale_temp = path.with_suffix(path.suffix + ".tmp")
    stale_temp.write_text("preexisting temp data", encoding="utf-8")
    path.write_text(
        json.dumps([{"id": "existing", "base_url": "https://example.test"}], ensure_ascii=False),
        encoding="utf-8",
    )
    config = config_type(path)

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

    assert stale_temp.read_text(encoding="utf-8") == "preexisting temp data"
