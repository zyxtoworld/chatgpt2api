from __future__ import annotations

import json
from unittest import mock

import pytest

import services.ccload_service as ccload_module
import services.cpa_service as cpa_module
import services.sub2api_service as sub2api_module
from services.ccload_service import CCLoadConfig
from services.cpa_service import CPAConfig
from services.storage.base import StorageDataError
from services.sub2api_service import Sub2APIConfig


CONFIG_CASES = (
    (CPAConfig, "cpa_config.json"),
    (Sub2APIConfig, "sub2api_config.json"),
    (CCLoadConfig, "ccload_config.json"),
)


def _job(*, errors: object = None, **overrides: object) -> dict:
    result = {
        "job_id": "job-1",
        "status": "failed",
        "created_at": "2026-08-11T00:00:00+00:00",
        "updated_at": "2026-08-11T00:00:01+00:00",
        "total": 1,
        "completed": 1,
        "added": 0,
        "skipped": 0,
        "refreshed": 0,
        "failed": 0,
        "errors": [] if errors is None else errors,
    }
    result.update(overrides)
    return result


def _record(config_type: type, job: dict) -> dict:
    if config_type is CPAConfig:
        return {
            "id": "pool-1",
            "name": "CPA",
            "base_url": "https://cpa.example.test",
            "secret_key": "secret",
            "import_job": job,
        }
    if config_type is Sub2APIConfig:
        return {
            "id": "server-1",
            "name": "Sub2API",
            "base_url": "https://sub2api.example.test",
            "email": "admin@example.test",
            "password": "password",
            "api_key": "",
            "group_id": "",
            "import_job": job,
        }
    return {
        "id": "server-1",
        "name": "ccLoad",
        "base_url": "https://ccload.example.test",
        "password": "password",
        "import_job": job,
    }


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
@pytest.mark.parametrize(
    "errors",
    (
        ["not-a-dict"],
        [{"name": "item-1", "error": 123}],
        [{"name": "item-1"}],
        [{"name": "item-1", "error": "import failed", "extra": "unexpected"}],
        [{"name": "item-1", "error": "opaque persisted secret"}],
        [{"name": "item-1", "error": " import failed "}],
        [{"name": "item-1", "error": "HTTP 500 "}],
        [{"name": "item-1", "error": "import failed"}] * 101,
    ),
)
def test_import_job_error_snapshot_must_be_canonical(
    tmp_path,
    config_type,
    filename,
    errors,
) -> None:
    path = tmp_path / filename
    original = json.dumps([_record(config_type, _job(errors=errors))], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")

    with pytest.raises(StorageDataError):
        config_type(path)

    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
@pytest.mark.parametrize(
    "overrides",
    (
        {"total": 0, "completed": 1},
        {"total": 1, "completed": 0, "failed": 1},
        {"total": 1, "completed": 2},
    ),
)
def test_import_job_snapshot_counts_must_be_consistent(
    tmp_path,
    config_type,
    filename,
    overrides,
) -> None:
    path = tmp_path / filename
    original = json.dumps([_record(config_type, _job(**overrides))], ensure_ascii=False)
    path.write_text(original, encoding="utf-8")

    with pytest.raises(StorageDataError):
        config_type(path)

    assert path.read_text(encoding="utf-8") == original


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
def test_canonical_import_job_errors_round_trip(tmp_path, config_type, filename) -> None:
    path = tmp_path / filename
    record = _record(
        config_type,
        _job(errors=[{"name": "item-1", "error": "import failed"}]),
    )
    path.write_text(json.dumps([record], ensure_ascii=False), encoding="utf-8")

    config = config_type(path)
    if config_type is CPAConfig:
        loaded = config.list_pools()[0]
    else:
        loaded = config.list_servers()[0]
    assert loaded["import_job"]["errors"] == [{"name": "item-1", "error": "import failed"}]


class _FailedFuture:
    def result(self):
        return None, "opaque import failure"


class _FailedExecutor:
    def submit(self, *_args):
        return _FailedFuture()


class _ResultFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class _ResultExecutor:
    def __init__(self, outcomes):
        self._outcomes = outcomes

    def submit(self, _function, _pool, name):
        return _ResultFuture(self._outcomes[name])


def _cpa_outcomes() -> dict[str, tuple[str | None, str | None]]:
    return {
        "file-1.json": ("token-1", None),
        "file-2.json": ("token-2", None),
        "file-3.json": (None, "credential unavailable"),
    }


def test_cpa_transition_keeps_failed_count_separate_from_bounded_errors(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = [f"file-{index}.json" for index in range(101)]
    config.set_import_job(pool["id"], _job(status="pending", total=101, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _FailedExecutor()),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures: iter(futures)),
    ):
        service._run_import(pool["id"], pool, names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 101
    assert job["failed"] == 101
    assert len(job["errors"]) == 100


@pytest.mark.parametrize(
    "add_result",
    (
        {},
        None,
        [],
        "invalid",
        {"items": []},
        {"added": True, "skipped": 0},
        {"added": 1.5, "skipped": 0},
        {"added": -1, "skipped": 0},
        {"added": "1", "skipped": 0},
    ),
)
def test_cpa_add_failure_terminates_job_without_refresh(tmp_path, add_result) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = list(_cpa_outcomes())
    config.set_import_job(pool["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor(_cpa_outcomes())),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures: iter(futures)),
        mock.patch.object(cpa_module.account_service, "add_accounts", return_value=add_result) as add_accounts,
        mock.patch.object(cpa_module.account_service, "refresh_accounts") as refresh_accounts,
    ):
        service._run_import(pool["id"], pool, names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert job["added"] == 0
    assert job["refreshed"] == 0
    assert len(job["errors"]) <= 100
    assert all(set(error) == {"name", "error"} for error in job["errors"])
    add_accounts.assert_called_once()
    refresh_accounts.assert_not_called()


def test_cpa_add_exception_terminates_job_without_refresh(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = list(_cpa_outcomes())
    config.set_import_job(pool["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor(_cpa_outcomes())),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures: iter(futures)),
        mock.patch.object(
            cpa_module.account_service,
            "add_accounts",
            side_effect=RuntimeError("account import failed"),
        ) as add_accounts,
        mock.patch.object(cpa_module.account_service, "refresh_accounts") as refresh_accounts,
    ):
        service._run_import(pool["id"], pool, names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert job["added"] == 0
    assert job["refreshed"] == 0
    assert len(job["errors"]) <= 100
    add_accounts.assert_called_once()
    refresh_accounts.assert_not_called()


def test_cpa_refresh_exception_preserves_known_counts(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = list(_cpa_outcomes())
    config.set_import_job(pool["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor(_cpa_outcomes())),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures: iter(futures)),
        mock.patch.object(
            cpa_module.account_service,
            "add_accounts",
            return_value={"added": 2, "skipped": 0},
        ) as add_accounts,
        mock.patch.object(
            cpa_module.account_service,
            "refresh_accounts",
            side_effect=RuntimeError("account refresh failed"),
        ) as refresh_accounts,
    ):
        service._run_import(pool["id"], pool, names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 1
    assert job["added"] == 2
    assert job["skipped"] == 0
    assert job["refreshed"] == 0
    add_accounts.assert_called_once()
    refresh_accounts.assert_called_once()


@pytest.mark.parametrize(
    "refresh_result",
    (
        {"refreshed": True},
        {"refreshed": 1.5},
        {"refreshed": -1},
        {"refreshed": "1"},
        {"items": []},
    ),
)
def test_cpa_invalid_refresh_count_terminates_job(tmp_path, refresh_result) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = list(_cpa_outcomes())
    config.set_import_job(pool["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor(_cpa_outcomes())),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures: iter(futures)),
        mock.patch.object(
            cpa_module.account_service,
            "add_accounts",
            return_value={"added": 2, "skipped": 0},
        ) as add_accounts,
        mock.patch.object(cpa_module.account_service, "refresh_accounts", return_value=refresh_result) as refresh_accounts,
    ):
        service._run_import(pool["id"], pool, names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 1
    assert job["added"] == 2
    assert job["skipped"] == 0
    assert job["refreshed"] == 0
    add_accounts.assert_called_once()
    refresh_accounts.assert_called_once()


def test_cpa_all_success_fetch_finishes_with_zero_failed(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = ["file-1.json", "file-2.json"]
    outcomes = {name: (f"token-{index}", None) for index, name in enumerate(names, 1)}
    config.set_import_job(pool["id"], _job(status="pending", total=2, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor(outcomes)),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures: iter(futures)),
        mock.patch.object(cpa_module.account_service, "add_accounts", return_value={"added": 2, "skipped": 0}),
        mock.patch.object(cpa_module.account_service, "refresh_accounts", return_value={"refreshed": 2}),
    ):
        service._run_import(pool["id"], pool, names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "completed"
    assert job["completed"] == 2
    assert job["failed"] == 0


def test_sub2api_transition_keeps_failed_count_separate_from_bounded_errors(tmp_path) -> None:
    config = Sub2APIConfig(tmp_path / "sub2api.json")
    server = config.add_server(
        name="Sub2API",
        base_url="https://sub2api.example.test",
        email="admin@example.test",
        password="password",
        api_key="",
    )
    service = sub2api_module.Sub2APIImportService(config)
    account_ids = [f"account-{index}" for index in range(101)]
    errors = [{"name": account_id, "error": "opaque import failure"} for account_id in account_ids]
    config.set_import_job(server["id"], _job(status="pending", total=101, completed=0))

    with mock.patch.object(
        sub2api_module,
        "_fetch_access_tokens_for_accounts",
        return_value=([], errors),
    ):
        service._run_import(server["id"], server, account_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 101
    assert job["failed"] == 101
    assert len(job["errors"]) == 100


def _sub2api_server_and_service(tmp_path):
    config = Sub2APIConfig(tmp_path / "sub2api.json")
    server = config.add_server(
        name="Sub2API",
        base_url="https://sub2api.example.test",
        email="admin@example.test",
        password="password",
        api_key="",
    )
    return config, server, sub2api_module.Sub2APIImportService(config)


@pytest.mark.parametrize(
    "add_result",
    (
        {},
        None,
        [],
        "invalid",
        {"items": []},
        {"added": True, "skipped": 0},
        {"added": 1.5, "skipped": 0},
        {"added": -1, "skipped": 0},
        {"added": "1", "skipped": 0},
    ),
)
def test_sub2api_add_failure_terminates_job_without_refresh(tmp_path, add_result) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    account_ids = ["account-1", "account-2", "account-3"]
    config.set_import_job(server["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(
            sub2api_module,
            "_fetch_access_tokens_for_accounts",
            return_value=(["token-1", "token-2"], [{"name": "account-3", "error": "credential unavailable"}]),
        ),
        mock.patch.object(sub2api_module.account_service, "add_accounts", return_value=add_result) as add_accounts,
        mock.patch.object(sub2api_module.account_service, "refresh_accounts") as refresh_accounts,
    ):
        service._run_import(server["id"], server, account_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert job["added"] == 0
    assert job["refreshed"] == 0
    assert len(job["errors"]) <= 100
    add_accounts.assert_called_once()
    refresh_accounts.assert_not_called()


def test_sub2api_add_exception_terminates_job_without_refresh(tmp_path) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    account_ids = ["account-1", "account-2", "account-3"]
    config.set_import_job(server["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(
            sub2api_module,
            "_fetch_access_tokens_for_accounts",
            return_value=(["token-1", "token-2"], [{"name": "account-3", "error": "credential unavailable"}]),
        ),
        mock.patch.object(
            sub2api_module.account_service,
            "add_accounts",
            side_effect=RuntimeError("account import failed"),
        ) as add_accounts,
        mock.patch.object(sub2api_module.account_service, "refresh_accounts") as refresh_accounts,
    ):
        service._run_import(server["id"], server, account_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert job["added"] == 0
    assert job["refreshed"] == 0
    add_accounts.assert_called_once()
    refresh_accounts.assert_not_called()


def test_sub2api_refresh_exception_preserves_known_counts(tmp_path) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    account_ids = ["account-1", "account-2", "account-3"]
    config.set_import_job(server["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(
            sub2api_module,
            "_fetch_access_tokens_for_accounts",
            return_value=(["token-1", "token-2"], [{"name": "account-3", "error": "credential unavailable"}]),
        ),
        mock.patch.object(
            sub2api_module.account_service,
            "add_accounts",
            return_value={"added": 2, "skipped": 0},
        ) as add_accounts,
        mock.patch.object(
            sub2api_module.account_service,
            "refresh_accounts",
            side_effect=RuntimeError("account refresh failed"),
        ) as refresh_accounts,
    ):
        service._run_import(server["id"], server, account_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 1
    assert job["added"] == 2
    assert job["skipped"] == 0
    assert job["refreshed"] == 0
    add_accounts.assert_called_once()
    refresh_accounts.assert_called_once()


@pytest.mark.parametrize(
    "refresh_result",
    (
        {"refreshed": True},
        {"refreshed": 1.5},
        {"refreshed": -1},
        {"refreshed": "1"},
        {"items": []},
    ),
)
def test_sub2api_invalid_refresh_count_terminates_job(tmp_path, refresh_result) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    account_ids = ["account-1", "account-2", "account-3"]
    config.set_import_job(server["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(
            sub2api_module,
            "_fetch_access_tokens_for_accounts",
            return_value=(["token-1", "token-2"], [{"name": "account-3", "error": "credential unavailable"}]),
        ),
        mock.patch.object(
            sub2api_module.account_service,
            "add_accounts",
            return_value={"added": 2, "skipped": 0},
        ) as add_accounts,
        mock.patch.object(sub2api_module.account_service, "refresh_accounts", return_value=refresh_result) as refresh_accounts,
    ):
        service._run_import(server["id"], server, account_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 1
    assert job["added"] == 2
    assert job["skipped"] == 0
    assert job["refreshed"] == 0
    add_accounts.assert_called_once()
    refresh_accounts.assert_called_once()


def test_sub2api_all_success_fetch_finishes_with_zero_failed(tmp_path) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    account_ids = ["account-1", "account-2"]
    config.set_import_job(server["id"], _job(status="pending", total=2, completed=0))

    with (
        mock.patch.object(
            sub2api_module,
            "_fetch_access_tokens_for_accounts",
            return_value=(["token-1", "token-2"], []),
        ),
        mock.patch.object(sub2api_module.account_service, "add_accounts", return_value={"added": 2, "skipped": 0}),
        mock.patch.object(sub2api_module.account_service, "refresh_accounts", return_value={"refreshed": 2}),
    ):
        service._run_import(server["id"], server, account_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "completed"
    assert job["completed"] == 2
    assert job["failed"] == 0


def test_ccload_transition_keeps_failed_count_separate_from_bounded_errors(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    channel_ids = [str(index) for index in range(102)]
    errors = [{"name": channel_id, "error": "credential unavailable"} for channel_id in channel_ids[1:]]
    config.set_import_job(server["id"], _job(status="pending", total=102, completed=0))

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=([{"access_token": "access-token"}], errors),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            return_value={"added": 1, "skipped": 0},
        ),
        mock.patch.object(
            ccload_module.account_service,
            "refresh_accounts",
            return_value={"refreshed": 1},
        ),
    ):
        service._run_import(server["id"], server, channel_ids)

    job = config.get_import_job(server["id"])
    assert job["completed"] == 102
    assert job["failed"] == 101
    assert len(job["errors"]) == 100


def test_ccload_add_accounts_failure_marks_the_entire_selected_batch_failed(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    channel_ids = ["1", "2", "3"]
    errors = [{"name": "3", "error": "credential unavailable"}]
    config.set_import_job(server["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=([{"access_token": "access-token-1"}, {"access_token": "access-token-2"}], errors),
        ),
        mock.patch.object(ccload_module.account_service, "add_account_items", return_value={}) as add_accounts,
        mock.patch.object(
            ccload_module.account_service,
            "refresh_accounts",
            return_value={"refreshed": 1},
        ) as refresh_accounts,
    ):
        service._run_import(server["id"], server, channel_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert len(job["errors"]) <= 100
    assert all(set(error) == {"name", "error"} for error in job["errors"])
    add_accounts.assert_called_once()
    refresh_accounts.assert_not_called()


def test_ccload_add_exception_does_not_use_bounded_errors_as_failed_count(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    channel_ids = [str(index) for index in range(102)]
    errors = [{"name": channel_id, "error": "credential unavailable"} for channel_id in channel_ids[1:]]
    config.set_import_job(server["id"], _job(status="pending", total=102, completed=0))

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=([{"access_token": "access-token"}], errors),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            side_effect=RuntimeError("account import failed"),
        ),
        mock.patch.object(ccload_module.account_service, "refresh_accounts") as refresh_accounts,
    ):
        service._run_import(server["id"], server, channel_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 102
    assert job["failed"] == 102
    assert len(job["errors"]) == 100
    refresh_accounts.assert_not_called()


def test_ccload_refresh_exception_preserves_known_counts_and_bounded_errors(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    channel_ids = [str(index) for index in range(102)]
    errors = [{"name": channel_id, "error": "credential unavailable"} for channel_id in channel_ids[1:]]
    config.set_import_job(server["id"], _job(status="pending", total=102, completed=0))

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=([{"access_token": "access-token"}], errors),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            return_value={"added": 1, "skipped": 0},
        ),
        mock.patch.object(
            ccload_module.account_service,
            "refresh_accounts",
            side_effect=RuntimeError("account refresh failed"),
        ),
    ):
        service._run_import(server["id"], server, channel_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 102
    assert job["added"] == 1
    assert job["refreshed"] == 0
    assert job["failed"] == 101
    assert len(job["errors"]) == 100
