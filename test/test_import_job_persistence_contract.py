from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event
from unittest import mock

import pytest

import services.ccload_service as ccload_module
import services.cpa_service as cpa_module
import services.secure_file as secure_file
import services.sub2api_service as sub2api_module
import services.task_executor as task_executor_module
from services.ccload_service import CCLoadConfig
from services.cpa_service import CPAConfig
from services.protocol.error_response import ImportJobActiveError
from services.storage.base import StorageDataError
from services.sub2api_service import Sub2APIConfig


CONFIG_CASES = (
    (CPAConfig, "cpa_config.json"),
    (Sub2APIConfig, "sub2api_config.json"),
    (CCLoadConfig, "ccload_config.json"),
)


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
def test_import_config_does_not_read_replaced_snapshot_path(tmp_path, config_type, filename) -> None:
    path = tmp_path / filename
    outside = tmp_path / f"outside-{filename}"
    original = [_record(config_type, _job())]
    replaced = [_record(config_type, _job(job_id="replaced-job"))]
    path.write_text(json.dumps(original), encoding="utf-8")
    outside.write_text(json.dumps(replaced), encoding="utf-8")
    original_read_text = Path.read_text

    def replace_before_path_read(target, *args, **kwargs):
        if target == path and path.exists():
            path.unlink()
            outside.replace(path)
        return original_read_text(target, *args, **kwargs)

    with mock.patch.object(Path, "read_text", autospec=True, side_effect=replace_before_path_read):
        config = config_type(path)

    items = config._pools if config_type is CPAConfig else config._servers
    assert items[0]["import_job"]["job_id"] == "job-1"


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
def test_import_config_save_passes_authorized_parent_identity_to_secure_writer(
    tmp_path, config_type, filename
) -> None:
    root = tmp_path / "state"
    root.mkdir()
    path = root / filename
    path.write_text("[]\n", encoding="utf-8")
    config = config_type(path)
    expected_identity = (root.stat().st_dev, root.stat().st_ino)
    if config_type is CPAConfig:
        config_module = cpa_module
    elif config_type is Sub2APIConfig:
        config_module = sub2api_module
    else:
        config_module = ccload_module

    def checked_atomic_write(target, authorized_root, payload, **kwargs):
        assert target == path
        assert authorized_root == root
        assert kwargs.pop("expected_root_identity", None) == expected_identity
        return secure_file.atomic_write_bytes(target, authorized_root, payload, **kwargs)

    with mock.patch.object(config_module, "atomic_write_bytes", side_effect=checked_atomic_write):
        if config_type is CPAConfig:
            config.add_pool("CPA", "https://cpa.example.test", "cpa-secret")
        elif config_type is Sub2APIConfig:
            config.add_server(
                name="Sub2API",
                base_url="https://sub2api.example.test",
                email="admin@example.test",
                password="sub2api-password",
                api_key="sub2api-key",
            )
        else:
            config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="ccload-password",
            )


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
@pytest.mark.parametrize("status", ("pending", "running"))
def test_delete_connection_rejects_an_active_import_job(tmp_path, config_type, filename, status) -> None:
    path = tmp_path / filename
    config = config_type(path)
    if config_type is CPAConfig:
        connection = config.add_pool("CPA", "https://cpa.example.test", "secret")
        delete = config.delete_pool
    elif config_type is Sub2APIConfig:
        connection = config.add_server(
            name="Sub2API",
            base_url="https://sub2api.example.test",
            email="admin@example.test",
            password="password",
            api_key="",
        )
        delete = config.delete_server
    else:
        connection = config.add_server(
            name="ccLoad",
            base_url="https://ccload.example.test",
            password="password",
        )
        delete = config.delete_server

    config.set_import_job(connection["id"], _job(status=status, completed=0))

    with pytest.raises(ImportJobActiveError):
        delete(connection["id"])

    assert config.get_import_job(connection["id"])["status"] == status


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
@pytest.mark.parametrize("status", ("completed", "failed"))
def test_delete_connection_allows_terminal_import_job(tmp_path, config_type, filename, status) -> None:
    path = tmp_path / filename
    config = config_type(path)
    if config_type is CPAConfig:
        connection = config.add_pool("CPA", "https://cpa.example.test", "secret")
        delete = config.delete_pool
    elif config_type is Sub2APIConfig:
        connection = config.add_server(
            name="Sub2API",
            base_url="https://sub2api.example.test",
            email="admin@example.test",
            password="password",
            api_key="",
        )
        delete = config.delete_server
    else:
        connection = config.add_server(
            name="ccLoad",
            base_url="https://ccload.example.test",
            password="password",
        )
        delete = config.delete_server

    config.set_import_job(
        connection["id"],
        _job(status=status, completed=1, failed=1 if status == "failed" else 0),
    )

    assert delete(connection["id"]) is True
    assert config.get_import_job(connection["id"]) is None


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
def test_delete_missing_connection_returns_false(tmp_path, config_type, filename) -> None:
    config = config_type(tmp_path / filename)
    delete = config.delete_pool if config_type is CPAConfig else config.delete_server
    assert delete("missing-connection") is False


@pytest.mark.parametrize(("config_type", "filename"), CONFIG_CASES)
def test_import_config_reads_the_fixed_handle_after_path_replacement(tmp_path, config_type, filename) -> None:
    path = tmp_path / filename
    outside = tmp_path / f"outside-{filename}"
    original = [_record(config_type, _job())]
    replaced = [_record(config_type, _job(job_id="replaced-job"))]
    path.write_text(json.dumps(original), encoding="utf-8")
    outside.write_text(json.dumps(replaced), encoding="utf-8")
    original_open = secure_file.open_no_follow_file

    def replace_after_open(target, root, expected_dir):
        opened = original_open(target, root, expected_dir)
        if target == path:
            path.unlink()
            outside.replace(path)
        return opened

    with mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_after_open):
        config = config_type(path)

    items = config._pools if config_type is CPAConfig else config._servers
    assert items[0]["import_job"]["job_id"] == "job-1"


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


def _cpa_pool_with_persisted_job(config: CPAConfig, pool: dict) -> dict:
    current = config.get_pool(pool["id"])
    assert current is not None
    return current


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
        {"total": 1, "added": 2},
        {"total": 1, "skipped": 2},
        {"total": 1, "added": 1, "skipped": 1},
        {"total": 1, "refreshed": 2},
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
    def submit(self, *_args, **_kwargs):
        return _FailedFuture()


class _ResultFuture:
    def __init__(self, result):
        self._result = result

    def result(self):
        return self._result


class _ResultExecutor:
    def __init__(self, outcomes):
        self._outcomes = outcomes

    def submit(self, _function, _pool, name, **_kwargs):
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
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
    ):
        service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 101
    assert job["failed"] == 101
    assert len(job["errors"]) == 100


def test_cpa_start_import_worker_state_write_failure_is_terminal(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    original_set_import_job = config.set_import_job
    calls = 0

    def fail_first_worker_state_write(pool_id: str, import_job: dict, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("state write failed")
        return original_set_import_job(pool_id, import_job)

    with mock.patch.object(config, "set_import_job", side_effect=fail_first_worker_state_write):
        service.start_import(pool, ["file-1.json"])
        task_executor_module.wait_for_background_tasks()

    job = config.get_import_job(pool["id"])
    assert calls == 2
    assert job is not None
    assert job["status"] == "failed"
    assert job["completed"] == 1
    assert job["failed"] == 1


def test_cpa_batch_submit_failure_cancels_already_submitted_futures(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = ["file-1.json", "file-2.json", "file-3.json"]
    config.set_import_job(pool["id"], _job(status="pending", total=len(names), completed=0))

    class Future:
        def __init__(self) -> None:
            self.cancel_calls = 0

        def cancel(self) -> bool:
            self.cancel_calls += 1
            return True

    first = Future()
    second = Future()

    class Executor:
        def __init__(self) -> None:
            self.calls = 0

        def submit(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return first
            if self.calls == 2:
                return second
            raise RuntimeError("submit failed")

    with mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", Executor()):
            service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == len(names)
    assert job["failed"] == len(names)
    assert first.cancel_calls == 1
    assert second.cancel_calls == 1


def test_cpa_batch_cancellation_cancels_submitted_futures(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    config.set_import_job(pool["id"], _job(status="pending", total=2, completed=0))

    class Future:
        def __init__(self) -> None:
            self.cancel_calls = 0

        def cancel(self) -> bool:
            self.cancel_calls += 1
            return True

    first = Future()
    second = Future()

    class Executor:
        def __init__(self) -> None:
            self.calls = 0

        def submit(self, *_args, **_kwargs):
            self.calls += 1
            return first if self.calls == 1 else second

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", Executor()),
        mock.patch.object(cpa_module, "as_completed", side_effect=KeyboardInterrupt),
    ):
        with pytest.raises(KeyboardInterrupt):
            service._run_import(
                pool["id"],
                _cpa_pool_with_persisted_job(config, pool),
                ["file-1.json", "file-2.json"],
            )

    assert first.cancel_calls == 1
    assert second.cancel_calls == 1


def test_ccload_batch_submit_failure_cancels_already_submitted_futures() -> None:
    class Future:
        def __init__(self) -> None:
            self.cancel_calls = 0

        def cancel(self) -> bool:
            self.cancel_calls += 1
            return True

    first = Future()
    second = Future()

    class Executor:
        def __init__(self) -> None:
            self.calls = 0

        def submit(self, *_args, **_kwargs):
            self.calls += 1
            if self.calls == 1:
                return first
            if self.calls == 2:
                return second
            raise RuntimeError("submit failed")

    admin_session = mock.MagicMock()
    admin_session.__enter__.return_value = (mock.Mock(), "https://ccload.example.test", {})
    with (
        mock.patch.object(ccload_module, "_CCLOAD_FETCH_EXECUTOR", Executor()),
        mock.patch.object(ccload_module, "_admin_session", return_value=admin_session),
    ):
        with pytest.raises(RuntimeError, match="submit failed"):
            ccload_module.fetch_remote_credentials({"base_url": "https://ccload.example.test"}, ["1", "2", "3"])

    assert first.cancel_calls == 1
    assert second.cancel_calls == 1


def test_cpa_import_timeout_terminates_job_instead_of_leaving_running(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    config.set_import_job(pool["id"], _job(status="pending", total=1, completed=0))

    class PendingFuture:
        def cancel(self):
            return True

    class Executor:
        def submit(self, *_args, **_kwargs):
            return PendingFuture()

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", Executor()),
        mock.patch.object(cpa_module, "as_completed", side_effect=TimeoutError()),
        mock.patch.object(cpa_module.time, "monotonic", return_value=1000.0),
    ):
            service._run_import(
                pool["id"],
                _cpa_pool_with_persisted_job(config, pool),
                ["file-1.json"],
            )

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 1
    assert job["failed"] == 1
    assert job["errors"] == [{"name": "item-1", "error": "import failed"}]


def test_cpa_import_shared_deadline_covers_account_refresh_after_files_are_fetched(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    config.set_import_job(pool["id"], _job(status="pending", total=1, completed=0))
    refresh_started = Event()
    release_refresh = Event()
    clock = [1000.0]

    def blocked_refresh(_tokens, *, on_progress=None, deadline=None):
        refresh_started.set()
        assert deadline == 1000.0 + cpa_module.CPA_IMPORT_TIMEOUT_SECS
        while deadline is None or clock[0] < deadline:
            release_refresh.wait(timeout=0.01)
        raise TimeoutError("refresh timed out")

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor({"file-1.json": ("token-1", None)})),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
        mock.patch.object(cpa_module.time, "monotonic", side_effect=lambda: clock[0]),
        mock.patch.object(cpa_module.account_service, "add_accounts", return_value={"added": 1, "skipped": 0}),
        mock.patch.object(cpa_module.account_service, "refresh_accounts", side_effect=blocked_refresh),
        ThreadPoolExecutor(max_workers=1) as executor,
    ):
        request = executor.submit(
            service._run_import,
            pool["id"],
            _cpa_pool_with_persisted_job(config, pool),
            ["file-1.json"],
        )
        assert refresh_started.wait(timeout=1)
        clock[0] += cpa_module.CPA_IMPORT_TIMEOUT_SECS + 1
        try:
            request.result(timeout=1)
        finally:
            release_refresh.set()

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 1
    assert job["failed"] == 1


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
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
        mock.patch.object(cpa_module.account_service, "add_accounts", return_value=add_result) as add_accounts,
        mock.patch.object(cpa_module.account_service, "refresh_accounts") as refresh_accounts,
    ):
        service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

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
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
        mock.patch.object(
            cpa_module.account_service,
            "add_accounts",
            side_effect=RuntimeError("account import failed"),
        ) as add_accounts,
        mock.patch.object(cpa_module.account_service, "refresh_accounts") as refresh_accounts,
    ):
        service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert job["added"] == 0
    assert job["refreshed"] == 0
    assert len(job["errors"]) <= 100
    add_accounts.assert_called_once()
    refresh_accounts.assert_not_called()


def test_cpa_impossible_add_counts_terminate_job_without_refresh(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = list(_cpa_outcomes())
    config.set_import_job(pool["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor(_cpa_outcomes())),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
        mock.patch.object(
            cpa_module.account_service,
            "add_accounts",
            return_value={"added": 3, "skipped": 0},
        ),
        mock.patch.object(
            cpa_module.account_service,
            "refresh_accounts",
            return_value={"refreshed": 2, "errors": []},
        ) as refresh_accounts,
    ):
        service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert job["added"] == 0
    assert job["skipped"] == 0
    refresh_accounts.assert_not_called()


def test_cpa_refresh_exception_preserves_known_counts(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = list(_cpa_outcomes())
    config.set_import_job(pool["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor(_cpa_outcomes())),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
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
        service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 1
    assert job["added"] == 2
    assert job["skipped"] == 0
    assert job["refreshed"] == 0
    add_accounts.assert_called_once()
    refresh_accounts.assert_called_once()


def test_cpa_refresh_returned_errors_finalize_job_as_failed(tmp_path) -> None:
    config = CPAConfig(tmp_path / "cpa.json")
    pool = config.add_pool("CPA", "https://cpa.example.test", "secret")
    service = cpa_module.CPAImportService(config)
    names = list(_cpa_outcomes())
    config.set_import_job(pool["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", _ResultExecutor(_cpa_outcomes())),
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
        mock.patch.object(cpa_module.account_service, "add_accounts", return_value={"added": 2, "skipped": 0}),
        mock.patch.object(
            cpa_module.account_service,
            "refresh_accounts",
            return_value={"refreshed": 1, "errors": [{"token": "token:opaque", "error": "refresh failed"}]},
        ),
    ):
        service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

    job = config.get_import_job(pool["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 2
    assert job["refreshed"] == 1
    assert len(job["errors"]) == 2


@pytest.mark.parametrize(
    "refresh_result",
    (
        {"refreshed": True},
        {"refreshed": 1.5},
        {"refreshed": -1},
        {"refreshed": "1"},
        {"refreshed": 3},
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
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
        mock.patch.object(
            cpa_module.account_service,
            "add_accounts",
            return_value={"added": 2, "skipped": 0},
        ) as add_accounts,
        mock.patch.object(cpa_module.account_service, "refresh_accounts", return_value=refresh_result) as refresh_accounts,
    ):
        service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

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
        mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
        mock.patch.object(cpa_module.account_service, "add_accounts", return_value={"added": 2, "skipped": 0}),
        mock.patch.object(cpa_module.account_service, "refresh_accounts", return_value={"refreshed": 2}),
    ):
        service._run_import(pool["id"], _cpa_pool_with_persisted_job(config, pool), names)

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


def test_sub2api_start_import_worker_state_write_failure_is_terminal(tmp_path) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    original_set_import_job = config.set_import_job
    calls = 0

    def fail_first_worker_state_write(server_id: str, import_job: dict, **_kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("state write failed")
        return original_set_import_job(server_id, import_job)

    with mock.patch.object(config, "set_import_job", side_effect=fail_first_worker_state_write):
        service.start_import(server, ["account-1"])
        task_executor_module.wait_for_background_tasks()

    job = config.get_import_job(server["id"])
    assert calls == 2
    assert job is not None
    assert job["status"] == "failed"
    assert job["completed"] == 1
    assert job["failed"] == 1


def test_sub2api_import_deadline_covers_account_refresh_after_fetch(tmp_path) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    config.set_import_job(server["id"], _job(status="pending", total=1, completed=0))
    started = Event()
    release = Event()
    clock = [1000.0]
    timeout_secs = getattr(sub2api_module, "SUB2API_IMPORT_TIMEOUT_SECS", 30 * 60.0)

    def blocked_refresh(_tokens, *, deadline=None):
        started.set()
        if deadline is None:
            release.wait()
        else:
            while clock[0] < deadline and not release.is_set():
                release.wait(0.01)
        raise TimeoutError("account refresh timed out")

    with (
        mock.patch.object(
            sub2api_module,
            "_fetch_access_tokens_for_accounts",
            return_value=(["token-1"], []),
        ),
        mock.patch.object(
            sub2api_module.account_service,
            "add_accounts",
            return_value={"added": 1, "skipped": 0},
        ),
        mock.patch.object(
            sub2api_module.account_service,
            "refresh_accounts",
            side_effect=blocked_refresh,
        ),
        mock.patch.object(sub2api_module.time, "monotonic", return_value=clock[0]),
        ThreadPoolExecutor(max_workers=1) as executor,
    ):
        request = executor.submit(service._run_import, server["id"], server, ["account-1"])
        assert started.wait(timeout=1)
        clock[0] += timeout_secs + 1
        try:
            request.result(timeout=1)
        finally:
            release.set()

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 1
    assert job["failed"] == 1


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


def test_sub2api_impossible_add_counts_terminate_job_without_refresh(tmp_path) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    account_ids = ["account-1", "account-2", "account-3"]
    config.set_import_job(server["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(
            sub2api_module,
            "_fetch_access_tokens_for_accounts",
            return_value=(
                ["token-1", "token-2"],
                [{"name": "account-3", "error": "credential unavailable"}],
            ),
        ),
        mock.patch.object(
            sub2api_module.account_service,
            "add_accounts",
            return_value={"added": 3, "skipped": 0},
        ),
        mock.patch.object(
            sub2api_module.account_service,
            "refresh_accounts",
            return_value={"refreshed": 2, "errors": []},
        ) as refresh_accounts,
    ):
        service._run_import(server["id"], server, account_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert job["added"] == 0
    assert job["skipped"] == 0
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


def test_sub2api_refresh_returned_errors_finalize_job_as_failed(tmp_path) -> None:
    config, server, service = _sub2api_server_and_service(tmp_path)
    account_ids = ["account-1", "account-2", "account-3"]
    config.set_import_job(server["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(
            sub2api_module,
            "_fetch_access_tokens_for_accounts",
            return_value=(
                ["token-1", "token-2"],
                [{"name": "account-3", "error": "credential unavailable"}],
            ),
        ),
        mock.patch.object(sub2api_module.account_service, "add_accounts", return_value={"added": 2, "skipped": 0}),
        mock.patch.object(
            sub2api_module.account_service,
            "refresh_accounts",
            return_value={"refreshed": 1, "errors": [{"token": "token:opaque", "error": "refresh failed"}]},
        ),
    ):
        service._run_import(server["id"], server, account_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 2
    assert job["refreshed"] == 1
    assert len(job["errors"]) == 2


@pytest.mark.parametrize(
    "refresh_result",
    (
        {"refreshed": True},
        {"refreshed": 1.5},
        {"refreshed": -1},
        {"refreshed": "1"},
        {"refreshed": 3},
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


def test_ccload_impossible_add_counts_terminate_job_without_refresh(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    channel_ids = ["1", "2", "3"]
    config.set_import_job(server["id"], _job(status="pending", total=3, completed=0))

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=(
                [{"access_token": "access-token-1"}, {"access_token": "access-token-2"}],
                [{"name": "3", "error": "credential unavailable"}],
            ),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            return_value={"added": 3, "skipped": 0},
        ),
        mock.patch.object(
            ccload_module.account_service,
            "refresh_accounts",
            return_value={"refreshed": 2, "errors": []},
        ) as refresh_accounts,
    ):
        service._run_import(server["id"], server, channel_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 3
    assert job["failed"] == 3
    assert job["added"] == 0
    assert job["skipped"] == 0
    refresh_accounts.assert_not_called()


def test_ccload_import_deadline_covers_account_refresh_after_fetch(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    config.set_import_job(server["id"], _job(status="pending", total=1, completed=0))
    started = Event()
    release = Event()
    clock = [1000.0]

    def blocked_refresh(_tokens, *, on_progress=None, deadline=None):
        started.set()
        if deadline is None:
            release.wait()
        else:
            while clock[0] < deadline and not release.is_set():
                release.wait(0.01)
        raise TimeoutError("account refresh timed out")

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=([{"access_token": "access-token"}], []),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            return_value={"added": 1, "skipped": 0},
        ),
        mock.patch.object(
            ccload_module.account_service,
            "refresh_accounts",
            side_effect=blocked_refresh,
        ),
        mock.patch.object(ccload_module.time, "monotonic", return_value=clock[0]),
        ThreadPoolExecutor(max_workers=1) as executor,
    ):
        request = executor.submit(service._run_import, server["id"], server, ["1"])
        if not started.wait(timeout=1):
            release.set()
            request.result(timeout=1)
            raise AssertionError("refresh did not start")
        clock[0] += ccload_module.CCLOAD_IMPORT_TIMEOUT_SECS + 1
        try:
            request.result(timeout=1)
        finally:
            release.set()

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 1
    assert job["failed"] == 1


def test_ccload_late_refresh_progress_cannot_update_failed_job(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    config.set_import_job(server["id"], _job(status="pending", total=1, completed=0))
    started = Event()
    late_callback = Event()
    clock = [1000.0]

    def blocked_refresh(_tokens, *, on_progress=None, deadline=None):
        started.set()
        while deadline is None or clock[0] < deadline:
            time.sleep(0.01)
        on_progress(1)
        late_callback.set()
        raise TimeoutError("account refresh timed out")

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=([{"access_token": "access-token"}], []),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            return_value={"added": 1, "skipped": 0},
        ),
        mock.patch.object(
            ccload_module.account_service,
            "refresh_accounts",
            side_effect=blocked_refresh,
        ),
        mock.patch.object(ccload_module.time, "monotonic", side_effect=lambda: clock[0]),
        ThreadPoolExecutor(max_workers=1) as executor,
    ):
        request = executor.submit(service._run_import, server["id"], server, ["1"])
        assert started.wait(timeout=1)
        clock[0] += ccload_module.CCLOAD_IMPORT_TIMEOUT_SECS + 1
        request.result(timeout=1)
        failed_job = config.get_import_job(server["id"])
        assert failed_job["status"] == "failed"
        assert failed_job["refreshed"] == 0
        assert late_callback.wait(timeout=1)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["refreshed"] == 0


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


def test_ccload_refresh_exception_counts_missing_results_without_error_detail(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    channel_ids = [str(index) for index in range(102)]
    credentials = [{"access_token": f"access-token-{index}"} for index in range(101)]
    config.set_import_job(server["id"], _job(status="pending", total=102, completed=0))

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=(credentials, []),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            return_value={"added": 101, "skipped": 0},
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
    assert job["failed"] == 1


def test_ccload_refresh_returned_errors_finalize_job_as_failed(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    credentials = [
        {"access_token": "access-token-1"},
        {"access_token": "access-token-2"},
    ]
    channel_ids = ["1", "2"]
    config.set_import_job(server["id"], _job(status="pending", total=2, completed=0))

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=(credentials, []),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            return_value={"added": 2, "skipped": 0},
        ),
        mock.patch.object(
            ccload_module.account_service,
            "refresh_accounts",
            return_value={
                "refreshed": 1,
                "errors": [{"token": "token:opaque", "error": "refresh failed"}],
            },
        ),
    ):
        service._run_import(server["id"], server, channel_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 2
    assert job["failed"] == 1
    assert job["refreshed"] == 1
    assert job["errors"] == [{"name": "item-1", "error": "import failed"}]


def test_ccload_refresh_missing_results_fail_without_bounded_error_details(tmp_path) -> None:
    config = CCLoadConfig(tmp_path / "ccload.json")
    server = config.add_server(
        name="ccLoad",
        base_url="https://ccload.example.test",
        password="password",
    )
    service = ccload_module.CCLoadImportService(config)
    credentials = [{"access_token": f"access-token-{index}"} for index in range(102)]
    channel_ids = [str(index) for index in range(102)]
    config.set_import_job(server["id"], _job(status="pending", total=102, completed=0))

    with (
        mock.patch.object(
            ccload_module,
            "fetch_remote_credentials",
            return_value=(credentials, []),
        ),
        mock.patch.object(
            ccload_module.account_service,
            "add_account_items",
            return_value={"added": 102, "skipped": 0},
        ),
        mock.patch.object(
            ccload_module.account_service,
            "refresh_accounts",
            return_value={"refreshed": 1, "errors": []},
        ),
    ):
        service._run_import(server["id"], server, channel_ids)

    job = config.get_import_job(server["id"])
    assert job["status"] == "failed"
    assert job["completed"] == 102
    assert job["refreshed"] == 1
    assert job["failed"] == 101
    assert len(job["errors"]) <= 100
