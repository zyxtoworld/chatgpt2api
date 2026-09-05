from __future__ import annotations

import unittest
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import services.cpa_service as cpa_module
import services.ccload_service as ccload_module
import services.sub2api_service as sub2api_module


class DeferredReservation:
    instances: list["DeferredReservation"] = []

    def __init__(self) -> None:
        type(self).instances.append(self)

    def submit(self, target, *args, **kwargs) -> None:
        self.target = target
        self.args = args
        self.kwargs = kwargs

    def cancel(self) -> None:
        pass


class ImportJobConcurrencyContractTests(unittest.TestCase):
    def setUp(self) -> None:
        DeferredReservation.instances.clear()

    def test_sub2api_second_import_cannot_replace_active_job(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = sub2api_module.Sub2APIConfig(Path(temp_dir) / "sub2api.json")
            server = config.add_server(
                name="sub2api",
                base_url="https://sub2api.example.test",
                email="admin@example.test",
                password="password-secret",
                api_key="",
            )
            service = sub2api_module.Sub2APIImportService(config)

            with mock.patch.object(
                sub2api_module,
                "reserve_background_task",
                side_effect=DeferredReservation,
            ):
                first = service.start_import(server, ["account-1"])
                with self.assertRaises(sub2api_module.PublicSafeValueError):
                    service.start_import(server, ["account-2"])

            current = config.get_import_job(server["id"])
            self.assertEqual(current["job_id"], first["job_id"])
            self.assertEqual(current["status"], "pending")
            self.assertEqual(len(DeferredReservation.instances), 2)

    def test_cpa_second_import_cannot_replace_active_job(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = cpa_module.CPAConfig(Path(temp_dir) / "cpa.json")
            pool = config.add_pool(
                "CPA",
                "https://cpa.example.test",
                "management-secret",
            )
            service = cpa_module.CPAImportService(config)

            with mock.patch.object(
                cpa_module,
                "reserve_background_task",
                side_effect=DeferredReservation,
            ):
                first = service.start_import(pool, ["one.json"])
                with self.assertRaises(cpa_module.PublicSafeValueError):
                    service.start_import(pool, ["two.json"])

            current = config.get_import_job(pool["id"])
            self.assertEqual(current["job_id"], first["job_id"])
            self.assertEqual(current["status"], "pending")
            self.assertEqual(len(DeferredReservation.instances), 2)

    def test_import_worker_uses_connection_snapshot_at_begin_commit(self) -> None:
        cases = (
            (cpa_module.CPAConfig, cpa_module.CPAImportService, "cpa.json", "cpa"),
            (ccload_module.CCLoadConfig, ccload_module.CCLoadImportService, "ccload.json", "ccload"),
            (sub2api_module.Sub2APIConfig, sub2api_module.Sub2APIImportService, "sub2api.json", "sub2api"),
        )

        for config_type, service_type, filename, kind in cases:
            with self.subTest(kind=kind), TemporaryDirectory() as temp_dir:
                config = config_type(Path(temp_dir) / filename)
                if kind == "cpa":
                    connection = config.add_pool("CPA", "https://old.example.test", "management-secret")
                    update = config.update_pool
                    item_names = ["one.json"]
                    module = cpa_module
                elif kind == "ccload":
                    connection = config.add_server(
                        name="ccLoad",
                        base_url="https://old.example.test",
                        password="password-secret",
                    )
                    update = config.update_server
                    item_names = ["1"]
                    module = ccload_module
                else:
                    connection = config.add_server(
                        name="Sub2API",
                        base_url="https://old.example.test",
                        email="admin@example.test",
                        password="password-secret",
                        api_key="",
                    )
                    update = config.update_server
                    item_names = ["account-1"]
                    module = sub2api_module

                original_begin = config.begin_import_job
                new_url = "https://new.example.test"

                def begin_after_concurrent_update(connection_id, job):
                    update(connection_id, {"base_url": new_url})
                    return original_begin(connection_id, job)

                service = service_type(config)
                with (
                    mock.patch.object(config, "begin_import_job", side_effect=begin_after_concurrent_update),
                    mock.patch.object(module, "reserve_background_task", side_effect=DeferredReservation),
                ):
                    service.start_import(connection, item_names)

                reservation = DeferredReservation.instances[-1]
                self.assertEqual(reservation.args[1]["base_url"], new_url)

    def test_cpa_reloaded_job_rejects_late_worker_before_account_import(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cpa.json"
            config = cpa_module.CPAConfig(path)
            pool = config.add_pool(
                "CPA",
                "https://cpa.example.test",
                "management-secret",
            )
            service = cpa_module.CPAImportService(config)
            first_job = {
                "job_id": "job-a",
                "status": "pending",
                "created_at": "2026-08-11T00:00:00+00:00",
                "updated_at": "2026-08-11T00:00:01+00:00",
                "total": 1,
                "completed": 0,
                "added": 0,
                "skipped": 0,
                "refreshed": 0,
                "failed": 0,
                "errors": [],
            }
            config.set_import_job(pool["id"], first_job)
            pool = config.get_pool(pool["id"])
            self.assertIsNotNone(pool)
            result_started = threading.Event()
            release_result = threading.Event()

            class Future:
                def result(self):
                    result_started.set()
                    if not release_result.wait(timeout=3):
                        raise AssertionError("worker was not released")
                    return "token-a", None

                def cancel(self):
                    return False

            class Executor:
                def submit(self, *_args, **_kwargs):
                    return Future()

            with (
                mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", Executor()),
                mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
                mock.patch.object(
                    cpa_module.account_service,
                    "add_accounts",
                    return_value={"added": 1, "skipped": 0},
                ) as add_accounts,
                mock.patch.object(
                    cpa_module.account_service,
                    "refresh_accounts",
                    return_value={"refreshed": 1},
                ),
                ThreadPoolExecutor(max_workers=1) as executor,
            ):
                request = executor.submit(service._run_import, pool["id"], pool, ["file-1.json"])
                self.assertTrue(result_started.wait(timeout=1))

                reloaded = cpa_module.CPAConfig(path)
                second_job = dict(first_job)
                second_job.update({
                    "job_id": "job-b",
                    "status": "pending",
                    "updated_at": "2026-08-11T00:00:02+00:00",
                })
                self.assertIsNotNone(reloaded.begin_import_job(pool["id"], second_job))

                release_result.set()
                request.result(timeout=3)

            current = config.get_import_job(pool["id"])
            self.assertEqual(current["job_id"], "job-b")
            self.assertEqual(current["status"], "pending")
            add_accounts.assert_not_called()

    def test_cpa_late_worker_cannot_write_after_job_replacement_between_check_and_add(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cpa.json"
            config = cpa_module.CPAConfig(path)
            pool = config.add_pool("CPA", "https://cpa.example.test", "management-secret")
            replacement = cpa_module.CPAConfig(path)
            service = cpa_module.CPAImportService(config)
            fetch_done = threading.Event()
            checked = threading.Event()
            release_check = threading.Event()
            add_started = threading.Event()
            release_add = threading.Event()
            replacement_started = threading.Event()
            replacement_done = threading.Event()
            replacement_errors: list[BaseException] = []
            post_fetch_reads = 0
            original_get = config.get_import_job

            def gated_get(pool_id: str):
                nonlocal post_fetch_reads
                current = original_get(pool_id)
                if fetch_done.is_set() and isinstance(current, dict) and current.get("status") == "running":
                    post_fetch_reads += 1
                    if post_fetch_reads == 3:
                        checked.set()
                        if not release_check.wait(2):
                            raise AssertionError("worker did not leave the pre-add gate")
                return current

            class Future:
                def result(self):
                    fetch_done.set()
                    return "token-a", None

                def cancel(self):
                    return False

            class Executor:
                def submit(self, *_args, **_kwargs):
                    return Future()

            def add_accounts(_tokens, *, source_type):
                add_started.set()
                if not release_add.wait(2):
                    raise AssertionError("account add was not released")
                return {"added": 1, "skipped": 0}

            with (
                mock.patch.object(cpa_module, "reserve_background_task", side_effect=DeferredReservation),
                mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", Executor()),
                mock.patch.object(cpa_module, "as_completed", side_effect=lambda futures, **_kwargs: iter(futures)),
                mock.patch.object(config, "get_import_job", side_effect=gated_get),
                mock.patch.object(cpa_module.account_service, "add_accounts", side_effect=add_accounts) as add_mock,
                mock.patch.object(
                    cpa_module.account_service,
                    "refresh_accounts",
                    return_value={"refreshed": 1},
                ),
            ):
                started = service.start_import(pool, ["file-1.json"])
                reservation = DeferredReservation.instances[-1]
                worker_errors: list[BaseException] = []

                def run_worker() -> None:
                    try:
                        reservation.target(*reservation.args, **reservation.kwargs)
                    except BaseException as exc:
                        worker_errors.append(exc)

                worker = threading.Thread(
                    target=run_worker,
                )
                worker.start()
                self.assertTrue(checked.wait(2), f"post-fetch get count={post_fetch_reads}")

                def replace_job() -> None:
                    try:
                        replacement_started.set()
                        old_job = replacement.get_import_job(pool["id"])
                        failed_old = dict(old_job, status="failed", completed=1, failed=1)
                        saved = None
                        for _ in range(3):
                            try:
                                saved = replacement.set_import_job(
                                    pool["id"], failed_old, expected_job_id=started["job_id"]
                                )
                            except cpa_module.StorageConflictError:
                                continue
                            break
                        if saved is None:
                            raise AssertionError("replacement did not commit")
                        new_job = dict(failed_old, job_id="job-b", status="pending", completed=0, failed=0)
                        begun = None
                        for _ in range(3):
                            try:
                                begun = replacement.begin_import_job(pool["id"], new_job)
                            except cpa_module.StorageConflictError:
                                continue
                            break
                        if begun is None:
                            raise AssertionError("replacement job did not begin")
                    except BaseException as exc:
                        replacement_errors.append(exc)
                    finally:
                        replacement_done.set()

                replacer = threading.Thread(target=replace_job)
                replacer.start()
                self.assertTrue(replacement_started.wait(1))
                release_check.set()
                self.assertTrue(add_started.wait(2))
                self.assertFalse(replacement_done.is_set())
                release_add.set()
                self.assertTrue(replacement_done.wait(2))
                worker.join(3)
                replacer.join(3)

            self.assertFalse(worker.is_alive())
            self.assertFalse(replacer.is_alive())
            self.assertEqual(worker_errors, [])
            self.assertEqual(replacement_errors, [])
            self.assertGreaterEqual(post_fetch_reads, 3)
            add_mock.assert_called_once()
            current = config.get_import_job(pool["id"])
            self.assertEqual(current["job_id"], "job-b")
            self.assertEqual(current["status"], "pending")

    def test_cpa_late_worker_conflict_is_treated_as_stale_job(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cpa.json"
            config = cpa_module.CPAConfig(path)
            pool = config.add_pool("CPA", "https://cpa.example.test", "management-secret")
            job_a = {
                "job_id": "job-a",
                "status": "running",
                "created_at": "2026-08-11T00:00:00+00:00",
                "updated_at": "2026-08-11T00:00:01+00:00",
                "total": 1,
                "completed": 0,
                "added": 0,
                "skipped": 0,
                "refreshed": 0,
                "failed": 0,
                "errors": [],
            }
            config.set_import_job(pool["id"], job_a)
            replacement = cpa_module.CPAConfig(path)
            failed_job_a = dict(job_a, status="failed", completed=1, failed=1)
            job_b = dict(failed_job_a, job_id="job-b", status="pending", completed=0, failed=0)
            original_set_import_job = config.set_import_job
            conflict_injected = False

            def replace_then_conflict(
                pool_id: str,
                import_job: dict | None,
                *,
                expected_job_id: str | None = None,
            ) -> dict | None:
                nonlocal conflict_injected
                if not conflict_injected:
                    conflict_injected = True
                    replacement.set_import_job(
                        pool_id,
                        failed_job_a,
                        expected_job_id="job-a",
                    )
                    replacement.begin_import_job(pool_id, job_b)
                    raise cpa_module.StorageConflictError()
                return original_set_import_job(
                    pool_id,
                    import_job,
                    expected_job_id=expected_job_id,
                )

            service = cpa_module.CPAImportService(config)
            with mock.patch.object(config, "set_import_job", side_effect=replace_then_conflict):
                result = service._update_job(
                    pool["id"],
                    expected_job_id="job-a",
                    status="completed",
                )

            self.assertIsNone(result)
            current = config.get_import_job(pool["id"])
            self.assertIsNotNone(current)
            self.assertEqual(current["job_id"], "job-b")
            self.assertEqual(current["status"], "pending")

    def test_ccload_late_worker_cannot_write_after_job_replacement_between_check_and_add(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "ccload.json"
            config = ccload_module.CCLoadConfig(path)
            server = config.add_server(
                name="ccLoad",
                base_url="https://ccload.example.test",
                password="password",
            )
            replacement = ccload_module.CCLoadConfig(path)
            service = ccload_module.CCLoadImportService(config)
            checked = threading.Event()
            release_check = threading.Event()
            add_started = threading.Event()
            release_add = threading.Event()
            replacement_started = threading.Event()
            replacement_done = threading.Event()
            replacement_errors: list[BaseException] = []
            original_check = service._job_is_current

            def gated_check(server_id: str, expected_job_id: str) -> bool:
                current = original_check(server_id, expected_job_id)
                if current and not checked.is_set():
                    checked.set()
                    if not release_check.wait(2):
                        raise AssertionError("worker did not leave the pre-add gate")
                return current

            credential = {
                "type": "codex",
                "access_token": "token-a",
                "refresh_token": "refresh-a",
                "expired": "2026-08-17T00:00:00+00:00",
            }

            def add_account_items(_items):
                add_started.set()
                if not release_add.wait(2):
                    raise AssertionError("account add was not released")
                return {"added": 1, "skipped": 0}

            with (
                mock.patch.object(ccload_module, "reserve_background_task", side_effect=DeferredReservation),
                mock.patch.object(
                    ccload_module,
                    "fetch_remote_credentials",
                    return_value=([credential], []),
                ),
                mock.patch.object(service, "_job_is_current", side_effect=gated_check),
                mock.patch.object(
                    ccload_module.account_service,
                    "add_account_items",
                    side_effect=add_account_items,
                ) as add_items,
                mock.patch.object(
                    ccload_module.account_service,
                    "refresh_accounts",
                    return_value={"refreshed": 1},
                ),
            ):
                started = service.start_import(server, ["1"])
                reservation = DeferredReservation.instances[-1]
                worker_errors: list[BaseException] = []

                def run_worker() -> None:
                    try:
                        reservation.target(*reservation.args, **reservation.kwargs)
                    except BaseException as exc:
                        worker_errors.append(exc)

                worker = threading.Thread(
                    target=run_worker,
                )
                worker.start()
                self.assertTrue(checked.wait(2))

                def replace_job() -> None:
                    try:
                        replacement_started.set()
                        old_job = replacement.get_import_job(server["id"])
                        failed_old = dict(old_job, status="failed", completed=1, failed=1)
                        saved = None
                        for _ in range(3):
                            try:
                                saved = replacement.set_import_job(
                                    server["id"], failed_old, expected_job_id=started["job_id"]
                                )
                            except ccload_module.StorageConflictError:
                                continue
                            break
                        if saved is None:
                            raise AssertionError("replacement did not commit")
                        new_job = dict(failed_old, job_id="job-b", status="pending", completed=0, failed=0)
                        begun = None
                        for _ in range(3):
                            try:
                                begun = replacement.begin_import_job(server["id"], new_job)
                            except ccload_module.StorageConflictError:
                                continue
                            break
                        if begun is None:
                            raise AssertionError("replacement job did not begin")
                    except BaseException as exc:
                        replacement_errors.append(exc)
                    finally:
                        replacement_done.set()

                replacer = threading.Thread(target=replace_job)
                replacer.start()
                self.assertTrue(replacement_started.wait(1))
                release_check.set()
                self.assertTrue(add_started.wait(2))
                self.assertFalse(replacement_done.is_set())
                release_add.set()
                self.assertTrue(replacement_done.wait(2))
                worker.join(3)
                replacer.join(3)

            self.assertFalse(worker.is_alive())
            self.assertFalse(replacer.is_alive())
            self.assertEqual(worker_errors, [])
            self.assertEqual(replacement_errors, [])
            add_items.assert_called_once()
            current = config.get_import_job(server["id"])
            self.assertEqual(current["job_id"], "job-b")
            self.assertEqual(current["status"], "pending")

    def test_ccload_job_current_fails_closed_on_secure_snapshot_change(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config = ccload_module.CCLoadConfig(Path(temp_dir) / "ccload.json")
            service = ccload_module.CCLoadImportService(config)

            for error in (
                OSError("file changed during secure open"),
                ccload_module.StorageDataError(),
            ):
                with self.subTest(error=type(error).__name__), mock.patch.object(
                    config,
                    "get_import_job",
                    side_effect=error,
                ):
                    self.assertFalse(service._job_is_current("server-a", "job-a"))

    def test_cpa_cross_instance_cas_rejects_late_job_overwrite(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cpa.json"
            seed = cpa_module.CPAConfig(path)
            pool = seed.add_pool("CPA", "https://cpa.example.test", "management-secret")
            job_a = {
                "job_id": "job-a",
                "status": "failed",
                "created_at": "2026-08-11T00:00:00+00:00",
                "updated_at": "2026-08-11T00:00:01+00:00",
                "total": 1,
                "completed": 1,
                "added": 0,
                "skipped": 0,
                "refreshed": 0,
                "failed": 1,
                "errors": [{"name": "item-1", "error": "import failed"}],
            }
            seed.set_import_job(pool["id"], job_a)
            writer_a = cpa_module.CPAConfig(path)
            writer_b = cpa_module.CPAConfig(path)
            job_a_done = dict(job_a, status="completed", failed=0)
            job_b = dict(job_a, job_id="job-b", status="pending", completed=0, failed=0, errors=[])
            gate = threading.Barrier(2)
            b_written = threading.Event()
            writer_kind: dict[int, str] = {}
            original_atomic_write = cpa_module.atomic_write_bytes

            def gated_atomic_write(target, root, payload, **kwargs):
                kind = writer_kind[threading.get_ident()]
                try:
                    gate.wait(timeout=1)
                except threading.BrokenBarrierError:
                    return original_atomic_write(target, root, payload, **kwargs)
                if kind == "b":
                    result = original_atomic_write(target, root, payload, **kwargs)
                    b_written.set()
                    return result
                if not b_written.wait(timeout=3):
                    raise AssertionError("writer B did not publish first")
                return original_atomic_write(target, root, payload, **kwargs)

            def finish_a():
                writer_kind[threading.get_ident()] = "a"
                return writer_a.set_import_job(
                    pool["id"],
                    job_a_done,
                    expected_job_id="job-a",
                )

            def begin_b():
                writer_kind[threading.get_ident()] = "b"
                return writer_b.begin_import_job(pool["id"], job_b)

            with (
                mock.patch.object(cpa_module, "atomic_write_bytes", side_effect=gated_atomic_write),
                ThreadPoolExecutor(max_workers=2) as executor,
            ):
                future_a = executor.submit(finish_a)
                future_b = executor.submit(begin_b)
                outcomes = {}
                for kind, future in (("a", future_a), ("b", future_b)):
                    try:
                        future.result(timeout=5)
                    except cpa_module.StorageConflictError:
                        outcomes[kind] = "conflict"
                    else:
                        outcomes[kind] = "committed"

            current = writer_a.get_import_job(pool["id"])
            self.assertIsNotNone(current)
            self.assertEqual(list(outcomes.values()).count("committed"), 1)
            self.assertEqual(list(outcomes.values()).count("conflict"), 1)
            committed_job_id = "job-a" if outcomes["a"] == "committed" else "job-b"
            self.assertEqual(current["job_id"], committed_job_id)

    def test_cpa_cross_instance_cas_reports_unrelated_pool_conflict(self) -> None:
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "cpa.json"
            seed = cpa_module.CPAConfig(path)
            first = seed.add_pool("first", "https://first.example.test", "secret")
            second = seed.add_pool("second", "https://second.example.test", "secret")
            writer_a = cpa_module.CPAConfig(path)
            writer_b = cpa_module.CPAConfig(path)
            gate = threading.Barrier(2)
            b_written = threading.Event()
            writer_kind: dict[int, str] = {}
            original_atomic_write = cpa_module.atomic_write_bytes

            def gated_atomic_write(target, root, payload, **kwargs):
                kind = writer_kind[threading.get_ident()]
                try:
                    gate.wait(timeout=1)
                except threading.BrokenBarrierError:
                    return original_atomic_write(target, root, payload, **kwargs)
                if kind == "b":
                    result = original_atomic_write(target, root, payload, **kwargs)
                    b_written.set()
                    return result
                if not b_written.wait(timeout=3):
                    raise AssertionError("writer B did not publish first")
                return original_atomic_write(target, root, payload, **kwargs)

            def update_a():
                writer_kind[threading.get_ident()] = "a"
                return writer_a.update_pool(first["id"], {"name": "first-updated"})

            def update_b():
                writer_kind[threading.get_ident()] = "b"
                return writer_b.update_pool(second["id"], {"name": "second-updated"})

            with (
                mock.patch.object(cpa_module, "atomic_write_bytes", side_effect=gated_atomic_write),
                ThreadPoolExecutor(max_workers=2) as executor,
            ):
                future_a = executor.submit(update_a)
                future_b = executor.submit(update_b)
                outcomes = {}
                for kind, future in (("a", future_a), ("b", future_b)):
                    try:
                        future.result(timeout=5)
                    except cpa_module.StorageConflictError:
                        outcomes[kind] = "conflict"
                    else:
                        outcomes[kind] = "committed"

            self.assertEqual(list(outcomes.values()).count("committed"), 1)
            self.assertEqual(list(outcomes.values()).count("conflict"), 1)
            persisted = writer_a.list_pools()
            names = {item["id"]: item["name"] for item in persisted}
            if outcomes["a"] == "committed":
                self.assertEqual(names[first["id"]], "first-updated")
                self.assertEqual(names[second["id"]], "second")
            else:
                self.assertEqual(names[first["id"]], "first")
                self.assertEqual(names[second["id"]], "second-updated")

    def test_sub2api_and_ccload_cross_instance_cas_reject_late_job_overwrite(self) -> None:
        cases = (
            ("sub2api", sub2api_module.Sub2APIConfig, sub2api_module),
            ("ccload", ccload_module.CCLoadConfig, ccload_module),
        )
        for kind, config_type, module in cases:
            with self.subTest(kind=kind), TemporaryDirectory() as temp_dir:
                path = Path(temp_dir) / f"{kind}.json"
                seed = config_type(path)
                if kind == "sub2api":
                    server = seed.add_server(
                        name="Sub2API",
                        base_url="https://sub2api.example.test",
                        email="admin@example.test",
                        password="password",
                        api_key="",
                    )
                else:
                    server = seed.add_server(
                        name="ccLoad",
                        base_url="https://ccload.example.test",
                        password="password",
                    )
                job_a = {
                    "job_id": "job-a",
                    "status": "failed",
                    "created_at": "2026-08-11T00:00:00+00:00",
                    "updated_at": "2026-08-11T00:00:01+00:00",
                    "total": 1,
                    "completed": 1,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 1,
                    "errors": [{"name": "item-1", "error": "import failed"}],
                }
                seed.set_import_job(server["id"], job_a)
                writer_a = config_type(path)
                writer_b = config_type(path)
                job_b = dict(job_a, job_id="job-b", status="pending", completed=0, failed=0, errors=[])
                gate = threading.Barrier(2)
                b_written = threading.Event()
                writer_kind: dict[int, str] = {}
                original_atomic_write = module.atomic_write_bytes

                def gated_atomic_write(target, root, payload, **kwargs):
                    kind_for_thread = writer_kind[threading.get_ident()]
                    try:
                        gate.wait(timeout=1)
                    except threading.BrokenBarrierError:
                        return original_atomic_write(target, root, payload, **kwargs)
                    if kind_for_thread == "b":
                        result = original_atomic_write(target, root, payload, **kwargs)
                        b_written.set()
                        return result
                    if not b_written.wait(timeout=3):
                        raise AssertionError("writer B did not publish first")
                    return original_atomic_write(target, root, payload, **kwargs)

                def finish_a():
                    writer_kind[threading.get_ident()] = "a"
                    return writer_a.set_import_job(server["id"], dict(job_a, status="completed", failed=0))

                def begin_b():
                    writer_kind[threading.get_ident()] = "b"
                    return writer_b.begin_import_job(server["id"], job_b)

                with (
                    mock.patch.object(module, "atomic_write_bytes", side_effect=gated_atomic_write),
                    ThreadPoolExecutor(max_workers=2) as executor,
                ):
                    future_a = executor.submit(finish_a)
                    future_b = executor.submit(begin_b)
                    outcomes = {}
                    for writer, future in (("a", future_a), ("b", future_b)):
                        try:
                            future.result(timeout=5)
                        except module.StorageConflictError:
                            outcomes[writer] = "conflict"
                        else:
                            outcomes[writer] = "committed"

                self.assertEqual(list(outcomes.values()).count("committed"), 1)
                self.assertEqual(list(outcomes.values()).count("conflict"), 1)
                current = writer_a.get_import_job(server["id"])
                self.assertIsNotNone(current)
                self.assertEqual(current["job_id"], "job-a" if outcomes["a"] == "committed" else "job-b")

    def test_import_workers_and_pending_queue_are_globally_bounded(self) -> None:
        condition = threading.Condition()
        release = threading.Event()
        all_finished = threading.Event()
        active = 0
        max_active = 0
        finished = 0
        accepted = 32

        def block_import(*_args, **_kwargs) -> None:
            nonlocal active, max_active, finished
            with condition:
                active += 1
                max_active = max(max_active, active)
                condition.notify_all()
            try:
                self.assertTrue(release.wait(timeout=10), "import worker release timed out")
            finally:
                with condition:
                    active -= 1
                    finished += 1
                    if finished == accepted:
                        all_finished.set()
                    condition.notify_all()

        with TemporaryDirectory() as temp_dir:
            config = cpa_module.CPAConfig(Path(temp_dir) / "bounded-cpa.json")
            pools = [
                config.add_pool(
                    f"CPA {index}",
                    f"https://cpa-{index}.example.test",
                    "management-secret",
                )
                for index in range(accepted + 1)
            ]
            service = cpa_module.CPAImportService(config)
            try:
                with mock.patch.object(service, "_run_import", side_effect=block_import):
                    for index in range(accepted):
                        service.start_import(pools[index], [f"{index}.json"])

                    with self.assertRaises(RuntimeError):
                        service.start_import(pools[-1], ["overflow.json"])

                    with condition:
                        self.assertTrue(condition.wait_for(lambda: active >= 16, timeout=3))
                        self.assertLessEqual(max_active, 16)
                    self.assertIsNone(config.get_import_job(pools[-1]["id"]))
            finally:
                release.set()
                self.assertTrue(all_finished.wait(timeout=5), "import workers did not finish")

    def test_cpa_import_reuses_one_process_executor_with_worker_sized_batches(self) -> None:
        observations: list[int] = []

        class FakeFuture:
            def __init__(self, token: str) -> None:
                self.token = token

            def result(self):
                return self.token, None

        class RecordingExecutor:
            def __init__(self, *, max_workers: int) -> None:
                self.max_workers = max_workers
                self.submitted: list[FakeFuture] = []

            def submit(self, _function, _pool, name: str, **_kwargs):
                future = FakeFuture(f"token-{name}")
                self.submitted.append(future)
                return future

        def recording_as_completed(futures, **_kwargs):
            observations.append(len(futures))
            return iter(futures)

        executor = RecordingExecutor(max_workers=16)
        with TemporaryDirectory() as temp_dir:
            config = cpa_module.CPAConfig(Path(temp_dir) / "batched-cpa.json")
            pool = config.add_pool("CPA", "https://cpa.example.test", "management-secret")
            config.set_import_job(
                pool["id"],
                {
                    "job_id": "batched",
                    "status": "pending",
                    "created_at": "2026-08-11T00:00:00+00:00",
                    "updated_at": "2026-08-11T00:00:01+00:00",
                    "total": 33,
                    "completed": 0,
                    "added": 0,
                    "skipped": 0,
                    "refreshed": 0,
                    "failed": 0,
                    "errors": [],
                },
            )
            service = cpa_module.CPAImportService(config)
            with (
                mock.patch.object(cpa_module, "_CPA_FETCH_EXECUTOR", executor, create=True),
                mock.patch.object(
                    cpa_module,
                    "ThreadPoolExecutor",
                    side_effect=AssertionError("CPA import must not create a per-job executor"),
                ),
                mock.patch.object(cpa_module, "as_completed", side_effect=recording_as_completed),
                mock.patch.object(
                    cpa_module.account_service,
                    "add_accounts",
                    return_value={"added": 33, "skipped": 0},
                ),
                mock.patch.object(
                    cpa_module.account_service,
                    "refresh_accounts",
                    return_value={"refreshed": 33},
                ),
            ):
                service._run_import(
                    pool["id"],
                    config.get_pool(pool["id"]),
                    [f"file-{index}" for index in range(33)],
                )
                config.set_import_job(
                    pool["id"],
                    {
                        "job_id": "batched-second",
                        "status": "pending",
                        "created_at": "2026-08-11T00:00:00+00:00",
                        "updated_at": "2026-08-11T00:00:01+00:00",
                        "total": 33,
                        "completed": 0,
                        "added": 0,
                        "skipped": 0,
                        "refreshed": 0,
                        "failed": 0,
                        "errors": [],
                    },
                )
                service._run_import(
                    pool["id"],
                    config.get_pool(pool["id"]),
                    [f"second-{index}" for index in range(33)],
                )

        self.assertGreater(len(observations), 1)
        self.assertLessEqual(max(observations), 16)
        self.assertEqual(len(executor.submitted), 66)


if __name__ == "__main__":
    unittest.main()
