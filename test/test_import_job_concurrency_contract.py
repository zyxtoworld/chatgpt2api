from __future__ import annotations

import unittest
import threading
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock

import services.cpa_service as cpa_module
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
                if not release.wait(timeout=5):
                    raise AssertionError("import worker was not released")
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

            def submit(self, _function, _pool, name: str):
                future = FakeFuture(f"token-{name}")
                self.submitted.append(future)
                return future

        def recording_as_completed(futures):
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
                service._run_import(pool["id"], pool, [f"file-{index}" for index in range(33)])
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
                service._run_import(pool["id"], pool, [f"second-{index}" for index in range(33)])

        self.assertGreater(len(observations), 1)
        self.assertLessEqual(max(observations), 16)
        self.assertEqual(len(executor.submitted), 66)


if __name__ == "__main__":
    unittest.main()
