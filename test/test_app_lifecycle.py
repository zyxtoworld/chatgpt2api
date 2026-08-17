from __future__ import annotations

import unittest
import asyncio
import subprocess
import sys
import threading
import time
from concurrent.futures import Future
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient

import api.app as app_module
import api.accounts as accounts_module
import services.backup_service as backup_service_module
import services.image_service as image_service_module
import services.protocol.conversation as conversation_module
import services.task_executor as task_executor_module


class _FakeThread:
    def __init__(self) -> None:
        self.join_calls: list[float] = []

    def join(self, *, timeout: float) -> None:
        self.join_calls.append(timeout)


class AppLifecycleTests(unittest.TestCase):
    def test_lifespan_closes_storage_after_all_task_drains(self) -> None:
        events: list[str] = []
        watcher_thread = _FakeThread()
        cleanup_thread = _FakeThread()

        class Storage:
            def close(self) -> None:
                events.append("storage")

        async def drain_management() -> None:
            events.append("management")

        def drain_background() -> None:
            events.append("background")

        def drain_image() -> None:
            events.append("image")

        with (
            mock.patch.object(app_module, "start_limited_account_watcher", return_value=watcher_thread),
            mock.patch.object(app_module, "start_image_cleanup_scheduler", return_value=cleanup_thread),
            mock.patch.object(app_module.backup_service, "start"),
            mock.patch.object(app_module.backup_service, "stop", side_effect=lambda: events.append("backup")),
            mock.patch.object(app_module.config, "cleanup_old_images"),
            mock.patch.object(app_module.config, "get_storage_backend", return_value=Storage()),
            mock.patch.object(app_module.accounts, "wait_for_management_tasks", side_effect=drain_management),
            mock.patch.object(app_module, "wait_for_background_tasks", side_effect=drain_background),
            mock.patch.object(app_module, "wait_for_image_cleanup_tasks", side_effect=drain_image),
        ):
            with TestClient(app_module.create_app()):
                pass

        self.assertEqual(events, ["backup", "management", "background", "image", "storage"])

    def test_image_cleanup_drain_cannot_miss_future_before_registration(self) -> None:
        source_backend = type("SourceBackend", (), {"access_token": "leased-token"})()
        submit_called = threading.Event()
        allow_submit_return = threading.Event()
        producer_done = threading.Event()
        drain_seen_empty = threading.Event()
        drain_seen_registered = threading.Event()
        future = Future()
        producer_thread_id: list[int] = []
        producer_first_entered = threading.Event()
        real_lock = threading.Lock()

        class RaceLock:
            def __enter__(self):
                current_thread_id = threading.get_ident()
                if current_thread_id == producer_thread_id[0] and not producer_first_entered.is_set():
                    producer_first_entered.set()
                    if submit_called.is_set():
                        if not drain_seen_empty.wait(2):
                            raise AssertionError("drain did not inspect the unregistered future")
                real_lock.acquire()
                if current_thread_id != producer_thread_id[0]:
                    if conversation_module._IMAGE_CLEANUP_FUTURES:
                        drain_seen_registered.set()
                    else:
                        drain_seen_empty.set()
                return self

            def __exit__(self, *_args):
                real_lock.release()

        class Executor:
            def submit(self, _function):
                submit_called.set()
                if not allow_submit_return.wait(2):
                    raise AssertionError("cleanup submission was not released")
                return future

        old_lock = conversation_module._IMAGE_CLEANUP_FUTURES_LOCK
        try:
            with (
                mock.patch.dict(conversation_module.config.data, {"image_remove_conversation_always": True}),
                mock.patch.object(conversation_module, "_IMAGE_CLEANUP_EXECUTOR", Executor()),
                mock.patch.object(conversation_module, "_IMAGE_CLEANUP_FUTURES_LOCK", RaceLock()),
            ):
                def produce() -> None:
                    producer_thread_id.append(threading.get_ident())
                    conversation_module._remove_image_conversation_later(
                        source_backend,
                        "conversation-race",
                        success=False,
                    )
                    producer_done.set()

                producer = threading.Thread(target=produce)
                producer.start()
                self.assertTrue(submit_called.wait(1))
                drain_done = threading.Event()

                def drain() -> None:
                    conversation_module.wait_for_image_cleanup_tasks()
                    drain_done.set()

                drain_thread = threading.Thread(target=drain)
                drain_thread.start()
                allow_submit_return.set()

                self.assertTrue(producer_done.wait(2))
                self.assertTrue(drain_seen_registered.wait(1))
                self.assertFalse(drain_seen_empty.is_set())
                self.assertFalse(drain_done.is_set())
                future.set_result(None)
                drain_thread.join(2)
                producer.join(2)
                self.assertTrue(drain_done.is_set())
        finally:
            if not future.done():
                future.set_result(None)
            conversation_module._IMAGE_CLEANUP_SLOTS.release()
            conversation_module._IMAGE_CLEANUP_FUTURES.clear()
            conversation_module._IMAGE_CLEANUP_FUTURES_LOCK = old_lock

    def test_error_response_import_does_not_trigger_storage_factory_cycle(self) -> None:
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "from services.protocol.error_response import exception_log_message; print('ok')",
            ],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            text=True,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "ok")

    def test_startup_failure_stops_already_started_workers(self) -> None:
        stop_events = []
        watcher_thread = _FakeThread()
        cleanup_thread = _FakeThread()

        def start_watcher(stop_event):
            stop_events.append(stop_event)
            return watcher_thread

        def start_cleanup(stop_event):
            stop_events.append(stop_event)
            return cleanup_thread

        with (
            mock.patch.object(app_module, "start_limited_account_watcher", side_effect=start_watcher),
            mock.patch.object(app_module, "start_image_cleanup_scheduler", side_effect=start_cleanup),
            mock.patch.object(app_module.backup_service, "start", side_effect=RuntimeError("startup failed")),
            mock.patch.object(app_module.backup_service, "stop") as stop_backup,
        ):
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                with TestClient(app_module.create_app()):
                    pass

        self.assertEqual(len(stop_events), 2)
        self.assertTrue(stop_events[0].is_set())
        self.assertIs(stop_events[0], stop_events[1])
        self.assertEqual(watcher_thread.join_calls, [1])
        self.assertEqual(cleanup_thread.join_calls, [1])
        stop_backup.assert_called_once_with()

    def test_startup_failure_does_not_stop_uninitialized_backup(self) -> None:
        watcher_thread = _FakeThread()

        with (
            mock.patch.object(app_module, "start_limited_account_watcher", return_value=watcher_thread),
            mock.patch.object(
                app_module,
                "start_image_cleanup_scheduler",
                side_effect=RuntimeError("image scheduler failed"),
            ),
            mock.patch.object(app_module.backup_service, "start") as start_backup,
            mock.patch.object(app_module.backup_service, "stop") as stop_backup,
        ):
            with self.assertRaisesRegex(RuntimeError, "image scheduler failed"):
                with TestClient(app_module.create_app()):
                    pass

        start_backup.assert_not_called()
        stop_backup.assert_not_called()
        self.assertEqual(watcher_thread.join_calls, [1])

    def test_lifespan_cleanup_failure_does_not_skip_remaining_rollback(self) -> None:
        watcher_thread = _FakeThread()
        cleanup_thread = _FakeThread()
        events: list[str] = []

        with (
            mock.patch.object(app_module, "start_limited_account_watcher", return_value=watcher_thread),
            mock.patch.object(app_module, "start_image_cleanup_scheduler", return_value=cleanup_thread),
            mock.patch.object(app_module.backup_service, "start", side_effect=RuntimeError("startup failed")),
            mock.patch.object(
                app_module,
                "stop_limited_account_watcher",
                side_effect=RuntimeError("watcher stop failed"),
            ),
            mock.patch.object(app_module.backup_service, "stop", side_effect=lambda: events.append("backup_stop")),
            mock.patch.object(app_module, "wait_for_background_tasks", side_effect=lambda: events.append("background_drain")),
            mock.patch.object(app_module, "wait_for_image_cleanup_tasks", side_effect=lambda: events.append("image_drain")),
            mock.patch.object(app_module.config, "cleanup_old_images"),
        ):
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                with TestClient(app_module.create_app()):
                    pass

        self.assertEqual(cleanup_thread.join_calls, [1])
        self.assertEqual(events, ["backup_stop", "background_drain", "image_drain"])

    def test_lifespan_does_not_exit_before_async_drains_complete(self) -> None:
        async def scenario() -> None:
            watcher_thread = _FakeThread()
            cleanup_thread = _FakeThread()
            management_started = asyncio.Event()
            release_management = asyncio.Event()
            background_started = threading.Event()
            release_background = threading.Event()
            image_started = threading.Event()
            release_image = threading.Event()
            events: list[str] = []

            async def drain_management() -> None:
                management_started.set()
                await release_management.wait()
                events.append("management")

            def drain_background() -> None:
                background_started.set()
                if not release_background.wait(2):
                    raise AssertionError("background drain was not released")
                events.append("background")

            def drain_image() -> None:
                image_started.set()
                if not release_image.wait(2):
                    raise AssertionError("image drain was not released")
                events.append("image")

            with (
                mock.patch.object(app_module, "start_limited_account_watcher", return_value=watcher_thread),
                mock.patch.object(app_module, "start_image_cleanup_scheduler", return_value=cleanup_thread),
                mock.patch.object(app_module.backup_service, "start"),
                mock.patch.object(app_module.backup_service, "stop"),
                mock.patch.object(app_module.config, "cleanup_old_images"),
                mock.patch.object(app_module.accounts, "wait_for_management_tasks", side_effect=drain_management),
                mock.patch.object(app_module, "wait_for_background_tasks", side_effect=drain_background),
                mock.patch.object(app_module, "wait_for_image_cleanup_tasks", side_effect=drain_image),
            ):
                app = app_module.create_app()
                context = app.router.lifespan_context(app)
                await context.__aenter__()
                exit_task = asyncio.create_task(context.__aexit__(None, None, None))
                try:
                    await asyncio.wait_for(management_started.wait(), timeout=1)
                    self.assertFalse(exit_task.done())
                    release_management.set()
                    self.assertTrue(
                        await asyncio.to_thread(background_started.wait, 2),
                        "background drain did not start",
                    )
                    self.assertFalse(exit_task.done())
                    release_background.set()
                    self.assertTrue(
                        await asyncio.to_thread(image_started.wait, 2),
                        "image drain did not start",
                    )
                    self.assertFalse(exit_task.done())
                    release_image.set()
                    await exit_task
                finally:
                    release_management.set()
                    release_background.set()
                    release_image.set()
                    if not exit_task.done():
                        await exit_task

            self.assertEqual(events, ["management", "background", "image"])

        asyncio.run(scenario())

    def test_startup_image_cleanup_stays_off_the_asgi_event_loop(self) -> None:
        watcher_thread = _FakeThread()
        cleanup_thread = _FakeThread()

        def cleanup_old_images() -> int:
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                return 0
            raise AssertionError("startup image cleanup ran on the ASGI event loop")

        with (
            mock.patch.object(app_module, "start_limited_account_watcher", return_value=watcher_thread),
            mock.patch.object(app_module, "start_image_cleanup_scheduler", return_value=cleanup_thread),
            mock.patch.object(app_module.backup_service, "start"),
            mock.patch.object(app_module.backup_service, "stop"),
            mock.patch.object(app_module.config, "cleanup_old_images", side_effect=cleanup_old_images),
            mock.patch.object(app_module, "wait_for_background_tasks"),
            mock.patch.object(app_module, "wait_for_image_cleanup_tasks"),
        ):
            with TestClient(app_module.create_app()):
                pass

    def test_shutdown_waits_for_accepted_management_task(self) -> None:
        async def scenario() -> None:
            started = asyncio.Event()
            release = asyncio.Event()

            async def task_factory() -> None:
                started.set()
                await release.wait()

            accounts_module._schedule_management_task(task_factory)
            await started.wait()
            drain = asyncio.create_task(accounts_module.wait_for_management_tasks())
            await asyncio.sleep(0)
            self.assertFalse(drain.done())
            release.set()
            await drain
            self.assertFalse(accounts_module._MANAGEMENT_TASKS)

        asyncio.run(scenario())

    def test_management_task_releases_reservation_if_cancelled_before_start(self) -> None:
        class Reservation:
            def __init__(self) -> None:
                self.release_calls = 0

            def release(self) -> None:
                self.release_calls += 1

        async def scenario() -> None:
            reservation = Reservation()

            async def task_factory() -> None:
                raise AssertionError("the cancelled task must not start")

            with mock.patch.object(
                accounts_module,
                "reserve_background_task",
                return_value=reservation,
            ):
                accounts_module._schedule_management_task(task_factory)
                task = next(iter(accounts_module._MANAGEMENT_TASKS))
                task.cancel()
                await asyncio.sleep(0)
                await asyncio.sleep(0)

            self.assertEqual(reservation.release_calls, 1)
            self.assertFalse(accounts_module._MANAGEMENT_TASKS)

        asyncio.run(scenario())

    def test_background_task_drain_waits_for_accepted_future(self) -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def worker() -> None:
            started.set()
            release.wait()
            finished.set()

        reservation = task_executor_module.reserve_background_task()
        future = reservation.submit(worker)
        self.assertTrue(started.wait(1))
        drain_thread = threading.Thread(
            target=task_executor_module.wait_for_background_tasks,
            daemon=True,
        )
        drain_thread.start()
        self.assertTrue(drain_thread.is_alive())
        release.set()
        drain_thread.join(1)
        self.assertFalse(drain_thread.is_alive())
        self.assertTrue(finished.is_set())
        self.assertTrue(future.done())

    def test_background_reservation_releases_capacity_when_executor_submit_fails(self) -> None:
        executor = task_executor_module._EXECUTOR
        with mock.patch.object(executor, "submit", side_effect=RuntimeError("submit failed")):
            reservation = task_executor_module.reserve_background_task()
            with self.assertRaisesRegex(RuntimeError, "submit failed"):
                reservation.submit(lambda: None)

        reservations = []
        try:
            for _ in range(task_executor_module.BACKGROUND_TASK_CAPACITY):
                reservations.append(task_executor_module.reserve_background_task())
        finally:
            for held in reservations:
                held.release()

        self.assertEqual(len(reservations), task_executor_module.BACKGROUND_TASK_CAPACITY)

    def test_background_reservation_cancel_is_serialized_with_submit(self) -> None:
        class Capacity:
            def __init__(self) -> None:
                self.available = 0

            def acquire(self, *, blocking: bool) -> bool:
                if self.available:
                    return False
                self.available = 1
                return True

            def release(self) -> None:
                self.available -= 1

        capacity = Capacity()
        submit_entered = threading.Event()
        allow_submit_return = threading.Event()
        worker_started = threading.Event()
        release_worker = threading.Event()
        cancel_done = threading.Event()
        submit_errors: list[BaseException] = []
        cancel_errors: list[BaseException] = []

        def worker() -> None:
            worker_started.set()
            release_worker.wait(5)

        executor = task_executor_module._EXECUTOR
        original_submit = executor.submit

        def delayed_submit(function, *args, **kwargs):
            future = original_submit(function, *args, **kwargs)
            submit_entered.set()
            if not allow_submit_return.wait(5):
                raise AssertionError("submit return was not released")
            return future

        def submit() -> None:
            try:
                reservation.submit(worker)
            except BaseException as exc:
                submit_errors.append(exc)

        def cancel() -> None:
            try:
                reservation.cancel()
            except BaseException as exc:
                cancel_errors.append(exc)
            finally:
                cancel_done.set()

        submit_thread = threading.Thread(target=submit, daemon=True)
        cancel_thread = threading.Thread(target=cancel, daemon=True)
        reservation = None
        extra_reservation = None
        try:
            with (
                mock.patch.object(task_executor_module, "_CAPACITY", capacity),
                mock.patch.object(executor, "submit", side_effect=delayed_submit),
            ):
                reservation = task_executor_module.reserve_background_task()
                submit_thread.start()
                self.assertTrue(submit_entered.wait(1))
                self.assertTrue(worker_started.wait(1))
                cancel_thread.start()
                self.assertFalse(
                    cancel_done.wait(0.1),
                    "cancel released capacity while submit still owned the reservation",
                )
                allow_submit_return.set()
                submit_thread.join(2)
                cancel_thread.join(2)

                try:
                    extra_reservation = task_executor_module.reserve_background_task()
                except task_executor_module.BackgroundTaskQueueFullError:
                    pass

            self.assertFalse(submit_thread.is_alive())
            self.assertFalse(cancel_thread.is_alive())
            self.assertEqual(submit_errors, [])
            self.assertEqual(cancel_errors, [])
            self.assertIsNone(
                extra_reservation,
                "cancel released capacity before the accepted task finished",
            )
        finally:
            allow_submit_return.set()
            release_worker.set()
            submit_thread.join(2)
            cancel_thread.join(2)
            capacity.available = 0

    def test_background_task_drain_cannot_miss_future_between_submit_and_registration(self) -> None:
        submit_returned = threading.Event()
        allow_submit_return = threading.Event()
        worker_started = threading.Event()
        release_worker = threading.Event()

        def worker() -> None:
            worker_started.set()
            release_worker.wait(5)

        executor = task_executor_module._EXECUTOR
        original_submit = executor.submit

        def delayed_submit(function, *args, **kwargs):
            future = original_submit(function, *args, **kwargs)
            submit_returned.set()
            if not allow_submit_return.wait(5):
                raise AssertionError("submit registration barrier was not released")
            return future

        reservation = task_executor_module.reserve_background_task()
        submit_thread = None
        drain_thread = None
        try:
            with mock.patch.object(executor, "submit", side_effect=delayed_submit):
                submit_thread = threading.Thread(
                    target=lambda: reservation.submit(worker),
                    daemon=True,
                )
                submit_thread.start()
                self.assertTrue(submit_returned.wait(1))
                self.assertTrue(worker_started.wait(1))

                drain_thread = threading.Thread(
                    target=task_executor_module.wait_for_background_tasks,
                    daemon=True,
                )
                drain_thread.start()
                drain_thread.join(0.1)
                self.assertTrue(drain_thread.is_alive())

                allow_submit_return.set()
                submit_thread.join(1)
                self.assertFalse(submit_thread.is_alive())
                release_worker.set()
                drain_thread.join(2)
                self.assertFalse(drain_thread.is_alive())
        finally:
            allow_submit_return.set()
            release_worker.set()
            if submit_thread is not None:
                submit_thread.join(2)
            if drain_thread is not None:
                drain_thread.join(2)
            reservation.release()

    def test_run_with_timeout_does_not_wait_for_non_cancellable_callable(self) -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def blocked() -> None:
            started.set()
            release.wait(5)
            finished.set()

        with self.assertRaisesRegex(TimeoutError, "refresh timed out"):
            task_executor_module.run_with_timeout(
                blocked,
                timeout=0.05,
                timeout_message="refresh timed out",
            )

        self.assertTrue(started.is_set())
        self.assertFalse(finished.is_set())
        release.set()
        self.assertTrue(finished.wait(1))

    def test_background_drain_waits_for_timed_out_child_operation(self) -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        def blocked() -> None:
            started.set()
            release.wait(5)
            finished.set()

        def timed_call() -> None:
            with self.assertRaises(TimeoutError):
                task_executor_module.run_with_timeout(
                    blocked,
                    timeout=0.05,
                    timeout_message="child timed out",
                )

        reservation = task_executor_module.reserve_background_task()
        future = reservation.submit(timed_call)
        self.assertTrue(started.wait(1))
        future.result(timeout=1)

        drain_finished = threading.Event()

        def drain() -> None:
            task_executor_module.wait_for_background_tasks()
            drain_finished.set()

        drain_thread = threading.Thread(target=drain, daemon=True)
        drain_thread.start()
        try:
            self.assertFalse(drain_finished.wait(0.2))
        finally:
            release.set()
            self.assertTrue(drain_finished.wait(2))
            drain_thread.join(timeout=1)
            self.assertFalse(drain_thread.is_alive())
            self.assertTrue(finished.is_set())

    def test_repeated_timeouts_keep_non_cancellable_workers_bounded(self) -> None:
        release = threading.Event()
        started_count = 0
        started_lock = threading.Lock()

        def blocked() -> None:
            nonlocal started_count
            with started_lock:
                started_count += 1
            release.wait(5)

        try:
            for _ in range(task_executor_module.BACKGROUND_TASK_WORKERS + 4):
                with self.assertRaisesRegex(TimeoutError, "refresh timed out"):
                    task_executor_module.run_with_timeout(
                        blocked,
                        timeout=0.05,
                        timeout_message="refresh timed out",
                    )

            self.assertLessEqual(
                started_count,
                task_executor_module.BACKGROUND_TASK_WORKERS,
            )
        finally:
            release.set()

    def test_timeout_admission_does_not_queue_after_workers_are_full(self) -> None:
        worker_count = task_executor_module.BACKGROUND_TASK_WORKERS
        release = threading.Event()
        started = threading.Event()
        started_count = 0
        started_lock = threading.Lock()
        holder_threads: list[threading.Thread] = []

        def blocked() -> None:
            nonlocal started_count
            with started_lock:
                started_count += 1
                if started_count == worker_count:
                    started.set()
            release.wait(5)

        def hold_worker() -> None:
            try:
                task_executor_module.run_with_timeout(
                    blocked,
                    timeout=5,
                    timeout_message="holder timed out",
                )
            except TimeoutError:
                pass

        try:
            holder_threads = [threading.Thread(target=hold_worker, daemon=True) for _ in range(worker_count)]
            for thread in holder_threads:
                thread.start()
            self.assertTrue(started.wait(2))

            work_queue = task_executor_module._TIMEOUT_EXECUTOR._work_queue
            queue_before = work_queue.qsize()
            late_started = threading.Event()
            started_at = time.monotonic()
            with self.assertRaisesRegex(TimeoutError, "late call timed out"):
                task_executor_module.run_with_timeout(
                    late_started.set,
                    timeout=0.05,
                    timeout_message="late call timed out",
                )
            elapsed = time.monotonic() - started_at

            self.assertLess(elapsed, 0.5)
            self.assertEqual(work_queue.qsize(), queue_before)
            release.set()
            for thread in holder_threads:
                thread.join(2)
            self.assertFalse(late_started.is_set())
            task_executor_module.run_with_timeout(
                late_started.set,
                timeout=0.5,
                timeout_message="released call timed out",
            )
            self.assertTrue(late_started.is_set())
        finally:
            release.set()
            for thread in holder_threads:
                thread.join(2)

    def test_timeout_admission_is_released_after_callable_error(self) -> None:
        def failed() -> None:
            raise RuntimeError("worker failed")

        with self.assertRaisesRegex(RuntimeError, "worker failed"):
            task_executor_module.run_with_timeout(
                failed,
                timeout=0.5,
                timeout_message="call timed out",
            )

        completed = threading.Event()
        task_executor_module.run_with_timeout(
            completed.set,
            timeout=0.5,
            timeout_message="call timed out",
        )
        self.assertTrue(completed.is_set())

    def test_timeout_admission_is_released_after_submit_failure(self) -> None:
        with mock.patch.object(
            task_executor_module._TIMEOUT_EXECUTOR,
            "submit",
            side_effect=RuntimeError("submit failed"),
        ):
            with self.assertRaisesRegex(RuntimeError, "submit failed"):
                task_executor_module.run_with_timeout(
                    lambda: None,
                    timeout=0.5,
                    timeout_message="call timed out",
                )

        completed = threading.Event()
        task_executor_module.run_with_timeout(
            completed.set,
            timeout=0.5,
            timeout_message="call timed out",
        )
        self.assertTrue(completed.is_set())

    def test_image_cleanup_scheduler_tracks_inflight_cleanup_until_shutdown_drain(self) -> None:
        class ControlledStopEvent:
            def __init__(self) -> None:
                self.first_wait = True
                self.stopped = False

            def wait(self, _timeout: float) -> bool:
                if self.first_wait:
                    self.first_wait = False
                    return False
                return self.stopped

            def set(self) -> None:
                self.stopped = True

            def is_set(self) -> bool:
                return self.stopped

        stop_event = ControlledStopEvent()
        cleanup_started = threading.Event()
        release_cleanup = threading.Event()
        cleanup_finished = threading.Event()

        def blocked_cleanup() -> None:
            cleanup_started.set()
            if not release_cleanup.wait(5):
                raise AssertionError("cleanup did not receive release")
            cleanup_finished.set()

        with (
            mock.patch.object(image_service_module.config, "cleanup_old_images", side_effect=blocked_cleanup),
            mock.patch.object(image_service_module, "cleanup_image_thumbnails", return_value=0),
            mock.patch.object(
                image_service_module.shutil,
                "disk_usage",
                return_value=type("Usage", (), {"free": 10**12})(),
            ),
        ):
            thread = image_service_module.start_image_cleanup_scheduler(stop_event)
            try:
                self.assertTrue(cleanup_started.wait(1))
                stop_event.set()
                thread.join(timeout=1)
                self.assertFalse(thread.is_alive())

                drain = threading.Thread(target=task_executor_module.wait_for_background_tasks)
                drain.start()
                self.assertTrue(drain.is_alive())
                release_cleanup.set()
                drain.join(timeout=2)
                self.assertFalse(drain.is_alive())
                self.assertTrue(cleanup_finished.is_set())
            finally:
                release_cleanup.set()
                stop_event.set()
                thread.join(timeout=2)

    def test_image_cleanup_restart_waits_for_timed_out_scheduler_generation(self) -> None:
        class BarrierEvent:
            def __init__(self, entered: threading.Event, release: threading.Event) -> None:
                self.entered = entered
                self.release = release
                self.stopped = False
                self.first_wait = True

            def wait(self, timeout: float) -> bool:
                if timeout == 1800 and self.first_wait:
                    self.first_wait = False
                    self.entered.set()
                    if not self.release.wait(2):
                        raise AssertionError("scheduler generation was not released")
                    return True
                return self.stopped

            def set(self) -> None:
                self.stopped = True

            def is_set(self) -> bool:
                return self.stopped

        first_entered = threading.Event()
        release_first = threading.Event()
        second_entered = threading.Event()
        release_second = threading.Event()
        first_event = BarrierEvent(first_entered, release_first)
        second_event = BarrierEvent(second_entered, release_second)

        first_thread = image_service_module.start_image_cleanup_scheduler(first_event)
        self.assertTrue(first_entered.wait(1))
        first_event.set()
        first_thread.join(timeout=0)
        self.assertTrue(first_thread.is_alive())

        second_thread = image_service_module.start_image_cleanup_scheduler(second_event)
        try:
            self.assertFalse(
                second_entered.wait(0.1),
                "a new image cleanup generation must wait for the timed-out old generation",
            )
            release_first.set()
            self.assertTrue(second_entered.wait(1))
        finally:
            second_event.set()
            release_first.set()
            release_second.set()
            first_thread.join(timeout=2)
            second_thread.join(timeout=2)

    def test_image_cleanup_scheduler_repeated_start_is_single_and_stop_allows_restart(self) -> None:
        def idle_worker(stop_event: threading.Event) -> None:
            stop_event.wait(2)

        with mock.patch.object(image_service_module, "_run_auto_cleanup_worker", side_effect=idle_worker):
            first_event = threading.Event()
            first_thread = image_service_module.start_image_cleanup_scheduler(first_event)
            try:
                self.assertIs(
                    image_service_module.start_image_cleanup_scheduler(first_event),
                    first_thread,
                )
                image_service_module.stop_image_cleanup_scheduler(first_event, first_thread)
                self.assertFalse(first_thread.is_alive())

                second_event = threading.Event()
                second_thread = image_service_module.start_image_cleanup_scheduler(second_event)
                try:
                    self.assertIsNot(second_thread, first_thread)
                finally:
                    image_service_module.stop_image_cleanup_scheduler(second_event, second_thread)
            finally:
                first_event.set()
                if first_thread.is_alive():
                    first_thread.join(timeout=2)

    def test_image_cleanup_scheduler_start_failure_clears_generation_owner(self) -> None:
        class FailingThread:
            def start(self) -> None:
                raise RuntimeError("thread start failed")

        stop_event = threading.Event()
        with mock.patch.object(image_service_module.threading, "Thread", return_value=FailingThread()):
            with self.assertRaisesRegex(RuntimeError, "thread start failed"):
                image_service_module.start_image_cleanup_scheduler(stop_event)

        self.assertIsNone(image_service_module._IMAGE_CLEANUP_THREAD)
        self.assertIsNone(image_service_module._IMAGE_CLEANUP_EVENT)

    def test_image_cleanup_scheduler_does_not_submit_after_stop_during_reservation(self) -> None:
        class ControlledStopEvent:
            def __init__(self) -> None:
                self.stopped = False
                self.first_wait = True

            def is_set(self) -> bool:
                return self.stopped

            def wait(self, timeout: float) -> bool:
                if timeout == 1800 and self.first_wait:
                    self.first_wait = False
                    return False
                return self.stopped

            def set(self) -> None:
                self.stopped = True

        stop_event = ControlledStopEvent()
        reservation_ready = threading.Event()
        release_reservation = threading.Event()
        submitted: list[object] = []

        class ImmediateFuture:
            def done(self) -> bool:
                return True

        class Reservation:
            def cancel(self) -> None:
                return None

            def submit(self, function, *args, **kwargs):
                submitted.append((function, args, kwargs))
                return ImmediateFuture()

        def reserve():
            reservation_ready.set()
            if not release_reservation.wait(5):
                raise AssertionError("reservation was not released")
            return Reservation()

        with mock.patch.object(image_service_module, "reserve_background_task", side_effect=reserve):
            thread = image_service_module.start_image_cleanup_scheduler(stop_event)
            self.assertTrue(reservation_ready.wait(1))
            stop_event.set()
            release_reservation.set()
            thread.join(timeout=2)

        self.assertEqual(submitted, [])

    def test_image_cleanup_scheduler_backs_off_when_task_queue_is_full(self) -> None:
        queue_full = threading.Event()
        stop_requested = threading.Event()
        wait_calls: list[float] = []

        class ControlledStopEvent:
            def wait(self, timeout: float) -> bool:
                wait_calls.append(timeout)
                if timeout == 1800:
                    return stop_requested.is_set()
                if timeout == 30:
                    stop_requested.set()
                    return True
                raise AssertionError(f"unexpected stop wait: {timeout}")

            def is_set(self) -> bool:
                return stop_requested.is_set()

        def queue_is_full():
            queue_full.set()
            raise task_executor_module.BackgroundTaskQueueFullError("full")

        stop_event = ControlledStopEvent()
        with mock.patch.object(image_service_module, "reserve_background_task", side_effect=queue_is_full):
            thread = image_service_module.start_image_cleanup_scheduler(stop_event)
            try:
                self.assertTrue(queue_full.wait(1))
                thread.join(timeout=1)
                self.assertFalse(thread.is_alive())
            finally:
                stop_requested.set()
                thread.join(timeout=2)

        self.assertIn(30, wait_calls)

    def test_image_cleanup_scheduler_backs_off_when_task_submission_fails(self) -> None:
        stop_requested = threading.Event()
        wait_calls: list[float] = []
        long_waits = 0

        class ControlledStopEvent:
            def wait(self, timeout: float) -> bool:
                nonlocal long_waits
                wait_calls.append(timeout)
                if timeout == 1800:
                    long_waits += 1
                    if long_waits > 1:
                        stop_requested.set()
                        return True
                    return False
                if timeout == 30:
                    stop_requested.set()
                    return True
                raise AssertionError(f"unexpected stop wait: {timeout}")

            def is_set(self) -> bool:
                return stop_requested.is_set()

        stop_event = ControlledStopEvent()
        with mock.patch.object(
            task_executor_module._EXECUTOR,
            "submit",
            side_effect=RuntimeError("submit failed"),
        ):
            thread = image_service_module.start_image_cleanup_scheduler(stop_event)
            thread.join(timeout=1)
            self.assertFalse(thread.is_alive())

        self.assertIn(30, wait_calls)

    def test_backup_scheduler_tracks_inflight_run_until_shutdown_drain(self) -> None:
        service = backup_service_module.BackupService()
        run_started = threading.Event()
        release_run = threading.Event()
        run_finished = threading.Event()

        def blocked_run() -> None:
            run_started.set()
            if not release_run.wait(5):
                raise AssertionError("backup run did not receive release")
            run_finished.set()

        with mock.patch.object(service, "run_scheduled_backup_if_needed", side_effect=blocked_run):
            service.start()
            thread = service._thread
            self.assertIsNotNone(thread)
            try:
                self.assertTrue(run_started.wait(1))
                service.stop()
                self.assertFalse(thread.is_alive())

                drain = threading.Thread(target=task_executor_module.wait_for_background_tasks)
                drain.start()
                self.assertTrue(drain.is_alive())
                release_run.set()
                drain.join(timeout=2)
                self.assertFalse(drain.is_alive())
                self.assertTrue(run_finished.is_set())
            finally:
                release_run.set()
                service.stop()

    def test_sequential_lifespan_restart_does_not_lose_backup_scheduler(self) -> None:
        service = backup_service_module.BackupService()
        first_reserve_entered = threading.Event()
        release_first_reserve = threading.Event()
        second_reserve_entered = threading.Event()
        reserve_calls = 0
        reserve_lock = threading.Lock()

        def reserve() -> object:
            nonlocal reserve_calls
            with reserve_lock:
                reserve_calls += 1
                call_number = reserve_calls
            if call_number == 1:
                first_reserve_entered.set()
                if not release_first_reserve.wait(1):
                    raise AssertionError("first scheduler reserve was not released")
                return mock.Mock()
            second_reserve_entered.set()
            raise backup_service_module.BackgroundTaskQueueFullError("queue full")

        with (
            mock.patch.object(backup_service_module, "reserve_background_task", side_effect=reserve),
            mock.patch.object(threading.Thread, "join", return_value=None),
        ):
            service.start()
            self.assertTrue(first_reserve_entered.wait(1))

            service.stop()
            service.start()
            release_first_reserve.set()

            self.assertTrue(
                second_reserve_entered.wait(1),
                "a restart requested after a timed-out stop must start after the old scheduler exits",
            )

        service.stop()

    def test_repeated_stop_cancels_pending_backup_scheduler_restart(self) -> None:
        service = backup_service_module.BackupService()
        first_reserve_entered = threading.Event()
        release_first_reserve = threading.Event()
        second_reserve_entered = threading.Event()
        reserve_calls = 0
        reserve_lock = threading.Lock()

        def reserve() -> object:
            nonlocal reserve_calls
            with reserve_lock:
                reserve_calls += 1
                call_number = reserve_calls
            if call_number == 1:
                first_reserve_entered.set()
                if not release_first_reserve.wait(1):
                    raise AssertionError("first scheduler reserve was not released")
                return mock.Mock()
            second_reserve_entered.set()
            raise backup_service_module.BackgroundTaskQueueFullError("queue full")

        with (
            mock.patch.object(backup_service_module, "reserve_background_task", side_effect=reserve),
            mock.patch.object(threading.Thread, "join", return_value=None),
        ):
            service.start()
            self.assertTrue(first_reserve_entered.wait(1))
            service.stop()
            service.start()
            service.stop()
            release_first_reserve.set()

        self.assertFalse(second_reserve_entered.wait(0.2))
        service.stop()
    def test_app_shutdown_invokes_background_task_drain(self) -> None:
        watcher_thread = _FakeThread()
        cleanup_thread = _FakeThread()
        with (
            mock.patch.object(app_module, "start_limited_account_watcher", return_value=watcher_thread),
            mock.patch.object(app_module, "start_image_cleanup_scheduler", return_value=cleanup_thread),
            mock.patch.object(app_module.backup_service, "start"),
            mock.patch.object(app_module.backup_service, "stop"),
            mock.patch.object(app_module.config, "cleanup_old_images"),
            mock.patch.object(app_module, "wait_for_background_tasks") as drain,
            mock.patch.object(app_module, "wait_for_image_cleanup_tasks") as image_drain,
        ):
            with TestClient(app_module.create_app()):
                pass
        drain.assert_called_once_with()
        image_drain.assert_called_once_with()

    def test_app_shutdown_stops_backup_scheduler_before_task_drains(self) -> None:
        events: list[str] = []
        watcher_thread = _FakeThread()
        cleanup_thread = _FakeThread()
        with (
            mock.patch.object(app_module, "start_limited_account_watcher", return_value=watcher_thread),
            mock.patch.object(app_module, "start_image_cleanup_scheduler", return_value=cleanup_thread),
            mock.patch.object(app_module.backup_service, "start"),
            mock.patch.object(app_module.backup_service, "stop", side_effect=lambda: events.append("backup_stop")),
            mock.patch.object(app_module.config, "cleanup_old_images"),
            mock.patch.object(app_module, "wait_for_background_tasks", side_effect=lambda: events.append("background_drain")),
            mock.patch.object(app_module, "wait_for_image_cleanup_tasks", side_effect=lambda: events.append("image_drain")),
        ):
            with TestClient(app_module.create_app()):
                pass
        self.assertEqual(events, ["backup_stop", "background_drain", "image_drain"])

    def test_image_cleanup_drain_waits_for_accepted_deletion(self) -> None:
        started = threading.Event()
        release = threading.Event()
        finished = threading.Event()

        class FakeBackend:
            access_token = "leased-token"

            def delete_conversation(self, _conversation_id: str) -> None:
                started.set()
                release.wait()
                finished.set()

            def close(self) -> None:
                pass

        source_backend = FakeBackend()
        with (
            mock.patch.dict(conversation_module.config.data, {"image_remove_conversation_always": True}),
            mock.patch.object(conversation_module, "OpenAIBackendAPI", return_value=FakeBackend()),
        ):
            conversation_module._remove_image_conversation_later(
                source_backend,
                "conversation-1",
                success=False,
            )
            self.assertTrue(started.wait(1))
            drain_thread = threading.Thread(
                target=conversation_module.wait_for_image_cleanup_tasks,
                daemon=True,
            )
            drain_thread.start()
            self.assertTrue(drain_thread.is_alive())
            release.set()
            drain_thread.join(1)

        self.assertFalse(drain_thread.is_alive())
        self.assertTrue(finished.is_set())


if __name__ == "__main__":
    unittest.main()
