from __future__ import annotations

import json
import multiprocessing
import os
import time
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import mock


def _wait_for_both(directory: Path, prefix: str) -> None:
    deadline = time.monotonic() + 10
    while not all((directory / f"{prefix}-{worker_id}").exists() for worker_id in ("a", "b")):
        if time.monotonic() >= deadline:
            raise TimeoutError(f"cross-process barrier timeout: {prefix}")
        time.sleep(0.005)


def _cross_process_writer(kind: str, path_text: str, barrier_text: str, worker_id: str, outcomes) -> None:
    from services.cpa_service import CPAConfig
    from services.ccload_service import CCLoadConfig
    from services.sub2api_service import Sub2APIConfig

    path = Path(path_text)
    barrier = Path(barrier_text)
    if kind == "cpa":
        config = CPAConfig(path)

        def operation() -> None:
            config.add_pool(
                f"pool-{worker_id}",
                "https://example.test",
                f"secret-{worker_id}",
            )
    elif kind == "sub2api":
        config = Sub2APIConfig(path)

        def operation() -> None:
            config.add_server(
                name=f"server-{worker_id}",
                base_url="https://example.test",
                email=f"{worker_id}@example.test",
                password=f"password-{worker_id}",
                api_key=f"key-{worker_id}",
            )
    elif kind == "ccload":
        config = CCLoadConfig(path)

        def operation() -> None:
            config.add_server(
                name=f"server-{worker_id}",
                base_url="https://example.test",
                password=f"password-{worker_id}",
            )
    else:
        raise AssertionError(kind)

    original_reload = config._reload_locked

    def reload_at_barrier() -> None:
        original_reload()
        (barrier / f"reloaded-{worker_id}").write_text("ready", encoding="ascii")
        _wait_for_both(barrier, "reloaded")

    config._reload_locked = reload_at_barrier
    try:
        operation()
    except Exception as exc:
        outcomes.put((worker_id, "error", type(exc).__name__))
    else:
        outcomes.put((worker_id, "success", ""))


def _crash_while_holding_path_lock(path_text: str, ready_text: str) -> None:
    from services.storage.base import canonical_path_write_lock

    with canonical_path_write_lock(Path(path_text)):
        Path(ready_text).write_text("locked", encoding="ascii")
        os._exit(0)


def _json_cas_writer(
    kind: str,
    accounts_path_text: str,
    auth_keys_path_text: str,
    barrier_text: str,
    worker_id: str,
    outcomes,
) -> None:
    from services.storage.json_storage import JSONStorageBackend

    backend = JSONStorageBackend(Path(accounts_path_text), Path(auth_keys_path_text))
    barrier = Path(barrier_text)
    if kind == "accounts":
        expected = backend.load_accounts_snapshot()
        records = [{"id": worker_id, "kind": "account"}]

        def write() -> None:
            backend.save_accounts_if_revision(expected, records)
    elif kind == "auth_keys":
        expected = backend.load_auth_keys_snapshot()
        records = [{"id": worker_id, "kind": "auth_key"}]

        def write() -> None:
            backend.save_auth_keys_if_revision(expected, records)
    else:
        raise AssertionError(kind)

    (barrier / f"json-cas-ready-{worker_id}").write_text("ready", encoding="ascii")
    _wait_for_both(barrier, "json-cas-ready")
    try:
        write()
    except Exception as exc:
        outcomes.put((worker_id, "error", type(exc).__name__))
    else:
        outcomes.put((worker_id, "success", ""))


def _json_direct_writer(
    kind: str,
    accounts_path_text: str,
    auth_keys_path_text: str,
    barrier_text: str,
    outcomes,
) -> None:
    from services.storage.json_storage import JSONStorageBackend

    backend = JSONStorageBackend(Path(accounts_path_text), Path(auth_keys_path_text))
    barrier = Path(barrier_text)
    (barrier / "json-direct-ready").write_text("ready", encoding="ascii")
    deadline = time.monotonic() + 10
    while not (barrier / "json-cas-entered-save").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("JSON CAS lock-entry timeout")
        time.sleep(0.005)
    (barrier / "json-direct-attempted").write_text("attempted", encoding="ascii")
    try:
        if kind == "accounts":
            backend.save_accounts([{"id": "direct", "kind": "account"}])
        elif kind == "auth_keys":
            backend.save_auth_keys([{"id": "direct", "kind": "auth_key"}])
        else:
            raise AssertionError(kind)
    except Exception as exc:
        outcomes.put(("direct", "error", type(exc).__name__))
    else:
        (barrier / "json-direct-done").write_text("done", encoding="ascii")
        outcomes.put(("direct", "success", ""))


def _json_cas_writer_with_gate(
    kind: str,
    accounts_path_text: str,
    auth_keys_path_text: str,
    barrier_text: str,
    outcomes,
) -> None:
    from services.storage.json_storage import (
        ACCOUNT_SNAPSHOT_MAX_BYTES,
        JSONStorageBackend,
        _AUTH_KEYS_MAX_BYTES,
    )

    backend = JSONStorageBackend(Path(accounts_path_text), Path(auth_keys_path_text))
    barrier = Path(barrier_text)
    if kind == "accounts":
        expected = backend.load_accounts_snapshot()
        records = [{"id": "cas", "kind": "account"}]

        def write() -> None:
            backend.save_accounts_if_revision(expected, records)

        expected_path = Path(accounts_path_text)
        expected_max_bytes = ACCOUNT_SNAPSHOT_MAX_BYTES
    elif kind == "auth_keys":
        expected = backend.load_auth_keys_snapshot()
        records = [{"id": "cas", "kind": "auth_key"}]

        def write() -> None:
            backend.save_auth_keys_if_revision(expected, records)

        expected_path = Path(auth_keys_path_text)
        expected_max_bytes = _AUTH_KEYS_MAX_BYTES
    else:
        raise AssertionError(kind)

    original_save_json_value = backend._save_json_value

    def gated_save(
        file_path,
        value,
        *,
        max_bytes: int,
        cumulative_total: int | None = None,
    ) -> None:
        assert Path(file_path) == expected_path
        assert max_bytes == expected_max_bytes
        assert cumulative_total is None
        (barrier / "json-cas-entered-save").write_text("entered", encoding="ascii")
        deadline = time.monotonic() + 10
        while not (barrier / "json-release-cas").exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("JSON CAS save gate timeout")
            time.sleep(0.005)
        original_save_json_value(
            file_path,
            value,
            max_bytes=max_bytes,
            cumulative_total=cumulative_total,
        )

    backend._save_json_value = gated_save
    (barrier / "json-cas-ready-cas").write_text("ready", encoding="ascii")
    deadline = time.monotonic() + 10
    while not (barrier / "json-direct-ready").exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("JSON CAS/direct readiness timeout")
        time.sleep(0.005)
    try:
        write()
    except Exception as exc:
        outcomes.put(("cas", "error", type(exc).__name__))
    else:
        outcomes.put(("cas", "success", ""))


class CrossProcessPathWriteLockTests(unittest.TestCase):
    @staticmethod
    def _wait_for_marker(directory: Path, name: str) -> None:
        deadline = time.monotonic() + 10
        marker = directory / name
        while not marker.exists():
            if time.monotonic() >= deadline:
                raise TimeoutError(f"marker timeout: {name}")
            time.sleep(0.005)

    def test_lock_initialization_closes_handle_on_non_contention_error_and_is_reentrant(self) -> None:
        from services.storage.base import _CrossProcessPathWriteLock

        with TemporaryDirectory() as temp_dir:
            lock = _CrossProcessPathWriteLock(Path(temp_dir) / "state.json")
            with mock.patch.object(
                lock,
                "_acquire_os_lock",
                side_effect=OSError("non-contention failure"),
            ):
                with self.assertRaisesRegex(OSError, "non-contention failure"):
                    with lock:
                        pass
            self.assertIsNone(lock._handle)
            self.assertIsNone(lock._os_lock_state)

            with lock:
                with lock:
                    pass

    def test_kernel_handle_releases_path_lock_after_process_exit(self) -> None:
        context = multiprocessing.get_context("spawn")
        with TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "state.json"
            ready = Path(temp_dir) / "locked"
            process = context.Process(
                target=_crash_while_holding_path_lock,
                args=(str(path), str(ready)),
            )
            process.start()
            deadline = time.monotonic() + 10
            while not ready.exists() and time.monotonic() < deadline:
                time.sleep(0.005)
            self.assertTrue(ready.exists())
            process.join(timeout=10)
            self.assertEqual(process.exitcode, 0)
            from services.storage.base import canonical_path_write_lock

            with canonical_path_write_lock(path):
                pass

    def test_three_json_writers_serialize_revision_check_and_replace_across_processes(self) -> None:
        context = multiprocessing.get_context("spawn")
        for kind in ("cpa", "sub2api", "ccload"):
            with self.subTest(kind=kind), TemporaryDirectory() as temp_dir:
                path = Path(temp_dir) / f"{kind}.json"
                path.write_text("[]", encoding="utf-8")
                barrier = Path(temp_dir) / "barrier"
                barrier.mkdir()
                outcomes = context.Queue()
                processes = [
                    context.Process(
                        target=_cross_process_writer,
                        args=(kind, str(path), str(barrier), worker_id, outcomes),
                    )
                    for worker_id in ("a", "b")
                ]
                for process in processes:
                    process.start()
                for process in processes:
                    process.join(timeout=15)
                self.assertTrue(all(not process.is_alive() for process in processes))
                self.assertTrue(all(process.exitcode == 0 for process in processes))

                results = [outcomes.get(timeout=2) for _ in processes]
                self.assertEqual([result[1] for result in results].count("success"), 1)
                self.assertEqual(
                    [result[2] for result in results].count("StorageConflictError"),
                    1,
                )
                self.assertEqual(len(json.loads(path.read_text(encoding="utf-8"))), 1)

    def test_json_storage_cas_writers_share_accounts_and_auth_keys_path_lock(self) -> None:
        context = multiprocessing.get_context("spawn")
        for kind in ("accounts", "auth_keys"):
            with self.subTest(kind=kind), TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                accounts_path = root / "accounts.json"
                auth_keys_path = root / "auth_keys.json"
                accounts_path.write_text("[]", encoding="utf-8")
                auth_keys_path.write_text('{"items": []}', encoding="utf-8")
                barrier = root / "barrier"
                barrier.mkdir()
                outcomes = context.Queue()
                processes = [
                    context.Process(
                        target=_json_cas_writer,
                        args=(
                            kind,
                            str(accounts_path),
                            str(auth_keys_path),
                            str(barrier),
                            worker_id,
                            outcomes,
                        ),
                    )
                    for worker_id in ("a", "b")
                ]
                for process in processes:
                    process.start()
                for process in processes:
                    process.join(timeout=15)
                self.assertTrue(all(not process.is_alive() for process in processes))
                self.assertTrue(all(process.exitcode == 0 for process in processes))

                results = [outcomes.get(timeout=2) for _ in processes]
                self.assertEqual([result[1] for result in results].count("success"), 1)
                self.assertEqual(
                    [result[2] for result in results].count("StorageConflictError"),
                    1,
                )
                if kind == "accounts":
                    persisted = json.loads(accounts_path.read_text(encoding="utf-8"))
                else:
                    persisted = json.loads(auth_keys_path.read_text(encoding="utf-8"))["items"]
                self.assertEqual(len(persisted), 1)

    def test_json_direct_save_waits_behind_cas_for_accounts_and_auth_keys(self) -> None:
        context = multiprocessing.get_context("spawn")
        for kind in ("accounts", "auth_keys"):
            with self.subTest(kind=kind), TemporaryDirectory() as temp_dir:
                root = Path(temp_dir)
                accounts_path = root / "accounts.json"
                auth_keys_path = root / "auth_keys.json"
                accounts_path.write_text("[]", encoding="utf-8")
                auth_keys_path.write_text('{"items": []}', encoding="utf-8")
                barrier = root / "barrier"
                barrier.mkdir()
                outcomes = context.Queue()
                cas_process = context.Process(
                    target=_json_cas_writer_with_gate,
                    args=(
                        kind,
                        str(accounts_path),
                        str(auth_keys_path),
                        str(barrier),
                        outcomes,
                    ),
                )
                direct_process = context.Process(
                    target=_json_direct_writer,
                    args=(
                        kind,
                        str(accounts_path),
                        str(auth_keys_path),
                        str(barrier),
                        outcomes,
                    ),
                )
                cas_process.start()
                direct_process.start()
                self._wait_for_marker(barrier, "json-cas-ready-cas")
                self._wait_for_marker(barrier, "json-direct-ready")
                self._wait_for_marker(barrier, "json-cas-entered-save")
                self._wait_for_marker(barrier, "json-direct-attempted")
                self.assertFalse((barrier / "json-direct-done").exists())
                (barrier / "json-release-cas").write_text("release", encoding="ascii")
                cas_process.join(timeout=15)
                direct_process.join(timeout=15)
                self.assertTrue(all(not process.is_alive() for process in (cas_process, direct_process)))
                self.assertTrue(all(process.exitcode == 0 for process in (cas_process, direct_process)))

                results = [outcomes.get(timeout=2) for _ in (cas_process, direct_process)]
                self.assertEqual([result[1] for result in results], ["success", "success"])
                self.assertTrue((barrier / "json-direct-done").exists())
                if kind == "accounts":
                    persisted = json.loads(accounts_path.read_text(encoding="utf-8"))
                else:
                    persisted = json.loads(auth_keys_path.read_text(encoding="utf-8"))["items"]
                self.assertEqual(persisted, [{"id": "direct", "kind": "account" if kind == "accounts" else "auth_key"}])


if __name__ == "__main__":
    unittest.main()
