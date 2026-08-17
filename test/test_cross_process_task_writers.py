from __future__ import annotations

import json
import multiprocessing
import time
from pathlib import Path
from tempfile import TemporaryDirectory


def _wait_for_all(directory: Path, prefix: str, workers: tuple[str, ...] = ("a", "b")) -> None:
    deadline = time.monotonic() + 10
    while not all((directory / f"{prefix}-{worker}").exists() for worker in workers):
        if time.monotonic() >= deadline:
            raise TimeoutError(prefix)
        time.sleep(0.005)


def _wait_for_any(directory: Path, prefix: str, workers: tuple[str, ...] = ("a", "b")) -> None:
    deadline = time.monotonic() + 10
    while not any((directory / f"{prefix}-{worker}").exists() for worker in workers):
        if time.monotonic() >= deadline:
            raise TimeoutError(prefix)
        time.sleep(0.005)


def _cross_process_writer(kind: str, path_text: str, barrier_text: str, worker: str, outcomes) -> None:
    path = Path(path_text)
    barrier = Path(barrier_text)
    if kind == "config":
        import services.config as module

        store = module.ConfigStore(path)
        operation = lambda: store.update({"base_url": f"https://{worker}.example"})
        original_atomic_write = module.atomic_write_bytes
    elif kind == "editable":
        import services.editable_file_task_service as module

        class FakeReservation:
            def submit(self, *args, **kwargs) -> None:
                return None

            def cancel(self) -> None:
                return None

        module.reserve_background_task = lambda: FakeReservation()
        service = module.EditableFileTaskService(path)
        operation = lambda: service.submit_ppt(
            {"access_token": f"owner-{worker}"},
            client_task_id=f"task-{worker}",
            prompt="test",
        )
        original_atomic_write = module.atomic_write_bytes
    elif kind == "image":
        import services.image_task_service as module

        class FakeReservation:
            def submit(self, *args, **kwargs) -> None:
                return None

            def cancel(self) -> None:
                return None

        module.reserve_background_task = lambda: FakeReservation()
        service = module.ImageTaskService(path)
        operation = lambda: service.submit_generation(
            {"access_token": f"owner-{worker}"},
            client_task_id=f"task-{worker}",
            prompt="test",
            model="gpt-image-2",
            size="1024x1024",
        )
        original_atomic_write = module.atomic_write_bytes
    else:
        raise AssertionError(kind)

    (barrier / f"ready-{worker}").write_text("ready", encoding="ascii")
    _wait_for_all(barrier, "ready")

    def gated_atomic_write(*args, **kwargs):
        (barrier / f"entered-{worker}").write_text("entered", encoding="ascii")
        deadline = time.monotonic() + 10
        while not (barrier / "release").exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("writer gate timeout")
            time.sleep(0.005)
        return original_atomic_write(*args, **kwargs)

    module.atomic_write_bytes = gated_atomic_write
    try:
        operation()
    except Exception as exc:
        outcomes.put((worker, "error", type(exc).__name__))
    else:
        outcomes.put((worker, "success", ""))


def _run_two_writers(kind: str, path: Path, initial_text: str = "") -> list[tuple[str, str, str]]:
    context = multiprocessing.get_context("spawn")
    root = path.parent
    barrier = root / "barrier"
    barrier.mkdir()
    if initial_text:
        path.write_text(initial_text, encoding="utf-8")
    outcomes = context.Queue()
    processes = [
        context.Process(
            target=_cross_process_writer,
            args=(kind, str(path), str(barrier), worker, outcomes),
        )
        for worker in ("a", "b")
    ]
    for process in processes:
        process.start()
    _wait_for_all(barrier, "ready")
    try:
        _wait_for_any(barrier, "entered")
    finally:
        (barrier / "release").write_text("release", encoding="ascii")
        for process in processes:
            process.join(timeout=15)
    assert all(not process.is_alive() for process in processes)
    assert all(process.exitcode == 0 for process in processes)
    return [outcomes.get(timeout=2) for _ in processes]


def test_config_store_cross_process_writers_have_one_success() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "config.json"
        initial = json.dumps({"auth-key": "test-auth-key", "base_url": "https://initial.example"})
        outcomes = _run_two_writers("config", path, initial)
        assert [result[1] for result in outcomes].count("success") == 1
        assert [result[2] for result in outcomes].count("StorageConflictError") == 1


def test_editable_file_task_cross_process_writers_merge_distinct_tasks() -> None:
    with TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "editable.json"
        outcomes = _run_two_writers("editable", path)
        assert [result[1] for result in outcomes].count("success") == 2
        assert [result[2] for result in outcomes].count("") == 2
        records = json.loads(path.read_text(encoding="utf-8"))["tasks"]
        assert {item["id"] for item in records} == {"task-a", "task-b"}


def test_image_task_cross_process_writers_have_one_success() -> None:
    with TemporaryDirectory() as temp_dir:
        outcomes = _run_two_writers("image", Path(temp_dir) / "image.json")
        assert [result[1] for result in outcomes].count("success") == 1
        assert [result[2] for result in outcomes].count("StorageConflictError") == 1
