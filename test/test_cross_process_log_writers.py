from __future__ import annotations

import json
import multiprocessing
import time
from pathlib import Path
from tempfile import TemporaryDirectory


def _wait_for(path: Path, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            raise TimeoutError(path.name)
        time.sleep(0.005)


def _wait_optional(path: Path, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while not path.exists():
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.005)
    return True


def _wait_until_done(processes, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while any(process.is_alive() for process in processes):
        if time.monotonic() >= deadline:
            raise TimeoutError("log writer process")
        time.sleep(0.01)
    for process in processes:
        assert process.exitcode == 0


def _log_writer(kind: str, path_text: str, barrier_text: str, worker: str) -> None:
    from services import log_service as module

    path = Path(path_text)
    barrier = Path(barrier_text)
    service = module.LogService(path, max_bytes=600, retain_bytes=450)

    if kind == "compact" and worker == "a":
        original_compact = service._compact_for_append_locked

        def gated_compact(incoming_size: int):
            retained = original_compact(incoming_size)
            if retained is None:
                raise AssertionError("compact race did not enter compaction")
            (barrier / "a-ready").write_text("ready", encoding="ascii")
            _wait_for(barrier / "release")
            return retained

        service._compact_for_append_locked = gated_compact
    elif kind == "compact" and worker == "b":
        original_compact = service._compact_for_append_locked

        def mark_compact_entry(incoming_size: int):
            (barrier / "b-entered").write_text("entered", encoding="ascii")
            _wait_for(barrier / "continue-b")
            return original_compact(incoming_size)

        service._compact_for_append_locked = mark_compact_entry
    elif kind == "delete" and worker == "b":
        original_append = module.append_checked_file_bytes

        def gated_append(*args, **kwargs):
            (barrier / "b-entered").write_text("entered", encoding="ascii")
            _wait_for(barrier / "continue-b")
            return original_append(*args, **kwargs)

        module.append_checked_file_bytes = gated_append
    elif kind == "delete" and worker == "a":
        original_replace = service._atomic_replace_bytes_locked

        def gated_replace(payload: bytes) -> None:
            (barrier / "a-ready").write_text("ready", encoding="ascii")
            _wait_for(barrier / "release")
            original_replace(payload)

        service._atomic_replace_bytes_locked = gated_replace

    (barrier / f"start-{worker}").write_text("ready", encoding="ascii")
    _wait_for(barrier / "start-a")
    _wait_for(barrier / "start-b")
    _wait_for(barrier / f"launch-{worker}")
    (barrier / f"began-{worker}").write_text("began", encoding="ascii")
    if kind == "compact":
        service.add("call", f"new-{worker}")
    else:
        if worker == "a":
            service.delete(["delete-me"])
        else:
            service.add("call", "new-b")
    (barrier / f"done-{worker}").write_text("done", encoding="ascii")


def _run_race(kind: str, path: Path, initial: str) -> Path:
    context = multiprocessing.get_context("spawn")
    barrier = path.parent / f"barrier-{kind}"
    barrier.mkdir()
    path.write_text(initial, encoding="utf-8")
    processes = [
        context.Process(
            target=_log_writer,
            args=(kind, str(path), str(barrier), worker),
        )
        for worker in ("a", "b")
    ]
    for process in processes:
        process.start()
    try:
        _wait_for(barrier / "start-a")
        _wait_for(barrier / "start-b")
        (barrier / "launch-a").write_text("launch", encoding="ascii")
        _wait_for(barrier / "began-a")
        _wait_for(barrier / "a-ready")
        (barrier / "launch-b").write_text("launch", encoding="ascii")
        _wait_for(barrier / "began-b")
        if _wait_optional(barrier / "b-entered"):
            (barrier / "continue-b").write_text("continue", encoding="ascii")
            _wait_for(barrier / "done-b")
    finally:
        (barrier / "continue-b").write_text("continue", encoding="ascii")
        (barrier / "release").write_text("release", encoding="ascii")
        _wait_until_done(processes)
    return path


def _initial_compact_log() -> str:
    seed = {
        "id": "seed",
        "time": "2026-08-17 00:00:00",
        "type": "call",
        "summary": "seed" * 100,
    }
    return json.dumps(seed, ensure_ascii=False, separators=(",", ":")) + "\n" + json.dumps(
        {**seed, "id": "seed-2"}, ensure_ascii=False, separators=(",", ":")
    ) + "\n"


def test_cross_process_compact_add_is_serialized() -> None:
    with TemporaryDirectory() as temp_dir:
        path = _run_race("compact", Path(temp_dir) / "logs.jsonl", _initial_compact_log())
        payload = path.read_text(encoding="utf-8")
        assert '"summary":"new-a"' in payload
        assert '"summary":"new-b"' in payload
        assert len(payload.encode("utf-8")) <= 600


def test_cross_process_delete_does_not_drop_concurrent_add() -> None:
    with TemporaryDirectory() as temp_dir:
        initial = json.dumps(
            {
                "id": "delete-me",
                "time": "2026-08-17 00:00:00",
                "type": "call",
                "summary": "to delete",
            },
            separators=(",", ":"),
        ) + "\n"
        path = _run_race("delete", Path(temp_dir) / "logs.jsonl", initial)
        payload = path.read_text(encoding="utf-8")
        assert '"summary":"new-b"' in payload
        assert "delete-me" not in payload
