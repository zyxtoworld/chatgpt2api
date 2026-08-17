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


def _wait_for_any(directory: Path, prefix: str) -> None:
    deadline = time.monotonic() + 10
    while not any((directory / f"{prefix}-{worker}").exists() for worker in ("a", "b")):
        if time.monotonic() >= deadline:
            raise TimeoutError(prefix)
        time.sleep(0.005)


def _tag_writer(path_text: str, barrier_text: str, worker: str, outcomes) -> None:
    import services.image_tags_service as module

    path = Path(path_text)
    barrier = Path(barrier_text)
    module.TAGS_FILE = path
    original_save = module._save_locked

    def gated_save(data, expected_raw):
        (barrier / f"entered-{worker}").write_text("entered", encoding="ascii")
        deadline = time.monotonic() + 10
        while not (barrier / "release").exists():
            if time.monotonic() >= deadline:
                raise TimeoutError("tags writer gate timeout")
            time.sleep(0.005)
        return original_save(data, expected_raw)

    module._save_locked = gated_save
    (barrier / f"ready-{worker}").write_text("ready", encoding="ascii")
    _wait_for_all(barrier, "ready")
    try:
        module.set_tags(f"images/{worker}.png", [f"tag-{worker}"])
    except Exception as exc:
        outcomes.put((worker, "error", type(exc).__name__))
    else:
        outcomes.put((worker, "success", ""))


def test_image_tags_setters_serialize_across_processes() -> None:
    context = multiprocessing.get_context("spawn")
    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        tags_path = root / "image_tags.json"
        tags_path.write_text("{}\n", encoding="utf-8")
        barrier = root / "barrier"
        barrier.mkdir()
        outcomes = context.Queue()
        processes = [
            context.Process(
                target=_tag_writer,
                args=(str(tags_path), str(barrier), worker, outcomes),
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
        results = [outcomes.get(timeout=2) for _ in processes]
        assert [result[1] for result in results].count("success") == 2
        assert [result[2] for result in results].count("") == 2
        persisted = json.loads(tags_path.read_text(encoding="utf-8"))
        assert persisted == {
            "images/a.png": ["tag-a"],
            "images/b.png": ["tag-b"],
        }
