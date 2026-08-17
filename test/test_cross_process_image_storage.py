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


class _FakeConfig:
    def __init__(self, images_dir: Path, mode: str) -> None:
        self.images_dir = images_dir
        self._settings = {
            "enabled": mode != "local",
            "mode": mode,
            "webdav_url": "https://dav.example.test",
            "webdav_username": "",
            "webdav_password": "",
            "webdav_root_path": "images",
            "public_base_url": "",
        }

    def cleanup_old_images(self) -> int:
        return 0

    def get_image_storage_settings(self) -> dict[str, object]:
        return dict(self._settings)


def _index_writer(index_text: str, images_text: str, barrier_text: str, worker: str) -> None:
    import services.image_storage_service as module

    index_path = Path(index_text)
    images_dir = Path(images_text)
    barrier = Path(barrier_text)
    module.config = _FakeConfig(images_dir, "local")
    service = module.ImageStorageService(index_path)
    original_load = service._load_clean_index
    load_count = 0

    def gated_load():
        nonlocal load_count
        result = original_load()
        load_count += 1
        if load_count == 2:
            (barrier / f"entered-{worker}").write_text("entered", encoding="ascii")
            _wait_for(barrier / "release")
        return result

    service._load_clean_index = gated_load
    service.make_relative_path = lambda _data, _extension=None: f"2026/08/17/{worker}.png"
    (barrier / f"ready-{worker}").write_text("ready", encoding="ascii")
    _wait_for(barrier / "go-a")
    if worker == "b":
        _wait_for(barrier / "go-b")
    service.save(f"image-{worker}".encode("ascii"))
    (barrier / f"done-{worker}").write_text("done", encoding="ascii")


def _rollback_writer(index_text: str, images_text: str, remote_text: str, barrier_text: str, worker: str) -> None:
    import services.image_storage_service as module

    index_path = Path(index_text)
    images_dir = Path(images_text)
    remote_path = Path(remote_text)
    barrier = Path(barrier_text)
    module.config = _FakeConfig(images_dir, "both")

    class FakeWebDAVClient:
        def __init__(self, _settings) -> None:
            pass

        def remote_exists(self, _rel: str) -> bool:
            if worker == "b" and not (barrier / "serial-mode").exists():
                present_before_wait = remote_path.exists()
                (barrier / "b-remote-checked").write_text("checked", encoding="ascii")
                _wait_for(barrier / "allow-b-put")
                return present_before_wait
            return remote_path.exists()

        def put(self, _rel: str, payload: bytes, content_type: str = "image/png") -> str:
            remote_path.write_bytes(payload)
            (barrier / f"put-{worker}").write_text("put", encoding="ascii")
            if worker == "b":
                _wait_for(barrier / "continue-b-put")
            return "https://dav.example.test/image.png"

        def delete(self, _rel: str) -> bool:
            (barrier / f"delete-{worker}").write_text("delete", encoding="ascii")
            try:
                remote_path.unlink()
            except FileNotFoundError:
                pass
            return True

        def close(self) -> None:
            return None

    module.WebDAVClient = FakeWebDAVClient
    service = module.ImageStorageService(index_path)
    rel = "2026/08/17/shared.png"
    service.make_relative_path = lambda _data, _extension=None: rel
    original_open_local = service.open_local

    def gated_open_local(open_rel: str):
        if worker == "b" and not (barrier / "serial-mode").exists():
            (barrier / "b-local-checked").write_text("checked", encoding="ascii")
            _wait_for(barrier / "allow-b-local")
        return original_open_local(open_rel)

    service.open_local = gated_open_local
    original_save_index = service._save_index

    def gated_save_index(items):
        if worker == "a":
            (barrier / "a-index-ready").write_text("ready", encoding="ascii")
            _wait_for(barrier / "allow-a-index")
            return original_save_index(items)
        (barrier / "b-index-failed").write_text("failed", encoding="ascii")
        raise OSError("injected index failure")

    service._save_index = gated_save_index
    (barrier / f"ready-{worker}").write_text("ready", encoding="ascii")
    if worker == "b":
        _wait_for(barrier / "go-b")
    else:
        _wait_for(barrier / "go-a")
    try:
        service.save(b"same-image-bytes")
    except Exception as exc:
        if worker != "b":
            raise
        (barrier / "b-error").write_text(type(exc).__name__, encoding="ascii")
    else:
        if worker == "b":
            raise AssertionError("injected index failure did not fire")
        (barrier / "a-done").write_text("done", encoding="ascii")


def _join(processes) -> None:
    deadline = time.monotonic() + 20
    while any(process.is_alive() for process in processes):
        if time.monotonic() >= deadline:
            raise TimeoutError("image storage worker")
        time.sleep(0.01)
    assert all(process.exitcode == 0 for process in processes)


def test_cross_process_index_rmw_keeps_different_rels() -> None:
    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        index_path = root / "image_index.json"
        images_dir = root / "images"
        images_dir.mkdir()
        index_path.write_text('{"items": {}}', encoding="utf-8")
        barrier = root / "barrier"
        barrier.mkdir()
        context = multiprocessing.get_context("spawn")
        processes = [
            context.Process(
                target=_index_writer,
                args=(str(index_path), str(images_dir), str(barrier), worker),
            )
            for worker in ("a", "b")
        ]
        for process in processes:
            process.start()
        try:
            _wait_for(barrier / "ready-a")
            _wait_for(barrier / "ready-b")
            (barrier / "go-a").write_text("go", encoding="ascii")
            _wait_for(barrier / "entered-a")
            (barrier / "go-b").write_text("go", encoding="ascii")
            _wait_optional(barrier / "entered-b")
        finally:
            (barrier / "release").write_text("release", encoding="ascii")
            _join(processes)
        items = json.loads(index_path.read_text(encoding="utf-8"))["items"]
        assert set(items) == {"2026/08/17/a.png", "2026/08/17/b.png"}


def test_cross_process_same_rel_rollback_keeps_committed_objects() -> None:
    with TemporaryDirectory() as temp_dir:
        root = Path(temp_dir)
        index_path = root / "image_index.json"
        images_dir = root / "images"
        images_dir.mkdir()
        remote_path = root / "remote-image.bin"
        index_path.write_text('{"items": {}}', encoding="utf-8")
        barrier = root / "barrier"
        barrier.mkdir()
        context = multiprocessing.get_context("spawn")
        processes = [
            context.Process(
                target=_rollback_writer,
                args=(str(index_path), str(images_dir), str(remote_path), str(barrier), worker),
            )
            for worker in ("a", "b")
        ]
        for process in processes:
            process.start()
        try:
            _wait_for(barrier / "ready-a")
            _wait_for(barrier / "ready-b")
            (barrier / "go-b").write_text("go", encoding="ascii")
            if _wait_optional(barrier / "b-local-checked"):
                (barrier / "allow-b-local").write_text("allow", encoding="ascii")
                _wait_for(barrier / "b-remote-checked")
                (barrier / "go-a").write_text("go", encoding="ascii")
                if _wait_optional(barrier / "a-index-ready"):
                    # The old process-local-only implementation permits A to
                    # publish while B is paused after its ownership probe.
                    (barrier / "allow-a-index").write_text("allow", encoding="ascii")
                    (barrier / "allow-b-put").write_text("allow", encoding="ascii")
                    (barrier / "continue-b-put").write_text("continue", encoding="ascii")
                    _wait_for(barrier / "delete-b")
                else:
                    # With the cross-process rel owner, B must finish before
                    # A can enter the same-rel transaction.
                    (barrier / "allow-b-put").write_text("allow", encoding="ascii")
                    (barrier / "continue-b-put").write_text("continue", encoding="ascii")
                    _wait_for(barrier / "b-error")
                    _wait_for(barrier / "a-index-ready")
            else:
                (barrier / "serial-mode").write_text("serial", encoding="ascii")
                (barrier / "go-a").write_text("go", encoding="ascii")
                _wait_for(barrier / "a-index-ready")
        finally:
            (barrier / "allow-b-local").write_text("allow", encoding="ascii")
            (barrier / "allow-b-put").write_text("allow", encoding="ascii")
            (barrier / "continue-b-put").write_text("continue", encoding="ascii")
            (barrier / "allow-a-index").write_text("allow", encoding="ascii")
            _join(processes)
        items = json.loads(index_path.read_text(encoding="utf-8"))["items"]
        local_path = images_dir / "2026/08/17/shared.png"
        assert "2026/08/17/shared.png" in items
        assert local_path.read_bytes() == b"same-image-bytes"
        assert remote_path.read_bytes() == b"same-image-bytes"
