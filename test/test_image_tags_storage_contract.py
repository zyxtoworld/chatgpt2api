from __future__ import annotations

from pathlib import Path
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

import api.system as system_module
import services.image_tags_service as tags_module
import services.secure_file as secure_file
from api.system import create_router
from services.storage.base import StorageDataError


def test_corrupt_tags_snapshot_fails_closed(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with pytest.raises(StorageDataError):
        tags_module.load_tags()


def test_symlinked_tags_snapshot_fails_closed_without_reading_target(tmp_path, monkeypatch) -> None:
    outside = tmp_path / "outside.json"
    outside.write_text('{"images/secret.png":["tags-canary"]}\n', encoding="utf-8")
    path = tmp_path / "image_tags.json"
    try:
        path.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symlink creation unavailable: {exc}")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with pytest.raises(StorageDataError):
        tags_module.load_tags()


def test_tags_read_fails_closed_when_parent_directory_is_rebound_before_open(tmp_path, monkeypatch) -> None:
    original_dir = tmp_path / "data"
    original_dir.mkdir()
    path = original_dir / "image_tags.json"
    path.write_text('{"images/original.png":["original"]}\n', encoding="utf-8")
    moved_dir = tmp_path / "moved-data"
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)
    original_authorized_root = secure_file.authorized_root
    rebound = False

    def rebind_after_root_authorization(root: Path) -> Path:
        nonlocal rebound
        authorized = original_authorized_root(root)
        if not rebound and root == original_dir:
            rebound = True
            original_dir.rename(moved_dir)
            original_dir.mkdir()
            (original_dir / "image_tags.json").write_text(
                '{"images/foreign.png":["tags-canary"]}\n',
                encoding="utf-8",
            )
        return authorized

    with (
        mock.patch.object(secure_file, "authorized_root", side_effect=rebind_after_root_authorization),
        pytest.raises(StorageDataError),
    ):
        tags_module.load_tags()

    assert rebound
    assert "original.png" in (moved_dir / "image_tags.json").read_text(encoding="utf-8")


def test_tag_write_fails_closed_when_parent_directory_is_rebound_before_atomic_write(tmp_path, monkeypatch) -> None:
    original_dir = tmp_path / "data"
    original_dir.mkdir()
    path = original_dir / "image_tags.json"
    path.write_text('{"images/original.png":["original"]}\n', encoding="utf-8")
    moved_dir = tmp_path / "moved-data"
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)
    original_atomic_write = tags_module.atomic_write_bytes
    rebound = False

    def rebind_before_atomic_write(path_obj: Path, root: Path, payload: bytes, **kwargs: object) -> None:
        nonlocal rebound
        rebound = True
        original_dir.rename(moved_dir)
        original_dir.mkdir()
        path.write_text('{"images/foreign.png":["tags-canary"]}\n', encoding="utf-8")
        original_atomic_write(path_obj, root, payload, **kwargs)

        with (
            mock.patch.object(tags_module, "atomic_write_bytes", side_effect=rebind_before_atomic_write),
            pytest.raises(OSError),
        ):
            tags_module.set_tags("images/original.png", ["updated"])

    if rebound:
        assert "tags-canary" in path.read_text(encoding="utf-8")
        assert "original" in (moved_dir / "image_tags.json").read_text(encoding="utf-8")
    else:
        # Windows 的 tags sidecar 句柄禁止父目录删除，重绑定在攻击发生前失败。
        assert "original" in path.read_text(encoding="utf-8")


def test_tag_mutation_does_not_overwrite_corrupt_snapshot(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with pytest.raises(StorageDataError):
        tags_module.set_tags("images/a.png", ["favorite"])

    assert path.read_text(encoding="utf-8") == "{broken"


def test_missing_tag_snapshot_initialization_failure_is_storage_error(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with mock.patch.object(tags_module, "atomic_write_bytes", side_effect=OSError("disk full canary")):
        with pytest.raises(StorageDataError):
            tags_module.load_tags()

    assert not path.exists()


def test_tag_snapshot_replace_failure_keeps_previous_file(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    original = "{}\n"
    path.write_text(original, encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with mock.patch.object(tags_module, "atomic_write_bytes", side_effect=OSError("replace failed")):
        with pytest.raises(OSError):
            tags_module.set_tags("images/a.png", ["favorite"])

    assert path.read_text(encoding="utf-8") == original
    assert not path.with_suffix(path.suffix + ".tmp").exists()


def test_tag_save_does_not_clobber_preexisting_temp_file(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    stale_temp = path.with_suffix(path.suffix + ".tmp")
    stale_temp.write_text("preexisting temp data", encoding="utf-8")
    path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    tags_module.set_tags("images/a.png", ["favorite"])

    assert stale_temp.read_text(encoding="utf-8") == "preexisting temp data"


def test_oversized_persisted_tag_is_rejected_without_rewriting_snapshot(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    original = '{"images/a.png":["' + ("x" * 257) + '"]}\n'
    path.write_text(original, encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with pytest.raises(StorageDataError):
        tags_module.load_tags()

    assert path.read_text(encoding="utf-8") == original


def test_tag_input_limit_is_rejected_before_snapshot_write(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with pytest.raises(ValueError):
        tags_module.set_tags("images/a.png", [f"tag-{index}" for index in range(65)])

    assert path.read_text(encoding="utf-8") == "{}\n"


def test_tag_input_limit_is_rejected_before_cleaning_the_oversized_list() -> None:
    class OversizedTags(list[str]):
        def __iter__(self):
            raise AssertionError("oversized tags must be rejected before iteration")

    tags = OversizedTags(f"tag-{index}" for index in range(65))

    with pytest.raises(ValueError, match="tag limit exceeded"):
        tags_module.set_tags("images/a.png", tags)


def test_tag_input_rejects_noncanonical_image_path_before_snapshot_write(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with mock.patch.object(tags_module, "_save_locked") as save_locked:
        with pytest.raises(ValueError, match="image path limit exceeded"):
            tags_module.set_tags(" images/a.png ", ["favorite"])
    save_locked.assert_not_called()

    assert path.read_text(encoding="utf-8") == "{}\n"


def test_tag_mutation_uses_the_shared_canonical_image_rel(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    tags_module.set_tags("/images/a.png", ["favorite"])

    assert tags_module.get_tags("images/a.png") == ["favorite"]
    assert tags_module.load_tags() == {
        "images/a.png": ["favorite"],
    }


def test_tag_input_limit_is_a_public_bad_request(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)
    app = FastAPI()
    app.include_router(create_router("test"))

    with mock.patch.object(
        system_module,
        "require_admin_async",
        new=mock.AsyncMock(return_value={"role": "admin"}),
    ):
        response = TestClient(app).post(
            "/api/images/tags",
            json={"path": "images/a.png", "tags": [f"tag-{index}" for index in range(65)]},
        )

    assert response.status_code == 400
    assert response.json() == {"detail": {"error": "标签数量或长度超出限制"}}
    assert path.read_text(encoding="utf-8") == "{}\n"
