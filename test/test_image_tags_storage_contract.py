from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

import services.image_tags_service as tags_module
from services.storage.base import StorageDataError


def test_corrupt_tags_snapshot_fails_closed(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with pytest.raises(StorageDataError):
        tags_module.load_tags()


def test_tag_mutation_does_not_overwrite_corrupt_snapshot(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    path.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with pytest.raises(StorageDataError):
        tags_module.set_tags("images/a.png", ["favorite"])

    assert path.read_text(encoding="utf-8") == "{broken"


def test_tag_snapshot_replace_failure_keeps_previous_file(tmp_path, monkeypatch) -> None:
    path = tmp_path / "image_tags.json"
    original = "{}\n"
    path.write_text(original, encoding="utf-8")
    monkeypatch.setattr(tags_module, "TAGS_FILE", path)

    with mock.patch.object(Path, "replace", side_effect=OSError("replace failed")):
        with pytest.raises(OSError):
            tags_module.set_tags("images/a.png", ["favorite"])

    assert path.read_text(encoding="utf-8") == original
    assert not path.with_suffix(path.suffix + ".tmp").exists()
