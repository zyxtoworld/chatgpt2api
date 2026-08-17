from __future__ import annotations

import io
import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi import HTTPException
from PIL import Image

import services.image_service as image_module
import services.image_storage_service as storage_module
import services.config as config_module
import services.secure_file as secure_file
from services.log_service import LogService
from services.image_storage_service import ImageStorageService


def _png_bytes(color: tuple[int, int, int]) -> bytes:
    payload = io.BytesIO()
    Image.new("RGB", (4, 4), color).save(payload, format="PNG")
    return payload.getvalue()


class ImageFileBoundaryTests(unittest.TestCase):
    def test_log_append_rejects_hardlinked_target(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            victim = root / "victim.jsonl"
            log_path = root / "logs.jsonl"
            victim.write_bytes(b"private-sentinel\n")
            try:
                log_path.hardlink_to(victim)
            except OSError as exc:
                self.skipTest(f"hard links unavailable: {exc}")

            service = LogService(log_path)
            with self.assertRaises(OSError):
                service.add("account", "safe summary")

            self.assertEqual(victim.read_bytes(), b"private-sentinel\n")

    @unittest.skipUnless(os.name == "nt", "requires Windows file handles")
    def test_windows_atomic_write_does_not_delete_unowned_collision_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "image_index.json"
            collision = root / ".image_index.json.fixed-token.tmp"
            target.write_bytes(b"old")
            collision.write_bytes(b"pre-existing")

            with mock.patch.object(secure_file.secrets, "token_hex", return_value="fixed-token"):
                with self.assertRaises(OSError):
                    secure_file.atomic_write_bytes(target, root, b"new")

            self.assertEqual(target.read_bytes(), b"old")
            self.assertEqual(collision.read_bytes(), b"pre-existing")

    @unittest.skipUnless(os.name == "nt", "requires Windows file handles")
    def test_windows_atomic_write_cleans_owned_temp_after_body_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "image_index.json"

            with mock.patch.object(secure_file.secrets, "token_hex", return_value="fixed-token"):
                with self.assertRaises(OSError):
                    secure_file.atomic_write_stream(target, root, [b"partial", "invalid"])

            self.assertFalse((root / ".image_index.json.fixed-token.tmp").exists())

    @unittest.skipUnless(os.name == "nt", "requires Windows file handles")
    def test_windows_atomic_write_rejects_root_rebind_after_identity_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "authorized"
            root.mkdir()
            target = root / "state.json"
            target.write_bytes(b"authorized")
            expected_identity = (root.stat().st_dev, root.stat().st_ino)
            moved = Path(tmp_dir) / "moved-authorized"
            rebound = False
            original_open = secure_file._windows_open_directory_handle

            def rebind_before_open(create_file, path, *args):
                nonlocal rebound
                if not rebound and Path(path) == root:
                    rebound = True
                    root.rename(moved)
                    root.mkdir()
                    (root / "state.json").write_bytes(b"foreign")
                return original_open(create_file, path, *args)

            with mock.patch.object(
                secure_file,
                "_windows_open_directory_handle",
                side_effect=rebind_before_open,
            ):
                with self.assertRaises(OSError):
                    secure_file.atomic_write_bytes(
                        target,
                        root,
                        b"attacker",
                        expected_root_identity=expected_identity,
                    )

            self.assertTrue(rebound)
            self.assertEqual((root / "state.json").read_bytes(), b"foreign")
            self.assertEqual((moved / "state.json").read_bytes(), b"authorized")

    @unittest.skipUnless(os.name == "nt", "requires Windows file handles")
    def test_windows_append_rejects_root_rebind_after_identity_check(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "authorized"
            root.mkdir()
            target = root / "events.log"
            target.write_bytes(b"authorized\n")
            expected_identity = (root.stat().st_dev, root.stat().st_ino)
            moved = Path(tmp_dir) / "moved-authorized"
            rebound = False
            original_open = secure_file._windows_open_directory_handle

            def rebind_before_open(create_file, path, *args):
                nonlocal rebound
                if not rebound and Path(path) == root:
                    rebound = True
                    root.rename(moved)
                    root.mkdir()
                    (root / "events.log").write_bytes(b"foreign\n")
                return original_open(create_file, path, *args)

            with mock.patch.object(
                secure_file,
                "_windows_open_directory_handle",
                side_effect=rebind_before_open,
            ):
                with self.assertRaises(OSError):
                    secure_file.append_checked_file_bytes(
                        target,
                        root,
                        b"attacker\n",
                        expected_root_identity=expected_identity,
                    )

            self.assertTrue(rebound)
            self.assertEqual((root / "events.log").read_bytes(), b"foreign\n")
            self.assertEqual((moved / "events.log").read_bytes(), b"authorized\n")

    def test_posix_atomic_write_calls_replace_when_supports_dir_fd_omits_it(self) -> None:
        replace = mock.Mock()

        with (
            mock.patch.object(secure_file.os, "supports_dir_fd", set()),
            mock.patch.object(secure_file.os, "O_NOFOLLOW", 0, create=True),
            mock.patch.object(secure_file, "_open_posix_directory", return_value=41),
            mock.patch.object(secure_file.os, "open", return_value=42),
            mock.patch.object(secure_file.os, "write", side_effect=lambda _fd, payload: len(payload)),
            mock.patch.object(secure_file.os, "fchmod", return_value=None, create=True),
            mock.patch.object(secure_file.os, "fsync"),
            mock.patch.object(secure_file.os, "close"),
            mock.patch.object(secure_file.os, "replace", replace),
            mock.patch.object(secure_file.os, "unlink", side_effect=FileNotFoundError),
        ):
            secure_file._atomic_write_posix(Path("/root/image_index.json"), Path("/root"), b"payload")

        replace.assert_called_once()
        self.assertEqual(replace.call_args.args[1], "image_index.json")
        self.assertEqual(replace.call_args.kwargs["src_dir_fd"], 41)
        self.assertEqual(replace.call_args.kwargs["dst_dir_fd"], 41)

    @unittest.skipUnless(os.name == "posix", "requires POSIX fd metadata APIs")
    def test_atomic_write_fails_closed_without_fd_mode_restore(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "image_index.json"
            target.write_bytes(b"old")

            with mock.patch.object(secure_file.os, "fchmod", None, create=True):
                with self.assertRaises(OSError):
                    secure_file.atomic_write_bytes(target, root, b"new")

            self.assertEqual(target.read_bytes(), b"old")
            self.assertFalse(list(root.glob(f".{target.name}.*.tmp")))

    @unittest.skipUnless(os.name == "posix", "requires POSIX fd owner APIs")
    def test_atomic_write_fails_closed_without_fd_owner_restore(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "image_index.json"
            target.write_bytes(b"old")
            current = target.stat()

            with mock.patch.object(secure_file.os, "fchown", None, create=True):
                with self.assertRaises(OSError):
                    secure_file.atomic_write_bytes(
                        target,
                        root,
                        b"new",
                        owner=(current.st_uid, current.st_gid),
                    )

            self.assertEqual(target.read_bytes(), b"old")
            self.assertFalse(list(root.glob(f".{target.name}.*.tmp")))

    def test_posix_atomic_write_replace_failure_preserves_target_and_cleans_temp(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "image_index.json"
            original = b'{"items": {}}\n'
            target.write_bytes(original)
            replace = mock.Mock(side_effect=OSError("replace failed"))

            with (
                mock.patch.object(secure_file.os, "O_NOFOLLOW", 0, create=True),
                mock.patch.object(secure_file, "_open_posix_directory", return_value=41),
                mock.patch.object(secure_file.os, "open", return_value=42),
                mock.patch.object(secure_file.os, "write", side_effect=lambda _fd, payload: len(payload)),
                mock.patch.object(secure_file.os, "fchmod", return_value=None, create=True),
                mock.patch.object(secure_file.os, "fsync"),
                mock.patch.object(secure_file.os, "close"),
                mock.patch.object(secure_file.os, "replace", replace),
                mock.patch.object(secure_file.os, "unlink", side_effect=FileNotFoundError) as unlink,
            ):
                with self.assertRaises(OSError):
                    secure_file._atomic_write_posix(target, root, b'{"items": {"new": true}}\n')

            self.assertEqual(target.read_bytes(), original)
            self.assertFalse(list(root.glob(f".{target.name}.*.tmp")))
            replace.assert_called_once()
            unlink.assert_called_once()
            self.assertRegex(unlink.call_args.args[0], rf"\.{target.name}\.[0-9a-f]+\.tmp")

    def _replace_directory_with_link(self, directory: Path, foreign_directory: Path) -> None:
        shutil.rmtree(directory)
        if os.name == "nt":
            result = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(directory), str(foreign_directory)],
                capture_output=True,
                text=True,
            )
            if result.returncode:
                self.skipTest(f"junction fixture unavailable: {result.stderr or result.stdout}")
        else:
            os.symlink(foreign_directory, directory, target_is_directory=True)

    def _remove_directory_link(self, directory: Path) -> None:
        if directory.is_symlink():
            directory.unlink()
            return
        is_junction = getattr(directory, "is_junction", None)
        if os.name == "nt" and callable(is_junction) and is_junction():
            directory.unlink()

    def _root_rebound_fixture(self, tmp_dir: str, payload: bytes = b"private-file-secret") -> tuple[Path, Path, str]:
        root = Path(tmp_dir) / "images"
        foreign = Path(tmp_dir) / "outside"
        relative_path = "2026/08/08/image.png"
        (root / relative_path).parent.mkdir(parents=True)
        (foreign / relative_path).parent.mkdir(parents=True)
        (root / relative_path).write_bytes(b"owner-image")
        (foreign / relative_path).write_bytes(payload)
        self._replace_directory_with_link(root, foreign)
        return root, foreign, relative_path

    def test_open_local_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir)
            try:
                storage = ImageStorageService(Path(tmp_dir) / "index.json")
                with mock.patch("services.image_storage_service.config") as config:
                    config.images_dir = root
                    with self.assertRaises(HTTPException) as error:
                        storage.open_local(relative_path)
                self.assertEqual(error.exception.status_code, 404)
                self.assertEqual((foreign / relative_path).read_bytes(), b"private-file-secret")
            finally:
                self._remove_directory_link(root)

    def test_open_local_rejects_a_rebound_authorized_root_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            authorized_parent = Path(tmp_dir) / "authorized-parent"
            foreign_parent = Path(tmp_dir) / "foreign-parent"
            relative_path = "2026/08/08/image.png"
            authorized_root = authorized_parent / "images"
            foreign_root = foreign_parent / "images"
            (authorized_root / relative_path).parent.mkdir(parents=True)
            (foreign_root / relative_path).parent.mkdir(parents=True)
            (authorized_root / relative_path).write_bytes(b"authorized-image")
            (foreign_root / relative_path).write_bytes(b"foreign-image")
            self._replace_directory_with_link(authorized_parent, foreign_parent)
            try:
                storage = ImageStorageService(Path(tmp_dir) / "index.json")
                with mock.patch("services.image_storage_service.config") as config:
                    config.images_dir = authorized_root
                    with self.assertRaises(HTTPException) as error:
                        storage.open_local(relative_path)
                self.assertEqual(error.exception.status_code, 404)
                self.assertEqual((foreign_root / relative_path).read_bytes(), b"foreign-image")
            finally:
                self._remove_directory_link(authorized_parent)

    def test_authorized_root_rejects_a_rebound_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            authorized_parent = Path(tmp_dir) / "authorized-parent"
            foreign_parent = Path(tmp_dir) / "foreign-parent"
            root = authorized_parent / "images"
            root.mkdir(parents=True)
            foreign_parent.mkdir()
            self._replace_directory_with_link(authorized_parent, foreign_parent)
            try:
                with self.assertRaises(OSError):
                    secure_file.authorized_root(root)
            finally:
                self._remove_directory_link(authorized_parent)

    def test_list_items_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir, _png_bytes((255, 0, 0)))
            try:
                storage = ImageStorageService(Path(tmp_dir) / "index.json")
                with (
                    mock.patch("services.image_storage_service.config") as config,
                    mock.patch.object(storage, "_load_clean_index", return_value={}),
                    mock.patch.object(storage, "_save_index"),
                ):
                    config.images_dir = root
                    self.assertEqual(storage.list_items(""), [])
                self.assertEqual((foreign / relative_path).read_bytes(), _png_bytes((255, 0, 0)))
            finally:
                self._remove_directory_link(root)

    def test_sync_all_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir, _png_bytes((255, 0, 0)))
            try:
                storage = ImageStorageService(Path(tmp_dir) / "index.json")

                class FakeClient:
                    puts = 0

                    def __init__(self, _settings):
                        pass

                    def put(self, *_args, **_kwargs):
                        type(self).puts += 1
                        return "https://dav.example/image.png"

                    def close(self):
                        pass

                with (
                    mock.patch("services.image_storage_service.config") as config,
                    mock.patch("services.image_storage_service.WebDAVClient", FakeClient),
                    mock.patch.object(storage, "_load_clean_index", return_value={}),
                    mock.patch.object(storage, "_save_index"),
                ):
                    config.images_dir = root
                    config.get_image_storage_settings.return_value = {"mode": "both"}
                    self.assertEqual(storage.sync_all(), {"uploaded": 0, "skipped": 0, "failed": 0})
                self.assertEqual(FakeClient.puts, 0)
                self.assertEqual((foreign / relative_path).read_bytes(), _png_bytes((255, 0, 0)))
            finally:
                self._remove_directory_link(root)

    def test_zip_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir)
            try:
                storage = ImageStorageService(Path(tmp_dir) / "index.json")
                with (
                    mock.patch("services.image_service.config") as config,
                    mock.patch("services.image_storage_service.config") as storage_config,
                    mock.patch.object(image_module, "image_storage_service", storage),
                    mock.patch.object(storage, "_load_clean_index", return_value={}),
                ):
                    config.images_dir = root
                    storage_config.images_dir = root
                    with self.assertRaises(HTTPException) as error:
                        image_module.download_images_zip([relative_path])
                self.assertEqual(error.exception.status_code, 404)
                self.assertEqual((foreign / relative_path).read_bytes(), b"private-file-secret")
            finally:
                self._remove_directory_link(root)

    def test_thumbnail_read_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir, _png_bytes((255, 0, 0)))
            thumbnails = Path(tmp_dir) / "thumbnails"
            thumbnails.mkdir()
            try:
                storage = ImageStorageService(Path(tmp_dir) / "index.json")
                with (
                    mock.patch("services.image_service.config") as image_config,
                    mock.patch("services.image_storage_service.config") as storage_config,
                    mock.patch.object(image_module, "image_storage_service", storage),
                    mock.patch.object(storage, "_load_clean_index", return_value={}),
                ):
                    image_config.images_dir = root
                    image_config.image_thumbnails_dir = thumbnails
                    storage_config.images_dir = root
                    with self.assertRaises(HTTPException) as error:
                        image_module.ensure_thumbnail(relative_path)
                self.assertEqual(error.exception.status_code, 404)
                self.assertEqual((foreign / relative_path).read_bytes(), _png_bytes((255, 0, 0)))
            finally:
                self._remove_directory_link(root)

    def test_compress_rejects_a_rebound_authorized_root(self) -> None:
        original = _png_bytes((255, 0, 0)) + b"padding" * 100
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir, original)
            try:
                with mock.patch("services.image_service.config") as config:
                    config.images_dir = root
                    result = image_module.compress_images()
                self.assertEqual(result["compressed"], 0)
                self.assertEqual((foreign / relative_path).read_bytes(), original)
            finally:
                self._remove_directory_link(root)

    def test_delete_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir)
            thumbnails = Path(tmp_dir) / "thumbnails"
            thumbnails.mkdir()
            try:
                with mock.patch("services.image_service.config") as config:
                    config.images_dir = root
                    config.image_thumbnails_dir = thumbnails
                    result = image_module.delete_to_target(10**9)
                self.assertEqual(result["removed"], 0)
                self.assertEqual((foreign / relative_path).read_bytes(), b"private-file-secret")
            finally:
                self._remove_directory_link(root)

    def test_storage_stats_ignores_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir, _png_bytes((255, 0, 0)))
            try:
                storage = ImageStorageService(Path(tmp_dir) / "index.json")
                with (
                    mock.patch("services.image_service.config") as image_config,
                    mock.patch("services.image_storage_service.config") as storage_config,
                    mock.patch.object(image_module, "image_storage_service", storage),
                ):
                    image_config.images_dir = root
                    storage_config.images_dir = root
                    result = image_module.storage_stats()
                self.assertEqual(result["image_count"], 0)
                self.assertEqual((foreign / relative_path).read_bytes(), _png_bytes((255, 0, 0)))
            finally:
                self._remove_directory_link(root)

    def test_old_image_cleanup_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir)
            os.utime(foreign / relative_path, (1.0, 1.0))

            class CleanupOwner:
                images_dir = root
                image_retention_days = 1

            try:
                removed = config_module.ConfigStore.cleanup_old_images(CleanupOwner())
                self.assertEqual(removed, 0)
                self.assertTrue((foreign / relative_path).exists())
            finally:
                self._remove_directory_link(root)

    def test_atomic_write_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir)
            target = foreign / relative_path
            try:
                with self.assertRaises(OSError):
                    secure_file.atomic_write_bytes(target, root, b"new-owner-image")
                self.assertEqual(target.read_bytes(), b"private-file-secret")
            finally:
                self._remove_directory_link(root)

    def test_save_rejects_a_rebound_authorized_root_before_atomic_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root, foreign, relative_path = self._root_rebound_fixture(tmp_dir)
            try:
                storage = ImageStorageService(Path(tmp_dir) / "index.json")
                with (
                    mock.patch("services.image_storage_service.config") as config,
                    mock.patch.object(storage, "make_relative_path", return_value=relative_path),
                ):
                    config.images_dir = root
                    config.cleanup_old_images.return_value = 0
                    config.get_image_storage_settings.return_value = {"mode": "local"}
                    with self.assertRaises(HTTPException) as error:
                        storage.save(b"new-owner-image")
                self.assertEqual(error.exception.status_code, 404)
                self.assertEqual((foreign / relative_path).read_bytes(), b"private-file-secret")
            finally:
                self._remove_directory_link(root)

    def test_thumbnail_write_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            images = Path(tmp_dir) / "images"
            relative_path = "2026/08/08/image.png"
            source = images / relative_path
            source.parent.mkdir(parents=True)
            source.write_bytes(_png_bytes((0, 255, 0)))
            thumbnails = Path(tmp_dir) / "thumbnails"
            foreign = Path(tmp_dir) / "outside-thumbnails"
            thumbnails.mkdir()
            foreign.mkdir()
            try:
                self._replace_directory_with_link(thumbnails, foreign)
                storage = ImageStorageService(Path(tmp_dir) / "index.json")
                with (
                    mock.patch("services.image_service.config") as image_config,
                    mock.patch("services.image_storage_service.config") as storage_config,
                    mock.patch.object(image_module, "image_storage_service", storage),
                    mock.patch.object(storage, "_load_clean_index", return_value={}),
                ):
                    image_config.images_dir = images
                    image_config.image_thumbnails_dir = thumbnails
                    storage_config.images_dir = images
                    with self.assertRaises(HTTPException) as error:
                        image_module.ensure_thumbnail(relative_path)
                self.assertEqual(error.exception.status_code, 404)
                self.assertFalse((foreign / f"{relative_path}.png").exists())
            finally:
                self._remove_directory_link(thumbnails)

    def test_empty_directory_cleanup_does_not_delete_an_external_empty_directory_after_rebind(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            thumbnails = Path(tmp_dir) / "thumbnails"
            parent = thumbnails / "2026" / "08"
            candidate = parent / "08"
            foreign_parent = Path(tmp_dir) / "outside"
            foreign_candidate = foreign_parent / "08"
            candidate.mkdir(parents=True)
            foreign_candidate.mkdir(parents=True)
            rebound = False
            real_rmdir = Path.rmdir

            def rebound_parent(path: Path) -> None:
                nonlocal rebound
                if not rebound and path == candidate:
                    rebound = True
                    self._replace_directory_with_link(parent, foreign_parent)
                return real_rmdir(path)

            try:
                with (
                    mock.patch("services.image_service.config") as config,
                    mock.patch.object(Path, "rmdir", rebound_parent),
                ):
                    config.image_thumbnails_dir = thumbnails
                    image_module.cleanup_image_thumbnails()
                self.assertFalse(rebound)
                self.assertTrue(foreign_candidate.exists())
            finally:
                if parent.is_symlink() or (os.name == "nt" and callable(getattr(parent, "is_junction", None)) and parent.is_junction()):
                    parent.unlink()

    def test_zip_does_not_read_a_directory_rebound_after_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            relative_path = "2026/08/08/image.png"
            target = root / relative_path
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(b"owner-image")
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")

            storage = ImageStorageService(Path(tmp_dir) / "index.json")
            real_open = secure_file.open_no_follow_file
            opened = False

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            with (
                mock.patch("services.image_storage_service.config") as config,
                mock.patch.object(image_module, "image_storage_service", storage),
                mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_before_open),
            ):
                config.images_dir = root
                with self.assertRaises(HTTPException) as error:
                    image_module.download_images_zip([relative_path])

            self.assertTrue(opened)
            self.assertEqual(error.exception.status_code, 404)

    def test_thumbnail_does_not_open_a_source_path_after_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            thumbnails = Path(tmp_dir) / "thumbnails"
            relative_path = "2026/08/08/image.png"
            source = root / relative_path
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            source.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            source.write_bytes(_png_bytes((0, 255, 0)))
            (foreign_directory / "image.png").write_bytes(_png_bytes((255, 0, 0)))
            storage = ImageStorageService(Path(tmp_dir) / "index.json")

            real_open = secure_file.open_no_follow_file
            opened = False

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(source.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            with (
                mock.patch("services.image_storage_service.config") as storage_config,
                mock.patch.object(image_module, "image_storage_service", storage),
                mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_before_open),
            ):
                storage_config.images_dir = root
                storage_config.image_thumbnails_dir = thumbnails
                with self.assertRaises(HTTPException) as error:
                    image_module.ensure_thumbnail(relative_path)

            self.assertTrue(opened)
            self.assertEqual(error.exception.status_code, 404)
            self.assertFalse((thumbnails / relative_path).with_suffix(".png").exists())

    def test_get_bytes_does_not_read_a_path_after_validation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            relative_path = "2026/08/08/image.png"
            target = root / relative_path
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(b"owner-image")
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            service = ImageStorageService(Path(tmp_dir) / "index.json")
            real_open = secure_file.open_no_follow_file
            opened = False

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            with mock.patch("services.image_storage_service.config") as config, mock.patch.object(
                secure_file, "open_no_follow_file", side_effect=replace_before_open
            ):
                config.images_dir = root
                with self.assertRaises(HTTPException) as error:
                    service.get_bytes(relative_path)

            self.assertTrue(opened)
            self.assertEqual(error.exception.status_code, 404)

    def test_save_does_not_write_through_a_rebound_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            relative_path = "2026/08/08/image.png"
            target = root / relative_path
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            storage = ImageStorageService(Path(tmp_dir) / "index.json")
            real_write = storage_module.atomic_write_bytes
            written = False

            def replace_before_write(path: Path, root_path: Path, payload: bytes):
                nonlocal written
                written = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_write(path, root_path, payload)

            with (
                mock.patch("services.image_storage_service.config") as config,
                mock.patch.object(storage, "make_relative_path", return_value=relative_path),
                mock.patch.object(storage_module, "atomic_write_bytes", side_effect=replace_before_write),
            ):
                config.images_dir = root
                config.cleanup_old_images.return_value = 0
                config.get_image_storage_settings.return_value = {"mode": "local"}
                with self.assertRaises(OSError):
                    storage.save(b"owner-image")

            self.assertTrue(written)
            self.assertEqual((foreign_directory / "image.png").read_bytes(), b"private-file-secret")

    def test_thumbnail_does_not_write_through_a_rebound_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            thumbnails = Path(tmp_dir) / "thumbnails"
            relative_path = "2026/08/08/image.png"
            source = root / relative_path
            target = thumbnails / f"{relative_path}.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            source.parent.mkdir(parents=True)
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            source.write_bytes(_png_bytes((0, 255, 0)))
            (foreign_directory / "image.png.png").write_bytes(b"private-file-secret")
            storage = ImageStorageService(Path(tmp_dir) / "index.json")
            real_write = image_module.atomic_write_bytes
            written = False

            def replace_before_write(path: Path, root_path: Path, payload: bytes):
                nonlocal written
                written = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_write(path, root_path, payload)

            with (
                mock.patch("services.image_service.config") as image_config,
                mock.patch("services.image_storage_service.config") as storage_config,
                mock.patch.object(image_module, "image_storage_service", storage),
                mock.patch.object(image_module, "atomic_write_bytes", side_effect=replace_before_write),
            ):
                image_config.image_thumbnails_dir = thumbnails
                storage_config.images_dir = root
                with self.assertRaises(HTTPException) as error:
                    image_module.ensure_thumbnail(relative_path)

            self.assertTrue(written)
            self.assertEqual(error.exception.status_code, 422)
            self.assertEqual((foreign_directory / "image.png.png").read_bytes(), b"private-file-secret")

    def test_list_items_skips_a_directory_rebound_before_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            target = root / "2026/08/08/image.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(_png_bytes((0, 255, 0)))
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            storage = ImageStorageService(Path(tmp_dir) / "index.json")
            real_open = secure_file.open_no_follow_file
            opened = False

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            with mock.patch("services.image_storage_service.config") as config, mock.patch.object(
                secure_file, "open_no_follow_file", side_effect=replace_before_open
            ):
                config.images_dir = root
                self.assertEqual(storage.list_items(""), [])

            self.assertTrue(opened)

    def test_sync_all_skips_a_directory_rebound_before_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            target = root / "2026/08/08/image.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(_png_bytes((0, 255, 0)))
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            storage = ImageStorageService(Path(tmp_dir) / "index.json")
            real_open = secure_file.open_no_follow_file
            opened = False

            class FakeClient:
                puts = 0

                def __init__(self, _settings):
                    pass

                def put(self, *_args, **_kwargs):
                    type(self).puts += 1
                    return "https://dav.example/image.png"

                def close(self):
                    pass

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            with (
                mock.patch("services.image_storage_service.config") as config,
                mock.patch("services.image_storage_service.WebDAVClient", FakeClient),
                mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_before_open),
            ):
                config.images_dir = root
                config.get_image_storage_settings.return_value = {"mode": "both"}
                result = storage.sync_all()

            self.assertTrue(opened)
            self.assertEqual(result["uploaded"], 0)
            self.assertEqual(FakeClient.puts, 0)

    def test_compress_images_skips_a_directory_rebound_before_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            target = root / "2026/08/08/image.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(_png_bytes((0, 255, 0)))
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            real_open = secure_file.open_no_follow_file
            opened = False

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            with (
                mock.patch("services.image_service.config") as config,
                mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_before_open),
            ):
                config.images_dir = root
                result = image_module.compress_images()

            self.assertTrue(opened)
            self.assertEqual(result["compressed"], 0)
            self.assertEqual((foreign_directory / "image.png").read_bytes(), b"private-file-secret")

    def test_compress_images_does_not_write_through_a_rebound_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            target = root / "2026/08/08/image.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(_png_bytes((0, 255, 0)) + b"padding" * 100)
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            real_write = image_module.atomic_write_bytes
            written = False

            def replace_before_write(path: Path, root_path: Path, payload: bytes):
                nonlocal written
                written = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_write(path, root_path, payload)

            with (
                mock.patch("services.image_service.config") as config,
                mock.patch.object(image_module, "atomic_write_bytes", side_effect=replace_before_write),
            ):
                config.images_dir = root
                result = image_module.compress_images()

            self.assertTrue(written)
            self.assertEqual(result["compressed"], 0)
            self.assertEqual((foreign_directory / "image.png").read_bytes(), b"private-file-secret")

    def test_delete_to_target_skips_a_directory_rebound_before_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            target = root / "2026/08/08/image.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(b"owner-image")
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            real_delete = image_module.delete_checked_file
            deleted = False

            def replace_before_delete(path: Path, root_path: Path):
                nonlocal deleted
                deleted = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_delete(path, root_path)

            with (
                mock.patch("services.image_service.config") as config,
                mock.patch.object(image_module, "delete_checked_file", side_effect=replace_before_delete),
            ):
                config.images_dir = root
                config.image_thumbnails_dir = Path(tmp_dir) / "thumbnails"
                result = image_module.delete_to_target(10**9)

            self.assertTrue(deleted)
            self.assertEqual(result["removed"], 0)
            self.assertEqual((foreign_directory / "image.png").read_bytes(), b"private-file-secret")

    def test_cleanup_thumbnail_skips_a_directory_rebound_before_delete(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            thumbnails = Path(tmp_dir) / "thumbnails"
            target = thumbnails / "2026/08/08/image.png.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(b"thumbnail")
            (foreign_directory / "image.png.png").write_bytes(b"private-file-secret")
            storage = ImageStorageService(Path(tmp_dir) / "index.json")
            real_delete = image_module.delete_checked_file
            deleted = False

            def replace_before_delete(path: Path, root_path: Path):
                nonlocal deleted
                deleted = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_delete(path, root_path)

            with (
                mock.patch("services.image_service.config") as image_config,
                mock.patch("services.image_storage_service.config") as storage_config,
                mock.patch.object(image_module, "image_storage_service", storage),
                mock.patch.object(image_module, "delete_checked_file", side_effect=replace_before_delete),
            ):
                image_config.image_thumbnails_dir = thumbnails
                storage_config.images_dir = root
                self.assertEqual(image_module.cleanup_image_thumbnails(), 0)

            self.assertTrue(deleted)
            self.assertEqual((foreign_directory / "image.png.png").read_bytes(), b"private-file-secret")

    def test_storage_stats_ignores_a_directory_rebound_before_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            target = root / "2026/08/08/image.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(_png_bytes((0, 255, 0)))
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            storage = ImageStorageService(Path(tmp_dir) / "index.json")
            real_open = secure_file.open_no_follow_file
            opened = False

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            with (
                mock.patch("services.image_service.config") as image_config,
                mock.patch("services.image_storage_service.config") as storage_config,
                mock.patch.object(image_module, "image_storage_service", storage),
                mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_before_open),
            ):
                image_config.images_dir = root
                storage_config.images_dir = root
                result = image_module.storage_stats()

            self.assertTrue(opened)
            self.assertEqual(result["image_count"], 0)

    def test_old_image_cleanup_does_not_delete_through_a_rebound_parent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            target = root / "2026/08/08/image.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(b"owner-image")
            old_time = 1.0
            os.utime(target, (old_time, old_time))
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            real_open = config_module.open_checked_file
            opened = False

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            class CleanupOwner:
                images_dir = root
                image_retention_days = 1

            with mock.patch.object(config_module, "open_checked_file", side_effect=replace_before_open):
                removed = config_module.ConfigStore.cleanup_old_images(CleanupOwner())

            self.assertTrue(opened)
            self.assertEqual(removed, 0)
            self.assertEqual((foreign_directory / "image.png").read_bytes(), b"private-file-secret")


if __name__ == "__main__":
    unittest.main()
