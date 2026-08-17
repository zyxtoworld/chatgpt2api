from __future__ import annotations

import io
import os
import shutil
import subprocess
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import services.backup_service as backup_module
import services.secure_file as secure_file


class BackupFileBoundaryTests(unittest.TestCase):
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

    @unittest.skipUnless(os.name == "posix", "requires POSIX dir-fd atomic writer")
    def test_atomic_writer_does_not_remove_preexisting_temp_on_name_collision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            target = root / "config.json"
            stale_temp = root / ".config.json.collision.tmp"
            stale_temp.write_bytes(b"stale-sentinel")

            with mock.patch.object(secure_file.secrets, "token_hex", return_value="collision"):
                with self.assertRaises(FileExistsError):
                    secure_file.atomic_write_bytes(target, root, b"new-secret")

            self.assertEqual(stale_temp.read_bytes(), b"stale-sentinel")
            self.assertFalse(target.exists())

    def test_image_backup_rejects_a_rebound_authorized_root(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            foreign = Path(tmp_dir) / "outside"
            relative_path = "2026/08/08/image.png"
            (root / relative_path).parent.mkdir(parents=True)
            (foreign / relative_path).parent.mkdir(parents=True)
            (root / relative_path).write_bytes(b"owner-image")
            (foreign / relative_path).write_bytes(b"private-file-secret")
            try:
                self._replace_directory_with_link(root, foreign)
                output = io.BytesIO()
                with tarfile.open(fileobj=output, mode="w:gz") as archive:
                    backup_module.BackupService()._add_directory_to_archive(archive, root, "data/images")
                self.assertNotIn(b"private-file-secret", output.getvalue())
                with tarfile.open(fileobj=io.BytesIO(output.getvalue()), mode="r:gz") as archive:
                    self.assertEqual(archive.getnames(), [])
            finally:
                self._remove_directory_link(root)

    def test_image_backup_does_not_archive_a_directory_rebound_before_open(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            target = root / "2026/08/08/image.png"
            foreign_directory = Path(tmp_dir) / "outside" / "08"
            target.parent.mkdir(parents=True)
            foreign_directory.mkdir(parents=True)
            target.write_bytes(b"owner-image")
            (foreign_directory / "image.png").write_bytes(b"private-file-secret")
            real_open = secure_file.open_no_follow_file
            opened = False

            def replace_before_open(path: Path, root_path: Path, expected_dir: Path):
                nonlocal opened
                opened = True
                self._replace_directory_with_link(target.parent, foreign_directory)
                return real_open(path, root_path, expected_dir)

            output = io.BytesIO()
            with (
                tarfile.open(fileobj=output, mode="w:gz") as archive,
                mock.patch.object(secure_file, "open_no_follow_file", side_effect=replace_before_open),
            ):
                backup_module.BackupService()._add_directory_to_archive(archive, root, "data/images")

            self.assertTrue(opened)
            self.assertNotIn(b"private-file-secret", output.getvalue())
            with tarfile.open(fileobj=io.BytesIO(output.getvalue()), mode="r:gz") as archive:
                self.assertEqual(archive.getnames(), [])

    def test_single_file_backup_reads_the_handle_before_path_replacement(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir)
            source = root / "config.json"
            replacement = root / "replacement.json"
            source.write_bytes(b"owner-snapshot")
            replacement.write_bytes(b"foreign-snapshot-secret")

            class RaceArchive:
                def __init__(self) -> None:
                    self.payload: bytes | None = None

                def _replace_source(self) -> None:
                    source.unlink()
                    replacement.rename(source)

                def add(self, name: Path, *, arcname: str) -> None:
                    self._replace_source()
                    self.payload = Path(name).read_bytes()

                def addfile(self, info: tarfile.TarInfo, fileobj) -> None:
                    self._replace_source()
                    self.payload = fileobj.read()

            archive = RaceArchive()
            backup_module.BackupService()._add_file_to_archive(archive, source, "config.json")

            self.assertEqual(archive.payload, b"owner-snapshot")
            self.assertEqual(source.read_bytes(), b"foreign-snapshot-secret")


if __name__ == "__main__":
    unittest.main()
