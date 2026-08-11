from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from fastapi.testclient import TestClient

from api.app import create_app
import api.support as support_module


class WebAssetBoundaryTests(unittest.TestCase):
    @staticmethod
    def _replace_directory_with_link(directory: Path, foreign_directory: Path) -> None:
        shutil.rmtree(directory)
        if os.name == "nt":
            result = subprocess.run(
                ["cmd", "/c", "mklink", "/J", str(directory), str(foreign_directory)],
                capture_output=True,
                text=True,
            )
            if result.returncode:
                raise unittest.SkipTest(f"junction fixture unavailable: {result.stderr or result.stdout}")
        else:
            directory.symlink_to(foreign_directory, target_is_directory=True)

    @staticmethod
    def _remove_directory_link(directory: Path) -> None:
        if directory.is_symlink():
            directory.unlink()
            return
        is_junction = getattr(directory, "is_junction", None)
        if os.name == "nt" and callable(is_junction) and is_junction():
            directory.unlink()

    def test_root_symlink_is_not_redefined_as_trusted_web_dist(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "web_dist"
            foreign = Path(temp_dir) / "outside"
            foreign.mkdir()
            (foreign / "secret.txt").write_text("outside-secret", encoding="utf-8")
            root.mkdir()
            self._replace_directory_with_link(root, foreign)
            try:
                with mock.patch.object(support_module, "WEB_DIST_DIR", root):
                    asset = support_module.open_web_asset("secret.txt")
                    response = TestClient(create_app()).get("/secret.txt")
                self.assertIsNone(asset)
                self.assertEqual(response.status_code, 404)
                self.assertNotIn("outside-secret", response.text)
            finally:
                self._remove_directory_link(root)

    def test_public_route_preserves_static_get_head_and_fallback_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "web_dist"
            root.mkdir()
            (root / "index.html").write_text("<main>home</main>", encoding="utf-8")
            (root / "app.js").write_text("console.log('ok')", encoding="utf-8")
            with mock.patch.object(support_module, "WEB_DIST_DIR", root):
                client = TestClient(create_app())
                get_response = client.get("/app.js")
                head_response = client.head("/app.js")
                fallback_response = client.get("/unknown-route")

            self.assertEqual(get_response.status_code, 200)
            self.assertEqual(get_response.text, "console.log('ok')")
            self.assertEqual(get_response.headers.get("content-type"), "text/javascript; charset=utf-8")
            self.assertNotIn("content-disposition", get_response.headers)
            self.assertEqual(head_response.status_code, 200)
            self.assertEqual(head_response.content, b"")
            self.assertEqual(head_response.headers.get("content-length"), str(len("console.log('ok')")))
            self.assertEqual(fallback_response.status_code, 200)
            self.assertEqual(fallback_response.text, "<main>home</main>")

    def test_nested_link_and_traversal_assets_fail_closed_without_foreign_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir) / "web_dist"
            foreign = Path(temp_dir) / "outside"
            root.mkdir()
            foreign.mkdir()
            (root / "index.html").write_text("<main>home</main>", encoding="utf-8")
            (foreign / "secret.txt").write_text("outside-secret", encoding="utf-8")
            linked = root / "linked"
            linked.mkdir()
            self._replace_directory_with_link(linked, foreign)
            try:
                with mock.patch.object(support_module, "WEB_DIST_DIR", root):
                    self.assertIsNone(support_module.open_web_asset("linked/secret.txt"))
                    client = TestClient(create_app())
                    response = client.get("/linked/secret.txt")

                self.assertEqual(response.status_code, 200)
                self.assertNotIn("outside-secret", response.text)
                self.assertEqual(response.text, "<main>home</main>")
            finally:
                linked.unlink()


if __name__ == "__main__":
    unittest.main()
