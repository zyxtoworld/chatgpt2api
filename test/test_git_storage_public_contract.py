from __future__ import annotations

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from services.storage.git_storage import GitStorageBackend


class GitStoragePublicContractTests(unittest.TestCase):
    def test_backend_info_masks_entire_repository_userinfo(self) -> None:
        raw_url = "https://user:opaque-git-secret@tail@github.com/example/private.git"

        redacted = GitStorageBackend._mask_token(raw_url)

        self.assertNotIn("opaque-git-secret", redacted)
        self.assertNotIn("tail", redacted)
        self.assertIn("github.com/example/private.git", redacted)

    def test_storage_failure_does_not_print_repository_credentials(self) -> None:
        secret = "opaque-git-token@example.com"
        backend = GitStorageBackend(
            "https://github.com/example/private.git",
            secret,
            local_cache_dir=Path(tempfile.mkdtemp()),
        )
        output = io.StringIO()
        with (
            mock.patch.object(backend, "_clone_or_pull", side_effect=RuntimeError(secret)),
            redirect_stdout(output),
            self.assertRaises(RuntimeError),
        ):
            backend.load_accounts()

        self.assertNotIn(secret, output.getvalue())
        self.assertIn("RuntimeError", output.getvalue())


if __name__ == "__main__":
    unittest.main()
