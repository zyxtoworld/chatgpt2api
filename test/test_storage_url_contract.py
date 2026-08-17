from __future__ import annotations

import io
import os
from contextlib import redirect_stdout
from pathlib import Path
import unittest
from unittest import mock

from services.storage.database_storage import DatabaseStorageBackend
import services.storage.factory as storage_factory
from services.storage.factory import _mask_password, create_storage_backend
from services.storage.git_storage import GitStorageBackend


class StorageUrlContractTests(unittest.TestCase):
    def test_database_url_masks_entire_userinfo_with_reserved_at_character(self) -> None:
        raw_url = "postgresql://user:opaque-secret@tail@db.example.test/app"

        for mask in (
            DatabaseStorageBackend._mask_password,
            _mask_password,
            GitStorageBackend._mask_token,
        ):
            with self.subTest(mask=mask):
                redacted = mask(raw_url)
                self.assertNotIn("opaque-secret", redacted)
                self.assertNotIn("tail", redacted)
                self.assertIn("[REDACTED]@db.example.test", redacted)

    def test_database_url_masks_query_and_fragment_without_userinfo(self) -> None:
        canary = "opaque-query-secret"
        raw_url = f"postgresql://db.example.test/app?password={canary}#fragment"

        for mask in (
            DatabaseStorageBackend._mask_password,
            _mask_password,
            GitStorageBackend._mask_token,
        ):
            with self.subTest(mask=mask):
                redacted = mask(raw_url)
                self.assertNotIn(canary, redacted)
                self.assertNotIn("?password=", redacted)
                self.assertNotIn("#fragment", redacted)
                self.assertEqual(redacted, "postgresql://db.example.test/app")

    def test_masks_query_and_fragment_with_userinfo(self) -> None:
        canary = "opaque-userinfo-query-secret"
        raw_url = f"https://user:pass@git.example.test/repo?token={canary}#fragment"

        redacted = GitStorageBackend._mask_token(raw_url)

        self.assertNotIn(canary, redacted)
        self.assertNotIn("?token=", redacted)
        self.assertNotIn("#fragment", redacted)
        self.assertEqual(redacted, "https://[REDACTED]@git.example.test/repo")

    def test_git_factory_log_does_not_print_query_or_fragment_secret(self) -> None:
        canary = "factory-query-token-canary"
        raw_url = f"https://git.example.test/repo?token={canary}#fragment"
        output = io.StringIO()
        env = {
            "STORAGE_BACKEND": "git",
            "GIT_REPO_URL": raw_url,
            "GIT_TOKEN": "separate-token",
            "GIT_BRANCH": "main",
            "GIT_FILE_PATH": "accounts.json",
            "GIT_AUTH_KEYS_FILE_PATH": "auth_keys.json",
        }

        with (
            mock.patch.dict(os.environ, env, clear=False),
            mock.patch.object(storage_factory, "GitStorageBackend", return_value=object()),
            redirect_stdout(output),
        ):
            create_storage_backend(Path("storage-test-data"))

        logged = output.getvalue()
        self.assertNotIn(canary, logged)
        self.assertNotIn("?token=", logged)
        self.assertNotIn("#fragment", logged)
        self.assertIn("https://git.example.test/repo", logged)


if __name__ == "__main__":
    unittest.main()
