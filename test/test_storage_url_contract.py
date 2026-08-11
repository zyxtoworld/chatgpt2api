from __future__ import annotations

import unittest

from services.storage.database_storage import DatabaseStorageBackend
from services.storage.factory import _mask_password


class StorageUrlContractTests(unittest.TestCase):
    def test_database_url_masks_entire_userinfo_with_reserved_at_character(self) -> None:
        raw_url = "postgresql://user:opaque-secret@tail@db.example.test/app"

        for mask in (DatabaseStorageBackend._mask_password, _mask_password):
            with self.subTest(mask=mask):
                redacted = mask(raw_url)
                self.assertNotIn("opaque-secret", redacted)
                self.assertNotIn("tail", redacted)
                self.assertIn("[REDACTED]@db.example.test", redacted)


if __name__ == "__main__":
    unittest.main()
