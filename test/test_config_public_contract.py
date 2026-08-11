from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from services.config import ConfigStore, _redact_url_credentials


class PublicConfigContractTests(unittest.TestCase):
    def test_public_config_drops_unknown_persisted_fields(self) -> None:
        secret = "unknown-config-secret owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps({"auth-key": "test-auth", "unknown_secret": secret}),
                encoding="utf-8",
            )
            public_config = ConfigStore(path).get()

        self.assertNotIn(secret, json.dumps(public_config, ensure_ascii=False))
        self.assertNotIn("unknown_secret", public_config)

    def test_public_config_masks_secrets_and_round_trips_masked_updates(self) -> None:
        secrets = {
            "ai_key": "ai-review-secret",
            "webdav_password": "webdav-secret",
            "backup_secret": "backup-secret",
            "backup_passphrase": "backup-passphrase",
            "proxy_password": "proxy-password",
            "runtime_proxy_password": "runtime-proxy-password",
            "resource_proxy_password": "resource-proxy-password",
            "flaresolverr_password": "flaresolverr-password",
        }
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "proxy": f"http://proxy-user:{secrets['proxy_password']}@proxy.example:8080",
                        "ai_review": {"enabled": True, "api_key": secrets["ai_key"]},
                        "image_storage": {
                            "enabled": True,
                            "mode": "webdav",
                            "webdav_url": "https://webdav.example/images",
                            "webdav_password": secrets["webdav_password"],
                        },
                        "backup": {
                            "enabled": True,
                            "secret_access_key": secrets["backup_secret"],
                            "passphrase": secrets["backup_passphrase"],
                        },
                        "proxy_runtime": {
                            "enabled": True,
                            "proxy_url": f"http://runtime-user:{secrets['runtime_proxy_password']}@proxy.example:8081",
                            "resource_proxy_url": f"http://resource-user:{secrets['resource_proxy_password']}@proxy.example:8082",
                            "clearance": {
                                "flaresolverr_url": f"http://flare-user:{secrets['flaresolverr_password']}@flare.example:8191",
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            store = ConfigStore(path)

            public_config = store.get()
            public_json = json.dumps(public_config, ensure_ascii=False)
            for secret in secrets.values():
                self.assertNotIn(secret, public_json)
            self.assertEqual(public_config["ai_review"]["api_key"], "********")
            self.assertEqual(public_config["image_storage"]["webdav_password"], "********")
            self.assertEqual(public_config["backup"]["secret_access_key"], "********")
            self.assertEqual(public_config["backup"]["passphrase"], "********")

            store.update(
                {
                    "ai_review": public_config["ai_review"],
                    "image_storage": public_config["image_storage"],
                    "backup": public_config["backup"],
                    "proxy": public_config["proxy"],
                    "proxy_runtime": public_config["proxy_runtime"],
                }
            )
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(saved["ai_review"]["api_key"], secrets["ai_key"])
            self.assertEqual(saved["image_storage"]["webdav_password"], secrets["webdav_password"])
            self.assertEqual(saved["backup"]["secret_access_key"], secrets["backup_secret"])
            self.assertEqual(saved["backup"]["passphrase"], secrets["backup_passphrase"])
            self.assertIn(secrets["proxy_password"], saved["proxy"])
            self.assertIn(secrets["runtime_proxy_password"], saved["proxy_runtime"]["proxy_url"])
            self.assertIn(secrets["resource_proxy_password"], saved["proxy_runtime"]["resource_proxy_url"])
            self.assertIn(
                secrets["flaresolverr_password"],
                saved["proxy_runtime"]["clearance"]["flaresolverr_url"],
            )

    def test_url_redaction_removes_entire_userinfo_and_fails_closed(self) -> None:
        raw_url = "http://user:p@ss:%2F@proxy.example:8080"
        redacted = _redact_url_credentials(raw_url)

        self.assertNotEqual(redacted, raw_url)
        self.assertNotIn("p@ss:%2F", redacted)
        self.assertIn("[REDACTED]@proxy.example:8080", redacted)
        self.assertEqual(_redact_url_credentials("http://user:p@ss:/%2F@proxy.example:8080"), "")
        self.assertEqual(_redact_url_credentials("http://user:p@ss:/%2F@[::1"), "")

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps({"auth-key": "test-auth", "proxy": raw_url}),
                encoding="utf-8",
            )
            store = ConfigStore(path)
            public_proxy = store.get()["proxy"]
            store.update({"proxy": public_proxy})

            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(saved["proxy"], raw_url)

    def test_invalid_public_image_base_url_is_not_exposed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "image_storage": {
                            "public_base_url": "https://user:secret@cdn.example.test/images?token=1",
                        },
                    }
                ),
                encoding="utf-8",
            )

            public_config = ConfigStore(path).get()

        self.assertEqual(public_config["image_storage"]["public_base_url"], "")


if __name__ == "__main__":
    unittest.main()
