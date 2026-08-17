from __future__ import annotations

import json
import errno
import os
import tempfile
import threading
import unittest
from unittest import mock
from pathlib import Path

import services.config as config_module
import services.secure_file as secure_file
from services.config import ConfigStore, _redact_url_credentials, parse_public_url
from services.storage.base import StorageConflictError


class PublicConfigContractTests(unittest.TestCase):
    def test_config_get_does_not_mix_fields_with_a_concurrent_update(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps({"auth-key": "test-auth", "proxy": "http://old.example"}),
                encoding="utf-8",
            )
            store = ConfigStore(path)
            entered_getter = threading.Event()
            release_getter = threading.Event()
            update_published = threading.Event()
            getter_calls = 0
            calls_lock = threading.Lock()
            original_getter = ConfigStore.image_retention_days.fget
            assert original_getter is not None

            def blocked_image_retention(config_store: ConfigStore) -> int:
                nonlocal getter_calls
                with calls_lock:
                    getter_calls += 1
                    call_number = getter_calls
                if call_number == 1:
                    entered_getter.set()
                    if not release_getter.wait(2):
                        raise AssertionError("config getter barrier did not release")
                elif call_number == 2 and not release_getter.is_set():
                    update_published.set()
                return original_getter(config_store)

            result: dict[str, object] = {}

            def read_config() -> None:
                result.update(store.get())

            def update_config() -> None:
                store.update({"proxy": "http://new.example", "image_retention_days": 45})

            with (
                mock.patch.object(
                    ConfigStore,
                    "image_retention_days",
                    new=property(blocked_image_retention),
                ),
                mock.patch.object(store, "_save"),
            ):
                reader = threading.Thread(target=read_config)
                reader.start()
                self.assertTrue(entered_getter.wait(2))
                updater = threading.Thread(target=update_config)
                updater.start()
                update_published.wait(0.2)
                release_getter.set()
                reader.join(2)
                updater.join(2)

            self.assertFalse(reader.is_alive())
            self.assertFalse(updater.is_alive())
            self.assertFalse(update_published.is_set())
            self.assertEqual(result["proxy"], "http://old.example")
            self.assertEqual(result["image_retention_days"], 30)

    def test_numeric_settings_do_not_coerce_bool_float_or_container_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "refresh_account_interval_minute": True,
                        "image_retention_days": 1.5,
                        "image_poll_timeout_secs": {"seconds": 1},
                        "image_poll_interval_secs": True,
                        "image_poll_initial_wait_secs": "nan",
                        "image_account_concurrency": False,
                        "image_settle_secs": {"seconds": 1},
                        "image_timeout_retry_secs": 1.5,
                        "backup": {"interval_minutes": True, "rotation_keep": 1.5},
                        "chat_completion_cache": {
                            "ttl_seconds": {"seconds": 1},
                            "max_entries": False,
                        },
                        "proxy_runtime": {
                            "clearance": {"timeout_sec": 1.5, "refresh_interval": True},
                        },
                    }
                ),
                encoding="utf-8",
            )
            store = ConfigStore(path)
            public_config = store.get()

        self.assertEqual(public_config["refresh_account_interval_minute"], 5)
        self.assertEqual(public_config["image_retention_days"], 30)
        self.assertEqual(public_config["image_poll_timeout_secs"], 120)
        self.assertEqual(public_config["image_poll_interval_secs"], 10.0)
        self.assertEqual(public_config["image_poll_initial_wait_secs"], 10.0)
        self.assertEqual(public_config["image_account_concurrency"], 3)
        self.assertEqual(store.image_settle_secs, 2.0)
        self.assertEqual(public_config["image_timeout_retry_secs"], 30)
        self.assertEqual(public_config["backup"]["interval_minutes"], 360)
        self.assertEqual(public_config["backup"]["rotation_keep"], 10)
        self.assertEqual(public_config["chat_completion_cache"]["ttl_seconds"], 60)
        self.assertEqual(public_config["chat_completion_cache"]["max_entries"], 256)
        self.assertEqual(public_config["proxy_runtime"]["clearance"]["timeout_sec"], 60)
        self.assertEqual(public_config["proxy_runtime"]["clearance"]["refresh_interval"], 3600)

    def test_config_save_rejects_path_replacement_after_snapshot_read(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            replacement = Path(tmp_dir) / "replacement.json"
            path.write_text(
                json.dumps({"auth-key": "test-auth", "marker": "original"}),
                encoding="utf-8",
            )
            replacement.write_text(
                json.dumps({"auth-key": "test-auth", "marker": "replacement"}),
                encoding="utf-8",
            )
            replaced = False
            store = ConfigStore(path)
            original_snapshot = config_module._read_config_snapshot

            def snapshot_with_replacement(
                target: Path,
            ) -> tuple[dict[str, object], str | None, tuple[int, int] | None]:
                nonlocal replaced
                snapshot = original_snapshot(target)
                if target == path and not replaced:
                    os.replace(replacement, path)
                    replaced = True
                return snapshot

            with mock.patch.object(config_module, "_read_config_snapshot", side_effect=snapshot_with_replacement):
                with self.assertRaises(StorageConflictError):
                    store.update({"marker": "updated"})

            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["marker"], "replacement")

    def test_config_save_rejects_parent_directory_rebind_before_atomic_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            root = base / "config-root"
            replacement = base / "replacement-root"
            displaced = base / "displaced-root"
            root.mkdir()
            replacement.mkdir()
            path = root / "config.json"
            replacement_path = replacement / "config.json"
            path.write_text(json.dumps({"auth-key": "test-auth", "marker": "original"}), encoding="utf-8")
            replacement_path.write_text(
                json.dumps({"auth-key": "test-auth", "marker": "foreign"}),
                encoding="utf-8",
            )
            store = ConfigStore(path)
            expected_root_identity = (root.stat().st_dev, root.stat().st_ino)
            original_atomic_write = secure_file.atomic_write_bytes
            rebound = False

            def rebind_before_write(target: Path, owner_root: Path, payload: bytes, **kwargs: object) -> None:
                nonlocal rebound
                os.replace(root, displaced)
                os.replace(replacement, root)
                rebound = True
                self.assertEqual(kwargs.get("expected_root_identity"), expected_root_identity)
                original_atomic_write(target, owner_root, payload, **kwargs)

            with (
                mock.patch.object(config_module, "atomic_write_bytes", side_effect=rebind_before_write),
                mock.patch.dict(os.environ, {"CHATGPT2API_AUTH_KEY": "env-auth"}, clear=False),
            ):
                with self.assertRaises(OSError):
                    store.update({"marker": "updated"})

            if rebound:
                self.assertEqual(json.loads((root / "config.json").read_text(encoding="utf-8"))["marker"], "foreign")
                self.assertEqual(json.loads((displaced / "config.json").read_text(encoding="utf-8"))["marker"], "original")
            else:
                # Windows 持有 sidecar 句柄且禁止 DELETE sharing，父目录 rename
                # 在攻击真正发生前就被内核拒绝，原目录与快照保持不变。
                self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["marker"], "original")

    def test_bind_mount_fallback_rejects_target_rebind_before_in_place_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            foreign = Path(tmp_dir) / "foreign.json"
            displaced = Path(tmp_dir) / "displaced.json"
            path.write_text(json.dumps({"auth-key": "test-auth", "marker": "original"}), encoding="utf-8")
            foreign.write_text(json.dumps({"auth-key": "foreign-auth", "marker": "foreign"}), encoding="utf-8")
            store = ConfigStore(path)
            original_foreign = foreign.read_bytes()
            def rebind_then_ebusy(*args, **kwargs):
                os.replace(path, displaced)
                os.replace(foreign, path)
                error = OSError("bind mount replacement is busy")
                error.errno = errno.EBUSY
                raise error

            with mock.patch.object(config_module, "atomic_write_bytes", side_effect=rebind_then_ebusy):
                with self.assertRaises(StorageConflictError):
                    store.update({"marker": "updated"})

            self.assertEqual(path.read_bytes(), original_foreign)
            self.assertEqual(json.loads(displaced.read_text(encoding="utf-8"))["marker"], "original")

    def test_cleanup_old_images_does_not_delete_non_image_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            root = Path(tmp_dir) / "images"
            root.mkdir()
            protected = root / "internal-state.json"
            protected.write_text("do-not-delete", encoding="utf-8")
            old_time = 1
            os.utime(protected, (old_time, old_time))
            config_file = Path(tmp_dir) / "config.json"
            config_file.write_text(json.dumps({"auth-key": "test-auth"}), encoding="utf-8")
            store = ConfigStore(config_file)
            with (
                mock.patch.object(type(store), "images_dir", new_callable=mock.PropertyMock, return_value=root),
                mock.patch.object(type(store), "image_retention_days", new_callable=mock.PropertyMock, return_value=1),
            ):
                store.cleanup_old_images()
            self.assertEqual(protected.read_text(encoding="utf-8"), "do-not-delete")

    def test_public_config_drops_unknown_persisted_fields(self) -> None:
        secret = "unknown-config-secret owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps({"auth-key": "test-auth", "unknown_secret": secret}),
                encoding="utf-8",
            )
            store = ConfigStore(path)
            public_config = store.get()

        self.assertNotIn(secret, json.dumps(public_config, ensure_ascii=False))
        self.assertNotIn("unknown_secret", public_config)

    def test_public_config_drops_container_values_in_known_text_fields(self) -> None:
        canary = "known-field-container-canary owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "backup": {
                            "account_id": {"secret": canary},
                            "bucket": [canary],
                        },
                        "image_storage": {
                            "webdav_username": {"secret": canary},
                            "webdav_root_path": [canary],
                        },
                    }
                ),
                encoding="utf-8",
            )
            store = ConfigStore(path)
            public_config = store.get()

        serialized = json.dumps(public_config, ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        self.assertEqual(public_config["backup"]["account_id"], "")
        self.assertEqual(public_config["backup"]["bucket"], "")
        self.assertEqual(public_config["image_storage"]["webdav_username"], "")

    def test_public_config_does_not_enable_features_from_container_flags(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "backup": {"enabled": {"enabled": True}},
                        "image_storage": {"enabled": ["true"], "mode": "webdav"},
                    }
                ),
                encoding="utf-8",
            )
            public_config = ConfigStore(path).get()

        self.assertFalse(public_config["backup"]["enabled"])
        self.assertFalse(public_config["image_storage"]["enabled"])
        self.assertEqual(public_config["image_storage"]["mode"], "local")

    def test_public_config_drops_container_values_from_proxy_runtime(self) -> None:
        canary = "proxy-runtime-container-canary owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "proxy_runtime": {
                            "proxy_url": [canary],
                            "clearance": {
                                "user_agent": {"secret": canary},
                                "flaresolverr_url": {"secret": canary},
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            store = ConfigStore(path)
            public_config = store.get()

        serialized = json.dumps(public_config, ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        self.assertEqual(public_config["proxy_runtime"]["proxy_url"], "")
        self.assertNotIn(canary, json.dumps(public_config["proxy_runtime"], ensure_ascii=False))

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

    def test_public_ai_review_is_allowlisted_and_rejects_container_secret(self) -> None:
        canary = "ai-review-internal-canary owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "ai_review": {
                            "enabled": True,
                            "base_url": "https://review.example.test",
                            "api_key": {"secret": canary},
                            "model": "review-model",
                            "prompt": "review prompt",
                            "internal_metadata": {"secret": canary},
                        },
                    }
                ),
                encoding="utf-8",
            )

            public_config = ConfigStore(path).get()

        public_review = public_config["ai_review"]
        self.assertEqual(
            set(public_review),
            {"enabled", "base_url", "api_key", "model", "prompt"},
        )
        self.assertFalse(public_review["api_key"])
        self.assertNotIn(canary, json.dumps(public_config, ensure_ascii=False))

    def test_public_config_rejects_container_scalar_values(self) -> None:
        canary = "scalar-container-canary owner@example.com"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "image_parallel_generation": {"value": canary},
                        "image_settle_enabled": [canary],
                        "sensitive_words": [{"value": canary}],
                        "global_system_prompt": {"value": canary},
                        "default_upstream_model_name": {"value": canary},
                        "default_thinking_effort": {"value": canary},
                    }
                ),
                encoding="utf-8",
            )

            store = ConfigStore(path)
            public_config = store.get()
            store.update(public_config)
            saved = json.loads(path.read_text(encoding="utf-8"))

        serialized = json.dumps(public_config, ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        self.assertEqual(public_config["sensitive_words"], [])
        self.assertEqual(public_config["global_system_prompt"], "")
        self.assertEqual(public_config["default_upstream_model_name"], "gpt-5-5")
        self.assertEqual(public_config["default_thinking_effort"], "auto")

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

    def test_parse_public_url_rejects_malformed_authority_and_percent_escape(self) -> None:
        for value in (
            "https://foo..bar/api",
            "https://-foo.example/api",
            "https://foo-.example/api",
            "https://foo_bar.example/api",
            "https://example.test/%",
            "https://example.test/%2",
        ):
            with self.subTest(value=value):
                self.assertEqual(parse_public_url(value), "")

    def test_parse_public_url_keeps_valid_http_base_url(self) -> None:
        self.assertEqual(
            parse_public_url("https://EXAMPLE.TEST/v1/%E4%B8%AD/"),
            "https://example.test/v1/%E4%B8%AD",
        )

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

    def test_public_base_url_rejects_query_and_fragment_credentials(self) -> None:
        canary = "base-url-query-canary"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "base_url": f"https://api.example.test/v1?token={canary}#fragment",
                    }
                ),
                encoding="utf-8",
            )
            store = ConfigStore(path)

            public_config = store.get()

        self.assertEqual(public_config["base_url"], "")
        self.assertEqual(store.base_url, "")
        self.assertNotIn(canary, json.dumps(public_config, ensure_ascii=False))

    def test_public_config_url_projections_drop_query_and_fragment_without_userinfo(self) -> None:
        canary = "public-url-query-canary"
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "test-auth",
                        "ai_review": {
                            "base_url": f"https://review.example.test/v1?token={canary}#review-fragment",
                        },
                        "image_storage": {
                            "webdav_url": f"https://webdav.example.test/root?token={canary}#webdav-fragment",
                        },
                        "proxy": f"http://proxy.example.test:8080?token={canary}#proxy-fragment",
                        "proxy_runtime": {
                            "proxy_url": f"http://runtime.example.test:8081?token={canary}#runtime-fragment",
                            "resource_proxy_url": f"http://resource.example.test:8082?token={canary}#resource-fragment",
                            "clearance": {
                                "flaresolverr_url": f"http://flare.example.test:8191?token={canary}#flare-fragment",
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            store = ConfigStore(path)
            public_config = store.get()
            store.update(public_config)
            saved = json.loads(path.read_text(encoding="utf-8"))

        serialized = json.dumps(public_config, ensure_ascii=False)
        self.assertNotIn(canary, serialized)
        self.assertNotIn("#review-fragment", serialized)
        self.assertEqual(public_config["ai_review"]["base_url"], "https://review.example.test/v1")
        self.assertEqual(public_config["image_storage"]["webdav_url"], "https://webdav.example.test/root")
        self.assertEqual(public_config["proxy"], "http://proxy.example.test:8080")
        self.assertEqual(public_config["proxy_runtime"]["proxy_url"], "http://runtime.example.test:8081")
        self.assertEqual(
            public_config["proxy_runtime"]["clearance"]["flaresolverr_url"],
            "http://flare.example.test:8191",
        )
        self.assertIn(f"token={canary}", saved["ai_review"]["base_url"])
        self.assertIn(f"token={canary}", saved["image_storage"]["webdav_url"])
        self.assertIn(f"token={canary}", saved["proxy"])
        self.assertIn(f"token={canary}", saved["proxy_runtime"]["proxy_url"])
        self.assertIn(
            f"token={canary}",
            saved["proxy_runtime"]["clearance"]["flaresolverr_url"],
        )


if __name__ == "__main__":
    unittest.main()
