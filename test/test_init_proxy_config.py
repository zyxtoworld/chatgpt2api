import errno
import importlib.util
import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "init_proxy_config.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("init_proxy_config_under_test", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


class InitProxyConfigTests(unittest.TestCase):
    def test_creates_warp_defaults_when_proxy_runtime_missing(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"auth-key": "secret", "proxy": ""}), encoding="utf-8")
            with patch.dict(os.environ, {"CHATGPT2API_CONFIG_FILE": str(path)}, clear=False):
                self.assertEqual(module.main(), 0)

            data = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(data["auth-key"], "secret")
            runtime = data["proxy_runtime"]
            self.assertTrue(runtime["enabled"])
            self.assertEqual(runtime["egress_mode"], "single_proxy")
            self.assertEqual(runtime["proxy_url"], "http://privoxy:8118")
            self.assertTrue(runtime["clearance"]["enabled"])
            self.assertEqual(runtime["clearance"]["mode"], "flaresolverr")
            self.assertEqual(runtime["clearance"]["flaresolverr_url"], "http://flaresolverr:8191")

    def test_existing_custom_runtime_is_not_overwritten(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "secret",
                        "proxy_runtime": {
                            "enabled": False,
                            "egress_mode": "single_proxy",
                            "proxy_url": "http://custom.proxy:8080",
                            "clearance": {
                                "enabled": True,
                                "mode": "manual",
                                "cf_clearance": "manual-token",
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )
            with patch.dict(os.environ, {"CHATGPT2API_CONFIG_FILE": str(path)}, clear=False):
                self.assertEqual(module.main(), 0)

            runtime = json.loads(path.read_text(encoding="utf-8"))["proxy_runtime"]
            self.assertFalse(runtime["enabled"])
            self.assertEqual(runtime["proxy_url"], "http://custom.proxy:8080")
            self.assertEqual(runtime["clearance"]["mode"], "manual")
            self.assertEqual(runtime["clearance"]["cf_clearance"], "manual-token")
            self.assertIn("timeout_sec", runtime["clearance"])
            self.assertIn("reset_session_status_codes", runtime)

    def test_env_can_disable_runtime_defaults_for_warp_compose(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"auth-key": "secret"}), encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "CHATGPT2API_CONFIG_FILE": str(path),
                    "CHATGPT2API_PROXY_RUNTIME_ENABLED": "false",
                    "CHATGPT2API_PROXY_RUNTIME_CLEARANCE_ENABLED": "false",
                },
                clear=False,
            ):
                self.assertEqual(module.main(), 0)

            runtime = json.loads(path.read_text(encoding="utf-8"))["proxy_runtime"]
            self.assertFalse(runtime["enabled"])
            self.assertFalse(runtime["clearance"]["enabled"])
            self.assertEqual(runtime["clearance"]["mode"], "none")

    def test_bind_mounted_config_file_falls_back_when_atomic_replace_is_busy(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"auth-key": "secret", "proxy": ""}), encoding="utf-8")

            def atomic_write_busy(*_args, **_kwargs):
                raise OSError(errno.EBUSY, "Device or resource busy")

            with patch.dict(os.environ, {"CHATGPT2API_CONFIG_FILE": str(path)}, clear=False):
                with patch.object(module, "atomic_write_bytes", atomic_write_busy):
                    self.assertEqual(module.main(), 0)

            data = json.loads(path.read_text(encoding="utf-8"))
            self.assertTrue(data["proxy_runtime"]["enabled"])
            self.assertEqual(list(path.parent.glob(f".{path.name}.*.tmp")), [])

    def test_config_update_uses_shared_authorized_atomic_writer(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"auth-key": "secret", "proxy": ""}), encoding="utf-8")
            calls = []

            def observe_atomic(*args, **kwargs):
                calls.append((args, kwargs))

            with patch.dict(os.environ, {"CHATGPT2API_CONFIG_FILE": str(path)}, clear=False):
                with patch.object(module, "atomic_write_bytes", observe_atomic, create=True):
                    self.assertEqual(module.main(), 0)

            self.assertEqual(len(calls), 1)
            args, kwargs = calls[0]
            self.assertEqual(args[0], path)
            self.assertEqual(args[1], path.parent)
            self.assertEqual(kwargs["mode"], 0o600)
            parent_stat = path.parent.stat()
            self.assertEqual(
                kwargs["expected_root_identity"],
                (parent_stat.st_dev, parent_stat.st_ino),
            )

    def test_bind_mounted_config_write_failure_preserves_previous_snapshot(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            original = json.dumps({"auth-key": "secret", "proxy": ""})
            path.write_text(original, encoding="utf-8")
            original_write_text = Path.write_text

            def atomic_write_busy(*_args, **_kwargs):
                raise OSError(errno.EBUSY, "Device or resource busy")

            def fail_after_partial_write(self: Path, data: str, *args, **kwargs) -> None:
                if self == path:
                    self.write_bytes(data.encode("utf-8")[:7])
                    raise OSError("simulated mounted-file write failure")
                original_write_text(self, data, *args, **kwargs)

            with patch.dict(os.environ, {"CHATGPT2API_CONFIG_FILE": str(path)}, clear=False):
                with (
                    patch.object(module, "atomic_write_bytes", atomic_write_busy),
                    patch.object(Path, "write_text", fail_after_partial_write),
                ):
                    with self.assertRaises(OSError):
                        module.main()

            self.assertEqual(path.read_text(encoding="utf-8"), original)
            self.assertEqual(list(path.parent.glob(f".{path.name}.*.tmp")), [])

    def test_stale_fixed_temp_file_is_not_overwritten_or_removed(self) -> None:
        module = load_script_module()
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"auth-key": "secret", "proxy": ""}), encoding="utf-8")
            stale_temp = path.with_suffix(path.suffix + ".tmp")
            stale_temp.write_text("stale-sentinel", encoding="utf-8")

            with patch.dict(os.environ, {"CHATGPT2API_CONFIG_FILE": str(path)}, clear=False):
                self.assertEqual(module.main(), 0)

            self.assertEqual(stale_temp.read_text(encoding="utf-8"), "stale-sentinel")

    def test_proxy_urls_are_not_written_to_init_logs(self) -> None:
        module = load_script_module()
        canary = "proxy-query-canary-6b8d"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps({"auth-key": "secret"}), encoding="utf-8")
            output = io.StringIO()
            with patch.dict(
                os.environ,
                {
                    "CHATGPT2API_CONFIG_FILE": str(path),
                    "CHATGPT2API_PROXY_RUNTIME_PROXY_URL": f"https://proxy.example.test/?token={canary}",
                },
                clear=False,
            ), redirect_stdout(output):
                self.assertEqual(module.main(), 0)

            self.assertNotIn(canary, output.getvalue())

    def test_proxy_mode_values_are_not_written_verbatim_to_init_logs(self) -> None:
        module = load_script_module()
        canary = "mode-canary-4f2a"
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(
                json.dumps(
                    {
                        "auth-key": "secret",
                        "proxy_runtime": {
                            "enabled": True,
                            "egress_mode": canary,
                            "proxy_url": "http://proxy.example.test:8118",
                            "clearance": {"enabled": True, "mode": canary},
                        },
                    }
                ),
                encoding="utf-8",
            )
            output = io.StringIO()
            with patch.dict(os.environ, {"CHATGPT2API_CONFIG_FILE": str(path)}, clear=False), redirect_stdout(output):
                self.assertEqual(module.main(), 0)

            self.assertNotIn(canary, output.getvalue())


if __name__ == "__main__":
    unittest.main()
