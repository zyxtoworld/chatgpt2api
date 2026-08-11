import unittest
from types import SimpleNamespace
from unittest import mock

import api.support as api_support


class ImageBaseUrlApiTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fake_config = SimpleNamespace(base_url="https://public.example.com")
        patcher = mock.patch.object(api_support, "config", self.fake_config)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_prefers_configured_base_url(self) -> None:
        request = SimpleNamespace(
            url=SimpleNamespace(scheme="http", netloc="127.0.0.1:8000"),
            headers={"host": "127.0.0.1:8000"},
        )

        self.assertEqual(api_support.resolve_image_base_url(request), "https://public.example.com")

    def test_falls_back_to_local_request_host(self) -> None:
        self.fake_config.base_url = ""
        request = SimpleNamespace(
            url=SimpleNamespace(scheme="http", netloc="127.0.0.1:8000"),
            headers={"host": "127.0.0.1:9000"},
        )

        self.assertEqual(api_support.resolve_image_base_url(request), "http://127.0.0.1:9000")

    def test_falls_back_to_local_request_netloc_when_host_missing(self) -> None:
        self.fake_config.base_url = ""
        request = SimpleNamespace(
            url=SimpleNamespace(scheme="https", netloc="127.0.0.1:8000"),
            headers={},
        )

        self.assertEqual(api_support.resolve_image_base_url(request), "https://127.0.0.1:8000")

    def test_rejects_untrusted_request_host_when_base_url_is_empty(self) -> None:
        self.fake_config.base_url = ""
        request = SimpleNamespace(
            url=SimpleNamespace(scheme="http", netloc="attacker.example"),
            headers={"host": "attacker.example"},
        )

        self.assertEqual(api_support.resolve_image_base_url(request), "")

    def test_rejects_noncanonical_local_request_authorities(self) -> None:
        self.fake_config.base_url = ""
        for host in ("attacker@127.0.0.1", "[::1", "127.0.0.1:bad", "127.0.0.1:8000/path"):
            with self.subTest(host=host):
                request = SimpleNamespace(
                    url=SimpleNamespace(scheme="http", netloc=host),
                    headers={"host": host},
                )
                self.assertEqual(api_support.resolve_image_base_url(request), "")

    def test_rejects_configured_base_url_with_userinfo(self) -> None:
        self.fake_config.base_url = "https://user:secret@public.example.com"
        request = SimpleNamespace(
            url=SimpleNamespace(scheme="https", netloc="127.0.0.1:8000"),
            headers={"host": "127.0.0.1:8000"},
        )

        self.assertEqual(api_support.resolve_image_base_url(request), "")


if __name__ == "__main__":
    unittest.main()
