import copy
import json
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from urllib.parse import urlparse

from services.config import DEFAULT_PROXY_RUNTIME
from services.proxy_service import (
    ClearanceBundle,
    FlareSolverrClearanceProvider,
    ProxySettingsStore,
    normalize_proxy_url,
)


class FakeConfig:
    def __init__(self, legacy_proxy: str = "", runtime: dict[str, object] | None = None) -> None:
        self.legacy_proxy = legacy_proxy
        self.runtime = runtime if runtime is not None else copy.deepcopy(DEFAULT_PROXY_RUNTIME)

    def get_proxy_settings(self) -> str:
        return self.legacy_proxy

    def get_proxy_runtime_settings(self) -> dict[str, object]:
        return copy.deepcopy(self.runtime)


def make_runtime(**overrides: object) -> dict[str, object]:
    runtime = copy.deepcopy(DEFAULT_PROXY_RUNTIME)
    clearance = overrides.pop("clearance", None)
    runtime.update(overrides)
    if clearance is not None:
        runtime["clearance"].update(clearance)  # type: ignore[index,union-attr]
    return runtime


class ProxyServiceTests(unittest.TestCase):
    def test_flaresolverr_solution_rejects_container_user_agent_and_cookie_fields(self) -> None:
        canary = "flaresolverr-container-canary"

        def fake_request(endpoint: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
            return json.dumps(
                {
                    "status": "ok",
                    "solution": {
                        "userAgent": {"secret": canary},
                        "cookies": [
                            {"name": {"secret": canary}, "value": "cookie-value"},
                            {"name": "valid", "value": {"secret": canary}},
                        ],
                    },
                }
            ).encode("utf-8")

        provider = FlareSolverrClearanceProvider("http://flare.local", request_method=fake_request)

        self.assertIsNone(provider.get_clearance("https://chatgpt.com"))

    def test_flaresolverr_urllib_response_is_bounded_before_json_parsing(self) -> None:
        observations: dict[str, object] = {"read_sizes": []}

        class Response:
            def __enter__(self) -> "Response":
                return self

            def __exit__(self, *_args: object) -> None:
                observations["closed"] = True

            def read(self, size: int = -1) -> bytes:
                cast_sizes = observations["read_sizes"]
                assert isinstance(cast_sizes, list)
                cast_sizes.append(size)
                return b"x" * (5 if size < 0 else min(size, 5))

        with (
            patch("services.proxy_service.urllib_request.urlopen", return_value=Response()),
            patch("services.proxy_service.FLARESOLVERR_MAX_RESPONSE_BYTES", 4),
        ):
            with self.assertRaises(RuntimeError):
                FlareSolverrClearanceProvider._urllib_post("http://flare.local/v1", b"{}", {}, 5)

        self.assertEqual(observations["read_sizes"], [64 * 1024])
        self.assertTrue(observations["closed"])

    def test_normalize_proxy_url_strips_and_converts_socks_schemes(self) -> None:
        self.assertEqual(normalize_proxy_url("  http://proxy.example:8080  "), "http://proxy.example:8080")
        self.assertEqual(normalize_proxy_url("\thttps://proxy.example:8443\n"), "https://proxy.example:8443")
        self.assertEqual(normalize_proxy_url(" socks://proxy.example:1080 "), "socks5h://proxy.example:1080")
        self.assertEqual(normalize_proxy_url("socks5://proxy.example:1080"), "socks5h://proxy.example:1080")
        self.assertEqual(normalize_proxy_url(" socks5h://proxy.example:1080 "), "socks5h://proxy.example:1080")
        self.assertEqual(normalize_proxy_url("   "), "")

    def test_build_session_kwargs_keeps_legacy_global_proxy_when_runtime_disabled(self) -> None:
        store = ProxySettingsStore(FakeConfig(legacy_proxy="  http://legacy.example:8080  "))

        kwargs = store.build_session_kwargs(impersonate="chrome")

        self.assertEqual(kwargs["impersonate"], "chrome")
        self.assertEqual(kwargs["proxy"], "http://legacy.example:8080")

    def test_runtime_proxy_is_limited_to_upstream_scope_by_default(self) -> None:
        runtime = make_runtime(enabled=True, egress_mode="single_proxy", proxy_url="http://runtime.example:8080")
        store = ProxySettingsStore(FakeConfig(legacy_proxy="http://legacy.example:8080", runtime=runtime))

        self.assertEqual(
            store.build_session_kwargs()["proxy"],
            "http://legacy.example:8080",
        )
        self.assertEqual(
            store.build_session_kwargs(upstream=True)["proxy"],
            "http://runtime.example:8080",
        )

    def test_sensitive_session_can_force_tls_verification(self) -> None:
        runtime = make_runtime(
            enabled=True,
            skip_ssl_verify=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
        )
        store = ProxySettingsStore(FakeConfig(runtime=runtime))

        kwargs = store.build_session_kwargs(require_tls_verification=True)

        self.assertTrue(kwargs["verify"])

    def test_account_proxy_wins_over_runtime_and_global_proxy(self) -> None:
        runtime = make_runtime(enabled=True, egress_mode="single_proxy", proxy_url="http://runtime.example:8080")
        store = ProxySettingsStore(FakeConfig(legacy_proxy="http://legacy.example:8080", runtime=runtime))

        kwargs = store.build_session_kwargs(account={"proxy": " socks://account.example:1080 "}, upstream=True)

        self.assertEqual(kwargs["proxy"], "socks5h://account.example:1080")

    def test_proxy_runtime_single_proxy_wins_over_explicit_and_legacy_proxy_when_enabled(self) -> None:
        runtime = make_runtime(enabled=True, egress_mode="single_proxy", proxy_url=" socks5://runtime.example:1080 ")
        store = ProxySettingsStore(FakeConfig(legacy_proxy="http://legacy.example:8080", runtime=runtime))

        kwargs = store.build_session_kwargs(proxy="http://explicit.example:8080", upstream=True)

        self.assertEqual(kwargs["proxy"], "socks5h://runtime.example:1080")

    def test_explicit_proxy_wins_over_legacy_global_proxy_when_runtime_disabled(self) -> None:
        store = ProxySettingsStore(FakeConfig(legacy_proxy="http://legacy.example:8080"))

        kwargs = store.build_session_kwargs(proxy=" socks5://explicit.example:1080 ")

        self.assertEqual(kwargs["proxy"], "socks5h://explicit.example:1080")

    def test_resource_requests_use_resource_proxy_url_when_configured(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            resource_proxy_url=" socks5://resource.example:1080 ",
        )
        store = ProxySettingsStore(FakeConfig(legacy_proxy="http://legacy.example:8080", runtime=runtime))

        kwargs = store.build_session_kwargs(resource=True, upstream=True)

        self.assertEqual(kwargs["proxy"], "socks5h://resource.example:1080")

    def test_manual_clearance_merges_cookies_and_preserves_explicit_user_agent(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "manual",
                "cf_cookies": "foo=bar; session=ok",
                "cf_clearance": "manual-token",
                "user_agent": "Manual UA",
            },
        )
        store = ProxySettingsStore(FakeConfig(runtime=runtime))

        headers = store.build_headers(headers={"Cookie": "existing=1"}, target_url="https://chatgpt.com/backend-api")

        self.assertEqual(headers["User-Agent"], "Manual UA")
        self.assertEqual(headers["Cookie"], "existing=1; foo=bar; session=ok; cf_clearance=manual-token")

        headers_with_ua = store.build_headers(
            headers={"User-Agent": "Caller UA", "Cookie": "cf_clearance=caller-token"},
            target_url="https://chatgpt.com/backend-api",
        )

        self.assertEqual(headers_with_ua["User-Agent"], "Caller UA")
        self.assertEqual(headers_with_ua["Cookie"], "cf_clearance=caller-token; foo=bar; session=ok")
        self.assertNotIn("cf_clearance=manual-token", headers_with_ua["Cookie"])

    def test_flaresolverr_provider_parses_solution_and_filters_cookies_by_host(self) -> None:
        calls: list[tuple[str, dict[str, object], dict[str, str], float]] = []

        def fake_request(endpoint: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
            payload = json.loads(body.decode("utf-8"))
            calls.append((endpoint, payload, headers, timeout))
            return json.dumps(
                {
                    "status": "ok",
                    "solution": {
                        "userAgent": "Flare UA",
                        "cookies": [
                            {"name": "cf_clearance", "value": "host-token", "domain": ".chatgpt.com"},
                            {"name": "no_domain", "value": "kept"},
                            {"name": "wrong_host", "value": "dropped", "domain": "example.net"},
                        ],
                    },
                }
            ).encode("utf-8")

        provider = FlareSolverrClearanceProvider("http://flare.local/", request_method=fake_request)

        bundle = provider.get_clearance(
            "https://chatgpt.com/backend-api/conversation",
            proxy_url="socks5h://proxy.example:1080",
            timeout_sec=12,
        )

        self.assertIsNotNone(bundle)
        assert bundle is not None
        endpoint, payload, headers, timeout = calls[0]
        self.assertEqual(endpoint, "http://flare.local/v1")
        self.assertEqual(payload["cmd"], "request.get")
        self.assertEqual(payload["url"], "https://chatgpt.com/backend-api/conversation")
        self.assertEqual(payload["maxTimeout"], 12000)
        self.assertEqual(payload["proxy"], {"url": "socks5h://proxy.example:1080"})
        self.assertEqual(headers["Content-Type"], "application/json")
        self.assertEqual(timeout, 12)
        self.assertEqual(bundle.user_agent, "Flare UA")
        self.assertEqual(bundle.cookies, {"cf_clearance": "host-token", "no_domain": "kept"})

    def test_flaresolverr_provider_keeps_only_matching_or_no_domain_cookies(self) -> None:
        def fake_request(endpoint: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
            return json.dumps(
                {
                    "status": "ok",
                    "solution": {
                        "userAgent": "Filtered UA",
                        "cookies": [
                            {"name": "wrong_host", "value": "dropped", "domain": "example.net"},
                            {"name": "also_wrong", "value": "dropped-too", "domain": ".example.org"},
                            {"name": "no_domain", "value": "kept"},
                        ],
                    },
                }
            ).encode("utf-8")

        provider = FlareSolverrClearanceProvider("http://flare.local", request_method=fake_request)

        bundle = provider.get_clearance("https://chatgpt.com", timeout_sec=5)

        self.assertIsNotNone(bundle)
        assert bundle is not None
        self.assertEqual(bundle.cookies, {"no_domain": "kept"})
        self.assertEqual(bundle.user_agent, "Filtered UA")

    def test_flaresolverr_header_values_reject_control_and_cookie_delimiters(self) -> None:
        def fake_request(endpoint: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
            return json.dumps(
                {
                    "status": "ok",
                    "solution": {
                        "userAgent": "Good UA\r\nX-Injected: canary",
                        "cookies": [
                            {"name": "safe", "value": "one; injected=canary"},
                            {"name": "line\nname", "value": "ok"},
                        ],
                    },
                }
            ).encode("utf-8")

        provider = FlareSolverrClearanceProvider("http://flare.local", request_method=fake_request)

        bundle = provider.get_clearance("https://chatgpt.com")

        self.assertIsNone(bundle)

    def test_flaresolverr_provider_drops_all_wrong_domain_cookies(self) -> None:
        def fake_request(endpoint: str, body: bytes, headers: dict[str, str], timeout: float) -> bytes:
            return json.dumps(
                {
                    "status": "ok",
                    "solution": {
                        "cookies": [
                            {"name": "wrong_host", "value": "dropped", "domain": "example.net"},
                        ],
                    },
                }
            ).encode("utf-8")

        provider = FlareSolverrClearanceProvider("http://flare.local", request_method=fake_request)

        self.assertIsNone(provider.get_clearance("https://chatgpt.com", timeout_sec=5))

    def test_cached_flaresolverr_bundle_is_merged_by_build_headers_and_can_be_invalidated(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )
        bundle = ClearanceBundle(
            target_host="chatgpt.com",
            proxy_url="http://runtime.example:8080",
            cookies={"cf_clearance": "flare-token"},
            user_agent="Flare UA",
        )

        class FakeProvider:
            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle | None:
                return bundle

        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: FakeProvider())
        store.refresh_clearance(target_url="https://chatgpt.com", force=True)

        headers = store.build_headers(headers={"Cookie": "existing=1"}, target_url="https://chatgpt.com/backend-api")

        self.assertEqual(headers["User-Agent"], "Flare UA")
        self.assertEqual(headers["Cookie"], "existing=1; cf_clearance=flare-token")

        store.invalidate_clearance(target_url="https://chatgpt.com")
        self.assertEqual(
            store.build_headers(headers={"Cookie": "existing=1"}, target_url="https://chatgpt.com/backend-api"),
            {"Cookie": "existing=1"},
        )

    def test_flaresolverr_refresh_failure_keeps_old_cached_bundle(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )
        first_bundle = ClearanceBundle(
            target_host="chatgpt.com",
            proxy_url="http://runtime.example:8080",
            cookies={"cf_clearance": "old-token"},
            user_agent="Old UA",
        )

        class FakeProvider:
            def __init__(self) -> None:
                self.calls = 0

            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle | None:
                self.calls += 1
                return first_bundle if self.calls == 1 else None

        provider = FakeProvider()
        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: provider)

        refreshed = store.refresh_clearance(target_url="https://chatgpt.com", force=True)
        fallback = store.refresh_clearance(target_url="https://chatgpt.com", force=True)

        self.assertIsNotNone(refreshed)
        assert refreshed is not None
        self.assertEqual(refreshed.cookies, first_bundle.cookies)
        self.assertIsNotNone(refreshed.expires_at)
        assert refreshed.expires_at is not None
        self.assertAlmostEqual(refreshed.expires_at - refreshed.created_at, 3600, places=2)
        self.assertIs(fallback, refreshed)
        self.assertEqual(provider.calls, 2)

    def test_invalidation_during_refresh_cannot_recache_the_stale_bundle(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )
        started = threading.Event()
        release = threading.Event()
        stale_bundle = ClearanceBundle(
            target_host="chatgpt.com",
            proxy_url="http://runtime.example:8080",
            cookies={"cf_clearance": "stale-token"},
            user_agent="Stale UA",
        )

        class SlowProvider:
            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle | None:
                started.set()
                if not release.wait(timeout=5):
                    raise AssertionError("provider was not released")
                return stale_bundle

        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: SlowProvider())
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(store.refresh_clearance, "https://chatgpt.com", force=True)
            self.assertTrue(started.wait(timeout=5))
            store.invalidate_clearance(target_url="https://chatgpt.com")
            release.set()
            self.assertIsNone(future.result(timeout=5))

        self.assertEqual(
            store.build_headers(target_url="https://chatgpt.com/backend-api"),
            {},
        )

    def test_invalidation_during_failed_refresh_does_not_return_old_cached_bundle(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )
        old_bundle = ClearanceBundle(
            target_host="chatgpt.com",
            proxy_url="http://runtime.example:8080",
            cookies={"cf_clearance": "old-token"},
            user_agent="Old UA",
        )
        started = threading.Event()
        release = threading.Event()

        class FailingProvider:
            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> None:
                started.set()
                if not release.wait(timeout=5):
                    raise AssertionError("provider was not released")
                return None

        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: FailingProvider())
        key = store._cache_key("http://runtime.example:8080", "chatgpt.com", runtime["clearance"])
        store._set_cached_bundle(key, old_bundle)

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(store.refresh_clearance, "https://chatgpt.com", force=True)
            self.assertTrue(started.wait(timeout=5))
            store.invalidate_clearance(target_url="https://chatgpt.com")
            release.set()
            self.assertIsNone(future.result(timeout=5))

        key = store._cache_key("http://runtime.example:8080", "chatgpt.com", runtime["clearance"])
        self.assertIsNone(store._get_cached_bundle(key))

    def test_provider_exception_releases_flight_lock_for_next_refresh(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )

        class FlakyProvider:
            def __init__(self) -> None:
                self.calls = 0

            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle:
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("provider failure")
                return ClearanceBundle(
                    target_host="chatgpt.com",
                    proxy_url=proxy_url,
                    cookies={"cf_clearance": "recovered-token"},
                )

        provider = FlakyProvider()
        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: provider)

        with self.assertRaisesRegex(RuntimeError, "provider failure"):
            store.refresh_clearance(target_url="https://chatgpt.com", force=True)
        self.assertEqual(store._flight_locks, {})
        self.assertEqual(store._flight_lock_refs, {})

        recovered = store.refresh_clearance(target_url="https://chatgpt.com", force=True)
        self.assertIsNotNone(recovered)
        self.assertEqual(provider.calls, 2)

    def test_clearance_refreshes_for_different_hosts_do_not_share_flight_lock(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )
        entered = threading.Lock()
        entered_count = 0
        both_entered = threading.Event()
        release = threading.Event()

        class ParallelProvider:
            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle:
                nonlocal entered_count
                with entered:
                    entered_count += 1
                    if entered_count == 2:
                        both_entered.set()
                if not release.wait(timeout=5):
                    raise AssertionError("provider was not released")
                return ClearanceBundle(
                    target_host=urlparse(target_url).hostname or "",
                    proxy_url=proxy_url,
                    cookies={"cf_clearance": urlparse(target_url).hostname or ""},
                )

        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: ParallelProvider())
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(store.refresh_clearance, "https://chatgpt.com", force=True)
            second = executor.submit(store.refresh_clearance, "https://api.example.com", force=True)
            self.assertTrue(both_entered.wait(timeout=2))
            release.set()
            self.assertIsNotNone(first.result(timeout=5))
            self.assertIsNotNone(second.result(timeout=5))

    def test_invalidation_starts_a_new_generation_without_old_result_reappearing(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )
        first_started = threading.Event()
        release_first = threading.Event()
        second_started = threading.Event()

        class GenerationalProvider:
            def __init__(self) -> None:
                self.calls = 0

            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle:
                self.calls += 1
                if self.calls == 1:
                    first_started.set()
                    if not release_first.wait(timeout=5):
                        raise AssertionError("first provider call was not released")
                    return ClearanceBundle(
                        target_host="chatgpt.com",
                        proxy_url=proxy_url,
                        cookies={"cf_clearance": "old-generation"},
                    )
                second_started.set()
                return ClearanceBundle(
                    target_host="chatgpt.com",
                    proxy_url=proxy_url,
                    cookies={"cf_clearance": "new-generation"},
                )

        provider = GenerationalProvider()
        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: provider)
        with ThreadPoolExecutor(max_workers=3) as executor:
            old = executor.submit(store.refresh_clearance, "https://chatgpt.com", force=True)
            self.assertTrue(first_started.wait(timeout=5))
            store.invalidate_clearance(target_url="https://chatgpt.com")
            current = executor.submit(store.refresh_clearance, "https://chatgpt.com", force=True)
            current_waiter = executor.submit(store.refresh_clearance, "https://chatgpt.com", force=True)
            release_first.set()
            self.assertIsNone(old.result(timeout=5))
            self.assertTrue(second_started.wait(timeout=5))
            fresh = current.result(timeout=5)
            fresh_waiter = current_waiter.result(timeout=5)

        self.assertIsNotNone(fresh)
        assert fresh is not None
        self.assertEqual(fresh.cookies["cf_clearance"], "new-generation")
        self.assertIs(fresh_waiter, fresh)
        headers = store.build_headers(target_url="https://chatgpt.com/backend-api")
        self.assertEqual(headers["Cookie"], "cf_clearance=new-generation")
        self.assertEqual(provider.calls, 2)

    def test_refresh_started_before_invalidation_does_not_return_captured_old_bundle(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )
        old_bundle = ClearanceBundle(
            target_host="chatgpt.com",
            proxy_url="http://runtime.example:8080",
            cookies={"cf_clearance": "old-clearance"},
            user_agent="Old UA",
        )
        started = threading.Event()
        release = threading.Event()

        class SlowProvider:
            def __init__(self) -> None:
                self.calls = 0

            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle | None:
                self.calls += 1
                started.set()
                if not release.wait(timeout=5):
                    raise AssertionError("provider was not released")
                return ClearanceBundle(
                    target_host="chatgpt.com",
                    proxy_url=proxy_url,
                    cookies={"cf_clearance": "stale-clearance"},
                    user_agent="Stale UA",
                )

        provider = SlowProvider()
        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: provider)
        store._set_cached_bundle(
            store._cache_key("http://runtime.example:8080", "chatgpt.com", runtime["clearance"]),
            old_bundle,
        )

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(store.refresh_clearance, "https://chatgpt.com", force=True)
            self.assertTrue(started.wait(timeout=5))
            second = executor.submit(store.refresh_clearance, "https://chatgpt.com", force=True)
            store.invalidate_clearance(target_url="https://chatgpt.com")
            release.set()

            self.assertIsNone(first.result(timeout=5))
            self.assertIsNone(second.result(timeout=5))

        self.assertEqual(store.build_headers(target_url="https://chatgpt.com/backend-api"), {})

    def test_clearance_config_change_does_not_reuse_old_cached_bundle(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare-old.local",
                "timeout_sec": 5,
            },
        )

        old_bundle = ClearanceBundle(
            target_host="chatgpt.com",
            proxy_url="http://runtime.example:8080",
            cookies={"cf_clearance": "old-clearance"},
            user_agent="Old UA",
        )
        new_bundle = ClearanceBundle(
            target_host="chatgpt.com",
            proxy_url="http://runtime.example:8080",
            cookies={"cf_clearance": "new-clearance"},
            user_agent="New UA",
        )

        class FakeProvider:
            def __init__(self, bundle: ClearanceBundle) -> None:
                self.bundle = bundle

            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle:
                return self.bundle

        providers = {
            "http://flare-old.local": FakeProvider(old_bundle),
            "http://flare-new.local": FakeProvider(new_bundle),
        }
        store_config = FakeConfig(runtime=runtime)
        store = ProxySettingsStore(store_config, clearance_provider_factory=lambda url: providers[url])

        store.refresh_clearance(target_url="https://chatgpt.com", force=True)
        old_headers = store.build_headers(target_url="https://chatgpt.com/backend-api")
        self.assertEqual(old_headers["Cookie"], "cf_clearance=old-clearance")

        runtime["clearance"]["flaresolverr_url"] = "http://flare-new.local"  # type: ignore[index]
        store.refresh_clearance(target_url="https://chatgpt.com", force=False)
        new_headers = store.build_headers(target_url="https://chatgpt.com/backend-api")

        self.assertEqual(new_headers["Cookie"], "cf_clearance=new-clearance")
        self.assertEqual(new_headers["User-Agent"], "New UA")

    def test_invalidation_during_manual_bundle_build_does_not_recache_bundle(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "manual",
                "cf_cookies": "cf_clearance=manual-token",
                "user_agent": "Manual UA",
            },
        )
        store = ProxySettingsStore(FakeConfig(runtime=runtime))
        started = threading.Event()
        release = threading.Event()
        original_build = store._build_manual_bundle

        def blocked_build(profile: object, target_host: str) -> ClearanceBundle | None:
            started.set()
            if not release.wait(timeout=5):
                raise AssertionError("manual bundle build was not released")
            return original_build(profile, target_host)  # type: ignore[arg-type]

        with patch.object(store, "_build_manual_bundle", side_effect=blocked_build):
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(store.refresh_clearance, "https://chatgpt.com")
                self.assertTrue(started.wait(timeout=5))
                store.invalidate_clearance(target_url="https://chatgpt.com")
                release.set()
                self.assertIsNone(future.result(timeout=5))

        key = store._cache_key("http://runtime.example:8080", "chatgpt.com", runtime["clearance"])
        self.assertIsNone(store._get_cached_bundle(key))

    def test_flaresolverr_bundle_expires_after_refresh_interval(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
                "refresh_interval": 60,
            },
        )

        class FakeProvider:
            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle | None:
                return ClearanceBundle(
                    target_host="chatgpt.com",
                    proxy_url=proxy_url,
                    cookies={"cf_clearance": "expiring-token"},
                    user_agent="Expiring UA",
                    created_at=1000,
                )

        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: FakeProvider())
        with patch("services.proxy_service.time.time", return_value=1000):
            bundle = store.refresh_clearance(target_url="https://chatgpt.com", force=True)

        self.assertIsNotNone(bundle)
        assert bundle is not None
        self.assertEqual(bundle.expires_at, 1060)
        self.assertTrue(bundle.is_valid_for("chatgpt.com", "http://runtime.example:8080", now=1059))
        self.assertFalse(bundle.is_valid_for("chatgpt.com", "http://runtime.example:8080", now=1060))

    def test_profile_repr_does_not_expose_clearance_secrets(self) -> None:
        runtime = make_runtime(
            enabled=True,
            clearance={
                "enabled": True,
                "mode": "manual",
                "cf_cookies": "foo=secret-cookie",
                "cf_clearance": "secret-clearance",
            },
        )
        profile = ProxySettingsStore(FakeConfig(runtime=runtime)).get_profile()

        text = repr(profile)

        self.assertNotIn("secret-cookie", text)
        self.assertNotIn("secret-clearance", text)

    def test_proxy_test_error_redacts_proxy_credentials(self) -> None:
        class FailingSession:
            def __init__(self, **kwargs: object) -> None:
                pass

            def get(self, *args: object, **kwargs: object) -> object:
                raise RuntimeError("proxy failed for http://user:pass@proxy.example:8080")

            def close(self) -> None:
                pass

        with patch("services.proxy_service.Session", FailingSession):
            result = __import__("services.proxy_service", fromlist=["test_proxy"]).test_proxy(
                "http://user:pass@proxy.example:8080"
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "代理测试失败，请稍后重试")
        self.assertNotIn("user:pass", result["error"])

    def test_proxy_test_constructor_failure_returns_safe_error(self) -> None:
        with patch("services.proxy_service.Session", side_effect=RuntimeError("constructor-canary")):
            result = __import__("services.proxy_service", fromlist=["test_proxy"]).test_proxy(
                "http://proxy.example:8080"
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], 0)
        self.assertEqual(result["error"], "代理测试失败，请稍后重试")


    def test_proxy_test_streams_and_closes_response(self) -> None:
        observations: dict[str, object] = {}

        class Response:
            status_code = 204

            def close(self) -> None:
                observations["closed"] = True

        class Session:
            def __init__(self, **kwargs: object) -> None:
                observations["session_kwargs"] = kwargs

            def get(self, *args: object, **kwargs: object) -> Response:
                observations["get_kwargs"] = kwargs
                return Response()

            def close(self) -> None:
                observations["session_closed"] = True

        with patch("services.proxy_service.Session", Session):
            result = __import__("services.proxy_service", fromlist=["test_proxy"]).test_proxy(
                "http://proxy.example:8080"
            )

        self.assertTrue(result["ok"])
        self.assertTrue(observations["get_kwargs"]["stream"])
        self.assertTrue(observations["closed"])
        self.assertTrue(observations["session_closed"])

    def test_proxy_test_session_close_failure_does_not_replace_result(self) -> None:
        class Response:
            status_code = 204

            def close(self) -> None:
                pass

        class Session:
            def __init__(self, **kwargs: object) -> None:
                pass

            def get(self, *args: object, **kwargs: object) -> Response:
                return Response()

            def close(self) -> None:
                raise RuntimeError("close-canary")

        with patch("services.proxy_service.Session", Session):
            result = __import__("services.proxy_service", fromlist=["test_proxy"]).test_proxy(
                "http://proxy.example:8080"
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], 204)

    def test_concurrent_flaresolverr_refresh_uses_single_flight_per_proxy_and_host(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )

        class SlowProvider:
            def __init__(self) -> None:
                self.calls = 0
                self.lock = threading.Lock()

            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle | None:
                with self.lock:
                    self.calls += 1
                time.sleep(0.15)
                return ClearanceBundle(
                    target_host=urlparse(target_url).hostname or "",
                    proxy_url=proxy_url,
                    cookies={"cf_clearance": "thread-token"},
                    user_agent="Thread UA",
                )

        provider = SlowProvider()
        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: provider)
        workers = 6
        start = threading.Barrier(workers)

        def refresh() -> ClearanceBundle | None:
            start.wait(timeout=5)
            return store.refresh_clearance(target_url="https://chatgpt.com", force=True)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            results = list(executor.map(lambda _index: refresh(), range(workers)))

        self.assertEqual(provider.calls, 1)
        self.assertTrue(all(result is results[0] for result in results))
        self.assertIsNotNone(results[0])

    def test_completed_clearance_refresh_releases_flight_lock_entry(self) -> None:
        runtime = make_runtime(
            enabled=True,
            egress_mode="single_proxy",
            proxy_url="http://runtime.example:8080",
            clearance={
                "enabled": True,
                "mode": "flaresolverr",
                "flaresolverr_url": "http://flare.local",
                "timeout_sec": 5,
            },
        )

        class Provider:
            def get_clearance(self, target_url: str, proxy_url: str = "", timeout_sec: int = 60) -> ClearanceBundle:
                return ClearanceBundle(
                    target_host=urlparse(target_url).hostname or "",
                    proxy_url=proxy_url,
                    cookies={"cf_clearance": "token"},
                )

        store = ProxySettingsStore(FakeConfig(runtime=runtime), clearance_provider_factory=lambda _url: Provider())
        store.refresh_clearance(target_url="https://unique-target.example", force=True)

        self.assertEqual(store._flight_locks, {})


if __name__ == "__main__":
    unittest.main()
