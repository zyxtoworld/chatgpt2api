from __future__ import annotations

import tempfile
import time
import threading
import unittest
from concurrent.futures import Future, ThreadPoolExecutor
from pathlib import Path
from unittest import mock
from threading import Event

from services.account_service import AccountService
import services.model_service as model_service_module
from services.model_service import ModelCatalogService
from services.storage.json_storage import JSONStorageBackend


def model_list(*model_ids: str) -> dict:
    return {
        "object": "list",
        "data": [
            {
                "id": model_id,
                "object": "model",
                "created": 0,
                "owned_by": "chatgpt",
                "permission": [],
                "root": model_id,
                "parent": None,
            }
            for model_id in model_ids
        ],
    }


class FakeBackend:
    def __init__(self, access_token: str, outcomes: dict[str, object], calls: list[str], closed: list[str]) -> None:
        self.access_token = access_token
        self._outcomes = outcomes
        self._calls = calls
        self._closed = closed

    def list_models(self, **_kwargs) -> dict:
        self._calls.append(self.access_token)
        outcome = self._outcomes[self.access_token]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    def close(self) -> None:
        self._closed.append(self.access_token)


class ModelCatalogServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "accounts.json")
        )
        self.accounts.add_account_items(
            [
                {"access_token": "free-bad", "type": "free", "status": "正常"},
                {"access_token": "free-good", "type": "FREE", "status": "正常"},
                {"access_token": "plus", "type": "Plus", "status": "正常"},
                {"access_token": "pro", "type": "pro", "status": "正常"},
                {"access_token": "team-disabled", "type": "Team", "status": "禁用"},
            ]
        )
        self.accounts.refresh_access_token = lambda token, **_kwargs: token
        self.now = 1000.0
        self.calls: list[str] = []
        self.closed: list[str] = []
        self.outcomes: dict[str, object] = {
            "": model_list("anon", "shared"),
            "free-bad": RuntimeError("expired"),
            "free-good": model_list("free-only", "shared"),
            "plus": model_list("plus-only", "shared"),
            "pro": model_list("pro-only"),
        }
        self.catalog = ModelCatalogService(
            self.accounts,
            backend_factory=lambda access_token="": FakeBackend(
                access_token, self.outcomes, self.calls, self.closed
            ),
            cache_ttl_seconds=300,
            clock=lambda: self.now,
        )

    def test_catalog_uses_account_text_eligibility_predicate(self) -> None:
        with mock.patch.object(self.accounts, "_is_text_account_available", return_value=False):
            result = self.catalog.list_models()

        self.assertEqual([item["id"] for item in result["data"]], ["anon", "shared"])
        self.assertEqual(self.calls, [""])

    def test_catalog_unions_anonymous_and_each_active_account_type(self) -> None:
        result = self.catalog.list_models()

        self.assertEqual(
            [item["id"] for item in result["data"]],
            ["anon", "free-only", "plus-only", "pro-only", "shared"],
        )
        capabilities = {
            item["id"]: (
                item["allow_anonymous"],
                item["supported_account_types"],
            )
            for item in result["data"]
        }
        self.assertEqual(capabilities["anon"], (True, []))
        self.assertEqual(capabilities["free-only"], (False, ["free"]))
        self.assertEqual(capabilities["plus-only"], (False, ["plus"]))
        self.assertEqual(capabilities["pro-only"], (False, ["pro"]))
        self.assertEqual(capabilities["shared"], (True, ["free", "plus"]))
        self.assertCountEqual(self.calls, ["", "free-bad", "free-good", "plus", "pro"])
        self.assertCountEqual(self.closed, self.calls)
        self.assertNotIn("team-disabled", self.calls)

        pro_route = self.catalog.route_for_model("pro-only")
        self.assertEqual(pro_route.access_tokens, frozenset({"pro"}))
        self.assertFalse(pro_route.allow_anonymous)

        shared_route = self.catalog.route_for_model("shared")
        self.assertEqual(shared_route.access_tokens, frozenset({"free-good", "plus"}))
        self.assertTrue(shared_route.allow_anonymous)

    def test_model_map_rejects_container_ids_instead_of_stringifying_them(self) -> None:
        canary = "catalog-container-id-canary"
        result = {
            "object": "list",
            "data": [
                {"id": {"secret": canary}, "owned_by": "chatgpt"},
                {"id": [canary], "owned_by": "chatgpt"},
                {"id": "valid-model", "owned_by": "chatgpt"},
            ],
        }

        models = ModelCatalogService._model_map(result)

        self.assertEqual(set(models), {"valid-model"})
        self.assertNotIn(canary, repr(models))

    def test_catalog_is_cached_until_ttl_expires(self) -> None:
        self.catalog.list_models()
        self.catalog.list_models()
        self.catalog.route_for_model("pro-only")

        self.assertEqual(self.calls.count("pro"), 1)
        self.assertEqual(self.calls.count(""), 1)

    def test_list_models_returns_isolated_nested_catalog_values(self) -> None:
        self.outcomes[""] = {
            "object": "list",
            "data": [
                {
                    "id": "nested-model",
                    "supported_reasoning_efforts": ["low"],
                    "internal_metadata": {"source": "anonymous"},
                },
            ],
        }

        first = self.catalog.list_models()
        first_item = next(item for item in first["data"] if item["id"] == "nested-model")
        first_item["supported_reasoning_efforts"].append("high")
        first_item["internal_metadata"]["source"] = "mutated-by-caller"

        second = self.catalog.list_models()
        second_item = next(item for item in second["data"] if item["id"] == "nested-model")
        self.assertEqual(second_item["supported_reasoning_efforts"], ["low"])
        self.assertEqual(second_item["internal_metadata"], {"source": "anonymous"})

    def test_catalog_refresh_passes_one_absolute_deadline_to_every_upstream_request(self) -> None:
        deadlines: list[float | None] = []
        refresh_deadlines: list[float | None] = []
        io_now = 5000.0

        class DeadlineBackend:
            def __init__(self, access_token: str = "") -> None:
                pass

            def list_models(self, *, deadline: float | None = None, **_kwargs) -> dict:
                deadlines.append(deadline)
                return model_list("deadline-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            self.accounts,
            backend_factory=DeadlineBackend,
            cache_ttl_seconds=300,
            clock=lambda: 1000.0,
            deadline_clock=lambda: io_now,
        )

        self.accounts.refresh_access_token = lambda token, **kwargs: (
            refresh_deadlines.append(kwargs.get("deadline")) or token
        )

        catalog.list_models()

        self.assertEqual(len(deadlines), 5)
        self.assertEqual(deadlines[0], 5090.0)
        self.assertEqual(set(deadlines), {5090.0})
        self.assertEqual(len(refresh_deadlines), 4)
        self.assertEqual(set(refresh_deadlines), {5090.0})

    def test_refresh_admission_failure_cancels_already_submitted_siblings(self) -> None:
        submitted: list[Future] = []

        def submit_refresh(_func, *_args, **_kwargs):
            if len(submitted) >= 3:
                raise model_service_module.ModelCatalogRefreshTimeout(
                    "model catalog refresh timed out"
                )
            future = Future()
            submitted.append(future)
            if len(submitted) == 1:
                future.set_result(model_list("anonymous"))
            return future

        with (
            mock.patch.object(self.catalog, "_submit_refresh_future", side_effect=submit_refresh),
            self.assertRaises(model_service_module.ModelCatalogRefreshTimeout),
        ):
            self.catalog._refresh(
                self.catalog._active_accounts_by_type(),
                {},
                {},
                deadline=5090.0,
            )

        self.assertEqual(len(submitted), 3)
        self.assertTrue(submitted[0].done())
        self.assertTrue(all(future.cancelled() for future in submitted[1:]))

    def test_expired_catalog_refresh_returns_old_snapshot_without_waiting_for_late_account_refresh(self) -> None:
        refresh_started = Event()
        release_refresh = Event()

        def blocked_refresh(token: str, *, deadline: float | None = None, **_kwargs) -> str:
            if token == "pro":
                refresh_started.set()
                release_refresh.wait(timeout=2)
            return token

        catalog = ModelCatalogService(
            self.accounts,
            backend_factory=lambda access_token="": FakeBackend(
                access_token, self.outcomes, self.calls, self.closed
            ),
            cache_ttl_seconds=300,
            clock=lambda: self.now,
            deadline_clock=lambda: self.now,
        )
        catalog.list_models()
        self.outcomes["pro"] = model_list("late-pro-model")
        self.now += 301
        self.accounts.refresh_access_token = blocked_refresh
        try:
            with ThreadPoolExecutor(max_workers=1) as executor:
                request = executor.submit(catalog.list_models)
                self.assertTrue(refresh_started.wait(timeout=1))
                self.now += 91
                result = request.result(timeout=1)

            self.assertIn("pro-only", {item["id"] for item in result["data"]})
            self.assertNotIn("late-pro-model", {item["id"] for item in result["data"]})
            release_refresh.set()
            self.assertIn(
                "pro-only",
                {
                    model_id
                    for model_id in catalog._models_by_access_token["pro"]
                },
            )
        finally:
            release_refresh.set()

    def test_repeated_catalog_timeouts_do_not_accumulate_refresh_pools(self) -> None:
        active = 0
        peak = 0
        state_lock = threading.Lock()
        release_refresh = threading.Event()

        def blocked_models(_access_token: str) -> dict:
            nonlocal active, peak
            with state_lock:
                active += 1
                peak = max(peak, active)
            try:
                release_refresh.wait(timeout=2)
                return model_list("late-model")
            finally:
                with state_lock:
                    active -= 1

        class BlockingBackend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs) -> dict:
                return blocked_models(self.access_token)

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            self.accounts,
            backend_factory=BlockingBackend,
            deadline_clock=time.monotonic,
        )
        try:
            with mock.patch.object(model_service_module, "MODEL_CATALOG_REFRESH_TIMEOUT_SECS", 0.02):
                for _ in range(2):
                    with self.assertRaises(model_service_module.ModelUnavailableError):
                        catalog.list_models()
            self.assertLessEqual(peak, 4)
        finally:
            release_refresh.set()

    def test_concurrent_readers_share_one_catalog_refresh(self) -> None:
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(lambda _index: self.catalog.list_models(), range(8)))

        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(self.calls.count(""), 1)
        self.assertEqual(self.calls.count("free-good"), 1)
        self.assertEqual(self.calls.count("plus"), 1)
        self.assertEqual(self.calls.count("pro"), 1)

    def test_cold_concurrent_readers_wait_for_one_refresh(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "cold-reader-accounts.json")
        )
        refresh_started = Event()
        release_refresh = Event()
        calls: list[int] = []

        class BlockingColdBackend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs) -> dict:
                calls.append(len(calls) + 1)
                refresh_started.set()
                if not release_refresh.wait(timeout=2):
                    raise TimeoutError("test did not release model refresh")
                return model_list("cold-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(accounts, backend_factory=BlockingColdBackend)
        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(catalog.route_for_model, "cold-model")
            self.assertTrue(refresh_started.wait(timeout=1))
            second = executor.submit(catalog.route_for_model, "cold-model")
            self.assertFalse(second.done())
            release_refresh.set()
            first_route = first.result(timeout=2)
            second_route = second.result(timeout=2)

        self.assertTrue(first_route.allow_anonymous)
        self.assertEqual(second_route, first_route)
        self.assertEqual(calls, [1])

    def test_expired_refresh_does_not_block_readers_of_last_successful_snapshot(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "stale-reader-accounts.json")
        )
        refresh_started = Event()
        release_refresh = Event()
        calls: list[int] = []
        now = [0.0]

        class BlockingRefreshBackend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs) -> dict:
                calls.append(len(calls) + 1)
                if len(calls) == 1:
                    return model_list("cached-model")
                refresh_started.set()
                if not release_refresh.wait(timeout=2):
                    raise TimeoutError("test did not release model refresh")
                return model_list("fresh-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=BlockingRefreshBackend,
            cache_ttl_seconds=1,
            clock=lambda: now[0],
        )
        self.assertTrue(catalog.route_for_model("cached-model").allow_anonymous)
        now[0] = 2.0

        with ThreadPoolExecutor(max_workers=2) as executor:
            refresh = executor.submit(catalog.route_for_model, "cached-model")
            self.assertTrue(refresh_started.wait(timeout=1))
            stale_reader = executor.submit(catalog.route_for_model, "cached-model")
            try:
                stale_route = stale_reader.result(timeout=0.2)
            finally:
                release_refresh.set()
            refreshed_route = refresh.result(timeout=2)

        self.assertTrue(stale_route.allow_anonymous)
        self.assertFalse(refreshed_route.allow_anonymous)
        self.assertEqual(calls, [1, 2])

    def test_failed_refresh_keeps_last_successful_models_for_that_type(self) -> None:
        self.catalog.list_models()
        self.outcomes["pro"] = RuntimeError("temporary upstream failure")
        self.now += 301

        result = self.catalog.list_models()

        self.assertIn("pro-only", {item["id"] for item in result["data"]})
        self.assertEqual(
            self.catalog.route_for_model("pro-only").access_tokens,
            frozenset({"pro"}),
        )
        self.assertEqual(self.calls.count("pro"), 2)

    def test_removed_account_type_drops_its_stale_capabilities(self) -> None:
        self.catalog.list_models()
        self.accounts.delete_accounts(["pro"])

        result = self.catalog.list_models()

        self.assertNotIn("pro-only", {item["id"] for item in result["data"]})
        self.assertEqual(
            self.catalog.route_for_model("pro-only").access_tokens,
            frozenset(),
        )

    def test_account_removed_during_failed_refresh_drops_stale_token_capabilities(self) -> None:
        self.catalog.list_models()

        def remove_then_fail(token: str, **_kwargs) -> str:
            self.accounts.delete_accounts([token])
            return token

        self.accounts.refresh_access_token = remove_then_fail
        self.outcomes["pro"] = RuntimeError("account removed during refresh")
        self.now += 301

        self.catalog.list_models()

        self.assertEqual(self.catalog.route_for_model("pro-only").access_tokens, frozenset())

    def test_account_type_changed_during_refresh_does_not_relabel_old_catalog(self) -> None:
        self.accounts.delete_accounts(["free-bad", "free-good", "plus", "pro", "team-disabled"])
        self.accounts.add_account_items([
            {"access_token": "changing", "type": "free", "status": "正常"},
        ])
        self.outcomes["changing"] = model_list("free-only")

        def change_type_during_refresh(token: str, **_kwargs) -> str:
            self.accounts.update_account(token, {"type": "Pro"})
            return token

        self.accounts.refresh_access_token = change_type_during_refresh
        result = self.catalog.list_models()

        self.assertNotIn("free-only", {item["id"] for item in result["data"]})
        with mock.patch.object(self.catalog, "_ensure_catalog", return_value=None):
            self.assertEqual(
                self.catalog.route_for_model("free-only").access_tokens,
                frozenset(),
            )

    def test_removed_account_is_not_routable_while_refresh_is_still_blocked(self) -> None:
        self.catalog.list_models()
        refresh_started = Event()
        release_refresh = Event()

        def blocked_pro_models() -> dict:
            refresh_started.set()
            release_refresh.wait(timeout=2)
            return model_list("pro-only")

        original_factory = self.catalog._backend_factory

        def backend_factory(access_token: str = ""):
            backend = original_factory(access_token=access_token)
            if access_token != "pro":
                return backend

            original_list_models = backend.list_models

            def list_models(**_kwargs) -> dict:
                blocked_pro_models()
                return original_list_models()

            backend.list_models = list_models
            return backend

        self.catalog._backend_factory = backend_factory
        self.now += 301
        with ThreadPoolExecutor(max_workers=1) as executor:
            refresh = executor.submit(self.catalog.list_models)
            self.assertTrue(refresh_started.wait(timeout=1))
            self.accounts.delete_accounts(["pro"])

            route = self.catalog.route_for_model("pro-only")

            release_refresh.set()
            refresh.result(timeout=2)

        self.assertEqual(route.access_tokens, frozenset())

    def test_type_changed_account_is_not_routable_while_refresh_is_blocked(self) -> None:
        self.catalog.list_models()
        self.accounts.update_account("pro", {"type": "free"})
        refresh_started = Event()
        release_refresh = Event()

        original_factory = self.catalog._backend_factory

        def backend_factory(access_token: str = ""):
            backend = original_factory(access_token=access_token)
            if access_token != "pro":
                return backend

            original_list_models = backend.list_models

            def list_models(**_kwargs) -> dict:
                refresh_started.set()
                release_refresh.wait(timeout=2)
                return original_list_models(**_kwargs)

            backend.list_models = list_models
            return backend

        self.catalog._backend_factory = backend_factory
        self.now += 301
        with ThreadPoolExecutor(max_workers=1) as executor:
            refresh = executor.submit(self.catalog.list_models)
            self.assertTrue(refresh_started.wait(timeout=1))

            route = self.catalog.route_for_model("pro-only")

            release_refresh.set()
            refresh.result(timeout=2)

        self.assertEqual(route.access_tokens, frozenset())

    def test_replaced_account_with_same_type_and_count_refreshes_catalog(self) -> None:
        self.catalog.list_models()
        self.accounts.delete_accounts(["pro"])
        self.outcomes["pro-new"] = model_list("pro-new-only")
        self.accounts.add_account_items(
            [{"access_token": "pro-new", "type": "PRO", "status": "正常"}]
        )

        result = self.catalog.list_models()

        self.assertIn("pro-new-only", {item["id"] for item in result["data"]})
        self.assertNotIn("pro-only", {item["id"] for item in result["data"]})
        self.assertEqual(self.calls.count("pro-new"), 1)

    def test_same_type_accounts_keep_distinct_model_capabilities(self) -> None:
        self.accounts.add_account_items([
            {"access_token": "pro-alt", "type": "Pro", "status": "正常"},
        ])
        self.outcomes["pro-alt"] = model_list("pro-alt-only")

        result = self.catalog.list_models()

        self.assertIn("pro-alt-only", {item["id"] for item in result["data"]})
        self.assertEqual(
            self.catalog.route_for_model("pro-alt-only").access_tokens,
            frozenset({"pro-alt"}),
        )

    def test_rate_limited_account_does_not_publish_its_model_catalog(self) -> None:
        self.accounts.add_account_items([
            {"access_token": "limited-pro", "type": "Pro", "status": "限流"},
        ])
        self.outcomes["limited-pro"] = model_list("limited-only")

        result = self.catalog.list_models()

        self.assertNotIn("limited-only", {item["id"] for item in result["data"]})
        self.assertNotIn("limited-pro", self.calls)

    def test_same_type_model_routes_select_the_owner_token_and_reasoning_catalog(self) -> None:
        self.accounts.add_account_items([
            {"access_token": "pro-alt", "type": "Pro", "status": "正常"},
        ])
        self.outcomes["pro"] = {
            "object": "list",
            "data": [
                {"id": "model-a", "supported_reasoning_efforts": ["minimal"]},
                {"id": "model-shared", "supported_reasoning_efforts": ["minimal"]},
            ],
        }
        self.outcomes["pro-alt"] = {
            "object": "list",
            "data": [
                {"id": "model-b", "supported_reasoning_efforts": ["high"]},
                {"id": "model-shared", "supported_reasoning_efforts": ["high"]},
            ],
        }

        with mock.patch("services.model_service.model_catalog_service", self.catalog):
            self.assertEqual(self.accounts.get_text_access_token(model="model-a"), "pro")
            self.assertEqual(self.accounts.get_text_access_token(model="model-b"), "pro-alt")
            self.assertEqual(
                self.catalog.supported_reasoning_efforts("model-a", access_token="pro"),
                ("minimal",),
            )
            self.assertIsNone(
                self.catalog.supported_reasoning_efforts("model-a", access_token="pro-alt"),
            )
            self.assertEqual(
                self.catalog.supported_reasoning_efforts("model-shared", access_token="pro"),
                ("minimal",),
            )
            self.assertEqual(
                self.catalog.supported_reasoning_efforts("model-shared", access_token="pro-alt"),
                ("high",),
            )

    def test_model_catalog_refresh_uses_rotated_token_identity_for_route(self) -> None:
        def rotate_token(token: str, **_kwargs) -> str:
            if token != "pro":
                return token
            self.accounts.delete_accounts(["pro"])
            self.accounts.add_account_items([
                {"access_token": "pro-rotated", "type": "Pro", "status": "正常"},
            ])
            return "pro-rotated"

        self.accounts.refresh_access_token = rotate_token
        self.outcomes["pro-rotated"] = model_list("rotated-only")

        result = self.catalog.list_models()

        self.assertIn("rotated-only", {item["id"] for item in result["data"]})
        route = self.catalog.route_for_model("rotated-only")
        self.assertEqual(route.access_tokens, frozenset({"pro-rotated"}))
        self.assertNotIn("pro", route.access_tokens)


if __name__ == "__main__":
    unittest.main()
