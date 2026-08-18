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
from services.model_service import (
    ModelCatalogPendingError,
    ModelCatalogService,
    ModelUnavailableError,
)
from services.openai_backend_api import InvalidAccessTokenError
from services.storage.json_storage import JSONStorageBackend
from utils.helper import UpstreamHTTPError

# Contract provenance: upstream/main at dc105e51 (services/openai_backend_api.py)
# uses one anonymous endpoint (/backend-anon/models?iim=false&is_gizmo=false)
# and one authenticated Web endpoint (/backend-api/models?history_and_training_disabled=false).
# This fork additionally merges the dedicated Codex catalog when the selected
# representative has a Codex source/account identity; the key remains the
# normalized account type and a type still has one representative.


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

    def test_image_restore_at_does_not_disable_text_catalog_representative(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "image-restore-catalog-accounts.json")
        )
        accounts.add_account_items([
            {
                "access_token": "image-cooling",
                "type": "Pro",
                "status": "正常",
                "quota": 0,
                "restore_at": "2099-01-01T00:00:00Z",
            },
            {
                "access_token": "text-ready",
                "type": "Pro",
                "status": "正常",
                "quota": 3,
                "restore_at": "2099-01-01T00:00:00Z",
            },
        ])
        calls: list[str] = []

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(self.access_token)
                return model_list("pro-text-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: self.now,
        )

        result = catalog.list_models()

        self.assertIn("pro-text-model", {item["id"] for item in result["data"]})
        self.assertCountEqual(calls, ["", "image-cooling"])

    def test_limited_account_is_not_a_catalog_representative(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "limited-catalog-accounts.json")
        )
        accounts.add_account_items([
            {
                "access_token": "limited-pro",
                "type": "Pro",
                "status": "限流",
                "quota": 3,
            },
        ])
        calls: list[str] = []

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(self.access_token)
                return model_list("anonymous-only-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: self.now,
        )

        result = catalog.list_models()

        self.assertIn("anonymous-only-model", {item["id"] for item in result["data"]})
        self.assertEqual(calls, [""])

    def test_anonymous_failure_preserves_authenticated_last_good_catalog(self) -> None:
        catalog = self.catalog
        original_outcomes = self.outcomes
        catalog.list_models()

        self.outcomes[""] = RuntimeError("anonymous catalog unavailable")
        self.outcomes["pro"] = model_list("pro-v2")
        self.now += 301
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))

        result = catalog.list_models(wait_for_cold=False)
        ids = {item["id"] for item in result["data"]}
        self.assertIn("pro-v2", ids)
        self.assertIn("anon", ids)
        self.outcomes = original_outcomes

    def test_authenticated_failure_preserves_anonymous_last_good_catalog(self) -> None:
        catalog = self.catalog
        original_outcomes = self.outcomes
        catalog.list_models()

        self.outcomes[""] = model_list("anon-v2")
        self.outcomes["pro"] = RuntimeError("authenticated catalog unavailable")
        self.now += 301
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))

        result = catalog.list_models(wait_for_cold=False)
        ids = {item["id"] for item in result["data"]}
        self.assertIn("anon-v2", ids)
        self.assertIn("pro-only", ids)
        self.outcomes = original_outcomes

    def test_dual_catalog_failure_keeps_last_good_type_snapshot(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "dual-last-good-accounts.json")
        )
        accounts.add_account_items([
            {
                "access_token": "codex-pro",
                "type": "Pro",
                "source_type": "codex",
                "chatgpt_account_id": "acct-pro",
                "status": "正常",
            },
        ])
        calls = {"anonymous": 0, "catalog": 0}
        now = [1000.0]
        catalog_outcome: object = model_list("pro-v1")

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls["anonymous"] += 1
                return model_list("anon-v1")

            def list_catalog_models(self, **_kwargs: object) -> dict:
                calls["catalog"] += 1
                if isinstance(catalog_outcome, Exception):
                    raise catalog_outcome
                return catalog_outcome

            def close(self) -> None:
                pass

        service = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            cache_ttl_seconds=1,
            clock=lambda: now[0],
        )
        service.list_models()
        catalog_outcome = RuntimeError("dual catalog temporarily unavailable")
        now[0] += 2
        service.list_models(wait_for_cold=False)
        self.assertTrue(service._refresh_done.wait(timeout=3))

        result = service.list_models(wait_for_cold=False)
        ids = {item["id"] for item in result["data"]}
        self.assertIn("pro-v1", ids)
        self.assertIn("anon-v1", ids)
        self.assertEqual(calls["catalog"], 2)

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
        self.assertEqual(
            shared_route.access_tokens,
            frozenset({"free-bad", "free-good", "plus"}),
        )
        self.assertTrue(shared_route.allow_anonymous)

    def test_empty_account_catalog_is_not_published_as_ready(self) -> None:
        self.outcomes["pro"] = model_list()

        with self.assertRaises(ModelCatalogPendingError):
            self.catalog.list_models()

        self.assertNotIn(
            "pro",
            self.catalog._ready_account_types,
        )
        self.assertGreater(
            self.catalog._account_group_retry_not_before["Pro"],
            self.now,
        )

        self.outcomes["pro"] = model_list("pro-recovered")
        self.now += model_service_module.MODEL_CATALOG_RETRY_BACKOFF_SECS + 0.1
        result = self.catalog.list_models()
        self.assertIn("pro-recovered", {item["id"] for item in result["data"]})

    def test_same_type_across_sources_uses_one_web_catalog_representative(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "source-groups-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "web-pro", "type": "Pro", "source_type": "web", "status": "正常"},
            {"access_token": "web-pro-2", "type": "pro", "source_type": "web", "status": "正常"},
            {
                "access_token": "codex-pro",
                "type": "Pro",
                "source_type": "codex",
                "chatgpt_account_id": "codex-account",
                "status": "正常",
            },
            {"access_token": "codex-pro-2", "type": "pro", "source_type": "codex", "status": "正常"},
        ])
        calls: list[tuple[str, object, object]] = []

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(
                    (
                        self.access_token,
                        getattr(self, "_catalog_source_type", None),
                        getattr(self, "_catalog_account_id", None),
                    )
                )
                return model_list(
                    "authenticated-pro-model"
                    if self.access_token
                    else "anonymous-model"
                )

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: self.now,
        )

        result = catalog.list_models()

        self.assertEqual(
            [token for token, _source, _account_id in calls if token],
            ["codex-pro"],
        )
        self.assertEqual(
            [(token, source) for token, source, _account_id in calls if token],
            [("codex-pro", "web")],
        )
        self.assertEqual(
            [
                (token, source, account_id)
                for token, source, account_id in calls
                if token
            ],
            [("codex-pro", "web", "codex-account")],
        )
        self.assertEqual(
            catalog.route_for_model("authenticated-pro-model").access_tokens,
            frozenset({"web-pro", "web-pro-2", "codex-pro", "codex-pro-2"}),
        )
        self.assertEqual(
            {item["id"] for item in result["data"]},
            {"anonymous-model", "authenticated-pro-model"},
        )

    def test_catalog_uses_one_dual_source_representative_per_account_type(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "dual-source-catalog-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "pro-a", "type": "Pro", "status": "正常"},
            {"access_token": "pro-b", "type": "Pro", "status": "正常"},
        ])
        calls: list[tuple[str, str]] = []
        endpoint_calls: list[tuple[str, str]] = []

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(("anonymous", self.access_token))
                return model_list("anonymous-model")

            def list_catalog_models(self, **_kwargs: object) -> dict:
                calls.append(("authenticated-dual", self.access_token))
                endpoint_calls.extend([
                    ("web", self.access_token),
                    ("codex", self.access_token),
                ])
                return model_list("web-model", "codex-model", "shared-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: self.now,
        )

        catalog.list_models()

        self.assertCountEqual(calls, [("anonymous", ""), ("authenticated-dual", "pro-a")])
        self.assertEqual(
            endpoint_calls,
            [("web", "pro-a"), ("codex", "pro-a")],
        )
        route = catalog.route_for_model("shared-model")
        self.assertEqual(route.access_tokens, frozenset({"pro-a", "pro-b"}))

    def test_codex_group_tries_next_representative_after_first_failure(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "codex-failover-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "codex-bad", "type": "Pro", "source_type": "codex", "status": "正常"},
            {"access_token": "codex-good", "type": "pro", "source_type": "codex", "status": "正常"},
        ])
        calls: list[str] = []

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(self.access_token)
                if self.access_token == "codex-bad":
                    raise RuntimeError("codex representative failed")
                return model_list("codex-recovered-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: self.now,
        )

        catalog.list_models()

        self.assertEqual(calls.count("codex-bad"), 1)
        self.assertEqual(calls.count("codex-good"), 1)
        self.assertEqual(
            catalog.route_for_model("codex-recovered-model").access_tokens,
            frozenset({"codex-bad", "codex-good"}),
        )

    def test_cold_catalog_requires_every_active_type_before_public_success(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "cold-all-types-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "free-token", "type": "free", "status": "正常"},
            {"access_token": "pro-token", "type": "pro", "status": "正常"},
        ])
        outcomes: dict[str, object] = {
            "": model_list("anonymous-model"),
            "free-token": model_list("free-real-model"),
            "pro-token": RuntimeError("pro representative unavailable"),
        }

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                outcome = outcomes[self.access_token]
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome

            def close(self) -> None:
                pass

        now = [1000.0]
        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: now[0],
        )
        with self.assertRaises(ModelCatalogPendingError):
            catalog.list_models()

        partial = catalog.list_models(wait_for_cold=False)
        self.assertIn("free-real-model", {item["id"] for item in partial["data"]})
        self.assertNotIn("pro-real-model", {item["id"] for item in partial["data"]})
        with self.assertRaises(ModelCatalogPendingError):
            catalog.list_models()

        outcomes["pro-token"] = model_list(
            "pro-real-1",
            "pro-real-2",
            "pro-real-3",
            "pro-real-4",
        )
        now[0] += model_service_module.MODEL_CATALOG_RETRY_BACKOFF_SECS + 0.1
        complete = catalog.list_models()
        ids = {item["id"] for item in complete["data"]}
        self.assertTrue({"pro-real-1", "pro-real-2", "pro-real-3", "pro-real-4"} <= ids)

    def test_invalid_representative_is_invalidated_and_does_not_block_other_groups(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "invalid-representative-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "free-live", "type": "free", "source_type": "codex", "status": "正常"},
            {"access_token": "pro-invalid", "type": "Pro", "source_type": "codex", "status": "正常"},
        ])
        accounts.refresh_access_token = lambda token, **_kwargs: token

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                if self.access_token == "pro-invalid":
                    raise InvalidAccessTokenError("fixture token invalid")
                return model_list("free-live-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: self.now,
        )
        result = catalog.list_models()

        self.assertIn("free-live-model", {item["id"] for item in result["data"]})
        self.assertNotIn("pro-invalid", catalog._active_accounts_by_group().get("Pro", []))
        invalid_account = accounts.get_account("pro-invalid")
        self.assertTrue(invalid_account is None or invalid_account.get("status") == "异常")
        self.assertEqual(
            catalog.route_for_model("free-live-model").access_tokens,
            frozenset({"free-live"}),
        )

    def test_invalid_group_removal_does_not_freeze_remaining_catalog_refresh(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "invalid-group-refresh-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "free-live", "type": "free", "source_type": "codex", "status": "正常"},
            {"access_token": "pro-invalid", "type": "Pro", "source_type": "codex", "status": "正常"},
        ])
        accounts.refresh_access_token = lambda token, **_kwargs: token
        outcomes = {
            "": model_list("anonymous-v1"),
            "free-live": model_list("free-v1"),
            "pro-invalid": InvalidAccessTokenError("fixture token invalid"),
        }

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                outcome = outcomes[self.access_token]
                if isinstance(outcome, Exception):
                    raise outcome
                return outcome

            def close(self) -> None:
                pass

        now = [1000.0]
        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            cache_ttl_seconds=1,
            clock=lambda: now[0],
        )
        first = catalog.list_models()
        self.assertIn("free-v1", {item["id"] for item in first["data"]})
        self.assertEqual(
            catalog._account_signature,
            catalog._signature({"free": ["free-live"]}),
        )

        outcomes[""] = model_list("anonymous-v2")
        outcomes["free-live"] = model_list("free-v2")
        now[0] += 2
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        second = catalog.list_models(wait_for_cold=False)

        self.assertIn("free-v2", {item["id"] for item in second["data"]})
        self.assertNotIn("free-v1", {item["id"] for item in second["data"]})
        self.assertTrue(catalog._catalog_complete)
        self.assertEqual(
            catalog._account_signature,
            catalog._signature({"free": ["free-live"]}),
        )

    def test_transient_representative_failures_do_not_disable_accounts(self) -> None:
        failures = (
            UpstreamHTTPError("models", 429, {"error": "upstream_error"}),
            UpstreamHTTPError("models", 503, {"error": "upstream_error"}),
            TimeoutError("catalog timeout"),
        )
        for index, failure in enumerate(failures):
            with self.subTest(failure=type(failure).__name__, index=index):
                accounts = AccountService(
                    JSONStorageBackend(Path(self.temp_dir.name) / f"transient-{index}.json")
                )
                token = f"transient-{index}"
                accounts.add_account_items([
                    {"access_token": token, "type": "Pro", "source_type": "codex", "status": "正常"},
                ])
                accounts.refresh_access_token = lambda value, **_kwargs: value

                class Backend:
                    def __init__(self, access_token: str = "") -> None:
                        self.access_token = access_token

                    def list_models(self, **_kwargs: object) -> dict:
                        if self.access_token:
                            raise failure
                        return model_list("anonymous-model")

                    def close(self) -> None:
                        pass

                catalog = ModelCatalogService(
                    accounts,
                    backend_factory=Backend,
                    clock=lambda: self.now,
                )
                with self.assertRaises(ModelCatalogPendingError):
                    catalog.list_models()

                current = accounts.get_account(token)
                self.assertIsNotNone(current)
                self.assertEqual(current["status"], "正常")
                self.assertIn("Pro", catalog._active_accounts_by_group())

    def test_invalid_transition_cannot_disable_replaced_account_identity(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "invalid-replacement-accounts.json")
        )
        token = "replaced-before-invalid-transition"
        accounts.add_account_items([
            {"access_token": token, "type": "Pro", "source_type": "codex", "status": "正常"},
        ])
        accounts.refresh_access_token = lambda value, **_kwargs: value
        entered = Event()
        release = Event()

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                if not self.access_token:
                    return model_list("anonymous-model")
                entered.set()
                if not release.wait(timeout=2):
                    raise AssertionError("invalid transition barrier was not released")
                raise InvalidAccessTokenError("fixture token invalid")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: self.now,
        )
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(entered.wait(timeout=2))
        accounts.update_account(token, {"quota": 42})
        release.set()
        self.assertTrue(catalog._refresh_done.wait(timeout=3))

        current = accounts.get_account(token)
        self.assertIsNotNone(current)
        self.assertEqual(current["status"], "正常")
        self.assertEqual(current["quota"], 42)

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

        self.assertEqual(len(deadlines), 4)
        self.assertEqual(deadlines[0], 5090.0)
        self.assertEqual(set(deadlines), {5090.0})
        self.assertEqual(len(refresh_deadlines), 3)
        self.assertEqual(set(refresh_deadlines), {5090.0})

    def test_expired_route_deadline_does_not_start_catalog_owner(self) -> None:
        calls: list[str] = []

        class DeadlineBackend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(self.access_token)
                return model_list("must-not-run")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            self.accounts,
            backend_factory=DeadlineBackend,
            clock=lambda: 1000.0,
            deadline_clock=lambda: 200.0,
        )

        route = catalog.route_for_model("cold-model", deadline=199.0)

        self.assertEqual(route.access_tokens, frozenset())
        self.assertFalse(route.catalog_complete)
        self.assertEqual(calls, [])
        self.assertFalse(catalog._refresh_in_progress)

    def test_cold_nonblocking_refresh_uses_one_representative_for_large_type(self) -> None:
        total_accounts = 1495
        accounts = [
            {"access_token": f"token-{index}", "type": "free", "status": "正常"}
            for index in range(total_accounts)
        ]
        refresh_started = Event()
        release_first = Event()
        request_count: list[str] = []

        class ManyAccounts:
            def list_accounts(self) -> list[dict[str, object]]:
                return [dict(item) for item in accounts]

            def _is_text_account_available(self, account: dict[str, object]) -> bool:
                return account.get("status") == "正常"

            def _normalize_account_type(self, value: object) -> str:
                return str(value or "")

            def refresh_access_token(self, token: str, **_kwargs: object) -> str:
                return token

            def _get_account_lease(self, token: str):
                for item in accounts:
                    if item["access_token"] == token:
                        return token, item
                return token, None

        class ManyBackend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                request_count.append(self.access_token)
                if self.access_token == "token-0":
                    refresh_started.set()
                    release_first.wait(timeout=2)
                return model_list(
                    "anonymous-model" if not self.access_token else "shared-free-model"
                )

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            ManyAccounts(),
            backend_factory=ManyBackend,
            cache_ttl_seconds=300,
        )

        result = catalog.list_models(wait_for_cold=False)

        self.assertIsInstance(result, dict)
        self.assertTrue(refresh_started.wait(timeout=1))
        self.assertEqual(request_count.count("token-0"), 1)
        self.assertEqual(sum(bool(token) for token in request_count), 1)
        self.assertNotIn(f"token-{total_accounts - 1}", request_count)

        release_first.set()
        self.assertTrue(catalog._refresh_done.wait(timeout=5))
        complete = catalog.list_models(wait_for_cold=False)
        self.assertIn("shared-free-model", {item["id"] for item in complete["data"]})
        self.assertEqual(sum(bool(token) for token in request_count), 1)
        self.assertEqual(catalog.route_for_model("shared-free-model").access_tokens, frozenset(
            f"token-{index}" for index in range(total_accounts)
        ))

    def test_refresh_request_count_scales_with_account_types_not_accounts(self) -> None:
        total_accounts = 1495
        account_types = ("free", "pro")
        accounts = [
            {
                "access_token": f"token-{index}",
                "type": "FREE" if index % len(account_types) == 0 else "Pro",
                "status": "正常",
            }
            for index in range(total_accounts)
        ]
        calls: list[str] = []

        class ManyAccounts:
            def list_accounts(self) -> list[dict[str, object]]:
                return [dict(item) for item in accounts]

            def _is_text_account_available(self, account: dict[str, object]) -> bool:
                return account.get("status") == "正常"

            def _normalize_account_type(self, value: object) -> str:
                return str(value or "").strip().lower()

            def refresh_access_token(self, token: str, **_kwargs: object) -> str:
                return token

            def _get_account_lease(self, token: str):
                for item in accounts:
                    if item["access_token"] == token:
                        return token, item
                return token, None

        class ManyBackend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(self.access_token)
                return model_list(f"{self.access_token or 'anonymous'}-model")

            def close(self) -> None:
                pass

        now = [1000.0]
        catalog = ModelCatalogService(
            ManyAccounts(),
            backend_factory=ManyBackend,
            clock=lambda: now[0],
            cache_ttl_seconds=300,
        )

        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))

        non_anonymous_calls = [token for token in calls if token]
        self.assertEqual(len(non_anonymous_calls), len(account_types))
        self.assertEqual(
            {
                next(item["type"].strip().lower() for item in accounts if item["access_token"] == token)
                for token in non_anonymous_calls
            },
            set(account_types),
        )
        self.assertEqual(calls.count(""), 1)

        accounts.pop(0)
        accounts.append({"access_token": "same-type-replacement", "type": "free", "status": "正常"})
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        self.assertEqual(len(calls), 4)

        now[0] += 301
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        self.assertEqual(calls.count(""), 2)
        refreshed_non_anonymous = [token for token in calls[4:] if token]
        self.assertEqual(len(refreshed_non_anonymous), len(account_types))
        self.assertEqual(
            {
                next(item["type"].strip().lower() for item in accounts if item["access_token"] == token)
                for token in refreshed_non_anonymous
            },
            set(account_types),
        )

    def test_nonblocking_route_marks_unscanned_model_as_pending(self) -> None:
        refresh_started = Event()
        release_refresh = Event()

        class BlockingBackend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                refresh_started.set()
                release_refresh.wait(timeout=2)
                return model_list("known-model")

            def close(self) -> None:
                pass

        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "pending-route-accounts.json")
        )
        accounts.add_account_items([{"access_token": "pending-token", "type": "free", "status": "正常"}])
        catalog = ModelCatalogService(accounts, backend_factory=BlockingBackend)

        route = catalog.route_for_model("unknown-model", wait_for_cold=False)

        self.assertFalse(route.catalog_complete)
        self.assertEqual(route.access_tokens, frozenset())
        release_refresh.set()
        self.assertTrue(refresh_started.wait(timeout=1))

    def test_nonblocking_refresh_reopens_when_a_new_account_type_appears(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "type-change-accounts.json")
        )
        accounts.add_account_items([{"access_token": "free-token", "type": "free", "status": "正常"}])
        free_started = Event()
        release_free = Event()
        plus_started = Event()
        release_plus = Event()
        calls: list[str] = []

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(self.access_token)
                if self.access_token == "free-token" and not free_started.is_set():
                    free_started.set()
                    if not release_free.wait(timeout=2):
                        raise AssertionError("free representative was not released")
                if self.access_token == "plus-token":
                    plus_started.set()
                    if not release_plus.wait(timeout=2):
                        raise AssertionError("plus representative was not released")
                return model_list(f"{self.access_token or 'anonymous'}-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(accounts, backend_factory=Backend)
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(free_started.wait(timeout=1))

        accounts.add_account_items([{"access_token": "plus-token", "type": "Plus", "status": "正常"}])
        release_free.set()
        self.assertTrue(catalog._refresh_done.wait(timeout=3))

        catalog.list_models(wait_for_cold=False)
        self.assertTrue(plus_started.wait(timeout=1))
        release_plus.set()
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        self.assertIn("plus-token-model", {item["id"] for item in catalog.list_models()["data"]})
        self.assertEqual(calls.count("free-token"), 1)
        self.assertEqual(calls.count("plus-token"), 1)

    def test_failed_type_without_last_good_is_retried_on_next_read(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "type-retry-accounts.json")
        )
        accounts.add_account_items([{"access_token": "free-token", "type": "free", "status": "正常"}])
        attempts = 0
        now = [1000.0]

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                nonlocal attempts
                if self.access_token == "free-token":
                    attempts += 1
                    if attempts == 1:
                        raise RuntimeError("representative unavailable")
                    return model_list("recovered-model")
                return model_list("anonymous-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: now[0],
        )
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        self.assertNotIn("recovered-model", {item["id"] for item in catalog.list_models(wait_for_cold=False)["data"]})

        now[0] += model_service_module.MODEL_CATALOG_RETRY_BACKOFF_SECS + 0.1
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        self.assertIn("recovered-model", {item["id"] for item in catalog.list_models()["data"]})
        self.assertEqual(attempts, 2)

    def test_cold_failure_backoff_prevents_repeated_blocking_reads(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "cold-failure-backoff.json")
        )
        accounts.add_account_items([{"access_token": "free-token", "type": "free", "status": "正常"}])
        calls: list[str] = []
        now = [1000.0]

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(self.access_token)
                if self.access_token:
                    raise RuntimeError("representative unavailable")
                return model_list("anonymous-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: now[0],
        )
        with self.assertRaises(ModelCatalogPendingError):
            catalog.list_models()
        first_attempts = list(calls)

        with self.assertRaises(ModelCatalogPendingError):
            catalog.list_models()

        self.assertEqual(calls, first_attempts)

    def test_type_refresh_tries_third_candidate_after_first_two_fail(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "third-candidate-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "bad-one", "type": "Pro", "status": "正常"},
            {"access_token": "bad-two", "type": "Pro", "status": "正常"},
            {"access_token": "good-three", "type": "Pro", "status": "正常"},
        ])
        calls: list[str] = []

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                if self.access_token:
                    calls.append(self.access_token)
                if self.access_token in {"bad-one", "bad-two"}:
                    raise RuntimeError("representative unavailable")
                return model_list("complete-pro-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(accounts, backend_factory=Backend)
        catalog.list_models()

        self.assertEqual(calls, ["bad-one", "bad-two", "good-three"])
        self.assertIn(
            "complete-pro-model",
            {item["id"] for item in catalog.list_models(wait_for_cold=False)["data"]},
        )

    def test_type_refresh_exhausts_all_candidates_and_deduplicates_rotated_aliases(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "all-candidate-failure-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "alias-one", "type": "Pro", "status": "正常"},
            {"access_token": "alias-two", "type": "Pro", "status": "正常"},
            {"access_token": "alias-three", "type": "Pro", "status": "正常"},
        ])
        calls: list[str] = []

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                if self.access_token:
                    calls.append(self.access_token)
                raise RuntimeError("representative unavailable")

            def close(self) -> None:
                pass

        original_refresh = accounts.refresh_access_token
        refresh_map = {
            "alias-one": "canonical",
            "alias-two": "canonical",
            "alias-three": "canonical",
        }

        def refresh(token: str, **kwargs: object) -> str:
            return refresh_map.get(token, original_refresh(token, **kwargs))

        accounts.refresh_access_token = refresh  # type: ignore[method-assign]
        accounts._get_account_lease = lambda token: (  # type: ignore[method-assign]
            token,
            {"access_token": token, "type": "Pro", "status": "正常"}
            if token == "canonical"
            else None,
        )
        catalog = ModelCatalogService(accounts, backend_factory=Backend)
        with self.assertRaises(ModelCatalogPendingError):
            catalog.list_models()

        self.assertEqual(calls, ["canonical"])

    def test_synchronous_failed_type_without_last_good_reopens_on_next_read(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "sync-type-retry-accounts.json")
        )
        accounts.add_account_items([{"access_token": "free-token", "type": "free", "status": "正常"}])
        attempts = 0
        now = [1000.0]

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                nonlocal attempts
                if self.access_token == "free-token":
                    attempts += 1
                    if attempts == 1:
                        raise RuntimeError("representative unavailable")
                    return model_list("synchronous-recovered-model")
                return model_list("anonymous-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: now[0],
        )
        with self.assertRaises(ModelCatalogPendingError):
            catalog.list_models()
        self.assertNotIn(
            "synchronous-recovered-model",
            {item["id"] for item in catalog.list_models(wait_for_cold=False)["data"]},
        )
        with self.assertRaises(ModelCatalogPendingError):
            catalog.list_models()
        self.assertEqual(attempts, 1)

        now[0] += model_service_module.MODEL_CATALOG_RETRY_BACKOFF_SECS + 0.1
        catalog.list_models()
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        second = catalog.list_models(wait_for_cold=False)
        self.assertIn(
            "synchronous-recovered-model",
            {item["id"] for item in second["data"]},
        )
        self.assertEqual(attempts, 2)

    def test_failed_type_does_not_block_ready_routes_or_retry_per_request(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "failed-type-isolation-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "ready-token", "type": "Ready", "status": "正常"},
            {"access_token": "bad-one", "type": "Broken", "status": "正常"},
            {"access_token": "bad-two", "type": "Broken", "status": "正常"},
        ])
        calls: list[str] = []
        bad_started = Event()
        release_bad = Event()
        block_bad = False
        now = [1000.0]

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                calls.append(self.access_token)
                if self.access_token == "ready-token":
                    return model_list("ready-model")
                if self.access_token in {"bad-one", "bad-two"}:
                    if block_bad:
                        bad_started.set()
                        release_bad.wait(timeout=2)
                    raise RuntimeError("broken representative")
                return model_list("anonymous-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(
            accounts,
            backend_factory=Backend,
            clock=lambda: now[0],
        )
        catalog.list_models(wait_for_cold=False)
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        self.assertEqual(calls.count("ready-token"), 1)
        self.assertEqual(calls.count("bad-one") + calls.count("bad-two"), 2)

        for _ in range(20):
            route = catalog.route_for_model("ready-model")
            self.assertEqual(route.access_tokens, frozenset({"ready-token"}))
        self.assertEqual(calls.count("ready-token"), 1)
        self.assertEqual(calls.count("bad-one") + calls.count("bad-two"), 2)

        block_bad = True
        now[0] += model_service_module.MODEL_CATALOG_RETRY_BACKOFF_SECS + 0.1
        route = catalog.route_for_model("ready-model")
        self.assertEqual(route.access_tokens, frozenset({"ready-token"}))
        self.assertTrue(bad_started.wait(timeout=1))
        self.assertEqual(calls.count("ready-token"), 1)

        pending_route = catalog.route_for_model("unknown-while-broken-retry")
        self.assertFalse(pending_route.catalog_complete)
        with mock.patch("services.model_service.model_catalog_service", catalog):
            with self.assertRaises(ModelCatalogPendingError):
                accounts.get_text_access_token(model="unknown-while-broken-retry")
        release_bad.set()
        self.assertTrue(catalog._refresh_done.wait(timeout=3))
        self.assertEqual(calls.count("ready-token"), 1)
        self.assertEqual(calls.count("bad-one") + calls.count("bad-two"), 4)

    def test_cold_ready_model_does_not_wait_for_unrelated_pending_type(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "cold-ready-isolation-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "ready-token", "type": "Ready", "status": "正常"},
            {"access_token": "blocked-token", "type": "Broken", "status": "正常"},
        ])
        ready_returned = Event()
        blocked_started = Event()
        release_blocked = Event()

        class Backend:
            def __init__(self, access_token: str = "") -> None:
                self.access_token = access_token

            def list_models(self, **_kwargs: object) -> dict:
                if self.access_token == "ready-token":
                    ready_returned.set()
                    return model_list("ready-model")
                if self.access_token == "blocked-token":
                    blocked_started.set()
                    if not release_blocked.wait(timeout=3):
                        raise AssertionError("blocked representative was not released")
                    raise RuntimeError("unrelated type is unavailable")
                return model_list("anonymous-model")

            def close(self) -> None:
                pass

        catalog = ModelCatalogService(accounts, backend_factory=Backend)
        executor = ThreadPoolExecutor(max_workers=1)
        self.addCleanup(executor.shutdown, wait=True)
        self.addCleanup(release_blocked.set)
        future = executor.submit(catalog.route_for_model, "ready-model")

        self.assertTrue(ready_returned.wait(timeout=1))
        self.assertTrue(blocked_started.wait(timeout=1))
        route = future.result(timeout=1)
        self.assertEqual(route.access_tokens, frozenset({"ready-token"}))

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

    def test_refresh_admission_timeout_backs_off_types_that_never_got_a_slot(self) -> None:
        accounts = AccountService(
            JSONStorageBackend(Path(self.temp_dir.name) / "admission-backoff-accounts.json")
        )
        accounts.add_account_items([
            {"access_token": "free-token", "type": "free", "status": "正常"},
            {"access_token": "plus-token", "type": "plus", "status": "正常"},
        ])
        now = [1000.0]
        submissions = 0

        def submit_refresh(_func, *_args, **_kwargs):
            nonlocal submissions
            submissions += 1
            if submissions == 1:
                future = Future()
                future.set_result({"anonymous-model": {"id": "anonymous-model"}})
                return future
            raise model_service_module.ModelCatalogRefreshTimeout(
                "model catalog refresh timed out"
            )

        catalog = ModelCatalogService(
            accounts,
            backend_factory=FakeBackend,
            clock=lambda: now[0],
            deadline_clock=lambda: now[0],
        )
        with (
            mock.patch.object(model_service_module, "MODEL_CATALOG_REFRESH_WORKERS", 1),
            mock.patch.object(catalog, "_submit_refresh_future", side_effect=submit_refresh),
        ):
            catalog.list_models(wait_for_cold=False)
            self.assertTrue(catalog._refresh_done.wait(timeout=2))
            first_submissions = submissions

            catalog.list_models(wait_for_cold=False)
            self.assertTrue(catalog._refresh_done.wait(timeout=2))
            self.assertEqual(submissions, first_submissions)

            now[0] += model_service_module.MODEL_CATALOG_RETRY_BACKOFF_SECS + 0.1
            catalog.list_models(wait_for_cold=False)
            self.assertTrue(catalog._refresh_done.wait(timeout=2))
            self.assertGreater(submissions, first_submissions)

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
                    for model_id in catalog._models_by_account_type["Pro"]
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
            refresh.result(timeout=2)
            self.assertTrue(catalog._refresh_done.wait(timeout=2))

        self.assertTrue(stale_route.allow_anonymous)
        self.assertFalse(
            catalog.route_for_model("cached-model", wait_for_cold=False).allow_anonymous
        )
        self.assertEqual(calls, [1, 2])

    def test_failed_refresh_keeps_last_successful_models_for_that_type(self) -> None:
        self.catalog.list_models()
        self.outcomes["pro"] = RuntimeError("temporary upstream failure")
        self.now += 301

        self.catalog.list_models(wait_for_cold=False)
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))
        result = self.catalog.list_models(wait_for_cold=False)

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

    def test_removed_group_without_fetch_converges_signature_and_refreshes_remaining_groups(self) -> None:
        self.catalog.list_models()
        self.accounts.delete_accounts(["pro"])

        self.catalog.list_models(wait_for_cold=False)
        self.assertEqual(
            self.catalog._account_signature,
            self.catalog._signature({
                "free": ["free-bad", "free-good"],
                "Plus": ["plus"],
            }),
        )
        self.assertTrue(self.catalog._catalog_complete)

        self.outcomes["free-good"] = model_list("free-refreshed")
        self.now += 301
        self.catalog.list_models(wait_for_cold=False)
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))
        result = self.catalog.list_models(wait_for_cold=False)

        self.assertIn("free-refreshed", {item["id"] for item in result["data"]})
        self.assertNotIn("free-only", {item["id"] for item in result["data"]})
        self.assertEqual(
            self.catalog._account_signature,
            self.catalog._signature({
                "free": ["free-bad", "free-good"],
                "Plus": ["plus"],
            }),
        )

    def test_account_removed_during_failed_refresh_drops_stale_token_capabilities(self) -> None:
        self.catalog.list_models()

        def remove_then_fail(token: str, **_kwargs) -> str:
            self.accounts.delete_accounts([token])
            return token

        self.accounts.refresh_access_token = remove_then_fail
        self.outcomes["pro"] = RuntimeError("account removed during refresh")
        self.now += 301

        self.catalog.list_models(wait_for_cold=False)
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))

        self.assertEqual(
            self.catalog.route_for_model("pro-only", wait_for_cold=False).access_tokens,
            frozenset(),
        )

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
        self.catalog.list_models(wait_for_cold=False)
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))
        with mock.patch.object(self.catalog, "_ensure_catalog_nonblocking", return_value=None):
            result = self.catalog.list_models(wait_for_cold=False)
            self.assertNotIn("free-only", {item["id"] for item in result["data"]})
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
        self.accounts.update_account("pro", {"type": "Enterprise"})
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

        self.catalog.list_models(wait_for_cold=False)
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))
        result = self.catalog.list_models(wait_for_cold=False)

        self.assertIn("pro-new-only", {item["id"] for item in result["data"]})
        self.assertNotIn("pro-only", {item["id"] for item in result["data"]})
        self.assertEqual(self.calls.count("pro-new"), 1)
        self.assertEqual(
            self.catalog.route_for_model("pro-new-only").access_tokens,
            frozenset({"pro-new"}),
        )

    def test_late_refresh_result_cannot_publish_after_same_token_account_replacement(self) -> None:
        self.catalog.list_models()
        refresh_started = Event()
        release_refresh = Event()
        original_factory = self.catalog._backend_factory

        def backend_factory(access_token: str = ""):
            backend = original_factory(access_token=access_token)
            if access_token != "pro":
                return backend

            original_list_models = backend.list_models

            def list_models(**kwargs) -> dict:
                refresh_started.set()
                self.assertTrue(release_refresh.wait(timeout=2))
                return model_list("late-pro-model")

            backend.list_models = list_models
            return backend

        self.catalog._backend_factory = backend_factory
        self.now += 301

        first = self.catalog.list_models(wait_for_cold=False)
        self.assertNotIn("late-pro-model", {item["id"] for item in first["data"]})
        self.assertTrue(refresh_started.wait(timeout=1))

        self.accounts.update_account("pro", {"quota": 99})
        release_refresh.set()
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))

        result = self.catalog.list_models(wait_for_cold=False)
        self.assertNotIn("late-pro-model", {item["id"] for item in result["data"]})
        self.assertEqual(
            self.catalog.route_for_model("late-pro-model", wait_for_cold=False).access_tokens,
            frozenset(),
        )

    def test_same_type_accounts_share_one_model_capability_catalog(self) -> None:
        self.accounts.add_account_items([
            {"access_token": "pro-alt", "type": "Pro", "status": "正常"},
        ])
        self.outcomes["pro-alt"] = model_list("pro-alt-only")

        result = self.catalog.list_models()

        self.assertIn("pro-only", {item["id"] for item in result["data"]})
        self.assertNotIn("pro-alt-only", {item["id"] for item in result["data"]})
        self.assertEqual(
            self.catalog.route_for_model("pro-only").access_tokens,
            frozenset({"pro", "pro-alt"}),
        )

    def test_same_type_membership_changes_invalidate_owner_and_refresh_catalog(self) -> None:
        self.catalog.list_models()
        initial_calls = list(self.calls)
        self.outcomes["pro-alt"] = model_list("pro-only")

        self.accounts.add_account_items(
            [{"access_token": "pro-alt", "type": "PRO", "status": "正常"}]
        )
        self.assertEqual(
            self.catalog.route_for_model("pro-only").access_tokens,
            frozenset(),
        )
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))
        self.assertEqual(
            self.catalog.route_for_model("pro-only").access_tokens,
            frozenset({"pro", "pro-alt"}),
        )
        self.accounts.delete_accounts(["pro"])
        self.assertEqual(
            self.catalog.route_for_model("pro-only").access_tokens,
            frozenset(),
        )
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))
        self.assertEqual(
            self.catalog.route_for_model("pro-only").access_tokens,
            frozenset({"pro-alt"}),
        )
        self.assertNotEqual(self.calls, initial_calls)

    def test_public_list_does_not_publish_stale_catalog_while_group_membership_refreshes(self) -> None:
        self.catalog.list_models()
        refresh_started = Event()
        release_refresh = Event()
        self.outcomes["pro"] = RuntimeError("old representative unavailable")
        self.accounts.add_account_items(
            [{"access_token": "pro-alt", "type": "PRO", "status": "正常"}]
        )

        original_factory = self.catalog._backend_factory

        def backend_factory(access_token: str = ""):
            backend = original_factory(access_token=access_token)
            if access_token != "pro-alt":
                return backend
            original_list_models = backend.list_models

            def list_models(**kwargs) -> dict:
                refresh_started.set()
                self.assertTrue(release_refresh.wait(timeout=2))
                return original_list_models(**kwargs)

            backend.list_models = list_models
            return backend

        self.catalog._backend_factory = backend_factory
        self.now += 301
        self.catalog.list_models(wait_for_cold=False)
        self.assertTrue(refresh_started.wait(timeout=1))

        try:
            result = self.catalog.list_models(wait_for_cold=False)
            self.assertNotIn("pro-only", {item["id"] for item in result["data"]})
        finally:
            release_refresh.set()
        self.assertTrue(self.catalog._refresh_done.wait(timeout=3))

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
            with self.assertRaises(ModelUnavailableError):
                self.accounts.get_text_access_token(model="model-b")
            self.assertEqual(
                self.catalog.supported_reasoning_efforts("model-a", access_token="pro"),
                ("minimal",),
            )
            self.assertEqual(
                self.catalog.supported_reasoning_efforts("model-a", access_token="pro-alt"),
                ("minimal",),
            )
            self.assertEqual(
                self.catalog.supported_reasoning_efforts("model-shared", access_token="pro"),
                ("minimal",),
            )
            self.assertEqual(
                self.catalog.supported_reasoning_efforts("model-shared", access_token="pro-alt"),
                ("minimal",),
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
