from __future__ import annotations

import io
import asyncio
import json
import threading
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace
from unittest import mock

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.accounts as accounts_module
import services.oauth_login_service as oauth_module
from services.oauth_login_service import OAuthLoginError


AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
CALLBACK_CODE = "oauth-code-secret"
CALLBACK_STATE = "oauth-state-secret"


class OAuthPublicContractTests(unittest.TestCase):
    def test_start_keeps_temporary_session_store_within_capacity(self) -> None:
        service = oauth_module.OAuthLoginService()

        with mock.patch.object(
            service,
            "_generate_pkce",
            return_value=("verifier", "challenge"),
        ):
            for _ in range(service._MAX_SESSIONS + 1):
                service.start()

        self.assertLessEqual(len(service._sessions), service._MAX_SESSIONS)

    def test_token_exchange_does_not_allow_runtime_skip_ssl_to_disable_tls(self) -> None:
        response = mock.Mock(status_code=200, text='{"access_token":"access-token","refresh_token":"refresh-token","id_token":"id-token"}')
        response.iter_content = None
        response.json.return_value = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }
        session = mock.Mock()
        session.post.return_value = response
        runtime_profile = SimpleNamespace(
            proxy_url="http://proxy.example.test:8080",
            runtime_enabled=True,
            skip_ssl_verify=True,
        )

        with (
            mock.patch.object(oauth_module.requests, "Session", return_value=session) as session_factory,
            mock.patch.object(oauth_module.proxy_settings, "get_profile", return_value=runtime_profile),
        ):
            self.assertEqual(
                oauth_module.OAuthLoginService._exchange_code(
                    "code", "verifier", "https://example.test/callback"
                )["access_token"],
                "access-token",
            )

        self.assertTrue(session_factory.call_args.kwargs["verify"])
        self.assertEqual(session_factory.call_args.kwargs["proxy"], "http://proxy.example.test:8080")

    def test_token_exchange_rejects_container_token_fields(self) -> None:
        canary = "oauth-container-token-canary"
        payload = {
            "access_token": {"secret": canary},
            "refresh_token": [canary],
            "id_token": {"secret": canary},
        }

        class Response:
            status_code = 200
            text = json.dumps(payload)

            def json(self):
                raise AssertionError("bounded parser must not call json()")

        class Session:
            def post(self, *_args, **_kwargs):
                return Response()

            def close(self):
                pass

        output = io.StringIO()
        with (
            mock.patch.object(oauth_module.requests, "Session", return_value=Session()),
            mock.patch.object(oauth_module.proxy_settings, "build_session_kwargs", return_value={}),
            redirect_stdout(output),
        ):
            with self.assertRaises(OAuthLoginError):
                oauth_module.OAuthLoginService._exchange_code("code", "verifier", "https://example.test/callback")

        self.assertNotIn(canary, output.getvalue())

    def test_token_exchange_closes_non_2xx_response(self) -> None:
        response = mock.Mock(status_code=401, text='{"error":"invalid_grant"}')
        response.iter_content = None
        response.json.return_value = {"error": "invalid_grant"}
        session = mock.Mock()
        session.post.return_value = response

        with (
            mock.patch.object(oauth_module.requests, "Session", return_value=session),
            mock.patch.object(
                oauth_module.proxy_settings,
                "build_session_kwargs",
                return_value={},
            ) as build_session_kwargs,
        ):
            with self.assertRaises(OAuthLoginError):
                oauth_module.OAuthLoginService._exchange_code(
                    "code", "verifier", "https://example.test/callback"
                )

        response.close.assert_called_once_with()
        self.assertTrue(session.post.call_args.kwargs["stream"])
        build_session_kwargs.assert_called_once_with(impersonate="chrome", verify=True)

    def test_token_exchange_closes_response_after_json_parse(self) -> None:
        response = mock.Mock(status_code=200, text='{"access_token":"access-token","refresh_token":"refresh-token","id_token":"id-token"}')
        response.iter_content = None
        response.json.return_value = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }
        session = mock.Mock()
        session.post.return_value = response

        with (
            mock.patch.object(oauth_module.requests, "Session", return_value=session),
            mock.patch.object(oauth_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            self.assertEqual(
                oauth_module.OAuthLoginService._exchange_code(
                    "code", "verifier", "https://example.test/callback"
                ),
                {
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "id_token": "id-token",
                },
            )

        response.close.assert_called_once_with()
        self.assertTrue(session.post.call_args.kwargs["stream"])

    def test_token_exchange_rejects_oversized_stream_and_closes_response(self) -> None:
        class Response:
            status_code = 200
            ok = True

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, *, chunk_size: int):
                self.chunk_size = chunk_size
                yield b"x" * (oauth_module.OAuthLoginService._MAX_TOKEN_RESPONSE_BYTES + 1)

            def close(self) -> None:
                self.closed = True

        class Session:
            response = None
            kwargs = None

            def post(self, *args, **kwargs):
                type(self).kwargs = kwargs
                type(self).response = Response()
                return type(self).response

            def close(self) -> None:
                pass

        with (
            mock.patch.object(oauth_module.requests, "Session", return_value=Session()),
            mock.patch.object(oauth_module.proxy_settings, "build_session_kwargs", return_value={}),
        ):
            with self.assertRaises(OAuthLoginError):
                oauth_module.OAuthLoginService._exchange_code(
                    "code", "verifier", "https://example.test/callback"
                )

        self.assertTrue(Session.kwargs["stream"])
        self.assertTrue(Session.response.closed)

    def test_finish_does_not_log_callback_code_or_state(self) -> None:
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        client = TestClient(app)
        callback = f"https://platform.openai.com/callback?code={CALLBACK_CODE}&state={CALLBACK_STATE}"
        output = io.StringIO()

        with (
            mock.patch.object(
                accounts_module.oauth_login_service,
                "finish",
                side_effect=OAuthLoginError("oauth rejected"),
            ),
            redirect_stdout(output),
        ):
            response = client.post(
                "/api/accounts/oauth/finish",
                headers=AUTH_HEADERS,
                json={"session_id": "session-secret", "callback": callback},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn(CALLBACK_CODE, output.getvalue())
        self.assertNotIn(CALLBACK_STATE, output.getvalue())
        self.assertNotIn(CALLBACK_CODE, response.text)
        self.assertNotIn(CALLBACK_STATE, response.text)

    def test_finish_rejects_a_concurrent_replay_before_second_token_exchange(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        first_started = threading.Event()
        release_first = threading.Event()
        second_done = threading.Event()
        calls = []
        results = {}

        def exchange(*_args):
            calls.append(True)
            if len(calls) == 1:
                first_started.set()
                release_first.wait(timeout=2)
            return {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "id_token": "id-token",
            }

        def finish(name: str) -> None:
            try:
                results[name] = service.finish("session-1", "code")
            except Exception as exc:
                results[name] = exc
            finally:
                if name == "second":
                    second_done.set()

        with mock.patch.object(service, "_exchange_code", side_effect=exchange):
            first = threading.Thread(target=finish, args=("first",))
            first.start()
            self.assertTrue(first_started.wait(timeout=2))
            second = threading.Thread(target=finish, args=("second",))
            second.start()
            second_finished_before_first = second_done.wait(timeout=0.2)
            release_first.set()
            first.join(timeout=2)
            second.join(timeout=2)

        self.assertTrue(second_finished_before_first)
        self.assertEqual(len(calls), 1)
        self.assertEqual(results["first"]["access_token"], "access-token")
        self.assertIsInstance(results["second"], OAuthLoginError)

    def test_failed_exchange_releases_session_claim_for_retry(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        with mock.patch.object(
            service,
            "_exchange_code",
            side_effect=[OAuthLoginError("temporary exchange failure"), {
                "access_token": "access-token",
                "refresh_token": "refresh-token",
                "id_token": "id-token",
            }],
        ) as exchange:
            with self.assertRaises(OAuthLoginError):
                service.finish("session-1", "code")
            tokens = service.finish("session-1", "code")

        self.assertEqual(tokens["access_token"], "access-token")
        self.assertEqual(exchange.call_count, 2)
        service.commit_finish("session-1", "code", tokens.claim_id)
        self.assertNotIn("session-1", service._sessions)

    def test_cancelled_exchange_releases_session_claim_for_retry(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        tokens = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }
        with mock.patch.object(
            service,
            "_exchange_code",
            side_effect=[asyncio.CancelledError(), tokens],
        ):
            with self.assertRaises(asyncio.CancelledError):
                service.finish("session-1", "code")
            retry_result = service.finish("session-1", "code")

        self.assertEqual(retry_result, tokens)
        service.abort_finish("session-1", "code", retry_result.claim_id)

    def test_successful_exchange_waits_for_explicit_persistence_commit(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        tokens = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }

        with mock.patch.object(service, "_exchange_code", return_value=tokens):
            finish_result = service.finish("session-1", "code")
            self.assertEqual(finish_result, tokens)

        self.assertIn("session-1", service._sessions)
        self.assertTrue(service._sessions["session-1"].get("_exchange_in_flight"))
        service.commit_finish("session-1", "code", finish_result.claim_id)
        self.assertNotIn("session-1", service._sessions)

    def test_stale_abort_cannot_release_a_new_finish_claim(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        tokens = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }

        with mock.patch.object(service, "_exchange_code", return_value=tokens):
            first = service.finish("session-1", "code")
            service.abort_finish("session-1", "code", first.claim_id)
            second = service.finish("session-1", "code")

        service.abort_finish("session-1", "code", first.claim_id)
        self.assertTrue(service._sessions["session-1"].get("_exchange_in_flight"))
        service.commit_finish("session-1", "code", second.claim_id)
        self.assertNotIn("session-1", service._sessions)

    def test_stale_commit_cannot_consume_a_new_finish_claim(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        tokens = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }

        with mock.patch.object(service, "_exchange_code", return_value=tokens):
            first = service.finish("session-1", "code")
            service.abort_finish("session-1", "code", first.claim_id)
            second = service.finish("session-1", "code")

        with self.assertRaises(OAuthLoginError):
            service.commit_finish("session-1", "code", first.claim_id)
        self.assertIn("session-1", service._sessions)
        self.assertTrue(service._sessions["session-1"].get("_exchange_in_flight"))
        service.commit_finish("session-1", "code", second.claim_id)
        self.assertNotIn("session-1", service._sessions)

    def test_api_finish_commits_pkce_session_after_account_persist(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        client = TestClient(app)
        tokens = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }
        with (
            mock.patch.object(accounts_module, "oauth_login_service", service),
            mock.patch.object(service, "_exchange_code", return_value=tokens),
            mock.patch.object(
                accounts_module.account_service,
                "add_account_items",
                return_value={"added": 1, "skipped": 0, "items": []},
            ) as add_items,
            mock.patch.object(
                accounts_module.account_service,
                "refresh_accounts",
                return_value={"refreshed": 1, "errors": [], "items": []},
            ) as refresh_accounts,
            mock.patch.object(service, "commit_finish", wraps=service.commit_finish) as commit_finish,
        ):
            response = client.post(
                "/api/accounts/oauth/finish",
                headers=AUTH_HEADERS,
                json={"session_id": "session-1", "callback": "code"},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertNotIn("session-1", service._sessions)
        add_items.assert_called_once()
        refresh_accounts.assert_called_once_with(["access-token"])
        self.assertEqual(commit_finish.call_count, 1)
        self.assertTrue(commit_finish.call_args.args[2])

    def test_finish_persistence_failure_does_not_consume_pkce_session(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        app = FastAPI()
        app.include_router(accounts_module.create_router())
        client = TestClient(app, raise_server_exceptions=False)
        tokens = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }

        with (
            mock.patch.object(accounts_module, "oauth_login_service", service),
            mock.patch.object(service, "_exchange_code", return_value=tokens),
            mock.patch.object(
                accounts_module.account_service,
                "add_account_items",
                side_effect=OSError("disk full"),
            ),
        ):
            response = client.post(
                "/api/accounts/oauth/finish",
                headers=AUTH_HEADERS,
                json={"session_id": "session-1", "callback": "code"},
            )

        self.assertEqual(response.status_code, 500)
        self.assertIn("session-1", service._sessions)
        self.assertFalse(service._sessions["session-1"].get("_exchange_in_flight", False))
        with mock.patch.object(service, "_exchange_code", return_value=tokens):
            retry_result = service.finish("session-1", "code")
        self.assertEqual(retry_result, tokens)
        service.abort_finish("session-1", "code", retry_result.claim_id)

    def test_finish_cancellation_releases_pkce_persistence_claim(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        router = accounts_module.create_router()
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/accounts/oauth/finish"
        )
        tokens = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }

        async def invoke() -> None:
            with (
                mock.patch.object(accounts_module, "oauth_login_service", service),
                mock.patch.object(accounts_module, "require_admin_async", return_value={}),
                mock.patch.object(service, "_exchange_code", return_value=tokens),
                mock.patch.object(
                    accounts_module.account_service,
                    "add_account_items",
                    side_effect=asyncio.CancelledError(),
                ),
            ):
                await endpoint(
                    accounts_module.OAuthLoginFinishRequest(
                        session_id="session-1",
                        callback="code",
                    ),
                    "Bearer chatgpt2api",
                )

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(invoke())
        self.assertIn("session-1", service._sessions)
        self.assertFalse(service._sessions["session-1"].get("_exchange_in_flight", False))

    def test_finish_cancellation_during_commit_consumes_persisted_claim(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        router = accounts_module.create_router()
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/accounts/oauth/finish"
        )
        tokens = {
            "access_token": "access-token",
            "refresh_token": "refresh-token",
            "id_token": "id-token",
        }
        calls = 0

        async def fake_run_management_io(function, *args, **kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                service._sessions["session-1"]["_exchange_in_flight"] = True
                service._sessions["session-1"]["_finish_claim_id"] = args[2]
                return oauth_module.OAuthFinishResult(tokens, args[2])
            if calls == 2:
                return {"added": 1, "skipped": 0, "items": []}
            if calls == 3:
                raise asyncio.CancelledError()
            raise AssertionError("refresh must not run after commit cancellation")

        async def invoke() -> None:
            with (
                mock.patch.object(accounts_module, "oauth_login_service", service),
                mock.patch.object(accounts_module, "require_admin_async", return_value={}),
                mock.patch.object(accounts_module, "run_management_io", side_effect=fake_run_management_io),
            ):
                await endpoint(
                    accounts_module.OAuthLoginFinishRequest(
                        session_id="session-1",
                        callback="code",
                    ),
                    "Bearer chatgpt2api",
                )

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(invoke())
        self.assertNotIn("session-1", service._sessions)

    def test_finish_cancellation_during_exchange_releases_claim(self) -> None:
        service = oauth_module.OAuthLoginService()
        service._sessions["session-1"] = {
            "code_verifier": "verifier",
            "state": "session-1.state",
            "created_at": oauth_module.time.time(),
            "redirect_uri": "https://example.test/callback",
        }
        router = accounts_module.create_router()
        endpoint = next(
            route.endpoint
            for route in router.routes
            if getattr(route, "path", "") == "/api/accounts/oauth/finish"
        )

        async def cancelled_run(function, *args, **kwargs):
            if function is service.finish:
                service._sessions["session-1"]["_exchange_in_flight"] = True
                service._sessions["session-1"]["_finish_claim_id"] = args[2]
            raise asyncio.CancelledError()

        async def invoke() -> None:
            with (
                mock.patch.object(accounts_module, "oauth_login_service", service),
                mock.patch.object(accounts_module, "require_admin_async", return_value={}),
                mock.patch.object(accounts_module, "run_management_io", side_effect=cancelled_run),
            ):
                await endpoint(
                    accounts_module.OAuthLoginFinishRequest(
                        session_id="session-1",
                        callback="code",
                    ),
                    "Bearer chatgpt2api",
                )

        with self.assertRaises(asyncio.CancelledError):
            asyncio.run(invoke())
        self.assertIn("session-1", service._sessions)
        self.assertFalse(service._sessions["session-1"].get("_exchange_in_flight", False))


if __name__ == "__main__":
    unittest.main()
