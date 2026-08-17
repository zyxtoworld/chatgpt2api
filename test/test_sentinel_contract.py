from __future__ import annotations

import json
import unittest
from unittest import mock

import utils.sentinel as sentinel_module


class SentinelResponseContractTests(unittest.TestCase):
    def test_required_pow_rejects_non_hex_difficulty_before_solving(self) -> None:
        class Response:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, chunk_size=None):
                del chunk_size
                yield json.dumps({
                    "token": "challenge",
                    "proofofwork": {"required": True, "seed": "seed", "difficulty": "gg"},
                }).encode("utf-8")

            def close(self) -> None:
                self.closed = True

        class Session:
            def post(self, *_args: object, **_kwargs: object) -> Response:
                return response

        response = Response()
        with mock.patch.object(sentinel_module.SentinelTokenGenerator, "generate_token", return_value="pow-token") as solve:
            with self.assertRaisesRegex(RuntimeError, "sentinel challenge is invalid"):
                sentinel_module.build_sentinel_token(Session(), "device-1", "password_verify")

        solve.assert_not_called()
        self.assertTrue(response.closed)

    def test_sentinel_request_streams_bounded_json_and_closes_response(self) -> None:
        class Response:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False
                self.iterated = False
                self.chunk_size = None

            def iter_content(self, chunk_size=None):
                self.iterated = True
                self.chunk_size = chunk_size
                body = json.dumps({"token": "challenge", "proofofwork": {}}).encode("utf-8")
                yield body[:7]
                yield body[7:]

            def close(self) -> None:
                self.closed = True

        class Session:
            def __init__(self, response: Response) -> None:
                self.response = response
                self.kwargs: dict[str, object] = {}

            def post(self, *_args: object, **kwargs: object) -> Response:
                self.kwargs = kwargs
                return self.response

        response = Response()
        session = Session(response)
        sentinel_value, cookie_value = sentinel_module.build_sentinel_token(
            session,
            "device-1",
            "password_verify",
        )

        self.assertIn('"c":"challenge"', sentinel_value)
        self.assertEqual(cookie_value, "0challenge")
        self.assertTrue(session.kwargs["verify"])
        self.assertTrue(session.kwargs["stream"])
        self.assertTrue(response.iterated)
        self.assertTrue(response.closed)

    def test_sentinel_oversized_body_falls_back_and_closes_response(self) -> None:
        class Response:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False
                self.iterated = False

            def iter_content(self, chunk_size=None):
                self.iterated = True
                yield b"x" * (1 * 1024 * 1024 + 1)

            def close(self) -> None:
                self.closed = True

        class Session:
            def __init__(self, response: Response) -> None:
                self.response = response
                self.kwargs: dict[str, object] = {}

            def post(self, *_args: object, **kwargs: object) -> Response:
                self.kwargs = kwargs
                return self.response

        response = Response()
        session = Session(response)
        fallback, cookie_value = sentinel_module.build_sentinel_token(session, "device-1", "password_verify")

        self.assertEqual(cookie_value, "")
        self.assertIn('"t":""', fallback)
        self.assertTrue(session.kwargs["stream"])
        self.assertTrue(response.iterated)
        self.assertTrue(response.closed)

    def test_required_pow_rejects_container_fields_before_solving(self) -> None:
        class Response:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, chunk_size=None):
                del chunk_size
                yield json.dumps({
                    "token": "challenge",
                    "proofofwork": {
                        "required": True,
                        "seed": {"canary": "sentinel-secret"},
                        "difficulty": ["canary"],
                    },
                }).encode("utf-8")

            def close(self) -> None:
                self.closed = True

        class Session:
            def __init__(self, response: Response) -> None:
                self.response = response

            def post(self, *_args: object, **_kwargs: object) -> Response:
                return self.response

        response = Response()
        with mock.patch.object(sentinel_module.SentinelTokenGenerator, "generate_token", return_value="pow-token") as solve:
            with self.assertRaisesRegex(RuntimeError, "sentinel challenge is invalid"):
                sentinel_module.build_sentinel_token(Session(response), "device-1", "password_verify")
        solve.assert_not_called()
        self.assertTrue(response.closed)

    def test_malformed_pow_required_flag_does_not_skip_the_challenge(self) -> None:
        class Response:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, chunk_size=None):
                del chunk_size
                yield json.dumps({
                    "token": "challenge",
                    "proofofwork": {"required": "true", "seed": "seed", "difficulty": "0"},
                }).encode("utf-8")

            def close(self) -> None:
                self.closed = True

        class Session:
            def post(self, *_args: object, **_kwargs: object) -> Response:
                return response

        response = Response()
        with mock.patch.object(sentinel_module.SentinelTokenGenerator, "generate_token", return_value="pow-token") as solve:
            with self.assertRaisesRegex(RuntimeError, "sentinel challenge is invalid"):
                sentinel_module.build_sentinel_token(Session(), "device-1", "password_verify")
        solve.assert_not_called()
        self.assertTrue(response.closed)

    def test_malformed_pow_container_does_not_skip_the_challenge(self) -> None:
        class Response:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, chunk_size=None):
                del chunk_size
                yield json.dumps({"token": "challenge", "proofofwork": ["canary"]}).encode("utf-8")

            def close(self) -> None:
                self.closed = True

        class Session:
            def post(self, *_args: object, **_kwargs: object) -> Response:
                return response

        response = Response()
        with self.assertRaisesRegex(RuntimeError, "sentinel challenge is invalid"):
            sentinel_module.build_sentinel_token(Session(), "device-1", "password_verify")
        self.assertTrue(response.closed)

    def test_sentinel_token_has_a_field_bound_before_cookie_creation(self) -> None:
        class Response:
            status_code = 200

            def __init__(self) -> None:
                self.closed = False

            def iter_content(self, chunk_size=None):
                del chunk_size
                yield json.dumps({"token": "x" * 4097, "proofofwork": {}}).encode("utf-8")

            def close(self) -> None:
                self.closed = True

        class Session:
            def post(self, *_args: object, **_kwargs: object) -> Response:
                return response

        response = Response()
        with self.assertRaisesRegex(RuntimeError, "sentinel response is invalid"):
            sentinel_module.build_sentinel_token(Session(), "device-1", "password_verify")
        self.assertTrue(response.closed)
