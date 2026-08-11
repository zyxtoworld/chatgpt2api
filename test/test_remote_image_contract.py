from __future__ import annotations

import socket
import unittest
from unittest import mock

from fastapi import HTTPException

import services.remote_image as remote_image
import services.image_payload as image_payload
from services.protocol import openai_v1_chat_complete, openai_v1_response
from test.fixtures.image_inputs import image_fixture_bytes
from utils.helper import normalize_json_edit_images


REMOTE_PNG = image_fixture_bytes("image.png")


class _FakeResponse:
    def __init__(self, headers=None, chunks=(), status_code=200):
        self.headers = headers or {}
        self._chunks = list(chunks)
        self.status_code = status_code
        self.closed = False

    def iter_content(self, chunk_size=None):
        yield from self._chunks

    def close(self):
        self.closed = True


class _FakeSession:
    instances = []
    responses = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.requests = []
        type(self).instances.append(self)

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return type(self).responses.pop(0)

    def close(self):
        return None


class RemoteImageContractTests(unittest.TestCase):
    def setUp(self) -> None:
        _FakeSession.instances = []
        _FakeSession.responses = []

    def _patch_transport(self, response):
        _FakeSession.responses = [response]
        return mock.patch.multiple(
            remote_image.requests,
            Session=_FakeSession,
        ), mock.patch.object(
            remote_image.socket,
            "getaddrinfo",
            return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
        )

    def _assert_public_protocol_image(self, parser, body) -> None:
        response = _FakeResponse(headers={"content-type": "image/png"}, chunks=[REMOTE_PNG])
        session_patch, dns_patch = self._patch_transport(response)
        with session_patch, dns_patch:
            _model, messages = parser(body)
        image_parts = [
            part
            for message in messages
            for part in message.get("content", [])
            if isinstance(part, dict) and part.get("type") == "image"
        ]
        self.assertEqual(image_parts, [{"type": "image", "data": REMOTE_PNG, "mime": "image/png"}])
        self.assertTrue(response.closed)

    def test_chat_completions_public_https_image_url_wiring(self) -> None:
        self._assert_public_protocol_image(
            openai_v1_chat_complete.text_chat_parts,
            {
                "model": "auto",
                "messages": [{
                    "role": "user",
                    "content": [
                        {"type": "text", "text": "Describe this"},
                        {"type": "image_url", "image_url": {"url": "https://public.example.test/image.png"}},
                    ],
                }],
            },
        )

    def test_responses_public_https_image_url_wiring(self) -> None:
        self._assert_public_protocol_image(
            openai_v1_response.text_response_parts,
            {
                "model": "auto",
                "input": [{
                    "type": "message",
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "Describe this"},
                        {"type": "input_image", "image_url": {"url": "https://public.example.test/image.png"}},
                    ],
                }],
            },
        )

    def test_json_edit_public_image_url_uses_same_downloader_and_keeps_filename(self) -> None:
        response = _FakeResponse(headers={"content-type": "image/png"}, chunks=[REMOTE_PNG])
        session_patch, dns_patch = self._patch_transport(response)
        with session_patch, dns_patch:
            images = normalize_json_edit_images(
                images=[{
                    "image_url": {"url": "https://public.example.test/image.png"},
                    "filename": "client-name.png",
                }],
            )
        self.assertEqual(images, [(REMOTE_PNG, "client-name.png", "image/png")])

    def test_remote_image_rejects_malformed_dns_authority_before_dns_or_session(self) -> None:
        bad_hosts = (
            "foo..bar.example",
            "foo_bar.example",
            "-foo.example",
            "foo-.example",
            "foo..",
            r"foo\bar.example",
            "a" * 254,
        )
        for host in bad_hosts:
            source = f"https://{host}/image.png"
            with self.subTest(host=host):
                with (
                    mock.patch.object(remote_image.requests, "Session") as session,
                    mock.patch.object(remote_image.socket, "getaddrinfo") as getaddrinfo,
                ):
                    with self.assertRaises(HTTPException) as raised:
                        remote_image.download_remote_image(source, max_bytes=10 * 1024 * 1024)
                self.assertEqual(raised.exception.status_code, 400)
                self.assertNotIn(host, str(raised.exception.detail))
                session.assert_not_called()
                getaddrinfo.assert_not_called()

    def test_remote_image_rejects_dimensions_before_pillow_decode(self) -> None:
        class HugeImage:
            format = "PNG"
            width = 5001
            height = 5001

            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return None

            def load(self):
                raise AssertionError("pixel limit must fail before decode")

        response = _FakeResponse(headers={"content-type": "image/png"}, chunks=[REMOTE_PNG])
        session_patch, dns_patch = self._patch_transport(response)
        with (
            session_patch,
            dns_patch,
            mock.patch.object(image_payload.Image, "open", return_value=HugeImage()),
        ):
            with self.assertRaises(HTTPException) as raised:
                remote_image.download_remote_image(
                    "https://public.example.test/image.png",
                    max_bytes=10 * 1024 * 1024,
                )
        self.assertEqual(raised.exception.status_code, 400)
        self.assertNotIn("public.example.test", str(raised.exception.detail))
        self.assertTrue(response.closed)


if __name__ == "__main__":
    unittest.main()
