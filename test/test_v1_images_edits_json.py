from __future__ import annotations

import base64
import io
import os
import socket
import unittest
from unittest import mock

os.environ.setdefault("CHATGPT2API_AUTH_KEY", "chatgpt2api")

from fastapi import FastAPI
from fastapi.testclient import TestClient
from PIL import Image

import api.ai as ai_module
import api.image_inputs as image_inputs_module
import services.remote_image as remote_image_module
from services.proxy_service import proxy_settings
from curl_cffi import CurlOpt
from test.fixtures.image_inputs import image_fixture_bytes

AUTH_HEADERS = {"Authorization": "Bearer chatgpt2api"}
PNG_BYTES = image_fixture_bytes("image.png")
PNG_SECOND_BYTES = image_fixture_bytes("image_edit.png")
_jpeg_buffer = io.BytesIO()
Image.new("RGB", (16, 16), (80, 120, 160)).save(_jpeg_buffer, format="JPEG")
JPEG_BYTES = _jpeg_buffer.getvalue()
PNG_DATA_URL = "data:image/png;base64," + base64.b64encode(PNG_BYTES).decode("ascii")
JPEG_DATA_URL = "data:image/jpeg;base64," + base64.b64encode(JPEG_BYTES).decode("ascii")
REMOTE_PNG = image_fixture_bytes("image.png")


class _FakeResponse:
    def __init__(self, status_code=200, headers=None, chunks=()):
        self.status_code = status_code
        self.headers = headers or {}
        self._chunks = list(chunks)
        self.iterated = False
        self.closed = False

    def iter_content(self, chunk_size=None):
        self.iterated = True
        yield from self._chunks

    def close(self):
        self.closed = True


class _FakeSession:
    instances = []
    responses = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.requests = []
        self.closed = False
        type(self).instances.append(self)

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return type(self).responses.pop(0)

    def close(self):
        self.closed = True


class ImageEditsJsonApiTests(unittest.TestCase):
    def setUp(self):
        self.calls = []
        _FakeSession.instances = []
        _FakeSession.responses = []

        def fake_handle(payload):
            self.calls.append(payload)
            return {"created": 1, "data": [{"b64_json": "ZmFrZQ=="}]}

        self.handle_patcher = mock.patch.object(ai_module.openai_v1_image_edit, "handle", fake_handle)
        self.filter_patcher = mock.patch.object(ai_module, "filter_or_log", mock.AsyncMock())
        self.handle_patcher.start()
        self.filter_patcher.start()
        self.addCleanup(self.handle_patcher.stop)
        self.addCleanup(self.filter_patcher.stop)

        app = FastAPI()
        app.include_router(ai_module.create_router())
        self.client = TestClient(app)

    def test_json_model_omitted_uses_existing_default_logic(self):
        response = self.client.post("/v1/images/edits", headers=AUTH_HEADERS, json={"prompt": "未传 model", "image": PNG_DATA_URL})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.calls[0]["model"], "gpt-image-2")

    def test_json_model_is_not_overwritten_when_provided(self):
        response = self.client.post(
            "/v1/images/edits",
            headers=AUTH_HEADERS,
            json={"model": "codex-gpt-image-2", "prompt": "保留 model", "image": PNG_DATA_URL},
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.calls[0]["model"], "codex-gpt-image-2")

    def test_image_edit_accepts_json_image_url(self):
        response = self.client.post(
            "/v1/images/edits",
            headers=AUTH_HEADERS,
            json={
                "model": "gpt-image-2",
                "prompt": "把图片改成夜景风格",
                "n": 1,
                "size": "1024x1536",
                "response_format": "b64_json",
                "images": [{"image_url": PNG_DATA_URL}],
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        payload = self.calls[0]
        self.assertEqual(payload["images"], [(PNG_BYTES, "image_url.png", "image/png")])
        self.assertEqual(payload["size"], "1024x1536")

    def test_image_edit_accepts_json_multiple_images_and_b64_json(self):
        response = self.client.post(
            "/v1/images/edits",
            headers=AUTH_HEADERS,
            json={
                "prompt": "把两张图合成海报",
                "images": [
                    PNG_DATA_URL,
                    {"b64_json": base64.b64encode(JPEG_BYTES).decode("ascii"), "mime_type": "image/jpeg", "filename": "two.jpg"},
                    {"image_url": {"url": JPEG_DATA_URL}},
                ],
            },
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.calls[0]["images"], [
            (PNG_BYTES, "image_url.png", "image/png"),
            (JPEG_BYTES, "two.jpg", "image/jpeg"),
            (JPEG_BYTES, "image_url.jpg", "image/jpeg"),
        ])

    def test_image_edit_keeps_original_multipart_multiple_image_logic(self):
        response = self.client.post(
            "/v1/images/edits",
            headers=AUTH_HEADERS,
            data={"prompt": "multipart 多图仍然可用", "model": "gpt-image-2", "n": "1"},
            files=[
                ("image", ("one.png", PNG_BYTES, "image/png")),
                ("image", ("two.jpg", JPEG_BYTES, "image/jpeg")),
                ("image[]", ("three.png", PNG_SECOND_BYTES, "image/png")),
            ],
        )
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.calls[0]["images"], [
            (PNG_BYTES, "one.png", "image/png"),
            (JPEG_BYTES, "two.jpg", "image/jpeg"),
            (PNG_SECOND_BYTES, "three.png", "image/png"),
        ])

    def test_image_edit_rejects_json_without_image(self):
        response = self.client.post("/v1/images/edits", headers=AUTH_HEADERS, json={"prompt": "缺少图片"})
        self.assertEqual(response.status_code, 400, response.text)
        self.assertIn("image file or image_url is required", response.text)

    def test_image_edit_rejects_private_or_malformed_remote_urls_without_network(self):
        remote_urls = [
            "http://127.0.0.1/image.png",
            "https://localhost/image.png",
            "http://169.254.169.254/latest/meta-data/",
            "https://10.0.0.1/image.png",
            "https://192.168.1.1/image.png",
            "http://[::1]/image.png",
            "https://[::ffff:127.0.0.1]/image.png",
            "https://user:opaque-secret@example.test/image.png",
            "https:///missing-authority/image.png",
            "https://public.example.test:",
            "ftp://public.example.test/image.png",
        ]
        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(
                remote_image_module.socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))],
            ) as getaddrinfo,
        ):
            responses = [
                self.client.post(
                    "/v1/images/edits",
                    headers=AUTH_HEADERS,
                    json={"prompt": "拒绝远程图片", "images": [{"image_url": url}]},
                )
                for url in remote_urls
            ]
        for response in responses:
            self.assertEqual(response.status_code, 400, response.text)
            self.assertNotIn("opaque-secret", response.text)
        self.assertFalse(_FakeSession.instances)
        self.assertGreaterEqual(getaddrinfo.call_count, 1)

    def test_image_edit_rejects_dns_resolution_with_any_private_address(self):
        def fake_getaddrinfo(host, port, *args, **kwargs):
            return [
                (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port)),
                (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", port, 0, 0)),
            ]

        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(remote_image_module.socket, "getaddrinfo", side_effect=fake_getaddrinfo),
        ):
            response = self.client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={"prompt": "拒绝 DNS 私网地址", "images": [{"image_url": "https://dns.example.test/image.png"}]},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertFalse(_FakeSession.instances)

    def test_image_edit_does_not_follow_public_redirect_to_private_target(self):
        redirect_response = _FakeResponse(
            status_code=302,
            headers={"location": "http://127.0.0.1/internal.png"},
        )
        _FakeSession.responses = [redirect_response]
        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(
                remote_image_module.socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
            ),
        ):
            response = self.client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={"prompt": "拒绝重定向", "images": [{"image_url": "https://public.example.test/image.png"}]},
            )
        self.assertEqual(response.status_code, 400, response.text)
        self.assertNotIn("127.0.0.1", response.text)
        self.assertEqual(len(_FakeSession.instances), 1)
        self.assertEqual(len(_FakeSession.instances[0].requests), 1)
        self.assertTrue(redirect_response.closed)

    def test_image_edit_revalidates_and_repins_each_public_redirect(self):
        first = _FakeResponse(
            status_code=302,
            headers={"location": "https://second.example.test/final.png"},
        )
        second = _FakeResponse(
            headers={"content-type": "image/png"},
            chunks=[REMOTE_PNG],
        )
        _FakeSession.responses = [first, second]
        dns_calls = []

        def fake_getaddrinfo(host, port, *args, **kwargs):
            dns_calls.append((host, port))
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]

        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(remote_image_module.socket, "getaddrinfo", side_effect=fake_getaddrinfo),
        ):
            response = self.client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={"prompt": "逐跳校验公开重定向", "images": [{"image_url": "https://first.example.test/image.png"}]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(dns_calls, [("first.example.test", 443), ("second.example.test", 443)])
        self.assertEqual([item.requests[0][0] for item in _FakeSession.instances], [
            "https://first.example.test/image.png",
            "https://second.example.test/final.png",
        ])
        self.assertEqual(
            _FakeSession.instances[1].kwargs["curl_options"][CurlOpt.RESOLVE],
            ["second.example.test:443:93.184.216.34"],
        )
        self.assertTrue(first.closed)
        self.assertTrue(second.closed)

    def test_image_edit_rejects_https_to_http_downgrade_before_second_request(self):
        response_body = _FakeResponse(
            status_code=302,
            headers={"location": "http://second.example.test/final.png"},
        )
        _FakeSession.responses = [response_body]
        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(
                remote_image_module.socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
            ) as getaddrinfo,
        ):
            response = self.client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={"prompt": "拒绝协议降级", "images": [{"image_url": "https://first.example.test/image.png"}]},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(getaddrinfo.call_count, 1)
        self.assertEqual(len(_FakeSession.instances), 1)
        self.assertTrue(response_body.closed)

    def test_image_edit_enforces_redirect_hop_limit(self):
        _FakeSession.responses = [
            _FakeResponse(status_code=302, headers={"location": f"https://hop-{index}.example.test/image.png"})
            for index in range(4)
        ]
        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(
                remote_image_module.socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
            ),
        ):
            response = self.client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={"prompt": "限制重定向层数", "images": [{"image_url": "https://hop-start.example.test/image.png"}]},
            )

        self.assertEqual(response.status_code, 400, response.text)
        self.assertEqual(len(_FakeSession.instances), 4)
        self.assertTrue(all(instance.requests for instance in _FakeSession.instances))

    def test_image_edit_accepts_public_remote_png_with_pinned_stream(self):
        response_body = _FakeResponse(
            headers={"content-type": "image/png", "content-length": str(len(REMOTE_PNG))},
            chunks=[REMOTE_PNG[:5], REMOTE_PNG[5:]],
        )
        _FakeSession.responses = [response_body]
        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(
                remote_image_module.socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
            ),
        ):
            response = self.client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={"prompt": "接受公开图片 URL", "images": [{"image_url": "https://public.example.test/image.png"}]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.calls[0]["images"], [(REMOTE_PNG, "image.png", "image/png")])
        self.assertTrue(response_body.iterated)
        self.assertTrue(response_body.closed)
        session = _FakeSession.instances[0]
        resolve = session.kwargs["curl_options"][CurlOpt.RESOLVE]
        self.assertEqual(resolve, ["public.example.test:443:93.184.216.34"])
        self.assertFalse(session.requests[0][1]["allow_redirects"])
        self.assertTrue(session.requests[0][1]["stream"])

    def test_image_edit_remote_fetch_ignores_configured_proxy(self):
        response_body = _FakeResponse(
            headers={"content-type": "image/png"},
            chunks=[REMOTE_PNG],
        )
        _FakeSession.responses = [response_body]
        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(
                remote_image_module.socket,
                "getaddrinfo",
                return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
            ),
            mock.patch.object(
                proxy_settings,
                "build_session_kwargs",
                return_value={
                    "proxy": "http://proxy-user:proxy-secret@proxy.example.test:8080",
                    "proxies": {"https": "http://proxy.example.test:8080"},
                },
            ) as build_session_kwargs,
        ):
            response = self.client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={"prompt": "直连远程图片", "images": [{"image_url": "https://public.example.test/image.png"}]},
            )

        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(self.calls[0]["images"], [(REMOTE_PNG, "image.png", "image/png")])
        build_session_kwargs.assert_not_called()
        session_kwargs = _FakeSession.instances[0].kwargs
        self.assertNotIn("proxy", session_kwargs)
        self.assertNotIn("proxies", session_kwargs)
        self.assertEqual(
            session_kwargs["curl_options"][CurlOpt.RESOLVE],
            ["public.example.test:443:93.184.216.34"],
        )

    def test_image_edit_canonicalizes_request_host_to_resolve_pin(self):
        cases = [
            (
                "https://Public.Example.Test./nested/image.png?sig=one",
                "https://public.example.test/nested/image.png?sig=one",
                "public.example.test",
            ),
            (
                "https://Bücher.Example./nested/image.png?sig=two",
                "https://xn--bcher-kva.example/nested/image.png?sig=two",
                "xn--bcher-kva.example",
            ),
        ]
        for source, expected_url, expected_host in cases:
            with self.subTest(source=source):
                response_body = _FakeResponse(
                    headers={"content-type": "image/png"},
                    chunks=[REMOTE_PNG],
                )
                _FakeSession.instances = []
                _FakeSession.responses = [response_body]
                dns_calls = []

                def fake_getaddrinfo(host, port, *args, **kwargs):
                    dns_calls.append((host, port))
                    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]

                with (
                    mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
                    mock.patch.object(remote_image_module.socket, "getaddrinfo", side_effect=fake_getaddrinfo),
                ):
                    response = self.client.post(
                        "/v1/images/edits",
                        headers=AUTH_HEADERS,
                        json={"prompt": "规范化解析域名", "images": [{"image_url": source}]},
                    )

                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(dns_calls, [(expected_host, 443)])
                session = _FakeSession.instances[0]
                self.assertEqual(session.requests[0][0], expected_url)
                self.assertEqual(
                    session.kwargs["curl_options"][CurlOpt.RESOLVE],
                    [f"{expected_host}:443:93.184.216.34"],
                )

    def test_image_edit_preserves_non_default_port_path_query_and_ipv6_pin(self):
        response_body = _FakeResponse(
            headers={"content-type": "image/png"},
            chunks=[REMOTE_PNG],
        )
        _FakeSession.responses = [response_body]
        with (
            mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
            mock.patch.object(
                remote_image_module.socket,
                "getaddrinfo",
                return_value=[
                    (
                        socket.AF_INET6,
                        socket.SOCK_STREAM,
                        6,
                        "",
                        ("2001:4860:4860::8888", 8443, 0, 0),
                    )
                ],
            ),
        ):
            response = self.client.post(
                "/v1/images/edits",
                headers=AUTH_HEADERS,
                json={
                    "prompt": "保留 IPv6 authority",
                    "images": [{
                        "image_url": "https://ipv6.example.test:8443/nested/image.png?sig=three&v=1",
                    }],
                },
            )

        self.assertEqual(response.status_code, 200, response.text)
        session = _FakeSession.instances[0]
        self.assertEqual(
            session.requests[0][0],
            "https://ipv6.example.test:8443/nested/image.png?sig=three&v=1",
        )
        self.assertEqual(
            session.kwargs["curl_options"][CurlOpt.RESOLVE],
            ["ipv6.example.test:8443:[2001:4860:4860::8888]"],
        )

    def test_image_edit_rejects_remote_content_type_or_decoder_mismatch(self):
        cases = [
            _FakeResponse(headers={"content-type": "application/octet-stream"}, chunks=[REMOTE_PNG]),
            _FakeResponse(headers={"content-type": "image/jpeg"}, chunks=[REMOTE_PNG]),
            _FakeResponse(headers={"content-type": "image/png"}, chunks=[b"not-a-real-image"]),
        ]
        for response_body in cases:
            with self.subTest(content_type=response_body.headers.get("content-type")):
                _FakeSession.instances = []
                _FakeSession.responses = [response_body]
                with (
                    mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
                    mock.patch.object(
                        remote_image_module.socket,
                        "getaddrinfo",
                        return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
                    ),
                ):
                    response = self.client.post(
                        "/v1/images/edits",
                        headers=AUTH_HEADERS,
                        json={"prompt": "拒绝错误图片类型", "images": [{"image_url": "https://public.example.test/image.png"}]},
                    )
                self.assertEqual(response.status_code, 400, response.text)
                self.assertTrue(response_body.closed)

    def test_image_edit_enforces_declared_and_actual_remote_size_limits(self):
        oversized_header = _FakeResponse(
            headers={"content-type": "image/png", "content-length": "9"},
            chunks=[b"ignored"],
        )
        overflowing_body = _FakeResponse(
            headers={"content-type": "image/png"},
            chunks=[b"123456789"],
        )
        for response_body in (oversized_header, overflowing_body):
            with self.subTest(response=response_body):
                _FakeSession.instances = []
                _FakeSession.responses = [response_body]
                with (
                    mock.patch.object(image_inputs_module, "MAX_IMAGE_REFERENCE_BYTES", 8),
                    mock.patch.object(remote_image_module.requests, "Session", _FakeSession),
                    mock.patch.object(
                        remote_image_module.socket,
                        "getaddrinfo",
                        return_value=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))],
                    ),
                ):
                    response = self.client.post(
                        "/v1/images/edits",
                        headers=AUTH_HEADERS,
                        json={"prompt": "限制远程图片大小", "images": [{"image_url": "https://public.example.test/image.png"}]},
                    )
                self.assertEqual(response.status_code, 400, response.text)
                self.assertTrue(response_body.closed)

    def test_image_edit_rejects_json_n_out_of_range(self):
        for count in (0, 11):
            with self.subTest(n=count):
                self.calls.clear()
                response = self.client.post(
                    "/v1/images/edits",
                    headers=AUTH_HEADERS,
                    json={"prompt": "n 越界", "n": count, "image": PNG_DATA_URL},
                )
                self.assertEqual(response.status_code, 400, response.text)
                self.assertFalse(self.calls)


if __name__ == "__main__":
    unittest.main()
