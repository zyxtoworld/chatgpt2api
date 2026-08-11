from __future__ import annotations

from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api.errors import install_exception_handlers


SECRET = "http-exception-upstream-secret owner@example.com"


def test_openai_http_exception_does_not_reflect_untrusted_detail() -> None:
    app = FastAPI()
    install_exception_handlers(app)

    @app.get("/v1/opaque")
    async def opaque_error():
        raise HTTPException(
            status_code=400,
            detail={
                "error": {
                    "message": SECRET,
                    "type": "upstream_error",
                    "param": SECRET,
                    "code": SECRET,
                }
            },
        )

    response = TestClient(app).get("/v1/opaque")

    assert response.status_code == 400
    assert SECRET not in response.text
