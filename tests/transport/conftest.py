"""transport 测试共享台架 — httpx MockTransport 故障注入.

测试不依赖真实网络与真实路径（02-engine §10 L1 纪律）；路径正确性由
L3 真飞书验收校准。
"""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Any

import httpx
import pytest

from lark_streaming.credentials import FeishuCredentials

TOKEN_RESPONSE = {
    "code": 0,
    "msg": "success",
    "data": {},
    "tenant_access_token": "t-g104testtoken",
    "expire": 7200,
}


def api_ok(data: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"code": 0, "msg": "success", "data": data or {}}


def api_err(code: int, msg: str) -> dict[str, Any]:
    return {"code": code, "msg": msg, "data": {}}


def make_credentials() -> FeishuCredentials:
    return FeishuCredentials(app_id="cli_test", app_secret="secret_test")


class Route:
    """一条路由：路径谓词 → 响应工厂（可记录调用次数与请求体）."""

    def __init__(
        self,
        match: Callable[[str, str], bool],
        respond: Callable[[httpx.Request], httpx.Response],
    ) -> None:
        self.match = match
        self.respond = respond
        self.calls: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        return self.respond(request)


def json_response(body: dict[str, Any], status_code: int = 200) -> httpx.Response:
    return httpx.Response(status_code, json=body)


def make_client(*routes: Route) -> httpx.AsyncClient:
    """构造 MockTransport 驱动的 AsyncClient（token 端点默认放行）."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        method = request.method
        if path.endswith("/auth/v3/tenant_access_token/internal"):
            return json_response(TOKEN_RESPONSE)
        for route in routes:
            if route.match(method, path):
                return route(request)
        return json_response(api_err(-1, f"unrouted {method} {path}"))

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


@pytest.fixture
def credentials() -> FeishuCredentials:
    return make_credentials()


def body_of(request: httpx.Request) -> dict[str, Any]:
    return json.loads(request.content.decode("utf-8") or "{}")
