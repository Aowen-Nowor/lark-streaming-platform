"""token 管理单测 — 惰性获取 / 提前刷新 / 并发单飞 / 凭证错误不重试."""

from __future__ import annotations

import asyncio
import time

import httpx
import pytest

from lark_streaming.transport.errors import FeishuAPIError
from lark_streaming.transport.token import TenantTokenManager, _REFRESH_MARGIN_SEC

from .conftest import TOKEN_RESPONSE, api_err, json_response


def patch_expire(expire: int) -> dict:
    body = dict(TOKEN_RESPONSE)
    body["expire"] = expire
    return body


class TestBasicFlow:
    async def test_fetch_and_cache(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return json_response(patch_expire(7200))

        http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        mgr = TenantTokenManager(make_cred(), http=http)
        try:
            t1 = await mgr.get_token()
            t2 = await mgr.get_token()
            assert t1 == t2 == "t-g104testtoken"
            assert len(calls) == 1  # 缓存生效
        finally:
            await mgr.aclose()

    async def test_refresh_margin_applied(self):
        http = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: json_response(patch_expire(7200))))
        mgr = TenantTokenManager(make_cred(), http=http)
        try:
            await mgr.get_token()
            # 内部到期时间 = expire - 300s 裕度
            assert mgr._expires_at <= time.monotonic() + (7200 - _REFRESH_MARGIN_SEC + 1)
        finally:
            await mgr.aclose()

    async def test_invalidate_forces_refresh(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return json_response(patch_expire(7200))

        http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        mgr = TenantTokenManager(make_cred(), http=http)
        try:
            await mgr.get_token()
            await mgr.invalidate()
            await mgr.get_token()
            assert len(calls) == 2
        finally:
            await mgr.aclose()


class TestErrorPaths:
    async def test_credential_error_no_retry(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            return json_response(api_err(10003, "invalid app_id"))

        http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        mgr = TenantTokenManager(make_cred(), http=http)
        try:
            with pytest.raises(FeishuAPIError):
                await mgr.get_token()
            assert len(calls) == 1  # 凭证错误不重试
        finally:
            await mgr.aclose()

    async def test_network_error_retries_then_raises(self):
        calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            raise httpx.ConnectError("down")

        http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        mgr = TenantTokenManager(make_cred(), http=http)
        try:
            with pytest.raises(FeishuAPIError):
                await mgr.get_token()
            assert len(calls) == 3
        finally:
            await mgr.aclose()


class TestConcurrency:
    async def test_concurrent_single_flight(self):
        calls = []

        async def slow_handler(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            await asyncio.sleep(0.05)
            return json_response(patch_expire(7200))

        transport = httpx.MockTransport(slow_handler)
        http = httpx.AsyncClient(transport=transport)
        mgr = TenantTokenManager(make_cred(), http=http)
        try:
            tokens = await asyncio.gather(*(mgr.get_token() for _ in range(8)))
            assert len(set(tokens)) == 1
            assert len(calls) == 1  # 单飞：只刷新一次
        finally:
            await mgr.aclose()


def make_cred():
    from lark_streaming.credentials import FeishuCredentials

    return FeishuCredentials(app_id="cli_test", app_secret="secret_test")


def test_credentials_secret_hidden_in_repr():
    """audit #6：app_secret 设 repr=False——repr/异常日志不泄漏明文."""
    from lark_streaming.credentials import FeishuCredentials

    cred = FeishuCredentials(app_id="cli_x", app_secret="supersecret123")
    r = repr(cred)
    assert "supersecret123" not in r
    assert "cli_x" in r  # app_id 不敏感可留
    # 语义可用性不受影响：字段仍可正常读取与校验
    assert cred.app_secret == "supersecret123"
    assert cred.is_complete() is True
