"""重试策略单测 — 退避分道 / 网络重试 / CancelledError 穿透."""

from __future__ import annotations

import asyncio

import httpx
import pytest

from lark_streaming.transport.errors import FeishuAPIError
from lark_streaming.transport.retry import (
    RATE_LIMIT_RETRY_DELAYS,
    TRANSIENT_RETRY_DELAYS,
    retry_transient,
)

from .conftest import api_err  # noqa: F401 — re-export 便利


class TestRetryTransient:
    async def test_success_first_try(self):
        calls = []

        async def op():
            calls.append(1)
            return "ok"

        assert await retry_transient("op", op) == "ok"
        assert len(calls) == 1

    async def test_transient_retry_then_success(self, monkeypatch):
        attempts = []
        sleeps = []

        async def fake_sleep(d):
            sleeps.append(d)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)

        async def op():
            attempts.append(len(attempts))
            if len(attempts) < 3:
                raise FeishuAPIError("op: code=2200, msg=timeout", 2200)
            return "ok"

        assert await retry_transient("op", op) == "ok"
        assert len(attempts) == 3
        assert sleeps == list(TRANSIENT_RETRY_DELAYS[:2])

    async def test_rate_limit_uses_second_scale_delays(self, monkeypatch):
        sleeps = []

        async def fake_sleep(d):
            sleeps.append(d)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        attempts = []

        async def op():
            attempts.append(1)
            raise FeishuAPIError("op: code=99991400, msg=too many", 99991400)

        with pytest.raises(FeishuAPIError):
            await retry_transient("op", op)
        assert sleeps == list(RATE_LIMIT_RETRY_DELAYS)

    async def test_non_transient_raises_immediately(self, monkeypatch):
        sleeps = []

        async def fake_sleep(d):
            sleeps.append(d)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        calls = []

        async def op():
            calls.append(1)
            raise FeishuAPIError("op: code=300317, msg=conflict", 300317)

        with pytest.raises(FeishuAPIError):
            await retry_transient("op", op)
        assert len(calls) == 1  # 300317 归引擎自愈，不在传输重试范围
        assert sleeps == []

    async def test_card_size_never_retried(self):
        calls = []

        async def op():
            calls.append(1)
            raise FeishuAPIError("op: code=200860, msg=too large", 200860)

        with pytest.raises(FeishuAPIError):
            await retry_transient("op", op)
        assert len(calls) == 1  # 确定性拒收零重试（OPT-1c）

    async def test_network_error_retried(self, monkeypatch):
        sleeps = []

        async def fake_sleep(d):
            sleeps.append(d)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        attempts = []

        async def op():
            attempts.append(1)
            if len(attempts) < 2:
                raise httpx.ConnectError("conn refused")
            return "ok"

        assert await retry_transient("op", op) == "ok"
        assert len(attempts) == 2
        assert sleeps == [TRANSIENT_RETRY_DELAYS[0]]

    async def test_cancelled_error_propagates(self):
        async def op():
            raise asyncio.CancelledError()

        with pytest.raises(asyncio.CancelledError):
            await retry_transient("op", op)

    async def test_exhaustion_raises_last_error(self):
        attempts = []

        async def op():
            attempts.append(1)
            raise FeishuAPIError("op: code=2200, msg=timeout", 2200)

        with pytest.raises(FeishuAPIError):
            await retry_transient("op", op, max_retries=2)
        assert len(attempts) == 3  # 1 原始 + 2 重试
