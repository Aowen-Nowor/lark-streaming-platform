"""protocol 测试共享台架 — 复用 engine 层 FakeTransport/make_engine.

绑定层测试的引擎全部跑在 FakeTransport 上（L1 纪律：不依赖真实网络，
02-engine §10）；HTTP/WS 走 aiohttp TestServer 真 socket。
"""

from __future__ import annotations

import json
from collections.abc import AsyncIterator, Awaitable, Callable

import pytest

from tests.engine.conftest import (  # noqa: F401 — re-export 供各测试文件使用
    CHAT,
    TURN,
    FakeClock,
    FakeTransport,
    env,
    make_engine,
    make_settings,
)


@pytest.fixture
def transport() -> FakeTransport:
    return FakeTransport()


async def lines(*items: str | dict) -> AsyncIterator[str]:
    """注入用 stdin 行源（dict 自动序列化）."""
    for item in items:
        yield item if isinstance(item, str) else json.dumps(item, ensure_ascii=False)


def make_emit(out: list[dict]) -> Callable[[str], Awaitable[None]]:
    """注入用回执收集器（JSON 行 → dict 追加）。"""

    async def _emit(text: str) -> None:
        out.append(json.loads(text))

    return _emit


@pytest.fixture(autouse=True)
def _isolate_proxy_env(monkeypatch):
    """audit #9-S3：隔离出口代理环境变量——NO_PROXY 含方括号 IPv6 条目时
    httpx 0.28.x 代理配置解析失败，会在带代理的开发机/CI 上误报 11 个
    HTTP 绑定测试。这不是产品缺陷，但必须让新贡献者的首跑是绿的。"""
    for var in (
        "http_proxy", "https_proxy", "all_proxy",
        "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY",
        "no_proxy", "NO_PROXY",
    ):
        monkeypatch.delenv(var, raising=False)
