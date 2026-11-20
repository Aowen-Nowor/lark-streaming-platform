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
