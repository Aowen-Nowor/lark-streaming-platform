"""harness 测试夹具 — L2 台架（04-knowledge-freeze §5）.

- ``harness``：默认 EngineSettings 的函数级台架（autouse=False）；
- ``make_harness``：自定义 settings / clock 的工厂（注册表统一在夹具
  收口 close，测试失败也不泄漏 httpx 客户端与引擎任务）。
"""

from __future__ import annotations

import pytest

from lark_streaming.engine import EngineSettings

from .framework import Harness

__all__ = ["Harness", "harness", "make_harness"]


@pytest.fixture
async def harness():
    """默认设置台架（函数级；yield 后统一 close）。"""
    h = Harness()
    try:
        yield h
    finally:
        await h.close()


@pytest.fixture
async def make_harness():
    """自定义设置台架工厂：``h = await make_harness(clarify_window_sec=0)``."""
    made: list[Harness] = []

    async def _make(**kw):
        clock = kw.pop("clock", None)
        h = Harness(settings=EngineSettings(**kw), clock=clock)
        made.append(h)
        return h

    try:
        yield _make
    finally:
        for h in made:
            await h.close()
