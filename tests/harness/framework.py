"""Harness — L2 回合编排器（04-knowledge-freeze §5 mock 台架移植主体）.

编排职责（对应前代 ``tests/e2e/framework.py``）：
- 组装 MockFeishuREST → httpx.MockTransport → httpx.AsyncClient →
  FeishuTransport（**真实传输代码**，注入 mock HTTP）→ StreamingEngine
  （**真实编排代码**，注入 FakeClock 与生命周期收集器）；
- LSEP/1 信封构造 + ``full_turn`` 回合编排 + ack / 卡 / 生命周期断言辅助。

与 tests/engine/conftest.py 的 FakeTransport 台架互不复用（L2 的意义是
真实 transport 代码跑 mock HTTP）；FakeClock 与 settle 手法按该文件同款
实现复制进本模块，不做跨测试目录私有 import。

定时器语义（铁律）：引擎定时器走**真实** ``loop.call_later``；FakeClock
只影响 ``clock()`` 读数。需要定时器真实触发时用真实短 sleep（把对应
settings 调小），参照 tests/engine/test_lifecycle.py 手法，绝不
``sleep(300)``。
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from lark_streaming.credentials import FeishuCredentials
from lark_streaming.engine import (
    Ack,
    EngineSettings,
    LifecycleEvent,
    StreamingEngine,
)
from lark_streaming.transport import FeishuTransport

from .mock_feishu import MockCardState, MockFeishuREST

__all__ = ["CHAT", "TURN", "FakeClock", "Harness", "json_contains"]

CHAT = "oc_chat1"
TURN = "t_20260101_0001"


class FakeClock:
    """可控单调时钟（引擎 clock 注入点；与 tests/engine/conftest.py 同款）."""

    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def json_contains(node: Any, needle: str) -> bool:
    """递归文本包含断言（在卡片 JSON 内检索行文本 / 页脚字段）."""
    if isinstance(node, str):
        return needle in node
    if isinstance(node, dict):
        return any(json_contains(v, needle) for v in node.values())
    if isinstance(node, (list, tuple)):
        return any(json_contains(v, needle) for v in node)
    return False


class Harness:
    """L2 台架编排器：真实 engine + 真实 transport + mock HTTP.

    ``server`` 即 :class:`MockFeishuREST`（故障注入与观测面）；
    ``engine`` / ``transport`` 暴露给测试做状态断言与直调（seq 自愈
    验证等）。``close()`` 按依赖逆序收口：engine.aclose →
    transport.aclose → client.aclose。
    """

    def __init__(
        self,
        *,
        settings: EngineSettings | None = None,
        clock: FakeClock | None = None,
    ) -> None:
        self.server = MockFeishuREST()
        self.clock = clock or FakeClock()
        self.settings = settings or EngineSettings()
        self.client = httpx.AsyncClient(transport=httpx.MockTransport(self.server))
        self.credentials = FeishuCredentials("cli_mock", "secret_mock")
        self.transport = FeishuTransport(self.credentials, http=self.client)
        self.lifecycle: list[LifecycleEvent] = []
        self.engine = StreamingEngine(
            self.transport,
            settings=self.settings,
            clock=self.clock,
            on_lifecycle=self.lifecycle.append,
        )
        self._acks: list[Ack] = []
        self._frame_n = 0

    # ── 事件面 ────────────────────────────────────────────────────────────

    async def submit(
        self,
        event: str,
        *,
        chat: str = CHAT,
        turn: str = TURN,
        payload: dict[str, Any] | None = None,
        frame: str | None = None,
        seq: int | None = None,
    ) -> Ack:
        """构造 LSEP/1 信封（tests/engine/conftest.py env() 同款）并投递."""
        self._frame_n += 1
        fid = frame or f"f_{self._frame_n}_{event.replace('.', '_')}"
        raw: dict[str, Any] = {
            "v": 1,
            "event": event,
            "frame_id": fid,
            "chat_id": chat,
            "turn_id": turn,
            "payload": payload or {},
        }
        if seq is not None:
            raw["seq"] = seq
        ack = await self.engine.submit(raw)
        self._acks.append(ack)
        return ack

    async def full_turn(
        self,
        chunks: list[str],
        *,
        chat: str = CHAT,
        turn: str = TURN,
        title: str = "",
        tools: list[str] | None = None,
    ) -> list[Ack]:
        """完整回合：turn.start → tool.start×K（可选）→ answer.delta×N → answer.final."""
        acks = [
            await self.submit(
                "turn.start", chat=chat, turn=turn,
                payload={"title": title}, frame=f"{turn}_start",
            )
        ]
        seq = 0
        for i, tool in enumerate(tools or ()):
            seq += 1
            acks.append(await self.submit(
                "tool.start", chat=chat, turn=turn,
                payload={"tool": tool}, seq=seq, frame=f"{turn}_tool{i}",
            ))
        for i, chunk in enumerate(chunks):
            seq += 1
            acks.append(await self.submit(
                "answer.delta", chat=chat, turn=turn,
                payload={"delta": chunk}, seq=seq, frame=f"{turn}_d{i}",
            ))
        seq += 1
        acks.append(await self.submit(
            "answer.final", chat=chat, turn=turn,
            payload={"text": "".join(chunks)}, seq=seq, frame=f"{turn}_final",
        ))
        return acks

    # ── 断言辅助 ──────────────────────────────────────────────────────────

    def card(self, card_id: str) -> MockCardState:
        return self.server.cards[card_id]

    def last_card(self) -> MockCardState:
        return list(self.server.cards.values())[-1]

    @property
    def cards(self) -> dict[str, MockCardState]:
        return self.server.cards

    def acks_of(self, frame_id: str) -> list[Ack]:
        """该 frame_id 的全部回执（逐帧恰好一个 ack 的观测记录）."""
        return [a for a in self._acks if a.frame_id == frame_id]

    # ── 节拍 ──────────────────────────────────────────────────────────────

    async def settle(self) -> None:
        """让出控制权数轮（与 tests/engine/conftest.py settle 同款）."""
        for _ in range(6):
            await asyncio.sleep(0)

    async def settle_timers(self, seconds: float) -> None:
        """真实 sleep——仅供定时器（pause / flush / watchdog）真实触发使用."""
        await asyncio.sleep(seconds)

    async def close(self) -> None:
        await self.engine.aclose()
        await self.transport.aclose()
        await self.client.aclose()
        # 让被取消的 sweeper/consumer 协程完成退出，避免悬置任务告警
        for _ in range(3):
            await asyncio.sleep(0)
