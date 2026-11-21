"""engine 测试共享台架 — FakeTransport 故障注入 + 假时钟（02-engine §10 L1）.

测试不依赖真实网络（L1 纪律）；传输行为分支由 transport 层测试覆盖，
本台架只验编排语义（状态机 / 守卫 / 降级 / 定时器 / 红线）。
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from lark_streaming.engine import EngineSettings, LifecycleEvent, StreamingEngine

CHAT = "oc_chat1"
TURN = "t_20260101_0001"


def env(
    event: str,
    *,
    chat: str = CHAT,
    turn: str = TURN,
    payload: dict[str, Any] | None = None,
    frame: str | None = None,
    seq: int | None = None,
) -> dict[str, Any]:
    """构造 LSEP/1 原始信封."""
    raw: dict[str, Any] = {
        "v": 1,
        "event": event,
        "frame_id": frame or f"f_{event}_{seq if seq is not None else 0}",
        "chat_id": chat,
        "turn_id": turn,
        "payload": payload or {},
    }
    if seq is not None:
        raw["seq"] = seq
    return raw


class FakeClock:
    """可控单调时钟（引擎 clock 注入点）."""

    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


class FakeTransport:
    """记录式假传输：全方法面 + 故障注入 + 延迟注入.

    故障注入约定：fail_next 注入「下一次调用抛出」；fail_always 注入
    「该操作恒抛」。非瞬态码（如 500）不触发重试——测试即时失败；
    瞬态/网络错误的重试行为由 transport 层测试覆盖。
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self._fail_next: dict[str, list[BaseException]] = {}
        self._always_fail: dict[str, BaseException] = {}
        self._delay: dict[str, float] = {}
        self._n = 0

    # ── 注入面 ────────────────────────────────────────────────────────
    def fail_next(self, op: str, exc: BaseException, times: int = 1) -> None:
        self._fail_next.setdefault(op, []).extend([exc] * times)

    def fail_always(self, op: str, exc: BaseException) -> None:
        self._always_fail[op] = exc

    def clear_failures(self, op: str | None = None) -> None:
        if op is None:
            self._fail_next.clear()
            self._always_fail.clear()
        else:
            self._fail_next.pop(op, None)
            self._always_fail.pop(op, None)

    def set_delay(self, op: str, seconds: float) -> None:
        self._delay[op] = seconds

    def calls_of(self, op: str) -> list[dict[str, Any]]:
        return [kw for name, kw in self.calls if name == op]

    def next_id(self) -> str:
        self._n += 1
        return str(self._n)

    async def _call(self, op: str, **kw: Any) -> Any:
        self.calls.append((op, kw))
        delay = self._delay.get(op)
        if delay:
            await asyncio.sleep(delay)
        exc = self._always_fail.get(op)
        if exc is not None:
            raise exc
        queue = self._fail_next.get(op)
        if queue:
            raise queue.pop(0)
        return None

    # ── 传输面（与 FeishuTransport 引擎消费面逐签名对齐）────────────────
    async def cardkit_create(self, card: dict[str, Any]) -> str:
        await self._call("cardkit_create", card=card)
        return f"card_{self.next_id()}"

    async def send_card_by_id_to_chat(self, chat_id: str, card_id: str) -> str:
        await self._call("send_card_by_id_to_chat", chat_id=chat_id, card_id=card_id)
        return f"om_{self.next_id()}"

    async def reply_card_by_id(self, message_id: str, card_id: str) -> str:
        await self._call("reply_card_by_id", message_id=message_id, card_id=card_id)
        return f"om_{self.next_id()}"

    async def cardkit_stream_element(
        self, card_id: str, element_id: str, content: str, *, sequence: int = 0,
    ) -> None:
        await self._call(
            "cardkit_stream_element",
            card_id=card_id, element_id=element_id, content=content, sequence=sequence,
        )

    async def cardkit_update(
        self, card_id: str, card: dict[str, Any], *, sequence: int = 0,
    ) -> None:
        await self._call("cardkit_update", card_id=card_id, card=card, sequence=sequence)

    async def cardkit_batch_update(
        self, card_id: str, actions: list[dict[str, Any]], *, sequence: int = 0,
    ) -> None:
        await self._call(
            "cardkit_batch_update", card_id=card_id, actions=actions, sequence=sequence,
        )

    async def cardkit_close_streaming(
        self, card_id: str, card: dict[str, Any], *, sequence: int = 0,
    ) -> None:
        # #14：定稿改走全量更新，调用方传入已定稿的完整卡 JSON。
        await self._call(
            "cardkit_close_streaming",
            card_id=card_id, card=card, sequence=sequence,
        )

    async def cardkit_reopen_streaming(
        self, card_id: str, card: dict[str, Any], *, sequence: int = 0,
    ) -> None:
        # #14：重开会话改走全量更新，调用方传入当前流式卡。
        await self._call(
            "cardkit_reopen_streaming",
            card_id=card_id, card=card, sequence=sequence,
        )

    async def send_card_to_chat(self, chat_id: str, card: dict[str, Any]) -> str:
        await self._call("send_card_to_chat", chat_id=chat_id, card=card)
        return f"om_{self.next_id()}"


def make_settings(**kw: Any) -> EngineSettings:
    kw.setdefault("gc_sweep_sec", 300.0)  # 测试内不跑周期巡检（显式 run_gc 驱动）
    return EngineSettings(**kw)


def make_engine(
    transport: FakeTransport,
    *,
    settings: EngineSettings | None = None,
    clock: FakeClock | None = None,
) -> tuple[StreamingEngine, list[LifecycleEvent], FakeClock]:
    """构造引擎 + 生命周期收集器 + 时钟."""
    clock = clock or FakeClock()
    events: list[LifecycleEvent] = []
    engine = StreamingEngine(
        transport,  # type: ignore[arg-type]
        settings=settings or make_settings(),
        clock=clock,
        on_lifecycle=events.append,
    )
    return engine, events, clock


async def settle() -> None:
    """让出控制权数轮，确保消费者处理完毕."""
    for _ in range(6):
        await asyncio.sleep(0)


@pytest.fixture
def transport() -> FakeTransport:
    return FakeTransport()
