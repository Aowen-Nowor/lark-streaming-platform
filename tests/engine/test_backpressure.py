"""队列背压回归（audit #2）— pending_max 显式接线 + QueueFull → FALLBACK.

红线语义（02-engine §3.3）：per-chat 队列 maxsize=pending_max（默认 64，
钉制 8~4096）；队列满时新帧立得 ``FALLBACK(queue_full:native_delivery)``
回执——背压就是另一种降级，帧不丢语义与永不丢帧红线同构。

修复前行为：``ChatBox.queue`` 无 maxsize、``pending_max`` 全仓库引擎侧
零引用——恶意/失控生产者可内存击穿。
"""

from __future__ import annotations

import asyncio

from lark_streaming.engine import Verdict
from tests.engine.conftest import CHAT, env, make_engine, make_settings


async def test_queue_full_yields_fallback_ack(transport):
    """建卡窗口内塞满队列 → 后续帧 FALLBACK（queue_full:native_delivery）."""
    engine, events, clock = make_engine(
        transport, settings=make_settings(pending_max=8)
    )
    try:
        # pending_max 钳制下限 8：建卡挂起 0.3s，消费者阻塞期间并发注入
        transport.set_delay("cardkit_create", 0.3)
        start_task = asyncio.create_task(engine.submit(env("turn.start", turn="tq1")))
        await asyncio.sleep(0.05)  # 消费者已进入建卡（阻塞在 0.3s delay）
        # 并发提交 8 帧：submit 同步段（future 创建 + put_nowait）立即入队
        tasks = [
            asyncio.create_task(engine.submit(env(
                "answer.delta", turn="tq1", payload={"delta": "x"}, seq=i + 1,
            )))
            for i in range(8)
        ]
        for _ in range(4):
            await asyncio.sleep(0)  # 让 8 个 submit 跑完同步段挂起在 await fut
        box = engine._chats[CHAT]
        assert box.queue.maxsize == 8
        assert box.queue.qsize() == 8  # 队列恰好满
        # 第 9 帧触发 QueueFull → FALLBACK（不阻塞、不丢帧语义）
        ack_overflow = await engine.submit(env(
            "answer.delta", turn="tq1", payload={"delta": "y"}, seq=9,
        ))
        assert ack_overflow.verdict is Verdict.FALLBACK
        assert ack_overflow.detail == "queue_full:native_delivery"
        # 队列内 8 帧正常消费完毕（背压不损伤已入队帧）
        acks_full = await asyncio.gather(start_task, *tasks)
        assert all(a.verdict is Verdict.CONSUMED for a in acks_full)
    finally:
        await engine.aclose()


async def test_queue_full_ack_is_cached_idempotent(transport):
    """queue_full 回执进幂等缓存——同 frame_id 重发得到同一回执."""
    engine, events, clock = make_engine(
        transport, settings=make_settings(pending_max=8)
    )
    try:
        transport.set_delay("cardkit_create", 0.3)
        start_task = asyncio.create_task(engine.submit(env("turn.start", turn="tq2")))
        await asyncio.sleep(0.05)
        tasks = [
            asyncio.create_task(engine.submit(env(
                "answer.delta", turn="tq2", payload={"delta": "x"}, seq=i + 1,
            )))
            for i in range(8)
        ]
        for _ in range(4):
            await asyncio.sleep(0)
        raw = env("answer.delta", turn="tq2", payload={"delta": "y"}, seq=9)
        ack1 = await engine.submit(raw)
        ack2 = await engine.submit(raw)  # 同帧重发
        await asyncio.gather(start_task, *tasks)
        assert ack1.verdict is Verdict.FALLBACK
        assert ack2.verdict is Verdict.FALLBACK
        assert ack2.detail == ack1.detail
    finally:
        await engine.aclose()


async def test_default_settings_queue_bounded(transport):
    """默认 settings 下 ChatBox 队列带 maxsize=pending_max（接线证据）."""
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="tq3"))
        box = engine._chats[CHAT]
        assert box.queue.maxsize == 64  # 默认 pending_max
    finally:
        await engine.aclose()
