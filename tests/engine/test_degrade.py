"""chat 级降级红线（02-engine §1.2/§7）：永不丢帧 + 冷却探测恢复."""

from __future__ import annotations

import asyncio

from lark_streaming.engine import Verdict
from lark_streaming.engine.state import ChatHealth
from lark_streaming.transport.errors import FeishuAPIError
from tests.engine.conftest import CHAT, env, make_engine, make_settings


async def test_stream_failure_degrades_chat_and_frame(transport):
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="td1"))
        transport.fail_always("cardkit_stream_element", FeishuAPIError("boom", 500))
        ack = await engine.submit(env("answer.delta", turn="td1", payload={"delta": "x"}, seq=1))
        # 红线：失败帧必回执 fallback（接入方原生投递）——不存在静默失败
        assert ack.verdict is Verdict.FALLBACK
        assert "500" in ack.detail
        box = engine._chats[CHAT]
        assert box.health is ChatHealth.DEGRADED
        assert box.degrade_count == 1
        # 生命周期通告
        assert any(e.type == "chat_degraded" and e.code == 500 for e in events)
    finally:
        await engine.aclose()


async def test_cooldown_window_fast_path_degraded_without_api(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(degrade_cooldown_other_sec=100)
    )
    try:
        await engine.submit(env("turn.start", turn="td2"))
        transport.fail_always("cardkit_stream_element", FeishuAPIError("boom", 500))
        await engine.submit(env("answer.delta", turn="td2", payload={"delta": "x"}, seq=1))
        box = engine._chats[CHAT]
        assert box.health is ChatHealth.DEGRADED
        n_calls = len(transport.calls)
        # 冷却期内的帧：零 API 零消费，快速 degraded 回执
        ack = await engine.submit(env("answer.delta", turn="td2", payload={"delta": "y"}, seq=2))
        assert ack.verdict is Verdict.DEGRADED
        assert ack.detail == "chat_degraded"
        assert len(transport.calls) == n_calls  # 没有任何新 API 调用
    finally:
        await engine.aclose()


async def test_cooldown_expiry_probes_with_next_frame(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(degrade_cooldown_other_sec=0.2, clarify_window_sec=0)
    )
    try:
        await engine.submit(env("turn.start", turn="td3"))
        transport.fail_next("cardkit_stream_element", FeishuAPIError("boom", 500))
        ack_fail = await engine.submit(env("answer.delta", turn="td3", payload={"delta": "x"}, seq=1))
        assert ack_fail.verdict is Verdict.FALLBACK
        clock.advance(10.0)  # 冷却期满（钳制下限 5s）
        # 探测帧：真实 API 试探——成功 → CHAT_OK + 该帧正常消费
        ack_ok = await engine.submit(env("answer.delta", turn="td3", payload={"delta": "恢复"}, seq=2, frame="f_probe1"))
        assert ack_ok.verdict is Verdict.CONSUMED
        box = engine._chats[CHAT]
        assert box.health is ChatHealth.OK
        assert any(e.type == "chat_recovered" for e in events)
    finally:
        await engine.aclose()


async def test_probe_failure_restarts_cooldown(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(degrade_cooldown_other_sec=0.1, clarify_window_sec=0)
    )
    try:
        await engine.submit(env("turn.start", turn="td4"))
        transport.fail_always("cardkit_stream_element", FeishuAPIError("boom", 500))
        await engine.submit(env("answer.delta", turn="td4", payload={"delta": "x"}, seq=1))
        first_until = engine._chats[CHAT].degraded_until
        clock.advance(10.0)
        transport.clear_failures()
        transport.fail_next("cardkit_stream_element", FeishuAPIError("still", 500))
        ack = await engine.submit(env("answer.delta", turn="td4", payload={"delta": "y"}, seq=2, frame="f_probe2"))
        assert ack.verdict is Verdict.FALLBACK
        box = engine._chats[CHAT]
        assert box.health is ChatHealth.DEGRADED
        assert box.degraded_until > first_until  # 冷却重启
        assert box.degrade_count == 2
    finally:
        await engine.aclose()


async def test_degrade_cooldown_429_vs_other(transport):
    engine, events, clock = make_engine(transport)
    try:
        box = engine._chat_box(CHAT)
        engine._degrade_chat(box, "unit", FeishuAPIError("rate", 99991400))
        assert box.degraded_until - clock() <= 60.0 + 1e-6
        assert box.degraded_until - clock() > 59.0  # 429 → 60s
        clock.advance(100)
        engine._degrade_chat(box, "unit", FeishuAPIError("net", 500))
        assert box.degraded_until - clock() <= 30.0 + 1e-6  # 其他 → 30s
    finally:
        await engine.aclose()


async def test_create_failure_seals_turn_without_card(transport):
    engine, events, clock = make_engine(transport)
    try:
        transport.fail_always("cardkit_create", FeishuAPIError("no", 500))
        ack = await engine.submit(env("turn.start", turn="td5"))
        assert ack.verdict is Verdict.FALLBACK
        assert ack.detail.startswith("create_failed")
        box = engine._chats[CHAT]
        assert box.turn is None
        assert len(box.sealed) == 1 and box.sealed[0].seal_failed is True
        assert box.health is ChatHealth.DEGRADED
        # 冷却期内帧 → degraded 快速回执（零 API）
        ack_dg = await engine.submit(env("answer.delta", turn="td5", payload={"delta": "x"}, frame="f_cf1"))
        assert ack_dg.verdict is Verdict.DEGRADED
        # 冷却期满 → 探测：无回合（建卡已失败）→ fallback(no_active_card)
        clock.advance(100)
        transport.clear_failures()
        ack2 = await engine.submit(env("answer.delta", turn="td5", payload={"delta": "x"}, frame="f_cf2"))
        assert ack2.verdict is Verdict.FALLBACK
        assert ack2.detail == "no_active_card"
    finally:
        await engine.aclose()


async def test_reply_anchor_failure_falls_back_to_direct_send(transport):
    """Q10：reply 锚定失败降级直发——内容永不丢."""
    engine, events, clock = make_engine(transport)
    try:
        transport.fail_always("reply_card_by_id", FeishuAPIError("gone", 1000023))
        ack = await engine.submit(env("turn.start", payload={"reply_to": "om_gone"}))
        assert ack.verdict is Verdict.CONSUMED  # 直发成功 → 消费成立
        assert len(transport.calls_of("reply_card_by_id")) == 1
        assert len(transport.calls_of("send_card_by_id_to_chat")) == 1  # 直发兜底
    finally:
        await engine.aclose()


async def test_send_failure_terminates_turn_with_fallback(transport):
    engine, events, clock = make_engine(transport)
    try:
        transport.fail_always("send_card_by_id_to_chat", FeishuAPIError("no", 500))
        ack = await engine.submit(env("turn.start"))
        assert ack.verdict is Verdict.FALLBACK
        assert ack.detail.startswith("send_failed")
        box = engine._chats[CHAT]
        assert box.turn is None
        assert box.health is ChatHealth.DEGRADED
    finally:
        await engine.aclose()


async def test_seal_step_failure_degrades_and_marks(transport):
    """定稿第一步（终值 patch）失败 → chat 降级 + 回合标注 seal_failed."""
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    try:
        await engine.submit(env("turn.start", turn="td6"))
        await engine.submit(env("answer.delta", turn="td6", payload={"delta": "x"}, seq=1))
        transport.fail_always("cardkit_stream_element", FeishuAPIError("boom", 500))
        await engine.submit(env("answer.final", turn="td6", payload={"text": "终值"}, seq=2))
        box = engine._chats[CHAT]
        assert box.health is ChatHealth.DEGRADED
        st = box.sealed[-1]
        assert st.seal_failed is True
        # 装饰性步骤不再执行（第一步失败即止——无 API 可依赖）
        assert len(transport.calls_of("cardkit_close_streaming")) == 0
    finally:
        await engine.aclose()


async def test_degraded_turn_starvation_sealed_by_watchdog_probe(transport):
    """降级后无后续帧的 STREAMING 回合：冷却期满看门狗探测落定.

    场景：流式失败降级（cooldown 5s）→ 适配方原生投递后不再发帧 →
    回合滞留 STREAMING → 冷却期满（RECOVERING）看门狗到期 → 探测性
    真落定（chat_recovered + card_sealed_fallback 双通告）。
    """
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    try:
        await engine.submit(env("turn.start", turn="td8"))
        await engine.submit(env("answer.delta", turn="td8", payload={"delta": "已产出"}, seq=1))
        transport.fail_always("cardkit_stream_element", FeishuAPIError("boom", 500))
        ack = await engine.submit(env("answer.delta", turn="td8", payload={"delta": "失败帧"}, seq=2, frame="f_d8a"))
        assert ack.verdict is Verdict.FALLBACK
        box = engine._chats[CHAT]
        assert box.health is ChatHealth.DEGRADED
        # 接入方此后不再发帧：冷却期满 + 静默超时同时成立
        transport.clear_failures()
        clock.advance(100)
        st = box.turn
        assert st is not None
        st.last_activity = clock() - 400
        engine._watchdog_due(box)  # RECOVERING 态不再让位 → 探测封卡
        for _ in range(10):
            await asyncio.sleep(0)
        assert len(box.sealed) == 1
        assert box.health is ChatHealth.OK  # 探测成功 → 恢复
        assert any(e.type == "chat_recovered" for e in events)
        assert any(e.type == "card_sealed_fallback" for e in events)
        assert len(transport.calls_of("cardkit_close_streaming")) == 1
    finally:
        await engine.aclose()


async def test_seal_probe_control_resolves_pending_window(transport):
    """防御路径直测：SEALING 挂起 + 降级冷却期满 → run_gc 的 SEAL_PROBE 落定.

    （SEALING 期帧全部 fallback 不触 API，该状态实际不可达；控制分支
    保留为 sweeper 防御面，本测试保证其行为正确。）
    """
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=100)
    )
    try:
        await engine.submit(env("turn.start", turn="td9"))
        await engine.submit(env("answer.final", turn="td9", payload={"text": "终值"}, seq=1))
        box = engine._chats[CHAT]
        st = box.turn
        assert st is not None and st.pause_pending is True
        # 人为构造「SEALING + 降级冷却期满」防御场景
        box.health = ChatHealth.DEGRADED
        box.degraded_until = clock() - 1
        engine.run_gc()  # → RECOVERING + SEAL_PROBE 入队
        for _ in range(10):
            await asyncio.sleep(0)
        assert len(box.sealed) == 1
        assert box.health is ChatHealth.OK
        assert len(transport.calls_of("cardkit_close_streaming")) == 1
    finally:
        await engine.aclose()
