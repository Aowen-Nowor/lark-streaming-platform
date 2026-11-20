"""建卡窗口 / 澄清竞态 / 看门狗 / 超权 turn（02-engine §2/§3.2/§1.1）."""

from __future__ import annotations

import asyncio

from lark_streaming.engine import TurnPhase, Verdict
from tests.engine.conftest import (
    CHAT,
    env,
    make_engine,
    make_settings,
    settle,
)


# ── 建卡窗口幂等屏障（02-engine §3.2；10-12 一轮 3 卡事故根治）─────────────


async def test_frames_during_create_window_queue_behind_single_create(transport):
    """建卡窗口（0.5~1s 挂起）内到达的帧排队等待，绝不触发第二建卡."""
    transport.set_delay("cardkit_create", 0.15)
    engine, events, clock = make_engine(transport)
    try:
        start_task = asyncio.create_task(engine.submit(env("turn.start", turn="tw")))
        await asyncio.sleep(0.03)  # 建卡进行中
        ack_d = await engine.submit(env("answer.delta", turn="tw", payload={"delta": "早期帧"}, seq=1))
        ack_t = await engine.submit(env("tool.start", turn="tw", payload={"tool": "grep"}, seq=2))
        ack_start = await start_task
        assert ack_start.verdict is Verdict.CONSUMED
        # 幂等屏障：恰好一次建卡
        assert len(transport.calls_of("cardkit_create")) == 1
        assert ack_d.verdict is Verdict.CONSUMED
        assert ack_t.verdict is Verdict.CONSUMED
        # 早期帧在 STREAMING 后按序消费：answer 直达打字机通道
        streams = transport.calls_of("cardkit_stream_element")
        assert any(c["content"] == "早期帧" for c in streams)
        assert len(transport.calls_of("cardkit_update")) == 1  # 工具行结构 flush
    finally:
        await engine.aclose()


# ── 澄清窗口 T_pause（answer.final 后 1.2s 内 clarify.request 命中）─────────


async def test_final_window_hit_by_clarify_goes_paused(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0.3)
    )
    try:
        await engine.submit(env("turn.start", turn="tc1"))
        await engine.submit(env("answer.final", turn="tc1", payload={"text": "答案"}, seq=1))
        st = engine._chats[CHAT].turn
        assert st is not None and st.phase is TurnPhase.SEALING
        assert st.pause_pending is True
        # 窗口内 clarify.request 到达 → PAUSED（真卡保流式态）
        ack = await engine.submit(env(
            "clarify.request", turn="tc1",
            payload={"form": {"kind": "select", "question": "选一个", "options": ["A", "B"]}},
            seq=2,
        ))
        assert ack.verdict is Verdict.CONSUMED
        assert st.phase is TurnPhase.PAUSED
        assert st.pause_pending is False
        assert st.clarify_id
        # 澄清卡经 CardKit 实体投递（回合卡无 reply_to → 直发一次；澄清卡直发一次）
        assert len(transport.calls_of("cardkit_create")) == 2  # 回合卡 + 澄清卡
        assert len(transport.calls_of("send_card_by_id_to_chat")) == 2
        # 未关流（同一张卡保流式态）
        assert len(transport.calls_of("cardkit_close_streaming")) == 0
        # 面板暂停行已插入
        from lark_streaming.renderer.theme import DEFAULT_THEME

        wait_text = DEFAULT_THEME.text("clarify_wait")[0]
        assert any(r.text == wait_text for r in st.render.panel_rows)
    finally:
        await engine.aclose()


async def test_final_window_expiry_seals_for_real(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0.08)
    )
    try:
        await engine.submit(env("turn.start", turn="tc2"))
        await engine.submit(env("answer.final", turn="tc2", payload={"text": "真落定"}, seq=1))
        await asyncio.sleep(0.2)  # 窗口到期 → 真落定
        box = engine._chats[CHAT]
        assert len(box.sealed) == 1
        assert len(transport.calls_of("cardkit_close_streaming")) == 1
    finally:
        await engine.aclose()


async def test_clarify_pause_and_resume_same_card(transport):
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="tc3"))
        await engine.submit(env("answer.delta", turn="tc3", payload={"delta": "前段"}, seq=1))
        await engine.submit(env("clarify.request", turn="tc3", payload={
            "form": {"kind": "input", "question": "补充信息"},
        }, seq=2))
        st = engine._chats[CHAT].turn
        assert st is not None and st.phase is TurnPhase.PAUSED
        # 暂停期 answer 帧只缓冲（零 API）
        n_streams = len(transport.calls_of("cardkit_stream_element"))
        await engine.submit(env("answer.delta", turn="tc3", payload={"delta": "待续"}, seq=3))
        assert len(transport.calls_of("cardkit_stream_element")) == n_streams
        # clarify.answer → 续流（同一张卡）
        ack = await engine.submit(env("clarify.answer", turn="tc3", payload={"values": {"q": "a"}}, seq=4))
        assert ack.verdict is Verdict.CONSUMED
        assert st.phase is TurnPhase.STREAMING
        # 续流冲刷完整缓冲（前段 + 待续）+ 暂停行摘除
        streams = transport.calls_of("cardkit_stream_element")
        assert streams[-1]["content"] == "前段待续"
        from lark_streaming.renderer.theme import DEFAULT_THEME

        wait_text = DEFAULT_THEME.text("clarify_wait")[0]
        assert not any(r.text == wait_text for r in st.render.panel_rows)
    finally:
        await engine.aclose()


async def test_clarify_request_on_idle_falls_back(transport):
    engine, events, clock = make_engine(transport)
    try:
        ack = await engine.submit(env("clarify.request", payload={
            "form": {"kind": "select", "question": "q", "options": ["A"]},
        }))
        assert ack.verdict is Verdict.FALLBACK
        assert ack.detail == "clarify_without_stream"
    finally:
        await engine.aclose()


async def test_clarify_answer_not_paused_ignored(transport):
    engine, events, clock = make_engine(transport)
    try:
        ack = await engine.submit(env("clarify.answer", payload={"values": {}}))
        assert ack.verdict is Verdict.CONSUMED  # 通告忽略，不是 fallback
        assert ack.detail == "not_paused"
    finally:
        await engine.aclose()


async def test_clarify_card_delivery_failure_rolls_back(transport):
    """表单卡投递失败：回滚暂停转 fallback（原生投递澄清文本）."""
    from lark_streaming.transport.errors import FeishuAPIError

    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="tc4"))
        transport.fail_next("cardkit_create", FeishuAPIError("boom", 500))
        # 第一个 create 用于回合卡（成功）；澄清卡 create 注入失败
        # 注意 fail_next 只影响下一次 create —— 回合卡已建完，这里必中澄清卡
        ack = await engine.submit(env("clarify.request", turn="tc4", payload={
            "form": {"kind": "input", "question": "q"},
        }, seq=1))
        assert ack.verdict is Verdict.FALLBACK
        assert ack.detail.startswith("clarify_card_failed")
        st = engine._chats[CHAT].turn
        assert st is not None and st.phase is TurnPhase.STREAMING  # 回滚
        assert st.clarify_id == ""
        assert engine._chats[CHAT].health.value == "chat_degraded"
    finally:
        await engine.aclose()


# ── 看门狗（前代 v2.0.21/26/32 语义 + 新 PAUSED 真落定超时）─────────────────


async def _streaming_box(transport, engine, turn="tw1"):
    await engine.submit(env("turn.start", turn=turn))
    return engine._chats[CHAT]


async def test_watchdog_seals_idle_streaming_card(transport):
    engine, events, clock = make_engine(transport, settings=make_settings(clarify_window_sec=0))
    try:
        box = await _streaming_box(transport, engine)
        st = box.turn
        assert st is not None
        st.last_activity = clock() - 400  # 静默 400s ≥ 300s 阈值
        n_close = len(transport.calls_of("cardkit_close_streaming"))
        engine._watchdog_due(box)  # 到期回调（advisory）→ 入队控制帧
        await settle()
        assert len(box.sealed) == 1  # 复查挡通过 → 真落定
        assert len(transport.calls_of("cardkit_close_streaming")) == n_close + 1
        # 看门狗封卡 → card_sealed_fallback 通告（01-protocol §4.6）
        assert any(e.type == "card_sealed_fallback" for e in events)
    finally:
        await engine.aclose()


async def test_watchdog_yields_on_fresh_activity(transport):
    engine, events, clock = make_engine(transport)
    try:
        box = await _streaming_box(transport, engine, turn="tw2")
        st = box.turn
        assert st is not None
        st.last_activity = clock()  # 刚活跃
        engine._watchdog_due(box)
        await settle()
        assert box.turn is not None  # 未封卡（回合仍活跃）
        assert not any(e.type == "card_sealed_fallback" for e in events)
    finally:
        await engine.aclose()


async def test_watchdog_yields_during_degrade(transport):
    from lark_streaming.engine.state import ChatHealth

    engine, events, clock = make_engine(transport)
    try:
        box = await _streaming_box(transport, engine, turn="tw3")
        st = box.turn
        assert st is not None
        st.last_activity = clock() - 400
        box.health = ChatHealth.DEGRADED
        box.degraded_until = clock() + 60
        engine._watchdog_due(box)
        await settle()
        assert box.turn is not None  # 降级期让位：无 API 无封卡
        assert len(transport.calls_of("cardkit_close_streaming")) == 0
    finally:
        await engine.aclose()


async def test_paused_turn_seals_after_timeout(transport):
    """PAUSED 超时 → 真落定（02-engine §1.1；缓冲内容随终值落卡）."""
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="tw4"))
        await engine.submit(env("answer.delta", turn="tw4", payload={"delta": "已产出"}, seq=1))
        await engine.submit(env("clarify.request", turn="tw4", payload={
            "form": {"kind": "input", "question": "q"},
        }, seq=2))
        box = engine._chats[CHAT]
        st = box.turn
        assert st is not None and st.phase is TurnPhase.PAUSED
        st.paused_at = clock() - 400  # 暂停已 400s ≥ 300s
        engine._watchdog_due(box)
        await settle()
        assert len(box.sealed) == 1
        assert st.aborted is False
        # 真落定：终值 + 关流式都发生
        streams = transport.calls_of("cardkit_stream_element")
        assert streams[-1]["content"] == "已产出"
        assert len(transport.calls_of("cardkit_close_streaming")) == 1
    finally:
        await engine.aclose()


# ── 超权 turn（01-protocol §5.1：新 turn 打断未定稿旧回合）──────────────────


async def test_new_turn_supersedes_active_turn(transport):
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="t_old", frame="f_ts_old"))
        await engine.submit(env("answer.delta", turn="t_old", payload={"delta": "旧回合"}, seq=1))
        ack = await engine.submit(env("turn.start", turn="t_new", frame="f_ts_new"))
        assert ack.verdict is Verdict.CONSUMED
        box = engine._chats[CHAT]
        # 旧卡真落定（已 consumed 帧不受影响）
        assert len(box.sealed) == 1 and box.sealed[0].turn_id == "t_old"
        assert len(transport.calls_of("cardkit_close_streaming")) == 1
        # 新回合建新卡
        assert box.turn is not None and box.turn.turn_id == "t_new"
        assert len(transport.calls_of("cardkit_create")) == 2
        # 旧回合迟到帧 → fallback
        ack_late = await engine.submit(env("answer.delta", turn="t_old", payload={"delta": "x"}, seq=2))
        assert ack_late.verdict is Verdict.FALLBACK
        assert ack_late.detail == "no_active_card"
    finally:
        await engine.aclose()


async def test_duplicate_turn_start_is_idempotent(transport):
    engine, events, clock = make_engine(transport)
    try:
        ack1 = await engine.submit(env("turn.start", turn="tdup", frame="f_ts1"))
        ack2 = await engine.submit(env("turn.start", turn="tdup", frame="f_ts2"))
        assert ack1.verdict is Verdict.CONSUMED
        assert ack2.verdict is Verdict.CONSUMED
        assert ack2.detail == "turn_start_idempotent"
        assert len(transport.calls_of("cardkit_create")) == 1  # 不重复建卡
    finally:
        await engine.aclose()
