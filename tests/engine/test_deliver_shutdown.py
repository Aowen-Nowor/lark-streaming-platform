"""回合外投递 / 中止 / 序列自愈 / 关停 / 观测面."""

from __future__ import annotations

import asyncio

from lark_streaming.engine import TurnPhase, Verdict
from lark_streaming.transport.errors import FeishuAPIError
from tests.engine.conftest import CHAT, env, make_engine, make_settings, settle


# ── deliver 旁路（02-engine §8）──────────────────────────────────────────


async def test_deliver_bypasses_active_turn(transport):
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="t_d"))
        st = engine._chats[CHAT].turn
        assert st is not None
        ack = await engine.submit(env(
            "deliver",
            turn="",
            payload={"text": "cron 结果正文", "title": "定时任务", "footer": {"job_id": "j_1"}},
        ))
        assert ack.verdict is Verdict.CONSUMED
        assert ack.message_id.startswith("om_")
        sends = transport.calls_of("send_card_to_chat")
        assert len(sends) == 1
        card = sends[0]["card"]
        assert card["config"]["summary"]["content"] == "定时任务"
        # 旁路：回合状态不受影响
        assert st.phase is TurnPhase.STREAMING
        assert engine._chats[CHAT].turn is st
    finally:
        await engine.aclose()


async def test_deliver_failure_degrades_chat(transport):
    engine, events, clock = make_engine(transport)
    try:
        transport.fail_always("send_card_to_chat", FeishuAPIError("boom", 500))
        ack = await engine.submit(env("deliver", turn="", payload={"text": "x"}))
        assert ack.verdict is Verdict.FALLBACK
        assert ack.detail.startswith("deliver_failed")
        assert engine._chats[CHAT].health.value == "chat_degraded"
    finally:
        await engine.aclose()


# ── turn.abort（01-protocol §4.1）────────────────────────────────────────


async def test_abort_seals_with_reason(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    try:
        await engine.submit(env("turn.start", turn="t_a"))
        await engine.submit(env("answer.delta", turn="t_a", payload={"delta": "部分产出"}, seq=1))
        ack = await engine.submit(env("turn.abort", turn="t_a", payload={"reason": "用户停止"}, seq=2))
        assert ack.verdict is Verdict.CONSUMED
        box = engine._chats[CHAT]
        st = box.sealed[-1]
        assert st.aborted is True
        assert st.abort_reason == "用户停止"
        assert st.render.footer is not None
        assert st.render.footer.is_aborted is True
        # 中止后同 turn 帧 → fallback
        late = await engine.submit(env("answer.delta", turn="t_a", payload={"delta": "x"}, seq=3))
        assert late.verdict is Verdict.FALLBACK
        assert late.detail == "no_active_card"
    finally:
        await engine.aclose()


# ── 300317 / 200850 序列自愈（04-knowledge-freeze Q1/Q2 经引擎全链路）──────


async def test_seq_conflict_self_heals_inline(transport):
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="t_s1"))
        transport.fail_next("cardkit_stream_element", FeishuAPIError("conflict", 300317))
        ack = await engine.submit(env("answer.delta", turn="t_s1", payload={"delta": "x"}, seq=1))
        assert ack.verdict is Verdict.CONSUMED  # 自愈不产生 verdict 变化（02-engine §6）
        streams = transport.calls_of("cardkit_stream_element")
        assert len(streams) == 2  # 首次 + seq+1 重发
        assert streams[0]["sequence"] == 1
        assert streams[1]["sequence"] == 2
        st = engine._chats[CHAT].turn
        assert st is not None and st.seq == 2
    finally:
        await engine.aclose()


async def test_stream_timeout_reopens_session(transport):
    """200850：seq+1 → reopen → seq+1 重发 + 会话基线刷新（Q2 对策）."""
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="t_s2"))
        st = engine._chats[CHAT].turn
        assert st is not None
        baseline = st.stream_session_at
        clock.advance(10)
        transport.fail_next("cardkit_stream_element", FeishuAPIError("timeout", 200850))
        ack = await engine.submit(env("answer.delta", turn="t_s2", payload={"delta": "x"}, seq=1))
        assert ack.verdict is Verdict.CONSUMED
        reopens = transport.calls_of("cardkit_reopen_streaming")
        assert len(reopens) == 1  # 官方恢复动作 settings streaming_mode:true
        streams = transport.calls_of("cardkit_stream_element")
        assert streams[-1]["sequence"] == 3  # 预递增1 + reopen消耗1 + 重发
        assert st.stream_session_at >= clock()  # 重开成功刷新续期基线
        assert st.stream_session_at > baseline
    finally:
        await engine.aclose()


async def test_proactive_session_renewal_before_frame(transport):
    """T_renew 540s 龄：下一帧更新前先续期（前代 _renew_stream_if_due）."""
    engine, events, clock = make_engine(
        transport, settings=make_settings(session_renew_age_sec=540)
    )
    try:
        await engine.submit(env("turn.start", turn="t_s3"))
        st = engine._chats[CHAT].turn
        assert st is not None
        clock.advance(600)  # 会话龄 600s ≥ 540s
        await engine.submit(env("answer.delta", turn="t_s3", payload={"delta": "x"}, seq=1))
        reopens = transport.calls_of("cardkit_reopen_streaming")
        assert len(reopens) == 1
        streams = transport.calls_of("cardkit_stream_element")
        assert streams[0]["sequence"] == 2  # 续期消耗 seq=1，流式帧 seq=2
        assert st.stream_session_at >= clock()  # 基线刷新
    finally:
        await engine.aclose()


# ── 关停语义（02-engine §7.3）────────────────────────────────────────────


async def test_shutdown_seals_all_active_cards(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    await engine.submit(env("turn.start", chat=CHAT, turn="t_x1", frame="f_x1"))
    await engine.submit(env("turn.start", chat="oc_chat2", turn="t_x2", frame="f_x2"))
    await engine.submit(env("deliver", turn="", payload={"text": "x"}))
    await engine.aclose()
    assert any(e.type == "shutdown" for e in events)
    # 两个活跃卡都被定稿（close_streaming 各一次；deliver 是静态卡不关流）
    assert len(transport.calls_of("cardkit_close_streaming")) == 2
    # 关停后新帧 → fallback（engine_shutdown）
    ack = await engine.submit(env("turn.start", chat=CHAT, turn="t_after"))
    assert ack.verdict is Verdict.FALLBACK
    assert ack.detail == "engine_shutdown"


async def test_queued_frames_get_fallback_on_close(transport):
    engine, events, clock = make_engine(transport)
    transport.set_delay("cardkit_create", 0.1)
    start_task = asyncio.create_task(engine.submit(env("turn.start", turn="t_q")))
    await asyncio.sleep(0.02)
    late_task = asyncio.create_task(engine.submit(env("answer.delta", turn="t_q", payload={"delta": "x"}, seq=1)))
    await asyncio.sleep(0.05)
    await engine.aclose()
    results = await asyncio.gather(start_task, late_task, return_exceptions=True)
    for r in results:
        assert not isinstance(r, BaseException)
        assert r.verdict is Verdict.FALLBACK  # 未决帧统一 fallback（接入方原生投递）


# ── 观测面（02-engine §9）────────────────────────────────────────────────


async def test_snapshot_health_shape(transport):
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", chat=CHAT, turn="t_h"))
        await engine.submit(env("answer.delta", chat=CHAT, turn="t_h", payload={"delta": "你好"}, seq=1))
        await engine.submit(env("deliver", chat="oc_chat2", turn="", payload={"text": "x"}))
        health = engine.snapshot_health(verbose=True)
        assert health["chats_total"] == 2
        assert health["active_cards"] == 1
        assert health["degraded_chats"] == 0
        c1 = health["chats"][CHAT]
        assert c1["turn_phase"] == "streaming"
        assert c1["chat_health"] == "chat_ok"
        assert c1["active_card_age_sec"] is not None
        assert c1["queue_depth"] == 0
        assert c1["degrade_count"] == 0
        assert c1["answer_bytes"] == len("你好".encode("utf-8"))
        assert c1["panel_rows"] == 0
        assert c1["sealed_pending_gc"] == 0
    finally:
        await engine.aclose()


async def test_gc_reclaims_sealed_after_cooldown(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0, gc_cooldown_sec=60)
    )
    try:
        await engine.submit(env("turn.start", turn="t_gc"))
        await engine.submit(env("answer.final", turn="t_gc", payload={"text": "x"}))
        box = engine._chats[CHAT]
        assert len(box.sealed) == 1
        mid = box.sealed[0].message_id
        assert engine._message_index.get(mid) == CHAT  # 反查窗口内
        clock.advance(120)  # > gc_cooldown 60s
        engine.run_gc()
        assert box.sealed == []  # 冷却回收
        assert mid not in engine._message_index  # 反查窗口关闭
    finally:
        await engine.aclose()


async def test_ttl_force_seals_idle_orphan(transport):
    """TTL 到期强制收口（空闲孤儿；活跃回合每帧刷新 last_activity 永不误杀）."""
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0, card_ttl_sec=600)
    )
    try:
        await engine.submit(env("turn.start", turn="t_ttl"))
        box = engine._chats[CHAT]
        st = box.turn
        assert st is not None
        st.created_at = clock() - 700
        st.last_activity = clock() - 700  # 建卡后全程静默
        engine.run_gc()
        await settle()
        assert len(box.sealed) == 1  # 强制收口
        assert len(transport.calls_of("cardkit_close_streaming")) == 1
    finally:
        await engine.aclose()


async def test_ttl_never_kills_active_turn(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0, card_ttl_sec=600)
    )
    try:
        await engine.submit(env("turn.start", turn="t_long"))
        box = engine._chats[CHAT]
        st = box.turn
        assert st is not None
        st.created_at = clock() - 700
        st.last_activity = clock() - 1  # 每帧刷新 → 活跃长回合
        engine.run_gc()
        await settle()
        assert box.turn is st  # 未被回收
    finally:
        await engine.aclose()
