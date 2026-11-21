"""回合生命周期 E2E（L1 mock）：turn.start → 流式 → 定稿 → SEALED.

本文件断言「直落定」路径（clarify_window_sec=0）；窗口竞态见
test_clarify.py（02-engine §2 T_pause 语义）。
"""

from __future__ import annotations

import asyncio

from lark_streaming.engine import TurnPhase, Verdict
from tests.engine.conftest import (
    CHAT,
    env,
    make_engine,
    make_settings,
)


async def _happy_stream(transport, *, window: float = 0.0):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=window)
    )
    ack1 = await engine.submit(env("turn.start", payload={"reply_to": "om_anchor"}))
    ack2 = await engine.submit(env("answer.delta", payload={"delta": "你好"}, seq=1))
    ack3 = await engine.submit(env("tool.start", payload={"tool": "terminal"}, seq=2))
    ack4 = await engine.submit(env("answer.final", payload={"text": "你好，世界"}, seq=3))
    return engine, events, clock, ack1, ack2, ack3, ack4


async def test_turn_start_creates_and_anchors(transport):
    engine, events, clock, ack1, *_ = await _happy_stream(transport)
    try:
        assert ack1.verdict is Verdict.CONSUMED
        assert ack1.message_id.startswith("om_")
        creates = transport.calls_of("cardkit_create")
        assert len(creates) == 1
        card = creates[0]["card"]
        assert card["schema"] == "2.0"
        assert card["config"]["streaming_mode"] is True
        # om_ 锚点 → reply 通道（Q10 前置：非 om_ 才直发）
        replies = transport.calls_of("reply_card_by_id")
        assert len(replies) == 1 and replies[0]["message_id"] == "om_anchor"
    finally:
        await engine.aclose()


async def test_stream_answer_uses_typewriter_channel(transport):
    engine, events, clock, _, ack2, _, _ = await _happy_stream(transport)
    try:
        assert ack2.verdict is Verdict.CONSUMED
        streams = transport.calls_of("cardkit_stream_element")
        assert len(streams) == 2  # 流式帧 + 定稿终值 patch
        call = streams[0]
        assert call["content"] == "你好"
        assert call["element_id"]  # answer 元素 id
        assert call["sequence"] == 1  # 前代预递增纪律：首帧 sequence=1
    finally:
        await engine.aclose()


async def test_tool_start_flushes_immediately_tool_end_debounced(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0, panel_flush_sec=0.05)
    )
    try:
        await engine.submit(env("turn.start", turn="t2"))
        await engine.submit(env("tool.start", turn="t2", payload={"tool": "grep"}, seq=1))
        # 结构性新增行 → 即时 cardkit_update（OPT-1a 红线：不合并）
        assert len(transport.calls_of("cardkit_update")) == 1
        await engine.submit(
            env("tool.end", turn="t2", payload={"tool": "grep", "duration_ms": 2300}, seq=2)
        )
        # 改写类 → 合并窗口（未到期前不发生 API）
        assert len(transport.calls_of("cardkit_update")) == 1
        await asyncio.sleep(0.12)  # > panel_flush_sec=0.05
        assert len(transport.calls_of("cardkit_update")) == 2
        st = engine._chats[CHAT].turn
        assert st is not None
        row = st.render.panel_rows[-1]
        assert row.duration_ms == 2300 and row.pending is False
    finally:
        await engine.aclose()


async def test_final_seals_with_full_update(transport):
    """定稿（#14）：终值 patch + 一次全量写回（关流式 + 页脚 + 面板收口）.

    settings / batch_update 端点 404 后，定稿不再走局部通道；一次
    cardkit_close_streaming（底层 cardkit_update）写回已定稿的完整卡。
    """
    engine, events, clock, *_, ack4 = await _happy_stream(transport)
    try:
        assert ack4.verdict is Verdict.CONSUMED
        box = engine._chats[CHAT]
        st = box.sealed[-1]
        assert st.phase is TurnPhase.SEALED and box.turn is None
        # 定稿管道：终值 patch + 一次全量写回（无 batch_update）
        streams = transport.calls_of("cardkit_stream_element")
        assert streams[-1]["content"] == "你好，世界"
        assert len(transport.calls_of("cardkit_batch_update")) == 0
        closes = transport.calls_of("cardkit_close_streaming")
        assert len(closes) == 1
        sealed = closes[0]["card"]
        assert sealed["config"]["streaming_mode"] is False
        assert sealed["config"]["summary"]["content"]
        elements = sealed["body"]["elements"]
        assert any(e.get("tag") == "hr" for e in elements)  # 页脚已拼入
        assert not any(e.get("element_id") == "loading_icon" for e in elements)
    finally:
        await engine.aclose()


async def test_final_with_stats_renders_footer(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    try:
        await engine.submit(env("turn.start", turn="t_stats"))
        await engine.submit(env("answer.delta", turn="t_stats", payload={"delta": "x"}, seq=1))
        await engine.submit(env(
            "answer.final", turn="t_stats",
            payload={"stats": {"duration_ms": 4200, "model": "glm-5",
                               "context_tokens": {"used": 12000, "max": 200000}}},
            seq=2,
        ))
        st = engine._chats[CHAT].sealed[-1]
        footer = st.render.footer
        assert footer is not None
        assert footer.duration_ms == 4200
        assert footer.model == "glm-5"
        assert footer.context_used == 12000 and footer.context_max == 200000
    finally:
        await engine.aclose()


async def test_empty_answer_seal_skips_final_patch(transport):
    """纯工具回合 answer 为空是合法终态（99992402 空守卫；前代语义）."""
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    try:
        await engine.submit(env("turn.start", turn="t4"))
        await engine.submit(env("tool.start", turn="t4", payload={"tool": "grep"}, seq=1))
        await engine.submit(env("answer.final", turn="t4", payload={}, seq=2))
        # answer 空 → 终值 patch 跳过；面板走 update 通道
        assert len(transport.calls_of("cardkit_stream_element")) == 0
        assert len(transport.calls_of("cardkit_close_streaming")) == 1
    finally:
        await engine.aclose()


async def test_thinking_rounds_grouped_and_closed(transport):
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="t5"))
        await engine.submit(env("thinking.delta", turn="t5", payload={"text": "思考A"}, seq=1))
        await engine.submit(env("thinking.delta", turn="t5", payload={"text": "B"}, seq=2))
        await engine.submit(env(
            "thinking.round.end", turn="t5",
            payload={"round": 1, "duration_ms": 1200}, seq=3,
        ))
        st = engine._chats[CHAT].turn
        assert st is not None
        thinking = [r for r in st.render.panel_rows if r.kind.value == "thinking"]
        assert len(thinking) == 1  # 同轮追加合并为一行
        assert thinking[0].text == "思考AB"
        assert thinking[0].round_index == 1
        assert thinking[0].round_finalized is True
        assert thinking[0].round_elapsed_ms == 1200
        assert st.last_round_closed == 1
    finally:
        await engine.aclose()


async def test_out_of_order_frames_still_consumed_with_detail(transport):
    """乱序检测 best-effort：仍消费，ack.detail 附加标记（01-protocol §5.3）."""
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="t6"))
        ack1 = await engine.submit(env("answer.delta", turn="t6", payload={"delta": "a"}, seq=5))
        ack2 = await engine.submit(env("answer.delta", turn="t6", payload={"delta": "b"}, seq=3))
        assert ack1.verdict is Verdict.CONSUMED
        assert ack2.verdict is Verdict.CONSUMED
        assert "out_of_order=true" in ack2.detail
        assert "out_of_order=true" not in ack1.detail
    finally:
        await engine.aclose()


async def test_duplicate_frame_id_not_reconsumed(transport):
    """frame_id 幂等（01-protocol §5.4）：重复帧返回同一回执，不重复消费."""
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="t7"))
        raw = env("answer.delta", turn="t7", payload={"delta": "x"}, frame="f_dup", seq=1)
        ack1 = await engine.submit(raw)
        ack2 = await engine.submit(dict(raw))
        assert ack1.verdict is Verdict.CONSUMED
        assert ack2.verdict == ack1.verdict
        streams = transport.calls_of("cardkit_stream_element")
        assert len(streams) == 1  # 未重复消费
    finally:
        await engine.aclose()


async def test_snapshot_replaces_delta_appends(transport):
    """双帧语义（01-protocol §5.2）：snapshot 无条件替换，delta 追加."""
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="t8"))
        await engine.submit(env("answer.delta", turn="t8", payload={"delta": "abc"}, seq=1))
        await engine.submit(env("answer.delta", turn="t8", payload={"delta": "def"}, seq=2))
        st = engine._chats[CHAT].turn
        assert st is not None and st.render.answer_text == "abcdef"
        await engine.submit(env("answer.snapshot", turn="t8", payload={"text": "干净快照"}, seq=3))
        assert st.render.answer_text == "干净快照"
        streams = transport.calls_of("cardkit_stream_element")
        assert streams[-1]["content"] == "干净快照"
    finally:
        await engine.aclose()
