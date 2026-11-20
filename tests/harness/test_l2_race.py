"""L2 竞态验收（04-knowledge-freeze §4 竞态基线 / §2 竞态事故档案）.

全部场景跑真实 FeishuTransport × mock HTTP：
- 建卡窗口并发帧（Q3 守卫回归：10-11 23:13 孤儿卡 / 10-12 14:30 一轮 3 卡
  事故的根治——per-chat 队列 + CREATING 幂等屏障）；
- 跨 chat 并行互不干扰（02-engine §3.1）；
- 新 turn 打断旧 turn（01-protocol §5.1 超权）；
- 主动续期 Q2 主流程 + 200850 撞线自愈兜底（11-07 两例 600s 撞线）；
- 300317 同号重放自愈（Q1：s98 真飞书实验定案 seq+1 闭环）。
"""

from __future__ import annotations

import asyncio
import json

import pytest

from lark_streaming.engine import TurnState, Verdict
from lark_streaming.renderer import ANSWER_ELEMENT_ID
from lark_streaming.transport import FeishuAPIError, call_with_seq_recovery

from tests.harness.framework import CHAT, FakeClock


def _reopen_indexes(log) -> list[int]:
    """call_log 中 settings streaming_mode:true（重开/续期）的索引."""
    out = []
    for i, c in enumerate(log):
        if c.op == "settings":
            config = (json.loads(c.body["settings"]) or {}).get("config") or {}
            if config.get("streaming_mode") is True:
                out.append(i)
    return out


# ── 建卡窗口并发帧（Q3 守卫回归；02-engine §3.2）──────────────────────────


async def test_create_window_concurrent_frames_single_card(harness):
    """create 挂起 0.2s 内齐发 1 start + 5 delta：恰好一卡一发送，全量消费."""
    h = harness
    h.server.delay("create", 0.2)
    turn = "t_race1"
    tasks = [h.submit("turn.start", chat=CHAT, turn=turn, frame="f_rs")]
    for i in range(5):
        tasks.append(h.submit(
            "answer.delta", chat=CHAT, turn=turn,
            payload={"delta": f"片{i}"}, seq=i + 1,
        ))
    acks = await asyncio.gather(*tasks)
    assert all(a.verdict is Verdict.CONSUMED for a in acks)
    # Q3 幂等屏障：并发帧排队等待而非触发第二次建卡
    assert h.server.count("create") == 1
    assert h.server.count("send") == 1
    # 最终卡含全部 delta 文本（按序消费）
    card = h.last_card()
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "片0片1片2片3片4"


# ── 跨 chat 并行（02-engine §3.1：跨 chat 完全并行，chat 内严格串行）────────


async def test_cross_chat_parallel_isolation(make_harness):
    h = await make_harness(clarify_window_sec=0)
    acks_a, acks_b = await asyncio.gather(
        h.full_turn(["甲答案"], chat="oc_chat_a", turn="t_a"),
        h.full_turn(["乙答案"], chat="oc_chat_b", turn="t_b"),
    )
    assert all(a.verdict is Verdict.CONSUMED for a in acks_a + acks_b)
    assert h.server.count("create") == 2
    assert h.server.count("send") == 2
    cards = list(h.server.cards.values())
    assert len(cards) == 2 and cards[0].card_id != cards[1].card_id
    # 两张卡互不干扰、互不串写
    contents = {c.elements[ANSWER_ELEMENT_ID]["content"] for c in cards}
    assert contents == {"甲答案", "乙答案"}
    for m in h.server.messages:
        if m.content.get("type") == "card":
            assert m.receive_id in ("oc_chat_a", "oc_chat_b")


# ── 新 turn 打断旧 turn（01-protocol §5.1 超权；02-engine §6）────────────────


async def test_new_turn_supersedes_old_streaming_card(harness):
    h = harness
    await h.submit("turn.start", turn="t_old")
    await h.submit("answer.delta", turn="t_old", payload={"delta": "旧回合"}, seq=1)
    old_card = h.last_card()
    ack = await h.submit("turn.start", turn="t_new")
    assert ack.verdict is Verdict.CONSUMED
    new_card = h.last_card()
    assert new_card.card_id != old_card.card_id
    # 旧卡被 close_streaming（superseded 真落定）
    assert any(e["streaming_mode"] is False for e in old_card.settings_log)
    assert h.server.count("create") == 2
    box = h.engine._chats[CHAT]
    assert box.turn is not None and box.turn.turn_id == "t_new"
    # 旧回合迟到帧 → fallback（stale_turn_frame 守卫）
    late = await h.submit("answer.delta", turn="t_old", payload={"delta": "x"}, seq=2)
    assert late.verdict is Verdict.FALLBACK
    assert late.detail == "no_active_card"


# ── 主动续期（Q2 主流程：T_renew 龄 ≥ 阈值 → 更新前先重开会话）───────────────


async def test_proactive_session_renew_before_200850(make_harness):
    clock = FakeClock(1000.0)
    # session_renew_age_sec 钳制下限 60（02-engine §2 钉制 60~590）
    h = await make_harness(session_renew_age_sec=60, clock=clock)
    await h.submit("turn.start", turn="t_renew")
    card = h.last_card()
    assert card.streaming_mode is True
    clock.advance(61)  # 会话龄 61s ≥ 60s
    ack = await h.submit("answer.delta", turn="t_renew", payload={"delta": "续期后帧"}, seq=1)
    assert ack.verdict is Verdict.CONSUMED
    # 该 delta 的 stream_element 前出现 reopen_streaming（settings streaming_mode:true）
    log = h.server.call_log
    reopens = _reopen_indexes(log)
    assert reopens, "主动续期未发生"
    i_delta = next(
        i for i, c in enumerate(log)
        if c.op == "stream_element" and c.body.get("content") == "续期后帧"
    )
    assert reopens[0] < i_delta
    # 流式正常，无 200850
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "续期后帧"
    assert card.streaming_mode is True
    assert not any(c.code == 200850 for c in log)


# ── 200850 撞线自愈（Q2 兜底：官方恢复动作 = settings 重开 + seq 重发）────────


async def test_stream_timeout_200850_self_heal(harness):
    h = harness
    await h.submit("turn.start", turn="t_850")
    await h.submit("answer.delta", turn="t_850", payload={"delta": "前段"}, seq=1)
    card = h.last_card()
    h.server.expire_streaming(card.card_id)  # 模拟官方 ~600s 会话寿命撞线
    ack = await h.submit("answer.delta", turn="t_850", payload={"delta": "撞线帧"}, seq=2)
    # 同帧自愈：200850 → reopen → 重发 → 仍 CONSUMED（不上浮 verdict 变化）
    assert ack.verdict is Verdict.CONSUMED
    log = h.server.call_log
    i_850 = next(i for i, c in enumerate(log) if c.code == 200850)
    assert log[i_850].op == "stream_element"
    reopens = [i for i in _reopen_indexes(log) if i > i_850]
    assert reopens, "撞线后未执行官方恢复动作（reopen）"
    i_resend = next(
        i for i, c in enumerate(log)
        if i > reopens[0] and c.op == "stream_element"
        and c.body.get("content") == "前段撞线帧"
    )
    assert log[i_resend].code == 0
    # reopen 解除武装，后续流式正常
    assert card.streaming_mode is True
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "前段撞线帧"
    ack3 = await h.submit("answer.delta", turn="t_850", payload={"delta": "后续"}, seq=3)
    assert ack3.verdict is Verdict.CONSUMED
    assert not any(c.code == 200850 for c in log[i_resend + 1:])
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "前段撞线帧后续"


# ── 300317 同号重放自愈（Q1：strict 台架 + call_with_seq_recovery 闭环）──────


async def test_seq_conflict_300317_self_heal(harness):
    h = harness
    await h.submit("turn.start", turn="t_317")
    await h.submit("answer.delta", turn="t_317", payload={"delta": "已流内容"}, seq=1)
    card = h.last_card()
    h.server.strict_sequence(card.card_id, True)
    last = card.last_sequence
    # 同号重放：直接走真实 transport → 300317 原样上抛（非瞬态、不重试）
    with pytest.raises(FeishuAPIError) as ei:
        await h.transport.cardkit_stream_element(
            card.card_id, ANSWER_ELEMENT_ID, "重放", sequence=last,
        )
    assert ei.value.code == 300317
    # call_with_seq_recovery 自愈：本地 seq 对齐服务端（+1）→ 重发同内容成功
    ts = TurnState(chat_id=CHAT, turn_id="t_317_replay")
    ts.card_id = card.card_id
    ts.seq = last
    await call_with_seq_recovery(
        ts,
        lambda s: h.transport.cardkit_stream_element(
            card.card_id, ANSWER_ELEMENT_ID, "重放成功", sequence=s,
        ),
        what="l2:replay",
    )
    assert ts.seq == last + 1
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "重放成功"
    assert card.last_sequence == last + 1
