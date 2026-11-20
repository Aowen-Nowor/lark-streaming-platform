"""L2 生命周期验收（04-knowledge-freeze §4 生命周期基线）.

与 tests/engine/test_lifecycle.py（L1）的区别：本文件跑 **真实
FeishuTransport × mock HTTP**——cardkit_create / send_card_by_id /
stream_element / settings（close_streaming）全链路 wire 在环，验证的
是「一回合一卡、定稿四步、回合外投递、中止收敛、停机 seal_all」这些
**可观测的卡片终态**，而非 transport 方法面调用次数。

clarify 窗口竞态归 test_l2_clarify.py（02-engine §2 T_pause 语义）；
本文件统一 clarify_window_sec=0 走直落定路径。
"""

from __future__ import annotations

import json

from lark_streaming.engine import TurnPhase, Verdict
from lark_streaming.renderer import (
    ANSWER_ELEMENT_ID,
    LOADING_ELEMENT_ID,
)

from tests.harness.framework import CHAT, json_contains


async def test_full_turn_single_card_streamed_and_sealed(make_harness):
    """E2E 全回合：turn.start → 3×delta → final；ack 全程 consumed，一回合一卡."""
    h = await make_harness(clarify_window_sec=0)
    turn = "t_full"
    acks = await h.full_turn(["你好", "，", "世界"], chat=CHAT, turn=turn)
    assert len(acks) == 5
    assert all(a.verdict is Verdict.CONSUMED for a in acks)
    # 恰好 1 次建卡 + 1 次锚定直发（无 reply_to → send_card_by_id 通道；
    # wire：POST /im/v1/messages content={"type":"card","data":{"card_id":...}}）
    assert h.server.count("create") == 1
    sends = [m for m in h.server.messages if m.content.get("type") == "card"]
    assert h.server.count("send") == 1 and len(sends) == 1
    assert acks[0].message_id == sends[0].message_id
    assert sends[0].card_id == h.last_card().card_id
    # 最终卡 answer 元素 content == 完整文本（mock 卡 elements 实测终态）
    card = h.last_card()
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "你好，世界"
    # close_streaming 已调用（settings_log 有 streaming_mode:false，含摘要）
    closes = [e for e in card.settings_log if e["streaming_mode"] is False]
    assert len(closes) == 1 and closes[0]["summary"]
    # 定稿装饰：spinner 已删
    ids = {e.get("element_id") for e in card.card_json["body"]["elements"]}
    assert LOADING_ELEMENT_ID not in ids
    # 逐帧恰好一个 ack（frame.ack 关联观测）
    assert len(h.acks_of(f"{turn}_final")) == 1


async def test_deliver_out_of_turn_static_card_no_hijack(harness):
    """回合外 deliver：独立静态卡，不劫持活跃流式卡（02-engine §8 通道隔离）."""
    h = harness
    # 无活跃卡：deliver → 独立静态卡（engine._do_deliver 走 send_card_to_chat
    # 直发通道，不经 CardKit 实体建卡；cron 溯源页脚）
    ack = await h.submit("deliver", payload={
        "text": "cron 结果", "footer": {"job_id": "cron-1"},
    })
    assert ack.verdict is Verdict.CONSUMED and ack.message_id
    assert h.server.count("create") == 0  # 实际语义：静态卡不建实体（见报告偏差注记）
    statics = [m for m in h.server.messages if m.content.get("schema") == "2.0"]
    assert len(statics) == 1
    assert statics[0].message_id == ack.message_id
    assert "cron 结果" in json.dumps(statics[0].content, ensure_ascii=False)
    assert "job_id: cron-1" in json.dumps(statics[0].content, ensure_ascii=False)
    # 与活跃流式卡共存：deliver 不劫持、不合并
    await h.submit("turn.start", turn="t_d1")
    ack2 = await h.submit("deliver", payload={"text": "进行中的旁路投递"})
    assert ack2.verdict is Verdict.CONSUMED
    box = h.engine._chats[CHAT]
    assert box.turn is not None and box.turn.phase is TurnPhase.STREAMING
    assert len(h.server.cards) == 1  # 未新建实体卡
    # send 总数 = 首次 deliver + turn.start 直发 + 二次 deliver（互不干扰）
    assert h.server.count("send") == 3


async def test_abort_seals_card_and_late_frames_fall_back(harness):
    """turn.abort：流式中止 → 卡收敛（终值 + 关流式）；迟到帧 fallback."""
    h = harness
    turn = "t_abort"
    await h.submit("turn.start", turn=turn)
    a2 = await h.submit("answer.delta", turn=turn, payload={"delta": "写到一半"}, seq=1)
    assert a2.verdict is Verdict.CONSUMED
    a3 = await h.submit("turn.abort", turn=turn, payload={"reason": "用户取消"}, frame="f_abort")
    assert a3.verdict is Verdict.CONSUMED
    card = h.last_card()
    # 卡收敛：close_streaming（settings_log streaming_mode:false）+ 已缓冲内容落卡
    assert any(e["streaming_mode"] is False for e in card.settings_log)
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "写到一半"
    # 中止页脚（is_aborted + reason 警示行）
    assert json_contains(card.card_json, "用户取消")
    box = h.engine._chats[CHAT]
    assert box.turn is None and len(box.sealed) == 1
    st = box.sealed[-1]
    assert st.aborted is True
    assert st.render.footer.is_aborted is True
    assert st.render.footer.abort_reason == "用户取消"
    # 后续同 turn 帧 fallback（guards 实际 detail：no_active_card，
    # 守卫 owner=stale_turn_frame/answer_no_active_card——见报告注记）
    a4 = await h.submit("answer.delta", turn=turn, payload={"delta": "迟到"}, seq=2)
    assert a4.verdict is Verdict.FALLBACK
    assert a4.detail == "no_active_card"


async def test_shutdown_seals_active_turn_then_fallback(harness):
    """seal_all_on_shutdown：活跃卡先定稿再关停；关闭后帧 fallback（02-engine §7）."""
    h = harness
    turn = "t_sd"
    await h.submit("turn.start", turn=turn)
    await h.submit("answer.delta", turn=turn, payload={"delta": "停机前内容"}, seq=1)
    n_close = len([e for e in h.last_card().settings_log if e["streaming_mode"] is False])
    await h.engine.aclose()  # 内含 seal_all_on_shutdown
    # 先 close+final 再 shutdown：卡终态已落
    card = h.last_card()
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "停机前内容"
    closes = [e for e in card.settings_log if e["streaming_mode"] is False]
    assert len(closes) == n_close + 1
    # shutdown 生命周期通告（01-protocol §4.6 封闭集）
    assert h.lifecycle and h.lifecycle[-1].type == "shutdown"
    assert h.lifecycle[-1].detail == "seal_all_on_shutdown"
    # 关闭后 submit → FALLBACK(engine_shutdown)，仍必得回执（零丢失红线）
    ack = await h.submit("answer.delta", turn=turn, payload={"delta": "x"}, seq=2)
    assert ack.verdict is Verdict.FALLBACK
    assert ack.detail == "engine_shutdown"
    assert ack.frame_id
