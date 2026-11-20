"""L2 clarify 验收（04-knowledge-freeze §4 clarify 基线 / 02-engine §1.1 PAUSED 态）.

- 边界窗口：answer.final 后窗口内 clarify.request → 同卡暂停续流
  （前代边界 finalize 与真终态参数一致竞态的协议化，02-engine §2 T_pause）；
- 窗口超时：真落定（PAUSE_EXPIRE 定时器真实触发——真实短 sleep 手法，
  settings 调小，绝不 sleep(300)）；
- 表单三态：直接调渲染层（烟雾级断言，详测归 L1 renderer 套件）。
"""

from __future__ import annotations

import json

from lark_streaming.engine import TurnPhase, Verdict
from lark_streaming.renderer import (
    ANSWER_ELEMENT_ID,
    CLARIFY_CALLBACK_KEY,
    ClarifyForm,
    render_clarify_answered,
    render_clarify_pending,
    render_clarify_unauthorized,
)
from lark_streaming.renderer.theme import DEFAULT_THEME

from tests.harness.framework import CHAT, json_contains


async def test_clarify_window_hit_pause_and_resume_same_card(make_harness):
    """边界窗口命中：SEALING 挂起 → clarify.request → PAUSED → resume 续流 → 落定."""
    h = await make_harness(clarify_window_sec=1.2, panel_flush_sec=0)
    turn = "t_cl1"
    await h.submit("turn.start", turn=turn)
    await h.submit("answer.delta", turn=turn, payload={"delta": "前段"}, seq=1)
    a_final = await h.submit("answer.final", turn=turn, payload={"text": "前段"}, seq=2)
    assert a_final.verdict is Verdict.CONSUMED
    assert a_final.detail == "seal_window_open"  # 窗口挂起（SEALING + pause_pending）
    st = h.engine._chats[CHAT].turn
    assert st is not None and st.phase is TurnPhase.SEALING
    assert st.pause_pending is True
    # 窗口内 clarify.request → PAUSED（前代 v2.0.26 同卡续流语义的协议化）
    a_cr = await h.submit("clarify.request", turn=turn, payload={
        "form": {"kind": "select", "question": "选哪个方案？", "options": ["甲", "乙"]},
    }, seq=3)
    assert a_cr.verdict is Verdict.CONSUMED
    assert st.phase is TurnPhase.PAUSED
    assert st.pause_row_present is True
    wait_text = DEFAULT_THEME.text("clarify_wait")[0]
    assert any(r.text == wait_text for r in st.render.panel_rows)  # 暂停行在渲染态
    turn_card = h.server.cards[st.card_id]
    # 原卡保流式：零 close_streaming，settings_log 无 streaming_mode:false
    assert turn_card.streaming_mode is True
    assert not any(e["streaming_mode"] is False for e in turn_card.settings_log)
    # 暂停行 flush 被 _flush_panel 的 STREAMING 相位守卫跳过（PAUSED 期零
    # update——实际语义，见报告注记；行只落引擎渲染态）
    assert h.server.count("update") == 0
    # 澄清独立卡经 CardKit 实体投递（回合卡 + 澄清卡）
    assert h.server.count("create") == 2
    assert h.server.count("send") == 2
    # clarify.answer → resume：同卡续流，暂停行摘除 + flush 恢复
    a_ca = await h.submit("clarify.answer", turn=turn, payload={
        "values": {"clarify_input": "乙"},
    }, seq=4)
    assert a_ca.verdict is Verdict.CONSUMED
    assert st.phase is TurnPhase.STREAMING
    assert st.pause_row_present is False
    await h.settle()
    assert h.server.count("update") == 1  # resume 冲刷（STREAMING 相位恢复）
    assert not json_contains(turn_card.card_json, wait_text)
    # 后续 final 落定：窗口挂起 → 真实 pause 定时器（1.2s）到点 PAUSE_EXPIRE → SEALED
    a_f2 = await h.submit("answer.final", turn=turn, payload={"text": "前段定稿"}, seq=5)
    assert a_f2.verdict is Verdict.CONSUMED
    await h.settle_timers(1.5)
    box = h.engine._chats[CHAT]
    assert len(box.sealed) == 1 and box.turn is None
    assert turn_card.elements[ANSWER_ELEMENT_ID]["content"] == "前段定稿"
    assert any(e["streaming_mode"] is False for e in turn_card.settings_log)


async def test_clarify_window_timeout_seals_for_real(make_harness):
    """窗口超时真落定：不发 clarify → PAUSE_EXPIRE → SEALED；此后 clarify fallback."""
    h = await make_harness(clarify_window_sec=0.15, panel_flush_sec=0)
    turn = "t_cl2"
    await h.submit("turn.start", turn=turn)
    await h.submit("answer.delta", turn=turn, payload={"delta": "等待落定"}, seq=1)
    await h.submit("answer.final", turn=turn, payload={"text": "等待落定"}, seq=2)
    await h.settle_timers(0.4)  # 真实 pause 定时器到点 → 真落定
    box = h.engine._chats[CHAT]
    assert len(box.sealed) == 1 and box.turn is None
    card = h.last_card()
    assert card.elements[ANSWER_ELEMENT_ID]["content"] == "等待落定"
    assert any(e["streaming_mode"] is False for e in card.settings_log)
    # turn 已终结 → clarify.request fallback（clarify_without_stream）
    a = await h.submit("clarify.request", turn=turn, payload={
        "form": {"kind": "input", "question": "q"},
    }, seq=3)
    assert a.verdict is Verdict.FALLBACK
    assert a.detail == "clarify_without_stream"


def test_clarify_form_three_states_smoke():
    """表单三态渲染烟雾级断言（select / answered / unauthorized；详测归 L1）。"""
    form = ClarifyForm(kind="select", question="继续吗？", options=("是", "否"))
    pending = render_clarify_pending(form, DEFAULT_THEME, clarify_id="c1")
    answered = render_clarify_answered(form, DEFAULT_THEME, selected="是", clarify_id="c1")
    unauthorized = render_clarify_unauthorized(form, DEFAULT_THEME)
    # pending：下拉 + 自定义输入，回调统一挂 lsp_clarify_action 命名空间
    tags_p = {e["tag"] for e in pending["body"]["elements"]}
    assert {"select_static", "input"} <= tags_p
    assert json.dumps(pending, ensure_ascii=False).count(CLARIFY_CALLBACK_KEY) >= 2
    assert json_contains(pending, "继续吗？")
    # answered（软锁）：按钮 + 选择摘要，无下拉
    tags_a = {e["tag"] for e in answered["body"]["elements"]}
    assert "select_static" not in tags_a and "button" in tags_a
    assert json_contains(answered, "已选择")
    assert json_contains(answered, "已提交")
    # unauthorized（fail-closed）：零交互控件 + 不渲染表单值
    tags_u = {e["tag"] for e in unauthorized["body"]["elements"]}
    assert not ({"select_static", "input", "button"} & tags_u)
    assert json_contains(unauthorized, "未通过授权校验")
    assert not json_contains(unauthorized, "是")
