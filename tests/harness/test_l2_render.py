"""L2 渲染集成验收（04-knowledge-freeze §4 渲染基线——集成级）.

与 tests/renderer/（L1 纯函数 Golden）的分工：L1 验渲染产物逐字节；
L2 验「引擎时点表 → 渲染产物 → CardKit 通道」的集成行为（02-engine §4
时点表 / renderer/streaming.py 模块 docstring 的通道绑定）——面板走
cardkit_update 全量通道、定稿亦走 cardkit_update 全量写回（#14：
settings / batch_update 端点 404）、字节预算裁剪发生在全量更新
之前（03-card-schema §6 / 前代 _CARD_BYTE_BUDGET 生产实测值）。
"""

from __future__ import annotations

import json

from lark_streaming.engine import Verdict
from lark_streaming.renderer import (
    ANSWER_ELEMENT_ID,
    LOADING_ELEMENT_ID,
    RowKind,
    UNIFIED_PANEL_ELEMENT_ID,
    measure_card_bytes,
)

from tests.harness.framework import CHAT


async def test_lazy_panel_no_empty_panel_for_pure_answer(make_harness):
    """懒面板：纯 answer 回合无空面板（前代 v2.0.24 体验定案）."""
    h = await make_harness(clarify_window_sec=0)
    acks = await h.full_turn(["纯", "回答"], chat=CHAT, turn="t_lazy")
    assert all(a.verdict is Verdict.CONSUMED for a in acks)
    # 恰好 1 次 cardkit_update 全量调用 = 定稿全量写回（#14）；无面板行 →
    # 无面板 flush 全量调用
    assert h.server.count("update") == 1
    # 定稿不再走 batch_update 局部通道（#14：端点 404）
    assert h.server.count("batch_update") == 0
    # 初始卡与终态卡均无面板元素
    card = h.last_card()
    ids = {e.get("element_id") for e in card.card_json["body"]["elements"]}
    assert UNIFIED_PANEL_ELEMENT_ID not in ids
    # 定稿卡已关流式且含页脚
    assert card.streaming_mode is False
    assert any(e.get("tag") == "hr" for e in card.card_json["body"]["elements"])


async def test_panel_rows_land_via_full_update_channel(make_harness):
    """面板装配：tool/thinking/interim 行经 cardkit_update 全量通道上卡."""
    h = await make_harness(clarify_window_sec=0, panel_flush_sec=0)
    turn = "t_panel"
    await h.submit("turn.start", turn=turn)
    a1 = await h.submit(
        "tool.start", turn=turn,
        payload={"tool": "grep", "preview": "rg foo"}, seq=1,
    )
    assert a1.verdict is Verdict.CONSUMED
    # 结构性新增行即时上卡（OPT-1a 红线：不合并）
    assert h.server.count("update") >= 1
    await h.submit("tool.end", turn=turn, payload={
        "tool": "grep", "duration_ms": 2300, "output_preview": "3 matches",
    }, seq=2)
    await h.submit("thinking.delta", turn=turn, payload={"text": "思考中"}, seq=3)
    await h.submit("thinking.round.end", turn=turn, payload={
        "round": 1, "duration_ms": 1200,
    }, seq=4)
    await h.submit("interim", turn=turn, payload={"text": "阶段性播报"}, seq=5)
    await h.settle_timers(0.1)  # 改写类行走 0s 合并窗口（真实定时器）
    card = h.last_card()
    dump = json.dumps(card.card_json, ensure_ascii=False)
    assert "Search text (2.3 s)" in dump  # 工具行 + 耗时标签回填
    assert "3 matches" in dump            # 工具命令摘要行
    assert "第 1 轮" in dump and "思考中" in dump  # 思考分组标题 + 正文
    assert "阶段性播报" in dump
    # tool.start 同名重复：引擎逐行追加（Q9 (×N) 合并语义在 legacy 适配层，
    # engine._apply_panel_mutation 实际无合并——见报告偏差注记）
    await h.submit("tool.start", turn=turn, payload={"tool": "grep"}, seq=6)
    st = h.engine._chats[CHAT].turn
    assert st is not None
    tool_rows = [r for r in st.render.panel_rows if r.kind is RowKind.TOOL]
    assert len(tool_rows) == 2


async def test_byte_budget_trim_on_full_update(harness):
    """字节预算：全量更新前裁剪到 27000（Q5 / 前代 _CARD_BYTE_BUDGET）."""
    h = harness
    turn = "t_budget"
    await h.submit("turn.start", turn=turn)
    await h.submit("answer.delta", turn=turn, payload={"delta": "甲" * 15000}, seq=1)
    await h.submit("answer.delta", turn=turn, payload={"delta": "乙" * 15000}, seq=2)
    # 面板行触发全量更新：大 answer → 裁剪（interim→thinking→answer 尾部顺序，L1 保证）
    await h.submit("tool.start", turn=turn, payload={"tool": "grep"}, seq=3)
    updates = h.server.calls_of("update")
    assert updates
    card_payload = json.loads(updates[-1].body["card"]["data"])
    assert measure_card_bytes(card_payload) <= 27_000
    ans_el = next(
        e for e in card_payload["body"]["elements"]
        if e.get("element_id") == ANSWER_ELEMENT_ID
    )
    assert "…（已截断）" in ans_el["content"]
    # 流式通道原文直发不裁剪（engine._stream_answer_now 语义；见报告注记）
    streams = h.server.calls_of("stream_element")
    assert len(streams[-1].body["content"].encode("utf-8")) > 27_000


async def test_footer_seal_and_no_duplicate(make_harness):
    """页脚：stats 全字段落卡；重复 final 不重放页脚（零 API 路径）。"""
    h = await make_harness(clarify_window_sec=0)
    turn = "t_footer"
    stats = {
        "duration_ms": 4200, "model": "glm-5",
        "context_tokens": {"used": 12000, "max": 200000},
    }
    await h.submit("turn.start", turn=turn)
    await h.submit("answer.delta", turn=turn, payload={"delta": "答案正文"}, seq=1)
    ack = await h.submit("answer.final", turn=turn, payload={
        "text": "答案正文", "stats": stats,
    }, seq=2)
    assert ack.verdict is Verdict.CONSUMED
    card = h.last_card()
    dump = json.dumps(card.card_json, ensure_ascii=False)
    assert "glm-5" in dump
    assert "已完成" in dump           # status 恒显示（zh i18n）
    assert "12.0K/200.0K" in dump     # 上下文用量
    assert any(e.get("tag") == "hr" for e in card.card_json["body"]["elements"])
    assert not any(
        e.get("element_id") == LOADING_ELEMENT_ID
        for e in card.card_json["body"]["elements"]
    )
    # 重复 answer.final：回合已终结 → fallback（guards 实际语义，见报告注记）
    # ——零 API、无重复页脚
    n_batch = h.server.count("batch_update")
    dup = await h.submit("answer.final", turn=turn, payload={
        "text": "答案正文", "stats": stats,
    }, seq=3, frame="f_dup_final")
    assert dup.verdict is Verdict.FALLBACK
    assert dup.frame_id == "f_dup_final"  # 回执全量（零丢失红线）
    assert h.server.count("batch_update") == n_batch
    assert sum(
        1 for e in card.card_json["body"]["elements"] if e.get("tag") == "hr"
    ) == 1
