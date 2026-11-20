"""hermes 适配器单测 — 元数据契约 → LSEP 事件流映射（05-adapters §5.1）.

映射语义逐点对齐设计契约；降级聚合（§1.4）与回合前暂存（前代 pending
语义）为本层核心验收面。端到端（真实引擎消费）另设集成用例。
"""

from __future__ import annotations

import pytest

from lark_streaming.adapters import HermesAdapter, NativeAction
from lark_streaming.engine import Verdict
from tests.adapters.conftest import (  # noqa: F401
    REAL_TERMINAL_BLOCK,
    REAL_THINK,
    FakeEngine,
)

CHAT = "oc_hermes_chat"


def build(adapter: HermesAdapter, engine: FakeEngine) -> tuple[HermesAdapter, FakeEngine]:
    return adapter, engine


@pytest.fixture
def adapter_and_engine() -> tuple[HermesAdapter, FakeEngine]:
    engine = FakeEngine()
    return HermesAdapter(engine), engine


class TestStreamMapping:
    """expect_edits → turn.start + answer.snapshot 序列."""

    async def test_first_send_opens_turn(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        res = await adapter.send(CHAT, "首帧答案", {"expect_edits": True}, reply_to="om_1",
                                 title="任务")
        assert res.action is NativeAction.CONSUMED
        assert engine.events() == ["turn.start", "answer.snapshot"]
        start, snap = engine.submitted
        assert start["payload"]["reply_to"] == "om_1"
        assert start["payload"]["title"] == "任务"
        assert snap["payload"]["text"] == "首帧答案"
        assert snap["turn_id"] == start["turn_id"]
        assert snap["seq"] >= 0
        assert res.turn_id == start["turn_id"]
        assert res.frames[0].verdict == "consumed"

    async def test_subsequent_send_same_explicit_turn_idempotent(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        r1 = await adapter.send(CHAT, "首帧", {"expect_edits": True}, turn_id="T1")
        n1 = len(engine.submitted)
        r2 = await adapter.send(CHAT, "续帧", {"expect_edits": True}, turn_id="T1")
        assert engine.events()[n1:] == ["answer.snapshot"]  # 幂等续流，无第二次 turn.start
        assert r2.turn_id == r1.turn_id

    async def test_send_without_turn_id_supersedes(self, adapter_and_engine):
        # expect_edits = 回合首帧：无显式 turn_id 时每 send 开新回合
        # （引擎对未定稿旧回合 seal_and_create——10-11 孤儿卡根治）
        adapter, engine = adapter_and_engine
        r1 = await adapter.send(CHAT, "旧回合", {"expect_edits": True})
        r2 = await adapter.send(CHAT, "新回合", {"expect_edits": True})
        assert r1.turn_id != r2.turn_id
        assert engine.events() == ["turn.start", "answer.snapshot",
                                   "turn.start", "answer.snapshot"]

    async def test_edit_stream_and_final(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        r1 = await adapter.send(CHAT, "首帧", {"expect_edits": True})
        turn_id = r1.turn_id
        res = await adapter.edit(CHAT, "更新正文")
        assert engine.events()[-1] == "answer.snapshot"
        assert engine.submitted[-1]["turn_id"] == turn_id
        fin = await adapter.edit(CHAT, "最终正文", finalize=True)
        assert engine.events()[-1] == "answer.final"
        assert engine.submitted[-1]["payload"]["text"] == "最终正文"
        assert fin.action is NativeAction.CONSUMED

    async def test_orphan_edit_native(self, adapter_and_engine):
        # 孤儿编辑（无活跃回合）→ 原生履行（hermes 编辑自己的消息）
        adapter, _ = adapter_and_engine
        res = await adapter.edit(CHAT, "正文", finalize=False)
        assert res.action is NativeAction.NATIVE
        assert res.detail == "orphan_edit_no_turn"
        res = await adapter.edit(CHAT, "正文", finalize=True)
        assert res.action is NativeAction.NATIVE


class TestFinalMapping:
    """notify=True → answer.final；无活跃回合微回合自愈."""

    async def test_notify_with_active_turn(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        res = await adapter.send(CHAT, "最终答案", {"notify": True},
                                 stats={"model": "glm-4", "context_tokens": {"used": 10, "max": 100}})
        assert engine.events()[-1] == "answer.final"
        env = engine.submitted[-1]
        assert env["payload"]["text"] == "最终答案"
        assert env["payload"]["stats"]["model"] == "glm-4"
        assert res.action is NativeAction.CONSUMED

    async def test_notify_micro_turn_self_heal(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        res = await adapter.send(CHAT, "短答案", {"notify": True})
        assert engine.events() == ["turn.start", "answer.final"]
        assert engine.submitted[1]["turn_id"] == engine.submitted[0]["turn_id"]
        assert res.action is NativeAction.CONSUMED
        # 回合终结：注册表已关闭（后续孤儿编辑走原生）
        res2 = await adapter.edit(CHAT, "正文")
        assert res2.action is NativeAction.NATIVE

    async def test_deliver_no_marker(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        res = await adapter.send(CHAT, "cron 输出", {"job_id": "job9"})
        assert engine.events() == ["deliver"]
        env = engine.submitted[0]
        assert env["payload"]["text"] == "cron 输出"
        assert env["payload"]["footer"] == {"job_id": "job9"}
        assert "turn_id" not in env  # 回合外旁路
        assert res.action is NativeAction.CONSUMED

    async def test_deliver_failure_native(self):
        engine = FakeEngine(script=[Verdict.FALLBACK])
        adapter = HermesAdapter(engine)
        res = await adapter.send(CHAT, "cron 输出", None)
        assert res.action is NativeAction.NATIVE
        assert res.native_text == "cron 输出"


class TestProgressMapping:
    """_interim_send 进度行 → tool/thinking/interim/notice（经 legacy 分类器）."""

    async def test_progress_buffered_before_turn(self, adapter_and_engine):
        # 回合前暂存（前代 pending 语义）：进度行零投递，turn.start 后并入
        adapter, engine = adapter_and_engine
        r1 = await adapter.send(CHAT, REAL_THINK, {"_interim_send": True})
        assert engine.submitted == []  # 暂存不投递
        assert r1.detail == "buffered_pre_turn"
        r2 = await adapter.send(CHAT, "答案", {"expect_edits": True})
        assert engine.events() == ["turn.start", "thinking.delta", "answer.snapshot"]
        assert engine.submitted[1]["payload"]["text"] == "正在检查系统状态，先收集内核与运行时长…"
        assert engine.submitted[1]["turn_id"] == engine.submitted[0]["turn_id"]

    async def test_terminal_block_flushed_with_tool_name(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, REAL_TERMINAL_BLOCK, {"_interim_send": True})
        await adapter.send(CHAT, "答案", {"expect_edits": True})
        assert engine.events() == ["turn.start", "tool.start", "answer.snapshot"]
        tool = engine.submitted[1]["payload"]
        assert tool["tool"] == "terminal"
        assert tool["preview"] == 'echo "=== 时间 ==="; date "+%Y-%m-%d %H:%M:%S"'

    async def test_progress_after_turn_streams_directly(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        await adapter.send(CHAT, REAL_THINK, {"_interim_send": True})
        assert engine.events()[-1] == "thinking.delta"

    async def test_thinking_then_tool_closes_round(self, adapter_and_engine):
        # 思考行与工具行交错 → 思考轮显式闭合（耗时标签可回填）
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        await adapter.send(CHAT, f"{REAL_THINK}\n💻 terminal...", {"_interim_send": True})
        assert engine.events()[-3:] == ["thinking.delta", "thinking.round.end", "tool.start"]

    async def test_thinking_growth_continues_same_round(self, adapter_and_engine):
        # 跨帧 💬 增长（无工具间隔）→ 同轮原地改写，不闭合
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        await adapter.send(CHAT, "💬 起点", {"_interim_send": True})
        await adapter.send(CHAT, "💬 起点·增长", {"_interim_send": True})
        assert engine.events() == ["turn.start", "answer.snapshot",
                                   "thinking.delta", "thinking.delta"]
        # 定稿前显式闭合
        await adapter.edit(CHAT, "终稿", finalize=True)
        assert engine.events()[-2:] == ["thinking.round.end", "answer.final"]

    async def test_q9_dedup_same_text_skipped(self, adapter_and_engine):
        # 前代 (×N) 合并语义：同回合同文本进度行跳过
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        r1 = await adapter.send(CHAT, REAL_TERMINAL_BLOCK, {"_interim_send": True})
        n = len(engine.submitted)
        r2 = await adapter.send(CHAT, REAL_TERMINAL_BLOCK, {"_interim_send": True})
        r3 = await adapter.send(CHAT, REAL_TERMINAL_BLOCK + " (×2)", {"_interim_send": True})
        assert len(engine.submitted) == n  # 两次重发均去重
        assert r2.action is r3.action is NativeAction.CONSUMED

    async def test_pending_buffer_cap_64(self, adapter_and_engine):
        # 暂存上限 64：溢出丢最旧（answer 帧永不进缓冲也永不丢）
        adapter, engine = adapter_and_engine
        dropped = 0
        for i in range(70):
            r = await adapter.send(CHAT, f"💻 tool{i}...", {"_interim_send": True})
            dropped = r.dropped_progress
        assert dropped == 6
        await adapter.send(CHAT, "答案", {"expect_edits": True})
        tool_events = [e for e in engine.events() if e == "tool.start"]
        assert len(tool_events) == 64
        assert engine.submitted[1]["payload"]["tool"] == "tool6"  # 最旧 6 行被丢


class TestNoticeAndInterim:
    """平台通知与播报（跑马灯根因防线：无卡永不建卡）."""

    async def test_notice_with_active_turn(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        res = await adapter.send(CHAT, "⚠️ Hermes is shutting down — …",
                                 {"_interim_send": True})
        assert engine.events()[-1] == "notice"
        assert engine.submitted[-1]["payload"]["level"] == "warn"
        assert res.action is NativeAction.CONSUMED

    async def test_notice_without_turn_native_never_card(self, adapter_and_engine):
        # 前代 v2.0.8/34：无卡通知原生文本，永不建卡
        adapter, engine = adapter_and_engine
        res = await adapter.send(CHAT, "⚠️ Hermes is shutting down — …",
                                 {"_interim_send": True})
        assert engine.submitted == []
        assert res.action is NativeAction.NATIVE
        assert res.native_text == "⚠️ Hermes is shutting down — …"

    async def test_interim_commentary_semantics(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        res = await adapter.send(CHAT, "我先看一下状态", {"_interim_send": True})
        assert engine.events()[-1] == "interim"
        assert res.action is NativeAction.CONSUMED
        # 无卡 chat：对话文本原生投递（不建卡不缓存）
        res2 = await adapter.send("oc_other_chat", "我先看一下状态", {"_interim_send": True})
        assert res2.action is NativeAction.NATIVE


class TestDegradationAggregation:
    """§1.4 降级履行：任一内容帧未消费 → native + native_text."""

    async def test_first_frame_fallback_native_full_text(self):
        engine = FakeEngine(script=[Verdict.FALLBACK, Verdict.CONSUMED])
        adapter = HermesAdapter(engine)
        res = await adapter.send(CHAT, "首帧答案", {"expect_edits": True})
        assert res.action is NativeAction.NATIVE
        assert res.native_text == "首帧答案"
        assert res.detail == "fake_fallback"
        assert [f.verdict for f in res.frames] == ["fallback", "consumed"]

    async def test_mid_stream_degraded_marks_native(self):
        # 冷却期内快速 degraded 回执 → 该帧文本原生投递
        engine = FakeEngine(script=[Verdict.CONSUMED, Verdict.CONSUMED, Verdict.DEGRADED])
        adapter = HermesAdapter(engine)
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        res = await adapter.edit(CHAT, "更新正文")
        assert res.action is NativeAction.NATIVE
        assert res.native_text == "更新正文"
        assert res.turn_streamed is True  # 卡上已有内容（宿主策略线索）

    async def test_consumed_never_native(self, adapter_and_engine):
        adapter, _ = adapter_and_engine
        res = await adapter.send(CHAT, "首帧", {"expect_edits": True})
        assert res.action is NativeAction.CONSUMED
        assert res.native_text == ""


class TestClarifyAndAbort:
    async def test_send_clarify_with_turn(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        res = await adapter.send_clarify(
            CHAT, kind="select", question="选一个", options=("A", "B"))
        assert engine.events()[-1] == "clarify.request"
        form = engine.submitted[-1]["payload"]["form"]
        assert form == {"kind": "select", "question": "选一个",
                        "options": ["A", "B"], "placeholder": ""}
        assert res.action is NativeAction.CONSUMED

    async def test_send_clarify_without_turn_native(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        res = await adapter.send_clarify(CHAT, question="选一个")
        assert engine.submitted == []
        assert res.action is NativeAction.NATIVE
        assert res.detail == "clarify_without_stream"

    async def test_clarify_answer_control_frame(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        res = await adapter.clarify_answer(CHAT, values={"q": "A"})
        assert engine.events() == ["clarify.answer"]
        assert res.action is NativeAction.CONSUMED  # 控制帧无原生履行义务

    async def test_abort_with_and_without_turn(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        res = await adapter.abort(CHAT, reason="user_cancel")
        assert engine.submitted == []
        assert res.detail == "abort_no_active_turn"
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        res2 = await adapter.abort(CHAT, reason="user_cancel")
        assert engine.events()[-1] == "turn.abort"
        assert engine.submitted[-1]["payload"]["reason"] == "user_cancel"
        assert res2.action is NativeAction.CONSUMED
        # 中止后注册表已关闭：后续编辑走原生
        res3 = await adapter.edit(CHAT, "正文")
        assert res3.action is NativeAction.NATIVE


class TestDualRunFindings:
    """Phase 6 双跑对照发现项修复的适配器级验收（docs/dual-run-report.md）.

    F1 澄清窗口可达性 / F2 无标记进度升格 / F3 cron 家族溯源。
    时序用可注入假钟控制宽限窗口（与引擎 clarify_window 对齐 1.2s）。
    """

    @staticmethod
    def _clock(t: float = 1000.0):
        holder = {"t": t}
        return holder, (lambda: holder["t"])

    async def test_f1_clarify_grace_window_hit(self):
        """边界 finalize → 窗口内 send_clarify → 同 turn PAUSED → 续流同卡."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        r1 = await adapter.send(CHAT, "首帧", {"expect_edits": True})
        turn_id = r1.turn_id
        await adapter.edit(CHAT, "边界终值", finalize=True)  # 注册表关闭
        res = await adapter.send_clarify(CHAT, question="选哪个？",
                                         options=["A", "B"])
        assert res.action is NativeAction.CONSUMED
        assert res.turn_id == turn_id  # 原回合身份（宽限册）
        req = engine.submitted[-1]
        assert req["event"] == "clarify.request"
        assert req["turn_id"] == turn_id
        # 窗口命中后注册表重开：续流帧归同卡（seq 连续）
        res2 = await adapter.edit(CHAT, "澄清后续流")
        assert res2.action is NativeAction.CONSUMED
        snap = engine.submitted[-1]
        assert snap["event"] == "answer.snapshot"
        assert snap["turn_id"] == turn_id
        # 回答回流（注册表活跃 → 常规路径）→ 真终值
        res3 = await adapter.clarify_answer(CHAT, values={"select": "A"})
        assert res3.action is NativeAction.CONSUMED
        await adapter.edit(CHAT, "真终值", finalize=True)
        assert engine.submitted[-1]["event"] == "answer.final"
        assert engine.submitted[-1]["turn_id"] == turn_id

    async def test_f1_clarify_grace_window_expired_native(self):
        """窗口已过（宽限册过期）→ 不触达引擎 → 原生 ask 履行."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        await adapter.edit(CHAT, "终值", finalize=True)
        holder["t"] += 5.0  # 窗口已过
        res = await adapter.send_clarify(CHAT, question="还在吗？")
        assert res.action is NativeAction.NATIVE
        assert res.native_text == "还在吗？"  # 原生 ask：问题文本必达
        assert res.detail == "clarify_without_stream"
        assert engine.events()[-1] == "answer.final"  # 引擎未收到 clarify 帧

    async def test_f1_clarify_engine_fallback_native(self):
        """宽限命中但引擎裁决 fallback（SEALED 竞态）→ 原生 ask 履行."""
        holder, clock = self._clock()
        engine = FakeEngine(script=lambda env: Verdict.FALLBACK)
        adapter = HermesAdapter(engine, clock=clock)
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        await adapter.edit(CHAT, "终值", finalize=True)
        res = await adapter.send_clarify(CHAT, question="还在吗？")
        assert res.action is NativeAction.NATIVE
        assert res.native_text == "还在吗？"
        assert res.detail == "fake_fallback"

    async def test_f1_grace_invalidated_by_new_turn(self):
        """新回合开启后宽限引用失效：clarify 归属新活跃回合."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        await adapter.send(CHAT, "旧回合", {"expect_edits": True})
        await adapter.edit(CHAT, "旧终值", finalize=True)
        r2 = await adapter.send(CHAT, "新回合", {"expect_edits": True})
        res = await adapter.send_clarify(CHAT, question="澄清")
        assert res.action is NativeAction.CONSUMED
        assert engine.submitted[-1]["turn_id"] == r2.turn_id  # 新回合，非宽限

    async def test_f1_clarify_answer_grace_turn_id(self):
        """注册表已关时 clarify.answer 携带宽限册内原 turn_id（幂等裁决）."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        r1 = await adapter.send(CHAT, "首帧", {"expect_edits": True})
        await adapter.edit(CHAT, "终值", finalize=True)
        res = await adapter.clarify_answer(CHAT, values={"select": "B"})
        assert res.action is NativeAction.CONSUMED
        ans = engine.submitted[-1]
        assert ans["event"] == "clarify.answer"
        assert ans["turn_id"] == r1.turn_id

    async def test_f2_unmarked_progress_buffers_pre_turn(self):
        """无标记进度行（回合前）升格 → 暂存 → turn.start 后并入面板."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        res = await adapter.send(CHAT, "💻 terminal：df -h")  # 无标记
        assert res.action is NativeAction.CONSUMED
        assert engine.submitted == []  # 回合前暂存（非 deliver 静态卡）
        r2 = await adapter.send(CHAT, "答案首帧", {"expect_edits": True})
        events = engine.events()
        assert events[0] == "turn.start"
        assert events[1] == "tool.start"  # 暂存进度并入
        assert events[-1] == "answer.snapshot"
        assert engine.submitted[1]["turn_id"] == r2.turn_id

    async def test_f2_unmarked_progress_with_active_turn(self):
        """活跃回合期无标记进度行 → 面板帧（不投散卡）."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        await adapter.send(CHAT, "首帧", {"expect_edits": True})
        n = len(engine.submitted)
        res = await adapter.send(CHAT, "🔍 搜索网页 · feishu cardkit")
        assert res.action is NativeAction.CONSUMED
        assert engine.events()[n:] == ["tool.start"]

    async def test_f2_plain_longform_still_deliver(self):
        """普通独立正文（非进度启发）维持 deliver 静态卡（冻结契约不变）."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        res = await adapter.send(CHAT, "今日站会纪要：三项决议，全部通过。")
        assert res.action is NativeAction.CONSUMED
        assert engine.events() == ["deliver"]

    async def test_f3_cron_wrapped_routes_to_deliver_with_footer(self):
        """job_id + 官方包装头 → deliver 静态卡：标题/正文剥离 + 溯源页脚."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        content = ("Cronjob Response: 每日索引\n(job_id: 967430ee4ab2)\n"
                   "--------------------------------\n"
                   "索引完成：12847 条文档。")
        res = await adapter.send(CHAT, content, {"notify": True, "job_id": "967430ee4ab2"})
        assert res.action is NativeAction.CONSUMED
        env = engine.submitted[-1]
        assert env["event"] == "deliver"
        assert env["payload"]["title"] == "每日索引"
        assert env["payload"]["text"] == "索引完成：12847 条文档。"
        assert env["payload"]["footer"] == {"job_id": "967430ee4ab2"}

    async def test_f3_cron_unwrapped_keeps_text(self):
        """job_id 但非包装内容 → deliver 原文透传 + 溯源页脚."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        res = await adapter.send(CHAT, "队列已排水，无待处理任务。",
                                 {"notify": True, "job_id": "j42"})
        env = engine.submitted[-1]
        assert env["event"] == "deliver"
        assert env["payload"]["text"] == "队列已排水，无待处理任务。"
        assert env["payload"]["footer"] == {"job_id": "j42"}

    async def test_f3_cron_stream_frame_not_deliver(self):
        """expect_edits + job_id → 流式回合路径（旧 oneshot 守卫同款）."""
        holder, clock = self._clock()
        engine = FakeEngine()
        adapter = HermesAdapter(engine, clock=clock)
        res = await adapter.send(CHAT, "首帧", {"expect_edits": True, "job_id": "j1"})
        assert engine.events() == ["turn.start", "answer.snapshot"]


class TestEnvelopeShape:
    """信封字段表逐项（01-protocol §2 消费面校验前置）。"""
    async def test_envelope_fields(self, adapter_and_engine):
        adapter, engine = adapter_and_engine
        await adapter.send(CHAT, "首帧", {"expect_edits": True}, reply_to="om_9")
        for env in engine.submitted:
            assert env["v"] == 1
            assert isinstance(env["frame_id"], str) and env["frame_id"]
            assert env["chat_id"] == CHAT
            assert isinstance(env["payload"], dict)
        ids = [env["frame_id"] for env in engine.submitted]
        assert len(ids) == len(set(ids))  # frame_id 全局唯一
        snap = engine.submitted[1]
        assert snap["seq"] == 1  # 回合内单调

    async def test_unique_across_adapters(self):
        e1, e2 = FakeEngine(), FakeEngine()
        a1, a2 = HermesAdapter(e1), HermesAdapter(e2)
        await a1.send(CHAT, "x", {"expect_edits": True})
        await a2.send(CHAT, "x", {"expect_edits": True})
        ids1 = {env["frame_id"] for env in e1.submitted}
        ids2 = {env["frame_id"] for env in e2.submitted}
        assert not (ids1 & ids2)  # 实例后缀隔离
