"""openclaw 映射器单测 — bridge 消息 → LSEP 信封（05-adapters §5.2）."""

from __future__ import annotations

from lark_streaming.adapters import OpenclawMapper


def msg(content, **meta):
    return {"chat_id": "oc_oclaw", "content": content, "metadata": meta}


class TestStreamMapping:
    def test_first_stream_frame_opens_turn(self):
        m = OpenclawMapper()
        envs = m.translate(msg("增量", streaming=True, incremental=True))
        assert [e["event"] for e in envs] == ["turn.start", "answer.delta"]
        assert envs[1]["payload"] == {"delta": "增量"}
        assert envs[1]["turn_id"] == envs[0]["turn_id"]

    def test_full_snapshot_mode_default(self):
        # D5 双模：incremental 缺省 = 全量快照（Node 系网关全量语义）
        m = OpenclawMapper()
        envs = m.translate(msg("全量", streaming=True))
        assert envs[-1]["event"] == "answer.snapshot"
        assert envs[-1]["payload"] == {"text": "全量"}

    def test_subsequent_frames_continue_turn(self):
        m = OpenclawMapper()
        m.translate(msg("首帧", streaming=True))
        envs = m.translate(msg("续帧", streaming=True))
        assert [e["event"] for e in envs] == ["answer.snapshot"]
        # 换新 turn_id → 越权开新回合（引擎 seal_and_create 对齐）
        envs2 = m.translate({"chat_id": "oc_oclaw", "content": "新回合",
                             "turn_id": "host_t2", "metadata": {"streaming": True}})
        assert [e["event"] for e in envs2] == ["turn.start", "answer.snapshot"]
        assert envs2[0]["turn_id"] == "host_t2"


class TestFinalizeMapping:
    def test_finalize_open_turn(self):
        m = OpenclawMapper()
        m.translate(msg("首帧", streaming=True))
        turn = m._turns.active("oc_oclaw").turn_id
        envs = m.translate(msg("终稿", finalize=True, stats={"model": "m1"}))
        assert [e["event"] for e in envs] == ["answer.final"]
        assert envs[0]["turn_id"] == turn
        assert envs[0]["payload"]["stats"] == {"model": "m1"}
        assert m._turns.active("oc_oclaw") is None  # 回合终结

    def test_finalize_without_turn_micro_turn(self):
        m = OpenclawMapper()
        envs = m.translate(msg("短答案", finalize=True))
        assert [e["event"] for e in envs] == ["turn.start", "answer.final"]


class TestProgressTemplates:
    def test_tool_template(self):
        m = OpenclawMapper()
        envs = m.translate(msg("ls -la", tool="terminal", preview="ls -la"))
        assert envs == []  # 无回合 → 缓冲
        envs2 = m.translate(msg("首帧", streaming=True))
        assert [e["event"] for e in envs2] == ["turn.start", "tool.start", "answer.snapshot"]
        assert envs2[1]["payload"] == {"tool": "terminal", "preview": "ls -la"}

    def test_thinking_notice_templates(self):
        m = OpenclawMapper()
        m.translate(msg("首帧", streaming=True))
        assert m.translate(msg("推理中", thinking=True))[0]["event"] == "thinking.delta"
        warn = m.translate(msg("外部依赖抖动", notice="warn"))[0]
        assert warn["event"] == "notice" and warn["payload"]["level"] == "warn"
        assert m.translate(msg("中途播报", interim=True))[0]["event"] == "interim"

    def test_notice_level_default(self):
        m = OpenclawMapper()
        m.translate(msg("首帧", streaming=True))
        env = m.translate(msg("提示", notice="bogus"))[0]
        assert env["payload"]["level"] == "info"

    def test_interim_without_turn_dropped_never_card(self):
        # 跑马灯根因防线：无卡播报不建卡不缓冲
        m = OpenclawMapper()
        assert m.translate(msg("播报", interim=True)) == []
        envs = m.translate(msg("首帧", streaming=True))
        assert [e["event"] for e in envs] == ["turn.start", "answer.snapshot"]


class TestDeliverMapping:
    def test_no_marker_deliver(self):
        m = OpenclawMapper()
        envs = m.translate(msg("cron 输出", title="定时", job_id="j1"))
        assert len(envs) == 1
        env = envs[0]
        assert env["event"] == "deliver"
        assert "turn_id" not in env
        assert env["payload"] == {"text": "cron 输出", "title": "定时",
                                  "footer": {"job_id": "j1"}}

    def test_empty_content_skipped(self):
        m = OpenclawMapper()
        assert m.translate(msg("", )) == []

    def test_bad_messages_never_raise(self):
        m = OpenclawMapper()
        assert m.translate({}) == []
        assert m.translate({"chat_id": "", "content": "x"}) == []
        # 非字符串 content 宽容转字符串（永不抛异常）
        envs = m.translate({"chat_id": "c", "content": 123})
        assert envs[0]["event"] == "deliver"
        assert envs[0]["payload"]["text"] == "123"
        assert m.translate("not-a-dict") == []  # type: ignore[arg-type]


class TestMvpAcceptance:
    """参考 MVP 验收（05-adapters §5.2）：一轮流式卡 + 一次 deliver + 一次降级回退."""

    async def test_full_mvp_envelope_sequences(self, transport):
        # 信封序列经真实引擎消费全 consumed（L2 mock 端到端）
        from tests.engine.conftest import make_engine

        m = OpenclawMapper()
        scripted: list[dict] = []
        # ① 一轮流式卡
        scripted += m.translate(msg("任务开始", tool="terminal", preview="ls"))
        scripted += m.translate(msg("部分答案", streaming=True))
        scripted += m.translate(msg("完整答案", finalize=True))
        # ② 一次 deliver 静态卡
        scripted += m.translate(msg("后台产物", title="报告"))
        # ③ 降级回退：降级期 streaming 帧（bridge 侧按 verdict 原生投递——
        #    映射器语义不变，回执裁决由引擎给出）
        scripted += m.translate(msg("降级期答案", streaming=True))

        engine, events, _clock = make_engine(transport)
        verdicts = []
        for env in scripted:
            ack = await engine.submit(env)
            verdicts.append(ack.verdict.value)
        # ①②⑤ 全 consumed（引擎健康）；降级路径由 transport 故障注入另测
        assert verdicts == ["consumed"] * len(scripted)
        await engine.aclose()

    async def test_degraded_turn_fallback_verdicts(self, transport):
        # 建卡失败 → 引擎降级：后续帧快速 degraded（bridge 据此原生投递）
        from tests.engine.conftest import make_engine

        transport.fail_always("cardkit_create", RuntimeError("code=500"))
        m = OpenclawMapper()
        engine, _events, _clock = make_engine(transport)
        envs = m.translate(msg("首帧", streaming=True))
        ack1 = await engine.submit(envs[0])   # turn.start → 建卡失败 → fallback + 降级
        assert ack1.verdict.value == "fallback"
        assert ack1.detail.startswith("create_failed")
        ack2 = await engine.submit(envs[1])   # snapshot → 冷却期内快速 degraded
        assert ack2.verdict.value == "degraded"
        ack3 = await engine.submit(
            {"v": 1, "event": "answer.snapshot", "frame_id": "fx", "chat_id": "oc_oclaw",
             "turn_id": envs[0]["turn_id"], "payload": {"text": "续帧"}})
        # 冷却期内快速 degraded（零 API，02-engine §1.2）
        assert ack3.verdict.value == "degraded"
        await engine.aclose()
