"""双形态接入端到端集成 — 适配器信封经真实引擎消费（02-engine §10 L2）.

hermes / openclaw 信封序列 → 真实 StreamingEngine + FakeTransport：
验证映射产物被引擎全量接受、回合生命周期在传输面正确落地。
"""

from __future__ import annotations

from lark_streaming.adapters import HermesAdapter, NativeAction, OpenclawMapper


async def test_hermes_full_turn_lifecycle(transport):
    """完整回合：进度暂存 → 流式 → 定稿 → 传输面 seal 落地."""
    from tests.engine.conftest import make_engine, make_settings

    engine, _lifecycle, _clock = make_engine(transport, settings=make_settings(clarify_window_sec=0.0))
    adapter = HermesAdapter(engine)

    # agent 干活：进度行先于流式帧（暂存）
    r1 = await adapter.send("oc_int", "💬 推理中…", {"_interim_send": True})
    assert r1.detail == "buffered_pre_turn"
    r2 = await adapter.send("oc_int", "💻 terminal...", {"_interim_send": True})
    assert r2.detail == "buffered_pre_turn"
    # 流式首帧：暂存并入 + 建卡
    r3 = await adapter.send("oc_int", "答案第一段", {"expect_edits": True})
    assert r3.action is NativeAction.CONSUMED
    assert transport.calls_of("cardkit_create")
    # 续流 + 工具 + 定稿
    await adapter.send("oc_int", "答案第二段", {"expect_edits": True})
    await adapter.send("oc_int", "🔍 搜索网页 · 资料", {"_interim_send": True})
    fin = await adapter.edit("oc_int", "最终答案", finalize=True,
                             stats={"model": "glm-4.6", "duration_ms": 1200})
    assert fin.action is NativeAction.CONSUMED
    # 传输面：answer 通道 + 面板更新 + 关流式收口
    assert transport.calls_of("cardkit_stream_element")
    assert transport.calls_of("cardkit_update")
    assert transport.calls_of("cardkit_close_streaming")
    await engine.aclose()


async def test_hermes_micro_turn_and_deliver(transport):
    from tests.engine.conftest import make_engine, make_settings

    engine, _lifecycle, _clock = make_engine(transport, settings=make_settings(clarify_window_sec=0.0))
    adapter = HermesAdapter(engine)
    # notify 短答案 → 微回合（建卡即定稿）
    res = await adapter.send("oc_int", "短答案", {"notify": True})
    assert res.action is NativeAction.CONSUMED
    assert transport.calls_of("cardkit_create") and transport.calls_of("cardkit_close_streaming")
    # 无标记 → deliver 静态卡
    res2 = await adapter.send("oc_int", "cron 输出", {"job_id": "j1"})
    assert res2.action is NativeAction.CONSUMED
    assert transport.calls_of("send_card_to_chat")
    await engine.aclose()


async def test_hermes_clarify_pause_resume(transport):
    """澄清暂停/续流：clarify.request → PAUSED → answer 回流 → 续流."""
    from tests.engine.conftest import make_engine, make_settings, settle

    engine, _lifecycle, _clock = make_engine(transport, settings=make_settings(clarify_window_sec=0.0))
    adapter = HermesAdapter(engine)
    await adapter.send("oc_int", "首帧", {"expect_edits": True})
    res = await adapter.send_clarify("oc_int", question="选一个", options=("A", "B"))
    assert res.action is NativeAction.CONSUMED
    await settle()
    health = engine.snapshot_health(verbose=True)["chats"]["oc_int"]
    assert health["turn_phase"] == "paused"
    # 交互回调回流 → 续流
    res2 = await adapter.clarify_answer("oc_int", values={"form": "A"})
    assert res2.action is NativeAction.CONSUMED
    await settle()
    assert engine.snapshot_health(verbose=True)["chats"]["oc_int"]["turn_phase"] == "streaming"
    # 定稿收口
    fin = await adapter.edit("oc_int", "最终答案", finalize=True)
    assert fin.action is NativeAction.CONSUMED
    assert transport.calls_of("cardkit_close_streaming")
    await engine.aclose()


async def test_hermes_degrade_native_aggregation(transport):
    """建卡失败 → 全帧 native 聚合（hermes 原生投递兜底，永不丢帧）."""
    from tests.engine.conftest import make_engine, make_settings

    transport.fail_always("cardkit_create", RuntimeError("code=500"))
    engine, _lifecycle, _clock = make_engine(transport, settings=make_settings(clarify_window_sec=0.0))
    adapter = HermesAdapter(engine)
    res = await adapter.send("oc_int", "首帧", {"expect_edits": True})
    assert res.action is NativeAction.NATIVE
    assert res.native_text == "首帧"
    # 冷却期内后续帧 degraded → 同样 native
    res2 = await adapter.send("oc_int", "续帧", {"expect_edits": True})
    assert res2.action is NativeAction.NATIVE
    # deliver 同 chat 也降级 → native
    res3 = await adapter.send("oc_int", "cron 输出", None)
    assert res3.action is NativeAction.NATIVE
    assert res3.native_text == "cron 输出"
    await engine.aclose()


async def test_openclaw_bridge_full_flow(transport):
    """openclaw MVP 主链：流式回合 + deliver + 生命周期落卡."""
    from tests.engine.conftest import make_engine, make_settings

    engine, _lifecycle, _clock = make_engine(transport, settings=make_settings(clarify_window_sec=0.0))
    m = OpenclawMapper()
    # 进度缓冲 → 流式回合 → 定稿
    assert m.translate({"chat_id": "oc_oc", "content": "ls",
                        "metadata": {"tool": "terminal", "preview": "ls"}}) == []
    seq: list[dict] = []
    seq += m.translate({"chat_id": "oc_oc", "content": "增量A",
                        "metadata": {"streaming": True, "incremental": True}})
    seq += m.translate({"chat_id": "oc_oc", "content": "B",
                        "metadata": {"streaming": True, "incremental": True}})
    seq += m.translate({"chat_id": "oc_oc", "content": "AB终稿",
                        "metadata": {"finalize": True}})
    seq += m.translate({"chat_id": "oc_oc", "content": "后台产物",
                        "metadata": {"title": "报告"}})
    for env in seq:
        ack = await engine.submit(env)
        assert ack.verdict.value == "consumed", (env["event"], ack.detail)
    assert transport.calls_of("cardkit_stream_element")
    assert transport.calls_of("cardkit_close_streaming")
    assert transport.calls_of("send_card_to_chat")
    await engine.aclose()
