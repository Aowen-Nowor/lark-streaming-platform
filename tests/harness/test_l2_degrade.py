"""L2 降级验收（04-knowledge-freeze §4 降级基线 / 02-engine §1.2 正交 chat 级态）.

红线（02-engine §7）：任何帧必得恰好一个 ack；卡片 API 失败 →
fallback/degraded 回执（接入方原生投递）——**不存在静默失败的帧**。
全部场景跑真实 FeishuTransport × mock HTTP（429 wire 形态：HTTP 400 +
code 承载，99991400 走 retry_transient 分道退避）。
"""

from __future__ import annotations

from lark_streaming.engine import ChatHealth, Verdict

from tests.harness.framework import CHAT, FakeClock


async def test_first_card_failure_degrades_chat(harness):
    """首次卡片失败 → 该帧 fallback + chat 降级；冷却期内帧零 API."""
    h = harness
    h.server.fail_always("create", 200860, msg="card size over budget")
    turn = "t_dg1"
    ack = await h.submit("turn.start", turn=turn)
    assert ack.verdict is Verdict.FALLBACK
    assert ack.detail == "create_failed:200860"
    assert any(e.type == "chat_degraded" and e.code == 200860 for e in h.lifecycle)
    box = h.engine._chats[CHAT]
    assert box.health is ChatHealth.DEGRADED
    assert box.degrade_count == 1
    # 回合已终结（无卡可收）：但冷却期拦截在守卫裁决之前——
    # engine._submit_event 对 DEGRADED 冷却期内一切帧快速回执 degraded（实际语义）
    ack_abort = await h.submit("turn.abort", turn=turn)
    assert ack_abort.verdict is Verdict.DEGRADED
    assert ack_abort.detail == "chat_degraded"
    # 冷却期内帧：DEGRADED 快速回执 + 零 API（02-engine §1.2）
    n_calls = len(h.server.call_log)
    ack_dg = await h.submit("answer.delta", turn=turn, payload={"delta": "x"})
    assert ack_dg.verdict is Verdict.DEGRADED
    assert ack_dg.detail == "chat_degraded"
    assert len(h.server.call_log) == n_calls  # mock 零新增调用


async def test_rate_limit_cooldown_then_probe_recovers(make_harness, monkeypatch):
    """429 路径：降级（rate-limit 档）→ 冷却期满 → 静默探测 → chat_recovered."""
    # 99991400 属 CardKit 瞬态码，真实 transport 会走分道退避链；测试内把
    # 限频退避压为 0 以保持用例秒级（退避档位语义由 transport L1 覆盖）。
    from lark_streaming.transport import retry as retry_mod

    monkeypatch.setattr(retry_mod, "RATE_LIMIT_RETRY_DELAYS", (0.0, 0.0, 0.0))
    clock = FakeClock(1000.0)
    h = await make_harness(degrade_cooldown_rate_limit_sec=6, clock=clock)
    # 99991400 限频约定 HTTP 400 + code 承载；4 次 = 首试 + 3 次重试全部撞墙
    h.server.fail_next("create", 99991400, msg="Too Many Requests",
                       times=4, http_status=400)
    ack = await h.submit("turn.start", turn="t_429")
    assert ack.verdict is Verdict.FALLBACK
    assert ack.detail == "create_failed:99991400"
    failed = [c for c in h.server.calls_of("create") if c.code == 99991400]
    assert len(failed) == 4  # 退避链全部经真实 transport 在环
    box = h.engine._chats[CHAT]
    assert box.health is ChatHealth.DEGRADED
    # rate-limit 档冷却（settings 钳制下限 5s；此处 6s）
    assert abs(box.degraded_until - clock() - 6.0) < 1e-6
    h.server.clear_failures()
    clock.advance(7.0)
    # 新 turn.start 即静默探测：成功 → CONSUMED + chat_recovered
    ack2 = await h.submit("turn.start", turn="t_429b")
    assert ack2.verdict is Verdict.CONSUMED
    assert ack2.message_id
    assert any(e.type == "chat_recovered" for e in h.lifecycle)
    assert box.health is ChatHealth.OK


async def test_sustained_failure_every_frame_receipt(make_harness):
    """持续失败：每轮 turn.start 都 fallback——ack 全量返回，绝不静默吞."""
    clock = FakeClock(1000.0)
    h = await make_harness(degrade_cooldown_other_sec=5, clock=clock)
    h.server.fail_always("create", 200860, msg="over budget")
    acks = []
    for i in range(3):
        acks.append(await h.submit("turn.start", turn=f"t_sus{i}", frame=f"f_sus{i}"))
        clock.advance(6.0)  # 越过冷却（钳制下限 5s）→ 下一轮再探
    # 消息零丢失红线：每帧必得 fallback 回执，且 frame_id 全量携带
    assert all(a.verdict is Verdict.FALLBACK for a in acks)
    assert all(a.detail == "create_failed:200860" for a in acks)
    assert all(a.frame_id for a in acks)
    assert h.server.count("create") == 3  # 每轮真实探测一次（200860 确定性拒收零重试）
    box = h.engine._chats[CHAT]
    assert box.degrade_count == 3
    assert len([e for e in h.lifecycle if e.type == "chat_degraded"]) == 3


async def test_stream_failure_degrades_30s_band(harness):
    """非建卡失败降级：STREAMING 中 stream_element 失败 → 30s 档（非 429）."""
    h = harness
    turn = "t_sdf"
    await h.submit("turn.start", turn=turn)
    ok = await h.submit("answer.delta", turn=turn, payload={"delta": "ok"}, seq=1)
    assert ok.verdict is Verdict.CONSUMED
    # 300315 + "not find elementID" 属元素未持久化竞态码族：真实 transport
    # 走 0.2s×3 固定重试（前代语义），times=4 = 首试 + 3 重试全部失败
    h.server.fail_next(
        "stream_element", 300315, msg="not find elementID: answer_content", times=4,
    )
    ack = await h.submit("answer.delta", turn=turn, payload={"delta": "撞故障"}, seq=2)
    assert ack.verdict is Verdict.FALLBACK
    assert ack.detail == "stream_failed:300315"
    box = h.engine._chats[CHAT]
    assert box.health is ChatHealth.DEGRADED
    assert abs(box.degraded_until - h.clock() - 30.0) < 1e-6  # 其他错误 30s 档
    assert any(e.type == "chat_degraded" and e.code == 300315 for e in h.lifecycle)
    # 冷却期内零 API
    n_calls = len(h.server.call_log)
    ack2 = await h.submit("answer.delta", turn=turn, payload={"delta": "y"}, seq=3)
    assert ack2.verdict is Verdict.DEGRADED
    assert len(h.server.call_log) == n_calls
