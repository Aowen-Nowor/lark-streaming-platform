"""锚点换道守卫回归（audit #3）— 恰一投递纵深防御的引擎侧证据.

红线语义：``reply_card_by_id`` 失败时，仅锚点**确定性失效**（1000023/
99992354/230011/231003）才允许换道直发；传输结果未知（网络挂起/瞬态/
限频）时直发可能与未竟的 reply 竞速产生重复卡片——必须走降级红线：
FALLBACK 回执，接入方原生投递。

修复前行为（两处裸 except）：任何锚点失败都无条件直发 → 双卡风险。
"""

from __future__ import annotations

from lark_streaming.engine import TurnPhase, Verdict
from lark_streaming.transport.errors import (
    MSG_NOT_FOUND,
    REPLY_ANCHOR_INVALID,
    FeishuAPIError,
)
from tests.engine.conftest import CHAT, env, make_engine, settle


async def test_anchor_unknown_failure_no_direct_send(transport):
    """传输结果未知（网络挂起）→ 不换道直发，FALLBACK 回执（修复前会双发）."""
    transport.fail_always("reply_card_by_id", TimeoutError("net hang"))
    engine, events, clock = make_engine(transport)
    try:
        ack = await engine.submit(env(
            "turn.start", turn="ta1",
            payload={"reply_to": "om_anchor1", "title": "T"},
        ))
        await settle()
        assert ack.verdict is Verdict.FALLBACK
        assert "send_failed" in (ack.detail or "")
        # 守卫核心证据：直发通道零调用（修复前此处为 1）
        assert len(transport.calls_of("send_card_by_id_to_chat")) == 0
        assert len(transport.calls_of("reply_card_by_id")) == 1
    finally:
        await engine.aclose()


async def test_anchor_definitive_failure_direct_send(transport):
    """锚点确定性失效（99992354 非 om_ id）→ 换道直发，CONSUMED."""
    transport.fail_next(
        "reply_card_by_id",
        FeishuAPIError("reply target invalid", REPLY_ANCHOR_INVALID),
    )
    engine, events, clock = make_engine(transport)
    try:
        ack = await engine.submit(env(
            "turn.start", turn="ta2",
            payload={"reply_to": "om_anchor2", "title": "T"},
        ))
        assert ack.verdict is Verdict.CONSUMED
        assert ack.message_id
        # 恰一投递：reply 1 次（失败）+ 直发 1 次（成功）
        assert len(transport.calls_of("reply_card_by_id")) == 1
        assert len(transport.calls_of("send_card_by_id_to_chat")) == 1
    finally:
        await engine.aclose()


async def test_anchor_transient_code_no_direct_send(transport):
    """瞬态错误码（99991400 限频）同样不换道——结果未知 ≠ 确定性失效."""
    transport.fail_always(
        "reply_card_by_id", FeishuAPIError("rate limited", 99991400)
    )
    engine, events, clock = make_engine(transport)
    try:
        ack = await engine.submit(env(
            "turn.start", turn="ta3",
            payload={"reply_to": "om_anchor3", "title": "T"},
        ))
        await settle()
        assert ack.verdict is Verdict.FALLBACK
        assert len(transport.calls_of("send_card_by_id_to_chat")) == 0
    finally:
        await engine.aclose()


async def test_clarify_anchor_unknown_failure_rollback(transport):
    """澄清卡锚点非确定性失败 → 回滚暂停 + FALLBACK，不直发（死等防线）."""
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="tb1", payload={"reply_to": "om_b1", "title": "T"}))
        await engine.submit(env("answer.final", turn="tb1", payload={"text": "答案"}, seq=1))
        st = engine._chats[CHAT].turn
        assert st is not None and st.pause_pending is True
        # 回合卡锚定已成功；注入澄清卡锚定非确定性失败
        transport.fail_next("reply_card_by_id", TimeoutError("net hang"))
        ack = await engine.submit(env(
            "clarify.request", turn="tb1",
            payload={"form": {"kind": "select", "question": "选", "options": ["A", "B"]}},
            seq=2,
        ))
        assert ack.verdict is Verdict.FALLBACK
        assert "clarify_card_failed" in (ack.detail or "")
        # 回滚：暂停解除、无澄清残留
        assert st.phase is TurnPhase.STREAMING
        assert st.clarify_id == ""
        assert st.clarify_form is None
        # 澄清卡未直发（守卫核心证据）：send_card_by_id_to_chat 零调用
        # （回合卡走 reply 成功路径，不产生直发）
        assert len(transport.calls_of("send_card_by_id_to_chat")) == 0
    finally:
        await engine.aclose()


async def test_clarify_anchor_definitive_failure_direct_send(transport):
    """澄清卡锚点确定性失效 → 直发澄清卡，CONSUMED，进入 PAUSED."""
    engine, events, clock = make_engine(transport)
    try:
        await engine.submit(env("turn.start", turn="tb2", payload={"reply_to": "om_b2", "title": "T"}))
        await engine.submit(env("answer.final", turn="tb2", payload={"text": "答案"}, seq=1))
        st = engine._chats[CHAT].turn
        transport.fail_next(
            "reply_card_by_id",
            FeishuAPIError("msg not found", MSG_NOT_FOUND),
        )
        ack = await engine.submit(env(
            "clarify.request", turn="tb2",
            payload={"form": {"kind": "select", "question": "选", "options": ["A", "B"]}},
            seq=2,
        ))
        assert ack.verdict is Verdict.CONSUMED
        assert st.phase is TurnPhase.PAUSED
        assert st.clarify_message_id
        # 澄清卡直发恰好一次
        assert len(transport.calls_of("send_card_by_id_to_chat")) == 1
    finally:
        await engine.aclose()
