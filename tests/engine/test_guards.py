"""钉制范围钳制（02-engine §2）与守卫裁决表（02-engine §1.1/§6）."""

from __future__ import annotations

from lark_streaming.engine import (
    Action,
    ChatBox,
    Decision,
    EngineSettings,
    EV,
    Event,
    TurnPhase,
    TurnState,
    decide,
)
from lark_streaming.engine.state import ChatHealth
from tests.engine.conftest import CHAT, TURN, env


# ── EngineSettings 钳制 ──────────────────────────────────────────────────


def test_settings_defaults_match_frozen_constants():
    s = EngineSettings()
    assert s.watchdog_idle_sec == 300.0
    assert s.session_renew_age_sec == 540.0
    assert s.panel_flush_sec == 0.2
    assert s.clarify_window_sec == 1.2
    assert s.card_ttl_sec == 600.0
    assert s.gc_cooldown_sec == 60.0
    assert s.pending_max == 64
    assert s.degrade_cooldown_rate_limit_sec == 60.0
    assert s.degrade_cooldown_other_sec == 30.0


def test_settings_clamped_into_pinned_ranges():
    s = EngineSettings(
        watchdog_idle_sec=1,        # < 60 → 60
        session_renew_age_sec=1000, # > 590 → 590
        panel_flush_sec=99,         # > 5 → 5
        clarify_window_sec=-5,      # < 0 → 0
        card_ttl_sec=1,             # < 60 → 60
        gc_cooldown_sec=1,          # < 10 → 10
        pending_max=1,              # < 8 → 8
        gc_sweep_sec=0.1,           # < 5 → 5
        frame_cache_max=1,          # < 16 → 16
    )
    assert s.watchdog_idle_sec == 60.0
    assert s.session_renew_age_sec == 590.0
    assert s.panel_flush_sec == 5.0
    assert s.clarify_window_sec == 0.0
    assert s.card_ttl_sec == 60.0
    assert s.gc_cooldown_sec == 10.0
    assert s.pending_max == 8
    assert s.gc_sweep_sec == 5.0
    assert s.frame_cache_max == 16


# ── 裁决表 ───────────────────────────────────────────────────────────────


def _box(phase: TurnPhase | None, *, turn_id: str = TURN) -> ChatBox:
    box = ChatBox(chat_id=CHAT)
    if phase is not None:
        st = TurnState(chat_id=CHAT, turn_id=turn_id)
        st.phase = phase
        box.turn = st
    return box


def _ev(name: str, *, turn: str = TURN, payload: dict | None = None) -> Event:
    return Event.from_envelope(env(name, turn=turn, payload=payload or {}))


def _decide(box: ChatBox, ev: Event, **kw) -> Decision:
    return decide(box, ev, EngineSettings(**kw))


def test_deliver_bypasses_any_phase():
    for phase in (None, TurnPhase.CREATING, TurnPhase.STREAMING,
                  TurnPhase.PAUSED, TurnPhase.SEALING):
        d = _decide(_box(phase), _ev(EV.DELIVER))
        assert d.action is Action.DELIVER
        assert d.owner == "deliver_bypass"


def test_turn_start_routing():
    assert _decide(_box(None), _ev(EV.TURN_START)).action is Action.CREATE_TURN
    # 同 turn_id 幂等
    for phase in (TurnPhase.CREATING, TurnPhase.STREAMING, TurnPhase.PAUSED, TurnPhase.SEALING):
        d = _decide(_box(phase), _ev(EV.TURN_START))
        assert d.action is Action.IGNORE_CONSUMED
        assert d.owner == "turn_start_idempotent"
    # 异 turn_id 打断
    d = _decide(_box(TurnPhase.STREAMING), _ev(EV.TURN_START, turn="t_new"))
    assert d.action is Action.SEAL_AND_CREATE
    assert d.owner == "turn_start_supersede"


def test_turn_abort_routing():
    d = _decide(_box(TurnPhase.STREAMING), _ev(EV.TURN_ABORT))
    assert d.action is Action.SEAL_ABORT
    d = _decide(_box(None), _ev(EV.TURN_ABORT))
    assert d.action is Action.FALLBACK
    assert d.detail == "no_active_card"


def test_answer_routing_matrix():
    # 无回合 / SEALED → fallback
    d = _decide(_box(None), _ev(EV.ANSWER_DELTA))
    assert d.action is Action.FALLBACK and d.detail == "no_active_card"
    d = _decide(_box(TurnPhase.SEALED), _ev(EV.ANSWER_SNAPSHOT))
    assert d.action is Action.FALLBACK
    # CREATING → 暂存
    assert _decide(_box(TurnPhase.CREATING), _ev(EV.ANSWER_DELTA)).action is Action.BUFFER_PENDING
    # STREAMING → 流式
    assert _decide(_box(TurnPhase.STREAMING), _ev(EV.ANSWER_DELTA)).action is Action.STREAM_ANSWER
    assert _decide(_box(TurnPhase.STREAMING), _ev(EV.ANSWER_SNAPSHOT)).action is Action.STREAM_ANSWER
    # PAUSED → 缓冲续流内容
    assert _decide(_box(TurnPhase.PAUSED), _ev(EV.ANSWER_DELTA)).action is Action.BUFFER_RESUME
    # SEALING → fallback（窗口期澄清由 clarify.request 分支承接）
    d = _decide(_box(TurnPhase.SEALING), _ev(EV.ANSWER_DELTA))
    assert d.action is Action.FALLBACK and d.detail == "sealing_in_progress"


def test_answer_final_window_vs_direct():
    ev = _ev(EV.ANSWER_FINAL)
    d = _decide(_box(TurnPhase.STREAMING), ev)
    assert d.action is Action.ANSWER_FINAL_WINDOW  # 默认窗口 1.2s > 0
    d = _decide(_box(TurnPhase.STREAMING), ev, clarify_window_sec=0)
    assert d.action is Action.ANSWER_FINAL_NOW
    d = _decide(_box(TurnPhase.PAUSED), ev)
    assert d.action is Action.ANSWER_FINAL_NOW  # 暂停期 final：真落定
    assert d.owner == "answer_final_paused"


def test_panel_routing_matrix():
    for name in (EV.TOOL_START, EV.TOOL_END, EV.THINKING_DELTA,
                 EV.THINKING_ROUND_END, EV.INTERIM, EV.NOTICE):
        assert _decide(_box(TurnPhase.STREAMING), _ev(name)).action is Action.PANEL_EVENT
        assert _decide(_box(TurnPhase.CREATING), _ev(name)).action is Action.BUFFER_PENDING
        assert _decide(_box(TurnPhase.PAUSED), _ev(name)).action is Action.BUFFER_PENDING
        d = _decide(_box(None), _ev(name))
        assert d.action is Action.FALLBACK and d.detail == "no_active_card"


def test_clarify_request_routing():
    ev = _ev(EV.CLARIFY_REQUEST, payload={"form": {"kind": "select"}})
    assert _decide(_box(TurnPhase.STREAMING), ev).action is Action.PAUSE_CLARIFY
    # SEALING + 窗口挂起 → 命中
    box = _box(TurnPhase.SEALING)
    assert box.turn is not None
    box.turn.pause_pending = True
    d = _decide(box, ev)
    assert d.action is Action.PAUSE_CLARIFY and d.owner == "clarify_window_hit"
    # SEALING 无挂起（窗口已过期）→ fallback
    d = _decide(_box(TurnPhase.SEALING), ev)
    assert d.action is Action.FALLBACK and d.detail == "clarify_without_stream"
    # PAUSED 重复请求 → 忽略
    assert _decide(_box(TurnPhase.PAUSED), ev).action is Action.IGNORE_CONSUMED
    # 无回合 → fallback
    assert _decide(_box(None), ev).action is Action.FALLBACK


def test_clarify_answer_routing():
    ev = _ev(EV.CLARIFY_ANSWER, payload={"values": {"k": "v"}})
    assert _decide(_box(TurnPhase.PAUSED), ev).action is Action.RESUME_CLARIFY
    d = _decide(_box(TurnPhase.STREAMING), ev)
    assert d.action is Action.IGNORE_CONSUMED  # 通告忽略（非 fallback）
    d = _decide(_box(None), ev)
    assert d.action is Action.IGNORE_CONSUMED


def test_health_state_values():
    assert ChatHealth.OK.value == "chat_ok"
    assert TurnPhase.CREATING.value == "creating"
