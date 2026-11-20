"""竞态守卫 — 事件裁决表（02-engine §1.1/§6，单一所有者）.

每个状态转移由且仅由一个守卫函数裁决；裁决表 :func:`decide` 是纯函数
（零 IO、零时钟、零随机），输入 chat 盒快照与事件，输出 :class:`Decision`
（动作 + 所有者名）。引擎消费循环对每个帧调用恰好一次 decide，并按
Decision.owner 打结构化日志（02-engine §9，Phase 6 双跑对照数据源）。

守卫注册表在模块加载期冻结（:data:`GUARD_OWNERS`——转移 → 所有者的
封闭映射），运行期不可变更（02-engine §1.1 单一所有者条款）。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from .events import EV, Event, TURN_SCOPED_EVENTS
from .settings import EngineSettings
from .state import ChatBox, TurnPhase

__all__ = ["Action", "Decision", "decide", "GUARD_OWNERS"]


class Action(str, Enum):
    """守卫裁决动作（引擎消费循环的执行原语）."""

    CREATE_TURN = "create_turn"              # turn.start → 新回合 CREATING
    SEAL_AND_CREATE = "seal_and_create"      # 旧回合真落定 → 新回合（§5.1）
    SEAL_ABORT = "seal_abort"                # turn.abort → 标注中止落定
    BUFFER_PENDING = "buffer_pending"        # CREATING 窗口暂存（ack 延迟）
    BUFFER_RESUME = "buffer_resume"          # PAUSED 期 answer 只缓冲无 API
    STREAM_ANSWER = "stream_answer"          # answer 流式帧 → 打字机通道
    ANSWER_FINAL_NOW = "answer_final_now"    # 直落定（窗口关闭）
    ANSWER_FINAL_WINDOW = "answer_final_window"  # 进澄清窗口（T_pause 挂起）
    PANEL_EVENT = "panel_event"              # 面板事件 → 行变更 + 合并窗口
    PAUSE_CLARIFY = "pause_clarify"          # clarify.request → PAUSED
    RESUME_CLARIFY = "resume_clarify"        # clarify.answer → STREAMING 续流
    DELIVER = "deliver"                      # 旁路静态卡（§8，不影响回合）
    IGNORE_CONSUMED = "ignore_consumed"      # 通告忽略（幂等/迟到，ack consumed）
    FALLBACK = "fallback"                    # 未消费 → 接入方原生投递


@dataclass(frozen=True, slots=True)
class Decision:
    """单帧裁决（owner = 守卫名，结构化日志与双跑对照键）."""

    action: Action
    owner: str
    detail: str = ""


def _phase_of(box: ChatBox) -> TurnPhase | None:
    """当前回合态；None = IDLE（无活跃回合）."""
    st = box.turn
    return None if st is None else st.phase


def decide(box: ChatBox, ev: Event, settings: EngineSettings) -> Decision:
    """事件裁决表（02-engine §6 摘要表的全量展开）."""
    name = ev.event
    phase = _phase_of(box)

    # ── 回合外投递：完全旁路（§8）───────────────────────────────────────
    if name == EV.DELIVER:
        return Decision(Action.DELIVER, "deliver_bypass")

    # ── 跨 turn 帧（单一活跃卡红线，01-protocol §5.1）──────────────────
    st = box.turn
    if (
        st is not None
        and name in TURN_SCOPED_EVENTS
        and name != EV.TURN_START
        and ev.turn_id != st.turn_id
    ):
        # 旧回合已终结/被超权：其迟到帧一律 fallback（接入方原生投递，
        # 永不误入新卡——前代 turn_id 防串卡旗标的协议化替代）
        return Decision(Action.FALLBACK, "stale_turn_frame", detail="no_active_card")

    # ── 回合生命周期 ────────────────────────────────────────────────────
    if name == EV.TURN_START:
        if phase is None:
            return Decision(Action.CREATE_TURN, "turn_start_fresh")
        st = box.turn
        if st is not None and st.turn_id == ev.turn_id:
            # 幂等（01-protocol §5.4：重复 turn.start 不重复建卡）
            return Decision(Action.IGNORE_CONSUMED, "turn_start_idempotent",
                            detail="turn_start_idempotent")
        # 新 turn 打断未定稿旧回合：旧卡真落定（已 consumed 帧不受影响）
        return Decision(Action.SEAL_AND_CREATE, "turn_start_supersede")

    if name == EV.TURN_ABORT:
        if phase is None or phase is TurnPhase.SEALED:
            return Decision(Action.FALLBACK, "turn_abort_no_active_turn",
                            detail="no_active_card")
        return Decision(Action.SEAL_ABORT, "turn_abort")

    # ── 回答流 ──────────────────────────────────────────────────────────
    if name in (EV.ANSWER_DELTA, EV.ANSWER_SNAPSHOT, EV.ANSWER_FINAL):
        if phase is None or phase is TurnPhase.SEALED:
            return Decision(Action.FALLBACK, "answer_no_active_card",
                            detail="no_active_card")
        if phase is TurnPhase.CREATING:
            return Decision(Action.BUFFER_PENDING, "answer_buffered_creating")
        if phase is TurnPhase.PAUSED:
            if name == EV.ANSWER_FINAL:
                # 暂停期收到 final：真落定（缓冲含暂停期内容）
                return Decision(Action.ANSWER_FINAL_NOW, "answer_final_paused")
            return Decision(Action.BUFFER_RESUME, "answer_buffered_paused")
        if phase is TurnPhase.SEALING:
            return Decision(Action.FALLBACK, "answer_sealing_in_progress",
                            detail="sealing_in_progress")
        # STREAMING
        if name == EV.ANSWER_FINAL:
            if settings.clarify_window_sec > 0:
                return Decision(Action.ANSWER_FINAL_WINDOW, "answer_final_window")
            return Decision(Action.ANSWER_FINAL_NOW, "answer_final_direct")
        return Decision(Action.STREAM_ANSWER, "answer_stream")

    # ── 进度面板 ────────────────────────────────────────────────────────
    if name in (EV.TOOL_START, EV.TOOL_END, EV.THINKING_DELTA,
                EV.THINKING_ROUND_END, EV.INTERIM, EV.NOTICE):
        if phase is None or phase is TurnPhase.SEALED:
            return Decision(Action.FALLBACK, "panel_no_active_card",
                            detail="no_active_card")
        if phase is TurnPhase.CREATING:
            return Decision(Action.BUFFER_PENDING, "panel_buffered_creating")
        if phase is TurnPhase.PAUSED:
            return Decision(Action.BUFFER_PENDING, "panel_buffered_paused")
        if phase is TurnPhase.SEALING:
            return Decision(Action.FALLBACK, "panel_sealing_in_progress",
                            detail="sealing_in_progress")
        return Decision(Action.PANEL_EVENT, "panel_apply")

    # ── 澄清交互 ────────────────────────────────────────────────────────
    if name == EV.CLARIFY_REQUEST:
        if phase is TurnPhase.STREAMING:
            return Decision(Action.PAUSE_CLARIFY, "clarify_pause")
        if phase is TurnPhase.SEALING:
            st = box.turn
            if st is not None and st.pause_pending:
                # T_pause 窗口命中：final 后 1.2s 内到达的 clarify（前代
                # 边界 finalize 与真终态参数一致竞态的协议化）
                return Decision(Action.PAUSE_CLARIFY, "clarify_window_hit")
            return Decision(Action.FALLBACK, "clarify_without_stream",
                            detail="clarify_without_stream")
        if phase is TurnPhase.PAUSED:
            return Decision(Action.IGNORE_CONSUMED, "clarify_already_paused",
                            detail="already_paused")
        return Decision(Action.FALLBACK, "clarify_without_stream",
                        detail="clarify_without_stream")

    if name == EV.CLARIFY_ANSWER:
        if phase is TurnPhase.PAUSED:
            return Decision(Action.RESUME_CLARIFY, "clarify_resume")
        # 非 PAUSED：通告忽略（02-engine §6——lifecycle 不发、原生不投递）
        return Decision(Action.IGNORE_CONSUMED, "clarify_answer_ignored",
                        detail="not_paused")

    # 未知事件由路由层在 decide 之前拦截（unknown_event）；此处兜底
    return Decision(Action.FALLBACK, "unrouted_event", detail="unknown_event")


#: 守卫注册表（模块加载期冻结；转移 → 所有者封闭映射，02-engine §1.1）
GUARD_OWNERS: dict[str, str] = {
    "turn.start×IDLE": "turn_start_fresh",
    "turn.start×ACTIVE(同id)": "turn_start_idempotent",
    "turn.start×ACTIVE(异id)": "turn_start_supersede",
    "turn.abort×ACTIVE": "turn_abort",
    "turn.abort×无回合": "turn_abort_no_active_turn",
    "answer.*×STREAMING": "answer_stream",
    "answer.final×STREAMING(窗口开)": "answer_final_window",
    "answer.final×STREAMING(窗口关)": "answer_final_direct",
    "answer.*×CREATING": "answer_buffered_creating",
    "answer.*×PAUSED": "answer_buffered_paused",
    "answer.final×PAUSED": "answer_final_paused",
    "answer.*×SEALING/无回合": "answer_no_active_card|answer_sealing_in_progress",
    "panel.*×STREAMING": "panel_apply",
    "panel.*×CREATING/PAUSED": "panel_buffered_creating|panel_buffered_paused",
    "panel.*×SEALING/无回合": "panel_no_active_card|panel_sealing_in_progress",
    "clarify.request×STREAMING": "clarify_pause",
    "clarify.request×SEALING(窗口挂起)": "clarify_window_hit",
    "clarify.request×其他": "clarify_without_stream|clarify_already_paused",
    "clarify.answer×PAUSED": "clarify_resume",
    "clarify.answer×非PAUSED": "clarify_answer_ignored",
    "deliver×任意": "deliver_bypass",
}
