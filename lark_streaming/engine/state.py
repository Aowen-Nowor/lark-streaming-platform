"""编排层运行时状态 — 回合状态机与 chat 盒（02-engine §1/§3）.

显式状态机（取代前代 FrameComposer 的隐式旗标组合
``finalized/degraded/pause_pending/paused/panel_on_card``）：

- 回合态 :class:`TurnPhase`：CREATING → STREAMING ⇄ PAUSED → SEALING →
  SEALED。IDLE 不是枚举值——``ChatBox.turn is None`` 即 IDLE（02-engine
  §1.1 状态表的空态）。
- 正交 chat 级态 :class:`ChatHealth`：OK ⇄ DEGRADED ⇄ RECOVERING → OK，
  独立于回合（前代实证：一个 turn 失败后同 chat 后续 turn 仍可恢复）。

前代对齐注记：
- ``TurnState.seq``：CardKit 卡片 sequence，**预递增纪律**（前代
  composer/engine.py: 每次 API 调用前 ``st.seq += 1``，300317/200850 由
  transport.call_with_seq_recovery 继续自愈递增）。
- ``TurnState.stream_session_at``：200850 会话续期计时基线；写入时钟由
  引擎注入（可测试性红线：transport helper 不自读时钟写入引擎状态）。
- 02-engine §3.3 的「建卡窗口 pending 缓冲（64 行上限）」在本架构由
  per-chat 有序队列 + ``maxsize=pending_max`` 显式实现（audit #2：
  修复前队列无界，pending_max 仅存于 settings 零接线）：队列满时新帧
  立得 ``FALLBACK(queue_full:native_delivery)`` 回执——背压就是另一种
  降级，帧不丢语义与永不丢帧红线同构；建卡窗口内帧排队等待而非触发
  第二次建卡；建卡失败时队列中后续帧按「无活跃回合」裁决 fallback。
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field, replace
from enum import Enum

from ..renderer.state import RenderState

__all__ = ["TurnPhase", "ChatHealth", "TurnState", "ChatBox"]


class TurnPhase(str, Enum):
    """卡片回合状态（02-engine §1.1 状态表）."""

    CREATING = "creating"      # turn.start 受理，建卡中（幂等屏障）
    STREAMING = "streaming"    # create+send 成功，流式消费
    PAUSED = "paused"          # clarify 等待（同一张卡保流式态）
    SEALING = "sealing"        # 定稿进行中（含澄清窗口挂起）
    SEALED = "sealed"          # 终态（正常/回退），GC 冷却后回收


class ChatHealth(str, Enum):
    """正交 chat 级状态（02-engine §1.2）."""

    OK = "chat_ok"
    DEGRADED = "chat_degraded"
    RECOVERING = "chat_recovering"


@dataclass(slots=True)
class TurnState:
    """单回合活跃卡状态（前代 CardTurnState 的状态机化重写）."""

    chat_id: str
    turn_id: str

    # 卡片身份
    card_id: str = ""
    message_id: str = ""
    reply_to: str = ""          # om_ 锚点（非 om_ 置空——99992354 必败前置，Q10）
    title: str = ""             # turn.start payload.title

    # 状态机
    phase: TurnPhase = TurnPhase.CREATING
    aborted: bool = False
    abort_reason: str = ""

    # CardKit sequence（预递增纪律）+ 会话续期基线（字段名与 seq.py 对齐）
    seq: int = 0
    stream_session_at: float = 0.0

    # 渲染输入（引擎维护、渲染层纯读）
    render: RenderState = field(default_factory=RenderState)

    # 计时（monotonic，经引擎 clock 注入可测）
    created_at: float = 0.0
    turn_started_at: float = 0.0
    last_activity: float = 0.0
    paused_at: float = 0.0      # PAUSED 进入时刻（PAUSED 超时判定基线）
    sealed_at: float = 0.0
    seal_failed: bool = False   # 定稿第一步失败（降级标注；内容经原生投递不丢）

    # 面板合并窗口标记（结构性新增行即时 flush；改写类 0.2s 合并——OPT-1a）
    panel_dirty: bool = False

    # 澄清（PAUSED 家族字段随回合重置——前代 v2.0.26 跨回合渗透防御）
    pause_pending: bool = False       # answer.final 后澄清窗口挂起（T_pause）
    clarify_id: str = ""
    clarify_form: object | None = None    # renderer ClarifyForm（避免 import 环，运行期赋值）
    clarify_message_id: str = ""
    pause_row_present: bool = False   # 面板暂停行在卡标记（resume 精确摘除）

    # 协议 seq 乱序检测（best-effort，01-protocol §5.3）
    last_seen_seq: int = -1

    # 思考轮游标（thinking.round.end 显式驱动，01-protocol §4.3）
    last_round_closed: int = 0

    def touch(self, now: float) -> None:
        self.last_activity = now

    def panel_title_for(self, now: float) -> str:
        """面板动态标题（前代 _panel_title 语义：标题 · 总耗时）.

        空标题走渲染层 i18n 默认（agent loop），不注入单语串——仅当回合
        显式带 title 或计时 ≥1s 时由引擎合成单语标题（前代同款）。
        """
        elapsed = max(0.0, now - self.turn_started_at)
        base = self.title
        if not base and elapsed < 1.0:
            return ""
        if not base:
            from ..renderer.theme import DEFAULT_THEME

            base = DEFAULT_THEME.text("agent_process")[0]
        return f"{base} · {elapsed:.1f}s"

    def with_panel_title(self, now: float) -> RenderState:
        return replace(self.render, panel_title=self.panel_title_for(now))


@dataclass(slots=True)
class ChatBox:
    """per-chat 运行时盒：有界队列 + 单消费者 + 级联定时器（02-engine §3）."""

    chat_id: str

    # 有界有序事件队列 + 单消费者协程（跨 chat 完全并行，chat 内严格串行）
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    consumer: asyncio.Task | None = None
    busy: bool = False            # 消费者在管道中（sweeper 避让依据）

    # 回合注册表
    turn: TurnState | None = None
    sealed: list[TurnState] = field(default_factory=list)  # SEALED 冷却注册表

    # 正交 chat 级降级态
    health: ChatHealth = ChatHealth.OK
    degraded_until: float = 0.0
    degrade_count: int = 0
    last_error: str = ""

    # 级联定时器（watchdog / pause / flush；chat GC 时级联取消，02-engine §3.4）
    timers: dict[str, asyncio.TimerHandle] = field(default_factory=dict)
    closing: bool = False

    @classmethod
    def create(cls, chat_id: str, *, pending_max: int) -> "ChatBox":
        """构造带背压上限的 chat 盒（02-engine §3.3 pending 缓冲显式接线）."""
        return cls(chat_id=chat_id, queue=asyncio.Queue(maxsize=pending_max))
