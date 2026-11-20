"""编排层公开面 — 回合状态机 + per-chat 队列 + 竞态守卫（02-engine）.

分层红线：本层向下依赖 renderer/transport，向上供 protocol（Phase 3
绑定）与 adapters 调用；零 agent 感知（编排层以下零宿主 ABI——
00-overview §3）。

典型用法（形态 A，05-adapters §2）::

    transport = FeishuTransport(FeishuCredentials.from_env())
    engine = StreamingEngine(transport, on_lifecycle=...)
    ack = await engine.submit({...LSEP/1 信封...})
    # ack.verdict == "fallback" → 接入方必须原生投递（永不丢帧）
"""

from .engine import Ctrl, StreamingEngine
from .events import (
    EV,
    KNOWN_EVENTS,
    LIFECYCLE_TYPES,
    TURN_SCOPED_EVENTS,
    Ack,
    EnvelopeError,
    Event,
    LifecycleEvent,
    Verdict,
)
from .guards import Action, Decision, GUARD_OWNERS, decide
from .settings import EngineSettings
from .state import ChatBox, ChatHealth, TurnPhase, TurnState

__all__ = [
    "StreamingEngine", "EngineSettings", "Ctrl",
    "Event", "EV", "KNOWN_EVENTS", "TURN_SCOPED_EVENTS",
    "EnvelopeError", "Ack", "Verdict", "LifecycleEvent", "LIFECYCLE_TYPES",
    "Action", "Decision", "decide", "GUARD_OWNERS",
    "TurnPhase", "ChatHealth", "TurnState", "ChatBox",
]
