"""LSEP/1 事件模型与消费面校验 — 编排层输入契约（01-protocol §2/§4/§7）.

职责边界：信封**编解码与传输绑定**（stdio/HTTP/WS、hello 握手）归协议层
（00-overview §3，Phase 3 交付）；本模块承载编排层的输入契约——已解析的
结构化事件（:class:`Event`）、逐帧回执（:class:`Ack`）与引擎级通告
（:class:`LifecycleEvent`），并提供信封**消费面校验**（字段缺失/类型错 →
:class:`EnvelopeError`，绑定层据 01-protocol §7 转 fallback ack）。

红线：引擎对原始 dict 再做一次防御性校验（:meth:`StreamingEngine.submit`）
——坏绑定不能击穿消费循环；事件目录未知 → fallback（不静默吞掉，
01-protocol §1.4）。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Any

__all__ = [
    "EV",
    "KNOWN_EVENTS",
    "TURN_SCOPED_EVENTS",
    "EnvelopeError",
    "Event",
    "Verdict",
    "Ack",
    "LIFECYCLE_TYPES",
    "LifecycleEvent",
]


class EV:
    """事件类型名常量（01-protocol §4 目录逐字对齐）."""

    TURN_START = "turn.start"
    TURN_ABORT = "turn.abort"
    ANSWER_DELTA = "answer.delta"
    ANSWER_SNAPSHOT = "answer.snapshot"
    ANSWER_FINAL = "answer.final"
    TOOL_START = "tool.start"
    TOOL_END = "tool.end"
    THINKING_DELTA = "thinking.delta"
    THINKING_ROUND_END = "thinking.round.end"
    INTERIM = "interim"
    NOTICE = "notice"
    CLARIFY_REQUEST = "clarify.request"
    CLARIFY_ANSWER = "clarify.answer"
    DELIVER = "deliver"


#: 引擎消费的事件全集（hello 是绑定层握手帧，不经引擎；frame.ack /
#: lifecycle.event 是反向事件，由引擎产出）
KNOWN_EVENTS = frozenset({
    EV.TURN_START, EV.TURN_ABORT,
    EV.ANSWER_DELTA, EV.ANSWER_SNAPSHOT, EV.ANSWER_FINAL,
    EV.TOOL_START, EV.TOOL_END,
    EV.THINKING_DELTA, EV.THINKING_ROUND_END,
    EV.INTERIM, EV.NOTICE,
    EV.CLARIFY_REQUEST, EV.CLARIFY_ANSWER,
    EV.DELIVER,
})

#: 需要 turn_id 的回合域事件（01-protocol §2：turn.* / answer / tool /
#: thinking / interim / notice / clarify.* 必填；deliver 例外）
TURN_SCOPED_EVENTS = frozenset(KNOWN_EVENTS - {EV.DELIVER})

#: 生命周期通告类型（01-protocol §4.6，封闭集）
LIFECYCLE_TYPES = frozenset({
    "card_sealed_fallback", "chat_degraded", "chat_recovered", "shutdown",
    "internal_error",
})


class EnvelopeError(ValueError):
    """信封消费面校验失败（01-protocol §7：fallback ack / error 对象素材）.

    ``frame_id`` 尽力携带（缺失时为空串）——绑定层据其决定回 fallback ack
    还是 error 对象（无 frame_id 无法关联回执）。
    """

    def __init__(self, detail: str, frame_id: str = "") -> None:
        super().__init__(detail)
        self.detail = detail
        self.frame_id = frame_id


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True, slots=True)
class Event:
    """已解析的 LSEP/1 事件（信封字段逐项对齐 01-protocol §2）.

    ``seq`` 是接入方在 turn 内的单调序号（乱序检测输入，01-protocol §5.3，
    -1 = 未提供）；与 CardKit 卡片 sequence（:attr:`TurnState.seq`）无关。
    payload 冻结为只读视图——校验通过后任何模块不得改写载荷。
    """

    event: str
    frame_id: str
    chat_id: str
    turn_id: str = ""
    ts: int = 0
    seq: int = -1
    payload: Mapping[str, Any] = field(default_factory=dict)
    v: int = 1

    def __post_init__(self) -> None:
        if not isinstance(self.payload, MappingProxyType):
            object.__setattr__(self, "payload", MappingProxyType(dict(self.payload)))

    def payload_str(self, key: str, default: str = "") -> str:
        value = self.payload.get(key)
        return str(value) if isinstance(value, str) else default

    # ── 消费面校验（01-protocol §2 字段表逐项）────────────────────────────

    @classmethod
    def from_envelope(cls, raw: Any) -> "Event":
        """解析并校验原始信封；失败抛 :class:`EnvelopeError`."""
        if not isinstance(raw, dict):
            raise EnvelopeError("invalid_envelope:not_an_object")
        frame_id = raw.get("frame_id")
        fid = frame_id if isinstance(frame_id, str) else ""

        v = raw.get("v")
        if v is None:
            raise EnvelopeError("invalid_envelope:missing_field:v", fid)
        if not _is_int(v):
            raise EnvelopeError("invalid_envelope:invalid_field:v", fid)
        if v != 1:
            raise EnvelopeError(f"invalid_envelope:unsupported_version:{v}", fid)

        event = raw.get("event")
        if not isinstance(event, str) or not event:
            raise EnvelopeError("invalid_envelope:missing_field:event", fid)

        if not isinstance(frame_id, str) or not frame_id:
            raise EnvelopeError("invalid_envelope:missing_field:frame_id", fid)

        chat_id = raw.get("chat_id")
        if not isinstance(chat_id, str) or not chat_id:
            raise EnvelopeError("invalid_envelope:missing_field:chat_id", fid)

        turn_id = raw.get("turn_id")
        if turn_id is None:
            turn_id = ""  # deliver 等回合外事件合法缺省（01-protocol §2）
        if not isinstance(turn_id, str):
            raise EnvelopeError("invalid_envelope:invalid_field:turn_id", fid)
        if event in TURN_SCOPED_EVENTS and not turn_id:
            raise EnvelopeError("invalid_envelope:missing_field:turn_id", fid)

        payload = raw.get("payload")
        if payload is None:
            raise EnvelopeError("invalid_envelope:missing_field:payload", fid)
        if not isinstance(payload, dict):
            raise EnvelopeError("invalid_envelope:invalid_field:payload", fid)

        ts = raw.get("ts")
        if ts is not None and not _is_int(ts):
            raise EnvelopeError("invalid_envelope:invalid_field:ts", fid)

        seq = raw.get("seq")
        if seq is not None and (not _is_int(seq) or seq < 0):
            raise EnvelopeError("invalid_envelope:invalid_field:seq", fid)

        return cls(
            event=event,
            frame_id=frame_id,
            chat_id=chat_id,
            turn_id=turn_id,
            ts=int(ts) if ts is not None else 0,
            seq=int(seq) if seq is not None else -1,
            payload=payload,
            v=int(v),
        )


class Verdict(str, Enum):
    """frame.ack 裁决（01-protocol §4.6）."""

    CONSUMED = "consumed"      # 已进卡片管线
    FALLBACK = "fallback"      # 未消费——接入方必须原生投递
    DEGRADED = "degraded"      # chat 降级中——本帧及后续帧均走原生


@dataclass(frozen=True, slots=True)
class Ack:
    """逐帧回执（每帧恰好一个，按 frame_id 关联）."""

    frame_id: str
    verdict: Verdict
    message_id: str = ""
    detail: str = ""

    def to_protocol(self) -> dict[str, Any]:
        """协议 wire 形态（frame.ack payload；Phase 3 绑定层直接序列化）."""
        payload: dict[str, Any] = {
            "frame_id": self.frame_id,
            "verdict": self.verdict.value,
        }
        if self.message_id:
            payload["message_id"] = self.message_id
        if self.detail:
            payload["detail"] = self.detail
        return {"v": 1, "event": "frame.ack", "payload": payload}


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    """引擎级通告（01-protocol §4.6；接入方可选消费）."""

    type: str
    detail: str = ""
    chat_id: str = ""
    turn_id: str = ""
    code: int = 0

    def to_protocol(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"type": self.type}
        if self.detail:
            payload["detail"] = self.detail
        if self.chat_id:
            payload["chat_id"] = self.chat_id
        if self.turn_id:
            payload["turn_id"] = self.turn_id
        if self.code:
            payload["code"] = self.code
        return {"v": 1, "event": "lifecycle.event", "payload": payload}
