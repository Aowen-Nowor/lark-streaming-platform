"""接入层共享素材 — LSEP/1 信封组装与回合注册表（05-adapters §1）.

适配器的公共词表：frame_id 生成、信封组装（01-protocol §2 字段表）、
per-chat 活跃回合注册表。仅供 ``adapters/*`` 内部使用（下划线前缀）；
接入方不应直接依赖（公开面见 ``adapters/__init__.py``）。

回合注册表是**适配器侧的加速路径**，不是权威：引擎是回合状态的唯一
裁决者（02-engine 守卫表）——注册表只回答「当前 chat 是否有我方开启的
活跃回合」，引擎侧的 stale/fallback 裁决始终生效（注册表漂移不能击穿
引擎守卫，只会多走一次 fallback → 原生投递，永不丢帧红线兜底）。
"""

from __future__ import annotations

import itertools
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "EnvelopeBuilder",
    "TurnRef",
    "TurnRegistry",
]


@dataclass(slots=True)
class TurnRef:
    """适配器侧活跃回合引用（引擎回合状态的本地缓存）."""

    turn_id: str
    seq: int = 0                 # 接入方侧单调序号（乱序检测输入，01-protocol §5.3）
    streamed: bool = False       # 是否已有 answer 帧上卡（降级履行决策线索）
    thinking_open: bool = False  # 思考轮未闭合（跨帧 💬 增长原地改写同一行）
    opened_at: float = 0.0
    seen_progress: set[str] = field(default_factory=set)  # Q9 去重（容量受限）


class TurnRegistry:
    """per-chat 活跃回合注册表（适配器侧；非权威，引擎守卫始终兜底）."""

    def __init__(self) -> None:
        self._turns: dict[str, TurnRef] = {}

    def active(self, chat_id: str) -> TurnRef | None:
        return self._turns.get(chat_id)

    def open(self, chat_id: str, turn_id: str, *, clock: float = 0.0) -> TurnRef:
        """登记新回合（覆盖旧引用；引擎侧 seal_and_create 裁决对齐）."""
        ref = TurnRef(turn_id=turn_id, opened_at=clock)
        self._turns[chat_id] = ref
        return ref

    def close(self, chat_id: str) -> TurnRef | None:
        """注销回合（final 已投递 / abort 后调用）."""
        return self._turns.pop(chat_id, None)


class EnvelopeBuilder:
    """LSEP/1 信封组装器（01-protocol §2 字段表；frame_id 全局唯一）.

    ``frame_id`` 格式：``{prefix}-{进程内单调计数}-{实例后缀}``——同进程
    唯一、跨重启不碰撞（实例后缀随机）。``ts`` 毫秒 epoch（时钟可注入）。
    """

    def __init__(self, *, prefix: str = "hf", clock: "callable | None" = None) -> None:  # type: ignore[valid-type]
        self._prefix = prefix
        self._counter = itertools.count(1)
        self._suffix = uuid.uuid4().hex[:6]
        self._clock = clock or (lambda: int(time.time() * 1000))

    def frame_id(self) -> str:
        return f"{self._prefix}-{next(self._counter)}-{self._suffix}"

    def build(
        self,
        event: str,
        *,
        chat_id: str,
        turn_id: str = "",
        payload: dict[str, Any] | None = None,
        seq: int = -1,
        frame_id: str | None = None,
        ts: int | None = None,
    ) -> dict[str, Any]:
        env: dict[str, Any] = {
            "v": 1,
            "event": event,
            "frame_id": frame_id or self.frame_id(),
            "chat_id": chat_id,
            "payload": dict(payload or {}),
        }
        if turn_id:
            env["turn_id"] = turn_id
        if ts is None:
            ts = int(self._clock())
        if ts:
            env["ts"] = ts
        if seq >= 0:
            env["seq"] = seq
        return env
