"""CardKit sequence 自愈 — 300317 冲突与 200850 会话超时主流程闭环.

语义定案（前代 seq.py 全量迁移；真飞书实验 s98 实证，04-knowledge-freeze Q1/Q2）：

- 300317：重放/回退 seq 必现冲突；**seq+1 立即自愈**；跳号允许。
  恢复策略 = 主流程自愈：捕获 → 本地 seq 对齐服务端（+1）→ 重发同内容。
  所有走本 helper 的调用内容均为全量语义（answer 缓冲/状态快照/页脚），
  重发幂等。
- 200850：官方流式会话 ~10min 寿命到期（生产实证 600s 整撞线）。
  官方恢复动作 = settings ``streaming_mode:true`` 同卡重开，sequence 跳号
  允许。恢复策略 = seq+1 → reopen(seq) → seq+1 重发；重开成功即刷新
  状态上的会话起点（主动续期计时基线）。

循环上限 3：网络重试最多使服务端领先一次；200850 自愈一次消耗 2 个 seq，
3 是防御性余量。上限外上抛——交由引擎降级红线兜住消息不丢。
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from typing import Any

from .errors import (
    CARDKIT_SEQUENCE_CONFLICT,
    CARDKIT_STREAMING_TIMEOUT,
)

_logger = logging.getLogger("lark_streaming.transport.seq")

SEQ_CONFLICT = CARDKIT_SEQUENCE_CONFLICT
STREAM_TIMEOUT = CARDKIT_STREAMING_TIMEOUT

#: 自愈重试上限（前代 _MAX_SEQ_RECOVERIES 语义）
MAX_SEQ_RECOVERIES = 3


def is_seq_conflict(exc: BaseException) -> bool:
    """异常是否为 300317（duck-type 读 .code）."""
    return getattr(exc, "code", None) == SEQ_CONFLICT


def is_stream_timeout(exc: BaseException) -> bool:
    """异常是否为 200850（duck-type 读 .code）."""
    return getattr(exc, "code", None) == STREAM_TIMEOUT


def mark_session_renewed(st: Any, *, now: float | None = None) -> None:
    """重开后刷新状态上的会话起点（主动续期计时基线；无该字段则跳过）.

    ``now`` 由调用方注入（引擎传 clock——可测试性红线：transport helper
    不得自读 wall/monotonic 时钟写入引擎状态，否则测试台架 clock 被绕过）。
    """
    try:
        st.stream_session_at = time.monotonic() if now is None else now
    except Exception:  # noqa: BLE001 — 装饰性计时基线，失败不影响自愈主链
        _logger.debug("seq: stream_session_at mark skipped", exc_info=True)


async def call_with_seq_recovery(
    st: Any,
    send: Callable[[int], Awaitable[Any]],
    *,
    what: str,
    reopen: Callable[[int], Awaitable[Any]] | None = None,
    clock: Callable[[], float] | None = None,
) -> None:
    """执行 ``send(st.seq)``，300317/200850 主流程自愈.

    ``st`` 需带可变 ``seq`` 属性（引擎回合状态对象）；
    ``reopen`` 未注入时 200850 原样上抛（行为与前代一致）；
    ``clock`` 注入重开基线时钟（引擎传 self._clock；缺省 monotonic）。
    非自愈范围异常原样上抛；超上限同样上抛。
    """
    for attempt in range(MAX_SEQ_RECOVERIES):
        try:
            await send(st.seq)
            return
        except Exception as exc:  # noqa: BLE001 — 分类见下
            if attempt == MAX_SEQ_RECOVERIES - 1:
                raise
            if reopen is not None and is_stream_timeout(exc):
                st.seq += 1
                _logger.warning(
                    "seq: 200850 streaming timeout on %s — reopen (seq=%d) and resend",
                    what, st.seq,
                )
                await reopen(st.seq)
                mark_session_renewed(st, now=clock() if clock else None)
                st.seq += 1
                continue
            if not is_seq_conflict(exc):
                raise
            st.seq += 1
            _logger.warning(
                "seq: 300317 conflict on %s — self-heal #%d seq=%d",
                what, attempt + 1, st.seq,
            )


__all__ = [
    "call_with_seq_recovery", "is_seq_conflict", "is_stream_timeout",
    "mark_session_renewed", "SEQ_CONFLICT", "STREAM_TIMEOUT", "MAX_SEQ_RECOVERIES",
]
