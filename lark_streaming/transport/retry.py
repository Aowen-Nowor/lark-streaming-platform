"""瞬态错误重试策略 — 前代 v1.3.x/v2.0.32 退避语义逐条迁移.

来源（04-knowledge-freeze §1 / 前代 client.py）：
- CardKit 瞬态码（2200/1663/300000）：0.1/0.3/0.6 递增退避
- 限频码 99991400：分道秒级退避 1.0/3.0/6.0（三连快重试只会继续撞墙）
- 网络错误（httpx.RequestError/TimeoutException）：同瞬态退避
- 200860 体积超限：确定性拒收，永不重试
- add_elements 批量操作：非幂等，调用方负责绕过自动重试
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TypeVar

import httpx

from .errors import FeishuAPIError, is_rate_limit_error, is_transient_error

_logger = logging.getLogger("lark_streaming.transport.retry")

T = TypeVar("T")

#: 普通瞬态退避（秒）
TRANSIENT_RETRY_DELAYS: tuple[float, ...] = (0.1, 0.3, 0.6)
#: 限频退避（秒）——v2.0.32 审计 P2：流式更新官方 10 QPS/卡豁免，
#: 撞限频的是 create/batch 突发，秒级退避足够等过窗口
RATE_LIMIT_RETRY_DELAYS: tuple[float, ...] = (1.0, 3.0, 6.0)

_NETWORK_ERRORS: tuple[type[BaseException], ...] = (
    httpx.RequestError,
    httpx.TimeoutException,
)


async def retry_transient(
    operation: str,
    coro_factory: Callable[[], Awaitable[T]],
    *,
    max_retries: int = len(TRANSIENT_RETRY_DELAYS),
) -> T:
    """执行异步操作，瞬态错误/网络错误自动重试；CancelledError 必须穿透.

    退避分道：限频码秒级，其余瞬态亚秒级（前代 v2.0.32 语义）。
    超限后上抛最后一次错误（调用方走降级红线）。
    """
    last_error: BaseException | None = None
    for attempt in range(max_retries + 1):
        try:
            return await coro_factory()
        except asyncio.CancelledError:
            raise  # 永不吞取消（前代 v1.3.4 红线）
        except FeishuAPIError as e:
            last_error = e
            if not is_transient_error(e):
                raise
            if attempt < max_retries:
                delays = (
                    RATE_LIMIT_RETRY_DELAYS if is_rate_limit_error(e)
                    else TRANSIENT_RETRY_DELAYS
                )
                delay = delays[attempt]
                _logger.info(
                    "transient retry: %s attempt=%d/%d code=%s delay=%.2fs",
                    operation, attempt + 1, max_retries, e.code, delay,
                )
                await asyncio.sleep(delay)
                continue
            raise
        except _NETWORK_ERRORS as e:
            last_error = e
            if attempt < max_retries:
                delay = TRANSIENT_RETRY_DELAYS[attempt]
                _logger.info(
                    "transient retry (network): %s attempt=%d/%d error=%s delay=%.2fs",
                    operation, attempt + 1, max_retries, type(e).__name__, delay,
                )
                await asyncio.sleep(delay)
                continue
            raise
    raise last_error  # unreachable, type-safe


__all__ = ["retry_transient", "TRANSIENT_RETRY_DELAYS", "RATE_LIMIT_RETRY_DELAYS"]
