"""卡片字节预算 — 27000 bytes 本地预检（03-card-schema §6 / 前代 OPT-1c）.

官方单卡上限 30KB（200860 确定性拒收）；前代生产实测安全值为 27000
（`_CARD_BYTE_BUDGET`），留出序列/settings 等外围开销裕度。本层职责：
上传前本地测量 + 预检拦截（省一次注定失败的往返）。裁剪策略属渲染层
（Phase 3，裁剪顺序 interim → thinking 历史 → answer 尾部；页脚与
answer 终值永不裁剪），本层只做测量与拦截。
"""

from __future__ import annotations

import json
from typing import Any

#: 前代生产实测安全预算（bytes）
CARD_BYTE_BUDGET = 27_000

_OFFICIAL_HARD_LIMIT = 30_000  # 官方 200860 阈值（参考值，L3 R6 实测校准）


def measure_card_bytes(card: dict[str, Any]) -> int:
    """按飞书上传形态测量卡片 JSON 字节数（ensure_ascii=False，UTF-8）."""
    return len(json.dumps(card, ensure_ascii=False).encode("utf-8"))


def assert_card_size(card: dict[str, Any], *, limit: int = CARD_BYTE_BUDGET) -> int:
    """预检卡片字节预算；超限抛 CardSizeExceeded（调用方转降级/裁剪路径）.

    返回实测字节数。
    """
    size = measure_card_bytes(card)
    if size > limit:
        from .errors import CardSizeExceeded

        raise CardSizeExceeded(size, limit)
    return size


__all__ = ["CARD_BYTE_BUDGET", "_OFFICIAL_HARD_LIMIT", "measure_card_bytes", "assert_card_size"]
