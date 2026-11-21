"""协议层共享 wire 素材 — 三绑定与引擎的接缝（01-protocol §3/§6/§7）.

本模块是 stdio / HTTP / WS 绑定的公共词表：帧解析、错误对象、hello
协商产物。信封编解码本体归引擎消费面（engine.events.Event.from_envelope
——绑定层不做二次语义校验，引擎是唯一的消费裁决者，坏绑定不能击穿）。

错误对象形态（01-protocol §7 的具体化，三绑定统一）::

    {"v": 1, "event": "error", "frame_id": "<尽力关联>",
     "payload": {"detail": "<机器可读原因>"}}

- HTTP 400 body / WS error 帧 / stdio stderr 行均为该形态；
- 「信封 JSON 解析失败」回错误对象、不产生 ack（§7 第一行）；
- 「字段缺失/类型错」由引擎裁决为 fallback ack（frame_id 尽力携带）。
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from lark_streaming import PROTOCOL_VERSION, __version__

__all__ = [
    "ENGINE_VERSION",
    "WireFrameError",
    "parse_frame",
    "wire_error",
    "settings_payload",
    "hello_ack",
    "negotiate_hello",
]

#: 引擎版本（hello.ack 通告用）
ENGINE_VERSION = __version__


class WireFrameError(ValueError):
    """信封 JSON 解析失败（01-protocol §7 第一行：错误对象，不产生 ack）.

    ``frame_id`` 尽力携带（JSON 根不是对象时无法提取，为空串）。
    """

    def __init__(self, detail: str, frame_id: str = "") -> None:
        super().__init__(detail)
        self.detail = detail
        self.frame_id = frame_id


def parse_frame(text: str) -> dict[str, Any]:
    """一行/一帧原始 JSON → 信封 dict；解析失败抛 :class:`WireFrameError`."""
    try:
        raw = json.loads(text)
    except ValueError:
        raise WireFrameError("invalid_json") from None
    if not isinstance(raw, dict):
        raise WireFrameError("invalid_envelope:not_an_object")
    return raw


def wire_error(detail: str, frame_id: str = "") -> dict[str, Any]:
    """绑定层错误对象（三绑定统一形态；HTTP 400 body / WS 帧 / stdio 行）."""
    return {
        "v": PROTOCOL_VERSION,
        "event": "error",
        "frame_id": frame_id,
        "payload": {"detail": detail},
    }


def settings_payload(settings: Any = None) -> dict[str, Any]:
    """引擎设置快照（hello.ack settings；Phase 4 配置面同源）."""
    from lark_streaming.engine import EngineSettings

    return asdict(settings if isinstance(settings, EngineSettings) else EngineSettings())


def hello_ack(frame_id: str, *, settings: Any = None) -> dict[str, Any]:
    """WS hello 握手应答（01-protocol §6）."""
    return {
        "v": PROTOCOL_VERSION,
        "event": "hello.ack",
        "frame_id": frame_id,
        "payload": {
            "protocol_v": PROTOCOL_VERSION,
            "engine_version": ENGINE_VERSION,
            "settings": settings_payload(settings),
        },
    }


def negotiate_hello(raw: dict[str, Any], *, settings: Any = None) -> tuple[dict[str, Any], bool]:
    """WS hello 帧裁决 → (应答帧, 是否保持连接).

    协商失败（payload 非 object / protocol_v 不支持）→ 错误对象 + 关闭
    连接（01-protocol §6：协商失败关闭连接）。frame_id 原样回带（可为空）。
    """
    fid = raw.get("frame_id")
    fid = fid if isinstance(fid, str) else ""
    payload = raw.get("payload")
    if not isinstance(payload, dict):
        return wire_error("handshake_failed:invalid_payload", fid), False
    pv = payload.get("protocol_v")
    if isinstance(pv, bool) or not isinstance(pv, int) or pv != PROTOCOL_VERSION:
        # audit #9-L1：客户端可控值不整帧进 detail（长度红线）——类型化 +
        # 截断，防恶意客户端撑大错误帧与日志
        pv_repr = str(pv)[:32] if isinstance(pv, (str, int, float)) else type(pv).__name__
        return wire_error(f"handshake_failed:unsupported_protocol_v:{pv_repr}", fid), False
    return hello_ack(fid, settings=settings), True
