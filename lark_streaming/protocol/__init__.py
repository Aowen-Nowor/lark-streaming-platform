"""协议层公开面 — LSEP/1 三绑定（stdio / HTTP / WS，01-protocol §3）.

绑定层职责：帧的搬运与回执回写——**零语义裁决**（信封校验、事件路由、
ack 语义全部归引擎消费面；绑定层只保证「坏帧不击穿」与「错误对象形态
统一」）。分层红线：本层向下依赖 engine（00-overview §3）；上层
（Phase 4 网关 shell / adapters）经本层投递。

依赖面：
- stdio 绑定零第三方依赖（纯 asyncio）；
- HTTP/WS 绑定依赖 aiohttp——经 ``lark_streaming.protocol.server`` 导入
  （安装 extra：``pip install lark-streaming-platform[gateway]``）。
"""

from .messages import (
    ENGINE_VERSION,
    WireFrameError,
    hello_ack,
    negotiate_hello,
    parse_frame,
    settings_payload,
    wire_error,
)
from .stdio import StdioBinding, serve_stdio

__all__ = [
    "StdioBinding", "serve_stdio",
    "WireFrameError", "parse_frame", "wire_error",
    "hello_ack", "negotiate_hello", "settings_payload", "ENGINE_VERSION",
]
