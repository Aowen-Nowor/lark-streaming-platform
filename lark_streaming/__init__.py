"""lark-streaming-platform — 独立的飞书/Lark 流式卡片平台.

任何 agent 通过统一事件协议（LSEP/1，docs/design/01-protocol.md）接入，
获得飞书 CardKit 2.0 流式卡片能力。分层见 docs/design/00-overview.md。

Phase 1 当前交付：transport 层（CardKit REST 客户端 + 前代怪癖知识落地）。
"""

__version__ = "0.0.3"
PROTOCOL_VERSION = 1
