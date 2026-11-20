"""lark-streaming-platform — 独立的飞书/Lark 流式卡片平台.

任何 agent 通过统一事件协议（LSEP/1，docs/design/01-protocol.md）接入，
获得飞书 CardKit 2.0 流式卡片能力。分层见 docs/design/00-overview.md。

交付进度：transport + renderer（Phase 1）→ engine 编排层（Phase 2）→
protocol 三绑定（Phase 3）→ adapters 接入层 + 网关 shell（Phase 4，
本包公开面见 :mod:`lark_streaming.adapters` 与 :mod:`lark_streaming.gateway`）。
"""

__version__ = "0.0.1"  # 版本号由用户明示裁决变更（2026-11-20 纪律），禁止自动 bump
PROTOCOL_VERSION = 1
