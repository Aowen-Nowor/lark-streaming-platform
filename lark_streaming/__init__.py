"""lark-streaming-platform — 独立的飞书/Lark 流式卡片平台.

任何 agent 通过统一事件协议（LSEP/1，docs/design/01-protocol.md）接入，
获得飞书 CardKit 2.0 流式卡片能力。分层见 docs/design/00-overview.md。

Phase 2 当前交付：编排层 engine（显式回合状态机 + per-chat 队列 +
竞态守卫，02-engine 规格）；Phase 1 已交付 transport + renderer。
"""

__version__ = "0.0.1"  # 版本号由用户明示裁决变更（2026-11-20 纪律），禁止自动 bump
PROTOCOL_VERSION = 1
