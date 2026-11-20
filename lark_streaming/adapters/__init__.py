"""接入层公开面 — 接入四件事的参考实现（05-adapters；ADR D8 脚手架定位）.

**本层是可丢弃的脚手架**：现阶段官方不会按 LSEP 协议接入，由本项目
代做适配让接入方零成本上手；未来 agent 官方按协议原生接入后，本层
整体退役。协议（01-protocol）与核心引擎（02/03）永续。本层不得向
协议文档泄漏内部实现细节，也不得成为接入的必要条件。

分层红线（00-overview §3）：本层位于最上层，向下依赖 engine/protocol；
核心层（engine 及以下）零 agent 感知——本模块的任何变更不触及核心层。

接入四件事（05-adapters §1）：
1. 事件映射器 → :mod:`._legacy_text`（前代文本启发式）/
   :class:`.hermes.HermesAdapter`（hermes 元数据契约）/
   :class:`.openclaw.OpenclawMapper`（openclaw bridge 消息）
2. 投递表面 → 形态 A ``engine.submit``（适配器内直调）；形态 B 经
   stdio/HTTP/WS 绑定（见 :mod:`lark_streaming.gateway`）
3. 凭证与设置供值 → :mod:`lark_streaming.credentials` + 网关配置面
4. 降级履行 → 接入方义务：ack verdict ∈ {fallback, degraded} 必须
   原生投递（适配器以 ``DeliveryResult.action="native"`` 聚合呈现）
"""

from ._envelope import EnvelopeBuilder, TurnRef, TurnRegistry
from ._legacy_text import (
    CH_FINAL,
    CH_INTERIM,
    CH_PLAIN,
    CH_PROGRESS,
    CH_STREAM,
    ClassifyResult,
    classify_edit,
    classify_send,
    is_notice_text,
    is_progress_text,
    match_verb_line,
    parse_progress_line,
    preview_text,
    split_progress_segments,
    strip_dedup_suffix,
)
from .hermes import DeliveryResult, FrameVerdict, HermesAdapter, NativeAction
from .openclaw import OpenclawMapper

__all__ = [
    # 形态 A：hermes（in-process 出站钩子翻译面）
    "HermesAdapter", "DeliveryResult", "NativeAction", "FrameVerdict",
    # 形态 B：openclaw（bridge 消息映射器）
    "OpenclawMapper",
    # legacy 文本兼容转换器（01-protocol §8 参考实现）
    "CH_STREAM", "CH_PROGRESS", "CH_INTERIM", "CH_FINAL", "CH_PLAIN",
    "ClassifyResult", "classify_send", "classify_edit",
    "is_notice_text", "is_progress_text",
    "strip_dedup_suffix", "parse_progress_line", "match_verb_line",
    "preview_text", "split_progress_segments",
    # 信封素材（适配器内部公共词表）
    "EnvelopeBuilder", "TurnRef", "TurnRegistry",
]
