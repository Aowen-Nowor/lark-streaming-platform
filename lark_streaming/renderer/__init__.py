"""渲染层公开面 — 卡片 Schema v1 纯函数渲染（03-card-schema）.

渲染是纯函数：``render(state, theme) → CardKit JSON``。无 IO、无时钟、
无随机——同输入恒同输出，Golden 快照可测（03 §8）。

分层红线：本层只依赖 ``transport.budget``（字节预算常量，测量口径）与
自身子模块；不得 import 协议层/引擎层/接入层任何符号（00-overview §3）。
"""

from .clarify import (
    CLARIFY_CALLBACK_KEY,
    clarify_callback_value,
    render_clarify_answered,
    render_clarify_confirmed,
    render_clarify_expired,
    render_clarify_pending,
    render_clarify_unauthorized,
)
from .elements import (
    ANSWER_ELEMENT_ID,
    FEISHU_ELEMENT_LIMIT,
    LOADING_ELEMENT_ID,
    UNIFIED_PANEL_ELEMENT_ID,
    build_answer_element,
    build_footer_elements,
    build_loading_element,
    build_notice_row,
    build_panel_children,
    build_panel_title_element,
    count_tag_objects,
    enforce_element_limit,
    escape_md,
    format_duration_label,
    format_elapsed,
)
from .md import (
    MarkdownCache,
    MAX_CARD_TABLES,
    MAX_CHUNK_CHARS,
    MAX_STATIC_TABLES,
    downgrade_tables,
    sanitize_markdown,
    split_long_text,
    strip_invalid_image_keys,
)
from .state import ClarifyForm, FooterStats, PanelRow, RenderState, RowKind
from .static import render_deliver_card
from .streaming import (
    TrimReport,
    final_answer_content,
    footer_batch_actions,
    footer_summary,
    measure_card_bytes,
    panel_reseal_actions,
    render_full_card,
    render_initial_card,
    trim_to_budget,
)
from .theme import (
    DEFAULT_THEME,
    DEFAULT_TOOL_ICON,
    TextSizes,
    Theme,
    resolve_notice_icon,
    resolve_tool_icon,
    resolve_tool_title,
)

__all__ = [
    # state
    "RowKind", "PanelRow", "FooterStats", "ClarifyForm", "RenderState",
    # theme
    "Theme", "TextSizes", "DEFAULT_THEME", "DEFAULT_TOOL_ICON",
    "resolve_tool_icon", "resolve_tool_title", "resolve_notice_icon",
    # elements
    "ANSWER_ELEMENT_ID", "UNIFIED_PANEL_ELEMENT_ID", "LOADING_ELEMENT_ID",
    "FEISHU_ELEMENT_LIMIT", "count_tag_objects", "enforce_element_limit",
    "build_answer_element", "build_footer_elements", "build_loading_element",
    "build_notice_row", "build_panel_children", "build_panel_title_element",
    "escape_md",
    "format_elapsed", "format_duration_label",
    # md
    "sanitize_markdown", "split_long_text", "downgrade_tables",
    "strip_invalid_image_keys", "MarkdownCache",
    "MAX_CARD_TABLES", "MAX_STATIC_TABLES", "MAX_CHUNK_CHARS",
    # streaming
    "render_initial_card", "render_full_card", "trim_to_budget", "TrimReport",
    "final_answer_content", "footer_summary", "footer_batch_actions",
    "panel_reseal_actions", "measure_card_bytes",
    # static
    "render_deliver_card",
    # clarify
    "CLARIFY_CALLBACK_KEY", "clarify_callback_value",
    "render_clarify_pending", "render_clarify_answered",
    "render_clarify_confirmed", "render_clarify_unauthorized",
    "render_clarify_expired",
]
