"""原语元素构造 — CardKit 2.0 wire 原语（结构化输入重写）.

前代来源：cardkit/elements.py + cards.py 的面板渲染链（生产验证 862 测试）。
与前代的差异仅在输入：前代 ``_render_status_children(lines: list[str])``
内嵌文本解析；本层输入是结构化 :class:`PanelRow`（state.py），语义逐条
对齐前代渲染产物（wire 格式逐字段一致——真飞书验证过的原语不重设计）。

关键 wire 决策（前代取证注释全量保留）：
- 灰色思考正文用 ``<font color='grey'>`` 内容级包裹而非 ``text_color``
  属性——卡片 2.0 文本元素 text_color 仅 plain_text 支持（s116 探针）；
- ``<font>`` 是行内元素，逐行包裹把灰色变成行内确定性行为（v2.0.12）；
- 「action」容器在 Card 2.0 已废弃（230099/200861），button 必须顶层元素
  （v1.7.0 E2E T8b→A3 铁证，clarify.py 消费）；
- 空面板/空 body 被 230099/200621 拒收——占位单空格 markdown（v1.7.0 R4-10）。
"""

from __future__ import annotations

from typing import Any

from .state import FooterStats, PanelRow, RowKind
from .theme import Theme

__all__ = [
    "ANSWER_ELEMENT_ID",
    "UNIFIED_PANEL_ELEMENT_ID",
    "LOADING_ELEMENT_ID",
    "FEISHU_ELEMENT_LIMIT",
    "ELEMENT_LIMIT_MARGIN",
    "count_tag_objects",
    "enforce_element_limit",
    "build_collapsible_panel",
    "build_answer_element",
    "build_loading_element",
    "build_panel_children",
    "build_footer_elements",
    "build_notice_row",
    "format_elapsed",
    "format_duration_label",
    "compact_number",
    "escape_md",
]

# ── 元素 ID（前代逐字节一致——wire 兼容 + 真飞书验证过的 id）────────────
STREAMING_ELEMENT_ID = "streaming_content"
ANSWER_ELEMENT_ID = "answer_content"
UNIFIED_PANEL_ELEMENT_ID = "agent_process_panel"
LOADING_ELEMENT_ID = "loading_icon"
LOADING_IMG_KEY = "img_v3_02vb_496bec09-4b43-4773-ad6b-0cdd103cd2bg"

# Feishu Card 2.0 element limit — every JSON object with a ``tag`` key
# counts toward this limit at all nesting levels.
FEISHU_ELEMENT_LIMIT = 200
ELEMENT_LIMIT_MARGIN = 5


def escape_md(value: str) -> str:
    """lark_md 行内特殊字符转义（前代 _escape_md 同语义）."""
    import re

    return re.sub(r"([`*_{}\[\]<>])", r"\\\1", value.replace("\\", "\\\\"))


def count_tag_objects(obj: Any) -> int:
    """递归统计含 tag 键的 JSON 对象数（200 元素上限的计数口径）."""
    count = 0
    if isinstance(obj, dict):
        if "tag" in obj:
            count += 1
        for v in obj.values():
            count += count_tag_objects(v)
    elif isinstance(obj, list):
        for item in obj:
            count += count_tag_objects(item)
    return count


def enforce_element_limit(
    card: dict[str, Any],
    *,
    panel_element_id: str = UNIFIED_PANEL_ELEMENT_ID,
    theme: Theme | None = None,
    collapsed_hint_template: str = "⚡ 还有 {n} 项已折叠",
) -> dict[str, Any]:
    """200 元素上限安全网（前代 _enforce_card_element_limit 逐语义迁移）.

    超限裁剪对象 = 面板子元素（最旧先出）；页脚与 answer 永不裁。
    折叠提示行计数累计（「⚡ 还有 N 项已折叠」）。原卡就地修改并返回。
    """
    threshold = FEISHU_ELEMENT_LIMIT - ELEMENT_LIMIT_MARGIN
    total = count_tag_objects(card)
    if total <= threshold:
        return card

    body = card.get("body", {})
    elements = body.get("elements", [])
    panel = None
    for elem in elements:
        if isinstance(elem, dict) and elem.get("element_id") == panel_element_id and elem.get("tag") == "collapsible_panel":
            panel = elem
            break

    if panel is None:
        # 无面板 —— 无处可裁（answer/footer 不可裁）
        return card

    aux_size = theme.aux_text_size() if theme is not None else "notation"
    children: list[dict] = panel.get("elements", [])

    hint_idx = None
    for i, child in enumerate(children):
        if isinstance(child, dict) and isinstance(child.get("content"), str) and "已折叠" in child["content"]:
            hint_idx = i
            break
    hint_template = {"tag": "markdown", "content": collapsed_hint_template.format(n=0), "text_size": aux_size}
    hint_tag_count = count_tag_objects([hint_template])  # typically 1
    if hint_idx is None:
        total += hint_tag_count  # 为新折叠提示预留精确空间

    trimmed_count = 0
    while total > threshold and len(children) > 1:
        first_content = children[0].get("content", "") if isinstance(children[0], dict) else ""
        remove_idx = 1 if isinstance(first_content, str) and first_content.endswith("已折叠") else 0
        removed = children.pop(remove_idx)
        total -= count_tag_objects([removed])
        trimmed_count += 1

    if trimmed_count > 0:
        # 重找提示行（可能因删除移位）
        hint_idx = None
        for i, child in enumerate(children):
            if isinstance(child, dict) and isinstance(child.get("content"), str) and "已折叠" in child["content"]:
                hint_idx = i
                break
        if hint_idx is not None:
            old_hint = children[hint_idx]["content"]
            existing_count = _parse_collapsed_count(old_hint)
            children[hint_idx]["content"] = collapsed_hint_template.format(n=existing_count + trimmed_count)
        else:
            children.insert(0, {
                "tag": "markdown",
                "content": collapsed_hint_template.format(n=trimmed_count),
                "text_size": aux_size,
            })

    panel["elements"] = children
    return card


def _parse_collapsed_count(hint: str) -> int:
    """「⚡ 还有 5 项已折叠」→ 5（无正则的轻量解析，前代同实现）."""
    idx = hint.find("项")
    if idx <= 0:
        return 0
    end = idx
    while end > 0 and hint[end - 1] == " ":
        end -= 1
    start = end
    while start > 0 and hint[start - 1].isdigit():
        start -= 1
    if start < end:
        try:
            return int(hint[start:end])
        except ValueError:
            return 0
    return 0


# ── 骨架原语 ──────────────────────────────────────────────────────────────


def build_collapsible_panel(
    *,
    expanded: bool,
    title_el: dict,
    elements: list[dict],
    element_id: str = UNIFIED_PANEL_ELEMENT_ID,
    vertical_spacing: str = "4px",
    icon_position: str = "right",
) -> dict:
    """折叠面板（前代 _collapsible_panel 逐字段一致）."""
    icon_el: dict[str, Any] = {
        "tag": "standard_icon",
        "token": "down-small-ccm_outlined",
        "size": "16px 16px",
    }
    if icon_position == "right":
        icon_el["color"] = "grey"
    return {
        "tag": "collapsible_panel",
        "expanded": expanded,
        "header": {
            "title": title_el,
            "vertical_align": "center",
            "icon": icon_el,
            "icon_position": icon_position,
            "icon_expanded_angle": -180,
        },
        "border": {"color": "grey", "corner_radius": "5px"},
        "vertical_spacing": vertical_spacing,
        "padding": "8px 8px 8px 8px",
        "elements": elements,
        "element_id": element_id,
    }


def build_panel_title_element(theme: Theme, title: str = "") -> dict:
    """面板标题元素（灰 notation；空 → theme.panel_title_default）."""
    text = title or theme.panel_title_default
    return {
        "tag": "plain_text",
        "content": text,
        "i18n_content": {"zh_cn": text, "en_us": text},
        "text_color": "grey",
        "text_size": theme.aux_text_size(),
    }


def build_answer_element(content: str, theme: Theme, *, element_id: str = ANSWER_ELEMENT_ID) -> dict:
    """answer 正文元素（流式打字机通道绑定此 element_id）."""
    return {
        "tag": "markdown",
        "content": content,
        "text_align": "left",
        "text_size": theme.answer_text_size(),
        "margin": "0px 0px 0px 0px",
        "element_id": element_id,
    }


def build_loading_element() -> dict:
    """Loading spinner（div + custom_icon；封卡 insert_before 锚点 + 首卡指示）."""
    return {
        "tag": "div",
        "icon": {
            "tag": "custom_icon",
            "img_key": LOADING_IMG_KEY,
            "size": "16px 16px",
        },
        "text": {
            "tag": "plain_text",
            "content": " ",
        },
        "element_id": LOADING_ELEMENT_ID,
    }


# ── 面板行渲染（结构化输入）───────────────────────────────────────────────


def build_tool_row(row: PanelRow, theme: Theme) -> dict:
    """工具标题行：官方图标 + 彩色粗体标题（pending 橙 running / done 绿 success）.

    耗时标签仅已执行行携带（V1 语义：running 行无耗时可计）。
    """
    title = theme.title_for_tool(row.tool_name)
    if row.duration_ms is not None and not row.pending:
        label = format_duration_label(row.duration_ms)
        if label:
            title = f"{title} ({label})"
    color = "orange-300" if row.pending else "green"
    content = f"<font color='{color}'>**{escape_md(title)}**</font>"
    return {
        "tag": "div",
        "icon": {
            "tag": "standard_icon",
            "token": theme.icon_for_tool(row.tool_name),
            "color": "grey",
        },
        "text": {
            "tag": "lark_md",
            "content": content,
            "text_size": theme.aux_text_size(),
        },
    }


def build_tool_detail_row(detail: str) -> dict | None:
    """工具命令摘要行（缩进 22px + plain_text 灰字；命令非 markdown 语义）."""
    detail = (detail or "").strip()
    if not detail:
        return None
    return {
        "tag": "div",
        "margin": "0px 0px 0px 22px",
        "text": {
            "tag": "plain_text",
            "content": detail,
            "text_color": "grey",
            "text_size": "notation",
        },
    }


def build_reasoning_round_title(
    round_index: int,
    elapsed_ms: float,
    *,
    finalized: bool,
    theme: Theme,
) -> dict:
    """思考轮次标题（三色语义，前代 v2.0.16）.

    绿 = 已封口；橙（orange-300）= 流式期未封口轮（V1 current_reasoning）；
    红不实施——前代全库零调用（帧流无失败数据源，不伪造状态）。
    耗时缺失省略不伪造（elapsed_ms <= 0 → 无「· X.Xs」段）。
    """
    color = "green" if finalized else "orange-300"
    en_label, zh_label = theme.text("round_n")
    text = zh_label.format(round_index)  # 中文行用 zh 格式（前代同款）
    elapsed = format_elapsed(elapsed_ms) if elapsed_ms > 0 else ""
    if elapsed:
        text += f" · {elapsed}"
    content = f"<font color='{color}'>**{text}**</font>"
    return {
        "tag": "div",
        "icon": {
            "tag": "standard_icon",
            "token": "robot-add_outlined",
            "size": "16px 16px",
            "color": "grey",
        },
        "text": {
            "tag": "lark_md",
            "content": content,
            "text_size": theme.aux_text_size(),
        },
    }


def build_reasoning_body_row(body: str) -> dict | None:
    """思考正文行 — lark_md 渲染 + 逐行 grey 包裹（v2.0.12 取证，见模块 docstring）."""
    detail = (body or "").strip()
    if not detail:
        return None
    wrapped = "\n".join(
        ln if ("<font" in ln or not ln.strip()) else f"<font color='grey'>{ln}</font>"
        for ln in detail.split("\n")
    )
    return {
        "tag": "div",
        "margin": "0px 0px 0px 22px",
        "text": {
            "tag": "lark_md",
            "content": wrapped,
            "text_size": "notation",
        },
    }


def build_notice_row(icon_token: str, body: str, *, color: str = "grey") -> dict:
    """通告/溯源行：官方 standard_icon + notation 正文（plain_text 无注入面）.

    v2.0.10 用户验收定案：面板一律官方图标 + 统一样式；text_color 仅
    plain_text 支持，与工具标题行同构。
    """
    return {
        "tag": "div",
        "icon": {
            "tag": "standard_icon",
            "token": icon_token,
            "color": color,
        },
        "text": {
            "tag": "plain_text",
            "content": body,
            "text_color": color,
            "text_size": "notation",
        },
    }


def build_panel_children(
    rows: tuple[PanelRow, ...] | list[PanelRow],
    theme: Theme,
    *,
    turn_state: str = "finalized",
) -> list[dict]:
    """面板行 → 子元素序列（前代 _render_status_children 结构化重写）.

    分组语义：连续同 ``round_index`` 的 thinking 行聚合为一轮（标题行 +
    正文行）；``round_finalized`` 是唯一封口依据（单一所有者，无推断）。
    ``turn_state="streaming"`` 时未封口轮渲染橙色；``"finalized"`` 全绿
    （定稿收口重渲染输入）。

    产出恒非空（飞书面板拒绝空 elements——末尾兜底单空格 markdown，
    前代同语义）。
    """
    children: list[dict] = []
    streaming = turn_state == "streaming"
    prev_round = 0
    for row in rows:
        if row.kind is RowKind.THINKING:
            if row.round_index != prev_round:
                # 新轮次 → 标题行（进行中橙仅 streaming + 未封口）
                children.append(build_reasoning_round_title(
                    row.round_index,
                    row.round_elapsed_ms or 0.0,
                    finalized=(not streaming) or row.round_finalized,
                    theme=theme,
                ))
                prev_round = row.round_index
            else:
                # 同轮后续行若改写了封口状态（引擎在 round.end 后追行），
                # 标题行颜色应跟随最新状态——简化语义：轮次标题已按首行
                # 渲染，后续行不再回改（引擎保证 round.end 先于后续行到达）。
                pass
            detail = build_reasoning_body_row(row.text)
            if detail is not None:
                children.append(detail)
            continue
        prev_round = 0
        if row.kind is RowKind.TOOL:
            children.append(build_tool_row(row, theme))
            if not row.pending and row.tool_detail:
                detail = build_tool_detail_row(row.tool_detail)
                if detail is not None:
                    children.append(detail)
            continue
        if row.kind is RowKind.NOTICE:
            children.append(build_notice_row(
                theme.icon_for_notice(row.level),
                row.text,
                color=theme.color_for_notice(row.level),
            ))
            continue
        # INTERIM：官方图标 + notation 灰字（前代 emoji 通报行同构；
        # 结构化输入不再解析 emoji，图标统一 info_outlined）
        children.append(build_notice_row("info_outlined", row.text, color="grey"))
    if not children:
        children.append({"tag": "markdown", "content": " "})
    return children


# ── 页脚 ──────────────────────────────────────────────────────────────────


def build_footer_elements(stats: FooterStats, theme: Theme) -> list[dict]:
    """定稿页脚：hr + 双语 markdown 统计行（+ 溯源/中止行）.

    字段显示规则（03-card-schema §2 / 前代语义）：
    - status 恒显示（已完成/出错/已停止）
    - elapsed / model / context / reasoning：有值才显示
    - reasoning 需 theme.reasoning_level 非空（v2.0.19 设置显隐）
    - job_id → 独立灰色溯源行（time_outlined，前代 cron 页脚同构）
    - abort_reason → 独立橙色警示行（warning_outlined）
    - is_error → 页脚红色（前代 _build_footer_elements 同款）
    空页脚（全字段缺失——理论上不可能，status 恒在）返回 []。
    """
    en_parts: list[str] = []
    zh_parts: list[str] = []

    en_done, zh_done = theme.text("status_error" if stats.is_error else "status_stopped" if stats.is_aborted else "status_completed")
    en_parts.append(en_done)
    zh_parts.append(zh_done)

    if stats.duration_ms > 0:
        val = format_elapsed(stats.duration_ms)
        en_label, zh_label = theme.text("elapsed")
        en_parts.append(en_label.format(val))
        zh_parts.append(zh_label.format(val))

    if stats.model:
        en_parts.append(stats.model)
        zh_parts.append(stats.model)

    if stats.context_max:
        pct = int(stats.context_used / stats.context_max * 100)
        val = f"{compact_number(stats.context_used)}/{compact_number(stats.context_max)} ({pct}%)"
        en_label, zh_label = theme.text("context")
        en_parts.append(en_label.format(val))
        zh_parts.append(zh_label.format(val))

    if theme.reasoning_level and stats.reasoning_effort:
        zh_val = theme.reasoning_level_zh(stats.reasoning_effort)
        en_parts.append(f"Reasoning {stats.reasoning_effort}")
        zh_parts.append(f"思考 {zh_val}")

    if not en_parts:
        return []

    en_content = "\n".join([" · ".join(en_parts)])
    zh_content = "\n".join([" · ".join(zh_parts)])
    if stats.is_error:
        en_content = f"<font color='red'>{en_content}</font>"
        zh_content = f"<font color='red'>{zh_content}</font>"

    elements: list[dict] = [
        {"tag": "hr"},
        {
            "tag": "markdown",
            "content": en_content,
            "i18n_content": {"zh_cn": zh_content, "en_us": en_content},
            "text_size": theme.aux_text_size(),
        },
    ]

    if stats.job_id:
        elements.append(build_notice_row("time_outlined", f"job_id: {stats.job_id}"))
    if stats.is_aborted and stats.abort_reason:
        elements.append(build_notice_row(
            "warning_outlined", stats.abort_reason, color="orange"))
    return elements


# ── 格式化 ────────────────────────────────────────────────────────────────


def format_elapsed(ms: float) -> str:
    """总耗时格式（前代 _format_elapsed）："12.3s" / "2m 30s"."""
    seconds = ms / 1000
    return f"{seconds:.1f}s" if seconds < 60 else f"{int(seconds // 60)}m {int(seconds % 60)}s"


def format_duration_label(ms: float) -> str:
    """单工具耗时标签（前代 _format_duration_label，V1 同款含空格）："820 ms" / "2.3 s"."""
    try:
        ms = max(0.0, float(ms))
    except Exception:  # noqa: BLE001 — 装饰性数据，永不抛
        return ""
    return f"{ms:.0f} ms" if ms < 1000 else f"{(ms / 1000):.1f} s"


def compact_number(n: int) -> str:
    """数字紧凑化（前代 _compact）："1.5K" / "2.3M"."""
    if n >= 1_000_000:
        m = n / 1_000_000
        return f"{int(m)}M" if m >= 100 else f"{m:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)
