"""clarify 交互渲染 — 独立澄清卡三态（03-card-schema §5 / 协议 clarify.*）.

形态裁决（可靠性第一 D7 的落地记录）：前代生产验证的澄清卡是**独立卡**
（select_static + input 回调 + 三态演进卡，v2.0.34 生产闭环）；03 §2 把
clarify 列为回合卡五槽位之一是终态愿景。Phase 1 按生产验证形态交付
独立卡；回合卡槽位内嵌（batch_update 插 form 元素 + 流式卡上的回调
behaviors 表现）未经真飞书实证，留待 L3 R7 验收点实测后定案——
未经实证的 wire 形态不进主链。

三态（03-card-schema §5）：
- ``pending``     ：表单可交互（select_static 下拉 + input 输入 + 选项列表）
- ``answered``    ：按钮置灰语义 = 软锁卡（展示应答摘要 + 重试按钮，
  前代 submitted 态）；confirmed 为引擎收到回流后的硬锁展示
- ``unauthorized``：操作者授权校验失败（fail-closed，不渲染表单值）

wire 红线（前代取证注释保留）：
- 「action」容器在 Card 2.0 已废弃（230099/200861 unsupported tag action，
  2026-11-11 生产事故）——button 必须顶层元素（v1.7.0 E2E T8b→A3）；
- select_static 用 plain_text（无 markdown）；选项 value = 序号字符串
  （回调载荷按 clarify_id 关联，引擎持有 form 原值）；
- 回调 behaviors value 的 ``lsp_clarify_action`` 是本协议命名空间
  （前代 ``hermes_clarify_action`` 不再泄漏宿主名词——ADR D8）。
"""

from __future__ import annotations

from typing import Any

from .elements import escape_md
from .state import ClarifyForm
from .theme import LOCALES, Theme

__all__ = [
    "CLARIFY_CALLBACK_KEY",
    "clarify_callback_value",
    "render_clarify_pending",
    "render_clarify_answered",
    "render_clarify_confirmed",
    "render_clarify_unauthorized",
    "render_clarify_expired",
]

#: 回调载荷键（本协议命名空间；前代 hermes_clarify_action 的协议化改名）
CLARIFY_CALLBACK_KEY = "lsp_clarify_action"

_MAX_CHOICE_LEN = 80  # 选项展示长度上限（前代 _CLARIFY_MAX_CHOICE_LEN）


def clarify_callback_value(action: str, clarify_id: str) -> dict[str, str]:
    """构造回调 behaviors.value（引擎/适配层与渲染层共用同一命名空间）."""
    return {CLARIFY_CALLBACK_KEY: action, "clarify_id": clarify_id}


def _normalize_choice(choice: str) -> str:
    """选项展示归一化：截断至 80 字符（前代 _normalize_choice 字符串分支）.

    协议 v1 输入是结构化 str 选项；前代的 dict-repr 解析属 legacy 转换器
    职责（接入层把非结构化输入归一为 str 后才进协议）。
    """
    text = (choice or "").strip()
    if not text:
        return ""
    if len(text) > _MAX_CHOICE_LEN:
        text = text[: _MAX_CHOICE_LEN - 1] + "…"
    return text


def _option_labels(form: ClarifyForm) -> list[str]:
    """选项归一化列表（空选项过滤；标签前缀 A. B. C. …，前代同款）."""
    out: list[str] = []
    for choice in form.options:
        n = _normalize_choice(choice)
        if n:
            out.append(n)
    return out


def _label_prefix(i: int) -> str:
    return chr(ord("A") + i) if i < 26 else str(i + 1)


def render_clarify_pending(
    form: ClarifyForm, theme: Theme, *, clarify_id: str = "",
) -> dict[str, Any]:
    """待答态（State 1）：标题 + 选项列表 + 下拉 + 自定义输入.

    select/multi_select 均渲染下拉（multi_select 的多选语义由回调
    payload 区分——CardKit 2.0 无原生 multi_select_static 控件时以
    select_static 承载，选项可重复提交合并；wire 能力以 L3 R7 实测校准）。
    input 恒渲染（自定义回答通道，前代语义 allow_custom 无条件为真）。
    """
    elements: list[dict[str, Any]] = []

    elements.append({
        "tag": "div",
        "icon": {
            "tag": "standard_icon",
            "token": "info_outlined",
            "size": "20px 20px",
            "color": "blue",
        },
        "text": {
            "tag": "lark_md",
            "content": f"**{escape_md(form.question)}**",
        },
    })

    choices = _option_labels(form)
    if form.kind in ("select", "multi_select") and choices:
        option_lines = [f"{_label_prefix(i)}. {escape_md(c)}" for i, c in enumerate(choices)]
        elements.append({"tag": "markdown", "content": "\n".join(option_lines)})

        en_ph, zh_ph = theme.text("clarify_select_placeholder")
        elements.append({
            "tag": "select_static",
            "element_id": "clarify_select",
            "placeholder": {
                "tag": "plain_text",
                "content": en_ph,
                "i18n_content": {"zh_cn": zh_ph, "en_us": en_ph},
            },
            "options": [
                {
                    "text": {"tag": "plain_text", "content": f"{_label_prefix(i)}. {c}"},
                    "value": str(i),
                }
                for i, c in enumerate(choices)
            ],
            "behaviors": [{
                "type": "callback",
                "value": clarify_callback_value(form.kind, clarify_id),
            }],
        })

    en_in, zh_in = theme.text("clarify_input_placeholder")
    elements.append({
        "tag": "input",
        "element_id": "clarify_input",
        "placeholder": {
            "tag": "plain_text",
            "content": en_in,
            "i18n_content": {"zh_cn": zh_in, "en_us": en_in},
        },
        "max_length": 500,
        "name": "clarify_input",
        "behaviors": [{
            "type": "callback",
            "value": clarify_callback_value("input_submit", clarify_id),
        }],
    })

    return {
        "schema": "2.0",
        "config": {"streaming_mode": False, "locales": LOCALES},
        "body": {"elements": elements},
    }


def render_clarify_answered(
    form: ClarifyForm, theme: Theme, *, selected: str, clarify_id: str = "",
) -> dict[str, Any]:
    """已答软锁态（State 2）：选择摘要 + 已提交提示 + 重试按钮（顶层 button）."""
    safe = escape_md(selected)
    en_sel, zh_sel = theme.text("clarify_selected")
    en_sel_label = en_sel.format(safe)
    zh_sel_label = zh_sel.format(safe)
    en_sub, zh_sub = theme.text("clarify_submitted")
    en_retry, zh_retry = theme.text("clarify_retry")

    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "icon": {
                "tag": "standard_icon",
                "token": "lock_outlined",
                "size": "20px 20px",
                "color": "orange",
            },
            "text": {"tag": "lark_md", "content": f"**{escape_md(form.question)}**"},
        },
        {
            "tag": "div",
            "icon": {
                "tag": "standard_icon",
                "token": "lock_outlined",
                "size": "16px 16px",
                "color": "orange",
            },
            "text": {
                "tag": "lark_md",
                "content": en_sel_label,
                "i18n_content": {"zh_cn": zh_sel_label, "en_us": en_sel_label},
            },
        },
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": f"*{en_sub}*",
                "i18n_content": {"zh_cn": f"*{zh_sub}*", "en_us": f"*{en_sub}*"},
            },
        },
        {
            # 「action」容器在 Card 2.0 已废弃（230099/200861）——button 必须
            # 顶层元素（v1.7.0 E2E T8b→A3 铁证，修复后真飞书 E2E 通过）。
            "tag": "button",
            "text": {
                "tag": "plain_text",
                "content": en_retry,
                "i18n_content": {"zh_cn": zh_retry, "en_us": en_retry},
            },
            "type": "primary",
            "behaviors": [{
                "type": "callback",
                "value": clarify_callback_value("retry_submit", clarify_id),
            }],
        },
    ]
    return {
        "schema": "2.0",
        "config": {"streaming_mode": False, "locales": LOCALES},
        "body": {"elements": elements},
    }


def render_clarify_confirmed(
    form: ClarifyForm, theme: Theme, *, selected: str,
) -> dict[str, Any]:
    """已确认硬锁态（State 3 回流展示）：绿色 resolve 图标 + 选择摘要."""
    safe = escape_md(selected)
    en_sel, zh_sel = theme.text("clarify_selected")
    en_sel_label = en_sel.format(safe)
    zh_sel_label = zh_sel.format(safe)
    en_conf, zh_conf = theme.text("clarify_confirmed")

    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "icon": {
                "tag": "standard_icon",
                "token": "resolve_filled",
                "size": "20px 20px",
                "color": "green",
            },
            "text": {"tag": "lark_md", "content": f"**{escape_md(form.question)}**"},
        },
        {
            "tag": "div",
            "icon": {
                "tag": "standard_icon",
                "token": "resolve_filled",
                "size": "16px 16px",
                "color": "green",
            },
            "text": {
                "tag": "lark_md",
                "content": en_sel_label,
                "i18n_content": {"zh_cn": zh_sel_label, "en_us": en_sel_label},
            },
        },
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": en_conf,
                "i18n_content": {"zh_cn": zh_conf, "en_us": en_conf},
            },
        },
    ]
    return {
        "schema": "2.0",
        "config": {"streaming_mode": False, "locales": LOCALES},
        "body": {"elements": elements},
    }


def render_clarify_unauthorized(
    form: ClarifyForm, theme: Theme,
) -> dict[str, Any]:
    """未授权态（fail-closed）：红色警示，不渲染表单值与任何可交互控件."""
    en_unauth, zh_unauth = theme.text("clarify_unauthorized")
    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "icon": {
                "tag": "standard_icon",
                "token": "warning_outlined",
                "size": "20px 20px",
                "color": "red",
            },
            "text": {"tag": "lark_md", "content": f"**{escape_md(form.question)}**"},
        },
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": en_unauth,
                "i18n_content": {"zh_cn": zh_unauth, "en_us": en_unauth},
            },
        },
    ]
    return {
        "schema": "2.0",
        "config": {"streaming_mode": False, "locales": LOCALES},
        "body": {"elements": elements},
    }


def render_clarify_expired(theme: Theme) -> dict[str, Any]:
    """过期态（回调命中已重启会话——前代 clarify._expired_response 语义）."""
    en_exp, zh_exp = theme.text("clarify_expired")
    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "icon": {
                "tag": "standard_icon",
                "token": "time_outlined",
                "size": "20px 20px",
                "color": "grey",
            },
            "text": {
                "tag": "lark_md",
                "content": en_exp,
                "i18n_content": {"zh_cn": zh_exp, "en_us": en_exp},
            },
        },
    ]
    return {
        "schema": "2.0",
        "config": {"streaming_mode": False, "locales": LOCALES},
        "body": {"elements": elements},
    }
