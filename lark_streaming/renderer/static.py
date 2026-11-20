"""deliver 旁路静态卡 — 回合外投递渲染（01-protocol §4.5 / 02-engine §8）.

完全旁路回合状态机：一次性静态卡，不劫持活跃流式卡、不合并（前代通道
隔离的显式化）。前代来源 = special.py build_cron_card 生产语义：

- 正文全链卫生化（sanitize_markdown → downgrade_tables(5) → 分块 2400）
  ——静态卡是 sanitize_markdown 的唯一主消费方（流式 answer 原文直发）；
- ``title`` 非空 → 正文前粗体标题行（v2.0.16 起无图标）+ summary 取标题；
- ``job_id`` 非空 → 末尾灰色溯源行（job_id: ...，正文不再重复包装头）；
- 空 body 被飞书 230099/200621 拒收 → 单空格 markdown 占位（v1.7.0 R4-10
  E2E T7 铁证）。

协议映射：``deliver`` payload `{text, title?, footer?}` →
``render_deliver_card(text, theme, title=..., job_id=footer.get("job_id"))``。
"""

from __future__ import annotations

from typing import Any

from .md import MAX_STATIC_TABLES, downgrade_tables, sanitize_markdown, split_long_text
from .elements import build_notice_row, escape_md
from .theme import LOCALES, Theme

__all__ = ["render_deliver_card"]


def render_deliver_card(
    text: str,
    theme: Theme,
    *,
    title: str = "",
    job_id: str = "",
) -> dict[str, Any]:
    """deliver 静态卡（前代 build_cron_card 逐语义迁移）."""
    card: dict[str, Any] = {
        "schema": "2.0",
        "config": {"locales": LOCALES, **theme.style_config()},
        "body": {"elements": []},
    }
    title = (title or "").strip()
    if title:
        card["body"]["elements"].append({
            "tag": "div",
            "text": {"tag": "lark_md", "content": f"**{escape_md(title)}**"},
        })
        card["config"]["summary"] = {"content": title}

    content = (text or "").strip()
    if content:
        if "summary" not in card["config"]:
            summary = content[:120].replace("\n", " ").replace("```", "").strip()
            if summary:
                card["config"]["summary"] = {"content": summary}
        chunks = [
            chunk
            for chunk in split_long_text(
                downgrade_tables(sanitize_markdown(content), limit=MAX_STATIC_TABLES))
            if chunk.strip()
        ]
        if chunks and title:
            card["body"]["elements"].append({"tag": "hr"})
        card["body"]["elements"].extend(
            {"tag": "markdown", "content": chunk} for chunk in chunks
        )

    job_id = (job_id or "").strip()
    if job_id:
        card["body"]["elements"].append(
            build_notice_row("time_outlined", f"Cronjob · job_id: {job_id}")
        )

    if not card["body"]["elements"]:
        # v1.7.0 (R4-10, E2E T7 铁证): body.elements=[] 被飞书 Card 2.0 直接
        # 拒绝（230099/200621 "parse card json err"）。单空格 markdown 与
        # 面板占位同构，schema 合法且渲染为空。
        card["body"]["elements"].append({"tag": "markdown", "content": " "})
    return card
