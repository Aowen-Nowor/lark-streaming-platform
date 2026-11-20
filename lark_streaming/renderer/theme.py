"""主题 token — 显式注入的呈现配置（03-card-schema §3）.

前代渲染元素直接读 ``Config()`` 单例（``elements._text_sizes_enabled``
里 ``from ..config import Config``）——那是宿主感知泄漏。新架构全部
呈现代价经 :class:`Theme` 显式传参：渲染纯函数 ``render(state, theme)``
的 theme 即此；核心层内置中性默认（:data:`DEFAULT_THEME`），接入方换
品牌只换 theme 不碰渲染代码（00-overview §3 分层红线 4）。

前代来源（整体迁移，语义逐条对齐）：
- 工具图标/标题表：cardkit/tool_icons.py（V1 state/tooluse.py 移植链，
  生产验证 862 测试）——``terminal`` 只进图标表不进标题表等怪癖保留
- i18n 文案表：cardkit/i18n.py（zh_cn/en_us 双语，卡片级 i18n 仅标题/
  页脚支持；面板行是单语文本通道）
- 字号 token：v2.0.20 card_text_sizes（完全 opt-in：未配置零注入，
  元素走 v2.0.18 字面值 normal_v2 / notation）
- 页脚推理等级：v2.0.19 footer_reasoning_level（空 = 跳过该字段）
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

__all__ = [
    "DEFAULT_TOOL_ICON",
    "DEFAULT_NOTICE_ICON",
    "TextSizes",
    "Theme",
    "DEFAULT_THEME",
    "resolve_tool_icon",
    "resolve_tool_title",
    "resolve_notice_icon",
    "resolve_notice_color",
    "i18n_pair",
]


# ── 工具图标表（前代 tool_icons.py _TOOL_ICONS 逐条迁移）─────────────────
# v2.0.10：默认图标从 tool_02 换为官方枚举 token；v2.0.16：terminal 并入
# code_outlined（用户验收：shell 行齿轮图标语义不符）。
DEFAULT_TOOL_ICON = "code_outlined"

_TOOL_ICONS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("skill",), "app-default_outlined"),
    (("read", "open"), "file-link-text_outlined"),
    (("write", "edit"), "edit_outlined"),
    (("web_search", "web-search", "search"), "search_outlined"),
    (("web_fetch", "web-fetch", "fetch"), "language_outlined"),
    (("grep",), "doc-search_outlined"),
    (("glob",), "folder_outlined"),
    # terminal 为 hermes 实际 shell 工具名（v2.0.4 生产补充；v2.0.16 并入 code）
    (("exec", "bash", "command", "run", "terminal"), "code_outlined"),
    (("browser", "playwright", "navigate"), "browser-mac_outlined"),
    (("agent", "task", "spawn"), "robot_outlined"),
    (("check", "determine", "verify"), "list-check_outlined"),
    (("summarize", "analyze", "prepare"), "report_outlined"),
)

# ── 工具标题表（前代 _TOOL_TITLES 逐条迁移）─────────────────────────────
# 注意：terminal 不在标题表（V1 描述符表无 terminal → humanize → "Terminal"，
# 与用户看到的 V1 效果对齐；v2.0.4 决策保留）。
_TOOL_TITLES: tuple[tuple[tuple[str, ...], str], ...] = (
    (("skill",), "Load skill"),
    (("read", "open"), "Read"),
    (("write", "edit"), "Edit"),
    (("web_search", "web-search", "search"), "Search"),
    (("web_fetch", "web-fetch", "fetch"), "Fetch web page"),
    (("grep",), "Search text"),
    (("glob",), "Search files"),
    (("exec", "bash", "command", "run"), "Run command"),
    (("browser", "playwright", "navigate"), "Browser"),
    (("agent", "task", "spawn"), "Run sub-agent"),
    (("check", "determine", "verify"), "Check"),
    (("summarize", "analyze", "prepare"), "Analyze"),
)

# ── 通告行图标/颜色（结构化 level → 官方图标；emoji 解析不进渲染层）──────
DEFAULT_NOTICE_ICON = "info_outlined"

_NOTICE_ICONS: dict[str, str] = {
    "warn": "warning_outlined",
    "error": "warning_outlined",
    "info": "info_outlined",
}

_NOTICE_COLORS: dict[str, str] = {
    # v2.0.16 用户验收：警告类不能用灰色（Model fallback 行 2026-10-13 截图）；
    # error 红为 V1 _build_error_panel 生产原语色板。
    "warn": "orange",
    "error": "red",
    "info": "grey",
}

# ── i18n 文案表（前代 i18n.py _T 整体迁移）───────────────────────────────
# 值 = (en, zh)；_LOCALES 顺序即卡片 config.locales。
LOCALES: list[str] = ["zh_cn", "en_us"]

_STRINGS: dict[str, tuple[str, str]] = {
    "status_completed": ("Completed", "已完成"),
    "status_error": ("Error", "出错"),
    "status_stopped": ("Stopped", "已停止"),
    "elapsed": ("Elapsed {}", "耗时 {}"),
    "context": ("Context {}", "上下文 {}"),
    "processing": ("Processing...", "处理中..."),
    "loading_context": ("Loading context...", "正在加载上下文..."),
    "clarify_select_placeholder": ("Quick select...", "快速选择..."),
    "clarify_input_placeholder": ("Type your answer...", "请输入你的回答..."),
    "clarify_selected": ("Selected: {}", "已选择: {}"),
    "clarify_submitted": ("Submitted, awaiting confirmation...", "已提交，等待确认..."),
    "clarify_retry": ("Retry submission", "重试提交"),
    "clarify_confirmed": ("Confirmed", "已确认"),
    "clarify_unauthorized": (
        "This action was rejected by authorization check.",
        "该操作未通过授权校验。",
    ),
    "clarify_expired": (
        "This clarify card has expired or the session was restarted. Please ask again.",
        "该澄清卡片已过期或会话已重启，请重新发起提问。",
    ),
    "agent_process": ("agent loop", "agent loop"),
    "round_n": ("Round {}", "第 {} 轮"),
    "clarify_wait": ("⏸ Waiting for your choice...", "⏸ 等待你的选择…"),
    "answer_truncated": ("… (truncated)", "…（已截断）"),
    "collapsed_hint": ("⚡ {} more collapsed", "⚡ 还有 {} 项已折叠"),
}

# ── 字号 token（v2.0.20 完全 opt-in；None = 零注入走字面值）──────────────
ANSWER_TEXT_SIZE_TOKEN = "cus-ans"
AUX_TEXT_SIZE_TOKEN = "cus-aux"
ANSWER_TEXT_SIZE_FALLBACK = "normal_v2"   # v2.0.18 answer 正文原样字面值
AUX_TEXT_SIZE_FALLBACK = "notation"       # v2.0.18 辅助行原样字面值

# footer reasoning 等级中文映射（英文行显示官方原值；未知等级双语显示原值）
_REASONING_LEVEL_ZH: dict[str, str] = {
    "minimal": "极简",
    "low": "低",
    "medium": "中",
    "high": "高",
    "xhigh": "极高",
    "max": "最高",
    "ultra": "极致",
}


@dataclass(frozen=True, slots=True)
class TextSizes:
    """自定义字号三档值（v2.0.20 s157 真飞书探针定案的 token 路径）."""

    answer: str
    aux: str


@dataclass(frozen=True)
class Theme:
    """呈现主题（03-card-schema §3 四 token 组）.

    ``tool_icons`` / ``tool_titles``：接入方可整体替换（品牌化）；默认
    表为前代生产迁移的中性通用映射。``overrides`` 合并在默认表之后
    （接入方条目优先）。
    """

    tool_icons: Mapping[str, str] = field(default_factory=dict)
    tool_titles: Mapping[str, str] = field(default_factory=dict)
    text_sizes: TextSizes | None = None      # None = 完全 opt-in 零注入
    reasoning_level: str = ""                # 页脚推理等级（空 = 跳过字段）
    panel_title_default: str = "agent loop"

    # ── 解析入口（渲染层唯一消费面）──────────────────────────────────

    def icon_for_tool(self, name: str) -> str:
        """工具名 → 官方图标 token；未命中 DEFAULT_TOOL_ICON.

        匹配语义同前代：归一化（lower + '-'→'_'）后全等或 ``alias_`` 前缀；
        overrides 优先于默认表。永不抛异常。
        """
        normalized = _normalize_tool_name(name)
        if normalized:
            for aliases, icon in _TOOL_ICONS:
                if _alias_hit(normalized, aliases):
                    return self.tool_icons.get(normalized) or icon
            if normalized in self.tool_icons:
                return self.tool_icons[normalized]
        return DEFAULT_TOOL_ICON

    def title_for_tool(self, name: str) -> str:
        """工具名 → 展示标题；描述符表未命中 → humanize 回退（前代语义）."""
        normalized = _normalize_tool_name(name)
        if normalized in self.tool_titles:
            return self.tool_titles[normalized]
        if normalized:
            for aliases, title in _TOOL_TITLES:
                if _alias_hit(normalized, aliases):
                    return title
        return _humanize_tool_name(name)

    def icon_for_notice(self, level: str) -> str:
        return _NOTICE_ICONS.get(level, DEFAULT_NOTICE_ICON)

    def color_for_notice(self, level: str) -> str:
        return _NOTICE_COLORS.get(level, "grey")

    def text(self, key: str) -> tuple[str, str]:
        """i18n 文案对 (en, zh)；未知键零兜底（内部键集合封闭，直接 KeyError）."""
        return _STRINGS[key]

    def answer_text_size(self) -> str:
        return ANSWER_TEXT_SIZE_TOKEN if self.text_sizes else ANSWER_TEXT_SIZE_FALLBACK

    def aux_text_size(self) -> str:
        return AUX_TEXT_SIZE_TOKEN if self.text_sizes else AUX_TEXT_SIZE_FALLBACK

    def style_config(self) -> dict:
        """骨架卡 ``config.style`` 注入体；未配置字号 → ``{}``（零注入，v2.0.20）."""
        if not self.text_sizes:
            return {}
        return {
            "style": {
                "text_size": {
                    ANSWER_TEXT_SIZE_TOKEN: self.text_sizes.answer,
                    AUX_TEXT_SIZE_TOKEN: self.text_sizes.aux,
                }
            }
        }

    def reasoning_level_zh(self, level: str) -> str:
        return _REASONING_LEVEL_ZH.get(level, level)


def _normalize_tool_name(name: str | None) -> str:
    return (name or "").strip().lower().replace("-", "_")


def _alias_hit(normalized: str, aliases: tuple[str, ...]) -> bool:
    return any(normalized == a or normalized.startswith(f"{a}_") for a in aliases)


def _humanize_tool_name(name: str | None) -> str:
    """前代 tool_icons.humanize_tool_name 同款：``-``/``_``→空格 + 首字母大写."""
    try:
        cleaned = (name or "").replace("-", " ").replace("_", " ").strip()
        if not cleaned:
            return "Tool"
        return cleaned[0].upper() + cleaned[1:]
    except Exception:  # noqa: BLE001 — 标题解析绝不影响面板渲染
        return "Tool"


# 模块级便捷入口（默认主题语义；渲染层内部用 Theme 方法，这两组留给
# legacy 转换器与脚本场景）
def resolve_tool_icon(name: str) -> str:
    return DEFAULT_THEME.icon_for_tool(name)


def resolve_tool_title(name: str) -> str:
    return DEFAULT_THEME.title_for_tool(name)


def resolve_notice_icon(level: str) -> str:
    return DEFAULT_THEME.icon_for_notice(level)


def resolve_notice_color(level: str) -> str:
    return DEFAULT_THEME.color_for_notice(level)


def i18n_pair(key: str) -> tuple[str, str]:
    return _STRINGS[key]


#: 核心层中性默认主题（空 overrides——接入方经设置供值替换，05-adapters §4）
DEFAULT_THEME = Theme()
