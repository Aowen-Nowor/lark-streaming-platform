"""渲染输入状态模型 — 引擎维护、渲染层纯读（02-engine §4 / 03-card-schema §2）.

结构化输入红线（03-card-schema §7）：协议 v1 下输入已是结构化事件，
渲染层**零文本解析**——前代 ``status_lines: list[str]`` 的文本启发式
（💬 思考前缀 / emoji 工具行 / (×N) 去重后缀）在协议化后不再存在；
面板行以 :class:`PanelRow` 显式承载语义，前代文本编码只存在于接入层
legacy 转换器（01-protocol §8，Phase 4 交付）。

单一所有者（02-engine §1.1 精神在渲染输入上的延续）：
- ``PanelRow.round_finalized`` 是轮次封口的**权威字段**，由引擎在
  ``thinking.round.end`` 或下一条非 thinking 行到达时翻转；渲染层纯读，
  不做任何「从未封口行推断」的兜底逻辑。
- ``RenderState`` 是不可变视图（frozen）；引擎裁剪/合并时以
  ``dataclasses.replace`` 派生新实例，渲染产物因此对输入快照可测。
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum

__all__ = [
    "RowKind",
    "PanelRow",
    "FooterStats",
    "ClarifyForm",
    "RenderState",
]


class RowKind(str, Enum):
    """面板行种类（协议 01-protocol §4.3 事件 → 行的一一映射）."""

    THINKING = "thinking"  # thinking.delta / thinking.round.end → 思考气泡行
    TOOL = "tool"          # tool.start / tool.end → 工具行（官方图标 + 耗时）
    INTERIM = "interim"    # interim → mid-turn 播报行
    NOTICE = "notice"      # notice → 平台通告行（level 决定图标与颜色）


@dataclass(frozen=True, slots=True)
class PanelRow:
    """单条面板行（结构化，取代前代文本行解析）.

    字段按 kind 分组使用；未与 kind 相关的字段保持默认值，渲染层按
    kind 选取字段（协议化后不存在「解析不出结构」的行——legacy 转换器
    保证进渲染层的行全部结构化）。
    """

    kind: RowKind

    # thinking / interim / notice 共用正文
    text: str = ""

    # tool 行
    tool_name: str = ""            # 工具名（图标/标题解析输入）
    tool_detail: str = ""          # 命令/输出摘要（已执行行缩进灰字；pending 行无）
    pending: bool = False          # True = tool.start 后未 end（running 橙）
    duration_ms: float | None = None  # tool.end 回填（渲染为 "(2.3 s)"）

    # thinking 轮次归属（协议显式事件驱动，见模块 docstring 单一所有者）
    round_index: int = 0           # 1 起；非 thinking 行恒 0
    round_finalized: bool = False  # 轮次封口（绿）；False = 进行中（橙）
    round_elapsed_ms: float | None = None  # thinking.round.end duration_ms

    # notice 行
    level: str = "info"            # warn | info | error（01-protocol §4.3）

    def with_tool_end(
        self, *, duration_ms: float | None = None, detail: str = "",
    ) -> "PanelRow":
        """tool.end 语义回填（引擎专用）：pending 翻转 + 耗时/摘要更新."""
        return replace(
            self,
            pending=False,
            duration_ms=duration_ms if duration_ms is not None else self.duration_ms,
            tool_detail=detail or self.tool_detail,
        )


@dataclass(frozen=True, slots=True)
class FooterStats:
    """页脚统计（answer.final.stats / turn.abort reason / deliver 溯源）.

    协议 stats 映射：duration_ms / model / context_tokens{used,max} /
    extra（透传 dict，供接入层扩展；渲染层只渲染下列显式字段）。
    空值字段渲染时自动跳过（前代语义：拿不到数据的字段不显示）。
    """

    duration_ms: float = 0.0
    model: str = ""
    context_used: int = 0
    context_max: int = 0
    reasoning_effort: str = ""     # 非空才渲染（theme.reasoning_level 控制显隐）
    is_error: bool = False         # 错误终态（页脚红色）
    is_aborted: bool = False       # 中止终态（页脚「已停止」）
    abort_reason: str = ""         # turn.abort reason（中止原因行）
    job_id: str = ""               # deliver / cron 溯源


@dataclass(frozen=True, slots=True)
class ClarifyForm:
    """澄清表单（协议 clarify.request form payload，03-card-schema §5）."""

    kind: str                      # select | multi_select | input
    question: str
    options: tuple[str, ...] = ()  # select/multi_select 选项（label 即 value）
    placeholder: str = ""          # input 占位文案
    allow_custom: bool = False     # 预留（v1 渲染恒含自定义输入框，前代语义）

    def __post_init__(self) -> None:
        if self.kind not in ("select", "multi_select", "input"):
            raise ValueError(f"ClarifyForm: unsupported kind {self.kind!r}")
        if self.kind in ("select", "multi_select") and not self.options:
            raise ValueError(f"ClarifyForm: kind={self.kind} requires options")
        if not isinstance(self.options, tuple):
            object.__setattr__(self, "options", tuple(self.options))


@dataclass(frozen=True, slots=True)
class RenderState:
    """渲染输入快照（02-engine §4：引擎维护，渲染层纯读）.

    引擎在以下时点构造并调用渲染（02-engine §4 时点表）：
    - CREATING 成功 → :func:`lark_streaming.renderer.render_initial_card`
    - STREAMING 每 answer 帧 / 面板合并窗口到期 → ``render_full_card``
    - PAUSED 进入 → 暂停行（clarify_wait）经 :meth:`with_panel_rows` 并入
    - SEALING → 终值 patch + 页脚 + 面板收口产物
    """

    answer_text: str = ""          # answer 缓冲（delta 追加 / snapshot 替换）
    panel_rows: tuple[PanelRow, ...] = ()
    panel_title: str = ""          # 动态标题（空 → i18n 默认 "agent loop"）
    panel_expanded: bool = True
    footer: FooterStats | None = None  # None = 无页脚（流式期）；有 = 定稿收口

    def with_answer(self, text: str) -> "RenderState":
        return replace(self, answer_text=text)

    def with_panel_rows(self, rows: tuple[PanelRow, ...] | list[PanelRow]) -> "RenderState":
        return replace(self, panel_rows=tuple(rows))

    def with_footer(self, footer: FooterStats | None) -> "RenderState":
        return replace(self, footer=footer)

    def has_panel(self) -> bool:
        """懒面板判据（02-engine §4：面板存在性由渲染输入实测，不做推断）."""
        return bool(self.panel_rows)
