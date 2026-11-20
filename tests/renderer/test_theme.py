"""主题 token 解析测试（theme.py）— 前代 tool_icons 语义逐条对齐."""

from __future__ import annotations

from lark_streaming.renderer import DEFAULT_TOOL_ICON, TextSizes, Theme
from lark_streaming.renderer.theme import (
    ANSWER_TEXT_SIZE_FALLBACK,
    AUX_TEXT_SIZE_FALLBACK,
    ANSWER_TEXT_SIZE_TOKEN,
    AUX_TEXT_SIZE_TOKEN,
)


class TestToolIcon:
    def test_exact_aliases(self) -> None:
        t = Theme()
        assert t.icon_for_tool("read") == "file-link-text_outlined"
        assert t.icon_for_tool("open") == "file-link-text_outlined"
        assert t.icon_for_tool("grep") == "doc-search_outlined"
        assert t.icon_for_tool("terminal") == "code_outlined"  # v2.0.16 并入 code

    def test_prefix_match(self) -> None:
        t = Theme()
        assert t.icon_for_tool("web_search_deep") == "search_outlined"
        assert t.icon_for_tool("exec-remote") == "code_outlined"  # '-'→'_' 归一化

    def test_unknown_falls_back(self) -> None:
        t = Theme()
        assert t.icon_for_tool("mystery_tool") == DEFAULT_TOOL_ICON == "code_outlined"
        assert t.icon_for_tool("") == DEFAULT_TOOL_ICON

    def test_override_priority(self) -> None:
        t = Theme(tool_icons={"mystery_tool": "robot_outlined"})
        assert t.icon_for_tool("mystery_tool") == "robot_outlined"
        # 命中默认表的工具 override 精确名才生效
        t2 = Theme(tool_icons={"read": "folder_outlined"})
        assert t2.icon_for_tool("read") == "folder_outlined"


class TestToolTitle:
    def test_descriptor_table(self) -> None:
        t = Theme()
        assert t.title_for_tool("grep") == "Search text"
        assert t.title_for_tool("read") == "Read"
        assert t.title_for_tool("web_fetch") == "Fetch web page"

    def test_terminal_not_in_title_table(self) -> None:
        # 前代怪癖：terminal 只进图标表不进标题表 → humanize "Terminal"
        t = Theme()
        assert t.title_for_tool("terminal") == "Terminal"

    def test_humanize_fallback(self) -> None:
        t = Theme()
        assert t.title_for_tool("my-cool_tool") == "My cool tool"
        assert t.title_for_tool("") == "Tool"
        assert t.title_for_tool(None) == "Tool"  # type: ignore[arg-type]


class TestNoticeIcon:
    def test_level_mapping(self) -> None:
        t = Theme()
        assert t.icon_for_notice("warn") == "warning_outlined"
        assert t.icon_for_notice("error") == "warning_outlined"
        assert t.icon_for_notice("info") == "info_outlined"
        assert t.icon_for_notice("unknown") == "info_outlined"  # DEFAULT_NOTICE_ICON

    def test_color_mapping(self) -> None:
        t = Theme()
        assert t.color_for_notice("warn") == "orange"   # v2.0.16 警告不用灰
        assert t.color_for_notice("error") == "red"
        assert t.color_for_notice("info") == "grey"
        assert t.color_for_notice("unknown") == "grey"


class TestTextSizes:
    def test_opt_in_defaults(self) -> None:
        t = Theme()
        assert t.text_sizes is None
        assert t.answer_text_size() == ANSWER_TEXT_SIZE_FALLBACK == "normal_v2"
        assert t.aux_text_size() == AUX_TEXT_SIZE_FALLBACK == "notation"
        assert t.style_config() == {}  # 未配置零注入（v2.0.20 完全 opt-in）

    def test_token_path_when_configured(self) -> None:
        t = Theme(text_sizes=TextSizes(answer="a1", aux="b2"))
        assert t.answer_text_size() == ANSWER_TEXT_SIZE_TOKEN == "cus-ans"
        assert t.aux_text_size() == AUX_TEXT_SIZE_TOKEN == "cus-aux"
        cfg = t.style_config()
        assert cfg["style"]["text_size"] == {"cus-ans": "a1", "cus-aux": "b2"}


class TestI18n:
    def test_string_pairs_are_en_zh(self) -> None:
        t = Theme()
        en, zh = t.text("processing")
        assert en == "Processing..."
        assert zh == "处理中..."

    def test_reasoning_level_zh(self) -> None:
        t = Theme()
        assert t.reasoning_level_zh("high") == "高"
        assert t.reasoning_level_zh("custom-level") == "custom-level"  # 未知原样
