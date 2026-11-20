"""原语元素与 200 元素上限测试（elements.py）."""

from __future__ import annotations

from lark_streaming.renderer import (
    ANSWER_ELEMENT_ID,
    FEISHU_ELEMENT_LIMIT,
    LOADING_ELEMENT_ID,
    UNIFIED_PANEL_ELEMENT_ID,
    Theme,
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
from lark_streaming.renderer.state import FooterStats, PanelRow, RowKind


class TestEscape:
    def test_escape_md_specials(self) -> None:
        assert escape_md("a*b") == "a\\*b"
        assert escape_md("a`b") == "a\\`b"
        assert escape_md("back\\slash") == "back\\\\slash"


class TestCountTags:
    def test_nested_counting(self) -> None:
        obj = {
            "tag": "div",
            "icon": {"tag": "standard_icon"},
            "text": {"tag": "plain_text"},
            "list": [{"tag": "markdown"}, {"x": {"tag": "hr"}}],
        }
        assert count_tag_objects(obj) == 5
        assert count_tag_objects({"no_tag": 1}) == 0


class TestAnswerElement:
    def test_wire_shape(self, theme: Theme) -> None:
        el = build_answer_element("hello", theme)
        assert el["tag"] == "markdown"
        assert el["content"] == "hello"
        assert el["element_id"] == ANSWER_ELEMENT_ID == "answer_content"
        assert el["text_size"] == "normal_v2"  # 默认字面值（opt-in 未启用）


class TestLoadingElement:
    def test_wire_shape(self) -> None:
        el = build_loading_element()
        assert el["element_id"] == LOADING_ELEMENT_ID == "loading_icon"
        assert el["icon"]["tag"] == "custom_icon"
        assert el["icon"]["img_key"].startswith("img_")


class TestPanelChildren:
    def test_thinking_round_grouping(self, theme: Theme) -> None:
        rows = (
            PanelRow(kind=RowKind.THINKING, text="第一段", round_index=1, round_finalized=True),
            PanelRow(kind=RowKind.THINKING, text="第二段", round_index=1, round_finalized=True),
            PanelRow(kind=RowKind.TOOL, tool_name="terminal", tool_detail="ls"),
        )
        children = build_panel_children(rows, theme, turn_state="finalized")
        # 1 轮标题 + 2 正文 + 工具行 + 工具 detail
        assert len(children) == 5
        title = children[0]
        assert title["icon"]["token"] == "robot-add_outlined"
        assert "第 1 轮" in title["text"]["content"]
        assert "green" in title["text"]["content"]  # finalized 绿

    def test_streaming_open_round_is_orange(self, theme: Theme) -> None:
        rows = (PanelRow(kind=RowKind.THINKING, text="思考中", round_index=2),)
        children = build_panel_children(rows, theme, turn_state="streaming")
        assert "orange-300" in children[0]["text"]["content"]

    def test_finalized_turn_force_green(self, theme: Theme) -> None:
        rows = (PanelRow(kind=RowKind.THINKING, text="思考", round_index=1),)
        children = build_panel_children(rows, theme, turn_state="finalized")
        assert "orange" not in children[0]["text"]["content"]
        assert "green" in children[0]["text"]["content"]

    def test_round_elapsed_label(self, theme: Theme) -> None:
        rows = (
            PanelRow(
                kind=RowKind.THINKING, text="x", round_index=1,
                round_finalized=True, round_elapsed_ms=2300.0,
            ),
        )
        children = build_panel_children(rows, theme)
        assert "2.3s" in children[0]["text"]["content"]

    def test_tool_row_pending_vs_done(self, theme: Theme) -> None:
        pending = PanelRow(kind=RowKind.TOOL, tool_name="terminal", pending=True)
        done = PanelRow(kind=RowKind.TOOL, tool_name="terminal", duration_ms=820.0, tool_detail="ps aux")
        p_children = build_panel_children((pending,), theme)
        d_children = build_panel_children((done,), theme)
        assert "orange-300" in p_children[0]["text"]["content"]
        assert len(p_children) == 1  # pending 无 detail 行
        assert "green" in d_children[0]["text"]["content"]
        assert "(820 ms)" in d_children[0]["text"]["content"]  # V1 标签含空格
        assert len(d_children) == 2  # 标题 + detail
        assert d_children[1]["text"]["content"] == "ps aux"
        assert d_children[1]["text"]["tag"] == "plain_text"  # 命令非 markdown

    def test_tool_long_duration_format(self, theme: Theme) -> None:
        row = PanelRow(kind=RowKind.TOOL, tool_name="grep", duration_ms=2300.0)
        children = build_panel_children((row,), theme)
        assert "(2.3 s)" in children[0]["text"]["content"]

    def test_notice_levels(self, theme: Theme) -> None:
        warn = PanelRow(kind=RowKind.NOTICE, text="Model fallback", level="warn")
        err = PanelRow(kind=RowKind.NOTICE, text="boom", level="error")
        w = build_panel_children((warn,), theme)
        e = build_panel_children((err,), theme)
        assert w[0]["icon"]["token"] == "warning_outlined"
        assert w[0]["text"]["text_color"] == "orange"
        assert e[0]["text"]["text_color"] == "red"

    def test_interim_row(self, theme: Theme) -> None:
        row = PanelRow(kind=RowKind.INTERIM, text="进度播报")
        children = build_panel_children((row,), theme)
        assert children[0]["icon"]["token"] == "info_outlined"
        assert children[0]["text"]["content"] == "进度播报"

    def test_empty_rows_placeholder(self, theme: Theme) -> None:
        children = build_panel_children((), theme)
        assert children == [{"tag": "markdown", "content": " "}]

    def test_output_never_empty(self, theme: Theme) -> None:
        # 空文本行也不产出空 children（占位单空格兜底）
        rows = (PanelRow(kind=RowKind.THINKING, text="", round_index=1),)
        children = build_panel_children(rows, theme)
        assert len(children) == 1  # 只有标题（空正文行被丢弃）
        assert children[0]["icon"]["token"] == "robot-add_outlined"


class TestFooter:
    def test_full_stats(self, theme: Theme) -> None:
        stats = FooterStats(
            duration_ms=150_000.0, model="gpt-x", context_used=50_000,
            context_max=200_000, reasoning_effort="high",
        )
        els = build_footer_elements(stats, Theme(reasoning_level="high"))
        assert els[0] == {"tag": "hr"}
        # 前代语义：content = 英文行，i18n_content 承载双语
        content = els[1]["content"]
        i18n = els[1]["i18n_content"]
        assert "Completed" in content
        assert "Elapsed 2m 30s" in content
        assert "gpt-x" in content
        assert "Context 50.0K/200.0K (25%)" in content
        assert "Reasoning high" in content
        assert "已完成" in i18n["zh_cn"]
        assert "耗时 2m 30s" in i18n["zh_cn"]
        assert "上下文 50.0K/200.0K (25%)" in i18n["zh_cn"]
        assert "思考 高" in i18n["zh_cn"]

    def test_reasoning_hidden_without_theme_flag(self, theme: Theme) -> None:
        stats = FooterStats(reasoning_effort="high")
        els = build_footer_elements(stats, theme)  # theme.reasoning_level 空
        assert "Reasoning" not in els[1]["content"]
        assert "思考" not in els[1]["i18n_content"]["zh_cn"]

    def test_error_red(self, theme: Theme) -> None:
        stats = FooterStats(is_error=True)
        els = build_footer_elements(stats, theme)
        assert "<font color='red'>" in els[1]["content"]
        assert "Error" in els[1]["content"]
        assert "出错" in els[1]["i18n_content"]["zh_cn"]

    def test_aborted_with_reason(self, theme: Theme) -> None:
        stats = FooterStats(is_aborted=True, abort_reason="用户中止")
        els = build_footer_elements(stats, theme)
        assert "Stopped" in els[1]["content"]
        assert "已停止" in els[1]["i18n_content"]["zh_cn"]
        # 中止原因独立橙色警示行
        assert els[2]["icon"]["token"] == "warning_outlined"
        assert els[2]["text"]["content"] == "用户中止"

    def test_job_id_row(self, theme: Theme) -> None:
        stats = FooterStats(job_id="job_123")
        els = build_footer_elements(stats, theme)
        assert els[-1]["text"]["content"] == "job_id: job_123"
        assert els[-1]["icon"]["token"] == "time_outlined"

    def test_missing_fields_skipped(self, theme: Theme) -> None:
        stats = FooterStats()  # 仅 status
        els = build_footer_elements(stats, theme)
        assert len(els) == 2
        assert els[1]["content"] == "Completed"  # content = en（前代语义）
        assert els[1]["i18n_content"]["zh_cn"] == "已完成"


class TestElementLimit:
    def _card_with_panel(self, n_rows: int) -> dict:
        rows = tuple(
            PanelRow(kind=RowKind.TOOL, tool_name=f"tool_{i}", tool_detail=f"cmd {i} " * 10)
            for i in range(n_rows)
        )
        children = build_panel_children(rows, Theme())
        return {
            "schema": "2.0",
            "body": {
                "elements": [
                    {
                        "tag": "collapsible_panel",
                        "element_id": UNIFIED_PANEL_ELEMENT_ID,
                        "elements": children,
                    },
                    build_answer_element("ans", Theme()),
                    build_loading_element(),
                ]
            },
        }

    def test_under_limit_untouched(self, theme: Theme) -> None:
        card = self._card_with_panel(5)
        before = count_tag_objects(card)
        out = enforce_element_limit(card, theme=theme)
        assert count_tag_objects(out) == before

    def test_over_limit_trimmed_with_hint(self, theme: Theme) -> None:
        card = self._card_with_panel(60)
        out = enforce_element_limit(card, theme=theme)
        total = count_tag_objects(out)
        assert total <= FEISHU_ELEMENT_LIMIT - 5
        panel = out["body"]["elements"][0]
        hint = panel["elements"][0]
        assert "已折叠" in hint["content"]

    def test_no_panel_returns_as_is(self, theme: Theme) -> None:
        card = {"schema": "2.0", "body": {"elements": [build_answer_element("x" * 500, theme)]}}
        out = enforce_element_limit(card, theme=theme)
        assert out is card

    def test_hint_count_accumulates(self, theme: Theme) -> None:
        card = self._card_with_panel(40)
        out = enforce_element_limit(card, theme=theme)
        panel = out["body"]["elements"][0]
        contents = [c.get("content", "") for c in panel["elements"]]
        hints = [c for c in contents if "已折叠" in c]
        if hints:
            # 提示行的数量语义 = 被裁行数
            assert hints[0].startswith("⚡")


class TestPanelTitle:
    def test_default_and_override(self, theme: Theme) -> None:
        el = build_panel_title_element(theme, "")
        assert el["content"] == "agent loop"
        el2 = build_panel_title_element(theme, "agent loop · 3 个工具 · 12.3s")
        assert el2["content"] == "agent loop · 3 个工具 · 12.3s"
        assert el2["i18n_content"] == {"zh_cn": el2["content"], "en_us": el2["content"]}


class TestNoticeRowPrimitive:
    def test_wire_shape(self) -> None:
        el = build_notice_row("warning_outlined", "注意", color="orange")
        assert el["tag"] == "div"
        assert el["icon"]["token"] == "warning_outlined"
        assert el["icon"]["color"] == "orange"
        assert el["text"]["text_color"] == "orange"
        assert el["text"]["text_size"] == "notation"


class TestFormatting:
    def test_format_elapsed(self) -> None:
        assert format_elapsed(12_300.0) == "12.3s"
        assert format_elapsed(150_000.0) == "2m 30s"

    def test_format_duration_label(self) -> None:
        assert format_duration_label(820.0) == "820 ms"
        assert format_duration_label(2300.0) == "2.3 s"
        assert format_duration_label(-5) == "0 ms"

    def test_compact(self) -> None:
        from lark_streaming.renderer.elements import compact_number

        assert compact_number(999) == "999"
        assert compact_number(1500) == "1.5K"
        assert compact_number(2_500_000) == "2.5M"
        assert compact_number(150_000_000) == "150M"
