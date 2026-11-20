"""渲染输入状态模型行为测试（state.py）."""

from __future__ import annotations

import dataclasses

import pytest

from lark_streaming.renderer import ClarifyForm, FooterStats, PanelRow, RenderState, RowKind


class TestPanelRow:
    def test_tool_end_flips_pending_and_backfills(self) -> None:
        row = PanelRow(kind=RowKind.TOOL, tool_name="terminal", pending=True)
        done = row.with_tool_end(duration_ms=2300.0, detail="ls -la")
        assert done.pending is False
        assert done.duration_ms == 2300.0
        assert done.tool_detail == "ls -la"
        # frozen：原行不可变
        assert row.pending is True

    def test_tool_end_keeps_duration_when_absent(self) -> None:
        row = PanelRow(kind=RowKind.TOOL, tool_name="read", pending=True, duration_ms=100.0)
        done = row.with_tool_end()  # duration 未供值 → 保留原值
        assert done.duration_ms == 100.0
        assert done.pending is False

    def test_thinking_row_defaults(self) -> None:
        row = PanelRow(kind=RowKind.THINKING, text="分析中", round_index=2)
        assert row.round_finalized is False
        assert row.round_elapsed_ms is None
        assert row.level == "info"


class TestRenderState:
    def test_frozen_dataclass(self) -> None:
        st = RenderState()
        with pytest.raises(dataclasses.FrozenInstanceError):
            st.answer_text = "x"  # type: ignore[misc]

    def test_with_answer_replaces(self) -> None:
        st = RenderState(answer_text="a").with_answer("ab")
        assert st.answer_text == "ab"

    def test_with_panel_rows_normalizes_to_tuple(self) -> None:
        row = PanelRow(kind=RowKind.TOOL, tool_name="grep")
        st = RenderState().with_panel_rows([row])
        assert st.panel_rows == (row,)

    def test_has_panel(self) -> None:
        assert RenderState().has_panel() is False
        row = PanelRow(kind=RowKind.INTERIM, text="tick")
        assert RenderState(panel_rows=(row,)).has_panel() is True

    def test_with_footer(self) -> None:
        footer = FooterStats(duration_ms=1200.0)
        st = RenderState().with_footer(footer)
        assert st.footer is footer
        assert RenderState().with_footer(None).footer is None


class TestFooterStats:
    def test_defaults_skip_rendering(self) -> None:
        s = FooterStats()
        assert s.duration_ms == 0.0
        assert s.model == ""
        assert s.is_error is False
        assert s.is_aborted is False


class TestClarifyForm:
    def test_kind_validation(self) -> None:
        with pytest.raises(ValueError, match="unsupported kind"):
            ClarifyForm(kind="radio", question="q")
        with pytest.raises(ValueError, match="requires options"):
            ClarifyForm(kind="select", question="q")
        ClarifyForm(kind="input", question="q")  # input 无需 options
        ClarifyForm(kind="multi_select", question="q", options=("a", "b"))

    def test_options_tuple_normalized(self) -> None:
        f = ClarifyForm(kind="select", question="q", options=["a", "b"])
        assert f.options == ("a", "b")
