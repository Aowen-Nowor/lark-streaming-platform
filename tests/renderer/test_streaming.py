"""流式回合卡三产物测试（streaming.py）— 含 Golden 快照（03 §8）."""

from __future__ import annotations

from lark_streaming.renderer import (
    ANSWER_ELEMENT_ID,
    UNIFIED_PANEL_ELEMENT_ID,
    PanelRow,
    RenderState,
    RowKind,
    Theme,
    build_panel_children,
    final_answer_content,
    footer_batch_actions,
    footer_summary,
    measure_card_bytes,
    panel_reseal_actions,
    render_full_card,
    render_initial_card,
)
from lark_streaming.renderer.state import FooterStats
from lark_streaming.renderer.streaming import _assemble_streaming_card  # noqa: PLC2701

from tests.renderer.golden import assert_golden


def _sample_state(**overrides) -> RenderState:
    """标准回合快照：两轮思考 + 两工具 + interim + notice + answer."""
    rows = (
        PanelRow(kind=RowKind.THINKING, text="先看目录结构", round_index=1,
                 round_finalized=True, round_elapsed_ms=1200.0),
        PanelRow(kind=RowKind.TOOL, tool_name="terminal", tool_detail="ls -la",
                 duration_ms=820.0),
        PanelRow(kind=RowKind.THINKING, text="分析 **输出** 中的模式", round_index=2,
                 round_finalized=False, round_elapsed_ms=None),
        PanelRow(kind=RowKind.TOOL, tool_name="read", pending=True),
        PanelRow(kind=RowKind.INTERIM, text="已扫描 120 个文件"),
        PanelRow(kind=RowKind.NOTICE, text="Model fallback", level="warn"),
    )
    kwargs = dict(
        answer_text="## 结论\n\n这是 **流式** 回答正文。",
        panel_rows=rows,
        panel_title="agent loop · 2 个工具 · 12.3s",
    )
    kwargs.update(overrides)
    return RenderState(**kwargs)


class TestInitialCard:
    def test_wire_skeleton(self, theme: Theme) -> None:
        card = render_initial_card(RenderState(), theme)
        assert card["schema"] == "2.0"
        assert card["config"]["streaming_mode"] is True
        assert card["config"]["streaming_config"]["print_strategy"] == "delay"
        assert card["config"]["streaming_config"]["print_frequency_ms"] == {"default": 70}
        assert card["config"]["locales"] == ["zh_cn", "en_us"]
        assert card["config"]["summary"]["content"] == "Processing..."
        assert "style" not in card["config"]  # opt-in 未启用零注入
        # 懒面板：空 rows 无面板元素
        tags = [el["tag"] for el in card["body"]["elements"]]
        assert "collapsible_panel" not in tags
        assert tags == ["markdown", "div"]  # answer 占位 + loading spinner
        assert card["body"]["elements"][0]["element_id"] == ANSWER_ELEMENT_ID
        assert card["body"]["elements"][1]["element_id"] == "loading_icon"

    def test_pending_rows_included_at_create(self, theme: Theme) -> None:
        rows = (PanelRow(kind=RowKind.TOOL, tool_name="grep", pending=True),)
        card = render_initial_card(RenderState(panel_rows=rows), theme)
        panel = card["body"]["elements"][0]
        assert panel["element_id"] == UNIFIED_PANEL_ELEMENT_ID
        assert "Search text" in panel["elements"][0]["text"]["content"]

    def test_answer_seeded_when_buffered(self, theme: Theme) -> None:
        card = render_initial_card(RenderState(answer_text="开场白"), theme)
        assert card["body"]["elements"][0]["content"] == "开场白"


class TestFullCard:
    def test_answer_backfilled(self, theme: Theme) -> None:
        state = _sample_state()
        card = render_full_card(state, theme)
        panel = card["body"]["elements"][0]
        answer = card["body"]["elements"][1]
        assert answer["element_id"] == ANSWER_ELEMENT_ID
        assert answer["content"] == state.answer_text  # 防全量重置为空
        assert panel["header"]["title"]["content"] == state.panel_title

    def test_golden_streaming_card(self, theme: Theme) -> None:
        card = render_full_card(_sample_state(), theme)
        assert_golden("streaming_full_card", card)

    def test_golden_initial_empty(self, theme: Theme) -> None:
        assert_golden("streaming_initial_empty", render_initial_card(RenderState(), theme))

    def test_golden_finalized_children_in_full_card(self, theme: Theme) -> None:
        # 全量卡在流式期渲染（turn_state=streaming）：末轮未封口 = 橙
        card = render_full_card(_sample_state(), theme)
        panel = card["body"]["elements"][0]
        contents = [c.get("text", {}).get("content", "") for c in panel["elements"]]
        assert any("orange-300" in c for c in contents)
        assert_golden("panel_children_two_rounds",
                      build_panel_children(_sample_state().panel_rows, theme,
                                           turn_state="finalized"))

    def test_text_sizes_injected_when_opt_in(self, sized_theme: Theme) -> None:
        card = render_full_card(RenderState(answer_text="x"), sized_theme)
        assert card["config"]["style"]["text_size"] == {
            "cus-ans": "cus-ans-value", "cus-aux": "cus-aux-value",
        }
        assert card["body"]["elements"][0]["text_size"] == "cus-ans"


class TestSealingProducts:
    def test_final_answer_verbatim(self, theme: Theme) -> None:
        state = RenderState(answer_text="# 标题\n\n**原文** 直发")
        assert final_answer_content(state) == state.answer_text  # 原文不卫生化

    def test_footer_actions_with_stats(self, theme: Theme) -> None:
        state = RenderState(
            answer_text="done",
            footer=FooterStats(duration_ms=1200.0, model="m-1"),
        )
        actions = footer_batch_actions(state, theme)
        assert len(actions) == 2
        add = actions[0]
        assert add["action"] == "add_elements"
        assert add["params"]["target_element_id"] == "loading_icon"
        assert add["params"]["type"] == "insert_before"
        assert add["params"]["elements"][0] == {"tag": "hr"}
        assert add["params"]["elements"][1]["content"] == "Completed · Elapsed 1.2s · m-1"
        assert "已完成" in add["params"]["elements"][1]["i18n_content"]["zh_cn"]
        assert actions[1] == {
            "action": "delete_elements",
            "params": {"element_ids": ["loading_icon"]},
        }

    def test_footer_actions_empty_footer_still_deletes_spinner(self, theme: Theme) -> None:
        actions = footer_batch_actions(RenderState(answer_text="x"), theme)
        assert actions == [{"action": "delete_elements", "params": {"element_ids": ["loading_icon"]}}]

    def test_golden_footer_actions(self, theme: Theme) -> None:
        state = RenderState(
            answer_text="done",
            footer=FooterStats(duration_ms=65_000.0, model="glm-5",
                               context_used=120_000, context_max=256_000,
                               reasoning_effort="high"),
        )
        assert_golden("footer_batch_actions", footer_batch_actions(
            state, Theme(reasoning_level="high")))

    def test_reseal_skipped_when_no_visual_delta(self, theme: Theme) -> None:
        # 全部轮次已封口 → 流式/定稿渲染一致 → None（API 经济性）
        rows = (PanelRow(kind=RowKind.THINKING, text="x", round_index=1, round_finalized=True),)
        state = RenderState(panel_rows=rows)
        assert panel_reseal_actions(state, theme) is None

    def test_reseal_built_when_open_round(self, theme: Theme) -> None:
        rows = (PanelRow(kind=RowKind.THINKING, text="x", round_index=1),)  # 未封口
        state = RenderState(panel_rows=rows)
        actions = panel_reseal_actions(state, theme)
        assert actions is not None
        assert actions[0]["action"] == "delete_elements"
        assert actions[0]["params"]["element_ids"] == [UNIFIED_PANEL_ELEMENT_ID]
        add = actions[1]
        assert add["action"] == "add_elements"
        assert add["params"]["target_element_id"] == ANSWER_ELEMENT_ID
        rebuilt = add["params"]["elements"][0]
        assert rebuilt["element_id"] == UNIFIED_PANEL_ELEMENT_ID
        # 重建面板里轮次已绿
        assert "green" in rebuilt["elements"][0]["text"]["content"]

    def test_reseal_none_without_rows(self, theme: Theme) -> None:
        assert panel_reseal_actions(RenderState(), theme) is None

    def test_summary_of(self, theme: Theme) -> None:
        assert footer_summary(RenderState(answer_text="第一行\n第二行"), theme) == "第一行"
        assert footer_summary(RenderState(), theme) == "已完成"
        long = "x" * 200
        assert len(footer_summary(RenderState(answer_text=long), theme)) == 120


class TestPureFunctionContract:
    def test_same_input_same_output(self, theme: Theme) -> None:
        state = _sample_state()
        a = render_full_card(state, theme)
        b = render_full_card(state, theme)
        assert a == b

    def test_input_not_mutated(self, theme: Theme) -> None:
        state = _sample_state()
        rows_before = state.panel_rows
        render_full_card(state, theme)
        assert state.panel_rows is rows_before
        assert state.answer_text == "## 结论\n\n这是 **流式** 回答正文。"

    def test_bytes_measured_as_upload_shape(self, theme: Theme) -> None:
        card = render_full_card(RenderState(answer_text="abc"), theme)
        # 与传输层 budget.measure_card_bytes 同口径
        from lark_streaming.transport.budget import measure_card_bytes as t_measure

        assert measure_card_bytes(card) == t_measure(card)


class TestTurnStateStability:
    def test_assemble_finalized_equivalent_to_children_swap(self, theme: Theme) -> None:
        """流式/定稿渲染差异只存在于面板子元素（收口重建的等值跳过依据）."""
        state = _sample_state()
        streaming = _assemble_streaming_card(state, theme, turn_state="streaming")
        finalized = _assemble_streaming_card(state, theme, turn_state="finalized")
        # 面板不同（橙→绿）
        assert streaming["body"]["elements"][0] != finalized["body"]["elements"][0]
        # 其余骨架逐字节一致
        assert streaming["schema"] == finalized["schema"]
        assert streaming["config"] == finalized["config"]
