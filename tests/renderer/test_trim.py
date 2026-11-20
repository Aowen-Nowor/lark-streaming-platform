"""字节预算裁剪测试（streaming.trim_to_budget）— 03-card-schema §6 裁剪顺序."""

from __future__ import annotations

from lark_streaming.renderer import (
    PanelRow,
    RenderState,
    RowKind,
    Theme,
    measure_card_bytes,
    render_full_card,
    trim_to_budget,
)
from lark_streaming.transport.budget import CARD_BYTE_BUDGET

BUDGET = CARD_BYTE_BUDGET  # 27000（前代生产实测冻结值）


def _big_state(*, interim: int = 30, rounds: int = 20, answer_kb: int = 40) -> RenderState:
    rows: list[PanelRow] = []
    for i in range(interim):
        rows.append(PanelRow(kind=RowKind.INTERIM, text=f"interim tick {i} " + "p" * 60))
    for r in range(rounds):
        rows.append(PanelRow(kind=RowKind.THINKING, text=f"round {r} thinking " + "t" * 200,
                             round_index=r + 1, round_finalized=True, round_elapsed_ms=1000.0))
    rows.append(PanelRow(kind=RowKind.TOOL, tool_name="terminal", tool_detail="ls " * 30))
    answer = ("# 长\n\n段落 " + "字" * 800 + "\n\n") * answer_kb
    return RenderState(answer_text=answer, panel_rows=tuple(rows))


class TestTrimOrder:
    def test_within_budget_no_trim(self, theme: Theme) -> None:
        state = RenderState(answer_text="短回答")
        trimmed, report = trim_to_budget(state, theme)
        assert trimmed is state  # 未裁剪返回原对象（零拷贝语义）
        assert report.trimmed_interim == 0
        assert report.trimmed_thinking_rounds == 0
        assert report.answer_truncated is False
        assert report.measured_bytes == report.final_bytes

    def test_interim_trimmed_first(self, theme: Theme) -> None:
        state = _big_state()
        trimmed, report = trim_to_budget(state, theme)
        assert report.trimmed_interim > 0
        kinds = {r.kind for r in trimmed.panel_rows}
        # interim 全清后才动 thinking
        if report.trimmed_thinking_rounds == 0 and report.answer_truncated is False:
            assert RowKind.INTERIM not in kinds or report.trimmed_interim == 30

    def test_thinking_history_before_answer(self, theme: Theme) -> None:
        state = _big_state(answer_kb=1)  # answer 小 → 裁剪主要落在行上
        trimmed, report = trim_to_budget(state, theme)
        # thinking 历史轮整轮淘汰，当前轮保留
        if report.trimmed_thinking_rounds > 0:
            remaining = {r.round_index for r in trimmed.panel_rows if r.kind is RowKind.THINKING}
            assert max(remaining) == 20  # 当前轮（最大轮号）保留
            assert report.trimmed_thinking_rounds == len({r for r in state.panel_rows if r.kind is RowKind.THINKING}) - len(remaining)

    def test_answer_truncated_with_marker(self, theme: Theme) -> None:
        # 无面板行 → 只有 answer 可裁
        state = RenderState(answer_text="字" * 60_000)
        trimmed, report = trim_to_budget(state, theme)
        assert report.answer_truncated is True
        assert "…（已截断）" in trimmed.answer_text
        card = render_full_card(trimmed, theme)
        assert measure_card_bytes(card) <= BUDGET

    def test_final_result_within_budget(self, theme: Theme) -> None:
        state = _big_state()
        trimmed, report = trim_to_budget(state, theme)
        assert report.final_bytes <= BUDGET
        card = render_full_card(trimmed, theme)
        assert measure_card_bytes(card) <= BUDGET

    def test_input_state_not_mutated(self, theme: Theme) -> None:
        state = _big_state()
        rows_before = state.panel_rows
        answer_before = state.answer_text
        trim_to_budget(state, theme)
        assert state.panel_rows is rows_before
        assert state.answer_text == answer_before

    def test_report_counts(self, theme: Theme) -> None:
        state = _big_state(rounds=60)  # 确保超限（无 answer 压力，靠行数）
        _, report = trim_to_budget(state, theme)
        assert report.budget_bytes == BUDGET
        assert report.measured_bytes > BUDGET  # 输入确实超限
        # 观测面：三项计数齐全可读
        assert (report.trimmed_interim, report.trimmed_thinking_rounds, report.answer_truncated) != (0, 0, False) or report.final_bytes <= BUDGET

    def test_custom_budget_respected(self, theme: Theme) -> None:
        state = RenderState(answer_text="字" * 20_000)
        trimmed, report = trim_to_budget(state, theme, budget_bytes=5_000)
        assert report.budget_bytes == 5_000
        assert report.final_bytes <= 5_000


class TestFenceSafetyInTrim:
    def test_truncation_keeps_fence_balanced(self, theme: Theme) -> None:
        answer = "开头\n\n```python\n" + "code line here\n" * 2000 + "```\n\n结尾"
        state = RenderState(answer_text=answer)
        trimmed, report = trim_to_budget(state, theme)
        if report.answer_truncated:
            # 截断点回退到围栏开栏前 → 围栏标记必成对
            opens = trimmed.answer_text.count("```")
            assert opens % 2 == 0
            assert trimmed.answer_text.endswith("…（已截断）")
