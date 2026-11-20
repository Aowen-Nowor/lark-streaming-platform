"""clarify 三态渲染测试（clarify.py）— wire 红线与回调命名空间."""

from __future__ import annotations

import pytest

from lark_streaming.renderer import (
    CLARIFY_CALLBACK_KEY,
    ClarifyForm,
    Theme,
    clarify_callback_value,
    count_tag_objects,
    render_clarify_answered,
    render_clarify_confirmed,
    render_clarify_expired,
    render_clarify_pending,
    render_clarify_unauthorized,
)

FORM = ClarifyForm(
    kind="select", question="选择部署环境？",
    options=("production 生产", "staging 预发", "development 开发"),
)
INPUT_FORM = ClarifyForm(kind="input", question="补充说明？")


class TestCallbackNamespace:
    def test_key_is_protocol_namespaced(self) -> None:
        # ADR D8：前代 hermes_clarify_action 不得泄漏进协议命名空间
        assert CLARIFY_CALLBACK_KEY == "lsp_clarify_action"
        assert "hermes" not in CLARIFY_CALLBACK_KEY

    def test_value_shape(self) -> None:
        v = clarify_callback_value("select", "c_123")
        assert v == {"lsp_clarify_action": "select", "clarify_id": "c_123"}


class TestPendingCard:
    def test_wire_shape(self, theme: Theme) -> None:
        card = render_clarify_pending(FORM, theme, clarify_id="c_1")
        assert card["schema"] == "2.0"
        assert card["config"]["streaming_mode"] is False
        assert card["config"]["locales"] == ["zh_cn", "en_us"]

    def test_question_bold_with_icon(self, theme: Theme) -> None:
        card = render_clarify_pending(FORM, theme)
        q = card["body"]["elements"][0]
        assert q["icon"]["token"] == "info_outlined"
        assert "**选择部署环境？**" in q["text"]["content"]

    def test_options_listed_with_labels(self, theme: Theme) -> None:
        card = render_clarify_pending(FORM, theme)
        options_md = card["body"]["elements"][1]["content"]
        assert "A. production 生产" in options_md
        assert "C. development 开发" in options_md

    def test_select_static_dropdown(self, theme: Theme) -> None:
        card = render_clarify_pending(FORM, theme, clarify_id="c_9")
        select = card["body"]["elements"][2]
        assert select["tag"] == "select_static"
        assert select["element_id"] == "clarify_select"
        assert len(select["options"]) == 3
        assert select["options"][0] == {
            "text": {"tag": "plain_text", "content": "A. production 生产"},
            "value": "0",  # value = 序号字符串（引擎持 form 原值）
        }
        cb = select["behaviors"][0]["value"]
        assert cb[CLARIFY_CALLBACK_KEY] == "select"
        assert cb["clarify_id"] == "c_9"
        # select_static 占位是 plain_text（wire 红线）
        assert select["placeholder"]["tag"] == "plain_text"

    def test_input_always_rendered(self, theme: Theme) -> None:
        card = render_clarify_pending(INPUT_FORM, theme, clarify_id="c_2")
        tags = [e["tag"] for e in card["body"]["elements"]]
        # input 表单：问题行 + 输入框（无选项列表与下拉）
        assert tags == ["div", "input"]
        inp = card["body"]["elements"][1]
        assert inp["max_length"] == 500
        assert inp["behaviors"][0]["value"][CLARIFY_CALLBACK_KEY] == "input_submit"

    def test_long_option_truncated(self, theme: Theme) -> None:
        form = ClarifyForm(kind="select", question="q", options=("长" * 100,))
        card = render_clarify_pending(form, theme)
        content = card["body"]["elements"][1]["content"]
        assert "…" in content
        assert len(content) < 120

    def test_golden_pending_select(self, theme: Theme) -> None:
        from tests.renderer.golden import assert_golden

        assert_golden("clarify_pending_select",
                      render_clarify_pending(FORM, theme, clarify_id="c_1"))


class TestAnsweredCard:
    def test_button_is_top_level(self, theme: Theme) -> None:
        # 红线：「action」容器在 Card 2.0 已废弃——button 必须顶层元素
        card = render_clarify_answered(FORM, theme, selected="production 生产")
        tags = [e["tag"] for e in card["body"]["elements"]]
        assert "action" not in tags
        assert "button" in tags

    def test_retry_callback(self, theme: Theme) -> None:
        card = render_clarify_answered(FORM, theme, selected="staging", clarify_id="c_3")
        btn = card["body"]["elements"][-1]
        assert btn["type"] == "primary"
        assert btn["behaviors"][0]["value"][CLARIFY_CALLBACK_KEY] == "retry_submit"
        assert btn["behaviors"][0]["value"]["clarify_id"] == "c_3"

    def test_soft_lock_icons(self, theme: Theme) -> None:
        card = render_clarify_answered(FORM, theme, selected="staging")
        assert card["body"]["elements"][0]["icon"]["token"] == "lock_outlined"
        # 前代语义：content = en 行，i18n_content 承载双语
        sub = card["body"]["elements"][2]["text"]
        assert "Submitted" in sub["content"]
        assert "已提交" in sub["i18n_content"]["zh_cn"]

    def test_golden_answered(self, theme: Theme) -> None:
        from tests.renderer.golden import assert_golden

        assert_golden("clarify_answered",
                      render_clarify_answered(FORM, theme, selected="production 生产",
                                              clarify_id="c_1"))


class TestConfirmedCard:
    def test_hard_lock_green(self, theme: Theme) -> None:
        card = render_clarify_confirmed(FORM, theme, selected="production 生产")
        assert card["body"]["elements"][0]["icon"]["token"] == "resolve_filled"
        assert "Confirmed" in card["body"]["elements"][-1]["text"]["content"]
        assert "已确认" in card["body"]["elements"][-1]["text"]["i18n_content"]["zh_cn"]
        tags = [e["tag"] for e in card["body"]["elements"]]
        assert "button" not in tags  # 硬锁无可交互控件


class TestUnauthorizedCard:
    def test_fail_closed(self, theme: Theme) -> None:
        card = render_clarify_unauthorized(FORM, theme)
        assert card["body"]["elements"][0]["icon"]["token"] == "warning_outlined"
        assert card["body"]["elements"][0]["icon"]["color"] == "red"
        # 不渲染表单值与可交互控件
        flat = str(card)
        assert "production" not in flat
        assert "select_static" not in flat
        assert "input" not in [e["tag"] for e in card["body"]["elements"]]


class TestExpiredCard:
    def test_expired_notice(self, theme: Theme) -> None:
        card = render_clarify_expired(theme)
        assert "expired" in card["body"]["elements"][0]["text"]["content"]
        assert "已过期" in card["body"]["elements"][0]["text"]["i18n_content"]["zh_cn"]


class TestElementBudget:
    def test_forms_within_element_limit(self, theme: Theme) -> None:
        big = ClarifyForm(kind="select", question="q", options=tuple(f"opt {i}" for i in range(30)))
        for card in (
            render_clarify_pending(big, theme),
            render_clarify_answered(big, theme, selected="x"),
            render_clarify_confirmed(big, theme, selected="x"),
            render_clarify_unauthorized(big, theme),
        ):
            assert count_tag_objects(card) <= 200


class TestFormValidationPropagates:
    def test_bad_form_raises(self, theme: Theme) -> None:
        with pytest.raises(ValueError):
            render_clarify_pending(ClarifyForm(kind="bad", question="q"), theme)
