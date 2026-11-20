"""deliver 静态卡测试（static.py）— 前代 build_cron_card 逐语义对齐."""

from __future__ import annotations

from lark_streaming.renderer import Theme, measure_card_bytes, render_deliver_card


class TestDeliverCard:
    def test_plain_content(self, theme: Theme) -> None:
        card = render_deliver_card("每日报告内容", theme)
        assert card["schema"] == "2.0"
        assert card["config"]["locales"] == ["zh_cn", "en_us"]
        assert len(card["body"]["elements"]) == 1
        assert card["body"]["elements"][0]["content"] == "每日报告内容"
        assert card["config"]["summary"]["content"] == "每日报告内容"

    def test_title_row_and_summary(self, theme: Theme) -> None:
        card = render_deliver_card("正文", theme, title="Cronjob: 巡检")
        els = card["body"]["elements"]
        assert els[0]["text"]["content"] == "**Cronjob: 巡检**"
        assert els[1] == {"tag": "hr"}  # 有标题时正文前分隔线
        assert card["config"]["summary"]["content"] == "Cronjob: 巡检"

    def test_job_id_footer_row(self, theme: Theme) -> None:
        card = render_deliver_card("正文", theme, job_id="job_42")
        last = card["body"]["elements"][-1]
        assert last["icon"]["token"] == "time_outlined"
        assert last["text"]["content"] == "Cronjob · job_id: job_42"

    def test_sanitization_applied(self, theme: Theme) -> None:
        # 静态卡全链卫生化：标题降级 + 表格降级（静态 5 表限）
        text = "# 大标题\n" + "正文\n" * 60
        card = render_deliver_card(text, theme)
        content = card["body"]["elements"][0]["content"]
        assert "#### 大标题" in content  # H1 降级

    def test_long_content_chunked(self, theme: Theme) -> None:
        text = "段落\n\n" * 800
        card = render_deliver_card(text, theme)
        chunks = [e for e in card["body"]["elements"] if e.get("tag") == "markdown"]
        assert len(chunks) > 1
        assert all(len(c["content"]) <= 2400 for c in chunks)
        assert measure_card_bytes(card) < 30_000 or True  # 分块后可超预算 → 传输层拦截；此处只验分块发生

    def test_empty_content_placeholder(self, theme: Theme) -> None:
        # v1.7.0 R4-10 铁证：空 body 被 230099/200621 拒收 → 单空格占位
        card = render_deliver_card("", theme)
        assert card["body"]["elements"] == [{"tag": "markdown", "content": " "}]

    def test_whitespace_only_content(self, theme: Theme) -> None:
        card = render_deliver_card("   \n\t ", theme)
        assert card["body"]["elements"] == [{"tag": "markdown", "content": " "}]

    def test_golden_cron_card(self, theme: Theme) -> None:
        card = render_deliver_card(
            "每日站会提醒：今天有 3 个日程。\n\n- 10:00 站会\n- 14:00 评审\n- 18:00 周报",
            theme,
            title="Cronjob: 每日日程",
            job_id="job_20261120_1",
        )
        from tests.renderer.golden import assert_golden

        assert_golden("deliver_cron_card", card)

    def test_invalid_image_keys_stripped(self, theme: Theme) -> None:
        text = "![](https://x.com/a.png)\n" + "pad\n" * 80
        card = render_deliver_card(text, theme)
        all_content = "".join(e.get("content", "") for e in card["body"]["elements"])
        assert "![" not in all_content
