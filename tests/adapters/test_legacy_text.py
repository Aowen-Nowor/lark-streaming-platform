"""legacy 文本转换器单测 — 前代真实格式回归（04-knowledge-freeze §4 legacy 基线）.

样本移植自旧仓库 tests/e2e/test_v206_real_formats.py（v2.0.6 真实网关
帧格式回归）+ classify 通道语义（v2.0.8/15/34 事故防线）。转换语义与
前代分类器逐点一致（05-adapters §7 前代迁移承诺）。
"""

from __future__ import annotations

import pytest

from lark_streaming.adapters import _legacy_text as lt
from tests.adapters.conftest import (  # noqa: F401
    REAL_BARE_BLOCK,
    REAL_HEAD,
    REAL_PENDING,
    REAL_PREVIEW,
    REAL_THINK,
    REAL_TERMINAL_BLOCK,
    REAL_TERMINAL_BLOCK_TRUNC,
    REAL_VERB_EN_RUN,
    REAL_VERB_EN_SEARCH,
    REAL_VERB_ZH_NO_PREVIEW,
    REAL_VERB_ZH_READ,
    REAL_VERB_ZH_SEARCH,
)


class TestProgline:
    """进度行解析（前代 progline.py 语义对齐）."""

    def test_strip_dedup_suffix(self):
        assert lt.strip_dedup_suffix("💻 terminal... (×3)") == "💻 terminal..."
        assert lt.strip_dedup_suffix("💬 思考 (×1)") == "💬 思考"
        assert lt.strip_dedup_suffix("普通文本") == "普通文本"

    def test_parse_terminal_block(self):
        kind, tool, clean = lt.parse_progress_line(REAL_TERMINAL_BLOCK)
        assert (kind, tool) == ("tool", "terminal")
        assert "```" in clean  # 保留原文供渲染层取命令

    def test_parse_bare_block_terminal(self):
        # v2.0.22：裸块归属 terminal（仅 terminal 产围栏块；连续调用省略 header）
        kind, tool, clean = lt.parse_progress_line(REAL_BARE_BLOCK)
        assert (kind, tool) == ("tool", "terminal")
        assert "```" in clean

    def test_parse_verb_lines_zh(self):
        assert lt.parse_progress_line(REAL_VERB_ZH_SEARCH)[:2] == ("tool", "web_search")
        assert lt.parse_progress_line(REAL_VERB_ZH_READ)[:2] == ("tool", "read_file")
        assert lt.parse_progress_line(REAL_VERB_ZH_NO_PREVIEW)[:2] == ("tool", "skills_list")

    def test_parse_verb_lines_en(self):
        assert lt.parse_progress_line(REAL_VERB_EN_RUN)[:2] == ("tool", "terminal")
        assert lt.parse_progress_line(REAL_VERB_EN_SEARCH)[:2] == ("tool", "web_search")

    def test_parse_bare_head_pending_preview(self):
        assert lt.parse_progress_line(REAL_HEAD)[:2] == ("tool", "terminal")
        assert lt.parse_progress_line(REAL_PENDING)[:2] == ("tool", "terminal")
        assert lt.parse_progress_line(REAL_PREVIEW)[:2] == ("tool", "terminal")

    def test_parse_thinking_and_unknown(self):
        assert lt.parse_progress_line(REAL_THINK)[0] == "thinking"
        assert lt.parse_progress_line(REAL_THINK)[2] == "正在检查系统状态，先收集内核与运行时长…"
        assert lt.parse_progress_line("普通回答文本")[0] == "unknown"

    def test_verb_prefix_without_connector_not_verb(self):
        # "编辑器 ..." 是普通文本不是 patch 动词行
        assert lt.match_verb_line("✍️ 编辑器配置文件检查") is None

    def test_preview_from_block(self):
        assert lt.preview_text(REAL_TERMINAL_BLOCK) == \
            'echo "=== 时间 ==="; date "+%Y-%m-%d %H:%M:%S"'
        assert lt.preview_text(REAL_TERMINAL_BLOCK_TRUNC) == "very_long_command --flag value"
        assert lt.preview_text(REAL_BARE_BLOCK) == "second_command --now"
        assert lt.preview_text(REAL_VERB_ZH_SEARCH) == "feishu cardkit 流式"
        assert lt.preview_text(REAL_VERB_ZH_READ) == "/tmp/status.py"
        assert lt.preview_text(REAL_VERB_ZH_NO_PREVIEW) == ""
        assert lt.preview_text(REAL_PENDING) == ""

    def test_preview_quoted_pair_strip(self):
        # 只剥成对包裹引号；命令自身的尾部引号不受影响（date 格式参数）
        assert lt.preview_text('💻 terminal："ls -la"') == "ls -la"
        assert lt.preview_text('💻 terminal:date "+%H:%M:%S"') == 'date "+%H:%M:%S"'

    def test_never_raises_on_garbage(self):
        # 设计红线：永不抛异常（解析失败 → unknown/原文）
        for garbage in (None, "", "   ", "```", "💬", "💻", "\n\n\n", "emoji🚀行"*50):
            lt.parse_progress_line(garbage)  # type: ignore[arg-type]
            lt.preview_text(garbage)  # type: ignore[arg-type]
            lt.match_verb_line(garbage)  # type: ignore[arg-type]
            lt.strip_dedup_suffix(garbage)  # type: ignore[arg-type]


class TestSplitSegments:
    """accumulate 快照逻辑分段（前代 v2.0.6/8 语义对齐）."""

    def test_block_stays_with_header(self):
        snap = f"{REAL_THINK}\n{REAL_TERMINAL_BLOCK}\n{REAL_VERB_ZH_READ}"
        segs = lt.split_progress_segments(snap)
        assert len(segs) == 3
        assert segs[0] == REAL_THINK
        assert segs[1] == REAL_TERMINAL_BLOCK  # 块与头一体，未拆散
        assert segs[2] == REAL_VERB_ZH_READ

    def test_bare_block_standalone(self):
        snap = f"{REAL_TERMINAL_BLOCK}\n{REAL_BARE_BLOCK}"
        segs = lt.split_progress_segments(snap)
        assert segs == [REAL_TERMINAL_BLOCK, REAL_BARE_BLOCK]

    def test_multiline_thinking_adsorbed(self):
        # v2.0.8：💬 行与其后的非 emoji 非围栏连续行吸附为一个逻辑段
        snap = "💬 草稿要点\n- 第一条\n- 第二条\n💻 terminal..."
        segs = lt.split_progress_segments(snap)
        assert len(segs) == 2
        assert segs[0] == "💬 草稿要点\n- 第一条\n- 第二条"
        assert segs[1] == "💻 terminal..."

    def test_thinking_growth_same_block(self):
        # 同一快照内 💬 行连续 → 合并进同段（思考增长）
        snap = "💬 第一轮起点\n💬 第一轮起点·增长"
        segs = lt.split_progress_segments(snap)
        assert segs == ["💬 第一轮起点\n💬 第一轮起点·增长"]

    def test_empty_lines_dropped_and_unclosed_fence(self):
        segs = lt.split_progress_segments("💻 terminal\n```\nls -la")
        assert segs == ["💻 terminal\n```\nls -la"]  # 未闭合围栏到末尾整体成段
        assert lt.split_progress_segments("") == []
        assert lt.split_progress_segments("   \n  \n") == []


class TestProgressHeuristics:
    """300/600 双限与通知轨（前代 v2.0.6/15 语义对齐）."""

    def test_thinking_always_progress(self):
        assert lt.is_progress_text("💬 " + "长" * 400)

    def test_fence_limit_600(self):
        assert lt.is_progress_text("```" + "x" * 590)
        assert not lt.is_progress_text("```" + "x" * 610)

    def test_platform_notice_exempt_300(self):
        # ⚠️ Subagent 固定前缀豁免 300 上限（实测最长 ~359；v2.0.15）
        long_line = '⚠️ Subagent failed — "goal" after 5m: ' + "r" * 330 + ". Details: /agents"
        assert len(long_line) > 300
        assert lt.is_progress_text(long_line)

    def test_emoji_limit_300(self):
        assert lt.is_progress_text("🔍 " + "x" * 280)
        assert not lt.is_progress_text("🔍 " + "x" * 310)

    def test_plain_long_text_not_progress(self):
        assert not lt.is_progress_text("普通文本" * 100)


class TestNoticeList:
    """后台非对话通知清单（前代 v2.0.8/34 跑马灯根因防线）."""

    @pytest.mark.parametrize("text", [
        "💾 Self-improvement review: 12 findings",
        "💾 自我改进回顾：12 项发现",
        "⏳ Working — 12 min elapsed",
        "⏳ 正在处理——12 分钟",
        "✅ Hermes update finished successfully",
        "❌ Hermes update timed out",
        "♻️ Gateway restarted successfully",
        "⚠️ Hermes is shutting down — your current task will be interrupted.",
        "⚠️ Hermes 正在关闭——你当前的任务将被中断。",
        "⚠️ Scheduled job 'nightly' was cut short because Hermes is shutting down; …",
        "✅ 后台任务完成\n提示词：xxx",
        "✅ Background task complete\nPrompt: xxx",
    ])
    def test_notice_texts(self, text):
        assert lt.is_notice_text(text)

    @pytest.mark.parametrize("text", [
        "💬 普通思考",
        "💻 terminal...",
        "你好，世界",
        "✅ 后台任务完成但格式不同的正文",
    ])
    def test_non_notice_texts(self, text):
        assert not lt.is_notice_text(text)


class TestClassifySend:
    """metadata 契约通道分类（前代 classify_send 语义对齐）."""

    def test_expect_edits_stream(self):
        r = lt.classify_send("答案正文", {"expect_edits": True})
        assert (r.channel, r.kind) == (lt.CH_STREAM, "answer")

    def test_interim_progress_lines(self):
        r = lt.classify_send(REAL_THINK, {"_interim_send": True})
        assert (r.channel, r.kind) == (lt.CH_PROGRESS, "thinking")
        r = lt.classify_send(REAL_TERMINAL_BLOCK, {"_interim_send": True})
        assert (r.channel, r.kind, r.tool_name) == (lt.CH_PROGRESS, "tool", "terminal")

    def test_interim_notice_never_progress(self):
        # 跑马灯根因防线：通知清单文本永不进 progress 通道
        r = lt.classify_send("⚠️ Hermes is shutting down — …", {"_interim_send": True})
        assert (r.channel, r.kind) == (lt.CH_INTERIM, "advisory")

    def test_interim_conversational_commentary(self):
        r = lt.classify_send("我先看一下状态", {"_interim_send": True})
        assert (r.channel, r.kind) == (lt.CH_INTERIM, "advisory")
        assert r.meta.get("conversational_commentary") is True

    def test_notify_final(self):
        r = lt.classify_send("最终答案", {"notify": True})
        assert (r.channel, r.kind) == (lt.CH_FINAL, "final")

    def test_plain_no_marker(self):
        r = lt.classify_send("cron 输出", None)
        assert (r.channel, r.kind) == (lt.CH_PLAIN, "plain")

    def test_background_result_plain(self):
        r = lt.classify_send("✅ 后台任务完成\n提示词：x", {"_interim_send": True})
        assert (r.channel, r.kind) == (lt.CH_PLAIN, "background_result")

    def test_internal_meta_stripped(self):
        r = lt.classify_send("答案", {"expect_edits": True, "_interim_send": False})
        assert "_interim_send" not in r.meta

    def test_never_raises(self):
        # 分类器永不抛异常：异常降级 CH_PLAIN（内容不丢）
        r = lt.classify_send("anything", {"expect_edits": "not-bool"})
        assert r.channel in (lt.CH_STREAM, lt.CH_PLAIN)


class TestClassifyEdit:
    def test_edit_stream(self):
        r = lt.classify_edit("更新正文", finalize=False)
        assert (r.channel, r.kind) == (lt.CH_STREAM, "answer")

    def test_edit_final(self):
        r = lt.classify_edit("最终正文", finalize=True)
        assert (r.channel, r.kind) == (lt.CH_FINAL, "final")
