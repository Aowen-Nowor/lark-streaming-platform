"""markdown 卫生化与缓存测试（md.py）— 前代 v2.0.21 审计语义逐条对齐."""

from __future__ import annotations

from lark_streaming.renderer import MarkdownCache, downgrade_tables, sanitize_markdown, split_long_text


class TestSanitizeMarkdown:
    def test_short_text_untouched(self) -> None:
        assert sanitize_markdown("hello") == "hello"

    def test_heading_demotion(self) -> None:
        long = "x" * 100 + "\n# Title\n## Sub"
        out = sanitize_markdown(long)
        assert "#### Title" in out
        assert "##### Sub" in out

    def test_fence_state_machine_protects_unfenced_code(self) -> None:
        # v2.0.21 审计 R5：未闭合围栏内的 # 行不降级（逐行状态机）
        text = (
            "intro\n"
            "```python\n"
            "# comment inside fence\n"
            "x = 1\n"
        )
        out = sanitize_markdown(text)
        assert "# comment inside fence" in out
        assert "#### comment" not in out

    def test_closed_fence_content_untouched(self) -> None:
        text = "a\n```bash\n# cmd\n```\nb " + "pad" * 40
        out = sanitize_markdown(text)
        assert "# cmd" in out

    def test_multiline_newlines_compressed(self) -> None:
        text = "a\n\n\n\n\nb " + "x" * 100
        out = sanitize_markdown(text)
        assert "\n\n\n" not in out

    def test_invalid_image_keys_stripped(self) -> None:
        text = "![](https://example.com/a.png) " + "pad " * 50
        out = sanitize_markdown(text)
        assert "![" not in out

    def test_valid_img_keys_kept(self) -> None:
        text = "![alt](img_v3_abc) " + "pad " * 50
        out = sanitize_markdown(text)
        assert "![alt](img_v3_abc)" in out

    def test_never_raises(self) -> None:
        # 装饰性处理永不抛（异常原文返回）
        assert isinstance(sanitize_markdown("```" * 10), str)


class TestSplitLongText:
    def test_short_passthrough(self) -> None:
        assert split_long_text("abc") == ["abc"]

    def test_respects_limit(self) -> None:
        text = "line\n" * 2000
        chunks = split_long_text(text, limit=500)
        assert all(len(c) <= 500 for c in chunks)
        assert "".join(chunks).replace("\n", "") == text.replace("\n", "")

    def test_fence_aware_cut(self) -> None:
        # v2.0.21 审计 R6：候选切点必须围栏闭合——正常场景每块围栏成对
        fence = "```py\n" + "code\n" * 5 + "```\n"      # ~45 字节小围栏
        filler = "para text line\n" * 30                # 围栏外段落
        text = (fence + filler) * 8
        chunks = split_long_text(text, limit=400)
        for c in chunks:
            starts = [ln for ln in c.split("\n") if ln.strip().startswith("```")]
            assert len(starts) % 2 == 0, "unbalanced fence in chunk (normal case)"

    def test_single_fence_over_half_limit_hard_cut(self) -> None:
        # 前代已知限制（v2.0.21 审计 R6 docstring）：单个未闭合围栏长度
        # > limit/2 时三候选全部落在围栏内 → 硬切（与原行为一致不再恶化）
        fence = "```py\n" + "code line\n" * 100 + "```\n"   # ~800 字节 > limit/2
        text = fence * 10
        chunks = split_long_text(text, limit=400)
        assert len(chunks) > 1  # 分块发生（块内围栏可能不成对——接受）


class TestDowngradeTables:
    def test_within_limit_untouched(self) -> None:
        text = "| a | b |\n|---|---|\n| 1 | 2 |"
        assert downgrade_tables(text, limit=5) == text

    def test_over_limit_downgraded(self) -> None:
        table = "| a | b |\n|---|---|\n| 1 | 2 |\n\n"
        text = table * 7
        out = downgrade_tables(text, limit=5)
        # 超限表被 ``` 包裹
        assert out.count("```") >= 2

    def test_no_pipes_fastpath(self) -> None:
        assert downgrade_tables("plain text", limit=5) == "plain text"


class TestMarkdownCache:
    def test_answer_single_slot(self) -> None:
        cache = MarkdownCache()
        r1 = cache.sanitize_answer("x" * 200 + "\n# T")
        assert cache.sanitize_answer("x" * 200 + "\n# T") is r1  # 精确串命中
        r2 = cache.sanitize_answer("y" * 200)
        assert r2 == "y" * 200
        # 单槽被替换后旧键不再命中（对象身份变化）
        assert cache.sanitize_answer("x" * 200 + "\n# T") is not r1

    def test_line_cache_lru(self) -> None:
        cache = MarkdownCache()
        line = "padded line " * 20
        r = cache.sanitize_line(line)
        assert cache.sanitize_line(line) is r
        # 灌满触发淘汰不抛
        for i in range(600):
            cache.sanitize_line(f"line-{i} " + "pad " * 30)
        assert isinstance(cache.sanitize_line(line), str)


class TestAtTagStripping:
    """audit #12：``<at>`` 标签默认剥除（LLM 输出不得真 @ 人/群），显式放开."""

    def test_at_tag_stripped_by_default(self) -> None:
        text = "请查看 <at id=ou_123></at> 的反馈 <at email=a@b.com></at>"
        out = sanitize_markdown(text)
        assert "<at" not in out
        assert "</at" not in out
        assert "的反馈" in out  # 正文内容保留

    def test_at_tag_short_text_also_stripped(self) -> None:
        """短文本（<100 字符直返路径）同样拦截——安全动作先于装饰性短路."""
        out = sanitize_markdown("<at id=all></at> hi")
        assert "<at" not in out
        assert "hi" in out

    def test_at_tag_self_closing_and_case_insensitive(self) -> None:
        out = sanitize_markdown("<AT id=x /> ping </AT >")
        assert "at id" not in out.lower().replace(" ", "")
        assert "ping" in out

    def test_allow_at_users_explicit_optin(self) -> None:
        text = "cc <at id=ou_123></at>"
        out = sanitize_markdown(text, allow_at_users=True)
        assert "<at id=ou_123></at>" in out  # 显式放开原样保留

    def test_text_without_at_untouched(self) -> None:
        assert sanitize_markdown("attendance at 9am") == "attendance at 9am"
