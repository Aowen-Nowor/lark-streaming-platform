"""Markdown 卫生化与长文本分块 — 前代 md.py 逐语义迁移 + OPT-2/5 缓存.

来源（04-knowledge-freeze §3 性能守恒表）：
- 标题降级围栏状态机：v2.0.21 审计 R5（未闭合围栏内 ``# 注释`` 行不再被误降级）
- 分块围栏感知切点：v2.0.21 审计 R6（段落 > 换行 > 硬切，切点必须围栏闭合）
- 表格降级阈值：流式卡 20 表 / 静态卡 5 表（飞书 Card 2.0 单卡硬限）
- OPT-2/5 缓存（03-card-schema §7）：流式 answer 卫生化单槽精确串缓存；
  面板行卫生化逐行精确串缓存（行合并后不可变，命中率近恒 1）。

应用边界（与前代生产语义一致）：
- 流式回合卡 answer 走 stream_element 打字机通道，**原文直发不卫生化**
  （前代生产验证语义；终值 patch 同样原文）；
- 静态卡（deliver / 通知）正文全链卫生化（前代 build_cron_card 链路）。
  卫生化缓存器为 Phase 2 引擎层 / legacy 转换器预留。
"""

from __future__ import annotations

import logging
import re

_logger = logging.getLogger("lark_streaming.renderer.md")

__all__ = [
    "MAX_CARD_TABLES",
    "MAX_STATIC_TABLES",
    "MAX_CHUNK_CHARS",
    "sanitize_markdown",
    "split_long_text",
    "find_tables_outside_code_blocks",
    "downgrade_tables",
    "strip_invalid_image_keys",
    "MarkdownCache",
]

_MAX_CARD_TABLES = 20   # 流式卡片：20表降级阈值（流式增量内容，飞书宽松执行）
_MAX_STATIC_TABLES = 5  # 静态卡片：5表降级阈值（飞书 Card 2.0 单卡硬限）
_MAX_CHUNK_CHARS = 2400

# 兼容别名（前代命名）
MAX_CARD_TABLES = _MAX_CARD_TABLES
MAX_STATIC_TABLES = _MAX_STATIC_TABLES
MAX_CHUNK_CHARS = _MAX_CHUNK_CHARS

# ── Pre-compiled regex patterns（前代 P2-01：避免每次调用重编译）─────────
_RE_FENCED_CODE = re.compile(r"```[\s\S]*?```")
_RE_TABLE_ROW = re.compile(r"\|.+\|\n\|[-:| ]+\|[\s\S]*?(?=\n\n|\n(?!\|)|$)")
_RE_IMAGE_REF = re.compile(r"!\[([^\]]*)\]\(([^)\s]+)\)")
_RE_CODE_BLOCK_EXTRACT = re.compile(r"(^|\n)(`{3,})([^\n]*)\n[\s\S]*?\n\2(?=\n|$)")
_RE_H1_TO_H3 = re.compile(r"^#{1,3} ", re.MULTILINE)
_RE_HEADING_DEMOTE = re.compile(r"^#{2,6} (.+)$", re.MULTILINE)
_RE_H1_DEMOTE = re.compile(r"^# (.+)$", re.MULTILINE)
_RE_MULTI_NEWLINE = re.compile(r"\n{3,}")
_RE_SHORT_MD_CHECK = re.compile(r"^#{1,6} |\n#{1,6} |```|!\[|\n{3,}")
_RE_FENCE_LINE = re.compile(r"^\s*(`{3,})")  # 行首围栏标记（v2.0.21 围栏状态机）

# 飞书 markdown at 标签（audit #12：默认剥除——LLM 输出的 ``<at id=...>``
# 会真 @ 人/群，引发打扰与仿冒风险；渲染器是纯函数红线，at 是否放开
# 是接入方安全策略，需显式 allow_at_users）
_RE_AT_TAG = re.compile(r"</?\s*at(?:\s[^>]*)?/?>", re.IGNORECASE)


def find_tables_outside_code_blocks(text: str) -> list[tuple[int, int, str]]:
    """查找代码块外的 markdown 表格，返回 [(start, end, raw), ...]."""
    code_ranges: list[tuple[int, int]] = []
    for m in _RE_FENCED_CODE.finditer(text):
        code_ranges.append((m.start(), m.end()))

    def _in_code(idx: int) -> bool:
        return any(s <= idx < e for s, e in code_ranges)

    results: list[tuple[int, int, str]] = []
    for m in _RE_TABLE_ROW.finditer(text):
        if not _in_code(m.start()):
            results.append((m.start(), m.end(), m.group(0)))
    return results


def downgrade_tables(text: str, limit: int = _MAX_CARD_TABLES) -> str:
    """超限表格降级为代码块（保留内容可见但飞书不渲染为表格元素）."""
    if "|" not in text:
        return text
    matches = find_tables_outside_code_blocks(text)
    if len(matches) <= limit:
        return text
    result = text
    for start, end, raw in reversed(matches[limit:]):
        replacement = f"```\n{raw}\n```"
        result = result[:start] + replacement + result[end:]
    return result


def strip_invalid_image_keys(text: str) -> str:
    """移除非 img_ 前缀的图片引用（外链图片飞书 CardKit 不渲染）."""
    if "![" not in text:
        return text

    def _replace(m: re.Match) -> str:
        return m.group(0) if m.group(2).startswith("img_") else ""

    return _RE_IMAGE_REF.sub(_replace, text)


def _demote_headings_outside_fences(text: str) -> str:
    """围栏感知的标题降级（v2.0.21 审计 R5：逐行状态机，围栏内行原样保留）.

    H1→####，H2-H6→#####，与原双正则语义逐行等价。
    """
    lines = text.split("\n")
    out: list[str] = []
    fence_len = 0  # 0 = 不在围栏内；>0 = 当前围栏开栏标记的反引号数
    for ln in lines:
        m = _RE_FENCE_LINE.match(ln)
        if m:
            if not fence_len:
                fence_len = len(m.group(1))
            elif len(m.group(1)) >= fence_len:
                fence_len = 0  # 同长或更长的 ``` 闭栏
            out.append(ln)
            continue
        if fence_len:
            out.append(ln)  # 围栏内行原样（代码/注释不降级）
            continue
        if _RE_H1_DEMOTE.match(ln):
            ln = _RE_H1_DEMOTE.sub(r"#### \1", ln)
        else:
            ln = _RE_HEADING_DEMOTE.sub(r"##### \1", ln)
        out.append(ln)
    return "\n".join(out)


def _fence_balanced(prefix: str) -> bool:
    """prefix 的行首围栏标记是否成对（偶数 = 闭合）."""
    return len(_RE_FENCE_LINE.findall(prefix)) % 2 == 0


def sanitize_markdown(text: str, *, allow_at_users: bool = False) -> str:
    """markdown 卫生化（前代 optimize_markdown_style 逐语义迁移）.

    0. 剥 ``<at>`` 标签（audit #12；``allow_at_users=True`` 显式放开——
       安全动作先于装饰性处理与短文本直返，短注入同样拦截）
    1. 提取闭合代码块为占位符保护
    2. 围栏状态机标题降级（仅存在 H1-H3 时）
    3. 还原代码块
    4. 压缩多余空行（\n{3,} → \n\n）
    5. 剥离无效图片 key
    短文本（<100 字符且无结构标记）零处理直返。任何异常原文返回（装饰性
    处理永不影响内容可见——前代同语义）。
    """
    if not allow_at_users:
        text = _RE_AT_TAG.sub("", text)
    if len(text) < 100 and not _RE_SHORT_MD_CHECK.search(text):
        return text
    try:
        mark = "___CB_"
        code_blocks: list[str] = []

        def _extract(m: re.Match) -> str:
            prefix = m.group(1) or ""
            block = m.group(0)[len(prefix):]
            idx = len(code_blocks)
            code_blocks.append(block)
            return f"{prefix}{mark}{idx}___"

        r = _RE_CODE_BLOCK_EXTRACT.sub(_extract, text)

        if _RE_H1_TO_H3.search(text):
            r = _demote_headings_outside_fences(r)

        for i, block in enumerate(code_blocks):
            r = r.replace(f"{mark}{i}___", block)

        r = _RE_MULTI_NEWLINE.sub("\n\n", r)
        r = strip_invalid_image_keys(r)
        return r
    except Exception:  # noqa: BLE001 — 卫生化失败原文返回（前代同语义）
        _logger.debug("sanitize_markdown failed", exc_info=True)
        return text


def split_long_text(text: str, limit: int = _MAX_CHUNK_CHARS) -> list[str]:
    """将超长文本按段落/换行拆分为多个不超过 limit 字符的块.

    切点围栏感知（v2.0.21 审计 R6）：候选切点（段落 > 换行 > 硬切）逐个
    检查围栏是否闭合，取第一个闭合的；三候选全部落在围栏内才硬切。
    """
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        candidates: list[int] = []
        cut = text.rfind("\n\n", 0, limit)
        if cut >= limit // 2:
            candidates.append(cut)
        cut = text.rfind("\n", 0, limit)
        if cut >= limit // 2:
            candidates.append(cut)
        chosen = limit  # 兜底硬切（极端罕见：单个未闭合围栏 > limit/2）
        for cand in candidates:
            if _fence_balanced(text[:cand]):
                chosen = cand
                break
        chunks.append(text[:chosen])
        text = text[chosen:].lstrip("\n")
    return chunks


class MarkdownCache:
    """OPT-2/5 精确串缓存（03-card-schema §7）.

    - ``sanitize_answer(text)``：单槽——流式场景缓冲单调增长，缓存最新
      一次即可命中终值 patch 的重复卫生化；
    - ``sanitize_line(text)``：逐行——面板行合并后不可变，同文本行反复
      渲染命中率近恒 1。

    纯内存装饰性缓存：容量上限防膨胀（answer 单槽天然 1；行缓存 LRU 截断），
    命中语义 = 精确串相等（键即输入原文）。
    """

    _LINE_CACHE_MAX = 512

    def __init__(self) -> None:
        self._answer_slot: tuple[str, str] | None = None
        self._line_cache: dict[str, str] = {}

    def sanitize_answer(self, text: str) -> str:
        if self._answer_slot is not None and self._answer_slot[0] == text:
            return self._answer_slot[1]
        result = sanitize_markdown(text)
        self._answer_slot = (text, result)
        return result

    def sanitize_line(self, text: str) -> str:
        cached = self._line_cache.get(text)
        if cached is not None:
            return cached
        result = sanitize_markdown(text)
        if len(self._line_cache) >= self._LINE_CACHE_MAX:
            # 淘汰最早一半（前代 adapter 缓存淘汰同语义：截断字典防膨胀）
            for key in list(self._line_cache)[: self._LINE_CACHE_MAX // 2]:
                self._line_cache.pop(key, None)
        self._line_cache[text] = result
        return result
