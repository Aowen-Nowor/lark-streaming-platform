"""Legacy 文本兼容转换器 — 前代文本启发式编码的参考实现（01-protocol §8）.

前代（hermes-lark-streaming v2.1.x）以文本模板承载进度信息：
``💬 {text}`` 思考 / ``{emoji} {tool}...`` 工具行 / 围栏命令块 /
``(×N)`` 去重后缀 / 平台通知前缀（⚠️）。这些启发式**不上核心层**——
由本转换器把原生文本帧转换为分类结论（:class:`ClassifyResult`），
接入方（hermes 适配器）据其组装 LSEP/1 事件投递（01-protocol §8：
转换器随仓库发布，前代接入方零改动语义迁移）。

移植来源（04-knowledge-freeze §4 legacy 兼容基线，语义逐点对齐）：
- 前代 ``cardkit/progline.py`` — 进度行解析（叶子模块，纯 stdlib）
- 前代 ``composer/classify.py`` — 通道分类（metadata 契约 → 通道）

设计红线（继承前代）：**永不抛异常**——任何解析失败都降级为最保守
的通道（interim / plain / unknown），消息内容永不丢失（最坏情况走
原生文本路径）。本模块纯函数、零 IO、零时钟；LSEP 信封组装归
``adapters/hermes.py``（分类结论 → 事件流）。
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

_logger = logging.getLogger("lark_streaming.adapters.legacy_text")

__all__ = [
    # 通道分类（前代 classify.py 对齐）
    "CH_STREAM", "CH_PROGRESS", "CH_INTERIM", "CH_FINAL", "CH_PLAIN",
    "ClassifyResult", "classify_send", "classify_edit",
    "is_notice_text", "is_background_completion_text", "is_progress_text",
    # 进度行解析（前代 progline.py 对齐）
    "THINKING_PREFIX", "EMOJI_RE",
    "strip_dedup_suffix", "parse_progress_line", "match_verb_line",
    "preview_text", "split_progress_segments",
    # 双跑对照发现项移植（Phase 6）
    "upgrade_plain", "parse_cron_delivery",
]

# ── 通道常量（前代 classify.py CH_* 逐字对齐）───────────────────────────────
CH_STREAM = "stream"      # answer 流式帧（send 首条 / edit 后续）
CH_PROGRESS = "progress"  # 工具进度行 / thinking 气泡（progress lane）
CH_INTERIM = "interim"    # interim 播报（非工具非思考的中途文本）
CH_FINAL = "final"        # turn 定稿
CH_PLAIN = "plain"        # 普通消息（cron / 后台 / 无标记发送）

# ── 进度行解析（前代 progline.py 逐点移植）──────────────────────────────────

THINKING_PREFIX = "\U0001f4ac"  # 💬

# 工具行首 emoji 检测（tool_head/tool_pending/tool_preview 均以 emoji 开头）
EMOJI_RE = re.compile(
    "^[\U0001f300-\U0001faff\u2600-\u27bf\U0001f1e6-\U0001f1ff\u2b00-\u2bff\u23e9-\u23fa]"
)
# 工具预览行：{emoji} {tool}："..." / {emoji} {tool}("...")（组 3 = 引号内预览）
# 前代 v2.0.34：分隔符类含 zh 全角冒号 ``：`` 与 en 半角冒号 ``:``（en 模板
# ``tool_preview: "{emoji} {tool}: \"{preview}\""``；bridge 工具无 curated
# 动词必走 preview 模板——生产实证 tool_search 行曾因缺半角冒号切不出工具名）。
_TOOL_PREVIEW_RE = re.compile(r"^(\S+)\s+([A-Za-z_][\w.\-]*)\s*[：（(:](.*)$")
# 工具待运行行：{emoji} {tool}...（组 2 = 工具名）
_TOOL_PENDING_RE = re.compile(r"^(\S+)\s+([A-Za-z_][\w.\-]*)\.\.\.$")
# 裸工具头行：{emoji} {tool}（tool_head 模板；单发时按 pending 渲染）
_TOOL_HEAD_RE = re.compile(r"^(\S+)\s+([A-Za-z_][\w.\-]*)$")
# 网关同文本去重后缀：重复进度行追加 " (×N)"
_DEDUP_SUFFIX_RE = re.compile(r"\s*\(×\d+\)\s*$")
# 网关命令截断尾：多行命令取首行后追加 " ..." / 截断追加 "..."
_TRUNC_TAIL_RE = re.compile(r"\s*\.{3}$")
# emoji 行拆头：{emoji} {rest}（emoji 为首个空白前 token）
_EMOJI_LEAD_RE = re.compile(r"^(\S+)\s+(.+)$")

# ── 动词行反查表（hermes locales display.verb.*，zh + en）───────────────────
# 同一动词文本多工具冲突时取更常见工具（en "Reading"：read_file 优先 web_extract）。
_VERB_TO_TOOL: tuple[tuple[str, str], ...] = (
    # zh（locales/zh.yaml display.verb）
    ("搜索网页", "web_search"),
    ("读取网页", "web_extract"),
    ("浏览", "browser_navigate"),
    ("点击", "browser_click"),
    ("输入", "browser_type"),
    ("读取文件", "read_file"),
    ("写入文件", "write_file"),
    ("编辑", "patch"),
    ("搜索文件", "search_files"),
    ("运行命令", "terminal"),
    ("运行代码", "execute_code"),
    ("生成图片", "image_generate"),
    ("生成视频", "video_generate"),
    ("生成语音", "text_to_speech"),
    ("查看图片", "vision_analyze"),
    ("搜索历史会话", "session_search"),
    ("阅读技能", "skill_view"),
    ("列出技能", "skills_list"),
    ("更新技能", "skill_manage"),
    ("委派任务", "delegate_task"),
    ("安排定时任务", "cronjob_manage"),
    ("提问", "clarify"),
    ("更新记忆", "memory"),
    ("更新任务", "todo_list"),
    # en（locales/en.yaml display.verb）
    ("Searching the web", "web_search"),
    ("Searching files", "search_files"),
    ("Searching past sessions", "session_search"),
    ("Reading skill", "skill_view"),
    ("Reading", "read_file"),
    ("Writing", "write_file"),
    ("Browsing", "browser_navigate"),
    ("Clicking", "browser_click"),
    ("Typing", "browser_type"),
    ("Editing", "patch"),
    ("Running code", "execute_code"),
    ("Running", "terminal"),
    ("Generating image", "image_generate"),
    ("Generating video", "video_generate"),
    ("Generating speech", "text_to_speech"),
    ("Looking at the image", "vision_analyze"),
    ("Listing skills", "skills_list"),
    ("Updating skill", "skill_manage"),
    ("Updating memory", "memory"),
    ("Updating tasks", "todo_list"),
    ("Delegating", "delegate_task"),
    ("Scheduling", "cronjob_manage"),
    ("Asking", "clarify"),
)
# 长动词优先（"运行代码" 先于 "运行"、"Reading skill" 先于 "Reading"）
_VERBS_SORTED: tuple[tuple[str, str], ...] = tuple(
    sorted(_VERB_TO_TOOL, key=lambda pair: len(pair[0]), reverse=True)
)
# 动词-预览连接符（locales display.verb_connector：search 类 " · "/" for "，默认 " "）
_VERB_CONNECTORS: tuple[str, ...] = (" · ", " for ", " ")


def strip_dedup_suffix(text: str) -> str:
    """剥掉网关去重后缀 `` (×N)``（同文本重发标记；Q9 语义）."""
    return _DEDUP_SUFFIX_RE.sub("", (text or "").strip()).strip()


def _split_fence_segment(text: str) -> tuple[str, str]:
    """多行段 → (header, 命令正文). 无围栏返回 (原文, "")；裸围栏 header 为空."""
    lines = text.splitlines()
    start = -1
    for i, ln in enumerate(lines):
        if ln.strip().startswith("```"):
            start = i
            break
    if start < 0:
        return text.strip(), ""
    end = -1
    for i in range(len(lines) - 1, start, -1):
        if lines[i].strip().startswith("```"):
            end = i
            break
    header = "\n".join(lines[:start]).strip()
    body = "\n".join(lines[start + 1: end if end > start else len(lines)])
    return header, body.strip()


def _parse_single_line(text: str) -> tuple[str, str, str]:
    """单行进度文本 → (kind, tool, clean). 多格式按确定性顺序尝试."""
    m = _TOOL_PENDING_RE.match(text)
    if m:
        return "tool", m.group(2), text
    m = _TOOL_PREVIEW_RE.match(text)
    if m:
        return "tool", m.group(2), text
    verb = match_verb_line(text)
    if verb is not None:
        return "tool", verb[0], text
    m = _TOOL_HEAD_RE.match(text)
    if m:
        return "tool", m.group(2), text
    if EMOJI_RE.match(text):
        return "tool", "", text
    return "unknown", "", text


def parse_progress_line(content: str) -> tuple[str, str, str]:
    """把 progress 文本拆成 (kind, tool_name, clean_text).

    kind: "thinking" | "tool" | "unknown"（unknown 时 clean_text 为原文）
    支持多行段（tool_head + 围栏代码块，header 可缺省于连续调用）、
    中文/英文动词行、去重后缀 ``(×N)``。
    永不抛异常；解析不出结构时返回原文。
    """
    try:
        text = strip_dedup_suffix(content)
        if text.startswith(THINKING_PREFIX):
            return "thinking", "", text[len(THINKING_PREFIX):].strip()
        if "```" in text:
            header, _body = _split_fence_segment(text)
            if header:
                kind, tool, _clean = _parse_single_line(header.splitlines()[0].strip())
                return ("tool" if kind == "unknown" else kind, tool, text)
            # 裸围栏块（无 header）归属 terminal（前代 v2.0.22 铁证：仅
            # terminal 工具产围栏代码块，连续调用省略重复 header）——
            # 归属后渲染带图标步骤行、耗时钩子可挂（前代用户验收根因）。
            return "tool", "terminal", text
        return _parse_single_line(text)
    except Exception:  # 解析纯装饰，绝不影响投递
        _logger.debug("parse_progress_line failed for %r", (content or "")[:50], exc_info=True)
        return "unknown", "", (content or "").strip()


def match_verb_line(text: str) -> tuple[str, str] | None:
    """中文/英文动词行 → (tool_name, preview)；非动词行返回 None.

    格式：``{emoji} {verb}{connector}{preview}``（连接符 " · " / " for " / " "；
    无预览动词只有 ``{emoji} {verb}``）。动词前缀命中但连接符不匹配时返回
    None（如 "编辑器 ..." 不是 patch 动词行）。永不抛异常。
    """
    try:
        t = strip_dedup_suffix(text)
        if not t or not EMOJI_RE.match(t):
            return None
        m = _EMOJI_LEAD_RE.match(t)
        if not m:
            return None
        rest = m.group(2).strip()
        for verb, tool in _VERBS_SORTED:
            if not rest.startswith(verb):
                continue
            tail = rest[len(verb):]
            if not tail:
                return (tool, "")  # 无预览动词（列出技能 / Listing skills）
            for conn in _VERB_CONNECTORS:
                if tail.startswith(conn):
                    return (tool, tail[len(conn):].strip())
            return None  # 动词前缀但无连接符 → 普通文本
        return None
    except Exception:  # noqa: BLE001
        return None


def preview_text(text: str) -> str:
    """工具进度行 → 命令/参数摘要；非工具行返回空串。永不抛异常。

    覆盖：围栏代码块（首行命令，剥截断尾）、动词行（连接符后正文）、
    引号预览（``{tool}："..."``）。
    """
    try:
        t = strip_dedup_suffix(text)
        if not t:
            return ""
        if "```" in t:
            _header, body = _split_fence_segment(t)
            return _TRUNC_TAIL_RE.sub("", body.splitlines()[0].strip()) if body else ""
        m = _TOOL_PREVIEW_RE.match(t)
        if m:
            preview = (m.group(3) or "").strip()
            # legacy 模板把预览包在引号里（`："{preview}"`）→ 剥一对包裹引号；
            # 只剥成对包裹（命令自身的尾部引号如 date "+%H:%M:%S" 不受影响）
            if len(preview) >= 2 and preview[0] in "\"“”" and preview[-1] in "\"“”":
                preview = preview[1:-1].strip()
            return _TRUNC_TAIL_RE.sub("", preview)
        verb = match_verb_line(t)
        if verb is not None:
            return _TRUNC_TAIL_RE.sub("", verb[1])
        return ""
    except Exception:  # 解析纯装饰，绝不影响投递
        _logger.debug("preview_text parse failed for %r", (text or "")[:50], exc_info=True)
        return ""


def split_progress_segments(raw: str) -> list[str]:
    """accumulate 快照 → 逻辑段列表（围栏块与 header 保持一体）.

    网关 accumulate 模式每个 tick 重发"全部进度行"（``"\\n"`` 连接，行本身
    可能是多行代码块）。按 ``str.splitlines`` 直拆会把代码块拆散。保证：

    - 围栏块（```...```）整体成段，且**向前吸附紧邻的单行 header**
      （``💻 terminal`` + 块 → 一个段；💬 思考行不被吸附）
    - 无 header 的裸围栏块独立成段
    - 未闭合围栏按到文本末尾处理（部分帧也渲染，不丢内容）
    - 普通行逐行成段；空行丢弃
    - 多行思考吸附：💬 行与其后的非 emoji 非围栏连续行吸附为一个逻辑段
      （思考块是多行逻辑单元，前代 v2.0.8 用户验收根因）
    """
    if not raw or not raw.strip():
        return []
    segments: list[str] = []
    buf: list[str] = []
    in_fence = False
    in_thinking = False
    for line in raw.splitlines():
        stripped = line.strip()
        if in_fence:
            buf.append(stripped)
            if stripped.startswith("```"):
                in_fence = False
                in_thinking = False
                segments.append("\n".join(buf))
                buf = []
            continue
        if stripped.startswith("```"):
            in_fence = True
            in_thinking = False
            # 向前吸附紧邻单行工具头（emoji 开头：tool_head/pending/preview/动词行），
            # 块与头一体；普通文本行与 💬 思考行不吸附（思考块独立成段）
            if segments:
                prev = segments[-1]
                if ("\n" not in prev and not prev.startswith("```")
                        and not prev.startswith(THINKING_PREFIX) and EMOJI_RE.match(prev)):
                    buf.append(segments.pop())
            buf.append(stripped)
            continue
        if buf:
            # 未闭合围栏中遇到的非围栏行不应出现（防御）：先落盘已开段
            segments.append("\n".join(buf))
            buf = []
        if not stripped:
            continue  # 空行丢弃（思考段内段落分隔坍缩，正文语义不变）
        if stripped.startswith(THINKING_PREFIX):
            # 思考行开启/延续思考块：同一快照内的同块续行不再新开段
            if in_thinking and segments:
                segments[-1] = f"{segments[-1]}\n{stripped}"
            else:
                segments.append(stripped)
                in_thinking = True
            continue
        if in_thinking and not EMOJI_RE.match(stripped):
            # 思考块续行（非 emoji 普通文本）→ 吸附进当前思考段
            segments[-1] = f"{segments[-1]}\n{stripped}"
            continue
        in_thinking = False
        segments.append(stripped)
    if buf:
        segments.append("\n".join(buf))
    return segments


# ── 通道分类（前代 classify.py 逐点移植）────────────────────────────────────

# 后台非对话通知清单（前代 v2.0.8 用户验收「新卡跑马灯不止」根因；v2.0.34
# 补网关生命周期通知）。这类文本带 emoji → 命中进度启发式 → 若进卡片通道
# 且无 finalize → 永久跑马灯。归入 notice 语义（活跃卡并入面板 / 无卡原生
# 文本），永不建卡。锚定 i18n 模板特征短语，en+zh 双语与动词表同口径。
_NOTICE_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"^\s*💾\s*Self-improvement review:\s+\S", re.IGNORECASE),
    re.compile(r"^\s*⏳\s+Working\s+—\s+\d+\s+min(?:\s|$)", re.IGNORECASE),
    re.compile(r"^\s*⏳\s*正在处理——\d+\s*分钟(?:\s|$)"),
    re.compile(r"^\s*⏳\s*處理中\s+—\s*\d+\s*分鐘(?:\s|$)"),
    re.compile(r"^\s*💾\s*自我改进回顾：\s*\S"),
    re.compile(r"^\s*💾\s*自我改進檢閱：\s*\S"),
    re.compile(r"^\s*(?:✅|❌)\s+Hermes update\s+(?:finished|failed|timed out)", re.IGNORECASE),
    re.compile(r"^\s*♻️?\s+Gateway\s+(?:restarted successfully|online\b)", re.IGNORECASE),
    # gateway.shutdown.*（en + zh 双语，锚定模板特征短语）
    re.compile(r"^\s*⚠\uFE0F?\s+Hermes\s+is\s+(?:shutting\s+down|restarting)\b", re.IGNORECASE),
    re.compile(r"^\s*⚠\uFE0F?\s+Hermes\s+(?:正在关闭|正在重启|正在關閉|正在重新啟動)"),
    re.compile(r"^\s*⚠\uFE0F?\s+(?:Scheduled\s+job|定时任务|排程任務)\s+'", re.IGNORECASE),
)

# 后台任务完成模板（gateway.background.complete_header，en/zh/zh-hant）
_BACKGROUND_COMPLETE_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"^✅ 后台任务完成\n提示词："),
    re.compile(r"^✅ 背景任務完成\n提示："),
    re.compile(r"^✅ Background task complete\nPrompt: ", re.IGNORECASE),
)

# 平台通知轨（前代 v2.0.15 子代理失败通知 >300 字符漏洞）：固定前缀 f-string
# 硬编码、reason cap 200 使整行可达 ~359——超工具行 300 上限，故 600 防御。
# hermes 该行硬编码英文（无 i18n），不猜测中文变体。
_PLATFORM_NOTICE_RES: tuple[re.Pattern[str], ...] = (
    re.compile(r"^⚠\uFE0F?\s+Subagent\s+(?:failed|timed out)\b"),
)


def is_background_completion_text(content: str) -> bool:
    """/bg 完成结果模板（独立正式投递，非 turn progress、不并入活跃回合）."""
    text = (content or "").strip()
    return any(rx.match(text) for rx in _BACKGROUND_COMPLETE_RES)


def is_notice_text(content: str) -> bool:
    """后台非对话通知 → 永不进卡片建卡通道（跑马灯根因防线）."""
    text = (content or "").strip()
    if not text:
        return False
    return is_background_completion_text(text) or any(rx.match(text) for rx in _NOTICE_RES)


def is_progress_text(content: str) -> bool:
    """启发式：progress lane 文本 = thinking 气泡 或 emoji 开头的短工具行.

    - 裸围栏代码块判为 progress，上限 600（块含命令正文比单行预览长）
    - 平台通知轨（⚠️ Subagent 固定前缀）豁免 300 上限（实测最长 ~359；
      前缀特异性高，误吸收风险≈0）
    - 其余 emoji 行上限 300（工具行都很短；长文本不可能是工具行）
    """
    text = (content or "").strip()
    if not text:
        return False
    if text.startswith(THINKING_PREFIX):
        return True
    if text.startswith("```"):
        return len(text) <= 600  # 命令块远短于 600；长文本不可能是进度块
    if any(rx.match(text) for rx in _PLATFORM_NOTICE_RES):
        return len(text) <= 600  # 通知行实测最长 ~359；600 防御未来文案变长
    if len(text) > 300:  # 工具行都很短；长文本不可能是工具行
        return False
    return bool(EMOJI_RE.match(text))


def _strip_internal_meta(metadata: dict[str, Any] | None) -> dict[str, Any]:
    """剥离网关内部标记（官方注释：adapters strip it before the wire）."""
    if not metadata:
        return {}
    meta = dict(metadata)
    meta.pop("_interim_send", None)
    return meta


@dataclass
class ClassifyResult:
    """一帧的分类结论（前代 classify.py ClassifyResult 对齐）."""

    channel: str
    kind: str = "text"          # 细分：answer / thinking / tool / advisory / plain
    tool_name: str = ""         # kind=tool 时尽力提取
    thinking_text: str = ""     # kind=thinking 时剥离前缀后的原文
    raw_content: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


def classify_send(
    content: str,
    metadata: dict[str, Any] | None,
) -> ClassifyResult:
    """分类一个 send() 帧（hermes v0.21.x 官方 metadata 契约，无私有 hack）.

    输入契约：
    - ``metadata["expect_edits"]=True``  → answer 流式帧（后续会被 edit 更新）
    - ``metadata["_interim_send"]=True`` → mid-turn 状态帧
    - ``metadata["notify"]=True``        → turn 定稿帧（原生流式 final 语义）
    - 无任何标记 → 原生路径（cron / 后台 / 外部投递）

    永不抛异常——任何失败降级为 CH_PLAIN（原生路径），消息内容永不丢失。
    """
    try:
        meta = metadata or {}
        interim = bool(meta.get("_interim_send"))
        expect_edits = bool(meta.get("expect_edits"))
        notify = bool(meta.get("notify"))

        # /bg 完成结果是独立正式投递，不是 turn progress，也不并入活跃回合。
        if is_background_completion_text(content):
            return ClassifyResult(
                CH_PLAIN, kind="background_result", raw_content=content,
                meta=_strip_internal_meta(meta),
            )

        # 1) answer 流首帧：显式 expect_edits 且不是内部状态帧
        if expect_edits and not interim:
            return ClassifyResult(CH_STREAM, kind="answer", raw_content=content,
                                  meta=_strip_internal_meta(meta))

        # 2) mid-turn 状态帧（progress 气泡首条 / interim 播报共用标记）
        if interim:
            # 后台非对话通知 → notice 语义（活跃卡并入面板 / 无卡原生文本），
            # 永不触发建卡（跑马灯根因）。
            if is_notice_text(content):
                return ClassifyResult(CH_INTERIM, kind="advisory", raw_content=content,
                                      meta=_strip_internal_meta(meta))
            if is_progress_text(content):
                kind, tool, clean = parse_progress_line(content)
                return ClassifyResult(CH_PROGRESS, kind=kind, tool_name=tool,
                                      thinking_text=clean, raw_content=content,
                                      meta=_strip_internal_meta(meta))
            result = ClassifyResult(
                CH_INTERIM, kind="advisory", raw_content=content,
                meta=_strip_internal_meta(meta),
            )
            # 排除精确后台模板后，其余 interim assistant 文本是对话 commentary
            result.meta["conversational_commentary"] = True
            return result

        # 3) 定稿帧
        if notify:
            return ClassifyResult(CH_FINAL, kind="final", raw_content=content,
                                  meta=_strip_internal_meta(meta))

        # 4) 兜底：无任何标记 → 原生路径（cron / 后台 / 外部投递）
        return ClassifyResult(CH_PLAIN, kind="plain", raw_content=content,
                              meta=_strip_internal_meta(meta))
    except Exception:
        _logger.debug("classify_send fallback to plain", exc_info=True)
        return ClassifyResult(CH_PLAIN, kind="plain", raw_content=content or "")


def classify_edit(
    content: str,
    finalize: bool,
) -> ClassifyResult:
    """分类一个 edit_message() 帧.

    edit 无 metadata 参数 → 通道归属由适配器的回合注册表决定，
    这里只判定 finalize 语义。
    """
    return ClassifyResult(
        CH_FINAL if finalize else CH_STREAM,
        kind="final" if finalize else "answer",
        raw_content=content or "",
    )


# ── v2.0.1 上下文升格（前代 adapter._upgrade_plain_to_progress 语义移植）──────
#
# 双跑对照发现项 F2（Phase 6，03_progress_unmarked）：edit-transport 平台的
# 工具进度气泡首发不带任何 metadata 标记——冻结契约「无标记 → deliver」会把
# 每条进度行投成独立静态卡（散卡刷屏）。前代 v2.0.1 起以升格启发式吸收进
# 状态面板（v2.0.4 去活跃卡前置：工具帧常早于答案首帧，升格后由 pending
# 暂存承接；v2.0.8 notify/非对话通知永不升格；v2.0.31 回合内宽吸收）。
# 本函数逐语义移植，供 HermesAdapter 在 CH_PLAIN 分支调用。


def upgrade_plain(
    content: str,
    metadata: dict[str, Any] | None,
    *,
    has_active_turn: bool,
) -> "ClassifyResult | None":
    """裸 send 升格判定（前代 v2.0.1/v2.0.4/v2.0.8/v2.0.31 主链复刻）.

    返回升格后的 ``ClassifyResult``（CH_PROGRESS）；不满足条件返回
    ``None``（维持原分类 → deliver）。永不抛异常（内部全吞，失败维持
    原分类，永不阻断投递主链）。
    """
    try:
        meta = metadata or {}
        # v2.0.8：notify/非对话通知（💾 Self-improvement review 等）永不升格
        if meta.get("notify") or meta.get("non_conversational"):
            return None
        if not content or is_notice_text(content):
            return None
        if not is_progress_text(content):
            if meta or len(content) > 4000:
                return None
            # 宽吸收只覆盖已知工具结构（emoji 工具头或 terminal 围栏），
            # 普通独立正文即使很短也不得并入状态面板（v2.0.31 回合内宽吸收
            # 前置：仅活跃回合期生效）
            if not EMOJI_RE.match(content) and not content.lstrip().startswith("```"):
                return None
            if not has_active_turn:
                return None
        kind, tool, clean = parse_progress_line(content)
        return ClassifyResult(
            channel=CH_PROGRESS,
            kind=kind,
            tool_name=tool,
            thinking_text=clean if kind == "thinking" else "",
            raw_content=content,
        )
    except Exception:  # noqa: BLE001 — 升格失败维持原分类，永不阻断
        return None


# ── cron 投递解析（前代 cardkit/special.parse_cron_delivery 移植）────────────
#
# 双跑对照发现项 F3（Phase 6，11_cron_delivery）：hermes 调度投递官方包装头
# （scheduler_delivery cron.wrap_response 默认 true）——旧侧按 R1 规则解析
# 包装并落 cron 卡（标题 + job_id 溯源页脚）；新侧冻结契约把 notify=True
# 映射为微回合定稿卡，包装头原文上卡、job_id 溯源丢失。移植解析供适配器
# 把 cron 家族路由到 deliver（静态卡 + footer.job_id 溯源）。

_CRON_HEAD_PREFIX = "Cronjob Response: "
_CRON_JOB_ID_RE = re.compile(r"^\(job_id:\s*(.*?)\)\s*$")


def parse_cron_delivery(content: str) -> "tuple[str, str, str]":
    """解析官方 cron 投递包装头 → ``(task_name, job_id, body)``.

    官方格式：首行 ``Cronjob Response: {name}``，次行 ``(job_id: {id})``，
    第三行分隔线。非包装内容原样透传：返回 ``("", "", content)``。
    任何解析异常降级为原样透传（内容永不丢失）。
    """
    try:
        text = (content or "").strip("\n")
        lines = text.split("\n")
        title = job_id = ""
        idx = 0
        if lines and lines[0].startswith(_CRON_HEAD_PREFIX):
            title = lines[0][len(_CRON_HEAD_PREFIX):].strip()
            idx = 1
            if len(lines) > idx:
                m = _CRON_JOB_ID_RE.match(lines[idx].strip())
                if m:
                    job_id = m.group(1).strip()
                    idx += 1
            if len(lines) > idx and lines[idx].strip() and set(lines[idx].strip()) <= {"-"}:
                idx += 1
        body = "\n".join(lines[idx:]).strip()
        return title, job_id, body
    except Exception:  # noqa: BLE001 — 解析失败原样透传（内容永不丢失）
        return "", "", content or ""
