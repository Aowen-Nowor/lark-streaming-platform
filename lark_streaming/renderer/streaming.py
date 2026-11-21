"""流式回合卡渲染 — 三产物 + 字节预算裁剪（03-card-schema §4/§6，02-engine §4）.

引擎调用点（02-engine §4 时点表）与本模块产物的绑定：

| 引擎时点 | 产物 | CardKit 通道 |
|----------|------|--------------|
| CREATING 成功 | :func:`render_initial_card` | cardkit_create（+ send_card_by_id） |
| STREAMING 面板合并窗口到期 | :func:`render_full_card` | cardkit_update 全量 |
| STREAMING answer 帧 | :attr:`ANSWER_ELEMENT_ID` + :meth:`RenderState.answer_text` | cardkit_stream_element（原文直发） |
| SEALING 第一步 | :func:`final_answer_content` | cardkit_stream_element 终值 patch（原文） |
| SEALING 第二~四步 | :func:`render_sealed_card` | cardkit_update 全量写回定稿卡（#14） |

**定稿改走全量重建**（#14）：原四步里的局部通道 `cardkit_batch_update`
与 `cardkit_close_streaming`（settings）所调端点 `PUT .../batch_update` /
`PUT .../settings` 经 L3 实测返回 HTTP 404，定稿无法落地。现改由
:func:`render_sealed_card` 一次全量写回（`cardkit_update`，已验证可用）。
前代“定稿不走全量重建”的铁律所防的是全量更新把已流 answer 覆盖回截断版；
此处安全是因为渲染输入的 ``answer_text`` 就是终值本身（且
:func:`render_sealed_card` 不裁剪 answer 终值），全量写回与终值 patch
内容一致。`footer_batch_actions` / `panel_reseal_actions` 仍保留（局部
通道的载荷计算），但引擎定稿不再调用它们。

**字节预算（03-card-schema §6）**：全量卡（initial/full）渲染前经
:func:`trim_to_budget` 按「interim → thinking 历史轮（保留当前轮）→
answer 尾部」顺序裁剪到 27000 bytes（前代 ``_CARD_BYTE_BUDGET`` 生产
实测值）；**页脚与 answer 终值永不裁剪**——终值超预算时信任传输层
200860 确定性拒收 → 引擎降级红线兜底（原生投递，消息不丢）。
answer 尾部截断附「…（已截断）」标注。裁剪发生必须可见（TrimReport，
Phase 6 双跑对照的观测面）。

纯函数边界：本模块零 IO、零时钟、零随机；``turn_state`` 等时序语义由
引擎显式传入，渲染层不推断。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .elements import (
    ANSWER_ELEMENT_ID,
    LOADING_ELEMENT_ID,
    UNIFIED_PANEL_ELEMENT_ID,
    build_answer_element,
    build_collapsible_panel,
    build_footer_elements,
    build_loading_element,
    build_panel_children,
    build_panel_title_element,
    enforce_element_limit,
)
from .state import RenderState, RowKind
from .theme import LOCALES, Theme

__all__ = [
    "ANSWER_ELEMENT_ID",
    "UNIFIED_PANEL_ELEMENT_ID",
    "LOADING_ELEMENT_ID",
    "TrimReport",
    "trim_to_budget",
    "render_initial_card",
    "render_full_card",
    "render_sealed_card",
    "final_answer_content",
    "footer_batch_actions",
    "panel_reseal_actions",
    "measure_card_bytes",
]


def _processing_summary(theme: Theme) -> dict:
    en, zh = theme.text("processing")
    return {"content": en, "i18n_content": {"zh_cn": zh, "en_us": en}}


def _final_summary(state: RenderState, theme: Theme) -> dict:
    """定稿摘要（写入定稿卡 ``config.summary``，#14）."""
    text = footer_summary(state, theme)
    return {"content": text, "i18n_content": {"zh_cn": text, "en_us": text}}


def _assemble_streaming_card(
    state: RenderState,
    theme: Theme,
    *,
    turn_state: str,
    print_strategy: str = "delay",
    print_step: int = 4,
    sealed: bool = False,
) -> dict[str, Any]:
    """流式卡骨架组装（前代 build_streaming_card_v2 结构化重写）.

    懒面板（02-engine §4 / 前代 v2.0.24）：``panel_rows`` 为空时不渲染
    面板——纯 answer 回合无空壳 "agent loop" 折叠面板；首个面板行到达
    后由 cardkit_update 全量补插（s172 真飞书探针实证：更新时新增元素
    合法，含流式卡建卡时未声明的元素）。

    sealed=True 时产出定稿形态（#14）：``streaming_mode`` 为 False、
    无 loading spinner、页脚直接拼在 answer 之后、面板按 finalized 态
    渲染、摘要为终值摘要。供 :func:`render_sealed_card` 使用。
    """
    elements: list[dict] = []

    if state.panel_rows:
        panel = build_collapsible_panel(
            expanded=state.panel_expanded,
            title_el=build_panel_title_element(theme, state.panel_title),
            elements=build_panel_children(state.panel_rows, theme, turn_state=turn_state),
        )
        elements.append(panel)

    # answer 元素骨架期无条件存在（空 content 占位）——页脚 insert_before
    # 锚点不受影响（前代 finalize docstring 语义）；流式通道按此 id 打字。
    elements.append(build_answer_element(state.answer_text or "", theme))
    if sealed:
        # 定稿形态：无 loading spinner；页脚直接拼在 answer 之后。
        if state.footer is not None:
            elements.extend(build_footer_elements(state.footer, theme))
    else:
        elements.append(build_loading_element())

    summary = _final_summary(state, theme) if sealed else _processing_summary(theme)
    card: dict[str, Any] = {
        "schema": "2.0",
        "config": {
            "streaming_mode": not sealed,
            "streaming_config": {
                "print_frequency_ms": {"default": 70},
                "print_step": {"default": print_step},
                "print_strategy": print_strategy,
            },
            **theme.style_config(),
            "locales": LOCALES,
            "summary": summary,
        },
        "body": {"elements": elements},
    }
    return enforce_element_limit(card, theme=theme)


def render_initial_card(
    state: RenderState,
    theme: Theme,
    *,
    print_strategy: str = "delay",
    print_step: int = 4,
) -> dict[str, Any]:
    """CREATING 成功时点产物（建卡 JSON）.

    建卡窗口 pending 帧（02-engine §3.2）已由引擎并入 ``panel_rows``；
    answer 缓冲若建卡前已有内容（pending answer 帧）一并回填并经引擎
    以 stream_element 播种（client.create:answer-seed 通道，前代语义）。
    """
    return _assemble_streaming_card(
        state, theme, turn_state="streaming",
        print_strategy=print_strategy, print_step=print_step,
    )


def render_full_card(
    state: RenderState,
    theme: Theme,
    *,
    print_strategy: str = "delay",
    print_step: int = 4,
) -> dict[str, Any]:
    """STREAMING 全量更新产物（面板合并窗口到期 / 面板变化时点）.

    answer 回填防全量更新把已流内容重置为空（前代 _patch_status 铁律）。
    输入先经 :func:`trim_to_budget` 字节预算裁剪（03-card-schema §6）。
    """
    trimmed, _report = trim_to_budget(state, theme)
    return _assemble_streaming_card(
        trimmed, theme, turn_state="streaming",
        print_strategy=print_strategy, print_step=print_step,
    )


def render_sealed_card(
    state: RenderState,
    theme: Theme,
    *,
    print_strategy: str = "delay",
    print_step: int = 4,
) -> dict[str, Any]:
    """SEALING 定稿全量产物（#14：settings / batch_update 端点 404 后改走全量更新）.

    与 :func:`render_full_card` 的区别：``streaming_mode`` 为 False、
    无 loading spinner、面板按 finalized 态渲染、页脚直接拼入、摘要为
    终值摘要。answer 终值不经 :func:`trim_to_budget` 裁剪——定稿前已由
    stream_element 终值 patch 全量送达，全量写回须与之一致，否则会把
    已流内容覆盖回截断版（前代“定稿不走全量重建”的铁律所防的正是此事；
    此处安全是因为渲染输入的 answer_text 就是终值本身）。字节超限由
    传输层 ``assert_card_size`` 拦截（CardSizeExceeded → 引擎降级红线）。
    """
    return _assemble_streaming_card(
        state, theme, turn_state="finalized", sealed=True,
        print_strategy=print_strategy, print_step=print_step,
    )


# ── SEALING 产物（定稿的局部通道载荷；引擎定稿已改走全量写回，#14）────


def final_answer_content(state: RenderState) -> str:
    """定稿终值（stream_element 终值 patch 内容）.

    **原文直发**——前代生产语义：打字机通道内容不做任何卫生化改写
    （markdown 卫生化只用于静态卡，见 md.py 应用边界）。空 answer 跳过
    patch 由引擎判定（stream_element content="" 被飞书拒 99992402，
    s152 探针定案）。
    """
    return state.answer_text or ""


def footer_summary(state: RenderState, theme: Theme) -> str:
    """会话摘要（写入定稿卡 ``config.summary``；前代 _summary_of 语义）."""
    text = (state.answer_text or "").strip()
    if not text:
        en, zh = theme.text("status_completed")
        return zh
    head = text.splitlines()[0].strip()
    return head[:120] or zh


def footer_batch_actions(state: RenderState, theme: Theme) -> list[dict[str, Any]]:
    """定稿第二步 batch_update 载荷：插页脚（spinner 前）+ 删 loading spinner.

    页脚元素经 :func:`build_footer_elements`（空页脚 → 仅删 spinner）。
    两步独立 action 分离返回（引擎可分两次 batch_update 调用——页脚插入
    失败不影响 spinner 删除的前代语义由引擎层编排）。
    """
    actions: list[dict[str, Any]] = []
    if state.footer is not None:
        footer_els = build_footer_elements(state.footer, theme)
        if footer_els:
            actions.append({
                "action": "add_elements",
                "params": {
                    "type": "insert_before",
                    "target_element_id": LOADING_ELEMENT_ID,
                    "elements": footer_els,
                },
            })
    actions.append({
        "action": "delete_elements",
        "params": {"element_ids": [LOADING_ELEMENT_ID]},
    })
    return actions


def panel_reseal_actions(state: RenderState, theme: Theme) -> list[dict[str, Any]] | None:
    """定稿第三步面板收口重建载荷（前代 _reseal_panel 语义）.

    s146 真飞书探针定案：官方 batch_update 无 update_elements action
    （code=10002），可行通道 = 同批次 delete 面板 + add 重建（insert_before
    answer 元素）。返回 None = 无需重建（流式末渲染与定稿渲染逐字节一致
    的幂等跳过，API 经济性——前代同语义）。
    """
    if not state.panel_rows:
        return None
    streaming_children = build_panel_children(state.panel_rows, theme, turn_state="streaming")
    final_children = build_panel_children(state.panel_rows, theme, turn_state="finalized")
    if streaming_children == final_children:
        return None  # 流式末渲染已无「进行中」轮，重建无视觉差异
    panel = build_collapsible_panel(
        expanded=state.panel_expanded,
        title_el=build_panel_title_element(theme, state.panel_title),
        elements=final_children,
    )
    return [
        {"action": "delete_elements", "params": {"element_ids": [UNIFIED_PANEL_ELEMENT_ID]}},
        {
            "action": "add_elements",
            "params": {
                "type": "insert_before",
                "target_element_id": ANSWER_ELEMENT_ID,
                "elements": [panel],
            },
        },
    ]


# ── 字节预算裁剪（03-card-schema §6）─────────────────────────────────────


@dataclass(frozen=True)
class TrimReport:
    """裁剪打点（Phase 6 双跑对照观测面；03-card-schema §6 裁剪必须打点）."""

    budget_bytes: int = 0
    measured_bytes: int = 0
    trimmed_interim: int = 0
    trimmed_thinking_rounds: int = 0
    answer_truncated: bool = False
    final_bytes: int = 0


def measure_card_bytes(card: dict[str, Any]) -> int:
    """按飞书上传形态测量卡片字节数（ensure_ascii=False，UTF-8）."""
    return len(json.dumps(card, ensure_ascii=False).encode("utf-8"))


def trim_to_budget(
    state: RenderState,
    theme: Theme,
    *,
    budget_bytes: int = 27_000,
) -> tuple[RenderState, TrimReport]:
    """全量卡字节预算裁剪（返回裁剪后副本 + 打点；输入 state 不可变）.

    裁剪顺序（03-card-schema §6 冻结稿）：
    1. interim 行（最旧先出）
    2. thinking 历史轮（整轮淘汰，保留当前轮）
    3. answer 尾部截断（附「…（已截断）」标注）

    页脚不参与（全量卡无页脚——页脚只在 SEALING 局部通道，永不裁）。
    answer 终值路径（final_answer_content）不经本函数——永不裁。
    极端兜底：三轮裁剪后仍超限（不可能常态）原样返回，传输层
    assert_card_size 拦截（CardSizeExceeded → 引擎降级红线）。
    """
    from ..transport.budget import CARD_BYTE_BUDGET

    budget = budget_bytes if budget_bytes > 0 else CARD_BYTE_BUDGET
    trimmed = state

    def _measure(s: RenderState) -> int:
        return measure_card_bytes(_assemble_streaming_card(s, theme, turn_state="streaming"))

    measured = _measure(trimmed)
    if measured <= budget:
        return trimmed, TrimReport(
            budget_bytes=budget, measured_bytes=measured, final_bytes=measured,
        )

    trimmed_interim = 0
    trimmed_rounds = 0
    answer_truncated = False

    # 1. interim 行（最旧先出）
    rows = list(trimmed.panel_rows)
    interim_idx = [i for i, r in enumerate(rows) if r.kind is RowKind.INTERIM]
    while interim_idx and measured > budget:
        rows.pop(interim_idx.pop(0))
        interim_idx = [i - 1 for i in interim_idx]  # 前删后索引左移
        trimmed = trimmed.with_panel_rows(rows)
        measured = _measure(trimmed)
    trimmed_interim = len(state.panel_rows) - len(rows)

    # 2. thinking 历史轮（整轮淘汰，保留当前轮 = 最大 round_index）
    if measured > budget and rows:
        current_round = max((r.round_index for r in rows if r.kind is RowKind.THINKING), default=0)
        while measured > budget:
            oldest = next(
                (i for i, r in enumerate(rows) if r.kind is RowKind.THINKING and r.round_index < current_round),
                None,
            )
            if oldest is None:
                break
            old_round = rows[oldest].round_index
            rows = [r for r in rows if not (r.kind is RowKind.THINKING and r.round_index == old_round)]
            trimmed_rounds += 1
            trimmed = trimmed.with_panel_rows(rows)
            measured = _measure(trimmed)

    # 3. answer 尾部截断（附标注；永不裁终值语义只针对 final_answer_content）
    if measured > budget and trimmed.answer_text:
        en_tail, zh_tail = theme.text("answer_truncated")
        truncated = _truncate_answer_to_budget(
            trimmed, theme, budget, marker=zh_tail,
        )
        if truncated is not None:
            trimmed = truncated
            measured = _measure(trimmed)
            answer_truncated = True

    report = TrimReport(
        budget_bytes=budget,
        measured_bytes=_measure(state) if trimmed is not state else measured,
        trimmed_interim=trimmed_interim,
        trimmed_thinking_rounds=trimmed_rounds,
        answer_truncated=answer_truncated,
        final_bytes=measured,
    )
    return trimmed, report


def _truncate_answer_to_budget(
    state: RenderState,
    theme: Theme,
    budget: int,
    *,
    marker: str,
) -> RenderState | None:
    """answer 尾部二分截断至预算内（纯截断——不做任何内容改写）.

    切点围栏闭合检查（前代 _fence_balanced 语义）：候选切点落在未闭合
    围栏内时回退到最近围栏开栏前，避免截断产生渲染坏的前后块。
    """
    text = state.answer_text
    if not text:
        return None
    # 无 answer 基线卡的固定开销
    base = measure_card_bytes(_assemble_streaming_card(
        state.with_answer(""), theme, turn_state="streaming",
    ))
    # marker 本身占字节，另留 JSON 编码裕度
    reserve = len((marker + "\n").encode("utf-8")) + 256
    allowed = budget - base - reserve
    if allowed <= 0:
        return None
    import re as _re

    _fence = _re.compile(r"^\s*(`{3,})", _re.MULTILINE)
    lo, hi = 0, len(text)
    best: str | None = None
    while lo <= hi:
        mid = (lo + hi) // 2
        candidate = text[:mid]
        # 切点围栏闭合检查：落在围栏内 → 回退到最近围栏开栏前
        if len(_fence.findall(candidate)) % 2 != 0:
            fence_pos = candidate.rfind("```")
            if fence_pos <= 0:
                lo = mid + 1
                continue
            candidate = candidate[:fence_pos]
        probe = _assemble_streaming_card(
            state.with_answer(candidate.rstrip("\n") + "\n" + marker),
            theme, turn_state="streaming",
        )
        if measure_card_bytes(probe) <= budget:
            best = candidate.rstrip("\n") + "\n" + marker
            lo = mid + 1
        else:
            hi = mid - 1
    if best is None:
        return None
    return state.with_answer(best)
