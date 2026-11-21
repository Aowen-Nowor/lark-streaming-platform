"""真飞书 L3 验收脚本 — StreamingEngine + FeishuTransport 全栈直驱真实飞书 API.

与前代 scripts/e2e_v2.py 同定位：全链路真火（真实 token、真实 CardKit
端点、真实 IM 投递），驱动的是**新框架本体**（引擎编排 → 渲染纯函数 →
raw REST 传输），不是 mock，不是适配层模拟。同时承担 04-knowledge-freeze
§6 五项待实测数值的现场测量（R1-R6）。

场景一览：
    connect  — token 获取 + 原生文本直发 + deliver 静态卡（连通性锚点）
    plain    — 纯文本直发（原生兜底通道）
    answer   — 流式回合：建卡 → 打字机流式 → 定稿全量写回
    progress — 面板通道：tool.start/end + thinking 轮 + interim + notice
    full     — 仿真实回合全时序（面板 + 流式 + 页脚 stats 全字段）
    burst    — 建卡窗口并发帧（Q3 幂等屏障真火回归：恰一张卡）
    clarify  — 澄清交互卡真投递（真实可点；回调接线归适配层，见 R4 注记）
    reply    — om_ 锚点回复形态 + 伪锚点降级直发（Q10；--dm 双验证 R3）
    renew    — 200850 官方恢复动作（reopen）真火探针；--long 加跑 60s+
               主动续期真时序（T_renew R2 主流程）
    budget   — §6-5 字节预算浮动测量：尺寸阶梯 → 200860 实际阈值
    ratelimit(--probe-rate-limit) — §6-1：99991400 响应体结构 +
               Retry-After 头是否存在（爆发 stream_element 触发）
    seqconflict(--probe-seq) — §6-2：300317 重放/跳号行为 + close→reopen
               最小重开间隔实测

用法：
    export FEISHU_E2E_REAL=1                   # 显式授权真实飞书写操作
    export FEISHU_E2E_APP_ID=cli_xxx           # （或 FEISHU_APP_ID）
    export FEISHU_E2E_APP_SECRET=xxx           # （或 FEISHU_APP_SECRET）
    export FEISHU_E2E_CHAT_ID=oc_xxx           # 群聊（机器人须在群内）
    # 可选：FEISHU_E2E_DM_CHAT_ID=oc_xxx      # 私聊 P2P（reply 双验证 R3）
    # 可选：FEISHU_DOMAIN=feishu|lark         # 站点，默认 feishu

    python3 scripts/e2e_real.py --mock              # L2 台架干跑（零真实写，免凭证）
    python3 scripts/e2e_real.py                     # 默认场景集
    python3 scripts/e2e_real.py --scenario answer,full
    python3 scripts/e2e_real.py --list              # 列出场景
    python3 scripts/e2e_real.py --long              # renew 加真续期时序
    python3 scripts/e2e_real.py --probe-rate-limit --probe-seq   # §6 破坏性探针

速率自保护：流式帧间隔 ≥0.45s（单卡 10 次/秒限的 2 倍裕度），场景间 2s；
破坏性探针仅在显式 flag 下执行。退出码：全部通过 0；任一失败 1。

诚实边界：clarify 回调授权失败码集（§6-4/R4）依赖平台回调接线
（hermes 网关回调面），本脚本只能投递真实可点卡——标 MANUAL，不伪造
结论；降级/竞态恢复语义已在 L2 台架（tests/harness/）穷尽，真火只验证
真实端点行为与 L2 假设一致。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import httpx

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from lark_streaming.credentials import FeishuCredentials  # noqa: E402
from lark_streaming.engine import EngineSettings, LifecycleEvent, StreamingEngine  # noqa: E402
from lark_streaming.engine.events import Ack, Verdict  # noqa: E402
from lark_streaming.renderer import (  # noqa: E402
    ANSWER_ELEMENT_ID,
    DEFAULT_THEME,
    RenderState,
    measure_card_bytes,
    render_full_card,
    render_initial_card,
    render_sealed_card,
)
from lark_streaming.transport import FeishuTransport  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s %(message)s")
_logger = logging.getLogger("e2e_real")

#: 流式帧节奏（秒）——单卡 10 次/秒限的 2 倍裕度
PULSE = 0.45
#: 场景间隔离间隔（秒）
SCENARIO_GAP = 2.0


# ── 环境与授权 ────────────────────────────────────────────────────────────


def _env(*names: str) -> str:
    for n in names:
        v = os.environ.get(n, "").strip()
        if v:
            return v
    return ""


@dataclass
class Env:
    app_id: str
    app_secret: str
    chat_id: str
    dm_chat_id: str
    base_url: str
    mock: bool = False

    @classmethod
    def load(cls) -> "Env":
        if os.environ.get("FEISHU_E2E_REAL") != "1":
            print("真实飞书 E2E 必须显式设置 FEISHU_E2E_REAL=1；默认拒绝执行。")
            raise SystemExit(2)
        e = cls(
            app_id=_env("FEISHU_E2E_APP_ID", "FEISHU_APP_ID"),
            app_secret=_env("FEISHU_E2E_APP_SECRET", "FEISHU_APP_SECRET"),
            chat_id=_env("FEISHU_E2E_CHAT_ID", "FEISHU_CHAT_ID"),
            dm_chat_id=_env("FEISHU_E2E_DM_CHAT_ID"),
            base_url=_env("FEISHU_E2E_BASE_URL"),
        )
        if not e.app_id or not e.app_secret:
            print("缺少凭证：FEISHU_E2E_APP_ID / FEISHU_E2E_APP_SECRET（或 FEISHU_APP_*）。")
            raise SystemExit(2)
        if not e.chat_id:
            print("缺少目标会话：FEISHU_E2E_CHAT_ID（oc_ 开头群聊 id，机器人须在群内）。")
            raise SystemExit(2)
        if not e.base_url:
            domain = _env("FEISHU_DOMAIN") or "feishu"
            e.base_url = (
                "https://open.larksuite.com/open-apis"
                if domain.lower() == "lark"
                else "https://open.feishu.cn/open-apis"
            )
        return e


# ── 记录式传输代理（API 计数 = 断言事实源）────────────────────────────────


class RecordingTransport:
    """薄代理：包 FeishuTransport 记录每类引擎消费面调用次数与关键参数.

    仅计数不改行为——真火断言的 ground truth（恰一张卡 / reopen 出现）.
    """

    def __init__(self, inner: FeishuTransport) -> None:
        self._inner = inner
        self.counts: dict[str, int] = {}
        self.events: list[str] = []

    def _note(self, op: str, detail: str = "") -> None:
        self.counts[op] = self.counts.get(op, 0) + 1
        self.events.append(f"{op}{'|' + detail if detail else ''}")

    async def cardkit_create(self, card: dict[str, Any]) -> str:
        self._note("cardkit_create", f"bytes={measure_card_bytes(card)}")
        return await self._inner.cardkit_create(card)

    async def send_card_by_id_to_chat(self, chat_id: str, card_id: str) -> str:
        self._note("send_card_by_id_to_chat")
        return await self._inner.send_card_by_id_to_chat(chat_id, card_id)

    async def send_card_to_chat(self, chat_id: str, card: dict[str, Any]) -> str:
        self._note("send_card_to_chat")
        return await self._inner.send_card_to_chat(chat_id, card)

    async def send_text_to_chat(self, chat_id: str, text: str) -> str:
        self._note("send_text_to_chat")
        return await self._inner.send_text_to_chat(chat_id, text)

    async def reply_card_by_id(self, message_id: str, card_id: str) -> str:
        self._note("reply_card_by_id")
        return await self._inner.reply_card_by_id(message_id, card_id)

    async def cardkit_stream_element(
        self, card_id: str, element_id: str, content: str, *, sequence: int = 0,
    ) -> None:
        self._note("cardkit_stream_element", f"seq={sequence}")
        return await self._inner.cardkit_stream_element(
            card_id, element_id, content, sequence=sequence,
        )

    async def cardkit_update(
        self, card_id: str, card: dict[str, Any], *, sequence: int = 0,
    ) -> None:
        self._note("cardkit_update", f"seq={sequence}")
        return await self._inner.cardkit_update(card_id, card, sequence=sequence)

    async def cardkit_batch_update(
        self, card_id: str, actions: list[dict[str, Any]], *, sequence: int = 0,
    ) -> None:
        self._note("cardkit_batch_update", f"seq={sequence}")
        return await self._inner.cardkit_batch_update(card_id, actions, sequence=sequence)

    async def cardkit_close_streaming(
        self, card_id: str, card: dict[str, Any], *, sequence: int = 0,
    ) -> None:
        # #14：定稿改走全量更新，调用方传入已定稿的完整卡 JSON；底层为一次
        # cardkit_update（不在此处重复计数，close_streaming 即定稿信号）。
        self._note("cardkit_close_streaming", f"seq={sequence}")
        return await self._inner.cardkit_close_streaming(
            card_id, card, sequence=sequence,
        )

    async def cardkit_reopen_streaming(
        self, card_id: str, card: dict[str, Any], *, sequence: int = 0,
    ) -> None:
        # #14：重开会话改走全量更新，调用方传入当前流式卡。
        self._note("cardkit_reopen_streaming", f"seq={sequence}")
        return await self._inner.cardkit_reopen_streaming(
            card_id, card, sequence=sequence,
        )

    def __getattr__(self, name: str) -> Any:  # aclose / _inner / _token 等 passthrough
        return getattr(self._inner, name)


# ── 场景结果 ──────────────────────────────────────────────────────────────


@dataclass
class ScenarioResult:
    name: str
    ok: bool = True
    skipped: bool = False
    notes: list[str] = field(default_factory=list)
    measurements: dict[str, str] = field(default_factory=dict)

    def check(self, cond: bool, what: str) -> None:
        mark = "PASS" if cond else "FAIL"
        self.notes.append(f"  [{mark}] {what}")
        if not cond:
            self.ok = False

    def info(self, text: str) -> None:
        self.notes.append(f"  [info] {text}")

    def measure(self, key: str, value: str) -> None:
        self.measurements[key] = value


RESULTS: list[ScenarioResult] = []


# ── 引擎驱动器 ────────────────────────────────────────────────────────────


class Driver:
    """一个场景一个驱动器：引擎 + 记录代理 + 生命周期收集 + 信封构造."""

    def __init__(
        self, env: Env, transport: FeishuTransport,
        *, settings: EngineSettings | None = None,
        probe_client: httpx.AsyncClient | None = None,
    ) -> None:
        self.env = env
        self.rt = RecordingTransport(transport)
        self.settings = settings or EngineSettings()
        self.probe_client = probe_client
        self.lifecycle: list[LifecycleEvent] = []
        self.engine = StreamingEngine(
            self.rt,  # type: ignore[arg-type]
            settings=self.settings,
            on_lifecycle=self.lifecycle.append,
        )
        self._n = 0
        self.message_ids: list[str] = []

    def _frame(self, tag: str) -> str:
        self._n += 1
        return f"f_{tag}_{self._n}_{uuid.uuid4().hex[:6]}"

    async def submit(
        self,
        event: str,
        payload: dict[str, Any],
        *,
        chat: str | None = None,
        turn: str = "",
        seq: int | None = None,
        tag: str = "e",
    ) -> Ack:
        raw: dict[str, Any] = {
            "v": 1,
            "event": event,
            "frame_id": self._frame(tag),
            "chat_id": chat or self.env.chat_id,
            "turn_id": turn,
            "payload": payload,
        }
        if seq is not None:
            raw["seq"] = seq
        ack = await self.engine.submit(raw)
        if ack.message_id:
            self.message_ids.append(ack.message_id)
        return ack

    async def turn_start(
        self, turn: str, *, title: str = "", reply_to: str = "",
        chat: str | None = None, seq: int = 0,
    ) -> Ack:
        payload: dict[str, Any] = {}
        if title:
            payload["title"] = title
        if reply_to:
            payload["reply_to"] = reply_to
        return await self.submit(
            "turn.start", payload, chat=chat, turn=turn, seq=seq, tag="ts",
        )

    async def delta(self, turn: str, text: str, seq: int, chat: str | None = None) -> Ack:
        return await self.submit(
            "answer.delta", {"delta": text}, chat=chat, turn=turn, seq=seq, tag="d",
        )

    async def final(
        self, turn: str, text: str, seq: int, chat: str | None = None,
        stats: dict[str, Any] | None = None,
    ) -> Ack:
        payload: dict[str, Any] = {"text": text}
        if stats:
            payload["stats"] = stats
        return await self.submit(
            "answer.final", payload, chat=chat, turn=turn, seq=seq, tag="fin",
        )

    async def close(self) -> None:
        await self.engine.aclose()
        await self.rt.aclose()


def _ok(ack: Ack, expect: Verdict = Verdict.CONSUMED) -> bool:
    return ack.verdict is expect


async def settle(seconds: float) -> None:
    await asyncio.sleep(seconds)


async def wait_for_close(
    transport: RecordingTransport, before: int, *,
    timeout: float = 10.0, interval: float = 0.5,
) -> int:
    """轮询等待定稿调用（#15）.

    L3 真实 API 单次调用约 1s，固定 settle(1.6) 不够 pause_expire 的
    定稿（timer 1.2s + 定稿全量更新）完成。轮询 ``cardkit_close_streaming``
    计数直至超过 ``before`` 或超时，返回当前计数。
    """
    deadline = time.monotonic() + timeout
    while True:
        count = transport.counts.get("cardkit_close_streaming", 0)
        if count > before or time.monotonic() >= deadline:
            return count
        await asyncio.sleep(interval)


# ── 探针共用：绕过本地预算预检的裸 create ─────────────────────────────────


async def raw_cardkit_create(transport: FeishuTransport, card: dict[str, Any]) -> str:
    """直连 POST /cardkit/v1/cards——刻意旁路 OPT-1c 本地预检.

    §6-5 的测量目的正是探测 27000 以上的真实拒收线，本地拦截必须被绕过。
    """
    data = await transport._request(  # noqa: SLF001 — L3 探针专用
        "POST", "/cardkit/v1/cards",
        json_body={"type": "card_json", "data": json.dumps(card, ensure_ascii=False)},
        operation="cardkit_create[probe]",
    )
    return str((data or {}).get("card_id") or "")


def padded_card(target_bytes: int) -> dict[str, Any]:
    """基准卡 + markdown 填充元素，逼近目标字节数（JSON 开销计入）."""
    card = render_initial_card(RenderState(), DEFAULT_THEME)
    body = card.setdefault("body", {}).setdefault("elements", [])
    overhead = 46  # 元素包裹 JSON 开销（",{}", 引号与 tag 字段）
    pad = max(0, target_bytes - measure_card_bytes(card) - overhead)
    body.append({"tag": "markdown", "content": "P" * pad})
    return card


# ── 场景 ──────────────────────────────────────────────────────────────────


async def sc_connect(d: Driver) -> ScenarioResult:
    r = ScenarioResult("connect")
    mid = await d.rt.send_text_to_chat(
        d.env.chat_id,
        "[L3 连通性锚点] lark-streaming-platform 0.0.1 真火验收开始——本条为原生文本通道探针，可忽略。",
    )
    r.check(bool(mid and mid.startswith("om_")), f"原生文本直发 message_id={mid[:16]}")
    ack = await d.submit(
        "deliver", {"text": "L3 连通性锚点：deliver 静态卡通道正常。", "title": "L3 验收"},
        tag="dl",
    )
    r.check(_ok(ack) and bool(ack.message_id), f"deliver 静态卡 verdict={ack.verdict.value} mid={ack.message_id[:16]}")
    r.info(f"lifecycle={[e.type for e in d.lifecycle]}")
    return r


async def sc_plain(d: Driver) -> ScenarioResult:
    r = ScenarioResult("plain")
    mid = await d.rt.send_text_to_chat(d.env.chat_id, "[plain 通道] 原生文本兜底路径真火验证。")
    r.check(bool(mid), "原生文本直发成功")
    return r


async def sc_answer(d: Driver) -> ScenarioResult:
    r = ScenarioResult("answer")
    turn = f"t_l3_answer_{uuid.uuid4().hex[:6]}"
    a0 = await d.turn_start(turn, title="L3 流式回合")
    r.check(_ok(a0) and bool(a0.message_id), f"turn.start 建卡 verdict={a0.verdict.value}")
    r.check(d.rt.counts.get("cardkit_create", 0) == 1, "恰好一次 cardkit_create")
    r.check(d.rt.counts.get("send_card_by_id_to_chat", 0) == 1, "恰好一次实体卡投递")

    chunks = [
        "## L3 流式验收\n", "打字机通道真火：", "delta 逐帧上行，", "间隔 0.45s，",
        "**markdown** 行内样式，", "`code_span`，", "- 列表项\n", "定稿前最后一帧。",
    ]
    full = ""
    for i, c in enumerate(chunks):
        full += c
        ack = await d.delta(turn, c, seq=i + 1)
        r.check(_ok(ack), f"delta#{i + 1} verdict={ack.verdict.value}")
        await settle(PULSE)

    fin = await d.final(
        turn, full.rstrip("。") + "。", seq=len(chunks) + 1,
        stats={
            "duration_ms": 4200, "model": "l3-probe",
            "context_tokens": {"used": 13312, "max": 128000},
            "extra": {"reasoning_effort": "high"},
        },
    )
    r.check(_ok(fin), f"answer.final verdict={fin.verdict.value} detail={fin.detail}")
    # 澄清窗口 1.2s 到期 → pause_expire 真落定（#15：轮询等待，L3 单次
    # 调用约 1s，固定 settle(1.6) 不够定稿全量更新完成）
    closes = await wait_for_close(
        d.rt, d.rt.counts.get("cardkit_close_streaming", 0),
    )
    r.check(
        closes == 1,
        "窗口到期自动落定（close_streaming 已执行，定稿全量写回）",
    )
    r.info(f"调用序列={d.rt.events}")
    if d.message_ids:
        r.measure("answer_message_id", d.message_ids[0])
    return r


async def sc_progress(d: Driver) -> ScenarioResult:
    r = ScenarioResult("progress")
    turn = f"t_l3_progress_{uuid.uuid4().hex[:6]}"
    a0 = await d.turn_start(turn, title="L3 面板通道")
    r.check(_ok(a0), "turn.start 建卡")

    ack = await d.submit(
        "tool.start", {"tool": "web_search", "preview": "query=L3 真火探针"},
        turn=turn, seq=1, tag="tls",
    )
    r.check(_ok(ack), f"tool.start verdict={ack.verdict.value}")
    await settle(0.4)
    ack = await d.submit(
        "tool.end", {"tool": "web_search", "duration_ms": 1234, "output_preview": "3 hits"},
        turn=turn, seq=2, tag="tle",
    )
    r.check(_ok(ack), f"tool.end verdict={ack.verdict.value}")
    await settle(0.4)
    for i, t in enumerate(["第一轮推理：", "检查面板行装配…"]):
        ack = await d.submit("thinking.delta", {"text": t}, turn=turn, seq=3 + i, tag="th")
        r.check(_ok(ack), f"thinking.delta#{i + 1} verdict={ack.verdict.value}")
        await settle(0.35)
    ack = await d.submit("thinking.round.end", {"round": 1, "duration_ms": 800}, turn=turn, seq=5, tag="thre")
    r.check(_ok(ack), f"thinking.round.end verdict={ack.verdict.value}")
    await settle(0.35)
    ack = await d.submit("interim", {"text": "中间结论：面板通道全通。"}, turn=turn, seq=6, tag="in")
    r.check(_ok(ack), f"interim verdict={ack.verdict.value}")
    await settle(0.35)
    ack = await d.submit("notice", {"text": "L3 通知行探针（warn 级）", "level": "warn"}, turn=turn, seq=7, tag="no")
    r.check(_ok(ack), f"notice verdict={ack.verdict.value}")
    await settle(0.4)

    ack = await d.delta(turn, "面板回合的正文答案（与懒面板对照：本回合有面板行）。", seq=8)
    r.check(_ok(ack), "answer.delta verdict=consumed")
    fin = await d.final(turn, "面板回合的正文答案（与懒面板对照：本回合有面板行）。", seq=9)
    r.check(_ok(fin), f"answer.final verdict={fin.verdict.value}")
    await settle(1.6)  # 窗口到期真落定
    r.info(f"update={d.rt.counts.get('cardkit_update', 0)} batch={d.rt.counts.get('cardkit_batch_update', 0)} close={d.rt.counts.get('cardkit_close_streaming', 0)}")
    return r


async def sc_full(d: Driver) -> ScenarioResult:
    r = ScenarioResult("full")
    turn = f"t_l3_full_{uuid.uuid4().hex[:6]}"
    a0 = await d.turn_start(turn, title="L3 全时序回合")
    r.check(_ok(a0) and bool(a0.message_id), "turn.start 建卡")
    await settle(0.3)
    ack = await d.submit("tool.start", {"tool": "run_command", "preview": "pytest -q"}, turn=turn, seq=1, tag="tls")
    r.check(_ok(ack), "tool.start")
    await settle(0.4)
    ack = await d.submit("tool.end", {"tool": "run_command", "duration_ms": 2300, "output_preview": "423 passed"}, turn=turn, seq=2, tag="tle")
    r.check(_ok(ack), "tool.end")
    await settle(0.4)
    ack = await d.submit("thinking.delta", {"text": "全时序：工具后接流式正文…"}, turn=turn, seq=3, tag="th")
    r.check(_ok(ack), "thinking.delta")
    await settle(0.35)
    parts = ["### 全时序回合\n", "工具面板 + 打字机 + 页脚 ", "**stats 全字段** ", "同回合真火。"]
    text = ""
    for i, c in enumerate(parts):
        text += c
        ack = await d.delta(turn, c, seq=4 + i)
        r.check(_ok(ack), f"delta#{i + 1}")
        await settle(PULSE)
    fin = await d.final(
        turn, text, seq=8,
        stats={"duration_ms": 5600, "model": "l3-probe", "context_tokens": {"used": 24000, "max": 200000}},
    )
    r.check(_ok(fin), f"answer.final verdict={fin.verdict.value}")
    await settle(1.6)  # 窗口到期真落定
    r.info(f"close_streaming={d.rt.counts.get('cardkit_close_streaming', 0)}（定稿全量写回，#14）")
    return r


async def sc_burst(d: Driver) -> ScenarioResult:
    r = ScenarioResult("burst")
    turn = f"t_l3_burst_{uuid.uuid4().hex[:6]}"
    a0 = await d.turn_start(turn, title="L3 建卡窗口并发")
    r.check(_ok(a0), "turn.start（建卡窗口自然挂起 0.5~1s，Q3）")
    # 建卡窗口内并发齐发 6 帧（10-12「一轮 3 卡」事故守卫的真火回归）
    tasks = [
        d.submit("answer.delta", {"delta": f"burst#{i} "}, turn=turn, seq=i + 1, tag="bd")
        for i in range(6)
    ]
    acks = await asyncio.gather(*tasks)
    r.check(
        all(_ok(a) for a in acks),
        f"6 并发帧 verdict 全 consumed（实得 {[a.verdict.value for a in acks]}）",
    )
    r.check(d.rt.counts.get("cardkit_create", 0) == 1, f"恰一张卡（create={d.rt.counts.get('cardkit_create', 0)}）")
    r.check(d.rt.counts.get("send_card_by_id_to_chat", 0) == 1, "恰一次投递")
    await settle(0.5)
    fin = await d.final(turn, "burst#0 burst#1 burst#2 burst#3 burst#4 burst#5 并发收敛完成。", seq=7)
    r.check(_ok(fin), "final 落定")
    return r


async def sc_clarify(d: Driver) -> ScenarioResult:
    r = ScenarioResult("clarify")
    turn = f"t_l3_clarify_{uuid.uuid4().hex[:6]}"
    a0 = await d.turn_start(turn, title="L3 澄清窗口")
    r.check(_ok(a0), "turn.start 建卡")
    for i, c in enumerate(["澄清探针正文开始。", "请求澄清前缓冲两帧。"]):
        await d.delta(turn, c, seq=i + 1)
        await settle(PULSE)
    fin = await d.final(turn, "澄清探针正文开始。请求澄清前缓冲两帧。", seq=3)
    r.check(_ok(fin) and fin.detail == "seal_window_open", f"final 进窗口 detail={fin.detail}")
    ack = await d.submit("clarify.request", {"form": {
        "kind": "select", "question": "L3 澄清探针：这张卡真实可点吗？",
        "options": ["选项 A", "选项 B"],
    }}, turn=turn, seq=4, tag="cr")
    r.check(_ok(ack), f"clarify.request → PAUSED verdict={ack.verdict.value}")
    r.check(d.rt.counts.get("cardkit_create", 0) == 2, "澄清独立卡已建（create=2）")
    r.info(">>> 飞书聊天里现在有一张真实可点的澄清卡（6s 观察窗）；回调接线归适配层（R4 MANUAL）")
    await settle(6.0)
    ack = await d.submit("clarify.answer", {"values": {"l3_probe": "manual_click_or_simulated"}}, turn=turn, seq=5, tag="ca")
    r.check(_ok(ack), f"clarify.answer → resume verdict={ack.verdict.value}")
    await settle(0.4)
    ack = await d.delta(turn, "澄清后续流正文。", seq=6)
    r.check(_ok(ack), "resume 后续流")
    fin = await d.final(turn, "澄清探针正文开始。请求澄清前缓冲两帧。澄清后续流正文。", seq=7)
    r.check(_ok(fin), f"resume 后第二次 final verdict={fin.verdict.value} detail={fin.detail}")
    # 语义定案（guards.py）：clarify_window_sec>0 时 STREAMING 中的每次 final
    # 都进窗口；无 clarify 命中则 1.2s 后 pause_expire 真落定——此处验证该
    # 自动落定链路真实可达（#15：轮询等待）。
    close_before = d.rt.counts.get("cardkit_close_streaming", 0)
    closes = await wait_for_close(d.rt, close_before)
    r.check(
        closes > close_before,
        "窗口到期 pause_expire 自动落定（close_streaming 增量出现）",
    )
    r.measure("clarify", "MANUAL-R4: 回调授权码集需平台回调接线实测（本脚本投递真实可点卡）")
    return r


async def sc_reply(d: Driver) -> ScenarioResult:
    r = ScenarioResult("reply")
    # 伪锚点先行探针：直接用 transport 撞真实拒绝码（R3 佐证）
    probe_card = await d.rt.cardkit_create(render_initial_card(RenderState(), DEFAULT_THEME))
    try:
        await d.rt.reply_card_by_id("om_fake_l3_probe_000000", probe_card)
        r.info("伪锚点 reply 意外成功（与冻结知识不符，需复核）")
    except Exception as exc:  # noqa: BLE001
        code = getattr(exc, "code", 0)
        r.info(f"伪锚点 reply 被拒 code={code}（预期 99992354 族）")
        r.measure("reply_anchor_invalid_code", str(code))

    anchor = await d.rt.send_text_to_chat(
        d.env.chat_id,
        "[L3 锚点] 这是回复引用的锚点消息（模拟用户提问），卡片将以回复形态挂在它下面——可忽略本条。",
    )
    r.check(bool(anchor), f"锚点消息发出 mid={anchor[:16]}")
    counts0 = dict(d.rt.counts)  # 分段计数基线（伪锚点探针已污染全局限计数）
    turn = f"t_l3_reply_{uuid.uuid4().hex[:6]}"
    a0 = await d.turn_start(turn, title="L3 回复形态", reply_to=anchor)
    r.check(_ok(a0), f"reply 形态建卡 verdict={a0.verdict.value}")
    reply_delta = d.rt.counts.get("reply_card_by_id", 0) - counts0.get("reply_card_by_id", 0)
    send_delta = d.rt.counts.get("send_card_by_id_to_chat", 0) - counts0.get("send_card_by_id_to_chat", 0)
    r.check(reply_delta == 1 and send_delta == 0, f"恰一次 reply 锚定（reply+{reply_delta} send+{send_delta}）")
    await d.delta(turn, "回复形态卡流式正文。", seq=1)
    await settle(PULSE)
    await d.final(turn, "回复形态卡流式正文。", seq=2)
    await settle(1.6)

    # 伪锚点引擎路径：Q10 降级直发（内容不丢）——mock 无 99992354 语义，跳过
    if d.env.mock:
        r.info("--mock 无锚点失效语义，伪锚点降级断言跳过（结论以真火为准）")
    else:
        turn2 = f"t_l3_reply_fake_{uuid.uuid4().hex[:6]}"
        counts1 = dict(d.rt.counts)
        a1 = await d.turn_start(turn2, title="L3 伪锚点降级", reply_to="om_fake_l3_probe_deadbeef")
        r.check(_ok(a1), f"伪锚点 turn.start verdict={a1.verdict.value}（Q10 降级直发）")
        send2 = d.rt.counts.get("send_card_by_id_to_chat", 0) - counts1.get("send_card_by_id_to_chat", 0)
        r.check(send2 == 1, "降级直发通道恰用一次")
        await d.final(turn2, "伪锚点回合：锚定失败直发，内容零丢失。", seq=1)
        await settle(1.6)

    if d.env.dm_chat_id:
        anchor_dm = await d.rt.send_text_to_chat(d.env.dm_chat_id, "[L3 锚点/私聊] 私聊 reply 双验证锚点。")
        turn3 = f"t_l3_reply_dm_{uuid.uuid4().hex[:6]}"
        a2 = await d.turn_start(turn3, title="L3 私聊回复", reply_to=anchor_dm, chat=d.env.dm_chat_id)
        r.check(_ok(a2), f"私聊 reply 建卡 verdict={a2.verdict.value}")
        await d.submit(
            "answer.delta", {"delta": "私聊回复形态正文。"},
            chat=d.env.dm_chat_id, turn=turn3, seq=1, tag="d",
        )
        await d.submit(
            "turn.abort", {"reason": "dm-probe-done"},
            chat=d.env.dm_chat_id, turn=turn3, tag="ab",
        )
        r.measure("reply_anchor_dm", "verified" if _ok(a2) else "failed")
        await settle(1.0)
    else:
        r.info("未设 FEISHU_E2E_DM_CHAT_ID，私聊双验证跳过（R3 群聊已验）")
        r.measure("reply_anchor_dm", "skipped")
    return r


async def sc_renew(d: Driver, *, long_mode: bool) -> ScenarioResult:
    r = ScenarioResult("renew")
    turn = f"t_l3_renew_{uuid.uuid4().hex[:6]}"
    a0 = await d.turn_start(turn, title="L3 会话续期探针")
    r.check(_ok(a0), "turn.start 建卡")
    await d.delta(turn, "续期探针：先落一帧。", seq=1)
    await settle(PULSE)
    # 官方恢复动作直探：独立探针卡 reopen（#14：改走全量更新，传入流式卡）
    t0 = time.monotonic()
    try:
        pc = await d.rt.cardkit_create(render_initial_card(RenderState(), DEFAULT_THEME))
        await d.rt.cardkit_reopen_streaming(
            pc, render_full_card(RenderState(), DEFAULT_THEME), sequence=99,
        )
        elapsed = time.monotonic() - t0
        r.check(True, f"reopen 官方动作可达（独立探针卡，{elapsed:.2f}s）")
        r.measure("reopen_probe_sec", f"{elapsed:.2f}")
    except Exception as exc:  # noqa: BLE001
        r.check(False, f"reopen 探针失败 code={getattr(exc, 'code', 0)} msg={exc}")
    ack = await d.final(turn, "续期探针：先落一帧。探针卡不影响本回合。", seq=2)
    r.check(_ok(ack), "final 落定")

    if long_mode:
        r.info("--long：session_renew_age_sec=60（钳制下限），12s/帧 × 8 帧观察主动续期")
        d2 = Driver(d.env, d.rt._inner, settings=EngineSettings(session_renew_age_sec=60.0))  # noqa: SLF001
        turn2 = f"t_l3_renew_long_{uuid.uuid4().hex[:6]}"
        a = await d2.turn_start(turn2, title="L3 主动续期真时序")
        r.check(_ok(a), "long turn.start")
        ok_renew = False
        for i in range(8):
            await settle(12.0)
            ack = await d2.delta(turn2, f"长回合帧#{i + 1}（age≈{(i + 1) * 12}s）。", seq=i + 1)
            if not _ok(ack):
                r.check(False, f"长回合帧#{i + 1} verdict={ack.verdict.value}（200850 中断？）")
                break
            if d2.rt.counts.get("cardkit_reopen_streaming", 0) >= 1:
                ok_renew = True
        r.check(ok_renew, "主动续期触发（reopen 出现于帧流中，无 200850 中断）")
        r.measure("t_renew_long", "verified" if ok_renew else "not-observed")
        fin = await d2.final(turn2, "长回合续期真时序完成。", seq=9)
        r.check(_ok(fin), "long final 落定")
        await d2.close()
    else:
        r.info("未加 --long：60s+ 主动续期真时序跳过（L2 已验，真火可后补）")
        r.measure("t_renew_long", "skipped")
    return r


async def sc_budget(d: Driver) -> ScenarioResult:
    r = ScenarioResult("budget")
    if d.env.mock:
        r.info("--mock 无 200860 拒收语义，阶梯测量跳过（结论以真火为准）")
        r.measure("byte_budget_real", "mock-skipped")
        return r
    # 0) 常规路径（含本地预检）sanity：26.5KB 应通过
    sane = padded_card(26_500)
    try:
        cid = await d.rt.cardkit_create(sane)
        r.check(bool(cid), f"常规 create 26.5KB 通过（本地口径 {measure_card_bytes(sane)}B）")
    except Exception as exc:  # noqa: BLE001
        r.check(False, f"常规 create 26.5KB 失败 code={getattr(exc, 'code', 0)}")
    # 1) 阶梯（旁路本地预检，测真实拒收线）
    # 首轮真火实测（2026-11-20）：39990B 仍被接受——官方文档 30KB 上限与
    # 实测严重不符，阶梯拉至 100KB 找真实拒收线（§6-5 的目的正在于此）。
    ladder = [45_000, 50_000, 55_000, 60_000, 70_000, 80_000, 100_000]
    accepted: list[int] = []
    rejected: dict[int, int] = {}
    for size in ladder:
        card = padded_card(size)
        actual = measure_card_bytes(card)
        try:
            await raw_cardkit_create(d.rt._inner, card)  # noqa: SLF001
            accepted.append(actual)
            r.info(f"create {actual}B → 接受")
        except Exception as exc:  # noqa: BLE001
            code = int(getattr(exc, "code", 0) or 0)
            rejected[actual] = code
            r.info(f"create {actual}B → 拒收 code={code}")
        await settle(0.3)
    if rejected:
        r.check(all(c in (200860, 230099) for c in rejected.values()), f"拒收码形态={set(rejected.values())}（预期 200860 族）")
    else:
        r.info("拒收线未在本阶梯出现（见下方事实记录）——不是缺陷，是测量发现")
    if rejected and not accepted:
        # 阶梯全拒：说明真实线低于阶梯下限——不可能（26.5KB 已过），防御分支
        r.measure("byte_budget_real", f"all-rejected>={min(rejected)}（异常形态，需人工复核）")
    elif rejected:
        lo, hi = max(accepted), min(rejected)
        r.measure("byte_budget_real", f"accept<={lo} reject>={hi}（浮动带 {hi - lo}B；冻结值 27000 的裕度区间）")
        # 2) 512B 步进细化
        cur, hi_v = lo, hi
        while hi_v - cur > 512:
            mid = (cur + hi_v) // 2
            card = padded_card(mid)
            actual = measure_card_bytes(card)
            try:
                await raw_cardkit_create(d.rt._inner, card)  # noqa: SLF001
                cur = actual
            except Exception:  # noqa: BLE001
                hi_v = actual
            await settle(0.25)
        r.measure("byte_budget_refined", f"accept<={cur} reject>={hi_v}")
        _logger.warning("budget refine: accept<=%d reject>=%d", cur, hi_v)
    else:
        # 整条阶梯（含 100KB）全被接受：真实拒收线高于测量上限——按事实记录，
        # 官方文档 30KB 口径与 CardKit 实体卡 create 实测不符（冻结知识待修正）。
        r.info(f"阶梯全接受至 {max(accepted)}B：create 路径未见拒收线（口径与官方文档不符，按事实记录）")
        r.measure("byte_budget_real", f"accept<={max(accepted)} no-reject-found")
    # 3) 流式内容 / 全量更新上限（200860 猎场迁移测量——2026-11-20 真火发现
    #    体积拒收不在 create 路径，而 stream content 上限以 99992402 字段校验
    #    形态出现在 100~120KB 之间）
    probe_card = await d.rt.cardkit_create(render_initial_card(RenderState(), DEFAULT_THEME))
    nxt = 1
    for kb in (30, 60, 100, 120):
        nxt += 10
        try:
            await d.rt.cardkit_stream_element(
                probe_card, ANSWER_ELEMENT_ID, "X" * (kb * 1000), sequence=nxt,
            )
            r.info(f"stream_element {kb}KB → 接受")
        except Exception as exc:  # noqa: BLE001
            r.info(f"stream_element {kb}KB → 拒 code={getattr(exc, 'code', 0)} msg={str(exc)[:60]}")
    for kb in (35, 100):
        card = render_initial_card(RenderState(), DEFAULT_THEME)
        card.setdefault("body", {}).setdefault("elements", []).append(
            {"tag": "markdown", "content": "P" * (kb * 1000 - measure_card_bytes(card) - 46)}
        )
        nxt += 10
        try:
            # 旁路本地预算预检（测量目的：服务端真实上限；与 raw_cardkit_create 同理）
            await d.rt._inner._request(  # noqa: SLF001
                "PUT", f"/cardkit/v1/cards/{probe_card}",
                json_body={
                    "card": {"type": "card_json", "data": json.dumps(card, ensure_ascii=False)},
                    "sequence": nxt,
                },
                operation="cardkit_update[probe]",
            )
            r.info(f"cardkit_update {measure_card_bytes(card) // 1000}KB → 接受")
        except Exception as exc:  # noqa: BLE001
            r.info(f"cardkit_update {measure_card_bytes(card) // 1000}KB → 拒 code={getattr(exc, 'code', 0)} msg={str(exc)[:60]}")
    return r


async def sc_ratelimit(d: Driver) -> ScenarioResult:
    r = ScenarioResult("ratelimit")
    pc = await d.rt.cardkit_create(render_initial_card(RenderState(), DEFAULT_THEME))
    token = await d.rt._inner._token.get_token()  # noqa: SLF001 — 探针需原始 HTTP 面
    url = f"{d.env.base_url}/cardkit/v1/cards/{pc}/elements/{ANSWER_ELEMENT_ID}/content"

    async def _one(i: int, seq: int | None = None) -> dict[str, Any]:
        hc = d.probe_client or httpx.AsyncClient(timeout=15.0)
        try:
            body_json: dict[str, Any] = {"content": f"rate probe {i}", "sequence": seq if seq is not None else 2000 + i}
            resp = await hc.put(
                url,
                headers={"Authorization": f"Bearer {token}"},
                json=body_json,
            )
            try:
                body = resp.json()
            except ValueError:
                body = {}
            interesting = {
                k.lower(): v for k, v in resp.headers.items()
                if "retry" in k.lower() or "rate" in k.lower() or "limit" in k.lower()
            }
            return {
                "status": resp.status_code,
                "code": body.get("code"),
                "msg": str(body.get("msg", ""))[:160],
                "retry_after": resp.headers.get("Retry-After", ""),
                "headers": interesting,
            }
        finally:
            if d.probe_client is None:
                await hc.aclose()

    # A 档：并发齐发（同元素不同 seq）——真实形态是服务端 seq 仲裁（300317），
    # 不是限频；此档用于诚实记录并发噪声形态。
    results = await asyncio.gather(*[_one(i) for i in range(14)])
    codes: dict[Any, int] = {}
    for x in results:
        codes[x["code"]] = codes.get(x["code"], 0) + 1
    r.info(f"A 档 14 连发（并发）分布={ {str(k): v for k, v in codes.items()} }（300317 = seq 仲裁噪声）")

    # B 档：顺序高速（seq 严格递增、零间隔、等响应）——单卡 10 次/秒限的
    # 正确探针形态；99991400 若触发，捕原始 HTTP 面（状态码/Retry-After/响应体）。
    seq_hits: list[dict[str, Any]] = []
    t0 = time.monotonic()
    seqv = 3000
    for i in range(25):
        try:
            await d.rt.cardkit_stream_element(pc, ANSWER_ELEMENT_ID, f"seq-speed {i}", sequence=seqv)
            seqv += 1
        except Exception as exc:  # noqa: BLE001 — 记录非限频错误但不中断
            code = int(getattr(exc, "code", 0) or 0)
            if code == 300317:
                seqv += 1  # 自愈语义：对齐服务端 +1
                continue
            seq_hits.append({"i": i, "code": code, "msg": str(exc)[:120]})
            break
    elapsed = time.monotonic() - t0
    r.info(f"B 档 25 连发（顺序高速）耗时 {elapsed:.2f}s ≈ {25 / max(elapsed, 0.01):.1f} 次/s，异常={seq_hits or '无'}")

    # C 档：原始 HTTP 面顺序爆发（绕过 transport 重试，捕 Retry-After）
    raw_hits: list[dict[str, Any]] = []
    seqv = 5000
    for i in range(30):
        x = await _one(i, seq=seqv)
        if x["code"] == 0:
            seqv += 1
        elif x["code"] == 300317:
            seqv += 1
        else:
            raw_hits.append(x)
            if len(raw_hits) >= 3:
                break
    limited = [x for x in raw_hits if x["code"] == 99991400 or x["status"] == 429]
    if limited:
        sample = limited[0]
        r.check(True, f"限频触发：http={sample['status']} code={sample['code']} retry_headers={sample['headers'] or '无'}")
        r.measure("rate_limit_body", f"http={sample['status']} code={sample['code']} msg={sample['msg'][:80]}")
        r.measure("rate_limit_retry_after", sample["retry_after"] or "HEADER-ABSENT")
    else:
        r.info(f"A/B/C 三档均未触发 99991400（本次配额余裕；C 档原始面异常={[(x['status'], x['code']) for x in raw_hits[:3]] or '无'}）——按『本次未触发』诚实记录")
        r.measure("rate_limit_body", "not-triggered")
        r.measure("rate_limit_retry_after", "not-triggered")
    return r


async def sc_seqconflict(d: Driver) -> ScenarioResult:
    r = ScenarioResult("seqconflict")
    pc = await d.rt.cardkit_create(render_initial_card(RenderState(), DEFAULT_THEME))

    async def stream(seq: int, content: str = "probe") -> None:
        await d.rt.cardkit_stream_element(pc, ANSWER_ELEMENT_ID, content, sequence=seq)

    # 重放：同号重发（冻结知识：重放/回退 seq 必现冲突）
    replay_code = 0
    try:
        await stream(1, "first")
        await stream(1, "replay-same")
        r.info("同号重放未被拒（与冻结知识不符，需复核）")
    except Exception as exc:  # noqa: BLE001
        replay_code = int(getattr(exc, "code", 0) or 0)
        r.info(f"同号重放被拒 code={replay_code}（预期 300317）")
    r.measure("seq_replay_code", str(replay_code))
    # 跳号：+1000 直接前跳（冻结知识：跳号允许）
    try:
        await stream(1000, "far-jump")
        r.info("跳号 +1000 接受（跳号允许实证）")
        r.measure("seq_far_jump", "accepted")
    except Exception as exc:  # noqa: BLE001
        r.measure("seq_far_jump", f"rejected:{getattr(exc, 'code', 0)}")
        r.info(f"跳号被拒 code={getattr(exc, 'code', 0)}（与冻结知识不符，需复核）")
    # 最小重开间隔：close → 立即 reopen → stream（#14：两者均改走全量更新）
    t0 = time.monotonic()
    try:
        await d.rt.cardkit_close_streaming(
            pc, render_sealed_card(RenderState(), DEFAULT_THEME), sequence=1001,
        )
        await d.rt.cardkit_reopen_streaming(
            pc, render_full_card(RenderState(), DEFAULT_THEME), sequence=1002,
        )
        await stream(1003, "after-close-reopen")
        elapsed = time.monotonic() - t0
        r.check(True, f"close→reopen→stream 最小链 {elapsed:.2f}s 可达")
        r.measure("reopen_min_interval_sec", f"{elapsed:.2f}")
    except Exception as exc:  # noqa: BLE001
        r.check(False, f"close→reopen 链失败 code={getattr(exc, 'code', 0)}（300309 族？）")
        r.measure("reopen_min_interval_sec", f"failed:{getattr(exc, 'code', 0)}")
    return r


# ── 编排 ──────────────────────────────────────────────────────────────────

SCENARIOS: dict[str, Any] = {
    "connect": sc_connect,
    "plain": sc_plain,
    "answer": sc_answer,
    "progress": sc_progress,
    "full": sc_full,
    "burst": sc_burst,
    "clarify": sc_clarify,
    "reply": sc_reply,
    "renew": sc_renew,
    "budget": sc_budget,
    "ratelimit": sc_ratelimit,
    "seqconflict": sc_seqconflict,
}

DEFAULT_SET = ["connect", "answer", "progress", "full", "burst", "clarify", "reply", "renew", "budget"]


async def run(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="L3 真飞书验收（lark-streaming-platform 0.0.1）")
    ap.add_argument("--scenario", default=",".join(DEFAULT_SET), help="逗号分隔场景名")
    ap.add_argument("--list", action="store_true", help="列出场景后退出")
    ap.add_argument("--long", action="store_true", help="renew 加跑 60s+ 主动续期真时序")
    ap.add_argument("--probe-rate-limit", action="store_true", help="§6-1 限频爆发探针（破坏性）")
    ap.add_argument("--probe-seq", action="store_true", help="§6-2 sequence 行为探针（破坏性）")
    ap.add_argument("--mock", action="store_true", help="L2 台架干跑（MockFeishuREST；零真实写操作，不需凭证）")
    args = ap.parse_args(argv)
    if args.list:
        print("场景：", ", ".join(SCENARIOS))
        print("默认集：", ", ".join(DEFAULT_SET))
        return 0
    if args.mock:
        from tests.harness.mock_feishu import MockFeishuREST

        env = Env(
            app_id="cli_e2e_mock", app_secret="mock-secret",
            chat_id="oc_mock_chat", dm_chat_id="oc_mock_dm",
            base_url="https://open.feishu.cn/open-apis", mock=True,
        )
        mock_server = MockFeishuREST()
        http = httpx.AsyncClient(
            timeout=15.0, transport=httpx.MockTransport(mock_server),
        )
        print("=== MOCK 干跑（零真实写操作；语义结论以真火为准）===")
    else:
        env = Env.load()
        http = httpx.AsyncClient(timeout=15.0)

    names = [s.strip() for s in args.scenario.split(",") if s.strip()]
    if args.probe_rate_limit and "ratelimit" not in names:
        names.append("ratelimit")
    if args.probe_seq and "seqconflict" not in names:
        names.append("seqconflict")
    unknown = [n for n in names if n not in SCENARIOS]
    if unknown:
        print(f"未知场景：{unknown}；可用：{list(SCENARIOS)}")
        return 2

    creds = FeishuCredentials(env.app_id, env.app_secret, base_url=env.base_url)
    transport = FeishuTransport(creds, http=http)

    if not env.mock:
        print("=== lark-streaming-platform 0.0.1 · L3 真飞书验收 ===")
    print(f"站点={env.base_url}  群聊={env.chat_id[:12]}…  私聊={'有' if env.dm_chat_id else '未设'}")
    print(f"场景序列：{names}\n")
    failures = 0
    for name in names:
        driver = Driver(env, transport, probe_client=http)
        print(f"─── [{name}] " + "─" * max(4, 52 - len(name)))
        t0 = time.monotonic()
        try:
            fn = SCENARIOS[name]
            if name == "renew":
                res = await fn(driver, long_mode=args.long)  # type: ignore[call-arg]
            else:
                res = await fn(driver)  # type: ignore[call-arg,misc]
        except Exception as exc:  # noqa: BLE001 — 场景异常不阻断后续场景
            res = ScenarioResult(name)
            res.check(False, f"场景异常：{type(exc).__name__}: {exc}")
        await driver.close()
        RESULTS.append(res)
        status = "SKIP" if res.skipped else ("PASS" if res.ok else "FAIL")
        if not res.ok:
            failures += 1
        print(f"  ⇒ {status}（{time.monotonic() - t0:.1f}s）")
        for line in res.notes:
            print(line)
        print()
        await settle(SCENARIO_GAP)

    await transport.aclose()
    await http.aclose()

    # §6 五项待实测汇总
    print("=== 04-knowledge-freeze §6 待实测补充项 · 现场测量汇总 ===")
    got: dict[str, str] = {}
    for res in RESULTS:
        got.update(res.measurements)
    rows = [
        ("1. 99991400 响应体 / Retry-After", got.get("rate_limit_body", "未测（--probe-rate-limit）"),
         got.get("rate_limit_retry_after", "—")),
        ("2. 300317 重放码 / reopen 最小间隔", got.get("seq_replay_code", "未测（--probe-seq）"),
         got.get("reopen_min_interval_sec", got.get("reopen_probe_sec", "—"))),
        ("3. send_card_by_id 锚定（群/私）", got.get("reply_anchor_invalid_code", "—"),
         got.get("reply_anchor_dm", "skipped")),
        ("4. clarify 回调授权失败码集", got.get("clarify", "MANUAL（需平台回调接线）"), "—"),
        ("5. 字节预算真实浮动", got.get("byte_budget_real", "未测"), got.get("byte_budget_refined", "—")),
    ]
    for title, a, b in rows:
        print(f"  {title}: {a}  |  {b}")
    print()
    print(f"=== 结果：{len(RESULTS) - failures}/{len(RESULTS)} 场景通过，{failures} 失败 ===")
    return 1 if failures else 0


def main() -> None:
    try:
        code = asyncio.run(run())
    except KeyboardInterrupt:
        code = 130
    raise SystemExit(code)


if __name__ == "__main__":
    main()
