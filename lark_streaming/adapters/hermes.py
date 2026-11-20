"""hermes 形态 A 适配器 — 出站钩子翻译面（05-adapters §2/§5.1）.

**脚手架定位（ADR D8）**：本模块是 hermes 的接入翻译面——把 hermes
网关出站流（send/edit/interim/notify 元数据契约，前代 v0.21.x 官方
metadata）翻译为 LSEP/1 事件流投递引擎，并把逐帧回执聚合为
:class:`DeliveryResult` 供 hermes 履行降级义务（§1.4：fallback 必
原生投递）。

不做继承（§5.1）：前代继承内置 ``FeishuAdapter`` 的入站能力留在
hermes 侧不动；本适配器只挂出站钩子——hermes 网关在其出站路径
（adapter.send / adapter.edit_message / send_clarify）调用本模块的
对应方法即可，零 hermes 私有 API 依赖（前代 compat 的内置类重建
glue 不进本仓库；hermes 侧单点接线示例见 docs/ADAPTER_SDK.md）。

映射表（05-adapters §5.1 冻结契约）：
- ``expect_edits=True`` 出站 → ``turn.start`` + ``answer.snapshot`` 序列
  （回合首帧语义：活跃回合在场 = 新回合越权，引擎 seal_and_create
  兜底旧卡真落定——前代 10-11 孤儿卡事故的协议化根治）
- 编辑流后续 → ``answer.snapshot``；final → ``answer.final``
- ``_interim_send`` 进度行 → ``tool.*`` / ``thinking.*`` / ``interim``
  （经 ``_legacy_text`` 分类器，前代文本启发式语义逐点对齐；多行
  accumulate 快照按段拆分，思考行与工具行交错处闭合思考轮）
- ``notify=True`` → ``answer.final``（无活跃回合 = 微回合自愈：
  turn.start + final，短答案直落定）
- 无标记发送 → ``deliver``（回合外旁路静态卡）
- 平台通知（⚠️ 子代理失败/超时前缀、后台通告清单）→ ``notice``；
  **无活跃回合永不建卡**（前代跑马灯根因防线，v2.0.8/34 语义）→ 原生投递

降级履行（§1.4）：任一内容帧 verdict ∈ {fallback, degraded} →
``DeliveryResult.action="native"`` + ``native_text``——hermes 必须
用原生通道投递该文本。消息永不丢由两侧共同保证。

回合前暂存（前代 pending 语义）：进度行先于首个流式帧到达时（agent
干活阶段）在适配器侧行缓冲（上限 :attr:`pending_max`，与引擎
CREATING 暂存 ``pending_max=64`` 对齐；answer 帧永不进缓冲也永不丢），
turn.start 后按序并入新卡。
"""

from __future__ import annotations

import logging
import uuid
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import Any

from ..engine import Ack, StreamingEngine, Verdict
from . import _legacy_text as lt
from ._envelope import EnvelopeBuilder, TurnRegistry, TurnRef

_logger = logging.getLogger("lark_streaming.adapters.hermes")

__all__ = ["DeliveryResult", "NativeAction", "HermesAdapter"]

#: Q9 去重集合容量（超限停用本回合去重，防御长回合内存增长）
_SEEN_PROGRESS_MAX = 256


class NativeAction(str, Enum):
    """聚合裁决动作（hermes 出站钩子的履行指令）."""

    CONSUMED = "consumed"  # 全部内容帧已被卡片管线消费——无需原生动作
    NATIVE = "native"      # 至少一帧未消费——必须原生投递 native_text（§1.4）


@dataclass(frozen=True, slots=True)
class FrameVerdict:
    """逐帧回执保真记录（05-adapters §1 红线：不得假设 ack 时序）."""

    frame_id: str
    event: str
    verdict: str
    detail: str = ""


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    """一次出站调用的聚合结论（hermes 据此履行降级义务）.

    ``action=NATIVE`` 时 ``native_text`` 是必须原生投递的文本：
    answer 流帧 = 该帧时刻的全文快照（hermes 可结合 ``turn_streamed``
    决定全文重发或续发——卡上已有部分内容时由宿主策略裁决）。
    控制帧（turn.abort / clarify.answer）的 fallback 无原生履行义务
    （非内容帧），action 恒 CONSUMED、detail 保真携带。
    """

    action: NativeAction
    turn_id: str = ""
    native_text: str = ""
    detail: str = ""
    frames: tuple[FrameVerdict, ...] = ()
    dropped_progress: int = 0          # pre-turn 缓冲溢出丢弃的进度行数
    turn_streamed: bool = False        # 本回合是否已有内容上卡（原生策略线索）
    card_active: bool = False          # 调用时是否存在适配器侧活跃回合


class HermesAdapter:
    """hermes 出站钩子翻译面（形态 A：进程内直调 engine.submit）.

    典型接线（hermes 网关出站路径；docs/ADAPTER_SDK.md §2 完整示例）::

        adapter = HermesAdapter(engine)
        # 网关 send 钩子：
        res = await adapter.send(chat_id, content, metadata)
        if res.action is NativeAction.NATIVE:
            await gateway_native_send(chat_id, res.native_text)  # 降级履行
    """

    def __init__(
        self,
        engine: StreamingEngine,
        *,
        pending_max: int = 64,
        frame_prefix: str = "hf",
        turn_prefix: str = "ht",
        clock: "callable | None" = None,  # type: ignore[valid-type]
    ) -> None:
        self._engine = engine
        self._pending_max = max(8, int(pending_max))
        self._turns = TurnRegistry()
        self._frames = EnvelopeBuilder(prefix=frame_prefix, clock=clock)
        self._turn_clock = clock or (lambda: 0.0)
        self._turn_counter = 0
        self._pending: dict[str, deque[tuple[str, dict[str, Any]]]] = {}
        self._dropped: dict[str, int] = {}

    # ── 公开出站钩子（hermes 元数据契约）────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        metadata: dict[str, Any] | None = None,
        *,
        title: str = "",
        reply_to: str = "",
        stats: dict[str, Any] | None = None,
        turn_id: str | None = None,
    ) -> DeliveryResult:
        """hermes ``adapter.send(chat_id, content, metadata)`` 出站钩子."""
        meta = metadata or {}
        if not title:
            title = str(meta.get("title") or "")
        if not reply_to:
            reply_to = str(meta.get("reply_to") or "")
        result = lt.classify_send(content, metadata)
        channel = result.channel
        if channel == lt.CH_STREAM:
            return await self._send_stream(chat_id, content, title=title, reply_to=reply_to,
                                           turn_id=turn_id)
        if channel == lt.CH_PROGRESS:
            return await self._send_progress(chat_id, result)
        if channel == lt.CH_INTERIM:
            return await self._send_interim(chat_id, result)
        if channel == lt.CH_FINAL:
            return await self._send_final(chat_id, content, stats=stats)
        # CH_PLAIN：无标记发送 / /bg 完成结果 → deliver（回合外旁路）
        return await self._deliver(chat_id, content, meta)

    async def edit(
        self,
        chat_id: str,
        content: str,
        *,
        finalize: bool = False,
        stats: dict[str, Any] | None = None,
    ) -> DeliveryResult:
        """hermes ``adapter.edit_message(chat_id, msg_id, content, finalize)`` 钩子.

        edit 无 metadata → 回合归属由适配器注册表决定（前代 message
        注册表语义的注册表化）。无活跃回合 = 孤儿编辑（编辑的是 hermes
        原生消息）→ 原生履行（hermes 编辑自己的消息，零重复零丢失）。
        """
        ref = self._turns.active(chat_id)
        if ref is None:
            return DeliveryResult(
                action=NativeAction.NATIVE, native_text=content,
                detail="orphan_edit_no_turn", card_active=False,
            )
        if not finalize:
            return await self._submit_content(
                chat_id, [self._snapshot_env(chat_id, ref, content)],
                native_text=content, ref=ref)
        envs = self._close_thinking_envs(chat_id, ref)
        envs.append(self._final_env(chat_id, ref, content, stats))
        res = await self._submit_content(chat_id, envs, native_text=content, ref=ref)
        self._turns.close(chat_id)
        return res

    async def send_clarify(
        self,
        chat_id: str,
        *,
        kind: str = "select",
        question: str,
        options: "list[str] | tuple[str, ...] | None" = None,
        placeholder: str = "",
    ) -> DeliveryResult:
        """hermes ``send_clarify`` → ``clarify.request``（仅活跃回合内）.

        前代 clarify 卡可独立存在；协议 v1 的澄清语义锚定活跃回合
        （02-engine：PAUSED 续流）。无活跃回合 → 原生履行（hermes
        原生 ask 通道，如纯文本提问）。
        """
        ref = self._turns.active(chat_id)
        if ref is None:
            return DeliveryResult(action=NativeAction.NATIVE, detail="clarify_without_stream")
        env = self._frames.build(
            "clarify.request", chat_id=chat_id, turn_id=ref.turn_id,
            payload={"form": {
                "kind": kind, "question": question,
                "options": list(options or ()), "placeholder": placeholder,
            }},
        )
        return await self._submit_content(chat_id, [env], native_text="", ref=ref)

    async def clarify_answer(self, chat_id: str, *, values: dict[str, Any]) -> DeliveryResult:
        """澄清交互回调回流 → ``clarify.answer``（非 PAUSED 被引擎幂等忽略）."""
        ref = self._turns.active(chat_id)
        turn_id = ref.turn_id if ref is not None else ""
        env = self._frames.build(
            "clarify.answer", chat_id=chat_id, turn_id=turn_id,
            payload={"values": dict(values or {})},
        )
        ack = await self._engine.submit(env)
        # 控制帧：fallback 无原生履行义务（02-engine §6 非 PAUSED 幂等忽略）
        return DeliveryResult(
            action=NativeAction.CONSUMED, turn_id=turn_id,
            frames=(FrameVerdict(env["frame_id"], "clarify.answer",
                                 ack.verdict.value, ack.detail),),
        )

    async def abort(self, chat_id: str, *, reason: str = "") -> DeliveryResult:
        """hermes 中断/取消钩子 → ``turn.abort``（无活跃回合为无害 no-op）."""
        ref = self._turns.active(chat_id)
        if ref is None:
            return DeliveryResult(action=NativeAction.CONSUMED, detail="abort_no_active_turn")
        env = self._frames.build(
            "turn.abort", chat_id=chat_id, turn_id=ref.turn_id,
            payload={"reason": reason},
        )
        ack = await self._engine.submit(env)
        self._turns.close(chat_id)  # 中止即终结注册表引用（引擎 SEAL_ABORT）
        return DeliveryResult(
            action=NativeAction.CONSUMED, turn_id=ref.turn_id,
            detail=ack.detail,
            frames=(FrameVerdict(env["frame_id"], "turn.abort",
                                 ack.verdict.value, ack.detail),),
        )

    # ── 通道分支 ────────────────────────────────────────────────────────

    async def _send_stream(
        self, chat_id: str, content: str, *, title: str, reply_to: str,
        turn_id: str | None,
    ) -> DeliveryResult:
        ref = self._turns.active(chat_id)
        same = ref is not None and turn_id is not None and turn_id == ref.turn_id
        if ref is None or not same:
            # 新回合（expect_edits = 回合首帧；或宿主显式换 turn）：
            # turn.start → 暂存并入 → 首帧快照。引擎对未定稿旧回合
            # seal_and_create（10-11 孤儿卡根治）。同 turn_id 重发
            # expect_edits → 幂等续流（引擎 turn_start_idempotent）。
            new_id = turn_id or self._new_turn_id()
            envelopes: list[dict[str, Any]] = [
                self._frames.build(
                    "turn.start", chat_id=chat_id, turn_id=new_id,
                    payload={"reply_to": reply_to, "title": title},
                ),
            ]
            dropped = self._drain_pending(chat_id, new_id, envelopes)
            ref = self._turns.open(chat_id, new_id, clock=float(self._turn_clock()))
            envelopes.append(self._snapshot_env(chat_id, ref, content))
            res = await self._submit_content(chat_id, envelopes,
                                             native_text=content, ref=ref)
            if dropped:
                res = DeliveryResult(
                    action=res.action, turn_id=res.turn_id, native_text=res.native_text,
                    detail=res.detail, frames=res.frames,
                    dropped_progress=dropped, turn_streamed=res.turn_streamed,
                    card_active=True,
                )
            return res
        # 活跃回合续流：纯快照帧
        return await self._submit_content(
            chat_id, [self._snapshot_env(chat_id, ref, content)],
            native_text=content, ref=ref)

    async def _send_progress(self, chat_id: str, result: lt.ClassifyResult) -> DeliveryResult:
        ref = self._turns.active(chat_id)
        raw = result.raw_content
        # accumulate 兼容：多行快照按段拆分（围栏块与 header 保持一体；
        # 多行思考吸附），逐段分类为 thinking / tool 事件。
        events: list[tuple[str, dict[str, Any]]] = []
        thinking_open = bool(ref and ref.thinking_open)
        for raw_seg in lt.split_progress_segments(raw):
            seg = lt.strip_dedup_suffix(raw_seg)
            if ref is not None:
                # Q9 去重：同回合同文本段跳过（前代 (×N) 合并语义；
                # 网关 accumulate 重发 tick 不重复上卡）。容量受限防御。
                if seg in ref.seen_progress:
                    continue
                if len(ref.seen_progress) < _SEEN_PROGRESS_MAX:
                    ref.seen_progress.add(seg)
            kind, tool, clean = lt.parse_progress_line(seg)
            if kind == "thinking":
                if events and events[-1][0] != "thinking.delta" and thinking_open:
                    # 思考轮闭合：上段非思考（交错），本段开启新思考行
                    events.append(("thinking.round.end", {"round": 0}))
                events.append(("thinking.delta", {"text": clean}))
                thinking_open = True
            else:  # tool（含 unknown → 灰字行兜底）
                if thinking_open:
                    events.append(("thinking.round.end", {"round": 0}))
                    thinking_open = False
                events.append(("tool.start", {"tool": tool, "preview": lt.preview_text(seg)}))
        if not events:
            return DeliveryResult(action=NativeAction.CONSUMED,
                                  turn_id=ref.turn_id if ref else "",
                                  card_active=ref is not None)
        if ref is None:
            # 回合前暂存（前代 pending 语义；answer 帧永不进缓冲）
            dropped = 0
            for name, payload in events:
                dropped = self._buffer_pending(chat_id, (name, payload))
            return DeliveryResult(action=NativeAction.CONSUMED, detail="buffered_pre_turn",
                                  dropped_progress=dropped)
        # 思考轮开合状态：以本帧末事件为准（末事件 = thinking.delta → 轮仍开，
        # 后续 💬 增长原地改写同一行；否则轮已闭合）
        ref.thinking_open = bool(events and events[-1][0] == "thinking.delta")
        envs = [self._frames.build(name, chat_id=chat_id, turn_id=ref.turn_id,
                                   payload=dict(payload))
                for name, payload in events]
        return await self._submit_content(chat_id, envs, native_text=raw, ref=ref)

    async def _send_interim(self, chat_id: str, result: lt.ClassifyResult) -> DeliveryResult:
        ref = self._turns.active(chat_id)
        content = result.raw_content
        if ref is None:
            # 无卡永不建卡（前代跑马灯根因防线）：notice/interim 原生文本
            return DeliveryResult(action=NativeAction.NATIVE, native_text=content,
                                  detail="no_active_turn_native")
        if result.kind == "advisory" and lt.is_notice_text(content):
            level = "warn" if content.lstrip().startswith(("⚠", "❌")) else "info"
            env = self._frames.build(
                "notice", chat_id=chat_id, turn_id=ref.turn_id,
                payload={"text": content, "level": level},
            )
        else:
            env = self._frames.build(
                "interim", chat_id=chat_id, turn_id=ref.turn_id,
                payload={"text": content},
            )
        return await self._submit_content(chat_id, [env], native_text=content, ref=ref)

    async def _send_final(
        self, chat_id: str, content: str, *, stats: dict[str, Any] | None,
    ) -> DeliveryResult:
        ref = self._turns.active(chat_id)
        if ref is None:
            # 微回合自愈：短答案无流式前帧 → turn.start + final 直落定；
            # 回合前暂存的进度行并入本卡（前代 pending 并入语义）。
            new_id = self._new_turn_id()
            ref = self._turns.open(chat_id, new_id, clock=float(self._turn_clock()))
            envelopes: list[dict[str, Any]] = [
                self._frames.build("turn.start", chat_id=chat_id, turn_id=new_id,
                                   payload={}),
            ]
            self._drain_pending(chat_id, new_id, envelopes)
            envelopes.extend(self._close_thinking_envs(chat_id, ref))
            envelopes.append(self._final_env(chat_id, ref, content, stats))
            res = await self._submit_content(chat_id, envelopes,
                                             native_text=content, ref=ref)
            self._turns.close(chat_id)
            return res
        envs = self._close_thinking_envs(chat_id, ref)
        envs.append(self._final_env(chat_id, ref, content, stats))
        res = await self._submit_content(chat_id, envs, native_text=content, ref=ref)
        self._turns.close(chat_id)
        return res

    async def _deliver(
        self, chat_id: str, content: str, metadata: dict[str, Any] | None,
    ) -> DeliveryResult:
        meta = metadata or {}
        payload: dict[str, Any] = {"text": content}
        title = str(meta.get("title") or "")
        if title:
            payload["title"] = title
        job_id = str(meta.get("job_id") or "")
        if job_id:
            payload["footer"] = {"job_id": job_id}
        env = self._frames.build("deliver", chat_id=chat_id, payload=payload)
        ack = await self._engine.submit(env)
        frames = (FrameVerdict(env["frame_id"], "deliver", ack.verdict.value, ack.detail),)
        if ack.verdict is Verdict.CONSUMED:
            return DeliveryResult(action=NativeAction.CONSUMED, frames=frames)
        # deliver 失败 → 原生文本投递（永不丢帧红线；卡片只是增强面）
        return DeliveryResult(action=NativeAction.NATIVE, native_text=content,
                              detail=ack.detail or f"deliver_{ack.verdict.value}",
                              frames=frames)

    # ── 信封素材 ────────────────────────────────────────────────────────

    def _new_turn_id(self) -> str:
        self._turn_counter += 1
        return f"ht-{self._turn_counter:06d}-{uuid.uuid4().hex[:6]}"

    def _snapshot_env(self, chat_id: str, ref: TurnRef, text: str) -> dict[str, Any]:
        ref.seq += 1
        return self._frames.build("answer.snapshot", chat_id=chat_id, turn_id=ref.turn_id,
                                  payload={"text": text}, seq=ref.seq)

    def _final_env(
        self, chat_id: str, ref: TurnRef, text: str, stats: dict[str, Any] | None,
    ) -> dict[str, Any]:
        ref.seq += 1
        payload: dict[str, Any] = {"text": text}
        if stats:
            payload["stats"] = dict(stats)
        return self._frames.build("answer.final", chat_id=chat_id, turn_id=ref.turn_id,
                                  payload=payload, seq=ref.seq)

    def _close_thinking_envs(self, chat_id: str, ref: TurnRef) -> list[dict[str, Any]]:
        """思考轮显式闭合（定稿前；未闭合轮的耗时标签无法回填）."""
        if not ref.thinking_open:
            return []
        ref.thinking_open = False
        return [self._frames.build("thinking.round.end", chat_id=chat_id,
                                   turn_id=ref.turn_id, payload={"round": 0})]

    # ── 回合前暂存（前代 pending 语义）──────────────────────────────────

    def _buffer_pending(self, chat_id: str, item: tuple[str, dict[str, Any]]) -> int:
        buf = self._pending.setdefault(chat_id, deque())
        buf.append(item)
        dropped = 0
        while len(buf) > self._pending_max:
            buf.popleft()
            dropped += 1
        if dropped:
            self._dropped[chat_id] = self._dropped.get(chat_id, 0) + dropped
        return self._dropped.get(chat_id, 0)

    def _drain_pending(
        self, chat_id: str, turn_id: str, out: list[dict[str, Any]],
    ) -> int:
        buf = self._pending.pop(chat_id, None)
        dropped = self._dropped.pop(chat_id, 0)
        if buf:
            for event, payload in buf:
                out.append(self._frames.build(event, chat_id=chat_id, turn_id=turn_id,
                                              payload=dict(payload)))
        return dropped

    # ── 投递与聚合 ──────────────────────────────────────────────────────

    async def _submit_content(
        self,
        chat_id: str,
        envelopes: list[dict[str, Any]],
        *,
        native_text: str,
        ref: TurnRef | None,
    ) -> DeliveryResult:
        """逐帧投递 + 聚合（05-adapters §1：每帧恰好一个 ack，不得假设时序）."""
        frames: list[FrameVerdict] = []
        action = NativeAction.CONSUMED
        detail = ""
        for env in envelopes:
            ack: Ack = await self._engine.submit(env)
            frames.append(FrameVerdict(env["frame_id"], env["event"],
                                       ack.verdict.value, ack.detail))
            if ack.verdict is not Verdict.CONSUMED and action is NativeAction.CONSUMED:
                action = NativeAction.NATIVE
                detail = ack.detail or f"ack_{ack.verdict.value}"
        if action is NativeAction.CONSUMED:
            # 全部帧已上卡 → 标记回合已流式（降级履行策略线索）+ 暂存清位
            if ref is not None:
                ref.streamed = True
            self._dropped.pop(chat_id, None)
        return DeliveryResult(
            action=action,
            turn_id=ref.turn_id if ref else "",
            native_text=native_text if action is NativeAction.NATIVE else "",
            detail=detail,
            frames=tuple(frames),
            turn_streamed=bool(ref and ref.streamed),
            card_active=ref is not None,
        )
