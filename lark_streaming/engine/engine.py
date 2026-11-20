"""编排层引擎 — 显式回合状态机 + per-chat 队列 + 竞态守卫（02-engine）.

前代 FrameComposer（v2.0.x composer/engine.py 1556 行 → v2.1.x 2306 行）
的全部隐式旗标组合在此显式化为「状态机转移 + 守卫裁决 + 单消费者队列」；
每个竞态防线标注前代实证来源（04-knowledge-freeze）。

并发模型（02-engine §3）：
1. per-chat 有序队列 + 单消费者协程——取代前代 per-chat asyncio.Lock；
   跨 chat 完全并行，chat 内严格串行。建卡窗口（0.5~1s 网络挂起）帧
   排队等待而非触发第二次建卡（10-12 一轮 3 卡事故的根治：锁内复查
   语义由单消费者天然提供）。
2. 全部状态变更（含定时器回调）经队列串行执行——定时器回调只做
   「武装/让位/入队控制帧」三类轻量动作，权威复查在消费者侧（复查挡）。
3. 降级是 chat 级而非 turn 级（前代实证：一个 turn 失败后同 chat 后续
   turn 仍可能恢复）；冷却期内所有帧 degraded 快速回执（零 API），
   冷却期满下一帧即静默探测。

红线（02-engine §7）：任何帧必得恰好一个 ack；卡片 API 失败 →
fallback/degraded 回执（接入方原生投递兜底）——不存在静默失败的帧；
seal_all_on_shutdown 先定稿再关停。

前代语义对齐索引（本模块与 v2.0.x composer/engine.py 逐点对应）：
- 看门狗让位：clarify 暂停/窗口期/降级期静默是预期而非孤儿（前代
  _watchdog_due v2.0.26 分支；新架构 PAUSED 超时真落定见 §1.1）。
- 定稿四步：终值 patch（致命）→ 页脚/spinner/面板收口/关流式（装饰性，
  失败仅告警绝不上抛——答案已送达，重发必重复，前代 finalize.py 铁律）。
- seq 预递增 + 300317/200850 自愈：transport.call_with_seq_recovery 承接，
  引擎只在调用前 ``st.seq += 1`` 并传 reopen（200850 重开）。
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any

from ..renderer import (
    ANSWER_ELEMENT_ID,
    ClarifyForm,
    FooterStats,
    PanelRow,
    RenderState,
    RowKind,
    Theme,
    footer_batch_actions,
    footer_summary,
    panel_reseal_actions,
    render_clarify_pending,
    render_deliver_card,
    render_full_card,
    render_initial_card,
    trim_to_budget,
)
from ..transport import (
    FeishuTransport,
    call_with_seq_recovery,
    is_rate_limit_error,
)
from .events import (
    EV,
    KNOWN_EVENTS,
    Ack,
    EnvelopeError,
    Event,
    LifecycleEvent,
    Verdict,
)
from .guards import Action, decide
from .settings import EngineSettings
from .state import ChatBox, ChatHealth, TurnPhase, TurnState

_logger = logging.getLogger("lark_streaming.engine")

__all__ = ["Ctrl", "StreamingEngine"]


class Ctrl(str, Enum):
    """引擎内部控制帧（不经协议；消费者串行执行的定时器动作）."""

    WATCHDOG_SEAL = "watchdog_seal"    # 静默超时封卡（复查挡在消费者侧）
    PAUSE_EXPIRE = "pause_expire"      # 澄清窗口到期真落定
    PANEL_FLUSH = "panel_flush"        # 面板合并窗口到期
    TTL_SEAL = "ttl_seal"              # 卡 TTL 空闲孤儿强制收口
    SEAL_PROBE = "seal_probe"          # 降级冷却期满的定稿饥饿探测


@dataclass(slots=True)
class _QueueItem:
    """chat 队列条目：协议事件（带 future）或引擎控制帧."""

    event: Event | None = None
    control: Ctrl | None = None
    future: asyncio.Future[Ack] | None = None

    @classmethod
    def for_event(cls, ev: Event, future: "asyncio.Future[Ack]") -> "_QueueItem":
        return cls(event=ev, future=future)

    @classmethod
    def for_control(cls, ctrl: Ctrl) -> "_QueueItem":
        return cls(control=ctrl)

    @property
    def frame_id(self) -> str:
        return self.event.frame_id if self.event is not None else ""


def _code_of(exc: BaseException) -> int:
    return int(getattr(exc, "code", 0) or 0)


class StreamingEngine:
    """飞书流式卡片编排引擎（形态 A 内嵌使用；形态 B 由网关 shell 包装）.

    生命周期：``submit`` 投递协议事件（逐帧恰好一个 ack）→
    ``snapshot_health`` 观测 → ``aclose``（内含 seal_all_on_shutdown）收口。
    transport 由调用方持有与关闭；引擎不代管。
    """

    def __init__(
        self,
        transport: FeishuTransport,
        *,
        theme: Theme | None = None,
        settings: EngineSettings | None = None,
        on_lifecycle: Callable[[LifecycleEvent], None] | None = None,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._transport = transport
        self._theme = theme or Theme()
        self._settings = settings or EngineSettings()
        self._on_lifecycle = on_lifecycle
        self._clock = clock or _monotonic
        self._chats: dict[str, ChatBox] = {}
        self._ack_cache: OrderedDict[str, Ack] = OrderedDict()
        self._ack_waiters: dict[str, asyncio.Future[Ack]] = {}
        self._message_index: dict[str, str] = {}  # message_id → chat_id（反查窗口）
        self._loop: asyncio.AbstractEventLoop | None = None
        self._sweeper: asyncio.Task[None] | None = None
        self._closed = False

    # ── 公开面 ──────────────────────────────────────────────────────────

    async def submit(self, raw: dict[str, Any]) -> Ack:
        """投递一帧协议事件，返回恰好一个 ack（01-protocol §4.6）.

        接收原始信封 dict 并做防御性消费面校验——协议层绑定（Phase 3）
        亦应先行校验；引擎侧再校验一次保证坏绑定不能击穿消费循环。
        """
        try:
            ev = Event.from_envelope(raw)
        except EnvelopeError as e:
            ack = Ack(e.frame_id, Verdict.FALLBACK, detail=e.detail)
            if e.frame_id:
                self._cache_ack(e.frame_id, ack)
            return ack
        if ev.event not in KNOWN_EVENTS:
            ack = Ack(ev.frame_id, Verdict.FALLBACK, detail=f"unknown_event:{ev.event}")
            self._cache_ack(ev.frame_id, ack)
            return ack
        return await self._submit_event(ev)

    async def submit_event(self, ev: Event) -> Ack:
        """投递已校验事件（协议层绑定的高效路径；校验责任在调用方）."""
        if ev.event not in KNOWN_EVENTS:
            ack = Ack(ev.frame_id, Verdict.FALLBACK, detail=f"unknown_event:{ev.event}")
            self._cache_ack(ev.frame_id, ack)
            return ack
        return await self._submit_event(ev)

    async def _submit_event(self, ev: Event) -> Ack:
        self._ensure_loop()
        cached = self._ack_cache.get(ev.frame_id)
        if cached is not None:
            return cached
        waiter = self._ack_waiters.get(ev.frame_id)
        if waiter is not None:
            # 同帧重发：共享同一回执（01-protocol §5.4 幂等）
            return await waiter
        if self._closed:
            ack = Ack(ev.frame_id, Verdict.FALLBACK, detail="engine_shutdown")
            self._cache_ack(ev.frame_id, ack)
            return ack
        self._ensure_sweeper()
        box = self._chat_box(ev.chat_id)
        now = self._clock()
        if box.health is ChatHealth.DEGRADED:
            if now < box.degraded_until:
                # 冷却期内：零 API 零消费，快速 degraded 回执（02-engine §1.2）
                ack = Ack(ev.frame_id, Verdict.DEGRADED, detail="chat_degraded")
                self._cache_ack(ev.frame_id, ack)
                return ack
            box.health = ChatHealth.RECOVERING  # 冷却期满——本帧即静默探测
        fut: asyncio.Future[Ack] = self._loop.create_future()  # type: ignore[union-attr]
        self._ack_waiters[ev.frame_id] = fut
        box.queue.put_nowait(_QueueItem.for_event(ev, fut))
        self._ensure_consumer(box)
        return await fut

    def snapshot_health(self) -> dict[str, Any]:
        """观测面（02-engine §9）：与 GET /v1/health 同源."""
        now = self._clock()
        chats: dict[str, dict[str, Any]] = {}
        for chat_id, box in self._chats.items():
            st = box.turn
            chats[chat_id] = {
                "turn_phase": st.phase.value if st is not None else "idle",
                "chat_health": box.health.value,
                "active_card_age_sec": (
                    round(now - st.created_at, 3) if st is not None and st.created_at else None
                ),
                "queue_depth": box.queue.qsize(),
                "degrade_count": box.degrade_count,
                "last_error": box.last_error,
                "answer_bytes": len(
                    (st.render.answer_text if st is not None else "").encode("utf-8")
                ),
                "panel_rows": len(st.render.panel_rows) if st is not None else 0,
                "sealed_pending_gc": len(box.sealed),
            }
        return {
            "chats_total": len(chats),
            "active_cards": sum(
                1 for c in chats.values() if c["turn_phase"] not in ("idle", "sealed")
            ),
            "degraded_chats": sum(
                1 for c in chats.values() if c["chat_health"] != ChatHealth.OK.value
            ),
            "chats": chats,
        }

    async def seal_all_on_shutdown(self) -> None:
        """关停语义（02-engine §7.3）：全部活跃卡先定稿再关流."""
        for box in list(self._chats.values()):
            box.closing = True
            st = box.turn
            if st is None or st.phase is TurnPhase.SEALED:
                continue
            if box.busy or not st.card_id:
                # 消费者在途/无卡可收：标注 seal_failed（内容经原生兜底不丢）
                st.seal_failed = True
                continue
            try:
                await self._seal_turn(box, st, reason="shutdown")
            except Exception:  # noqa: BLE001 — 关停收口尽力而为
                _logger.debug(
                    "engine: shutdown seal skipped chat=%s", box.chat_id, exc_info=True
                )
        self._emit_lifecycle(LifecycleEvent("shutdown", detail="seal_all_on_shutdown"))

    async def aclose(self) -> None:
        """关停：定稿全部活跃卡 → 排空队列（未决帧 fallback）→ 级联取消."""
        if self._closed:
            return
        self._closed = True
        await self.seal_all_on_shutdown()
        if self._sweeper is not None and not self._sweeper.done():
            self._sweeper.cancel()
        for box in list(self._chats.values()):
            self._cancel_all_timers(box)
            while True:
                try:
                    item = box.queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._settle(
                    item,
                    Ack(item.frame_id, Verdict.FALLBACK, detail="engine_shutdown"),
                )
                box.queue.task_done()
            if box.consumer is not None and not box.consumer.done():
                box.consumer.cancel()
        self._chats.clear()

    def run_gc(self) -> None:
        """GC 巡检（引擎级 sweeper 周期调用；亦可显式触发）."""
        now = self._clock()
        for chat_id, box in list(self._chats.items()):
            self._reap_sealed(box, now)
            st = box.turn
            if st is not None and not box.busy and st.phase is not TurnPhase.SEALED:
                # TTL 强制收口（空闲孤儿——活跃回合每帧刷新 last_activity，
                # 长回合永不误杀，前代 v2.0.9 GC 语义）
                if (
                    now - st.created_at >= self._settings.card_ttl_sec
                    and now - st.last_activity >= self._settings.card_ttl_sec
                ):
                    self._enqueue_control(box, Ctrl.TTL_SEAL)
            # 降级冷却期满的定稿饥饿解除（SEALING 挂起且无帧可达 → 探测落定）
            if (
                st is not None
                and st.phase is TurnPhase.SEALING
                and not box.busy
                and box.health is ChatHealth.DEGRADED
                and now >= box.degraded_until
            ):
                box.health = ChatHealth.RECOVERING
                self._enqueue_control(box, Ctrl.SEAL_PROBE)
            if (
                box.turn is None
                and not box.sealed
                and box.queue.empty()
                and box.health is ChatHealth.OK
                and not box.busy
                and not box.closing
            ):
                self._cancel_all_timers(box)
                if box.consumer is not None and not box.consumer.done():
                    box.consumer.cancel()
                self._chats.pop(chat_id, None)

    # ── 循环基座 ────────────────────────────────────────────────────────

    def _ensure_loop(self) -> None:
        if self._loop is None:
            self._loop = asyncio.get_running_loop()
        elif self._loop.is_closed():
            raise RuntimeError("engine event loop is closed")

    def _chat_box(self, chat_id: str) -> ChatBox:
        box = self._chats.get(chat_id)
        if box is None:
            box = ChatBox(chat_id=chat_id)
            self._chats[chat_id] = box
        return box

    def _ensure_consumer(self, box: ChatBox) -> None:
        if box.consumer is None or box.consumer.done():
            box.consumer = self._loop.create_task(  # type: ignore[union-attr]
                self._consume(box.chat_id), name=f"lsp-consumer-{box.chat_id[:12]}"
            )

    def _ensure_sweeper(self) -> None:
        if self._sweeper is None or self._sweeper.done():
            self._sweeper = self._loop.create_task(self._sweep_loop())  # type: ignore[union-attr]

    async def _sweep_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(self._settings.gc_sweep_sec)
                try:
                    self.run_gc()
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001 — 巡检失败不终止
                    _logger.debug("engine: gc sweep failed", exc_info=True)
        except asyncio.CancelledError:
            raise

    async def _consume(self, chat_id: str) -> None:
        box = self._chats.get(chat_id)
        if box is None:
            return
        while True:
            item = await box.queue.get()
            if box.closing:
                self._settle(
                    item, Ack(item.frame_id, Verdict.FALLBACK, detail="engine_shutdown")
                )
                box.queue.task_done()
                continue
            box.busy = True
            try:
                ack = await self._dispatch(box, item)
            except asyncio.CancelledError:
                self._settle(
                    item, Ack(item.frame_id, Verdict.FALLBACK, detail="engine_shutdown")
                )
                box.busy = False
                raise
            except Exception as exc:  # noqa: BLE001 — 永不丢帧红线：内部异常 → fallback
                _logger.warning(
                    "engine: dispatch failed chat=%s frame=%s — fallback "
                    "(engine guarantees failure receipts, 02-engine §7)",
                    chat_id,
                    item.frame_id,
                    exc_info=True,
                )
                self._emit_lifecycle(
                    LifecycleEvent(
                        "internal_error",
                        chat_id=chat_id,
                        turn_id=item.event.turn_id if item.event else "",
                        detail=type(exc).__name__,
                    )
                )
                ack = Ack(
                    item.frame_id, Verdict.FALLBACK,
                    detail=f"internal_error:{type(exc).__name__}",
                )
            box.busy = False
            self._settle(item, ack)
            box.queue.task_done()
            self._reap_sealed(box, self._clock())

    async def _dispatch(self, box: ChatBox, item: _QueueItem) -> Ack:
        if item.control is not None:
            await self._dispatch_control(box, item.control)
            return Ack("", Verdict.CONSUMED)
        ev = item.event
        assert ev is not None
        ooo = self._note_protocol_seq(box, ev)
        ack = await self._dispatch_event(box, ev)
        if ooo and ack.verdict is Verdict.CONSUMED:
            # 乱序帧仍被消费，ack.detail 附加诊断标记（01-protocol §5.3）
            ack = replace(
                ack,
                detail=(f"{ack.detail}|out_of_order=true" if ack.detail else "out_of_order=true"),
            )
        return ack

    @staticmethod
    def _note_protocol_seq(box: ChatBox, ev: Event) -> bool:
        """协议 seq 乱序检测（best-effort，不重传）；返回是否乱序."""
        st = box.turn
        if st is None or ev.seq < 0 or ev.turn_id != st.turn_id:
            return False
        ooo = st.last_seen_seq >= 0 and ev.seq <= st.last_seen_seq
        if ev.seq > st.last_seen_seq:
            st.last_seen_seq = ev.seq
        return ooo

    async def _dispatch_event(self, box: ChatBox, ev: Event) -> Ack:
        d = decide(box, ev, self._settings)
        self._log_decision(box, ev, d)
        if d.action is Action.FALLBACK:
            return Ack(ev.frame_id, Verdict.FALLBACK, detail=d.detail)
        if d.action is Action.IGNORE_CONSUMED:
            return Ack(ev.frame_id, Verdict.CONSUMED, detail=d.detail)
        if d.action is Action.DELIVER:
            return await self._do_deliver(box, ev)
        if d.action is Action.CREATE_TURN:
            return await self._do_create(box, ev)
        if d.action is Action.SEAL_AND_CREATE:
            st = box.turn
            if st is not None:
                await self._seal_turn(box, st, reason="superseded")
            return await self._do_create(box, ev)
        if d.action is Action.SEAL_ABORT:
            await self._do_abort(box, ev)
            return Ack(ev.frame_id, Verdict.CONSUMED)
        if d.action is Action.BUFFER_PENDING:
            # 队列架构下不可达（02-engine §3.2 实现注记，见 state.py docstring）；
            # 防御分支：并入渲染输入，下一次 flush 承载
            assert box.turn is not None
            self._apply_event_to_render(box.turn, ev)
            box.turn.touch(self._clock())
            return Ack(ev.frame_id, Verdict.CONSUMED, detail="buffered")
        if d.action is Action.BUFFER_RESUME:
            assert box.turn is not None
            self._apply_event_to_render(box.turn, ev)
            box.turn.touch(self._clock())
            return Ack(ev.frame_id, Verdict.CONSUMED, detail="buffered_paused")
        if d.action is Action.STREAM_ANSWER:
            return await self._do_stream_answer(box, ev)
        if d.action in (Action.ANSWER_FINAL_NOW, Action.ANSWER_FINAL_WINDOW):
            return await self._do_answer_final(
                box, ev, window=d.action is Action.ANSWER_FINAL_WINDOW
            )
        if d.action is Action.PANEL_EVENT:
            return await self._do_panel_event(box, ev)
        if d.action is Action.PAUSE_CLARIFY:
            return await self._do_clarify_request(box, ev)
        if d.action is Action.RESUME_CLARIFY:
            return await self._do_clarify_answer(box, ev)
        return Ack(ev.frame_id, Verdict.FALLBACK, detail="unrouted_decision")

    async def _dispatch_control(self, box: ChatBox, ctrl: Ctrl) -> None:
        st = box.turn
        now = self._clock()
        if ctrl is Ctrl.WATCHDOG_SEAL:
            if st is None or st.phase is TurnPhase.SEALED:
                return
            if st.phase is TurnPhase.PAUSED:
                # PAUSED 超时真落定（02-engine §1.1：超时 → SEALING）
                if now - st.paused_at >= self._settings.watchdog_idle_sec:
                    await self._seal_turn(box, st, reason="pause_expired", watchdog=True)
                return
            if st.pause_pending:
                return  # 澄清窗口期（1.2s）：pause 定时器主责，让位
            if box.health is ChatHealth.DEGRADED:
                return  # 降级期无 API；恢复后由帧或 sweeper 承接
            if now - st.last_activity >= self._settings.watchdog_idle_sec:
                await self._seal_turn(box, st, reason="watchdog_idle", watchdog=True)
            return
        if ctrl is Ctrl.PAUSE_EXPIRE:
            if st is None or not st.pause_pending or st.phase is not TurnPhase.SEALING:
                return  # 复查挡：窗口已被 clarify 命中/回合已终结
            if box.health is ChatHealth.DEGRADED and now < box.degraded_until:
                return  # 冷却后由 sweeper SEAL_PROBE 承接
            await self._seal_turn(box, st, reason="pause_expire")
            return
        if ctrl is Ctrl.PANEL_FLUSH:
            if st is None or st.phase is not TurnPhase.STREAMING or not st.panel_dirty:
                return
            if box.health is ChatHealth.DEGRADED and now < box.degraded_until:
                # 恢复后仍需补 flush——续武装而非丢弃（面板再同步）
                self._arm_timer(box, "flush", self._settings.panel_flush_sec)
                return
            try:
                await self._flush_panel(box, st)
            except Exception as exc:  # noqa: BLE001 — flush 失败走降级红线
                self._degrade_chat(box, "panel:flush", exc)
            return
        if ctrl is Ctrl.TTL_SEAL:
            if st is None or st.phase is TurnPhase.SEALED:
                return
            if (
                now - st.created_at >= self._settings.card_ttl_sec
                and now - st.last_activity >= self._settings.card_ttl_sec
            ):
                await self._seal_turn(box, st, reason="ttl")
            return
        if ctrl is Ctrl.SEAL_PROBE:
            if st is not None and st.phase is TurnPhase.SEALING:
                await self._seal_turn(box, st, reason="probe")
            return

    # ── 管道：回合建立 ──────────────────────────────────────────────────

    async def _do_create(self, box: ChatBox, ev: Event) -> Ack:
        now = self._clock()
        st = TurnState(chat_id=box.chat_id, turn_id=ev.turn_id)
        reply_to = ev.payload_str("reply_to").strip()
        st.reply_to = reply_to if reply_to.startswith("om_") else ""
        st.title = ev.payload_str("title").strip()
        st.created_at = st.turn_started_at = st.last_activity = now
        st.seq = 0
        st.stream_session_at = now  # 会话续期计时基线（前代 stream_session_at）
        box.turn = st
        st.phase = TurnPhase.CREATING
        try:
            st.card_id = await self._transport.cardkit_create(
                render_initial_card(RenderState(), self._theme)
            )
        except Exception as exc:  # noqa: BLE001 — 建卡失败 → 降级红线
            self._degrade_chat(box, "create", exc)
            self._seal_without_card(box, st, now, failed=True)
            return Ack(
                ev.frame_id, Verdict.FALLBACK, detail=f"create_failed:{_code_of(exc)}"
            )
        # 锚定投递（Q10：reply 失败降级直发——内容永不丢）
        message_id = ""
        try:
            if st.reply_to:
                try:
                    message_id = await self._transport.reply_card_by_id(
                        st.reply_to, st.card_id
                    )
                except Exception as anchor_exc:  # noqa: BLE001
                    _logger.warning(
                        "engine: reply anchor failed chat=%s anchor=%s — direct send (Q10)",
                        box.chat_id, st.reply_to[:20], exc_info=anchor_exc,
                    )
                    message_id = await self._transport.send_card_by_id_to_chat(
                        box.chat_id, st.card_id
                    )
            else:
                message_id = await self._transport.send_card_by_id_to_chat(
                    box.chat_id, st.card_id
                )
        except Exception as exc:  # noqa: BLE001 — 直发也失败：回合终结
            self._degrade_chat(box, "create:send", exc)
            self._seal_without_card(box, st, now, failed=True)
            _logger.warning(
                "engine: card entity orphaned card=%s (delivery failed)",
                st.card_id[:12],
            )
            return Ack(
                ev.frame_id, Verdict.FALLBACK, detail=f"send_failed:{_code_of(exc)}"
            )
        st.message_id = message_id
        self._message_index[message_id] = box.chat_id
        st.phase = TurnPhase.STREAMING
        self._note_api_success(box)
        self._arm_timer(box, "watchdog", self._settings.watchdog_idle_sec)
        _logger.info(
            "engine: card created chat=%s turn=%s card=%s msg=%s",
            box.chat_id, ev.turn_id, st.card_id[:12], message_id[:12],
        )
        return Ack(ev.frame_id, Verdict.CONSUMED, message_id=message_id)

    def _seal_without_card(self, box: ChatBox, st: TurnState, now: float, *, failed: bool) -> None:
        """无卡可收的回合终结（建卡/投递失败边缘）：状态入册，零 API."""
        st.phase = TurnPhase.SEALED
        st.sealed_at = now
        st.seal_failed = failed
        box.sealed.append(st)
        box.turn = None

    # ── 管道：回答流 ────────────────────────────────────────────────────

    async def _do_stream_answer(self, box: ChatBox, ev: Event) -> Ack:
        st = box.turn
        assert st is not None
        old = st.render.answer_text
        new = self._apply_answer_payload(st, ev)
        st.touch(self._clock())
        if new == old:
            return Ack(ev.frame_id, Verdict.CONSUMED)  # 等值跳过（OPT-4 语义）
        try:
            await self._stream_answer_now(box, st, what="stream:answer")
        except Exception as exc:  # noqa: BLE001 — 降级红线在管道层收口（02-engine §7）
            self._degrade_chat(box, "stream:answer", exc)
            return Ack(
                ev.frame_id, Verdict.FALLBACK,
                detail=f"stream_failed:{_code_of(exc)}",
            )
        return Ack(ev.frame_id, Verdict.CONSUMED)

    def _apply_answer_payload(self, st: TurnState, ev: Event) -> str:
        """delta 追加 / snapshot 替换（01-protocol §5.2 统一化）."""
        if ev.event == EV.ANSWER_SNAPSHOT:
            text = ev.payload.get("text")
            if isinstance(text, str):
                st.render = st.render.with_answer(text)
            return st.render.answer_text
        if ev.event == EV.ANSWER_DELTA:
            delta = ev.payload_str("delta")
            if delta:
                st.render = st.render.with_answer(st.render.answer_text + delta)
            return st.render.answer_text
        return st.render.answer_text

    def _apply_event_to_render(self, st: TurnState, ev: Event) -> None:
        """防御路径/暂停期缓冲：只改渲染输入，零 API."""
        if ev.event in (EV.ANSWER_DELTA, EV.ANSWER_SNAPSHOT):
            self._apply_answer_payload(st, ev)
            return
        self._apply_panel_mutation(st, ev)

    async def _stream_answer_now(self, box: ChatBox, st: TurnState, *, what: str) -> None:
        text = st.render.answer_text
        if not (text or "").strip():
            return  # 99992402 空守卫（s152 真飞书探针定案）
        await self._renew_if_due(box, st)
        st.seq += 1  # 前代预递增纪律
        st.touch(self._clock())
        await call_with_seq_recovery(
            st,
            lambda s: self._transport.cardkit_stream_element(
                st.card_id, ANSWER_ELEMENT_ID, text, sequence=s
            ),
            what=what,
            reopen=lambda s: self._transport.cardkit_reopen_streaming(
                st.card_id, sequence=s
            ),
        )
        self._note_api_success(box)

    async def _renew_if_due(self, box: ChatBox, st: TurnState) -> None:
        """主动续期（T_renew 540s 龄，02-engine §2）：更新前先重开会话."""
        if st.phase is not TurnPhase.STREAMING or not st.card_id:
            return
        age = self._clock() - st.stream_session_at
        if age < self._settings.session_renew_age_sec:
            return
        st.seq += 1
        try:
            await self._transport.cardkit_reopen_streaming(st.card_id, sequence=st.seq)
        except Exception as exc:  # noqa: BLE001 — 续期失败不阻断主链：
            # 200850 由下一次 stream 的 reopen 自愈承接；其余错误由真实
            # API 调用自己暴露（降级红线）
            _logger.info(
                "engine: proactive renew skipped chat=%s card=%s err=%s",
                box.chat_id, st.card_id[:12], _code_of(exc),
            )
            return
        st.stream_session_at = self._clock()
        _logger.info(
            "engine: streaming session renewed chat=%s card=%s age=%.0fs",
            box.chat_id, st.card_id[:12], age,
        )

    # ── 管道：面板 ──────────────────────────────────────────────────────

    def _apply_panel_mutation(self, st: TurnState, ev: Event) -> bool:
        """面板事件 → 行变更（纯内存）；返回是否结构性新增行."""
        rows = list(st.render.panel_rows)
        name = ev.event
        structural = False
        if name == EV.TOOL_START:
            rows.append(PanelRow(
                kind=RowKind.TOOL,
                tool_name=ev.payload_str("tool"),
                tool_detail=ev.payload_str("preview"),
                pending=True,
            ))
            structural = True
        elif name == EV.TOOL_END:
            dur = self._payload_float(ev, "duration_ms")
            detail = ev.payload_str("output_preview")
            idx = self._last_index(rows, lambda r: (
                r.kind is RowKind.TOOL and r.tool_name == ev.payload_str("tool") and r.pending
            ))
            if idx is not None:
                rows[idx] = rows[idx].with_tool_end(duration_ms=dur, detail=detail)
            else:
                rows.append(PanelRow(
                    kind=RowKind.TOOL,
                    tool_name=ev.payload_str("tool"),
                    tool_detail=detail,
                    pending=False,
                    duration_ms=dur,
                ))
                structural = True  # 迟到 end（无 start）：新增执行行
        elif name == EV.THINKING_DELTA:
            text = ev.payload_str("text")
            rnd = st.last_round_closed + 1
            idx = self._last_index(rows, lambda r: (
                r.kind is RowKind.THINKING
                and r.round_index == rnd
                and not r.round_finalized
            ))
            if idx is not None:
                rows[idx] = replace(rows[idx], text=rows[idx].text + text)
            elif text:
                rows.append(PanelRow(
                    kind=RowKind.THINKING, text=text, round_index=rnd,
                ))
                structural = True
        elif name == EV.THINKING_ROUND_END:
            rnd = self._payload_int(ev, "round") or (st.last_round_closed + 1)
            dur = self._payload_float(ev, "duration_ms")
            for i, r in enumerate(rows):
                if (
                    r.kind is RowKind.THINKING
                    and r.round_index == rnd
                    and not r.round_finalized
                ):
                    rows[i] = replace(
                        r,
                        round_finalized=True,
                        round_elapsed_ms=dur if dur is not None else r.round_elapsed_ms,
                    )
            st.last_round_closed = max(st.last_round_closed, rnd)
        elif name == EV.INTERIM:
            rows.append(PanelRow(kind=RowKind.INTERIM, text=ev.payload_str("text")))
            structural = True
        elif name == EV.NOTICE:
            level = ev.payload_str("level") or "info"
            if level not in ("warn", "info", "error"):
                level = "info"
            rows.append(PanelRow(
                kind=RowKind.NOTICE, text=ev.payload_str("text"), level=level,
            ))
            structural = True
        st.render = st.render.with_panel_rows(rows)
        return structural

    @staticmethod
    def _last_index(rows: list[PanelRow], pred: Callable[[PanelRow], bool]) -> int | None:
        for i in range(len(rows) - 1, -1, -1):
            if pred(rows[i]):
                return i
        return None

    @staticmethod
    def _payload_float(ev: Event, key: str) -> float | None:
        v = ev.payload.get(key)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        return None

    @staticmethod
    def _payload_int(ev: Event, key: str) -> int | None:
        v = ev.payload.get(key)
        if isinstance(v, int) and not isinstance(v, bool):
            return int(v)
        return None

    async def _do_panel_event(self, box: ChatBox, ev: Event) -> Ack:
        st = box.turn
        assert st is not None
        structural = self._apply_panel_mutation(st, ev)
        st.touch(self._clock())
        if structural:
            # 结构性新增行不合并（即时可见，OPT-1a 红线）
            self._cancel_timer(box, "flush")
            st.panel_dirty = True
            try:
                await self._flush_panel(box, st)
            except Exception as exc:  # noqa: BLE001 — 降级红线在管道层收口
                self._degrade_chat(box, "panel:flush", exc)
                return Ack(
                    ev.frame_id, Verdict.FALLBACK,
                    detail=f"panel_flush_failed:{_code_of(exc)}",
                )
        else:
            st.panel_dirty = True
            self._arm_timer(box, "flush", self._settings.panel_flush_sec)
        return Ack(ev.frame_id, Verdict.CONSUMED)

    async def _flush_panel(self, box: ChatBox, st: TurnState) -> None:
        if st.phase is not TurnPhase.STREAMING or not st.card_id or not st.panel_dirty:
            return
        st.panel_dirty = False
        state = st.with_panel_title(self._clock())  # 动态标题刷新（TurnState 合成）
        st.render = state
        trimmed, report = trim_to_budget(state, self._theme)
        if (
            report.trimmed_interim or report.trimmed_thinking_rounds
            or report.answer_truncated
        ):
            _logger.info(
                "engine: panel trimmed chat=%s turn=%s interim=%d rounds=%d answer=%s",
                box.chat_id, st.turn_id, report.trimmed_interim,
                report.trimmed_thinking_rounds, report.answer_truncated,
            )
        card = render_full_card(trimmed, self._theme)
        await self._renew_if_due(box, st)
        st.seq += 1
        await call_with_seq_recovery(
            st,
            lambda s: self._transport.cardkit_update(st.card_id, card, sequence=s),
            what="panel:flush",
            reopen=lambda s: self._transport.cardkit_reopen_streaming(
                st.card_id, sequence=s
            ),
        )
        self._note_api_success(box)

    # ── 管道：定稿（前代 finalize.py 四步铁律）────────────────────────────

    async def _do_answer_final(self, box: ChatBox, ev: Event, *, window: bool) -> Ack:
        st = box.turn
        assert st is not None
        now = self._clock()
        text = ev.payload.get("text")
        if isinstance(text, str):
            st.render = st.render.with_answer(text)
        st.render = st.render.with_footer(self._footer_from_stats(st, ev, now))
        st.phase = TurnPhase.SEALING
        st.touch(now)
        if window:
            st.pause_pending = True
            self._arm_timer(box, "pause", self._settings.clarify_window_sec)
            return Ack(ev.frame_id, Verdict.CONSUMED, detail="seal_window_open")
        await self._seal_turn(box, st, reason="final")
        return Ack(ev.frame_id, Verdict.CONSUMED)

    def _footer_from_stats(self, st: TurnState, ev: Event, now: float) -> FooterStats:
        duration_ms = (now - st.turn_started_at) * 1000.0
        stats = ev.payload.get("stats")
        model = ""
        used = 0
        mx = 0
        effort = ""
        if isinstance(stats, dict):
            d = stats.get("duration_ms")
            if isinstance(d, (int, float)) and not isinstance(d, bool):
                duration_ms = float(d)
            model = str(stats.get("model") or "")
            ctx = stats.get("context_tokens")
            if isinstance(ctx, dict):
                used = int(ctx.get("used") or 0)
                mx = int(ctx.get("max") or 0)
            elif isinstance(ctx, int) and not isinstance(ctx, bool):
                used = int(ctx)
            extra = stats.get("extra")
            if isinstance(extra, dict):
                effort = str(extra.get("reasoning_effort") or "")
        return FooterStats(
            duration_ms=duration_ms, model=model,
            context_used=used, context_max=mx, reasoning_effort=effort,
        )

    async def _seal_turn(
        self, box: ChatBox, st: TurnState, *, reason: str,
        abort: bool = False, watchdog: bool = False,
    ) -> None:
        now = self._clock()
        if st.phase is TurnPhase.SEALED:
            return
        st.pause_pending = False
        self._cancel_timer(box, "pause")
        self._cancel_timer(box, "flush")
        if abort:
            st.aborted = True
        footer = st.render.footer
        if footer is None:
            footer = FooterStats(duration_ms=(now - st.turn_started_at) * 1000.0)
        if abort:
            footer = replace(
                footer, is_aborted=True,
                abort_reason=st.abort_reason or footer.abort_reason,
            )
        st.render = st.render.with_footer(footer)
        st.phase = TurnPhase.SEALING
        # 第一步（致命路径）：answer 终值 patch；空 answer 跳过（99992402）
        if (st.render.answer_text or "").strip():
            st.seq += 1
            try:
                await call_with_seq_recovery(
                    st,
                    lambda s: self._transport.cardkit_stream_element(
                        st.card_id, ANSWER_ELEMENT_ID, st.render.answer_text, sequence=s
                    ),
                    what=f"finalize:answer[{reason}]",
                    reopen=lambda s: self._transport.cardkit_reopen_streaming(
                        st.card_id, sequence=s
                    ),
                )
            except Exception as exc:  # noqa: BLE001 — 第一步失败 → 降级红线
                self._degrade_chat(box, f"seal:{reason}", exc)
                self._finish_seal(box, st, now, failed=True)
                if watchdog:
                    self._emit_card_sealed_fallback(box, st, reason)
                return
        self._note_api_success(box)
        # 第二~四步：装饰性（前代 finalize.py 铁律：失败仅告警绝不上抛——
        # 答案已送达，上抛会触发原生重发造成重复消息）
        await self._seal_decorations(box, st)
        self._finish_seal(box, st, now, failed=False)
        if watchdog:
            self._emit_card_sealed_fallback(box, st, reason)
        _logger.info(
            "engine: card sealed chat=%s turn=%s reason=%s failed=%s",
            box.chat_id, st.turn_id, reason, st.seal_failed,
        )

    async def _seal_decorations(self, box: ChatBox, st: TurnState) -> None:
        actions = footer_batch_actions(st.render, self._theme)
        footer_add = actions[0] if len(actions) == 2 else None
        spinner_del = actions[-1] if actions else None
        if footer_add is not None:
            try:
                st.seq += 1
                await call_with_seq_recovery(
                    st,
                    lambda s: self._transport.cardkit_batch_update(
                        st.card_id, [footer_add], sequence=s
                    ),
                    what="finalize:footer-add",
                )
            except Exception:  # noqa: BLE001
                _logger.warning(
                    "engine: footer seal failed (non-fatal) card=%s",
                    st.card_id[:12], exc_info=True,
                )
        if spinner_del is not None:
            try:
                st.seq += 1
                await call_with_seq_recovery(
                    st,
                    lambda s: self._transport.cardkit_batch_update(
                        st.card_id, [spinner_del], sequence=s
                    ),
                    what="finalize:spinner-del",
                )
            except Exception:  # noqa: BLE001
                _logger.debug(
                    "engine: spinner delete skipped card=%s", st.card_id[:12],
                    exc_info=True,
                )
        reseal = panel_reseal_actions(st.render, self._theme)
        if reseal is not None:
            try:
                st.seq += 1
                await call_with_seq_recovery(
                    st,
                    lambda s: self._transport.cardkit_batch_update(
                        st.card_id, reseal, sequence=s
                    ),
                    what="finalize:panel-reseal",
                )
            except Exception:  # noqa: BLE001
                _logger.warning(
                    "engine: panel reseal skipped (non-fatal) card=%s",
                    st.card_id[:12], exc_info=True,
                )
        try:
            st.seq += 1
            await call_with_seq_recovery(
                st,
                lambda s: self._transport.cardkit_close_streaming(
                    st.card_id, sequence=s,
                    summary=footer_summary(st.render, self._theme),
                ),
                what="finalize:close",
            )
        except Exception:  # noqa: BLE001 — v2.0.21 装饰性化：最坏=卡留流式态
            _logger.warning(
                "engine: close_streaming skipped (non-fatal) card=%s",
                st.card_id[:12], exc_info=True,
            )

    def _finish_seal(self, box: ChatBox, st: TurnState, now: float, *, failed: bool) -> None:
        st.phase = TurnPhase.SEALED
        st.sealed_at = now
        st.seal_failed = failed
        box.sealed.append(st)
        box.turn = None

    def _emit_card_sealed_fallback(self, box: ChatBox, st: TurnState, reason: str) -> None:
        self._emit_lifecycle(LifecycleEvent(
            "card_sealed_fallback",
            chat_id=box.chat_id,
            turn_id=st.turn_id,
            detail=reason,
        ))
        _logger.warning(
            "engine: watchdog sealed card chat=%s turn=%s reason=%s "
            "(no final frame — adapter goes native)",
            box.chat_id, st.turn_id, reason,
        )

    # ── 管道：中止 ──────────────────────────────────────────────────────

    async def _do_abort(self, box: ChatBox, ev: Event) -> None:
        st = box.turn
        assert st is not None
        st.abort_reason = ev.payload_str("reason")
        if st.phase is TurnPhase.CREATING or not st.card_id:
            # 卡未建成：零 API 终结（无内容在卡上；暂存由队列天然持有）
            self._seal_without_card(box, st, self._clock(), failed=True)
            return
        await self._seal_turn(box, st, reason="abort", abort=True)

    # ── 管道：澄清（前代 v2.0.26/34 同卡续流语义的协议化）──────────────────

    def _form_from_payload(self, raw: Any) -> ClarifyForm:
        if not isinstance(raw, dict):
            raise ValueError("form_not_an_object")
        options_raw = raw.get("options")
        options = (
            tuple(str(o) for o in options_raw)
            if isinstance(options_raw, (list, tuple)) else ()
        )
        return ClarifyForm(
            kind=str(raw.get("kind") or ""),
            question=str(raw.get("question") or ""),
            options=options,
            placeholder=str(raw.get("placeholder") or ""),
        )

    @staticmethod
    def _remove_pause_row(st: TurnState) -> None:
        if not st.pause_row_present:
            return
        from ..renderer.theme import DEFAULT_THEME

        wait_text = DEFAULT_THEME.text("clarify_wait")[0]
        rows = tuple(
            r for r in st.render.panel_rows
            if not (r.kind is RowKind.NOTICE and r.text == wait_text)
        )
        st.render = st.render.with_panel_rows(rows)
        st.pause_row_present = False

    async def _do_clarify_request(self, box: ChatBox, ev: Event) -> Ack:
        st = box.turn
        assert st is not None
        now = self._clock()
        try:
            form = self._form_from_payload(ev.payload.get("form"))
        except ValueError as exc:
            return Ack(ev.frame_id, Verdict.FALLBACK, detail=f"clarify_invalid_form:{exc}")
        st.clarify_id = uuid.uuid4().hex[:12]
        st.clarify_form = form
        if st.pause_pending:
            # T_pause 窗口命中：取消落定，转暂停（前代边界竞态的协议化）
            st.pause_pending = False
            self._cancel_timer(box, "pause")
        st.paused_at = now
        st.phase = TurnPhase.PAUSED
        # 面板暂停行（前代 _clarify_wait_row：单语 en）
        wait_text = self._theme.text("clarify_wait")[0]
        rows = list(st.render.panel_rows)
        if not any(r.kind is RowKind.NOTICE and r.text == wait_text for r in rows):
            rows.append(PanelRow(kind=RowKind.NOTICE, level="info", text=wait_text))
            st.pause_row_present = True
            st.render = st.render.with_panel_rows(rows)
        # 澄清独立卡（前代生产验证形态；经 CardKit 实体投递——语义等价，
        # 回调链路与卡状态演进留 L3 R7 实测校准）
        card = render_clarify_pending(form, self._theme, clarify_id=st.clarify_id)
        clarify_card_id = ""
        clarify_mid = ""
        try:
            clarify_card_id = await self._transport.cardkit_create(card)
            if st.reply_to:
                try:
                    clarify_mid = await self._transport.reply_card_by_id(
                        st.reply_to, clarify_card_id
                    )
                except Exception:  # noqa: BLE001 — Q10 锚定失败降级直发
                    clarify_mid = await self._transport.send_card_by_id_to_chat(
                        box.chat_id, clarify_card_id
                    )
            else:
                clarify_mid = await self._transport.send_card_by_id_to_chat(
                    box.chat_id, clarify_card_id
                )
        except Exception as exc:  # noqa: BLE001 — 表单卡投递失败：
            # 暂停不可交互（死等 = 看门狗 300s 后真落定）——回滚暂停转 fallback，
            # 接入方原生投递澄清文本
            self._degrade_chat(box, "clarify:card", exc)
            st.phase = TurnPhase.STREAMING
            self._remove_pause_row(st)
            st.clarify_id = ""
            st.clarify_form = None
            st.paused_at = 0.0
            return Ack(
                ev.frame_id, Verdict.FALLBACK,
                detail=f"clarify_card_failed:{_code_of(exc)}",
            )
        st.clarify_message_id = clarify_mid
        self._message_index[clarify_mid] = box.chat_id
        self._note_api_success(box)
        st.panel_dirty = True
        try:
            await self._flush_panel(box, st)  # 暂停行即时可见（前代 _patch_status）
        except Exception as exc:  # noqa: BLE001 — 行 flush 失败走降级红线
            self._degrade_chat(box, "clarify:panel", exc)
        _logger.info(
            "engine: clarify paused chat=%s turn=%s clarify_id=%s question=%r",
            box.chat_id, st.turn_id, st.clarify_id,
            (form.question or "")[:40],
        )
        return Ack(ev.frame_id, Verdict.CONSUMED)

    async def _do_clarify_answer(self, box: ChatBox, ev: Event) -> Ack:
        st = box.turn
        assert st is not None
        st.phase = TurnPhase.STREAMING
        values = ev.payload.get("values")
        _logger.info(
            "engine: clarify resumed chat=%s turn=%s values_keys=%s",
            box.chat_id, st.turn_id,
            sorted(values.keys()) if isinstance(values, dict) else [],
        )
        self._remove_pause_row(st)
        self._arm_timer(box, "watchdog", self._settings.watchdog_idle_sec)
        # 续流：缓冲（含暂停期 BUFFER_RESUME 内容）+ 面板补插（前代 resume 语义）
        try:
            if (st.render.answer_text or "").strip():
                await self._stream_answer_now(box, st, what="resume:answer")
            st.panel_dirty = True
            await self._flush_panel(box, st)
        except Exception as exc:  # noqa: BLE001 — 降级红线在管道层收口
            self._degrade_chat(box, "clarify:resume", exc)
            return Ack(
                ev.frame_id, Verdict.FALLBACK,
                detail=f"resume_failed:{_code_of(exc)}",
            )
        return Ack(ev.frame_id, Verdict.CONSUMED)

    # ── 管道：回合外投递（02-engine §8 完全旁路）──────────────────────────

    async def _do_deliver(self, box: ChatBox, ev: Event) -> Ack:
        footer = ev.payload.get("footer")
        job_id = ""
        if isinstance(footer, dict):
            job_id = str(footer.get("job_id") or "")
        card = render_deliver_card(
            ev.payload_str("text"), self._theme,
            title=ev.payload_str("title"), job_id=job_id,
        )
        try:
            mid = await self._transport.send_card_to_chat(box.chat_id, card)
        except Exception as exc:  # noqa: BLE001 — 静态卡失败同样进降级统计
            self._degrade_chat(box, "deliver", exc)
            return Ack(
                ev.frame_id, Verdict.FALLBACK, detail=f"deliver_failed:{_code_of(exc)}"
            )
        self._message_index[mid] = box.chat_id
        self._note_api_success(box)
        return Ack(ev.frame_id, Verdict.CONSUMED, message_id=mid)

    # ── 降级与恢复（02-engine §1.2 正交 chat 级态）───────────────────────

    def _degrade_chat(self, box: ChatBox, where: str, exc: BaseException) -> None:
        code = _code_of(exc)
        cooldown = (
            self._settings.degrade_cooldown_rate_limit_sec
            if is_rate_limit_error(exc)
            else self._settings.degrade_cooldown_other_sec
        )
        turn_id = box.turn.turn_id if box.turn is not None else ""
        box.health = ChatHealth.DEGRADED
        box.degraded_until = self._clock() + cooldown
        box.degrade_count += 1
        box.last_error = f"{where}:code={code}"
        self._emit_lifecycle(LifecycleEvent(
            "chat_degraded",
            chat_id=box.chat_id,
            turn_id=turn_id,
            detail=f"{where} code={code}",
            code=code,
        ))
        _logger.warning(
            "engine: card channel degraded chat=%s at %s code=%s cooldown=%.0fs "
            "— frames go native (零丢失)",
            box.chat_id, where, code, cooldown,
        )

    def _note_api_success(self, box: ChatBox) -> None:
        if box.health is not ChatHealth.OK:
            box.health = ChatHealth.OK
            box.degraded_until = 0.0
            self._emit_lifecycle(LifecycleEvent(
                "chat_recovered",
                chat_id=box.chat_id,
                turn_id=box.turn.turn_id if box.turn is not None else "",
            ))
            _logger.info("engine: chat recovered chat=%s", box.chat_id)

    # ── 定时器（02-engine §3.4：按 chat 注册，级联取消，重复武装幂等）──────

    def _arm_timer(self, box: ChatBox, key: str, delay: float) -> None:
        self._cancel_timer(box, key)
        loop = self._loop
        if loop is None or loop.is_closed() or box.closing:
            return
        if key == "pause" and delay <= 0:
            return  # 澄清窗口关闭（clarify_window_sec=0）
        try:
            box.timers[key] = loop.call_later(max(0.0, delay), self._on_timer, box.chat_id, key)
        except RuntimeError:
            pass  # loop 已停转（跨线程窗口）

    def _cancel_timer(self, box: ChatBox, key: str) -> None:
        timer = box.timers.pop(key, None)
        if timer is not None:
            timer.cancel()

    def _cancel_all_timers(self, box: ChatBox) -> None:
        for key in list(box.timers):
            self._cancel_timer(box, key)

    def _on_timer(self, chat_id: str, key: str) -> None:
        box = self._chats.get(chat_id)
        if box is None or box.closing:
            return
        box.timers.pop(key, None)
        if key == "watchdog":
            self._watchdog_due(box)
            return
        self._enqueue_control(box, Ctrl.PAUSE_EXPIRE if key == "pause" else Ctrl.PANEL_FLUSH)

    def _watchdog_due(self, box: ChatBox) -> None:
        """看门狗到期回调（advisory，loop 线程）：达标 → 入队；否则让位/续守."""
        if box.closing:
            return
        st = box.turn
        now = self._clock()
        if st is None or st.phase is TurnPhase.SEALED:
            return
        if st.phase is TurnPhase.PAUSED:
            # clarify 等待静默是预期（前代 v2.0.26 让位）；但 PAUSED 有
            # 真落定超时（02-engine §1.1）——paused_at 起 T_idle 到期封卡
            if now - st.paused_at >= self._settings.watchdog_idle_sec:
                self._enqueue_control(box, Ctrl.WATCHDOG_SEAL)
            else:
                self._arm_timer(box, "watchdog", self._settings.watchdog_idle_sec)
            return
        if st.pause_pending:
            self._arm_timer(box, "watchdog", self._settings.watchdog_idle_sec)
            return  # 澄清窗口期：pause 定时器主责
        if box.health is ChatHealth.DEGRADED:
            if now < box.degraded_until:
                self._arm_timer(box, "watchdog", self._settings.watchdog_idle_sec)
                return  # 降级冷却期让位：无 API 可用
            box.health = ChatHealth.RECOVERING  # 冷却期满：看门狗即探测者
        if now - st.last_activity >= self._settings.watchdog_idle_sec:
            self._enqueue_control(box, Ctrl.WATCHDOG_SEAL)
        else:
            # 回合仍活跃（帧持续刷新 last_activity）——再守一周期
            self._arm_timer(box, "watchdog", self._settings.watchdog_idle_sec)

    def _enqueue_control(self, box: ChatBox, ctrl: Ctrl) -> None:
        if box.closing:
            return
        box.queue.put_nowait(_QueueItem.for_control(ctrl))
        self._ensure_consumer(box)

    # ── GC / 回执缓存 / 生命周期 ────────────────────────────────────────

    def _reap_sealed(self, box: ChatBox, now: float) -> None:
        if not box.sealed:
            return
        keep: list[TurnState] = []
        for s in box.sealed:
            if now - s.sealed_at < self._settings.gc_cooldown_sec:
                keep.append(s)
            else:
                for mid in (s.message_id, s.clarify_message_id):
                    if mid:
                        self._message_index.pop(mid, None)
        box.sealed = keep

    def _cache_ack(self, frame_id: str, ack: Ack) -> None:
        cache = self._ack_cache
        if frame_id in cache:
            cache.move_to_end(frame_id)
            cache[frame_id] = ack
            return
        cache[frame_id] = ack
        while len(cache) > self._settings.frame_cache_max:
            cache.popitem(last=False)

    def _settle(self, item: _QueueItem, ack: Ack) -> None:
        if item.frame_id:
            self._cache_ack(item.frame_id, ack)
            self._ack_waiters.pop(item.frame_id, None)
        fut = item.future
        if fut is not None and not fut.done():
            fut.set_result(ack)

    def _emit_lifecycle(self, ev: LifecycleEvent) -> None:
        _logger.info(
            "engine: lifecycle %s chat=%s turn=%s detail=%s",
            ev.type, ev.chat_id[:12], ev.turn_id[:12], ev.detail,
        )
        cb = self._on_lifecycle
        if cb is not None:
            try:
                cb(ev)
            except Exception:  # noqa: BLE001 — 观测面故障不击穿主链
                _logger.debug("engine: lifecycle sink error", exc_info=True)

    def _log_decision(self, box: ChatBox, ev: Event, d: Any) -> None:
        """守卫打点（02-engine §9：竞态守卫触发结构化日志，Phase 6 双跑对照）."""
        if d.action is Action.FALLBACK:
            _logger.warning(
                "engine: guard fallback chat=%s turn=%s frame=%s guard=%s detail=%s",
                box.chat_id, ev.turn_id, ev.frame_id, d.owner, d.detail,
            )
        elif d.owner in (
            "turn_start_supersede", "turn_abort", "clarify_pause",
            "clarify_resume", "clarify_window_hit", "answer_final_paused",
        ):
            _logger.info(
                "engine: guard chat=%s turn=%s frame=%s guard=%s action=%s",
                box.chat_id, ev.turn_id, ev.frame_id, d.owner, d.action.value,
            )
        else:
            _logger.debug(
                "engine: guard chat=%s turn=%s frame=%s guard=%s action=%s",
                box.chat_id, ev.turn_id, ev.frame_id, d.owner, d.action.value,
            )


def _monotonic() -> float:
    import time

    return time.monotonic()
