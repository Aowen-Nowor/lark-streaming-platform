"""openclaw 形态 B 映射器 — bridge 出站事件 → LSEP/1 信封（05-adapters §5.2）.

openclaw（Node 系 agent）经 bridge 子进程跑网关 stdio 绑定接入：
openclaw 侧把出站事件投递进来，本模块把 bridge 消息翻译为 LSEP/1
信封序列，由 bridge 逐帧写入 stdio（或经 HTTP/WS 投递）。

bridge 消息契约（本模块定义；JSON 一行一消息）::

    {
      "chat_id": "oc_xxx",              # 必填
      "turn_id": "…",                   # 可选（缺省映射器自动分配）
      "content": "文本",                 # 必填（deliver / answer / 进度行正文）
      "metadata": {
        "streaming": true,              # → answer 流（delta/snapshot 双模，D5）
        "incremental": true,            # true = content 为增量 → answer.delta；
                                        # 缺省/false = content 为全量 → answer.snapshot
        "finalize": true,               # → answer.final（回合终结）
        "tool": "terminal",             # 进度模板（可选）：→ tool.start
        "preview": "…",
        "thinking": true,               # content 为思考文本 → thinking.delta
        "notice": "warn",               # 通知模板：→ notice（值为 level）
        "interim": true,                # 中途播报 → interim
        "title": "…", "reply_to": "om_…", "job_id": "…", "stats": {…}
      }
    }

裁决优先级：finalize > tool/thinking/notice/interim > streaming > 无标记
（无标记 → ``deliver`` 回合外静态卡）。

映射器语义与引擎守卫表对齐（02-engine §6）：无活跃回合时的面板类/
answer 类帧由适配器侧承接——进度类缓冲（turn.start 后并入）、answer
类自愈开回合；``deliver`` 完全旁路。降级履行（§1.4）由 openclaw 侧
承接：bridge 逐帧读取 stderr 回执行，verdict ∈ {fallback, degraded}
必须用 openclaw 原生通道投递（bridge 协议见 docs/ADAPTER_SDK.md §3）。

进度/通知文本模板：openclaw 官方输出以 metadata 结构化字段优先
（``tool`` / ``thinking`` / ``notice`` / ``interim``）；纯文本进度行
（emoji 启发式）复用 legacy 分类器（接入时以官方输出实测为准补齐锚定，
05-adapters §5.2 诚实标注）。
"""

from __future__ import annotations

import logging
from collections import deque
from typing import Any

from . import _legacy_text as lt
from ._envelope import EnvelopeBuilder, TurnRegistry

_logger = logging.getLogger("lark_streaming.adapters.openclaw")

__all__ = ["OpenclawMapper"]

#: 回合前进度缓冲上限（与引擎 CREATING 暂存 pending_max=64 对齐）
_PENDING_MAX = 64


class OpenclawMapper:
    """bridge 消息 → LSEP/1 信封序列（有状态：per-chat 回合注册表）.

    典型接线（bridge 子进程主循环；docs/ADAPTER_SDK.md §3 完整示例）::

        mapper = OpenclawMapper()
        for msg in openclaw_outbound_queue:
            for env in mapper.translate(msg):
                proc.stdin.write(json.dumps(env) + "\\n")
    """

    def __init__(
        self,
        *,
        pending_max: int = _PENDING_MAX,
        frame_prefix: str = "of",
        turn_prefix: str = "ot",
        clock: "callable | None" = None,  # type: ignore[valid-type]
    ) -> None:
        self._pending_max = max(8, int(pending_max))
        self._turns = TurnRegistry()
        self._frames = EnvelopeBuilder(prefix=frame_prefix, clock=clock)
        self._turn_clock = clock or (lambda: 0.0)
        self._turn_counter = 0
        self._pending: dict[str, deque[tuple[str, dict[str, Any]]]] = {}

    def translate(self, msg: dict[str, Any]) -> list[dict[str, Any]]:
        """bridge 消息 → 信封序列（永不抛异常；翻译失败 → deliver 兜底）.

        audit #9-S2：翻译失败不再静默返回空序列（内容无声丢失只剩日志）
        ——兜底为 ``deliver`` 直投原文（引擎旁路静态卡通道，02-engine §8），
        失败的消息至少可达用户；deliver 自身失败由引擎降级红线兜住。
        返回空序列 = 消息缺字段 / 进度回合前缓冲（合法的无帧语义）。
        """
        try:
            return self._translate(msg)
        except Exception:  # noqa: BLE001 — 映射器永不击穿 bridge 主循环
            _logger.warning("openclaw mapper: translate failed → deliver fallback", exc_info=True)
            return self._deliver_fallback(msg)

    def _deliver_fallback(self, msg: dict[str, Any]) -> list[dict[str, Any]]:
        """翻译失败的原文兜底：chat 可知则以 deliver 直投（尽力而为）."""
        chat_id = msg.get("chat_id") if isinstance(msg, dict) else None
        if not isinstance(chat_id, str) or not chat_id:
            return []
        content = msg.get("content")
        text = content if isinstance(content, str) else ("" if content is None else str(content))
        if not text:
            return []
        try:
            env = self._frames.build(
                "deliver", chat_id=chat_id, payload={"text": text},
            )
        except Exception:  # noqa: BLE001 — 兜底构造也失败：只剩日志（不击穿 bridge）
            _logger.error("openclaw mapper: deliver fallback construction failed", exc_info=True)
            return []
        return [env]

    # ── 翻译核心 ────────────────────────────────────────────────────────

    def _translate(self, msg: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(msg, dict):
            return []
        chat_id = msg.get("chat_id")
        content = msg.get("content")
        if not isinstance(chat_id, str) or not chat_id:
            return []
        if not isinstance(content, str):
            content = "" if content is None else str(content)
        meta = msg.get("metadata")
        meta = meta if isinstance(meta, dict) else {}

        ref = self._turns.active(chat_id)
        turn_id = self._host_turn_id(msg)

        # 1) finalize：回合终结（优先级最高）
        if meta.get("finalize"):
            if ref is None or (turn_id and turn_id != ref.turn_id):
                new_id = turn_id or self._new_turn_id()
                ref = self._turns.open(chat_id, new_id, clock=float(self._turn_clock()))
                envelopes = [self._turn_start_env(chat_id, new_id, meta)]
                self._drain_pending(chat_id, new_id, envelopes)
                envelopes.append(self._final_env(chat_id, ref, content, meta))
                self._turns.close(chat_id)
                return envelopes
            envelopes = [self._final_env(chat_id, ref, content, meta)]
            self._turns.close(chat_id)
            return envelopes

        # 2) 结构化进度模板：tool / thinking / notice / interim
        progress = self._progress_event(meta, content)
        if progress is not None:
            name, payload = progress
            if ref is None:
                self._buffer_pending(chat_id, (name, payload))
                return []
            return [self._frames.build(name, chat_id=chat_id, turn_id=ref.turn_id,
                                       payload=dict(payload))]

        # 3) 纯文本进度行（emoji 启发式；仅 streaming 之外的 interim 语义帧——
        #    openclaw bridge 目前无 _interim_send 标记，锚定以官方输出实测为准）
        if meta.get("interim") and not meta.get("streaming"):
            if ref is None:
                # 无卡永不建卡（跑马灯根因防线，与 hermes 适配器同轨）：
                # bridge 层无原生通道可回退，跳过并日志（内容不进卡片管线）
                _logger.info("openclaw mapper: interim without turn dropped chat=%s",
                             chat_id)
                return []
            return [self._frames.build("interim", chat_id=chat_id, turn_id=ref.turn_id,
                                       payload={"text": content})]

        # 4) answer 流（streaming=True；delta/snapshot 双模 D5）
        if meta.get("streaming"):
            if ref is None or (turn_id and turn_id != ref.turn_id):
                new_id = turn_id or self._new_turn_id()
                ref = self._turns.open(chat_id, new_id, clock=float(self._turn_clock()))
                envelopes = [self._turn_start_env(chat_id, new_id, meta)]
                self._drain_pending(chat_id, new_id, envelopes)
                envelopes.append(self._answer_env(chat_id, ref, content, meta))
                return envelopes
            return [self._answer_env(chat_id, ref, content, meta)]

        # 5) 无标记 → deliver（回合外旁路静态卡；空文本无可投递直接跳过）
        if not content:
            return []
        payload: dict[str, Any] = {"text": content}
        title = str(meta.get("title") or "")
        if title:
            payload["title"] = title
        job_id = str(meta.get("job_id") or "")
        if job_id:
            payload["footer"] = {"job_id": job_id}
        return [self._frames.build("deliver", chat_id=chat_id, payload=payload)]

    # ── 素材 ────────────────────────────────────────────────────────────

    @staticmethod
    def _host_turn_id(msg: dict[str, Any]) -> str:
        turn_id = msg.get("turn_id")
        return turn_id if isinstance(turn_id, str) and turn_id else ""

    def _new_turn_id(self) -> str:
        self._turn_counter += 1
        return f"ot-{self._turn_counter:06d}"

    def _turn_start_env(self, chat_id: str, turn_id: str, meta: dict[str, Any]) -> dict[str, Any]:
        payload = {
            "reply_to": str(meta.get("reply_to") or ""),
            "title": str(meta.get("title") or ""),
        }
        return self._frames.build("turn.start", chat_id=chat_id, turn_id=turn_id,
                                  payload=payload)

    def _answer_env(
        self, chat_id: str, ref: Any, content: str, meta: dict[str, Any],
    ) -> dict[str, Any]:
        # delta/snapshot 双模（D5）：incremental=true → 增量帧；否则全量快照
        if meta.get("incremental"):
            event, payload = "answer.delta", {"delta": content}
        else:
            event, payload = "answer.snapshot", {"text": content}
        ref.seq += 1
        ref.streamed = True
        return self._frames.build(event, chat_id=chat_id, turn_id=ref.turn_id,
                                  payload=payload, seq=ref.seq)

    def _final_env(
        self, chat_id: str, ref: Any, content: str, meta: dict[str, Any],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"text": content}
        stats = meta.get("stats")
        if isinstance(stats, dict):
            payload["stats"] = dict(stats)
        ref.seq += 1
        return self._frames.build("answer.final", chat_id=chat_id, turn_id=ref.turn_id,
                                  payload=payload, seq=ref.seq)

    @staticmethod
    def _progress_event(
        meta: dict[str, Any], content: str,
    ) -> "tuple[str, dict[str, Any]] | None":
        if meta.get("tool"):
            return ("tool.start", {"tool": str(meta["tool"]),
                                   "preview": str(meta.get("preview") or "")})
        if meta.get("thinking"):
            return ("thinking.delta", {"text": content})
        notice = meta.get("notice")
        if notice:
            level = str(notice) if str(notice) in ("info", "warn", "error") else "info"
            return ("notice", {"text": content, "level": level})
        return None

    def _buffer_pending(self, chat_id: str, item: tuple[str, dict[str, Any]]) -> None:
        buf = self._pending.setdefault(chat_id, deque())
        buf.append(item)
        while len(buf) > self._pending_max:
            buf.popleft()  # 溢出丢最旧进度行（answer 帧永不进缓冲）

    def _drain_pending(
        self, chat_id: str, turn_id: str, out: list[dict[str, Any]],
    ) -> None:
        buf = self._pending.pop(chat_id, None)
        if buf:
            for event, payload in buf:
                out.append(self._frames.build(event, chat_id=chat_id, turn_id=turn_id,
                                              payload=dict(payload)))
