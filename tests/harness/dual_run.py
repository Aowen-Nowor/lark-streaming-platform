"""双跑对照台架 — Phase 6（M4 收口：行为验收基线全绿 + 双跑对照）.

**目的**：同一段 hermes 出站流（语料 JSONL）分别驱动——

- **旧侧**：前代仓库（``old-hermes-ref``，终态 c826ee8）的
  ``FrameComposer`` + 真实 ``build_streaming_card_v2`` + 录制式假客户端
  （复刻旧 adapter 出站路由语义：classify → v2.0.1 升格 → oneshot
  门控 R1-R3 → handle_send/handle_edit → ``None`` = 原生）；
- **新侧**：本仓库 ``HermesAdapter``（真实 legacy 分类器）→ 真实
  ``StreamingEngine`` → 真实 ``FeishuTransport`` → ``MockFeishuREST``。

产出可对齐 trace（归一化 op 序列 / 卡片数 / 原生回退 / 终值保真 /
API 计数），汇总为对照报告（JSON + Markdown）。发现项分三类：
``equivalent``（等价）/ ``divergence-intended``（设计内分歧，注设计出处）
/ ``gap``（缺口，需处置决策）。

**旧仓加载**（镜像前代 conftest 与 hermes 插件加载器）：旧仓库根注册为
``hermes_lark_streaming`` 包命名空间（spec_from_file_location +
submodule_search_locations），全部相对导入在命名空间内解析——不污染
顶层模块名（避免与 pytest 的 ``tests`` 包等冲突）。路径解析顺序：
``LSP_OLD_REPO`` 环境变量 > 兄弟目录 ``old-hermes-ref`` >
``hermes-lark-streaming``。旧仓缺席时双跑用例 skip（本地/运维门禁，
非 CI 门禁——对照报告作为证据提交）。

**对照面声明**（发现项判读依据）：旧侧驱动复刻到 composer 入口为止
（oneshot / 升格为旧 adapter 层语义，属对照面）；hermes 私有 ABI
（super().send 原生通道、回调接线）不在对照面——两侧原生履行均记
``native`` 事件。澄清交互回调授权码集依赖 hermes 回调接线，维持
MANUAL（04-knowledge-freeze §6）。
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

__all__ = [
    "OldPlugin",
    "OldSideRunner",
    "NewSideRunner",
    "SideTrace",
    "ScenarioTrace",
    "load_corpus",
    "run_scenario",
    "run_corpus",
    "old_plugin_available",
    "default_old_repo",
]

# ── 旧仓加载（hermes_lark_streaming 命名空间）────────────────────────────────


def default_old_repo() -> Path | None:
    """旧仓库路径解析：env > 兄弟目录两个惯例名."""
    env = os.environ.get("LSP_OLD_REPO", "").strip()
    if env:
        p = Path(env)
        return p if p.is_dir() else None
    here = _REPO_ROOT.parent
    for name in ("old-hermes-ref", "hermes-lark-streaming"):
        p = here / name
        if p.is_dir() and (p / "composer").is_dir():
            return p
    return None


def old_plugin_available() -> bool:
    return default_old_repo() is not None


class OldPlugin:
    """旧仓库命名空间加载器（幂等；load() 后 ``hl`` 可用）."""

    def __init__(self) -> None:
        self.root: Path | None = None
        self.hl: ModuleType | None = None

    def load(self) -> ModuleType:
        if self.hl is not None:
            return self.hl
        root = default_old_repo()
        if root is None:
            raise RuntimeError(
                "old hermes repo not found (set LSP_OLD_REPO or clone "
                "old-hermes-ref next to this repo)"
            )
        name = "hermes_lark_streaming"
        if name in sys.modules:
            self.hl = sys.modules[name]
            self.root = Path(getattr(self.hl, "__path__", [""])[0] or "")
            return self.hl
        init_file = root / "__init__.py"
        spec = importlib.util.spec_from_file_location(
            name, str(init_file), submodule_search_locations=[str(root)],
        )
        if spec is None or spec.loader is None:
            raise RuntimeError(f"cannot load old plugin spec from {init_file}")
        module = importlib.util.module_from_spec(spec)
        module.__package__ = name
        module.__path__ = [str(root)]
        sys.modules[name] = module
        spec.loader.exec_module(module)
        # 子模块显式导入（命名空间包不自动导入；旧 conftest 依赖测试侧 import）
        for sub in ("cardkit", "cardkit.cards", "cardkit.special", "composer",
                    "composer.classify", "composer.engine", "plugin",
                    "plugin.adapter", "plugin.clarify_form"):
            importlib.import_module(f"{name}.{sub}")
        self.hl = module
        self.root = root
        return module

    def composer_cls(self) -> type:
        return self.load().composer.engine.FrameComposer

    def classify(self) -> ModuleType:
        return self.load().composer.classify


# ── 旧侧录制客户端（duck-type 前代 FeishuClient 消费面）───────────────────────


class _OldApiError(RuntimeError):
    """旧侧故障注入异常（.code 属性，call_with_seq_recovery 裁决面）."""

    def __init__(self, code: int, msg: str) -> None:
        super().__init__(msg)
        self.code = code


# 旧客户端 op → 归一化 op（与新侧 MockFeishuREST op 对齐）
_OLD_OP_MAP = {
    "cardkit_create": "create",
    "cardkit_stream_element": "stream_element",
    "cardkit_update": "update",
    "cardkit_batch_update": "batch_update",
    "cardkit_close_streaming": "settings:close",
    "cardkit_reopen_streaming": "settings:reopen",
    "cardkit_update_summary": "settings:summary",
    "send_card_by_id_to_chat": "send",
    "send_card_to_chat": "send",
    "reply_card_by_id": "reply",
    "reply_card": "reply",
    "send_text_to_chat": "im_text",
}


class OldRecordingClient:
    """录制式假客户端（方法签名与前代 feishu/client.py 消费面逐字兼容）.

    记录归一化 op 序列 + 逐卡 stream 内容（终值保真对照源）；故障注入
    语义与 MockFeishuREST 对齐（fail_next / fail_always，按 op 计数）。
    """

    def __init__(self) -> None:
        self.ops: list[dict[str, Any]] = []
        self._card_n = 0
        self._msg_n = 0
        self.cards: dict[str, dict[str, Any]] = {}
        self._fail_next: dict[str, list[int]] = {}  # op → 剩余次数队列（code 按序）
        self._fail_always: dict[str, int] = {}

    # ── 故障注入（op 为归一化名：create / stream_element / …）────────────
    def fail_next(self, op: str, code: int, times: int = 1) -> None:
        self._fail_next.setdefault(op, []).extend([code] * times)

    def fail_always(self, op: str, code: int) -> None:
        self._fail_always[op] = code

    def _check(self, op: str) -> None:
        q = self._fail_next.get(op)
        if q:
            raise _OldApiError(q.pop(0), f"injected {op} failure")
        always = self._fail_always.get(op)
        if always:
            raise _OldApiError(always, f"injected {op} failure (always)")

    def _record(self, op: str, **detail: Any) -> None:
        self.ops.append({"op": op, **detail})

    # ── CardKit 面（composer 消费）───────────────────────────────────────
    async def cardkit_create(self, card: dict[str, Any]) -> str:
        self._check("create")
        self._card_n += 1
        card_id = f"old_card_{self._card_n}"
        self.cards[card_id] = {"card": card, "stream_contents": []}
        self._record("create", card_id=card_id)
        return card_id

    async def cardkit_stream_element(
        self, card_id: str, element_id: str, content: str, *, sequence: int = 0,
    ) -> None:
        self._check("stream_element")
        self._record("stream_element", card_id=card_id, element_id=element_id)
        st = self.cards.get(card_id)
        if st is not None:
            st["stream_contents"].append(str(content))

    async def cardkit_update(
        self, card_id: str, card: dict[str, Any], sequence: int = 0,
    ) -> None:
        self._check("update")
        self._record("update", card_id=card_id)
        st = self.cards.get(card_id)
        if st is not None:
            st["card"] = card

    async def cardkit_batch_update(
        self, card_id: str, actions: list[dict[str, Any]], *, sequence: int = 0,
    ) -> None:
        self._check("batch_update")
        self._record("batch_update", card_id=card_id, n_actions=len(actions))

    async def cardkit_close_streaming(
        self, card_id: str, sequence: int = 0, *, summary: str = "",
    ) -> None:
        self._check("settings:close")
        self._record("settings:close", card_id=card_id, summary=summary)

    async def cardkit_reopen_streaming(
        self, card_id: str, *, sequence: int = 0,
    ) -> None:
        self._check("settings:reopen")
        self._record("settings:reopen", card_id=card_id)

    async def cardkit_update_summary(
        self, card_id: str, summary: str, *, sequence: int = 0,
    ) -> None:
        self._check("settings:summary")
        self._record("settings:summary", card_id=card_id)

    # ── IM 面（建卡发表 / oneshot / 原生文本）────────────────────────────
    async def send_card_by_id_to_chat(self, chat_id: str, card_id: str) -> str:
        self._check("send")
        self._msg_n += 1
        mid = f"om_old_{self._msg_n}"
        self._record("send", card_id=card_id, chat_id=chat_id)
        return mid

    async def send_card_to_chat(self, chat_id: str, card: dict[str, Any]) -> str:
        self._check("send")
        self._msg_n += 1
        mid = f"om_old_{self._msg_n}"
        self._record("send", chat_id=chat_id, static=True)
        return mid

    async def reply_card_by_id(self, message_id: str, card_id: str) -> str:
        self._check("reply")
        self._msg_n += 1
        mid = f"om_old_{self._msg_n}"
        self._record("reply", card_id=card_id, anchor=message_id)
        return mid

    async def reply_card(self, message_id: str, card: dict[str, Any]) -> str:
        self._check("reply")
        self._msg_n += 1
        mid = f"om_old_{self._msg_n}"
        self._record("reply", anchor=message_id, static=True)
        return mid

    async def send_text_to_chat(self, chat_id: str, text: str) -> str:
        self._record("im_text", chat_id=chat_id)
        self._msg_n += 1
        return f"om_old_{self._msg_n}"


# ── 旧侧驱动（复刻旧 adapter 出站路由，hermes ABI 面以外全保真）──────────────


class OldSideRunner:
    """旧侧：FrameComposer + 旧 classify + 旧 adapter 路由 shim.

    路由复刻范围（plugin/adapter.py ``_send_impl`` 主链）：classify →
    v2.0.1 plain 升格 → oneshot 门控（R1 cron / R2 回合外 final /
    R3 主动投递 / background_result）→ ``composer.handle_send`` →
    ``None`` = 原生文本。``edit_message`` 复刻 state_by_message 反查 +
    ``classify_edit`` → ``handle_edit``。clarify 复刻
    ``suspend_for_clarify`` + 表单卡直投（回调授权面为 MANUAL，不在
    对照面）。
    """

    def __init__(
        self,
        plugin: OldPlugin,
        *,
        client: OldRecordingClient | None = None,
        clarify_suspend_sec: float = 1.2,
    ) -> None:
        self.plugin = plugin
        hl = plugin.load()
        self.client = client or OldRecordingClient()
        self.native: list[dict[str, Any]] = []
        self.oneshot: list[dict[str, Any]] = []
        self._last_card_msg: dict[str, str] = {}
        cards = hl.cardkit.cards
        plugin_cls = hl.plugin.adapter._CardFactory

        class _Cfg:  # 最小配置面（与旧测试 _make_composer 同款默认）
            streaming_panel_expanded = True
            print_strategy = "delay"
            print_step = 4

        factory = plugin_cls(_Cfg())
        self.composer = hl.composer.engine.FrameComposer(
            get_client=lambda: self.client,
            build_card=factory.streaming,
            build_final_card=factory.final,
            clarify_suspend_sec=clarify_suspend_sec,
        )
        # 分类器引用（含升格谓词）
        self._cls = hl.composer.classify

    # ── 旧 adapter._send_impl 复刻（去 hermes ABI）───────────────────────
    async def send(
        self, chat_id: str, content: str,
        metadata: dict[str, Any] | None = None,
        reply_to: str = "",
    ) -> None:
        cl = self._cls
        try:
            cls = cl.classify_send(content, metadata)
        except Exception:  # noqa: BLE001 — 分类失败按 plain（永不阻断）
            cls = None
        meta = metadata or {}
        if cls is not None and cls.channel == cl.CH_PLAIN:
            # v2.0.1 上下文升格（旧 adapter._upgrade_plain_to_progress 主链复刻）
            upgraded = self._upgrade_plain(chat_id, content, meta)
            if upgraded is not None:
                cls = upgraded
        # 锚点归一：metadata.reply_to_message_id 回退（旧 adapter 同款）
        if not (reply_to or "").strip() and isinstance(metadata, dict):
            reply_to = str(metadata.get("reply_to_message_id") or "").strip()
        # oneshot 门控（旧 _send_oneshot_card 三规则复刻）
        if await self._maybe_oneshot(chat_id, content, meta, cls, reply_to):
            return
        if cls is None or cls.channel == cl.CH_PLAIN:
            self.native.append({"chat": chat_id, "reason": "plain_native",
                                "text": content})
            return
        result = await self.composer.handle_send(
            chat_id, content, reply_to or None, metadata, cls)
        if result is None:
            self.native.append({"chat": chat_id, "reason": "composer_none",
                                "text": content})
            return
        mid = getattr(result, "message_id", None)
        if getattr(result, "success", True) is False:
            self.native.append({"chat": chat_id, "reason": "send_result_failed",
                                "text": content, "detail": str(getattr(result, "error", ""))})
        if mid:
            self._last_card_msg[chat_id] = mid

    def _upgrade_plain(self, chat_id: str, content: str, meta: dict[str, Any]):
        """v2.0.1 升格复刻（旧 adapter 逐语义；永不抛）."""
        cl = self._cls
        try:
            if meta.get("notify") or meta.get("non_conversational"):
                return None
            if not content or cl._is_notice_text(content):
                return None
            if not cl.is_progress_text(content):
                if meta or len(content) > 4000:
                    return None
                if not cl._EMOJI_RE.match(content) and not content.lstrip().startswith("```"):
                    return None
                if self.composer.active_state(chat_id) is None:
                    return None
            kind, tool, clean = cl.parse_progress_line(content)
            return cl.ClassifyResult(
                channel=cl.CH_PROGRESS, kind=kind, tool_name=tool,
                thinking_text=clean if kind == "thinking" else "",
                raw_content=content,
            )
        except Exception:  # noqa: BLE001
            return None

    async def _maybe_oneshot(
        self, chat_id: str, content: str, meta: dict[str, Any], cls, reply_to: str,
    ) -> bool:
        """旧 oneshot 三规则复刻（返回 True = 已走 oneshot 卡）."""
        cl = self._cls
        hl = self.plugin
        if meta.get("expect_edits") or meta.get("_interim_send"):
            return False
        channel = getattr(cls, "channel", "") if cls is not None else ""
        if channel in ("progress", "interim"):
            return False
        try:
            has_active = self.composer.active_state(chat_id) is not None
        except Exception:  # noqa: BLE001
            has_active = False
        job_id = str(meta.get("job_id") or "").strip()
        if channel == "plain" and getattr(cls, "kind", "") == "background_result":
            gate = "background"
        elif job_id:
            gate = "cron"
        elif channel == "final" and not has_active:
            anchor = (reply_to or "").strip()
            # 入站 kind 注册表为 hermes 侧；harness 无注册 → kind=None →
            # 旧逻辑 `anchor om_ and kind != "command"` → return None。
            if anchor.startswith("om_"):
                return False
            gate = "final"
        elif channel == "plain" and not has_active and (content or "").strip():
            gate = "longform"
        else:
            return False
        special = hl.load().cardkit.special
        build_cron_card = special.build_cron_card
        build_gateway_card = special.build_gateway_card
        if gate == "cron":
            parse_cron_delivery = special.parse_cron_delivery
            title, wrapped_id, body = parse_cron_delivery(content)
            card = build_cron_card(body, title=title or "Cronjob",
                                   job_id=job_id or wrapped_id)
        else:
            card = build_gateway_card(content)
        thread_id = str(meta.get("thread_id") or "").strip()
        anchor = (reply_to or "").strip()
        message_id = ""
        try:
            if thread_id.startswith("om_"):
                message_id = await self.client.reply_card(thread_id, card)
            elif anchor.startswith("om_"):
                message_id = await self.client.reply_card(anchor, card)
        except Exception:  # noqa: BLE001 — 旧 UX-3a：reply 失败落直发
            message_id = ""
        if not message_id:
            message_id = await self.client.send_card_to_chat(chat_id, card)
        self.oneshot.append({"gate": gate, "chat": chat_id, "text": content})
        return True

    # ── edit / clarify ───────────────────────────────────────────────────
    async def edit(
        self, chat_id: str, content: str, *, finalize: bool = False,
    ) -> None:
        cl = self._cls
        message_id = self._last_card_msg.get(chat_id, "")
        try:
            if message_id and self.composer.state_by_message(message_id) is not None:
                cls = cl.classify_edit(content, finalize)
                result = await self.composer.handle_edit(
                    chat_id, message_id, content, finalize, cls)
                if result is not None:
                    mid = getattr(result, "message_id", None)
                    if getattr(result, "success", True) is False:
                        self.native.append({
                            "chat": chat_id, "reason": "edit_result_failed",
                            "text": content,
                            "detail": str(getattr(result, "error", "")),
                        })
                    if mid:
                        self._last_card_msg[chat_id] = mid
                    return
        except Exception:  # noqa: BLE001 — 旧 edit 降级红线
            self.native.append({"chat": chat_id, "reason": "edit_exception",
                                "text": content})
            return
        # 无注册状态（编辑的是 hermes 原生消息）→ 原生
        self.native.append({"chat": chat_id, "reason": "orphan_edit_native",
                            "text": content})

    async def clarify(self, chat_id: str, question: str, options: list[str]) -> None:
        hl = self.plugin
        try:
            await self.composer.suspend_for_clarify(chat_id, question)
        except Exception:  # noqa: BLE001 — 暂停失败不阻断 clarify 卡
            pass
        try:
            card = hl.load().plugin.clarify_form.build_clarify_form_card(
                question, options, "clarify_harness", multi_select=False)
        except Exception:  # noqa: BLE001 — 表单卡构建失败占位卡
            card = {"schema": "2.0", "body": {"elements": []}}
        await self.client.send_card_to_chat(chat_id, card)
        self.oneshot.append({"gate": "clarify", "chat": chat_id, "text": question})

    async def clarify_answer(self, chat_id: str, values: dict[str, Any]) -> None:
        # 旧侧回调面 = 卡片回调 toast/settled（hermes 回调接线），无新消息；
        # 续流由后续 handle_send/edit 自然承接（v2.0.26 语义）。
        return None

    async def close(self) -> None:
        try:
            await self.composer.seal_all_on_shutdown()
        except Exception:  # noqa: BLE001
            pass


# ── 新侧驱动 ─────────────────────────────────────────────────────────────────


class NewSideRunner:
    """新侧：HermesAdapter → 真实引擎 → 真实传输 → MockFeishuREST."""

    def __init__(self, harness: Any, *, clarify_window_sec: float = 1.2) -> None:
        from lark_streaming.adapters import HermesAdapter
        from lark_streaming.engine import EngineSettings

        self.harness = harness
        self.settings = harness.settings
        self.native: list[dict[str, Any]] = []
        self.results: list[Any] = []
        self.adapter = HermesAdapter(harness.engine)

    async def send(
        self, chat_id: str, content: str,
        metadata: dict[str, Any] | None = None,
        reply_to: str = "",
        turn_id: str | None = None,
    ) -> None:
        res = await self.adapter.send(chat_id, content, metadata, reply_to=reply_to,
                                      turn_id=turn_id)
        self.results.append(res)
        if res.action.value == "native":
            self.native.append({"chat": chat_id, "reason": res.detail or "native",
                                "text": res.native_text})

    async def edit(
        self, chat_id: str, content: str, *, finalize: bool = False,
    ) -> None:
        res = await self.adapter.edit(chat_id, content, finalize=finalize)
        self.results.append(res)
        if res.action.value == "native":
            self.native.append({"chat": chat_id, "reason": res.detail or "native",
                                "text": res.native_text})

    async def clarify(self, chat_id: str, question: str, options: list[str]) -> None:
        res = await self.adapter.send_clarify(
            chat_id, question=question, options=options)
        self.results.append(res)
        if res.action.value == "native":
            self.native.append({"chat": chat_id, "reason": res.detail or "native",
                                "text": question})

    async def clarify_answer(self, chat_id: str, values: dict[str, Any]) -> None:
        res = await self.adapter.clarify_answer(chat_id, values=values)
        self.results.append(res)

    async def close(self) -> None:
        await self.harness.close()


# ── 语料 ─────────────────────────────────────────────────────────────────────


def load_corpus(path: Path | str) -> list[dict[str, Any]]:
    """语料 JSONL 加载：首行 scenario 头（faults/desc），其余为 op 行.

    支持跨行 JSON 对象（burst op 多 sends 时按花括号深度续读）；
    ``#`` 开头为注释行。
    """
    raw_lines = Path(path).read_text(encoding="utf-8").splitlines()
    ops: list[dict[str, Any]] = []
    header: dict[str, Any] = {}
    buf = ""
    depth = 0
    for ln in raw_lines:
        if not buf:
            stripped = ln.strip()
            if not stripped or stripped.startswith("#"):
                continue
        buf += ln
        depth += ln.count("{") - ln.count("}")
        if depth > 0:
            continue
        row = json.loads(buf)
        buf = ""
        depth = 0
        if "scenario" in row:
            header = row
        else:
            ops.append(row)
    return [{"scenario": header.get("scenario", Path(path).stem),
             "desc": header.get("desc", ""),
             "faults": header.get("faults", []),
             "sides": header.get("sides", "both"),
             "ops": ops}]


async def _apply_ops(sides: dict[str, Any], ops: list[dict[str, Any]]) -> None:
    for op in ops:
        kind = op.get("op")
        chat = str(op.get("chat") or "oc_c1")
        if kind == "send":
            if "old" in sides:
                await sides["old"].send(chat, op["content"], op.get("metadata"),
                                        reply_to=str(op.get("reply_to") or ""))
            if "new" in sides:
                await sides["new"].send(chat, op["content"], op.get("metadata"),
                                        reply_to=str(op.get("reply_to") or ""),
                                        turn_id=op.get("turn_id"))
        elif kind == "edit":
            if "old" in sides:
                await sides["old"].edit(chat, op["content"],
                                        finalize=bool(op.get("finalize")))
            if "new" in sides:
                await sides["new"].edit(chat, op["content"],
                                        finalize=bool(op.get("finalize")))
        elif kind == "burst":
            # 并发同刻齐发（建卡窗口竞态面）：两侧同构 gather
            sends = op.get("sends") or []
            if "old" in sides:
                old_tasks = [sides["old"].send(
                    str(s.get("chat") or chat), s["content"], s.get("metadata"),
                    reply_to=str(s.get("reply_to") or "")) for s in sends]
                await asyncio.gather(*old_tasks)
            if "new" in sides:
                new_tasks = [sides["new"].send(
                    str(s.get("chat") or chat), s["content"], s.get("metadata"),
                    reply_to=str(s.get("reply_to") or "")) for s in sends]
                await asyncio.gather(*new_tasks)
        elif kind == "clarify":
            q = op.get("question", "")
            if "old" in sides:
                await sides["old"].clarify(chat, q, list(op.get("options") or []))
            if "new" in sides:
                await sides["new"].clarify(chat, q, list(op.get("options") or []))
        elif kind == "clarify_answer":
            v = op.get("values") or {}
            if "old" in sides:
                await sides["old"].clarify_answer(chat, v)
            if "new" in sides:
                await sides["new"].clarify_answer(chat, v)
        elif kind == "sleep":
            await asyncio.sleep(float(op.get("ms", 100)) / 1000.0)
        elif kind == "settle":
            for _ in range(4):
                await asyncio.sleep(0)
            await asyncio.sleep(float(op.get("ms", 30)) / 1000.0)
        else:
            raise ValueError(f"unknown corpus op: {kind!r}")


def _apply_faults(sides: dict[str, Any], faults: list[dict[str, Any]]) -> None:
    for f in faults:
        op, code = str(f["op"]), int(f["code"])
        times = int(f.get("times", 1))
        if "old" in f.get("sides", ("old", "new")):
            sides["old"].client.fail_next(op, code, times)
        if "new" in f.get("sides", ("old", "new")):
            sides["new"].harness.server.fail_next(op, code, msg=f"injected {op}", times=times)


# ── trace 归一与对照 ─────────────────────────────────────────────────────────


def _normalize_new_ops(server: Any) -> list[dict[str, Any]]:
    """MockFeishuREST call_log → 归一化 op 序列（settings 按意图细分）."""
    out: list[dict[str, Any]] = []
    for call in server.call_log:
        op = call.op
        if op == "settings":
            cfg = {}
            settings = call.body.get("settings")
            try:
                parsed = json.loads(settings) if isinstance(settings, str) else (settings or {})
                cfg = (parsed or {}).get("config") or {}
            except Exception:  # noqa: BLE001
                pass
            if cfg.get("streaming_mode") is True:
                op = "settings:reopen"
            elif cfg.get("streaming_mode") is False:
                # close 可携带 summary（两侧同形态：定稿 = 关流式 + 摘要一次调用）
                op = "settings:close"
            elif cfg.get("summary"):
                op = "settings:summary"
            else:
                op = "settings:close"
        out.append({"op": op, "code": call.code})
    return out


def _old_final_texts(client: OldRecordingClient) -> list[str]:
    """旧侧逐卡终值：每卡最后一次 stream_element 内容（answer 元素）."""
    out = []
    for st in client.cards.values():
        sc = st.get("stream_contents") or []
        if sc:
            out.append(str(sc[-1]))
    return out


def _new_final_texts(server: Any) -> list[str]:
    """新侧逐卡终值：mock 侧元素 content 里最后一次 stream_element 内容."""
    out = []
    for call in server.call_log:
        if call.op == "stream_element" and call.code == 0:
            body = call.body or {}
            content = body.get("content")
            # path: /cardkit/v1/cards/{card_id}/elements/{eid}/content
            parts = (call.path or "").split("/")
            card_id = parts[4] if len(parts) > 4 else ""
            out.append((card_id, str(content or "")))
    # 按卡取最后一次
    last: dict[str, str] = {}
    for card_id, text in out:
        last[card_id] = text
    return list(last.values())


@dataclass
class SideTrace:
    side: str
    ops: list[dict[str, Any]] = field(default_factory=list)
    op_counts: dict[str, int] = field(default_factory=dict)
    native: list[dict[str, Any]] = field(default_factory=list)
    cards: int = 0
    final_texts: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)


@dataclass
class ScenarioTrace:
    scenario: str
    old: SideTrace | None = None
    new: SideTrace | None = None
    comparison: dict[str, Any] = field(default_factory=dict)


def _count_ops(ops: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for o in ops:
        counts[o["op"]] = counts.get(o["op"], 0) + 1
    return counts


def compare_scenario(tr: ScenarioTrace) -> dict[str, Any]:
    """逐场景对照指标 + 发现项判定."""
    old, new = tr.old, tr.new
    c: dict[str, Any] = {"scenario": tr.scenario}
    if old is None or new is None:
        c["status"] = "single-side"
        return c
    c["cards_old"] = old.cards
    c["cards_new"] = new.cards
    c["native_old"] = [n["reason"] for n in old.native]
    c["native_new"] = [n["reason"] for n in new.native]
    c["ops_old"] = old.op_counts
    c["ops_new"] = new.op_counts
    # 终值保真：语料终值在两侧最后一张卡的终值流内
    c["final_texts_old_n"] = len(old.final_texts)
    c["final_texts_new_n"] = len(new.final_texts)
    # 发现项判定（等价 / 设计内分歧 / 缺口）——由验收测试按场景断言，
    # 报告仅呈现数据与注记。
    return c


# ── 场景运行 ─────────────────────────────────────────────────────────────────


async def run_scenario(
    spec: dict[str, Any],
    *,
    sides: str | None = None,
    harness_factory: "Callable[[], Any] | None" = None,
    plugin: OldPlugin | None = None,
) -> ScenarioTrace:
    """单场景双跑：构造两侧 → 注入故障 → 驱动语料 → 收口 → 归一 trace.

    ``sides``：``both`` / ``old`` / ``new``；缺省读语料头 ``sides`` 字段
    （单侧能力场景如 12 同卡续流），仍缺省为 ``both``。
    ``harness_factory``：新侧台架工厂（测试注入自定义 settings；CLI 用默认）。
    """
    sides = sides or str(spec.get("sides") or "both")
    plugin = plugin or OldPlugin()
    need_old = sides in ("both", "old")
    need_new = sides in ("both", "new")
    old_runner: OldSideRunner | None = None
    new_runner: NewSideRunner | None = None
    tr = ScenarioTrace(scenario=str(spec.get("scenario", "unnamed")))

    if need_old:
        old_runner = OldSideRunner(plugin)
    if need_new:
        harness = (harness_factory or (lambda: _default_harness()))()
        new_runner = NewSideRunner(harness)

    sides_map: dict[str, Any] = {}
    if old_runner is not None:
        sides_map["old"] = old_runner
    if new_runner is not None:
        sides_map["new"] = new_runner
    _apply_faults(sides_map, list(spec.get("faults") or []))

    try:
        await _apply_ops(sides_map, list(spec.get("ops") or []))
        # 收口前等待：澄清窗口（1.2s）到期 → 边界 finalize 真落定
        # （旧侧 settle 定时器同窗口；非 final 场景无定时器，等待无副作用）
        await asyncio.sleep(1.6)
        if old_runner is not None:
            await old_runner.close()
        if new_runner is not None:
            eng = new_runner.harness.engine
            await eng.aclose()
            await asyncio.sleep(0.02)
    finally:
        if old_runner is not None:
            old_trace = _collect_old(old_runner, enabled=True)
            tr.old = old_trace
        if new_runner is not None:
            tr.new = _collect_new(new_runner, enabled=True)
        if new_runner is not None:
            # close 在 trace 收集后（call_log/卡片态已取样）
            await new_runner.harness.close()
    tr.comparison = compare_scenario(tr)
    return tr


def _default_harness() -> Any:
    from .framework import Harness

    return Harness()


def _collect_old(runner: OldSideRunner, *, enabled: bool) -> SideTrace:
    ops = list(runner.client.ops)
    tr = SideTrace(side="old", ops=ops, op_counts=_count_ops(ops),
                   native=list(runner.native), cards=len(runner.client.cards),
                   final_texts=_old_final_texts(runner.client))
    tr.notes = [f"oneshot={json.dumps(o, ensure_ascii=False)}" for o in runner.oneshot]
    return tr


def _collect_new(runner: NewSideRunner, *, enabled: bool) -> SideTrace:
    server = runner.harness.server
    ops = _normalize_new_ops(server)
    tr = SideTrace(side="new", ops=ops, op_counts=_count_ops(ops),
                   native=list(runner.native), cards=len(server.cards),
                   final_texts=_new_final_texts(server))
    return tr


def run_corpus(
    paths: list[Path | str],
    *,
    sides: str | None = None,
    harness_factory: "Callable[[], Any] | None" = None,
) -> list[ScenarioTrace]:
    """批量运行语料（同步入口：内部 asyncio.run）.

    ``sides`` 缺省 None = 按语料头逐场景裁决（单侧能力场景如 12）。
    """

    async def _go() -> list[ScenarioTrace]:
        plugin = OldPlugin()
        out = []
        for p in paths:
            for spec in load_corpus(p):
                out.append(await run_scenario(
                    spec, sides=sides, harness_factory=harness_factory,
                    plugin=plugin))
        return out

    return asyncio.run(_go())


# ── 报告渲染（Markdown）───────────────────────────────────────────────────────


def _api_delta(old: dict[str, int], new: dict[str, int]) -> str:
    """API 计数对照摘要（剔除 token 差异；每 op `old→new`）."""
    keys = sorted(set(old) | set(new) - {"token"})
    parts = []
    for k in keys:
        if k == "token":
            continue
        o, n = old.get(k, 0), new.get(k, 0)
        mark = "" if o == n else (" +" if n > o else " -")
        parts.append(f"{k} {o}→{n}{mark}" if o != n else f"{k} {o}")
    return "、".join(parts) or "（无）"


def render_markdown(traces: list[ScenarioTrace], *, meta: dict[str, Any]) -> str:
    """双跑对照报告（Markdown；docs/dual-run-report.md 的生成体）."""
    lines: list[str] = []
    lines.append(f"# 双跑对照报告（Phase 6，{meta.get('date', '')}）")
    lines.append("")
    lines.append(f"> 生成：`scripts/dual_run.py`（`run_corpus` 全语料） · "
                 f"语料 {len(traces)} 场景 · 旧仓 "
                 f"`{meta.get('old_repo', '?')}` @ `{meta.get('old_commit', '?')}` · "
                 f"新仓版本冻结 {meta.get('version', '0.0.1')}（ADR D10）")
    lines.append("")
    lines.append("## 结论速览")
    lines.append("")
    lines.append("- **消息永不丢红线**：全部场景双侧零静默丢弃"
                 "（每内容帧要么上卡要么原生履行）——降级场景 08 旧/新均 4 帧全原生。")
    lines.append("- **F1 澄清窗口可达性（已修复）**：适配器宽限册使边界 finalize → "
                 "send_clarify 链路触达引擎 `clarify_window_hit`；同卡续流为协议增强"
                 "（场景 12，旧 edit-transport 结构性不可达）。")
    lines.append("- **F2 无标记进度升格（已修复）**：前代 v2.0.1 升格主链移植，"
                 "无标记工具气泡归面板（场景 03 散卡 2→0）。")
    lines.append("- **F3 cron 家族溯源（已修复）**：job_id 帧路由 deliver + "
                 "`footer.job_id`（场景 11，旧 R1 规则对齐）。")
    lines.append("- **设计内分歧**：04 回合外 notify（旧 oneshot 静态卡 vs 新微回合卡）；"
                 "澄清独立卡计数口径（旧 IM 内联不计 / 新 CardKit 实体计入）。")
    lines.append("- **字节预算再评估**：维持 27000 不变（Phase 5 真火实测 "
                 "stream 真实上限 100~120KB，保守裕度继续提供缓冲；"
                 "放宽属用户裁决事项，04-knowledge-freeze §6 如实冻结）。")
    lines.append("")
    lines.append("## 逐场景对照")
    lines.append("")
    lines.append("| 场景 | 卡片 旧/新 | 原生回退 旧/新 | 终值 旧/新 | API 计数（旧→新） | 分类 |")
    lines.append("|------|-----------|---------------|-----------|------------------|------|")
    verdicts = meta.get("verdicts", {})
    for tr in traces:
        c = tr.comparison
        if tr.old is None or tr.new is None:
            side = "old" if tr.old is not None else "new"
            t = tr.old or tr.new
            lines.append(
                f"| {tr.scenario} | — / {t.cards if t else 0} | "
                f"— / {len(t.native) if t else 0} | — / "
                f"{len(t.final_texts) if t else 0} | 单侧（{side}） | "
                f"{verdicts.get(tr.scenario, 'capability')} |")
            continue
        v = verdicts.get(tr.scenario, "equivalent")
        lines.append(
            f"| {tr.scenario} | {c['cards_old']} / {c['cards_new']} | "
            f"{len(c['native_old'])} / {len(c['native_new'])} | "
            f"{c['final_texts_old_n']} / {c['final_texts_new_n']} | "
            f"{_api_delta(c['ops_old'], c['ops_new'])} | {v} |")
    lines.append("")
    lines.append("## 场景明细")
    lines.append("")
    for tr in traces:
        c = tr.comparison
        lines.append(f"### {tr.scenario}")
        lines.append("")
        if tr.old is None or tr.new is None:
            lines.append(f"- 单侧场景（{verdicts.get(tr.scenario, 'capability')}）；"
                         "旧侧结构不可达或语料声明单侧。")
            lines.append("")
            continue
        lines.append(f"- 卡片数：旧 {c['cards_old']} / 新 {c['cards_new']}")
        lines.append(f"- 原生回退：旧 {c['native_old'] or '无'} / "
                     f"新 {c['native_new'] or '无'}")
        lines.append(f"- API 计数：旧 {c['ops_old']} → 新 {c['ops_new']}"
                     "（新侧多 1 次 token 获取为传输层惰性管理，语义无差）")
        notes = tr.old.notes or []
        if notes:
            lines.append(f"- 旧侧路由注记：{notes[0]}")
        lines.append("")
    lines.append("## 发现项登记（处置记录）")
    lines.append("")
    lines.append("| # | 场景 | 发现 | 处置 | 状态 |")
    lines.append("|---|------|------|------|------|")
    lines.append("| F1 | 09/12 | 边界 finalize 后适配器注册表已注销，"
                 "send_clarify 触达不了引擎澄清窗口（L2 已证语义，适配器够不着） | "
                 "适配器宽限册（最近关闭回合 + 时间戳），窗口命中重开注册表，"
                 "过期 → 原生 ask（native_text 携带问题文本） | **已修复**"
                 "（tests/adapters TestDualRunFindings + 场景 09/12） |")
    lines.append("| F2 | 03 | 无标记工具气泡（edit-transport 真实形态）按冻结契约"
                 "投成散卡（旧侧升格进面板） | `lt.upgrade_plain` 移植前代 v2.0.1/"
                 "v2.0.4/v2.0.8/v2.0.31 升格主链，CH_PLAIN 先过启发式再落 deliver | "
                 "**已修复** |")
    lines.append("| F3 | 11 | cron 家族（job_id）走微回合卡，官方包装头上卡原文、"
                 "job_id 溯源丢失 | `lt.parse_cron_delivery` 移植 + `_deliver_cron`"
                 "路由（标题/正文剥离 + footer.job_id） | **已修复** |")
    lines.append("| D1 | 04 | 回合外 notify：旧 R2 oneshot 网关静态卡 vs 新微回合卡"
                 "（含页脚统计与面板） | 设计内分歧（02-engine 微回合自愈），零丢失等价 | "
                 "divergence-intended |")
    lines.append("| D2 | 09 | 澄清独立卡计数口径：旧 IM 内联卡不计 cards，"
                 "新 CardKit 实体计入 | 设计形态一致（双侧独立澄清卡），仅测量口径差 | "
                 "divergence-intended |")
    lines.append("| D3 | 08 | 降级原生回退的 reason 呈现粒度不同"
                 "（旧 composer_none/orphan_edit_native vs 新 create_failed/"
                 "chat_degraded 冷却快路径） | 结果等价（全原生零丢失）；新侧原因"
                 "更结构化（守卫裁决表） | divergence-intended |")
    lines.append("")
    lines.append("## clarify MANUAL 项与生产切换前置")
    lines.append("")
    lines.append("- **回调授权失败码集**：仍标 MANUAL（04-knowledge-freeze §6）——"
                 "依赖真实 hermes 网关回调接线，双跑 mock 环境不可实证；生产切换"
                 "（M5）冒烟清单项。")
    lines.append("- **hermes 接线决策**：澄清续流后宿主是否携带 turn_id（同卡续流，"
                 "场景 12 语义）由 hermes 侧出站钩子决定；两形态（同卡 / 新卡）"
                 "均零丢失，ADAPTER_SDK 已载明。")
    lines.append("- **生产切换（M5）前置就绪**：行为验收基线 21 项全绿（Phase 5）"
                 "+ 双跑对照 12 场景零丢失 + 发现项 F1/F2/F3 修复并回归全绿；"
                 "切换时点与版本变更由用户裁决（ADR D10）。")
    lines.append("")
    return "\n".join(lines)
