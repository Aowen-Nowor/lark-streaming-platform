"""MockFeishuREST — 飞书 Open API HTTP 层 mock（04-knowledge-freeze §5）.

前代 ``tests/e2e/mock_feishu.py`` 的移植：在 ``httpx.MockTransport`` 层
应答飞书 Open API——端点路径与 wire 形态逐字对齐
``lark_streaming/transport/client.py``（data / actions / settings 均为
JSON 字符串；token 端点响应**不包 data 层**，token.py 直接
``body.get("tenant_access_token")``），使真实 FeishuTransport 全链路
（路径、token 管理、retry_transient、seq 自愈、错误解析）在环。

端点面（base = https://open.feishu.cn/open-apis）：
- POST /auth/v3/tenant_access_token/internal → ``{"code":0,
  "tenant_access_token":"t-mock-1","expire":7200}``
- POST /im/v1/messages（query receive_id_type=chat_id）→ data.message_id
- POST /im/v1/messages/{mid}/reply → data.message_id
- POST /cardkit/v1/cards（body.type="card_json"，body.data=卡片 JSON 字符串）
  → data.card_id
- PUT /cardkit/v1/cards/{id}（body.card.data 字符串 + sequence）
- PUT /cardkit/v1/cards/{id}/elements/{eid}/content（body.content + sequence）
- PUT /cardkit/v1/cards/{id}/batch_update（body.actions=JSON 字符串 + sequence）
- PUT /cardkit/v1/cards/{id}/settings（body.settings=JSON 字符串 + sequence）

语义校验（宽松但诚实；错误以飞书 wire 形态 ``{"code":...,"msg":...}``
返回，HTTP 200 承载业务码、由 transport.parse_api_response 裁决）：
- stream_element 打到不存在的卡/元素 → **300313**（Q1 邻域：add 后
  未持久化竞态码族）；
- streaming_mode 已关再 stream_element → **300309**（前代定稿后迟到流帧）；
- ``strict_sequence`` 开启后 sequence ≤ 已见值 → **300317**（Q1：
  并发/重试导致序列错位，10-11 孤儿卡事故同族）；
- ``expire_streaming`` 武装后 stream_element/settings → **200850**（Q2：
  官方 ~600s 流式会话寿命，11-07 12:01 与 15:08 两例撞线），直到收到
  settings ``streaming_mode:true``（官方恢复动作）解除恢复。

L3 纪律（02-engine §10）：mock 不依赖真实网络时序；真实值只作断言
区间与默认参数来源，保证 L2 可重复。
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, NamedTuple

import httpx

from lark_streaming.transport.errors import (
    CARDKIT_CARD_RATE_LIMIT,      # 200937（audit #8：卡级频控入册 2026-11-20）
    CARDKIT_ELEMENT_NOT_FOUND,   # 300313
    CARDKIT_SEQUENCE_CONFLICT,   # 300317
    CARDKIT_STREAMING_CLOSED,    # 300309
    CARDKIT_STREAMING_TIMEOUT,   # 200850
)

__all__ = [
    "MockCall", "MockCardState", "MockMessage", "MockFeishuREST",
]

_OPEN_APIS_PREFIX = "/open-apis"
_TOKEN_PATH = "/auth/v3/tenant_access_token/internal"
_IM_MESSAGES = "/im/v1/messages"
_REPLY_SUFFIX = "/reply"
_CARDS_PREFIX = "/cardkit/v1/cards/"
_CARDS_CREATE = "/cardkit/v1/cards"
_BATCH_SUFFIX = "/batch_update"
_SETTINGS_SUFFIX = "/settings"
_CONTENT_SUFFIX = "/content"

#: 故障注入 op 封闭集（fail_next / fail_always / delay 的 key）
OPS = (
    "token", "create", "send", "reply", "update",
    "stream_element", "batch_update", "settings",
)


class MockCall(NamedTuple):
    """一次 HTTP 调用的观测记录（op, path, body, code 序列）."""

    op: str
    method: str
    path: str
    body: dict[str, Any]
    code: int  # 本次应答承载的飞书业务码（成功 = 0）


@dataclass
class MockCardState:
    """CardKit 实体卡在 mock 侧的状态（引擎可见行为的权威镜像）."""

    card_id: str
    card_json: dict[str, Any]                 # 当前卡片全量 JSON
    elements: dict[str, dict[str, Any]]       # element_id → element（引用）
    streaming_mode: bool
    last_sequence: int = 0                    # 已见最大 sequence（strict 判据）
    settings_log: list[dict[str, Any]] = field(default_factory=list)
    actions_log: list[list[dict[str, Any]]] = field(default_factory=list)
    created_at: float = field(default_factory=time.monotonic)
    strict_sequence: bool = False             # strict_sequence(card_id, True) 开启
    expired: bool = False                     # 200850 武装（expire_streaming）


@dataclass
class MockMessage:
    """IM 消息投递记录（send / reply 共用）."""

    message_id: str
    receive_id: str
    msg_type: str
    content: Any                              # 解析后的 content JSON（失败原串）
    reply_to: str = ""                        # reply 锚点 message_id
    card_id: str = ""                         # content 为 {"type":"card",...} 时登记


def _parse_json(raw: Any) -> Any:
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return raw
    return raw


def _seq_of(body: dict[str, Any]) -> int:
    v = body.get("sequence")
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return 0
    return int(v)


def _bound_card_id(content: Any) -> str:
    """content = {"type":"card","data":{"card_id":...}}（send_by_id/reply wire）."""
    if isinstance(content, dict) and content.get("type") == "card":
        data = content.get("data")
        if isinstance(data, dict):
            return str(data.get("card_id") or "")
    return ""


def _extract_elements(card: Any) -> dict[str, dict[str, Any]]:
    """从卡片 JSON 递归提取 element_id → element（body.elements 或扁平 elements）."""
    out: dict[str, dict[str, Any]] = {}

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            eid = node.get("element_id")
            if isinstance(eid, str) and eid:
                out[eid] = node
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for v in node:
                _walk(v)

    _walk(card)
    return out


def _apply_batch_action(state: MockCardState, action: Any) -> None:
    """batch_update action 应用到卡片全量 JSON（add/delete 两族，宽松语义）."""
    if not isinstance(action, dict):
        return
    name = action.get("action")
    params = action.get("params") or {}
    body = state.card_json.setdefault("body", {})
    elements = body.setdefault("elements", [])
    if name == "add_elements":
        els = [e for e in (params.get("elements") or []) if isinstance(e, dict)]
        target = params.get("target_element_id")
        ptype = params.get("type") or "append"
        idx = None
        if target:
            idx = next(
                (i for i, e in enumerate(elements)
                 if isinstance(e, dict) and e.get("element_id") == target),
                None,
            )
        if ptype == "insert_before" and idx is not None:
            elements[idx:idx] = els
        elif ptype == "insert_after" and idx is not None:
            elements[idx + 1:idx + 1] = els
        else:
            elements.extend(els)
    elif name == "delete_elements":
        ids = {str(i) for i in (params.get("element_ids") or [])}
        body["elements"] = [
            e for e in elements
            if not (isinstance(e, dict) and str(e.get("element_id")) in ids)
        ]


class MockFeishuREST:
    """飞书 Open API mock 服务（httpx.MockTransport handler；async callable）.

    用法：``httpx.AsyncClient(transport=httpx.MockTransport(server))``。
    """

    def __init__(self) -> None:
        self.cards: dict[str, MockCardState] = {}
        self.messages: list[MockMessage] = []
        self.call_log: list[MockCall] = []
        self._fail_next: dict[str, deque[tuple[int, str, int]]] = {}
        self._fail_always: dict[str, tuple[int, str, int]] = {}
        self._delays: dict[str, float] = {}
        self._card_n = 0
        self._msg_n = 0

    # ── 故障注入面（04-knowledge-freeze §5：429/300317/200850/挂起/拒收）────

    def fail_next(
        self, op: str, code: int, msg: str = "", times: int = 1,
        http_status: int = 200,
    ) -> None:
        """下次该 op 调用返回 ``{"code":code,"msg":msg}``（times 次）.

        99991400 限频约定 HTTP 400 + code 承载（飞书惯例，client.py 注释）；
        http_status 可覆盖（如 429/500）。
        """
        self._fail_next.setdefault(op, deque()).extend(
            [(code, msg, http_status)] * times
        )

    def fail_always(
        self, op: str, code: int, msg: str = "", http_status: int = 200,
    ) -> None:
        self._fail_always[op] = (code, msg, http_status)

    def clear_failures(self) -> None:
        self._fail_next.clear()
        self._fail_always.clear()

    def delay(self, op: str, seconds: float) -> None:
        """该 op 响应前挂起 seconds 秒（Q3：建卡窗口 0.5~1s 网络挂起模拟）."""
        self._delays[op] = seconds

    def strict_sequence(self, card_id: str, enabled: bool) -> None:
        """开启后 sequence ≤ 已见值 → 300317（Q1 序列错位；成功调用推进游标）."""
        self.cards[card_id].strict_sequence = enabled

    def expire_streaming(self, card_id: str) -> None:
        """一次性武装 200850（Q2）：下次 stream_element 撞线；settings
        ``streaming_mode:true``（官方恢复动作）后解除并恢复."""
        self.cards[card_id].expired = True

    def rate_limit_card(self, card_id: str, times: int = 1) -> None:
        """一次性武装 200937 卡级频控（audit #8：与 99991400 平台级分道；
        2026-11-20 L3 真火入册 "card updated too frequently"）.

        作用于该卡下次 stream_element 更新（卡级频控的主战场），times 次
        后自动解除——2026-11-20 实测语义：并发同元素竞速触发。

        纪律（audit #8 规则）：errors.py 错误码注册表新增条目时，必须
        同步本 mock 的注入面与 L2 台架用例——mock 滞后于错误码表 =
        新码在接近真实的台架层零覆盖。
        """
        self.fail_next(
            "stream_element", CARDKIT_CARD_RATE_LIMIT,
            msg="card updated too frequently", times=times,
        )

    # ── 观测面 ────────────────────────────────────────────────────────────

    def count(self, op: str) -> int:
        return sum(1 for c in self.call_log if c.op == op)

    def calls_of(self, op: str) -> list[MockCall]:
        return [c for c in self.call_log if c.op == op]

    # ── HTTP handler（httpx.MockTransport 异步约定）───────────────────────

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.startswith(_OPEN_APIS_PREFIX):
            path = path[len(_OPEN_APIS_PREFIX):]
        method = request.method
        try:
            body = json.loads(request.content.decode("utf-8")) if request.content else {}
        except ValueError:
            body = {"_raw": request.content.decode("utf-8", "replace")}
        op = self._classify(method, path)
        if op is None:
            self.call_log.append(MockCall("unrouted", method, path, body, -1))
            return httpx.Response(200, json={"code": -1, "msg": f"mock unrouted: {method} {path}"})
        delay = self._delays.get(op)
        if delay:
            await asyncio.sleep(delay)  # Q3：网络挂起窗口
        fail = self._pop_failure(op)
        if fail is not None:
            code, msg, status = fail
            self.call_log.append(MockCall(op, method, path, body, code))
            return httpx.Response(status, json={"code": code, "msg": msg})
        code, payload = self._dispatch(op, path, body)
        self.call_log.append(MockCall(op, method, path, body, code))
        return httpx.Response(200, json=payload)

    # ── 内部：路由与裁决 ──────────────────────────────────────────────────

    def _pop_failure(self, op: str) -> tuple[int, str, int] | None:
        queue = self._fail_next.get(op)
        if queue:
            item = queue.popleft()
            if not queue:
                self._fail_next.pop(op, None)
            return item
        return self._fail_always.get(op)

    @staticmethod
    def _classify(method: str, path: str) -> str | None:
        if method == "POST" and path == _TOKEN_PATH:
            return "token"
        if method == "POST" and path == _IM_MESSAGES:
            return "send"
        if (
            method == "POST"
            and path.startswith(_IM_MESSAGES + "/")
            and path.endswith(_REPLY_SUFFIX)
        ):
            return "reply"
        if method == "POST" and path == _CARDS_CREATE:
            return "create"
        if method == "PUT" and path.startswith(_CARDS_PREFIX):
            rest = path[len(_CARDS_PREFIX):]
            if rest.endswith(_BATCH_SUFFIX):
                return "batch_update"
            if rest.endswith(_SETTINGS_SUFFIX):
                return "settings"
            if rest.endswith(_CONTENT_SUFFIX) and "/elements/" in rest:
                return "stream_element"
            if "/" not in rest:
                return "update"
        return None

    def _advance_seq(self, state: MockCardState, sequence: Any) -> None:
        v = _seq_of({"sequence": sequence})
        if v > state.last_sequence:
            state.last_sequence = v

    @staticmethod
    def _err(code: int, msg: str) -> tuple[int, dict[str, Any]]:
        # 飞书 wire 形态：业务码 + msg（HTTP 200 承载，parse_api_response 裁决）
        return code, {"code": code, "msg": msg}

    @staticmethod
    def _ok(data: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
        return 0, {"code": 0, "msg": "ok", "data": data or {}}

    def _dispatch(
        self, op: str, path: str, body: dict[str, Any],
    ) -> tuple[int, dict[str, Any]]:
        if op == "token":
            # 此端点响应不包 data 层（token.py 直接 body.get("tenant_access_token")）
            return 0, {"code": 0, "tenant_access_token": "t-mock-1", "expire": 7200}

        if op == "create":
            card = _parse_json(body.get("data"))
            if not isinstance(card, dict):
                return self._err(300315, "card data is not a json object")
            self._card_n += 1
            card_id = f"vc_mock_{self._card_n}"
            config = card.get("config") or {}
            state = MockCardState(
                card_id=card_id,
                card_json=card,
                elements=_extract_elements(card),
                streaming_mode=bool(config.get("streaming_mode", False)),
            )
            self.cards[card_id] = state
            return self._ok({"card_id": card_id})

        if op == "send":
            self._msg_n += 1
            mid = f"om_mock_{self._msg_n}"
            content = _parse_json(body.get("content"))
            self.messages.append(MockMessage(
                message_id=mid,
                receive_id=str(body.get("receive_id") or ""),
                msg_type=str(body.get("msg_type") or ""),
                content=content,
                card_id=_bound_card_id(content),
            ))
            return self._ok({"message_id": mid})

        if op == "reply":
            anchor = path[len(_IM_MESSAGES) + 1:-len(_REPLY_SUFFIX)]
            self._msg_n += 1
            mid = f"om_mock_{self._msg_n}"
            content = _parse_json(body.get("content"))
            self.messages.append(MockMessage(
                message_id=mid,
                receive_id="",
                msg_type=str(body.get("msg_type") or ""),
                content=content,
                reply_to=anchor,
                card_id=_bound_card_id(content),
            ))
            return self._ok({"message_id": mid})

        if op == "update":
            card_id = path[len(_CARDS_PREFIX):]
            state = self.cards.get(card_id)
            if state is None:
                return self._err(
                    CARDKIT_ELEMENT_NOT_FOUND, f"card not found: {card_id}")
            card = _parse_json((body.get("card") or {}).get("data"))
            if not isinstance(card, dict):
                return self._err(300315, "card.data is not a json object")
            state.card_json = card
            state.elements = _extract_elements(card)
            config = card.get("config") or {}
            if "streaming_mode" in config:
                state.streaming_mode = bool(config["streaming_mode"])
            self._advance_seq(state, body.get("sequence"))
            return self._ok()

        if op == "stream_element":
            parts = path[len(_CARDS_PREFIX):].split("/")
            card_id, element_id = parts[0], parts[2]
            state = self.cards.get(card_id)
            if state is None:
                return self._err(
                    CARDKIT_ELEMENT_NOT_FOUND, f"card not found: {card_id}")
            if state.expired:
                return self._err(
                    CARDKIT_STREAMING_TIMEOUT, "streaming session expired")
            if not state.streaming_mode:
                return self._err(
                    CARDKIT_STREAMING_CLOSED, "streaming mode is closed")
            if state.strict_sequence and _seq_of(body) <= state.last_sequence:
                return self._err(
                    CARDKIT_SEQUENCE_CONFLICT,
                    f"sequence conflict: {_seq_of(body)} <= {state.last_sequence}")
            element = _extract_elements(state.card_json).get(element_id)
            if element is None:
                return self._err(
                    CARDKIT_ELEMENT_NOT_FOUND,
                    f"not find elementID: {element_id}")
            element["content"] = str(body.get("content") or "")
            state.elements = _extract_elements(state.card_json)
            self._advance_seq(state, body.get("sequence"))
            return self._ok()

        if op == "batch_update":
            card_id = path[len(_CARDS_PREFIX):-len(_BATCH_SUFFIX)]
            state = self.cards.get(card_id)
            if state is None:
                return self._err(
                    CARDKIT_ELEMENT_NOT_FOUND, f"card not found: {card_id}")
            actions = _parse_json(body.get("actions"))
            if not isinstance(actions, list):
                return self._err(300315, "actions is not a json array")
            for action in actions:
                _apply_batch_action(state, action)
            state.actions_log.append(actions)
            state.elements = _extract_elements(state.card_json)
            self._advance_seq(state, body.get("sequence"))
            return self._ok()

        if op == "settings":
            card_id = path[len(_CARDS_PREFIX):-len(_SETTINGS_SUFFIX)]
            state = self.cards.get(card_id)
            if state is None:
                return self._err(
                    CARDKIT_ELEMENT_NOT_FOUND, f"card not found: {card_id}")
            parsed = _parse_json(body.get("settings"))
            config: dict[str, Any] = {}
            if isinstance(parsed, dict):
                config = parsed.get("config") or {}
            if state.expired:
                if config.get("streaming_mode") is True:
                    state.expired = False  # 官方恢复动作：重开即解除（Q2）
                else:
                    return self._err(
                        CARDKIT_STREAMING_TIMEOUT, "streaming session expired")
            if "streaming_mode" in config:
                state.streaming_mode = bool(config["streaming_mode"])
            state.settings_log.append({
                "sequence": _seq_of(body),
                "streaming_mode": config.get("streaming_mode"),
                "summary": config.get("summary"),
            })
            self._advance_seq(state, body.get("sequence"))
            return self._ok()

        return self._err(-1, f"mock: unhandled op {op}")
