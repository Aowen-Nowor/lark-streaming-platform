"""feishu-callback 绑定 — 第四类入站面（01-protocol §3.2）.

飞书侧回调（卡片交互）→ LSEP 事件的翻译绑定，与 stdio/HTTP/WS 同级：
同一条引擎入口（``engine.submit``，同队列同背压同幂等），不开旁路。

鉴权域（独立于 ``LSP_GATEWAY_TOKEN``——飞书回调无法携带 Bearer）：
- ``verification_token`` 明文比对（url_verification.token / header.token）；
- ``encrypt_key`` 可选：配置后接受 ``{"encrypt": <base64>}`` 密文
  （AES-256-CBC，key = sha256(encrypt_key)，iv = 密文前 16 字节，PKCS7）；
- fail-closed：未配置 token → 端点恒 503（可见的禁用态，不裸奔）；
  密文到达而无 key / 解密失败 / token 错配 → 403，引擎零状态变更。

事件分道（§3.2 注记）：
- ``url_verification``  → echo challenge（token 比对通过才 echo）；
- ``card.action.trigger`` 且 value 含 ``lsp_clarify_action`` →
  ``engine.resolve_clarify`` 路由 → LSEP ``clarify.answer`` 提交；
  verdict 分道：fallback(queue_full) → 503（飞书回调自带退避重试 =
  该方向的传输级重试；PAUSED→RESUME 守卫天然幂等吸收重复投递），
  其余 fallback → 200（控制帧无原生履行义务，02-engine §6），
  consumed → 200；
- clarify_id 未命中（过期/已答/重启）→ 200 ignored
  （前代 clarify._expired_response 语义：不可挽回的事不触发飞书
  重试风暴）；
- 其他事件类型（im.message.receive_v1 等——事件订阅 URL 误指到
  平台时）→ 200 ignored + info 日志。

action 载荷提取（wire 细节标注 L3 R7 实测校准——宽松提取多槽位，
不猜测单一槽位）：``action.value``（behaviors value，必含
``lsp_clarify_action`` + ``clarify_id``）、``action.option``
（select_static 选中项序号）、``action.input_value``（input 文本）；
values 透传给引擎，选项序号 → 原值映射由引擎持有的 form 原值承担。
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import web

from lark_streaming.engine import StreamingEngine

__all__ = [
    "CLARIFY_CALLBACK_KEY",
    "decrypt_feishu_payload",
    "build_feishu_callback_handler",
    "FEISHU_CALLBACK_PATH",
]

_logger = logging.getLogger("lark_streaming.protocol.feishu_callback")

#: 端点路径（01-protocol §3.2）
FEISHU_CALLBACK_PATH = "/v1/feishu/callback"

#: 本协议命名空间的回调键（renderer/clarify.py 同源常量的再声明——
#: 协议层不得反向 import 渲染层，值契约由 01-protocol §3.2 冻结）
CLARIFY_CALLBACK_KEY = "lsp_clarify_action"

#: PKCS7 块大小（AES）
_AES_BLOCK = 16


class CallbackRejected(Exception):
    """验签失败/解密失败——端点回 403，引擎零状态变更."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def decrypt_feishu_payload(encrypt_b64: str, encrypt_key: str) -> dict[str, Any]:
    """飞书回调密文解密（AES-256-CBC；官方约定：key=sha256(encrypt_key)）.

    密文结构：iv(16B) + ciphertext；明文为 UTF-8 JSON。解密失败/去填充
    失败/明文非 JSON → CallbackRejected（fail-closed，不猜内容）。
    """
    # 延迟导入：非 gateway extra 安装（纯 stdio 形态）不强制 cryptography
    from cryptography.hazmat.primitives.ciphers import (
        Cipher, algorithms, modes,
    )

    if not encrypt_key:
        raise CallbackRejected("encrypt_payload_without_key")
    try:
        raw = base64.b64decode(encrypt_b64, validate=True)
    except Exception as exc:  # noqa: BLE001 — base64 非法即伪造
        raise CallbackRejected("encrypt_payload_bad_base64") from exc
    if len(raw) <= _AES_BLOCK or len(raw) % _AES_BLOCK != 0:
        raise CallbackRejected("encrypt_payload_bad_length")
    key = hashlib.sha256(encrypt_key.encode("utf-8")).digest()
    iv, data = raw[:_AES_BLOCK], raw[_AES_BLOCK:]
    dec = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
    padded = dec.update(data) + dec.finalize()
    pad = padded[-1]
    if not 1 <= pad <= _AES_BLOCK or padded[-pad:] != bytes([pad]) * pad:
        raise CallbackRejected("encrypt_payload_bad_padding")
    try:
        plain = json.loads(padded[:-pad].decode("utf-8"))
    except Exception as exc:  # noqa: BLE001 — 明文非 JSON 同样 fail-closed
        raise CallbackRejected("encrypt_payload_bad_json") from exc
    if not isinstance(plain, dict):
        raise CallbackRejected("encrypt_payload_not_an_object")
    return plain


def _header_token(body: dict[str, Any]) -> str:
    """提取回调 token（v2.0 在 header.token；v1/旧格式在顶层 token）."""
    header = body.get("header")
    if isinstance(header, dict) and isinstance(header.get("token"), str):
        return header["token"]
    tok = body.get("token")
    return tok if isinstance(tok, str) else ""


def _extract_action(body: dict[str, Any]) -> dict[str, Any] | None:
    """提取 card.action.trigger 的 action 槽位（宽松结构，异常即 None）."""
    event = body.get("event")
    if not isinstance(event, dict):
        return None
    action = event.get("action")
    return action if isinstance(action, dict) else None


def _clarify_values(action: dict[str, Any]) -> dict[str, Any] | None:
    """behaviors value → clarify 提交 values（无 lsp_clarify_action → None）.

    L3 R7 待校准项：select 选中项在 ``action.option``（序号字符串）、
    input 在 ``action.input_value``——槽位名以真飞书实测为准，此处宽松
    提取多槽位并透传，选项序号 → 原值映射由引擎持有的 form 承担。
    """
    value = action.get("value")
    if not isinstance(value, dict) or CLARIFY_CALLBACK_KEY not in value:
        return None
    values: dict[str, Any] = dict(value)
    values["action"] = str(value.get(CLARIFY_CALLBACK_KEY) or "")
    option = action.get("option")
    if isinstance(option, str):
        values["option"] = option
    input_value = action.get("input_value")
    if isinstance(input_value, str):
        values["input"] = input_value
    return values


def build_feishu_callback_handler(
    engine: StreamingEngine,
    *,
    verification_token: str,
    encrypt_key: str = "",
) -> Callable[[web.Request], Awaitable[web.Response]]:
    """构造 ``POST /v1/feishu/callback`` 的 aiohttp handler（同 app 挂载）.

    fail-closed：未配置 verification_token → 恒 503 可见禁用态。
    """
    async def feishu_callback(request: web.Request) -> web.Response:
        if not verification_token:
            # 未配置验签材料：端点存在但拒绝一切（503 = 部署配置问题，
            # 区别于 403 伪造请求；可见性优先——audit #1 同族 fail-closed）
            return web.json_response(
                {"error": "feishu_callback_disabled: verification_token not configured"},
                status=503,
            )
        try:
            raw = await request.json()
        except Exception:  # noqa: BLE001 — 非 JSON body
            return web.json_response({"error": "invalid_json"}, status=400)
        if not isinstance(raw, dict):
            return web.json_response({"error": "invalid_body:not_an_object"}, status=400)

        body = raw
        if isinstance(raw.get("encrypt"), str):
            try:
                body = decrypt_feishu_payload(raw["encrypt"], encrypt_key)
            except CallbackRejected as exc:
                _logger.warning("feishu-callback: reject (%s)", exc.reason)
                return web.json_response({"error": exc.reason}, status=403)

        # 1) URL 验证（飞书后台配置回调地址时的握手探针）
        if body.get("type") == "url_verification":
            if _header_token(body) != verification_token:
                return web.json_response({"error": "verification_token_mismatch"}, status=403)
            challenge = body.get("challenge")
            if not isinstance(challenge, str) or not challenge:
                return web.json_response({"error": "challenge_missing"}, status=400)
            return web.json_response({"challenge": challenge})

        # 2) 事件/卡片回调：token 比对（header.token，v1 顶层 token 兼容）
        if _header_token(body) != verification_token:
            return web.json_response({"error": "verification_token_mismatch"}, status=403)

        event_type = ""
        header = body.get("header")
        if isinstance(header, dict):
            event_type = str(header.get("event_type") or "")
        if not event_type:
            event_type = str(body.get("event_type") or "")

        if event_type != "card.action.trigger":
            # 事件订阅 URL 误指到平台等场景：无害忽略，不给飞书重试理由
            _logger.info("feishu-callback: ignored event_type=%s", event_type or "<none>")
            return web.json_response({"ok": True, "ignored": event_type or "no_event_type"})

        action = _extract_action(body)
        values = _clarify_values(action) if action is not None else None
        if values is None:
            # 外来按钮/非本协议命名空间的交互：无害忽略
            _logger.info("feishu-callback: card action without %s — ignored", CLARIFY_CALLBACK_KEY)
            return web.json_response({"ok": True, "ignored": "not_lsp_clarify_action"})

        clarify_id = str(values.get("clarify_id") or "")
        resolved = engine.resolve_clarify(clarify_id)
        if resolved is None:
            # 过期/已答/重启（前代 expired 语义）：200 不重试
            _logger.info("feishu-callback: clarify_id=%s not resolvable — ignored", clarify_id)
            return web.json_response({"ok": True, "ignored": "clarify_not_active"})

        chat_id, turn_id = resolved
        envelope = {
            "v": 1,
            "event": "clarify.answer",
            "frame_id": f"f_cb_{uuid.uuid4().hex[:12]}",
            "chat_id": chat_id,
            "turn_id": turn_id,
            "payload": {"values": values},
        }
        ack = await engine.submit(envelope)
        verdict = ack.verdict.value
        detail = ack.detail or ""
        if verdict == "fallback" and detail.startswith("queue_full"):
            # 背压即降级（audit #2 同族）：503 让飞书回调重试退避接管
            # 该方向的传输级重试；PAUSED→RESUME 守卫幂等吸收重复投递
            _logger.warning(
                "feishu-callback: queue_full chat=%s — 503 for feishu retry", chat_id,
            )
            return web.json_response({"error": "queue_full"}, status=503)
        if verdict == "fallback":
            # 控制帧无原生履行义务（02-engine §6）：降级红线已在引擎侧收口
            _logger.info(
                "feishu-callback: clarify.answer fallback chat=%s detail=%s", chat_id, detail,
            )
        else:
            _logger.info(
                "feishu-callback: clarify.answer consumed chat=%s turn=%s", chat_id, turn_id,
            )
        return web.json_response({"ok": True, "verdict": verdict})

    return feishu_callback
