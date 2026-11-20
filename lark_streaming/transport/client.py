"""飞书 REST 传输客户端 — CardKit 2.0 + IM 消息（raw httpx 直连）.

设计决策（对比前代 lark-oapi SDK 通道）：
- raw REST 全权掌控请求/响应——怪癖行为（200860 确定性拒收、300313
  未持久化竞态、99991400 限频分道退避）不再隔 SDK 一层；
- 端点与 wire 形态与前代生产验证逐字段一致（data/actions/settings
  均为 JSON 字符串——lark-oapi builder 接收字符串即 wire 语义，
  生产实证）；
- 精确路径在 L3 真飞书验收（R5）最终校准；mock 测试不依赖真实路径。

本层职责边界：单次 API 调用的正确性与重试；sequence 递增、300317/200850
自愈循环、per-chat 串行化归引擎层（02-engine）。本层只透传 sequence。
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import httpx

from ..credentials import FeishuCredentials
from .budget import assert_card_size
from .errors import (
    FeishuAPIError,
    is_element_not_found_error,
    parse_api_response,
)
from .retry import retry_transient
from .seq import MAX_SEQ_RECOVERIES
from .token import TenantTokenManager

_logger = logging.getLogger("lark_streaming.transport.client")

#: token 失效错误码（旧 SDK 内部处理的场景，raw REST 显式接管）
_TOKEN_INVALID_CODES = {99991661, 99991663, 99991664}
_TOKEN_RETRY_DELAYS = (0.2, 0.5, 1.0)

#: 300313 元素未持久化竞态重试（前代 _ELEMENT_NOT_FOUND_RETRY_DELAYS 语义）
_ELEMENT_NOT_FOUND_RETRY_DELAYS: tuple[float, ...] = (0.2, 0.2, 0.2)

_LOGID_HEADER = "x-tt-logid"


class FeishuTransport:
    """飞书 Open API 传输客户端（CardKit 实体卡生命周期 + IM 投递）."""

    def __init__(
        self,
        credentials: FeishuCredentials,
        *,
        http: httpx.AsyncClient | None = None,
        timeout: float = 15.0,
    ) -> None:
        self._credentials = credentials
        self._own_http = http is None
        self._http = http or httpx.AsyncClient(timeout=timeout)
        self._token = TenantTokenManager(credentials, http=self._http)

    async def aclose(self) -> None:
        await self._token.aclose()
        if self._own_http:
            await self._http.aclose()

    # ── HTTP 基座 ──────────────────────────────────────────────────────

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        operation: str = "",
    ) -> Any:
        """单次 API 请求：鉴权 → 发送 → 业务码裁决；token 失效自刷新重试."""
        last_error: Exception | None = None
        for attempt in range(3):
            token = await self._token.get_token()
            headers = {"Authorization": f"Bearer {token}"}
            url = self._credentials.base_url + path
            try:
                resp = await self._http.request(
                    method, url, json=json_body, params=params, headers=headers,
                )
                log_id = resp.headers.get(_LOGID_HEADER, "")
                try:
                    payload = resp.json()
                except ValueError:
                    payload = {}
                if resp.status_code in (401, 403):
                    await self._token.invalidate()
                    last_error = FeishuAPIError(
                        f"{operation}: http {resp.status_code}", 0, log_id
                    )
                    if attempt < 2:
                        await asyncio.sleep(_TOKEN_RETRY_DELAYS[attempt])
                        continue
                    raise last_error
                # HTTP 4xx/5xx 但 body 有业务码 → 交给业务裁决（飞书惯例：
                # 99991400 等限频以 HTTP 400 + code 承载）
                return parse_api_response(payload, operation, log_id)
            except FeishuAPIError as e:
                if e.code in _TOKEN_INVALID_CODES:
                    await self._token.invalidate()
                    last_error = e
                    if attempt < 2:
                        await asyncio.sleep(_TOKEN_RETRY_DELAYS[attempt])
                        continue
                raise
            except asyncio.CancelledError:
                raise
            except (httpx.RequestError, httpx.TimeoutException) as e:
                last_error = e
                if attempt < 2:
                    await asyncio.sleep(_TOKEN_RETRY_DELAYS[attempt])
                    continue
                raise
        raise last_error  # unreachable, type-safe

    # ── IM 消息端点 ────────────────────────────────────────────────────

    async def send_card_to_chat(self, chat_id: str, card: dict[str, Any]) -> str:
        """直发独立卡片 JSON 到会话（非回复），返回 message_id."""
        return await self._send_message(chat_id, "interactive", json.dumps(card, ensure_ascii=False))

    async def send_card_by_id_to_chat(self, chat_id: str, card_id: str) -> str:
        """按 card_id 直发 CardKit 卡片实体到会话（非回复），返回 message_id.

        回复目标非合法 om_ id（如后台任务合成 id）时的正确投递通道
        （reply 对非 om_ id 必报 99992354）。
        """
        content = json.dumps({"type": "card", "data": {"card_id": card_id}}, ensure_ascii=False)
        return await self._send_message(chat_id, "interactive", content)

    async def send_text_to_chat(self, chat_id: str, text: str) -> str:
        """直发纯文本（原生兜底路径），返回 message_id."""
        return await self._send_message(chat_id, "text", json.dumps({"text": text}, ensure_ascii=False))

    async def reply_card_by_id(self, message_id: str, card_id: str) -> str:
        """以 reply 形态发送 CardKit 卡片实体（锚定引用），返回 message_id."""
        content = json.dumps({"type": "card", "data": {"card_id": card_id}}, ensure_ascii=False)
        return await retry_transient("reply_card_by_id", lambda: self._reply_message(message_id, "interactive", content))

    async def _send_message(self, chat_id: str, msg_type: str, content: str) -> str:
        async def _do() -> Any:
            data = await self._request(
                "POST", "/im/v1/messages",
                params={"receive_id_type": "chat_id"},
                json_body={"receive_id": chat_id, "msg_type": msg_type, "content": content},
                operation=f"send_message[{msg_type}]",
            )
            mid = (data or {}).get("message_id")
            if not mid:
                raise FeishuAPIError("send_message: response missing message_id")
            return str(mid)

        return await retry_transient(f"send_message[{msg_type}]", _do)

    async def _reply_message(self, message_id: str, msg_type: str, content: str) -> str:
        async def _do() -> Any:
            data = await self._request(
                "POST", f"/im/v1/messages/{message_id}/reply",
                json_body={"msg_type": msg_type, "content": content},
                operation=f"reply_message[{msg_type}]",
            )
            mid = (data or {}).get("message_id")
            if not mid:
                raise FeishuAPIError("reply_message: response missing message_id")
            return str(mid)

        return await retry_transient(f"reply_message[{msg_type}]", _do)

    # ── CardKit 端点 ───────────────────────────────────────────────────

    async def cardkit_create(self, card: dict[str, Any]) -> str:
        """创建 CardKit 实体卡，返回 card_id（wire：data 为 JSON 字符串）."""
        assert_card_size(card)  # 200860 确定性拒收 → 上传前本地拦截（OPT-1c）

        async def _do() -> Any:
            data = await self._request(
                "POST", "/cardkit/v1/cards",
                json_body={"type": "card_json", "data": json.dumps(card, ensure_ascii=False)},
                operation="cardkit_create",
            )
            card_id = (data or {}).get("card_id")
            if not card_id:
                raise FeishuAPIError("cardkit_create: response missing card_id")
            return str(card_id)

        return await retry_transient("cardkit_create", _do)

    async def cardkit_stream_element(
        self, card_id: str, element_id: str, content: str, *, sequence: int = 0,
    ) -> None:
        """流式更新卡片内指定 element 内容（打字机通道）.

        300313/300314（add 后服务端未持久化竞态）固定 0.2s ×3 重试
        （前代语义）；其余瞬态走通用退避。成功静默（流式期间调用频繁）。
        """
        last_error: FeishuAPIError | None = None
        for attempt in range(len(_ELEMENT_NOT_FOUND_RETRY_DELAYS) + 1):
            try:
                await retry_transient(
                    "cardkit_stream_element",
                    lambda: self._request(
                        "PUT",
                        f"/cardkit/v1/cards/{card_id}/elements/{element_id}/content",
                        json_body={"content": content, "sequence": sequence},
                        operation="cardkit_stream_element",
                    ),
                )
                return
            except asyncio.CancelledError:
                raise
            except FeishuAPIError as e:
                if not is_element_not_found_error(e):
                    raise
                last_error = e
                if attempt < len(_ELEMENT_NOT_FOUND_RETRY_DELAYS):
                    delay = _ELEMENT_NOT_FOUND_RETRY_DELAYS[attempt]
                    _logger.info(
                        "stream_element element-not-found retry card=%s el=%s attempt=%d delay=%.1fs",
                        card_id[:12], element_id[:16], attempt + 1, delay,
                    )
                    await asyncio.sleep(delay)
                    continue
                raise
        if last_error:
            raise last_error

    async def cardkit_update(
        self, card_id: str, card: dict[str, Any], *, sequence: int = 0,
    ) -> None:
        """全量更新 CardKit 卡片（wire：card.data 为 JSON 字符串）."""
        assert_card_size(card)

        async def _do() -> Any:
            return await self._request(
                "PUT", f"/cardkit/v1/cards/{card_id}",
                json_body={
                    "card": {"type": "card_json", "data": json.dumps(card, ensure_ascii=False)},
                    "sequence": sequence,
                },
                operation="cardkit_update",
            )

        await retry_transient("cardkit_update", _do)

    async def cardkit_batch_update(
        self, card_id: str, actions: list[dict[str, Any]], *, sequence: int = 0,
    ) -> None:
        """局部更新（增删改组件；wire：actions 为 JSON 字符串）.

        红线（前代语义）：纯 add_elements 非幂等——网络错误时首次请求可能
        已落卡，自动重放会插重复元素；此类调用绕过自动重试直发。
        """
        has_blind_add = any(
            isinstance(a, dict) and a.get("action") == "add_elements" for a in actions
        ) and not any(
            isinstance(a, dict) and a.get("action") == "delete_elements" for a in actions
        )

        async def _do() -> Any:
            return await self._request(
                "PUT", f"/cardkit/v1/cards/{card_id}/batch_update",
                json_body={"sequence": sequence, "actions": json.dumps(actions, ensure_ascii=False)},
                operation="cardkit_batch_update",
            )

        if has_blind_add:
            await _do()
        else:
            await retry_transient("cardkit_batch_update", _do)

    async def _cardkit_settings(
        self, card_id: str, settings: dict[str, Any], *, sequence: int, op: str,
    ) -> None:
        """settings 通道（wire：settings 为 JSON 字符串）——close/reopen/summary 共用."""
        async def _do() -> Any:
            return await self._request(
                "PUT", f"/cardkit/v1/cards/{card_id}/settings",
                json_body={"settings": json.dumps(settings, ensure_ascii=False), "sequence": sequence},
                operation=op,
            )

        await retry_transient(op, _do)

    async def cardkit_close_streaming(
        self, card_id: str, *, sequence: int = 0, summary: str = "",
    ) -> None:
        """关闭流式模式（可选摘要；两步定稿第一步）."""
        settings: dict[str, Any] = {"config": {"streaming_mode": False}}
        if summary:
            truncated = summary[:120]
            settings["config"]["summary"] = {
                "content": truncated,
                "i18n_content": {"zh_cn": truncated, "en_us": truncated},
            }
        await self._cardkit_settings(card_id, settings, sequence=sequence, op="cardkit_close_streaming")

    async def cardkit_reopen_streaming(self, card_id: str, *, sequence: int = 0) -> None:
        """重开流式会话（200850 官方恢复动作：streaming_mode:true；跳号允许）.

        双用途：事后自愈（seq.py 200850 分支）+ 主动续期（会话龄逼近
        ~10min 寿命前调用，02-engine §2 T_renew）。
        """
        await self._cardkit_settings(
            card_id, {"config": {"streaming_mode": True}},
            sequence=sequence, op="cardkit_reopen_streaming",
        )

    async def cardkit_update_summary(
        self, card_id: str, summary: str, *, sequence: int = 0,
    ) -> None:
        """更新会话摘要而不关闭流式（会话列表预览文案）."""
        if not summary:
            return
        truncated = summary[:120]
        settings = {"config": {"summary": {
            "content": truncated,
            "i18n_content": {"zh_cn": truncated, "en_us": truncated},
        }}}
        await self._cardkit_settings(card_id, settings, sequence=sequence, op="cardkit_update_summary")

    # 供引擎层自愈循环引用的常量与工具（单点 re-export）
    SEQ_RECOVERY_LIMIT = MAX_SEQ_RECOVERIES


__all__ = ["FeishuTransport", "_TOKEN_INVALID_CODES", "_ELEMENT_NOT_FOUND_RETRY_DELAYS"]
