"""传输客户端单测 — CardKit 端点 wire 形态 / 字节预算预检 / 300313 竞态 / IM 端点."""

from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from lark_streaming.transport.budget import CARD_BYTE_BUDGET, measure_card_bytes
from lark_streaming.transport.client import FeishuTransport
from lark_streaming.transport.errors import (
    CARDKIT_CARD_SIZE,
    CARDKIT_ELEMENT_NOT_FOUND,
    CARDKIT_SEQUENCE_CONFLICT,
    FeishuAPIError,
)

from .conftest import TOKEN_RESPONSE, Route, api_err, api_ok, body_of, json_response

CARD = {"schema": "2.0", "body": {"elements": [{"tag": "markdown", "content": "hi"}]}}


def make_transport(*routes: Route) -> FeishuTransport:
    return FeishuTransport(make_credentials(), http=make_client(*routes))


def make_credentials():
    from lark_streaming.credentials import FeishuCredentials

    return FeishuCredentials(app_id="cli_test", app_secret="secret_test")


def make_client(*routes):
    from .conftest import make_client as _mk

    return _mk(*routes)


class TestCardkitCreate:
    async def test_wire_format_and_return(self):
        route = Route(
            lambda m, p: m == "POST" and p.endswith("/cardkit/v1/cards"),
            lambda r: json_response(api_ok({"card_id": "c_abc"})),
        )
        tr = make_transport(route)
        try:
            card_id = await tr.cardkit_create(CARD)
            assert card_id == "c_abc"
            body = body_of(route.calls[0])
            # wire 形态与前代生产验证一致：data 为 JSON 字符串
            assert body["type"] == "card_json"
            assert isinstance(body["data"], str)
            assert json.loads(body["data"]) == CARD
        finally:
            await tr.aclose()

    async def test_local_budget_precheck_blocks_upload(self):
        """字节预算超限 → 本地拦截，零网络往返（OPT-1c 语义）."""
        route = Route(
            lambda m, p: m == "POST" and p.endswith("/cardkit/v1/cards"),
            lambda r: json_response(api_err(200860, "card size exceeded")),
        )
        tr = make_transport(route)
        try:
            big = {"schema": "2.0", "body": {"elements": [{"tag": "markdown", "content": "x" * (CARD_BYTE_BUDGET + 10)}]}}
            assert measure_card_bytes(big) > CARD_BYTE_BUDGET
            with pytest.raises(Exception) as exc_info:
                await tr.cardkit_create(big)
            assert "budget" in str(exc_info.value)
            assert len(route.calls) == 0  # 零往返
        finally:
            await tr.aclose()

    async def test_300317_passes_through_transport(self):
        """300317 不在传输层重试（归引擎 seq 自愈循环）——透传上抛."""
        route = Route(
            lambda m, p: m == "POST" and p.endswith("/cardkit/v1/cards"),
            lambda r: json_response(api_err(300317, "sequence compare failed")),
        )
        tr = make_transport(route)
        try:
            with pytest.raises(FeishuAPIError) as exc_info:
                await tr.cardkit_create(CARD)
            assert exc_info.value.code == CARDKIT_SEQUENCE_CONFLICT
            assert len(route.calls) == 1
        finally:
            await tr.aclose()

    async def test_200860_not_retried(self):
        route = Route(
            lambda m, p: m == "POST" and p.endswith("/cardkit/v1/cards"),
            lambda r: json_response(api_err(200860, "card size exceeded")),
        )
        tr = make_transport(route)
        try:
            with pytest.raises(FeishuAPIError) as exc_info:
                # 预算内但服务端仍判超限的边缘场景 → 确定性拒收零重试
                await tr.cardkit_create({"schema": "2.0", "body": {"elements": [{"tag": "markdown", "content": "y" * 26000}]}})
            assert exc_info.value.code == CARDKIT_CARD_SIZE
            assert len(route.calls) == 1
        finally:
            await tr.aclose()


class TestStreamElement:
    async def test_wire_and_sequence_passthrough(self):
        route = Route(
            lambda m, p: m == "PUT" and "/elements/el_answer/content" in p,
            lambda r: json_response(api_ok()),
        )
        tr = make_transport(route)
        try:
            await tr.cardkit_stream_element("c_1", "el_answer", "chunk **md**", sequence=42)
            body = body_of(route.calls[0])
            assert body == {"content": "chunk **md**", "sequence": 42}
            assert "/cards/c_1/elements/el_answer/content" in str(route.calls[0].url.path)
        finally:
            await tr.aclose()

    async def test_300313_retries_with_fixed_delay(self, monkeypatch):
        sleeps = []

        async def fake_sleep(d):
            sleeps.append(d)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        attempts = []

        def respond(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            if len(attempts) <= 2:
                return json_response(api_err(300313, "element not found"))
            return json_response(api_ok())

        route = Route(
            lambda m, p: m == "PUT" and "/elements/el_1/content" in p, respond,
        )
        tr = make_transport(route)
        try:
            await tr.cardkit_stream_element("c_1", "el_1", "txt", sequence=1)
            assert len(attempts) == 3
            assert sleeps == [0.2, 0.2]
        finally:
            await tr.aclose()

    async def test_300313_exhaustion_raises(self, monkeypatch):
        async def fake_sleep(d):
            pass

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        attempts = []

        def respond(request: httpx.Request) -> httpx.Response:
            attempts.append(1)
            return json_response(api_err(CARDKIT_ELEMENT_NOT_FOUND, "element not found"))

        route = Route(lambda m, p: m == "PUT" and "/elements/" in p, respond)
        tr = make_transport(route)
        try:
            with pytest.raises(FeishuAPIError):
                await tr.cardkit_stream_element("c_1", "el_1", "txt", sequence=1)
            assert len(attempts) == 4  # 1 原始 + 3 重试
        finally:
            await tr.aclose()


class TestSettings:
    async def test_close_streaming_wire(self):
        """#14：关流式改走全量更新（PUT /cards/{id}），不再调 settings 端点."""
        sealed = {
            "schema": "2.0",
            "config": {
                "streaming_mode": False,
                "summary": {"content": "定稿摘要", "i18n_content": {"zh_cn": "定稿摘要", "en_us": "定稿摘要"}},
            },
            "body": {"elements": []},
        }
        route = Route(
            lambda m, p: m == "PUT" and p.endswith("/cardkit/v1/cards/c_9"),
            lambda r: json_response(api_ok()),
        )
        tr = make_transport(route)
        try:
            await tr.cardkit_close_streaming("c_9", sealed, sequence=77)
            body = body_of(route.calls[0])
            assert body["sequence"] == 77
            card = json.loads(body["card"]["data"])
            assert card["config"]["streaming_mode"] is False
            assert card["config"]["summary"]["content"] == "定稿摘要"
        finally:
            await tr.aclose()

    async def test_reopen_streaming_wire(self):
        """#14：重开会话改走全量更新（PUT /cards/{id}），不再调 settings 端点."""
        streaming = {
            "schema": "2.0",
            "config": {"streaming_mode": True},
            "body": {"elements": []},
        }
        route = Route(
            lambda m, p: m == "PUT" and p.endswith("/cardkit/v1/cards/c_9"),
            lambda r: json_response(api_ok()),
        )
        tr = make_transport(route)
        try:
            await tr.cardkit_reopen_streaming("c_9", streaming, sequence=80)
            card = json.loads(body_of(route.calls[0])["card"]["data"])
            assert card["config"]["streaming_mode"] is True
        finally:
            await tr.aclose()


class TestBatchUpdate:
    async def test_blind_add_skips_retry(self, monkeypatch):
        """纯 add_elements 非幂等 → 绕过自动重试（前代红线）."""
        retry_calls = []
        orig = None
        calls = []

        def respond(request: httpx.Request) -> httpx.Response:
            calls.append(1)
            return json_response(api_err(2200, "transient"))  # 若重试会多次

        route = Route(lambda m, p: m == "PUT" and p.endswith("/batch_update"), respond)
        tr = make_transport(route)
        try:
            with pytest.raises(FeishuAPIError):
                await tr.cardkit_batch_update("c_1", [{"action": "add_elements", "elements": []}], sequence=1)
            assert len(calls) == 1  # 非幂等操作不自动重试
        finally:
            await tr.aclose()

    async def test_mixed_actions_retry(self, monkeypatch):
        async def fake_sleep(d):
            pass

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        calls = []

        def respond(request: httpx.Request) -> httpx.Response:
            calls.append(1)
            if len(calls) == 1:
                return json_response(api_err(2200, "transient"))
            return json_response(api_ok())

        route = Route(lambda m, p: m == "PUT" and p.endswith("/batch_update"), respond)
        tr = make_transport(route)
        try:
            actions = [{"action": "add_elements", "elements": []}, {"action": "delete_elements", "element_ids": ["e1"]}]
            await tr.cardkit_batch_update("c_1", actions, sequence=1)
            assert len(calls) == 2  # 含 delete → 幂等可重试
        finally:
            await tr.aclose()


class TestImEndpoints:
    async def test_send_card_by_id_wire(self):
        route = Route(
            lambda m, p: m == "POST" and p.endswith("/im/v1/messages"),
            lambda r: json_response(api_ok({"message_id": "om_123"})),
        )
        tr = make_transport(route)
        try:
            mid = await tr.send_card_by_id_to_chat("oc_chat", "c_abc")
            assert mid == "om_123"
            assert "receive_id_type=chat_id" in str(route.calls[0].url)
            body = body_of(route.calls[0])
            assert body["msg_type"] == "interactive"
            content = json.loads(body["content"])
            assert content == {"type": "card", "data": {"card_id": "c_abc"}}
        finally:
            await tr.aclose()

    async def test_send_text_wire(self):
        route = Route(
            lambda m, p: m == "POST" and p.endswith("/im/v1/messages"),
            lambda r: json_response(api_ok({"message_id": "om_9"})),
        )
        tr = make_transport(route)
        try:
            mid = await tr.send_text_to_chat("oc_chat", "fallback 文本")
            assert mid == "om_9"
            body = body_of(route.calls[0])
            assert body["msg_type"] == "text"
            assert json.loads(body["content"]) == {"text": "fallback 文本"}
        finally:
            await tr.aclose()

    async def test_reply_card_wire(self):
        route = Route(
            lambda m, p: m == "POST" and "/reply" in p,
            lambda r: json_response(api_ok({"message_id": "om_reply"})),
        )
        tr = make_transport(route)
        try:
            mid = await tr.reply_card_by_id("om_anchor", "c_abc")
            assert mid == "om_reply"
            assert "/im/v1/messages/om_anchor/reply" in str(route.calls[0].url.path)
        finally:
            await tr.aclose()

    async def test_missing_message_id_raises(self):
        route = Route(
            lambda m, p: m == "POST" and p.endswith("/im/v1/messages"),
            lambda r: json_response(api_ok({})),
        )
        tr = make_transport(route)
        try:
            with pytest.raises(FeishuAPIError):
                await tr.send_text_to_chat("oc_chat", "x")
        finally:
            await tr.aclose()


class TestTokenRetry:
    async def test_token_invalid_invalidates_and_retries(self, monkeypatch):
        async def fake_sleep(d):
            pass

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        state = {"count": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path.endswith("/auth/v3/tenant_access_token/internal"):
                state["count"] += 1
                return json_response({**TOKEN_RESPONSE, "tenant_access_token": f"t-token{state['count']}"})
            auth = request.headers.get("authorization", "")
            if auth.endswith("t-token1"):
                return json_response(api_err(99991663, "invalid token"), status_code=200)
            return json_response(api_ok({"card_id": "c_ok"}))

        tr = FeishuTransport(make_credentials(), http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        try:
            card_id = await tr.cardkit_create(CARD)
            assert card_id == "c_ok"  # token 失效 → 自刷新 → 成功
        finally:
            await tr.aclose()


class TestRetrySingleLayer:
    """audit #10：retry_transient 是唯一通用重试层，_request 不再内层重试网络错误."""

    async def test_network_error_max_attempts_is_four(self, monkeypatch):
        """网络错误最坏尝试 = 1 原始 + 3 重试 = 4（修复前内层 3 × 外层 4 = 12）."""
        async def fake_sleep(d):
            pass

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        calls = []

        def respond(request: httpx.Request) -> httpx.Response:
            calls.append(1)
            raise httpx.ConnectError("net down", request=request)

        route = Route(lambda m, p: m == "POST" and p.endswith("/im/v1/messages"), respond)
        tr = make_transport(route)
        try:
            with pytest.raises(httpx.ConnectError):
                await tr.send_text_to_chat("oc_chat", "x")
            assert len(calls) == 4
        finally:
            await tr.aclose()

    async def test_http_401_exhaustion_not_amplified(self, monkeypatch):
        """401 token 自愈耗尽（内层 3 次）后上抛，不被外层 retry_transient 放大."""
        async def fake_sleep(d):
            pass

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        msg_calls = []

        def handler(request: httpx.Request) -> httpx.Response:
            path = request.url.path
            if path.endswith("/auth/v3/tenant_access_token/internal"):
                return json_response(TOKEN_RESPONSE)
            msg_calls.append(1)
            return json_response(api_err(0, "unauthorized"), status_code=401)

        tr = FeishuTransport(make_credentials(), http=httpx.AsyncClient(transport=httpx.MockTransport(handler)))
        try:
            with pytest.raises(FeishuAPIError):
                await tr.send_text_to_chat("oc_chat", "x")
            # 消息端点恰好 3 次（token 自愈层），非 3×4
            assert len(msg_calls) == 3
        finally:
            await tr.aclose()

    async def test_reply_no_outer_retry_wrapper(self, monkeypatch):
        """reply_card_by_id 不再包外层 retry_transient（修复前双层叠加）."""
        async def fake_sleep(d):
            pass

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        calls = []

        def respond(request: httpx.Request) -> httpx.Response:
            calls.append(1)
            raise httpx.ConnectError("net down", request=request)

        route = Route(lambda m, p: m == "POST" and "/reply" in p, respond)
        tr = make_transport(route)
        try:
            with pytest.raises(httpx.ConnectError):
                await tr.reply_card_by_id("om_anchor", "c_abc")
            assert len(calls) == 4  # 单层：1 + 3
        finally:
            await tr.aclose()


class TestIdempotencyKey:
    """audit #3：IM 投递公开方法生成一次 uuid4，贯穿全部重试（飞书侧去重键）."""

    async def test_send_uuid_query_stable_across_retries(self, monkeypatch):
        async def fake_sleep(d):
            pass

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        uuids: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            uuids.append(request.url.params.get("uuid") or "")
            if len(uuids) == 1:
                raise httpx.ConnectError("net down", request=request)
            return json_response(api_ok({"message_id": "om_1"}))

        route = Route(lambda m, p: m == "POST" and p.endswith("/im/v1/messages"), respond)
        tr = make_transport(route)
        try:
            mid = await tr.send_text_to_chat("oc_chat", "hi")
            assert mid == "om_1"
            assert len(uuids) == 2
            assert uuids[0] and uuids[0] == uuids[1]  # 同一逻辑投递共享 uuid
        finally:
            await tr.aclose()

    async def test_reply_uuid_body_stable_across_retries(self, monkeypatch):
        async def fake_sleep(d):
            pass

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        uuids: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            uuids.append(str(body_of(request).get("uuid", "")))
            if len(uuids) == 1:
                raise httpx.ConnectError("net down", request=request)
            return json_response(api_ok({"message_id": "om_r"}))

        route = Route(lambda m, p: m == "POST" and "/reply" in p, respond)
        tr = make_transport(route)
        try:
            mid = await tr.reply_card_by_id("om_anchor", "c_abc")
            assert mid == "om_r"
            assert len(uuids) == 2
            assert uuids[0] and uuids[0] == uuids[1]
        finally:
            await tr.aclose()

    async def test_distinct_calls_distinct_uuids(self):
        """不同逻辑投递 → 不同 uuid（不互相误伤去重）."""
        seen: list[str] = []

        def respond(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.params.get("uuid") or "")
            return json_response(api_ok({"message_id": f"om_{len(seen)}"}))

        route = Route(lambda m, p: m == "POST" and p.endswith("/im/v1/messages"), respond)
        tr = make_transport(route)
        try:
            await tr.send_text_to_chat("oc_chat", "a")
            await tr.send_text_to_chat("oc_chat", "b")
            assert len(seen) == 2
            assert seen[0] and seen[1] and seen[0] != seen[1]
        finally:
            await tr.aclose()
