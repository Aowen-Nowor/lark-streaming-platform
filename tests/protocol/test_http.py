"""HTTP 绑定：三端点语义 + 错误分道（01-protocol §3/§7）.

业务失败恒 200 + ack.verdict；协议级失败 400 + 错误对象（与 WS/stdio
同形态）；批量逐帧回执、顺序与请求元素一致。
"""

from __future__ import annotations

import json

import httpx
import pytest
from aiohttp.test_utils import TestServer

from lark_streaming.protocol.server import build_app
from tests.protocol.conftest import CHAT, env, make_engine, make_settings


@pytest.fixture
async def http(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    server = TestServer(build_app(engine))
    await server.start_server()
    client = httpx.AsyncClient(base_url=str(server.make_url("")), timeout=10)
    try:
        yield engine, transport, client, events
    finally:
        await client.aclose()
        await server.close()
        await engine.aclose()


async def post_json(client: httpx.AsyncClient, path: str, payload) -> httpx.Response:
    if isinstance(payload, (dict, list)):
        return await client.post(path, json=payload)
    return await client.post(path, content=payload, headers={"Content-Type": "application/json"})


async def test_events_single_event_sync_ack(http):
    engine, transport, client, events = http
    r = await post_json(client, "/v1/events", env("turn.start", turn="t_h", frame="f1"))
    assert r.status_code == 200
    body = r.json()
    assert body["event"] == "frame.ack"
    assert body["payload"]["verdict"] == "consumed"
    assert body["payload"]["message_id"].startswith("om_")
    assert len(transport.calls_of("cardkit_create")) == 1


async def test_events_full_turn_flow(http):
    engine, transport, client, events = http
    for frame in (
        env("turn.start", turn="t_f", frame="f1"),
        env("answer.delta", turn="t_f", frame="f2", payload={"delta": "你好"}),
        env("answer.final", turn="t_f", frame="f3", payload={"text": "终值"}),
    ):
        r = await post_json(client, "/v1/events", frame)
        assert r.status_code == 200
        assert r.json()["payload"]["verdict"] == "consumed"
    assert len(transport.calls_of("cardkit_stream_element")) == 2  # 流式 + 终值
    assert len(transport.calls_of("cardkit_close_streaming")) == 1


async def test_events_invalid_json_400_error_object(http):
    engine, transport, client, events = http
    r = await post_json(client, "/v1/events", b"{oops")
    assert r.status_code == 400
    body = r.json()
    assert body["event"] == "error"
    assert body["payload"]["detail"] == "invalid_json"


async def test_events_non_object_body_400(http):
    engine, transport, client, events = http
    r = await post_json(client, "/v1/events", [1, 2])
    assert r.status_code == 400
    assert r.json()["payload"]["detail"] == "invalid_envelope:not_an_object"


async def test_events_invalid_envelope_200_fallback(http):
    """字段缺失 → 业务级失败：200 + fallback ack（01-protocol §7 第二行）."""
    engine, transport, client, events = http
    bad = {"v": 1, "event": "answer.delta", "frame_id": "f_bad", "chat_id": "oc_x", "payload": {}}
    r = await post_json(client, "/v1/events", bad)
    assert r.status_code == 200
    payload = r.json()["payload"]
    assert payload["verdict"] == "fallback"
    assert payload["detail"].startswith("invalid_envelope")


async def test_events_unknown_event_200_fallback(http):
    engine, transport, client, events = http
    r = await post_json(client, "/v1/events", env("widget.spin", turn="", frame="f_u"))
    assert r.status_code == 200
    payload = r.json()["payload"]
    assert payload["verdict"] == "fallback"
    assert payload["detail"] == "unknown_event:widget.spin"


async def test_events_duplicate_frame_id_idempotent(http):
    engine, transport, client, events = http
    await post_json(client, "/v1/events", env("turn.start", turn="t_i", frame="f_ts"))
    raw = env("answer.delta", turn="t_i", payload={"delta": "x"}, frame="f_dup", seq=1)
    r1 = await post_json(client, "/v1/events", raw)
    r2 = await post_json(client, "/v1/events", raw)
    assert r1.json()["payload"]["verdict"] == "consumed"
    assert r2.json()["payload"]["verdict"] == "consumed"
    assert len(transport.calls_of("cardkit_stream_element")) == 1  # 不重复消费


async def test_batch_mixed_elements_ordered(http):
    engine, transport, client, events = http
    frames = [
        env("deliver", turn="", frame="f_b1", payload={"text": "第一条"}),
        "not-a-dict",
        env("deliver", turn="", frame="f_b2", payload={"text": "第二条"}),
    ]
    r = await post_json(client, "/v1/events:batch", frames)
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, list) and len(body) == 3
    assert body[0]["event"] == "frame.ack"
    assert body[0]["payload"]["frame_id"] == "f_b1"
    assert body[1]["event"] == "error"  # 非 object 元素槽位回错误对象
    assert body[1]["payload"]["detail"] == "invalid_envelope:not_an_object"
    assert body[2]["event"] == "frame.ack"
    assert body[2]["payload"]["frame_id"] == "f_b2"
    assert len(transport.calls_of("send_card_to_chat")) == 2


async def test_batch_non_array_body_400(http):
    engine, transport, client, events = http
    r = await post_json(client, "/v1/events:batch", {"v": 1})
    assert r.status_code == 400
    assert r.json()["payload"]["detail"] == "invalid_envelope:not_an_array"


async def test_health_endpoint_shape(http):
    engine, transport, client, events = http
    await post_json(client, "/v1/events", env("turn.start", turn="t_hh", frame="f_h"))
    r = await client.get("/v1/health")
    assert r.status_code == 200
    body = r.json()
    assert body["chats_total"] == 1
    assert body["active_cards"] == 1
    assert CHAT in body["chats"]
    assert body["chats"][CHAT]["turn_phase"] == "streaming"


async def test_unknown_path_404_and_wrong_method_405(http):
    engine, transport, client, events = http
    assert (await client.get("/v1/nope")).status_code == 404
    assert (await client.get("/v1/events")).status_code == 405
