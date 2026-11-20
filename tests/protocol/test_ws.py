"""WS 绑定：hello 握手协商 + 帧回执 + 违规关闭（01-protocol §6/§7）."""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

import aiohttp
import pytest
from aiohttp import WSMsgType
from aiohttp.test_utils import TestServer

from lark_streaming.protocol.server import build_app
from tests.protocol.conftest import CHAT, env, make_engine, make_settings

HELLO = {
    "v": 1,
    "event": "hello",
    "frame_id": "f_0",
    "chat_id": "",
    "payload": {"agent": "openclaw", "protocol_v": 1, "capabilities": {"snapshot_only": False}},
}


@pytest.fixture
async def ws_env(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    server = TestServer(build_app(engine))
    await server.start_server()
    session = aiohttp.ClientSession()
    try:
        yield SimpleNamespace(
            engine=engine, transport=transport, server=server,
            session=session, events=events,
        )
    finally:
        await session.close()
        await server.close()
        await engine.aclose()


async def _connect(env_ns) -> aiohttp.ClientWebSocketResponse:
    return await env_ns.session.ws_connect(env_ns.server.make_url("/v1/ws"))


async def _recv_json(ws: aiohttp.ClientWebSocketResponse, timeout: float = 5) -> dict:
    msg = await ws.receive(timeout=timeout)
    assert msg.type is WSMsgType.TEXT, f"expect text frame, got {msg.type}: {msg.data}"
    return json.loads(msg.data)


async def _recv_closed(ws: aiohttp.ClientWebSocketResponse, timeout: float = 5) -> None:
    msg = await ws.receive(timeout=timeout)
    assert msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED), (
        f"expect close, got {msg.type}: {msg.data}"
    )


async def test_hello_handshake_then_event_flow(ws_env):
    ns = ws_env
    ws = await _connect(ns)
    try:
        await ws.send_str(json.dumps(HELLO))
        ack = await _recv_json(ws)
        assert ack["event"] == "hello.ack"
        assert ack["frame_id"] == "f_0"
        assert ack["payload"]["protocol_v"] == 1
        assert ack["payload"]["engine_version"] == "0.0.1"
        assert ack["payload"]["settings"]["watchdog_idle_sec"] == 300.0

        await ws.send_str(json.dumps(env("turn.start", turn="t_w", frame="f1")))
        ack = await _recv_json(ws)
        assert ack["event"] == "frame.ack"
        assert ack["payload"]["verdict"] == "consumed"
        assert ack["payload"]["message_id"].startswith("om_")

        await ws.send_str(json.dumps(env("answer.delta", turn="t_w", frame="f2", payload={"delta": "你好"})))
        ack = await _recv_json(ws)
        assert ack["payload"]["verdict"] == "consumed"
        st = ns.engine._chats[CHAT].turn
        assert st is not None and st.render.answer_text == "你好"
    finally:
        await ws.close()


async def test_first_frame_not_hello_is_rejected_and_closed(ws_env):
    ns = ws_env
    ws = await _connect(ns)
    await ws.send_str(json.dumps(env("deliver", turn="", frame="f_x", payload={"text": "x"})))
    err = await _recv_json(ws)
    assert err["event"] == "error"
    assert err["payload"]["detail"] == "handshake_required"
    await _recv_closed(ws)  # §6：首帧必须 hello——违规关闭连接


async def test_hello_unsupported_version_closes(ws_env):
    ns = ws_env
    ws = await _connect(ns)
    bad = json.loads(json.dumps(HELLO))
    bad["payload"]["protocol_v"] = 2
    await ws.send_str(json.dumps(bad))
    err = await _recv_json(ws)
    assert err["event"] == "error"
    assert "unsupported_protocol_v:2" in err["payload"]["detail"]
    await _recv_closed(ws)  # 协商失败 → 关闭连接（§6）


async def test_hello_invalid_payload_closes(ws_env):
    ns = ws_env
    ws = await _connect(ns)
    bad = json.loads(json.dumps(HELLO))
    bad["payload"] = "not-an-object"
    await ws.send_str(json.dumps(bad))
    err = await _recv_json(ws)
    assert err["event"] == "error"
    assert err["payload"]["detail"] == "handshake_failed:invalid_payload"
    await _recv_closed(ws)


async def test_json_garbage_keeps_connection_alive(ws_env):
    """JSON 解析失败 → 错误帧（不产生 ack）但不断连（§7 只对协商失败关连）."""
    ns = ws_env
    ws = await _connect(ns)
    await ws.send_str("wtf-not-json")
    err = await _recv_json(ws)
    assert err["event"] == "error"
    assert err["payload"]["detail"] == "invalid_json"
    # 连接仍活着：正常握手继续
    await ws.send_str(json.dumps(HELLO))
    ack = await _recv_json(ws)
    assert ack["event"] == "hello.ack"
    await ws.close()


async def test_duplicate_hello_after_handshake_ignored_with_error(ws_env):
    ns = ws_env
    ws = await _connect(ns)
    await ws.send_str(json.dumps(HELLO))
    assert (await _recv_json(ws))["event"] == "hello.ack"
    await ws.send_str(json.dumps(HELLO))
    err = await _recv_json(ws)
    assert err["event"] == "error"
    assert err["payload"]["detail"] == "already_handshaked"
    # 连接保持：后续事件帧照常回执
    await ws.send_str(json.dumps(env("deliver", turn="", frame="f_d", payload={"text": "x"})))
    ack = await _recv_json(ws)
    assert ack["event"] == "frame.ack"
    assert ack["payload"]["verdict"] == "consumed"
    await ws.close()


async def test_connection_close_with_inflight_frame_still_consumed(ws_env):
    """连接断开不等回执：在途帧已被引擎消费（卡片管线完成性优先）."""
    ns = ws_env
    ws = await _connect(ns)
    await ws.send_str(json.dumps(HELLO))
    assert (await _recv_json(ws))["event"] == "hello.ack"
    ns.transport.set_delay("cardkit_create", 0.15)
    await ws.send_str(json.dumps(env("turn.start", turn="t_z", frame="f_z")))
    await ws.close()  # 不等 ack 即断
    for _ in range(20):
        await asyncio.sleep(0.05)
        if ns.transport.calls_of("cardkit_create"):
            break
    assert len(ns.transport.calls_of("cardkit_create")) == 1  # 帧被完整消费
