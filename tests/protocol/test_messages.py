"""共享 wire 素材：帧解析 / 错误对象 / hello 协商（01-protocol §3/§6/§7）."""

from __future__ import annotations

import pytest

from lark_streaming.protocol import (
    ENGINE_VERSION,
    WireFrameError,
    hello_ack,
    negotiate_hello,
    parse_frame,
    wire_error,
)
from lark_streaming.protocol import settings_payload


def test_parse_frame_valid():
    raw = parse_frame('{"v":1,"event":"deliver","frame_id":"f1","chat_id":"oc","payload":{}}')
    assert raw["event"] == "deliver"


def test_parse_frame_invalid_json():
    with pytest.raises(WireFrameError) as ei:
        parse_frame("{not json")
    assert ei.value.detail == "invalid_json"
    assert ei.value.frame_id == ""


def test_parse_frame_non_object_root():
    for text in ("[1,2]", '"str"', "42", "null"):
        with pytest.raises(WireFrameError) as ei:
            parse_frame(text)
        assert ei.value.detail == "invalid_envelope:not_an_object"


def test_wire_error_shape():
    obj = wire_error("invalid_json", frame_id="f_9")
    assert obj == {
        "v": 1,
        "event": "error",
        "frame_id": "f_9",
        "payload": {"detail": "invalid_json"},
    }


def test_settings_payload_shape():
    s = settings_payload()
    assert s["watchdog_idle_sec"] == 300.0
    assert s["session_renew_age_sec"] == 540.0
    assert s["clarify_window_sec"] == 1.2
    assert s["card_ttl_sec"] == 600.0
    assert s["degrade_cooldown_rate_limit_sec"] == 60.0


def test_hello_ack_shape():
    ack = hello_ack("f_0")
    assert ack["event"] == "hello.ack"
    assert ack["frame_id"] == "f_0"
    assert ack["payload"]["protocol_v"] == 1
    assert ack["payload"]["engine_version"] == ENGINE_VERSION
    assert "watchdog_idle_sec" in ack["payload"]["settings"]


def test_negotiate_hello_ok():
    frame = {
        "v": 1,
        "event": "hello",
        "frame_id": "f_0",
        "chat_id": "",
        "payload": {"agent": "openclaw", "protocol_v": 1, "capabilities": {}},
    }
    reply, keep = negotiate_hello(frame)
    assert keep is True
    assert reply["event"] == "hello.ack"
    assert reply["frame_id"] == "f_0"


@pytest.mark.parametrize(
    ("payload", "detail_frag"),
    [
        ("not-an-object", "invalid_payload"),
        ({"agent": "x"}, "unsupported_protocol_v:None"),
        ({"protocol_v": 2}, "unsupported_protocol_v:2"),
        ({"protocol_v": True}, "unsupported_protocol_v:True"),
    ],
)
def test_negotiate_hello_failures(payload, detail_frag: str):
    frame = {"v": 1, "event": "hello", "frame_id": "f_0", "chat_id": "", "payload": payload}
    reply, keep = negotiate_hello(frame)
    assert keep is False
    assert reply["event"] == "error"
    assert detail_frag in reply["payload"]["detail"]
    assert reply["frame_id"] == "f_0"
