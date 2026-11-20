"""信封消费面校验与回执 wire 形态（01-protocol §2/§4.6/§7）."""

from __future__ import annotations

import pytest

from lark_streaming.engine.events import (
    Ack,
    EnvelopeError,
    Event,
    LifecycleEvent,
    Verdict,
)
from tests.engine.conftest import CHAT, TURN, env


def _ok(raw: dict) -> Event:
    return Event.from_envelope(raw)


def test_valid_envelope_full_fields():
    ev = _ok(env("answer.delta", payload={"delta": "x"}, frame="f_1", seq=7))
    assert ev.event == "answer.delta"
    assert ev.frame_id == "f_1"
    assert ev.chat_id == CHAT
    assert ev.turn_id == TURN
    assert ev.seq == 7
    assert ev.v == 1
    assert dict(ev.payload) == {"delta": "x"}


def test_deliver_without_turn_id_is_valid():
    raw = env("deliver", turn="", payload={"text": "cron 结果"})
    assert _ok(raw).turn_id == ""


@pytest.mark.parametrize("missing", ["v", "event", "frame_id", "chat_id", "payload"])
def test_missing_required_field_rejected(missing: str):
    raw = env("turn.start", payload={})
    if missing == "turn_id":
        pass  # turn.start 缺 turn_id 单独测
        raw.pop("turn_id")
    else:
        raw.pop(missing)
    with pytest.raises(EnvelopeError) as ei:
        _ok(raw)
    assert f"missing_field:{missing}" in ei.value.detail


def test_turn_scoped_event_requires_turn_id():
    raw = env("answer.delta", turn="", payload={"delta": "x"})
    with pytest.raises(EnvelopeError) as ei:
        _ok(raw)
    assert ei.value.detail == "invalid_envelope:missing_field:turn_id"


def test_unsupported_version_rejected():
    raw = env("turn.start")
    raw["v"] = 2
    with pytest.raises(EnvelopeError) as ei:
        _ok(raw)
    assert "unsupported_version:2" in ei.value.detail


def test_wrong_types_rejected():
    bad_cases = [
        ("v", "1"),          # v 字符串
        ("ts", 1.5),         # ts 浮点
        ("seq", -1),         # seq 负数
        ("seq", True),       # bool 冒充 int
        ("payload", "x"),    # payload 非对象
        ("turn_id", 3),      # turn_id 非字符串
    ]
    for key, value in bad_cases:
        raw = env("turn.start", payload={}, seq=1 if key != "seq" else None)
        if key == "seq":
            raw.pop("seq", None)
        raw[key] = value
        with pytest.raises(EnvelopeError):
            _ok(raw)


def test_non_dict_envelope_rejected():
    with pytest.raises(EnvelopeError) as ei:
        Event.from_envelope("not-a-dict")
    assert ei.value.detail == "invalid_envelope:not_an_object"


def test_payload_is_readonly_view():
    ev = _ok(env("answer.delta", payload={"delta": "x"}))
    with pytest.raises(TypeError):
        ev.payload["delta"] = "mutated"  # type: ignore[index]


def test_ack_to_protocol_wire_shape():
    ack = Ack("f_9", Verdict.CONSUMED, message_id="om_1", detail="")
    wire = ack.to_protocol()
    assert wire["event"] == "frame.ack"
    assert wire["v"] == 1
    assert wire["payload"] == {"frame_id": "f_9", "verdict": "consumed", "message_id": "om_1"}
    fallback = Ack("f_10", Verdict.FALLBACK, detail="no_active_card").to_protocol()
    assert fallback["payload"]["verdict"] == "fallback"
    assert fallback["payload"]["detail"] == "no_active_card"
    assert "message_id" not in fallback["payload"]


def test_lifecycle_to_protocol_wire_shape():
    wire = LifecycleEvent("chat_degraded", chat_id="oc_a", code=99991400).to_protocol()
    assert wire["event"] == "lifecycle.event"
    assert wire["payload"]["type"] == "chat_degraded"
    assert wire["payload"]["chat_id"] == "oc_a"
    assert wire["payload"]["code"] == 99991400
