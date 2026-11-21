"""stdio 绑定：行协议 / 错误对象 / 并发回执 / 版本参数校验（01-protocol §3）."""

from __future__ import annotations

import pytest

from lark_streaming.protocol import StdioBinding
from tests.protocol.conftest import (
    CHAT,
    env,
    lines,
    make_engine,
    make_emit,
    make_settings,
)


async def test_happy_turn_flow_emits_acks(transport):
    engine, events, clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    out: list[dict] = []
    binding = StdioBinding(engine)
    n = await binding.serve(
        lines=lines(
            env("turn.start", turn="t_s", frame="f1"),
            env("answer.delta", turn="t_s", frame="f2", payload={"delta": "你好"}),
            env("answer.final", turn="t_s", frame="f3", payload={"text": "终值"}),
        ),
        emit=make_emit(out),
    )
    try:
        assert n == 3
        assert len(out) == 3
        assert all(a["event"] == "frame.ack" for a in out)
        assert all(a["payload"]["verdict"] == "consumed" for a in out)
        assert out[0]["payload"]["message_id"].startswith("om_")
        # 引擎侧真消费：建卡 + 流式 + 关流
        assert len(transport.calls_of("cardkit_create")) == 1
        assert len(transport.calls_of("cardkit_stream_element")) >= 1
        assert len(transport.calls_of("cardkit_close_streaming")) == 1
    finally:
        await engine.aclose()


async def test_invalid_json_line_emits_error_object_no_ack(transport):
    engine, events, clock = make_engine(transport)
    out: list[dict] = []
    binding = StdioBinding(engine)
    n = await binding.serve(
        lines=lines("{not json", env("deliver", turn="", frame="f_ok", payload={"text": "x"})),
        emit=make_emit(out),
    )
    try:
        assert n == 2
        assert out[0]["event"] == "error"  # §7：解析失败 → 错误对象，不产生 ack
        assert out[0]["payload"]["detail"] == "invalid_json"
        assert out[1]["event"] == "frame.ack"  # 后续行不受影响
        assert out[1]["payload"]["verdict"] == "consumed"
    finally:
        await engine.aclose()


async def test_invalid_envelope_emits_fallback_ack(transport):
    """字段缺失 → 引擎裁决 fallback（01-protocol §7 第二行）。"""
    engine, events, clock = make_engine(transport)
    out: list[dict] = []
    binding = StdioBinding(engine)
    bad = {"v": 1, "event": "answer.delta", "frame_id": "f_bad", "chat_id": "oc_x", "payload": {}}
    n = await binding.serve(lines=lines(bad), emit=make_emit(out))
    try:
        assert n == 1
        assert out[0]["event"] == "frame.ack"
        assert out[0]["payload"]["verdict"] == "fallback"
        assert out[0]["payload"]["detail"].startswith("invalid_envelope")
        assert out[0]["payload"]["frame_id"] == "f_bad"
    finally:
        await engine.aclose()


async def test_unknown_event_emits_fallback_ack(transport):
    engine, events, clock = make_engine(transport)
    out: list[dict] = []
    binding = StdioBinding(engine)
    n = await binding.serve(
        lines=lines(env("widget.spin", turn="", frame="f_u", payload={})),
        emit=make_emit(out),
    )
    try:
        assert out[0]["payload"]["verdict"] == "fallback"
        assert out[0]["payload"]["detail"] == "unknown_event:widget.spin"
    finally:
        await engine.aclose()


async def test_blank_lines_skipped_and_counted_only_real_frames(transport):
    engine, events, clock = make_engine(transport)
    out: list[dict] = []
    binding = StdioBinding(engine)
    n = await binding.serve(
        lines=lines("", "   ", env("deliver", turn="", frame="f_b", payload={"text": "x"})),
        emit=make_emit(out),
    )
    try:
        assert n == 1  # 空行不受理
        assert len(out) == 1
    finally:
        await engine.aclose()


async def test_unsupported_protocol_v_rejected_at_construction():
    """stdio 进程参数等效握手（§6）：不支持即拒绝启动."""
    transport = None
    with pytest.raises(ValueError) as ei:
        StdioBinding(object(), protocol_v=2)
    assert "unsupported protocol_v 2" in str(ei.value)


async def test_out_of_order_acks_allowed_but_engine_order_held(transport):
    """并发提交：同 chat 回执顺序 = 消费顺序（引擎单消费者保证）。"""
    engine, events, clock = make_engine(transport)
    out: list[dict] = []
    binding = StdioBinding(engine)
    n = await binding.serve(
        lines=lines(
            env("turn.start", turn="t_o", frame="f_o1"),
            env("answer.delta", turn="t_o", frame="f_o2", payload={"delta": "a"}),
            env("answer.delta", turn="t_o", frame="f_o3", payload={"delta": "b"}),
        ),
        emit=make_emit(out),
    )
    try:
        assert n == 3
        seqs = [a["payload"]["frame_id"] for a in out]
        assert seqs == ["f_o1", "f_o2", "f_o3"]  # 同 chat：回执与消费序一致
        st = engine._chats[CHAT].turn
        assert st is not None and st.render.answer_text == "ab"
    finally:
        await engine.aclose()


# ── 输入劣化防线（audit #4：劣化输入 ≠ 进程故障）─────────────────────────


async def test_line_too_large_emits_error_and_continues(transport):
    """超限哨兵行 → frame_too_large 错误行，serve 继续消费后续正常行."""
    from lark_streaming.protocol.stdio import _LINE_TOO_LARGE

    engine, events, clock = make_engine(transport)
    out: list[dict] = []
    binding = StdioBinding(engine)
    n = await binding.serve(
        lines=lines(
            _LINE_TOO_LARGE,
            env("deliver", turn="", frame="f_after", payload={"text": "ok"}),
        ),
        emit=make_emit(out),
    )
    try:
        assert n == 2
        assert out[0]["event"] == "error"
        assert out[0]["payload"]["detail"] == "frame_too_large"
        assert out[1]["event"] == "frame.ack"  # 后续行不受影响（修复前一帧带停）
        assert out[1]["payload"]["verdict"] == "consumed"
    finally:
        await engine.aclose()


async def test_thread_pump_line_limit(monkeypatch):
    """线程泵逐行上限（audit #4）：正常流透传（哨兵仅在超限时产出）."""
    import io
    from types import SimpleNamespace

    from lark_streaming.protocol.stdio import _stdin_lines_via_thread

    huge = b"x" * 100 + b"\n"  # 100B 远小于 8MB：不触发哨兵，全部透传
    payload = b'{"v":1,"event":"deliver"}\n' + huge + b'{"v":1}\n'
    fake = io.BufferedReader(io.BytesIO(payload))
    monkeypatch.setattr("sys.stdin", SimpleNamespace(buffer=fake))

    got: list[str] = []
    async for line in _stdin_lines_via_thread():
        got.append(line)
    assert len(got) == 3
    assert got[0].strip().startswith('{"v":1')
    assert got[2].strip() == '{"v":1}'


async def test_thread_pump_oversized_line_yields_sentinel(monkeypatch):
    """真实超限行为：以小 MAX 值注入验证泵的截断丢弃与哨兵产出."""
    import io
    from types import SimpleNamespace

    from lark_streaming.protocol import stdio as stdio_mod

    payload = b"ok\n" + b"y" * 20 + b"\n" + b"tail\n"
    fake = io.BufferedReader(io.BytesIO(payload))
    monkeypatch.setattr("sys.stdin", SimpleNamespace(buffer=fake))
    monkeypatch.setattr(stdio_mod, "MAX_STDIN_LINE_BYTES", 8)

    got: list[str] = []
    async for line in stdio_mod._stdin_lines_via_thread():
        got.append(line)
    assert len(got) == 3
    assert got[0].strip() == "ok"
    assert got[1] == stdio_mod._LINE_TOO_LARGE  # 哨兵
    assert got[2].strip() == "tail"  # 行剩余丢弃后后续行继续
