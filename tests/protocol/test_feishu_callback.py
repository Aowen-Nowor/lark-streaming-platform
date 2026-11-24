"""feishu-callback 绑定回归（01-protocol §3.2）— 第四类入站面.

- 独立鉴权域：verification_token 验签 / encrypt_key 解密（AES-256-CBC），
  Bearer 中间件豁免回调路径（飞书回调无法携带 Bearer）；
- card.action.trigger → resolve_clarify → clarify.answer 走引擎同一条
  入口（同队列同背压同幂等）；
- verdict 分道：consumed → 200 / queue_full → 503（飞书回调退避重试
  接管该方向的传输级重试）/ 其余 fallback → 200（控制帧无原生履行
  义务）；
- fail-closed：未配置 token → 503 可见禁用态；密文无 key / 解密失败 /
  token 错配 → 403 且引擎零状态变更。
"""

from __future__ import annotations

import base64
import hashlib
import json

import httpx
import pytest
from aiohttp.test_utils import TestServer

from lark_streaming.engine import Ack, Verdict
from lark_streaming.protocol.feishu_callback import (
    CLARIFY_CALLBACK_KEY,
    CallbackRejected,
    decrypt_feishu_payload,
)
from lark_streaming.protocol.server import build_app
from tests.engine.conftest import CHAT, env, make_engine, make_settings

TOKEN = "vt_test_token"
KEY = "enc_test_key"


def _encrypt(payload: dict, *, key: str = KEY) -> str:
    """按官方约定加密回调明文（AES-256-CBC，key=sha256(key)，iv 前 16B）."""
    from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

    iv = b"\x01" * 16
    raw = json.dumps(payload).encode("utf-8")
    pad = 16 - len(raw) % 16
    raw += bytes([pad]) * pad
    enc = Cipher(
        algorithms.AES(hashlib.sha256(key.encode("utf-8")).digest()), modes.CBC(iv)
    ).encryptor()
    return base64.b64encode(iv + enc.update(raw) + enc.finalize()).decode("utf-8")


def card_action_body(
    clarify_id: str, *, token: str | None = TOKEN, kind: str = "select",
    option: str | None = "0", event_id: str = "e1",
) -> dict:
    """构造 card.action.trigger 回调体（v2.0 信封；token=None 时缺省 header）。"""
    action: dict = {
        "tag": "select_static",
        "value": {CLARIFY_CALLBACK_KEY: kind, "clarify_id": clarify_id},
    }
    if option is not None:
        action["option"] = option
    body: dict = {
        "schema": "2.0",
        "event": {"operator": {"open_id": "ou_x"}, "action": action},
    }
    if token is not None:
        body["header"] = {"event_id": event_id, "event_type": "card.action.trigger", "token": token}
    return body


async def _pause_chat(client: httpx.AsyncClient) -> None:
    """经 /v1/events 造一个 PAUSED chat（turn.start + clarify.request）。"""
    r = await client.post("/v1/events", json=env("turn.start", turn="t_cb", frame="f1"))
    assert r.json()["payload"]["verdict"] == "consumed"
    r = await client.post(
        "/v1/events",
        json=env("clarify.request", turn="t_cb", frame="f2", payload={
            "form": {"kind": "select", "question": "选哪个", "options": ["甲", "乙"]},
        }),
    )
    assert r.json()["payload"]["verdict"] == "consumed"


@pytest.fixture
async def cb(transport):
    """带 feishu-callback 验签材料的 HTTP 台架（真实引擎）。"""
    engine, _events, _clock = make_engine(
        transport, settings=make_settings(clarify_window_sec=0)
    )
    server = TestServer(build_app(
        engine, feishu_verification_token=TOKEN, feishu_encrypt_key=KEY,
    ))
    await server.start_server()
    client = httpx.AsyncClient(base_url=str(server.make_url("")), timeout=10)
    try:
        yield engine, transport, client
    finally:
        await client.aclose()
        await server.close()
        await engine.aclose()


async def test_unconfigured_token_503_fail_closed(transport):
    """未配置 verification_token：端点存在但恒 503（可见禁用态）."""
    engine, _events, _clock = make_engine(transport)
    server = TestServer(build_app(engine))
    await server.start_server()
    client = httpx.AsyncClient(base_url=str(server.make_url("")), timeout=10)
    try:
        r = await client.post("/v1/feishu/callback", json={"type": "url_verification"})
        assert r.status_code == 503
        assert "feishu_callback_disabled" in r.json()["error"]
    finally:
        await client.aclose()
        await server.close()
        await engine.aclose()


async def test_url_verification_echo(cb):
    _engine, _transport, client = cb
    r = await client.post(
        "/v1/feishu/callback",
        json={"type": "url_verification", "token": TOKEN, "challenge": "ajls384kdd"},
    )
    assert r.status_code == 200
    assert r.json() == {"challenge": "ajls384kdd"}


async def test_url_verification_wrong_token_403(cb):
    _engine, _transport, client = cb
    r = await client.post(
        "/v1/feishu/callback",
        json={"type": "url_verification", "token": "wrong", "challenge": "x"},
    )
    assert r.status_code == 403
    assert r.json()["error"] == "verification_token_mismatch"


async def test_url_verification_missing_challenge_400(cb):
    _engine, _transport, client = cb
    r = await client.post(
        "/v1/feishu/callback", json={"type": "url_verification", "token": TOKEN},
    )
    assert r.status_code == 400


async def test_card_action_paused_chat_resumes(cb):
    """PAUSED chat 回调命中 → clarify.answer 进引擎 → 续流（同一条入口）."""
    engine, transport, client = cb
    await _pause_chat(client)
    clarify_id = engine._chats[CHAT].turn.clarify_id
    assert clarify_id
    r = await client.post("/v1/feishu/callback", json=card_action_body(clarify_id))
    assert r.status_code == 200
    assert r.json() == {"ok": True, "verdict": "consumed"}
    # 状态机：PAUSED → STREAMING；暂停行摘除 → 面板 flush 走传输层
    assert engine._chats[CHAT].turn.phase.name == "STREAMING"
    assert len(transport.calls_of("cardkit_update")) >= 1


async def test_card_action_stale_clarify_ignored(cb):
    """已答后的重复回调：resolve 落空 → 200 ignored（幂等二道护栏）."""
    engine, _transport, client = cb
    await _pause_chat(client)
    clarify_id = engine._chats[CHAT].turn.clarify_id
    r = await client.post("/v1/events", json=env(
        "clarify.answer", turn="t_cb", frame="f3", payload={"values": {"action": "select"}},
    ))
    assert r.json()["payload"]["verdict"] == "consumed"
    r = await client.post("/v1/feishu/callback", json=card_action_body(clarify_id))
    assert r.status_code == 200
    assert r.json()["ignored"] == "clarify_not_active"


async def test_card_action_unknown_clarify_ignored(cb):
    """过期/重启后的回调（前代 expired 语义）：200 不触发飞书重试风暴."""
    _engine, _transport, client = cb
    r = await client.post("/v1/feishu/callback", json=card_action_body("deadbeef0000"))
    assert r.status_code == 200
    assert r.json()["ignored"] == "clarify_not_active"


async def test_foreign_action_ignored(cb):
    """非本协议命名空间的按钮（无 lsp_clarify_action）：无害忽略."""
    _engine, _transport, client = cb
    body = card_action_body("whatever", token=TOKEN)
    body["event"]["action"]["value"] = {"other_key": "x"}
    r = await client.post("/v1/feishu/callback", json=body)
    assert r.status_code == 200
    assert r.json()["ignored"] == "not_lsp_clarify_action"


async def test_other_event_type_ignored(cb):
    """事件订阅 URL 误指到平台（im.message.receive_v1 等）：200 无害忽略."""
    _engine, _transport, client = cb
    r = await client.post("/v1/feishu/callback", json={
        "schema": "2.0",
        "header": {"event_id": "e2", "event_type": "im.message.receive_v1", "token": TOKEN},
        "event": {"message": {"message_id": "om_x"}},
    })
    assert r.status_code == 200
    assert r.json()["ignored"] == "im.message.receive_v1"


async def test_card_action_wrong_token_403(cb):
    """token 错配 → 403（引擎零状态变更，不进 resolve/submit）."""
    _engine, _transport, client = cb
    r = await client.post(
        "/v1/feishu/callback", json=card_action_body("deadbeef0000", token="wrong"),
    )
    assert r.status_code == 403


async def test_invalid_json_400(cb):
    _engine, _transport, client = cb
    r = await client.post("/v1/feishu/callback", content=b"{oops",
                          headers={"Content-Type": "application/json"})
    assert r.status_code == 400
    assert r.json()["error"] == "invalid_json"


async def test_encrypted_url_verification_roundtrip(cb):
    """encrypt 密文 + encrypt_key → 解密后走正常分道（echo challenge）。"""
    _engine, _transport, client = cb
    plain = {"type": "url_verification", "token": TOKEN, "challenge": "enc_challenge"}
    r = await client.post("/v1/feishu/callback", json={"encrypt": _encrypt(plain)})
    assert r.status_code == 200
    assert r.json() == {"challenge": "enc_challenge"}


async def test_encrypted_without_key_403(transport):
    """密文到达而无 encrypt_key → fail-closed 403（不猜内容）."""
    engine, _events, _clock = make_engine(transport)
    server = TestServer(build_app(engine, feishu_verification_token=TOKEN))
    await server.start_server()
    client = httpx.AsyncClient(base_url=str(server.make_url("")), timeout=10)
    try:
        plain = {"type": "url_verification", "token": TOKEN, "challenge": "x"}
        r = await client.post("/v1/feishu/callback", json={"encrypt": _encrypt(plain)})
        assert r.status_code == 403
        assert r.json()["error"] == "encrypt_payload_without_key"
    finally:
        await client.aclose()
        await server.close()
        await engine.aclose()


async def test_encrypted_bad_ciphertext_403(cb):
    _engine, _transport, client = cb
    r = await client.post(
        "/v1/feishu/callback", json={"encrypt": base64.b64encode(b"short").decode()},
    )
    assert r.status_code == 403


def test_decrypt_rejects_bad_variants():
    """解密单元分道：坏 base64 / 坏长度 / 坏填充 / 明文非 JSON."""
    good_plain = json.dumps({"a": 1}).encode()
    pad = 16 - len(good_plain) % 16
    good_plain += bytes([pad]) * pad
    with pytest.raises(CallbackRejected):
        decrypt_feishu_payload("!!!not-base64!!!", KEY)
    with pytest.raises(CallbackRejected):
        decrypt_feishu_payload(base64.b64encode(b"0" * 8).decode(), KEY)
    with pytest.raises(CallbackRejected):
        decrypt_feishu_payload("", KEY)
    # 明文非 JSON（填充恰好合法）：fail-closed
    raw = b"x" * 16
    raw += bytes([16]) * 16
    blob = base64.b64encode(raw).decode()
    with pytest.raises(CallbackRejected):
        decrypt_feishu_payload(blob, KEY)
    assert _encrypt({"ok": 1})  # 加密侧自检（供端到端用例使用）


class _StubQueueFullEngine:
    """仅满足 handler 依赖面的桩：submit 恒回 queue_full FALLBACK."""

    def resolve_clarify(self, clarify_id: str):
        return (CHAT, "t_stub")

    async def submit(self, envelope: dict) -> Ack:
        return Ack("", Verdict.FALLBACK, detail="queue_full:native_delivery")


async def test_queue_full_503_feishu_retry(transport):
    """queue_full → 503：飞书回调退避重试接管该方向的传输级重试."""
    server = TestServer(build_app(
        _StubQueueFullEngine(),  # type: ignore[arg-type]
        feishu_verification_token=TOKEN,
    ))
    await server.start_server()
    client = httpx.AsyncClient(base_url=str(server.make_url("")), timeout=10)
    try:
        r = await client.post(
            "/v1/feishu/callback",
            json=card_action_body("whatever-clarify"),  # 桩恒 resolve 成功
        )
        assert r.status_code == 503
        assert r.json()["error"] == "queue_full"
    finally:
        await client.aclose()
        await server.close()


async def test_bearer_exempt_callback_and_protects_others(transport):
    """Bearer 豁免仅回调路径：无 Bearer 回调可达；/v1/events 恒 401."""
    engine, _events, _clock = make_engine(transport)
    server = TestServer(build_app(
        engine, auth_token="gw_secret",
        feishu_verification_token=TOKEN,
    ))
    await server.start_server()
    client = httpx.AsyncClient(base_url=str(server.make_url("")), timeout=10)
    try:
        r = await client.post("/v1/events", json=env("turn.start", turn="t_nb"))
        assert r.status_code == 401
        r = await client.post(
            "/v1/feishu/callback",
            json={"type": "url_verification", "token": TOKEN, "challenge": "c1"},
        )
        assert r.status_code == 200
        assert r.json() == {"challenge": "c1"}
    finally:
        await client.aclose()
        await server.close()
        await engine.aclose()
