"""HTTP / WS 绑定 — aiohttp 承载（01-protocol §3/§6；依赖 gateway extra）.

端点（01-protocol §3）：
- ``POST /v1/events``       单事件，同步回单个 ack（业务失败恒 200 + verdict）
- ``POST /v1/events:batch`` 数组逐个回 ack 数组（顺序与请求元素一致；
  非 object 元素槽位回错误对象）
- ``GET  /v1/health``       引擎状态摘要（02-engine §9 同源）
- ``GET  /v1/ws``           WS 长连接（路径为 01-protocol §3 的具体化）

协议级失败 → HTTP 400 + 错误对象（与 WS error 帧/stdio 行同形态）；
业务失败恒 200 + ack.verdict（§7）。HTTP 无握手——信封 ``v`` 字段即协商
（§6）；WS 连接首帧必须是 hello，协商失败关闭连接。

WS 回执时序：逐帧并发提交、完成即回写（接入方不得假设 ack 时序，
05-adapters §1）；同 chat ack 顺序由引擎 per-chat 单消费者保证。
连接断开时在途帧继续被引擎消费（卡片管线不因回执通道消失而中断），
仅回执发送失败被吞掉——永不丢帧红线由引擎侧独立成立。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from aiohttp import WSMsgType, web

from lark_streaming.engine import StreamingEngine

from .feishu_callback import FEISHU_CALLBACK_PATH, build_feishu_callback_handler
from .messages import WireFrameError, negotiate_hello, parse_frame, wire_error

_logger = logging.getLogger("lark_streaming.protocol.server")

#: 单次 batch 请求元素上限（audit #2：无上限 batch = 放大攻击面——
# 单请求打满引擎全部 chat 队列；256 覆盖正常批量，超出走协议错误）
MAX_BATCH_SIZE = 256

#: loopback 监听地址集合（fail-closed 判定基线：这些地址之外必须带 token）
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})

#: 网关 Bearer token 环境变量名（audit #1：secure-by-default 信任模型）
GATEWAY_TOKEN_ENV = "LSP_GATEWAY_TOKEN"

#: feishu-callback 绑定验签材料 env 名（01-protocol §3.2；独立鉴权域——
#: 飞书回调无法携带 Bearer，验签走 verification_token/encrypt_key）
FEISHU_VERIFICATION_TOKEN_ENV = "FEISHU_VERIFICATION_TOKEN"
FEISHU_ENCRYPT_KEY_ENV = "FEISHU_ENCRYPT_KEY"

__all__ = [
    "build_app", "serve_http", "stop_http", "run_ws_connection",
    "MAX_BATCH_SIZE", "GATEWAY_TOKEN_ENV",
    "FEISHU_VERIFICATION_TOKEN_ENV", "FEISHU_ENCRYPT_KEY_ENV",
]


def _dump(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False)


def _bearer_middleware(token: str):
    """Bearer token 鉴权中间件（audit #1）：token 非空时全端点强制.

    覆盖 /v1/events、/v1/events:batch、/v1/health、/v1/ws（WS 握手同样
    走 HTTP 升级请求，middleware 生效）；未携带或错配 → 401 + 错误对象
    （与协议错误同形态，§7）。

    唯一豁免：/v1/feishu/callback（§3.2 独立鉴权域）——飞书回调无法
    携带 Bearer，验签走 verification_token/encrypt_key；若被 Bearer
    覆盖，非 loopback 部署（强制配 token）下回调端点恒 401 等于不存在。
    """
    expected = f"Bearer {token}"

    @web.middleware
    async def mw(request: web.Request, handler) -> web.StreamResponse:
        if request.path == FEISHU_CALLBACK_PATH:
            return await handler(request)
        if request.headers.get("Authorization", "") != expected:
            return web.json_response(wire_error("unauthorized"), status=401)
        return await handler(request)

    return mw


def build_app(
    engine: StreamingEngine,
    *,
    settings: Any = None,
    auth_token: str = "",
    feishu_verification_token: str = "",
    feishu_encrypt_key: str = "",
) -> web.Application:
    """装配 HTTP+WS 网关应用（Phase 4 网关 shell 的组装基座）.

    ``auth_token`` 非空 → 全端点 Bearer 鉴权（audit #1 secure-by-default）；
    feishu-callback 路由独立鉴权域（飞书回调无法携带 Bearer，§3.2），
    未配置 verification_token → 端点恒 503（fail-closed 可见禁用态）。
    """
    app = web.Application()
    if auth_token:
        app.middlewares.append(_bearer_middleware(auth_token))

    async def events(request: web.Request) -> web.Response:
        try:
            raw = await request.json()
        except Exception:  # noqa: BLE001 — 非 JSON body
            return web.json_response(wire_error("invalid_json"), status=400)
        if not isinstance(raw, dict):
            return web.json_response(wire_error("invalid_envelope:not_an_object"), status=400)
        ack = await engine.submit(raw)
        return web.json_response(ack.to_protocol())

    async def events_batch(request: web.Request) -> web.Response:
        try:
            raw = await request.json()
        except Exception:  # noqa: BLE001
            return web.json_response(wire_error("invalid_json"), status=400)
        if not isinstance(raw, list):
            return web.json_response(wire_error("invalid_envelope:not_an_array"), status=400)
        if len(raw) > MAX_BATCH_SIZE:
            return web.json_response(
                wire_error(f"batch_too_large:max_{MAX_BATCH_SIZE}"), status=400
            )
        acks: list[dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                acks.append(wire_error("invalid_envelope:not_an_object"))
                continue
            ack = await engine.submit(item)
            acks.append(ack.to_protocol())
        return web.json_response(acks)

    async def health(request: web.Request) -> web.Response:
        # audit #11：默认只回汇总三计数；chat 明细需显式 verbose 拉取
        verbose = request.query.get("verbose", "") in ("1", "true")
        return web.json_response(engine.snapshot_health(verbose=verbose))

    async def websocket(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30.0)
        await ws.prepare(request)
        await run_ws_connection(ws, engine, settings=settings)
        return ws

    app.router.add_post("/v1/events", events)
    app.router.add_post("/v1/events:batch", events_batch)
    app.router.add_get("/v1/health", health)
    app.router.add_get("/v1/ws", websocket)
    app.router.add_post(
        FEISHU_CALLBACK_PATH,
        build_feishu_callback_handler(
            engine,
            verification_token=feishu_verification_token,
            encrypt_key=feishu_encrypt_key,
        ),
    )
    return app


async def run_ws_connection(
    ws: web.WebSocketResponse,
    engine: StreamingEngine,
    *,
    settings: Any = None,
) -> None:
    """单条 WS 连接的生命周期：hello 握手 → 事件帧 → 逐帧回执."""
    send_lock = asyncio.Lock()
    pending: set[asyncio.Task[None]] = set()
    handshaked = False

    async def send(payload: dict[str, Any]) -> None:
        async with send_lock:
            await ws.send_str(_dump(payload))

    async def submit_and_send(raw: dict[str, Any]) -> None:
        try:
            ack = await engine.submit(raw)
            wire = ack.to_protocol()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — 回执通道兜底
            wire = wire_error("binding_internal_error")
        try:
            await send(wire)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — 连接已断：帧已被引擎消费，
            # 回执不可达（接入方重连后以 frame_id 幂等查询语义由上层处理）
            _logger.debug("ws: ack send failed (connection closed)", exc_info=True)

    try:
        async for msg in ws:
            if msg.type is not WSMsgType.TEXT:
                if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                    break
                continue
            try:
                raw = parse_frame(msg.data)
            except WireFrameError as e:
                await send(wire_error(e.detail, e.frame_id))
                continue
            fid = raw.get("frame_id")
            fid = fid if isinstance(fid, str) else ""
            if not handshaked:
                if raw.get("event") != "hello":
                    # §6：连接首帧必须是 hello——违规回错误帧并关闭
                    await send(wire_error("handshake_required", fid))
                    await ws.close(
                        code=web.WSCloseCode.POLICY_VIOLATION, message=b"hello required"
                    )
                    break
                reply, keep = negotiate_hello(raw, settings=settings)
                await send(reply)
                if not keep:
                    await ws.close(
                        code=web.WSCloseCode.POLICY_VIOLATION, message=b"unsupported protocol"
                    )
                    break
                handshaked = True
                continue
            if raw.get("event") == "hello":
                await send(wire_error("already_handshaked", fid))
                continue
            task = asyncio.create_task(submit_and_send(raw))
            pending.add(task)
            task.add_done_callback(pending.discard)
    finally:
        if pending:
            # 连接关闭后等待在途帧被引擎消费完（卡片管线完成性优先于回执）
            await asyncio.gather(*pending, return_exceptions=True)


async def serve_http(
    engine: StreamingEngine,
    *,
    host: str = "127.0.0.1",
    port: int = 7800,
    settings: Any = None,
    auth_token: str | None = None,
    feishu_verification_token: str | None = None,
    feishu_encrypt_key: str | None = None,
) -> web.AppRunner:
    """启动 HTTP/WS 监听（返回 runner 供调用方管理生命周期）.

    fail-closed（audit #1）：非 loopback 监听且无 token（参数未传时回落
    env ``LSP_GATEWAY_TOKEN``）→ 拒绝启动——secure-by-default 信任模型：
    错误配置宁可起不来，不可裸奔上线。
    feishu-callback 验签材料同样参数优先、env 回落
    （``FEISHU_VERIFICATION_TOKEN`` / ``FEISHU_ENCRYPT_KEY``，§3.2）。
    """
    token = auth_token if auth_token is not None else os.environ.get(GATEWAY_TOKEN_ENV, "")
    if host not in _LOOPBACK_HOSTS and not token:
        raise RuntimeError(
            f"gateway: non-loopback bind {host} without {GATEWAY_TOKEN_ENV} "
            "is refused (fail-closed; set the env or bind 127.0.0.1)"
        )
    cb_token = (
        feishu_verification_token
        if feishu_verification_token is not None
        else os.environ.get(FEISHU_VERIFICATION_TOKEN_ENV, "")
    )
    cb_key = (
        feishu_encrypt_key
        if feishu_encrypt_key is not None
        else os.environ.get(FEISHU_ENCRYPT_KEY_ENV, "")
    )
    runner = web.AppRunner(build_app(
        engine, settings=settings, auth_token=token,
        feishu_verification_token=cb_token, feishu_encrypt_key=cb_key,
    ))
    await runner.setup()
    site = web.TCPSite(runner, host=host, port=port)
    await site.start()
    return runner


async def stop_http(runner: web.AppRunner) -> None:
    await runner.cleanup()
