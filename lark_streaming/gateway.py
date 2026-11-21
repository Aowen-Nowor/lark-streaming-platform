"""网关 shell — 形态 B 常驻进程（05-adapters §3/§6；ADR D8：B = A + server shell）.

组装基座（协议层 Phase 3 已交付）：
- stdio 绑定（零依赖）：stdin 事件行 / stderr 回执行（托管桥接子进程）
- HTTP 绑定：``POST /v1/events``、``/v1/events:batch``、``GET /v1/health``
- WS 绑定：``GET /v1/ws``（hello 握手）——与 HTTP 同进程同端口

配置面（05-adapters §6：env > 配置文件 > 默认值）::

    FEISHU_APP_ID / FEISHU_APP_SECRET   # 凭证（必需）
    FEISHU_DOMAIN=feishu|lark           # 站点（默认 feishu）
    LSP_GATEWAY_BIND=stdio|http|ws      # 形态 B 绑定（可多值，逗号分隔）
    LSP_GATEWAY_PORT=7800               # HTTP/WS 端口
    LSP_GATEWAY_HOST=127.0.0.1          # HTTP/WS 监听地址（audit #13）
    LSP_GATEWAY_TOKEN=<secret>          # HTTP/WS Bearer 鉴权（audit #1：
                                        # 非 loopback 监听必须配置，否则拒绝启动）
    LSP_LOG_LEVEL=INFO

配置文件（``lsp.yaml``，经 ``--config`` 指定）支持全部设置键（§4 表）
与 env 同名覆盖；设置键接受 §4 冠名（``clarify_suspend_sec`` /
``footer_reasoning_level`` / ``card_text_sizes``）与引擎原名
（``clarify_window_sec`` / ``reasoning_level`` / ``text_sizes``）双轨。

停机语义（§3）：SIGINT/SIGTERM → ``engine.aclose()``（seal_all_on_shutdown
先定稿再关流）→ stdio 接入方经 stderr 收 ``lifecycle.event{shutdown}``
后转原生；HTTP/WS 接入方以连接断开感知（在途帧已被引擎消费完）。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from dataclasses import dataclass, field
from typing import Any

from .credentials import FeishuCredentials
from .engine import EngineSettings, LifecycleEvent, StreamingEngine
from .renderer import TextSizes, Theme
from .transport import FeishuTransport

_logger = logging.getLogger("lark_streaming.gateway")

__all__ = ["GatewayConfig", "load_config", "run_gateway"]

#: 合法绑定名（LSP_GATEWAY_BIND；ws 与 http 同进程——ws 端点由 HTTP app 承载）
VALID_BINDS = ("stdio", "http", "ws")

#: §4 设置键 → EngineSettings 字段双轨映射（§4 冠名 → 引擎原名）
_SETTINGS_KEY_ALIASES: dict[str, str] = {
    "clarify_suspend_sec": "clarify_window_sec",
    "degrade_cooldown_sec": "_degrade_cooldown_sec",
}


@dataclass(frozen=True)
class GatewayConfig:
    """网关 shell 配置（env > 配置文件 > 默认值）."""

    binds: tuple[str, ...] = ("stdio",)
    host: str = "127.0.0.1"
    port: int = 7800
    log_level: str = "INFO"
    auth_token: str = ""
    settings: EngineSettings = field(default_factory=EngineSettings)
    theme: Theme = field(default_factory=Theme)
    credentials: FeishuCredentials | None = None


def _env_scalar(raw: str) -> Any:
    """env 标量解析：数值直转；JSON 对象/数组解析；其余字符串."""
    try:
        return json.loads(raw)
    except ValueError:
        pass
    try:
        return float(raw) if ("." in raw or "e" in raw.lower()) else int(raw)
    except ValueError:
        return raw


def _apply_settings_kwargs(source: dict[str, Any]) -> dict[str, Any]:
    """§4 设置键（含 env 同名覆盖）→ EngineSettings 字段 kwargs.

    未知键忽略（配置面宽容：接入方配置错误不阻断消息主链；越界值由
    EngineSettings ``__post_init__`` 钳制，不抛异常）。
    """
    fields = EngineSettings.__dataclass_fields__
    kwargs: dict[str, Any] = {}
    for key, value in source.items():
        name = _SETTINGS_KEY_ALIASES.get(key, key)
        if name == "_degrade_cooldown_sec":
            # 标量 → 双冷却同值；dict → 分道（429 / 其他）
            if isinstance(value, dict):
                if "rate_limit" in value:
                    kwargs["degrade_cooldown_rate_limit_sec"] = float(value["rate_limit"])
                if "other" in value:
                    kwargs["degrade_cooldown_other_sec"] = float(value["other"])
            else:
                try:
                    both = float(value)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    continue
                kwargs["degrade_cooldown_rate_limit_sec"] = both
                kwargs["degrade_cooldown_other_sec"] = both
            continue
        if name in fields:
            kwargs[name] = value
    return kwargs


def _build_theme(source: dict[str, Any]) -> Theme:
    """theme 配置段 → :class:`Theme`（03-card-schema §3 token 组）."""
    text_sizes_raw = source.get("text_sizes") or source.get("card_text_sizes")
    text_sizes: TextSizes | None = None
    if isinstance(text_sizes_raw, dict):
        answer = str(text_sizes_raw.get("answer") or "")
        aux = str(text_sizes_raw.get("aux") or text_sizes_raw.get("normal") or "")
        if answer or aux:
            text_sizes = TextSizes(answer=answer, aux=aux)
    level = str(source.get("reasoning_level") or source.get("footer_reasoning_level") or "")
    return Theme(
        tool_icons=dict(source.get("tool_icons") or {}),
        tool_titles=dict(source.get("tool_titles") or {}),
        text_sizes=text_sizes,
        reasoning_level=level,
        panel_title_default=str(source.get("panel_title_default") or "agent loop"),
    )


def load_config(
    *,
    env: dict[str, str] | None = None,
    config_path: str | None = None,
) -> GatewayConfig:
    """配置装配：env > 配置文件（lsp.yaml）> 默认值（05-adapters §6）."""
    e = env if env is not None else dict(os.environ)

    file_data: dict[str, Any] = {}
    if config_path:
        import yaml  # PyYAML（核心依赖，随包分发）

        with open(config_path, encoding="utf-8") as fh:
            loaded = yaml.safe_load(fh) or {}
        if isinstance(loaded, dict):
            file_data = loaded

    feishu_file = file_data.get("feishu") if isinstance(file_data.get("feishu"), dict) else {}
    gateway_file = file_data.get("gateway") if isinstance(file_data.get("gateway"), dict) else {}

    # ── 绑定与端口 ─────────────────────────────────────────────────────
    bind_raw = e.get("LSP_GATEWAY_BIND") or gateway_file.get("bind") or "stdio"
    if isinstance(bind_raw, str):
        binds = tuple(b.strip().lower() for b in bind_raw.replace(",", " ").split() if b.strip())
    elif isinstance(bind_raw, (list, tuple)):
        binds = tuple(str(b).strip().lower() for b in bind_raw if str(b).strip())
    else:
        binds = ("stdio",)
    binds = tuple(b for b in binds if b in VALID_BINDS) or ("stdio",)

    port_raw = e.get("LSP_GATEWAY_PORT") or gateway_file.get("port")
    try:
        port = int(port_raw) if port_raw is not None else 7800
    except (TypeError, ValueError):
        port = 7800
    port = min(max(port, 1), 65535)

    # audit #13：host 同样走 env > 配置文件（与 PORT 一致；§6 顺序约定）
    host = str(e.get("LSP_GATEWAY_HOST") or gateway_file.get("host") or "127.0.0.1")

    # ── HTTP/WS Bearer token（audit #1：env > 配置文件；空 = 仅 loopback 可用）─
    auth_token = str(
        e.get("LSP_GATEWAY_TOKEN") or gateway_file.get("auth_token") or ""
    )

    # ── 日志级别 ───────────────────────────────────────────────────────
    log_level = str(e.get("LSP_LOG_LEVEL") or file_data.get("log_level") or "INFO").upper()

    # ── 凭证（env > 配置文件；app_id/app_secret 必需——transport 构造期校验）─
    credentials = FeishuCredentials.from_env(env=e) if e.get("FEISHU_APP_ID") else None
    if credentials is None:
        app_id = str(feishu_file.get("app_id") or "")
        app_secret = str(feishu_file.get("app_secret") or "")
        domain = str(feishu_file.get("domain") or "feishu").strip().lower()
        if app_id and app_secret:
            from .credentials import FEISHU_BASE, LARK_BASE

            credentials = FeishuCredentials(
                app_id=app_id, app_secret=app_secret,
                base_url=LARK_BASE if domain == "lark" else FEISHU_BASE,
            )

    # ── 设置键：env 同名覆盖 > 配置文件 settings 段 > 默认值 ────────────
    setting_sources: dict[str, Any] = {}
    file_settings = file_data.get("settings")
    if isinstance(file_settings, dict):
        setting_sources.update(file_settings)
    env_scan_keys = (
        tuple(setting_sources)
        + tuple(EngineSettings.__dataclass_fields__)
        + tuple(_SETTINGS_KEY_ALIASES)
        + ("footer_reasoning_level", "card_text_sizes")  # §4 theme 冠名键
    )
    for key in env_scan_keys:
        if key in e:  # env 同名覆盖（§6：env > 配置文件）
            setting_sources[key] = _env_scalar(e[key])
    # §4 渲染保留键不进引擎设置（panel_expanded / print_strategy /
    # print_step：v1 渲染器固定默认，诚实标注不静默吞）
    _reserved = ("panel_expanded", "print_strategy", "print_step")
    settings_kwargs = _apply_settings_kwargs(
        {k: v for k, v in setting_sources.items() if k not in _reserved}
    )
    settings = EngineSettings(**settings_kwargs)

    # ── theme 段（card_text_sizes / footer_reasoning_level §4 冠名同轨）──
    theme_raw = file_data.get("theme") if isinstance(file_data.get("theme"), dict) else {}
    theme_source = dict(theme_raw)
    if "card_text_sizes" in setting_sources and isinstance(setting_sources["card_text_sizes"], dict):
        theme_source.setdefault("card_text_sizes", setting_sources["card_text_sizes"])
    if "footer_reasoning_level" in setting_sources:
        theme_source.setdefault("footer_reasoning_level", setting_sources["footer_reasoning_level"])
    theme = _build_theme(theme_source)

    return GatewayConfig(
        binds=binds, host=host, port=port, log_level=log_level,
        auth_token=auth_token,
        settings=settings, theme=theme, credentials=credentials,
    )


async def run_gateway(
    config: GatewayConfig,
    *,
    transport: FeishuTransport | None = None,
    engine: StreamingEngine | None = None,
    stdin_lines: Any = None,
    emit: Any = None,
    stop_event: asyncio.Event | None = None,
) -> int:
    """网关主循环：装配 → 绑定编排 → 优雅停机.

    ``transport``/``engine``/``stdin_lines``/``emit``/``stop_event`` 可注入
    （测试台架；缺省 = 生产装配）。返回退出码（0 = 正常停机）。
    """
    logging.basicConfig(level=getattr(logging, config.log_level, logging.INFO))

    own_transport = transport is None
    own_engine = engine is None
    if transport is None:
        if config.credentials is None:
            _logger.error("gateway: credentials missing (FEISHU_APP_ID/SECRET)")
            return 2
        transport = FeishuTransport(config.credentials)
    if engine is None:
        engine = StreamingEngine(
            transport, theme=config.theme, settings=config.settings,
            on_lifecycle=_lifecycle_logger,
        )

    stop = stop_event or asyncio.Event()
    loop = asyncio.get_running_loop()
    _install_signal_handlers(loop, stop)

    runner = None
    stdio_task: asyncio.Task[int] | None = None
    try:
        if "http" in config.binds or "ws" in config.binds:
            from .protocol.server import serve_http, stop_http

            runner = await serve_http(
                engine, host=config.host, port=config.port, settings=config.settings,
                auth_token=config.auth_token,
            )
            _logger.info("gateway: http/ws listening %s:%s", config.host, config.port)
        if "stdio" in config.binds:
            from .protocol.stdio import StdioBinding

            _logger.info("gateway: stdio binding active (stdin events / stderr acks)")
            stdio_task = asyncio.create_task(
                StdioBinding(engine, settings=config.settings).serve(
                    lines=stdin_lines, emit=emit,
                )
            )
            # stdio 主导停机：输入耗尽 = 接入方收口 → 触发统一停机
            def _stdio_done(task: asyncio.Task[int]) -> None:
                if not task.cancelled() and task.exception() is not None:
                    # audit #4：绑定层崩溃 ≠ 正常收口——error 摘要留痕，
                    # 停机仍执行（网关无 stdio 即无事件源），但日志可归因
                    exc = task.exception()
                    _logger.error(
                        "gateway: stdio binding crashed: %s: %s",
                        type(exc).__name__, exc,
                    )
                stop.set()
            stdio_task.add_done_callback(_stdio_done)
        if "stdio" not in config.binds and runner is None:
            _logger.error("gateway: no bindable transport in %s", config.binds)
            return 2
        await stop.wait()
        if stdio_task is not None and not stdio_task.done():
            # 信号停机：取消 stdin 等待（在途帧由引擎排空语义承接）
            stdio_task.cancel()
            try:
                await stdio_task
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 — 停机尽力而为
                pass
        return 0
    finally:
        if runner is not None:
            from .protocol.server import stop_http

            try:
                await stop_http(runner)
            except Exception:  # noqa: BLE001 — 停机尽力而为
                _logger.debug("gateway: http stop skipped", exc_info=True)
        if own_engine and engine is not None:
            await engine.aclose()  # seal_all_on_shutdown 先定稿再关流
        if own_transport and transport is not None:
            await transport.aclose()


def _lifecycle_logger(ev: LifecycleEvent) -> None:
    """lifecycle 通告 → stderr JSON 行（stdio 桥接面）+ 日志.

    stdio 绑定的回执通道（01-protocol §3）承载全部反向事件；HTTP/WS
    接入方以连接/日志感知（v1 绑定面诚实边界，ADAPTER_SDK §3）。
    """
    line = json.dumps(ev.to_protocol(), ensure_ascii=False)
    try:
        sys.stderr.write(line + "\n")
        sys.stderr.flush()
    except Exception:  # noqa: BLE001 — stderr 不可写（守护化）只留日志
        pass
    _logger.info("lifecycle: %s", line)


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    def _stop() -> None:
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _stop)
        except (NotImplementedError, RuntimeError, ValueError):
            pass  # 非 unix / 非主线程：测试台架以 stop_event 显式停机
