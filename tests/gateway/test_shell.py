"""网关 shell 单测 — 配置面 / 绑定编排 / 优雅停机（05-adapters §3/§6）."""

from __future__ import annotations

import asyncio
import json

import aiohttp

from lark_streaming.gateway import GatewayConfig, load_config, run_gateway


def yaml_config(tmp_path, data: str):
    path = tmp_path / "lsp.yaml"
    path.write_text(data, encoding="utf-8")
    return str(path)


class TestLoadConfig:
    def test_defaults(self):
        cfg = load_config(env={})
        assert cfg.binds == ("stdio",)
        assert cfg.port == 7800
        assert cfg.host == "127.0.0.1"
        assert cfg.log_level == "INFO"

    def test_env_bind_multi_value_and_port(self):
        cfg = load_config(env={"LSP_GATEWAY_BIND": "stdio, http,ws",
                               "LSP_GATEWAY_PORT": "9100"})
        assert cfg.binds == ("stdio", "http", "ws")
        assert cfg.port == 9100

    def test_env_invalid_values_fall_back(self):
        cfg = load_config(env={"LSP_GATEWAY_BIND": "bogus,ws",
                               "LSP_GATEWAY_PORT": "not-a-number"})
        assert cfg.binds == ("ws",)  # 非法名剔除，合法保留
        assert cfg.port == 7800

    def test_env_credentials(self):
        cfg = load_config(env={"FEISHU_APP_ID": "cli_x", "FEISHU_APP_SECRET": "s3cr3t",
                               "FEISHU_DOMAIN": "lark"})
        assert cfg.credentials is not None
        assert cfg.credentials.app_id == "cli_x"
        assert "larksuite" in cfg.credentials.base_url

    def test_env_settings_override_namesake(self, ):
        # §6：env 同名覆盖（设置键 env 直覆）
        cfg = load_config(env={"watchdog_idle_sec": "120", "pending_max": "128"})
        assert cfg.settings.watchdog_idle_sec == 120.0
        assert cfg.settings.pending_max == 128

    def test_env_settings_clamped(self):
        # 越界钳制不抛异常（02-engine §2 钉制范围）
        cfg = load_config(env={"watchdog_idle_sec": "99999"})
        assert cfg.settings.watchdog_idle_sec == 1800.0

    def test_alias_keys(self):
        cfg = load_config(env={"clarify_suspend_sec": "5",
                               "footer_reasoning_level": "high"})
        assert cfg.settings.clarify_window_sec == 5.0
        assert cfg.theme.reasoning_level == "high"

    def test_degrade_cooldown_scalar_splits_both(self):
        cfg = load_config(env={"degrade_cooldown_sec": "45"})
        assert cfg.settings.degrade_cooldown_rate_limit_sec == 45.0
        assert cfg.settings.degrade_cooldown_other_sec == 45.0

    def test_reserved_renderer_keys_accepted_not_wired(self):
        # §4 渲染保留键：接受但 v1 不接线（诚实标注，不静默吞）
        cfg = load_config(env={"panel_expanded": "false", "print_strategy": "instant"})
        assert cfg.settings is not None  # 不抛异常，键不入引擎设置

    def test_yaml_full_surface(self, tmp_path):
        path = yaml_config(tmp_path, """
feishu:
  app_id: cli_y
  app_secret: sec
  domain: lark
gateway:
  bind: [stdio, http]
  port: 8801
  host: 0.0.0.0
settings:
  watchdog_idle_sec: 240
  clarify_suspend_sec: 2.0
  degrade_cooldown_sec:
    rate_limit: 90
    other: 20
theme:
  reasoning_level: medium
  text_sizes:
    answer: "normal-16"
    aux: "aux-13"
  panel_title_default: "openclaw loop"
log_level: DEBUG
""")
        cfg = load_config(env={}, config_path=path)
        assert cfg.binds == ("stdio", "http")
        assert cfg.port == 8801 and cfg.host == "0.0.0.0"
        assert cfg.log_level == "DEBUG"
        assert cfg.credentials is not None
        assert "larksuite" in cfg.credentials.base_url
        assert cfg.settings.watchdog_idle_sec == 240.0
        assert cfg.settings.clarify_window_sec == 2.0
        assert cfg.settings.degrade_cooldown_rate_limit_sec == 90.0
        assert cfg.settings.degrade_cooldown_other_sec == 20.0
        assert cfg.theme.reasoning_level == "medium"
        assert cfg.theme.panel_title_default == "openclaw loop"
        assert cfg.theme.text_sizes is not None
        assert cfg.theme.text_sizes.answer == "normal-16"

    def test_env_overrides_yaml(self, tmp_path):
        path = yaml_config(tmp_path, """
gateway:
  bind: http
  port: 8802
""")
        cfg = load_config(env={"LSP_GATEWAY_BIND": "stdio", "LSP_GATEWAY_PORT": "9200"},
                          config_path=path)
        assert cfg.binds == ("stdio",)  # env > 文件
        assert cfg.port == 9200


class TestStdioRun:
    """stdio 绑定端到端（注入行源；stderr 收回执行）。"""

    async def test_events_to_acks_and_graceful_shutdown(self, transport):
        from tests.engine.conftest import make_engine, make_settings

        engine, lifecycle, _clock = make_engine(
            transport, settings=make_settings(clarify_window_sec=0.0))
        lines = [
            json.dumps({"v": 1, "event": "turn.start", "frame_id": "f1",
                        "chat_id": "oc_g", "turn_id": "t1", "payload": {}}),
            json.dumps({"v": 1, "event": "answer.snapshot", "frame_id": "f2",
                        "chat_id": "oc_g", "turn_id": "t1", "payload": {"text": "hi"}}),
            json.dumps({"v": 1, "event": "answer.final", "frame_id": "f3",
                        "chat_id": "oc_g", "turn_id": "t1", "payload": {"text": "hi"}}),
        ]

        async def lines_source():
            for ln in lines:
                yield ln

        acks: list[str] = []

        async def emit(text: str) -> None:
            acks.append(text)

        cfg = GatewayConfig(binds=("stdio",))
        code = await run_gateway(cfg, transport=transport, engine=engine,
                                 stdin_lines=lines_source(), emit=emit)
        assert code == 0
        # 逐帧 ack 回写（stderr 通道）
        assert len(acks) == 3
        parsed = [json.loads(a) for a in acks]
        assert all(p["event"] == "frame.ack" and p["payload"]["verdict"] == "consumed"
                   for p in parsed)
        # 建卡 + 关流式（seal 落地）
        assert transport.calls_of("cardkit_create")
        assert transport.calls_of("cardkit_close_streaming")
        # 注入的 engine 归调用方关闭（引擎不代管红线）；关闭即停机通告
        await engine.aclose()
        assert any(ev.type == "shutdown" for ev in lifecycle)

    async def test_bad_frame_error_line_not_ack(self, transport):
        from tests.engine.conftest import make_engine

        engine, _l, _c = make_engine(transport)
        acks: list[str] = []

        async def emit(text: str) -> None:
            acks.append(text)

        async def lines_source():
            yield "not-json"

        code = await run_gateway(GatewayConfig(binds=("stdio",)),
                                 transport=transport, engine=engine,
                                 stdin_lines=lines_source(), emit=emit)
        assert code == 0
        wire = json.loads(acks[0])
        assert wire["event"] == "error"  # 解析失败 → 错误对象，不产生 ack
        assert wire["payload"]["detail"] == "invalid_json"


class TestHttpRun:
    """HTTP 绑定端到端（真 socket；aiohttp TestServer 同源模式）。"""

    async def test_events_health_and_stop(self, transport):
        from tests.engine.conftest import make_engine

        engine, _l, _c = make_engine(transport)
        stop = asyncio.Event()
        cfg = GatewayConfig(binds=("http",), port=7891)
        task = asyncio.create_task(run_gateway(
            cfg, transport=transport, engine=engine, stop_event=stop))
        await asyncio.sleep(0.2)  # 等待监听就绪

        async with aiohttp.ClientSession() as session:
            async with session.post(
                "http://127.0.0.1:7891/v1/events",
                json={"v": 1, "event": "deliver", "frame_id": "d1",
                      "chat_id": "oc_g", "payload": {"text": "hello"}},
            ) as resp:
                assert resp.status == 200
                body = await resp.json()
                assert body["event"] == "frame.ack"
                assert body["payload"]["verdict"] == "consumed"
            async with session.get("http://127.0.0.1:7891/v1/health") as resp:
                assert resp.status == 200
                health = await resp.json()
                assert health["chats_total"] == 1  # engine.snapshot_health 同源

        stop.set()
        code = await asyncio.wait_for(task, timeout=5)
        assert code == 0
        assert transport.calls_of("send_card_to_chat")  # deliver 已落卡

    async def test_missing_credentials_exit_code(self):
        cfg = GatewayConfig(binds=("http",), credentials=None)
        code = await run_gateway(cfg)
        assert code == 2
