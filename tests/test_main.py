"""``python -m lark_streaming`` CLI 入口回归（audit #9-L6：覆盖率 0% → 参数/配置组合）.

覆盖面：默认加载 CWD ``lsp.yaml``（audit #5 告警）、``--bind/--port`` env
传递、凭证缺失退出码、错误绑定名过滤。
"""

from __future__ import annotations

import logging

import pytest

from lark_streaming import __main__ as cli
from lark_streaming.gateway import GatewayConfig


def test_main_without_credentials_exits_2():
    """stdio-only 无凭证 → 网关装配层拒绝启动（退出码 2）。"""
    rc = cli.main(["--bind", "stdio"])
    assert rc == 2


def test_main_default_lsp_yaml_autoload_warns(tmp_path, monkeypatch, caplog):
    """CWD 存在 lsp.yaml → 自动加载 + 明文 secret 风险告警（audit #5）。"""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "lsp.yaml").write_text(
        "gateway:\n  bind: stdio\n", encoding="utf-8"
    )
    captured: dict = {}

    async def fake_run(config, **kw):
        captured["config"] = config
        return 0

    monkeypatch.setattr(cli, "run_gateway", fake_run)
    with caplog.at_level(logging.WARNING, logger="lark_streaming.__main__"):
        rc = cli.main([])
    assert rc == 0
    assert isinstance(captured["config"], GatewayConfig)
    assert captured["config"].binds == ("stdio",)
    assert any("lsp.yaml" in rec.message for rec in caplog.records)


def test_main_bind_and_port_flow_into_config(monkeypatch):
    """--bind/--port 经 env 面（LSP_GATEWAY_BIND/PORT）进 GatewayConfig."""
    captured: dict = {}

    async def fake_run(config, **kw):
        captured["config"] = config
        return 0

    monkeypatch.setattr(cli, "run_gateway", fake_run)
    rc = cli.main(["--bind", "stdio", "--port", "7901"])
    assert rc == 0
    assert captured["config"].binds == ("stdio",)
    assert captured["config"].port == 7901


def test_main_invalid_bind_falls_back_to_stdio(monkeypatch):
    """非法绑定名被 VALID_BINDS 过滤 → 回落 stdio（配置面宽容不阻断）。"""
    captured: dict = {}

    async def fake_run(config, **kw):
        captured["config"] = config
        return 0

    monkeypatch.setattr(cli, "run_gateway", fake_run)
    rc = cli.main(["--bind", "carrier-pigeon"])
    assert rc == 0
    assert captured["config"].binds == ("stdio",)
