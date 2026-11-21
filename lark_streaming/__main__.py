"""``python -m lark_streaming`` — 网关 shell 入口（形态 B 常驻进程）.

用法::

    python -m lark_streaming                     # 默认 stdio 绑定
    python -m lark_streaming --bind stdio,http --port 7800
    python -m lark_streaming --config lsp.yaml

env 面（05-adapters §6）见 :mod:`lark_streaming.gateway`。
"""

from __future__ import annotations

import argparse
import asyncio
import dataclasses
import logging
import os
import sys
from pathlib import Path

from .gateway import load_config, run_gateway

_logger = logging.getLogger("lark_streaming.__main__")

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="lark_streaming",
        description="LSEP/1 网关 shell — 飞书流式卡片平台（形态 B 常驻进程）",
    )
    parser.add_argument(
        "--config", default=None,
        help="配置文件路径（默认：CWD 下 lsp.yaml 存在则加载）",
    )
    parser.add_argument(
        "--bind", default=None,
        help="绑定集：stdio|http|ws（可逗号多值；覆盖配置文件/env）",
    )
    parser.add_argument("--port", type=int, default=None, help="HTTP/WS 端口")
    parser.add_argument("--host", default=None, help="HTTP/WS 监听地址")
    args = parser.parse_args(argv)

    config_path = args.config
    if config_path is None and Path("lsp.yaml").is_file():
        config_path = "lsp.yaml"
    if config_path:
        _logger.warning(
            "gateway: config file %s in use — file-sourced app_secret is a "
            "plaintext-on-disk secret (audit #5); prefer env injection "
            "(FEISHU_APP_SECRET) and keep lsp.yaml out of VCS", config_path,
        )
    env = dict(os.environ)
    if args.bind:
        env["LSP_GATEWAY_BIND"] = args.bind
    if args.port is not None:
        env["LSP_GATEWAY_PORT"] = str(args.port)

    config = load_config(env=env, config_path=config_path)
    if args.host:
        config = dataclasses.replace(config, host=args.host)

    return asyncio.run(run_gateway(config))


if __name__ == "__main__":
    sys.exit(main())
