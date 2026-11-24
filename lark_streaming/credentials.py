"""飞书凭证 — app_id / app_secret / 站点域名 / 回调鉴权材料.

env 约定（05-adapters §6）：FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_DOMAIN。
凭证只在本层与 token 管理器之间流动，不向渲染/引擎层泄漏。

回调鉴权材料（01-protocol §3.2 feishu-callback 绑定，独立鉴权域——飞书
回调无法携带 Bearer）：FEISHU_VERIFICATION_TOKEN / FEISHU_ENCRYPT_KEY。
两者仅在协议层回调验签时使用，与 app_secret 同等密钥纪律。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

FEISHU_BASE = "https://open.feishu.cn/open-apis"
LARK_BASE = "https://open.larksuite.com/open-apis"


@dataclass(frozen=True)
class FeishuCredentials:
    """飞书应用凭证（tenant_access_token 的换取材料 + 回调验签材料）.

    密钥红线（audit #6）：``app_secret`` 设 ``repr=False``——异常日志、
    REPL 倾印、dataclasses.asdict 外的任意 repr 场景不再泄漏明文。
    回调鉴权材料同纪律：``verification_token`` / ``encrypt_key`` 均
    ``repr=False``（feishu-callback 绑定引入，01-protocol §3.2）。
    """

    app_id: str
    app_secret: str = field(repr=False)
    base_url: str = FEISHU_BASE
    verification_token: str = field(default="", repr=False)
    encrypt_key: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        if not self.app_id or not isinstance(self.app_id, str):
            raise ValueError("FeishuCredentials: app_id is required")
        if not self.app_secret or not isinstance(self.app_secret, str):
            raise ValueError("FeishuCredentials: app_secret is required")

    @classmethod
    def from_env(cls, *, env: dict[str, str] | None = None) -> "FeishuCredentials":
        """从环境变量构造（FEISHU_DOMAIN=feishu|lark 决定站点）.

        回调鉴权材料可选：FEISHU_VERIFICATION_TOKEN / FEISHU_ENCRYPT_KEY
        未设置时为空串——feishu-callback 绑定未配置 token 时 fail-closed
        （路由恒 503，01-protocol §3.2）。
        """
        e = env if env is not None else os.environ
        domain = str(e.get("FEISHU_DOMAIN", "feishu") or "feishu").strip().lower()
        base = LARK_BASE if domain == "lark" else FEISHU_BASE
        return cls(
            app_id=str(e.get("FEISHU_APP_ID", "") or ""),
            app_secret=str(e.get("FEISHU_APP_SECRET", "") or ""),
            base_url=base,
            verification_token=str(e.get("FEISHU_VERIFICATION_TOKEN", "") or ""),
            encrypt_key=str(e.get("FEISHU_ENCRYPT_KEY", "") or ""),
        )

    def is_complete(self) -> bool:
        return bool(self.app_id) and bool(self.app_secret)


__all__ = ["FeishuCredentials", "FEISHU_BASE", "LARK_BASE"]
