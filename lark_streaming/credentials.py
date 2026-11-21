"""飞书凭证 — app_id / app_secret / 站点域名.

env 约定（05-adapters §6）：FEISHU_APP_ID / FEISHU_APP_SECRET / FEISHU_DOMAIN。
凭证只在本层与 token 管理器之间流动，不向渲染/引擎层泄漏。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

FEISHU_BASE = "https://open.feishu.cn/open-apis"
LARK_BASE = "https://open.larksuite.com/open-apis"


@dataclass(frozen=True)
class FeishuCredentials:
    """飞书应用凭证（tenant_access_token 的换取材料）.

    密钥红线（audit #6）：``app_secret`` 设 ``repr=False``——异常日志、
    REPL 倾印、dataclasses.asdict 外的任意 repr 场景不再泄漏明文。
    """

    app_id: str
    app_secret: str = field(repr=False)
    base_url: str = FEISHU_BASE

    def __post_init__(self) -> None:
        if not self.app_id or not isinstance(self.app_id, str):
            raise ValueError("FeishuCredentials: app_id is required")
        if not self.app_secret or not isinstance(self.app_secret, str):
            raise ValueError("FeishuCredentials: app_secret is required")

    @classmethod
    def from_env(cls, *, env: dict[str, str] | None = None) -> "FeishuCredentials":
        """从环境变量构造（FEISHU_DOMAIN=feishu|lark 决定站点）."""
        e = env if env is not None else os.environ
        domain = str(e.get("FEISHU_DOMAIN", "feishu") or "feishu").strip().lower()
        base = LARK_BASE if domain == "lark" else FEISHU_BASE
        return cls(
            app_id=str(e.get("FEISHU_APP_ID", "") or ""),
            app_secret=str(e.get("FEISHU_APP_SECRET", "") or ""),
            base_url=base,
        )

    def is_complete(self) -> bool:
        return bool(self.app_id) and bool(self.app_secret)


__all__ = ["FeishuCredentials", "FEISHU_BASE", "LARK_BASE"]
