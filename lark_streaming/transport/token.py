"""tenant_access_token 管理 — 惰性获取、提前刷新、并发单飞.

前代依赖 lark-oapi SDK 的 token 自动管理；新框架 raw REST 直管：
- 官方端点 POST {base}/auth/v3/tenant_access_token/internal
- 有效期 expire（约 7200s），提前 300s 刷新（网络裕度）
- 并发刷新单飞：首个调用者刷新，其余等待复用
- 刷新失败可重试（网络错误），不吞 CancelledError
"""

from __future__ import annotations

import asyncio
import logging
import time

import httpx

from ..credentials import FeishuCredentials
from .errors import FeishuAPIError, sanitize_message

_logger = logging.getLogger("lark_streaming.transport.token")

#: 提前刷新裕度（秒）——到期前 300s 即视为需要刷新
_REFRESH_MARGIN_SEC = 300.0
_TOKEN_URL_PATH = "/auth/v3/tenant_access_token/internal"


class TenantTokenManager:
    """tenant_access_token 惰性缓存管理器（async 安全）."""

    def __init__(
        self,
        credentials: FeishuCredentials,
        *,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        self._credentials = credentials
        self._own_http = http is None
        self._http = http or httpx.AsyncClient(timeout=15.0)
        self._token: str = ""
        self._expires_at: float = 0.0  # monotonic
        self._refresh_lock = asyncio.Lock()

    async def aclose(self) -> None:
        if self._own_http:
            await self._http.aclose()

    async def get_token(self) -> str:
        """返回有效 token；临期或缺失时刷新（并发单飞）."""
        if self._token and time.monotonic() < self._expires_at:
            return self._token
        async with self._refresh_lock:
            # 双检：等待期间其他协程已完成刷新
            if self._token and time.monotonic() < self._expires_at:
                return self._token
            await self._refresh()
            return self._token

    async def invalidate(self) -> None:
        """令牌失效（401/400 权限类错误后强制刷新）."""
        async with self._refresh_lock:
            self._token = ""
            self._expires_at = 0.0

    async def _refresh(self) -> None:
        url = self._credentials.base_url + _TOKEN_URL_PATH
        payload = {
            "app_id": self._credentials.app_id,
            "app_secret": self._credentials.app_secret,
        }
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                resp = await self._http.post(url, json=payload)
                try:
                    body = resp.json()
                except ValueError:
                    # audit #9-L3：非 JSON 响应（502 网关错误页等）按网络
                    # 错误重试——与 client._request 同款防护，不裸抛
                    last_error = FeishuAPIError(
                        f"tenant_access_token: non-json response "
                        f"(http {resp.status_code})"
                    )
                    if attempt < 2:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    raise last_error
                raw_code = body.get("code", -1)
                try:
                    code = int(raw_code)
                except (TypeError, ValueError):
                    # 同 L2 精神：畸形响应按可重试异常处理（代理/中间件产物）
                    last_error = FeishuAPIError(
                        f"tenant_access_token: malformed code {str(raw_code)[:32]}"
                    )
                    if attempt < 2:
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    raise last_error
                if code != 0:
                    msg = sanitize_message(str(body.get("msg", "")))
                    # 凭证错误（无有效 token 返回）不可重试
                    raise FeishuAPIError(
                        f"tenant_access_token: code={code}, msg={msg}", code
                    )
                token = str(body.get("tenant_access_token", "") or "")
                try:
                    expire = int(body.get("expire", 0) or 0)
                except (TypeError, ValueError):
                    expire = 0  # 畸形 expire → 走下方 empty token/expire 拒绝
                if not token or expire <= 0:
                    raise FeishuAPIError("tenant_access_token: empty token/expire")
                self._token = token
                self._expires_at = time.monotonic() + max(expire - _REFRESH_MARGIN_SEC, 60.0)
                _logger.debug("token refreshed, expire_in=%ds", expire)
                return
            except FeishuAPIError:
                raise  # 业务码错误不重试
            except (httpx.RequestError, httpx.TimeoutException) as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
        raise FeishuAPIError(
            f"tenant_access_token: network failure after retries: {last_error!r}"
        )


__all__ = ["TenantTokenManager", "_REFRESH_MARGIN_SEC"]
