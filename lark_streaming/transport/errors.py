"""飞书 API 错误分类 — 前代生产知识冻结（04-knowledge-freeze Q1-Q10）.

错误码全集与语义全部来自前代 hermes-lark-streaming v2.1.x 生产验证
（`core/feishu/client.py`），逐码迁移，禁止凭记忆新增语义。
"""

from __future__ import annotations

import re
from typing import Any

# ── 错误码常量（04-knowledge-freeze §1 同源）─────────────────────────────
CARDKIT_CONTENT_FAILED = 230099      # 卡片内容创建失败（通用码，需检查 ErrCode 子码）
CARDKIT_ELEMENT_LIMIT = 11310        # 子码：卡片元素数量超限
CARDKIT_ELEMENT_LIMIT_DIRECT = 300305  # 直报码：元素数量超限（batch_update/update）
CARDKIT_SCHEMA_ERROR = 300315        # 卡片 Schema 非法属性 OR insert_before 引用不存在元素
CARDKIT_STREAMING_CLOSED = 300309    # 卡片流式模式已关闭
CARDKIT_SEQUENCE_CONFLICT = 300317   # sequence 冲突（可自愈，见 seq.py）
CARDKIT_STREAMING_TIMEOUT = 200850   # 流式会话超时（官方 ~10min 寿命，见 seq.py）
CARDKIT_CARD_SIZE = 200860           # 卡片体积超限（官方 30KB 上限，确定性拒收）
CARDKIT_CARD_RATE_LIMIT = 200937     # 卡级频控（2026-11-20 L3 真火实测：并发同元素
                                     # 竞速触发，msg="card updated too frequently"；
                                     # 与 99991400 平台 API 级频控分道）
CARDKIT_ELEMENT_NOT_FOUND = 300313   # 元素不存在（add 后服务端未持久化的竞态）
CARDKIT_ELEMENT_NOT_FOUND_ALT = 300314  # delete_elements 不存在的元素
MSG_NOT_FOUND = 1000023              # 消息不存在/已删除
REPLY_ANCHOR_INVALID = 99992354      # reply 目标不是合法 om_ 消息 id
MSG_RECALLED = 230011                # 消息已撤回
MSG_DELETED = 231003                 # 消息已删除

#: reply 锚点确定性失效码集合（仅这些码允许换通道直发——传输结果未知不算）
REPLY_ANCHOR_TERMINAL_CODES = frozenset({
    MSG_NOT_FOUND, REPLY_ANCHOR_INVALID, MSG_RECALLED, MSG_DELETED,
})

#: CardKit 瞬态错误（可自动重试）
CARDKIT_TRANSIENT_CODES = frozenset({
    2200,       # CardKit 内部超时
    1663,       # CardKit 服务端瞬态错误
    300000,     # CardKit 通用内部错误
    99991400,   # 接口频率限制（per-API rate limit，HTTP 400 承载）
    200937,     # 卡级频控 "card updated too frequently"（2026-11-20 L3 真火实测入册）
})

_RATE_LIMIT_CODE = 99991400

_RE_SUB_CODE = re.compile(r"ErrCode:\s*(\d+)")
_RE_ELEMENT_NOT_FOUND_MSG = re.compile(r"not find elementID", re.IGNORECASE)
_RE_NOT_FOUND_ELEMENT_ID = re.compile(
    r"not find elementID\s*[:：]?\s*([A-Za-z0-9_-]+)", re.IGNORECASE
)


def sanitize_message(msg: str) -> str:
    """从错误消息中移除 token 与 secret（日志红线，不可省略）."""
    msg = re.sub(r'(tenant_access_token["\s:=]+)([A-Za-z0-9_-]{10,})', r"\1***", msg)
    msg = re.sub(r'(app_secret["\s:=]+)([A-Za-z0-9]{10,})', r"\1***", msg)
    msg = re.sub(r"(Bearer\s+)([A-Za-z0-9_-]{10,})", r"\1***", msg)
    return msg


class FeishuAPIError(RuntimeError):
    """飞书 API 错误：携带 API 错误码与 log_id（开放平台排查链路 id）."""

    def __init__(self, message: str, code: int = 0, log_id: str = "") -> None:
        super().__init__(sanitize_message(message))
        self.code = code
        self.log_id = log_id

    def __str__(self) -> str:
        base = super().__str__()
        return f"{base} [log_id={self.log_id}]" if self.log_id else base

    def extract_sub_code(self) -> int | None:
        """230099 通用码包裹形态下从消息中提取 ErrCode 子码."""
        m = _RE_SUB_CODE.search(str(self))
        return int(m.group(1)) if m else None


class CardSizeExceeded(RuntimeError):
    """本地字节预算预检失败（03-card-schema §6，27000 bytes）.

    确定性拒收前置到上传前——省一次注定 200860 的往返（OPT-1c 语义）。
    """

    def __init__(self, size_bytes: int, limit_bytes: int) -> None:
        super().__init__(f"card size {size_bytes} exceeds budget {limit_bytes}")
        self.size_bytes = size_bytes
        self.limit_bytes = limit_bytes


# ── 判定谓词（前代逐语义迁移）──────────────────────────────────────────


def is_definitive_reply_anchor_error(exc: BaseException) -> bool:
    """仅锚点确定性失效才允许换通道直发；传输结果未知不属于此类."""
    return getattr(exc, "code", None) in REPLY_ANCHOR_TERMINAL_CODES


def is_element_limit_error(e: BaseException) -> bool:
    """元素数量超限（300305 直报 或 230099+11310 子码包裹）."""
    code = getattr(e, "code", None)
    if code == CARDKIT_ELEMENT_LIMIT_DIRECT:
        return True
    if code == CARDKIT_CONTENT_FAILED and isinstance(e, FeishuAPIError):
        return e.extract_sub_code() == CARDKIT_ELEMENT_LIMIT
    return False


def is_schema_error(e: BaseException) -> bool:
    """300315 且非 "not find elementID"（后者是元素不存在，不是 schema 问题）."""
    if getattr(e, "code", None) != CARDKIT_SCHEMA_ERROR:
        return False
    return not _RE_ELEMENT_NOT_FOUND_MSG.search(str(e))


def is_streaming_timeout_error(e: BaseException) -> bool:
    """200850 流式会话超时（官方恢复动作 = settings streaming_mode:true 重开）."""
    return getattr(e, "code", None) == CARDKIT_STREAMING_TIMEOUT


def is_card_size_error(e: BaseException) -> bool:
    """200860 体积超限——确定性拒收，重试无意义."""
    return getattr(e, "code", None) == CARDKIT_CARD_SIZE


def is_element_not_found_error(e: BaseException) -> bool:
    """300313 / 300314 / 300315+not find elementID（add 后未持久化竞态）."""
    code = getattr(e, "code", None)
    if code in (CARDKIT_ELEMENT_NOT_FOUND, CARDKIT_ELEMENT_NOT_FOUND_ALT):
        return True
    if code == CARDKIT_SCHEMA_ERROR and _RE_ELEMENT_NOT_FOUND_MSG.search(str(e)):
        return True
    return False


def extract_not_found_element_id(e: BaseException) -> str | None:
    """300315 变体消息携带被点名的 element_id（300313/300314 不带，返回 None）."""
    if not is_element_not_found_error(e):
        return None
    m = _RE_NOT_FOUND_ELEMENT_ID.search(str(e))
    return m.group(1) if m else None


def is_transient_error(e: BaseException) -> bool:
    """CardKit 瞬态错误判定（可自动重试）.

    红线语义（OPT-1c）：200860 体积超限显式不可重试（确定性拒收）；
    230099 通用码按 ErrCode 子码裁决——11310（元素超限）/200860（体积超限）
    不可重试，其余可重试。
    """
    if getattr(e, "code", None) == CARDKIT_CARD_SIZE:
        return False
    if getattr(e, "code", None) in CARDKIT_TRANSIENT_CODES:
        return True
    if getattr(e, "code", None) == CARDKIT_CONTENT_FAILED and isinstance(e, FeishuAPIError):
        sub = e.extract_sub_code()
        return sub is not None and sub not in (CARDKIT_ELEMENT_LIMIT, CARDKIT_CARD_SIZE)
    return False


def is_rate_limit_error(e: BaseException) -> bool:
    """99991400 per-API 频率限制（HTTP 400 承载）——退避策略分道."""
    return getattr(e, "code", None) == _RATE_LIMIT_CODE


# ── 响应解析（raw REST 通道）───────────────────────────────────────────


def parse_api_response(payload: Any, operation: str, log_id: str = "") -> Any:
    """解析飞书 OpenAPI JSON 响应；code != 0 抛 FeishuAPIError.

    飞书约定：HTTP 200 承载业务码（code=0 成功）；HTTP 4xx/5xx 时 body
    亦含 code/msg。调用方负责 HTTP 层异常，本函数只裁决业务码。
    """
    if not isinstance(payload, dict):
        raise FeishuAPIError(f"{operation}: non-dict response payload", 0, log_id)
    code = payload.get("code", 0)
    if code == 0:
        return payload.get("data")
    msg = str(payload.get("msg", ""))
    raise FeishuAPIError(
        f"{operation}: code={code}, msg={msg}", int(code), log_id=log_id
    )


__all__ = [
    "FeishuAPIError", "CardSizeExceeded", "sanitize_message",
    "CARDKIT_CONTENT_FAILED", "CARDKIT_ELEMENT_LIMIT", "CARDKIT_ELEMENT_LIMIT_DIRECT",
    "CARDKIT_SCHEMA_ERROR", "CARDKIT_STREAMING_CLOSED", "CARDKIT_SEQUENCE_CONFLICT",
    "CARDKIT_STREAMING_TIMEOUT", "CARDKIT_CARD_SIZE", "CARDKIT_CARD_RATE_LIMIT",
    "CARDKIT_ELEMENT_NOT_FOUND",
    "CARDKIT_ELEMENT_NOT_FOUND_ALT", "MSG_NOT_FOUND", "REPLY_ANCHOR_INVALID",
    "MSG_RECALLED", "MSG_DELETED", "REPLY_ANCHOR_TERMINAL_CODES",
    "CARDKIT_TRANSIENT_CODES",
    "is_definitive_reply_anchor_error", "is_element_limit_error", "is_schema_error",
    "is_streaming_timeout_error", "is_card_size_error", "is_element_not_found_error",
    "extract_not_found_element_id", "is_transient_error", "is_rate_limit_error",
    "parse_api_response",
]
