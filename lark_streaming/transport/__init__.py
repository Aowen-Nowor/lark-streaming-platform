"""transport 层公开面 — CardKit REST 传输与怪癖知识落地."""

from ..credentials import FeishuCredentials
from .budget import CARD_BYTE_BUDGET, assert_card_size, measure_card_bytes
from .client import FeishuTransport
from .errors import CardSizeExceeded, FeishuAPIError, is_transient_error
from .retry import retry_transient
from .seq import call_with_seq_recovery
from .token import TenantTokenManager

__all__ = [
    "FeishuCredentials", "FeishuTransport", "TenantTokenManager",
    "FeishuAPIError", "CardSizeExceeded",
    "CARD_BYTE_BUDGET", "assert_card_size", "measure_card_bytes",
    "retry_transient", "call_with_seq_recovery", "is_transient_error",
]
