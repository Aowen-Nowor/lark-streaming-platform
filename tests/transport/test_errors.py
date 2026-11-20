"""错误分类单测 — 前代语义逐条回归（04-knowledge-freeze Q 系列对应）."""

from __future__ import annotations

import pytest

from lark_streaming.transport.errors import (
    CARDKIT_CARD_SIZE,
    CARDKIT_CONTENT_FAILED,
    CARDKIT_ELEMENT_LIMIT,
    CARDKIT_ELEMENT_LIMIT_DIRECT,
    CARDKIT_ELEMENT_NOT_FOUND,
    CARDKIT_SCHEMA_ERROR,
    CARDKIT_SEQUENCE_CONFLICT,
    CARDKIT_STREAMING_TIMEOUT,
    MSG_DELETED,
    MSG_NOT_FOUND,
    MSG_RECALLED,
    REPLY_ANCHOR_INVALID,
    CardSizeExceeded,
    FeishuAPIError,
    extract_not_found_element_id,
    is_card_size_error,
    is_definitive_reply_anchor_error,
    is_element_limit_error,
    is_element_not_found_error,
    is_rate_limit_error,
    is_schema_error,
    is_streaming_timeout_error,
    is_transient_error,
    parse_api_response,
    sanitize_message,
)


def err(code: int, msg: str = "boom", log_id: str = "log123") -> FeishuAPIError:
    return FeishuAPIError(f"op: code={code}, msg={msg}", code, log_id=log_id)


class TestFeishuAPIError:
    def test_carries_code_and_log_id(self):
        e = err(300317, "seq compare failed")
        assert e.code == CARDKIT_SEQUENCE_CONFLICT
        assert e.log_id == "log123"
        assert "log_id=log123" in str(e)

    def test_sub_code_extraction(self):
        e = err(230099, "ErrCode: 200860 detail")
        assert e.extract_sub_code() == 200860

    def test_sanitize_secret(self):
        msg = "failed with app_secret=abcdef1234567890abc and Bearer tg104abcde12345"
        cleaned = sanitize_message(msg)
        assert "abcdef1234567890abc" not in cleaned
        assert "***" in cleaned

    def test_parse_api_response_ok(self):
        assert parse_api_response({"code": 0, "msg": "success", "data": {"card_id": "c1"}}, "op") == {"card_id": "c1"}

    def test_parse_api_response_err(self):
        with pytest.raises(FeishuAPIError) as exc_info:
            parse_api_response({"code": 300317, "msg": "seq compare failed"}, "op", "lg")
        assert exc_info.value.code == 300317
        assert exc_info.value.log_id == "lg"


class TestTransientClassification:
    """可重试判定——200860 确定性拒收红线 + 230099 子码裁决（OPT-1c）."""

    def test_transient_codes_retryable(self):
        for code in (2200, 1663, 300000, 99991400):
            assert is_transient_error(err(code)), f"code={code} should be transient"

    def test_card_size_never_retryable(self):
        assert is_card_size_error(err(200860))
        assert not is_transient_error(err(200860))

    def test_230099_wrapped_card_size_not_retryable(self):
        e = err(230099, "ErrCode: 200860")
        assert not is_transient_error(e)

    def test_230099_wrapped_element_limit_not_retryable(self):
        e = err(230099, "ErrCode: 11310")
        assert not is_transient_error(e)
        assert is_element_limit_error(e)

    def test_230099_wrapped_other_sub_retryable(self):
        assert is_transient_error(err(230099, "ErrCode: 12345"))

    def test_bare_230099_not_transient(self):
        # 无子码的裸通用码不进重试白名单（保守裁决）
        assert not is_transient_error(err(230099, "no sub code"))

    def test_element_limit_direct(self):
        assert is_element_limit_error(err(300305))
        assert not is_transient_error(err(300305))

    def test_rate_limit_flag(self):
        assert is_rate_limit_error(err(99991400))
        assert not is_rate_limit_error(err(2200))


class TestSpecializedPredicates:
    def test_streaming_timeout(self):
        assert is_streaming_timeout_error(err(200850))
        assert not is_streaming_timeout_error(err(300317))

    def test_element_not_found_variants(self):
        assert is_element_not_found_error(err(300313))
        assert is_element_not_found_error(err(300314))
        # 300315 + "not find elementID" = element not found 变体
        assert is_element_not_found_error(err(300315, "not find elementID : el_1;"))
        # 300315 纯 schema 错误
        assert is_schema_error(err(300315, "unknown property foo"))
        assert not is_schema_error(err(300315, "not find elementID : el_1;"))

    def test_extract_not_found_element_id(self):
        e = err(300315, "ErrMsg: not find elementID : context_loading_hint;")
        assert extract_not_found_element_id(e) == "context_loading_hint"
        # 300313 消息不带 element_id
        assert extract_not_found_element_id(err(300313)) is None


class TestReplyAnchor:
    def test_terminal_codes(self):
        for code in (MSG_NOT_FOUND, REPLY_ANCHOR_INVALID, MSG_RECALLED, MSG_DELETED):
            assert is_definitive_reply_anchor_error(err(code)), f"code={code}"

    def test_unknown_transport_result_is_not_terminal(self):
        # 传输结果未知（网络类/瞬态类）不允许换通道直发
        assert not is_definitive_reply_anchor_error(err(300317))
        assert not is_definitive_reply_anchor_error(err(99991400))


class TestCardSizeExceeded:
    def test_local_precheck_error(self):
        e = CardSizeExceeded(28500, 27000)
        assert e.size_bytes == 28500
        assert e.limit_bytes == 27000
        assert "27000" in str(e)
