"""seq 自愈单测 — 300317 对齐重发 / 200850 重开会话 / 上限外上抛.

语义来源：前代 seq.py + 真飞书实验 s98 定案（04-knowledge-freeze Q1/Q2）。
"""

from __future__ import annotations

import pytest

from lark_streaming.transport.errors import FeishuAPIError
from lark_streaming.transport.seq import (
    MAX_SEQ_RECOVERIES,
    call_with_seq_recovery,
    is_seq_conflict,
    is_stream_timeout,
    mark_session_renewed,
)


class FakeState:
    def __init__(self, seq: int = 0) -> None:
        self.seq = seq
        self.stream_session_at = 0.0


def conflict() -> FeishuAPIError:
    return FeishuAPIError("op: code=300317, msg=sequence compare failed", 300317)


def stream_timeout() -> FeishuAPIError:
    return FeishuAPIError("op: code=200850, msg=card streaming timeout", 200850)


class TestSeqConflict:
    async def test_first_try_ok(self):
        st = FakeState(seq=5)
        sent = []

        async def send(seq):
            sent.append(seq)

        await call_with_seq_recovery(st, send, what="test")
        assert sent == [5]
        assert st.seq == 5

    async def test_300317_aligns_and_resends(self):
        """300317 → 本地 seq+1 对齐服务端 → 重发同内容（s98 定案）."""
        st = FakeState(seq=10)
        sent = []

        async def send(seq):
            sent.append(seq)
            if seq == 10:
                raise conflict()

        await call_with_seq_recovery(st, send, what="test")
        assert sent == [10, 11]  # 同内容重发由调用方闭包承接（全量语义幂等）
        assert st.seq == 11

    async def test_multiple_conflicts_within_limit(self):
        st = FakeState(seq=0)
        sent = []

        async def send(seq):
            sent.append(seq)
            if seq < 2:
                raise conflict()

        await call_with_seq_recovery(st, send, what="test")
        assert sent == [0, 1, 2]
        assert st.seq == 2

    async def test_conflict_beyond_limit_raises(self):
        """上限语义：末次尝试直接上抛、不再 +1（末次冲突未自愈，交降级红线）."""
        st = FakeState(seq=0)

        async def send(seq):
            raise conflict()  # 每次都撞

        with pytest.raises(FeishuAPIError):
            await call_with_seq_recovery(st, send, what="test")
        assert st.seq == MAX_SEQ_RECOVERIES - 1  # 末次上抛不消耗 seq


class TestStreamTimeout:
    async def test_200850_reopens_and_resends(self):
        st = FakeState(seq=7)
        sent, reopened = [], []

        async def send(seq):
            sent.append(seq)
            if seq == 7:
                raise stream_timeout()

        async def reopen(seq):
            reopened.append(seq)

        await call_with_seq_recovery(st, send, what="test", reopen=reopen)
        assert sent == [7, 9]           # seq+1 reopen → seq+1 重发（s98 跳号允许）
        assert reopened == [8]
        assert st.seq == 9
        assert st.stream_session_at > 0  # 重开成功刷新续期计时基线

    async def test_200850_without_reopen_raises(self):
        st = FakeState(seq=1)

        async def send(seq):
            raise stream_timeout()

        with pytest.raises(FeishuAPIError):
            await call_with_seq_recovery(st, send, what="test")

    async def test_reopen_failure_propagates(self):
        st = FakeState(seq=1)

        async def send(seq):
            raise stream_timeout()

        async def reopen(seq):
            raise FeishuAPIError("reopen: code=500, msg=boom", 500)

        with pytest.raises(FeishuAPIError):
            await call_with_seq_recovery(st, send, what="test", reopen=reopen)


class TestMisc:
    async def test_unrelated_error_propagates(self):
        st = FakeState(seq=1)

        async def send(seq):
            raise FeishuAPIError("op: code=300315, msg=schema", 300315)

        with pytest.raises(FeishuAPIError):
            await call_with_seq_recovery(st, send, what="test")
        assert st.seq == 1  # 未自愈不改 seq

    async def test_predicates(self):
        assert is_seq_conflict(conflict())
        assert is_stream_timeout(stream_timeout())
        assert not is_seq_conflict(stream_timeout())
        assert not is_stream_timeout(conflict())

    def test_mark_session_renewed_tolerates_bad_state(self):
        mark_session_renewed(object())  # 无字段对象 → 静默跳过
