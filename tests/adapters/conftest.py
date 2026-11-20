"""adapters 测试共享台架 — 录制式假引擎 + 前代真实帧样本（02-engine §10 L1）.

映射器/适配器单测不依赖真实网络（L1 纪律）；与真实引擎的端到端
集成用例复用 ``tests/engine/conftest.py`` 的 FakeTransport。
"""

from __future__ import annotations

from typing import Any, Callable

import pytest

from lark_streaming.engine import Ack, Verdict


class FakeEngine:
    """录制式假引擎：信封录制 + 裁决脚本（可注入逐帧 verdict 序列）.

    ``script`` 可以是 verdict 序列（按提交顺序弹出，耗尽后恒 consumed）
    或 ``Callable[[dict], Verdict]``（按信封裁决）。
    """

    def __init__(
        self,
        script: "list[Verdict] | Callable[[dict[str, Any]], Verdict] | None" = None,
    ) -> None:
        self.submitted: list[dict[str, Any]] = []
        self._queue: list[Verdict] = []
        self._fn: Callable[[dict[str, Any]], Verdict] | None = None
        if callable(script):
            self._fn = script
        elif script:
            self._queue = list(script)

    async def submit(self, raw: dict[str, Any]) -> Ack:
        self.submitted.append(raw)
        if self._fn is not None:
            verdict = self._fn(raw)
        elif self._queue:
            verdict = self._queue.pop(0)
        else:
            verdict = Verdict.CONSUMED
        return Ack(raw.get("frame_id", ""), verdict, detail=f"fake_{verdict.value}")

    def events(self) -> list[str]:
        return [env.get("event", "") for env in self.submitted]


@pytest.fixture
def fake_engine() -> FakeEngine:
    return FakeEngine()


@pytest.fixture
def transport():
    """引擎集成用假传输（tests/engine/conftest.FakeTransport 跨目录复用）."""
    from tests.engine.conftest import FakeTransport

    return FakeTransport()


# ── 前代真实帧样本（旧仓库 tests/e2e/test_v206_real_formats.py 移植）────────
REAL_TERMINAL_BLOCK = '💻 terminal\n```\necho "=== 时间 ==="; date "+%Y-%m-%d %H:%M:%S"\n```'
REAL_TERMINAL_BLOCK_TRUNC = '💻 terminal\n```\nvery_long_command --flag value ...\n```'
REAL_BARE_BLOCK = '```\nsecond_command --now\n```'
REAL_VERB_ZH_SEARCH = "🔍 搜索网页 · feishu cardkit 流式"
REAL_VERB_ZH_READ = "📖 读取文件 /tmp/status.py"
REAL_VERB_ZH_NO_PREVIEW = "📚 列出技能"
REAL_VERB_EN_RUN = "💻 Running uname -r"
REAL_VERB_EN_SEARCH = "🔍 Searching the web for python 3.12"
REAL_PENDING = "💻 terminal..."
REAL_PREVIEW = '💻 terminal："echo \\"1\\""'
REAL_HEAD = "💻 terminal"
REAL_THINK = "💬 正在检查系统状态，先收集内核与运行时长…"
REAL_DEDUP_BLOCK = REAL_TERMINAL_BLOCK + " (×2)"

__all__ = ["FakeEngine"]
