"""gateway 测试共享台架 — 引擎集成用假传输（跨目录复用）."""

from __future__ import annotations

import pytest


@pytest.fixture
def transport():
    from tests.engine.conftest import FakeTransport

    return FakeTransport()
