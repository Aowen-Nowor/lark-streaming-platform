"""渲染层测试共享 fixture."""

from __future__ import annotations

import pytest

from lark_streaming.renderer import TextSizes, Theme


@pytest.fixture
def theme() -> Theme:
    """核心层中性默认主题."""
    return Theme()


@pytest.fixture
def sized_theme() -> Theme:
    """启用自定义字号的 opt-in 主题（v2.0.20 token 路径覆盖）."""
    return Theme(text_sizes=TextSizes(answer="cus-ans-value", aux="cus-aux-value"))
