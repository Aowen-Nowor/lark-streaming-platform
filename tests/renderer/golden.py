"""Golden 快照断言（03-card-schema §8 契约）.

渲染变更必须显式更新快照并在 CHANGELOG 说明。快照文件不存在时首跑
写入（生成后即冻结——二跑起不匹配即失败）。
"""

from __future__ import annotations

import json
from pathlib import Path

GOLDEN_DIR = Path(__file__).parent / "golden"


def assert_golden(name: str, payload: object) -> None:
    """Golden 快照断言：文件存在则规范化对比，缺失则首跑写入."""
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    path = GOLDEN_DIR / f"{name}.json"
    normalized = json.loads(json.dumps(payload, ensure_ascii=False, sort_keys=True))
    rendered = json.dumps(normalized, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if path.exists():
        expected = path.read_text(encoding="utf-8")
        assert rendered == expected, (
            f"golden snapshot mismatch: {name}.json — "
            "rendering changed; update the snapshot explicitly (03-card-schema §8)"
        )
    else:
        path.write_text(rendered, encoding="utf-8")
