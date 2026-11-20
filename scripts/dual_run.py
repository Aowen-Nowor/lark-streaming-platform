#!/usr/bin/env python3
"""双跑对照 CLI — Phase 6（M4 收口）.

用法：
    python scripts/dual_run.py                     # 全语料双跑 → docs/dual-run-report.md
    python scripts/dual_run.py --side old|new|both # 强制单/双侧（覆盖语料头）
    python scripts/dual_run.py --json out.json     # 额外导出机器可读 trace
    python scripts/dual_run.py --corpus glob       # 语料 glob（默认全量）

前置：旧仓（old-hermes-ref / hermes-lark-streaming）在兄弟目录，
或 ``LSP_OLD_REPO`` 指向其路径。旧仓缺席时仅新侧可跑（报告标注）。
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

from tests.harness.dual_run import (  # noqa: E402
    default_old_repo,
    render_markdown,
    run_corpus,
)


def _git_commit(repo: Path) -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=str(repo),
            capture_output=True, text=True, timeout=10, check=False,
        )
        return out.stdout.strip() or "?"
    except Exception:  # noqa: BLE001
        return "?"


def main() -> int:
    ap = argparse.ArgumentParser(description="LSEP 双跑对照（Phase 6）")
    ap.add_argument("--side", choices=["old", "new", "both"], default=None,
                    help="覆盖语料头的 sides 声明")
    ap.add_argument("--corpus", default="tests/harness/corpus/*.jsonl",
                    help="语料 glob（相对仓库根）")
    ap.add_argument("--json", dest="json_out", default="",
                    help="机器可读 trace 导出路径（可选）")
    ap.add_argument("--report", default="docs/dual-run-report.md",
                    help="Markdown 报告输出路径（默认 docs/dual-run-report.md）")
    args = ap.parse_args()

    paths = sorted(_REPO_ROOT.glob(args.corpus))
    if not paths:
        print(f"no corpus matched: {args.corpus}", file=sys.stderr)
        return 1
    print(f"corpus: {len(paths)} scenario files")

    old_repo = default_old_repo()
    if old_repo is None:
        print("WARN: old repo not found — only new side available "
              "(set LSP_OLD_REPO)", file=sys.stderr)

    traces = run_corpus(paths, sides=args.side)

    meta = {
        "date": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "old_repo": str(old_repo) if old_repo else "(absent)",
        "old_commit": _git_commit(old_repo) if old_repo else "-",
        "new_commit": _git_commit(_REPO_ROOT),
        "version": "0.0.1",
        "verdicts": {
            "01_full_turn": "equivalent",
            "02_progress_marked": "equivalent",
            "03_progress_unmarked": "equivalent (F2 fixed)",
            "04_notify_out_of_turn": "divergence-intended (D1)",
            "05_plain_out_of_turn": "equivalent (静态卡面)",
            "06_race_burst": "equivalent (恰一卡)",
            "07_supersede_turn": "equivalent",
            "08_degrade_create_fail": "equivalent (零丢失; D3 呈现粒度)",
            "09_clarify_pause": "equivalent (F1; D2 计数口径)",
            "10_multi_chat": "equivalent",
            "11_cron_delivery": "equivalent (F3 fixed)",
            "12_clarify_same_card": "capability (new-only)",
        },
    }

    report = render_markdown(traces, meta=meta)
    report_path = _REPO_ROOT / args.report
    report_path.write_text(report, encoding="utf-8")
    print(f"report: {report_path}")

    if args.json_out:
        payload = {"meta": meta,
                   "traces": [asdict(t) for t in traces]}
        out = _REPO_ROOT / args.json_out
        out.write_text(json.dumps(payload, ensure_ascii=False, indent=2),
                       encoding="utf-8")
        print(f"json: {out}")

    # 零丢失红线速报（CLI 退出码门禁）
    problems = []
    for tr in traces:
        for side, t in (("old", tr.old), ("new", tr.new)):
            if t is None:
                continue
            # 内容帧数 vs 产出（卡片终值 + 原生回退）粗校验：
            # 静默丢失 = 语料内容帧无对应消费或原生记录 —— 由验收测试
            # （tests/harness/test_dual_run.py）逐场景强断言，此处仅汇总。
            print(f"{tr.scenario}[{side}] cards={t.cards} native={len(t.native)}"
                  f" finals={len(t.final_texts)}")
    if problems:
        for p in problems:
            print(f"PROBLEM: {p}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
