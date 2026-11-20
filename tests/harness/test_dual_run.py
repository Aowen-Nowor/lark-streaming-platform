"""双跑对照验收 — Phase 6（M4 收口：同语料旧/新双跑行为对照）.

对照判据（发现项三分类，见 docs/dual-run-report.md）：
- ``equivalent``：卡片数 / 终值保真 / 原生回退逐项一致；
- ``divergence-intended``：设计内分歧（微回合形态、澄清独立卡计数口径）；
- ``gap``：发现项已修复（F1/F2/F3），本套用例固化修复后的对齐行为。

旧仓缺席时 skip（本地/运维门禁；对照报告作为证据提交，非 CI 门禁）。
"""

from __future__ import annotations

from pathlib import Path

import pytest

from tests.harness.dual_run import (
    load_corpus,
    old_plugin_available,
    run_scenario,
)

pytestmark = pytest.mark.skipif(
    not old_plugin_available(),
    reason="old hermes repo not available (set LSP_OLD_REPO)",
)

_CORPUS = Path(__file__).parent / "corpus"


def _spec(name: str) -> dict:
    return load_corpus(_CORPUS / f"{name}.jsonl")[0]


async def _run(name: str, sides: str | None = None):
    return await run_scenario(_spec(name), sides=sides)


# ── equivalent 族：全回合 / 进度 / 竞态 / 隔离 ───────────────────────────────


async def test_01_full_turn_equivalent():
    tr = await _run("01_full_turn")
    c = tr.comparison
    assert c["cards_old"] == c["cards_new"] == 1
    assert c["native_old"] == [] and c["native_new"] == []
    final = "内核时钟源为 tsc，无漂移记录。"
    assert any(final in t for t in tr.old.final_texts)
    assert any(final in t for t in tr.new.final_texts)


async def test_02_progress_marked_equivalent():
    tr = await _run("02_progress_marked")
    c = tr.comparison
    assert c["cards_old"] == c["cards_new"] == 1
    assert c["native_old"] == [] and c["native_new"] == []
    # 标记进度行进面板：两侧面板行数一致（_send_progress 语义对齐）
    assert "运行 12 天无重启" in tr.old.final_texts[0]
    assert "运行 12 天无重启" in tr.new.final_texts[0]


async def test_03_unmarked_progress_f2_fixed():
    """F2 修复验收：无标记进度行升格进面板——新侧不再投散卡."""
    tr = await _run("03_progress_unmarked")
    c = tr.comparison
    assert c["cards_old"] == c["cards_new"] == 1
    assert c["native_old"] == [] and c["native_new"] == []
    # 新侧 send 计数 = 1（仅回合卡发表；修复前为 3 = 2 散卡 + 回合卡）
    assert c["ops_new"].get("send") == 1
    final = "无异常挂载点"
    assert any(final in t for t in tr.new.final_texts)


async def test_06_race_burst_single_card():
    """10-12 事故形态：建卡窗口竞态双跑——两侧都必须恰建 1 卡."""
    tr = await _run("06_race_burst")
    c = tr.comparison
    assert c["cards_old"] == c["cards_new"] == 1
    assert c["native_old"] == [] and c["native_new"] == []
    assert "新版本 v2.3.1 已生效" in tr.old.final_texts[0]
    assert "新版本 v2.3.1 已生效" in tr.new.final_texts[0]


async def test_07_supersede_turn_two_cards():
    """同 turn_id 幂等续流 + 新回合换卡：两侧 2 卡 2 终值."""
    tr = await _run("07_supersede_turn")
    c = tr.comparison
    assert c["cards_old"] == c["cards_new"] == 2
    assert c["native_old"] == [] and c["native_new"] == []
    assert len(tr.old.final_texts) == 2 and len(tr.new.final_texts) == 2
    assert any("结论与第一问一致" in t for t in tr.old.final_texts)
    assert any("结论与第一问一致" in t for t in tr.new.final_texts)


async def test_10_multi_chat_isolation():
    """双 chat 交错：隔离面对照（per-chat 卡片 / 终值互不串卡）."""
    tr = await _run("10_multi_chat")
    c = tr.comparison
    assert c["cards_old"] == c["cards_new"] == 2
    assert c["native_old"] == [] and c["native_new"] == []
    assert any("延迟 12ms" in t for t in tr.old.final_texts)
    assert any("延迟 18ms" in t for t in tr.new.final_texts)


# ── divergence-intended 族：形态分歧（零丢失等价）────────────────────────────


async def test_04_notify_out_of_turn_divergence_intended():
    """回合外 notify：旧 R2 oneshot 静态卡 vs 新微回合卡（设计内分歧）.

    对照面：零丢失等价（双侧零原生回退、内容必达）；形态分歧注设计
    出处（02-engine 微回合自愈，05-adapters §5.1 notify 映射）。
    """
    tr = await _run("04_notify_out_of_turn")
    c = tr.comparison
    assert c["native_old"] == [] and c["native_new"] == []
    # 旧：oneshot IM 直投（非 CardKit 实体，cards=0）；新：微回合实体卡
    assert c["cards_old"] == 0 and c["cards_new"] == 1
    assert c["ops_old"].get("send") == 1
    assert c["ops_new"].get("create") == 1


async def test_05_plain_out_of_turn_both_card():
    """无标记普通文本（无活跃卡）：旧 R3 longform 卡 vs 新 deliver 静态卡."""
    tr = await _run("05_plain_out_of_turn")
    c = tr.comparison
    assert c["native_old"] == [] and c["native_new"] == []
    assert c["ops_old"].get("reply") == 1     # 旧：reply 发表（om_ 锚）
    assert c["ops_new"].get("send") == 1      # 新：deliver 静态卡直投


async def test_09_clarify_pause_two_turn_cards():
    """澄清边界双跑（edit-transport 真实流）：旧/新同构 2 张回合卡.

    定稿后续流以新 expect_edits 发送（旧网关真实形态）——旧侧新建卡，
    新侧 supersede 路径（暂停卡 seal 真落定不丢内容）。澄清独立卡为
    设计形态：旧 IM 内联卡不计 cards / 新 CardKit 实体计入 → 2/3 计数
    口径差，非行为分歧。全部内容双侧必达（零原生回退）。
    """
    tr = await _run("09_clarify_pause")
    c = tr.comparison
    assert c["native_old"] == [] and c["native_new"] == []
    assert c["cards_old"] == 2 and c["cards_new"] == 3  # +澄清独立卡（计数口径）
    assert any("需要确认目标环境" in t for t in tr.old.final_texts)
    assert any("需要确认目标环境" in t for t in tr.new.final_texts)
    assert any("预计 40 分钟" in t for t in tr.old.final_texts)
    assert any("预计 40 分钟" in t for t in tr.new.final_texts)


async def test_12_clarify_same_card_new_capability():
    """同卡澄清续流（协议增强，仅新侧）：暂停 → 回答 → 同卡续流真终值.

    旧 edit-transport 结构性无法续写已定稿卡（对照基线 09 = 2 张回合
    卡）；新架构宿主携 turn_id 即同卡续流（v2.0.26 用户验收语义在协议
    层达成）。
    """
    tr = await _run("12_clarify_same_card")
    assert tr.old is None  # 单侧场景
    c = tr.comparison
    assert c["status"] == "single-side"
    assert tr.new.cards == 2  # 回合卡 + 澄清独立卡
    assert tr.new.native == []
    assert len(tr.new.final_texts) == 1  # 恰一张回合卡
    assert "预计 40 分钟" in tr.new.final_texts[0]


async def test_11_cron_delivery_f3_fixed():
    """F3 修复验收：cron 家族 → deliver 静态卡 + footer.job_id 溯源."""
    tr = await _run("11_cron_delivery")
    c = tr.comparison
    assert c["native_old"] == [] and c["native_new"] == []
    assert c["ops_old"].get("send") == 1
    assert c["ops_new"].get("send") == 1
    assert c["ops_new"].get("create", 0) == 0  # 静态卡（非 CardKit 实体）


# ── 降级红线族：零丢失 ───────────────────────────────────────────────────────


async def test_08_degrade_zero_loss_both_sides():
    """create 确定性拒收：两回合 4 帧全部原生履行，零丢失，零卡片."""
    tr = await _run("08_degrade_create_fail")
    c = tr.comparison
    assert c["cards_old"] == c["cards_new"] == 0
    # 旧：4 帧原生（composer_none / orphan_edit_native）
    assert len(c["native_old"]) == 4
    # 新：4 帧原生（create_failed + 冷却快路径 chat_degraded ×3）
    assert len(c["native_new"]) == 4
    assert c["native_new"][0] == "create_failed:200860"
    assert set(c["native_new"][1:]) == {"chat_degraded"}
    # 零丢失红线：两侧均无静默丢弃（每帧都有 native 记录）
    assert len(c["native_old"]) == len(c["native_new"]) == 4
