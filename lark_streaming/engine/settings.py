"""引擎设置 — 定时器与预算冻结常量（02-engine §2，钉制范围强制钳制）.

默认值全部来自前代生产实证（04-knowledge-freeze），禁止凭记忆调整：
- 看门狗 300s：11-08 16:05 网关压缩静默被 120s 旧值误封 → v2.0.32 提至 300
- 会话续期 540s 龄：11-07 两例 200850 撞 600s 会话寿命 → 提前 60s 续期
- 面板合并 0.2s：499s 回合 35 次工具 ≈70 次全量上传 ≈1.3MB（OPT-1a）
- 澄清窗口 1.2s：前代 clarify_suspend_sec 生产值（边界 finalize →
  send_clarify 实测间隔 <200ms，裕度充分）
- 卡 TTL 600s：前代 card_duration_sec
- 降级冷却：429 → 60s（前代 429 冷却）；其他 → 30s

配置异常防御：越界值一律钳制进钉制范围（02-engine §2 括号区间），
不抛异常——引擎配置错误不能阻止消息投递主链。
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = ["EngineSettings"]


def _clamp(value: float, lo: float, hi: float, default: float) -> float:
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    if v != v:  # NaN
        return default
    return min(max(v, lo), hi)


@dataclass(frozen=True)
class EngineSettings:
    """编排层全部可调参数（02-engine §2 定时器表 + §3 并发参数）."""

    # 看门狗 T_idle：STREAMING 静默超时 → seal（跑马灯永挂根治）
    watchdog_idle_sec: float = 300.0        # 钉制 60~1800
    # 会话续期 T_renew：流式会话龄 ≥ 该值 → 下一帧更新前先续期
    session_renew_age_sec: float = 540.0    # 钉制 60~590（会话寿命 ~600s 内）
    # 面板尾沿合并 T_flush：原地改写类更新合并窗口；结构性新增行不合并
    panel_flush_sec: float = 0.2            # 钉制 0~5
    # 澄清窗口 T_pause：answer.final 后窗口内 clarify.request → PAUSED
    clarify_window_sec: float = 1.2         # 钉制 0~30（0 = 关闭窗口直落定）
    # 卡 TTL T_ttl：空闲孤儿强制收口阈值（活跃回合每帧刷新 last_activity，
    # 长回合永不误杀——前代 v2.0.9 GC 语义）
    card_ttl_sec: float = 600.0             # 钉制 60~86400
    # SEALED 冷却：保留 message_id 反查窗口的时长，到期 GC 回收
    gc_cooldown_sec: float = 60.0           # 钉制 10~3600
    # pending 缓冲上限（CREATING 建卡窗口暂存，02-engine §3.3）
    pending_max: int = 64                   # 钉制 8~4096
    # 降级冷却：429 与其他错误分道（02-engine §1.2）
    degrade_cooldown_rate_limit_sec: float = 60.0   # 钉制 5~3600
    degrade_cooldown_other_sec: float = 30.0        # 钉制 5~3600
    # GC 巡检间隔（引擎级 sweeper 任务；TTL 强制收口 / 冷却回收 / 死锁解除）
    gc_sweep_sec: float = 30.0              # 钉制 5~300
    # frame_id 幂等缓存容量（01-protocol §5.4）
    frame_cache_max: int = 4096             # 钉制 16~1000000

    def __post_init__(self) -> None:
        _set = lambda name, value: object.__setattr__(self, name, value)  # noqa: E731
        _set("watchdog_idle_sec", _clamp(self.watchdog_idle_sec, 60, 1800, 300.0))
        _set("session_renew_age_sec", _clamp(self.session_renew_age_sec, 60, 590, 540.0))
        _set("panel_flush_sec", _clamp(self.panel_flush_sec, 0, 5, 0.2))
        _set("clarify_window_sec", _clamp(self.clarify_window_sec, 0, 30, 1.2))
        _set("card_ttl_sec", _clamp(self.card_ttl_sec, 60, 86400, 600.0))
        _set("gc_cooldown_sec", _clamp(self.gc_cooldown_sec, 10, 3600, 60.0))
        _set("pending_max", int(_clamp(self.pending_max, 8, 4096, 64)))
        _set("degrade_cooldown_rate_limit_sec", _clamp(
            self.degrade_cooldown_rate_limit_sec, 5, 3600, 60.0))
        _set("degrade_cooldown_other_sec", _clamp(
            self.degrade_cooldown_other_sec, 5, 3600, 30.0))
        _set("gc_sweep_sec", _clamp(self.gc_sweep_sec, 5, 300, 30.0))
        _set("frame_cache_max", int(_clamp(self.frame_cache_max, 16, 1000000, 4096)))
