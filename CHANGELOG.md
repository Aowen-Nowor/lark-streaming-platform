# Changelog

本仓库遵循语义化版本；0.0.x 为设计与骨架阶段。
格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。

## [0.0.1] - 2026-11-20

### Added

- Phase 0 设计文档集（全规格冻结稿）：
  - `docs/design/00-overview.md` — 终态定义、范围、分层架构、部署双形态、ADR
  - `docs/design/01-protocol.md` — 统一事件协议 LSEP/1（信封、事件目录、
    传输绑定、握手协商、错误语义、legacy 文本兼容模式）
  - `docs/design/02-engine.md` — 显式回合状态机、正交 chat 降级态、
    定时器表、per-chat 显式队列、事件裁决表
  - `docs/design/03-card-schema.md` — 卡片 Schema v1、主题 token、
    流式生命周期绑定、字节预算、Golden 快照契约
  - `docs/design/04-knowledge-freeze.md` — 前代知识冻结（CardKit 怪癖表、
    竞态事故档案、性能守恒、行为验收基线、mock 台架移植计划）
  - `docs/design/05-adapters.md` — 接入四件事、双形态接入、hermes /
    openclaw 参考接入方计划
- 项目脚手架（pyproject / LICENSE / README）

### 决策记录

- D1 从头重设计（用户裁决 2026-11-20）
- D2 语言保持 Python
- D3 新仓库仅 GitHub，版本 0.0.1 起步（用户裁决 2026-11-20）
- D4 只做出站侧，入站留给宿主 agent
- D5 协议 delta/snapshot 双模式
- D6 多语言 SDK 延后

[0.0.1]: https://github.com/Aowen-Nowor/lark-streaming-platform/releases/tag/v0.0.1
