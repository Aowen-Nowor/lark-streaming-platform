# Changelog

本仓库遵循语义化版本；0.0.x 为设计与骨架阶段。
格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。

## [0.0.3] - 2026-11-20

### Added

- **Phase 1（进行中）transport 层交付** — 前代怪癖知识代码化（04-knowledge-freeze Q1-Q10 对应落地）：
  - `errors.py`：飞书错误码全集与判定谓词（16 常量：300317/200850/200860/
    300313/300314/300315/230099/11310/300305/99991400/300309/2200/1663/300000/
    1000023/99992354 等）；200860 确定性拒收不可重试红线；230099 通用码
    ErrCode 子码裁决；日志脱敏（token/secret）
  - `token.py`：tenant_access_token 惰性管理（提前 300s 刷新、并发单飞、
    凭证错误零重试）
  - `retry.py`：退避分道（瞬态 0.1/0.3/0.6；限频 99991400 秒级 1/3/6；
    网络错误同瞬态；CancelledError 永不吞）
  - `seq.py`：300317 对齐自愈（seq+1 重发）+ 200850 重开会话闭环
    （settings streaming_mode:true，跳号允许）；上限 3 次外上抛交降级红线
  - `budget.py`：27000 字节本地预检（OPT-1c：省一次注定 200860 的往返）
  - `client.py`：raw httpx REST 全端点（cardkit create/stream_element/
    update/batch_update/settings[close|reopen|summary] + im send/reply）；
    wire 形态与前代生产验证逐字段一致；纯 add_elements 非幂等绕过自动重试；
    token 失效自刷新重试
  - 测试 59 项全绿（L1 mock HTTP + 故障注入：429/300317/200850/200860/
    300313/token 失效/取消穿透）
- 依赖调整：lark-oapi SDK → httpx（raw REST 全权掌控，怪癖行为不再隔 SDK）

## [0.0.2] - 2026-11-20

### Changed

- 设计原则新增**可靠性第一**（最高优先级）：飞书交互不出问题 > 功能 > 优雅；
  冲突时先保可靠性（ADR D7）
- 02-engine 新增 §10 可靠性验证策略：三层验证漏斗（L1 mock 单元 /
  L2 mock 端到端 / **L3 真飞书验收**——真实飞书应用与群聊，授权已获，
  R1-R7 七个验收点）
- 协议独立性声明：LSEP 是面向第三方的公开标准，官方仅凭文档即可接入，
  SDK 只是便利品（ADR D8）
- 接入层定位改为**可丢弃脚手架**：现阶段代官方做适配，未来官方
  原生接入后整体退役，协议与核心引擎永续（ADR D8）
- 知识冻结待实测项改由 L3 真飞书验收补测冻结

### 决策记录

- D7 可靠性第一 + 真飞书测试授权（用户裁决 2026-11-20）
- D8 协议 = 公开标准 / 适配层 = 可丢弃脚手架（用户裁决 2026-11-20）
- D9 前代仓库终态化：停止更新、只读归档、2027-01-01 转私有（用户裁决 2026-11-20）

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