# Changelog

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。

> **版本纪律（用户裁决 2026-11-20，最高优先级）**：本仓库版本号由用户
> 明示裁决变更，**任何自动化 bump 均不允许**——里程碑进度、交付批次
> 都不自动联动版本号。当前版本冻结 **0.0.1**。开发进度记录于
> [Unreleased] 段，待用户明示升版时整体并入对应版本段。

## [Unreleased]

- Phase 4：
  - **接入层交付（05-adapters 全规格落地；ADR D8 脚手架定位）** —
    接入四件事的参考实现 + 网关 shell，核心层零感知（分层红线自检通过）：
    - `adapters/_legacy_text.py`：前代文本启发式兼容转换器（01-protocol §8
      参考实现）——classify/progline 语义逐点移植（真实模板解析、300/600
      双限、通知清单、`(×N)` 去重、裸围栏块归属 terminal、多行思考吸附），
      永不抛异常红线继承；回归样本固化自前代 v2.0.6/8/15/22/34 事故防线
    - `adapters/hermes.py`：形态 A 出站钩子翻译面——hermes v0.21.x 官方
      metadata 契约 → LSEP/1 信封流（expect_edits→turn.start+snapshot、
      edit 流→snapshot/final、notify→final 或微回合自愈、无标记→deliver、
      平台通知→notice 且无卡永不建卡）；回合前暂存（pending_max=64 对齐
      引擎 CREATING 缓冲）、Q9 同文本去重、思考轮显式闭合、逐帧回执聚合
      为 `DeliveryResult`（§1.4 降级履行：action=native + native_text）
    - `adapters/openclaw.py`：形态 B bridge 消息映射器——streaming 双模
      （incremental→delta / 缺省→snapshot，D5）、finalize→final、无标记
      →deliver、结构化进度模板（tool/thinking/notice/interim）；映射器
      永不抛异常（坏消息空序列，不击穿 bridge 主循环）
    - `adapters/_envelope.py`：信封组装器（frame_id 全局唯一、ts/seq）
      + per-chat 回合注册表（加速路径非权威，引擎守卫始终兜底）
    - `gateway.py` + `__main__.py`：网关 shell（B = A + server shell）——
      配置面 env > lsp.yaml > 默认值（§4 设置键双轨冠名/原生名、env 同名
      覆盖、越界钳制、渲染保留键诚实标注）；stdio/HTTP/WS 绑定编排；
      SIGINT/SIGTERM → engine.aclose（seal_all_on_shutdown）→ stdio 桥
      经 stderr 收 lifecycle.event{shutdown} 后转原生
    - `docs/ADAPTER_SDK.md`：接入清单（四件事总览 / 形态 A·B 上手 /
      元数据契约映射表 / 降级履行规则 / 事件 payload 速查 / 配置面 /
      legacy 转换器 / 版本兼容承诺）——05-adapters §5.3 随代码发布项
    - 测试 110 项全绿（legacy 48 + hermes 28 + openclaw 14 + 双形态
      真实引擎集成 5 + 网关 shell 15）；全仓 402 passed
- Phase 3：
  - **protocol 层交付（01-protocol §3/§6/§7 全绑定落地）** — LSEP/1 三绑定，
    绑定层零语义裁决（信封校验/事件路由/ack 语义全归引擎消费面）：
    - `messages.py`：共享 wire 素材——帧解析（JSON 根非 object → 错误对象）、
      错误对象统一形态（HTTP 400 body / WS error 帧 / stdio 行三形同源）、
      hello 协商产物（hello.ack 含 protocol_v/engine_version/settings 快照）
    - `stdio.py`：零依赖绑定（纯 asyncio）——stdin 收事件行 / stderr 回回执
      行 / stdout 保留日志（fd 拓扑由 01-protocol §3 ack 语义锚定）；
      进程参数等效握手（protocol_v 不支持拒绝启动）；pipe 直连 + 线程泵
      回退；逐帧并发提交（05-adapters「不得假设 ack 时序」语义）
    - `server.py`：aiohttp 承载（gateway extra）——`POST /v1/events` 单事件
      同步 ack、`POST /v1/events:batch` 逐帧回执（非 object 元素槽位错误
      对象）、`GET /v1/health` 引擎观测同源、`GET /v1/ws` 长连接（hello
      握手矩阵：首帧违规/版本不支持/载荷非法 → 错误帧+关闭；JSON 垃圾帧
      → 错误帧不断连；重复 hello → 错误帧保连）；WS 断连时在途帧继续被
      引擎消费（卡片管线完成性优先于回执通道）
    - **01-protocol §3.1 增量修订**（绑定实现注记，无破坏）：WS 路径 /v1/ws、
      stdio fd 拓扑、错误对象形态、batch 元素裁决、回执时序语义
    - pyproject `[gateway]` extra 引入 aiohttp>=3.9（stdio 零依赖不受影响）
    - 测试 36 项全绿（wire 素材/stdio 行协议/HTTP 三端点/WS 握手矩阵，
      HTTP/WS 走 aiohttp TestServer 真 socket）；全仓 292 passed
- Phase 2：
  - **engine 编排层交付（02-engine 全规格落地）** — 显式回合状态机 + per-chat
    队列 + 竞态守卫（前代 FrameComposer 2306 行隐式旗标的结构化重写）：
    - `events.py`：LSEP/1 事件模型与消费面校验（信封字段表逐项；payload
      只读视图；Ack/LifecycleEvent 协议 wire 形态；未知事件 fallback 不静默）
    - `settings.py`：定时器冻结常量钳制（看门狗 300s/续期 540s/合并 0.2s/
      澄清窗口 1.2s/卡 TTL 600s/降级冷却 429→60s 其他→30s，越界一律
      钳进钉制范围不抛异常）
    - `state.py`：TurnPhase（CREATING→STREAMING⇄PAUSED→SEALING→SEALED）×
      ChatHealth（OK⇄DEGRADED⇄RECOVERING）正交双态；TurnState.seq 预递增
      纪律 + stream_session_at 字段名与 transport.seq 写入约定逐字对齐
    - `guards.py`：事件裁决表（decide 纯函数，owner 单一所有者）——含
      **跨 turn 帧 fallback 红线**（旧回合迟到帧绝不误入新卡，01-protocol
      §5.1 的协议化替代）
    - `engine.py`：per-chat 有序队列 + 单消费者协程（取代前代 per-chat
      asyncio.Lock；建卡窗口帧排队即幂等屏障，10-12 一轮 3 卡事故根治）；
      全部定时器回调经队列串行执行（武装/让位/入队三动作，权威复查在
      消费者侧）；定稿四步管道（终值 patch 致命 + 页脚/spinner/面板收口/
      关流式装饰性——前代 finalize.py 铁律）；chat 级降级红线（失败帧必
      fallback 带飞书码 + 冷却 fast-path + 冷却期满看门狗/帧双探测恢复）；
      200850 自愈 reopen + 540s 主动续期；面板结构性行即时 flush / 改写类
      0.2s 合并；deliver 完全旁路；seal_all_on_shutdown 先定稿再关停；
      snapshot_health 观测面与守卫结构化打点（Phase 6 双跑对照数据源）
    - 架构注记：02-engine §3.2/§3.3 建卡窗口 pending 缓冲（64 行上限）由
      per-chat 有序队列天然实现（state.py docstring 实现注记）
    - 测试 71 项全绿（信封校验矩阵/设置钳制/裁决表矩阵/生命周期 E2E/
      建卡窗口幂等/澄清窗口竞态与续流/看门狗四态/降级红线全链/序列自愈
      300317+200850/锚定降级直发 Q10/关停语义/GC 与 TTL/观测面）；
      全仓 256 passed（185 基线零回归）；分层红线 grep 自查通过
      （renderer/transport 零上层依赖、engine 零接入层依赖、零宿主 ABI
      try-import）
- Phase 1：
  - **renderer 层交付（M1 收口）** — 卡片 Schema v1 纯函数渲染（03-card-schema 全规格落地）：
    - `state.py`：结构化渲染输入（RenderState/PanelRow/FooterStats/ClarifyForm）——
      协议 v1 下渲染层零文本解析（前代 status_lines 文本启发式上移接入层
      legacy 转换器）；round_finalized 显式字段承接 thinking.round.end，
      单一所有者无推断
    - `theme.py`：主题 token 显式注入（前代 tool_icons 180 行映射/i18n 文案/
      字号 opt-in/页脚推理等级整体迁移）——前代 Config 单例直读的宿主感知
      泄漏根除；terminal 只进图标表不进标题表等怪癖保留
    - `md.py`：markdown 卫生化逐语义迁移（v2.0.21 审计 R5/R6 围栏状态机与
      分块围栏感知）+ OPT-2/5 精确串缓存器（answer 单槽 / 面板行逐行 LRU）
    - `elements.py`：CardKit wire 原语（前代生产验证结构逐字段一致）+
      200 元素上限安全网（面板裁旧 + 折叠提示计数累计）
    - `streaming.py`：引擎时点三产物绑定（initial/full/sealing 局部通道）+
      字节预算裁剪（03 §6 冻结顺序 interim → thinking 历史轮 → answer 尾部
      标注；页脚与终值永不裁——终值超限信任 200860 确定性拒收 → 降级红线
      兜底）；面板收口幂等跳过（流式末渲染等值则零 API）
    - `static.py`：deliver 旁路静态卡（前代 build_cron_card 语义：卫生化全链
      + job_id 溯源行 + 空 body 占位红线）
    - `clarify.py`：独立澄清卡五态（pending/answered/confirmed/unauthorized/
      expired）——回调命名空间协议化（lsp_clarify_action，hermes 名词不进
      协议）；「action」容器废弃红线（button 顶层元素）保留
    - **wire 形态裁决**：clarify 按前代生产验证的独立卡形态交付，回合卡
      槽位内嵌留待 L3 R7 真飞书实测定案（可靠性第一 D7——未经实证的
      wire 形态不进主链）
    - 测试 126 项全绿（行为回归 + 7 张 Golden 快照，03 §8 契约：
      空面板/首行/多轮思考/工具合并/三态/页脚全字段/裁剪）
  - **transport 层交付** — 前代怪癖知识代码化（04-knowledge-freeze Q1-Q10 对应落地）：
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
- 设计更新：可靠性第一（D7）+ 三层验证漏斗（L1/L2/L3 真飞书 R1-R7）+
  协议公开标准与适配层脚手架定位（D8）+ 前代仓库终态化（D9）
- **版本归位（用户裁决 2026-11-20）**：0.0.2 / 0.0.3 为未经批准的版本
  漂移（上轮会话自动 bump），全部归位合并至 [0.0.1]，对应 tag 已删除；
  新增纪律——版本号变更仅由用户明示触发（D10）

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
- 设计更新（可靠性裁决批次）：可靠性第一（最高优先级）落地为 02-engine
  §10 三层验证漏斗（L1 mock 单元 / L2 mock 端到端 / L3 真飞书验收 R1-R7，
  真实飞书应用与群聊授权已获）；知识冻结待实测项改由 L3 补测冻结
- 协议独立性声明：LSEP 是面向第三方的公开标准，官方仅凭文档即可接入，
  SDK 只是便利品（ADR D8）；接入层定位为**可丢弃脚手架**，官方原生接入
  后整体退役，协议与核心引擎永续（ADR D8）
- 项目脚手架（pyproject / LICENSE / README）

### 决策记录

- D1 从头重设计（用户裁决 2026-11-20）
- D2 语言保持 Python
- D3 新仓库仅 GitHub，版本 0.0.1 起步（用户裁决 2026-11-20）
- D4 只做出站侧，入站留给宿主 agent
- D5 协议 delta/snapshot 双模式
- D6 多语言 SDK 延后
- D7 可靠性第一 + 真飞书测试授权（用户裁决 2026-11-20）
- D8 协议 = 公开标准 / 适配层 = 可丢弃脚手架（用户裁决 2026-11-20）
- D9 前代仓库终态化：停止更新、只读归档、2027-01-01 转私有（用户裁决 2026-11-20）
- D10 版本号冻结：变更仅由用户明示触发，禁止一切自动 bump（用户裁决 2026-11-20）

[0.0.1]: https://github.com/Aowen-Nowor/lark-streaming-platform/releases/tag/v0.0.1
