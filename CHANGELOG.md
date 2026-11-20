# Changelog

格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)。

> **版本纪律（用户裁决 2026-11-20，最高优先级）**：本仓库版本号由用户
> 明示裁决变更，**任何自动化 bump 均不允许**——里程碑进度、交付批次
> 都不自动联动版本号。当前版本冻结 **0.0.1**。开发进度记录于
> [Unreleased] 段，待用户明示升版时整体并入对应版本段。

## [Unreleased]

- Phase 6：
  - **双跑对照交付（M4 收口）+ 发现项 F1/F2/F3 修复** — 同一段 hermes
    出站流（12 场景语料）分别驱动前代终态（old-hermes-ref c826ee8
    FrameComposer + 真实 build_streaming_card_v2 + 录制客户端）与本平台
    全栈（HermesAdapter → 引擎 → 渲染 → 传输 → MockFeishuREST），逐场景
    对照（报告：docs/dual-run-report.md，生成器：scripts/dual_run.py，
    台架：tests/harness/dual_run.py + corpus/*.jsonl）：
    - **消息永不丢红线跨实现成立**：全部场景双侧零静默丢弃；降级注入
      （create 拒收 200860）双侧 4 帧全原生；竞态注入（10-12 事故形态）
      双侧恰建 1 卡（结构性根治互证）
    - **F1 澄清窗口可达性（修复）**：边界 finalize 后适配器注册表已注销，
      hermes send_clarify 触达不了引擎 clarify_window_hit（L1/L2 各自
      全绿但集成面断裂——双跑独有发现形态）。`TurnRegistry.reopen` +
      适配器宽限册（最近关闭回合 + 时间戳，窗口对齐引擎 clarify_window_sec），
      窗口命中重开注册表（seq/thinking/Q9 状态连续），过期 → 原生 ask
      （native_text 携带问题文本，内容永不丢）
    - **F2 无标记进度升格（修复）**：edit-transport 工具气泡首发无标记，
      冻结契约「无标记 → deliver」散卡刷屏——前代 v2.0.1/v2.0.4/v2.0.8/
      v2.0.31 升格主链移植 `lt.upgrade_plain`（CH_PLAIN 先过启发式再落
      deliver）；05-adapters §5.1 映射表同步修订
    - **F3 cron 家族溯源（修复）**：job_id 帧（官方调度标记）此前走微回合
      卡、包装头原文上卡、job_id 溯源丢失——`lt.parse_cron_delivery` 移植
      + `_deliver_cron` 路由（标题/正文剥离 + footer.job_id），前代 R1
      规则对齐
    - **同卡澄清续流 = 协议增强**：旧 edit-transport 定稿后结构性无法续写
      （澄清续流 = 2 张回合卡）；新架构宿主携 turn_id 即同卡续流（语料
      场景 12 实证）——v2.0.26 用户验收语义在协议层达成
    - **字节预算再评估结论（04 §6 尾项闭环）**：双跑 mock 面零裁剪触发
      （TrimReport 打点）；27000 维持不变（Phase 5 真火 100~120KB 实测
      结论维持，保守裕度优先；放宽属用户裁决事项）
    - clarify 回调授权失败码集维持 **MANUAL**（hermes 网关回调接线依赖，
      M5 生产切换冒烟清单项）
    - 设计/知识回填：05-adapters §5.1（Phase 6 双跑对照修订）+
      04-knowledge-freeze §7（双跑对照冻结）+ ADAPTER_SDK §2.1/§2.1.1
    - 测试：双跑验收 12 项（tests/harness/test_dual_run.py，旧仓缺席
      自动 skip）+ 发现项专项 11 项（tests/adapters TestDualRunFindings）
      全绿；全仓 **446 passed**（Phase 5 基线 423 零回归）
- Phase 5：
  - **L2 mock 台架交付（04-knowledge-freeze §5 移植主体）+ 行为验收基线全绿 +
    L3 真飞书火力脚本**：
    - `tests/harness/mock_feishu.py`：MockFeishuREST —— HTTP 层 mock（httpx
      MockTransport 承载**真实 FeishuTransport 代码在环**：token 管理 / wire
      JSON 字符串形态 / retry / seq 自愈 / 业务码裁决全走真路径），CardKit
      五端点 + IM 三端点 + 可注入故障（fail_next/fail_always 分 HTTP 状态与
      业务码、delay 建卡窗口挂起、strict_sequence 300317、expire_streaming
      200850+官方 reopen 解除）
    - `tests/harness/framework.py`：Harness 回合编排器（真实引擎 + 真实传输 +
      FakeClock + 生命周期收集 + LSEP/1 信封构造）
    - 行为验收基线 21 项全绿（§4 五族）：生命周期（全回合/回合外 deliver/
      abort/停机 seal_all）、竞态（建卡窗口并发恰一卡 Q3 / 跨 chat 并行 /
      新回合打断旧卡 superseded / T_renew 主动续期 / 200850 撞线自愈 Q2 /
      300317 自愈 Q1）、降级（create 失败 fallback+降级 / 冷却静默探测恢复 /
      持续失败零丢失 / stream 失败 30s 档）、clarify（窗口命中同卡暂停 /
      超时真落定 / 表单三态）、渲染集成（懒面板 / 面板装配 / 27000 字节
      裁剪 / 页脚零重放）
    - `scripts/e2e_real.py`：L3 真火脚本（引擎+渲染+传输全栈直驱真实飞书
      API）——12 场景（connect/plain/answer/progress/full/burst/clarify/
      reply/renew/budget/ratelimit/seqconflict）+ `--mock` 零写干跑 + `--long`
      主动续期真时序 + §6 五项数值补测探针（限频响应体/Retry-After、
      300317 重放码与 reopen 最小间隔、reply 锚定群私差异、clarify 回调
      码集 MANUAL 标注、字节预算阶梯细化）；速率自保护 ≥0.45s/帧
    - L2 mock 干跑 9/9 场景 PASS；全仓 **423 passed**（基线 402 + harness 21）
    - **真火执行完成（2026-11-20，真实租户全栈直驱）**：默认 9 场景 8/9 PASS
      （budget 为测量发现非缺陷）+ 探针/续期补测全通过——卡片/面板/澄清/
      回复/并发收敛全链路在真实飞书可视化验证；§6 五项实测冻结（详见
      04-knowledge-freeze §6）：限频新码 **200937**（卡级频控 "card updated
      too frequently"，已入册 errors.py 瞬态族）与 99991400 分道；300317
      同号重放必拒/跳号允许/reopen 最小链 0.20s；伪锚点 99992354 + Q10
      降级直发真火回归；**官方 30KB 口径与实体卡实测严重不符**（create/
      stream/update 均至 100KB 接受，stream 内容上限 100~120KB 以 99992402
      字段校验形态出现，200860 全路径未复现）——27000 本地预算保持保守
      裕度不变；T_renew 主动续期真时序（60s 钳制档）触发无 200850；clarify
      回调码集标 MANUAL 留 Phase 6

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
