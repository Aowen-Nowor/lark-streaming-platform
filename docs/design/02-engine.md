# 02 · 编排层 — 显式回合状态机与竞态守卫

> 引擎核心规格。前代 engine.py（2306 行）的全部隐式旗标组合在这里被
> 显式化为状态机 + 守卫注册表；每个竞态防线标注前代实证来源
> （见 04-knowledge-freeze）。

## 1. 状态机

### 1.1 卡片回合状态（per turn，即 per 活跃卡）

```
                    ┌────────────────────────────────────────────┐
                    │ watchdog 静默 ≥ T_idle                      │
                    ▼                                            │
IDLE ──turn.start──► CREATING ──create+send ok──► STREAMING ──answer.final──► SEALING ──ok──► SEALED
                       │                            │  ▲                        │fail
                       │ create/send fail           │  └── clarify.resume ──┐   ▼
                       ▼                            │                       │  SEALED(fallback)
                    SEALED(fallback)                │ answer.final          │
                                                    ▼                       │
                                                 PAUSED(clarify) ──超时─────┘
                                                    │ clarify.answer
                                                    └──► STREAMING（同一张卡续流）
```

| 状态 | 进入条件 | 允许消费的事件 |
|------|----------|----------------|
| `IDLE` | 初始 / GC 收口后 | turn.start → CREATING；deliver → 旁路静态卡（§8）；其余事件按 §6 裁决 |
| `CREATING` | turn.start 受理 | 帧全部暂存（pending 缓冲，上限见 §5）；create 失败 → SEALED(fallback) |
| `STREAMING` | create+send_card_by_id 成功 | 全部流事件；answer.final → SEALING；clarify.request → PAUSED |
| `PAUSED` | clarify.request 受理（同一张卡保流式态） | clarify.answer → STREAMING（续流）；超时 → SEALING（真落定） |
| `SEALING` | answer.final / pause 超时 / watchdog | 定稿（① answer 终值 patch〔致命〕→ ② 全量写回定稿卡〔装饰性，含页脚 + 删 spinner + 面板收口 + 关流式〕）；①失败 → SEALED(fallback)，装饰步（②）失败仅告警——答案已送达，重发必重复（audit #7 对齐 engine.py `_seal_decorations`；#14：settings / batch_update 端点 404 后改走全量更新） |
| `SEALED` | 定稿成功（正常/回退） | 终态；60s 冷却后 GC 回收（保留 message_id 反查窗口） |

**单一所有者**：每个状态转移由且仅由一个守卫函数裁决；守卫注册表在
引擎构造时冻结，运行期不可变更。

### 1.2 正交 chat 级状态（per chat，独立于回合）

```
CHAT_OK ⇄ CHAT_DEGRADED ⇄ CHAT_RECOVERING → CHAT_OK
```

| 状态 | 进入 | 离开 |
|------|------|------|
| `CHAT_OK` | 初始 | 任何卡片 API 调用失败（网络/4xx/5xx/429）→ CHAT_DEGRADED |
| `CHAT_DEGRADED` | 首次失败 | 冷却期（429: 60s；其他: 30s）后进入 RECOVERING；期间所有帧 `ack.verdict=fallback`（消息走原生，零丢失） |
| `CHAT_RECOVERING` | 冷却期满 | 静默探测（下一帧以真实 API 试探）：成功 → CHAT_OK（该帧正常消费）；失败 → CHAT_DEGRADED（冷却重启） |

降级是 chat 级而非 turn 级（前代实证：一个 turn 内失败后同 chat 后续 turn
仍可能恢复）。

## 2. 定时器（全部可经配置覆盖，钉制范围见括号）

| 定时器 | 默认 | 触发动作 | 前代实证 |
|--------|------|----------|----------|
| 看门狗 `T_idle` | 300s（60~1800） | STREAMING 态静默超时 → seal 卡（标注 fallback），防跑马灯永挂 | 11-08 16:05 网关压缩静默 300s+ 被 120s 旧值误封 → 提至 300s |
| 会话续期 `T_renew` | 540s 龄 | STREAMING 态且流式会话龄 ≥540s → 下一帧更新前先续期（streaming_mode:true） | 11-07 两例：会话 600s 整撞 200850 |
| 面板尾沿合并 `T_flush` | 0.2s | 原地改写类面板更新合并为单次 cardkit_update；**结构性新增行不合并**（即时可见） | 499s 回合 35 次工具 ≈70 次全量上传 ≈1.3MB（OPT-1a） |
| 澄清窗口 `T_pause` | 1.2s | answer.final 后窗口内收到 clarify.request → PAUSED；超时 → 真落定 SEALING | 前代边界 finalize 与真终态参数一致的竞态 |
| GC `T_ttl` | 600s（min 60） | SEALED 后 60s 冷却回收反查窗口；TTL 到期强制收口（先置降级+定稿再回收） | 前代 card_ttl_sec |

## 3. 并发模型

1. **per-chat 显式队列**（取代前代 per-chat asyncio.Lock）：每个 chat 一个
   有界有序事件队列（`maxsize=pending_max`，默认 64，钉制 8~4096）+
   单消费者协程。事件入队即返回受理，消费按序执行；消费完成时逐帧发
   ack。**跨 chat 完全并行，chat 内严格串行**。
2. **建卡窗口幂等**：CREATING 态本身即幂等屏障——建卡窗口（0.5~1s 网络挂起）
   内到达的帧进 pending 缓冲而非触发第二次建卡（前代 10-12 14:30「几点了」
   一轮 3 卡事故的根治；锁内复查语义由状态机天然提供）。
3. **队列背压契约（2026-12 audit #2 显式化）**：队列满时新帧**不排队、
   不阻塞**——立得 `FALLBACK(queue_full:native_delivery)` 回执，接入方
   原生投递。背压就是另一种降级：帧不丢语义与永不丢帧红线同构
   （降级红线 = API 失败必回执；背压红线 = 队列容量不可用时也必回执）。
   控制帧（watchdog/TTL/flush）入队失败可安全丢弃：TTL/SEAL_PROBE 由
   sweep 周期重发，watchdog 场景的队列拥塞本身意味着消费者活跃。
   另：面板 pending 行缓冲上限 64 行（前代 `_PENDING_STATUS_MAX`）；
   超限丢弃最旧 interim/thinking，保留 tool 行与 answer 帧
   （answer 帧永不丢弃）。
4. **定时器归属**：全部定时器按 chat 注册于引擎级调度表；chat GC 时级联
   取消。重复武装幂等（复查挡）。

## 4. 渲染协同

引擎不拼卡片 JSON——只维护渲染输入状态（`RenderState`：answer 缓冲、
面板行列表、行到达时间、工具耗时表、页脚统计），在以下时点调用渲染层
（03-card-schema）：

- CREATING 成功 → 初始卡（含 pending 状态、loading 提示）
- STREAMING 每个 answer 帧 / 面板合并窗口到期 → 流式更新
- PAUSED 进入/离开 → 暂停行补插
- SEALING → close_streaming + 终值 patch + 页脚

**懒面板**：首个面板行到达前渲染无面板卡（纯 answer 回合无空面板，
前代 v2.0.24 体验定案）；面板存在性由渲染产物实测，不做推断。

## 5. 字节预算与卫生化

- 渲染产物在传输前强制字节预算校验（27000 bytes，前代 `_CARD_BYTE_BUDGET`）；
  超限裁剪顺序：interim 行 → thinking 历史轮 → answer 尾部截断标注。
  **页脚与最终 answer 永不裁剪**。
- answer 的 markdown 卫生化与行解析 memo 化（前代 OPT-1b/5）在渲染层
  实现，缓存键与失效语义同前代（单槽精确串缓存 / 逐行缓存）。

## 6. 事件裁决表（摘要）

| 事件 × 状态 | 裁决 |
|-------------|------|
| answer.* × 非 STREAMING/PAUSED/CREATING | fallback（detail=no_active_card） |
| turn.start × 存在未定稿 turn | 旧 turn seal → 新 turn CREATING（§5.1） |
| clarify.request × 非 STREAMING | fallback（detail=clarify_without_stream） |
| clarify.answer × 非 PAUSED | 通告忽略（lifecycle.event 记录） |
| deliver × 任意 | 旁路静态卡（§8），不影响当前回合 |
| 序列自愈（300317） | 传输层内部自愈重开，不产生 verdict 变化（§7） |

## 7. 降级红线（重申）

1. 引擎侧：任何卡片 API 失败 → chat 降级 + 该帧 fallback 回执 + 后续帧
   全部 fallback（直至恢复探测成功）。**不存在静默失败的帧**。
2. 协议侧：每个帧恰好一个 ack；`fallback/degraded` 是接入方必须原生投递
   的强制指令。
3. 关闭语义：`seal_all_on_shutdown`——全部活跃卡先置定稿再关流（前代
   同名语义），关闭后收到的事件回 fallback。

## 8. 回合外投递（deliver）

- 完全旁路回合状态机与看门狗；独立静态卡渲染路径（一次性卡，短回复
  也收）。
- 与活跃流式卡共存：不劫持、不合并（前代通道隔离的显式化）。
- 失败同样进 chat 降级统计（静态卡 API 也可能 429）。

## 9. 观测面

- 引擎暴露最小观测接口：per-chat 状态摘要（状态机态、chat 级态、活跃
  卡龄、队列深度、降级计数）——`GET /v1/health` 与形态 A 的
  `engine.snapshot_health()` 同源。
- 全部竞态守卫触发时打点（结构化日志字段：chat_id、turn_id、guard 名、
  裁决结果）——为 Phase 6 双跑对照提供对齐数据。

## 10. 可靠性验证策略（可靠性第一，ADR D7）

用户裁决（2026-11-20）：**与飞书的交互一定不能出问题；必要时动用真实
飞书环境测试。** 落地为三层验证漏斗：

| 层 | 手段 | 覆盖 |
|----|------|------|
| L1 单元 | mock HTTP + 故障注入（429/300317/200850/超时/字节超限） | 传输层全部行为分支 |
| L2 mock 端到端 | 移植的前代 mock 台架（04 §5） | 状态机 / 守卫 / 验收基线全量 |
| L3 真飞书验收 | **真实飞书应用 + 真实群聊**（授权已获） | 仅关键怪癖实证与上线前冒烟，见下表 |

L3 真飞书验收点（每点必须在新框架下真实复现并记录实测值，回填
04-knowledge-freeze 待实测项）：

| # | 验收点 | 对应怪癖 |
|---|--------|----------|
| R1 | 流式会话寿命实测（建卡后自然静默至撞线，记录精确秒数） | Q2 / 200850 |
| R2 | 540s 主动续期是否消除 200850 | Q2 对策有效性 |
| R3 | 300317 复现条件与自愈闭环 | Q1 |
| R4 | 429 触发阈值、响应体结构、Retry-After 头有无 | Q4 |
| R5 | 建卡→发送→流式→关流全链路在真实群聊的表现 | 基线 |
| R6 | 字节预算边界（27000 附近实测官方行为） | Q5 |
| R7 | clarify 交互回调与授权校验失败码 | 待实测项 |

L3 纪律：验收点实测值只回填知识冻结文档；单测/mock 不依赖实测网络
时序（真实值只作断言区间与默认参数来源），保证 L1/L2 可重复。
