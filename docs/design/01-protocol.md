# 01 · 统一事件协议 v1（LSEP/1）

> Lark Streaming Event Protocol，版本 1。本文是协议冻结稿；实现（编解码、
> 传输绑定）必须与本文逐字段一致。扩展走 v1.x 小版本，破坏性变更走 v2。
>
> **协议独立性声明**（ADR D8）：LSEP 是面向第三方的公开标准。任何 agent
> 官方仅凭本文档即可实现投递端——不需要本仓库的任何代码或 SDK；
> SDK 只是便利品，不是接入条件。本文必须自包含：字段语义完备、
> 传输绑定明确、错误行为可推断。

## 1. 设计原则

1. **协议先行**：协议是接入方与引擎之间唯一的契约词汇表；核心层
   零文本启发式（前代的字符串编码上移为适配器侧的兼容转换，见 §8）。
2. **双帧语义**：`answer.delta`（增量）与 `answer.snapshot`（全量）一等公民，
   引擎内部统一化为全量缓冲；接入方按自身能力二选一。
3. **永不丢帧**：协议层任何解析失败不抛给接入方流程——返回
   `ack.verdict=fallback`，接入方走原生投递。
4. **可演进**：`v` 字段协商；未知事件类型 → `ack.verdict=fallback` + 
   `detail=unknown_event`（不静默吞掉）。

## 2. 信封（Envelope）

所有事件共用一个信封。字段表：

| 字段 | 类型 | 必填 | 语义 |
|------|------|------|------|
| `v` | int | 是 | 协议版本，恒 `1` |
| `event` | string | 是 | 事件类型名（§4 目录） |
| `frame_id` | string | 是 | 帧唯一标识（接入方生成，如 `f_{n自增}`），ack 回执按此关联 |
| `turn_id` | string | 视事件 | 回合标识；`turn.*` / answer / tool / thinking / interim / notice / clarify.* 必填 |
| `chat_id` | string | 是 | 飞书 chat open_id（回合外投递亦必填） |
| `ts` | int | 否 | 发送方毫秒时间戳（诊断用，引擎不依赖） |
| `seq` | int | 否 | 接入方在 turn 内的单调递增序号；提供时引擎做乱序检测（§5.3） |
| `payload` | object | 是 | 事件载荷（§4 逐事件定义） |

示例：

```json
{
  "v": 1,
  "event": "answer.delta",
  "frame_id": "f_42",
  "turn_id": "t_20260101_0930_a1b2",
  "chat_id": "oc_abc123",
  "ts": 1767000000123,
  "seq": 42,
  "payload": { "delta": "流式增量文本" }
}
```

## 3. 传输绑定

同一信封，三种接入绑定 + 一类回调绑定；接入方按部署形态选择。

| 绑定 | 交互模型 | 适用 |
|------|----------|------|
| **stdio** | 行分隔 JSON：子进程 stdout 每行一个事件，引擎 stderr 同格式回 ack | 桥接子进程形态（openclaw 起托管进程） |
| **HTTP** | `POST /v1/events` 单事件（同步返回 ack）；`POST /v1/events:batch` 批量（数组逐个返回 ack 数组） | 低频接入、脚本、调试 |
| **WS** | 长连接：客户端→服务端事件帧，服务端→客户端 ack 帧；连接级 `hello` 握手 | 高频流式、多 chat 复用 |
| **feishu-callback** | `POST /v1/feishu/callback`：飞书侧卡片交互回调 → LSEP 事件（仅 clarify.answer）；验签+解密后走引擎同一条入口 | 卡片交互闭环（澄清按钮回调；§3.2） |

HTTP 端点约定：

- `POST /v1/events` → 200 + 单个 ack 对象（协议级失败返回 400 + error 对象，
  业务失败恒 200 + ack.verdict）
- `GET /v1/health` → 引擎状态摘要（chat 数、活跃卡数、降级中的 chat）

WS 握手（§6）替代 HTTP 的无状态性；stdio 无握手（进程即会话）。

### 3.1 绑定实现注记（Phase 3 具体化，增量无破坏）

- **WS 端点路径**：`GET /v1/ws`（HTTP 端点约定的补全；服务端实现为
  aiohttp，参考实现随仓库发布）。
- **stdio fd 拓扑**（引擎进程视角）：stdin 收事件行；stderr 回 ack /
  error 行；stdout 保留给运行日志（不承载协议流量）。单帧上限 8MB。
- **错误对象统一形态**（§7「错误对象」的三绑定具体化）：
  `{"v": 1, "event": "error", "frame_id": "<尽力关联>", "payload": {"detail": "<机器可读原因>"}}`
  ——HTTP 400 body / WS error 帧 / stdio stderr 行同形。
- **batch 元素裁决**：非 object 元素槽位回错误对象（逐帧回执语义保持，
  无 frame_id 可关联）；数组内 dict 元素全部走引擎裁决（含字段缺失的
  fallback ack）。数组根本身非数组 → 400 + 错误对象。
- **回执时序**：stdio/WS 逐帧并发提交、完成即回写（05-adapters §1：
  接入方不得假设 ack 时序）；同 chat 的 ack 顺序由引擎 per-chat 单
  消费者保证与消费序一致。WS 连接断开时在途帧继续被引擎消费（卡片
  管线完成性优先于回执通道）。

### 3.2 feishu-callback 绑定注记（卡片交互回程）

飞书回调是**第四类入站面**：方向与接入绑定相反（飞书服务器 → 平台），
承载卡片交互（澄清按钮）回流。与接入绑定的三同三异：

- **同**：解析产物走 `engine.submit` 同一条入口（同队列同背压同幂等，
  不开旁路）；永不丢帧红线同构；引擎状态机守卫同一套。
- **异 1（独立鉴权域）**：飞书回调无法携带 Bearer——验签走
  `verification_token`（明文比对）/ `encrypt_key`（AES-256-CBC，
  key = sha256(encrypt_key)）独立鉴权域，**不受**
  `LSP_GATEWAY_TOKEN` 中间件覆盖（若被覆盖，非 loopback 部署强制
  配 token 时回调恒 401 等于不存在）。
- **异 2（HTTP 语义替 ack）**：飞书不是 LSEP 客户端，不认 ack 帧——
  HTTP 状态码承载裁决：consumed → 200；`fallback(queue_full)` → 503
  （飞书回调自带退避重试接管该方向的传输级重试；PAUSED→RESUME
  守卫幂等吸收重复投递）；其余 fallback → 200（控制帧无原生履行
  义务，02-engine §6）。
- **异 3（路由反向解析）**：回调载荷只带 `clarify_id`，(chat_id,
  turn_id) 由 `engine.resolve_clarify` 从状态解析（仅 PAUSED 态
  命中；状态是唯一事实源，不建反向索引）；未命中（过期/已答/重启）
  → 200 ignored（前代 clarify._expired_response 语义，不触发飞书
  重试风暴）。

事件分道：`url_verification` → echo challenge（token 比对通过才
  echo）；`card.action.trigger` 且 behaviors value 含
  `lsp_clarify_action` → clarify.answer 提交（values 透传，选项序号
  → 原值映射由引擎持有的 form 原值承担；select/input 载荷槽位以
  L3 R7 实测校准）；其余事件类型（im.message.receive_v1 等——事件
  订阅 URL 误指场景）→ 200 无害忽略。用户消息入站不在本绑定范围
  （05-adapters §5.1：留在 hermes 原生渠道）。

fail-closed：未配置 `verification_token` → 端点恒 503（可见禁用态）；
密文到达而无 key / 解密失败 / token 错配 → 403，引擎零状态变更。
配置面：`FEISHU_VERIFICATION_TOKEN` / `FEISHU_ENCRYPT_KEY` env，或
lsp.yaml `feishu.verification_token` / `feishu.encrypt_key`
（均密钥纪律：repr=False、永不入库）。

## 4. 事件目录（v1 最小集）

### 4.1 回合生命周期

| 事件 | payload | 语义 |
|------|---------|------|
| `turn.start` | `{ reply_to?, title?, turn_kind? }` | 开启回合。`reply_to`：被回复消息 id（锚定引用）；`turn_kind`：`qa`（默认）预留扩展 |
| `turn.abort` | `{ reason? }` | 接入方主动中止回合；引擎 seal 当前卡（标注中止原因），后续同 turn_id 帧全部 fallback |

### 4.2 回答流

| 事件 | payload | 语义 |
|------|---------|------|
| `answer.delta` | `{ delta }` | 增量追加。引擎缓冲：`buffer += delta` |
| `answer.snapshot` | `{ text }` | 全量快照替换。引擎缓冲：`buffer = text`。**快照语义优先**：同 seq 下 snapshot 覆盖先前 delta |
| `answer.final` | `{ text?, stats? }` | 回合定稿。`text` 缺省用当前缓冲；`stats`：`{ duration_ms?, model?, context_tokens?, extra? }` 渲染进页脚 |

### 4.3 进度面板

| 事件 | payload | 语义 |
|------|---------|------|
| `tool.start` | `{ tool, preview? }` | 工具开始；渲染为面板工具行（含 spinner 期） |
| `tool.end` | `{ tool, duration_ms?, ok?, output_preview? }` | 工具结束；行内回填耗时标签（`(2.3s)`）；重复同名合并 `(×N)` |
| `thinking.delta` | `{ text }` | 思考流追加（面板思考气泡，按轮分组） |
| `thinking.round.end` | `{ round, duration_ms? }` | 思考轮收口；轮标题「第 N 轮 · X.Xs」 |
| `interim` | `{ text }` | mid-turn 播报（非工具非思考的中途文本），渲染为面板 interim 行 |
| `notice` | `{ level, text }` | 平台通知（子代理失败/超时/生命周期通告）。`level`：`warn|info|error`；豁免面板行长度上限 |

### 4.4 澄清交互

| 事件 | payload | 语义 |
|------|---------|------|
| `clarify.request` | `{ form }` | 发起澄清。`form`：Schema 见 03-card-schema §5（select / multi_select / input） |
| `clarify.answer` | `{ values, operator? }` | 澄清应答回流（引擎收到交互回调后由网关侧转交接入方，形态 A 为回调注册） |

### 4.5 回合外投递

| 事件 | payload | 语义 |
|------|---------|------|
| `deliver` | `{ text, title?, footer? }` | cron / 后台任务结果投递。**旁路回合状态机**：一次性静态卡，`footer`：`{ job_id?, ... }` 溯源 |

### 4.6 引擎 → 接入方回执（唯一的反向事件）

| 事件 | payload | 语义 |
|------|---------|------|
| `frame.ack` | `{ frame_id, verdict, message_id?, detail? }` | 逐帧回执。`verdict`：`consumed`（已进卡片管线）/ `fallback`（未消费，接入方必须走原生投递）/ `degraded`（该 chat 已降级，本帧及后续帧均走原生）。`message_id`：卡片消息 id（consumed 时可锚定后续 edit 场景，协议 v1 中流式更新全部走引擎内部，接入方无需 edit） |
| `lifecycle.event` | `{ type, detail? }` | 引擎级通告：`card_sealed_fallback`（看门狗封卡）/ `chat_degraded` / `chat_recovered` / `shutdown`。接入方据此调整投递策略（可选消费） |

**回执语义**：每个投递进引擎的帧都必须恰好收到一个 `frame.ack`（按
`frame_id` 关联）。批量投递逐帧回执。接入方在收到 `fallback/degraded`
前不得认为消息已呈现——**消息永不丢红线由两侧共同保证**：
引擎保证失败必回执，接入方保证 fallback 必投递。

## 5. 语义规则

### 5.1 turn_id 隔离

- 同一 chat 允许并发多个 turn_id（接入方保证业务上合理，如新一轮提问
  打断旧回合时发 `turn.abort`）；引擎对**同一 chat 同一时刻只维护一张
  活跃流式卡**——新 turn.start 到达且旧 turn 未定稿时，旧卡被 seal
  （verdict 已 consumed 的帧不受影响），新 turn 建新卡。此规则取代前代
  `turn_id` 防串卡旗标。

### 5.2 snapshot 与 delta 的统一化

- 缓冲起点：`turn.start` 后首个 answer 帧建立缓冲。
- `snapshot`：无条件替换缓冲。
- `delta`：追加。
- `final.text` 缺省：取当前缓冲作为终值。
- 混用合法（如 delta 流 + 周期 snapshot 校准）；引擎以最后到达帧为准。

### 5.3 seq 乱序检测

- 接入方提供 `seq` 时：引擎按 (turn_id, seq) 检测乱序。乱序帧仍被消费
  （best-effort），但 ack.detail 附加 `out_of_order=true` 供诊断。
  v1 不做重传协议——流式场景丢帧由后续 snapshot 校准兜底。

### 5.4 幂等

- `frame_id` 重复：引擎返回首个结果的缓存 ack（不重复消费）。
- `turn.start` 重复（同 turn_id）：幂等返回，不重复建卡。

## 6. 握手与版本协商

WS 连接建立后第一帧必须是：

```json
{ "v": 1, "event": "hello", "frame_id": "f_0", "chat_id": "",
  "payload": { "agent": "openclaw", "protocol_v": 1,
               "capabilities": { "snapshot_only": false } } }
```

引擎回 `hello.ack`（payload 含 `protocol_v`、`engine_version`、
`settings`）。协商失败（protocol_v 不支持）→ 关闭连接。
stdio 绑定以进程参数传递等效信息；HTTP 绑定无握手（信封 `v` 字段即协商）。

## 7. 错误语义

| 场景 | 行为 |
|------|------|
| 信封 JSON 解析失败 | 该绑定回错误对象（HTTP 400 / WS error 帧 / stdio stderr 行）；不产生 ack |
| 信封字段缺失/类型错 | `ack.verdict=fallback`，`detail=invalid_envelope:{原因}` |
| 未知 event 类型 | `ack.verdict=fallback`，`detail=unknown_event:{event}` |
| 卡片 API 失败（引擎内部） | `ack.verdict=fallback` 或 `degraded`（该 chat 已进入降级态，见 02-engine §7）；detail 携带飞书错误码 |
| 引擎内部异常 | 同上 fallback；`lifecycle.event{type=internal_error}` 通告 |

## 8. 文本兼容模式（legacy）

前代的文本启发式编码（`{emoji} {tool}…` 工具行 / `💬` 思考 / 围栏命令块 /
`(×N)` 去重）**不上核心层**：由接入方（如 hermes 适配器）把原生文本帧
转换为协议事件后投递。转换器作为参考实现随仓库发布
（`adapters/_legacy_text.py`，Phase 4 交付），使前代接入方零改动语义迁移。

## 9. 版本化策略

- v1.x：新增可选 payload 字段 / 新增事件类型（未知名 fallback 兜底），
  向后兼容。
- v2：破坏性变更（字段删除 / 语义反转）。双版本并行期由 `hello` 协商。
