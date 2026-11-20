# 05 · 接入契约 — 双形态与参考接入方计划

> 接入方只写 `adapters/<agent>/`，不改核心层。本文定义接入四件事与
> 两个参考接入方的落地计划。
>
> **脚手架定位声明**（ADR D8）：本层是**可丢弃的脚手架**——现阶段
> 官方不会按 LSEP 协议接入，由本项目代做适配让接入方零成本上手；
> 未来 agent 官方按协议原生接入后，本层整体退役。协议（01-protocol）
> 与核心引擎（02/03）永续。本层不得向协议文档泄漏任何内部实现细节，
> 也不得成为接入的必要条件。

## 1. 接入四件事

任何 agent 接入本平台，只需提供：

1. **事件映射器**（必需）：把 agent 原生出站流翻译为协议 v1 事件
   （01-protocol）。前代文本启发式编码在此层转换（`_legacy_text.py`
   参考实现随仓库发布）。
2. **投递表面**（必需）：形态 A 直接调 `engine.submit(event)`；形态 B
   经 stdio / HTTP / WS 投递。每帧消费 `frame.ack`。
3. **凭证与设置供值**（必需）：`FEISHU_APP_ID / FEISHU_APP_SECRET /
   FEISHU_DOMAIN`（env 约定与配置文件二选一）；呈现设置经
   `settings` 注入 theme（panel_expanded、print_strategy、text_sizes、
   reasoning_level 等）。
4. **降级履行**（必需）：收到 `ack.verdict ∈ {fallback, degraded}` 必须
   用 agent 原生通道投递该消息。**消息永不丢由两侧共同保证**。

红线：接入方不得绕过协议直调渲染/传输层；不得假设 ack 时序
（stdio/WS 是异步回执，HTTP 是同步回执）。

## 2. 形态 A · in-process SDK（Python agent，hermes 路线）

```python
from lark_streaming import StreamingEngine

engine = StreamingEngine(
    credentials=FeishuCredentials.from_env(),
    settings=my_settings,          # theme 供值
)
engine.start()

# agent 出站钩子里：
ack = engine.submit({
    "v": 1, "event": "answer.delta", "frame_id": "f_42",
    "turn_id": turn_id, "chat_id": chat_id,
    "payload": {"delta": chunk},
})
if ack["verdict"] != "consumed":
    native_send(chat_id, chunk)   # 降级履行
```

- 同步 ack（进程内直调）；引擎内部 per-chat 队列异步消费，ack 在帧
  受理与消费两个语义间以 `accepted_at` / `verdict_at` 区分（v1 简化：
  submit 同步等待消费完成，超时可配）。
- hermes 无需网络跳；引擎崩溃 = 进程崩溃 = 网关整体降级（宿主原生
  通道不受影响——本平台只做出站增强）。

## 3. 形态 B · 网关（非 Python agent，openclaw/codex 路线）

常驻进程，三种绑定并存（01-protocol §3）：

- stdio：托管桥接子进程（openclaw 默认形态）
- HTTP：`POST /v1/events`（脚本/低频）
- WS：长连接（高频多 chat）

网关进程持有全部引擎状态；接入方进程崩溃不影响网关，重连后以
`turn_id` 继续投递（引擎按 §5.1 处理新 turn）。网关崩溃 = 全部活跃卡
走 seal_all_on_shutdown + 接入方收 `lifecycle.event{shutdown}` 后转原生。

## 4. 设置供值（theme 与引擎参数）

| 设置 | 默认 | 说明 |
|------|------|------|
| `panel_expanded` | true | 面板默认展开 |
| `print_strategy` / `print_step` | delay / 4 | 打字机节奏 |
| `card_ttl_sec` | 600（min 60） | GC TTL |
| `watchdog_idle_sec` | 300（60~1800） | 看门狗静默阈值 |
| `clarify_suspend_sec` | 1.2 | 澄清窗口 |
| `card_text_sizes` | {} | 字号（空 = 不注入 style） |
| `footer_reasoning_level` | "" | 页脚推理等级（空 = 跳过） |
| `degraded_cooldown_sec` | 429→60 / 其他→30 | 降级冷却 |

## 5. 参考接入方计划

### 5.1 hermes（形态 A，Phase 4 交付；Phase 6 双跑对照修订）

- 适配器翻译面：hermes 网关出站流（send/edit/interim/notify 元数据契约）
  → 事件流：
  - `expect_edits=True` 出站 → `turn.start` + `answer.snapshot` 序列；
    **同显式 `turn_id` 重发 = 幂等续流**（宿主携回合身份；无 turn_id 的
    重复 expect_edits = 新回合越权，引擎 seal_and_create 兜底）
  - 编辑流 final → `answer.final`
  - `_interim_send` 进度行 → `tool.*` / `thinking.*` / `interim`（经
    legacy 转换器）
  - `notify=True` → `answer.final`（无活跃回合 = 微回合自愈）；
    **`job_id` 帧（cron 家族）→ `deliver` 静态卡 + `footer.job_id`
    溯源**（官方包装头解析，前代 R1 规则对齐；Phase 6 F3）
  - 无标记发送 → **进度启发式升格**（emoji 工具行/围栏块 → 面板，
    前代 v2.0.1/v2.0.4/v2.0.8/v2.0.31 主链对齐；Phase 6 F2），
    未命中启发式 → `deliver`
  - 平台通知（⚠️ 子代理失败/超时前缀）→ `notice`
- hermes 私有 API 访问继续收口在适配器内单点（前代 compat 先例），
  核心层零感知。
- clarify：hermes `send_clarify` → `clarify.request`；交互回调 →
  `clarify.answer` 回流。**澄清窗口宽限（Phase 6 F1）**：边界 finalize
  → send_clarify（实测 <200ms 跟随）经适配器宽限册触达引擎
  `clarify_window_hit`（同卡 PAUSED + 注册表重开续流）；窗口已过 →
  原生 ask 履行（native_text 携带问题文本，内容永不丢）。
- **不做继承**：前代继承内置 `FeishuAdapter` 的入站能力留在 hermes 侧
  不动；本适配器只挂出站钩子。入站（WebSocket/webhook/媒体/门控）
  由 hermes 原生平台继续负责。

### 5.2 openclaw（形态 B，Phase 4 交付）

- bridge 子进程跑网关 stdio 绑定；openclaw 侧把出站事件投递进来：
  - `metadata.streaming=True` → `answer.delta`（若为全量则 snapshot）
  - `metadata.finalize=True` → `answer.final`
  - 无标记 → `deliver`
  - 进度/通知模板按 openclaw 实际输出补齐映射器（接入时以官方输出为准）
- 参考 MVP 验收：一轮问答流式卡 + 一次 deliver 静态卡 + 一次降级回退。

### 5.3 其他 agent（codex / Claude 等）

按 §1 四件事接入；codex/Claude 类 CLI agent 建议 WS 绑定（宿主进程外
网关常驻）。接入清单由 ADAPTER_SDK（Phase 4 随代码发布）承载。

## 6. 配置面（env 优先级 > 配置文件 > 默认值）

```
FEISHU_APP_ID / FEISHU_APP_SECRET   # 凭证（必需）
FEISHU_DOMAIN=feishu|lark           # 站点（默认 feishu）
LSP_GATEWAY_BIND=stdio|http|ws      # 形态 B 绑定（可多值）
LSP_GATEWAY_PORT=7800               # HTTP/WS 端口
LSP_LOG_LEVEL=INFO
```

配置文件（`lsp.yaml`）支持全部设置键（§4）与 env 同名覆盖。

## 7. 版本兼容承诺

- 引擎对协议 v1.x 全兼容（未知事件 fallback 不崩）；接入方对
  `lifecycle.event` 必须容忍未知 type。
- 前代迁移承诺：`_legacy_text.py` 转换器语义与前代分类器一致（04 §4
  legacy 兼容基线验收）；hermes 切换 = 换适配器，业务侧零改动。
