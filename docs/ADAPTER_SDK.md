# ADAPTER_SDK — 接入清单（Phase 4 随代码发布）

> 任何 agent 接入本平台只需完成本文四件事（05-adapters §1）；本清单是
> 接入的唯一必要材料——官方仅凭 LSEP 协议文档（`01-protocol.md`）亦可
> 直接接入（ADR D8），本 SDK 只是便利品，未来官方原生接入后本层整体
> 退役，协议与核心引擎永续。

## 1. 接入四件事总览

| # | 事项 | 形态 A（in-process） | 形态 B（网关） |
|---|------|---------------------|---------------|
| 1 | 事件映射器 | `HermesAdapter`（hermes 元数据契约） | `OpenclawMapper`（bridge 消息契约）/ 自写映射器 |
| 2 | 投递表面 | `engine.submit()` 进程内直调 | stdio 行协议 / `POST /v1/events` / WS `/v1/ws` |
| 3 | 凭证与设置供值 | `FeishuCredentials.from_env()` + `EngineSettings`/`Theme` | env / `lsp.yaml`（§5 配置面） |
| 4 | 降级履行 | `DeliveryResult.action == "native"` → 原生通道投递 `native_text` | 逐帧读回执，`verdict ∈ {fallback, degraded}` → 原生投递 |

红线（05-adapters §1）：

- 不得绕过协议直调渲染/传输层；
- **不得假设 ack 时序**——stdio/WS 是异步回执，HTTP 是同步回执；逐帧恰好一个 ack；
- `lifecycle.event` 必须容忍未知 type。

## 2. 形态 A · in-process（Python agent，hermes 路线）

```python
import asyncio
from lark_streaming.credentials import FeishuCredentials
from lark_streaming.engine import StreamingEngine
from lark_streaming.transport import FeishuTransport
from lark_streaming.adapters import HermesAdapter, NativeAction

async def main():
    transport = FeishuTransport(FeishuCredentials.from_env())
    engine = StreamingEngine(transport)          # theme/settings 可注入
    adapter = HermesAdapter(engine)              # 出站钩子翻译面
    try:
        # ── hermes 网关出站路径单点接线（私有 API 收口在宿主侧）──────
        # adapter.send(chat_id, content, metadata)      ← 网关 send 钩子
        # adapter.edit(chat_id, content, finalize=...)  ← 网关 edit 钩子
        # adapter.send_clarify(chat_id, ...)            ← ask 钩子
        # adapter.clarify_answer(chat_id, values=...)   ← 交互回调回流
        # adapter.abort(chat_id, reason=...)            ← 中断钩子
        res = await adapter.send(
            "oc_xxx", "答案增量", {"expect_edits": True},
            reply_to="om_xxx", title="任务",
        )
        if res.action is NativeAction.NATIVE:
            ...  # 降级履行：用 hermes 原生通道投递 res.native_text
        ...
    finally:
        await engine.aclose()   # seal_all_on_shutdown 先定稿再关流
        await transport.aclose()
```

### 2.1 元数据契约映射表（前代 v0.21.x 官方 metadata，逐字对齐；Phase 6 双跑对照修订）

| hermes 出站 | LSEP/1 事件 | 说明 |
|-------------|-------------|------|
| `send(expect_edits=True)` | `turn.start` + `answer.snapshot` | 回合首帧；活跃回合在场 = 新回合越权（引擎 seal_and_create）；**同显式 `turn_id` 重发 = 幂等续流** |
| `edit_message(finalize=False)` | `answer.snapshot` | 无活跃回合 = 孤儿编辑 → 原生履行 |
| `edit_message(finalize=True)` | `answer.final` | 定稿；回合终结（引擎开澄清窗口 T_pause） |
| `_interim_send` 进度行 | `tool.start` / `thinking.delta`(+`thinking.round.end`) / `interim` | 经 legacy 分类器；回合前暂存，turn.start 后并入 |
| `_interim_send` 通知行 | `notice` | 活跃卡并入面板；**无卡原生文本永不建卡**（跑马灯根因防线） |
| `send(notify=True)` | `answer.final` | 无活跃回合 = 微回合自愈（turn.start + final） |
| `send(job_id=…)`（cron 家族） | `deliver{title, text, footer.job_id}` | 官方包装头解析（标题/正文剥离）；前代 R1 规则对齐（Phase 6 F3） |
| 无标记发送（启发式命中） | `tool.start` / `thinking.delta` | 前代 v2.0.1 升格主链：emoji 工具行/围栏块归面板，防散卡刷屏（Phase 6 F2） |
| 无标记发送（未命中） | `deliver` | 回合外旁路静态卡（title/job_id 经 metadata） |
| 平台通知（⚠️ 前缀） | `notice{level=warn}` | 子代理失败/超时固定前缀 |

### 2.1.1 澄清窗口宽限（Phase 6 F1）

边界 finalize → `send_clarify`（hermes 实测 <200ms 跟随）期间适配器
注册表已注销。适配器内建**宽限册**（最近关闭回合 + 时间戳，窗口与
引擎 `clarify_window_sec` 对齐）：

- 窗口内：以原 `turn_id` 提交 `clarify.request` → 引擎
  `clarify_window_hit` 同卡 PAUSED → 注册表重开（续流帧归同卡）；
- 窗口已过 / 引擎 fallback：`DeliveryResult.action="native"` +
  `native_text=question`（hermes 原生 ask 通道，问题文本必达）；
- 澄清续流后宿主携原 `turn_id` 续投 = 同卡续流（v2.0.26 验收语义）；
  不携 = 新卡（supersede 落定），两形态均零丢失。

### 2.2 降级履行规则（§1.4，消息永不丢由两侧共同保证）

- `DeliveryResult.action == "consumed"`：全部内容帧已上卡，无需动作；
- `action == "native"`：必须用 hermes 原生通道投递 `res.native_text`；
  `res.turn_streamed=True` 表示卡上已有部分内容（全文重发 or 续发由
  宿主策略裁决）；`res.frames` 逐帧保真（审计用）；
- 控制帧（`turn.abort` / `clarify.answer`）的 fallback 无履行义务；
- 前代语义保真：降级是 chat 级（冷却期内所有帧快速 degraded），冷却
  期满下一帧即静默探测——适配层无需自行退避。

## 3. 形态 B · 网关（非 Python agent，openclaw / codex / Claude 路线）

### 3.1 启动

```bash
export FEISHU_APP_ID=cli_xxx FEISHU_APP_SECRET=xxx
python -m lark_streaming --bind stdio            # 托管桥接子进程（零依赖）
python -m lark_streaming --bind http --port 7800 # 脚本/低频
python -m lark_streaming --bind ws   --port 7800 # 高频多 chat（含 HTTP 端点）
```

### 3.2 stdio 行协议（fd 拓扑，01-protocol §3）

- **stdin** ：接入方写事件行（一行一个 JSON 信封）
- **stderr**：网关回 ack / error / `lifecycle.event` 行（同格式 JSON）
- **stdout**：运行日志（不承载协议流量）

bridge 主循环（openclaw 侧参考实现，Node 伪码）：

```js
// 映射器（Python 侧）: OpenclawMapper().translate(bridge_msg) → 信封序列
// bridge: openclaw 出站事件 → bridge_msg(JSON) → python -c 映射 → stdin
for (const msg of openclawOutboundQueue) {
  stdin.write(JSON.stringify(msg) + "\n");        // 事件行
}
stderr.on("data", line => {
  const wire = JSON.parse(line);
  if (wire.event === "frame.ack"
      && ["fallback", "degraded"].includes(wire.payload.verdict)) {
    nativeSend(wire.payload.frame_id);            // 降级履行（§1.4）
  }
  // lifecycle.event{type="shutdown"} → 转原生投递
});
```

> openclaw MVP 验收（05-adapters §5.2）：一轮问答流式卡 + 一次 deliver
> 静态卡 + 一次降级回退——三项端到端用例见
> `tests/adapters/test_integration.py::test_openclaw_bridge_full_flow`。

### 3.3 HTTP 绑定

```bash
curl -s http://127.0.0.1:7800/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"v":1,"event":"deliver","frame_id":"d1","chat_id":"oc_x","payload":{"text":"hi"}}'
# → {"v":1,"event":"frame.ack","payload":{"frame_id":"d1","verdict":"consumed"}}
```

- `POST /v1/events`：单事件同步 ack（业务失败恒 200 + verdict）
- `POST /v1/events:batch`：数组逐帧回执（顺序与请求一致）
- `GET /v1/health`：引擎观测面（`engine.snapshot_health` 同源）

### 3.4 WS 绑定（长连接）

- 连接 `GET /v1/ws` 后**首帧必须 hello**：
  `{"v":1,"event":"hello","frame_id":"h1","payload":{"protocol_v":1}}`
  → `hello.ack{protocol_v, engine_version, settings}`；协商失败关闭连接；
- 之后逐帧投递事件，ack 异步回写（不得假设时序）。

### 3.5 停机语义

网关收到 SIGINT/SIGTERM → 全部活跃卡 `seal_all_on_shutdown` 先定稿再
关流 → stdio 接入方经 stderr 收 `lifecycle.event{type="shutdown"}` 后
转原生；HTTP/WS 接入方以连接断开感知（在途帧已被引擎消费完）。

## 4. 事件映射器契约（自写映射器参考）

信封字段表（01-protocol §2 消费面逐项校验）：

```json
{
  "v": 1,
  "event": "answer.delta",
  "frame_id": "唯一 ID（全局）",
  "chat_id": "oc_xxx",
  "turn_id": "回合内必填（deliver 例外）",
  "ts": 1737400000000,
  "seq": 1,
  "payload": { "delta": "增量文本" }
}
```

payload 速查（引擎消费面）：

| event | payload |
|-------|---------|
| `turn.start` | `reply_to`（om_* 锚定）、`title` |
| `answer.delta` / `answer.snapshot` | `delta` / `text`（D5 双模，引擎统一为全量缓冲） |
| `answer.final` | `text`、`stats{duration_ms, model, context_tokens{used,max}, extra{reasoning_effort}}` |
| `tool.start` / `tool.end` | `tool`,`preview` / `tool`,`duration_ms`,`output_preview` |
| `thinking.delta` / `thinking.round.end` | `text` / `round`,`duration_ms` |
| `interim` / `notice` | `text` / `text`,`level(info\|warn\|error)` |
| `clarify.request` | `form{kind, question, options[], placeholder}` |
| `clarify.answer` | `values{}` |
| `deliver` | `text`、`title`、`footer{job_id}` |

## 5. 凭证与设置供值（env > 配置文件 > 默认值）

```bash
FEISHU_APP_ID / FEISHU_APP_SECRET   # 凭证（必需）
FEISHU_DOMAIN=feishu|lark           # 站点（默认 feishu）
LSP_GATEWAY_BIND=stdio|http|ws      # 形态 B 绑定（可多值，逗号分隔）
LSP_GATEWAY_PORT=7800               # HTTP/WS 端口
LSP_LOG_LEVEL=INFO
```

配置文件 `lsp.yaml`（`python -m lark_streaming --config lsp.yaml`）：

```yaml
feishu: {app_id: …, app_secret: …, domain: feishu}
gateway: {bind: [stdio, http], port: 7800, host: 127.0.0.1}
settings:
  watchdog_idle_sec: 300        # 看门狗静默阈值（钉制 60~1800）
  card_ttl_sec: 600             # 卡 TTL（min 60）
  clarify_suspend_sec: 1.2      # §4 冠名（= clarify_window_sec）
  degrade_cooldown_sec: {rate_limit: 60, other: 30}
theme:
  footer_reasoning_level: ""    # 页脚推理等级（空 = 跳过）
  text_sizes: {answer: "…", aux: "…"}
  panel_title_default: "agent loop"
  tool_icons: {}                # 品牌化整体替换
log_level: INFO
```

- 设置键接受 §4 冠名与引擎原名双轨（`clarify_suspend_sec` ≡
  `clarify_window_sec`；`footer_reasoning_level` ≡ `reasoning_level`；
  `card_text_sizes` ≡ `text_sizes`）；env 同名覆盖生效；
- 越界值一律钳制进钉制范围，不抛异常（配置错误不阻断消息主链）；
- `panel_expanded` / `print_strategy` / `print_step` 为渲染保留键：
  v1 接受但不接线（渲染器固定默认，诚实标注）。

## 6. legacy 文本兼容转换器（前代迁移专用）

前代接入方（文本启发式编码）零改动语义迁移：

```python
from lark_streaming.adapters import classify_send, classify_edit

result = classify_send(content, metadata)   # → channel/kind/tool/clean
# CH_STREAM → turn 生命周期帧；CH_PROGRESS → tool/thinking；
# CH_INTERIM(advisory) → notice；CH_FINAL → answer.final；CH_PLAIN → deliver
```

转换语义与前代分类器逐点一致（前代 v2.0.6/8/15/22/34 全部怪癖防线
内置：真实模板解析、300/600 双限、通知清单、`(×N)` 去重、裸围栏块
归属 terminal）；回归样本固化于 `tests/adapters/test_legacy_text.py`。

## 7. 版本兼容承诺（05-adapters §7）

- 引擎对协议 v1.x 全兼容（未知事件 fallback 不崩）；接入方对
  `lifecycle.event` 必须容忍未知 type；
- 信封字段缺失/类型错 → `fallback` ack（frame_id 尽力携带）；未知
  event → `fallback{detail=unknown_event:*}`；卡片 API 失败 →
  `fallback/degraded`（detail 携带飞书错误码）——**不存在静默失败的帧**。
