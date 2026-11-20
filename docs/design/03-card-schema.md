# 03 · 渲染层 — 卡片 Schema v1 与主题 token

> 渲染是纯函数：`render(state: RenderState, theme: Theme) → CardKit JSON`。
> 无 IO、无时钟、无随机——同输入恒同输出，快照可测。

## 1. Schema 定位

**不使用飞书 template_id 变量卡**：流式模式要求实体卡（entity-based），
变量卡装不下流式会话。模板定义 = 本 Schema（版本化结构声明）+ 渲染函数。
接入方换品牌只换 theme，不碰渲染代码。

## 2. 卡片结构（card_schema: v1）

```
┌─────────────────────────────────────────┐
│ header   标题（动态：回合进行中/已定稿/中止）│
├─────────────────────────────────────────┤
│ panel    agent loop 状态面板（懒渲染）     │
│   · 面板标题（当前活动描述 + · 总耗时）    │
│   · thinking 分组行（按轮，轮标题+气泡）    │
│   · tool 行（官方图标 + spinner/耗时标签） │
│   · interim 行 / notice 行               │
├─────────────────────────────────────────┤
│ answer   流式 markdown 正文（打字机）      │
├─────────────────────────────────────────┤
│ clarify  交互区（仅澄清回合，见 §5）       │
├─────────────────────────────────────────┤
│ footer   统计页脚（定稿后）：耗时/模型/    │
│          上下文/ job_id 溯源/中止原因      │
└─────────────────────────────────────────┘
```

槽位规格：

| 槽位 | 内容 | 流式行为 |
|------|------|----------|
| `header` | 状态标题 | 建卡时初始 → 面板标题动态更新 → 定稿收敛 |
| `panel` | 状态行列表 | 懒渲染：首行到达前无面板；原地改写走 0.2s 合并窗口，新增行即时 |
| `answer` | markdown 正文 | `cardkit_stream_element` 打字机；定稿终值 patch（内容与最近流式值一致时跳过，OPT-4） |
| `clarify` | 表单控件 | 澄清回合插入；三态反馈（待答/已答/未授权） |
| `footer` | kv 页脚 | SEALING 阶段一次性插入；重复定稿不重放（`footer_sealed` 语义） |

## 3. 主题 token（Theme）

| token 组 | 内容 | 前代来源 |
|----------|------|----------|
| `icons` | 工具名 → 官方图标映射 | cardkit/tool_icons.py（180 行映射表整体迁移） |
| `i18n` | 面板/页脚/状态文案（zh 默认，可扩展） | cardkit/i18n.py |
| `text_sizes` | 字号自定义（opt-in；空 = 不注入 style） | v2.0.20 card_text_sizes |
| `reasoning_level` | 页脚推理等级（空 = 跳过该字段） | v2.0.19 footer_reasoning_level |

Theme 经设置供值注入（05-adapters §4），核心层中性默认。

## 4. 流式生命周期绑定（CardKit API 序列）

```
cardkit_create ─→ send_card_by_id ─→ stream_element(answer) ─→ update(面板)
                                                          │
              clarify: update(暂停行) ⇄ resume 续流        │
                                                          ▼
                                        close_streaming ─→ update(终值+页脚)   ← 两步定稿
```

- 每次更新携带单调递增 `seq`；300317 冲突由传输层自愈重开（04-knowledge-freeze Q1）。
- 流式会话龄 ≥540s 由引擎先续期（02-engine §2）。

## 5. clarify 表单 Schema

```json
{
  "form": {
    "kind": "select | multi_select | input",
    "question": "…",
    "options": [ {"label": "…", "value": "…"} ],   // select/multi_select
    "placeholder": "…",                              // input
    "allow_custom": false
  }
}
```

三态反馈渲染：`pending`（表单可交互）→ `answered`（按钮置灰/输入锁定，
展示应答摘要）→ `unauthorized`（操作者授权校验失败提示，fail-closed）。

## 6. 字节预算（渲染边界强制）

- 上限 **27000 bytes**（前代 `_CARD_BYTE_BUDGET`，生产实测值冻结）。
- 超限裁剪顺序：interim 行 → thinking 历史轮（保留当前轮）→ answer 尾部
  截断（附「…（已截断）」标注）；页脚与 answer 终值永不裁剪。
- 裁剪发生必须打点（观测面），Phase 6 对照裁剪频次。

## 7. markdown 卫生化与行解析

- 流式 answer 的 markdown 卫生化：缓存键 = 原文精确串（单槽）；
  面板行卫生化：逐行精确串缓存（行合并后不可变，命中率近恒 1）（OPT-5）。
- 面板行解析（tool/thinking/interim 识别、去重后缀 `(×N)`、耗时标签）：
  协议 v1 下输入已是结构化事件，解析器仅作 legacy 文本转换器
  （05-adapters §5）的内部件，不再暴露给核心层。

## 8. 快照测试契约

渲染纯函数是回归测试的主体：`tests/renderer/` 对每个槽位组合维护
Golden 快照（含：空面板 / 首行 / 多轮思考 / 工具合并 / 截断 / clarify
三态 / 页脚全字段）。任何渲染变更必须显式更新快照并在 CHANGELOG 说明。
