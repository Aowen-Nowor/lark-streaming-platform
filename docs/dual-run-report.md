# 双跑对照报告（Phase 6，2026-11-20 16:47）

> 生成：`scripts/dual_run.py`（`run_corpus` 全语料） · 语料 12 场景 · 旧仓 `/home/z/work/old-hermes-ref` @ `c826ee8` · 新仓版本冻结 0.0.1（ADR D10）

## 结论速览

- **消息永不丢红线**：全部场景双侧零静默丢弃（每内容帧要么上卡要么原生履行）——降级场景 08 旧/新均 4 帧全原生。
- **F1 澄清窗口可达性（已修复）**：适配器宽限册使边界 finalize → send_clarify 链路触达引擎 `clarify_window_hit`；同卡续流为协议增强（场景 12，旧 edit-transport 结构性不可达）。
- **F2 无标记进度升格（已修复）**：前代 v2.0.1 升格主链移植，无标记工具气泡归面板（场景 03 散卡 2→0）。
- **F3 cron 家族溯源（已修复）**：job_id 帧路由 deliver + `footer.job_id`（场景 11，旧 R1 规则对齐）。
- **设计内分歧**：04 回合外 notify（旧 oneshot 静态卡 vs 新微回合卡）；澄清独立卡计数口径（旧 IM 内联不计 / 新 CardKit 实体计入）。
- **字节预算再评估**：维持 27000 不变（Phase 5 真火实测 stream 真实上限 100~120KB，保守裕度继续提供缓冲；放宽属用户裁决事项，04-knowledge-freeze §6 如实冻结）。

## 逐场景对照

| 场景 | 卡片 旧/新 | 原生回退 旧/新 | 终值 旧/新 | API 计数（旧→新） | 分类 |
|------|-----------|---------------|-----------|------------------|------|
| 01_full_turn | 1 / 1 | 0 / 0 | 1 / 1 | batch_update 2、create 1、reply 1、settings:close 1、stream_element 3 | equivalent |
| 02_progress_marked | 1 / 1 | 0 / 0 | 1 / 1 | batch_update 2→3 +、create 1、send 1、settings:close 1、stream_element 2、update 2→3 + | equivalent |
| 03_progress_unmarked | 1 / 1 | 0 / 0 | 1 / 1 | batch_update 2、create 1、send 1、settings:close 1、stream_element 2、update 1→2 + | equivalent (F2 fixed) |
| 04_notify_out_of_turn | 0 / 1 | 0 / 0 | 0 / 1 | batch_update 0→2 +、create 0→1 +、send 1、settings:close 0→1 +、stream_element 0→1 + | divergence-intended (D1) |
| 05_plain_out_of_turn | 0 / 0 | 0 / 0 | 0 / 0 | reply 1→0 -、send 0→1 + | equivalent (静态卡面) |
| 06_race_burst | 1 / 1 | 0 / 0 | 1 / 1 | batch_update 3→2 -、create 1、send 1、settings:close 1、stream_element 2、update 2 | equivalent (恰一卡) |
| 07_supersede_turn | 2 / 2 | 0 / 0 | 2 / 2 | batch_update 4、create 2、send 2、settings:close 2、stream_element 5→4 - | equivalent |
| 08_degrade_create_fail | 0 / 0 | 4 / 4 | 0 / 0 | create 0→1 + | equivalent (零丢失; D3 呈现粒度) |
| 09_clarify_pause | 2 / 3 | 0 / 0 | 2 / 2 | batch_update 4、create 2→3 +、send 3、settings:close 2、stream_element 4→5 +、update 0→1 + | equivalent (F1; D2 计数口径) |
| 10_multi_chat | 2 / 2 | 0 / 0 | 2 / 2 | batch_update 4、create 2、send 2、settings:close 2、stream_element 4、update 2 | equivalent |
| 11_cron_delivery | 0 / 0 | 0 / 0 | 0 / 0 | send 1 | equivalent (F3 fixed) |
| 12_clarify_same_card | — / 2 | — / 0 | — / 1 | 单侧（new） | capability (new-only) |

## 场景明细

### 01_full_turn

- 卡片数：旧 1 / 新 1
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'create': 1, 'reply': 1, 'stream_element': 3, 'batch_update': 2, 'settings:close': 1} → 新 {'token': 1, 'create': 1, 'reply': 1, 'stream_element': 3, 'batch_update': 2, 'settings:close': 1}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）

### 02_progress_marked

- 卡片数：旧 1 / 新 1
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'create': 1, 'send': 1, 'update': 2, 'stream_element': 2, 'batch_update': 2, 'settings:close': 1} → 新 {'token': 1, 'create': 1, 'send': 1, 'update': 3, 'stream_element': 2, 'batch_update': 3, 'settings:close': 1}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）

### 03_progress_unmarked

- 卡片数：旧 1 / 新 1
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'create': 1, 'send': 1, 'update': 1, 'stream_element': 2, 'batch_update': 2, 'settings:close': 1} → 新 {'token': 1, 'create': 1, 'send': 1, 'update': 2, 'stream_element': 2, 'batch_update': 2, 'settings:close': 1}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）

### 04_notify_out_of_turn

- 卡片数：旧 0 / 新 1
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'send': 1} → 新 {'token': 1, 'create': 1, 'send': 1, 'stream_element': 1, 'batch_update': 2, 'settings:close': 1}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）
- 旧侧路由注记：oneshot={"gate": "final", "chat": "oc_c1", "text": "后台任务完成：全量索引已重建，共 12847 条文档，耗时 3m12s。"}

### 05_plain_out_of_turn

- 卡片数：旧 0 / 新 0
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'reply': 1} → 新 {'token': 1, 'send': 1}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）
- 旧侧路由注记：oneshot={"gate": "longform", "chat": "oc_c1", "text": "今日站会纪要：1) 版本冻结纪律重申；2) 双跑对照本周收口；3) 生产切换待用户裁决。"}

### 06_race_burst

- 卡片数：旧 1 / 新 1
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'create': 1, 'send': 1, 'stream_element': 2, 'update': 2, 'batch_update': 3, 'settings:close': 1} → 新 {'token': 1, 'create': 1, 'send': 1, 'update': 2, 'stream_element': 2, 'batch_update': 2, 'settings:close': 1}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）

### 07_supersede_turn

- 卡片数：旧 2 / 新 2
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'create': 2, 'send': 2, 'stream_element': 5, 'batch_update': 4, 'settings:close': 2} → 新 {'token': 1, 'create': 2, 'send': 2, 'stream_element': 4, 'batch_update': 4, 'settings:close': 2}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）

### 08_degrade_create_fail

- 卡片数：旧 0 / 新 0
- 原生回退：旧 ['composer_none', 'orphan_edit_native', 'composer_none', 'orphan_edit_native'] / 新 ['create_failed:200860', 'chat_degraded', 'chat_degraded', 'chat_degraded']
- API 计数：旧 {} → 新 {'token': 1, 'create': 1}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）

### 09_clarify_pause

- 卡片数：旧 2 / 新 3
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'create': 2, 'send': 3, 'stream_element': 4, 'batch_update': 4, 'settings:close': 2} → 新 {'token': 1, 'create': 3, 'send': 3, 'stream_element': 5, 'update': 1, 'batch_update': 4, 'settings:close': 2}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）
- 旧侧路由注记：oneshot={"gate": "clarify", "chat": "oc_c1", "text": "迁移目标是测试环境还是生产环境？"}

### 10_multi_chat

- 卡片数：旧 2 / 新 2
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'create': 2, 'send': 2, 'stream_element': 4, 'update': 2, 'batch_update': 4, 'settings:close': 2} → 新 {'token': 1, 'create': 2, 'send': 2, 'stream_element': 4, 'update': 2, 'batch_update': 4, 'settings:close': 2}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）

### 11_cron_delivery

- 卡片数：旧 0 / 新 0
- 原生回退：旧 无 / 新 无
- API 计数：旧 {'send': 1} → 新 {'token': 1, 'send': 1}（新侧多 1 次 token 获取为传输层惰性管理，语义无差）
- 旧侧路由注记：oneshot={"gate": "cron", "chat": "oc_c1", "text": "Cronjob Response: 每日索引\n(job_id: 967430ee4ab2)\n--------------------------------\n索引完成：共 12847 条文档，增量 342 条，耗时 3m12s。"}

### 12_clarify_same_card

- 单侧场景（capability (new-only)）；旧侧结构不可达或语料声明单侧。

## 发现项登记（处置记录）

| # | 场景 | 发现 | 处置 | 状态 |
|---|------|------|------|------|
| F1 | 09/12 | 边界 finalize 后适配器注册表已注销，send_clarify 触达不了引擎澄清窗口（L2 已证语义，适配器够不着） | 适配器宽限册（最近关闭回合 + 时间戳），窗口命中重开注册表，过期 → 原生 ask（native_text 携带问题文本） | **已修复**（tests/adapters TestDualRunFindings + 场景 09/12） |
| F2 | 03 | 无标记工具气泡（edit-transport 真实形态）按冻结契约投成散卡（旧侧升格进面板） | `lt.upgrade_plain` 移植前代 v2.0.1/v2.0.4/v2.0.8/v2.0.31 升格主链，CH_PLAIN 先过启发式再落 deliver | **已修复** |
| F3 | 11 | cron 家族（job_id）走微回合卡，官方包装头上卡原文、job_id 溯源丢失 | `lt.parse_cron_delivery` 移植 + `_deliver_cron`路由（标题/正文剥离 + footer.job_id） | **已修复** |
| D1 | 04 | 回合外 notify：旧 R2 oneshot 网关静态卡 vs 新微回合卡（含页脚统计与面板） | 设计内分歧（02-engine 微回合自愈），零丢失等价 | divergence-intended |
| D2 | 09 | 澄清独立卡计数口径：旧 IM 内联卡不计 cards，新 CardKit 实体计入 | 设计形态一致（双侧独立澄清卡），仅测量口径差 | divergence-intended |
| D3 | 08 | 降级原生回退的 reason 呈现粒度不同（旧 composer_none/orphan_edit_native vs 新 create_failed/chat_degraded 冷却快路径） | 结果等价（全原生零丢失）；新侧原因更结构化（守卫裁决表） | divergence-intended |

## clarify MANUAL 项与生产切换前置

- **回调授权失败码集**：仍标 MANUAL（04-knowledge-freeze §6）——依赖真实 hermes 网关回调接线，双跑 mock 环境不可实证；生产切换（M5）冒烟清单项。
- **hermes 接线决策**：澄清续流后宿主是否携带 turn_id（同卡续流，场景 12 语义）由 hermes 侧出站钩子决定；两形态（同卡 / 新卡）均零丢失，ADAPTER_SDK 已载明。
- **生产切换（M5）前置就绪**：行为验收基线 21 项全绿（Phase 5）+ 双跑对照 12 场景零丢失 + 发现项 F1/F2/F3 修复并回归全绿；切换时点与版本变更由用户裁决（ADR D10）。
