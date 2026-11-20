<div align="center">

# lark-streaming-platform

**独立的飞书 / Lark 流式卡片平台 — 任何 agent 通过统一事件协议接入**

[![version](https://img.shields.io/badge/version-0.0.1-blue)](CHANGELOG.md)
[![python](https://img.shields.io/badge/python-3.11%2B-informational)](#)
[![license](https://img.shields.io/badge/license-MIT-yellow)](LICENSE)
[![status](https://img.shields.io/badge/status-Phase%200%20设计阶段-orange)](docs/design/00-overview.md)

*统一事件协议 · 显式回合状态机 · 零宿主依赖 · 消息永不丢*

</div>

---

## 这是什么

一个**独立**的飞书 CardKit 2.0 流式卡片平台：把「agent 出站事件流」翻译成
「飞书流式卡片体验」（打字机回答、工具与思考实时面板、澄清交互、回合外投递）。

**任何 agent**——hermes、openclaw、codex、Claude 或其他——只要能把出站事件
按统一协议投递进来，就获得完整的流式卡片能力。hermes 是第一个接入方，
不是地基。

### 与前代项目的关系

前代 [hermes-lark-streaming](https://github.com/Aowen-Nowor/hermes-lark-streaming)
（v2.1.x）是以 hermes 内置飞书平台为地基的插件实现。本项目是其架构终点
的从头重设计：

- 前代生产验证的全部平台知识（CardKit 怪癖、竞态事故档案、性能守恒）
  **冻结**在 [`docs/design/04-knowledge-freeze.md`](docs/design/04-knowledge-freeze.md)
- 前代在双跑对照通过后进入维护模式；切换前 hermes 生产继续跑前代插件
- 版本从 0.0.1 起步：0.0.x = 设计与骨架，0.1.x = 引擎可用，1.0 = 生产切换

## 架构（五层，依赖单向向下）

```
┌ 接入层   adapters/hermes | openclaw | codex | …（每接入方只写这层）
├ 协议层   统一事件信封（JSON over stdio / HTTP / WS）+ 多语言 SDK
├ 编排层   显式回合状态机 / 竞态守卫 / 会话续期 / 看门狗 / GC / 降级
├ 渲染层   版本化卡片 Schema + 主题 token → 纯函数渲染
└ 传输层   CardKit REST 客户端 / 限频 / 序列自愈 / 重试 / 字节预算
```

依赖方向红线：**下层永不 import 上层**；编排层及以下零 agent 感知、
零宿主 ABI 依赖。

## 部署双形态（同一 core）

| 形态 | 适用 | 说明 |
|------|------|------|
| **A · 库** | Python agent（hermes 等） | `pip install`，进程内直调引擎，零网络跳 |
| **B · 网关** | 非 Python agent（openclaw / codex 等） | 常驻服务，HTTP / WS / stdio 三种协议绑定 |

## 设计纪律

1. **可靠性第一（最高优先级）**——与飞书的交互一定不能出问题 >
   功能丰富度 > 通用性优雅度；必要时动用真实飞书环境验证（授权已获，
   见设计文档 02-engine §10 三层验证漏斗）
2. **设计先行**——全部规格冻结后才写引擎代码（Phase 0 是本仓库的锚点）
3. **知识冻结**——前代的伤疤（带事故日期）先变成规范与验收基线，再动代码
4. **架构动而语言不动**——不同时变更架构与语言，重写风险不乘法放大
5. **协议是永久契约，适配层是脚手架**——LSEP 是面向第三方的公开标准
   （官方仅凭文档即可接入）；现阶段适配层代官方做接入，未来官方
   原生接入后适配层整体退役

## 路线图

| 阶段 | 内容 | 状态 |
|------|------|------|
| Phase 0 | 设计文档（协议 / 状态机 / 卡片 Schema / 知识冻结 / 接入契约） | **完成（0.0.2）** |
| Phase 1 | 骨架 + 传输层（Feishu client，怪癖知识落地） | **进行中** |
| Phase 2 | 编排层（状态机 + 全部竞态守卫 + 降级） | 待开始 |
| Phase 3 | 渲染层（Schema + 主题 + 组件重写） | 待开始 |
| Phase 4 | 协议层 + 接入方（hermes SDK、openclaw bridge、编解码） | 待开始 |
| Phase 5 | 测试台架（mock 移植 + 行为验收基线跑通） | 待开始 |
| Phase 6 | 双跑对照 → 生产切换 | 待开始 |

## 文档索引

- [`docs/design/00-overview.md`](docs/design/00-overview.md) — 终态定义、范围、分层、里程碑
- [`docs/design/01-protocol.md`](docs/design/01-protocol.md) — 统一事件协议 v1（核心规格）
- [`docs/design/02-engine.md`](docs/design/02-engine.md) — 回合状态机与竞态守卫
- [`docs/design/03-card-schema.md`](docs/design/03-card-schema.md) — 卡片 Schema 与主题 token
- [`docs/design/04-knowledge-freeze.md`](docs/design/04-knowledge-freeze.md) — 前代知识冻结（怪癖表 / 事故档案 / 验收基线）
- [`docs/design/05-adapters.md`](docs/design/05-adapters.md) — 接入契约与双形态接入计划

## License

MIT
