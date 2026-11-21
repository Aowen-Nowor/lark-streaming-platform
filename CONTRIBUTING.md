# 贡献指南

欢迎为 lark-streaming-platform 做贡献！无论是报 bug、提功能建议、补文档，还是直接提交代码，都欢迎。

## 提 Issue

- 先搜索一下已有的 Issue，避免重复。
- 报 bug 请用「Bug 报告」模板，写清复现步骤、期望结果、实际结果、环境（Python 版本、安装方式、绑定形态 stdio / HTTP / WS）。
- 功能建议请用「功能建议」模板，写清解决的问题和使用场景。
- 安全问题请不要公开提 Issue：涉及密钥、凭证、鉴权绕过的问题，请先私下联系维护者，不要把真实凭证贴进 Issue。

## 提 PR

1. Fork 本仓库，基于 `main` 分支新建分支（分支名如 `fix/xxx`、`feat/xxx`）。
2. 改动尽量小而聚焦，一个 PR 只做一件事。
3. 提交前本地跑通测试（见下）。
4. 提交 PR 时按模板填写：改了什么、为什么改、怎么测的。
5. 大的架构或协议改动请先提 Issue 讨论，设计先行：先更新 `docs/design/` 对应文档，再动代码。

## 本地开发

```bash
git clone https://github.com/Aowen-Nowor/lark-streaming-platform.git
cd lark-streaming-platform
python3 -m venv .venv && source .venv/bin/activate  # Python >= 3.11
pip install -e ".[dev,gateway]"
pytest  # 全套测试
```

- `pytest`：单元 + L2 mock 台架测试，不需要真实飞书凭证。
- `python3 scripts/e2e_real.py --mock`：零写干跑。
- 真飞书验收（L3）需要飞书应用凭证，默认不跑；见 README「真飞书验收」一节。

## 代码规范

- 五层架构、依赖单向向下：下层永不 import 上层；编排层及以下零 agent 感知、零宿主 ABI 依赖。
- 保持现有的错误分类与重试分道；新增飞书错误码先入 `transport/errors.py` 的码表，再进 mock 与测试。
- 日志只记 id 与错误码，不记消息内容与密钥；新增日志先过 `errors.py` 的脱敏。
- 不要提交真实凭证：`app_id` / `app_secret` / token 只放环境变量，不要写进 `lsp.yaml` 后提交，也不要提交 `.env` 文件。
- 版本号不自动 bump：任何情况下都不要改 `pyproject.toml` 里的版本号（ADR D10，版本号由维护者明示变更）。

## 行为准则

友善、就事论事。欢迎新手提问，第一次贡献遇到问题可以直接在 Issue 里问。
