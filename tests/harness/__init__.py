"""L2 mock 台架 — 真实 FeishuTransport × mock HTTP 的行为验收（04-knowledge-freeze §5）.

定位（04-knowledge-freeze §5 / 02-engine §10 验证漏斗第二层）：前代
``tests/e2e/mock_feishu.py + framework.py`` 的移植——mock 飞书 REST 面
（token / create / send / reply / update / stream_element / batch_update /
settings 全端点）+ 可注入故障（429 / 300317 / 200850 / 网络挂起 / 确定性
拒收），跑 **04-knowledge-freeze §4 行为验收基线**（生命周期 / 竞态 /
clarify / 降级 / 渲染集成）。

与 L1（tests/engine，FakeTransport 直接伪装 transport 方法面）的边界：
本台架在 ``httpx.MockTransport`` 层应答——引擎与 transport 代码零感知、
零改动，路径 / wire 形态（data/actions/settings 均为 JSON 字符串）/
token 管理 / retry_transient 退避 / seq 自愈 / 错误解析全链路在环。
"""
