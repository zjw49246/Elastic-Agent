# PROGRESS — 经验教训沉淀

## 2026-06-11 PTY 框架支持（task-pty-support）

**做了什么**：worker 支持用 claude-pty 把 Claude Code 宿主在持久 PTY 会话中执行任务，替代每任务 spawn `claude -p`。Manager 侧只加了可选的 `ExecuteMessage.agent_params`（向后兼容）+ `TaskRouter(use_pty=True)` 开关；PTY 仓库零改动。

**关键设计决策**：
1. **raw_json 透传**：claude_pty 的 PTYEvent 保留 JSONL 原始行，worker 直接当 stdout NDJSON 转发 → Manager 的 `_try_parse_ndjson` / LogEventParser 解析链完全不用动。不要自己造事件映射表。
2. **合成 result 行**：交互模式 JSONL 没有 `result` 行（回合结束是 `system/turn_duration` 哨兵），但 Manager 靠 result 事件提取 session_id——worker 在 turn 结束时合成一条（`synthesized_by: "pty_backend"`）。注意 `cost_usd` 在 PTY 模式拿不到。
3. **错误 turn 强制非零 exit**：API error / 限流把 turn 标记 error 但进程不死；on_exit 把 exit_code 0 改成 1，否则 Manager 侧凭证轮换不触发。

**遇到的坑**：
- venv 的 editable 安装指向主仓库 src，worktree 里跑测试改动不生效——必须 `PYTHONPATH=src` 覆盖。
- **同机多 BridgeHub 串话**：冒烟测试在本机起第二个 BridgeHub，channel 注入打进了同机另一个已存在的 PTY 会话（注入端口撞了），目标会话靠 15s stdin fallback 兜底成功。生产 worker 单 backend 无此问题，但同机多 backend 部署前要先解决端口/会话路由隔离。
- 仓库基线测试本就有 43 failed / 91 errors（环境性，fastapi/oauth 相关）；本任务相关子集（runtime/router/protocol/agent_type 等 7 个文件）基线 154 passed 全绿，改后 185 passed。

**测试**：tests/unit/test_pty_backend.py（31 个用例）+ 真实 claude 端到端冒烟（assistant 原行透传、合成 result 带 session_id、exit 0 验证通过）。

**Commit**: 见 git log（task-pty-support 合入 main 的 commit）。

## 2026-06-11 PTY Phase 2：热会话 follow-up + 注入串话修复（验证）

**结论**：elastic-agent 侧零代码改动。`BasePTYBackend.launch(resume_session_id=...)` → pool `get_or_create` 命中存活会话即热复用；本仓库的 `on_exit` 只清 task 级映射、不动 pool，会话保持温热。

**验证**（真 claude 双 turn 冒烟）：
- turn 1 冷启动 14s；turn 2 同 session/同 PID 注入新 turn，5s 完成
- 两个 turn 都走 channel 注入（无 stdin fallback），合成 result 带 session_id，exit 0
- 串话不再出现——修复在 PTY 仓库（commit aa23aab）：inject 端口 OS 分配 + /inject 校验 session_id（不匹配 409）+ bind 失败不崩 MCP

**注意**：worker 需要 claude-pty >= aa23aab；旧版在同机多宿主下有注入串话风险（消息可能漏进别的会话且发送方以为成功）。

## 2026-06-11 PTY Phase 3：凭证轮换 × 温热会话（recycle_config_dir）

**缺口**：轮换是原地换凭证（新账号 token 写进同一 config_dir）。subprocess 路径每任务新进程重读凭证没问题；PTY 温热会话是旧账号启动的、一直带着旧凭证活着——follow-up 热复用会继续烧已耗尽的账号。

**解决**：`ElasticPTYBackend.recycle_config_dir(config_dir)`——回收该 config_dir 上所有会话（有任务的走 stop()，Manager 收到 PROCESS_EXIT；纯温热的直接 pool.remove）。worker 在 `_handle_credential_login` 写完新凭证后调用；回收失败不影响登录结果上报。

**验证**：6 个新单元测试 + 真 claude 冒烟（turn1 温热 → recycle → turn2 resume 冷恢复新 PID、干净完成）。

**附带发现**：冒烟首跑因 API 529 overloaded_error 失败（CC 内部重试 10 次未过）——`system/api_error` 是 CC 的重试事件（带 retryAttempt/maxRetries），不带 isApiErrorMessage，turn 未被掐断，等待即可恢复。区别于 isApiErrorMessage:true（turn 被终止、无哨兵）。

**Commit**: 见本节合入 main 的 commit。
