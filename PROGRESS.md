# PROGRESS — 经验教训沉淀

## 2026-07-15 用 CCM Worker 系统更新框架 · P1（task-ccm-sync）

**背景**：CCM（Claude-Code-Manager）当初借鉴本框架的 Worker 系统，之后独立演进出大量 PTY/凭证运行时经验；本框架落后。二者已不共享代码，但共用同一上游 `claude-pty`。本次先落地风险最低、价值最高的 P1。

**做了什么**：
1. **claude-pty bump `88b77ad → d6ff732`**（uv.lock）。本框架 pin 落后上游约 30 个提交；`88b77ad` 是 HEAD 的直系祖先。中间含多个影响既有功能的修复：subagent autonomous-callback 在 PTYEvent 上崩溃、cold-resume 强制 stdin、idle reaper、轮换 exit handler 孤儿 proxy、首启 theme-picker/登录方式 drain 修复等。
2. **orphan / autonomous 守卫**（`pty_backend.py::on_event`）——**修确认的潜在 bug**。此前 `on_event` 完全不看 `orphan`/`autonomous` 标记，而这两个字段在 pin 的 `88b77ad` 就已存在。后果：温热会话 resume 时 claude-pty 重放上一 turn 的 JSONL backlog（orphan），旧 api_error 被重新标 turn-fatal → 毒化本 turn 合成的 result → 刚成功的 turn 被误报 failed；autonomous 子 agent 的 session_id 还会覆盖主 task 的。修法照搬 CCM `_process_event` 的 `turn_scoped = not orphan and not autonomous` 门：二者不参与 session_id/错误/`_saw_result`/`_saw_claude_output` 记账；orphan 事件另从转发流丢弃。

**遇到的坑**：
- 主 venv **没装 claude-pty**（`pty` extra 未 sync），导致 `@pty_required` 事件测试长期被 skip。装上后暴露 **4 个 pre-经年失效的 stale 测试**——它们断言 `usage limit reached → pty_turn_error`，但 `classify_turn_error` 早已把限速文案重分类为 `claude_rate_limited`（一直被 skip 没人发现）。已改用泛化错误文案解耦分类器，并补 `classify_turn_error` 的限速/泛化分支专测。
- worktree 里跑测试要用主 venv 的 python 且 pytest 的 `pythonpath=["src"]` 会解析到 worktree 的 src；`uv pip install --python <主venv> pytest pytest-asyncio` 装测试依赖，claude-pty 用 pinned URL@d6ff732 装进主 venv 才能真正跑事件测试。

**测试**：`tests/unit/test_pty_backend.py` 63 passed（含 3 个新 orphan/autonomous 回归 + 2 个新 classify）；PTY 相关子集（pty_backend/worker_runtime/task_router/protocol/agent_type/bootstrap_steps）208 passed。

**待续（P2/P3）**：瞬时过载同号退避重试、限速 actionability + assistant 文本兜底、hardlink 会话迁移保温热会话、首启 warmup、前端批次编排。

**Commit**: 见本节合入的 commit。

## 2026-07-15 自动登录同步成 CCM 版本 · worker 本地登录（task-ccm-sync）

**背景**：用户要求「凭证在 worker 那里登录，Manager 只管 email+接码token 分配」，且「自动登录逻辑同步成 CCM 版本」。原 `claude_oauth.py` 是 Manager 侧经 SSH 反向驱动 worker 上 Chrome 的 CDP 状态机（自研 `_CDPClient` + 只支持 171mail）；CCM 的 `auto_login.py`+`cdp_login.py` 是 **worker 本地自洽**脚本（纯 Chrome CDP、多后端接码、按域名自动选），更贴合意图。

**做了什么**：
1. **Vendored** CCM 的 `auto_login.py`+`cdp_login.py` 到 `src/elastic_agent/worker/login/`（near-verbatim，只把 CCM 特定硬编码参数化：`CLAUDE_MAILCATCHER_URL`/`CLAUDE_171MAIL_URL`/`CLAUDE_SETTINGS_EXTRA_DIRS`——原来写死了 `mail.claude-code-manager.com` 和 `/home/ubuntu/Claude-Code-Manager`）。
2. **重写 `ClaudeOAuthProvider`** 为薄壳：删掉自研 `_CDPClient` + 全部 Manager 侧 CDP/接码/CF 机器（~850 行），改为委派给 vendored `perform_login`——`worker_host` 有值时经 SSH 在 worker 上跑 `python -m elastic_agent.worker.login.auto_login`（脚本起 Xvfb:99），无值时进程内直跑；登录后读回凭证填 `LoginResult`。**保留**所有被 quota_checker/runtime 依赖的模块级符号（`refresh_access_token`/`read_credentials`/`write_credentials`/`OAUTH_CLIENT_ID`/`ANTHROPIC_USAGE_URL`）和 `OAuthConfig`(+`provider` 字段)/`LoginResult` 契约 → `CredentialLoginService/Step` 零改动。

**遇到的坑 / 注意**：
- `httpx`+`websockets` 已是顶层依赖，vendored 脚本无需加依赖；`mitmproxy`/`playwright` 仅在函数内 lazy import + `from __future__ import annotations` 使类型注解不求值，故 worker 无须装它们（当前 CDP 路径也不用）。
- 删内部方法后 `test_claude_oauth.py`/`test_oauth_race_and_retry.py` 有 18 个测旧 CDP 内部的用例失效——已重写为委派契约测试（local 成功/失败/无凭证/超时/provider 传递、remote 构造 worker 命令 + SSH 失败透传）+ 删 3 个测已删内部方法的用例。顺带修 `test_defaults` 的 stale 断言（login_timeout 240→480）。
- **未在本沙箱端到端验证浏览器登录**（无 Chrome/真账号/接码可达）；风险集中在 vendored 的 CCM 已验证代码，我方胶水（SSH invoke + LoginResult 映射）已单测覆盖。**合并前需在真 worker 上验证一次真登录**。

**测试**：`test_claude_oauth.py` 24 passed、OAuth+credential+quota 子集 59 passed；全 unit 套件 1074 passed，失败/错误全部是既有环境性（fastapi/DryRunProvider abstract/reconnect timing），与本改动无关（均不 import 改动模块）。

**Commit**: 见本节合入的 commit。

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
