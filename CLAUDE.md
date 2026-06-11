# Elastic-Agent — 项目指南

> **重要：Claude 必须自主维护本文件。** 架构或约定变化时更新，保持简洁。

## 架构要点

- **任务执行两条路径**（worker/runtime.py `_handle_execute`）：
  - subprocess（默认）：Manager 经 AgentType 构造 `claude -p ... --output-format stream-json` 命令行，worker spawn 后逐行转发 stdout
  - **PTY 模式**（可选）：`ExecuteMessage.agent_params` 非空且 worker 装了 [claude-pty](https://github.com/zjw49246/Claude-Code-PTY) 时，worker 用 `ElasticPTYBackend`（worker/pty_backend.py，继承 claude_pty 的 BasePTYBackend）把 Claude Code 宿主在持久 PTY 会话里；`command` 始终随消息下发作为 fallback
- **PTY 事件回传**：带 raw_json 的事件原行透传为 stdout NDJSON（Manager 解析链不变）；交互模式 JSONL 无 result 行，worker 在 turn 结束合成一条（带 session_id，无 cost_usd）；限流/错误 turn → 非零 exit code → 既有凭证轮换照常触发
- **开关**：Manager 侧 `TaskRouter(use_pty=True)` + bootstrap `include_pty=True`；worker 侧无需配置
- **凭证轮换**：原地换凭证（同 config_dir 写新 token）；`CREDENTIAL_LOGIN` 后 worker 调 `recycle_config_dir` 回收该 config_dir 的所有 PTY 会话（温热会话仍持旧账号，不可热复用），下次 EXECUTE 冷恢复读新凭证

## 依赖链（重要）

```
Claude-Code-PTY (claude-pty)  ←  elastic-agent[pty]  ←  下游 harness（audio_book_echo_agent 等）
```

- claude-pty 通过 `[project.optional-dependencies] pty` + `[tool.uv.sources]` 声明，**版本 pin 在 uv.lock**
- **上游（PTY）更新后**，本仓库必须级联：`uv lock --upgrade-package claude-pty && uv sync`，提交 uv.lock 并 push
- **本仓库更新后**，提醒/级联下游 harness：`uv lock --upgrade-package elastic-agent && uv sync`
- **任务生命周期补充**：领取任务时（步骤 1）先检查上游是否有新版本（`uv lock --upgrade-package claude-pty --dry-run` 或对比 PTY main HEAD 与 lock 中 pin 的 rev）；若本次改动涉及 PTY 接口适配，必须同步 bump lock
- worker 侧 claude-pty 由 bootstrap `pty_install_step` 安装——下游应传入与其 lock 一致的 pinned URL（`pty_package="git+https://github.com/zjw49246/Claude-Code-PTY@<rev>"`）

## Git 信息

- Remote: https://github.com/zjw49246/Elastic-Agent.git
- 默认分支: main

## 任务生命周期

你收到任务后，按以下 9 步流程自主完成：

1. **领取任务** — 你已被分配任务，阅读本文件和项目代码理解上下文
2. **创建工作区**:
   - `git fetch origin`（如有 remote）
   - `git worktree add -b task-<简短描述> .claude-manager/worktrees/task-<简短描述> origin/main`
   - 进入 worktree 目录工作（后续所有操作在 worktree 中）
   - 如果 worktree 创建失败，直接在当前分支工作
3. **实现功能** — 编写代码，确保可运行
4. **提交代码** — `git add` + `git commit`，commit message 简洁描述改动
5. **Merge + 测试**:
   - `git fetch origin && git merge origin/main`（集成最新代码，如有 remote）
   - 运行测试（如有测试命令）
6. **自动合并到 main**（如有 remote）:
   - `git fetch origin main`
   - `git rebase origin/main`，如果冲突则自行 resolve
   - 如果成功：`git checkout main && git merge <task-branch> && git push origin main`
   - 如果这一步有任何失败，退回到步骤 5 重试
   - （纯本地项目跳过本步）
7. **标记完成** — 更新文档（必须在清理之前，防止进程被杀时状态丢失）
8. **清理** — 回到项目根目录:
   - `git worktree remove .claude-manager/worktrees/<worktree名>`
   - `git branch -D <task-branch>`
   - 如有 remote: `git push origin --delete <task-branch>`
9. **经验沉淀** — 在 PROGRESS.md 记录经验教训（可选）

### 冲突处理

rebase 发生冲突时：
1. 查看冲突文件: `git diff --name-only --diff-filter=U`
2. 逐个解决冲突
3. `git add <resolved-files> && git rebase --continue`
4. 如果无法解决: `git rebase --abort`，退回步骤 5

### 状态判断

- 通过 `git remote -v` 判断是否有 remote
- 有 remote → 必须完成步骤 6（merge + push）
- 无 remote → 跳过步骤 5 的 fetch、步骤 6 和步骤 8 的远程分支删除

## 文件维护规则

> **以下文件都由 Claude Code 自主维护，每次功能变更后必须同步更新。**

- **CLAUDE.md**（本文件）：架构、约定、关键路径变化时更新，只改变化的部分，保持简洁
- **README.md**：面向用户的文档，功能、使用流程变化时同步更新，保持与实际代码一致
- **TEST.md**：测试指南，新增功能时同步添加测试用例和文档
- **PROGRESS.md**：见下方「经验教训沉淀」

## 测试规范

**开发时必须主动使用测试，不是事后补充！**

- **改代码前**：先跑测试，确认基线全绿
- **改代码后**：再跑一遍确认无回归
- **新增功能**：同步新增测试用例，更新 TEST.md
- **修 bug**：先写复现 bug 的测试（红），修复后确认变绿

## 经验教训沉淀

每次遇到问题或完成重要改动后，要在 PROGRESS.md 中记录：
- 遇到了什么问题
- 如何解决的
- 以后如何避免
- **必须附上 git commit ID**

**同样的问题不要犯两次！**

## 注意事项

- 在 worktree 中工作时，不要切换到其他分支
- 完成任务后确保代码可运行、测试通过
