# Audiobook x Elastic-Agent 方案缺陷分析与补充

> 本文档记录对 Elastic-Agent MVP 方案、Audiobook Harness 方案、audio_book_echo_editor 适配方案的全面审查结果。
> 每个问题包含：现状描述、影响评估、补充方案。

---

## 1. 分析方法

- 对照三份文档（MVP 方案、Harness 示例、跑书双引擎方案）逐段交叉检查
- 与 audio_book_echo_editor 真实代码库（数据模型、API、工作流配置）对比验证
- 模拟端到端场景（新书生产、修改、重试、崩溃恢复）寻找断点

---

## 2. 严重度定义

| 级别 | 含义 | 处理方式 |
|------|------|---------|
| **P0 - 阻塞** | 不解决则方案不可执行 | 必须在开发前解决 |
| **P1 - 重要** | 不解决则核心流程有明显缺陷 | 必须在 MVP 中解决 |
| **P2 - 改进** | 不解决可以工作但有隐患 | MVP 中应解决，可后推到 Phase 2 |
| **P3 - 建议** | 优化项 | 记录，后续迭代 |

---

## 3. 问题清单

### 3.1 [P0] FileSyncManager 多任务路径映射机制缺失

**现状：**

harness-example 中 `get_file_sync_config()` 返回静态配置：
```python
path_mapping={
    "/root/.work/{book_slug}/": "tasks/{task_id}/workspace/",
    "~/.claude/projects/{path_hash}/": "tasks/{task_id}/session/",
}
```

但 `{task_id}` 和 `{book_slug}` 是每个任务不同的动态值。一台 Worker 上可能同时有多本书（1 个生产 + 多个修改），每本书的 `book_slug → task_id → OSS prefix` 映射不同。

**影响：**
- FileSyncManager 运行在 Worker 上，但映射信息在 Manager 的 TaskRegistry 中
- 如果映射缺失或错误，文件会同步到错误的 OSS 路径
- 方案中未说明 FileSyncManager 如何获取和维护这个映射

**补充方案：**

引入 **TaskSyncMapper** 组件，运行在 Worker Runtime 内部：

```
Manager 侧:
  新任务分配到 Worker 时:
    1. TaskRegistry 注册 task_id → (worker_id, book_slug)
    2. 发送 REGISTER_SYNC_MAPPING 消息到 Worker:
       {task_id, book_slug, oss_prefix, path_hash, watch_paths}

Worker 侧 (TaskSyncMapper):
  维护映射表:
    /root/.work/outliers/        → oss://bucket/prefix/tasks/123/workspace/
    ~/.claude/projects/abc123/   → oss://bucket/prefix/tasks/123/session/

  FileSyncManager 使用此映射:
    文件变更 /root/.work/outliers/state.json
      → 匹配映射: /root/.work/outliers/ → tasks/123/workspace/
      → 上传到: tasks/123/workspace/state.json

  任务完成时:
    Manager 发送 UNREGISTER_SYNC_MAPPING {task_id}
    → Worker 移除映射（但保留已完成任务的目录，供 --resume 使用）
    → 停止该任务的文件同步
```

**对 Elastic-Agent 框架的影响：**
- Worker Runtime 新增 `TaskSyncMapper` 子组件
- 通信协议新增 `REGISTER_SYNC_MAPPING` / `UNREGISTER_SYNC_MAPPING` 消息类型
- FileSyncManager 从静态配置改为动态查询 TaskSyncMapper
- `get_file_sync_config()` 接口改为返回模板规则，实际映射由 Manager 推送

**变更点：** MVP 方案 §3.4 (FileSyncManager)、harness-example §4.1 (get_file_sync_config)

---

### 3.2 [P0] 仓库边界未明确定义

**现状：**

原方案将 AudiobookHarness 代码描述为"Harness 接入 Elastic-Agent 框架"，但没有说明：
- AudiobookHarness 运行在哪个进程中？
- Audiobook 专用 API 如何挂载到 Manager 上？
- 用户提到"三个 repo 分别执行"，但没有定义第三个 repo 的边界

**影响：**
- 开发时不知道哪些代码放在哪个仓库
- 如果 Audiobook 逻辑混入 Elastic-Agent 框架，框架失去通用性

**补充方案：**

见 [00-overview.md §2 和 §3](00-overview.md) — 明确定义各仓库为：
1. Elastic-Agent (Library/Package)
2. Audiobook Agent Service (Application，依赖 elastic-agent 包)
3. audio_book_echo_editor (Existing app，通过 HTTP/Webhook 与 Audiobook Agent Service 交互)

**变更点：** 三份文档均需更新以体现这个边界

---

### 3.3 [P0] TaskRegistry 无持久化导致崩溃后修改不可路由

**现状：**

harness-example 中 `TaskRegistry` 是内存数据结构。如果 Audiobook Agent Service 进程重启，所有 `task_id → (worker_id, session_id)` 映射丢失。

**影响：**
- 重启后所有已完成任务的修改请求无法路由
- 即使 Worker 和 session 仍然存在，Manager 也不知道如何找到它们

**补充方案：**

1. **TaskRegistry JSON 持久化**（与 NodeRegistry 一致的策略）：
   ```
   ~/.elastic-agent/task_registry.json
   写入策略: 每次注册/更新后写入（写入频率低，不需要防抖）
   格式: {task_id: {worker_id, session_id, book_slug, status, cwd, ...}}
   ```

2. **启动时重建**：
   - 读取 task_registry.json
   - 对比 NodeRegistry 中的在线 Worker → 清理已不存在 Worker 的 session
   - 对比 OSS manifest → 验证 session 数据完整性

3. **备选: 从 OSS manifest 重建**：
   - 每个 task 的 `_sync_manifest.json` 包含 `worker_id` 和 `latest_session_id`
   - 启动时扫描所有 manifest 可重建映射
   - 但扫描全量 manifest 较慢，适合作为兜底

**变更点：** harness-example §3.2 (TaskRegistry)、MVP 方案 §5.2 (Manager 崩溃恢复)

---

### 3.4 [P1] _sync_manifest.json 格式在文档间不一致

**现状：**

- harness-example §3.4 使用 **dict** 格式: `{"files": {"path": {md5, size, synced_at}}}`
- 跑书双引擎方案 §5.9.7 使用 **array** 格式: `{"files": [{"path", "oss_key", "role", ...}]}`
- 跑书方案的 array 格式多了 `role`、`oss_key`、`content_type` 等字段

**影响：**
- 如果两端实现不同格式，文件查询会失败
- `role` 字段对于最终稿选择（delivery > compliant > final）很重要

**补充方案：**

统一使用 **array** 格式（跑书方案版本更完善），增加必要字段：

```json
{
  "task_id": "123",
  "worker_id": "aliyun:i-bp1xxx",
  "status": "syncing",
  "updated_at": "2026-05-17T14:30:03Z",
  "files": [
    {
      "path": "workspace/state.json",
      "oss_key": "elastic-agent/tasks/123/workspace/state.json",
      "size": 1234,
      "md5": "abc123",
      "content_type": "application/json",
      "role": "state",
      "synced_at": "2026-05-17T14:30:01Z"
    }
  ]
}
```

- harness-example 需要同步更新
- `role` 枚举值需在 05-interface-contracts.md 中统一定义

**变更点：** harness-example §3.4、MVP 方案 §3.4

---

### 3.5 [P1] Session ID 提取可靠性风险

**现状：**

方案依赖从 Claude Code `stream-json` 输出的最后一行 `{"type": "result", "session_id": "..."}` 提取 session_id。

**风险场景：**
1. Claude Code 进程 crash（SIGKILL）→ 不会输出 result 行 → session_id 丢失
2. Claude Code 输出格式版本更新 → 字段名或结构变化
3. 超长输出导致 Worker Runtime 解析延迟或 OOM

**影响：**
- 丢失 session_id 意味着该任务无法 `--resume`，修改功能不可用

**补充方案：**

1. **多源提取 session_id**（防御性设计）：
   ```
   优先级:
     A. 从 stream-json 的 result 事件提取（正常路径）
     B. 从 ~/.claude/projects/{path_hash}/ 目录扫描最新 .jsonl 文件名
        文件名格式通常包含 session identifier
     C. 从 Claude Code 的 --output-format json 输出（如果支持）
   ```

2. **PROCESS_EXIT 时立即同步 session 文件到 OSS**：
   ```
   Worker Runtime 收到 PROCESS_EXIT:
     → 触发 FileSyncManager 立即同步 session 目录（跳过防抖）
     → 即使 session_id 提取失败，session 文件已在 OSS，后续可手动恢复
   ```

3. **session_id 在 state.json 中冗余存储**：
   ```
   audiobook 插件在 Phase 完成时写入 state.json:
     {"session_id": "abc123", "phase": 9, ...}
   这样即使 stream-json 解析失败，也能从 state.json 恢复
   ```

**变更点：** harness-example §5.2、audiobook 插件本身（需要在 state.json 中写入 session_id）

---

### 3.6 [P1] 单 Worker 多凭证并发的额度管理

**现状：**

harness-example §5.4 提到 4 个并发 Claude Code 进程共享同一个 `~/.claude/.credentials.json`。MVP 方案的 CredentialPool 设计是 per-Worker 分配一个凭证。

**风险：**
- Claude Max 账号有 token-per-minute 限制
- 1 个生产（Opus 密集）+ 3 个修改（Sonnet）并发时，可能超出单账号限额
- 额度耗尽导致所有进程阻塞

**补充方案：**

1. **凭证按槽位类型分配**：
   ```
   Worker 凭证模型:
     primary_credential:   用于生产槽位（Opus 密集，需要高额度账号）
     secondary_credentials: 用于修改槽位（Sonnet 轻量，可用低额度账号）

   CredentialPool 分配逻辑:
     分配 Worker 时:
       → 分配 1 个高额度凭证作为 primary
       → 分配 N 个普通凭证作为 secondary (N = max_edit_slots)
       → Worker 上不同 Claude Code 进程使用不同凭证文件
   ```

2. **进程级凭证隔离**：
   ```
   Claude Code 进程启动时:
     生产模式: CLAUDE_CONFIG_DIR=/root/.claude-prod/
     修改模式 #1: CLAUDE_CONFIG_DIR=/root/.claude-edit-1/
     修改模式 #2: CLAUDE_CONFIG_DIR=/root/.claude-edit-2/
     修改模式 #3: CLAUDE_CONFIG_DIR=/root/.claude-edit-3/

   每个目录有独立的 .credentials.json
   ```

3. **额度预检查**：
   ```
   分配任务前检查凭证剩余额度:
     如果额度 < 预估消耗 → 拒绝分配 → 换用其他 Worker 或排队等待
   ```

**变更点：** MVP 方案 §3.6 (凭证与安全)、harness-example §5.4 (并发控制)

---

### 3.7 [P1] Retry from Phase 的执行主体不明

**现状：**

跑书双引擎方案 §5.9.4 描述了 retry 的流程：
1. 从 OSS 下载 workspace
2. 删除 Phase N 及之后的产物
3. 修改 state.json
4. 上传到 Worker
5. 启动 /continue-book

但没有说明这些操作由谁执行（Audiobook Agent Service？Worker Runtime？）

**影响：**
- 多个组件都可能修改文件系统和 OSS，容易产生竞态条件
- 如果 Worker 上还有旧文件残留，可能与恢复的文件冲突

**补充方案：**

**Retry 全部由 Audiobook Agent Service 编排**，具体步骤：

```
Audiobook Agent Service 收到 retry(task_id, from_phase):
  1. 检查 task 当前状态（是否允许 retry）
  2. 如果有活跃进程 → 先停止
  3. 确定目标 Worker:
     a. 优先使用原 Worker（session 文件在本地）
     b. 原 Worker 不可用 → 分配新 Worker → 从 OSS 恢复 workspace
  4. 通过 Worker Runtime 执行清理脚本:
     EXECUTE: python cleanup_phases.py --from-phase {N} --work-dir /root/.work/{slug}/
     (这个脚本需要在 audiobook 插件中提供)
  5. 通过 Worker Runtime 启动 Claude Code:
     claude -p "/continue-book {slug}" ...
  6. 更新 TaskRegistry
  7. Webhook → audio_book: task.production.started
```

**变更点：** harness-example §4.1（新增 retry 方法）、Audiobook Agent Service 需包含 phase cleanup 逻辑

---

### 3.8 [P1] Worker 上多书目录的生命周期管理缺失

**现状：**

Worker 常驻运行，不断接收新书生产任务。每本书在 `/root/.work/{book_slug}/` 创建工作目录，但方案没有说明：
- 已完成且不再需要修改的书何时清理？
- session 文件何时清理？
- 磁盘空间不足时怎么办？

**影响：**
- 长期运行的 Worker 磁盘可能被历史书目数据填满
- 特别是 session .jsonl 文件可能很大（几百 MB/本书）

**补充方案：**

1. **空间管理策略**：
   ```
   Worker Runtime 定期检查磁盘使用:
     总使用 > 80%: 报告 WARNING 到 Manager
     总使用 > 90%: 触发清理

   清理策略:
     按 last_access_time 排序
     最近 7 天内有 --resume 活动的 → 保留
     超过 7 天未活动的 → 归档到 OSS → 清理本地
     正在被 FileSyncManager 同步的 → 不清理
   ```

2. **手动清理 API**：
   ```
   DELETE /api/tasks/{task_id}/workspace
     → 检查无活跃进程
     → 确认 OSS 备份完整（对比 manifest）
     → 清理 Worker 本地文件
     → 从 TaskRegistry 标记为 archived
   ```

3. **session 文件压缩**：
   ```
   已完成的 session .jsonl 不需要随时读取
   可以 gzip 压缩后保留在本地，--resume 时再解压
   或直接从 OSS 下载（OSS 上已有备份）
   ```

**变更点：** harness-example §5.5 (Worker 手动管理)、新增 workspace 生命周期管理

---

### 3.9 [P1] Webhook 丢失或延迟的补偿机制不够完善

**现状：**

跑书方案提到了轮询兜底（`GET /api/tasks/{id}/status`），但没有说明：
- 轮询频率多少？
- Webhook 失败时是否重试？
- 事件顺序乱序时如何处理？

**补充方案：**

1. **Webhook 重试策略（Audiobook Agent Service 侧）**：
   ```
   首次发送失败 → 1s 后重试
   第 2 次失败 → 5s 后重试
   第 3 次失败 → 30s 后重试
   第 4 次失败 → 5min 后重试
   第 5 次失败 → 30min 后重试
   仍失败 → 标记为 failed，写入死信队列
   死信队列: 手动重发或 cron 每小时扫描重发
   ```

2. **audio_book_echo_editor 侧轮询兜底**：
   ```
   前端打开任务详情页 → 调用 GET /api/tasks/{id}/script-production
   后端逻辑:
     1. 先读本地 elastic_book_runs 状态
     2. 如果状态是 running 且 last_event_at > 5min
        → 调用 Audiobook Agent Service GET /api/tasks/{id}/status
        → 对比并更新本地状态
     3. 后台定时任务: 每 5 分钟扫描所有 running 状态的 elastic_book_runs
        → 批量查询 Audiobook Agent Service 状态
        → 更新不一致的记录
   ```

3. **事件顺序保证**：
   ```
   每个 Webhook 事件包含 sequence_number (递增)
   audio_book 侧:
     收到事件时对比 elastic_book_runs.last_event_sequence
     如果 new_sequence <= last_sequence → 忽略（乱序到达）
     如果 new_sequence > last_sequence + 1 → 中间有丢失 → 触发轮询补偿
   ```

**变更点：** 跑书方案 §5.7 (Webhook)、05-interface-contracts.md (事件格式增加 sequence_number)

---

### 3.10 [已解决] 聊天数据获取方式

**原问题：** 前端通过 WebSocket 获取实时 chat 流涉及三层跳转（前端 → ABE → ABS → Worker），架构复杂。

**最终方案：** 聊天数据统一通过 OSS 获取，与文件目录走相同的数据路径。Worker Runtime 将 Claude Code 输出双写到本地 NDJSON 日志文件，FileSyncManager 同步到 OSS，前端通过 ABE 后端的 `chat/live?offset=N` 接口轮询增量读取。延迟 2-3 秒，对 Audiobook 场景完全可接受。

不再需要前端直连 ABS WebSocket、stream-config token、JWT 认证等机制。前端只与 ABE 后端通信。

---

### 3.11 [P2] Claude Code `--resume` 长期可靠性未验证

**现状：**

修改流程核心依赖 `--resume session_id`。但：
- Claude Code session 文件是内部格式，可能随版本变化
- session 文件可能很大（做完一本书的对话历史可达数百 MB）
- 长时间后（天/周/月）`--resume` 是否仍能正常工作未知

**影响：**
- 如果 `--resume` 不可靠，修改功能的核心价值受损

**补充方案：**

1. **在 Phase 1 即验证**：
   ```
   测试矩阵:
     - 做书完成后立即 resume → 预期: 可用
     - 做书完成 24 小时后 resume → 预期: 可用
     - 做书完成 7 天后 resume → 预期: 待验证
     - Claude Code 升级后 resume 旧 session → 预期: 待验证
     - session 文件从 OSS 恢复到新 Worker 后 resume → 预期: 待验证
   ```

2. **降级方案: 不用 `--resume`，用 `/continue-book`**：
   ```
   如果 --resume 不可靠:
     修改请求 → 启动新的 Claude Code 会话
     → 读取 .work/{slug}/ 下的所有产物（上下文足够丰富）
     → 执行修改指令
     → 不恢复对话历史，但文件上下文完整
   
   这种方式的缺点: 无法引用之前的对话（如"上次你说的那个..."）
   优点: 不依赖 session 文件，更可靠
   ```

3. **hybrid 策略**：
   ```
   尝试 --resume → 成功则用
   --resume 失败 → 降级到 /continue-book + 新会话 + 文件上下文
   ```

**变更点：** harness-example §5.1 (Session 路由)、Phase 1 测试计划

---

### 3.12 [P2] audiobook 插件需要的代码调整未列全

**现状：**

跑书方案集中在 audio_book_echo_editor 的后端/前端改造，但 audiobook 插件（audiobook-nonfiction）本身也需要调整：

**需要调整的插件行为：**

1. **state.json 增加 session_id 字段** — 做书完成时将 Claude Code 的 session_id 写入 state.json，作为 session_id 提取的冗余来源
2. **交付目录标准化** — 确保 delivery/ 目录下的文件名和结构与方案一致（`audiobook_manuscript.md`、`audiobook_delivery.zip`）
3. **输入方式调整** — 当前插件通过 `/audiobook <file_path>` 接收输入，需要确认与 Worker Runtime 的 EXECUTE 命令参数一致
4. **进度事件格式** — 确认 state.json 中的 phase 名称与方案中的 phase 枚举一致

**补充方案：**

在 audiobook-nonfiction 插件仓库中创建适配清单，确保：
- `/audiobook` 命令参数格式兼容 Worker Runtime 调用
- state.json 输出字段包含 Elastic-Agent 需要的所有信息
- delivery/ 目录结构标准化

**变更点：** 需要新增一个 audiobook 插件适配文档（在 audiobook-nonfiction 独立仓库中执行）

---

### 3.13 [P2] L3 健康检查缺少业务级"卡住"检测

**现状：**

MVP 方案的 L3 健康检查只检查"Agent 进程是否活着"。但 Claude Code 进程可能存活但实际卡死（例如子 Agent 无限循环、等待不会到来的输入）。

**补充方案：**

在 AudiobookHarness 的事件回调中增加 **进度超时检测**：

```
Audiobook Agent Service 维护每个 task 的 last_progress_time:
  收到 LOG 事件 → 更新 last_progress_time
  收到 FILE_CHANGE 事件 → 更新 last_progress_time

定时检查（每 5 分钟）:
  now - last_progress_time > PROGRESS_TIMEOUT (默认 30 分钟)
    → 标记任务为 "stalled"
    → 告警通知运维
    → 可选: 自动 SIGINT → 等待优雅退出 → PROCESS_EXIT → 标记失败
```

这不是框架级功能（不同 Harness 的"卡住"定义不同），而是 Audiobook Agent Service 的业务逻辑。

**变更点：** harness-example §4.1 (get_event_handlers 增加进度监控)

---

### 3.14 [P2] book_slug 生成规则和唯一性保证

**现状：**

跑书方案 §5.10.3 提到 `book_slug` 由后端生成，格式为 `{title_slug}-{task_id}`。但：
- 中文书名如何 slug 化？（如《思考，快与慢》→ ?）
- 多个 task 处理同一本书时 slug 不同 → Worker 上多个目录
- slug 过长导致路径过长

**补充方案：**

```
book_slug 生成规则:
  1. 取 book.title 的拼音首字母或英文（限 30 字符）
  2. 拼接 task_id
  3. 示例: "sikuaimansi-123" 或 "thinking-fast-123"
  4. 如果生成失败（纯特殊字符书名）→ 直接使用 "book-{task_id}"

唯一性: 靠 task_id 后缀保证，不需要额外检查
```

**变更点：** 跑书方案 §5.10.3（明确 slug 生成规则）

---

### 3.15 [P2] OSS 操作失败的重试和断点续传

**现状：**

FileSyncManager 将文件上传到 OSS，但没有说明：
- 上传失败怎么办？
- 大文件（session .jsonl 可达数百 MB）是否需要分片上传？
- OSS 临时不可用时的缓冲策略？

**补充方案：**

```
FileSyncManager 上传策略:
  小文件 (< 10MB): PutObject 直接上传
    失败重试: 3 次，指数退避 (1s, 3s, 10s)
    3 次都失败: 写入 failed_uploads 队列，5 分钟后重试

  大文件 (>= 10MB): Multipart Upload
    5MB/part
    失败重试: per-part 3 次
    全部失败: 同上

  OSS 不可用:
    继续在本地写文件（不阻塞 Claude Code）
    每 30 秒检查 OSS 可用性
    恢复后按 _sync_manifest 对比差异，增量补传

  critical 文件 (state.json, manifest): 使用同步写入（等待确认）
  其他文件: 使用异步写入（不阻塞主流程）
```

**变更点：** MVP 方案 §3.4 (FileSyncManager 增加容错设计)

---

### 3.16 [P3] 取消任务状态映射不理想

**现状：**

跑书方案 §6.3 将 `cancelled` 映射为 `Task.status=FAILED`，因为 `TaskStatus` 枚举没有 `CANCELLED`。

**补充方案：**

建议在 audio_book_echo_editor 中新增 `CANCELLED` 状态：

```python
class TaskStatus(str, enum.Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    REVIEWING = "REVIEWING"
    FAILED = "FAILED"
    PUSHED = "PUSHED"
    CANCELLED = "CANCELLED"  # 新增
```

如果不想动枚举（考虑迁移风险），则：
- 保持 `FAILED` 映射
- `script_error_message = "[用户取消] 任务由 {operator} 于 {time} 取消"`
- 前端通过 `error_message` 前缀 `[用户取消]` 区分展示

**变更点：** 跑书方案 §6.3（推荐方案）

---

### 3.17 [已解决] 前端与 ABS 的通信安全

**原问题：** 前端直连 ABS WebSocket 需要独立的认证机制和 CORS 配置。

**最终方案：** 前端不再直连 ABS。所有数据（聊天、文件）统一通过 ABE 后端读取 OSS，前端只与 ABE 后端通信，复用现有的登录态认证。不需要额外的 JWT token、CORS 配置或 Rate Limit 机制。

---

### 3.18 [P3] 监控告警体系缺失

**现状：**

方案中没有描述运维监控和告警。

**补充方案（非 MVP，记录备用）：**

```
关键指标:
  - 队列深度 (BookQueue 排队任务数)
  - 活跃 Worker 数量 / 空闲 Worker 数量
  - 平均做书时长
  - 做书成功率 / 失败率
  - 修改请求延迟（从发送到完成）
  - OSS 同步延迟
  - 凭证额度剩余

告警规则:
  - 队列深度 > 10 且无空闲 Worker → 通知扩容
  - 做书失败率 > 20% → 紧急告警
  - Worker 3 分钟内连续 unhealthy → 紧急告警
  - 凭证额度 < 10% → 通知换号
```

---

### 3.19 [P1] 修改流程未重新注册文件同步映射

**现状：**

04-gap-analysis §3.1 方案中，生产完成后发送 `UNREGISTER_SYNC_MAPPING` 停止文件同步。但修改流程（`--resume`）会产生新的文件变更（如更新 manuscript_final.md），这些变更需要同步到 OSS。

**影响：**
- 修改过程中的文件变更不会同步到 OSS
- audio_book_echo_editor 读取的最终稿仍是修改前的旧版本
- `task.edit.completed` webhook 中指向的 manuscript 可能是过时的

**补充方案：**

修改流程需要包含同步映射的注册和注销：

```
handle_edit_request(task_id, message):
  1. 检查 session 状态和槽位
  2. 重新注册同步映射:
     await runtime.send_message("REGISTER_SYNC_MAPPING", mapping)
  3. 启动 Claude Code --resume
  4. 修改完成 (_on_process_exit):
     → 触发 FileSyncManager 立即 flush 所有待同步文件
     → 等待 flush 完成
     → 发送 UNREGISTER_SYNC_MAPPING
     → 发送 task.edit.completed webhook
```

**变更点：** 02-audiobook-agent-service §4.1 (handle_edit_request)

---

### 3.20 [P1] 同一任务的并发修改请求未做互斥

**现状：**

`handle_edit_request` 只检查 Worker 的 edit_slots 总量是否满，不检查当前 task_id 是否已有正在进行的修改。如果用户快速连续发送两条修改指令（如"修改第三章"和"修改第五章"），两个 `--resume` 进程会同时恢复同一个 session，可能导致 session 文件损坏。

**影响：**
- Claude Code session .jsonl 被两个进程同时读写 → 文件损坏
- 两个进程产生的输出交叉混乱
- 可能导致该 session 永久不可用

**补充方案：**

在 `handle_edit_request` 开头增加 per-task 互斥检查：

```python
async def handle_edit_request(self, task_id: str, message: str):
    session = self.task_registry.get(task_id)
    if not session:
        raise NotFoundError(...)

    # 新增: 检查该任务是否已有正在进行的修改
    if session.status == "editing":
        raise ConflictError(
            f"Task {task_id} already has an active modification. "
            "Please wait for the current edit to complete."
        )

    # 检查 Worker edit_slots
    worker_state = await self.get_worker_slot_state(session.worker_id)
    if worker_state.edit_slots.used >= worker_state.edit_slots.max:
        raise CapacityError(...)

    session.status = "editing"
    # ... 启动 --resume
```

audio_book_echo_editor 前端也应在 UI 层面禁止同一任务的并发修改请求（按钮置灰 + 提示"修改进行中"）。

**变更点：** 02-audiobook-agent-service §4.1 (handle_edit_request)、03-audiobook-app-adaptation 前端

---

## 4. 问题汇总与状态

| # | 严重度 | 标题 | 状态 | 影响文档 |
|---|--------|------|------|---------|
| 3.1 | P0 | FileSyncManager 多任务路径映射 | 补充方案已定义 | MVP §3.4, harness §4.1 |
| 3.2 | P0 | 仓库边界未定义 | 已在 00-overview.md 解决 | 全部 |
| 3.3 | P0 | TaskRegistry 无持久化 | 补充方案已定义 | harness §3.2, MVP §5.2 |
| 3.4 | P1 | manifest 格式不一致 | 统一为 array 格式 | harness §3.4, MVP §3.4 |
| 3.5 | P1 | session_id 提取可靠性 | 多源提取 + 冗余存储 | harness §5.2, 插件调整 |
| 3.6 | P1 | 多凭证并发额度 | 按槽位隔离凭证 | MVP §3.6, harness §5.4 |
| 3.7 | P1 | Retry 执行主体不明 | 全部由 Agent Service 编排 | harness §4.1 |
| 3.8 | P1 | Worker 目录生命周期 | 定时清理 + 手动清理 API | harness §5.5 |
| 3.9 | P1 | Webhook 补偿机制 | 重试 + 轮询 + 序号 | 跑书方案 §5.7, contracts |
| 3.10 | 已解决 | 聊天数据获取方式 | 统一走 OSS 轮询（chat/live） | 跑书方案 §5.5 |
| 3.11 | P2 | --resume 长期可靠性 | 早期验证 + 降级方案 | harness §5.1, 测试 |
| 3.12 | P2 | 插件侧调整未列全 | 新增插件适配清单 | 插件仓库 |
| 3.13 | P2 | L3 卡住检测 | 进度超时监控 | harness §4.1 |
| 3.14 | P2 | book_slug 生成规则 | 明确规则 | 跑书方案 §5.10.3 |
| 3.15 | P2 | OSS 上传容错 | 重试 + 分片 + 缓冲 | MVP §3.4 |
| 3.16 | P3 | 取消状态映射 | 建议新增 CANCELLED 枚举 | 跑书方案 §6.3 |
| 3.17 | 已解决 | 前端与 ABS 通信安全 | 前端不直连 ABS，复用 ABE 登录态 | 跑书方案 §5.5 |
| 3.18 | P3 | 监控告警 | 后续迭代 | 非 MVP |
| 3.19 | P1 | 修改流程未重新注册同步映射 | 修改前 REGISTER、完成后 flush+UNREGISTER | harness §4.1 |
| 3.20 | P1 | 同一任务并发修改无互斥 | session.status=="editing" 时拒绝新修改 | harness §4.1, 前端 |
