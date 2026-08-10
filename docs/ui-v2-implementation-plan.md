# Elastic-Agent UI v2 详细实现设计

> 状态：Proposed，尚未实施  
> 日期：2026-08-10  
> 范围：Manager 管理前端、为前端提供的只读聚合/分页 API、无中断发布与回滚  
> 生产安全原则：UI v2 灰度期间不修改、不重启现有 Manager，不中断运行中的 Job 或 Worker WebSocket

## 1. 决策摘要

UI v2 采用“统一应用壳 + 持久导航 + 页面级数据加载”，将当前 Batch Console 中混在一起的账号、Job 表单、Job 历史和结果拆开，同时保留独立 Fleet 页面能力。

逻辑页面为：

- 总览：`/overview`
- 账号：`/accounts`
- 添加账号：`/accounts/new`
- 提交 Job：`/jobs/new`
- Jobs：`/jobs`
- Job 详情：`/jobs/{job_id}`
- 结果：`/results`
- Workers：`/fleet`

生产灰度阶段统一挂在 `/ui-v2/` 下，例如 `/ui-v2/jobs/new`。旧 `/batch` 和 `/fleet` 在整个灰度期保持不变，作为立即回退入口。

第一版不引入 React/Vue 和生产 Node 构建依赖，使用原生 ES Modules、History API、集中状态仓库和可取消的页面级 poller。静态资源从当前 2,975 行内联 `ui.py` 中拆出，但 canary 阶段由 Cloudflare Workers Static Assets 提供，不并入 Manager 进程。

发布策略分为两层：

1. **静态 UI canary**：Cloudflare 边缘只接管 `/ui-v2/*`；`/api/*` 和 `/ws/runtime` 原样通过现有 Tunnel 到 Manager。该阶段不需要 Manager 重启。
2. **后端扩展与最终整合**：Jobs/Results 分页、结果目录索引等需要 Manager 代码变更的工作只在无活跃 Job 的维护窗口发布。是否最终把静态资源并回 Manager，不是 UI v2 上线的前置条件。

## 2. 背景与现状

### 2.1 当前前端结构

当前 UI 全部位于 `src/elastic_agent/api/routes/ui.py`：

- 文件约 2,975 行、144 KB。
- `_DASHBOARD_HTML` 提供 Fleet 页面。
- `_BATCH_HTML` 同时包含：
  - 全局 OTP 卡片；
  - OAuth/Agent API 账号列表和添加表单；
  - 8 个区块的完整 Job 表单；
  - Jobs 列表、Worker 状态、取消和日志；
  - 已收集结果和流式下载。
- `/`、`/batch` 返回 Batch Console。
- `/fleet`、`/dashboard` 返回 Fleet Dashboard。
- 两个页面之间只有普通链接，切换时整页重新加载。
- HTML、CSS、JavaScript 都是 Python 字符串，没有模板目录、静态资源目录或前端构建流程。

当前 Batch 页面已经有大量值得保留的正确行为，拆页不是从头重写：

- API Key 只放在 `sessionStorage`，通过 Bearer Header 使用。
- Job 提交先运行 `/api/jobs/plan`，再使用稳定 `Idempotency-Key` 提交。
- Job 卡片增量 reconcile，避免轮询时丢失展开状态、焦点、横向滚动和页面位置。
- 结果请求有 request version，迟到响应不会覆盖新状态。
- 结果下载使用带认证的 `fetch`、`AbortController` 和可取消流读取。
- 日志支持暂停、跟随、复制和下载，并在 Worker 被销毁后停止 journal 轮询。
- OTP 精确绑定 login request、challenge、账号、Job、shard 和 Worker。
- 删除绑定 EIP 的账号需要先 decommission，再删除 identity，并进行完整账号 ID 双确认。

### 2.2 当前扩展性边界

现有页面在少量 Job/Worker 时可用，但不能直接支撑规划中的大规模容量：

- Fleet 每 5 秒请求 `/api/nodes?limit=200`，UI 没有分页；超过 200 台时分类统计只基于当前页，可能不准确。
- `/api/jobs` 没有分页，会返回所有内存 Job，并扫描所有持久化 JobSpec；每项还内联 `workers_detail`。
- Batch 每 5 秒刷新 Jobs，并可能继续为最多 30 个 Job 分别请求结果。
- 当前“已收集结果”只来自可见 Job 的缓存，较旧且被折叠的 Job 结果默认不可见。
- `/api/results` 虽然存在，但会扫描所有 S3 Job 前缀，并逐 Job 枚举对象以统计文件，不能作为高频全局轮询接口。
- Accounts、Bindings 和 Allocations 当前也都是全量列表。

UI v2 必须保证“只加载当前页”，不能把拆页变成多个页面同时后台拉取全量数据。

### 2.3 当前生产发布边界

当前生产请求链路为：

```text
浏览器 / Worker
  → Cloudflare
  → Cloudflare Tunnel connector
  → Manager 私网地址 :8080
  → 单进程 FastAPI / Uvicorn
```

源站没有 Nginx/Caddy，也没有独立静态站点。UI、REST API、Worker WebSocket 和 Manager 生命周期位于同一进程。

直接发布修改后的 `ui.py` 需要重启 Manager。FastAPI lifespan 关闭会调用 `manager.stop()`，进一步调用 `BatchOrchestrator.shutdown()`；该流程会以 `manager shutting down` 为原因取消活跃 Job、停止命令、最终收集并销毁临时 Worker。因此“只是前端修改”在当前部署形态下仍会中断任务。

2026-08-10 本文编写时的只读快照为 543 个 Job，其中 10 个处于 `running`，Manager 报告 10 个在线 Worker。该数字只是风险说明，不是发布门禁；任何维护前都必须重新读取实时状态。

## 3. 目标与非目标

### 3.1 目标

1. 提供始终可见的统一导航，不整页刷新。
2. 将账号、添加账号、提交 Job、Jobs、结果和 Fleet 拆为职责单一的页面。
3. 页面切换时保留非秘密的内存状态和未提交表单。
4. 为 Job、结果和 Worker 提供可直接访问、可刷新、可前进/后退的深链接。
5. OTP 在任何页面都可见，并始终精确提交给原 challenge。
6. 将轮询限制在当前页面，离开页面立即取消 timer 和网络请求。
7. 结果页面覆盖所有已收集结果，不依赖当前 Jobs 页面是否展示该 Job。
8. 在 1,000 Worker、5,000 历史 Job、10,000 结果摘要的目标规模下保持有界 DOM、请求量和内存。
9. 通过 `/ui-v2/` 完成不重启 Manager 的生产 canary 和回滚演练。
10. 保留现有 API、秘密边界、幂等、EIP 删除和结果下载安全约束。

### 3.2 非目标

- 不在本项目中实现新的用户体系、RBAC 或多租户隔离；v2 仍使用现有管理员 API Key。
- 不改变 JobSpec、账号分配、EIP 生命周期或 Worker 执行语义。
- 不把 Agent API Key、OAuth password、邮箱查询 Token 或 OTP 放入浏览器持久存储。
- 不把 Harness 上传提升为普通用户流程；它继续是默认关闭的管理员高级功能。
- 不在第一阶段启动第二个 Manager，也不修改 Worker WebSocket 路由。
- 不在 UI canary 阶段删除旧 `/batch`、`/fleet` 或旧 API 响应字段。
- 不在第一版注册 Service Worker，避免缓存 API、认证状态或旧静态版本。

## 4. 必须保持的系统不变量

### 4.1 认证与秘密

- 静态 HTML 可以公开加载；除 `/api/health` 外，数据和操作 API 必须继续由 `require_api_key` 保护。
- API Key 只允许通过 `Authorization: Bearer`，禁止 query string、Cookie 和下载 URL。
- API Key 只保存在内存与 `sessionStorage.ea_api_key`；禁止 `localStorage`。
- 页面发现 URL 中存在 `api_key` 时，必须在任何 API 请求前移除它，并且不能把该值当作凭据使用。
- OAuth password、邮箱查询 Token、Agent API Key、OTP 都是 write-only；不得进入 store、草稿、toast、错误日志或前端遥测。
- 下载必须继续通过带 Bearer Header 的 `fetch`，不得退化为携带 key 的 `<a href>`。
- Job 详情继续使用服务端脱敏数据；不得回显 env value、secret reference、setup step env 或带凭据的仓库 URL。

### 4.2 Job 幂等与破坏性操作

- “计划”只调用 `/api/jobs/plan`，不得持久化、claim 账号或创建资源。
- 一次提交意图生成一个 `Idempotency-Key`；网络重试复用该 key，表单内容发生变化后生成新 key。
- 提交、取消、重跑、终止 Worker、释放 EIP 等按钮必须防双击和并发重复操作。
- EIP decommission 顺序固定为：读取 binding → 显示不可恢复警告 → 输入完整 account ID → `release_eip=true` decommission → 删除 identity。
- 任一 decommission 失败或 409 都不得继续删除账号。

### 4.3 OTP

- challenge 的客户端稳定键为 `(login_request_id, challenge_id)`。
- 展示信息同时保留 `account_id/account_email/job_id/shard_index/worker_id`，不可仅按当前账号或当前 Job 推断目标。
- OTP 仅接受 6 位数字；同一 challenge 同时只允许一个提交请求。
- 404、409、410 分别展示“已不存在”“冲突/已处理”“已过期”，不能自动重试旧码。
- OTP 值不得进入全局 store；页面切换可保留原 DOM 节点，刷新或明确离开登录操作时应清空。

### 4.4 结果与日志

- 非空结果不能被迟到的空响应覆盖。
- 同一个 Job 同时只允许一个结果下载；取消必须触发 `AbortController.abort()` 和 reader cancel。
- 大文件不能在不支持流式落盘的浏览器中无界聚合到内存。
- 日志只在详情或日志对话框打开时轮询；页面隐藏、暂停、关闭或离开路由时停止。
- Worker journal 返回 404/409 后停止该 Worker 的系统日志轮询，并引导用户查看 Job 归档日志。

## 5. 信息架构与路由

### 5.1 应用壳

桌面端使用固定左侧导航和顶部状态栏：

```text
┌────────────────┬──────────────────────────────────────────────┐
│ Elastic-Agent  │ Manager 健康 · Provider/Region · OTP · 主题 │
│                ├──────────────────────────────────────────────┤
│ 总览           │                                              │
│ 账号           │                                              │
│ ＋ 提交 Job    │                 当前页面                     │
│ Jobs       N   │                                              │
│ 结果           │                                              │
│ Workers    N   │                                              │
│                │                                              │
│ 换 Key / 主题  │                                              │
└────────────────┴──────────────────────────────────────────────┘
```

移动端使用顶部 app bar 与可折叠导航抽屉；“提交 Job”保持为显眼的主操作。导航抽屉必须支持 focus trap、Esc 关闭和关闭后焦点恢复。

顶部状态栏只展示轻量状态：Manager 是否健康、provider/region、活跃 Job 数、Worker 数和待处理 OTP 数。结果总数不能为了徽标而扫描 S3。

### 5.2 路由表

| 逻辑路由 | Canary 路由 | 页面职责 | 数据加载 |
|---|---|---|---|
| `/overview` | `/ui-v2/overview` | 健康、容量、近期异常、快捷入口 | summary + health |
| `/accounts` | `/ui-v2/accounts` | 账号列表、额度、EIP、allocation | accounts/allocations/bindings |
| `/accounts/new` | `/ui-v2/accounts/new` | 新增 OAuth 或 Agent API 账号 | provider metadata |
| `/jobs/new` | `/ui-v2/jobs/new` | Job 表单、预检、计划、提交 | provider defaults + accounts |
| `/jobs` | `/ui-v2/jobs` | 活跃/历史 Job 分页、搜索和筛选 | paginated jobs |
| `/jobs/{id}` | `/ui-v2/jobs/{id}` | Job 时间线、Worker、日志和操作 | one job + optional logs/result |
| `/results` | `/ui-v2/results` | 全部结果摘要、分数、S3、下载 | paginated results |
| `/fleet` | `/ui-v2/fleet` | Worker 分页、状态、系统操作 | paginated nodes |

不存在的 UI 路由显示应用内 404。`/api/*` 和 `/ws/*` 永远不由前端 fallback 接管。

### 5.3 旧入口兼容

Canary 期间：

- `/`、`/batch` 继续返回旧 Batch Console。
- `/fleet`、`/dashboard` 继续返回旧 Fleet Dashboard。
- `/ui-v2/*` 只返回新静态应用。

稳定后可在 Cloudflare 增加“仅精确 `/`”的 Redirect Rule，将其重定向到 `/ui-v2/overview`。旧 `/batch` 必须至少保留一个完整观察期。清洁路由是否替代 `/ui-v2/*` 在后续单独决策，不阻塞 v2 上线。

## 6. 页面规格

### 6.1 总览

内容：

- Manager：health、uptime、provider、region。
- Jobs：活跃、准备中、失败待关注、近期完成。
- Accounts：总数、enabled、allocated、需要处理的不可用账号。
- Workers：总数、连接数、状态分布。
- OTP：待处理 challenge 数及直接跳转按钮。
- 最近异常：最多 10 个失败 Job 或 cleanup pending 项。
- 快捷操作：添加账号、提交 Job、查看活跃 Jobs。

总览不能加载全部 Job、全部 Worker 或结果列表。所有卡片来自轻量 summary；点击后进入对应分页页面。

### 6.2 账号列表

默认使用统一列表，并提供筛选：

- 类型：OAuth / Agent API。
- Agent：Claude / Codex。
- Provider：CloudRouter / ApexRouter。
- Group、enabled、allocated、EIP 状态。
- 关键词：账号 ID 或邮箱。

每行展示：

- 账号 ID、邮箱或 provider 显示名。
- 支持 Agent 和模型。
- write-only secret 是否已配置，只显示存在标志。
- usage/额度及最近刷新状态。
- EIP 和当前 Worker/Job allocation。
- 允许的操作：刷新额度、启停、绑定、删除/释放。

Agent API 删除仍显示禁用状态，因为后端当前明确拒绝删除。

### 6.3 添加账号

先选择四种类型之一：Claude OAuth、Codex OAuth、CloudRouter、ApexRouter。不同类型只显示适用字段。

要求：

- Claude/Codex 登录秘密控件使用 `autocomplete="new-password"`。
- Codex 至少有 password/email token 之一；必须明确 email token 只是邮箱取码凭据，不是 OpenAI Token。
- CloudRouter 支持 Claude/Codex；ApexRouter 只支持 Codex。
- API Key/password/token 不写入 store；提交成功后立即清空。
- 离开页面时若秘密输入仍有值，显示“离开将清空”的确认。
- 失败提示只显示安全错误摘要，不回显请求体。

### 6.4 提交 Job

保留现有八组 JobSpec 语义，但界面组合为四步：

1. **基本信息与计算资源**：basics + compute。
2. **代码、环境与数据**：source。
3. **Agent、账号与命令**：account + run。
4. **结果、续跑与高级设置**：results + rotation + advanced。

每一步都保留现有字段 ID 对应的语义、帮助文本和动态约束。桌面端右侧显示 sticky 摘要；移动端摘要折叠。

客户端校验至少包括：

- run command 非空。
- setup steps 为合法 JSON 且 schema 可解析。
- TTL 不短于 run timeout。
- S3 dataset 行格式正确。
- 数值边界符合 JobSpec。
- Codex/Claude account mode 合法。
- EIP 模式指定账号数等于 Worker 数。
- Agent API 账号支持所选 Agent/模型。
- rotation 只在适用模式进入最终 spec。

校验失败时自动进入相应步骤、展开高级区域并聚焦第一个错误控件。不得在客户端校验失败时调用 plan 或 submit。

草稿策略：

- 页面内导航使用应用内存保存完整非秘密表单，返回时恢复。
- MVP 不自动把 Job 草稿写入 session/local storage。
- password、email token、Agent API Key、OTP 永不进入草稿。
- `run.env`、`run.secret_env`、Harness code 也不做浏览器持久化，避免误把敏感内容保存到 sessionStorage。
- 后续若增加“显式保存草稿”，必须由服务端提供加密/权限边界和字段级脱敏设计，不能直接序列化 DOM。

计划与提交：

- “仅校验并查看计划”调用 `/api/jobs/plan`。
- “校验并启动”先调用相同 plan，再提交 `/api/jobs`。
- 同一次提交意图复用稳定 `Idempotency-Key`。
- 提交成功后跳转 `/jobs/{job_id}`，但保留成功摘要，避免用户因跳转失败重复提交。

### 6.5 Jobs 列表

Jobs 页面采用服务端分页，默认活跃优先、时间倒序。筛选：

- 状态：preparing/running/succeeded/failed/cancelled/recovered/interrupted。
- 名称或 Job ID。
- Agent、账号、创建时间。
- cleanup pending / 有结果。

列表只展示摘要，不在每轮轮询中为每个 Job 请求结果。每行提供进入详情、取消或结果入口；破坏性操作必须确认和防重入。

活跃 Job 页面每 5 秒刷新当前页；只有终态 Job 的页面使用 30–60 秒或手动刷新。后台标签页暂停，恢复可见时立即刷新一次。

### 6.6 Job 详情

详情区：

- 状态时间线：prepared → launching → running → terminal/cleanup。
- 脱敏后的 JobSpec。
- Worker 表：shard、账号、phase、rotation、EIP、task、final collect、cleanup。
- 全局/单 Worker Job 日志。
- 存活 Worker 的 systemd journal。
- OTP 卡片。
- 结果摘要和进入结果页按钮。
- cancel/resubmit 等操作。

日志仅在日志区域打开时轮询。同一个作用域最多一个请求；切换 Worker 或关闭详情时取消旧请求。迟到的旧请求不得覆盖新 Worker/Job 的日志。

### 6.7 结果

结果页面必须独立于 Jobs 当前页，显示 S3 和 Manager 本地的全部结果摘要。字段：

- Job ID/名称。
- 中间快照或最终收集。
- 文件数、总大小、更新时间。
- score 摘要和有效性。
- S3 URI。
- 进入详情、复制 URI、下载全部。

MVP 在旧 API 上只在进入页面或用户手动刷新时调用 `/api/results`，禁止全局 5 秒轮询。后端结果 catalog 上线后再启用分页和低频更新。

单 Job 展开时才调用 `/api/jobs/{id}/results` 获取文件明细和分数。流式下载继续使用 `/api/jobs/{id}/results/download/stream`。

### 6.8 Workers

复用 `/api/nodes` 已有 `limit/offset/status`，每页最多 100 条。分类计数不能根据当前页推导，使用 summary API。

每行显示：

- Worker/node ID、instance ID。
- 状态、WebSocket、心跳。
- private/public IP。
- Job/账号关联（若 API 提供）。
- drain、terminate、remove 和日志操作。

所有销毁操作需要明确确认；Worker 已释放时只显示历史状态，不再显示 terminate。

## 7. 前端实现架构

### 7.1 目录建议

```text
src/elastic_agent/api/ui_v2/
  index.html
  assets/
    app.css
    icons.svg
  js/
    app.js
    core/
      api.js
      auth.js
      router.js
      store.js
      poller.js
      errors.js
      dom.js
      downloads.js
    components/
      app-shell.js
      data-table.js
      dialog.js
      otp-center.js
      status-badge.js
      toast.js
    pages/
      overview.js
      accounts.js
      account-new.js
      job-new.js
      jobs.js
      job-detail.js
      results.js
      fleet.js
      not-found.js

scripts/
  build_ui_v2.py

deploy/cloudflare/ui-v2/
  wrangler.jsonc
  worker.js

tests/ui_v2/
  ...
```

运行时资产使用原生 ES Modules，不需要前端 runtime dependency。`build_ui_v2.py` 只负责：

- 校验 import 路径。
- 计算 JS/CSS 内容哈希。
- 生成不可变 asset manifest。
- 把 `index.html` 中的入口替换为哈希文件名。
- 输出可部署静态目录。

构建输出不提交仓库。Cloudflare token、account/zone 信息由 CI secret 提供，不写入源码、环境示例或日志。

### 7.2 启动与 Router

`app.js` 的启动顺序必须固定：

1. 清理 URL 中的 `api_key`。
2. 初始化 auth store，但不把 key 写入 DOM。
3. 安装全局 error/unhandled rejection 的安全处理器。
4. 初始化 Router 和 App Shell。
5. 恢复当前深链接。
6. 无 key 时显示认证对话框；成功后继续原路由。
7. 启动 health 和 OTP 全局 poller。
8. 挂载当前页面，并启动该页面独有的 loader/poller。

Router 使用 History API，支持可配置 base path。Canary base 为 `/ui-v2`。每次路由切换：

- 调用旧页面 `dispose()`。
- abort 旧页面所有 fetch。
- 清理 timer、事件监听和临时 object URL。
- 更新 `aria-current="page"`。
- 挂载新页面。
- 将焦点移动到主内容标题，但普通后台状态更新不得抢焦点。

### 7.3 Store 分层

```text
global:
  auth metadata（key 本身由 auth 模块私有持有）
  route
  health
  ui summary
  login attempts
  theme

page cache:
  accounts page/filter
  jobs pages/filter/request generation
  job detail cache
  results pages/request generation
  fleet pages/filter

ephemeral:
  Job form draft（内存）
  active downloads
  open dialogs/log context

never store:
  password/email token/API key/OTP
```

Store 更新必须以不可变 snapshot 或显式 reducer 完成；页面不得通过全局变量互相修改 DOM。所有列表使用实体 key reconcile。

### 7.4 API Client

统一 `api.js` 负责：

- 添加 Bearer Header。
- JSON 编解码与安全错误对象。
- 保留 HTTP status，但不保留原始秘密请求体。
- 接受外部 `AbortSignal`。
- 为 GET 提供 request generation，避免迟到覆盖。
- 401 只触发一次全局重新认证流程并暂停 poller。
- 429/5xx/网络错误由 poller 指数退避；普通交互请求不自动重复破坏性操作。
- 503“服务未配置 API Key/恢复中”和 401 区分展示。

任何错误消息进入页面时使用 `textContent`。禁止把 API 返回字符串直接拼入 `innerHTML` 或 inline event handler。

### 7.5 Poller

统一 poller 必须满足：

- single-flight：前一次未完成时不启动下一次。
- 路由离开时 abort + clear timer。
- `document.hidden` 时暂停；恢复时立即执行一次。
- 网络错误和 5xx 指数退避并加入 jitter；成功后恢复正常周期。
- 4xx 除 401 不无限重试。
- 每个 poller 暴露状态，便于测试是否泄漏。

建议频率：

| Poller | 可见页面 | 周期 |
|---|---|---|
| health | 全局 | 30 秒 |
| OTP | 全局 | 5 秒；隐藏标签页暂停 |
| summary | overview/导航 | 10 秒 |
| Jobs 当前页 | jobs/job detail | 活跃 5 秒，纯终态 30–60 秒 |
| Accounts allocation | accounts | 15–30 秒 |
| Fleet 当前页 | fleet | 5 秒 |
| Job logs | 对话框打开 | 3 秒 |
| Results | results | 默认手动；运行中快照可 30 秒 |

## 8. API 方案

### 8.1 MVP 直接复用

UI v2 静态 canary 可先复用：

- `GET /api/health`
- `GET /api/accounts`
- `GET /api/accounts/allocations`
- `GET /api/accounts/bindings`
- OAuth/Agent API account mutation endpoints
- `GET /api/accounts/login-attempts`
- `POST /api/accounts/login-attempts/{request_id}/otp`
- `POST /api/jobs/plan`
- `POST /api/jobs`
- Job cancel/resubmit/detail/log endpoints
- `GET /api/results`
- 单 Job result/download endpoints
- `GET /api/nodes?limit=&offset=&status=`
- Fleet mutation/log endpoints

适配旧 API 时的限制：Jobs 仍是全量响应；Results 只允许页面进入/手动刷新；不得在 App Shell 启动时预取这些数据。

### 8.2 轻量 UI Summary

新增受 Bearer 保护的 `GET /api/ui/summary`：

```json
{
  "generated_at": "2026-08-10T12:00:00Z",
  "manager": {
    "status": "healthy",
    "provider": "aws",
    "region": "ap-northeast-1",
    "uptime_seconds": 12345.6
  },
  "jobs": {
    "active": 10,
    "by_state": {"running": 10},
    "terminal_total": 533,
    "cleanup_pending": 0
  },
  "workers": {
    "total": 10,
    "connected": 10,
    "by_status": {"running": 10}
  },
  "accounts": {
    "total": 80,
    "enabled": 76,
    "allocated": 10
  },
  "otp": {"pending": 0}
}
```

约束：

- 不返回账号 ID、邮箱、Job 名称或秘密。
- 不扫描 S3，不计算结果总数。
- 最多缓存 5 秒，响应设置 `Cache-Control: no-store`。
- 统计必须来自内存索引、Registry、账号 store 或有界目录元数据，不能调用全量详情 API 再聚合。

### 8.3 Jobs 分页

在保持“无 query 时返回旧全量响应”的兼容期内，为 `/api/jobs` 增加：

```text
GET /api/jobs?limit=50&cursor=<opaque>&state=running&query=abc
```

分页响应：

```json
{
  "jobs": [],
  "total": 543,
  "next_cursor": "opaque-or-null",
  "counts": {
    "active": 10,
    "succeeded": 500,
    "failed": 30,
    "cancelled": 3
  }
}
```

要求：

- 稳定排序为时间倒序、`job_id` 作为 tie-breaker。
- cursor 为 opaque；客户端不得解析。
- limit 默认 50，最大 200。
- filter/query 改变时丢弃旧 cursor。
- cursor 非法返回 400/422，不退回第一页造成重复。
- 服务端搜索作用于完整数据集，而不是当前页。
- 详情 spec 继续只由 `/api/jobs/{id}` 返回，列表不携带 heavy spec。
- API contract 测试必须覆盖旧 `/batch` 的无参数行为。

如果 5,000+ Job 时每次仍需读取、排序所有 JSON journal，应再增加持久 Job summary index；仅限制响应大小不等于限制服务端工作量。

### 8.4 Results 分页与目录

短期：`/api/results` 只在 Results 页面按需调用，不轮询。

规模化方案：在 Manager state 目录增加权限 0600 的 durable Result Catalog，可使用 stdlib SQLite。建议字段：

```text
job_id primary key
source                 s3 | local
s3_uri
snapshot_kind          intermediate | final
file_count
total_bytes
score_summary_json
collection_generation
collected_at
updated_at
```

Catalog 更新来源：

- 周期/最终 collection 成功后 upsert。
- Manager relay upload 成功后 upsert。
- Worker direct S3 collection 根据 `_elastic_agent/collection.json` 更新。
- 提供有界后台 backfill，按 S3 paginator 增量扫描历史前缀；不能在 API 请求线程内一次扫描全部历史。

分页 API：

```text
GET /api/results?limit=50&cursor=<opaque>&query=&snapshot_kind=final
```

响应只包含摘要。文件列表和 score 详情仍由 `/api/jobs/{id}/results` 懒加载。Catalog 不成为结果内容权威来源；S3/local 文件仍是下载和详情的权威，Catalog 只用于列表发现与分页。

### 8.5 Fleet 分页

Fleet 直接使用现有 offset API：

```text
GET /api/nodes?limit=100&offset=0&status=running
```

当前页不得计算全局状态卡片；全局计数来自 `/api/ui/summary`。当 Registry 达到数万节点或 offset 成本成为问题时，再迁移 cursor；1,000 Worker 目标下 offset 足够。

### 8.6 API 兼容策略

- 所有 API 变更 additive。
- Canary 期间不移除旧字段、不改变默认无分页行为。
- UI v2 对 `next_cursor`、summary endpoint 做 capability detection；新 API 未上线时降级到有界的旧行为。
- 前端回滚到 `/batch` 时不依赖回滚后端。
- 任何需要 Manager 重启的 API 发布都必须进入无活跃 Job 的维护窗口。

## 9. 关键交互时序

### 9.1 认证

```text
打开深链接
  → 静态 shell 200
  → 清理 URL api_key
  → 读取 sessionStorage key
  → 无 key：显示登录对话框
  → 使用 /api/ui/summary 或受保护轻量请求验证
  → 成功：恢复原深链接
  → 401：暂停 poller，重新认证
```

“忘记 Key”必须清理内存/sessionStorage、abort 所有请求和下载，并返回安全的认证状态。

### 9.2 添加账号

```text
选择账号类型
  → 客户端字段校验
  → POST OAuth 或 Agent API endpoint
  → 成功：立即清空秘密控件
  → 刷新账号当前页
  → 跳转 /accounts 并聚焦新增行
```

### 9.3 Job 提交

```text
客户端校验
  → POST /api/jobs/plan
  → 展示无秘密 plan
  → 用户确认
  → 生成/复用 Idempotency-Key
  → POST /api/jobs
  → 201 job_id
  → 跳转 /jobs/{job_id}
```

网络断开后只有在 spec 未变化时复用 key。不能通过“页面没跳转”判断提交失败；应先按同一 key重试或查询已返回的 job_id。

### 9.4 OTP

```text
全局 poller 获取 challenges
  → keyed reconcile(login_request_id, challenge_id)
  → 导航徽标/浮层提示
  → 用户输入 6 位码
  → 禁用该 challenge 提交按钮
  → 精确 POST request_id + challenge_id + code
  → 清空输入并刷新 challenges
```

两个账号或 Worker 同时等待时必须生成两个独立卡片和 in-flight key。

### 9.5 结果下载

```text
点击下载
  → 单 Job download lock
  → authenticated fetch(stream endpoint)
  → 读取进度响应头/字节流
  → File System Access API 可用：流式写文件
  → 不可用且文件可能过大：给出明确兼容提示
  → 用户取消：abort + reader.cancel + 清理 UI 状态
```

## 10. 无中断部署设计

### 10.1 为什么不能直接部署到 Manager

当前 Manager 是单控制器进程。重启会执行主动 Job shutdown，而不是简单等待 Worker 重连。启动第二个 Manager 也不安全，因为存在 controller lock、内存 Job 归属、Worker WebSocket、账号 claim 和 EIP recovery 边界。

因此，只要有活跃 Job，以下操作都禁止：

- 替换 Manager Python release 并重启 systemd。
- 通过 `--reload` 或多 Uvicorn worker 热加载。
- 启动第二个连接相同 state/registry/provider 的 Manager。
- 重启当前唯一 Cloudflare Tunnel connector 来尝试增加静态 path。

### 10.2 Canary：Cloudflare Workers Static Assets

在 Cloudflare 边缘增加严格限定的 route：

```text
elastic-agent.claude-code-manager.com/ui-v2
elastic-agent.claude-code-manager.com/ui-v2/*
```

流量边界：

```text
/ui-v2/*  → Cloudflare Worker Static Assets
/api/*    → 原 Cloudflare Tunnel → Manager
/ws/*     → 原 Cloudflare Tunnel → Manager
/batch    → 原 Manager UI
/fleet    → 原 Manager UI
```

Worker route 不能使用会覆盖全站的宽泛模式。SPA fallback 只在 `/ui-v2/*` 内生效；任何 `/api`、`/ws`、结果下载请求都不得回落到 `index.html`。

同源部署使 UI 继续调用绝对路径 `/api/*`，无需启用 CORS，也不会改变 API Key 的 sessionStorage origin。

### 10.3 静态发布要求

- 每次发布生成不可变版本号、Git commit 和 asset manifest。
- `index.html`：`Cache-Control: no-store` 或 `no-cache, must-revalidate`。
- 哈希 JS/CSS：`Cache-Control: public, max-age=31536000, immutable`。
- 正确 MIME，并设置 `X-Content-Type-Options: nosniff`。
- 建议安全头：
  - `Content-Security-Policy: default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self' wss:; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'`
  - `Referrer-Policy: no-referrer`
  - `Permissions-Policy` 关闭不需要的浏览器能力。
- 不使用 inline script/onclick，不加入 `unsafe-inline`。
- 不注册 Service Worker。
- Cloudflare 部署凭证只放 CI secret；日志不得输出 token、API Key 或账号秘密。

### 10.4 发布阶段

#### 阶段 A：预览

1. 构建静态包并记录 manifest。
2. 发布到 preview route 或 preview hostname。
3. 使用 Mock/测试 Manager 完成写流程。
4. 使用生产 `/api` 只读验证 health、accounts、jobs、results、fleet。
5. 确认请求没有进入 `/ws` 或改变 Manager 配置。

#### 阶段 B：`/ui-v2/` 管理员 canary

1. 启用精确 `/ui-v2*` Worker route。
2. 旧 `/batch` 保持默认入口。
3. 少量管理员直接访问 `/ui-v2/overview`。
4. 观察至少一个完整 Job 周期，覆盖添加测试账号、plan、隔离测试 Job、OTP、日志、结果和取消。
5. 核对 Manager CPU/内存、API QPS、错误率和事件循环延迟。

生产写 canary 必须使用明确的隔离测试账号/Job；不能把真实运行中的 Job 当测试对象。

#### 阶段 C：默认入口切换

1. 确认 canary 门禁全部通过。
2. 添加只匹配精确 `/` 的 Cloudflare Redirect Rule 到 `/ui-v2/overview`。
3. 不改变 `/api/*`、`/ws/*`、`/batch`、`/fleet`。
4. 保留旧 UI 一个完整观察期。

该切换只发生在边缘，不需要活跃 Job 归零，也不重启 Manager。

#### 阶段 D：后端/API 发布

Jobs/Results 分页、summary 和 catalog 需要发布 Manager 代码。发布前必须：

1. 暂停新的 Job admission；不能只隐藏 UI 按钮，因为 API 客户端仍可提交。
2. 等待活跃 Job、launch、login、final collect 和 cleanup 全部为 0。
3. 连续两次读取状态确认稳定为空。
4. 备份当前 release、systemd 配置和 state 元数据。
5. 运行完整 contract、migration 和 production launcher 测试。
6. 重启 Manager，等待 health 与 startup recovery ready。
7. 验证 Worker 重连、账号/EIP store、S3 和 UI API。
8. 运行一个隔离 canary Job。
9. 恢复 admission。

当前系统没有独立的在线维护开关；实现阶段应增加明确的 Job admission gate，或在 Cloudflare/API 边界临时拒绝新的 Job submit/resubmit。该 gate 不能阻断 health、GET、Worker WebSocket、PROCESS_EXIT、OTP 或 cleanup 流量。

### 10.5 回滚

静态 UI 回滚不触碰 Manager：

- 单版本故障：将 Worker deployment 回滚到上一静态版本。
- 整体撤回：禁用 `/ui-v2*` Worker route。
- 默认入口故障：禁用根路径 Redirect Rule。
- 旧 `/batch` 始终可直接使用。

至少保留两个已验证静态版本，不在发布当天删除上一版本。

后端 API 必须 additive，保证前端回滚后旧 UI 仍工作。若后端发布失败，只能在确认无活跃 Job 的维护窗口回滚 Manager release；禁止为了 UI 问题在运行中重启 Manager。

立即回滚触发条件：

- API Key、password、email token、Agent API Key、OTP 或 env 泄漏。
- 重复 Job 提交。
- OTP 串到错误账号/Worker。
- EIP/Worker 破坏性操作误触发。
- 新 UI 导致 Manager 明显 QPS、CPU、内存或错误率劣化。
- 结果下载损坏、无界内存或无法取消。

## 11. 安全设计

### 11.1 XSS 与 DOM

- 服务端字符串默认通过 `textContent`、属性赋值和 `addEventListener` 使用。
- 禁止 inline handler 和拼接 `<script>`。
- 如必须渲染受控 HTML，使用集中模板/escape，并以 `<img onerror>`、`</script>`、引号、Unicode 控制字符做测试。
- Job/account/worker ID 必须在 URL 中使用 `encodeURIComponent`。
- CSP 不允许任意第三方脚本。

### 11.2 浏览器存储与缓存

- `sessionStorage` 只保存 API Key 与非敏感 UI 偏好；Key 仍由 auth 模块封装，不暴露给普通组件。
- 禁止 `localStorage`、IndexedDB 和 Service Worker 缓存认证或业务数据。
- account secret、OTP、env/secret_env 不进入浏览器持久存储。
- HTML 不缓存；敏感 API 和日志/下载设置 `no-store`。

### 11.3 API 与错误

- 保持全局 422 handler 不返回 rejected input 或 validator context。
- 前端错误对象不得附带 request body。
- 401 停止轮询并请求重新认证；不能无限重试。
- 破坏性请求不因 5xx/网络断开自动重放。
- 结果流设置 `nosniff/no-store`，文件名只能来自安全 Job ID。

## 12. 测试策略

### 12.1 迁移现有测试意图

`tests/unit/test_web_ui.py` 当前约 842 行、25 项，主要通过正则检查内联 HTML/CSS/JS。ES Modules 拆分后，不能简单删除这些测试。迁移方式：

- FastAPI/静态路由：`pytest + ASGITransport`。
- `buildJobSpec`、validator、router、store、poller、result merge：模块单测。
- 导航、焦点、OTP、下载、响应式：Playwright。
- API 行为继续由现有 API tests 覆盖。
- Canary 期旧 `/batch` 测试保持全绿；只有等价 v2 测试存在后才能移除旧源码断言。

### 12.2 路由与导航

- 每个路由可直接访问、刷新、前进和后退。
- 导航不整页刷新，当前项有 `aria-current="page"`。
- API Key 输入后恢复原深链接。
- 非法 UI 路由显示 404；不得误请求同名 API。
- 静态 shell 无 key 返回 200，数据 API 无 key 返回 401。
- JS/CSS MIME、缓存、nosniff 和 SPA fallback 正确。

### 12.3 认证与秘密

- Key 不进入 URL、Cookie、localStorage、DOM、Referer 和下载 URL。
- `?api_key=` 在 API 请求前被清除且不能认证。
- 401 暂停 poller；更新 key 后只重试一次。
- password/token/API key/OTP/secret env 不进入草稿、toast、console 或异常。
- 422 不回显哨兵秘密。
- 忘记 key 会 abort fetch/download 并清空会话。

### 12.4 Job 表单

- 八组现有字段语义与 JobSpec golden 输出一致。
- account mode、Agent、EIP、rotation、model 动态约束全覆盖。
- 客户端非法输入不调用 plan/submit。
- 校验跳转到正确步骤并聚焦错误。
- 双击、慢网络、重试只创建一个 Job。
- 页面切换恢复内存草稿，但秘密不恢复。
- Harness 默认关闭边界不变。

### 12.5 OTP

- 多账号、多 Worker challenge 不串位。
- `(login_request_id, challenge_id)` 精确提交。
- 6 位校验、防双击、成功清空。
- 404/409/410 正确展示且不自动重试。
- 页面切换不创建重复 poller。
- 新 challenge 不抢占用户正在编辑的其他输入框焦点。

### 12.6 Jobs、日志与结果

- 所有 Job/Worker phase、cleanup pending 和资源已释放状态有显示测试。
- 列表 reconcile 保留焦点、展开、滚动。
- 迟到响应不覆盖新状态。
- 日志只有打开时轮询；隐藏、暂停、关闭、terminal 后停止。
- Worker journal 404/409 后停止并回退归档提示。
- Results 只加载当前页，Job 详情只加载当前 Job。
- 中间快照和最终收集区分。
- 流式下载覆盖认证、进度、取消、reader cancel 和大文件保护。

### 12.7 分页、性能与并发

测试数据至少包括：

- 1,000 Workers。
- 10 活跃 Job。
- 5,000 历史 Job。
- 10,000 结果摘要。
- 单 Job 100 Workers、5,000 行日志。

验收：

- 连续翻页无重复/遗漏，过滤后 cursor 重置。
- 无效 cursor 返回 400/422。
- 首屏不预取其他页面数据。
- Jobs/Results/Fleet DOM 行数受 page size 限制。
- 60 秒运行无 timer/fetch 增长；往返切页 50 次后 poller 回到基线。
- 不再产生“最多 30 Job × 结果查询”的刷新风暴。
- 100 个并发浏览器标签模拟时请求频率仍有界。

### 12.8 移动端与无障碍

视口至少：375×667、768×1024、1440×900。

- 无页面级横向滚动。
- 导航抽屉 focus trap、Esc、焦点恢复正确。
- 320 CSS px 与 200% 缩放下仍可完成关键流程。
- 可全键盘完成认证、导航、添加账号、plan、submit、OTP、日志、下载和取消。
- 每页一个 `h1`，标题层级连续，存在 nav/main landmark。
- 图标有 accessible name，颜色不是唯一状态提示。
- 普通轮询更新不反复打扰 screen reader。
- axe 目标 WCAG 2.2 AA，critical/serious 为 0。

### 12.9 CI 分层

1. Python API/unit：现有测试 + static route、summary、分页、catalog。
2. JS module unit：spec builder、validator、router、store、poller、escaping。
3. Playwright mocked API：完整 UI 流程和响应式。
4. ASGI/browser integration：真实 FastAPI + Fake Manager。
5. axe/Lighthouse：主要页面和 dialog。
6. Static build reproducibility、asset manifest 和 CSP 检查。
7. 生产前 smoke：默认只读；写测试使用隔离账号和 canary Job。

## 13. 性能与可观测性

建议预算：

- gzip 后首屏 JS < 250 KiB，CSS < 80 KiB。
- 测试环境 P95 首次可交互 < 2.5 秒。
- 客户端路由切换 < 300 ms。
- 当前页并发 GET 默认不超过 4。
- 单 poller 永不重叠。
- Results 页面不在后台高频扫描 S3。
- Fleet 每页 ≤100，Jobs/Results 每页默认 50、最大 200。

Canary 观察项：

- UI JS error/unhandled rejection。
- 401/403/409/422/429/5xx 比例。
- `/api/jobs`、`/api/results`、logs 和 downloads P50/P95。
- Manager CPU、RSS、事件循环延迟。
- OTP 出现到提交的成功率和过期率。
- 重复 Job 提交数，目标为 0。
- 下载成功、失败、取消和传输字节。
- 页面 poller 数与 API QPS。

客户端诊断不得包含 API Key、账号秘密、OTP、env 或完整敏感 URL。第一版可以只在浏览器 console 输出经过脱敏的版本/route/request-id，不必新建遥测写 API。

## 14. 分阶段工作包

### Phase 0：契约冻结

- 确认路由、中文术语和页面字段。
- 把现有 `test_web_ui.py` 的业务意图映射到 v2 测试。
- 确认 Cloudflare Workers Static Assets、Routes、Redirect Rules 权限。
- 确认 `/ui-v2*` 不与现有 Worker/WAF/Access/cache 规则冲突。
- 决定 Results catalog 是否与首次默认切换同批上线。

完成标准：本文未决项被明确记录，没有生产变更。

### Phase 1：静态 App Shell

- 建立 ES Module 目录、build script 和 asset manifest。
- 实现 auth、api client、router、store、poller、shell、toast/dialog。
- 实现 `/ui-v2` base path、404、主题和响应式导航。
- 实现全局 health/OTP。
- 补静态路由、模块和基础 Playwright 测试。

完成标准：可在 Fake Manager 上导航；无秘密持久化；切页无 poller 泄漏。

### Phase 2：页面迁移

- Accounts + Account New。
- Job New + plan/submit/idempotency。
- Jobs + Job Detail + logs/OTP。
- Results + streaming download。
- Fleet pagination/actions。
- 逐项迁移旧 UI 测试意图。

完成标准：旧 `/batch` 与 `/ui-v2` 对同一 API 的关键行为一致。

### Phase 3：规模化 API

- `/api/ui/summary`。
- Jobs 服务端分页/筛选。
- Result Catalog、backfill 和 Results 分页。
- Fleet 全局计数修正。
- 负载和错误注入测试。

完成标准：目标规模下响应、DOM、内存和 QPS 有界。

### Phase 4：边缘 canary

- 发布 preview。
- 配置精确 `/ui-v2*` Worker route。
- 管理员 canary 和一个隔离 Job 全流程。
- 观察性能、安全和 Manager 指标。
- 演练静态版本回滚和 route 禁用。

完成标准：不重启 Manager，活跃 Job/Worker 数不因发布变化。

### Phase 5：默认切换与后续清理

- 根路径精确重定向到 `/ui-v2/overview`。
- 保留 `/batch` 观察期。
- 无活跃 Job 的维护窗口发布 additive API。
- 稳定后再决定是否将静态资产并回 Python wheel、启用干净路由或退役旧内联 UI。

## 15. 上线验收清单

- [ ] `/ui-v2/*` 发布和回滚均不重启 Manager/Tunnel。
- [ ] `/api/*`、`/ws/*` 未被静态 fallback 接管。
- [ ] 旧 `/batch`、`/fleet` 可用。
- [ ] API Key 不出现在 URL、Cookie、localStorage、DOM 和下载 URL。
- [ ] 账号秘密、OTP、env 不持久化、不进日志。
- [ ] Job plan/submit 语义与旧 UI 一致。
- [ ] 重复提交测试为 0。
- [ ] 多 challenge OTP 不串位。
- [ ] EIP decommission 双确认和顺序不变。
- [ ] Job/Worker/Result 列表有界且页面级轮询无泄漏。
- [ ] 日志和结果下载可暂停/取消。
- [ ] 深链接、前进后退、刷新和移动端通过。
- [ ] axe critical/serious 为 0。
- [ ] 目标规模测试通过。
- [ ] Canary 指标无明显劣化。
- [ ] 静态回滚演练通过。
- [ ] 需要 Manager 重启的发布已确认活跃 Job/cleanup 为 0。

## 16. 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| 直接重启 Manager | 活跃 Job 被取消并销毁 Worker | 静态 edge canary；后端发布只在零活跃维护窗口 |
| Worker route 匹配过宽 | API/WS 被静态站点接管 | route 严格限定 `/ui-v2*`，发布前做负向路由测试 |
| 拆页后重复 poller | Manager QPS/事件循环负载上升 | route dispose、single-flight、AbortController、泄漏测试 |
| Results 页面扫描全 S3 | 高延迟、请求风暴 | MVP 手动加载；Result Catalog + pagination |
| DOM 重建导致 OTP/focus 丢失 | 验证码误提交、操作中断 | keyed reconcile，OTP 真实节点保留，浏览器测试 |
| 草稿泄漏秘密 | API Key/password/env 落浏览器存储 | MVP 只内存草稿，字段级 never-store 策略 |
| 幂等 key 使用错误 | 重复 Job 或错误复用旧提交 | spec fingerprint + per-intent key 状态机 |
| 旧 UI 无法回滚 | UI 故障影响运维 | additive API，旧 `/batch` 保留并持续测试 |
| 第二 Manager/多 worker 进程 | controller/lease/WS 归属冲突 | 明确禁止；静态 UI 与 Manager 进程隔离 |
| Cloudflare 凭证泄漏 | Edge 配置被篡改 | CI secret、最小权限、禁止日志输出、版本回滚 |

## 17. 实施前未决项

1. Cloudflare 账户是否已开通 Workers Static Assets，以及是否具备 Scripts/Routes/Redirect Rules 权限。
2. `/ui-v2*` 是否与现有 WAF、Access、缓存或其他 Worker route 冲突。
3. UI 文案是否统一为中文，还是保留 API/状态名英文。
4. Result Catalog 是否作为默认入口切换的前置条件；建议在结果规模继续增长前完成。
5. 是否在后端增加 durable Job summary index，避免 5,000+ journal 每次分页仍全量读取。
6. 新 Job admission gate 放在 Manager 还是 Cloudflare 运维层；长期建议 Manager 提供显式、可审计的维护模式。
7. 旧 `/batch` 的保留周期和退役标准。
8. UI v2 最终是否继续由 Cloudflare 托管，还是在零活跃维护窗口并回 Python wheel。

在上述未决项确认前，可以完成本地 App Shell、页面模块与测试，但不应修改生产默认入口。
