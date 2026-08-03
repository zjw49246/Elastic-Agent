# JSON JobBatch 操作说明

JSON JobBatch 用于一次上传多份完整 `JobSpec`，由 Manager 按容量逐个接受并启动。它适合一组参数不同、其余配置相同的任务，例如多个 AI4SCI seed。

## 使用方式

1. 在 Batch Console 的“提交 Job”区域切换到“批量 JSON”。
2. 选择本地 `.json` 文件并点击“解析并校验全部”。
3. 检查 Job 数、总 Workers、Worker-hours、实例类型、账号来源/组/绑定方式、并发量和逐项警告。
4. 只有全部 Job 都通过服务端 preflight 后，页面才会开放确认按钮。
5. 最后由管理员确认，才会接受批次并开始创建真实资源。

文件只保存在当前页面内存中，不写入 `localStorage` 或 `sessionStorage`。页面会在 preflight 和正式提交时发送同一份 JSON 内容；重新选择文件会清除旧计划和回执。

## Manifest schema v1

```json
{
  "schema_version": 1,
  "batch_id": "ai4sci-replay-20260801-a",
  "policy": {
    "max_active_jobs": 2,
    "on_job_failure": "continue"
  },
  "jobs": [
    {
      "client_id": "seed-7263",
      "spec": {
        "name": "完整 JobSpec"
      }
    }
  ]
}
```

- `batch_id` 和每个 `client_id` 只允许字母、数字、点、下划线和连字符，长度不超过 128。
- `client_id` 在一个 manifest 内必须唯一。
- `spec` 必须是完整、可独立提交的声明式 `JobSpec`，不支持公共模板、`$ref` 或继承。
- `policy` 可省略；默认 `max_active_jobs=3`、`on_job_failure="continue"`。
- 单个 manifest 的 `max_active_jobs` 始终限制在 1–10；Manager 的
  `ELASTIC_AGENT_JOB_BATCH_MAX_ACTIVE_JOBS` 是跨所有批次的独立全局上限，
  可配置为 1–50，不会放宽单批 schema。
- v1 只支持 `continue`：一项提交或执行失败会被记录，但不会取消其他项。
- schema 硬限 100 项；部署默认最多 20 项、100 Workers、1440 Worker-hours、3 个活跃 Job，请以页面 preflight 返回为准。
- 文件和请求体上限均为 2 MiB。

## AI4SCI Codex 示例

下面是一项 seed `7263` 的完整配置。批量执行时，将整个 `jobs[0]` 复制多份，为每份设置唯一 `client_id`，并同步替换 Job 名称、`--seed`、`--output-dir`、`run.resume_command` 和 `rotation.resume_args` 中的 seed。

```json
{
  "schema_version": 1,
  "batch_id": "ai4sci-codex-replay-20260801-a",
  "policy": {
    "max_active_jobs": 2,
    "on_job_failure": "continue"
  },
  "jobs": [
    {
      "client_id": "seed-7263",
      "spec": {
        "name": "ai4sci-codex-seed7263",
        "environment": {
          "profile": "ubuntu-agent-docker-v1"
        },
        "setup": {
          "repo": "https://github.com/apexin-ai/Agent-AI4Sci-Bench.git",
          "ref": "archive/youchengsong-managed-agent-api-20260728",
          "target_dir": "/opt/elastic-agent/harness",
          "commands": [
            "curl -LsSf https://astral.sh/uv/install.sh | sh",
            "export PATH=\"$HOME/.local/bin:$PATH\" && uv python pin 3.13 && uv sync --python 3.13"
          ],
          "deliver": "manager_rsync",
          "needs_docker": true
        },
        "run": {
          "command": "uv run ai4sci-bench codex-replay-run --tasks all --prompt-levels b1,b2,b3,b4 --instances-per-task 1 --sandbox os --seed 7263 --agent-config '{\"model\":\"gpt-5.6-sol\",\"effort\":\"xhigh\"}' --parallel 6 --replay-session-workers 2 --replay-workers 8 --max-pending-replays 8 --replay-resume --output-dir results/codex_5_6_sol_seed7263",
          "resume_command": "uv run ai4sci-bench codex-replay-run --tasks all --prompt-levels b1,b2,b3,b4 --instances-per-task 1 --sandbox os --seed 7263 --agent-config '{\"model\":\"gpt-5.6-sol\",\"effort\":\"xhigh\"}' --parallel 6 --replay-session-workers 2 --replay-workers 8 --max-pending-replays 8 --replay-resume --output-dir results/codex_5_6_sol_seed7263 --resume results/codex_5_6_sol_seed7263",
          "env": {
            "AI4SCI_SANDBOX_CPU": "1",
            "AI4SCI_SANDBOX_MEM": "4g"
          },
          "secret_env": {
            "TOKENROUTER_API_KEY": "aws-secretsmanager://elastic-agent/ai4sci/tokenrouter-api-key"
          },
          "cwd": ".",
          "timeout": 259200,
          "shell": true
        },
        "account": {
          "agent_type": "codex",
          "mode": "worker_local_login",
          "per_worker": 1,
          "group": "standard",
          "binding": "none",
          "ids": [],
          "config_dir": "/home/ubuntu/.codex"
        },
        "rotation": {
          "strategy": "on_exhaust_restart_resume",
          "resume_args": "--resume \"results/codex_5_6_sol_seed7263\"",
          "max_rotations": 20
        },
        "fanout": {
          "workers": 1,
          "shard_by": "hostname",
          "name_prefix": "ai4sci-codex-seed7263",
          "instance_type": "r5.2xlarge",
          "disk_gb": 100,
          "spot": false
        },
        "collect": {
          "paths": ["results"],
          "checkpoint": false,
          "interval_seconds": 120
        },
        "ttl_seconds": 259200
      }
    }
  ]
}
```

`run.secret_env` 只接受 AWS Secrets Manager 或 SSM 引用，不接受明文 Key。不要从脱敏后的 Job JSON 复制 `[REDACTED]` 或 `[SECRET_REFERENCE]`；页面和 API 会拒绝这些占位符。若应用确实不调用对应 evaluator，可以删除该环境变量；否则必须提供部署中真实存在且 Worker IAM 有权读取的引用。

## 幂等与状态

Console 用 `batch_id` 派生稳定的 `Idempotency-Key`。网络超时后，用原文件重试不会重复创建 Job；同一 `batch_id` 改了内容会收到 `409`，要启动新的逻辑批次必须显式更换 `batch_id`。

直接调用 API 时，调用方必须自行保存并复用同一个 `Idempotency-Key`：

```bash
curl -fsS -X POST "$ELASTIC_AGENT_URL/api/job-batches/plan" \
  -H "Authorization: Bearer $ELASTIC_AGENT_API_KEY" \
  -H 'Content-Type: application/json' \
  --data-binary @job-batch.json

curl -fsS -X POST "$ELASTIC_AGENT_URL/api/job-batches" \
  -H "Authorization: Bearer $ELASTIC_AGENT_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: ai4sci-codex-replay-20260801-a' \
  --data-binary @job-batch.json
```

状态含义：

- `queued`：已持久接受，等待 Job 数或 Worker 容量。
- `accepted`：底层单 Job 已通过其自身幂等边界并开始生命周期。
- `terminal`：底层 Job 已结束；实际结果看 `job_state`，例如 `succeeded`、`failed`、`cancelled`。
- `error`：该项未能通过底层 Job 接受边界；错误被隔离，后续项继续。

Manager 重启会从权限 `0600` 的私有 journal 恢复队列。JobBatch 不是云事务：批次被接受后，每个 Job 独立启动和结束，不会因后续某项失败而回滚已经启动的 Job。需要停止时，应通过现有单 Job 操作处理回执中的 `job_id`。
