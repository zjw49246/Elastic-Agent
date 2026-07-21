# Elastic-Agent

Elastic computing framework for managing cloud-based agent workers.

## Installation

```bash
uv add git+https://github.com/zjw49246/Elastic-Agent.git
```

## Overview

Elastic-Agent is a Python library that provides:

- **Multi-cloud resource management** — Unified CloudProvider interface for Alibaba Cloud ECS and AWS EC2
- **Worker Runtime** — WebSocket-based communication between Manager and Workers
- **Task scheduling** — Capacity-aware task distribution with pluggable Harness interface
- **File sync** — Automatic file synchronization from Workers to OSS/S3
- **Credential management** — Account pool with auto-login, quota monitoring, and rotation
- **AWS account/EIP affinity** — Keep one public IP per stable account ID while creating and destroying EC2 workers per Job
- **PTY-hosted execution** (optional) — Workers host Claude Code in persistent PTY sessions via [claude-pty](https://github.com/zjw49246/Claude-Code-PTY) instead of spawning `claude -p` per task

## Usage

```python
from elastic_agent.manager import ElasticAgentManager
from elastic_agent.core.providers import AliyunProvider
from elastic_agent.harness import Harness

class MyHarness(Harness):
    def get_bootstrap_steps(self):
        return [...]

manager = ElasticAgentManager(
    harness=MyHarness(),
    provider=AliyunProvider(config),
)
app = manager.create_app()
```

## PTY mode (claude-pty)

When enabled, Workers host Claude Code in a persistent interactive PTY session:
prompts are delivered via MCP channel injection (stdin fallback), output is read
from the session JSONL, and follow-ups can reuse the warm session. Rate limits
surface as non-zero exits, so credential rotation keeps working unchanged.

Enable in three places:

1. **Bootstrap** — install claude-pty on Workers:
   `build_default_bootstrap_steps(..., include_pty=True)`
2. **Manager** — attach structured launch params to EXECUTE messages:
   `TaskRouter(..., agent_type=ClaudeCodeAgentType(), use_pty=True)`
3. **Worker** — nothing to configure; if `ExecuteMessage.agent_params` is set
   and claude-pty is importable, the runtime uses a PTY session and falls back
   to subprocess execution otherwise. `command` is always sent as fallback.

Warm-session follow-ups: when a follow-up EXECUTE carries
`resume_session_id` and the Worker's session pool still holds that live
session, the prompt is injected into the warm session as a new turn — no
process respawn, no cold `--resume` (verified: ~3x faster turnaround).
A STOP tears the session down; the next resume is cold.

Requires claude-pty >= commit aa23aab (cross-host inject isolation: OS-assigned
inject ports + session_id validation on /inject).

Credential rotation: account swaps are in-place (new tokens written into the
same config_dir). On CREDENTIAL_LOGIN the Worker recycles every PTY session
bound to that config_dir — warm sessions authenticated under the old account
must not be hot-reused; the next EXECUTE cold-resumes with the new
credentials.

Timeouts: `ExecuteMessage.timeout` (or `agent_params.response_timeout`) is
plumbed into the PTY session's turn timeout, so long production turns are not
cut off by claude-pty's 30-minute default. The runtime keeps a hard watchdog
at timeout+60s as backstop.

Protocol notes:
- Events with the original session-JSONL line are forwarded verbatim as stdout
  NDJSON, so Manager-side parsers see native Claude Code types.
- Interactive sessions emit no `result` line; the Worker synthesizes one at
  turn end (`synthesized_by: "pty_backend"`) carrying the session_id.
  `cost_usd` is not available in PTY mode.

## Batch jobs (declarative)

Two task shapes are supported:

- **Mode A — Elastic-hosted agent** (PTY, above): a task is a prompt; Elastic
  hosts Claude Code and rotates credentials per turn.
- **Mode B — opaque long command**: a task is an arbitrary shell command (e.g. a
  benchmark harness that spawns its own sandboxes and consumes the account
  internally). Elastic provisions the worker, logs an account in locally, runs
  the command, watches its output for exhaustion, and rotates by restarting with
  the harness's own `--resume`.

Mode-B jobs are described declaratively as a **JobSpec** — no Python subclass
needed — and fanned out across the fleet:

```python
from elastic_agent.core.job_spec import JobSpec

spec = JobSpec.model_validate({
    "name": "ai4sci-opus48-seed128",
    "setup": {"repo": "https://github.com/ApexIntelligence-AI/Agent-AI4Sci-Bench.git",
              "commands": ["uv sync"]},
    "run": {"command": 'uv run ai4sci-bench run --output-dir "results/opus48_$(hostname -s)_seed128"',
            "env": {"AI4SCI_SANDBOX_CPU": "1", "AI4SCI_SANDBOX_MEM": "4g"},
            "cwd": "Agent-AI4Sci-Bench"},
    "account": {"mode": "worker_local_login", "per_worker": 1},
    "rotation": {"strategy": "on_exhaust_restart_resume",
                 "resume_args": '--resume "results/opus48_$(hostname -s)_seed128"'},
    "fanout": {"workers": 8, "shard_by": "hostname"},
})
job = await manager.batch.launch(spec)   # scale → bootstrap → login → run, per worker
```

Template `{{shard_index}}` / `{{num_shards}}` / `{{hostname}}` are rendered by the
Manager; shell constructs like `$(hostname -s)` are evaluated on the worker.

### Account data and auto-login scope

The Manager stores account ID, email, group, and the mailbox/接码 authorization
token in its accounts JSON file with mode `0600`. The token is write-only in
REST responses (`has_email_token` only). `ACCOUNT_LOGIN` sends it to the chosen
worker; a cross-host Manager URL must be `wss://` unless
`ELASTIC_AGENT_ALLOW_INSECURE_ACCOUNT_LOGIN=1` is deliberately enabled on a
trusted test network. The worker generates the Claude OAuth credentials, never
returns them, verifies that `claude auth status` reports the selected email
exactly (case-insensitive), and runs a successful warm-up command before the Job.

Automatic login currently supports **Claude only**. A group named `codex` is
only a pool label; there is no `codex login`, `CODEX_HOME`, or Codex execution
path yet. The implemented mailbox backends are 171mail and mail.com relay/Web,
not generic IMAP. A future IMAP integration (for example QQ Mail) should use an
app-specific mailbox authorization code/password, not the normal web password.

### One account, one AWS EIP

Set `account.binding` to `"eip"` when an account must always use the same public
IP without keeping an EC2 instance around between Jobs. The durable mapping is
keyed by `account.id`, not email, and its binding/lease journal is fsynced with
mode `0600`. A Job reserves all requested account/EIP leases concurrently,
waits for every cloud transaction to settle, and only then creates temporary
EC2 instances and attaches the addresses. Its terminal lifecycle is:

```text
final collect → detach EIP → terminate EC2 and its root EBS → release lease
                                                           ↳ retain EIP
```

For example, explicitly assign two configured accounts to two workers:

```python
spec = JobSpec.model_validate({
    "name": "fixed-egress-run",
    "run": {"command": "uv run benchmark --shard {{shard_index}}"},
    "account": {
        "mode": "worker_local_login",
        "binding": "eip",
        "per_worker": 1,
        "ids": ["account-001", "account-002"],
    },
    "rotation": {"strategy": "none"},
    "fanout": {
        "workers": 2,
        "shard_by": "shard_index",
        "region": "us-east-1",
    },
    "collect": {"paths": ["results"]},
})
job = await manager.batch.launch(spec)
```

Leave `account.ids` empty to let the allocator choose one account per worker
from `account.group`. If IDs are supplied, they must be unique and their count
must equal `fanout.workers`.

Current EIP-binding constraints:

- AWS only; the Job region, Manager AWS region, and the account's existing EIP
  region must match.
- `account.per_worker` must be `1`. In-place
  `on_exhaust_restart_resume` account rotation is rejected because another
  account means another EIP and therefore a new worker.
- A new EC2 worker still performs a fresh worker-local login. An EIP preserves
  only the public IPv4; it does not preserve `auth.json`, a browser profile, or
  a device fingerprint. EIP bootstrap disables IPv6 before login so traffic
  cannot silently bypass the stable address. It also rsyncs the Manager's
  currently running `elastic_agent` package and starts that worker runtime from
  source, stops any legacy runtime, and requires a fresh WebSocket reconnect, so
  request correlation, exact-email verification, and warm-up checks cannot
  silently fall back to an older PyPI worker. EIP specs reject `run.env.HOME`
  and `run.env.CLAUDE_CONFIG_DIR`; the verified credential directory is injected
  by the orchestrator. Only the generated Claude OAuth credentials stay
  worker-local; the mailbox token follows the boundary above.
- Releasing a Job keeps the EIP allocated and billable. AWS charges public IPv4
  addresses whether attached or idle, and the default EIP quota is commonly
  five per Region; request a quota increase and review current
  [VPC pricing](https://aws.amazon.com/vpc/pricing/) and
  [VPC quotas](https://docs.aws.amazon.com/vpc/latest/userguide/amazon-vpc-limits.html)
  before provisioning a large account pool.

Bindings are created lazily on first EIP Job or explicitly through the
authenticated management API:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/accounts/bindings` | List durable account/EIP mappings |
| `GET` | `/api/accounts/{account_id}/binding` | Read one mapping |
| `PUT` | `/api/accounts/{account_id}/binding` | Idempotently allocate/ensure the EIP; optional body `{"region":"us-east-1"}` |
| `POST` | `/api/accounts/{account_id}/binding/decommission` | Permanently release the EIP; requires `{"release_eip":true,"confirm_account_id":"..."}` and no active claim/lease |
| `GET` | `/api/accounts/allocations` | Inspect current account-to-Job/worker allocations |

Deleting an account identity does not release infrastructure implicitly: first
call the explicit decommission endpoint. This separation prevents an ordinary
Job cleanup or account edit from losing its stable address.

Manager-wired `submit()` and `launch()` (including REST) atomically persist the
JobSpec in a mode-`0600` recovery journal before registration, account/EIP
reservation, or cloud creation; a journal failure produces no launch side
effect. An EIP Job also reserves the whole fanout against provider
`max_instances` before allocating any EIP. Schema limits are 100 workers, 32
accounts per unbound worker, 100 rotations, 2048 GiB disk, a 30-day maximum for nonzero run timeouts,
and an 86,400-second collection interval; provider `max_instances` defaults to
30. At terminal state, periodic collection stops and final collection is awaited
for up to three attempts/300 seconds before teardown. Collection failure marks
the Job failed but does not retain the billable EC2 indefinitely.

**Upload-code escape hatch**: for jobs needing custom logic, set
`harness_ref: "module:Class"` (or upload a `.py` via `POST /api/jobs/harness`) to
drive the job with a real `Harness` subclass instead.

**Frontend**: the Manager serves a **Batch Console** at `/batch` — an Accounts
panel (email + 接码 token pool), a Job form (both the declarative and upload-code
paths), and a live per-worker Job monitor. REST: `/api/accounts`, `/api/jobs`,
`/api/jobs/harness`. The mailbox token is entered through this UI, stored by the
Manager as a write-only field, and sent over the protected login channel; only
the generated Claude OAuth credentials stay on the worker.

Live batch runs require provision/login hooks wired at deployment:
`manager.configure_batch(provision_hook=..., login_hook=...)`.

## Development

```bash
uv sync --extra dev
uv run pytest -q
```

Run the EIP lifecycle and integration-focused unit tests with:

```bash
uv run pytest -q \
  tests/unit/test_account_binding.py \
  tests/unit/test_binding_manager.py \
  tests/unit/test_provider_eip.py \
  tests/unit/test_aws_provider.py \
  tests/unit/test_batch_hooks.py \
  tests/unit/test_batch_orchestrator.py \
  tests/unit/test_job_spec.py \
  tests/unit/test_api_batch.py
```

See [TEST.md](TEST.md) for the test matrix and safe AWS smoke-test checklist.
