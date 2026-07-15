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

**Upload-code escape hatch**: for jobs needing custom logic, set
`harness_ref: "module:Class"` (or upload a `.py` via `POST /api/jobs/harness`) to
drive the job with a real `Harness` subclass instead.

**Frontend**: the Manager serves a **Batch Console** at `/batch` — an Accounts
panel (email + 接码 token pool), a Job form (both the declarative and upload-code
paths), and a live per-worker Job monitor. REST: `/api/accounts`, `/api/jobs`,
`/api/jobs/harness`. Credentials are always minted on the worker and never
transit the frontend or Manager.

Live batch runs require provision/login hooks wired at deployment:
`manager.configure_batch(provision_hook=..., login_hook=...)`.

## Development

```bash
uv sync --extra dev
pytest
```
