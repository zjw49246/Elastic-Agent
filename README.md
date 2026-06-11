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

## Development

```bash
uv sync --extra dev
pytest
```
