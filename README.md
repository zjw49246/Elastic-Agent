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
- **Credential management** — Claude/Codex account pools with worker-local auto-login, interactive OTP, quota monitoring, and rotation
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

The worker keeps a stdin pipe open so interactive callers can send
`SEND_INPUT`. For an unattended CLI that waits for stdin EOF even when a prompt
argument is present, redirect stdin explicitly in `run.command`; for example,
use `codex exec ... </dev/null`.

Mode-B jobs are described declaratively as a **JobSpec** — no Python subclass
needed — and fanned out across the fleet:

```python
from elastic_agent.core.job_spec import JobSpec

spec = JobSpec.model_validate({
    "name": "ai4sci-opus48-seed128",
    "environment": {"profile": "ubuntu-agent-docker-v1"},
    "setup": {
        "repo": "https://github.com/ApexIntelligence-AI/Agent-AI4Sci-Bench.git",
        "ref": "main",
        "steps": [{"name": "install", "command": "uv sync",
                   "timeout": 1200, "retries": 1}],
    },
    "run": {"command": 'uv run ai4sci-bench run --output-dir "results/opus48_$(hostname -s)_seed128"',
            "env": {"AI4SCI_SANDBOX_CPU": "1", "AI4SCI_SANDBOX_MEM": "4g"},
            "cwd": ".", "timeout": 86400, "shell": True},
    "ttl_seconds": 172800,
    "account": {"mode": "worker_local_login", "per_worker": 1},
    "rotation": {"strategy": "on_exhaust_restart_resume",
                 "resume_args": '--resume "results/opus48_$(hostname -s)_seed128"'},
    "fanout": {"workers": 8, "shard_by": "hostname"},
})
job = await manager.batch.launch(spec)   # scale → bootstrap → login → run, per worker
```

Template `{{shard_index}}` / `{{num_shards}}` / `{{hostname}}` are rendered by the
Manager; shell constructs like `$(hostname -s)` are evaluated on the worker.

`environment.profile` selects a versioned common platform definition maintained
by the framework. Jobs add only their repository, setup steps, datasets, run
environment, and command. `ubuntu-agent-v1` is the compatibility default;
`ubuntu-agent-docker-v1` adds the common Docker capability. Profile ids are
immutable—publish/select a new `*-vN` id instead of changing an old Job's base
environment.

Legacy `setup.commands: ["..."]` remains accepted and runs as one shell. New
`setup.steps` entries have independent `name`, `command`, `env`, `cwd`,
`timeout`, and `retries`; every Job-owned setup operation runs as the same
non-root Job/runtime user that later executes `run.command`. Use `setup.ref` for
a branch/tag and provide the full `setup.resolved_commit` when replay must fail
unless the checkout is byte-for-byte the expected Git revision.

JobSpec sections reject unknown fields instead of silently ignoring typos.
Missing, `null`, or legacy-zero `run.timeout` is normalized to 24 hours;
`ttl_seconds` defaults to 48 hours, must cover the run timeout, and both are
capped at 30 days. Preview the resolved source, setup policy, command, capacity,
account availability, and S3 collection mode without creating any state:

```bash
curl -fsS -X POST -H "Authorization: Bearer $EA_TEST_KEY" \
  -H 'Content-Type: application/json' \
  --data @job.json "$EA_TEST_URL/api/jobs/plan"
```

Real submit and resubmit repeat this pure preflight before persisting a spec,
claiming an account, or creating an instance. A Job cannot select a different
Region from the Manager's configured provider; cross-Region AMI/subnet/security
group selection is not currently supported.

Keep plaintext values in `run.env`. For managed secrets, put only AWS references
in `run.secret_env`, for example `{"OPENAI_API_KEY":
"aws-secretsmanager://prod/agent#OPENAI_API_KEY"}` or an `aws-ssm://...`
reference. The reference is persisted and shown in plans, while the value is
resolved immediately before dispatch and is never returned by the Job API.
Cross-host secret delivery requires `ELASTIC_AGENT_MANAGER_URL=wss://...`;
plaintext WebSocket delivery is rejected before Secrets Manager/SSM is read.

`setup.repo` must be a remote HTTP(S), SSH/Git, or scp-style Git URL and may not
contain embedded HTTP credentials, query parameters, or fragments. Use
`worker_clone` only for a repository the Worker can clone without a Manager
credential. Private repositories should use `manager_rsync`; the Manager uses
`ELASTIC_AGENT_GIT_TOKEN` only for its local clone and does not copy `.git` or
the token to the Worker.

### Account data and worker-local auto-login

Declarative Mode-B Jobs support worker-local login for both Claude and Codex.
Select the implementation with `account.agent_type` (`"claude"` by default):

```python
"account": {
    "agent_type": "codex",
    "mode": "worker_local_login",
    "group": "standard",
    "config_dir": "",  # Codex uses the runtime user's ~/.codex
}
```

A Codex account must contain at least one of its OpenAI login password or a
supported mailbox-query `email_token`; both may be configured:

```json
{
  "id": "codex-001",
  "agent_type": "codex",
  "email": "user@example.com",
  "password": "<optional OpenAI account password>",
  "email_token": "<optional mailbox query token; required without password>",
  "group": "standard"
}
```

The password is the OpenAI account password; it is not an IMAP/app password.
The optional mailbox token is not an OpenAI API/OAuth token. Passwords and
mailbox tokens are stored in the Manager's mode-`0600` accounts file and are
write-only over REST: account responses expose only `has_password` and
`has_email_token`. Cross-host `ACCOUNT_LOGIN` traffic must use `wss://` unless
`ELASTIC_AGENT_ALLOW_INSECURE_ACCOUNT_LOGIN=1` is deliberately enabled on a
trusted test network.

On update, blank secret fields preserve their current write-only values. Send
`clear_email_token: true` or `clear_password: true` to deliberately remove the
corresponding stored input. The API rejects an update that would leave a Codex
account with neither login input.

Claude continues to use the Chrome-CDP flow, exact-email `claude auth status`
verification, and a successful `claude -p` warm-up. For Codex, the worker starts
`codex login` and drives OpenAI OAuth with Playwright under Xvfb. Password-only
accounts use the password page and request manual OTP if needed; token-only
accounts switch to OpenAI's email-code path and query the OTP automatically;
with both configured, the password is used and the token handles any OTP. If
OpenAI does not offer an email-code action, token-only login fails clearly and
requires the password. The CLI and browser stay on the same worker because the
OAuth callback is local. The resulting `CODEX_HOME/auth.json` is accepted only when
it contains ChatGPT OAuth tokens, its id-token email exactly matches the
selected account case-insensitively, and a real `codex exec` smoke test
succeeds. Failure or cancellation restores the previous auth file. OAuth
credentials are never returned to the Manager.

With one Codex account per worker, an empty `config_dir` resolves to that
runtime user's `~/.codex` (including non-root workers). Codex Jobs that use
multiple pre-logged accounts or restart/resume rotation must provide an explicit
absolute `config_dir` writable by the runtime user; Elastic does not guess
`/root`. Manager timeout or orchestration cancellation sends a correlated
worker cancel and waits for the worker's cleanup acknowledgement, so the
still-running browser/CLI cannot commit credentials later. A disconnect ends
the Manager wait immediately. If an ordinary worker cannot confirm cleanup
within 60 seconds, that account is quarantined from further allocation; an EIP
Job instead remains protected by terminating its temporary instance before the
account claim is released.

If no mailbox token is configured, or automatic mailbox polling fails, the
Batch Console displays the live OTP challenge. The corresponding API is:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/accounts/login-attempts` | List active, correlated OTP challenges |
| `POST` | `/api/accounts/login-attempts/{login_request_id}/otp` | Forward `{"challenge_id":"...","code":"123456"}` to the owning worker |

Submitted verification codes are not persisted. Codex mailbox polling currently
supports 171mail and the MailCatcher-backed 163.com, mail.com, onet.pl, and
gazeta.pl flows; generic IMAP is not implemented. Before mailbox polling, the
worker suppresses `httpx`/`httpcore` request logging so a query token cannot be
written as part of a full request URL in the worker journal.

Codex support here is for declarative Mode-B `worker_local_login` Jobs.
`manager_distribute` is rejected for Codex because `auth.json` must be minted
and verified on the worker. Codex Jobs also deploy the current Manager's worker
source so an older runtime cannot interpret the login as a legacy Claude flow.
Mode-A PTY-hosted execution remains Claude-only.

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
  and the selected agent's credential variable (`CLAUDE_CONFIG_DIR` or
  `CODEX_HOME`); the verified directory is injected by the orchestrator.
  Generated Claude/Codex OAuth credentials stay worker-local; the write-only
  login inputs follow the protected Manager-to-worker boundary.
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
accounts per unbound worker, 100 rotations, 2048 GiB disk, a 30-day maximum for run timeout/Job TTL,
and an 86,400-second collection interval; provider `max_instances` defaults to
30. At terminal state, periodic collection stops and final collection is awaited
for up to three attempts/300 seconds before teardown. Collection failure marks
the Job failed but does not retain the billable EC2 indefinitely.

Collected output is isolated per stable fan-out slot at
`<prefix>/<job_id>/workers/shard-00000/...` (with a collision-resistant Worker
ID fallback during restart recovery), so same-named files from different
Workers cannot overwrite each other. Every slot includes
`_elastic_agent/collection.json` with its Job, Worker, shard, paths, collection
time, and transfer mode. `collect.interval_seconds > 0` uploads snapshots while
the command runs; success and failure both perform an awaited final collection.

S3 upload is automatic only when `ELASTIC_AGENT_RESULTS_S3_BUCKET` is set. On
AWS Workers with `worker_instance_profile`, each Worker pushes directly with its
instance role. Otherwise results first rsync to the Manager and its configured
AWS credentials upload them. `ELASTIC_AGENT_RESULTS_S3_PREFIX` defaults to
`jobs`. With no bucket, results remain under the Manager's `collected/` tree;
with a bucket configured, upload/list/download failures are explicit and a
failed final upload marks the Job failed instead of silently reporting durable
results.

Only files below the explicitly declared `collect.paths` are collected. An
empty list is an intentional no-op; the Batch Console defaults this field to
`results`, while API/SDK callers must set it themselves. Worker stdout/stderr
remain execution logs and are **not** automatically result objects—redirect or
write them into a collected directory if they must be retained in S3. Final
collection also runs for failed and cancelled Jobs, so already-written partial
results are preserved before the ephemeral EC2 is terminated.

Use an `Idempotency-Key` header when retrying `POST /api/jobs`: the same key and
spec resolve to the same deterministic Job, while reusing it for different
content returns `409`. `POST /api/jobs/{job_id}/cancel` sends TERM/KILL as
needed, waits for the reliable process-exit event, performs final collection,
and then force-terminates ordinary Job Workers (EIP Jobs detach/terminate via
their lease). On restart, durable `prepared/launching/running/terminal` state is
used to resume preparation or collect and clean up interrupted Workers.

For cost control, `ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES` is a comma-separated
Job allowlist (default: only the provider's configured instance type), and
`ELASTIC_AGENT_MAX_JOB_WORKER_HOURS` caps `fanout.workers * ttl_seconds / 3600`
(default 1440). These checks happen before Job persistence or cloud creation.

**Upload-code escape hatch**: because a Python Harness executes arbitrary code
inside the Manager, upload and `harness_ref` use are disabled by default. A
trusted deployment may explicitly set `ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD=1`,
then upload a `.py` through `POST /api/jobs/harness` and use the returned
`harness_ref`. Prefer declarative JobSpec for untrusted submitters.

**Frontend**: the Batch Console at `/batch` manages Claude and Codex identities,
accepts write-only OpenAI passwords/mailbox query tokens (at least one for Codex), filters
Job account choices by `agent_type`, and displays active Codex OTP challenges
with a six-digit submission form. API keys are accepted only in the
`Authorization: Bearer` or `X-API-Key` header; the UI keeps a key in
`sessionStorage` and strips legacy query-string credentials. REST includes `/api/accounts`,
`/api/accounts/login-attempts`, `/api/jobs`, and `/api/jobs/harness`.

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
