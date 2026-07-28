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
- **File sync** — Automatic Worker-to-OSS/S3 synchronization; unreadable
  candidate roots are skipped, and only standard delivery manuscript names
  receive the high-priority `delivery_manuscript` role
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

The lock currently pins claude-pty commit `7d5a0e5` (cross-host inject
isolation plus cancellation-safe Session publication and cleanup).

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
- STOP, shutdown, and transient-retry launch cancellation converge through one
  terminal finalizer. A PTY teardown exception is logged but cannot suppress
  the reliable `PROCESS_EXIT` handoff.

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

On POSIX, each Mode-B command runs in its own process session. STOP, timeout,
exhaustion, and a parent that exits while leaving children behind terminate the
whole process group before the Worker publishes its terminal event. A
CloudRouter 500/502 is classified as transient and the CLI may recover
internally, but Elastic does not silently replay an arbitrary outer Mode-B
command after the CLI gives up: that could duplicate benchmark side effects.
The Job therefore fails even when `rotation.resume_args` is configured; that
policy responds only to proven account auth/hard-quota exhaustion, not terminal
500/502. Put any idempotent transient retry inside the harness itself. Likewise,
a hard CloudRouter limit observed by a custom Mode-A PTY task durably benches
that account and terminates the current task; it does not automatically
cross-account-resume the PTY session.

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

The hardened AWS Manager keeps its home read-only. HTTPS repositories need no
extra host state; SSH/scp-style repository URLs require the deployment to
pre-seed the server key in the Manager user's `known_hosts` before starting.

### Account data and worker-local auto-login

Declarative Mode-B Jobs support worker-local login for both Claude and Codex.
Select the implementation with `account.agent_type` (`"claude"` by default):

```python
"account": {
    "agent_type": "codex",
    "model": "gpt-5.4",  # optional Agent API model admission
    "mode": "worker_local_login",
    "group": "standard",
    "config_dir": "",  # Codex uses the runtime user's ~/.codex
}
```

The same account pool can also contain CloudRouter and ApexRouter Agent API
identities. Add one from the Batch Console's **Agent API accounts** form or
through the authenticated management API:

```bash
curl -fsS -X POST "$EA_URL/api/agent-api/accounts" \
  -H "Authorization: Bearer $EA_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"provider":"cloudrouter","name":"research-router","group":"standard","api_key":"<write-only-key>"}'
```

Use `"provider":"apex"` for ApexRouter. CloudRouter validates against its fixed
`/v1/models` endpoint and may project the key into Claude, Codex, or both.
ApexRouter is Codex-only: it queries
`https://35-75-22-186.sslip.io/v1/models` with the pinned Codex CLI version and
configures the `apexrouter` Responses API provider. Fresh allocation prefers a
compatible, available API identity and falls back to OAuth; `account.ids` can
select a generated ID such as `cloudrouter-1` or `apex-1` explicitly. Set
optional `account.model` to require an exact advertised model (Claude stable
aliases also match their dated variants); without it, admission checks only
the selected Agent family for backward compatibility. This field validates
routing but does not rewrite the opaque run command's own model arguments.
Jobs do not need a separate API mode and API identities support the same
persistent EIP binding flow.

Provider waits have nested wall-clock bounds: each Agent API HTTP request is
limited to 15 seconds; automatic pool selection refreshes at most 16 keys
concurrently for 30 seconds total, excludes unfinished keys for that attempt,
and can fall back to OAuth. An explicit native/OAuth ID skips unrelated API
probes. For a selected API identity, usage admission, key read/delivery, and the
Worker acknowledgement share one 60-second deadline and fail closed.

For non-EIP rotation, pre-logged credential slots are used first. If a dynamic
Agent-API-to-OAuth fallback is then needed, the OAuth login goes to a sibling
`<source-slot>-rot-N` directory outside `.elastic-agent-api`; Elastic never
writes OAuth state into a delegated-key projection and fails closed if the
source slot cannot be derived safely.

Agent API keys live in a mode-`0700` Manager account directory with
mode-`0600` files. They are never returned by REST and never enter JobSpec, CLI
configuration, process environment, or command arguments. After the same WSS
transport check used for login secrets, a correlated setup message writes the
key once to the selected Worker. Claude and Codex read it through a private
helper; routing is fixed to the selected provider, inherited official and
gateway auth/base overrides are removed, and a structured provider failure is
reported as a failed Job even when the CLI process exits `0`. Managed Claude
is available only through CloudRouter and loads only its Worker-owned user
settings; project/local settings, hooks, and MCP configuration are excluded so
Job files cannot redirect the provider or credential helper.

During Manager startup recovery, Agent API allocation stays closed until every
previous Worker has a confirmed terminal cloud readback. OAuth allocation can
continue. This prevents a surviving orphan Worker and a new Job from using the
same API key concurrently.

Nested containers require an explicit container-owner contract. For a validated
projection the Worker exports the non-secret
`ELASTIC_AGENT_API_PROJECTION_ROOT` path; ordinary/OAuth Jobs have any
user-supplied value removed. A compatible runner validates the projection
version-2 marker and ownership, mounts exactly that account root read-only at
the same absolute path, and forwards `CLAUDE_CONFIG_DIR` or `CODEX_HOME`.
Version 2 adds a byte-exact mode-`0700` launcher that clears the inherited
environment before invoking the credential helper, so a task-writable
`PATH`/`PYTHONPATH` cannot replace the Python interpreter that reads the key.
The supported AI4Sci OS sandbox consumes this contract and starts managed Codex
through the root-owned Node binary and exact npm entrypoint, bypassing its
`#!/usr/bin/env node` wrapper.

Managed Agent API traffic uses direct Worker egress. EIP Job preflight rejects
`HTTP_PROXY`, `HTTPS_PROXY`, and `ALL_PROXY` (including lowercase variants) in
`run.env` or `run.secret_env`; the supported container consumer independently
rejects ambient or adapter-supplied routing proxies. This prevents the selected
provider from observing a proxy's public IP instead of the account's bound EIP. Ordinary
non-managed CLI Jobs retain their existing proxy support. Elastic-Agent
deliberately does not intercept arbitrary `docker`, Compose, or SDK calls.

`GET /api/agent-api/accounts`, `POST
/api/agent-api/accounts/{id}/refresh`, and `GET
/api/agent-api/accounts/{id}/usage` expose only non-secret models and normalized
usage. Usage is cached for 60 seconds; invalid, expired, or exhausted keys are
not allocated. The last known unavailable result is fingerprint-bound and
durable across Manager restart, so a transient refresh cannot revive it. A
runtime hard-quota event writes the same recoverable durable state; a later
successful provider probe clears it, while invalid-key/model tombstones require
an explicit refresh. Invalid/unknown 200-response schemas fail closed, numeric
and nested display fields are bounded and allowlisted, and deterministic
model-refresh failures bench the stale catalog. Deletion is intentionally
disabled until every delegated Worker can be durably fenced; terminate Jobs and
retire the upstream key when necessary.

ApexRouter `/usage` reports per-key `used` values but shared-group
`remaining`, `limits`, and `concurrency`; Elastic keeps those scopes separate
and excludes the key when any shared limit is exhausted. ApexRouter does not
currently supply an expiry time. At runtime, Apex authentication failures and
explicit quota exhaustion rotate credentials, while ordinary HTTP `429` and
`500`/`502` failures are treated as transient provider errors rather than proof
that the individual key is exhausted.

An Agent API key is delegated to the Job's Unix user. Arbitrary Job code running
as that user can invoke the helper or read the private key file, so use Agent
API accounts only with trusted Job code. Ordinary ephemeral Workers are
destroyed before the account claim returns to the pool; EIP Jobs retain the
existing durable detach/terminate/release ordering.

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

`account.login_timeout_seconds` controls only the Codex browser state machine
(default `900`, accepted range `60`–`1200`). The Manager keeps a separate
3600-second end-to-end budget for mailbox/manual OTP waits, exact-account
validation, the real `codex exec` smoke test, and correlated cleanup. Current
OpenAI email-code labels including “one-time code” and “login code” are handled;
a timeout reports only a bounded page-state category, never the OAuth URL. The
browser keeps the installed system Chrome's native user agent instead of
spoofing an obsolete Chrome major version that can increase risk-page mismatch.
A visible managed anti-bot challenge gets up to 120 seconds to clear, then fails
with explicit bound-EIP guidance instead of consuming the full browser budget.

The mailbox token is a query credential for the configured mailbox service,
not an OpenAI API/OAuth token or password. Token-only login still causes OpenAI
to send an email OTP; the worker normally retrieves and fills it automatically.
If no usable mailbox token is configured, polling fails, or OpenAI rejects the
automatically retrieved code, only the affected Worker publishes a live manual
OTP challenge. The Batch Console places a separate card inside that Worker's
Job and labels the account email/ID, full Worker ID, Job, and shard. Concurrent
Workers keep independent cards and submissions. A floating reminder appears
only while at least one challenge is active and collapses after navigating to a
card so it does not cover the mobile input.

The corresponding API is:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/accounts/login-attempts` | List active challenges with exact Worker/account/Job/shard metadata |
| `POST` | `/api/accounts/login-attempts/{login_request_id}/otp` | Forward `{"challenge_id":"...","code":"123456"}` to the owning worker |

Submitted verification codes are not persisted, and concurrent submissions for
one challenge are forwarded only once. Codex mailbox polling currently
supports 171mail and the MailCatcher-backed 163.com, mail.com, onet.pl, and
gazeta.pl flows. Other domains, including 139.com, use the default 171mail
route and therefore require a compatible 171mail query token; generic IMAP is
not implemented. Before mailbox polling, the
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

On AWS Managers the Batch Console selects EIP mode by default and shows each
account's durable address. Selecting `binding="none"` remains available for
ordinary fleet jobs, but the Job plan warns that it bypasses any account EIP
and logs in from the instance's temporary public address.

```text
final collect → detach EIP → terminate EC2 and its root EBS → release lease
              → remove disposable Node record              ↳ retain EIP
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
        "login_timeout_seconds": 900,
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

EC2 may continue listing a terminated instance for several minutes. The
Manager ignores that row only when the durable released lease proves an exact
lease/instance/account/Job match and every teardown phase is committed.
Unknown, incomplete, or mismatched state remains on the fail-closed recovery
path. If another active lease already claims the same instance, reconciliation
quarantines the conflict and performs no detach or termination.
Node/task/runtime-status state and the account claim are removed only after an
identity-matched durable lease returns `RELEASED`; otherwise they are retained
so cleanup remains observable and retryable. A durable worker without an exact
instance ID is treated as corrupt state, and claim cleanup additionally proves
the exact claim owner and account before making that identity reusable.

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
effect. Ordinary instance publication is fenced against live recovery, and a
cloud create that times out or is cancelled after acceptance triggers a bounded
controller/Job-tag scan so an instance that appears later is collected and
terminated without waiting for a Manager restart. An EIP Job also reserves the whole fanout against provider
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
The first running snapshot is collected after one full interval. A value of
`0` (the schema and Batch Console default) means final collection only, so set a
positive interval such as `120` for a long Job whose intermediate files must be
visible in S3 and downloadable before the command exits. A running download is
the latest completed snapshot; it does not trigger an immediate sync from the
Worker.

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

For diagnosis, the Manager separately archives the bounded tail of each
command's stdout/stderr before Worker teardown (up to 5,000 entries, 8 MiB per
task, 64 KiB per entry, 512 task attempts/64 MiB per Job, and 1 GiB across the
Manager). Oldest snapshots are pruned at the configured logging retention
boundary (30 days by default) and when a byte/task quota is reached. If a
reliable exit event is replayed after a Manager restart, it first attempts a
bounded recovery from the Worker's private `ea-logs` file while the instance is
still available. Query live or archived output with
`GET /api/jobs/{job_id}/logs?worker_id=&task_id=&lines=400`; responses are
private, uncached, and remain available after the ephemeral Worker is
destroyed. These diagnostic snapshots are stored in the Manager state
directory, not uploaded to S3, because stdout/stderr may contain sensitive
material. Jobs completed before this archive existed cannot be recovered after
their Workers have already been destroyed. In the Batch Console, a failed Job
uses a prominent **查看失败日志** action; terminal runs load the complete bounded
5,000-line archive and show the task exit code/error summary alongside stderr.

Use an `Idempotency-Key` header when retrying `POST /api/jobs`: the same key and
spec resolve to the same deterministic Job, while reusing it for different
content returns `409`. `POST /api/jobs/{job_id}/cancel` sends TERM/KILL as
needed, waits for the reliable process-exit event, performs final collection,
and then force-terminates ordinary Job Workers (EIP Jobs detach/terminate via
their lease). Disposable ordinary Workers are also removed from the live Node
registry after cloud termination, preventing unbounded dashboard/state growth.
On restart, durable `prepared/launching/running/terminal` state is
used to resume preparation or collect and clean up interrupted Workers.
Ordinary Job cloud creates also have a separate private
`unbound-launches.json` intent journal, written before the provider call.
That journal remains authoritative even after the Job is marked failed, so an
accepted create hidden by a timeout or cancellation is still scanned,
collected, and terminated after a later Manager restart. An intent is cleared
only after confirmed instance termination, a complete successful no-match
visibility window, or transfer to a durably published exact NodeRecord. A
fresh Manager also quarantines and checks those exact instance IDs across the
full visibility window, covering a crash immediately after publication.

For cost control, `ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES` is a comma-separated
Job allowlist (default: only the provider's configured instance type), and
`ELASTIC_AGENT_MAX_JOB_WORKER_HOURS` caps `fanout.workers * ttl_seconds / 3600`
(default 1440). These checks happen before Job persistence or cloud creation.
The checked-in Tokyo production profile permits the common x86_64 T3,
M5/M6i/M7i, C5/C6i/C7i, and R5/R6i/R7i families from `large` through
`4xlarge` (T3 through `2xlarge`); Graviton, GPU, metal, and larger high-cost
shapes remain excluded.

**Upload-code escape hatch**: because a Python Harness executes arbitrary code
inside the Manager, upload and `harness_ref` use are disabled by default. A
trusted deployment may explicitly set `ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD=1`,
then upload a `.py` through `POST /api/jobs/harness` and use the returned
`harness_ref`. Prefer declarative JobSpec for untrusted submitters.

**Frontend**: the Batch Console at `/batch` uses a light theme by default, with
an optional session-scoped dark theme. The Job submission form keeps the
JobSpec wire format unchanged while grouping inputs into eight numbered
sections: basics, compute, source/setup, account, run, results, rotation, and
trusted Harness settings. Labels name the user-facing purpose first and show
the raw JobSpec field second; low-frequency settings use disclosure panels.
Conditional account, EIP, repo, and rotation controls are visibly disabled
with an adjacent reason when they do not apply. Result paths and the in-run
collection interval remain prominent, and the validation/launch action stays
reachable on desktop while stacking into full-width buttons on narrow screens.
The client also checks native numeric limits, required run command, Job TTL
ordering, and S3 dataset line format before preflight.

The console manages Claude and Codex identities,
accepts write-only OpenAI passwords/mailbox query tokens (at least one for
Codex; both may be configured), filters Job account choices by `agent_type`,
and shows active Codex OTP challenges as Worker-specific cards inside the
corresponding Job. Each card is keyed by login request and challenge, while a
single floating reminder links to all affected Workers and remains hidden when
no manual OTP is needed. Stable keyed rendering and
non-overlapping, visibility-aware polling preserve focus, expanded sections,
scroll position, and log viewing instead of rebuilding the whole page every
five seconds. OTP inputs, focus, and cursor selection also survive a Job-card
replacement without persisting the code in browser storage. Job cards start
collapsed with their identity, state, phase,
submission time, and Worker count visible; opening a card reveals its actions,
errors, cleanup state, results, and Worker execution table, and polling keeps
the user's open/closed choice. Completed execution rows remain available as
history. Command output remains queryable after teardown; the read-only live
system-journal action remains available until the Worker resource is released,
then stops polling on a not-found/conflict response; destructive terminate
actions disappear at execution terminal state. Each Job keeps a stable result
action while metadata loads. Per-Job request versions reject stale responses,
known non-empty results never regress to empty on a transient or out-of-order
refresh, terminal empty results retry with bounded backoff, and duplicate
archive downloads are suppressed. S3 result archives use the UI's cancellable
streaming endpoint, which starts returning the tarball while objects are read
instead of waiting for a complete Manager-side temporary archive. The action
shows received bytes and elapsed time; secure desktop Chromium writes chunks
directly to the selected file, while browsers without the File System Access
API use a memory-backed fallback only below 256 MiB of source data and reject
larger snapshots with a desktop-Chrome instruction instead of risking a tab
crash. Running Jobs label the action as a download of the latest uploaded
intermediate snapshot.
The original strict download endpoint remains available to API clients that
prefer a prebuilt archive and an HTTP error before response headers. API keys
are accepted only in
the `Authorization: Bearer` or `X-API-Key` header; the UI keeps a key in
`sessionStorage` and strips legacy query-string credentials. REST includes
`/api/accounts`, `/api/agent-api/accounts`,
`/api/accounts/login-attempts`, `/api/jobs`, `/api/jobs/{job_id}/logs`, and
`/api/jobs/harness`.

Live batch runs require provision/login hooks wired at deployment:
`manager.configure_batch(provision_hook=..., login_hook=...)`.

## AWS production launcher

Run the Manager from the version-controlled `deploy/aws_manager.py` entry point
and `deploy/aws/elastic-agent-manager.service` unit instead of machine-local
Python/unit files. The unit keeps the release and home directory read-only and
allows writes only below the configured production state directory. It does not
discover credentials from local CLIs or contain deployment fallbacks: the
Manager instance profile supplies AWS credentials, while a mode-`0600` systemd
`EnvironmentFile` must set these non-secret deployment values:

```text
ELASTIC_AGENT_AWS_REGION
ELASTIC_AGENT_AWS_AMI_ID
ELASTIC_AGENT_AWS_INSTANCE_TYPE
ELASTIC_AGENT_AWS_WORKER_SECURITY_GROUP_IDS
ELASTIC_AGENT_AWS_SUBNET_ID
ELASTIC_AGENT_AWS_KEY_PAIR_NAME
ELASTIC_AGENT_AWS_SSH_KEY_PATH
ELASTIC_AGENT_AWS_WORKER_INSTANCE_PROFILE
ELASTIC_AGENT_AWS_EXPECTED_ROLE_NAME
ELASTIC_AGENT_AWS_MAX_INSTANCES
ELASTIC_AGENT_STATE_DIR
ELASTIC_AGENT_MANAGER_URL
ELASTIC_AGENT_FRAMEWORK_SRC
ELASTIC_AGENT_SERVER_HOST
ELASTIC_AGENT_SERVER_PORT
ELASTIC_AGENT_WORKER_SSH_USER
ELASTIC_AGENT_LOG_LEVEL
ELASTIC_AGENT_RESULTS_S3_BUCKET
ELASTIC_AGENT_RESULTS_S3_PREFIX
ELASTIC_AGENT_RESULTS_S3_INTERVAL
```

Keep secrets such as `ELASTIC_AGENT_EXTERNAL_API_KEYS` in
`/etc/elastic-agent-manager.env`; keep the non-secret AWS deployment settings in
the separately managed `/etc/elastic-agent-manager.aws.env` (the checked-in
production source is `deploy/aws/elastic-agent-manager.aws.env`). Both files are
mandatory and mode `0600`, so a partial deployment fails closed. The launcher
refuses to start without it and never places its value in the parsed settings or
startup logs. Optional Git access comes only from `ELASTIC_AGENT_GIT_TOKEN`—the
launcher never falls back to a local `gh` login. Start one process with
the supplied systemd unit (or use `uv run python deploy/aws_manager.py` for an
interactive preflight).

The supplied unit disables IMDSv1, points the AWS shared/config/Boto files at
`/dev/null`, removes environment/web-identity/container credential inputs, and
the launcher requires STS to report `ELASTIC_AGENT_AWS_EXPECTED_ROLE_NAME`.
This makes a healthy process proof that it is using the dedicated EC2 instance
role rather than same-account static/admin credentials. The configured state
directory must be created as the service user with mode `0700` before starting;
the unit asserts that path exists and exposes readiness only after its local
health check succeeds.

Startup verifies that the worker AMI is available, x86_64/HVM, ENA- and
IMDSv2-capable, has an encrypted root snapshot, is owned by the Manager account,
and has `ManagedBy=elastic-agent` plus `Role=worker-golden` tags. Emergency
rollback to an official Canonical image (`099720109477`) is rejected unless
`ELASTIC_AGENT_ALLOW_CANONICAL_BASE_AMI=true` is explicitly set. That
break-glass path may use Canonical's unencrypted publisher snapshot; workers
still request encrypted root volumes. The dedicated Manager IAM policy also
pins allowed image ARNs, so the flag alone is insufficient: an administrator
must update the exact IAM image pin and environment together, run Access
Analyzer/full-policy simulation, and complete a launch/upload/terminate canary.
Restore a tagged golden image and its narrow IAM pin immediately.

On AWS, Manager-initiated SSH traffic (bootstrap, login, logs, code delivery,
and collection) prefers the Worker's VPC-private address. The Worker's EIP is
only its stable outbound identity, so port 22 can be restricted to the Manager
security group. A least-privilege Manager/Worker policy and a staged
cutover/rollback procedure are maintained in
[`deploy/aws/iam-cutover.md`](deploy/aws/iam-cutover.md). The supplied Worker
policy intentionally writes only to the configured results prefix, and the
results bucket policy denies plaintext transport. S3 datasets and additional
EC2 instance types require explicit policy allow-list updates.

## Development

```bash
uv sync --extra dev --extra pty
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
  tests/unit/test_reconciler.py \
  tests/unit/test_manager.py \
  tests/unit/test_job_spec.py \
  tests/unit/test_api_batch.py
```

See [TEST.md](TEST.md) for the test matrix and safe AWS smoke-test checklist.
