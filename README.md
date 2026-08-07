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
credentials. The update is transactional: a write or PTY-recycle failure
restores the prior private credential snapshot and reports failure without
adding the new quota slot. If both the update and rollback fail, that config
directory is tombstoned in the Worker process and both PTY and subprocess
execution refuse it until a complete successful reconfiguration.

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
production Worker places that process group under the independent
`ea-task-supervisor.service`, not the reconnectable `ea-runtime.service`.
Restarting the runtime or losing its Manager WebSocket therefore inventories
and reattaches the original task instead of launching the command again;
private, bounded output and a stable terminal event are replayed after
reconnect. A socket grace period alone never destroys an active supervised
batch task: reconnect, its reliable terminal event, explicit cancellation,
cloud-terminal reconciliation, or the Job TTL supplies the liveness decision.
A lost instance or supervisor is a different boundary and requires an S3
checkpoint on a replacement Worker.

A CloudRouter 500/502 is classified as transient and the CLI may recover
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
        "ref": "archive/youchengsong-managed-agent-api-20260728",
        "steps": [{"name": "install", "command": "uv sync",
                   "timeout": 1200, "retries": 1}],
    },
    "run": {"command": 'uv run ai4sci-bench run --output-dir "results/opus48_shard-{{shard_id}}_seed128"',
            "resume_command": 'uv run ai4sci-bench run --output-dir "results/opus48_shard-{{shard_id}}_seed128" --resume "results/opus48_shard-{{shard_id}}_seed128"',
            "env": {"AI4SCI_SANDBOX_CPU": "1", "AI4SCI_SANDBOX_MEM": "4g"},
            "cwd": ".", "timeout": 86400, "shell": True},
    "ttl_seconds": 172800,
    "account": {"mode": "worker_local_login", "per_worker": 1},
    "rotation": {"strategy": "on_exhaust_restart_resume",
                 "resume_args": '--resume "results/opus48_shard-{{shard_id}}_seed128"'},
    "fanout": {"workers": 8, "shard_by": "shard_index"},
})
job = await manager.batch.launch(spec)   # scale → bootstrap → login → run, per worker
```

The AI4Sci Bench example intentionally uses the archived
`archive/youchengsong-managed-agent-api-20260728` branch. Change `setup.ref`
when submitting a different repository.

Template `{{shard_index}}` / `{{shard_id}}` / `{{num_shards}}` /
`{{hostname}}` are rendered by the Manager; `shard_id` is the zero-padded
five-digit shard index. Shell constructs like `$(hostname -s)` are evaluated on
the worker, but checkpoint Jobs reject hostname-derived workload paths because
a replacement EC2 has a different hostname. Use `{{shard_id}}` for recoverable
output and resume paths. Whitespace inside a token, such as
`{{ shard_id }}`, is accepted.
The same templates work in `setup.s3_datasets[].uri` and `dest`, so
a fanout Job can stage one exact S3 object per worker instead of copying a whole
prefix to every worker:

```json
{"uri": "s3://private-data/run/shard-{{shard_id}}.jsonl",
 "dest": "/srv/replay/shard-{{shard_id}}.jsonl"}
```

Dataset provisioning requires the exact Worker context. Missing context,
unknown or empty template values, and an unavailable hostname fail closed
instead of falling back to shard zero or converting a single-object `cp` into a
whole-prefix `sync`. Destination parent paths are computed and quoted without
shell word splitting, including spaces, globs, and single quotes.

The AWS worker instance profile must grant `s3:GetObject` for the exact dataset
prefix. The production policy limits that read grant to `jobs/datasets/*`;
result objects remain unreadable and undeletable from workers.

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
a branch/tag and provide the full `setup.resolved_commit` for an immutable
checkout. Manager delivery fetches that commit directly, so a mutable branch
advancing after Job creation does not change or invalidate the selected source.

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
CloudRouter's explicit `mode="unrestricted"` means the key has no spend cap:
top-level `balance=0` and `remaining=0` stay visible but are not exhaustion
signals. Explicit exhausted status, expiry, quota, and rate-limit windows still
block allocation.
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

For `account.binding="none"`, one Agent API identity may be shared by multiple
Workers or Jobs through independent reference-counted claims. OAuth identities
remain exclusive. Any identity with a durable EIP binding also remains
exclusive even when it has no active lease; it must be used with
`binding="eip"` or explicitly decommissioned first. Automatic `per_worker`
slots never repeat one API identity on the same Worker, while explicit
`account.ids` may repeat only an unbound Agent API ID.

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
continue. Once recovery completes, intentional unbound sharing is tracked by
claim refcounts; recovery fencing prevents an untracked orphan from retaining a
delegated key outside that ownership graph.

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
model-refresh failures bench the stale catalog. `DELETE
/api/agent-api/accounts/{id}` is reference-aware: it succeeds only after
startup recovery is complete and the identity has no active claim, lease, or
durable binding. A bound identity must first use the explicit EIP decommission
flow; adding `"delete_identity":true` to that request retires the identity under
the same allocation fence. Storage failure never reports a full success: the
released-EIP/retained-identity partial state is quarantined and shown explicitly
in the UI.

ApexRouter `/usage` reports per-key `used` values but shared-group
`remaining`, `limits`, and `concurrency`; Elastic keeps those scopes separate
and excludes the key when any shared limit is exhausted. An explicitly present
`null` value in one window's `limits` entry means that shared window is
unlimited; its `remaining` value is not used for admission. Limited and
unlimited windows may coexist, while a missing limit or invalid finite values
fail closed. ApexRouter does not currently supply an expiry time. At runtime,
Apex authentication failures and explicit quota exhaustion rotate credentials,
while ordinary HTTP `429` and `500`/`502` failures are treated as transient
provider errors rather than proof that the individual key is exhausted.

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

Claude credential writes and both login implementations are transactional
through identity validation and warm-up. A failed login result carries
`cleanup_complete=true` only after every tracked CLI/browser/Xvfb process has
exited and the previous credential snapshot has been restored. `false`, or a
missing field from an older Worker, quarantines an ordinary account; successful
logins intentionally carry no cleanup claim, so a late cancel cannot falsely
say committed credentials were removed. The legacy `CREDENTIAL_LOGIN` rotation
path follows the same rollback rule around PTY recycle and fails closed on an
unrecoverable slot instead of confirming a mixed old-session/new-file identity.

`account.login_timeout_seconds` controls the Claude or Codex browser state
machine (default `900`, accepted range `60`–`1200`). The Manager keeps a separate
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

Managed account support here is for declarative Mode-B `worker_local_login`
Jobs. `manager_distribute` is rejected for both Claude and Codex because no
production Batch path implements Manager-side credential distribution. Codex
Jobs also deploy the current Manager's worker source so an older runtime cannot
interpret the login as a legacy Claude flow. Mode-A PTY-hosted execution remains
Claude-only. Restart recovery has a teardown-only compatibility reader for an
already-running legacy `manager_distribute` Job: it exposes only collection
paths and the setup root, performs final collection, and cannot replay the run.

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
| `POST` | `/api/accounts/{account_id}/binding/decommission` | Permanently release the EIP; requires `{"release_eip":true,"confirm_account_id":"..."}` and no active claim/lease. Optional `"delete_identity":true` retires the OAuth/API identity under the same fence |
| `GET` | `/api/accounts/allocations` | Inspect current account-to-Job/worker allocations |

The standalone OAuth/Agent-API DELETE endpoints never release infrastructure
implicitly. Decommission first, or use the double-confirmed
`delete_identity:true` form to release the EIP and retire its identity in one
fenced operation. This prevents ordinary Job cleanup or account edits from
losing a stable address while also closing a claim race between two admin
requests.

The Batch Console guides this sequence for OAuth and Agent API identities. If
the account has a binding, the delete action shows the exact EIP, warns that
release is permanent, requires the full account ID to be typed, then sends one
atomic decommission-and-retire request. A failed or completed Job does not
release the EIP automatically. An active claim, lease, unfinished cleanup, or
incomplete startup recovery still returns `409`, leaving both the binding and
identity intact. API clients should use the same atomic form when retiring the
identity; omit `delete_identity` only when intentionally keeping the identity
after releasing its EIP.

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
for up to three attempts/7200 seconds before teardown. The longer bounded
window permits a large rsync plus separate 30-minute immutable-checkpoint and
public-result S3 stages. Collection failure marks the Job failed but does not
retain the billable EC2 indefinitely.

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

For replacement-instance recovery, set `collect.checkpoint=true`,
`fanout.shard_by="shard_index"`, and a positive interval. Each shard commits an
immutable hash manifest, then Elastic publishes a Job-level checkpoint set only
after every shard is present. Content-addressed blobs deduplicate unchanged
files and `checkpoint_keep_generations` (default `3`) bounds retained complete
sets. A new Job can select the latest complete set with
`recovery.policy="checkpoint"` and `recovery.source_job_id`; recovery verifies
the source spec and resolves a real complete S3 set during preflight. It checks
the exact shard map, metadata, hashes, aggregate totals, Manager staging
limits, and target disk allowance, then pins that generation in the private
target JobSpec before durable prepare. All data is staged before creating an
instance. `POST /api/jobs/recover` (also exposed as **从检查点恢复** in the
Batch page) builds that new Job from the Manager's private source journal, so
redacted environment values and secret references never need to round-trip
through the browser. Only checkpoint generation, run command/run timeout, and
Job TTL may be overridden, and the normal Idempotency-Key submit path is
retained. S3 `COMMITTED` is authoritative even if the Manager crashed before
updating its local `latest` convenience pointer. Work written after the last
complete set must run again. If a source entry vanishes during rsync, Elastic
retries only the canonical rsync rc=24 vanished-source case and still requires
a subsequent complete rc=0 pass before publishing the snapshot.

On the replacement Worker, Elastic transfers every recovery path into a
root-private transaction tree outside the workload checkout
(`/var/lib/elastic-agent/recovery-transactions-v1`), fsyncs and re-measures it,
and durably enters an installing state before any selected directory is renamed
into place. The transaction tree and checkout must be on the same filesystem.
Login and dispatch are gated on one `installed` marker. A Manager crash between
directory renames is rolled forward on startup; an incomplete transfer is never
collected as a new final checkpoint. Every Manager-side rsync is fenced by a
pre-spawn durable transfer journal; staging and its reservation remain
quarantined until the entire transfer process group is proven gone. Startup
also stops/masks framework and Job-user services, removes all containers from
the dedicated Worker, stops Docker/containerd, verifies their cgroups, and
scans for surviving worktree writers before reconciling the transaction and
collecting. Disk admission includes at least one filesystem allocation block
per restored object, rather than trusting logical file bytes alone.

Ordinary results from Jobs created before checkpoint mode remain downloadable
but are not accepted as recovery input. Their mutable S3 prefixes do not carry
an authoritative deletion manifest and may retain stale objects. With no
complete immutable set, restart the workload from the beginning.

The resumed command must opt into the application's own resume mode and use the
same stable shard-relative output path. For AI4Sci `run`, for example, keep
`results/opus48_shard-{{shard_id}}_seed128` as both the restored output and
`--resume` directory; for `batch-run`, pass the same batch root to
`--output-dir` and `--resume`. Do not use a hostname-derived path because the
replacement EC2 has a different hostname.

For an operator-controlled cold stop, also set `run.resume_command` and use
**中断并保存进度** on the live Job. The API equivalent is
`POST /api/jobs/{job_id}/interrupt` with a stable `Idempotency-Key`. Elastic
first durably commits `suspending`, sends a non-escalating `SIGINT` to the task
process group, then uses bounded `SIGTERM`/`SIGKILL` fallbacks. It stops and
proves all task, runtime, container, and escaped writers quiescent before the
final checkpoint attempt and compute teardown. A Job becomes `suspended` only
after cleanup is complete and an exact complete checkpoint set is available;
otherwise it becomes `failed`. If final collection fails but a previous
complete set exists, the Job remains resumable from that older set and reports
the fallback warning. Host quiescence requires the configured Worker runtime
user to be non-root; root-user deployments reject the cold-interrupt
transaction rather than snapshotting an ambiguous filesystem.

**一键续跑** calls `POST /api/jobs/{job_id}/resume` with the exact verified
generation and another stable `Idempotency-Key`. It creates a new Job rather
than mutating or replaying the stopped one, uses the private persisted
`run.resume_command`, and records `resumed_from_job_id`, `root_job_id`, and
`attempt_no` (parallel branches from one source may share an attempt number).
Secret references and redacted environment values never round-trip through the
browser. The first signal can also reach active child processes; already
published application completion markers are retained, but any unit without a
complete marker must run again after restore.
See [Mode-B reconnect and checkpoint recovery](docs/operations/checkpoint-recovery.md).

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

Result metadata is bounded independently from collected data size. The listing
reports the authoritative `file_count` but returns at most 500 preview paths
with explicit truncation fields and aggregate path/JSON budgets. Local and S3
score discovery share a 500-candidate, 16 MiB read, 500-result ceiling and
accept only bounded printable scalar metadata plus finite numeric scores.
Local reads bind an open fd to the listed inode/stat and verify exact EOF; S3
reads bind ETag/size and likewise reject short, long, or changed objects.

The cancellable S3 archive endpoint acquires admission before creating its pipe
or queueing a producer, holds it until the producer exits, and closes an active
object body on disconnect. The strict prebuilt endpoint uses a response-level
`finally` for its temporary file plus global build concurrency and a 20 GiB
logical spool budget. Its reservation includes worst-case UTF-8 PAX headers,
tar block/trailer overhead and gzip bounds, and it refuses a build unless the
temporary filesystem retains 512 MiB of free headroom.

Authenticated external sync reads under
`/api/external/files/{task_id}/...` are also bounded. Task IDs and paths are
validated before storage access; Local storage opens every path component with
`O_NOFOLLOW`, while S3/OSS readers bind one GET body's authoritative length.
Manifests are limited to 8 MiB, 10,000 entries, and bounded printable fields.
Content mode streams at most 2 GiB in 256 KiB chunks, verifies exact EOF, and
closes the reader on normal completion, errors, or client disconnect. URL mode
intentionally returns a direct presigned-storage URL without the 2 GiB Manager
streaming limit. Incoming HTTP request bodies are capped before JSON parsing by
`ELASTIC_AGENT_MAX_REQUEST_BODY_BYTES` (16 MiB default; configurable from
64 KiB through 64 MiB), including chunked requests and dishonest
`Content-Length`. Conventionally bodyless GET/HEAD/OPTIONS/TRACE requests
bypass pre-reading. Body-bearing requests have one strict whole-read deadline
(`ELASTIC_AGENT_REQUEST_BODY_READ_TIMEOUT_SECONDS`, 30 seconds by default) and
fail-fast admission
(`ELASTIC_AGENT_MAX_CONCURRENT_REQUEST_BODIES`, 16 by default). Their
conservative three-copy reservation remains held through route JSON/Pydantic
processing under
`ELASTIC_AGENT_MAX_AGGREGATE_REQUEST_BODY_BYTES` (256 MiB by default, 1 MiB–4
GiB, and always at least three times the per-request limit). Saturation returns
an uncached 503 without reading the body; timeout returns an uncached 408.

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

Worker pipe drainage is independent of Manager link speed: arbitrarily long
physical lines are split into 64 KiB byte frames without breaking UTF-8, while
the byte-exact raw record remains in the Worker's local NDJSON. LOG and
file-data transports have separate frame-count and serialized-byte budgets
that include retry/in-flight frames; reliable terminal events retain their
durable priority path. Manager result accounting rejects non-finite, negative,
boolean or cumulative-overflow costs and accepts only bounded printable
session IDs.

Use an `Idempotency-Key` header when retrying `POST /api/jobs`: the same key and
spec resolve to the same deterministic Job, while reusing it for different
content returns `409`. `POST /api/jobs/{job_id}/cancel` sends TERM/KILL as
needed, waits for the reliable process-exit event, performs final collection,
and then force-terminates ordinary Job Workers (EIP Jobs detach/terminate via
their lease). Disposable ordinary Workers are also removed from the live Node
registry after cloud termination, preventing unbounded dashboard/state growth.
Each ordinary shard settles independently as soon as it is terminal:
final collect, exact cloud termination proof, authoritative registry-absence
readback, then only that Worker's claim release. Partial registry failures,
replayed exits, concurrent cleanup and Manager shutdown retry only the
unsettled Worker; another long-running shard cannot keep a failed instance
billable.
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
`harness_ref`. Uploads are capped at 1 MiB, validated from a private temporary
file, then published under a SHA-256 content address. The same bytes are
idempotent; new bytes cannot overwrite or delete the code referenced by an old
Job. Prefer declarative JobSpec for untrusted submitters.

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
ordering, template-aware S3 dataset line format, and strict `KEY=VALUE`
environment-variable lines before preflight.

The console manages Claude and Codex identities,
accepts write-only OpenAI passwords/mailbox query tokens (at least one for
Codex; both may be configured), filters Job account choices by `agent_type`,
supports reference-aware Agent API deletion, and lets EIP/non-EIP Jobs choose
the required number of unique identities in account-list order. Selecting one
unbound Agent API identity in non-EIP mode can auto-fill every slot. Use
JobSpec `account.ids` directly for another ordered/repeated mapping; OAuth and
EIP-bound identities cannot repeat.
The console shows active Codex OTP challenges as Worker-specific cards inside the
corresponding Job. Each card is keyed by login request and challenge, while a
single floating reminder links to all affected Workers and remains hidden when
no manual OTP is needed. Stable keyed rendering and
non-overlapping, visibility-aware polling preserve focus, expanded sections,
scroll position, and log viewing instead of rebuilding the whole page every
five seconds. Accounts and allocation state use their own visible-page
15-second single-flight refresh plus a manual action; backend state-read
failures render as unavailable rather than an empty/free allocation map.
OTP inputs, focus, and cursor selection also survive a Job-card
replacement without persisting the code in browser storage. Job cards start
collapsed with their identity, state, phase,
submission time, and Worker count visible; opening a card reveals its actions,
errors, cleanup state, results, and Worker execution table, and polling keeps
the user's open/closed choice. Completed execution rows remain available as
history. Each card also has a nested **Submission-time effective config
(redacted)** disclosure. It lazily fetches the single-Job detail only when
opened, formats the immutable journaled JobSpec as copyable JSON, and remains
available after a Manager restart. This is the validated/normalized effective
spec, not the raw request: `run.env`, setup-step environment values, and secret
references are replaced by markers, while command text is shown as submitted.
Do not embed credentials directly in commands. The polling list never carries
all specs, and the browser uses bounded in-memory concurrency, preview, and LRU
budgets rather than persistent storage. Command output remains queryable after
teardown; the read-only live
system-journal action remains available until the Worker resource is released,
then stops polling on a not-found/conflict response; destructive terminate
actions disappear at execution terminal state. Historical Job enumeration is
explicitly bounded and reports `truncated`; archived Job-log disk reads use a
dedicated fail-fast worker pool, so concurrent/cancelled readers cannot occupy
the Manager's shared lifecycle executor. Each Job keeps a stable result
action while metadata loads. Per-Job request versions reject stale responses,
known non-empty results never regress to empty on a transient or out-of-order
refresh, and terminal empty/error reads retain the last snapshot but retry with
bounded backoff until a successful non-empty final snapshot. Duplicate
archive downloads are suppressed. S3 result archives use the UI's cancellable
streaming endpoint, which starts returning the tarball while objects are read
instead of waiting for a complete Manager-side temporary archive. The action
shows received bytes and elapsed time; secure desktop Chromium writes chunks
directly to the selected file, while browsers without the File System Access
API use a memory-backed fallback only below 256 MiB of source data and reject
larger snapshots with a desktop-Chrome instruction instead of risking a tab
crash. Running Jobs label the action as a download of the latest uploaded
intermediate snapshot.
An unresolved submission stores its Idempotency-Key together with the frozen
spec in `sessionStorage`. After refresh or form edits, the console recommends
replaying that exact pair and does not wait for mutable provider defaults;
discarding it to create a new Key requires a separate double confirmation.
The original strict download endpoint remains available to API clients that
prefer a prebuilt archive and an HTTP error before response headers. API keys
are accepted only in
the `Authorization: Bearer` or `X-API-Key` header; the UI keeps a key in
`sessionStorage` and strips legacy query-string credentials. REST includes
`/api/accounts`, `/api/agent-api/accounts`,
`/api/accounts/login-attempts`, `/api/jobs`, `/api/jobs/{job_id}` (including
the redacted submission snapshot), `/api/jobs/{job_id}/logs`, and
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
ELASTIC_AGENT_MAX_REQUEST_BODY_BYTES
ELASTIC_AGENT_REQUEST_BODY_READ_TIMEOUT_SECONDS
ELASTIC_AGENT_MAX_CONCURRENT_REQUEST_BODIES
ELASTIC_AGENT_MAX_AGGREGATE_REQUEST_BODY_BYTES
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
