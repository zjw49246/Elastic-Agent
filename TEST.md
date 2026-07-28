# Test Guide

Use the locked development environment for every test run:

```bash
uv sync --extra dev --extra pty
```

## Standard suites

Run the complete suite before merging:

```bash
uv run pytest -q
```

The configuration, credential-rotation, and legacy file-sync contracts can be
checked together without the rest of the suite:

```bash
uv run pytest -q \
  tests/unit/test_config.py \
  tests/unit/test_credential_pool.py \
  tests/unit/test_credential_rotator.py \
  tests/unit/test_quota_monitor.py \
  tests/integration/test_credential_rotation_e2e.py \
  tests/integration/test_quota_monitor_e2e.py \
  tests/unit/test_file_sync.py \
  tests/integration/test_file_sync_e2e.py
```

Validate the production AWS launcher's fail-closed environment and AMI policy
without making cloud calls:

```bash
uv run pytest -q tests/unit/test_aws_manager_launcher.py
```

These tests cover required environment-only configuration, secret-free
settings/errors, WSS enforcement, an IMDSv2-only credential chain with exact
Manager-role identity, local state/key permissions, systemd readiness/teardown
bounds, exact production IAM resource pins, complete IAM simulator coverage,
the Worker's write-only result-prefix policy, S3 plaintext-transport
denial, the versioned common x86_64 production instance allowlist, and AMI availability,
architecture, HVM, ENA, IMDSv2, encryption, and provenance checks including the
explicit Canonical break-glass path.

Validate AWS private management-path selection across bootstrap, login, logs,
and collection:

```bash
uv run pytest -q \
  tests/unit/test_network.py \
  tests/unit/test_batch_hooks.py \
  tests/unit/test_manager_fleet_driver.py \
  tests/unit/test_oauth_race_and_retry.py \
  tests/unit/test_api.py
```

Validate the golden worker image's fail-closed fast paths and build-script
safety contract without making AWS writes:

```bash
uv run pytest -q \
  tests/unit/test_bootstrap_steps.py \
  tests/unit/test_golden_image_verify.py \
  tests/unit/test_golden_ami_script.py
bash -n scripts/build_golden_ami.sh
```

After installing a production release at `/home/ubuntu/elastic-agent`, validate
the shipped unit on that host with `systemd-analyze verify
deploy/aws/elastic-agent-manager.service` before replacing the active unit.

The marker alone is never sufficient: these tests cover exact dpkg versions,
commands, agent CLIs, Python distributions/imports, Chrome, Docker/buildx, and
the claude-pty VCS commit, plus complete fallback installers. Real promotion
also requires the standard/Docker/login/S3/EIP canaries documented in
`docs/operations/golden-worker-ami.md`.

Run only the account/EIP lifecycle surface while developing this feature:

```bash
uv run pytest -q \
  tests/unit/test_account_binding.py \
  tests/unit/test_binding_manager.py \
  tests/unit/test_provider_eip.py \
  tests/unit/test_aws_provider.py \
  tests/unit/test_batch_hooks.py \
  tests/unit/test_batch_orchestrator.py \
  tests/unit/test_reconciler.py \
  tests/unit/test_job_spec.py \
  tests/unit/test_job_log_store.py \
  tests/unit/test_api_batch.py \
  tests/unit/test_manager_fleet_driver.py \
  tests/unit/test_result_uploader.py \
  tests/unit/test_manager.py \
  tests/unit/test_account_store.py \
  tests/unit/test_agent_api_accounts.py \
  tests/unit/test_account_login_api.py \
  tests/unit/test_codex_login.py \
  tests/unit/test_generic_harness.py \
  tests/unit/test_protocol_messages.py \
  tests/unit/test_connection_manager.py \
  tests/unit/test_worker_runtime.py \
  tests/unit/test_worker_agent_api.py \
  tests/unit/test_bootstrap_steps.py \
  tests/unit/test_web_ui.py
```

The focused suite covers:

- atomic mode-`0600` `account_id → EIP` mappings and exclusive Job leases;
- EIP allocation/association/disassociation calls, including the AWS
  `AllowReassociation=False` safety guard;
- concurrent all-settled reserve-before-create orchestration, rollback without
  scale-out, and explicit/group-based account choice;
- durable mode-0600 JobSpec persistence before direct/API launch side effects,
  crash recovery collection, and whole-fanout capacity rejection before any EIP;
- strict unknown-field rejection, immutable environment-profile selection,
  finite run/Job TTL defaults and bounds, exact Git manifest validation, and
  backward-compatible legacy plus structured per-step setup executed as the Job
  user with isolated env/cwd/timeout/retry policy;
- side-effect-free `/api/jobs/plan` previews with secret values omitted, plus
  pre-persistence checks for Manager region/capacity, account availability, S3
  dataset instance-role requirements, and resolved S3 collection mode;
- validation of AWS EIP mode (`per_worker=1`, matching account/worker counts,
  and no in-place account rotation);
- cleanup ordering and idempotent retry: final collect, detach EIP, terminate
  the temporary instance/root disk, require an identity-matched `RELEASED`
  lease, clear task/Node/WS status state, retain the EIP, and preserve the Node
  plus account claim on missing/conflicting durable state, crossed claim
  identity, or a worker lease missing its durable instance ID;
- managed primary-ENI tagging plus tagged EIP/ENI detach authorization, and
  reconciler guards that ignore exact fully-released terminated history,
  recover unknown/incomplete leased orphans fail-closed, and quarantine an
  instance claimed by a conflicting active lease without cloud mutation;
- bounded final collection (three attempts/300 seconds by default), IPv6
  disablement before login, current-source worker deployment, WSS enforcement,
  forced fresh runtime reconnect, strict request correlation, protected
  credential-home env, exact authenticated-email checks, and successful
  credential warm-up;
- per-shard Worker result namespaces and collection manifests, direct/fallback
  S3 prefix parity, content-hash change detection, bounded/consistent result
  reads and downloads, running snapshot visibility for positive collection
  intervals, awaited Manager-side upload, explicit S3 failures, and
  force-termination plus Node-record removal (not registry-only draining) after
  ordinary Job completion;
- mode-0600 bounded Job command-log snapshots, ownership/path/symlink guards,
  replay-safe archive replacement, per-task/Job/global byte and retention
  quotas, streaming tail API filtering, and best-effort recovery from the
  Worker's local NDJSON before teardown after a Manager restart, including
  archived failed-task access after the in-memory Job is gone;
- stable failed-Job log/result controls, 5,000-line terminal log requests with
  exit summaries, per-Job result request versions, non-empty monotonic cache
  protection, finite terminal-empty retry, duplicate-download suppression, and
  cancellable S3 archive streaming that yields before later objects finish,
  closes the active S3 body on cancellation, and produces a valid tarball;
- ordered durable lifecycle-event replay, single-handler in-flight replay with
  failure/cancellation takeover, reconnect-on-handler-failure, stale-socket ACK
  suppression with deduplicated reconnect ACK, active-socket send-error
  propagation, STATUS coverage during final sync, cancellation during dispatch,
  non-blocking exhaustion/login rotation, and live compensation retry after
  ordinary EC2 creation failures, including timeout/cancellation after cloud
  acceptance, eventual-consistency tag rescans, and a create-to-event
  publication fence that protects current Jobs from orphan recovery;
- `run.secret_env` resolution only at dispatch and rejection of plaintext
  cross-host WebSockets before AWS secrets are read; worker clone never receives
  the Manager's repository token;
- compensation after allocation, create, attach, bootstrap, login, or run
  failures, plus REST API write-only token behavior and active claim/lease guards.
- CloudRouter and Codex-only ApexRouter Agent API provider registration,
  fixed-endpoint 15-second wall-clock-bounded no-redirect Bearer model/usage
  requests, Apex's pinned-Codex-version native model catalog and distinct
  per-key usage/shared-group quota normalization,
  30-second/concurrency-16 automatic pool refresh with unfinished-key OAuth
  fallback, one shared 60-second usage→key-delivery→Worker-ACK deadline,
  Claude/Codex model projection,
  optional exact `account.model` admission at plan/allocation/configure,
  60-second usage caching, unlimited/exhausted/auth/transient-last-known-dead
  admission, invalid-schema/numeric/expiry fail-closed behavior, per-account
  concurrent refresh, deterministic model-refresh benching, and private atomic
  Manager storage;
- correlated Manager-to-Worker API-key projection, 0700/0600 ownership and
  no-symlink checks, version-2 marker, fixed Claude/Codex routing, byte-exact
  helper plus environment-sanitizing launcher integrity,
  inherited auth/base/provider scrubbing, Claude project/local settings and
  hook/MCP exclusion, PTY wrapper selection, structured provider error
  promotion from exit 0, durable runtime hard-quota benching with successful
  re-probe recovery, API-first OAuth fallback, explicit-ID mapping, and
  projection-external sibling slots for dynamic API→OAuth rotation,
  claim release only after ordinary Worker teardown, and startup-recovery
  admission fencing before any API key read or Worker send while OAuth remains
  available; runtime auth feedback is pinned to an immutable
  `task_id/account_id/auth_kind` dispatch snapshot so stale exhaustion/exit
  replay cannot bench a newly rotated account;
- Mode-B POSIX process-group teardown for STOP, timeout, exhaustion, and
  parent-before-child exit; terminal provider-transient failures (CloudRouter
  500/502 and ApexRouter 429/500/502) fail without emitting `RUN_EXHAUSTED` or
  triggering `rotation.resume_args`; PTY hard limits terminate
  without claiming an automatic cross-account resume; PTY launch/STOP/shutdown
  cancellation and teardown exceptions still converge on one reliable terminal
  handoff;
- reserved nested-container projection hand-off scrubbing/override across
  subprocess, login shell, and PTY paths; compatible container runners must
  validate the marker and use an exact one-root read-only mount without putting
  the key in environment variables or Docker arguments; EIP JobSpec rejects
  HTTP(S)/ALL proxy variables from both plain and secret env so managed traffic
  cannot bypass the account's stable public egress;
- Agent API REST/UI write-only behavior, CloudRouter/ApexRouter
  add/refresh/usage controls, provider-aware Agent support (ApexRouter is
  Codex-only), quota/model display, no browser-persisted Key, and sanitized
  validation/upstream errors.
- Batch Job form information architecture: eight ordered fieldsets retain every
  existing JobSpec control ID and mapping; representative controls stay in the
  correct section; labels/help are programmatically associated; conditional
  account/EIP/rotation controls expose clear disabled states; native bounds,
  mobile full-width actions, and CloudRouter/ApexRouter behavior remain covered.
- Batch Console Worker history/resource separation: completed execution rows
  report their release proof explicitly, display destroyed resources as history,
  suppress live actions after teardown, keep read-only system logs until release,
  stop polling those logs after a 404/409, and suppress terminate once execution
  reaches a terminal phase.
- Batch/Fleet default-light theming, keyed DOM reconciliation, non-overlapping
  visibility-aware polling, default-collapsed Job cards whose user-selected
  disclosure state survives refresh, and a persistent Job output viewer that
  remains available for terminal execution history. OTP UI tests cover
  per-Worker cards in the exact Job, account email/ID + full Worker + shard
  labels, challenge-only visibility, keyed DOM transplant with input/focus
  preservation, single collapsible mobile reminder, and no browser-persisted
  code.
- agent-type-aware account uniqueness/allocation, Codex password/email-token
  one-of validation, explicit secret clearing, token-only email/one-time/login-code
  switching, bounded anti-bot diagnostics, 900-second worker timeout propagation, and
  write-only `has_password`/`has_email_token` REST behavior;
- correlated OTP-required events, six-digit validation, stale/mismatched
  challenge rejection, 32-hex injection protection, one-shot forwarding, retry
  races, multi-Worker account/request isolation, cross-Worker spoof rejection,
  concurrent-submit claiming, transport recovery, Manager timeout/cancel
  propagation, cleanup acknowledgement, immediate disconnect failure,
  uncertain-cleanup account quarantine, and no retained OTP;
- Codex `CODEX_HOME` injection, pinned CLI/current-source bootstrap, exact
  JWT-email validation, mandatory `codex exec` smoke test, secret redaction, and
  transactional auth rollback on failure/cancellation, including non-root
  single-slot HOME resolution and Codex-specific runtime health reporting.

## Management API smoke test

Configure `ELASTIC_AGENT_EXTERNAL_API_KEYS`, then pass one key as a bearer
token. The following calls are safe against a test Manager, except the final
decommission call, which permanently releases the EIP:

```bash
export EA_TEST_URL=http://127.0.0.1:8002
export EA_TEST_KEY=replace-with-test-key

curl -fsS -H "Authorization: Bearer $EA_TEST_KEY" \
  "$EA_TEST_URL/api/accounts/bindings"

curl -fsS -X PUT \
  -H "Authorization: Bearer $EA_TEST_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"region":"us-east-1"}' \
  "$EA_TEST_URL/api/accounts/test-account/binding"

curl -fsS -H "Authorization: Bearer $EA_TEST_KEY" \
  "$EA_TEST_URL/api/accounts/test-account/binding"

curl -fsS -H "Authorization: Bearer $EA_TEST_KEY" \
  "$EA_TEST_URL/api/accounts/allocations"
```

Only after the test Job is terminal and the address is intentionally no longer
needed, decommission it with both confirmations:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $EA_TEST_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"release_eip":true,"confirm_account_id":"test-account"}' \
  "$EA_TEST_URL/api/accounts/test-account/binding/decommission"
```

The endpoint must return `409` while an active account claim or lease exists.
Account deletion must also return `409` until its binding has been explicitly
decommissioned.

The Batch Console account-delete regression additionally verifies that a
bound OAuth account is handled in this order: read the current binding, show
the irreversible EIP warning, require an exact typed account ID, decommission
with both confirmations, and only then delete the identity. A missing binding
uses the ordinary confirmed delete path. A `409` from an active Job or cleanup
must stop before identity deletion.

## Real AWS smoke checklist

Use a disposable account identity and a non-production AWS account/Region.
Verify the Region has EIP quota available and remember that allocated public
IPv4 addresses incur hourly charges even while detached.

1. Add the test account to `/api/accounts` and ensure its binding with `PUT
   /api/accounts/{id}/binding`. The account and Job must use the same
   `agent_type`; a Codex account requires an OpenAI password or supported
   mailbox query token. Use password-only to exercise manual OTP, or a 163.com
   token-only account to exercise MailCatcher email-code login.
2. Submit a one-worker Job with `account.binding="eip"`, `per_worker=1`, the
   test account in `ids`, `rotation.strategy="none"`, the matching
   `account.agent_type`, and a matching `fanout.region`.
3. Confirm the lease is reserved before `RunInstances`, and the new EC2 has
   tags for the Job/account/lease.
4. Confirm the worker's observed public IP equals the binding returned by the
   API, then allow the worker-local login and command to finish. For Codex,
   confirm the fresh instance creates and verifies its own `CODEX_HOME/auth.json`;
   redirect a direct, unattended `codex exec` smoke command with `</dev/null`
   because the worker keeps its interactive stdin pipe open.
5. Confirm final results are collected before cleanup, the EIP becomes
   detached, and the EC2 plus its root EBS are terminated.
6. Query the binding again: the same EIP allocation must still exist and be
   reusable by a second Job for the same account.
7. Exercise one forced failure (for example, an invalid setup command) and
   verify the temporary EC2 is still terminated while the EIP remains.
8. When testing is complete, call the explicit decommission endpoint and
   confirm the EIP allocation is released. Then remove the account identity.

For an ordinary (non-EIP) Job recovery drill, inject a provider timeout after
`RunInstances` acceptance, persist the Job as failed, and restart the Manager
before its first recovery scan. Verify that `unbound-launches.json` keeps Agent
API admission closed across an initially empty cloud scan, then that a later
visible matching instance is collected, confirmed terminated, and removed
from the intent journal. Repeat once with a fully published NodeRecord and no
remaining launch intent: make both the first tag-list lookup and first exact-ID
lookup miss, then verify startup stays blocked until the exact durable worker
appears and is terminated.

Do not infer login persistence from a repeated EIP. Each new instance must run
the worker-local login again; this feature does not persist auth files, browser
state, or device identity.

Automatic worker-local login supports Claude and Codex. For Codex, create an
account with `agent_type="codex"` and at least one of an OpenAI password or
supported mailbox-query `email_token`. Leave `email_token` empty to exercise
password plus manual OTP; use a token-only 163.com account to verify the
email-code switch and MailCatcher lookup. Submit a Job with the same
`account.agent_type`, then poll `GET /api/accounts/login-attempts` and forward
the current six-digit code with
`POST /api/accounts/login-attempts/{login_request_id}/otp`. Verify the Job does
not start before identity verification and the `codex exec` smoke test passes,
REST/logs never expose the password, mailbox token, OTP, or authorization URL,
and a failed/cancelled/timed-out login restores the previous
`CODEX_HOME/auth.json` rather than completing after the Job has failed.
In particular, run mailbox polling with `httpx` INFO logging enabled and assert
that neither `httpx` nor inherited `httpcore` request logs contain the mailbox
query token or its complete request URL.

For the multi-Worker manual path, launch at least two password-only Codex
Workers and wait until both publish challenges. Confirm each GET item and UI
card has the exact `login_request_id`, `worker_id`, `account_id`,
`account_email`, `job_id`, `job_name`, and `shard_index`; only affected Job
cards should expand. Enter different six-digit values without submitting,
force a Job refresh, and verify both values, focus, and cursor selection remain.
Submit one card and confirm only that request disappears/receives
`ACCOUNT_LOGIN_OTP`; the other card must remain actionable. On a narrow mobile
viewport, “查看并填写” must collapse the floating reminder and leave the focused
input visible. With no active challenge, the reminder, badges, and inline OTP
regions must all be hidden.

The Codex `email_token` is only a supported mailbox-query token; it is not an
OpenAI API/OAuth token or a password. If OpenAI does not expose an email-code
action for a token-only account, the login must fail safely and request the
OpenAI password.
Generic IMAP is not implemented. For cross-host workers, set
`ELASTIC_AGENT_MANAGER_URL=wss://...`; the plaintext override is for trusted
test networks only.
