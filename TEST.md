# Test Guide

Use the locked development environment for every test run:

```bash
uv sync --extra dev
```

## Standard suites

Run the complete suite before merging:

```bash
uv run pytest -q
```

Run only the account/EIP lifecycle surface while developing this feature:

```bash
uv run pytest -q \
  tests/unit/test_account_binding.py \
  tests/unit/test_binding_manager.py \
  tests/unit/test_provider_eip.py \
  tests/unit/test_aws_provider.py \
  tests/unit/test_batch_hooks.py \
  tests/unit/test_batch_orchestrator.py \
  tests/unit/test_job_spec.py \
  tests/unit/test_api_batch.py \
  tests/unit/test_manager.py \
  tests/unit/test_account_store.py \
  tests/unit/test_account_login_api.py \
  tests/unit/test_codex_login.py \
  tests/unit/test_generic_harness.py \
  tests/unit/test_protocol_messages.py \
  tests/unit/test_worker_runtime.py \
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
- validation of AWS EIP mode (`per_worker=1`, matching account/worker counts,
  and no in-place account rotation);
- cleanup ordering and idempotent retry: final collect, detach EIP, terminate
  the temporary instance/root disk, release the lease, retain the EIP;
- bounded final collection (three attempts/300 seconds by default), IPv6
  disablement before login, current-source worker deployment, WSS enforcement,
  forced fresh runtime reconnect, strict request correlation, protected
  credential-home env, exact authenticated-email checks, and successful
  credential warm-up;
- compensation after allocation, create, attach, bootstrap, login, or run
  failures, plus REST API write-only token behavior and active claim/lease guards.
- agent-type-aware account uniqueness/allocation, Codex password/email-token
  one-of validation, explicit secret clearing, and token-only email-code switching, and
  write-only `has_password`/`has_email_token` REST behavior;
- correlated OTP-required events, six-digit validation, stale/mismatched
  challenge rejection, 32-hex injection protection, one-shot forwarding, retry
  races, Manager timeout/cancel propagation, cleanup acknowledgement, immediate
  disconnect failure, uncertain-cleanup account quarantine, and no retained OTP;
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
   confirm the fresh instance creates and verifies its own `CODEX_HOME/auth.json`.
5. Confirm final results are collected before cleanup, the EIP becomes
   detached, and the EC2 plus its root EBS are terminated.
6. Query the binding again: the same EIP allocation must still exist and be
   reusable by a second Job for the same account.
7. Exercise one forced failure (for example, an invalid setup command) and
   verify the temporary EC2 is still terminated while the EIP remains.
8. When testing is complete, call the explicit decommission endpoint and
   confirm the EIP allocation is released. Then remove the account identity.

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

The Codex `email_token` is only a supported mailbox-query token; it is not an
OpenAI API/OAuth token or a password. If OpenAI does not expose an email-code
action for a token-only account, the login must fail safely and request the
OpenAI password.
Generic IMAP is not implemented. For cross-host workers, set
`ELASTIC_AGENT_MANAGER_URL=wss://...`; the plaintext override is for trusted
test networks only.
