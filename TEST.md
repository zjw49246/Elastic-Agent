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
   /api/accounts/{id}/binding`.
2. Submit a one-worker Job with `account.binding="eip"`, `per_worker=1`, the
   test account in `ids`, `rotation.strategy="none"`, and a matching
   `fanout.region`.
3. Confirm the lease is reserved before `RunInstances`, and the new EC2 has
   tags for the Job/account/lease.
4. Confirm the worker's observed public IP equals the binding returned by the
   API, then allow the worker-local login and command to finish.
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

Automatic login in this repository is Claude-only. `group=codex` does not add a
Codex login/runtime path. The supported mail flows are 171mail and mail.com;
generic IMAP is not implemented, and a future IMAP test must use an app-specific
mailbox authorization code rather than a normal web password. For cross-host
workers, set `ELASTIC_AGENT_MANAGER_URL=wss://...`; the plaintext override is
for trusted test networks only.
