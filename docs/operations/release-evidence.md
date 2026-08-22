# Release Evidence

`deploy/release-manifest.json` is the canonical, non-secret release manifest.
It is content addressed with two SHA-256 values:

- `worker_profile_digest` covers the complete `worker_profile` object.
- `release_digest` covers the complete manifest after removing only
  `release_digest` itself.

Both digest strings use the exact `sha256:<64 lowercase hex>` wire format.
`manager_state_schema` uses the exact `v[1-9][0-9]{0,8}` format and is currently
`v1`.

The verifier requires the exact manifest schema, the archived source commit,
the archive SHA-256, and the stable `v1` state schema. It rejects unknown
fields and field names that could carry tokens,
passwords, or other credentials. Manager startup validates the manifest before
loading durable state; a missing, changed, or malformed manifest keeps the
Manager stopped (fail closed).

`GET /api/health` is authenticated with the existing Bearer service token or
administrator session. In addition to the existing route contract it returns
only non-secret release evidence fields: `manager_state_schema`,
`worker_profile_digest`, and `release_digest`. The systemd readiness probe
passes its first configured service token through curl stdin, so the token is
not placed in the command line or response body.

## Task Platform consumer contract

Task Platform must read and retain these three values directly from the
authenticated health response's top level:

```json
{
  "manager_state_schema": "v1",
  "worker_profile_digest": "sha256:<64 lowercase hex>",
  "release_digest": "sha256:<64 lowercase hex>"
}
```

The field names, location, prefix, case, and value formats are normative. A
consumer must not strip `sha256:`, substitute another image/source digest,
derive a value from `revision`, or silently drop an unrecognized field. Missing
or invalid values make deployment verification fail closed. The existing
`revision`, `aws_account_id`, `region`, and `route_contract` fields do not
replace any of these three evidence fields.

The AWS launcher compares the manifest with runtime settings for Worker AMI,
AWS account, Region, and release revision before touching state or making cloud
calls. The canonical production Worker AMI is
`ami-0c7d40ac988a900c5`; the historical `ami-0aec7ffcbe44c6f7a` is rejected.

## Immutable rollout

1. Build the release from the archived source commit recorded in the manifest.
2. Verify the archive SHA-256 and run the local manifest verifier and focused
   tests before copying the release to a new immutable directory.
3. Copy the release directory to an immutable path named
   `/opt/task-platform/elastic-agent-<release_revision>` without modifying it;
   the manifest and `uv.lock`
   remain read-only. Configure state and secret EnvironmentFiles separately.
4. Start the new Manager and wait for the authenticated local health probe.
   Record all three health evidence fields and compare them byte-for-byte with
   the manifest before allowing traffic.
5. Promote traffic only after the private route contract and idempotency-route
   checks pass. Do not edit a manifest in place; a changed source or worker
   profile is a new release and must receive a new digest.

## Immutable rollback

1. Stop admission at the API boundary while allowing cleanup, WebSocket
   terminal events, and authenticated health requests to drain.
2. Select the previously verified release directory by its recorded
   `release_digest`; never rebuild it from a mutable branch or edit the current
   manifest.
3. Start that exact directory with the existing state/secret files and wait
   for authenticated health. Confirm all three evidence fields match the
   recorded values before restoring traffic.
4. Re-run the release's focused tests and retain the failed release directory
   for forensic comparison. No AWS deployment, instance termination, or
   Manager restart is part of evidence generation itself.
