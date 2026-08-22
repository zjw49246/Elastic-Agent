# Release Evidence

`deploy/release-manifest.json` is the canonical, non-secret release manifest.
Schema v3 separates two digest domains: `worker_runtime_provenance` and its
`worker_runtime_provenance_digest` identify EA/AMI provenance, while
`worker_profile` is the exact 11-field Task Platform `WorkerProfileInput` and
`worker_profile_digest` hashes that object. The v3 generator requires an
external, complete profile JSON and fails closed when it is absent or has
unknown/missing fields. Test-only fixtures must never be copied into a
production manifest.
`deploy/release-files.json` indexes every tracked release file except those two
self-referential generated files. Each record binds path, type, executable
mode, size, and SHA-256. It is content addressed with three SHA-256 values:

- `release_artifact_digest` covers the canonical complete file index.
- `worker_profile_digest` covers the complete `worker_profile` object.
- `release_digest` covers the complete manifest after removing only
  `release_digest` itself.

Both digest strings use the exact `sha256:<64 lowercase hex>` wire format.
`manager_state_schema` uses the exact `v[1-9][0-9]{0,8}` format and is currently
`v1`.

`upstream_source_commit` and `upstream_archive_sha256` truthfully retain the
original `e06ac...` provenance; they are not claimed as the final release.
`release_revision` is `artifact-sha256:<64 hex>` derived from the exact final
file index. The verifier rejects unknown fields and field names that could carry
tokens, passwords, or other credentials. Manager startup validates the manifest
and hashes every indexed file before loading durable state; missing, changed,
mode-shifted, or unexpected files keep the Manager stopped (fail closed).

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
AWS account, Region, and artifact release revision before touching state or
making cloud calls. The canonical production Worker AMI is
`ami-0c7d40ac988a900c5`; the historical `ami-0aec7ffcbe44c6f7a` is rejected.

## Immutable rollout

1. Build the release from the upstream archive recorded in the manifest.
2. After all tracked changes are committed and the authoritative Task Platform
   11-field WorkerProfileInput JSON is available, run
   `uv run python scripts/generate_release_evidence.py --worker-profile-input /absolute/path/profile.json`, commit only the
   generated manifest/index, and require its `--check` mode to pass.
3. Copy the release directory to an immutable path named
   `/opt/task-platform/elastic-agent-<artifact hex>` without modifying it;
   the manifest, file index, and `uv.lock`
   remain read-only. Configure state and secret EnvironmentFiles separately.
4. Start the new Manager and wait for the authenticated local health probe.
   Record all three health evidence fields and compare them byte-for-byte with
   the manifest before allowing traffic.
   The root-owned deployment EnvironmentFile must set
   `ELASTIC_AGENT_RELEASE_REVISION` to the manifest's exact
   `artifact-sha256:<64 hex>` value.
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
