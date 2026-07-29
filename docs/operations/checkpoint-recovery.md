# Mode-B reconnect and checkpoint recovery

Long-running Mode-B Jobs use two recovery layers. They deliberately do not
silently replay an arbitrary shell command.

## Recovery boundary

| Failure | Behaviour |
| --- | --- |
| Manager WebSocket disconnect or `ea-runtime` restart on the same Worker | `ea-task-supervisor` keeps the original PID/process group running. The replacement runtime inventories it, resumes the private output spool, and sends the same terminal event id. A socket grace period alone does not destroy the Worker; reconnect, reliable exit, explicit cancellation, cloud-terminal reconciliation, or Job TTL decides liveness. |
| Worker instance, EBS volume, or supervisor loss | The original process cannot be reattached. Submit a new Job from the latest complete S3 checkpoint set. Only work newer than that set is repeated. |
| No complete checkpoint set | Start from the beginning. Ordinary/legacy S3 results remain downloadable, but cannot safely seed an automatic resume because their mutable prefix cannot prove deletions or one complete generation. |

The supervisor and runtime are separate systemd units and cgroups. Restarting
`ea-runtime.service` must not restart `ea-task-supervisor.service`. Worker
bootstrap disables background package upgrades and configures `needrestart` to
exclude both units.

## Creating recoverable checkpoints

Configure a stable logical shard, a positive collection interval, and immutable
checkpoint mode:

```json
{
  "fanout": {
    "workers": 8,
    "shard_by": "shard_index"
  },
  "collect": {
    "paths": ["results"],
    "exclude": [".venv/**", "**/core"],
    "interval_seconds": 120,
    "checkpoint": true,
    "checkpoint_keep_generations": 3
  }
}
```

Checkpoint mode requires `ELASTIC_AGENT_RESULTS_S3_BUCKET`. It always stages a
Manager-side exact snapshot; Worker-direct mutable uploads are not used as
recovery proof.

Every workload path that participates in output, resume, setup, or rotation
must be stable across replacement instances. Use `{{shard_id}}` or
`{{shard_index}}`; checkpoint Jobs reject `{{hostname}}`, `$HOSTNAME`, and
`$(hostname ...)` because a new EC2 has a different hostname.

Each successful shard collection publishes an immutable shard manifest only
after every referenced blob exists. Blobs are addressed by SHA-256 and reused
across generations of the same Job. A Job-level checkpoint set explicitly
freezes one immutable generation for every `shard-00000` ... `shard-N`.
Normally those are one coordinated periodic generation; after a shard reaches
terminal state, a later complete set may intentionally combine that shard's
frozen `final` generation with periodic generations from still-running shards.
The resolver selects the newest committed complete mapping and never assembles
one ad hoc from independently newest shards. Retention keeps the latest complete
sets and garbage-collects unreferenced manifests/blobs.

Application output should itself use atomic file replacement. In the AI4Sci
branch, metadata, trajectory, raw and execution artifacts are published with a
same-directory temporary file, file `fsync`, atomic rename, and directory
`fsync`; the instance result JSON is published last as the resume completion
marker. A checkpoint taken while several related files are being rewritten is
file-integrity safe, but cannot invent an application-level transaction across
arbitrary files.

## Resuming on replacement Workers

The Batch page exposes **从检查点恢复** on a Job that has a complete checkpoint
set. Prefer that action, or call the server-side recovery endpoint:

```http
POST /api/jobs/recover
Idempotency-Key: <caller-generated stable key>
Content-Type: application/json

{
  "source_job_id": "job-source",
  "generation": "",
  "run": {
    "command": "uv run bench --resume",
    "timeout": 172800
  },
  "ttl_seconds": 259200
}
```

`generation`, `run.command`, `run.timeout`, and `ttl_seconds` are optional.
Everything else is copied from the source's private persisted JobSpec. This is
important because Job detail deliberately redacts ordinary environment values,
setup-step environment values, and secret references; the browser cannot and
must not reconstruct them. The recovery response remains redacted, while the
new mode-`0600` journal retains the original private values server-side.
This convenience endpoint is intentionally limited to declarative Jobs; a
custom `harness_ref` remains an explicit trusted-code workflow.

The endpoint uses the normal preflight, durable prepare, and idempotent submit
path. An empty generation is resolved once and pinned into the private target
JobSpec before that durable prepare, so a newer set cannot slip in between
preflight and staging. Reusing the same `Idempotency-Key` with the same request
returns the same Job; reusing it with different overrides returns `409`.

The replacement command still needs the application's resume flag. For AI4Sci
`run`, use the same stable path for `--output-dir` and `--resume`, for example
`results/opus48_shard-{{shard_id}}_seed128`. For AI4Sci `batch-run`,
`--output-dir` and `--resume` must name the same restored batch root. Leaving
the command unchanged is appropriate only when that command already resumes
from the restored state.

For lower-level clients, the equivalent complete target JobSpec is:

```json
{
  "setup": {
    "repo": "https://github.com/example/bench.git",
    "resolved_commit": "0123456789abcdef0123456789abcdef01234567"
  },
  "fanout": {
    "workers": 8,
    "shard_by": "shard_index"
  },
  "recovery": {
    "policy": "checkpoint",
    "source_job_id": "job-source",
    "paths": ["results"],
    "generation": ""
  }
}
```

Leave `generation` empty for the latest complete set, or copy an exact set id
from the source Job. Recovery is rejected unless:

- the source is terminal, or an interrupted historical Job is proven to have
  no live Worker, cloud instance, launch intent, or account lease;
- source and target have the same fan-out size, stable shard mode, repository,
  resolved commit, agent type, and model;
- `recovery.paths` exactly matches the source checkpoint paths;
- every shard manifest, object size, SHA-256, source metadata, aggregate
  byte/object total, Manager staging limit, and target-disk allowance matches.

Preflight first resolves the requested real S3 complete set without persisting
a target Job or creating resources. All shards are then downloaded into a
private, bounded Manager staging tree before any EC2 is created. The Manager
reserves logical bytes, conservative filesystem allocation blocks, and inodes;
it rejects symlinks and special files. Each rsync gets a pre-spawn, fsynced
Manager transfer record. Staging and its capacity reservation are quarantined
until that transfer's complete process group is proven terminated; startup
settles any surviving journal before deleting stale staging. After provisioning,
every selected directory is first `rsync --delete`-ed into the root-private
`/var/lib/elastic-agent/recovery-transactions-v1` tree outside the workload
checkout. That control tree must share the target filesystem so installation
can use atomic rename. The Worker fsyncs and re-measures the entire staged shard
against its authenticated totals, writes a durable `installing` journal, then
replaces each directory by same-filesystem rename. Its capacity check charges
every object and transaction wrapper at least one 4 KiB allocation unit in
addition to logical bytes and the 10 GiB Worker reserve. Only a durable
`installed` marker unlocks login and command dispatch.
If the Manager dies between two directory renames, startup rolls the exact
pinned transaction forward; if transfer never committed, startup refuses final
collection rather than publishing a half-restored checkpoint. Cancellation
during Manager staging never crosses the cloud-create boundary.

Before restart-time final collection, the Manager stops and runtime-masks the
current and legacy runtime/task units, removes all Docker containers on the
dedicated Worker, stops Docker/containerd, verifies their cgroups are empty,
and scans `/proc` twice for surviving Job-user, container-runtime, or
worktree-linked processes. It then verifies or completes the recovery
transaction and only afterwards collects. Failure to prove either quiescence or
an installed transaction skips collection but does not retain the billable
instance.

The S3 set's `COMMITTED` marker is authoritative. If the Manager crashes after
that object is published but before its local latest-generation pointer is
updated, the Job remains recoverable and an empty generation resolves the
newest complete set directly from S3.

## Older Jobs

Jobs created before immutable checkpoints cannot be used as automatic recovery
sources. The old `legacy_final_collection` literal remains readable in private
historical journals, but new plan/submit requests reject it. Its mutable S3
prefix uploads current files without an authoritative deletion manifest, so a
stale object could otherwise be resurrected into the resumed workload. Download
what is useful for diagnosis, then start the Job from the beginning with
checkpoint mode enabled.

Unsaved process memory and bytes written after the last completed collection
cannot be reconstructed from S3. Those items must run again.

## Operational checks

Before relying on reconnect:

```bash
sudo systemctl is-active ea-task-supervisor.service ea-runtime.service
sudo systemctl show -p ControlGroup \
  ea-task-supervisor.service ea-runtime.service
sudo stat -c '%a %U %G %n' \
  /run/elastic-agent-task-supervisor/control.sock \
  "$HOME/ea-tasks" "$HOME/ea-logs"
```

The two cgroups must differ. The socket and state files must remain private.
Monitor Manager disk capacity as well as the S3 bucket; checkpoint staging
fails closed when its byte, object, inode, or free-space reserve would be
exceeded. Manager-relay collection first scans the Worker with bounds, receives
into a private attempt directory, validates the complete tree, commits the
immutable checkpoint, and only then atomically replaces the last published
mutable result snapshot. A failed or partial rsync leaves the previous
successful snapshot intact.

The versioned AWS Manager policy grants `s3:DeleteObject` only below the
default internal `jobs/.elastic-agent-checkpoints/*` prefix so retention can
prune old manifests and blobs without deleting public Job results. If
`ELASTIC_AGENT_CHECKPOINT_S3_PREFIX` or the results prefix is changed, update
and re-simulate that exact IAM resource before enabling checkpoint Jobs.
