# Mode-B task supervisor

Long-running opaque commands run below `ea-task-supervisor.service`, not below
the reconnectable `ea-runtime.service`.

## Runtime restart contract

The two systemd services are separate units and cgroups. The supervisor owns:

- the child and `waitpid`;
- one POSIX session/process group per task;
- stdin and bounded stdout/stderr frames;
- the absolute timeout and signal escalation;
- a 0600 NDJSON spool;
- a 0600 non-secret descriptor and stable terminal event.

`ea-runtime` owns the Manager WebSocket and performs error classification,
rotation signalling and terminal delivery. On startup it inventories
`/run/elastic-agent-task-supervisor/control.sock` before its process list is
authoritative. A recovering STATUS has `process_inventory_complete=false`;
Manager reconciliation must retain the run until a complete inventory or a
reliable terminal event arrives.

Restarting only `ea-runtime.service` leaves the child PID and its files intact:

```bash
sudo systemctl restart ea-runtime.service
```

The replacement runtime tails the existing spool, resumes stdin/STOP control,
and reports the preallocated terminal event id. It does not rerun the command.

## Durable files and secret boundary

The supervisor state root is `~/ea-tasks` (0700). Each hashed task directory
contains owner-only JSON:

- `descriptor.json`: task id, PID/PGID, Linux PID start ticks, timing,
  non-secret error-classification metadata and event ids;
- `terminal.json`: exit status and the stable terminal event.

Command, cwd and environment are sent in a one-shot request over the 0600 Unix
socket and remain memory-only. They are not written to a systemd unit, journal,
descriptor or terminal record. Job secrets must continue to use
`run.secret_env`; placing a secret directly in a command argument inherently
exposes it in the child process's own `/proc/<pid>/cmdline` and is unsupported.

Complete task output remains in `~/ea-logs/<task-id>.ndjson` with mode 0600.
Application output can itself contain sensitive material, so the spool must
retain the same private handling as other Worker logs.

## Terminal and rotation ordering

The terminal event id is allocated and fsynced at launch. The supervisor keeps
the terminal record until Manager ACK reaches it through the runtime. If the
runtime dies at any send/ACK boundary, the next runtime emits the same id and
Manager event deduplication prevents duplicate lifecycle work.
After ACK, a bounded 0600 task-id tombstone remains so a delayed duplicate
`EXECUTE` cannot rerun the already-completed outer command.

When exhaustion is detected, runtime first fsyncs `{event_id, reason}` through
the supervisor, then stops the old process group, and only after terminal proof
emits `RUN_EXHAUSTED`. Its later `PROCESS_EXIT` remains the stale old-task exit
and existing task-id guards ignore it after rotation.

## Failure boundary

This mechanism covers runtime process/service replacement and WebSocket
disconnects on the same EC2 instance. It does not make arbitrary shell commands
portable across loss of the supervisor, EBS volume or instance. A supervisor
restart cannot recover pipe/waitpid ownership safely; it kills an exact
PID/start-time match and publishes `task_supervisor_restarted` fail-closed.

Cross-instance recovery still requires an application checkpoint committed to
S3 and an explicitly idempotent resume command. Never automatically replay an
arbitrary Mode-B outer command.

## Rollout checks

Before dispatching a long Job:

```bash
sudo systemctl is-active ea-task-supervisor.service ea-runtime.service
sudo systemctl show -p ControlGroup ea-task-supervisor.service ea-runtime.service
sudo stat -c '%a %U %G %n' \
  /run/elastic-agent-task-supervisor \
  /run/elastic-agent-task-supervisor/control.sock \
  "$HOME/ea-tasks" "$HOME/ea-logs"
```

The two `ControlGroup` values must differ. Directories must be 0700 and the
socket 0600. Automatic package maintenance/`needrestart` must be disabled for
both services; protecting only `ea-runtime` is insufficient.
