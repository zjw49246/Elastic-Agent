"""Durable account-to-EIP bindings and short-lived instance leases.

The stable resource is an Elastic IP keyed by the provider account id.  EC2
instances are deliberately absent from :class:`AccountBinding`: every job gets
an :class:`AccountLease`, attaches the account's EIP to its temporary instance,
then destroys that instance while keeping the allocation for the next job.

Both records share one JSON file and one lock.  Reserving a lease is therefore
an atomic check-and-insert operation inside a Manager process, and the durable
record prevents a restarted Manager from handing an already leased account to
another job.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Collection
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from elastic_agent.core.secure_store import (
    atomic_write_private,
    secure_state_directory,
    tighten_state_file,
)

logger = logging.getLogger(__name__)


class BindingState:
    """Lifecycle of the persistent EIP allocation."""

    ALLOCATING = "allocating"
    READY = "ready"
    DECOMMISSIONING = "decommissioning"
    ERROR = "error"


class LeaseState:
    """Lifecycle of one job's exclusive use of an account and its EIP."""

    RESERVED = "reserved"
    ATTACHING = "attaching"
    ATTACHED = "attached"
    RELEASING = "releasing"
    ERROR = "error"
    RELEASED = "released"


class LeaseConflictError(RuntimeError):
    """The account or temporary instance is already claimed by another lease."""


class BindingStoreCorruptError(RuntimeError):
    """The durable binding file cannot be trusted and must not be overwritten."""


class AccountBinding(BaseModel):
    """One account's permanent Elastic IP assignment.

    ``email`` is only a display snapshot.  It is intentionally not an identity
    key because addresses can be edited or reused while ``account_id`` remains
    stable.
    """

    model_config = ConfigDict(extra="forbid")

    account_id: str
    email: str = ""
    eip_allocation_id: str | None = None
    eip_ip: str | None = None
    cloud_provider: str = ""
    cloud_account_id: str = ""
    region: str = ""
    controller_id: str = ""
    state: Literal["allocating", "ready", "decommissioning", "error"] = (
        BindingState.ALLOCATING
    )
    error: str | None = None
    # Durable administrative intent.  In particular, a decommission must not
    # be silently undone by the next Job after the Manager crashes between the
    # cloud release and the local record removal.
    last_operation: str | None = None
    # Set durably immediately before ReleaseAddress.  A decommission retry may
    # treat a missing allocation as success only after this proves that a prior
    # release call was actually attempted; a single eventually-consistent
    # DescribeAddresses miss must never discard the only allocation handle.
    eip_release_attempted: bool = False
    # ``attempted`` is an intent marker and can survive a crash immediately
    # before the API call.  Record the stronger post-call fact separately.
    eip_release_succeeded: bool = False
    # A stable absence observed without a confirmed ReleaseAddress response
    # must be repeated by a later decommission request before the durable
    # allocation handle is discarded.
    eip_absence_confirmed: bool = False
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)

    def touch(self) -> None:
        self.updated_at = time.time()


class AccountLease(BaseModel):
    """Durable, exclusive claim on an account for one job worker slot."""

    model_config = ConfigDict(extra="forbid")

    lease_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    account_id: str
    email: str = ""
    job_id: str
    slot: int = 0
    generation: int = 1
    instance_id: str | None = None
    worker_id: str = ""
    # Set immediately before the external RunInstances call and cleared only
    # after its exact instance id is durably known (or bounded tag scans prove
    # no instance appeared).  This prevents an SDK timeout-after-create from
    # being mistaken for a harmless unattached reservation.
    launch_uncertain: bool = False
    state: Literal[
        "reserved", "attaching", "attached", "releasing", "error", "released"
    ] = LeaseState.RESERVED
    error: str | None = None
    last_operation: str | None = None

    # Release is a retryable multi-step transaction.  Persisting each completed
    # phase makes a second call idempotent after a Manager/cloud failure.
    worker_cleanup_required: bool = False
    worker_cleanup_done: bool = False
    eip_detached: bool = False
    instance_terminated: bool = False
    # Manager-restart recovery cannot reconstruct the in-memory Job, but it can
    # use the persisted JobSpec to make one bounded final collection attempt
    # before tearing the temporary disk down.  Persist the outcome for APIs.
    recovery_collection_attempted: bool = False
    recovery_collected: bool = False
    recovery_collection_error: str | None = None

    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    released_at: float | None = None

    def touch(self) -> None:
        self.updated_at = time.time()


class BindingsConfig(BaseModel):
    """Root model for ``bindings.json``."""

    model_config = ConfigDict(extra="forbid")

    version: Literal[2] = 2
    # Stable deployment identity used in every EIP/EC2 tag.  A filesystem lock
    # only coordinates processes sharing this JSON path; this id prevents a
    # different Elastic Agent deployment in the same AWS account/region from
    # adopting or terminating our resources.
    controller_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    bindings: list[AccountBinding] = Field(default_factory=list)
    leases: list[AccountLease] = Field(default_factory=list)


class AccountBindingStore:
    """JSON-backed binding and lease store.

    Bindings are keyed by ``account_id`` and leases by ``lease_id``.  Returned
    models are deep copies so callers cannot mutate in-memory durable state
    without an explicit update.
    """

    def __init__(self, path: str) -> None:
        self._path = Path(path).expanduser()
        self._lock = asyncio.Lock()
        self._config = BindingsConfig()
        self._loaded = False

    @property
    def path(self) -> Path:
        return self._path

    @property
    def controller_id(self) -> str:
        self._ensure_loaded()
        return self._config.controller_id

    async def load(self) -> None:
        async with self._lock:
            if not self._loaded:
                self._load_sync()

    def _load_sync(self) -> None:
        secure_state_directory(self._path.parent)
        if self._path.exists():
            try:
                tighten_state_file(self._path)
                raw = json.loads(self._path.read_text(encoding="utf-8"))
                legacy = raw.get("version", 1) < 2 or any(
                    "instance_id" in item for item in raw.get("bindings", [])
                )
                if legacy:
                    migrated = self._migrate_v1(raw)
                    self._validate_invariants(migrated)
                    self._write_config_sync(migrated)
                else:
                    loaded = BindingsConfig.model_validate(raw)
                    self._validate_invariants(loaded)
                    if not raw.get("controller_id"):
                        # Persist a generated id immediately; allowing it to
                        # change across restarts would defeat cloud tag scope.
                        self._write_config_sync(loaded)
                    else:
                        self._config = loaded
                # Older versions created this journal using the process umask,
                # which commonly left account emails and cloud identifiers
                # world-readable.  Repair a valid legacy file on first load.
                tighten_state_file(self._path)
            except Exception as exc:
                # This file is the source of truth for billable EIPs and active
                # instance leases.  Treating corruption as an empty store could
                # allocate duplicate addresses and orphan running EC2s, then
                # atomically overwrite the only recovery data.  Fail closed and
                # leave the original bytes untouched for operator recovery.
                self._loaded = False
                logger.error("Cannot load account binding store %s", self._path)
                raise BindingStoreCorruptError(
                    f"account binding store is corrupt: {self._path}"
                ) from exc
        else:
            # The controller id scopes destructive cloud recovery.  Persist it
            # even before the first binding exists so it cannot change between
            # process starts (or between Manager construction and ``load``).
            self._write_config_sync(BindingsConfig())
        self._loaded = True

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self._load_sync()

    def _write_config_sync(self, config: BindingsConfig) -> None:
        """Durably write ``config`` before publishing it as in-memory state."""
        self._validate_invariants(config)
        payload = json.loads(config.model_dump_json())
        atomic_write_private(self._path, json.dumps(payload, indent=2))
        self._config = config

    @staticmethod
    def _validate_invariants(config: BindingsConfig) -> None:
        """Reject contradictory ownership instead of guessing billable state."""
        if not config.controller_id.strip():
            raise ValueError("EIP binding controller_id must not be empty")
        binding_states = {
            BindingState.ALLOCATING,
            BindingState.READY,
            BindingState.DECOMMISSIONING,
            BindingState.ERROR,
        }
        lease_states = {
            LeaseState.RESERVED,
            LeaseState.ATTACHING,
            LeaseState.ATTACHED,
            LeaseState.RELEASING,
            LeaseState.ERROR,
            LeaseState.RELEASED,
        }
        for binding in config.bindings:
            if binding.state not in binding_states:
                raise ValueError(
                    f"binding {binding.account_id!r} has invalid state "
                    f"{binding.state!r}"
                )
            if binding.eip_release_succeeded and not binding.eip_release_attempted:
                raise ValueError(
                    f"binding {binding.account_id!r} records EIP release success "
                    "without a release attempt"
                )
            if (
                binding.eip_release_attempted
                or binding.eip_release_succeeded
                or binding.eip_absence_confirmed
            ) and not binding.eip_allocation_id:
                raise ValueError(
                    f"binding {binding.account_id!r} has EIP release state "
                    "without an allocation handle"
                )
        account_ids = [binding.account_id for binding in config.bindings]
        if len(account_ids) != len(set(account_ids)):
            raise ValueError("duplicate account_id in EIP bindings")
        foreign_controllers = {
            binding.controller_id
            for binding in config.bindings
            if binding.controller_id
            and binding.controller_id != config.controller_id
        }
        if foreign_controllers:
            raise ValueError(
                "EIP bindings contain controller ids outside this store: "
                + ", ".join(sorted(foreign_controllers))
            )

        allocations = [
            binding.eip_allocation_id
            for binding in config.bindings
            if binding.eip_allocation_id
        ]
        if len(allocations) != len(set(allocations)):
            raise ValueError("duplicate eip_allocation_id in EIP bindings")

        lease_ids = [lease.lease_id for lease in config.leases]
        if len(lease_ids) != len(set(lease_ids)):
            raise ValueError("duplicate lease_id in account leases")

        binding_accounts = set(account_ids)
        active_by_account: dict[str, str] = {}
        active_instances: dict[str, str] = {}
        for lease in config.leases:
            if lease.state not in lease_states:
                raise ValueError(
                    f"lease {lease.lease_id!r} has invalid state {lease.state!r}"
                )
            if lease.worker_id and not lease.instance_id:
                raise ValueError(
                    f"lease {lease.lease_id!r} identifies worker "
                    f"{lease.worker_id!r} without an instance"
                )
            if lease.state in {LeaseState.ATTACHING, LeaseState.ATTACHED} and (
                not lease.instance_id
            ):
                raise ValueError(
                    f"{lease.state} lease {lease.lease_id!r} has no instance"
                )
            if (
                lease.state == LeaseState.ERROR
                and lease.last_operation in {"attach", "create_instance"}
                and not lease.launch_uncertain
                and not lease.instance_id
            ):
                raise ValueError(
                    f"failed {lease.last_operation} lease {lease.lease_id!r} "
                    "has no instance and is not launch-uncertain"
                )
            if lease.state == LeaseState.RELEASED:
                if lease.launch_uncertain:
                    raise ValueError(
                        f"released lease {lease.lease_id!r} has unresolved launch"
                    )
                if not lease.eip_detached:
                    raise ValueError(
                        f"released lease {lease.lease_id!r} has not detached its EIP"
                    )
                if not lease.instance_terminated:
                    raise ValueError(
                        f"released lease {lease.lease_id!r} has not terminated its instance"
                    )
                if lease.worker_cleanup_required and not lease.worker_cleanup_done:
                    raise ValueError(
                        f"released lease {lease.lease_id!r} has pending worker cleanup"
                    )
                if lease.last_operation != "release":
                    raise ValueError(
                        f"released lease {lease.lease_id!r} has invalid last operation"
                    )
                if lease.released_at is None:
                    raise ValueError(
                        f"released lease {lease.lease_id!r} has no release timestamp"
                    )
                continue
            if lease.account_id not in binding_accounts:
                raise ValueError(
                    f"active lease {lease.lease_id!r} has no account binding"
                )
            prior = active_by_account.get(lease.account_id)
            if prior is not None:
                raise ValueError(
                    f"account {lease.account_id!r} has multiple active leases: "
                    f"{prior!r}, {lease.lease_id!r}"
                )
            active_by_account[lease.account_id] = lease.lease_id
            if lease.instance_id:
                prior_instance = active_instances.get(lease.instance_id)
                if prior_instance is not None:
                    raise ValueError(
                        f"instance {lease.instance_id!r} has multiple active leases: "
                        f"{prior_instance!r}, {lease.lease_id!r}"
                    )
                active_instances[lease.instance_id] = lease.lease_id

    @staticmethod
    def _migrate_v1(raw: dict[str, Any]) -> BindingsConfig:
        """Turn the abandoned persistent-box schema into cleanup leases.

        A legacy ``instance_id`` must not be silently dropped: doing so would
        leave its stopped/running EC2 and attached EIP invisible forever.  The
        migrated active lease makes Manager startup detach/terminate that old
        box before the EIP is reused.
        """
        bindings: list[AccountBinding] = []
        leases: list[AccountLease] = []
        seen: set[str] = set()
        for item in raw.get("bindings", []):
            account_id = str(item.get("account_id") or "").strip()
            if not account_id:
                raise ValueError(
                    "legacy EIP binding has no stable account_id; "
                    "restore it manually before migration"
                )
            if account_id in seen:
                raise ValueError(f"duplicate legacy account binding {account_id!r}")
            seen.add(account_id)
            allocation_id = item.get("eip_allocation_id")
            bindings.append(AccountBinding(
                account_id=account_id,
                email=str(item.get("email") or ""),
                eip_allocation_id=allocation_id,
                eip_ip=item.get("eip_ip"),
                cloud_provider=str(item.get("cloud_provider") or ""),
                cloud_account_id=str(item.get("cloud_account_id") or ""),
                region=str(item.get("region") or ""),
                state=BindingState.READY if allocation_id else BindingState.ERROR,
                error=None if allocation_id else "legacy binding has no EIP allocation",
                last_operation=None,
                created_at=float(item.get("created_at") or time.time()),
                updated_at=float(item.get("updated_at") or time.time()),
            ))
            instance_id = str(item.get("instance_id") or "").strip()
            if instance_id:
                lease_uuid = uuid.uuid5(
                    uuid.NAMESPACE_URL,
                    f"elastic-agent:v1:{account_id}:{instance_id}",
                )
                leases.append(AccountLease(
                    lease_id=f"legacy-{lease_uuid}",
                    account_id=account_id,
                    email=str(item.get("email") or ""),
                    job_id="legacy-binding-migration",
                    instance_id=instance_id,
                    worker_id=instance_id,
                    state=LeaseState.ERROR,
                    error="legacy persistent instance pending teardown",
                    last_operation="release",
                ))
        return BindingsConfig(version=2, bindings=bindings, leases=leases)

    def _find_binding(self, account_id: str) -> AccountBinding | None:
        return next(
            (b for b in self._config.bindings if b.account_id == account_id),
            None,
        )

    def _find_lease(self, lease_id: str) -> AccountLease | None:
        return next(
            (lease for lease in self._config.leases if lease.lease_id == lease_id),
            None,
        )

    @staticmethod
    def _is_active(lease: AccountLease) -> bool:
        return lease.state != LeaseState.RELEASED

    # -- Persistent EIP bindings -----------------------------------------

    async def list_bindings(self) -> list[AccountBinding]:
        async with self._lock:
            self._ensure_loaded()
            return [binding.model_copy(deep=True) for binding in self._config.bindings]

    async def get_binding(self, account_id: str) -> AccountBinding | None:
        async with self._lock:
            self._ensure_loaded()
            binding = self._find_binding(account_id)
            return binding.model_copy(deep=True) if binding else None

    async def upsert_binding(self, binding: AccountBinding) -> AccountBinding:
        async with self._lock:
            self._ensure_loaded()
            stored = binding.model_copy(deep=True)
            stored.touch()
            candidate = self._config.model_copy(deep=True)
            candidate.bindings = [
                current
                for current in candidate.bindings
                if current.account_id != stored.account_id
            ]
            candidate.bindings.append(stored)
            self._write_config_sync(candidate)
            return stored.model_copy(deep=True)

    async def update_binding(
        self, account_id: str, **fields: Any
    ) -> AccountBinding | None:
        async with self._lock:
            self._ensure_loaded()
            candidate = self._config.model_copy(deep=True)
            binding = next(
                (b for b in candidate.bindings if b.account_id == account_id), None
            )
            if binding is None:
                return None
            self._patch(binding, fields)
            self._write_config_sync(candidate)
            return binding.model_copy(deep=True)

    async def remove_binding(self, account_id: str) -> bool:
        async with self._lock:
            self._ensure_loaded()
            if any(
                lease.account_id == account_id and self._is_active(lease)
                for lease in self._config.leases
            ):
                raise LeaseConflictError(
                    f"account {account_id!r} still has an active lease"
                )
            candidate = self._config.model_copy(deep=True)
            before = len(candidate.bindings)
            candidate.bindings = [
                binding
                for binding in candidate.bindings
                if binding.account_id != account_id
            ]
            removed = len(candidate.bindings) < before
            if removed:
                self._write_config_sync(candidate)
            return removed

    # -- Durable account leases ------------------------------------------

    async def list_leases(
        self,
        *,
        account_id: str | None = None,
        active_only: bool = False,
        job_ids: Collection[str] | None = None,
        limit: int | None = None,
    ) -> list[AccountLease]:
        if limit is not None and limit < 1:
            raise ValueError("lease list limit must be positive")
        selected_jobs = frozenset(job_ids) if job_ids is not None else None
        async with self._lock:
            self._ensure_loaded()
            result: list[AccountLease] = []
            for lease in self._config.leases:
                if account_id is not None and lease.account_id != account_id:
                    continue
                if (
                    selected_jobs is not None
                    and lease.job_id not in selected_jobs
                ):
                    continue
                if active_only and not self._is_active(lease):
                    continue
                result.append(lease.model_copy(deep=True))
                if limit is not None and len(result) >= limit:
                    break
            return result

    async def get_lease(self, lease_id: str) -> AccountLease | None:
        async with self._lock:
            self._ensure_loaded()
            lease = self._find_lease(lease_id)
            return lease.model_copy(deep=True) if lease else None

    async def get_lease_by_instance(self, instance_id: str) -> AccountLease | None:
        async with self._lock:
            self._ensure_loaded()
            lease = next(
                (
                    current
                    for current in self._config.leases
                    if current.instance_id == instance_id and self._is_active(current)
                ),
                None,
            )
            return lease.model_copy(deep=True) if lease else None

    async def reserve_lease(
        self,
        account_id: str,
        *,
        email: str = "",
        job_id: str,
        slot: int = 0,
    ) -> AccountLease:
        """Atomically reserve an account for one job slot.

        Repeating the same ``account_id/job_id/slot`` request returns its
        existing active lease.  A different active owner gets a conflict.
        """

        async with self._lock:
            self._ensure_loaded()
            if self._find_binding(account_id) is None:
                raise KeyError(f"no EIP binding exists for account {account_id!r}")

            active = [
                lease
                for lease in self._config.leases
                if lease.account_id == account_id and self._is_active(lease)
            ]
            for lease in active:
                if lease.job_id == job_id and lease.slot == slot:
                    return lease.model_copy(deep=True)
            if active:
                owner = active[0]
                raise LeaseConflictError(
                    f"account {account_id!r} is leased by job {owner.job_id!r} "
                    f"slot {owner.slot} ({owner.lease_id})"
                )

            generation = 1 + max(
                (
                    lease.generation
                    for lease in self._config.leases
                    if lease.account_id == account_id
                ),
                default=0,
            )
            lease = AccountLease(
                account_id=account_id,
                email=email,
                job_id=job_id,
                slot=slot,
                generation=generation,
            )
            candidate = self._config.model_copy(deep=True)
            candidate.leases.append(lease)
            self._write_config_sync(candidate)
            return lease.model_copy(deep=True)

    async def begin_attach(
        self,
        lease_id: str,
        instance_id: str,
        worker_id: str = "",
    ) -> AccountLease:
        """Atomically claim an instance and persist the start of EIP attach."""

        async with self._lock:
            self._ensure_loaded()
            candidate = self._config.model_copy(deep=True)
            lease = next(
                (current for current in candidate.leases if current.lease_id == lease_id),
                None,
            )
            if lease is None:
                raise KeyError(f"unknown account lease {lease_id!r}")
            if lease.state == LeaseState.RELEASED:
                raise ValueError(f"lease {lease_id!r} has already been released")
            if lease.state == LeaseState.RELEASING or (
                lease.state == LeaseState.ERROR and lease.last_operation == "release"
            ):
                raise ValueError(f"lease {lease_id!r} is being released")
            if lease.instance_id and lease.instance_id != instance_id:
                raise LeaseConflictError(
                    f"lease {lease_id!r} already claims instance {lease.instance_id!r}"
                )
            for other in candidate.leases:
                if (
                    other.lease_id != lease_id
                    and self._is_active(other)
                    and other.instance_id == instance_id
                ):
                    raise LeaseConflictError(
                        f"instance {instance_id!r} is already claimed by "
                        f"lease {other.lease_id!r}"
                    )

            # ATTACHED + same instance is an idempotent fast path.  A newly
            # learned worker id can still be recorded.
            if lease.state == LeaseState.ATTACHED:
                if worker_id and worker_id != lease.worker_id:
                    lease.worker_id = worker_id
                    lease.touch()
                    self._write_config_sync(candidate)
                return lease.model_copy(deep=True)

            lease.instance_id = instance_id
            if worker_id:
                lease.worker_id = worker_id
            lease.launch_uncertain = False
            lease.state = LeaseState.ATTACHING
            lease.last_operation = "attach"
            lease.error = None
            lease.touch()
            self._write_config_sync(candidate)
            return lease.model_copy(deep=True)

    async def begin_release(
        self,
        lease_id: str,
        *,
        cleanup_worker_required: bool = False,
        expected_lease: AccountLease | None = None,
    ) -> AccountLease | None:
        """Atomically validate lease identity and enter ``RELEASING``.

        Identity-changing attach/recovery writes use this same store lock.  A
        caller's snapshot therefore cannot be validated and then replaced by a
        different instance before the release intent is durably committed.
        """

        async with self._lock:
            self._ensure_loaded()
            candidate = self._config.model_copy(deep=True)
            lease = next(
                (
                    current
                    for current in candidate.leases
                    if current.lease_id == lease_id
                ),
                None,
            )
            if lease is None:
                return None
            if expected_lease is not None:
                identity_fields = (
                    "lease_id",
                    "account_id",
                    "job_id",
                    "slot",
                    "generation",
                    "instance_id",
                    "worker_id",
                )
                mismatched = [
                    field
                    for field in identity_fields
                    if getattr(lease, field) != getattr(expected_lease, field)
                ]
                if mismatched:
                    raise LeaseConflictError(
                        f"lease {lease_id!r} changed identity before release: "
                        + ", ".join(mismatched)
                    )
            if lease.state == LeaseState.RELEASED:
                return lease.model_copy(deep=True)
            if lease.worker_id and not lease.instance_id:
                raise RuntimeError(
                    f"lease {lease_id!r} identifies worker "
                    f"{lease.worker_id!r} but has no durable instance id"
                )
            if lease.launch_uncertain and not lease.instance_id:
                raise RuntimeError(
                    f"lease {lease_id!r} has an unresolved instance launch; "
                    "bounded cloud-tag recovery must finish before release"
                )

            lease.state = LeaseState.RELEASING
            lease.last_operation = "release"
            lease.error = None
            if cleanup_worker_required:
                lease.worker_cleanup_required = True
            lease.touch()
            self._write_config_sync(candidate)
            return lease.model_copy(deep=True)

    async def update_lease(
        self, lease_id: str, **fields: Any
    ) -> AccountLease | None:
        async with self._lock:
            self._ensure_loaded()
            candidate = self._config.model_copy(deep=True)
            lease = next(
                (current for current in candidate.leases if current.lease_id == lease_id),
                None,
            )
            if lease is None:
                return None
            release_started = lease.state in {
                LeaseState.RELEASING,
                LeaseState.RELEASED,
            } or (
                lease.state == LeaseState.ERROR
                and lease.last_operation == "release"
            )
            identity_fields = {
                "lease_id",
                "account_id",
                "job_id",
                "slot",
                "generation",
                "instance_id",
                "worker_id",
            }
            changed_identity = [
                field
                for field in identity_fields.intersection(fields)
                if fields[field] != getattr(lease, field)
            ]
            if release_started and changed_identity:
                raise LeaseConflictError(
                    f"lease {lease_id!r} identity is frozen after release "
                    "intent: " + ", ".join(sorted(changed_identity))
                )
            self._patch(lease, fields)
            self._write_config_sync(candidate)
            return lease.model_copy(deep=True)

    @staticmethod
    def _patch(model: AccountBinding | AccountLease, fields: dict[str, Any]) -> None:
        for key, value in fields.items():
            if key in {"account_id", "lease_id", "created_at"}:
                raise ValueError(f"field {key!r} is immutable")
            if key not in type(model).model_fields:
                raise ValueError(f"unknown field {key!r}")
            setattr(model, key, value)
        model.touch()
