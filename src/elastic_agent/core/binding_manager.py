"""Lifecycle manager for persistent account EIPs and ephemeral job instances.

An account owns one Elastic IP, not one EC2 instance.  ``reserve`` claims the
account for a job slot, ``attach_instance`` connects its EIP to the job's newly
created instance, and ``release`` cleans the worker, detaches the EIP, and
terminates the instance.  Only the EIP binding survives between jobs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from typing import AsyncIterator, Awaitable, Callable

from elastic_agent.core.account_binding import (
    AccountBinding,
    AccountBindingStore,
    AccountLease,
    BindingState,
    LeaseConflictError,
    LeaseState,
)
from elastic_agent.core.providers.base import (
    CloudIdentity,
    CloudProvider,
    ElasticIp,
    InstanceNotFoundError,
    InstanceState,
)

logger = logging.getLogger(__name__)

CleanupWorkerHook = Callable[[AccountLease], Awaitable[None]]
RecoveryNotifier = Callable[[str], None]
# AllocateAddress has no idempotency token.  A durable ALLOCATING/ERROR record
# may mean AWS succeeded just before a Manager crash, so quarantine that retry
# for the full eventual-consistency window before creating another billable IP.
EIP_ADOPTION_RETRY_ATTEMPTS = 30
EIP_ADOPTION_RETRY_SECONDS = 10.0
# A second controller can pass the initial list just before this controller's
# AllocateAddress.  Do not publish the new binding until repeated broad tag
# scans agree that our allocation is the only one for the account.
EIP_ALLOCATION_CONVERGENCE_ATTEMPTS = 30
EIP_ALLOCATION_CONVERGENCE_SECONDS = 10.0
TEARDOWN_CONFIRM_ATTEMPTS = 60
TEARDOWN_CONFIRM_SECONDS = 5.0
EIP_ABSENCE_CONFIRM_SCANS = 3

EIP_ROLE = "account-eip"
LEGACY_EIP_ROLE = "codex-account-box"


class BindingManager:
    """Coordinate cloud operations with durable bindings and leases."""

    def __init__(
        self,
        provider: CloudProvider,
        store: AccountBindingStore,
        *,
        recovery_notifier: RecoveryNotifier | None = None,
    ) -> None:
        self._provider = provider
        self._store = store
        self._recovery_notifier = recovery_notifier
        self._account_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._lease_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def _account_lock(self, account_id: str) -> asyncio.Lock:
        return self._account_locks[account_id]

    def _lease_lock(self, lease_id: str) -> asyncio.Lock:
        return self._lease_locks[lease_id]

    @asynccontextmanager
    async def account_transaction(
        self, account_id: str
    ) -> AsyncIterator[None]:
        """Serialize identity CRUD checks with binding/lease lifecycle work.

        External callers must not invoke ``ensure_binding``, ``reserve``, or
        ``decommission`` from inside this context because those operations
        acquire the same non-reentrant account lock.  It is intended for the
        account API's read-check-mutate identity transactions.
        """
        if not account_id:
            raise ValueError("account_id is required for an account transaction")
        async with self._account_lock(account_id):
            yield

    async def _list_account_eips(
        self, account_id: str, *, include_legacy_unscoped: bool = False
    ) -> list[ElasticIp]:
        strict_tags = {
            "AccountId": account_id,
            "Role": EIP_ROLE,
            "ElasticAgentController": self._store.controller_id,
        }
        tagged = await self._provider.list_eips(filters=strict_tags)
        if tagged or not include_legacy_unscoped:
            return tagged
        legacy = await self._provider.list_eips(filters={
            "AccountId": account_id,
            "Role": EIP_ROLE,
        })
        # A durable pre-controller intent may adopt an unscoped address once.
        # Explicitly exclude any address already owned by another controller.
        return [
            eip
            for eip in legacy
            if eip.tags.get("ElasticAgentController", "")
            in ("", self._store.controller_id)
        ]

    async def _list_matching_account_eips(
        self, account_id: str, *, email: str = ""
    ) -> list[ElasticIp]:
        """List every current or v1 managed EIP claiming this account."""
        matches = await self._provider.list_eips(filters={
            "AccountId": account_id,
            "Role": EIP_ROLE,
        })
        if email:
            matches.extend(await self._provider.list_eips(filters={
                "account": email,
                "role": LEGACY_EIP_ROLE,
            }))
        # A retagged v1 allocation matches both queries until its old tags are
        # removed.  Deduplicate by the provider's immutable allocation handle.
        return list({eip.allocation_id: eip for eip in matches}.values())

    @staticmethod
    def _error_text(exc: BaseException) -> str:
        return str(exc) or type(exc).__name__

    def _candidate_is_locally_owned(
        self, binding: AccountBinding, eip: ElasticIp
    ) -> bool:
        tags = eip.tags
        if tags.get(self._provider.MANAGED_TAG_KEY) != (
            self._provider.MANAGED_TAG_VALUE
        ):
            return False
        controller = tags.get("ElasticAgentController", "")
        if (
            tags.get("AccountId") == binding.account_id
            and tags.get("Role") == EIP_ROLE
        ):
            if binding.controller_id:
                return controller == binding.controller_id
            return controller in ("", self._store.controller_id)
        return (
            not binding.controller_id
            and bool(binding.email)
            and tags.get("account") == binding.email
            and tags.get("role") == LEGACY_EIP_ROLE
            and not controller
        )

    def _scope_is_durably_verified(
        self, binding: AccountBinding, identity: CloudIdentity
    ) -> bool:
        """Whether absence/NotFound is meaningful in the current cloud scope."""
        if binding.controller_id != self._store.controller_id:
            return False
        # Every EIP-capable deployment must persist at least provider and
        # provider-account identity.  Region is additionally mandatory when
        # the provider reports one (AWS always does).
        if not binding.cloud_provider or not binding.cloud_account_id:
            return False
        if identity.provider and binding.cloud_provider != identity.provider:
            return False
        if identity.account_id and binding.cloud_account_id != identity.account_id:
            return False
        if identity.region and binding.region != identity.region:
            return False
        return True

    async def verify_eip_ownership(
        self,
        binding: AccountBinding,
        eip: ElasticIp,
        *,
        _allow_retag: bool = True,
    ) -> tuple[AccountBinding, ElasticIp]:
        """Prove that an observed EIP belongs to ``binding``.

        This guard is intentionally public for Manager recovery paths that
        must detach an EIP without going through a normal lease operation.
        Legacy v1 bindings are accepted only by their exact historical
        ``account=<email>, role=codex-account-box`` tags and are immediately
        retagged into the controller-scoped schema before use or cleanup.
        """
        if (
            binding.eip_allocation_id
            and eip.allocation_id != binding.eip_allocation_id
        ):
            raise LeaseConflictError(
                f"EIP lookup returned {eip.allocation_id!r}, expected "
                f"{binding.eip_allocation_id!r}"
            )
        tags = eip.tags
        if tags.get(self._provider.MANAGED_TAG_KEY) != (
            self._provider.MANAGED_TAG_VALUE
        ):
            raise LeaseConflictError(
                f"EIP {eip.allocation_id!r} is not managed by elastic-agent"
            )

        controller = tags.get("ElasticAgentController", "")
        current_tags = (
            tags.get("AccountId") == binding.account_id
            and tags.get("Role") == EIP_ROLE
        )
        legacy_tags = (
            not binding.controller_id
            and bool(binding.email)
            and tags.get("account") == binding.email
            and tags.get("role") == LEGACY_EIP_ROLE
            and not controller
        )

        if current_tags:
            expected_controller = (
                binding.controller_id or self._store.controller_id
            )
            if controller not in (expected_controller, "") or (
                binding.controller_id and not controller
            ):
                raise LeaseConflictError(
                    f"EIP {eip.allocation_id!r} belongs to controller "
                    f"{controller!r}, not {expected_controller!r}"
                )
            needs_retag = not controller
        elif legacy_tags:
            needs_retag = True
        else:
            raise LeaseConflictError(
                f"EIP {eip.allocation_id!r} ownership tags do not match "
                f"account {binding.account_id!r}"
            )

        if needs_retag:
            if not _allow_retag:
                raise LeaseConflictError(
                    f"EIP {eip.allocation_id!r} controller tags did not persist"
                )
            await self._provider.tag_eip(eip.allocation_id, {
                "AccountId": binding.account_id,
                "Role": EIP_ROLE,
                "ElasticAgentController": self._store.controller_id,
            })
            observed = await self._provider.describe_eip(eip.allocation_id)
            if observed is None:
                raise LeaseConflictError(
                    f"EIP {eip.allocation_id!r} disappeared while retagging"
                )
            # Verify the cloud read-back rather than trusting CreateTags.
            return await self.verify_eip_ownership(
                binding, observed, _allow_retag=False
            )

        if controller != self._store.controller_id:
            raise LeaseConflictError(
                f"EIP {eip.allocation_id!r} belongs to controller "
                f"{controller!r}, not {self._store.controller_id!r}"
            )
        if not binding.controller_id:
            adopted = await self._store.update_binding(
                binding.account_id,
                controller_id=self._store.controller_id,
            )
            assert adopted is not None
            binding = adopted
        return binding, eip

    async def verify_binding_eip(
        self, binding: AccountBinding, eip: ElasticIp
    ) -> tuple[AccountBinding, ElasticIp]:
        """Verify both durable cloud scope and the EIP's actual ownership tags.

        Manager recovery code should use this combined public guard before an
        orphan detach.  Seeing and validating the exact allocation in the
        current provider scope is also the only safe way to fill missing v1
        provider/account/region snapshots.
        """
        identity = await self._provider.get_identity()
        self._assert_scope(binding, identity)
        binding, eip = await self.verify_eip_ownership(binding, eip)
        changes: dict[str, object] = {}
        if identity.provider and not binding.cloud_provider:
            changes["cloud_provider"] = identity.provider
        if identity.account_id and not binding.cloud_account_id:
            changes["cloud_account_id"] = identity.account_id
        if identity.region and not binding.region:
            changes["region"] = identity.region
        if changes:
            updated = await self._store.update_binding(
                binding.account_id, **changes
            )
            assert updated is not None
            binding = updated
        return binding, eip

    async def _assert_no_other_account_eips(
        self, binding: AccountBinding, allocation_id: str
    ) -> None:
        matches = await self._list_matching_account_eips(
            binding.account_id, email=binding.email
        )
        others = [
            eip for eip in matches if eip.allocation_id != allocation_id
        ]
        if others:
            raise LeaseConflictError(
                f"found {len(others)} other managed EIP(s) for account "
                f"{binding.account_id!r}; refusing ambiguous binding"
            )

    async def _converge_new_allocation(
        self, binding: AccountBinding, allocation_id: str
    ) -> tuple[ElasticIp, list[ElasticIp]]:
        """Wait for broad tag scans to converge after AllocateAddress.

        Returns the observed allocation and any competing allocations.  The
        caller owns compensation because it must durably distinguish a
        successfully released attempt from an ambiguous one.
        """
        observed: ElasticIp | None = None
        # First wait until our own allocation reaches the broad tag index.
        for attempt in range(EIP_ALLOCATION_CONVERGENCE_ATTEMPTS):
            matches = await self._list_matching_account_eips(
                binding.account_id, email=binding.email
            )
            own = [
                eip for eip in matches if eip.allocation_id == allocation_id
            ]
            if own:
                binding, observed = await self.verify_eip_ownership(
                    binding, own[0]
                )
                conflicts = [
                    eip
                    for eip in matches
                    if eip.allocation_id != allocation_id
                ]
                if conflicts:
                    return observed, conflicts
                break
            else:
                # Our allocation has not reached DescribeAddresses yet.  A
                # foreign result is not proof that our request failed.
                observed = None
            if attempt + 1 < EIP_ALLOCATION_CONVERGENCE_ATTEMPTS:
                await asyncio.sleep(EIP_ALLOCATION_CONVERGENCE_SECONDS)
        if observed is None:
            raise RuntimeError(
                f"EIP {allocation_id!r} did not converge in cloud tag listings"
            )

        # A couple of clean reads are not enough: another controller's EIP can
        # remain hidden by eventual consistency longer than ours.  Observe the
        # entire bounded propagation window after our first visible read.
        for scan in range(1, EIP_ALLOCATION_CONVERGENCE_ATTEMPTS):
            await asyncio.sleep(EIP_ALLOCATION_CONVERGENCE_SECONDS)
            matches = await self._list_matching_account_eips(
                binding.account_id, email=binding.email
            )
            own = [
                eip for eip in matches if eip.allocation_id == allocation_id
            ]
            if not own:
                raise RuntimeError(
                    f"EIP {allocation_id!r} disappeared from convergence scan "
                    f"{scan + 1}"
                )
            binding, observed = await self.verify_eip_ownership(binding, own[0])
            conflicts = [
                eip for eip in matches if eip.allocation_id != allocation_id
            ]
            if conflicts:
                return observed, conflicts
        return observed, []

    async def _compensate_new_allocation(
        self, binding: AccountBinding, allocation_id: str
    ) -> None:
        """Release only the exact, freshly-created, ownership-verified EIP."""
        observed = await self._provider.describe_eip(allocation_id)
        if observed is None:
            raise RuntimeError(
                f"cannot verify newly allocated EIP {allocation_id!r} for cleanup"
            )
        _binding, observed = await self.verify_eip_ownership(binding, observed)
        if observed.instance_id:
            raise LeaseConflictError(
                f"new EIP {allocation_id!r} unexpectedly attached to "
                f"{observed.instance_id!r}; refusing compensation"
            )
        await self._provider.release_eip(allocation_id)
        await self._wait_eip_released(allocation_id)

    async def _settle_recovery_conflict(
        self,
        binding: AccountBinding,
        candidates: list[ElasticIp],
    ) -> None:
        local = [
            candidate
            for candidate in candidates
            if self._candidate_is_locally_owned(binding, candidate)
        ]
        foreign_count = len(candidates) - len(local)
        for candidate in local:
            await self._compensate_new_allocation(
                binding, candidate.allocation_id
            )
        await self._store.update_binding(
            binding.account_id,
            state=BindingState.ERROR,
            error=(
                f"allocation recovery found {len(local)} local and "
                f"{foreign_count} foreign EIP(s); local allocations released"
            ),
            last_operation="ensure",
        )

    async def _wait_eip_detached(
        self, binding: AccountBinding, expected_instance_id: str | None
    ) -> tuple[AccountBinding, ElasticIp]:
        """Confirm the persistent EIP is visible, owned, and detached."""
        assert binding.eip_allocation_id
        for attempt in range(TEARDOWN_CONFIRM_ATTEMPTS):
            observed = await self._provider.describe_eip(
                binding.eip_allocation_id
            )
            if observed is None:
                raise RuntimeError(
                    f"persistent EIP {binding.eip_allocation_id!r} disappeared"
                )
            binding, observed = await self.verify_eip_ownership(
                binding, observed
            )
            if observed.instance_id is None:
                return binding, observed
            if (
                expected_instance_id
                and observed.instance_id != expected_instance_id
            ):
                raise LeaseConflictError(
                    f"EIP {binding.eip_allocation_id!r} is attached to foreign "
                    f"instance {observed.instance_id!r}; refusing detach"
                )
            if attempt + 1 < TEARDOWN_CONFIRM_ATTEMPTS:
                await asyncio.sleep(TEARDOWN_CONFIRM_SECONDS)
        raise RuntimeError(
            f"EIP {binding.eip_allocation_id!r} did not become detached"
        )

    async def _wait_instance_terminated(self, instance_id: str) -> None:
        """Confirm TerminateInstances reached a terminal cloud state."""
        for attempt in range(TEARDOWN_CONFIRM_ATTEMPTS):
            try:
                instance = await self._provider.get_instance(instance_id)
            except InstanceNotFoundError:
                return
            if instance is None:
                raise RuntimeError(
                    f"provider returned no state for instance {instance_id!r}"
                )
            if instance.state == InstanceState.TERMINATED:
                return
            if attempt + 1 < TEARDOWN_CONFIRM_ATTEMPTS:
                await asyncio.sleep(TEARDOWN_CONFIRM_SECONDS)
        raise RuntimeError(
            f"instance {instance_id!r} did not reach terminated state"
        )

    async def _wait_eip_released(self, allocation_id: str) -> None:
        missing_scans = 0
        for attempt in range(TEARDOWN_CONFIRM_ATTEMPTS):
            if await self._provider.describe_eip(allocation_id) is None:
                missing_scans += 1
                if missing_scans >= EIP_ABSENCE_CONFIRM_SCANS:
                    return
            else:
                missing_scans = 0
            if attempt + 1 < TEARDOWN_CONFIRM_ATTEMPTS:
                await asyncio.sleep(TEARDOWN_CONFIRM_SECONDS)
        raise RuntimeError(f"EIP {allocation_id!r} did not disappear after release")

    async def _observe_eip_for_decommission(
        self, allocation_id: str
    ) -> ElasticIp | None:
        """Return a visible EIP, or None only after stable absence scans."""
        missing_scans = 0
        for attempt in range(TEARDOWN_CONFIRM_ATTEMPTS):
            observed = await self._provider.describe_eip(allocation_id)
            if observed is not None:
                return observed
            missing_scans += 1
            if missing_scans >= EIP_ABSENCE_CONFIRM_SCANS:
                return None
            if attempt + 1 < TEARDOWN_CONFIRM_ATTEMPTS:
                await asyncio.sleep(TEARDOWN_CONFIRM_SECONDS)
        raise RuntimeError(
            f"could not confirm whether EIP {allocation_id!r} exists"
        )

    def _assert_scope(self, binding: AccountBinding, identity: CloudIdentity) -> None:
        """Refuse cloud cleanup/use through changed credentials or region."""
        mismatches: list[str] = []
        if (
            binding.cloud_provider
            and identity.provider
            and binding.cloud_provider != identity.provider
        ):
            mismatches.append(
                f"provider {binding.cloud_provider!r} != {identity.provider!r}"
            )
        if (
            binding.cloud_account_id
            and identity.account_id
            and binding.cloud_account_id != identity.account_id
        ):
            mismatches.append(
                f"cloud account {binding.cloud_account_id!r} != {identity.account_id!r}"
            )
        if binding.region and identity.region and binding.region != identity.region:
            mismatches.append(f"region {binding.region!r} != {identity.region!r}")
        if (
            binding.controller_id
            and binding.controller_id != self._store.controller_id
        ):
            mismatches.append(
                f"controller {binding.controller_id!r} != "
                f"{self._store.controller_id!r}"
            )
        if mismatches:
            raise LeaseConflictError(
                f"account {binding.account_id!r} binding scope mismatch: "
                + ", ".join(mismatches)
            )

    async def _verify_and_adopt_scope(
        self, binding: AccountBinding, identity: CloudIdentity
    ) -> AccountBinding:
        """Verify cloud scope before any destructive operation.

        Controller adoption is deliberately deferred to
        :meth:`verify_eip_ownership`, where the cloud resource's actual tags
        can be checked (and a genuine v1 allocation safely retagged).
        """
        self._assert_scope(binding, identity)
        changes: dict[str, object] = {}
        missing_scope = (
            (identity.provider and not binding.cloud_provider)
            or (identity.account_id and not binding.cloud_account_id)
            or (identity.region and not binding.region)
            or not binding.controller_id
        )
        if missing_scope:
            # Any missing scope component can turn a wrong-account/region
            # NotFound into false cleanup evidence.  Adopt all missing fields
            # together only after the exact EIP is visible and ownership-tag
            # verified in the current provider scope.
            if not binding.eip_allocation_id:
                raise LeaseConflictError(
                    f"account {binding.account_id!r} binding has no verifiable EIP"
                )
            visible = await self._provider.describe_eip(binding.eip_allocation_id)
            if visible is None:
                raise LeaseConflictError(
                    f"cannot verify cloud owner for EIP {binding.eip_allocation_id!r}; "
                    "refusing cleanup"
                )
            binding, _ = await self.verify_eip_ownership(binding, visible)
        if identity.provider and not binding.cloud_provider:
            changes["cloud_provider"] = identity.provider
        if identity.account_id and not binding.cloud_account_id:
            changes["cloud_account_id"] = identity.account_id
        if identity.region and not binding.region:
            changes["region"] = identity.region
        if changes:
            adopted = await self._store.update_binding(
                binding.account_id, **changes
            )
            assert adopted is not None
            return adopted
        return binding

    # -- Read API ---------------------------------------------------------

    async def get_binding(self, account_id: str) -> AccountBinding | None:
        return await self._store.get_binding(account_id)

    async def list_bindings(self) -> list[AccountBinding]:
        return await self._store.list_bindings()

    async def get_lease(self, lease_id: str) -> AccountLease | None:
        return await self._store.get_lease(lease_id)

    async def list_leases(
        self,
        *,
        account_id: str | None = None,
        active_only: bool = False,
    ) -> list[AccountLease]:
        return await self._store.list_leases(
            account_id=account_id, active_only=active_only
        )

    # -- Stable account -> EIP resource ---------------------------------

    async def ensure_binding(
        self,
        account_id: str,
        *,
        email: str = "",
        region: str = "",
    ) -> AccountBinding:
        """Return the account's EIP binding, allocating it on first use.

        Allocation errors are durable and retryable.  A binding's region is
        immutable once known because AWS EIPs cannot move between regions.
        """

        if not account_id:
            raise ValueError("account_id is required for an EIP binding")
        async with self._account_lock(account_id):
            return await self._ensure_binding_locked(
                account_id, email=email, region=region
            )

    async def _ensure_binding_locked(
        self,
        account_id: str,
        *,
        email: str,
        region: str,
    ) -> AccountBinding:
        identity = await self._provider.get_identity()
        if region and identity.region and region != identity.region:
            raise ValueError(
                f"requested region {region!r} does not match provider region "
                f"{identity.region!r}"
            )
        binding = await self._store.get_binding(account_id)
        legacy_controller = binding is not None and not binding.controller_id
        recovering_allocation = (
            binding is not None and binding.last_operation == "allocate_eip"
        )
        if binding is not None:
            self._assert_scope(binding, identity)
            if (
                email
                and binding.email
                and email.casefold() != binding.email.casefold()
            ):
                raise LeaseConflictError(
                    f"account {account_id!r} EIP is bound to email "
                    f"{binding.email!r}, not {email!r}"
                )
            if binding.region and region and binding.region != region:
                raise ValueError(
                    f"account {account_id!r} is bound to region "
                    f"{binding.region!r}, not {region!r}"
                )
            changes: dict[str, object] = {}
            if region and not binding.region:
                changes["region"] = region

            # A persisted retirement request is an administrative intent, not
            # a transient allocation error.  Never turn it back into READY just
            # because the allocation still exists; only decommission() (or
            # startup recovery calling it) may complete that transaction.
            if binding.last_operation == "decommission" or (
                binding.state == BindingState.DECOMMISSIONING
            ):
                raise LeaseConflictError(
                    f"account {account_id!r} EIP decommission is pending"
                )

            # An allocation id is the durable fact for normal ensure/allocation
            # retries.  If it remains, refresh its display data in place.
            if binding.eip_allocation_id:
                existing = await self._provider.describe_eip(
                    binding.eip_allocation_id
                )
                if existing is not None:
                    binding, existing = await self.verify_eip_ownership(
                        binding, existing
                    )
                    conflict_binding = binding
                    if email and not binding.email:
                        conflict_binding = binding.model_copy(
                            update={"email": email}
                        )
                    await self._assert_no_other_account_eips(
                        conflict_binding, existing.allocation_id
                    )
                    # Filling a v1 empty display snapshot is allowed only
                    # after the cloud allocation's ownership was proven.
                    if email and not binding.email:
                        changes["email"] = email
                    changes.update(
                        eip_ip=existing.public_ip,
                        state=BindingState.READY,
                        error=None,
                        last_operation="ensure",
                    )
                    if identity.provider and not binding.cloud_provider:
                        changes["cloud_provider"] = identity.provider
                    if identity.account_id and not binding.cloud_account_id:
                        changes["cloud_account_id"] = identity.account_id
                    if identity.region and not binding.region:
                        changes["region"] = identity.region
                    if changes:
                        binding = await self._store.update_binding(
                            account_id, **changes
                        )
                    return binding
                # Never silently replace a missing address: the fixed public IP
                # is the account identity this feature promises.  Preserve the
                # allocation id for forensics and require an explicit
                # decommission + rebind decision from an administrator.
                await self._store.update_binding(
                    account_id,
                    state=BindingState.ERROR,
                    error=(
                        f"recorded EIP allocation {binding.eip_allocation_id!r} "
                        "no longer exists; decommission and rebind explicitly"
                    ),
                )
                raise LeaseConflictError(
                    f"account {account_id!r} lost its recorded EIP "
                    f"{binding.eip_allocation_id!r}; refusing automatic replacement"
                )
        else:
            binding = AccountBinding(
                account_id=account_id,
                email=email,
                cloud_provider=identity.provider,
                cloud_account_id=identity.account_id,
                region=region or identity.region,
                controller_id=self._store.controller_id,
                state=BindingState.ALLOCATING,
                last_operation="ensure",
            )
            binding = await self._store.upsert_binding(binding)

        # Existing ERROR/ALLOCATING records without an allocation are retried
        # in place.  Persist identity snapshots before the external API call.
        retry_changes: dict[str, object] = {
            "state": BindingState.ALLOCATING,
            "error": None,
            # Persist the external-call intent before AllocateAddress.  If the
            # process dies after AWS succeeds but before recording its id, the
            # next call knows it must quarantine/adopt rather than allocate.
            "last_operation": "allocate_eip",
        }
        if region:
            retry_changes["region"] = region
        elif identity.region and not binding.region:
            retry_changes["region"] = identity.region
        if identity.provider and not binding.cloud_provider:
            retry_changes["cloud_provider"] = identity.provider
        if identity.account_id and not binding.cloud_account_id:
            retry_changes["cloud_account_id"] = identity.account_id
        binding = await self._store.update_binding(account_id, **retry_changes)
        assert binding is not None

        tags = {
            "AccountId": account_id,
            "Role": "account-eip",
            "ElasticAgentController": self._store.controller_id,
        }
        allocation_attempted = False
        try:
            # AllocateAddress has no idempotency token.  Tags are applied in
            # the allocation call, so after a Manager crash between AWS success
            # and the local write, the next attempt adopts that orphan instead
            # of allocating (and billing for) another address.
            tagged = []
            attempts = EIP_ADOPTION_RETRY_ATTEMPTS if recovering_allocation else 1
            for attempt in range(attempts):
                tagged = await self._list_account_eips(
                    account_id,
                    include_legacy_unscoped=legacy_controller,
                )
                if tagged or attempt + 1 >= attempts:
                    break
                # AllocateAddress has no client token and DescribeAddresses is
                # eventually consistent.  A pre-existing ALLOCATING/ERROR
                # record may mean AWS succeeded immediately before a crash, so
                # give its tagged address time to appear before allocating a
                # second billable EIP.
                await asyncio.sleep(EIP_ADOPTION_RETRY_SECONDS)
            if len(tagged) > 1:
                if recovering_allocation:
                    matches = await self._list_matching_account_eips(
                        account_id, email=binding.email or email
                    )
                    await self._settle_recovery_conflict(binding, matches)
                raise LeaseConflictError(
                    f"found {len(tagged)} managed EIPs for account {account_id!r}; "
                    "refusing to choose one automatically"
                )
            if tagged:
                # The cloud resource is now a known fact but its allocation id
                # is not durable yet.  A following fsync failure needs the same
                # adoption marker/notifier as an AllocateAddress timeout.
                allocation_attempted = True
                binding, eip = await self.verify_eip_ownership(
                    binding, tagged[0]
                )
                conflict_binding = binding
                if email and not binding.email:
                    conflict_binding = binding.model_copy(
                        update={"email": email}
                    )
                await self._assert_no_other_account_eips(
                    conflict_binding, eip.allocation_id
                )
            else:
                # A pre-controller/foreign deployment may already own an EIP
                # with the same account tag.  Without a durable local legacy
                # intent we cannot prove ownership, but allocating another
                # address would silently leak cost and split account identity.
                conflicts = await self._list_matching_account_eips(
                    account_id, email=binding.email or email
                )
                if conflicts:
                    raise LeaseConflictError(
                        f"found {len(conflicts)} unscoped/foreign EIP(s) for "
                        f"account {account_id!r}; refusing duplicate allocation"
                    )
                # From this point through the durable allocation-id write, any
                # exception is ambiguous: AWS may have accepted the request even
                # when the SDK reports a timeout.  Keep the adoption quarantine
                # marker until the allocation id is safely fsynced.
                allocation_attempted = True
                eip = await self._provider.allocate_eip(tags=tags)
                operation_binding = binding
                if email and not binding.email:
                    operation_binding = binding.model_copy(
                        update={"email": email}
                    )
                eip, conflicts = await self._converge_new_allocation(
                    operation_binding, eip.allocation_id
                )
                if conflicts:
                    await self._compensate_new_allocation(
                        binding, eip.allocation_id
                    )
                    # The exact allocation made by this call is now confirmed
                    # released.  Do not leave an allocation-recovery marker
                    # which could later adopt the competing controller's EIP.
                    allocation_attempted = False
                    raise LeaseConflictError(
                        f"concurrent controller allocated {len(conflicts)} "
                        f"competing EIP(s) for account {account_id!r}; "
                        "released this controller's allocation"
                    )
            ready = await self._store.update_binding(
                account_id,
                eip_allocation_id=eip.allocation_id,
                eip_ip=eip.public_ip,
                state=BindingState.READY,
                error=None,
                last_operation="ensure",
                cloud_provider=identity.provider or binding.cloud_provider,
                cloud_account_id=identity.account_id or binding.cloud_account_id,
                controller_id=eip.tags["ElasticAgentController"],
                email=binding.email or email,
            )
            assert ready is not None
            logger.info(
                "Bound account %s to EIP %s (%s)",
                account_id,
                eip.public_ip,
                eip.allocation_id,
            )
            return ready
        except BaseException as exc:  # noqa: BLE001
            try:
                await self._store.update_binding(
                    account_id,
                    state=BindingState.ERROR,
                    error=self._error_text(exc),
                    last_operation=(
                        "allocate_eip" if allocation_attempted else "ensure"
                    ),
                )
            except BaseException:  # noqa: BLE001
                logger.exception(
                    "Could not persist failed EIP allocation for account %s",
                    account_id,
                )
            finally:
                if allocation_attempted and self._recovery_notifier is not None:
                    try:
                        self._recovery_notifier(account_id)
                    except Exception:  # noqa: BLE001
                        logger.exception(
                            "EIP allocation recovery notifier failed for %s",
                            account_id,
                        )
            if isinstance(exc, Exception):
                logger.exception(
                    "Failed to allocate EIP for account %s", account_id
                )
            raise

    async def recover_pending_allocation(self, account_id: str) -> bool:
        """Adopt one tagged EIP after an ambiguous AllocateAddress result.

        Recovery performs the same repeated broad-tag convergence as a normal
        allocation before it publishes READY.  ``True`` means the intent is
        settled (adopted or compensated), ``False`` means this controller's
        tagged allocation is not visible yet.
        """
        async with self._account_lock(account_id):
            binding = await self._store.get_binding(account_id)
            if binding is None or binding.last_operation != "allocate_eip":
                return True
            self._assert_scope(binding, await self._provider.get_identity())
            matches = await self._list_matching_account_eips(
                account_id, email=binding.email
            )
            owned = [
                eip
                for eip in matches
                if self._candidate_is_locally_owned(binding, eip)
            ]
            foreign = [
                eip
                for eip in matches
                if eip.allocation_id
                not in {candidate.allocation_id for candidate in owned}
            ]
            if not owned:
                return False
            if len(owned) > 1 or foreign:
                # No allocation id was durably selected.  Releasing every EIP
                # that is provably ours is safer than choosing one while a
                # concurrent controller also claims the account.
                await self._settle_recovery_conflict(
                    binding, [*owned, *foreign]
                )
                return True
            observed, conflicts = await self._converge_new_allocation(
                binding, owned[0].allocation_id
            )
            if conflicts:
                await self._settle_recovery_conflict(
                    binding, [observed, *conflicts]
                )
                return True
            identity = await self._provider.get_identity()
            binding, observed = await self.verify_eip_ownership(binding, observed)
            adopted = await self._store.update_binding(
                account_id,
                eip_allocation_id=observed.allocation_id,
                eip_ip=observed.public_ip,
                state=BindingState.READY,
                error=None,
                last_operation="ensure",
                cloud_provider=identity.provider or binding.cloud_provider,
                cloud_account_id=identity.account_id or binding.cloud_account_id,
                region=identity.region or binding.region,
                controller_id=binding.controller_id,
            )
            assert adopted is not None
            return True

    # -- Exclusive per-job claim -----------------------------------------

    async def reserve(
        self,
        account_id: str,
        *,
        email: str = "",
        job_id: str,
        slot: int = 0,
        region: str = "",
    ) -> AccountLease:
        """Ensure the EIP and atomically reserve the account for a job slot."""

        if not job_id:
            raise ValueError("job_id is required for an account lease")
        async with self._account_lock(account_id):
            binding = await self._ensure_binding_locked(
                account_id, email=email, region=region
            )
            return await self._store.reserve_lease(
                account_id, email=binding.email, job_id=job_id, slot=slot
            )

    async def attach_instance(
        self,
        lease_id: str,
        instance_id: str,
        worker_id: str = "",
    ) -> AccountLease:
        """Attach a reserved account's EIP to its temporary instance.

        An EIP already attached to any other instance is never moved.  This
        explicit precondition, together with AWS ``AllowReassociation=False``,
        prevents a stale/concurrent job from stealing a live worker's IP.
        """

        if not instance_id:
            raise ValueError("instance_id is required when attaching an EIP")
        async with self._lease_lock(lease_id):
            prepared = await self._store.begin_attach(
                lease_id, instance_id, worker_id
            )

            try:
                binding = await self._store.get_binding(prepared.account_id)
                if binding is None or not binding.eip_allocation_id:
                    raise RuntimeError(
                        f"account {prepared.account_id!r} has no EIP allocation"
                    )
                binding = await self._verify_and_adopt_scope(
                    binding, await self._provider.get_identity()
                )

                eip = await self._provider.describe_eip(binding.eip_allocation_id)
                if eip is None:
                    raise RuntimeError(
                        f"EIP allocation {binding.eip_allocation_id!r} no longer exists"
                    )
                binding, eip = await self.verify_eip_ownership(binding, eip)
                await self._assert_no_other_account_eips(
                    binding, eip.allocation_id
                )
                if eip.instance_id and eip.instance_id != instance_id:
                    raise LeaseConflictError(
                        f"EIP {binding.eip_allocation_id!r} is already attached "
                        f"to instance {eip.instance_id!r}"
                    )
                if eip.instance_id is None:
                    associated = await self._provider.associate_eip(
                        instance_id, binding.eip_allocation_id
                    )
                    binding, associated = await self.verify_eip_ownership(
                        binding, associated
                    )
                    if associated.instance_id != instance_id:
                        raise LeaseConflictError(
                            f"provider associated EIP to unexpected instance "
                            f"{associated.instance_id!r}"
                        )

                attached = await self._store.update_lease(
                    lease_id,
                    state=LeaseState.ATTACHED,
                    error=None,
                    last_operation="attach",
                )
                assert attached is not None
                logger.info(
                    "Attached account %s EIP %s to %s for lease %s",
                    prepared.account_id,
                    binding.eip_ip,
                    instance_id,
                    lease_id,
                )
                return attached
            except BaseException as exc:  # noqa: BLE001
                try:
                    await self._store.update_lease(
                        lease_id,
                        state=LeaseState.ERROR,
                        error=self._error_text(exc),
                        last_operation="attach",
                    )
                except BaseException:  # noqa: BLE001
                    logger.exception(
                        "Could not persist failed attach for lease %s", lease_id
                    )
                if isinstance(exc, Exception):
                    logger.exception(
                        "Failed to attach EIP for lease %s", lease_id
                    )
                raise

    # -- Idempotent teardown of a temporary instance ---------------------

    async def release(
        self,
        lease_id: str,
        cleanup_worker: CleanupWorkerHook | None = None,
        *,
        expected_lease: AccountLease | None = None,
    ) -> AccountLease | None:
        """Release a lease while preserving its account's EIP allocation.

        Final result collection belongs to the orchestrator.  This method first
        attempts to detach the EIP, always attempts to terminate the instance
        even if detach fails, and only then invokes the control-plane cleanup
        hook (registry/disconnect).  Every completed phase is persisted.  A
        failure leaves the lease in ERROR and a retry resumes at incomplete
        phases without repeating successful destructive operations.
        """

        async with self._lease_lock(lease_id):
            return await self._release_locked(
                lease_id, cleanup_worker, expected_lease=expected_lease
            )

    async def _release_locked(
        self,
        lease_id: str,
        cleanup_worker: CleanupWorkerHook | None,
        *,
        expected_lease: AccountLease | None,
    ) -> AccountLease | None:
        lease = await self._store.begin_release(
            lease_id,
            cleanup_worker_required=cleanup_worker is not None,
            expected_lease=expected_lease,
        )
        if lease is None:
            return lease
        if lease.state == LeaseState.RELEASED:
            return lease

        try:
            phase_errors: list[BaseException] = []
            binding = await self._store.get_binding(lease.account_id)
            identity = await self._provider.get_identity()
            scope_verified = False
            scope_error: BaseException | None = None
            if binding is None:
                scope_error = RuntimeError(
                    f"active lease {lease_id!r} has no account binding"
                )
            else:
                try:
                    self._assert_scope(binding, identity)
                    scope_verified = self._scope_is_durably_verified(
                        binding, identity
                    )
                except BaseException as exc:  # noqa: BLE001
                    scope_error = exc

            # A v1 binding may not have durable provider/account/region scope
            # yet.  Even when its EIP was already marked detached, verify the
            # exact visible allocation before trusting NotFound for its EC2.
            if scope_error is not None:
                phase_errors.append(scope_error)
            elif not lease.eip_detached or not scope_verified:
                try:
                    assert binding is not None
                    if not binding.eip_allocation_id:
                        raise RuntimeError(
                            f"active lease {lease_id!r} has no EIP binding"
                        )
                    eip = await self._provider.describe_eip(
                        binding.eip_allocation_id
                    )
                    if eip is None:
                        raise RuntimeError(
                            f"persistent EIP {binding.eip_allocation_id!r} disappeared"
                        )
                    binding, eip = await self.verify_binding_eip(binding, eip)
                    scope_verified = self._scope_is_durably_verified(
                        binding, identity
                    )
                    if not scope_verified:
                        raise LeaseConflictError(
                            f"cloud scope for account {lease.account_id!r} "
                            "could not be durably verified"
                        )
                    if not lease.eip_detached:
                        if eip.instance_id:
                            if eip.instance_id != lease.instance_id:
                                raise LeaseConflictError(
                                    f"EIP {binding.eip_allocation_id!r} is attached "
                                    f"to foreign instance {eip.instance_id!r}; "
                                    "refusing detach"
                                )
                            await self._provider.disassociate_eip(
                                binding.eip_allocation_id,
                                association_id=eip.association_id,
                                expected_instance_id=lease.instance_id,
                            )
                        await self._wait_eip_detached(
                            binding, lease.instance_id
                        )
                        lease = await self._store.update_lease(
                            lease_id, eip_detached=True
                        )
                        assert lease is not None
                except BaseException as exc:  # noqa: BLE001
                    # Do not stop here: an EIP API failure must not leave the
                    # temporary EC2 instance accruing charges once its cloud
                    # scope has been independently proven.
                    phase_errors.append(exc)

            if not lease.instance_terminated:
                if lease.instance_id and not scope_verified:
                    if not phase_errors:
                        phase_errors.append(LeaseConflictError(
                            f"cloud scope for instance {lease.instance_id!r} "
                            "is not verified; refusing terminal NotFound"
                        ))
                else:
                    try:
                        if lease.instance_id:
                            await self._provider.terminate_instance(
                                lease.instance_id
                            )
                            await self._wait_instance_terminated(
                                lease.instance_id
                            )
                        lease = await self._store.update_lease(
                            lease_id, instance_terminated=True
                        )
                        assert lease is not None
                    except BaseException as exc:  # noqa: BLE001
                        phase_errors.append(exc)

            # The callback mirrors completed cloud teardown into Manager state.
            # Do not announce/disconnect a terminated worker when termination
            # actually failed; it will run after a successful retry instead.
            if not lease.worker_cleanup_done and lease.instance_terminated:
                try:
                    if lease.worker_cleanup_required and cleanup_worker is None:
                        raise ValueError(
                            f"lease {lease_id!r} requires cleanup_worker to retry release"
                        )
                    if cleanup_worker is not None:
                        await cleanup_worker(lease.model_copy(deep=True))
                    lease = await self._store.update_lease(
                        lease_id, worker_cleanup_done=True
                    )
                    assert lease is not None
                except BaseException as exc:  # noqa: BLE001
                    phase_errors.append(exc)

            if phase_errors:
                raise phase_errors[0]

            released = await self._store.update_lease(
                lease_id,
                state=LeaseState.RELEASED,
                error=None,
                last_operation="release",
                released_at=time.time(),
            )
            assert released is not None
            logger.info(
                "Released account lease %s; EIP retained for account %s",
                lease_id,
                released.account_id,
            )
            return released
        except BaseException as exc:  # noqa: BLE001
            try:
                await self._store.update_lease(
                    lease_id,
                    state=LeaseState.ERROR,
                    error=self._error_text(exc),
                    last_operation="release",
                )
            except BaseException:  # noqa: BLE001
                logger.exception(
                    "Could not persist failed release for lease %s", lease_id
                )
            if isinstance(exc, Exception):
                logger.exception("Failed to release account lease %s", lease_id)
            raise

    # -- Permanent account retirement ------------------------------------

    async def decommission(
        self, account_id: str, *, confirm_absent: bool = False
    ) -> bool:
        """Remove an account binding and permanently release its EIP.

        Active jobs are never killed by this administrative operation.  The
        caller must release their leases first; this method then permanently
        releases the detached allocation.  Failures leave an ERROR binding so
        the same decommission request can be retried.
        """

        async with self._account_lock(account_id):
            binding = await self._store.get_binding(account_id)
            if binding is None:
                return False

            identity = await self._provider.get_identity()
            self._assert_scope(binding, identity)

            active = await self._store.list_leases(
                account_id=account_id, active_only=True
            )
            if active:
                lease_ids = ", ".join(lease.lease_id for lease in active)
                raise LeaseConflictError(
                    f"account {account_id!r} has active lease(s): {lease_ids}"
                )

            if (
                binding.last_operation == "allocate_eip"
                and not binding.eip_allocation_id
            ):
                raise LeaseConflictError(
                    f"account {account_id!r} has an unresolved EIP allocation; "
                    "recover it before decommission"
                )

            binding = await self._store.update_binding(
                account_id,
                state=BindingState.DECOMMISSIONING,
                error=None,
                last_operation="decommission",
            )
            assert binding is not None
            try:
                if binding.eip_allocation_id:
                    eip = await self._observe_eip_for_decommission(
                        binding.eip_allocation_id
                    )
                    if eip is None:
                        if not self._scope_is_durably_verified(
                            binding, identity
                        ):
                            raise LeaseConflictError(
                                f"cannot confirm absent EIP "
                                f"{binding.eip_allocation_id!r}: cloud scope "
                                "has never been verified from a visible resource"
                            )
                        if binding.eip_release_succeeded:
                            pass
                        elif not binding.eip_absence_confirmed:
                            await self._store.update_binding(
                                account_id,
                                state=BindingState.ERROR,
                                error=(
                                    f"EIP {binding.eip_allocation_id!r} was absent; "
                                    "retry decommission to confirm before removing "
                                    "the durable handle"
                                ),
                                last_operation="decommission",
                                eip_absence_confirmed=True,
                            )
                            raise LeaseConflictError(
                                f"EIP {binding.eip_allocation_id!r} absence "
                                "requires an explicit retry"
                            )
                        elif not confirm_absent:
                            raise LeaseConflictError(
                                f"EIP {binding.eip_allocation_id!r} remains "
                                "stably absent; explicit administrator "
                                "confirmation is required"
                            )
                    else:
                        binding = await self._verify_and_adopt_scope(
                            binding, identity
                        )
                        # Scope adoption may have retagged a v1 allocation;
                        # never validate or release from the stale pre-retag
                        # observation.
                        eip = await self._provider.describe_eip(
                            binding.eip_allocation_id
                        )
                        if eip is None:
                            raise RuntimeError(
                                f"EIP {binding.eip_allocation_id!r} disappeared "
                                "during ownership verification"
                            )
                        binding, eip = await self.verify_eip_ownership(
                            binding, eip
                        )
                        await self._assert_no_other_account_eips(
                            binding, eip.allocation_id
                        )
                        if eip.instance_id:
                            raise LeaseConflictError(
                                f"EIP {binding.eip_allocation_id!r} is still attached "
                                f"to instance {eip.instance_id!r}; refusing decommission"
                            )
                        binding = await self._store.update_binding(
                            account_id,
                            eip_release_attempted=True,
                            eip_release_succeeded=False,
                            eip_absence_confirmed=False,
                        )
                        assert binding is not None
                        await self._provider.release_eip(
                            binding.eip_allocation_id
                        )
                        binding = await self._store.update_binding(
                            account_id,
                            eip_release_succeeded=True,
                        )
                        assert binding is not None
                        await self._wait_eip_released(
                            binding.eip_allocation_id
                        )
                removed = await self._store.remove_binding(account_id)
                logger.info("Decommissioned EIP binding for account %s", account_id)
                return removed
            except BaseException as exc:  # noqa: BLE001
                try:
                    await self._store.update_binding(
                        account_id,
                        state=BindingState.ERROR,
                        error=self._error_text(exc),
                        last_operation="decommission",
                    )
                except BaseException:  # noqa: BLE001
                    logger.exception(
                        "Could not persist failed EIP decommission for account %s",
                        account_id,
                    )
                if isinstance(exc, Exception):
                    logger.exception(
                        "Failed to decommission EIP binding for account %s",
                        account_id,
                    )
                raise
