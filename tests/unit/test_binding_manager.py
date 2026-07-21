"""Account EIP binding and ephemeral-instance lease lifecycle."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from elastic_agent.core import binding_manager as binding_manager_module
from elastic_agent.core.account_binding import (
    AccountBinding,
    AccountBindingStore,
    BindingState,
    LeaseConflictError,
    LeaseState,
)
from elastic_agent.core.binding_manager import BindingManager
from elastic_agent.core.providers.base import CloudIdentity, InstanceConfig
from elastic_agent.testing.dry_run_provider import DryRunProvider

pytestmark = pytest.mark.asyncio

CFG = InstanceConfig(instance_type="t3.large", image_id="ami-x", key_pair_name="k")


@pytest.fixture(autouse=True)
def _fast_cloud_confirmation(monkeypatch):
    monkeypatch.setattr(
        binding_manager_module, "EIP_ALLOCATION_CONVERGENCE_SECONDS", 0
    )
    monkeypatch.setattr(binding_manager_module, "TEARDOWN_CONFIRM_SECONDS", 0)


def _make(tmp_path):
    provider = DryRunProvider()
    store = AccountBindingStore(str(tmp_path / "bindings.json"))
    return provider, store, BindingManager(provider, store)


async def _instance(provider):
    return await provider.create_instance(CFG)


async def test_ensure_binding_allocates_only_persistent_eip(tmp_path):
    provider, store, manager = _make(tmp_path)
    binding = await manager.ensure_binding(
        "acc-1", email="a@example.com", region="dryrun-region"
    )
    assert binding.account_id == "acc-1"
    assert binding.eip_allocation_id
    assert binding.eip_ip
    assert binding.state == BindingState.READY
    assert provider.get_operations("create") == []
    assert len(provider.get_operations("allocate_eip")) == 1
    assert (await store.get_binding("acc-1")).eip_ip == binding.eip_ip


async def test_ensure_binding_is_concurrent_and_idempotent(tmp_path):
    provider, _store, manager = _make(tmp_path)
    bindings = await asyncio.gather(
        manager.ensure_binding("acc-1", email="a@example.com"),
        manager.ensure_binding("acc-1", email="A@example.com"),
        manager.ensure_binding("acc-1"),
    )
    assert len({binding.eip_allocation_id for binding in bindings}) == 1
    assert len(provider.get_operations("allocate_eip")) == 1
    assert (await manager.get_binding("acc-1")).email == "a@example.com"


async def test_bound_email_is_immutable_for_stable_account_id(tmp_path):
    provider, _store, manager = _make(tmp_path)
    first = await manager.ensure_binding("acc-1", email="one@example.com")

    with pytest.raises(LeaseConflictError, match="one@example.com"):
        await manager.ensure_binding("acc-1", email="other@example.com")

    assert (await manager.get_binding("acc-1")).email == "one@example.com"
    assert await provider.describe_eip(first.eip_allocation_id) is not None
    assert len(provider.get_operations("allocate_eip")) == 1


async def test_failed_initial_allocation_is_marked_and_retryable(tmp_path):
    provider, _store, manager = _make(tmp_path)
    provider.fail_next("allocate_eip", RuntimeError("AddressLimitExceeded"))
    with pytest.raises(RuntimeError, match="AddressLimit"):
        await manager.ensure_binding("acc-1", email="a@example.com")
    failed = await manager.get_binding("acc-1")
    assert failed.state == BindingState.ERROR
    assert "AddressLimit" in failed.error

    # The failed AllocateAddress response may be ambiguous (network timeout
    # after AWS accepted it), so retry performs the bounded adoption quarantine.
    with patch(
        "elastic_agent.core.binding_manager.asyncio.sleep", new=AsyncMock()
    ):
        ready = await manager.ensure_binding("acc-1", email="a@example.com")
    assert ready.state == BindingState.READY
    assert ready.eip_allocation_id


async def test_unscoped_eip_without_durable_intent_blocks_duplicate(tmp_path):
    provider, _store, manager = _make(tmp_path)
    orphan = await provider.allocate_eip(tags={
        "AccountId": "acc-1",
        "Role": "account-eip",
    })

    with pytest.raises(LeaseConflictError, match="refusing duplicate allocation"):
        await manager.ensure_binding("acc-1", email="a@example.com")

    assert orphan.allocation_id
    assert len(provider.get_operations("allocate_eip")) == 1


async def test_retry_waits_for_eventually_visible_tagged_eip(tmp_path):
    provider, store, manager = _make(tmp_path)
    # Durable intent exists, as it would after a crash during AllocateAddress.
    await store.upsert_binding(AccountBinding(
        account_id="acc-1",
        state=BindingState.ALLOCATING,
        last_operation="allocate_eip",
    ))
    orphan = await provider.allocate_eip(tags={
        "AccountId": "acc-1",
        "Role": "account-eip",
    })
    real_list = provider.list_eips
    visible = await real_list(filters={"AccountId": "acc-1", "Role": "account-eip"})
    # First recovery iteration: strict + legacy both invisible; second strict
    # remains empty, then the pre-controller allocation appears.
    responses = iter([[], [], [], visible])

    async def eventually_visible(*, filters=None):
        try:
            return next(responses)
        except StopIteration:
            return await real_list(filters=filters)

    provider.list_eips = AsyncMock(side_effect=eventually_visible)

    with patch(
        "elastic_agent.core.binding_manager.asyncio.sleep", new=AsyncMock()
    ) as sleep_mock:
        adopted = await manager.ensure_binding("acc-1")

    assert adopted.eip_allocation_id == orphan.allocation_id
    assert provider.list_eips.await_count >= 5
    sleep_mock.assert_awaited_once()
    # Only the simulated pre-crash allocation exists; no duplicate was made.
    assert len(provider.get_operations("allocate_eip")) == 1


async def test_multiple_tagged_orphans_fail_closed(tmp_path):
    provider, _store, manager = _make(tmp_path)
    tags = {"AccountId": "acc-1", "Role": "account-eip"}
    await provider.allocate_eip(tags=tags)
    await provider.allocate_eip(tags=tags)

    with pytest.raises(LeaseConflictError, match="found 2 unscoped/foreign EIP"):
        await manager.ensure_binding("acc-1")

    failed = await manager.get_binding("acc-1")
    assert failed.state == BindingState.ERROR
    assert failed.eip_allocation_id is None


async def test_missing_stored_allocation_is_not_silently_replaced(tmp_path):
    provider, store, manager = _make(tmp_path)
    first = await manager.ensure_binding("acc-1")
    await provider.release_eip(first.eip_allocation_id)

    with pytest.raises(LeaseConflictError, match="refusing automatic replacement"):
        await manager.ensure_binding("acc-1")

    failed = await store.get_binding("acc-1")
    assert failed.state == BindingState.ERROR
    assert failed.eip_allocation_id == first.eip_allocation_id
    assert len(provider.get_operations("allocate_eip")) == 1


async def test_existing_binding_rejects_missing_managed_tag(tmp_path):
    provider, _store, manager = _make(tmp_path)
    binding = await manager.ensure_binding("acc-1")
    provider.eips[binding.eip_allocation_id].tags.pop("ManagedBy")

    with pytest.raises(LeaseConflictError, match="not managed"):
        await manager.ensure_binding("acc-1")


async def test_legacy_v1_eip_is_verified_and_retagged_before_adoption(tmp_path):
    provider, store, manager = _make(tmp_path)
    legacy = await provider.allocate_eip(tags={
        "account": "legacy@example.com",
        "role": "codex-account-box",
    })
    await store.upsert_binding(AccountBinding(
        account_id="acc-legacy",
        email="legacy@example.com",
        eip_allocation_id=legacy.allocation_id,
        eip_ip=legacy.public_ip,
        state=BindingState.READY,
    ))

    adopted = await manager.ensure_binding(
        "acc-legacy", email="LEGACY@example.com"
    )
    observed = await provider.describe_eip(legacy.allocation_id)

    assert adopted.controller_id == store.controller_id
    assert observed.tags["AccountId"] == "acc-legacy"
    assert observed.tags["Role"] == "account-eip"
    assert observed.tags["ElasticAgentController"] == store.controller_id
    assert len(provider.get_operations("tag_eip")) == 1


async def test_legacy_v1_eip_is_retagged_before_direct_decommission(tmp_path):
    provider, store, manager = _make(tmp_path)
    legacy = await provider.allocate_eip(tags={
        "account": "legacy@example.com",
        "role": "codex-account-box",
    })
    await store.upsert_binding(AccountBinding(
        account_id="acc-legacy",
        email="legacy@example.com",
        eip_allocation_id=legacy.allocation_id,
        eip_ip=legacy.public_ip,
        state=BindingState.READY,
    ))

    assert await manager.decommission("acc-legacy") is True
    assert len(provider.get_operations("tag_eip")) == 1
    assert await provider.describe_eip(legacy.allocation_id) is None


async def test_request_region_must_match_provider_identity(tmp_path):
    provider, store, manager = _make(tmp_path)
    provider.get_identity = AsyncMock(return_value=CloudIdentity(
        provider="dryrun", account_id="dryrun-account", region="region-a",
    ))

    with pytest.raises(ValueError, match="provider region"):
        await manager.ensure_binding("acc-1", region="region-b")

    assert await store.get_binding("acc-1") is None
    assert provider.get_operations("allocate_eip") == []


async def test_same_email_can_have_distinct_account_bindings(tmp_path):
    provider, _store, manager = _make(tmp_path)
    one = await manager.ensure_binding("acc-1", email="same@example.com")
    two = await manager.ensure_binding("acc-2", email="same@example.com")
    assert one.eip_allocation_id != two.eip_allocation_id
    assert len(provider.get_operations("allocate_eip")) == 2


async def test_binding_region_is_immutable(tmp_path):
    _provider, _store, manager = _make(tmp_path)
    await manager.ensure_binding("acc-1", region="us-east-1")
    with pytest.raises(ValueError, match="us-east-1"):
        await manager.ensure_binding("acc-1", region="us-west-2")


async def test_scope_mismatch_blocks_detach_and_terminal_not_found(tmp_path):
    provider, store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    await store.update_binding("acc-1", cloud_account_id="different-account")

    with pytest.raises(LeaseConflictError, match="cloud account"):
        await manager.release(lease.lease_id)

    assert (await provider.get_instance(instance.instance_id)).state.value == "running"
    assert len(provider.get_operations("terminate")) == 0
    assert (await manager.get_lease(lease.lease_id)).state == LeaseState.ERROR


async def test_partial_scope_is_not_adopted_from_wrong_region_not_found(tmp_path):
    provider, store, manager = _make(tmp_path)
    eip = await provider.allocate_eip(tags={
        "AccountId": "acc-partial",
        "Role": "account-eip",
        "ElasticAgentController": store.controller_id,
    })
    await store.upsert_binding(AccountBinding(
        account_id="acc-partial",
        eip_allocation_id=eip.allocation_id,
        eip_ip=eip.public_ip,
        cloud_account_id="dryrun-account",
        controller_id=store.controller_id,
        state=BindingState.READY,
    ))
    lease = await store.reserve_lease("acc-partial", job_id="job-partial")
    instance = await _instance(provider)
    real_describe = provider.describe_eip
    provider.get_identity = AsyncMock(return_value=CloudIdentity(
        provider="dryrun", account_id="dryrun-account", region="wrong-region",
    ))
    provider.describe_eip = AsyncMock(return_value=None)

    with pytest.raises(LeaseConflictError, match="cannot verify cloud owner"):
        await manager.attach_instance(lease.lease_id, instance.instance_id)

    unadopted = await store.get_binding("acc-partial")
    assert unadopted.cloud_provider == ""
    assert unadopted.region == ""

    provider.get_identity = AsyncMock(return_value=CloudIdentity(
        provider="dryrun", account_id="dryrun-account", region="correct-region",
    ))
    provider.describe_eip = real_describe
    attached = await manager.attach_instance(lease.lease_id, instance.instance_id)
    adopted = await store.get_binding("acc-partial")
    assert attached.state == LeaseState.ATTACHED
    assert adopted.cloud_provider == "dryrun"
    assert adopted.region == "correct-region"


async def test_decommission_fails_closed_after_region_change(tmp_path):
    provider, _store, manager = _make(tmp_path)
    binding = await manager.ensure_binding("acc-1", region="region-a")
    provider.get_identity = AsyncMock(return_value=CloudIdentity(
        provider="dryrun", account_id="dryrun-account", region="region-b",
    ))

    with pytest.raises(LeaseConflictError, match="region"):
        await manager.decommission("acc-1")

    assert await provider.describe_eip(binding.eip_allocation_id) is not None
    assert await manager.get_binding("acc-1") is not None


async def test_reserve_ensures_eip_and_does_not_create_instance(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve(
        "acc-1", email="a@example.com", job_id="job-1", slot=3,
        region="dryrun-region",
    )
    binding = await manager.get_binding("acc-1")
    assert binding.eip_allocation_id
    assert lease.job_id == "job-1"
    assert lease.slot == 3
    assert lease.state == LeaseState.RESERVED
    assert lease.generation == 1
    assert provider.get_operations("create") == []


async def test_concurrent_reserve_blocks_other_job(tmp_path):
    _provider, _store, manager = _make(tmp_path)
    results = await asyncio.gather(
        manager.reserve("acc-1", job_id="job-a"),
        manager.reserve("acc-1", job_id="job-b"),
        return_exceptions=True,
    )
    assert sum(getattr(result, "state", None) == LeaseState.RESERVED for result in results) == 1
    assert sum(isinstance(result, LeaseConflictError) for result in results) == 1


async def test_account_transaction_serializes_reserve(tmp_path):
    _provider, _store, manager = _make(tmp_path)

    async with manager.account_transaction("acc-1"):
        reserve_task = asyncio.create_task(
            manager.reserve("acc-1", email="one@example.com", job_id="job-1")
        )
        await asyncio.sleep(0)
        assert reserve_task.done() is False

    lease = await reserve_task
    assert lease.account_id == "acc-1"


async def test_same_job_slot_reserve_is_idempotent_across_restart(tmp_path):
    provider, store, manager = _make(tmp_path)
    first = await manager.reserve("acc-1", job_id="job-1", slot=1)
    restarted = BindingManager(provider, AccountBindingStore(str(store.path)))
    second = await restarted.reserve("acc-1", job_id="job-1", slot=1)
    assert second.lease_id == first.lease_id
    with pytest.raises(LeaseConflictError):
        await restarted.reserve("acc-1", job_id="job-2", slot=1)


async def test_attach_instance_associates_eip_and_records_worker(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    attached = await manager.attach_instance(
        lease.lease_id, instance.instance_id, worker_id="worker-1"
    )
    binding = await manager.get_binding("acc-1")
    eip = await provider.describe_eip(binding.eip_allocation_id)
    assert attached.state == LeaseState.ATTACHED
    assert attached.instance_id == instance.instance_id
    assert attached.worker_id == "worker-1"
    assert eip.instance_id == instance.instance_id
    assert len(provider.get_operations("associate_eip")) == 1


async def test_attach_same_instance_is_idempotent(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    first = await manager.attach_instance(lease.lease_id, instance.instance_id)
    second = await manager.attach_instance(lease.lease_id, instance.instance_id)
    assert second.lease_id == first.lease_id
    assert len(provider.get_operations("associate_eip")) == 1


async def test_attach_refuses_to_steal_eip_from_other_instance(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    binding = await manager.get_binding("acc-1")
    old_instance = await _instance(provider)
    new_instance = await _instance(provider)
    await provider.associate_eip(old_instance.instance_id, binding.eip_allocation_id)
    before = len(provider.get_operations("associate_eip"))

    with pytest.raises(LeaseConflictError, match=old_instance.instance_id):
        await manager.attach_instance(lease.lease_id, new_instance.instance_id)
    assert len(provider.get_operations("associate_eip")) == before
    failed = await manager.get_lease(lease.lease_id)
    assert failed.state == LeaseState.ERROR
    assert failed.last_operation == "attach"


async def test_attach_provider_failure_is_retryable(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    provider.fail_next("associate_eip", RuntimeError("throttled"))
    with pytest.raises(RuntimeError, match="throttled"):
        await manager.attach_instance(lease.lease_id, instance.instance_id)
    assert (await manager.get_lease(lease.lease_id)).state == LeaseState.ERROR

    attached = await manager.attach_instance(lease.lease_id, instance.instance_id)
    assert attached.state == LeaseState.ATTACHED


async def test_attach_rejects_foreign_controller_tag(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    binding = await manager.get_binding("acc-1")
    instance = await _instance(provider)
    provider.eips[binding.eip_allocation_id].tags[
        "ElasticAgentController"
    ] = "foreign-controller"

    with pytest.raises(LeaseConflictError, match="foreign-controller"):
        await manager.attach_instance(lease.lease_id, instance.instance_id)

    assert provider.get_operations("associate_eip") == []
    assert (await manager.get_lease(lease.lease_id)).state == LeaseState.ERROR


async def test_cancelled_attach_is_marked_and_retryable(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    real_associate = provider.associate_eip

    async def cancel_after_associate(instance_id, allocation_id):
        await real_associate(instance_id, allocation_id)
        raise asyncio.CancelledError()

    provider.associate_eip = cancel_after_associate
    with pytest.raises(asyncio.CancelledError):
        await manager.attach_instance(lease.lease_id, instance.instance_id)

    failed = await manager.get_lease(lease.lease_id)
    assert failed.state == LeaseState.ERROR
    assert failed.last_operation == "attach"
    provider.associate_eip = real_associate
    assert (
        await manager.attach_instance(lease.lease_id, instance.instance_id)
    ).state == LeaseState.ATTACHED


async def test_release_cleans_worker_detaches_terminates_and_keeps_eip(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id, "worker-1")
    binding = await manager.get_binding("acc-1")
    calls = []

    async def cleanup(current):
        # The callback is control-plane cleanup only.  Cloud teardown has
        # already happened, so it must never call scale_in/terminate itself.
        current_eip = await provider.describe_eip(binding.eip_allocation_id)
        current_instance = await provider.get_instance(instance.instance_id)
        assert current_eip.instance_id is None
        assert current_instance.state.value == "terminated"
        calls.append((current.worker_id, current.instance_id))

    released = await manager.release(lease.lease_id, cleanup)
    eip = await provider.describe_eip(binding.eip_allocation_id)
    assert calls == [("worker-1", instance.instance_id)]
    assert released.state == LeaseState.RELEASED
    assert released.worker_cleanup_done
    assert released.eip_detached
    assert released.instance_terminated
    assert eip is not None and eip.instance_id is None
    assert (await provider.get_instance(instance.instance_id)).state.value == "terminated"
    assert len(provider.get_operations("release_eip")) == 0


async def test_release_is_idempotent(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    cleanup_calls = 0

    async def cleanup(_lease):
        nonlocal cleanup_calls
        cleanup_calls += 1

    await manager.release(lease.lease_id, cleanup)
    again = await manager.release(lease.lease_id, cleanup)
    assert again.state == LeaseState.RELEASED
    assert cleanup_calls == 1
    assert len(provider.get_operations("disassociate_eip")) == 1
    assert len(provider.get_operations("terminate")) == 1
    assert await manager.release("missing", cleanup) is None


async def test_release_failure_is_marked_and_retries_remaining_phases(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    cleanup_calls = 0

    async def cleanup(_lease):
        nonlocal cleanup_calls
        cleanup_calls += 1

    provider.fail_next("terminate_instance", RuntimeError("EC2 busy"))
    with pytest.raises(RuntimeError, match="busy"):
        await manager.release(lease.lease_id, cleanup)
    failed = await manager.get_lease(lease.lease_id)
    assert failed.state == LeaseState.ERROR
    assert failed.last_operation == "release"
    # Registry/disconnect runs only after the instance is truly terminated.
    assert failed.worker_cleanup_done is False
    assert failed.eip_detached is True
    assert failed.instance_terminated is False
    assert cleanup_calls == 0

    released = await manager.release(lease.lease_id, cleanup)
    assert released.state == LeaseState.RELEASED
    assert cleanup_calls == 1
    assert len(provider.get_operations("disassociate_eip")) == 1
    assert len(provider.get_operations("terminate")) == 1


async def test_release_commits_detach_only_after_cloud_confirmation(
    tmp_path, monkeypatch,
):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    real_disassociate = provider.disassociate_eip
    provider.disassociate_eip = AsyncMock(return_value=None)
    monkeypatch.setattr(binding_manager_module, "TEARDOWN_CONFIRM_ATTEMPTS", 2)

    with pytest.raises(RuntimeError, match="did not become detached"):
        await manager.release(lease.lease_id)

    failed = await manager.get_lease(lease.lease_id)
    assert failed.eip_detached is False
    assert failed.instance_terminated is True
    provider.disassociate_eip = real_disassociate
    assert (await manager.release(lease.lease_id)).state == LeaseState.RELEASED


async def test_release_commits_termination_only_after_terminal_readback(
    tmp_path, monkeypatch,
):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    real_terminate = provider.terminate_instance
    provider.terminate_instance = AsyncMock(return_value=None)
    monkeypatch.setattr(binding_manager_module, "TEARDOWN_CONFIRM_ATTEMPTS", 2)

    with pytest.raises(RuntimeError, match="did not reach terminated"):
        await manager.release(lease.lease_id)

    failed = await manager.get_lease(lease.lease_id)
    assert failed.eip_detached is True
    assert failed.instance_terminated is False
    provider.terminate_instance = real_terminate
    assert (await manager.release(lease.lease_id)).state == LeaseState.RELEASED


async def test_cancelled_detach_still_terminates_and_retry_finishes(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    real_disassociate = provider.disassociate_eip

    async def cancel_after_detach(*args, **kwargs):
        await real_disassociate(*args, **kwargs)
        raise asyncio.CancelledError()

    provider.disassociate_eip = cancel_after_detach
    with pytest.raises(asyncio.CancelledError):
        await manager.release(lease.lease_id)

    failed = await manager.get_lease(lease.lease_id)
    assert failed.state == LeaseState.ERROR
    assert failed.last_operation == "release"
    assert failed.eip_detached is False
    assert failed.instance_terminated is True
    provider.disassociate_eip = real_disassociate
    assert (await manager.release(lease.lease_id)).state == LeaseState.RELEASED


async def test_cleanup_hook_failure_retries_without_repeating_cloud_cleanup(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    attempts = 0

    async def cleanup(_lease):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("collect failed")

    with pytest.raises(RuntimeError, match="collect failed"):
        await manager.release(lease.lease_id, cleanup)
    # A control-plane callback must never leave a billable instance behind.
    assert len(provider.get_operations("disassociate_eip")) == 1
    assert len(provider.get_operations("terminate")) == 1
    assert (await manager.release(lease.lease_id, cleanup)).state == LeaseState.RELEASED
    assert attempts == 2
    assert len(provider.get_operations("disassociate_eip")) == 1
    assert len(provider.get_operations("terminate")) == 1


async def test_detach_failure_still_terminates_and_cleanup_then_retries(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    cleanup_calls = 0

    async def cleanup(_lease):
        nonlocal cleanup_calls
        cleanup_calls += 1

    provider.fail_next("disassociate_eip", RuntimeError("EC2 detach throttled"))
    with pytest.raises(RuntimeError, match="detach throttled"):
        await manager.release(lease.lease_id, cleanup)
    failed = await manager.get_lease(lease.lease_id)
    assert failed.eip_detached is False
    assert failed.instance_terminated is True
    assert failed.worker_cleanup_done is True
    assert len(provider.get_operations("terminate")) == 1
    assert cleanup_calls == 1

    released = await manager.release(lease.lease_id, cleanup)
    assert released.state == LeaseState.RELEASED
    assert len(provider.get_operations("terminate")) == 1
    assert cleanup_calls == 1


async def test_stale_lease_never_detaches_eip_from_new_instance(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    stale_instance = await _instance(provider)
    new_instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, stale_instance.instance_id)
    binding = await manager.get_binding("acc-1")
    # Simulate out-of-band replacement after the persisted lease went stale.
    await provider.disassociate_eip(binding.eip_allocation_id)
    await provider.associate_eip(new_instance.instance_id, binding.eip_allocation_id)
    detach_count = len(provider.get_operations("disassociate_eip"))

    with pytest.raises(LeaseConflictError, match=new_instance.instance_id):
        await manager.release(lease.lease_id)
    assert len(provider.get_operations("disassociate_eip")) == detach_count
    assert (await provider.describe_eip(binding.eip_allocation_id)).instance_id == new_instance.instance_id
    assert (await provider.get_instance(stale_instance.instance_id)).state.value == "terminated"
    assert (await provider.get_instance(new_instance.instance_id)).state.value == "running"


async def test_release_tag_mismatch_blocks_detach_but_not_termination(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    binding = await manager.get_binding("acc-1")
    provider.eips[binding.eip_allocation_id].tags["AccountId"] = "other-account"

    with pytest.raises(LeaseConflictError, match="ownership tags"):
        await manager.release(lease.lease_id)

    failed = await manager.get_lease(lease.lease_id)
    assert failed.eip_detached is False
    assert failed.instance_terminated is True
    assert provider.get_operations("disassociate_eip") == []
    assert (await provider.get_instance(instance.instance_id)).state.value == "terminated"


async def test_unverified_legacy_scope_does_not_commit_wrong_scope_not_found(
    tmp_path,
):
    provider, store, manager = _make(tmp_path)
    legacy = await provider.allocate_eip(tags={
        "account": "legacy@example.com",
        "role": "codex-account-box",
    })
    await store.upsert_binding(AccountBinding(
        account_id="acc-legacy",
        email="legacy@example.com",
        eip_allocation_id=legacy.allocation_id,
        eip_ip=legacy.public_ip,
        state=BindingState.READY,
    ))
    lease = await store.reserve_lease("acc-legacy", job_id="legacy-job")
    instance = await _instance(provider)
    await store.begin_attach(lease.lease_id, instance.instance_id, "legacy-worker")
    await provider.associate_eip(instance.instance_id, legacy.allocation_id)
    await store.update_lease(
        lease.lease_id,
        state=LeaseState.ATTACHED,
        last_operation="attach",
    )
    real_identity = provider.get_identity
    real_describe = provider.describe_eip
    provider.get_identity = AsyncMock(return_value=CloudIdentity(
        provider="dryrun", account_id="wrong-account", region="wrong-region",
    ))
    provider.describe_eip = AsyncMock(return_value=None)

    with pytest.raises(RuntimeError, match="disappeared"):
        await manager.release(lease.lease_id)

    failed = await manager.get_lease(lease.lease_id)
    assert failed.instance_terminated is False
    assert len(provider.get_operations("terminate")) == 0
    assert provider.instances[instance.instance_id].state.value == "running"

    provider.get_identity = real_identity
    provider.describe_eip = real_describe
    released = await manager.release(lease.lease_id)
    assert released.state == LeaseState.RELEASED
    assert released.instance_terminated is True
    assert provider.instances[instance.instance_id].state.value == "terminated"


async def test_release_reserved_lease_needs_no_instance(tmp_path):
    _provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    released = await manager.release(lease.lease_id)
    assert released.state == LeaseState.RELEASED
    assert released.eip_detached is True
    assert released.instance_terminated is True


async def test_decommission_releases_eip_and_removes_binding(tmp_path):
    provider, _store, manager = _make(tmp_path)
    binding = await manager.ensure_binding("acc-1", email="a@example.com")
    assert await manager.decommission("acc-1") is True
    assert await manager.get_binding("acc-1") is None
    assert await provider.describe_eip(binding.eip_allocation_id) is None
    assert len(provider.get_operations("release_eip")) == 1
    assert await manager.decommission("acc-1") is False


async def test_decommission_refuses_active_lease(tmp_path):
    provider, _store, manager = _make(tmp_path)
    lease = await manager.reserve("acc-1", job_id="job-1")
    instance = await _instance(provider)
    await manager.attach_instance(lease.lease_id, instance.instance_id)
    with pytest.raises(LeaseConflictError, match="active lease"):
        await manager.decommission("acc-1")
    assert (await manager.get_lease(lease.lease_id)).state == LeaseState.ATTACHED
    assert len(provider.get_operations("terminate")) == 0
    assert len(provider.get_operations("release_eip")) == 0
    await manager.release(lease.lease_id)
    assert await manager.decommission("acc-1") is True


async def test_decommission_failure_keeps_error_binding_for_retry(tmp_path):
    provider, _store, manager = _make(tmp_path)
    await manager.ensure_binding("acc-1")
    provider.fail_next("release_eip", RuntimeError("dependency violation"))
    with pytest.raises(RuntimeError, match="dependency"):
        await manager.decommission("acc-1")
    failed = await manager.get_binding("acc-1")
    assert failed.state == BindingState.ERROR
    assert failed.last_operation == "decommission"
    with pytest.raises(LeaseConflictError, match="decommission is pending"):
        await manager.ensure_binding("acc-1")
    assert await manager.decommission("acc-1") is True


async def test_decommission_rejects_wrong_role_tag_without_release(tmp_path):
    provider, _store, manager = _make(tmp_path)
    binding = await manager.ensure_binding("acc-1")
    provider.eips[binding.eip_allocation_id].tags["Role"] = "not-account-eip"

    with pytest.raises(LeaseConflictError, match="ownership tags"):
        await manager.decommission("acc-1")

    assert await manager.get_binding("acc-1") is not None
    assert provider.get_operations("release_eip") == []


async def test_decommission_false_missing_read_keeps_handle_until_retry(tmp_path):
    provider, _store, manager = _make(tmp_path)
    binding = await manager.ensure_binding("acc-1")
    real_describe = provider.describe_eip
    provider.describe_eip = AsyncMock(return_value=None)

    with pytest.raises(LeaseConflictError, match="explicit retry"):
        await manager.decommission("acc-1")

    retained = await manager.get_binding("acc-1")
    assert retained.eip_allocation_id == binding.eip_allocation_id
    assert retained.eip_absence_confirmed is True
    assert binding.eip_allocation_id in provider.eips
    provider.describe_eip = real_describe
    assert await manager.decommission("acc-1") is True
    assert binding.eip_allocation_id not in provider.eips


async def test_unverified_legacy_scope_never_confirms_missing_eip(tmp_path):
    provider, store, manager = _make(tmp_path)
    legacy = await provider.allocate_eip(tags={
        "account": "legacy@example.com",
        "role": "codex-account-box",
    })
    await store.upsert_binding(AccountBinding(
        account_id="acc-legacy",
        email="legacy@example.com",
        eip_allocation_id=legacy.allocation_id,
        eip_ip=legacy.public_ip,
        state=BindingState.READY,
    ))
    real_identity = provider.get_identity
    real_describe = provider.describe_eip
    provider.get_identity = AsyncMock(return_value=CloudIdentity(
        provider="dryrun", account_id="wrong-account", region="wrong-region",
    ))
    provider.describe_eip = AsyncMock(return_value=None)

    for _attempt in range(3):
        with pytest.raises(LeaseConflictError, match="never been verified"):
            await manager.decommission("acc-legacy", confirm_absent=True)

    retained = await manager.get_binding("acc-legacy")
    assert retained.eip_allocation_id == legacy.allocation_id
    assert retained.eip_absence_confirmed is False
    assert legacy.allocation_id in provider.eips
    provider.get_identity = real_identity
    provider.describe_eip = real_describe
    assert await manager.decommission("acc-legacy") is True
    assert legacy.allocation_id not in provider.eips


async def test_decommission_preserves_ambiguous_allocation_recovery(tmp_path):
    provider, store, manager = _make(tmp_path)
    await store.upsert_binding(AccountBinding(
        account_id="acc-pending",
        controller_id=store.controller_id,
        state=BindingState.ERROR,
        last_operation="allocate_eip",
    ))
    orphan = await provider.allocate_eip(tags={
        "AccountId": "acc-pending",
        "Role": "account-eip",
        "ElasticAgentController": store.controller_id,
    })

    with pytest.raises(LeaseConflictError, match="recover it before"):
        await manager.decommission("acc-pending")

    pending = await manager.get_binding("acc-pending")
    assert pending.last_operation == "allocate_eip"
    assert pending.eip_allocation_id is None
    assert await manager.recover_pending_allocation("acc-pending") is True
    assert await manager.decommission("acc-pending") is True
    assert await provider.describe_eip(orphan.allocation_id) is None


async def test_cancelled_decommission_keeps_durable_recovery_markers(tmp_path):
    provider, _store, manager = _make(tmp_path)
    binding = await manager.ensure_binding("acc-1")
    real_release = provider.release_eip

    async def cancel_after_release(allocation_id):
        await real_release(allocation_id)
        raise asyncio.CancelledError()

    provider.release_eip = cancel_after_release
    with pytest.raises(asyncio.CancelledError):
        await manager.decommission("acc-1")

    pending = await manager.get_binding("acc-1")
    assert pending.state == BindingState.ERROR
    assert pending.last_operation == "decommission"
    assert pending.eip_release_attempted is True
    assert pending.eip_release_succeeded is False
    provider.release_eip = real_release
    with pytest.raises(LeaseConflictError, match="explicit retry"):
        await manager.decommission("acc-1")
    assert await manager.decommission("acc-1", confirm_absent=True) is True
    assert await provider.describe_eip(binding.eip_allocation_id) is None


async def test_interrupted_decommission_intent_cannot_be_reactivated(tmp_path):
    provider, store, manager = _make(tmp_path)
    binding = await manager.ensure_binding("acc-1")
    await store.update_binding(
        "acc-1",
        state=BindingState.DECOMMISSIONING,
        last_operation="decommission",
    )

    with pytest.raises(LeaseConflictError, match="decommission is pending"):
        await manager.reserve("acc-1", job_id="job-after-crash")

    assert await provider.describe_eip(binding.eip_allocation_id) is not None
    assert await manager.decommission("acc-1") is True
    assert await provider.describe_eip(binding.eip_allocation_id) is None


async def test_same_account_in_another_controller_fails_without_duplicate_eip(
    tmp_path,
):
    provider = DryRunProvider()
    first_store = AccountBindingStore(str(tmp_path / "controller-a.json"))
    second_store = AccountBindingStore(str(tmp_path / "controller-b.json"))
    first = BindingManager(provider, first_store)
    second = BindingManager(provider, second_store)

    first_binding = await first.ensure_binding("shared-account")
    with pytest.raises(LeaseConflictError, match="foreign EIP"):
        await second.ensure_binding("shared-account")

    assert first_store.controller_id != second_store.controller_id
    assert await provider.describe_eip(first_binding.eip_allocation_id) is not None
    assert len(provider.get_operations("allocate_eip")) == 1


async def test_concurrent_first_binding_across_controllers_compensates_duplicates(
    tmp_path,
):
    provider = DryRunProvider()
    first = BindingManager(
        provider, AccountBindingStore(str(tmp_path / "controller-a.json"))
    )
    second = BindingManager(
        provider, AccountBindingStore(str(tmp_path / "controller-b.json"))
    )
    real_list = provider.list_eips
    real_allocate = provider.allocate_eip
    broad_ready = asyncio.Event()
    allocation_ready = asyncio.Event()
    broad_calls = 0
    allocation_calls = 0

    async def synchronized_list(*, filters=None):
        nonlocal broad_calls
        if filters == {"AccountId": "shared", "Role": "account-eip"} and (
            broad_calls < 2
        ):
            broad_calls += 1
            if broad_calls == 2:
                broad_ready.set()
            await broad_ready.wait()
            return []
        return await real_list(filters=filters)

    async def synchronized_allocate(*, tags=None):
        nonlocal allocation_calls
        allocated = await real_allocate(tags=tags)
        allocation_calls += 1
        if allocation_calls == 2:
            allocation_ready.set()
        await allocation_ready.wait()
        return allocated

    provider.list_eips = synchronized_list
    provider.allocate_eip = synchronized_allocate
    results = await asyncio.gather(
        first.ensure_binding("shared"),
        second.ensure_binding("shared"),
        return_exceptions=True,
    )

    successes = [result for result in results if isinstance(result, AccountBinding)]
    conflicts = [result for result in results if isinstance(result, LeaseConflictError)]
    assert len(successes) <= 1
    assert len(conflicts) >= 1
    assert len(successes) + len(conflicts) == 2
    assert len(provider.eips) <= 1
    assert len(provider.get_operations("allocate_eip")) == 2
    assert len(provider.get_operations("release_eip")) == 2 - len(successes)


async def test_convergence_catches_competitor_hidden_beyond_two_clean_scans(
    tmp_path,
):
    provider = DryRunProvider()
    store = AccountBindingStore(str(tmp_path / "bindings.json"))
    manager = BindingManager(provider, store)
    foreign = await provider.allocate_eip(tags={
        "AccountId": "shared",
        "Role": "account-eip",
        "ElasticAgentController": "foreign-controller",
    })
    real_list = provider.list_eips
    broad_calls = 0

    async def delayed_foreign_visibility(*, filters=None):
        nonlocal broad_calls
        values = await real_list(filters=filters)
        if filters == {"AccountId": "shared", "Role": "account-eip"}:
            broad_calls += 1
            if broad_calls <= 4:
                return [
                    eip
                    for eip in values
                    if eip.tags.get("ElasticAgentController")
                    == store.controller_id
                ]
        return values

    provider.list_eips = delayed_foreign_visibility
    with pytest.raises(LeaseConflictError, match="concurrent controller"):
        await manager.ensure_binding("shared")

    assert broad_calls >= 5
    assert set(provider.eips) == {foreign.allocation_id}
    assert len(provider.get_operations("release_eip")) == 1


async def test_timeout_recovery_also_converges_across_controllers(tmp_path):
    provider = DryRunProvider()
    first_store = AccountBindingStore(str(tmp_path / "controller-a.json"))
    second_store = AccountBindingStore(str(tmp_path / "controller-b.json"))
    first = BindingManager(provider, first_store)
    second = BindingManager(provider, second_store)
    await first_store.upsert_binding(AccountBinding(
        account_id="shared",
        controller_id=first_store.controller_id,
        state=BindingState.ERROR,
        last_operation="allocate_eip",
    ))
    await second_store.upsert_binding(AccountBinding(
        account_id="shared",
        controller_id=second_store.controller_id,
        state=BindingState.ERROR,
        last_operation="allocate_eip",
    ))
    first_eip = await provider.allocate_eip(tags={
        "AccountId": "shared",
        "Role": "account-eip",
        "ElasticAgentController": first_store.controller_id,
    })
    second_eip = await provider.allocate_eip(tags={
        "AccountId": "shared",
        "Role": "account-eip",
        "ElasticAgentController": second_store.controller_id,
    })
    real_list = provider.list_eips
    calls = {"recovery-a": 0, "recovery-b": 0}

    async def initially_partitioned(*, filters=None):
        task_name = asyncio.current_task().get_name()
        if filters == {"AccountId": "shared", "Role": "account-eip"} and (
            task_name in calls
        ):
            calls[task_name] += 1
            if calls[task_name] == 1:
                return [first_eip if task_name == "recovery-a" else second_eip]
        return await real_list(filters=filters)

    provider.list_eips = initially_partitioned
    results = await asyncio.gather(
        asyncio.create_task(
            first.recover_pending_allocation("shared"), name="recovery-a"
        ),
        asyncio.create_task(
            second.recover_pending_allocation("shared"), name="recovery-b"
        ),
    )

    assert results == [True, True]
    assert len(provider.eips) <= 1
    recovered_bindings = await asyncio.gather(
        first_store.get_binding("shared"),
        second_store.get_binding("shared"),
    )
    assert all(
        binding.last_operation != "allocate_eip"
        for binding in recovered_bindings
    )


async def test_allocate_timeout_notifies_and_live_recovery_adopts(tmp_path):
    provider = DryRunProvider()
    store = AccountBindingStore(str(tmp_path / "bindings.json"))
    notified: list[str] = []
    manager = BindingManager(
        provider, store, recovery_notifier=notified.append
    )
    real_allocate = provider.allocate_eip

    async def timeout_after_allocate(*, tags=None):
        await real_allocate(tags=tags)
        raise TimeoutError("response lost")

    provider.allocate_eip = timeout_after_allocate
    with pytest.raises(TimeoutError):
        await manager.ensure_binding("acc-timeout")

    pending = await manager.get_binding("acc-timeout")
    assert pending.last_operation == "allocate_eip"
    assert pending.eip_allocation_id is None
    assert notified == ["acc-timeout"]
    assert await manager.recover_pending_allocation("acc-timeout") is True
    adopted = await manager.get_binding("acc-timeout")
    assert adopted.state == BindingState.READY
    assert adopted.eip_allocation_id is not None


async def test_cancelled_allocate_notifies_and_live_recovery_adopts(tmp_path):
    provider = DryRunProvider()
    store = AccountBindingStore(str(tmp_path / "bindings.json"))
    notified: list[str] = []
    manager = BindingManager(provider, store, recovery_notifier=notified.append)
    real_allocate = provider.allocate_eip

    async def cancel_after_allocate(*, tags=None):
        await real_allocate(tags=tags)
        raise asyncio.CancelledError()

    provider.allocate_eip = cancel_after_allocate
    with pytest.raises(asyncio.CancelledError):
        await manager.ensure_binding("acc-cancelled")

    pending = await manager.get_binding("acc-cancelled")
    assert pending.state == BindingState.ERROR
    assert pending.last_operation == "allocate_eip"
    assert pending.eip_allocation_id is None
    assert notified == ["acc-cancelled"]
    provider.allocate_eip = real_allocate
    assert await manager.recover_pending_allocation("acc-cancelled") is True
    assert (await manager.get_binding("acc-cancelled")).state == BindingState.READY


async def test_adoption_write_failure_keeps_recovery_marker_and_notifies(tmp_path):
    provider = DryRunProvider()
    store = AccountBindingStore(str(tmp_path / "bindings.json"))
    await store.upsert_binding(AccountBinding(
        account_id="acc-adopt",
        controller_id=store.controller_id,
        state=BindingState.ALLOCATING,
        last_operation="allocate_eip",
    ))
    orphan = await provider.allocate_eip(tags={
        "AccountId": "acc-adopt",
        "Role": "account-eip",
        "ElasticAgentController": store.controller_id,
    })
    notified: list[str] = []
    manager = BindingManager(
        provider, store, recovery_notifier=notified.append
    )
    real_update = store.update_binding

    async def fail_ready_write(account_id, **fields):
        if fields.get("eip_allocation_id"):
            raise OSError("fsync failed")
        return await real_update(account_id, **fields)

    store.update_binding = fail_ready_write
    with pytest.raises(OSError, match="fsync"):
        await manager.ensure_binding("acc-adopt")

    pending = await manager.get_binding("acc-adopt")
    assert pending.eip_allocation_id is None
    assert pending.last_operation == "allocate_eip"
    assert notified == ["acc-adopt"]

    store.update_binding = real_update
    assert await manager.recover_pending_allocation("acc-adopt") is True
    assert (await manager.get_binding("acc-adopt")).eip_allocation_id == orphan.allocation_id
