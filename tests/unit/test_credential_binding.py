"""Unit tests for CredentialBinding (T-048)."""

from __future__ import annotations

import asyncio

import pytest

from elastic_agent.core.credential_binding import CredentialBinding


class TestCredentialBinding:
    """Tests for account-worker binding with mutual exclusion and affinity."""

    @pytest.fixture
    def binding(self) -> CredentialBinding:
        return CredentialBinding(max_accounts_per_worker=2)

    # 1. Bind account to worker
    async def test_bind_account_to_worker(self, binding: CredentialBinding):
        result = await binding.bind("acct-1", "worker-A")
        assert result is True
        assert binding.is_bound("acct-1")
        assert binding.get_worker("acct-1") == "worker-A"

    # 2. Bind fails when account already bound (mutual exclusion)
    async def test_bind_fails_mutual_exclusion(self, binding: CredentialBinding):
        await binding.bind("acct-1", "worker-A")
        result = await binding.bind("acct-1", "worker-B")
        assert result is False
        # Account stays bound to original worker
        assert binding.get_worker("acct-1") == "worker-A"

    # 3. Bind fails when worker at max capacity
    async def test_bind_fails_at_max_capacity(self, binding: CredentialBinding):
        await binding.bind("acct-1", "worker-A")
        await binding.bind("acct-2", "worker-A")
        # Worker-A already has 2 accounts (max)
        result = await binding.bind("acct-3", "worker-A")
        assert result is False

    # 4. Unbind account
    async def test_unbind_account(self, binding: CredentialBinding):
        await binding.bind("acct-1", "worker-A")
        worker_id = await binding.unbind("acct-1")
        assert worker_id == "worker-A"
        assert not binding.is_bound("acct-1")
        assert binding.get_worker("acct-1") is None

    # 5. Unbind all accounts from worker
    async def test_unbind_worker(self, binding: CredentialBinding):
        await binding.bind("acct-1", "worker-A")
        await binding.bind("acct-2", "worker-A")
        released = await binding.unbind_worker("worker-A")
        assert set(released) == {"acct-1", "acct-2"}
        assert not binding.is_bound("acct-1")
        assert not binding.is_bound("acct-2")
        assert binding.worker_account_count("worker-A") == 0

    # 6. Check is_bound / get_worker / get_accounts
    async def test_query_methods(self, binding: CredentialBinding):
        assert not binding.is_bound("acct-1")
        assert binding.get_worker("acct-1") is None
        assert binding.get_accounts("worker-A") == set()

        await binding.bind("acct-1", "worker-A")
        await binding.bind("acct-2", "worker-A")

        assert binding.is_bound("acct-1")
        assert binding.get_worker("acct-1") == "worker-A"
        assert binding.get_accounts("worker-A") == {"acct-1", "acct-2"}

    # 7. Can_bind checks max limit
    async def test_can_bind_checks_limit(self, binding: CredentialBinding):
        assert binding.can_bind("worker-A") is True
        await binding.bind("acct-1", "worker-A")
        assert binding.can_bind("worker-A") is True
        await binding.bind("acct-2", "worker-A")
        assert binding.can_bind("worker-A") is False

    # 8. Record and retrieve affinity
    async def test_affinity_tracking(self, binding: CredentialBinding):
        # No affinity yet
        assert binding.get_preferred_worker("acct-1") is None

        await binding.bind("acct-1", "worker-A")
        assert binding.get_preferred_worker("acct-1") == "worker-A"

        # Unbind and record affinity to a different worker
        await binding.unbind("acct-1")
        await binding.record_affinity("acct-1", "worker-B")
        # worker-B should now be preferred (more recent)
        assert binding.get_preferred_worker("acct-1") == "worker-B"

    # 9. Concurrent bind/unbind safety
    async def test_concurrent_bind_unbind(self):
        binding = CredentialBinding(max_accounts_per_worker=3)
        results: list[bool] = []

        async def do_bind(acct: str, worker: str):
            r = await binding.bind(acct, worker)
            results.append(r)

        # Attempt to bind same account to different workers concurrently
        tasks = [
            do_bind("acct-1", f"worker-{i}") for i in range(5)
        ]
        await asyncio.gather(*tasks)

        # Exactly one should succeed due to mutual exclusion
        assert results.count(True) == 1
        # Account must be bound to exactly one worker
        w = binding.get_worker("acct-1")
        assert w is not None
        assert w.startswith("worker-")

    # 10. Worker cleanup releases all bindings
    async def test_worker_cleanup_releases_all(self, binding: CredentialBinding):
        await binding.bind("acct-1", "worker-A")
        await binding.bind("acct-2", "worker-A")

        # Confirm bindings exist
        assert binding.worker_account_count("worker-A") == 2

        released = await binding.unbind_worker("worker-A")
        assert len(released) == 2
        assert binding.worker_account_count("worker-A") == 0
        # Can now bind new accounts to this worker
        assert binding.can_bind("worker-A") is True

        # And the previously bound accounts can be bound elsewhere
        result = await binding.bind("acct-1", "worker-B")
        assert result is True

    # Additional: idempotent bind to same worker
    async def test_bind_idempotent_same_worker(self, binding: CredentialBinding):
        await binding.bind("acct-1", "worker-A")
        result = await binding.bind("acct-1", "worker-A")
        assert result is True
        assert binding.worker_account_count("worker-A") == 1

    # Additional: unbind non-existent account
    async def test_unbind_nonexistent(self, binding: CredentialBinding):
        result = await binding.unbind("no-such-account")
        assert result is None

    # Additional: unbind_worker for non-existent worker
    async def test_unbind_worker_nonexistent(self, binding: CredentialBinding):
        released = await binding.unbind_worker("no-such-worker")
        assert released == []


# ===========================================================================
# T-137: Account-Worker binding extended — mutual exclusion, affinity reuse
#        (Step 1-3), worker offline cleanup, max_accounts_per_worker
# ===========================================================================


class TestT137MutualExclusion:
    """T-137: Mutual exclusion enforcement."""

    async def test_same_account_different_workers_rejected(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        assert await binding.bind("acct-1", "worker-A") is True
        assert await binding.bind("acct-1", "worker-B") is False
        assert binding.get_worker("acct-1") == "worker-A"

    async def test_different_accounts_same_worker_allowed(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        assert await binding.bind("acct-1", "worker-A") is True
        assert await binding.bind("acct-2", "worker-A") is True
        assert binding.get_accounts("worker-A") == {"acct-1", "acct-2"}

    async def test_rebind_after_unbind_to_different_worker(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        await binding.bind("acct-1", "worker-A")
        await binding.unbind("acct-1")
        assert await binding.bind("acct-1", "worker-B") is True
        assert binding.get_worker("acct-1") == "worker-B"


class TestT137AffinityReuse:
    """T-137: Step 1-3 affinity reuse pattern."""

    async def test_step1_bind_creates_affinity(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        await binding.bind("acct-1", "worker-A")
        assert binding.get_preferred_worker("acct-1") == "worker-A"

    async def test_step2_unbind_preserves_affinity(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        await binding.bind("acct-1", "worker-A")
        await binding.unbind("acct-1")
        assert not binding.is_bound("acct-1")
        assert binding.get_preferred_worker("acct-1") == "worker-A"

    async def test_step3_rebind_prefers_affinity_worker(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        await binding.bind("acct-1", "worker-A")
        await binding.unbind("acct-1")
        preferred = binding.get_preferred_worker("acct-1")
        assert preferred == "worker-A"
        await binding.bind("acct-1", preferred)
        assert binding.get_worker("acct-1") == "worker-A"

    async def test_affinity_updates_to_most_recent(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        await binding.bind("acct-1", "worker-A")
        await binding.unbind("acct-1")
        await asyncio.sleep(0.01)
        await binding.record_affinity("acct-1", "worker-B")
        assert binding.get_preferred_worker("acct-1") == "worker-B"

    async def test_multiple_accounts_affinity(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        await binding.bind("acct-1", "worker-A")
        await binding.bind("acct-2", "worker-B")
        assert binding.get_preferred_worker("acct-1") == "worker-A"
        assert binding.get_preferred_worker("acct-2") == "worker-B"

    async def test_no_affinity_returns_none(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        assert binding.get_preferred_worker("never-bound") is None


class TestT137WorkerOfflineCleanup:
    """T-137: Worker offline cleanup."""

    async def test_unbind_worker_releases_all_accounts(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        await binding.bind("a1", "w1")
        await binding.bind("a2", "w1")
        await binding.bind("a3", "w1")
        released = await binding.unbind_worker("w1")
        assert set(released) == {"a1", "a2", "a3"}
        assert binding.worker_account_count("w1") == 0
        for aid in ["a1", "a2", "a3"]:
            assert not binding.is_bound(aid)

    async def test_cleanup_allows_rebinding(self):
        binding = CredentialBinding(max_accounts_per_worker=2)
        await binding.bind("a1", "w1")
        await binding.bind("a2", "w1")
        assert not binding.can_bind("w1")
        await binding.unbind_worker("w1")
        assert binding.can_bind("w1")
        assert await binding.bind("a3", "w1") is True

    async def test_cleanup_preserves_other_workers(self):
        binding = CredentialBinding(max_accounts_per_worker=4)
        await binding.bind("a1", "w1")
        await binding.bind("a2", "w2")
        await binding.unbind_worker("w1")
        assert binding.is_bound("a2")
        assert binding.get_worker("a2") == "w2"


class TestT137MaxAccountsPerWorker:
    """T-137: max_accounts_per_worker enforcement."""

    async def test_max_1(self):
        binding = CredentialBinding(max_accounts_per_worker=1)
        assert await binding.bind("a1", "w1") is True
        assert await binding.bind("a2", "w1") is False

    async def test_max_large(self):
        binding = CredentialBinding(max_accounts_per_worker=100)
        for i in range(50):
            assert await binding.bind(f"a{i}", "w1") is True
        assert binding.worker_account_count("w1") == 50
        assert binding.can_bind("w1") is True

    async def test_max_reached_then_unbind_one(self):
        binding = CredentialBinding(max_accounts_per_worker=2)
        await binding.bind("a1", "w1")
        await binding.bind("a2", "w1")
        assert await binding.bind("a3", "w1") is False
        await binding.unbind("a1")
        assert await binding.bind("a3", "w1") is True
        assert binding.worker_account_count("w1") == 2

    async def test_idempotent_bind_does_not_increase_count(self):
        binding = CredentialBinding(max_accounts_per_worker=2)
        await binding.bind("a1", "w1")
        await binding.bind("a1", "w1")
        assert binding.worker_account_count("w1") == 1
