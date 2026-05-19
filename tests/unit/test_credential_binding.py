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
