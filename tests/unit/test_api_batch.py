"""Tests for the accounts + jobs REST API (frontend backend)."""

from __future__ import annotations

import asyncio
import copy
import hashlib
import io
import json
import os
import stat
import tarfile
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.account_binding import AccountBinding, BindingState
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.credential_pool import AccountDefinition
from elastic_agent.core.providers.base import CloudProvider, Instance, InstanceConfig, InstanceState
from elastic_agent.manager.manager import ElasticAgentManager

API_KEY = "test-api-key-batch"


class InMemoryProvider(CloudProvider):
    def __init__(self):
        self._n = 0

    @property
    def platform(self) -> str:
        return "dryrun"

    async def create_instance(self, config: InstanceConfig) -> Instance:
        self._n += 1
        return Instance(
            instance_id=f"dryrun:i-{self._n}", platform="dryrun", native_id=f"i-{self._n}",
            state=InstanceState.PENDING, public_ip=f"1.2.3.{self._n}", private_ip=f"10.0.0.{self._n}",
            instance_type=config.instance_type, region="test", zone="test-a", tags=config.tags,
        )

    async def terminate_instance(self, instance_id: str) -> None: ...
    async def start_instance(self, instance_id: str) -> None: ...
    async def stop_instance(self, instance_id: str) -> None: ...
    async def reboot_instance(self, instance_id: str) -> None: ...
    async def list_instances(self, filters=None): return []
    async def get_instance(self, instance_id): return None
    async def wait_until_running(self, instance_id, timeout=300): return None


class FakeBatch:
    """Stand-in orchestrator so route logic is tested without live workers."""

    def __init__(self, persist_spec_hook=None):
        self._jobs = {}
        self.started: list[str] = []
        self.before_start = None
        self.persist_spec_hook = persist_spec_hook

    def prepare(self, spec):
        from elastic_agent.core.batch_orchestrator import BatchJob
        from elastic_agent.harness.generic import resolve_harness

        return BatchJob(
            job_id=f"job-{len(self._jobs)}",
            spec=spec,
            harness=resolve_harness(spec),
        )

    async def submit_prepared(self, job):
        from elastic_agent.core.batch_orchestrator import (
            JobSpecPersistenceError,
            WorkerPhase,
            WorkerRun,
        )
        from elastic_agent.core.job_spec import WorkerContext

        if self.persist_spec_hook is not None:
            try:
                await self.persist_spec_hook(
                    job.job_id,
                    job.spec,
                    job.request_fingerprint,
                )
            except Exception as exc:  # noqa: BLE001
                raise JobSpecPersistenceError(
                    f"failed to persist JobSpec before launch: {exc}"
                ) from exc
        if self.before_start is not None:
            self.before_start(job)
        self.started.append(job.job_id)
        for i in range(max(1, job.spec.fanout.workers)):
            job.runs[f"w{i}"] = WorkerRun(
                worker_id=f"w{i}",
                ctx=WorkerContext(shard_index=i),
                phase=WorkerPhase.RUNNING,
            )
        self._jobs[job.job_id] = job
        return job

    async def launch(self, spec):
        return await self.submit_prepared(self.prepare(spec))

    async def submit(self, spec):
        # Route uses submit() (background bring-up); for tests it mirrors launch().
        return await self.launch(spec)

    async def cancel_job(self, job_id, reason="job cancelled"):
        from elastic_agent.core.batch_orchestrator import WorkerPhase

        job = self._jobs.get(job_id)
        if job is None:
            return False
        job.cancel_requested = True
        job.cancel_reason = reason
        job.launch_complete = True
        job.resources_released = True
        for run in job.runs.values():
            run.phase = WorkerPhase.CANCELLED
            run.error = reason
        return True

    async def shutdown(self):
        return None

    def list_jobs(self):
        return list(self._jobs.values())

    def get_job(self, jid):
        return self._jobs.get(jid)


class FakeBindingManager:
    """Route-level stand-in for the durable EIP binding service."""

    def __init__(self):
        self.bindings: dict[str, AccountBinding] = {}
        self.ensure_calls: list[tuple[str, str, str]] = []
        self.decommissioned: list[str] = []
        self.active_accounts: set[str] = set()

    async def list_bindings(self):
        return list(self.bindings.values())

    @asynccontextmanager
    async def account_transaction(self, account_id):
        yield

    async def get_binding(self, account_id):
        return self.bindings.get(account_id)

    async def list_leases(self, *, account_id=None, active_only=False):
        if account_id in self.active_accounts:
            return [{"account_id": account_id, "state": "attached"}]
        return []

    async def ensure_binding(self, account_id, *, email="", region=""):
        self.ensure_calls.append((account_id, email, region))
        if account_id not in self.bindings:
            n = len(self.bindings) + 1
            self.bindings[account_id] = AccountBinding(
                account_id=account_id,
                email=email,
                eip_allocation_id=f"eipalloc-{n}",
                eip_ip=f"198.51.100.{n}",
                region=region or "test-1",
                state=BindingState.READY,
            )
        return self.bindings[account_id]

    async def decommission(self, account_id, *, confirm_absent=False):
        assert confirm_absent is True
        if account_id in self.active_accounts:
            raise RuntimeError(f"account {account_id} has an active lease")
        removed = self.bindings.pop(account_id, None)
        if removed is None:
            return False
        self.decommissioned.append(account_id)
        return True


@pytest.fixture(autouse=True)
def setup_api_keys(monkeypatch):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", API_KEY)
    reset_api_keys()
    yield
    reset_api_keys()


@pytest.fixture
def manager(tmp_path):
    cfg = ElasticAgentConfig()
    cfg.registry.path = str(tmp_path / "registry.json")
    cfg.provider.type = "aws"
    cfg.provider.aws.region = "us-west-2"
    mgr = ElasticAgentManager(cfg, InMemoryProvider())
    binding_manager = FakeBindingManager()
    mgr.binding_manager = binding_manager
    return mgr


@pytest.fixture
async def client(manager):
    app = create_app(manager)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://testserver",
        headers={"Authorization": f"Bearer {API_KEY}"},
    ) as ac:
        await manager.start()
        manager._batch = FakeBatch(
            manager._persist_batch_job_spec
        )  # inject fake orchestrator with the production persistence boundary
        yield ac
        await manager.stop()


class TestAccountsAPI:
    @pytest.mark.asyncio
    async def test_cancelled_job_allocation_remains_until_release_proof(
        self, client, manager,
    ):
        submitted = await client.post(
            "/api/jobs",
            json={
                "name": "cancelled-allocation",
                "run": {"command": "true"},
                "account": {"mode": "none"},
            },
        )
        assert submitted.status_code == 201
        job = manager.batch.get_job(submitted.json()["job_id"])
        run = next(iter(job.runs.values()))
        run.account_ids = ["released-account"]
        run.account_emails = ["released@example.com"]

        cancelled = await client.post(f"/api/jobs/{job.job_id}/cancel")
        pending = await client.get("/api/accounts/allocations")

        assert cancelled.status_code == 200
        assert pending.status_code == 200
        assert pending.json()["allocations"]["released-account"] == [{
            "job_id": job.job_id,
            "job_name": "cancelled-allocation",
            "worker_id": run.worker_id,
            "phase": "cancelled",
            "email": "released@example.com",
            "active": False,
            "cleanup_pending": True,
        }]

        job.accounts_released = True
        released = await client.get("/api/accounts/allocations")
        assert released.status_code == 200
        assert released.json() == {
            "allocations": {},
            "total_accounts_bound": 0,
        }

    @pytest.mark.asyncio
    async def test_allocation_view_fails_closed_when_job_state_is_unavailable(
        self, client, manager, monkeypatch,
    ):
        def fail_list_jobs():
            raise RuntimeError("in-memory job state failed")

        monkeypatch.setattr(manager.batch, "list_jobs", fail_list_jobs)

        response = await client.get("/api/accounts/allocations")

        assert response.status_code == 503
        assert response.json()["detail"] == (
            "Account allocation state is temporarily unavailable"
        )
        assert "in-memory job state failed" not in response.text

    @pytest.mark.asyncio
    async def test_allocation_view_fails_closed_when_leases_are_unavailable(
        self, client, manager, monkeypatch,
    ):
        monkeypatch.setattr(
            manager.binding_manager,
            "list_leases",
            AsyncMock(side_effect=RuntimeError("durable lease read failed")),
        )

        response = await client.get("/api/accounts/allocations")

        assert response.status_code == 503
        assert response.json()["detail"] == (
            "Durable account allocation state is temporarily unavailable"
        )
        assert "durable lease read failed" not in response.text

    @pytest.mark.asyncio
    async def test_cloudrouter_agent_api_account_is_write_only_and_shared(
        self, client, manager, monkeypatch,
    ):
        adapter = manager.agent_api_store.registry.require("cloudrouter")
        key = "cloudrouter-key-that-must-never-echo"
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(return_value={
                "claude": ["claude-opus-4-8"],
                "codex": ["gpt-5.4"],
            }),
        )
        monkeypatch.setattr(
            adapter,
            "fetch_usage",
            AsyncMock(return_value={
                "account_id": "cloudrouter-1",
                "state": "active",
                "status": "active",
                "known": True,
                "available": True,
                "reason": "active",
                "mode": "wallet",
                "windows": [],
            }),
        )

        created = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "Shared CloudRouter",
            "group": "research",
            "api_key": key,
        })

        assert created.status_code == 201
        body = created.json()
        assert body["id"] == "cloudrouter-1"
        assert body["auth_kind"] == "agent_api"
        assert body["agent_type"] is None
        assert body["supported_agent_types"] == ["claude", "codex"]
        assert body["has_api_key"] is True
        assert body["api_usage"]["available"] is True
        assert key not in created.text
        assert "api_key" not in body

        combined = (await client.get("/api/accounts")).json()
        assert combined["total"] == 1
        assert combined["accounts"][0]["id"] == "cloudrouter-1"
        assert combined["accounts"][0]["supported_agent_types"] == [
            "claude",
            "codex",
        ]
        assert key not in json.dumps(combined)

        provider_accounts = (
            await client.get("/api/agent-api/accounts")
        ).json()
        assert provider_accounts["total"] == 1
        assert key not in json.dumps(provider_accounts)
        for agent_type in ("claude", "codex"):
            plan = await client.post("/api/jobs/plan", json={
                "name": f"cloudrouter-{agent_type}",
                "run": {"command": "true"},
                "account": {
                    "agent_type": agent_type,
                    "ids": ["cloudrouter-1"],
                    "binding": "none",
                },
            })
            assert plan.status_code == 200
            assert plan.json()["valid"] is True
            assert key not in plan.text
        incompatible = await client.post("/api/jobs/plan", json={
            "name": "cloudrouter-unsupported-model",
            "run": {"command": "true"},
            "account": {
                "agent_type": "codex",
                "model": "gpt-not-on-router",
                "ids": ["cloudrouter-1"],
                "binding": "none",
            },
        })
        assert incompatible.status_code == 422
        assert "gpt-not-on-router" in incompatible.json()["detail"]
        key_file = (
            Path(manager.config.registry.path).with_name("agent-api-accounts")
            / "cloudrouter-1"
            / "api.key"
        )
        assert key_file.read_text() == key
        assert stat.S_IMODE(key_file.stat().st_mode) == 0o600

        removed = await client.delete(
            "/api/agent-api/accounts/cloudrouter-1"
        )
        assert removed.status_code == 200
        assert removed.json() == {
            "account_id": "cloudrouter-1",
            "status": "removed",
        }
        assert not key_file.exists()

    @pytest.mark.asyncio
    async def test_agent_api_delete_rejects_active_claim_then_succeeds(
        self, client, manager, monkeypatch,
    ):
        adapter = manager.agent_api_store.registry.require("cloudrouter")
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(return_value={"claude": ["claude-opus-4-8"]}),
        )
        monkeypatch.setattr(
            adapter,
            "fetch_usage",
            AsyncMock(return_value={
                "account_id": "cloudrouter-1",
                "known": True,
                "available": True,
                "reason": "active",
                "windows": [],
            }),
        )
        created = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "Shared",
            "api_key": "private-key",
        })
        assert created.status_code == 201
        first_claim = await manager.account_allocator.reserve(
            "job-1:w1",
            "standard",
            account_id="cloudrouter-1",
            agent_type="claude",
            allow_shared_agent_api=True,
        )
        second_claim = await manager.account_allocator.reserve(
            "job-2:w2",
            "standard",
            account_id="cloudrouter-1",
            agent_type="claude",
            allow_shared_agent_api=True,
        )
        assert first_claim is not None
        assert second_claim is not None

        blocked = await client.delete(
            "/api/agent-api/accounts/cloudrouter-1"
        )
        assert blocked.status_code == 409

        await manager.account_allocator.release_claim(first_claim.claim_id)
        still_blocked = await client.delete(
            "/api/agent-api/accounts/cloudrouter-1"
        )
        assert still_blocked.status_code == 409

        await manager.account_allocator.release_claim(second_claim.claim_id)
        removed = await client.delete(
            "/api/agent-api/accounts/cloudrouter-1"
        )
        assert removed.status_code == 200

    @pytest.mark.asyncio
    async def test_cloudrouter_invalid_key_errors_are_sanitized(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.core.agent_api import AgentApiUpstreamError

        adapter = manager.agent_api_store.registry.require("cloudrouter")
        key = "rejected-key-that-must-not-echo"
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(
                side_effect=AgentApiUpstreamError("invalid_api_key", 401)
            ),
        )

        rejected = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "Rejected",
            "api_key": key,
        })
        malformed = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "Malformed",
            "api_key": {"secret": key},
        })
        unknown = await client.post("/api/agent-api/accounts", json={
            "provider": "unregistered",
            "name": "Not implemented",
            "api_key": key,
        })

        assert rejected.status_code == 422
        assert rejected.json()["detail"] == "CloudRouter rejected the API key"
        assert malformed.status_code == 422
        assert unknown.status_code == 422
        assert key not in rejected.text
        assert key not in malformed.text
        assert key not in unknown.text
        assert await manager.agent_api_store.list() == []

    @pytest.mark.asyncio
    async def test_apex_agent_api_account_is_codex_only_and_write_only(
        self, client, manager, monkeypatch,
    ):
        adapter = manager.agent_api_store.registry.require("apex")
        key = "apex-key-that-must-never-echo"
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(return_value={
                "claude": [],
                "codex": ["gpt-5.4"],
            }),
        )
        monkeypatch.setattr(
            adapter,
            "fetch_usage",
            AsyncMock(return_value={
                "account_id": "apex-1",
                "state": "active",
                "status": "active",
                "known": True,
                "available": True,
                "reason": "active",
                "mode": "fixed_windows",
                "key_used": "3",
                "windows": [],
            }),
        )

        providers = await client.get("/api/agent-api/providers")
        assert providers.status_code == 200
        assert providers.json()["providers"] == ["cloudrouter", "apex"]

        created = await client.post("/api/agent-api/accounts", json={
            "provider": "apex",
            "name": "Apex Codex",
            "group": "research",
            "api_key": key,
        })

        assert created.status_code == 201
        body = created.json()
        assert body["id"] == "apex-1"
        assert body["api_provider"] == "apex"
        assert body["auth_kind"] == "agent_api"
        assert body["supported_agent_types"] == ["codex"]
        assert body["models"] == {"claude": [], "codex": ["gpt-5.4"]}
        assert body["has_api_key"] is True
        assert "api_key" not in body
        assert key not in created.text

        codex_plan = await client.post("/api/jobs/plan", json={
            "name": "apex-codex",
            "run": {"command": "true"},
            "account": {
                "agent_type": "codex",
                "ids": ["apex-1"],
                "binding": "none",
            },
        })
        claude_plan = await client.post("/api/jobs/plan", json={
            "name": "apex-claude",
            "run": {"command": "true"},
            "account": {
                "agent_type": "claude",
                "ids": ["apex-1"],
                "binding": "none",
            },
        })
        assert codex_plan.status_code == 200
        assert claude_plan.status_code == 422
        assert "supports codex, not claude" in claude_plan.json()["detail"]

        provider_accounts = await client.get("/api/agent-api/accounts")
        assert provider_accounts.status_code == 200
        assert key not in provider_accounts.text
        key_file = (
            Path(manager.config.registry.path).with_name("agent-api-accounts")
            / "apex-1"
            / "api.key"
        )
        assert key_file.read_text() == key
        assert stat.S_IMODE(key_file.stat().st_mode) == 0o600

    @pytest.mark.asyncio
    async def test_apex_invalid_key_error_is_provider_specific_and_sanitized(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.core.agent_api import AgentApiUpstreamError

        adapter = manager.agent_api_store.registry.require("apex")
        key = "rejected-apex-key-that-must-not-echo"
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(
                side_effect=AgentApiUpstreamError("invalid_api_key", 401)
            ),
        )

        rejected = await client.post("/api/agent-api/accounts", json={
            "provider": "apex",
            "name": "Rejected Apex",
            "api_key": key,
        })

        assert rejected.status_code == 422
        assert rejected.json()["detail"] == "ApexRouter rejected the API key"
        assert key not in rejected.text
        assert await manager.agent_api_store.list() == []

    @pytest.mark.asyncio
    async def test_native_account_cannot_use_agent_api_reserved_id(
        self, client,
    ):
        response = await client.post("/api/accounts", json={
            "id": "cloudrouter-1",
            "email": "native@example.com",
        })
        assert response.status_code == 409
        assert "reserved" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_agent_api_id_skips_legacy_native_collision(
        self, client, manager, monkeypatch,
    ):
        await manager.account_store.add(AccountDefinition(
            id="cloudrouter-1",
            email="legacy-native@example.com",
        ))
        adapter = manager.agent_api_store.registry.require("cloudrouter")
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(return_value={
                "claude": ["claude-opus-4-8"],
                "codex": [],
            }),
        )
        monkeypatch.setattr(
            adapter,
            "fetch_usage",
            AsyncMock(return_value={
                "account_id": "cloudrouter-2",
                "state": "active",
                "status": "active",
                "known": True,
                "available": True,
                "reason": "active",
                "mode": "wallet",
                "windows": [],
            }),
        )

        created = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "API after legacy row",
            "api_key": "cloudrouter-private",
        })

        assert created.status_code == 201
        assert created.json()["id"] == "cloudrouter-2"
        combined = await client.get("/api/accounts")
        assert combined.status_code == 200
        assert {row["id"] for row in combined.json()["accounts"]} == {
            "cloudrouter-1",
            "cloudrouter-2",
        }

    @pytest.mark.asyncio
    async def test_legacy_native_reserved_id_can_still_be_updated(
        self, client, manager,
    ):
        await manager.account_store.add(AccountDefinition(
            id="cloudrouter-1",
            email="legacy-native@example.com",
            group="legacy",
        ))

        updated = await client.post("/api/accounts", json={
            "id": "cloudrouter-1",
            "email": "legacy-native@example.com",
            "group": "updated",
        })

        assert updated.status_code == 201
        assert updated.json()["auth_kind"] == "oauth"
        assert updated.json()["group"] == "updated"

    @pytest.mark.asyncio
    async def test_add_list_remove(self, client):
        assert (await client.get("/api/accounts")).json()["total"] == 0

        r = await client.post("/api/accounts", json={
            "id": "acc-1", "email": "a@x.com", "email_token": "tok", "group": "prod",
        })
        assert r.status_code == 201
        assert r.json()["email"] == "a@x.com"
        assert "email_token" not in r.json()
        assert r.json()["has_email_token"] is True

        lst = (await client.get("/api/accounts")).json()
        assert lst["total"] == 1
        assert lst["accounts"][0]["id"] == "acc-1"
        assert "email_token" not in lst["accounts"][0]

        r = await client.delete("/api/accounts/acc-1")
        assert r.status_code == 200
        assert (await client.get("/api/accounts")).json()["total"] == 0

    @pytest.mark.asyncio
    async def test_codex_password_is_write_only_and_persisted_mode_0600(
        self, client, manager
    ):
        created = await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "first-password",
            "email_token": "mail-secret",
        })

        assert created.status_code == 201
        assert created.json()["agent_type"] == "codex"
        assert created.json()["has_password"] is True
        assert created.json()["has_email_token"] is True
        assert "password" not in created.json()
        assert "email_token" not in created.json()

        listed = (await client.get("/api/accounts")).json()["accounts"][0]
        assert listed["has_password"] is True
        assert "password" not in listed
        path = Path(manager.config.registry.path).with_name("accounts.json")
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["accounts"][0]["password"] == "first-password"

    @pytest.mark.asyncio
    async def test_empty_secrets_preserve_existing_and_nonempty_values_rotate(
        self, client, manager
    ):
        await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "first-password",
            "email_token": "first-mail-token",
        })

        preserved = await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "",
            "email_token": "",
            "group": "new-group",
        })
        assert preserved.status_code == 201
        stored = await manager.account_store.get("codex-1")
        assert stored.password == "first-password"
        assert stored.email_token == "first-mail-token"
        assert stored.group == "new-group"

        rotated = await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "second-password",
            "email_token": "second-mail-token",
        })
        assert rotated.status_code == 201
        stored = await manager.account_store.get("codex-1")
        assert stored.password == "second-password"
        assert stored.email_token == "second-mail-token"

        cleared = await client.post("/api/accounts", json={
            "id": "codex-1",
            "agent_type": "codex",
            "email": "codex@example.com",
            "password": "",
            "email_token": "",
            "clear_email_token": True,
        })
        assert cleared.status_code == 201
        stored = await manager.account_store.get("codex-1")
        assert stored.password == "second-password"
        assert stored.email_token == ""
        assert cleared.json()["has_email_token"] is False

    @pytest.mark.asyncio
    async def test_codex_email_token_without_password_is_write_only(
        self, client, manager
    ):
        response = await client.post("/api/accounts", json={
            "id": "codex-token-only",
            "agent_type": "codex",
            "email": "codex@example.com",
            "email_token": "mail-token-that-must-not-echo",
        })

        assert response.status_code == 201
        assert response.json()["has_email_token"] is True
        assert response.json()["has_password"] is False
        assert "mail-token-that-must-not-echo" not in response.text
        assert "email_token" not in response.json()
        assert "password" not in response.json()
        stored = await manager.account_store.get("codex-token-only")
        assert stored.email_token == "mail-token-that-must-not-echo"
        assert stored.password == ""

    @pytest.mark.asyncio
    async def test_clear_password_switches_codex_account_to_token_only(
        self, client, manager
    ):
        created = await client.post("/api/accounts", json={
            "id": "codex-both",
            "agent_type": "codex",
            "email": "codex-both@example.com",
            "password": "password-that-must-be-removed",
            "email_token": "mail-token-that-must-remain",
        })
        assert created.status_code == 201

        cleared = await client.post("/api/accounts", json={
            "id": "codex-both",
            "agent_type": "codex",
            "email": "codex-both@example.com",
            "clear_password": True,
        })

        assert cleared.status_code == 201
        assert cleared.json()["has_password"] is False
        assert cleared.json()["has_email_token"] is True
        stored = await manager.account_store.get("codex-both")
        assert stored.password == ""
        assert stored.email_token == "mail-token-that-must-remain"

    @pytest.mark.asyncio
    async def test_codex_without_email_token_or_password_is_rejected(self, client):
        response = await client.post("/api/accounts", json={
            "id": "codex-no-credentials",
            "agent_type": "codex",
            "email": "codex@example.com",
        })

        assert response.status_code == 409
        assert response.json()["detail"] == (
            "Codex accounts require an email token or OpenAI password"
        )

    @pytest.mark.asyncio
    async def test_malformed_secret_validation_never_echoes_input(self, client):
        token_marker = "mail-token-that-must-not-echo-from-422"
        password_marker = "password-that-must-not-echo-from-422"
        response = await client.post("/api/accounts", json={
            "id": "codex-malformed",
            "agent_type": "codex",
            "email": "codex@example.com",
            "email_token": {"secret": token_marker},
            "password": [password_marker],
        })

        assert response.status_code == 422
        assert token_marker not in response.text
        assert password_marker not in response.text

    @pytest.mark.asyncio
    async def test_cannot_clear_only_codex_login_credential(
        self, client, manager
    ):
        created = await client.post("/api/accounts", json={
            "id": "codex-token-only",
            "agent_type": "codex",
            "email": "codex@example.com",
            "email_token": "mail-token-that-must-survive",
        })
        assert created.status_code == 201

        cleared = await client.post("/api/accounts", json={
            "id": "codex-token-only",
            "agent_type": "codex",
            "email": "codex@example.com",
            "clear_email_token": True,
        })

        assert cleared.status_code == 409
        assert "mail-token-that-must-survive" not in cleared.text
        stored = await manager.account_store.get("codex-token-only")
        assert stored.email_token == "mail-token-that-must-survive"

        created = await client.post("/api/accounts", json={
            "id": "codex-password-only",
            "agent_type": "codex",
            "email": "password-only@example.com",
            "password": "password-that-must-survive",
        })
        assert created.status_code == 201

        cleared = await client.post("/api/accounts", json={
            "id": "codex-password-only",
            "agent_type": "codex",
            "email": "password-only@example.com",
            "clear_password": True,
        })

        assert cleared.status_code == 409
        assert "password-that-must-survive" not in cleared.text
        stored = await manager.account_store.get("codex-password-only")
        assert stored.password == "password-that-must-survive"

    @pytest.mark.asyncio
    async def test_same_email_is_unique_per_agent_type(self, client):
        claude = await client.post("/api/accounts", json={
            "id": "claude-a", "email": "User@example.com", "agent_type": "claude",
        })
        codex = await client.post("/api/accounts", json={
            "id": "codex-a", "email": "user@EXAMPLE.com", "agent_type": "codex",
            "password": "codex-password-a",
        })
        duplicate = await client.post("/api/accounts", json={
            "id": "codex-b", "email": "USER@example.com", "agent_type": "codex",
            "password": "codex-password-b",
        })

        assert claude.status_code == codex.status_code == 201
        assert duplicate.status_code == 409

    @pytest.mark.asyncio
    async def test_active_job_claim_blocks_identity_edit_and_delete(
        self, client, manager,
    ):
        created = await client.post(
            "/api/accounts",
            json={"id": "claimed", "email": "old@x.com"},
        )
        assert created.status_code == 201
        claim = await manager.account_allocator.reserve(
            "job-live:0", "standard", account_id="claimed"
        )
        assert claim is not None

        edited = await client.post(
            "/api/accounts",
            json={"id": "claimed", "email": "new@x.com"},
        )
        deleted = await client.delete("/api/accounts/claimed")

        assert edited.status_code == 409
        assert deleted.status_code == 409
        assert "actively claimed" in edited.json()["detail"]
        stored = await manager.account_store.get("claimed")
        assert stored.email == "old@x.com"

        await manager.account_allocator.release_claim(claim.claim_id)
        edited = await client.post(
            "/api/accounts",
            json={"id": "claimed", "email": "new@x.com"},
        )
        assert edited.status_code == 201

    @pytest.mark.asyncio
    async def test_active_job_claim_blocks_manual_binding_ensure(
        self, client, manager,
    ):
        created = await client.post(
            "/api/accounts",
            json={"id": "claimed", "email": "claimed@x.com"},
        )
        assert created.status_code == 201
        claim = await manager.account_allocator.reserve(
            "job-live:0", "standard", account_id="claimed"
        )
        assert claim is not None

        blocked = await client.put("/api/accounts/claimed/binding")

        assert blocked.status_code == 409
        assert "actively claimed" in blocked.json()["detail"]
        assert manager.binding_manager.ensure_calls == []

        await manager.account_allocator.release_claim(claim.claim_id)
        ensured = await client.put("/api/accounts/claimed/binding")
        assert ensured.status_code == 200
        assert manager.binding_manager.ensure_calls == [
            ("claimed", "claimed@x.com", "us-west-2"),
        ]

    @pytest.mark.asyncio
    async def test_remove_missing_404(self, client):
        assert (await client.delete("/api/accounts/nope")).status_code == 404

    @pytest.mark.asyncio
    async def test_upsert_replaces(self, client):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        await client.post("/api/accounts", json={"id": "a", "email": "two@x.com"})
        lst = (await client.get("/api/accounts")).json()
        assert lst["total"] == 1
        assert lst["accounts"][0]["email"] == "two@x.com"

    @pytest.mark.asyncio
    async def test_duplicate_email_casefold_is_rejected(self, client):
        assert (await client.post(
            "/api/accounts", json={"id": "a", "email": "User@x.com"}
        )).status_code == 201
        duplicate = await client.post(
            "/api/accounts", json={"id": "b", "email": "user@X.com"}
        )
        assert duplicate.status_code == 409

    @pytest.mark.asyncio
    async def test_bound_identity_email_is_immutable_but_token_can_rotate(
        self, client
    ):
        await client.post(
            "/api/accounts",
            json={"id": "a", "email": "one@x.com", "email_token": "old"},
        )
        await client.put("/api/accounts/a/binding")

        changed = await client.post(
            "/api/accounts", json={"id": "a", "email": "two@x.com"}
        )
        assert changed.status_code == 409

        changed_agent = await client.post(
            "/api/accounts",
            json={
                "id": "a",
                "email": "one@x.com",
                "agent_type": "codex",
                "password": "openai-password",
            },
        )
        assert changed_agent.status_code == 409

        rotated = await client.post(
            "/api/accounts",
            json={"id": "a", "email": "ONE@x.com", "email_token": "new"},
        )
        assert rotated.status_code == 201
        assert "email_token" not in rotated.json()

    @pytest.mark.asyncio
    async def test_ensure_get_and_list_eip_binding_is_idempotent(self, client, manager):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})

        first = await client.put(
            "/api/accounts/a/binding", json={"region": "us-west-2"},
        )
        second = await client.put(
            "/api/accounts/a/binding", json={"region": "us-west-2"},
        )
        assert first.status_code == second.status_code == 200
        assert first.json()["eip_allocation_id"] == "eipalloc-1"
        assert second.json()["eip_ip"] == "198.51.100.1"
        assert manager.binding_manager.ensure_calls == [
            ("a", "one@x.com", "us-west-2"),
            ("a", "one@x.com", "us-west-2"),
        ]

        got = await client.get("/api/accounts/a/binding")
        assert got.status_code == 200
        assert got.json()["state"] == BindingState.READY
        listed = (await client.get("/api/accounts/bindings")).json()
        assert listed["total"] == 1
        assert listed["bindings"][0]["account_id"] == "a"

    @pytest.mark.asyncio
    async def test_ensure_requires_known_enabled_account(self, client):
        assert (await client.put("/api/accounts/nope/binding")).status_code == 404
        await client.post(
            "/api/accounts",
            json={"id": "off", "email": "off@x.com", "enabled": False},
        )
        assert (await client.put("/api/accounts/off/binding")).status_code == 409

    @pytest.mark.asyncio
    async def test_ensure_uses_configured_region_and_rejects_other_region(
        self, client, manager,
    ):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        ensured = await client.put("/api/accounts/a/binding")
        assert ensured.status_code == 200
        assert ensured.json()["region"] == "us-west-2"
        assert manager.binding_manager.ensure_calls == [
            ("a", "one@x.com", "us-west-2"),
        ]

        mismatch = await client.put(
            "/api/accounts/a/binding", json={"region": "us-east-1"},
        )
        assert mismatch.status_code == 409
        assert manager.binding_manager.ensure_calls == [
            ("a", "one@x.com", "us-west-2"),
        ]

    @pytest.mark.asyncio
    async def test_ensure_rejects_non_aws_provider(self, client, manager):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        manager.config.provider.type = "aliyun"
        response = await client.put("/api/accounts/a/binding")
        assert response.status_code == 501
        assert manager.binding_manager.ensure_calls == []

    @pytest.mark.asyncio
    async def test_binding_get_missing_404(self, client):
        assert (await client.get("/api/accounts/nope/binding")).status_code == 404

    @pytest.mark.asyncio
    async def test_delete_bound_identity_requires_explicit_decommission(
        self, client, manager,
    ):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        await client.put("/api/accounts/a/binding")

        blocked = await client.delete("/api/accounts/a")
        assert blocked.status_code == 409
        assert "decommission" in blocked.json()["detail"]
        assert await manager.account_store.get("a") is not None
        assert await manager.binding_manager.get_binding("a") is not None

    @pytest.mark.asyncio
    async def test_decommission_requires_double_confirmation(self, client, manager):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        await client.put("/api/accounts/a/binding")

        missing = await client.post("/api/accounts/a/binding/decommission", json={})
        refused = await client.post(
            "/api/accounts/a/binding/decommission",
            json={"release_eip": False, "confirm_account_id": "a"},
        )
        mismatch = await client.post(
            "/api/accounts/a/binding/decommission",
            json={"release_eip": True, "confirm_account_id": "someone-else"},
        )
        assert missing.status_code == 422
        assert refused.status_code == 422
        assert mismatch.status_code == 400
        assert await manager.binding_manager.get_binding("a") is not None

    @pytest.mark.asyncio
    async def test_decommission_rejects_active_lease_then_releases_eip(
        self, client, manager,
    ):
        await client.post("/api/accounts", json={"id": "a", "email": "one@x.com"})
        await client.put("/api/accounts/a/binding")
        manager.binding_manager.active_accounts.add("a")
        body = {"release_eip": True, "confirm_account_id": "a"}

        active = await client.post(
            "/api/accounts/a/binding/decommission", json=body,
        )
        assert active.status_code == 409
        assert "active lease" in active.json()["detail"]

        manager.binding_manager.active_accounts.clear()
        released = await client.post(
            "/api/accounts/a/binding/decommission", json=body,
        )
        assert released.status_code == 200
        assert released.json() == {
            "account_id": "a",
            "status": "decommissioned",
            "eip_released": True,
            "identity_removed": False,
        }
        assert manager.binding_manager.decommissioned == ["a"]
        assert (await client.get("/api/accounts/a/binding")).status_code == 404
        # Identity CRUD stays separate and becomes available after explicit
        # infrastructure decommissioning.
        assert (await client.delete("/api/accounts/a")).status_code == 200

    @pytest.mark.asyncio
    async def test_decommission_can_atomically_remove_identity(
        self, client, manager,
    ):
        await client.post(
            "/api/accounts",
            json={"id": "a", "email": "one@x.com"},
        )
        await client.put("/api/accounts/a/binding")

        retired = await client.post(
            "/api/accounts/a/binding/decommission",
            json={
                "release_eip": True,
                "confirm_account_id": "a",
                "delete_identity": True,
            },
        )

        assert retired.status_code == 200
        assert retired.json() == {
            "account_id": "a",
            "status": "decommissioned_and_removed",
            "eip_released": True,
            "identity_removed": True,
        }
        assert await manager.account_store.get("a") is None
        assert await manager.binding_manager.get_binding("a") is None

    @pytest.mark.asyncio
    async def test_decommission_can_atomically_remove_agent_api_identity(
        self, client, manager, monkeypatch,
    ):
        adapter = manager.agent_api_store.registry.require("cloudrouter")
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(return_value={"claude": ["claude-opus-4-8"]}),
        )
        monkeypatch.setattr(
            adapter,
            "fetch_usage",
            AsyncMock(return_value={
                "account_id": "cloudrouter-1",
                "known": True,
                "available": True,
                "reason": "active",
                "windows": [],
            }),
        )
        created = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "Retired API key",
            "api_key": "private-key",
        })
        account_id = created.json()["id"]
        assert (
            await client.put(f"/api/accounts/{account_id}/binding")
        ).status_code == 200

        retired = await client.post(
            f"/api/accounts/{account_id}/binding/decommission",
            json={
                "release_eip": True,
                "confirm_account_id": account_id,
                "delete_identity": True,
            },
        )

        assert retired.status_code == 200
        assert retired.json() == {
            "account_id": account_id,
            "status": "decommissioned_and_removed",
            "eip_released": True,
            "identity_removed": True,
        }
        assert await manager.agent_api_store.get(account_id) is None
        assert await manager.binding_manager.get_binding(account_id) is None

    @pytest.mark.asyncio
    async def test_agent_api_atomic_retirement_waits_for_startup_recovery(
        self, client, manager, monkeypatch,
    ):
        adapter = manager.agent_api_store.registry.require("cloudrouter")
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(return_value={"claude": ["claude-opus-4-8"]}),
        )
        monkeypatch.setattr(
            adapter,
            "fetch_usage",
            AsyncMock(return_value={
                "account_id": "cloudrouter-1",
                "known": True,
                "available": True,
                "reason": "active",
                "windows": [],
            }),
        )
        created = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "Recovery fenced key",
            "api_key": "private-key",
        })
        account_id = created.json()["id"]
        assert (
            await client.put(f"/api/accounts/{account_id}/binding")
        ).status_code == 200
        manager._binding_recovery_ready = False

        blocked = await client.post(
            f"/api/accounts/{account_id}/binding/decommission",
            json={
                "release_eip": True,
                "confirm_account_id": account_id,
                "delete_identity": True,
            },
        )

        assert blocked.status_code == 409
        assert "startup resource recovery" in blocked.json()["detail"]
        assert await manager.agent_api_store.get(account_id) is not None
        assert await manager.binding_manager.get_binding(account_id) is not None

    @pytest.mark.asyncio
    async def test_atomic_retirement_closes_decommission_claim_gap(
        self, client, manager,
    ):
        await client.post(
            "/api/accounts",
            json={"id": "a", "email": "one@x.com"},
        )
        await client.put("/api/accounts/a/binding")
        decommission_entered = asyncio.Event()
        allow_decommission = asyncio.Event()
        original_decommission = manager.binding_manager.decommission

        async def blocking_decommission(account_id, *, confirm_absent=False):
            decommission_entered.set()
            await allow_decommission.wait()
            return await original_decommission(
                account_id,
                confirm_absent=confirm_absent,
            )

        manager.binding_manager.decommission = blocking_decommission
        retirement = asyncio.create_task(client.post(
            "/api/accounts/a/binding/decommission",
            json={
                "release_eip": True,
                "confirm_account_id": "a",
                "delete_identity": True,
            },
        ))
        await decommission_entered.wait()
        competing_claim = asyncio.create_task(
            manager.account_allocator.reserve(
                "new-job:0",
                "standard",
                account_id="a",
            )
        )
        await asyncio.sleep(0)
        assert competing_claim.done() is False

        allow_decommission.set()
        retired = await retirement
        claim = await competing_claim

        assert retired.status_code == 200
        assert retired.json()["identity_removed"] is True
        assert claim is None

    @pytest.mark.asyncio
    async def test_cancelled_retirement_settles_identity_removal_before_unfence(
        self, client, manager,
    ):
        await client.post(
            "/api/accounts",
            json={"id": "a", "email": "one@x.com"},
        )
        await client.put("/api/accounts/a/binding")
        removal_entered = asyncio.Event()
        allow_removal = asyncio.Event()
        original_remove = manager.account_store.remove

        async def blocking_remove(account_id):
            removal_entered.set()
            await allow_removal.wait()
            return await original_remove(account_id)

        manager.account_store.remove = blocking_remove
        retirement = asyncio.create_task(client.post(
            "/api/accounts/a/binding/decommission",
            json={
                "release_eip": True,
                "confirm_account_id": "a",
                "delete_identity": True,
            },
        ))
        await removal_entered.wait()
        retirement.cancel()
        competing_claim = asyncio.create_task(
            manager.account_allocator.reserve(
                "new-job:0",
                "standard",
                account_id="a",
            )
        )
        await asyncio.sleep(0)
        assert competing_claim.done() is False

        allow_removal.set()
        retired = await retirement
        claim = await competing_claim

        assert retired.status_code == 200
        assert retired.json()["identity_removed"] is True
        assert claim is None

    @pytest.mark.asyncio
    async def test_decommission_missing_binding_404(self, client):
        response = await client.post(
            "/api/accounts/nope/binding/decommission",
            json={"release_eip": True, "confirm_account_id": "nope"},
        )
        assert response.status_code == 404


class TestJobsAPI:
    _SPEC = {
        "name": "ai4sci",
        "run": {"command": "uv run bench --shard {{shard_index}}"},
        "fanout": {"workers": 3},
        "account": {"mode": "none"},
    }

    @staticmethod
    def _idempotent_job_id(key: str) -> str:
        return "job-" + hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]

    def _seed_job_journal(self, manager, key: str, state: str = "prepared") -> str:
        from elastic_agent.core.job_spec import JobSpec
        from elastic_agent.core.job_spec_store import persist_job_spec, update_job_state

        job_id = self._idempotent_job_id(key)
        persist_job_spec(
            manager.config.registry.path, job_id, JobSpec.model_validate(self._SPEC),
        )
        if state != "prepared":
            update_job_state(
                manager.config.registry.path,
                job_id,
                state,
                summary={"workers": 3, "phases": {state: 3}},
            )
        return job_id

    @pytest.mark.asyncio
    async def test_submit_returns_detail(self, client, manager):
        r = await client.post("/api/jobs", json=self._SPEC)
        assert r.status_code == 201
        body = r.json()
        assert body["workers"] == 3
        assert len(body["workers_detail"]) == 3
        assert all(w["phase"] == "running" for w in body["workers_detail"])
        assert all(w["worker_released"] is False for w in body["workers_detail"])
        assert all(
            w["worker_release_expected"] is True for w in body["workers_detail"]
        )

        job = manager.batch.get_job(body["job_id"])
        first_run = next(iter(job.runs.values()))
        first_run.cleaned_up = True
        partially_released = (
            await client.get(f"/api/jobs/{body['job_id']}")
        ).json()
        assert [
            worker["worker_released"]
            for worker in partially_released["workers_detail"]
        ] == [True, False, False]
        first_run.cleaned_up = False

        job.resources_released = True
        released = (await client.get(f"/api/jobs/{body['job_id']}")).json()
        assert all(w["worker_released"] is True for w in released["workers_detail"])

        job.resources_released = False
        job.release_workers_on_complete = False
        retained = (await client.get(f"/api/jobs/{body['job_id']}")).json()
        assert all(
            w["worker_release_expected"] is False
            for w in retained["workers_detail"]
        )
        assert all(w["worker_released"] is False for w in retained["workers_detail"])

        job.spec.account.binding = "eip"
        next(iter(job.runs.values())).cleaned_up = True
        eip = (await client.get(f"/api/jobs/{body['job_id']}")).json()
        assert all(
            w["worker_release_expected"] is True for w in eip["workers_detail"]
        )
        assert [w["worker_released"] for w in eip["workers_detail"]] == [
            True,
            False,
            False,
        ]

    @pytest.mark.asyncio
    async def test_non_eip_agent_api_key_can_fill_multiple_worker_slots(
        self, client, manager, monkeypatch,
    ):
        adapter = manager.agent_api_store.registry.require("cloudrouter")
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(return_value={"claude": ["claude-opus-4-8"]}),
        )
        monkeypatch.setattr(
            adapter,
            "fetch_usage",
            AsyncMock(return_value={
                "account_id": "cloudrouter-1",
                "state": "active",
                "known": True,
                "available": True,
                "reason": "active",
            }),
        )
        created = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "Shared key",
            "group": "shared",
            "api_key": "shared-cloudrouter-key",
        })
        assert created.status_code == 201
        account_id = created.json()["id"]
        automatic = {
            "name": "shared-api-auto",
            "run": {"command": "true"},
            "fanout": {"workers": 2},
            "account": {
                "agent_type": "claude",
                "group": "shared",
                "binding": "none",
            },
        }
        explicit = {
            **automatic,
            "name": "shared-api-explicit",
            "account": {
                **automatic["account"],
                "ids": [account_id, account_id],
            },
        }

        plan = await client.post("/api/jobs/plan", json=automatic)
        submitted = await client.post("/api/jobs", json=explicit)
        insufficient_distinct_slots = await client.post(
            "/api/jobs/plan",
            json={
                **automatic,
                "name": "shared-api-two-slots-one-worker",
                "fanout": {"workers": 1},
                "account": {
                    **automatic["account"],
                    "per_worker": 2,
                },
            },
        )

        assert plan.status_code == 200
        assert submitted.status_code == 201
        assert submitted.json()["workers"] == 2
        assert insufficient_distinct_slots.status_code == 422

    @pytest.mark.asyncio
    async def test_non_eip_oauth_account_cannot_be_repeated(
        self, client,
    ):
        created = await client.post("/api/accounts", json={
            "id": "oauth-one",
            "email": "oauth-one@example.com",
            "agent_type": "claude",
            "group": "shared",
        })
        assert created.status_code == 201

        response = await client.post("/api/jobs/plan", json={
            "name": "duplicate-oauth",
            "run": {"command": "true"},
            "fanout": {"workers": 2},
            "account": {
                "agent_type": "claude",
                "binding": "none",
                "ids": ["oauth-one", "oauth-one"],
            },
        })

        assert response.status_code == 422
        assert "cannot be shared" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_plan_renders_hostname_dataset_with_synthetic_worker(
        self, client, manager,
    ):
        manager.config.provider.aws.worker_instance_profile = "worker-profile"
        response = await client.post("/api/jobs/plan", json={
            "name": "hostname-dataset-preview",
            "setup": {
                "s3_datasets": [{
                    "uri": "s3://bucket/shard-{{hostname}}.tar",
                    "dest": "/home/ubuntu/data/{{hostname}}",
                }],
            },
            "run": {"command": "echo {{hostname}}"},
            "account": {"mode": "none"},
        })

        assert response.status_code == 200
        assert response.json()["datasets"] == [{
            "uri": "s3://bucket/shard-plan-worker-00000.tar",
            "dest": "/home/ubuntu/data/plan-worker-00000",
        }]
        assert response.json()["run"]["command"] == [
            "bash",
            "-lc",
            "echo plan-worker-00000",
        ]

    @pytest.mark.asyncio
    async def test_submit_idempotency_key_never_launches_duplicate_fleet(
        self, client, manager
    ):
        headers = {"Idempotency-Key": "request-123"}
        first = await client.post("/api/jobs", json=self._SPEC, headers=headers)
        second = await client.post("/api/jobs", json=self._SPEC, headers=headers)

        assert first.status_code == 201
        assert second.status_code == 201
        assert first.json()["job_id"] == second.json()["job_id"]
        assert second.json()["idempotent_replay"] is True
        assert manager.batch.started == [first.json()["job_id"]]

    @pytest.mark.asyncio
    async def test_new_fingerprint_replay_preserves_current_semantic_defaults(
        self, client, manager,
    ):
        from elastic_agent.core.job_spec import JobSpec

        key = "request-semantic-defaults"
        headers = {"Idempotency-Key": key}
        first = await client.post("/api/jobs", json=self._SPEC, headers=headers)
        explicitly_defaulted = JobSpec.model_validate(
            self._SPEC
        ).model_dump(mode="json")
        second = await client.post(
            "/api/jobs",
            json=explicitly_defaulted,
            headers=headers,
        )

        assert first.status_code == 201
        assert second.status_code == 201
        assert second.json()["job_id"] == first.json()["job_id"]
        assert second.json()["idempotent_replay"] is True
        assert manager.batch.started == [first.json()["job_id"]]

    @pytest.mark.asyncio
    async def test_idempotent_retry_reschedules_durably_prepared_job(
        self, client, manager,
    ):
        key = "prepared-before-crash"
        job_id = self._seed_job_journal(manager, key)

        response = await client.post(
            "/api/jobs", json=self._SPEC, headers={"Idempotency-Key": key},
        )

        assert response.status_code == 201
        assert response.json()["job_id"] == job_id
        assert response.json()["idempotent_replay"] is True
        assert response.json()["done"] is False
        assert manager.batch.started == [job_id]

    @pytest.mark.asyncio
    async def test_idempotent_retry_normalizes_defaults_added_after_persistence(
        self, client, manager,
    ):
        key = "prepared-before-login-timeout-field"
        job_id = self._seed_job_journal(manager, key)
        journal = (
            Path(manager.config.registry.path).with_name("specs") / f"{job_id}.json"
        )
        payload = json.loads(journal.read_text())
        payload["spec"]["account"].pop("login_timeout_seconds")
        journal.write_text(json.dumps(payload))

        response = await client.post(
            "/api/jobs", json=self._SPEC, headers={"Idempotency-Key": key},
        )

        assert response.status_code == 201
        assert response.json()["job_id"] == job_id
        assert response.json()["idempotent_replay"] is True
        assert manager.batch.started == [job_id]

    @pytest.mark.asyncio
    async def test_terminal_legacy_manager_distribute_replays_before_current_validation(
        self, client, manager,
    ):
        key = "legacy-manager-distribute-terminal"
        job_id = self._seed_job_journal(manager, key, "succeeded")
        journal = (
            Path(manager.config.registry.path).with_name("specs") / f"{job_id}.json"
        )
        payload = json.loads(journal.read_text())
        payload["spec"]["account"]["mode"] = "manager_distribute"
        journal.write_text(json.dumps(payload))

        response = await client.post(
            "/api/jobs",
            json=payload["spec"],
            headers={"Idempotency-Key": key},
        )

        assert response.status_code == 201
        assert response.json()["job_id"] == job_id
        assert response.json()["submission_state"] == "succeeded"
        assert response.json()["idempotent_replay"] is True
        assert manager.batch.started == []

    @pytest.mark.asyncio
    async def test_terminal_legacy_journal_without_fingerprint_fails_closed_on_mismatch(
        self, client, manager,
    ):
        key = "legacy-manager-distribute-mismatch"
        job_id = self._seed_job_journal(manager, key, "succeeded")
        journal = (
            Path(manager.config.registry.path).with_name("specs") / f"{job_id}.json"
        )
        payload = json.loads(journal.read_text())
        payload["spec"]["account"]["mode"] = "manager_distribute"
        journal.write_text(json.dumps(payload))
        changed = copy.deepcopy(payload["spec"])
        changed["name"] = "different"

        response = await client.post(
            "/api/jobs",
            json=changed,
            headers={"Idempotency-Key": key},
        )

        assert response.status_code == 409
        assert "legacy Job journal" in response.json()["detail"]
        assert "cannot be proven" in response.json()["detail"]
        assert manager.batch.started == []

    @pytest.mark.asyncio
    async def test_prepared_legacy_request_must_pass_current_validation_before_launch(
        self, client, manager,
    ):
        key = "legacy-manager-distribute-prepared"
        job_id = self._seed_job_journal(manager, key, "prepared")
        journal = (
            Path(manager.config.registry.path).with_name("specs") / f"{job_id}.json"
        )
        payload = json.loads(journal.read_text())
        payload["spec"]["account"]["mode"] = "manager_distribute"
        journal.write_text(json.dumps(payload))

        response = await client.post(
            "/api/jobs",
            json=payload["spec"],
            headers={"Idempotency-Key": key},
        )

        assert response.status_code == 422
        assert manager.batch.started == []
        assert json.loads(journal.read_text())["submission_state"] == "prepared"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["launching", "running"])
    async def test_idempotent_retry_does_not_duplicate_interrupted_fleet(
        self, client, manager, state,
    ):
        key = f"interrupted-{state}"
        job_id = self._seed_job_journal(manager, key, state)

        response = await client.post(
            "/api/jobs", json=self._SPEC, headers={"Idempotency-Key": key},
        )

        assert response.status_code == 201
        assert response.json()["job_id"] == job_id
        assert response.json()["submission_state"] == state
        assert response.json()["state"] == "interrupted"
        assert response.json()["done"] is False
        assert manager.batch.started == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["succeeded", "failed", "cancelled"])
    async def test_idempotent_retry_reports_durable_terminal_state(
        self, client, manager, state,
    ):
        key = f"terminal-{state}"
        job_id = self._seed_job_journal(manager, key, state)

        response = await client.post(
            "/api/jobs", json=self._SPEC, headers={"Idempotency-Key": key},
        )

        assert response.status_code == 201
        assert response.json()["job_id"] == job_id
        assert response.json()["submission_state"] == state
        assert response.json()["state"] == state
        assert response.json()["done"] is True
        assert manager.batch.started == []

    @pytest.mark.asyncio
    async def test_exact_terminal_replay_bypasses_current_preflight(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        key = "terminal-after-policy-change"
        job_id = self._seed_job_journal(manager, key, "succeeded")

        async def rejected_preflight(*_args, **_kwargs):
            raise HTTPException(422, "current policy rejects new jobs")

        monkeypatch.setattr(jobs_route, "_preflight_job", rejected_preflight)
        response = await client.post(
            "/api/jobs", json=self._SPEC, headers={"Idempotency-Key": key},
        )

        assert response.status_code == 201
        assert response.json()["job_id"] == job_id
        assert response.json()["idempotent_replay"] is True
        assert manager.batch.started == []

    @pytest.mark.asyncio
    async def test_idempotency_key_reuse_with_different_spec_is_rejected(
        self, client
    ):
        headers = {"Idempotency-Key": "request-conflict"}
        assert (
            await client.post("/api/jobs", json=self._SPEC, headers=headers)
        ).status_code == 201
        changed = {**self._SPEC, "name": "different"}
        response = await client.post("/api/jobs", json=changed, headers=headers)
        assert response.status_code == 409

    @pytest.mark.asyncio
    async def test_cancel_live_job_is_idempotent(self, client):
        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]

        first = await client.post(f"/api/jobs/{job_id}/cancel")
        second = await client.post(f"/api/jobs/{job_id}/cancel")

        assert first.status_code == 200
        assert second.status_code == 200
        assert first.json()["state"] == "cancelled"
        assert all(
            worker["phase"] == "cancelled"
            for worker in first.json()["workers_detail"]
        )

    @pytest.mark.asyncio
    async def test_submit_fsyncs_spec_before_starting_job(self, client, manager, monkeypatch):
        from elastic_agent.core import job_spec_store

        fsync_kinds = []
        real_fsync = job_spec_store.os.fsync

        def recording_fsync(fd):
            fsync_kinds.append(
                "dir" if stat.S_ISDIR(job_spec_store.os.fstat(fd).st_mode) else "file"
            )
            return real_fsync(fd)

        monkeypatch.setattr(job_spec_store.os, "fsync", recording_fsync)

        def assert_persisted(job):
            path = Path(manager.config.registry.path).with_name("specs") / f"{job.job_id}.json"
            data = json.loads(path.read_text(encoding="utf-8"))
            assert data["job_id"] == job.job_id
            assert data["spec"]["name"] == "ai4sci"
            assert data["request_fingerprint"] == {
                "schema": 1,
                "algorithm": "sha256",
                "digest": job.request_fingerprint,
            }
            assert len(data["request_fingerprint"]["digest"]) == 64
            assert stat.S_IMODE(path.stat().st_mode) == 0o600

        manager.batch.before_start = assert_persisted
        response = await client.post("/api/jobs", json=self._SPEC)

        assert response.status_code == 201
        assert manager.batch.started == [response.json()["job_id"]]
        assert fsync_kinds == ["file", "dir"]
        assert list(Path(manager.config.registry.path).with_name("specs").glob("*.tmp")) == []

    @pytest.mark.asyncio
    async def test_submit_persistence_failure_never_starts_job(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.core import job_spec_store

        def fail_replace(_src, _dest):
            raise OSError("disk is read-only")

        monkeypatch.setattr(job_spec_store.os, "replace", fail_replace)
        response = await client.post("/api/jobs", json=self._SPEC)

        assert response.status_code == 500
        assert "persist" in response.json()["detail"]
        assert manager.batch.started == []
        assert manager.batch.list_jobs() == []
        assert manager.provider._n == 0
        specs = Path(manager.config.registry.path).with_name("specs")
        assert list(specs.iterdir()) == []

    @pytest.mark.asyncio
    async def test_submit_route_has_independent_raw_body_limit(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        monkeypatch.setattr(jobs_route, "JOB_SUBMIT_MAX_BODY_BYTES", 128)
        response = await client.post(
            "/api/jobs",
            content=json.dumps({
                **self._SPEC,
                "padding": "x" * 256,
            }).encode(),
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 413
        assert manager.batch.started == []
        assert manager.provider._n == 0

    @pytest.mark.asyncio
    async def test_submit_openapi_retains_jobspec_request_schema(self, client):
        document = (await client.get("/openapi.json")).json()
        schema = document["paths"]["/api/jobs"]["post"]["requestBody"][
            "content"
        ]["application/json"]["schema"]

        assert schema == {"$ref": "#/components/schemas/JobSpec"}

    @pytest.mark.asyncio
    async def test_list_and_get(self, client):
        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        jid = submitted["job_id"]

        lst = (await client.get("/api/jobs")).json()
        assert lst["total"] == 1

        detail = (await client.get(f"/api/jobs/{jid}")).json()
        assert detail["job_id"] == jid
        assert detail["spec"]["name"] == "ai4sci"

    @pytest.mark.asyncio
    async def test_get_uses_immutable_persisted_submission_config_live_and_after_restart(
        self, client, manager,
    ):
        spec = {
            **self._SPEC,
            "setup": {"steps": [{
                "name": "install",
                "command": "true",
                "env": {"SETUP_TOKEN": "setup-plaintext"},
            }]},
            "run": {
                "command": "original-command",
                "env": {"RUN_TOKEN": "run-plaintext"},
                "secret_env": {
                    "SECRET_TOKEN": "aws-secretsmanager://prod/token#value",
                },
            },
        }
        submitted = (await client.post("/api/jobs", json=spec)).json()
        job_id = submitted["job_id"]
        submitted_config = submitted["spec"]

        # Runtime code owns a mutable model.  The detail API must still expose
        # what was submitted, not a later in-memory mutation.
        live_job = manager.batch.get_job(job_id)
        live_job.spec.name = "mutated-name"
        live_job.spec.run.command = "mutated-command"
        live_job.spec.run.env["RUN_TOKEN"] = "mutated-secret"

        live = await client.get(f"/api/jobs/{job_id}")
        assert live.status_code == 200
        assert live.headers["cache-control"] == "no-store"
        assert live.headers["pragma"] == "no-cache"
        assert live.json()["spec"] == submitted_config
        assert live.json()["spec"]["run"]["command"] == "original-command"
        assert live.json()["spec"]["run"]["env"] == {
            "RUN_TOKEN": "[REDACTED]",
        }
        assert live.json()["spec"]["run"]["secret_env"] == {
            "SECRET_TOKEN": "[SECRET_REFERENCE]",
        }
        assert live.json()["spec"]["setup"]["steps"][0]["env"] == {
            "SETUP_TOKEN": "[REDACTED]",
        }
        assert "plaintext" not in live.text
        assert "aws-secretsmanager" not in live.text

        # Simulate the in-memory batch registry being lost on Manager restart.
        manager.batch._jobs.clear()
        historical = await client.get(f"/api/jobs/{job_id}")
        assert historical.status_code == 200
        assert historical.headers["cache-control"] == "no-store"
        assert historical.headers["pragma"] == "no-cache"
        assert historical.json()["spec"] == submitted_config
        assert "plaintext" not in historical.text
        assert "aws-secretsmanager" not in historical.text

    @pytest.mark.asyncio
    async def test_get_config_snapshot_read_fails_fast_when_capacity_is_full(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(jobs_route, "_JOB_HISTORY_ADMISSION", admission)
        held = admission.try_acquire()
        assert held is not None
        try:
            response = await client.get(f"/api/jobs/{submitted['job_id']}")
            assert response.status_code == 503
            assert response.headers["retry-after"] == "1"
            assert response.headers["cache-control"] == "no-store"
            assert response.headers["pragma"] == "no-cache"
            assert admission.active == 1
        finally:
            held.release()
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_get_live_job_fails_closed_on_invalid_or_oversized_snapshot(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]
        journal = (
            Path(manager.config.registry.path).with_name("specs")
            / f"{job_id}.json"
        )

        original = journal.read_bytes()
        journal.write_text(
            json.dumps({"job_id": job_id, "spec": []}),
            encoding="utf-8",
        )
        invalid = await client.get(f"/api/jobs/{job_id}")
        assert invalid.status_code == 500
        assert invalid.headers["cache-control"] == "no-store"
        assert invalid.headers["pragma"] == "no-cache"
        assert "persisted Job config is unavailable" in invalid.json()["detail"]

        journal.write_bytes(original)
        monkeypatch.setattr(
            jobs_route,
            "JOB_JOURNAL_MAX_BYTES",
            journal.stat().st_size - 1,
        )
        oversized = await client.get(f"/api/jobs/{job_id}")
        assert oversized.status_code == 500
        assert oversized.headers["cache-control"] == "no-store"
        assert oversized.headers["pragma"] == "no-cache"
        assert "persisted Job config is unavailable" in oversized.json()["detail"]

    @pytest.mark.asyncio
    async def test_get_config_projection_stays_bounded_off_the_event_loop(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(jobs_route, "_JOB_HISTORY_ADMISSION", admission)
        original_redactor = jobs_route._redacted_spec
        entered = threading.Event()
        release = threading.Event()

        def blocking_redactor(spec):
            entered.set()
            if not release.wait(timeout=5):
                raise AssertionError("test did not release config projection")
            return original_redactor(spec)

        monkeypatch.setattr(jobs_route, "_redacted_spec", blocking_redactor)
        first = asyncio.create_task(
            client.get(f"/api/jobs/{submitted['job_id']}")
        )
        assert await asyncio.to_thread(entered.wait, 2)

        # The projection is running on the owned executor, so the event loop is
        # responsive and the admission token remains held for the heavy work.
        saturated = await client.get(f"/api/jobs/{submitted['job_id']}")
        assert saturated.status_code == 503
        assert saturated.headers["retry-after"] == "1"
        assert admission.active == 1

        release.set()
        completed = await first
        assert completed.status_code == 200
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_get_legacy_snapshot_without_lifecycle_metadata_is_compatible(
        self, client, manager,
    ):
        job_id = "legacy-config-snapshot"
        specs = Path(manager.config.registry.path).with_name("specs")
        specs.mkdir(mode=0o700, exist_ok=True)
        journal = specs / f"{job_id}.json"
        journal.write_text(
            json.dumps({
                "job_id": job_id,
                "name": self._SPEC["name"],
                "spec": self._SPEC,
            }),
            encoding="utf-8",
        )
        journal.chmod(0o600)

        detail = await client.get(f"/api/jobs/{job_id}")

        assert detail.status_code == 200
        assert detail.json()["submission_state"] == "unknown"
        assert detail.json()["spec"]["name"] == "ai4sci"

    @pytest.mark.asyncio
    async def test_get_known_legacy_manager_distribute_snapshot_preserves_mode(
        self, client, manager,
    ):
        job_id = self._seed_job_journal(
            manager,
            "known-legacy-config-snapshot",
            "succeeded",
        )
        journal = (
            Path(manager.config.registry.path).with_name("specs")
            / f"{job_id}.json"
        )
        payload = json.loads(journal.read_text(encoding="utf-8"))
        payload["spec"]["account"]["mode"] = "manager_distribute"
        journal.write_text(json.dumps(payload), encoding="utf-8")

        detail = await client.get(f"/api/jobs/{job_id}")

        assert detail.status_code == 200
        assert detail.json()["spec"]["account"]["mode"] == "manager_distribute"

    @pytest.mark.asyncio
    async def test_get_historical_snapshot_uses_bounded_job_scoped_lease_query(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        job_id = self._seed_job_journal(
            manager,
            "bounded-config-lease-query",
            "succeeded",
        )
        observed = {}

        async def list_leases(*, job_ids, limit):
            observed["job_ids"] = job_ids
            observed["limit"] = limit
            return []

        monkeypatch.setattr(
            manager.account_binding_store,
            "list_leases",
            list_leases,
        )

        detail = await client.get(f"/api/jobs/{job_id}")

        assert detail.status_code == 200
        assert observed == {
            "job_ids": {job_id},
            "limit": jobs_route.JOB_DETAIL_MAX_RECOVERY_LEASES + 1,
        }
        assert detail.json()["recovery_leases_truncated"] is False

    @pytest.mark.asyncio
    async def test_get_historical_snapshot_marks_truncated_leases_not_done(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route
        from elastic_agent.core.account_binding import AccountLease

        job_id = self._seed_job_journal(
            manager,
            "truncated-config-lease-query",
            "succeeded",
        )
        monkeypatch.setattr(jobs_route, "JOB_DETAIL_MAX_RECOVERY_LEASES", 1)

        async def list_leases(*, job_ids, limit):
            assert job_ids == {job_id}
            assert limit == 2
            return [
                AccountLease(
                    lease_id="released-visible",
                    account_id="account-1",
                    job_id=job_id,
                    state="released",
                ),
                AccountLease(
                    lease_id="possibly-active-truncated",
                    account_id="account-2",
                    job_id=job_id,
                    state="attached",
                    instance_id="i-hidden",
                    worker_id="w-hidden",
                ),
            ]

        monkeypatch.setattr(
            manager.account_binding_store,
            "list_leases",
            list_leases,
        )

        detail = (await client.get(f"/api/jobs/{job_id}")).json()

        assert detail["recovery_leases_truncated"] is True
        assert len(detail["recovery_leases"]) == 1
        assert detail["cleanup_pending"] == 1
        assert detail["done"] is False

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mutate", "secret"),
        [
            (
                lambda spec: spec.update({
                    "operator_secret": "unknown-top-level-secret",
                }),
                "unknown-top-level-secret",
            ),
            (
                lambda spec: spec["run"].update({
                    "private_note": "unknown-nested-secret",
                }),
                "unknown-nested-secret",
            ),
            (
                lambda spec: spec.update({
                    "setup": {
                        "repo": (
                            "https://operator:repository-secret@example.com/"
                            "project.git?token=query-secret"
                        ),
                    },
                }),
                "repository-secret",
            ),
        ],
    )
    async def test_get_incompatible_legacy_snapshot_never_echoes_unknown_fields(
        self, client, manager, mutate, secret,
    ):
        job_id = "incompatible-config-snapshot"
        raw_spec = copy.deepcopy(self._SPEC)
        mutate(raw_spec)
        specs = Path(manager.config.registry.path).with_name("specs")
        specs.mkdir(mode=0o700, exist_ok=True)
        journal = specs / f"{job_id}.json"
        journal.write_text(
            json.dumps({
                "job_id": job_id,
                "name": self._SPEC["name"],
                "spec": raw_spec,
            }),
            encoding="utf-8",
        )
        journal.chmod(0o600)

        detail = await client.get(f"/api/jobs/{job_id}")

        assert detail.status_code == 500
        assert detail.headers["cache-control"] == "no-store"
        assert detail.headers["pragma"] == "no-cache"
        assert "persisted Job config is unavailable" in detail.json()["detail"]
        assert secret not in detail.text
        assert "query-secret" not in detail.text

    @pytest.mark.asyncio
    async def test_historical_job_list_uses_scandir_and_explicit_caps(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        for index in range(6):
            self._seed_job_journal(manager, f"bounded-history-{index}")
        monkeypatch.setattr(
            jobs_route,
            "JOB_LIST_HISTORY_MAX_SCANNED_ENTRIES",
            3,
        )
        monkeypatch.setattr(
            jobs_route,
            "JOB_LIST_HISTORY_MAX_RETURNED",
            2,
        )

        def forbid_glob(*_args, **_kwargs):
            raise AssertionError("historical listing must use bounded scandir")

        monkeypatch.setattr(Path, "glob", forbid_glob)
        response = await client.get("/api/jobs")

        assert response.status_code == 200
        payload = response.json()
        assert payload["history_scanned"] == 3
        assert payload["history_returned"] == 2
        assert payload["total"] == 2
        assert len(payload["jobs"]) == 2
        assert payload["truncated"] is True

    @pytest.mark.asyncio
    async def test_historical_job_list_caps_aggregate_read_and_response(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        for index in range(3):
            self._seed_job_journal(manager, f"bounded-read-{index}")
        specs = Path(manager.config.registry.path).with_name("specs")
        one_journal = max(
            path.stat().st_size for path in specs.glob("*.json")
        )
        monkeypatch.setattr(
            jobs_route,
            "JOB_LIST_HISTORY_MAX_READ_BYTES",
            one_journal,
        )

        read_limited = (await client.get("/api/jobs")).json()
        assert read_limited["history_returned"] == 1
        assert read_limited["truncated"] is True

        monkeypatch.setattr(
            jobs_route,
            "JOB_LIST_HISTORY_MAX_RESPONSE_BYTES",
            1,
        )
        response_limited = (await client.get("/api/jobs")).json()
        assert response_limited["history_returned"] == 0
        assert response_limited["total"] == 0
        assert response_limited["truncated"] is True

    @pytest.mark.asyncio
    async def test_historical_job_list_fails_fast_when_capacity_is_full(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(
            jobs_route,
            "_JOB_HISTORY_ADMISSION",
            admission,
        )
        held = admission.try_acquire()
        assert held is not None
        try:
            response = await client.get("/api/jobs")
            assert response.status_code == 503
            assert response.headers["retry-after"] == "1"
            assert admission.active == 1
        finally:
            held.release()
        assert admission.active == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("submission_state", "api_state", "done"),
        [
            ("prepared", "prepared", False),
            ("running", "interrupted", False),
            ("succeeded", "succeeded", True),
        ],
    )
    async def test_recovered_list_and_get_use_submission_journal_state(
        self, client, manager, submission_state, api_state, done,
    ):
        key = f"list-state-{submission_state}"
        job_id = self._seed_job_journal(manager, key, submission_state)

        item = (await client.get("/api/jobs")).json()["jobs"][0]
        detail = (await client.get(f"/api/jobs/{job_id}")).json()

        for view in (item, detail):
            assert view["job_id"] == job_id
            assert view["submission_state"] == submission_state
            assert view["state"] == api_state
            assert view["done"] is done

    @pytest.mark.asyncio
    async def test_terminal_journal_preserves_worker_error_and_log_task_reference(
        self, client, manager,
    ):
        from elastic_agent.core.batch_orchestrator import WorkerPhase

        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]
        job = manager.batch.get_job(job_id)
        runs = list(job.runs.values())
        runs[0].phase = WorkerPhase.FAILED
        runs[0].task_id = f"{job_id}:w0:abcdef"
        runs[0].error = "run exited 1"
        manager.job_log_store.save_snapshot(
            job_id=job_id,
            task_id=runs[0].task_id,
            worker_id="w0",
            entries=[{
                "task_id": runs[0].task_id,
                "worker_id": "w0",
                "stream": "stderr",
                "data": "durable failure detail",
                "timestamp": "2026-07-26T00:00:00+00:00",
            }],
            exit_info={"exit_code": 1, "error_message": "run exited 1"},
        )
        for run in runs[1:]:
            run.phase = WorkerPhase.DONE
        job.launch_complete = True
        job.resources_released = True
        await manager._update_batch_job_state(
            job_id, "failed", job.summary(),
        )
        manager.batch._jobs.clear()

        detail = (await client.get(f"/api/jobs/{job_id}")).json()

        assert detail["state"] == "failed"
        assert detail["error"] == "run exited 1"
        assert detail["created_at"] == job.created_at.isoformat()
        assert detail["started_at"] == job.started_at
        assert detail["completed_at"] == job.completed_at
        assert detail["workers_detail"][0]["task_id"].endswith(":w0:abcdef")
        assert detail["workers_detail"][0]["error"] == "run exited 1"
        assert detail["workers_detail"][0]["worker_released"] is True

        archived = await client.get(f"/api/jobs/{job_id}/logs?lines=5000")
        assert archived.status_code == 200
        assert archived.headers["cache-control"] == "no-store"
        payload = archived.json()
        assert payload["source"] == "archive"
        assert payload["status"] == "archived"
        assert payload["complete"] is True
        assert payload["tasks"][0]["exit_code"] == 1
        assert payload["tasks"][0]["error_message"] == "run exited 1"
        assert payload["entries"][0]["data"] == "durable failure detail"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("method", "suffix"),
        [
            ("get", ""),
            ("post", "/cancel"),
            ("post", "/resubmit"),
            ("get", "/logs"),
            ("get", "/results"),
            ("get", "/results/download"),
        ],
    )
    async def test_job_routes_reject_non_component_job_ids(
        self, client, method, suffix,
    ):
        response = await getattr(client, method)(f"/api/jobs/bad$id{suffix}")

        assert response.status_code == 400
        assert response.json()["detail"] == "invalid job_id"

    @pytest.mark.asyncio
    async def test_list_includes_workers_detail(self, client):
        # The UI renders each job card straight from the list response, so every
        # item must carry workers_detail (no per-job detail fetch) — and drop the
        # heavy spec to keep the list lean.
        await client.post("/api/jobs", json=self._SPEC)
        item = (await client.get("/api/jobs")).json()["jobs"][0]
        assert len(item["workers_detail"]) == 3
        assert all(
            worker["worker_released"] is False
            and worker["worker_release_expected"] is True
            for worker in item["workers_detail"]
        )
        assert "spec" not in item

    @pytest.mark.asyncio
    async def test_get_missing_404(self, client):
        response = await client.get("/api/jobs/nope")
        assert response.status_code == 404
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["pragma"] == "no-cache"

    @pytest.mark.asyncio
    async def test_job_logs_are_available_live_and_after_worker_release(
        self, client, manager,
    ):
        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]
        task_id = f"{job_id}:w0:abcdef"
        job = manager.batch.get_job(job_id)
        job.runs["w0"].task_id = task_id
        for index, (stream, data) in enumerate([
            ("stdout", "starting"),
            ("stderr", "useful failure detail"),
        ]):
            manager.log_event_parser.process_log_event("w0", {
                "task_id": task_id,
                "stream": stream,
                "data": data,
                "timestamp": f"2026-07-25T12:00:0{index}+00:00",
                "parsed": None,
            })

        live = await client.get(f"/api/jobs/{job_id}/logs?lines=1")
        assert live.status_code == 200
        assert live.headers["cache-control"] == "no-store"
        assert live.json()["source"] == "live"
        assert live.json()["returned"] == 1
        assert live.json()["entries"][0]["data"] == "useful failure detail"

        manager.job_log_store.save_snapshot(
            job_id=job_id,
            task_id=task_id,
            worker_id="w0",
            entries=manager.log_event_parser.get_task_logs(task_id),
            exit_info={"exit_code": 1, "error_message": "run exited 1"},
        )
        manager.log_event_parser.release_task(task_id)
        job.runs["w0"].cleaned_up = True

        archived = await client.get(
            f"/api/jobs/{job_id}/logs?worker_id=w0&lines=100",
        )
        assert archived.status_code == 200
        assert archived.json()["source"] == "archive"
        assert [entry["data"] for entry in archived.json()["entries"]] == [
            "starting",
            "useful failure detail",
        ]
        assert archived.json()["tasks"][0]["exit_code"] == 1

    @pytest.mark.asyncio
    async def test_job_logs_use_bounded_tail_reader_and_keep_polling_active_scope(
        self, client, manager, monkeypatch,
    ):
        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]
        old_task = f"{job_id}:w0:old"
        manager.job_log_store.save_snapshot(
            job_id=job_id,
            task_id=old_task,
            worker_id="w0",
            entries=[{
                "task_id": old_task,
                "worker_id": "w0",
                "stream": "stderr",
                "data": "old attempt",
            }],
            exit_info={"exit_code": 1},
        )
        job = manager.batch.get_job(job_id)
        job.runs["w0"].task_id = f"{job_id}:w0:new"

        def forbid_full_read(_job_id):
            raise AssertionError("logs API must not materialize the full Job")

        monkeypatch.setattr(manager.job_log_store, "read_job", forbid_full_read)
        response = await client.get(f"/api/jobs/{job_id}/logs?lines=1")

        assert response.status_code == 200
        payload = response.json()
        assert payload["returned"] == 1
        assert payload["entries"][0]["data"] == "old attempt"
        assert payload["status"] == "live"
        assert payload["complete"] is False

    @pytest.mark.asyncio
    async def test_job_logs_use_dedicated_executor_and_fail_fast_when_full(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]
        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(
            jobs_route,
            "_JOB_LOG_READ_ADMISSION",
            admission,
        )
        entered = threading.Event()
        release = threading.Event()
        thread_names: list[str] = []

        def blocking_tail(*_args, **_kwargs):
            thread_names.append(threading.current_thread().name)
            entered.set()
            release.wait(timeout=5)
            return {
                "tasks": [],
                "entries": [],
                "total": 0,
                "history_truncated": False,
                "truncated": False,
            }

        monkeypatch.setattr(
            manager.job_log_store,
            "read_job_tail",
            blocking_tail,
        )
        active = asyncio.create_task(
            client.get(f"/api/jobs/{job_id}/logs")
        )
        assert await asyncio.to_thread(entered.wait, 1)

        saturated = await client.get(f"/api/jobs/{job_id}/logs")
        assert saturated.status_code == 503
        assert saturated.headers["retry-after"] == "1"
        assert admission.active == 1

        release.set()
        assert (await active).status_code == 200
        assert thread_names
        assert all(name.startswith("job-log-read") for name in thread_names)
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_cancelled_job_log_read_keeps_permit_until_thread_exits(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]
        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(
            jobs_route,
            "_JOB_LOG_READ_ADMISSION",
            admission,
        )
        entered = threading.Event()
        release = threading.Event()

        def blocking_tail(*_args, **_kwargs):
            entered.set()
            release.wait(timeout=5)
            return {
                "tasks": [],
                "entries": [],
                "total": 0,
                "history_truncated": False,
                "truncated": False,
            }

        monkeypatch.setattr(
            manager.job_log_store,
            "read_job_tail",
            blocking_tail,
        )
        cancelled = asyncio.create_task(
            client.get(f"/api/jobs/{job_id}/logs")
        )
        assert await asyncio.to_thread(entered.wait, 1)
        cancelled.cancel()
        await asyncio.sleep(0)

        assert cancelled.done() is False
        assert admission.active == 1
        saturated = await client.get(f"/api/jobs/{job_id}/logs")
        assert saturated.status_code == 503
        assert admission.active == 1

        release.set()
        result = await asyncio.gather(cancelled, return_exceptions=True)
        assert isinstance(result[0], asyncio.CancelledError)
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_job_logs_stop_polling_after_run_while_cleanup_is_pending(
        self, client, manager,
    ):
        from elastic_agent.core.batch_orchestrator import WorkerPhase

        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]
        job = manager.batch.get_job(job_id)
        job.launch_complete = True
        for index, run in enumerate(job.runs.values()):
            run.phase = WorkerPhase.DONE
            run.task_id = f"{job_id}:{run.worker_id}:done-{index}"
            manager.job_log_store.save_snapshot(
                job_id=job_id,
                task_id=run.task_id,
                worker_id=run.worker_id,
                entries=[],
                exit_info={"exit_code": 0},
            )

        # resources_released remains false, so the Job itself is not yet done;
        # command output is nevertheless final and must not poll every 3s.
        payload = (await client.get(f"/api/jobs/{job_id}/logs")).json()
        assert job.summary()["done"] is False
        assert payload["status"] == "archived"
        assert payload["complete"] is True

    @pytest.mark.asyncio
    async def test_job_logs_validate_job_and_bounds(self, client):
        assert (await client.get("/api/jobs/nope/logs")).status_code == 404
        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        job_id = submitted["job_id"]
        assert (
            await client.get(f"/api/jobs/{job_id}/logs?lines=5001")
        ).status_code == 422
        assert (
            await client.get(
                f"/api/jobs/{job_id}/logs?task_id=job-other:w0:abcdef",
            )
        ).status_code == 404

    @pytest.mark.asyncio
    async def test_invalid_spec_422(self, client):
        # missing run.command
        assert (await client.post("/api/jobs", json={"name": "x"})).status_code == 422

    @pytest.mark.asyncio
    async def test_plan_is_secret_safe_and_has_no_launch_side_effects(
        self, client, manager, monkeypatch,
    ):
        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "results-bucket")
        manager.config.provider.aws.worker_instance_profile = "worker-role"
        spec = {
            **self._SPEC,
            "environment": {"profile": "ubuntu-agent-docker-v1"},
            "setup": {
                "repo": "https://github.com/org/repo.git",
                "ref": "release-v1",
                "resolved_commit": "a" * 40,
                "steps": [{
                    "name": "install", "command": "uv sync",
                    "env": {"PRIVATE_VALUE": "supersecret"},
                    "timeout": 1000, "retries": 1,
                }],
                "s3_datasets": [{
                    "uri": "s3://private-data/run/shard-{{shard_id}}.jsonl",
                    "dest": "/srv/replay/shard-{{shard_id}}.jsonl",
                }],
            },
            "run": {
                "command": "bench --token $TOKEN",
                "env": {"TOKEN": "another-secret"},
                "secret_env": {"DB_PASSWORD": "aws-ssm:///prod/db/password"},
            },
            "collect": {"paths": ["results"], "interval_seconds": 120},
        }

        response = await client.post("/api/jobs/plan", json=spec)

        assert response.status_code == 200
        plan = response.json()
        assert plan["valid"] is True
        assert plan["side_effects"] is False
        assert plan["environment"]["docker"] is True
        assert plan["results"]["mode"] == "worker-direct-s3"
        assert plan["results"]["automatic_final_collect"] is True
        assert plan["setup_steps"][0]["run_as"] == "job"
        assert "PRIVATE_VALUE" in plan["setup_steps"][0]["env_keys"]
        assert "supersecret" not in response.text
        assert "another-secret" not in response.text
        assert plan["run"]["secret_env_keys"] == ["DB_PASSWORD"]
        assert "aws-ssm" not in response.text
        assert plan["datasets"] == [{
            "uri": "s3://private-data/run/shard-00000.jsonl",
            "dest": "/srv/replay/shard-00000.jsonl",
        }]
        assert plan["fanout"]["worst_case_worker_hours"] == 144
        assert plan["fanout"]["instance_type_allowlist"] == [
            manager.config.provider.aws.default_instance_type
        ]
        assert any(
            "private repositories must use setup.deliver='manager_rsync'"
            in warning
            for warning in plan["warnings"]
        )
        assert manager.batch.started == []
        assert manager.provider._n == 0
        specs = Path(manager.config.registry.path).with_name("specs")
        assert list(specs.glob("*.json")) == []

    @pytest.mark.asyncio
    async def test_plan_warns_when_worker_login_bypasses_account_eip(
        self, client,
    ):
        created = await client.post("/api/accounts", json={
            "id": "codex-eip-warning",
            "email": "warning@163.com",
            "agent_type": "codex",
            "email_token": "mail-query-token",
            "group": "standard",
        })
        assert created.status_code == 201

        response = await client.post("/api/jobs/plan", json={
            "name": "unbound-codex",
            "run": {"command": "true"},
            "account": {
                "agent_type": "codex",
                "mode": "worker_local_login",
                "binding": "none",
            },
        })

        assert response.status_code == 200
        assert any(
            "excludes identities with durable EIP bindings" in warning
            for warning in response.json()["warnings"]
        )

    @pytest.mark.asyncio
    async def test_durable_api_binding_is_not_available_to_unbound_jobs(
        self, client, manager, monkeypatch,
    ):
        adapter = manager.agent_api_store.registry.require("cloudrouter")
        monkeypatch.setattr(
            adapter,
            "probe_models",
            AsyncMock(return_value={"codex": ["gpt-5.4"]}),
        )
        monkeypatch.setattr(
            adapter,
            "fetch_usage",
            AsyncMock(return_value={
                "account_id": "cloudrouter-1",
                "known": True,
                "available": True,
                "reason": "active",
                "windows": [],
            }),
        )
        created = await client.post("/api/agent-api/accounts", json={
            "provider": "cloudrouter",
            "name": "Bound API",
            "group": "bound-only",
            "api_key": "private-bound-key",
        })
        assert created.status_code == 201
        manager.binding_manager.bindings["cloudrouter-1"] = AccountBinding(
            account_id="cloudrouter-1",
            eip_allocation_id="eipalloc-bound-api",
            eip_ip="198.51.100.42",
            region="us-west-2",
            state=BindingState.READY,
        )

        explicit_unbound = await client.post("/api/jobs/plan", json={
            "name": "explicit-unbound-api",
            "run": {"command": "true"},
            "account": {
                "agent_type": "codex",
                "binding": "none",
                "ids": ["cloudrouter-1"],
            },
        })
        automatic_unbound = await client.post("/api/jobs/plan", json={
            "name": "automatic-unbound-api",
            "run": {"command": "true"},
            "account": {
                "agent_type": "codex",
                "binding": "none",
                "group": "bound-only",
            },
        })
        bound = await client.post("/api/jobs/plan", json={
            "name": "bound-api",
            "run": {"command": "true"},
            "account": {
                "agent_type": "codex",
                "binding": "eip",
                "ids": ["cloudrouter-1"],
            },
        })

        assert explicit_unbound.status_code == 422
        assert "has a durable EIP binding" in explicit_unbound.json()["detail"]
        assert automatic_unbound.status_code == 422
        assert "requires 1" in automatic_unbound.json()["detail"]
        assert bound.status_code == 200

    @pytest.mark.asyncio
    async def test_submit_preflight_rejects_unavailable_region_before_persisting(
        self, client, manager,
    ):
        spec = {**self._SPEC, "fanout": {"workers": 1, "region": "eu-west-1"}}

        response = await client.post("/api/jobs", json=spec)

        assert response.status_code == 422
        assert "configured only for 'us-west-2'" in response.json()["detail"]
        assert manager.batch.started == []
        assert manager.provider._n == 0
        specs = Path(manager.config.registry.path).with_name("specs")
        assert list(specs.glob("*.json")) == []

    @pytest.mark.asyncio
    async def test_preflight_rejects_missing_account_capacity(self, client):
        response = await client.post("/api/jobs/plan", json={
            "name": "needs-accounts",
            "run": {"command": "bench"},
            "fanout": {"workers": 2},
            "account": {"agent_type": "codex", "group": "prod"},
        })

        assert response.status_code == 422
        assert "requires 2" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_job_detail_and_persisted_replay_redact_all_env_values(
        self, client, manager,
    ):
        spec = {
            **self._SPEC,
            "setup": {"steps": [{
                "name": "install", "command": "true",
                "env": {"SETUP_TOKEN": "setup-plaintext"},
            }]},
            "run": {
                "command": "true",
                "env": {"VISIBLE_TOKEN": "run-plaintext"},
                "secret_env": {
                    "SECRET_TOKEN": "aws-secretsmanager://prod/token#value",
                },
            },
        }
        headers = {"Idempotency-Key": "redaction-replay"}
        first = await client.post("/api/jobs", json=spec, headers=headers)
        assert first.status_code == 201
        body = first.json()["spec"]
        assert body["run"]["env"] == {"VISIBLE_TOKEN": "[REDACTED]"}
        assert body["run"]["secret_env"] == {
            "SECRET_TOKEN": "[SECRET_REFERENCE]",
        }
        assert body["setup"]["steps"][0]["env"] == {
            "SETUP_TOKEN": "[REDACTED]",
        }
        assert "plaintext" not in first.text
        assert "aws-secretsmanager" not in first.text

        # Force the idempotency path to use the persisted journal rather than
        # the in-memory Job, and verify that response is redacted too.
        manager.batch._jobs.clear()
        replay = await client.post("/api/jobs", json=spec, headers=headers)
        assert replay.status_code == 201
        assert replay.json()["spec"] == body
        assert "plaintext" not in replay.text
        assert "aws-secretsmanager" not in replay.text

        detail = await client.get(f"/api/jobs/{first.json()['job_id']}")
        assert detail.status_code == 200
        assert detail.json()["spec"] == body

    @pytest.mark.asyncio
    async def test_preflight_instance_allowlist_fails_closed_and_can_be_configured(
        self, client, monkeypatch,
    ):
        monkeypatch.delenv("ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES", raising=False)
        custom = {
            **self._SPEC,
            "fanout": {"workers": 1, "instance_type": "m5.4xlarge"},
        }
        rejected = await client.post("/api/jobs/plan", json=custom)
        assert rejected.status_code == 422
        assert "not allowed" in rejected.json()["detail"]

        monkeypatch.setenv(
            "ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES", "t3.large,m5.4xlarge",
        )
        allowed = await client.post("/api/jobs/plan", json=custom)
        assert allowed.status_code == 200
        assert allowed.json()["fanout"]["instance_type"] == "m5.4xlarge"

    @pytest.mark.asyncio
    async def test_preflight_rejects_excess_worst_case_worker_hours(
        self, client, manager, monkeypatch,
    ):
        monkeypatch.setenv("ELASTIC_AGENT_MAX_JOB_WORKER_HOURS", "100")
        response = await client.post("/api/jobs", json={
            **self._SPEC,
            "fanout": {"workers": 3},
            "ttl_seconds": 172800,
        })

        assert response.status_code == 422
        assert "worker-hours 144" in response.json()["detail"]
        assert manager.batch.started == []
        assert manager.provider._n == 0


class TestHarnessUpload:
    _CODE = (
        "from elastic_agent.harness.base import Harness, BootstrapStep\n"
        "class MyHarness(Harness):\n"
        "    def get_bootstrap_steps(self):\n"
        "        return [BootstrapStep(name='c', command='echo c')]\n"
    )

    @pytest.mark.asyncio
    async def test_upload_disabled_by_default(self, client):
        response = await client.post("/api/jobs/harness", json={
            "filename": "myh.py", "content": self._CODE, "class_name": "MyHarness",
        })
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_upload_returns_ref(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD", "1")
        r = await client.post("/api/jobs/harness", json={
            "filename": "myh.py", "content": self._CODE, "class_name": "MyHarness",
        })
        assert r.status_code == 201
        ref = r.json()["harness_ref"]
        assert ref.endswith(":MyHarness")

    @pytest.mark.asyncio
    async def test_bad_filename_rejected(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD", "1")
        r = await client.post("/api/jobs/harness", json={
            "filename": "../evil.py", "content": self._CODE, "class_name": "MyHarness",
        })
        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_not_a_harness_rejected(self, client, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD", "1")
        r = await client.post("/api/jobs/harness", json={
            "filename": "plain.py", "content": "class Nope:\n    pass\n", "class_name": "Nope",
        })
        assert r.status_code == 400

    @pytest.mark.asyncio
    async def test_invalid_reupload_does_not_destroy_existing_harness(
        self, client, monkeypatch,
    ):
        from elastic_agent.harness.generic import load_harness_class

        monkeypatch.setenv("ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD", "1")
        first = await client.post("/api/jobs/harness", json={
            "filename": "stable.py",
            "content": self._CODE,
            "class_name": "MyHarness",
        })
        assert first.status_code == 201
        first_ref = first.json()["harness_ref"]
        first_path = Path(first.json()["path"])

        invalid = await client.post("/api/jobs/harness", json={
            "filename": "stable.py",
            "content": "class Nope:\n    pass\n",
            "class_name": "Nope",
        })

        assert invalid.status_code == 400
        assert first_path.is_file()
        assert load_harness_class(first_ref).__name__ == "MyHarness"

    @pytest.mark.asyncio
    async def test_reupload_publishes_immutable_content_addressed_version(
        self, client, monkeypatch,
    ):
        monkeypatch.setenv("ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD", "1")
        first = await client.post("/api/jobs/harness", json={
            "filename": "versioned.py",
            "content": self._CODE,
            "class_name": "MyHarness",
        })
        changed_code = self._CODE.replace("echo c", "echo changed")
        second = await client.post("/api/jobs/harness", json={
            "filename": "versioned.py",
            "content": changed_code,
            "class_name": "MyHarness",
        })

        assert first.status_code == second.status_code == 201
        assert first.json()["harness_ref"] != second.json()["harness_ref"]
        assert Path(first.json()["path"]).read_text() == self._CODE
        assert Path(second.json()["path"]).read_text() == changed_code

    @pytest.mark.asyncio
    async def test_harness_upload_has_hard_content_limit(
        self, client, monkeypatch,
    ):
        monkeypatch.setenv("ELASTIC_AGENT_ENABLE_HARNESS_UPLOAD", "1")
        response = await client.post("/api/jobs/harness", json={
            "filename": "huge.py",
            "content": "x" * (1_048_576 + 1),
            "class_name": "Huge",
        })
        assert response.status_code in {413, 422}


class TestJobResults:
    def _seed(self, manager, job_id="job-r"):
        import json as _json
        from pathlib import Path
        base = Path(manager.config.registry.path).with_name("collected") / job_id
        (base / "math.foo").mkdir(parents=True, exist_ok=True)
        (base / "run_metadata.json").write_text("{}")
        (base / "math.foo" / "res_b1.json").write_text(_json.dumps({
            "task_id": "math.foo", "prompt_level": "b1", "status": "completed", "final_score": 39.06,
        }))
        return job_id

    @pytest.mark.asyncio
    async def test_list_results_with_scores(self, client, manager):
        jid = self._seed(manager)
        r = await client.get(f"/api/jobs/{jid}/results")
        assert r.status_code == 200
        body = r.json()
        assert body["file_count"] == 2
        assert body["files_returned"] == 2
        assert body["files_truncated"] is False
        assert body["scores"] == [
            {"task_id": "math.foo", "prompt_level": "b1", "status": "completed", "final_score": 39.06}
        ]

    @pytest.mark.asyncio
    async def test_local_result_file_preview_is_explicitly_bounded(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        jid = "job-local-many"
        base = (
            Path(manager.config.registry.path).with_name("collected") / jid
        )
        base.mkdir(parents=True)
        anchor = base / "anchor.txt"
        anchor.write_text("x")
        listed_stat = anchor.lstat()
        regular = [
            (anchor, f"worker/result-{index:05d}.txt", listed_stat)
            for index in range(5_184)
        ]
        monkeypatch.setattr(
            jobs_route,
            "_local_regular_files",
            lambda *_args, **_kwargs: regular,
        )

        response = await client.get(f"/api/jobs/{jid}/results")

        assert response.status_code == 200
        body = response.json()
        assert body["file_count"] == 5_184
        assert body["files_returned"] == 500
        assert body["files_truncated"] is True
        assert len(body["files"]) == 500

    @pytest.mark.asyncio
    async def test_s3_result_file_preview_uses_the_same_bound(
        self, client, monkeypatch,
    ):
        class ManyObjectPaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [
                    {
                        "Key": (
                            f"jobs/job-s3-many/worker/"
                            f"result-{index:05d}.txt"
                        ),
                        "Size": 1,
                        "ETag": f'"many-{index}"',
                    }
                    for index in range(5_184)
                ]}]

        class ManyObjectS3:
            def get_paginator(self, name):
                return ManyObjectPaginator()

        monkeypatch.setenv(
            "ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket",
        )
        monkeypatch.setattr(
            "elastic_agent.api.routes.jobs._s3_client",
            lambda: ManyObjectS3(),
        )

        response = await client.get("/api/jobs/job-s3-many/results")

        assert response.status_code == 200
        body = response.json()
        assert body["file_count"] == 5_184
        assert body["files_returned"] == 500
        assert body["files_truncated"] is True
        assert len(body["files"]) == 500

    def test_result_file_preview_has_utf8_and_serialized_byte_budgets(
        self, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        row = ("科学-result.txt", 1)
        monkeypatch.setattr(
            jobs_route,
            "RESULT_FILE_LIST_MAX_PATH_BYTES",
            len(row[0].encode("utf-8")) - 1,
        )
        with pytest.raises(
            jobs_route.ResultsLimitExceeded, match="UTF-8 path metadata",
        ):
            jobs_route._bounded_result_files([row], file_count=1)

        monkeypatch.setattr(
            jobs_route, "RESULT_FILE_LIST_MAX_PATH_BYTES", 1_024,
        )
        monkeypatch.setattr(
            jobs_route, "RESULT_FILE_LIST_MAX_JSON_BYTES", 2,
        )
        with pytest.raises(
            jobs_route.ResultsLimitExceeded, match="serialized file metadata",
        ):
            jobs_route._bounded_result_files([row], file_count=1)

    @pytest.mark.asyncio
    async def test_download_tarball(self, client, manager):
        jid = self._seed(manager, "job-dl")
        r = await client.get(f"/api/jobs/{jid}/results/download")
        assert r.status_code == 200
        assert r.headers["content-type"] == "application/gzip"
        assert "job-dl-results.tar.gz" in r.headers["content-disposition"]
        assert int(r.headers["content-length"]) == len(r.content)
        assert len(r.content) > 0

    @pytest.mark.asyncio
    async def test_local_results_ignore_symlinks_directories_and_special_files(
        self, client, manager, tmp_path,
    ):
        jid = self._seed(manager, "job-safe-local")
        base = Path(manager.config.registry.path).with_name("collected") / jid
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "stolen.json").write_text(json.dumps({"final_score": 999}))
        (base / "linked-file.json").symlink_to(outside / "stolen.json")
        (base / "linked-directory").symlink_to(outside, target_is_directory=True)
        os.mkfifo(base / "named-pipe")

        listed = await client.get(f"/api/jobs/{jid}/results")
        downloaded = await client.get(f"/api/jobs/{jid}/results/download")

        assert listed.status_code == 200
        assert listed.json()["file_count"] == 2
        assert listed.json()["scores"][0]["final_score"] == 39.06
        assert all("linked" not in item["path"] for item in listed.json()["files"])
        with tarfile.open(fileobj=io.BytesIO(downloaded.content), mode="r:gz") as archive:
            names = archive.getnames()
        assert names == [
            "job-safe-local/run_metadata.json",
            "job-safe-local/math.foo/res_b1.json",
        ] or names == [
            "job-safe-local/math.foo/res_b1.json",
            "job-safe-local/run_metadata.json",
        ]
        assert all("linked" not in name and "named-pipe" not in name for name in names)

    @pytest.mark.asyncio
    async def test_collected_job_symlink_cannot_escape_root(
        self, client, manager, tmp_path,
    ):
        collected = Path(manager.config.registry.path).with_name("collected")
        collected.mkdir()
        outside = tmp_path / "outside-results"
        outside.mkdir()
        (outside / "secret.txt").write_text("must not escape")
        (collected / "job-link").symlink_to(outside, target_is_directory=True)

        response = await client.get("/api/jobs/job-link/results")

        assert response.status_code == 400
        assert "escapes collected root" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_download_uses_mode_0600_temp_file_and_cleans_it(
        self, client, manager, monkeypatch, tmp_path,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        jid = self._seed(manager, "job-temp")
        real_mkstemp = jobs_route.tempfile.mkstemp
        created: list[tuple[Path, int]] = []

        def recording_mkstemp(*args, **kwargs):
            kwargs["dir"] = tmp_path
            fd, raw_path = real_mkstemp(*args, **kwargs)
            created.append((Path(raw_path), stat.S_IMODE(os.fstat(fd).st_mode)))
            return fd, raw_path

        monkeypatch.setattr(jobs_route.tempfile, "mkstemp", recording_mkstemp)

        response = await client.get(f"/api/jobs/{jid}/results/download")

        assert response.status_code == 200
        assert created and created[0][1] == 0o600
        assert all(not path.exists() for path, _ in created)

    @pytest.mark.asyncio
    async def test_local_score_attempts_and_aggregate_reads_are_bounded(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        base = (
            Path(manager.config.registry.path).with_name("collected")
            / "job-local-score-budget"
        )
        base.mkdir(parents=True)
        for index in range(10):
            (base / f"{index:02}.json").write_text("x" * 8)

        monkeypatch.setattr(jobs_route, "RESULT_SCORE_MAX_ATTEMPTS", 3)
        monkeypatch.setattr(jobs_route, "RESULT_SCORE_TOTAL_READ_BYTES", 16)
        original = jobs_route._read_small_regular_file
        reads = 0

        def recording_read(*args, **kwargs):
            nonlocal reads
            reads += 1
            return original(*args, **kwargs)

        monkeypatch.setattr(
            jobs_route, "_read_small_regular_file", recording_read,
        )
        response = await client.get(
            "/api/jobs/job-local-score-budget/results"
        )

        assert response.status_code == 200
        assert response.json()["scores"] == []
        assert reads == 2

    @pytest.mark.asyncio
    async def test_score_fields_are_scalar_and_bounded(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        base = (
            Path(manager.config.registry.path).with_name("collected")
            / "job-score-scalars"
        )
        base.mkdir(parents=True)
        (base / "oversized.json").write_text(json.dumps({
            "task_id": "x" * 2_000,
            "prompt_level": "b1",
            "status": "done",
            "final_score": 1,
        }))
        (base / "structured.json").write_text(json.dumps({
            "task_id": "task",
            "prompt_level": "b1",
            "status": "done",
            "final_score": {"nested": "not-a-number"},
        }))
        (base / "huge-integer.json").write_text(
            '{"task_id":"task","prompt_level":"b1","status":"done",'
            f'"final_score":{"9" * 5_000}}}'
        )
        (base / "infinite.json").write_text(
            '{"task_id":"task","prompt_level":"b1","status":"done",'
            '"final_score":1e309}'
        )
        (base / "surrogate.json").write_text(json.dumps({
            "task_id": "\ud800",
            "prompt_level": "b1",
            "status": "done",
            "final_score": 2,
        }))
        (base / "c1-control.json").write_text(json.dumps({
            "task_id": "task",
            "prompt_level": "b1",
            "status": "done\u0085",
            "final_score": 2,
        }))
        (base / "valid.json").write_text(json.dumps({
            "task_id": "task",
            "prompt_level": "b1",
            "status": "done",
            "final_score": 3.5,
        }))
        monkeypatch.setattr(jobs_route, "RESULT_SCORE_TEXT_MAX_CHARS", 128)

        response = await client.get("/api/jobs/job-score-scalars/results")

        assert response.status_code == 200
        assert response.json()["scores"] == [{
            "task_id": "task",
            "prompt_level": "b1",
            "status": "done",
            "final_score": 3.5,
        }]

    @pytest.mark.asyncio
    async def test_local_score_read_rejects_replaced_snapshot(
        self, manager,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        base = (
            Path(manager.config.registry.path).with_name("collected")
            / "job-replaced-score"
        )
        base.mkdir(parents=True)
        score = base / "score.json"
        score.write_text('{"final_score":1}')
        listed = score.lstat()
        replacement = base / "replacement.json"
        replacement.write_text('{"final_score":9}')
        replacement.replace(score)

        assert jobs_route._read_small_regular_file(
            score,
            expected_stat=listed,
            max_bytes=2_000_000,
        ) is None

    @pytest.mark.asyncio
    async def test_temp_archive_stream_always_unlinks_on_disconnect(
        self, tmp_path,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        archive = tmp_path / "prepared.tar.gz"
        archive.write_bytes(b"x" * (512 * 1024))
        stream = jobs_route._stream_temp_archive(archive)
        assert await anext(stream)
        await stream.aclose()
        assert not archive.exists()

    @pytest.mark.asyncio
    async def test_temp_archive_response_unlinks_when_body_send_is_cancelled(
        self,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        archive = jobs_route._new_temp_archive(reserve_bytes=1024)
        archive.write_bytes(b"prepared archive")
        response = jobs_route._TemporaryArchiveResponse(
            archive, job_id="job-disconnect",
        )

        async def receive():
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.body":
                raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await response(
                {
                    "type": "http",
                    "asgi": {"version": "3.0", "spec_version": "2.4"},
                    "http_version": "1.1",
                    "method": "GET",
                    "scheme": "http",
                    "path": "/",
                    "raw_path": b"/",
                    "query_string": b"",
                    "headers": [],
                    "client": ("127.0.0.1", 1),
                    "server": ("127.0.0.1", 80),
                    "root_path": "",
                },
                receive,
                send,
            )
        assert not archive.exists()

    @pytest.mark.asyncio
    async def test_temp_archive_read_cancellation_holds_permit_until_read_exits(
        self, monkeypatch, tmp_path,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        read_started = threading.Event()
        allow_read = threading.Event()
        stream_closed = threading.Event()

        class SlowStream:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                stream_closed.set()

            def read(self, _size):
                read_started.set()
                assert allow_read.wait(timeout=5)
                return b"x"

        archive = tmp_path / "slow-prepared.tar.gz"
        archive.write_bytes(b"placeholder")
        original_open = jobs_route.Path.open

        def slow_open(path, *args, **kwargs):
            if path == archive:
                return SlowStream()
            return original_open(path, *args, **kwargs)

        monkeypatch.setattr(jobs_route.Path, "open", slow_open)
        admission = jobs_route._ResultOperationAdmission(1)
        permit = admission.try_acquire()
        assert permit is not None
        stream = jobs_route._stream_temp_archive(
            archive,
            permit=permit,
        )
        pending = asyncio.create_task(anext(stream))
        assert await asyncio.to_thread(read_started.wait, 1)

        pending.cancel()
        await asyncio.sleep(0)
        assert not pending.done()
        assert admission.active == 1
        assert admission.try_acquire() is None

        allow_read.set()
        with pytest.raises(asyncio.CancelledError):
            await pending
        assert stream_closed.is_set()
        assert admission.active == 0
        assert not archive.exists()

    @pytest.mark.asyncio
    async def test_archive_build_cancellation_holds_permit_until_builder_exits(
        self, tmp_path,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        build_started = threading.Event()
        allow_build = threading.Event()
        archive = tmp_path / "late-build.tar.gz"

        def slow_builder() -> Path:
            build_started.set()
            assert allow_build.wait(timeout=5)
            archive.write_bytes(b"archive")
            return archive

        admission = jobs_route._ResultOperationAdmission(1)
        permit = admission.try_acquire()
        assert permit is not None
        build = asyncio.create_task(
            jobs_route._prepare_temp_archive(
                slow_builder,
                permit=permit,
            )
        )
        assert await asyncio.to_thread(build_started.wait, 1)
        build.cancel()
        with pytest.raises(asyncio.CancelledError):
            await build

        assert admission.active == 1
        assert admission.try_acquire() is None
        allow_build.set()

        async def wait_for_cleanup() -> None:
            while admission.active or archive.exists():
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_cleanup(), timeout=1)
        assert admission.active == 0
        assert not archive.exists()

    @pytest.mark.asyncio
    async def test_temp_archive_response_holds_stream_permit_for_slow_client(
        self,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        archive = jobs_route._new_temp_archive(reserve_bytes=1024)
        archive.write_bytes(b"prepared archive")
        admission = jobs_route._ResultOperationAdmission(1)
        permit = admission.try_acquire()
        assert permit is not None
        response = jobs_route._TemporaryArchiveResponse(
            archive,
            job_id="job-slow-client",
            permit=permit,
        )
        body_started = asyncio.Event()
        allow_send = asyncio.Event()

        async def receive():
            await asyncio.Future()

        async def send(message):
            if (
                message["type"] == "http.response.body"
                and message.get("body")
            ):
                body_started.set()
                await allow_send.wait()

        sending = asyncio.create_task(response(
            {
                "type": "http",
                "asgi": {"version": "3.0", "spec_version": "2.4"},
                "http_version": "1.1",
                "method": "GET",
                "scheme": "http",
                "path": "/",
                "raw_path": b"/",
                "query_string": b"",
                "headers": [],
                "client": ("127.0.0.1", 1),
                "server": ("127.0.0.1", 80),
                "root_path": "",
            },
            receive,
            send,
        ))
        await asyncio.wait_for(body_started.wait(), timeout=1)
        assert admission.active == 1
        assert admission.try_acquire() is None

        allow_send.set()
        await asyncio.wait_for(sending, timeout=1)
        assert admission.active == 0
        assert not archive.exists()

    @pytest.mark.asyncio
    async def test_local_archive_rejects_replace_between_list_and_open(
        self, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        base = (
            Path(manager.config.registry.path).with_name("collected")
            / "job-archive-replaced"
        )
        base.mkdir(parents=True)
        target = base / "result.txt"
        target.write_text("listed generation")
        replacement = base / "replacement.txt"
        replacement.write_text("new generation")
        original_new_temp = jobs_route._new_temp_archive
        replaced = False

        def replace_after_listing(*, reserve_bytes):
            nonlocal replaced
            if not replaced:
                replacement.replace(target)
                replaced = True
            return original_new_temp(reserve_bytes=reserve_bytes)

        monkeypatch.setattr(
            jobs_route, "_new_temp_archive", replace_after_listing,
        )
        with pytest.raises(
            jobs_route.LocalResultsUnavailable, match="changed",
        ):
            jobs_route._build_local_archive("job-archive-replaced", base)

    @pytest.mark.asyncio
    async def test_archive_spool_reservations_are_global_and_released(
        self, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        monkeypatch.setattr(
            jobs_route, "RESULT_ARCHIVE_SPOOL_MAX_BYTES", 1_000,
        )
        monkeypatch.setattr(jobs_route, "_RESULT_ARCHIVE_SPOOL_RESERVED", 0)
        jobs_route._RESULT_ARCHIVE_TEMP_RESERVATIONS.clear()

        first = jobs_route._new_temp_archive(reserve_bytes=700)
        try:
            with pytest.raises(
                jobs_route.ResultsSpoolUnavailable, match="budget",
            ):
                jobs_route._new_temp_archive(reserve_bytes=400)
        finally:
            jobs_route._remove_temp_archive(first)

        second = jobs_route._new_temp_archive(reserve_bytes=400)
        jobs_route._remove_temp_archive(second)
        assert jobs_route._RESULT_ARCHIVE_SPOOL_RESERVED == 0

    @pytest.mark.asyncio
    async def test_archive_spool_preserves_real_disk_safety_margin(
        self, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        monkeypatch.setattr(jobs_route, "_RESULT_ARCHIVE_SPOOL_RESERVED", 0)
        jobs_route._RESULT_ARCHIVE_TEMP_RESERVATIONS.clear()
        reserve = 4 * 1024 * 1024
        monkeypatch.setattr(
            jobs_route.shutil,
            "disk_usage",
            lambda _path: SimpleNamespace(
                free=(
                    jobs_route.RESULT_ARCHIVE_DISK_SAFETY_BYTES
                    + reserve
                    - 1
                ),
            ),
        )

        with pytest.raises(
            jobs_route.ResultsSpoolUnavailable, match="safety margin",
        ):
            jobs_route._new_temp_archive(reserve_bytes=reserve)

        assert jobs_route._RESULT_ARCHIVE_SPOOL_RESERVED == 0
        assert jobs_route._RESULT_ARCHIVE_TEMP_RESERVATIONS == {}

    @pytest.mark.asyncio
    async def test_archive_disk_admission_counts_unwritten_reservations(
        self, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        monkeypatch.setattr(jobs_route, "_RESULT_ARCHIVE_SPOOL_RESERVED", 0)
        jobs_route._RESULT_ARCHIVE_TEMP_RESERVATIONS.clear()
        monkeypatch.setattr(
            jobs_route.shutil,
            "disk_usage",
            lambda _path: SimpleNamespace(free=19_000_000_000),
        )
        first_reserve = 6 * 1024 * 1024
        first = jobs_route._new_temp_archive(reserve_bytes=first_reserve)
        try:
            second_reserve = 3 * 1024 * 1024
            monkeypatch.setattr(
                jobs_route.shutil,
                "disk_usage",
                lambda _path: SimpleNamespace(
                    free=(
                        jobs_route.RESULT_ARCHIVE_DISK_SAFETY_BYTES
                        + first_reserve
                        + second_reserve
                        - 1
                    ),
                ),
            )
            with pytest.raises(
                jobs_route.ResultsSpoolUnavailable, match="safety margin",
            ):
                jobs_route._new_temp_archive(
                    reserve_bytes=second_reserve,
                )
        finally:
            jobs_route._remove_temp_archive(first)

    @pytest.mark.asyncio
    async def test_archive_spool_reservation_bounds_long_pax_paths(self):
        from elastic_agent.api.routes import jobs as jobs_route

        members = [
            (("目录" * 300) + f"/result-{index}.json", index % 17)
            for index in range(128)
        ]
        reservation = jobs_route._archive_spool_reservation(
            "job-" + ("x" * 124),
            members,
        )

        payload = io.BytesIO()
        with tarfile.open(fileobj=payload, mode="w:gz") as archive:
            for relative, size in members:
                info = tarfile.TarInfo(
                    name=f"job-{'x' * 124}/{relative}"
                )
                info.size = size
                archive.addfile(info, io.BytesIO(b"x" * size))

        assert len(payload.getvalue()) <= reservation
        # The old source+1%-plus-1KiB/member estimate did not account for
        # multi-block PAX path records.
        old_estimate = (
            sum(size for _, size in members)
            + sum(size for _, size in members) // 100
            + len(members) * 1024
            + 1024 * 1024
        )
        assert reservation > old_estimate

    @pytest.mark.asyncio
    async def test_live_archive_rejects_saturation_before_allocating_pipe(
        self, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(
            jobs_route, "_RESULT_ARCHIVE_STREAM_ADMISSION", admission,
        )
        held = admission.try_acquire()
        assert held is not None
        pipe_called = False
        original_pipe = jobs_route.os.pipe

        def recording_pipe():
            nonlocal pipe_called
            pipe_called = True
            return original_pipe()

        monkeypatch.setattr(jobs_route.os, "pipe", recording_pipe)
        stream = jobs_route._stream_s3_archive("job-queued", [])
        try:
            with pytest.raises(
                HTTPException, match="capacity is currently exhausted",
            ) as exc_info:
                await anext(stream)
            assert exc_info.value.status_code == 503
            assert exc_info.value.headers == {"Retry-After": "1"}
        finally:
            await stream.aclose()
            held.release()
        assert pipe_called is False
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_live_response_start_cancellation_releases_unstarted_permit(
        self,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        admission = jobs_route._ResultOperationAdmission(1)
        permit = admission.try_acquire()
        assert permit is not None
        owner = jobs_route._LiveArchivePermitOwner(permit)
        iterator = jobs_route._stream_s3_archive(
            "job-unstarted",
            [],
            permit=permit,
            owner=owner,
        )
        response = jobs_route._LiveArchiveResponse(
            iterator,
            owner=owner,
            headers={},
        )

        async def receive():
            return {"type": "http.disconnect"}

        async def send(message):
            if message["type"] == "http.response.start":
                raise asyncio.CancelledError

        with pytest.raises(asyncio.CancelledError):
            await response(
                {
                    "type": "http",
                    "asgi": {"version": "3.0", "spec_version": "2.4"},
                    "http_version": "1.1",
                    "method": "GET",
                    "scheme": "http",
                    "path": "/",
                    "raw_path": b"/",
                    "query_string": b"",
                    "headers": [],
                    "client": ("127.0.0.1", 1),
                    "server": ("127.0.0.1", 80),
                    "root_path": "",
                },
                receive,
                send,
            )

        assert owner.started is False
        assert admission.active == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("route", "admission_name"),
        [
            (
                "/api/jobs/job-saturated/results/download",
                "_RESULT_ARCHIVE_BUILD_ADMISSION",
            ),
            (
                "/api/jobs/job-saturated/results/download/stream",
                "_RESULT_ARCHIVE_STREAM_ADMISSION",
            ),
        ],
    )
    async def test_archive_routes_reject_saturation_before_s3_list(
        self, client, monkeypatch, route, admission_name,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(jobs_route, admission_name, admission)
        held = admission.try_acquire()
        assert held is not None
        list_calls = 0

        def unexpected_list(*_args, **_kwargs):
            nonlocal list_calls
            list_calls += 1
            return []

        monkeypatch.setattr(jobs_route, "_s3_list_job", unexpected_list)
        try:
            response = await client.get(route)
        finally:
            held.release()

        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"
        assert list_calls == 0
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_result_read_saturation_is_rejected_before_s3_list(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(jobs_route, "_RESULT_READ_ADMISSION", admission)
        held = admission.try_acquire()
        assert held is not None
        list_calls = 0

        def unexpected_list(*_args, **_kwargs):
            nonlocal list_calls
            list_calls += 1
            return []

        monkeypatch.setattr(jobs_route, "_s3_list_job", unexpected_list)
        try:
            response = await client.get(
                "/api/jobs/job-saturated/results"
            )
        finally:
            held.release()

        assert response.status_code == 503
        assert response.headers["retry-after"] == "1"
        assert list_calls == 0
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_cancelled_result_list_holds_permit_until_thread_exits(
        self, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        list_started = threading.Event()
        allow_list = threading.Event()

        def slow_list(*_args, **_kwargs):
            list_started.set()
            assert allow_list.wait(timeout=5)
            return []

        admission = jobs_route._ResultOperationAdmission(1)
        monkeypatch.setattr(jobs_route, "_RESULT_READ_ADMISSION", admission)
        monkeypatch.setattr(jobs_route, "_s3_list_job", slow_list)
        request = asyncio.create_task(
            jobs_route.job_results("job-cancelled-list")
        )
        assert await asyncio.to_thread(list_started.wait, 1)
        request.cancel()
        await asyncio.sleep(0)

        assert not request.done()
        assert admission.active == 1
        with pytest.raises(HTTPException) as saturated:
            await jobs_route.job_results("job-second")
        assert saturated.value.status_code == 503

        allow_list.set()
        with pytest.raises(asyncio.CancelledError):
            await request
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_missing_results_404(self, client):
        assert (await client.get("/api/jobs/nope/results")).status_code == 404

    @pytest.mark.asyncio
    async def test_list_all_results(self, client, manager):
        self._seed(manager, "job-a")
        self._seed(manager, "job-b")
        r = await client.get("/api/results")
        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 2
        assert {j["job_id"] for j in body["jobs"]} == {"job-a", "job-b"}
        assert all(j["file_count"] == 2 for j in body["jobs"])
        assert body["truncated"] is False

    @pytest.mark.asyncio
    async def test_global_local_summary_directory_scan_is_bounded(
        self, client, manager, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        collected = Path(manager.collected_root)
        for index in range(3):
            job = collected / f"job-local-{index}"
            job.mkdir(parents=True)
            (job / "result.txt").write_text("x")
        monkeypatch.setattr(
            jobs_route,
            "RESULT_SUMMARY_MAX_DIRECTORY_ENTRIES",
            2,
        )

        response = await client.get("/api/results")

        assert response.status_code == 200
        assert response.json()["truncated"] is True
        assert response.json()["total"] <= 2

    @pytest.mark.asyncio
    async def test_global_s3_root_entry_scan_is_bounded(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        class RootPaginator:
            def paginate(self, **kwargs):
                assert kwargs["Delimiter"] == "/"
                return [{"Contents": [
                    {"Key": f"jobs/root-marker-{index}"}
                    for index in range(3)
                ]}]

        class RootS3:
            def get_paginator(self, _name):
                return RootPaginator()

        monkeypatch.setenv(
            "ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket",
        )
        monkeypatch.setattr(jobs_route, "_s3_client", lambda: RootS3())
        monkeypatch.setattr(
            jobs_route, "RESULT_SUMMARY_MAX_S3_ROOT_ENTRIES", 2,
        )

        response = await client.get("/api/results")

        assert response.status_code == 200
        assert response.json() == {
            "jobs": [],
            "total": 0,
            "truncated": True,
        }

    @pytest.mark.asyncio
    async def test_global_s3_summary_has_aggregate_object_budget(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        class SummaryPaginator:
            def paginate(self, **kwargs):
                if kwargs.get("Delimiter") == "/":
                    return [{"CommonPrefixes": [
                        {"Prefix": "jobs/job-one/"},
                        {"Prefix": "jobs/job-two/"},
                    ]}]
                prefix = kwargs["Prefix"]
                job_id = prefix.rstrip("/").rsplit("/", 1)[-1]
                return [{"Contents": [{
                    "Key": f"{prefix}result.txt",
                    "Size": 1,
                    "ETag": f'"{job_id}"',
                }]}]

        class SummaryS3:
            def get_paginator(self, _name):
                return SummaryPaginator()

        monkeypatch.setenv(
            "ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket",
        )
        monkeypatch.setattr(jobs_route, "_s3_client", lambda: SummaryS3())
        monkeypatch.setattr(
            jobs_route, "RESULT_SUMMARY_MAX_OBJECTS", 1,
        )

        response = await client.get("/api/results")

        assert response.status_code == 200
        assert response.json()["truncated"] is True
        assert response.json()["total"] == 1
        assert response.json()["jobs"][0]["file_count"] == 1

    @pytest.mark.asyncio
    async def test_configured_s3_list_failure_is_explicit_503(
        self, client, monkeypatch,
    ):
        class BrokenPaginator:
            def paginate(self, **kwargs):
                raise RuntimeError("AccessDenied")

        class BrokenS3:
            def get_paginator(self, name):
                return BrokenPaginator()

        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(
            "elastic_agent.api.routes.jobs._s3_client", lambda: BrokenS3(),
        )

        r = await client.get("/api/jobs/job-s3/results")
        assert r.status_code == 503
        assert "AccessDenied" in r.json()["detail"]

    @pytest.mark.asyncio
    async def test_s3_unsafe_relative_object_key_is_rejected(
        self, client, monkeypatch,
    ):
        class UnsafePaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [{
                    "Key": "jobs/job-s3/../../manager-secret",
                    "Size": 1,
                }]}]

        class UnsafeS3:
            def get_paginator(self, name):
                return UnsafePaginator()

        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(
            "elastic_agent.api.routes.jobs._s3_client", lambda: UnsafeS3(),
        )

        response = await client.get("/api/jobs/job-s3/results")

        assert response.status_code == 503
        assert "unsafe object key" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_s3_result_without_etag_fails_before_get(
        self, client, monkeypatch,
    ):
        class MissingETagPaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [{
                    "Key": "jobs/job-no-etag/result.txt",
                    "Size": 1,
                }]}]

        class MissingETagS3:
            get_calls = 0

            def get_paginator(self, _name):
                return MissingETagPaginator()

            def get_object(self, **kwargs):
                self.get_calls += 1
                raise AssertionError("GET must not run without immutable ETag")

        s3 = MissingETagS3()
        monkeypatch.setenv(
            "ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket",
        )
        monkeypatch.setattr(
            "elastic_agent.api.routes.jobs._s3_client",
            lambda: s3,
        )

        response = await client.get(
            "/api/jobs/job-no-etag/results/download"
        )

        assert response.status_code == 503
        assert "immutable ETag" in response.json()["detail"]
        assert s3.get_calls == 0

    @pytest.mark.asyncio
    async def test_s3_result_page_scan_is_bounded(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        class EmptyPagePaginator:
            def paginate(self, **kwargs):
                return [
                    {"Contents": []},
                    {"Contents": []},
                    {"Contents": []},
                ]

        class EmptyPageS3:
            def get_paginator(self, _name):
                return EmptyPagePaginator()

        monkeypatch.setenv(
            "ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket",
        )
        monkeypatch.setattr(
            jobs_route, "_s3_client", lambda: EmptyPageS3(),
        )
        monkeypatch.setattr(jobs_route, "RESULT_LIST_MAX_S3_PAGES", 2)

        response = await client.get("/api/jobs/job-many-pages/results")

        assert response.status_code == 413
        assert "more than 2 pages" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_s3_download_streams_objects_without_full_body_read(
        self, client, monkeypatch,
    ):
        class StreamingBody(io.BytesIO):
            def __init__(self, payload):
                super().__init__(payload)
                self.full_reads = 0

            def read(self, size=-1):
                if size is None or size < 0:
                    self.full_reads += 1
                    raise AssertionError("archive attempted an unbounded object read")
                return super().read(size)

        body = StreamingBody(b"streamed-result")

        class OneObjectPaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [{
                    "Key": "jobs/job-stream/output.txt",
                    "Size": len(b"streamed-result"),
                    "ETag": '"streamed-etag"',
                }]}]

        class StreamingS3:
            def get_paginator(self, name):
                return OneObjectPaginator()

            def get_object(self, **kwargs):
                return {
                    "Body": body,
                    "ContentLength": len(b"streamed-result"),
                    "ETag": '"streamed-etag"',
                }

        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(
            "elastic_agent.api.routes.jobs._s3_client", lambda: StreamingS3(),
        )

        response = await client.get("/api/jobs/job-stream/results/download")

        assert response.status_code == 200
        assert body.full_reads == 0
        with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:gz") as archive:
            assert archive.extractfile("job-stream/output.txt").read() == b"streamed-result"

    @pytest.mark.asyncio
    async def test_s3_live_download_returns_stream_metadata_and_valid_archive(
        self, client, monkeypatch,
    ):
        class OneObjectPaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [{
                    "Key": "jobs/job-live/output.txt",
                    "Size": len(b"live-result"),
                    "ETag": '"live-etag"',
                }]}]

        class StreamingS3:
            def get_paginator(self, name):
                return OneObjectPaginator()

            def get_object(self, **kwargs):
                assert kwargs["IfMatch"] == '"live-etag"'
                return {
                    "Body": io.BytesIO(b"live-result"),
                    "ContentLength": len(b"live-result"),
                    "ETag": '"live-etag"',
                }

        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(
            "elastic_agent.api.routes.jobs._s3_client", lambda: StreamingS3(),
        )

        response = await client.get(
            "/api/jobs/job-live/results/download/stream"
        )

        assert response.status_code == 200
        assert response.headers["content-type"] == "application/gzip"
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["x-elastic-agent-object-count"] == "1"
        assert response.headers["x-elastic-agent-source-bytes"] == str(
            len(b"live-result")
        )
        with tarfile.open(
            fileobj=io.BytesIO(response.content), mode="r:gz"
        ) as archive:
            assert archive.extractfile("job-live/output.txt").read() == b"live-result"

    @pytest.mark.asyncio
    async def test_s3_live_archive_yields_before_later_object_finishes(
        self, monkeypatch,
    ):
        import threading

        from elastic_agent.api.routes import jobs as jobs_route

        second_started = threading.Event()
        allow_second = threading.Event()
        first_payload = os.urandom(32 * 1_024)

        class SequencedS3:
            def get_object(self, **kwargs):
                if kwargs["Key"].endswith("/second.txt"):
                    second_started.set()
                    assert allow_second.wait(timeout=5)
                    payload = b"second"
                else:
                    payload = first_payload
                return {
                    "Body": io.BytesIO(payload),
                    "ContentLength": len(payload),
                    "ETag": kwargs["IfMatch"],
                }

        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(jobs_route, "_s3_client", lambda: SequencedS3())
        objects = [
            (
                "first.txt",
                len(first_payload),
                "jobs/job-early/first.txt",
                '"early-first"',
            ),
            (
                "second.txt",
                6,
                "jobs/job-early/second.txt",
                '"early-second"',
            ),
        ]
        stream = jobs_route._stream_s3_archive("job-early", objects)

        try:
            first_chunk_task = asyncio.create_task(anext(stream))
            assert await asyncio.to_thread(second_started.wait, 1)
            first_chunk = await asyncio.wait_for(first_chunk_task, timeout=1)
            assert first_chunk
            allow_second.set()
            remaining = [chunk async for chunk in stream]
        finally:
            allow_second.set()
            await stream.aclose()

        payload = first_chunk + b"".join(remaining)
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            assert (
                archive.extractfile("job-early/first.txt").read()
                == first_payload
            )
            assert archive.extractfile("job-early/second.txt").read() == b"second"

    @pytest.mark.asyncio
    async def test_s3_live_archive_cancel_survives_active_body_close_failure(
        self, monkeypatch,
    ):
        import threading

        from elastic_agent.api.routes import jobs as jobs_route

        read_started = threading.Event()
        body_closed = threading.Event()
        release_read = threading.Event()
        first_payload = os.urandom(32 * 1_024)

        class BlockingBody:
            def read(self, size=-1):
                read_started.set()
                release_read.wait(timeout=5)
                return b""

            def close(self):
                body_closed.set()
                release_read.set()
                raise OSError("simulated close failure")

        class SequencedS3:
            def get_object(self, **kwargs):
                if kwargs["Key"].endswith("/blocked.txt"):
                    return {
                        "Body": BlockingBody(),
                        "ContentLength": 1,
                        "ETag": kwargs["IfMatch"],
                    }
                return {
                    "Body": io.BytesIO(first_payload),
                    "ContentLength": len(first_payload),
                    "ETag": kwargs["IfMatch"],
                }

        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(jobs_route, "_s3_client", lambda: SequencedS3())
        objects = [
            (
                "first.txt",
                len(first_payload),
                "jobs/job-cancel/first.txt",
                '"cancel-first"',
            ),
            (
                "blocked.txt",
                1,
                "jobs/job-cancel/blocked.txt",
                '"cancel-blocked"',
            ),
        ]
        admission = jobs_route._ResultOperationAdmission(1)
        permit = admission.try_acquire()
        assert permit is not None
        stream = jobs_route._stream_s3_archive(
            "job-cancel",
            objects,
            permit=permit,
        )

        try:
            first_chunk = await asyncio.wait_for(anext(stream), timeout=1)
            assert first_chunk
            assert await asyncio.to_thread(read_started.wait, 1)
            assert admission.active == 1
        finally:
            await stream.aclose()

        assert await asyncio.to_thread(body_closed.wait, 1)

        async def wait_for_release() -> None:
            while admission.active:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_release(), timeout=1)
        assert admission.active == 0

    @pytest.mark.asyncio
    async def test_s3_score_reads_are_bounded_and_attempt_limited(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        class RecordingBody(io.BytesIO):
            def __init__(self, payload):
                super().__init__(payload)
                self.read_sizes: list[int] = []

            def read(self, size=-1):
                self.read_sizes.append(size)
                if size is None or size < 0:
                    raise AssertionError("score parser attempted an unbounded read")
                return super().read(size)

        class ScorePaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [
                    {
                        "Key": f"jobs/job-scores/bad-{index}.json",
                        "Size": 1,
                        "ETag": f'"score-{index}"',
                    }
                    for index in range(5)
                ]}]

        class ScoreS3:
            def __init__(self):
                self.get_calls = 0
                self.bodies: list[RecordingBody] = []

            def get_paginator(self, name):
                return ScorePaginator()

            def get_object(self, **kwargs):
                self.get_calls += 1
                body = RecordingBody(b"x")  # invalid JSON must still count
                self.bodies.append(body)
                return {
                    "Body": body,
                    "ContentLength": 1,
                    "ETag": kwargs["IfMatch"],
                }

        s3 = ScoreS3()
        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(jobs_route, "_s3_client", lambda: s3)
        monkeypatch.setattr(jobs_route, "RESULT_SCORE_MAX_ATTEMPTS", 2)

        response = await client.get("/api/jobs/job-scores/results")

        assert response.status_code == 200
        assert response.json()["scores"] == []
        assert s3.get_calls == 2
        assert all(
            size >= 0
            for body in s3.bodies
            for size in body.read_sizes
        )

    @pytest.mark.asyncio
    async def test_s3_score_reads_share_an_aggregate_byte_budget(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        payload = b"not-json"

        class ScorePaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [
                    {
                        "Key": f"jobs/job-score-bytes/{index}.json",
                        "Size": len(payload),
                        "ETag": f'"score-bytes-{index}"',
                    }
                    for index in range(10)
                ]}]

        class ScoreS3:
            def __init__(self):
                self.get_calls = 0

            def get_paginator(self, name):
                return ScorePaginator()

            def get_object(self, **kwargs):
                self.get_calls += 1
                return {
                    "Body": io.BytesIO(payload),
                    "ContentLength": len(payload),
                    "ETag": kwargs["IfMatch"],
                }

        s3 = ScoreS3()
        monkeypatch.setenv(
            "ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket",
        )
        monkeypatch.setattr(jobs_route, "_s3_client", lambda: s3)
        monkeypatch.setattr(jobs_route, "RESULT_SCORE_MAX_ATTEMPTS", 10)
        monkeypatch.setattr(
            jobs_route, "RESULT_SCORE_TOTAL_READ_BYTES", len(payload) * 2,
        )

        response = await client.get("/api/jobs/job-score-bytes/results")

        assert response.status_code == 200
        assert response.json()["scores"] == []
        assert s3.get_calls == 2

    @pytest.mark.asyncio
    async def test_s3_download_fails_closed_when_object_changes_after_list(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        class ChangedPaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [{
                    "Key": "jobs/job-changing/output.txt",
                    "Size": 3,
                    "ETag": '"listed-version"',
                }]}]

        class ChangedS3:
            def __init__(self):
                self.get_kwargs = None

            def get_paginator(self, name):
                return ChangedPaginator()

            def get_object(self, **kwargs):
                self.get_kwargs = kwargs
                return {
                    "Body": io.BytesIO(b"new-larger-content"),
                    "ContentLength": len(b"new-larger-content"),
                    "ETag": '"new-version"',
                }

        s3 = ChangedS3()
        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(jobs_route, "_s3_client", lambda: s3)

        response = await client.get("/api/jobs/job-changing/results/download")

        assert response.status_code == 503
        assert "changed" in response.json()["detail"]
        assert s3.get_kwargs["IfMatch"] == '"listed-version"'

    @pytest.mark.asyncio
    async def test_s3_download_eof_probe_rejects_larger_body_without_metadata(
        self, client, monkeypatch,
    ):
        class ChangedPaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [{
                    "Key": "jobs/job-changing-no-meta/output.txt",
                    "Size": 3,
                    "ETag": '"stable-identity"',
                }]}]

        class MetadataFreeS3:
            def get_paginator(self, name):
                return ChangedPaginator()

            def get_object(self, **kwargs):
                return {
                    "Body": io.BytesIO(b"abcdef"),
                    "ETag": '"stable-identity"',
                }

        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(
            "elastic_agent.api.routes.jobs._s3_client",
            lambda: MetadataFreeS3(),
        )

        response = await client.get(
            "/api/jobs/job-changing-no-meta/results/download"
        )

        assert response.status_code == 503
        assert "became larger" in response.json()["detail"]

    @pytest.mark.asyncio
    async def test_s3_download_enforces_object_and_byte_limits_before_get(
        self, client, monkeypatch,
    ):
        from elastic_agent.api.routes import jobs as jobs_route

        class ObjectsPaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [
                    {
                        "Key": "jobs/job-limit/a",
                        "Size": 2,
                        "ETag": '"limit-a"',
                    },
                    {
                        "Key": "jobs/job-limit/b",
                        "Size": 2,
                        "ETag": '"limit-b"',
                    },
                ]}]

        class ObjectsS3:
            get_calls = 0

            def get_paginator(self, name):
                return ObjectsPaginator()

            def get_object(self, **kwargs):
                self.get_calls += 1
                raise AssertionError("limits must be checked before object download")

        s3 = ObjectsS3()
        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(jobs_route, "_s3_client", lambda: s3)
        monkeypatch.setattr(jobs_route, "RESULT_ARCHIVE_MAX_OBJECTS", 1)
        monkeypatch.setattr(jobs_route, "RESULT_ARCHIVE_MAX_BYTES", 100)

        too_many = await client.get("/api/jobs/job-limit/results/download")
        assert too_many.status_code == 413

        monkeypatch.setattr(jobs_route, "RESULT_ARCHIVE_MAX_OBJECTS", 10)
        monkeypatch.setattr(jobs_route, "RESULT_ARCHIVE_MAX_BYTES", 3)
        too_large = await client.get("/api/jobs/job-limit/results/download")
        assert too_large.status_code == 413
        assert s3.get_calls == 0

    @pytest.mark.asyncio
    async def test_s3_download_object_failure_is_not_silently_skipped(
        self, client, monkeypatch,
    ):
        class OneObjectPaginator:
            def paginate(self, **kwargs):
                return [{"Contents": [{
                    "Key": "jobs/job-s3/workers/shard-00000/result.json",
                    "Size": 2,
                    "ETag": '"broken-get"',
                }]}]

        class BrokenGetS3:
            def get_paginator(self, name):
                return OneObjectPaginator()

            def get_object(self, **kwargs):
                raise RuntimeError("NoSuchKey")

        monkeypatch.setenv("ELASTIC_AGENT_RESULTS_S3_BUCKET", "result-bucket")
        monkeypatch.setattr(
            "elastic_agent.api.routes.jobs._s3_client", lambda: BrokenGetS3(),
        )

        r = await client.get("/api/jobs/job-s3/results/download")
        assert r.status_code == 503
        assert "NoSuchKey" in r.json()["detail"]


class TestBatchConsoleUI:
    @pytest.mark.asyncio
    async def test_batch_page_served(self, client):
        r = await client.get("/batch")
        assert r.status_code == 200
        assert "Batch Console" in r.text
        assert "Submit Job" in r.text
        assert "Accounts" in r.text
        assert 'id="jProfile"' in r.text
        assert 'id="jRepoRef"' in r.text
        assert 'id="jRunTimeout"' in r.text
        assert 'id="jSpot"' in r.text
        assert 'id="jSecretEnv"' in r.text
        assert "aws-secretsmanager://" in r.text
        assert "currently not support" not in r.text.lower()
