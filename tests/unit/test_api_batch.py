"""Tests for the accounts + jobs REST API (frontend backend)."""

from __future__ import annotations

import hashlib
import io
import json
import os
import stat
import tarfile
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.account_binding import AccountBinding, BindingState
from elastic_agent.core.config import ElasticAgentConfig
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
                await self.persist_spec_hook(job.job_id, job.spec)
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
        }
        assert manager.binding_manager.decommissioned == ["a"]
        assert (await client.get("/api/accounts/a/binding")).status_code == 404
        # Identity CRUD stays separate and becomes available after explicit
        # infrastructure decommissioning.
        assert (await client.delete("/api/accounts/a")).status_code == 200

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
    async def test_list_and_get(self, client):
        submitted = (await client.post("/api/jobs", json=self._SPEC)).json()
        jid = submitted["job_id"]

        lst = (await client.get("/api/jobs")).json()
        assert lst["total"] == 1

        detail = (await client.get(f"/api/jobs/{jid}")).json()
        assert detail["job_id"] == jid
        assert detail["spec"]["name"] == "ai4sci"

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
        assert (await client.get("/api/jobs/nope")).status_code == 404

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
            "bypasses the account's durable EIP" in warning
            for warning in response.json()["warnings"]
        )

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
        assert body["scores"] == [
            {"task_id": "math.foo", "prompt_level": "b1", "status": "completed", "final_score": 39.06}
        ]

    @pytest.mark.asyncio
    async def test_download_tarball(self, client, manager):
        jid = self._seed(manager, "job-dl")
        r = await client.get(f"/api/jobs/{jid}/results/download")
        assert r.status_code == 200
        assert r.headers["content-type"] == "application/gzip"
        assert "job-dl-results.tar.gz" in r.headers["content-disposition"]
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
                }]}]

        class StreamingS3:
            def get_paginator(self, name):
                return OneObjectPaginator()

            def get_object(self, **kwargs):
                return {"Body": body}

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
                    {"Key": f"jobs/job-scores/bad-{index}.json", "Size": 1}
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
                return {"Body": body, "ContentLength": 1}

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
                }]}]

        class MetadataFreeS3:
            def get_paginator(self, name):
                return ChangedPaginator()

            def get_object(self, **kwargs):
                return {"Body": io.BytesIO(b"abcdef")}

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
                    {"Key": "jobs/job-limit/a", "Size": 2},
                    {"Key": "jobs/job-limit/b", "Size": 2},
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
