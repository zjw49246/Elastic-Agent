"""Contract tests for durable JSON Job batch submission.

These tests deliberately replace the real BatchOrchestrator with a small
recording implementation.  The API, preflight, idempotency, queue, and
journal boundaries are exercised without creating a cloud instance or
dispatching a Worker command.
"""

from __future__ import annotations

import asyncio
import copy
import hashlib
import json
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient

from elastic_agent.api.app import create_app
from elastic_agent.api.auth import reset_api_keys
from elastic_agent.core.batch_orchestrator import (
    BatchJob,
    WorkerPhase,
    WorkerRun,
)
from elastic_agent.core.config import ElasticAgentConfig
from elastic_agent.core.job_batch import (
    JobBatchLimits,
    JobBatchManifest,
    aggregate_manifest,
)
from elastic_agent.core.job_spec import JobSpec, WorkerContext
from elastic_agent.core.providers.base import (
    CloudProvider,
    Instance,
    InstanceConfig,
)
from elastic_agent.harness.generic import resolve_harness
from elastic_agent.manager.manager import ElasticAgentManager

API_KEY = "test-job-batch-key"
TWO_MIB = 2 * 1024 * 1024


class NoCloudProvider(CloudProvider):
    """Provider that records accidental cloud mutation and never performs it."""

    def __init__(self) -> None:
        self.create_calls = 0

    @property
    def platform(self) -> str:
        return "test"

    async def create_instance(self, config: InstanceConfig) -> Instance:
        self.create_calls += 1
        raise AssertionError("Job batch API tests must not create cloud instances")

    async def terminate_instance(self, instance_id: str) -> None: ...
    async def start_instance(self, instance_id: str) -> None: ...
    async def stop_instance(self, instance_id: str) -> None: ...
    async def reboot_instance(self, instance_id: str) -> None: ...
    async def list_instances(self, filters=None):
        return []

    async def get_instance(self, instance_id):
        return None

    async def wait_until_running(self, instance_id, timeout=300):
        return None


class RecordingBatch:
    """In-memory submit target with controllable terminal and failure states."""

    def __init__(self, persist_spec_hook: Callable[..., Any]) -> None:
        self._jobs: dict[str, BatchJob] = {}
        self._sequence = 0
        self.persist_spec_hook = persist_spec_hook
        self.started: list[str] = []
        self.started_names: list[str] = []
        self.fail_names: set[str] = set()
        self.started_event = asyncio.Event()

    def prepare(self, spec) -> BatchJob:
        self._sequence += 1
        return BatchJob(
            job_id=f"job-recording-{self._sequence:04d}",
            spec=spec,
            harness=resolve_harness(spec),
        )

    async def submit_prepared(self, job: BatchJob) -> BatchJob:
        if job.spec.name in self.fail_names:
            raise RuntimeError(f"controlled submit failure for {job.spec.name}")
        if self.persist_spec_hook is not None:
            await self.persist_spec_hook(
                job.job_id,
                job.spec,
                job.request_fingerprint,
            )
        job.runs["worker-0"] = WorkerRun(
            worker_id="worker-0",
            ctx=WorkerContext(shard_index=0),
            phase=WorkerPhase.RUNNING,
        )
        job.started_at = job.created_at
        job.persisted_state = "running"
        self._jobs[job.job_id] = job
        self.started.append(job.job_id)
        self.started_names.append(job.spec.name)
        self.started_event.set()
        return job

    async def submit(self, spec) -> BatchJob:
        return await self.submit_prepared(self.prepare(spec))

    async def launch(self, spec) -> BatchJob:
        return await self.submit(spec)

    async def submit_payload(
        self,
        raw_spec: dict[str, object],
        idempotency_key: str,
    ) -> dict[str, Any]:
        """Test double for the already-covered canonical /jobs boundary."""

        job_id = "job-" + hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:32]
        existing = self._jobs.get(job_id)
        if existing is not None:
            return {**existing.summary(), "idempotent_replay": True}
        spec = JobSpec.model_validate(raw_spec)
        job = self.prepare(spec)
        job.job_id = job_id
        await self.submit_prepared(job)
        return job.summary()

    def list_jobs(self) -> list[BatchJob]:
        return list(self._jobs.values())

    def get_job(self, job_id: str) -> BatchJob | None:
        return self._jobs.get(job_id)

    def finish(self, job_id: str, *, failed: bool = False) -> None:
        job = self._jobs[job_id]
        phase = WorkerPhase.FAILED if failed else WorkerPhase.DONE
        for run in job.runs.values():
            run.phase = phase
            run.cleaned_up = True
            if failed:
                run.error = "controlled runtime failure"
        job.error = "controlled runtime failure" if failed else None
        job.launch_complete = True
        job.resources_released = True
        job.accounts_released = True
        job.persisted_state = "failed" if failed else "succeeded"

    async def shutdown(self) -> None:
        return None


def _spec(
    name: str,
    *,
    workers: int = 1,
    ttl_seconds: int = 3_600,
    secret_marker: str | None = None,
) -> dict[str, object]:
    run: dict[str, object] = {"command": "true", "timeout": ttl_seconds}
    if secret_marker is not None:
        run["env"] = {"PRIVATE_MARKER": secret_marker}
        run["secret_env"] = {
            "TOKEN": "aws-secretsmanager://prod/job-batch-token#value",
        }
    return {
        "name": name,
        "run": run,
        "fanout": {"workers": workers},
        "account": {"mode": "none"},
        "ttl_seconds": ttl_seconds,
    }


def _manifest(
    *specs: dict[str, object],
    batch_id: str = "batch-contract-001",
    max_active_jobs: int = 2,
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "batch_id": batch_id,
        "policy": {
            "max_active_jobs": max_active_jobs,
            "on_job_failure": "continue",
        },
        "jobs": [{"client_id": f"client-{index:03d}", "spec": spec} for index, spec in enumerate(specs, start=1)],
    }


async def _wait_for(
    predicate: Callable[[], bool],
    *,
    timeout: float = 3.0,
) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def _stop_job_batch_queue(manager: ElasticAgentManager) -> None:
    queue = getattr(manager, "job_batch_queue", None)
    if queue is None:
        queue = getattr(manager, "_job_batch_queue", None)
    if queue is None:
        return
    stop = getattr(queue, "stop", None) or getattr(queue, "shutdown", None)
    if stop is not None:
        await stop()


def _new_manager(state_dir: Path) -> tuple[ElasticAgentManager, NoCloudProvider]:
    config = ElasticAgentConfig()
    config.registry.path = str(state_dir / "registry.json")
    config.task_registry.path = str(state_dir / "task-registry.json")
    config.credentials.accounts_file = str(state_dir / "accounts.json")
    config.credentials.pool_status_file = str(state_dir / "pool-status.json")
    config.logging.operations_log = str(state_dir / "operations.log")
    config.webhook.dead_letter_path = str(state_dir / "webhook-dead-letters.json")
    config.provider.type = "aws"
    config.provider.aws.region = "us-west-2"
    config.provider.aws.default_instance_type = "t3.medium"
    config.provider.aws.max_instances = 30
    provider = NoCloudProvider()
    return ElasticAgentManager(config, provider), provider


@pytest.fixture(autouse=True)
def _safe_settings(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ELASTIC_AGENT_EXTERNAL_API_KEYS", API_KEY)
    monkeypatch.setenv("ELASTIC_AGENT_MAX_JOB_BATCH_ITEMS", "20")
    monkeypatch.setenv("ELASTIC_AGENT_MAX_JOB_BATCH_TOTAL_WORKERS", "100")
    monkeypatch.setenv("ELASTIC_AGENT_MAX_JOB_BATCH_WORKER_HOURS", "1440")
    monkeypatch.setenv("ELASTIC_AGENT_JOB_BATCH_MAX_ACTIVE_JOBS", "3")
    reset_api_keys()
    yield
    reset_api_keys()


@pytest.fixture
async def batch_client(
    tmp_path: Path,
) -> AsyncIterator[tuple[AsyncClient, ElasticAgentManager, RecordingBatch]]:
    manager, provider = _new_manager(tmp_path)
    batch = RecordingBatch(manager._persist_batch_job_spec)
    manager._batch = batch
    manager.job_batch_queue._poll_interval_seconds = 0.05
    await manager.start()
    manager.job_batch_queue._submitter = batch.submit_payload
    app = create_app(manager)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
        headers={"Authorization": f"Bearer {API_KEY}"},
    ) as client:
        yield client, manager, batch
    await _stop_job_batch_queue(manager)
    await manager.stop()
    assert provider.create_calls == 0


class TestJobBatchValidationAndPlan:
    def test_account_aggregation_keeps_auth_kinds_in_separate_pools(self):
        def account_spec(name: str, auth_kind: str) -> dict[str, object]:
            return {
                "name": name,
                "run": {"command": "true"},
                "fanout": {"workers": 2},
                "account": {
                    "agent_type": "codex",
                    "auth_kind": auth_kind,
                    "group": "standard",
                },
            }

        manifest = JobBatchManifest.model_validate(_manifest(
            account_spec("oauth", "oauth"),
            account_spec("api", "agent_api"),
            account_spec("compatible", "any"),
        ))
        pools = aggregate_manifest(manifest)["account_requirements"]["by_pool"]

        assert [(pool["auth_kind"], pool["required_slots"]) for pool in pools] == [
            ("agent_api", 2), ("any", 2), ("oauth", 2),
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mutate", "location"),
        [
            (lambda body: body.update({"unexpected": True}), "unexpected"),
            (lambda body: body["policy"].update({"unexpected": True}), "unexpected"),
            (lambda body: body["jobs"][0].update({"unexpected": True}), "unexpected"),
            (
                lambda body: body["jobs"][0]["spec"].update({"unexpected": True}),
                "unexpected",
            ),
            (lambda body: body.update({"schema_version": 2}), "schema_version"),
            (
                lambda body: body["jobs"].append(copy.deepcopy(body["jobs"][0])),
                "client_id",
            ),
        ],
    )
    async def test_manifest_is_strict_at_every_level(
        self,
        batch_client,
        mutate: Callable[[dict[str, Any]], None],
        location: str,
    ):
        client, _, batch = batch_client
        body = _manifest(_spec("strict"))
        mutate(body)

        response = await client.post("/api/job-batches/plan", json=body)

        assert response.status_code == 422
        assert location in response.text
        assert batch.started == []

    @pytest.mark.asyncio
    async def test_plan_preflights_every_item_without_side_effects(
        self,
        batch_client,
    ):
        client, manager, batch = batch_client
        body = _manifest(
            _spec("valid"),
            {
                **_spec("invalid"),
                "fanout": {"workers": 31},
            },
        )

        response = await client.post("/api/job-batches/plan", json=body)

        assert response.status_code == 200
        result = response.json()
        assert result["valid"] is False
        assert result["side_effects"] is False
        assert [item["client_id"] for item in result["items"]] == [
            "client-001",
            "client-002",
        ]
        assert [item["valid"] for item in result["items"]] == [True, False]
        assert result["items"][1]["errors"]
        assert batch.started == []
        assert manager.provider.create_calls == 0
        assert not (Path(manager.config.registry.path).with_name("specs")).exists()

    @pytest.mark.asyncio
    async def test_valid_plan_reports_effective_requested_instance_type(
        self, batch_client, monkeypatch,
    ):
        client, manager, batch = batch_client
        monkeypatch.setenv(
            "ELASTIC_AGENT_ALLOWED_INSTANCE_TYPES",
            "t3.medium,r5.2xlarge",
        )
        spec = _spec("requested-instance")
        spec["fanout"] = {"workers": 1, "instance_type": "r5.2xlarge"}

        response = await client.post(
            "/api/job-batches/plan",
            json=_manifest(spec),
        )

        assert response.status_code == 200
        result = response.json()
        assert result["valid"] is True
        assert result["side_effects"] is False
        assert result["summary"]["instance_types"] == ["r5.2xlarge"]
        assert batch.started == []
        assert manager.provider.create_calls == 0

    @pytest.mark.asyncio
    async def test_submit_is_all_or_nothing_when_any_preflight_fails(
        self,
        batch_client,
    ):
        client, manager, batch = batch_client
        body = _manifest(
            _spec("would-have-been-valid"),
            {
                **_spec("invalid"),
                "fanout": {"instance_type": "p5.48xlarge", "workers": 1},
            },
        )

        response = await client.post(
            "/api/job-batches",
            json=body,
            headers={"Idempotency-Key": "batch-all-or-nothing"},
        )

        assert response.status_code == 422
        assert batch.started == []
        assert manager.provider.create_calls == 0
        specs_dir = Path(manager.config.registry.path).with_name("specs")
        assert not specs_dir.exists() or list(specs_dir.glob("*.json")) == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("setting", "value", "specs", "needle"),
        [
            (
                "ELASTIC_AGENT_MAX_JOB_BATCH_ITEMS",
                "1",
                [_spec("one"), _spec("two")],
                "item",
            ),
            (
                "ELASTIC_AGENT_MAX_JOB_BATCH_TOTAL_WORKERS",
                "3",
                [_spec("one", workers=2), _spec("two", workers=2)],
                "worker",
            ),
            (
                "ELASTIC_AGENT_MAX_JOB_BATCH_WORKER_HOURS",
                "3",
                [_spec("one", workers=2, ttl_seconds=7_200)],
                "hour",
            ),
            (
                "ELASTIC_AGENT_JOB_BATCH_MAX_ACTIVE_JOBS",
                "1",
                [_spec("one")],
                "max_active_jobs",
            ),
        ],
    )
    async def test_aggregate_admission_limits_fail_before_submission(
        self,
        batch_client,
        monkeypatch: pytest.MonkeyPatch,
        setting: str,
        value: str,
        specs: list[dict[str, object]],
        needle: str,
    ):
        client, manager, batch = batch_client
        monkeypatch.setenv(setting, value)
        manager.job_batch_queue.limits = JobBatchLimits()

        planned = await client.post(
            "/api/job-batches/plan",
            json=_manifest(*specs),
        )
        submitted = await client.post(
            "/api/job-batches",
            json=_manifest(*specs),
            headers={"Idempotency-Key": f"aggregate-limit-{setting}"},
        )

        assert planned.status_code == 200
        assert planned.json()["valid"] is False
        assert needle.lower() in planned.text.lower()
        assert submitted.status_code == 422
        assert batch.started == []
        assert manager.provider.create_calls == 0

    @pytest.mark.asyncio
    async def test_redacted_secret_reference_sentinel_is_rejected_without_echo(
        self,
        batch_client,
    ):
        client, _, batch = batch_client
        marker = "[SECRET_REFERENCE]"
        spec = _spec("sentinel")
        spec["run"]["secret_env"] = {"TOKENROUTER_API_KEY": marker}

        response = await client.post(
            "/api/job-batches/plan",
            json=_manifest(spec),
        )

        assert response.status_code == 422
        assert marker not in response.text
        assert batch.started == []

    @pytest.mark.asyncio
    async def test_request_body_has_an_independent_exact_two_mib_limit(
        self,
        batch_client,
    ):
        client, _, batch = batch_client
        encoded = json.dumps(
            _manifest(_spec("body-boundary")),
            separators=(",", ":"),
        ).encode()
        at_limit = encoded + b" " * (TWO_MIB - len(encoded))
        over_limit = at_limit + b" "

        accepted = await client.post(
            "/api/job-batches/plan",
            content=at_limit,
            headers={"Content-Type": "application/json"},
        )
        rejected = await client.post(
            "/api/job-batches/plan",
            content=over_limit,
            headers={"Content-Type": "application/json"},
        )

        assert accepted.status_code == 200
        assert accepted.json()["valid"] is True
        assert rejected.status_code == 413
        assert batch.started == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "invalid_json",
        [
            b'{"schema_version":1,"schema_version":1,"batch_id":"duplicate","jobs":[]}',
            b'{"schema_version":1,"batch_id":"nan","jobs":[],"value":NaN}',
        ],
    )
    async def test_noncanonical_json_is_rejected_before_validation(
        self,
        batch_client,
        invalid_json: bytes,
    ):
        client, manager, batch = batch_client

        response = await client.post(
            "/api/job-batches/plan",
            content=invalid_json,
            headers={"Content-Type": "application/json"},
        )

        assert response.status_code == 422
        assert response.json()["detail"] == "invalid Job batch JSON request body"
        assert batch.started == []
        assert manager.provider.create_calls == 0

    @pytest.mark.asyncio
    async def test_plan_summary_is_aggregate_and_secret_safe(self, batch_client):
        client, _, batch = batch_client
        private_marker = "plaintext-private-marker-must-not-echo"
        response = await client.post(
            "/api/job-batches/plan",
            json=_manifest(
                _spec("one", workers=2, ttl_seconds=7_200),
                _spec(
                    "two",
                    workers=3,
                    ttl_seconds=3_600,
                    secret_marker=private_marker,
                ),
                max_active_jobs=1,
            ),
        )

        assert response.status_code == 200
        body = response.json()
        assert body["valid"] is True
        assert body["summary"]["job_count"] == 2
        assert body["summary"]["total_workers"] == 5
        assert body["summary"]["total_worker_hours"] == 7
        assert body["summary"]["max_active_jobs"] == 1
        assert private_marker not in response.text
        assert "aws-secretsmanager://prod/job-batch-token#value" not in response.text
        assert batch.started == []


class TestJobBatchSubmissionAndQueue:
    @pytest.mark.asyncio
    async def test_default_any_replays_legacy_manifest_without_auth_kind(
        self, batch_client,
    ):
        client, manager, _batch = batch_client
        manifest = _manifest(_spec("legacy-any"))
        headers = {"Idempotency-Key": "legacy-any-batch"}
        submitted = await client.post(
            "/api/job-batches", json=manifest, headers=headers,
        )
        assert submitted.status_code == 201
        job_batch_id = submitted.json()["job_batch_id"]
        await manager.job_batch_queue.stop()

        legacy = copy.deepcopy(manager.job_batch_queue._journals[job_batch_id])
        for item in legacy["manifest"]["jobs"]:
            item["spec"]["account"].pop("auth_kind")
        canonical = json.dumps(
            legacy["manifest"], ensure_ascii=True, allow_nan=False,
            sort_keys=True, separators=(",", ":"),
        ).encode("ascii")
        legacy["manifest_fingerprint"] = hashlib.sha256(canonical).hexdigest()
        manager.job_batch_queue._validate_journal(
            legacy, expected_id=job_batch_id,
        )
        manager.job_batch_queue._journals[job_batch_id] = legacy

        replay = await client.post(
            "/api/job-batches", json=manifest, headers=headers,
        )

        assert replay.status_code == 201
        assert replay.json()["idempotent_replay"] is True

    @pytest.mark.asyncio
    async def test_submit_requires_an_idempotency_key(self, batch_client):
        client, manager, batch = batch_client

        response = await client.post(
            "/api/job-batches",
            json=_manifest(_spec("missing-key")),
        )

        assert response.status_code == 400
        assert response.json()["detail"] == "Idempotency-Key is required"
        assert batch.started == []
        assert manager.provider.create_calls == 0

    @pytest.mark.asyncio
    async def test_idempotent_replay_is_stable_and_conflict_is_409(
        self,
        batch_client,
    ):
        client, _, batch = batch_client
        manifest = _manifest(_spec("one"), _spec("two"))
        headers = {"Idempotency-Key": "stable-batch-request"}

        first = await client.post("/api/job-batches", json=manifest, headers=headers)
        replay = await client.post("/api/job-batches", json=manifest, headers=headers)
        changed = copy.deepcopy(manifest)
        changed["jobs"][1]["spec"]["name"] = "changed"
        conflict = await client.post(
            "/api/job-batches",
            json=changed,
            headers=headers,
        )

        assert first.status_code == 201
        assert replay.status_code == 201
        assert replay.json()["idempotent_replay"] is True
        assert replay.json()["job_batch_id"] == first.json()["job_batch_id"]
        assert [item["client_id"] for item in replay.json()["items"]] == [
            "client-001",
            "client-002",
        ]
        assert [item.get("job_id") for item in replay.json()["items"]] == [
            item.get("job_id") for item in first.json()["items"]
        ]
        assert conflict.status_code == 409
        assert "changed" not in conflict.text
        assert len(set(batch.started)) == len(batch.started)

    @pytest.mark.asyncio
    async def test_detail_and_list_never_echo_job_secrets(self, batch_client):
        client, _, batch = batch_client
        marker = "batch-secret-which-must-never-echo"
        submitted = await client.post(
            "/api/job-batches",
            json=_manifest(_spec("private", secret_marker=marker)),
            headers={"Idempotency-Key": "secret-safe-batch"},
        )
        assert submitted.status_code == 201
        batch_id = submitted.json()["job_batch_id"]
        await _wait_for(lambda: bool(batch.started))

        detail = await client.get(f"/api/job-batches/{batch_id}")
        listing = await client.get("/api/job-batches")

        assert detail.status_code == 200
        assert listing.status_code == 200
        for response in (submitted, detail, listing):
            assert marker not in response.text
            assert "aws-secretsmanager://prod/job-batch-token#value" not in response.text
        listed_ids = {item["job_batch_id"] for item in listing.json()["batches"]}
        assert batch_id in listed_ids
        assert [item["client_id"] for item in detail.json()["items"]] == ["client-001"]

    @pytest.mark.asyncio
    async def test_queue_never_exceeds_manifest_max_active_jobs(
        self,
        batch_client,
    ):
        client, _, batch = batch_client
        response = await client.post(
            "/api/job-batches",
            json=_manifest(
                _spec("one"),
                _spec("two"),
                _spec("three"),
                max_active_jobs=2,
            ),
            headers={"Idempotency-Key": "bounded-concurrency"},
        )
        assert response.status_code == 201
        batch_id = response.json()["job_batch_id"]

        await _wait_for(lambda: len(batch.started) == 2)
        await asyncio.sleep(0.05)
        assert batch.started_names == ["one", "two"]
        queued = (await client.get(f"/api/job-batches/{batch_id}")).json()
        assert [item["state"] for item in queued["items"]].count("queued") == 1

        batch.finish(batch.started[0])
        await _wait_for(lambda: len(batch.started) == 3)
        assert batch.started_names == ["one", "two", "three"]

    @pytest.mark.asyncio
    async def test_cross_batch_queue_reserves_capacity_by_worker_fanout(self, batch_client):
        client, manager, batch = batch_client
        manager.config.provider.aws.max_instances = 3

        first = await client.post(
            "/api/job-batches",
            json=_manifest(
                _spec("weighted-active", workers=2),
                batch_id="weighted-first",
            ),
            headers={"Idempotency-Key": "weighted-first"},
        )
        assert first.status_code == 201
        await _wait_for(lambda: batch.started_names == ["weighted-active"])

        blocked = await client.post(
            "/api/job-batches",
            json=_manifest(
                _spec("weighted-blocked", workers=2),
                batch_id="weighted-second",
            ),
            headers={"Idempotency-Key": "weighted-second"},
        )
        assert blocked.status_code == 201
        small = await client.post(
            "/api/job-batches",
            json=_manifest(
                _spec("weighted-small", workers=1),
                batch_id="weighted-third",
            ),
            headers={"Idempotency-Key": "weighted-third"},
        )
        assert small.status_code == 201

        await asyncio.sleep(0.15)
        assert batch.started_names == ["weighted-active"]
        blocked_detail = (await client.get(f"/api/job-batches/{blocked.json()['job_batch_id']}")).json()
        assert blocked_detail["items"][0]["state"] == "queued"
        small_detail = (await client.get(f"/api/job-batches/{small.json()['job_batch_id']}")).json()
        assert small_detail["items"][0]["state"] == "queued"

        batch.finish(batch.started[0])
        await _wait_for(lambda: len(batch.started) == 3)
        assert batch.started_names == [
            "weighted-active",
            "weighted-blocked",
            "weighted-small",
        ]

    @pytest.mark.asyncio
    async def test_manual_live_job_reserves_worker_capacity_from_batch_queue(self, batch_client):
        client, manager, batch = batch_client
        manager.config.provider.aws.max_instances = 3
        manual = batch.prepare(JobSpec.model_validate(_spec("manual-live", workers=2)))
        manual.job_id = "job-" + "a" * 32
        await batch.submit_prepared(manual)

        blocked = await client.post(
            "/api/job-batches",
            json=_manifest(
                _spec("after-manual-large", workers=2),
                batch_id="after-manual-large",
            ),
            headers={"Idempotency-Key": "after-manual-large"},
        )
        assert blocked.status_code == 201
        later_small = await client.post(
            "/api/job-batches",
            json=_manifest(
                _spec("after-manual-small", workers=1),
                batch_id="after-manual-small",
            ),
            headers={"Idempotency-Key": "after-manual-small"},
        )
        assert later_small.status_code == 201

        await asyncio.sleep(0.15)
        assert batch.started_names == ["manual-live"]
        for response in (blocked, later_small):
            detail = (await client.get(f"/api/job-batches/{response.json()['job_batch_id']}")).json()
            assert detail["items"][0]["state"] == "queued"

        batch.finish(manual.job_id)
        await _wait_for(lambda: len(batch.started) == 3)
        assert batch.started_names == [
            "manual-live",
            "after-manual-large",
            "after-manual-small",
        ]

    @pytest.mark.asyncio
    async def test_submit_failure_is_visible_and_continue_policy_advances(
        self,
        batch_client,
    ):
        client, _, batch = batch_client
        batch.fail_names.add("broken")
        response = await client.post(
            "/api/job-batches",
            json=_manifest(
                _spec("broken"),
                _spec("healthy"),
                max_active_jobs=1,
            ),
            headers={"Idempotency-Key": "partial-failure"},
        )
        assert response.status_code == 201
        batch_id = response.json()["job_batch_id"]

        await _wait_for(lambda: batch.started_names == ["healthy"])
        detail = (await client.get(f"/api/job-batches/{batch_id}")).json()
        by_client = {item["client_id"]: item for item in detail["items"]}
        assert by_client["client-001"]["state"] == "error"
        assert by_client["client-001"]["error"]
        assert by_client["client-002"]["state"] == "accepted"

        batch.finish(batch.started[0])
        await _wait_for(
            lambda: (
                # Refresh is API-driven; the predicate only waits for the
                # queue's polling interval before the final assertion below.
                batch.get_job(batch.started[0]).summary()["done"] is True
            )
        )
        await asyncio.sleep(0.15)
        terminal = (await client.get(f"/api/job-batches/{batch_id}")).json()
        assert terminal["items"][0]["state"] == "error"
        assert terminal["items"][1]["state"] == "terminal"
        assert terminal["state"] == "terminal"


class TestJobBatchDurability:
    @pytest.mark.asyncio
    async def test_queued_item_resumes_after_restart_when_capacity_is_released(self, tmp_path: Path):
        from elastic_agent.core.job_spec_store import update_job_state

        manifest = _manifest(
            _spec("restart-active"),
            _spec("restart-queued"),
            max_active_jobs=1,
        )

        first_manager, first_provider = _new_manager(tmp_path)
        first_batch = RecordingBatch(first_manager._persist_batch_job_spec)
        first_manager._batch = first_batch
        first_manager.job_batch_queue._poll_interval_seconds = 0.05
        first_manager.job_batch_queue._submitter = first_batch.submit_payload
        await first_manager.start()
        first_app = create_app(first_manager)
        async with AsyncClient(
            transport=ASGITransport(app=first_app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {API_KEY}"},
        ) as first_client:
            submitted = await first_client.post(
                "/api/job-batches",
                json=manifest,
                headers={"Idempotency-Key": "restart-queued-batch"},
            )
            assert submitted.status_code == 201
            job_batch_id = submitted.json()["job_batch_id"]
            await _wait_for(lambda: first_batch.started_names == ["restart-active"])
            before = (await first_client.get(f"/api/job-batches/{job_batch_id}")).json()
            assert [item["state"] for item in before["items"]] == [
                "accepted",
                "queued",
            ]
            active_job_id = before["items"][0]["job_id"]

        await _stop_job_batch_queue(first_manager)
        await first_manager.stop()
        assert first_provider.create_calls == 0
        update_job_state(
            first_manager.config.registry.path,
            active_job_id,
            "succeeded",
            summary={"state": "succeeded", "done": True, "error": None},
        )

        second_manager, second_provider = _new_manager(tmp_path)
        second_batch = RecordingBatch(second_manager._persist_batch_job_spec)
        second_manager._batch = second_batch
        second_manager.job_batch_queue._poll_interval_seconds = 0.05
        second_manager.job_batch_queue._submitter = second_batch.submit_payload
        await second_manager.start()
        second_app = create_app(second_manager)
        async with AsyncClient(
            transport=ASGITransport(app=second_app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {API_KEY}"},
        ) as second_client:
            await _wait_for(lambda: second_batch.started_names == ["restart-queued"])
            recovered = (await second_client.get(f"/api/job-batches/{job_batch_id}")).json()

        assert [item["state"] for item in recovered["items"]] == [
            "terminal",
            "accepted",
        ]
        assert recovered["items"][0]["job_state"] == "succeeded"
        assert recovered["items"][1]["job_id"]
        assert second_provider.create_calls == 0
        await _stop_job_batch_queue(second_manager)
        await second_manager.stop()

    @pytest.mark.asyncio
    async def test_detail_and_client_mapping_survive_manager_restart(
        self,
        tmp_path: Path,
    ):
        manifest = _manifest(_spec("durable-one"), _spec("durable-two"))
        key = "durable-batch-request"

        first_manager, first_provider = _new_manager(tmp_path)
        first_batch = RecordingBatch(first_manager._persist_batch_job_spec)
        first_manager._batch = first_batch
        first_manager.job_batch_queue._poll_interval_seconds = 0.05
        await first_manager.start()
        first_manager.job_batch_queue._submitter = first_batch.submit_payload
        first_app = create_app(first_manager)
        async with AsyncClient(
            transport=ASGITransport(app=first_app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {API_KEY}"},
        ) as first_client:
            submitted = await first_client.post(
                "/api/job-batches",
                json=manifest,
                headers={"Idempotency-Key": key},
            )
            assert submitted.status_code == 201
            submitted_body = submitted.json()
            job_batch_id = submitted_body["job_batch_id"]
            await _wait_for(lambda: len(first_batch.started) == 2)
            first_batch.finish(first_batch.started[0])
            first_batch.finish(first_batch.started[1])
            await asyncio.sleep(0.05)
            before = (await first_client.get(f"/api/job-batches/{job_batch_id}")).json()
        await _stop_job_batch_queue(first_manager)
        await first_manager.stop()
        assert first_provider.create_calls == 0

        second_manager, second_provider = _new_manager(tmp_path)
        second_batch = RecordingBatch(second_manager._persist_batch_job_spec)
        second_manager._batch = second_batch
        second_manager.job_batch_queue._poll_interval_seconds = 0.05
        await second_manager.start()
        second_manager.job_batch_queue._submitter = second_batch.submit_payload
        second_app = create_app(second_manager)
        async with AsyncClient(
            transport=ASGITransport(app=second_app),
            base_url="http://testserver",
            headers={"Authorization": f"Bearer {API_KEY}"},
        ) as second_client:
            detail = await second_client.get(f"/api/job-batches/{job_batch_id}")
            listing = await second_client.get("/api/job-batches")
            replay = await second_client.post(
                "/api/job-batches",
                json=manifest,
                headers={"Idempotency-Key": key},
            )

        assert detail.status_code == 200
        assert listing.status_code == 200
        assert replay.status_code == 201
        assert replay.json()["idempotent_replay"] is True
        assert replay.json()["job_batch_id"] == job_batch_id
        assert [item["client_id"] for item in detail.json()["items"]] == [
            "client-001",
            "client-002",
        ]
        assert [item.get("job_id") for item in detail.json()["items"]] == [
            item.get("job_id") for item in before["items"]
        ]
        assert job_batch_id in {item["job_batch_id"] for item in listing.json()["batches"]}
        assert second_batch.started == []
        assert second_provider.create_calls == 0
        await _stop_job_batch_queue(second_manager)
        await second_manager.stop()
