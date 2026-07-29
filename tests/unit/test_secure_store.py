"""Security invariants shared by Manager-local durable state stores."""

from __future__ import annotations

import json
import stat

import pytest

from elastic_agent.core.job_spec import JobSpec, RunSpec
from elastic_agent.core.job_spec_store import (
    job_specs_dir,
    persist_job_spec,
    update_job_checkpoint,
    update_job_interrupt_intent,
    update_job_state,
)


def test_job_spec_journal_is_private_and_durable(tmp_path):
    state = tmp_path / "state"
    state.mkdir(mode=0o755)
    registry = state / "registry.json"

    destination = persist_job_spec(
        registry,
        "job-secure-1",
        JobSpec(name="secure", run=RunSpec(command="true")),
    )

    assert stat.S_IMODE(destination.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600
    assert json.loads(destination.read_text())["submission_state"] == "prepared"
    assert not list(destination.parent.glob("*.tmp"))

    update_job_checkpoint(
        registry,
        "job-secure-1",
        "periodic-00000001",
        committed_at="2026-07-29T00:00:02+00:00",
    )
    update_job_checkpoint(
        registry,
        "job-secure-1",
        "periodic-00000000",
        committed_at="2026-07-29T00:00:01+00:00",
    )
    checkpointed = json.loads(destination.read_text())
    assert checkpointed["submission_state"] == "prepared"
    assert (
        checkpointed["latest_checkpoint_generation"]
        == "periodic-00000001"
    )
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600

    update_job_state(
        registry,
        "job-secure-1",
        "succeeded",
        summary={"done": True},
    )
    updated = json.loads(destination.read_text())
    assert updated["submission_state"] == "succeeded"
    assert updated["terminal_summary"] == {"done": True}
    assert stat.S_IMODE(destination.stat().st_mode) == 0o600


def test_job_specs_dir_repairs_legacy_spec_permissions(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    specs = state / "specs"
    specs.mkdir(mode=0o755)
    legacy = specs / "legacy.json"
    legacy.write_text("{}", encoding="utf-8")
    legacy.chmod(0o644)

    resolved = job_specs_dir(state / "registry.json")

    assert resolved == specs
    assert stat.S_IMODE(specs.stat().st_mode) == 0o700
    assert stat.S_IMODE(legacy.stat().st_mode) == 0o600


def test_suspended_state_requires_exact_checkpoint_and_zero_cleanup(tmp_path):
    registry = tmp_path / "registry.json"
    destination = persist_job_spec(
        registry,
        "job-suspend-proof",
        JobSpec(name="suspend", run=RunSpec(command="true")),
    )
    update_job_checkpoint(
        registry,
        "job-suspend-proof",
        "periodic-00000003",
        committed_at="2026-07-29T12:00:00+00:00",
    )
    digest = "a" * 64
    update_job_interrupt_intent(
        registry,
        "job-suspend-proof",
        digest,
        summary={
            "state": "suspending",
            "interrupt_requested": True,
            "resume_available": False,
        },
    )
    intent_payload = json.loads(destination.read_text())
    assert intent_payload["interrupt_intent"] == {
        "schema": 1,
        "idempotency_digest": digest,
        "requested_at": None,
    }
    assert digest not in json.dumps(
        intent_payload["terminal_summary"],
    )
    with pytest.raises(ValueError, match="identity conflicts"):
        update_job_interrupt_intent(
            registry,
            "job-suspend-proof",
            "b" * 64,
            summary={
                "state": "suspending",
                "interrupt_requested": True,
                "resume_available": False,
            },
        )

    invalid = {
        "state": "suspended",
        "done": True,
        "cleanup_pending": 0,
        "resume_available": True,
        "resume_generation": "periodic-00000002",
        "resume_committed_at": "2026-07-29T12:00:00+00:00",
    }
    with pytest.raises(ValueError, match="exact committed checkpoint"):
        update_job_state(
            registry,
            "job-suspend-proof",
            "suspended",
            summary=invalid,
        )
    invalid["resume_generation"] = "periodic-00000003"
    invalid["cleanup_pending"] = 1
    with pytest.raises(ValueError, match="zero pending cleanup"):
        update_job_state(
            registry,
            "job-suspend-proof",
            "suspended",
            summary=invalid,
        )

    invalid["cleanup_pending"] = 0
    update_job_state(
        registry,
        "job-suspend-proof",
        "suspended",
        summary=invalid,
    )
    payload = json.loads(destination.read_text())
    assert payload["submission_state"] == "suspended"
    assert payload["terminal_summary"]["resume_available"] is True


def test_generic_state_cannot_forge_interrupt_without_private_intent(tmp_path):
    registry = tmp_path / "registry.json"
    persist_job_spec(
        registry,
        "job-no-private-intent",
        JobSpec(name="running", run=RunSpec(command="true")),
    )
    summary = {
        "state": "suspending",
        "interrupt_requested": True,
        "resume_available": False,
    }

    with pytest.raises(ValueError, match="private interrupt intent"):
        update_job_state(
            registry,
            "job-no-private-intent",
            "suspending",
            summary=summary,
        )


def test_interrupt_intent_read_rejects_symlink_and_oversize_journal(tmp_path):
    registry = tmp_path / "state" / "registry.json"
    specs = job_specs_dir(registry)
    outside = tmp_path / "outside.json"
    outside.write_text(
        json.dumps({
            "job_id": "job-symlink-intent",
            "spec": {},
            "submission_state": "running",
        }),
        encoding="utf-8",
    )
    (specs / "job-symlink-intent.json").symlink_to(outside)
    with pytest.raises(RuntimeError, match="must not be a symlink"):
        update_job_interrupt_intent(
            registry,
            "job-symlink-intent",
            "a" * 64,
            summary={
                "state": "suspending",
                "interrupt_requested": True,
                "resume_available": False,
            },
        )
    assert json.loads(outside.read_text())["submission_state"] == "running"

    (specs / "job-symlink-intent.json").unlink()
    oversized = persist_job_spec(
        registry,
        "job-oversize-intent",
        JobSpec(name="oversize", run=RunSpec(command="true")),
    )
    with oversized.open("r+b") as stream:
        stream.truncate(32 * 1024 * 1024 + 1)
    with pytest.raises(ValueError, match="invalid Job journal file"):
        update_job_interrupt_intent(
            registry,
            "job-oversize-intent",
            "b" * 64,
            summary={
                "state": "suspending",
                "interrupt_requested": True,
                "resume_available": False,
            },
        )


def test_job_lineage_is_derived_from_persisted_source(tmp_path):
    registry = tmp_path / "registry.json"
    persist_job_spec(
        registry,
        "job-root",
        JobSpec(name="root", run=RunSpec(command="true")),
    )
    first_resume = JobSpec.model_validate({
        "name": "resume-1",
        "run": {"command": "true"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-root",
            "paths": ["results"],
            "generation": "periodic-00000001",
        },
    })
    first_path = persist_job_spec(
        registry,
        "job-resume-1",
        first_resume,
    )
    first_lineage = json.loads(first_path.read_text())["lineage"]
    assert first_lineage == {
        "resumed_from_job_id": "job-root",
        "root_job_id": "job-root",
        "attempt_no": 2,
    }

    second_resume = first_resume.model_copy(
        update={
            "recovery": first_resume.recovery.model_copy(
                update={"source_job_id": "job-resume-1"},
            ),
        },
    )
    second_path = persist_job_spec(
        registry,
        "job-resume-2",
        second_resume,
    )
    second_lineage = json.loads(second_path.read_text())["lineage"]
    assert second_lineage == {
        "resumed_from_job_id": "job-resume-1",
        "root_job_id": "job-root",
        "attempt_no": 3,
    }


def test_recovery_lineage_rejects_missing_source_journal(tmp_path):
    registry = tmp_path / "registry.json"
    recovery = JobSpec.model_validate({
        "name": "missing-source",
        "run": {"command": "true"},
        "fanout": {"workers": 1, "shard_by": "shard_index"},
        "collect": {"paths": ["results"], "checkpoint": True},
        "recovery": {
            "policy": "checkpoint",
            "source_job_id": "job-does-not-exist",
            "paths": ["results"],
            "generation": "periodic-00000001",
        },
    })

    with pytest.raises(FileNotFoundError):
        persist_job_spec(
            registry,
            "job-untrusted-resume",
            recovery,
        )
    assert not (
        job_specs_dir(registry) / "job-untrusted-resume.json"
    ).exists()
