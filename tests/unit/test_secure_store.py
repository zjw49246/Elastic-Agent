"""Security invariants shared by Manager-local durable state stores."""

from __future__ import annotations

import json
import stat

from elastic_agent.core.job_spec import JobSpec, RunSpec
from elastic_agent.core.job_spec_store import (
    job_specs_dir,
    persist_job_spec,
    update_job_checkpoint,
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
