from __future__ import annotations

import pytest

from elastic_agent.core.job_spec import JobSpec, RunSpec, WorkerContext
from elastic_agent.core.trajectory_prompt import (
    build_trajectory_prompt_metadata,
    normalize_trajectory_prompt_metadata,
)


@pytest.mark.parametrize("agent_type", ["claude", "codex"])
def test_opaque_agent_prompt_capture_is_honest(agent_type: str) -> None:
    spec = JobSpec(
        name="opaque",
        account={"agent_type": agent_type, "mode": "none"},
        run=RunSpec(command="agent task"),
    )

    metadata = build_trajectory_prompt_metadata(
        spec,
        WorkerContext(),
        command=["agent", "task"],
        resumed=False,
    )

    assert metadata["agent_type"] == agent_type
    assert metadata["capture_mode"] == "opaque_command"
    assert metadata["complete"] is False
    assert metadata["components"] == {}
    assert "undeclared_system_prompt" in metadata["unavailable_components"]
    assert normalize_trajectory_prompt_metadata(metadata) == metadata


def test_prompt_integrity_tampering_is_rejected() -> None:
    spec = JobSpec(
        name="declared",
        run=RunSpec(
            command="agent task",
            trajectory_prompt={"system": "original"},
        ),
    )
    metadata = build_trajectory_prompt_metadata(
        spec,
        WorkerContext(),
        command=["agent", "task"],
        resumed=False,
    )
    metadata["components"]["system"]["text"] = "tampered"

    with pytest.raises(ValueError, match="integrity"):
        normalize_trajectory_prompt_metadata(metadata)
