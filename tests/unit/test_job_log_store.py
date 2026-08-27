from __future__ import annotations

import json
import os
import stat
from pathlib import Path

import pytest

from elastic_agent.core.job_log_store import JobLogStore
from elastic_agent.core.job_spec import JobSpec, RunSpec, WorkerContext
from elastic_agent.core.trajectory_prompt import build_trajectory_prompt_metadata


def _entry(task_id: str, index: int, *, worker_id: str = "aws:i-123") -> dict:
    return {
        "task_id": task_id,
        "worker_id": worker_id,
        "stream": "stderr" if index % 2 else "stdout",
        "data": f"line {index}",
        "timestamp": f"2026-07-25T12:00:{index:02d}+00:00",
        "parsed": {"type": "assistant", "ignored": "not persisted"},
    }


def test_job_log_store_snapshots_bounded_private_run_output(tmp_path: Path) -> None:
    store = JobLogStore(tmp_path / "job-logs", max_entries=3, max_bytes=64_000)
    task_id = "job-safe:aws:i-123:abcdef"

    saved = store.save_snapshot(
        job_id="job-safe",
        task_id=task_id,
        worker_id="aws:i-123",
        entries=[_entry(task_id, index) for index in range(5)],
        exit_info={"exit_code": 1, "error_message": "command failed"},
    )

    assert saved is not None
    assert stat.S_IMODE((tmp_path / "job-logs").stat().st_mode) == 0o700
    assert stat.S_IMODE(saved.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(saved.stat().st_mode) == 0o600
    payload = json.loads(saved.read_text(encoding="utf-8"))
    assert payload["job_id"] == "job-safe"
    assert payload["task_id"] == task_id
    assert payload["truncated"] is True
    assert [entry["data"] for entry in payload["entries"]] == [
        "line 2",
        "line 3",
        "line 4",
    ]
    assert all("parsed" not in entry for entry in payload["entries"])

    loaded = store.read_job("job-safe")
    assert loaded[0]["exit"]["exit_code"] == 1
    assert loaded[0]["entries"] == payload["entries"]

    replayed = store.save_snapshot(
        job_id="job-safe",
        task_id=task_id,
        worker_id="aws:i-123",
        entries=[],
        exit_info={"exit_code": 1, "event_id": "replayed"},
    )
    assert replayed == saved
    assert [entry["data"] for entry in store.read_job("job-safe")[0]["entries"]] == [
        "line 2",
        "line 3",
        "line 4",
    ]


def test_job_log_store_rejects_non_batch_or_unsafe_task_ids(tmp_path: Path) -> None:
    store = JobLogStore(tmp_path / "job-logs")

    with pytest.raises(ValueError, match="does not belong"):
        store.save_snapshot(
            job_id="job-safe",
            task_id="plain-task",
            worker_id="worker",
            entries=[],
            exit_info={},
        )
    with pytest.raises(ValueError, match="invalid job_id"):
        store.save_snapshot(
            job_id="../escape",
            task_id="../escape:worker:abc",
            worker_id="worker",
            entries=[],
            exit_info={},
        )


def test_job_log_store_ignores_corrupt_and_mismatched_snapshots(
    tmp_path: Path,
) -> None:
    store = JobLogStore(tmp_path / "job-logs")
    task_id = "job-safe:worker:abcdef"
    saved = store.save_snapshot(
        job_id="job-safe",
        task_id=task_id,
        worker_id="worker",
        entries=[_entry(task_id, 1, worker_id="worker")],
        exit_info={"exit_code": 0},
    )
    assert saved is not None

    (saved.parent / "corrupt.json").write_text("{", encoding="utf-8")
    (saved.parent / "array.json").write_text("[]", encoding="utf-8")
    mismatched = json.loads(saved.read_text(encoding="utf-8"))
    mismatched["job_id"] = "job-other"
    (saved.parent / "mismatch.json").write_text(
        json.dumps(mismatched),
        encoding="utf-8",
    )

    snapshots = store.read_job("job-safe")
    assert [item["task_id"] for item in snapshots] == [task_id]


def test_job_log_store_enforces_job_task_and_byte_quotas(tmp_path: Path) -> None:
    store = JobLogStore(
        tmp_path / "job-logs",
        max_entries=20,
        max_bytes=4_096,
        max_job_bytes=8_192,
        max_total_bytes=16_384,
        max_tasks_per_job=2,
        retention_days=0,
    )
    saved = []
    for index in range(3):
        task_id = f"job-bounded:worker:{index}"
        path = store.save_snapshot(
            job_id="job-bounded",
            task_id=task_id,
            worker_id="worker",
            entries=[
                {
                    **_entry(task_id, index, worker_id="worker"),
                    "data": "x" * 2_000,
                }
            ],
            exit_info={"exit_code": 0},
        )
        # Make the retention order deterministic on filesystems with coarse
        # timestamp resolution.
        current = path.stat().st_mtime_ns
        os.utime(path, ns=(current + index, current + index))
        saved.append(path)

    store.prune()

    snapshots = store.read_job("job-bounded")
    assert [item["task_id"] for item in snapshots] == [
        "job-bounded:worker:1",
        "job-bounded:worker:2",
    ]
    assert sum(path.stat().st_size for path in saved if path.exists()) <= 8_192
    tail = store.read_job_tail("job-bounded", lines=20)
    assert tail["history_truncated"] is True
    marker = tmp_path / "job-logs" / "job-bounded" / ".pruned"
    assert stat.S_IMODE(marker.stat().st_mode) == 0o600


def test_job_log_tail_is_bounded_and_preserves_source_truncation(
    tmp_path: Path,
) -> None:
    store = JobLogStore(tmp_path / "job-logs", max_entries=10)
    for task_index in range(3):
        task_id = f"job-tail:worker-{task_index}:abcdef"
        store.save_snapshot(
            job_id="job-tail",
            task_id=task_id,
            worker_id=f"worker-{task_index}",
            entries=[
                {
                    **_entry(task_id, line, worker_id=f"worker-{task_index}"),
                    "timestamp": f"2026-07-25T12:{task_index:02d}:{line:02d}+00:00",
                }
                for line in range(4)
            ],
            exit_info={"exit_code": 0},
            source_truncated=task_index == 0,
        )

    tail = store.read_job_tail("job-tail", lines=3)

    assert tail["total"] == 12
    assert len(tail["entries"]) == 3
    assert [entry["data"] for entry in tail["entries"]] == [
        "line 1",
        "line 2",
        "line 3",
    ]
    assert tail["truncated"] is True
    assert tail["history_truncated"] is True
    assert len(tail["tasks"]) == 3


def test_job_log_store_rejects_symlink_job_directory(tmp_path: Path) -> None:
    root = tmp_path / "job-logs"
    root.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "job-safe").symlink_to(outside, target_is_directory=True)
    store = JobLogStore(root)

    with pytest.raises(RuntimeError, match="real directory"):
        store.save_snapshot(
            job_id="job-safe",
            task_id="job-safe:worker:abcdef",
            worker_id="worker",
            entries=[],
            exit_info={"exit_code": 0},
        )
    assert list(outside.iterdir()) == []


def test_prompt_metadata_survives_tail_truncation_and_v1_remains_readable(
    tmp_path: Path,
) -> None:
    store = JobLogStore(
        tmp_path / "job-logs",
        max_entries=2,
        max_bytes=64_000,
    )
    task_id = "job-prompt:worker:abcdef"
    spec = JobSpec(
        name="prompt",
        account={"agent_type": "claude", "mode": "none"},
        run=RunSpec(
            command="claude -p task",
            trajectory_prompt={
                "system": "important system prompt",
                "sources": [{"name": "CLAUDE.md", "content": "project rules"}],
            },
        ),
    )
    prompt = build_trajectory_prompt_metadata(
        spec,
        WorkerContext(),
        command=["bash", "-lc", "claude -p task"],
        resumed=False,
    )
    staged = store.save_prompt_metadata(
        job_id="job-prompt",
        task_id=task_id,
        worker_id="worker",
        prompt_metadata=prompt,
    )
    assert json.loads(staged.read_text())["complete"] is False

    store.save_snapshot(
        job_id="job-prompt",
        task_id=task_id,
        worker_id="worker",
        entries=[_entry(task_id, index, worker_id="worker") for index in range(10)],
        exit_info={"exit_code": 0},
        source_truncated=True,
    )

    full = store.read_job_tail("job-prompt", lines=10, task_id=task_id)
    assert len(full["entries"]) == 2
    assert full["tasks"][0]["truncated"] is True
    assert full["tasks"][0]["prompt"]["components"]["system"]["text"] == (
        "important system prompt"
    )
    summary = store.read_job_tail("job-prompt", lines=10)
    assert "text" not in summary["tasks"][0]["prompt"]["components"]["system"]
    assert summary["tasks"][0]["prompt"]["components"]["system"]["sha256"]

    empty_task = "job-prompt:worker:empty"
    store.save_prompt_metadata(
        job_id="job-prompt",
        task_id=empty_task,
        worker_id="worker",
        prompt_metadata=prompt,
    )
    store.save_snapshot(
        job_id="job-prompt",
        task_id=empty_task,
        worker_id="worker",
        entries=[],
        exit_info={"exit_code": 0},
    )
    empty = store.read_job_tail(
        "job-prompt",
        lines=10,
        task_id=empty_task,
    )["tasks"][0]
    assert empty["complete"] is True
    assert empty["prompt"]["components"]["system"]["text"] == (
        "important system prompt"
    )

    legacy_payload = json.loads(staged.read_text())
    legacy_payload["version"] = 1
    legacy_payload.pop("prompt")
    staged.write_text(json.dumps(legacy_payload), encoding="utf-8")
    assert 1 in {snapshot["version"] for snapshot in store.read_job("job-prompt")}
