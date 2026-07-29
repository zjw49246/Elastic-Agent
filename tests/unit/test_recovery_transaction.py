from __future__ import annotations

import json
import os
import pwd
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from elastic_agent.worker import recovery_transaction as transaction


@pytest.fixture(autouse=True)
def _private_transaction_root(tmp_path, monkeypatch):
    root = tmp_path / "worker-private" / "recovery-transactions-v1"
    monkeypatch.setattr(transaction, "_CONTROL_ROOT", root)


def _payload(target: Path) -> dict:
    return {
        "schema_version": 1,
        "job_id": "job-recovery",
        "shard_index": 3,
        "target_dir": str(target),
        "generation": "periodic-00000007",
        "source_job_id": "job-source",
        "worker_id": "aws:i-0123456789",
        "run_user": pwd.getpwuid(os.getuid()).pw_name,
        "recovery_contract_sha256": "a" * 64,
        "total_bytes": 20,
        "total_objects": 4,
        "disk_reserve_bytes": 1024,
        "paths": ["results", "state/cache"],
    }


def _write(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def test_transaction_rolls_forward_after_crash_between_paths(
    tmp_path, monkeypatch,
):
    target = tmp_path / "work"
    _write(target / "results" / "value.txt", "old-results")
    _write(target / "state/cache" / "value.txt", "old-cache")
    payload = _payload(target)

    transaction.run_transaction("prepare", payload)
    _write(
        transaction.staged_path(payload, "results") / "value.txt",
        "new-results",
    )
    _write(
        transaction.staged_path(payload, "state/cache") / "value.txt",
        "new-cache",
    )

    original_write_state = transaction._write_state
    installing_writes = 0

    def crash_after_first_path(root, state):
        nonlocal installing_writes
        original_write_state(root, state)
        if state["status"] == "installing" and state["completed_paths"]:
            installing_writes += 1
            if installing_writes == 1:
                raise OSError("simulated Manager/SSH crash")

    monkeypatch.setattr(transaction, "_write_state", crash_after_first_path)
    with pytest.raises(OSError, match="simulated"):
        transaction.run_transaction("install", payload)

    assert (
        target / "results" / "value.txt"
    ).read_text(encoding="utf-8") == "new-results"
    assert (
        target / "state/cache" / "value.txt"
    ).read_text(encoding="utf-8") == "old-cache"

    monkeypatch.setattr(transaction, "_write_state", original_write_state)
    result = transaction.reconcile_existing(
        transaction._identity_from_payload(payload)
    )

    assert result["status"] == "installed"
    assert (
        target / "results" / "value.txt"
    ).read_text(encoding="utf-8") == "new-results"
    assert (
        target / "state/cache" / "value.txt"
    ).read_text(encoding="utf-8") == "new-cache"
    state = json.loads(
        (
            transaction.transaction_root(payload) / "state.json"
        ).read_text(encoding="utf-8")
    )
    assert state["status"] == "installed"
    assert state["completed_paths"] == payload["paths"]


def test_reconcile_refuses_an_uncommitted_transfer(tmp_path):
    target = tmp_path / "work"
    target.mkdir()
    payload = _payload(target)
    transaction.run_transaction("prepare", payload)
    _write(
        transaction.staged_path(payload, "results") / "partial.txt",
        "partial",
    )

    with pytest.raises(
        transaction.RecoveryTransactionError,
        match="not durably committed",
    ):
        transaction.run_transaction("reconcile", payload)

    assert not (target / "results").exists()
    assert not (target / "state/cache").exists()


def test_installed_marker_never_resurrects_application_deletion(tmp_path):
    target = tmp_path / "work"
    target.mkdir()
    payload = _payload(target)
    transaction.run_transaction("prepare", payload)
    _write(
        transaction.staged_path(payload, "results") / "value.txt",
        "new-results",
    )
    _write(
        transaction.staged_path(payload, "state/cache") / "value.txt",
        "new-cache",
    )
    transaction.run_transaction("install", payload)

    (target / "results" / "value.txt").unlink()
    transaction.run_transaction("reconcile", payload)

    assert not (target / "results" / "value.txt").exists()
    assert (
        target / "state/cache" / "value.txt"
    ).read_text(encoding="utf-8") == "new-cache"


def test_transaction_journal_survives_workload_tree_cleanup(tmp_path):
    target = tmp_path / "work"
    target.mkdir()
    payload = _payload(target)
    transaction.run_transaction("prepare", payload)
    _write(
        transaction.staged_path(payload, "results") / "value.txt",
        "new-results",
    )
    _write(
        transaction.staged_path(payload, "state/cache") / "value.txt",
        "new-cache",
    )
    transaction.run_transaction("install", payload)
    state_path = transaction.transaction_root(payload) / "state.json"

    assert target not in state_path.parents
    shutil.rmtree(target)
    target.mkdir()

    assert json.loads(state_path.read_text(encoding="utf-8"))["status"] == (
        "installed"
    )


def test_reconcile_existing_rejects_different_worker_identity(tmp_path):
    target = tmp_path / "work"
    target.mkdir()
    payload = _payload(target)
    transaction.run_transaction("prepare", payload)
    _write(
        transaction.staged_path(payload, "results") / "value.txt",
        "new-results",
    )
    _write(
        transaction.staged_path(payload, "state/cache") / "value.txt",
        "new-cache",
    )
    transaction.run_transaction("install", payload)
    identity = transaction._identity_from_payload(payload)
    identity["worker_id"] = "aws:i-different"

    with pytest.raises(
        transaction.RecoveryTransactionError,
        match="identity changed",
    ):
        transaction.reconcile_existing(identity)


def test_prepare_rejects_symlinked_control_directory(tmp_path):
    target = tmp_path / "work"
    outside = tmp_path / "outside"
    target.mkdir()
    outside.mkdir()
    transaction._CONTROL_ROOT.parent.mkdir(parents=True)
    transaction._CONTROL_ROOT.symlink_to(
        outside, target_is_directory=True,
    )

    with pytest.raises(
        transaction.RecoveryTransactionError,
        match="not a real directory",
    ):
        transaction.run_transaction("prepare", _payload(target))


def test_prepare_rejects_control_root_on_another_filesystem(
    tmp_path, monkeypatch,
):
    target = tmp_path / "work"
    target.mkdir()
    control = transaction._private_control_root(create=True)
    real_stat = transaction.os.stat

    def different_control_device(path, *args, **kwargs):
        metadata = real_stat(path, *args, **kwargs)
        if Path(path) == control:
            return SimpleNamespace(st_dev=metadata.st_dev + 1)
        return metadata

    monkeypatch.setattr(transaction.os, "stat", different_control_device)

    with pytest.raises(
        transaction.RecoveryTransactionError,
        match="control root must share the target filesystem",
    ):
        transaction.run_transaction("prepare", _payload(target))

    assert not (
        control / "job-recovery" / "shard-00003" / "state.json"
    ).exists()


def test_prepare_checks_worker_staging_capacity_before_receive(
    tmp_path, monkeypatch,
):
    target = tmp_path / "work"
    target.mkdir()
    payload = _payload(target)
    wrapper_objects = 64 + 2 * sum(
        len(Path(path).parts) for path in payload["paths"]
    )
    required = (
        payload["total_bytes"]
        + (payload["total_objects"] + wrapper_objects) * 4_096
        + payload["disk_reserve_bytes"]
    )
    monkeypatch.setattr(
        transaction.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            # A one-byte fragment count must not be multiplied by the
            # conservative per-object 4 KiB allocation estimate.
            f_bavail=required - 1,
            f_frsize=1,
            f_bsize=4_096,
            f_favail=100_000,
        ),
    )

    with pytest.raises(
        transaction.RecoveryTransactionError,
        match="insufficient Worker disk",
    ):
        transaction.run_transaction("prepare", payload)


def test_prepare_accounts_for_one_block_per_tiny_object(
    tmp_path, monkeypatch,
):
    target = tmp_path / "work"
    target.mkdir()
    payload = _payload(target)
    payload["total_bytes"] = 1
    payload["total_objects"] = 50_000
    payload["disk_reserve_bytes"] = 4_095
    wrapper_objects = 64 + 2 * sum(
        len(Path(path).parts) for path in payload["paths"]
    )
    required = (
        payload["total_bytes"]
        + (payload["total_objects"] + wrapper_objects) * 4_096
        + payload["disk_reserve_bytes"]
    )
    monkeypatch.setattr(
        transaction.os,
        "statvfs",
        lambda _path: SimpleNamespace(
            # ``required`` is block-aligned, so this is exactly one
            # allocation block short.
            f_bavail=(required // 4_096) - 1,
            f_frsize=4_096,
            f_bsize=4_096,
            f_favail=100_000,
        ),
    )

    with pytest.raises(
        transaction.RecoveryTransactionError,
        match="insufficient Worker disk",
    ):
        transaction.run_transaction("prepare", payload)


@pytest.mark.parametrize(
    "path",
    ["../escape", "/absolute", ".", "a/../b", ".elastic-agent-managed-recovery-v1"],
)
def test_payload_rejects_unsafe_or_reserved_paths(tmp_path, path):
    target = tmp_path / "work"
    target.mkdir()
    payload = _payload(target)
    payload["paths"] = [path]

    with pytest.raises(transaction.RecoveryTransactionError):
        transaction.encode_payload(payload)
