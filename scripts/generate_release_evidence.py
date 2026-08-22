#!/usr/bin/env python3
"""Generate or check deterministic evidence for the exact tracked release tree."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from elastic_agent.core.release_evidence import (  # noqa: E402
    RELEASE_INDEX_SCHEMA,
    RELEASE_MANIFEST_SCHEMA,
    artifact_revision,
    canonical_json,
    compute_release_artifact_digest,
    compute_release_digest,
    compute_task_platform_worker_profile_digest,
    compute_worker_runtime_provenance_digest,
    validate_task_platform_worker_profile,
)

MANIFEST_PATH = ROOT / "deploy/release-manifest.json"
INDEX_PATH = ROOT / "deploy/release-files.json"
EXCLUDED = {"deploy/release-manifest.json", "deploy/release-files.json"}


def _tracked_entries() -> list[tuple[str, str]]:
    result = subprocess.run(
        ["git", "ls-files", "--stage", "-z"],
        cwd=ROOT,
        check=True,
        stdout=subprocess.PIPE,
    )
    entries: list[tuple[str, str]] = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        metadata, raw_path = raw.split(b"\t", 1)
        mode = metadata.split(b" ", 1)[0].decode("ascii")
        path = os.fsdecode(raw_path)
        if path in EXCLUDED:
            continue
        if mode not in {"100644", "100755", "120000"}:
            raise SystemExit(f"unsupported tracked release entry mode {mode}: {path}")
        entries.append((path, mode))
    entries.sort()
    if len({path for path, _mode in entries}) != len(entries):
        raise SystemExit("tracked release contains duplicate paths")
    return entries


def _record(path: str, git_mode: str) -> dict[str, Any]:
    absolute = ROOT / path
    if git_mode == "120000":
        if not absolute.is_symlink():
            raise SystemExit(f"tracked symlink is not a symlink: {path}")
        data = os.fsencode(os.readlink(absolute))
        kind = "symlink"
        mode = "120000"
    else:
        if not absolute.is_file() or absolute.is_symlink():
            raise SystemExit(f"tracked file is not a regular file: {path}")
        data = absolute.read_bytes()
        kind = "file"
        mode = "0755" if git_mode == "100755" else "0644"
    return {
        "path": path,
        "type": kind,
        "mode": mode,
        "size": len(data),
        "sha256": f"sha256:{hashlib.sha256(data).hexdigest()}",
    }


def _load_worker_profile(path: Path) -> dict[str, Any]:
    try:
        if path.stat().st_size > 1_048_576:
            raise SystemExit("worker profile input exceeds size limit")
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise SystemExit("worker profile input cannot be read") from exc
    try:
        return validate_task_platform_worker_profile(raw)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc


def generate(worker_profile_input: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    previous = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    index = {
        "schema_version": RELEASE_INDEX_SCHEMA,
        "files": [_record(path, mode) for path, mode in _tracked_entries()],
    }
    artifact_digest = compute_release_artifact_digest(index)
    revision = artifact_revision(artifact_digest)
    provenance_key = (
        "worker_runtime_provenance"
        if previous.get("schema_version") == RELEASE_MANIFEST_SCHEMA
        else "worker_profile"
    )
    worker_runtime_provenance = dict(previous[provenance_key])
    worker_runtime_provenance["release_revision"] = revision
    worker_profile = _load_worker_profile(worker_profile_input)
    if worker_profile["ami_id"] != worker_runtime_provenance["ami_id"]:
        raise SystemExit("worker profile AMI does not match runtime provenance")
    manifest = {
        "schema_version": RELEASE_MANIFEST_SCHEMA,
        "product": "elastic-agent",
        "upstream_source_commit": previous.get(
            "upstream_source_commit", previous.get("source_commit")
        ),
        "upstream_archive_sha256": previous.get(
            "upstream_archive_sha256", previous.get("archive_sha256")
        ),
        "release_revision": revision,
        "release_index": "deploy/release-files.json",
        "release_artifact_digest": artifact_digest,
        "manager_state_schema": previous["manager_state_schema"],
        "worker_runtime_provenance": worker_runtime_provenance,
        "worker_runtime_provenance_digest": compute_worker_runtime_provenance_digest(
            worker_runtime_provenance
        ),
        "worker_profile": worker_profile,
        "worker_profile_digest": compute_task_platform_worker_profile_digest(
            worker_profile
        ),
        "release_digest": "",
    }
    manifest["release_digest"] = compute_release_digest(manifest)
    return index, manifest


def _render(value: dict[str, Any]) -> bytes:
    # Pretty output is reviewable; digests remain based on canonical_json().
    canonical_json(value)
    return (json.dumps(value, indent=2, ensure_ascii=True) + "\n").encode("utf-8")


def _atomic_write(path: Path, data: bytes) -> None:
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    try:
        temporary.chmod(0o644)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if checked-in evidence differs from the tracked release tree",
    )
    parser.add_argument(
        "--worker-profile-input",
        type=Path,
        required=True,
        help="path to the authoritative Task Platform WorkerProfileInput JSON",
    )
    args = parser.parse_args()
    index, manifest = generate(args.worker_profile_input)
    expected = {
        INDEX_PATH: _render(index),
        MANIFEST_PATH: _render(manifest),
    }
    if args.check:
        stale = [path for path, data in expected.items() if path.read_bytes() != data]
        if stale:
            for path in stale:
                print(f"stale release evidence: {path.relative_to(ROOT)}", file=sys.stderr)
            return 1
        return 0
    for path, data in expected.items():
        _atomic_write(path, data)
    print(manifest["release_revision"])
    print(manifest["release_artifact_digest"])
    print(manifest["release_digest"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
