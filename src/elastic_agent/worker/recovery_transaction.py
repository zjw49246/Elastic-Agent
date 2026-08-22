"""Crash-safe installation of a prepared checkpoint on one Worker.

The Manager transfers every selected directory into a private staging tree,
then invokes this module to durably switch all directories into the workload
checkout.  A directory rename is atomic, but a set of directory renames is
not.  The transaction journal makes the set *roll-forward atomic*: startup
recovery either completes an interrupted install before collecting results, or
rejects an uncommitted transfer without publishing a partial checkpoint.
"""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import os
import pwd
import re
import shutil
import stat
import tempfile
from pathlib import Path, PurePosixPath

_SCHEMA_VERSION = 1
_CONTROL_DIR = ".elastic-agent-managed-recovery-v1"
_DEFAULT_CONTROL_ROOT = Path(
    "/var/lib/elastic-agent/recovery-transactions-v1"
)
_CONTROL_ROOT = _DEFAULT_CONTROL_ROOT
_SAFE_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SAFE_GENERATION = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")
_SAFE_WORKER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9:._-]{0,255}")
_SAFE_RUN_USER = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,63}")
_SHA256 = re.compile(r"[0-9a-f]{64}")
_VALID_STATES = frozenset({"receiving", "installing", "installed"})
_FREE_INODE_RESERVE = 10_000


class RecoveryTransactionError(RuntimeError):
    """The remote recovery transaction cannot be proven safe."""


def _validated_payload(raw: dict) -> dict:
    if not isinstance(raw, dict) or raw.get("schema_version") != _SCHEMA_VERSION:
        raise RecoveryTransactionError("unsupported recovery transaction schema")
    job_id = raw.get("job_id")
    if not isinstance(job_id, str) or _SAFE_ID.fullmatch(job_id) is None:
        raise RecoveryTransactionError("invalid recovery transaction Job id")
    shard_index = raw.get("shard_index")
    if (
        isinstance(shard_index, bool)
        or not isinstance(shard_index, int)
        or shard_index < 0
        or shard_index > 99_999
    ):
        raise RecoveryTransactionError("invalid recovery transaction shard")
    target_raw = raw.get("target_dir")
    if not isinstance(target_raw, str) or not target_raw.startswith("/"):
        raise RecoveryTransactionError("recovery target_dir must be absolute")
    target = PurePosixPath(target_raw)
    if (
        str(target) != target_raw.rstrip("/")
        or target == PurePosixPath("/")
        or ".." in target.parts
    ):
        raise RecoveryTransactionError("invalid recovery transaction target_dir")
    generation = raw.get("generation")
    if (
        not isinstance(generation, str)
        or _SAFE_GENERATION.fullmatch(generation) is None
    ):
        raise RecoveryTransactionError(
            "recovery transaction requires an exact generation"
        )
    source_job_id = raw.get("source_job_id")
    if (
        not isinstance(source_job_id, str)
        or _SAFE_ID.fullmatch(source_job_id) is None
    ):
        raise RecoveryTransactionError(
            "invalid recovery transaction source Job id"
        )
    worker_id = raw.get("worker_id")
    if (
        not isinstance(worker_id, str)
        or _SAFE_WORKER_ID.fullmatch(worker_id) is None
    ):
        raise RecoveryTransactionError(
            "invalid recovery transaction Worker id"
        )
    run_user = raw.get("run_user")
    if (
        not isinstance(run_user, str)
        or _SAFE_RUN_USER.fullmatch(run_user) is None
    ):
        raise RecoveryTransactionError(
            "invalid recovery transaction run user"
        )
    contract_sha256 = raw.get("recovery_contract_sha256")
    if (
        not isinstance(contract_sha256, str)
        or _SHA256.fullmatch(contract_sha256) is None
    ):
        raise RecoveryTransactionError(
            "invalid recovery transaction contract hash"
        )
    total_bytes = raw.get("total_bytes")
    total_objects = raw.get("total_objects")
    disk_reserve_bytes = raw.get("disk_reserve_bytes")
    if (
        isinstance(total_bytes, bool)
        or not isinstance(total_bytes, int)
        or total_bytes < 0
        or isinstance(total_objects, bool)
        or not isinstance(total_objects, int)
        or total_objects < 0
        or isinstance(disk_reserve_bytes, bool)
        or not isinstance(disk_reserve_bytes, int)
        or disk_reserve_bytes <= 0
    ):
        raise RecoveryTransactionError(
            "invalid recovery transaction capacity"
        )
    raw_paths = raw.get("paths")
    if (
        not isinstance(raw_paths, list)
        or not raw_paths
        or len(raw_paths) > 32
    ):
        raise RecoveryTransactionError("invalid recovery transaction paths")
    paths: list[str] = []
    for candidate in raw_paths:
        if not isinstance(candidate, str):
            raise RecoveryTransactionError("invalid recovery transaction path")
        path = PurePosixPath(candidate)
        if (
            not candidate
            or candidate != str(path)
            or path.is_absolute()
            or path == PurePosixPath(".")
            or ".." in path.parts
            or _CONTROL_DIR in path.parts
        ):
            raise RecoveryTransactionError("invalid recovery transaction path")
        paths.append(candidate)
    ordered = sorted(paths, key=lambda value: PurePosixPath(value).parts)
    if len(set(ordered)) != len(ordered):
        raise RecoveryTransactionError("duplicate recovery transaction path")
    for parent, child in zip(ordered, ordered[1:]):
        if child.startswith(parent.rstrip("/") + "/"):
            raise RecoveryTransactionError(
                "overlapping recovery transaction paths"
            )
    return {
        "schema_version": _SCHEMA_VERSION,
        "job_id": job_id,
        "shard_index": shard_index,
        "target_dir": str(target),
        "generation": generation,
        "source_job_id": source_job_id,
        "worker_id": worker_id,
        "run_user": run_user,
        "recovery_contract_sha256": contract_sha256,
        "total_bytes": total_bytes,
        "total_objects": total_objects,
        "disk_reserve_bytes": disk_reserve_bytes,
        "paths": paths,
    }


def descriptor_sha256(payload: dict) -> str:
    normalized = _validated_payload(payload)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def encode_payload(payload: dict) -> str:
    """Return a shell/argv-safe encoding of a validated transaction payload."""

    normalized = _validated_payload(payload)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(encoded).decode("ascii")


def decode_payload(encoded: str) -> dict:
    try:
        raw = base64.b64decode(
            encoded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        value = json.loads(raw)
    except (UnicodeEncodeError, ValueError, json.JSONDecodeError) as exc:
        raise RecoveryTransactionError(
            "invalid recovery transaction payload"
        ) from exc
    return _validated_payload(value)


def _identity_from_payload(payload: dict) -> dict:
    normalized = _validated_payload(payload)
    return {
        key: normalized[key]
        for key in (
            "schema_version",
            "job_id",
            "shard_index",
            "target_dir",
            "generation",
            "source_job_id",
            "worker_id",
            "run_user",
            "recovery_contract_sha256",
            "paths",
        )
    }


def _validated_identity(raw: dict) -> dict:
    if not isinstance(raw, dict):
        raise RecoveryTransactionError(
            "invalid recovery transaction identity"
        )
    candidate = {
        **raw,
        "total_bytes": 0,
        "total_objects": 0,
        "disk_reserve_bytes": 1,
    }
    return _identity_from_payload(candidate)


def encode_identity(identity: dict) -> str:
    normalized = _validated_identity(identity)
    encoded = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return base64.urlsafe_b64encode(encoded).decode("ascii")


def decode_identity(encoded: str) -> dict:
    try:
        raw = base64.b64decode(
            encoded.encode("ascii"),
            altchars=b"-_",
            validate=True,
        )
        value = json.loads(raw)
    except (UnicodeEncodeError, ValueError, json.JSONDecodeError) as exc:
        raise RecoveryTransactionError(
            "invalid recovery transaction identity"
        ) from exc
    return _validated_identity(value)


def transaction_root(payload: dict) -> Path:
    normalized = _validated_payload(payload)
    return (
        _CONTROL_ROOT
        / normalized["job_id"]
        / f"shard-{normalized['shard_index']:05d}"
    )


def staged_path(payload: dict, relative: str) -> Path:
    normalized = _validated_payload(payload)
    if relative not in normalized["paths"]:
        raise RecoveryTransactionError("unknown recovery transaction path")
    return transaction_root(normalized) / "staged" / relative


def _transaction_root_from_identity(identity: dict) -> Path:
    normalized = _validated_identity(identity)
    return (
        _CONTROL_ROOT
        / normalized["job_id"]
        / f"shard-{normalized['shard_index']:05d}"
    )


def _fsync_directory(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    fd = os.open(path, flags)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _require_real_directory(path: Path) -> None:
    try:
        mode = os.lstat(path).st_mode
    except OSError as exc:
        raise RecoveryTransactionError(
            f"recovery directory is unavailable: {path}"
        ) from exc
    if not stat.S_ISDIR(mode):
        raise RecoveryTransactionError(
            f"recovery path is not a real directory: {path}"
        )


def _require_real_chain(path: Path) -> None:
    current = Path(path.anchor)
    _require_real_directory(current)
    for part in path.parts[1:]:
        current /= part
        _require_real_directory(current)


def _mkdir_real_chain(root: Path, relative: PurePosixPath) -> Path:
    _require_real_chain(root)
    current = root
    for part in relative.parts:
        current /= part
        try:
            os.mkdir(current, mode=0o700)
            _fsync_directory(current.parent)
        except FileExistsError:
            pass
        _require_real_directory(current)
    return current


def _private_control_root(*, create: bool) -> Path:
    """Return the supervisor-owned journal root outside the workload checkout."""

    root = _CONTROL_ROOT
    if not root.is_absolute() or root == Path("/"):
        raise RecoveryTransactionError(
            "invalid recovery transaction control root"
        )
    if root == _DEFAULT_CONTROL_ROOT and os.geteuid() != 0:
        raise RecoveryTransactionError(
            "recovery transaction helper must run as root"
        )
    if create:
        _mkdir_real_chain(
            Path(root.anchor),
            PurePosixPath(*root.parts[1:]),
        )
    else:
        _require_real_chain(root)
    try:
        metadata = os.lstat(root)
    except OSError as exc:
        raise RecoveryTransactionError(
            "recovery transaction control root is unavailable"
        ) from exc
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.geteuid()
        or metadata.st_mode & 0o077
    ):
        raise RecoveryTransactionError(
            "recovery transaction control root is not private"
        )
    return root


def _open_lock(path: Path) -> int:
    flags = os.O_RDWR | os.O_CREAT
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o600)
    except OSError as exc:
        raise RecoveryTransactionError(
            "cannot open recovery transaction lock"
        ) from exc
    if not stat.S_ISREG(os.fstat(fd).st_mode):
        os.close(fd)
        raise RecoveryTransactionError(
            "recovery transaction lock is not a regular file"
        )
    os.fchmod(fd, 0o600)
    fcntl.flock(fd, fcntl.LOCK_EX)
    return fd


def _state_payload(payload: dict, status: str, completed: list[str]) -> dict:
    if status not in _VALID_STATES:
        raise RecoveryTransactionError("invalid recovery transaction state")
    state = {
        **_validated_payload(payload),
        "status": status,
        "completed_paths": completed,
    }
    state["descriptor_sha256"] = descriptor_sha256(payload)
    return state


def _write_state(root: Path, state: dict) -> None:
    encoded = (
        json.dumps(
            state,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    fd, temporary = tempfile.mkstemp(prefix=".state-", dir=root)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as handle:
            fd = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, root / "state.json")
        _fsync_directory(root)
    finally:
        if fd >= 0:
            os.close(fd)
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


def _load_state(root: Path, payload: dict) -> dict:
    path = root / "state.json"
    try:
        if not stat.S_ISREG(os.lstat(path).st_mode):
            raise RecoveryTransactionError(
                "recovery transaction state is not a regular file"
            )
        if path.stat().st_size > 64 * 1024:
            raise RecoveryTransactionError(
                "recovery transaction state is oversized"
            )
        state = json.loads(path.read_text(encoding="utf-8"))
    except RecoveryTransactionError:
        raise
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise RecoveryTransactionError(
            "recovery transaction state is unavailable"
        ) from exc
    expected = _validated_payload(payload)
    if not isinstance(state, dict):
        raise RecoveryTransactionError("invalid recovery transaction state")
    for key, value in expected.items():
        if state.get(key) != value:
            raise RecoveryTransactionError(
                "recovery transaction identity changed"
            )
    if state.get("descriptor_sha256") != descriptor_sha256(expected):
        raise RecoveryTransactionError(
            "recovery transaction descriptor hash changed"
        )
    status = state.get("status")
    completed = state.get("completed_paths")
    if (
        status not in _VALID_STATES
        or not isinstance(completed, list)
        or any(
            not isinstance(path, str)
            or path not in expected["paths"]
            for path in completed
        )
        or len(set(completed)) != len(completed)
    ):
        raise RecoveryTransactionError("invalid recovery transaction state")
    return state


def _remove_entry(path: Path) -> None:
    try:
        mode = os.lstat(path).st_mode
    except FileNotFoundError:
        return
    if stat.S_ISDIR(mode):
        shutil.rmtree(path)
    else:
        os.unlink(path)


def _fsync_and_measure_staged(payload: dict, staged_root: Path) -> None:
    try:
        run_identity = pwd.getpwnam(payload["run_user"])
    except KeyError as exc:
        raise RecoveryTransactionError(
            "recovery transaction run user does not exist"
        ) from exc
    run_uid = run_identity.pw_uid
    run_gid = run_identity.pw_gid
    total_bytes = 0
    total_objects = 0
    for relative in payload["paths"]:
        selected = staged_root.joinpath(*PurePosixPath(relative).parts)
        _require_real_directory(selected)
        pending = [selected]
        directories: list[Path] = []
        while pending:
            directory = pending.pop()
            directories.append(directory)
            total_objects += 1
            os.chown(
                directory,
                run_uid,
                run_gid,
                follow_symlinks=False,
            )
            try:
                entries = os.scandir(directory)
            except OSError as exc:
                raise RecoveryTransactionError(
                    "cannot scan staged recovery path"
                ) from exc
            with entries:
                for entry in entries:
                    try:
                        entry_stat = entry.stat(follow_symlinks=False)
                    except OSError as exc:
                        raise RecoveryTransactionError(
                            "cannot inspect staged recovery path"
                        ) from exc
                    path = Path(entry.path)
                    if stat.S_ISDIR(entry_stat.st_mode):
                        pending.append(path)
                        continue
                    if not stat.S_ISREG(entry_stat.st_mode):
                        raise RecoveryTransactionError(
                            "staged recovery contains a non-regular entry"
                        )
                    total_objects += 1
                    total_bytes += entry_stat.st_size
                    if (
                        total_bytes > payload["total_bytes"]
                        or total_objects > payload["total_objects"]
                    ):
                        raise RecoveryTransactionError(
                            "staged recovery exceeds its authenticated manifest"
                        )
                    flags = os.O_RDONLY
                    if hasattr(os, "O_NOFOLLOW"):
                        flags |= os.O_NOFOLLOW
                    fd = os.open(path, flags)
                    try:
                        opened = os.fstat(fd)
                        if (
                            not stat.S_ISREG(opened.st_mode)
                            or opened.st_dev != entry_stat.st_dev
                            or opened.st_ino != entry_stat.st_ino
                            or opened.st_size != entry_stat.st_size
                        ):
                            raise RecoveryTransactionError(
                                "staged recovery changed while being verified"
                            )
                        os.fchown(fd, run_uid, run_gid)
                        os.fsync(fd)
                    finally:
                        os.close(fd)
        for directory in reversed(directories):
            _fsync_directory(directory)
    if (
        total_bytes != payload["total_bytes"]
        or total_objects != payload["total_objects"]
    ):
        raise RecoveryTransactionError(
            "staged recovery does not match its authenticated manifest"
        )
    filesystem = os.statvfs(staged_root)
    if (
        filesystem.f_bavail * filesystem.f_frsize
        < payload["disk_reserve_bytes"]
        or filesystem.f_favail < _FREE_INODE_RESERVE
    ):
        raise RecoveryTransactionError(
            "Worker recovery transaction exhausted its disk reserve"
        )


def _prepare(payload: dict) -> dict:
    target = Path(payload["target_dir"])
    _require_real_chain(target)
    try:
        run_identity = pwd.getpwnam(payload["run_user"])
    except KeyError as exc:
        raise RecoveryTransactionError(
            "recovery transaction run user does not exist"
        ) from exc
    if os.stat(target).st_uid != run_identity.pw_uid:
        raise RecoveryTransactionError(
            "recovery target is not owned by the run user"
        )
    control = _private_control_root(create=True)
    target_stat = os.stat(target)
    if os.stat(control).st_dev != target_stat.st_dev:
        raise RecoveryTransactionError(
            "recovery control root must share the target filesystem"
        )
    job_root = _mkdir_real_chain(
        control,
        PurePosixPath(
            payload["job_id"],
            f"shard-{payload['shard_index']:05d}",
        ),
    )
    if os.stat(job_root).st_dev != target_stat.st_dev:
        raise RecoveryTransactionError(
            "recovery transaction root must share the target filesystem"
        )
    lock_fd = _open_lock(job_root / "transaction.lock")
    try:
        state_path = job_root / "state.json"
        if state_path.exists():
            state = _load_state(job_root, payload)
            if state["status"] != "receiving":
                return {
                    "status": state["status"],
                    "transaction_root": str(job_root),
                    "descriptor_sha256": descriptor_sha256(payload),
                }
        else:
            for relative in payload["paths"]:
                parent = target.joinpath(
                    *PurePosixPath(relative).parts[:-1]
                )
                while not parent.exists():
                    parent = parent.parent
                _require_real_chain(parent)
                if os.stat(parent).st_dev != target_stat.st_dev:
                    raise RecoveryTransactionError(
                        "recovery paths must share the target filesystem"
                    )
            filesystem = os.statvfs(target)
            wrapper_objects = 64 + 2 * sum(
                len(PurePosixPath(path).parts)
                for path in payload["paths"]
            )
            fragment_size = int(
                getattr(filesystem, "f_frsize", 0)
                or getattr(filesystem, "f_bsize", 0)
                or 0
            )
            if fragment_size <= 0:
                raise RecoveryTransactionError(
                    "Worker filesystem reported an invalid fragment size"
                )
            allocation_unit = max(4_096, fragment_size)
            available_bytes = filesystem.f_bavail * fragment_size
            # Logical manifest bytes alone understate real disk consumption for
            # large tiny-file checkpoints. Every file/directory and transaction
            # wrapper consumes at least one allocation unit in addition to the
            # authenticated logical payload.
            required_bytes = (
                payload["total_bytes"]
                + (
                    payload["total_objects"] + wrapper_objects
                ) * allocation_unit
                + payload["disk_reserve_bytes"]
            )
            if (
                available_bytes < required_bytes
            ):
                raise RecoveryTransactionError(
                    "insufficient Worker disk for recovery transaction"
                )
            if (
                filesystem.f_favail
                < (
                    payload["total_objects"]
                    + wrapper_objects
                    + _FREE_INODE_RESERVE
                )
            ):
                raise RecoveryTransactionError(
                    "insufficient Worker inodes for recovery transaction"
                )
            _write_state(job_root, _state_payload(payload, "receiving", []))
        staged_root = _mkdir_real_chain(
            job_root, PurePosixPath("staged"),
        )
        for relative in payload["paths"]:
            _mkdir_real_chain(staged_root, PurePosixPath(relative))
        return {
            "status": "receiving",
            "transaction_root": str(job_root),
            "descriptor_sha256": descriptor_sha256(payload),
        }
    finally:
        os.close(lock_fd)


def _roll_forward(root: Path, payload: dict, state: dict) -> dict:
    target = Path(payload["target_dir"])
    staged_root = root / "staged"
    backups_root = _mkdir_real_chain(root, PurePosixPath("backups"))
    completed = list(state["completed_paths"])
    for relative in payload["paths"]:
        if relative in completed:
            continue
        rel = PurePosixPath(relative)
        stage = staged_root.joinpath(*rel.parts)
        destination = target.joinpath(*rel.parts)
        backup = backups_root.joinpath(*rel.parts)
        _mkdir_real_chain(target, PurePosixPath(*rel.parts[:-1]))
        _mkdir_real_chain(backups_root, PurePosixPath(*rel.parts[:-1]))
        stage_exists = os.path.lexists(stage)
        destination_exists = os.path.lexists(destination)
        backup_exists = os.path.lexists(backup)
        if stage_exists:
            _require_real_directory(stage)
            if destination_exists and backup_exists:
                raise RecoveryTransactionError(
                    f"ambiguous recovery install state for {relative!r}"
                )
            if destination_exists:
                os.replace(destination, backup)
                _fsync_directory(destination.parent)
                _fsync_directory(backup.parent)
                destination_exists = False
            os.replace(stage, destination)
            _fsync_directory(stage.parent)
            _fsync_directory(destination.parent)
        elif not destination_exists:
            raise RecoveryTransactionError(
                f"recovery install lost both staged and target path {relative!r}"
            )
        # ``stage`` can disappear only through the rename above after the
        # durable installing state was written.  Thus destination-without-stage
        # is a safe crash-recovery proof even if the per-path state write did
        # not happen before power loss.
        completed.append(relative)
        state = _state_payload(payload, "installing", completed)
        _write_state(root, state)
    state = _state_payload(payload, "installed", completed)
    _write_state(root, state)
    _remove_entry(backups_root)
    _remove_entry(staged_root)
    _fsync_directory(root)
    return {
        "status": "installed",
        "transaction_root": str(root),
        "descriptor_sha256": descriptor_sha256(payload),
    }


def _install_or_reconcile(payload: dict, *, reconcile: bool) -> dict:
    _private_control_root(create=False)
    root = transaction_root(payload)
    _require_real_chain(root)
    lock_fd = _open_lock(root / "transaction.lock")
    try:
        state = _load_state(root, payload)
        if state["status"] == "receiving":
            if reconcile:
                raise RecoveryTransactionError(
                    "recovery transfer was not durably committed"
                )
            staged_root = root / "staged"
            _fsync_and_measure_staged(payload, staged_root)
            state = _state_payload(payload, "installing", [])
            _write_state(root, state)
        if state["status"] == "installed":
            # Never recreate an application path after dispatch: the workload
            # is allowed to modify or delete restored files.  The installed
            # marker proves only that the initial switch completed.
            _remove_entry(root / "backups")
            _remove_entry(root / "staged")
            _fsync_directory(root)
            return {
                "status": "installed",
                "transaction_root": str(root),
                "descriptor_sha256": descriptor_sha256(payload),
            }
        return _roll_forward(root, payload, state)
    finally:
        os.close(lock_fd)


def reconcile_existing(identity: dict) -> dict:
    """Reconcile a persisted transaction without re-reading its S3 manifest.

    Startup knows the immutable Job identity and pinned generation, but its
    Manager-local staging tree may have been removed.  The Worker journal owns
    the authenticated byte/object totals; validate its descriptor hash and all
    caller-known identity fields before using those totals to roll forward.
    """

    normalized_identity = _validated_identity(identity)
    _private_control_root(create=False)
    root = _transaction_root_from_identity(normalized_identity)
    try:
        state_path = root / "state.json"
        if not stat.S_ISREG(os.lstat(state_path).st_mode):
            raise RecoveryTransactionError(
                "recovery transaction state is not a regular file"
            )
        if state_path.stat().st_size > 64 * 1024:
            raise RecoveryTransactionError(
                "recovery transaction state is oversized"
            )
        raw_state = json.loads(state_path.read_text(encoding="utf-8"))
        persisted_payload = _validated_payload(raw_state)
    except RecoveryTransactionError:
        raise
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise RecoveryTransactionError(
            "recovery transaction state is unavailable"
        ) from exc
    for key, value in normalized_identity.items():
        if persisted_payload.get(key) != value:
            raise RecoveryTransactionError(
                "recovery transaction identity changed"
            )
    if raw_state.get("descriptor_sha256") != descriptor_sha256(
        persisted_payload
    ):
        raise RecoveryTransactionError(
            "recovery transaction descriptor hash changed"
        )
    return _install_or_reconcile(persisted_payload, reconcile=True)


def run_transaction(mode: str, payload: dict) -> dict:
    normalized = _validated_payload(payload)
    if mode == "prepare":
        return _prepare(normalized)
    if mode == "install":
        return _install_or_reconcile(normalized, reconcile=False)
    if mode == "reconcile":
        return _install_or_reconcile(normalized, reconcile=True)
    raise RecoveryTransactionError("unknown recovery transaction mode")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=(
            "prepare",
            "install",
            "reconcile",
            "reconcile-existing",
        ),
        required=True,
    )
    parser.add_argument("--payload", required=True)
    args = parser.parse_args(argv)
    try:
        if args.mode == "reconcile-existing":
            result = reconcile_existing(decode_identity(args.payload))
        else:
            result = run_transaction(
                args.mode, decode_payload(args.payload),
            )
    except RecoveryTransactionError as exc:
        parser.exit(1, f"recovery transaction failed: {exc}\n")
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
