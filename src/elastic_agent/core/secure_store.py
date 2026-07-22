"""Small, dependency-free helpers for Manager-local durable state.

Manager state may contain worker bearer tokens, account metadata, repository
coordinates, commands, and environment variables.  Relying on the process
umask is therefore not sufficient: each store must repair legacy modes before
reading and publish replacements durably.
"""

from __future__ import annotations

import os
import stat
import uuid
from pathlib import Path

STATE_DIRECTORY_MODE = 0o700
STATE_FILE_MODE = 0o600


def secure_state_directory(path: str | Path) -> Path:
    """Create (or tighten) one state directory and return its expanded path."""

    directory = Path(path).expanduser()
    directory.mkdir(parents=True, exist_ok=True, mode=STATE_DIRECTORY_MODE)
    os.chmod(directory, STATE_DIRECTORY_MODE)
    return directory


def tighten_state_file(path: str | Path) -> Path:
    """Repair a legacy state file's mode before any potentially secret read."""

    state_path = Path(path).expanduser()
    if state_path.is_symlink():
        raise RuntimeError(f"state path must not be a symlink: {state_path}")
    if state_path.exists():
        mode = state_path.lstat().st_mode
        if not stat.S_ISREG(mode):
            raise RuntimeError(f"state path is not a regular file: {state_path}")
        os.chmod(state_path, STATE_FILE_MODE)
    return state_path


def tighten_private_json_directory(
    path: str | Path, *, create: bool = False
) -> Path:
    """Tighten a directory and existing JSON journals without reading them."""

    directory = Path(path).expanduser()
    if not directory.exists() and not create:
        return directory
    secure_state_directory(directory)
    for journal in directory.glob("*.json"):
        tighten_state_file(journal)
    return directory


def fsync_directory(path: str | Path) -> None:
    """Persist a directory entry when the platform exposes directory fsync."""

    if not hasattr(os, "O_DIRECTORY"):
        return
    directory_fd = os.open(Path(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def atomic_write_private(path: str | Path, data: str) -> Path:
    """Atomically write UTF-8 state with mode 0600 and fsync file + directory."""

    destination = Path(path).expanduser()
    directory = secure_state_directory(destination.parent)
    temporary = directory / f".{destination.name}.{uuid.uuid4().hex}.tmp"
    fd: int | None = None
    try:
        fd = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            STATE_FILE_MODE,
        )
        os.fchmod(fd, STATE_FILE_MODE)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            fd = None  # ownership transferred to ``stream``
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, destination)
        # Be explicit even though the replacement inherits the temp file mode.
        os.chmod(destination, STATE_FILE_MODE)
        fsync_directory(directory)
    finally:
        if fd is not None:
            os.close(fd)
        temporary.unlink(missing_ok=True)
    return destination
