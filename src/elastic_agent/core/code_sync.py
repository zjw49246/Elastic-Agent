"""Token-safe, content-addressed Manager-to-worker code delivery.

Private repository credentials exist only in the Manager-side git process.  A
resolved commit is checked out beneath a repository-identity hash, inspected
for common credential files, and rsynced without git metadata.  The checkout is
immutable by convention and can safely be shared by concurrent worker
provisioning calls.
"""

from __future__ import annotations

import asyncio
import fcntl
import hashlib
import logging
import os
import re
import shlex
import shutil
import stat
import uuid
from contextlib import asynccontextmanager
from pathlib import Path, PurePosixPath
from urllib.parse import urlparse, urlsplit, urlunsplit

from elastic_agent.core.secure_store import secure_state_directory

logger = logging.getLogger(__name__)

_GIT_COMMIT = re.compile(r"[0-9a-fA-F]{40,64}")
_SAFE_GIT_REF = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]{0,255}")
_DENIED_EXACT_NAMES = {
    ".netrc",
    ".npmrc",
    ".pypirc",
    ".auth",
    ".auth.json",
    "application_default_credentials.json",
    ".git-credentials",
    "auth.json",
    "auth.toml",
    "auth.yaml",
    "auth.yml",
    "credentials",
    "credentials.json",
    "id_dsa",
    "id_ed25519",
    "id_ecdsa",
    "id_rsa",
    "service-account.json",
    "service_account.json",
}
_DENIED_SUFFIXES = {
    ".jks",
    ".kdbx",
    ".key",
    ".p12",
    ".pem",
    ".pfx",
}
_DENIED_DIRECTORY_NAMES = {".aws", ".docker", ".gnupg", ".kube", ".ssh"}
_RSYNC_SECRET_EXCLUDES = (
    ".git",
    ".env*",
    ".secret*",
    ".aws",
    ".docker",
    ".gnupg",
    ".kube",
    ".ssh",
    ".git-credentials",
    "auth.json",
    "credentials",
    "credentials.json",
    "id_*",
    "*.jks",
    "*.key",
    "*.p12",
    "*.pem",
    "*.pfx",
)


class UnsafeCodePayloadError(RuntimeError):
    """A checkout contains files that must never be copied to a worker."""


def _safe_s3_relative_path(object_key: str, prefix: str) -> PurePosixPath:
    """Map one listed S3 key beneath its requested prefix, without escape."""
    if not object_key.startswith(prefix):
        raise ValueError("S3 returned an object outside the requested prefix")
    relative = object_key[len(prefix):]
    path = PurePosixPath(relative)
    if (
        not relative
        or path.is_absolute()
        or "\\" in relative
        or "\x00" in relative
        or any(part in {"", ".", ".."} for part in relative.split("/"))
        or any(ord(char) < 32 for char in relative)
    ):
        raise ValueError(f"unsafe S3 object key: {object_key!r}")
    return path


def _download_s3(uri: str, dest_dir: str) -> int:
    """Download an S3 object or prefix to ``dest_dir`` (Manager-side)."""

    import boto3  # lazy — workers never import this Manager-only dependency

    parsed = urlparse(uri)
    bucket, key = parsed.netloc, parsed.path.lstrip("/")
    if parsed.scheme != "s3" or not bucket or parsed.query or parsed.fragment:
        raise ValueError("S3 URI must be s3://<bucket>/<key>")
    s3 = boto3.client("s3")
    destination = Path(dest_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    count = 0
    if uri.endswith("/") or not key:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=key):
            for obj in page.get("Contents", []):
                object_key = obj["Key"]
                if object_key.endswith("/"):
                    continue
                relative = _safe_s3_relative_path(object_key, key)
                target = destination.joinpath(*relative.parts)
                if not target.resolve(strict=False).is_relative_to(destination):
                    raise ValueError(f"unsafe S3 object key: {object_key!r}")
                target.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, object_key, str(target))
                count += 1
    else:
        filename = _safe_s3_relative_path(key, "").name
        target = destination / filename
        s3.download_file(bucket, key, str(target))
        count = 1
    return count


class ManagerCodeSync:
    def __init__(
        self,
        cache_dir: str,
        *,
        git_token: str | None = None,
        ssh_key: str | None = None,
        ssh_user: str = "ubuntu",
        runner=None,
    ) -> None:
        self._cache = Path(cache_dir).expanduser()
        self._token = git_token
        self._ssh_key = ssh_key
        self._ssh_user = ssh_user
        self._run = runner or self._real_run
        self._locks: dict[str, asyncio.Lock] = {}

    async def _real_run(self, *cmd: str) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await proc.communicate()
        return proc.returncode or 0, output.decode(errors="replace")

    @staticmethod
    def repo_name(repo: str) -> str:
        name = repo.rstrip("/").split("/")[-1]
        return name[:-4] if name.endswith(".git") else name

    @staticmethod
    def _canonical_repo(repo: str) -> str:
        value = repo.strip()
        if not value or value.startswith("-") or any(ord(ch) < 32 for ch in value):
            raise ValueError("repository location is empty or unsafe")
        parsed = urlsplit(value)
        if parsed.scheme and parsed.hostname:
            hostname = parsed.hostname
            if ":" in hostname and not hostname.startswith("["):
                hostname = f"[{hostname}]"
            if parsed.port:
                hostname = f"{hostname}:{parsed.port}"
            # Userinfo may carry a token.  It is never part of cache identity.
            # Git HTTP(S) repository URLs do not need query parameters.  Drop
            # them because access tokens are sometimes supplied there.
            return urlunsplit((parsed.scheme.lower(), hostname, parsed.path, "", ""))
        return value

    @staticmethod
    def _validate_ref(ref: str) -> str:
        value = ref.strip()
        if (
            _SAFE_GIT_REF.fullmatch(value) is None
            or ".." in value
            or "//" in value
            or value.endswith(("/", ".", ".lock"))
            or "@{" in value
        ):
            raise ValueError("git branch/ref is unsafe")
        return value

    def _auth_url(self, repo: str) -> str:
        if self._token and repo.startswith("https://github.com/"):
            return repo.replace(
                "https://github.com/",
                f"https://x-access-token:{self._token}@github.com/",
                1,
            )
        return repo

    def _redact(self, output: str) -> str:
        redacted = output
        if self._token:
            redacted = redacted.replace(self._token, "[REDACTED]")
        return redacted[-300:]

    async def _checked(self, action: str, *cmd: str) -> str:
        rc, output = await self._run(*cmd)
        if rc != 0:
            raise RuntimeError(f"{action} failed: {self._redact(output)}")
        return output

    def _ssh_cmd(self) -> str:
        parts = [
            "ssh",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
        ]
        if self._ssh_key:
            parts += ["-i", self._ssh_key]
        return shlex.join(parts)

    @asynccontextmanager
    async def _repo_lock(self, repo_key: str):
        """Serialize cache mutation across tasks and Manager processes."""

        in_process = self._locks.setdefault(repo_key, asyncio.Lock())
        async with in_process:
            locks_dir = secure_state_directory(self._cache / "locks")
            lock_path = locks_dir / f"{repo_key}.lock"
            fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
            os.fchmod(fd, 0o600)
            try:
                while True:
                    try:
                        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                        break
                    except BlockingIOError:
                        await asyncio.sleep(0.05)
                yield
            finally:
                fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)

    @staticmethod
    def _denied_relative_path(relative: Path) -> bool:
        name = relative.name.casefold()
        parts = {part.casefold() for part in relative.parts}
        return (
            name.startswith(".env")
            or name.startswith(".secret")
            or name in _DENIED_EXACT_NAMES
            or relative.suffix.casefold() in _DENIED_SUFFIXES
            or bool(parts & _DENIED_DIRECTORY_NAMES)
            or (
                "credential" in name
                and relative.suffix.casefold() in {"", ".json", ".toml", ".yaml", ".yml"}
            )
            or (
                "secret" in name
                and relative.suffix.casefold() in {"", ".json", ".toml", ".yaml", ".yml"}
            )
        )

    @classmethod
    def inspect_payload(cls, local_path: str | Path) -> None:
        """Fail closed when a payload contains likely credentials or escapes."""

        root = Path(local_path).resolve()
        if not root.is_dir():
            raise UnsafeCodePayloadError("code payload is not a directory")

        denied: list[str] = []
        for current, directories, files in os.walk(root, followlinks=False):
            current_path = Path(current)
            kept_directories: list[str] = []
            for directory in directories:
                child = current_path / directory
                relative = child.relative_to(root)
                if directory == ".git":
                    continue
                if cls._denied_relative_path(relative):
                    denied.append(relative.as_posix())
                    continue
                if child.is_symlink():
                    try:
                        child.resolve().relative_to(root)
                    except (OSError, ValueError):
                        denied.append(relative.as_posix())
                        continue
                kept_directories.append(directory)
            directories[:] = kept_directories

            for filename in files:
                child = current_path / filename
                relative = child.relative_to(root)
                if cls._denied_relative_path(relative):
                    denied.append(relative.as_posix())
                    continue
                try:
                    mode = child.lstat().st_mode
                    if stat.S_ISLNK(mode):
                        child.resolve().relative_to(root)
                    elif not stat.S_ISREG(mode):
                        denied.append(relative.as_posix())
                except (OSError, ValueError):
                    denied.append(relative.as_posix())

        if denied:
            visible = ", ".join(sorted(set(denied))[:20])
            suffix = " …" if len(set(denied)) > 20 else ""
            raise UnsafeCodePayloadError(
                f"code payload contains prohibited secret/special paths: {visible}{suffix}"
            )

    async def ensure_clone(
        self,
        repo: str,
        branch: str = "main",
        *,
        resolved_commit: str = "",
    ) -> str:
        """Return a checkout keyed by canonical repository URL + resolved commit."""

        canonical_repo = self._canonical_repo(repo)
        ref = self._validate_ref(branch)
        expected_commit = resolved_commit.strip().lower()
        if expected_commit and _GIT_COMMIT.fullmatch(expected_commit) is None:
            raise ValueError("resolved_commit must be a full Git commit id")
        fetch_ref = expected_commit or ref
        repo_key = hashlib.sha256(canonical_repo.encode()).hexdigest()
        secure_state_directory(self._cache)

        async with self._repo_lock(repo_key):
            mirrors = secure_state_directory(self._cache / "mirrors")
            checkouts = secure_state_directory(self._cache / "checkouts" / repo_key)
            mirror = mirrors / f"{repo_key}.git"
            temp_mirror: Path | None = None

            try:
                if not mirror.is_dir():
                    temp_mirror = mirrors / f".{repo_key}.{uuid.uuid4().hex}.tmp"
                    temp_mirror.mkdir(mode=0o700)
                    await self._checked(
                        "manager git init",
                        "git", "init", "--bare", str(temp_mirror),
                    )
                    await self._checked(
                        "manager git remote setup",
                        "git", "--git-dir", str(temp_mirror),
                        "remote", "add", "origin", canonical_repo,
                    )
                    active_mirror = temp_mirror
                else:
                    active_mirror = mirror
                    await self._checked(
                        "manager git remote scrub",
                        "git", "--git-dir", str(active_mirror),
                        "remote", "set-url", "origin", canonical_repo,
                    )

                # Fetch through the credential-bearing URL without persisting it
                # in config or FETCH_HEAD.  The fixed private ref is protected by
                # the repository lock and is used only to resolve the commit.
                await self._checked(
                    "manager git fetch",
                    "git", "--git-dir", str(active_mirror), "fetch",
                    "--no-write-fetch-head", "--force", "--depth", "1", "--",
                    self._auth_url(canonical_repo),
                    f"{fetch_ref}:refs/elastic-agent/resolved",
                )
                resolved = (
                    await self._checked(
                        "manager git resolve",
                        "git", "--git-dir", str(active_mirror), "rev-parse",
                        "refs/elastic-agent/resolved^{commit}",
                    )
                ).strip().splitlines()[0]
                if _GIT_COMMIT.fullmatch(resolved) is None:
                    raise RuntimeError("manager git resolve returned an invalid commit id")
                if expected_commit and resolved.casefold() != expected_commit:
                    raise RuntimeError(
                        "manager git resolve did not return resolved_commit"
                    )

                if temp_mirror is not None:
                    os.replace(temp_mirror, mirror)
                    temp_mirror = None
                    active_mirror = mirror

                checkout = checkouts / resolved.lower()
                if not checkout.exists():
                    temporary = checkouts / f".{resolved}.{uuid.uuid4().hex}.tmp"
                    temporary.mkdir(mode=0o700)
                    try:
                        await self._checked(
                            "manager checkout clone",
                            "git", "clone", "--no-hardlinks", "--no-checkout",
                            "--", str(active_mirror), str(temporary),
                        )
                        await self._checked(
                            "manager checkout object fetch",
                            "git", "-C", str(temporary), "fetch",
                            "--no-write-fetch-head", "--force", "--depth", "1", "--",
                            str(active_mirror),
                            f"{resolved}:refs/elastic-agent/resolved",
                        )
                        await self._checked(
                            "manager checkout",
                            "git", "-C", str(temporary), "checkout",
                            "--detach", resolved,
                        )
                        await self._checked(
                            "manager checkout remote removal",
                            "git", "-C", str(temporary), "remote", "remove", "origin",
                        )
                        self.inspect_payload(temporary)
                        os.replace(temporary, checkout)
                    finally:
                        if temporary.exists():
                            shutil.rmtree(temporary)

                current = (
                    await self._checked(
                        "manager cached checkout verification",
                        "git", "-C", str(checkout), "rev-parse", "HEAD",
                    )
                ).strip().splitlines()[0]
                if current.casefold() != resolved.casefold():
                    raise RuntimeError(
                        "content-addressed checkout failed integrity verification"
                    )
                dirty = await self._checked(
                    "manager cached checkout cleanliness",
                    "git", "-C", str(checkout), "status", "--porcelain",
                    "--untracked-files=all",
                )
                if dirty.strip():
                    raise RuntimeError(
                        "content-addressed checkout contains modified or untracked files"
                    )
                self.inspect_payload(checkout)
                return str(checkout)
            finally:
                if temp_mirror is not None and temp_mirror.exists():
                    shutil.rmtree(temp_mirror)

    async def deliver(self, local_path: str, host: str, target_dir: str) -> bool:
        """Inspect and rsync a checkout without any credential-like paths."""

        self.inspect_payload(local_path)
        quoted_target = shlex.quote(target_dir)
        prep = (
            f"sudo mkdir -p {quoted_target} && "
            f"sudo chown -R {shlex.quote(self._ssh_user)} {quoted_target}"
        )
        rc, _ = await self._run(
            *shlex.split(self._ssh_cmd()),
            f"{self._ssh_user}@{host}",
            prep,
        )
        if rc != 0:
            logger.warning("deliver: could not prep %s on %s", target_dir, host)

        rsync_excludes: list[str] = []
        for pattern in _RSYNC_SECRET_EXCLUDES:
            rsync_excludes.extend(("--exclude", pattern))
        remote_target = (
            f"{self._ssh_user}@{host}:"
            f"{shlex.quote(target_dir.rstrip('/') + '/')}"
        )
        rc, output = await self._run(
            "rsync", "-az", "--delete", *rsync_excludes,
            "-e", self._ssh_cmd(),
            f"{local_path.rstrip('/')}/", remote_target,
        )
        if rc != 0:
            logger.error("deliver: rsync to %s failed: %s", host, self._redact(output))
        return rc == 0

    async def stage_s3(self, uri: str, host: str, dest: str) -> bool:
        """Legacy Manager-side S3 staging, protected by the same payload scan."""

        local = self._cache / "s3" / self.repo_name(uri.rstrip("/"))
        try:
            count = await asyncio.to_thread(_download_s3, uri, str(local))
        except Exception:
            logger.exception("stage_s3: download of %s failed", uri)
            return False
        logger.info("stage_s3: downloaded %d object(s) from %s", count, uri)
        return await self.deliver(str(local), host, dest)
