"""ManagerCodeSync — clone a (private) repo on the Manager, rsync it to workers.

The token-safe way to deliver private code: the GitHub token lives ONLY on the
Manager and is used only for the local clone. The checkout is then rsynced to
each worker **excluding ``.git``**, so no token (and no git remote) ever reaches
a worker — preventing credential sprawl across the fleet.

    sync = ManagerCodeSync(cache_dir, git_token=TOKEN, ssh_key=KEY, ssh_user="ubuntu")
    local = await sync.ensure_clone(repo, branch)     # on the Manager (uses token)
    await sync.deliver(local, host, target_dir)       # rsync to worker (no token)
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


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
        self._cache = Path(cache_dir)
        self._token = git_token
        self._ssh_key = ssh_key
        self._ssh_user = ssh_user
        self._run = runner or self._real_run

    async def _real_run(self, *cmd: str) -> tuple[int, str]:
        proc = await asyncio.create_subprocess_exec(
            *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT
        )
        out, _ = await proc.communicate()
        return proc.returncode or 0, out.decode(errors="replace")

    @staticmethod
    def repo_name(repo: str) -> str:
        name = repo.rstrip("/").split("/")[-1]
        return name[:-4] if name.endswith(".git") else name

    def _auth_url(self, repo: str) -> str:
        # Token embedded ONLY here (Manager-side clone). Never rsynced out.
        if self._token and repo.startswith("https://github.com/"):
            return repo.replace(
                "https://github.com/", f"https://x-access-token:{self._token}@github.com/"
            )
        return repo

    def _ssh_cmd(self) -> str:
        parts = ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null"]
        if self._ssh_key:
            parts += ["-i", self._ssh_key]
        return " ".join(parts)

    async def ensure_clone(self, repo: str, branch: str = "main") -> str:
        """Clone (or fast-update) the repo into the Manager's cache. Returns the
        local path. Uses the token for private repos — Manager-side only."""
        local = self._cache / self.repo_name(repo)
        self._cache.mkdir(parents=True, exist_ok=True)
        url = self._auth_url(repo)
        if (local / ".git").is_dir():
            await self._run("git", "-C", str(local), "remote", "set-url", "origin", url)
            await self._run("git", "-C", str(local), "fetch", "--depth", "1", "origin", branch)
            await self._run("git", "-C", str(local), "reset", "--hard", f"origin/{branch}")
        else:
            rc, out = await self._run(
                "git", "clone", "--depth", "1", "--branch", branch, url, str(local)
            )
            if rc != 0:
                raise RuntimeError(f"manager clone failed: {out[-300:]}")
        # Scrub the token from the persisted remote even on the Manager cache.
        await self._run("git", "-C", str(local), "remote", "set-url", "origin", repo)
        return str(local)

    async def deliver(self, local_path: str, host: str, target_dir: str) -> bool:
        """rsync the checkout to worker:target_dir, excluding .git (no token, no
        remote reaches the worker). Ensures the dir exists + is user-writable."""
        prep = (
            f"sudo mkdir -p {target_dir} && sudo chown -R {self._ssh_user} {target_dir}"
        )
        rc, _ = await self._run(
            *self._ssh_cmd().split(), f"{self._ssh_user}@{host}", prep
        )
        if rc != 0:
            logger.warning("deliver: could not prep %s on %s", target_dir, host)
        rc, out = await self._run(
            "rsync", "-az", "--delete", "--exclude", ".git",
            "-e", self._ssh_cmd(),
            f"{local_path.rstrip('/')}/", f"{self._ssh_user}@{host}:{target_dir}/",
        )
        if rc != 0:
            logger.error("deliver: rsync to %s failed: %s", host, out[-300:])
        return rc == 0
