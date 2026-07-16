"""Tests for ManagerCodeSync — token-safe private-repo delivery."""

from __future__ import annotations

import pytest

from elastic_agent.core.code_sync import ManagerCodeSync

pytestmark = pytest.mark.asyncio


class FakeRunner:
    def __init__(self, rc=0):
        self.calls = []
        self._rc = rc

    async def __call__(self, *cmd):
        self.calls.append(cmd)
        return self._rc, ""

    def joined(self):
        return [" ".join(c) for c in self.calls]


async def test_clone_uses_token_then_scrubs_it(tmp_path):
    r = FakeRunner()
    sync = ManagerCodeSync(str(tmp_path / "cache"), git_token="ghp_secret",
                           ssh_key="/k.pem", runner=r)
    path = await sync.ensure_clone("https://github.com/org/private.git", "main")
    joined = r.joined()
    # clone URL carries the token (Manager-side only)
    assert any("x-access-token:ghp_secret@github.com/org/private.git" in c for c in joined)
    # and the persisted remote is scrubbed back to the tokenless URL
    assert any("remote set-url origin https://github.com/org/private.git" in c for c in joined)
    assert path.endswith("/private")


async def test_deliver_excludes_git_and_carries_no_token(tmp_path):
    r = FakeRunner()
    sync = ManagerCodeSync(str(tmp_path), git_token="ghp_secret", ssh_key="/k.pem",
                           ssh_user="ubuntu", runner=r)
    ok = await sync.deliver("/cache/private", "1.2.3.4", "/opt/harness")
    assert ok is True
    joined = r.joined()
    rsync = [c for c in joined if c.startswith("rsync")][0]
    assert "--exclude .git" in rsync
    assert "ubuntu@1.2.3.4:/opt/harness/" in rsync
    # the token must never appear in anything sent to the worker
    assert all("ghp_secret" not in c for c in joined)
    # target dir is prepped writable first
    assert any("mkdir -p /opt/harness" in c and "chown" in c for c in joined)


async def test_no_token_public_repo(tmp_path):
    r = FakeRunner()
    sync = ManagerCodeSync(str(tmp_path), runner=r)
    await sync.ensure_clone("https://github.com/org/pub.git", "main")
    assert all("x-access-token" not in c for c in r.joined())


async def test_deliver_rsync_failure_returns_false(tmp_path):
    r = FakeRunner(rc=1)
    sync = ManagerCodeSync(str(tmp_path), ssh_key="/k.pem", runner=r)
    assert await sync.deliver("/l", "h", "/t") is False


def test_repo_name():
    assert ManagerCodeSync.repo_name("https://github.com/a/b.git") == "b"
    assert ManagerCodeSync.repo_name("https://github.com/a/b/") == "b"
