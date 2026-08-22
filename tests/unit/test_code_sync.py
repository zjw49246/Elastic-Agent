"""Tests for ManagerCodeSync — token-safe private-repo delivery."""

from __future__ import annotations

import asyncio
import subprocess
from pathlib import Path

import pytest

import elastic_agent.core.code_sync as cs
from elastic_agent.core.code_sync import (
    ManagerCodeSync,
    UnsafeCodePayloadError,
)

pytestmark = pytest.mark.asyncio


class FakeRunner:
    commit = "a" * 40

    def __init__(self, rc=0, output=""):
        self.calls = []
        self._rc = rc
        self._output = output

    async def __call__(self, *cmd):
        self.calls.append(cmd)
        if "rev-parse" in cmd and self._rc == 0:
            return 0, self.commit + "\n"
        return self._rc, self._output

    def joined(self):
        return [" ".join(c) for c in self.calls]


async def test_clone_uses_token_then_scrubs_it(tmp_path):
    r = FakeRunner()
    sync = ManagerCodeSync(str(tmp_path / "cache"), git_token="ghp_secret",
                           ssh_key="/k.pem", runner=r)
    path = await sync.ensure_clone("https://github.com/org/private.git", "main")
    joined = r.joined()
    # Fetch carries the token only in the Manager-side process.
    assert any("x-access-token:ghp_secret@github.com/org/private.git" in c for c in joined)
    # The persisted remote is created tokenless; credential-bearing FETCH_HEAD
    # is disabled as well.
    assert any("remote add origin https://github.com/org/private.git" in c for c in joined)
    assert any("--no-write-fetch-head" in c for c in joined)
    assert path.endswith("/" + FakeRunner.commit)
    assert "checkouts" in path


async def test_resolved_commit_is_the_fetch_target(tmp_path):
    r = FakeRunner()
    expected = "b" * 40
    r.commit = expected
    sync = ManagerCodeSync(str(tmp_path / "cache"), runner=r)

    path = await sync.ensure_clone(
        "https://github.com/org/private.git",
        "main",
        resolved_commit=expected,
    )

    assert path.endswith("/" + expected)
    assert any(
        f"{expected}:refs/elastic-agent/resolved" in command
        for command in r.joined()
    )


async def test_deliver_excludes_git_and_carries_no_token(tmp_path):
    r = FakeRunner()
    sync = ManagerCodeSync(str(tmp_path), git_token="ghp_secret", ssh_key="/k.pem",
                           ssh_user="ubuntu", runner=r)
    payload = tmp_path / "private"
    payload.mkdir()
    ok = await sync.deliver(str(payload), "1.2.3.4", "/opt/harness")
    assert ok is True
    joined = r.joined()
    rsync = [c for c in joined if c.startswith("rsync")][0]
    assert "--exclude .git" in rsync
    assert "--exclude .env*" in rsync
    assert "--exclude *.pem" in rsync
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
    payload = tmp_path / "payload"
    payload.mkdir()
    assert await sync.deliver(str(payload), "h", "/t") is False


async def test_repo_name():
    assert ManagerCodeSync.repo_name("https://github.com/a/b.git") == "b"
    assert ManagerCodeSync.repo_name("https://github.com/a/b/") == "b"


async def test_stage_s3_downloads_on_manager_then_rsyncs_to_worker(tmp_path, monkeypatch):
    """S3 dataset staging: Manager downloads (boto3), then rsyncs to the worker
    (workers get no S3 creds). The rsync target is the requested dest."""
    seen = {}
    def fake_dl(uri, dest):
        seen["uri"], seen["dest"] = uri, dest
        from pathlib import Path
        Path(dest).mkdir(parents=True)
        return 5
    monkeypatch.setattr(cs, "_download_s3", fake_dl)
    r = FakeRunner()
    sync = ManagerCodeSync(str(tmp_path), ssh_key="/k.pem", ssh_user="ubuntu", runner=r)

    ok = await sync.stage_s3("s3://bkt/data/", "1.2.3.4", "/home/ubuntu/data")

    assert ok is True
    assert seen["uri"] == "s3://bkt/data/"
    joined = r.joined()
    assert any("rsync" in c and "ubuntu@1.2.3.4:/home/ubuntu/data/" in c for c in joined)


async def test_stage_s3_download_failure_returns_false(tmp_path, monkeypatch):
    import elastic_agent.core.code_sync as cs
    def boom(uri, dest):
        raise RuntimeError("no creds")
    monkeypatch.setattr(cs, "_download_s3", boom)
    sync = ManagerCodeSync(str(tmp_path), ssh_key="/k.pem", runner=FakeRunner())
    assert await sync.stage_s3("s3://b/x", "h", "/d") is False


@pytest.mark.parametrize(
    "relative",
    [
        ".env.tokenrouter",
        ".envrc",
        "nested/auth.json",
        "config/application_default_credentials.json",
        "deploy/private.pem",
        ".aws/credentials",
        "service-credentials.yaml",
        "config/secrets.yml",
    ],
)
async def test_deliver_refuses_likely_secret_files(tmp_path, relative):
    payload = tmp_path / "payload"
    target = payload / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.touch()
    runner = FakeRunner()
    sync = ManagerCodeSync(str(tmp_path / "cache"), runner=runner)

    with pytest.raises(UnsafeCodePayloadError, match="prohibited"):
        await sync.deliver(str(payload), "worker.invalid", "/opt/job")

    assert runner.calls == []


async def test_clone_cache_identity_includes_full_repository_url(tmp_path):
    first_runner = FakeRunner()
    second_runner = FakeRunner()
    first = ManagerCodeSync(str(tmp_path / "cache"), runner=first_runner)
    second = ManagerCodeSync(str(tmp_path / "cache"), runner=second_runner)

    path_a = await first.ensure_clone("https://example.invalid/team-a/job.git")
    path_b = await second.ensure_clone("https://example.invalid/team-b/job.git")

    assert path_a != path_b
    assert path_a.endswith(FakeRunner.commit)
    assert path_b.endswith(FakeRunner.commit)


async def test_git_failure_is_checked_and_token_is_redacted(tmp_path):
    runner = FakeRunner(rc=1, output="request used ghp_do_not_print")
    sync = ManagerCodeSync(
        str(tmp_path / "cache"),
        git_token="ghp_do_not_print",
        runner=runner,
    )

    with pytest.raises(RuntimeError) as raised:
        await sync.ensure_clone("https://github.com/org/private.git")

    assert "ghp_do_not_print" not in str(raised.value)
    assert "[REDACTED]" in str(raised.value)


async def test_repeated_commit_reuses_content_addressed_checkout(tmp_path):
    runner = FakeRunner()
    sync = ManagerCodeSync(str(tmp_path / "cache"), runner=runner)

    first = await sync.ensure_clone("https://example.invalid/org/repo.git")
    second = await sync.ensure_clone("https://example.invalid/org/repo.git")

    assert first == second
    checkout_clones = [
        call for call in runner.calls
        if len(call) > 1 and call[0:2] == ("git", "clone")
    ]
    assert len(checkout_clones) == 1


async def test_real_git_concurrent_clone_produces_one_verified_checkout(tmp_path):
    source = tmp_path / "source"
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(source)],
        check=True,
    )
    subprocess.run(
        [
            "git", "-C", str(source),
            "-c", "user.name=Elastic Agent Test",
            "-c", "user.email=test@example.invalid",
            "commit", "--quiet", "--allow-empty", "-m", "initial",
        ],
        check=True,
    )
    expected = subprocess.run(
        ["git", "-C", str(source), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    sync = ManagerCodeSync(str(tmp_path / "cache"))

    paths = await asyncio.gather(
        *(sync.ensure_clone(str(source), "main") for _ in range(4))
    )

    assert len(set(paths)) == 1
    assert paths[0].endswith(expected)
    checkout_head = subprocess.run(
        ["git", "-C", paths[0], "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert checkout_head == expected


async def test_content_addressed_checkout_rejects_local_mutation(tmp_path):
    source = tmp_path / "source"
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(source)],
        check=True,
    )
    subprocess.run(
        [
            "git", "-C", str(source),
            "-c", "user.name=Elastic Agent Test",
            "-c", "user.email=test@example.invalid",
            "commit", "--quiet", "--allow-empty", "-m", "initial",
        ],
        check=True,
    )
    sync = ManagerCodeSync(str(tmp_path / "cache"))
    checkout = await sync.ensure_clone(str(source), "main")
    (Path(checkout) / "unexpected.txt").touch()

    with pytest.raises(RuntimeError, match="modified or untracked"):
        await sync.ensure_clone(str(source), "main")


async def test_resolved_commit_survives_mutable_branch_advance(tmp_path):
    source = tmp_path / "source"
    subprocess.run(
        ["git", "init", "--quiet", "--initial-branch=main", str(source)],
        check=True,
    )
    for message in ("first", "second"):
        subprocess.run(
            [
                "git", "-C", str(source),
                "-c", "user.name=Elastic Agent Test",
                "-c", "user.email=test@example.invalid",
                "commit", "--quiet", "--allow-empty", "-m", message,
            ],
            check=True,
        )
        if message == "first":
            first = subprocess.run(
                ["git", "-C", str(source), "rev-parse", "HEAD"],
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()

    sync = ManagerCodeSync(str(tmp_path / "cache"))
    checkout = await sync.ensure_clone(
        str(source),
        "main",
        resolved_commit=first,
    )

    actual = subprocess.run(
        ["git", "-C", checkout, "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert actual == first


@pytest.mark.parametrize(
    "key",
    ["data/../../secret", "data/../secret", "data/bad\\name", "data//empty"],
)
async def test_s3_stage_rejects_keys_that_escape_or_are_ambiguous(key):
    with pytest.raises(ValueError, match="unsafe S3 object key"):
        cs._safe_s3_relative_path(key, "data/")
