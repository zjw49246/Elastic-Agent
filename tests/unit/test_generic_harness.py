"""Tests for GenericJobHarness + harness resolution (declarative vs uploaded code)."""

from __future__ import annotations

import textwrap

import pytest

from elastic_agent.core.job_spec import JobSpec, RunSpec, WorkerContext
from elastic_agent.harness.base import Harness
from elastic_agent.harness.generic import (
    GenericJobHarness,
    build_execute,
    compile_bootstrap_steps,
    load_harness_class,
    resolve_harness,
)


def _spec(**kw):
    kw.setdefault("name", "j")
    kw.setdefault("run", RunSpec(command="uv run bench"))
    return JobSpec(**kw)


class TestBootstrapSteps:
    def test_harness_step_carries_repo_and_commands(self):
        spec = _spec(setup={"repo": "https://x/y.git", "commands": ["uv sync"]})
        steps = GenericJobHarness(spec).get_bootstrap_steps()
        assert len(steps) == 1
        assert steps[0].name == "harness-code"
        assert "git clone" in steps[0].command
        assert "uv sync" in steps[0].command

    def test_compile_full_sequence_order(self):
        spec = _spec(setup={"repo": "https://x/y.git", "commands": ["uv sync"]})
        names = [s.name for s in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w", include_pty=True,
        )]
        # harness-code must land after runtime-deploy; refresh/health after it.
        assert names.index("harness-code") > names.index("runtime-deploy")
        assert names.index("pty-refresh-hook") > names.index("harness-code")
        assert "credential-login-deps" in names  # auto-enabled for worker_local_login

    def test_login_deps_skipped_when_account_none(self):
        spec = _spec(account={"mode": "none"})
        names = [s.name for s in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
        )]
        assert "credential-login-deps" not in names

    def test_no_pty_omits_pty_steps(self):
        spec = _spec()
        names = [s.name for s in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w", include_pty=False,
        )]
        assert "pty-install" not in names
        assert "pty-refresh-hook" not in names


class TestCredentialSlots:
    def test_single_slot_uses_configured_dir(self):
        spec = _spec(account={"per_worker": 1, "config_dir": "/root/.claude", "group": "prod"})
        slots = GenericJobHarness(spec).get_credential_slots()
        assert slots == [{"slot_type": "prod", "config_dir": "/root/.claude"}]

    def test_multi_slot_distinct_dirs(self):
        spec = _spec(account={"per_worker": 3, "config_dir": "/root/.claude"})
        slots = GenericJobHarness(spec).get_credential_slots()
        assert len(slots) == 3
        dirs = [s["config_dir"] for s in slots]
        assert len(set(dirs)) == 3
        assert dirs == ["/root/.claude-slot-0", "/root/.claude-slot-1", "/root/.claude-slot-2"]

    def test_account_none_yields_no_slots(self):
        spec = _spec(account={"mode": "none"})
        assert GenericJobHarness(spec).get_credential_slots() == []


class TestScalingSignal:
    def test_desired_workers_from_fanout(self):
        spec = _spec(fanout={"workers": 8})
        sig = GenericJobHarness(spec).get_scaling_signal()
        assert sig.desired_workers == 8
        assert "j" in sig.reason


class TestBuildExecute:
    def test_execute_kwargs(self):
        spec = _spec(run=RunSpec(command="bench --out {{shard_index}}", cwd="repo", timeout=0))
        ex = build_execute(spec, WorkerContext(shard_index=2))
        assert ex["command"] == ["bash", "-lc", "bench --out 2"]
        assert ex["cwd"] == "repo"
        assert ex["timeout"] is None  # 0 → no wall-clock limit

    def test_resume_appends_args(self):
        spec = _spec(
            run=RunSpec(command="bench"),
            rotation={"resume_args": "--resume r"},
        )
        ex = build_execute(spec, WorkerContext(), resume=True)
        assert ex["command"] == ["bash", "-lc", "bench --resume r"]


class TestResolveHarness:
    def test_declarative_returns_generic(self):
        assert isinstance(resolve_harness(_spec()), GenericJobHarness)

    def test_uploaded_file_ref(self, tmp_path):
        code = textwrap.dedent('''
            from elastic_agent.harness.base import Harness, BootstrapStep

            class MyHarness(Harness):
                def __init__(self, spec=None):
                    self.spec = spec
                def get_bootstrap_steps(self):
                    return [BootstrapStep(name="custom", command="echo custom")]
        ''')
        f = tmp_path / "myharness.py"
        f.write_text(code)
        spec = _spec(harness_ref=f"{f}:MyHarness")
        h = resolve_harness(spec)
        assert isinstance(h, Harness)
        assert h.get_bootstrap_steps()[0].name == "custom"
        assert h.spec is spec  # spec injected into constructor

    def test_noarg_harness_ref(self, tmp_path):
        code = textwrap.dedent('''
            from elastic_agent.harness.base import Harness, BootstrapStep

            class NoArgHarness(Harness):
                def get_bootstrap_steps(self):
                    return [BootstrapStep(name="na", command="echo na")]
        ''')
        f = tmp_path / "noarg.py"
        f.write_text(code)
        h = resolve_harness(_spec(harness_ref=f"{f}:NoArgHarness"))
        assert h.get_bootstrap_steps()[0].name == "na"

    def test_bad_ref_no_colon(self):
        with pytest.raises(ValueError):
            load_harness_class("no_colon_here")

    def test_ref_not_a_harness(self, tmp_path):
        f = tmp_path / "plain.py"
        f.write_text("class NotHarness:\n    pass\n")
        with pytest.raises(TypeError):
            load_harness_class(f"{f}:NotHarness")

    def test_missing_file(self):
        with pytest.raises(FileNotFoundError):
            load_harness_class("/nonexistent/harness.py:X")
