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
    compile_job_setup_steps,
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
        assert len(steps) == 2
        assert steps[0].name == "harness-code"
        assert "git clone" in steps[0].command
        assert steps[1].name.endswith("legacy-commands")
        assert steps[1].command == "uv sync"
        assert steps[1].cwd == "/opt/elastic-agent/harness"

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

    def test_runtime_from_src_skips_pypi_runtime_deploy(self):
        spec = _spec()
        names = [s.name for s in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
            include_pty=True, runtime_from_src=True)]
        assert "runtime-deploy" not in names       # PyPI deploy skipped
        assert "pty-refresh-hook" not in names      # patches that unit → also skipped
        assert "agent-install" in names             # claude CLI still installed

    def test_docker_step_added_only_when_needs_docker(self):
        spec = _spec(setup={"needs_docker": True})
        steps = compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
            runtime_from_src=True, run_as="ubuntu")
        names = [s.name for s in steps]
        assert "docker-install" in names
        # before any runtime deploy, and adds the run user to the docker group
        dstep = next(s for s in steps if s.name == "docker-install")
        assert "usermod -aG docker ubuntu" in dstep.command
        # absent by default
        spec2 = _spec()
        names2 = [s.name for s in compile_bootstrap_steps(
            spec2, manager_url="u", auth_token="t", worker_id="w")]
        assert "docker-install" not in names2

    def test_docker_environment_profile_enables_common_docker_step(self):
        spec = _spec(environment={"profile": "ubuntu-agent-docker-v1"})
        names = [step.name for step in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
        )]
        assert "docker-install" in names

    def test_structured_setup_steps_run_as_job_user_with_own_policy(self):
        spec = _spec(setup={
            "steps": [{
                "name": "install deps",
                "command": "uv sync",
                "env": {"UV_LINK_MODE": "copy"},
                "cwd": "python",
                "timeout": 1234,
                "retries": 2,
            }],
        })
        step = compile_job_setup_steps(spec, run_as="ubuntu")[0]
        assert step.name.endswith("install-deps")
        assert "sudo -n -H -u ubuntu" in step.command
        assert "UV_LINK_MODE" in step.command
        assert "/opt/elastic-agent/harness/python" in step.command
        assert step.timeout == 1234
        assert step.retry_count == 2
        assert step.env == {}
        assert step.cwd is None

    def test_resolved_source_manifest_is_verified_before_setup(self):
        commit = "a" * 40
        spec = _spec(setup={
            "repo": "https://github.com/x/y.git",
            "ref": "release-v1",
            "resolved_commit": commit,
            "steps": [{"name": "install", "command": "uv sync"}],
        })
        steps = compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
            run_as="ubuntu",
        )
        clone = next(step for step in steps if step.name == "harness-code")
        assert "--branch release-v1" in clone.command
        assert "chown -R ubuntu:ubuntu" in clone.command
        source = next(step for step in steps if "source-manifest" in step.name)
        assert commit in source.command
        assert steps.index(source) < next(
            i for i, step in enumerate(steps)
            if step.name.startswith("job-setup-") and step.name.endswith("install")
        )

    def test_eip_binding_disables_ipv6_before_any_install_or_login(self):
        spec = _spec(account={"binding": "eip", "ids": ["acct-1"]})
        steps = compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
        )
        names = [step.name for step in steps]
        assert names[0] == "ipv4-only-egress"
        assert names.index("ipv4-only-egress") < names.index("system-init")
        command = steps[0].command
        assert "net.ipv6.conf.all.disable_ipv6 = 1" in command
        assert "/etc/sysctl.d/99-elastic-agent-eip-ipv4.conf" in command

        unbound = compile_bootstrap_steps(
            _spec(), manager_url="u", auth_token="t", worker_id="w",
        )
        assert "ipv4-only-egress" not in [step.name for step in unbound]

    def test_manager_rsync_skips_worker_clone(self):
        # Code is delivered by the Manager (rsync), so no worker git-clone step.
        spec = _spec(setup={"repo": "https://github.com/x/y.git", "commands": ["uv sync"],
                            "deliver": "manager_rsync"})
        names = [s.name for s in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w")]
        assert "harness-code" not in names

    def test_worker_clone_never_receives_manager_git_token(self, monkeypatch):
        monkeypatch.setenv("ELASTIC_AGENT_GIT_TOKEN", "ghp_manager_only_secret")
        spec = _spec(setup={
            "repo": "https://github.com/example/public.git",
            "deliver": "worker_clone",
        })

        steps = compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
        )
        command = next(
            step.command for step in steps if step.name == "harness-code"
        )

        assert "ghp_manager_only_secret" not in command
        assert "x-access-token" not in command
        assert "https://github.com/example/public.git" in command

    def test_repo_less_manager_rsync_still_runs_declared_setup(self):
        spec = _spec(setup={
            "deliver": "manager_rsync",
            "target_dir": "/opt/jobs/repo-less",
            "commands": ["mkdir -p results", "touch results/ready"],
        })
        names = [step.name for step in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
        )]

        assert "harness-code" in names
        assert any(name.startswith("job-setup-") for name in names)

    def test_no_pty_omits_pty_steps(self):
        spec = _spec()
        names = [s.name for s in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w", include_pty=False,
        )]
        assert "pty-install" not in names
        assert "pty-refresh-hook" not in names

    def test_codex_installs_pinned_cli_and_playwright_login_deps(self):
        spec = _spec(account={"agent_type": "codex"})
        steps = compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
        )

        install = next(step for step in steps if step.name == "agent-install")
        login_deps = next(
            step for step in steps if step.name == "credential-login-deps"
        )
        assert "@openai/codex@0.144.6" in install.command
        assert "codex --version" in install.command
        assert "@anthropic-ai/claude-code" not in install.command
        assert "playwright" in login_deps.command
        assert "playwright-stealth" not in login_deps.command
        assert "google-chrome" in login_deps.command

    def test_codex_safely_skips_claude_only_pty_steps(self):
        spec = _spec(account={"agent_type": "codex"})
        names = [step.name for step in compile_bootstrap_steps(
            spec, manager_url="u", auth_token="t", worker_id="w",
            include_pty=True,
        )]

        assert "pty-install" not in names
        assert "pty-refresh-hook" not in names
        assert "claude-cli-health-hook" not in names

    def test_claude_bootstrap_remains_the_default(self):
        steps = compile_bootstrap_steps(
            _spec(), manager_url="u", auth_token="t", worker_id="w",
        )

        install = next(step for step in steps if step.name == "agent-install")
        login_deps = next(
            step for step in steps if step.name == "credential-login-deps"
        )
        assert "@anthropic-ai/claude-code@2.1.181" in install.command
        assert "@openai/codex" not in install.command
        assert "playwright" not in login_deps.command


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

    def test_codex_single_slot_leaves_home_for_runtime_user_to_resolve(self):
        spec = _spec(account={"agent_type": "codex", "group": "codex"})

        assert GenericJobHarness(spec).get_credential_slots() == [
            {"slot_type": "codex", "config_dir": ""}
        ]

    def test_codex_multi_slot_uses_explicit_worker_writable_prefix(self):
        spec = _spec(account={
            "agent_type": "codex",
            "per_worker": 3,
            "config_dir": "/home/ubuntu/.codex",
        })
        slots = GenericJobHarness(spec).get_credential_slots()

        assert [slot["config_dir"] for slot in slots] == [
            "/home/ubuntu/.codex-slot-0",
            "/home/ubuntu/.codex-slot-1",
            "/home/ubuntu/.codex-slot-2",
        ]


class TestScalingSignal:
    def test_desired_workers_from_fanout(self):
        spec = _spec(fanout={"workers": 8})
        sig = GenericJobHarness(spec).get_scaling_signal()
        assert sig.desired_workers == 8
        assert "j" in sig.reason


class TestBuildExecute:
    def test_execute_kwargs(self):
        spec = _spec(
            run=RunSpec(command="bench --out {{shard_index}}", cwd="repo", timeout=0),
            setup={"target_dir": "/opt/h"},
        )
        ex = build_execute(spec, WorkerContext(shard_index=2))
        assert ex["command"] == ["bash", "-lc", "bench --out 2"]
        assert ex["cwd"] == "/opt/h/repo"   # relative cwd joined under the repo root
        assert ex["timeout"] == 86_400  # legacy 0 → finite safety default

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

    def test_same_stem_in_distinct_content_paths_has_distinct_module_cache(
        self, tmp_path,
    ):
        source = (
            "from elastic_agent.harness.base import Harness\n"
            "class Versioned(Harness):\n"
            "    marker = {marker!r}\n"
        )
        first_dir = tmp_path / "digest-a"
        second_dir = tmp_path / "digest-b"
        first_dir.mkdir()
        second_dir.mkdir()
        first = first_dir / "plugin.py"
        second = second_dir / "plugin.py"
        first.write_text(source.format(marker="first"))
        second.write_text(source.format(marker="second"))

        first_cls = load_harness_class(f"{first}:Versioned")
        second_cls = load_harness_class(f"{second}:Versioned")

        assert first_cls.marker == "first"
        assert second_cls.marker == "second"
        assert first_cls.__module__ != second_cls.__module__
