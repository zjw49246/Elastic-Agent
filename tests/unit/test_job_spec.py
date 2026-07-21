"""Tests for the declarative JobSpec model + template rendering."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from elastic_agent.core.job_spec import (
    AccountSpec,
    JobSpec,
    RunSpec,
    WorkerContext,
    render_template,
)


class TestRenderTemplate:
    def test_substitutes_known_vars(self):
        assert render_template("hi {{name}}", {"name": "bob"}) == "hi bob"

    def test_whitespace_tolerant(self):
        assert render_template("{{ shard_index }}", {"shard_index": 3}) == "3"

    def test_unknown_var_raises(self):
        with pytest.raises(KeyError):
            render_template("{{nope}}", {"name": "x"})

    def test_leaves_shell_syntax_untouched(self):
        # $(...) and $VAR must survive rendering — the worker's shell evaluates them.
        out = render_template("results/$(hostname -s)_{{shard_index}}", {"shard_index": 2})
        assert out == "results/$(hostname -s)_2"

    def test_multiple_occurrences(self):
        out = render_template("{{n}}-{{n}}", {"n": "a"})
        assert out == "a-a"


class TestWorkerContexts:
    def test_one_context_per_worker(self):
        spec = JobSpec(name="j", run=RunSpec(command="echo hi"), fanout={"workers": 4})
        ctxs = spec.worker_contexts()
        assert len(ctxs) == 4
        assert [c.shard_index for c in ctxs] == [0, 1, 2, 3]
        assert all(c.num_shards == 4 for c in ctxs)
        assert all(c.job_name == "j" for c in ctxs)

    def test_zero_workers_is_rejected_instead_of_silently_creating_one(self):
        with pytest.raises(ValidationError):
            JobSpec(
                name="j",
                run=RunSpec(command="echo hi"),
                fanout={"workers": 0},
            )

    @pytest.mark.parametrize(
        ("section", "value"),
        [
            ("fanout", {"workers": 101}),
            ("fanout", {"disk_gb": 2049}),
            ("account", {"per_worker": 33}),
            ("rotation", {"max_rotations": 101}),
            ("collect", {"interval_seconds": 86_401}),
        ],
    )
    def test_financial_resource_bounds_reject_oversized_job(
        self, section, value
    ):
        kwargs = {section: value}
        with pytest.raises(ValidationError):
            JobSpec(name="j", run=RunSpec(command="echo hi"), **kwargs)

    def test_run_timeout_has_a_bounded_maximum(self):
        with pytest.raises(ValidationError):
            RunSpec(command="echo hi", timeout=2_592_001)


class TestRenderCommand:
    def test_shell_mode_wraps_bash(self):
        spec = JobSpec(name="j", run=RunSpec(command="a && b", shell=True))
        assert spec.render_command(WorkerContext()) == ["bash", "-lc", "a && b"]

    def test_non_shell_mode_shlex_splits(self):
        spec = JobSpec(name="j", run=RunSpec(command="uv run bench --flag x", shell=False))
        assert spec.render_command(WorkerContext()) == ["uv", "run", "bench", "--flag", "x"]

    def test_shard_index_rendered(self):
        spec = JobSpec(name="j", run=RunSpec(command="bench --shard {{shard_index}}/{{num_shards}}"))
        ctx = WorkerContext(shard_index=2, num_shards=8)
        assert spec.render_command(ctx) == ["bash", "-lc", "bench --shard 2/8"]

    def test_ai4sci_style_command(self):
        # The real benchmark command shape: hostname-sharded output dir.
        spec = JobSpec(name="ai4sci", run=RunSpec(
            command='uv run ai4sci-bench run --output-dir "results/opus48_$(hostname -s)_seed128"',
        ))
        cmd = spec.render_command(spec.worker_contexts()[0])
        assert cmd[0:2] == ["bash", "-lc"]
        assert "$(hostname -s)" in cmd[2]


class TestRenderResumeCommand:
    def test_appends_resume_args(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench --out r"),
            rotation={"strategy": "on_exhaust_restart_resume", "resume_args": "--resume r"},
        )
        assert spec.render_resume_command(WorkerContext()) == ["bash", "-lc", "bench --out r --resume r"]

    def test_no_resume_args_equals_base(self):
        spec = JobSpec(name="j", run=RunSpec(command="bench"))
        assert spec.render_resume_command(WorkerContext()) == spec.render_command(WorkerContext())

    def test_resume_args_templated(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            rotation={"resume_args": "--resume out_{{shard_index}}"},
        )
        cmd = spec.render_resume_command(WorkerContext(shard_index=5))
        assert cmd == ["bash", "-lc", "bench --resume out_5"]


class TestRenderEnv:
    def test_passes_env_through_with_templating(self):
        spec = JobSpec(name="j", run=RunSpec(command="x", env={"SHARD": "{{shard_index}}", "K": "v"}))
        env = spec.render_env(WorkerContext(shard_index=7))
        assert env["SHARD"] == "7"
        assert env["K"] == "v"

    def test_injects_config_dir_when_set(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="x"),
            account={"config_dir": "/root/.claude-a"},
        )
        env = spec.render_env(WorkerContext())
        assert env["CLAUDE_CONFIG_DIR"] == "/root/.claude-a"

    def test_no_config_dir_leaves_default(self):
        spec = JobSpec(name="j", run=RunSpec(command="x"))
        assert "CLAUDE_CONFIG_DIR" not in spec.render_env(WorkerContext())

    def test_explicit_config_dir_must_be_absolute(self):
        with pytest.raises(ValidationError, match="must be empty or an absolute path"):
            AccountSpec(config_dir="relative-slot")

    def test_per_worker_config_dir_from_context_wins(self):
        spec = JobSpec(name="j", run=RunSpec(command="x"), account={"config_dir": "/root/.claude"})
        env = spec.render_env(WorkerContext(config_dir="/root/.claude-slot-1"))
        assert env["CLAUDE_CONFIG_DIR"] == "/root/.claude-slot-1"

    def test_eip_run_env_cannot_override_verified_account_directory(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="x"),
            account={
                "binding": "eip",
                "config_dir": "/root/.claude-selected",
            },
            fanout={"workers": 1},
        )

        env = spec.render_env(
            WorkerContext(config_dir="/root/.claude-verified")
        )

        assert env["CLAUDE_CONFIG_DIR"] == "/root/.claude-verified"

    @pytest.mark.parametrize("unsafe_name", ["CLAUDE_CONFIG_DIR", "HOME"])
    def test_eip_rejects_run_env_that_can_redirect_authenticated_home(
        self, unsafe_name
    ):
        with pytest.raises(ValidationError, match="does not allow run.env"):
            JobSpec(
                name="j",
                run=RunSpec(command="x", env={unsafe_name: "/tmp/wrong"}),
                account={"binding": "eip"},
                fanout={"workers": 1},
            )


class TestResolvedCwd:
    def test_default_is_repo_root(self):
        spec = JobSpec(name="j", run=RunSpec(command="x"), setup={"target_dir": "/opt/h"})
        assert spec.resolved_cwd() == "/opt/h"

    def test_relative_joined_under_repo(self):
        spec = JobSpec(name="j", run=RunSpec(command="x", cwd="sub/dir"), setup={"target_dir": "/opt/h"})
        assert spec.resolved_cwd() == "/opt/h/sub/dir"

    def test_absolute_used_as_is(self):
        spec = JobSpec(name="j", run=RunSpec(command="x", cwd="/tmp/work"), setup={"target_dir": "/opt/h"})
        assert spec.resolved_cwd() == "/tmp/work"


class TestCollectPaths:
    @pytest.mark.parametrize("path", ["", ".", "../secret", "out/../../secret", "/etc"])
    def test_rejects_paths_outside_job_collection_root(self, path):
        with pytest.raises(ValidationError, match="collect.paths"):
            JobSpec(
                name="unsafe",
                run=RunSpec(command="x"),
                collect={"paths": [path]},
            )

    def test_normalizes_relative_path_trailing_slash(self):
        spec = JobSpec(
            name="safe",
            run=RunSpec(command="x"),
            collect={"paths": ["results/nested/"]},
        )
        assert spec.collect.paths == ["results/nested"]


class TestJobSpecDefaults:
    def test_minimal_spec(self):
        spec = JobSpec(name="j", run=RunSpec(command="echo hi"))
        assert spec.fanout.workers == 1
        assert spec.fanout.shard_by == "hostname"
        assert spec.account.mode == "worker_local_login"
        assert spec.account.per_worker == 1
        assert spec.account.binding == "none"
        assert spec.account.ids == []
        assert spec.rotation.strategy == "none"
        assert spec.run.shell is True
        assert spec.harness_ref is None


class TestEipAccountBinding:
    def test_account_ids_are_trimmed_and_deduplicated_in_order(self):
        account = AccountSpec(ids=[" acct-2 ", "acct-1", "acct-2", "", "acct-1"])

        assert account.ids == ["acct-2", "acct-1"]

    def test_eip_binding_accepts_one_explicit_account_per_worker(self):
        spec = JobSpec(
            name="bound",
            run=RunSpec(command="bench"),
            account={"binding": "eip", "ids": ["acct-1", "acct-2"]},
            fanout={"workers": 2},
        )

        assert spec.account.binding == "eip"
        assert spec.account.ids == ["acct-1", "acct-2"]

    def test_eip_binding_allows_group_based_selection_when_ids_empty(self):
        spec = JobSpec(
            name="bound",
            run=RunSpec(command="bench"),
            account={"binding": "eip", "group": "codex"},
            fanout={"workers": 3},
        )

        assert spec.account.ids == []
        assert spec.account.group == "codex"

    def test_eip_binding_requires_one_account_per_worker(self):
        with pytest.raises(ValidationError, match="account.ids must contain exactly 2"):
            JobSpec(
                name="bound",
                run=RunSpec(command="bench"),
                account={"binding": "eip", "ids": ["acct-1", "acct-1"]},
                fanout={"workers": 2},
            )

    def test_eip_binding_requires_per_worker_one(self):
        with pytest.raises(ValidationError, match="account.per_worker must be 1"):
            JobSpec(
                name="bound",
                run=RunSpec(command="bench"),
                account={"binding": "eip", "per_worker": 2},
            )

    def test_eip_binding_rejects_same_machine_rotation(self):
        with pytest.raises(ValidationError, match="does not support on_exhaust_restart_resume"):
            JobSpec(
                name="bound",
                run=RunSpec(command="bench"),
                account={"binding": "eip"},
                rotation={"strategy": "on_exhaust_restart_resume"},
            )

    @pytest.mark.parametrize("mode", ["none", "manager_distribute"])
    def test_eip_binding_requires_verified_worker_local_login(self, mode):
        with pytest.raises(ValidationError, match="worker_local_login"):
            JobSpec(
                name="bound",
                run=RunSpec(command="bench"),
                account={"binding": "eip", "mode": mode},
            )

    def test_unbound_mode_keeps_multi_account_rotation_compatible(self):
        spec = JobSpec(
            name="legacy",
            run=RunSpec(command="bench"),
            account={"per_worker": 2},
            rotation={"strategy": "on_exhaust_restart_resume"},
        )

        assert spec.account.binding == "none"
        assert spec.account.per_worker == 2
        assert spec.rotation.strategy == "on_exhaust_restart_resume"
