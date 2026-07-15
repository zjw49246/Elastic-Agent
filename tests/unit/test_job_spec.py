"""Tests for the declarative JobSpec model + template rendering."""

from __future__ import annotations

import pytest

from elastic_agent.core.job_spec import (
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

    def test_minimum_one_worker(self):
        spec = JobSpec(name="j", run=RunSpec(command="echo hi"), fanout={"workers": 0})
        assert len(spec.worker_contexts()) == 1


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

    def test_per_worker_config_dir_from_context_wins(self):
        spec = JobSpec(name="j", run=RunSpec(command="x"), account={"config_dir": "/root/.claude"})
        env = spec.render_env(WorkerContext(config_dir="/root/.claude-slot-1"))
        assert env["CLAUDE_CONFIG_DIR"] == "/root/.claude-slot-1"


class TestJobSpecDefaults:
    def test_minimal_spec(self):
        spec = JobSpec(name="j", run=RunSpec(command="echo hi"))
        assert spec.fanout.workers == 1
        assert spec.fanout.shard_by == "hostname"
        assert spec.account.mode == "worker_local_login"
        assert spec.account.per_worker == 1
        assert spec.rotation.strategy == "none"
        assert spec.run.shell is True
        assert spec.harness_ref is None
