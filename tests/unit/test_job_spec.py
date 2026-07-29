"""Tests for the declarative JobSpec model + template rendering."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from elastic_agent.core.job_spec import (
    DEFAULT_JOB_TTL_SECONDS,
    DEFAULT_RUN_TIMEOUT_SECONDS,
    AccountSpec,
    EnvironmentSpec,
    JobSpec,
    RunSpec,
    S3Dataset,
    SetupSpec,
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
        assert [c.as_dict()["shard_id"] for c in ctxs] == [
            "00000", "00001", "00002", "00003",
        ]
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

    def test_legacy_unlimited_timeout_is_normalized_to_safe_default(self):
        assert RunSpec(command="echo hi", timeout=0).timeout == DEFAULT_RUN_TIMEOUT_SECONDS
        assert RunSpec(command="echo hi", timeout=None).timeout == DEFAULT_RUN_TIMEOUT_SECONDS

    def test_job_ttl_must_cover_run_and_is_bounded(self):
        with pytest.raises(ValidationError, match="greater than or equal"):
            JobSpec(
                name="j",
                run={"command": "x", "timeout": 7200},
                ttl_seconds=3600,
            )
        with pytest.raises(ValidationError):
            JobSpec(
                name="j",
                run={"command": "x"},
                ttl_seconds=2_592_001,
            )


class TestCheckpointRecovery:
    def test_checkpoint_collection_requires_paths(self):
        with pytest.raises(
            ValidationError,
            match="checkpoint collection requires collect.paths",
        ):
            JobSpec(
                name="checkpoint",
                run=RunSpec(command="bench"),
                collect={"checkpoint": True},
            )

    @pytest.mark.parametrize(
        "paths",
        [
            ["results", "results"],
            ["results", "results/nested"],
            ["results", "results-old", "results/nested"],
        ],
    )
    def test_checkpoint_collection_rejects_duplicate_or_overlapping_paths(
        self, paths,
    ):
        with pytest.raises(ValidationError, match="must (?:be unique|not overlap)"):
            JobSpec(
                name="checkpoint",
                run=RunSpec(command="bench"),
                collect={"paths": paths, "checkpoint": True},
            )

    def test_checkpoint_recovery_requires_source_and_paths(self):
        with pytest.raises(
            ValidationError,
            match="recovery.source_job_id is required",
        ):
            JobSpec(
                name="resume",
                run=RunSpec(command="bench --resume"),
                recovery={"policy": "checkpoint", "paths": ["results"]},
            )
        with pytest.raises(
            ValidationError,
            match="recovery.paths is required",
        ):
            JobSpec(
                name="resume",
                run=RunSpec(command="bench --resume"),
                recovery={
                    "policy": "checkpoint",
                    "source_job_id": "job-0123456789abcdef",
                },
            )

    @pytest.mark.parametrize(
        "source_job_id",
        ["../job-secret", "/tmp/job", "job x", "x" * 129],
    )
    def test_checkpoint_recovery_rejects_unsafe_source_job_id(
        self, source_job_id
    ):
        with pytest.raises(ValidationError, match="source_job_id"):
            JobSpec(
                name="resume",
                run=RunSpec(command="bench --resume"),
                recovery={
                    "policy": "checkpoint",
                    "source_job_id": source_job_id,
                    "paths": ["results"],
                },
            )

    def test_recovery_none_rejects_hidden_source(self):
        with pytest.raises(
            ValidationError,
            match="must be empty when recovery.policy is 'none'",
        ):
            JobSpec(
                name="resume",
                run=RunSpec(command="bench"),
                recovery={
                    "source_job_id": "job-0123456789abcdef",
                    "paths": ["results"],
                },
            )

    def test_recovery_rejects_overlapping_paths(self):
        for paths in (
            ["results", "results/nested"],
            ["results", "results-old", "results/nested"],
        ):
            with pytest.raises(ValidationError, match="must not overlap"):
                JobSpec(
                    name="resume",
                    run=RunSpec(command="bench --resume"),
                    recovery={
                        "policy": "checkpoint",
                        "source_job_id": "job-0123456789abcdef",
                        "paths": paths,
                    },
                )

    def test_checkpoint_and_legacy_recovery_are_explicit(self):
        checkpoint = JobSpec(
            name="resume",
            run=RunSpec(command="bench --resume"),
            fanout={"shard_by": "shard_index"},
            collect={
                "paths": ["results"],
                "checkpoint": True,
                "exclude": ["**/core", "*.tmp"],
            },
            recovery={
                "policy": "checkpoint",
                "source_job_id": "job-0123456789abcdef",
                "paths": ["results"],
            },
        )
        legacy = JobSpec(
            name="resume-legacy",
            run=RunSpec(command="bench --resume"),
            recovery={
                "policy": "legacy_final_collection",
                "source_job_id": "job-fedcba9876543210",
                "paths": ["results"],
            },
        )

        assert checkpoint.collect.checkpoint is True
        assert checkpoint.collect.checkpoint_keep_generations == 3
        assert checkpoint.collect.exclude == ["**/core", "*.tmp"]
        assert checkpoint.recovery.policy == "checkpoint"
        assert legacy.recovery.policy == "legacy_final_collection"

    @pytest.mark.parametrize(
        "pattern",
        ["/absolute", "../escape", "results/../../escape", "-danger", "bad\\path"],
    )
    def test_collect_exclude_patterns_are_safe(self, pattern):
        with pytest.raises(ValidationError, match="collect.exclude"):
            JobSpec(
                name="checkpoint",
                run=RunSpec(command="bench"),
                collect={
                    "paths": ["results"],
                    "exclude": [pattern],
                },
            )

    def test_checkpoint_list_and_retention_limits_are_bounded(self):
        with pytest.raises(ValidationError):
            JobSpec(
                name="checkpoint",
                run=RunSpec(command="bench"),
                collect={"paths": [f"p{i}" for i in range(33)]},
            )
        with pytest.raises(ValidationError):
            JobSpec(
                name="checkpoint",
                run=RunSpec(command="bench"),
                collect={
                    "paths": ["results"],
                    "checkpoint": True,
                    "checkpoint_keep_generations": 101,
                },
            )

    def test_checkpoint_retention_leaves_inventory_room_for_next_generation(
        self,
    ):
        valid = JobSpec(
            name="checkpoint-boundary",
            run=RunSpec(command="bench"),
            fanout={"workers": 100, "shard_by": "shard_index"},
            collect={
                "paths": ["results"],
                "checkpoint": True,
                "checkpoint_keep_generations": 91,
            },
        )
        assert valid.collect.checkpoint_keep_generations == 91

        with pytest.raises(
            ValidationError, match="10000-manifest checkpoint retention budget",
        ):
            JobSpec(
                name="checkpoint-overflow",
                run=RunSpec(command="bench"),
                fanout={"workers": 100, "shard_by": "shard_index"},
                collect={
                    "paths": ["results"],
                    "checkpoint": True,
                    "checkpoint_keep_generations": 92,
                },
            )

    def test_checkpoint_requires_stable_shard_index(self):
        with pytest.raises(
            ValidationError,
            match="fanout.shard_by='shard_index'",
        ):
            JobSpec(
                name="checkpoint",
                run=RunSpec(command="bench"),
                collect={
                    "paths": ["results"],
                    "checkpoint": True,
                },
            )

    @pytest.mark.parametrize(
        ("command", "resume_args"),
        [
            ('bench --output "results/$(hostname -s)"', ""),
            ('bench --output "results/$HOSTNAME"', ""),
            ('bench --output "results/${HOSTNAME}"', ""),
            ('bench --output "results/{{hostname}}"', ""),
            ("bench", '--resume "results/$(hostname -s)"'),
        ],
    )
    def test_checkpoint_rejects_hostname_derived_recovery_paths(
        self, command, resume_args,
    ):
        with pytest.raises(
            ValidationError,
            match="replacement Workers have different hostnames",
        ):
            JobSpec(
                name="checkpoint-hostname",
                run=RunSpec(command=command),
                rotation={"resume_args": resume_args},
                fanout={"shard_by": "shard_index"},
                collect={"paths": ["results"], "checkpoint": True},
            )

    def test_checkpoint_accepts_stable_shard_id_paths(self):
        spec = JobSpec(
            name="checkpoint-shard",
            run=RunSpec(
                command='bench --output "results/shard-{{shard_id}}"',
            ),
            rotation={
                "resume_args": '--resume "results/shard-{{shard_id}}"',
            },
            fanout={"workers": 2, "shard_by": "shard_index"},
            collect={"paths": ["results"], "checkpoint": True},
        )

        first, second = spec.worker_contexts()
        assert "shard-00000" in spec.render_command(first)[2]
        assert "shard-00001" in spec.render_command(second)[2]

    def test_non_checkpoint_collection_paths_must_also_be_disjoint(self):
        with pytest.raises(ValidationError, match="must not overlap"):
            JobSpec(
                name="ordinary-overlap",
                run=RunSpec(command="bench"),
                collect={"paths": ["results", "results/nested"]},
            )

        spec = JobSpec(
            name="checkpoint",
            run=RunSpec(command="bench"),
            fanout={"shard_by": "shard_index"},
            collect={
                "paths": ["results"],
                "checkpoint": True,
            },
        )
        assert spec.fanout.shard_by == "shard_index"


class TestEnvironmentAndSetup:
    def test_versioned_environment_profile_is_fixed(self):
        env = EnvironmentSpec(profile="ubuntu-agent-docker-v1")
        assert env.manifest()["docker"] is True
        with pytest.raises(ValidationError, match="unknown environment profile"):
            EnvironmentSpec(profile="latest")

    @pytest.mark.parametrize(
        "payload",
        [
            {"name": "j", "run": {"command": "x"}, "timeuot": 1},
            {"name": "j", "run": {"command": "x", "timeuot": 1}},
            {"name": "j", "run": {"command": "x"}, "setup": {"commnads": []}},
        ],
    )
    def test_unknown_fields_are_rejected_at_every_level(self, payload):
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            JobSpec.model_validate(payload)

    def test_legacy_commands_and_structured_steps_normalize_in_order(self):
        setup = SetupSpec.model_validate({
            "commands": ["export A=1", "echo $A"],
            "steps": [{
                "name": "install",
                "command": "uv sync",
                "env": {"UV_LINK_MODE": "copy"},
                "cwd": "python",
                "timeout": 1200,
                "retries": 2,
            }],
        })
        steps = setup.normalized_steps()
        assert [step.name for step in steps] == ["legacy-commands", "install"]
        assert steps[0].command == "export A=1 && echo $A"
        assert steps[1].env == {"UV_LINK_MODE": "copy"}
        assert steps[1].retries == 2
        assert steps[1].run_as == "job"

    def test_setup_step_rejects_root_escape_and_bad_env(self):
        with pytest.raises(ValidationError):
            SetupSpec(steps=[{"name": "x", "command": "x", "run_as": "root"}])
        with pytest.raises(ValidationError, match="invalid variable"):
            SetupSpec(steps=[{
                "name": "x", "command": "x", "env": {"BAD-NAME": "x"},
            }])
        with pytest.raises(ValidationError, match="cannot escape"):
            SetupSpec(steps=[{"name": "x", "command": "x", "cwd": "../etc"}])

    def test_source_ref_and_resolved_commit_are_explicit(self):
        commit = "a" * 40
        setup = SetupSpec(
            repo="https://github.com/org/repo.git",
            ref="release/v1",
            resolved_commit=commit,
        )
        assert setup.checkout_ref == "release/v1"
        assert setup.resolved_commit == commit
        with pytest.raises(ValidationError, match="full 40-64 hex"):
            SetupSpec(repo="https://github.com/org/repo.git", resolved_commit="abc")
        with pytest.raises(ValidationError, match="require setup.repo"):
            SetupSpec(ref="release/v1")

    @pytest.mark.parametrize("repo", [
        "https://token@github.com/org/repo.git",
        "https://user:secret@github.com/org/repo.git",
        "https://github.com/org/repo.git?token=secret",
        "file:///srv/private/repo",
        "/srv/private/repo",
    ])
    def test_repo_rejects_embedded_credentials_and_local_paths(self, repo):
        with pytest.raises(ValidationError, match="setup.repo"):
            SetupSpec(repo=repo)

    def test_repo_accepts_scp_style_ssh_remote(self):
        setup = SetupSpec(repo="git@github.com:org/repo.git")
        assert setup.repo == "git@github.com:org/repo.git"


class TestRunSafetyAndSecretEnv:
    def test_secret_env_accepts_references_and_rejects_plaintext(self):
        run = RunSpec(command="bench", secret_env={
            "TOKEN": "aws-secretsmanager://prod/service#token",
            "PASSWORD": "aws-ssm:///prod/password",
        })
        assert set(run.secret_env) == {"TOKEN", "PASSWORD"}

        with pytest.raises(ValidationError, match="must use"):
            RunSpec(command="bench", secret_env={"TOKEN": "plaintext"})

    def test_plain_and_secret_env_keys_cannot_overlap(self):
        with pytest.raises(ValidationError, match="cannot define the same"):
            RunSpec(
                command="bench",
                env={"TOKEN": "not-secret"},
                secret_env={"TOKEN": "aws-ssm:///prod/token"},
            )

    @pytest.mark.parametrize("field", ["env", "secret_env"])
    def test_run_env_names_are_validated(self, field):
        value = "x" if field == "env" else "aws-ssm:///x"
        with pytest.raises(ValidationError, match="invalid variable"):
            RunSpec(command="bench", **{field: {"BAD-NAME": value}})

    def test_command_cwd_and_env_reject_unsafe_values_but_allow_absolute_cwd(self):
        assert RunSpec(command="bench", cwd="/srv/jobs/input").cwd == "/srv/jobs/input"
        with pytest.raises(ValidationError, match="cannot be empty"):
            RunSpec(command="  ")
        with pytest.raises(ValidationError, match="control"):
            RunSpec(command="echo\x00bad")
        with pytest.raises(ValidationError, match="cannot contain"):
            RunSpec(command="bench", cwd="../escape")
        with pytest.raises(ValidationError, match="NUL"):
            RunSpec(command="bench", env={"TOKEN": "bad\x00value"})

    def test_s3_dataset_validates_uri_and_absolute_non_root_dest(self):
        dataset = S3Dataset(uri="s3://example-data/prefix/", dest="/srv/data")
        assert dataset.uri == "s3://example-data/prefix/"
        for payload in (
            {"uri": "https://example/data", "dest": "/srv/data"},
            {"uri": "s3://Bad_Bucket/data", "dest": "/srv/data"},
            {"uri": "s3://example-data/data", "dest": "relative"},
            {"uri": "s3://example-data/data", "dest": "/"},
            {"uri": "s3://example-data/data", "dest": "/srv/../etc"},
        ):
            with pytest.raises(ValidationError):
                S3Dataset.model_validate(payload)


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


class TestRenderS3Datasets:
    def test_uri_template_whitespace_is_canonicalized_and_rendered(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            setup={
                "s3_datasets": [{
                    "uri": "s3://private-data/shard-{{ shard_id }}.jsonl",
                    "dest": "/srv/replay/input.jsonl",
                }],
            },
        )

        assert spec.setup.s3_datasets[0].uri == (
            "s3://private-data/shard-{{shard_id}}.jsonl"
        )
        assert spec.render_s3_datasets(
            WorkerContext(shard_index=7)
        )[0].uri == "s3://private-data/shard-00007.jsonl"

    def test_renders_one_object_for_each_worker(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            fanout={"workers": 3, "shard_by": "shard_index"},
            setup={
                "s3_datasets": [{
                    "uri": "s3://private-data/run/shard-{{shard_id}}.jsonl",
                    "dest": "/srv/replay/shard-{{shard_id}}.jsonl",
                }],
            },
        )

        rendered = [
            spec.render_s3_datasets(ctx)[0]
            for ctx in spec.worker_contexts()
        ]

        assert [dataset.uri for dataset in rendered] == [
            "s3://private-data/run/shard-00000.jsonl",
            "s3://private-data/run/shard-00001.jsonl",
            "s3://private-data/run/shard-00002.jsonl",
        ]
        assert rendered[2].dest == "/srv/replay/shard-00002.jsonl"

    def test_unknown_dataset_template_variable_fails(self):
        with pytest.raises(ValidationError, match="unknown template variable"):
            JobSpec(
                name="j",
                run=RunSpec(command="bench"),
                setup={
                    "s3_datasets": [{
                        "uri": "s3://private-data/{{unknown}}.jsonl",
                        "dest": "/srv/replay/input.jsonl",
                    }],
                },
            )

    def test_dataset_template_value_cannot_render_empty(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="bench"),
            setup={
                "s3_datasets": [{
                    "uri": "s3://private-data/{{hostname}}",
                    "dest": "/srv/replay/input.jsonl",
                }],
            },
        )

        # Submission validation uses a throwaway synthetic hostname; real
        # worker contexts remain unmodified and must still fail closed until
        # discovery supplies a concrete hostname.
        assert spec.worker_contexts()[0].hostname == ""
        with pytest.raises(ValueError, match="hostname.*empty"):
            spec.render_s3_datasets(WorkerContext(hostname=""))


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

    def test_renders_explicit_checkpoint_recovery_command(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(
                command="bench --out out_{{shard_id}}",
                resume_command=(
                    "bench --out out_{{shard_id}} "
                    "--resume out_{{shard_id}}"
                ),
            ),
        )

        assert spec.render_recovery_command(
            WorkerContext(shard_index=5),
        ) == [
            "bash",
            "-lc",
            "bench --out out_00005 --resume out_00005",
        ]

    def test_checkpoint_recovery_command_is_optional_but_fail_closed_when_used(self):
        spec = JobSpec(name="j", run=RunSpec(command="bench"))

        with pytest.raises(ValueError, match="run.resume_command"):
            spec.render_recovery_command(WorkerContext())

    def test_checkpoint_rejects_hostname_derived_recovery_command(self):
        with pytest.raises(
            ValidationError,
            match="hostname-derived workload paths",
        ):
            JobSpec(
                name="j",
                run=RunSpec(
                    command="bench --out results/{{shard_id}}",
                    resume_command=(
                        "bench --resume results/$(hostname -s)"
                    ),
                ),
                fanout={"shard_by": "shard_index"},
                collect={"paths": ["results"], "checkpoint": True},
            )


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

    def test_codex_injects_codex_home_instead_of_claude_config_dir(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="x"),
            account={"agent_type": "codex", "config_dir": "/root/.codex-prod"},
        )

        env = spec.render_env(WorkerContext(config_dir="/root/.codex-slot-1"))

        assert env["CODEX_HOME"] == "/root/.codex-slot-1"
        assert "CLAUDE_CONFIG_DIR" not in env

    @pytest.mark.parametrize(
        "account",
        [
            {"agent_type": "codex", "per_worker": 2},
            {"agent_type": "codex"},
        ],
    )
    def test_codex_multi_account_or_rotation_requires_explicit_writable_home(
        self, account,
    ):
        rotation = (
            {"strategy": "on_exhaust_restart_resume", "resume_args": "--resume"}
            if account.get("per_worker", 1) == 1
            else {}
        )

        with pytest.raises(ValidationError, match="explicit absolute"):
            JobSpec(
                name="j",
                run=RunSpec(command="x"),
                account=account,
                rotation=rotation,
            )

    def test_codex_single_account_default_home_is_valid_for_non_root_worker(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="x"),
            account={"agent_type": "codex"},
        )

        assert spec.account.config_dir == ""
        assert "CODEX_HOME" not in spec.render_env(WorkerContext())

    @pytest.mark.parametrize("unsafe_name", ["CODEX_HOME", "HOME"])
    def test_codex_default_worker_login_rejects_run_credential_redirect(
        self, unsafe_name,
    ):
        with pytest.raises(ValidationError, match="managed credential paths"):
            JobSpec(
                name="j",
                run=RunSpec(command="x", env={unsafe_name: "/tmp/wrong"}),
                account={"agent_type": "codex"},
            )

    def test_codex_explicit_home_forces_verified_config_dir(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="x", env={"HOME": "/tmp/job-home"}),
            account={
                "agent_type": "codex",
                "config_dir": "/home/ubuntu/.codex-selected",
            },
        )

        env = spec.render_env(WorkerContext(
            config_dir="/home/ubuntu/.codex-verified"
        ))

        assert env["HOME"] == "/tmp/job-home"
        assert env["CODEX_HOME"] == "/home/ubuntu/.codex-verified"

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

    @pytest.mark.parametrize(
        ("agent_type", "unsafe_name"),
        [
            ("claude", "CLAUDE_CONFIG_DIR"),
            ("claude", "HOME"),
            ("codex", "CODEX_HOME"),
            ("codex", "HOME"),
        ],
    )
    def test_eip_rejects_run_env_that_can_redirect_authenticated_home(
        self, agent_type, unsafe_name
    ):
        with pytest.raises(ValidationError, match="does not allow run.env"):
            JobSpec(
                name="j",
                run=RunSpec(command="x", env={unsafe_name: "/tmp/wrong"}),
                account={"binding": "eip", "agent_type": agent_type},
                fanout={"workers": 1},
            )

    @pytest.mark.parametrize(
        ("agent_type", "unrelated_name"),
        [("claude", "CODEX_HOME"), ("codex", "CLAUDE_CONFIG_DIR")],
    )
    def test_eip_allows_other_agent_credential_env(
        self, agent_type, unrelated_name
    ):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="x", env={unrelated_name: "/tmp/unused"}),
            account={"binding": "eip", "agent_type": agent_type},
        )

        assert spec.run.env[unrelated_name] == "/tmp/unused"

    @pytest.mark.parametrize(
        "unsafe_name",
        [
            "http_proxy",
            "https_proxy",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "all_proxy",
        ],
    )
    @pytest.mark.parametrize("field", ["env", "secret_env"])
    def test_eip_rejects_proxy_environment_that_bypasses_stable_egress(
        self, unsafe_name, field
    ):
        value = (
            "http://proxy.invalid"
            if field == "env"
            else "aws-ssm:///jobs/proxy"
        )
        with pytest.raises(ValidationError, match="direct EIP egress"):
            JobSpec(
                name="j",
                run=RunSpec(command="x", **{field: {unsafe_name: value}}),
                account={"binding": "eip"},
            )

    def test_non_eip_job_may_use_proxy_environment(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(
                command="x",
                env={"HTTPS_PROXY": "http://proxy.invalid"},
            ),
        )

        assert spec.run.env["HTTPS_PROXY"] == "http://proxy.invalid"


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
    @pytest.mark.parametrize(
        "path",
        ["", ".", "../secret", "out/../../secret", "/etc", "bad\\path", "bad\x00path"],
    )
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
        assert spec.account.agent_type == "claude"
        assert spec.account.model == ""
        assert spec.account.per_worker == 1
        assert spec.account.binding == "none"
        assert spec.account.ids == []
        assert spec.account.login_timeout_seconds == 900
        assert spec.rotation.strategy == "none"
        assert spec.run.shell is True
        assert spec.run.resume_command == ""
        assert spec.run.timeout == DEFAULT_RUN_TIMEOUT_SECONDS
        assert spec.ttl_seconds == DEFAULT_JOB_TTL_SECONDS
        assert spec.environment.profile == "ubuntu-agent-v1"
        assert spec.harness_ref is None

    def test_codex_agent_type_is_supported(self):
        spec = JobSpec(
            name="j",
            run=RunSpec(command="echo hi"),
            account={"agent_type": "codex"},
        )

        assert spec.account.agent_type == "codex"

    def test_optional_agent_model_is_normalized_and_bounded(self):
        account = AccountSpec(model="  gpt-5.4  ")

        assert account.model == "gpt-5.4"
        with pytest.raises(ValidationError):
            AccountSpec(model="bad\nmodel")
        with pytest.raises(ValidationError):
            AccountSpec(model="x" * 201)

    @pytest.mark.parametrize("timeout", [59, 1201])
    def test_account_login_timeout_is_bounded(self, timeout):
        with pytest.raises(ValidationError):
            AccountSpec(login_timeout_seconds=timeout)

    def test_unknown_agent_type_is_rejected(self):
        with pytest.raises(ValidationError):
            AccountSpec(agent_type="other")

    def test_codex_rejects_manager_token_distribution(self):
        with pytest.raises(ValidationError, match="worker_local_login"):
            JobSpec(
                name="j",
                run=RunSpec(command="codex exec task"),
                account={
                    "agent_type": "codex",
                    "mode": "manager_distribute",
                },
            )

    def test_claude_also_rejects_unimplemented_manager_distribution(self):
        with pytest.raises(ValidationError, match="not implemented"):
            JobSpec(
                name="j",
                run=RunSpec(command="claude -p task"),
                account={
                    "agent_type": "claude",
                    "mode": "manager_distribute",
                },
            )


class TestEipAccountBinding:
    def test_account_ids_are_trimmed_without_losing_slot_positions(self):
        account = AccountSpec(ids=[" acct-2 ", "acct-1", "acct-2", "", "acct-1"])

        assert account.ids == ["acct-2", "acct-1", "acct-2", "acct-1"]

    def test_unbound_explicit_ids_preserve_agent_api_references(self):
        spec = JobSpec(
            name="shared-api",
            run=RunSpec(command="bench"),
            account={"ids": ["cloudrouter-1", "cloudrouter-1"]},
            fanout={"workers": 2},
        )

        assert spec.account.ids == ["cloudrouter-1", "cloudrouter-1"]

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

    def test_unbound_explicit_ids_cover_every_worker_slot(self):
        spec = JobSpec(
            name="explicit",
            run=RunSpec(command="bench"),
            account={
                "per_worker": 2,
                "config_dir": "/root/.claude",
                "ids": ["a1", "a2", "a3", "a4"],
            },
            fanout={"workers": 2},
        )

        assert spec.account.ids == ["a1", "a2", "a3", "a4"]

    def test_unbound_explicit_ids_reject_partial_assignment(self):
        with pytest.raises(
            ValidationError,
            match="account.ids must contain exactly 4",
        ):
            JobSpec(
                name="explicit",
                run=RunSpec(command="bench"),
                account={
                    "per_worker": 2,
                    "config_dir": "/root/.claude",
                    "ids": ["a1", "a2"],
                },
                fanout={"workers": 2},
            )
