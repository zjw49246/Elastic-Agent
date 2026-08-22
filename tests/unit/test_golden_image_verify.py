"""Behaviour tests for the standalone golden-image verifier."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "golden_image_verify.py"
SPEC = importlib.util.spec_from_file_location("golden_image_verify", SCRIPT)
assert SPEC and SPEC.loader
golden = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(golden)


def _manifest() -> dict:
    return {
        "schema_version": 1,
        "image_profile": "elastic-agent-worker-union-v1",
        "components": {
            "system": {
                "packages": {
                    "python3": "3.14.4-1",
                    "python3-venv": "3.14.4-1",
                    "nodejs": "22.22.1-1",
                    "bubblewrap": "0.11.0-1",
                    "util-linux": "2.40.2-1",
                },
            },
            "agents": {"claude": "2.1.181", "codex": "0.144.6"},
            "login": {
                "chrome_version": "150.0.7871.181-1",
                "system_packages": {"xvfb": "1", "xdotool": "2"},
                "python_packages": {"httpx": "0.28.1", "playwright": "1.61.0"},
            },
            "docker": {
                "system_packages": {"docker.io": "29.1", "docker-buildx": "0.30"},
            },
            "runtime": {
                "python_packages": {
                    "pydantic": "2.13.4",
                    "pydantic-settings": "2.14.1",
                    "psutil": "7.2.2",
                },
            },
            "pty": {"commit": "d6ff732d633b8b7bdb3ada717ffd1cbc9e701163"},
        },
    }


def test_load_manifest_requires_supported_schema(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"schema_version": 99, "components": {}}))

    with pytest.raises(golden.VerificationError, match="schema"):
        golden.load_manifest(path)


def test_system_requires_manifest_and_installed_versions(monkeypatch) -> None:
    installed = {"python3": "3.14.4-1", "nodejs": "22.22.1-1"}
    monkeypatch.setattr(golden, "_dpkg_version", installed.__getitem__)
    monkeypatch.setattr(golden, "_require_commands", lambda _commands: None)

    golden.verify_system(_manifest(), ["python3", "nodejs"])

    installed["nodejs"] = "unexpected"
    with pytest.raises(golden.VerificationError, match="nodejs"):
        golden.verify_system(_manifest(), ["python3", "nodejs"])


def test_system_requires_bwrap_command_but_only_records_util_linux(
    monkeypatch,
) -> None:
    installed = {
        "python3-venv": "3.14.4-1",
        "bubblewrap": "0.11.0-1",
        "util-linux": "2.40.2-1",
    }
    required: list[str] = []
    monkeypatch.setattr(golden, "_dpkg_version", installed.__getitem__)
    monkeypatch.setattr(
        golden,
        "_require_commands",
        lambda commands: required.extend(commands),
    )

    golden.verify_system(
        _manifest(),
        ["python3-venv", "bubblewrap", "util-linux"],
    )

    assert required == ["bwrap"]


@pytest.mark.parametrize(
    ("agent", "version", "output"),
    [
        ("claude", "2.1.181", "2.1.181 (Claude Code)"),
        ("codex", "0.144.6", "codex-cli 0.144.6"),
    ],
)
def test_agent_checks_manifest_and_real_cli(
    monkeypatch,
    agent: str,
    version: str,
    output: str,
) -> None:
    monkeypatch.setattr(golden, "_command_output", lambda _argv: output)

    golden.verify_agent(_manifest(), agent, version)

    monkeypatch.setattr(golden, "_command_output", lambda _argv: "0.0.0")
    with pytest.raises(golden.VerificationError, match="version"):
        golden.verify_agent(_manifest(), agent, version)


def test_python_checks_distribution_version_and_import(monkeypatch) -> None:
    versions = {
        "pydantic": "2.13.4",
        "pydantic-settings": "2.14.1",
        "psutil": "7.2.2",
    }
    imported: list[str] = []
    monkeypatch.setattr(golden.metadata, "version", versions.__getitem__)
    monkeypatch.setattr(
        golden.importlib,
        "import_module",
        lambda name: imported.append(name),
    )

    golden.verify_python(
        _manifest(),
        ["pydantic", "pydantic-settings", "psutil"],
    )

    assert imported == ["pydantic", "pydantic_settings", "psutil"]


def test_login_checks_chrome_system_and_python_dependencies(monkeypatch) -> None:
    versions = {
        "xvfb": "1",
        "xdotool": "2",
        "google-chrome-stable": "150.0.7871.181-1",
    }
    monkeypatch.setattr(golden, "_dpkg_version", versions.__getitem__)
    monkeypatch.setattr(golden, "_require_commands", lambda _commands: None)
    monkeypatch.setattr(
        golden,
        "_command_output",
        lambda _argv: "Google Chrome 150.0.7871.181",
    )
    monkeypatch.setattr(
        golden.metadata,
        "version",
        lambda name: {
            "httpx": "0.28.1",
            "playwright": "1.61.0",
        }[name],
    )
    monkeypatch.setattr(golden.importlib, "import_module", lambda _name: None)

    golden.verify_login(_manifest(), ["httpx", "playwright"])


def test_pty_requires_direct_url_commit_and_import(monkeypatch) -> None:
    commit = "d6ff732d633b8b7bdb3ada717ffd1cbc9e701163"
    dist = SimpleNamespace(
        read_text=lambda name: json.dumps({"vcs_info": {"commit_id": commit}}) if name == "direct_url.json" else None,
    )
    monkeypatch.setattr(golden.metadata, "distribution", lambda _name: dist)
    monkeypatch.setattr(golden.importlib, "import_module", lambda _name: None)

    golden.verify_pty(_manifest(), commit)

    with pytest.raises(golden.VerificationError, match="commit"):
        golden.verify_pty(_manifest(), "a" * 40)
