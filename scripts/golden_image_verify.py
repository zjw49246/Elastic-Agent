#!/usr/bin/env python3
"""Fail-closed verifier for Elastic-Agent golden-image fast paths.

The image builder writes a root-owned manifest containing the exact versions it
installed.  Bootstrap may skip a network install only when this program proves
both that the requested component is declared in that manifest *and* that the
corresponding packages, commands, imports, or VCS checkout still match it.

This file intentionally uses only the Python standard library so it can run
before any worker Python dependencies have been repaired.
"""

from __future__ import annotations

import argparse
import importlib
import json
import re
import shutil
import subprocess
import sys
from importlib import metadata
from pathlib import Path
from typing import Any

DEFAULT_MANIFEST = Path("/etc/elastic-agent/image-manifest.json")
SCHEMA_VERSION = 1
_COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
_DIST_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*")

_SYSTEM_COMMANDS = {
    "python3": "python3",
    "python3-pip": "pip3",
    "git": "git",
    "curl": "curl",
    "rsync": "rsync",
    "nodejs": "node",
    "npm": "npm",
    "awscli": "aws",
    "xvfb": "Xvfb",
    "xdotool": "xdotool",
    "wget": "wget",
}

_IMPORT_NAMES = {
    "pydantic-settings": "pydantic_settings",
    "pyyaml": "yaml",
    "claude-pty": "claude_pty",
}


class VerificationError(RuntimeError):
    """The baked declaration and the live machine do not match exactly."""


def load_manifest(path: str | Path = DEFAULT_MANIFEST) -> dict[str, Any]:
    manifest_path = Path(path)
    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise VerificationError(f"cannot read golden image manifest: {exc}") from exc
    if not isinstance(raw, dict) or raw.get("schema_version") != SCHEMA_VERSION:
        raise VerificationError("unsupported golden image manifest schema")
    if not isinstance(raw.get("image_profile"), str) or not raw["image_profile"]:
        raise VerificationError("golden image manifest has no image_profile")
    if not isinstance(raw.get("components"), dict):
        raise VerificationError("golden image manifest has no components object")
    return raw


def _component(manifest: dict[str, Any], name: str) -> dict[str, Any]:
    value = manifest["components"].get(name)
    if not isinstance(value, dict):
        raise VerificationError(f"golden image manifest has no {name!r} component")
    return value


def _command_output(argv: list[str]) -> str:
    if not argv or shutil.which(argv[0]) is None:
        raise VerificationError(f"required command is missing: {argv[0] if argv else ''}")
    try:
        result = subprocess.run(
            argv,
            check=False,
            capture_output=True,
            text=True,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise VerificationError(f"cannot execute {argv[0]}: {exc}") from exc
    if result.returncode != 0:
        output = (result.stdout or result.stderr or "").strip()
        raise VerificationError(f"{argv[0]} exited {result.returncode}: {output[:160]}")
    return (result.stdout or result.stderr or "").strip()


def _require_commands(commands: list[str]) -> None:
    missing = [command for command in commands if shutil.which(command) is None]
    if missing:
        raise VerificationError(f"required commands are missing: {', '.join(missing)}")


def _dpkg_version(package: str) -> str:
    output = _command_output(["dpkg-query", "-W", "-f=${Status}\n${Version}", package])
    lines = output.splitlines()
    if len(lines) < 2 or lines[0].strip() != "install ok installed":
        raise VerificationError(f"Debian package is not installed: {package}")
    return lines[1].strip()


def _declared_versions(component: dict[str, Any], key: str) -> dict[str, str]:
    versions = component.get(key)
    if not isinstance(versions, dict) or not all(
        isinstance(name, str) and isinstance(version, str) and version for name, version in versions.items()
    ):
        raise VerificationError(f"golden image component has invalid {key!r}")
    return versions


def _verify_dpkg_versions(declared: dict[str, str], packages: list[str]) -> None:
    for package in packages:
        expected = declared.get(package)
        if not expected:
            raise VerificationError(f"Debian package is not declared: {package}")
        actual = _dpkg_version(package)
        if actual != expected:
            raise VerificationError(f"Debian package {package} version mismatch: expected {expected}, got {actual}")


def verify_system(manifest: dict[str, Any], packages: list[str]) -> None:
    if not packages:
        raise VerificationError("no system packages requested")
    declared = _declared_versions(_component(manifest, "system"), "packages")
    _verify_dpkg_versions(declared, packages)
    commands = sorted({_SYSTEM_COMMANDS[p] for p in packages if p in _SYSTEM_COMMANDS})
    _require_commands(commands)


def verify_agent(
    manifest: dict[str, Any],
    agent: str,
    expected_version: str,
) -> None:
    agents = _component(manifest, "agents")
    if agent not in {"claude", "codex"}:
        raise VerificationError(f"unsupported agent: {agent}")
    if agents.get(agent) != expected_version:
        raise VerificationError(f"manifest {agent} version does not match {expected_version}")
    output = _command_output([agent, "--version"])
    if agent == "claude":
        valid = output.split(maxsplit=1)[0] == expected_version and "native binary not installed" not in output.lower()
    else:
        valid = output == f"codex-cli {expected_version}"
    if not valid:
        raise VerificationError(f"{agent} CLI version mismatch: expected {expected_version}, got {output!r}")


def _distribution_name(requirement: str) -> str:
    match = _DIST_RE.match(requirement.strip())
    if not match:
        raise VerificationError(f"invalid Python requirement: {requirement!r}")
    return match.group(0).lower().replace("_", "-").replace(".", "-")


def _verify_python_versions(
    declared: dict[str, str],
    requirements: list[str],
) -> None:
    for requirement in requirements:
        name = _distribution_name(requirement)
        expected = declared.get(name)
        if not expected:
            raise VerificationError(f"Python distribution is not declared: {name}")
        try:
            actual = metadata.version(name)
        except metadata.PackageNotFoundError as exc:
            raise VerificationError(f"Python distribution is missing: {name}") from exc
        if actual != expected:
            raise VerificationError(f"Python distribution {name} version mismatch: expected {expected}, got {actual}")
        import_name = _IMPORT_NAMES.get(name, name.replace("-", "_"))
        try:
            importlib.import_module(import_name)
        except Exception as exc:  # noqa: BLE001 - any broken native/import state is unsafe
            raise VerificationError(f"cannot import {import_name}: {exc}") from exc


def verify_python(manifest: dict[str, Any], requirements: list[str]) -> None:
    if not requirements:
        raise VerificationError("no Python distributions requested")
    declared = _declared_versions(_component(manifest, "runtime"), "python_packages")
    _verify_python_versions(declared, requirements)


def verify_login(manifest: dict[str, Any], requirements: list[str]) -> None:
    login = _component(manifest, "login")
    declared_system = _declared_versions(login, "system_packages")
    _verify_dpkg_versions(declared_system, ["xvfb", "xdotool"])
    _require_commands(["google-chrome", "Xvfb", "xdotool"])

    expected_deb = login.get("chrome_version")
    if not isinstance(expected_deb, str) or not expected_deb:
        raise VerificationError("manifest has no Chrome version")
    actual_deb = _dpkg_version("google-chrome-stable")
    if actual_deb != expected_deb:
        raise VerificationError(f"Chrome package version mismatch: expected {expected_deb}, got {actual_deb}")
    # Debian's package revision is not part of `google-chrome --version`.
    expected_cli = expected_deb.rsplit("-", 1)[0]
    output = _command_output(["google-chrome", "--version"])
    if output != f"Google Chrome {expected_cli}":
        raise VerificationError(f"Chrome CLI version mismatch: expected {expected_cli}, got {output!r}")

    declared_python = _declared_versions(login, "python_packages")
    _verify_python_versions(declared_python, requirements)


def verify_docker(manifest: dict[str, Any]) -> None:
    docker = _component(manifest, "docker")
    declared = _declared_versions(docker, "system_packages")
    _verify_dpkg_versions(declared, ["docker.io", "docker-buildx"])
    _require_commands(["docker"])
    _command_output(["docker", "--version"])
    _command_output(["docker", "buildx", "version"])


def verify_pty(manifest: dict[str, Any], expected_commit: str) -> None:
    expected_commit = expected_commit.lower()
    if not _COMMIT_RE.fullmatch(expected_commit):
        raise VerificationError("claude-pty expected commit must be a full SHA-1")
    pty = _component(manifest, "pty")
    if str(pty.get("commit", "")).lower() != expected_commit:
        raise VerificationError("manifest claude-pty commit does not match requested commit")
    try:
        dist = metadata.distribution("claude-pty")
        direct_url_text = dist.read_text("direct_url.json")
        direct_url = json.loads(direct_url_text or "{}")
    except (metadata.PackageNotFoundError, json.JSONDecodeError, OSError) as exc:
        raise VerificationError(f"cannot inspect installed claude-pty: {exc}") from exc
    actual = str(direct_url.get("vcs_info", {}).get("commit_id", "")).lower()
    if actual != expected_commit:
        raise VerificationError(f"installed claude-pty commit mismatch: expected {expected_commit}, got {actual}")
    try:
        importlib.import_module("claude_pty")
    except Exception as exc:  # noqa: BLE001
        raise VerificationError(f"cannot import claude_pty: {exc}") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    commands = parser.add_subparsers(dest="component", required=True)

    system = commands.add_parser("system")
    system.add_argument("packages", nargs="+")
    agent = commands.add_parser("agent")
    agent.add_argument("agent", choices=["claude", "codex"])
    agent.add_argument("version")
    login = commands.add_parser("login")
    login.add_argument("requirements", nargs="+")
    commands.add_parser("docker")
    python = commands.add_parser("python")
    python.add_argument("requirements", nargs="+")
    pty = commands.add_parser("pty")
    pty.add_argument("commit")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        manifest = load_manifest(args.manifest)
        if args.component == "system":
            verify_system(manifest, args.packages)
        elif args.component == "agent":
            verify_agent(manifest, args.agent, args.version)
        elif args.component == "login":
            verify_login(manifest, args.requirements)
        elif args.component == "docker":
            verify_docker(manifest)
        elif args.component == "python":
            verify_python(manifest, args.requirements)
        elif args.component == "pty":
            verify_pty(manifest, args.commit)
    except VerificationError as exc:
        print(f"golden image verification failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
