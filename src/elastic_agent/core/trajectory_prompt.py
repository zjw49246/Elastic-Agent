"""Stable prompt metadata for reproducible CC/Codex trajectories."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from elastic_agent.core.job_spec import JobSpec, WorkerContext, render_template

PROMPT_METADATA_SCHEMA = 1
MAX_PROMPT_METADATA_BYTES = 6 * 1024 * 1024


def _captured_text(text: str) -> dict[str, Any]:
    encoded = text.encode("utf-8")
    return {
        "text": text,
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "bytes": len(encoded),
    }


def build_trajectory_prompt_metadata(
    spec: JobSpec,
    ctx: WorkerContext,
    *,
    command: list[str],
    resumed: bool,
) -> dict[str, Any]:
    """Build the immutable, framework-visible prompt envelope for one attempt."""

    declared = spec.run.trajectory_prompt
    context = ctx.as_dict()
    components: dict[str, dict[str, Any]] = {}
    for name in ("system", "developer", "user"):
        value = render_template(getattr(declared, name), context)
        if value:
            components[name] = _captured_text(value)

    sources = []
    for source in declared.sources:
        content = render_template(source.content, context)
        item = _captured_text(content)
        item["name"] = render_template(source.name, context)
        sources.append(item)

    unavailable = [
        "provider_builtin_system_prompt",
        "compiled_session_context",
    ]
    for name in ("system", "developer", "user"):
        if name not in components:
            unavailable.append(f"undeclared_{name}_prompt")

    invocation = json.dumps(command, ensure_ascii=False, separators=(",", ":"))
    return {
        "schema": PROMPT_METADATA_SCHEMA,
        "agent_type": spec.account.agent_type,
        "capture_mode": "declared" if components or sources else "opaque_command",
        # The provider-owned prompt and resumed context are not exposed by either
        # CLI, so a Manager-side capture must never claim byte-exact completeness.
        "complete": False,
        "components": components,
        "sources": sources,
        "unavailable_components": unavailable,
        "invocation": {
            "argv_sha256": hashlib.sha256(invocation.encode("utf-8")).hexdigest(),
            "resumed": resumed,
        },
    }


def summarize_trajectory_prompt(metadata: dict[str, Any]) -> dict[str, Any]:
    """Remove prompt plaintext while retaining completeness and integrity data."""

    summary = dict(metadata)
    components = {}
    for name, component in dict(metadata.get("components") or {}).items():
        if isinstance(component, dict):
            components[name] = {
                key: component[key]
                for key in ("sha256", "bytes")
                if key in component
            }
    summary["components"] = components
    sources = []
    for source in list(metadata.get("sources") or []):
        if isinstance(source, dict):
            sources.append({
                key: source[key]
                for key in ("name", "sha256", "bytes")
                if key in source
            })
    summary["sources"] = sources
    return summary


def normalize_trajectory_prompt_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    """Validate and copy a prompt envelope before persistence or API use."""

    if not isinstance(metadata, dict):
        raise ValueError("prompt metadata must be an object")
    encoded = json.dumps(
        metadata,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > MAX_PROMPT_METADATA_BYTES:
        raise ValueError("prompt metadata exceeds the byte limit")
    value = json.loads(encoded)
    expected_keys = {
        "schema",
        "agent_type",
        "capture_mode",
        "complete",
        "components",
        "sources",
        "unavailable_components",
        "invocation",
    }
    if (
        set(value) != expected_keys
        or value.get("schema") != PROMPT_METADATA_SCHEMA
        or value.get("agent_type") not in {"claude", "codex"}
        or value.get("capture_mode") not in {"declared", "opaque_command"}
        or not isinstance(value.get("complete"), bool)
        or not isinstance(value.get("components"), dict)
        or not isinstance(value.get("sources"), list)
        or not isinstance(value.get("unavailable_components"), list)
        or not isinstance(value.get("invocation"), dict)
    ):
        raise ValueError("invalid prompt metadata envelope")

    components = value["components"]
    if set(components).difference({"system", "developer", "user"}):
        raise ValueError("invalid prompt component")
    for component in components.values():
        if (
            not isinstance(component, dict)
            or set(component) != {"text", "sha256", "bytes"}
        ):
            raise ValueError("invalid captured prompt fields")
        _validate_captured_text(component)
    if len(value["sources"]) > 64:
        raise ValueError("too many prompt sources")
    for source in value["sources"]:
        if (
            not isinstance(source, dict)
            or not isinstance(source.get("name"), str)
            or not source["name"]
            or len(source["name"]) > 4_096
            or set(source) != {"name", "text", "sha256", "bytes"}
        ):
            raise ValueError("invalid prompt source")
        _validate_captured_text(source)
    if any(
        not isinstance(item, str) or not item or len(item) > 256
        for item in value["unavailable_components"]
    ):
        raise ValueError("invalid unavailable prompt component")
    invocation = value["invocation"]
    if (
        set(invocation) != {"argv_sha256", "resumed"}
        or not _valid_sha256(invocation.get("argv_sha256"))
        or not isinstance(invocation.get("resumed"), bool)
    ):
        raise ValueError("invalid prompt invocation metadata")
    return value


def _valid_sha256(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _validate_captured_text(component: object) -> None:
    if not isinstance(component, dict) or not isinstance(component.get("text"), str):
        raise ValueError("invalid captured prompt text")
    encoded = component["text"].encode("utf-8")
    if (
        component.get("bytes") != len(encoded)
        or not _valid_sha256(component.get("sha256"))
        or component["sha256"] != hashlib.sha256(encoded).hexdigest()
    ):
        raise ValueError("prompt content integrity mismatch")
