"""ApexRouter adapter for provider-neutral Agent API accounts.

ApexRouter exposes the native Codex model catalog rather than an
OpenAI-compatible ``data[].id`` list.  It is intentionally Codex-only and its
quota response combines per-key usage with shared group limits; those values
must remain separate when deciding availability.  An explicit null/null
remaining-limit pair marks one shared window as unlimited.
"""

from __future__ import annotations

import os
import time
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Mapping

import httpx

from elastic_agent.core.agent_api import (
    MAX_AGENT_API_MODEL_ID_LENGTH,
    MAX_AGENT_API_MODELS,
    AgentApiUpstreamError,
)
from elastic_agent.core.bootstrap_steps import CODEX_CLI_VERSION
from elastic_agent.core.cloudrouter import CloudRouterAdapter

# Public ApexRouter gateway.  The former sslip.io address is kept out of the
# active endpoint set because it can still answer usage requests while its
# model/Responses route has no ready upstream account.
APEX_CODEX_BASE_URL = "https://api.apexin.ai/v1"
APEX_MODELS_URL = f"{APEX_CODEX_BASE_URL}/models"
APEX_USAGE_URL = f"{APEX_CODEX_BASE_URL}/usage"
APEX_CODEX_CLIENT_VERSION = CODEX_CLI_VERSION
APEX_ENDPOINTS: Mapping[str, str | None] = MappingProxyType(
    {
        "anthropic_base_url": None,
        "openai_base_url": APEX_CODEX_BASE_URL,
        "models_url": APEX_MODELS_URL,
        "usage_url": APEX_USAGE_URL,
    }
)

_MAX_ABS_USAGE_NUMBER = Decimal("1e18")
_MIN_ABS_NONZERO_USAGE_NUMBER = Decimal("1e-18")
_WINDOW_DEFINITIONS = (
    ("requests_5h", "5h requests (shared group)", "requests"),
    ("requests_day", "Daily requests (shared group)", "requests"),
    ("tokens_day", "Daily tokens (shared group)", "tokens"),
    ("tokens_month", "Monthly tokens (shared group)", "tokens"),
)


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(
        value,
        (str, int, float, Decimal),
    ):
        return None
    if isinstance(value, str) and (
        not value
        or value.strip() != value
        or len(value) > 64
    ):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    if (
        not result.is_finite()
        or abs(result) > _MAX_ABS_USAGE_NUMBER
        or (
            result != 0
            and abs(result) < _MIN_ABS_NONZERO_USAGE_NUMBER
        )
    ):
        return None
    return result


def _json_number(value: Decimal) -> int | float:
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _is_codex_model(model: str) -> bool:
    value = model.lower()
    return value.startswith(("gpt-", "o1", "o3", "o4", "codex-"))


def _normalise_apex_models(payload: Any) -> dict[str, list[str]]:
    if not isinstance(payload, dict):
        raise AgentApiUpstreamError("invalid_models_response")
    native_items = payload.get("models")
    openai_items = payload.get("data")
    native_schema = isinstance(native_items, list)
    openai_schema = isinstance(openai_items, list)
    if native_schema == openai_schema:
        raise AgentApiUpstreamError("invalid_models_response")
    if openai_schema:
        if payload.get("object") != "list" or payload.get("success") is not True:
            raise AgentApiUpstreamError("invalid_models_response")
        items = openai_items
    else:
        items = native_items
    assert isinstance(items, list)
    if len(items) > MAX_AGENT_API_MODELS:
        raise AgentApiUpstreamError("invalid_models_response")

    selected: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        if native_schema and (
            item.get("supported_in_api") is False
            or item.get("visibility") == "hide"
        ):
            continue
        model = item.get("slug" if native_schema else "id")
        if not isinstance(model, str):
            continue
        try:
            encoded = model.encode("utf-8")
        except UnicodeEncodeError as exc:
            raise AgentApiUpstreamError(
                "invalid_models_response"
            ) from exc
        if (
            not model
            or model.strip() != model
            or not encoded
            or len(model) > MAX_AGENT_API_MODEL_ID_LENGTH
            or any(character.isspace() for character in model)
            or any(not character.isprintable() for character in model)
        ):
            raise AgentApiUpstreamError("invalid_models_response")
        if _is_codex_model(model):
            selected.add(model)
    if not selected:
        raise AgentApiUpstreamError("no_supported_models")
    return {"claude": [], "codex": sorted(selected)}


def _runtime_guarded_usage(account_id: str) -> dict[str, Any]:
    """Represent a valid key when Apex exposes no public usage endpoint."""

    return {
        "account_id": account_id,
        "fetched_at": time.time(),
        "stale": False,
        "state": "active",
        "status": "active",
        "mode": "runtime_guarded",
        "quota": None,
        "windows": [],
        "usage": {},
        "available": True,
        "known": True,
        "reason": "provider_usage_endpoint_unavailable",
    }


def _normalise_apex_usage(
    account_id: str,
    payload: Any,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise AgentApiUpstreamError("invalid_usage_response")
    raw_used = payload.get("used")
    raw_remaining = payload.get("remaining")
    raw_limits = payload.get("limits")
    if not all(
        isinstance(value, dict)
        for value in (raw_used, raw_remaining, raw_limits)
    ):
        raise AgentApiUpstreamError("invalid_usage_response")

    windows: list[dict[str, Any]] = []
    key_usage: dict[str, int | float] = {}
    exhausted = False
    for window_id, label, unit in _WINDOW_DEFINITIONS:
        key_used = _decimal(raw_used.get(window_id))
        has_remaining = window_id in raw_remaining
        has_limit = window_id in raw_limits
        raw_window_remaining = raw_remaining.get(window_id)
        raw_window_limit = raw_limits.get(window_id)
        unlimited = (
            has_remaining
            and has_limit
            and raw_window_remaining is None
            and raw_window_limit is None
        )
        # Apex documents every fixed window. Missing or asymmetric values are
        # not enough to prove availability, while an explicit null/null pair
        # is its sentinel for a window with no shared quota.
        if (
            key_used is None
            or key_used < 0
            or not has_remaining
            or not has_limit
        ):
            raise AgentApiUpstreamError("invalid_usage_response")

        parsed_key_used = _json_number(key_used)
        key_usage[window_id] = parsed_key_used
        if unlimited:
            windows.append(
                {
                    "id": window_id,
                    "label": label,
                    "unit": unit,
                    "currency": unit,
                    "scope": "group",
                    "unlimited": True,
                    "key_used": parsed_key_used,
                }
            )
            continue

        remaining = _decimal(raw_window_remaining)
        limit = _decimal(raw_window_limit)
        if (
            remaining is None
            or limit is None
            or remaining < 0
            or limit < 0
            or remaining > limit
        ):
            raise AgentApiUpstreamError("invalid_usage_response")

        parsed_remaining = _json_number(remaining)
        parsed_limit = _json_number(limit)
        parsed_group_used = _json_number(limit - remaining)
        windows.append(
            {
                "id": window_id,
                "label": label,
                "unit": unit,
                "currency": unit,
                "scope": "group",
                "used": parsed_group_used,
                "remaining": parsed_remaining,
                "limit": parsed_limit,
                "key_used": parsed_key_used,
            }
        )
        exhausted = exhausted or remaining <= 0

    concurrency_value = _decimal(raw_limits.get("concurrency"))
    if concurrency_value is None or concurrency_value < 0:
        raise AgentApiUpstreamError("invalid_usage_response")
    concurrency = _json_number(concurrency_value)
    exhausted = exhausted or concurrency_value <= 0

    state = "exhausted" if exhausted else "active"
    snapshot: dict[str, Any] = {
        "account_id": account_id,
        "fetched_at": time.time(),
        "stale": False,
        "state": state,
        "status": state,
        "mode": "shared_group",
        "currency": None,
        "unit": None,
        "quota": None,
        "windows": windows,
        "usage": {"key": dict(key_usage)},
        "key_usage": key_usage,
        "concurrency": concurrency,
        "available": not exhausted,
        "known": True,
        "reason": state,
    }
    for field in ("key_name", "group_name"):
        value = payload.get(field)
        if isinstance(value, str):
            value = value.strip()
            if 0 < len(value) <= 256 and all(
                character.isprintable() for character in value
            ):
                snapshot[field] = value
    return snapshot


class ApexRouterAdapter(CloudRouterAdapter):
    """Bounded HTTP adapter for ApexRouter's fixed Codex API."""

    provider = "apex"

    def __init__(self, *, usage_policy: str | None = None) -> None:
        super().__init__()
        policy = usage_policy or os.environ.get(
            "ELASTIC_AGENT_APEX_USAGE_POLICY",
            "runtime",
        )
        if policy not in {"runtime", "strict"}:
            raise ValueError(
                "ELASTIC_AGENT_APEX_USAGE_POLICY must be runtime or strict"
            )
        self.usage_policy = policy

    @property
    def endpoints(self) -> Mapping[str, str | None]:
        return APEX_ENDPOINTS

    async def probe_models(self, api_key: str) -> dict[str, list[str]]:
        models_url = str(
            httpx.URL(APEX_MODELS_URL).copy_set_param(
                "client_version",
                APEX_CODEX_CLIENT_VERSION,
            )
        )
        return _normalise_apex_models(
            await self._request_json(models_url, api_key)
        )

    async def fetch_usage(
        self,
        account_id: str,
        api_key: str,
    ) -> dict[str, Any]:
        try:
            payload = await self._request_json(APEX_USAGE_URL, api_key)
        except AgentApiUpstreamError as exc:
            if (
                self.usage_policy == "runtime"
                and exc.status_code == 404
                and exc.code == "upstream_rejected"
            ):
                return _runtime_guarded_usage(account_id)
            raise
        return _normalise_apex_usage(account_id, payload)


__all__ = [
    "APEX_CODEX_BASE_URL",
    "APEX_CODEX_CLIENT_VERSION",
    "APEX_ENDPOINTS",
    "APEX_MODELS_URL",
    "APEX_USAGE_URL",
    "ApexRouterAdapter",
]
