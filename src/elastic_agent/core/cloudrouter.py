"""CloudRouter adapter for provider-neutral Agent API accounts."""

from __future__ import annotations

import asyncio
import json
import math
import re
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from types import MappingProxyType
from typing import Any, Mapping

import httpx

from elastic_agent.core.agent_api import (
    MAX_AGENT_API_KEY_BYTES,
    MAX_AGENT_API_MODEL_ID_LENGTH,
    MAX_AGENT_API_MODELS,
    AgentApiUpstreamError,
)

CLOUDROUTER_ANTHROPIC_BASE_URL = "https://console.cloudrouter.online"
CLOUDROUTER_OPENAI_BASE_URL = "https://console.cloudrouter.online/v1"
CLOUDROUTER_MODELS_URL = f"{CLOUDROUTER_OPENAI_BASE_URL}/models"
CLOUDROUTER_USAGE_URL = f"{CLOUDROUTER_OPENAI_BASE_URL}/usage"
CLOUDROUTER_ENDPOINTS: Mapping[str, str] = MappingProxyType(
    {
        "anthropic_base_url": CLOUDROUTER_ANTHROPIC_BASE_URL,
        "openai_base_url": CLOUDROUTER_OPENAI_BASE_URL,
        "models_url": CLOUDROUTER_MODELS_URL,
        "usage_url": CLOUDROUTER_USAGE_URL,
    }
)

MAX_API_RESPONSE_BYTES = 1024 * 1024
MAX_API_KEY_BYTES = MAX_AGENT_API_KEY_BYTES
DEFAULT_HTTP_TIMEOUT = httpx.Timeout(15.0, connect=10.0)
DEFAULT_HTTP_TOTAL_TIMEOUT_SECONDS = 15.0
MAX_USAGE_WINDOWS = 100
MAX_USAGE_MODEL_STATS = 1000
MAX_USAGE_TEXT_LENGTH = 256
MAX_ABS_USAGE_NUMBER = Decimal("1e18")
MIN_ABS_NONZERO_USAGE_NUMBER = Decimal("1e-18")

_ACTIVE_USAGE_STATUSES = frozenset({"active"})
_EXHAUSTED_USAGE_STATUSES = frozenset({"quota_exhausted", "exhausted"})
_EXPIRED_USAGE_STATUSES = frozenset({"expired"})
_UNAVAILABLE_USAGE_STATUSES = frozenset(
    {
        "disabled",
        "error",
        "forbidden",
        "inactive",
        "invalid",
        "revoked",
        "suspended",
    }
)
_MODEL_FINGERPRINT_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def _normalise_model(model: str | None) -> str:
    value = str(model or "").strip()
    if value.endswith("[1m]"):
        value = value[:-4]
    return value


def _agent_type_for_model(model: str) -> str | None:
    value = _normalise_model(model)
    parts = value.split("/")
    if len(parts) == 1:
        bare_model = parts[0]
    elif (
        len(parts) == 2
        and _MODEL_FINGERPRINT_RE.fullmatch(parts[0])
        and parts[1]
    ):
        bare_model = parts[1]
    else:
        return None
    value = bare_model.lower()
    if value.startswith("claude-"):
        return "claude"
    if value.startswith(("gpt-", "o1", "o3", "o4", "codex-")):
        return "codex"
    return None


def _normalise_models(payload: Any) -> dict[str, list[str]]:
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
        raise AgentApiUpstreamError("invalid_models_response")
    models: dict[str, list[str]] = {"claude": [], "codex": []}
    seen: set[str] = set()
    for item in payload["data"]:
        model_id = item.get("id") if isinstance(item, dict) else None
        if not isinstance(model_id, str):
            continue
        model_id = model_id.strip()
        if (
            not model_id
            or len(model_id) > MAX_AGENT_API_MODEL_ID_LENGTH
            or not _is_utf8(model_id)
            or any(not character.isprintable() for character in model_id)
        ):
            raise AgentApiUpstreamError("invalid_models_response")
        if "/" in model_id:
            parts = model_id.split("/")
            if (
                len(parts) != 2
                or not _MODEL_FINGERPRINT_RE.fullmatch(parts[0])
                or not parts[1]
            ):
                raise AgentApiUpstreamError("invalid_models_response")
        agent_type = _agent_type_for_model(model_id)
        if agent_type is None or model_id in seen:
            continue
        if len(seen) >= MAX_AGENT_API_MODELS:
            raise AgentApiUpstreamError("invalid_models_response")
        seen.add(model_id)
        models[agent_type].append(model_id)
    for values in models.values():
        values.sort()
    if not any(models.values()):
        raise AgentApiUpstreamError("no_supported_models")
    return models


def _decimal(value: Any) -> Decimal | None:
    if isinstance(value, bool) or not isinstance(
        value,
        (str, int, float, Decimal),
    ):
        return None
    if isinstance(value, int) and abs(value) > int(MAX_ABS_USAGE_NUMBER):
        return None
    if isinstance(value, str) and (
        len(value) > 64 or value.strip() != value
    ):
        return None
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if (
        not result.is_finite()
        or abs(result) > MAX_ABS_USAGE_NUMBER
        or (
            result != 0
            and abs(result) < MIN_ABS_NONZERO_USAGE_NUMBER
        )
    ):
        return None
    return result


def _json_number(value: Decimal | None) -> float | int | None:
    if value is None:
        return None
    if value == value.to_integral_value():
        return int(value)
    return float(value)


def _number(value: Any) -> float | int | None:
    return _json_number(_decimal(value))


def _is_utf8(value: str) -> bool:
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _safe_text(value: Any, *, maximum: int = MAX_USAGE_TEXT_LENGTH) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if (
        not normalized
        or normalized != value
        or len(normalized) > maximum
        or not _is_utf8(normalized)
        or any(not character.isprintable() for character in normalized)
    ):
        return None
    return normalized


def _usage_metrics(value: Any) -> dict[str, Any] | None:
    """Allowlist display-only usage fields before exposing them over REST."""

    if not isinstance(value, dict):
        return None
    numeric_keys = (
        "requests",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cache_creation_tokens",
        "cache_read_tokens",
        "cost",
        "actual_cost",
        "rpm",
        "tpm",
        "average_duration_ms",
    )
    result = {
        key: parsed
        for key in numeric_keys
        if (parsed := _number(value.get(key))) is not None
    }
    return result or None


def _normalise_usage_metrics(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    result: dict[str, Any] = {}
    for key in ("today", "total"):
        if metrics := _usage_metrics(value.get(key)):
            result[key] = metrics
    for key in ("rpm", "tpm", "average_duration_ms"):
        if (parsed := _number(value.get(key))) is not None:
            result[key] = parsed
    return result or None


def _normalise_model_stats(value: Any) -> list[dict[str, Any]] | None:
    """Project provider model statistics through a bounded scalar allowlist."""

    if not isinstance(value, list):
        return None
    if len(value) > MAX_USAGE_MODEL_STATS:
        raise AgentApiUpstreamError("invalid_usage_response")
    result: list[dict[str, Any]] = []
    numeric_keys = (
        "requests",
        "tokens",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cost",
        "actual_cost",
    )
    for raw in value:
        if not isinstance(raw, dict):
            continue
        item: dict[str, Any] = {}
        if model := _safe_text(
            raw.get("model"),
            maximum=MAX_AGENT_API_MODEL_ID_LENGTH,
        ):
            item["model"] = model
        for key in numeric_keys:
            if (parsed := _number(raw.get(key))) is not None:
                item[key] = parsed
        if item:
            result.append(item)
    return result or None


def _window(
    *,
    window_id: str,
    label: str,
    currency: str,
    raw_used: Any,
    raw_limit: Any,
    raw_remaining: Any = None,
    reset_at: Any = None,
) -> tuple[dict[str, Any], bool]:
    safe_window_id = _safe_text(window_id, maximum=64)
    safe_label = _safe_text(label, maximum=64)
    if safe_window_id is None or safe_label is None:
        raise AgentApiUpstreamError("invalid_usage_response")
    used = _decimal(raw_used)
    limit = _decimal(raw_limit)
    remaining = _decimal(raw_remaining)
    for raw, parsed in (
        (raw_used, used),
        (raw_limit, limit),
        (raw_remaining, remaining),
    ):
        if raw is not None and parsed is None:
            raise AgentApiUpstreamError("invalid_usage_response")
    if (
        (used is not None and used < 0)
        or (limit is not None and limit < 0)
        # A bounded window is actionable only with an explicit remaining
        # value, or with the complete used+limit pair from which remaining can
        # be derived.  A lone used/limit value must not admit an exhausted key.
        or (
            remaining is None
            and (used is None or limit is None)
            and limit != 0
        )
    ):
        raise AgentApiUpstreamError("invalid_usage_response")
    unlimited = remaining == Decimal("-1")
    if remaining is None and used is not None and limit is not None:
        remaining = limit - used
    result: dict[str, Any] = {
        "id": safe_window_id,
        "label": safe_label,
        "currency": currency,
    }
    for key, value in (
        ("used", used),
        ("limit", limit),
        ("remaining", remaining),
    ):
        if (number := _json_number(value)) is not None:
            result[key] = number
    if limit is not None and limit > 0 and used is not None:
        result["utilization"] = float((used / limit) * Decimal(100))
    if reset_at is not None:
        safe_reset = (
            _safe_text(reset_at)
            if isinstance(reset_at, str)
            else _number(reset_at)
        )
        if safe_reset is None:
            raise AgentApiUpstreamError("invalid_usage_response")
        result["reset_at"] = safe_reset
    if unlimited:
        result["unlimited"] = True
    exhausted = bool(
        not unlimited
        and (
            (remaining is not None and remaining <= 0)
            or (
                limit is not None
                and limit >= 0
                and (
                    limit == 0
                    or (used is not None and used >= limit)
                )
            )
        )
    )
    return result, exhausted


def _normalise_usage(account_id: str, payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict) or not payload:
        raise AgentApiUpstreamError("invalid_usage_response")

    raw_valid = payload.get("isValid")
    if raw_valid is not None and not isinstance(raw_valid, bool):
        raise AgentApiUpstreamError("invalid_usage_response")

    raw_status = payload.get("status")
    if raw_status is None or raw_status == "":
        upstream_status = "active"
    elif isinstance(raw_status, str):
        upstream_status = raw_status.strip().lower()
    else:
        raise AgentApiUpstreamError("invalid_usage_response")

    known_statuses = (
        _ACTIVE_USAGE_STATUSES
        | _EXHAUSTED_USAGE_STATUSES
        | _EXPIRED_USAGE_STATUSES
        | _UNAVAILABLE_USAGE_STATUSES
    )
    invalid = raw_valid is False or upstream_status in _UNAVAILABLE_USAGE_STATUSES
    if upstream_status not in known_statuses and not invalid:
        raise AgentApiUpstreamError("invalid_usage_response")

    subscription = payload.get("subscription")
    raw_mode = payload.get("mode")
    if raw_mode is None or raw_mode == "":
        if isinstance(subscription, dict):
            mode = "subscription"
        elif isinstance(payload.get("quota"), dict) or isinstance(
            payload.get("rate_limits"), list
        ):
            mode = "quota_limited"
        elif any(payload.get(key) is not None for key in ("balance", "remaining")):
            mode = "wallet"
        elif invalid or upstream_status != "active":
            mode = "unknown"
        else:
            raise AgentApiUpstreamError("invalid_usage_response")
    elif not isinstance(raw_mode, str):
        raise AgentApiUpstreamError("invalid_usage_response")
    else:
        mode = raw_mode.strip().lower()
        if mode not in {
            "quota_limited",
            "subscription",
            "unrestricted",
            "wallet",
        }:
            if invalid or upstream_status != "active":
                mode = "unknown"
            else:
                raise AgentApiUpstreamError("invalid_usage_response")

    expired = upstream_status in _EXPIRED_USAGE_STATUSES
    exhausted = upstream_status in _EXHAUSTED_USAGE_STATUSES
    if invalid and upstream_status == "active":
        upstream_status = "invalid"

    subscription_uses_usd = False
    if isinstance(subscription, dict):
        recognized_credits = {
            f"{prefix}_{metric}_credits"
            for prefix in ("daily", "weekly", "monthly")
            for metric in ("usage", "limit")
        }
        recognized_usd = {
            f"{prefix}_{metric}_usd"
            for prefix in ("daily", "weekly", "monthly")
            for metric in ("usage", "limit")
        }
        has_credits = any(key in subscription for key in recognized_credits)
        has_usd = any(key in subscription for key in recognized_usd)
        if has_credits and has_usd:
            raise AgentApiUpstreamError("invalid_usage_response")
        subscription_uses_usd = has_usd
    currency = (
        "USD"
        if (
            mode in {"quota_limited", "unrestricted", "wallet"}
            or subscription_uses_usd
        )
        else "credits"
    )

    quota_value = payload.get("quota")
    quota: dict[str, Any] | None = None
    if quota_value is not None and not isinstance(quota_value, dict):
        raise AgentApiUpstreamError("invalid_usage_response")
    if isinstance(quota_value, dict):
        quota_item, quota_exhausted = _window(
            window_id="quota",
            label="quota",
            currency=currency,
            raw_used=quota_value.get("used"),
            raw_limit=quota_value.get("limit"),
            raw_remaining=quota_value.get("remaining"),
        )
        quota = {
            key: value
            for key, value in quota_item.items()
            if key not in {"id", "label"}
        }
        exhausted = exhausted or quota_exhausted

    windows: list[dict[str, Any]] = []
    rate_limits = payload.get("rate_limits")
    if rate_limits is not None and not isinstance(rate_limits, list):
        raise AgentApiUpstreamError("invalid_usage_response")
    if isinstance(rate_limits, list):
        if len(rate_limits) > MAX_USAGE_WINDOWS:
            raise AgentApiUpstreamError("invalid_usage_response")
        for raw in rate_limits:
            if not isinstance(raw, dict):
                raise AgentApiUpstreamError("invalid_usage_response")
            window_id = _safe_text(raw.get("window"), maximum=64)
            if window_id is None:
                raise AgentApiUpstreamError("invalid_usage_response")
            item, item_exhausted = _window(
                window_id=window_id,
                label=window_id,
                currency=currency,
                raw_used=raw.get("used"),
                raw_limit=raw.get("limit"),
                raw_remaining=raw.get("remaining"),
                reset_at=raw.get("reset_at"),
            )
            windows.append(item)
            exhausted = exhausted or item_exhausted

    if subscription is not None and not isinstance(subscription, dict):
        raise AgentApiUpstreamError("invalid_usage_response")
    subscription_window_count = 0
    if isinstance(subscription, dict):
        suffix = "usd" if subscription_uses_usd else "credits"
        # A malformed declared quota field is an invalid response even if a
        # different window happens to be complete.  Silently skipping it can
        # turn an exhausted or corrupt daily window into an active account.
        for prefix in ("daily", "weekly", "monthly"):
            for candidate_suffix in ("credits", "usd"):
                for metric in ("usage", "limit"):
                    key = f"{prefix}_{metric}_{candidate_suffix}"
                    if key in subscription and _decimal(subscription[key]) is None:
                        raise AgentApiUpstreamError("invalid_usage_response")
        for prefix, label in (
            ("daily", "1d"),
            ("weekly", "7d"),
            ("monthly", "30d"),
        ):
            used_key = f"{prefix}_usage_{suffix}"
            limit_key = f"{prefix}_limit_{suffix}"
            if used_key not in subscription and limit_key not in subscription:
                continue
            used = subscription.get(used_key)
            limit = subscription.get(limit_key)
            item, item_exhausted = _window(
                window_id=prefix,
                label=label,
                currency=currency,
                raw_used=used,
                raw_limit=limit,
            )
            windows.append(item)
            subscription_window_count += 1
            exhausted = exhausted or item_exhausted

    balance = _decimal(payload.get("balance"))
    remaining = _decimal(payload.get("remaining"))
    for raw, parsed in (
        (payload.get("balance"), balance),
        (payload.get("remaining"), remaining),
    ):
        if raw is not None and parsed is None:
            raise AgentApiUpstreamError("invalid_usage_response")

    if not invalid and upstream_status == "active":
        scalar_evidence = balance is not None or remaining is not None
        rate_limit_count = len(windows) - subscription_window_count
        if (
            (mode == "wallet" and not scalar_evidence)
            or (
                mode == "subscription"
                and not scalar_evidence
                and subscription_window_count == 0
            )
            or (
                mode == "quota_limited"
                and quota is None
                and rate_limit_count == 0
                and not scalar_evidence
            )
        ):
            raise AgentApiUpstreamError("invalid_usage_response")

    def scalar_depleted(value: Decimal | None) -> bool:
        # CloudRouter uses exactly -1 as the unlimited sentinel.
        return value is not None and value != Decimal("-1") and value <= 0

    # CloudRouter reports spend-cap-free accounts as unrestricted. Their
    # top-level balance/remaining values are informational, commonly both
    # zero, and must not bench the key. Explicit status, expiry, quota, and
    # rate-limit exhaustion above remain authoritative.
    if mode != "unrestricted" and (
        scalar_depleted(balance) or scalar_depleted(remaining)
    ):
        exhausted = True

    expiry_sources = [payload]
    if isinstance(subscription, dict):
        expiry_sources.append(subscription)

    # Providers have emitted several aliases at both the top level and inside
    # ``subscription``. Inspect every declared value: taking only the first
    # alias lets a benign future value mask a simultaneously expired one.
    expiry_candidates: list[tuple[float, str | float | int]] = []
    for source in expiry_sources:
        for key in ("expires_at", "expiry", "expiresAt"):
            raw_expiry = source.get(key)
            if raw_expiry is None:
                continue
            if isinstance(raw_expiry, str):
                normalized_expiry = _safe_text(raw_expiry)
                if normalized_expiry is None:
                    raise AgentApiUpstreamError("invalid_usage_response")
                try:
                    parsed_expiry = datetime.fromisoformat(
                        normalized_expiry.replace("Z", "+00:00")
                    )
                    if parsed_expiry.tzinfo is None:
                        parsed_expiry = parsed_expiry.replace(
                            tzinfo=timezone.utc
                        )
                    expiry_timestamp = parsed_expiry.timestamp()
                except (OSError, OverflowError, ValueError) as exc:
                    raise AgentApiUpstreamError(
                        "invalid_usage_response"
                    ) from exc
                normalized_value: str | float | int = normalized_expiry
            else:
                normalized_number = _number(raw_expiry)
                if normalized_number is None:
                    raise AgentApiUpstreamError("invalid_usage_response")
                normalized_value = normalized_number
                expiry_timestamp = float(normalized_number)
            expiry_candidates.append((expiry_timestamp, normalized_value))

    expires_at: str | float | int | None = None
    if expiry_candidates:
        earliest_timestamp, expires_at = min(
            expiry_candidates,
            key=lambda candidate: candidate[0],
        )
        if earliest_timestamp <= time.time():
            expired = True

    days_candidates: list[Decimal] = []
    for source in expiry_sources:
        for key in ("days_until_expiry", "daysUntilExpiry"):
            raw_days = source.get(key)
            if raw_days is None:
                continue
            normalized_days = _decimal(raw_days)
            if normalized_days is None:
                raise AgentApiUpstreamError("invalid_usage_response")
            days_candidates.append(normalized_days)

    days_until_expiry = (
        _json_number(min(days_candidates))
        if days_candidates
        else None
    )
    if days_until_expiry is not None and days_until_expiry <= 0:
        expired = True

    if invalid:
        state = "unavailable"
    elif expired:
        state = "expired"
    elif exhausted:
        state = "exhausted"
    else:
        state = "active"
    available = state == "active"
    reason = (
        upstream_status
        if not available and upstream_status != "active"
        else state
    )
    snapshot: dict[str, Any] = {
        "account_id": account_id,
        "fetched_at": time.time(),
        "state": state,
        "status": state,
        "stale": False,
        "available": available,
        "known": True,
        "reason": reason,
        "mode": mode,
        "currency": currency,
        "unit": currency,
        "quota": quota,
        "windows": windows,
    }
    if (number := _json_number(balance)) is not None:
        snapshot["balance"] = number
        if balance == Decimal("-1"):
            snapshot["balance_unlimited"] = True
    if (number := _json_number(remaining)) is not None:
        snapshot["remaining"] = number
        if remaining == Decimal("-1"):
            snapshot["remaining_unlimited"] = True

    if expires_at is not None:
        snapshot["expires_at"] = expires_at
    if days_until_expiry is not None:
        snapshot["days_until_expiry"] = days_until_expiry
    plan_name = payload.get("planName", payload.get("plan_name"))
    if plan_name is None and isinstance(subscription, dict):
        plan_name = subscription.get("planName", subscription.get("plan_name"))
    if safe_plan_name := _safe_text(plan_name):
        snapshot["plan_name"] = safe_plan_name

    usage_value = payload.get("usage")
    if usage := _normalise_usage_metrics(usage_value):
        snapshot["usage"] = usage
    raw_model_stats = payload.get("model_stats")
    if raw_model_stats is None and isinstance(usage_value, dict):
        raw_model_stats = usage_value.get("model_stats")
    if model_stats := _normalise_model_stats(raw_model_stats):
        snapshot["model_stats"] = model_stats
    return snapshot


class CloudRouterAdapter:
    """Bounded HTTP adapter for CloudRouter's fixed public API."""

    provider = "cloudrouter"

    def __init__(
        self,
        *,
        http_timeout: httpx.Timeout | float = DEFAULT_HTTP_TIMEOUT,
        total_timeout_seconds: float = DEFAULT_HTTP_TOTAL_TIMEOUT_SECONDS,
    ) -> None:
        if isinstance(total_timeout_seconds, bool):
            raise ValueError("total_timeout_seconds must be positive")
        try:
            total_timeout = float(total_timeout_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "total_timeout_seconds must be positive"
            ) from exc
        if total_timeout <= 0 or not math.isfinite(total_timeout):
            raise ValueError("total_timeout_seconds must be positive")
        self._http_timeout = http_timeout
        self._total_timeout_seconds = total_timeout

    @property
    def endpoints(self) -> Mapping[str, str]:
        return CLOUDROUTER_ENDPOINTS

    @staticmethod
    def _validate_key(api_key: str) -> str:
        try:
            encoded = api_key.encode("utf-8") if isinstance(api_key, str) else b""
        except UnicodeEncodeError:
            encoded = b""
        if (
            not isinstance(api_key, str)
            or not api_key
            or not encoded
            or api_key.strip() != api_key
            or any(not 33 <= ord(character) <= 126 for character in api_key)
            or len(encoded) > MAX_API_KEY_BYTES
        ):
            raise ValueError("Invalid API key")
        return api_key

    async def _request_json(self, url: str, api_key: str) -> Any:
        key = self._validate_key(api_key)
        headers = {
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        }
        try:
            async with asyncio.timeout(self._total_timeout_seconds):
                async with httpx.AsyncClient(
                    timeout=self._http_timeout,
                    follow_redirects=False,
                ) as client:
                    async with client.stream(
                        "GET",
                        url,
                        headers=headers,
                    ) as response:
                        status_code = response.status_code
                        if 300 <= status_code < 400:
                            raise AgentApiUpstreamError(
                                "unexpected_redirect",
                                status_code,
                            )
                        chunks: list[bytes] = []
                        size = 0
                        async for chunk in response.aiter_bytes():
                            size += len(chunk)
                            if size > MAX_API_RESPONSE_BYTES:
                                raise AgentApiUpstreamError(
                                    "response_too_large"
                                )
                            chunks.append(chunk)
        except AgentApiUpstreamError:
            raise
        except (httpx.TimeoutException, TimeoutError) as exc:
            raise AgentApiUpstreamError("timeout") from exc
        except (httpx.RequestError, OSError) as exc:
            raise AgentApiUpstreamError("network_error") from exc

        if status_code == 401:
            raise AgentApiUpstreamError("invalid_api_key", 401)
        if status_code == 403:
            raise AgentApiUpstreamError("forbidden", 403)
        if status_code == 429:
            raise AgentApiUpstreamError("rate_limited", 429)
        if status_code >= 500:
            raise AgentApiUpstreamError("upstream_unavailable", status_code)
        if not 200 <= status_code < 300:
            raise AgentApiUpstreamError("upstream_rejected", status_code)
        try:
            return json.loads(b"".join(chunks).decode("utf-8"))
        except (UnicodeDecodeError, ValueError, RecursionError) as exc:
            raise AgentApiUpstreamError("invalid_json") from exc

    async def probe_models(self, api_key: str) -> dict[str, list[str]]:
        return _normalise_models(
            await self._request_json(CLOUDROUTER_MODELS_URL, api_key)
        )

    async def fetch_usage(
        self,
        account_id: str,
        api_key: str,
    ) -> dict[str, Any]:
        return _normalise_usage(
            account_id,
            await self._request_json(CLOUDROUTER_USAGE_URL, api_key),
        )
