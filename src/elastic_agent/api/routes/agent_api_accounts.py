"""Provider-neutral Agent API account endpoints.

CloudRouter is the only registered provider in this release. API keys are
write-only and are validated against the provider's fixed models endpoint
before a private account directory is published.
"""

from __future__ import annotations

import asyncio
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from elastic_agent.api.auth import require_api_key
from elastic_agent.api.routes.accounts import AccountResponse, _public_account
from elastic_agent.core.agent_api import (
    AgentApiAccountNotFoundError,
    AgentApiDuplicateKeyError,
    AgentApiError,
    AgentApiStorageError,
    AgentApiUnsupportedProviderError,
    AgentApiUpstreamError,
)
from elastic_agent.core.batch_hooks import AccountClaimConflictError

router = APIRouter(
    tags=["agent-api-accounts"],
    dependencies=[Depends(require_api_key)],
)


def _mgr():
    from elastic_agent.api.app import get_manager

    return get_manager()


class AgentApiAccountRequest(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    provider: Literal["cloudrouter"] = "cloudrouter"
    name: str
    api_key: SecretStr = Field(repr=False)
    group: str = "standard"

    @field_validator("name", "group")
    @classmethod
    def require_printable_text(cls, value: str) -> str:
        normalized = value.strip()
        if (
            not normalized
            or len(normalized) > 100
            or any(ord(character) < 32 for character in normalized)
        ):
            raise ValueError("must be 1-100 printable characters")
        return normalized


class AgentApiAccountListResponse(BaseModel):
    accounts: list[AccountResponse]
    total: int


class AgentApiProvidersResponse(BaseModel):
    providers: list[str]


def _safe_upstream_error(exc: AgentApiUpstreamError) -> HTTPException:
    safe_messages = {
        "invalid_api_key": "CloudRouter rejected the API key",
        "forbidden": "CloudRouter denied access for this API key",
        "no_supported_models": (
            "CloudRouter returned no supported Claude or Codex models"
        ),
        "invalid_models_response": "CloudRouter returned an invalid model list",
        "unexpected_redirect": "CloudRouter returned an unexpected redirect",
        "response_too_large": "CloudRouter returned an oversized response",
        "rate_limited": "CloudRouter temporarily rate limited the request",
        "timeout": "CloudRouter request timed out",
        "network_error": "CloudRouter could not be reached",
        "upstream_unavailable": "CloudRouter is temporarily unavailable",
        "upstream_rejected": "CloudRouter rejected the request",
        "invalid_json": "CloudRouter returned an invalid response",
    }
    client_error = exc.status_code in {400, 401, 403, 404}
    return HTTPException(
        422 if client_error or exc.code in {
            "no_supported_models",
            "invalid_models_response",
        } else 502,
        safe_messages.get(exc.code, "CloudRouter request failed"),
    )


async def _account_response(account, *, force_usage: bool) -> AccountResponse:
    store = _mgr().agent_api_store
    usage = await store.fetch_usage(account.id, force=force_usage)
    return _public_account(account, api_usage=usage)


@router.get("/agent-api/providers", response_model=AgentApiProvidersResponse)
async def list_agent_api_providers() -> AgentApiProvidersResponse:
    return AgentApiProvidersResponse(
        providers=list(_mgr().agent_api_store.registry.providers)
    )


@router.get(
    "/agent-api/accounts",
    response_model=AgentApiAccountListResponse,
)
async def list_agent_api_accounts(
    refresh_usage: bool = Query(default=False),
) -> AgentApiAccountListResponse:
    store = _mgr().agent_api_store
    accounts = await store.list()
    responses = await asyncio.gather(
        *(
            _account_response(account, force_usage=refresh_usage)
            for account in accounts
        )
    )
    return AgentApiAccountListResponse(
        accounts=list(responses),
        total=len(responses),
    )


@router.post(
    "/agent-api/accounts",
    response_model=AccountResponse,
    status_code=201,
)
async def add_agent_api_account(
    req: AgentApiAccountRequest,
) -> AccountResponse:
    manager = _mgr()
    store = manager.agent_api_store
    try:
        native_account_ids = {
            account.id for account in await manager.account_store.list()
        }
        bindings, leases = await asyncio.gather(
            manager.binding_manager.list_bindings(),
            manager.binding_manager.list_leases(active_only=False),
        )
        reserved_account_ids = native_account_ids | {
            binding.account_id for binding in bindings
        } | {
            lease.account_id for lease in leases
        }
        account = await store.add(
            req.provider,
            req.name,
            req.api_key.get_secret_value(),
            req.group,
            excluded_ids=reserved_account_ids,
        )
        # Probe usage once at creation so an accepted model key that is already
        # exhausted cannot enter scheduling before the first Job login.
        usage = await store.fetch_usage(account.id, force=True)
        return _public_account(account, api_usage=usage)
    except AgentApiUpstreamError as exc:
        raise _safe_upstream_error(exc) from exc
    except AgentApiDuplicateKeyError as exc:
        raise HTTPException(
            409,
            "CloudRouter API key is already registered",
        ) from exc
    except (ValueError, AgentApiUnsupportedProviderError) as exc:
        raise HTTPException(422, "invalid Agent API account") from exc
    except AgentApiStorageError as exc:
        raise HTTPException(500, "Agent API credential storage failed") from exc


@router.post(
    "/agent-api/accounts/{account_id}/refresh",
    response_model=AccountResponse,
)
async def refresh_agent_api_account(account_id: str) -> AccountResponse:
    manager = _mgr()
    try:
        async with manager.account_allocator.mutation_guard(account_id):
            account = await manager.agent_api_store.refresh(account_id)
            # Keep claims fenced until the successful forced usage probe has
            # atomically cleared any runtime-auth tombstone. A model-only
            # refresh never makes a rejected key schedulable.
            usage = await manager.agent_api_store.fetch_usage(
                account_id,
                force=True,
                allow_model_tombstone_clear=True,
            )
        return _public_account(account, api_usage=usage)
    except AgentApiAccountNotFoundError as exc:
        raise HTTPException(404, f"Agent API account {account_id!r} not found") from exc
    except AgentApiUpstreamError as exc:
        raise _safe_upstream_error(exc) from exc
    except AgentApiStorageError as exc:
        raise HTTPException(500, "Agent API credential storage failed") from exc
    except AccountClaimConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except AgentApiError as exc:
        raise HTTPException(409, "Agent API account refresh was rejected") from exc


@router.get(
    "/agent-api/accounts/{account_id}/usage",
)
async def get_agent_api_usage(
    account_id: str,
    refresh: bool = Query(default=False),
) -> dict:
    try:
        return await _mgr().agent_api_store.fetch_usage(
            account_id,
            force=refresh,
        )
    except AgentApiAccountNotFoundError as exc:
        raise HTTPException(404, f"Agent API account {account_id!r} not found") from exc
    except AgentApiStorageError as exc:
        raise HTTPException(500, "Agent API credential storage failed") from exc


@router.delete("/agent-api/accounts/{account_id}")
async def remove_agent_api_account(account_id: str) -> dict:
    """Keep deletion disabled until every delegated Worker is durably fenced."""

    try:
        account = await _mgr().agent_api_store.get(account_id)
    except AgentApiAccountNotFoundError as exc:
        raise HTTPException(404, f"Agent API account {account_id!r} not found") from exc
    if account is None:
        raise HTTPException(404, f"Agent API account {account_id!r} not found")
    raise HTTPException(
        409,
        "Agent API account deletion is disabled; terminate all delegated "
        "Workers before credential retirement",
    )
