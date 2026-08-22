"""Provider-neutral Agent API account endpoints.

API keys are write-only and are validated against each provider's fixed models
endpoint before a private account directory is published.
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

    provider: Literal["cloudrouter", "apex"] = "cloudrouter"
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


def _provider_label(provider: str) -> str:
    return {
        "cloudrouter": "CloudRouter",
        "apex": "ApexRouter",
    }.get(str(provider or "").strip().lower(), "Agent API provider")


def _safe_upstream_error(
    exc: AgentApiUpstreamError,
    *,
    provider: str,
) -> HTTPException:
    label = _provider_label(provider)
    safe_messages = {
        "invalid_api_key": f"{label} rejected the API key",
        "forbidden": f"{label} denied access for this API key",
        "no_supported_models": f"{label} returned no supported agent models",
        "invalid_models_response": f"{label} returned an invalid model list",
        "unexpected_redirect": f"{label} returned an unexpected redirect",
        "response_too_large": f"{label} returned an oversized response",
        "rate_limited": f"{label} temporarily rate limited the request",
        "timeout": f"{label} request timed out",
        "network_error": f"{label} could not be reached",
        "upstream_unavailable": f"{label} is temporarily unavailable",
        "upstream_rejected": f"{label} rejected the request",
        "invalid_json": f"{label} returned an invalid response",
    }
    client_error = exc.status_code in {400, 401, 403, 404}
    return HTTPException(
        422 if client_error or exc.code in {
            "no_supported_models",
            "invalid_models_response",
        } else 502,
        safe_messages.get(exc.code, f"{label} request failed"),
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
        raise _safe_upstream_error(exc, provider=req.provider) from exc
    except AgentApiDuplicateKeyError as exc:
        raise HTTPException(
            409,
            f"{_provider_label(req.provider)} API key is already registered",
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
    provider = ""
    try:
        async with manager.account_allocator.mutation_guard(account_id):
            current = await manager.agent_api_store.get(account_id)
            if current is None:
                raise AgentApiAccountNotFoundError(
                    f"Agent API account {account_id!r} not found"
                )
            provider = current.api_provider
            account = await manager.agent_api_store.refresh(account_id)
            # Keep claims fenced until the successful forced usage probe has
            # atomically cleared any runtime-auth tombstone. A model-only
            # refresh never makes a rejected key schedulable.
            usage = await manager.agent_api_store.fetch_usage(
                account_id,
                force=True,
                allow_model_tombstone_clear=True,
            )
            if usage.get("available") is True:
                await manager.account_allocator.clear_quarantine(account_id)
        return _public_account(account, api_usage=usage)
    except AgentApiAccountNotFoundError as exc:
        raise HTTPException(404, f"Agent API account {account_id!r} not found") from exc
    except AgentApiUpstreamError as exc:
        raise _safe_upstream_error(exc, provider=provider) from exc
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
    """Retire one key only after every delegated Worker is fenced."""

    manager = _mgr()
    if not getattr(manager, "binding_recovery_ready", True):
        raise HTTPException(
            409,
            "Agent API account deletion is blocked while startup resource "
            "recovery is incomplete",
        )
    try:
        async with manager.account_allocator.mutation_guard(account_id):
            # Keep the identity, durable binding, and lease snapshots stable
            # through the same-directory tombstone rename. Lifecycle cleanup
            # releases an allocator reference only after its ordinary Worker
            # has been confirmed terminated, so an empty claim set proves no
            # live unbound Worker still has delegated access.
            async with manager.binding_manager.account_transaction(account_id):
                account = await manager.agent_api_store.get(account_id)
                if account is None:
                    raise AgentApiAccountNotFoundError(
                        f"Agent API account {account_id!r} not found"
                    )
                binding = await manager.binding_manager.get_binding(account_id)
                active_leases = await manager.binding_manager.list_leases(
                    account_id=account_id,
                    active_only=True,
                )
                if binding is not None or active_leases:
                    raise HTTPException(
                        409,
                        f"Agent API account {account_id} still has an EIP "
                        "binding/lease; finish its Job and decommission it first",
                    )
                removed = await manager.agent_api_store.remove(account_id)
                if removed:
                    await manager.account_allocator.clear_quarantine(account_id)
    except AgentApiAccountNotFoundError as exc:
        raise HTTPException(404, f"Agent API account {account_id!r} not found") from exc
    except AccountClaimConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except AgentApiStorageError as exc:
        raise HTTPException(
            500, "Agent API credential storage failed"
        ) from exc
    if not removed:
        raise HTTPException(404, f"Agent API account {account_id!r} not found")
    return {"account_id": account_id, "status": "removed"}
