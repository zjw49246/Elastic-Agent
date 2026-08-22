"""Account pool REST API — the frontend's Accounts panel.

Manages account *identities* and their worker-side login inputs. Passwords and
mailbox tokens are write-only API fields stored mode-0600 and never returned.
OAuth tokens are minted on the worker at login time and never enter this API.
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
)

from elastic_agent.api.auth import require_api_key
from elastic_agent.core.account_binding import AccountBinding, LeaseConflictError
from elastic_agent.core.batch_hooks import AccountClaimConflictError
from elastic_agent.core.credential_pool import (
    MAX_ACCOUNT_EMAIL_LENGTH,
    MAX_ACCOUNT_GROUP_LENGTH,
    AccountDefinition,
    normalize_account_id,
    normalize_account_text,
    validate_account_secret,
)

router = APIRouter(tags=["accounts"], dependencies=[Depends(require_api_key)])
_TERMINAL_ALLOCATION_PHASES = frozenset({"done", "failed", "cancelled"})


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


def _binding_manager():
    """Return the EIP binding service, or a useful error on old deployments."""
    service = getattr(_mgr(), "binding_manager", None)
    if service is None:
        raise HTTPException(503, "Account/EIP binding service is not configured")
    return service


def _account_allocator():
    """Return the claim coordinator shared with batch launch."""

    return _mgr().account_allocator


async def _await_owned_account_mutation(task: asyncio.Task):
    """Settle an irreversible account mutation despite request cancellation."""

    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done() and task.cancelled():
                raise
            continue
    return task.result()


class AccountRequest(BaseModel):
    model_config = ConfigDict(hide_input_in_errors=True)

    id: str
    email: str
    agent_type: Literal["claude", "codex"] = "claude"
    email_token: str = Field(default="", repr=False)
    password: str = Field(default="", repr=False)
    clear_email_token: bool = False
    clear_password: bool = False
    group: str = "standard"
    enabled: bool = True

    @field_validator("id")
    @classmethod
    def safe_id(cls, value: str) -> str:
        return normalize_account_id(value)

    @field_validator("email")
    @classmethod
    def safe_email(cls, value: str) -> str:
        return normalize_account_text(
            value, field="email", maximum=MAX_ACCOUNT_EMAIL_LENGTH,
        )

    @field_validator("group")
    @classmethod
    def safe_group(cls, value: str) -> str:
        return normalize_account_text(
            value, field="group", maximum=MAX_ACCOUNT_GROUP_LENGTH,
        )

    @field_validator("email_token")
    @classmethod
    def safe_email_token(cls, value: str) -> str:
        return validate_account_secret(
            value, field="email_token", strip=True,
        )

    @field_validator("password")
    @classmethod
    def safe_password(cls, value: str) -> str:
        return validate_account_secret(value, field="password")


class AccountListResponse(BaseModel):
    accounts: list["AccountResponse"]
    total: int


class AccountResponse(BaseModel):
    """Public account metadata; login secrets are always write-only."""

    id: str
    email: str
    agent_type: Literal["claude", "codex"] | None = None
    group: str
    enabled: bool
    auth_kind: Literal["oauth", "agent_api"] = "oauth"
    api_provider: str | None = None
    supported_agent_types: list[Literal["claude", "codex"]] = Field(
        default_factory=list
    )
    models: dict[str, list[str]] = Field(default_factory=dict)
    has_api_key: bool = False
    api_usage: dict[str, Any] | None = None
    has_email_token: bool = False
    has_password: bool = False


def _public_account(account: Any, *, api_usage: dict | None = None) -> AccountResponse:
    if getattr(account, "auth_kind", "oauth") == "agent_api":
        return AccountResponse(
            id=account.id,
            email=account.email,
            agent_type=None,
            group=account.group,
            enabled=account.enabled,
            auth_kind="agent_api",
            api_provider=account.api_provider,
            supported_agent_types=list(account.supported_agent_types),
            models={
                agent_type: list(models)
                for agent_type, models in account.models.items()
            },
            has_api_key=bool(account.has_api_key),
            api_usage=api_usage,
        )
    return AccountResponse(
        id=account.id,
        email=account.email,
        agent_type=account.agent_type,
        group=account.group,
        enabled=account.enabled,
        auth_kind="oauth",
        supported_agent_types=[account.agent_type],
        has_email_token=bool(account.email_token),
        has_password=bool(account.password),
    )


async def _get_account_identity(account_id: str) -> Any | None:
    """Resolve one id across the shared OAuth/API claim namespace."""

    manager = _mgr()
    api_store = getattr(manager, "agent_api_store", None)
    if api_store is not None:
        from elastic_agent.core.agent_api import ACCOUNT_ID_RE

        if ACCOUNT_ID_RE.fullmatch(account_id):
            account = await api_store.get(account_id)
            if account is not None:
                return account
    return await manager.account_store.get(account_id)


class AccountBindingListResponse(BaseModel):
    bindings: list[AccountBinding]
    total: int


class EnsureAccountBindingRequest(BaseModel):
    """Optional placement constraint for a newly allocated EIP."""

    region: str = ""


class DecommissionAccountBindingRequest(BaseModel):
    """Double confirmation for the only API that permanently releases an EIP."""

    release_eip: Literal[True]
    confirm_account_id: str
    # Batch UI uses this to close the decommission→identity-delete claim race.
    # API clients may keep the original infrastructure-only behavior.
    delete_identity: bool = False


@router.get("/accounts", response_model=AccountListResponse)
async def list_accounts() -> AccountListResponse:
    manager = _mgr()
    native_accounts = await manager.account_store.list()
    api_store = getattr(manager, "agent_api_store", None)
    api_accounts = await api_store.list() if api_store is not None else []
    accounts = [*api_accounts, *native_accounts]
    if len({account.id for account in accounts}) != len(accounts):
        raise HTTPException(500, "duplicate account ids across credential stores")
    return AccountListResponse(
        accounts=[
            _public_account(
                account,
                api_usage=(
                    api_store.usage_snapshot(account.id)
                    if getattr(account, "auth_kind", "oauth") == "agent_api"
                    else None
                ),
            )
            for account in accounts
        ],
        total=len(accounts),
    )


@router.get("/accounts/allocations")
async def account_allocations() -> dict:
    """Which accounts are currently bound to which worker/job.

    Derived from the orchestrator's in-memory jobs (reflects the current Manager
    session; cleared on restart). Answers "is this account allocated, and where"
    (Q1) and, filtered by worker, "which accounts does this worker have" (Q2).
    Keyed by ``account_id`` → list of bindings (a worker can hold several when
    ``per_worker > 1``; ``active`` flags the one currently driving the run)."""
    mgr = _mgr()
    out: dict[str, list[dict]] = {}
    try:
        jobs = mgr.batch.list_jobs()
    except Exception as exc:
        # Returning an empty map here makes the management UI claim every
        # identity is idle even though the allocator snapshot is unavailable.
        # Fail closed at the observation boundary; mutation_guard remains the
        # authoritative safety fence for actual CRUD/allocation.
        raise HTTPException(
            503, "Account allocation state is temporarily unavailable"
        ) from exc
    live_by_lease: dict[str, tuple[object, str, object]] = {}
    for job in jobs:
        for wid, run in getattr(job, "runs", {}).items():
            if getattr(run, "lease_id", ""):
                live_by_lease[run.lease_id] = (job, wid, run)
                continue
            ids = getattr(run, "account_ids", []) or []
            emails = getattr(run, "account_emails", []) or []
            active = getattr(run, "active_slot", 0)
            phase = run.phase.value if hasattr(run.phase, "value") else str(run.phase)
            terminal = phase in _TERMINAL_ALLOCATION_PHASES
            account_release_proven = (
                bool(getattr(run, "cleaned_up", False))
                or bool(getattr(job, "accounts_released", False))
            )
            if terminal and account_release_proven:
                continue
            for i, aid in enumerate(ids):
                if not aid:
                    continue
                out.setdefault(aid, []).append({
                    "job_id": job.job_id,
                    "job_name": getattr(job.spec, "name", ""),
                    "worker_id": wid,
                    "phase": phase,
                    "email": emails[i] if i < len(emails) else "",
                    "active": not terminal and i == active,
                    "cleanup_pending": terminal,
                })

    # Durable leases are the source of truth for EIP accounts.  They survive a
    # Manager restart, disappear exactly when cleanup commits RELEASED, and do
    # not leave completed historical jobs looking permanently allocated.
    try:
        leases = await _binding_manager().list_leases(active_only=True)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            503, "Durable account allocation state is temporarily unavailable"
        ) from exc
    for lease in leases:
        live = live_by_lease.get(lease.lease_id)
        if live is not None:
            job, wid, run = live
            phase = run.phase.value if hasattr(run.phase, "value") else str(run.phase)
            job_name = getattr(job.spec, "name", "")
            active = phase not in _TERMINAL_ALLOCATION_PHASES
            cleanup_pending = not getattr(run, "cleaned_up", False)
        else:
            wid = lease.worker_id
            phase = lease.state
            job_name = ""
            active = lease.state not in {"releasing", "error"}
            cleanup_pending = True
        out.setdefault(lease.account_id, []).append({
            "job_id": lease.job_id,
            "job_name": job_name,
            "worker_id": wid,
            "phase": phase,
            "email": lease.email,
            "active": active,
            "lease_id": lease.lease_id,
            "generation": lease.generation,
            "cleanup_pending": cleanup_pending,
            "error": lease.error,
        })
    return {"allocations": out, "total_accounts_bound": len(out)}


@router.get("/accounts/bindings", response_model=AccountBindingListResponse)
async def list_account_bindings() -> AccountBindingListResponse:
    """List durable account→EIP mappings, including their availability state."""
    bindings = await _binding_manager().list_bindings()
    return AccountBindingListResponse(bindings=bindings, total=len(bindings))


@router.get("/accounts/{account_id}/binding", response_model=AccountBinding)
async def get_account_binding(account_id: str) -> AccountBinding:
    binding = await _binding_manager().get_binding(account_id)
    if binding is None:
        raise HTTPException(404, f"Account {account_id} has no EIP binding")
    return binding


@router.put("/accounts/{account_id}/binding", response_model=AccountBinding)
async def ensure_account_binding(
    account_id: str,
    req: EnsureAccountBindingRequest | None = None,
) -> AccountBinding:
    """Idempotently allocate the persistent EIP for one configured account."""
    mgr = _mgr()
    try:
        # Keep the account snapshot stable from validation until the durable
        # binding has committed.  This is the same allocator lock used by Job
        # claims and account CRUD; BindingManager takes its own account lock
        # inside ensure_binding, so the global lock order remains allocator ->
        # binding and account_transaction must not be nested here.
        async with _account_allocator().mutation_guard(account_id):
            account = await _get_account_identity(account_id)
            if account is None:
                raise HTTPException(404, f"Account {account_id} not found")
            if not account.enabled:
                raise HTTPException(409, f"Account {account_id} is disabled")

            if mgr.config.provider.type != "aws":
                raise HTTPException(501, "Account/EIP binding currently requires AWS")
            configured_region = mgr.config.provider.aws.region
            requested_region = (req.region if req is not None else "").strip()
            if requested_region and requested_region != configured_region:
                raise HTTPException(
                    409,
                    f"Manager AWS provider is configured for region {configured_region!r}, "
                    f"not {requested_region!r}",
                )
            region = requested_region or configured_region

            return await _binding_manager().ensure_binding(
                account_id,
                email=account.email,
                region=region,
            )
    except AccountClaimConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    except NotImplementedError as exc:
        raise HTTPException(501, str(exc)) from exc
    except (ValueError, LeaseConflictError) as exc:
        # Most commonly an attempt to move an existing regional EIP to a
        # different region. The durable binding is intentionally left intact.
        raise HTTPException(409, str(exc)) from exc


@router.post("/accounts/{account_id}/binding/decommission")
async def decommission_account_binding(
    account_id: str,
    req: DecommissionAccountBindingRequest,
) -> dict:
    """Release an EIP, optionally retiring its identity under the same fence."""
    if req.confirm_account_id != account_id:
        raise HTTPException(400, "confirm_account_id must exactly match account_id")

    identity_removed = False
    try:
        # Reject a live claim and prevent a new claim entering the gap before
        # its durable lease is written. When requested, identity removal remains
        # under this same mutation guard, closing the old two-request window in
        # which a new Job could claim the account after EIP release but before
        # DELETE. decommission() owns the binding account lock itself, so
        # account_transaction must not be nested here.
        async with _account_allocator().mutation_guard(account_id):
            async def retire_under_guard() -> tuple[bool, bool]:
                identity = None
                if req.delete_identity:
                    identity = await _get_account_identity(account_id)
                    if identity is None:
                        raise HTTPException(
                            404, f"Account {account_id} not found"
                        )
                    if (
                        getattr(identity, "auth_kind", "oauth") == "agent_api"
                        and not getattr(
                            _mgr(), "binding_recovery_ready", True
                        )
                    ):
                        raise HTTPException(
                            409,
                            "Agent API identity retirement is blocked while "
                            "startup resource recovery is incomplete",
                        )
                service = _binding_manager()
                active_leases = await service.list_leases(
                    account_id=account_id, active_only=True,
                )
                if active_leases:
                    raise HTTPException(
                        409,
                        f"Account {account_id} has "
                        f"{len(active_leases)} active lease(s); finish the "
                        "owning Job before decommissioning",
                    )
                # This authenticated, double-confirmed endpoint is the only
                # caller allowed to acknowledge a stably missing EIP. Startup
                # recovery keeps confirm_absent=False.
                decommissioned = await service.decommission(
                    account_id, confirm_absent=True
                )
                retired = False
                if decommissioned and identity is not None:
                    try:
                        if (
                            getattr(identity, "auth_kind", "oauth")
                            == "agent_api"
                        ):
                            retired = await _mgr().agent_api_store.remove(
                                account_id
                            )
                        else:
                            retired = await _mgr().account_store.remove(
                                account_id
                            )
                    except BaseException as exc:  # noqa: BLE001
                        # EIP release is irreversible. Keep a failed identity
                        # retirement unavailable in this process and report the
                        # partial state instead of allowing immediate reuse.
                        await _account_allocator().quarantine(account_id)
                        if isinstance(exc, asyncio.CancelledError):
                            raise
                        raise HTTPException(
                            500,
                            "EIP was decommissioned but identity removal "
                            "failed; the account remains quarantined",
                        ) from exc
                    if not retired:
                        await _account_allocator().quarantine(account_id)
                        raise HTTPException(
                            500,
                            "EIP was decommissioned but identity removal was "
                            "not confirmed; the account remains quarantined",
                        )
                    await _account_allocator().clear_quarantine(account_id)
                return decommissioned, retired

            mutation = asyncio.create_task(retire_under_guard())
            removed, identity_removed = await _await_owned_account_mutation(
                mutation
            )
    except (AccountClaimConflictError, LeaseConflictError) as exc:
        # Active leases make a decommission unsafe; BindingManager rejects it.
        raise HTTPException(409, str(exc)) from exc
    if not removed:
        raise HTTPException(404, f"Account {account_id} has no EIP binding")
    return {
        "account_id": account_id,
        "status": (
            "decommissioned_and_removed"
            if identity_removed
            else "decommissioned"
        ),
        "eip_released": True,
        "identity_removed": identity_removed,
    }


@router.post("/accounts", response_model=AccountResponse, status_code=201)
async def add_account(req: AccountRequest) -> AccountResponse:
    manager = _mgr()
    try:
        async with _account_allocator().mutation_guard(req.id):
            # Lock order is allocator -> binding account transaction. Re-read
            # every fact under both locks before changing the identity.
            async with _binding_manager().account_transaction(req.id):
                existing = await manager.account_store.get(req.id)
                api_store = getattr(manager, "agent_api_store", None)
                if api_store is not None and existing is None:
                    reserved_prefixes = tuple(
                        f"{provider}-" for provider in api_store.registry.providers
                    )
                    if req.id.startswith(reserved_prefixes):
                        raise HTTPException(
                            409,
                            "account id uses a reserved Agent API provider prefix",
                        )
                binding = await _binding_manager().get_binding(req.id)
                active_leases = await _binding_manager().list_leases(
                    account_id=req.id, active_only=True,
                )
                incoming = req.model_dump()
                clear_email_token = bool(incoming.pop("clear_email_token"))
                clear_password = bool(incoming.pop("clear_password"))
                if clear_email_token:
                    incoming["email_token"] = ""
                if clear_password:
                    incoming["password"] = ""
                if existing is not None:
                    if "agent_type" not in req.model_fields_set:
                        # Older clients do not know agent_type. Preserve it
                        # instead of silently converting a Codex account.
                        incoming["agent_type"] = existing.agent_type
                    same_agent = incoming["agent_type"] == existing.agent_type
                    # Blank secret fields mean "keep the write-only value" so
                    # metadata can be edited without reading a secret that the
                    # API deliberately never returns. Non-empty values rotate.
                    # A platform change is a new login identity, so it must not
                    # inherit either platform's old secrets.
                    if same_agent:
                        for secret_name in ("email_token", "password"):
                            explicitly_cleared = (
                                (secret_name == "email_token" and clear_email_token)
                                or (secret_name == "password" and clear_password)
                            )
                            if not incoming[secret_name] and not explicitly_cleared:
                                incoming[secret_name] = getattr(
                                    existing, secret_name
                                )
                if (
                    existing is not None
                    and (
                        existing.email.casefold() != req.email.casefold()
                        or existing.agent_type != incoming["agent_type"]
                    )
                    and (binding is not None or active_leases)
                ):
                    raise HTTPException(
                        409,
                        "cannot change the email or agent type of an "
                        "EIP-bound/leased account; "
                        "finish its Job and decommission the binding first",
                    )
                if (
                    incoming["agent_type"] == "codex"
                    and not incoming["email_token"].strip()
                    and not incoming["password"]
                ):
                    # Keep the useful validation error without constructing a
                    # Pydantic ValidationError whose default text can include
                    # other write-only inputs from the request.
                    raise HTTPException(
                        409,
                        "Codex accounts require an email token or OpenAI password",
                    )
                defn = AccountDefinition(**incoming)
                saved = await manager.account_store.add(defn)
    except ValidationError as exc:
        # Pydantic's default ValidationError string includes the rejected input
        # value.  That value can be a write-only mailbox token/password, so the
        # REST error must stay deliberately generic.
        raise HTTPException(409, "invalid account definition") from exc
    except (AccountClaimConflictError, ValueError) as exc:
        raise HTTPException(409, str(exc)) from exc
    return _public_account(saved)


@router.delete("/accounts/{account_id}")
async def remove_account(account_id: str) -> dict:
    # Never turn identity deletion into an implicit infrastructure deletion:
    # doing so would either leak a billable orphan EIP or unexpectedly release
    # the account's stable address. The admin must decommission explicitly.
    manager = _mgr()
    try:
        async with _account_allocator().mutation_guard(account_id):
            async with _binding_manager().account_transaction(account_id):
                binding = await _binding_manager().get_binding(account_id)
                active_leases = await _binding_manager().list_leases(
                    account_id=account_id, active_only=True,
                )
                if binding is not None or active_leases:
                    raise HTTPException(
                        409,
                        f"Account {account_id} still has an EIP binding/lease; "
                        "finish its Job and decommission it first",
                    )
                ok = await manager.account_store.remove(account_id)
                if ok:
                    await _account_allocator().clear_quarantine(account_id)
    except AccountClaimConflictError as exc:
        raise HTTPException(409, str(exc)) from exc
    if not ok:
        raise HTTPException(404, f"Account {account_id} not found")
    return {"account_id": account_id, "status": "removed"}
