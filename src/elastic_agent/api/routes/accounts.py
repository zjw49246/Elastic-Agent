"""Account pool REST API — the frontend's Accounts panel.

Manages account *identities* (email + 接码 token + group). Credentials are never
stored or returned here; they are minted on the worker at login time.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from elastic_agent.api.auth import require_api_key
from elastic_agent.core.credential_pool import AccountDefinition

router = APIRouter(tags=["accounts"], dependencies=[Depends(require_api_key)])


def _mgr():
    from elastic_agent.api.app import get_manager
    return get_manager()


class AccountRequest(BaseModel):
    id: str
    email: str
    email_token: str = ""
    group: str = "standard"
    enabled: bool = True


class AccountListResponse(BaseModel):
    accounts: list[AccountDefinition]
    total: int


@router.get("/accounts", response_model=AccountListResponse)
async def list_accounts() -> AccountListResponse:
    accounts = await _mgr().account_store.list()
    return AccountListResponse(accounts=accounts, total=len(accounts))


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
    except Exception:
        jobs = []
    for job in jobs:
        for wid, run in getattr(job, "runs", {}).items():
            ids = getattr(run, "account_ids", []) or []
            emails = getattr(run, "account_emails", []) or []
            active = getattr(run, "active_slot", 0)
            phase = run.phase.value if hasattr(run.phase, "value") else str(run.phase)
            for i, aid in enumerate(ids):
                if not aid:
                    continue
                out.setdefault(aid, []).append({
                    "job_id": job.job_id,
                    "job_name": getattr(job.spec, "name", ""),
                    "worker_id": wid,
                    "phase": phase,
                    "email": emails[i] if i < len(emails) else "",
                    "active": (i == active),
                })
    return {"allocations": out, "total_accounts_bound": len(out)}


@router.post("/accounts", response_model=AccountDefinition, status_code=201)
async def add_account(req: AccountRequest) -> AccountDefinition:
    defn = AccountDefinition(**req.model_dump())
    return await _mgr().account_store.add(defn)


@router.delete("/accounts/{account_id}")
async def remove_account(account_id: str) -> dict:
    ok = await _mgr().account_store.remove(account_id)
    if not ok:
        raise HTTPException(404, f"Account {account_id} not found")
    return {"account_id": account_id, "status": "removed"}
