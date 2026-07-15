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
