"""REST bridge for interactive worker-local account-login challenges."""

from __future__ import annotations

import re

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, field_validator

from elastic_agent.api.auth import require_api_key

router = APIRouter(tags=["account-login"], dependencies=[Depends(require_api_key)])


def _coordinator():
    from elastic_agent.api.app import get_manager

    coordinator = get_manager().account_login_coordinator
    if coordinator is None:
        raise HTTPException(503, "Account login coordinator is not configured")
    return coordinator


class OtpSubmission(BaseModel):
    """One-time response to a correlated worker-side OpenAI challenge."""

    challenge_id: str
    code: str = Field(repr=False)

    @field_validator("challenge_id")
    @classmethod
    def require_challenge_id(cls, value: str) -> str:
        normalized = value.strip()
        if not re.fullmatch(r"[0-9a-f]{32}", normalized):
            raise ValueError("challenge_id must be a 32-character lowercase hex id")
        return normalized


@router.get("/accounts/login-attempts")
async def list_account_login_attempts() -> dict:
    challenges = _coordinator().list_otp_challenges()
    return {"attempts": challenges, "total": len(challenges)}


@router.post("/accounts/login-attempts/{login_request_id}/otp")
async def submit_account_login_otp(
    login_request_id: str,
    submission: OtpSubmission,
) -> dict:
    try:
        return await _coordinator().submit_otp(
            login_request_id,
            submission.challenge_id,
            submission.code,
        )
    except KeyError as exc:
        raise HTTPException(404, str(exc)) from exc
    except TimeoutError as exc:
        raise HTTPException(410, str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(409, str(exc)) from exc
