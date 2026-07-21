"""Tests for AccountStore (frontend-editable account pool)."""

from __future__ import annotations

import json
import stat

import pytest

from elastic_agent.core.account_store import AccountStore, AccountStoreCorruptError
from elastic_agent.core.credential_pool import AccountDefinition, AccountsConfig

pytestmark = pytest.mark.asyncio


def _store(tmp_path):
    return AccountStore(str(tmp_path / "accounts.json"))


async def test_empty_initially(tmp_path):
    assert await _store(tmp_path).list() == []


async def test_add_and_list(tmp_path):
    s = _store(tmp_path)
    await s.add(AccountDefinition(id="a", email="a@x.com", email_token="t", group="prod"))
    accts = await s.list()
    assert len(accts) == 1
    assert accts[0].id == "a"
    assert accts[0].group == "prod"


async def test_add_persists_to_disk_compatible_with_pool(tmp_path):
    path = tmp_path / "accounts.json"
    s = AccountStore(str(path))
    await s.add(AccountDefinition(id="a", email="a@x.com"))
    # File is readable as AccountsConfig (same schema CredentialPool loads).
    raw = json.loads(path.read_text())
    cfg = AccountsConfig.model_validate(raw)
    assert cfg.accounts[0].id == "a"
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


async def test_load_tightens_legacy_account_file_permissions(tmp_path):
    path = tmp_path / "accounts.json"
    path.write_text(json.dumps({
        "accounts": [{
            "id": "a", "email": "a@x.com", "email_token": "secret"
        }],
        "groups": {},
    }), encoding="utf-8")
    path.chmod(0o644)

    await AccountStore(str(path)).load()

    assert stat.S_IMODE(path.stat().st_mode) == 0o600


async def test_upsert_replaces_by_id(tmp_path):
    s = _store(tmp_path)
    await s.add(AccountDefinition(id="a", email="one@x.com"))
    await s.add(AccountDefinition(id="a", email="two@x.com"))
    accts = await s.list()
    assert len(accts) == 1
    assert accts[0].email == "two@x.com"


async def test_same_email_cannot_be_registered_under_two_ids(tmp_path):
    s = _store(tmp_path)
    await s.add(AccountDefinition(id="a", email="User@Example.com"))
    with pytest.raises(ValueError, match="already account"):
        await s.add(AccountDefinition(id="b", email="user@example.COM"))
    assert [account.id for account in await s.list()] == ["a"]


async def test_duplicate_email_on_disk_fails_closed(tmp_path):
    path = tmp_path / "accounts.json"
    source = json.dumps({
        "accounts": [
            {"id": "a", "email": "same@example.com"},
            {"id": "b", "email": "SAME@example.com"},
        ],
        "groups": {},
    })
    path.write_text(source, encoding="utf-8")

    with pytest.raises(AccountStoreCorruptError, match="account store is corrupt"):
        await AccountStore(str(path)).load()
    assert path.read_text(encoding="utf-8") == source


async def test_get(tmp_path):
    s = _store(tmp_path)
    await s.add(AccountDefinition(id="a", email="a@x.com"))
    assert (await s.get("a")).email == "a@x.com"
    assert await s.get("missing") is None


async def test_remove(tmp_path):
    s = _store(tmp_path)
    await s.add(AccountDefinition(id="a", email="a@x.com"))
    assert await s.remove("a") is True
    assert await s.list() == []
    assert await s.remove("a") is False


async def test_reload_from_disk(tmp_path):
    path = tmp_path / "accounts.json"
    s1 = AccountStore(str(path))
    await s1.add(AccountDefinition(id="a", email="a@x.com"))
    s2 = AccountStore(str(path))
    await s2.load()
    assert len(await s2.list()) == 1


async def test_invalid_legacy_identity_fails_closed_without_overwrite(tmp_path):
    path = tmp_path / "accounts.json"
    source = json.dumps({
        "accounts": [{"id": " ", "email": "x@example.com", "group": "standard"}],
        "groups": {},
    })
    path.write_text(source, encoding="utf-8")

    with pytest.raises(AccountStoreCorruptError, match="account store is corrupt"):
        await AccountStore(str(path)).load()

    assert path.read_text(encoding="utf-8") == source
