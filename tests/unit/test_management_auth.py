"""Security and behavior tests for management-user password authentication."""

from __future__ import annotations

import io
import json
import os
import stat
from datetime import UTC, datetime

import pytest
from argon2 import PasswordHasher, Type, extract_parameters

from elastic_agent import management_auth_cli
from elastic_agent.core.management_auth import (
    MAX_STATE_BYTES,
    ManagementAuthConfigurationError,
    ManagementAuthStoreError,
    ManagementPasswordConflictError,
    ManagementUserNotFoundError,
    ManagementUserStore,
    create_password_hasher,
    normalize_email,
)


@pytest.fixture
def fast_hasher() -> PasswordHasher:
    return PasswordHasher(
        time_cost=1,
        memory_cost=32,
        parallelism=1,
        hash_len=16,
        salt_len=8,
        type=Type.ID,
    )


def _store(tmp_path, fast_hasher, **kwargs) -> ManagementUserStore:
    return ManagementUserStore(
        tmp_path / "management-auth" / "users.json",
        password_hasher=fast_hasher,
        **kwargs,
    )


def _write_private(path, value) -> None:
    path.parent.mkdir(mode=0o700)
    path.write_text(json.dumps(value), encoding="utf-8")
    path.chmod(0o600)


def test_production_hasher_uses_required_argon2id_costs():
    hasher = create_password_hasher()
    parameters = extract_parameters(hasher.hash("not-a-real-password"))

    assert parameters.type is Type.ID
    assert parameters.memory_cost == 64 * 1024
    assert parameters.time_cost == 3
    assert parameters.parallelism == 2


@pytest.mark.parametrize(
    "raw",
    ["", "missing-at", "two@@example.com", "a@", "@example.com", "a b@example.com", "a\n@example.com"],
)
def test_normalize_email_rejects_invalid_values_without_echoing_them(raw):
    with pytest.raises(ValueError) as raised:
        normalize_email(raw)

    if raw:
        assert raw not in str(raised.value)


def test_email_is_trimmed_and_casefolded():
    assert normalize_email("  OWNER@Example.Test  ") == "owner@example.test"
    assert normalize_email("Straße@EXAMPLE.COM") == "strasse@example.com"


def test_upsert_persists_only_hash_and_private_permissions(tmp_path, fast_hasher):
    store = _store(tmp_path, fast_hasher)
    user = store.upsert_user(
        " Owner@Example.Test ",
        "temporary-test-passphrase",
        must_change_password=True,
    )

    path = store.state_file
    raw_text = path.read_text(encoding="utf-8")
    document = json.loads(raw_text)
    assert "temporary-test-passphrase" not in raw_text
    assert document["users"][0]["password_hash"].startswith("$argon2id$")
    assert user.email == "owner@example.test"
    assert user.role == "admin"
    assert user.enabled is True
    assert user.must_change_password is True
    assert user.must_change is True
    assert user.password_version == 1
    assert "temporary-test-passphrase" not in repr(user)
    assert document["users"][0]["password_hash"] not in repr(user)
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    assert stat.S_IMODE(path.stat().st_mode) == 0o600


def test_multiple_users_survive_reload_and_lookup_is_case_insensitive(tmp_path, fast_hasher):
    store = _store(tmp_path, fast_hasher)
    store.upsert_user("first@example.com", "first-password")
    store.upsert_user("second@example.com", "second-password")

    reloaded = _store(tmp_path, fast_hasher)
    assert [user.email for user in reloaded.list_users()] == [
        "first@example.com",
        "second@example.com",
    ]
    assert reloaded.get(" SECOND@EXAMPLE.COM ").email == "second@example.com"
    assert reloaded.verify_credentials("FIRST@example.com", "first-password") is not None
    assert reloaded.verify_credentials("second@example.com", "second-password") is not None


def test_upsert_updates_one_user_and_increments_password_version(tmp_path, fast_hasher):
    store = _store(tmp_path, fast_hasher)
    before = store.upsert_user("admin@example.com", "old-password")
    store.upsert_user("other@example.com", "other-password")
    after = store.upsert_user(
        "ADMIN@example.com",
        "new-password",
        must_change_password=True,
    )

    assert before.password_version == 1
    assert after.password_version == 2
    assert after.created_at == before.created_at
    assert after.must_change_password is True
    assert store.verify_credentials("admin@example.com", "old-password") is None
    assert store.verify_credentials("admin@example.com", "new-password") == after
    assert store.verify_credentials("other@example.com", "other-password") is not None


def test_set_password_preserves_identity_and_enabled_state(tmp_path, fast_hasher):
    moments = iter(
        [
            datetime(2026, 8, 1, tzinfo=UTC),
            datetime(2026, 8, 2, tzinfo=UTC),
        ]
    )
    store = _store(tmp_path, fast_hasher, clock=lambda: next(moments))
    before = store.upsert_user("admin@example.com", "old-password", enabled=False)
    after = store.set_password(
        "ADMIN@example.com",
        "new-password",
        must_change_password=True,
    )

    assert after.created_at == before.created_at
    assert after.updated_at > before.updated_at
    assert after.password_changed_at == after.updated_at
    assert after.password_version == 2
    assert after.enabled is False
    assert after.must_change_password is True


def test_set_password_rejects_stale_expected_version(tmp_path, fast_hasher):
    store = _store(tmp_path, fast_hasher)
    original = store.upsert_user("owner@example.test", "first-test-passphrase")
    reset = store.set_password(
        original.email,
        "external-reset-passphrase",
        expected_password_version=original.password_version,
    )

    with pytest.raises(ManagementPasswordConflictError):
        store.set_password(
            original.email,
            "stale-request-passphrase",
            expected_password_version=original.password_version,
        )

    assert store.verify_credentials(original.email, "external-reset-passphrase") == reset
    assert store.verify_credentials(original.email, "stale-request-passphrase") is None
    # Disabled users cannot authenticate even with their new valid password.
    assert store.verify_credentials("admin@example.com", "new-password") is None


def test_set_password_for_missing_user_is_generic(tmp_path, fast_hasher):
    store = _store(tmp_path, fast_hasher)
    with pytest.raises(ManagementUserNotFoundError, match="management user not found") as raised:
        store.set_password("missing@example.com", "new-password")
    assert "missing@example.com" not in str(raised.value)
    assert "new-password" not in str(raised.value)


class _CountingHasher:
    def __init__(self, delegate: PasswordHasher) -> None:
        self.delegate = delegate
        self.verify_count = 0

    def hash(self, password: str) -> str:
        return self.delegate.hash(password)

    def verify(self, password_hash: str, password: str) -> bool:
        self.verify_count += 1
        return self.delegate.verify(password_hash, password)


def test_unknown_invalid_and_wrong_credentials_each_run_one_hash_check(tmp_path, fast_hasher):
    counting = _CountingHasher(fast_hasher)
    store = ManagementUserStore(
        tmp_path / "private" / "users.json",
        password_hasher=counting,  # type: ignore[arg-type]
    )
    store.upsert_user("admin@example.com", "correct-password")

    before = counting.verify_count
    assert store.verify_credentials("missing@example.com", "wrong-password") is None
    assert counting.verify_count == before + 1
    assert store.verify_credentials("not-an-email", "wrong-password") is None
    assert counting.verify_count == before + 2
    assert store.verify_credentials("admin@example.com", "wrong-password") is None
    assert counting.verify_count == before + 3


def test_require_enabled_admin_fails_closed(tmp_path, fast_hasher):
    store = _store(tmp_path, fast_hasher)
    with pytest.raises(ManagementAuthConfigurationError, match="enabled administrator"):
        store.require_enabled_admin()

    store.upsert_user("disabled@example.com", "temporary-test-passphrase", enabled=False)
    with pytest.raises(ManagementAuthConfigurationError, match="enabled administrator"):
        store.require_enabled_admin()

    store.upsert_user("enabled@example.com", "temporary-test-passphrase")
    store.require_enabled_admin()


def test_load_tightens_existing_private_modes(tmp_path, fast_hasher):
    store = _store(tmp_path, fast_hasher)
    store.upsert_user("admin@example.com", "temporary-test-passphrase")
    store.state_file.chmod(0o644)
    store.state_file.parent.chmod(0o755)

    assert _store(tmp_path, fast_hasher).get("admin@example.com") is not None
    assert stat.S_IMODE(store.state_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(store.state_file.parent.stat().st_mode) == 0o700


@pytest.mark.parametrize(
    "mutate",
    [
        lambda document: document.update({"extra": True}),
        lambda document: document.update({"version": 999}),
        lambda document: document["users"].append(dict(document["users"][0])),
        lambda document: document["users"][0].update({"role": "viewer"}),
        lambda document: document["users"][0].update({"password_hash": "plaintext"}),
        lambda document: document["users"][0].update({"password_version": 0}),
        lambda document: document["users"][0].update({"email": "ADMIN@example.com"}),
        lambda document: document["users"][0].update({"unknown": "field"}),
    ],
)
def test_corrupt_or_noncanonical_store_fails_closed(tmp_path, fast_hasher, mutate):
    store = _store(tmp_path, fast_hasher)
    store.upsert_user("admin@example.com", "temporary-test-passphrase")
    document = json.loads(store.state_file.read_text(encoding="utf-8"))
    mutate(document)
    store.state_file.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ManagementAuthStoreError, match="corrupt"):
        _store(tmp_path, fast_hasher).list_users()


def test_invalid_json_fails_closed_without_overwriting(tmp_path, fast_hasher):
    path = tmp_path / "management-auth" / "users.json"
    _write_private(path, {"temporarily": "valid json"})
    original = path.read_bytes()

    with pytest.raises(ManagementAuthStoreError):
        ManagementUserStore(path, password_hasher=fast_hasher).upsert_user(
            "admin@example.com",
            "temporary-test-passphrase",
        )
    assert path.read_bytes() == original


def test_duplicate_json_keys_fail_closed(tmp_path, fast_hasher):
    path = tmp_path / "management-auth" / "users.json"
    path.parent.mkdir(mode=0o700)
    path.write_text('{"version":1,"version":1,"users":[]}', encoding="utf-8")
    path.chmod(0o600)

    with pytest.raises(ManagementAuthStoreError, match="corrupt"):
        ManagementUserStore(path, password_hasher=fast_hasher).list_users()


def test_oversized_store_is_rejected_before_parsing(tmp_path, fast_hasher):
    path = tmp_path / "management-auth" / "users.json"
    path.parent.mkdir(mode=0o700)
    path.write_bytes(b"{" + b" " * MAX_STATE_BYTES + b"}")
    path.chmod(0o600)

    with pytest.raises(ManagementAuthStoreError, match="unsafe|large"):
        ManagementUserStore(path, password_hasher=fast_hasher).list_users()


def test_state_file_symlink_is_rejected_without_touching_target(tmp_path, fast_hasher):
    private = tmp_path / "management-auth"
    private.mkdir(mode=0o700)
    target = tmp_path / "target.json"
    target.write_text("do not replace", encoding="utf-8")
    path = private / "users.json"
    path.symlink_to(target)

    with pytest.raises(ManagementAuthStoreError, match="unsafe"):
        ManagementUserStore(path, password_hasher=fast_hasher).upsert_user(
            "admin@example.com",
            "temporary-test-passphrase",
        )
    assert target.read_text(encoding="utf-8") == "do not replace"
    assert path.is_symlink()


def test_parent_directory_symlink_is_rejected(tmp_path, fast_hasher):
    target = tmp_path / "real-private"
    target.mkdir(mode=0o700)
    linked = tmp_path / "linked-private"
    linked.symlink_to(target, target_is_directory=True)

    with pytest.raises(ManagementAuthStoreError, match="unsafe"):
        ManagementUserStore(linked / "users.json", password_hasher=fast_hasher).list_users()


def test_atomic_write_leaves_no_temporary_files(tmp_path, fast_hasher):
    store = _store(tmp_path, fast_hasher)
    store.upsert_user("admin@example.com", "temporary-test-passphrase")
    store.set_password("admin@example.com", "new-password")

    assert [path.name for path in store.state_file.parent.iterdir()] == ["users.json"]


def test_cli_upsert_reads_password_only_from_stdin_and_never_prints_it(
    tmp_path,
    fast_hasher,
    monkeypatch,
    capsys,
):
    path = tmp_path / "management-auth" / "users.json"
    monkeypatch.setattr(
        management_auth_cli.sys,
        "stdin",
        io.StringIO("temporary-test-passphrase\n"),
    )
    monkeypatch.setattr(
        management_auth_cli,
        "ManagementUserStore",
        lambda state_file: ManagementUserStore(state_file, password_hasher=fast_hasher),
    )

    result = management_auth_cli.main(
        [
            "--state-file",
            os.fspath(path),
            "upsert",
            "--email",
            "OWNER@Example.Test",
            "--password-stdin",
            "--temporary",
        ]
    )

    output = capsys.readouterr()
    assert result == 0
    assert "temporary-test-passphrase" not in output.out
    assert "temporary-test-passphrase" not in output.err
    assert json.loads(output.out) == {
        "email": "owner@example.test",
        "role": "admin",
        "enabled": True,
        "must_change_password": True,
        "password_version": 1,
    }
    assert ManagementUserStore(path, password_hasher=fast_hasher).verify_credentials(
        "owner@example.test",
        "temporary-test-passphrase",
    )


def test_cli_has_no_password_argument_and_does_not_echo_unknown_argument_value(capsys):
    with pytest.raises(SystemExit) as raised:
        management_auth_cli.main(
            [
                "--state-file",
                "/tmp/not-used.json",
                "upsert",
                "--email",
                "admin@example.com",
                "--password",
                "do-not-echo-this",
            ]
        )

    output = capsys.readouterr()
    assert raised.value.code == 2
    assert "do-not-echo-this" not in output.err
    assert "invalid arguments" in output.err


def test_cli_failure_never_prints_password(tmp_path, monkeypatch, capsys):
    secret = "short"
    monkeypatch.setattr(management_auth_cli.sys, "stdin", io.StringIO(secret))

    assert management_auth_cli.main(
        [
            "--state-file",
            os.fspath(tmp_path / "users.json"),
            "upsert",
            "--email",
            "admin@example.com",
            "--password-stdin",
        ]
    ) == 1
    output = capsys.readouterr()
    assert secret not in output.out
    assert secret not in output.err
