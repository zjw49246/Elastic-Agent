"""Durable password authentication for Elastic-Agent management users.

This store is intentionally separate from the agent account pool.  Agent
accounts are credentials delegated to Jobs; management users authenticate
people to the Manager UI/API.  Only Argon2id password hashes are persisted.
"""

from __future__ import annotations

import fcntl
import json
import os
import secrets
import stat
import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Final

from argon2 import PasswordHasher, Type, extract_parameters
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from argon2.low_level import ARGON2_VERSION

STORE_SCHEMA_VERSION: Final = 1
MAX_STATE_BYTES: Final = 1024 * 1024
MAX_USERS: Final = 10_000
MAX_PASSWORD_CHARACTERS: Final = 4096


class ManagementAuthError(RuntimeError):
    """Base class for management authentication failures."""


class ManagementAuthStoreError(ManagementAuthError):
    """The private management-user store is unsafe or corrupt."""


class ManagementAuthConfigurationError(ManagementAuthError):
    """Management authentication is not safely configured."""


class ManagementUserNotFoundError(ManagementAuthError):
    """The requested management user does not exist."""


class ManagementPasswordConflictError(ManagementAuthError):
    """The password changed after the caller authenticated."""


def normalize_email(email: str) -> str:
    """Return the canonical, case-insensitive management-user identity."""

    if not isinstance(email, str) or len(email) > 512:
        raise ValueError("email must be a valid address")
    normalized = email.strip().casefold()
    try:
        normalized.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("email must be a valid address") from exc
    if (
        not normalized
        or len(normalized) > 254
        or normalized.count("@") != 1
        or any(character.isspace() or not character.isprintable() for character in normalized)
    ):
        raise ValueError("email must be a valid address")
    local, domain = normalized.rsplit("@", 1)
    if not local or not domain or domain.startswith(".") or domain.endswith("."):
        raise ValueError("email must be a valid address")
    return normalized


def create_password_hasher() -> PasswordHasher:
    """Create the production Argon2id hasher (64 MiB, time 3, parallelism 2)."""

    return PasswordHasher(
        time_cost=3,
        memory_cost=64 * 1024,
        parallelism=2,
        hash_len=32,
        salt_len=16,
        type=Type.ID,
    )


@dataclass(frozen=True, slots=True)
class ManagementUser:
    """Non-secret management-user metadata safe to return from APIs."""

    email: str
    role: str
    enabled: bool
    must_change_password: bool
    password_version: int
    created_at: str
    updated_at: str
    password_changed_at: str

    @property
    def must_change(self) -> bool:
        """Compatibility shorthand for policy code and UI projections."""

        return self.must_change_password


@dataclass(frozen=True, slots=True)
class _StoredManagementUser:
    email: str
    role: str
    enabled: bool
    must_change_password: bool
    password_version: int
    created_at: str
    updated_at: str
    password_changed_at: str
    password_hash: str = field(repr=False)

    def public(self) -> ManagementUser:
        return ManagementUser(
            email=self.email,
            role=self.role,
            enabled=self.enabled,
            must_change_password=self.must_change_password,
            password_version=self.password_version,
            created_at=self.created_at,
            updated_at=self.updated_at,
            password_changed_at=self.password_changed_at,
        )

    def as_json(self) -> dict[str, Any]:
        return {
            "email": self.email,
            "role": self.role,
            "enabled": self.enabled,
            "must_change_password": self.must_change_password,
            "password_version": self.password_version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "password_changed_at": self.password_changed_at,
            "password_hash": self.password_hash,
        }


_USER_FIELDS: Final = {
    "email",
    "role",
    "enabled",
    "must_change_password",
    "password_version",
    "created_at",
    "updated_at",
    "password_changed_at",
    "password_hash",
}


def _timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("invalid timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("invalid timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError("invalid timestamp")
    parsed = parsed.astimezone(UTC)
    if _timestamp(parsed) != value:
        raise ValueError("invalid timestamp")
    return parsed


def _validate_password(password: str) -> str:
    if (
        not isinstance(password, str)
        or len(password) < 8
        or len(password) > MAX_PASSWORD_CHARACTERS
        or any(not character.isprintable() for character in password)
    ):
        raise ValueError("password must be 8-4096 printable characters")
    return password


def _validate_password_hash(value: Any) -> str:
    if not isinstance(value, str) or len(value) > 2048 or not value.startswith("$argon2id$"):
        raise ValueError("invalid password hash")
    try:
        parameters = extract_parameters(value)
    except InvalidHashError as exc:
        raise ValueError("invalid password hash") from exc
    if (
        parameters.type is not Type.ID
        or parameters.version != ARGON2_VERSION
        or not 1 <= parameters.time_cost <= 10
        or not 8 <= parameters.memory_cost <= 256 * 1024
        or not 1 <= parameters.parallelism <= 16
        or not 8 <= parameters.hash_len <= 128
        or not 8 <= parameters.salt_len <= 128
    ):
        raise ValueError("invalid password hash")
    return value


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    value: dict[str, Any] = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("duplicate JSON field")
        value[key] = item
    return value


def _decode_user(raw: Any) -> _StoredManagementUser:
    if not isinstance(raw, dict) or set(raw) != _USER_FIELDS:
        raise ValueError("invalid user record")
    email = normalize_email(raw["email"])
    if email != raw["email"] or raw["role"] != "admin":
        raise ValueError("invalid user record")
    if not isinstance(raw["enabled"], bool) or not isinstance(raw["must_change_password"], bool):
        raise ValueError("invalid user record")
    password_version = raw["password_version"]
    if isinstance(password_version, bool) or not isinstance(password_version, int) or password_version < 1:
        raise ValueError("invalid user record")
    created = _parse_timestamp(raw["created_at"])
    updated = _parse_timestamp(raw["updated_at"])
    changed = _parse_timestamp(raw["password_changed_at"])
    if created > changed or changed > updated:
        raise ValueError("invalid user record")
    return _StoredManagementUser(
        email=email,
        role="admin",
        enabled=raw["enabled"],
        must_change_password=raw["must_change_password"],
        password_version=password_version,
        created_at=raw["created_at"],
        updated_at=raw["updated_at"],
        password_changed_at=raw["password_changed_at"],
        password_hash=_validate_password_hash(raw["password_hash"]),
    )


class ManagementUserStore:
    """A fail-closed, atomically persisted collection of administrator users."""

    def __init__(
        self,
        state_file: str | Path,
        *,
        password_hasher: PasswordHasher | None = None,
        hasher: PasswordHasher | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if password_hasher is not None and hasher is not None:
            raise ValueError("provide only one password hasher")
        self._path = Path(state_file).expanduser().absolute()
        self._hasher = password_hasher or hasher or create_password_hasher()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._thread_lock = threading.RLock()
        # Missing-user checks still perform one real Argon2 verification.  The
        # random dummy secret and its hash never leave this process.
        self._dummy_hash = self._hasher.hash(secrets.token_urlsafe(32))

    @property
    def state_file(self) -> Path:
        return self._path

    def _check_path_components(self) -> None:
        for candidate in reversed((self._path.parent, *self._path.parent.parents)):
            try:
                metadata = candidate.lstat()
            except FileNotFoundError:
                continue
            except OSError as exc:
                raise ManagementAuthStoreError("management user store is unavailable") from exc
            if stat.S_ISLNK(metadata.st_mode):
                raise ManagementAuthStoreError("management user store path is unsafe")

    def _open_directory(self) -> int:
        self._check_path_components()
        descriptor = -1
        try:
            self._path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
            self._check_path_components()
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
            descriptor = os.open(self._path.parent, flags)
            metadata = os.fstat(descriptor)
            if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.geteuid():
                raise ManagementAuthStoreError("management user store directory is unsafe")
            os.fchmod(descriptor, 0o700)
            return descriptor
        except ManagementAuthStoreError:
            if descriptor >= 0:
                os.close(descriptor)
            raise
        except OSError as exc:
            if descriptor >= 0:
                os.close(descriptor)
            raise ManagementAuthStoreError("management user store is unavailable") from exc

    def _read_users(self, directory_fd: int) -> list[_StoredManagementUser]:
        flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self._path.name, flags, dir_fd=directory_fd)
        except FileNotFoundError:
            return []
        except OSError as exc:
            raise ManagementAuthStoreError("management user store is unsafe") from exc
        try:
            metadata = os.fstat(descriptor)
            if (
                not stat.S_ISREG(metadata.st_mode)
                or metadata.st_uid != os.geteuid()
                or metadata.st_size > MAX_STATE_BYTES
            ):
                raise ManagementAuthStoreError("management user store is unsafe")
            os.fchmod(descriptor, 0o600)
            chunks: list[bytes] = []
            remaining = MAX_STATE_BYTES + 1
            while remaining:
                chunk = os.read(descriptor, min(65536, remaining))
                if not chunk:
                    break
                chunks.append(chunk)
                remaining -= len(chunk)
            payload = b"".join(chunks)
            if len(payload) > MAX_STATE_BYTES:
                raise ManagementAuthStoreError("management user store is too large")
            current = os.stat(self._path.name, dir_fd=directory_fd, follow_symlinks=False)
            if (
                not stat.S_ISREG(current.st_mode)
                or current.st_dev != metadata.st_dev
                or current.st_ino != metadata.st_ino
            ):
                raise ManagementAuthStoreError("management user store changed while reading")
        except ManagementAuthStoreError:
            raise
        except OSError as exc:
            raise ManagementAuthStoreError("management user store is unavailable") from exc
        finally:
            os.close(descriptor)

        try:
            raw = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_json_object)
            if not isinstance(raw, dict) or set(raw) != {"version", "users"}:
                raise ValueError("invalid store")
            if (
                isinstance(raw["version"], bool)
                or not isinstance(raw["version"], int)
                or raw["version"] != STORE_SCHEMA_VERSION
                or not isinstance(raw["users"], list)
            ):
                raise ValueError("invalid store")
            if len(raw["users"]) > MAX_USERS:
                raise ValueError("invalid store")
            users = [_decode_user(item) for item in raw["users"]]
            emails = [user.email for user in users]
            if len(emails) != len(set(emails)):
                raise ValueError("duplicate user")
            return users
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError, TypeError) as exc:
            raise ManagementAuthStoreError("management user store is corrupt") from exc

    def _write_users(self, directory_fd: int, users: list[_StoredManagementUser]) -> None:
        users = sorted(users, key=lambda user: user.email)
        document = {
            "version": STORE_SCHEMA_VERSION,
            "users": [user.as_json() for user in users],
        }
        payload = (json.dumps(document, ensure_ascii=False, indent=2) + "\n").encode("utf-8")
        if len(payload) > MAX_STATE_BYTES:
            raise ManagementAuthStoreError("management user store is too large")
        try:
            existing = os.stat(self._path.name, dir_fd=directory_fd, follow_symlinks=False)
        except FileNotFoundError:
            existing = None
        except OSError as exc:
            raise ManagementAuthStoreError("management user store is unavailable") from exc
        if existing is not None and not stat.S_ISREG(existing.st_mode):
            raise ManagementAuthStoreError("management user store path is unsafe")

        temporary_name = f".{self._path.name}.{uuid.uuid4().hex}.tmp"
        descriptor = -1
        try:
            descriptor = os.open(
                temporary_name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
                dir_fd=directory_fd,
            )
            os.fchmod(descriptor, 0o600)
            view = memoryview(payload)
            while view:
                written = os.write(descriptor, view)
                view = view[written:]
            os.fsync(descriptor)
            os.close(descriptor)
            descriptor = -1
            os.replace(
                temporary_name,
                self._path.name,
                src_dir_fd=directory_fd,
                dst_dir_fd=directory_fd,
            )
            os.fsync(directory_fd)
        except OSError as exc:
            raise ManagementAuthStoreError("unable to persist management user store") from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            try:
                os.unlink(temporary_name, dir_fd=directory_fd)
            except FileNotFoundError:
                pass
            except OSError:
                pass

    def _locked_users(self, *, exclusive: bool) -> tuple[int, list[_StoredManagementUser]]:
        directory_fd = self._open_directory()
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
            return directory_fd, self._read_users(directory_fd)
        except Exception:
            os.close(directory_fd)
            raise

    @staticmethod
    def _close_locked(directory_fd: int) -> None:
        try:
            fcntl.flock(directory_fd, fcntl.LOCK_UN)
        finally:
            os.close(directory_fd)

    def list_users(self) -> list[ManagementUser]:
        with self._thread_lock:
            directory_fd, users = self._locked_users(exclusive=False)
            try:
                return [user.public() for user in users]
            finally:
                self._close_locked(directory_fd)

    def get(self, email: str) -> ManagementUser | None:
        try:
            identity = normalize_email(email)
        except ValueError:
            return None
        with self._thread_lock:
            directory_fd, users = self._locked_users(exclusive=False)
            try:
                stored = next((user for user in users if user.email == identity), None)
                return stored.public() if stored is not None else None
            finally:
                self._close_locked(directory_fd)

    def upsert_user(
        self,
        email: str,
        password: str,
        must_change_password: bool = False,
        *,
        enabled: bool = True,
    ) -> ManagementUser:
        identity = normalize_email(email)
        candidate_password = _validate_password(password)
        if not isinstance(must_change_password, bool) or not isinstance(enabled, bool):
            raise ValueError("invalid management user policy")
        password_hash = self._hasher.hash(candidate_password)
        with self._thread_lock:
            directory_fd, users = self._locked_users(exclusive=True)
            try:
                existing = next((user for user in users if user.email == identity), None)
                now = _timestamp(self._clock())
                if (
                    existing is not None
                    and _parse_timestamp(now) < _parse_timestamp(existing.updated_at)
                ):
                    now = existing.updated_at
                user = _StoredManagementUser(
                    email=identity,
                    role="admin",
                    enabled=enabled,
                    must_change_password=must_change_password,
                    password_version=(existing.password_version + 1 if existing else 1),
                    created_at=existing.created_at if existing else now,
                    updated_at=now,
                    password_changed_at=now,
                    password_hash=password_hash,
                )
                updated = [item for item in users if item.email != identity]
                updated.append(user)
                self._write_users(directory_fd, updated)
                return user.public()
            finally:
                self._close_locked(directory_fd)

    def set_password(
        self,
        email: str,
        new_password: str,
        must_change_password: bool = False,
        *,
        expected_password_version: int | None = None,
    ) -> ManagementUser:
        identity = normalize_email(email)
        candidate_password = _validate_password(new_password)
        if not isinstance(must_change_password, bool):
            raise ValueError("invalid management user policy")
        if (
            expected_password_version is not None
            and (
                isinstance(expected_password_version, bool)
                or not isinstance(expected_password_version, int)
                or expected_password_version < 1
            )
        ):
            raise ValueError("invalid expected password version")
        password_hash = self._hasher.hash(candidate_password)
        with self._thread_lock:
            directory_fd, users = self._locked_users(exclusive=True)
            try:
                existing = next((user for user in users if user.email == identity), None)
                if existing is None:
                    raise ManagementUserNotFoundError("management user not found")
                if (
                    expected_password_version is not None
                    and existing.password_version != expected_password_version
                ):
                    raise ManagementPasswordConflictError(
                        "management password changed concurrently"
                    )
                now = _timestamp(self._clock())
                if _parse_timestamp(now) < _parse_timestamp(existing.updated_at):
                    now = existing.updated_at
                user = _StoredManagementUser(
                    email=existing.email,
                    role=existing.role,
                    enabled=existing.enabled,
                    must_change_password=must_change_password,
                    password_version=existing.password_version + 1,
                    created_at=existing.created_at,
                    updated_at=now,
                    password_changed_at=now,
                    password_hash=password_hash,
                )
                self._write_users(
                    directory_fd,
                    [user if item.email == identity else item for item in users],
                )
                return user.public()
            finally:
                self._close_locked(directory_fd)

    def verify_credentials(self, email: str, password: str) -> ManagementUser | None:
        try:
            identity = normalize_email(email)
        except ValueError:
            identity = ""
        password_valid = (
            isinstance(password, str)
            and 1 <= len(password) <= MAX_PASSWORD_CHARACTERS
            and all(character.isprintable() for character in password)
        )
        supplied_password = password if password_valid else ""

        with self._thread_lock:
            directory_fd, users = self._locked_users(exclusive=False)
            try:
                stored = next((user for user in users if user.email == identity), None)
                password_hash = stored.password_hash if stored is not None else self._dummy_hash
            finally:
                self._close_locked(directory_fd)

        verified = False
        try:
            verified = self._hasher.verify(password_hash, supplied_password)
        except (VerifyMismatchError, VerificationError, InvalidHashError):
            verified = False
        if (
            not verified
            or not password_valid
            or stored is None
            or stored.role != "admin"
            or not stored.enabled
        ):
            return None
        return stored.public()

    def require_enabled_admin(self) -> None:
        with self._thread_lock:
            directory_fd, users = self._locked_users(exclusive=False)
            try:
                if not any(user.role == "admin" and user.enabled for user in users):
                    raise ManagementAuthConfigurationError(
                        "management authentication requires an enabled administrator"
                    )
            finally:
                self._close_locked(directory_fd)
