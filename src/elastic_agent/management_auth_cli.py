"""Administrative CLI for bootstrapping management-user passwords.

Passwords are accepted only through stdin so they cannot appear in process
arguments, shell history, or service-manager command lines.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence

from elastic_agent.core.management_auth import (
    MAX_PASSWORD_CHARACTERS,
    ManagementAuthError,
    ManagementUserStore,
)


class _PrivateArgumentParser(argparse.ArgumentParser):
    """Avoid reflecting an accidentally supplied secret argument in errors."""

    def error(self, message: str) -> None:  # noqa: ARG002 - argparse callback
        self.print_usage(sys.stderr)
        self.exit(2, f"{self.prog}: error: invalid arguments\n")


def _parser() -> argparse.ArgumentParser:
    parser = _PrivateArgumentParser(
        prog="python -m elastic_agent.management_auth_cli",
        description="Manage Elastic-Agent administrator accounts.",
    )
    parser.add_argument(
        "--state-file",
        required=True,
        help="Path to the private management-users JSON file.",
    )
    commands = parser.add_subparsers(
        dest="command",
        required=True,
        parser_class=_PrivateArgumentParser,
    )
    upsert = commands.add_parser("upsert", help="Create or replace an administrator password.")
    upsert.add_argument("--email", required=True)
    upsert.add_argument(
        "--password-stdin",
        action="store_true",
        required=True,
        help="Read the password from stdin (the only supported password input).",
    )
    upsert.add_argument(
        "--temporary",
        action="store_true",
        help="Require a password change after the first successful login.",
    )
    return parser


def _read_password_stdin() -> str:
    # Read one extra character so oversized input is rejected without keeping
    # an unbounded secret in memory.  Remove only the conventional pipe/TTY
    # line terminator; all other whitespace remains part of the password.
    password = sys.stdin.read(MAX_PASSWORD_CHARACTERS + 2)
    if len(password) > MAX_PASSWORD_CHARACTERS + 1:
        raise ValueError("password input is invalid")
    if password.endswith("\n"):
        password = password[:-1]
        if password.endswith("\r"):
            password = password[:-1]
    return password


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command != "upsert":  # pragma: no cover - argparse is exhaustive
            parser.error("unsupported command")
        password = _read_password_stdin()
        store = ManagementUserStore(args.state_file)
        user = store.upsert_user(
            args.email,
            password,
            must_change_password=args.temporary,
        )
    except (ManagementAuthError, ValueError):
        # Never include the supplied password or hashing-library exception text.
        print("unable to update management user", file=sys.stderr)
        return 1
    finally:
        if "password" in locals():
            password = ""  # best-effort removal of the direct local reference

    print(
        json.dumps(
            {
                "email": user.email,
                "role": user.role,
                "enabled": user.enabled,
                "must_change_password": user.must_change_password,
                "password_version": user.password_version,
            },
            separators=(",", ":"),
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
