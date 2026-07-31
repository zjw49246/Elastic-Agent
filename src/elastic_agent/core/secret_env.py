"""Resolve Job environment secret references immediately before dispatch.

Only opaque AWS references are accepted by :class:`RunSpec`; this module is the
sole place that turns them into plaintext.  Values live in the dispatch-local
dictionary and are never written back to JobSpec or its recovery journal.
"""

from __future__ import annotations

import asyncio
import base64
import json
import re
from dataclasses import dataclass
from typing import Literal

_JSON_KEY = re.compile(r"[A-Za-z0-9_.-]{1,256}", re.ASCII)
_AWS_PARTITION = r"aws(?:-[a-z0-9]+)*"
_AWS_REGION = r"[a-z0-9-]{1,64}"
_AWS_ACCOUNT_ID = r"[0-9]{12}"
_SECRETS_MANAGER_NAME = r"[A-Za-z0-9/_+=.@-]{1,512}"
_SECRETS_MANAGER_ARN_RESOURCE = r"[A-Za-z0-9/_+=.@-]{1,519}"
_SECRETS_MANAGER_IDENTIFIER = re.compile(
    rf"(?:{_SECRETS_MANAGER_NAME}|"
    rf"arn:{_AWS_PARTITION}:secretsmanager:{_AWS_REGION}:"
    rf"{_AWS_ACCOUNT_ID}:secret:{_SECRETS_MANAGER_ARN_RESOURCE})",
    re.ASCII,
)
_SSM_PARAMETER_PATH = r"/?[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+){0,14}"
_SSM_PARAMETER_IDENTIFIER = re.compile(
    rf"(?:{_SSM_PARAMETER_PATH}|"
    rf"arn:{_AWS_PARTITION}:ssm:{_AWS_REGION}:{_AWS_ACCOUNT_ID}:"
    rf"parameter/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+){{0,14}})",
    re.ASCII,
)
_SSM_LABEL = re.compile(r"[A-Za-z_.-][A-Za-z0-9_.-]{0,99}", re.ASCII)
_SSM_PARAMETER_IDENTIFIER_MAX_LENGTH = 1_011
_SSM_PARAMETER_REFERENCE_MAX_LENGTH = 2_048
_SSM_VERSION_MAX = (1 << 63) - 1


@dataclass(frozen=True)
class SecretReference:
    provider: Literal["secretsmanager", "ssm"]
    identifier: str
    json_key: str | None = None


def _valid_ssm_identifier(identifier: str) -> bool:
    if (
        len(identifier) > _SSM_PARAMETER_REFERENCE_MAX_LENGTH
        or not identifier
    ):
        return False
    if (
        len(identifier) <= _SSM_PARAMETER_IDENTIFIER_MAX_LENGTH
        and _SSM_PARAMETER_IDENTIFIER.fullmatch(identifier) is not None
    ):
        return True

    base, separator, selector = identifier.rpartition(":")
    if (
        not separator
        or len(base) > _SSM_PARAMETER_IDENTIFIER_MAX_LENGTH
        or _SSM_PARAMETER_IDENTIFIER.fullmatch(base) is None
    ):
        return False
    if selector.isascii() and selector.isdigit():
        return (
            len(selector) <= 19
            and not selector.startswith("0")
            and int(selector) <= _SSM_VERSION_MAX
        )
    return bool(
        _SSM_LABEL.fullmatch(selector)
        and not selector.lower().startswith(("aws", "ssm"))
    )


def parse_secret_reference(reference: str) -> SecretReference:
    """Validate one supported reference without contacting AWS."""
    if not isinstance(reference, str):
        raise ValueError("secret reference must be a string")
    value = reference.strip()
    if (
        not value
        or value != reference
        or any(ord(char) < 0x20 or char.isspace() for char in value)
    ):
        raise ValueError("secret reference is empty or contains whitespace/control characters")

    if value.startswith("aws-secretsmanager://"):
        payload = value.removeprefix("aws-secretsmanager://")
        identifier, separator, json_key = payload.partition("#")
        if (
            _SECRETS_MANAGER_IDENTIFIER.fullmatch(identifier) is None
            or "#" in json_key
        ):
            raise ValueError("invalid aws-secretsmanager secret reference")
        if separator and _JSON_KEY.fullmatch(json_key) is None:
            raise ValueError(
                "Secrets Manager JSON key must contain only letters, digits, '.', '_' or '-'"
            )
        return SecretReference(
            provider="secretsmanager",
            identifier=identifier,
            json_key=json_key if separator else None,
        )

    if value.startswith("aws-ssm://"):
        identifier = value.removeprefix("aws-ssm://")
        if not _valid_ssm_identifier(identifier):
            raise ValueError("invalid aws-ssm parameter reference")
        return SecretReference(provider="ssm", identifier=identifier)

    raise ValueError(
        "secret references must use aws-secretsmanager:// or aws-ssm://"
    )


def _stringify_secret_value(value: object) -> str:
    if isinstance(value, str):
        return value
    if value is None or isinstance(value, (bool, int, float)):
        return json.dumps(value, separators=(",", ":"))
    raise ValueError("selected Secrets Manager JSON value must be a scalar")


def _resolve_sync(
    references: dict[str, str],
    *,
    region_name: str | None = None,
    secrets_client=None,
    ssm_client=None,
) -> dict[str, str]:
    parsed = {name: parse_secret_reference(ref) for name, ref in references.items()}
    needs_secrets = any(ref.provider == "secretsmanager" for ref in parsed.values())
    needs_ssm = any(ref.provider == "ssm" for ref in parsed.values())

    # boto3 is deliberately imported only when a submitted Job actually needs
    # secret resolution, never at model import/startup time.
    if (needs_secrets and secrets_client is None) or (needs_ssm and ssm_client is None):
        import boto3

        if needs_secrets and secrets_client is None:
            secrets_client = boto3.client(
                "secretsmanager", region_name=region_name,
            )
        if needs_ssm and ssm_client is None:
            ssm_client = boto3.client("ssm", region_name=region_name)

    resolved: dict[str, str] = {}
    for name, reference in parsed.items():
        if reference.provider == "secretsmanager":
            response = secrets_client.get_secret_value(SecretId=reference.identifier)
            if "SecretString" in response:
                value = response["SecretString"]
            elif "SecretBinary" in response:
                raw = response["SecretBinary"]
                if isinstance(raw, str):
                    raw = base64.b64decode(raw)
                value = bytes(raw).decode("utf-8")
            else:
                raise ValueError(
                    f"Secrets Manager returned no value for environment key {name!r}"
                )
            if reference.json_key is not None:
                try:
                    payload = json.loads(value)
                except (TypeError, json.JSONDecodeError) as exc:
                    raise ValueError(
                        f"secret for environment key {name!r} is not valid JSON"
                    ) from exc
                if not isinstance(payload, dict) or reference.json_key not in payload:
                    raise ValueError(
                        f"secret JSON key {reference.json_key!r} was not found for {name!r}"
                    )
                value = _stringify_secret_value(payload[reference.json_key])
            resolved[name] = str(value)
        else:
            response = ssm_client.get_parameter(
                Name=reference.identifier,
                WithDecryption=True,
            )
            resolved[name] = str(response["Parameter"]["Value"])
    return resolved


async def resolve_secret_env(
    references: dict[str, str],
    *,
    region_name: str | None = None,
    secrets_client=None,
    ssm_client=None,
) -> dict[str, str]:
    """Resolve references off the event loop and return plaintext env values."""
    if not references:
        return {}
    return await asyncio.to_thread(
        _resolve_sync,
        dict(references),
        region_name=region_name,
        secrets_client=secrets_client,
        ssm_client=ssm_client,
    )
