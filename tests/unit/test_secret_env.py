"""Secret-reference validation and just-in-time AWS resolution."""

from __future__ import annotations

import base64

import pytest

from elastic_agent.core.secret_env import (
    parse_secret_reference,
    resolve_secret_env,
)


class FakeSecrets:
    def __init__(self):
        self.calls = []

    def get_secret_value(self, **kwargs):
        secret_id = kwargs["SecretId"]
        self.calls.append(secret_id)
        values = {
            "plain": {"SecretString": "plain-value"},
            "json": {"SecretString": '{"token":"json-value","n":7}'},
            "binary": {"SecretBinary": base64.b64encode(b"binary-value").decode()},
        }
        return values[secret_id]


class FakeSSM:
    def __init__(self):
        self.calls = []

    def get_parameter(self, **kwargs):
        self.calls.append((kwargs["Name"], kwargs["WithDecryption"]))
        return {"Parameter": {"Value": "parameter-value"}}


def test_parse_reference_accepts_only_supported_opaque_schemes():
    parsed = parse_secret_reference("aws-secretsmanager://prod/service#token")
    assert (parsed.provider, parsed.identifier, parsed.json_key) == (
        "secretsmanager", "prod/service", "token",
    )
    ssm = parse_secret_reference("aws-ssm:///prod/service/token")
    assert (ssm.provider, ssm.identifier, ssm.json_key) == (
        "ssm", "/prod/service/token", None,
    )
    for invalid in (
        "plaintext", "env://TOKEN", "aws-secretsmanager://", "aws-ssm://",
        "aws-ssm:///path#key", "aws-secretsmanager://secret#bad/key",
    ):
        with pytest.raises(ValueError):
            parse_secret_reference(invalid)


@pytest.mark.asyncio
async def test_resolve_secret_env_supports_string_json_binary_and_ssm():
    secrets = FakeSecrets()
    ssm = FakeSSM()
    resolved = await resolve_secret_env({
        "PLAIN": "aws-secretsmanager://plain",
        "JSON": "aws-secretsmanager://json#token",
        "NUMBER": "aws-secretsmanager://json#n",
        "BINARY": "aws-secretsmanager://binary",
        "PARAM": "aws-ssm:///prod/value",
    }, secrets_client=secrets, ssm_client=ssm)

    assert resolved == {
        "PLAIN": "plain-value",
        "JSON": "json-value",
        "NUMBER": "7",
        "BINARY": "binary-value",
        "PARAM": "parameter-value",
    }
    assert ssm.calls == [("/prod/value", True)]


@pytest.mark.asyncio
async def test_missing_json_key_fails_without_returning_other_plaintext():
    with pytest.raises(ValueError, match="was not found"):
        await resolve_secret_env(
            {"TOKEN": "aws-secretsmanager://json#missing"},
            secrets_client=FakeSecrets(),
        )


@pytest.mark.asyncio
async def test_empty_reference_map_needs_no_aws_client():
    assert await resolve_secret_env({}) == {}
