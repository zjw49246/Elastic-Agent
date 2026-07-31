"""Secret-reference validation and just-in-time AWS resolution."""

from __future__ import annotations

import base64
import sys
from types import SimpleNamespace

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
    secret_arn = (
        "arn:aws:secretsmanager:ap-northeast-1:123456789012:"
        "secret:elastic-agent/prod-token-AbCdEf"
    )
    assert parse_secret_reference(
        f"aws-secretsmanager://{secret_arn}#token"
    ).identifier == secret_arn
    longest_secret_arn = (
        "arn:aws:secretsmanager:ap-northeast-1:123456789012:secret:"
        + ("x" * 512)
        + "-AbCdEf"
    )
    assert parse_secret_reference(
        f"aws-secretsmanager://{longest_secret_arn}"
    ).identifier == longest_secret_arn
    ssm_arn = (
        "arn:aws:ssm:ap-northeast-1:123456789012:"
        "parameter/elastic-agent/prod-token"
    )
    assert parse_secret_reference(f"aws-ssm://{ssm_arn}:current").identifier == (
        f"{ssm_arn}:current"
    )
    assert parse_secret_reference("aws-ssm:///prod/service/token:7").identifier == (
        "/prod/service/token:7"
    )
    longest_ssm_name = "/" + ("x" * 1_010)
    assert parse_secret_reference(
        f"aws-ssm://{longest_ssm_name}:current"
    ).identifier == f"{longest_ssm_name}:current"
    for invalid in (
        "plaintext", "env://TOKEN", "aws-secretsmanager://", "aws-ssm://",
        "aws-ssm:///path#key", "aws-secretsmanager://secret#bad/key",
        " aws-secretsmanager://prod/token",
        "aws-secretsmanager://prod/token ",
        "aws-secretsmanager://prod/token?api_key=sk-plaintext",
        "aws-secretsmanager://https://user:plaintext@example/x",
        "aws-secretsmanager://prod/token%3Fapi_key=plaintext",
        "aws-ssm:///prod/token?password=plaintext",
        "aws-ssm://https://user:plaintext@example/x",
        "aws-ssm:///prod/token%3Fpassword=plaintext",
        "aws-ssm:///prod/token:01",
        "aws-ssm:///prod/token:awsCurrent",
        "aws-ssm:///prod/token:ssmCurrent",
        f"aws-secretsmanager://{'x' * 513}",
        f"aws-ssm:///{'x' * 1_011}",
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


@pytest.mark.asyncio
async def test_explicit_region_is_used_when_constructing_aws_clients(monkeypatch):
    calls = []
    secrets = FakeSecrets()
    ssm = FakeSSM()

    def fake_client(service_name, *, region_name):
        calls.append((service_name, region_name))
        return secrets if service_name == "secretsmanager" else ssm

    monkeypatch.setitem(
        sys.modules,
        "boto3",
        SimpleNamespace(client=fake_client),
    )

    resolved = await resolve_secret_env(
        {
            "TOKEN": "aws-secretsmanager://plain",
            "PARAM": "aws-ssm:///prod/value",
        },
        region_name="ap-northeast-1",
    )

    assert resolved == {
        "TOKEN": "plain-value",
        "PARAM": "parameter-value",
    }
    assert calls == [
        ("secretsmanager", "ap-northeast-1"),
        ("ssm", "ap-northeast-1"),
    ]
