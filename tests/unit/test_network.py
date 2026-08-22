"""Tests for provider-aware Manager-to-Worker address selection."""

from types import SimpleNamespace

from elastic_agent.core.network import worker_management_host


def _node(*, public_ip="198.51.100.10", private_ip="10.0.0.10", platform=""):
    return SimpleNamespace(
        public_ip=public_ip,
        private_ip=private_ip,
        platform=platform,
    )


def test_aws_prefers_private_address():
    assert worker_management_host(
        _node(), provider_type="aws",
    ) == "10.0.0.10"


def test_aws_falls_back_to_public_when_private_is_unavailable():
    assert worker_management_host(
        _node(private_ip=None), provider_type="aws",
    ) == "198.51.100.10"


def test_non_aws_preserves_public_first_behavior():
    assert worker_management_host(
        _node(), provider_type="aliyun",
    ) == "198.51.100.10"


def test_platform_inference_supports_legacy_callers_without_config():
    assert worker_management_host(_node(platform="aws")) == "10.0.0.10"
