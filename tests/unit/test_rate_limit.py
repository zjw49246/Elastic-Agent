"""Tests for the rate-limit / transient-overload detectors (P2)."""

from __future__ import annotations

import pytest

from elastic_agent.core.rate_limit import (
    is_apexrouter_auth_failure,
    is_apexrouter_hard_limit,
    is_apexrouter_transient,
    is_auth_failure,
    is_cloudrouter_auth_failure,
    is_cloudrouter_hard_limit,
    is_cloudrouter_transient,
    is_rate_limited,
    is_transient_overload,
    rate_limit_event_is_actionable,
    transient_retry_delay,
)


class TestApexRouterClassification:
    @pytest.mark.parametrize("text", [
        (
            '{"type":"turn.failed","error":{"message":"unexpected status '
            '401 Unauthorized: Invalid API key"}}'
        ),
        (
            '{"type":"turn.failed","error":{"status":403,'
            '"message":"Forbidden"}}'
        ),
        (
            '{"type":"error","error":{"type":"authentication_error",'
            '"message":"credentials rejected"}}'
        ),
    ])
    def test_auth_failure(self, text):
        assert is_apexrouter_auth_failure(text)
        assert not is_apexrouter_hard_limit(text)
        assert not is_apexrouter_transient(text)

    @pytest.mark.parametrize("text", [
        (
            '{"type":"turn.failed","error":{"message":"unexpected status '
            '429 Too Many Requests"}}'
        ),
        (
            '{"type":"error","error":{"type":"rate_limit_error",'
            '"message":"request rate limited"}}'
        ),
        (
            '{"type":"turn.failed","error":{"status":500,'
            '"message":"Internal Server Error"}}'
        ),
        (
            '{"type":"turn.failed","error":{"message":"unexpected status '
            '502 Bad Gateway: upstream_error"}}'
        ),
    ])
    def test_shared_gateway_conditions_are_transient(self, text):
        assert is_apexrouter_transient(text)
        assert not is_apexrouter_auth_failure(text)
        assert not is_apexrouter_hard_limit(text)

    @pytest.mark.parametrize("text", [
        (
            '{"type":"turn.failed","error":{"code":"insufficient_quota",'
            '"message":"account quota exhausted"}}'
        ),
        (
            '{"type":"turn.failed","error":{"message":'
            '"ApexRouter API key is out of credits"}}'
        ),
        (
            '{"type":"turn.failed","error":{"message":'
            '"monthly spend limit reached"}}'
        ),
        (
            '{"type":"turn.failed","error":{"status":403,'
            '"message":"insufficient balance"}}'
        ),
        (
            '{"type":"turn.failed","error":{"status":403,'
            '"message":"account balance is too low"}}'
        ),
    ])
    def test_explicit_key_quota_is_hard(self, text):
        assert is_apexrouter_hard_limit(text)
        assert not is_apexrouter_auth_failure(text)
        assert not is_apexrouter_transient(text)

    def test_narrow_negative_cases(self):
        assert not is_apexrouter_auth_failure("application returned 403 rows")
        assert not is_apexrouter_auth_failure(
            "HTTP 403 from unrelated dataset endpoint"
        )
        assert not is_apexrouter_auth_failure(
            "Error: processed 403 records"
        )
        assert not is_apexrouter_transient("processed 429 records")
        assert not is_apexrouter_transient(
            "HTTP Error: 429 from unrelated telemetry service"
        )
        assert not is_apexrouter_hard_limit(
            "The documentation describes insufficient_quota errors."
        )
        assert not is_apexrouter_auth_failure(
            '{"type":"turn.completed","result":"HTTP 401 Unauthorized"}'
        )
        assert not is_apexrouter_transient(
            '{"type":"turn.completed","result":"HTTP 429 Too Many Requests"}'
        )

    def test_plain_fallback_requires_apex_identity(self):
        assert is_apexrouter_auth_failure(
            "ApexRouter API: HTTP 403 Forbidden"
        )
        assert is_apexrouter_auth_failure(
            "https://api.apexin.ai/v1/responses: HTTP 403 Forbidden"
        )
        assert not is_apexrouter_auth_failure(
            "HTTP 403 from unrelated dataset endpoint"
        )
        assert is_apexrouter_transient(
            "Apex gateway: HTTP 429 Too Many Requests"
        )
        assert is_apexrouter_transient(
            "https://api.apexin.ai/v1/responses: HTTP 429 Too Many Requests"
        )
        assert is_apexrouter_hard_limit(
            "ApexRouter API key is out of credits"
        )
        assert is_apexrouter_hard_limit("ApexRouter: insufficient balance")


class TestCloudRouterClassification:
    @pytest.mark.parametrize("text", [
        "HTTP 401 Unauthorized: invalid API key",
        '{"type":"error","error":{"type":"authentication_error"}}',
        (
            '{"type":"assistant","isApiErrorMessage":true,'
            '"message":{"content":[{"type":"text","text":'
            '"API Error: HTTP 401 Unauthorized: invalid API key"}]}}'
        ),
        (
            '{"type":"assistant","isApiErrorMessage":true,'
            '"error":"authentication_error"}'
        ),
        (
            '{"type":"result","subtype":"error","is_error":true,'
            '"api_error_status":401,"result":"API request failed"}'
        ),
        (
            '{"type":"error","message":"Reconnecting... '
            '(unexpected status 401 Unauthorized: Invalid API key, '
            'url: https://console.cloudrouter.online/v1/responses)"}'
        ),
        (
            '{"type":"turn.failed","error":{"message":"unexpected status '
            '401 Unauthorized: Invalid API key, url: '
            'https://console.cloudrouter.online/v1/responses"}}'
        ),
    ])
    def test_auth_failure(self, text):
        assert is_cloudrouter_auth_failure(text)

    @pytest.mark.parametrize("text", [
        "API Error: HTTP 502 upstream error",
        (
            '{"type":"turn.failed","error":{"type":"upstream_error",'
            '"message":"CloudRouter upstream failed"}}'
        ),
        (
            '{"type":"result","is_error":true,'
            '"api_error_status":502,"result":"API request failed"}'
        ),
        (
            '{"type":"turn.failed","error":{"message":"unexpected status '
            '502 Bad Gateway: upstream_error"}}'
        ),
        (
            '{"type":"turn.failed","error":{"message":"unexpected status '
            '500 Internal Server Error"}}'
        ),
    ])
    def test_gateway_transient(self, text):
        assert is_cloudrouter_transient(text)

    @pytest.mark.parametrize("text", [
        "API Error: HTTP 429 too many requests",
        "status code 403 forbidden",
        (
            '{"type":"result","subtype":"error","is_error":true,'
            '"api_error_status":429,"result":"API request failed"}'
        ),
        (
            '{"type":"result","subtype":"error","is_error":true,'
            '"api_error_status":403,"result":"API request failed"}'
        ),
        (
            '{"type":"turn.failed","error":{"code":'
            '"API_KEY_RATE_5H_EXCEEDED","message":"quota exhausted"}}'
        ),
        (
            '{"type":"turn.failed","error":{"message":"exceeded retry '
            'limit, last status: 429 Too Many Requests"}}'
        ),
    ])
    def test_gateway_hard_limit(self, text):
        assert is_cloudrouter_hard_limit(text)
        assert not is_cloudrouter_transient(text)

    def test_narrow_negative_cases(self):
        assert not is_cloudrouter_auth_failure("application returned 401 rows")
        assert not is_cloudrouter_transient("processed 429 records")
        assert not is_cloudrouter_auth_failure(
            "The operation is forbidden by policy."
        )
        assert not is_cloudrouter_auth_failure(
            "authentication_error is an API error type we document."
        )
        assert not is_cloudrouter_transient(
            "Experiment notes: HTTP 429 errors should be retried."
        )
        assert not is_cloudrouter_auth_failure(
            '{"type":"assistant","message":{"content":[{"type":"text",'
            '"text":"API Error: HTTP 401 Unauthorized: invalid API key"}]}}'
        )
        assert not is_cloudrouter_transient(
            '{"type":"result","result":"API Error: HTTP 429 too many requests"}'
        )
        # CloudRouter's documented retryable platform statuses are 500/502.
        # Do not silently widen that contract to an undocumented 503.
        assert not is_cloudrouter_transient(
            '{"type":"turn.failed","error":{"message":'
            '"unexpected status 503 Service Unavailable"}}'
        )

    def test_pathological_json_is_not_a_provider_signal(self):
        text = (
            '{"type":"turn.failed","error":{"code":'
            + ("9" * 5000)
            + "}}"
        )

        assert not is_cloudrouter_auth_failure(text)
        assert not is_cloudrouter_hard_limit(text)
        assert not is_cloudrouter_transient(text)


class TestIsRateLimited:
    @pytest.mark.parametrize("text", [
        "You hit your session limit",
        "You hit your weekly limit",
        "usage limit reached",
        "Session limit reached",
        "resets 5pm (America/New_York)",
        "resets 5:50pm (UTC)",
        "Your organization has been disabled",
        "account has been disabled",
        "当前限速，请稍后再试",
    ])
    def test_positive(self, text):
        assert is_rate_limited(text)

    @pytest.mark.parametrize("text", [
        "", None, "everything is fine", "wrote 3 files",
        "Server is temporarily limiting requests",
    ])
    def test_negative(self, text):
        assert not is_rate_limited(text)


class TestIsAuthFailure:
    @pytest.mark.parametrize("text", [
        "You are not logged in", "please run /login",
        "not authenticated", "failed to authenticate",
    ])
    def test_positive(self, text):
        assert is_auth_failure(text)

    def test_negative(self):
        assert not is_auth_failure("usage limit reached")
        assert not is_auth_failure(None)


class TestIsTransientOverload:
    @pytest.mark.parametrize("text", [
        "Server is temporarily limiting requests (not your usage limit)",
        "API Error: overloaded_error",
        "the api overloaded, retry later",
    ])
    def test_positive(self, text):
        assert is_transient_overload(text)

    def test_usage_limit_takes_precedence(self):
        # A usage-limit banner must never be treated as transient overload,
        # otherwise it would enter a same-account retry loop instead of rotating.
        assert not is_transient_overload("usage limit reached; overloaded_error")

    def test_auth_failure_takes_precedence(self):
        assert not is_transient_overload("not logged in — overloaded_error")

    def test_negative(self):
        assert not is_transient_overload("")
        assert not is_transient_overload(None)
        assert not is_transient_overload("normal output")


class TestRateLimitEventActionable:
    def test_allowed_never_actionable(self):
        assert not rate_limit_event_is_actionable({"status": "allowed"})

    def test_five_hour_warning_high_util_actionable(self):
        assert rate_limit_event_is_actionable({
            "status": "allowed_warning", "rateLimitType": "five_hour", "utilization": 0.95,
        })

    def test_five_hour_warning_low_util_not_actionable(self):
        assert not rate_limit_event_is_actionable({
            "status": "allowed_warning", "rateLimitType": "five_hour", "utilization": 0.5,
        })

    def test_seven_day_warning_never_actionable(self):
        assert not rate_limit_event_is_actionable({
            "status": "allowed_warning", "rateLimitType": "seven_day", "utilization": 0.99,
        })

    def test_surpassed_threshold_fallback(self):
        assert rate_limit_event_is_actionable({
            "status": "allowed_warning", "rateLimitType": "five_hour", "surpassedThreshold": 0.92,
        })

    def test_rejected_actionable(self):
        assert rate_limit_event_is_actionable({"status": "rejected"})

    def test_non_dict_not_actionable(self):
        assert not rate_limit_event_is_actionable(None)
        assert not rate_limit_event_is_actionable("allowed")


class TestTransientRetryDelay:
    def test_exponential_growth_capped(self):
        # base=10, cap=120 → 10, 20, 40, 80, 120(cap), 120(cap)
        for attempt, expected in [(1, 10), (2, 20), (3, 40), (4, 80), (5, 120), (6, 120)]:
            d = transient_retry_delay(attempt, base=10, cap=120)
            # ±20% jitter around the capped value
            assert expected * 0.8 - 0.01 <= d <= expected * 1.2 + 0.01

    def test_minimum_one_second(self):
        assert transient_retry_delay(1, base=0.1, cap=1) >= 1.0

    def test_attempt_floored_at_one(self):
        assert transient_retry_delay(0, base=10, cap=120) >= 1.0
