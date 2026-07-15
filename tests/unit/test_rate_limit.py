"""Tests for the rate-limit / transient-overload detectors (P2)."""

from __future__ import annotations

import pytest

from elastic_agent.core.rate_limit import (
    is_auth_failure,
    is_rate_limited,
    is_transient_overload,
    rate_limit_event_is_actionable,
    transient_retry_delay,
)


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
