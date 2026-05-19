"""Tests for per-Worker Bearer Token authentication (T-010)."""

from __future__ import annotations

import pytest

from elastic_agent.core.auth import (
    generate_worker_token,
    hash_token,
    verify_token_constant_time,
)


class TestGenerateWorkerToken:
    def test_default_length_produces_nonempty_string(self):
        token = generate_worker_token()
        assert isinstance(token, str)
        assert len(token) > 0

    def test_tokens_are_unique(self):
        tokens = {generate_worker_token() for _ in range(100)}
        assert len(tokens) == 100

    def test_custom_length(self):
        short = generate_worker_token(length=8)
        long = generate_worker_token(length=64)
        assert len(short) < len(long)

    def test_url_safe_characters(self):
        token = generate_worker_token()
        safe_chars = set("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_")
        assert all(c in safe_chars for c in token)


class TestVerifyTokenConstantTime:
    def test_matching_tokens(self):
        assert verify_token_constant_time("abc123", "abc123") is True

    def test_non_matching_tokens(self):
        assert verify_token_constant_time("abc123", "xyz789") is False

    def test_empty_strings(self):
        assert verify_token_constant_time("", "") is True

    def test_one_empty(self):
        assert verify_token_constant_time("", "notempty") is False
        assert verify_token_constant_time("notempty", "") is False

    def test_similar_tokens(self):
        assert verify_token_constant_time("token-a", "token-b") is False

    def test_generated_token_verifies(self):
        token = generate_worker_token()
        assert verify_token_constant_time(token, token) is True
        assert verify_token_constant_time(token, token + "x") is False


class TestHashToken:
    def test_returns_hex_string(self):
        h = hash_token("my-secret-token")
        assert isinstance(h, str)
        assert len(h) == 16
        assert all(c in "0123456789abcdef" for c in h)

    def test_deterministic(self):
        h1 = hash_token("same-token")
        h2 = hash_token("same-token")
        assert h1 == h2

    def test_different_tokens_different_hashes(self):
        h1 = hash_token("token-a")
        h2 = hash_token("token-b")
        assert h1 != h2
