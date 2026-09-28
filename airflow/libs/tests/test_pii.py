"""Pseudonimizacao: determinismo e dependencia do salt."""

from __future__ import annotations

import pytest

from dataops_core.security import hash_value, salt_configured
from dataops_core.security.pii import SALT_ENV


@pytest.fixture
def salt(monkeypatch):
    monkeypatch.setenv(SALT_ENV, "salt-de-teste")


def test_hash_is_deterministic(salt):
    assert hash_value("898001160125335") == hash_value("898001160125335")


def test_hash_ignores_surrounding_spaces(salt):
    assert hash_value("  12345  ") == hash_value("12345")


def test_hash_changes_with_salt(monkeypatch):
    monkeypatch.setenv(SALT_ENV, "salt-a")
    primeiro = hash_value("12345")
    monkeypatch.setenv(SALT_ENV, "salt-b")
    assert hash_value("12345") != primeiro


def test_missing_value_is_not_hashed(salt):
    assert hash_value(None) is None
    assert hash_value("   ") is None


def test_salt_configured_reflects_environment(monkeypatch):
    monkeypatch.delenv(SALT_ENV, raising=False)
    assert salt_configured() is False
    monkeypatch.setenv(SALT_ENV, "x")
    assert salt_configured() is True


def test_hash_length_is_sha256(salt):
    assert len(hash_value("12345")) == 64
