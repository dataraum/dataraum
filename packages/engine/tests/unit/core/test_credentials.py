"""Tests for credential resolution chain."""

from __future__ import annotations

import os

import pytest

from dataraum.core.credentials import (
    CredentialChain,
    EnvProvider,
    ResolvedCredential,
)

# === EnvProvider ===


class TestEnvProvider:
    def test_resolve_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATARAUM_ACCOUNTING_URL", "postgres://u:p@host/db")
        provider = EnvProvider()
        result = provider.resolve("accounting")

        assert result is not None
        assert result.url == "postgres://u:p@host/db"
        assert result.source == "env"

    def test_resolve_missing_env(self) -> None:
        os.environ.pop("DATARAUM_NONEXISTENT_URL", None)
        provider = EnvProvider()
        assert provider.resolve("nonexistent") is None

    def test_env_key_uppercased(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATARAUM_MY_SOURCE_URL", "mysql://u:p@host/db")
        provider = EnvProvider()
        result = provider.resolve("my_source")

        assert result is not None
        assert result.url == "mysql://u:p@host/db"


# === CredentialChain ===


class TestCredentialChain:
    def test_resolves_via_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATARAUM_SRC_URL", "env://url")
        chain = CredentialChain()
        result = chain.resolve("src")

        assert result is not None
        assert result.url == "env://url"
        assert result.source == "env"

    def test_returns_none_when_not_found(self) -> None:
        os.environ.pop("DATARAUM_MISSING_URL", None)
        chain = CredentialChain()
        assert chain.resolve("missing") is None


class TestResolvedCredential:
    def test_frozen(self) -> None:
        cred = ResolvedCredential(url="x://y", source="env")
        with pytest.raises(AttributeError):
            cred.url = "z://w"  # type: ignore[misc]
