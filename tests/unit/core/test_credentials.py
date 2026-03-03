"""Tests for credential resolution chain."""

from __future__ import annotations

import os
import stat
from pathlib import Path

import pytest
import yaml

from dataraum.core.credentials import (
    CredentialChain,
    EnvProvider,
    FileProvider,
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
        # Ensure the var isn't set
        os.environ.pop("DATARAUM_NONEXISTENT_URL", None)
        provider = EnvProvider()
        assert provider.resolve("nonexistent") is None

    def test_env_key_uppercased(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DATARAUM_MY_SOURCE_URL", "mysql://u:p@host/db")
        provider = EnvProvider()
        result = provider.resolve("my_source")

        assert result is not None
        assert result.url == "mysql://u:p@host/db"


# === FileProvider ===


class TestFileProvider:
    def test_resolve_from_file(self, tmp_path: Path) -> None:
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text(yaml.dump({"sources": {"accounting": "postgres://u:p@host/db"}}))

        provider = FileProvider(cred_file)
        result = provider.resolve("accounting")

        assert result is not None
        assert result.url == "postgres://u:p@host/db"
        assert result.source == "credentials_file"

    def test_resolve_missing_source(self, tmp_path: Path) -> None:
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text(yaml.dump({"sources": {"other": "x://y"}}))

        provider = FileProvider(cred_file)
        assert provider.resolve("accounting") is None

    def test_resolve_no_file(self, tmp_path: Path) -> None:
        provider = FileProvider(tmp_path / "nonexistent.yaml")
        assert provider.resolve("accounting") is None

    def test_resolve_empty_file(self, tmp_path: Path) -> None:
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text("")

        provider = FileProvider(cred_file)
        assert provider.resolve("accounting") is None

    def test_resolve_malformed_sources(self, tmp_path: Path) -> None:
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text(yaml.dump({"sources": "not_a_dict"}))

        provider = FileProvider(cred_file)
        assert provider.resolve("accounting") is None

    def test_resolve_non_string_url(self, tmp_path: Path) -> None:
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text(yaml.dump({"sources": {"accounting": 12345}}))

        provider = FileProvider(cred_file)
        assert provider.resolve("accounting") is None

    def test_permission_warning(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text(yaml.dump({"sources": {"x": "url://val"}}))
        cred_file.chmod(0o644)  # group-readable — should warn

        provider = FileProvider(cred_file)
        with caplog.at_level("WARNING"):
            provider.resolve("x")

        assert "permissions" in caplog.text.lower()


# === CredentialChain ===


class TestCredentialChain:
    def test_env_takes_precedence(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # Set up both env and file
        monkeypatch.setenv("DATARAUM_SRC_URL", "env://url")
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text(yaml.dump({"sources": {"src": "file://url"}}))

        chain = CredentialChain(credentials_dir=tmp_path)
        result = chain.resolve("src")

        assert result is not None
        assert result.url == "env://url"
        assert result.source == "env"

    def test_falls_through_to_file(self, tmp_path: Path) -> None:
        os.environ.pop("DATARAUM_SRC2_URL", None)
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text(yaml.dump({"sources": {"src2": "file://url"}}))

        chain = CredentialChain(credentials_dir=tmp_path)
        result = chain.resolve("src2")

        assert result is not None
        assert result.url == "file://url"
        assert result.source == "credentials_file"

    def test_returns_none_when_not_found(self, tmp_path: Path) -> None:
        os.environ.pop("DATARAUM_MISSING_URL", None)
        chain = CredentialChain(credentials_dir=tmp_path)
        assert chain.resolve("missing") is None

    def test_save_creates_file(self, tmp_path: Path) -> None:
        chain = CredentialChain(credentials_dir=tmp_path)
        path = chain.save("mydb", "postgres://u:p@host/db")

        assert path.exists()
        with open(path) as f:
            config = yaml.safe_load(f)
        assert config["sources"]["mydb"] == "postgres://u:p@host/db"

    def test_save_sets_permissions(self, tmp_path: Path) -> None:
        chain = CredentialChain(credentials_dir=tmp_path)
        path = chain.save("mydb", "x://y")

        file_mode = stat.S_IMODE(path.stat().st_mode)
        assert file_mode == 0o600

    def test_save_preserves_existing(self, tmp_path: Path) -> None:
        cred_file = tmp_path / "credentials.yaml"
        cred_file.write_text(yaml.dump({"sources": {"existing": "x://y"}}))

        chain = CredentialChain(credentials_dir=tmp_path)
        chain.save("new_source", "z://w")

        with open(cred_file) as f:
            config = yaml.safe_load(f)
        assert config["sources"]["existing"] == "x://y"
        assert config["sources"]["new_source"] == "z://w"

    def test_save_creates_dir(self, tmp_path: Path) -> None:
        nested = tmp_path / "subdir" / ".dataraum"
        chain = CredentialChain(credentials_dir=nested)
        path = chain.save("src", "url://val")

        assert nested.exists()
        assert path.exists()

    def test_instructions_for_postgres(self, tmp_path: Path) -> None:
        chain = CredentialChain(credentials_dir=tmp_path)
        instructions = chain.instructions_for("accounting", "postgres")

        assert instructions["ref"] == "accounting"
        assert "postgres://" in instructions["url_template"]
        assert "accounting" in instructions["file_template"]
        assert instructions["env_alternative"] == "DATARAUM_ACCOUNTING_URL"
        assert str(tmp_path) in instructions["file_path"]

    def test_instructions_for_unknown_backend(self, tmp_path: Path) -> None:
        chain = CredentialChain(credentials_dir=tmp_path)
        instructions = chain.instructions_for("src", "clickhouse")

        assert "clickhouse://" in instructions["url_template"]

    def test_credentials_file_property(self, tmp_path: Path) -> None:
        chain = CredentialChain(credentials_dir=tmp_path)
        assert chain.credentials_file == tmp_path / "credentials.yaml"

    def test_default_credentials_dir(self) -> None:
        chain = CredentialChain()
        assert chain.credentials_dir == Path.home() / ".dataraum"


class TestResolvedCredential:
    def test_frozen(self) -> None:
        cred = ResolvedCredential(url="x://y", source="env")
        with pytest.raises(AttributeError):
            cred.url = "z://w"  # type: ignore[misc]
