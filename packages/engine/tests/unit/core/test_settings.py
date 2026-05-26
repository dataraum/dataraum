"""Tests for typed application settings (DAT-363).

The contract under test: required vars fail loud at boot naming the field,
defaulted vars fall back, and the slice-1 Temporal vars are optional.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from pydantic import ValidationError

from dataraum.core.settings import get_settings, reset_settings

_REQUIRED_ENV = {
    "DATABASE_URL": "postgresql+psycopg://u:p@localhost:5432/db",
    "DUCKLAKE_CATALOG_URL": "postgresql://u:p@localhost:5432/lake",
    "DUCKLAKE_DATA_PATH": "/tmp/lake",
    "DATARAUM_HOME": "/tmp/home",
    "DATARAUM_WORKSPACE_ID": "test",
    "ANTHROPIC_API_KEY": "sk-ant-test",
}

_OPTIONAL_ENV = [
    "DUCKLAKE_PG_POOL_MAX",
    "DUCKLAKE_SKIP_INSTALL",
    "DUCKDB_EXTENSION_DIRECTORY",
    "TEMPORAL_HOST",
    "TEMPORAL_NAMESPACE",
    "TEMPORAL_TASK_QUEUE",
]


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Strip every settings-backed var so each test starts from a known state.

    The root conftest exports ``DATARAUM_WORKSPACE_ID`` globally, so without
    this the missing-var test would never see it absent.
    """
    for var in (*_REQUIRED_ENV, *_OPTIONAL_ENV):
        monkeypatch.delenv(var, raising=False)
    reset_settings()
    yield
    reset_settings()


def _set_required(monkeypatch: pytest.MonkeyPatch) -> None:
    for key, value in _REQUIRED_ENV.items():
        monkeypatch.setenv(key, value)
    reset_settings()


@pytest.mark.usefixtures("clean_env")
def test_missing_required_var_fails_loud(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    monkeypatch.delenv("DATABASE_URL", raising=False)
    reset_settings()

    with pytest.raises(ValidationError) as exc:
        get_settings()

    # Fail-loud contract: the error names the offending field.
    assert "database_url" in str(exc.value).lower()


@pytest.mark.usefixtures("clean_env")
def test_all_required_present_constructs(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    settings = get_settings()

    assert settings.database_url.startswith("postgresql+psycopg://")
    assert settings.dataraum_workspace_id == "test"
    # SecretStr never leaks the value via repr/str.
    assert settings.anthropic_api_key.get_secret_value() == "sk-ant-test"
    assert "sk-ant-test" not in repr(settings.anthropic_api_key)


@pytest.mark.usefixtures("clean_env")
def test_temporal_vars_optional(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    settings = get_settings()

    assert settings.temporal_host is None
    assert settings.temporal_namespace is None
    assert settings.temporal_task_queue is None


@pytest.mark.usefixtures("clean_env")
def test_ducklake_tuning_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    settings = get_settings()

    assert settings.ducklake_pg_pool_max == 64
    assert settings.ducklake_skip_install is False
    assert settings.duckdb_extension_directory is None


@pytest.mark.usefixtures("clean_env")
def test_get_settings_is_cached_until_reset(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_required(monkeypatch)
    first = get_settings()
    assert get_settings() is first

    reset_settings()
    assert get_settings() is not first
