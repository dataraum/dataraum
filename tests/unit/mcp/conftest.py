"""Shared fixtures for MCP unit tests."""

import pytest


@pytest.fixture(autouse=True)
def _set_dummy_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure prereq checks pass in unit tests.

    Tests that explicitly test missing-key behavior override
    this with monkeypatch.delenv().
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")


@pytest.fixture(autouse=True)
def _set_database_url(monkeypatch: pytest.MonkeyPatch, pg_url_clean: str) -> None:
    """Wire DATABASE_URL to the per-test Postgres URL for every MCP unit test.

    Post-DAT-321, ``ConnectionConfig.for_workspace()`` and ``for_directory()``
    both read ``DATABASE_URL`` from the environment. The session-scoped
    Postgres container is reused across tests; ``pg_url_clean`` truncates
    every Base-registered table before each test for isolation.
    """
    monkeypatch.setenv("DATABASE_URL", pg_url_clean)
