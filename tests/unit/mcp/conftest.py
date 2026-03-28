"""Shared fixtures for MCP unit tests."""

import pytest


@pytest.fixture(autouse=True)
def _set_dummy_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ensure prereq checks pass in unit tests.

    Tests that explicitly test missing-key behavior override
    this with monkeypatch.delenv().
    """
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-ant-test-key")
