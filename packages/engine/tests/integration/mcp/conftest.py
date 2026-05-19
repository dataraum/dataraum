"""Shared fixtures for MCP integration tests.

Mirrors ``tests/unit/mcp/conftest.py``: every test in this module that opens a
per-session ``ConnectionManager`` needs the DuckLake anchor bootstrapped
(post-DAT-323) and a clean per-test lake schema set.
"""

import pytest


@pytest.fixture(autouse=True)
def _bootstrap_lake_for_mcp_integration(lake_anchor, lake_clean) -> None:
    """Bootstrap DuckLake + clean session schemas around every test.

    The session-scoped ``lake_anchor`` opens the anchor once per pytest
    invocation; ``lake_clean`` drops ``session_*`` / ``archive_*`` schemas
    between tests so per-test state stays hermetic.
    """
    return None
