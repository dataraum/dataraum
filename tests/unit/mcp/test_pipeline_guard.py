"""Tests for pipeline-in-progress guard."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

import pytest
from mcp.types import CallToolRequest, CallToolRequestParams

from dataraum.mcp.server import _pipeline_idle


@pytest.fixture(autouse=True)
def _restore_pipeline_idle():
    """Ensure the flag is always restored to idle after each test."""
    yield
    _pipeline_idle.set()


async def _call(server: Any, name: str, arguments: dict | None = None) -> dict:
    """Call a tool through the MCP server handler and parse the JSON result."""
    handler = server.request_handlers[CallToolRequest]
    req = CallToolRequest(
        method="tools/call",
        params=CallToolRequestParams(name=name, arguments=arguments or {}),
    )
    raw = await handler(req)
    return json.loads(raw.root.content[0].text)


async def _setup_session(server: Any, tmp_path: Path) -> None:
    """Create a CSV source and begin a session."""
    csv = tmp_path / "data.csv"
    csv.write_text("a,b\n1,2\n")
    await _call(server, "add_source", {"name": "src", "path": str(csv)})
    result = await _call(server, "begin_session", {"intent": "test"})
    assert "error" not in result


class TestPipelineGuard:
    """Verify the pipeline guard flag blocks/unblocks correctly."""

    def test_starts_idle(self) -> None:
        """Pipeline idle flag should be set (idle) by default."""
        assert _pipeline_idle.is_set()

    def test_clear_blocks(self) -> None:
        """Clearing the flag simulates pipeline running."""
        _pipeline_idle.clear()
        assert not _pipeline_idle.is_set()

    def test_set_unblocks(self) -> None:
        """Setting the flag after clear simulates pipeline completion."""
        _pipeline_idle.clear()
        _pipeline_idle.set()
        assert _pipeline_idle.is_set()

    def test_cross_thread_visibility(self) -> None:
        """Flag changes in one thread are visible in another."""
        _pipeline_idle.clear()
        observed: list[bool] = []

        def worker() -> None:
            observed.append(_pipeline_idle.is_set())
            _pipeline_idle.set()

        t = threading.Thread(target=worker)
        t.start()
        t.join()

        assert observed == [False], "Worker should see flag cleared"
        assert _pipeline_idle.is_set(), "Worker should have set the flag"


class TestPipelineGuardDispatch:
    """Verify that call_tool blocks query/run_sql when pipeline is running."""

    @pytest.mark.asyncio
    async def test_query_blocked_when_pipeline_running(self, tmp_path) -> None:
        """query returns pipeline-running error when flag is cleared."""
        from dataraum.mcp.server import create_server

        workspace = tmp_path / "workspace"
        server = create_server(output_dir=workspace)
        await _setup_session(server, tmp_path)

        _pipeline_idle.clear()
        result = await _call(server, "query", {"question": "test"})
        assert "error" in result
        assert "Pipeline is currently running" in result["error"]

    @pytest.mark.asyncio
    async def test_run_sql_blocked_when_pipeline_running(self, tmp_path) -> None:
        """run_sql returns pipeline-running error when flag is cleared."""
        from dataraum.mcp.server import create_server

        workspace = tmp_path / "workspace"
        server = create_server(output_dir=workspace)
        await _setup_session(server, tmp_path)

        _pipeline_idle.clear()
        result = await _call(server, "run_sql", {"sql": "SELECT 1"})
        assert "error" in result
        assert "Pipeline is currently running" in result["error"]

    @pytest.mark.asyncio
    async def test_look_allowed_when_pipeline_running(self, tmp_path) -> None:
        """look is NOT blocked by pipeline guard — proceeds to normal execution."""
        from dataraum.mcp.server import create_server

        workspace = tmp_path / "workspace"
        server = create_server(output_dir=workspace)
        await _setup_session(server, tmp_path)

        _pipeline_idle.clear()
        result = await _call(server, "look")
        # look should succeed (returning data or empty schema), not hit the guard
        assert "Pipeline is currently running" not in result.get("error", "")
