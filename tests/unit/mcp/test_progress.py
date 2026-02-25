"""Tests for MCP tasks API integration in the analyze tool."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataraum.mcp.server import _analyze, _make_task_progress_callback


class TestMakeTaskProgressCallback:
    """Tests for _make_task_progress_callback sync→async bridge."""

    @pytest.mark.asyncio
    async def test_callback_calls_update_status(self):
        """Callback bridges sync call to async task.update_status()."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_progress_callback(task, asyncio.get_running_loop())

        await asyncio.to_thread(callback, 3, 10, "Running typing...")

        task.update_status.assert_called_once_with("Phase 3/10: Running typing...")

    @pytest.mark.asyncio
    async def test_callback_swallows_update_failure(self):
        """Callback does not raise even if update_status fails."""
        task = MagicMock()
        task.update_status = AsyncMock(side_effect=RuntimeError("connection lost"))

        callback = _make_task_progress_callback(task, asyncio.get_running_loop())

        # Should not raise
        await asyncio.to_thread(callback, 1, 5, "test")

    @pytest.mark.asyncio
    async def test_callback_formats_message(self):
        """Callback formats [current/total] message."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_progress_callback(task, asyncio.get_running_loop())

        await asyncio.to_thread(callback, 15, 18, "Pipeline complete")

        task.update_status.assert_called_once_with("Phase 15/18: Pipeline complete")

    @pytest.mark.asyncio
    async def test_callback_called_multiple_times(self):
        """Callback works across multiple progress updates."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_progress_callback(task, asyncio.get_running_loop())

        await asyncio.to_thread(callback, 1, 5, "phase A")
        await asyncio.to_thread(callback, 2, 5, "phase B")
        await asyncio.to_thread(callback, 3, 5, "phase C")

        assert task.update_status.call_count == 3
        task.update_status.assert_any_call("Phase 1/5: phase A")
        task.update_status.assert_any_call("Phase 2/5: phase B")
        task.update_status.assert_any_call("Phase 3/5: phase C")


class TestAnalyzeFunction:
    """Tests for the _analyze sync function."""

    def test_missing_path_returns_error(self):
        """Returns error string for non-existent path."""
        result = _analyze(
            output_dir=MagicMock(),
            path="/nonexistent/path/to/data.csv",
        )

        assert "Path not found" in result

    def test_pipeline_failure_returns_error(self):
        """Returns error when pipeline fails."""
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.value = None
        mock_result.error = "something broke"

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            tmp_path = f.name

        try:
            with patch("dataraum.pipeline.runner.run", return_value=mock_result):
                result = _analyze(
                    output_dir=MagicMock(),
                    path=tmp_path,
                )
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        assert "Error: Pipeline failed" in result

    def test_success_returns_formatted_result(self):
        """Returns formatted output on success."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()
        mock_result.error = None

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            tmp_path = f.name

        try:
            with (
                patch("dataraum.pipeline.runner.run", return_value=mock_result),
                patch(
                    "dataraum.mcp.server.format_pipeline_result",
                    return_value="Pipeline completed: 18/18 phases",
                ),
            ):
                result = _analyze(
                    output_dir=Path("/tmp/test_output"),
                    path=tmp_path,
                    name="test_source",
                )
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        assert result == "Pipeline completed: 18/18 phases"

    def test_progress_callback_forwarded(self):
        """Progress callback is passed through to RunConfig."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()

        cb = MagicMock()

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            tmp_path = f.name

        try:
            with (
                patch("dataraum.pipeline.runner.run", return_value=mock_result) as mock_run,
                patch("dataraum.mcp.server.format_pipeline_result", return_value="ok"),
            ):
                _analyze(
                    output_dir=Path("/tmp/test_output"),
                    path=tmp_path,
                    progress_callback=cb,
                )
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        # Verify the RunConfig was created with the callback
        run_config = mock_run.call_args[0][0]
        assert run_config.progress_callback is cb


class TestCreateServer:
    """Tests for server configuration."""

    def test_tool_execution_optional_task_support(self):
        """ToolExecution with taskSupport='optional' is a valid configuration."""
        from mcp.types import Tool, ToolExecution

        # Verify the exact configuration used for the analyze tool
        tool = Tool(
            name="analyze",
            description="test",
            inputSchema={"type": "object", "properties": {}},
            execution=ToolExecution(taskSupport="optional"),
        )
        assert tool.execution is not None
        assert tool.execution.taskSupport == "optional"

    def test_server_has_tasks_enabled(self):
        """Server has experimental tasks enabled."""
        from dataraum.mcp.server import create_server

        server = create_server(output_dir=Path("/tmp/test"))

        # The experimental handlers should exist after enable_tasks()
        assert server._experimental_handlers is not None
