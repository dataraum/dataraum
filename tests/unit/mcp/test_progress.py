"""Tests for MCP tasks API integration in the analyze tool."""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataraum.mcp.server import _analyze, _make_task_event_callback
from dataraum.pipeline.events import EventType, PipelineEvent


class TestMakeTaskEventCallback:
    """Tests for _make_task_event_callback sync→async bridge."""

    @pytest.mark.asyncio
    async def test_callback_calls_update_status_on_phase_started(self):
        """Callback bridges sync PHASE_STARTED event to async task.update_status()."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_event_callback(task, asyncio.get_running_loop())

        event = PipelineEvent(
            event_type=EventType.PHASE_STARTED,
            phase="typing",
            step=3,
            total=10,
        )
        await asyncio.to_thread(callback, event)

        task.update_status.assert_called_once()
        msg = task.update_status.call_args[0][0]
        assert msg.startswith("Phase 3/10: Running ")

    @pytest.mark.asyncio
    async def test_callback_calls_update_status_on_phase_completed(self):
        """Callback bridges sync PHASE_COMPLETED event."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_event_callback(task, asyncio.get_running_loop())

        event = PipelineEvent(
            event_type=EventType.PHASE_COMPLETED,
            phase="typing",
            step=3,
            total=10,
        )
        await asyncio.to_thread(callback, event)

        task.update_status.assert_called_once()
        msg = task.update_status.call_args[0][0]
        assert msg.startswith("Phase 3/10: Completed ")

    @pytest.mark.asyncio
    async def test_callback_swallows_update_failure(self):
        """Callback does not raise even if update_status fails."""
        task = MagicMock()
        task.update_status = AsyncMock(side_effect=RuntimeError("connection lost"))

        callback = _make_task_event_callback(task, asyncio.get_running_loop())

        event = PipelineEvent(
            event_type=EventType.PHASE_STARTED,
            phase="test",
            step=1,
            total=5,
        )
        # Should not raise
        await asyncio.to_thread(callback, event)

    @pytest.mark.asyncio
    async def test_callback_formats_pipeline_complete(self):
        """Callback formats PIPELINE_COMPLETED message."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_event_callback(task, asyncio.get_running_loop())

        event = PipelineEvent(
            event_type=EventType.PIPELINE_COMPLETED,
            step=15,
            total=18,
        )
        await asyncio.to_thread(callback, event)

        task.update_status.assert_called_once_with("Phase 15/18: Pipeline complete")

    @pytest.mark.asyncio
    async def test_callback_ignores_non_progress_events(self):
        """Events like POST_VERIFICATION don't trigger update_status."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_event_callback(task, asyncio.get_running_loop())

        event = PipelineEvent(
            event_type=EventType.POST_VERIFICATION,
            phase="typing",
            step=3,
            total=10,
        )
        await asyncio.to_thread(callback, event)

        task.update_status.assert_not_called()

    @pytest.mark.asyncio
    async def test_callback_called_multiple_times(self):
        """Callback works across multiple progress updates."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_event_callback(task, asyncio.get_running_loop())

        events = [
            PipelineEvent(event_type=EventType.PHASE_STARTED, phase="a", step=1, total=5),
            PipelineEvent(event_type=EventType.PHASE_COMPLETED, phase="a", step=1, total=5),
            PipelineEvent(event_type=EventType.PHASE_STARTED, phase="b", step=2, total=5),
        ]

        for event in events:
            await asyncio.to_thread(callback, event)

        assert task.update_status.call_count == 3


class TestAnalyzeFunction:
    """Tests for the _analyze sync function."""

    def test_missing_path_returns_error(self):
        """Returns error dict for non-existent path."""
        result = _analyze(
            output_dir=MagicMock(),
            path="/nonexistent/path/to/data.csv",
        )

        assert isinstance(result, dict)
        assert "Path not found" in result["error"]

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

        assert isinstance(result, dict)
        assert "Pipeline failed" in result["error"]

    def test_success_returns_formatted_result(self):
        """Returns formatted dict on success."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()
        mock_result.error = None

        mock_formatted = {"status": "complete", "phases": {"completed": 18}}

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            tmp_path = f.name

        try:
            with (
                patch("dataraum.pipeline.runner.run", return_value=mock_result),
                patch(
                    "dataraum.mcp.server.format_pipeline_result",
                    return_value=mock_formatted,
                ),
            ):
                result = _analyze(
                    output_dir=Path("/tmp/test_output"),
                    path=tmp_path,
                    name="test_source",
                )
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        assert result == mock_formatted

    def test_event_callback_forwarded(self):
        """Event callback is passed through to RunConfig."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()

        cb = MagicMock()

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            tmp_path = f.name

        try:
            with (
                patch("dataraum.pipeline.runner.run", return_value=mock_result) as mock_run,
                patch("dataraum.mcp.server.format_pipeline_result", return_value={"status": "ok"}),
            ):
                _analyze(
                    output_dir=Path("/tmp/test_output"),
                    path=tmp_path,
                    event_callback=cb,
                )
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        # Verify the RunConfig was created with the callback
        run_config = mock_run.call_args[0][0]
        assert run_config.event_callback is cb

    def test_target_gate_and_contract_forwarded(self):
        """target_gate and contract are passed through to RunConfig."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()

        mock_zone = {"zone": "foundation", "violations": []}

        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            tmp_path = f.name

        try:
            with (
                patch("dataraum.pipeline.runner.run", return_value=mock_result) as mock_run,
                patch("dataraum.mcp.server.format_pipeline_result", return_value={"status": "ok"}),
                patch("dataraum.mcp.server._get_zone_status", return_value=mock_zone),
            ):
                result = _analyze(
                    output_dir=Path("/tmp/test_output"),
                    path=tmp_path,
                    target_gate="quality_review",
                    contract="executive_dashboard",
                )
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        run_config = mock_run.call_args[0][0]
        assert run_config.target_phase == "quality_review"
        assert run_config.contract == "executive_dashboard"
        # Zone status should be included when target_gate is set
        assert result["gate_status"] == mock_zone


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
