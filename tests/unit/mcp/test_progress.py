"""Tests for MCP tasks API integration and pipeline trigger."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from dataraum.mcp.server import _make_task_event_callback, _run_pipeline
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
        """Events like PHASE_SKIPPED don't trigger update_status."""
        task = MagicMock()
        task.update_status = AsyncMock()

        callback = _make_task_event_callback(task, asyncio.get_running_loop())

        event = PipelineEvent(
            event_type=EventType.PHASE_SKIPPED,
            phase="semantic",
            step=5,
            total=10,
            message="No data",
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


class TestRunPipeline:
    """Tests for the _run_pipeline function."""

    def test_pipeline_failure_returns_error(self, tmp_path):
        """Returns error when pipeline fails."""
        mock_result = MagicMock()
        mock_result.success = False
        mock_result.value = None
        mock_result.error = "something broke"

        with patch("dataraum.pipeline.runner.run", return_value=mock_result):
            result = _run_pipeline(output_dir=tmp_path)

        assert isinstance(result, dict)
        assert "Pipeline failed" in result["error"]

    def test_success_returns_status_complete(self, tmp_path):
        """Returns status=complete on success."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()
        mock_result.value.phases_completed = 17

        with patch("dataraum.pipeline.runner.run", return_value=mock_result):
            result = _run_pipeline(output_dir=tmp_path)

        assert result["status"] == "complete"

    def test_always_multi_source_mode(self, tmp_path):
        """Pipeline always runs in multi-source mode (source_path=None)."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()
        mock_result.value.phases_completed = 17

        with patch("dataraum.pipeline.runner.run", return_value=mock_result) as mock_run:
            _run_pipeline(output_dir=tmp_path)

        run_config = mock_run.call_args[0][0]
        assert run_config.source_path is None

    def test_event_callback_forwarded(self, tmp_path):
        """Event callback is passed through to RunConfig."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()
        mock_result.value.phases_completed = 17

        cb = MagicMock()

        with patch("dataraum.pipeline.runner.run", return_value=mock_result) as mock_run:
            _run_pipeline(output_dir=tmp_path, event_callback=cb)

        run_config = mock_run.call_args[0][0]
        assert run_config.event_callback is cb

    def test_contract_passed_through(self, tmp_path):
        """Contract from session is passed to RunConfig."""
        mock_result = MagicMock()
        mock_result.success = True
        mock_result.value = MagicMock()
        mock_result.value.phases_completed = 17

        with patch("dataraum.pipeline.runner.run", return_value=mock_result) as mock_run:
            _run_pipeline(output_dir=tmp_path, contract="executive_dashboard")

        run_config = mock_run.call_args[0][0]
        assert run_config.contract == "executive_dashboard"


class TestCreateServer:
    """Tests for server configuration."""

    def test_tool_execution_optional_task_support(self):
        """ToolExecution with taskSupport='optional' is a valid configuration."""
        from mcp.types import Tool, ToolExecution

        tool = Tool(
            name="measure",
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
