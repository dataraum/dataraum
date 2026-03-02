"""Tests for pipeline event callback notifications."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from dataraum.pipeline.base import PhaseResult
from dataraum.pipeline.events import EventType
from dataraum.pipeline.orchestrator import Pipeline, PipelineConfig


def _make_mock_manager():
    """Create a mock ConnectionManager that won't trigger SQLAlchemy."""
    mgr = MagicMock()

    @contextmanager
    def _session_scope():
        session = MagicMock()
        result_mock = MagicMock()
        result_mock.scalars.return_value.all.return_value = []
        session.execute.return_value = result_mock
        yield session

    mgr.session_scope = _session_scope
    return mgr


class TestPipelineEventCallback:
    """Tests that Pipeline.run() invokes event_callback at the right points."""

    @pytest.fixture()
    def mock_phase(self):
        from tests.unit.pipeline.conftest import MockPhase

        return MockPhase

    @pytest.fixture()
    def manager(self):
        return _make_mock_manager()

    def _run_with_callback(self, pipeline, manager, cb=None):
        """Run pipeline with DB models and metrics patched out."""
        with (
            patch("dataraum.pipeline.orchestrator.PipelineRun"),
            patch("dataraum.pipeline.orchestrator.update", return_value=MagicMock()),
            patch("dataraum.pipeline.orchestrator.select", return_value=MagicMock()),
            patch("dataraum.pipeline.orchestrator.start_pipeline_metrics"),
            patch("dataraum.pipeline.orchestrator.end_pipeline_metrics", return_value=None),
        ):
            return pipeline.run(
                manager=manager,
                source_id="test-src",
                run_id=str(uuid4()),
                event_callback=cb,
            )

    def test_events_emitted_for_each_phase(self, mock_phase, manager):
        """Event callback receives started + completed events for each phase."""
        cb = MagicMock()

        pipeline = Pipeline(config=PipelineConfig(max_parallel=1, skip_completed=False))
        pipeline.register(mock_phase("phase_a"))
        pipeline.register(mock_phase("phase_b", dependencies=["phase_a"]))

        with patch.object(Pipeline, "_execute_phase") as mock_exec:
            mock_exec.return_value = PhaseResult.success(outputs={})
            self._run_with_callback(pipeline, manager, cb)

        event_types = [c[0][0].event_type for c in cb.call_args_list]

        assert EventType.PHASE_STARTED in event_types
        assert EventType.PHASE_COMPLETED in event_types
        assert EventType.PIPELINE_COMPLETED in event_types

    def test_pipeline_works_without_callback(self, mock_phase, manager):
        """Pipeline runs normally when event_callback is None."""
        pipeline = Pipeline(config=PipelineConfig(max_parallel=1, skip_completed=False))
        pipeline.register(mock_phase("solo"))

        with patch.object(Pipeline, "_execute_phase") as mock_exec:
            mock_exec.return_value = PhaseResult.success(outputs={})
            results = self._run_with_callback(pipeline, manager, None)

        assert "solo" in results

    def test_callback_exception_does_not_crash_pipeline(self, mock_phase, manager):
        """If the callback raises, the pipeline still completes."""
        cb = MagicMock(side_effect=RuntimeError("notification failed"))

        pipeline = Pipeline(config=PipelineConfig(max_parallel=1, skip_completed=False))
        pipeline.register(mock_phase("resilient"))

        with patch.object(Pipeline, "_execute_phase") as mock_exec:
            mock_exec.return_value = PhaseResult.success(outputs={})
            results = self._run_with_callback(pipeline, manager, cb)

        assert "resilient" in results

    def test_failed_phase_emits_failed_event(self, mock_phase, manager):
        """Failed phases trigger PHASE_FAILED events."""
        cb = MagicMock()

        pipeline = Pipeline(
            config=PipelineConfig(max_parallel=1, skip_completed=False, fail_fast=True)
        )
        pipeline.register(mock_phase("bad", should_fail=True))

        with patch.object(Pipeline, "_execute_phase") as mock_exec:
            mock_exec.return_value = PhaseResult.failed("intentional")
            self._run_with_callback(pipeline, manager, cb)

        event_types = [c[0][0].event_type for c in cb.call_args_list]
        assert EventType.PHASE_FAILED in event_types

    def test_skipped_phase_emits_skipped_event(self, mock_phase, manager):
        """Skipped phases trigger PHASE_SKIPPED events."""
        cb = MagicMock()

        pipeline = Pipeline(config=PipelineConfig(max_parallel=1, skip_completed=False))
        pipeline.register(mock_phase("skipper", skip_reason="already done"))

        with patch.object(Pipeline, "_execute_phase") as mock_exec:
            mock_exec.return_value = PhaseResult.skipped("already done")
            self._run_with_callback(pipeline, manager, cb)

        event_types = [c[0][0].event_type for c in cb.call_args_list]
        assert EventType.PHASE_SKIPPED in event_types
