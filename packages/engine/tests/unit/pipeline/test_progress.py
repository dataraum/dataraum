"""Tests for pipeline event types and structures."""

from __future__ import annotations

import pytest

from dataraum.pipeline.events import EventType, PipelineEvent


class TestPipelineEvent:
    """Tests for PipelineEvent data class."""

    def test_event_has_expected_fields(self):
        event = PipelineEvent(
            event_type=EventType.PHASE_STARTED,
            phase="import",
            step=1,
            total=10,
        )
        assert event.event_type == EventType.PHASE_STARTED
        assert event.phase == "import"
        assert event.step == 1
        assert event.total == 10

    def test_event_defaults(self):
        event = PipelineEvent(event_type=EventType.PIPELINE_STARTED)
        assert event.phase == ""
        assert event.step == 0
        assert event.total == 0
        assert event.message == ""
        assert event.duration_seconds == 0.0
        assert event.error == ""

    def test_event_is_frozen(self):
        event = PipelineEvent(event_type=EventType.PHASE_COMPLETED)
        with pytest.raises(AttributeError):
            event.phase = "modified"  # type: ignore[misc]

    def test_completed_event_has_duration(self):
        event = PipelineEvent(
            event_type=EventType.PHASE_COMPLETED,
            phase="typing",
            duration_seconds=3.14,
        )
        assert event.duration_seconds == 3.14

    def test_failed_event_has_error(self):
        event = PipelineEvent(
            event_type=EventType.PHASE_FAILED,
            phase="semantic",
            error="Something went wrong",
        )
        assert event.error == "Something went wrong"
