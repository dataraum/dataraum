"""Tests for pipeline events."""

import pytest

from dataraum.pipeline.events import EventType, PipelineEvent


class TestEventType:
    def test_values(self):
        assert EventType.PHASE_STARTED.value == "phase_started"
        assert EventType.PHASE_COMPLETED.value == "phase_completed"
        assert EventType.PIPELINE_COMPLETED.value == "pipeline_completed"

    def test_is_str_enum(self):
        assert isinstance(EventType.PHASE_STARTED, str)
        assert EventType.PHASE_STARTED == "phase_started"


class TestPipelineEvent:
    def test_creation_minimal(self):
        event = PipelineEvent(event_type=EventType.PHASE_STARTED)
        assert event.event_type == EventType.PHASE_STARTED
        assert event.phase == ""
        assert event.step == 0
        assert event.total == 0
        assert event.duration_seconds == 0.0

    def test_creation_with_data(self):
        event = PipelineEvent(
            event_type=EventType.PHASE_COMPLETED,
            phase="statistics",
            step=3,
            total=19,
            duration_seconds=1.5,
        )
        assert event.phase == "statistics"
        assert event.step == 3
        assert event.total == 19
        assert event.duration_seconds == 1.5

    def test_frozen_immutability(self):
        event = PipelineEvent(event_type=EventType.PHASE_STARTED, phase="test")
        with pytest.raises(AttributeError):
            event.phase = "other"  # type: ignore[misc]


class TestLegacyAdapter:
    """Test that the legacy progress callback can be adapted from events."""

    def test_legacy_adapter_from_event(self):
        """Events carry step/total/message for legacy adapter translation."""
        calls: list[tuple[int, int, str]] = []

        def legacy_callback(current: int, total: int, message: str) -> None:
            calls.append((current, total, message))

        event = PipelineEvent(
            event_type=EventType.PHASE_STARTED,
            phase="statistics",
            step=3,
            total=19,
            message="Running statistics",
        )
        legacy_callback(event.step, event.total, event.message)
        assert calls == [(3, 19, "Running statistics")]
