"""Tests for CLI pipeline driver (_drive_pipeline)."""

from __future__ import annotations

import re
from collections.abc import Generator
from io import StringIO

from rich.console import Console

from dataraum.cli.commands.run import _drive_pipeline
from dataraum.pipeline.events import EventType, PipelineEvent
from dataraum.pipeline.runner import GateMode
from dataraum.pipeline.scheduler import (
    ExitCheckIssue,
    PipelineResult,
    Resolution,
    ResolutionAction,
)


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape sequences from text."""
    return re.sub(r"\x1b\[[0-9;]*[a-zA-Z]", "", text)


def _mock_generator(
    events: list[PipelineEvent],
    result: PipelineResult,
    expected_resolutions: list[Resolution] | None = None,
) -> Generator[PipelineEvent, Resolution | None, PipelineResult]:
    """Create a mock generator that yields scripted events.

    Args:
        events: Events to yield in order.
        result: The final PipelineResult to return.
        expected_resolutions: If provided, EXIT_CHECK events will
            receive these resolutions via send().
    """
    resolution_iter = iter(expected_resolutions or [])
    for event in events:
        if event.event_type == EventType.EXIT_CHECK:
            resolution = yield event
            # Validate the sent resolution matches expectations
            try:
                expected = next(resolution_iter)
                assert resolution is not None, "Expected a Resolution via send()"
                assert resolution.action == expected.action
            except StopIteration:
                pass
        else:
            yield event
    return result


def _ok_result(**kwargs) -> PipelineResult:
    """Create a successful PipelineResult with defaults."""
    defaults = {
        "success": True,
        "phases_completed": ["import", "typing", "statistics"],
        "phases_failed": [],
        "phases_skipped": [],
        "phases_blocked": [],
        "final_scores": {},
        "deferred_issues": [],
    }
    defaults.update(kwargs)
    return PipelineResult(**defaults)


class TestDriveBasicPipeline:
    def test_three_phase_pipeline(self):
        """Drives 3-phase pipeline, correct print order."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=3),
            PipelineEvent(
                event_type=EventType.PHASE_STARTED, phase="import", step=2, total=3
            ),
            PipelineEvent(
                event_type=EventType.PHASE_COMPLETED,
                phase="import",
                step=3,
                total=3,
                duration_seconds=1.5,
            ),
            PipelineEvent(
                event_type=EventType.PHASE_STARTED, phase="typing", step=4, total=3
            ),
            PipelineEvent(
                event_type=EventType.PHASE_COMPLETED,
                phase="typing",
                step=5,
                total=3,
                duration_seconds=2.3,
            ),
            PipelineEvent(
                event_type=EventType.PHASE_STARTED,
                phase="statistics",
                step=6,
                total=3,
            ),
            PipelineEvent(
                event_type=EventType.PHASE_COMPLETED,
                phase="statistics",
                step=7,
                total=3,
                duration_seconds=0.8,
            ),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=8, total=3),
        ]

        result = _drive_pipeline(
            gen=_mock_generator(events, _ok_result()),
            console=console,
            gate_mode=GateMode.SKIP,
        )

        rendered = output.getvalue()
        assert result.success
        assert "import" in rendered
        assert "typing" in rendered
        assert "statistics" in rendered
        # Check ordering: import appears before typing
        assert rendered.index("import") < rendered.index("typing")


class TestDriveWithSkip:
    def test_skipped_phase_renders(self):
        """PHASE_SKIPPED renders with skip marker."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=2),
            PipelineEvent(
                event_type=EventType.PHASE_SKIPPED,
                phase="semantic",
                step=2,
                total=2,
                message="No typed tables found",
            ),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=3, total=2),
        ]

        _drive_pipeline(
            gen=_mock_generator(
                events, _ok_result(phases_skipped=["semantic"], phases_completed=[])
            ),
            console=console,
            gate_mode=GateMode.SKIP,
        )

        rendered = output.getvalue()
        assert "semantic" in rendered
        assert "No typed tables found" in rendered


class TestDriveWithFailure:
    def test_failed_phase_renders(self):
        """PHASE_FAILED renders with error."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=1),
            PipelineEvent(
                event_type=EventType.PHASE_STARTED, phase="import", step=2, total=1
            ),
            PipelineEvent(
                event_type=EventType.PHASE_FAILED,
                phase="import",
                step=3,
                total=1,
                error="File not found",
                duration_seconds=0.1,
            ),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=4, total=1),
        ]

        result = _drive_pipeline(
            gen=_mock_generator(
                events,
                _ok_result(
                    success=False,
                    phases_completed=[],
                    phases_failed=["import"],
                ),
            ),
            console=console,
            gate_mode=GateMode.SKIP,
        )

        rendered = output.getvalue()
        assert not result.success
        assert "import" in rendered
        assert "File not found" in rendered


class TestExitCheckSkipMode:
    def test_auto_sends_defer(self):
        """gate_mode=SKIP auto-sends DEFER at EXIT_CHECK."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=2),
            PipelineEvent(
                event_type=EventType.PHASE_STARTED, phase="typing", step=2, total=2
            ),
            PipelineEvent(
                event_type=EventType.PHASE_COMPLETED,
                phase="typing",
                step=3,
                total=2,
                duration_seconds=1.0,
            ),
            PipelineEvent(
                event_type=EventType.EXIT_CHECK,
                step=4,
                total=2,
                violations={"structural.types.type_fidelity": (0.62, 0.50)},
            ),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=5, total=2),
        ]

        result = _drive_pipeline(
            gen=_mock_generator(
                events,
                _ok_result(),
                expected_resolutions=[Resolution(action=ResolutionAction.DEFER)],
            ),
            console=console,
            gate_mode=GateMode.SKIP,
        )

        assert result.success
        assert "deferred" in output.getvalue().lower()


class TestExitCheckFailMode:
    def test_auto_sends_abort(self):
        """gate_mode=FAIL auto-sends ABORT at EXIT_CHECK."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=2),
            PipelineEvent(
                event_type=EventType.PHASE_COMPLETED,
                phase="typing",
                step=2,
                total=2,
                duration_seconds=1.0,
            ),
            PipelineEvent(
                event_type=EventType.EXIT_CHECK,
                step=3,
                total=2,
                violations={"structural.types.type_fidelity": (0.62, 0.50)},
            ),
            # After ABORT, the generator will raise PipelineAborted,
            # which the scheduler catches internally and returns a failed result
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=4, total=2),
        ]

        _drive_pipeline(
            gen=_mock_generator(
                events,
                _ok_result(success=False, error="Pipeline aborted by user"),
                expected_resolutions=[Resolution(action=ResolutionAction.ABORT)],
            ),
            console=console,
            gate_mode=GateMode.FAIL,
        )

        # The mock generator doesn't actually abort — it just yields remaining events.
        # But the handle_exit_check sends ABORT resolution.
        assert "abort" in output.getvalue().lower()


class TestQuietMode:
    def test_no_phase_output(self):
        """Quiet mode suppresses phase progress output."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=1),
            PipelineEvent(
                event_type=EventType.PHASE_STARTED, phase="import", step=2, total=1
            ),
            PipelineEvent(
                event_type=EventType.PHASE_COMPLETED,
                phase="import",
                step=3,
                total=1,
                duration_seconds=1.0,
            ),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=4, total=1),
        ]

        result = _drive_pipeline(
            gen=_mock_generator(events, _ok_result()),
            console=console,
            gate_mode=GateMode.SKIP,
            quiet=True,
        )

        assert result.success
        # No output in quiet mode
        assert output.getvalue() == ""


class TestPipelineResultReturned:
    def test_result_captured_from_stop_iteration(self):
        """StopIteration.value correctly captured as PipelineResult."""
        console = Console(file=StringIO())

        expected_result = _ok_result(
            final_scores={"structural.types.type_fidelity": 0.15},
            deferred_issues=[
                ExitCheckIssue(
                    dimension_path="semantic.units",
                    score=0.45,
                    threshold=0.30,
                    producing_phase="typing",
                )
            ],
        )

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=0),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=2, total=0),
        ]

        result = _drive_pipeline(
            gen=_mock_generator(events, expected_result),
            console=console,
            gate_mode=GateMode.SKIP,
        )

        assert result is not None
        assert result.success
        assert result.final_scores == {"structural.types.type_fidelity": 0.15}
        assert len(result.deferred_issues) == 1


class TestSummaryDisplay:
    def test_shows_completed_failed_counts(self):
        """Post-run summary shows completed/failed counts."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=0),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=2, total=0),
        ]

        result_data = _ok_result(
            phases_completed=["import", "typing"],
            phases_failed=["statistics"],
            phases_skipped=["semantic"],
            success=False,
        )

        _drive_pipeline(
            gen=_mock_generator(events, result_data),
            console=console,
            gate_mode=GateMode.SKIP,
        )

        rendered = output.getvalue()
        assert "completed" in rendered
        assert "failed" in rendered
        assert "skipped" in rendered
        assert "2" in rendered  # 2 completed
        assert "1" in rendered  # 1 failed

    def test_contract_evaluation_in_summary(self):
        """Summary shows pass/fail per dimension when contract is provided."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=0),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=2, total=0),
        ]

        result_data = _ok_result(
            final_scores={
                "structural.types.type_fidelity": 0.18,
                "structural.relations.join_det": 0.45,
            },
        )

        _drive_pipeline(
            gen=_mock_generator(events, result_data),
            console=console,
            gate_mode=GateMode.SKIP,
            contract_name="aggregation_safe",
            contract_thresholds={
                "structural.types.type_fidelity": 0.30,
                "structural.relations.join_det": 0.20,
            },
        )

        rendered = _strip_ansi(output.getvalue())
        assert "aggregation_safe" in rendered
        assert "1/2" in rendered
        assert "50%" in rendered

    def test_fixes_in_summary(self):
        """Summary shows accumulated fix decisions."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=1),
            PipelineEvent(
                event_type=EventType.FIX_APPLIED,
                step=2,
                total=1,
                message="override_type on column:orders.amount",
                before_scores={"structural.types": 0.65},
                after_scores={"structural.types": 0.18},
            ),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=3, total=1),
        ]

        _drive_pipeline(
            gen=_mock_generator(events, _ok_result()),
            console=console,
            gate_mode=GateMode.SKIP,
        )

        rendered = _strip_ansi(output.getvalue())
        assert "Fixes Applied (1):" in rendered
        assert "override_type" in rendered
        assert "0.65" in rendered
        assert "0.18" in rendered


    def test_unmeasured_contracted_dimensions_shown(self):
        """Contracted dimensions not in final_scores shown as 'not measured'."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=0),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=2, total=0),
        ]

        result_data = _ok_result(
            final_scores={
                "structural.types.type_fidelity": 0.18,
            },
        )

        _drive_pipeline(
            gen=_mock_generator(events, result_data),
            console=console,
            gate_mode=GateMode.SKIP,
            contract_name="test_contract",
            contract_thresholds={
                "structural.types.type_fidelity": 0.30,
                "semantic.meaning.naming_clarity": 0.40,
            },
        )

        rendered = _strip_ansi(output.getvalue())
        assert "not measured" in rendered
        # Dimension name is truncated to 28 chars in display
        assert "naming_clar" in rendered
        # 1 passing out of 2 total (1 measured + 1 not measured)
        assert "1/2" in rendered
        assert "1 not measured" in rendered


class TestParallelSpinner:
    def test_parallel_phases_tracked(self):
        """Multiple PHASE_STARTED before PHASE_COMPLETED tracks running set."""
        output = StringIO()
        console = Console(file=output, force_terminal=True, width=100)

        events = [
            PipelineEvent(event_type=EventType.PIPELINE_STARTED, step=1, total=2),
            PipelineEvent(
                event_type=EventType.PHASE_STARTED, phase="typing", step=2, total=2
            ),
            PipelineEvent(
                event_type=EventType.PHASE_STARTED,
                phase="statistics",
                step=3,
                total=2,
            ),
            PipelineEvent(
                event_type=EventType.PHASE_COMPLETED,
                phase="typing",
                step=4,
                total=2,
                duration_seconds=1.0,
            ),
            PipelineEvent(
                event_type=EventType.PHASE_COMPLETED,
                phase="statistics",
                step=5,
                total=2,
                duration_seconds=1.5,
            ),
            PipelineEvent(event_type=EventType.PIPELINE_COMPLETED, step=6, total=2),
        ]

        result = _drive_pipeline(
            gen=_mock_generator(events, _ok_result()),
            console=console,
            gate_mode=GateMode.SKIP,
        )

        assert result.success
        rendered = output.getvalue()
        # Both phases should appear as completed
        assert "typing" in rendered
        assert "statistics" in rendered
