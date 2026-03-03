"""Tests for PipelineScheduler."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.entropy.fix_executor import FixExecutor, FixRequest, FixResult
from dataraum.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.db_models import Fix, PhaseLog, PipelineRun
from dataraum.pipeline.events import EventType
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.scheduler import (
    PipelineScheduler,
    Resolution,
    ResolutionAction,
)

# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


class MockPhase(BasePhase):
    """Configurable mock phase for scheduler tests."""

    def __init__(
        self,
        name: str,
        dependencies: list[str] | None = None,
        should_fail: bool = False,
        skip_reason: str | None = None,
        post_verification_dims: list[str] | None = None,
    ):
        self._name = name
        self._dependencies = dependencies or []
        self._should_fail = should_fail
        self._skip_reason = skip_reason
        self._post_verification = post_verification_dims or []
        self.run_count = 0

    @property
    def name(self) -> str:
        return self._name

    @property
    def description(self) -> str:
        return f"Mock: {self._name}"

    @property
    def dependencies(self) -> list[str]:
        return self._dependencies

    @property
    def post_verification(self) -> list[str]:
        return self._post_verification

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        self.run_count += 1
        if self._should_fail:
            return PhaseResult.failed("Intentional failure")
        return PhaseResult.success(records_processed=10, records_created=5)

    def should_skip(self, ctx: PhaseContext) -> str | None:
        return self._skip_reason


def _make_run(session: Session, source_id: str = "src-1") -> str:
    """Create a PipelineRun and return its run_id."""
    run_id = str(uuid4())
    run = PipelineRun(
        run_id=run_id,
        source_id=source_id,
        status="running",
        started_at=datetime.now(UTC),
    )
    session.add(run)
    session.flush()
    return run_id


def _drive(gen, *, resolutions: dict[int, Resolution] | None = None):
    """Drive a scheduler generator to completion.

    Args:
        gen: The generator returned by scheduler.run().
        resolutions: Map of exit-check index (0-based) to Resolution to send.

    Returns:
        Tuple of (list of events, PipelineResult).
    """
    resolutions = resolutions or {}
    events = []
    exit_check_idx = 0
    try:
        event = next(gen)
        events.append(event)
        while True:
            send_val = None
            if event.event_type == EventType.EXIT_CHECK:
                send_val = resolutions.get(exit_check_idx)
                exit_check_idx += 1
            event = gen.send(send_val)
            events.append(event)
    except StopIteration as e:
        return events, e.value


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEmptyPipeline:
    def test_empty_pipeline(self, session: Session, duckdb_conn):
        """No phases → immediate PipelineResult(success=True)."""
        run_id = _make_run(session)
        scheduler = PipelineScheduler(
            phases={},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is True
        assert result.phases_completed == []
        assert result.phases_failed == []
        assert result.phases_skipped == []
        # Should have PIPELINE_STARTED and PIPELINE_COMPLETED
        types = [e.event_type for e in events]
        assert EventType.PIPELINE_STARTED in types
        assert EventType.PIPELINE_COMPLETED in types


class TestSinglePhase:
    def test_single_phase_completes(self, session: Session, duckdb_conn):
        """One phase runs, yields STARTED+COMPLETED, PhaseLog written."""
        run_id = _make_run(session)
        phase = MockPhase("alpha")
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is True
        assert result.phases_completed == ["alpha"]
        assert phase.run_count == 1

        types = [e.event_type for e in events]
        assert EventType.PHASE_STARTED in types
        assert EventType.PHASE_COMPLETED in types

    def test_phase_skipped(self, session: Session, duckdb_conn):
        """should_skip returns reason → SKIPPED event, PhaseLog(status='skipped')."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", skip_reason="already done")
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is True
        assert result.phases_skipped == ["alpha"]
        assert result.phases_completed == []
        assert phase.run_count == 0

        types = [e.event_type for e in events]
        assert EventType.PHASE_SKIPPED in types
        assert EventType.PHASE_STARTED not in types

        # Verify PhaseLog
        logs = session.execute(select(PhaseLog)).scalars().all()
        assert len(logs) == 1
        assert logs[0].status == "skipped"

    def test_phase_fails(self, session: Session, duckdb_conn):
        """run returns FAILED → FAILED event, PhaseLog(status='failed')."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", should_fail=True)
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is False
        assert result.phases_failed == ["alpha"]
        assert result.phases_completed == []

        types = [e.event_type for e in events]
        assert EventType.PHASE_FAILED in types

        # Verify PhaseLog
        logs = session.execute(select(PhaseLog)).scalars().all()
        assert len(logs) == 1
        assert logs[0].status == "failed"
        assert logs[0].error == "Intentional failure"


class TestDependencyOrdering:
    def test_dependency_ordering(self, session: Session, duckdb_conn):
        """B depends on A → A runs first."""
        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", dependencies=["A"])
        scheduler = PipelineScheduler(
            phases={"A": a, "B": b},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is True
        assert result.phases_completed == ["A", "B"]

        # Check execution order via events
        started = [e.phase for e in events if e.event_type == EventType.PHASE_STARTED]
        assert started == ["A", "B"]

    def test_dependency_chain(self, session: Session, duckdb_conn):
        """C→B→A: runs in order A, B, C."""
        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", dependencies=["A"])
        c = MockPhase("C", dependencies=["B"])
        scheduler = PipelineScheduler(
            phases={"A": a, "B": b, "C": c},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is True
        started = [e.phase for e in events if e.event_type == EventType.PHASE_STARTED]
        assert started == ["A", "B", "C"]

    def test_failed_dep_skips_downstream(self, session: Session, duckdb_conn):
        """A fails → B (depends on A) doesn't run."""
        run_id = _make_run(session)
        a = MockPhase("A", should_fail=True)
        b = MockPhase("B", dependencies=["A"])
        scheduler = PipelineScheduler(
            phases={"A": a, "B": b},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is False
        assert result.phases_failed == ["A"]
        assert b.run_count == 0

        # B should NOT appear in started events
        started = [e.phase for e in events if e.event_type == EventType.PHASE_STARTED]
        assert "B" not in started


class TestContractExitCheck:
    def test_no_contract_no_exit_check(self, session: Session, duckdb_conn):
        """Without contract thresholds, no EXIT_CHECK events."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", post_verification_dims=["type_fidelity"])
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            # No contract_thresholds
        )
        # Mock _post_verify to return scores without needing real typed tables
        with patch.object(scheduler, "_post_verify", return_value={"structural.types.type_fidelity": 0.8}):
            events, result = _drive(scheduler.run())

        types = [e.event_type for e in events]
        assert EventType.EXIT_CHECK not in types

    def test_exit_check_fires(self, session: Session, duckdb_conn):
        """Post-verify score exceeds contract → EXIT_CHECK yielded."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", post_verification_dims=["type_fidelity"])
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )
        with patch.object(
            scheduler, "_post_verify", return_value={"structural.types.type_fidelity": 0.8}
        ):
            events, result = _drive(scheduler.run())

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        assert "structural.types.type_fidelity" in exit_checks[0].violations

    def test_exit_check_defer(self, session: Session, duckdb_conn):
        """Resolution(DEFER) → issues in deferred_issues."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", post_verification_dims=["type_fidelity"])
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )
        with patch.object(
            scheduler, "_post_verify", return_value={"structural.types.type_fidelity": 0.8}
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.DEFER)},
            )

        assert result.success is True
        assert len(result.deferred_issues) == 1
        assert result.deferred_issues[0].dimension_path == "structural.types.type_fidelity"

    def test_exit_check_abort(self, session: Session, duckdb_conn):
        """Resolution(ABORT) → PipelineResult(success=False)."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", post_verification_dims=["type_fidelity"])
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )
        with patch.object(
            scheduler, "_post_verify", return_value={"structural.types.type_fidelity": 0.8}
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.ABORT)},
            )

        assert result.success is False
        assert "aborted" in (result.error or "").lower()


class TestFixReplay:
    def test_fix_replay(self, session: Session, duckdb_conn):
        """Active Fix record → replayed after phase completes."""
        run_id = _make_run(session)
        phase = MockPhase("profiling")

        # Create a persistent Fix record
        fix = Fix(
            source_id="src-1",
            action_type="override_type",
            target="column:orders.amount",
            parameters={"target_type": "DECIMAL(10,2)"},
            after_phase="profiling",
            status="active",
        )
        session.add(fix)
        session.flush()

        # Create a mock FixExecutor
        mock_executor = MagicMock(spec=FixExecutor)
        mock_executor.execute.return_value = FixResult(success=True, improved=True)

        scheduler = PipelineScheduler(
            phases={"profiling": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            fix_executor=mock_executor,
        )
        events, result = _drive(scheduler.run())

        assert result.success is True
        # Verify the executor was called
        mock_executor.execute.assert_called_once()
        call_args = mock_executor.execute.call_args
        request = call_args[0][0]
        assert request.action_type == "override_type"
        assert request.target == "column:orders.amount"

    def test_fix_replay_updates_status(self, session: Session, duckdb_conn):
        """After replay, Fix.status set to 'applied'."""
        run_id = _make_run(session)
        phase = MockPhase("profiling")

        fix = Fix(
            source_id="src-1",
            action_type="override_type",
            target="column:orders.amount",
            parameters={"target_type": "DECIMAL(10,2)"},
            after_phase="profiling",
            status="active",
        )
        session.add(fix)
        session.flush()
        fix_id = fix.fix_id

        mock_executor = MagicMock(spec=FixExecutor)
        mock_executor.execute.return_value = FixResult(success=True, improved=True)

        scheduler = PipelineScheduler(
            phases={"profiling": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            fix_executor=mock_executor,
        )
        _drive(scheduler.run())

        # Reload and check
        updated_fix = session.get(Fix, fix_id)
        assert updated_fix is not None
        assert updated_fix.status == "applied"
        assert updated_fix.last_applied_at is not None
        assert updated_fix.last_applied_run_id == run_id


class TestInvalidateDownstream:
    def test_invalidate_downstream(self, session: Session, duckdb_conn):
        """Completed downstream phases set back to PENDING."""
        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", dependencies=["A"])
        c = MockPhase("C", dependencies=["B"])

        scheduler = PipelineScheduler(
            phases={"A": a, "B": b, "C": c},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )

        # Simulate all phases having completed
        scheduler._state["A"] = PhaseStatus.COMPLETED
        scheduler._state["B"] = PhaseStatus.COMPLETED
        scheduler._state["C"] = PhaseStatus.COMPLETED

        with patch("dataraum.pipeline.scheduler.cleanup_phase"):
            scheduler._invalidate_downstream("A")

        # B and C (transitive downstream of A) should be back to PENDING
        assert scheduler._state["B"] == PhaseStatus.PENDING
        assert scheduler._state["C"] == PhaseStatus.PENDING
        # A itself should remain COMPLETED
        assert scheduler._state["A"] == PhaseStatus.COMPLETED

        # _ready_phases should now pick up B (its dep A is still completed)
        ready = scheduler._ready_phases()
        assert "B" in ready
        # C is not ready yet (depends on B which is now PENDING)
        assert "C" not in ready


class TestPhaseLog:
    def test_phase_log_written(self, session: Session, duckdb_conn):
        """Each phase execution writes a PhaseLog with correct fields."""
        run_id = _make_run(session)
        phase = MockPhase("alpha")
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        _drive(scheduler.run())

        logs = session.execute(select(PhaseLog)).scalars().all()
        assert len(logs) == 1

        log = logs[0]
        assert log.run_id == run_id
        assert log.source_id == "src-1"
        assert log.phase_name == "alpha"
        assert log.status == "completed"
        assert log.started_at is not None
        assert log.completed_at is not None
        assert log.duration_seconds >= 0.0
        assert log.error is None


class TestPipelineResult:
    def test_pipeline_result(self, session: Session, duckdb_conn):
        """Final result has correct completed/failed/skipped/scores."""
        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", should_fail=True, dependencies=["A"])
        c = MockPhase("C", skip_reason="not needed", dependencies=["A"])
        scheduler = PipelineScheduler(
            phases={"A": a, "B": b, "C": c},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is False  # B failed
        assert "A" in result.phases_completed
        assert "B" in result.phases_failed
        assert "C" in result.phases_skipped
        assert result.error is None
        assert result.deferred_issues == []


class TestDependencyValidation:
    def test_unknown_dependency_raises(self, session: Session, duckdb_conn):
        """Phase with dependency not in phases dict → ValueError at init."""
        run_id = _make_run(session)
        b = MockPhase("B", dependencies=["A"])
        with pytest.raises(ValueError, match="unknown dependencies.*'A'"):
            PipelineScheduler(
                phases={"B": b},
                source_id="src-1",
                run_id=run_id,
                session=session,
                duckdb_conn=duckdb_conn,
            )


class TestDiamondDependency:
    def test_diamond_dependency(self, session: Session, duckdb_conn):
        """Diamond: A → (B, C) → D. B and C run in same wave after A."""
        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", dependencies=["A"])
        c = MockPhase("C", dependencies=["A"])
        d = MockPhase("D", dependencies=["B", "C"])
        scheduler = PipelineScheduler(
            phases={"A": a, "B": b, "C": c, "D": d},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        events, result = _drive(scheduler.run())

        assert result.success is True
        started = [e.phase for e in events if e.event_type == EventType.PHASE_STARTED]

        # A must be first, D must be last, B and C in between
        assert started[0] == "A"
        assert started[-1] == "D"
        assert set(started[1:3]) == {"B", "C"}


class TestFixResolution:
    def test_exit_check_fix_creates_record(self, session: Session, duckdb_conn):
        """Resolution(FIX) with successful fix → Fix record created."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", post_verification_dims=["type_fidelity"])

        mock_executor = MagicMock(spec=FixExecutor)
        mock_executor.execute.return_value = FixResult(success=True, improved=True)

        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
            fix_executor=mock_executor,
        )

        fix_request = FixRequest(
            action_type="override_type",
            target="column:orders.amount",
            parameters={"target_type": "DECIMAL(10,2)"},
            blocked_phase="alpha",
            source_id="src-1",
            run_id=run_id,
        )

        with patch.object(
            scheduler, "_post_verify", return_value={"structural.types.type_fidelity": 0.8}
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={
                    0: Resolution(action=ResolutionAction.FIX, fixes=[fix_request])
                },
            )

        # Fix record should have been created
        fixes = session.execute(select(Fix)).scalars().all()
        assert len(fixes) == 1
        assert fixes[0].action_type == "override_type"
        assert fixes[0].target == "column:orders.amount"
        assert fixes[0].status == "active"

    def test_exit_check_fix_failure_defers_issues(self, session: Session, duckdb_conn):
        """Resolution(FIX) where fix fails → issues deferred, not lost."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", post_verification_dims=["type_fidelity"])

        mock_executor = MagicMock(spec=FixExecutor)
        mock_executor.execute.return_value = FixResult(
            success=False, error="Cannot apply fix"
        )

        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
            fix_executor=mock_executor,
        )

        fix_request = FixRequest(
            action_type="override_type",
            target="column:orders.amount",
            parameters={"target_type": "DECIMAL(10,2)"},
            blocked_phase="alpha",
        )

        with patch.object(
            scheduler, "_post_verify", return_value={"structural.types.type_fidelity": 0.8}
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={
                    0: Resolution(action=ResolutionAction.FIX, fixes=[fix_request])
                },
            )

        # Issues should be deferred, not lost
        assert len(result.deferred_issues) == 1
        assert result.deferred_issues[0].dimension_path == "structural.types.type_fidelity"

        # No Fix record should have been created
        fixes = session.execute(select(Fix)).scalars().all()
        assert len(fixes) == 0


class TestThresholdMatching:
    def test_most_specific_prefix_wins(self, session: Session, duckdb_conn):
        """When multiple prefixes match, most specific wins."""
        run_id = _make_run(session)
        phase = MockPhase("alpha")
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={
                "structural": 0.5,
                "structural.types": 0.3,
            },
        )
        # Most specific prefix "structural.types" (0.3) should win
        assert scheduler._match_threshold("structural.types.type_fidelity") == 0.3

    def test_exact_match_wins_over_prefix(self, session: Session, duckdb_conn):
        """Exact dimension path match takes priority over prefix."""
        run_id = _make_run(session)
        phase = MockPhase("alpha")
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={
                "structural.types": 0.5,
                "structural.types.type_fidelity": 0.1,
            },
        )
        assert scheduler._match_threshold("structural.types.type_fidelity") == 0.1

    def test_no_matching_threshold(self, session: Session, duckdb_conn):
        """Dimension with no matching prefix returns None."""
        run_id = _make_run(session)
        phase = MockPhase("alpha")
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"semantic.units": 0.3},
        )
        assert scheduler._match_threshold("structural.types.type_fidelity") is None
