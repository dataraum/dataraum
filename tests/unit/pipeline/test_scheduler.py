"""Tests for PipelineScheduler."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector
from dataraum.entropy.dimensions import AnalysisKey, Dimension, Layer, SubDimension
from dataraum.entropy.gate import (
    GateResult,
    assess_contracts,
    match_threshold,
)
from dataraum.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.db_models import PhaseLog, PipelineRun
from dataraum.pipeline.events import EventType
from dataraum.pipeline.fixes import FixInput
from dataraum.pipeline.fixes.models import FixSchema
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
        produces_analyses_keys: set[AnalysisKey] | None = None,
        outputs: dict | None = None,
        is_quality_gate: bool = False,
        detectors: list[str] | None = None,
    ):
        self._name = name
        self._dependencies = dependencies or []
        self._should_fail = should_fail
        self._skip_reason = skip_reason
        self._produces_analyses = produces_analyses_keys or set()
        self._outputs = outputs
        self._is_quality_gate = is_quality_gate
        self._detectors = detectors or []
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
    def produces_analyses(self) -> set[AnalysisKey]:
        return self._produces_analyses

    @property
    def is_quality_gate(self) -> bool:
        return self._is_quality_gate

    @property
    def detectors(self) -> list[str]:
        return self._detectors

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        self.run_count += 1
        if self._should_fail:
            return PhaseResult.failed("Intentional failure")
        return PhaseResult.success(records_processed=10, records_created=5, outputs=self._outputs)

    def should_skip(self, ctx: PhaseContext) -> str | None:
        return self._skip_reason


def _ensure_source(session: Session, source_id: str = "src-1") -> None:
    """Ensure a Source row exists for FK-dependent writes (e.g. fix_ledger)."""
    from dataraum.storage import Source

    existing = session.get(Source, source_id)
    if not existing:
        session.add(Source(source_id=source_id, name=source_id, source_type="csv"))
        session.flush()


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

        # PHASE_COMPLETED carries metadata from PhaseResult
        completed = [e for e in events if e.event_type == EventType.PHASE_COMPLETED][0]
        assert completed.records_processed == 10
        assert completed.records_created == 5
        assert completed.duration_seconds > 0

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
        phase = MockPhase("alpha", is_quality_gate=True)
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            # No contract_thresholds
        )
        gate = GateResult(scores={"structural.types.type_fidelity": 0.8})
        with patch("dataraum.pipeline.scheduler.aggregate_at_gate", return_value=gate):
            events, result = _drive(scheduler.run())

        types = [e.event_type for e in events]
        assert EventType.EXIT_CHECK not in types

    def test_exit_check_fires(self, session: Session, duckdb_conn):
        """Gate score exceeds contract → EXIT_CHECK yielded."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", is_quality_gate=True)
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )
        gate = GateResult(scores={"structural.types.type_fidelity": 0.8})
        with patch("dataraum.pipeline.scheduler.aggregate_at_gate", return_value=gate):
            events, result = _drive(scheduler.run())

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        assert "structural.types.type_fidelity" in exit_checks[0].violations

    def test_exit_check_defer(self, session: Session, duckdb_conn):
        """Resolution(DEFER) → issues in deferred_issues."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", is_quality_gate=True)
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )
        gate = GateResult(scores={"structural.types.type_fidelity": 0.8})
        with patch("dataraum.pipeline.scheduler.aggregate_at_gate", return_value=gate):
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
        phase = MockPhase("alpha", is_quality_gate=True)
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )
        gate = GateResult(scores={"structural.types.type_fidelity": 0.8})
        with patch("dataraum.pipeline.scheduler.aggregate_at_gate", return_value=gate):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.ABORT)},
            )

        assert result.success is False
        assert "aborted" in (result.error or "").lower()

    def test_non_gate_phase_no_exit_check(self, session: Session, duckdb_conn):
        """Non-gate phases never trigger EXIT_CHECK even with contracts."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", produces_analyses_keys={AnalysisKey.TYPING})
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )
        events, result = _drive(scheduler.run())

        types = [e.event_type for e in events]
        assert EventType.POST_VERIFICATION not in types
        assert EventType.EXIT_CHECK not in types


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

    def test_invalidate_skipped_and_failed(self, session: Session, duckdb_conn):
        """SKIPPED and FAILED downstream phases also reset to PENDING."""
        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", dependencies=["A"])
        c = MockPhase("C", dependencies=["A"])
        d = MockPhase("D", dependencies=["A"])

        scheduler = PipelineScheduler(
            phases={"A": a, "B": b, "C": c, "D": d},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )

        scheduler._state["A"] = PhaseStatus.COMPLETED
        scheduler._state["B"] = PhaseStatus.COMPLETED
        scheduler._state["C"] = PhaseStatus.SKIPPED
        scheduler._state["D"] = PhaseStatus.FAILED

        with patch("dataraum.pipeline.scheduler.cleanup_phase") as mock_cleanup:
            scheduler._invalidate_downstream("A")

        # All three downstream should be PENDING
        assert scheduler._state["B"] == PhaseStatus.PENDING
        assert scheduler._state["C"] == PhaseStatus.PENDING
        assert scheduler._state["D"] == PhaseStatus.PENDING

        # cleanup_phase called for COMPLETED (B) and SKIPPED (C), not FAILED (D)
        assert mock_cleanup.call_count == 2


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


class TestEntropyScoresInResult:
    def test_entropy_scores_in_final_scores(self, session: Session, duckdb_conn):
        """Entropy phase outputs flow into PipelineResult.final_scores."""
        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase(
            "B",
            dependencies=["A"],
            outputs={
                "entropy_scores": {
                    "structural.types": 0.12,
                    "semantic.business_meaning": 0.45,
                }
            },
        )
        scheduler = PipelineScheduler(
            phases={"A": a, "B": b},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        _, result = _drive(scheduler.run())

        assert result.success
        assert result.final_scores["structural.types"] == pytest.approx(0.12)
        assert result.final_scores["semantic.business_meaning"] == pytest.approx(0.45)


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


class TestColumnDetails:
    def test_assess_contracts_populates_affected_targets(self):
        """assess_contracts uses column_details to populate affected_targets."""
        column_details = {
            "structural.types.type_fidelity": {
                "column:orders.amount": 0.95,
                "column:orders.discount": 0.88,
                "column:orders.id": 0.10,  # Below threshold
            }
        }

        scores = {"structural.types.type_fidelity": 0.65}
        thresholds = {"structural.types": 0.3}
        issues = assess_contracts(scores, thresholds, column_details, "alpha")

        assert len(issues) == 1
        issue = issues[0]
        # Only columns exceeding threshold (0.3) should be affected
        assert "column:orders.amount" in issue.affected_targets
        assert "column:orders.discount" in issue.affected_targets
        assert "column:orders.id" not in issue.affected_targets

    def test_exit_check_event_carries_column_details(self, session: Session, duckdb_conn):
        """EXIT_CHECK event has column_details populated from gate measurement."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", is_quality_gate=True)
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        col_data = {
            "structural.types.type_fidelity": {
                "column:orders.amount": 0.95,
                "column:orders.id": 0.10,
            }
        }

        gate = GateResult(
            scores={"structural.types.type_fidelity": 0.8},
            column_details=col_data,
        )
        with patch("dataraum.pipeline.scheduler.aggregate_at_gate", return_value=gate):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.DEFER)},
            )

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        assert exit_checks[0].column_details == col_data


class TestThresholdMatching:
    def test_most_specific_prefix_wins(self):
        """When multiple prefixes match, most specific wins."""
        thresholds = {
            "structural": 0.5,
            "structural.types": 0.3,
        }
        # Most specific prefix "structural.types" (0.3) should win
        assert match_threshold("structural.types.type_fidelity", thresholds) == 0.3

    def test_exact_match_wins_over_prefix(self):
        """Exact dimension path match takes priority over prefix."""
        thresholds = {
            "structural.types": 0.5,
            "structural.types.type_fidelity": 0.1,
        }
        assert match_threshold("structural.types.type_fidelity", thresholds) == 0.1

    def test_no_matching_threshold(self):
        """Dimension with no matching prefix returns None."""
        thresholds = {"semantic.units": 0.3}
        assert match_threshold("structural.types.type_fidelity", thresholds) is None


class TestParallelExecution:
    """Tests for ThreadPoolExecutor-based parallel phase execution."""

    def test_parallel_diamond_with_session_factory(self, session: Session, duckdb_conn, engine):
        """With session_factory, B and C in diamond run via ThreadPoolExecutor."""
        from contextlib import contextmanager

        from sqlalchemy.orm import sessionmaker

        factory = sessionmaker(bind=engine, expire_on_commit=False)

        @contextmanager
        def session_scope():
            s = factory()
            try:
                yield s
                s.commit()
            except Exception:
                s.rollback()
                raise
            finally:
                s.close()

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
            session_factory=session_scope,
        )

        events, result = _drive(scheduler.run())

        assert result.success is True
        assert set(result.phases_completed) == {"A", "B", "C", "D"}

        # B and C should both have STARTED events before D
        started = [e.phase for e in events if e.event_type == EventType.PHASE_STARTED]
        assert started[0] == "A"
        assert started[-1] == "D"
        assert set(started[1:3]) == {"B", "C"}

    def test_sequential_fallback_without_session_factory(self, session: Session, duckdb_conn):
        """Without session_factory, falls back to sequential even with multiple ready."""
        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", dependencies=["A"])
        c = MockPhase("C", dependencies=["A"])

        scheduler = PipelineScheduler(
            phases={"A": a, "B": b, "C": c},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            # No session_factory → sequential
        )

        events, result = _drive(scheduler.run())

        assert result.success is True
        assert a.run_count == 1
        assert b.run_count == 1
        assert c.run_count == 1

    def test_parallel_phase_failure(self, session: Session, duckdb_conn, engine):
        """A failing phase in parallel wave doesn't crash other phases."""
        from contextlib import contextmanager

        from sqlalchemy.orm import sessionmaker

        factory = sessionmaker(bind=engine, expire_on_commit=False)

        @contextmanager
        def session_scope():
            s = factory()
            try:
                yield s
                s.commit()
            except Exception:
                s.rollback()
                raise
            finally:
                s.close()

        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", dependencies=["A"], should_fail=True)
        c = MockPhase("C", dependencies=["A"])

        scheduler = PipelineScheduler(
            phases={"A": a, "B": b, "C": c},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            session_factory=session_scope,
        )

        events, result = _drive(scheduler.run())

        assert result.success is False
        assert "B" in result.phases_failed
        assert "C" in result.phases_completed

    def test_single_ready_phase_runs_sequentially(self, session: Session, duckdb_conn, engine):
        """Single phase in wave uses sequential path even with session_factory."""
        from contextlib import contextmanager

        from sqlalchemy.orm import sessionmaker

        factory = sessionmaker(bind=engine, expire_on_commit=False)

        @contextmanager
        def session_scope():
            s = factory()
            try:
                yield s
                s.commit()
            except Exception:
                s.rollback()
                raise
            finally:
                s.close()

        run_id = _make_run(session)
        a = MockPhase("A")
        b = MockPhase("B", dependencies=["A"])

        scheduler = PipelineScheduler(
            phases={"A": a, "B": b},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            session_factory=session_scope,
        )

        events, result = _drive(scheduler.run())

        # Both phases run successfully — each was the only phase in its wave
        assert result.success is True
        assert result.phases_completed == ["A", "B"]


class TestAggregateAtGate:
    """Tests for aggregate_at_gate reading persisted records."""

    def test_aggregate_at_gate_reads_records(self, session: Session, duckdb_conn):
        """aggregate_at_gate reads persisted EntropyObjectRecords."""
        from dataraum.entropy.db_models import EntropyObjectRecord
        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector
        from dataraum.entropy.gate import aggregate_at_gate

        class StubCol(EntropyDetector):
            detector_id = "stub_col"
            layer = Layer.STRUCTURAL
            dimension = Dimension.TYPES
            sub_dimension = SubDimension.TYPE_FIDELITY
            scope = "column"
            required_analyses = [AnalysisKey.TYPING]

            def detect(self, ctx):
                return []

        class StubTbl(EntropyDetector):
            detector_id = "stub_tbl"
            layer = Layer.SEMANTIC
            dimension = Dimension.DIMENSIONAL
            sub_dimension = SubDimension.CROSS_COLUMN_PATTERNS
            scope = "table"
            required_analyses = [AnalysisKey.SLICE_VARIANCE]

            def detect(self, ctx):
                return []

        registry = DetectorRegistry()
        registry.register(StubCol())
        registry.register(StubTbl())

        # Insert mock records directly
        _ensure_source(session)
        session.add(
            EntropyObjectRecord(
                source_id="src-1",
                target="column:orders.amount",
                layer="structural",
                dimension="types",
                sub_dimension="type_fidelity",
                score=0.3,
                detector_id="stub_col",
            )
        )
        session.add(
            EntropyObjectRecord(
                source_id="src-1",
                target="table:orders",
                layer="semantic",
                dimension="dimensional",
                sub_dimension="cross_column_patterns",
                score=0.7,
                detector_id="stub_tbl",
            )
        )
        session.flush()

        with patch(
            "dataraum.entropy.detectors.base.get_default_registry",
            return_value=registry,
        ):
            gate_result = aggregate_at_gate(session, "src-1", ["stub_col", "stub_tbl"])

        assert "structural.types.type_fidelity" in gate_result.scores
        assert "semantic.dimensional.cross_column_patterns" in gate_result.scores


def _make_test_fix_schema(action: str, phase_name: str) -> FixSchema:
    """Create a test FixSchema."""
    return FixSchema(
        action=action,
        target="config",
        description="Test fix",
        config_path="phases/typing.yaml",
        key_path=["overrides"],
        operation="set",
        requires_rerun=phase_name,
        routing="preprocess",
        fields={},
    )


def _make_test_detector_registry(
    action: str, phase_name: str, dim_path: str = "structural.types.type_fidelity"
) -> tuple[DetectorRegistry, FixSchema]:
    """Create a DetectorRegistry with a test detector and a corresponding FixSchema.

    Returns:
        Tuple of (registry, fix_schema) — callers must also mock the YAML
        loader to return the schema.
    """
    parts = dim_path.split(".")
    layer_str, dim_str, subdim_str = parts[0], parts[1], parts[2]

    class TestDetector(EntropyDetector):
        detector_id = "test_detector"
        layer = Layer(layer_str)
        dimension = Dimension(dim_str)
        sub_dimension = SubDimension(subdim_str)
        scope = "column"
        required_analyses: list[AnalysisKey] = []

        def detect(self, ctx):
            return []

    registry = DetectorRegistry()
    registry.register(TestDetector())
    schema = _make_test_fix_schema(action, phase_name)
    return registry, schema


class TestFixResolution:
    """Tests for ResolutionAction.FIX — the inline fix flow."""

    def test_fix_calls_handler_and_resets_phase(self, session: Session, duckdb_conn):
        """FIX resolution builds documents, applies via interpreter, resets affected phase."""
        _ensure_source(session)
        run_id = _make_run(session)

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            is_quality_gate=True,
        )
        beta = MockPhase("beta", dependencies=["alpha"])

        scheduler = PipelineScheduler(
            phases={"alpha": alpha, "beta": beta},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        fix_input = FixInput(
            action_name="override_type",
            parameters={"resolved_type": "DECIMAL"},
            affected_columns=["orders.amount"],
        )

        # Mock aggregate_at_gate to return high score first, low after fix
        measure_count = 0

        def mock_measure(session, source_id, detector_ids):
            nonlocal measure_count
            measure_count += 1
            if measure_count == 1:
                return GateResult(scores={"structural.types.type_fidelity": 0.8})
            return GateResult(scores={"structural.types.type_fidelity": 0.1})

        detector_registry, test_schema = _make_test_detector_registry("override_type", "alpha")
        with (
            patch(
                "dataraum.pipeline.scheduler.aggregate_at_gate",
                side_effect=mock_measure,
            ),
            patch("dataraum.pipeline.scheduler.cleanup_phase"),
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_registry,
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_schemas_for_detector",
                return_value=[test_schema],
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_fix_schema",
                return_value=test_schema,
            ),
            patch(
                "dataraum.pipeline.fixes.interpreters.apply_and_persist",
                return_value=[],
            ) as mock_apply,
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={
                    0: Resolution(
                        action=ResolutionAction.FIX,
                        fix_inputs=[fix_input],
                    )
                },
            )

        # apply_and_persist was called
        mock_apply.assert_called_once()

        # Alpha ran twice (original + re-run after fix)
        assert alpha.run_count == 2

        # Pipeline succeeded (score now below threshold)
        assert result.success is True

        # Fix was logged to ledger
        from dataraum.documentation.ledger import get_active_fixes

        fixes = get_active_fixes(session, "src-1")
        assert len(fixes) == 1
        assert fixes[0].action_name == "override_type"
        assert fixes[0].table_name == "orders"
        assert fixes[0].column_name == "amount"

    def test_fix_schema_not_found_continues(self, session: Session, duckdb_conn):
        """Unknown action_name in FIX resolution logs warning, doesn't crash."""
        _ensure_source(session)
        run_id = _make_run(session)
        alpha = MockPhase("alpha", is_quality_gate=True)

        scheduler = PipelineScheduler(
            phases={"alpha": alpha},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        fix_input = FixInput(action_name="nonexistent_action")

        gate = GateResult(scores={"structural.types.type_fidelity": 0.8})
        with patch("dataraum.pipeline.scheduler.aggregate_at_gate", return_value=gate):
            events, result = _drive(
                scheduler.run(),
                resolutions={
                    0: Resolution(
                        action=ResolutionAction.FIX,
                        fix_inputs=[fix_input],
                    )
                },
            )

        # Pipeline completes — schema not found, violation silently dropped
        assert result.success is True
        assert alpha.run_count == 1
        assert result.deferred_issues == []

    def test_fix_invalidates_downstream(self, session: Session, duckdb_conn):
        """FIX resets affected phase and all downstream to PENDING."""
        _ensure_source(session)
        run_id = _make_run(session)

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            is_quality_gate=True,
        )
        beta = MockPhase("beta", dependencies=["alpha"])
        gamma = MockPhase("gamma", dependencies=["beta"])

        scheduler = PipelineScheduler(
            phases={"alpha": alpha, "beta": beta, "gamma": gamma},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        measure_count = 0

        def mock_measure(session, source_id, detector_ids):
            nonlocal measure_count
            measure_count += 1
            if measure_count == 1:
                return GateResult(scores={"structural.types.type_fidelity": 0.8})
            return GateResult(scores={"structural.types.type_fidelity": 0.1})

        fix_input = FixInput(action_name="override_type", affected_columns=["orders.amount"])
        detector_registry, test_schema = _make_test_detector_registry("override_type", "alpha")

        with (
            patch(
                "dataraum.pipeline.scheduler.aggregate_at_gate",
                side_effect=mock_measure,
            ),
            patch("dataraum.pipeline.scheduler.cleanup_phase"),
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_registry,
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_schemas_for_detector",
                return_value=[test_schema],
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_fix_schema",
                return_value=test_schema,
            ),
            patch(
                "dataraum.pipeline.fixes.interpreters.apply_and_persist",
                return_value=[],
            ),
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={
                    0: Resolution(
                        action=ResolutionAction.FIX,
                        fix_inputs=[fix_input],
                    )
                },
            )

        # Alpha ran twice (original + re-run after fix).
        # Beta and gamma were still PENDING when fix happened, so they
        # only run once (after the fixed alpha re-completes).
        assert alpha.run_count == 2
        assert beta.run_count == 1
        assert gamma.run_count == 1
        assert result.success is True

    def test_fix_multiple_attempts(self, session: Session, duckdb_conn):
        """If fix doesn't resolve violation, EXIT_CHECK fires again."""
        _ensure_source(session)
        run_id = _make_run(session)

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            is_quality_gate=True,
        )

        scheduler = PipelineScheduler(
            phases={"alpha": alpha},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        # Score stays high — fix doesn't help, second attempt defers
        def mock_measure(session, source_id, detector_ids):
            return GateResult(scores={"structural.types.type_fidelity": 0.8})

        fix_input = FixInput(action_name="override_type", affected_columns=["orders.amount"])
        detector_registry, test_schema = _make_test_detector_registry("override_type", "alpha")

        with (
            patch(
                "dataraum.pipeline.scheduler.aggregate_at_gate",
                side_effect=mock_measure,
            ),
            patch("dataraum.pipeline.scheduler.cleanup_phase"),
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_registry,
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_schemas_for_detector",
                return_value=[test_schema],
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_fix_schema",
                return_value=test_schema,
            ),
            patch(
                "dataraum.pipeline.fixes.interpreters.apply_and_persist",
                return_value=[],
            ) as mock_apply,
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={
                    0: Resolution(
                        action=ResolutionAction.FIX,
                        fix_inputs=[fix_input],
                    ),
                    1: Resolution(action=ResolutionAction.DEFER),
                },
            )

        # apply_and_persist called once (first EXIT_CHECK)
        mock_apply.assert_called_once()

        # Alpha ran twice (original + re-run)
        assert alpha.run_count == 2

        # Two EXIT_CHECKs total
        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 2

        # Deferred on second attempt
        assert len(result.deferred_issues) == 1
        assert result.success is True


class TestPostprocessFixRouting:
    """Tests for postprocess fix routing — no cascade-clean or phase re-run."""

    def test_postprocess_fix_skips_cleanup(self, session: Session, duckdb_conn):
        """Postprocess fix applies documents but does NOT reset phases."""
        _ensure_source(session)
        run_id = _make_run(session)

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            is_quality_gate=True,
        )

        scheduler = PipelineScheduler(
            phases={"alpha": alpha},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        fix_input = FixInput(
            action_name="document_accepted_type_fidelity",
            affected_columns=["orders.amount"],
        )

        # Create a postprocess schema (routing="postprocess", no requires_rerun)
        postprocess_schema = FixSchema(
            action="document_accepted_type_fidelity",
            target="metadata",
            description="Accept finding",
            routing="postprocess",
            gate="quality_review",
            fields={},
        )

        # After postprocess fix, score drops below threshold
        measure_count = 0

        def mock_measure(session, source_id, detector_ids):
            nonlocal measure_count
            measure_count += 1
            if measure_count == 1:
                return GateResult(scores={"structural.types.type_fidelity": 0.8})
            return GateResult(scores={"structural.types.type_fidelity": 0.1})

        detector_registry, _ = _make_test_detector_registry(
            "document_accepted_type_fidelity", "alpha"
        )

        with (
            patch(
                "dataraum.pipeline.scheduler.aggregate_at_gate",
                side_effect=mock_measure,
            ),
            patch("dataraum.pipeline.scheduler.cleanup_phase") as mock_cleanup,
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_registry,
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_schemas_for_detector",
                return_value=[postprocess_schema],
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_fix_schema",
                return_value=postprocess_schema,
            ),
            patch(
                "dataraum.pipeline.fixes.interpreters.apply_and_persist",
                return_value=[],
            ) as mock_apply,
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={
                    0: Resolution(
                        action=ResolutionAction.FIX,
                        fix_inputs=[fix_input],
                    )
                },
            )

        # Fix was applied (MetadataInterpreter patches DB directly)
        mock_apply.assert_called_once()

        # cleanup_phase was NOT called (postprocess skips cascade-clean)
        mock_cleanup.assert_not_called()

        # Alpha ran only ONCE (no re-run for postprocess)
        assert alpha.run_count == 1

        # Pipeline succeeded
        assert result.success is True


class TestFixScoresCleared:
    """Tests that fix re-runs clear measurement state."""

    def test_fix_clears_scores_on_rerun(self, session: Session, duckdb_conn):
        """After FIX resolution, scores are cleared so gate re-measures fresh."""
        _ensure_source(session)
        run_id = _make_run(session)

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            is_quality_gate=True,
        )

        scheduler = PipelineScheduler(
            phases={"alpha": alpha},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        # First measurement: high score. Second: low (after fix cleared scores).
        measure_count = 0

        def mock_measure(session, source_id, detector_ids):
            nonlocal measure_count
            measure_count += 1
            if measure_count == 1:
                return GateResult(scores={"structural.types.type_fidelity": 0.8})
            # After fix, return different dimension to prove old scores were cleared
            return GateResult(scores={"value.nulls.null_ratio": 0.05})

        fix_input = FixInput(action_name="override_type", affected_columns=["orders.amount"])
        detector_registry, test_schema = _make_test_detector_registry("override_type", "alpha")
        with (
            patch(
                "dataraum.pipeline.scheduler.aggregate_at_gate",
                side_effect=mock_measure,
            ),
            patch("dataraum.pipeline.scheduler.cleanup_phase"),
            patch("dataraum.core.config.load_phase_config", return_value={}),
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_registry,
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_schemas_for_detector",
                return_value=[test_schema],
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_fix_schema",
                return_value=test_schema,
            ),
            patch(
                "dataraum.pipeline.fixes.interpreters.apply_and_persist",
                return_value=[],
            ),
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={
                    0: Resolution(
                        action=ResolutionAction.FIX,
                        fix_inputs=[fix_input],
                    ),
                },
            )

        # Old score gone, only new score present (proves scores were cleared)
        assert "structural.types.type_fidelity" not in result.final_scores
        assert result.final_scores.get("value.nulls.null_ratio") == pytest.approx(0.05)


class TestDetectorRegistryFixSchema:
    """Tests for DetectorRegistry.get_fix_schema() (delegates to YAML loader)."""

    def test_finds_schema_via_yaml_loader(self):
        """Schema is found by action name via YAML loader."""
        test_schema = _make_test_fix_schema("override_type", "alpha")
        registry = DetectorRegistry()
        with patch(
            "dataraum.entropy.fix_schemas.get_fix_schema",
            return_value=test_schema,
        ):
            schema = registry.get_fix_schema("override_type")
        assert schema is not None
        assert schema.action == "override_type"
        assert schema.requires_rerun == "alpha"

    def test_returns_none_for_unknown_action(self):
        """Unknown action returns None."""
        registry = DetectorRegistry()
        with patch(
            "dataraum.entropy.fix_schemas.get_fix_schema",
            return_value=None,
        ):
            assert registry.get_fix_schema("nonexistent") is None


class TestExitCheckAvailableFixes:
    """Tests for available_fixes on EXIT_CHECK events."""

    def test_exit_check_includes_available_fixes(self, session: Session, duckdb_conn):
        """EXIT_CHECK event carries available_fixes from detector fix_schemas."""
        run_id = _make_run(session)
        phase = MockPhase(
            "typing",
            produces_analyses_keys={AnalysisKey.TYPING},
            is_quality_gate=True,
        )
        scheduler = PipelineScheduler(
            phases={"typing": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        detector_registry, test_schema = _make_test_detector_registry("override_type", "typing")

        gate = GateResult(scores={"structural.types.type_fidelity": 0.8})
        with (
            patch(
                "dataraum.pipeline.scheduler.aggregate_at_gate",
                return_value=gate,
            ),
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_registry,
            ),
            patch(
                "dataraum.entropy.fix_schemas.get_schemas_for_detector",
                return_value=[test_schema],
            ),
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.DEFER)},
            )

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        fa = exit_checks[0].available_fixes
        assert "structural.types.type_fidelity" in fa
        assert fa["structural.types.type_fidelity"] == [
            {"action_name": "override_type", "phase_name": "typing"}
        ]

    def test_exit_check_empty_available_fixes(self, session: Session, duckdb_conn):
        """EXIT_CHECK with no fixable detectors has empty available_fixes."""
        run_id = _make_run(session)
        phase = MockPhase("alpha", is_quality_gate=True)
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        # Use empty detector registry — no detectors = no available fixes
        empty_registry = DetectorRegistry()

        gate = GateResult(scores={"structural.types.type_fidelity": 0.8})
        with (
            patch(
                "dataraum.pipeline.scheduler.aggregate_at_gate",
                return_value=gate,
            ),
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=empty_registry,
            ),
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.DEFER)},
            )

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        assert exit_checks[0].available_fixes == {}


class TestAnalysisCoverageValidation:
    """Tests for _validate_analysis_coverage at scheduler startup."""

    def test_warns_on_uncovered_detector(self, session: Session, duckdb_conn):
        """Warning logged when detector needs analyses no phase produces."""
        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector

        class NeedsStats(EntropyDetector):
            detector_id = "needs_stats"
            layer = Layer.VALUE
            dimension = Dimension.OUTLIERS
            sub_dimension = SubDimension.OUTLIER_RATE
            scope = "column"
            required_analyses = [AnalysisKey.STATISTICS]

            def detect(self, ctx):
                return []

        detector_reg = DetectorRegistry()
        detector_reg.register(NeedsStats())

        run_id = _make_run(session)
        # Phase only produces TYPING — not STATISTICS
        alpha = MockPhase("alpha", produces_analyses_keys={AnalysisKey.TYPING})

        with (
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_reg,
            ),
            patch("dataraum.pipeline.scheduler.logger") as mock_logger,
        ):
            PipelineScheduler(
                phases={"alpha": alpha},
                source_id="src-1",
                run_id=run_id,
                session=session,
                duckdb_conn=duckdb_conn,
            )

        mock_logger.warning.assert_called_once()
        call_kwargs = mock_logger.warning.call_args
        assert call_kwargs[0][0] == "detector_analysis_gap"
        assert call_kwargs[1]["detector"] == "needs_stats"
        assert "statistics" in call_kwargs[1]["missing"]

    def test_no_warning_when_covered(self, session: Session, duckdb_conn):
        """No warning when all detector analyses are covered."""
        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector

        class NeedsTyping(EntropyDetector):
            detector_id = "needs_typing"
            layer = Layer.STRUCTURAL
            dimension = Dimension.TYPES
            sub_dimension = SubDimension.TYPE_FIDELITY
            scope = "column"
            required_analyses = [AnalysisKey.TYPING]

            def detect(self, ctx):
                return []

        detector_reg = DetectorRegistry()
        detector_reg.register(NeedsTyping())

        run_id = _make_run(session)
        alpha = MockPhase("alpha", produces_analyses_keys={AnalysisKey.TYPING})

        with (
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=detector_reg,
            ),
            patch("dataraum.pipeline.scheduler.logger") as mock_logger,
        ):
            PipelineScheduler(
                phases={"alpha": alpha},
                source_id="src-1",
                run_id=run_id,
                session=session,
                duckdb_conn=duckdb_conn,
            )

        mock_logger.warning.assert_not_called()
