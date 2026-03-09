"""Tests for PipelineScheduler."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.entropy.dimensions import AnalysisKey
from dataraum.pipeline.base import PhaseContext, PhaseResult, PhaseStatus
from dataraum.pipeline.db_models import PhaseLog, PipelineRun
from dataraum.pipeline.events import EventType
from dataraum.pipeline.fixes import ConfigPatch, FixInput, FixResult
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
        fix_handlers_map: dict | None = None,
        is_quality_gate: bool = False,
    ):
        self._name = name
        self._dependencies = dependencies or []
        self._should_fail = should_fail
        self._skip_reason = skip_reason
        self._produces_analyses = produces_analyses_keys or set()
        self._outputs = outputs
        self._fix_handlers = fix_handlers_map or {}
        self._is_quality_gate = is_quality_gate
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

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        self.run_count += 1
        if self._should_fail:
            return PhaseResult.failed("Intentional failure")
        return PhaseResult.success(
            records_processed=10, records_created=5, outputs=self._outputs
        )

    def should_skip(self, ctx: PhaseContext) -> str | None:
        return self._skip_reason

    @property
    def fix_handlers(self):
        return self._fix_handlers


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
        phase = MockPhase("alpha", produces_analyses_keys={AnalysisKey.TYPING})
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            # No contract_thresholds
        )
        # Mock _post_verify to return scores without needing real typed tables
        with patch.object(scheduler, "_auto_post_verify", return_value={"structural.types.type_fidelity": 0.8}):
            events, result = _drive(scheduler.run())

        types = [e.event_type for e in events]
        assert EventType.EXIT_CHECK not in types

    def test_exit_check_fires(self, session: Session, duckdb_conn):
        """Post-verify score exceeds contract → EXIT_CHECK yielded."""
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
        with patch.object(
            scheduler, "_auto_post_verify", return_value={"structural.types.type_fidelity": 0.8}
        ):
            events, result = _drive(scheduler.run())

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        assert "structural.types.type_fidelity" in exit_checks[0].violations

    def test_exit_check_defer(self, session: Session, duckdb_conn):
        """Resolution(DEFER) → issues in deferred_issues."""
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
        with patch.object(
            scheduler, "_auto_post_verify", return_value={"structural.types.type_fidelity": 0.8}
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
        phase = MockPhase("alpha", produces_analyses_keys={AnalysisKey.TYPING})
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )
        with patch.object(
            scheduler, "_auto_post_verify", return_value={"structural.types.type_fidelity": 0.8}
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.ABORT)},
            )

        assert result.success is False
        assert "aborted" in (result.error or "").lower()


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

        # cleanup_phase only called for COMPLETED (B), not SKIPPED/FAILED
        mock_cleanup.assert_called_once()


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
    def test_assess_impact_populates_affected_targets(self, session: Session, duckdb_conn):
        """_assess_impact uses _column_details to populate affected_targets."""
        run_id = _make_run(session)
        phase = MockPhase("alpha")
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        # Pre-populate _column_details as _post_verify would
        scheduler._column_details = {
            "structural.types.type_fidelity": {
                "column:orders.amount": 0.95,
                "column:orders.discount": 0.88,
                "column:orders.id": 0.10,  # Below threshold
            }
        }

        scores = {"structural.types.type_fidelity": 0.65}
        issues = scheduler._assess_impact(scores, "alpha")

        assert len(issues) == 1
        issue = issues[0]
        # Only columns exceeding threshold (0.3) should be affected
        assert "column:orders.amount" in issue.affected_targets
        assert "column:orders.discount" in issue.affected_targets
        assert "column:orders.id" not in issue.affected_targets

    def test_exit_check_event_carries_column_details(self, session: Session, duckdb_conn):
        """EXIT_CHECK event has column_details populated from _post_verify."""
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

        col_data = {
            "structural.types.type_fidelity": {
                "column:orders.amount": 0.95,
                "column:orders.id": 0.10,
            }
        }

        def mock_post_verify(phase_name):
            scheduler._column_details = dict(col_data)
            return {"structural.types.type_fidelity": 0.8}

        with patch.object(scheduler, "_auto_post_verify", side_effect=mock_post_verify):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.DEFER)},
            )

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        assert exit_checks[0].column_details == col_data

    def test_column_details_cleared_after_exit_check(self, session: Session, duckdb_conn):
        """_column_details is cleared after EXIT_CHECK emission."""
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

        def mock_post_verify(phase_name):
            scheduler._column_details = {
                "structural.types.type_fidelity": {
                    "column:orders.amount": 0.95,
                }
            }
            return {"structural.types.type_fidelity": 0.8}

        with patch.object(scheduler, "_auto_post_verify", side_effect=mock_post_verify):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.DEFER)},
            )

        # After the exit check is processed, _column_details should be empty
        assert scheduler._column_details == {}


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


class TestAutoPostVerifyWithTableDimension:
    """Tests for _auto_post_verify dispatching table-scoped dimensions."""

    def test_auto_post_verify_with_table_dimension(self, session: Session, duckdb_conn):
        """Table dims trigger table-scoped snapshots alongside column snapshots."""
        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector
        from dataraum.entropy.snapshot import Snapshot

        run_id = _make_run(session)
        phase = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING, AnalysisKey.SLICE_VARIANCE},
        )
        scheduler = PipelineScheduler(
            phases={"alpha": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )
        # Pre-populate accumulated analyses so detectors are runnable
        scheduler._accumulated_analyses = {"typing", "slice_variance"}

        # Build a registry with one column-scoped and one table-scoped detector
        class StubCol(EntropyDetector):
            detector_id = "stub_col"
            layer = "structural"
            dimension = "types"
            sub_dimension = "type_fidelity"
            scope = "column"
            required_analyses = ["typing"]

            def detect(self, ctx):
                return [self.create_entropy_object(ctx, score=0.3)]

        class StubTbl(EntropyDetector):
            detector_id = "stub_tbl"
            layer = "semantic"
            dimension = "dimensional"
            sub_dimension = "cross_column_patterns"
            scope = "table"
            required_analyses = ["slice_variance"]

            def detect(self, ctx):
                return [self.create_entropy_object(ctx, score=0.7)]

        registry = DetectorRegistry()
        registry.register(StubCol())
        registry.register(StubTbl())

        # Track take_snapshot calls to verify correct targets
        snapshot_calls: list[str] = []

        def mock_take_snapshot(target, session, duckdb_conn=None, dimensions=None):
            snapshot_calls.append(target)
            if target.startswith("table:"):
                return Snapshot(
                    scores={"cross_column_patterns": 0.7},
                    detectors_run=["stub_tbl"],
                )
            else:
                return Snapshot(
                    scores={"type_fidelity": 0.3},
                    detectors_run=["stub_col"],
                )

        # Mock typed tables and columns (avoid FK constraints)
        mock_table = MagicMock()
        mock_table.table_id = "tbl-1"
        mock_table.table_name = "orders"

        mock_col = MagicMock()
        mock_col.column_name = "amount"

        # Two queries: first for typed tables, second for columns
        call_count = 0

        def mock_execute(stmt, *args, **kwargs):
            nonlocal call_count
            call_count += 1
            result = MagicMock()
            if call_count == 1:  # typed tables query
                result.scalars.return_value.all.return_value = [mock_table]
            elif call_count == 2:  # columns query
                result.scalars.return_value.all.return_value = [mock_col]
            else:
                result.scalars.return_value.all.return_value = []
            return result

        with (
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=registry,
            ),
            patch(
                "dataraum.entropy.snapshot.take_snapshot",
                side_effect=mock_take_snapshot,
            ),
            patch.object(session, "execute", side_effect=mock_execute),
        ):
            scores = scheduler._auto_post_verify("alpha")

        # Both column and table targets were called
        assert any(t.startswith("column:") for t in snapshot_calls)
        assert any(t.startswith("table:") for t in snapshot_calls)

        # Both dimension paths present in result
        assert "structural.types.type_fidelity" in scores
        assert "semantic.dimensional.cross_column_patterns" in scores


class TestFixResolution:
    """Tests for ResolutionAction.FIX — the inline fix flow."""

    def _make_fix_handler(self, rerun_phase: str = "typing"):
        """Create a mock fix handler that records calls and returns a FixResult."""
        calls = []

        def handler(fix_input: FixInput, config: dict) -> FixResult:
            calls.append((fix_input, config))
            return FixResult(
                config_patches=[
                    ConfigPatch(
                        config_path="phases/typing.yaml",
                        operation="set",
                        key_path=["overrides", "amount"],
                        value={"resolved_type": "DECIMAL"},
                        reason="Test fix",
                    )
                ],
                requires_rerun=rerun_phase,
                summary="Applied type override",
            )

        return handler, calls

    def test_fix_calls_handler_and_resets_phase(self, session: Session, duckdb_conn):
        """FIX resolution calls handler, applies patches, resets affected phase."""
        _ensure_source(session)
        run_id = _make_run(session)
        handler, calls = self._make_fix_handler(rerun_phase="alpha")

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            fix_handlers_map={"override_type": handler},
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

        # First pass: alpha runs, post-verify finds violation, EXIT_CHECK fires
        # We send FIX resolution, which should call handler and reset alpha
        fix_input = FixInput(
            action_name="override_type",
            parameters={"resolved_type": "DECIMAL"},
            affected_columns=["orders.amount"],
        )

        # Mock _post_verify to return high score on first call, low on second
        post_verify_count = 0

        def mock_post_verify(phase_name):
            nonlocal post_verify_count
            post_verify_count += 1
            if post_verify_count == 1:
                return {"structural.types.type_fidelity": 0.8}
            # After fix, score drops below threshold
            return {"structural.types.type_fidelity": 0.1}

        with (
            patch.object(scheduler, "_auto_post_verify", side_effect=mock_post_verify),
            patch("dataraum.pipeline.scheduler.cleanup_phase"),
            patch("dataraum.pipeline.scheduler.apply_config_patch") as mock_apply,
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(
                    action=ResolutionAction.FIX,
                    fix_inputs=[fix_input],
                )},
            )

        # Handler was called
        assert len(calls) == 1
        assert calls[0][0].action_name == "override_type"

        # Config patch was applied
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

    def test_fix_handler_not_found_continues(self, session: Session, duckdb_conn):
        """Unknown action_name in FIX resolution logs warning, doesn't crash."""
        _ensure_source(session)
        run_id = _make_run(session)
        alpha = MockPhase("alpha", produces_analyses_keys={AnalysisKey.TYPING})

        scheduler = PipelineScheduler(
            phases={"alpha": alpha},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        fix_input = FixInput(action_name="nonexistent_action")

        with patch.object(
            scheduler, "_auto_post_verify", return_value={"structural.types.type_fidelity": 0.8}
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(
                    action=ResolutionAction.FIX,
                    fix_inputs=[fix_input],
                )},
            )

        # Pipeline completes (no fix happened, no re-run, violation cleared)
        assert result.success is True
        assert alpha.run_count == 1

    def test_fix_invalidates_downstream(self, session: Session, duckdb_conn):
        """FIX resets affected phase and all downstream to PENDING."""
        _ensure_source(session)
        run_id = _make_run(session)
        handler, _ = self._make_fix_handler(rerun_phase="alpha")

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            fix_handlers_map={"override_type": handler},
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

        post_verify_count = 0

        def mock_post_verify(phase_name):
            nonlocal post_verify_count
            post_verify_count += 1
            if post_verify_count == 1:
                return {"structural.types.type_fidelity": 0.8}
            return {"structural.types.type_fidelity": 0.1}

        fix_input = FixInput(action_name="override_type")

        with (
            patch.object(scheduler, "_auto_post_verify", side_effect=mock_post_verify),
            patch("dataraum.pipeline.scheduler.cleanup_phase"),
            patch("dataraum.pipeline.scheduler.apply_config_patch"),
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(
                    action=ResolutionAction.FIX,
                    fix_inputs=[fix_input],
                )},
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
        handler, calls = self._make_fix_handler(rerun_phase="alpha")

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            fix_handlers_map={"override_type": handler},
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
        def mock_post_verify(phase_name):
            return {"structural.types.type_fidelity": 0.8}

        fix_input = FixInput(action_name="override_type")

        with (
            patch.object(scheduler, "_auto_post_verify", side_effect=mock_post_verify),
            patch("dataraum.pipeline.scheduler.cleanup_phase"),
            patch("dataraum.pipeline.scheduler.apply_config_patch"),
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

        # Handler called once (first EXIT_CHECK)
        assert len(calls) == 1

        # Alpha ran twice (original + re-run)
        assert alpha.run_count == 2

        # Two EXIT_CHECKs total
        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 2

        # Deferred on second attempt
        assert len(result.deferred_issues) == 1
        assert result.success is True


class TestFixClearsAutoDerive:
    """Tests that fix re-runs clear auto-derive state."""

    def test_fix_clears_measured_sub_dims_and_scores(self, session: Session, duckdb_conn):
        """_apply_fixes clears _measured_sub_dims and _scores so detectors re-run."""
        from dataraum.entropy.dimensions import SubDimension

        _ensure_source(session)
        run_id = _make_run(session)

        handler = lambda fi, cfg: FixResult(  # noqa: E731
            config_patches=[],
            requires_rerun="alpha",
            summary="test",
        )

        alpha = MockPhase(
            "alpha",
            produces_analyses_keys={AnalysisKey.TYPING},
            fix_handlers_map={"override_type": handler},
        )

        scheduler = PipelineScheduler(
            phases={"alpha": alpha},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )

        # Simulate state after initial measurement pass
        scheduler._measured_sub_dims = {SubDimension.TYPE_FIDELITY, SubDimension.NULL_RATIO}
        scheduler._scores = {
            "structural.types.type_fidelity": 0.8,
            "value.nulls.null_ratio": 0.1,
        }
        scheduler._column_details = {"structural.types.type_fidelity": {"col:a": 0.8}}

        fix_input = FixInput(action_name="override_type")
        with (
            patch("dataraum.pipeline.scheduler.cleanup_phase"),
            patch("dataraum.pipeline.scheduler.apply_config_patch"),
            patch("dataraum.core.config.load_phase_config", return_value={}),
        ):
            scheduler._apply_fixes([fix_input])

        assert scheduler._measured_sub_dims == set()
        assert scheduler._scores == {}
        assert scheduler._column_details == {}


class TestForceSequential:
    """Tests for force_sequential parameter."""

    def test_force_sequential_prevents_parallel(self, session: Session, duckdb_conn, engine):
        """With force_sequential=True, multiple ready phases run sequentially."""
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

        scheduler = PipelineScheduler(
            phases={"A": a, "B": b, "C": c},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            session_factory=session_scope,
            force_sequential=True,
        )

        events, result = _drive(scheduler.run())

        assert result.success is True
        assert a.run_count == 1
        assert b.run_count == 1
        assert c.run_count == 1

        # B and C ran sequentially (both completed)
        completed = [e.phase for e in events if e.event_type == EventType.PHASE_COMPLETED]
        assert set(completed) == {"A", "B", "C"}


class TestFindFixHandler:
    """Tests for _find_fix_handler helper."""

    def test_finds_handler_on_correct_phase(self, session: Session, duckdb_conn):
        """Handler is found on the phase that declares it."""
        run_id = _make_run(session)
        handler = lambda fi, cfg: FixResult()  # noqa: E731

        alpha = MockPhase("alpha", fix_handlers_map={"override_type": handler})
        beta = MockPhase("beta", dependencies=["alpha"])

        scheduler = PipelineScheduler(
            phases={"alpha": alpha, "beta": beta},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )

        found_handler, found_phase = scheduler._find_fix_handler("override_type")
        assert found_handler is handler
        assert found_phase == "alpha"

    def test_returns_none_for_unknown_action(self, session: Session, duckdb_conn):
        """Unknown action returns (None, "")."""
        run_id = _make_run(session)
        alpha = MockPhase("alpha")

        scheduler = PipelineScheduler(
            phases={"alpha": alpha},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
        )

        found_handler, found_phase = scheduler._find_fix_handler("nonexistent")
        assert found_handler is None
        assert found_phase == ""


class TestExitCheckFixableActions:
    """Tests for fixable_actions on EXIT_CHECK events."""

    def test_exit_check_includes_fixable_actions(self, session: Session, duckdb_conn):
        """EXIT_CHECK event carries fixable_actions from detector registry."""
        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector

        run_id = _make_run(session)
        # Phase declares fix_handlers — single source of truth for phase_name
        dummy_handler = lambda fi, cfg: None  # noqa: E731
        phase = MockPhase(
            "typing",
            produces_analyses_keys={AnalysisKey.TYPING},
            fix_handlers_map={"override_type": dummy_handler},
        )
        scheduler = PipelineScheduler(
            phases={"typing": phase},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            contract_thresholds={"structural.types": 0.3},
        )

        # Detector declares which actions are fixable (no phase_name)
        class FixableDetector(EntropyDetector):
            detector_id = "test_fixable"
            layer = "structural"
            dimension = "types"
            sub_dimension = "type_fidelity"
            scope = "column"
            required_analyses: list[str] = []

            @property
            def fixable_actions(self):
                return {"override_type"}

            def detect(self, ctx):
                return []

        registry = DetectorRegistry()
        registry.register(FixableDetector())

        with (
            patch.object(
                scheduler, "_auto_post_verify",
                return_value={"structural.types.type_fidelity": 0.8},
            ),
            patch(
                "dataraum.entropy.detectors.base.get_default_registry",
                return_value=registry,
            ),
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.DEFER)},
            )

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        fa = exit_checks[0].fixable_actions
        assert "structural.types.type_fidelity" in fa
        assert fa["structural.types.type_fidelity"] == [
            {"action_name": "override_type", "phase_name": "typing"}
        ]

    def test_exit_check_empty_fixable_actions(self, session: Session, duckdb_conn):
        """EXIT_CHECK with no fixable detectors has empty fixable_actions."""
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

        with patch.object(
            scheduler, "_auto_post_verify",
            return_value={"structural.types.type_fidelity": 0.8},
        ):
            events, result = _drive(
                scheduler.run(),
                resolutions={0: Resolution(action=ResolutionAction.DEFER)},
            )

        exit_checks = [e for e in events if e.event_type == EventType.EXIT_CHECK]
        assert len(exit_checks) == 1
        # Default detectors have no fixable_actions
        assert exit_checks[0].fixable_actions == {} or all(
            v == [] for v in exit_checks[0].fixable_actions.values()
        )
