"""Tests for PipelineScheduler."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import patch
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.entropy.dimensions import AnalysisKey
from dataraum.entropy.measurement import match_threshold
from dataraum.pipeline.base import PhaseContext, PhaseResult
from dataraum.pipeline.db_models import PhaseLog, PipelineRun
from dataraum.pipeline.events import EventType
from dataraum.pipeline.phases.base import BasePhase
from dataraum.pipeline.scheduler import PipelineScheduler

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
        detectors: list[str] | None = None,
    ):
        self._name = name
        self._dependencies = dependencies or []
        self._should_fail = should_fail
        self._skip_reason = skip_reason
        self._produces_analyses = produces_analyses_keys or set()
        self._outputs = outputs
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
    def detectors(self) -> list[str]:
        return self._detectors

    def _run(self, ctx: PhaseContext) -> PhaseResult:
        self.run_count += 1
        if self._should_fail:
            return PhaseResult.failed("Intentional failure")
        return PhaseResult.success(records_processed=10, records_created=5, outputs=self._outputs)

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


def _drive(gen):
    """Drive a scheduler generator to completion.

    Args:
        gen: The generator returned by scheduler.run().

    Returns:
        Tuple of (list of events, PipelineResult).
    """
    events = []
    try:
        event = next(gen)
        events.append(event)
        while True:
            event = next(gen)
            events.append(event)
    except StopIteration as e:
        return events, e.value


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestEmptyPipeline:
    def test_empty_pipeline(self, session: Session, duckdb_conn):
        """No phases -> immediate PipelineResult(success=True)."""
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
        """should_skip returns reason -> SKIPPED event, PhaseLog(status='skipped')."""
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
        """run returns FAILED -> FAILED event, PhaseLog(status='failed')."""
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
        """B depends on A -> A runs first."""
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
        """C->B->A: runs in order A, B, C."""
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
        """A fails -> B (depends on A) doesn't run."""
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


class TestPhaseLogWithFactory:
    """PhaseLog via factory path — covers _check_should_skip and _write_phase_log with scoped sessions."""

    @staticmethod
    def _make_manager_stub(duckdb_conn):
        from contextlib import contextmanager
        from unittest.mock import MagicMock

        stub = MagicMock()

        @contextmanager
        def _cursor():
            yield duckdb_conn

        stub.duckdb_cursor = _cursor
        return stub

    def test_skipped_phase_log_via_factory(self, session: Session, duckdb_conn, engine):
        """Skipped phase writes PhaseLog through the factory session path."""
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
        a = MockPhase("A", skip_reason="already done")
        b = MockPhase("B", dependencies=["A"])

        scheduler = PipelineScheduler(
            phases={"A": a, "B": b},
            source_id="src-1",
            run_id=run_id,
            session=session,
            duckdb_conn=duckdb_conn,
            session_factory=session_scope,
            manager=self._make_manager_stub(duckdb_conn),
        )

        events, result = _drive(scheduler.run())

        assert "A" in result.phases_skipped
        assert "B" in result.phases_completed

        # PhaseLog for the skipped phase must exist (written via factory session)
        logs = session.execute(select(PhaseLog).where(PhaseLog.phase_name == "A")).scalars().all()
        assert len(logs) == 1
        assert logs[0].status == "skipped"
        assert logs[0].error == "already done"


class TestPipelineResult:
    def test_pipeline_result(self, session: Session, duckdb_conn):
        """Final result has correct completed/failed/skipped fields."""
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
        assert result.final_scores == {}


class TestDependencyValidation:
    def test_unknown_dependency_raises(self, session: Session, duckdb_conn):
        """Phase with dependency not in phases dict -> ValueError at init."""
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


class TestDiamondDependency:
    def test_diamond_dependency(self, session: Session, duckdb_conn):
        """Diamond: A -> (B, C) -> D. B and C run in same wave after A."""
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


class TestParallelExecution:
    """Tests for ThreadPoolExecutor-based parallel phase execution."""

    @staticmethod
    def _make_manager_stub(duckdb_conn):
        """Minimal manager stub with duckdb_cursor() context manager."""
        from contextlib import contextmanager
        from unittest.mock import MagicMock

        stub = MagicMock()

        @contextmanager
        def _cursor():
            yield duckdb_conn

        stub.duckdb_cursor = _cursor
        return stub

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
            manager=self._make_manager_stub(duckdb_conn),
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
            # No session_factory -> sequential
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
            manager=self._make_manager_stub(duckdb_conn),
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
            manager=self._make_manager_stub(duckdb_conn),
        )

        events, result = _drive(scheduler.run())

        # Both phases run successfully -- each was the only phase in its wave
        assert result.success is True
        assert result.phases_completed == ["A", "B"]


class TestAnalysisCoverageValidation:
    """Tests for _validate_analysis_coverage at scheduler startup."""

    def test_warns_on_uncovered_detector(self, session: Session, duckdb_conn):
        """Warning logged when detector needs analyses no phase produces."""
        from dataraum.entropy.detectors.base import DetectorRegistry, EntropyDetector
        from dataraum.entropy.dimensions import Dimension, Layer, SubDimension

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
        # Phase only produces TYPING -- not STATISTICS
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
        from dataraum.entropy.dimensions import Dimension, Layer, SubDimension

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
