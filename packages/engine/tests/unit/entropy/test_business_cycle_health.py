"""Tests for business_cycle_health entropy detector."""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.semantic.business_cycle_health import (
    BusinessCycleHealthDetector,
)
from dataraum.storage import init_database


@pytest.fixture
def detector() -> BusinessCycleHealthDetector:
    return BusinessCycleHealthDetector()


def _make_context(
    cycles: list | None = None,
    table_id: str = "t1",
    table_name: str = "orders",
) -> DetectorContext:
    ctx = DetectorContext(
        table_id=table_id,
        table_name=table_name,
    )
    if cycles is not None:
        ctx.analysis_results["business_cycles"] = cycles
    return ctx


def _make_cycle(
    *,
    cycle_name: str = "order_to_cash",
    cycle_type: str = "revenue",
    canonical_type: str | None = "order_to_cash",
    confidence: float = 0.9,
    completion_rate: float = 0.85,
    total_records: int = 1000,
    completed_cycles: int = 850,
    tables_involved: list[str] | None = None,
) -> MagicMock:
    c = MagicMock()
    c.cycle_name = cycle_name
    c.cycle_type = cycle_type
    c.canonical_type = canonical_type
    c.confidence = confidence
    c.completion_rate = completion_rate
    c.total_records = total_records
    c.completed_cycles = completed_cycles
    c.tables_involved = tables_involved or ["orders", "invoices"]
    return c


class TestDetectNoCycles:
    def test_no_cycles_returns_zero(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context(cycles=[])
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 0.0
        assert objects[0].evidence[0]["reason"] == "no_cycles_involving_table"

    def test_no_data_loaded(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context()
        objects = detector.detect(ctx)
        assert len(objects) == 1
        assert objects[0].score == 0.0


class TestDetectHealthy:
    def test_high_completion_high_confidence(self, detector: BusinessCycleHealthDetector):
        """90% confidence, 85% completion → score = max(0.15, 0.10) = 0.15."""
        ctx = _make_context(cycles=[_make_cycle(confidence=0.9, completion_rate=0.85)])
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.15)

    def test_perfect_cycle(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context(cycles=[_make_cycle(confidence=1.0, completion_rate=1.0)])
        objects = detector.detect(ctx)
        assert objects[0].score == 0.0


class TestDetectUnhealthy:
    def test_low_completion(self, detector: BusinessCycleHealthDetector):
        """30% completion → score = max(0.7, 0.1) = 0.7."""
        ctx = _make_context(cycles=[_make_cycle(completion_rate=0.3, confidence=0.9)])
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.7)

    def test_low_confidence(self, detector: BusinessCycleHealthDetector):
        """40% confidence → score = max(0.15, 0.6) = 0.6."""
        ctx = _make_context(cycles=[_make_cycle(confidence=0.4, completion_rate=0.85)])
        objects = detector.detect(ctx)
        assert objects[0].score == pytest.approx(0.6)

    def test_null_completion_rate(self, detector: BusinessCycleHealthDetector):
        """None completion_rate treated as 0.0 → score = 1.0."""
        ctx = _make_context(cycles=[_make_cycle(completion_rate=None, confidence=0.9)])
        # MagicMock returns the default we set, but let's override
        cycle = _make_cycle()
        cycle.completion_rate = None
        cycle.confidence = 0.9
        ctx = _make_context(cycles=[cycle])
        objects = detector.detect(ctx)
        assert objects[0].score == 1.0


class TestMaxAggregation:
    def test_worst_cycle_drives_score(self, detector: BusinessCycleHealthDetector):
        """Two cycles — worst one wins."""
        ctx = _make_context(
            cycles=[
                _make_cycle(cycle_name="healthy", confidence=0.95, completion_rate=0.9),
                _make_cycle(cycle_name="unhealthy", confidence=0.3, completion_rate=0.2),
            ]
        )
        objects = detector.detect(ctx)
        # max(1-0.2, 1-0.3) = max(0.8, 0.7) = 0.8
        assert objects[0].score == pytest.approx(0.8)


class TestEvidence:
    def test_evidence_per_cycle(self, detector: BusinessCycleHealthDetector):
        ctx = _make_context(
            cycles=[
                _make_cycle(cycle_name="c1"),
                _make_cycle(cycle_name="c2"),
            ]
        )
        objects = detector.detect(ctx)
        evidence = objects[0].evidence
        assert len(evidence) == 2
        assert evidence[0]["cycle_name"] == "c1"
        assert evidence[1]["cycle_name"] == "c2"
        assert "confidence" in evidence[0]
        assert "completion_rate" in evidence[0]


class TestDetectorProperties:
    def test_detector_id(self, detector: BusinessCycleHealthDetector):
        assert detector.detector_id == "business_cycle_health"

    def test_scope(self, detector: BusinessCycleHealthDetector):
        assert detector.scope == "table"

    def test_layer(self, detector: BusinessCycleHealthDetector):
        assert str(detector.layer) == "semantic"

    def test_required_analyses(self, detector: BusinessCycleHealthDetector):
        assert str(detector.required_analyses[0]) == "business_cycles"


@pytest.fixture
def db_session() -> Iterator[Session]:
    """In-memory SQLite session with all tables created — for the real-DB
    load_data path (StaticPool + explicit dispose, mirrors the context-builder
    test fixture)."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _set_pragma(dbapi_conn, _record):  # noqa: ANN001, ANN202
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.close()

    init_database(engine)
    sess = sessionmaker(bind=engine)()
    try:
        yield sess
    finally:
        sess.close()
        engine.dispose()


def _seed_cycle(
    session: Session,
    *,
    sess_id: str,
    run_id: str,
    tables_involved: list[str],
    cycle_name: str = "order_to_cash",
) -> None:
    from dataraum.analysis.cycles.db_models import DetectedBusinessCycle

    session.add(
        DetectedBusinessCycle(
            cycle_id=str(uuid4()),
            session_id=sess_id,
            run_id=run_id,
            cycle_name=cycle_name,
            cycle_type="revenue",
            canonical_type="order_to_cash",
            confidence=0.9,
            completion_rate=0.85,
            tables_involved=tables_involved,
            total_records=1000,
            completed_cycles=850,
        )
    )


def _promote_om_head(session: Session, sess_id: str, run_id: str) -> None:
    from dataraum.storage.snapshot_head import MetadataSnapshotHead, session_head_target

    session.add(
        MetadataSnapshotHead(
            target=session_head_target(sess_id), stage="operating_model", run_id=run_id
        )
    )


def _seed_investigation(session: Session, sess_id: str) -> None:
    from dataraum.investigation.db_models import InvestigationSession

    session.add(InvestigationSession(session_id=sess_id, intent="test"))


class TestLoadData:
    """The cross-stage promoted-head resolution in load_data (DAT-455).

    The detector runs in the *detect* pass — BEFORE the operating_model stage
    writes cycles — so it must resolve the session's promoted operating_model
    head and fail closed when there isn't one. The detect() tests above pre-seed
    ``analysis_results`` and never exercise this path; the handoff itself names it
    the trickiest seam, so it gets real-DB coverage here.
    """

    def test_reads_cycles_at_the_promoted_operating_model_head(
        self, detector: BusinessCycleHealthDetector, db_session: Session
    ) -> None:
        sess_id = "sess-load"
        _seed_investigation(db_session, sess_id)
        _seed_cycle(db_session, sess_id=sess_id, run_id="run-om", tables_involved=["orders"])
        _promote_om_head(db_session, sess_id, "run-om")
        db_session.flush()

        ctx = DetectorContext(
            table_id="t1", table_name="orders", session_id=sess_id, session=db_session
        )
        detector.load_data(ctx)

        loaded = ctx.analysis_results.get("business_cycles", [])
        assert len(loaded) == 1
        assert loaded[0].cycle_name == "order_to_cash"

    def test_no_promoted_head_reads_nothing(
        self, detector: BusinessCycleHealthDetector, db_session: Session
    ) -> None:
        """The common case: the detector runs before operating_model exists."""
        sess_id = "sess-nohead"
        _seed_investigation(db_session, sess_id)
        _seed_cycle(db_session, sess_id=sess_id, run_id="run-om", tables_involved=["orders"])
        # No MetadataSnapshotHead promoted → head_run_id returns None.
        db_session.flush()

        ctx = DetectorContext(
            table_id="t1", table_name="orders", session_id=sess_id, session=db_session
        )
        detector.load_data(ctx)

        assert ctx.analysis_results.get("business_cycles") is None

    def test_cross_run_isolation_reads_only_the_promoted_run(
        self, detector: BusinessCycleHealthDetector, db_session: Session
    ) -> None:
        """A superseded run's cycle must not bleed into the promoted-head read."""
        sess_id = "sess-xrun"
        _seed_investigation(db_session, sess_id)
        _seed_cycle(
            db_session,
            sess_id=sess_id,
            run_id="run-om",
            tables_involved=["orders"],
            cycle_name="current",
        )
        _seed_cycle(
            db_session,
            sess_id=sess_id,
            run_id="run-old",
            tables_involved=["orders"],
            cycle_name="superseded",
        )
        _promote_om_head(db_session, sess_id, "run-om")
        db_session.flush()

        ctx = DetectorContext(
            table_id="t1", table_name="orders", session_id=sess_id, session=db_session
        )
        detector.load_data(ctx)

        loaded = ctx.analysis_results.get("business_cycles", [])
        assert [c.cycle_name for c in loaded] == ["current"]

    def test_table_not_in_cycle_is_not_loaded(
        self, detector: BusinessCycleHealthDetector, db_session: Session
    ) -> None:
        sess_id = "sess-othertable"
        _seed_investigation(db_session, sess_id)
        _seed_cycle(db_session, sess_id=sess_id, run_id="run-om", tables_involved=["invoices"])
        _promote_om_head(db_session, sess_id, "run-om")
        db_session.flush()

        ctx = DetectorContext(
            table_id="t1", table_name="orders", session_id=sess_id, session=db_session
        )
        detector.load_data(ctx)

        assert ctx.analysis_results.get("business_cycles") is None
