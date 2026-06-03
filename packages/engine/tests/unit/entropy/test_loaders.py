"""Tests for entropy detector loader helpers."""

from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.entropy.detectors.loaders import (
    load_correlation,
    load_drift_summaries,
    load_semantic,
    load_statistics,
    load_typing,
)
from dataraum.storage import init_database


class TestLoadTyping:
    def test_returns_none_when_no_data(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_typing(session, "col1") is None

    def test_returns_decision_with_candidate(self):
        session = MagicMock()
        td = MagicMock()
        td.decided_type = "DECIMAL"
        td.decision_source = "inference"
        td.decision_reason = "high confidence"

        tc = MagicMock()
        tc.confidence = 0.95
        tc.parse_success_rate = 0.99
        tc.failed_examples = ["N/A"]
        tc.detected_pattern = r"\d+\.\d+"
        tc.pattern_match_rate = 0.98
        tc.detected_unit = "USD"
        tc.unit_confidence = 0.8

        session.execute.return_value.scalar_one_or_none.side_effect = [td, tc]

        result = load_typing(session, "col1")
        assert result is not None
        assert result["resolved_type"] == "DECIMAL"
        assert result["confidence"] == 0.95
        assert result["detected_unit"] == "USD"

    def test_returns_candidate_only_when_no_decision(self):
        session = MagicMock()
        tc = MagicMock()
        tc.data_type = "INTEGER"
        tc.confidence = 0.8
        tc.parse_success_rate = 1.0
        tc.failed_examples = []
        tc.detected_pattern = None
        tc.pattern_match_rate = None
        tc.detected_unit = None
        tc.unit_confidence = None

        session.execute.return_value.scalar_one_or_none.side_effect = [None, tc]

        result = load_typing(session, "col1")
        assert result is not None
        assert result["data_type"] == "INTEGER"
        assert result["confidence"] == 0.8


class TestLoadStatistics:
    def test_returns_none_when_no_profile(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_statistics(session, "col1") is None

    def test_returns_stats_with_quality(self):
        session = MagicMock()
        sp = MagicMock()
        sp.null_count = 5
        sp.total_count = 100
        sp.distinct_count = 80
        sp.cardinality_ratio = 0.8
        sp.profile_data = {"numeric_stats": {}}

        qm = MagicMock()
        qm.iqr_outlier_ratio = 0.02
        qm.zscore_outlier_ratio = 0.01
        qm.has_outliers = True
        qm.benford_compliant = True
        qm.quality_data = {"outlier_detection": {"iqr_outlier_count": 2}}

        session.execute.return_value.scalar_one_or_none.side_effect = [sp, qm]

        result = load_statistics(session, "col1")
        assert result is not None
        assert result["null_ratio"] == 0.05
        assert result["quality"]["benford_compliant"] is True
        assert "outlier_detection" in result["quality"]

    def test_excluded_column_omits_outlier_detection(self):
        """When outlier analysis was skipped (iqr_outlier_ratio is NULL),
        the quality dict should NOT contain outlier_detection so detectors
        return [] ('not assessed') instead of a false 0-score."""
        session = MagicMock()
        sp = MagicMock()
        sp.null_count = 0
        sp.total_count = 100
        sp.distinct_count = 50
        sp.cardinality_ratio = 0.5
        sp.profile_data = {}

        qm = MagicMock()
        qm.iqr_outlier_ratio = None
        qm.zscore_outlier_ratio = None
        qm.has_outliers = None
        qm.benford_compliant = True
        qm.quality_data = {"benford_analysis": {"is_compliant": True}}

        session.execute.return_value.scalar_one_or_none.side_effect = [sp, qm]

        result = load_statistics(session, "col1")
        assert result is not None
        assert "outlier_detection" not in result["quality"]
        assert result["quality"]["benford_compliant"] is True


class TestLoadSemantic:
    def test_returns_none_when_no_annotation(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_semantic(session, "col1") is None

    def test_returns_semantic_dict(self):
        session = MagicMock()
        sa = MagicMock()
        sa.semantic_role = "measure"
        sa.entity_type = "monetary"
        sa.business_name = "Revenue"
        sa.business_description = "Total revenue"
        sa.confidence = 0.9
        sa.business_concept = "revenue"
        sa.unit_source_column = None

        session.execute.return_value.scalar_one_or_none.return_value = sa

        result = load_semantic(session, "col1")
        assert result is not None
        assert result["semantic_role"] == "measure"
        assert "unit_source_column" not in result

    def test_includes_unit_source_column(self):
        session = MagicMock()
        sa = MagicMock()
        sa.semantic_role = "measure"
        sa.entity_type = "monetary"
        sa.business_name = "Amount"
        sa.business_description = ""
        sa.confidence = 0.7
        sa.business_concept = None
        sa.unit_source_column = "currency"

        session.execute.return_value.scalar_one_or_none.return_value = sa

        result = load_semantic(session, "col1")
        assert result is not None
        assert result["unit_source_column"] == "currency"


class TestLoadCorrelation:
    def test_returns_none_when_no_derived_columns(self):
        session = MagicMock()
        session.execute.return_value.scalars.return_value.all.return_value = []
        assert load_correlation(session, "col1", "amount") is None

    def test_returns_derived_column_info(self):
        session = MagicMock()
        dc = MagicMock()
        dc.formula = "price * quantity"
        dc.match_rate = 0.98
        dc.derivation_type = "multiplication"
        dc.source_column_ids = ["col2", "col3"]

        session.execute.return_value.scalars.return_value.all.return_value = [dc]

        result = load_correlation(session, "col1", "total")
        assert result is not None
        assert len(result["derived_columns"]) == 1
        assert result["derived_columns"][0]["derived_column_name"] == "total"
        assert result["derived_columns"][0]["match_rate"] == 0.98


class TestLoadDriftSummaries:
    def test_returns_none_when_no_column(self):
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = None
        assert load_drift_summaries(session, "col1", "tbl1", "orders") is None

    def test_returns_none_when_no_slice_tables(self):
        session = MagicMock()
        col = MagicMock()
        col.column_name = "amount"

        col_result = MagicMock()
        col_result.scalar_one_or_none.return_value = col

        slice_result = MagicMock()
        slice_result.scalars.return_value.all.return_value = []

        cols_result = MagicMock()
        cols_result.scalars.return_value.all.return_value = []

        session.execute.side_effect = [col_result, slice_result, cols_result]

        assert load_drift_summaries(session, "col1", "tbl1", "orders") is None


@pytest.fixture
def real_session():
    """In-memory SQLite session with all tables; FKs off so we skip parent rows."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _record):  # noqa: ANN001, ANN202
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    try:
        with factory() as s:
            yield s
    finally:
        engine.dispose()


class TestLoaderRunIdFilter:
    """DAT-413: the run_id-stamped loaders read only the requested run's row.

    Real-DB tests (the MagicMock tests above can't exercise the SQL filter): a
    single ``TypeDecision`` stamped with one run is returned when the loader is
    asked for that run and skipped for any other; ``run_id=None`` (non-detect
    callers) keeps the legacy unfiltered behavior.
    """

    def _insert_decision(self, session, column_id: str, run_id: str) -> None:
        from dataraum.analysis.typing.db_models import TypeDecision

        session.add(
            TypeDecision(
                decision_id=str(uuid4()),
                session_id="sess-1",
                column_id=column_id,
                run_id=run_id,
                decided_type="DECIMAL",
                decision_source="automatic",
            )
        )
        session.flush()

    def test_load_typing_picks_matching_run(self, real_session):
        self._insert_decision(real_session, "col-1", "run-A")

        # Requested run matches the stored row → returned.
        matched = load_typing(real_session, "col-1", run_id="run-A")
        assert matched is not None
        assert matched["resolved_type"] == "DECIMAL"

        # A different run → the filter excludes the only row → None.
        assert load_typing(real_session, "col-1", run_id="run-B") is None

        # No run_id (non-detect / legacy callers) → unfiltered → still found.
        assert load_typing(real_session, "col-1") is not None
