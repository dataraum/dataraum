"""Tests for DimensionCoverageDetector."""

from unittest.mock import MagicMock

import pytest

from dataraum.entropy.detectors.base import DetectorContext
from dataraum.entropy.detectors.semantic.dimension_coverage import DimensionCoverageDetector


@pytest.fixture
def detector() -> DimensionCoverageDetector:
    return DimensionCoverageDetector()


def _make_enriched_view(dimension_columns: list[str] | None = None) -> MagicMock:
    """Create a mock EnrichedView."""
    view = MagicMock()
    view.dimension_columns = dimension_columns
    view.view_name = "enriched_orders"
    view.fact_table_id = "tbl1"
    return view


def _make_context(
    view: MagicMock | None = None,
    duckdb_conn: MagicMock | None = None,
) -> DetectorContext:
    """Build a DetectorContext with enriched_view pre-populated."""
    ctx = DetectorContext(
        view_name="enriched_orders",
        duckdb_conn=duckdb_conn,
    )
    if view is not None:
        ctx.analysis_results["enriched_view"] = view
    return ctx


class TestDetectAllPopulated:
    def test_score_near_zero(self, detector: DimensionCoverageDetector):
        """All dimension columns fully populated → score ≈ 0.0."""
        view = _make_enriched_view(["customers__country", "customers__city"])
        conn = MagicMock()
        # Both columns have 0% NULLs
        conn.execute.return_value.fetchone.return_value = (0.0,)
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        assert len(objects) == 1
        assert objects[0].score == pytest.approx(0.0)
        assert objects[0].sub_dimension == "dimension_coverage"


class TestDetectPartialNulls:
    def test_score_reflects_null_rate(self, detector: DimensionCoverageDetector):
        """50% NULLs across columns → honest mean rate 0.5 (no boost, DAT-442)."""
        view = _make_enriched_view(["customers__country", "customers__city"])
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0.5,)
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        assert len(objects) == 1
        assert objects[0].score == pytest.approx(0.5, abs=1e-6)


class TestDetectNoDimensionColumns:
    def test_score_zero_when_no_dims(self, detector: DimensionCoverageDetector):
        """No dimension columns → score 0.0 (no uncertainty)."""
        view = _make_enriched_view([])
        ctx = _make_context(view=view)

        objects = detector.detect(ctx)

        assert len(objects) == 1
        assert objects[0].score == 0.0
        assert objects[0].evidence[0]["reason"] == "no_dimension_columns"

    def test_score_zero_when_dims_none(self, detector: DimensionCoverageDetector):
        """dimension_columns is None → score 0.0."""
        view = _make_enriched_view(None)
        ctx = _make_context(view=view)

        objects = detector.detect(ctx)

        assert len(objects) == 1
        assert objects[0].score == 0.0


class TestCanRun:
    def test_can_run_without_enriched_view(self, detector: DimensionCoverageDetector):
        """Returns False when enriched_view not in analysis_results."""
        ctx = DetectorContext(view_name="enriched_orders")
        assert detector.can_run(ctx) is False

    def test_can_run_with_enriched_view(self, detector: DimensionCoverageDetector):
        """Returns True when enriched_view is present."""
        view = _make_enriched_view(["col1"])
        ctx = _make_context(view=view)
        assert detector.can_run(ctx) is True


class TestEvidence:
    def test_evidence_per_column_rates(self, detector: DimensionCoverageDetector):
        """Evidence contains per-column null rates."""
        view = _make_enriched_view(["customers__country", "products__category"])
        conn = MagicMock()
        # First column 20% NULLs, second 60% NULLs
        conn.execute.return_value.fetchone.side_effect = [(0.2,), (0.6,)]
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        evidence = objects[0].evidence
        assert len(evidence) == 2
        assert evidence[0]["column"] == "customers__country"
        assert evidence[0]["null_rate"] == pytest.approx(0.2)
        assert evidence[1]["column"] == "products__category"
        assert evidence[1]["null_rate"] == pytest.approx(0.6)

    def test_mean_score_from_mixed_rates(self, detector: DimensionCoverageDetector):
        """Score is the honest mean of per-column null rates: (0.2+0.6)/2 = 0.4 (no boost)."""
        view = _make_enriched_view(["a", "b"])
        conn = MagicMock()
        conn.execute.return_value.fetchone.side_effect = [(0.2,), (0.6,)]
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        assert objects[0].score == pytest.approx(0.4, abs=1e-6)


class TestTargetRef:
    def test_view_target_ref(self):
        """DetectorContext with view_name produces view: target_ref."""
        ctx = DetectorContext(view_name="enriched_orders")
        assert ctx.target_ref == "view:enriched_orders"

    def test_view_takes_precedence(self):
        """view_name takes precedence over column_name and table_name."""
        ctx = DetectorContext(
            view_name="enriched_orders",
            table_name="orders",
            column_name="amount",
        )
        assert ctx.target_ref == "view:enriched_orders"


class TestLoadData:
    def test_load_data_populates_enriched_view(self, detector: DimensionCoverageDetector):
        """load_data queries the fact table's EnrichedView by table_id (DAT-415)."""
        mock_view = _make_enriched_view(["col1"])
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = mock_view

        ctx = DetectorContext(table_id="fact-1", session=session)
        detector.load_data(ctx)

        assert ctx.analysis_results["enriched_view"] is mock_view

    def test_load_data_no_session(self, detector: DimensionCoverageDetector):
        """load_data is no-op without session."""
        ctx = DetectorContext(table_id="fact-1")
        detector.load_data(ctx)
        assert "enriched_view" not in ctx.analysis_results

    def test_load_data_no_table_id(self, detector: DimensionCoverageDetector):
        """load_data is no-op without table_id (a non-fact context → skipped)."""
        ctx = DetectorContext(session=MagicMock())
        detector.load_data(ctx)
        assert "enriched_view" not in ctx.analysis_results


class TestLoadNullRatesFiltersOrigin:
    def test_fact_origin_columns_are_excluded(self, session):
        """DAT-811: the enriched view now also registers the fact's own f.* columns
        (origin='fact'); coverage measures only the JOINED dimensions. ``_load_null_rates``
        returns the dim column and NEVER the fact-origin sibling under the SAME
        view_table_id — a real-session proof the ``origin='dimension'`` filter discriminates
        (a regression dropping it would surface the fact column here)."""
        from uuid import uuid4

        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.storage import Column, Source, Table

        src = Source(source_id=str(uuid4()), name="csv", source_type="csv")
        session.add(src)
        session.flush()
        view_table = Table(
            table_id=str(uuid4()),
            source_id=src.source_id,
            table_name="enriched_orders",
            layer="enriched",
            duckdb_path="enriched_orders",
            row_count=10,
        )
        session.add(view_table)
        session.flush()

        dim = Column(
            column_id=str(uuid4()),
            table_id=view_table.table_id,
            column_name="customers__country",
            column_position=0,
            origin="dimension",
        )
        fact = Column(
            column_id=str(uuid4()),
            table_id=view_table.table_id,
            column_name="amount",
            column_position=1,
            origin="fact",
        )
        session.add_all([dim, fact])
        session.flush()
        for col, nr in ((dim, 0.2), (fact, 0.9)):
            session.add(
                StatisticalProfile(
                    profile_id=str(uuid4()),
                    column_id=col.column_id,
                    run_id="run-1",
                    layer="enriched",
                    total_count=10,
                    null_count=int(nr * 10),
                    null_ratio=nr,
                    profile_data={},
                )
            )
        session.flush()

        view = MagicMock()
        view.view_table_id = view_table.table_id
        ctx = DetectorContext(view_name="enriched_orders", session=session)

        rates = DimensionCoverageDetector._load_null_rates(ctx, view)

        assert rates == {"customers__country": 0.2}  # the fact-origin 'amount' is excluded


class TestQueryFallback:
    def test_no_duckdb_conn_returns_1(self, detector: DimensionCoverageDetector):
        """Without duckdb_conn, null rate defaults to 1.0 (worst case)."""
        view = _make_enriched_view(["col1"])
        ctx = _make_context(view=view, duckdb_conn=None)

        objects = detector.detect(ctx)

        assert objects[0].score == pytest.approx(1.0)

    def test_duckdb_exception_returns_1(self, detector: DimensionCoverageDetector):
        """DuckDB query failure → null rate defaults to 1.0."""
        view = _make_enriched_view(["col1"])
        conn = MagicMock()
        conn.execute.side_effect = RuntimeError("DuckDB error")
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        assert objects[0].score == pytest.approx(1.0)
