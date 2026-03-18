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
        """50% NULLs across columns → sqrt-boosted score ≈ 0.707."""
        view = _make_enriched_view(["customers__country", "customers__city"])
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0.5,)
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        assert len(objects) == 1
        assert objects[0].score == pytest.approx(0.7071, abs=1e-3)


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
        """Score is sqrt-boosted mean of per-column null rates: sqrt(0.4) ≈ 0.632."""
        view = _make_enriched_view(["a", "b"])
        conn = MagicMock()
        conn.execute.return_value.fetchone.side_effect = [(0.2,), (0.6,)]
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        assert objects[0].score == pytest.approx(0.6325, abs=1e-3)


class TestResolutionOption:
    def test_resolution_option_present(self, detector: DimensionCoverageDetector):
        """High NULL columns produce an investigate_relationship resolution."""
        view = _make_enriched_view(["dim__col"])
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0.8,)
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        assert len(objects[0].resolution_options) == 1
        opt = objects[0].resolution_options[0]
        assert opt.action == "investigate_relationship"
        assert opt.effort == "medium"

    def test_no_resolution_when_low_nulls(self, detector: DimensionCoverageDetector):
        """Low NULL rates → no resolution options."""
        view = _make_enriched_view(["dim__col"])
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (0.1,)
        ctx = _make_context(view=view, duckdb_conn=conn)

        objects = detector.detect(ctx)

        assert len(objects[0].resolution_options) == 0


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
        """load_data queries EnrichedView by view_name."""
        mock_view = _make_enriched_view(["col1"])
        session = MagicMock()
        session.execute.return_value.scalar_one_or_none.return_value = mock_view

        ctx = DetectorContext(view_name="enriched_orders", session=session)
        detector.load_data(ctx)

        assert ctx.analysis_results["enriched_view"] is mock_view

    def test_load_data_no_session(self, detector: DimensionCoverageDetector):
        """load_data is no-op without session."""
        ctx = DetectorContext(view_name="enriched_orders")
        detector.load_data(ctx)
        assert "enriched_view" not in ctx.analysis_results

    def test_load_data_no_view_name(self, detector: DimensionCoverageDetector):
        """load_data is no-op without view_name."""
        ctx = DetectorContext(session=MagicMock())
        detector.load_data(ctx)
        assert "enriched_view" not in ctx.analysis_results


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
