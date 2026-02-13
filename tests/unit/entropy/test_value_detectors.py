"""Tests for value layer entropy detectors."""

import pytest

from dataraum.entropy.detectors import (
    DetectorContext,
    NullRatioDetector,
    OutlierRateDetector,
    TemporalDriftDetector,
)


class TestNullRatioDetector:
    """Tests for NullRatioDetector."""

    @pytest.fixture
    def detector(self) -> NullRatioDetector:
        """Create detector instance."""
        return NullRatioDetector()

    def test_no_nulls(self, detector: NullRatioDetector):
        """Test entropy is 0 for no nulls."""
        context = DetectorContext(
            table_name="orders",
            column_name="id",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.0,
                    "null_count": 0,
                    "total_count": 1000,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "none"

    def test_low_nulls(self, detector: NullRatioDetector):
        """Test low entropy for minimal nulls."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.02,
                    "null_count": 20,
                    "total_count": 1000,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.02, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "minimal"

    def test_high_nulls(self, detector: NullRatioDetector):
        """Test high entropy for significant nulls."""
        context = DetectorContext(
            table_name="orders",
            column_name="discount",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.5,
                    "null_count": 500,
                    "total_count": 1000,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.5, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "critical"
        # Should have resolution options
        actions = [opt.action for opt in results[0].resolution_options]
        assert "document_null_semantics" in actions
        assert "transform_filter_nulls" in actions

    def test_max_entropy_at_full_nulls(self, detector: NullRatioDetector):
        """Test entropy is 1.0 for fully null column."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "null_ratio": 1.0,
                }
            },
        )

        results = detector.detect(context)

        assert results[0].score == pytest.approx(1.0, abs=0.01)

    def test_resolution_cascade_dimensions(self, detector: NullRatioDetector):
        """Test resolution options include cascade dimensions."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.2,
                }
            },
        )

        results = detector.detect(context)

        # document_null_semantics should cascade to semantic.business_meaning
        null_semantics_opt = next(
            (
                opt
                for opt in results[0].resolution_options
                if opt.action == "document_null_semantics"
            ),
            None,
        )
        assert null_semantics_opt is not None
        assert "semantic.business_meaning" in null_semantics_opt.cascade_dimensions

    def test_detector_properties(self, detector: NullRatioDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "null_ratio"
        assert detector.layer == "value"
        assert detector.dimension == "nulls"
        assert detector.required_analyses == ["statistics"]


class TestOutlierRateDetector:
    """Tests for OutlierRateDetector."""

    @pytest.fixture
    def detector(self) -> OutlierRateDetector:
        """Create detector instance."""
        return OutlierRateDetector()

    def test_no_outliers(self, detector: OutlierRateDetector):
        """Test entropy is 0 for no outliers."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.0,
                        "iqr_outlier_count": 0,
                        "iqr_lower_fence": 10.0,
                        "iqr_upper_fence": 100.0,
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "none"

    def test_few_outliers(self, detector: OutlierRateDetector):
        """Test low entropy for few outliers."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.005,
                        "iqr_outlier_count": 5,
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.05, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "minimal"

    def test_significant_outliers(self, detector: OutlierRateDetector):
        """Test high entropy for significant outliers."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.08,
                        "iqr_outlier_count": 80,
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.8, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "significant"
        # Should have resolution options
        actions = [opt.action for opt in results[0].resolution_options]
        assert "transform_winsorize" in actions
        assert "transform_exclude_outliers" in actions

    def test_max_entropy_at_10_percent(self, detector: OutlierRateDetector):
        """Test entropy caps at 1.0 for 10% or more outliers."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.15,
                    }
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "critical"

    def test_direct_stats_format(self, detector: OutlierRateDetector):
        """Test detector works with direct stats format."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "iqr_outlier_ratio": 0.03,
                    "iqr_outlier_count": 30,
                },
                "semantic": {"semantic_role": "measure"},
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.3, abs=0.01)

    def test_skip_key_column(self, detector: OutlierRateDetector):
        """Test outlier detection is skipped for key columns."""
        context = DetectorContext(
            table_name="orders",
            column_name="order_id",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.05,
                        "iqr_outlier_count": 50,
                    }
                },
                "semantic": {
                    "semantic_role": "key",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 0

    def test_skip_foreign_key_column(self, detector: OutlierRateDetector):
        """Test outlier detection is skipped for foreign key columns."""
        context = DetectorContext(
            table_name="order_items",
            column_name="order_id",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.08,
                    }
                },
                "semantic": {
                    "semantic_role": "foreign_key",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 0

    def test_runs_for_measure_column(self, detector: OutlierRateDetector):
        """Test outlier detection runs normally for measure columns."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.05,
                    }
                },
                "semantic": {
                    "semantic_role": "measure",
                },
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score > 0

    def test_detector_properties(self, detector: OutlierRateDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "outlier_rate"
        assert detector.layer == "value"
        assert detector.dimension == "outliers"
        assert detector.required_analyses == ["statistics", "semantic"]


class _MockDriftSummary:
    """Lightweight mock for ColumnDriftSummary (avoids DB session)."""

    def __init__(
        self,
        column_name: str,
        max_js_divergence: float,
        mean_js_divergence: float,
        periods_analyzed: int,
        periods_with_drift: int,
        drift_evidence_json: dict | None = None,
    ):
        self.column_name = column_name
        self.max_js_divergence = max_js_divergence
        self.mean_js_divergence = mean_js_divergence
        self.periods_analyzed = periods_analyzed
        self.periods_with_drift = periods_with_drift
        self.drift_evidence_json = drift_evidence_json


class TestTemporalDriftDetector:
    """Tests for TemporalDriftDetector."""

    @pytest.fixture
    def detector(self) -> TemporalDriftDetector:
        return TemporalDriftDetector()

    def test_no_drift_summaries(self, detector: TemporalDriftDetector):
        """Returns empty when no drift summaries available."""
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": []},
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_no_matching_column(self, detector: TemporalDriftDetector):
        """Returns empty when column not in drift summaries."""
        summary = _MockDriftSummary("other_col", 0.5, 0.3, 5, 2)
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": [summary]},
        )
        results = detector.detect(context)
        assert len(results) == 0

    def test_zero_drift(self, detector: TemporalDriftDetector):
        """Score is 0 when JS divergence is 0."""
        summary = _MockDriftSummary("status", 0.0, 0.0, 5, 0)
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": [summary]},
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)

    def test_mild_drift(self, detector: TemporalDriftDetector):
        """Score ~0.3 for 0.1 JS divergence."""
        summary = _MockDriftSummary("status", 0.1, 0.05, 5, 1)
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": [summary]},
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.3, abs=0.01)

    def test_moderate_drift(self, detector: TemporalDriftDetector):
        """Score ~0.7 for 0.3 JS divergence."""
        summary = _MockDriftSummary("status", 0.3, 0.15, 5, 2)
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": [summary]},
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7, abs=0.01)

    def test_severe_drift(self, detector: TemporalDriftDetector):
        """Score 1.0 for 0.5+ JS divergence."""
        summary = _MockDriftSummary("status", 0.6, 0.3, 5, 4)
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": [summary]},
        )
        results = detector.detect(context)
        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)

    def test_evidence_includes_drift_details(self, detector: TemporalDriftDetector):
        """Evidence includes drift summary info."""
        summary = _MockDriftSummary(
            "status",
            0.4,
            0.2,
            5,
            3,
            drift_evidence_json={
                "worst_period": "2024-Q3",
                "worst_js": 0.4,
                "top_shifts": [
                    {
                        "category": "Active",
                        "baseline_pct": 45,
                        "period_pct": 12,
                        "period": "2024-Q3",
                    }
                ],
            },
        )
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": [summary]},
        )
        results = detector.detect(context)
        assert len(results) == 1
        ev = results[0].evidence[0]
        assert ev["max_js_divergence"] == 0.4
        assert ev["worst_period"] == "2024-Q3"
        assert len(ev["top_shifts"]) == 1

    def test_resolution_options_for_high_drift(self, detector: TemporalDriftDetector):
        """High drift produces resolution options."""
        summary = _MockDriftSummary("status", 0.8, 0.4, 5, 4)
        context = DetectorContext(
            table_name="orders",
            column_name="status",
            analysis_results={"drift_summaries": [summary]},
        )
        results = detector.detect(context)
        actions = [opt.action for opt in results[0].resolution_options]
        assert "investigate_drift" in actions
        assert "add_time_filter" in actions

    def test_detector_properties(self, detector: TemporalDriftDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "temporal_drift"
        assert detector.layer == "value"
        assert detector.dimension == "temporal"
        assert detector.required_analyses == ["drift_summaries"]
