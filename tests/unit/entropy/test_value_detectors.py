"""Tests for value layer entropy detectors."""

import pytest

from dataraum.entropy.detectors import (
    DetectorContext,
    NullRatioDetector,
    OutlierRateDetector,
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
