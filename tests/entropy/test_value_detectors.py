"""Tests for value layer entropy detectors."""

import pytest

from dataraum_context.entropy.detectors import (
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

    @pytest.mark.asyncio
    async def test_no_nulls(self, detector: NullRatioDetector):
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

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "none"

    @pytest.mark.asyncio
    async def test_low_nulls(self, detector: NullRatioDetector):
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

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.04, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "minimal"

    @pytest.mark.asyncio
    async def test_high_nulls(self, detector: NullRatioDetector):
        """Test high entropy for significant nulls."""
        context = DetectorContext(
            table_name="orders",
            column_name="discount",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.35,
                    "null_count": 350,
                    "total_count": 1000,
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7, abs=0.01)
        assert results[0].evidence[0]["null_impact"] == "significant"
        # Should have resolution options
        actions = [opt.action for opt in results[0].resolution_options]
        assert "declare_null_meaning" in actions
        assert "filter_nulls" in actions

    @pytest.mark.asyncio
    async def test_max_entropy_at_50_percent(self, detector: NullRatioDetector):
        """Test entropy caps at 1.0 for 50% or more nulls."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "null_ratio": 0.5,
                }
            },
        )

        results = await detector.detect(context)

        assert results[0].score == pytest.approx(1.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_resolution_cascade_dimensions(self, detector: NullRatioDetector):
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

        results = await detector.detect(context)

        # declare_null_meaning should cascade to semantic.business_meaning
        null_meaning_opt = next(
            (opt for opt in results[0].resolution_options if opt.action == "declare_null_meaning"),
            None,
        )
        assert null_meaning_opt is not None
        assert "semantic.business_meaning" in null_meaning_opt.cascade_dimensions

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

    @pytest.mark.asyncio
    async def test_no_outliers(self, detector: OutlierRateDetector):
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
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "none"

    @pytest.mark.asyncio
    async def test_few_outliers(self, detector: OutlierRateDetector):
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
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.05, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "minimal"

    @pytest.mark.asyncio
    async def test_significant_outliers(self, detector: OutlierRateDetector):
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
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.8, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "significant"
        # Should have resolution options
        actions = [opt.action for opt in results[0].resolution_options]
        assert "winsorize" in actions
        assert "exclude_outliers" in actions

    @pytest.mark.asyncio
    async def test_max_entropy_at_10_percent(self, detector: OutlierRateDetector):
        """Test entropy caps at 1.0 for 10% or more outliers."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "outlier_detection": {
                        "iqr_outlier_ratio": 0.15,
                    }
                }
            },
        )

        results = await detector.detect(context)

        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert results[0].evidence[0]["outlier_impact"] == "critical"

    @pytest.mark.asyncio
    async def test_direct_stats_format(self, detector: OutlierRateDetector):
        """Test detector works with direct stats format."""
        context = DetectorContext(
            table_name="test",
            column_name="col",
            analysis_results={
                "statistics": {
                    "iqr_outlier_ratio": 0.03,
                    "iqr_outlier_count": 30,
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.3, abs=0.01)

    def test_detector_properties(self, detector: OutlierRateDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "outlier_rate"
        assert detector.layer == "value"
        assert detector.dimension == "outliers"
        assert detector.required_analyses == ["statistics"]
