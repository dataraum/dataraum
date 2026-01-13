"""Tests for semantic layer entropy detectors."""

import pytest

from dataraum_context.entropy.detectors import (
    BusinessMeaningDetector,
    DetectorContext,
)


class TestBusinessMeaningDetector:
    """Tests for BusinessMeaningDetector."""

    @pytest.fixture
    def detector(self) -> BusinessMeaningDetector:
        """Create detector instance."""
        return BusinessMeaningDetector()

    @pytest.mark.asyncio
    async def test_no_description(self, detector: BusinessMeaningDetector):
        """Test max entropy for missing description."""
        context = DetectorContext(
            table_name="orders",
            column_name="col1",
            analysis_results={
                "semantic": {
                    "business_description": None,
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert results[0].evidence[0]["clarity"] == "missing"

    @pytest.mark.asyncio
    async def test_empty_description(self, detector: BusinessMeaningDetector):
        """Test max entropy for empty description."""
        context = DetectorContext(
            table_name="orders",
            column_name="col1",
            analysis_results={
                "semantic": {
                    "business_description": "",
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)

    @pytest.mark.asyncio
    async def test_brief_description(self, detector: BusinessMeaningDetector):
        """Test high entropy for brief description."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",  # 12 chars
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.7, abs=0.01)
        assert results[0].evidence[0]["clarity"] == "brief"

    @pytest.mark.asyncio
    async def test_moderate_description(self, detector: BusinessMeaningDetector):
        """Test moderate entropy for moderate description."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Total amount of the order in USD",  # 33 chars
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.4, abs=0.01)
        assert results[0].evidence[0]["clarity"] == "moderate"

    @pytest.mark.asyncio
    async def test_substantial_description(self, detector: BusinessMeaningDetector):
        """Test low entropy for substantial description."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": (
                        "Total amount of the order in USD. "
                        "Includes all line items before tax and shipping. "
                        "Used for revenue reporting."
                    ),
                }
            },
        )

        results = await detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.2, abs=0.01)
        assert results[0].evidence[0]["clarity"] == "substantial"

    @pytest.mark.asyncio
    async def test_business_name_reduces_entropy(self, detector: BusinessMeaningDetector):
        """Test that business name reduces entropy."""
        # Without business name
        context_without = DetectorContext(
            table_name="orders",
            column_name="amt",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": None,
                }
            },
        )

        # With business name
        context_with = DetectorContext(
            table_name="orders",
            column_name="amt",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": "Order Amount",
                }
            },
        )

        results_without = await detector.detect(context_without)
        results_with = await detector.detect(context_with)

        assert results_with[0].score < results_without[0].score

    @pytest.mark.asyncio
    async def test_low_confidence_increases_entropy(self, detector: BusinessMeaningDetector):
        """Test that low semantic confidence increases entropy."""
        context_high = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "confidence": 0.9,
                }
            },
        )

        context_low = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "confidence": 0.5,
                }
            },
        )

        results_high = await detector.detect(context_high)
        results_low = await detector.detect(context_low)

        assert results_low[0].score > results_high[0].score

    @pytest.mark.asyncio
    async def test_resolution_options_for_missing(self, detector: BusinessMeaningDetector):
        """Test resolution options for missing description."""
        context = DetectorContext(
            table_name="orders",
            column_name="col1",
            analysis_results={
                "semantic": {
                    "business_description": "",
                }
            },
        )

        results = await detector.detect(context)

        actions = [opt.action for opt in results[0].resolution_options]
        assert "add_description" in actions
        assert "add_business_name" in actions
        assert "add_entity_type" in actions

    @pytest.mark.asyncio
    async def test_cascade_dimensions(self, detector: BusinessMeaningDetector):
        """Test resolution options include cascade dimensions."""
        context = DetectorContext(
            table_name="orders",
            column_name="col1",
            analysis_results={
                "semantic": {
                    "business_description": "",
                }
            },
        )

        results = await detector.detect(context)

        add_desc_opt = next(
            (opt for opt in results[0].resolution_options if opt.action == "add_description"),
            None,
        )
        assert add_desc_opt is not None
        assert "computational.aggregations" in add_desc_opt.cascade_dimensions

    def test_detector_properties(self, detector: BusinessMeaningDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "business_meaning"
        assert detector.layer == "semantic"
        assert detector.dimension == "business_meaning"
        assert detector.required_analyses == ["semantic"]
