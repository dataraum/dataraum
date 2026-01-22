"""Tests for semantic layer entropy detectors.

NOTE: BusinessMeaningDetector tests updated to reflect simplified scoring.
Character-counting heuristics removed - semantic quality evaluation
will be done by LLM in Phase 2.5.
"""

import pytest

from dataraum.entropy.detectors import (
    BusinessMeaningDetector,
    DetectorContext,
)


class TestBusinessMeaningDetector:
    """Tests for BusinessMeaningDetector.

    The detector now collects raw metrics and uses simplified scoring:
    - No description = 1.0 (high entropy)
    - Description only = 0.6 (moderate entropy)
    - Description + business_name or entity_type = 0.2 (low entropy)

    Semantic quality evaluation will be done by LLM in Phase 2.5.
    """

    @pytest.fixture
    def detector(self) -> BusinessMeaningDetector:
        """Create detector instance."""
        return BusinessMeaningDetector()

    def test_no_description(self, detector: BusinessMeaningDetector):
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

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)
        assert results[0].evidence[0]["provisional_assessment"] == "missing"

    def test_empty_description(self, detector: BusinessMeaningDetector):
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

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(1.0, abs=0.01)

    def test_description_only(self, detector: BusinessMeaningDetector):
        """Test moderate entropy for description without additional context."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": None,
                    "entity_type": None,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Has description but no business_name/entity_type = 0.6
        assert results[0].score == pytest.approx(0.6, abs=0.01)
        assert results[0].evidence[0]["provisional_assessment"] == "partial"

    def test_description_with_business_name(self, detector: BusinessMeaningDetector):
        """Test low entropy for description with business name."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": "Order Amount",
                    "entity_type": None,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Has description + business_name = 0.2
        assert results[0].score == pytest.approx(0.2, abs=0.01)
        assert results[0].evidence[0]["provisional_assessment"] == "documented"

    def test_description_with_entity_type(self, detector: BusinessMeaningDetector):
        """Test low entropy for description with entity type."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": None,
                    "entity_type": "monetary_amount",
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Has description + entity_type = 0.2
        assert results[0].score == pytest.approx(0.2, abs=0.01)

    def test_full_documentation(self, detector: BusinessMeaningDetector):
        """Test low entropy for fully documented column."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": (
                        "Total amount of the order in USD. "
                        "Includes all line items before tax and shipping."
                    ),
                    "business_name": "Order Total Amount",
                    "entity_type": "monetary_amount",
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        assert results[0].score == pytest.approx(0.2, abs=0.01)

    def test_raw_metrics_collected(self, detector: BusinessMeaningDetector):
        """Test that raw metrics are collected for LLM interpretation."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": "Order Amount",
                    "entity_type": "monetary_amount",
                    "semantic_role": "measure",
                    "confidence": 0.95,
                }
            },
        )

        results = detector.detect(context)

        raw_metrics = results[0].evidence[0]["raw_metrics"]
        assert raw_metrics["description"] == "Order amount"
        assert raw_metrics["description_length"] == 12
        assert raw_metrics["has_description"] is True
        assert raw_metrics["business_name"] == "Order Amount"
        assert raw_metrics["has_business_name"] is True
        assert raw_metrics["entity_type"] == "monetary_amount"
        assert raw_metrics["has_entity_type"] is True
        assert raw_metrics["semantic_role"] == "measure"
        assert raw_metrics["semantic_confidence"] == 0.95

    def test_resolution_options_for_missing(self, detector: BusinessMeaningDetector):
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

        results = detector.detect(context)

        actions = [opt.action for opt in results[0].resolution_options]
        assert "add_description" in actions
        assert "add_business_name" in actions
        assert "add_entity_type" in actions

    def test_resolution_options_with_description(self, detector: BusinessMeaningDetector):
        """Test resolution options when description exists but not others."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": None,
                    "entity_type": None,
                }
            },
        )

        results = detector.detect(context)

        actions = [opt.action for opt in results[0].resolution_options]
        # Has description, so add_description not suggested
        assert "add_description" not in actions
        # Missing business_name and entity_type
        assert "add_business_name" in actions
        assert "add_entity_type" in actions

    def test_cascade_dimensions(self, detector: BusinessMeaningDetector):
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

        results = detector.detect(context)

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
