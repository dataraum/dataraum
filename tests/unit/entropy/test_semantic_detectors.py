"""Tests for semantic layer entropy detectors.

NOTE: BusinessMeaningDetector uses confidence-weighted scoring.
The detector now uses:
- Base score from presence of description/metadata
- Confidence factor: low LLM confidence increases entropy
- Ontology bonus: business_concept presence reduces entropy
"""

import pytest

from dataraum.entropy.detectors import (
    BusinessMeaningDetector,
    DetectorContext,
)


class TestBusinessMeaningDetector:
    """Tests for BusinessMeaningDetector.

    The detector uses confidence-weighted scoring:
    - No description = ~1.0 (high entropy) * confidence_factor
    - Description only = ~0.6 (moderate entropy) * confidence_factor
    - Description + business_name or entity_type = ~0.2 (low entropy) * confidence_factor
    - business_concept presence = ontology_bonus reduction
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
        # Base score 1.0 * confidence_factor (default confidence=1.0 -> factor=1.0)
        assert results[0].score == pytest.approx(1.0, abs=0.05)
        assert results[0].evidence[0]["assessment"] == "missing"

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
        # Has description but no business_name/entity_type = base 0.6 * confidence_factor
        assert results[0].score == pytest.approx(0.6, abs=0.05)
        assert results[0].evidence[0]["assessment"] == "partial"

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
        # Has description + business_name = base 0.2 * confidence_factor
        assert results[0].score == pytest.approx(0.2, abs=0.05)
        assert results[0].evidence[0]["assessment"] == "documented"

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
        """Test zero entropy for fully documented column (description + business_name + entity_type)."""
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
        assert results[0].score == pytest.approx(0.0, abs=0.01)
        assert results[0].evidence[0]["assessment"] == "fully_documented"

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
        assert "document_description" in actions
        assert "document_business_name" in actions
        assert "document_entity_type" in actions

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
        # Has description, so document_description not suggested
        assert "document_description" not in actions
        # Missing business_name and entity_type
        assert "document_business_name" in actions
        assert "document_entity_type" in actions

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
            (opt for opt in results[0].resolution_options if opt.action == "document_description"),
            None,
        )
        assert add_desc_opt is not None
        assert "computational.aggregations" in add_desc_opt.cascade_dimensions

    def test_fully_documented_low_confidence_nonzero(self, detector: BusinessMeaningDetector):
        """Test that fully documented column with low confidence has nonzero score.

        This is the key regression test for the additive formula fix:
        the old multiplicative formula produced 0.0 * confidence_factor = 0.0.
        The additive formula gives: 0.0 + 0.3 * (1.0 - 0.5) - 0.0 = 0.15.
        """
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Total order amount in USD",
                    "business_name": "Order Amount",
                    "entity_type": "monetary_amount",
                    "confidence": 0.5,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Additive: 0.0 + 0.3 * 0.5 - 0.0 = 0.15
        assert results[0].score > 0.0, "Low confidence should produce nonzero score"
        assert results[0].score == pytest.approx(0.15, abs=0.01)

    def test_fully_documented_high_confidence_near_zero(self, detector: BusinessMeaningDetector):
        """Test that fully documented column with high confidence has near-zero score."""
        context = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Total order amount in USD",
                    "business_name": "Order Amount",
                    "entity_type": "monetary_amount",
                    "confidence": 0.95,
                }
            },
        )

        results = detector.detect(context)

        assert len(results) == 1
        # Additive: 0.0 + 0.3 * 0.05 = 0.015
        assert results[0].score == pytest.approx(0.015, abs=0.01)

    def test_low_confidence_increases_partial_score(self, detector: BusinessMeaningDetector):
        """Test that low confidence increases score for partially documented columns."""
        # High confidence
        context_high = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": None,
                    "entity_type": None,
                    "confidence": 0.95,
                }
            },
        )
        # Low confidence
        context_low = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": None,
                    "entity_type": None,
                    "confidence": 0.5,
                }
            },
        )

        results_high = detector.detect(context_high)
        results_low = detector.detect(context_low)

        assert results_low[0].score > results_high[0].score

    def test_concept_bonus_reduces_score(self, detector: BusinessMeaningDetector):
        """Test that business_concept presence reduces score."""
        # Without concept
        context_no_concept = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": "Order Amount",
                    "entity_type": None,
                    "business_concept": None,
                }
            },
        )
        # With concept
        context_with_concept = DetectorContext(
            table_name="orders",
            column_name="amount",
            analysis_results={
                "semantic": {
                    "business_description": "Order amount",
                    "business_name": "Order Amount",
                    "entity_type": None,
                    "business_concept": "revenue",
                }
            },
        )

        results_no = detector.detect(context_no_concept)
        results_yes = detector.detect(context_with_concept)

        assert results_yes[0].score < results_no[0].score
        # Ontology bonus is 0.1
        assert results_no[0].score - results_yes[0].score == pytest.approx(0.1, abs=0.01)

    def test_detector_properties(self, detector: BusinessMeaningDetector):
        """Test detector has correct properties."""
        assert detector.detector_id == "business_meaning"
        assert detector.layer == "semantic"
        assert detector.dimension == "business_meaning"
        assert detector.required_analyses == ["semantic"]
