"""Tests for entropy processor."""

import pytest

from dataraum.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    EntropyDetector,
)
from dataraum.entropy.models import EntropyObject
from dataraum.entropy.processor import EntropyProcessor


class MockTypeFidelityDetector(EntropyDetector):
    """Mock type fidelity detector."""

    detector_id = "type_fidelity"
    layer = "structural"
    dimension = "types"
    sub_dimension = "type_fidelity"
    required_analyses = ["typing"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        typing_result = context.get_analysis("typing", {})
        parse_rate = typing_result.get("parse_success_rate", 1.0)
        score = 1.0 - parse_rate

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=[{"parse_success_rate": parse_rate}],
            )
        ]


class MockNullRatioDetector(EntropyDetector):
    """Mock null ratio detector."""

    detector_id = "null_ratio"
    layer = "value"
    dimension = "nulls"
    sub_dimension = "null_ratio"
    required_analyses = ["statistics"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        stats = context.get_analysis("statistics", {})
        null_ratio = stats.get("null_ratio", 0.0)
        score = min(1.0, null_ratio * 2)  # Double the null ratio, cap at 1.0

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=[{"null_ratio": null_ratio}],
            )
        ]


class MockSemanticDetector(EntropyDetector):
    """Mock semantic detector."""

    detector_id = "business_meaning"
    layer = "semantic"
    dimension = "business_meaning"
    sub_dimension = "naming_clarity"
    required_analyses = ["semantic"]

    def detect(self, context: DetectorContext) -> list[EntropyObject]:
        semantic = context.get_analysis("semantic", {})
        description = semantic.get("business_description", "")

        if not description:
            score = 1.0
        elif len(description) < 20:
            score = 0.7
        else:
            score = 0.2

        return [
            self.create_entropy_object(
                context=context,
                score=score,
                evidence=[{"has_description": bool(description)}],
            )
        ]


@pytest.fixture
def test_registry() -> DetectorRegistry:
    """Registry with mock detectors."""
    registry = DetectorRegistry()
    registry.register(MockTypeFidelityDetector())
    registry.register(MockNullRatioDetector())
    registry.register(MockSemanticDetector())
    return registry


class TestEntropyProcessor:
    """Tests for EntropyProcessor."""

    def test_process_column_returns_entropy_objects(
        self,
        test_registry: DetectorRegistry,
        sample_detector_context: DetectorContext,
    ):
        """Test processing a single column returns list of EntropyObject."""
        processor = EntropyProcessor(registry=test_registry)

        result = processor.process_column(
            table_name=sample_detector_context.table_name,
            column_name=sample_detector_context.column_name,
            analysis_results=sample_detector_context.analysis_results,
        )

        assert isinstance(result, list)
        assert len(result) == 3  # One per mock detector
        assert all(isinstance(obj, EntropyObject) for obj in result)

        # Check layers present
        layers = {obj.layer for obj in result}
        assert "structural" in layers
        assert "semantic" in layers
        assert "value" in layers

    def test_process_column_with_high_entropy(
        self,
        test_registry: DetectorRegistry,
        high_entropy_context: DetectorContext,
    ):
        """Test processing a column with high entropy characteristics."""
        processor = EntropyProcessor(registry=test_registry)

        result = processor.process_column(
            table_name=high_entropy_context.table_name,
            column_name=high_entropy_context.column_name,
            analysis_results=high_entropy_context.analysis_results,
        )

        scores_by_layer = {obj.layer: obj.score for obj in result}

        # High parse failure rate (0.60) -> structural entropy = 0.40
        assert scores_by_layer["structural"] == pytest.approx(0.40, abs=0.01)
        # High null ratio (0.35) -> value entropy = 0.70
        assert scores_by_layer["value"] == pytest.approx(0.70, abs=0.01)
        # No description -> semantic entropy = 1.0
        assert scores_by_layer["semantic"] == pytest.approx(1.0, abs=0.01)

    def test_process_column_with_low_entropy(
        self,
        test_registry: DetectorRegistry,
        low_entropy_context: DetectorContext,
    ):
        """Test processing a clean column with low entropy."""
        processor = EntropyProcessor(registry=test_registry)

        result = processor.process_column(
            table_name=low_entropy_context.table_name,
            column_name=low_entropy_context.column_name,
            analysis_results=low_entropy_context.analysis_results,
        )

        scores_by_layer = {obj.layer: obj.score for obj in result}

        # Perfect parse rate -> structural entropy = 0.0
        assert scores_by_layer["structural"] == pytest.approx(0.0, abs=0.01)
        # Zero null ratio -> value entropy = 0.0
        assert scores_by_layer["value"] == pytest.approx(0.0, abs=0.01)

    def test_process_column_with_missing_analyses(
        self,
        test_registry: DetectorRegistry,
    ):
        """Test processing column when some analyses are missing."""
        processor = EntropyProcessor(registry=test_registry)

        # Only provide typing data - semantic and statistics detectors won't run
        result = processor.process_column(
            table_name="orders",
            column_name="amount",
            analysis_results={"typing": {"parse_success_rate": 0.9}},
        )

        # Only the typing detector should have run
        assert len(result) == 1
        assert result[0].layer == "structural"
