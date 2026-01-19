"""Tests for entropy processor."""

import pytest

from dataraum_context.entropy.detectors.base import (
    DetectorContext,
    DetectorRegistry,
    EntropyDetector,
)
from dataraum_context.entropy.models import EntropyObject
from dataraum_context.entropy.processor import (
    EntropyProcessor,
    ProcessorConfig,
    process_column_entropy,
)


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

    def test_process_column(
        self,
        test_registry: DetectorRegistry,
        sample_detector_context: DetectorContext,
    ):
        """Test processing a single column."""
        processor = EntropyProcessor(registry=test_registry)

        profile = processor.process_column(
            table_name=sample_detector_context.table_name,
            column_name=sample_detector_context.column_name,
            analysis_results=sample_detector_context.analysis_results,
        )

        assert profile.table_name == "orders"
        assert profile.column_name == "amount"
        assert profile.structural_entropy >= 0
        assert profile.semantic_entropy >= 0
        assert profile.value_entropy >= 0
        assert profile.composite_score >= 0
        assert profile.readiness in ["ready", "investigate", "blocked"]

    def test_process_column_with_high_entropy(
        self,
        test_registry: DetectorRegistry,
        high_entropy_context: DetectorContext,
    ):
        """Test processing a column with high entropy characteristics."""
        processor = EntropyProcessor(registry=test_registry)

        profile = processor.process_column(
            table_name=high_entropy_context.table_name,
            column_name=high_entropy_context.column_name,
            analysis_results=high_entropy_context.analysis_results,
        )

        # High parse failure rate (0.60) -> structural entropy = 0.40
        # High null ratio (0.35) -> value entropy = 0.70
        # No description -> semantic entropy = 1.0
        assert profile.structural_entropy == pytest.approx(0.40, abs=0.01)
        assert profile.value_entropy == pytest.approx(0.70, abs=0.01)
        assert profile.semantic_entropy == pytest.approx(1.0, abs=0.01)

    def test_process_column_with_low_entropy(
        self,
        test_registry: DetectorRegistry,
        low_entropy_context: DetectorContext,
    ):
        """Test processing a clean column with low entropy."""
        processor = EntropyProcessor(registry=test_registry)

        profile = processor.process_column(
            table_name=low_entropy_context.table_name,
            column_name=low_entropy_context.column_name,
            analysis_results=low_entropy_context.analysis_results,
        )

        # Perfect parse rate -> structural entropy = 0.0
        # Zero null ratio -> value entropy = 0.0
        # Has description -> semantic entropy = 0.2
        assert profile.structural_entropy == pytest.approx(0.0, abs=0.01)
        assert profile.value_entropy == pytest.approx(0.0, abs=0.01)
        assert profile.readiness == "ready"

    def test_process_table(self, test_registry: DetectorRegistry):
        """Test processing a table with multiple columns."""
        processor = EntropyProcessor(registry=test_registry)

        columns = [
            {
                "name": "id",
                "analysis_results": {
                    "typing": {"parse_success_rate": 1.0},
                    "statistics": {"null_ratio": 0.0},
                    "semantic": {"business_description": "Primary key"},
                },
            },
            {
                "name": "amount",
                "analysis_results": {
                    "typing": {"parse_success_rate": 0.95},
                    "statistics": {"null_ratio": 0.05},
                    "semantic": {"business_description": "Order total amount in USD"},
                },
            },
        ]

        table_profile = processor.process_table(
            table_name="orders",
            columns=columns,
        )

        assert table_profile.table_name == "orders"
        assert len(table_profile.column_profiles) == 2
        assert table_profile.avg_composite_score >= 0
        assert table_profile.max_composite_score >= table_profile.avg_composite_score

    def test_build_entropy_context(self, test_registry: DetectorRegistry):
        """Test building complete entropy context."""
        processor = EntropyProcessor(registry=test_registry)

        # Process a table
        table_profile = processor.process_table(
            table_name="orders",
            columns=[
                {
                    "name": "amount",
                    "analysis_results": {
                        "typing": {"parse_success_rate": 0.90},
                        "statistics": {"null_ratio": 0.1},
                        "semantic": {"business_description": "Amount"},
                    },
                },
            ],
        )

        # Build context
        context = processor.build_entropy_context([table_profile])

        assert "orders" in context.table_profiles
        assert "orders.amount" in context.column_profiles
        assert context.overall_readiness in ["ready", "investigate", "blocked"]


class TestProcessorConfig:
    """Tests for ProcessorConfig."""

    def test_default_weights(self):
        """Test default layer weights."""
        config = ProcessorConfig()

        assert config.layer_weights["structural"] == 0.25
        assert config.layer_weights["semantic"] == 0.30
        assert config.layer_weights["value"] == 0.30
        assert config.layer_weights["computational"] == 0.15

    def test_default_thresholds(self):
        """Test default thresholds."""
        config = ProcessorConfig()

        assert config.high_entropy_threshold == 0.5
        assert config.critical_entropy_threshold == 0.8


class TestConvenienceFunctions:
    """Tests for convenience functions."""

    def test_process_column_entropy(self):
        """Test process_column_entropy convenience function."""
        # This uses the default (empty) registry, so no detectors run
        profile = process_column_entropy(
            table_name="test",
            column_name="col",
            analysis_results={},
        )

        assert profile.table_name == "test"
        assert profile.column_name == "col"
        # No detectors ran, so all entropies are 0
        assert profile.composite_score == 0.0
