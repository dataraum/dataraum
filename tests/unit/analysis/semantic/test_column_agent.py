"""Tests for ColumnAnnotationAgent and related models."""

from dataraum.analysis.semantic.column_agent import ColumnAnnotationAgent
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    ColumnSemanticOutput,
    TableColumnAnnotation,
    UnitRelationship,
)
from dataraum.core.models.base import SemanticRole


class TestColumnAnnotationOutput:
    """Tests for tier 1 column annotation output models."""

    def test_column_annotation_output_round_trip(self):
        """Test serialization/deserialization of ColumnAnnotationOutput."""
        output = ColumnAnnotationOutput(
            tables=[
                TableColumnAnnotation(
                    table_name="orders",
                    columns=[
                        ColumnSemanticOutput(
                            column_name="order_id",
                            semantic_role="key",
                            entity_type="order_identifier",
                            business_term="Order ID",
                            business_concept=None,
                            description="Unique order identifier",
                            confidence=0.95,
                        ),
                        ColumnSemanticOutput(
                            column_name="amount",
                            semantic_role="measure",
                            entity_type="transaction_amount",
                            business_term="Order Amount",
                            business_concept="transaction_amount",
                            description="Monetary value of the order",
                            confidence=0.9,
                            unit_source_column="currency",
                        ),
                    ],
                )
            ]
        )

        # Serialize and deserialize
        data = output.model_dump()
        restored = ColumnAnnotationOutput.model_validate(data)

        assert len(restored.tables) == 1
        assert len(restored.tables[0].columns) == 2
        assert restored.tables[0].columns[0].column_name == "order_id"
        assert restored.tables[0].columns[1].unit_source_column == "currency"

    def test_unit_source_column_defaults_to_none(self):
        """Test that unit_source_column defaults to None."""
        col = ColumnSemanticOutput(
            column_name="name",
            semantic_role="attribute",
            entity_type="customer_name",
            business_term="Customer Name",
            description="Customer's full name",
            confidence=0.8,
        )
        assert col.unit_source_column is None


class TestUnitRelationship:
    """Tests for UnitRelationship model."""

    def test_unit_relationship_basic(self):
        """Test basic UnitRelationship creation."""
        ur = UnitRelationship(
            unit_column="currency_code",
            measure_columns=["amount", "debit", "credit"],
            unit_values=["USD", "EUR", "GBP"],
        )
        assert ur.unit_column == "currency_code"
        assert len(ur.measure_columns) == 3
        assert "USD" in ur.unit_values

    def test_unit_relationship_empty_values(self):
        """Test UnitRelationship with no known values."""
        ur = UnitRelationship(
            unit_column="uom",
            measure_columns=["quantity"],
        )
        assert ur.unit_values == []


class TestColumnAnnotationAgentConversion:
    """Tests for ColumnAnnotationAgent.to_enrichment_result."""

    def test_to_enrichment_result(self):
        """Test converting tier 1 output to enrichment result."""
        output = ColumnAnnotationOutput(
            tables=[
                TableColumnAnnotation(
                    table_name="orders",
                    columns=[
                        ColumnSemanticOutput(
                            column_name="order_id",
                            semantic_role="key",
                            entity_type="order_id",
                            business_term="Order ID",
                            description="Unique order identifier",
                            confidence=0.95,
                        ),
                        ColumnSemanticOutput(
                            column_name="total",
                            semantic_role="measure",
                            entity_type="order_total",
                            business_term="Order Total",
                            business_concept="transaction_amount",
                            description="Total order value",
                            confidence=0.85,
                            unit_source_column="currency",
                        ),
                    ],
                )
            ]
        )

        # Create a minimal agent (we only need the conversion method)
        result = ColumnAnnotationAgent.to_enrichment_result(None, output, "test-model")

        assert len(result.annotations) == 2
        assert result.source == "llm_tier1"
        assert result.entity_detections == []
        assert result.relationships == []

        # Check annotations
        order_id_ann = result.annotations[0]
        assert order_id_ann.semantic_role == SemanticRole.KEY
        assert order_id_ann.column_ref.table_name == "orders"
        assert order_id_ann.column_ref.column_name == "order_id"

        total_ann = result.annotations[1]
        assert total_ann.semantic_role == SemanticRole.MEASURE
        assert total_ann.business_concept == "transaction_amount"
        assert total_ann.unit_source_column == "currency"
        assert total_ann.annotated_by == "test-model"


class TestFormatColumnAnnotations:
    """Tests for SemanticAgent._format_column_annotations."""

    def test_format_none(self):
        """Test formatting when no annotations provided."""
        from dataraum.analysis.semantic.agent import SemanticAgent

        result = SemanticAgent._format_column_annotations(None)
        assert result == "No prior column annotations available."

    def test_format_with_annotations(self):
        """Test formatting with annotations."""
        from dataraum.analysis.semantic.agent import SemanticAgent

        output = ColumnAnnotationOutput(
            tables=[
                TableColumnAnnotation(
                    table_name="orders",
                    columns=[
                        ColumnSemanticOutput(
                            column_name="order_id",
                            semantic_role="key",
                            entity_type="order_id",
                            business_term="Order ID",
                            description="ID",
                            confidence=0.95,
                        ),
                        ColumnSemanticOutput(
                            column_name="amount",
                            semantic_role="measure",
                            entity_type="amount",
                            business_term="Amount",
                            business_concept="transaction_amount",
                            description="Amount",
                            confidence=0.6,  # Low confidence
                        ),
                    ],
                )
            ]
        )

        result = SemanticAgent._format_column_annotations(output)

        assert "orders" in result
        assert "order_id" in result
        assert "role=key" in result
        assert "LOW CONFIDENCE" in result  # Low confidence annotation flagged
