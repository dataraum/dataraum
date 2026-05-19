"""Tests for ColumnAnnotationAgent and related models."""

from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    ColumnSemanticOutput,
    TableColumnAnnotation,
)


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
