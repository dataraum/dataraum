"""Tests for unit_source_column propagation from unit_relationships.

Verifies that table-level unit_relationships backfill unit_source_column
on per-column annotations in _parse_tool_output().
"""

from unittest.mock import MagicMock

from dataraum.analysis.semantic.agent import SemanticAgent


def _make_agent() -> SemanticAgent:
    """Create a SemanticAgent with mocked dependencies."""
    mock_config = MagicMock()
    mock_provider = MagicMock()
    mock_renderer = MagicMock()
    return SemanticAgent(
        config=mock_config,
        provider=mock_provider,
        prompt_renderer=mock_renderer,
    )


class TestUnitSourceColumnPropagation:
    """Tests for unit_relationships backfilling unit_source_column."""

    def test_unit_relationships_backfill_unit_source_column(self):
        """unit_relationships populates unit_source_column on measure columns."""
        agent = _make_agent()

        # Build a tool output with unit_relationships at the table level
        # but NO unit_source_column on individual columns
        tool_output = {
            "tables": [
                {
                    "table_name": "transactions",
                    "entity_type": "transaction",
                    "description": "Financial transactions",
                    "grain": ["transaction_id"],
                    "is_fact_table": True,
                    "time_column": "created_at",
                    "unit_relationships": [
                        {
                            "unit_column": "currency_code",
                            "measure_columns": ["amount", "fee"],
                            "unit_values": ["USD", "EUR"],
                        }
                    ],
                    "columns": [
                        {
                            "column_name": "amount",
                            "semantic_role": "measure",
                            "entity_type": "monetary_amount",
                            "business_term": "Transaction Amount",
                            "description": "The transaction amount",
                            "business_concept": "monetary_value",
                            "confidence": 0.9,
                        },
                        {
                            "column_name": "fee",
                            "semantic_role": "measure",
                            "entity_type": "monetary_amount",
                            "business_term": "Transaction Fee",
                            "description": "The transaction fee",
                            "business_concept": "monetary_value",
                            "confidence": 0.9,
                        },
                        {
                            "column_name": "currency_code",
                            "semantic_role": "dimension",
                            "entity_type": "currency",
                            "business_term": "Currency",
                            "description": "Currency of the transaction",
                            "business_concept": "currency",
                            "confidence": 0.95,
                        },
                    ],
                },
            ],
            "relationships": [],
        }

        result = agent._parse_tool_output(tool_output, "test-model")
        assert result.success

        enrichment = result.unwrap()
        annotations = enrichment.annotations

        # Find annotations for measure columns
        amount_ann = next(a for a in annotations if a.column_ref.column_name == "amount")
        fee_ann = next(a for a in annotations if a.column_ref.column_name == "fee")
        currency_ann = next(a for a in annotations if a.column_ref.column_name == "currency_code")

        # Measure columns should have unit_source_column backfilled
        assert amount_ann.unit_source_column == "currency_code"
        assert fee_ann.unit_source_column == "currency_code"

        # Non-measure column (the unit column itself) should NOT have it set
        assert currency_ann.unit_source_column is None

    def test_unit_relationships_does_not_overwrite_explicit(self):
        """unit_relationships does NOT overwrite explicitly set unit_source_column."""
        agent = _make_agent()

        tool_output = {
            "tables": [
                {
                    "table_name": "transactions",
                    "entity_type": "transaction",
                    "description": "Financial transactions",
                    "grain": ["transaction_id"],
                    "is_fact_table": True,
                    "time_column": None,
                    "unit_relationships": [
                        {
                            "unit_column": "currency_code",
                            "measure_columns": ["amount"],
                            "unit_values": ["USD"],
                        }
                    ],
                    "columns": [
                        {
                            "column_name": "amount",
                            "semantic_role": "measure",
                            "entity_type": "monetary_amount",
                            "business_term": "Amount",
                            "description": "Amount",
                            "business_concept": "monetary_value",
                            "confidence": 0.9,
                            # Explicitly set on the column
                            "unit_source_column": "local_currency",
                        },
                    ],
                },
            ],
            "relationships": [],
        }

        result = agent._parse_tool_output(tool_output, "test-model")
        assert result.success

        enrichment = result.unwrap()
        amount_ann = next(a for a in enrichment.annotations if a.column_ref.column_name == "amount")

        # Should keep the explicit value, NOT overwrite with unit_relationships
        assert amount_ann.unit_source_column == "local_currency"

    def test_unit_relationships_multiple_tables(self):
        """unit_relationships works correctly across multiple tables."""
        agent = _make_agent()

        tool_output = {
            "tables": [
                {
                    "table_name": "orders",
                    "entity_type": "order",
                    "description": "Orders",
                    "grain": ["order_id"],
                    "is_fact_table": True,
                    "time_column": None,
                    "unit_relationships": [
                        {
                            "unit_column": "currency",
                            "measure_columns": ["total"],
                            "unit_values": ["USD"],
                        }
                    ],
                    "columns": [
                        {
                            "column_name": "total",
                            "semantic_role": "measure",
                            "entity_type": "monetary_amount",
                            "business_term": "Total",
                            "description": "Order total",
                            "business_concept": "monetary_value",
                            "confidence": 0.9,
                        },
                        {
                            "column_name": "currency",
                            "semantic_role": "dimension",
                            "entity_type": "currency",
                            "business_term": "Currency",
                            "description": "Currency",
                            "business_concept": "currency",
                            "confidence": 0.95,
                        },
                    ],
                },
                {
                    "table_name": "items",
                    "entity_type": "item",
                    "description": "Line items",
                    "grain": ["item_id"],
                    "is_fact_table": True,
                    "time_column": None,
                    "unit_relationships": [],
                    "columns": [
                        {
                            "column_name": "price",
                            "semantic_role": "measure",
                            "entity_type": "monetary_amount",
                            "business_term": "Price",
                            "description": "Item price",
                            "business_concept": "monetary_value",
                            "confidence": 0.9,
                        },
                    ],
                },
            ],
            "relationships": [],
        }

        result = agent._parse_tool_output(tool_output, "test-model")
        assert result.success

        enrichment = result.unwrap()

        # Orders table: total should get currency as unit_source
        total_ann = next(
            a
            for a in enrichment.annotations
            if a.column_ref.table_name == "orders" and a.column_ref.column_name == "total"
        )
        assert total_ann.unit_source_column == "currency"

        # Items table: price should NOT get any unit_source (no unit_relationships)
        price_ann = next(
            a
            for a in enrichment.annotations
            if a.column_ref.table_name == "items" and a.column_ref.column_name == "price"
        )
        assert price_ann.unit_source_column is None

    def test_empty_unit_relationships(self):
        """Empty unit_relationships list doesn't cause errors."""
        agent = _make_agent()

        tool_output = {
            "tables": [
                {
                    "table_name": "simple",
                    "entity_type": "record",
                    "description": "Simple table",
                    "grain": ["id"],
                    "is_fact_table": False,
                    "time_column": None,
                    "unit_relationships": [],
                    "columns": [
                        {
                            "column_name": "name",
                            "semantic_role": "attribute",
                            "entity_type": "name",
                            "business_term": "Name",
                            "description": "Record name",
                            "business_concept": None,
                            "confidence": 0.9,
                        },
                    ],
                },
            ],
            "relationships": [],
        }

        result = agent._parse_tool_output(tool_output, "test-model")
        assert result.success

        enrichment = result.unwrap()
        name_ann = next(a for a in enrichment.annotations if a.column_ref.column_name == "name")
        assert name_ann.unit_source_column is None
