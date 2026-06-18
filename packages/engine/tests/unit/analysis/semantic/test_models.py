"""Unit tests for semantic tool-output models (DAT-362 split invariants)."""

from __future__ import annotations

from dataraum.analysis.semantic.models import TableSynthesisOutput


def test_table_synthesis_output_has_no_column_field() -> None:
    """The per-table schema must NOT carry per-column annotations.

    Column annotations are owned by the per-column phase; if a ``columns`` field
    leaks back into the per-table tool schema, the LLM would re-emit (and the
    processor could re-persist) columns — re-coupling the two phases.
    """
    fields = set(
        TableSynthesisOutput.model_json_schema()["$defs"]["TableEntityOutput"]["properties"]
    )
    assert "columns" not in fields
    assert {"table_name", "entity_type", "is_fact_table", "grain"} <= fields
    # DAT-565: multi-temporal + identity roles live on the per-table schema.
    assert {"time_columns", "identity_columns"} <= fields


def test_table_entity_output_parses_multi_time_and_identity() -> None:
    """A denormalized table emits every event-time axis (aspect + note) plus its
    recurring identity columns, each with a one-line note (DAT-565)."""
    out = TableSynthesisOutput.model_validate(
        {
            "tables": [
                {
                    "table_name": "orders",
                    "entity_type": "orders",
                    "description": "Customer orders.",
                    "is_fact_table": True,
                    "grain": ["order_id"],
                    "time_columns": [
                        {"column": "order_date", "aspect": "order", "note": "When placed."},
                        {"column": "ship_date", "aspect": "ship", "note": "When shipped."},
                    ],
                    "identity_columns": [
                        {"column": "customer_id", "note": "Buying account; recurs across orders."}
                    ],
                }
            ],
            "relationships": [],
        }
    )
    table = out.tables[0]
    assert [tc.column for tc in table.time_columns] == ["order_date", "ship_date"]
    assert table.time_columns[1].aspect == "ship"
    assert table.identity_columns[0].column == "customer_id"


def test_table_synthesis_output_validates_entities_and_relationships() -> None:
    out = TableSynthesisOutput.model_validate(
        {
            "tables": [
                {
                    "table_name": "orders",
                    "entity_type": "orders",
                    "description": "Customer orders.",
                    "is_fact_table": True,
                    "grain": ["order_id"],
                }
            ],
            "relationships": [
                {
                    "from_table": "orders",
                    "from_column": "customer_id",
                    "to_table": "customers",
                    "to_column": "id",
                    "relationship_type": "foreign_key",
                    "confidence": 0.9,
                    "reasoning": "FK by name + value overlap.",
                }
            ],
        }
    )
    assert out.tables[0].is_fact_table is True
    assert out.relationships[0].to_table == "customers"
