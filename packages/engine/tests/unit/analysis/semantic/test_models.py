"""Unit tests for semantic tool-output models (DAT-362 split invariants)."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from dataraum.analysis.semantic.models import TableSynthesisOutput


def _table(time_columns: list[dict], **overrides: object) -> dict:
    """A minimal valid TableEntityOutput dict with the given time_columns."""
    base = {
        "table_name": "orders",
        "entity_type": "orders",
        "description": "Customer orders.",
        "is_fact_table": True,
        "grain": ["order_id"],
        "time_columns": time_columns,
        "identity_columns": [],
    }
    base.update(overrides)
    return base


def _synthesis(table: dict) -> dict:
    return {"tables": [table], "relationships": [], "column_concepts": []}


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
    """A denormalized table emits every event-time axis (aspect + role + anchor +
    note) plus its recurring identity columns, each with a one-line note
    (DAT-565/780)."""
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
                        {
                            "column": "order_date",
                            "aspect": "order",
                            "role": "event",
                            "is_anchor": True,
                            "note": "When placed.",
                        },
                        {
                            "column": "ship_date",
                            "aspect": "ship",
                            "role": "event",
                            "is_anchor": False,
                            "note": "When shipped.",
                        },
                        {
                            "column": "due_date",
                            "aspect": "due",
                            "role": "attribute",
                            "is_anchor": False,
                            "note": "When payment is owed.",
                        },
                    ],
                    "identity_columns": [
                        {"column": "customer_id", "note": "Buying account; recurs across orders."}
                    ],
                }
            ],
            "relationships": [],
            "column_concepts": [],
        }
    )
    table = out.tables[0]
    assert [tc.column for tc in table.time_columns] == ["order_date", "ship_date", "due_date"]
    assert table.time_columns[1].aspect == "ship"
    # role + anchor are committed per column (DAT-780).
    assert [tc.role for tc in table.time_columns] == ["event", "event", "attribute"]
    assert [tc.is_anchor for tc in table.time_columns] == [True, False, False]
    assert table.identity_columns[0].column == "customer_id"


def test_anchor_defaults_to_position_are_rejected_scrambled_order() -> None:
    """The anchor is the TYPED is_anchor flag, never array position (DAT-780).

    The anchor sits at index 1 here; a positional reader would wrongly pick
    order_date at index 0. The model accepts it because is_anchor names ship_date,
    proving position carries no meaning.
    """
    out = TableSynthesisOutput.model_validate(
        _synthesis(
            _table(
                [
                    {
                        "column": "order_date",
                        "aspect": "order",
                        "role": "event",
                        "is_anchor": False,
                        "note": "When placed.",
                    },
                    {
                        "column": "ship_date",
                        "aspect": "ship",
                        "role": "event",
                        "is_anchor": True,
                        "note": "When shipped.",
                    },
                ]
            )
        )
    )
    anchors = [tc.column for tc in out.tables[0].time_columns if tc.is_anchor]
    assert anchors == ["ship_date"]


def test_zero_anchor_with_events_present_is_rejected() -> None:
    """A table with event dates but no anchor fails validation → repair turn."""
    with pytest.raises(ValidationError, match="exactly one event time_column"):
        TableSynthesisOutput.model_validate(
            _synthesis(
                _table(
                    [
                        {
                            "column": "order_date",
                            "aspect": "order",
                            "role": "event",
                            "is_anchor": False,
                            "note": "When placed.",
                        },
                    ]
                )
            )
        )


def test_two_anchors_is_rejected() -> None:
    """A table with two anchors fails validation → repair turn."""
    with pytest.raises(ValidationError, match="exactly one event time_column"):
        TableSynthesisOutput.model_validate(
            _synthesis(
                _table(
                    [
                        {
                            "column": "order_date",
                            "aspect": "order",
                            "role": "event",
                            "is_anchor": True,
                            "note": "When placed.",
                        },
                        {
                            "column": "ship_date",
                            "aspect": "ship",
                            "role": "event",
                            "is_anchor": True,
                            "note": "When shipped.",
                        },
                    ]
                )
            )
        )


def test_attribute_role_anchor_is_rejected() -> None:
    """An anchor with role='attribute' is a contract violation (DAT-780)."""
    with pytest.raises(ValidationError, match="anchor time_column must have role='event'"):
        TableSynthesisOutput.model_validate(
            _synthesis(
                _table(
                    [
                        {
                            "column": "order_date",
                            "aspect": "order",
                            "role": "event",
                            "is_anchor": False,
                            "note": "When placed.",
                        },
                        {
                            "column": "due_date",
                            "aspect": "due",
                            "role": "attribute",
                            "is_anchor": True,
                            "note": "When owed.",
                        },
                    ]
                )
            )
        )


def test_attribute_only_table_needs_no_anchor() -> None:
    """A table whose only dates are attributes has no event axis, so no anchor."""
    out = TableSynthesisOutput.model_validate(
        _synthesis(
            _table(
                [
                    {
                        "column": "valid_until",
                        "aspect": "valid",
                        "role": "attribute",
                        "is_anchor": False,
                        "note": "Contract expiry.",
                    },
                ]
            )
        )
    )
    assert all(not tc.is_anchor for tc in out.tables[0].time_columns)


def test_no_time_columns_is_clean() -> None:
    """A table with no date column at all validates with an empty list."""
    ok = TableSynthesisOutput.model_validate(_synthesis(_table([])))
    assert ok.tables[0].time_columns == []


def test_missing_role_is_a_validation_error() -> None:
    """`role` is required — an omission raises so the DAT-710 repair turn fires."""
    with pytest.raises(ValidationError):
        TableSynthesisOutput.model_validate(
            _synthesis(
                _table([{"column": "order_date", "aspect": "order", "note": "When placed."}])
            )
        )


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
                    "time_columns": [],
                    "identity_columns": [],
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
                    "key_columns": [],
                }
            ],
            "column_concepts": [],
        }
    )
    assert out.tables[0].is_fact_table is True
    assert out.relationships[0].to_table == "customers"


def test_every_field_is_required_in_the_output_schema() -> None:
    """No optional fields anywhere in the semantic_per_table schema (DAT-807).

    An optional field is a modelling mistake — either the model must state the
    attribute or it should not exist — and under constrained decoding every
    optional also spends one of the request's 24 optional-parameter slots (an
    ``X | None`` renders as an anyOf, so it spends a union slot too). The
    not-applicable case is a documented empty value ("" / []), never an omitted
    key: ``column_concepts`` crowded out by an omission was schema-legal before
    DAT-768 and silently zeroed the surface (the DAT-672 class).
    """
    schema = TableSynthesisOutput.model_json_schema()
    for owner, node in [("TableSynthesisOutput", schema), *schema["$defs"].items()]:
        props = node.get("properties")
        if not props:
            continue
        optional = set(props) - set(node.get("required", []))
        assert not optional, f"{owner} has optional fields: {sorted(optional)}"


def test_omitting_column_concepts_is_a_validation_error() -> None:
    """Omitting the whole field now raises — the signal the repair turn keys on."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        TableSynthesisOutput.model_validate({"tables": [], "relationships": []})
