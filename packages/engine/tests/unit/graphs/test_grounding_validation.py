"""Provenance contract v2 enforcement — the pure validator (DAT-727).

``validate_grounding_basis`` is the save-time gate for the operating-model
graph's ``uses`` substrate: enumerated columns must be MEMBERS of the served
relation schema and COMPLETE against the SQL parts. The parts cross-check is a
validator only (never a source): DuckDB's catalog-free parse when the fragment
parses — identifier-precise, string literals and subquery-internal names
excluded — with a lexical-token fallback intersected against the relation's
known vocabulary otherwise.
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.graphs.grounding_validation import (
    schema_tables_from_info,
    validate_grounding_basis,
)
from dataraum.graphs.models import (
    ConceptGroundingBasis,
    ExtractGroundingOutput,
    GraphProvenanceOutput,
)

_SCHEMA = {"t": {"amount", "account_type", "posted", "credit", "debit"}}


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect(":memory:")
    yield c
    c.close()


def _output(
    *,
    relation: str | None = "t",
    select_expr: str = "SUM(amount)",
    where: list[str] | None = None,
    basis: dict[str, ConceptGroundingBasis] | None = None,
) -> ExtractGroundingOutput:
    return ExtractGroundingOutput(
        grounding="evidence",
        relation=relation,
        where=where or [],
        select_expr=select_expr,
        description="d",
        provenance=GraphProvenanceOutput(
            field_resolution="direct", column_mappings_basis=basis or {}
        ),
    )


def _basis(concept: str, measure: list[str], filters: list[str] | None = None):
    return {
        concept: ConceptGroundingBasis(
            measure_columns=measure, filter_columns=filters or [], resolution="direct"
        )
    }


def test_clean_enumeration_passes(conn) -> None:
    out = _output(
        select_expr="SUM(credit) - SUM(debit)",
        where=["account_type IN ('asset')", "posted = true"],
        basis=_basis("account_balance", ["credit", "debit"], ["account_type", "posted"]),
    )
    assert validate_grounding_basis(out, _SCHEMA, conn) == []


def test_fall_loud_shape_is_exempt(conn) -> None:
    out = _output(relation=None, select_expr="NULL")
    assert validate_grounding_basis(out, _SCHEMA, conn) == []


def test_unserved_relation_is_a_violation(conn) -> None:
    out = _output(relation="invented", basis=_basis("revenue", ["amount"]))
    violations = validate_grounding_basis(out, _SCHEMA, conn)
    assert len(violations) == 1
    assert "not among the served relations" in violations[0]


def test_membership_violation_names_the_column(conn) -> None:
    out = _output(basis=_basis("revenue", ["amout"]))
    violations = validate_grounding_basis(out, _SCHEMA, conn)
    assert any("'amout'" in v and "not a column" in v for v in violations)


def test_used_but_not_enumerated_is_a_violation(conn) -> None:
    """The completeness net: the where filters on account_type, the enumeration
    only carries the measure column."""
    out = _output(
        where=["account_type IN ('revenue')"],
        basis=_basis("revenue", ["amount"]),
    )
    violations = validate_grounding_basis(out, _SCHEMA, conn)
    assert any("'account_type'" in v and "does not enumerate" in v for v in violations)


def test_empty_basis_with_real_grounding_is_incomplete(conn) -> None:
    """A real grounding with NO enumeration at all fails completeness — the
    graph cannot ground `uses` edges on nothing."""
    out = _output(basis={})
    violations = validate_grounding_basis(out, _SCHEMA, conn)
    assert any("'amount'" in v for v in violations)


def test_count_star_with_empty_basis_is_clean(conn) -> None:
    """COUNT(*) touches no column — an empty enumeration is honest, not lazy."""
    out = _output(select_expr="COUNT(*)", basis={})
    assert validate_grounding_basis(out, _SCHEMA, conn) == []


def test_phantom_enumeration_is_a_violation_under_parse(conn) -> None:
    """An enumerated column the SQL never touches would mint a false `uses`
    edge — rejected (parse-verified, so it cannot be a tokenizer artifact)."""
    out = _output(basis=_basis("revenue", ["amount", "credit"]))
    violations = validate_grounding_basis(out, _SCHEMA, conn)
    assert any("'credit'" in v and "never" in v for v in violations)


def test_string_literal_containing_a_column_name_is_not_used(conn) -> None:
    """Parser precision: 'amount' inside a VALUE literal is not a column
    reference — the lexical net alone would flag it; the parse must not."""
    out = _output(
        where=["account_type IN ('amount', 'revenue')"],
        basis=_basis("revenue", ["amount"], ["account_type"]),
    )
    assert validate_grounding_basis(out, _SCHEMA, conn) == []


def test_subquery_internal_columns_are_not_used(conn) -> None:
    """`x IN (SELECT id FROM ref)` references columns of the OTHER relation —
    a name collision with a served column ('posted' here) must not count."""
    out = _output(
        where=["account_type IN (SELECT posted FROM ref_table WHERE x = 1)"],
        basis=_basis("revenue", ["amount"], ["account_type"]),
    )
    assert validate_grounding_basis(out, _SCHEMA, conn) == []


def test_qualified_reference_strips_the_table_prefix(conn) -> None:
    out = _output(select_expr="SUM(t.amount)", basis=_basis("revenue", ["amount"]))
    assert validate_grounding_basis(out, _SCHEMA, conn) == []


def test_unparseable_fragment_falls_back_to_lexical_tokens(conn) -> None:
    """A fragment the parser rejects still gets the coarse net: tokens matched
    against the known vocabulary. 'credit' appears lexically, is a served
    column, and is not enumerated → violation. The phantom check stays OFF
    under fallback (the over-collecting tokenizer would misread honest rows)."""
    out = _output(
        # DuckDB cannot parse this fragment ('??' operator); 'credit' rides in it.
        where=["credit ?? 1"],
        basis=_basis("revenue", ["amount", "posted"]),
    )
    violations = validate_grounding_basis(out, _SCHEMA, conn)
    assert any("'credit'" in v and "does not enumerate" in v for v in violations)
    # 'posted' is enumerated-but-unused, yet fallback mode must NOT flag it.
    assert not any("never" in v for v in violations)


def test_schema_tables_from_info_shapes_the_served_schema() -> None:
    info = {
        "tables": [
            {"table_name": "t", "columns": [{"name": "a", "type": "X"}], "row_count": 1},
            {"table_name": "u", "columns": [], "row_count": 0},
        ]
    }
    assert schema_tables_from_info(info) == {"t": {"a"}, "u": set()}
