"""Format-stability contract for DuckDB's ``json_serialize_sql`` (DAT-654).

Both SQL consumers — the enriched-view equality gate
(:mod:`dataraum.core.sql_normalize`) and the derived-formula parser
(:mod:`dataraum.entropy.measurements.derived_value`) — walk the parse tree by
these exact field names. This test pins the shape so a DuckDB upgrade that
changes the serialization breaks loudly *here* rather than silently degrading
the gate (it would fall back to byte-equality) or the formula parser (it would
abstain), both of which fail quietly and would pass every behavioural test.
"""

from __future__ import annotations

import json

import duckdb


def _tree(sql: str) -> dict:
    conn = duckdb.connect(":memory:")
    row = conn.cursor().execute("SELECT json_serialize_sql(?::VARCHAR)", [sql]).fetchone()
    assert row is not None and row[0] is not None
    return json.loads(row[0])


def test_top_level_error_flag_and_statement_node() -> None:
    tree = _tree('SELECT f.* FROM "orders" AS f')
    assert tree["error"] is False
    node = tree["statements"][0]["node"]
    assert node["type"] == "SELECT_NODE"
    assert "select_list" in node and "from_table" in node


def test_non_select_sets_error_true() -> None:
    # The wrapper-strip depends on CREATE VIEW being unserializable (error=True),
    # so serialize_sql returns None and the view-gate falls back correctly.
    tree = _tree("CREATE OR REPLACE VIEW v AS SELECT 1")
    assert tree["error"] is True


def test_every_node_carries_query_location() -> None:
    # ``query_location`` is the formatting noise the gate strips; if it ever
    # stopped appearing (or a NEW volatile field appeared), canonicalization
    # would drift.
    node = _tree('SELECT f."a" FROM "orders" AS f')["statements"][0]["node"]
    assert "query_location" in node["select_list"][0]
    assert "query_location" in node["from_table"]


def test_column_ref_shape() -> None:
    node = _tree('SELECT t."net" FROM t')["statements"][0]["node"]
    col = node["select_list"][0]
    assert col["type"] == "COLUMN_REF"
    # Qualified name → ``[qualifier, column]``; the formula parser reads ``[-1]``.
    assert col["column_names"] == ["t", "net"]


def test_operator_function_shape() -> None:
    # The derived-formula parser keys on FUNCTION + is_operator + function_name
    # (the operator token) + a two-element ``children`` list.
    node = _tree("SELECT net + tax AS x")["statements"][0]["node"]
    fn = node["select_list"][0]
    assert fn["type"] == "FUNCTION"
    assert fn["is_operator"] is True
    assert fn["function_name"] == "+"
    assert [c["type"] for c in fn["children"]] == ["COLUMN_REF", "COLUMN_REF"]


def test_named_function_is_not_an_operator() -> None:
    # A call like ``UPPER(name)`` must be distinguishable from an arithmetic
    # operator so the formula parser rejects it (``is_operator`` False).
    fn = _tree("SELECT UPPER(name) AS x")["statements"][0]["node"]["select_list"][0]
    assert fn["type"] == "FUNCTION"
    assert fn["is_operator"] is False
    assert fn["function_name"] == "upper"


def test_equality_is_a_comparison_with_left_right() -> None:
    # ``target = expr`` is a COMPARISON (COMPARE_EQUAL) with ``left``/``right`` —
    # NOT ``children`` — which is how the formula parser unwraps the equation.
    node = _tree("SELECT total = net + tax AS x")["statements"][0]["node"]
    cmp = node["select_list"][0]
    assert cmp["class"] == "COMPARISON"
    assert cmp["type"] == "COMPARE_EQUAL"
    assert cmp["left"]["type"] == "COLUMN_REF"
    assert cmp["right"]["type"] == "FUNCTION"


def test_base_table_qualifier_fields() -> None:
    tbl = _tree('SELECT * FROM lake.typed."orders"')["statements"][0]["node"]["from_table"]
    assert tbl["type"] == "BASE_TABLE"
    assert tbl["catalog_name"] == "lake"
    assert tbl["schema_name"] == "typed"
    assert tbl["table_name"] == "orders"


def test_constant_operand_is_not_a_column_ref() -> None:
    # A literal operand (``amount * 0.19``) must not read as a COLUMN_REF, so the
    # formula parser rejects it rather than inventing a column name.
    fn = _tree("SELECT amount * 0.19 AS x")["statements"][0]["node"]["select_list"][0]
    kinds = [c["type"] for c in fn["children"]]
    assert "COLUMN_REF" in kinds
    assert any(k != "COLUMN_REF" for k in kinds)
