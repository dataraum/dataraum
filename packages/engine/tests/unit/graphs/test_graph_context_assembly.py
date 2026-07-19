"""Unit tests for the graph-read assembly fold (DAT-734).

``_assemble_concept_contexts`` is the pure fold from traversal rows to
``ConceptContext`` — exercised here with fake rows so every loud-absence branch
(dropped edge, empty uses, missing relation, unresolved concept) is pinned
without Postgres. The live PGQ reads are covered by
``tests/integration/graphs/test_graph_context.py``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from dataraum.graphs.context import _assemble_concept_contexts


def _row(**kw: Any) -> SimpleNamespace:
    return SimpleNamespace(**kw)


def _grounding_row(**overrides: Any) -> SimpleNamespace:
    base: dict[str, Any] = {
        "concept_name": "revenue",
        "snippet_id": "sn_1",
        "relation": "enriched_sales",
        "select_expr": 'SUM("amount")',
        "where_predicates": "[\"x IN ('a')\"]",
        "statement": "income_statement",
        "aggregation": "sum",
        "description": "d",
        "failed": False,
    }
    base.update(overrides)
    return _row(**base)


_TABLES = {"t1": ("enriched_sales", "enriched")}


def test_groundings_fold_with_uses_and_where() -> None:
    uses = [
        _row(snippet_id="sn_1", role="filter", column_name="x", table_id="t1"),
        _row(snippet_id="sn_1", role="measure", column_name="amount", table_id="t1"),
    ]
    out = _assemble_concept_contexts(
        [("revenue", "measure")], [], {}, [_grounding_row()], uses, {}, _TABLES
    )
    assert len(out) == 1
    g = out[0].groundings[0]
    assert g.where == ["x IN ('a')"]
    # deterministic role-first ordering (filter < measure alphabetically)
    assert [(u.role, u.column_name) for u in g.uses] == [("filter", "x"), ("measure", "amount")]


def test_healthy_grounding_without_relation_skipped_loud() -> None:
    rows = [_grounding_row(relation=None, select_expr=None, where_predicates=None)]
    out = _assemble_concept_contexts([("revenue", "measure")], [], {}, rows, [], {}, _TABLES)
    assert out[0].groundings == []


def test_failed_grounding_served_with_failure_keys() -> None:
    rows = [_grounding_row(failed=True, relation=None, snippet_id="sn_f")]
    prov = {
        "sn_f": _row(
            concept="revenue",
            failed=True,
            failure_mode="execution_failed",
            failure_reason="boom",
        )
    }
    out = _assemble_concept_contexts([("revenue", "measure")], [], {}, rows, [], prov, _TABLES)
    g = out[0].groundings[0]
    assert g.failed is True
    assert g.failure_mode == "execution_failed"
    assert g.failure_reason == "boom"
    assert g.uses == []


def test_uses_with_unresolvable_table_endpoint_dropped_not_crashed() -> None:
    uses = [_row(snippet_id="sn_1", role="measure", column_name="amount", table_id="t_gone")]
    out = _assemble_concept_contexts(
        [("revenue", "measure")], [], {}, [_grounding_row()], uses, {}, _TABLES
    )
    assert out[0].groundings[0].uses == []


def test_concept_edge_buckets_and_ordering() -> None:
    edges = [
        _row(from_name="ap", predicate="part_of", tolerance=None, to_name="wc"),
        _row(from_name="ap", predicate="disjoint_with", tolerance=None, to_name="ar"),
        _row(from_name="ar", predicate="disjoint_with", tolerance=None, to_name="ap"),
        _row(from_name="ap", predicate="reconciles_with", tolerance=0.01, to_name="ap"),
    ]
    out = _assemble_concept_contexts(
        [("ap", "measure"), ("ar", "measure"), ("wc", "measure")],
        edges,
        {"ap": ["fin_position"]},
        [],
        [],
        {},
        _TABLES,
    )
    ap = next(c for c in out if c.name == "ap")
    wc = next(c for c in out if c.name == "wc")
    assert ap.part_of_parents == ["wc"]
    assert wc.part_of_children == ["ap"]
    assert ap.part_of_ancestry == ["fin_position"]
    assert ap.disjoint_with == ["ar"]
    assert len(ap.reconciles_with) == 1
    assert ap.reconciles_with[0].partner == "ap"
    assert ap.reconciles_with[0].tolerance == 0.01


def test_concept_order_is_input_order_and_multi_grounding_sorted() -> None:
    rows = [
        _grounding_row(snippet_id="sn_b", statement="balance_sheet"),
        _grounding_row(snippet_id="sn_a", statement="trial_balance"),
        _grounding_row(snippet_id="sn_x", statement="cash_flow", failed=True),
    ]
    out = _assemble_concept_contexts([("revenue", None)], [], {}, rows, [], {}, _TABLES)
    ids = [g.snippet_id for g in out[0].groundings]
    # healthy first (failed sorts last), then (relation, snippet_id)
    assert ids == ["sn_a", "sn_b", "sn_x"]
