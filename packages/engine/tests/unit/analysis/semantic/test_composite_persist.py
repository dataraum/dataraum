"""LLM-confirmed composite-key persistence + consumer exclusion (DAT-277 B2a).

The rescue surfaces a composite to the LLM; when the LLM confirms it via
``RelationshipOutput.key_columns`` the processor persists the whole key as ONE
group (N component rows sharing a ``relationship_group_id`` + the composite
cardinality). Single-column consumers must NOT see those grouped rows (they join
on one column and would fan out), so ``load_defined_relationships`` excludes them
by default — the multi-column enrichment path opts in.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.utils import load_defined_relationships
from dataraum.analysis.semantic.models import Relationship as SemanticRelationship
from dataraum.analysis.semantic.processor import (
    _build_composite_group_rows,
    _dedup_relationship_rows,
)
from dataraum.core.models.base import RelationshipType
from dataraum.storage import Column, Source, Table


def _composite_rel(key_columns: list[tuple[str, str]]) -> SemanticRelationship:
    return SemanticRelationship(
        relationship_id="r1",
        from_table="txn",
        from_column="account",
        to_table="coa",
        to_column="account_name",
        key_columns=key_columns,
        relationship_type=RelationshipType.FOREIGN_KEY,
        cardinality="many-to-many",
        confidence=0.9,
        detection_method="llm_tool",
    )


def _column_map() -> dict[tuple[str, str], str]:
    return {
        ("txn", "account"): "txn_account",
        ("txn", "business_id"): "txn_biz",
        ("coa", "account_name"): "coa_name",
        ("coa", "business_id"): "coa_biz",
    }


def test_single_column_relationship_is_not_a_group() -> None:
    """No key_columns → None, so the caller persists the plain single-column row."""
    rows = _build_composite_group_rows(
        rel=_composite_rel([]),
        from_table_id="tt",
        from_col_id="txn_account",
        to_table_id="tc",
        to_col_id="coa_name",
        column_map=_column_map(),
        evidence={},
        run_id="run-A",
        duckdb_conn=None,
    )
    assert rows is None


def test_composite_persists_as_one_group() -> None:
    """Anchor at position 0 + each key column, all sharing one group id + cardinality."""
    rows = _build_composite_group_rows(
        rel=_composite_rel([("business_id", "business_id")]),
        from_table_id="tt",
        from_col_id="txn_account",
        to_table_id="tc",
        to_col_id="coa_name",
        column_map=_column_map(),
        evidence={"source": "table_synthesis"},
        run_id="run-A",
        duckdb_conn=None,  # no lake → composite cardinality falls back to rel.cardinality
    )
    assert rows is not None
    assert len(rows) == 2
    group_ids = {r["relationship_group_id"] for r in rows}
    assert len(group_ids) == 1 and None not in group_ids
    by_pos = {r["key_position"]: r for r in rows}
    assert by_pos[0]["from_column_id"] == "txn_account"
    assert by_pos[0]["to_column_id"] == "coa_name"
    assert by_pos[1]["from_column_id"] == "txn_biz"
    assert by_pos[1]["to_column_id"] == "coa_biz"
    # every component row carries the composite cardinality + the component count
    assert all(r["cardinality"] == "many-to-many" for r in rows)
    assert all(r["evidence"]["composite_key_columns"] == 2 for r in rows)


def test_anchor_echoed_in_key_columns_does_not_duplicate() -> None:
    """The LLM echoing the anchor pair in key_columns must not produce a colliding row.

    Two component rows sharing (run_id, from_column_id, to_column_id, 'llm') would
    violate the upsert unique key. With only the anchor + an echo, the result is a
    plain single-column relationship (None) — never a duplicate-component group.
    """
    rows = _build_composite_group_rows(
        rel=_composite_rel([("account", "account_name")]),  # == the anchor pair
        from_table_id="tt",
        from_col_id="txn_account",
        to_table_id="tc",
        to_col_id="coa_name",
        column_map=_column_map(),
        evidence={},
        run_id="run-A",
        duckdb_conn=None,
    )
    assert rows is None


def test_anchor_echo_plus_real_scope_keeps_only_the_scope() -> None:
    """An echoed anchor alongside a genuine scope column yields a clean 2-row group."""
    rows = _build_composite_group_rows(
        rel=_composite_rel([("account", "account_name"), ("business_id", "business_id")]),
        from_table_id="tt",
        from_col_id="txn_account",
        to_table_id="tc",
        to_col_id="coa_name",
        column_map=_column_map(),
        evidence={},
        run_id="run-A",
        duckdb_conn=None,
    )
    assert rows is not None
    assert len(rows) == 2
    assert {(r["from_column_id"], r["to_column_id"]) for r in rows} == {
        ("txn_account", "coa_name"),
        ("txn_biz", "coa_biz"),
    }


def test_unresolvable_key_column_falls_back_to_single() -> None:
    """A composite component that doesn't map to a column id → None (persist anchor only)."""
    rows = _build_composite_group_rows(
        rel=_composite_rel([("ghost", "phantom")]),
        from_table_id="tt",
        from_col_id="txn_account",
        to_table_id="tc",
        to_col_id="coa_name",
        column_map=_column_map(),
        evidence={},
        run_id="run-A",
        duckdb_conn=None,
    )
    assert rows is None


def _row(frm: str, to: str, group_id: str | None) -> dict:
    return {
        "run_id": "run-A",
        "from_column_id": frm,
        "to_column_id": to,
        "detection_method": "llm",
        "relationship_group_id": group_id,
        "cardinality": "many-to-one",
    }


def test_dedup_prefers_composite_over_plain_on_conflict() -> None:
    """The smoke failure: a pair emitted BOTH standalone and as a composite component.

    Postgres ON CONFLICT can't touch a row twice in one batch — the two rows sharing
    (run_id, from_column_id, to_column_id, detection_method) must collapse to one,
    keeping the composite-component row (it carries the group).
    """
    plain = _row("txn_biz", "cust_biz", group_id=None)
    component = _row("txn_biz", "cust_biz", group_id="g1")
    # order-independent: composite wins whether it comes first or second
    assert _dedup_relationship_rows([plain, component]) == [component]
    assert _dedup_relationship_rows([component, plain]) == [component]


def test_dedup_folds_duplicate_plain_rows() -> None:
    """Two identical plain relationships (LLM listed a pair twice) collapse to one."""
    a = _row("txn_acct", "coa_name", group_id=None)
    b = _row("txn_acct", "coa_name", group_id=None)
    assert _dedup_relationship_rows([a, b]) == [a]


def test_dedup_keeps_distinct_pairs() -> None:
    """Distinct column pairs (incl. composite components on different columns) all survive."""
    rows = [
        _row("txn_acct", "coa_name", group_id="g1"),
        _row("txn_biz", "coa_biz", group_id="g1"),
        _row("txn_cust", "cust_name", group_id="g2"),
    ]
    assert _dedup_relationship_rows(rows) == rows


def _seed(session: Session) -> None:
    session.add(Source(source_id="s1", name="s1", source_type="csv"))
    session.add(Table(table_id="t1", source_id="s1", table_name="txn", layer="typed"))
    session.add(Table(table_id="t2", source_id="s1", table_name="coa", layer="typed"))
    for cid in ("txn_account", "txn_biz"):
        session.add(Column(column_id=cid, table_id="t1", column_name=cid, column_position=0))
    for cid in ("coa_name", "coa_biz"):
        session.add(Column(column_id=cid, table_id="t2", column_name=cid, column_position=0))
    session.flush()


def _add_rel(session: Session, frm: str, to: str, group_id: str | None, position: int | None) -> None:
    session.add(
        Relationship(
            run_id="run-A",
            from_table_id="t1",
            from_column_id=frm,
            to_table_id="t2",
            to_column_id=to,
            relationship_type="foreign_key",
            cardinality="many-to-one",
            relationship_group_id=group_id,
            key_position=position,
            confidence=0.9,
            detection_method="llm",
        )
    )


def test_load_defined_relationships_excludes_composite_groups_by_default(session: Session) -> None:
    """Grouped component rows are hidden from single-column consumers; opt-in shows them."""
    _seed(session)
    # A plain single-column relationship on a DISTINCT pair (the unique key is
    # (run_id, from_column_id, to_column_id, detection_method) — an anchor never
    # collides with a real single-column row because the LLM emits one or the other).
    _add_rel(session, "txn_biz", "coa_name", group_id=None, position=None)  # plain
    _add_rel(session, "txn_account", "coa_name", group_id="g1", position=0)  # composite anchor
    _add_rel(session, "txn_biz", "coa_biz", group_id="g1", position=1)  # composite scope
    session.flush()

    default = load_defined_relationships(session, ["t1", "t2"], run_id="run-A")
    assert {r.relationship_group_id for r in default} == {None}
    assert len(default) == 1

    with_groups = load_defined_relationships(
        session, ["t1", "t2"], run_id="run-A", include_composite_groups=True
    )
    assert len(with_groups) == 3
