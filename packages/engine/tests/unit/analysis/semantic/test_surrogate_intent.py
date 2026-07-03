"""Confirmed composites persist as surrogate-key intents, never llm rows (DAT-277).

``synthesize_and_store_tables`` routes a relationship carrying ``key_columns``
into ``surrogate_key_intents`` — the handoff to the ``surrogate_mint`` phase —
so no single-column consumer ever sees the fan-out anchor as a defined
relationship. Every unbuildable intent falls back to the ordinary single-column
persist: the anchor is still a real confirmed relationship, and its empirical
cardinality/fan-trap flag say what joining it alone would do.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock

import duckdb
import pytest
from sqlalchemy import select

from dataraum.analysis.relationships.db_models import Relationship as RelationshipDB
from dataraum.analysis.relationships.db_models import SurrogateKeyIntent
from dataraum.analysis.semantic.models import Relationship, SemanticEnrichmentResult
from dataraum.analysis.semantic.processor import synthesize_and_store_tables
from dataraum.core.models.base import RelationshipType, Result
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_run_id


def _table_with_columns(session, name: str, columns: list[str]) -> Table:
    src = Source(name=f"src_{name}", source_type="csv")
    session.add(src)
    session.flush()
    table = Table(source_id=src.source_id, table_name=name, layer="typed", row_count=6)
    session.add(table)
    session.flush()
    for pos, col in enumerate(columns):
        session.add(
            Column(
                table_id=table.table_id, column_name=col, column_position=pos, raw_type="VARCHAR"
            )
        )
    session.flush()
    return table


@pytest.fixture
def lake() -> Iterator[duckdb.DuckDBPyConnection]:
    """The BookSQL fan-out shape in a ``lake.typed`` schema (mirrors the worker attach)."""
    c = duckdb.connect()
    try:
        c.execute("ATTACH ':memory:' AS lake")
        c.execute("CREATE SCHEMA lake.typed")
        c.execute('CREATE TABLE lake.typed."txn" (account VARCHAR, business_id VARCHAR)')
        c.execute(
            'INSERT INTO lake.typed."txn" VALUES '
            "('Sales','B1'),('Sales','B1'),('COGS','B1'),('Sales','B2'),('COGS','B2')"
        )
        c.execute(
            'CREATE TABLE lake.typed."coa" (account_name VARCHAR, business_id VARCHAR)'
        )
        c.execute(
            'INSERT INTO lake.typed."coa" VALUES '
            "('Sales','B1'),('COGS','B1'),('Sales','B2'),('COGS','B2')"
        )
        yield c
    finally:
        c.close()


def _rel(key_columns: list[tuple[str, str]], confidence: float = 0.9) -> Relationship:
    return Relationship(
        relationship_id="rel-1",
        from_table="txn",
        from_column="account",
        to_table="coa",
        to_column="account_name",
        key_columns=key_columns,
        relationship_type=RelationshipType.FOREIGN_KEY,
        confidence=confidence,
        detection_method="llm_tool",
        evidence={"source": "table_synthesis", "reasoning": "composite key"},
    )


def _agent(relationships: list[Relationship]) -> MagicMock:
    agent = MagicMock()
    agent.synthesize_tables = MagicMock(
        return_value=Result.ok(
            SemanticEnrichmentResult(
                annotations=[], entity_detections=[], relationships=relationships
            )
        )
    )
    return agent


def _store(session, agent, tables, conn=None, run_id=None):
    return synthesize_and_store_tables(
        session,
        agent,
        [t.table_id for t in tables],
        duckdb_conn=conn,
        run_id=run_id or baseline_run_id(),
    )


def test_confirmed_composite_persists_intent_not_llm_row(session, lake) -> None:
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    agent = _agent([_rel(key_columns=[("business_id", "business_id")])])

    result = _store(session, agent, [txn, coa], conn=lake)
    session.flush()

    assert result.success
    llm_rows = (
        session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
        .scalars()
        .all()
    )
    assert llm_rows == []  # the half-key anchor must NOT enter the defined catalog

    intents = session.execute(select(SurrogateKeyIntent)).scalars().all()
    assert len(intents) == 1
    intent = intents[0]
    cols = {(c.table_id, c.column_name): c.column_id for t in (txn, coa) for c in t.columns}
    assert intent.column_pairs == [
        [cols[(txn.table_id, "account")], cols[(coa.table_id, "account_name")]],
        [cols[(txn.table_id, "business_id")], cols[(coa.table_id, "business_id")]],
    ]  # anchor FIRST, then the scoping component
    assert intent.cardinality == "many-to-one"  # the collapse proof, measured
    assert intent.from_table_id == txn.table_id
    assert intent.to_table_id == coa.table_id
    assert intent.intent_digest


def test_anchor_echoed_alone_falls_back_to_single(session) -> None:
    """key_columns that dedup down to just the anchor = a single-column relationship."""
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    agent = _agent([_rel(key_columns=[("account", "account_name")])])  # echo of the anchor

    assert _store(session, agent, [txn, coa]).success
    session.flush()

    assert session.execute(select(SurrogateKeyIntent)).scalars().all() == []
    llm_rows = (
        session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
        .scalars()
        .all()
    )
    assert len(llm_rows) == 1


def test_unresolvable_component_falls_back_to_single(session) -> None:
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    agent = _agent([_rel(key_columns=[("ghost", "ghost")])])

    assert _store(session, agent, [txn, coa]).success
    session.flush()

    assert session.execute(select(SurrogateKeyIntent)).scalars().all() == []
    llm_rows = (
        session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
        .scalars()
        .all()
    )
    assert len(llm_rows) == 1


def test_missing_run_id_builds_no_intent(session) -> None:
    """No run_id = nothing to version the intent under → the builder abstains.

    Tested at the helper level: the workflow always stamps run_id, and the
    public path with run_id=None cannot persist ANY relationship anyway
    (``relationships.run_id`` is NOT NULL — pre-existing main behavior), so
    the guard's job is just to never mint an unversioned intent row.
    """
    from dataraum.analysis.semantic.processor import _build_surrogate_intent

    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    cols = {(c.table_id, c.column_name): c.column_id for t in (txn, coa) for c in t.columns}

    intent = _build_surrogate_intent(
        rel=_rel(key_columns=[("business_id", "business_id")]),
        from_table_id=txn.table_id,
        from_col_id=cols[(txn.table_id, "account")],
        to_table_id=coa.table_id,
        to_col_id=cols[(coa.table_id, "account_name")],
        column_map={
            ("txn", "account"): cols[(txn.table_id, "account")],
            ("txn", "business_id"): cols[(txn.table_id, "business_id")],
            ("coa", "account_name"): cols[(coa.table_id, "account_name")],
            ("coa", "business_id"): cols[(coa.table_id, "business_id")],
        },
        run_id=None,
        duckdb_conn=None,
    )

    assert intent is None


def test_intent_upsert_is_idempotent_for_retry(session, lake) -> None:
    """A Temporal at-least-once retry (same run_id) refreshes, never duplicates."""
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    agent = _agent([_rel(key_columns=[("business_id", "business_id")])])

    assert _store(session, agent, [txn, coa], conn=lake).success
    session.flush()
    assert _store(session, agent, [txn, coa], conn=lake).success
    session.flush()

    assert len(session.execute(select(SurrogateKeyIntent)).scalars().all()) == 1


def test_scope_component_order_is_canonical(session) -> None:
    """A shuffled key_columns order must produce the SAME digest and pair order —
    the surrogate name (and its upserted column_id) derive from it, and the
    LLM's ordering is not stable across runs.
    """
    from dataraum.analysis.semantic.processor import _build_surrogate_intent

    txn = _table_with_columns(session, "txn", ["account", "business_id", "region"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id", "region"])
    cols = {(c.table_id, c.column_name): c.column_id for t in (txn, coa) for c in t.columns}
    column_map = {
        ("txn", "account"): cols[(txn.table_id, "account")],
        ("txn", "business_id"): cols[(txn.table_id, "business_id")],
        ("txn", "region"): cols[(txn.table_id, "region")],
        ("coa", "account_name"): cols[(coa.table_id, "account_name")],
        ("coa", "business_id"): cols[(coa.table_id, "business_id")],
        ("coa", "region"): cols[(coa.table_id, "region")],
    }

    def _build(key_columns: list[tuple[str, str]]):
        return _build_surrogate_intent(
            rel=_rel(key_columns=key_columns),
            from_table_id=txn.table_id,
            from_col_id=cols[(txn.table_id, "account")],
            to_table_id=coa.table_id,
            to_col_id=cols[(coa.table_id, "account_name")],
            column_map=column_map,
            run_id=baseline_run_id(),
            duckdb_conn=None,
        )

    a = _build([("region", "region"), ("business_id", "business_id")])
    b = _build([("business_id", "business_id"), ("region", "region")])

    assert a is not None and b is not None
    assert a["intent_digest"] == b["intent_digest"]
    assert a["column_pairs"] == b["column_pairs"]
    # Anchor first, scope sorted by from-side name.
    assert a["column_pairs"][0][0] == cols[(txn.table_id, "account")]
    assert a["column_pairs"][1][0] == cols[(txn.table_id, "business_id")]
    assert a["column_pairs"][2][0] == cols[(txn.table_id, "region")]


def test_duplicate_llm_relationships_fold_to_one_row(session) -> None:
    """The LLM emitting one pair twice must fold, not crash the ON CONFLICT batch."""
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    agent = _agent([_rel(key_columns=[]), _rel(key_columns=[])])

    assert _store(session, agent, [txn, coa]).success
    session.flush()

    llm_rows = (
        session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
        .scalars()
        .all()
    )
    assert len(llm_rows) == 1
