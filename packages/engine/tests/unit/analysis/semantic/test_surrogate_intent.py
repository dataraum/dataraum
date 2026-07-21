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
from dataraum.analysis.semantic.models import (
    Relationship,
    SemanticEnrichmentResult,
)
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
    """The tenant-scoped fan-out shape in a ``lake.typed`` schema (mirrors the worker attach)."""
    c = duckdb.connect()
    try:
        c.execute("ATTACH ':memory:' AS lake")
        c.execute("CREATE SCHEMA lake.typed")
        c.execute('CREATE TABLE lake.typed."txn" (account VARCHAR, business_id VARCHAR)')
        c.execute(
            'INSERT INTO lake.typed."txn" VALUES '
            "('Sales','B1'),('Sales','B1'),('COGS','B1'),('Sales','B2'),('COGS','B2')"
        )
        c.execute('CREATE TABLE lake.typed."coa" (account_name VARCHAR, business_id VARCHAR)')
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
                annotations=[],
                entity_detections=[],
                relationships=relationships,
            )
        )
    )
    agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
    return agent


def _store(session, agent, tables, conn=None, run_id=None, candidates=None):
    return synthesize_and_store_tables(
        session,
        agent,
        [t.table_id for t in tables],
        relationship_candidates=candidates,
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
    ]  # canonical direction-neutral order ('account…' sorts before 'business_id')
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


def test_non_collapsing_composite_falls_back_to_single(session, lake) -> None:
    """A confirmed composite the data REJECTS (still m2m — dup dim rows) must not
    become an intent: fall back to the plain anchor with its honest fan-trap
    flag. Seen live on the bookkeeping smoke corpus (duplicate (account, business) rows in the
    chart of accounts)."""
    lake.execute("INSERT INTO lake.typed.\"coa\" VALUES ('Sales','B1')")  # dup dim row
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    agent = _agent([_rel(key_columns=[("business_id", "business_id")])])

    assert _store(session, agent, [txn, coa], conn=lake).success
    session.flush()

    assert session.execute(select(SurrogateKeyIntent)).scalars().all() == []
    llm_rows = (
        session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
        .scalars()
        .all()
    )
    assert len(llm_rows) == 1  # the anchor persists, honestly flagged


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
    # ALL pairs sorted by a direction-neutral name key — neither the anchor nor
    # the from/to orientation holds positional privilege in the column identity.
    assert a["column_pairs"][0][0] == cols[(txn.table_id, "account")]
    assert a["column_pairs"][1][0] == cols[(txn.table_id, "business_id")]
    assert a["column_pairs"][2][0] == cols[(txn.table_id, "region")]


def test_anchor_choice_does_not_change_the_surrogate_identity(session) -> None:
    """Seen live: the LLM anchored the same composite on payment_method one run
    and business_id the next, minting two differently-named surrogates for one
    key. The digest and pair order must be identical whichever pair the LLM
    picks as the anchor."""
    from dataraum.analysis.semantic.processor import _build_surrogate_intent

    txn = _table_with_columns(session, "txn", ["payment_method", "business_id"])
    pm = _table_with_columns(session, "pm", ["payment_method", "business_id"])
    cols = {(c.table_id, c.column_name): c.column_id for t in (txn, pm) for c in t.columns}
    column_map = {
        ("txn", "payment_method"): cols[(txn.table_id, "payment_method")],
        ("txn", "business_id"): cols[(txn.table_id, "business_id")],
        ("pm", "payment_method"): cols[(pm.table_id, "payment_method")],
        ("pm", "business_id"): cols[(pm.table_id, "business_id")],
    }

    def _build(anchor: str, scope: str):
        rel = Relationship(
            relationship_id="rel-1",
            from_table="txn",
            from_column=anchor,
            to_table="pm",
            to_column=anchor,
            key_columns=[(scope, scope)],
            relationship_type=RelationshipType.FOREIGN_KEY,
            confidence=0.9,
            detection_method="llm_tool",
            evidence={"source": "table_synthesis"},
        )
        return _build_surrogate_intent(
            rel=rel,
            from_table_id=txn.table_id,
            from_col_id=cols[(txn.table_id, anchor)],
            to_table_id=pm.table_id,
            to_col_id=cols[(pm.table_id, anchor)],
            column_map=column_map,
            run_id=baseline_run_id(),
            duckdb_conn=None,
        )

    anchored_on_pm = _build("payment_method", "business_id")
    anchored_on_biz = _build("business_id", "payment_method")
    assert anchored_on_pm is not None and anchored_on_biz is not None
    assert anchored_on_pm["intent_digest"] == anchored_on_biz["intent_digest"]
    assert anchored_on_pm["column_pairs"] == anchored_on_biz["column_pairs"]


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


def test_llm_persist_orients_and_marks_judge_confirmed(session) -> None:
    """Write path 2 (DAT-777/776): the judge emits a single-column FK in the
    dim-first (measured one-to-many) direction; it persists many→one child→parent,
    confirmed by the judge. Cardinality rides on the candidate metrics — no duckdb
    needed. Uses the txn/coa shape so the `_agent` mock's meaning resolves (the
    DAT-768/769 empty-surface gate)."""
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    cols = {(c.table_id, c.column_name): c.column_id for t in (txn, coa) for c in t.columns}
    rel = Relationship(
        relationship_id="rel-1",
        from_table="coa",  # dim side named FROM (reversed / one-to-many)
        from_column="account_name",
        to_table="txn",
        to_column="account",
        key_columns=[],
        relationship_type=RelationshipType.FOREIGN_KEY,
        confidence=0.9,
        detection_method="llm_tool",
        evidence={"source": "table_synthesis"},
    )
    candidate = {
        "table1": "coa",
        "table2": "txn",
        "join_columns": [
            {"column1": "account_name", "column2": "account", "cardinality": "one-to-many"}
        ],
    }

    assert _store(session, _agent([rel]), [txn, coa], candidates=[candidate]).success
    session.flush()

    row = session.execute(
        select(RelationshipDB).where(RelationshipDB.detection_method == "llm")
    ).scalar_one()
    # Flipped to child(txn.account) → parent(coa.account_name), many-to-one.
    assert row.from_column_id == cols[(txn.table_id, "account")]
    assert row.to_column_id == cols[(coa.table_id, "account_name")]
    assert row.cardinality == "many-to-one"
    assert row.confirmation_source == "judge"


# --- Composite VERDICT records (DAT-697) ------------------------------------------
#
# Every offered rescue hint is adjudicated: confirmed → a status='confirmed'
# intent (the mint's instruction), declined → a status='declined' record (the
# keeper machinery's stop sign). The hint dicts below are pre-attached to the
# candidates (augmentation only ADDs hints; with duckdb_conn=None it is a no-op).


def _hint_candidate(
    table1: str, table2: str, pairs: list[list[str]], coverage: float | None = 0.003
) -> dict:
    hint: dict = {"column_pairs": pairs, "cardinality": "many-to-one", "coverage_table": table1}
    if coverage is not None:
        hint["coverage"] = coverage
    return {"table1": table1, "table2": table2, "join_columns": [], "composite_key": hint}


def test_offered_hint_not_confirmed_records_declined_intent(session) -> None:
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    candidate = _hint_candidate(
        "txn", "coa", [["account", "account_name"], ["business_id", "business_id"]]
    )

    assert _store(session, _agent([]), [txn, coa], candidates=[candidate]).success
    session.flush()

    intents = session.execute(select(SurrogateKeyIntent)).scalars().all()
    assert len(intents) == 1
    declined = intents[0]
    assert declined.status == "declined"
    assert declined.confidence == 0.0
    assert declined.from_table_id == txn.table_id
    assert declined.to_table_id == coa.table_id
    assert "0.3%" in (declined.reasoning or "")
    cols = {(c.table_id, c.column_name): c.column_id for t in (txn, coa) for c in t.columns}
    assert sorted(map(tuple, declined.column_pairs)) == sorted(
        [
            (cols[(txn.table_id, "account")], cols[(coa.table_id, "account_name")]),
            (cols[(txn.table_id, "business_id")], cols[(coa.table_id, "business_id")]),
        ]
    )


def test_confirmed_hint_records_no_declined_row_even_direction_flipped(session) -> None:
    """The hint arrives in the candidate's orientation, the confirmation in the
    LLM's — a direction-sensitive digest would record the SAME composite as
    both confirmed and declined. The verdict arithmetic must see one identity."""
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    # Hint oriented coa→txn; the LLM confirms txn→coa (_rel's orientation).
    flipped = _hint_candidate(
        "coa", "txn", [["account_name", "account"], ["business_id", "business_id"]]
    )
    agent = _agent([_rel(key_columns=[("business_id", "business_id")])])

    assert _store(session, agent, [txn, coa], candidates=[flipped]).success
    session.flush()

    intents = session.execute(select(SurrogateKeyIntent)).scalars().all()
    assert [i.status for i in intents] == ["confirmed"]


def test_unresolvable_hint_records_no_verdict(session) -> None:
    """No identity, no verdict — keeper protection stands for the pair."""
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    ghost = _hint_candidate("txn", "coa", [["account", "account_name"], ["ghost", "ghost"]])

    assert _store(session, _agent([]), [txn, coa], candidates=[ghost]).success
    session.flush()

    assert session.execute(select(SurrogateKeyIntent)).scalars().all() == []


def test_mint_loader_excludes_declined_verdicts(session) -> None:
    """`load_surrogate_key_intents` feeds ONLY confirmed composites to the mint."""
    from dataraum.analysis.relationships.utils import load_surrogate_key_intents

    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    candidate = _hint_candidate(
        "txn", "coa", [["account", "account_name"], ["business_id", "business_id"]]
    )
    assert _store(session, _agent([]), [txn, coa], candidates=[candidate]).success
    session.flush()

    assert session.execute(select(SurrogateKeyIntent)).scalars().all() != []
    assert load_surrogate_key_intents(session, baseline_run_id()) == []


def test_unbuildable_confirmation_still_records_the_declined_verdict(session) -> None:
    """The LLM confirms the offered composite but names a nonexistent component —
    the intent build fails and falls back to the single-column anchor. This run
    still produced no usable composite, so the verdict arithmetic records the
    OFFERED hint as declined: the hint re-offers deterministically next run,
    and the keeper machinery must not resurrect the pair in the meantime."""
    txn = _table_with_columns(session, "txn", ["account", "business_id"])
    coa = _table_with_columns(session, "coa", ["account_name", "business_id"])
    candidate = _hint_candidate(
        "txn", "coa", [["account", "account_name"], ["business_id", "business_id"]]
    )
    agent = _agent([_rel(key_columns=[("ghost", "ghost")])])  # confirmation that can't build

    assert _store(session, agent, [txn, coa], candidates=[candidate]).success
    session.flush()

    intents = session.execute(select(SurrogateKeyIntent)).scalars().all()
    assert [i.status for i in intents] == ["declined"]
    llm_rows = (
        session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
        .scalars()
        .all()
    )
    assert len(llm_rows) == 1  # the anchor fallback persists with honest cardinality
