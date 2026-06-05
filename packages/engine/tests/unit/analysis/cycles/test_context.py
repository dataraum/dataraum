"""Fail-closed session isolation for the cycle-detection context (DAT-429).

``build_cycle_detection_context`` assembles two run-versioned reads — entity
classifications and the defined relationships — both of which coexist across runs
(DAT-408/413). With no resolved run (no ``session_id``, or a session whose head was
never promoted) the builder must surface NEITHER: a cross-run read here would mix
other sessions' entities/relationships into this context. These pin that contract,
mirroring ``graphs/test_context_builder`` for the cycles reader.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import pytest

from dataraum.analysis.cycles.context import build_cycle_detection_context
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.investigation.db_models import InvestigationSession
from dataraum.storage import Column, Source, Table
from dataraum.storage.snapshot_head import MetadataSnapshotHead, session_head_target


def _id() -> str:
    return str(uuid4())


@pytest.fixture
def two_tables_two_runs(session):
    """Two related tables with entity + relationship rows under two coexisting runs.

    ``run-current`` and ``run-stale`` each carry a fact classification for the
    transactions table and the same directional relationship (distinguishable by
    confidence). No head is promoted here — each test promotes the one it needs.

    Returns ``(table_ids, session_id)``.
    """
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    txn = Table(
        source_id=source.source_id,
        table_name="transactions",
        layer="typed",
        row_count=1000,
        duckdb_path="typed_transactions",
    )
    acct = Table(
        source_id=source.source_id,
        table_name="accounts",
        layer="typed",
        row_count=50,
        duckdb_path="typed_accounts",
    )
    session.add_all([txn, acct])
    session.flush()

    txn_account_col = Column(
        table_id=txn.table_id,
        column_name="account_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    acct_id_col = Column(
        table_id=acct.table_id,
        column_name="account_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    session.add_all([txn_account_col, acct_id_col])
    session.flush()

    session_id = "sess-cycles"
    session.add(InvestigationSession(session_id=session_id, intent="test"))
    session.flush()
    for run_id, conf, is_fact, desc in (
        ("run-current", 0.95, True, "CURRENT classification"),
        ("run-stale", 0.10, False, "STALE classification"),
    ):
        session.add(
            Relationship(
                session_id=session_id,
                run_id=run_id,
                from_table_id=txn.table_id,
                from_column_id=txn_account_col.column_id,
                to_table_id=acct.table_id,
                to_column_id=acct_id_col.column_id,
                relationship_type="foreign_key",
                cardinality="many-to-one",
                confidence=conf,
                detection_method="llm",
            )
        )
        session.add(
            TableEntity(
                entity_id=_id(),
                session_id=session_id,
                table_id=txn.table_id,
                run_id=run_id,
                detected_entity_type="fact" if is_fact else "dimension",
                description=desc,
                is_fact_table=is_fact,
            )
        )
    session.commit()

    return [txn.table_id, acct.table_id], session_id


def _build(session, table_ids, **kwargs):
    """Build the cycle context against an ephemeral DuckDB (row counts → None)."""
    return build_cycle_detection_context(
        session,
        duckdb.connect(),
        table_ids,
        vertical="finance",
        **kwargs,
    )


def test_unresolved_session_reads_no_run_versioned_data(session, two_tables_two_runs) -> None:
    """No resolved run ⇒ no entities, no relationships — never the cross-run union."""
    table_ids, session_id = two_tables_two_runs

    # No session_id ⇒ unresolved run.
    ctx_none = _build(session, table_ids)
    assert ctx_none["entity_classifications"] == []
    assert ctx_none["relationships"] == []

    # A recognized session whose head was never promoted is equally unresolved
    # (``session_id`` is seeded as an InvestigationSession by the fixture, but no
    # detect head was ever promoted for it).
    ctx_unpromoted = _build(session, table_ids, session_id=session_id)
    assert ctx_unpromoted["entity_classifications"] == []
    assert ctx_unpromoted["relationships"] == []


def test_scopes_to_promoted_head(session, two_tables_two_runs) -> None:
    """With a promoted head, only that run's entity + relationship surface."""
    table_ids, session_id = two_tables_two_runs
    session.add(
        MetadataSnapshotHead(
            target=session_head_target(session_id), stage="detect", run_id="run-current"
        )
    )
    session.commit()

    ctx = _build(session, table_ids, session_id=session_id)

    rels = ctx["relationships"]
    assert len(rels) == 1
    assert rels[0]["confidence"] == 0.95

    entities = ctx["entity_classifications"]
    assert len(entities) == 1
    assert entities[0]["is_fact_table"] is True
    assert entities[0]["description"] == "CURRENT classification"
