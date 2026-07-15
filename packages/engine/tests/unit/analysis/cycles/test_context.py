"""Fail-closed run isolation for the cycle-detection context (DAT-429/455).

``build_cycle_detection_context`` assembles two run-versioned reads — entity
classifications and the defined relationships — both of which coexist across runs
(DAT-408/413). The builder is an in-run reader (ADR-0008): it scopes by the
:class:`BaseRunMap` pinned once at run start and passed in, never resolving a head
itself. With no pinned run (``relationship_run_id is None``) it must surface
NEITHER: a cross-run read here would mix other runs' entities/relationships
into this context. These pin that contract, mirroring ``graphs/test_context_builder``
for the cycles reader.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import pytest

from dataraum.analysis.correlation.db_models import DerivedColumn
from dataraum.analysis.cycles.context import (
    build_cycle_detection_context,
    format_context_for_prompt,
)
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.lifecycle import BaseRunMap
from dataraum.storage import Column, Source, Table


def _id() -> str:
    return str(uuid4())


@pytest.fixture
def two_tables_two_runs(session):
    """Two related tables with entity + relationship rows under two coexisting runs.

    ``run-current`` and ``run-stale`` each carry a fact classification for the
    transactions table and the same directional relationship (distinguishable by
    confidence). No head is promoted here — each test promotes the one it needs.

    Returns ``table_ids``.
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

    for run_id, conf, is_fact, desc, grain in (
        ("run-current", 0.95, True, "CURRENT classification", ["account_id", "period"]),
        ("run-stale", 0.10, False, "STALE classification", ["stale_id"]),
    ):
        session.add(
            Relationship(
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
                table_id=txn.table_id,
                run_id=run_id,
                detected_entity_type="fact" if is_fact else "dimension",
                description=desc,
                table_role="fact" if is_fact else "dimension",
                grain_columns=grain,
            )
        )
    session.commit()

    return [txn.table_id, acct.table_id]


def _build(session, table_ids, *, base_runs: BaseRunMap, **kwargs):
    """Build the cycle context against an ephemeral DuckDB (row counts → None)."""
    return build_cycle_detection_context(
        session,
        duckdb.connect(),
        table_ids,
        vertical="finance",
        base_runs=base_runs,
        **kwargs,
    )


def test_unpinned_run_reads_no_run_versioned_data(session, two_tables_two_runs) -> None:
    """No pinned run ⇒ no entities, no relationships — never the cross-run union."""
    table_ids = two_tables_two_runs

    # An empty base-run map (relationship_run_id is None) is the unresolved case
    # — the operating_model resolve activity pins nothing when begin_session has
    # no promoted run. The read is empty.
    ctx_none = _build(session, table_ids, base_runs=BaseRunMap())
    assert ctx_none["entity_classifications"] == []
    assert ctx_none["relationships"] == []


def test_scopes_to_pinned_run(session, two_tables_two_runs) -> None:
    """With a pinned relationship run, only that run's entity + relationship surface."""
    table_ids = two_tables_two_runs

    ctx = _build(
        session,
        table_ids,
        base_runs=BaseRunMap(relationship_run_id="run-current"),
    )

    rels = ctx["relationships"]
    assert len(rels) == 1
    assert rels[0]["confidence"] == 0.95

    entities = ctx["entity_classifications"]
    assert len(entities) == 1
    assert entities[0]["table_role"] == "fact"
    assert entities[0]["description"] == "CURRENT classification"
    # DAT-775: a bare list of column names, never a {"columns": [...]} wrapper —
    # format_context_for_prompt joins this straight into the LLM prompt.
    assert entities[0]["grain_columns"] == ["account_id", "period"]


def test_format_context_for_prompt_renders_grain_column_names() -> None:
    """DAT-775 regression: the cycle-detection prompt renders the table's ACTUAL
    grain columns, never the literal string "columns" — the symptom of the fixed
    bug, where a persisted ``{"columns": [...]}`` wrapper had its sole dict key
    joined into the prompt instead of the real grain."""
    context = {
        "tables": [{"table_name": "accounts", "row_count": 50, "columns": []}],
        "entity_classifications": [
            {
                "table_name": "accounts",
                "entity_type": "account",
                "description": "Chart of accounts.",
                "table_role": "dimension",
                "grain_columns": ["account_id", "period"],
            }
        ],
    }

    rendered = format_context_for_prompt(context)

    assert "grain: account_id, period" in rendered
    assert "grain: columns" not in rendered


@pytest.fixture
def ledger_with_derivations(session):
    """A ledger table with a debit/credit/net triple + derivation rows.

    Under ``run-current``: a ``difference`` derivation (net = debit − credit) at
    98% and a ``upper`` string transform. Under ``run-stale``: the same
    difference at 10%. Returns ``table_ids``.
    """
    source = Source(name="ledger_source", source_type="csv")
    session.add(source)
    session.flush()

    ledger = Table(
        source_id=source.source_id,
        table_name="journal",
        layer="typed",
        row_count=1000,
        duckdb_path="typed_journal",
    )
    session.add(ledger)
    session.flush()

    debit = Column(
        table_id=ledger.table_id, column_name="debit", column_position=0, raw_type="DECIMAL"
    )
    credit = Column(
        table_id=ledger.table_id, column_name="credit", column_position=1, raw_type="DECIMAL"
    )
    net = Column(table_id=ledger.table_id, column_name="net", column_position=2, raw_type="DECIMAL")
    name = Column(
        table_id=ledger.table_id, column_name="name", column_position=3, raw_type="VARCHAR"
    )
    name_up = Column(
        table_id=ledger.table_id, column_name="name_upper", column_position=4, raw_type="VARCHAR"
    )
    session.add_all([debit, credit, net, name, name_up])
    session.flush()

    def _derived(run_id, derived_col, sources, dtype, formula, rate):
        return DerivedColumn(
            run_id=run_id,
            table_id=ledger.table_id,
            derived_column_id=derived_col.column_id,
            source_column_ids=[c.column_id for c in sources],
            derivation_type=dtype,
            formula=formula,
            match_rate=rate,
            total_rows=1000,
            matching_rows=int(1000 * rate),
        )

    session.add_all(
        [
            _derived("run-current", net, [debit, credit], "difference", "debit - credit", 0.98),
            _derived("run-current", name_up, [name], "upper", "UPPER(name)", 1.0),
            _derived("run-stale", net, [debit, credit], "difference", "debit - credit", 0.10),
        ]
    )
    session.commit()
    return [ledger.table_id]


def test_derived_relationships_scoped_and_arithmetic_only(session, ledger_with_derivations) -> None:
    """Only the pinned run's ARITHMETIC derivations surface — string ops excluded."""
    ctx = _build(
        session,
        ledger_with_derivations,
        base_runs=BaseRunMap(relationship_run_id="run-current"),
    )

    derived = ctx["derived_relationships"]
    assert len(derived) == 1  # the difference; the upper transform and the stale row are out
    dr = derived[0]
    assert dr["derivation_type"] == "difference"
    assert dr["match_rate"] == 0.98
    assert dr["derived_column"] == "net"
    assert sorted(dr["source_columns"]) == ["credit", "debit"]


def test_derived_relationships_fail_closed_when_unpinned(session, ledger_with_derivations) -> None:
    """No pinned run ⇒ no derived relationships — never a cross-run read."""
    ctx = _build(session, ledger_with_derivations, base_runs=BaseRunMap())
    assert ctx["derived_relationships"] == []


@pytest.fixture
def sliced_status_column(session):
    """A status column with a slice (under the catalogue run) + a typed profile
    (under the generation run). Returns ``(table_id, catalogue_run, gen_run)``."""
    source = Source(name="status_source", source_type="csv")
    session.add(source)
    session.flush()

    tbl = Table(
        source_id=source.source_id,
        table_name="invoices",
        layer="typed",
        row_count=100,
        duckdb_path="typed_invoices",
    )
    session.add(tbl)
    session.flush()

    col = Column(table_id=tbl.table_id, column_name="status", column_position=0, raw_type="VARCHAR")
    session.add(col)
    session.flush()

    session.add(
        SliceDefinition(
            run_id="cat",
            table_id=tbl.table_id,
            column_id=col.column_id,
            column_name="status",
            slice_priority=1,
            distinct_values=["paid", "open"],
        )
    )
    session.add(
        StatisticalProfile(
            column_id=col.column_id,
            run_id="gen",
            layer="typed",
            total_count=100,
            null_count=0,
            profile_data={
                "top_values": [
                    {"value": "paid", "count": 80, "percentage": 80.0},
                    {"value": "open", "count": 20, "percentage": 20.0},
                ]
            },
        )
    )
    session.commit()
    return tbl.table_id, "cat", "gen"


def test_value_counts_scoped_to_generation_run(session, sliced_status_column) -> None:
    """Value counts read at the table's pinned generation head, not an arbitrary run."""
    table_id, cat, gen = sliced_status_column
    ctx = _build(
        session,
        [table_id],
        base_runs=BaseRunMap(relationship_run_id=cat, semantic_runs={table_id: gen}),
    )
    slices = ctx["slice_definitions"]
    assert len(slices) == 1
    values = {vc["value"] for vc in slices[0]["value_counts"]}
    assert values == {"paid", "open"}


def test_value_counts_fail_closed_without_generation_pin(session, sliced_status_column) -> None:
    """No pinned generation run for the table ⇒ no value counts (never an arbitrary run)."""
    table_id, cat, _ = sliced_status_column
    ctx = _build(
        session,
        [table_id],
        base_runs=BaseRunMap(relationship_run_id=cat, semantic_runs={}),
    )
    slices = ctx["slice_definitions"]
    assert len(slices) == 1
    assert slices[0]["value_counts"] == []
