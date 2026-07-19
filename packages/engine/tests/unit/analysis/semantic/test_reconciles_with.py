"""Derived reconciles_with self-loops — witness + multi-grounding (DAT-727 part c).

Pins the concept-grain derivation: a healthy grounding set plus the pinned-run
aggregation-lineage witnesses reconcile to ``source='derived'`` self-loop rows
in ``concept_edges`` — insert-if-absent, supersede-on-vanished-support, seed
rows untouched. The witness's relation→column resolution mirrors ``og_uses``
(enriched view's served columns via ``source_column_id``; typed table direct);
the PGQ binding of a self-loop is exercised in
``tests/integration/storage/test_property_graph.py``.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.semantic.db_models import Concept, ConceptEdge
from dataraum.analysis.semantic.reconciles_with import derive_reconciles_with
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.query.snippet_models import SQLSnippetRecord
from dataraum.storage.models import Column, Table
from dataraum.storage.snapshot_head import MetadataSnapshotHead

VERTICAL = "finance"
CAT_RUN = "cat-run-1"
_TEST_SOURCE_ID = "00000000-0000-0000-0000-000000000002"  # conftest baseline Source


def _concept(session: Session, name: str) -> None:
    session.add(Concept(concept_id=f"con_{name}", vertical=VERTICAL, name=name, kind="measure"))


def _snippet(
    session: Session,
    sid: str,
    field: str,
    statement: str,
    relation: str | None,
    *,
    measure_columns: list[str] | None = None,
    failed: bool = False,
    source: str | None = None,
    v1_basis: bool = False,
) -> None:
    parts = (
        {
            "select": [{"expr": "SUM(amount)", "alias": "value"}],
            "from": [relation],
            "where": [],
        }
        if relation
        else None
    )
    if v1_basis:
        basis: dict = {field: {"column": "amount"}}
    else:
        basis = {field: {"measure_columns": measure_columns or [], "filter_columns": []}}
    session.add(
        SQLSnippetRecord(
            snippet_id=sid,
            workspace_id="test",
            snippet_type="extract",
            standard_field=field,
            statement=statement,
            aggregation="sum",
            schema_mapping_id="test",
            sql="SELECT SUM(amount) AS value",
            description="d",
            source=source or f"graph:{field}",
            provenance={"column_mappings_basis": basis, "assumptions": []},
            parts=parts,
            failure_count=1 if failed else 0,
        )
    )


def _typed_table(session: Session, tid: str, name: str) -> None:
    session.add(Table(table_id=tid, source_id=_TEST_SOURCE_ID, table_name=name, layer="typed"))


def _promote(session: Session, tid: str) -> None:
    """A promoted (table:{id}, generation) head — the typed-branch scoping gate."""
    session.add(MetadataSnapshotHead(target=f"table:{tid}", stage="generation", run_id=CAT_RUN))


def _column(
    session: Session, cid: str, tid: str, name: str, *, source_column_id: str | None = None
) -> None:
    session.add(
        Column(
            column_id=cid,
            table_id=tid,
            column_name=name,
            column_position=0,
            origin="fact" if source_column_id else None,
            source_column_id=source_column_id,
        )
    )


def _witness(session: Session, measure_column_id: str, tid: str, *, run_id: str = CAT_RUN) -> None:
    # Flush first: the plain-FK dependency (mal → tables/columns) is not a
    # relationship(), so the unit of work won't order the inserts for us.
    session.flush()
    session.add(
        MeasureAggregationLineage(
            run_id=run_id,
            measure_table_id=tid,
            measure_column_id=measure_column_id,
            event_table_id=tid,
            measure_time_axis_column="period",
            event_time_axis_column="period",
            measure_slice_column_id=measure_column_id,
            event_slice_column_id=measure_column_id,
            slice_dimension="account",
            convention_sql="SUM(amount)",
            period_grain="month",
            pattern="per_period",
            match_rate=1.0,
            r_flow_median=0.9,
            r_stock_median=0.1,
            n_entities=10,
            n_entities_fired=10,
        )
    )


def _derived_loops(session: Session) -> set[str]:
    return {
        e.from_concept
        for e in session.execute(
            select(ConceptEdge).where(
                ConceptEdge.source == "derived", ConceptEdge.superseded_at.is_(None)
            )
        ).scalars()
        if e.from_concept == e.to_concept
    }


def test_multi_grounding_concept_gets_a_self_loop(session: Session) -> None:
    _concept(session, "account_balance")
    _snippet(session, "s1", "account_balance", "trial_balance", "enriched_journal")
    _snippet(session, "s2", "account_balance", "balance_sheet", "enriched_journal")
    session.flush()

    inserted, superseded = derive_reconciles_with(
        session, vertical=VERTICAL, catalogue_run_id=CAT_RUN
    )

    assert (inserted, superseded) == (1, 0)
    edge = session.execute(select(ConceptEdge).where(ConceptEdge.source == "derived")).scalar_one()
    assert edge.from_concept == edge.to_concept == "account_balance"
    assert edge.predicate == "reconciles_with"
    assert edge.vertical == VERTICAL
    assert edge.tolerance is None  # no vertical declares a per-concept band


def test_no_active_concept_no_row(session: Session) -> None:
    """Two groundings of a field naming no active Concept assert nothing —
    the edge endpoints are concept names and the graph must never dangle."""
    _snippet(session, "s1", "expenses", "income_statement", "enriched_journal")
    _snippet(session, "s2", "expenses", "cash_flow", "enriched_journal")
    session.flush()

    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (0, 0)


def test_failed_and_query_sourced_groundings_do_not_count(session: Session) -> None:
    """One healthy + one retained-failure + one query-sourced row = ONE
    grounding — failure retention and the cockpit's rows must never read as
    multi-grounding."""
    _concept(session, "revenue")
    _snippet(session, "s1", "revenue", "income_statement", "enriched_journal")
    _snippet(session, "s2", "revenue", "balance_sheet", "enriched_journal", failed=True)
    _snippet(session, "s3", "revenue", "cash_flow", "enriched_journal", source="query:exec_9")
    session.flush()

    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (0, 0)


def test_witness_resolves_through_the_enriched_views_served_column(session: Session) -> None:
    """The og_uses-mirror resolution: the grounding reads the SERVED 'amount'
    (an enriched f.* passthrough) whose source_column_id is the witnessed
    typed measure column."""
    _concept(session, "account_balance")
    _typed_table(session, "t_fact", "journal")
    _typed_table(session, "t_enr", "enriched_journal")
    _column(session, "c_amt", "t_fact", "amount")
    _column(session, "ec_amt", "t_enr", "amount", source_column_id="c_amt")
    session.add(
        EnrichedView(
            view_id="v1",
            fact_table_id="t_fact",
            view_table_id="t_enr",
            view_name="enriched_journal",
            run_id=CAT_RUN,
        )
    )
    _snippet(
        session,
        "s1",
        "account_balance",
        "trial_balance",
        "enriched_journal",
        measure_columns=["amount"],
    )
    _witness(session, "c_amt", "t_fact")
    session.flush()

    inserted, _ = derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN)

    assert inserted == 1
    assert _derived_loops(session) == {"account_balance"}


def test_witness_resolves_a_typed_relation_directly(session: Session) -> None:
    _concept(session, "revenue")
    _typed_table(session, "t_fact", "journal")
    _promote(session, "t_fact")
    _column(session, "c_amt", "t_fact", "amount")
    _snippet(session, "s1", "revenue", "income_statement", "journal", measure_columns=["amount"])
    _witness(session, "c_amt", "t_fact")
    session.flush()

    inserted, _ = derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN)

    assert inserted == 1
    assert _derived_loops(session) == {"revenue"}


def test_unpromoted_typed_table_does_not_witness(session: Session) -> None:
    """The current_tables scoping mirrored in ORM: a typed table with NO
    promoted generation head must not resolve the grounding's relation."""
    _concept(session, "revenue")
    _typed_table(session, "t_fact", "journal")  # no _promote
    _column(session, "c_amt", "t_fact", "amount")
    _snippet(session, "s1", "revenue", "income_statement", "journal", measure_columns=["amount"])
    _witness(session, "c_amt", "t_fact")
    session.flush()

    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (0, 0)


def test_witness_at_another_run_or_v1_basis_asserts_nothing(session: Session) -> None:
    """Run-pinning + the clean contract cut: an unpinned-run witness is not
    THIS session's evidence, and a pre-v2 basis carries no arrays to resolve."""
    _concept(session, "revenue")
    _typed_table(session, "t_fact", "journal")
    _promote(session, "t_fact")
    _column(session, "c_amt", "t_fact", "amount")
    _snippet(session, "s1", "revenue", "income_statement", "journal", v1_basis=True)
    _witness(session, "c_amt", "t_fact", run_id="other-run")
    session.flush()

    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (0, 0)

    # Even with the witness pinned right, the v1 basis resolves to nothing.
    _witness(session, "c_amt", "t_fact")
    session.flush()
    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (0, 0)


def test_rerun_is_idempotent(session: Session) -> None:
    _concept(session, "account_balance")
    _snippet(session, "s1", "account_balance", "trial_balance", "enriched_journal")
    _snippet(session, "s2", "account_balance", "balance_sheet", "enriched_journal")
    session.flush()

    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (1, 0)
    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (0, 0)
    assert _derived_loops(session) == {"account_balance"}


def test_vanished_support_supersedes_derived_but_never_seed(session: Session) -> None:
    """The lifecycle half: a decayed grounding retires the derived assertion;
    a seeded (declared) reconciles_with row is NEVER touched."""
    _concept(session, "account_balance")
    _snippet(session, "s1", "account_balance", "trial_balance", "enriched_journal")
    _snippet(session, "s2", "account_balance", "balance_sheet", "enriched_journal")
    # A declared cross-concept reconciliation from the seed — out of scope.
    session.add(
        ConceptEdge(
            vertical=VERTICAL,
            predicate="reconciles_with",
            from_concept="trial_balance_total",
            to_concept="general_ledger_total",
            source="seed",
            tolerance=0.01,
        )
    )
    session.flush()
    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (1, 0)

    # One grounding decays to a retained failure → support gone.
    s2 = session.execute(
        select(SQLSnippetRecord).where(SQLSnippetRecord.snippet_id == "s2")
    ).scalar_one()
    s2.failure_count = 1
    session.flush()

    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (0, 1)
    assert _derived_loops(session) == set()
    seed = session.execute(select(ConceptEdge).where(ConceptEdge.source == "seed")).scalar_one()
    assert seed.superseded_at is None
    assert seed.tolerance == 0.01

    # Support returns (the snippet heals) → a fresh active row is inserted.
    s2.failure_count = 0
    session.flush()
    assert derive_reconciles_with(session, vertical=VERTICAL, catalogue_run_id=CAT_RUN) == (1, 0)
    assert _derived_loops(session) == {"account_balance"}
    # The superseded row remains as history alongside the new active one.
    all_derived = list(
        session.execute(select(ConceptEdge).where(ConceptEdge.source == "derived")).scalars()
    )
    assert len(all_derived) == 2
    assert sum(1 for e in all_derived if e.superseded_at is None) == 1
