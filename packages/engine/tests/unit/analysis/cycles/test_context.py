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
from sqlalchemy import select

from dataraum.analysis.correlation.db_models import DerivedColumn
from dataraum.analysis.cycles.context import (
    build_cycle_detection_context,
    format_context_for_prompt,
)
from dataraum.analysis.cycles.cycle_family_store import ensure_cycle_families_seeded
from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import SemanticAnnotation, TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.lifecycle import BaseRunMap
from dataraum.llm.config import LLMPrivacy
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


def test_cycle_families_served_and_threaded(session) -> None:
    """The seeded family declaration (DAT-856) reaches the judge as DOMAIN KNOWLEDGE
    data AND is threaded into the context for the save-time direction resolution."""
    ensure_cycle_families_seeded(session, "finance")
    ctx = _build(session, [], base_runs=BaseRunMap())
    # Threaded for _parse_output's resolve_cycle_identity.
    assert ctx["cycle_families"] == {
        "settlement": {"incoming": "accounts_receivable", "outgoing": "accounts_payable"}
    }
    # Served as data (the family + member names come from the declaration, never
    # hardcoded in the generic prompt — the tripwire pins that boundary).
    vocab = ctx["domain_vocabulary"]
    assert "CYCLE FAMILIES" in vocab
    assert "settlement" in vocab
    assert "incoming → accounts_receivable" in vocab


def test_no_families_leaves_domain_vocabulary_unchanged(session) -> None:
    """A vertical with no declared families serves no family block (empty ⇒ omitted)."""
    ctx = _build(session, [], base_runs=BaseRunMap())
    assert ctx["cycle_families"] == {}
    assert "CYCLE FAMILIES" not in ctx["domain_vocabulary"]


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


def test_conformed_meetings_split_out_of_the_reference_serve(session, two_tables_two_runs) -> None:
    """DAT-850: a 'conformed_dimension' row is not served as a reference.

    It leaves the relationships list (no entity flow rides a shared axis, and
    the graph topology below consumes that list) and lands in the
    explicitly-labelled conformed_meetings block the prompt renders — loud
    typing, never a silent drop.
    """
    from dataraum.analysis.cycles.context import format_context_for_prompt

    table_ids = two_tables_two_runs
    # A shared-axis column on each table — the shape a meeting actually links.
    region_cols = [
        Column(
            table_id=tid,
            column_name="region",
            column_position=5,
            raw_type="VARCHAR",
            resolved_type="VARCHAR",
        )
        for tid in table_ids
    ]
    session.add_all(region_cols)
    session.flush()
    session.add(
        Relationship(
            run_id="run-current",
            from_table_id=table_ids[0],
            from_column_id=region_cols[0].column_id,
            to_table_id=table_ids[1],
            to_column_id=region_cols[1].column_id,
            relationship_type="conformed_dimension",
            cardinality="many-to-many",
            confidence=0.9,
            detection_method="llm",
            evidence={"resolved_from_type": "foreign_key"},
        )
    )
    session.commit()

    ctx = _build(
        session,
        table_ids,
        base_runs=BaseRunMap(relationship_run_id="run-current"),
    )

    assert [r["relationship_type"] for r in ctx["relationships"]] == ["foreign_key"]
    assert [r["relationship_type"] for r in ctx["conformed_meetings"]] == ["conformed_dimension"]
    assert ctx["summary"]["conformed_meetings_found"] == 1

    prompt = format_context_for_prompt(ctx)
    assert "CONFORMED DIMENSION MEETINGS" in prompt
    assert "NOT references" in prompt


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


def test_format_context_for_prompt_renders_uncomputable_completeness() -> None:
    """DAT-810 regression: a temporal profile whose grain is irregular/unknown carries
    ``completeness=None`` — no bucket exists, so the ratio is not computable and the
    three fields fall loud together. Rendering it with ``:.0%`` raised ``TypeError:
    unsupported format string passed to NoneType.__format__``, crashing the whole
    business-cycles phase (``format_context_for_prompt`` is called unguarded from
    ``cycles/agent.py`` → ``business_cycles_phase.py``). The absence must render as
    absence, never as a number."""
    context = {
        "tables": [{"table_name": "events", "row_count": 10, "columns": []}],
        "temporal_profiles": [
            {
                "table_name": "events",
                "column_name": "occurred_at",
                "granularity": "irregular",
                "date_range_start": "2025-01-01",
                "date_range_end": "2026-02-11",
                "completeness": None,
                "is_stale": False,
            }
        ],
    }

    rendered = format_context_for_prompt(context)

    assert "completeness=not computable (no grain)" in rendered
    assert "completeness=0%" not in rendered
    assert "completeness=100%" not in rendered


def test_format_context_for_prompt_renders_known_completeness_as_percent() -> None:
    """The computable case is unchanged — a real grain still renders its ratio."""
    context = {
        "tables": [{"table_name": "events", "row_count": 10, "columns": []}],
        "temporal_profiles": [
            {
                "table_name": "events",
                "column_name": "occurred_at",
                "granularity": "day",
                "date_range_start": "2025-01-01",
                "date_range_end": "2025-01-31",
                "completeness": 0.909,
                "is_stale": True,
            }
        ],
    }

    rendered = format_context_for_prompt(context)

    assert "completeness=91% [STALE]" in rendered


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


def test_curated_slice_budget_and_priority_order(session) -> None:
    """DAT-725: the catalog is the full deterministic inventory, so this LLM-facing
    context reads only the top-priority budget, ascending (1 = most interesting),
    with a deterministic column_name tiebreak across floor-priority rows."""
    from dataraum.analysis.slicing.models import CURATED_SLICE_BUDGET, UNRANKED_SLICE_PRIORITY

    source = Source(name="s", source_type="csv")
    session.add(source)
    session.flush()
    tbl = Table(
        source_id=source.source_id,
        table_name="facts",
        layer="typed",
        row_count=100,
        duckdb_path="typed_facts",
    )
    session.add(tbl)
    session.flush()

    n_total = CURATED_SLICE_BUDGET + 3
    for i in range(n_total):
        col = Column(
            table_id=tbl.table_id,
            column_name=f"dim_{i:02d}",
            column_position=i,
            raw_type="VARCHAR",
        )
        session.add(col)
        session.flush()
        # Two ranked rows (priorities 1 and 2), the rest structural at the floor.
        priority = i + 1 if i < 2 else UNRANKED_SLICE_PRIORITY
        session.add(
            SliceDefinition(
                run_id="cat",
                table_id=tbl.table_id,
                column_id=col.column_id,
                column_name=f"dim_{i:02d}",
                slice_priority=priority,
                distinct_values=["a", "b"],
                detection_source="llm" if i < 2 else "structural",
            )
        )
    session.commit()

    ctx = _build(
        session,
        [tbl.table_id],
        base_runs=BaseRunMap(relationship_run_id="cat", semantic_runs={}),
    )
    slices = ctx["slice_definitions"]
    assert len(slices) == CURATED_SLICE_BUDGET, "inventory is complete; context is curated"
    priorities = [s["priority"] for s in slices]
    assert priorities == sorted(priorities), "ascending — 1 = most interesting first"
    assert slices[0]["column_name"] == "dim_00"
    # The floor rows fill the remaining budget in deterministic name order.
    floor_names = [s["column_name"] for s in slices if s["priority"] == UNRANKED_SLICE_PRIORITY]
    assert floor_names == sorted(floor_names)


# ---------------------------------------------------------------------------
# Entity-flow evidence (DAT-725): value samples + annotation confidence for the
# columns cycles' entity flows ride on — identity columns and confirmed-
# relationship endpoints. The gate is structural (served metadata only); the
# reads are run-pinned and fail-closed like every other run-versioned read here.
# ---------------------------------------------------------------------------


@pytest.fixture
def identity_column_with_samples(session):
    """A fact table with an identity column, a hedged annotation, and profiles.

    ``zq_p4x`` (an unreadably named identity column) carries a low-confidence
    annotation + typed top_values under the generation run; ``amount`` is a
    plain measure whose profile must NOT be served as samples; ``contact_email``
    is an identity column with a privacy-sensitive name. Returns
    ``(table_id, catalogue_run, gen_run)``.
    """
    source = Source(name="flow_source", source_type="csv")
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

    id_col = Column(
        table_id=tbl.table_id, column_name="zq_p4x", column_position=0, raw_type="VARCHAR"
    )
    measure_col = Column(
        table_id=tbl.table_id, column_name="amount", column_position=1, raw_type="DECIMAL"
    )
    email_col = Column(
        table_id=tbl.table_id, column_name="contact_email", column_position=2, raw_type="VARCHAR"
    )
    session.add_all([id_col, measure_col, email_col])
    session.flush()

    session.add(
        TableEntity(
            entity_id=_id(),
            table_id=tbl.table_id,
            run_id="cat",
            detected_entity_type="invoice",
            description="Invoice rows.",
            table_role="fact",
            grain_columns=["invoice_id"],
            identity_columns=[
                {"column": "zq_p4x", "note": "recurring counterparty identifier"},
                {"column": "contact_email", "note": "contact address"},
                # A hallucinated name + a malformed entry: the builder must
                # existence-filter BOTH before serving — a served-but-nonexistent
                # name would pass the prompt's cite-only-served contract and then
                # be silently dropped by the membership floor.
                {"column": "ghost_col", "note": "does not exist"},
                "not-a-dict",
            ],
        )
    )

    session.add(
        SemanticAnnotation(
            column_id=id_col.column_id,
            run_id="gen",
            semantic_role="dimension",
            entity_type="entity_identifier",
            business_name="Entity Code",
            business_description="the entity associated with the row",
            confidence=0.25,
        )
    )

    for col, values in (
        (id_col, ["E-0002", "E-0003"]),
        (measure_col, ["10.5"]),
        (email_col, ["a@example.com"]),
    ):
        session.add(
            StatisticalProfile(
                column_id=col.column_id,
                run_id="gen",
                layer="typed",
                total_count=100,
                null_count=0,
                profile_data={
                    "top_values": [{"value": v, "count": 5, "percentage": 5.0} for v in values]
                },
            )
        )
    session.commit()
    return tbl.table_id, "cat", "gen"


def test_identity_columns_serve_samples_and_annotation_confidence(
    session, identity_column_with_samples
) -> None:
    """An identity column carries its value samples + the annotation's confidence;
    a plain measure column (profiled, but not identity/endpoint) carries neither."""
    table_id, cat, gen = identity_column_with_samples
    ctx = _build(
        session,
        [table_id],
        base_runs=BaseRunMap(relationship_run_id=cat, semantic_runs={table_id: gen}),
    )

    cols = {c["name"]: c for c in ctx["tables"][0]["columns"]}
    assert cols["zq_p4x"]["sample_values"] == ["E-0002", "E-0003"]
    assert cols["zq_p4x"]["annotation_confidence"] == 0.25
    assert "sample_values" not in cols["amount"]

    # The identity columns ride along on the classification (rendered with
    # notes) — existence-filtered: the fixture's hallucinated "ghost_col" and
    # its malformed non-dict entry must NOT be served.
    ents = ctx["entity_classifications"]
    assert ents[0]["identity_columns"] == [
        {"column": "zq_p4x", "note": "recurring counterparty identifier"},
        {"column": "contact_email", "note": "contact address"},
    ]


def test_identity_samples_fail_closed_without_generation_pin(
    session, identity_column_with_samples
) -> None:
    """No pinned generation run for the table ⇒ no samples, no annotation — never
    an arbitrary run's profile."""
    table_id, cat, _ = identity_column_with_samples
    ctx = _build(
        session,
        [table_id],
        base_runs=BaseRunMap(relationship_run_id=cat, semantic_runs={}),
    )
    cols = {c["name"]: c for c in ctx["tables"][0]["columns"]}
    assert "sample_values" not in cols["zq_p4x"]
    assert "annotation_confidence" not in cols["zq_p4x"]


def test_sensitive_identity_column_serves_no_samples(session, identity_column_with_samples) -> None:
    """A privacy-sensitive name serves NO samples (absence, not a placeholder) —
    the same pattern gate the semantic agents' DataSampler enforces."""
    table_id, cat, gen = identity_column_with_samples
    ctx = _build(
        session,
        [table_id],
        base_runs=BaseRunMap(relationship_run_id=cat, semantic_runs={table_id: gen}),
        privacy=LLMPrivacy(sensitive_patterns=[".*email.*"]),
    )
    cols = {c["name"]: c for c in ctx["tables"][0]["columns"]}
    assert "sample_values" not in cols["contact_email"]
    # The non-sensitive identity column still serves its samples.
    assert cols["zq_p4x"]["sample_values"] == ["E-0002", "E-0003"]


def test_relationship_endpoint_columns_serve_samples(session, two_tables_two_runs) -> None:
    """Confirmed-relationship endpoints are entity-flow candidates too — both
    sides of the pinned run's relationship carry samples when profiled."""
    table_ids = two_tables_two_runs
    cols = list(
        session.execute(select(Column).where(Column.table_id.in_(table_ids))).scalars().all()
    )
    for c in cols:
        session.add(
            StatisticalProfile(
                column_id=c.column_id,
                run_id="gen",
                layer="typed",
                total_count=10,
                null_count=0,
                profile_data={"top_values": [{"value": "A-1", "count": 5, "percentage": 50.0}]},
            )
        )
    session.commit()

    ctx = _build(
        session,
        table_ids,
        base_runs=BaseRunMap(
            relationship_run_id="run-current",
            semantic_runs=dict.fromkeys(table_ids, "gen"),
        ),
    )
    for t in ctx["tables"]:
        endpoint = next(c for c in t["columns"] if c["name"] == "account_id")
        assert endpoint["sample_values"] == ["A-1"]


def test_entity_flow_samples_capped_and_truncated(session) -> None:
    """Served samples are the HEAD of the frequency-ordered top values (budget
    10), each value truncated at 100 chars — the profile's full top_k inventory
    (hundreds of rows for a high-cardinality identity column) must never reach
    the one cross-table prompt."""
    source = Source(name="cap_source", source_type="csv")
    session.add(source)
    session.flush()
    tbl = Table(
        source_id=source.source_id,
        table_name="events",
        layer="typed",
        row_count=100,
        duckdb_path="typed_events",
    )
    session.add(tbl)
    session.flush()
    col = Column(table_id=tbl.table_id, column_name="idc", column_position=0, raw_type="VARCHAR")
    session.add(col)
    session.flush()

    session.add(
        TableEntity(
            entity_id=_id(),
            table_id=tbl.table_id,
            run_id="cat",
            detected_entity_type="event",
            table_role="fact",
            identity_columns=[{"column": "idc", "note": "recurring identifier"}],
        )
    )
    long_value = "x" * 150
    top_values = [{"value": long_value, "count": 100, "percentage": 10.0}] + [
        {"value": f"v{i:02d}", "count": 50 - i, "percentage": 1.0} for i in range(14)
    ]
    session.add(
        StatisticalProfile(
            column_id=col.column_id,
            run_id="gen",
            layer="typed",
            total_count=100,
            null_count=0,
            profile_data={"top_values": top_values},
        )
    )
    session.commit()

    ctx = _build(
        session,
        [tbl.table_id],
        base_runs=BaseRunMap(relationship_run_id="cat", semantic_runs={tbl.table_id: "gen"}),
    )
    samples = ctx["tables"][0]["columns"][0]["sample_values"]
    assert len(samples) == 10
    assert samples[0] == "x" * 100 + "..."
    assert samples[1] == "v00"


# ---------------------------------------------------------------------------
# Chain-conditioned label samples (DAT-853): the from-side identity labels of a
# reference, aggregated over ONLY the rows that resolve across the join. The
# flat per-column samples blur populations sharing a table — the AP forensics
# case (counterparty at 37% vendors flat, 100% vendors on payment-linked rows).
# ---------------------------------------------------------------------------


@pytest.fixture
def payment_chain_with_labels(session):
    """bank_txns.payment_id -> payments.payment_id + a real DuckDB whose
    conditioned counterparty distribution differs from the flat one.

    Flat counterparty: customer-dominated (4x Customer X, 2x Bank Fee, one
    orphan). Payment-linked counterparty: 100% vendors. Returns
    ``(duck_conn, table_ids)``.
    """
    duck = duckdb.connect()
    duck.execute("CREATE TABLE typed_payments (payment_id VARCHAR)")
    duck.execute("INSERT INTO typed_payments VALUES ('P1'), ('P2'), ('P3'), ('P4')")
    duck.execute("CREATE TABLE typed_bank_txns (payment_id VARCHAR, counterparty VARCHAR)")
    duck.execute(
        "INSERT INTO typed_bank_txns VALUES "
        "('P1', 'Vendor A'), ('P2', 'Vendor A'), ('P3', 'Vendor A'), ('P4', 'Vendor B'), "
        "('ORPHAN-1', 'Orphan Corp'), "  # fk set, never resolves — must not ride
        "(NULL, 'Customer X'), (NULL, 'Customer X'), (NULL, 'Customer X'), "
        "(NULL, 'Customer X'), (NULL, 'Bank Fee'), (NULL, 'Bank Fee')"
    )

    source = Source(name="chain_source", source_type="csv")
    session.add(source)
    session.flush()
    bank = Table(
        source_id=source.source_id,
        table_name="bank_txns",
        layer="typed",
        row_count=11,
        duckdb_path="typed_bank_txns",
    )
    payments = Table(
        source_id=source.source_id,
        table_name="payments",
        layer="typed",
        row_count=4,
        duckdb_path="typed_payments",
    )
    session.add_all([bank, payments])
    session.flush()

    fk_col = Column(
        table_id=bank.table_id, column_name="payment_id", column_position=0, raw_type="VARCHAR"
    )
    label_col = Column(
        table_id=bank.table_id, column_name="counterparty", column_position=1, raw_type="VARCHAR"
    )
    key_col = Column(
        table_id=payments.table_id, column_name="payment_id", column_position=0, raw_type="VARCHAR"
    )
    session.add_all([fk_col, label_col, key_col])
    session.flush()

    session.add(
        TableEntity(
            entity_id=_id(),
            table_id=bank.table_id,
            run_id="run-current",
            detected_entity_type="bank transaction",
            table_role="fact",
            identity_columns=[
                {"column": "payment_id", "note": "matches the payments key"},
                {"column": "counterparty", "note": "external party label"},
            ],
        )
    )
    session.add(
        Relationship(
            run_id="run-current",
            from_table_id=bank.table_id,
            from_column_id=fk_col.column_id,
            to_table_id=payments.table_id,
            to_column_id=key_col.column_id,
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.9,
            detection_method="llm",
        )
    )
    session.commit()
    return duck, [bank.table_id, payments.table_id]


def test_conditioned_labels_serve_the_discriminating_distribution(
    session, payment_chain_with_labels
) -> None:
    """The conditioned line carries the join-riding population only: vendors at
    their joined-row shares — no NULL-fk populations (customers, fees) and no
    orphan fks (a claimed link that never resolves does not ride the join)."""
    duck, table_ids = payment_chain_with_labels
    ctx = build_cycle_detection_context(
        session,
        duck,
        table_ids,
        vertical="finance",
        base_runs=BaseRunMap(relationship_run_id="run-current"),
    )

    (rel,) = ctx["relationships"]
    assert rel["conditioned_label_samples"] == [
        {"column": "counterparty", "samples": ["Vendor A (75%)", "Vendor B (25%)"]}
    ]

    rendered = format_context_for_prompt(ctx)
    assert (
        "bank_txns.counterparty (payment_id-joined rows only): Vendor A (75%), Vendor B (25%)"
        in rendered
    )
    assert "Customer X" not in rendered
    assert "Orphan Corp" not in rendered
    # The fk column itself is a join key — never a conditioned label.
    assert "bank_txns.payment_id (payment_id-joined rows only)" not in rendered


def test_conditioned_labels_sensitive_name_serves_nothing(
    session, payment_chain_with_labels
) -> None:
    """This builder's privacy convention: a sensitive label is ABSENT from the
    conditioned serve (no placeholder), while non-sensitive labels still serve."""
    duck, table_ids = payment_chain_with_labels
    ctx = build_cycle_detection_context(
        session,
        duck,
        table_ids,
        vertical="finance",
        base_runs=BaseRunMap(relationship_run_id="run-current"),
        privacy=LLMPrivacy(sensitive_patterns=[".*counterparty.*"]),
    )
    (rel,) = ctx["relationships"]
    assert "conditioned_label_samples" not in rel
    assert "Vendor A" not in format_context_for_prompt(ctx)


def test_conditioned_labels_fail_soft_on_missing_typed_table(session) -> None:
    """A dangling duckdb_path logs and serves no conditioned line — the
    context build survives (the row-count read has the same posture)."""
    source = Source(name="dangling_source", source_type="csv")
    session.add(source)
    session.flush()
    a = Table(
        source_id=source.source_id,
        table_name="a_fact",
        layer="typed",
        row_count=1,
        duckdb_path="typed_missing_a",
    )
    b = Table(
        source_id=source.source_id,
        table_name="b_dim",
        layer="typed",
        row_count=1,
        duckdb_path="typed_missing_b",
    )
    session.add_all([a, b])
    session.flush()
    a_fk = Column(table_id=a.table_id, column_name="b_id", column_position=0, raw_type="VARCHAR")
    a_label = Column(
        table_id=a.table_id, column_name="label", column_position=1, raw_type="VARCHAR"
    )
    b_key = Column(table_id=b.table_id, column_name="b_id", column_position=0, raw_type="VARCHAR")
    session.add_all([a_fk, a_label, b_key])
    session.flush()
    session.add(
        TableEntity(
            entity_id=_id(),
            table_id=a.table_id,
            run_id="run-current",
            detected_entity_type="fact",
            table_role="fact",
            identity_columns=[{"column": "label", "note": "n"}],
        )
    )
    session.add(
        Relationship(
            run_id="run-current",
            from_table_id=a.table_id,
            from_column_id=a_fk.column_id,
            to_table_id=b.table_id,
            to_column_id=b_key.column_id,
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.9,
            detection_method="llm",
        )
    )
    session.commit()

    ctx = build_cycle_detection_context(
        session,
        duckdb.connect(),  # empty database — both paths dangle
        [a.table_id, b.table_id],
        vertical="finance",
        base_runs=BaseRunMap(relationship_run_id="run-current"),
    )
    (rel,) = ctx["relationships"]
    assert "conditioned_label_samples" not in rel


# ---------------------------------------------------------------------------
# Chain-conditioned measure sign/range (DAT-853): the from-side measures'
# min/max over ONLY the rows that resolve across the join. The flat profile
# blurs populations sharing a table — a measure globally mixed-sign can be
# uniformly one sign on the chain-linked rows, and that conditioned sign is
# the direction evidence.
# ---------------------------------------------------------------------------


@pytest.fixture
def payment_chain_with_measures(session):
    """bank_txns with two annotated measures over the payments join.

    ``amount`` is globally mixed-sign (positive unlinked rows, a positive
    orphan) but uniformly negative on the join-riding rows — the
    discriminating case where the conditioned sign differs from the global
    sign. ``fee`` is genuinely mixed-sign on the joined rows. Returns
    ``(duck_conn, table_ids, bank_table_id)``.
    """
    duck = duckdb.connect()
    duck.execute("CREATE TABLE typed_payments (payment_id VARCHAR)")
    duck.execute("INSERT INTO typed_payments VALUES ('P1'), ('P2')")
    duck.execute("CREATE TABLE typed_bank_txns (payment_id VARCHAR, amount DOUBLE, fee DOUBLE)")
    duck.execute(
        "INSERT INTO typed_bank_txns VALUES "
        "('P1', -135000.0, -5.0), ('P2', -50.25, 10.0), "
        "('ORPHAN-1', 77.0, 1.0), "  # fk set, never resolves — must not ride
        "(NULL, 200.0, 2.0), (NULL, 950.5, 3.0)"  # unlinked rows: positive
    )

    source = Source(name="measure_chain_source", source_type="csv")
    session.add(source)
    session.flush()
    bank = Table(
        source_id=source.source_id,
        table_name="bank_txns",
        layer="typed",
        row_count=5,
        duckdb_path="typed_bank_txns",
    )
    payments = Table(
        source_id=source.source_id,
        table_name="payments",
        layer="typed",
        row_count=2,
        duckdb_path="typed_payments",
    )
    session.add_all([bank, payments])
    session.flush()

    fk_col = Column(
        table_id=bank.table_id, column_name="payment_id", column_position=0, raw_type="VARCHAR"
    )
    amount_col = Column(
        table_id=bank.table_id, column_name="amount", column_position=1, raw_type="DOUBLE"
    )
    fee_col = Column(
        table_id=bank.table_id, column_name="fee", column_position=2, raw_type="DOUBLE"
    )
    key_col = Column(
        table_id=payments.table_id, column_name="payment_id", column_position=0, raw_type="VARCHAR"
    )
    session.add_all([fk_col, amount_col, fee_col, key_col])
    session.flush()

    # The measure selection is this builder's pinned annotations — pin both
    # measures under the bank table's generation run.
    for col in (amount_col, fee_col):
        session.add(
            SemanticAnnotation(column_id=col.column_id, run_id="gen-run", semantic_role="measure")
        )
    session.add(
        Relationship(
            run_id="run-current",
            from_table_id=bank.table_id,
            from_column_id=fk_col.column_id,
            to_table_id=payments.table_id,
            to_column_id=key_col.column_id,
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.9,
            detection_method="llm",
        )
    )
    session.commit()
    return duck, [bank.table_id, payments.table_id], bank.table_id


def test_conditioned_measure_ranges_serve_the_conditioned_sign(
    session, payment_chain_with_measures
) -> None:
    """The range line carries the join-riding population only: globally the
    amounts are mixed-sign, on the joined rows uniformly negative — the sign
    evidence the flat profile blurs. A genuinely mixed measure says so
    plainly, in column order."""
    duck, table_ids, bank_table_id = payment_chain_with_measures
    ctx = build_cycle_detection_context(
        session,
        duck,
        table_ids,
        vertical="finance",
        base_runs=BaseRunMap(
            relationship_run_id="run-current", semantic_runs={bank_table_id: "gen-run"}
        ),
    )

    (rel,) = ctx["relationships"]
    assert rel["conditioned_measure_ranges"] == [
        {"column": "amount", "min": -135000.0, "max": -50.25, "summary": "all negative"},
        {"column": "fee", "min": -5.0, "max": 10.0, "summary": "mixed signs"},
    ]

    rendered = format_context_for_prompt(ctx)
    assert (
        "bank_txns.amount (payment_id-joined rows only): min=-135000.0 max=-50.25 — all negative"
        in rendered
    )
    assert (
        "bank_txns.fee (payment_id-joined rows only): min=-5.0 max=10.0 — mixed signs" in rendered
    )


def test_conditioned_measure_ranges_sensitive_name_serves_nothing(
    session, payment_chain_with_measures
) -> None:
    """This builder's privacy convention: a sensitive measure is ABSENT from
    the conditioned serve (no placeholder); non-sensitive measures still serve."""
    duck, table_ids, bank_table_id = payment_chain_with_measures
    ctx = build_cycle_detection_context(
        session,
        duck,
        table_ids,
        vertical="finance",
        base_runs=BaseRunMap(
            relationship_run_id="run-current", semantic_runs={bank_table_id: "gen-run"}
        ),
        privacy=LLMPrivacy(sensitive_patterns=[".*amount.*"]),
    )
    (rel,) = ctx["relationships"]
    assert rel["conditioned_measure_ranges"] == [
        {"column": "fee", "min": -5.0, "max": 10.0, "summary": "mixed signs"}
    ]
    assert "-135000.0" not in format_context_for_prompt(ctx)


def test_conditioned_measure_ranges_nan_serves_nothing(session) -> None:
    """A NaN row poisons MIN/MAX (DuckDB sorts NaN greatest) and every sign
    comparison — a poisoned range would read 'all positive'. Absence is the
    honest serving."""
    duck = duckdb.connect()
    duck.execute("CREATE TABLE typed_payments (payment_id VARCHAR)")
    duck.execute("INSERT INTO typed_payments VALUES ('P1'), ('P2')")
    duck.execute("CREATE TABLE typed_bank_txns (payment_id VARCHAR, amount DOUBLE)")
    duck.execute("INSERT INTO typed_bank_txns VALUES ('P1', 2.0), ('P2', CAST('nan' AS DOUBLE))")

    source = Source(name="nan_chain_source", source_type="csv")
    session.add(source)
    session.flush()
    bank = Table(
        source_id=source.source_id,
        table_name="bank_txns",
        layer="typed",
        row_count=2,
        duckdb_path="typed_bank_txns",
    )
    payments = Table(
        source_id=source.source_id,
        table_name="payments",
        layer="typed",
        row_count=2,
        duckdb_path="typed_payments",
    )
    session.add_all([bank, payments])
    session.flush()
    fk_col = Column(
        table_id=bank.table_id, column_name="payment_id", column_position=0, raw_type="VARCHAR"
    )
    amount_col = Column(
        table_id=bank.table_id, column_name="amount", column_position=1, raw_type="DOUBLE"
    )
    key_col = Column(
        table_id=payments.table_id, column_name="payment_id", column_position=0, raw_type="VARCHAR"
    )
    session.add_all([fk_col, amount_col, key_col])
    session.flush()
    session.add(
        SemanticAnnotation(
            column_id=amount_col.column_id, run_id="gen-run", semantic_role="measure"
        )
    )
    session.add(
        Relationship(
            run_id="run-current",
            from_table_id=bank.table_id,
            from_column_id=fk_col.column_id,
            to_table_id=payments.table_id,
            to_column_id=key_col.column_id,
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.9,
            detection_method="llm",
        )
    )
    session.commit()

    ctx = build_cycle_detection_context(
        session,
        duck,
        [bank.table_id, payments.table_id],
        vertical="finance",
        base_runs=BaseRunMap(
            relationship_run_id="run-current", semantic_runs={bank.table_id: "gen-run"}
        ),
    )
    (rel,) = ctx["relationships"]
    assert "conditioned_measure_ranges" not in rel


def test_format_context_renders_zero_annotation_confidence() -> None:
    """0.0 is a real confidence, not absence — the falsy boundary must render."""
    context = {
        "tables": [
            {
                "table_name": "t",
                "row_count": 1,
                "columns": [{"name": "c", "type": "VARCHAR", "annotation_confidence": 0.0}],
            }
        ],
        "entity_classifications": [],
    }

    rendered = format_context_for_prompt(context)

    assert "annotation_confidence=0.00" in rendered


def test_format_context_renders_entity_flow_evidence() -> None:
    """The prompt renders identity columns (with notes), annotation confidence,
    and the samples line for entity-flow columns."""
    context = {
        "tables": [
            {
                "table_name": "invoices",
                "row_count": 100,
                "columns": [
                    {
                        "name": "zq_p4x",
                        "type": "VARCHAR",
                        "semantic_role": "dimension",
                        "entity_type": "entity_identifier",
                        "business_description": "the entity associated with the row",
                        "annotation_confidence": 0.25,
                        "sample_values": ["E-0002", "E-0003"],
                    }
                ],
            }
        ],
        "entity_classifications": [
            {
                "table_name": "invoices",
                "entity_type": "invoice",
                "description": "Invoice rows.",
                "table_role": "fact",
                "grain_columns": ["invoice_id"],
                "identity_columns": [
                    {"column": "zq_p4x", "note": "recurring counterparty identifier"}
                ],
            }
        ],
    }

    rendered = format_context_for_prompt(context)

    assert "identity columns: zq_p4x (recurring counterparty identifier)" in rendered
    assert "annotation_confidence=0.25" in rendered
    assert "samples: E-0002, E-0003" in rendered


def test_format_context_omits_absent_entity_flow_evidence() -> None:
    """A column without samples/confidence renders neither line — absence stays
    absence, no placeholder formatting."""
    context = {
        "tables": [
            {
                "table_name": "invoices",
                "row_count": 100,
                "columns": [{"name": "amount", "type": "DECIMAL", "semantic_role": "measure"}],
            }
        ],
        "entity_classifications": [],
    }

    rendered = format_context_for_prompt(context)

    assert "samples:" not in rendered
    assert "annotation_confidence=" not in rendered
    assert "identity columns:" not in rendered


def test_format_context_renders_structural_slice_without_confidence() -> None:
    """DAT-725: structural inventory rows carry no LLM confidence — the prompt
    header must render without one, never crash formatting None."""
    context = {
        "tables": [{"table_name": "facts", "row_count": 10, "columns": []}],
        "slice_definitions": [
            {
                "table_name": "facts",
                "column_name": "region",
                "slice_type": "categorical",
                "values": ["EMEA", "APAC"],
                "value_counts": [],
                "confidence": None,
                "business_context": None,
                "priority": 1000,
            },
            {
                "table_name": "facts",
                "column_name": "status",
                "slice_type": "categorical",
                "values": ["open", "paid"],
                "value_counts": [],
                "confidence": 0.9,
                "business_context": "lifecycle",
                "priority": 1,
            },
        ],
    }

    rendered = format_context_for_prompt(context)

    assert "### facts.region" in rendered
    assert "### facts.region (confidence:" not in rendered
    assert "### facts.status (confidence: 90%)" in rendered
