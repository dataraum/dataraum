"""Graph-traversal context assembly on real Postgres 19 (DAT-734).

``build_execution_context`` reads the operating-model property graph (ADR-0021)
as its traversal core: concept → part_of subconcepts → groundings (grounded_by)
→ columns (uses), plus disjoint_with / reconciles_with / conformed-dimension /
materializes_as served AS STRUCTURE. This exercises that read against the SAME
seeded, fully-promoted workspace the property-graph acceptance tests use
(``tests.integration.storage.test_property_graph``), so the context assembly is
tested against the exact substrate the graph binds — including the AP-class
scorecard shape: ``account_balance`` grounded twice (trial_balance /
balance_sheet) with a derived ``reconciles_with`` self-loop.

Loud-absence cases ride the same seed: a grounding whose concept names no
active row (``sn_old``), a healthy pre-parts row (``sn_nul``), and a retained
failure (``sn_fail``) — each must surface exactly as designed (dropped loud /
skipped loud / served discriminated), never silently.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import Engine
from sqlalchemy.orm import Session, sessionmaker

from dataraum.graphs.context_models import GraphExecutionContext
from dataraum.graphs.context_reads import build_execution_context
from dataraum.server.workspace import schema_name_for
from tests.integration.storage.test_property_graph import _boot, _seed

pytestmark = pytest.mark.integration


@pytest.fixture
def graph_ctx_engine(integration_engine: Engine) -> Engine:
    """The seeded, promoted workspace with read views + property graph live."""
    schema = schema_name_for(os.environ["DATARAUM_WORKSPACE_ID"])
    _seed(integration_engine)
    _boot(integration_engine, schema)
    return integration_engine


@pytest.fixture
def ctx(graph_ctx_engine: Engine) -> GraphExecutionContext:
    """One assembled context over the seeded journal/accounts/statement tables."""
    factory = sessionmaker(bind=graph_ctx_engine)
    with factory() as session:
        return build_execution_context(
            session,
            ["t1", "t2", "t4"],
            workspace_id=os.environ["DATARAUM_WORKSPACE_ID"],
        )


def _concept(ctx: GraphExecutionContext, name: str):
    match = [c for c in ctx.concepts if c.name == name]
    assert match, f"concept {name!r} not served; got {[c.name for c in ctx.concepts]}"
    return match[0]


class TestTraversalCore:
    """concept → grounded_by → uses, read through the graph."""

    def test_ap_class_concept_serves_both_groundings_and_reconciliation(
        self, ctx: GraphExecutionContext
    ) -> None:
        """The DAT-734 AC shape: the multi-grounded concept surfaces BOTH healthy
        groundings AND its reconciles_with verdict in the served context."""
        bal = _concept(ctx, "account_balance")

        healthy = [g for g in bal.groundings if not g.failed]
        assert len(healthy) == 2
        assert {g.statement for g in healthy} == {"trial_balance", "balance_sheet"}
        assert {g.relation for g in healthy} == {"enriched_journal"}

        # The derived self-loop: this concept's computations must tie out.
        assert [r.partner for r in bal.reconciles_with] == ["account_balance"]

    def test_grounding_serves_parts_and_uses_as_structure(self, ctx: GraphExecutionContext) -> None:
        """select_expr / where[] / uses come from the graph (parts + contract v2),
        never parsed out of SQL text."""
        bal = _concept(ctx, "account_balance")
        tb = next(g for g in bal.groundings if g.statement == "trial_balance")

        assert tb.select_expr == "SUM(amount)"
        assert tb.where == [
            "account_id__account_type IN ('asset','liability')",
            "account_id__account_type IS NOT NULL",
        ]
        # uses resolve to the SERVED relation's columns (DAT-811): the enriched
        # view's own vertices, role-tagged from column_mappings_basis.
        used = {(u.column_name, u.table_name, u.role) for u in tb.uses}
        assert used == {
            ("amount", "enriched_journal", "measure"),
            ("account_id__account_type", "enriched_journal", "filter"),
        }

    def test_typed_relation_fallback_grounding(self, ctx: GraphExecutionContext) -> None:
        """A grounding over the typed fact (no enriched view of that name) resolves
        its uses against the typed columns."""
        rev = _concept(ctx, "revenue")
        healthy = [g for g in rev.groundings if not g.failed]
        assert len(healthy) == 1
        assert healthy[0].relation == "journal"
        assert {(u.column_name, u.table_name) for u in healthy[0].uses} == {("amount", "journal")}

    def test_retained_failure_served_discriminated(self, ctx: GraphExecutionContext) -> None:
        """A DAT-543 retained failure is part of the served knowledge — failed +
        mode + reason, with no uses (its provenance carries no basis)."""
        rev = _concept(ctx, "revenue")
        failed = [g for g in rev.groundings if g.failed]
        assert len(failed) == 1
        assert failed[0].failure_mode == "verifier_rejected"
        assert failed[0].failure_reason == "no support"
        assert failed[0].uses == []

    def test_unresolved_concept_grounding_not_served(self, ctx: GraphExecutionContext) -> None:
        """``sn_old``/``sn_nul`` name 'expenses' — no active concept row, so the
        grounded_by edge drops (loud in the log) and nothing serves them."""
        assert all(c.name != "expenses" for c in ctx.concepts)
        served_snippets = {g.snippet_id for c in ctx.concepts for g in c.groundings}
        assert "sn_old" not in served_snippets
        assert "sn_nul" not in served_snippets


class TestConceptAdditivity:
    """has_additivity read live (DAT-671 R4): the drill's gate, in the author's context.

    The seed carries the shape that matters: ``revenue`` (an active measure
    concept) with a categorical class row, a time class row, and a concrete
    ``period`` refinement carrying an observed cadence — plus ``mk_margin`` and
    ``mk_unknown``, which are METRIC targets keyed by ``graph_id`` and therefore
    have no concept to hang an edge from.
    """

    def test_measure_verdicts_reach_the_concept_through_the_edge(
        self, ctx: GraphExecutionContext
    ) -> None:
        rev = _concept(ctx, "revenue")

        # Class row before its refinement — a concrete axis REFINES the class
        # verdict, so reading the refinement first inverts the relationship.
        assert [(a.axis_kind, a.axis_key) for a in rev.additivity] == [
            ("categorical", "*"),
            ("time", "*"),
            ("time", "period"),
        ]
        categorical, time_class, time_period = rev.additivity
        assert (categorical.status, categorical.verdict) == ("classified", "additive")
        assert categorical.reason is None
        assert (time_class.verdict, time_class.reason) == ("semi_additive", "stock")
        assert time_period.bucket_grain == "month"

    def test_metric_targets_never_reach_a_concept(self, ctx: GraphExecutionContext) -> None:
        """A ``metric`` target keys on a formula ``graph_id`` with no concept
        vertex, so ``og_has_additivity`` cannot speak for it — and this document
        has no metric block to render one into. The absence is by construction,
        and pinned so a later widening of the edge is a deliberate act."""
        served = {(c.name, a.axis_key) for c in ctx.concepts for a in c.additivity}
        assert all(name not in ("mk_margin", "mk_unknown") for name, _ in served)
        assert all(c.additivity == [] for c in ctx.concepts if c.name != "revenue")

    def test_the_verdicts_are_rendered_into_the_served_document(
        self, ctx: GraphExecutionContext
    ) -> None:
        """Reaching the dataclass is not reaching the AGENT — the grounding
        author reads the rendered document, which is the whole point of R4."""
        from dataraum.graphs.context_format import format_served_context

        out = format_served_context(ctx)

        assert "aggregation (last promoted run):" in out
        assert "by category — additive" in out
        assert (
            "over time — semi_additive: sums a point-in-time balance, so adding period "
            "buckets double-counts" in out
        )
        # WHOLE line, not a substring: the seed's `period` row repeats its class
        # row's verdict, so a substring check is satisfied by the un-elided line
        # too and would pass against a broken inheritance rule. This is the only
        # test driving the real MATCH → fold → render path, so it has to pin the
        # rendered form exactly.
        assert (
            '    - on "period" (time) — semi_additive; finest bucket the data supports: month'
            in out.splitlines()
        )
        assert out.count("sums a point-in-time balance") == 1


class TestConceptEdges:
    """part_of / disjoint_with served from og_concept_edges."""

    def test_part_of_parents_children_and_bounded_ancestry(
        self, ctx: GraphExecutionContext
    ) -> None:
        """comp_a → comp_b → comp_c with a back edge comp_c → comp_a: 1-hop served
        as parents/children; the closure serves depth-2 ancestry and the cycle
        guard keeps comp_a out of its own ancestry."""
        a = _concept(ctx, "comp_a")
        assert a.part_of_parents == ["comp_b"]
        assert a.part_of_children == ["comp_c"]  # via the back edge comp_c → comp_a
        assert a.part_of_ancestry == ["comp_c"]
        assert "comp_a" not in a.part_of_ancestry

        b = _concept(ctx, "comp_b")
        assert b.part_of_children == ["comp_a"]
        assert b.part_of_parents == ["comp_c"]

    def test_disjoint_with_served_symmetrically(self, ctx: GraphExecutionContext) -> None:
        ap = _concept(ctx, "accounts_payable")
        ar = _concept(ctx, "accounts_receivable")
        assert ap.disjoint_with == ["accounts_receivable"]
        assert ar.disjoint_with == ["accounts_payable"]


class TestStructuralEdges:
    """conformed-dimension / derived_from / materializes_as served as structure."""

    def test_conformed_dimension_axes_deduped_per_pair(self, ctx: GraphExecutionContext) -> None:
        """journal and statement share TWO accounts axes (DAT-788): account_type via
        the SAME account_id role, and segment via differently-named roles the judge
        CONFORMED. Each is served as ONE unordered pair (deduped from both directions +
        role multiplicity); the bill-to/ship-to region slices do NOT conform."""
        axes = sorted((cd.dimension_table, cd.attribute) for cd in ctx.conformed_dimensions)
        assert axes == [("accounts", "account_type"), ("accounts", "segment")]
        for cd in ctx.conformed_dimensions:
            assert {cd.table_a, cd.table_b} == {"journal", "statement"}

    def test_materialization_and_anchor_on_columns(self, ctx: GraphExecutionContext) -> None:
        """og_columns semantics land on the column contexts: witness posterior
        (flow) for the witnessed measure; declared anchor for the unwitnessed one."""
        t1 = next(t for t in ctx.tables if t.table_name == "journal")
        cols = {c.column_name: c for c in t1.columns}
        assert cols["amount"].materialization == "flow"
        assert cols["amount"].anchor_time_axis == "period_date"  # witness axis
        assert cols["amount_declared"].materialization is None
        assert cols["amount_declared"].anchor_time_axis == "txn_date"  # declared anchor

    def test_stored_sign_reaches_the_grounding_author(self, ctx: GraphExecutionContext) -> None:
        """DAT-875/886: the resolved storage convention rides og_columns onto the
        served column context, so a metric extract SUMming a stored balance knows
        whether the magnitude it returns is already a natural balance. Without it a
        credit-normal liability extract returns a negative magnitude into a ratio
        (the accounts_payable / dpo defect). NULL stays NULL — no fact beats a guess.
        """
        t1 = next(t for t in ctx.tables if t.table_name == "journal")
        cols = {c.column_name: c for c in t1.columns}
        assert cols["amount"].stored_sign == "ledger_signed"
        assert cols["amount_declared"].stored_sign is None

    def test_stored_sign_is_rendered_into_the_column_notes(self) -> None:
        """The served fact has to reach the PROMPT, not just the dataclass — the
        notes column is what the grounding author actually reads."""
        from dataraum.graphs.context_format import _build_column_notes
        from dataraum.graphs.context_models import ColumnContext

        ledger = _build_column_notes(
            ColumnContext(
                column_id="c",
                column_name="ending_balance",
                table_name="balance_sheet",
                semantic_role="measure",
                stored_sign="ledger_signed",
            )
        )
        assert "ledger_signed" in ledger
        assert "opposite sign to its natural balance" in ledger
        # DESCRIPTIVE, never prescriptive. "A bare SUM returns a signed quantity"
        # is false wherever the reconciling population is single-family or the
        # measure is not account-shaped — the label is still right there, but the
        # consequence is not, and it invited a sign flip on a family that need not
        # exist. State the convention; let the author reason about its own query.
        assert "bare SUM" not in ledger
        assert "SUM" not in ledger

        natural = _build_column_notes(
            ColumnContext(
                column_id="c",
                column_name="ending_balance",
                table_name="balance_sheet",
                semantic_role="measure",
                stored_sign="natural_balance",
            )
        )
        assert "natural_balance" in natural

        undetermined = _build_column_notes(
            ColumnContext(column_id="c", column_name="x", table_name="t", semantic_role="measure")
        )
        assert "Stored sign" not in undetermined

    def test_enriched_view_serves_dimension_bases(self, ctx: GraphExecutionContext) -> None:
        """derived_from edges attach the view's dimension base TABLES."""
        ev = next(v for v in ctx.enriched_views if v.view_name == "enriched_journal")
        assert ev.dimension_tables == ["accounts"]

    def test_time_axes_are_served_from_the_temporal_coverage_edge(
        self, ctx: GraphExecutionContext
    ) -> None:
        """A relation's time axes come from the graph edge, whole (DAT-671 R6).

        Role, aspect, the authored note, the observed window/grain AND the worst
        discontinuity all ride ONE relation now — the assembly used to iterate the
        raw ``table_entities.time_columns`` JSON and look each name up in the
        separately-read column list, which found nothing for an axis living only
        on the enriched layer (DAT-866).
        """
        t1 = next(t for t in ctx.tables if t.table_name == "journal")
        axes = {a.column_name: a for a in t1.time_axes}
        assert set(axes) == {
            "txn_date",
            "created_date",
            "due_date",
            "account_id__open_date",
            "account_id__close_date",
            "orphaned__date",
        }
        # The declared anchor sorts first, then the remaining EVENT axes, then the
        # attribute dates — the order the served document reads them in.
        assert [a.column_name for a in t1.time_axes][:2] == ["txn_date", "created_date"]

        txn = axes["txn_date"]
        assert (txn.role, txn.aspect, txn.is_anchor, txn.note) == ("event", "txn", True, "x")
        assert txn.detected_granularity == "month"
        assert txn.span_days == 334
        assert txn.largest_gap_days is None  # a complete series has no worst gap

        # The widening: the worst discontinuity now reaches the author.
        created = axes["created_date"]
        assert (created.role, created.is_anchor) == ("event", False)
        assert created.detected_granularity == "day"
        assert created.largest_gap_days == 5

        # An axis with no temporal profile keeps its edge with NULL observations —
        # absence falls loud, never a fabricated window.
        due = axes["due_date"]
        assert due.role == "attribute"
        assert (due.detected_granularity, due.span_days, due.largest_gap_days) == (None, None, None)

        # DAT-866: the enriched-only axis is served at all — the exact case the old
        # column-list lookup silently dropped.
        assert axes["account_id__open_date"].detected_granularity == "year"
        # `orphaned__date` rides the set assertion only: it exists to prove the
        # edge survives a dangling dim reference, and its properties are pinned
        # where that behaviour lives (tests/integration/storage/test_property_graph.py).

    def test_time_axes_reach_the_served_document(self, ctx: GraphExecutionContext) -> None:
        """The graph-served axes have to reach the PROMPT — EVENT axes only, with
        the gap warning the agent needs before doing period-over-period work."""
        from dataraum.graphs.context_format import format_served_context

        doc = format_served_context(ctx)
        rendered = [line for line in doc.splitlines() if "**Time column**" in line]
        assert len(rendered) == 1, rendered  # both event axes ride ONE meta line
        line = rendered[0]
        assert "**Time column**: txn_date (by txn) — month" in line
        assert "334d span" in line
        assert "**Time column**: created_date (by created) — day" in line
        assert "largest gap 5d" in line, "the worst discontinuity reaches the author"
        # Attribute dates are normal columns, never presented as a trend lens.
        assert "**Time column**: due_date" not in doc
        assert "**Time column**: account_id__open_date" not in doc


class TestGraphUnreachable:
    """No workspace identity ⇒ graph sections empty, assembly intact (loud log)."""

    def test_no_workspace_id_serves_empty_graph_sections(self, graph_ctx_engine: Engine) -> None:
        factory = sessionmaker(bind=graph_ctx_engine)
        session: Session
        with factory() as session:
            ctx = build_execution_context(session, ["t1"])
        assert ctx.concepts == []
        assert ctx.conformed_dimensions == []
        assert ctx.tables  # the non-graph assembly still built


class TestEndpointMissesDropLoudNotCrash:
    """A graph edge whose endpoint the vertex maps cannot resolve drops VISIBLY
    (warning + drop), never crashes the build and never serves a broken row.

    Seeds a table ``t9`` that exists physically but has NO generation head — so
    it is absent from ``current_tables`` and therefore from the ``og_tables``
    vertex map — then hangs a reference, a conformed-dimension pair, and a
    derived_from base off it (reviewer finding: these paths were untested).
    """

    def test_reference_conformed_and_derived_misses(self, graph_ctx_engine: Engine) -> None:
        from sqlalchemy import text

        ts = "2026-01-01 00:00:00"
        src = "00000000-0000-0000-0000-000000000002"
        run = "00000000-0000-0000-0000-000000000001"
        stmts = [
            # t9: physically present, NO generation head → not an og_tables vertex.
            f"INSERT INTO tables (table_id, source_id, table_name, layer, created_at) "
            f"VALUES ('t9', '{src}', 'ghost', 'typed', '{ts}')",
            "INSERT INTO columns (column_id, table_id, column_name, column_position) "
            "VALUES ('c9', 't9', 'ghost_id', 1)",
            # Reference whose to-endpoint is the unresolvable t9.
            "INSERT INTO relationships (relationship_id, run_id, from_table_id, "
            " from_column_id, to_table_id, to_column_id, relationship_type, cardinality, "
            " confidence, confirmation_source, detected_at) "
            f"VALUES ('r9', '{run}', 't1', 'c_k1', 't9', 'c9', 'foreign_key', "
            f"'many-to-one', 0.9, 'judge', '{ts}')",
            # A conformed pair whose shared dimension table is t9.
            "INSERT INTO slice_definitions (slice_id, run_id, table_id, column_id, "
            " column_name, dimension_table_id, dimension_attribute, fk_role, "
            " slice_relevance, slice_interest, slice_type, detection_source, created_at) "
            f"VALUES ('sl_9', '{run}', 't1', 'c_k1', 'ghost__region', 't9', 'region9', "
            f"'account_id', 0.7, 'supporting', 'categorical', 'llm', '{ts}')",
            "INSERT INTO slice_definitions (slice_id, run_id, table_id, column_id, "
            " column_name, dimension_table_id, dimension_attribute, fk_role, "
            " slice_relevance, slice_interest, slice_type, detection_source, created_at) "
            f"VALUES ('sl_9b', '{run}', 't4', 'c_k4', 'ghost__region', 't9', 'region9', "
            f"'account_id', 0.7, 'supporting', 'categorical', 'llm', '{ts}')",
            # DAT-788: referenced cells so the t9 conformed edge FORMS (same account_id
            # role → one group) — it must then drop on the unresolvable t9 endpoint,
            # not silently vanish for want of a cell. 'judge' because the edge serves
            # CONFIRMED cells only (DAT-809), so an unconfirmed pair never forms one.
            "INSERT INTO bus_matrix (entry_id, run_id, fact_table_id, attachment, "
            " concept_label, dimension_table_id, roles, attributes, confirmation_source, "
            " conformed_group, needs_confirmation, signature, created_at) "
            f"VALUES ('bm_9', '{run}', 't1', 'referenced', 'ghost', 't9', "
            f"'[\"account_id\"]', '[]', 'judge', 'ref:t9:account_id', false, "
            f"'bus:referenced:t1:t9:account_id', '{ts}')",
            "INSERT INTO bus_matrix (entry_id, run_id, fact_table_id, attachment, "
            " concept_label, dimension_table_id, roles, attributes, confirmation_source, "
            " conformed_group, needs_confirmation, signature, created_at) "
            f"VALUES ('bm_9b', '{run}', 't4', 'referenced', 'ghost', 't9', "
            f"'[\"account_id\"]', '[]', 'judge', 'ref:t9:account_id', false, "
            f"'bus:referenced:t4:t9:account_id', '{ts}')",
            # An enriched view deriving from the unresolvable t9 dimension base.
            # Fact t4 — one enriched view per fact (uq_enriched_view_fact_table),
            # and t1 already carries the seed's enriched_journal.
            f"INSERT INTO tables (table_id, source_id, table_name, layer, created_at) "
            f"VALUES ('t_enr9', '{src}', 'enriched_l9', 'enriched', '{ts}')",
            "INSERT INTO enriched_views (view_id, fact_table_id, view_table_id, "
            " view_name, run_id, dimension_table_ids, is_grain_verified, created_at) "
            f"VALUES ('v_9', 't4', 't_enr9', 'enriched_l9', '{run}', "
            f"'[\"t9\"]'::json, true, '{ts}')",
        ]
        with graph_ctx_engine.begin() as conn:
            for s in stmts:
                conn.execute(text(s))

        factory = sessionmaker(bind=graph_ctx_engine)
        with factory() as session:
            ctx = build_execution_context(
                session,
                ["t1", "t2", "t4", "t9"],
                workspace_id=os.environ["DATARAUM_WORKSPACE_ID"],
            )

        # Reference: the resolvable edge serves; the t9 edge dropped, no crash.
        pairs = {(r.from_table, r.to_table) for r in ctx.relationships}
        assert ("journal", "accounts") in pairs
        assert not any("ghost" in p for pair in pairs for p in pair)

        # Conformed: only the resolvable accounts axes (account_type + the judge-merged
        # segment); the t9 axis formed an edge but dropped on its unresolvable endpoint.
        assert [(c.dimension_table, c.attribute) for c in ctx.conformed_dimensions] == [
            ("accounts", "account_type"),
            ("accounts", "segment"),
        ]

        # Derived: the view serves, its unresolvable base dropped (empty bases).
        l9 = next(v for v in ctx.enriched_views if v.view_name == "enriched_l9")
        assert l9.dimension_tables == []
        # The healthy view's bases are untouched.
        ej = next(v for v in ctx.enriched_views if v.view_name == "enriched_journal")
        assert ej.dimension_tables == ["accounts"]
