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

from dataraum.graphs.context import GraphExecutionContext, build_execution_context
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

    def test_enriched_view_serves_dimension_bases(self, ctx: GraphExecutionContext) -> None:
        """derived_from edges attach the view's dimension base TABLES."""
        ev = next(v for v in ctx.enriched_views if v.view_name == "enriched_journal")
        assert ev.dimension_tables == ["accounts"]


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
            " slice_priority, slice_type, detection_source, created_at) "
            f"VALUES ('sl_9', '{run}', 't1', 'c_k1', 'ghost__region', 't9', 'region9', "
            f"'account_id', 2, 'categorical', 'llm', '{ts}')",
            "INSERT INTO slice_definitions (slice_id, run_id, table_id, column_id, "
            " column_name, dimension_table_id, dimension_attribute, fk_role, "
            " slice_priority, slice_type, detection_source, created_at) "
            f"VALUES ('sl_9b', '{run}', 't4', 'c_k4', 'ghost__region', 't9', 'region9', "
            f"'account_id', 2, 'categorical', 'llm', '{ts}')",
            # DAT-788: referenced cells so the t9 conformed edge FORMS (same account_id
            # role → one group) — it must then drop on the unresolvable t9 endpoint,
            # not silently vanish for want of a cell.
            "INSERT INTO bus_matrix (entry_id, run_id, fact_table_id, attachment, "
            " concept_label, dimension_table_id, roles, attributes, confirmation_source, "
            " conformed_group, needs_confirmation, signature, created_at) "
            f"VALUES ('bm_9', '{run}', 't1', 'referenced', 'ghost', 't9', "
            f"'[\"account_id\"]', '[]', 'unconfirmed', 'ref:t9:account_id', false, "
            f"'bus:referenced:t1:t9:account_id', '{ts}')",
            "INSERT INTO bus_matrix (entry_id, run_id, fact_table_id, attachment, "
            " concept_label, dimension_table_id, roles, attributes, confirmation_source, "
            " conformed_group, needs_confirmation, signature, created_at) "
            f"VALUES ('bm_9b', '{run}', 't4', 'referenced', 'ghost', 't9', "
            f"'[\"account_id\"]', '[]', 'unconfirmed', 'ref:t9:account_id', false, "
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
