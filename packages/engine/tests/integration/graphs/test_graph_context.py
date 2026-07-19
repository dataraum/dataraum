"""Graph-traversal context assembly on real Postgres 19 (DAT-734).

``build_execution_context`` reads the operating-model property graph (ADR-0021)
as its traversal core: concept → part_of subconcepts → groundings (grounded_by)
→ columns (uses), plus disjoint_with / reconciles_with / conformed-dimension /
materializes_as served AS STRUCTURE. This exercises that read against the SAME
seeded, fully-promoted workspace the property-graph P1/P2 acceptance tests use
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

    def test_conformed_dimension_axis_deduped_to_one_pair(self, ctx: GraphExecutionContext) -> None:
        """journal and statement share the accounts.account_type axis — served as
        ONE unordered pair; the cross-level (region) slice does not conform."""
        assert len(ctx.conformed_dimensions) == 1
        cd = ctx.conformed_dimensions[0]
        assert {cd.table_a, cd.table_b} == {"journal", "statement"}
        assert cd.dimension_table == "accounts"
        assert cd.attribute == "account_type"

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
