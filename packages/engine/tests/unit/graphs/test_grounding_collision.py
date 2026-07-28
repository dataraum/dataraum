"""Cross-concept grounding collision guard (DAT-709).

The live bug this pins: ``current_assets`` and ``current_liabilities`` ground to
the IDENTICAL extract, so ``current_ratio`` ships a confident 1.0. Grounding is
one LLM call per concept and the snippet key includes ``standard_field``, so
nothing upstream of this guard can see it.

Two grains here: the pure detector (canonical-SQL equality, scoped to disjoint
pairs) and the pass that drives it (re-ground once, then abstain typed). The LLM
is stubbed throughout — the ``reauthor`` stub models the real contract exactly
(demote, then heal the row with whatever the retry produced), because the guard's
correctness depends on that demotion being what unblocks the LLM.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest
from sqlalchemy import select

from dataraum.analysis.semantic.concept_edge_store import ensure_concept_edges_seeded
from dataraum.graphs.grounding_collision import (
    GroundedExtract,
    collision_reason,
    disjoint_map,
    find_collisions,
    flag_collision,
    resolve_grounding_collisions,
)
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    SnippetFailureMode,
    StepSource,
    StepType,
    TransformationGraph,
)
from dataraum.graphs.node_warming import NodeDecision, build_warm_dag
from dataraum.query.snippet_library import SnippetLibrary
from dataraum.query.snippet_models import SQLSnippetRecord

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.graphs.node_warming import NodeKey, WarmNode

_WS = "ws-collision"
_SCHEMA = "ws-collision"
# The colliding statement: what BOTH concepts grounded to in the live bug.
_SAME = 'SELECT SUM("amount") AS value FROM enriched_gl WHERE "period" = \'2024-12\''


def _extract(step_id: str, standard_field: str) -> GraphStep:
    return GraphStep(
        step_id=step_id,
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field=standard_field, statement="balance_sheet"),
        aggregation="sum",
        output_step=True,
    )


def _graph(graph_id: str, steps: dict[str, GraphStep]) -> TransformationGraph:
    return TransformationGraph(
        graph_id=graph_id,
        version="1.0",
        metadata=GraphMetadata(
            name=graph_id, description="", category="liquidity", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
    )


def _nodes_for(*concepts: str) -> dict[NodeKey, WarmNode]:
    """One warm node per concept, keyed exactly as the authoring pass keys them."""
    _, nodes = build_warm_dag(
        {c: _graph(c, {c: _extract(c, c)}) for c in concepts},
    )
    return nodes


def _save(session: Session, concept: str, sql: str, *, flush: bool = True) -> None:
    SnippetLibrary(session, workspace_id=_WS).save_snippet(
        snippet_type="extract",
        sql=sql,
        description=f"{concept} extract",
        schema_mapping_id=_SCHEMA,
        source=f"graph:{concept}",
        standard_field=concept,
        statement="balance_sheet",
        aggregation="sum",
    )
    if flush:
        session.flush()


def _record(session: Session, concept: str) -> SQLSnippetRecord:
    return (
        session.execute(select(SQLSnippetRecord).where(SQLSnippetRecord.standard_field == concept))
        .scalars()
        .one()
    )


class TestFindCollisions:
    """The detector: canonical-SQL equality, scoped to disjoint pairs."""

    _DISJOINT = {
        "current_assets": frozenset({"current_liabilities"}),
        "current_liabilities": frozenset({"current_assets"}),
    }

    def test_identical_statements_across_disjoint_concepts_collide(self) -> None:
        groups = find_collisions(
            [
                GroundedExtract(
                    key=("extract", "current_assets"), concept="current_assets", sql=_SAME
                ),
                GroundedExtract(
                    key=("extract", "current_liabilities"),
                    concept="current_liabilities",
                    sql=_SAME,
                ),
            ],
            self._DISJOINT,
        )
        assert len(groups) == 1
        assert [m.concept for m in groups[0].members] == ["current_assets", "current_liabilities"]

    def test_syntax_noise_does_not_hide_the_collision(self) -> None:
        """Canonical form, not bytes: whitespace and keyword casing collapse."""
        noisy = (
            'select   SUM("amount")   AS value\n  from enriched_gl\n  WHERE "period" = \'2024-12\''
        )
        groups = find_collisions(
            [
                GroundedExtract(key=("a",), concept="current_assets", sql=_SAME),
                GroundedExtract(key=("b",), concept="current_liabilities", sql=noisy),
            ],
            self._DISJOINT,
        )
        assert len(groups) == 1

    def test_different_statements_do_not_collide(self) -> None:
        groups = find_collisions(
            [
                GroundedExtract(key=("a",), concept="current_assets", sql=_SAME),
                GroundedExtract(
                    key=("b",),
                    concept="current_liabilities",
                    sql=_SAME.replace("enriched_gl", "enriched_ap"),
                ),
            ],
            self._DISJOINT,
        )
        assert groups == []

    def test_identical_statements_across_NON_disjoint_concepts_are_left_alone(self) -> None:
        """A whole with exactly one part legitimately shares its extract.

        ``cash`` sits INSIDE ``current_assets`` (same group in every finance
        convention, plus a part_of edge), so an identity check over all concepts
        would fire on a correct grounding. The guard is defined over disjointness
        precisely to avoid that.
        """
        groups = find_collisions(
            [
                GroundedExtract(key=("a",), concept="current_assets", sql=_SAME),
                GroundedExtract(key=("b",), concept="cash", sql=_SAME),
            ],
            self._DISJOINT,
        )
        assert groups == []

    def test_only_the_implicated_concepts_join_the_group(self) -> None:
        """A third concept sharing the statement without a disjoint edge stays out."""
        groups = find_collisions(
            [
                GroundedExtract(key=("a",), concept="current_assets", sql=_SAME),
                GroundedExtract(key=("b",), concept="current_liabilities", sql=_SAME),
                GroundedExtract(key=("c",), concept="cash", sql=_SAME),
            ],
            self._DISJOINT,
        )
        assert len(groups) == 1
        assert [m.concept for m in groups[0].members] == ["current_assets", "current_liabilities"]

    def test_same_concept_twice_is_not_a_collision(self) -> None:
        """Two nodes of ONE concept (different statement/aggregation) may agree."""
        groups = find_collisions(
            [
                GroundedExtract(key=("a",), concept="current_assets", sql=_SAME),
                GroundedExtract(key=("b",), concept="current_assets", sql=_SAME),
            ],
            self._DISJOINT,
        )
        assert groups == []

    def test_reason_names_the_partner(self) -> None:
        groups = find_collisions(
            [
                GroundedExtract(key=("a",), concept="current_assets", sql=_SAME),
                GroundedExtract(key=("b",), concept="current_liabilities", sql=_SAME),
            ],
            self._DISJOINT,
        )
        member = groups[0].members[0]
        first = collision_reason(groups[0], member, abstained=False)
        final = collision_reason(groups[0], member, abstained=True)
        assert "current_liabilities" in first and "current_assets" in first
        # The retry has to know WHICH concept to distinguish itself from.
        assert "current_liabilities" in final
        assert "ungrounded" in final


class TestDisjointMap:
    def test_finance_asset_liability_pair_is_covered_by_the_shipped_seed(
        self, session: Session
    ) -> None:
        """The live bug's pair carries a disjoint edge — no ontology gap to close.

        Both directions, from the ``concept_groups`` partitions alone (finance
        declares no explicit ``disjoint`` block).
        """
        ensure_concept_edges_seeded(session, "finance")
        edges = disjoint_map(session, "finance")
        assert "current_liabilities" in edges["current_assets"]
        assert "current_assets" in edges["current_liabilities"]
        # ...and a same-group pair is NOT disjoint.
        assert "cash" not in edges["current_assets"]

    def test_unseeded_vertical_has_no_disjointness(self, session: Session) -> None:
        assert disjoint_map(session, "finance") == {}


def _reauthor_stub(
    session: Session,
    nodes: dict[NodeKey, WarmNode],
    retry_sql: dict[str, str | None],
    calls: list[tuple[str, str]],
):
    """Model the real re-authoring contract: demote, then heal with the retry's SQL.

    The demotion is not incidental — a healthy snippet is assembled from cache
    with no LLM call, so a stub that skipped it would make the re-ground pass
    look green while testing nothing. ``retry_sql[concept] is None`` models a
    retry that fell loud (row stays flagged, node ungrounded).

    Deliberately does NOT flush the heal, because ``save_snippet``'s in-place
    refresh does not either: the pass has to flush before it expires, or it
    discards every repair it just triggered.
    """

    def _reauthor(key: NodeKey, reason: str) -> NodeDecision:
        node = nodes[key]
        assert node.step.source is not None
        concept = node.step.source.standard_field
        assert concept is not None
        calls.append((concept, reason))
        flag_collision(session, node, workspace_id=_WS, schema_mapping_id=_SCHEMA, reason=reason)
        sql = retry_sql.get(concept)
        if sql is None:
            return NodeDecision(grounded=False, reason="stub: fell loud")
        _save(session, concept, sql, flush=False)
        return NodeDecision(grounded=True)

    return _reauthor


class TestResolveGroundingCollisions:
    """The pass: detect → one re-ground round → typed abstention."""

    @pytest.fixture()
    def _seeded(self, session: Session) -> dict[NodeKey, WarmNode]:
        ensure_concept_edges_seeded(session, "finance")
        nodes = _nodes_for("current_assets", "current_liabilities")
        for concept in ("current_assets", "current_liabilities"):
            _save(session, concept, _SAME)
        return nodes

    def _bindings(self, nodes: dict[NodeKey, WarmNode]) -> dict[NodeKey, NodeDecision]:
        return {key: NodeDecision(grounded=True) for key in nodes}

    def test_collision_is_re_grounded_with_the_partner_named(
        self, session: Session, _seeded: dict[NodeKey, WarmNode]
    ) -> None:
        """Both members re-ground — symmetric, nothing picks a winner."""
        calls: list[tuple[str, str]] = []
        out = resolve_grounding_collisions(
            self._bindings(_seeded),
            _seeded,
            session=session,
            workspace_id=_WS,
            schema_mapping_id=_SCHEMA,
            vertical="finance",
            reauthor=_reauthor_stub(
                session,
                _seeded,
                {
                    "current_assets": _SAME.replace("enriched_gl", "enriched_assets"),
                    "current_liabilities": _SAME.replace("enriched_gl", "enriched_ap"),
                },
                calls,
            ),
        )

        assert sorted(c for c, _ in calls) == ["current_assets", "current_liabilities"]
        # Each was told which disjoint concept it collided with.
        by_concept = dict(calls)
        assert "current_liabilities" in by_concept["current_assets"]
        assert "current_assets" in by_concept["current_liabilities"]
        # Resolved: both stay grounded and both rows are healthy again.
        assert all(d.grounded for d in out.values())
        for concept in ("current_assets", "current_liabilities"):
            assert _record(session, concept).failure_count == 0

    def test_unresolved_collision_abstains_typed(
        self, session: Session, _seeded: dict[NodeKey, WarmNode]
    ) -> None:
        """The retry produced the same statement again → both concepts abstain.

        The acceptance condition: no silent identical pair persists. Both rows
        end flagged ``disjoint_collision`` (retained, excluded from reuse) and
        both nodes go ungrounded, so every metric over them honest-fails instead
        of shipping a ratio of 1.0.
        """
        calls: list[tuple[str, str]] = []
        out = resolve_grounding_collisions(
            self._bindings(_seeded),
            _seeded,
            session=session,
            workspace_id=_WS,
            schema_mapping_id=_SCHEMA,
            vertical="finance",
            reauthor=_reauthor_stub(
                session,
                _seeded,
                {"current_assets": _SAME, "current_liabilities": _SAME},
                calls,
            ),
        )

        assert len(calls) == 2, "exactly one re-ground round, both members"
        assert not any(d.grounded for d in out.values())
        for decision in out.values():
            assert decision.reason is not None
            assert "disjoint" in decision.reason

        for concept in ("current_assets", "current_liabilities"):
            record = _record(session, concept)
            assert record.failure_count == 1, "retained, but out of reuse"
            assert record.provenance is not None
            assert record.provenance["failure_mode"] == SnippetFailureMode.DISJOINT_COLLISION.value
            assert record.sql == _SAME, "the SQL is retained, not deleted"
        # No healthy row survives for either concept — the pair cannot be reused.
        library = SnippetLibrary(session, workspace_id=_WS)
        for concept in ("current_assets", "current_liabilities"):
            assert (
                library.find_by_key(
                    snippet_type="extract",
                    schema_mapping_id=_SCHEMA,
                    standard_field=concept,
                    statement="balance_sheet",
                    aggregation="sum",
                )
                is None
            )

    def test_a_retry_that_falls_loud_leaves_the_row_flagged(
        self, session: Session, _seeded: dict[NodeKey, WarmNode]
    ) -> None:
        """One side re-grounds, the other abstains — no winner is imposed.

        The surviving concept keeps its (now distinct) grounding; the one that
        could not distinguish itself stays flagged. That asymmetry is the LLM's,
        not the guard's.
        """
        calls: list[tuple[str, str]] = []
        out = resolve_grounding_collisions(
            self._bindings(_seeded),
            _seeded,
            session=session,
            workspace_id=_WS,
            schema_mapping_id=_SCHEMA,
            vertical="finance",
            reauthor=_reauthor_stub(
                session,
                _seeded,
                {
                    "current_assets": _SAME.replace("enriched_gl", "enriched_assets"),
                    "current_liabilities": None,
                },
                calls,
            ),
        )

        grounded = {
            _seeded[k].step.source.standard_field  # type: ignore[union-attr]
            for k, d in out.items()
            if d.grounded
        }
        assert grounded == {"current_assets"}
        assert _record(session, "current_assets").failure_count == 0
        assert _record(session, "current_liabilities").failure_count == 1

    def test_no_disjointness_declared_is_a_no_op(self, session: Session) -> None:
        """Nothing seeded = nothing to enforce; the guard must not invent an edge."""
        nodes = _nodes_for("current_assets", "current_liabilities")
        for concept in ("current_assets", "current_liabilities"):
            _save(session, concept, _SAME)
        calls: list[tuple[str, str]] = []

        out = resolve_grounding_collisions(
            self._bindings(nodes),
            nodes,
            session=session,
            workspace_id=_WS,
            schema_mapping_id=_SCHEMA,
            vertical="finance",
            reauthor=_reauthor_stub(session, nodes, {}, calls),
        )

        assert calls == []
        assert all(d.grounded for d in out.values())

    def test_distinct_groundings_are_untouched(self, session: Session) -> None:
        ensure_concept_edges_seeded(session, "finance")
        nodes = _nodes_for("current_assets", "current_liabilities")
        _save(session, "current_assets", _SAME)
        _save(session, "current_liabilities", _SAME.replace("enriched_gl", "enriched_ap"))
        calls: list[tuple[str, str]] = []

        out = resolve_grounding_collisions(
            self._bindings(nodes),
            nodes,
            session=session,
            workspace_id=_WS,
            schema_mapping_id=_SCHEMA,
            vertical="finance",
            reauthor=_reauthor_stub(session, nodes, {}, calls),
        )

        assert calls == []
        assert all(d.grounded for d in out.values())
