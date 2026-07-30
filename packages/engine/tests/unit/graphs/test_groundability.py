"""The deterministic ungroundable-dimension verdict (DAT-620).

Per-leg pins for :mod:`dataraum.graphs.groundability`: each leg absent → no
verdict (``groundable`` names the failing leg); all three present →
``ungroundable``; the resolving table LINKED → ``groundable`` with
``resolving_reference_linked`` (the ticket's no-false-abstention criterion);
unjudgeable → a typed abstention, never a hole. Plus the two resolution
helpers — served-name → typed column, and the DuckDB where-predicate column
parse that recovers filter columns from a retained-failure row's parts.
"""

from __future__ import annotations

from collections.abc import Iterator
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.graphs.groundability import (
    GroundabilityAbstainReason,
    GroundabilityReason,
    GroundabilityStatus,
    GroundabilityVerdict,
    evaluate_groundability,
    resolve_served_filter_columns,
    where_predicate_columns,
)
from dataraum.storage import Column, Table, init_database
from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

CAT_RUN = "cat-run"
GEN_RUN = "gen-run"


@pytest.fixture
def session() -> Iterator[Session]:
    """In-memory SQLite catalog (FKs off, the resolve-test pattern)."""
    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def _pragma(dbapi_conn, _record):  # noqa: ANN001, ANN202
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA foreign_keys=OFF")
        cur.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    try:
        with factory() as s:
            yield s
    finally:
        engine.dispose()


@pytest.fixture
def conn() -> Iterator[duckdb.DuckDBPyConnection]:
    c = duckdb.connect(":memory:")
    try:
        yield c
    finally:
        c.close()


def _table(session: Session, name: str, layer: str = "typed") -> Table:
    t = Table(table_id=str(uuid4()), source_id="src", table_name=name, layer=layer)
    session.add(t)
    return t


def _column(
    session: Session,
    table: Table,
    name: str,
    *,
    resolved_type: str = "VARCHAR",
    source_column_id: str | None = None,
    position: int = 0,
) -> Column:
    c = Column(
        column_id=str(uuid4()),
        table_id=table.table_id,
        column_name=name,
        column_position=position,
        resolved_type=resolved_type,
        source_column_id=source_column_id,
    )
    session.add(c)
    return c


@pytest.fixture
def workspace(session: Session) -> dict[str, object]:
    """A typed fact with a coded discriminator, served through an enriched view.

    The discriminator holds all three legs by default: role ``identifier`` at
    the pinned generation head, NO ColumnConcept meaning at the catalogue run,
    and no reference edge — each test then breaks exactly one leg.
    """
    fact = _table(session, "journal")
    code = _column(session, fact, "entity_code")
    view_table = _table(session, "enriched_journal", layer="enriched")
    _column(session, view_table, "entity_code", source_column_id=code.column_id)
    session.add(
        EnrichedView(
            fact_table_id=fact.table_id,
            view_table_id=view_table.table_id,
            view_name="enriched_journal",
            run_id=CAT_RUN,
        )
    )
    session.add(
        SemanticAnnotation(column_id=code.column_id, run_id=GEN_RUN, semantic_role="identifier")
    )
    session.flush()
    return {"fact": fact, "code": code, "view_table": view_table}


def _evaluate(session: Session, workspace: dict[str, object], **kwargs: object):
    fact, code = workspace["fact"], workspace["code"]
    defaults: dict[str, object] = {
        "catalogue_run_id": CAT_RUN,
        "semantic_runs": {str(fact.table_id): GEN_RUN},  # type: ignore[union-attr]
    }
    defaults.update(kwargs)
    results = evaluate_groundability(session, [(code, fact)], **defaults)  # type: ignore[arg-type]
    assert len(results) == 1
    return results[0]


def _reference_edge(
    session: Session,
    workspace: dict[str, object],
    *,
    run_id: str = CAT_RUN,
    detection_method: str = "llm",
    with_text: bool = True,
) -> Table:
    """A defined FK from the discriminator to a reference table (± non-key text)."""
    fact, code = workspace["fact"], workspace["code"]
    ref = _table(session, "entity_ref")
    key = _column(session, ref, "entity_code")
    if with_text:
        _column(session, ref, "entity_name", position=1)
    session.add(
        Relationship(
            run_id=run_id,
            from_table_id=fact.table_id,  # type: ignore[union-attr]
            from_column_id=code.column_id,  # type: ignore[union-attr]
            to_table_id=ref.table_id,
            to_column_id=key.column_id,
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.95,
            detection_method=detection_method,
            confirmation_source="judge",
        )
    )
    session.flush()
    return ref


class TestTrigger:
    def test_all_three_legs_hold_is_ungroundable(self, session, workspace) -> None:
        v = _evaluate(session, workspace)
        assert v.status is GroundabilityStatus.CLASSIFIED
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE
        assert v.reason is GroundabilityReason.NO_RESOLVING_REFERENCE
        # The evidence names the column, its table, and the generic cure — and
        # claims only what leg 3 measured: no CONFIRMED relationship, not
        # "nothing in the workspace" (an unconfirmed candidate may exist).
        assert "entity_code" in v.evidence()
        assert "journal" in v.evidence()
        assert "no confirmed relationship" in v.evidence()
        assert "link a reference/lookup table" in v.evidence()
        assert "or confirm the relationship" in v.evidence()

    def test_resolved_meaning_breaks_the_trigger(self, session, workspace) -> None:
        code = workspace["code"]
        session.add(
            ColumnConcept(
                column_id=code.column_id,
                run_id=CAT_RUN,
                meaning="the ledger entity code, resolved by the served hierarchy",
                meaning_status="determined",
            )
        )
        session.flush()
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.GROUNDABLE
        assert v.reason is GroundabilityReason.MEANING_RESOLVED

    def test_ambiguous_meaning_is_the_agents_abstention_and_still_triggers(
        self, session, workspace
    ) -> None:
        """DAT-823 declared ignorance: a meaning PRESENT with status 'ambiguous'
        is the catalogue agent saying the values stay undetermined — opacity holds."""
        code = workspace["code"]
        session.add(
            ColumnConcept(
                column_id=code.column_id,
                run_id=CAT_RUN,
                meaning="coded values; what they denote is undetermined",
                meaning_status="ambiguous",
            )
        )
        session.flush()
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE

    def test_meaning_at_another_run_does_not_count(self, session, workspace) -> None:
        code = workspace["code"]
        session.add(
            ColumnConcept(
                column_id=code.column_id,
                run_id="other-run",
                meaning="a meaning sealed under some other catalogue head",
                meaning_status="determined",
            )
        )
        session.flush()
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE

    def test_non_identifier_role_breaks_the_trigger(self, session, workspace) -> None:
        """A plain-word categorical (role 'dimension') is not a coded discriminator."""
        session.query(SemanticAnnotation).delete()
        code = workspace["code"]
        session.add(
            SemanticAnnotation(column_id=code.column_id, run_id=GEN_RUN, semantic_role="dimension")
        )
        session.flush()
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.GROUNDABLE
        assert v.reason is GroundabilityReason.NOT_CODED_DISCRIMINATOR


class TestResolvingReference:
    def test_linked_reference_with_text_is_groundable(self, session, workspace) -> None:
        """The no-false-abstention criterion: the resolving table IS linked →
        negative verdict, grounding proceeds untouched."""
        _reference_edge(session, workspace)
        v = _evaluate(session, workspace)
        assert v.status is GroundabilityStatus.CLASSIFIED
        assert v.verdict is GroundabilityVerdict.GROUNDABLE
        assert v.reason is GroundabilityReason.RESOLVING_REFERENCE_LINKED

    def test_reference_without_nonkey_text_does_not_resolve(self, session, workspace) -> None:
        """An FK to a table that is nothing but the key carries no resolution."""
        _reference_edge(session, workspace, with_text=False)
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE

    def test_candidate_edge_does_not_resolve(self, session, workspace) -> None:
        """og_references membership: a structural candidate is not a defined reference."""
        _reference_edge(session, workspace, detection_method="candidate")
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE

    def test_candidate_link_to_text_bearing_table_is_disclosed(self, session, workspace) -> None:
        """The settled candidate policy: never a resolution (candidate confidence
        is overlap noise, judge-declined pairs stay candidate), but DISCLOSED —
        the evidence routes the user to CONFIRM the link, not re-upload."""
        _reference_edge(session, workspace, detection_method="candidate")
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE
        assert v.candidate_links == 1
        assert "1 unconfirmed candidate link(s) exist" in v.evidence()
        assert "confirming one may resolve these values" in v.evidence()

    def test_candidate_link_without_text_is_not_disclosed(self, session, workspace) -> None:
        """A candidate edge to a table with nothing but the key carries no
        potential resolution — nothing to route the user to."""
        _reference_edge(session, workspace, detection_method="candidate", with_text=False)
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE
        assert v.candidate_links == 0
        assert "unconfirmed candidate" not in v.evidence()

    def test_edge_at_another_run_does_not_resolve(self, session, workspace) -> None:
        _reference_edge(session, workspace, run_id="other-run")
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE

    def test_parent_side_key_resolved_by_own_table_text(self, session, workspace) -> None:
        """A column that IS a reference table's key: its own non-key text siblings
        are the resolution (the fact pointing at it is the edge)."""
        fact, code = workspace["fact"], workspace["code"]
        _column(session, fact, "entity_name", position=1)
        child = _table(session, "movements")
        child_code = _column(session, child, "entity_code")
        session.add(
            Relationship(
                run_id=CAT_RUN,
                from_table_id=child.table_id,
                from_column_id=child_code.column_id,
                to_table_id=fact.table_id,
                to_column_id=code.column_id,
                relationship_type="foreign_key",
                cardinality="many-to-one",
                confidence=0.9,
                detection_method="llm",
                confirmation_source="judge",
            )
        )
        session.flush()
        v = _evaluate(session, workspace)
        assert v.verdict is GroundabilityVerdict.GROUNDABLE
        assert v.reason is GroundabilityReason.RESOLVING_REFERENCE_LINKED


class TestAbstention:
    def test_no_annotation_abstains(self, session, workspace) -> None:
        session.query(SemanticAnnotation).delete()
        session.flush()
        v = _evaluate(session, workspace)
        assert v.status is GroundabilityStatus.ABSTAINED
        assert v.abstain_reason is GroundabilityAbstainReason.NO_SEMANTIC_ANNOTATION
        assert v.verdict is None and v.reason is None

    def test_annotation_off_the_pinned_head_reads_as_absent(self, session, workspace) -> None:
        v = _evaluate(session, workspace, semantic_runs={str(workspace["fact"].table_id): "other"})
        assert v.status is GroundabilityStatus.ABSTAINED
        assert v.abstain_reason is GroundabilityAbstainReason.NO_SEMANTIC_ANNOTATION

    def test_no_catalogue_run_abstains(self, session, workspace) -> None:
        v = _evaluate(session, workspace, catalogue_run_id=None)
        assert v.status is GroundabilityStatus.ABSTAINED
        assert v.abstain_reason is GroundabilityAbstainReason.NO_CATALOGUE_RUN

    def test_head_self_resolution_matches_the_pinned_map(self, session, workspace) -> None:
        """The classification seam passes no pinned map — the evaluator resolves
        the same promoted generation head ``BaseRunMap`` is built from."""
        session.add(
            MetadataSnapshotHead(
                target=f"table:{workspace['fact'].table_id}",
                stage=GENERATION_STAGE,
                run_id=GEN_RUN,
            )
        )
        session.flush()
        v = _evaluate(session, workspace, semantic_runs=None)
        assert v.status is GroundabilityStatus.CLASSIFIED
        assert v.verdict is GroundabilityVerdict.UNGROUNDABLE


class TestServedResolution:
    def test_served_name_resolves_to_the_typed_column(self, session, workspace) -> None:
        resolved = resolve_served_filter_columns(session, "enriched_journal", {"entity_code"})
        assert set(resolved) == {"entity_code"}
        col, table = resolved["entity_code"]
        assert col.column_id == workspace["code"].column_id
        assert table.table_id == workspace["fact"].table_id

    def test_unknown_relation_resolves_nothing(self, session, workspace) -> None:
        assert resolve_served_filter_columns(session, "not_a_view", {"entity_code"}) == {}

    def test_served_column_without_typed_source_is_skipped(self, session, workspace) -> None:
        view_table = workspace["view_table"]
        _column(session, view_table, "minted", source_column_id=None, position=1)
        session.flush()
        assert resolve_served_filter_columns(session, "enriched_journal", {"minted"}) == {}


class TestWherePredicateColumns:
    def test_in_list_and_qualified_references(self, conn) -> None:
        cols = where_predicate_columns(
            ["\"entity_code\" IN ('4000', '5000')", "x = 1 AND t.qualified_col > 2"], conn
        )
        assert cols == {"entity_code", "x", "qualified_col"}

    def test_unparseable_predicate_contributes_nothing(self, conn) -> None:
        assert where_predicate_columns(["NOT ) VALID ("], conn) == set()

    def test_empty_and_blank_parts_yield_nothing(self, conn) -> None:
        assert where_predicate_columns([], conn) == set()
        assert where_predicate_columns(["", "   "], conn) == set()
