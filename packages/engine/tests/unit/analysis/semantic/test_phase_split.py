"""Unit tests for the DAT-362 semantic phase split (per-column + per-table).

Covers the new processor entry points and the per-table parse/format helpers,
without invoking a live LLM (the agent is faked where needed).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy import select

from dataraum.analysis.relationships.db_models import Relationship as RelationshipDB
from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.db_models import ColumnConcept as ColumnConceptDB
from dataraum.analysis.semantic.db_models import SemanticAnnotation as AnnotationDB
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    ColumnConceptOutput,
    ColumnSemanticOutput,
    EntityDetection,
    IdentityColumn,
    Relationship,
    SemanticEnrichmentResult,
    TableColumnAnnotation,
    TableSynthesisOutput,
    TimeColumn,
)
from dataraum.analysis.semantic.processor import (
    persist_column_annotations,
    persist_column_concepts,
    synthesize_and_store_tables,
)
from dataraum.core.models.base import RelationshipType, Result
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_run_id

# A minimal resolvable meaning entry (orders.order_id exists in every relationship
# fixture below) so the DAT-768/769 empty-surface gate — not under test in the
# relationship flows — stays quiet.
_MEANING_MIN = [
    ColumnConceptOutput(table_name="orders", column_name="order_id", meaning="test meaning")
]


def _table_with_columns(session, name: str, columns: list[str]) -> Table:
    src = Source(name=f"src_{name}", source_type="csv")
    session.add(src)
    session.flush()
    table = Table(source_id=src.source_id, table_name=name, layer="raw", row_count=10)
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


def _col(name: str, role: str, **kw) -> ColumnSemanticOutput:
    # Object-grain only (DAT-637): business_concept / unit_source_column /
    # derived_formula moved to the table agent's ColumnConceptOutput.
    return ColumnSemanticOutput(
        column_name=name,
        semantic_role=role,
        entity_type=kw.get("entity_type", f"{name}_entity"),
        business_term=kw.get("business_term", name.title()),
        description=kw.get("description", f"{name} column"),
        confidence=kw.get("confidence", 0.9),
        temporal_behavior_claim=kw.get("temporal_behavior_claim", "unsure"),
        temporal_behavior_claim_confidence=kw.get("temporal_behavior_claim_confidence", 0.0),
    )


# ---------------------------------------------------------------------------
# persist_column_annotations
# ---------------------------------------------------------------------------


class TestPersistColumnAnnotations:
    def test_persists_one_row_per_resolvable_column(self, session) -> None:
        table = _table_with_columns(session, "customers", ["customer_id", "revenue"])
        output = ColumnAnnotationOutput(
            tables=[
                TableColumnAnnotation(
                    table_name="customers",
                    columns=[
                        _col("customer_id", "key"),
                        _col("revenue", "measure"),
                    ],
                )
            ]
        )

        count = persist_column_annotations(
            session,
            output,
            [table.table_id],
            annotated_by="test-model",
            run_id=baseline_run_id(),
        )
        session.flush()

        rows = session.execute(select(AnnotationDB)).scalars().all()
        assert count == 2
        assert len(rows) == 2
        by_role = {r.semantic_role: r for r in rows}
        # Object-grain fields only — catalogue-grain (business_concept, unit
        # source) is the table agent's ColumnConcept, not this writer (DAT-637).
        assert by_role["key"].business_name == "Customer_Id"
        assert by_role["measure"].entity_type == "revenue_entity"
        assert all(r.annotation_source == "llm" and r.annotated_by == "test-model" for r in rows)

    def test_skips_columns_not_in_the_table(self, session) -> None:
        table = _table_with_columns(session, "orders", ["order_id"])
        output = ColumnAnnotationOutput(
            tables=[
                TableColumnAnnotation(
                    table_name="orders",
                    columns=[_col("order_id", "key"), _col("ghost_col", "attribute")],
                )
            ]
        )

        count = persist_column_annotations(
            session,
            output,
            [table.table_id],
            annotated_by="m",
            run_id=baseline_run_id(),
        )
        assert count == 1


class TestPersistColumnConcepts:
    """The catalogue-grain authoring the table agent owns (DAT-637)."""

    def test_persists_concept_unit_and_normalizes_formula(self, session) -> None:
        """meaning / hints / unit source / derived-formula land on ColumnConcept.

        Whitespace-only hypotheses normalize to None so the detector's
        truthiness read ("no hypothesis → witness abstains") holds.
        """
        table = _table_with_columns(session, "orders", ["total", "discount"])
        concepts = [
            ColumnConceptOutput(
                table_name="orders",
                column_name="total",
                meaning="Order total including tax",
                ontology_hints=["revenue"],
                unit_source_column="currency_code",
                derived_formula_hypothesis="subtotal + tax",
                derived_formula_confidence=0.85,
            ),
            ColumnConceptOutput(
                table_name="orders",
                column_name="discount",
                meaning="Per-order discount amount",
                derived_formula_hypothesis="   ",
            ),
        ]

        result = persist_column_concepts(
            session,
            concepts,
            [table.table_id],
            annotated_by="m",
            run_id=baseline_run_id(),
        )
        session.flush()

        assert result.resolved == 2
        assert result.emitted == 2
        assert result.dropped_unresolved == 0
        rows = {r.column_id: r for r in session.execute(select(ColumnConceptDB)).scalars()}
        cols = {c.column_name: c.column_id for c in session.execute(select(Column)).scalars()}
        total = rows[cols["total"]]
        assert total.meaning == "Order total including tax"
        assert total.ontology_hints == ["revenue"]
        assert total.unit_source_column == "currency_code"
        assert total.derived_formula_hypothesis == "subtotal + tax"
        assert total.derived_formula_confidence == 0.85
        assert rows[cols["discount"]].derived_formula_hypothesis is None

    def test_duplicate_column_concepts_collapse_to_one_row(self, session) -> None:
        """The table agent can list the same column twice; the upsert batch must dedup.

        Two ColumnConceptOutput for the same (table, column) share the (column_id,
        run_id) upsert key — without dedup Postgres raises CardinalityViolation
        ("ON CONFLICT cannot affect a row twice"). Last mention wins.
        """
        table = _table_with_columns(session, "orders", ["total"])
        concepts = [
            ColumnConceptOutput(table_name="orders", column_name="total", meaning="gross"),
            ColumnConceptOutput(table_name="orders", column_name="total", meaning="net"),
        ]

        result = persist_column_concepts(
            session, concepts, [table.table_id], annotated_by="m", run_id=baseline_run_id()
        )
        session.flush()

        assert result.resolved == 1  # collapsed
        assert result.emitted == 2  # both mentions counted as emitted
        rows = list(session.execute(select(ColumnConceptDB)).scalars())
        assert len(rows) == 1
        assert rows[0].meaning == "net"  # last mention wins

    def test_unresolvable_concept_dropped_and_counted(self, session) -> None:
        """DAT-768 path #2: a concept whose (table, column) name resolves to no column
        is dropped, and the breakdown surfaces it (resolved 0, dropped 1) instead of
        being indistinguishable from an empty emission."""
        table = _table_with_columns(session, "orders", ["total"])
        concepts = [
            ColumnConceptOutput(table_name="orders", column_name="ghost", meaning="phantom")
        ]

        result = persist_column_concepts(
            session, concepts, [table.table_id], annotated_by="m", run_id=baseline_run_id()
        )
        session.flush()

        assert result.emitted == 1
        assert result.resolved == 0
        assert result.dropped_unresolved == 1
        assert list(session.execute(select(ColumnConceptDB)).scalars()) == []


class TestNearConstantFeed:
    """The per-table feed flags near-constant columns (DAT-637 quality fix) so the
    table agent refuses to bind a concept to a status flag."""

    @staticmethod
    def _profile(name: str, top: list[tuple[object, int]]):
        from datetime import UTC, datetime

        from dataraum.analysis.statistics.models import ColumnProfile, ValueCount
        from dataraum.core.models.base import ColumnRef

        total = sum(c for _v, c in top)
        return ColumnProfile(
            column_id=name,
            column_ref=ColumnRef(table_name="t", column_name=name),
            profiled_at=datetime.now(UTC),
            total_count=total,
            null_count=0,
            distinct_count=len(top),
            null_ratio=0.0,
            cardinality_ratio=len(top) / total,
            top_values=[ValueCount(value=v, count=c, percentage=100 * c / total) for v, c in top],
        )

    def test_dominant_value_flagged_balanced_column_not(self) -> None:
        agent = SemanticAgent.__new__(SemanticAgent)
        profiles = [
            self._profile("flag", [(True, 99), (False, 1)]),  # 99% → near-constant
            self._profile("region", [("a", 40), ("b", 35), ("c", 25)]),  # balanced
        ]
        cols = {c["column_name"]: c for c in agent._build_tables_json(profiles, {})[0]["columns"]}
        assert cols["flag"].get("near_constant") is True
        assert "near_constant" not in cols["region"]


# ---------------------------------------------------------------------------
# synthesize_and_store_tables
# ---------------------------------------------------------------------------


class TestSynthesizeAndStoreTables:
    def test_stores_entities_and_relationships_no_annotations(self, session) -> None:
        orders = _table_with_columns(session, "orders", ["order_id", "customer_id"])
        customers = _table_with_columns(session, "customers", ["id"])

        agent = MagicMock()
        agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
                    column_concepts=_MEANING_MIN,
                    entity_detections=[
                        EntityDetection(
                            table_id="",
                            table_name="orders",
                            entity_type="orders",
                            confidence=0.9,
                            grain_columns=["order_id"],
                            table_role="fact",
                            time_columns=[
                                TimeColumn(column="order_date", aspect="order", note="Placed."),
                                TimeColumn(column="ship_date", aspect="ship", note="Shipped."),
                            ],
                            identity_columns=[
                                IdentityColumn(column="customer_id", note="Buying account.")
                            ],
                        )
                    ],
                    relationships=[
                        Relationship(
                            relationship_id="rel-1",
                            from_table="orders",
                            from_column="customer_id",
                            to_table="customers",
                            to_column="id",
                            relationship_type=RelationshipType.FOREIGN_KEY,
                            confidence=0.9,
                            detection_method="llm_tool",
                            evidence={"source": "table_synthesis"},
                        )
                    ],
                )
            )
        )

        result = synthesize_and_store_tables(
            session,
            agent,
            [orders.table_id, customers.table_id],
            run_id=baseline_run_id(),
        )
        session.flush()

        assert result.success
        entities = session.execute(select(TableEntity)).scalars().all()
        rels = (
            session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
            .scalars()
            .all()
        )
        anns = session.execute(select(AnnotationDB)).scalars().all()
        assert len(entities) == 1 and entities[0].table_role == "fact"
        # DAT-565: all event-time axes + identities persisted run-versioned (JSON).
        assert [tc["column"] for tc in entities[0].time_columns] == ["order_date", "ship_date"]
        assert entities[0].time_columns[1]["aspect"] == "ship"
        assert [ic["column"] for ic in entities[0].identity_columns] == ["customer_id"]
        assert len(rels) == 1 and rels[0].cardinality is None  # no duckdb → unresolved
        assert anns == []  # per-table synthesis never writes column annotations

    def test_declined_relationship_persists_as_candidate_not_llm(self, session) -> None:
        """DAT-699 follow-up: a judge-DECLINED relationship (confidence below the
        judge's own decision boundary, REL_CONFIRM_MIN) is persisted as
        ``candidate`` with its evidence/reasoning kept — NOT as ``llm`` — so it
        never enters the "defined" catalog (``detection_method != 'candidate'``)
        that every downstream consumer reads. Cuts declines at the source instead
        of making each consumer re-weigh confidence.
        """
        orders = _table_with_columns(session, "orders", ["order_id", "customer_id"])
        customers = _table_with_columns(session, "customers", ["id"])

        agent = MagicMock()
        agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
                    column_concepts=_MEANING_MIN,
                    entity_detections=[],
                    relationships=[
                        Relationship(
                            relationship_id="rel-accept",
                            from_table="orders",
                            from_column="customer_id",
                            to_table="customers",
                            to_column="id",
                            relationship_type=RelationshipType.FOREIGN_KEY,
                            confidence=0.9,  # >= 0.7 → confirmed (defined)
                            detection_method="llm_tool",
                            evidence={"source": "table_synthesis", "reasoning": "clean FK"},
                        ),
                        Relationship(
                            relationship_id="rel-decline",
                            from_table="orders",
                            from_column="order_id",
                            to_table="customers",
                            to_column="id",
                            relationship_type=RelationshipType.FOREIGN_KEY,
                            confidence=0.3,  # < 0.7 → judge declined
                            detection_method="llm_tool",
                            evidence={
                                "source": "table_synthesis",
                                "reasoning": "coincidental overlap; decline",
                            },
                        ),
                    ],
                )
            )
        )

        result = synthesize_and_store_tables(
            session, agent, [orders.table_id, customers.table_id], run_id=baseline_run_id()
        )
        session.flush()
        assert result.success

        llm_rels = (
            session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
            .scalars()
            .all()
        )
        cand_rels = (
            session.execute(
                select(RelationshipDB).where(RelationshipDB.detection_method == "candidate")
            )
            .scalars()
            .all()
        )
        # Only the accepted FK is "defined" (llm); the declined one is a candidate.
        assert len(llm_rels) == 1 and llm_rels[0].confidence == 0.9
        assert len(cand_rels) == 1 and cand_rels[0].confidence == 0.3
        # The judge's reasoning is preserved on the candidate row.
        assert (cand_rels[0].evidence or {}).get("reasoning") == "coincidental overlap; decline"

    def test_declined_composite_does_not_become_confirmed_intent(self, session) -> None:
        """A composite (``key_columns``) the judge did NOT confirm (confidence below
        REL_CONFIRM_MIN) must not slip into the "defined" catalog via the surrogate-
        intent → mint path (DAT-722). It falls through to the gated single-column
        persist (→ ``candidate``), like any other declined verdict — no confirmed
        intent, no ``llm`` row.
        """
        from dataraum.analysis.relationships.db_models import SurrogateKeyIntent

        orders = _table_with_columns(session, "orders", ["order_id", "customer_id"])
        customers = _table_with_columns(session, "customers", ["id"])

        agent = MagicMock()
        agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
                    column_concepts=_MEANING_MIN,
                    entity_detections=[],
                    relationships=[
                        Relationship(
                            relationship_id="rel-composite-decline",
                            from_table="orders",
                            from_column="customer_id",
                            to_table="customers",
                            to_column="id",
                            key_columns=[("order_id", "id")],  # composite proposal
                            relationship_type=RelationshipType.FOREIGN_KEY,
                            confidence=0.3,  # < 0.7 → judge declined
                            detection_method="llm_tool",
                            evidence={"source": "table_synthesis", "reasoning": "weak; decline"},
                        )
                    ],
                )
            )
        )

        result = synthesize_and_store_tables(
            session, agent, [orders.table_id, customers.table_id], run_id=baseline_run_id()
        )
        session.flush()
        assert result.success

        confirmed_intents = [
            i
            for i in session.execute(select(SurrogateKeyIntent)).scalars().all()
            if i.status == "confirmed"
        ]
        llm_rels = (
            session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
            .scalars()
            .all()
        )
        cand_rels = (
            session.execute(
                select(RelationshipDB).where(RelationshipDB.detection_method == "candidate")
            )
            .scalars()
            .all()
        )
        assert confirmed_intents == []
        assert llm_rels == []
        assert len(cand_rels) == 1 and cand_rels[0].confidence == 0.3

    def test_declined_relationship_merges_onto_structural_candidate(self, session) -> None:
        """A declined semantic rel upserts onto the PRE-EXISTING structural
        ``candidate`` row for the same oriented pair (DAT-722) — replacing it with
        the judge's confidence/reasoning, one row not two, still not "defined".
        """
        orders = _table_with_columns(session, "orders", ["order_id", "customer_id"])
        customers = _table_with_columns(session, "customers", ["id"])
        cust_col = session.execute(
            select(Column).where(
                Column.table_id == orders.table_id, Column.column_name == "customer_id"
            )
        ).scalar_one()
        id_col = session.execute(
            select(Column).where(Column.table_id == customers.table_id, Column.column_name == "id")
        ).scalar_one()
        # The structural detector's prior candidate row for this pair.
        session.add(
            RelationshipDB(
                run_id=baseline_run_id(),
                from_table_id=orders.table_id,
                from_column_id=cust_col.column_id,
                to_table_id=customers.table_id,
                to_column_id=id_col.column_id,
                relationship_type="candidate",
                cardinality=None,
                confidence=1.0,
                detection_method="candidate",
                evidence={"source": "structural"},
            )
        )
        session.flush()

        agent = MagicMock()
        agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
                    column_concepts=_MEANING_MIN,
                    entity_detections=[],
                    relationships=[
                        Relationship(
                            relationship_id="rel-decline",
                            from_table="orders",
                            from_column="customer_id",
                            to_table="customers",
                            to_column="id",
                            relationship_type=RelationshipType.FOREIGN_KEY,
                            confidence=0.3,  # < 0.7 → judge declined
                            detection_method="llm_tool",
                            evidence={"source": "table_synthesis", "reasoning": "coincidental"},
                        )
                    ],
                )
            )
        )

        result = synthesize_and_store_tables(
            session, agent, [orders.table_id, customers.table_id], run_id=baseline_run_id()
        )
        session.flush()
        assert result.success

        cand_rels = (
            session.execute(
                select(RelationshipDB).where(RelationshipDB.detection_method == "candidate")
            )
            .scalars()
            .all()
        )
        # One merged candidate row (not two), now carrying the judge's verdict.
        assert len(cand_rels) == 1
        assert cand_rels[0].confidence == 0.3
        assert (cand_rels[0].evidence or {}).get("reasoning") == "coincidental"
        assert (
            session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
            .scalars()
            .all()
            == []
        )

    def test_synthesized_relationship_gets_fan_trap_flag_from_data(self, session) -> None:
        """Regression: a synthesized (table_synthesis) relationship with NO structural
        candidate must still get introduces_duplicates computed EMPIRICALLY from the lake.

        The DAT-362 semantic split rebuilt this path to recompute cardinality + RI from
        data but dropped the duplicate-introduction check, so synthesized relationships
        carried a NULL fan-trap flag — both SQL agents' fan-out cautions then read a dead
        flag and a many-to-many join silently double-counts (the gross_margin smoke).
        """
        import duckdb

        orders = _table_with_columns(session, "orders", ["order_id", "customer_id"])
        customers = _table_with_columns(session, "customers", ["id"])

        conn = duckdb.connect()
        conn.execute("ATTACH ':memory:' AS lake")
        conn.execute("CREATE SCHEMA lake.typed")
        conn.execute("CREATE TABLE lake.typed.orders (order_id INTEGER, customer_id INTEGER)")
        conn.execute("INSERT INTO lake.typed.orders VALUES (1, 100), (2, 100)")
        # customer 100 recurs THREE times → joining fans out (2 rows → 6): a fan trap.
        conn.execute("CREATE TABLE lake.typed.customers (id INTEGER)")
        conn.execute("INSERT INTO lake.typed.customers VALUES (100), (100), (100)")

        agent = MagicMock()
        agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
                    column_concepts=_MEANING_MIN,
                    entity_detections=[],
                    relationships=[
                        Relationship(
                            relationship_id="rel-1",
                            from_table="orders",
                            from_column="customer_id",
                            to_table="customers",
                            to_column="id",
                            relationship_type=RelationshipType.FOREIGN_KEY,
                            confidence=0.9,
                            detection_method="llm_tool",
                            evidence={"source": "table_synthesis"},  # no candidate flag
                        )
                    ],
                )
            )
        )

        result = synthesize_and_store_tables(
            session,
            agent,
            [orders.table_id, customers.table_id],
            duckdb_conn=conn,
            run_id=baseline_run_id(),
        )
        session.flush()

        assert result.success
        rel = (
            session.execute(select(RelationshipDB).where(RelationshipDB.detection_method == "llm"))
            .scalars()
            .one()
        )
        assert rel.evidence["introduces_duplicates"] is True

    @staticmethod
    def _agent() -> MagicMock:
        """An agent that always classifies `orders` and confirms the orders→customers FK."""
        agent = MagicMock()
        agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
                    column_concepts=_MEANING_MIN,
                    entity_detections=[
                        EntityDetection(
                            table_id="",
                            table_name="orders",
                            entity_type="orders",
                            confidence=0.9,
                            grain_columns=["order_id"],
                            table_role="fact",
                            time_columns=[
                                TimeColumn(column="order_date", aspect="order", note="Placed."),
                                TimeColumn(column="ship_date", aspect="ship", note="Shipped."),
                            ],
                            identity_columns=[
                                IdentityColumn(column="customer_id", note="Buying account.")
                            ],
                        )
                    ],
                    relationships=[
                        Relationship(
                            relationship_id="rel-1",
                            from_table="orders",
                            from_column="customer_id",
                            to_table="customers",
                            to_column="id",
                            relationship_type=RelationshipType.FOREIGN_KEY,
                            confidence=0.9,
                            detection_method="llm_tool",
                            evidence={"source": "table_synthesis"},
                        )
                    ],
                )
            )
        )
        return agent

    def test_rerun_is_run_versioned_and_idempotent(self, session) -> None:
        """A re-run is non-destructive for entities (run-versioned) + idempotent for llm.

        DAT-408: a session has MANY runs. Both `TableEntity` and the `llm` relationship
        are versioned by `run_id`, so a new run COEXISTS with earlier runs
        (non-destructive); a same-run retry is a no-op — the entity via a run-scoped
        delete-before-insert, the relationship via an upsert on the
        `(session, run_id, from, to, method)` key.
        """
        orders = _table_with_columns(session, "orders", ["order_id", "customer_id"])
        customers = _table_with_columns(session, "customers", ["id"])
        tids = [orders.table_id, customers.table_id]

        def run(run_id: str) -> None:
            assert synthesize_and_store_tables(session, self._agent(), tids, run_id=run_id).success
            session.flush()

        def counts() -> tuple[int, int]:
            ents = session.execute(select(TableEntity)).scalars().all()
            rels = (
                session.execute(
                    select(RelationshipDB).where(RelationshipDB.detection_method == "llm")
                )
                .scalars()
                .all()
            )
            return len(ents), len(rels)

        run("run-A")
        assert counts() == (1, 1)

        run("run-A")  # Temporal at-least-once retry: same run_id → idempotent.
        assert counts() == (1, 1), "a same-run retry must not duplicate"

        run("run-B")  # A second run in the SAME session.
        ent_runs = {e.run_id for e in session.execute(select(TableEntity)).scalars()}
        rel_runs = {
            r.run_id
            for r in session.execute(
                select(RelationshipDB).where(RelationshipDB.detection_method == "llm")
            ).scalars()
        }
        # Both entity AND llm are run-versioned (DAT-408): run-B's rows coexist with
        # run-A's, non-destructive. The seal/head names which run is current.
        assert counts() == (2, 2), "run-B's rows coexist with run-A's (non-destructive)"
        assert ent_runs == {"run-A", "run-B"} and rel_runs == {"run-A", "run-B"}

    def test_propagates_agent_failure(self, session) -> None:
        agent = MagicMock()
        agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
        agent.synthesize_tables = MagicMock(return_value=Result.fail("LLM down"))

        result = synthesize_and_store_tables(session, agent, ["t1"], run_id=baseline_run_id())
        assert not result.success
        assert "LLM down" in (result.error or "")

    @staticmethod
    def _agent_returning_empty_concepts() -> MagicMock:
        agent = MagicMock()
        agent.provider.get_model_for_tier = MagicMock(return_value="test-model")
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[], entity_detections=[], relationships=[], column_concepts=[]
                )
            )
        )
        return agent

    @staticmethod
    def _annotate(session, table, column: str, role: str) -> None:
        persist_column_annotations(
            session,
            ColumnAnnotationOutput(
                tables=[
                    TableColumnAnnotation(table_name=table.table_name, columns=[_col(column, role)])
                ]
            ),
            [table.table_id],
            annotated_by="m",
            run_id=baseline_run_id(),
        )
        session.flush()

    def test_empty_concepts_fails_loud(self, session) -> None:
        """DAT-768/769: zero resolved column_concepts for a non-empty schema is an
        emptied grounding surface — every column carries a meaning by contract, so
        emptiness is never a judgment. begin_session fails loud."""
        tbl = _table_with_columns(session, "trial_balance", ["debit_balance"])
        self._annotate(session, tbl, "debit_balance", "measure")

        result = synthesize_and_store_tables(
            session,
            self._agent_returning_empty_concepts(),
            [tbl.table_id],
            run_id=baseline_run_id(),
        )

        assert not result.success
        assert "resolved to zero rows" in (result.error or "")
        assert "DAT-768" in (result.error or "")

    def test_empty_concepts_fails_loud_without_measures_too(self, session) -> None:
        """The gate is blanket under the meaning contract (DAT-769) — a
        dimension-only batch still carries meanings, so emptiness fails there too
        (the old gate was measure-conditional)."""
        tbl = _table_with_columns(session, "regions", ["region_name"])
        self._annotate(session, tbl, "region_name", "dimension")

        result = synthesize_and_store_tables(
            session,
            self._agent_returning_empty_concepts(),
            [tbl.table_id],
            run_id=baseline_run_id(),
        )

        assert not result.success
        assert "resolved to zero rows" in (result.error or "")


# ---------------------------------------------------------------------------
# per-table parse / format helpers
# ---------------------------------------------------------------------------


class TestTableSynthesisHelpers:
    def test_build_enrichment_result_yields_no_annotations(self) -> None:
        agent = SemanticAgent.__new__(SemanticAgent)  # no LLM init needed
        # Validation (with the DAT-710 repair turn on failure) is the call site's
        # job; _build_enrichment_result transforms an already-validated output.
        synthesis = TableSynthesisOutput.model_validate(
            {
                "tables": [
                    {
                        "table_name": "orders",
                        "entity_type": "orders",
                        "description": "orders",
                        "is_fact_table": True,
                        "grain": ["order_id"],
                    }
                ],
                "relationships": [],
                "column_concepts": [],
            }
        )
        result = agent._build_enrichment_result(synthesis)
        enrichment = result.unwrap()
        assert enrichment.annotations == []
        assert len(enrichment.entity_detections) == 1
        assert enrichment.entity_detections[0].table_role == "fact"

    def test_format_persisted_annotations_groups_by_table(self) -> None:
        formatted = SemanticAgent._format_persisted_annotations(
            [
                {
                    "table_name": "orders",
                    "column_name": "order_id",
                    "semantic_role": "key",
                    "business_concept": None,
                    "entity_type": "order",
                    "confidence": 0.95,
                }
            ]
        )
        assert "### orders" in formatted
        assert "order_id" in formatted and "role=key" in formatted

    def test_format_persisted_annotations_empty(self) -> None:
        assert "No prior column annotations" in SemanticAgent._format_persisted_annotations([])
