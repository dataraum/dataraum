"""Unit tests for the DAT-362 semantic phase split (per-column + per-table).

Covers the new processor entry points and the per-table parse/format helpers,
without invoking a live LLM (the agent is faked where needed).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy import select

from dataraum.analysis.relationships.db_models import Relationship as RelationshipDB
from dataraum.analysis.semantic.agent import SemanticAgent
from dataraum.analysis.semantic.db_models import SemanticAnnotation as AnnotationDB
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.semantic.models import (
    ColumnAnnotationOutput,
    ColumnSemanticOutput,
    EntityDetection,
    IdentityColumn,
    Relationship,
    SemanticEnrichmentResult,
    TableColumnAnnotation,
    TimeColumn,
)
from dataraum.analysis.semantic.processor import (
    persist_column_annotations,
    synthesize_and_store_tables,
)
from dataraum.core.models.base import RelationshipType, Result
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_run_id


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
    return ColumnSemanticOutput(
        column_name=name,
        semantic_role=role,
        entity_type=kw.get("entity_type", f"{name}_entity"),
        business_term=kw.get("business_term", name.title()),
        business_concept=kw.get("business_concept"),
        description=kw.get("description", f"{name} column"),
        confidence=kw.get("confidence", 0.9),
        unit_source_column=kw.get("unit_source_column"),
        temporal_behavior_claim=kw.get("temporal_behavior_claim", "unsure"),
        temporal_behavior_claim_confidence=kw.get("temporal_behavior_claim_confidence", 0.0),
        derived_formula_hypothesis=kw.get("derived_formula_hypothesis"),
        derived_formula_confidence=kw.get("derived_formula_confidence", 0.0),
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
                        _col("customer_id", "key", business_concept="customer"),
                        _col("revenue", "measure", unit_source_column="currency_code"),
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
        assert by_role["key"].business_concept == "customer"
        assert by_role["measure"].unit_source_column == "currency_code"
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

    def test_persists_derived_formula_hypothesis(self, session) -> None:
        """The formula-hypothesis witness lands on the annotation row (ADR-0009).

        Whitespace-only hypotheses normalize to None so the detector's
        truthiness read ("no hypothesis → witness abstains") holds.
        """
        table = _table_with_columns(session, "orders", ["total", "discount"])
        output = ColumnAnnotationOutput(
            tables=[
                TableColumnAnnotation(
                    table_name="orders",
                    columns=[
                        _col(
                            "total",
                            "measure",
                            derived_formula_hypothesis="subtotal + tax",
                            derived_formula_confidence=0.85,
                        ),
                        _col("discount", "measure", derived_formula_hypothesis="   "),
                    ],
                )
            ]
        )

        persist_column_annotations(
            session,
            output,
            [table.table_id],
            annotated_by="m",
            run_id=baseline_run_id(),
        )
        session.flush()

        rows = {r.column_id: r for r in session.execute(select(AnnotationDB)).scalars()}
        cols = {c.column_name: c.column_id for c in session.execute(select(Column)).scalars()}
        total = rows[cols["total"]]
        assert total.derived_formula_hypothesis == "subtotal + tax"
        assert total.derived_formula_confidence == 0.85
        assert rows[cols["discount"]].derived_formula_hypothesis is None


# ---------------------------------------------------------------------------
# synthesize_and_store_tables
# ---------------------------------------------------------------------------


class TestSynthesizeAndStoreTables:
    def test_stores_entities_and_relationships_no_annotations(self, session) -> None:
        orders = _table_with_columns(session, "orders", ["order_id", "customer_id"])
        customers = _table_with_columns(session, "customers", ["id"])

        agent = MagicMock()
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
                    entity_detections=[
                        EntityDetection(
                            table_id="",
                            table_name="orders",
                            entity_type="orders",
                            confidence=0.9,
                            grain_columns=["order_id"],
                            is_fact_table=True,
                            is_dimension_table=False,
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
        assert len(entities) == 1 and entities[0].is_fact_table is True
        # DAT-565: all event-time axes + identities persisted run-versioned (JSON).
        assert [tc["column"] for tc in entities[0].time_columns] == ["order_date", "ship_date"]
        assert entities[0].time_columns[1]["aspect"] == "ship"
        assert [ic["column"] for ic in entities[0].identity_columns] == ["customer_id"]
        assert len(rels) == 1 and rels[0].cardinality is None  # no duckdb → unresolved
        assert anns == []  # per-table synthesis never writes column annotations

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
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
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
        agent.synthesize_tables = MagicMock(
            return_value=Result.ok(
                SemanticEnrichmentResult(
                    annotations=[],
                    entity_detections=[
                        EntityDetection(
                            table_id="",
                            table_name="orders",
                            entity_type="orders",
                            confidence=0.9,
                            grain_columns=["order_id"],
                            is_fact_table=True,
                            is_dimension_table=False,
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
        agent.synthesize_tables = MagicMock(return_value=Result.fail("LLM down"))

        result = synthesize_and_store_tables(session, agent, ["t1"], run_id=baseline_run_id())
        assert not result.success
        assert "LLM down" in (result.error or "")


# ---------------------------------------------------------------------------
# per-table parse / format helpers
# ---------------------------------------------------------------------------


class TestTableSynthesisHelpers:
    def test_parse_table_synthesis_output_yields_no_annotations(self) -> None:
        agent = SemanticAgent.__new__(SemanticAgent)  # no LLM init needed
        result = agent._parse_table_synthesis_output(
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
            },
            "test-model",
        )
        enrichment = result.unwrap()
        assert enrichment.annotations == []
        assert len(enrichment.entity_detections) == 1
        assert enrichment.entity_detections[0].is_dimension_table is False

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
