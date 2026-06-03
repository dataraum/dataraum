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
    Relationship,
    SemanticEnrichmentResult,
    TableColumnAnnotation,
)
from dataraum.analysis.semantic.processor import (
    persist_column_annotations,
    synthesize_and_store_tables,
)
from dataraum.core.models.base import RelationshipType, Result
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_session_id


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
            session_id=baseline_session_id(),
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
            session_id=baseline_session_id(),
        )
        assert count == 1


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
            session_id=baseline_session_id(),
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
        assert len(rels) == 1 and rels[0].cardinality is None  # no duckdb → unresolved
        assert anns == []  # per-table synthesis never writes column annotations

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

        DAT-408: a session has MANY runs. `TableEntity` is versioned by `run_id`, so a
        new run COEXISTS with earlier runs (non-destructive); a same-run retry is a
        no-op (run-scoped delete-before-insert). The `llm` relationship is session-grain
        — upserted on the `(session, from, to, method)` key, so it never duplicates and
        a re-run refreshes it.
        """
        orders = _table_with_columns(session, "orders", ["order_id", "customer_id"])
        customers = _table_with_columns(session, "customers", ["id"])
        tids = [orders.table_id, customers.table_id]
        sid = baseline_session_id()

        def run(run_id: str) -> None:
            assert synthesize_and_store_tables(
                session, self._agent(), tids, session_id=sid, run_id=run_id
            ).success
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
        assert counts() == (2, 1), (
            "run-B's entity coexists with run-A's (non-destructive); llm stays single"
        )
        assert ent_runs == {"run-A", "run-B"}, "earlier run's entity survives the re-run"

    def test_propagates_agent_failure(self, session) -> None:
        agent = MagicMock()
        agent.synthesize_tables = MagicMock(return_value=Result.fail("LLM down"))

        result = synthesize_and_store_tables(
            session, agent, ["t1"], session_id=baseline_session_id()
        )
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
