"""Tests for build_execution_context field extraction.

Inserts DB records directly into an in-memory SQLite session
and verifies the context builder reads the new metadata fields.
"""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, sessionmaker

from dataraum.graphs.context import build_execution_context
from dataraum.storage import init_database


def _id() -> str:
    return str(uuid4())


@pytest.fixture
def session():
    """In-memory SQLite session with all tables created.

    ``StaticPool`` + explicit dispose keeps Python 3.12+ ``ResourceWarning``
    quiet by closing the sqlite3 connection deterministically.
    """
    from sqlalchemy.pool import StaticPool

    engine = create_engine(
        "sqlite:///:memory:",
        echo=False,
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    try:
        with factory() as s:
            yield s
    finally:
        engine.dispose()


def _insert_source_table_column(session: Session) -> tuple[str, str, str]:
    """Insert a source, table, and column. Return (source_id, table_id, column_id)."""
    from dataraum.storage import Column, Source, Table

    source_id = _id()
    table_id = _id()
    column_id = _id()

    session.add(Source(source_id=source_id, name="test_source", source_type="csv"))
    session.add(
        Table(
            table_id=table_id,
            source_id=source_id,
            table_name="invoices",
            layer="typed",
            duckdb_path="typed_invoices",
        )
    )
    session.add(
        Column(
            column_id=column_id,
            table_id=table_id,
            column_name="amount",
            column_position=0,
        )
    )
    session.flush()
    return source_id, table_id, column_id


class TestBuilderExtractsSemanticFields:
    """Verify builder reads business_name, business_description, unit_source_column."""

    def test_business_name_from_semantic_annotation(self, session: Session) -> None:
        from dataraum.analysis.semantic.db_models import ColumnConcept, SemanticAnnotation

        source_id, table_id, column_id = _insert_source_table_column(session)

        # Object-grain (business_name/description) on SemanticAnnotation; the
        # catalogue-grain unit source on ColumnConcept at the catalogue run (DAT-637).
        session.add(
            SemanticAnnotation(
                annotation_id=_id(),
                column_id=column_id,
                semantic_role="measure",
                business_name="Invoice Amount",
                business_description="Total value before tax in local currency",
                confidence=0.9,
            )
        )
        session.add(
            ColumnConcept(column_id=column_id, run_id="cat-run", unit_source_column="currency_code")
        )
        session.flush()

        ctx = build_execution_context(session, [table_id], catalogue_run_id="cat-run")

        col = ctx.tables[0].columns[0]
        assert col.business_name == "Invoice Amount"
        assert col.business_description == "Total value before tax in local currency"
        assert col.unit_source_column == "currency_code"

    def test_stale_addsource_run_dropped(self, session: Session) -> None:
        """Coexisting add_source runs ⇒ read only the table's promoted run (DAT-429 #2).

        A replay/teach leaves >1 column-metadata row per column (distinct run_id).
        The promoted ``table:{id}``/``detect`` head names the current add_source run;
        the builder must surface ONLY that run's annotation. Head-flip guard: with
        identical data, flipping the head flips the result — without run-scoping the
        head is ignored and both builds return the same value, so one assert fails.
        """
        from dataraum.analysis.semantic.db_models import SemanticAnnotation
        from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

        _source_id, table_id, column_id = _insert_source_table_column(session)
        for rid, name in (("new", "NEW name"), ("old", "OLD name")):
            session.add(
                SemanticAnnotation(
                    annotation_id=_id(),
                    column_id=column_id,
                    run_id=rid,
                    semantic_role="measure",
                    business_name=name,
                    confidence=0.9,
                )
            )
        head = MetadataSnapshotHead(
            head_id=_id(), target=f"table:{table_id}", stage=GENERATION_STAGE, run_id="new"
        )
        session.add(head)
        session.flush()

        ctx_new = build_execution_context(session, [table_id])
        assert ctx_new.tables[0].columns[0].business_name == "NEW name"

        head.run_id = "old"
        session.flush()
        ctx_old = build_execution_context(session, [table_id])
        assert ctx_old.tables[0].columns[0].business_name == "OLD name"


class TestBuilderExtractsTableEntity:
    """Verify builder reads table_description, grain_columns, time/identity columns."""

    def test_table_description_and_grain(self, session: Session) -> None:
        from dataraum.analysis.semantic.db_models import TableEntity
        from dataraum.storage.snapshot_head import MetadataSnapshotHead, catalog_head_target

        source_id, table_id, column_id = _insert_source_table_column(session)

        # Entity fields only flow into the context when the catalog run resolves
        # (DAT-429 fail-closed), so seed a promoted catalog head + run-stamped entity.
        session.add(
            TableEntity(
                entity_id=_id(),
                table_id=table_id,
                run_id="r1",
                detected_entity_type="financial_transaction",
                description="Records of all financial transactions",
                grain_columns=["invoice_id"],
                time_columns=[
                    {"column": "created_at", "aspect": "created", "note": "Row created."}
                ],
                identity_columns=[
                    {"column": "customer_id", "note": "Recurring customer identity."}
                ],
                is_fact_table=True,
            )
        )
        session.add(
            MetadataSnapshotHead(
                head_id=_id(), target=catalog_head_target(), stage="catalog", run_id="r1"
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id])

        table = ctx.tables[0]
        assert table.table_description == "Records of all financial transactions"
        assert table.grain_columns == ["invoice_id"]
        assert [tc["column"] for tc in table.time_columns] == ["created_at"]
        # DAT-566: identity_columns flows through the DB→context read path (the
        # `or []` None-guard branch is the common pre-DAT-565 case).
        assert [ic["column"] for ic in table.identity_columns] == ["customer_id"]

    def test_unresolved_catalog_reads_no_run_versioned_data(self, session: Session) -> None:
        """Fail-closed (DAT-429): no resolved catalog run ⇒ no entities/relationships.

        Entities exist under runs, but with no promoted catalog head the context
        must surface NONE of them — reading cross-run here is the isolation leak
        this guards against.
        """
        from dataraum.analysis.semantic.db_models import TableEntity

        _source_id, table_id, _column_id = _insert_source_table_column(session)
        for rid in ("run-1", "run-2"):
            session.add(
                TableEntity(
                    entity_id=_id(),
                    table_id=table_id,
                    run_id=rid,
                    detected_entity_type="fact",
                    description=f"{rid} classification",
                    is_fact_table=True,
                )
            )
        session.flush()

        # No promoted catalog head ⇒ unresolved run ⇒ no entity data, no relationships.
        ctx_none = build_execution_context(session, [table_id])
        assert ctx_none.tables[0].table_description is None
        assert ctx_none.relationships == []

    def test_run_scoped_to_promoted_head(self, session: Session) -> None:
        """Coexisting runs' classifications must not bleed in — read the promoted run.

        Regression: TableEntity is run-versioned and coexists across runs
        (DAT-408/413). ``build_execution_context`` must read only the workspace's
        promoted catalog run (per the snapshot head) — exactly like its relationship
        read — not last-write an arbitrary run's classification into the context dict.

        Deterministic guard: with identical seeded data, flipping the catalog head
        between the two runs must flip the context. Without run-scoping the head is
        ignored, so both builds return the same last-write-wins value and one fails.
        """
        from dataraum.analysis.semantic.db_models import TableEntity
        from dataraum.storage.snapshot_head import (
            MetadataSnapshotHead,
            catalog_head_target,
            head_run_id,
        )

        _source_id, table_id, _column_id = _insert_source_table_column(session)

        for run_id, desc, is_fact in (
            ("run-1", "RUN ONE classification", False),
            ("run-2", "RUN TWO classification", True),
        ):
            session.add(
                TableEntity(
                    entity_id=_id(),
                    table_id=table_id,
                    run_id=run_id,
                    detected_entity_type="fact" if is_fact else "dimension",
                    description=desc,
                    is_fact_table=is_fact,
                )
            )
        head = MetadataSnapshotHead(
            head_id=_id(), target=catalog_head_target(), stage="catalog", run_id="run-2"
        )
        session.add(head)
        session.flush()

        # Head → run-2: context reflects run-2's classification.
        ctx2 = build_execution_context(session, [table_id])
        assert ctx2.tables[0].table_description == "RUN TWO classification"
        assert ctx2.tables[0].is_fact_table is True

        # Flip head → run-1: same data, the context must follow the promoted run.
        head.run_id = "run-1"
        session.flush()
        assert head_run_id(session, catalog_head_target(), "catalog") == "run-1"
        ctx1 = build_execution_context(session, [table_id])
        assert ctx1.tables[0].table_description == "RUN ONE classification"
        assert ctx1.tables[0].is_fact_table is False


class TestBuilderRecomputesValidationVerdict:
    """The validation verdict is recomputed on demand (ADR-0017), not read.

    The builder no longer surfaces a stored verdict — it re-runs ``sql_used``.
    Recompute needs a DuckDB connection AND a declared spec, so the unit path
    (no connection) is fail-closed empty; the populated path is covered by the
    integration metrics/validation-phase tests (real connection + vertical).
    """

    def test_no_connection_yields_no_validations(self, session: Session) -> None:
        from dataraum.analysis.validation.db_models import ValidationResultRecord
        from dataraum.storage.snapshot_head import MetadataSnapshotHead, catalog_head_target

        source_id, table_id, column_id = _insert_source_table_column(session)

        # The slimmed record: grounded SQL + declared params, no verdict.
        session.add(
            ValidationResultRecord(
                result_id=_id(),
                run_id="run-om",
                validation_id="balance_check",
                table_ids=[table_id],
                severity="critical",
                tolerance=0.01,
                sql_used="SELECT 42.5 AS deviation, 100 AS magnitude",
            )
        )
        session.add(
            MetadataSnapshotHead(
                target=catalog_head_target(), stage="operating_model", run_id="run-om"
            )
        )
        session.flush()

        # No duckdb_conn → the builder cannot re-run, so it surfaces nothing
        # (never a stale stored verdict).
        ctx = build_execution_context(session, [table_id])
        assert ctx.validations == []


class TestBuilderExtractsCycleVolume:
    """Verify builder reads total_records, completed_cycles, evidence.

    Run-versioned since DAT-455: the builder reads detected cycles only at the
    workspace's promoted operating_model catalog head — the test seeds the head.
    Without it, the read is fail-closed empty (mirrors the validation-details test).
    """

    def test_cycle_volume_fields(self, session: Session) -> None:
        from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
        from dataraum.storage.snapshot_head import MetadataSnapshotHead, catalog_head_target

        source_id, table_id, column_id = _insert_source_table_column(session)

        session.add(
            DetectedBusinessCycle(
                cycle_id=_id(),
                run_id="run-om",
                cycle_name="Accounts Receivable",
                cycle_type="accounts_receivable",
                canonical_type="accounts_receivable",
                tables_involved=["invoices"],
                total_records=10000,
                completed_cycles=8500,
                completion_rate=0.85,
                evidence=["Status column tracks lifecycle", "Payment dates correlate"],
            )
        )
        session.add(
            MetadataSnapshotHead(
                target=catalog_head_target(), stage="operating_model", run_id="run-om"
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id])

        assert len(ctx.business_cycles) == 1
        cycle = ctx.business_cycles[0]
        assert cycle.total_records == 10000
        assert cycle.completed_cycles == 8500
        assert cycle.evidence == ["Status column tracks lifecycle", "Payment dates correlate"]


class TestBuilderOmRunIdOverride:
    """The in-run metrics phase (DAT-456) passes ``om_run_id`` so the graph
    context reads THIS operating_model run's cycle/validation evidence — written
    by the earlier validation + business_cycles activities in the same run —
    BEFORE the run is promoted. Without the override the read scopes to the
    promoted head, which does not exist yet mid-run.
    """

    def test_explicit_run_reads_in_run_cycles_without_a_promoted_head(
        self, session: Session
    ) -> None:
        from dataraum.analysis.cycles.db_models import DetectedBusinessCycle

        _source_id, table_id, _column_id = _insert_source_table_column(session)
        session.add(
            DetectedBusinessCycle(
                cycle_id=_id(),
                run_id="run-current",
                cycle_name="Order to Cash",
                cycle_type="order_to_cash",
                canonical_type="order_to_cash",
                tables_involved=["invoices"],
            )
        )
        session.flush()  # NB: no promoted operating_model head seeded.

        # Default derivation: no promoted operating_model head → reads nothing.
        ctx_default = build_execution_context(session, [table_id])
        assert ctx_default.business_cycles == []

        # In-run override: read this run's cycle directly, head or not.
        ctx_override = build_execution_context(session, [table_id], om_run_id="run-current")
        assert len(ctx_override.business_cycles) == 1
        assert ctx_override.business_cycles[0].cycle_name == "Order to Cash"


class TestBuilderExtractsDimensionHierarchies:
    """Verify the builder exposes DimensionHierarchy rows (DAT-537), run-scoped.

    Run-versioned, sealed at the begin_session catalog head: the builder reads only
    the promoted run, exactly like the slice read — an unpromoted run's rows must
    not bleed in (fail-closed).
    """

    def test_hierarchies_exposed_at_promoted_head(self, session: Session) -> None:
        from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
        from dataraum.storage.snapshot_head import (
            MetadataSnapshotHead,
            catalog_head_target,
        )

        _source_id, table_id, column_id = _insert_source_table_column(session)
        session.add(
            DimensionHierarchy(
                run_id="run-1",
                table_id=table_id,
                kind="drilldown",
                members=[
                    {"column_name": "zip", "column_id": column_id, "distinct_count": 6},
                    {"column_name": "city", "column_id": "", "distinct_count": 4},
                    {"column_name": "state", "column_id": "", "distinct_count": 2},
                ],
                canonical_label="zip → city → state",
                signature="drilldown:t:city|state|zip",
                score=0.0,
            )
        )
        head = MetadataSnapshotHead(
            head_id=_id(), target=catalog_head_target(), stage="catalog", run_id="run-1"
        )
        session.add(head)
        session.flush()

        ctx = build_execution_context(session, [table_id])
        assert len(ctx.dimension_hierarchies) == 1
        hier = ctx.dimension_hierarchies[0]
        assert hier.kind == "drilldown"
        assert hier.members == ["zip", "city", "state"]
        assert hier.canonical_label == "zip → city → state"
        assert hier.table_name == "invoices"

        # Flip the head to a run with no hierarchy rows → fail-closed empty.
        head.run_id = "run-2"
        session.flush()
        assert build_execution_context(session, [table_id]).dimension_hierarchies == []
