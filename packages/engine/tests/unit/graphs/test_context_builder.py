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
        from dataraum.analysis.semantic.db_models import SemanticAnnotation

        source_id, table_id, column_id = _insert_source_table_column(session)

        session.add(
            SemanticAnnotation(
                annotation_id=_id(),
                column_id=column_id,
                semantic_role="measure",
                business_name="Invoice Amount",
                business_description="Total value before tax in local currency",
                unit_source_column="currency_code",
                confidence=0.9,
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id])

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
        from dataraum.storage.snapshot_head import MetadataSnapshotHead

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
            head_id=_id(), target=f"table:{table_id}", stage="detect", run_id="new"
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
    """Verify builder reads table_description, grain_columns, time_column."""

    def test_table_description_and_grain(self, session: Session) -> None:
        from dataraum.analysis.semantic.db_models import TableEntity
        from dataraum.storage.snapshot_head import MetadataSnapshotHead, session_head_target

        sess_id = "sess-fields"
        source_id, table_id, column_id = _insert_source_table_column(session)

        # Entity fields only flow into the context when the session's run resolves
        # (DAT-429 fail-closed), so seed a promoted head + run-stamped entity.
        session.add(
            TableEntity(
                entity_id=_id(),
                session_id=sess_id,
                table_id=table_id,
                run_id="r1",
                detected_entity_type="financial_transaction",
                description="Records of all financial transactions",
                grain_columns=["invoice_id"],
                time_column="created_at",
                is_fact_table=True,
            )
        )
        session.add(
            MetadataSnapshotHead(
                head_id=_id(), target=session_head_target(sess_id), stage="detect", run_id="r1"
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id], session_id=sess_id)

        table = ctx.tables[0]
        assert table.table_description == "Records of all financial transactions"
        assert table.grain_columns == ["invoice_id"]
        assert table.time_column == "created_at"

    def test_unresolved_session_reads_no_run_versioned_data(self, session: Session) -> None:
        """Fail-closed (DAT-429): no resolved run ⇒ no entities/relationships, never cross-run.

        Entities exist under runs, but with no promoted head for the queried
        session the context must surface NONE of them — reading cross-run here is
        the session-isolation leak this guards against.
        """
        from dataraum.analysis.semantic.db_models import TableEntity

        _source_id, table_id, _column_id = _insert_source_table_column(session)
        for rid in ("run-1", "run-2"):
            session.add(
                TableEntity(
                    entity_id=_id(),
                    session_id="sX",
                    table_id=table_id,
                    run_id=rid,
                    detected_entity_type="fact",
                    description=f"{rid} classification",
                    is_fact_table=True,
                )
            )
        session.flush()

        # No session_id ⇒ unresolved run ⇒ no entity data, no relationships.
        ctx_none = build_execution_context(session, [table_id])
        assert ctx_none.tables[0].table_description is None
        assert ctx_none.relationships == []

        # A session_id whose head was never promoted is equally unresolved.
        ctx_unpromoted = build_execution_context(session, [table_id], session_id="sX")
        assert ctx_unpromoted.tables[0].table_description is None
        assert ctx_unpromoted.relationships == []

    def test_run_scoped_to_promoted_head(self, session: Session) -> None:
        """Coexisting runs' classifications must not bleed in — read the promoted run.

        Regression: TableEntity is run-versioned and coexists across runs
        (DAT-408/413). ``build_execution_context`` must read only the session's
        promoted run (per the snapshot head) — exactly like its relationship read —
        not last-write an arbitrary run's classification into the context dict.

        Deterministic guard: with identical seeded data, flipping the head between
        the two runs must flip the context. Without run-scoping the head is ignored,
        so both builds return the same last-write-wins value and one assert fails.
        """
        from dataraum.analysis.semantic.db_models import TableEntity
        from dataraum.storage.snapshot_head import (
            MetadataSnapshotHead,
            head_run_id,
            session_head_target,
        )

        sess_id = "sess-multirun"
        _source_id, table_id, _column_id = _insert_source_table_column(session)

        for run_id, desc, is_fact in (
            ("run-1", "RUN ONE classification", False),
            ("run-2", "RUN TWO classification", True),
        ):
            session.add(
                TableEntity(
                    entity_id=_id(),
                    session_id=sess_id,
                    table_id=table_id,
                    run_id=run_id,
                    detected_entity_type="fact" if is_fact else "dimension",
                    description=desc,
                    is_fact_table=is_fact,
                )
            )
        head = MetadataSnapshotHead(
            head_id=_id(), target=session_head_target(sess_id), stage="detect", run_id="run-2"
        )
        session.add(head)
        session.flush()

        # Head → run-2: context reflects run-2's classification.
        ctx2 = build_execution_context(session, [table_id], session_id=sess_id)
        assert ctx2.tables[0].table_description == "RUN TWO classification"
        assert ctx2.tables[0].is_fact_table is True

        # Flip head → run-1: same data, the context must follow the promoted run.
        head.run_id = "run-1"
        session.flush()
        assert head_run_id(session, session_head_target(sess_id), "detect") == "run-1"
        ctx1 = build_execution_context(session, [table_id], session_id=sess_id)
        assert ctx1.tables[0].table_description == "RUN ONE classification"
        assert ctx1.tables[0].is_fact_table is False


class TestBuilderExtractsValidationDetails:
    """Verify builder reads ValidationResultRecord.details.

    Run-versioned since DAT-438: the builder reads validation results only at
    the session's promoted operating_model head — the test seeds the head and
    passes the session_id. Without either, the read is fail-closed empty.
    """

    def test_validation_details(self, session: Session) -> None:
        from dataraum.analysis.validation.db_models import ValidationResultRecord
        from dataraum.investigation.db_models import InvestigationSession
        from dataraum.storage.snapshot_head import MetadataSnapshotHead, session_head_target

        source_id, table_id, column_id = _insert_source_table_column(session)

        sess_id = "sess-validation-details"
        session.add(InvestigationSession(session_id=sess_id, intent="test"))
        session.add(
            ValidationResultRecord(
                result_id=_id(),
                session_id=sess_id,
                run_id="run-om",
                validation_id="balance_check",
                table_ids=[table_id],
                status="failed",
                severity="critical",
                passed=False,
                message="Balance mismatch",
                details={"summary": "Off by 42.50", "affected_rows": 3},
            )
        )
        session.add(
            MetadataSnapshotHead(
                target=session_head_target(sess_id), stage="operating_model", run_id="run-om"
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id], session_id=sess_id)

        assert len(ctx.validations) == 1
        assert ctx.validations[0].details == {"summary": "Off by 42.50", "affected_rows": 3}

        # Fail-closed: no session_id → no promoted head to read at → empty.
        unscoped = build_execution_context(session, [table_id])
        assert unscoped.validations == []


class TestBuilderExtractsCycleVolume:
    """Verify builder reads total_records, completed_cycles, evidence.

    Run-versioned + session-scoped since DAT-455: the builder reads detected
    cycles only at the session's promoted operating_model head — the test seeds
    the head and passes the session_id. Without either, the read is fail-closed
    empty (mirrors the validation-details test).
    """

    def test_cycle_volume_fields(self, session: Session) -> None:
        from dataraum.analysis.cycles.db_models import DetectedBusinessCycle
        from dataraum.investigation.db_models import InvestigationSession
        from dataraum.storage.snapshot_head import MetadataSnapshotHead, session_head_target

        source_id, table_id, column_id = _insert_source_table_column(session)

        sess_id = "sess-cycle-volume"
        session.add(InvestigationSession(session_id=sess_id, intent="test"))
        session.add(
            DetectedBusinessCycle(
                cycle_id=_id(),
                session_id=sess_id,
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
                target=session_head_target(sess_id), stage="operating_model", run_id="run-om"
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id], session_id=sess_id)

        assert len(ctx.business_cycles) == 1
        cycle = ctx.business_cycles[0]
        assert cycle.total_records == 10000
        assert cycle.completed_cycles == 8500
        assert cycle.evidence == ["Status column tracks lifecycle", "Payment dates correlate"]

        # Fail-closed: no session_id → no promoted head to read at → empty.
        unscoped = build_execution_context(session, [table_id])
        assert unscoped.business_cycles == []


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
        from dataraum.investigation.db_models import InvestigationSession

        _source_id, table_id, _column_id = _insert_source_table_column(session)
        sess_id = "sess-om-inrun"
        session.add(InvestigationSession(session_id=sess_id, intent="test"))
        session.add(
            DetectedBusinessCycle(
                cycle_id=_id(),
                session_id=sess_id,
                run_id="run-current",
                cycle_name="Order to Cash",
                cycle_type="order_to_cash",
                canonical_type="order_to_cash",
                tables_involved=["invoices"],
            )
        )
        session.flush()  # NB: no promoted operating_model head seeded.

        # Default derivation: no promoted operating_model head → reads nothing.
        ctx_default = build_execution_context(session, [table_id], session_id=sess_id)
        assert ctx_default.business_cycles == []

        # In-run override: read this run's cycle directly, head or not.
        ctx_override = build_execution_context(
            session, [table_id], session_id=sess_id, om_run_id="run-current"
        )
        assert len(ctx_override.business_cycles) == 1
        assert ctx_override.business_cycles[0].cycle_name == "Order to Cash"
