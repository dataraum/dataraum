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
    """In-memory SQLite session with all tables created."""
    engine = create_engine("sqlite:///:memory:", echo=False)

    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.close()

    init_database(engine)
    factory = sessionmaker(bind=engine)
    with factory() as s:
        yield s


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


class TestBuilderExtractsTableEntity:
    """Verify builder reads table_description, grain_columns, time_column."""

    def test_table_description_and_grain(self, session: Session) -> None:
        from dataraum.analysis.semantic.db_models import TableEntity

        source_id, table_id, column_id = _insert_source_table_column(session)

        session.add(
            TableEntity(
                entity_id=_id(),
                table_id=table_id,
                detected_entity_type="financial_transaction",
                description="Records of all financial transactions",
                grain_columns=["invoice_id"],
                time_column="created_at",
                is_fact_table=True,
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id])

        table = ctx.tables[0]
        assert table.table_description == "Records of all financial transactions"
        assert table.grain_columns == ["invoice_id"]
        assert table.time_column == "created_at"


class TestBuilderExtractsValidationDetails:
    """Verify builder reads ValidationResultRecord.details."""

    def test_validation_details(self, session: Session) -> None:
        from dataraum.analysis.validation.db_models import ValidationResultRecord

        source_id, table_id, column_id = _insert_source_table_column(session)

        session.add(
            ValidationResultRecord(
                result_id=_id(),
                validation_id="balance_check",
                table_ids=[table_id],
                status="failed",
                severity="critical",
                passed=False,
                message="Balance mismatch",
                details={"summary": "Off by 42.50", "affected_rows": 3},
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id])

        assert len(ctx.validations) == 1
        assert ctx.validations[0].details == {"summary": "Off by 42.50", "affected_rows": 3}


class TestBuilderExtractsCycleVolume:
    """Verify builder reads total_records, completed_cycles, evidence."""

    def test_cycle_volume_fields(self, session: Session) -> None:
        from dataraum.analysis.cycles.db_models import DetectedBusinessCycle

        source_id, table_id, column_id = _insert_source_table_column(session)

        session.add(
            DetectedBusinessCycle(
                cycle_id=_id(),
                source_id=source_id,
                cycle_name="Accounts Receivable",
                cycle_type="accounts_receivable",
                tables_involved=["invoices"],
                total_records=10000,
                completed_cycles=8500,
                completion_rate=0.85,
                evidence=["Status column tracks lifecycle", "Payment dates correlate"],
            )
        )
        session.flush()

        ctx = build_execution_context(session, [table_id])

        assert len(ctx.business_cycles) == 1
        cycle = ctx.business_cycles[0]
        assert cycle.total_records == 10000
        assert cycle.completed_cycles == 8500
        assert cycle.evidence == ["Status column tracks lifecycle", "Payment dates correlate"]
