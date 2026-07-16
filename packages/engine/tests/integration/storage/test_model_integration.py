"""Tests for SQLAlchemy models."""

from datetime import datetime

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.semantic.db_models import (
    SemanticAnnotation,
    TableEntity,
)
from dataraum.analysis.statistics.db_models import (
    StatisticalProfile,
)
from dataraum.analysis.temporal import TemporalColumnProfile
from dataraum.analysis.typing.db_models import (
    TypeCandidate,
    TypeDecision,
)
from dataraum.storage import Column, Source, Table


class TestCoreModels:
    """Test core models: Source, Table, Column."""

    def test_create_source(self, session: Session):
        source = Source(
            name="test_csv",
            source_type="csv",
            connection_config={"path": "/data/test.csv"},
        )
        session.add(source)
        session.commit()

        result = session.execute(select(Source).where(Source.name == "test_csv"))
        saved = result.scalar_one()

        assert saved.name == "test_csv"
        assert saved.source_type == "csv"
        assert saved.connection_config
        assert saved.connection_config["path"] == "/data/test.csv"
        assert saved.source_id is not None

    def test_create_table_with_source(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        session.add(source)
        session.flush()

        table = Table(
            source=source,
            table_name="sales",
            layer="raw",
            row_count=1000,
        )
        session.add(table)
        session.commit()

        result = session.execute(select(Table).where(Table.table_name == "sales"))
        saved = result.scalar_one()

        assert saved.table_name == "sales"
        assert saved.layer == "raw"
        assert saved.row_count == 1000
        assert saved.source.name == "test_source"

    def test_create_column(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(
            table=table,
            column_name="amount",
            column_position=1,
            raw_type="VARCHAR",
            resolved_type="DOUBLE",
        )
        session.add_all([source, table, column])
        session.commit()

        result = session.execute(select(Column).where(Column.column_name == "amount"))
        saved = result.scalar_one()

        assert saved.column_name == "amount"
        assert saved.column_position == 1
        assert saved.raw_type == "VARCHAR"
        assert saved.resolved_type == "DOUBLE"
        assert saved.table.table_name == "sales"

    def test_cascade_delete_source(self, session: Session):
        """Test that deleting a source deletes its tables and columns."""
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        session.commit()

        # Delete source
        session.delete(source)
        session.commit()

        # Verify cascade
        tables = session.execute(select(Table))
        columns = session.execute(select(Column))

        assert len(tables.scalars().all()) == 0
        assert len(columns.scalars().all()) == 0


class TestStatisticalModels:
    """Test statistical metadata models."""

    def test_create_column_profile(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        session.flush()

        profile = StatisticalProfile(
            column=column,
            layer="typed",
            total_count=1000,
            null_count=50,
            distinct_count=800,
            cardinality_ratio=0.8,
            null_ratio=0.05,
            profile_data={"percentiles": {"p50": 100.0, "p95": 500.0}},
        )
        session.add(profile)
        session.commit()

        result = session.execute(select(StatisticalProfile))
        saved = result.scalar_one()

        assert saved.total_count == 1000
        assert saved.null_count == 50
        assert saved.cardinality_ratio == 0.8
        assert saved.profile_data["percentiles"]
        assert saved.profile_data["percentiles"]["p50"] == 100.0

    def test_create_type_candidate(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        session.flush()

        candidate = TypeCandidate(
            column=column,
            data_type="DOUBLE",
            confidence=0.95,
            parse_success_rate=0.98,
            detected_pattern="numeric",
            detected_unit="USD",
            unit_confidence=0.85,
        )
        session.add(candidate)
        session.commit()

        result = session.execute(select(TypeCandidate))
        saved = result.scalar_one()

        assert saved.data_type == "DOUBLE"
        assert saved.confidence == 0.95
        assert saved.detected_unit == "USD"

    def test_create_type_decision(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        session.flush()

        decision = TypeDecision(
            column=column,
            decided_type="DOUBLE",
            decision_source="automatic",
            decided_by="system",
            decision_reason="High confidence from pattern detection",
        )
        session.add(decision)
        session.commit()

        result = session.execute(select(TypeDecision))
        saved = result.scalar_one()

        assert saved.decided_type == "DOUBLE"
        assert saved.decision_source == "automatic"


class TestSemanticModels:
    """Test semantic metadata models."""

    def test_create_semantic_annotation(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        session.flush()

        annotation = SemanticAnnotation(
            column=column,
            semantic_role="measure",
            entity_type="transaction",
            business_name="Sale Amount",
            business_description="Total transaction amount in USD",
            annotation_source="llm",
            annotated_by="claude-sonnet-4",
            confidence=0.92,
        )
        session.add(annotation)
        session.commit()

        result = session.execute(select(SemanticAnnotation))
        saved = result.scalar_one()

        assert saved.semantic_role == "measure"
        assert saved.business_name == "Sale Amount"
        assert saved.annotation_source == "llm"

    def test_create_table_entity(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        session.add_all([source, table])
        session.flush()

        entity = TableEntity(
            table=table,
            detected_entity_type="transaction",
            description="Daily sales transactions",
            grain_columns=["sale_id"],
            table_role="fact",
            detection_source="llm",
        )
        session.add(entity)
        session.commit()

        result = session.execute(select(TableEntity))
        saved = result.scalar_one()

        assert saved.detected_entity_type == "transaction"
        assert saved.table_role == "fact"
        assert saved.grain_columns == ["sale_id"]


class TestTopologicalModels:
    """Test topological metadata models."""

    def test_create_relationship(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        sales_table = Table(source=source, table_name="sales", layer="raw")
        customer_table = Table(source=source, table_name="customers", layer="raw")
        customer_id_col = Column(table=sales_table, column_name="customer_id", column_position=1)
        id_col = Column(table=customer_table, column_name="id", column_position=1)

        session.add_all([source, sales_table, customer_table, customer_id_col, id_col])
        session.flush()

        # Now IDs are populated, we can use them for the Relationship
        relationship = Relationship(
            from_table_id=sales_table.table_id,
            from_column_id=customer_id_col.column_id,
            to_table_id=customer_table.table_id,
            to_column_id=id_col.column_id,
            relationship_type="foreign_key",
            cardinality="many-to-one",
            confidence=0.95,
            detection_method="candidate",
            evidence={"overlap_rate": 0.98},
        )
        session.add(relationship)
        session.commit()

        result = session.execute(select(Relationship))
        saved = result.scalar_one()

        assert saved.relationship_type == "foreign_key"
        assert saved.cardinality == "many-to-one"
        assert saved.confidence == 0.95

    def test_relationship_type_check_constraint(self, session: Session):
        """The closed relationship_type vocabulary is DB-enforced (DAT-782).

        ``ck_relationships_relationship_type`` admits exactly the values the
        writers produce ('foreign_key', 'hierarchy', 'candidate'). A dead
        value — e.g. 'semantic_reference', once advertised in the column
        comment but never written — must be rejected at INSERT, not merely
        discouraged by a producer-side Literal (which drifted once already).
        """
        source = Source(name="test_source", source_type="csv")
        sales_table = Table(source=source, table_name="sales", layer="raw")
        customer_table = Table(source=source, table_name="customers", layer="raw")
        customer_id_col = Column(table=sales_table, column_name="customer_id", column_position=1)
        id_col = Column(table=customer_table, column_name="id", column_position=1)
        session.add_all([source, sales_table, customer_table, customer_id_col, id_col])
        session.flush()

        def _rel(relationship_type: str, detection_method: str) -> Relationship:
            # detection_method varies per row to sidestep the run-grain unique
            # constraint (run_id, from_column_id, to_column_id, detection_method) —
            # every value here must itself be real (ck_relationships_detection_method,
            # DAT-802), so the 4 rows below exhaust the closed detection_method set.
            return Relationship(
                from_table_id=sales_table.table_id,
                from_column_id=customer_id_col.column_id,
                to_table_id=customer_table.table_id,
                to_column_id=id_col.column_id,
                relationship_type=relationship_type,
                confidence=0.9,
                detection_method=detection_method,
            )

        # Every value a real writer produces is admitted.
        real_detection_methods = ("candidate", "llm", "manual")
        for relationship_type, detection_method in zip(
            ("foreign_key", "hierarchy", "candidate"), real_detection_methods, strict=True
        ):
            session.add(_rel(relationship_type, detection_method=detection_method))
        session.flush()

        # A value outside the closed vocabulary is rejected by the CHECK — the
        # row's detection_method ('keeper', the one real value unused above) is
        # itself valid, so the rejection is proven to come from relationship_type.
        session.add(_rel("semantic_reference", detection_method="keeper"))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()


class TestTemporalModels:
    """Test temporal metadata models."""

    def test_create_temporal_profile(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="sale_date", column_position=1)
        session.add_all([source, table, column])
        session.flush()

        from uuid import uuid4

        temporal = TemporalColumnProfile(
            profile_id=str(uuid4()),
            column_id=column.column_id,
            run_id=str(uuid4()),
            profiled_at=datetime.now(),
            min_timestamp=datetime(2024, 1, 1),
            max_timestamp=datetime(2024, 12, 31),
            span_days=364.0,
            detected_granularity="day",
            granularity_confidence=0.98,
            completeness_ratio=0.96,
            expected_periods=365,
            actual_periods=360,
            gap_count=5,
            largest_gap_days=12.0,
            is_stale=False,
            gaps=[
                {
                    "gap_start": "2024-06-01T00:00:00",
                    "gap_end": "2024-06-13T00:00:00",
                    "gap_length_days": 12.0,
                    "missing_periods": 11,
                    "severity": "moderate",
                }
            ],
        )
        session.add(temporal)
        session.commit()

        result = session.execute(select(TemporalColumnProfile))
        saved = result.scalar_one()

        assert saved.detected_granularity == "day"
        assert saved.span_days == 364.0
        assert saved.granularity_confidence == 0.98
        assert saved.completeness_ratio == 0.96
        assert saved.gap_count == 5
        assert saved.largest_gap_days == 12.0
        assert saved.gaps[0]["severity"] == "moderate"
        assert saved.is_stale is False
