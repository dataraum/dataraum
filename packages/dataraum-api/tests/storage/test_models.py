"""Tests for SQLAlchemy models."""

from datetime import datetime

from sqlalchemy import select
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
from dataraum.llm.db_models import LLMCache
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
            decision_source="auto",
            decided_by="system",
            decision_reason="High confidence from pattern detection",
        )
        session.add(decision)
        session.commit()

        result = session.execute(select(TypeDecision))
        saved = result.scalar_one()

        assert saved.decided_type == "DOUBLE"
        assert saved.decision_source == "auto"


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
            confidence=0.88,
            grain_columns=["sale_id"],
            is_fact_table=True,
            is_dimension_table=False,
            detection_source="llm",
        )
        session.add(entity)
        session.commit()

        result = session.execute(select(TableEntity))
        saved = result.scalar_one()

        assert saved.detected_entity_type == "transaction"
        assert saved.is_fact_table is True
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
            cardinality="n:1",
            confidence=0.95,
            detection_method="tda",
            evidence={"overlap_rate": 0.98},
        )
        session.add(relationship)
        session.commit()

        result = session.execute(select(Relationship))
        saved = result.scalar_one()

        assert saved.relationship_type == "foreign_key"
        assert saved.cardinality == "n:1"
        assert saved.confidence == 0.95


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
            profiled_at=datetime.now(),
            min_timestamp=datetime(2024, 1, 1),
            max_timestamp=datetime(2024, 12, 31),
            detected_granularity="day",
            completeness_ratio=0.96,
            has_seasonality=True,
            has_trend=True,
            is_stale=False,
            profile_data={
                "span_days": 364.0,
                "granularity_confidence": 0.98,
                "gap_count": 5,
                "seasonality_period": "quarterly",
                "trend_direction": "increasing",
            },
        )
        session.add(temporal)
        session.commit()

        result = session.execute(select(TemporalColumnProfile))
        saved = result.scalar_one()

        assert saved.detected_granularity == "day"
        assert saved.completeness_ratio == 0.96
        assert saved.has_seasonality is True


class TestLLMCache:
    """Test LLM cache model."""

    def test_create_llm_cache(self, session: Session):
        source = Source(name="test_source", source_type="csv")
        session.add(source)
        session.flush()

        cache = LLMCache(
            cache_key="abc123",
            feature="semantic_analysis",
            source_id=source.source_id,
            table_ids=["table1", "table2"],
            ontology="financial_reporting",
            provider="anthropic",
            model="claude-sonnet-4",
            prompt_hash="prompt123",
            response_json={"annotations": []},
            input_tokens=1000,
            output_tokens=500,
            is_valid=True,
        )
        session.add(cache)
        session.commit()

        result = session.execute(select(LLMCache).where(LLMCache.cache_key == "abc123"))
        saved = result.scalar_one()

        assert saved.feature == "semantic_analysis"
        assert saved.provider == "anthropic"
        assert saved.input_tokens == 1000
