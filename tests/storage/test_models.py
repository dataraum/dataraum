"""Tests for SQLAlchemy models."""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.analysis.statistics.db_models import (
    StatisticalProfile,
)
from dataraum_context.analysis.typing.db_models import (
    TypeCandidate,
    TypeDecision,
)
from dataraum_context.enrichment.db_models import (
    JoinPath,
    Relationship,
    SemanticAnnotation,
    TableEntity,
    TemporalQualityMetrics,
)
from dataraum_context.llm.db_models import LLMCache
from dataraum_context.quality.db_models import QualityResult, QualityRule
from dataraum_context.storage import Column, Source, Table


class TestCoreModels:
    """Test core models: Source, Table, Column."""

    async def test_create_source(self, session: AsyncSession):
        source = Source(
            name="test_csv",
            source_type="csv",
            connection_config={"path": "/data/test.csv"},
        )
        session.add(source)
        await session.commit()

        result = await session.execute(select(Source).where(Source.name == "test_csv"))
        saved = result.scalar_one()

        assert saved.name == "test_csv"
        assert saved.source_type == "csv"
        assert saved.connection_config
        assert saved.connection_config["path"] == "/data/test.csv"
        assert saved.source_id is not None

    async def test_create_table_with_source(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        session.add(source)
        await session.flush()

        table = Table(
            source=source,
            table_name="sales",
            layer="raw",
            row_count=1000,
        )
        session.add(table)
        await session.commit()

        result = await session.execute(select(Table).where(Table.table_name == "sales"))
        saved = result.scalar_one()

        assert saved.table_name == "sales"
        assert saved.layer == "raw"
        assert saved.row_count == 1000
        assert saved.source.name == "test_source"

    async def test_create_column(self, session: AsyncSession):
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
        await session.commit()

        result = await session.execute(select(Column).where(Column.column_name == "amount"))
        saved = result.scalar_one()

        assert saved.column_name == "amount"
        assert saved.column_position == 1
        assert saved.raw_type == "VARCHAR"
        assert saved.resolved_type == "DOUBLE"
        assert saved.table.table_name == "sales"

    async def test_cascade_delete_source(self, session: AsyncSession):
        """Test that deleting a source deletes its tables and columns."""
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        await session.commit()

        # Delete source
        await session.delete(source)
        await session.commit()

        # Verify cascade
        tables = await session.execute(select(Table))
        columns = await session.execute(select(Column))

        assert len(tables.scalars().all()) == 0
        assert len(columns.scalars().all()) == 0


class TestStatisticalModels:
    """Test statistical metadata models."""

    async def test_create_column_profile(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        await session.flush()

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
        await session.commit()

        result = await session.execute(select(StatisticalProfile))
        saved = result.scalar_one()

        assert saved.total_count == 1000
        assert saved.null_count == 50
        assert saved.cardinality_ratio == 0.8
        assert saved.profile_data["percentiles"]
        assert saved.profile_data["percentiles"]["p50"] == 100.0

    async def test_create_type_candidate(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        await session.flush()

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
        await session.commit()

        result = await session.execute(select(TypeCandidate))
        saved = result.scalar_one()

        assert saved.data_type == "DOUBLE"
        assert saved.confidence == 0.95
        assert saved.detected_unit == "USD"

    async def test_create_type_decision(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        await session.flush()

        decision = TypeDecision(
            column=column,
            decided_type="DOUBLE",
            decision_source="auto",
            decided_by="system",
            decision_reason="High confidence from pattern detection",
        )
        session.add(decision)
        await session.commit()

        result = await session.execute(select(TypeDecision))
        saved = result.scalar_one()

        assert saved.decided_type == "DOUBLE"
        assert saved.decision_source == "auto"


class TestSemanticModels:
    """Test semantic metadata models."""

    async def test_create_semantic_annotation(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        await session.flush()

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
        await session.commit()

        result = await session.execute(select(SemanticAnnotation))
        saved = result.scalar_one()

        assert saved.semantic_role == "measure"
        assert saved.business_name == "Sale Amount"
        assert saved.annotation_source == "llm"

    async def test_create_table_entity(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        session.add_all([source, table])
        await session.flush()

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
        await session.commit()

        result = await session.execute(select(TableEntity))
        saved = result.scalar_one()

        assert saved.detected_entity_type == "transaction"
        assert saved.is_fact_table is True
        assert saved.grain_columns == ["sale_id"]


class TestTopologicalModels:
    """Test topological metadata models."""

    async def test_create_relationship(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        sales_table = Table(source=source, table_name="sales", layer="raw")
        customer_table = Table(source=source, table_name="customers", layer="raw")
        customer_id_col = Column(table=sales_table, column_name="customer_id", column_position=1)
        id_col = Column(table=customer_table, column_name="id", column_position=1)

        session.add_all([source, sales_table, customer_table, customer_id_col, id_col])
        await session.flush()

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
        await session.commit()

        result = await session.execute(select(Relationship))
        saved = result.scalar_one()

        assert saved.relationship_type == "foreign_key"
        assert saved.cardinality == "n:1"
        assert saved.confidence == 0.95

    async def test_create_join_path(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table1 = Table(source=source, table_name="sales", layer="raw")
        table2 = Table(source=source, table_name="customers", layer="raw")
        session.add_all([source, table1, table2])
        await session.flush()

        join_path = JoinPath(
            from_table_id=table1.table_id,
            to_table_id=table2.table_id,
            path_steps=[{"from_col": "customer_id", "to_table": "customers", "to_col": "id"}],
            path_length=1,
            total_confidence=0.95,
        )
        session.add(join_path)
        await session.commit()

        result = await session.execute(select(JoinPath))
        saved = result.scalar_one()

        assert saved.path_length == 1
        assert saved.total_confidence == 0.95


class TestTemporalModels:
    """Test temporal metadata models."""

    async def test_create_temporal_profile(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="sale_date", column_position=1)
        session.add_all([source, table, column])
        await session.flush()

        from uuid import uuid4

        temporal = TemporalQualityMetrics(
            metric_id=str(uuid4()),
            column_id=column.column_id,
            computed_at=datetime.now(),
            min_timestamp=datetime(2024, 1, 1),
            max_timestamp=datetime(2024, 12, 31),
            detected_granularity="day",
            completeness_ratio=0.96,
            has_seasonality=True,
            temporal_data={
                "span_days": 364.0,
                "granularity_confidence": 0.98,
                "gap_count": 5,
                "seasonality_period": "quarterly",
                "trend_direction": "increasing",
            },
        )
        session.add(temporal)
        await session.commit()

        result = await session.execute(select(TemporalQualityMetrics))
        saved = result.scalar_one()

        assert saved.detected_granularity == "day"
        assert saved.completeness_ratio == 0.96
        assert saved.has_seasonality is True


class TestQualityModels:
    """Test quality metadata models."""

    async def test_create_quality_rule(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        column = Column(table=table, column_name="amount", column_position=1)
        session.add_all([source, table, column])
        await session.flush()

        rule = QualityRule(
            table=table,
            column=column,
            rule_name="amount_non_negative",
            rule_type="range",
            rule_expression="amount >= 0",
            rule_parameters={"min": 0},
            severity="error",
            source="llm",
            description="Sale amounts should be non-negative",
            is_active=True,
        )
        session.add(rule)
        await session.commit()

        result = await session.execute(select(QualityRule))
        saved = result.scalar_one()

        assert saved.rule_name == "amount_non_negative"
        assert saved.rule_type == "range"
        assert saved.severity == "error"

    async def test_create_quality_result(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        table = Table(source=source, table_name="sales", layer="raw")
        rule = QualityRule(
            table=table,
            rule_name="test_rule",
            rule_type="range",
            rule_expression="amount >= 0",
            source="manual",
        )
        session.add_all([source, table, rule])
        await session.flush()

        result = QualityResult(
            rule=rule,
            total_records=1000,
            passed_records=950,
            failed_records=50,
            pass_rate=0.95,
            failure_samples=[{"row": 1, "value": -10}],
        )
        session.add(result)
        await session.commit()

        saved_result = await session.execute(select(QualityResult))
        saved = saved_result.scalar_one()

        assert saved.total_records == 1000
        assert saved.pass_rate == 0.95


class TestLLMCache:
    """Test LLM cache model."""

    async def test_create_llm_cache(self, session: AsyncSession):
        source = Source(name="test_source", source_type="csv")
        session.add(source)
        await session.flush()

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
        await session.commit()

        result = await session.execute(select(LLMCache).where(LLMCache.cache_key == "abc123"))
        saved = result.scalar_one()

        assert saved.feature == "semantic_analysis"
        assert saved.provider == "anthropic"
        assert saved.input_tokens == 1000
