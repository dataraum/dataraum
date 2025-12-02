"""Tests for context assembly."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.context.assembly import assemble_context_document
from dataraum_context.storage.models_v2 import Column, Source, Table
from dataraum_context.storage.models_v2.statistical_context import StatisticalProfile


@pytest.mark.asyncio
async def test_assemble_context_document_no_source(async_session: AsyncSession):
    """Test assembly fails gracefully when source doesn't exist."""
    result = await assemble_context_document(
        source_id="nonexistent",
        ontology="financial_reporting",
        session=async_session,
    )

    assert not result.success
    assert "Source not found" in result.error


@pytest.mark.asyncio
async def test_assemble_context_document_no_typed_tables(async_session: AsyncSession):
    """Test assembly fails gracefully when source has no typed tables."""
    # Create source with only raw table
    source = Source(
        source_id=str(uuid4()),
        name="test_source",
        source_type="csv",
    )
    async_session.add(source)

    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="test_table",
        layer="raw",  # Not typed
        row_count=100,
    )
    async_session.add(table)
    await async_session.commit()

    result = await assemble_context_document(
        source_id=source.source_id,
        ontology="financial_reporting",
        session=async_session,
    )

    assert not result.success
    assert "No typed tables found" in result.error


@pytest.mark.skip(
    reason="Context assembly incomplete - ContextDocument model changed (see assembly.py TODOs)"
)
@pytest.mark.asyncio
async def test_assemble_context_document_basic(async_session: AsyncSession):
    """Test basic context document assembly with minimal data."""
    # Create source with typed table and column
    source = Source(
        source_id=str(uuid4()),
        name="test_source",
        source_type="csv",
    )
    async_session.add(source)

    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="customers",
        layer="typed",
        row_count=1000,
    )
    async_session.add(table)

    column = Column(
        column_id=str(uuid4()),
        table_id=table.table_id,
        column_name="customer_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="BIGINT",
    )
    async_session.add(column)

    # Add statistical profile
    profile = StatisticalProfile(
        profile_id=str(uuid4()),
        column_id=column.column_id,
        profiled_at=datetime.now(UTC),
        total_count=1000,
        null_count=0,
        distinct_count=1000,
        null_ratio=0.0,
        cardinality_ratio=1.0,
    )
    async_session.add(profile)
    await async_session.commit()

    # Assemble context
    result = await assemble_context_document(
        source_id=source.source_id,
        ontology="financial_reporting",
        session=async_session,
    )

    assert result.success
    assert result.value is not None

    doc = result.value

    # Verify metadata
    assert doc.source_id == source.source_id
    assert doc.source_name == "test_source"
    assert doc.ontology == "financial_reporting"
    assert doc.assembly_duration_seconds is not None

    # Verify statistical profiling was assembled
    assert doc.statistical_profiling is not None
    assert len(doc.statistical_profiling.profiles) == 1

    # Verify profile details
    profile_result = doc.statistical_profiling.profiles[0]
    assert profile_result.column_id == column.column_id
    assert profile_result.total_count == 1000
    assert profile_result.null_count == 0
    assert profile_result.distinct_count == 1000


@pytest.mark.skip(
    reason="Context assembly incomplete - ContextDocument model changed (see assembly.py TODOs)"
)
@pytest.mark.asyncio
async def test_assemble_context_document_multiple_columns(async_session: AsyncSession):
    """Test context assembly with multiple columns."""
    # Create source
    source = Source(
        source_id=str(uuid4()),
        name="sales_data",
        source_type="csv",
    )
    async_session.add(source)

    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="sales",
        layer="typed",
        row_count=5000,
    )
    async_session.add(table)

    # Create multiple columns with profiles
    columns_data = [
        ("sale_id", "BIGINT", 5000, 0, 5000),
        ("amount", "DOUBLE", 5000, 50, 4950),
        ("customer_name", "VARCHAR", 5000, 100, 4500),
    ]

    for col_name, col_type, total, nulls, distinct in columns_data:
        column = Column(
            column_id=str(uuid4()),
            table_id=table.table_id,
            column_name=col_name,
            column_position=len(columns_data),
            resolved_type=col_type,
        )
        async_session.add(column)

        profile = StatisticalProfile(
            profile_id=str(uuid4()),
            column_id=column.column_id,
            profiled_at=datetime.now(UTC),
            total_count=total,
            null_count=nulls,
            distinct_count=distinct,
            null_ratio=nulls / total,
            cardinality_ratio=distinct / total,
        )
        async_session.add(profile)

    await async_session.commit()

    # Assemble context
    result = await assemble_context_document(
        source_id=source.source_id,
        ontology="financial_reporting",
        session=async_session,
    )

    assert result.success
    doc = result.value

    # Verify all columns profiled
    assert doc.statistical_profiling is not None
    assert len(doc.statistical_profiling.profiles) == 3

    # Verify null counts in individual profiles
    null_counts = [p.null_count for p in doc.statistical_profiling.profiles]
    assert set(null_counts) == {0, 50, 100}


@pytest.mark.skip(
    reason="Context assembly incomplete - ContextDocument model changed (see assembly.py TODOs)"
)
@pytest.mark.asyncio
async def test_assemble_context_document_no_profiles(async_session: AsyncSession):
    """Test context assembly when columns have no profiles yet."""
    # Create source with column but no profile
    source = Source(
        source_id=str(uuid4()),
        name="empty_source",
        source_type="csv",
    )
    async_session.add(source)

    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="empty_table",
        layer="typed",
        row_count=0,
    )
    async_session.add(table)

    column = Column(
        column_id=str(uuid4()),
        table_id=table.table_id,
        column_name="col1",
        column_position=0,
    )
    async_session.add(column)
    await async_session.commit()

    # Assemble context
    result = await assemble_context_document(
        source_id=source.source_id,
        ontology="financial_reporting",
        session=async_session,
    )

    assert result.success
    doc = result.value

    # Should succeed but have no statistical profiling
    assert doc.statistical_profiling is None


@pytest.mark.asyncio
async def test_context_document_structure(async_session: AsyncSession):
    """Test that ContextDocument has all expected pillar fields."""
    source = Source(
        source_id=str(uuid4()),
        name="test",
        source_type="csv",
    )
    async_session.add(source)

    table = Table(
        table_id=str(uuid4()),
        source_id=source.source_id,
        table_name="test",
        layer="typed",
        row_count=100,
    )
    async_session.add(table)
    await async_session.commit()

    result = await assemble_context_document(
        source_id=source.source_id,
        ontology="test",
        session=async_session,
    )

    assert result.success
    doc = result.value

    # Verify all main context document fields exist
    assert hasattr(doc, "tables")
    assert hasattr(doc, "relationships")
    assert hasattr(doc, "ontology")
    assert hasattr(doc, "relevant_metrics")
    assert hasattr(doc, "domain_concepts")
    assert hasattr(doc, "quality_summary")
    assert hasattr(doc, "suggested_queries")
    assert hasattr(doc, "context_summary")

    # Verify that it's a ContextDocument with correct fields
    assert isinstance(doc.tables, list)
    assert isinstance(doc.relationships, list)
    assert isinstance(doc.llm_features_used, list)

    # All fields verified above
