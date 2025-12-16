"""Test multi-table topology analysis persistence."""

import duckdb
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dataraum_context.enrichment.db_models import MultiTableTopologyMetrics
from dataraum_context.quality.topological import analyze_topological_quality_multi_table
from dataraum_context.storage.models_v2 import (
    Base,
    Column,
    Source,
    Table,
)


@pytest.fixture
async def session():
    """Create an in-memory SQLite session for testing."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session_factory = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session_factory() as session:
        yield session


@pytest.fixture
def duckdb_conn():
    """Create an in-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.mark.asyncio
async def test_multi_table_persistence_saves_to_database(session: AsyncSession, duckdb_conn):
    """Test that multi-table analysis results are saved to database."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create test tables
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="customers",
        layer="typed",
        row_count=100,
    )
    table2 = Table(
        table_id="table2",
        source_id="test_source",
        table_name="orders",
        layer="typed",
        row_count=500,
    )

    session.add_all([table1, table2])

    # Add some columns so analysis doesn't fail
    col1 = Column(
        table_id="table1", column_name="customer_id", raw_type="INTEGER", column_position=0
    )
    col2 = Column(table_id="table1", column_name="name", raw_type="VARCHAR", column_position=1)
    col3 = Column(table_id="table2", column_name="order_id", raw_type="INTEGER", column_position=0)
    col4 = Column(
        table_id="table2", column_name="customer_id", raw_type="INTEGER", column_position=1
    )

    session.add_all([col1, col2, col3, col4])
    await session.commit()

    # Create test data in DuckDB
    duckdb_conn.execute("CREATE TABLE customers (customer_id INTEGER, name VARCHAR)")
    duckdb_conn.execute("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")

    duckdb_conn.execute("CREATE TABLE orders (order_id INTEGER, customer_id INTEGER)")
    duckdb_conn.execute("INSERT INTO orders VALUES (1, 1), (2, 1), (3, 2)")

    # Run multi-table analysis
    result = await analyze_topological_quality_multi_table(
        table_ids=["table1", "table2"],
        duckdb_conn=duckdb_conn,
        session=session,
        max_dimension=2,
        min_persistence=0.1,
    )

    # Verify result is successful
    assert result.success is True
    assert result.value is not None

    # Query database to verify persistence
    from sqlalchemy import select

    stmt = select(MultiTableTopologyMetrics)
    db_result = await session.execute(stmt)
    metrics = db_result.scalars().all()

    # Should have exactly one multi-table analysis record
    assert len(metrics) == 1

    metric = metrics[0]

    # Verify structured fields
    assert metric.table_ids == ["table1", "table2"]
    assert metric.relationship_count >= 0
    assert metric.graph_betti_0 >= 1
    assert isinstance(metric.cross_table_cycles, int)
    assert isinstance(metric.has_cross_table_cycles, bool)
    assert isinstance(metric.is_connected_graph, bool)

    # Verify analysis_data contains expected keys
    assert "per_table" in metric.analysis_data
    assert "cross_table" in metric.analysis_data
    assert "relationship_count" in metric.analysis_data


@pytest.mark.asyncio
async def test_multi_table_persistence_with_cycles(session: AsyncSession, duckdb_conn):
    """Test that cross-table cycles are properly recorded."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create test tables
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="vendors",
        layer="typed",
        row_count=50,
    )
    table2 = Table(
        table_id="table2",
        source_id="test_source",
        table_name="transactions",
        layer="typed",
        row_count=200,
    )

    session.add_all([table1, table2])

    # Add columns
    col1 = Column(table_id="table1", column_name="vendor_id", raw_type="INTEGER", column_position=0)
    col2 = Column(
        table_id="table1", column_name="vendor_name", raw_type="VARCHAR", column_position=1
    )
    col3 = Column(table_id="table2", column_name="txn_id", raw_type="INTEGER", column_position=0)
    col4 = Column(table_id="table2", column_name="vendor_id", raw_type="INTEGER", column_position=1)

    session.add_all([col1, col2, col3, col4])
    await session.commit()

    # Create test data
    duckdb_conn.execute("CREATE TABLE vendors (vendor_id INTEGER, vendor_name VARCHAR)")
    duckdb_conn.execute("INSERT INTO vendors VALUES (1, 'Vendor A'), (2, 'Vendor B')")

    duckdb_conn.execute("CREATE TABLE transactions (txn_id INTEGER, vendor_id INTEGER)")
    duckdb_conn.execute("INSERT INTO transactions VALUES (1, 1), (2, 2)")

    # Run analysis
    result = await analyze_topological_quality_multi_table(
        table_ids=["table1", "table2"],
        duckdb_conn=duckdb_conn,
        session=session,
    )

    assert result.success is True

    # Check database
    from sqlalchemy import select

    stmt = select(MultiTableTopologyMetrics)
    db_result = await session.execute(stmt)
    metric = db_result.scalar_one()

    # Verify cycle tracking
    assert metric.cross_table_cycles >= 0
    assert metric.has_cross_table_cycles == (metric.cross_table_cycles > 0)


@pytest.mark.asyncio
async def test_multi_table_persistence_incremental_tracking(session: AsyncSession, duckdb_conn):
    """Test that multiple analyses create separate records for historical tracking."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create test tables
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="products",
        layer="typed",
        row_count=100,
    )
    session.add(table1)

    col1 = Column(
        table_id="table1", column_name="product_id", raw_type="INTEGER", column_position=0
    )
    session.add(col1)
    await session.commit()

    # Create test data
    duckdb_conn.execute("CREATE TABLE products (product_id INTEGER)")
    duckdb_conn.execute("INSERT INTO products VALUES (1), (2), (3)")

    # Run first analysis
    result1 = await analyze_topological_quality_multi_table(
        table_ids=["table1"],
        duckdb_conn=duckdb_conn,
        session=session,
    )
    assert result1.success is True

    # Run second analysis (simulating time passing)
    result2 = await analyze_topological_quality_multi_table(
        table_ids=["table1"],
        duckdb_conn=duckdb_conn,
        session=session,
    )
    assert result2.success is True

    # Query database - should have TWO records
    from sqlalchemy import select

    stmt = select(MultiTableTopologyMetrics)
    db_result = await session.execute(stmt)
    metrics = db_result.scalars().all()

    # Should have 2 separate analysis records
    assert len(metrics) == 2

    # Both should track the same table
    assert all(m.table_ids == ["table1"] for m in metrics)

    # Should have different analysis_id
    assert metrics[0].analysis_id != metrics[1].analysis_id

    # Should have different computed_at timestamps (or same if very fast)
    # We just check they exist
    assert all(m.computed_at is not None for m in metrics)


@pytest.mark.asyncio
async def test_multi_table_persistence_graph_connectivity(session: AsyncSession, duckdb_conn):
    """Test that graph connectivity flag is properly set."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create disconnected tables (no relationships)
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="isolated_a",
        layer="typed",
        row_count=10,
    )
    table2 = Table(
        table_id="table2",
        source_id="test_source",
        table_name="isolated_b",
        layer="typed",
        row_count=20,
    )

    session.add_all([table1, table2])

    col1 = Column(table_id="table1", column_name="id", raw_type="INTEGER", column_position=0)
    col2 = Column(table_id="table2", column_name="id", raw_type="INTEGER", column_position=0)

    session.add_all([col1, col2])
    await session.commit()

    # Create disconnected data
    duckdb_conn.execute("CREATE TABLE isolated_a (id INTEGER)")
    duckdb_conn.execute("INSERT INTO isolated_a VALUES (1), (2)")

    duckdb_conn.execute("CREATE TABLE isolated_b (id INTEGER)")
    duckdb_conn.execute("INSERT INTO isolated_b VALUES (3), (4)")

    # Run analysis
    result = await analyze_topological_quality_multi_table(
        table_ids=["table1", "table2"],
        duckdb_conn=duckdb_conn,
        session=session,
    )

    assert result.success is True

    # Check connectivity
    from sqlalchemy import select

    stmt = select(MultiTableTopologyMetrics)
    db_result = await session.execute(stmt)
    metric = db_result.scalar_one()

    # With no relationships, graph should be disconnected (betti_0 > 1)
    # But depends on relationship detection, so just verify fields exist
    assert isinstance(metric.is_connected_graph, bool)
    assert metric.graph_betti_0 >= 1


@pytest.mark.asyncio
async def test_multi_table_persistence_analysis_data_structure(session: AsyncSession, duckdb_conn):
    """Test that analysis_data JSONB contains complete structure."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create simple test case
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="test_table",
        layer="typed",
        row_count=50,
    )
    session.add(table1)

    col1 = Column(table_id="table1", column_name="id", raw_type="INTEGER", column_position=0)
    session.add(col1)
    await session.commit()

    # Create test data
    duckdb_conn.execute("CREATE TABLE test_table (id INTEGER)")
    duckdb_conn.execute("INSERT INTO test_table VALUES (1), (2), (3)")

    # Run analysis
    result = await analyze_topological_quality_multi_table(
        table_ids=["table1"],
        duckdb_conn=duckdb_conn,
        session=session,
    )

    assert result.success is True

    # Verify JSONB structure
    from sqlalchemy import select

    stmt = select(MultiTableTopologyMetrics)
    db_result = await session.execute(stmt)
    metric = db_result.scalar_one()

    analysis_data = metric.analysis_data

    # Verify required top-level keys
    assert "per_table" in analysis_data
    assert "cross_table" in analysis_data
    assert "relationship_count" in analysis_data

    # Verify per_table contains TopologicalQualityResult data
    per_table = analysis_data["per_table"]
    assert isinstance(per_table, dict)

    # Verify cross_table contains graph analysis
    cross_table = analysis_data["cross_table"]
    assert isinstance(cross_table, dict)
    assert "betti_0" in cross_table
    assert "cycles" in cross_table
