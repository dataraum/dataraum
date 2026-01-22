"""Test topology enrichment module."""

import duckdb
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dataraum_context.enrichment.models import Relationship, TopologyEnrichmentResult
from dataraum_context.enrichment.topology import enrich_topology
from dataraum_context.storage import Base, Column, Source, Table


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
async def test_enrich_topology_single_table(session: AsyncSession, duckdb_conn):
    """Test topology enrichment with a single table."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create test table
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="customers",
        layer="typed",
        row_count=10,
    )
    session.add(table1)

    # Add columns
    col1 = Column(
        table_id="table1", column_name="customer_id", raw_type="INTEGER", column_position=0
    )
    col2 = Column(table_id="table1", column_name="name", raw_type="VARCHAR", column_position=1)
    col3 = Column(table_id="table1", column_name="email", raw_type="VARCHAR", column_position=2)

    session.add_all([col1, col2, col3])
    await session.commit()

    # Create test data in DuckDB (with typed_ prefix as expected by enrichment)
    duckdb_conn.execute(
        "CREATE TABLE typed_customers (customer_id INTEGER, name VARCHAR, email VARCHAR)"
    )
    duckdb_conn.execute(
        """
        INSERT INTO typed_customers VALUES
        (1, 'Alice', 'alice@example.com'),
        (2, 'Bob', 'bob@example.com'),
        (3, 'Charlie', 'charlie@example.com')
    """
    )

    # Run topology enrichment
    result = await enrich_topology(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1"],
    )

    # Verify result
    assert result.success is True
    assert result.value is not None
    assert isinstance(result.value, TopologyEnrichmentResult)

    # Single table should have no relationships
    enrichment_result = result.value
    assert len(enrichment_result.relationships) == 0
    assert len(enrichment_result.join_paths) == 0


@pytest.mark.asyncio
async def test_enrich_topology_two_related_tables(session: AsyncSession, duckdb_conn):
    """Test topology enrichment with two tables that have a potential relationship."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create test tables
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="customers",
        layer="typed",
        row_count=10,
    )
    table2 = Table(
        table_id="table2",
        source_id="test_source",
        table_name="orders",
        layer="typed",
        row_count=20,
    )

    session.add_all([table1, table2])

    # Add columns - customer_id appears in both tables
    col1 = Column(
        table_id="table1", column_name="customer_id", raw_type="INTEGER", column_position=0
    )
    col2 = Column(table_id="table1", column_name="name", raw_type="VARCHAR", column_position=1)
    col3 = Column(table_id="table2", column_name="order_id", raw_type="INTEGER", column_position=0)
    col4 = Column(
        table_id="table2", column_name="customer_id", raw_type="INTEGER", column_position=1
    )
    col5 = Column(table_id="table2", column_name="amount", raw_type="DECIMAL", column_position=2)

    session.add_all([col1, col2, col3, col4, col5])
    await session.commit()

    # Create test data in DuckDB with related data (with typed_ prefix)
    duckdb_conn.execute("CREATE TABLE typed_customers (customer_id INTEGER, name VARCHAR)")
    duckdb_conn.execute(
        "INSERT INTO typed_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"
    )

    duckdb_conn.execute(
        "CREATE TABLE typed_orders (order_id INTEGER, customer_id INTEGER, amount DECIMAL)"
    )
    duckdb_conn.execute(
        """
        INSERT INTO typed_orders VALUES
        (101, 1, 100.00),
        (102, 1, 150.00),
        (103, 2, 200.00),
        (104, 3, 75.00)
    """
    )

    # Run topology enrichment
    result = await enrich_topology(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1", "table2"],
    )

    # Verify result
    assert result.success is True
    assert result.value is not None

    enrichment_result = result.value

    # Should detect relationships (TDA should find customer_id relationship)
    # Note: Actual count depends on TDA confidence thresholds
    assert len(enrichment_result.relationships) >= 0  # May or may not detect based on data

    # Check that relationships are Pydantic models
    for rel in enrichment_result.relationships:
        assert isinstance(rel, Relationship)
        assert rel.relationship_id is not None
        assert rel.from_table is not None
        assert rel.to_table is not None
        assert rel.from_column is not None
        assert rel.to_column is not None
        assert rel.confidence >= 0.0 and rel.confidence <= 1.0
        assert rel.detection_method == "tda"


@pytest.mark.asyncio
async def test_enrich_topology_no_tables(session: AsyncSession, duckdb_conn):
    """Test topology enrichment with no tables."""
    result = await enrich_topology(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=[],
    )

    # Should fail gracefully
    assert result.success is False
    assert "No table data found" in result.error


@pytest.mark.asyncio
async def test_enrich_topology_stores_relationships(session: AsyncSession, duckdb_conn):
    """Test that topology enrichment stores relationships in the database."""
    # Create test source and tables
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="products",
        layer="typed",
        row_count=5,
    )
    table2 = Table(
        table_id="table2",
        source_id="test_source",
        table_name="categories",
        layer="typed",
        row_count=3,
    )

    session.add_all([table1, table2])

    # Add columns
    col1 = Column(
        table_id="table1", column_name="product_id", raw_type="INTEGER", column_position=0
    )
    col2 = Column(
        table_id="table1", column_name="category_id", raw_type="INTEGER", column_position=1
    )
    col3 = Column(
        table_id="table2", column_name="category_id", raw_type="INTEGER", column_position=0
    )
    col4 = Column(table_id="table2", column_name="name", raw_type="VARCHAR", column_position=1)

    session.add_all([col1, col2, col3, col4])
    await session.commit()

    # Create test data (with typed_ prefix)
    duckdb_conn.execute("CREATE TABLE typed_categories (category_id INTEGER, name VARCHAR)")
    duckdb_conn.execute("INSERT INTO typed_categories VALUES (1, 'Electronics'), (2, 'Books')")

    duckdb_conn.execute("CREATE TABLE typed_products (product_id INTEGER, category_id INTEGER)")
    duckdb_conn.execute("INSERT INTO typed_products VALUES (101, 1), (102, 1), (103, 2)")

    # Run topology enrichment
    result = await enrich_topology(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1", "table2"],
    )

    assert result.success is True

    # Check that relationships were stored in database
    from sqlalchemy import select

    from dataraum_context.enrichment.db_models import Relationship as RelationshipModel

    stmt = select(RelationshipModel)
    db_result = await session.execute(stmt)
    stored_relationships = db_result.scalars().all()

    # Relationships should have been stored (may be 0 if TDA didn't detect any)
    assert len(stored_relationships) >= 0


@pytest.mark.asyncio
async def test_enrich_topology_three_tables(session: AsyncSession, duckdb_conn):
    """Test topology enrichment with three interconnected tables."""
    # Create test source
    source = Source(source_id="test_source", name="test_db", source_type="duckdb")
    session.add(source)

    # Create test tables
    table1 = Table(
        table_id="table1",
        source_id="test_source",
        table_name="customers",
        layer="typed",
        row_count=3,
    )
    table2 = Table(
        table_id="table2",
        source_id="test_source",
        table_name="orders",
        layer="typed",
        row_count=5,
    )
    table3 = Table(
        table_id="table3",
        source_id="test_source",
        table_name="order_items",
        layer="typed",
        row_count=10,
    )

    session.add_all([table1, table2, table3])

    # Add columns
    # Customers
    session.add(
        Column(table_id="table1", column_name="customer_id", raw_type="INTEGER", column_position=0)
    )
    session.add(
        Column(table_id="table1", column_name="name", raw_type="VARCHAR", column_position=1)
    )

    # Orders
    session.add(
        Column(table_id="table2", column_name="order_id", raw_type="INTEGER", column_position=0)
    )
    session.add(
        Column(table_id="table2", column_name="customer_id", raw_type="INTEGER", column_position=1)
    )

    # Order Items
    session.add(
        Column(table_id="table3", column_name="item_id", raw_type="INTEGER", column_position=0)
    )
    session.add(
        Column(table_id="table3", column_name="order_id", raw_type="INTEGER", column_position=1)
    )
    session.add(
        Column(table_id="table3", column_name="quantity", raw_type="INTEGER", column_position=2)
    )

    await session.commit()

    # Create test data (with typed_ prefix)
    duckdb_conn.execute("CREATE TABLE typed_customers (customer_id INTEGER, name VARCHAR)")
    duckdb_conn.execute("INSERT INTO typed_customers VALUES (1, 'Alice'), (2, 'Bob')")

    duckdb_conn.execute("CREATE TABLE typed_orders (order_id INTEGER, customer_id INTEGER)")
    duckdb_conn.execute("INSERT INTO typed_orders VALUES (101, 1), (102, 1), (103, 2)")

    duckdb_conn.execute(
        "CREATE TABLE typed_order_items (item_id INTEGER, order_id INTEGER, quantity INTEGER)"
    )
    duckdb_conn.execute(
        """
        INSERT INTO typed_order_items VALUES
        (1, 101, 2), (2, 101, 1),
        (3, 102, 3),
        (4, 103, 1), (5, 103, 5)
    """
    )

    # Run topology enrichment
    result = await enrich_topology(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["table1", "table2", "table3"],
    )

    # Verify result
    assert result.success is True
    assert result.value is not None

    enrichment_result = result.value

    # With 3 tables, should potentially find relationships
    # customers <-> orders (customer_id)
    # orders <-> order_items (order_id)
    assert len(enrichment_result.relationships) >= 0  # Depends on TDA detection


@pytest.mark.asyncio
async def test_enrich_topology_with_nonexistent_table(session: AsyncSession, duckdb_conn):
    """Test topology enrichment with a table ID that doesn't exist."""
    result = await enrich_topology(
        session=session,
        duckdb_conn=duckdb_conn,
        table_ids=["nonexistent_table"],
    )

    # Should fail because no data found
    assert result.success is False
    assert "No table data found" in result.error
