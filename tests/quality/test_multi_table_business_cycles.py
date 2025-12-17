"""Tests for multi-table business cycle detection.

Tests the cross-table cycle analysis for financial datasets:
- Relationship gathering
- Cross-table cycle detection
- LLM business cycle classification (with mock)
- Holistic interpretation
"""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from dataraum_context.core.models.base import Cardinality, RelationshipType, Result
from dataraum_context.enrichment.db_models import Relationship
from dataraum_context.quality.domains.financial_orchestrator import (
    analyze_complete_financial_dataset_quality,
    classify_cross_table_cycle_with_llm,
)
from dataraum_context.quality.topological import analyze_relationship_graph
from dataraum_context.storage import Base, Column, Source, Table


@pytest.fixture
async def multi_table_db_session():
    """Create an in-memory SQLite session with multiple related tables."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async_session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with async_session() as session:
        # Create test source
        source = Source(
            source_id=str(uuid4()),
            name="test_financial_dataset",
            source_type="csv",
            connection_config={},
        )
        session.add(source)
        await session.commit()

        # Create tables mimicking finance_csv_example
        tables = {}

        # Transactions table (central hub)
        transactions_table = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="transactions",
            layer="typed",
            row_count=1000,
            duckdb_path="transactions",
        )
        session.add(transactions_table)
        tables["transactions"] = transactions_table

        # Customers table
        customers_table = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="customers",
            layer="typed",
            row_count=100,
            duckdb_path="customers",
        )
        session.add(customers_table)
        tables["customers"] = customers_table

        # Vendors table
        vendors_table = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="vendors",
            layer="typed",
            row_count=50,
            duckdb_path="vendors",
        )
        session.add(vendors_table)
        tables["vendors"] = vendors_table

        # Products table
        products_table = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="products",
            layer="typed",
            row_count=200,
            duckdb_path="products",
        )
        session.add(products_table)
        tables["products"] = products_table

        # Chart of accounts table
        accounts_table = Table(
            table_id=str(uuid4()),
            source_id=source.source_id,
            table_name="chart_of_accounts",
            layer="typed",
            row_count=50,
            duckdb_path="chart_of_accounts",
        )
        session.add(accounts_table)
        tables["chart_of_accounts"] = accounts_table

        await session.commit()

        # Create columns for each table
        columns = {}

        # Transactions columns
        txn_cols = [
            ("transaction_id", "INTEGER"),
            ("customer_name", "VARCHAR"),
            ("vendor_name", "VARCHAR"),
            ("product_service", "VARCHAR"),
            ("account", "VARCHAR"),
            ("amount", "DOUBLE"),
            ("ar_paid", "VARCHAR"),
            ("ap_paid", "VARCHAR"),
            ("debit", "DOUBLE"),
            ("credit", "DOUBLE"),
        ]
        for i, (name, dtype) in enumerate(txn_cols):
            col = Column(
                column_id=str(uuid4()),
                table_id=transactions_table.table_id,
                column_name=name,
                column_position=i,
                resolved_type=dtype,
            )
            session.add(col)
            columns[f"transactions.{name}"] = col

        # Customers columns
        for i, (name, dtype) in enumerate(
            [
                ("customer_name", "VARCHAR"),
                ("customer_id", "INTEGER"),
                ("balance", "DOUBLE"),
            ]
        ):
            col = Column(
                column_id=str(uuid4()),
                table_id=customers_table.table_id,
                column_name=name,
                column_position=i,
                resolved_type=dtype,
            )
            session.add(col)
            columns[f"customers.{name}"] = col

        # Vendors columns
        for i, (name, dtype) in enumerate(
            [
                ("vendor_name", "VARCHAR"),
                ("vendor_id", "INTEGER"),
                ("balance", "DOUBLE"),
            ]
        ):
            col = Column(
                column_id=str(uuid4()),
                table_id=vendors_table.table_id,
                column_name=name,
                column_position=i,
                resolved_type=dtype,
            )
            session.add(col)
            columns[f"vendors.{name}"] = col

        # Products columns
        for i, (name, dtype) in enumerate(
            [
                ("product_service", "VARCHAR"),
                ("product_type", "VARCHAR"),
            ]
        ):
            col = Column(
                column_id=str(uuid4()),
                table_id=products_table.table_id,
                column_name=name,
                column_position=i,
                resolved_type=dtype,
            )
            session.add(col)
            columns[f"products.{name}"] = col

        # Chart of accounts columns
        for i, (name, dtype) in enumerate(
            [
                ("account_name", "VARCHAR"),
                ("account_type", "VARCHAR"),
            ]
        ):
            col = Column(
                column_id=str(uuid4()),
                table_id=accounts_table.table_id,
                column_name=name,
                column_position=i,
                resolved_type=dtype,
            )
            session.add(col)
            columns[f"chart_of_accounts.{name}"] = col

        await session.commit()

        yield session, tables, columns


@pytest.fixture
async def multi_table_with_star_schema(multi_table_db_session):
    """Add relationships that form a star schema (no cycles).

    This represents a typical financial dataset:
    - transactions (fact table) references customers, vendors, products
    - No circular references (proper star schema design)
    """
    session, tables, columns = multi_table_db_session

    relationships = []

    # transactions → customers (FK via customer_name)
    rel1 = Relationship(
        relationship_id=str(uuid4()),
        from_table_id=tables["transactions"].table_id,
        to_table_id=tables["customers"].table_id,
        from_column_id=columns["transactions.customer_name"].column_id,
        to_column_id=columns["customers.customer_name"].column_id,
        relationship_type=RelationshipType.FOREIGN_KEY.value,
        cardinality=Cardinality.ONE_TO_MANY.value,
        confidence=0.9,
        detection_method="semantic",
        evidence={"column_match": "customer_name"},
    )
    session.add(rel1)
    relationships.append(rel1)

    # transactions → vendors (FK via vendor_name)
    rel2 = Relationship(
        relationship_id=str(uuid4()),
        from_table_id=tables["transactions"].table_id,
        to_table_id=tables["vendors"].table_id,
        from_column_id=columns["transactions.vendor_name"].column_id,
        to_column_id=columns["vendors.vendor_name"].column_id,
        relationship_type=RelationshipType.FOREIGN_KEY.value,
        cardinality=Cardinality.ONE_TO_MANY.value,
        confidence=0.85,
        detection_method="semantic",
        evidence={"column_match": "vendor_name"},
    )
    session.add(rel2)
    relationships.append(rel2)

    # transactions → products
    rel3 = Relationship(
        relationship_id=str(uuid4()),
        from_table_id=tables["transactions"].table_id,
        to_table_id=tables["products"].table_id,
        from_column_id=columns["transactions.product_service"].column_id,
        to_column_id=columns["products.product_service"].column_id,
        relationship_type=RelationshipType.FOREIGN_KEY.value,
        cardinality=Cardinality.ONE_TO_MANY.value,
        confidence=0.8,
        detection_method="semantic",
        evidence={"column_match": "product_service"},
    )
    session.add(rel3)
    relationships.append(rel3)

    await session.commit()

    yield session, tables, columns, relationships


@pytest.fixture
async def multi_table_with_cycle(multi_table_db_session):
    """Add relationships that form a 3-table cycle.

    Cycle: transactions → customers → vendors → transactions
    This is artificial but tests the cycle detection algorithm.

    In real data, cycles might occur with:
    - Intercompany transactions (entity A bills entity B, B bills C, C bills A)
    - Inventory flows (supplier → warehouse → retailer → supplier returns)
    """
    session, tables, columns = multi_table_db_session

    # Add a column to customers for vendor reference
    customer_vendor_ref = Column(
        column_id=str(uuid4()),
        table_id=tables["customers"].table_id,
        column_name="preferred_vendor_name",
        column_position=10,
        resolved_type="VARCHAR",
    )
    session.add(customer_vendor_ref)

    # Add a column to vendors for transaction reference
    vendor_txn_ref = Column(
        column_id=str(uuid4()),
        table_id=tables["vendors"].table_id,
        column_name="last_transaction_id",
        column_position=10,
        resolved_type="INTEGER",
    )
    session.add(vendor_txn_ref)
    await session.commit()

    columns["customers.preferred_vendor_name"] = customer_vendor_ref
    columns["vendors.last_transaction_id"] = vendor_txn_ref

    relationships = []

    # Edge 1: transactions → customers
    rel1 = Relationship(
        relationship_id=str(uuid4()),
        from_table_id=tables["transactions"].table_id,
        to_table_id=tables["customers"].table_id,
        from_column_id=columns["transactions.customer_name"].column_id,
        to_column_id=columns["customers.customer_name"].column_id,
        relationship_type=RelationshipType.FOREIGN_KEY.value,
        cardinality=Cardinality.ONE_TO_MANY.value,
        confidence=0.9,
        detection_method="semantic",
        evidence={"column_match": "customer_name"},
    )
    session.add(rel1)
    relationships.append(rel1)

    # Edge 2: customers → vendors (customer has preferred vendor)
    rel2 = Relationship(
        relationship_id=str(uuid4()),
        from_table_id=tables["customers"].table_id,
        to_table_id=tables["vendors"].table_id,
        from_column_id=columns["customers.preferred_vendor_name"].column_id,
        to_column_id=columns["vendors.vendor_name"].column_id,
        relationship_type=RelationshipType.FOREIGN_KEY.value,
        cardinality=Cardinality.ONE_TO_MANY.value,
        confidence=0.85,
        detection_method="semantic",
        evidence={"column_match": "preferred_vendor"},
    )
    session.add(rel2)
    relationships.append(rel2)

    # Edge 3: vendors → transactions (vendor tracks last transaction)
    # This creates the cycle: transactions → customers → vendors → transactions
    rel3 = Relationship(
        relationship_id=str(uuid4()),
        from_table_id=tables["vendors"].table_id,
        to_table_id=tables["transactions"].table_id,
        from_column_id=columns["vendors.last_transaction_id"].column_id,
        to_column_id=columns["transactions.transaction_id"].column_id,
        relationship_type=RelationshipType.FOREIGN_KEY.value,
        cardinality=Cardinality.ONE_TO_MANY.value,
        confidence=0.8,
        detection_method="semantic",
        evidence={"column_match": "transaction_id"},
    )
    session.add(rel3)
    relationships.append(rel3)

    await session.commit()

    yield session, tables, columns, relationships


@pytest.fixture
def duckdb_conn_multi_table():
    """Create DuckDB with multiple related tables."""
    conn = duckdb.connect(":memory:")

    # Create transactions table
    conn.execute("""
        CREATE TABLE transactions (
            transaction_id INTEGER,
            customer_name VARCHAR,
            vendor_name VARCHAR,
            product_service VARCHAR,
            account VARCHAR,
            amount DOUBLE,
            ar_paid VARCHAR,
            ap_paid VARCHAR,
            debit DOUBLE,
            credit DOUBLE
        )
    """)

    # Insert sample transactions
    conn.execute("""
        INSERT INTO transactions VALUES
            (1, 'Customer A', NULL, 'Service 1', 'Revenue', 1000, 'Paid', NULL, 0, 1000),
            (2, 'Customer B', NULL, 'Product 1', 'Revenue', 500, 'Pending', NULL, 0, 500),
            (3, NULL, 'Vendor X', 'Supplies', 'Expense', 300, NULL, 'Paid', 300, 0),
            (4, NULL, 'Vendor Y', 'Equipment', 'Asset', 2000, NULL, 'Pending', 2000, 0),
            (5, 'Customer A', NULL, 'Service 2', 'Revenue', 750, 'Paid', NULL, 0, 750)
    """)

    # Create customers table
    conn.execute("""
        CREATE TABLE customers (
            customer_name VARCHAR,
            customer_id INTEGER,
            balance DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO customers VALUES
            ('Customer A', 1, 1750),
            ('Customer B', 2, 500)
    """)

    # Create vendors table
    conn.execute("""
        CREATE TABLE vendors (
            vendor_name VARCHAR,
            vendor_id INTEGER,
            balance DOUBLE
        )
    """)
    conn.execute("""
        INSERT INTO vendors VALUES
            ('Vendor X', 1, 300),
            ('Vendor Y', 2, 2000)
    """)

    # Create products table
    conn.execute("""
        CREATE TABLE products (
            product_service VARCHAR,
            product_type VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO products VALUES
            ('Service 1', 'Service'),
            ('Service 2', 'Service'),
            ('Product 1', 'Product'),
            ('Supplies', 'Supply'),
            ('Equipment', 'Asset')
    """)

    # Create chart_of_accounts table
    conn.execute("""
        CREATE TABLE chart_of_accounts (
            account_name VARCHAR,
            account_type VARCHAR
        )
    """)
    conn.execute("""
        INSERT INTO chart_of_accounts VALUES
            ('Revenue', 'Income'),
            ('Expense', 'Expense'),
            ('Asset', 'Asset'),
            ('Liability', 'Liability')
    """)

    yield conn
    conn.close()


@pytest.fixture
def mock_llm_provider():
    """Create a mock LLM provider for testing."""
    mock_provider = AsyncMock()

    # Mock successful classification response
    def create_classification_response(cycle_tables: list[str]) -> str:
        """Generate mock classification based on tables in cycle."""
        import json

        # Determine cycle type based on tables
        cycle_types = []
        primary_type = "unknown_cycle"

        table_names = [t.lower() for t in cycle_tables]

        if "customers" in table_names or "customer" in str(table_names):
            cycle_types.append({"type": "accounts_receivable_cycle", "confidence": 0.85})
            primary_type = "accounts_receivable_cycle"

        if "vendors" in table_names or "vendor" in str(table_names):
            cycle_types.append({"type": "expense_cycle", "confidence": 0.80})
            if primary_type == "unknown_cycle":
                primary_type = "expense_cycle"

        if "products" in table_names or "product" in str(table_names):
            cycle_types.append({"type": "inventory_cycle", "confidence": 0.70})
            if primary_type == "unknown_cycle":
                primary_type = "inventory_cycle"

        if not cycle_types:
            cycle_types.append({"type": "unknown_cycle", "confidence": 0.5})

        return json.dumps(
            {
                "cycle_types": cycle_types,
                "primary_type": primary_type,
                "explanation": f"Cycle involving tables: {', '.join(cycle_tables)}",
                "business_value": "high"
                if primary_type in ["accounts_receivable_cycle", "expense_cycle"]
                else "medium",
                "completeness": "complete",
                "missing_elements": None,
            }
        )

    # Mock interpretation response
    interpretation_response = {
        "overall_quality_score": 0.82,
        "business_process_health": {
            "accounts_receivable": "healthy",
            "expense": "healthy",
            "inventory": "partial",
        },
        "critical_issues": [],
        "recommendations": [
            {
                "priority": "MEDIUM",
                "action": "Add payroll cycle tables",
                "rationale": "Common business process not detected",
            }
        ],
        "summary": "Dataset has healthy AR and expense cycles. Consider adding payroll tracking.",
    }

    async def mock_complete(request):
        """Mock LLM completion."""
        import json

        prompt = request.prompt

        # Check if this is a cycle classification or interpretation request
        if "Cross-Table Cycle Analysis" in prompt:
            # Extract table info from prompt to generate appropriate response
            # Simple heuristic: look for table names
            if "customer" in prompt.lower():
                content = create_classification_response(["transactions", "customers"])
            elif "vendor" in prompt.lower():
                content = create_classification_response(["transactions", "vendors"])
            else:
                content = create_classification_response(["unknown"])
        else:
            # Interpretation request
            content = json.dumps(interpretation_response)

        mock_response = MagicMock()
        mock_response.content = content
        return Result.ok(mock_response)

    mock_provider.complete = mock_complete

    return mock_provider


# =============================================================================
# Tests
# =============================================================================


class TestAnalyzeRelationshipGraph:
    """Tests for the graph cycle detection algorithm."""

    def test_detects_simple_cycle(self):
        """Test detection of a simple 2-node cycle."""
        table_ids = ["table_a", "table_b"]

        # Create mock relationships that form a cycle
        rel1 = MagicMock()
        rel1.from_table_id = "table_a"
        rel1.to_table_id = "table_b"
        rel1.confidence = 0.9
        rel1.relationship_type = "foreign_key"
        rel1.cardinality = "many_to_one"

        rel2 = MagicMock()
        rel2.from_table_id = "table_b"
        rel2.to_table_id = "table_a"
        rel2.confidence = 0.9
        rel2.relationship_type = "foreign_key"
        rel2.cardinality = "one_to_many"

        result = analyze_relationship_graph(table_ids, [rel1, rel2])

        assert "cycles" in result
        assert len(result["cycles"]) >= 1
        assert result["betti_0"] == 1  # One connected component

    def test_detects_no_cycles_in_tree(self):
        """Test that no cycles are detected in a tree structure."""
        table_ids = ["root", "child1", "child2"]

        # Tree structure: root → child1, root → child2
        rel1 = MagicMock()
        rel1.from_table_id = "root"
        rel1.to_table_id = "child1"
        rel1.confidence = 0.9
        rel1.relationship_type = "foreign_key"
        rel1.cardinality = "one_to_many"

        rel2 = MagicMock()
        rel2.from_table_id = "root"
        rel2.to_table_id = "child2"
        rel2.confidence = 0.9
        rel2.relationship_type = "foreign_key"
        rel2.cardinality = "one_to_many"

        result = analyze_relationship_graph(table_ids, [rel1, rel2])

        assert result["cycles"] == []
        assert result["betti_0"] == 1  # Still one connected component

    def test_detects_disconnected_components(self):
        """Test detection of disconnected graph components."""
        table_ids = ["a", "b", "c", "d"]

        # Two disconnected pairs: a↔b, c↔d
        rel1 = MagicMock()
        rel1.from_table_id = "a"
        rel1.to_table_id = "b"
        rel1.confidence = 0.9
        rel1.relationship_type = "foreign_key"
        rel1.cardinality = "many_to_one"

        rel2 = MagicMock()
        rel2.from_table_id = "c"
        rel2.to_table_id = "d"
        rel2.confidence = 0.9
        rel2.relationship_type = "foreign_key"
        rel2.cardinality = "many_to_one"

        result = analyze_relationship_graph(table_ids, [rel1, rel2])

        assert result["betti_0"] == 2  # Two connected components


class TestMultiTableAnalysisNoLLM:
    """Tests for multi-table analysis without LLM."""

    @pytest.mark.asyncio
    async def test_analysis_without_llm_returns_raw_data(
        self, multi_table_with_star_schema, duckdb_conn_multi_table
    ):
        """Test that analysis without LLM returns raw data structure."""
        session, tables, columns, relationships = multi_table_with_star_schema

        table_ids = [t.table_id for t in tables.values()]

        result = await analyze_complete_financial_dataset_quality(
            table_ids=table_ids,
            duckdb_conn=duckdb_conn_multi_table,
            session=session,
            llm_provider=None,  # No LLM
        )

        assert result.success
        data = result.unwrap()

        # Should return raw data without classification
        assert data["llm_available"] is False
        assert "per_table_metrics" in data
        assert "relationships" in data
        assert "cross_table_cycles" in data
        assert data["classified_cycles"] == []

    @pytest.mark.asyncio
    async def test_star_schema_has_no_cycles(
        self, multi_table_with_star_schema, duckdb_conn_multi_table
    ):
        """Test that a proper star schema has no FK cycles.

        A star schema (fact table referencing dimension tables) should
        not have circular references - that's good database design.
        """
        session, tables, columns, relationships = multi_table_with_star_schema

        table_ids = [t.table_id for t in tables.values()]

        result = await analyze_complete_financial_dataset_quality(
            table_ids=table_ids,
            duckdb_conn=duckdb_conn_multi_table,
            session=session,
            llm_provider=None,
        )

        assert result.success
        data = result.unwrap()

        # Star schema should have no cycles
        assert len(data["cross_table_cycles"]) == 0

        # But should have relationships
        assert len(data["relationships"]) > 0

    @pytest.mark.asyncio
    async def test_detects_three_table_cycle(self, multi_table_with_cycle, duckdb_conn_multi_table):
        """Test detection of a 3-table cycle: transactions → customers → vendors → transactions."""
        session, tables, columns, relationships = multi_table_with_cycle

        table_ids = [t.table_id for t in tables.values()]

        result = await analyze_complete_financial_dataset_quality(
            table_ids=table_ids,
            duckdb_conn=duckdb_conn_multi_table,
            session=session,
            llm_provider=None,
        )

        assert result.success
        data = result.unwrap()

        # Should detect at least one cycle
        cycles = data["cross_table_cycles"]
        assert len(cycles) >= 1, "Should detect at least one cycle"

        # The cycle should involve transactions, customers, and vendors
        transactions_id = tables["transactions"].table_id
        customers_id = tables["customers"].table_id
        vendors_id = tables["vendors"].table_id

        # Find a cycle that includes all three tables
        has_full_cycle = any(
            transactions_id in cycle and customers_id in cycle and vendors_id in cycle
            for cycle in cycles
        )

        assert has_full_cycle, (
            "Should detect a cycle involving transactions, customers, and vendors"
        )


class TestMultiTableAnalysisWithLLM:
    """Tests for multi-table analysis with mock LLM."""

    @pytest.mark.asyncio
    async def test_llm_classifies_cycles(
        self, multi_table_with_cycle, duckdb_conn_multi_table, mock_llm_provider
    ):
        """Test that LLM classifies detected cycles."""
        session, tables, columns, relationships = multi_table_with_cycle

        table_ids = [t.table_id for t in tables.values()]

        result = await analyze_complete_financial_dataset_quality(
            table_ids=table_ids,
            duckdb_conn=duckdb_conn_multi_table,
            session=session,
            llm_provider=mock_llm_provider,
        )

        assert result.success
        data = result.unwrap()

        # Should have LLM classification
        assert data["llm_available"] is True
        assert len(data["classified_cycles"]) > 0

        # Check classification structure
        for classified in data["classified_cycles"]:
            assert "cycle_types" in classified
            assert "primary_type" in classified
            assert "explanation" in classified
            assert "business_value" in classified

    @pytest.mark.asyncio
    async def test_llm_provides_interpretation(
        self, multi_table_with_star_schema, duckdb_conn_multi_table, mock_llm_provider
    ):
        """Test that LLM provides holistic interpretation even without cycles."""
        session, tables, columns, relationships = multi_table_with_star_schema

        table_ids = [t.table_id for t in tables.values()]

        result = await analyze_complete_financial_dataset_quality(
            table_ids=table_ids,
            duckdb_conn=duckdb_conn_multi_table,
            session=session,
            llm_provider=mock_llm_provider,
        )

        assert result.success
        data = result.unwrap()

        # Should have interpretation (even without cycles)
        assert data["interpretation"] is not None
        interpretation = data["interpretation"]

        assert "overall_quality_score" in interpretation
        assert "business_process_health" in interpretation
        assert "recommendations" in interpretation
        assert "summary" in interpretation


class TestClassifyCrossTableCycleWithLLM:
    """Tests for the cycle classification function directly."""

    @pytest.mark.asyncio
    async def test_classification_returns_multi_label(self, mock_llm_provider):
        """Test that classification supports multiple labels."""
        from dataraum_context.analysis.correlation.models import (
            EnrichedRelationship,
        )

        # Create test cycle
        cycle_table_ids = ["transactions_id", "customers_id"]

        # Create mock relationship
        relationships = [
            EnrichedRelationship(
                relationship_id=str(uuid4()),
                from_table="transactions",
                from_column="customer_name",
                from_column_id=str(uuid4()),
                to_table="customers",
                to_column="customer_name",
                to_column_id=str(uuid4()),
                from_table_id="transactions_id",
                to_table_id="customers_id",
                relationship_type=RelationshipType.FOREIGN_KEY,
                cardinality=Cardinality.ONE_TO_MANY,
                confidence=0.9,
                detection_method="semantic",
                evidence={},
            )
        ]

        table_semantics = {
            "transactions_id": {
                "table_name": "transactions",
                "key_columns": ["transaction_id", "customer_name"],
                "semantic_roles": {"customer_name": "customer_identifier"},
            },
            "customers_id": {
                "table_name": "customers",
                "key_columns": ["customer_id"],
                "semantic_roles": {},
            },
        }

        result = await classify_cross_table_cycle_with_llm(
            cycle_table_ids=cycle_table_ids,
            relationships=relationships,
            table_semantics=table_semantics,
            llm_provider=mock_llm_provider,
        )

        assert result.success
        classification = result.unwrap()

        # Should have cycle_types list (multi-label)
        assert "cycle_types" in classification
        assert isinstance(classification["cycle_types"], list)
        assert len(classification["cycle_types"]) >= 1

        # Each type should have type and confidence
        for ct in classification["cycle_types"]:
            assert "type" in ct
            assert "confidence" in ct
            assert 0 <= ct["confidence"] <= 1


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_empty_table_list(self, multi_table_db_session, duckdb_conn_multi_table):
        """Test handling of empty table list."""
        session, tables, columns = multi_table_db_session

        result = await analyze_complete_financial_dataset_quality(
            table_ids=[],
            duckdb_conn=duckdb_conn_multi_table,
            session=session,
            llm_provider=None,
        )

        # Should handle gracefully
        assert result.success
        data = result.unwrap()
        assert len(data["cross_table_cycles"]) == 0

    @pytest.mark.asyncio
    async def test_single_table_no_cycles(self, multi_table_db_session, duckdb_conn_multi_table):
        """Test that single table has no cross-table cycles."""
        session, tables, columns = multi_table_db_session

        result = await analyze_complete_financial_dataset_quality(
            table_ids=[tables["transactions"].table_id],
            duckdb_conn=duckdb_conn_multi_table,
            session=session,
            llm_provider=None,
        )

        assert result.success
        data = result.unwrap()
        assert len(data["cross_table_cycles"]) == 0

    @pytest.mark.asyncio
    async def test_tables_without_relationships(
        self, multi_table_db_session, duckdb_conn_multi_table
    ):
        """Test handling of tables without relationships."""
        session, tables, columns = multi_table_db_session

        # Don't add any relationships
        table_ids = [t.table_id for t in tables.values()]

        result = await analyze_complete_financial_dataset_quality(
            table_ids=table_ids,
            duckdb_conn=duckdb_conn_multi_table,
            session=session,
            llm_provider=None,
        )

        assert result.success
        data = result.unwrap()

        # No relationships = no cycles
        assert len(data["cross_table_cycles"]) == 0
        assert len(data["relationships"]) == 0
