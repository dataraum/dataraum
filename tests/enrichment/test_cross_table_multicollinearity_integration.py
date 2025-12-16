"""End-to-end integration test for cross-table multicollinearity detection.

Tests the complete pipeline from CSV data → relationships → unified correlation matrix → VDP analysis.
"""

import tempfile
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dataraum_context.core.models.base import RelationshipType
from dataraum_context.enrichment.cross_table_multicollinearity import (
    compute_cross_table_multicollinearity,
)
from dataraum_context.enrichment.db_models import Relationship
from dataraum_context.storage.models_v2 import (
    Column,
    Source,
    Table,
)


def create_synthetic_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Create synthetic orders and customers data with known multicollinearity.

    Creates a scenario where:
    - orders.customer_id → customers.id (FK)
    - orders.customer_name is denormalized from customers.name (high correlation)
    - orders.total_price and orders.subtotal have multicollinearity

    Returns:
        Tuple of (orders_df, customers_df)
    """
    np.random.seed(42)

    # Create customers
    num_customers = 50
    customers = pd.DataFrame(
        {
            "id": range(1, num_customers + 1),
            "name": [f"Customer_{i}" for i in range(1, num_customers + 1)],
            "email": [f"customer{i}@example.com" for i in range(1, num_customers + 1)],
            "credit_score": np.random.randint(300, 850, num_customers),
        }
    )

    # Create orders with denormalization and multicollinearity
    num_orders = 200
    customer_ids = np.random.choice(customers["id"], size=num_orders)
    subtotals = np.random.uniform(10, 1000, num_orders)
    tax_rate = 0.1
    taxes = subtotals * tax_rate
    # Total = subtotal + tax (perfect linear combination)
    totals = subtotals + taxes

    # Add small noise to avoid perfect correlation (makes it more realistic)
    totals += np.random.normal(0, 0.01, num_orders)

    orders = pd.DataFrame(
        {
            "order_id": range(1, num_orders + 1),
            "customer_id": customer_ids,
            # Denormalized customer name (will be highly correlated with customer.name via FK)
            "customer_name": [
                customers.loc[customers["id"] == cid, "name"].values[0] for cid in customer_ids
            ],
            "order_date": pd.date_range("2024-01-01", periods=num_orders, freq="D")[:num_orders],
            "subtotal": subtotals,
            "tax": taxes,
            "total_price": totals,  # Derived column: subtotal + tax
        }
    )

    return orders, customers


@pytest.mark.asyncio
async def test_end_to_end_cross_table_multicollinearity(db_session: AsyncSession):
    """Test end-to-end cross-table multicollinearity detection pipeline.

    This test validates:
    1. Data loading into DuckDB
    2. Relationship detection (mock FK)
    3. Unified correlation matrix construction
    4. Belsley VDP analysis
    5. Cross-table dependency detection
    6. Quality issue generation
    """
    # === 1. Create synthetic data ===
    orders_df, customers_df = create_synthetic_data()

    # === 2. Load data into DuckDB ===
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Save to CSV
        orders_csv = tmpdir_path / "orders.csv"
        customers_csv = tmpdir_path / "customers.csv"
        orders_df.to_csv(orders_csv, index=False)
        customers_df.to_csv(customers_csv, index=False)

        # Create DuckDB connection and load data
        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute(f"CREATE TABLE orders AS SELECT * FROM '{orders_csv}'")
        duckdb_conn.execute(f"CREATE TABLE customers AS SELECT * FROM '{customers_csv}'")

        # === 3. Set up metadata in database ===
        source = Source(source_id="src1", name="test_e2e", source_type="csv")
        db_session.add(source)
        await db_session.flush()

        # Create table metadata
        orders_table = Table(
            table_id="t_orders",
            source_id="src1",
            table_name="orders",
            layer="typed",
            duckdb_path="orders",
        )
        customers_table = Table(
            table_id="t_customers",
            source_id="src1",
            table_name="customers",
            layer="typed",
            duckdb_path="customers",
        )

        db_session.add_all([orders_table, customers_table])
        await db_session.flush()

        # Create column metadata for numeric columns
        # Orders: customer_id, subtotal, tax, total_price
        orders_cols = [
            Column(
                column_id="c_orders_customer_id",
                table_id="t_orders",
                column_name="customer_id",
                column_position=1,
                raw_type="BIGINT",
                resolved_type="BIGINT",  # Resolved type is what the analysis uses
            ),
            Column(
                column_id="c_orders_subtotal",
                table_id="t_orders",
                column_name="subtotal",
                column_position=4,
                raw_type="DOUBLE",
                resolved_type="DOUBLE",
            ),
            Column(
                column_id="c_orders_tax",
                table_id="t_orders",
                column_name="tax",
                column_position=5,
                raw_type="DOUBLE",
                resolved_type="DOUBLE",
            ),
            Column(
                column_id="c_orders_total",
                table_id="t_orders",
                column_name="total_price",
                column_position=6,
                raw_type="DOUBLE",
                resolved_type="DOUBLE",
            ),
        ]

        # Customers: id, credit_score
        customers_cols = [
            Column(
                column_id="c_customers_id",
                table_id="t_customers",
                column_name="id",
                column_position=0,
                raw_type="BIGINT",
                resolved_type="BIGINT",
            ),
            Column(
                column_id="c_customers_credit_score",
                table_id="t_customers",
                column_name="credit_score",
                column_position=3,
                raw_type="BIGINT",
                resolved_type="BIGINT",
            ),
        ]

        db_session.add_all(orders_cols + customers_cols)
        await db_session.flush()

        # === 4. Create relationships (simulating FK + semantic detection) ===
        # FK relationship: orders.customer_id → customers.id
        fk_rel = Relationship(
            relationship_id="rel_fk",
            from_table_id="t_orders",
            from_column_id="c_orders_customer_id",
            to_table_id="t_customers",
            to_column_id="c_customers_id",
            relationship_type=RelationshipType.FOREIGN_KEY,
            confidence=0.95,
            detection_method="tda",
        )

        db_session.add(fk_rel)
        await db_session.commit()

        # === 5. Run cross-table multicollinearity analysis ===
        result = await compute_cross_table_multicollinearity(
            table_ids=["t_orders", "t_customers"],
            duckdb_conn=duckdb_conn,
            session=db_session,
        )

        # === 6. Validate results ===
        assert result.success, f"Analysis failed: {result.error}"
        analysis = result.value
        assert analysis is not None

        # Check scope
        assert len(analysis.table_ids) == 2
        assert set(analysis.table_names) == {"orders", "customers"}

        # Should have analyzed numeric columns from both tables
        assert analysis.total_columns_analyzed >= 5  # At least 4 from orders + 1-2 from customers
        assert analysis.total_relationships_used == 1  # The FK relationship

        # Check dependency groups were found
        assert len(analysis.dependency_groups) > 0

        # Should detect within-orders multicollinearity (subtotal, tax, total)
        single_table_groups = [g for g in analysis.dependency_groups if g.num_tables == 1]
        assert len(single_table_groups) > 0, "Should detect within-table dependencies"

        # Verify at least one severe group (for the linear combination)
        severe_groups = [g for g in analysis.dependency_groups if g.severity == "severe"]
        assert len(severe_groups) > 0, "Should detect severe dependencies (subtotal + tax = total)"

        # Check that analysis detected the linear combination
        total_col_in_severe = any(
            any("total" in col.lower() for _, col in group.involved_columns)
            for group in severe_groups
        )
        assert total_col_in_severe, "Should detect total_price in severe dependency group"

        # Check quality issues were generated
        assert len(analysis.quality_issues) > 0

        # Verify overall condition index is reasonable
        assert analysis.overall_condition_index > 0
        assert analysis.overall_severity in ["none", "moderate", "severe"]

        # Print summary for debugging
        print("\n=== Cross-Table Multicollinearity Analysis Summary ===")
        print(f"Tables: {analysis.table_names}")
        print(f"Columns analyzed: {analysis.total_columns_analyzed}")
        print(f"Relationships used: {analysis.total_relationships_used}")
        print(f"Overall Condition Index: {analysis.overall_condition_index:.2f}")
        print(f"Overall Severity: {analysis.overall_severity}")
        print(f"Dependency groups: {len(analysis.dependency_groups)}")
        print(f"Cross-table groups: {len(analysis.cross_table_groups)}")
        print(f"Quality issues: {len(analysis.quality_issues)}")

        for i, group in enumerate(analysis.dependency_groups[:3], 1):  # Show first 3
            print(f"\nGroup {i} (CI={group.condition_index:.1f}, {group.severity}):")
            print(f"  Columns: {[f'{t}.{c}' for t, c in group.involved_columns]}")
            print(f"  VDPs: {[f'{vdp:.2f}' for vdp in group.variance_proportions]}")
            print(f"  Num tables: {group.num_tables}")

        duckdb_conn.close()


@pytest.mark.asyncio
async def test_integration_no_relationships(db_session: AsyncSession):
    """Test cross-table analysis gracefully handles case with no relationships."""
    # === Create minimal setup without relationships ===
    source = Source(source_id="src1", name="test_no_rel", source_type="csv")
    db_session.add(source)
    await db_session.flush()

    table1 = Table(
        table_id="t1",
        source_id="src1",
        table_name="table1",
        layer="typed",
    )
    table2 = Table(
        table_id="t2",
        source_id="src1",
        table_name="table2",
        layer="typed",
    )

    db_session.add_all([table1, table2])
    await db_session.commit()

    # Create DuckDB connection (empty)
    duckdb_conn = duckdb.connect(":memory:")

    # Run analysis
    result = await compute_cross_table_multicollinearity(
        table_ids=["t1", "t2"],
        duckdb_conn=duckdb_conn,
        session=db_session,
    )

    # Should succeed but return empty analysis (no relationships = no cross-table analysis)
    assert result.success
    analysis = result.value
    assert analysis is not None
    assert analysis.total_relationships_used == 0
    assert analysis.total_columns_analyzed == 0
    assert len(analysis.dependency_groups) == 0
    assert analysis.overall_severity == "none"

    duckdb_conn.close()


@pytest.mark.asyncio
async def test_integration_single_table(db_session: AsyncSession):
    """Test that analysis works with a single table (no cross-table aspect)."""
    # This should work but only analyze within-table multicollinearity
    # Create synthetic data with multicollinearity
    np.random.seed(42)
    n = 100
    x1 = np.random.randn(n)
    x2 = 2 * x1 + np.random.randn(n) * 0.1  # Highly correlated with x1
    x3 = x1 + x2 + np.random.randn(n) * 0.05  # Linear combination
    x4 = np.random.randn(n)  # Independent

    df = pd.DataFrame(
        {
            "col1": x1,
            "col2": x2,
            "col3": x3,
            "col4": x4,
        }
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        csv_path = Path(tmpdir) / "data.csv"
        df.to_csv(csv_path, index=False)

        duckdb_conn = duckdb.connect(":memory:")
        duckdb_conn.execute(f"CREATE TABLE data AS SELECT * FROM '{csv_path}'")

        # Set up metadata
        source = Source(source_id="src1", name="test_single", source_type="csv")
        db_session.add(source)
        await db_session.flush()

        table = Table(
            table_id="t1",
            source_id="src1",
            table_name="data",
            layer="typed",
            duckdb_path="data",
        )
        db_session.add(table)
        await db_session.flush()

        # Add columns
        cols = [
            Column(
                column_id=f"c{i}",
                table_id="t1",
                column_name=f"col{i}",
                column_position=i - 1,
                raw_type="DOUBLE",
                resolved_type="DOUBLE",
            )
            for i in range(1, 5)
        ]
        db_session.add_all(cols)
        await db_session.commit()

        # Run analysis (with single table, no relationships needed)
        result = await compute_cross_table_multicollinearity(
            table_ids=["t1"],
            duckdb_conn=duckdb_conn,
            session=db_session,
        )

        # Should succeed but return empty analysis (no relationships = no cross-table aspect)
        assert result.success
        analysis = result.value
        assert analysis is not None
        assert analysis.total_relationships_used == 0
        assert analysis.total_columns_analyzed == 0  # No columns because no relationships
        assert len(analysis.dependency_groups) == 0
        assert analysis.overall_severity == "none"

        duckdb_conn.close()
