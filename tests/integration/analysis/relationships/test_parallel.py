"""Tests for parallel processing in relationships detection."""

import duckdb
import pytest

from dataraum.analysis.relationships.evaluator import evaluate_candidates
from dataraum.analysis.relationships.joins import find_join_columns
from dataraum.analysis.relationships.models import JoinCandidate, RelationshipCandidate


@pytest.fixture
def test_duckdb(tmp_path):
    """Create file-based DuckDB with test data for relationships.

    Returns a connection that can coexist with parallel read-only connections.
    Uses tmp_path so parallel workers can connect to the same database file.
    """
    db_path = str(tmp_path / "test_relationships.duckdb")

    # Create and populate the database
    conn = duckdb.connect(db_path)

    # Create customers table (parent)
    conn.execute("""
        CREATE TABLE customers AS
        SELECT
            i AS customer_id,
            'customer_' || i AS name,
            'email_' || i || '@example.com' AS email
        FROM generate_series(1, 100) AS t(i)
    """)

    # Create orders table (child) - each customer has 2-3 orders
    conn.execute("""
        CREATE TABLE orders AS
        SELECT
            i AS order_id,
            ((i - 1) % 100) + 1 AS customer_id,
            (RANDOM() * 1000)::DOUBLE AS amount,
            '2024-01-' || LPAD(((i - 1) % 28 + 1)::VARCHAR, 2, '0') AS order_date
        FROM generate_series(1, 250) AS t(i)
    """)

    # Create products table (no relationship to others)
    # Use product_id range that doesn't overlap with customer_id
    conn.execute("""
        CREATE TABLE products AS
        SELECT
            i + 10000 AS product_id,
            'product_' || i AS name,
            (RANDOM() * 100)::DOUBLE AS price
        FROM generate_series(1, 50) AS t(i)
    """)

    # Close setup connection so parallel workers can open read-only connections
    conn.close()

    # Return a fresh read-only connection (compatible with parallel workers)
    conn = duckdb.connect(db_path, read_only=True)
    yield conn
    conn.close()


class TestFindJoinColumnsParallel:
    """Tests for parallel join column detection."""

    # Column types matching the test fixture data
    CUSTOMER_TYPES = {
        "customer_id": "BIGINT",
        "name": "VARCHAR",
        "email": "VARCHAR",
    }
    ORDER_TYPES = {
        "order_id": "BIGINT",
        "customer_id": "BIGINT",
        "amount": "DOUBLE",
        "order_date": "VARCHAR",
    }
    PRODUCT_TYPES = {
        "product_id": "BIGINT",
        "name": "VARCHAR",
        "price": "DOUBLE",
    }

    def test_finds_customer_order_relationship(self, test_duckdb):
        """Test finding join between customers and orders on customer_id."""
        candidates = find_join_columns(
            test_duckdb,
            "customers",
            "orders",
            ["customer_id", "name", "email"],
            ["order_id", "customer_id", "amount", "order_date"],
            min_score=0.3,
            max_workers=4,
            column_types1=self.CUSTOMER_TYPES,
            column_types2=self.ORDER_TYPES,
        )

        # Should find customer_id as a join column
        customer_id_join = next(
            (
                c
                for c in candidates
                if c["column1"] == "customer_id" and c["column2"] == "customer_id"
            ),
            None,
        )
        assert customer_id_join is not None
        assert customer_id_join["join_confidence"] > 0.5  # High overlap
        assert customer_id_join["cardinality"] == "one-to-many"  # customers -> orders

    def test_no_relationship_between_unrelated_tables(self, test_duckdb):
        """Test that unrelated tables have no high-confidence joins."""
        candidates = find_join_columns(
            test_duckdb,
            "customers",
            "products",
            ["customer_id", "name", "email"],
            ["product_id", "name", "price"],
            min_score=0.3,
            max_workers=4,
            column_types1=self.CUSTOMER_TYPES,
            column_types2=self.PRODUCT_TYPES,
        )

        # Should not find high-confidence joins (name columns might have low overlap)
        high_conf = [c for c in candidates if c["join_confidence"] > 0.7]
        assert len(high_conf) == 0

    def test_parallel_execution_uses_file_db(self, test_duckdb):
        """Test that parallel execution is used for file-based DBs."""
        # Verify we have a file-based DB
        db_info = test_duckdb.execute("PRAGMA database_list").fetchall()
        db_path = db_info[0][2] if db_info else ""
        assert db_path, "Expected file-based DuckDB"

        # Run find_join_columns - should use parallel processing
        candidates = find_join_columns(
            test_duckdb,
            "customers",
            "orders",
            ["customer_id", "name", "email"],
            ["order_id", "customer_id", "amount", "order_date"],
            min_score=0.3,
            max_workers=4,
            column_types1=self.CUSTOMER_TYPES,
            column_types2=self.ORDER_TYPES,
        )

        # Should return results
        assert len(candidates) >= 1


class TestEvaluateCandidatesParallel:
    """Tests for parallel candidate evaluation."""

    def test_evaluates_referential_integrity(self, test_duckdb):
        """Test that referential integrity is computed correctly."""
        # Create a candidate to evaluate
        candidates = [
            RelationshipCandidate(
                table1="customers",
                table2="orders",
                join_candidates=[
                    JoinCandidate(
                        column1="customer_id",
                        column2="customer_id",
                        join_confidence=0.9,
                        cardinality="one-to-many",
                    )
                ],
            )
        ]

        table_paths = {
            "customers": "customers",
            "orders": "orders",
        }

        evaluated = evaluate_candidates(candidates, table_paths, test_duckdb, max_workers=4)

        assert len(evaluated) == 1
        result = evaluated[0]

        # Check join candidate was evaluated
        assert len(result.join_candidates) == 1
        jc = result.join_candidates[0]

        # All customers should be referenced (100% right RI)
        # All orders have valid customer_id (100% left RI)
        assert jc.left_referential_integrity is not None
        assert jc.left_referential_integrity == 100.0  # All orders match a customer
        assert jc.right_referential_integrity is not None
        assert jc.right_referential_integrity == 100.0  # All customers are referenced
        assert jc.orphan_count == 0
        assert jc.cardinality_verified is True

    def test_detects_duplicate_introduction(self, test_duckdb):
        """Test detection of fan trap (row multiplication)."""
        candidates = [
            RelationshipCandidate(
                table1="customers",
                table2="orders",
                join_candidates=[
                    JoinCandidate(
                        column1="customer_id",
                        column2="customer_id",
                        join_confidence=0.9,
                        cardinality="one-to-many",
                    )
                ],
            )
        ]

        table_paths = {
            "customers": "customers",
            "orders": "orders",
        }

        evaluated = evaluate_candidates(candidates, table_paths, test_duckdb, max_workers=4)

        # Joining customers to orders introduces duplicates (each customer has multiple orders)
        assert evaluated[0].introduces_duplicates is True

    def test_parallel_execution_multiple_candidates(self, test_duckdb):
        """Test parallel evaluation of multiple candidates."""
        # Create multiple candidates
        candidates = [
            RelationshipCandidate(
                table1="customers",
                table2="orders",
                join_candidates=[
                    JoinCandidate(
                        column1="customer_id",
                        column2="customer_id",
                        join_confidence=0.9,
                        cardinality="one-to-many",
                    )
                ],
            ),
            RelationshipCandidate(
                table1="customers",
                table2="products",
                join_candidates=[
                    JoinCandidate(
                        column1="name",
                        column2="name",
                        join_confidence=0.1,
                        cardinality="unknown",
                    )
                ],
            ),
        ]

        table_paths = {
            "customers": "customers",
            "orders": "orders",
            "products": "products",
        }

        evaluated = evaluate_candidates(candidates, table_paths, test_duckdb, max_workers=4)

        # Both should be evaluated
        assert len(evaluated) == 2

        # First should have evaluation metrics
        assert evaluated[0].join_candidates[0].left_referential_integrity is not None

        # Second (weak relationship) should also have metrics computed
        assert evaluated[1].join_candidates[0].left_referential_integrity is not None
