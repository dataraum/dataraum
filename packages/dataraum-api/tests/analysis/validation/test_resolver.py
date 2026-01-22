"""Tests for validation schema resolver."""

import pytest

from dataraum.analysis.validation.resolver import (
    format_multi_table_schema_for_prompt,
    get_multi_table_schema_for_llm,
)
from dataraum.storage import Column, Source, Table


@pytest.fixture
def table_with_columns(session):
    """Create a test table with columns and semantic annotations."""
    from dataraum.analysis.semantic.db_models import (
        SemanticAnnotation as SemanticAnnotationDB,
    )

    # Create source and table
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    table = Table(
        source_id=source.source_id,
        table_name="transactions",
        layer="typed",
        row_count=1000,
        duckdb_path="typed_transactions",
    )
    session.add(table)
    session.flush()

    # Create columns
    col1 = Column(
        table_id=table.table_id,
        column_name="transaction_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    col2 = Column(
        table_id=table.table_id,
        column_name="amount",
        column_position=1,
        raw_type="VARCHAR",
        resolved_type="DECIMAL(18,2)",
    )
    col3 = Column(
        table_id=table.table_id,
        column_name="account_type",
        column_position=2,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    session.add_all([col1, col2, col3])
    session.flush()

    # Add semantic annotation to amount column
    annotation = SemanticAnnotationDB(
        column_id=col2.column_id,
        semantic_role="measure",
        entity_type="amount",
        business_name="Transaction Amount",
    )
    session.add(annotation)
    session.commit()

    return table


@pytest.fixture
def two_tables_with_relationship(session):
    """Create two tables with a relationship for multi-table tests."""
    from dataraum.analysis.relationships.db_models import Relationship
    from dataraum.analysis.semantic.db_models import (
        SemanticAnnotation as SemanticAnnotationDB,
    )

    # Create source
    source = Source(name="test_source", source_type="csv")
    session.add(source)
    session.flush()

    # Create transactions table
    txn_table = Table(
        source_id=source.source_id,
        table_name="transactions",
        layer="typed",
        row_count=1000,
        duckdb_path="typed_transactions",
    )
    session.add(txn_table)
    session.flush()

    # Create accounts table
    acct_table = Table(
        source_id=source.source_id,
        table_name="accounts",
        layer="typed",
        row_count=50,
        duckdb_path="typed_accounts",
    )
    session.add(acct_table)
    session.flush()

    # Create columns for transactions
    txn_account_col = Column(
        table_id=txn_table.table_id,
        column_name="account_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    txn_amount_col = Column(
        table_id=txn_table.table_id,
        column_name="amount",
        column_position=1,
        raw_type="DECIMAL",
        resolved_type="DECIMAL(18,2)",
    )
    session.add_all([txn_account_col, txn_amount_col])
    session.flush()

    # Create columns for accounts
    acct_id_col = Column(
        table_id=acct_table.table_id,
        column_name="account_id",
        column_position=0,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    acct_type_col = Column(
        table_id=acct_table.table_id,
        column_name="account_type",
        column_position=1,
        raw_type="VARCHAR",
        resolved_type="VARCHAR",
    )
    session.add_all([acct_id_col, acct_type_col])
    session.flush()

    # Add semantic annotation to amount column
    annotation = SemanticAnnotationDB(
        column_id=txn_amount_col.column_id,
        semantic_role="measure",
        entity_type="amount",
        business_name="Transaction Amount",
    )
    session.add(annotation)

    # Create relationship between tables
    relationship = Relationship(
        from_table_id=txn_table.table_id,
        from_column_id=txn_account_col.column_id,
        to_table_id=acct_table.table_id,
        to_column_id=acct_id_col.column_id,
        relationship_type="foreign_key",
        cardinality="many-to-one",
        confidence=0.95,
    )
    session.add(relationship)
    session.commit()

    return txn_table, acct_table


def test_get_multi_table_schema_for_llm(session, two_tables_with_relationship):
    """Test fetching multi-table schema with relationships."""
    txn_table, acct_table = two_tables_with_relationship

    schema = get_multi_table_schema_for_llm(session, [txn_table.table_id, acct_table.table_id])

    assert "error" not in schema
    assert "tables" in schema
    assert "relationships" in schema

    # Check tables are included
    assert len(schema["tables"]) == 2
    table_names = [t["table_name"] for t in schema["tables"]]
    assert "transactions" in table_names
    assert "accounts" in table_names

    # Check relationship is included
    assert len(schema["relationships"]) == 1
    rel = schema["relationships"][0]
    assert rel["from_table"] == "transactions"
    assert rel["from_column"] == "account_id"
    assert rel["to_table"] == "accounts"
    assert rel["to_column"] == "account_id"
    assert rel["relationship_type"] == "foreign_key"
    assert rel["confidence"] == 0.95


def test_get_multi_table_schema_for_llm_single_table(session, table_with_columns):
    """Test fetching multi-table schema with single table (no relationships)."""
    table = table_with_columns

    schema = get_multi_table_schema_for_llm(session, [table.table_id])

    assert "error" not in schema
    assert "tables" in schema
    assert len(schema["tables"]) == 1
    assert schema["tables"][0]["table_name"] == "transactions"
    assert schema["relationships"] == []

    # Check semantic annotations are included
    amount_col = next(c for c in schema["tables"][0]["columns"] if c["column_name"] == "amount")
    assert "semantic" in amount_col
    assert amount_col["semantic"]["role"] == "measure"
    assert amount_col["semantic"]["entity_type"] == "amount"


def test_get_multi_table_schema_for_llm_empty_list(session):
    """Test fetching multi-table schema with empty list."""
    schema = get_multi_table_schema_for_llm(session, [])

    assert "error" in schema
    assert "No tables" in schema["error"]


def test_get_multi_table_schema_for_llm_nonexistent_tables(session):
    """Test fetching multi-table schema with nonexistent table IDs."""
    schema = get_multi_table_schema_for_llm(session, ["nonexistent-id"])

    assert "error" in schema


class TestFormatMultiTableSchemaForPrompt:
    """Tests for formatting multi-table schema as prompt text."""

    def test_format_multi_table_basic(self):
        """Test formatting a basic multi-table schema."""
        schema = {
            "tables": [
                {
                    "table_name": "orders",
                    "duckdb_path": "typed_orders",
                    "columns": [
                        {"column_name": "order_id", "data_type": "VARCHAR"},
                        {"column_name": "customer_id", "data_type": "VARCHAR"},
                    ],
                },
                {
                    "table_name": "customers",
                    "duckdb_path": "typed_customers",
                    "columns": [
                        {"column_name": "customer_id", "data_type": "VARCHAR"},
                        {"column_name": "name", "data_type": "VARCHAR"},
                    ],
                },
            ],
            "relationships": [],
        }

        result = format_multi_table_schema_for_prompt(schema)

        # New XML format
        assert "<tables>" in result
        assert 'name="orders"' in result
        assert 'name="customers"' in result
        assert 'duckdb_path="typed_orders"' in result
        assert 'name="order_id"' in result
        assert 'name="customer_id"' in result

    def test_format_multi_table_with_relationships(self):
        """Test formatting multi-table schema with relationships."""
        schema = {
            "tables": [
                {
                    "table_name": "orders",
                    "duckdb_path": "typed_orders",
                    "columns": [{"column_name": "customer_id", "data_type": "VARCHAR"}],
                },
                {
                    "table_name": "customers",
                    "duckdb_path": "typed_customers",
                    "columns": [{"column_name": "customer_id", "data_type": "VARCHAR"}],
                },
            ],
            "relationships": [
                {
                    "from_table": "orders",
                    "from_column": "customer_id",
                    "to_table": "customers",
                    "to_column": "customer_id",
                    "relationship_type": "foreign_key",
                    "cardinality": "many-to-one",
                    "confidence": 0.92,
                },
            ],
        }

        result = format_multi_table_schema_for_prompt(schema)

        # New XML format
        assert "<relationships>" in result
        assert 'from_table="orders"' in result
        assert 'from_column="customer_id"' in result
        assert 'to_table="customers"' in result
        assert 'type="foreign_key"' in result
        assert 'cardinality="many-to-one"' in result
        assert 'confidence="92%"' in result

    def test_format_multi_table_with_semantic_annotations(self):
        """Test formatting multi-table schema with semantic annotations."""
        schema = {
            "tables": [
                {
                    "table_name": "accounts",
                    "duckdb_path": "typed_accounts",
                    "columns": [
                        {
                            "column_name": "balance",
                            "data_type": "DECIMAL",
                            "semantic": {
                                "role": "measure",
                                "entity_type": "amount",
                                "business_name": "Account Balance",
                            },
                        },
                    ],
                },
            ],
            "relationships": [],
        }

        result = format_multi_table_schema_for_prompt(schema)

        # New XML format with attributes
        assert 'entity="amount"' in result
        assert 'role="measure"' in result
        assert 'business_name="Account Balance"' in result

    def test_format_multi_table_with_error(self):
        """Test formatting an error schema."""
        schema = {"error": "No tables found"}

        result = format_multi_table_schema_for_prompt(schema)

        # New XML format
        assert "<error>No tables found</error>" in result
