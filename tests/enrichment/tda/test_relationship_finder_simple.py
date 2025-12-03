"""Simplified tests for TDA relationship finder focusing on core behavior."""

import pandas as pd
import pytest

from dataraum_context.enrichment.tda.relationship_finder import TableRelationshipFinder


@pytest.fixture
def finder():
    """Create a relationship finder instance."""
    return TableRelationshipFinder()


@pytest.fixture
def related_tables():
    """Create related tables with a common column."""
    customers = pd.DataFrame(
        {
            "customer_id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
        }
    )

    orders = pd.DataFrame(
        {
            "order_id": [101, 102, 103],
            "customer_id": [1, 2, 3],
            "amount": [100.0, 200.0, 300.0],
        }
    )

    return {"customers": customers, "orders": orders}


def test_finder_initialization(finder):
    """Test that finder initializes correctly."""
    assert finder is not None
    assert hasattr(finder, "extractor")


def test_find_relationships_basic(finder, related_tables):
    """Test basic relationship finding."""
    result = finder.find_relationships(related_tables)

    assert isinstance(result, dict)
    assert "relationships" in result
    assert "join_graph" in result
    assert "suggested_joins" in result


def test_relationships_structure(finder, related_tables):
    """Test that relationships have expected structure."""
    result = finder.find_relationships(related_tables)

    relationships = result["relationships"]
    assert isinstance(relationships, list)

    for rel in relationships:
        assert isinstance(rel, dict)
        assert "table1" in rel
        assert "table2" in rel
        assert "confidence" in rel
        assert 0.0 <= rel["confidence"] <= 1.0


def test_single_table_no_relationships(finder):
    """Test that single table produces no relationships."""
    single_table = {"table1": pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})}

    result = finder.find_relationships(single_table)

    assert len(result["relationships"]) == 0


def test_find_join_columns(finder, related_tables):
    """Test finding join columns between tables."""
    customers = related_tables["customers"]
    orders = related_tables["orders"]

    join_columns = finder.find_join_columns(customers, orders)

    assert isinstance(join_columns, list)


def test_compare_topologies(finder, related_tables):
    """Test topology comparison."""
    customers = related_tables["customers"]
    orders = related_tables["orders"]

    topo1 = finder.extractor.extract_topology(customers)
    topo2 = finder.extractor.extract_topology(orders)

    result = finder.compare_topologies("customers", customers, topo1, "orders", orders, topo2)

    assert isinstance(result, dict)
    assert "table1" in result
    assert "table2" in result
    assert "confidence" in result


def test_multiple_tables(finder):
    """Test with three tables."""
    t1 = pd.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})
    t2 = pd.DataFrame({"id": [1, 2, 3], "amount": [100, 200, 300]})
    t3 = pd.DataFrame({"id": [1, 2, 3], "score": [5, 6, 7]})

    tables = {"table1": t1, "table2": t2, "table3": t3}
    result = finder.find_relationships(tables)

    assert isinstance(result, dict)


def test_empty_tables_handling(finder):
    """Test handling of empty tables."""
    empty_tables = {
        "table1": pd.DataFrame({"a": [], "b": []}),
        "table2": pd.DataFrame({"x": [], "y": []}),
    }

    result = finder.find_relationships(empty_tables)

    assert isinstance(result, dict)
    assert "relationships" in result


def test_join_graph_basic(finder, related_tables):
    """Test join graph creation."""
    result = finder.find_relationships(related_tables)
    join_graph = result["join_graph"]

    assert isinstance(join_graph, dict)


def test_suggested_joins_basic(finder, related_tables):
    """Test suggested joins."""
    result = finder.find_relationships(related_tables)
    suggested = result["suggested_joins"]

    assert isinstance(suggested, list)


def test_mixed_dtypes(finder):
    """Test with mixed data types."""
    t1 = pd.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10.0, 20.0, 30.0],
        }
    )

    t2 = pd.DataFrame(
        {
            "ref_id": [1, 2, 3],
            "amount": [100, 200, 300],
        }
    )

    tables = {"table1": t1, "table2": t2}
    result = finder.find_relationships(tables)

    assert isinstance(result, dict)


def test_confidence_values(finder, related_tables):
    """Test that confidence values are reasonable."""
    result = finder.find_relationships(related_tables)

    for rel in result["relationships"]:
        confidence = rel["confidence"]
        assert isinstance(confidence, (float, int))
        assert 0.0 <= confidence <= 1.0
