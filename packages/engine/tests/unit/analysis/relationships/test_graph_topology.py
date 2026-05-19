"""Tests for graph topology analysis."""

from dataraum.analysis.relationships.graph_topology import (
    GraphStructure,
    SchemaCycle,
    TableRole,
    _classify_graph_pattern,
    _extract_table_ids,
    _extract_table_name,
    analyze_graph_topology,
    format_graph_structure_for_context,
)


class TestClassifyGraphPattern:
    """Tests for _classify_graph_pattern."""

    def test_empty(self):
        pattern, _ = _classify_graph_pattern(0, 0, 0, 0, 0, 0, 0)
        assert pattern == "empty"

    def test_single_table(self):
        pattern, _ = _classify_graph_pattern(1, 0, 0, 0, 0, 0, 0)
        assert pattern == "single_table"

    def test_no_relationships(self):
        pattern, _ = _classify_graph_pattern(3, 0, 0, 0, 0, 0, 0)
        assert pattern == "disconnected"

    def test_star_schema(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=3,
            hub_count=1,
            leaf_count=3,
            isolated_count=0,
            cycle_count=0,
            component_count=1,
        )
        assert pattern == "star_schema"

    def test_hub_and_spoke(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=5,
            total_relationships=5,
            hub_count=2,
            leaf_count=2,
            isolated_count=0,
            cycle_count=0,
            component_count=1,
        )
        assert pattern == "hub_and_spoke"

    def test_chain(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=3,
            hub_count=0,
            leaf_count=2,
            isolated_count=0,
            cycle_count=0,
            component_count=1,
        )
        assert pattern == "chain"

    def test_mesh_with_cycles(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=5,
            hub_count=1,
            leaf_count=0,
            isolated_count=0,
            cycle_count=2,
            component_count=1,
        )
        assert pattern == "mesh_with_cycles"

    def test_cyclic_no_hubs(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=3,
            total_relationships=3,
            hub_count=0,
            leaf_count=0,
            isolated_count=0,
            cycle_count=1,
            component_count=1,
        )
        assert pattern == "cyclic"

    def test_disconnected_components(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=2,
            hub_count=0,
            leaf_count=2,
            isolated_count=0,
            cycle_count=0,
            component_count=2,
        )
        assert pattern == "disconnected"

    def test_sparse(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=1,
            hub_count=0,
            leaf_count=1,
            isolated_count=3,
            cycle_count=0,
            component_count=1,
        )
        assert pattern == "sparse"


class TestExtractTableIds:
    """Tests for _extract_table_ids."""

    def test_dict_with_from_to(self):
        rel = {"from_table_id": "t1", "to_table_id": "t2"}
        assert _extract_table_ids(rel) == ("t1", "t2")

    def test_dict_with_table1_table2(self):
        rel = {"table1": "t1", "table2": "t2"}
        assert _extract_table_ids(rel) == ("t1", "t2")


class TestExtractTableName:
    """Tests for _extract_table_name."""

    def test_dict_from_side(self):
        rel = {"from_table": "customers", "to_table": "orders"}
        assert _extract_table_name(rel, "from") == "customers"
        assert _extract_table_name(rel, "to") == "orders"

    def test_dict_table1_table2(self):
        rel = {"table1": "customers", "table2": "orders"}
        assert _extract_table_name(rel, "from") == "customers"
        assert _extract_table_name(rel, "to") == "orders"


class TestAnalyzeGraphTopology:
    """Tests for analyze_graph_topology."""

    def test_empty_tables(self):
        result = analyze_graph_topology([], [])
        assert result.pattern == "empty"

    def test_single_table_no_relationships(self):
        result = analyze_graph_topology(["t1"], [])
        assert result.pattern == "single_table"
        assert result.total_tables == 1
        assert len(result.isolated_tables) == 1

    def test_star_schema_with_dicts(self):
        table_ids = ["t1", "t2", "t3", "t4"]
        relationships = [
            {"from_table_id": "t1", "to_table_id": "t2"},
            {"from_table_id": "t1", "to_table_id": "t3"},
            {"from_table_id": "t1", "to_table_id": "t4"},
        ]
        names = {"t1": "fact_sales", "t2": "dim_date", "t3": "dim_product", "t4": "dim_customer"}

        result = analyze_graph_topology(table_ids, relationships, table_names=names)

        assert result.pattern == "star_schema"
        assert "fact_sales" in result.hub_tables
        assert len(result.leaf_tables) == 3
        assert result.total_relationships == 3

    def test_cycle_detection(self):
        table_ids = ["t1", "t2", "t3"]
        relationships = [
            {"from_table_id": "t1", "to_table_id": "t2"},
            {"from_table_id": "t2", "to_table_id": "t3"},
            {"from_table_id": "t3", "to_table_id": "t1"},
        ]

        result = analyze_graph_topology(table_ids, relationships)

        assert len(result.schema_cycles) >= 1
        cycle_lengths = [c.length for c in result.schema_cycles]
        assert 3 in cycle_lengths

    def test_table_roles_assigned(self):
        table_ids = ["t1", "t2", "t3"]
        relationships = [
            {"from_table_id": "t1", "to_table_id": "t2"},
        ]
        names = {"t1": "orders", "t2": "customers", "t3": "products"}

        result = analyze_graph_topology(table_ids, relationships, table_names=names)

        roles = {r.table_name: r.role for r in result.tables}
        assert roles["orders"] == "dimension"  # 1 connection
        assert roles["customers"] == "dimension"  # 1 connection
        assert roles["products"] == "isolated"  # 0 connections


class TestFormatGraphStructure:
    """Tests for format_graph_structure_for_context."""

    def test_formats_basic_structure(self):
        structure = GraphStructure(
            pattern="star_schema",
            pattern_description="Classic star schema",
            total_tables=3,
            total_relationships=2,
            connected_components=1,
            hub_tables=["fact_sales"],
            leaf_tables=["dim_date", "dim_product"],
            tables=[
                TableRole(
                    table_name="fact_sales",
                    table_id="t1",
                    connection_count=2,
                    connects_to=["dim_date", "dim_product"],
                    role="hub",
                ),
            ],
        )

        text = format_graph_structure_for_context(structure)

        assert "star_schema" in text
        assert "fact_sales" in text
        assert "dim_date" in text

    def test_formats_cycles(self):
        structure = GraphStructure(
            pattern="cyclic",
            pattern_description="Has cycles",
            schema_cycles=[
                SchemaCycle(tables=["A", "B", "C"], table_ids=["1", "2", "3"], length=3),
            ],
        )

        text = format_graph_structure_for_context(structure)

        assert "cycle" in text.lower()
        assert "A" in text
