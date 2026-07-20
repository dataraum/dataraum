"""Tests for graph topology analysis."""

from dataraum.analysis.relationships.graph_topology import (
    CyclicGroup,
    GraphStructure,
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
        pattern, _ = _classify_graph_pattern(0, 0, 0, 0, 0, 0, 0, 0.0)
        assert pattern == "empty"

    def test_single_table(self):
        pattern, _ = _classify_graph_pattern(1, 0, 0, 0, 0, 0, 0, 0.0)
        assert pattern == "single_table"

    def test_no_relationships(self):
        pattern, _ = _classify_graph_pattern(3, 0, 0, 0, 0, 0, 0, 0.0)
        assert pattern == "disconnected"

    def test_star_schema(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=3,
            hub_count=1,
            leaf_count=3,
            isolated_count=0,
            cyclic_group_count=0,
            component_count=1,
            density=0.5,
        )
        assert pattern == "star_schema"

    def test_hub_and_spoke(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=5,
            total_relationships=5,
            hub_count=2,
            leaf_count=2,
            isolated_count=0,
            cyclic_group_count=0,
            component_count=1,
            density=0.5,
        )
        assert pattern == "hub_and_spoke"

    def test_chain(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=3,
            hub_count=0,
            leaf_count=2,
            isolated_count=0,
            cyclic_group_count=0,
            component_count=1,
            density=0.5,
        )
        assert pattern == "chain"

    def test_mesh_with_cycles(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=5,
            hub_count=1,
            leaf_count=0,
            isolated_count=0,
            cyclic_group_count=2,
            component_count=1,
            density=0.5,
        )
        assert pattern == "mesh_with_cycles"

    def test_cyclic_no_hubs(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=3,
            total_relationships=3,
            hub_count=0,
            leaf_count=0,
            isolated_count=0,
            cyclic_group_count=1,
            component_count=1,
            density=0.5,
        )
        assert pattern == "cyclic"

    def test_disconnected_components(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=2,
            hub_count=0,
            leaf_count=2,
            isolated_count=0,
            cyclic_group_count=0,
            component_count=2,
            density=0.5,
        )
        assert pattern == "disconnected"

    def test_sparse(self):
        pattern, _ = _classify_graph_pattern(
            total_tables=4,
            total_relationships=1,
            hub_count=0,
            leaf_count=1,
            isolated_count=3,
            cyclic_group_count=0,
            component_count=1,
            density=0.5,
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

    def test_circular_reference_group_detection(self):
        table_ids = ["t1", "t2", "t3"]
        relationships = [
            {"from_table_id": "t1", "to_table_id": "t2"},
            {"from_table_id": "t2", "to_table_id": "t3"},
            {"from_table_id": "t3", "to_table_id": "t1"},
        ]

        result = analyze_graph_topology(table_ids, relationships)

        # One SCC holding all three, not three rotations of the same cycle.
        assert len(result.cyclic_groups) == 1
        assert result.cyclic_groups[0].size == 3
        assert result.cyclic_groups[0].tables == ["t1", "t2", "t3"]

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

    def test_self_referential_fk_does_not_inflate_role(self):
        # DAT-763: a self-FK (chart_of_accounts.parent_id -> account_id) is a
        # within-table hierarchy, not an inter-table edge. A NetworkX self-loop
        # counts degree +2 and lists the table as its own neighbor, which would
        # misclassify a dimension carrying ONE external FK + a self-FK as a hub.
        table_ids = ["coa", "journal"]
        relationships = [
            {"from_table_id": "journal", "to_table_id": "coa"},  # one external FK
            {"from_table_id": "coa", "to_table_id": "coa"},  # self-referential FK
        ]
        names = {"coa": "chart_of_accounts", "journal": "journal_lines"}

        result = analyze_graph_topology(table_ids, relationships, table_names=names)

        by_name = {r.table_name: r for r in result.tables}
        coa = by_name["chart_of_accounts"]
        assert coa.role == "dimension"  # degree 1, NOT hub (would be 3 with the loop)
        assert coa.connection_count == 1
        assert "chart_of_accounts" not in coa.connects_to  # never its own neighbor
        assert "chart_of_accounts" not in result.hub_tables


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

    def test_formats_cyclic_groups(self):
        structure = GraphStructure(
            pattern="cyclic",
            pattern_description="Has circular references",
            cyclic_groups=[
                CyclicGroup(tables=["A", "B", "C"], table_ids=["1", "2", "3"], size=3),
            ],
        )

        text = format_graph_structure_for_context(structure)

        assert "circular" in text.lower()
        assert "A" in text


class TestDenseGraphIsDescribedNotEnumerated:
    """DAT-834: topology output must not grow with the CYCLE COUNT of the graph.

    A complete candidate graph on 9 tables has 53,902 simple cycles. Enumerating
    them built a 1.9M-token prompt — twice the model's context window — from a
    1.2 MB corpus. The output is now bounded by the number of TABLES, which is
    what a schema description should scale with, and by construction rather than
    by a cap: strongly connected components partition the nodes.
    """

    @staticmethod
    def _complete_digraph(n: int) -> tuple[list[str], list[dict[str, str]]]:
        ids = [f"t{i}" for i in range(n)]
        rels = [{"from_table_id": a, "to_table_id": b} for a in ids for b in ids if a != b]
        return ids, rels

    def test_complete_graph_yields_one_group_not_thousands_of_cycles(self):
        ids, rels = self._complete_digraph(9)

        result = analyze_graph_topology(ids, rels)

        # Every table is mutually reachable => exactly ONE component naming all 9.
        assert len(result.cyclic_groups) == 1
        assert result.cyclic_groups[0].size == 9
        assert result.density == 1.0
        assert result.pattern == "complete"

    def test_output_scales_with_tables_not_cycles(self):
        """Doubling the tables must not explode the rendered context.

        9 -> 14 tables takes the simple-cycle count from ~54k to astronomically
        more; the formatted context must stay proportional to table count.
        """
        small = format_graph_structure_for_context(
            analyze_graph_topology(*self._complete_digraph(9))
        )
        big = format_graph_structure_for_context(
            analyze_graph_topology(*self._complete_digraph(14))
        )

        # Linear-ish in tables (each table contributes a role line naming its
        # neighbours, so growth is quadratic at worst) — never exponential.
        assert len(big) < len(small) * 6

    def test_circuit_rank_is_the_cycle_space_dimension(self):
        # A triangle: 3 nodes, 3 undirected edges, 1 component => mu = 3-3+1 = 1.
        ids = ["t1", "t2", "t3"]
        rels = [
            {"from_table_id": "t1", "to_table_id": "t2"},
            {"from_table_id": "t2", "to_table_id": "t3"},
            {"from_table_id": "t3", "to_table_id": "t1"},
        ]
        assert analyze_graph_topology(ids, rels).circuit_rank == 1

    def test_tree_has_no_independent_cycles(self):
        # A star is a tree: exactly one join path between any two tables.
        ids = ["f", "d1", "d2", "d3"]
        rels = [{"from_table_id": "f", "to_table_id": d} for d in ("d1", "d2", "d3")]

        result = analyze_graph_topology(ids, rels)

        assert result.circuit_rank == 0
        assert result.cyclic_groups == []

    def test_self_loop_is_not_a_circular_reference_group(self):
        # A within-table hierarchy is a size-1 component; it is not two tables
        # referencing each other.
        ids = ["coa", "journal"]
        rels = [
            {"from_table_id": "journal", "to_table_id": "coa"},
            {"from_table_id": "coa", "to_table_id": "coa"},
        ]

        assert analyze_graph_topology(ids, rels).cyclic_groups == []
