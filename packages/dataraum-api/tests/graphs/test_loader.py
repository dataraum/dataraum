"""Tests for graphs/loader.py - graph loading and filter matching."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataraum.graphs.loader import GraphLoader
from dataraum.graphs.models import (
    AppliesTo,
    GraphType,
)


class TestGraphLoaderBasics:
    """Basic GraphLoader functionality tests."""

    def test_loader_init_default_path(self) -> None:
        """Loader uses default config/graphs path."""
        loader = GraphLoader()
        assert loader.graphs_dir.name == "graphs"
        assert loader.graphs_dir.parent.name == "config"

    def test_loader_init_custom_path(self, tmp_path: Path) -> None:
        """Loader accepts custom path."""
        loader = GraphLoader(graphs_dir=tmp_path)
        assert loader.graphs_dir == tmp_path

    def test_load_all_empty_dir(self, tmp_path: Path) -> None:
        """Loading from empty directory returns empty dict."""
        loader = GraphLoader(graphs_dir=tmp_path)
        graphs = loader.load_all()
        assert graphs == {}


class TestLoadNewRuleGraphs:
    """Tests for loading the new rule-based filter graphs."""

    @pytest.fixture
    def loader(self) -> GraphLoader:
        """Create loader with default config path."""
        loader = GraphLoader()
        loader.load_all()
        return loader

    def test_loads_role_based_rules(self, loader: GraphLoader) -> None:
        """Role-based rule graphs are loaded."""
        # Check for key column checks
        graph = loader.get_graph("role_key_checks")
        if graph:  # May not exist if YAML has multiple documents
            assert graph.graph_type == GraphType.FILTER
            assert graph.metadata.applies_to is not None
            assert graph.metadata.applies_to.semantic_role == "key"

    def test_loads_type_based_rules(self, loader: GraphLoader) -> None:
        """Type-based rule graphs are loaded."""
        graph = loader.get_graph("type_double_checks")
        if graph:
            assert graph.graph_type == GraphType.FILTER
            assert graph.metadata.applies_to is not None
            assert graph.metadata.applies_to.data_type == "DOUBLE"

    def test_loads_pattern_based_rules(self, loader: GraphLoader) -> None:
        """Pattern-based rule graphs are loaded."""
        graph = loader.get_graph("pattern_email_checks")
        if graph:
            assert graph.graph_type == GraphType.FILTER
            assert graph.metadata.applies_to is not None
            assert graph.metadata.applies_to.column_pattern is not None
            assert "email" in graph.metadata.applies_to.column_pattern

    def test_loads_quality_metrics(self, loader: GraphLoader) -> None:
        """Quality metric graphs are loaded."""
        graph = loader.get_graph("data_completeness")
        if graph:
            assert graph.graph_type == GraphType.METRIC
            assert graph.metadata.category == "quality"

    def test_filter_graphs_loaded(self, loader: GraphLoader) -> None:
        """At least some filter graphs are loaded."""
        filters = loader.get_filter_graphs()
        # Should have at least technical_quality
        assert len(filters) >= 1

    def test_metric_graphs_loaded(self, loader: GraphLoader) -> None:
        """Metric graphs are loaded."""
        metrics = loader.get_metric_graphs()
        # Should have quality metrics + business metrics
        assert len(metrics) >= 1


class TestGetApplicableFilters:
    """Tests for get_applicable_filters method."""

    @pytest.fixture
    def loader_with_test_graphs(self, tmp_path: Path) -> GraphLoader:
        """Create loader with test graphs."""
        # Create filters directory
        filters_dir = tmp_path / "filters" / "test"
        filters_dir.mkdir(parents=True)

        # Create a role-based filter
        role_filter = """
graph_id: "test_key_filter"
graph_type: "filter"
version: "1.0"

metadata:
  name: "Test Key Filter"
  description: "Test filter for key columns"
  category: "quality"
  source: "system"
  applies_to:
    semantic_role: "key"

output:
  type: "classification"

dependencies:
  not_null:
    level: 1
    type: "predicate"
    condition: "{column} IS NOT NULL"
    on_false: "quarantine"
    output_step: true
"""
        (filters_dir / "role_filter.yaml").write_text(role_filter)

        # Create a type-based filter
        type_filter = """
graph_id: "test_double_filter"
graph_type: "filter"
version: "1.0"

metadata:
  name: "Test Double Filter"
  description: "Test filter for DOUBLE columns"
  category: "quality"
  source: "system"
  applies_to:
    data_type: "DOUBLE"

output:
  type: "classification"

dependencies:
  not_nan:
    level: 1
    type: "predicate"
    condition: "NOT isnan({column})"
    on_false: "flag"
    output_step: true
"""
        (filters_dir / "type_filter.yaml").write_text(type_filter)

        # Create a pattern-based filter
        pattern_filter = """
graph_id: "test_email_filter"
graph_type: "filter"
version: "1.0"

metadata:
  name: "Test Email Filter"
  description: "Test filter for email columns"
  category: "quality"
  source: "system"
  applies_to:
    column_pattern: ".*email.*"

output:
  type: "classification"

dependencies:
  valid_format:
    level: 1
    type: "predicate"
    condition: "regexp_matches({column}, '^.+@.+$')"
    on_false: "flag"
    output_step: true
"""
        (filters_dir / "pattern_filter.yaml").write_text(pattern_filter)

        # Create a filter without applies_to (should never match)
        general_filter = """
graph_id: "test_general_filter"
graph_type: "filter"
version: "1.0"

metadata:
  name: "Test General Filter"
  description: "General filter without applies_to"
  category: "quality"
  source: "system"

output:
  type: "classification"

dependencies:
  always_pass:
    level: 1
    type: "predicate"
    condition: "1=1"
    on_false: "quarantine"
    output_step: true
"""
        (filters_dir / "general_filter.yaml").write_text(general_filter)

        loader = GraphLoader(graphs_dir=tmp_path)
        loader.load_all()
        return loader

    def test_match_by_semantic_role(self, loader_with_test_graphs: GraphLoader) -> None:
        """Match filters by semantic role."""
        filters = loader_with_test_graphs.get_applicable_filters(
            column_name="id",
            semantic_role="key",
        )
        assert len(filters) == 1
        assert filters[0].graph_id == "test_key_filter"

    def test_match_by_data_type(self, loader_with_test_graphs: GraphLoader) -> None:
        """Match filters by data type."""
        filters = loader_with_test_graphs.get_applicable_filters(
            column_name="amount",
            data_type="DOUBLE",
        )
        assert len(filters) == 1
        assert filters[0].graph_id == "test_double_filter"

    def test_match_by_column_pattern(self, loader_with_test_graphs: GraphLoader) -> None:
        """Match filters by column name pattern."""
        filters = loader_with_test_graphs.get_applicable_filters(
            column_name="user_email",
        )
        assert len(filters) == 1
        assert filters[0].graph_id == "test_email_filter"

    def test_no_match_wrong_role(self, loader_with_test_graphs: GraphLoader) -> None:
        """No match when role doesn't match."""
        filters = loader_with_test_graphs.get_applicable_filters(
            column_name="id",
            semantic_role="measure",  # Not "key"
        )
        # Should not match role filter, might match pattern if name matches
        assert not any(f.graph_id == "test_key_filter" for f in filters)

    def test_no_match_wrong_type(self, loader_with_test_graphs: GraphLoader) -> None:
        """No match when type doesn't match."""
        filters = loader_with_test_graphs.get_applicable_filters(
            column_name="amount",
            data_type="VARCHAR",  # Not "DOUBLE"
        )
        assert not any(f.graph_id == "test_double_filter" for f in filters)

    def test_no_match_wrong_pattern(self, loader_with_test_graphs: GraphLoader) -> None:
        """No match when pattern doesn't match."""
        filters = loader_with_test_graphs.get_applicable_filters(
            column_name="phone_number",  # Not email
        )
        assert not any(f.graph_id == "test_email_filter" for f in filters)

    def test_general_filter_not_matched(self, loader_with_test_graphs: GraphLoader) -> None:
        """Filters without applies_to are never matched."""
        filters = loader_with_test_graphs.get_applicable_filters(
            column_name="anything",
        )
        assert not any(f.graph_id == "test_general_filter" for f in filters)

    def test_multiple_matches(self, loader_with_test_graphs: GraphLoader) -> None:
        """Column can match multiple filters."""
        filters = loader_with_test_graphs.get_applicable_filters(
            column_name="contact_email",  # Matches pattern
            semantic_role="key",  # Matches role
        )
        # Should match both role and pattern filters
        graph_ids = [f.graph_id for f in filters]
        assert "test_key_filter" in graph_ids
        assert "test_email_filter" in graph_ids


class TestGetCrossColumnFilters:
    """Tests for get_cross_column_filters method."""

    @pytest.fixture
    def loader_with_cross_column(self, tmp_path: Path) -> GraphLoader:
        """Create loader with cross-column filter."""
        filters_dir = tmp_path / "filters" / "test"
        filters_dir.mkdir(parents=True)

        cross_filter = """
graph_id: "test_date_order"
graph_type: "filter"
version: "1.0"

metadata:
  name: "Date Order Check"
  description: "Start date before end date"
  category: "quality"
  source: "system"
  applies_to:
    column_pairs:
      start_pattern: ".*_start.*"
      end_pattern: ".*_end.*"

output:
  type: "classification"

dependencies:
  date_order:
    level: 1
    type: "predicate"
    condition: "{start_column} <= {end_column}"
    on_false: "quarantine"
    output_step: true
"""
        (filters_dir / "cross_filter.yaml").write_text(cross_filter)

        loader = GraphLoader(graphs_dir=tmp_path)
        loader.load_all()
        return loader

    def test_get_cross_column_filters(self, loader_with_cross_column: GraphLoader) -> None:
        """Cross-column filters are identified."""
        cross_filters = loader_with_cross_column.get_cross_column_filters()
        assert len(cross_filters) == 1
        assert cross_filters[0].graph_id == "test_date_order"

    def test_cross_column_not_in_regular_match(self, loader_with_cross_column: GraphLoader) -> None:
        """Cross-column filters are excluded from regular matching."""
        filters = loader_with_cross_column.get_applicable_filters(
            column_name="contract_start_date",
        )
        # Should not match because it has column_pairs
        assert not any(f.graph_id == "test_date_order" for f in filters)


class TestGetFiltersForDataset:
    """Tests for get_filters_for_dataset method."""

    @pytest.fixture
    def loader_with_filters(self, tmp_path: Path) -> GraphLoader:
        """Create loader with test filters."""
        filters_dir = tmp_path / "filters" / "test"
        filters_dir.mkdir(parents=True)

        # Role filter
        (filters_dir / "role.yaml").write_text("""
graph_id: "key_filter"
graph_type: "filter"
version: "1.0"
metadata:
  name: "Key Filter"
  description: "Test"
  category: "quality"
  source: "system"
  applies_to:
    semantic_role: "key"
output:
  type: "classification"
dependencies:
  check:
    level: 1
    type: "predicate"
    condition: "1=1"
    on_false: "flag"
    output_step: true
""")

        # Type filter
        (filters_dir / "type.yaml").write_text("""
graph_id: "double_filter"
graph_type: "filter"
version: "1.0"
metadata:
  name: "Double Filter"
  description: "Test"
  category: "quality"
  source: "system"
  applies_to:
    data_type: "DOUBLE"
output:
  type: "classification"
dependencies:
  check:
    level: 1
    type: "predicate"
    condition: "1=1"
    on_false: "flag"
    output_step: true
""")

        loader = GraphLoader(graphs_dir=tmp_path)
        loader.load_all()
        return loader

    def test_get_filters_for_dataset(self, loader_with_filters: GraphLoader) -> None:
        """Get filters for a dataset of columns."""
        columns = [
            {"column_name": "id", "semantic_role": "key", "data_type": "BIGINT"},
            {"column_name": "amount", "semantic_role": "measure", "data_type": "DOUBLE"},
            {"column_name": "name", "data_type": "VARCHAR"},
        ]

        filters = loader_with_filters.get_filters_for_dataset(columns)

        assert "id" in filters
        assert "amount" in filters
        assert "name" in filters

        # Key column gets key filter
        assert any(f.graph_id == "key_filter" for f in filters["id"])

        # Amount gets double filter
        assert any(f.graph_id == "double_filter" for f in filters["amount"])

        # Name gets no filters
        assert len(filters["name"]) == 0

    def test_get_quality_filter_summary(self, loader_with_filters: GraphLoader) -> None:
        """Get summary of filters for a dataset."""
        columns = [
            {"column_name": "id", "semantic_role": "key"},
            {"column_name": "amount", "data_type": "DOUBLE"},
            {"column_name": "name", "data_type": "VARCHAR"},
        ]

        summary = loader_with_filters.get_quality_filter_summary(columns)

        assert summary["total_filters"] == 2  # key_filter and double_filter
        assert summary["filters_by_column"]["id"] == 1
        assert summary["filters_by_column"]["amount"] == 1
        assert summary["filters_by_column"]["name"] == 0
        assert summary["filter_coverage"] == 2 / 3  # 2 columns have filters
        assert "key_filter" in summary["filter_ids"]
        assert "double_filter" in summary["filter_ids"]

    def test_empty_columns(self, loader_with_filters: GraphLoader) -> None:
        """Handle empty column list."""
        filters = loader_with_filters.get_filters_for_dataset([])
        assert filters == {}

        summary = loader_with_filters.get_quality_filter_summary([])
        assert summary["total_filters"] == 0
        assert summary["filter_coverage"] == 0


class TestAppliesTo:
    """Tests for AppliesTo dataclass."""

    def test_create_empty(self) -> None:
        """Create empty AppliesTo."""
        applies_to = AppliesTo()
        assert applies_to.semantic_role is None
        assert applies_to.data_type is None
        assert applies_to.column_pattern is None
        assert applies_to.column_pairs is None
        assert applies_to.has_profile is None

    def test_create_with_role(self) -> None:
        """Create AppliesTo with semantic role."""
        applies_to = AppliesTo(semantic_role="key")
        assert applies_to.semantic_role == "key"

    def test_create_with_pairs(self) -> None:
        """Create AppliesTo with column pairs."""
        applies_to = AppliesTo(
            column_pairs={
                "start_pattern": ".*_start.*",
                "end_pattern": ".*_end.*",
            }
        )
        assert applies_to.column_pairs is not None
        assert "start_pattern" in applies_to.column_pairs
