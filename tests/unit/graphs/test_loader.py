"""Tests for graphs/loader.py - graph loading."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataraum.graphs.loader import GraphLoader
from dataraum.graphs.models import (
    AppliesTo,
)


class TestGraphLoaderBasics:
    """Basic GraphLoader functionality tests."""

    def test_loader_init_with_vertical(self) -> None:
        """Loader resolves config/verticals/finance path from vertical name."""
        loader = GraphLoader(vertical="finance")
        assert loader.graphs_dir.name == "finance"
        assert loader.graphs_dir.parent.name == "verticals"

    def test_loader_init_no_args_raises(self) -> None:
        """Loader without vertical or graphs_dir raises ValueError."""
        with pytest.raises(ValueError, match="vertical is required"):
            GraphLoader()

    def test_loader_init_custom_path(self, tmp_path: Path) -> None:
        """Loader accepts custom path."""
        loader = GraphLoader(graphs_dir=tmp_path)
        assert loader.graphs_dir == tmp_path

    def test_load_all_empty_dir(self, tmp_path: Path) -> None:
        """Loading from empty directory returns empty dict."""
        loader = GraphLoader(graphs_dir=tmp_path)
        graphs = loader.load_all()
        assert graphs == {}


class TestLoadMetricGraphs:
    """Tests for loading metric graphs from verticals."""

    @pytest.fixture
    def loader(self) -> GraphLoader:
        """Create loader with finance vertical."""
        loader = GraphLoader(vertical="finance")
        loader.load_all()
        return loader

    def test_metric_graphs_loaded(self, loader: GraphLoader) -> None:
        """Metric graphs are loaded."""
        metrics = loader.get_metric_graphs()
        assert len(metrics) >= 1

    def test_quality_metrics_not_loaded(self, loader: GraphLoader) -> None:
        """Quality metrics were relocated out of verticals — not loaded by default."""
        assert loader.graphs.get("data_completeness") is None
        assert loader.graphs.get("data_freshness") is None
        assert loader.graphs.get("anomaly_rate") is None


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


class TestValidateStandardFields:
    """Tests for validate_standard_fields() method."""

    def test_all_known_fields_no_warnings(self) -> None:
        """Finance graphs + finance ontology = no warnings."""
        loader = GraphLoader(vertical="finance")
        loader.load_all()
        warnings = loader.validate_standard_fields("finance")
        assert warnings == []

    def test_unknown_field_produces_warning(self, tmp_path: Path) -> None:
        """Graph with made-up standard_field = warning."""
        metrics_dir = tmp_path / "metrics"
        metrics_dir.mkdir()
        graph_yaml = metrics_dir / "fake.yaml"
        graph_yaml.write_text(
            """
graph_id: fake_metric
graph_type: metric
version: "1.0"
metadata:
  name: Fake Metric
  description: Test metric
  category: test
  source: system
output:
  type: scalar
  metric_id: fake
dependencies:
  extract_nonexistent:
    level: 1
    type: extract
    source:
      standard_field: nonexistent_concept_xyz
    output_step: true
"""
        )

        loader = GraphLoader(graphs_dir=tmp_path)
        loader.load_all()

        warnings = loader.validate_standard_fields("finance")
        assert len(warnings) == 1
        assert "nonexistent_concept_xyz" in warnings[0]
        assert "finance" in warnings[0]

    def test_no_ontology_returns_empty(self, tmp_path: Path) -> None:
        """Nonexistent vertical returns no warnings."""
        loader = GraphLoader(graphs_dir=tmp_path)
        warnings = loader.validate_standard_fields("nonexistent_vertical")
        assert warnings == []
