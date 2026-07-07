"""Tests for graphs/loader.py — GraphLoader parses definition dicts into graphs.

DAT-481 retired the file-only directory loader (``load_all``); the loader is now
seeded from the overlay-aware declared set (``get_metric_definitions``) via
``graphs_from_definitions`` — exactly as the metrics phase / grounding do.
"""

from __future__ import annotations

import pytest

from dataraum.graphs.config import get_metric_definitions
from dataraum.graphs.loader import GraphLoader


def _finance_loader() -> GraphLoader:
    """A GraphLoader seeded with the finance vertical's declared metric graphs.

    Mirrors production: parse the overlay-aware declared set via
    ``graphs_from_definitions``. No overlay resolver is registered in unit tests,
    so this resolves the shipped finance set off disk.
    """
    loader = GraphLoader()
    loader.graphs.update(loader.graphs_from_definitions(get_metric_definitions("finance")))
    return loader


class TestGraphLoaderBasics:
    def test_empty_loader_has_no_graphs(self) -> None:
        """A fresh loader holds no graphs until seeded."""
        assert GraphLoader().graphs == {}


class TestLoadMetricGraphs:
    """Seeding from the finance declared set populates the graphs."""

    @pytest.fixture
    def loader(self) -> GraphLoader:
        return _finance_loader()

    def test_metric_graphs_loaded(self, loader: GraphLoader) -> None:
        """Metric graphs are present after seeding."""
        assert len(loader.get_metric_graphs()) >= 1

    def test_quality_metrics_not_loaded(self, loader: GraphLoader) -> None:
        """Quality metrics were relocated out of verticals — not in the finance set."""
        assert loader.graphs.get("data_completeness") is None
        assert loader.graphs.get("data_freshness") is None
        assert loader.graphs.get("anomaly_rate") is None


class TestParseStepValidation:
    """The catalogue's per-extract `validation:` block is parsed, not dropped (DAT-616)."""

    def test_finance_dso_formula_expectation_parsed(self) -> None:
        """dso's formula step carries its declared plausibility range — a chained
        comparison at severity=warning, parsed from the shipped config."""
        loader = _finance_loader()
        dso = loader.graphs["dso"]
        formula = dso.steps["dso"]
        assert [v.condition for v in formula.validations] == ["0 <= value <= 365"]
        assert formula.validations[0].severity == "warning"

    def test_finance_extract_steps_declare_no_sign_bounds(self) -> None:
        """DAT-699: extract-level sign expectations were removed from the metric
        catalogue — the sign rule's home is the vertical's sign_natural_balance
        convention (authoring) + the sign_conventions validation (dataset-level).
        A per-metric `value > 0` on a shared extract was a duplicated hard gate
        that blocked real numbers and drifted between hand-copied blocks."""
        loader = _finance_loader()
        from dataraum.graphs.models import StepType

        for graph in loader.graphs.values():
            for step in graph.steps.values():
                if step.step_type == StepType.EXTRACT:
                    assert step.validations == [], (
                        f"{graph.graph_id}.{step.step_id} declares an extract-level bound"
                    )

    def test_missing_validation_block_yields_empty_list(self) -> None:
        """A step with no `validation:` key parses to an empty list, not None."""
        loader = GraphLoader()
        loader.graphs.update(
            loader.graphs_from_definitions(
                {
                    "m": {
                        "graph_id": "m",
                        "version": "1.0",
                        "metadata": {"name": "M"},
                        "output": {"type": "scalar"},
                        "dependencies": {
                            "x": {"type": "extract", "source": {"standard_field": "revenue"}}
                        },
                    }
                }
            )
        )
        assert loader.graphs["m"].steps["x"].validations == []


class TestValidateStandardFields:
    """Tests for validate_standard_fields() against a vertical's ontology."""

    def test_all_known_fields_no_warnings(self) -> None:
        """Finance graphs + finance ontology = no warnings."""
        loader = _finance_loader()
        assert loader.validate_standard_fields("finance") == []

    def test_unknown_field_produces_warning(self) -> None:
        """A graph with a made-up standard_field warns — parsed straight from a
        definition dict, no file IO."""
        loader = GraphLoader()
        loader.graphs.update(
            loader.graphs_from_definitions(
                {
                    "fake_metric": {
                        "graph_id": "fake_metric",
                        "version": "1.0",
                        "metadata": {
                            "name": "Fake Metric",
                            "description": "Test metric",
                            "category": "test",
                            "source": "system",
                        },
                        "output": {"type": "scalar", "metric_id": "fake"},
                        "dependencies": {
                            "extract_nonexistent": {
                                "level": 1,
                                "type": "extract",
                                "source": {"standard_field": "nonexistent_concept_xyz"},
                                "output_step": True,
                            }
                        },
                    }
                }
            )
        )

        warnings = loader.validate_standard_fields("finance")
        assert len(warnings) == 1
        assert "nonexistent_concept_xyz" in warnings[0]
        assert "finance" in warnings[0]

    def test_no_ontology_returns_empty(self) -> None:
        """An unknown vertical resolves to no ontology → no warnings."""
        assert GraphLoader().validate_standard_fields("nonexistent_vertical") == []
