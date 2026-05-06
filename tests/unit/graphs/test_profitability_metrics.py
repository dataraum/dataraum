"""Tests for profitability metric graphs (P&L metrics + margins)."""

from __future__ import annotations

import pytest

from dataraum.graphs.loader import GraphLoader
from dataraum.graphs.models import OutputType

PROFITABILITY_GRAPH_IDS = [
    "gross_profit",
    "operating_income",
    "ebitda",
    "net_income",
    "gross_margin",
    "operating_margin",
    "ebitda_margin",
    "net_margin",
]

CURRENCY_METRICS = {"gross_profit", "operating_income", "ebitda", "net_income"}
PERCENTAGE_METRICS = {"gross_margin", "operating_margin", "ebitda_margin", "net_margin"}


@pytest.fixture(scope="module")
def loader() -> GraphLoader:
    """Load all finance graphs once for the module."""
    loader = GraphLoader(vertical="finance")
    loader.load_all()
    return loader


class TestProfitabilityMetricsLoad:
    """All 8 profitability metrics load without errors."""

    def test_all_eight_metrics_present(self, loader: GraphLoader) -> None:
        """All profitability metric graph IDs are loadable."""
        for graph_id in PROFITABILITY_GRAPH_IDS:
            graph = loader.graphs.get(graph_id)
            assert graph is not None, f"Graph '{graph_id}' not found"

    @pytest.mark.parametrize("graph_id", PROFITABILITY_GRAPH_IDS)
    def test_output_type_is_scalar(self, loader: GraphLoader, graph_id: str) -> None:
        """Each profitability graph outputs a scalar."""
        graph = loader.graphs.get(graph_id)
        assert graph is not None
        assert graph.output.output_type == OutputType.SCALAR

    @pytest.mark.parametrize("graph_id", PROFITABILITY_GRAPH_IDS)
    def test_has_exactly_one_output_step(self, loader: GraphLoader, graph_id: str) -> None:
        """Each graph has exactly one step with output_step=true."""
        graph = loader.graphs.get(graph_id)
        assert graph is not None
        output_steps = [s for s in graph.steps.values() if s.output_step]
        assert len(output_steps) == 1, (
            f"{graph_id}: expected 1 output_step, got {len(output_steps)}"
        )


class TestProfitabilityOutputUnits:
    """Currency vs percentage output units are correct."""

    @pytest.mark.parametrize("graph_id", sorted(CURRENCY_METRICS))
    def test_currency_unit(self, loader: GraphLoader, graph_id: str) -> None:
        """Base P&L metrics output in currency."""
        graph = loader.graphs.get(graph_id)
        assert graph is not None
        assert graph.output.unit == "currency"

    @pytest.mark.parametrize("graph_id", sorted(PERCENTAGE_METRICS))
    def test_percentage_unit(self, loader: GraphLoader, graph_id: str) -> None:
        """Margin metrics output in percentage."""
        graph = loader.graphs.get(graph_id)
        assert graph is not None
        assert graph.output.unit == "percentage"


class TestProfitabilityStandardFields:
    """All standard_field values resolve against the finance ontology."""

    def test_no_warnings_from_validate(self, loader: GraphLoader) -> None:
        """validate_standard_fields returns no warnings for finance graphs."""
        warnings = loader.validate_standard_fields("finance")
        assert warnings == [], f"Unexpected warnings: {warnings}"


class TestProfitabilityFormulaDependencies:
    """Formula depends_on references exist as sibling steps."""

    @pytest.mark.parametrize("graph_id", PROFITABILITY_GRAPH_IDS)
    def test_depends_on_references_exist(self, loader: GraphLoader, graph_id: str) -> None:
        """Every depends_on reference points to an existing step in the same graph."""
        graph = loader.graphs.get(graph_id)
        assert graph is not None
        step_names = set(graph.steps.keys())
        for step_name, step in graph.steps.items():
            if step.depends_on:
                for dep in step.depends_on:
                    assert dep in step_names, (
                        f"{graph_id}.{step_name} depends on '{dep}' "
                        f"but available steps are {step_names}"
                    )


class TestProfitabilityCategories:
    """Metadata categories are consistent."""

    @pytest.mark.parametrize("graph_id", PROFITABILITY_GRAPH_IDS)
    def test_category_is_profitability(self, loader: GraphLoader, graph_id: str) -> None:
        """All profitability graphs have category == 'profitability'."""
        graph = loader.graphs.get(graph_id)
        assert graph is not None
        assert graph.metadata.category == "profitability"

    @pytest.mark.parametrize("graph_id", PROFITABILITY_GRAPH_IDS)
    def test_has_interpretation_ranges(self, loader: GraphLoader, graph_id: str) -> None:
        """All profitability graphs have interpretation ranges."""
        graph = loader.graphs.get(graph_id)
        assert graph is not None
        assert graph.interpretation is not None
        assert len(graph.interpretation.ranges) >= 3
