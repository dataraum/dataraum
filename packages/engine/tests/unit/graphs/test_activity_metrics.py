"""Activity metric graphs — the COUNT / AVG / COUNT(DISTINCT) matrix cells (DAT-718).

These exercise the additivity matrix's function-symmetry cells that the P&L
catalogue lacked: a COUNT flow (additive), an AVG (non-additive), and a
COUNT(DISTINCT) (non-additive). ``count_distinct`` required adding the aggregation
to the grounding vocabulary (``graph_sql_generation.yaml`` ``<aggregation_types>``
+ the ``GraphStep.aggregation`` vocabulary). The verdict itself is proven against
the grounded SQL shape each aggregation produces.
"""

from __future__ import annotations

import duckdb
import pytest

from dataraum.graphs.additivity import classify_extract, parse_aggregate_calls
from dataraum.graphs.config import get_metric_definitions
from dataraum.graphs.loader import GraphLoader
from dataraum.graphs.models import OutputType

# graph_id -> the extract's declared aggregation.
ACTIVITY_METRICS = {
    "transaction_count": "count",
    "average_transaction_value": "avg",
    "active_accounts": "count_distinct",
}


@pytest.fixture(scope="module")
def loader() -> GraphLoader:
    ldr = GraphLoader()
    ldr.graphs.update(ldr.graphs_from_definitions(get_metric_definitions("finance")))
    return ldr


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


class TestActivityMetricsLoad:
    @pytest.mark.parametrize("graph_id", sorted(ACTIVITY_METRICS))
    def test_present_and_scalar(self, loader: GraphLoader, graph_id: str) -> None:
        graph = loader.graphs.get(graph_id)
        assert graph is not None, f"{graph_id} not loaded from the finance vertical"
        assert graph.output.output_type == OutputType.SCALAR
        assert graph.metadata.category == "activity"

    @pytest.mark.parametrize("graph_id,aggregation", sorted(ACTIVITY_METRICS.items()))
    def test_single_extract_with_declared_aggregation(
        self, loader: GraphLoader, graph_id: str, aggregation: str
    ) -> None:
        graph = loader.graphs.get(graph_id)
        assert graph is not None
        extracts = [s for s in graph.steps.values() if s.step_type.value == "extract"]
        assert len(extracts) == 1
        assert extracts[0].aggregation == aggregation
        assert extracts[0].output_step is True

    def test_standard_fields_resolve(self, loader: GraphLoader) -> None:
        """account + transaction_amount resolve against the finance ontology (no warnings)."""
        assert loader.validate_standard_fields("finance") == []


class TestActivityMetricsAdditivity:
    """Each aggregation's grounded SQL shape classifies per the DAT-716 doctrine."""

    def test_count_flow_is_additive(self, con: duckdb.DuckDBPyConnection) -> None:
        # transaction_count → COUNT over an event fact (journal_lines) → additive on both axes.
        cls = classify_extract(
            parse_aggregate_calls("COUNT(amount)", con), {"amount": "additive"}, False
        )
        assert cls.categorical_additive is True
        assert cls.time_additive is True

    def test_avg_is_non_additive(self, con: duckdb.DuckDBPyConnection) -> None:
        cls = classify_extract(
            parse_aggregate_calls("AVG(amount)", con), {"amount": "additive"}, False
        )
        assert cls.categorical_additive is False
        assert cls.time_additive is False

    def test_count_distinct_is_non_additive(self, con: duckdb.DuckDBPyConnection) -> None:
        # active_accounts → COUNT(DISTINCT account_id) → non-additive (distinct sets overlap).
        cls = classify_extract(parse_aggregate_calls("COUNT(DISTINCT account_id)", con), {}, False)
        assert cls.categorical_additive is False
        assert cls.time_additive is False
