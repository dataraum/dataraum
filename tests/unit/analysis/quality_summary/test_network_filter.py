"""Tests for _filter_by_network_readiness in quality_summary processor."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dataraum.analysis.quality_summary.models import AggregatedColumnData
from dataraum.analysis.quality_summary.processor import _filter_by_network_readiness
from dataraum.entropy.views.network_context import (
    ColumnNetworkResult,
    EntropyForNetwork,
    IntentReadiness,
)


def _make_col(name: str) -> AggregatedColumnData:
    """Create a minimal AggregatedColumnData for testing."""
    return AggregatedColumnData(
        column_name=name,
        column_id=f"col-{name}",
        source_table_id="table-1",
        source_table_name="test_table",
        slice_column_name="region",
        slice_data=[{"slice_value": "A", "row_count": 100}],
    )


def _make_network_ctx(
    col_results: dict[str, tuple[str, float]],
) -> EntropyForNetwork:
    """Build EntropyForNetwork from {col_name: (readiness, worst_p_high)}.

    Target keys are formatted as "column:test_table.col_name".
    """
    columns: dict[str, ColumnNetworkResult] = {}
    for col_name, (readiness, worst_p_high) in col_results.items():
        target = f"column:test_table.{col_name}"
        columns[target] = ColumnNetworkResult(
            target=target,
            readiness=readiness,
            worst_intent_p_high=worst_p_high,
            intents=[
                IntentReadiness(
                    intent_name="analytical_reliability",
                    p_high=worst_p_high,
                    readiness=readiness,
                ),
            ],
        )
    return EntropyForNetwork(
        columns=columns,
        total_columns=len(columns),
    )


@pytest.fixture
def _patch_entropy():
    """Patch build_for_network for filter tests.

    Yields a setter to control the network context returned.
    """
    ctx_holder: list[EntropyForNetwork] = [EntropyForNetwork()]

    def _build_for_network(session, table_ids):
        return ctx_holder[0]

    with patch(
        "dataraum.entropy.views.network_context.build_for_network",
        side_effect=_build_for_network,
    ):

        def _set_ctx(ctx: EntropyForNetwork) -> None:
            ctx_holder[0] = ctx

        yield _set_ctx


class TestFilterByNetworkReadiness:
    """Tests for _filter_by_network_readiness."""

    def test_investigate_column_included(self, _patch_entropy):
        """Column with 'investigate' readiness is included."""
        _patch_entropy(_make_network_ctx({"amount": ("investigate", 0.45)}))
        session = MagicMock()
        cols = [_make_col("amount")]

        filtered, readiness_map = _filter_by_network_readiness(
            session, cols, "table-1", p_high_threshold=0.35
        )

        assert len(filtered) == 1
        assert filtered[0].column_name == "amount"
        assert readiness_map["amount"] == "investigate"

    def test_blocked_column_included(self, _patch_entropy):
        """Column with 'blocked' readiness is included."""
        _patch_entropy(_make_network_ctx({"amount": ("blocked", 0.75)}))
        session = MagicMock()
        cols = [_make_col("amount")]

        filtered, _ = _filter_by_network_readiness(session, cols, "table-1", p_high_threshold=0.35)

        assert len(filtered) == 1

    def test_ready_low_p_high_excluded(self, _patch_entropy):
        """Column that is 'ready' with low P(high) is excluded."""
        _patch_entropy(_make_network_ctx({"amount": ("ready", 0.10)}))
        session = MagicMock()
        cols = [_make_col("amount")]

        filtered, readiness_map = _filter_by_network_readiness(
            session, cols, "table-1", p_high_threshold=0.35
        )

        assert len(filtered) == 0
        assert readiness_map["amount"] == "ready"

    def test_ready_high_p_high_included(self, _patch_entropy):
        """Column that is 'ready' but P(high) above threshold is included."""
        _patch_entropy(_make_network_ctx({"amount": ("ready", 0.40)}))
        session = MagicMock()
        cols = [_make_col("amount")]

        filtered, _ = _filter_by_network_readiness(
            session,
            cols,
            "table-1",
            p_high_threshold=0.35,
        )

        assert len(filtered) == 1

    def test_no_network_data_returns_all(self):
        """When build_for_network returns empty, returns all columns with no_signal."""
        with patch(
            "dataraum.entropy.views.network_context.build_for_network",
            return_value=EntropyForNetwork(),
        ):
            session = MagicMock()
            cols = [_make_col("amount")]

            filtered, readiness_map = _filter_by_network_readiness(
                session, cols, "table-1", p_high_threshold=0.35
            )

            assert len(filtered) == 1
            assert readiness_map == {"amount": "no_signal"}

    def test_column_with_no_network_result_excluded(self, _patch_entropy):
        """Column not in network results is excluded with 'no_signal'."""
        # Network only has results for "amount", not "name"
        _patch_entropy(_make_network_ctx({"amount": ("blocked", 0.70)}))
        session = MagicMock()
        cols = [_make_col("amount"), _make_col("name")]

        filtered, readiness_map = _filter_by_network_readiness(
            session, cols, "table-1", p_high_threshold=0.35
        )

        assert len(filtered) == 1
        assert filtered[0].column_name == "amount"
        assert readiness_map["name"] == "no_signal"

    def test_mixed_readiness_filters_correctly(self, _patch_entropy):
        """Mix of blocked/investigate/ready columns filters correctly."""
        _patch_entropy(
            _make_network_ctx(
                {
                    "col_a": ("blocked", 0.80),
                    "col_b": ("investigate", 0.50),
                    "col_c": ("ready", 0.10),
                    "col_d": ("ready", 0.40),
                }
            )
        )
        session = MagicMock()
        cols = [
            _make_col("col_a"),
            _make_col("col_b"),
            _make_col("col_c"),
            _make_col("col_d"),
        ]

        filtered, readiness_map = _filter_by_network_readiness(
            session,
            cols,
            "table-1",
            p_high_threshold=0.35,
        )

        filtered_names = {c.column_name for c in filtered}
        assert filtered_names == {"col_a", "col_b", "col_d"}
        assert readiness_map["col_c"] == "ready"
