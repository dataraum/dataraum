"""Unit tests for the query-time contract-gate column summaries."""

from __future__ import annotations

from dataraum.entropy.views.query_context import network_to_column_summaries
from dataraum.entropy.views.readiness_context import (
    ColumnNodeEvidence,
    ColumnReadinessResult,
    EntropyForReadiness,
)


def test_network_to_column_summaries_skips_non_column_targets() -> None:
    """Only ``column:`` targets feed the per-column contract gate (DAT-415).

    ``EntropyForReadiness.columns`` now also holds ``relationship:`` (DAT-408) and
    ``table:`` (DAT-415) grains that roll up the same network. The contract gate is
    per-column, so those must be skipped — otherwise a ``table:orders`` target
    surfaces as a junk column named ``table:orders`` and pollutes the dimension
    averages.
    """
    ne = ColumnNodeEvidence(
        node_name="dimension_coverage",
        dimension_path="semantic.coverage.dimension_coverage",
        score=0.8,
        state="high",
    )
    ctx = EntropyForReadiness(
        columns={
            "column:orders.amount": ColumnReadinessResult(
                target="column:orders.amount", node_evidence=[ne], readiness="ready"
            ),
            "table:orders": ColumnReadinessResult(
                target="table:orders", node_evidence=[ne], readiness="ready"
            ),
            "relationship:fk-a::pk-b": ColumnReadinessResult(
                target="relationship:fk-a::pk-b", node_evidence=[ne], readiness="ready"
            ),
        }
    )

    summaries = network_to_column_summaries(ctx)

    assert set(summaries) == {"orders.amount"}, "non-column grains must not become columns"
    assert summaries["orders.amount"].table_name == "orders"
    assert summaries["orders.amount"].column_name == "amount"
