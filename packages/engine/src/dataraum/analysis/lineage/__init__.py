"""Events→measure aggregation lineage discovery (DAT-491).

Discovers whether a measure column is an AGGREGATE of event-level rows in
another table (e.g. ``trial_balance.debit_balance`` over ``journal_lines``),
and — when it is — whether the measure reconciles as a per-period movement
(flow) or a carried-forward cumulative level (stock). The LLM proposes
candidate rollups; a deterministic DuckDB reconciliation statistic disposes
them. The persisted ``MeasureAggregationLineage`` rows feed the
``structural_reconciliation`` witness of the ``temporal_behavior`` pooled
measurement — the data-grounded witness that escapes name-anchoring.
"""

from dataraum.analysis.lineage.models import (
    AggregationLineageProposals,
    CandidateDisposal,
    LineageCandidate,
)
from dataraum.analysis.lineage.reconcile import classify_entity, dispose, reconcile

__all__ = [
    "AggregationLineageProposals",
    "CandidateDisposal",
    "LineageCandidate",
    "classify_entity",
    "dispose",
    "reconcile",
]
