"""Events→measure aggregation lineage discovery (DAT-491).

Discovers whether a measure column is an AGGREGATE of event-level rows in
another table (e.g. ``trial_balance.debit_balance`` over ``journal_lines``),
and — when it is — whether the measure reconciles as a per-period movement
(flow) or a carried-forward cumulative level (stock). Discovery is
deterministic arithmetic over the slice substrate: per-(slice value, period)
sums persisted by temporal slice analysis, paired across facts by their shared
slice dimensions, with signed conventions enumerated and disposed by the
reconciliation statistic. The persisted ``MeasureAggregationLineage`` rows
feed the ``structural_reconciliation`` witness of the ``temporal_behavior``
pooled measurement — the data-grounded witness that escapes name-anchoring.
"""

from dataraum.analysis.lineage.models import CandidateDisposal
from dataraum.analysis.lineage.reconcile import classify_entity, dispose, reconcile

__all__ = [
    "CandidateDisposal",
    "classify_entity",
    "dispose",
    "reconcile",
]
