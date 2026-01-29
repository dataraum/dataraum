"""Entropy analysis and aggregation module.

Layer 2 of the entropy framework - provides:
- Dynamic aggregation (compute summaries on demand)
- Summary dataclasses (ColumnSummary, TableSummary, RelationshipSummary)
- Compound risk detection
- Resolution hint generation
"""

from dataraum.entropy.analysis.aggregator import (
    ColumnSummary,
    EntropyAggregator,
    RelationshipSummary,
    TableSummary,
)

__all__ = [
    "ColumnSummary",
    "TableSummary",
    "RelationshipSummary",
    "EntropyAggregator",
]
