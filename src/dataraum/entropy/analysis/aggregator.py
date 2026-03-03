"""Entropy summary dataclasses.

Minimal data classes for contract evaluation and query context.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ColumnSummary:
    """Minimal column entropy summary for contract evaluation.

    Built from network context via network_to_column_summaries().
    """

    column_id: str = ""
    column_name: str = ""
    table_id: str = ""
    table_name: str = ""
    readiness: str = "ready"
    dimension_scores: dict[str, float] = field(default_factory=dict)
