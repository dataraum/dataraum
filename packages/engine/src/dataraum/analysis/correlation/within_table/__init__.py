"""Within-table correlation analysis.

Analyzes patterns within a single table:
- Derived columns (computed from other columns)

These analyses run BEFORE semantic analysis to enrich the context.
"""

from dataraum.analysis.correlation.within_table.derived_columns import (
    detect_derived_columns,
    detect_enriched_derived_columns,
)

__all__ = [
    "detect_derived_columns",
    "detect_enriched_derived_columns",
]
