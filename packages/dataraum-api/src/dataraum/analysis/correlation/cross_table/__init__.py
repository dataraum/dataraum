"""Cross-table correlation analysis.

Analyzes quality issues that can only be detected AFTER relationships
are confirmed by the semantic agent:

- Cross-table correlations (unexpected relationships between columns in different tables)
- Redundant columns (r ≈ 1.0 within same table)
- Derived columns (r ≈ 1.0 suggesting one column is computed from another)
- Multicollinearity groups (VDP-based, with cross-table flag)

These analyses run AFTER semantic analysis confirms relationships.
"""

from dataraum.analysis.correlation.cross_table.quality import (
    analyze_relationship_quality,
)

__all__ = [
    "analyze_relationship_quality",
]
