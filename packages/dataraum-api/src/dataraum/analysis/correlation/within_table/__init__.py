"""Within-table correlation analysis.

Analyzes correlations and patterns within a single table:
- Numeric correlations (Pearson, Spearman)
- Categorical associations (Cramér's V)
- Functional dependencies (A → B)
- Derived columns (computed from other columns)

These analyses run BEFORE semantic analysis to enrich the context.
"""

from dataraum.analysis.correlation.within_table.categorical import (
    compute_categorical_associations,
)
from dataraum.analysis.correlation.within_table.derived_columns import (
    detect_derived_columns,
)
from dataraum.analysis.correlation.within_table.functional_dependency import (
    detect_functional_dependencies,
)
from dataraum.analysis.correlation.within_table.numeric import (
    compute_numeric_correlations,
)

__all__ = [
    "compute_numeric_correlations",
    "compute_categorical_associations",
    "detect_functional_dependencies",
    "detect_derived_columns",
]
