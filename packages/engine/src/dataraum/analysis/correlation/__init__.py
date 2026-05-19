"""Correlation analysis module.

Analyzes relationships between columns:

Within-table:
- Derived columns (same-table and cross-table via enriched views)

Main entry points:
- analyze_correlations: Within-table correlation analysis (typed table)
- analyze_enriched_correlations: Enriched view analysis (same + cross-table)
"""

# DB Models
from dataraum.analysis.correlation.db_models import (
    DerivedColumn as DBDerivedColumn,
)

# Pydantic Models
from dataraum.analysis.correlation.models import (
    CorrelationAnalysisResult,
    DerivedColumn,
    NumericCorrelation,
)
from dataraum.analysis.correlation.processor import (
    analyze_correlations,
    analyze_enriched_correlations,
)

# Within-table functions (for direct access)
from dataraum.analysis.correlation.within_table import (
    compute_numeric_correlations,
    detect_derived_columns,
    detect_enriched_derived_columns,
)

__all__ = [
    # Processors (main entry points)
    "analyze_correlations",
    "analyze_enriched_correlations",
    # Within-table functions
    "compute_numeric_correlations",
    "detect_derived_columns",
    "detect_enriched_derived_columns",
    # DB Models
    "DBDerivedColumn",
    # Pydantic Models
    "NumericCorrelation",
    "DerivedColumn",
    "CorrelationAnalysisResult",
]
