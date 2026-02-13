"""Correlation analysis module.

Analyzes relationships between columns:

Within-table (pre-semantic):
- Derived columns

Cross-table (post-semantic):
- Cross-table quality analysis (correlations, redundant/derived columns)
- Requires confirmed relationships from semantic agent

Main entry points:
- analyze_correlations: Within-table correlation analysis
- analyze_cross_table_quality: Cross-table quality analysis on confirmed relationships
"""

# Processors (main entry points)
# Cross-table functions (for direct access)
from dataraum.analysis.correlation.cross_table import (
    analyze_relationship_quality,
)

# DB Models
from dataraum.analysis.correlation.db_models import (
    DerivedColumn as DBDerivedColumn,
)

# Pydantic Models
from dataraum.analysis.correlation.models import (
    CorrelationAnalysisResult,
    CrossTableCorrelation,
    CrossTableQualityResult,
    DependencyGroup,
    DerivedColumn,
    DerivedColumnCandidate,
    EnrichedRelationship,
    NumericCorrelation,
    QualityIssue,
    RedundantColumnPair,
)
from dataraum.analysis.correlation.processor import (
    analyze_correlations,
    analyze_cross_table_quality,
)

# Within-table functions (for direct access)
from dataraum.analysis.correlation.within_table import (
    compute_numeric_correlations,
    detect_derived_columns,
)

__all__ = [
    # Processors (main entry points)
    "analyze_correlations",
    "analyze_cross_table_quality",
    # Within-table functions
    "compute_numeric_correlations",
    "detect_derived_columns",
    # Cross-table functions
    "analyze_relationship_quality",
    # DB Models
    "DBDerivedColumn",
    # Pydantic Models - Within-table
    "NumericCorrelation",
    "DerivedColumn",
    "CorrelationAnalysisResult",
    # Pydantic Models - Cross-table quality
    "CrossTableQualityResult",
    "CrossTableCorrelation",
    "RedundantColumnPair",
    "DerivedColumnCandidate",
    "DependencyGroup",
    "QualityIssue",
    "EnrichedRelationship",
]
