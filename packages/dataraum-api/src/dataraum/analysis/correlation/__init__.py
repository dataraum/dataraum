"""Correlation analysis module.

Analyzes relationships between columns:

Within-table (pre-semantic):
- Numeric correlations (Pearson, Spearman)
- Categorical associations (Cramér's V)
- Functional dependencies (A → B)
- Derived columns

Cross-table (post-semantic):
- Cross-table quality analysis (VDP, redundant/derived columns)
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

# DB Models - Within-table
from dataraum.analysis.correlation.db_models import (
    CategoricalAssociation as DBCategoricalAssociation,
)
from dataraum.analysis.correlation.db_models import (
    ColumnCorrelation,
    CorrelationAnalysisRun,
    CrossTableCorrelationDB,
    MulticollinearityGroup,
    QualityIssueDB,
)
from dataraum.analysis.correlation.db_models import (
    DerivedColumn as DBDerivedColumn,
)
from dataraum.analysis.correlation.db_models import (
    FunctionalDependency as DBFunctionalDependency,
)

# Pydantic Models
from dataraum.analysis.correlation.models import (
    CategoricalAssociation,
    CorrelationAnalysisResult,
    CrossTableCorrelation,
    CrossTableQualityResult,
    DependencyGroup,
    DerivedColumn,
    DerivedColumnCandidate,
    EnrichedRelationship,
    FunctionalDependency,
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
    compute_categorical_associations,
    compute_numeric_correlations,
    detect_derived_columns,
    detect_functional_dependencies,
)

__all__ = [
    # Processors (main entry points)
    "analyze_correlations",
    "analyze_cross_table_quality",
    # Within-table functions
    "compute_numeric_correlations",
    "compute_categorical_associations",
    "detect_functional_dependencies",
    "detect_derived_columns",
    # Cross-table functions
    "analyze_relationship_quality",
    # DB Models - Within-table
    "ColumnCorrelation",
    "DBCategoricalAssociation",
    "DBFunctionalDependency",
    "DBDerivedColumn",
    # DB Models - Cross-table quality
    "CorrelationAnalysisRun",
    "CrossTableCorrelationDB",
    "MulticollinearityGroup",
    "QualityIssueDB",
    # Pydantic Models - Within-table
    "NumericCorrelation",
    "CategoricalAssociation",
    "FunctionalDependency",
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
