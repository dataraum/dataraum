"""Correlation analysis module.

Analyzes relationships between columns:
- Per-table: Numeric correlations, categorical associations, functional dependencies, derived columns
- Post-confirmation: Cross-table quality analysis (VDP, redundant/derived columns)

Cross-table relationship evaluation is in analysis/relationships/evaluator.py.
Quality-focused cross-table analysis is in analysis/correlation/cross_table.py.
"""

# Algorithms (pure computation)
from dataraum_context.analysis.correlation.algorithms import (
    AssociationResult,
    CorrelationResult,
    DependencyGroupResult,
    MulticollinearityResult,
    compute_cramers_v,
    compute_multicollinearity,
    compute_pairwise_correlations,
)

# Per-table runners
from dataraum_context.analysis.correlation.categorical import compute_categorical_associations

# Post-confirmation quality analysis
from dataraum_context.analysis.correlation.cross_table import (
    analyze_relationship_quality,
)

# DB Models - Within-table
from dataraum_context.analysis.correlation.db_models import (
    CategoricalAssociation as DBCategoricalAssociation,
)

# DB Models - Cross-table quality
from dataraum_context.analysis.correlation.db_models import (
    ColumnCorrelation,
    CorrelationAnalysisRun,
    CrossTableCorrelationDB,
    MulticollinearityGroup,
    QualityIssueDB,
)
from dataraum_context.analysis.correlation.db_models import (
    DerivedColumn as DBDerivedColumn,
)
from dataraum_context.analysis.correlation.db_models import (
    FunctionalDependency as DBFunctionalDependency,
)
from dataraum_context.analysis.correlation.derived_columns import detect_derived_columns
from dataraum_context.analysis.correlation.functional_dependency import (
    detect_functional_dependencies,
)

# Pydantic Models - Within-table
# Pydantic Models - Cross-table quality
from dataraum_context.analysis.correlation.models import (
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
from dataraum_context.analysis.correlation.numeric import compute_numeric_correlations
from dataraum_context.analysis.correlation.processor import analyze_correlations

__all__ = [
    # Main entry points
    "analyze_correlations",  # Per-table
    "analyze_relationship_quality",  # Post-confirmation cross-table
    # Per-table analysis functions
    "compute_numeric_correlations",
    "compute_categorical_associations",
    "detect_functional_dependencies",
    "detect_derived_columns",
    # Algorithms (pure computation)
    "compute_pairwise_correlations",
    "compute_cramers_v",
    "compute_multicollinearity",
    "CorrelationResult",
    "AssociationResult",
    "DependencyGroupResult",
    "MulticollinearityResult",
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
