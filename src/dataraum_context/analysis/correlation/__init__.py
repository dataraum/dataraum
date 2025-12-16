"""Correlation analysis module.

Analyzes relationships between columns within a table:
- Numeric correlations (Pearson, Spearman)
- Categorical associations (Cramér's V)
- Functional dependencies (A → B)
- Derived columns (col3 = col1 + col2)
- Multicollinearity (VIF, Tolerance, Condition Index)

All analysis is within a single table. Cross-table relationships
are handled by the analysis/relationships module (Phase 6).
"""

from dataraum_context.analysis.correlation.categorical import compute_categorical_associations
from dataraum_context.analysis.correlation.db_models import (
    CategoricalAssociation as DBCategoricalAssociation,
)
from dataraum_context.analysis.correlation.db_models import (
    ColumnCorrelation,
    CrossTableMulticollinearityMetrics,
    MulticollinearityMetrics,
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
from dataraum_context.analysis.correlation.models import (
    CategoricalAssociation,
    ColumnVIF,
    ConditionIndexAnalysis,
    CorrelationAnalysisResult,
    CrossTableDependencyGroup,
    CrossTableMulticollinearityAnalysis,
    DependencyGroup,
    DerivedColumn,
    FunctionalDependency,
    MulticollinearityAnalysis,
    NumericCorrelation,
    SingleRelationshipJoin,
)
from dataraum_context.analysis.correlation.multicollinearity import (
    compute_multicollinearity_for_table,
)
from dataraum_context.analysis.correlation.numeric import compute_numeric_correlations
from dataraum_context.analysis.correlation.processor import analyze_correlations

__all__ = [
    # Main entry point
    "analyze_correlations",
    # Individual analysis functions
    "compute_numeric_correlations",
    "compute_categorical_associations",
    "detect_functional_dependencies",
    "detect_derived_columns",
    "compute_multicollinearity_for_table",
    # DB Models
    "ColumnCorrelation",
    "DBCategoricalAssociation",
    "DBFunctionalDependency",
    "DBDerivedColumn",
    "MulticollinearityMetrics",
    "CrossTableMulticollinearityMetrics",
    # Pydantic Models
    "NumericCorrelation",
    "CategoricalAssociation",
    "FunctionalDependency",
    "DerivedColumn",
    "CorrelationAnalysisResult",
    # Multicollinearity models
    "DependencyGroup",
    "ColumnVIF",
    "ConditionIndexAnalysis",
    "MulticollinearityAnalysis",
    "SingleRelationshipJoin",
    "CrossTableDependencyGroup",
    "CrossTableMulticollinearityAnalysis",
]
