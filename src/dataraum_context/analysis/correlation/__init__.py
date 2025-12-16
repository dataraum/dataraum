"""Correlation analysis module.

Analyzes relationships between columns within a table:
- Numeric correlations (Pearson, Spearman)
- Categorical associations (Cramér's V)
- Functional dependencies (A → B)
- Derived columns (col3 = col1 + col2)

Note: Multicollinearity analysis has moved to analysis/relationships module
as it only makes sense across tables with relationship context.
"""

from dataraum_context.analysis.correlation.categorical import compute_categorical_associations
from dataraum_context.analysis.correlation.db_models import (
    CategoricalAssociation as DBCategoricalAssociation,
)
from dataraum_context.analysis.correlation.db_models import (
    ColumnCorrelation,
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
    CorrelationAnalysisResult,
    DerivedColumn,
    FunctionalDependency,
    NumericCorrelation,
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
    # DB Models
    "ColumnCorrelation",
    "DBCategoricalAssociation",
    "DBFunctionalDependency",
    "DBDerivedColumn",
    # Pydantic Models
    "NumericCorrelation",
    "CategoricalAssociation",
    "FunctionalDependency",
    "DerivedColumn",
    "CorrelationAnalysisResult",
]
