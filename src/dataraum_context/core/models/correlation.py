"""Pydantic models for Correlation Analysis (Part of Pillar 1).

These models are used for:
- API responses for correlation queries
- Internal data transfer for correlation analysis
- Validation of correlation results

Note: These are separate from SQLAlchemy models (storage/models_v2/correlation.py)
"""

from datetime import datetime

from pydantic import BaseModel, Field

# ============================================================================
# Numeric Correlation Models
# ============================================================================


class NumericCorrelation(BaseModel):
    """Pearson and Spearman correlation between two numeric columns."""

    correlation_id: str
    table_id: str
    column1_id: str
    column2_id: str
    column1_name: str
    column2_name: str

    # Pearson (linear relationship)
    pearson_r: float | None = None
    pearson_p_value: float | None = None

    # Spearman (monotonic relationship)
    spearman_rho: float | None = None
    spearman_p_value: float | None = None

    # Metadata
    sample_size: int
    computed_at: datetime

    # Interpretation
    correlation_strength: str  # 'none', 'weak', 'moderate', 'strong', 'very_strong'
    is_significant: bool  # p_value < 0.05


# ============================================================================
# Categorical Association Models
# ============================================================================


class CategoricalAssociation(BaseModel):
    """Cramér's V association between two categorical columns."""

    association_id: str
    table_id: str
    column1_id: str
    column2_id: str
    column1_name: str
    column2_name: str

    # Cramér's V (0 to 1)
    cramers_v: float

    # Chi-square test
    chi_square: float
    p_value: float
    degrees_of_freedom: int

    # Metadata
    sample_size: int
    computed_at: datetime

    # Interpretation
    association_strength: str  # 'none', 'weak', 'moderate', 'strong'
    is_significant: bool


# ============================================================================
# Functional Dependency Models
# ============================================================================


class FunctionalDependency(BaseModel):
    """A functional dependency: determinant → dependent.

    Represents that values in determinant column(s) uniquely determine
    values in the dependent column.
    """

    dependency_id: str
    table_id: str

    # Determinant (left side) - can be multiple columns
    determinant_column_ids: list[str]
    determinant_column_names: list[str]

    # Dependent (right side) - single column
    dependent_column_id: str
    dependent_column_name: str

    # Confidence (1.0 = exact, < 1.0 = approximate)
    confidence: float

    # Evidence
    unique_determinant_values: int
    violation_count: int

    # Example
    example: dict | None = None  # {determinant_values: [...], dependent_value: ...}

    # Metadata
    computed_at: datetime


# ============================================================================
# Derived Column Models
# ============================================================================


class DerivedColumn(BaseModel):
    """A column that appears to be derived from other columns."""

    derived_id: str
    table_id: str

    # Derived column
    derived_column_id: str
    derived_column_name: str

    # Source columns
    source_column_ids: list[str]
    source_column_names: list[str]

    # Derivation
    derivation_type: str  # 'sum', 'difference', 'product', 'ratio', 'concat', 'upper', etc.
    formula: str  # Human-readable formula

    # Match quality
    match_rate: float  # 0 to 1
    total_rows: int
    matching_rows: int

    # Evidence
    mismatch_examples: list[dict] | None = None

    # Metadata
    computed_at: datetime


# ============================================================================
# Aggregate Results
# ============================================================================


class CorrelationMatrix(BaseModel):
    """Correlation matrix for all numeric columns in a table."""

    table_id: str
    table_name: str
    column_names: list[str]
    column_ids: list[str]

    # Correlation matrix (row-major order)
    pearson_matrix: list[list[float]]  # n x n matrix
    spearman_matrix: list[list[float]] | None = None

    # Computed at
    computed_at: datetime


class CorrelationAnalysisResult(BaseModel):
    """Complete correlation analysis result for a table."""

    table_id: str
    table_name: str

    # Numeric correlations
    numeric_correlations: list[NumericCorrelation] = Field(default_factory=list)

    # Categorical associations
    categorical_associations: list[CategoricalAssociation] = Field(default_factory=list)

    # Functional dependencies
    functional_dependencies: list[FunctionalDependency] = Field(default_factory=list)

    # Derived columns
    derived_columns: list[DerivedColumn] = Field(default_factory=list)

    # Summary stats
    total_column_pairs: int
    significant_correlations: int
    strong_correlations: int  # |r| > 0.7

    # Performance
    duration_seconds: float
    computed_at: datetime
