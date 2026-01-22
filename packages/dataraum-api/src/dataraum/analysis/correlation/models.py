"""Correlation Analysis Pydantic Models.

This module contains all Pydantic models for the correlation analysis module.

Within-Table Analysis:
- NumericCorrelation: Pearson and Spearman correlations
- CategoricalAssociation: Cramér's V associations
- FunctionalDependency: A → B dependencies
- DerivedColumn: Detected derived columns

Cross-Table Quality (post-confirmation):
- CrossTableCorrelation: Correlation between columns in different tables
- RedundantColumnPair: Perfectly correlated columns within same table
- DependencyGroup: VDP multicollinearity group
- QualityIssue: Generic quality issue

Result Containers:
- CorrelationAnalysisResult: Complete per-table analysis result
- CrossTableQualityResult: Cross-table quality analysis result

Utilities:
- EnrichedRelationship: Relationship with metadata for building joins
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from dataraum.core.models.base import Cardinality, RelationshipType

# =============================================================================
# Within-Table Analysis Models
# =============================================================================


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
    example: dict[str, Any] | None = None

    # Metadata
    computed_at: datetime


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
    derivation_type: str  # 'sum', 'difference', 'product', 'ratio', 'concat', etc.
    formula: str  # Human-readable formula

    # Match quality
    match_rate: float  # 0 to 1
    total_rows: int
    matching_rows: int

    # Evidence
    mismatch_examples: list[dict[str, Any]] | None = None

    # Metadata
    computed_at: datetime


# =============================================================================
# Cross-Table Quality Models (post-confirmation)
# =============================================================================


class CrossTableCorrelation(BaseModel):
    """A correlation between columns in different tables."""

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    pearson_r: float
    spearman_rho: float
    strength: str  # 'weak', 'moderate', 'strong', 'very_strong'
    is_join_column: bool  # True if this is the join column pair


class RedundantColumnPair(BaseModel):
    """Two columns that appear to contain the same data."""

    table: str
    column1: str
    column2: str
    correlation: float
    recommendation: str  # e.g., "Consider removing one column"


class DerivedColumnCandidate(BaseModel):
    """A column that may be derived from another (cross-table detection)."""

    table: str
    derived_column: str
    source_column: str
    correlation: float
    likely_formula: str | None = None  # e.g., "derived = source * 0.1"


class DependencyGroup(BaseModel):
    """A group of columns involved in multicollinearity."""

    model_config = {"frozen": False}

    columns: list[tuple[str, str]]  # [(table, column), ...]
    condition_index: float
    severity: Literal["moderate", "severe"]
    variance_proportions: dict[tuple[str, str], float]  # {(table, col): vdp}
    is_cross_table: bool  # True if columns span multiple tables


class QualityIssue(BaseModel):
    """A detected quality issue."""

    issue_type: str  # 'redundant_column', 'unexpected_correlation', 'multicollinearity'
    severity: Literal["info", "warning", "error"]
    message: str
    affected_columns: list[tuple[str, str]]  # [(table, column), ...]


# =============================================================================
# Result Container Models
# =============================================================================


class CorrelationAnalysisResult(BaseModel):
    """Complete correlation analysis result for a single table."""

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


class CrossTableQualityResult(BaseModel):
    """Quality analysis result for a confirmed relationship (cross-table)."""

    relationship_id: str
    from_table: str
    to_table: str
    join_column_from: str
    join_column_to: str

    # Metrics
    joined_row_count: int
    numeric_columns_analyzed: int

    # Cross-table correlations (excluding join columns)
    cross_table_correlations: list[CrossTableCorrelation] = Field(default_factory=list)

    # Within-table issues
    redundant_columns: list[RedundantColumnPair] = Field(default_factory=list)
    derived_columns: list[DerivedColumnCandidate] = Field(default_factory=list)

    # Multicollinearity
    overall_condition_index: float
    overall_severity: Literal["none", "moderate", "severe"]
    dependency_groups: list[DependencyGroup] = Field(default_factory=list)
    cross_table_dependency_groups: list[DependencyGroup] = Field(default_factory=list)

    # Summary
    issues: list[QualityIssue] = Field(default_factory=list)
    analyzed_at: datetime


# =============================================================================
# Utility Models
# =============================================================================


class EnrichedRelationship(BaseModel):
    """Relationship enriched with column and table metadata for join construction.

    This model extends the basic relationship with human-readable names
    and additional metadata needed for building SQL joins and analysis.
    Used by quality analysis and other modules that need to build joins.
    """

    relationship_id: str
    from_table: str
    from_column: str
    from_column_id: str
    from_table_id: str
    to_table: str
    to_column: str
    to_column_id: str
    to_table_id: str
    relationship_type: RelationshipType
    cardinality: Cardinality | None = None
    confidence: float
    detection_method: str
    evidence: dict[str, Any] = Field(default_factory=dict)
