"""Profiling layer models.

Defines data structures for statistical profiling and type inference."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from dataraum_context.core.models.base import (
    Cardinality,
    ColumnRef,
    DataType,
    DecisionSource,
    RelationshipType,
)


class NumericStats(BaseModel):
    """Statistics for numeric columns."""

    min_value: float
    max_value: float
    mean: float
    stddev: float
    skewness: float | None = None
    kurtosis: float | None = None
    cv: float | None = None  # Coefficient of variation (stddev/mean)
    percentiles: dict[str, float | None] = Field(default_factory=dict)


class StringStats(BaseModel):
    """Statistics for string columns."""

    min_length: int
    max_length: int
    avg_length: float


class HistogramBucket(BaseModel):
    """A histogram bucket."""

    bucket_min: float | str
    bucket_max: float | str
    count: int


class ValueCount(BaseModel):
    """A value with its count."""

    value: Any
    count: int
    percentage: float


class DetectedPattern(BaseModel):
    """A detected pattern in column values."""

    name: str
    match_rate: float
    semantic_type: str | None = None


class TypeCandidate(BaseModel):
    """A candidate type for a column."""

    column_id: str
    column_ref: ColumnRef

    data_type: DataType
    confidence: float
    parse_success_rate: float
    failed_examples: list[str] = Field(default_factory=list)

    detected_pattern: str | None = None
    pattern_match_rate: float | None = None

    detected_unit: str | None = None
    unit_confidence: float | None = None


class ColumnProfile(BaseModel):
    """Statistical profile of a column."""

    column_id: str
    column_ref: ColumnRef
    profiled_at: datetime

    total_count: int
    null_count: int
    distinct_count: int

    null_ratio: float
    cardinality_ratio: float

    numeric_stats: NumericStats | None = None
    string_stats: StringStats | None = None

    histogram: list[HistogramBucket] | None = None
    top_values: list[ValueCount] | None = None
    detected_patterns: list[DetectedPattern] = Field(default_factory=list)


class ProfileResult(BaseModel):
    """Result of profiling operation."""

    profiles: list[ColumnProfile]
    type_candidates: list[TypeCandidate]
    duration_seconds: float


class SchemaProfileResult(BaseModel):
    """Result of schema profiling (raw stage, discovery only).

    Contains only sample-based, stable results that don't change
    when rows are quarantined during type resolution.
    """

    type_candidates: list[TypeCandidate]
    detected_patterns: dict[str, list[DetectedPattern]] = Field(default_factory=dict)
    duration_seconds: float


class StatisticsProfileResult(BaseModel):
    """Result of statistics profiling (typed stage, all stats).

    Contains all row-based statistics computed on clean typed data.
    """

    column_profiles: list[ColumnProfile] = Field(default_factory=list)
    correlation_result: CorrelationAnalysisResult | None = None
    duration_seconds: float


# === Type Resolution Models ===


class TypeDecision(BaseModel):
    """A type decision for a column."""

    column_id: str
    decided_type: DataType
    decision_source: DecisionSource = DecisionSource.AUTO
    decision_reason: str | None = None


class ColumnCastResult(BaseModel):
    """Cast result for a single column."""

    column_id: str
    column_ref: ColumnRef
    source_type: str
    target_type: DataType
    success_count: int
    failure_count: int
    success_rate: float
    failure_samples: list[str] = Field(default_factory=list)


class TypeResolutionResult(BaseModel):
    """Result of type resolution."""

    typed_table_name: str
    quarantine_table_name: str

    total_rows: int
    typed_rows: int
    quarantined_rows: int

    column_results: list[ColumnCastResult]


# === Statistical Quality Models ===


class BenfordAnalysis(BaseModel):
    """Benford's Law compliance analysis."""

    chi_square: float
    p_value: float
    is_compliant: bool  # p_value > 0.05
    digit_distribution: dict[str, float]  # {1: 0.301, 2: 0.176, ...}
    interpretation: str


class OutlierDetection(BaseModel):
    """Outlier detection results."""

    # IQR Method
    iqr_lower_fence: float
    iqr_upper_fence: float
    iqr_outlier_count: int
    iqr_outlier_ratio: float

    # Isolation Forest
    isolation_forest_score: float  # Average anomaly score
    isolation_forest_anomaly_count: int
    isolation_forest_anomaly_ratio: float

    # Sample outliers
    outlier_samples: list[dict[str, Any]] = Field(default_factory=list)  # [{value, method, score}]


class StatisticalQualityResult(BaseModel):
    """Comprehensive statistical quality assessment.

    This is the Pydantic source of truth for statistical quality metrics.
    Gets serialized to StatisticalQualityMetrics.quality_data JSONB field.
    """

    column_id: str
    column_ref: ColumnRef

    # Benford's Law (for financial/count columns)
    benford_analysis: BenfordAnalysis | None = None

    # Outlier detection
    outlier_detection: OutlierDetection | None = None

    # Quality issues detected
    quality_issues: list[dict[str, Any]] = Field(
        default_factory=list
    )  # [{issue_type, severity, description}]


# === Multicollinearity Models ===


class DependencyGroup(BaseModel):
    """A group of columns involved in a linear dependency.

    Identified via Variance Decomposition Proportions (VDP) analysis per
    Belsley, Kuh, Welsch (1980). Columns with high VDP (>0.5 or >0.8) on the
    same high-CI dimension are interdependent.

    VDP Calculation (Belsley method):
    - For each variable k: phi_kj = V_kj² / D_j²  (across all dimensions j)
    - VDP_kj = phi_kj / Σ_j(phi_kj)  (normalize across dimensions)
    - VDPs for a variable sum to 1 across ALL dimensions

    This correctly identifies groups with 2, 3, or more variables. Classical
    threshold is 0.5-0.8 per Belsley et al.
    """

    dimension: int  # Eigenvector dimension index
    eigenvalue: float  # Eigenvalue for this dimension
    condition_index: float  # CI for this specific dimension
    severity: Literal["moderate", "severe"]  # CI: 10-30, >30
    involved_column_ids: list[str]  # Column IDs with VDP > threshold
    variance_proportions: list[float]  # VDP values for each involved column

    @property
    def interpretation(self) -> str:
        """Human-readable interpretation of dependency group."""
        num_cols = len(self.involved_column_ids)
        if self.severity == "severe":
            return (
                f"{num_cols} columns share >90% variance in a near-singular dimension "
                f"(CI={self.condition_index:.1f}). Likely linear combination or derived columns."
            )
        else:
            return (
                f"{num_cols} columns have elevated shared variance "
                f"(CI={self.condition_index:.1f}). May indicate related metrics."
            )


class ColumnVIF(BaseModel):
    """Column-level multicollinearity assessment.

    VIF (Variance Inflation Factor) measures how much a column's variance
    is inflated due to correlation with other columns.
    """

    column_id: str
    column_ref: ColumnRef
    vif: float  # Variance Inflation Factor
    tolerance: float  # 1/VIF
    has_multicollinearity: bool  # VIF > 10 or Tolerance < 0.1
    severity: Literal["none", "moderate", "severe"]  # VIF: <5, 5-10, >10
    correlated_with: list[str] = Field(default_factory=list)  # Related column IDs

    @property
    def interpretation(self) -> str:
        """Human-readable interpretation of VIF score."""
        if self.vif < 5:
            return f"Low multicollinearity (VIF={self.vif:.1f})"
        elif self.vif < 10:
            return f"Moderate multicollinearity (VIF={self.vif:.1f})"
        else:
            return f"Severe multicollinearity (VIF={self.vif:.1f})"


class ConditionIndexAnalysis(BaseModel):
    """Table-level multicollinearity assessment via eigenvalue analysis.

    The Condition Index is the square root of the ratio of max to min eigenvalues
    of the correlation matrix. It provides overall multicollinearity severity.

    Variance Decomposition Proportions (VDP) identify which specific columns
    are involved in each linear dependency, enabling targeted recommendations.
    """

    condition_index: float  # sqrt(max(eigenvalue) / min(eigenvalue))
    eigenvalues: list[float]  # All eigenvalues from correlation matrix
    has_multicollinearity: bool  # Condition Index >= 10
    severity: Literal["none", "moderate", "severe"]  # CI: <10, 10-30, >30
    problematic_dimensions: int  # Count of near-zero eigenvalues
    dependency_groups: list[DependencyGroup] = Field(default_factory=list)  # VDP-identified groups

    @property
    def interpretation(self) -> str:
        """Human-readable interpretation of Condition Index.

        Thresholds (Belsley, Kuh, Welsch, 1980):
        - CI < 10: Weak or no multicollinearity
        - CI 10-30: Moderate multicollinearity
        - CI > 30: Severe multicollinearity
        """
        if self.condition_index < 10:
            return "No significant table-level multicollinearity"
        elif self.condition_index < 30:
            return f"Moderate table-level multicollinearity (CI={self.condition_index:.1f})"
        else:
            return f"Severe table-level multicollinearity (CI={self.condition_index:.1f})"


class MulticollinearityAnalysis(BaseModel):
    """Complete multicollinearity analysis for a table.

    Combines column-level VIF/Tolerance with table-level Condition Index
    to provide comprehensive multicollinearity assessment.
    """

    table_id: str
    table_name: str
    computed_at: datetime

    # Column-level analysis
    column_vifs: list[ColumnVIF] = Field(default_factory=list)
    num_problematic_columns: int = 0  # VIF > 10

    # Table-level analysis
    condition_index: ConditionIndexAnalysis | None = None

    # Summary flags
    has_severe_multicollinearity: bool = False
    overall_severity: Literal["none", "moderate", "severe"] = "none"

    # Quality issues detected
    quality_issues: list[dict[str, Any]] = Field(default_factory=list)


# === Cross-Table Multicollinearity Models (Phase 2) ===


class SingleRelationshipJoin(BaseModel):
    """Describes a single relationship connecting two columns across tables.

    Represents a direct join between two columns through a single relationship.
    For multi-step join paths, see enrichment.models.JoinPath.
    """

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_id: str
    relationship_type: RelationshipType
    cardinality: Cardinality | None
    confidence: float
    detection_method: str  # 'tda', 'semantic', 'manual'


class CrossTableDependencyGroup(BaseModel):
    """A dependency group spanning multiple tables.

    Identified via Belsley VDP on unified correlation matrix.
    """

    dimension: int  # Eigenvector dimension
    eigenvalue: float
    condition_index: float  # CI for this dimension
    severity: Literal["moderate", "severe"]  # CI: 10-30, >30

    # Involved columns (may span multiple tables)
    involved_columns: list[tuple[str, str]]  # [(table_name, column_name), ...]
    column_ids: list[str]  # For storage reference
    variance_proportions: list[float]  # VDP values for each column

    # Relationship context
    join_paths: list[SingleRelationshipJoin]  # How columns are connected
    relationship_types: list[RelationshipType]  # FK, CORRELATION, SEMANTIC, etc.

    @property
    def num_tables(self) -> int:
        """Number of distinct tables involved."""
        return len({table for table, _ in self.involved_columns})

    @property
    def interpretation(self) -> str:
        """Human-readable interpretation."""
        if self.num_tables == 1:
            return f"Single-table dependency: {len(self.involved_columns)} columns in same table"
        else:
            avg_vdp = (
                sum(self.variance_proportions) / len(self.variance_proportions)
                if self.variance_proportions
                else 0
            )
            return (
                f"{len(self.involved_columns)} columns across {self.num_tables} tables "
                f"share {avg_vdp * 100:.0f}% "
                f"variance in near-singular dimension (CI={self.condition_index:.1f})"
            )


class CrossTableMulticollinearityAnalysis(BaseModel):
    """Complete cross-table multicollinearity analysis."""

    # Scope
    table_ids: list[str]  # Tables included in analysis
    table_names: list[str]
    computed_at: datetime

    # Unified matrix info
    total_columns_analyzed: int
    total_relationships_used: int

    # Overall condition index for unified matrix
    overall_condition_index: float
    overall_severity: Literal["none", "moderate", "severe"]

    # Dependency groups (may include single-table and cross-table)
    dependency_groups: list[CrossTableDependencyGroup] = Field(default_factory=list)
    cross_table_groups: list[CrossTableDependencyGroup] = Field(
        default_factory=list
    )  # Filtered: num_tables > 1

    # Quality issues
    quality_issues: list[dict[str, Any]] = Field(default_factory=list)

    @property
    def num_cross_table_dependencies(self) -> int:
        """Count of dependency groups spanning multiple tables."""
        return len(self.cross_table_groups)


# ============================================================================
# Correlation Analysis Models (Pillar 1)
# ============================================================================
# Moved from core/models/correlation.py - these are profiling outputs


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
    example: dict[str, Any] | None = None  # {determinant_values: [...], dependent_value: ...}

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
    derivation_type: str  # 'sum', 'difference', 'product', 'ratio', 'concat', 'upper', etc.
    formula: str  # Human-readable formula

    # Match quality
    match_rate: float  # 0 to 1
    total_rows: int
    matching_rows: int

    # Evidence
    mismatch_examples: list[dict[str, Any]] | None = None

    # Metadata
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
