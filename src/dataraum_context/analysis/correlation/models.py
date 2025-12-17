"""Correlation Analysis Pydantic Models.

Data structures for correlation analysis:
- NumericCorrelation: Pearson and Spearman correlations (within-table)
- CategoricalAssociation: Cramér's V associations (within-table)
- FunctionalDependency: A → B dependencies
- DerivedColumn: Detected derived columns
- CorrelationAnalysisResult: Complete analysis result

Cross-table models:
- CrossTableNumericCorrelation: Correlations across joined tables
- CrossTableCategoricalAssociation: Associations across joined tables
- CrossTableDependencyGroup: Multicollinearity groups spanning tables
- CrossTableMulticollinearityAnalysis: Complete cross-table analysis result
- EnrichedRelationship: Relationship with metadata for join construction
- SingleRelationshipJoin: Single join path between tables
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, Field

if TYPE_CHECKING:
    pass

from dataraum_context.core.models.base import Cardinality, RelationshipType

# =============================================================================
# Numeric Correlation Models
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


# =============================================================================
# Categorical Association Models
# =============================================================================


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


# =============================================================================
# Functional Dependency Models
# =============================================================================


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


# =============================================================================
# Derived Column Models
# =============================================================================


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


# =============================================================================
# Correlation Analysis Result
# =============================================================================


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


# =============================================================================
# Enriched Relationship Models (for cross-table analysis)
# =============================================================================


class EnrichedRelationship(BaseModel):
    """Relationship enriched with column and table metadata for join construction.

    This model extends the basic relationship with human-readable names
    and additional metadata needed for building SQL joins and analysis.
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


class SingleRelationshipJoin(BaseModel):
    """Describes a single relationship connecting two columns across tables.

    Represents a direct join between two columns through a single relationship.
    """

    from_table: str
    from_column: str
    to_table: str
    to_column: str
    relationship_id: str
    relationship_type: RelationshipType
    cardinality: Cardinality | None
    confidence: float
    detection_method: str  # 'candidate', 'llm', 'manual'


# =============================================================================
# Cross-Table Correlation Models
# =============================================================================


class CrossTableNumericCorrelation(BaseModel):
    """A numeric correlation between columns across tables."""

    table1: str
    column1: str
    table2: str
    column2: str
    pearson_r: float
    pearson_p: float
    spearman_rho: float
    spearman_p: float
    sample_size: int
    strength: str  # 'none', 'weak', 'moderate', 'strong', 'very_strong'
    is_significant: bool
    is_cross_table: bool  # True if table1 != table2


class CrossTableCategoricalAssociation(BaseModel):
    """A categorical association between columns across tables."""

    table1: str
    column1: str
    table2: str
    column2: str
    cramers_v: float
    chi_square: float
    p_value: float
    sample_size: int
    strength: str  # 'none', 'weak', 'moderate', 'strong'
    is_significant: bool
    is_cross_table: bool


# =============================================================================
# Cross-Table Multicollinearity Models
# =============================================================================


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
    """Complete cross-table correlation and multicollinearity analysis.

    Computes correlation matrix across all related tables and identifies:
    - Numeric correlations (Pearson/Spearman) between columns
    - Categorical associations (Cramér's V) between columns
    - Multicollinearity groups (columns that are linearly dependent)
    """

    # Scope
    table_ids: list[str]  # Tables included in analysis
    table_names: list[str]
    computed_at: datetime

    # Unified matrix info
    total_columns_analyzed: int
    total_numeric_columns: int = 0
    total_categorical_columns: int = 0
    total_relationships_used: int

    # Pairwise correlations (from compute_pairwise_correlations)
    numeric_correlations: list[CrossTableNumericCorrelation] = Field(default_factory=list)
    cross_table_correlations: list[CrossTableNumericCorrelation] = Field(
        default_factory=list
    )  # Filtered: is_cross_table=True

    # Categorical associations (from compute_cramers_v)
    categorical_associations: list[CrossTableCategoricalAssociation] = Field(default_factory=list)
    cross_table_associations: list[CrossTableCategoricalAssociation] = Field(
        default_factory=list
    )  # Filtered: is_cross_table=True

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
