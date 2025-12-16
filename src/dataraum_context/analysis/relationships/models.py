"""Relationship detection models.

Models for:
- Relationship candidates (TDA + join detection)
- Cross-table correlation analysis
- Multicollinearity analysis (VDP, Condition Index)
"""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field

from dataraum_context.core.models.base import Cardinality, RelationshipType


class JoinCandidate(BaseModel):
    """A potential join between two columns."""

    column1: str
    column2: str
    confidence: float
    cardinality: str  # one-to-one, one-to-many, many-to-one


class RelationshipCandidate(BaseModel):
    """A candidate relationship between two tables."""

    table1: str
    table2: str
    confidence: float
    topology_similarity: float
    relationship_type: str

    join_candidates: list[JoinCandidate] = Field(default_factory=list)


class RelationshipDetectionResult(BaseModel):
    """Result of relationship detection."""

    candidates: list[RelationshipCandidate] = Field(default_factory=list)

    total_tables: int = 0
    total_candidates: int = 0
    high_confidence_count: int = 0

    computed_at: datetime | None = None
    duration_seconds: float = 0.0


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
# Cross-Table Multicollinearity Models
# =============================================================================


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
    """Complete cross-table multicollinearity analysis.

    Computes correlation matrix across all related tables and identifies
    columns that are linearly dependent across table boundaries.
    """

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
