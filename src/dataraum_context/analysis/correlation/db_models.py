"""Correlation Analysis Database Models.

SQLAlchemy models for correlation analysis persistence:
- ColumnCorrelation: Numeric correlations (Pearson, Spearman)
- CategoricalAssociation: Categorical associations (Cramér's V)
- FunctionalDependency: Functional dependencies (A → B)
- DerivedColumn: Derived column detection

Note: Multicollinearity models are in analysis/relationships/db_models.py
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum_context.storage import Base


class ColumnCorrelation(Base):
    """Correlation between two numeric columns.

    Stores both Pearson (linear) and Spearman (monotonic) correlations.
    Only stores correlations above a configurable threshold to avoid noise.
    """

    __tablename__ = "column_correlations"

    correlation_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    # Column pair (order doesn't matter, but we store both for query convenience)
    column1_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    column2_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Pearson correlation (linear relationship)
    pearson_r: Mapped[float | None] = mapped_column(Float)
    pearson_p_value: Mapped[float | None] = mapped_column(Float)

    # Spearman correlation (monotonic relationship)
    spearman_rho: Mapped[float | None] = mapped_column(Float)
    spearman_p_value: Mapped[float | None] = mapped_column(Float)

    # Metadata
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Interpretation
    correlation_strength: Mapped[str | None] = mapped_column(
        String
    )  # 'none', 'weak', 'moderate', 'strong', 'very_strong'
    is_significant: Mapped[bool | None] = mapped_column(Integer)  # p_value < 0.05


class CategoricalAssociation(Base):
    """Association between two categorical columns using Cramér's V.

    Cramér's V is based on chi-square test and measures association strength
    between categorical variables (0 = no association, 1 = perfect association).
    """

    __tablename__ = "categorical_associations"

    association_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    # Column pair
    column1_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    column2_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Cramér's V statistic (0 to 1)
    cramers_v: Mapped[float] = mapped_column(Float, nullable=False)

    # Chi-square test results
    chi_square: Mapped[float] = mapped_column(Float, nullable=False)
    p_value: Mapped[float] = mapped_column(Float, nullable=False)
    degrees_of_freedom: Mapped[int] = mapped_column(Integer, nullable=False)

    # Metadata
    sample_size: Mapped[int] = mapped_column(Integer, nullable=False)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Interpretation
    association_strength: Mapped[str | None] = mapped_column(
        String
    )  # 'none', 'weak', 'moderate', 'strong'
    is_significant: Mapped[bool | None] = mapped_column(Integer)  # p_value < 0.05


class FunctionalDependency(Base):
    """Functional dependency: determinant → dependent.

    A functional dependency A → B means that for each value of A,
    there is exactly one value of B (or very close to exact with confidence).

    Can be single-column (A → B) or multi-column ((A, B) → C).
    """

    __tablename__ = "functional_dependencies"

    dependency_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    # Determinant (left side) - list of column IDs
    determinant_column_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    # Dependent (right side) - single column
    dependent_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Confidence (1.0 = exact FD, < 1.0 = approximate FD)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)

    # Evidence
    unique_determinant_values: Mapped[int] = mapped_column(
        Integer, nullable=False
    )  # Count of unique determinant combinations
    violation_count: Mapped[int] = mapped_column(
        Integer, nullable=False
    )  # How many determinants map to multiple dependents

    # Metadata
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Example of the dependency
    example: Mapped[dict[str, Any] | None] = mapped_column(
        JSON
    )  # {determinant_values: [...], dependent_value: ...}


class DerivedColumn(Base):
    """Detected derived column (computed from other columns).

    Examples:
    - col3 = col1 + col2 (arithmetic)
    - col2 = UPPER(col1) (transformation)
    - col3 = CONCAT(col1, col2) (string operation)
    """

    __tablename__ = "derived_columns"

    derived_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    # The derived column
    derived_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    # Source columns (list of column IDs)
    source_column_ids: Mapped[list[str]] = mapped_column(JSON, nullable=False)

    # Derivation type
    derivation_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'sum', 'difference', 'product', 'ratio', 'concat', 'upper', 'lower', 'substr', etc.

    # Formula (human-readable)
    formula: Mapped[str] = mapped_column(
        String, nullable=False
    )  # e.g., "col_a + col_b", "UPPER(col_a)"

    # Match rate (how often the formula holds)
    match_rate: Mapped[float] = mapped_column(Float, nullable=False)

    # Metadata
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Evidence
    total_rows: Mapped[int] = mapped_column(Integer, nullable=False)
    matching_rows: Mapped[int] = mapped_column(Integer, nullable=False)
    mismatch_examples: Mapped[list[dict[str, object]] | None] = mapped_column(
        JSON
    )  # Sample of rows where formula doesn't hold


# =============================================================================
# Indexes for efficient queries
# =============================================================================

# Correlations
Index("idx_correlations_table", ColumnCorrelation.table_id)
Index("idx_correlations_col1", ColumnCorrelation.column1_id)
Index("idx_correlations_col2", ColumnCorrelation.column2_id)
Index("idx_associations_table", CategoricalAssociation.table_id)
Index("idx_associations_col1", CategoricalAssociation.column1_id)
Index("idx_associations_col2", CategoricalAssociation.column2_id)
Index("idx_dependencies_table", FunctionalDependency.table_id)
Index("idx_dependencies_dependent", FunctionalDependency.dependent_column_id)
Index("idx_derived_table", DerivedColumn.table_id)
Index("idx_derived_column", DerivedColumn.derived_column_id)
