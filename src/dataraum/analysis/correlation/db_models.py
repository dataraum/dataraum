"""Correlation Analysis Database Models.

SQLAlchemy models for correlation analysis persistence.

Within-Table Analysis:
- DerivedColumn: Derived column detection

Cross-Table Analysis:
- CrossTableCorrelationRecord: Cross-table correlation findings
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


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

Index("idx_derived_table", DerivedColumn.table_id)
Index("idx_derived_column", DerivedColumn.derived_column_id)


class CrossTableCorrelationRecord(Base):
    """Cross-table correlation finding between columns in different tables.

    Persists the results of cross-table quality analysis, specifically
    Pearson/Spearman correlations between numeric columns across joined tables.
    """

    __tablename__ = "cross_table_correlations"

    correlation_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    relationship_id: Mapped[str] = mapped_column(
        ForeignKey("relationships.relationship_id", ondelete="CASCADE"), nullable=False
    )
    from_table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    from_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    to_table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    to_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    pearson_r: Mapped[float] = mapped_column(Float, nullable=False)
    spearman_rho: Mapped[float] = mapped_column(Float, nullable=False)
    strength: Mapped[str] = mapped_column(
        String, nullable=False
    )  # weak/moderate/strong/very_strong
    is_join_column: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


# Cross-table correlation indexes
Index("idx_ctc_relationship", CrossTableCorrelationRecord.relationship_id)
Index("idx_ctc_from_column", CrossTableCorrelationRecord.from_column_id)
Index("idx_ctc_to_column", CrossTableCorrelationRecord.to_column_id)
