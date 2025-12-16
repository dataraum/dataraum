"""SQLAlchemy models for relationship detection.

Contains the Relationship database model for storing detected relationships
between tables (both raw TDA candidates and LLM-confirmed relationships).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
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
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum_context.storage import Base

if TYPE_CHECKING:
    from dataraum_context.storage import Column


class Relationship(Base):
    """Detected relationships between columns.

    Represents foreign key relationships or other associations
    detected through TDA, cardinality analysis, or semantic similarity.

    detection_method values:
    - 'candidate': Raw candidate from TDA/join detection (Phase 6)
    - 'llm': Confirmed/refined by LLM semantic analysis (Phase 5)
    - 'manual': Manually specified by user

    Multiple records can exist for the same column pair with different
    detection methods, providing full traceability of how relationships
    were discovered and confirmed.
    """

    __tablename__ = "relationships"
    __table_args__ = (
        # Allow same column pair with different detection methods
        UniqueConstraint(
            "from_column_id",
            "to_column_id",
            "detection_method",
            name="uq_relationship_columns_method",
        ),
        {"extend_existing": True},
    )

    relationship_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )

    # Source side
    from_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    from_column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)

    # Target side
    to_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    to_column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)

    # Classification
    relationship_type: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'foreign_key', 'semantic_reference', 'derived', 'candidate'
    cardinality: Mapped[str | None] = mapped_column(String)  # '1:1', '1:N', 'N:1', 'N:M'

    # Confidence and evidence
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    detection_method: Mapped[str | None] = mapped_column(
        String
    )  # 'tda', 'join_detection', 'llm', 'manual'
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Verification (human-in-loop)
    is_confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    confirmed_at: Mapped[datetime | None] = mapped_column(DateTime)
    confirmed_by: Mapped[str | None] = mapped_column(String)

    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    from_column: Mapped[Column] = relationship(
        foreign_keys=[from_column_id], back_populates="relationships_from"
    )
    to_column: Mapped[Column] = relationship(
        foreign_keys=[to_column_id], back_populates="relationships_to"
    )


Index("idx_relationships_from", Relationship.from_table_id)
Index("idx_relationships_to", Relationship.to_table_id)


class CrossTableMulticollinearityMetrics(Base):
    """Cross-table multicollinearity analysis results (hybrid storage).

    HYBRID STORAGE APPROACH:
    - Structured fields: Queryable dimensions for filtering/sorting
    - JSONB field: Full CrossTableMulticollinearityAnalysis Pydantic model

    Computes correlation matrix across related tables and identifies
    columns that are linearly dependent across table boundaries.

    Per-dataset storage: One record represents analysis across multiple tables.
    """

    __tablename__ = "cross_table_multicollinearity_metrics"
    __table_args__ = {"extend_existing": True}

    metric_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Scope (multiple tables) - stored as JSON array
    table_ids: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)  # {"table_ids": [...]}

    # STRUCTURED: Queryable dimensions for filtering
    overall_condition_index: Mapped[float | None] = mapped_column(Float)
    num_cross_table_groups: Mapped[int | None] = mapped_column(Integer)
    num_total_groups: Mapped[int | None] = mapped_column(Integer)
    has_severe_cross_table_dependencies: Mapped[bool | None] = mapped_column(Boolean)
    total_columns_analyzed: Mapped[int | None] = mapped_column(Integer)
    total_relationships_used: Mapped[int | None] = mapped_column(Integer)

    # JSONB: Full CrossTableMulticollinearityAnalysis Pydantic model
    # Stores: dependency_groups, cross_table_groups, join_paths, quality_issues
    analysis_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)


Index(
    "idx_cross_multicollinearity_severity",
    CrossTableMulticollinearityMetrics.has_severe_cross_table_dependencies,
    CrossTableMulticollinearityMetrics.num_cross_table_groups,
)


__all__ = ["Relationship", "CrossTableMulticollinearityMetrics"]
