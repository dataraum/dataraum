"""Relationship Models.

SQLAlchemy models for storing detected relationships between columns/tables.
Part of Pillar 2 (Topological Context).
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

from dataraum_context.storage.models_v2.base import Base

if TYPE_CHECKING:
    from dataraum_context.storage.models_v2.core import Column


class Relationship(Base):
    """Detected relationships between columns.

    Represents foreign key relationships or other associations
    detected through TDA, cardinality analysis, or semantic similarity.
    """

    __tablename__ = "relationships"
    __table_args__ = (
        UniqueConstraint("from_column_id", "to_column_id", name="uq_relationship_columns"),
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
    )  # 'foreign_key', 'semantic_reference', 'derived'
    cardinality: Mapped[str | None] = mapped_column(String)  # '1:1', '1:N', 'N:1', 'N:M'

    # Confidence and evidence
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    detection_method: Mapped[str | None] = mapped_column(
        String
    )  # 'tda', 'cardinality', 'semantic', 'manual'
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


class JoinPath(Base):
    """Computed join paths between tables.

    Pre-computed paths for efficient multi-table queries.
    Updated when relationships change.
    """

    __tablename__ = "join_paths"
    __table_args__ = (
        UniqueConstraint("from_table_id", "to_table_id", "path_steps", name="uq_join_path"),
    )

    path_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    from_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    to_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    # Path definition
    path_steps: Mapped[dict[str, Any]] = mapped_column(
        JSON, nullable=False
    )  # List of relationship IDs forming the path
    path_length: Mapped[int] = mapped_column(Integer, nullable=False)  # Number of hops
    total_confidence: Mapped[float | None] = mapped_column(
        Float
    )  # Product of relationship confidences

    computed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
