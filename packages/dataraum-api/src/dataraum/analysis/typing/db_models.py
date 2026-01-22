"""Type inference database models.

SQLAlchemy models for type inference persistence:
- TypeCandidate: Detected type candidates with confidence scores
- TypeDecision: Final type decision (automatic or human override)

These models form the persisted interface between the typing module
and downstream modules (statistics, semantic analysis).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column


class TypeCandidate(Base):
    """Type candidates from value pattern detection.

    Each column may have multiple type candidates with different
    confidence scores based on pattern matching and parsing success.

    Confidence is calculated from:
    - Pattern match rate (regex patterns on VALUES)
    - Parse success rate (TRY_CAST on VALUES)
    - Unit detection confidence (Pint on VALUES)

    Note: Type inference is based ONLY on value analysis,
    NOT on column names (column names are semantically meaningful
    but fragile for type inference).
    """

    __tablename__ = "type_candidates"

    candidate_id: Mapped[str] = mapped_column(
        String, primary_key=True, default=lambda: str(uuid4())
    )
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )
    detected_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Type candidate
    data_type: Mapped[str] = mapped_column(String, nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    parse_success_rate: Mapped[float | None] = mapped_column(Float)
    failed_examples: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    # Pattern info (value-based, NOT column name based)
    detected_pattern: Mapped[str | None] = mapped_column(String)
    pattern_match_rate: Mapped[float | None] = mapped_column(Float)

    # Unit detection (from Pint)
    detected_unit: Mapped[str | None] = mapped_column(String)
    unit_confidence: Mapped[float | None] = mapped_column(Float)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="type_candidates")


class TypeDecision(Base):
    """Type decisions (human-reviewable).

    Final type decision for a column after inference and optional human review.
    One decision per column.

    Decision sources:
    - 'automatic': System selected best TypeCandidate
    - 'manual': Human override via UI/API
    - 'override': Configuration-based override
    """

    __tablename__ = "type_decisions"
    __table_args__ = (UniqueConstraint("column_id", name="uq_column_type_decision"),)

    decision_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False
    )

    decided_type: Mapped[str] = mapped_column(String, nullable=False)
    decision_source: Mapped[str] = mapped_column(
        String, nullable=False
    )  # 'automatic', 'manual', 'override'
    decided_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    decided_by: Mapped[str | None] = mapped_column(String)

    # Audit trail
    previous_type: Mapped[str | None] = mapped_column(String)
    decision_reason: Mapped[str | None] = mapped_column(String)

    # Relationships
    column: Mapped[Column] = relationship(back_populates="type_decision")


# Indexes for efficient queries
Index("idx_type_candidates_column", TypeCandidate.column_id)
Index("idx_type_decisions_column", TypeDecision.column_id)
