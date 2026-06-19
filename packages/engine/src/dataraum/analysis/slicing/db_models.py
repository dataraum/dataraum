"""SQLAlchemy models for slicing analysis.

Contains the database model for slice definitions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import (
    JSON,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage import Base

if TYPE_CHECKING:
    from dataraum.storage import Column, Table


class SliceDefinition(Base):
    """The dimension catalog: one recommended aggregation/filter dimension per row.

    Each record is a grain-safe dimension a fact can be sliced/grouped by — the
    durable output of the slicing phase, consumed downstream by the answer agent,
    the metrics page, and the driver-tree engine (DAT-545). Slice *materialization*
    was removed (DAT-536): the structural_reconciliation substrate is aggregated
    inline over the enriched views, so there is no ``sql_template`` to store.

    One definition per ``(table_id, column_name, run_id)`` (DAT-502): the
    slicing agent can emit a dimension twice and the propagation pass adds
    more, so the writer dedups in-batch and UPSERTs on this key — a Temporal
    success-redelivery (same ``run_id``) converges instead of duplicating,
    and a new run's definitions coexist with prior runs'.
    """

    __tablename__ = "slice_definitions"
    __table_args__ = (
        UniqueConstraint("table_id", "column_name", "run_id", name="uq_slice_def_table_column_run"),
        Index("idx_slice_definitions_table", "table_id"),
        Index("idx_slice_definitions_column", "column_id"),
    )

    slice_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    # Snapshot version axis (DAT-448): the begin_session run that derived this
    # definition. Definitions were table-scoped and immortal before — stale
    # cross-run reuse was the DAT-405 bug class.
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    column_id: Mapped[str] = mapped_column(ForeignKey("columns.column_id"), nullable=False)
    # Actual column name used for slicing — may differ from columns.column_name when the
    # slice dimension is an enriched FK-prefixed dim col (e.g. "kontonummer_des_gegenkontos__land")
    # while column_id points to the underlying FK column record.
    column_name: Mapped[str | None] = mapped_column(String, nullable=True)

    # Slice configuration
    slice_priority: Mapped[int] = mapped_column(Integer, nullable=False)
    slice_type: Mapped[str] = mapped_column(String, nullable=False, default="categorical")
    distinct_values: Mapped[list[str] | None] = mapped_column(JSON)
    value_count: Mapped[int | None] = mapped_column(Integer)

    # Analysis reasoning
    reasoning: Mapped[str | None] = mapped_column(Text)
    business_context: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(Float)

    # Provenance
    detection_source: Mapped[str] = mapped_column(String, nullable=False, default="llm")
    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )

    # Relationships
    table: Mapped[Table] = relationship()
    column: Mapped[Column] = relationship()


__all__ = [
    "SliceDefinition",
]
