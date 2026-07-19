"""SQLAlchemy models for slicing analysis.

Contains the database model for slice definitions.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import (
    JSON,
    CheckConstraint,
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
    """The dimension inventory: one aggregation/filter dimension per row.

    Existence is DETERMINISTIC (DAT-725 rescope): the slicing phase persists
    every eligible column — grain-safe pre-filter survivor (DAT-805: not a
    constant, not majority-NULL, not a near-unique key, on a scale-invariant
    near-key fraction) whose ``semantic_role`` is not measure/timestamp. The
    LLM's role is ENRICHMENT only: the slicing agent ranks the most interesting
    dimensions, and its ``slice_priority`` / ``business_context`` / ``reasoning``
    / ``confidence`` merge onto rows that exist regardless (un-ranked rows carry
    the ``UNRANKED_SLICE_PRIORITY`` floor). Same data + same code ⇒ the same
    persisted dimension set, run to run — an elected-subset catalog silently
    dropped real axes (a folded ``account_id`` was elected 0-2 times across
    runs) from every existence consumer (drivers, lineage, bus_matrix).

    Grain-safety is by construction — enriched dimensions are grain-verified FK
    joins; thin per-group support is folded by the driver-tree's ``min_support``
    (DAT-538 removed the redundant always-true ``grain_safe`` flag). Curation
    surfaces (cycles/graphs/validation context + the cockpit ``<dimensions>``
    block) read ``ORDER BY slice_priority LIMIT CURATED_SLICE_BUDGET``; existence
    consumers read the full inventory. Slice *materialization* was removed
    (DAT-536): the structural_reconciliation substrate is aggregated inline over
    the enriched views, so there is no ``sql_template`` to store.

    One definition per ``(table_id, column_name, run_id)`` (DAT-502): the writer
    dedups in-batch and UPSERTs on this key — a Temporal success-redelivery
    (same ``run_id``) converges instead of duplicating, and a new run's
    definitions coexist with prior runs'.
    """

    __tablename__ = "slice_definitions"
    __table_args__ = (
        UniqueConstraint("table_id", "column_name", "run_id", name="uq_slice_def_table_column_run"),
        Index("idx_slice_definitions_table", "table_id"),
        Index("idx_slice_definitions_column", "column_id"),
        Index("idx_slice_definitions_dim_table", "dimension_table_id"),
        # Closed-vocabulary enforcement (DAT-802 enum-standard sweep): the ONLY
        # value ``slicing_phase.py`` (the sole writer) ever produces today —
        # numeric/date-bucket slice types are not yet built. Extending the CHECK
        # is the cost of shipping a second slice type, same as any other closed
        # vocabulary here.
        CheckConstraint("slice_type IN ('categorical')", name="slice_type"),
        # Detection-source vocabulary (DAT-802 / DAT-725): 'llm' = the slicing
        # agent ranked this row (its enrichment fields are LLM-derived);
        # 'structural' = a deterministic inventory row the ranker did not touch
        # (enrichment NULL, priority = the UNRANKED_SLICE_PRIORITY floor).
        # ``slicing_phase.py`` is still the sole writer.
        CheckConstraint("detection_source IN ('llm', 'structural')", name="detection_source"),
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

    # Referenced-dimension identity (DAT-756): what makes two slices "the same
    # dimension" — resolved structurally from the confirmed relationship catalog,
    # never from ``column_name``. For an enriched slice (``column_id`` is the fact's
    # FK column), ``dimension_table_id`` is the FK-target dim table, ``fk_role`` is
    # the FK column name (carried for role-playing dims — NOT yet a Phase-A identity
    # key), and ``dimension_attribute`` is the enriched suffix (the level, e.g.
    # ``account_type``; NULL when grouping by the FK key itself). All three are NULL
    # for a folded slice (an own categorical column with no grain-safe FK): a folded
    # dimension has no cross-table identity in Phase A and abstains from conformed
    # pairing (that residual is DAT-757). The identity ``(dimension_table_id,
    # dimension_attribute)`` is the single key both the lineage stock/flow witness
    # (``shared_dims``) and the operating-model ``conformed_dimension`` edge group on.
    dimension_table_id: Mapped[str | None] = mapped_column(
        ForeignKey("tables.table_id"), nullable=True
    )
    dimension_attribute: Mapped[str | None] = mapped_column(String, nullable=True)
    fk_role: Mapped[str | None] = mapped_column(String, nullable=True)

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

    # Relationships. ``table_id`` and ``dimension_table_id`` both FK to
    # ``tables.table_id`` (DAT-756), so the fact-table relationship must name its
    # column explicitly; the dimension table is read as a plain id, no ORM edge.
    table: Mapped[Table] = relationship(foreign_keys=[table_id])
    column: Mapped[Column] = relationship()


__all__ = [
    "SliceDefinition",
]
