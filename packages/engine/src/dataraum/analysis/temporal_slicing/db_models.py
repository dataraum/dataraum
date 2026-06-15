"""Database models for temporal slice analysis persistence."""

from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

from sqlalchemy import JSON, Date, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base


class TemporalSliceAnalysis(Base):
    """Per-(slice table, period) row count + numeric-column sums.

    One row per period per slice table PER RUN (DAT-502): the writer dedups
    in-batch and UPSERTs on ``(slice_table_name, period_label, run_id)`` — a
    Temporal success-redelivery (same ``run_id``) converges without a
    run-scoped clear, and a new run's rows coexist with prior runs'.

    The aggregation-lineage reconciliation's substrate (DAT-491): ``Σ events ≈
    Δ stock`` is arithmetic over ``column_sums`` (linearity of SUM). The drift /
    completeness / volume-anomaly columns that used to live here were cut with
    their writer (DAT-518) — they had no reader.
    """

    __tablename__ = "temporal_slice_analyses"
    __table_args__ = (
        UniqueConstraint(
            "slice_table_name", "period_label", "run_id", name="uq_tsa_slice_period_run"
        ),
    )

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    # Snapshot version axis (DAT-448): the begin_session run that computed these
    # sums. Nullable for the legacy/test path; the unique constraint guards the
    # grain for stamped rows (NULL run_id rows are NULLS-DISTINCT).
    run_id: Mapped[str | None] = mapped_column(String, nullable=True)

    slice_table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    time_column: Mapped[str] = mapped_column(String(255), nullable=False)

    # Period info
    period_label: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Per-period SUM of each numeric column of the slice table (DAT-491) —
    # the aggregation-lineage reconciliation's substrate.
    column_sums: Mapped[dict[str, float] | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = [
    "TemporalSliceAnalysis",
]
