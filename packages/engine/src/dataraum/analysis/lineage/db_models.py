"""SQLAlchemy models for aggregation-lineage discovery (DAT-491)."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class MeasureAggregationLineage(Base):
    """A reconciled events→measure rollup for one measure column, per run.

    Run-versioned like ``TableEntity``: one row per ``(measure_column_id, run_id)``,
    written by the ``aggregation_lineage`` session phase after the deterministic
    reconciliation statistic confirmed the LLM-proposed rollup. Only RECONCILED
    candidates persist — a row's existence means the measure provably aggregates
    the event table, and ``pattern`` says how it reconciles (``per_period`` ⇒ flow,
    ``cumulative`` ⇒ stock). Read by the ``structural_reconciliation`` witness of
    the ``temporal_behavior`` measurement (exact-run match: the witness fires at
    this run's session detect and abstains everywhere else).

    Discovery competes every event-time axis per fact (DAT-565) and every
    role-playing physical slice column at a shared dimension (DAT-756); DAT-778
    persists the WINNING axis/column of each competition on the fields below —
    previously discarded, which made the "audit + re-run reproducibility" claim
    unfulfillable. This is also the substrate for the K2 measure-anchor
    designation (DAT-780): anchor = witness axis where a witness (this row)
    exists.
    """

    __tablename__ = "measure_aggregation_lineage"
    __table_args__ = (
        UniqueConstraint("measure_column_id", "run_id", name="uq_measure_lineage_column_run"),
    )

    lineage_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    # Snapshot version axis (DAT-413): the begin_session run that discovered this.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)

    measure_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)
    measure_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id"), nullable=False, index=True
    )
    event_table_id: Mapped[str] = mapped_column(ForeignKey("tables.table_id"), nullable=False)

    # The winning event-time axis NAME per side (DAT-565 competes every axis a
    # table names; DAT-778 persists the winner) — the raw name that won, always
    # populated. The K2 anchor designation (``og_columns.anchor_time_axis``) reads
    # ``event_time_axis_column`` BY NAME and matches it against the enriched view's
    # served columns; the axis's typed identity is then that served column's own
    # ``source_column_id``. No separate ``*_time_axis_column_id`` is kept — the
    # DAT-778 id was vestigial (its intended K2 consumer resolves by name instead,
    # and the id is NULL exactly when the axis is a served/header name anyway).
    measure_time_axis_column: Mapped[str] = mapped_column(String, nullable=False)
    event_time_axis_column: Mapped[str] = mapped_column(String, nullable=False)

    # The pairing the verdict was computed under (audit): the shared slice
    # dimension's human-readable label (the conformed identity), the signed
    # convention over the event fact's per-period sums, and the period grain.
    slice_dimension: Mapped[str] = mapped_column(String, nullable=False)
    convention_sql: Mapped[str] = mapped_column(Text, nullable=False)
    period_grain: Mapped[str] = mapped_column(String, nullable=False)

    # The deterministic verdict (reconcile.dispose).
    pattern: Mapped[str] = mapped_column(String, nullable=False)  # per_period | cumulative
    match_rate: Mapped[float] = mapped_column(Float, nullable=False)
    r_flow_median: Mapped[float] = mapped_column(Float, nullable=False)
    r_stock_median: Mapped[float] = mapped_column(Float, nullable=False)
    n_entities: Mapped[int] = mapped_column(Integer, nullable=False)
    n_entities_fired: Mapped[int] = mapped_column(Integer, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
