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

    # The winning event-time axis per side (DAT-565 competes every axis a table
    # names; DAT-778 persists the winner). ``*_time_axis_column`` is the raw axis
    # name — always populated, since it is literally the name that won the
    # competition. ``*_time_axis_column_id`` is that name resolved against the
    # table's typed ``columns`` and is NULLABLE: ``TimeColumn.column`` is
    # unvalidated LLM output (DAT-780 adds the event/attribute rule + a
    # real-column check at save) and can name a column that isn't in ``columns``
    # — an honest NULL then, never a sentinel string. Consumed by DAT-780's K2
    # anchor designation: "witness axis overrides where a witness exists."
    measure_time_axis_column: Mapped[str] = mapped_column(String, nullable=False)
    measure_time_axis_column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id"), nullable=True
    )
    event_time_axis_column: Mapped[str] = mapped_column(String, nullable=False)
    event_time_axis_column_id: Mapped[str | None] = mapped_column(
        ForeignKey("columns.column_id"), nullable=True
    )

    # The winning PHYSICAL slice column per side (DAT-756: a table can carry
    # several role-playing ``SliceDefinition``s at the same conformed identity —
    # e.g. ``debit_account`` vs ``credit_account`` both -> chart_of_accounts —
    # and the competition can pick either independently per side; collapsing to
    # one field would silently drop reproducibility on whichever side isn't
    # captured, the same bug class this row exists to fix). Always resolvable:
    # sourced from ``SliceDefinition.column_id``, which is NOT NULL by schema.
    measure_slice_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id"), nullable=False
    )
    event_slice_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id"), nullable=False
    )

    # The pairing the verdict was computed under (audit + re-run reproducibility,
    # now honored by the six columns above plus these three): the shared slice
    # dimension's human-readable label (the conformed identity, not either side's
    # physical column — see ``measure_slice_column_id``/``event_slice_column_id``
    # for that), the signed convention over the event fact's per-period sums, and
    # the period grain.
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
