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
    """

    __tablename__ = "measure_aggregation_lineage"
    __table_args__ = (
        UniqueConstraint("measure_column_id", "run_id", name="uq_measure_lineage_column_run"),
    )

    lineage_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    session_id: Mapped[str] = mapped_column(
        ForeignKey("investigation_sessions.session_id"), nullable=False, index=True
    )
    # Snapshot version axis (DAT-413): the begin_session run that discovered this.
    run_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)

    measure_table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    measure_column_id: Mapped[str] = mapped_column(
        ForeignKey("columns.column_id", ondelete="CASCADE"), nullable=False, index=True
    )
    event_table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )

    # The alignment the verdict was computed under (audit + re-run reproducibility).
    # event_join_*: the optional header join for split header/line event data
    # (line table aliased ``e``, header ``h`` in the *_sql expressions).
    event_join_duckdb_path: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_join_on_sql: Mapped[str | None] = mapped_column(Text, nullable=True)
    event_value_sql: Mapped[str] = mapped_column(Text, nullable=False)
    measure_key_sql: Mapped[str] = mapped_column(Text, nullable=False)
    event_key_sql: Mapped[str] = mapped_column(Text, nullable=False)
    measure_period_sql: Mapped[str] = mapped_column(Text, nullable=False)
    event_period_sql: Mapped[str] = mapped_column(Text, nullable=False)
    event_filter_sql: Mapped[str | None] = mapped_column(Text, nullable=True)

    # The deterministic verdict (reconcile.dispose).
    pattern: Mapped[str] = mapped_column(String, nullable=False)  # per_period | cumulative
    match_rate: Mapped[float] = mapped_column(Float, nullable=False)
    r_flow_median: Mapped[float] = mapped_column(Float, nullable=False)
    r_stock_median: Mapped[float] = mapped_column(Float, nullable=False)
    n_entities: Mapped[int] = mapped_column(Integer, nullable=False)
    n_entities_fired: Mapped[int] = mapped_column(Integer, nullable=False)

    rationale: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
