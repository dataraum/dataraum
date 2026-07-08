"""SQLAlchemy model for the per-metric additivity verdict (DAT-716).

The durable form of the metric-drill additivity verdict computed at the
operating_model ``metrics`` phase (logic in :mod:`dataraum.graphs.additivity`).
One run-versioned row per ``(metric_key, run_id)`` — the drill reads
``current_metric_additivity`` to decide, for a metric node, whether to offer a
time grain and whether a categorical breakdown *reconciles* (sums to the total)
or shows the honest dash.

Run-versioned like the metric ``lifecycle_artifacts`` it derives from: the
version axis is the operating_model ``run_id``, current once that run is promoted
under the ``(catalog, "operating_model")`` head (``current_metric_additivity``,
DAT-506 read-view machinery). Recomputed every session cascade from the run's
live ``temporal_behavior`` — never frozen. The ``(metric_key, run_id)`` UNIQUE is
the run-grain contract's form-(a) upsert key (ADR-0010).
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import Boolean, DateTime, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage import Base


class MetricAdditivity(Base):
    """One metric's additivity verdict, run-versioned (DAT-716).

    Keyed ``(metric_key, run_id)`` where ``metric_key`` is the metric's
    ``lifecycle_artifacts.artifact_key`` (its ``graph_id``) — the id the drill
    resolves a canvas node to. ``*_additive`` say whether a breakdown by that
    axis class reconciles to the unsliced total; ``*_reason`` names the cause
    when it does not (``stock`` / ``average`` / ``distinct_count`` /
    ``snapshot_count`` / ``min_max`` / ``ratio`` / ``unknown_aggregate``), NULL
    when it reconciles.
    """

    __tablename__ = "metric_additivity"
    __table_args__ = (
        UniqueConstraint("metric_key", "run_id", name="uq_metric_additivity_metric_run"),
    )

    additivity_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    # Snapshot version axis (DAT-413): the operating_model run that computed this.
    run_id: Mapped[str] = mapped_column(String, nullable=False, index=True)
    metric_key: Mapped[str] = mapped_column(String, nullable=False, index=True)

    categorical_additive: Mapped[bool] = mapped_column(Boolean, nullable=False)
    time_additive: Mapped[bool] = mapped_column(Boolean, nullable=False)
    categorical_reason: Mapped[str | None] = mapped_column(String)
    time_reason: Mapped[str | None] = mapped_column(String)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=lambda: datetime.now(UTC)
    )
