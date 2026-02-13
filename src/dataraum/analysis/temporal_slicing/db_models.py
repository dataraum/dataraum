"""Database models for temporal slice analysis persistence."""

from __future__ import annotations

from datetime import UTC, date, datetime
from uuid import uuid4

from sqlalchemy import JSON, Date, DateTime, Float, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base


class ColumnDriftSummary(Base):
    """Stores drift analysis summary for one column in one slice table.

    One row per column per slice table — compact summary of JS divergence
    and drift evidence across all analyzed periods.
    """

    __tablename__ = "column_drift_summaries"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    slice_table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    column_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    time_column: Mapped[str] = mapped_column(String(255), nullable=False)

    # Core metrics
    max_js_divergence: Mapped[float] = mapped_column(Float, nullable=False)
    mean_js_divergence: Mapped[float] = mapped_column(Float, nullable=False)
    periods_analyzed: Mapped[int] = mapped_column(Integer, nullable=False)
    periods_with_drift: Mapped[int] = mapped_column(Integer, nullable=False)

    # Drift evidence (JSON) — for future LLM interpretation
    # Structure: {
    #   "worst_period": "2024-Q3",
    #   "worst_js": 0.42,
    #   "top_shifts": [{"category": "Active", "baseline_pct": 45.2,
    #                    "period_pct": 12.1, "period": "2024-Q3"}],
    #   "emerged_categories": [{"category": "Unknown", "period": "2024-Q3", "pct": 8.5}],
    #   "vanished_categories": [{"category": "Pending", "period": "2024-Q2",
    #                            "last_seen_pct": 3.1}],
    #   "change_points": ["2024-Q2"]
    # }
    drift_evidence_json: Mapped[dict[str, object] | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


class TemporalSliceAnalysis(Base):
    """Period-level completeness and volume anomaly metrics for a slice table.

    One row per period per slice table. Tracks data completeness (coverage,
    early cutoffs) and volume anomalies (z-scores, spikes/drops/gaps).
    """

    __tablename__ = "temporal_slice_analyses"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))

    slice_table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    time_column: Mapped[str] = mapped_column(String(255), nullable=False)

    # Period info
    period_label: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    # Completeness metrics
    row_count: Mapped[int | None] = mapped_column(Integer, nullable=True)
    expected_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    observed_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    coverage_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_complete: Mapped[int | None] = mapped_column(Integer, nullable=True)  # SQLite bool
    has_early_cutoff: Mapped[int | None] = mapped_column(Integer, nullable=True)  # SQLite bool
    days_missing_at_end: Mapped[int | None] = mapped_column(Integer, nullable=True)
    last_day_ratio: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Volume anomaly metrics
    z_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    rolling_avg: Mapped[float | None] = mapped_column(Float, nullable=True)
    rolling_std: Mapped[float | None] = mapped_column(Float, nullable=True)
    is_volume_anomaly: Mapped[int | None] = mapped_column(Integer, nullable=True)  # SQLite bool
    anomaly_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    period_over_period_change: Mapped[float | None] = mapped_column(Float, nullable=True)

    # Issues summary
    issues_json: Mapped[list[dict[str, str]] | None] = mapped_column(JSON, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


__all__ = [
    "ColumnDriftSummary",
    "TemporalSliceAnalysis",
]
