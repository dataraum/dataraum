"""Database models for temporal slice analysis persistence."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, Date, DateTime, Float, ForeignKey, Index, Integer, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from dataraum.storage.base import Base


class TemporalSliceRun(Base):
    """Records a temporal analysis run."""

    __tablename__ = "temporal_slice_runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    slice_table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    time_column: Mapped[str] = mapped_column(String(255), nullable=False)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)
    time_grain: Mapped[str] = mapped_column(String(50), nullable=False)

    # Run metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    # Summary stats
    total_periods: Mapped[int] = mapped_column(Integer, nullable=True)
    incomplete_periods: Mapped[int] = mapped_column(Integer, nullable=True)
    anomaly_count: Mapped[int] = mapped_column(Integer, nullable=True)
    drift_detected: Mapped[bool] = mapped_column(Integer, nullable=True)  # SQLite bool

    # Full config
    config_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)

    # Relationships - allows SQLAlchemy to handle FK ordering automatically
    analyses: Mapped[list["TemporalSliceAnalysis"]] = relationship(
        "TemporalSliceAnalysis", back_populates="run", cascade="all, delete-orphan"
    )
    drift_analyses: Mapped[list["TemporalDriftAnalysis"]] = relationship(
        "TemporalDriftAnalysis", back_populates="run", cascade="all, delete-orphan"
    )
    matrix_entries: Mapped[list["SliceTimeMatrixEntry"]] = relationship(
        "SliceTimeMatrixEntry", back_populates="run", cascade="all, delete-orphan"
    )


class TemporalSliceAnalysis(Base):
    """Stores temporal analysis results for a slice table."""

    __tablename__ = "temporal_slice_analyses"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(
        ForeignKey("temporal_slice_runs.run_id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Relationship to parent
    run: Mapped["TemporalSliceRun"] = relationship("TemporalSliceRun", back_populates="analyses")

    slice_table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    time_column: Mapped[str] = mapped_column(String(255), nullable=False)

    # Period info
    period_label: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    period_start: Mapped[date] = mapped_column(Date, nullable=False)
    period_end: Mapped[date] = mapped_column(Date, nullable=False)

    # Level 1: Completeness metrics
    row_count: Mapped[int] = mapped_column(Integer, nullable=True)
    expected_days: Mapped[int] = mapped_column(Integer, nullable=True)
    observed_days: Mapped[int] = mapped_column(Integer, nullable=True)
    coverage_ratio: Mapped[float] = mapped_column(Float, nullable=True)
    is_complete: Mapped[bool] = mapped_column(Integer, nullable=True)  # SQLite bool
    has_early_cutoff: Mapped[bool] = mapped_column(Integer, nullable=True)
    days_missing_at_end: Mapped[int] = mapped_column(Integer, nullable=True)
    last_day_ratio: Mapped[float] = mapped_column(Float, nullable=True)

    # Level 4: Volume metrics
    z_score: Mapped[float] = mapped_column(Float, nullable=True)
    rolling_avg: Mapped[float] = mapped_column(Float, nullable=True)
    rolling_std: Mapped[float] = mapped_column(Float, nullable=True)
    is_volume_anomaly: Mapped[bool] = mapped_column(Integer, nullable=True)  # SQLite bool
    anomaly_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    period_over_period_change: Mapped[float] = mapped_column(Float, nullable=True)

    # Issues summary
    issues_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)


class TemporalDriftAnalysis(Base):
    """Stores distribution drift analysis results per column per period."""

    __tablename__ = "temporal_drift_analyses"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(
        ForeignKey("temporal_slice_runs.run_id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Relationship to parent
    run: Mapped["TemporalSliceRun"] = relationship("TemporalSliceRun", back_populates="drift_analyses")

    slice_table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    column_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    period_label: Mapped[str] = mapped_column(String(50), nullable=False, index=True)

    # Drift metrics
    js_divergence: Mapped[float] = mapped_column(Float, nullable=True)
    chi_square_statistic: Mapped[float] = mapped_column(Float, nullable=True)
    chi_square_p_value: Mapped[float] = mapped_column(Float, nullable=True)

    # Category changes
    new_categories_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    missing_categories_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    # Flags
    has_significant_drift: Mapped[bool] = mapped_column(Integer, nullable=True)
    has_category_changes: Mapped[bool] = mapped_column(Integer, nullable=True)


class SliceTimeMatrixEntry(Base):
    """Stores slice × time matrix entries for cross-slice comparison."""

    __tablename__ = "slice_time_matrix_entries"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str] = mapped_column(
        ForeignKey("temporal_slice_runs.run_id", ondelete="CASCADE"), nullable=False, index=True
    )

    # Relationship to parent
    run: Mapped["TemporalSliceRun"] = relationship("TemporalSliceRun", back_populates="matrix_entries")

    slice_table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    slice_column: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    slice_value: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    period_label: Mapped[str] = mapped_column(String(50), nullable=False, index=True)
    row_count: Mapped[int] = mapped_column(Integer, nullable=True)
    period_over_period_change: Mapped[float] = mapped_column(Float, nullable=True)


class TemporalTopologyAnalysis(Base):
    """Temporal topology analysis results.

    Tracks how data structure (correlation topology) changes over time.
    Detects structural drift, complexity trends, and anomalous periods.
    """

    __tablename__ = "temporal_topology_analyses"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    run_id: Mapped[str | None] = mapped_column(
        ForeignKey("temporal_slice_runs.run_id", ondelete="CASCADE"), nullable=True, index=True
    )
    slice_table_name: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    # Analysis parameters
    time_column: Mapped[str] = mapped_column(String(255), nullable=False)
    period_granularity: Mapped[str] = mapped_column(String(50), default="month")
    correlation_threshold: Mapped[float] = mapped_column(Float, default=0.5)

    # Summary metrics
    periods_analyzed: Mapped[int] = mapped_column(Integer, default=0)
    avg_complexity: Mapped[float] = mapped_column(Float, nullable=True)
    complexity_variance: Mapped[float] = mapped_column(Float, nullable=True)
    trend_direction: Mapped[str] = mapped_column(String(50), default="stable")
    num_drifts_detected: Mapped[int] = mapped_column(Integer, default=0)
    num_anomaly_periods: Mapped[int] = mapped_column(Integer, default=0)

    # Detailed data (JSON)
    period_topologies_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    topology_drifts_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    anomaly_periods_json: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)

    # Metadata
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)


# Composite indexes for common query patterns
Index(
    "idx_slice_analysis_run_table",
    TemporalSliceAnalysis.run_id,
    TemporalSliceAnalysis.slice_table_name,
)
Index(
    "idx_drift_run_table_column",
    TemporalDriftAnalysis.run_id,
    TemporalDriftAnalysis.slice_table_name,
    TemporalDriftAnalysis.column_name,
)
Index(
    "idx_matrix_run_table_column",
    SliceTimeMatrixEntry.run_id,
    SliceTimeMatrixEntry.slice_table_name,
    SliceTimeMatrixEntry.slice_column,
)


__all__ = [
    "TemporalSliceRun",
    "TemporalSliceAnalysis",
    "TemporalDriftAnalysis",
    "SliceTimeMatrixEntry",
    "TemporalTopologyAnalysis",
]
