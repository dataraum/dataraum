"""Database models for temporal slice analysis persistence."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import JSON, DateTime, Float, Integer, String
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


__all__ = [
    "ColumnDriftSummary",
]
