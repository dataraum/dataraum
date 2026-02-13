"""Pydantic models for temporal slice analysis."""

from __future__ import annotations

from datetime import date
from enum import Enum

from pydantic import BaseModel, Field


class TimeGrain(str, Enum):
    """Time granularity for period analysis."""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class TemporalSliceConfig(BaseModel):
    """Configuration for temporal slice analysis."""

    time_column: str
    period_start: date
    period_end: date
    time_grain: TimeGrain = TimeGrain.MONTHLY

    # Thresholds
    drift_threshold: float = 0.1  # JS divergence threshold


class CategoryShift(BaseModel):
    """A significant change in category proportion."""

    category: str
    baseline_pct: float
    period_pct: float
    period: str


class CategoryAppearance(BaseModel):
    """A category that emerged (wasn't in baseline)."""

    category: str
    period: str
    pct: float


class CategoryDisappearance(BaseModel):
    """A category that vanished (was in baseline, gone in period)."""

    category: str
    period: str
    last_seen_pct: float


class DriftEvidence(BaseModel):
    """Evidence for interpreting what drifted."""

    worst_period: str
    worst_js: float
    top_shifts: list[CategoryShift] = Field(default_factory=list)
    emerged_categories: list[CategoryAppearance] = Field(default_factory=list)
    vanished_categories: list[CategoryDisappearance] = Field(default_factory=list)
    change_points: list[str] = Field(default_factory=list)


class ColumnDriftResult(BaseModel):
    """Result of drift analysis for one column."""

    column_name: str
    max_js_divergence: float
    mean_js_divergence: float
    periods_analyzed: int
    periods_with_drift: int
    drift_evidence: DriftEvidence | None = None


__all__ = [
    "TimeGrain",
    "TemporalSliceConfig",
    "CategoryShift",
    "CategoryAppearance",
    "CategoryDisappearance",
    "DriftEvidence",
    "ColumnDriftResult",
]
