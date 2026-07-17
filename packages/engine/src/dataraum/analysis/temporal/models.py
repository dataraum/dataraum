"""Temporal analysis models.

Consolidated Pydantic models for temporal analysis:
- Detection: granularity, gaps, completeness (the served coverage substrate)

The value-series pattern analyzers (seasonality/trend/change-point) were removed
in DAT-524; the fiscal-calendar and update-frequency analyzers were removed in
DAT-783 after the finance-corpus validation found them WRONG (fiscal false-positives
on any span that isn't a whole number of years — wrap-around months double-count;
update-frequency regularity collapses to a meaningless value on multi-row-per-
timestamp fact tables — median interval is 0). What survives is the completeness/gap
substrate the P5 temporal-coverage re-cut and the cockpit look_profile surface consume.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from dataraum.core.models.base import ColumnRef

# =============================================================================
# Basic Temporal Detection Models
# =============================================================================


class TemporalGapInfo(BaseModel):
    """Information about a gap in the time series.

    Persisted inside ``temporal_column_profiles.gaps`` (a JSON interior) — the
    ``severity`` vocabulary is closed and enforced at construction (the two-layer
    standard: JSON interiors get a strict Pydantic submodel at the writer, DAT-783).
    """

    gap_start: datetime
    gap_end: datetime
    gap_length_days: float
    missing_periods: int
    severity: Literal["minor", "moderate", "severe"]


class TemporalCompletenessAnalysis(BaseModel):
    """Temporal completeness analysis.

    Computed from the DISTINCT timestamps (robust to duplicate-per-day fact rows),
    so the gaps are genuine absences between consecutive present periods.

    ``actual_periods`` and ``expected_periods`` are both counts of **detected-grain
    buckets** over the same ``[min, max]`` window, so ``completeness_ratio`` is
    ``actual / expected`` in one unit and lands in [0, 1] by construction — no clamp
    (DAT-810). The three are ``None`` together when the grain is ``irregular``/
    ``unknown``: those have no bucket, so completeness over them is not computable and
    falls loud rather than resolving to a plausible 1.0/0.0. The gap fields stay
    populated — a gap is measured against the median gap, not against a grain.
    """

    completeness_ratio: float | None  # 0-1, or None when the grain has no bucket
    expected_periods: int | None
    actual_periods: int | None
    gap_count: int
    largest_gap_days: float | None = None
    gaps: list[TemporalGapInfo] = Field(default_factory=list)


# =============================================================================
# Main Result Models
# =============================================================================


class TemporalAnalysisResult(BaseModel):
    """Complete temporal analysis result for a single column.

    This is the per-column result type returned in TemporalProfileResult.column_profiles.
    Every field here has a typed home on ``temporal_column_profiles`` — nothing is
    left in a write-only JSON blob (DAT-783).
    """

    metric_id: str
    column_id: str
    column_ref: ColumnRef
    column_name: str
    table_name: str
    computed_at: datetime

    # Basic temporal info
    min_timestamp: datetime
    max_timestamp: datetime
    span_days: float
    detected_granularity: str
    granularity_confidence: float

    # Staleness (freshness of the last observation vs the detected cadence). The
    # only survivor of the deleted update-frequency analysis — served flat + read.
    is_stale: bool = False

    # Completeness / gaps (the coverage substrate)
    completeness: TemporalCompletenessAnalysis | None = None


class TemporalProfileResult(BaseModel):
    """Result of temporal profiling for a table.

    This is the main return type for profile_temporal(), following the
    same pattern as StatisticsProfileResult.
    """

    column_profiles: list[TemporalAnalysisResult] = Field(default_factory=list)
    duration_seconds: float = 0.0


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Basic detection
    "TemporalGapInfo",
    "TemporalCompletenessAnalysis",
    # Main results
    "TemporalAnalysisResult",
    "TemporalProfileResult",
]
