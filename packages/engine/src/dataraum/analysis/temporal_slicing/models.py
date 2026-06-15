"""Pydantic models for temporal slice analysis."""

from __future__ import annotations

from datetime import date
from enum import StrEnum

from pydantic import BaseModel, Field


class TimeGrain(StrEnum):
    """Time granularity for period bucketing."""

    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class PeriodSums(BaseModel):
    """One populated period of a slice table: row count + numeric-column sums.

    The aggregation-lineage reconciliation substrate (DAT-491): sums are linear,
    so signed conventions (debit−credit, …) are arithmetic over these per-period
    values. ``period_label`` is the cross-fact alignment key.
    """

    period_label: str
    period_start: date
    period_end: date
    row_count: int
    column_sums: dict[str, float] = Field(default_factory=dict)


__all__ = [
    "TimeGrain",
    "PeriodSums",
]
