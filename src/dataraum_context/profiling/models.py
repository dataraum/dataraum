"""Profiling layer models.

Defines data structures for statistical profiling and type inference."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from dataraum_context.core.models.base import (
    ColumnRef,
    DataType,
    DecisionSource,
)


class NumericStats(BaseModel):
    """Statistics for numeric columns."""

    min_value: float
    max_value: float
    mean: float
    stddev: float
    percentiles: dict[str, float] = Field(default_factory=dict)


class StringStats(BaseModel):
    """Statistics for string columns."""

    min_length: int
    max_length: int
    avg_length: float


class HistogramBucket(BaseModel):
    """A histogram bucket."""

    bucket_min: float | str
    bucket_max: float | str
    count: int


class ValueCount(BaseModel):
    """A value with its count."""

    value: Any
    count: int
    percentage: float


class DetectedPattern(BaseModel):
    """A detected pattern in column values."""

    name: str
    match_rate: float
    semantic_type: str | None = None


class TypeCandidate(BaseModel):
    """A candidate type for a column."""

    column_id: str
    column_ref: ColumnRef

    data_type: DataType
    confidence: float
    parse_success_rate: float
    failed_examples: list[str] = Field(default_factory=list)

    detected_pattern: str | None = None
    pattern_match_rate: float | None = None

    detected_unit: str | None = None
    unit_confidence: float | None = None


class ColumnProfile(BaseModel):
    """Statistical profile of a column."""

    column_id: str
    column_ref: ColumnRef
    profiled_at: datetime

    total_count: int
    null_count: int
    distinct_count: int

    null_ratio: float
    cardinality_ratio: float

    numeric_stats: NumericStats | None = None
    string_stats: StringStats | None = None

    histogram: list[HistogramBucket] | None = None
    top_values: list[ValueCount] | None = None
    detected_patterns: list[DetectedPattern] = Field(default_factory=list)


class ProfileResult(BaseModel):
    """Result of profiling operation."""

    profiles: list[ColumnProfile]
    type_candidates: list[TypeCandidate]
    duration_seconds: float


# === Type Resolution Models ===


class TypeDecision(BaseModel):
    """A type decision for a column."""

    column_id: str
    decided_type: DataType
    decision_source: DecisionSource = DecisionSource.AUTO
    decision_reason: str | None = None


class ColumnCastResult(BaseModel):
    """Cast result for a single column."""

    column_id: str
    column_ref: ColumnRef
    source_type: str
    target_type: DataType
    success_count: int
    failure_count: int
    success_rate: float
    failure_samples: list[str] = Field(default_factory=list)


class TypeResolutionResult(BaseModel):
    """Result of type resolution."""

    typed_table_name: str
    quarantine_table_name: str

    total_rows: int
    typed_rows: int
    quarantined_rows: int

    column_results: list[ColumnCastResult]


# === Enrichment Models ===
