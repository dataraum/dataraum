"""Type inference Pydantic models.

These models are used for computation and API responses.
They are NOT persisted directly - the db_models are the persistence layer.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from dataraum.core.models.base import (
    ColumnRef,
    DataType,
    DecisionSource,
)


class TypeCandidate(BaseModel):
    """A candidate type for a column (computation model).

    Created during type inference from value pattern matching.
    """

    column_id: str
    column_ref: ColumnRef

    data_type: DataType
    confidence: float
    parse_success_rate: float
    failed_examples: list[str] = Field(default_factory=list)

    # Pattern info (value-based only)
    detected_pattern: str | None = None
    pattern_match_rate: float | None = None

    # Unit detection (from Pint)
    detected_unit: str | None = None
    unit_confidence: float | None = None


class TypeDecision(BaseModel):
    """A type decision for a column (computation model)."""

    column_id: str
    decided_type: DataType
    decision_source: DecisionSource = DecisionSource.AUTO
    decision_reason: str | None = None


class ColumnCastResult(BaseModel):
    """Cast result for a single column during type resolution."""

    column_id: str
    column_ref: ColumnRef
    source_type: str
    target_type: DataType
    success_count: int
    failure_count: int
    success_rate: float
    failure_samples: list[str] = Field(default_factory=list)


class TypeResolutionResult(BaseModel):
    """Result of type resolution.

    Contains information about the typed and quarantine tables created.
    """

    typed_table_id: str  # UUID of the typed table record
    typed_table_name: str
    quarantine_table_name: str

    total_rows: int
    typed_rows: int
    quarantined_rows: int

    column_results: list[ColumnCastResult]
