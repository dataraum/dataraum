"""Pydantic models for generic validation checks.

Contains data structures for validation specs and results.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(UTC)


class ValidationSeverity(str, Enum):
    """Severity levels for validation failures."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationStatus(str, Enum):
    """Status of a validation check."""

    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    ERROR = "error"


class ValidationSpec(BaseModel):
    """Specification for a validation check.

    Loaded from YAML configuration files. The LLM interprets the schema
    and description to identify relevant columns - no pre-resolution needed.
    """

    validation_id: str
    name: str
    description: str
    category: str  # 'financial', 'data_quality', 'business_rule'
    severity: ValidationSeverity = ValidationSeverity.ERROR

    # Check definition
    check_type: str  # 'balance', 'comparison', 'constraint', 'aggregate'
    parameters: dict[str, Any] = Field(default_factory=dict)

    # SQL generation hints for LLM
    sql_hints: str | None = None  # Free-form hints for SQL generation
    expected_outcome: str | None = None  # What a passing result looks like

    # Metadata
    tags: list[str] = Field(default_factory=list)
    version: str = "1.0"
    source: str = "config"


class GeneratedSQL(BaseModel):
    """LLM-generated SQL for a validation check."""

    validation_id: str
    sql_query: str
    explanation: str | None = None
    columns_used: list[str] = Field(default_factory=list)  # Columns identified by LLM

    # Generation metadata
    generated_at: datetime = Field(default_factory=_utc_now)
    model_used: str | None = None

    # Validation info
    is_valid: bool = True
    validation_error: str | None = None


class ValidationResult(BaseModel):
    """Result of executing a validation check."""

    validation_id: str
    spec_name: str
    status: ValidationStatus
    severity: ValidationSeverity

    # Execution details
    table_ids: list[str] = Field(default_factory=list)
    table_name: str
    executed_at: datetime = Field(default_factory=_utc_now)

    # Results
    passed: bool = False
    message: str = ""
    details: dict[str, Any] = Field(default_factory=dict)

    # SQL execution
    sql_used: str | None = None
    columns_used: list[str] = Field(default_factory=list)  # Columns LLM identified
    result_rows: list[dict[str, Any]] = Field(default_factory=list)
    row_count: int = 0


class ValidationRunResult(BaseModel):
    """Result of running all validations across tables."""

    run_id: str
    table_ids: list[str] = Field(default_factory=list)
    table_name: str
    started_at: datetime = Field(default_factory=_utc_now)
    completed_at: datetime | None = None

    # Results
    results: list[ValidationResult] = Field(default_factory=list)
    total_checks: int = 0
    passed_checks: int = 0
    failed_checks: int = 0
    skipped_checks: int = 0
    error_checks: int = 0

    # Summary
    overall_status: ValidationStatus = ValidationStatus.PASSED
    has_critical_failures: bool = False


__all__ = [
    "ValidationSeverity",
    "ValidationStatus",
    "ValidationSpec",
    "GeneratedSQL",
    "ValidationResult",
    "ValidationRunResult",
]
