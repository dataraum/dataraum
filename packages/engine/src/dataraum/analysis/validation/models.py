"""Pydantic models for generic validation checks.

Contains data structures for validation specs and results.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


def _utc_now() -> datetime:
    """Return current UTC time (timezone-aware)."""
    return datetime.now(UTC)


class ValidationSeverity(StrEnum):
    """Severity levels for validation failures."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class ValidationStatus(StrEnum):
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
    relevant_cycles: list[str] = Field(
        default_factory=list
    )  # cycle types this applies to; empty = universal
    version: str = "1.0"
    source: str = "config"


class ValidationSQLOutput(BaseModel):
    """Pydantic model for LLM tool output - validation SQL generation.

    Used as a tool definition for structured LLM output via tool use API.
    """

    sql: str | None = Field(
        description="The DuckDB SQL query to execute. Null if validation cannot be performed."
    )
    # No free-text `explanation` field: it flowed into GeneratedSQL and was read by
    # nothing (DAT-603 consumer audit) — an unread sentence per call is pure
    # serial-decode latency. The judgeable context lives in columns_used +
    # skip_reason; the spec itself already says what is being validated.
    columns_used: list[str] = Field(
        default_factory=list,
        description="List of columns used in the query, in 'table.column' format.",
    )
    can_validate: bool = Field(
        description="Whether the validation can be performed with the available schema."
    )
    skip_reason: str | None = Field(
        default=None,
        description="If can_validate is false, explain why (e.g., 'Missing required columns: ...').",
    )


class GeneratedSQL(BaseModel):
    """LLM-generated SQL for a validation check."""

    validation_id: str
    sql_query: str
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

    @classmethod
    def from_results(
        cls,
        *,
        run_id: str,
        table_ids: list[str],
        table_name: str,
        started_at: datetime,
        results: list[ValidationResult],
    ) -> ValidationRunResult:
        """Summarize a run's individual results into the aggregate.

        ``run_id`` is the workflow-minted run (DAT-408), never minted here.
        """
        passed = sum(1 for r in results if r.status == ValidationStatus.PASSED)
        failed = sum(1 for r in results if r.status == ValidationStatus.FAILED)
        skipped = sum(1 for r in results if r.status == ValidationStatus.SKIPPED)
        errors = sum(1 for r in results if r.status == ValidationStatus.ERROR)
        return cls(
            run_id=run_id,
            table_ids=table_ids,
            table_name=table_name,
            started_at=started_at,
            completed_at=_utc_now(),
            results=results,
            total_checks=len(results),
            passed_checks=passed,
            failed_checks=failed,
            skipped_checks=skipped,
            error_checks=errors,
            # ``overall_status`` collapses to FAILED on either a judged data
            # failure OR an inconclusive/errored check (``errors`` now
            # includes inconclusive evaluations, DAT-439) — it is a coarse
            # "not all-clean" flag, NOT a pure data-failure signal. A
            # cockpit/readiness consumer that needs to distinguish "data is
            # wrong" from "couldn't judge" must read the per-check
            # ``failed_checks`` vs ``error_checks`` axes, not this rollup
            # (a DEGRADED/INCONCLUSIVE overall state is DAT-440+).
            overall_status=(
                ValidationStatus.FAILED if (failed or errors) else ValidationStatus.PASSED
            ),
            has_critical_failures=any(
                r.status == ValidationStatus.FAILED and r.severity == ValidationSeverity.CRITICAL
                for r in results
            ),
        )


__all__ = [
    "ValidationSeverity",
    "ValidationStatus",
    "ValidationSpec",
    "GeneratedSQL",
    "ValidationResult",
    "ValidationRunResult",
]
