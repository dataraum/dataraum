"""On-demand validation verdict — re-run the stored SQL and judge it (ADR-0017).

The durable validation artifact stores the run-versioned SQL, not the pass/fail
verdict: a stored verdict goes stale the moment data is re-imported, the SQL does
not (DAT-617). So the verdict is *computed*, never stored-and-read.

The judgement is uniform: every validation SQL returns ONE row with a
non-negative numeric ``deviation`` (0 = perfectly satisfied) and a ``magnitude``
(the reference scale). The verdict is the single rule ``deviation <= tolerance``
— no per-check_type branching, no guessing which column carries the answer (the
deleted column-name string-matching).

Entry points:
- ``evaluate_result(spec, rows)`` — pure judgement over already-fetched rows.
- ``verdict_from_sql(conn, sql, tolerance=...)`` — re-run SQL, then judge. The
  spec-free form the in-run entropy detector uses (it holds ``tolerance`` from
  the result record, not a full spec).
- ``evaluate_validation(conn, sql, spec)`` — ``verdict_from_sql`` keyed off a spec.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dataraum.analysis.validation.models import ValidationSpec, ValidationStatus
from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    import duckdb

logger = get_logger(__name__)

# Numeric tolerance applied when the spec/record declares none.
DEFAULT_TOLERANCE = 0.01


@dataclass
class ValidationVerdict:
    """The recomputed pass/fail judgement for one validation.

    ``status`` PASSED/FAILED is a judged measurement of the data; ERROR means
    the evaluation is INCONCLUSIVE (the SQL ran but did not honor the output
    contract, OR re-running it failed). ``passed`` is ``status == PASSED``.
    """

    status: ValidationStatus
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def verdict_from_sql(
    duckdb_conn: duckdb.DuckDBPyConnection,
    sql_used: str | None,
    *,
    tolerance: float = DEFAULT_TOLERANCE,
    check_type: str = "",
) -> ValidationVerdict:
    """Re-run a validation's stored SQL against current data and judge it.

    The on-demand verdict (DAT-617): consumers hold the run-versioned
    ``sql_used`` and the declared ``tolerance``, never a stale stored pass/fail.
    Re-run the SQL on the given connection (which must point at the current
    typed lake) and apply the contract judgement.

    A re-run that raises is INCONCLUSIVE (ERROR), never FAILED — a query that no
    longer plans against re-imported data is ignorance, not a measured failure.

    Args:
        duckdb_conn: Connection scoped to the current typed lake.
        sql_used: The validation's grounded SQL. ``None``/empty means the spec
            never bound (skipped / generation error) — no data verdict to
            recompute; the caller keeps the durable grounding outcome instead.
        tolerance: The declared pass threshold (``deviation <= tolerance``).
        check_type: Optional label carried into the message/details.

    Returns:
        ValidationVerdict with the freshly computed status/passed/message/details.
    """
    if not sql_used:
        return ValidationVerdict(
            status=ValidationStatus.ERROR,
            passed=False,
            message="No SQL bound for this validation",
            details={"check_type": check_type},
        )

    try:
        result_obj = duckdb_conn.execute(sql_used)
        col_names = [desc[0] for desc in result_obj.description]
        result_rows: list[dict[str, Any]] = [
            dict(zip(col_names, row, strict=True)) for row in result_obj.fetchall()
        ]
    except Exception as e:
        logger.warning("validation_reevaluate_failed", error=str(e))
        return ValidationVerdict(
            status=ValidationStatus.ERROR,
            passed=False,
            message=f"SQL execution error: {e}",
            details={"check_type": check_type},
        )

    status, message, details = _judge(check_type, tolerance, result_rows)
    return ValidationVerdict(
        status=status,
        passed=status == ValidationStatus.PASSED,
        message=message,
        details=details,
    )


def evaluate_validation(
    duckdb_conn: duckdb.DuckDBPyConnection,
    sql_used: str | None,
    spec: ValidationSpec,
) -> ValidationVerdict:
    """``verdict_from_sql`` keyed off a declared spec (its tolerance + check_type)."""
    return verdict_from_sql(
        duckdb_conn,
        sql_used,
        tolerance=float(spec.parameters.get("tolerance", DEFAULT_TOLERANCE)),
        check_type=spec.check_type,
    )


def evaluate_result(
    spec: ValidationSpec,
    result_rows: list[dict[str, Any]],
) -> tuple[ValidationStatus, str, dict[str, Any]]:
    """Pure judgement over already-fetched rows, keyed off a spec."""
    return _judge(
        spec.check_type,
        float(spec.parameters.get("tolerance", DEFAULT_TOLERANCE)),
        result_rows,
    )


def _judge(
    check_type: str,
    tolerance: float,
    result_rows: list[dict[str, Any]],
) -> tuple[ValidationStatus, str, dict[str, Any]]:
    """The contract judgement (ADR-0017): ``deviation <= tolerance``.

    Every validation SQL returns ONE row with a non-negative numeric
    ``deviation`` (0 = perfectly satisfied) and a ``magnitude`` (the reference
    scale severity is judged against). PASSED/FAILED is the judged measurement.
    ERROR means INCONCLUSIVE: the SQL ran but did not honor the contract (no
    row, or no numeric ``deviation``). Inconclusive is never FAILED — it would
    pollute the ``cross_table_consistency`` failure measurements (DAT-439).

    Returns ``(status, message, details)``; ``details`` carries the flat
    ``deviation``/``magnitude``/``tolerance`` the entropy scorer reads.
    """
    if not result_rows:
        return (
            ValidationStatus.ERROR,
            f"{check_type or 'validation'} check inconclusive: query returned no rows",
            {"check_type": check_type},
        )

    row = result_rows[0]
    raw_deviation = row.get("deviation")
    if raw_deviation is None:
        return (
            ValidationStatus.ERROR,
            f"{check_type or 'validation'} check inconclusive: SQL did not return the "
            f"contracted 'deviation' column (got {list(row.keys())})",
            {"check_type": check_type, "row": row},
        )
    try:
        deviation = abs(float(raw_deviation))
        # magnitude falls back so the scorer's deviation/magnitude never divides
        # by zero: a 0/absent magnitude falls to the deviation itself, then 1.0.
        magnitude = abs(float(row.get("magnitude") or 0)) or deviation or 1.0
    except TypeError, ValueError:
        return (
            ValidationStatus.ERROR,
            f"{check_type or 'validation'} check inconclusive: non-numeric deviation "
            f"{raw_deviation!r}",
            {"check_type": check_type, "row": row},
        )

    passed = deviation <= tolerance
    status = ValidationStatus.PASSED if passed else ValidationStatus.FAILED
    return (
        status,
        f"{check_type or 'validation'}: deviation {deviation:.6g} (tolerance {tolerance:.6g})",
        {
            "check_type": check_type,
            "deviation": deviation,
            "magnitude": magnitude,
            "tolerance": tolerance,
        },
    )


__all__ = [
    "DEFAULT_TOLERANCE",
    "ValidationVerdict",
    "evaluate_result",
    "evaluate_validation",
    "verdict_from_sql",
]
