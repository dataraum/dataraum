"""On-demand validation verdict — re-run the stored SQL and judge it.

The durable validation artifact stores the run-versioned SQL, not the pass/fail
verdict: a stored verdict goes stale the moment data is re-imported, the SQL does
not (DAT-617). So the verdict is *computed*, never stored-and-read. Both the
execute phase (at write time, on freshly generated SQL) and every verdict
consumer (on demand, by re-running ``sql_used`` at read time) judge a result
through the ONE per-``check_type`` evaluation here — there is no second copy of
the pass/fail logic to drift.

``evaluate_result`` is the pure judgement over already-fetched rows;
``evaluate_validation`` is the on-demand wrapper that runs ``sql_used`` first.
A bind failure (no ``sql_used``) has no data verdict to recompute — its
grounding outcome is durable and lives on the lifecycle artifact, not here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dataraum.analysis.validation.models import ValidationSpec, ValidationStatus
from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    import duckdb

logger = get_logger(__name__)

# Numeric tolerance applied to balance/comparison/aggregate checks when the spec
# declares none (parameters.tolerance). Shared by the write-time and read-time paths.
DEFAULT_TOLERANCE = 0.01


@dataclass
class ValidationVerdict:
    """The recomputed pass/fail judgement for one validation.

    ``status`` PASSED/FAILED is a judged measurement of the data; ERROR means
    the evaluation is INCONCLUSIVE (the SQL ran but its result shape cannot be
    judged, OR re-running it failed). ``passed`` is ``status == PASSED``.
    """

    status: ValidationStatus
    passed: bool
    message: str
    details: dict[str, Any] = field(default_factory=dict)


def evaluate_validation(
    duckdb_conn: duckdb.DuckDBPyConnection,
    sql_used: str | None,
    spec: ValidationSpec,
) -> ValidationVerdict:
    """Re-run a validation's stored SQL against current data and judge it.

    The on-demand verdict (DAT-617): consumers hold the run-versioned
    ``sql_used`` and the declared spec, never a stale stored pass/fail. Re-run
    the SQL on the given connection (which must point at the current typed
    lake) and re-apply the same per-``check_type`` evaluation the execute phase
    uses.

    A re-run that raises is INCONCLUSIVE (ERROR), never FAILED — a query that
    no longer plans against re-imported data is ignorance, not a measured data
    failure (mirrors ``execute_validation``).

    Args:
        duckdb_conn: Connection scoped to the current typed lake.
        sql_used: The validation's grounded SQL. ``None``/empty means the spec
            never bound (skipped or generation error) — there is no data
            verdict to recompute; the caller keeps the durable grounding
            outcome from the lifecycle artifact instead.
        spec: The declared validation spec (drives check_type + tolerance).

    Returns:
        ValidationVerdict with the freshly computed status/passed/message/details.
    """
    if not sql_used:
        # Unbound: no grounded SQL to run. The absence is the grounding outcome
        # (durable, does not go stale) — surfaced by the caller from the
        # lifecycle artifact, not recomputed here.
        return ValidationVerdict(
            status=ValidationStatus.ERROR,
            passed=False,
            message="No SQL bound for this validation",
            details={"check_type": spec.check_type},
        )

    try:
        result_obj = duckdb_conn.execute(sql_used)
        col_names = [desc[0] for desc in result_obj.description]
        result_rows: list[dict[str, Any]] = [
            dict(zip(col_names, row, strict=True)) for row in result_obj.fetchall()
        ]
    except Exception as e:
        logger.warning("validation_reevaluate_failed", validation_id=spec.validation_id, error=str(e))
        return ValidationVerdict(
            status=ValidationStatus.ERROR,
            passed=False,
            message=f"SQL execution error: {e}",
            details={"check_type": spec.check_type},
        )

    status, message, details = evaluate_result(spec, result_rows, len(result_rows))
    return ValidationVerdict(
        status=status,
        passed=status == ValidationStatus.PASSED,
        message=message,
        details=details,
    )


def evaluate_result(
    spec: ValidationSpec,
    result_rows: list[dict[str, Any]],
    row_count: int,
) -> tuple[ValidationStatus, str, dict[str, Any]]:
    """Evaluate validation result based on check type.

    PASSED/FAILED is a *judged measurement* of the data. ERROR means the
    evaluation is INCONCLUSIVE: the SQL ran, but the result shape cannot be
    judged (no recognizable columns, zero rows on a summary check, an
    unrecognized check type). An inconclusive evaluation is not a data
    failure — reporting it FAILED would pollute the failure measurements
    ``cross_table_consistency`` scores, so it must never reach FAILED
    (DAT-439; the artifact stays ``grounded`` with the reason).

    Args:
        spec: Validation spec
        result_rows: Query result rows
        row_count: Total row count

    Returns:
        Tuple of (status, message, details) with status PASSED/FAILED/ERROR
    """
    check_type = spec.check_type
    params = spec.parameters
    tolerance = params.get("tolerance", DEFAULT_TOLERANCE)

    def measured(passed: bool) -> ValidationStatus:
        return ValidationStatus.PASSED if passed else ValidationStatus.FAILED

    if check_type == "balance":
        # Balance checks compare two values
        if row_count == 0:
            return (
                ValidationStatus.ERROR,
                "Balance check inconclusive: query returned no rows",
                {"check_type": check_type},
            )

        row = result_rows[0]

        # Look for difference column first (preferred: LLM computes the diff)
        if "difference" in row or "diff" in row:
            diff = abs(float(row.get("difference", row.get("diff", 0)) or 0))
            # Promote magnitude into flat details so the scorer can
            # read it directly (it expects details["magnitude"]).
            mag = abs(float(row.get("magnitude") or 0)) or abs(diff) or 1
            return (
                measured(diff <= tolerance),
                f"Balance difference: {diff:.2f} (tolerance: {tolerance})",
                {
                    "check_type": check_type,
                    "difference": diff,
                    "magnitude": mag,
                    "tolerance": tolerance,
                    "row": row,
                },
            )

        # Look for standard balance column names
        value_cols = [k for k in row.keys() if "total" in k.lower() or "sum" in k.lower()]
        if len(value_cols) >= 2:
            val1 = float(row[value_cols[0]] or 0)
            val2 = float(row[value_cols[1]] or 0)
            diff = abs(val1 - val2)
            return (
                measured(diff <= tolerance),
                f"Balance check: {value_cols[0]}={val1:.2f}, {value_cols[1]}={val2:.2f}, diff={diff:.2f}",
                {
                    "check_type": check_type,
                    "values": row,
                    "difference": diff,
                    "tolerance": tolerance,
                },
            )

        # No recognizable columns — inconclusive, never FAILED
        return (
            ValidationStatus.ERROR,
            f"Balance check inconclusive: could not identify balance columns in result. "
            f"Columns returned: {list(row.keys())}",
            {"check_type": check_type, "row": row},
        )

    elif check_type == "constraint":
        # Constraint checks return violating rows; an empty result IS the
        # judgement (no violations), unlike the summary checks above.
        if row_count == 0:
            return (
                ValidationStatus.PASSED,
                "No constraint violations found",
                {"check_type": check_type},
            )
        # Extract total_rows from result columns if the LLM included it
        details: dict[str, Any] = {"check_type": check_type, "violation_count": row_count}
        if result_rows:
            for key in ("total_rows", "total_count", "total"):
                val = result_rows[0].get(key)
                if val is not None:
                    details["total_rows"] = int(val)
                    break
            # Check for violation_count column (LLM may return a single summary row)
            vc = result_rows[0].get("violation_count")
            if vc is not None and row_count == 1:
                # Single row with violation_count → summary, not raw violations
                details["violation_count"] = int(vc)
        return (
            ValidationStatus.FAILED,
            f"Found {details['violation_count']} constraint violations",
            details,
        )

    elif check_type == "comparison":
        # Comparison checks (e.g., Assets = Liabilities + Equity)
        if row_count == 0:
            return (
                ValidationStatus.ERROR,
                "Comparison check inconclusive: query returned no rows",
                {"check_type": check_type},
            )

        row = result_rows[0]
        tolerance = params.get("tolerance", DEFAULT_TOLERANCE)

        # Check for an equation_holds or is_valid column
        if "equation_holds" in row:
            passed = bool(row["equation_holds"])
            return (
                measured(passed),
                f"Equation check: {'passed' if passed else 'failed'}",
                {**row, "check_type": check_type},
            )

        if "is_valid" in row:
            passed = bool(row["is_valid"])
            return (
                measured(passed),
                f"Comparison check: {'passed' if passed else 'failed'}",
                {**row, "check_type": check_type},
            )

        # Check for difference column
        if "difference" in row:
            diff = abs(float(row["difference"] or 0))
            return (
                measured(diff <= tolerance),
                f"Comparison difference: {diff:.2f}",
                {"check_type": check_type, "difference": diff},
            )

        # No recognizable columns — inconclusive, never FAILED (the
        # smoke-proven three_way_match shape, DAT-439).
        return (
            ValidationStatus.ERROR,
            f"Comparison check inconclusive: could not identify comparison columns in result. "
            f"Columns returned: {list(row.keys())}",
            {"check_type": check_type, "row": row},
        )

    elif check_type == "aggregate":
        # Aggregate checks return summary values with a rate metric
        if row_count == 0:
            return (
                ValidationStatus.ERROR,
                "Aggregate check inconclusive: query returned no rows",
                {"check_type": check_type},
            )

        row = result_rows[0]
        details = {**row, "check_type": check_type}

        # Check orphan_rate / violation_rate against tolerance
        rate = None
        for key in ("orphan_rate", "violation_rate", "mismatch_rate", "error_rate"):
            val = row.get(key)
            if val is not None:
                rate = float(val)
                break

        if rate is not None:
            return (measured(rate <= tolerance), f"Aggregate rate: {rate:.4f}", details)

        # DAT-439 decision: no rate metric stays PASSED — the prompt
        # contract for aggregate checks is "summary values for review"
        # (no rate required); the rate judgement above is opportunistic.
        return (ValidationStatus.PASSED, "Aggregate check completed", details)

    else:
        # Unrecognized check type: the evaluator has no semantics to
        # judge with — inconclusive, never a row_count>0 guess (DAT-439
        # sweep; previously "assume passing if any results").
        return (
            ValidationStatus.ERROR,
            f"Cannot evaluate check_type {check_type!r}: no evaluation semantics defined "
            f"(query returned {row_count} rows)",
            {"check_type": check_type, "row_count": row_count},
        )


__all__ = [
    "DEFAULT_TOLERANCE",
    "ValidationVerdict",
    "evaluate_result",
    "evaluate_validation",
]
