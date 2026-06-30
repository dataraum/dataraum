"""Tests for the on-demand validation verdict (DAT-617).

``evaluate_result`` is the pure per-check_type judgement (moved verbatim from
``ValidationAgent._evaluate_result``); ``evaluate_validation`` re-runs a stored
``sql_used`` against current data and applies it. The verdict is computed, never
stored-and-read — a stored pass/fail goes stale on re-import, the SQL does not.
"""

from __future__ import annotations

import pytest

from dataraum.analysis.validation.evaluate import (
    ValidationVerdict,
    evaluate_result,
    evaluate_validation,
)
from dataraum.analysis.validation.models import ValidationSpec, ValidationStatus


def _eval_spec(check_type: str, **parameters) -> ValidationSpec:
    return ValidationSpec(
        validation_id="test",
        name="Test",
        description="Test",
        category="test",
        check_type=check_type,
        parameters=parameters,
    )


class TestEvaluateResult:
    """The pure judgement over already-fetched rows.

    Returns (status, message, details): PASSED/FAILED is a judged measurement;
    ERROR means INCONCLUSIVE — the result shape could not be judged.
    Inconclusive must never surface as FAILED (DAT-439).
    """

    def test_balance_check_passed(self) -> None:
        spec = _eval_spec("balance", tolerance=0.01)
        rows = [{"total_debits": 150.00, "total_credits": 150.00, "difference": 0.00}]
        status, message, _ = evaluate_result(spec, rows, 1)
        assert status == ValidationStatus.PASSED
        assert "0.00" in message

    def test_balance_check_failed(self) -> None:
        spec = _eval_spec("balance", tolerance=0.01)
        rows = [{"total_debits": 150.00, "total_credits": 100.00, "difference": 50.00}]
        status, _, details = evaluate_result(spec, rows, 1)
        assert status == ValidationStatus.FAILED
        assert details["difference"] == 50.0

    def test_balance_unrecognizable_columns_is_error_not_failed(self) -> None:
        status, message, _ = evaluate_result(_eval_spec("balance"), [{"a": 1, "b": 2}], 1)
        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message
        assert "a" in message

    def test_balance_zero_rows_is_error(self) -> None:
        status, message, _ = evaluate_result(_eval_spec("balance"), [], 0)
        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message

    def test_constraint_no_violations_is_passed(self) -> None:
        status, message, _ = evaluate_result(_eval_spec("constraint"), [], 0)
        assert status == ValidationStatus.PASSED
        assert "No constraint violations" in message

    def test_constraint_with_violations_is_failed(self) -> None:
        rows = [{"id": 1, "violation": "negative amount"}]
        status, message, _ = evaluate_result(_eval_spec("constraint"), rows, 1)
        assert status == ValidationStatus.FAILED
        assert "1 constraint violations" in message

    def test_comparison_equation_holds(self) -> None:
        rows = [{"assets": 1000, "liabilities": 600, "equity": 400, "equation_holds": True}]
        status, message, _ = evaluate_result(_eval_spec("comparison"), rows, 1)
        assert status == ValidationStatus.PASSED
        assert "passed" in message

    def test_comparison_equation_fails(self) -> None:
        rows = [{"assets": 1000, "liabilities": 600, "equity": 300, "equation_holds": False}]
        status, _, _ = evaluate_result(_eval_spec("comparison"), rows, 1)
        assert status == ValidationStatus.FAILED

    def test_comparison_inconclusive_is_error_not_failed(self) -> None:
        # The smoke-proven three_way_match shape (DAT-439): no
        # equation_holds/is_valid/difference column → inconclusive, never FAILED.
        rows = [{"po_count": 5, "invoice_count": 3, "receipt_count": 4}]
        status, message, details = evaluate_result(_eval_spec("comparison"), rows, 1)
        assert status == ValidationStatus.ERROR
        assert "Comparison check inconclusive" in message
        assert "could not identify comparison columns" in message
        assert details["check_type"] == "comparison"

    def test_comparison_zero_rows_is_error(self) -> None:
        status, message, _ = evaluate_result(_eval_spec("comparison"), [], 0)
        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message

    def test_aggregate_no_rate_stays_passed(self) -> None:
        # DAT-439 pin: aggregate without a rate metric stays PASSED — the prompt
        # contract is "summary values for review"; the rate judgement is opportunistic.
        rows = [{"min_date": "2024-01-01", "max_date": "2024-12-31", "total_records": 1000}]
        status, message, _ = evaluate_result(_eval_spec("aggregate"), rows, 1)
        assert status == ValidationStatus.PASSED
        assert "Aggregate check completed" in message

    def test_aggregate_rate_above_tolerance_fails(self) -> None:
        rows = [{"orphan_rate": 0.5, "total": 100}]
        status, _, _ = evaluate_result(_eval_spec("aggregate", tolerance=0.01), rows, 1)
        assert status == ValidationStatus.FAILED

    def test_aggregate_zero_rows_is_error(self) -> None:
        status, message, _ = evaluate_result(_eval_spec("aggregate"), [], 0)
        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message

    def test_unknown_check_type_is_error(self) -> None:
        status, message, details = evaluate_result(_eval_spec("referential"), [{"x": 1}], 1)
        assert status == ValidationStatus.ERROR
        assert "Cannot evaluate check_type 'referential'" in message
        assert details["row_count"] == 1


class TestEvaluateValidation:
    """The on-demand wrapper: re-run sql_used, then judge (DAT-617).

    Uses literal SELECTs as ``sql_used`` so the verdict is exercised against a
    real DuckDB connection without any table setup — the same judgement the
    execute phase applies, now reproduced from the stored SQL alone.
    """

    def test_reruns_sql_and_passes(self, duckdb_conn) -> None:
        sql = "SELECT 150.0 AS total_debits, 150.0 AS total_credits, 0.0 AS difference"
        verdict = evaluate_validation(duckdb_conn, sql, _eval_spec("balance", tolerance=0.01))
        assert isinstance(verdict, ValidationVerdict)
        assert verdict.status == ValidationStatus.PASSED
        assert verdict.passed is True
        # magnitude is promoted into flat details for the entropy scorer.
        assert "magnitude" in verdict.details

    def test_reruns_sql_and_fails(self, duckdb_conn) -> None:
        sql = "SELECT 150.0 AS total_debits, 100.0 AS total_credits, 50.0 AS difference"
        verdict = evaluate_validation(duckdb_conn, sql, _eval_spec("balance", tolerance=0.01))
        assert verdict.status == ValidationStatus.FAILED
        assert verdict.passed is False
        assert verdict.details["difference"] == 50.0

    def test_no_sql_is_error(self, duckdb_conn) -> None:
        # Unbound (skipped / generation error): no data verdict to recompute.
        verdict = evaluate_validation(duckdb_conn, None, _eval_spec("balance"))
        assert verdict.status == ValidationStatus.ERROR
        assert verdict.passed is False
        assert "No SQL bound" in verdict.message

    def test_broken_sql_is_error_not_failed(self, duckdb_conn) -> None:
        # A query that no longer plans against current data is ignorance, never
        # a measured data failure — ERROR, mirroring execute_validation.
        verdict = evaluate_validation(
            duckdb_conn, "SELECT * FROM _no_such_table_xyz", _eval_spec("constraint")
        )
        assert verdict.status == ValidationStatus.ERROR
        assert verdict.passed is False
        assert "SQL execution error" in verdict.message

    def test_inconclusive_shape_is_error(self, duckdb_conn) -> None:
        # SQL runs but the balance result has no judgeable columns → inconclusive.
        sql = "SELECT 5 AS po_count, 3 AS invoice_count"
        verdict = evaluate_validation(duckdb_conn, sql, _eval_spec("balance"))
        assert verdict.status == ValidationStatus.ERROR
        assert verdict.passed is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
