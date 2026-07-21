"""Tests for the on-demand validation verdict (ADR-0017 / DAT-617).

``evaluate_result`` is the uniform judgement over a contracted result row
(``deviation``/``magnitude``); ``evaluate_validation`` re-runs a stored
``sql_used`` and applies it. The verdict is computed, never stored-and-read —
a stored pass/fail goes stale on re-import, the SQL does not.

``TestSharedVerdictVectors`` drives the SAME truth table the cockpit's TS mirror
asserts (tests/fixtures/validation_verdict_vectors.json) — the guardrail that
the two judgement copies cannot drift.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from dataraum.analysis.validation.evaluate import (
    ValidationVerdict,
    evaluate_result,
    evaluate_validation,
)
from dataraum.analysis.validation.models import ValidationSpec, ValidationStatus

_VECTORS = json.loads(
    (Path(__file__).parents[3] / "fixtures" / "validation_verdict_vectors.json").read_text()
)["cases"]


def _eval_spec(check_type: str, **parameters) -> ValidationSpec:
    return ValidationSpec(
        validation_id="test",
        name="Test",
        description="Test",
        category="test",
        check_type=check_type,
        parameters=parameters,
    )


class TestSharedVerdictVectors:
    """The single judgement truth table — shared verbatim with the cockpit mirror."""

    @pytest.mark.parametrize("case", _VECTORS, ids=[c["name"] for c in _VECTORS])
    def test_vector(self, case: dict) -> None:
        spec = _eval_spec(case["check_type"], tolerance=case["tolerance"])
        status, _, details = evaluate_result(spec, case["rows"])
        assert status.value == case["expected"]["status"]
        assert (status == ValidationStatus.PASSED) == case["expected"]["passed"]
        # Optional numeric pins (DAT-852): the WORST-row numbers both mirrors
        # must serve — guards the selection, not just the verdict.
        if "deviation" in case["expected"]:
            assert details["deviation"] == pytest.approx(case["expected"]["deviation"])
        if "magnitude" in case["expected"]:
            assert details["magnitude"] == pytest.approx(case["expected"]["magnitude"])


class TestEvaluateResultDetails:
    """The flat details the entropy scorer reads (deviation/magnitude/tolerance)."""

    def test_passed_carries_deviation_and_magnitude(self) -> None:
        spec = _eval_spec("balance", tolerance=0.01)
        status, _, details = evaluate_result(spec, [{"deviation": 0.0, "magnitude": 150.0}])
        assert status == ValidationStatus.PASSED
        assert details["deviation"] == 0.0
        assert details["magnitude"] == 150.0
        assert details["tolerance"] == 0.01

    def test_failed_carries_deviation(self) -> None:
        spec = _eval_spec("balance", tolerance=0.01)
        status, _, details = evaluate_result(spec, [{"deviation": 50.0, "magnitude": 150.0}])
        assert status == ValidationStatus.FAILED
        assert details["deviation"] == 50.0

    def test_absent_magnitude_defaults_to_deviation_then_one(self) -> None:
        # magnitude falls back so the scorer's deviation/magnitude never divides by 0.
        spec = _eval_spec("balance", tolerance=0.01)
        _, _, details = evaluate_result(spec, [{"deviation": 5.0}])
        assert details["magnitude"] == 5.0
        _, _, zero = evaluate_result(spec, [{"deviation": 0.0}])
        assert zero["magnitude"] == 1.0

    def test_missing_deviation_is_inconclusive_not_failed(self) -> None:
        spec = _eval_spec("comparison")
        status, message, _ = evaluate_result(spec, [{"po_count": 5}])
        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message
        assert "deviation" in message

    def test_multi_leg_flat_details_are_the_worst_leg(self) -> None:
        """DAT-852: the entropy scorer's flat contract is the WORST leg's numbers,
        and the full per-leg breakdown rides ``legs`` — never a pooled number."""
        spec = _eval_spec("aggregate", tolerance=0.02)
        rows = [
            {"leg": "txns.account_id->accounts.id", "deviation": 0.0, "magnitude": 1.0},
            {"leg": "txns.vendor_id->vendors.id", "deviation": 0.315, "magnitude": 1.0},
        ]
        status, message, details = evaluate_result(spec, rows)
        assert status == ValidationStatus.FAILED
        assert details["deviation"] == 0.315
        assert details["magnitude"] == 1.0
        assert details["legs"] == [
            {"leg": "txns.account_id->accounts.id", "deviation": 0.0, "magnitude": 1.0},
            {"leg": "txns.vendor_id->vendors.id", "deviation": 0.315, "magnitude": 1.0},
        ]
        assert "txns.vendor_id->vendors.id" in message
        assert "2 legs judged" in message

    def test_single_row_details_carry_no_legs_breakdown(self) -> None:
        spec = _eval_spec("balance", tolerance=0.01)
        _, message, details = evaluate_result(spec, [{"deviation": 0.0, "magnitude": 1.0}])
        assert "legs" not in details
        assert "leg" not in message


class TestNonFiniteInputs:
    """NaN/inf handling — per-side tests because JSON vectors cannot encode them.

    A NaN deviation is REACHABLE, not theoretical: DuckDB's IEEE division
    returns NaN for an orphan-rate leg over an all-NULL FK column (0.0/0.0).
    Unguarded, a NaN in the worst-row max() made the verdict ORDER-DEPENDENT
    (NaN comparisons are always False) while the TS mirror returned ERROR for
    the same rows — a live divergence the shared truth table cannot see.
    """

    @pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
    def test_non_finite_deviation_is_inconclusive(self, bad: float) -> None:
        spec = _eval_spec("aggregate", tolerance=10.0)
        status, message, _ = evaluate_result(spec, [{"deviation": bad}])
        assert status == ValidationStatus.ERROR
        assert "non-finite" in message

    def test_nan_deviation_is_inconclusive_regardless_of_row_order(self) -> None:
        spec = _eval_spec("aggregate", tolerance=10.0)
        rows_nan_first = [{"deviation": float("nan")}, {"deviation": 5.0}]
        rows_nan_last = [{"deviation": 5.0}, {"deviation": float("nan")}]
        for rows in (rows_nan_first, rows_nan_last):
            status, _, _ = evaluate_result(spec, rows)
            assert status == ValidationStatus.ERROR, "verdict must never depend on row order"

    def test_nan_magnitude_degrades_to_fallback_not_error(self) -> None:
        # magnitude is severity garnish, never verdict-bearing — a NaN
        # magnitude must not reach the entropy scorer's divisor.
        spec = _eval_spec("aggregate", tolerance=10.0)
        status, _, details = evaluate_result(spec, [{"deviation": 5.0, "magnitude": float("nan")}])
        assert status == ValidationStatus.PASSED
        assert details["magnitude"] == 5.0


class TestEvaluateValidation:
    """The on-demand wrapper: re-run sql_used (contracted output), then judge.

    Uses literal SELECTs as ``sql_used`` so the verdict is exercised against a
    real DuckDB connection without table setup.
    """

    def test_reruns_sql_and_passes(self, duckdb_conn) -> None:
        sql = "SELECT 0.0 AS deviation, 150.0 AS magnitude"
        verdict = evaluate_validation(duckdb_conn, sql, _eval_spec("balance", tolerance=0.01))
        assert isinstance(verdict, ValidationVerdict)
        assert verdict.status == ValidationStatus.PASSED
        assert verdict.passed is True
        assert verdict.details["magnitude"] == 150.0

    def test_reruns_sql_and_fails(self, duckdb_conn) -> None:
        sql = "SELECT 50.0 AS deviation, 150.0 AS magnitude"
        verdict = evaluate_validation(duckdb_conn, sql, _eval_spec("balance", tolerance=0.01))
        assert verdict.status == ValidationStatus.FAILED
        assert verdict.passed is False
        assert verdict.details["deviation"] == 50.0

    def test_no_sql_is_error(self, duckdb_conn) -> None:
        verdict = evaluate_validation(duckdb_conn, None, _eval_spec("balance"))
        assert verdict.status == ValidationStatus.ERROR
        assert verdict.passed is False
        assert "No SQL bound" in verdict.message

    def test_broken_sql_is_error_not_failed(self, duckdb_conn) -> None:
        verdict = evaluate_validation(
            duckdb_conn, "SELECT * FROM _no_such_table_xyz", _eval_spec("constraint")
        )
        assert verdict.status == ValidationStatus.ERROR
        assert verdict.passed is False
        assert "SQL execution error" in verdict.message

    def test_uncontracted_output_is_inconclusive(self, duckdb_conn) -> None:
        # SQL runs but ignores the contract (no `deviation` column) → inconclusive.
        sql = "SELECT 5 AS po_count, 3 AS invoice_count"
        verdict = evaluate_validation(duckdb_conn, sql, _eval_spec("balance"))
        assert verdict.status == ValidationStatus.ERROR
        assert verdict.passed is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
