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
        status, _, _ = evaluate_result(spec, case["rows"], len(case["rows"]))
        assert status.value == case["expected"]["status"]
        assert (status == ValidationStatus.PASSED) == case["expected"]["passed"]


class TestEvaluateResultDetails:
    """The flat details the entropy scorer reads (deviation/magnitude/tolerance)."""

    def test_passed_carries_deviation_and_magnitude(self) -> None:
        spec = _eval_spec("balance", tolerance=0.01)
        status, _, details = evaluate_result(spec, [{"deviation": 0.0, "magnitude": 150.0}], 1)
        assert status == ValidationStatus.PASSED
        assert details["deviation"] == 0.0
        assert details["magnitude"] == 150.0
        assert details["tolerance"] == 0.01

    def test_failed_carries_deviation(self) -> None:
        spec = _eval_spec("balance", tolerance=0.01)
        status, _, details = evaluate_result(spec, [{"deviation": 50.0, "magnitude": 150.0}], 1)
        assert status == ValidationStatus.FAILED
        assert details["deviation"] == 50.0

    def test_absent_magnitude_defaults_to_deviation_then_one(self) -> None:
        # magnitude falls back so the scorer's deviation/magnitude never divides by 0.
        spec = _eval_spec("balance", tolerance=0.01)
        _, _, details = evaluate_result(spec, [{"deviation": 5.0}], 1)
        assert details["magnitude"] == 5.0
        _, _, zero = evaluate_result(spec, [{"deviation": 0.0}], 1)
        assert zero["magnitude"] == 1.0

    def test_missing_deviation_is_inconclusive_not_failed(self) -> None:
        spec = _eval_spec("comparison")
        status, message, _ = evaluate_result(spec, [{"po_count": 5}], 1)
        assert status == ValidationStatus.ERROR
        assert "inconclusive" in message
        assert "deviation" in message


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
