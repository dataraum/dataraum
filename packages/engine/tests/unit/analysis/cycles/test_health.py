"""Tests for cycle health scoring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dataraum.analysis.cycles.health import (
    HealthReport,
    compute_cycle_health,
)
from dataraum.analysis.validation.evaluate import ValidationVerdict
from dataraum.analysis.validation.models import ValidationStatus


# NOTE on namespaces (DAT-455): a cycle's ``tables_involved`` holds table NAMES
# (the LLM's table_name form), while a validation result's ``table_ids`` holds
# table UUIDs. health.py resolves the result ids → names via a Table lookup and
# matches in NAME space. These fixtures keep names and ids DISTINCT (``invoices``
# vs ``tid-invoices``) so a regression back to comparing names against ids would
# stop matching and fail the test — the previous same-string fixtures masked it.
def _make_cycle(
    cycle_id: str = "c1",
    cycle_name: str = "Journal Entry Cycle",
    canonical_type: str | None = "journal_entry_cycle",
    completion_rate: float | None = 0.8,
    tables_involved: list[str] | None = None,
) -> MagicMock:
    cycle = MagicMock()
    cycle.cycle_id = cycle_id
    cycle.cycle_name = cycle_name
    cycle.canonical_type = canonical_type
    cycle.completion_rate = completion_rate
    cycle.tables_involved = tables_involved or ["invoices", "payments"]
    return cycle


def _make_validation_result(
    validation_id: str = "double_entry_balance",
    table_ids: list[str] | None = None,
    sql_used: str | None = "SELECT 1",
) -> MagicMock:
    # DAT-617: the verdict is no longer read off the row — health re-runs
    # ``sql_used`` on demand. A row carries only its sql_used (None = unbound:
    # skipped / generation error, never re-run) plus its table_ids.
    vr = MagicMock()
    vr.validation_id = validation_id
    vr.table_ids = table_ids or ["tid-invoices"]
    vr.sql_used = sql_used
    return vr


def _make_table(table_id: str, table_name: str) -> MagicMock:
    table = MagicMock()
    table.table_id = table_id
    table.table_name = table_name
    return table


# The default id→name resolution the health query performs (3rd scalars call).
_DEFAULT_TABLES = [
    _make_table("tid-invoices", "invoices"),
    _make_table("tid-payments", "payments"),
]


def _make_validation_spec(validation_id: str) -> MagicMock:
    spec = MagicMock()
    spec.validation_id = validation_id
    return spec


def _verdict(passed: bool, status: ValidationStatus | None = None) -> ValidationVerdict:
    """A recomputed verdict the patched ``evaluate_validation`` returns."""
    st = status or (ValidationStatus.PASSED if passed else ValidationStatus.FAILED)
    return ValidationVerdict(
        status=st, passed=(st == ValidationStatus.PASSED), message="", details={}
    )


def _by_id(verdicts: dict[str, ValidationVerdict]):
    """side_effect: map evaluate_validation(conn, sql, spec) → verdict by spec id."""
    return lambda _conn, _sql, spec: verdicts[spec.validation_id]


class TestComputeCycleHealth:
    """Tests for compute_cycle_health."""

    @patch("dataraum.analysis.cycles.health.evaluate_validation")
    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_composite_score_both_signals(
        self, mock_get_specs: MagicMock, mock_eval: MagicMock
    ) -> None:
        """Cycle with completion_rate=0.8 and validation_pass_rate=1.0 → composite=0.88."""
        mock_get_specs.return_value = [
            _make_validation_spec("double_entry_balance"),
            _make_validation_spec("sign_conventions"),
        ]
        mock_eval.side_effect = _by_id(
            {
                "double_entry_balance": _verdict(passed=True),
                "sign_conventions": _verdict(passed=True),
            }
        )

        cycle = _make_cycle(completion_rate=0.8)
        vr1 = _make_validation_result("double_entry_balance", ["tid-invoices"])
        vr2 = _make_validation_result("sign_conventions", ["tid-payments"])

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),  # cycles query
            MagicMock(all=MagicMock(return_value=[vr1, vr2])),  # validation query
            MagicMock(all=MagicMock(return_value=_DEFAULT_TABLES)),  # id→name query
        ]

        report = compute_cycle_health(
            session, duckdb_conn=MagicMock(), vertical="finance", run_id="run-om"
        )

        assert len(report.cycle_scores) == 1
        score = report.cycle_scores[0]
        assert score.completion_rate == 0.8
        # Crosses the name/id namespace: matched only because table_ids resolved
        # to names that the cycle's tables_involved carry (the masked-bug guard).
        assert score.validation_pass_rate == 1.0
        assert score.validations_run == 2
        assert score.validations_passed == 2
        assert score.composite_score == pytest.approx(0.88)
        assert report.overall_health == pytest.approx(0.88)

    @patch("dataraum.analysis.cycles.health.evaluate_validation")
    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_composite_score_completion_only(
        self, mock_get_specs: MagicMock, mock_eval: MagicMock
    ) -> None:
        """No matching validation results → falls back to completion_rate."""
        mock_get_specs.return_value = [_make_validation_spec("double_entry_balance")]

        cycle = _make_cycle(completion_rate=0.75)
        # Validation result exists but for a table the cycle doesn't span:
        # its id resolves to the name "ledger", absent from tables_involved.
        vr = _make_validation_result("double_entry_balance", ["tid-ledger"])

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[vr])),
            MagicMock(all=MagicMock(return_value=[_make_table("tid-ledger", "ledger")])),
        ]

        report = compute_cycle_health(
            session, duckdb_conn=MagicMock(), vertical="finance", run_id="run-om"
        )

        score = report.cycle_scores[0]
        assert score.validation_pass_rate is None
        assert score.validations_run == 0
        assert score.composite_score == pytest.approx(0.75)
        # No matched check → the SQL is never re-run.
        mock_eval.assert_not_called()

    @patch("dataraum.analysis.cycles.health.evaluate_validation")
    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_composite_score_validation_only(
        self, mock_get_specs: MagicMock, mock_eval: MagicMock
    ) -> None:
        """No completion_rate → falls back to validation_pass_rate."""
        mock_get_specs.return_value = [_make_validation_spec("double_entry_balance")]
        mock_eval.side_effect = _by_id({"double_entry_balance": _verdict(passed=True)})

        cycle = _make_cycle(completion_rate=None)
        vr = _make_validation_result("double_entry_balance", ["tid-invoices"])

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[vr])),
            MagicMock(all=MagicMock(return_value=_DEFAULT_TABLES)),
        ]

        report = compute_cycle_health(
            session, duckdb_conn=MagicMock(), vertical="finance", run_id="run-om"
        )

        score = report.cycle_scores[0]
        assert score.completion_rate is None
        assert score.validation_pass_rate == 1.0
        assert score.composite_score == pytest.approx(1.0)

    @patch("dataraum.analysis.cycles.health.evaluate_validation")
    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_unjudged_results_stay_out_of_the_pass_rate(
        self, mock_get_specs: MagicMock, mock_eval: MagicMock
    ) -> None:
        """DAT-439/DAT-617: an inconclusive re-run (ERROR) or an unbound check
        (no sql_used) is ignorance, not an assessment.

        Neither lands in the pass-rate numerator OR denominator, instead of
        silently deflating cycle health.
        """
        mock_get_specs.return_value = [
            _make_validation_spec("double_entry_balance"),
            _make_validation_spec("three_way_match"),
            _make_validation_spec("sign_conventions"),
        ]
        # double_entry_balance re-runs PASSED; three_way_match re-runs ERROR
        # (inconclusive); sign_conventions is unbound (sql_used=None) so it is
        # never re-run at all.
        mock_eval.side_effect = _by_id(
            {
                "double_entry_balance": _verdict(passed=True),
                "three_way_match": _verdict(passed=False, status=ValidationStatus.ERROR),
            }
        )

        cycle = _make_cycle(completion_rate=None)
        judged = _make_validation_result("double_entry_balance", ["tid-invoices"])
        inconclusive = _make_validation_result("three_way_match", ["tid-invoices"])
        never_ran = _make_validation_result("sign_conventions", ["tid-invoices"], sql_used=None)

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[judged, inconclusive, never_ran])),
            MagicMock(all=MagicMock(return_value=_DEFAULT_TABLES)),
        ]

        report = compute_cycle_health(
            session, duckdb_conn=MagicMock(), vertical="finance", run_id="run-om"
        )

        score = report.cycle_scores[0]
        assert score.validations_run == 1  # only the judged measurement
        assert score.validations_passed == 1
        assert score.validation_pass_rate == 1.0  # NOT 1/3

    @patch("dataraum.analysis.cycles.health.evaluate_validation")
    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_no_connection_skips_pass_rate(
        self, mock_get_specs: MagicMock, mock_eval: MagicMock
    ) -> None:
        """No duckdb_conn ⇒ no recompute: pass rate absent, SQL never re-run."""
        mock_get_specs.return_value = [_make_validation_spec("double_entry_balance")]

        cycle = _make_cycle(completion_rate=0.6)
        vr = _make_validation_result("double_entry_balance", ["tid-invoices"])

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[vr])),
            MagicMock(all=MagicMock(return_value=_DEFAULT_TABLES)),
        ]

        report = compute_cycle_health(
            session, duckdb_conn=None, vertical="finance", run_id="run-om"
        )

        score = report.cycle_scores[0]
        assert score.validation_pass_rate is None
        assert score.validations_run == 0
        assert score.composite_score == pytest.approx(0.6)
        mock_eval.assert_not_called()

    def test_no_run_reads_nothing(self) -> None:
        """Fail-closed (DAT-455): run_id=None issues NO read at all.

        Both the detected cycles AND the validation results are run-versioned;
        without a promoted operating_model run there is no current state — the
        report is empty, never a cross-run read that would mix superseded runs.
        """
        session = MagicMock()

        report = compute_cycle_health(
            session, duckdb_conn=MagicMock(), vertical="finance", run_id=None
        )

        assert report.cycle_scores == []
        assert report.overall_health is None
        assert session.scalars.call_count == 0  # no query was ever issued

    def test_no_cycles_returns_empty(self) -> None:
        """Run with no detected cycles → empty report."""
        session = MagicMock()
        session.scalars.return_value = MagicMock(all=MagicMock(return_value=[]))

        report = compute_cycle_health(
            session, duckdb_conn=MagicMock(), vertical="finance", run_id="run-om"
        )

        assert isinstance(report, HealthReport)
        assert report.cycle_scores == []
        assert report.overall_health is None
