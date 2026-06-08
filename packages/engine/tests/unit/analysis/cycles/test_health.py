"""Tests for cycle health scoring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dataraum.analysis.cycles.health import (
    HealthReport,
    compute_cycle_health,
)


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
    passed: bool = True,
    status: str | None = None,
) -> MagicMock:
    vr = MagicMock()
    vr.validation_id = validation_id
    vr.table_ids = table_ids or ["tid-invoices"]
    vr.passed = passed
    vr.status = status if status is not None else ("passed" if passed else "failed")
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


class TestComputeCycleHealth:
    """Tests for compute_cycle_health."""

    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_composite_score_both_signals(self, mock_get_specs: MagicMock) -> None:
        """Cycle with completion_rate=0.8 and validation_pass_rate=1.0 → composite=0.88."""
        mock_get_specs.return_value = [
            _make_validation_spec("double_entry_balance"),
            _make_validation_spec("sign_conventions"),
        ]

        cycle = _make_cycle(completion_rate=0.8)
        vr1 = _make_validation_result("double_entry_balance", ["tid-invoices"], passed=True)
        vr2 = _make_validation_result("sign_conventions", ["tid-payments"], passed=True)

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),  # cycles query
            MagicMock(all=MagicMock(return_value=[vr1, vr2])),  # validation query
            MagicMock(all=MagicMock(return_value=_DEFAULT_TABLES)),  # id→name query
        ]

        report = compute_cycle_health(session, "sess1", vertical="finance", run_id="run-om")

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

    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_composite_score_completion_only(self, mock_get_specs: MagicMock) -> None:
        """No matching validation results → falls back to completion_rate."""
        mock_get_specs.return_value = [_make_validation_spec("double_entry_balance")]

        cycle = _make_cycle(completion_rate=0.75)
        # Validation result exists but for a table the cycle doesn't span:
        # its id resolves to the name "ledger", absent from tables_involved.
        vr = _make_validation_result("double_entry_balance", ["tid-ledger"], passed=True)

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[vr])),
            MagicMock(all=MagicMock(return_value=[_make_table("tid-ledger", "ledger")])),
        ]

        report = compute_cycle_health(session, "sess1", vertical="finance", run_id="run-om")

        score = report.cycle_scores[0]
        assert score.validation_pass_rate is None
        assert score.validations_run == 0
        assert score.composite_score == pytest.approx(0.75)

    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_composite_score_validation_only(self, mock_get_specs: MagicMock) -> None:
        """No completion_rate → falls back to validation_pass_rate."""
        mock_get_specs.return_value = [_make_validation_spec("double_entry_balance")]

        cycle = _make_cycle(completion_rate=None)
        vr = _make_validation_result("double_entry_balance", ["tid-invoices"], passed=True)

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[vr])),
            MagicMock(all=MagicMock(return_value=_DEFAULT_TABLES)),
        ]

        report = compute_cycle_health(session, "sess1", vertical="finance", run_id="run-om")

        score = report.cycle_scores[0]
        assert score.completion_rate is None
        assert score.validation_pass_rate == 1.0
        assert score.composite_score == pytest.approx(1.0)

    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_unjudged_results_stay_out_of_the_pass_rate(self, mock_get_specs: MagicMock) -> None:
        """DAT-439: error = inconclusive and skipped = never executed.

        Neither is an assessment of the data — they leave the pass-rate
        numerator AND denominator, instead of silently deflating cycle
        health as passed=False rows.
        """
        mock_get_specs.return_value = [
            _make_validation_spec("double_entry_balance"),
            _make_validation_spec("three_way_match"),
            _make_validation_spec("sign_conventions"),
        ]

        cycle = _make_cycle(completion_rate=None)
        judged = _make_validation_result("double_entry_balance", ["tid-invoices"], passed=True)
        inconclusive = _make_validation_result(
            "three_way_match", ["tid-invoices"], passed=False, status="error"
        )
        never_ran = _make_validation_result(
            "sign_conventions", ["tid-invoices"], passed=False, status="skipped"
        )

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[judged, inconclusive, never_ran])),
            MagicMock(all=MagicMock(return_value=_DEFAULT_TABLES)),
        ]

        report = compute_cycle_health(session, "sess1", vertical="finance", run_id="run-om")

        score = report.cycle_scores[0]
        assert score.validations_run == 1  # only the judged measurement
        assert score.validations_passed == 1
        assert score.validation_pass_rate == 1.0  # NOT 1/3

    def test_no_run_reads_nothing(self) -> None:
        """Fail-closed (DAT-455): run_id=None issues NO read at all.

        Both the detected cycles AND the validation results are run-versioned;
        without a promoted operating_model run there is no current state — the
        report is empty, never a cross-run read that would mix superseded runs.
        """
        session = MagicMock()

        report = compute_cycle_health(session, "sess1", vertical="finance", run_id=None)

        assert report.cycle_scores == []
        assert report.overall_health is None
        assert session.scalars.call_count == 0  # no query was ever issued

    def test_no_cycles_returns_empty(self) -> None:
        """Session run with no detected cycles → empty report."""
        session = MagicMock()
        session.scalars.return_value = MagicMock(all=MagicMock(return_value=[]))

        report = compute_cycle_health(session, "sess_empty", vertical="finance", run_id="run-om")

        assert isinstance(report, HealthReport)
        assert report.session_id == "sess_empty"
        assert report.cycle_scores == []
        assert report.overall_health is None
