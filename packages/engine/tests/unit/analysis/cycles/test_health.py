"""Tests for cycle health scoring."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from dataraum.analysis.cycles.health import (
    HealthReport,
    compute_cycle_health,
)


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
    cycle.tables_involved = tables_involved or ["t1", "t2"]
    return cycle


def _make_validation_result(
    validation_id: str = "double_entry_balance",
    table_ids: list[str] | None = None,
    passed: bool = True,
) -> MagicMock:
    vr = MagicMock()
    vr.validation_id = validation_id
    vr.table_ids = table_ids or ["t1"]
    vr.passed = passed
    return vr


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
        vr1 = _make_validation_result("double_entry_balance", ["t1"], passed=True)
        vr2 = _make_validation_result("sign_conventions", ["t2"], passed=True)

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),  # cycles query
            MagicMock(all=MagicMock(return_value=[vr1, vr2])),  # validation query
        ]

        report = compute_cycle_health(session, "src1", vertical="finance")

        assert len(report.cycle_scores) == 1
        score = report.cycle_scores[0]
        assert score.completion_rate == 0.8
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
        # Validation result exists but for a different table
        vr = _make_validation_result("double_entry_balance", ["other_table"], passed=True)

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[vr])),
        ]

        report = compute_cycle_health(session, "src1", vertical="finance")

        score = report.cycle_scores[0]
        assert score.validation_pass_rate is None
        assert score.validations_run == 0
        assert score.composite_score == pytest.approx(0.75)

    @patch("dataraum.analysis.cycles.health.get_validation_specs_for_cycles")
    def test_composite_score_validation_only(self, mock_get_specs: MagicMock) -> None:
        """No completion_rate → falls back to validation_pass_rate."""
        mock_get_specs.return_value = [_make_validation_spec("double_entry_balance")]

        cycle = _make_cycle(completion_rate=None)
        vr = _make_validation_result("double_entry_balance", ["t1"], passed=True)

        session = MagicMock()
        session.scalars.side_effect = [
            MagicMock(all=MagicMock(return_value=[cycle])),
            MagicMock(all=MagicMock(return_value=[vr])),
        ]

        report = compute_cycle_health(session, "src1", vertical="finance")

        score = report.cycle_scores[0]
        assert score.completion_rate is None
        assert score.validation_pass_rate == 1.0
        assert score.composite_score == pytest.approx(1.0)

    def test_no_cycles_returns_empty(self) -> None:
        """Source with no detected cycles → empty report."""
        session = MagicMock()
        session.scalars.return_value = MagicMock(all=MagicMock(return_value=[]))

        report = compute_cycle_health(session, "src_empty", vertical="finance")

        assert isinstance(report, HealthReport)
        assert report.source_id == "src_empty"
        assert report.cycle_scores == []
        assert report.overall_health is None
