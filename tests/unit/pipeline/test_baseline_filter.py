"""Tests for _is_interesting baseline filter in entropy interpretation phase."""

import pytest

from dataraum.entropy.interpretation import InterpretationInput
from dataraum.pipeline.phases.entropy_interpretation_phase import _is_interesting


def _make_input(**overrides: object) -> InterpretationInput:
    defaults = {
        "table_name": "t",
        "column_name": "c",
        "detected_type": "varchar",
        "business_description": None,
        "quality_grade": None,
        "quality_findings": None,
        "network_analysis": None,
    }
    defaults.update(overrides)
    return InterpretationInput(**defaults)  # type: ignore[arg-type]


class TestIsInteresting:
    """Test the baseline filtering logic with default thresholds."""

    def test_baseline_column_no_signals(self) -> None:
        inp = _make_input()
        assert not _is_interesting(inp)

    def test_baseline_with_low_p_high(self) -> None:
        inp = _make_input(
            network_analysis={
                "high_impact_nodes": [],
                "intents": {"aggregation": {"p_high": 0.30, "readiness": "ready"}},
            }
        )
        assert not _is_interesting(inp)

    def test_interesting_high_impact_nodes(self) -> None:
        inp = _make_input(
            network_analysis={
                "high_impact_nodes": [{"node": "n", "state": "medium", "impact_delta": 0.1}],
                "intents": {"aggregation": {"p_high": 0.30, "readiness": "ready"}},
            }
        )
        assert _is_interesting(inp)

    def test_interesting_high_p_high(self) -> None:
        inp = _make_input(
            network_analysis={
                "high_impact_nodes": [],
                "intents": {"aggregation": {"p_high": 0.50, "readiness": "investigate"}},
            }
        )
        assert _is_interesting(inp)

    def test_interesting_at_threshold_boundary(self) -> None:
        """P(high) exactly at 0.35 is NOT interesting (must exceed)."""
        inp = _make_input(
            network_analysis={
                "high_impact_nodes": [],
                "intents": {"aggregation": {"p_high": 0.35, "readiness": "ready"}},
            }
        )
        assert not _is_interesting(inp)

    def test_interesting_just_above_threshold(self) -> None:
        inp = _make_input(
            network_analysis={
                "high_impact_nodes": [],
                "intents": {"aggregation": {"p_high": 0.36, "readiness": "ready"}},
            }
        )
        assert _is_interesting(inp)

    @pytest.mark.parametrize("grade", ["C", "D", "F"])
    def test_interesting_poor_quality_grade(self, grade: str) -> None:
        inp = _make_input(quality_grade=grade)
        assert _is_interesting(inp)

    @pytest.mark.parametrize("grade", ["A", "B"])
    def test_baseline_good_quality_grade(self, grade: str) -> None:
        inp = _make_input(quality_grade=grade)
        assert not _is_interesting(inp)

    def test_interesting_quality_findings(self) -> None:
        inp = _make_input(quality_findings=["missing values detected"])
        assert _is_interesting(inp)

    def test_baseline_empty_quality_findings(self) -> None:
        inp = _make_input(quality_findings=[])
        assert not _is_interesting(inp)

    def test_interesting_multiple_intents_one_high(self) -> None:
        """If any intent exceeds threshold, column is interesting."""
        inp = _make_input(
            network_analysis={
                "high_impact_nodes": [],
                "intents": {
                    "aggregation": {"p_high": 0.30, "readiness": "ready"},
                    "trend_analysis": {"p_high": 0.50, "readiness": "investigate"},
                },
            }
        )
        assert _is_interesting(inp)

    def test_no_network_analysis_no_quality(self) -> None:
        """Column with no network analysis and no quality signals is baseline."""
        inp = _make_input(network_analysis=None, quality_grade=None, quality_findings=None)
        assert not _is_interesting(inp)


class TestIsInterestingConfigurable:
    """Test that p_high_threshold and poor_quality_grades are configurable."""

    def test_custom_lower_threshold_catches_more(self) -> None:
        """A lower threshold makes more columns interesting."""
        inp = _make_input(
            network_analysis={
                "high_impact_nodes": [],
                "intents": {"aggregation": {"p_high": 0.25, "readiness": "ready"}},
            }
        )
        assert not _is_interesting(inp)  # default 0.35 -> baseline
        assert _is_interesting(inp, p_high_threshold=0.20)  # 0.20 -> interesting

    def test_custom_higher_threshold_filters_more(self) -> None:
        """A higher threshold makes more columns baseline."""
        inp = _make_input(
            network_analysis={
                "high_impact_nodes": [],
                "intents": {"aggregation": {"p_high": 0.40, "readiness": "investigate"}},
            }
        )
        assert _is_interesting(inp)  # default 0.35 -> interesting
        assert not _is_interesting(inp, p_high_threshold=0.50)  # 0.50 -> baseline

    def test_custom_quality_grades(self) -> None:
        """Only grades in the configured set trigger LLM."""
        inp = _make_input(quality_grade="C")
        assert _is_interesting(inp)  # default includes C
        assert not _is_interesting(inp, poor_quality_grades={"D", "F"})  # C excluded

    def test_custom_quality_grades_add_b(self) -> None:
        """Adding B to poor grades makes B-grade columns interesting."""
        inp = _make_input(quality_grade="B")
        assert not _is_interesting(inp)  # default excludes B
        assert _is_interesting(inp, poor_quality_grades={"B", "C", "D", "F"})
