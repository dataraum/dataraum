"""Tests for Actions screen merge logic."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dataraum.cli.tui.screens.actions import ActionsScreen, MergedAction


@dataclass
class FakeResolutionOption:
    """Minimal stub for ResolutionOption."""

    action: str = "declare_unit"
    parameters: dict[str, Any] = field(default_factory=dict)
    expected_entropy_reduction: float = 0.3
    effort: str = "low"
    description: str = "Add unit declaration"
    cascade_dimensions: list[str] = field(default_factory=list)


@dataclass
class FakeColumnSummary:
    """Minimal stub for ColumnSummary."""

    column_id: str = "col1"
    column_name: str = "amount"
    table_id: str = "t1"
    table_name: str = "orders"
    composite_score: float = 0.25
    readiness: str = "investigate"
    top_resolution_hints: list[Any] = field(default_factory=list)
    compound_risks: list[Any] = field(default_factory=list)


@dataclass
class FakeInterp:
    """Minimal stub for EntropyInterpretationRecord."""

    table_name: str = "orders"
    column_name: str = "amount"
    resolution_actions_json: list[dict[str, Any]] | None = None


@dataclass
class FakeEntropyObject:
    """Minimal stub for EntropyObjectRecord."""

    target: str = "column:orders.amount"
    layer: str = "semantic"
    dimension: str = "units"
    sub_dimension: str = "unit_ambiguity"
    score: float = 0.4
    confidence: float = 0.9
    evidence: dict[str, Any] | None = None
    resolution_options: list[dict[str, Any]] | None = None


class TestMergeActions:
    """Tests for ActionsScreen._merge_actions()."""

    def _make_screen(self) -> ActionsScreen:
        from pathlib import Path

        return ActionsScreen(Path("/tmp/fake"))

    def test_empty_inputs(self):
        screen = self._make_screen()
        result = screen._merge_actions(
            column_summaries={},
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
        )
        assert result == []

    def test_detector_hints_create_actions(self):
        screen = self._make_screen()
        hint = FakeResolutionOption(action="declare_unit", expected_entropy_reduction=0.3)
        summary = FakeColumnSummary(top_resolution_hints=[hint])

        result = screen._merge_actions(
            column_summaries={"orders.amount": summary},
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
        )

        assert len(result) == 1
        assert result[0].action == "declare_unit"
        assert result[0].from_detector is True
        assert "orders.amount" in result[0].affected_columns

    def test_llm_actions_create_actions(self):
        screen = self._make_screen()
        interp = FakeInterp(
            resolution_actions_json=[
                {
                    "action": "add_definition",
                    "description": "Add business definition",
                    "priority": "high",
                    "effort": "low",
                }
            ]
        )

        result = screen._merge_actions(
            column_summaries={},
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={},
        )

        assert len(result) == 1
        assert result[0].action == "add_definition"
        assert result[0].from_llm is True
        assert result[0].priority == "high"

    def test_merge_deduplicates_by_action_name(self):
        screen = self._make_screen()
        hint = FakeResolutionOption(action="declare_unit", expected_entropy_reduction=0.3)
        summary = FakeColumnSummary(top_resolution_hints=[hint])

        interp = FakeInterp(
            resolution_actions_json=[
                {
                    "action": "declare_unit",
                    "description": "Declare unit for column",
                    "priority": "high",
                    "effort": "low",
                }
            ]
        )

        result = screen._merge_actions(
            column_summaries={"orders.amount": summary},
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={},
        )

        assert len(result) == 1
        merged = result[0]
        assert merged.from_detector is True
        assert merged.from_llm is True
        assert merged.priority == "high"  # LLM priority takes precedence

    def test_multiple_columns_same_action(self):
        screen = self._make_screen()
        hint1 = FakeResolutionOption(action="declare_unit", expected_entropy_reduction=0.3)
        hint2 = FakeResolutionOption(action="declare_unit", expected_entropy_reduction=0.2)

        result = screen._merge_actions(
            column_summaries={
                "orders.amount": FakeColumnSummary(top_resolution_hints=[hint1]),
                "orders.price": FakeColumnSummary(
                    column_name="price", top_resolution_hints=[hint2]
                ),
            },
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
        )

        assert len(result) == 1
        assert len(result[0].affected_columns) == 2
        assert result[0].max_reduction == 0.3
        assert result[0].total_reduction == 0.5

    def test_sorted_by_priority_then_score(self):
        screen = self._make_screen()

        interp1 = FakeInterp(
            table_name="orders",
            column_name="amount",
            resolution_actions_json=[
                {"action": "low_action", "priority": "low", "effort": "low"},
            ],
        )
        interp2 = FakeInterp(
            table_name="orders",
            column_name="price",
            resolution_actions_json=[
                {"action": "high_action", "priority": "high", "effort": "low"},
            ],
        )

        result = screen._merge_actions(
            column_summaries={},
            interp_by_col={
                "orders.amount": interp1,
                "orders.price": interp2,
            },
            entropy_objects_by_col={},
            violation_dims={},
        )

        assert result[0].action == "high_action"
        assert result[1].action == "low_action"

    def test_violation_dims_mapped_to_actions(self):
        screen = self._make_screen()
        hint = FakeResolutionOption(action="declare_unit", expected_entropy_reduction=0.3)
        summary = FakeColumnSummary(top_resolution_hints=[hint])

        result = screen._merge_actions(
            column_summaries={"orders.amount": summary},
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={"semantic.units": ["orders.amount"]},
        )

        assert len(result) == 1
        assert "semantic.units" in result[0].fixes_violations


class TestMergedAction:
    """Tests for MergedAction dataclass."""

    def test_defaults(self):
        action = MergedAction(action="test")
        assert action.priority == "medium"
        assert action.effort == "medium"
        assert action.affected_columns == []
        assert action.from_llm is False
        assert action.from_detector is False
