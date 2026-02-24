"""Tests for Actions screen merge logic."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from dataraum.cli.tui.screens.actions import ActionsScreen, MergedAction


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
            interp_by_col={},
            entropy_objects_by_col={},
            violation_dims={},
        )
        assert result == []

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
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={},
        )

        assert len(result) == 1
        assert result[0].action == "add_definition"
        assert result[0].from_llm is True
        # Priority is now score-derived, not from LLM JSON
        # score = (0.0 + 1*0.1) / 1.0 = 0.1 -> low
        assert result[0].priority == "low"

    def test_sorted_by_priority_score_descending(self):
        """Actions sorted by score: more affected columns = higher score."""
        screen = self._make_screen()

        # low_action affects 1 column
        interp1 = FakeInterp(
            table_name="orders",
            column_name="amount",
            resolution_actions_json=[
                {"action": "low_action", "effort": "high"},
            ],
        )
        # high_action affects 2 columns (appears in both interps)
        interp2 = FakeInterp(
            table_name="orders",
            column_name="price",
            resolution_actions_json=[
                {"action": "high_action", "effort": "low"},
            ],
        )
        interp3 = FakeInterp(
            table_name="orders",
            column_name="qty",
            resolution_actions_json=[
                {"action": "high_action", "effort": "low"},
            ],
        )

        result = screen._merge_actions(
            interp_by_col={
                "orders.amount": interp1,
                "orders.price": interp2,
                "orders.qty": interp3,
            },
            entropy_objects_by_col={},
            violation_dims={},
        )

        # high_action: (0.0 + 2*0.1) / 1.0 = 0.20
        # low_action: (0.0 + 1*0.1) / 4.0 = 0.025
        assert result[0].action == "high_action"
        assert result[1].action == "low_action"

    def test_violation_dims_mapped_to_actions(self):
        screen = self._make_screen()
        interp = FakeInterp(
            resolution_actions_json=[
                {
                    "action": "declare_unit",
                    "description": "Add unit declaration",
                    "effort": "low",
                }
            ]
        )

        result = screen._merge_actions(
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={},
            violation_dims={"semantic.units": ["orders.amount"]},
        )

        assert len(result) == 1
        assert "semantic.units" in result[0].fixes_violations

    def test_llm_actions_attach_entropy_objects(self):
        """Entropy objects for a column are attached to LLM actions."""
        screen = self._make_screen()
        interp = FakeInterp(
            resolution_actions_json=[
                {"action": "resolve_ambiguity", "effort": "medium"}
            ]
        )
        eo = FakeEntropyObject(target="column:orders.amount")

        result = screen._merge_actions(
            interp_by_col={"orders.amount": interp},
            entropy_objects_by_col={"orders.amount": [eo]},
            violation_dims={},
        )

        assert len(result) == 1
        assert eo in result[0].related_objects

    def test_multiple_columns_same_llm_action(self):
        """Same action from LLM interps on different columns is deduplicated."""
        screen = self._make_screen()
        interp1 = FakeInterp(
            table_name="orders",
            column_name="amount",
            resolution_actions_json=[
                {"action": "standardize_format", "effort": "low"}
            ],
        )
        interp2 = FakeInterp(
            table_name="orders",
            column_name="price",
            resolution_actions_json=[
                {"action": "standardize_format", "effort": "low"}
            ],
        )

        result = screen._merge_actions(
            interp_by_col={
                "orders.amount": interp1,
                "orders.price": interp2,
            },
            entropy_objects_by_col={},
            violation_dims={},
        )

        assert len(result) == 1
        assert set(result[0].affected_columns) == {"orders.amount", "orders.price"}


class TestMergedAction:
    """Tests for MergedAction dataclass."""

    def test_defaults(self):
        action = MergedAction(action="test")
        assert action.priority == "medium"
        assert action.effort == "medium"
        assert action.affected_columns == []
        assert action.from_llm is False
