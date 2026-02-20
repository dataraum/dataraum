"""Merge resolution actions from multiple sources into a unified, prioritized list.

Used by MCP server and API to produce actionable steps for improving data quality.
"""

from __future__ import annotations

from typing import Any


def merge_actions(
    column_summaries: dict[str, Any],
    interp_by_col: dict[str, Any],
    entropy_objects_by_col: dict[str, list[Any]],
    violation_dims: dict[str, list[str]],
) -> list[dict[str, Any]]:
    """Merge actions from all sources into a unified list.

    Args:
        column_summaries: Column key -> ColumnSummary from EntropyAggregator
        interp_by_col: Column key -> EntropyInterpretationRecord from LLM
        entropy_objects_by_col: Column key -> list of EntropyObjectRecord
        violation_dims: Dimension -> list of affected column keys from contracts

    Returns:
        Sorted list of action dicts with priority, effort, affected columns, etc.
    """
    actions_map: dict[str, dict[str, Any]] = {}

    # From ColumnSummary.top_resolution_hints (detector source)
    for col_key, summary in column_summaries.items():
        for hint in summary.top_resolution_hints:
            if hint.action not in actions_map:
                actions_map[hint.action] = {
                    "action": hint.action,
                    "priority": "medium",
                    "description": hint.description,
                    "effort": hint.effort,
                    "expected_impact": "",
                    "parameters": {},
                    "affected_columns": [],
                    "cascade_dimensions": list(hint.cascade_dimensions),
                    "max_reduction": hint.expected_entropy_reduction,
                    "total_reduction": 0.0,
                    "from_llm": False,
                    "from_detector": True,
                    "fixes_violations": [],
                    "evidence": [],
                }
            ma = actions_map[hint.action]
            if col_key not in ma["affected_columns"]:
                ma["affected_columns"].append(col_key)
            ma["max_reduction"] = max(ma["max_reduction"], hint.expected_entropy_reduction)
            ma["total_reduction"] += hint.expected_entropy_reduction

    # From LLM interpretation resolution_actions_json
    for col_key, interp in interp_by_col.items():
        actions = interp.resolution_actions_json
        if isinstance(actions, dict):
            actions = list(actions.values()) if actions else []
        elif not isinstance(actions, list):
            continue

        for action_dict in actions:
            if not isinstance(action_dict, dict):
                continue

            action_name = action_dict.get("action", "")
            if not action_name:
                continue

            if action_name not in actions_map:
                actions_map[action_name] = {
                    "action": action_name,
                    "priority": "medium",
                    "description": "",
                    "effort": "medium",
                    "expected_impact": "",
                    "parameters": {},
                    "affected_columns": [],
                    "cascade_dimensions": [],
                    "max_reduction": 0.0,
                    "total_reduction": 0.0,
                    "from_llm": True,
                    "from_detector": False,
                    "fixes_violations": [],
                    "evidence": [],
                }

            ma = actions_map[action_name]
            ma["from_llm"] = True

            # LLM provides richer metadata
            if not ma["description"]:
                ma["description"] = action_dict.get("description", "")
            if not ma["expected_impact"]:
                ma["expected_impact"] = action_dict.get("expected_impact", "")
            if not ma["parameters"]:
                ma["parameters"] = action_dict.get("parameters", {})

            # Priority from LLM
            llm_priority = action_dict.get("priority", "medium")
            ma["priority"] = str(llm_priority).lower()

            if action_dict.get("effort"):
                ma["effort"] = str(action_dict["effort"])

            if col_key not in ma["affected_columns"]:
                ma["affected_columns"].append(col_key)

    # Map contract violations to actions
    for dim, cols in violation_dims.items():
        for ma in actions_map.values():
            overlap = set(ma["affected_columns"]) & set(cols)
            if overlap and dim not in ma["fixes_violations"]:
                ma["fixes_violations"].append(dim)

    # Calculate priority scores
    effort_factors = {"low": 1.0, "medium": 2.0, "high": 4.0}
    for ma in actions_map.values():
        effort_factor = effort_factors.get(ma["effort"], 2.0)
        impact = ma["total_reduction"] + len(ma["affected_columns"]) * 0.1
        ma["priority_score"] = impact / effort_factor

    # Sort by priority bucket then by priority_score
    priority_order = {"high": 0, "medium": 1, "low": 2}
    result = sorted(
        actions_map.values(),
        key=lambda a: (priority_order.get(a["priority"], 1), -a["priority_score"]),
    )

    return result
