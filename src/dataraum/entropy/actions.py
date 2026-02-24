"""Merge resolution actions from multiple sources into a unified, prioritized list.

Used by MCP server and API to produce actionable steps for improving data quality.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dataraum.entropy.views.network_context import EntropyForNetwork


def _build_network_impact(
    network_context: EntropyForNetwork,
) -> dict[str, dict[str, Any]]:
    """Build action_name -> network impact mapping from per-column results.

    Walks all columns' node evidence, finds non-low nodes with resolution
    options, and sums causal impact_delta per action across columns.

    Returns:
        Dict keyed by action name with total_delta, columns_affected,
        column list, and a representative resolution_option dict.
    """
    impact: dict[str, dict[str, Any]] = {}

    for target, col_result in network_context.columns.items():
        for node_ev in col_result.node_evidence:
            if node_ev.state == "low" or not node_ev.resolution_options:
                continue

            for ro in node_ev.resolution_options:
                action_name = ro.get("action", "")
                if not action_name:
                    continue

                if action_name not in impact:
                    impact[action_name] = {
                        "total_delta": 0.0,
                        "columns_affected": 0,
                        "columns": [],
                        "resolution_option": ro,
                    }

                ni = impact[action_name]
                ni["total_delta"] += node_ev.impact_delta
                ni["columns_affected"] += 1
                if target not in ni["columns"]:
                    ni["columns"].append(target)

    return impact


def merge_actions(
    interp_by_col: dict[str, Any],
    entropy_objects_by_col: dict[str, list[Any]],
    violation_dims: dict[str, list[str]],
    network_context: EntropyForNetwork | None = None,
) -> list[dict[str, Any]]:
    """Merge actions from all sources into a unified list.

    Args:
        interp_by_col: Column key -> EntropyInterpretationRecord from LLM
        entropy_objects_by_col: Column key -> list of EntropyObjectRecord
        violation_dims: Dimension -> list of affected column keys from contracts
        network_context: Optional EntropyForNetwork with per-column Bayesian
            network results. Provides causal impact_delta for prioritization.

    Returns:
        Sorted list of action dicts with priority, effort, affected columns, etc.
    """
    actions_map: dict[str, dict[str, Any]] = {}

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
                    "from_network": False,
                    "network_impact": 0.0,
                    "network_columns": 0,
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

            if action_dict.get("effort"):
                ma["effort"] = str(action_dict["effort"])

            if col_key not in ma["affected_columns"]:
                ma["affected_columns"].append(col_key)

    # From Bayesian network: causal impact per action
    if network_context is not None:
        network_impact = _build_network_impact(network_context)

        for action_name, ni in network_impact.items():
            if action_name not in actions_map:
                # New action from network — create from resolution_option
                ro = ni["resolution_option"]
                actions_map[action_name] = {
                    "action": action_name,
                    "priority": "medium",
                    "description": ro.get("description", ""),
                    "effort": ro.get("effort", "medium"),
                    "expected_impact": "",
                    "parameters": ro.get("parameters", {}),
                    "affected_columns": [],
                    "cascade_dimensions": list(ro.get("cascade_dimensions", [])),
                    "max_reduction": ro.get("expected_entropy_reduction", 0.0),
                    "total_reduction": 0.0,
                    "from_llm": False,
                    "from_detector": False,
                    "from_network": True,
                    "network_impact": ni["total_delta"],
                    "network_columns": ni["columns_affected"],
                    "fixes_violations": [],
                    "evidence": [],
                }
            else:
                ma = actions_map[action_name]
                ma["from_network"] = True
                ma["network_impact"] = ni["total_delta"]
                ma["network_columns"] = ni["columns_affected"]

    # Map contract violations to actions
    for dim, cols in violation_dims.items():
        for ma in actions_map.values():
            overlap = set(ma["affected_columns"]) & set(cols)
            if overlap and dim not in ma["fixes_violations"]:
                ma["fixes_violations"].append(dim)

    # Calculate priority scores
    # network_impact is the sum of causal impact_delta across columns,
    # measuring how much fixing this action reduces P(intent=high).
    effort_factors = {"low": 1.0, "medium": 2.0, "high": 4.0}
    for ma in actions_map.values():
        effort_factor = effort_factors.get(ma["effort"], 2.0)
        impact = ma["total_reduction"] + len(ma["affected_columns"]) * 0.1
        impact += ma.get("network_impact", 0.0)
        ma["priority_score"] = impact / effort_factor

    # Derive priority labels from score thresholds (replaces LLM-assigned labels)
    for ma in actions_map.values():
        if ma["priority_score"] > 1.0:
            ma["priority"] = "high"
        elif ma["priority_score"] > 0.3:
            ma["priority"] = "medium"
        else:
            ma["priority"] = "low"

    # Sort by priority_score descending
    result = sorted(
        actions_map.values(),
        key=lambda a: -a["priority_score"],
    )

    return result
