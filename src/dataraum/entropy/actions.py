"""Merge resolution actions from multiple sources into a unified, prioritized list.

Used by MCP server, CLI, and API to produce actionable steps for improving data quality.
"""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.entropy.contracts import evaluate_all_contracts
from dataraum.entropy.db_models import EntropyObjectRecord
from dataraum.entropy.interpretation_db_models import EntropyInterpretationRecord
from dataraum.entropy.views.network_context import build_for_network
from dataraum.entropy.views.query_context import network_to_column_summaries
from dataraum.storage import Column, Source, Table

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
        column list, per-column deltas, and a representative resolution_option dict.
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
                        "column_deltas": {},
                        "resolution_option": ro,
                    }

                ni = impact[action_name]
                ni["total_delta"] += node_ev.impact_delta
                ni["columns_affected"] += 1
                if target not in ni["columns"]:
                    ni["columns"].append(target)
                # Accumulate per-column delta (a column may appear in multiple nodes)
                ni["column_deltas"][target] = (
                    ni["column_deltas"].get(target, 0.0) + node_ev.impact_delta
                )

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
                    "from_llm": True,
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
                    "from_llm": False,
                    "network_impact": ni["total_delta"],
                    "network_columns": ni["columns_affected"],
                    "column_deltas": ni["column_deltas"],
                    "fixes_violations": [],
                    "evidence": [],
                }
            else:
                ma = actions_map[action_name]
                ma["network_impact"] = ni["total_delta"]
                ma["network_columns"] = ni["columns_affected"]
                ma["column_deltas"] = ni["column_deltas"]

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
        impact = len(ma["affected_columns"]) * 0.1 + ma.get("network_impact", 0.0)
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


def load_actions(session: Session, source: Source) -> list[dict[str, Any]]:
    """Load and merge all resolution actions for a source.

    Queries entropy interpretations, entropy objects, Bayesian network context,
    and contract violations, then merges them via merge_actions().

    Args:
        session: Database session
        source: Source model instance

    Returns:
        Sorted list of merged action dicts (same format as merge_actions output)
    """
    # Get typed tables
    tables_result = session.execute(
        select(Table).where(
            Table.source_id == source.source_id,
            Table.layer == "typed",
        )
    )
    tables = tables_result.scalars().all()
    if not tables:
        return []

    table_ids = [t.table_id for t in tables]

    # Build column_id -> column_key mapping
    col_id_to_key: dict[str, str] = {}
    for tbl in tables:
        cols_result = session.execute(select(Column).where(Column.table_id == tbl.table_id))
        for col in cols_result.scalars().all():
            col_id_to_key[col.column_id] = f"{tbl.table_name}.{col.column_name}"

    # Build column summaries and network context
    network_context = build_for_network(session, table_ids)
    column_summaries = network_to_column_summaries(network_context)

    # Get LLM interpretations with resolution actions (column-level and table-level)
    interp_result = session.execute(
        select(EntropyInterpretationRecord).where(
            EntropyInterpretationRecord.source_id == source.source_id,
        )
    )
    interp_by_col: dict[str, Any] = {}
    for interp in interp_result.scalars().all():
        if interp.column_name:
            col_key = f"{interp.table_name}.{interp.column_name}"
        else:
            col_key = interp.table_name
        interp_by_col[col_key] = interp

    # Get entropy objects for evidence
    entropy_objects_by_col: dict[str, list[Any]] = defaultdict(list)
    if table_ids:
        eo_result = session.execute(
            select(EntropyObjectRecord)
            .where(EntropyObjectRecord.table_id.in_(table_ids))
            .order_by(EntropyObjectRecord.score.desc())
        )
        for obj in eo_result.scalars().all():
            col_key = col_id_to_key.get(obj.column_id, "") if obj.column_id else ""
            if col_key:
                entropy_objects_by_col[col_key].append(obj)

    # Get contract violations
    evaluations = evaluate_all_contracts(column_summaries)
    all_column_keys = list(column_summaries.keys())
    violation_dims: dict[str, list[str]] = {}
    for eval_result in evaluations.values():
        for v in eval_result.violations:
            if v.dimension:
                violation_dims.setdefault(v.dimension, []).extend(v.affected_columns)
            elif v.violation_type == "overall":
                violation_dims.setdefault("overall", []).extend(all_column_keys)
            elif v.affected_columns:
                key = v.condition or v.violation_type
                violation_dims.setdefault(key, []).extend(v.affected_columns)

    # Merge actions from all sources
    return merge_actions(
        interp_by_col=interp_by_col,
        entropy_objects_by_col=entropy_objects_by_col,
        violation_dims=violation_dims,
        network_context=network_context,
    )
