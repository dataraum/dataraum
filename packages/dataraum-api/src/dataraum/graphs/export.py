"""Visualization export for React Flow.

Converts graph execution traces to JSON format for React Flow visualization.

Usage:
    from dataraum.graphs.export import export_to_react_flow

    json_data = export_to_react_flow(execution)
    # Returns dict ready for JSON serialization
"""

from __future__ import annotations

from typing import Any

from .models import (
    GraphExecution,
    StepResult,
    StepType,
    TransformationGraph,
)


def export_to_react_flow(
    execution: GraphExecution,
    graph: TransformationGraph | None = None,
) -> dict[str, Any]:
    """Export execution trace to React Flow JSON format.

    Args:
        execution: The graph execution with step results
        graph: Optional graph definition for additional metadata

    Returns:
        Dict with nodes and edges for React Flow visualization
    """
    nodes = []
    edges = []

    # Build nodes from step results
    for step_result in execution.step_results:
        node = _build_node(step_result, graph)
        nodes.append(node)

        # Build edges from dependencies
        if graph and step_result.step_id in graph.steps:
            step_def = graph.steps[step_result.step_id]
            for dep_id in step_def.depends_on:
                edges.append(
                    {
                        "id": f"{dep_id}-{step_result.step_id}",
                        "source": dep_id,
                        "target": step_result.step_id,
                        "animated": False,
                    }
                )

    return {
        "execution_id": execution.execution_id,
        "graph_id": execution.graph_id,
        "graph_type": execution.graph_type.value,
        "graph_version": execution.graph_version,
        "period": execution.period,
        "executed_at": execution.executed_at.isoformat(),
        "output_value": execution.output_value,
        "output_interpretation": execution.output_interpretation,
        "nodes": nodes,
        "edges": edges,
    }


def _build_node(step_result: StepResult, graph: TransformationGraph | None) -> dict[str, Any]:
    """Build a React Flow node from a step result."""
    # Determine node type based on step type
    node_type = _get_node_type(step_result.step_type)

    # Get step definition for additional metadata
    step_def = None
    if graph and step_result.step_id in graph.steps:
        step_def = graph.steps[step_result.step_id]

    # Build node data
    data: dict[str, Any] = {
        "label": _format_label(step_result.step_id),
        "value": step_result.value,
        "level": step_result.level,
        "stepType": step_result.step_type.value,
    }

    # Add type-specific data
    if step_result.step_type == StepType.EXTRACT:
        data["expandable"] = True
        if step_result.source_query:
            data["drilldown"] = {
                "query": step_result.source_query,
                "row_count": step_result.rows_affected,
            }
        if step_result.inputs_used:
            data["source"] = (
                f"{step_result.inputs_used.get('table', '')}.{step_result.inputs_used.get('column', '')}"
            )
            data["aggregation"] = step_result.inputs_used.get("aggregation")

    elif step_result.step_type == StepType.FORMULA:
        if step_def and step_def.expression:
            data["formula"] = step_def.expression
        data["inputs"] = step_result.inputs_used

    elif step_result.step_type == StepType.PREDICATE:
        if step_def and step_def.condition:
            data["condition"] = step_def.condition
        data["pass_count"] = step_result.rows_passed
        data["fail_count"] = step_result.rows_failed
        if step_result.classification:
            data["fail_action"] = step_result.classification.value
        if step_def and step_def.reason:
            data["reason"] = step_def.reason

    elif step_result.step_type == StepType.CONSTANT:
        if step_def and step_def.parameter:
            data["parameter"] = step_def.parameter

    elif step_result.step_type == StepType.COMPOSITE:
        if step_def and step_def.logic:
            data["logic"] = step_def.logic
        data["inputs"] = step_result.inputs_used

    # Add unit if available from graph output
    if graph and step_def and step_def.output_step:
        if graph.output.unit:
            data["unit"] = graph.output.unit

    # Position hint based on level (actual positioning done by React Flow)
    position = {
        "x": step_result.level * 250,
        "y": 0,  # Will be calculated by layout algorithm
    }

    return {
        "id": step_result.step_id,
        "type": node_type,
        "position": position,
        "data": data,
    }


def _get_node_type(step_type: StepType) -> str:
    """Map step type to React Flow node type."""
    mapping = {
        StepType.EXTRACT: "extractNode",
        StepType.CONSTANT: "constantNode",
        StepType.PREDICATE: "predicateNode",
        StepType.FORMULA: "formulaNode",
        StepType.COMPOSITE: "compositeNode",
    }
    return mapping.get(step_type, "default")


def _format_label(step_id: str) -> str:
    """Format step ID into human-readable label."""
    # Convert snake_case to Title Case
    return step_id.replace("_", " ").title()


def export_graph_definition(graph: TransformationGraph) -> dict[str, Any]:
    """Export graph definition (without execution) for preview.

    Args:
        graph: The graph definition

    Returns:
        Dict with nodes and edges showing graph structure
    """
    nodes = []
    edges = []

    for step_id, step in graph.steps.items():
        node_type = _get_node_type(step.step_type)

        data: dict[str, Any] = {
            "label": _format_label(step_id),
            "level": step.level,
            "stepType": step.step_type.value,
        }

        # Add step-specific metadata
        if step.step_type == StepType.EXTRACT and step.source:
            data["source"] = (
                step.source.standard_field or f"{step.source.table}.{step.source.column}"
            )
            data["aggregation"] = step.aggregation

        elif step.step_type == StepType.FORMULA:
            data["formula"] = step.expression

        elif step.step_type == StepType.PREDICATE:
            data["condition"] = step.condition
            if step.on_false:
                data["fail_action"] = step.on_false.value

        elif step.step_type == StepType.CONSTANT:
            data["value"] = step.value
            data["parameter"] = step.parameter

        elif step.step_type == StepType.COMPOSITE:
            data["logic"] = step.logic

        if step.output_step:
            data["isOutput"] = True

        position = {"x": step.level * 250, "y": 0}

        nodes.append(
            {
                "id": step_id,
                "type": node_type,
                "position": position,
                "data": data,
            }
        )

        # Add edges from dependencies
        for dep_id in step.depends_on:
            edges.append(
                {
                    "id": f"{dep_id}-{step_id}",
                    "source": dep_id,
                    "target": step_id,
                }
            )

    return {
        "graph_id": graph.graph_id,
        "graph_type": graph.graph_type.value,
        "version": graph.version,
        "name": graph.metadata.name,
        "description": graph.metadata.description,
        "nodes": nodes,
        "edges": edges,
    }
