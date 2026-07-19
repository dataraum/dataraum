"""The graph-spec YAML served to the authoring LLM (DAT-792).

``_graph_to_yaml`` is the single serialization of the graph the grounding
prompt sees. It must carry each step's declared ``validations`` — the
catalogue's post-execution expectations (``StepValidation``, DAT-616) — so the
model grounds consistently with what the catalogue declares about the value.
Enforcement stays post-hoc in ``graphs.verifier`` (flags, never gates); this
only serves the declared facts at authoring time.
"""

from __future__ import annotations

import yaml

from dataraum.graphs.agent import GraphAgent
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    GraphStep,
    OutputDef,
    OutputType,
    StepSource,
    StepType,
    StepValidation,
    TransformationGraph,
)
from dataraum.graphs.node_warming import WarmNode, build_mini_graph


def _graph(steps: dict[str, GraphStep]) -> TransformationGraph:
    return TransformationGraph(
        graph_id="revenue_total",
        version="1.0",
        metadata=GraphMetadata(
            name="revenue_total",
            description="",
            category="profitability",
            source=GraphSource.SYSTEM,
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps=steps,
    )


def _to_yaml(graph: TransformationGraph) -> dict:
    # ``_graph_to_yaml`` reads only ``graph``; skip the LLM-plumbing __init__.
    agent = GraphAgent.__new__(GraphAgent)
    return yaml.safe_load(agent._graph_to_yaml(graph))


def test_step_validations_are_served_to_the_authoring_prompt() -> None:
    step = GraphStep(
        step_id="revenue",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="revenue", statement="income_statement"),
        aggregation="sum",
        validations=[
            StepValidation(
                condition="value > 0", severity="error", message="Revenue must be positive"
            ),
            StepValidation(condition="value < 1000000000", severity="warning"),
        ],
    )
    parsed = _to_yaml(_graph({"revenue": step}))

    served = parsed["dependencies"]["revenue"]["validations"]
    assert served == [
        {"condition": "value > 0", "severity": "error", "message": "Revenue must be positive"},
        # An empty message is omitted — serve the declared facts, no filler keys.
        {"condition": "value < 1000000000", "severity": "warning"},
    ]


def test_step_without_validations_serves_no_empty_key() -> None:
    """No declared expectations → no ``validations`` key at all (lean YAML)."""
    step = GraphStep(
        step_id="revenue",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="revenue", statement="income_statement"),
        aggregation="sum",
    )
    parsed = _to_yaml(_graph({"revenue": step}))

    assert "validations" not in parsed["dependencies"]["revenue"]


def test_mini_graph_carries_validations_into_the_yaml() -> None:
    """The authoring path serializes the WARM mini-graph (node_warming), not the
    full metric graph — the ``dataclasses.replace`` copy must keep the leaf's
    declared validations so DAT-792 reaches the prompt that actually grounds."""
    step = GraphStep(
        step_id="revenue",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="revenue", statement="income_statement"),
        aggregation="sum",
        validations=[StepValidation(condition="value >= 0")],
    )
    graph = _graph({"revenue": step})
    mini = build_mini_graph(
        WarmNode(key=("extract", "revenue", "income_statement", "sum"), graph=graph, step=step)
    )

    parsed = _to_yaml(mini)
    assert parsed["dependencies"]["revenue"]["validations"] == [
        {"condition": "value >= 0", "severity": "error"}
    ]
