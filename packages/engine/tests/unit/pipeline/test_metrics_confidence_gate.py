"""Unit tests for the grounding-confidence gate (DAT-631).

The graph agent records an honest per-concept confidence in each metric's
assumptions. These tests lock two halves of consuming it:

* ``_low_confidence_reason`` — the phase-level gate: a metric reaches executed
  only as strongly as its WEAKEST grounded input; below the floor it returns a
  reason naming the weak grounding, otherwise ``None``.
* cache-assembly confidence propagation — a metric assembled from cached
  snippets (no LLM call, the post-warming common path) must still surface its
  snippets' assumptions, or the gate would be inert for exactly the warmed
  low-confidence metrics.
"""

from __future__ import annotations

from dataclasses import dataclass

from dataraum.graphs.models import GraphAssumptionOutput
from dataraum.pipeline.phases import metrics_phase as gep
from dataraum.pipeline.phases.metrics_phase import _LOW_CONFIDENCE_FLOOR


@dataclass
class _Assumption:
    confidence: float
    assumption: str = "grounded on a proxy discriminator"


@dataclass
class _Execution:
    assumptions: list[_Assumption]


def test_no_assumptions_is_plainly_executed() -> None:
    assert gep._low_confidence_reason(_Execution(assumptions=[])) is None


def test_none_execution_is_plainly_executed() -> None:
    assert gep._low_confidence_reason(None) is None


def test_all_above_floor_is_plainly_executed() -> None:
    exe = _Execution(assumptions=[_Assumption(0.9), _Assumption(_LOW_CONFIDENCE_FLOOR)])
    assert gep._low_confidence_reason(exe) is None


def test_weakest_below_floor_is_flagged() -> None:
    # A 0.9-confidence input does NOT rescue a 0.35 proxy in the same metric —
    # the metric is only as trustworthy as its weakest grounding.
    exe = _Execution(
        assumptions=[
            _Assumption(0.9, "revenue grounded on complete value-set"),
            _Assumption(0.35, "COGS proxied via transaction_type"),
        ]
    )
    reason = gep._low_confidence_reason(exe)
    assert reason is not None
    assert "0.35" in reason
    assert "COGS proxied via transaction_type" in reason


def test_compose_metric_from_dag_propagates_confidence() -> None:
    """Per-metric composition must carry each EXTRACT leaf's assumptions forward — the
    gate's fuel (DAT-646: extract leaves come from the warm cache with their authored
    confidence; the formula/constant CTEs are composed on top)."""
    from dataraum.graphs.agent import GraphAgent
    from dataraum.graphs.models import (
        GraphMetadata,
        GraphSource,
        GraphStep,
        OutputDef,
        OutputType,
        StepSource,
        StepType,
        TransformationGraph,
    )

    step = GraphStep(
        step_id="cost_of_goods_sold",
        step_type=StepType.EXTRACT,
        source=StepSource(standard_field="cost_of_goods_sold", statement="income_statement"),
        aggregation="sum",
        output_step=True,
    )
    graph = TransformationGraph(
        graph_id="gross_profit",
        version="1.0",
        metadata=GraphMetadata(
            name="g", description="", category="profitability", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={"cost_of_goods_sold": step},
    )
    cached = {
        "cost_of_goods_sold": {
            "sql": "SELECT SUM(amount) AS value FROM v WHERE x IN ('a')",
            "description": "COGS proxy",
            "snippet_id": "snip-1",
            "assumptions": [
                {
                    "assumption": "COGS proxied via transaction_type",
                    "basis": "inferred",
                    "confidence": 0.35,
                }
            ],
        }
    }

    agent = GraphAgent.__new__(GraphAgent)  # bypass __init__; method is pure
    code = agent._compose_metric_from_dag(graph, cached, {})

    assert code is not None
    assert len(code.assumptions) == 1
    a = code.assumptions[0]
    assert isinstance(a, GraphAssumptionOutput)
    assert a.confidence == 0.35
    assert a.assumption == "COGS proxied via transaction_type"
    # And the gate would flag a metric carrying it.
    assert gep._low_confidence_reason(_Execution(assumptions=[a])) is not None  # type: ignore[arg-type]
