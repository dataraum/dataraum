"""The completion measurement is an EXPLICIT tri-state (DAT-807).

``CycleSummaryOutput`` carries a required ``measured`` flag plus three required
numbers, instead of three nullable numbers. Two things had to be true of that
design and are pinned here:

- the distinction survives — a cycle MEASURED at 0% is not the same fact as a
  cycle that could not be measured, and the artifact lifecycle acts on the
  difference (``completion_rate is None`` keeps an artifact grounded rather
  than executed);
- the PERSISTED shape does not move — ``_parse_output`` normalizes the
  unmeasured case back to ``None`` on the domain model, so the nullable columns
  and every downstream ``is None`` reader (health scoring, graph context, the
  cockpit's null-branch) are untouched by the wire-format change.

``_parse_output`` had no coverage at all before this: every phase test mocks
``ground_cycles``, so the whole parse seam was invisible to the suite.
"""

from __future__ import annotations

from typing import Any

from dataraum.analysis.cycles.agent import BusinessCycleAgent
from dataraum.analysis.cycles.models import BusinessCycleAnalysisOutput

_CONTEXT: dict[str, Any] = {
    "tables": [{"table_name": "orders", "columns": [{"name": "status"}]}],
    "summary": {"total_columns": 1, "total_relationships": 0},
}


def _summary(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "cycle_name": "Order Fulfillment",
        "cycle_type": "order_fulfillment",
        "family": "",
        "direction": "",
        "description": "orders move to shipped",
        "business_value": "high",
        "status_column": "",
        "status_table": "",
        "completion_value": "",
        "tables_involved": ["orders"],
        "measured": True,
        "total_records": 100,
        "completed_cycles": 40,
        "completion_rate": 0.4,
        "confidence": 0.9,
        "evidence": ["status column value counts"],
    }
    base.update(overrides)
    return base


# The finance settlement family, as the context builder threads it in (config→DB).
_FAMILY_CONTEXT: dict[str, Any] = {
    **_CONTEXT,
    "cycle_families": {
        "settlement": {"incoming": "accounts_receivable", "outgoing": "accounts_payable"}
    },
}


def _parse_with_families(summary: dict[str, Any]) -> Any:
    output = BusinessCycleAnalysisOutput.model_validate(
        {
            "cycles": [summary],
            "stages": [],
            "entity_flows": [],
            "business_summary": "s",
            "detected_processes": [],
            "data_quality_observations": [],
            "recommendations": [],
        }
    )
    agent = BusinessCycleAgent.__new__(BusinessCycleAgent)
    analysis = agent._parse_output(output, _FAMILY_CONTEXT, 0.0, model="m", vertical="finance")
    (cycle,) = analysis.cycles
    return cycle


def _parse(summary: dict[str, Any]) -> Any:
    output = BusinessCycleAnalysisOutput.model_validate(
        {
            "cycles": [summary],
            "stages": [],
            "entity_flows": [],
            "business_summary": "s",
            "detected_processes": [],
            "data_quality_observations": [],
            "recommendations": [],
        }
    )
    agent = BusinessCycleAgent.__new__(BusinessCycleAgent)
    analysis = agent._parse_output(output, _CONTEXT, 0.0, model="m", vertical="general")
    (cycle,) = analysis.cycles
    return cycle


def test_measured_cycle_keeps_its_numbers() -> None:
    cycle = _parse(_summary())
    assert cycle.total_records == 100
    assert cycle.completed_cycles == 40
    assert cycle.completion_rate == 0.4


def test_non_family_cycle_has_no_direction_axis() -> None:
    cycle = _parse(_summary())
    assert cycle.family is None
    assert cycle.direction is None


def test_family_decided_direction_resolves_to_member(monkeypatch: Any) -> None:
    # A decided settlement/outgoing resolves to accounts_payable (the directed member).
    monkeypatch.setattr(
        "dataraum.analysis.cycles.agent.verify_cycles", lambda cycles, ctx: (cycles, [])
    )
    cycle = _parse_with_families(
        _summary(cycle_type="settlement", family="settlement", direction="outgoing")
    )
    assert cycle.canonical_type == "accounts_payable"
    assert cycle.is_known_type is True
    assert cycle.family == "settlement"
    assert cycle.direction == "outgoing"


def test_family_undetermined_keeps_the_family(monkeypatch: Any) -> None:
    # The honest detected-but-undirected state: canonical is the family, never coerced.
    monkeypatch.setattr(
        "dataraum.analysis.cycles.agent.verify_cycles", lambda cycles, ctx: (cycles, [])
    )
    cycle = _parse_with_families(
        _summary(cycle_type="settlement", family="settlement", direction="undetermined")
    )
    assert cycle.canonical_type == "settlement"
    assert cycle.family == "settlement"
    assert cycle.direction == "undetermined"


def test_unmeasured_cycle_normalizes_to_none() -> None:
    """The wire carries 0 / 0 / 0.0 with measured=false; the domain model must
    show None so the nullable columns still store NULL and the artifact stays
    grounded-but-not-measured rather than 'executed at 0%'."""
    cycle = _parse(
        _summary(measured=False, total_records=0, completed_cycles=0, completion_rate=0.0)
    )
    assert cycle.total_records is None
    assert cycle.completed_cycles is None
    assert cycle.completion_rate is None


def test_a_genuine_zero_percent_measurement_survives() -> None:
    """The whole point of the explicit flag: 0% MEASURED is a real, different
    fact from 'not measured'. A numeric sentinel could not express this without
    colliding, and an omitted field could not express it at all."""
    cycle = _parse(
        _summary(measured=True, total_records=100, completed_cycles=0, completion_rate=0.0)
    )
    assert cycle.completion_rate == 0.0
    assert cycle.completion_rate is not None
    assert cycle.total_records == 100


def test_measured_is_required_on_the_wire() -> None:
    """A model that cannot measure must SAY so — omitting the flag is not a way
    to express it (the field is required, so constrained decoding emits it)."""
    import pytest
    from pydantic import ValidationError

    summary = _summary()
    del summary["measured"]
    with pytest.raises(ValidationError, match="measured"):
        BusinessCycleAnalysisOutput.model_validate(
            {
                "cycles": [summary],
                "stages": [],
                "entity_flows": [],
                "business_summary": "s",
                "detected_processes": [],
                "data_quality_observations": [],
                "recommendations": [],
            }
        )


def test_overall_health_ignores_unmeasured_cycles() -> None:
    """``overall_cycle_health`` averages completion_rates; an unmeasured cycle
    must not drag it toward zero (the -1/0.0 sentinel failure mode)."""
    output = BusinessCycleAnalysisOutput.model_validate(
        {
            "cycles": [
                _summary(cycle_name="A", cycle_type="a", completion_rate=0.8, completed_cycles=80),
                _summary(
                    cycle_name="B",
                    cycle_type="b",
                    measured=False,
                    total_records=0,
                    completed_cycles=0,
                    completion_rate=0.0,
                ),
            ],
            "stages": [],
            "entity_flows": [],
            "business_summary": "s",
            "detected_processes": [],
            "data_quality_observations": [],
            "recommendations": [],
        }
    )
    agent = BusinessCycleAgent.__new__(BusinessCycleAgent)
    analysis = agent._parse_output(output, _CONTEXT, 0.0, model="m", vertical="general")

    assert analysis.overall_cycle_health == 0.8


def test_verify_cycles_is_still_applied(monkeypatch) -> None:
    """The DAT-630 membership floor runs on the parsed cycles — the tri-state
    change must not have moved the guardrail."""
    seen: dict[str, Any] = {}

    def fake_verify(cycles: Any, context: Any) -> Any:
        seen["called"] = True
        return [], ["rejected everything"]

    monkeypatch.setattr("dataraum.analysis.cycles.agent.verify_cycles", fake_verify)
    output = BusinessCycleAnalysisOutput.model_validate(
        {
            "cycles": [_summary()],
            "stages": [],
            "entity_flows": [],
            "business_summary": "s",
            "detected_processes": [],
            "data_quality_observations": [],
            "recommendations": [],
        }
    )
    agent = BusinessCycleAgent.__new__(BusinessCycleAgent)
    analysis = agent._parse_output(output, _CONTEXT, 0.0, model="m", vertical="general")

    assert seen["called"] is True
    assert analysis.cycles == []


def test_stage_and_flow_sentinels_normalize(monkeypatch) -> None:
    """The "" sentinels on the nested stage/flow entries reach the domain model
    as None, so their nullable columns keep storing NULL."""
    output = BusinessCycleAnalysisOutput.model_validate(
        {
            "cycles": [_summary()],
            "stages": [
                {
                    "cycle_name": "Order Fulfillment",
                    "stage_name": "Shipped",
                    "stage_order": 1,
                    "indicator_column": "",
                    "indicator_value": "",
                }
            ],
            "entity_flows": [
                {
                    "cycle_name": "Order Fulfillment",
                    "entity_type": "customer",
                    "entity_column": "customer_id",
                    "entity_table": "orders",
                    "fact_table": "",
                    "fact_column": "",
                }
            ],
            "business_summary": "s",
            "detected_processes": [],
            "data_quality_observations": [],
            "recommendations": [],
        }
    )
    monkeypatch.setattr(
        "dataraum.analysis.cycles.agent.verify_cycles", lambda cycles, ctx: (cycles, [])
    )
    agent = BusinessCycleAgent.__new__(BusinessCycleAgent)
    (cycle,) = agent._parse_output(output, _CONTEXT, 0.0, model="m", vertical="general").cycles

    assert cycle.stages[0].indicator_column is None
    assert cycle.entity_flows[0].fact_table is None
    assert cycle.entity_flows[0].fact_column is None
