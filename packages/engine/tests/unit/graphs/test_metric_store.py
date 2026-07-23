"""The metric-DAG typed home — seed + runtime read (DAT-732, config→DB).

Pins the config→DB seam for metrics: the shipped vertical's declared graphs seed
typed ``Metric`` / ``MetricParameter`` / ``MetricDerivesFrom`` rows once
(idempotently, ON CONFLICT DO NOTHING), and the runtime resolves a parameter's
declared default from those rows — the same discipline the concept vocabulary uses.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import WorkspaceSettings
from dataraum.graphs.agent import GraphAgent
from dataraum.graphs.metric_graph_db_models import (
    Metric,
    MetricDerivesFrom,
    MetricParameter,
)
from dataraum.graphs.metric_store import ensure_metrics_seeded, metric_parameter_defaults
from dataraum.graphs.models import (
    GraphMetadata,
    GraphSource,
    OutputDef,
    OutputType,
    ParameterDef,
    TransformationGraph,
)


def _bind_vertical(session: Session, vertical: str = "finance") -> None:
    session.add(WorkspaceSettings(pin=True, active_vertical=vertical))
    session.flush()


def _param_graph(graph_id: str, default: Any) -> TransformationGraph:
    """A minimal graph carrying one ``days_in_period`` parameter with a given default."""
    return TransformationGraph(
        graph_id=graph_id,
        version="1.0",
        metadata=GraphMetadata(
            name=graph_id, description="", category="working_capital", source=GraphSource.SYSTEM
        ),
        output=OutputDef(output_type=OutputType.SCALAR),
        steps={},
        parameters=[ParameterDef(name="days_in_period", param_type="integer", default=default)],
    )


def _count(session: Session, model: type) -> int:
    return session.scalar(select(func.count()).select_from(model)) or 0


def test_seed_creates_metric_nodes_params_and_edges(session: Session) -> None:
    seeded = ensure_metrics_seeded(session, "finance")
    # One node per declared finance metric graph.
    assert seeded == 16
    assert _count(session, Metric) == 16
    # A metric node carries its declared output metadata.
    dpo = session.execute(select(Metric).where(Metric.graph_id == "dpo")).scalar_one()
    assert dpo.name == "Days Payable Outstanding"
    assert dpo.category == "working_capital"
    assert dpo.unit == "days"
    assert dpo.output_type == "scalar"
    assert dpo.source == "seed"
    # Only the four working-capital metrics declare a parameter (days_in_period).
    assert _count(session, MetricParameter) == 4


def test_seed_derives_from_edges_are_the_distinct_extract_concepts(session: Session) -> None:
    ensure_metrics_seeded(session, "finance")

    def edges(graph_id: str) -> list[str]:
        return sorted(
            r.concept_name
            for r in session.execute(
                select(MetricDerivesFrom).where(MetricDerivesFrom.graph_id == graph_id)
            ).scalars()
        )

    assert edges("dpo") == ["accounts_payable", "cost_of_goods_sold"]
    # ccc extracts five distinct concepts across its statements — one edge each, deduped.
    assert edges("cash_conversion_cycle") == [
        "accounts_payable",
        "accounts_receivable",
        "cost_of_goods_sold",
        "inventory",
        "revenue",
    ]


def test_days_in_period_parameter_carries_its_derivation_marker(session: Session) -> None:
    ensure_metrics_seeded(session, "finance")
    param = session.execute(
        select(MetricParameter).where(
            MetricParameter.graph_id == "dpo", MetricParameter.name == "days_in_period"
        )
    ).scalar_one()
    # The declared default is stored TYPED (JSON), not stringified.
    assert param.default_value == 30
    assert isinstance(param.default_value, int)
    assert param.derivation == "period_grain"
    assert param.param_type == "integer"


def test_seed_is_idempotent(session: Session) -> None:
    assert ensure_metrics_seeded(session, "finance") == 16
    # A re-run inserts nothing — never duplicates, never clobbers.
    assert ensure_metrics_seeded(session, "finance") == 0
    assert _count(session, Metric) == 16
    assert _count(session, MetricParameter) == 4


def test_seed_does_not_clobber_a_supersede(session: Session) -> None:
    """A frame-style edit (supersede + a new active row) survives a re-seed.

    The re-seed's ON CONFLICT DO NOTHING skips the metric whose active row is the
    edit, never overwriting it and never RAISING on the collision — the race-safety
    contract shared with ``ensure_concepts_seeded``.
    """
    ensure_metrics_seeded(session, "finance")
    # Supersede the seeded dpo node and insert a differently-named active row.
    session.execute(
        update(Metric)
        .where(Metric.graph_id == "dpo", Metric.superseded_at.is_(None))
        .values(superseded_at=datetime.now(UTC))
    )
    session.add(
        Metric(
            vertical="finance",
            graph_id="dpo",
            name="Days Payable Outstanding (edited)",
            source="seed",
        )
    )
    session.flush()
    # A re-seed must NOT raise and must NOT clobber the edit.
    ensure_metrics_seeded(session, "finance")
    active = session.execute(
        select(Metric).where(Metric.graph_id == "dpo", Metric.superseded_at.is_(None))
    ).scalar_one()
    assert active.name == "Days Payable Outstanding (edited)"


def test_metric_parameter_defaults_reads_the_typed_home(session: Session) -> None:
    _bind_vertical(session)
    ensure_metrics_seeded(session, "finance")
    # A metric with a parameter resolves its declared default from the DB.
    assert metric_parameter_defaults(session, "dpo") == {"days_in_period": 30}
    # A metric with no parameters resolves to an empty mapping (caller falls back).
    assert metric_parameter_defaults(session, "gross_profit") == {}


def test_metric_parameter_defaults_empty_when_unseeded(session: Session) -> None:
    # No seed, no bound vertical → the scoped read finds nothing, never raises.
    assert metric_parameter_defaults(session, "dpo") == {}


def test_one_malformed_metric_does_not_sink_the_seed(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A single unparseable / CHECK-violating metric is skipped, never raised —
    the good metrics still seed. One bad definition (reachable via a user-authored
    ``metric`` teach row) must not fail the whole add_source grounding phase."""
    good = {
        "graph_id": "good",
        "metadata": {"name": "Good"},
        "output": {"type": "scalar"},
        "dependencies": {
            "revenue": {"type": "extract", "source": {"standard_field": "revenue"}},
        },
    }
    bad_no_name = {"graph_id": "bad_noname", "metadata": {}, "output": {"type": "scalar"}}
    bad_derivation = {
        "graph_id": "bad_deriv",
        "metadata": {"name": "BadDeriv"},
        "output": {"type": "scalar"},
        "parameters": {"p": {"type": "integer", "default": 1, "derivation": "not_a_rule"}},
    }
    monkeypatch.setattr(
        "dataraum.graphs.metric_store.get_metric_definitions",
        lambda _vertical: {
            "good": good,
            "bad_noname": bad_no_name,
            "bad_deriv": bad_derivation,
        },
    )
    # Never raises; only the well-formed metric seeds.
    seeded = ensure_metrics_seeded(session, "finance")
    assert seeded == 1
    graph_ids = {r.graph_id for r in session.execute(select(Metric)).scalars()}
    assert graph_ids == {"good"}
    assert _count(session, MetricDerivesFrom) == 1  # good → revenue


def test_write_failure_rolls_back_only_that_metric(
    session: Session, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mid-metric WRITE failure rolls back that metric's savepoint (node included) and
    the seed continues — the load-bearing isolation on Postgres, where a failed
    statement otherwise aborts the whole transaction. Here every parameter INSERT is
    forced to raise, so the four working-capital metrics (which have a days_in_period
    param) roll back wholesale while the param-less metrics seed cleanly."""
    import dataraum.graphs.metric_store as ms

    real_insert = ms.insert_if_absent

    def _insert(session_: Session, model: Any, rows: list[dict[str, Any]], **kw: Any) -> int:
        if model is MetricParameter:
            raise RuntimeError("forced parameter-insert failure")
        return real_insert(session_, model, rows, **kw)

    monkeypatch.setattr(ms, "insert_if_absent", _insert)

    seeded = ensure_metrics_seeded(session, "finance")
    # The 12 param-less metrics seeded; the 4 param-bearing metrics rolled back whole.
    assert seeded == 12
    graph_ids = {r.graph_id for r in session.execute(select(Metric)).scalars()}
    assert "gross_profit" in graph_ids
    assert graph_ids.isdisjoint({"dpo", "dso", "dio", "cash_conversion_cycle"})
    assert _count(session, MetricParameter) == 0  # every param write rolled back


def test_resolve_parameters_precedence_provided_then_db_then_parsed(session: Session) -> None:
    """The three-way merge (DAT-732): provided wins over the DB default, which wins
    over the parsed graph default; an unseeded metric falls back to the parsed default."""
    _bind_vertical(session)
    ensure_metrics_seeded(session, "finance")  # dpo.days_in_period = 30 in the DB
    agent = GraphAgent.__new__(GraphAgent)
    # The parsed default (999) differs from the DB default (30) so we can tell them apart.
    dpo = _param_graph("dpo", default=999)

    # (a) provided wins over both the DB and the parsed default.
    assert agent._resolve_parameters(session, dpo, {"days_in_period": 7}) == {"days_in_period": 7}
    # (b) nothing provided → the DB default (30) wins over the parsed default (999).
    assert agent._resolve_parameters(session, dpo, {}) == {"days_in_period": 30}
    # (c) a metric with no seeded row → fall back to the parsed default.
    unseeded = _param_graph("not_seeded", default=999)
    assert agent._resolve_parameters(session, unseeded, {}) == {"days_in_period": 999}
