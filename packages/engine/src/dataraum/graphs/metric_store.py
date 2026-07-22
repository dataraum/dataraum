"""Seed + read the metric-DAG typed home (DAT-732).

The metric transformation graphs are *declared* — parsed from the vertical's
``metrics/**`` YAML ⊕ ``metric`` teach overlay rows
(:func:`dataraum.graphs.config.get_metric_definitions`). :func:`ensure_metrics_seeded`
normalizes that declared set into the typed rows
(:mod:`dataraum.graphs.metric_graph_db_models`) once per workspace, the same
config→DB seed the concept vocabulary uses (DAT-728): ``INSERT … ON CONFLICT DO
NOTHING`` on the active-row partial-unique index, so a re-run is a no-op, a future
``frame`` edit (which supersedes) is never clobbered, and a concurrent seed can't
collide (no read-then-insert TOCTOU).

:func:`metric_parameter_defaults` is the runtime read: the merge point
(``GraphAgent._resolve_parameters``) resolves a parameter's DECLARED default from
this typed home, not from the raw parsed graph — the DB is the authority. The
data-derived override (``days_in_period`` from the observed flow window) stays a
RESOLVER computation (:mod:`dataraum.graphs.period_resolver`), never a stored value.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select
from sqlalchemy import text as sa_text

from dataraum.analysis.semantic.db_models import WorkspaceSettings
from dataraum.core.logging import get_logger
from dataraum.graphs.config import get_metric_definitions
from dataraum.graphs.loader import GraphLoader, GraphLoadError
from dataraum.graphs.metric_graph_db_models import (
    Metric,
    MetricDerivesFrom,
    MetricParameter,
)
from dataraum.graphs.models import StepType
from dataraum.storage.upsert import insert_if_absent

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.graphs.models import TransformationGraph

logger = get_logger(__name__)

_ADHOC = "_adhoc"


def ensure_metrics_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the declared metric DAG as typed rows (DAT-732).

    Loads the vertical's declared metric set (shipped ``metrics/**`` ⊕ ``metric``
    teach overlays) and inserts, for each metric with no active row yet:

    * one :class:`Metric` node (its declared output metadata),
    * one :class:`MetricParameter` per parameter (declared default + derivation marker),
    * one :class:`MetricDerivesFrom` edge per distinct extract ``standard_field`` (the
      concept the metric derives from).

    All via ``INSERT … ON CONFLICT DO NOTHING`` on the active-row partial-unique index,
    so a re-run / a frame edit is never clobbered and a concurrent seed is race-safe —
    the same discipline as ``ensure_concepts_seeded``. A framed vertical with no on-disk
    metrics and no ``metric`` overlays seeds nothing. Returns the number of metric
    NODES inserted (conflicts skipped).

    **Per-metric fault isolation.** Each declared metric is parsed AND written on its
    own, mirroring the sibling ``ground_columns`` in this same phase ("a malformed graph
    must not sink column grounding"): a definition that fails to parse (a shipped-YAML
    typo, or a user-authored ``metric`` teach-overlay row) is logged and skipped, and
    each metric's node+params+edges are written inside their OWN ``begin_nested``
    savepoint so a bad row (e.g. a CHECK violation from an invalid teach payload) rolls
    back only THAT metric — never the whole batch, and never the concept/edge seeds this
    phase already committed. One malformed metric must not fail the add_source grounding
    phase (a non-transient failure Temporal would retry forever).
    """
    seeded = 0
    for graph_id, defn in get_metric_definitions(vertical).items():
        try:
            graph = GraphLoader().graphs_from_definitions({graph_id: defn})[graph_id]
        except GraphLoadError as exc:
            logger.warning("metric_seed_parse_skip", graph_id=graph_id, error=str(exc))
            continue
        try:
            with session.begin_nested():
                inserted = insert_if_absent(
                    session,
                    Metric,
                    [_metric_row(vertical, graph)],
                    index_elements=["vertical", "graph_id"],
                    index_where=sa_text("superseded_at IS NULL"),
                )
                param_rows = _parameter_rows(vertical, graph)
                if param_rows:
                    insert_if_absent(
                        session,
                        MetricParameter,
                        param_rows,
                        index_elements=["vertical", "graph_id", "name"],
                        index_where=sa_text("superseded_at IS NULL"),
                    )
                edge_rows = _derives_from_rows(vertical, graph)
                if edge_rows:
                    insert_if_absent(
                        session,
                        MetricDerivesFrom,
                        edge_rows,
                        index_elements=["vertical", "graph_id", "concept_name"],
                        index_where=sa_text("superseded_at IS NULL"),
                    )
        except Exception as exc:  # noqa: BLE001 - one bad metric must not sink the seed
            # The savepoint rolled back only THIS metric's partial writes; the outer
            # session (concept seeds, prior metrics) is intact. Degrade to skip-and-log.
            logger.warning("metric_seed_write_skip", graph_id=graph_id, error=str(exc))
            continue
        seeded += inserted
    if seeded:
        logger.info("metrics_seeded", vertical=vertical, count=seeded)
    return seeded


def _metric_row(vertical: str, graph: TransformationGraph) -> dict[str, Any]:
    """One :class:`Metric` node row from a parsed graph's declared metadata."""
    return {
        "vertical": vertical,
        "graph_id": graph.graph_id,
        "name": graph.metadata.name,
        "category": graph.metadata.category or None,
        "unit": graph.output.unit,
        "output_type": graph.output.output_type.value,
        "version": graph.version,
        "source": "seed",
    }


def _parameter_rows(vertical: str, graph: TransformationGraph) -> list[dict[str, Any]]:
    """One :class:`MetricParameter` node row per declared parameter."""
    return [
        {
            "vertical": vertical,
            "graph_id": graph.graph_id,
            "name": param.name,
            "param_type": param.param_type,
            "default_value": param.default,
            "options": param.options,
            "description": param.description,
            "derivation": param.derivation,
            "source": "seed",
        }
        for param in graph.parameters
    ]


def _derives_from_rows(vertical: str, graph: TransformationGraph) -> list[dict[str, Any]]:
    """One :class:`MetricDerivesFrom` edge per DISTINCT extract ``standard_field``.

    The metric derives from the concepts its EXTRACT leaves ground; ``standard_field``
    IS the concept name the ``og_derives_from`` view resolves to the active concept.
    Dedup so a metric that extracts a concept twice (different statements) yields one
    edge — the active-row unique index requires it.
    """
    concepts: list[str] = []
    seen: set[str] = set()
    for step in graph.steps.values():
        if step.step_type != StepType.EXTRACT or step.source is None:
            continue
        field = step.source.standard_field
        if field and field not in seen:
            seen.add(field)
            concepts.append(field)
    return [
        {"vertical": vertical, "graph_id": graph.graph_id, "concept_name": field}
        for field in concepts
    ]


def metric_parameter_defaults(session: Session, graph_id: str) -> dict[str, Any]:
    """The declared parameter defaults for one metric, from the typed home (DAT-732).

    The runtime authority for ``GraphAgent._resolve_parameters``: returns
    ``{parameter_name: declared_default}`` for the metric's ACTIVE parameter rows,
    scoped to the workspace's single bound ``active_vertical`` (the same
    ``workspace_settings`` scoping the read views apply, so a wrong ``--vertical`` can't
    surface another vertical's parameters). An unbound workspace / unseeded substrate
    resolves to ``_adhoc`` → no rows → ``{}``, and the caller falls back to the parsed
    graph's declared defaults (YAML is the seed; the DB is the authority once seeded).
    """
    active_vertical = select(WorkspaceSettings.active_vertical).scalar_subquery()
    stmt = select(MetricParameter.name, MetricParameter.default_value).where(
        MetricParameter.graph_id == graph_id,
        MetricParameter.vertical == func.coalesce(active_vertical, _ADHOC),
        MetricParameter.superseded_at.is_(None),
    )
    return dict(session.execute(stmt).tuples().all())
