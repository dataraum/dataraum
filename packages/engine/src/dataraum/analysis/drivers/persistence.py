"""Persist driver rankings as a run-versioned begin_session artifact (DAT-546).

Bridges the pure engine (:func:`discover_drivers`) to the durable store: enumerate
the session's measure-role fact columns, run the validated discovery over each, and
write one run-versioned :class:`DriverRankingArtifact` per ``(measure_column_id,
run_id)``. The engine and ``resolve_target_type`` are NOT touched here — this module
only orchestrates + serializes.

Run-scoping follows the shipped begin_session convention (``slicing_phase``): the
measure role + temporal behavior are read by ``column_id`` without a run filter —
``semantic_role`` is generation-stable, and the value layer has no base-run pinning
(that is operating_model's concern). ``discover_drivers``' own substrate reads
(enriched view, slice catalog, ``identity_columns``) ARE scoped to the begin_session
``run_id`` passed through, because those artifacts are written this run.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from dataraum.analysis.drivers.db_models import DriverRankingArtifact
from dataraum.analysis.drivers.models import DriverRanking, Measure
from dataraum.analysis.drivers.processor import discover_drivers
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.core.logging import get_logger
from dataraum.storage import Column
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# temporal_behavior → driver target type (mirrors processor.resolve_target_type's
# mapping; inlined so the validated module stays untouched). Anything else → flow,
# the additive reading. Ratio measures are not enumerated here (v1 = measure-role
# columns); they arrive via the deferred on-demand path.
_TEMPORAL_TO_TARGET = {"additive": "flow", "point_in_time": "stock"}


def ranking_to_row(
    ranking: DriverRanking, *, run_id: str, measure_table_id: str, measure_column_id: str
) -> dict[str, Any]:
    """Serialize a :class:`DriverRanking` into a ``DriverRankingArtifact`` row dict.

    Grain labels are preserved verbatim: the primary family's ``grain``/``entity``
    plus every ``SecondaryDriver``'s own ``grain``/``entity`` — never flattened into
    one ranking. The PK + ``created_at`` are omitted so their model defaults apply.
    """
    return {
        "run_id": run_id,
        "measure_table_id": measure_table_id,
        "measure_column_id": measure_column_id,
        "measure_label": ranking.measure,
        "target_type": ranking.target_type,
        "grain": ranking.grain,
        "entity": ranking.entity,
        "n_rows": ranking.n_rows,
        "ranked_dimensions": [
            {"dimension": dim, "gain": gain} for dim, gain in ranking.ranked_dimensions
        ],
        "driver_paths": [list(path) for path in ranking.driver_paths],
        "interesting_slices": [
            {"dimension": s.dimension, "value": s.value, "effect": s.effect, "support": s.support}
            for s in ranking.interesting_slices
        ],
        "secondary_dimensions": [
            {"dimension": sd.dimension, "gain": sd.gain, "grain": sd.grain, "entity": sd.entity}
            for sd in ranking.secondary_dimensions
        ],
    }


def _measure_columns(
    session: Session, table_ids: list[str]
) -> list[tuple[str, str, str, str | None]]:
    """The session's measure-role columns: ``(column_id, table_id, name, behavior)``.

    Read by ``column_id`` without a run filter (the ``slicing_phase`` convention):
    ``semantic_role`` is stable from add_source generation. Deduped to one row per
    column (a deterministic pick by ``column_id``) so a re-adjudicated annotation can
    never run discovery twice for the same measure.
    """
    rows = session.execute(
        select(
            Column.column_id,
            Column.table_id,
            Column.column_name,
            SemanticAnnotation.temporal_behavior,
        )
        .join(SemanticAnnotation, SemanticAnnotation.column_id == Column.column_id)
        .where(
            Column.table_id.in_(table_ids),
            SemanticAnnotation.semantic_role == "measure",
        )
        .order_by(Column.column_id)
    ).all()
    by_column: dict[str, tuple[str, str, str, str | None]] = {}
    for column_id, table_id, column_name, behavior in rows:
        by_column.setdefault(column_id, (column_id, table_id, column_name, behavior))
    return list(by_column.values())


def persist_driver_rankings(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str,
) -> int:
    """Run driver discovery over each measure-role fact column and persist the rankings.

    Idempotent per run — one row per ``(measure_column_id, run_id)`` UPSERTed on
    ``uq_driver_rankings_column_run``; a Temporal success-redelivery converges in place
    (the engine is deterministic per ``(seed, candidate set)``). EVERY measure-role
    column gets a row, including an empty ranking (``n_rows`` records the power) —
    born loud, never silently absent.

    Returns:
        The number of driver-ranking rows persisted.
    """
    if not table_ids:
        return 0
    measures = _measure_columns(session, table_ids)
    if not measures:
        logger.info("driver_rankings_no_measures", table_ids=table_ids)
        return 0

    rows: list[dict[str, Any]] = []
    for column_id, table_id, column_name, behavior in measures:
        target_type = _TEMPORAL_TO_TARGET.get(behavior or "", "flow")
        measure = Measure(target_type=target_type, column=column_name)
        ranking = discover_drivers(
            session,
            duckdb_conn=duckdb_conn,
            fact_table_id=table_id,
            run_id=run_id,
            measure=measure,
        )
        rows.append(
            ranking_to_row(
                ranking,
                run_id=run_id,
                measure_table_id=table_id,
                measure_column_id=column_id,
            )
        )
        logger.info(
            "driver_ranking_persisted",
            measure=column_name,
            target_type=target_type,
            grain=ranking.grain,
            entity=ranking.entity,
            n_rows=ranking.n_rows,
            ranked=len(ranking.ranked_dimensions),
            secondary=len(ranking.secondary_dimensions),
        )

    upsert(session, DriverRankingArtifact, rows, index_elements=["measure_column_id", "run_id"])
    return len(rows)
