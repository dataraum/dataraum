"""Aggregation-lineage discovery orchestration (DAT-491).

The LLM candidate carries only the hypothesis (measure ← event table, value
expression, filter). This module derives the whole alignment from substrate
the engine already verified — the entity key from the relationship catalog
(a direct defined relationship, or two columns referencing the same dimension
key), the event date from ``TableEntity.time_column`` (joining a header table
through a verified many-to-one relationship when the line table has no date),
and the period bridge from the measure period column's type + the temporal
profile's ``detected_granularity`` — then reconciles deterministically
(:mod:`dataraum.analysis.lineage.reconcile`) and persists one run-versioned
``MeasureAggregationLineage`` row per RECONCILED candidate.

Every dropped candidate logs its stage and detail (``lineage_candidate_dropped``)
— the witness downstream abstains, it never guesses, and the abstention is
visible, never silent.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.lineage.models import CandidateDisposal, LineageCandidate
from dataraum.analysis.lineage.reconcile import dispose
from dataraum.analysis.relationships.utils import load_defined_relationships
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.temporal import TemporalColumnProfile
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table
from dataraum.storage.snapshot_head import head_run_id

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# A candidate whose join covers less than this fraction of the measure's
# (entity, period) rows is misaligned (wrong key or period bridge) — drop it
# before reconciling. Coverage, not correctness: the residual gate still rules.
MIN_JOIN_COVERAGE = 0.5

# Measure-period granularities the bridge supports, mapped to date_trunc parts.
# Finer/irregular granularities mean the column is not a period key — drop.
_GRAIN_TO_TRUNC = {
    "day": "day",
    "week": "week",
    "month": "month",
    "quarter": "quarter",
    "year": "year",
}

_TEMPORAL_TYPES = {"DATE", "TIMESTAMP", "TIMESTAMPTZ"}

# Event-side grain safety, mirroring enriched_views: the event (line) table must
# be the many side of the header join or the join changes the anchor sums.
_HEADER_SAFE = {
    ("from", "many-to-one"),
    ("from", "one-to-one"),
    ("to", "one-to-many"),
    ("to", "one-to-one"),
}


@dataclass(frozen=True)
class _Alignment:
    """The derived (never proposed) alignment for one candidate."""

    measure_table: Table
    measure_column: Column
    key_measure: Column
    key_event: Column
    bridge_measure_sql: str  # over alias m
    bridge_event_sql: str  # over aliases e/h
    event_table: Table
    header_table: Table | None
    join_on_sql: str | None  # e."x" = h."y"


def _drop(candidate: LineageCandidate, stage: str, **detail: object) -> None:
    logger.info(
        "lineage_candidate_dropped",
        measure=f"{candidate.measure_table}.{candidate.measure_column}",
        event_table=candidate.event_table,
        stage=stage,
        **detail,
    )


def _columns_by_name(session: Session, table_id: str) -> dict[str, Column]:
    cols = session.execute(select(Column).where(Column.table_id == table_id)).scalars()
    return {c.column_name: c for c in cols}


def _time_column(session: Session, table_id: str, run_id: str | None) -> Column | None:
    """The table's semantic time column (``TableEntity.time_column``), typed temporal."""
    entity = session.execute(
        select(TableEntity).where(TableEntity.table_id == table_id, TableEntity.run_id == run_id)
    ).scalar_one_or_none()
    if entity is None or not entity.time_column:
        return None
    col = _columns_by_name(session, table_id).get(entity.time_column)
    if col is None or col.resolved_type not in _TEMPORAL_TYPES:
        return None
    return col


def _period_grain(session: Session, period_col: Column) -> str | None:
    """date_trunc part for the measure period column, from its promoted temporal profile.

    Pinned via the ``(table, "temporal")`` snapshot head (DAT-448) — fail closed
    when no head or no profile exists rather than reading a stale run's row.
    """
    pinned = head_run_id(session, f"table:{period_col.table_id}", "temporal")
    if pinned is None:
        return None
    profile = session.execute(
        select(TemporalColumnProfile).where(
            TemporalColumnProfile.column_id == period_col.column_id,
            TemporalColumnProfile.run_id == pinned,
        )
    ).scalar_one_or_none()
    if profile is None:
        return None
    return _GRAIN_TO_TRUNC.get(profile.detected_granularity)


def _entity_key_pairs(
    session: Session,
    measure_table_id: str,
    event_table_id: str,
    table_ids: list[str],
    run_id: str | None,
) -> list[tuple[Column, Column]]:
    """Evidenced entity-key pairs between measure and event table.

    A pair is evidenced by a direct defined relationship between the two
    tables, or by both columns referencing the SAME dimension column
    (shared-dimension equivalence: ``measure.col → dim.pk ← event.col``).
    """
    rels = load_defined_relationships(
        session, table_ids, run_id=run_id, both_tables=False, eager_columns=True
    )
    pairs: dict[tuple[str, str], tuple[Column, Column]] = {}

    # Direct relationships between the two tables, either direction.
    for rel in rels:
        endpoints = {rel.from_table_id: rel.from_column, rel.to_table_id: rel.to_column}
        if set(endpoints) == {measure_table_id, event_table_id}:
            m_col, e_col = endpoints[measure_table_id], endpoints[event_table_id]
            pairs[(m_col.column_id, e_col.column_id)] = (m_col, e_col)

    # Shared dimension: group child→parent edges by the parent column.
    by_parent: dict[str, dict[str, Column]] = {}
    for rel in rels:
        by_parent.setdefault(rel.to_column_id, {})[rel.from_table_id] = rel.from_column
    for children in by_parent.values():
        shared_m, shared_e = children.get(measure_table_id), children.get(event_table_id)
        if shared_m is not None and shared_e is not None:
            pairs[(shared_m.column_id, shared_e.column_id)] = (shared_m, shared_e)

    return list(pairs.values())


def _event_date_options(
    session: Session,
    event_table: Table,
    table_ids: list[str],
    run_id: str | None,
) -> list[tuple[str, Table | None, str | None]]:
    """Every way to date the event rows: ``(date_sql, header_table, join_on_sql)``.

    The event table's own time column (header/join ``None``) when it has one;
    otherwise each table reachable through a verified grain-safe (event-is-many)
    defined relationship whose ``time_column`` is set. The caller requires
    exactly one option — zero or several is a logged abstention.
    """
    own = _time_column(session, event_table.table_id, run_id)
    if own is not None:
        return [(f'e."{own.column_name}"', None, None)]

    rels = load_defined_relationships(
        session, table_ids, run_id=run_id, both_tables=False, eager_columns=True
    )
    options: dict[str, tuple[str, Table | None, str | None]] = {}
    for rel in rels:
        if rel.from_table_id == event_table.table_id:
            side, header_col, event_col = "from", rel.to_column, rel.from_column
        elif rel.to_table_id == event_table.table_id:
            side, header_col, event_col = "to", rel.from_column, rel.to_column
        else:
            continue
        if (side, rel.cardinality or "") not in _HEADER_SAFE:
            continue
        header_table = header_col.table
        date_col = _time_column(session, header_table.table_id, run_id)
        if date_col is None:
            continue
        join_on = f'e."{event_col.column_name}" = h."{header_col.column_name}"'
        options[header_table.table_id] = (f'h."{date_col.column_name}"', header_table, join_on)
    return list(options.values())


def _derive_alignment(
    session: Session,
    candidate: LineageCandidate,
    table_ids: list[str],
    run_id: str | None,
) -> _Alignment | None:
    """Derive the full alignment for a candidate; ``None`` (logged) when it can't be."""
    tables = {
        t.table_name: t
        for t in session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars()
    }
    measure_table = tables.get(candidate.measure_table)
    event_table = tables.get(candidate.event_table)
    if measure_table is None or event_table is None:
        _drop(candidate, "unknown_table")
        return None
    measure_column = _columns_by_name(session, measure_table.table_id).get(candidate.measure_column)
    if measure_column is None:
        _drop(candidate, "unknown_measure_column")
        return None

    period_col = _time_column(session, measure_table.table_id, run_id)
    if period_col is None:
        _drop(candidate, "no_measure_time_column")
        return None
    grain = _period_grain(session, period_col)
    if grain is None:
        _drop(candidate, "no_period_granularity", period_column=period_col.column_name)
        return None

    date_options = _event_date_options(session, event_table, table_ids, run_id)
    if len(date_options) != 1:
        _drop(
            candidate,
            "no_event_date" if not date_options else "ambiguous_event_date_header",
            headers=[h.table_name for _, h, _ in date_options if h is not None],
        )
        return None
    date_sql, header_table, join_on_sql = date_options[0]

    key_pairs = _entity_key_pairs(
        session, measure_table.table_id, event_table.table_id, table_ids, run_id
    )
    if len(key_pairs) != 1:
        _drop(
            candidate,
            "no_entity_key" if not key_pairs else "ambiguous_entity_key",
            pairs=[(m.column_name, e.column_name) for m, e in key_pairs],
        )
        return None
    key_measure, key_event = key_pairs[0]

    return _Alignment(
        measure_table=measure_table,
        measure_column=measure_column,
        key_measure=key_measure,
        key_event=key_event,
        bridge_measure_sql=f'CAST(m."{period_col.column_name}" AS DATE)',
        bridge_event_sql=f"CAST(date_trunc('{grain}', {date_sql}) AS DATE)",
        event_table=event_table,
        header_table=header_table,
        join_on_sql=join_on_sql,
    )


def _series_query(a: _Alignment, c: LineageCandidate) -> str:
    """The alignment query: one row per matched ``(entity, period)``.

    The measure table is aliased ``m``, the event (line) table ``e``, the
    optional header ``h``. The candidate's ``event_value_sql``/``event_filter_sql``
    use bare double-quoted columns; the binder resolves them across e/h and a
    genuine ambiguity fails the candidate loudly (logged drop), never silently.
    """
    event_join = f"JOIN {a.header_table.duckdb_path} h ON {a.join_on_sql}" if a.header_table else ""
    event_where = f"WHERE {c.event_filter_sql}" if c.event_filter_sql else ""
    return f"""
        WITH series AS (
            SELECT m."{a.key_measure.column_name}" AS k, {a.bridge_measure_sql} AS p,
                   m."{a.measure_column.column_name}" AS y
            FROM {a.measure_table.duckdb_path} m
        ),
        anchor AS (
            SELECT e."{a.key_event.column_name}" AS k, {a.bridge_event_sql} AS p,
                   SUM({c.event_value_sql}) AS m
            FROM {a.event_table.duckdb_path} e
            {event_join}
            {event_where}
            GROUP BY 1, 2
        )
        SELECT s.k, s.p, s.y, a.m,
               (SELECT COUNT(*) FROM series) AS n_series
        FROM series s
        JOIN anchor a ON s.k = a.k AND s.p = a.p
        WHERE s.y IS NOT NULL AND s.k IS NOT NULL AND s.p IS NOT NULL
        ORDER BY s.k, s.p
    """


def _fetch_series(
    duckdb_conn: duckdb.DuckDBPyConnection, alignment: _Alignment, candidate: LineageCandidate
) -> dict[str, tuple[list[float], list[float]]] | None:
    """Fetch aligned ``entity → (y, m)`` series; ``None`` (logged) = misaligned.

    Drops entities with duplicate ``(entity, period)`` rows (the measure is not
    at the derived grain) and the whole candidate when the join covers too
    little of the measure series.
    """
    try:
        rows = duckdb_conn.execute(_series_query(alignment, candidate)).fetchall()
    except Exception as e:  # the value/filter SQL is LLM-proposed — drop, don't crash
        _drop(candidate, "sql_failed", error=str(e))
        return None
    if not rows:
        _drop(candidate, "empty_join")
        return None
    n_series = int(rows[0][4])
    coverage = len(rows) / n_series if n_series else 0.0
    if coverage < MIN_JOIN_COVERAGE:
        _drop(candidate, "low_coverage", coverage=round(coverage, 3), gate=MIN_JOIN_COVERAGE)
        return None

    by_entity: dict[str, tuple[list[float], list[float]]] = {}
    seen: set[tuple[str, str]] = set()
    dup_entities: set[str] = set()
    for k, p, y, m, _ in rows:
        key = (str(k), str(p))
        if key in seen:
            dup_entities.add(str(k))
            continue
        seen.add(key)
        ys, ms = by_entity.setdefault(str(k), ([], []))
        ys.append(float(y))
        ms.append(float(m))
    for k in dup_entities:
        by_entity.pop(k, None)  # ambiguous grain for this entity — abstain
    if dup_entities:
        logger.info(
            "lineage_duplicate_grain_entities",
            measure=f"{candidate.measure_table}.{candidate.measure_column}",
            dropped_entities=len(dup_entities),
        )
    if not by_entity:
        _drop(candidate, "all_entities_duplicate_grain")
        return None
    return by_entity


def discover_aggregation_lineage(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    *,
    candidates: list[LineageCandidate],
    table_ids: list[str],
    session_id: str,
    run_id: str | None,
) -> int:
    """Dispose every candidate and persist the reconciled ones for this run.

    Idempotent per run: delete-before-insert on ``run_id`` (mirrors the
    detector post-step convention; the phase runs sequentially in the
    begin_session spine, no concurrent writers).

    Returns:
        The number of lineage rows persisted.
    """
    session.execute(
        delete(MeasureAggregationLineage).where(MeasureAggregationLineage.run_id == run_id)
    )

    persisted = 0
    for candidate in candidates:
        alignment = _derive_alignment(session, candidate, table_ids, run_id)
        if alignment is None:
            continue
        series = _fetch_series(duckdb_conn, alignment, candidate)
        if series is None:
            continue
        verdict: CandidateDisposal | None = dispose(series)
        if verdict is None:
            _drop(candidate, "dispose_abstained", entities=len(series))
            continue
        session.add(
            MeasureAggregationLineage(
                session_id=session_id,
                run_id=run_id,
                measure_table_id=alignment.measure_table.table_id,
                measure_column_id=alignment.measure_column.column_id,
                event_table_id=alignment.event_table.table_id,
                event_join_duckdb_path=(
                    alignment.header_table.duckdb_path if alignment.header_table else None
                ),
                event_join_on_sql=alignment.join_on_sql,
                event_value_sql=candidate.event_value_sql,
                measure_key_sql=f'"{alignment.key_measure.column_name}"',
                event_key_sql=f'"{alignment.key_event.column_name}"',
                measure_period_sql=alignment.bridge_measure_sql,
                event_period_sql=alignment.bridge_event_sql,
                event_filter_sql=candidate.event_filter_sql,
                pattern=verdict.pattern,
                match_rate=verdict.match_rate,
                r_flow_median=verdict.r_flow_median,
                r_stock_median=verdict.r_stock_median,
                n_entities=verdict.n_entities,
                n_entities_fired=verdict.n_entities_fired,
                rationale=candidate.rationale,
            )
        )
        persisted += 1
        logger.info(
            "lineage_reconciled",
            measure=f"{candidate.measure_table}.{candidate.measure_column}",
            event_table=candidate.event_table,
            header=alignment.header_table.table_name if alignment.header_table else None,
            pattern=verdict.pattern,
            match_rate=round(verdict.match_rate, 3),
        )
    return persisted
