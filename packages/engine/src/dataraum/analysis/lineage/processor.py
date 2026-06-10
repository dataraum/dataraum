"""Aggregation-lineage discovery orchestration (DAT-491).

Per candidate: build the alignment query in DuckDB (series ``y`` per
``(entity, period)`` from the measure table; independent anchor ``m`` =
per-period SUM of the event expression), reconcile deterministically
(:mod:`dataraum.analysis.lineage.reconcile`), and persist one run-versioned
``MeasureAggregationLineage`` row per RECONCILED candidate. A candidate whose
alignment fails (bad SQL, empty join, duplicate grain, split vote) is dropped —
the witness downstream abstains, it never guesses.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.lineage.models import CandidateDisposal, LineageCandidate
from dataraum.analysis.lineage.reconcile import dispose
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# A candidate whose join covers less than this fraction of the measure's
# (entity, period) rows is misaligned (wrong key or period bridge) — drop it
# before reconciling. Coverage, not correctness: the residual gate still rules.
MIN_JOIN_COVERAGE = 0.5


def _series_query(c: LineageCandidate) -> str:
    """The alignment query: one row per matched ``(entity, period)``.

    The event (line) table is always aliased ``e`` so bare-column expressions
    keep resolving; an optional header join (``h``) covers the split
    header/line shape where the event date lives one table away.
    """
    event_where = f"WHERE {c.event_filter_sql}" if c.event_filter_sql else ""
    event_join = (
        f"JOIN {c.event_join_duckdb_path} h ON {c.event_join_on_sql}"
        if c.event_join_duckdb_path and c.event_join_on_sql
        else ""
    )
    return f"""
        WITH series AS (
            SELECT {c.measure_key_sql} AS k, {c.measure_period_sql} AS p,
                   "{c.measure_column}" AS y
            FROM {c.measure_duckdb_path}
        ),
        anchor AS (
            SELECT {c.event_key_sql} AS k, {c.event_period_sql} AS p,
                   SUM({c.event_value_sql}) AS m
            FROM {c.event_duckdb_path} e
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
    duckdb_conn: duckdb.DuckDBPyConnection, candidate: LineageCandidate
) -> dict[str, tuple[list[float], list[float]]] | None:
    """Fetch aligned ``entity → (y, m)`` series; ``None`` = misaligned candidate.

    Drops entities with duplicate ``(entity, period)`` rows (the measure is not
    at the proposed grain) and the whole candidate when the join covers too
    little of the measure series.
    """
    try:
        rows = duckdb_conn.execute(_series_query(candidate)).fetchall()
    except Exception as e:  # LLM-proposed SQL — failure is a dropped candidate, not a crash
        logger.info(
            "lineage_candidate_sql_failed",
            measure=f"{candidate.measure_table}.{candidate.measure_column}",
            error=str(e),
        )
        return None
    if not rows:
        return None
    n_series = int(rows[0][4])
    if n_series == 0 or len(rows) / n_series < MIN_JOIN_COVERAGE:
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
    return by_entity or None


def _column_id(
    session: Session, table_ids: list[str], table_name: str, column_name: str
) -> tuple[str, str] | None:
    """Resolve ``(table_id, column_id)`` within the session's scope by name."""
    row = session.execute(
        select(Table.table_id, Column.column_id)
        .join(Column, Column.table_id == Table.table_id)
        .where(
            Table.table_id.in_(table_ids),
            Table.table_name == table_name,
            Column.column_name == column_name,
        )
    ).first()
    return (row.table_id, row.column_id) if row else None


def _table_id(session: Session, table_ids: list[str], table_name: str) -> str | None:
    row = session.execute(
        select(Table.table_id).where(Table.table_id.in_(table_ids), Table.table_name == table_name)
    ).first()
    return row.table_id if row else None


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
        measure = _column_id(session, table_ids, candidate.measure_table, candidate.measure_column)
        event_table_id = _table_id(session, table_ids, candidate.event_table)
        if measure is None or event_table_id is None:
            logger.info(
                "lineage_candidate_unknown_target",
                measure=f"{candidate.measure_table}.{candidate.measure_column}",
                event_table=candidate.event_table,
            )
            continue
        series = _fetch_series(duckdb_conn, candidate)
        if series is None:
            continue
        verdict: CandidateDisposal | None = dispose(series)
        if verdict is None:
            continue
        measure_table_id, measure_column_id = measure
        session.add(
            MeasureAggregationLineage(
                session_id=session_id,
                run_id=run_id,
                measure_table_id=measure_table_id,
                measure_column_id=measure_column_id,
                event_table_id=event_table_id,
                event_join_duckdb_path=candidate.event_join_duckdb_path,
                event_join_on_sql=candidate.event_join_on_sql,
                event_value_sql=candidate.event_value_sql,
                measure_key_sql=candidate.measure_key_sql,
                event_key_sql=candidate.event_key_sql,
                measure_period_sql=candidate.measure_period_sql,
                event_period_sql=candidate.event_period_sql,
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
            pattern=verdict.pattern,
            match_rate=round(verdict.match_rate, 3),
        )
    return persisted
