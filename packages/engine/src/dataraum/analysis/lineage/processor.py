"""Aggregation-lineage discovery over the slice substrate (DAT-491).

No LLM call and no SQL assembly: the begin_session value layer already built
everything this needs. The slicing agent partitioned the facts by shared
dimensions (propagated across tables) and named each table's time axis;
temporal slice analysis segmented every slice by calendar period and persisted
per-period SUMs of each numeric column (``TemporalSliceAnalysis.column_sums``).

Discovery is arithmetic over those stored numbers: for every dimension shared
by two facts, pair the slice series by (slice value, period_label), enumerate
the signed conventions (each numeric column, and ordered pair differences like
``debit − credit`` — sums are linear, so conventions distribute over them),
and let the deterministic reconciliation statistic
(:mod:`dataraum.analysis.lineage.reconcile`) dispose every pairing. A wrong
pairing lands at residual ≈ 1 and abstains (probe margins: true ≈ 0.02 vs
wrong-anchor ≈ 1.0); the best reconciling verdict per measure column persists
as one run-versioned ``MeasureAggregationLineage`` row.

Every abstention logs its stage (``lineage_candidate_dropped``) — visible,
never silent.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import delete, select

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.lineage.models import CandidateDisposal
from dataraum.analysis.lineage.reconcile import dispose
from dataraum.analysis.relationships.utils import load_defined_relationships
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.naming import slice_table_name
from dataraum.analysis.temporal_slicing.db_models import TemporalSliceAnalysis
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# Per-table-pair convention budget: numeric columns n yields n singles plus
# n·(n−1) ordered differences. Wide event facts are capped (sorted column
# names, deterministic) — and the cap is LOGGED, never silent.
MAX_CONVENTION_COLUMNS = 8


@dataclass(frozen=True)
class _SliceSeries:
    """One fact's per-(slice value, period) sums + row counts for one dimension."""

    table: Table
    # value -> period_label -> {column -> sum}
    sums: dict[str, dict[str, dict[str, float]]]
    # value -> period_label -> row_count (0 = measured empty period)
    rows: dict[str, dict[str, int]]
    numeric_columns: list[str]


def _slice_series(
    table: Table,
    slice_def: SliceDefinition,
    dim_col: str,
    tsa_rows: list[TemporalSliceAnalysis],
) -> _SliceSeries | None:
    """Assemble the fact's slice series for one dimension from persisted rows.

    Slice tables are matched by EXACT reconstructed name per declared value
    (``slice_table_name``), never by prefix scan — sanitized prefixes are not
    prefix-free (``account`` vs ``account_type``), and a prefix match would
    merge two different partitions into one vote-contaminated series.
    """
    name_to_value = {
        slice_table_name(table.duckdb_path or "", dim_col, v): str(v)
        for v in (slice_def.distinct_values or [])
    }
    sums: dict[str, dict[str, dict[str, float]]] = {}
    rows: dict[str, dict[str, int]] = {}
    columns: set[str] = set()
    for row in tsa_rows:
        value = name_to_value.get(row.slice_table_name.lower())
        if value is None:
            continue
        sums.setdefault(value, {})[row.period_label] = dict(row.column_sums or {})
        rows.setdefault(value, {})[row.period_label] = int(row.row_count or 0)
        columns.update((row.column_sums or {}).keys())
    if not sums:
        return None
    return _SliceSeries(table=table, sums=sums, rows=rows, numeric_columns=sorted(columns))


def _conventions(columns: list[str]) -> list[tuple[str, tuple[str, ...]]]:
    """The signed-convention hypotheses: ``(sql_text, column_terms)``.

    Singles read as ``+col``; ordered pairs as ``a − b``. Evaluated as
    arithmetic over the stored per-period sums (linearity of SUM).
    """
    cols = columns[:MAX_CONVENTION_COLUMNS]
    if len(columns) > MAX_CONVENTION_COLUMNS:
        logger.info(
            "lineage_convention_columns_capped",
            kept=cols,
            dropped=columns[MAX_CONVENTION_COLUMNS:],
        )
    out: list[tuple[str, tuple[str, ...]]] = [(f'"{c}"', (c,)) for c in cols]
    out.extend((f'"{a}" - "{b}"', (a, b)) for a in cols for b in cols if a != b)
    return out


def _convention_value(period_sums: dict[str, float], terms: tuple[str, ...]) -> float | None:
    """Evaluate one convention for one period; ``None`` when a term is absent."""
    if any(t not in period_sums for t in terms):
        return None
    if len(terms) == 1:
        return period_sums[terms[0]]
    return period_sums[terms[0]] - period_sums[terms[1]]


def _aligned_series(
    measure: _SliceSeries,
    event: _SliceSeries,
    measure_col: str,
    terms: tuple[str, ...],
) -> dict[str, tuple[list[float], list[float]]]:
    """Pair the two facts' series by (slice value, period_label).

    An event period persisted with ``row_count == 0`` is MEASURED zero
    movement (the strongest stock evidence — the level did not change), not
    missing data: conventions evaluate to 0.0 there. A measure-side period
    without a value is missing data and drops.
    """
    by_entity: dict[str, tuple[list[float], list[float]]] = {}
    for value in sorted(set(measure.sums) & set(event.sums)):
        m_periods = measure.sums[value]
        e_periods = event.sums[value]
        e_rows = event.rows.get(value, {})
        ys: list[float] = []
        ms: list[float] = []
        for label in sorted(set(m_periods) & set(e_periods)):
            y = m_periods[label].get(measure_col)
            m = _convention_value(e_periods[label], terms)
            if m is None and e_rows.get(label) == 0:
                m = 0.0
            if y is None or m is None:
                continue
            ys.append(y)
            ms.append(m)
        if ys:
            by_entity[value] = (ys, ms)
    return by_entity


def _paired_row_counts(measure: _SliceSeries, event: _SliceSeries) -> tuple[int, int]:
    """Total row counts over the shared (value, period) cells of a pairing."""
    m_total = e_total = 0
    for value in set(measure.rows) & set(event.rows):
        m_periods, e_periods = measure.rows[value], event.rows[value]
        for label in set(m_periods) & set(e_periods):
            m_total += m_periods[label]
            e_total += e_periods[label]
    return m_total, e_total


@dataclass(frozen=True)
class _Best:
    verdict: CandidateDisposal
    event_table: Table
    convention_sql: str
    winning_residual: float


def discover_aggregation_lineage(
    session: Session,
    *,
    table_ids: list[str],
    session_id: str,
    run_id: str | None,
    period_grain: str = "monthly",
) -> int:
    """Reconcile every shared-dimension fact pair and persist the verdicts.

    Idempotent per run: delete-before-insert on ``run_id``. One row per
    measure column — the best reconciling (event table, convention) by
    winning residual.

    Returns:
        The number of lineage rows persisted.
    """
    if run_id is None:
        raise ValueError("discover_aggregation_lineage requires a run_id (run-versioned rows)")
    session.execute(
        delete(MeasureAggregationLineage).where(
            MeasureAggregationLineage.session_id == session_id,
            MeasureAggregationLineage.run_id == run_id,
        )
    )

    tables = {
        t.table_id: t
        for t in session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars()
    }
    # This run's slice dimensions, grouped by dimension column name.
    defs = (
        session.execute(
            select(SliceDefinition).where(
                SliceDefinition.table_id.in_(table_ids),
                SliceDefinition.run_id == run_id,
            )
        )
        .scalars()
        .all()
    )
    defs_by_dim: dict[str, dict[str, SliceDefinition]] = {}
    for sd in defs:
        name = sd.column_name or ""
        if name:
            defs_by_dim.setdefault(name, {})[sd.table_id] = sd
    shared_dims = {d: by_table for d, by_table in defs_by_dim.items() if len(by_table) >= 2}
    if not shared_dims:
        logger.info("lineage_no_shared_dimension", dimensions=sorted(defs_by_dim))
        return 0

    # Key/identifier columns are not quantities: a SUM over a key has no
    # meaning, and identical key sets reconcile trivially (identity noise).
    # Grounded in the catalog: every endpoint of a defined relationship is a
    # key — excluded from measure columns AND convention terms.
    key_columns_by_table: dict[str, set[str]] = {}
    for rel in load_defined_relationships(
        session, table_ids, run_id=run_id, both_tables=False, eager_columns=True
    ):
        key_columns_by_table.setdefault(rel.from_table_id, set()).add(rel.from_column.column_name)
        key_columns_by_table.setdefault(rel.to_table_id, set()).add(rel.to_column.column_name)

    tsa_rows = (
        session.execute(
            select(TemporalSliceAnalysis).where(
                TemporalSliceAnalysis.session_id == session_id,
                TemporalSliceAnalysis.run_id == run_id,
            )
        )
        .scalars()
        .all()
    )

    # column name -> Column row per table, to persist measure_column_id.
    columns_by_table: dict[str, dict[str, Column]] = {}
    for col in session.execute(select(Column).where(Column.table_id.in_(table_ids))).scalars():
        columns_by_table.setdefault(col.table_id, {})[col.column_name] = col

    # Best verdict per measure column across dimensions, event tables, conventions.
    best_by_measure: dict[str, tuple[_Best, Table, str, str]] = {}

    for dim_col in sorted(shared_dims):
        series_by_table = {
            tid: s
            for tid, sd in shared_dims[dim_col].items()
            if (t := tables.get(tid)) is not None
            and (s := _slice_series(t, sd, dim_col, list(tsa_rows))) is not None
        }
        if len(series_by_table) < 2:
            logger.info("lineage_no_slice_series", dimension=dim_col)
            continue
        conventions_by_table = {
            tid: _conventions(
                [c for c in s.numeric_columns if c not in key_columns_by_table.get(tid, set())]
            )
            for tid, s in series_by_table.items()
        }

        for m_tid, measure in sorted(series_by_table.items()):
            keys_m = key_columns_by_table.get(m_tid, set())
            measure_cols = [
                c
                for c in measure.numeric_columns
                if c not in keys_m
                and c in columns_by_table.get(m_tid, {})  # persistable, not an enriched name
            ]
            for e_tid, event in sorted(series_by_table.items()):
                if e_tid == m_tid:
                    continue
                # Direction gate: a rollup aggregates MANY event rows into each
                # measure cell — the event side must be strictly finer-grained
                # over the paired cells. Symmetric arithmetic would otherwise
                # persist inverted lineage (the measure "aggregating" its own
                # summary), and equal-grain pairs (1:1 mirrors) are
                # relationships, not rollups.
                m_rows, e_rows = _paired_row_counts(measure, event)
                if e_rows <= m_rows:
                    logger.info(
                        "lineage_direction_gate",
                        dimension=dim_col,
                        measure_table=measure.table.table_name,
                        event_table=event.table.table_name,
                        measure_rows=m_rows,
                        event_rows=e_rows,
                    )
                    continue
                for measure_col in measure_cols:
                    for convention_sql, terms in conventions_by_table[e_tid]:
                        by_entity = _aligned_series(measure, event, measure_col, terms)
                        if not by_entity:
                            continue
                        verdict = dispose(by_entity)
                        if verdict is None:
                            continue
                        residual = (
                            verdict.r_flow_median
                            if verdict.pattern == "per_period"
                            else verdict.r_stock_median
                        )
                        prior = best_by_measure.get(columns_by_table[m_tid][measure_col].column_id)
                        if prior is None or residual < prior[0].winning_residual:
                            best_by_measure[columns_by_table[m_tid][measure_col].column_id] = (
                                _Best(
                                    verdict=verdict,
                                    event_table=event.table,
                                    convention_sql=convention_sql,
                                    winning_residual=residual,
                                ),
                                measure.table,
                                measure_col,
                                dim_col,
                            )

    persisted = 0
    for measure_column_id, (best, m_table, m_col, dim_col) in best_by_measure.items():
        session.add(
            MeasureAggregationLineage(
                session_id=session_id,
                run_id=run_id,
                measure_table_id=m_table.table_id,
                measure_column_id=measure_column_id,
                event_table_id=best.event_table.table_id,
                slice_dimension=dim_col,
                convention_sql=best.convention_sql,
                period_grain=period_grain,
                pattern=best.verdict.pattern,
                match_rate=best.verdict.match_rate,
                r_flow_median=best.verdict.r_flow_median,
                r_stock_median=best.verdict.r_stock_median,
                n_entities=best.verdict.n_entities,
                n_entities_fired=best.verdict.n_entities_fired,
            )
        )
        persisted += 1
        logger.info(
            "lineage_reconciled",
            measure=f"{m_table.table_name}.{m_col}",
            event_table=best.event_table.table_name,
            dimension=dim_col,
            convention=best.convention_sql,
            pattern=best.verdict.pattern,
            match_rate=round(best.verdict.match_rate, 3),
        )
    return persisted
