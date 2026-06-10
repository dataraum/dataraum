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
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.slicing.naming import slice_table_prefix
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
    """One fact's per-(slice value, period) sums for one dimension."""

    table: Table
    # value -> period_label -> {column -> sum}
    sums: dict[str, dict[str, dict[str, float]]]
    numeric_columns: list[str]


def _slice_series(
    table: Table,
    dim_col: str,
    tsa_rows: list[TemporalSliceAnalysis],
) -> _SliceSeries | None:
    """Assemble the fact's slice series for one dimension from persisted rows."""
    prefix = slice_table_prefix(table.duckdb_path or "", dim_col)
    sums: dict[str, dict[str, dict[str, float]]] = {}
    columns: set[str] = set()
    for row in tsa_rows:
        if not row.slice_table_name.startswith(prefix):
            continue
        value = row.slice_table_name[len(prefix) :]
        per_value = sums.setdefault(value, {})
        per_value[row.period_label] = dict(row.column_sums or {})
        columns.update((row.column_sums or {}).keys())
    if not sums:
        return None
    return _SliceSeries(table=table, sums=sums, numeric_columns=sorted(columns))


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
    """Pair the two facts' series by (slice value, period_label)."""
    by_entity: dict[str, tuple[list[float], list[float]]] = {}
    for value in sorted(set(measure.sums) & set(event.sums)):
        m_periods = measure.sums[value]
        e_periods = event.sums[value]
        ys: list[float] = []
        ms: list[float] = []
        for label in sorted(set(m_periods) & set(e_periods)):
            y = m_periods[label].get(measure_col)
            m = _convention_value(e_periods[label], terms)
            if y is None or m is None:
                continue
            ys.append(y)
            ms.append(m)
        if ys:
            by_entity[value] = (ys, ms)
    return by_entity


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
    session.execute(
        delete(MeasureAggregationLineage).where(MeasureAggregationLineage.run_id == run_id)
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
    tables_by_dim: dict[str, set[str]] = {}
    for sd in defs:
        name = sd.column_name or ""
        if name:
            tables_by_dim.setdefault(name, set()).add(sd.table_id)
    shared_dims = {d: tids for d, tids in tables_by_dim.items() if len(tids) >= 2}
    if not shared_dims:
        logger.info("lineage_no_shared_dimension", dimensions=sorted(tables_by_dim))
        return 0

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
            for tid in shared_dims[dim_col]
            if (t := tables.get(tid)) is not None
            and (s := _slice_series(t, dim_col, list(tsa_rows))) is not None
        }
        if len(series_by_table) < 2:
            logger.info("lineage_no_slice_series", dimension=dim_col)
            continue

        for m_tid, measure in sorted(series_by_table.items()):
            measure_cols = [
                c
                for c in measure.numeric_columns
                if c in columns_by_table.get(m_tid, {})  # persistable, not an enriched name
            ]
            for e_tid, event in sorted(series_by_table.items()):
                if e_tid == m_tid:
                    continue
                for measure_col in measure_cols:
                    for convention_sql, terms in _conventions(event.numeric_columns):
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
