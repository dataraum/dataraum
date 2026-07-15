"""Aggregation-lineage discovery over the enriched-view substrate (DAT-491/536).

No LLM call and no slice materialization: the begin_session value layer already
declared everything this needs. The slicing agent partitioned the facts by
shared dimensions (the catalog ``SliceDefinition``, propagated across tables)
and named each fact's event-time axes (``TableEntity.time_columns``); every axis
is competed and the best-reconciling verdict per measure is kept (DAT-565).

Discovery aggregates **inline** (DAT-536, one-view model): for each fact × shared
dimension, a single ``GROUP BY dim, period`` over the fact's enriched view —
keyed to the catalog's declared values, summing the fact's own numeric columns —
yields the per-(slice value, period) row counts + sums the reconciliation needs.
(This replaced the slice→``TemporalSliceAnalysis`` substrate; the re-point is
verdict-preserving — ``tests/unit/analysis/lineage/test_processor.py`` seeds a
DuckDB fixture and asserts the same reconciliation verdicts.) It then pairs the series by (slice
value, period_label), enumerates the signed conventions (each numeric column,
and ordered pair differences like ``debit − credit`` — sums are linear, so
conventions distribute over them), and lets the deterministic reconciliation
statistic (:mod:`dataraum.analysis.lineage.reconcile`) dispose every pairing. A
wrong pairing lands at residual ≈ 1 and abstains (probe margins: true ≈ 0.02 vs
wrong-anchor ≈ 1.0).

Convention selection is SUPPORT-FIRST (DAT-759): candidates are ranked by the
Wilson lower bound of their vote rate over the pairing's COMMON entity
denominator, LCB ties break to the lower arity unless the difference wins by
ΔBIC > 10 (Kass–Raftery), then by median residual. Minimum-residual selection
was the prior criterion and is monotone under search freedom — the ordered
differences structurally out-raced true singles (``debit − net_amount ≈ credit``
beat ``debit`` on a half-entity subset), persisting value-wrong ``convention_sql``
into the property-graph grounding. Grounded in the eval probe
``scripts/probes/dat759-convention-selection`` (truth 3/3, margins 0.345–0.620
LCB). The best candidate per measure column persists as one run-versioned
``MeasureAggregationLineage`` row.

Every abstention logs its stage (``lineage_no_shared_dimension`` /
``lineage_no_slice_series`` / ``lineage_direction_gate`` /
``inline_slice_series_failed`` / ``lineage_convention_columns_capped``) —
visible, never silent; per-convention abstentions inside a live pairing are
the search doing its job and stay quiet.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.analysis.lineage.db_models import MeasureAggregationLineage
from dataraum.analysis.lineage.models import CandidateDisposal
from dataraum.analysis.lineage.reconcile import (
    MIN_PERIODS,
    classify_series,
    dispose_classified,
    wilson_lcb,
)
from dataraum.analysis.relationships.utils import load_defined_relationships
from dataraum.analysis.semantic.db_models import TableEntity
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# Per-table-pair convention budget: numeric columns n yields n singles plus
# n·(n−1) ordered differences. Wide event facts are capped (sorted column
# names, deterministic) — and the cap is LOGGED, never silent.
MAX_CONVENTION_COLUMNS = 8


# DuckDB ``date_trunc`` unit + ``strftime`` label per grain — the cross-fact
# alignment key (ISO semantics; stable across facts of the same grain). Mirrors
# the retired ``temporal_slicing`` analyzer so period labels are unchanged.
_GRAIN_SQL: dict[str, tuple[str, str]] = {
    "daily": ("day", "%Y-%m-%d"),
    "weekly": ("week", "%G-W%V"),
    "monthly": ("month", "%Y-%m"),
}

_NUMERIC_TYPES = frozenset(
    {"TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "FLOAT", "DOUBLE", "DECIMAL"}
)


def _is_numeric(resolved_type: str | None) -> bool:
    """A column's resolved type is a summable numeric (ignores DECIMAL precision)."""
    return (
        resolved_type is not None and resolved_type.split("(")[0].strip().upper() in _NUMERIC_TYPES
    )


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
    duckdb_conn: duckdb.DuckDBPyConnection,
    table: Table,
    slice_def: SliceDefinition,
    dim_col: str,
    *,
    source_name: str | None,
    time_col: str | None,
    numeric_cols: list[str],
    grain: str,
) -> _SliceSeries | None:
    """Assemble the fact's per-(value, period) series by inline aggregation.

    One ``GROUP BY dim, period`` over the fact's enriched view (DAT-536), keyed
    to the catalog's declared values and summing the fact's own numeric columns
    — the path-independent replacement for the slice→``TemporalSliceAnalysis``
    substrate (verdict-equivalence proven in ``test_processor.py``).
    Returns ``None`` when the fact lacks a queryable source, a time axis,
    declared values, or numeric columns: the witness simply cannot fire on it —
    a visible abstention, never a silent guess.
    """
    values = [str(v) for v in (slice_def.distinct_values or [])]
    if not (source_name and time_col and numeric_cols and values):
        return None
    unit, label_fmt = _GRAIN_SQL.get(grain, _GRAIN_SQL["monthly"])
    sum_parts = "".join(f', SUM("{c}") AS s{i}' for i, c in enumerate(numeric_cols))
    values_sql = ", ".join("'" + v.replace("'", "''") + "'" for v in values)
    sql = f"""
        SELECT "{dim_col}" AS dim_value,
            CAST(date_trunc('{unit}', CAST("{time_col}" AS DATE)) AS DATE) AS period_start,
            COUNT(*) AS row_count{sum_parts}
        FROM "{source_name}"
        WHERE "{time_col}" IS NOT NULL AND "{dim_col}" IN ({values_sql})
        GROUP BY 1, 2
    """
    try:
        result_rows = duckdb_conn.execute(sql).fetchall()
    except Exception as e:
        logger.warning(
            "inline_slice_series_failed", table=table.table_name, dimension=dim_col, error=str(e)
        )
        return None
    sums: dict[str, dict[str, dict[str, float]]] = {}
    rows: dict[str, dict[str, int]] = {}
    columns: set[str] = set()
    for r in result_rows:
        value = str(r[0])
        label = r[1].strftime(label_fmt)
        period_sums = {
            col: float(r[3 + i]) for i, col in enumerate(numeric_cols) if r[3 + i] is not None
        }
        sums.setdefault(value, {})[label] = period_sums
        rows.setdefault(value, {})[label] = int(r[2])
        columns.update(period_sums.keys())
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


def _pairing_universe(measure: _SliceSeries, event: _SliceSeries, measure_col: str) -> int:
    """The COMMON entity denominator for one (measure, event) pairing (DAT-759).

    Entities whose measure series is evaluable at all — ≥ ``MIN_PERIODS`` aligned
    periods carrying the measure value, not identically zero — independent of any
    convention's terms. Vote rates are comparable across the family only on this
    shared denominator: a convention whose terms are absent on some entities must
    NOT get a shrunken denominator and a flattered rate (the support-gameability
    trap, probe leg b2 — own-subset LCB 0.722 vs common 0.299 on the same votes).
    """
    n = 0
    for value in measure.sums.keys() & event.sums.keys():
        m_periods = measure.sums[value]
        ys = [
            m_periods[label][measure_col]
            for label in m_periods.keys() & event.sums[value].keys()
            if measure_col in m_periods[label]
        ]
        if len(ys) >= MIN_PERIODS and any(ys):
            n += 1
    return n


@dataclass(frozen=True)
class _Best:
    verdict: CandidateDisposal
    event_table: Table
    convention_sql: str
    winning_residual: float  # median winning-pattern voter residual
    support_lcb: float  # Wilson LCB of voters over the pairing's common denominator
    arity: int  # convention terms: 1 = single, 2 = ordered difference
    voter_residuals: tuple[float, ...]  # winning-pattern per-entity residuals (ΔBIC)


def _bic(candidate: _Best) -> float:
    """Schwarz BIC over the pooled winning-voter residual mass; ``k`` = arity.

    Winning-pattern voters only — consistent with the verdict's medians (the
    grounding probe pooled ALL voters; immaterial where it matters, since
    collinear twins share voter sets). The 1e-12 RSS floor means two exact-fit
    candidates with different voter counts compare through the floor, not
    evidence — reachable only on a cross-pairing exact-LCB tie, accepted.
    """
    n = len(candidate.voter_residuals)
    if n == 0:
        return float("inf")
    rss = max(sum(r * r for r in candidate.voter_residuals), 1e-12)
    return n * math.log(rss / n) + candidate.arity * math.log(n)


def _better(challenger: _Best, incumbent: _Best) -> bool:
    """DAT-759 selection order: support, then description length, then residual.

    1. Higher Wilson LCB wins — breadth of reconciling entities is the
       generalization estimate, not residual depth on a subset. Support counts
       every reconciling voter (a ≤20% pattern-dissenting minority included):
       support means "this convention reconciles the entity", not "votes my
       pattern".
    2. On an LCB tie across arities, the single wins unless the difference is
       very strongly better (ΔBIC > 10, Kass–Raftery) — exact collinear twins
       (``debit − net_amount ≡ credit``) are numerically identical, so only
       description length can order them. ΔBIC is statistically meaningful
       within one pairing (same entities, same periods); a cross-pairing exact
       LCB tie compares different data and the step degrades to a heuristic.
    3. Same arity: lower median residual.

    Pairwise, not a strict weak order: mixing BIC (step 2) with median residual
    (step 3) admits rare tie-surface cycles, so the streaming argmax is defined
    by enumeration order — which is fully sorted (identities, tables, axes,
    conventions) and therefore deterministic per run and per re-delivery.
    """
    if challenger.support_lcb != incumbent.support_lcb:
        return challenger.support_lcb > incumbent.support_lcb
    if challenger.arity != incumbent.arity:
        single, difference = (
            (incumbent, challenger)
            if challenger.arity > incumbent.arity
            else (challenger, incumbent)
        )
        difference_wins = _bic(single) - _bic(difference) > 10.0
        return difference_wins == (challenger is difference)
    return challenger.winning_residual < incumbent.winning_residual


def discover_aggregation_lineage(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str,
    period_grain: str = "monthly",
) -> int:
    """Reconcile every shared-dimension fact pair and persist the verdicts.

    Idempotent per run — form-(a) writer (DAT-502): one row per measure
    column (the best reconciling event table + convention by support-first
    selection, see :func:`_better`), UPSERTed on ``uq_measure_lineage_column_run``. A Temporal
    success-redelivery (same ``run_id``) converges in place; prior runs'
    rows stay untouched. Deterministic SQL producer: the recomputed batch
    is the same verdict set, so no run-scoped clear is needed.

    Returns:
        The number of lineage rows persisted.
    """
    tables = {
        t.table_id: t
        for t in session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars()
    }
    # This run's slice dimensions (grouped by referenced identity below).
    # Deterministic order: candidate enumeration inherits it, and the streaming
    # argmax in ``_better`` resolves exact ties by that order — an unordered
    # scan could let a Temporal success-redelivery pick a different equally-
    # ranked verdict, breaking the deterministic-producer claim above.
    defs = (
        session.execute(
            select(SliceDefinition)
            .where(
                SliceDefinition.table_id.in_(table_ids),
                SliceDefinition.run_id == run_id,
            )
            .order_by(SliceDefinition.slice_priority, SliceDefinition.column_name)
        )
        .scalars()
        .all()
    )
    # Group by the slice's REFERENCED-dimension identity (DAT-756), not its
    # ``column_name``. Two facts share a dimension iff they reference the SAME
    # dim table at the SAME attribute — so the same conformed dimension reached
    # via differently-named FK columns is paired (the false-negative that
    # silently disabled this witness), and two unrelated same-named FOLDED columns
    # are not (the false-positive). A folded slice (null ``dimension_table_id``)
    # has no cross-table identity in Phase A and abstains (DAT-757).
    # ``{table -> [slices]}`` per identity: one fact can carry MULTIPLE slices at the
    # same identity — role-playing FKs to one dim (``kontonummer`` vs
    # ``kontonummer_des_gegenkontos``, both -> accounts at ``land``). Keep them all
    # (a list, not last-write-wins): each is a distinct bucketing lens the search
    # competes; dropping one would silently lose a reconciliation candidate. Role
    # disambiguation itself (bill-to vs ship-to as SEPARATE dimensions) is DAT-757 —
    # here they share the Phase-A identity and are simply both tried.
    defs_by_dim: dict[tuple[str, str], dict[str, list[SliceDefinition]]] = {}
    for sd in defs:
        if not sd.dimension_table_id:
            continue
        identity = (sd.dimension_table_id, sd.dimension_attribute or "")
        defs_by_dim.setdefault(identity, {}).setdefault(sd.table_id, []).append(sd)
    shared_dims = {ident: by_table for ident, by_table in defs_by_dim.items() if len(by_table) >= 2}
    if not shared_dims:
        logger.info("lineage_no_shared_dimension", identities=sorted(defs_by_dim))
        return 0

    # Readable label per identity for the persisted ``slice_dimension`` + logs:
    # ``<dim table>.<attribute>`` (the conformed axis), not a per-fact column name
    # (which now differs across the paired facts).
    dim_names = {
        t.table_id: t.table_name
        for t in session.execute(
            select(Table).where(Table.table_id.in_({ident[0] for ident in shared_dims}))
        ).scalars()
    }
    labels = {
        ident: (
            f"{dim_names.get(ident[0], ident[0])}.{ident[1]}"
            if ident[1]
            else dim_names.get(ident[0], ident[0])
        )
        for ident in shared_dims
    }

    # Key/identifier columns are not quantities: a SUM over a key has no
    # meaning, and identical key sets reconcile trivially (identity noise).
    # Grounded in the catalog: every endpoint of a defined relationship is a key
    # — excluded from measure columns AND convention terms. "Defined" is now
    # judge-CONFIRMED at the source (DAT-722: a declined verdict is persisted as a
    # ``candidate``, never ``llm``), so this consumer trusts the catalog and does
    # NOT re-weigh confidence — one threshold lives at the source, not mirrored here.
    key_columns_by_table: dict[str, set[str]] = {}
    for rel in load_defined_relationships(
        session, table_ids, run_id=run_id, both_tables=False, eager_columns=True
    ):
        key_columns_by_table.setdefault(rel.from_table_id, set()).add(rel.from_column.column_name)
        key_columns_by_table.setdefault(rel.to_table_id, set()).add(rel.to_column.column_name)

    # column name -> Column row per table, to persist measure_column_id.
    columns_by_table: dict[str, dict[str, Column]] = {}
    for col in session.execute(select(Column).where(Column.table_id.in_(table_ids))).scalars():
        columns_by_table.setdefault(col.table_id, {})[col.column_name] = col

    # Inline-aggregation inputs per fact (DAT-536): the queryable source (the
    # grain-verified enriched view, else the typed fact), the agent-named time
    # axes (run-scoped ``TableEntity.time_columns``), and the fact's own numeric
    # columns (the SUM targets — the same set the retired slice path summed).
    enriched_by_fact = {
        ev.fact_table_id: ev.view_name
        for ev in session.execute(
            select(EnrichedView).where(
                EnrichedView.fact_table_id.in_(table_ids),
                EnrichedView.is_grain_verified.is_(True),
            )
        ).scalars()
    }
    source_by_table = {
        tid: enriched_by_fact.get(tid) or (t.duckdb_path or None) for tid, t in tables.items()
    }
    # Every event-time axis per table (DAT-565): each named time column is a
    # distinct temporal lens. The reconciliation competes all of them and keeps
    # the best-reconciling verdict per measure (revenue-by-order-date vs
    # by-ship-date genuinely differ); the persisted grain is unchanged — still
    # one best row per ``(measure_column, run)``.
    time_entity_stmt = select(TableEntity).where(TableEntity.table_id.in_(table_ids))
    if run_id is not None:
        time_entity_stmt = time_entity_stmt.where(TableEntity.run_id == run_id)
    time_cols_by_table: dict[str, list[str]] = {}
    for entity in session.execute(time_entity_stmt).scalars():
        axes = [tc["column"] for tc in (entity.time_columns or []) if tc.get("column")]
        if axes:
            time_cols_by_table[entity.table_id] = axes
    numeric_cols_by_table = {
        tid: sorted(name for name, col in by_name.items() if _is_numeric(col.resolved_type))
        for tid, by_name in columns_by_table.items()
    }

    # Best verdict per measure column across dimensions, event tables, conventions.
    best_by_measure: dict[str, tuple[_Best, Table, str, str]] = {}

    for identity in sorted(shared_dims):
        slice_label = labels[identity]
        # One series per (table, time-axis): each event-time column the catalog
        # named for the table is a distinct lens to bucket by (DAT-565). A table
        # contributes a series for every axis; the search below competes them.
        # Each fact groups by its OWN physical slice column (``sd.column_name``) —
        # the shared identity may be reached via differently-named columns (DAT-756),
        # while the VALUE domain is common, so ``_aligned_series`` still pairs them.
        series_by_table: dict[str, list[tuple[str, _SliceSeries]]] = {}
        for tid, sds in shared_dims[identity].items():
            t = tables.get(tid)
            if t is None:
                continue
            # Every (role-playing slice × time axis) is a distinct lens to bucket by.
            axis_series: list[tuple[str, _SliceSeries]] = []
            for sd in sds:
                for axis in time_cols_by_table.get(tid, []):
                    s = _slice_series(
                        duckdb_conn,
                        t,
                        sd,
                        sd.column_name or "",
                        source_name=source_by_table.get(tid),
                        time_col=axis,
                        numeric_cols=numeric_cols_by_table.get(tid, []),
                        grain=period_grain,
                    )
                    if s is not None:
                        axis_series.append((axis, s))
            if axis_series:
                series_by_table[tid] = axis_series
        if len(series_by_table) < 2:
            logger.info("lineage_no_slice_series", dimension=slice_label)
            continue

        for m_tid, m_axis_series in sorted(series_by_table.items()):
            keys_m = key_columns_by_table.get(m_tid, set())
            for _m_axis, measure in m_axis_series:
                measure_cols = [
                    c
                    for c in measure.numeric_columns
                    if c not in keys_m
                    and c in columns_by_table.get(m_tid, {})  # persistable, not enriched
                ]
                if not measure_cols:
                    continue
                for e_tid, e_axis_series in sorted(series_by_table.items()):
                    if e_tid == m_tid:
                        continue
                    keys_e = key_columns_by_table.get(e_tid, set())
                    for _e_axis, event in e_axis_series:
                        # Direction gate: a rollup aggregates MANY event rows into
                        # each measure cell — the event side must be strictly
                        # finer-grained over the paired cells. Symmetric arithmetic
                        # would otherwise persist inverted lineage (the measure
                        # "aggregating" its own summary), and equal-grain pairs
                        # (1:1 mirrors) are relationships, not rollups.
                        m_rows, e_rows = _paired_row_counts(measure, event)
                        if e_rows <= m_rows:
                            logger.info(
                                "lineage_direction_gate",
                                dimension=slice_label,
                                measure_table=measure.table.table_name,
                                event_table=event.table.table_name,
                                measure_rows=m_rows,
                                event_rows=e_rows,
                            )
                            continue
                        conventions = _conventions(
                            [c for c in event.numeric_columns if c not in keys_e]
                        )
                        for measure_col in measure_cols:
                            # The common denominator every convention's vote rate
                            # is judged against (DAT-759) — fixed per pairing, so
                            # a term missing on some entities can't flatter a rate.
                            universe = _pairing_universe(measure, event, measure_col)
                            if universe == 0:
                                continue
                            for convention_sql, terms in conventions:
                                by_entity = _aligned_series(measure, event, measure_col, terms)
                                if not by_entity:
                                    continue
                                results = classify_series(by_entity)
                                verdict = dispose_classified(results)
                                if verdict is None:
                                    continue
                                residual = (
                                    verdict.r_flow_median
                                    if verdict.pattern == "per_period"
                                    else verdict.r_stock_median
                                )
                                challenger = _Best(
                                    verdict=verdict,
                                    event_table=event.table,
                                    convention_sql=convention_sql,
                                    winning_residual=residual,
                                    support_lcb=wilson_lcb(verdict.n_entities_fired, universe),
                                    arity=len(terms),
                                    voter_residuals=tuple(
                                        min(r.r_flow, r.r_stock)
                                        for r in results.values()
                                        if r.label == verdict.pattern
                                    ),
                                )
                                key = columns_by_table[m_tid][measure_col].column_id
                                prior = best_by_measure.get(key)
                                if prior is None or _better(challenger, prior[0]):
                                    best_by_measure[key] = (
                                        challenger,
                                        measure.table,
                                        measure_col,
                                        slice_label,
                                    )

    # ``best_by_measure`` is keyed by measure_column_id, so the batch is
    # dedup'd by construction; PK omitted so the model's default applies.
    rows: list[dict[str, object]] = []
    for measure_column_id, (best, m_table, m_col, slice_label) in best_by_measure.items():
        rows.append(
            {
                "run_id": run_id,
                "measure_table_id": m_table.table_id,
                "measure_column_id": measure_column_id,
                "event_table_id": best.event_table.table_id,
                "slice_dimension": slice_label,
                "convention_sql": best.convention_sql,
                "period_grain": period_grain,
                "pattern": best.verdict.pattern,
                "match_rate": best.verdict.match_rate,
                "r_flow_median": best.verdict.r_flow_median,
                "r_stock_median": best.verdict.r_stock_median,
                "n_entities": best.verdict.n_entities,
                "n_entities_fired": best.verdict.n_entities_fired,
            }
        )
        logger.info(
            "lineage_reconciled",
            measure=f"{m_table.table_name}.{m_col}",
            event_table=best.event_table.table_name,
            dimension=slice_label,
            convention=best.convention_sql,
            pattern=best.verdict.pattern,
            match_rate=round(best.verdict.match_rate, 3),
            support_lcb=round(best.support_lcb, 3),
            n_entities_fired=best.verdict.n_entities_fired,
        )
    upsert(
        session,
        MeasureAggregationLineage,
        rows,
        index_elements=["measure_column_id", "run_id"],
    )
    return len(rows)
