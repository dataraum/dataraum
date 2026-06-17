"""Driver discovery over the real catalog + enriched view (DAT-545 P3).

Binds the engine (:mod:`tree`) to the begin_session substrate:

- **Candidate dims** = this run's grain-safe ``SliceDefinition`` columns (DAT-536),
  with ``DimensionHierarchy`` 1:1 alias groups collapsed to their canonical axis
  (DAT-537) so a redundant dimension never competes in the permutation null.
- **Substrate** = the fact's grain-verified enriched view, read at ROW grain via
  DuckDB (required so the (B) missingness gate sees NULL structure). Columns are
  pulled ONCE into memory; the permutation null runs in numpy (the design's "GROUP
  BYs over aggregation views" is moot — ADR-0013 removed those, and 500 shuffles in
  SQL would be hundreds of scans).
- **Target type** = the measure's ``SemanticAnnotation.temporal_behavior``
  (``additive`` → flow, ``point_in_time`` → stock) via :func:`resolve_target_type`.

On-demand and pure: returns a :class:`DriverRanking`, persists nothing (DAT-546).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import numpy as np
from sqlalchemy import select

from dataraum.analysis.drivers.criterion import DEFAULT_MIN_SUPPORT, DEFAULT_MISSINGNESS_GATE
from dataraum.analysis.drivers.models import DriverRanking, Measure
from dataraum.analysis.drivers.tree import (
    DEFAULT_ALPHA,
    DEFAULT_MAX_DEPTH,
    DEFAULT_N_PERM,
    DEFAULT_TOP_K_SLICES,
    discover_tree,
)
from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

_TEMPORAL_TO_TARGET = {"additive": "flow", "point_in_time": "stock"}


def resolve_target_type(session: Session, *, column_id: str, run_id: str) -> str:
    """Map the measure column's ``temporal_behavior`` to a driver target type.

    ``additive`` → ``flow``, ``point_in_time`` → ``stock``; anything else (or no
    annotation) defaults to ``flow`` — the additive reading, logged. Ratio is not a
    ``temporal_behavior`` value: a ratio measure is constructed explicitly by the
    caller (computed metric), not resolved here.
    """
    behavior = session.execute(
        select(SemanticAnnotation.temporal_behavior).where(
            SemanticAnnotation.column_id == column_id,
            SemanticAnnotation.run_id == run_id,
        )
    ).scalar_one_or_none()
    target = _TEMPORAL_TO_TARGET.get(behavior or "", "flow")
    if behavior not in _TEMPORAL_TO_TARGET:
        logger.info("driver_target_type_defaulted", column_id=column_id, behavior=behavior)
    return target


def _enriched_view_name(session: Session, fact_table_id: str, run_id: str) -> str | None:
    """The grain-verified enriched view for the fact this run (the split substrate)."""
    return session.execute(
        select(EnrichedView.view_name).where(
            EnrichedView.fact_table_id == fact_table_id,
            EnrichedView.run_id == run_id,
            EnrichedView.is_grain_verified.is_(True),
        )
    ).scalar_one_or_none()


def _candidate_dims(session: Session, fact_table_id: str, run_id: str) -> list[str]:
    """This run's grain-safe slice dimensions, with alias groups collapsed to canonical.

    A DAT-537 1:1 alias group is a redundant axis — keep only its canonical member so
    it doesn't compete as a separate candidate (the de-confounding the spike deferred).
    """
    defs = session.execute(
        select(SliceDefinition.column_name).where(
            SliceDefinition.table_id == fact_table_id,
            SliceDefinition.run_id == run_id,
            SliceDefinition.grain_safe.is_(True),
            SliceDefinition.column_name.isnot(None),
        )
    ).scalars()
    candidates = {name for name in defs if name}

    aliases = session.execute(
        select(DimensionHierarchy).where(
            DimensionHierarchy.table_id == fact_table_id,
            DimensionHierarchy.run_id == run_id,
            DimensionHierarchy.kind == "alias",
        )
    ).scalars()
    for group in aliases:
        for member in group.members:
            name = member.get("column_name")
            if name and name != group.canonical_label:
                candidates.discard(name)
    return sorted(candidates)


def _measure_columns(measure: Measure) -> list[str]:
    """The enriched-view columns a measure needs read."""
    if measure.target_type in ("flow", "stock"):
        return [measure.column] if measure.column else []
    raise NotImplementedError(f"target_type {measure.target_type!r} arrives in DAT-545 P4")


def discover_drivers(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    fact_table_id: str,
    run_id: str,
    measure: Measure,
    seed: int = 0,
    max_depth: int = DEFAULT_MAX_DEPTH,
    alpha: float = DEFAULT_ALPHA,
    min_support: int = DEFAULT_MIN_SUPPORT,
    missingness_gate: float = DEFAULT_MISSINGNESS_GATE,
    n_perm: int = DEFAULT_N_PERM,
    top_k_slices: int = DEFAULT_TOP_K_SLICES,
) -> DriverRanking:
    """Rank the catalog's dimensions as drivers of ``measure`` over the enriched view.

    Pure + deterministic for a given ``seed`` (so a future cache keyed by
    ``(measure, run)`` is stable). Returns an empty ranking — never an error — when
    the fact has no grain-verified enriched view or fewer than two candidate dims.
    """
    empty = DriverRanking(measure=measure.label, target_type=measure.target_type, n_rows=0)

    view = _enriched_view_name(session, fact_table_id, run_id)
    if view is None:
        logger.info("driver_no_enriched_view", fact_table_id=fact_table_id, run_id=run_id)
        return empty
    dims = _candidate_dims(session, fact_table_id, run_id)
    if len(dims) < 2:
        logger.info("driver_too_few_candidates", fact_table_id=fact_table_id, n=len(dims))
        return empty

    def quote(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    select_cols = dims + _measure_columns(measure)
    sql = f"SELECT {', '.join(quote(c) for c in select_cols)} FROM {quote(view)}"  # noqa: S608 — catalog identifiers
    frame = duckdb_conn.execute(sql).df()

    # Only dims actually present in the view participate (a catalog/view skew is
    # logged, not fatal).
    present_dims = [d for d in dims if d in frame.columns]
    if len(present_dims) < 2:
        logger.info("driver_dims_absent_from_view", view=view, present=present_dims)
        return empty
    values_by_dim = {d: frame[d].astype(object).to_numpy() for d in present_dims}
    measure_array = frame[measure.column].to_numpy(dtype=float)

    return discover_tree(
        values_by_dim,
        measure_array,
        measure_label=measure.label,
        target_type=measure.target_type,
        dims=present_dims,
        rng=np.random.default_rng(seed),
        max_depth=max_depth,
        alpha=alpha,
        min_support=min_support,
        missingness_gate=missingness_gate,
        n_perm=n_perm,
        top_k_slices=top_k_slices,
    )
