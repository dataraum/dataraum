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
import pandas as pd
from sqlalchemy import select

from dataraum.analysis.drivers.criterion import (
    DEFAULT_MIN_SUPPORT,
    DEFAULT_MISSINGNESS_GATE,
    intraclass_correlation,
)
from dataraum.analysis.drivers.models import DriverRanking, Measure
from dataraum.analysis.drivers.targets import EntityMeanTarget, FlowTarget, RatioTarget, Target
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

# Above this ICC (η² of the measure between entities), the row-wise permutation null
# is invalid — the cluster is the exchangeable unit, so switch to the entity grain
# (DAT-552 / DAT-544 E1). Conservative: even modest clustering flips it.
DEFAULT_ICC_THRESHOLD = 0.10
# At entity grain a candidate group is evaluated only with at least this many ENTITIES
# (the min_support analogue — power scales with entity count, not rows).
DEFAULT_MIN_ENTITIES = 10

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
    assert measure.numerator and measure.denominator  # guaranteed by Measure.__post_init__
    return [measure.numerator, measure.denominator]


def _make_target(measure: Measure, frame: pd.DataFrame) -> Target:
    """Build the row-aligned target from the measure's columns in ``frame``."""
    if measure.target_type in ("flow", "stock"):
        assert measure.column  # guaranteed by Measure.__post_init__
        return FlowTarget(
            frame[measure.column].to_numpy(dtype=float), target_type=measure.target_type
        )
    assert measure.numerator and measure.denominator  # guaranteed by Measure.__post_init__
    return RatioTarget(
        frame[measure.numerator].to_numpy(dtype=float),
        frame[measure.denominator].to_numpy(dtype=float),
    )


def discover_drivers(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    fact_table_id: str,
    run_id: str,
    measure: Measure,
    cluster_key: str | None = None,
    seed: int = 0,
    max_depth: int = DEFAULT_MAX_DEPTH,
    alpha: float = DEFAULT_ALPHA,
    min_support: int = DEFAULT_MIN_SUPPORT,
    missingness_gate: float = DEFAULT_MISSINGNESS_GATE,
    n_perm: int = DEFAULT_N_PERM,
    top_k_slices: int = DEFAULT_TOP_K_SLICES,
    icc_threshold: float = DEFAULT_ICC_THRESHOLD,
    min_entities: int = DEFAULT_MIN_ENTITIES,
) -> DriverRanking:
    """Rank the catalog's dimensions as drivers of ``measure`` over the enriched view.

    Pure + deterministic for a given ``(seed, candidate-dim set)`` — the permutation
    draw sequence depends on the dims, so a future cache (DAT-546) must key on the
    candidate set too, not just ``(measure, run, seed)``. Returns an empty ranking —
    never an error — when the fact has no grain-verified enriched view, fewer than
    two candidate dims survive in the view, or the measure columns are absent
    (a catalog/view skew is logged, not fatal).

    **Cluster-aware (DAT-552):** when ``cluster_key`` names a repeated-entity column
    and the measure's ICC within it exceeds ``icc_threshold``, the row-wise
    permutation null is invalid (the entity, not the row, is exchangeable) — the
    search switches to the **entity grain** (one row per entity, permute entities;
    ``DriverRanking.grain == "entity"``, power scales with entity count). Below the
    threshold, or with no ``cluster_key``, the row-wise null (DAT-545) is used. Only
    entity-LEVEL candidates (constant within entity) participate at entity grain;
    row-level dims are logged and skipped that pass. Ratio measures stay row-wise for
    now (cluster-aware ratio is a follow-up).

    NOTE: the ``(present_dims + measure)`` columns are read into memory at row grain
    in one pass. At ~1M rows × ~15 dims that is several hundred MB; DAT-546 should add
    a row-count gate before calling on very large views.
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

    # Probe the view's columns first (LIMIT 0 — no scan) so a catalog/view skew is a
    # logged empty result, not a DuckDB BinderException, AND we still read only the
    # columns we need (no SELECT *). The measure columns must exist; dims intersect.
    view_cols = set(duckdb_conn.execute(f"SELECT * FROM {quote(view)} LIMIT 0").df().columns)  # noqa: S608
    present_dims = [d for d in dims if d in view_cols]
    measure_cols = _measure_columns(measure)
    if len(present_dims) < 2 or any(c not in view_cols for c in measure_cols):
        logger.info("driver_view_skew", view=view, present=present_dims, measure_cols=measure_cols)
        return empty

    # The cluster key is read alongside if it exists in the view (DAT-552).
    cluster_col = cluster_key if (cluster_key and cluster_key in view_cols) else None
    select_cols = present_dims + measure_cols + ([cluster_col] if cluster_col else [])
    sql = f"SELECT {', '.join(quote(c) for c in select_cols)} FROM {quote(view)}"  # noqa: S608 — catalog identifiers
    frame = duckdb_conn.execute(sql).df()

    # Cluster-aware switch: a high-ICC measure within the declared entity invalidates
    # the row-wise null (DAT-552). flow/stock only; ratio stays row-wise for now.
    if cluster_col is not None and measure.target_type in ("flow", "stock"):
        ent_codes, ent_uniques = pd.factorize(frame[cluster_col])
        icc = intraclass_correlation(
            ent_codes.astype(int), len(ent_uniques), frame[measure.column].to_numpy(dtype=float)
        )
        if icc > icc_threshold:
            logger.info(
                "driver_cluster_aware_entity_grain",
                cluster_key=cluster_col,
                icc=round(icc, 3),
                n_entities=len(ent_uniques),
            )
            return _entity_grain_ranking(
                frame,
                present_dims,
                measure,
                cluster_col,
                seed=seed,
                alpha=alpha,
                n_perm=n_perm,
                top_k_slices=top_k_slices,
                min_entities=min_entities,
            )
        logger.info("driver_row_wise_low_icc", cluster_key=cluster_key, icc=round(icc, 3))

    values_by_dim = {d: frame[d].astype(object).to_numpy() for d in present_dims}
    return discover_tree(
        values_by_dim,
        _make_target(measure, frame),
        measure_label=measure.label,
        dims=present_dims,
        rng=np.random.default_rng(seed),
        max_depth=max_depth,
        alpha=alpha,
        min_support=min_support,
        missingness_gate=missingness_gate,
        n_perm=n_perm,
        top_k_slices=top_k_slices,
    )


def _entity_grain_ranking(
    frame: pd.DataFrame,
    dims: list[str],
    measure: Measure,
    cluster_key: str,
    *,
    seed: int,
    alpha: float,
    n_perm: int,
    top_k_slices: int,
    min_entities: int,
) -> DriverRanking:
    """Collapse to one row per entity and rank entity-level candidates at that grain.

    Only candidates that are CONSTANT within entity participate (row-level dims can't
    be collapsed without losing their within-entity variation — logged + skipped this
    pass). Each entity contributes its mean measure weighted by its observed-row count;
    entities with no observed measure are dropped. Single-level (``max_depth=1``):
    recursion at entity grain is low-power and a follow-up.
    """
    empty = DriverRanking(
        measure=measure.label, target_type=measure.target_type, n_rows=0, grain="entity"
    )
    # Entity-level = constant within every entity (nunique ≤ 1).
    nunique = frame.groupby(cluster_key)[dims].nunique().max()
    entity_dims = [d for d in dims if int(nunique[d]) <= 1]
    skipped = [d for d in dims if d not in entity_dims]
    if skipped:
        logger.info("driver_row_level_dims_skipped_at_entity_grain", dropped=skipped)
    if len(entity_dims) < 2:
        return empty

    assert measure.column is not None  # entity grain is flow/stock only (Measure-guaranteed)
    grouped = frame.groupby(cluster_key, sort=False)
    agg = grouped[measure.column].agg(["mean", "count"])
    keep = agg["count"].to_numpy() > 0  # drop entities with no observed measure
    means = agg["mean"].to_numpy(dtype=float)[keep]
    sizes = agg["count"].to_numpy(dtype=float)[keep]
    values_by_dim = {d: grouped[d].first().to_numpy()[keep].astype(object) for d in entity_dims}

    target = EntityMeanTarget(means, sizes, target_type=measure.target_type)
    return discover_tree(
        values_by_dim,
        target,
        measure_label=measure.label,
        dims=entity_dims,
        rng=np.random.default_rng(seed),
        max_depth=1,
        alpha=alpha,
        min_support=min_entities,
        n_perm=n_perm,
        top_k_slices=top_k_slices,
    )
