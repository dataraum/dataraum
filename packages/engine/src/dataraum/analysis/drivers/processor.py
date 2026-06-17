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

from dataclasses import replace
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
from sqlalchemy import select

from dataraum.analysis.drivers.criterion import (
    DEFAULT_MIN_SUPPORT,
    DEFAULT_MISSINGNESS_GATE,
    intraclass_correlation,
)
from dataraum.analysis.drivers.models import DriverRanking, Measure, SecondaryDriver
from dataraum.analysis.drivers.targets import (
    EntityDemeanedRatioTarget,
    EntityMeanTarget,
    FlowTarget,
    RatioTarget,
    Target,
)
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

# The measure's ICC (η² between entities) selects which grain family is PRIMARY
# (DAT-561): above this, the entity-grain family leads (the measure clusters, so the
# between-entity story is the headline); below, the row-wise family leads. It no longer
# gates WHICH dims go to which grain — that is now decided per-candidate by within-
# entity constancy (an entity-constant candidate takes the entity-grain null at ANY
# ICC, since the row-wise null is structurally invalid for it). Conservative: even
# modest clustering makes the entity story primary (DAT-552 / DAT-544 E1).
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


def _icc_measure(frame: pd.DataFrame, measure: Measure) -> np.ndarray:
    """The per-row scalar the ICC is computed on.

    The column itself (flow/stock) or the per-row ratio num/den (ratio; NaN where
    the denominator is missing or ≤ 0).
    """
    if measure.target_type in ("flow", "stock"):
        assert measure.column is not None
        return frame[measure.column].to_numpy(dtype=float)
    assert measure.numerator and measure.denominator
    num = frame[measure.numerator].to_numpy(dtype=float)
    den = frame[measure.denominator].to_numpy(dtype=float)
    valid = ~np.isnan(num) & ~np.isnan(den) & (den > 0)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(valid, num / np.where(valid, den, 1.0), np.nan)


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

    **Cluster-aware candidate-grain routing (DAT-552/561):** when ``cluster_key`` names
    a repeated-entity column, candidates are routed by their **within-entity
    constancy**, not by the measure's global ICC:

    - **Entity-constant** candidates (one value per entity) take the **entity-grain**
      null ALWAYS — collapse to one row per entity, permute entities. The row-wise null
      is structurally invalid for them at any ICC > 0 (their groups are whole entities,
      so correlated within-entity rows would be counted as independent — DAT-561).
    - **Row-level** candidates (vary within entity) take the **row-wise** null, which is
      valid for them at any ICC (it just loses power as the measure clusters — see the
      de-mean power add-on below).

    The two families are merged into one ranking: the **primary** family (its tree,
    paths, slices, ``ranked_dimensions``, and ``grain``) is the one selected by the
    measure's ICC — entity grain above ``icc_threshold`` (the between-entity story is
    the headline), row-wise below; the **secondary** family's significant dims are
    exposed as ``secondary_dimensions`` (a flat grain-labeled list, not folded into the
    primary ranking — the grains are not cross-comparable). Ratio routes the same way
    (entity statistic = Σnum/Σden, weight = Σden). With no ``cluster_key`` the plain
    row-wise null (DAT-545) is used. ``max_depth`` applies to the row-wise family; the
    entity grain always uses ``max_depth=1`` (recursion there is low-power).

    **Power add-on (DAT-561):** under HIGH ICC the row-level (secondary) family's
    row-wise null on the raw measure has little power — the between-entity variance is
    noise. It gates on the **within-entity de-meaned residual** instead (the
    fixed-effects "within" transform), which is row-exchangeable and powered — this is
    the within-entity driver analysis. Flow/stock de-mean the measure
    (``measure − entity_mean``); ratio de-means the per-row ratio by its entity's
    volume-weighted mean (its pooled ``Σnum/Σden``).

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

    # Cluster-aware candidate-grain routing (DAT-561): a declared entity column splits
    # the candidates by within-entity constancy and runs two grain families; the
    # measure's ICC only picks which is primary. With no cluster_key, the plain
    # row-wise null (DAT-545) over all candidates.
    if cluster_col is not None:
        return _routed_ranking(
            frame,
            present_dims,
            measure,
            cluster_col,
            seed=seed,
            max_depth=max_depth,
            alpha=alpha,
            min_support=min_support,
            missingness_gate=missingness_gate,
            n_perm=n_perm,
            top_k_slices=top_k_slices,
            icc_threshold=icc_threshold,
            min_entities=min_entities,
        )

    return _row_wise_ranking(
        frame,
        present_dims,
        measure,
        seed=seed,
        max_depth=max_depth,
        alpha=alpha,
        min_support=min_support,
        missingness_gate=missingness_gate,
        n_perm=n_perm,
        top_k_slices=top_k_slices,
    )


def _collapse_to_entity(
    frame: pd.DataFrame, cluster_key: str, measure: Measure, entity_dims: list[str]
) -> tuple[np.ndarray, np.ndarray, dict[str, np.ndarray]]:
    """One row per entity: (statistic, weight, entity-level dim values), row-aligned.

    flow/stock → (mean measure, observed-row count); ratio → (Σnum/Σden, Σden), the
    support-correct entity ratio weighted by its denominator mass. Entities with no
    usable measure are dropped; the dim values come from the SAME grouping so they
    stay aligned with the kept entities.
    """
    if measure.target_type == "ratio":
        assert measure.numerator and measure.denominator
        valid = (
            frame[measure.numerator].notna()
            & frame[measure.denominator].notna()
            & (frame[measure.denominator] > 0)
        )
        grouped = frame[valid].groupby(cluster_key, sort=False)
        sum_num = grouped[measure.numerator].sum().to_numpy(dtype=float)
        sum_den = grouped[measure.denominator].sum().to_numpy(dtype=float)
        keep = sum_den > 0
        with np.errstate(divide="ignore", invalid="ignore"):
            values = (sum_num / sum_den)[keep]
        sizes = sum_den[keep]  # weight = denominator mass
    else:
        assert measure.column is not None
        grouped = frame.groupby(cluster_key, sort=False)
        agg = grouped[measure.column].agg(["mean", "count"])
        keep = agg["count"].to_numpy() > 0
        values = agg["mean"].to_numpy(dtype=float)[keep]
        sizes = agg["count"].to_numpy(dtype=float)[keep]

    values_by_dim = {d: grouped[d].first().to_numpy()[keep].astype(object) for d in entity_dims}
    return values, sizes, values_by_dim


def _partition_by_entity_constancy(
    frame: pd.DataFrame, cluster_key: str, dims: list[str]
) -> tuple[list[str], list[str]]:
    """Split candidates into ``(entity_constant, row_level)`` by within-entity nunique.

    Entity-constant = one value per entity (nunique ≤ 1) → the entity-grain null;
    everything else varies within entity → the row-wise null (DAT-561). This is the
    routing decision: it is per-candidate, independent of the measure's global ICC.
    """
    # ``nunique`` counts non-null distinct values, so ``<= 1`` also catches an all-null
    # dim (nunique 0) as entity-constant — harmless: it contributes nothing (every row
    # gated out by the (A) gate) wherever it lands, exactly as on the old row-wise path.
    nunique = frame.groupby(cluster_key)[dims].nunique().max()
    entity_constant = [d for d in dims if int(nunique[d]) <= 1]
    row_level = [d for d in dims if d not in entity_constant]
    return entity_constant, row_level


def _within_entity_residual(frame: pd.DataFrame, cluster_key: str, column: str) -> np.ndarray:
    """The fixed-effects "within" transform: ``measure − entity_mean`` (DAT-561).

    Removes the between-entity level so the row-wise null on the residual is both valid
    (residuals are row-exchangeable within entity) and powered for a within-entity
    row-level driver — the entity-mean subtraction strips the clustered variance that
    would otherwise swamp it. NaN measure rows stay NaN (the (B) gate handles them).
    """
    measure = frame[column].to_numpy(dtype=float)
    entity_mean = frame.groupby(cluster_key)[column].transform("mean").to_numpy(dtype=float)
    return np.asarray(measure - entity_mean, dtype=float)


def _within_entity_ratio_residual(
    frame: pd.DataFrame, cluster_key: str, numerator: str, denominator: str
) -> tuple[np.ndarray, np.ndarray]:
    """``(residual_ratio, weight)`` for the within-entity de-meaned RATIO (DAT-561).

    The per-row ratio ``r = num/den`` minus its entity's VOLUME-WEIGHTED mean — which is
    the entity's pooled ratio ``Σnum/Σden`` (the weighted mean of ``r`` with weight
    ``den``). Strips the between-entity ratio level so the row-wise null on the residual
    is valid + powered for a within-entity ratio driver. NaN where the row has no usable
    ratio (missing/≤0 denominator); ``weight`` is the denominator mass (0 where invalid).
    """
    num = frame[numerator].to_numpy(dtype=float)
    den = frame[denominator].to_numpy(dtype=float)
    valid = ~np.isnan(num) & ~np.isnan(den) & (den > 0)
    codes, uniques = pd.factorize(frame[cluster_key])
    codes = codes.astype(int)
    n_ent = len(uniques)
    sum_num = np.bincount(codes[valid], weights=num[valid], minlength=n_ent)
    sum_den = np.bincount(codes[valid], weights=den[valid], minlength=n_ent)
    with np.errstate(divide="ignore", invalid="ignore"):
        entity_ratio = np.where(sum_den > 0, sum_num / np.where(sum_den > 0, sum_den, 1.0), np.nan)
        r = np.where(valid, num / np.where(valid, den, 1.0), np.nan)
    residual = r - entity_ratio[codes]  # NaN where r or the entity ratio is NaN
    weight = np.where(valid, den, 0.0)
    return residual, weight


def _merge_secondary(
    primary: DriverRanking | None, secondary: DriverRanking | None
) -> DriverRanking:
    """Fold the secondary family's significant dims into ``primary`` as a labeled list.

    The ICC-preferred family is primary; if it has no candidate dims the other becomes
    primary (so a real driver in the only populated family is never hidden behind an
    empty primary). The remaining family contributes ``secondary_dimensions`` only —
    its gains are at a different grain, never mixed into the primary ranking.
    """
    if primary is None:
        primary, secondary = secondary, None
    if primary is None:  # neither family had candidate dims (caller guarantees ≥2 total)
        raise AssertionError("candidate-grain routing produced no family")
    if secondary is None:
        return primary
    labeled = [SecondaryDriver(d, g, secondary.grain) for d, g in secondary.ranked_dimensions]
    return replace(primary, secondary_dimensions=labeled)


def _routed_ranking(
    frame: pd.DataFrame,
    dims: list[str],
    measure: Measure,
    cluster_key: str,
    *,
    seed: int,
    max_depth: int,
    alpha: float,
    min_support: int,
    missingness_gate: float,
    n_perm: int,
    top_k_slices: int,
    icc_threshold: float,
    min_entities: int,
) -> DriverRanking:
    """Route candidates to two grain families and merge primary (by ICC) + secondary."""
    ent_codes, ent_uniques = pd.factorize(frame[cluster_key])
    icc = intraclass_correlation(
        ent_codes.astype(int), len(ent_uniques), _icc_measure(frame, measure)
    )
    high_icc = icc > icc_threshold
    entity_dims, row_dims = _partition_by_entity_constancy(frame, cluster_key, dims)
    logger.info(
        "driver_candidate_grain_routing",
        cluster_key=cluster_key,
        icc=round(icc, 3),
        n_entities=len(ent_uniques),
        primary="entity" if high_icc else "row",
        entity_constant=entity_dims,
        row_level=row_dims,
    )

    entity_ranking = (
        _entity_grain_ranking(
            frame,
            entity_dims,
            measure,
            cluster_key,
            seed=seed,
            alpha=alpha,
            n_perm=n_perm,
            top_k_slices=top_k_slices,
            min_entities=min_entities,
        )
        if entity_dims
        else None
    )
    # The row-level family de-means within entity ONLY under high ICC (the power add-on);
    # at low ICC the raw measure is already powered and stays the primary tree as-is.
    row_ranking = (
        _row_wise_ranking(
            frame,
            row_dims,
            measure,
            seed=seed + 1,  # an independent permutation stream from the entity family
            max_depth=max_depth,
            alpha=alpha,
            min_support=min_support,
            missingness_gate=missingness_gate,
            n_perm=n_perm,
            top_k_slices=top_k_slices,
            cluster_key=cluster_key if high_icc else None,
        )
        if row_dims
        else None
    )

    if high_icc:
        return _merge_secondary(entity_ranking, row_ranking)
    return _merge_secondary(row_ranking, entity_ranking)


def _row_wise_ranking(
    frame: pd.DataFrame,
    dims: list[str],
    measure: Measure,
    *,
    seed: int,
    max_depth: int,
    alpha: float,
    min_support: int,
    missingness_gate: float,
    n_perm: int,
    top_k_slices: int,
    cluster_key: str | None = None,
) -> DriverRanking:
    """Rank ``dims`` row-wise. ``cluster_key`` set → de-mean the measure within entity.

    The within-entity de-mean is the DAT-561 power add-on for the row-level family under
    high ICC: flow/stock de-mean the measure (``FlowTarget`` on the residual), ratio
    de-means the per-row ratio by its entity's volume-weighted mean
    (``EntityDemeanedRatioTarget``). With ``cluster_key=None`` this is the plain DAT-545
    row-wise search on the raw measure.
    """
    if cluster_key is not None:
        if measure.target_type in ("flow", "stock"):
            assert measure.column is not None
            residual = _within_entity_residual(frame, cluster_key, measure.column)
            target: Target = FlowTarget(residual, target_type=measure.target_type)
        else:  # ratio
            assert measure.numerator and measure.denominator
            res_ratio, weight = _within_entity_ratio_residual(
                frame, cluster_key, measure.numerator, measure.denominator
            )
            target = EntityDemeanedRatioTarget(res_ratio, weight)
    else:
        target = _make_target(measure, frame)
    values_by_dim = {d: frame[d].astype(object).to_numpy() for d in dims}
    return discover_tree(
        values_by_dim,
        target,
        measure_label=measure.label,
        dims=dims,
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
    entity_dims: list[str],
    measure: Measure,
    cluster_key: str,
    *,
    seed: int,
    alpha: float,
    n_perm: int,
    top_k_slices: int,
    min_entities: int,
) -> DriverRanking:
    """Collapse to one row per entity and rank the (pre-partitioned) entity-level dims.

    ``entity_dims`` are already constant within entity (the caller's routing). The
    entity statistic is the mean measure weighted by observed-row count (flow/stock) or
    Σnum/Σden weighted by Σden (ratio); entities with no usable measure are dropped.
    Single-level (``max_depth=1``): recursion at entity grain is low-power and a
    follow-up.
    """
    empty = DriverRanking(
        measure=measure.label, target_type=measure.target_type, n_rows=0, grain="entity"
    )
    values, sizes, values_by_dim = _collapse_to_entity(frame, cluster_key, measure, entity_dims)
    if values.size == 0:  # every entity has no usable measure — nothing to rank
        return empty

    target = EntityMeanTarget(values, sizes, target_type=measure.target_type)
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
