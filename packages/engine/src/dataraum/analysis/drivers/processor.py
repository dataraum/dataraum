"""Driver discovery over the real catalog + enriched view (DAT-545).

Binds the engine (:mod:`tree`) to the begin_session substrate:

- **Candidate dims** = this run's grain-safe ``SliceDefinition`` columns (DAT-536),
  with ``DimensionHierarchy`` 1:1 alias groups collapsed to their canonical axis
  (DAT-537) so a redundant dimension never competes in the permutation null.
- **Substrate** = the fact's grain-verified enriched view, read at ROW grain via
  DuckDB (required so the (B) missingness gate sees NULL structure). Columns are
  pulled ONCE into memory; the permutation null runs in numpy (the design's "GROUP
  BYs over aggregation views" is moot — ADR-0013 removed those, and 500 shuffles in
  SQL would be hundreds of scans).
- **Target type** = the measure's ``ColumnConcept.temporal_behavior`` (DAT-637)
  (``additive`` → flow, ``point_in_time`` → stock) via :func:`resolve_target_type`.
  NULL/unmapped behavior ABSTAINS (DAT-859) rather than defaulting to flow — a
  measure whose type we don't know is never silently ranked as one.

On-demand and pure: returns a :class:`DriverRanking`, persists nothing (DAT-546).
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING

import numpy as np
import polars as pl
from sqlalchemy import select

from dataraum.analysis.drivers.criterion import (
    DEFAULT_MIN_SUPPORT,
    DEFAULT_MISSINGNESS_GATE,
    intraclass_correlation,
)
from dataraum.analysis.drivers.models import (
    AbstainReason,
    DriverRanking,
    Measure,
    RankingStatus,
    SecondaryDriver,
    TargetTypeResolution,
)
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
    discover_tree,
)
from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.semantic.db_models import ColumnConcept, TableEntity
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
# modest clustering makes the entity story primary (DAT-552 / DAT-544).
DEFAULT_ICC_THRESHOLD = 0.10
# At entity grain a candidate group is evaluated only with at least this many ENTITIES
# (the min_support analogue — power scales with entity count, not rows).
DEFAULT_MIN_ENTITIES = 10
# Above this row count the enriched view is sub-sampled before the in-memory load
# (DAT-571). The bound is the in-memory working set, NOT DuckDB's scan (which is
# memory_limit-capped). DAT-580 made the load arrow-backed: dims are arrow strings then
# physical int codes (no ~50–80 B/value Python str objects) and the measure a float view,
# so the same per-activity byte budget that held ~800k pandas-frame rows now holds ~3×
# (the spike measured −67% peak RSS at 1M×15). 2.4M keeps sampling a rare fallback while
# staying conservative under the worker's concurrent activities.
DEFAULT_MAX_ROWS = 2_400_000

_TEMPORAL_TO_TARGET = {"additive": "flow", "point_in_time": "stock"}


def resolve_target_type_for_behavior(behavior: str | None) -> TargetTypeResolution:
    """Map a measure's catalog ``temporal_behavior`` to a target-type resolution (DAT-859).

    ``additive`` → ``flow``, ``point_in_time`` → ``stock``; anything else (NULL, or
    an unrecognised value) ABSTAINS — reason ``missing_inputs`` — rather than
    defaulting to ``"flow"``: post-DAT-847 the upstream deliberately fails closed to
    undetermined, so silently ranking it as a flow was a landed-contract breach.
    Ratio is not a ``temporal_behavior`` value: a ratio measure is constructed
    explicitly by the caller (computed metric), never resolved here.

    Pure — the DB-querying :func:`resolve_target_type` delegates here, and
    ``persistence.persist_driver_rankings`` (which already has ``behavior`` from its
    batched catalog read) calls this directly to avoid an N+1 re-query per measure.
    """
    target_type = _TEMPORAL_TO_TARGET.get(behavior or "")
    if target_type is None:
        logger.info("driver_target_type_undetermined", behavior=behavior)
        return TargetTypeResolution(
            status=RankingStatus.ABSTAINED, abstain_reason=AbstainReason.MISSING_INPUTS
        )
    return TargetTypeResolution(status=RankingStatus.MEASURED, target_type=target_type)


def resolve_target_type(session: Session, *, column_id: str, run_id: str) -> TargetTypeResolution:
    """Resolve the measure column's ``temporal_behavior`` to a driver target type.

    DB-querying convenience wrapper over :func:`resolve_target_type_for_behavior`
    (see there for the mapping + the DAT-859 fail-closed abstention).
    """
    # temporal_behavior is catalogue-grain (DAT-637): on ColumnConcept, written by
    # the table agent under THIS begin_session run earlier in the session spine.
    behavior = session.execute(
        select(ColumnConcept.temporal_behavior).where(
            ColumnConcept.column_id == column_id,
            ColumnConcept.run_id == run_id,
        )
    ).scalar_one_or_none()
    return resolve_target_type_for_behavior(behavior)


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
    """This run's grain-safe slice dimensions, with CONFIRMED alias groups collapsed.

    A DAT-537 1:1 alias group is a redundant axis. Confirmed groups are union-found
    into equivalence classes (so a manual teach that overlaps an auto-detected group
    collapses as ONE class, independent of row order), and each class keeps only ONE
    of its ELECTED members so the rest don't compete as separate candidates (the
    de-confounding the spike deferred). The representative is an elected canonical when
    one exists, else the sorted-first elected member: the canonical may be a raw-FK
    near-key the slicing gate excluded, so collapsing to it would orphan the whole
    dimension (DAT-806). Only CONFIRMED aliases collapse: a ``needs_confirmation``
    alias is an UNCONFIRMED redundancy (a coincidental bijection the DAT-762 identity
    judge declined, or an undecidable role-check near-copy), and collapsing it would
    silently drop a real axis the flag says we are not sure about — keep both.
    """
    defs = session.execute(
        select(SliceDefinition.column_name).where(
            SliceDefinition.table_id == fact_table_id,
            SliceDefinition.run_id == run_id,
            SliceDefinition.column_name.isnot(None),
        )
    ).scalars()
    candidates = {name for name in defs if name}

    aliases = session.execute(
        select(DimensionHierarchy).where(
            DimensionHierarchy.table_id == fact_table_id,
            DimensionHierarchy.run_id == run_id,
            DimensionHierarchy.kind == "alias",
            DimensionHierarchy.needs_confirmation.is_(False),
        )
    ).scalars()
    # Union-find over every confirmed alias group's members: overlapping groups
    # (a manual teach that partially covers an auto-detected group — ``overlay.py``
    # sets no overlap guard, and ``_apply_teaches`` writes ``needs_confirmation=False``)
    # must collapse as ONE equivalence class, independent of row order. Discarding
    # per-group while reading the live ``candidates`` set would make the survivor set
    # (hence the driver candidate set) depend on the ORDER-BY-less query order —
    # varying across Temporal redeliveries. Same connected-component discipline the
    # hierarchy assembly uses.
    parent: dict[str, str] = {}
    canonicals: set[str] = set()

    def _root(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]  # path-halving
            x = parent[x]
        return x

    for group in aliases:
        members = [
            name for m in group.members if isinstance(name := m.get("column_name"), str) and name
        ]
        if not members:
            continue
        canonicals.add(group.canonical_label)
        for m in members[1:]:
            parent[_root(members[0])] = _root(m)

    classes: dict[str, list[str]] = {}
    for col in parent:
        classes.setdefault(_root(col), []).append(col)

    for members in classes.values():
        # Keep ONE representative among the class's ELECTED members (1:1 aliases
        # partition identically, so the choice is immaterial for ranking). With 0-or-1
        # elected there is nothing redundant to drop — critically, when the canonical
        # is an un-elected raw-FK near-key the slicing gate excluded, the surviving
        # member is the class's ONLY representative and must NOT be orphaned (DAT-806).
        elected = sorted(m for m in members if m in candidates)
        if len(elected) < 2:
            continue
        # Prefer an elected canonical — a manual teach's canonical is the user's chosen
        # label, NOT sorted-first (``_apply_teaches`` sets it to ``ordered[0]``), so
        # respect it over blind alphabetical order; else the sorted-first elected
        # member. Deterministic → rerun-stable.
        elected_canon = [m for m in elected if m in canonicals]
        keeper = elected_canon[0] if elected_canon else elected[0]
        for m in elected:
            if m != keeper:
                candidates.discard(m)
    return sorted(candidates)


def _identity_columns(session: Session, fact_table_id: str, run_id: str) -> list[str]:
    """The fact's persisted recurring-identity column names (DAT-565), this run.

    Read from ``TableEntity.identity_columns`` — the cluster-entity roles
    ``semantic_per_table`` named (would-be foreign keys, distinct from grain). Empty when
    none were named, so a fact with no identities falls back to the plain row-wise null.
    These are PROPOSALS; :func:`_resolve_cluster_keys` ICC-verifies them before routing.
    """
    blob = session.execute(
        select(TableEntity.identity_columns).where(
            TableEntity.table_id == fact_table_id,
            TableEntity.run_id == run_id,
        )
    ).scalar_one_or_none()
    if not blob:
        return []
    return [c["column"] for c in blob if isinstance(c, dict) and c.get("column")]


def _measure_columns(measure: Measure) -> list[str]:
    """The enriched-view columns a measure needs read."""
    if measure.target_type in ("flow", "stock"):
        return [measure.column] if measure.column else []
    assert measure.numerator and measure.denominator  # guaranteed by Measure.__post_init__
    return [measure.numerator, measure.denominator]


def _floats(frame: pl.DataFrame, col: str) -> np.ndarray:
    """A column as a C-contiguous float64 array (nulls → NaN); the numpy core's input.

    The measure columns are cast to ``DOUBLE`` at load, so this is a clean float view
    with no int→float null-upcast copy (DAT-580); ``ascontiguousarray`` keeps the
    permutation/bincount math stride-free.
    """
    return np.ascontiguousarray(frame[col].cast(pl.Float64).to_numpy(), dtype=np.float64)


def _physical_codes(col: pl.Series) -> tuple[np.ndarray, int]:
    """Physical int codes (``-1`` = null) + distinct-value count for one column (DAT-580).

    The arrow→polars categorical encoding assigns each distinct value a contiguous code;
    NULL becomes ``-1`` (the criterion's dim-null sentinel). Deterministic for a given
    column, so downstream bincount aggregation is rerun-stable.
    """
    cat = col.cast(pl.String).cast(
        pl.Categorical
    )  # String first: ints/floats aren't castable direct
    codes = np.ascontiguousarray(cat.to_physical().cast(pl.Int64).fill_null(-1).to_numpy())
    return codes, len(cat.cat.get_categories())


def _icc_measure(frame: pl.DataFrame, measure: Measure) -> np.ndarray:
    """The per-row scalar the ICC is computed on.

    The column itself (flow/stock) or the per-row ratio num/den (ratio; NaN where
    the denominator is missing or ≤ 0).
    """
    if measure.target_type in ("flow", "stock"):
        assert measure.column is not None
        return _floats(frame, measure.column)
    assert measure.numerator and measure.denominator
    num = _floats(frame, measure.numerator)
    den = _floats(frame, measure.denominator)
    valid = ~np.isnan(num) & ~np.isnan(den) & (den > 0)
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.where(valid, num / np.where(valid, den, 1.0), np.nan)


def _entity_icc(frame: pl.DataFrame, entity: str, measure: Measure) -> float:
    """The measure's ICC (η² between entities) for one identity column (DAT-563)."""
    codes, n = _physical_codes(frame[entity])
    return intraclass_correlation(codes, n, _icc_measure(frame, measure))


def _resolve_cluster_keys(
    frame: pl.DataFrame, proposed: list[str], measure: Measure, *, icc_threshold: float
) -> list[str]:
    """ICC-verify the proposed identities — keep those the measure actually clusters within.

    For each proposed identity present in ``frame``, compute the measure's ICC and keep it
    only when ICC > ``icc_threshold`` (DAT-563). **No heuristic:** an identity and a
    high-cardinality ATTRIBUTE are indistinguishable by cardinality/recurrence, so the only
    sound test is whether the measure clusters within it. Dropping an unverified column is
    load-bearing — routing a mis-named identity would de-mean a real row-level driver away.
    Proposed order is preserved (deterministic).
    """
    # Deferred (DAT-563, optional): a born-loud miss-audit — warn when a high-ICC recurring
    # column was NOT proposed as an identity — would need scanning un-read high-cardinality
    # columns (an extra view scan); it is a guardrail, never a routing fallback.
    verified: list[str] = []
    for col in proposed:
        if col not in frame.columns:  # defensive: callers pass view-filtered cols, so never fires
            continue
        icc = _entity_icc(frame, col, measure)
        if icc > icc_threshold:
            verified.append(col)
        else:
            logger.info("driver_identity_unverified", identity=col, icc=round(icc, 3))
    return verified


def _factorize_dims(
    frame: pl.DataFrame, dims: list[str]
) -> tuple[dict[str, np.ndarray], dict[str, list[str]]]:
    """Physical int codes (-1 = null) + label-per-code per dim — the tree's input (DAT-580).

    Factorizes straight from the polars frame, so dim strings never materialize as Python
    objects: the tree reasons over int codes, resolving labels only for the few surfaced
    slices. ``labels_by_dim[d][code]`` is the raw value of physical ``code``.
    """
    codes_by_dim: dict[str, np.ndarray] = {}
    labels_by_dim: dict[str, list[str]] = {}
    for d in dims:
        cat = frame[d].cast(pl.String).cast(pl.Categorical)  # String first (ints/floats)
        codes_by_dim[d] = np.ascontiguousarray(
            cat.to_physical().cast(pl.Int64).fill_null(-1).to_numpy()
        )
        labels_by_dim[d] = [str(x) for x in cat.cat.get_categories().to_list()]
    return codes_by_dim, labels_by_dim


def _make_target(measure: Measure, frame: pl.DataFrame) -> Target:
    """Build the row-aligned target from the measure's columns in ``frame``."""
    if measure.target_type in ("flow", "stock"):
        assert measure.column  # guaranteed by Measure.__post_init__
        return FlowTarget(_floats(frame, measure.column), target_type=measure.target_type)
    assert measure.numerator and measure.denominator  # guaranteed by Measure.__post_init__
    return RatioTarget(_floats(frame, measure.numerator), _floats(frame, measure.denominator))


def discover_drivers(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    fact_table_id: str,
    run_id: str,
    measure: Measure,
    cluster_keys: list[str] | None = None,
    seed: int = 0,
    max_depth: int = DEFAULT_MAX_DEPTH,
    alpha: float = DEFAULT_ALPHA,
    min_support: int = DEFAULT_MIN_SUPPORT,
    missingness_gate: float = DEFAULT_MISSINGNESS_GATE,
    n_perm: int = DEFAULT_N_PERM,
    icc_threshold: float = DEFAULT_ICC_THRESHOLD,
    min_entities: int = DEFAULT_MIN_ENTITIES,
    max_rows: int = DEFAULT_MAX_ROWS,
) -> DriverRanking:
    """Rank the catalog's dimensions as drivers of ``measure`` over the enriched view.

    Pure + deterministic for a given ``(seed, candidate-dim set)`` — the permutation
    draw sequence depends on the dims, so a future cache (DAT-546) must key on the
    candidate set too, not just ``(measure, run, seed)``. Returns an empty ranking —
    never an error — when the fact has no grain-verified enriched view, fewer than
    two candidate dims survive in the view, or the measure columns are absent
    (a catalog/view skew is logged, not fatal).

    **Cluster-aware home-grain routing (DAT-552/561/563):** ``cluster_keys`` names the
    fact's recurring identity columns (customer, product, …) — one entity grain each.
    Each candidate is routed to its **home grain**, by within-entity constancy, not by
    the measure's global ICC:

    - A candidate **constant within entity E** (one value per E) takes E's **entity-grain**
      null — collapse to one row per E, permute entities. The row-wise null is
      structurally invalid for it at any ICC > 0 (its groups are whole entities, so
      correlated within-entity rows would be counted as independent — DAT-561). Constant
      within several entities → its home is the **finest** (highest-cardinality) one.
    - A candidate **constant within none** is row-level and takes the **row-wise** null,
      valid at any ICC (it just loses power as the measure clusters — see the de-mean
      power add-on below).

    One family per entity-with-home-dims plus a row-level family are assembled into one
    ranking: the **primary** (its tree, paths, slices, ``ranked_dimensions``, ``grain``)
    is the highest-ICC entity above ``icc_threshold`` (the between-entity story is the
    headline), else row-wise; every other family's significant dims are exposed as
    ``secondary_dimensions`` — a flat list, each labeled with its ``grain`` and
    ``entity`` (not folded into the primary ranking — the grains are not cross-comparable).
    Ratio routes the same way (entity statistic = Σnum/Σden, weight = Σden). With no
    ``cluster_keys`` the plain row-wise null (DAT-545) is used. ``max_depth`` applies to
    the row-wise family; the entity grain always uses ``max_depth=1`` (recursion there is
    low-power). N=1 reduces exactly to the DAT-561 primary/secondary split.

    **v1 limit (crossed effects):** a candidate constant within NO single entity but
    clustered on two at once cannot be fully de-clustered by a single-entity de-mean; it
    is handled row-wise, de-meaned against the highest-ICC entity only. Per-entity
    marginals are surfaced; there is no joint two-way model.

    **Power add-on (DAT-561):** under HIGH ICC the row-level (secondary) family's
    row-wise null on the raw measure has little power — the between-entity variance is
    noise. It gates on the **within-entity de-meaned residual** instead (the
    fixed-effects "within" transform), which is row-exchangeable and powered — this is
    the within-entity driver analysis. Flow/stock de-mean the measure
    (``measure − entity_mean``); ratio de-means the per-row ratio by its entity's
    volume-weighted mean (its pooled ``Σnum/Σden``).

    **Bounded load (DAT-571 / DAT-580):** the ``(present_dims + measure)`` columns are read
    into memory at row grain in one arrow→polars pass, then factorized to int codes + a
    float measure view (the spike measured −67% peak RSS vs the old pandas frame at 1M×15).
    Above ``max_rows`` the view is deterministically sub-sampled to ``max_rows``
    rows via a bottom-k-by-hash sketch (the N smallest row-hashes are a uniform sample
    without replacement) rather than dropping the analysis: large finance/logistics facts
    are exactly where drivers matter most. The sketch is deterministic regardless of DuckDB
    thread count (``ORDER BY`` is a total order — ``REPEATABLE()`` only holds single-threaded,
    which the shared multi-thread worker connection can't guarantee), and uniform PER ROW, so
    unlike ``SYSTEM``/block sampling it does not shred the entity grain. Cost: the entity-grain
    family loses power on weak drivers under sampling (the permutation null is recomputed on
    the sample, so FDR holds — it degrades to a miss, never a fabricated driver). DAT-580
    (arrow-backed load) will raise the ceiling so sampling becomes a rare fallback.
    """

    def abstain(reason: AbstainReason) -> DriverRanking:
        """The honest-empty ranking (DAT-859): known target_type, no ranked content."""
        return DriverRanking(
            measure=measure.label,
            target_type=measure.target_type,
            n_rows=0,
            status=RankingStatus.ABSTAINED,
            abstain_reason=reason,
        )

    view = _enriched_view_name(session, fact_table_id, run_id)
    if view is None:
        logger.info("driver_no_enriched_view", fact_table_id=fact_table_id, run_id=run_id)
        return abstain(AbstainReason.MISSING_INPUTS)
    dims = _candidate_dims(session, fact_table_id, run_id)
    if len(dims) < 2:
        logger.info("driver_too_few_candidates", fact_table_id=fact_table_id, n=len(dims))
        return abstain(AbstainReason.INSUFFICIENT_CANDIDATES)

    def quote(name: str) -> str:
        return '"' + name.replace('"', '""') + '"'

    # Probe the view's columns first (LIMIT 0 — no scan) so a catalog/view skew is a
    # logged empty result, not a DuckDB BinderException, AND we still read only the
    # columns we need (no SELECT *). The measure columns must exist; dims intersect.
    view_cols = {
        c[0] for c in duckdb_conn.execute(f"SELECT * FROM {quote(view)} LIMIT 0").description
    }  # noqa: S608
    present_dims = [d for d in dims if d in view_cols]
    measure_cols = _measure_columns(measure)
    if len(present_dims) < 2:
        logger.info("driver_view_skew", view=view, present=present_dims, measure_cols=measure_cols)
        return abstain(AbstainReason.INSUFFICIENT_CANDIDATES)
    if any(c not in view_cols for c in measure_cols):
        logger.info("driver_view_skew", view=view, present=present_dims, measure_cols=measure_cols)
        return abstain(AbstainReason.MISSING_INPUTS)

    # Resolve the clustering identities (DAT-563). An explicit ``cluster_keys`` list is a
    # caller override (tests) used verbatim; otherwise read the fact's persisted
    # ``identity_columns`` (DAT-565) — those are PROPOSALS, ICC-verified below. Read the
    # proposed columns into the frame alongside the dims + measure.
    explicit = cluster_keys is not None
    proposed = (
        cluster_keys
        if cluster_keys is not None
        else _identity_columns(session, fact_table_id, run_id)
    )
    present_proposed = [k for k in proposed if k in view_cols]
    # Born-loud: a named identity missing from the view (LLM hallucination, or a column
    # renamed between semantic_per_table and the enriched view) is dropped — say so.
    for col in proposed:
        if col not in view_cols:
            logger.info("driver_identity_not_in_view", identity=col, view=view)
    select_cols = list(dict.fromkeys(present_dims + measure_cols + present_proposed))

    # Cast measure columns to DOUBLE in the projection so the polars→numpy handoff is a
    # clean float view (no int/decimal→float null-upcast copy, DAT-580); dims/identities
    # stay raw for categorical factorization. The hash sketch hashes the RAW columns so
    # the DAT-571 cutoff is unchanged byte-for-byte. TRY_CAST (not ::DOUBLE): a measure the
    # typing left VARCHAR — e.g. a column carrying null sentinels ('~~~~~') from a
    # null_tokens injection, or any not-cleanly-numeric measure — yields NULL→NaN, which
    # the numpy core already treats as missing (_floats nulls→NaN; ICC/targets mask
    # ~isnan), instead of a hard ConversionException that fails the whole begin_session
    # (the engine's "failed casts → null, never pipeline failure" rule).
    def project(c: str) -> str:
        return f"TRY_CAST({quote(c)} AS DOUBLE) AS {quote(c)}" if c in measure_cols else quote(c)

    select_proj = ", ".join(project(c) for c in select_cols)
    hash_cols = ", ".join(quote(c) for c in select_cols)
    # Bound the in-memory frame (DAT-571): a COUNT(*) keeps normal-size views — the common
    # case — on the validated full-load path byte-for-byte (a single plain SELECT); above
    # max_rows, deterministically sub-sample to max_rows rows via a bottom-k-by-hash sketch
    # instead of dropping the analysis. hash() is variadic over the selected columns, so
    # identical rows hash alike and the cutoff is stable across runs and thread counts. The
    # oversized branch deliberately scans twice (COUNT, then the ORDER BY) — fusing them via
    # COUNT(*) OVER () would force the sort + a reload onto the common small-view path; that
    # path is hot, the oversized path is rare, and memory (not scan time) is what we bound.
    count_row = duckdb_conn.execute(f"SELECT COUNT(*) FROM {quote(view)}").fetchone()  # noqa: S608
    assert count_row is not None  # COUNT(*) on an existing view always returns one row
    n_full = int(count_row[0])
    if n_full > max_rows:
        logger.info("driver_rankings_view_sampled", view=view, full_n=n_full, sample_n=max_rows)
        sql = f"SELECT {select_proj} FROM {quote(view)} ORDER BY hash({hash_cols}) LIMIT {max_rows}"  # noqa: S608 — catalog identifiers
    else:
        sql = f"SELECT {select_proj} FROM {quote(view)}"  # noqa: S608 — catalog identifiers
    frame = pl.from_arrow(duckdb_conn.execute(sql).to_arrow_table())
    assert isinstance(frame, pl.DataFrame)  # from_arrow on a Table is always a DataFrame

    # The resolver path ICC-verifies the persisted identities (drop those the measure does
    # not cluster within — no heuristic); an explicit override is asserted by the caller.
    keys = (
        present_proposed
        if explicit
        else _resolve_cluster_keys(frame, present_proposed, measure, icc_threshold=icc_threshold)
    )

    # Cluster-aware home-grain routing (DAT-561/563): each resolved identity column is an
    # entity grain; candidates are routed to their home grain (the entity they are
    # constant within) and ranked there, row-level candidates row-wise; the highest-ICC
    # entity (or row-wise when nothing clusters) is primary. With no verified identities,
    # the plain row-wise null (DAT-545) over all candidates.
    if keys:
        return _routed_ranking(
            frame,
            present_dims,
            measure,
            keys,
            seed=seed,
            max_depth=max_depth,
            alpha=alpha,
            min_support=min_support,
            missingness_gate=missingness_gate,
            n_perm=n_perm,
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
    )


def _entity_first_codes(ent_codes: np.ndarray, dim_codes: np.ndarray, n_ent: int) -> np.ndarray:
    """First NON-null physical code of a dim per entity (``-1`` if all-null), code-indexed.

    Matches pandas ``groupby.first()`` (which skips nulls): for an entity-constant dim the
    single value is returned regardless of where a stray null sits. Indexed by entity
    physical code ``0..n_ent-1`` (DAT-580).
    """
    out = np.full(n_ent, -1, dtype=np.int64)
    ok = (dim_codes >= 0) & (ent_codes >= 0)
    if ok.any():
        ent_ok = ent_codes[ok]
        uniq, first = np.unique(ent_ok, return_index=True)  # first occurrence per entity
        out[uniq] = dim_codes[ok][first]
    return out


def _collapse_to_entity(
    frame: pl.DataFrame, cluster_key: str, measure: Measure, entity_dims: list[str]
) -> tuple[np.ndarray, np.ndarray, dict[str, np.ndarray], dict[str, list[str]]]:
    """One row per entity: (statistic, weight, entity-level dim codes, labels), aligned.

    flow/stock → (mean measure, observed-row count); ratio → (Σnum/Σden, Σden), the
    support-correct entity ratio weighted by its denominator mass. Entities with no usable
    measure are dropped; the dim codes are the entity's representative value (constant
    within entity), kept-aligned. Aggregation is numpy ``bincount`` over the entity's
    physical codes — deterministic across runs/threads (the rerun-determinism contract),
    unlike a threaded polars group-sum (DAT-580).
    """
    ent_codes, n_ent = _physical_codes(frame[cluster_key])
    if measure.target_type == "ratio":
        assert measure.numerator and measure.denominator
        num = _floats(frame, measure.numerator)
        den = _floats(frame, measure.denominator)
        valid = ~np.isnan(num) & ~np.isnan(den) & (den > 0) & (ent_codes >= 0)
        sum_num = np.bincount(ent_codes[valid], weights=num[valid], minlength=n_ent)
        sum_den = np.bincount(ent_codes[valid], weights=den[valid], minlength=n_ent)
        keep = sum_den > 0
        with np.errstate(divide="ignore", invalid="ignore"):
            values = (sum_num / np.where(sum_den > 0, sum_den, 1.0))[keep]
        sizes = sum_den[keep]  # weight = denominator mass
    else:
        assert measure.column is not None
        m = _floats(frame, measure.column)
        obs = ~np.isnan(m) & (ent_codes >= 0)
        count = np.bincount(ent_codes[obs], minlength=n_ent)
        total = np.bincount(ent_codes[obs], weights=m[obs], minlength=n_ent)
        keep = count > 0
        with np.errstate(divide="ignore", invalid="ignore"):
            values = (total / np.where(count > 0, count, 1))[keep]
        sizes = count[keep].astype(float)

    codes_by_dim, labels_by_dim = _factorize_dims(frame, entity_dims)
    entity_codes_by_dim = {
        d: _entity_first_codes(ent_codes, codes_by_dim[d], n_ent)[keep] for d in entity_dims
    }
    return values, sizes, entity_codes_by_dim, labels_by_dim


def _partition_by_entity_constancy(
    frame: pl.DataFrame, cluster_key: str, dims: list[str]
) -> tuple[list[str], list[str]]:
    """Split candidates into ``(entity_constant, row_level)`` by within-entity nunique.

    Entity-constant = one value per entity (nunique ≤ 1) → the entity-grain null;
    everything else varies within entity → the row-wise null (DAT-561). This is the
    routing decision: it is per-candidate, independent of the measure's global ICC.
    """
    # ``drop_nulls().n_unique()`` counts non-null distinct values (matching pandas
    # ``nunique``), so ``<= 1`` also catches an all-null dim (0) as entity-constant —
    # harmless: it contributes nothing (every row gated out by the (A) gate) wherever it
    # lands, exactly as on the old row-wise path.
    if not dims:
        return [], []
    # Prefix the aggregate aliases so a candidate dim that IS the cluster_key (constant
    # within its own group → nunique 1) doesn't collide with the group-key column.
    agg = frame.group_by(cluster_key).agg(
        [pl.col(d).drop_nulls().n_unique().alias(f"__nu__{i}") for i, d in enumerate(dims)]
    )
    maxes = agg.select([f"__nu__{i}" for i in range(len(dims))]).max().row(0)
    entity_constant = [d for d, m in zip(dims, maxes, strict=True) if int(m) <= 1]
    row_level = [d for d in dims if d not in entity_constant]
    return entity_constant, row_level


def _within_entity_residual(frame: pl.DataFrame, cluster_key: str, column: str) -> np.ndarray:
    """The fixed-effects "within" transform: ``measure − entity_mean`` (DAT-561).

    Removes the between-entity level so the row-wise null on the residual is both valid
    (residuals are row-exchangeable within entity) and powered for a within-entity
    row-level driver — the entity-mean subtraction strips the clustered variance that
    would otherwise swamp it. NaN measure rows (and null-entity rows) stay NaN. The
    entity mean is a deterministic numpy ``bincount`` (DAT-580).
    """
    measure = _floats(frame, column)
    ent_codes, n_ent = _physical_codes(frame[cluster_key])
    obs = ~np.isnan(measure) & (ent_codes >= 0)
    count = np.bincount(ent_codes[obs], minlength=n_ent)
    total = np.bincount(ent_codes[obs], weights=measure[obs], minlength=n_ent)
    with np.errstate(divide="ignore", invalid="ignore"):
        entity_mean = np.where(count > 0, total / np.where(count > 0, count, 1), np.nan)
    gather = np.where(ent_codes >= 0, ent_codes, 0)  # safe index; null-entity rows → NaN below
    return np.where(ent_codes >= 0, measure - entity_mean[gather], np.nan)


def _within_entity_ratio_residual(
    frame: pl.DataFrame, cluster_key: str, numerator: str, denominator: str
) -> tuple[np.ndarray, np.ndarray]:
    """``(residual_ratio, weight)`` for the within-entity de-meaned RATIO (DAT-561).

    The per-row ratio ``r = num/den`` minus its entity's VOLUME-WEIGHTED mean — which is
    the entity's pooled ratio ``Σnum/Σden`` (the weighted mean of ``r`` with weight
    ``den``). Strips the between-entity ratio level so the row-wise null on the residual
    is valid + powered for a within-entity ratio driver. NaN where the row has no usable
    ratio (missing/≤0 denominator); ``weight`` is the denominator mass (0 where invalid).
    """
    num = _floats(frame, numerator)
    den = _floats(frame, denominator)
    codes, n_ent = _physical_codes(frame[cluster_key])
    # A NaN cluster key factorizes to code -1 (no entity to de-mean against): exclude it
    # — bincount rejects negative codes, and a -1 gather would wrap to the last entity.
    valid = ~np.isnan(num) & ~np.isnan(den) & (den > 0) & (codes >= 0)
    sum_num = np.bincount(codes[valid], weights=num[valid], minlength=n_ent)
    sum_den = np.bincount(codes[valid], weights=den[valid], minlength=n_ent)
    with np.errstate(divide="ignore", invalid="ignore"):
        entity_ratio = np.where(sum_den > 0, sum_num / np.where(sum_den > 0, sum_den, 1.0), np.nan)
        r = np.where(valid, num / np.where(valid, den, 1.0), np.nan)
    gather = np.where(codes >= 0, codes, 0)  # safe index; invalid rows masked to NaN below
    residual = np.where(valid, r - entity_ratio[gather], np.nan)
    weight = np.where(valid, den, 0.0)
    return residual, weight


def _home_grain_partition(
    frame: pl.DataFrame, cluster_keys: list[str], dims: list[str]
) -> tuple[dict[str, list[str]], list[str]]:
    """Assign each candidate its HOME grain — the entity it is constant within (DAT-563).

    Per entity, reuse the validated :func:`_partition_by_entity_constancy` nunique logic
    to find the dims constant within it. A dim constant within ONE entity homes there; a
    dim constant within SEVERAL homes at the **finest** (highest-cardinality) one — the
    most specific grain — with a deterministic name tiebreak; a dim constant within none
    is row-level. A dim SATURATED against its home entity (a 1:1 alias of the key) is
    DROPPED — neither homed nor row-level (DAT-695); callers must not assume every dim
    survives. Returns ``({entity: [home dims]}, row_dims)`` with empty entities dropped.
    """
    card = {e: int(frame[e].drop_nulls().n_unique()) for e in cluster_keys}
    constant_within = {
        e: set(_partition_by_entity_constancy(frame, e, dims)[0]) for e in cluster_keys
    }
    home_by_entity: dict[str, list[str]] = {e: [] for e in cluster_keys}
    row_dims: list[str] = []
    for d in dims:
        homes = [e for e in cluster_keys if d in constant_within[e]]
        if not homes:
            row_dims.append(d)
            continue
        home = max(homes, key=lambda e: (card[e], e))
        # A dim SATURATED against its home entity — as many distinct values as
        # the entity has members, while constant within each — is a 1:1 alias
        # of the key itself. Ranking an entity's own renaming across those same
        # entities is structurally information-free, and worse: it fabricates a
        # family whose ranking is guaranteed empty and can win headline
        # precedence (DAT-695: business_id ↔ created_user, 27 = 27). Drop it —
        # neither a home dim nor row-level (row-wise it is still just the key).
        if int(frame[d].drop_nulls().n_unique()) == card[home]:
            logger.info("driver_alias_dim_dropped", dim=d, entity=home)
            continue
        home_by_entity[home].append(d)
    return {e: ds for e, ds in home_by_entity.items() if ds}, row_dims


def _routed_ranking(
    frame: pl.DataFrame,
    dims: list[str],
    measure: Measure,
    cluster_keys: list[str],
    *,
    seed: int,
    max_depth: int,
    alpha: float,
    min_support: int,
    missingness_gate: float,
    n_perm: int,
    icc_threshold: float,
    min_entities: int,
) -> DriverRanking:
    """Route candidates to per-entity home grains + row-wise; primary = highest-ICC entity.

    Reuse-orchestrate (DAT-563): the validated :func:`_entity_grain_ranking` and
    :func:`_row_wise_ranking` are called verbatim, once per family. N=1 reduces exactly to
    the DAT-561 two-family split. Each family carries ``(ranking, grain, entity)``; the
    primary is the highest-ICC entity family when the measure clusters, the row family
    otherwise (with low-ICC entity families behind it as a deterministic fallback). Every
    non-primary family's dims surface as grain+entity-labeled ``secondary_dimensions``.
    """
    icc_by_entity = {e: _entity_icc(frame, e, measure) for e in cluster_keys}
    top_entity = max(cluster_keys, key=lambda e: (icc_by_entity[e], e))
    high_icc = icc_by_entity[top_entity] > icc_threshold
    home_by_entity, row_dims = _home_grain_partition(frame, cluster_keys, dims)
    logger.info(
        "driver_home_grain_routing",
        cluster_keys=cluster_keys,
        icc={e: round(v, 3) for e, v in icc_by_entity.items()},
        n_entities={e: int(frame[e].drop_nulls().n_unique()) for e in cluster_keys},
        primary=f"entity:{top_entity}" if high_icc else "row",
        home_dims=home_by_entity,
        row_level=row_dims,
    )

    # One family per entity-with-home-dims (deterministic seed by sorted name), plus the
    # row-level family. The row family de-means within the highest-ICC entity ONLY under
    # high ICC (the DAT-561 power add-on; the v1 crossed-effects limit lives here).
    families: list[tuple[DriverRanking, str, str | None]] = []
    for i, e in enumerate(sorted(home_by_entity)):
        families.append(
            (
                _entity_grain_ranking(
                    frame,
                    home_by_entity[e],
                    measure,
                    e,
                    seed=seed + i,
                    alpha=alpha,
                    n_perm=n_perm,
                    min_entities=min_entities,
                ),
                "entity",
                e,
            )
        )
    if row_dims:
        families.append(
            (
                _row_wise_ranking(
                    frame,
                    row_dims,
                    measure,
                    seed=seed + len(cluster_keys),
                    max_depth=max_depth,
                    alpha=alpha,
                    min_support=min_support,
                    missingness_gate=missingness_gate,
                    n_perm=n_perm,
                    cluster_key=top_entity if high_icc else None,
                ),
                "row",
                None,
            )
        )

    # Primary precedence: high-ICC entity families (by ICC desc) → row family → low-ICC
    # entity families. The first is the headline; the rest become labeled secondaries.
    def precedence(fam: tuple[DriverRanking, str, str | None]) -> tuple[int, float, str]:
        _ranking, grain, entity = fam
        if grain == "entity" and entity is not None:  # entity families always carry a name
            ic = icc_by_entity[entity]
            return (0 if ic > icc_threshold else 2, -ic, entity)
        return (1, 0.0, "")  # row family sits between high- and low-ICC entity families

    families.sort(key=precedence)
    # The alias-drop above can discard EVERY candidate (a table whose only dims
    # were renamings of its entity keys) — no family exists then; return the
    # honest empty ranking rather than index into nothing (DAT-695 review).
    if not families:
        return DriverRanking(
            measure=measure.label,
            target_type=measure.target_type,
            n_rows=frame.height,
            status=RankingStatus.ABSTAINED,
            abstain_reason=AbstainReason.INSUFFICIENT_CANDIDATES,
        )
    # The headline must carry content: an empty high-ICC entity family ahead of
    # a non-empty row family would bury every real driver in ``secondary`` and
    # persist ``ranked: 0`` (DAT-695). Take the first family WITH ranked
    # dimensions — but never past the row family: the low-ICC entity families
    # behind it are DELIBERATELY demoted (DAT-561 — at low ICC the measure does
    # not cluster by that entity, so its grain must not headline), content or
    # not. All of buckets 0–1 empty → strict precedence, as before.
    primary_idx = next(
        (
            i
            for i, fam in enumerate(families)
            if fam[0].ranked_dimensions and precedence(fam)[0] <= 1
        ),
        0,
    )
    primary, _primary_grain, primary_entity = families[primary_idx]
    secondary = [
        SecondaryDriver(d, g, grain, entity)
        for i, (ranking, grain, entity) in enumerate(families)
        if i != primary_idx
        for d, g in ranking.ranked_dimensions
    ]
    return replace(primary, secondary_dimensions=secondary, entity=primary_entity)


def _row_wise_ranking(
    frame: pl.DataFrame,
    dims: list[str],
    measure: Measure,
    *,
    seed: int,
    max_depth: int,
    alpha: float,
    min_support: int,
    missingness_gate: float,
    n_perm: int,
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
    codes_by_dim, labels_by_dim = _factorize_dims(frame, dims)
    return discover_tree(
        codes_by_dim,
        labels_by_dim,
        target,
        measure_label=measure.label,
        dims=dims,
        rng=np.random.default_rng(seed),
        max_depth=max_depth,
        alpha=alpha,
        min_support=min_support,
        missingness_gate=missingness_gate,
        n_perm=n_perm,
    )


def _entity_grain_ranking(
    frame: pl.DataFrame,
    entity_dims: list[str],
    measure: Measure,
    cluster_key: str,
    *,
    seed: int,
    alpha: float,
    n_perm: int,
    min_entities: int,
) -> DriverRanking:
    """Collapse to one row per entity and rank the (pre-partitioned) entity-level dims.

    ``entity_dims`` are already constant within entity (the caller's routing). The
    entity statistic is the mean measure weighted by observed-row count (flow/stock) or
    Σnum/Σden weighted by Σden (ratio); entities with no usable measure are dropped.
    Single-level (``max_depth=1``): recursion at entity grain is low-power and a
    follow-up.
    """
    values, sizes, codes_by_dim, labels_by_dim = _collapse_to_entity(
        frame, cluster_key, measure, entity_dims
    )
    if values.size == 0:  # every entity has no usable measure — nothing to rank
        return DriverRanking(
            measure=measure.label,
            target_type=measure.target_type,
            n_rows=0,
            grain="entity",
            status=RankingStatus.ABSTAINED,
            abstain_reason=AbstainReason.INSUFFICIENT_DATA,
        )

    target = EntityMeanTarget(values, sizes, target_type=measure.target_type)
    return discover_tree(
        codes_by_dim,
        labels_by_dim,
        target,
        measure_label=measure.label,
        dims=entity_dims,
        rng=np.random.default_rng(seed),
        max_depth=1,
        alpha=alpha,
        min_support=min_entities,
        n_perm=n_perm,
    )
