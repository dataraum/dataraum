"""Dimension-identity discovery over the enriched views (DAT-761, stack v4 from DAT-757).

For each fact's grain-verified enriched view, EVERY dimension-like view column
is a candidate (measures excluded by their ``semantic_role`` — the additivity
lane: ``revenue → tier`` is reliably asserted by every statistic, so measures
never enter FD discovery). The upstream pipeline ``max_columns`` limit is the
only width cap; all other exclusions are data-grounded guards, each logged
(born-loud).

The statistics are deterministic and decide everything EXCEPT one class they
cannot separate: a relabeling bijection (code ↔ name) and a coincidental 1:1
(an entity key that lines up with a per-row timestamp) are identical to every
statistic (g3 = 0, λ = 1, both survive the null). That one call is the identity
judge (DAT-762 §5b); a failed call surfaces the pair, never merges it.

The decision layer is the DAT-757 gate stack (32/32 on the adversarial matrix,
100% recoverable-truth recall on rel-f1/rel-hm/rel-salt folded by their own FK
metadata — verdicts on DAT-757, build ticket DAT-761):

1. **Null policy** — eligibility counts NULL as a category (a null-coded binary
   ``{1, NULL}`` is a lane, not a silent constant-drop); row statistics use
   null-as-category codes.
2. **Effect screens** — EDGES: classic row-g3 ≤ 0.01 (``a`` determines ``b`` up
   to a 1% dirty-row tolerance) + determinant guards; ALIASES: pair-count g3
   ≤ 0.01 in both directions (the conservative semantics for near-copies).
3. **Goodman–Kruskal λ ≥ 0.5** (edge arm) — kills the vacuous-skew class
   (≥98%-dominant dependents pass g3 vacuously; exact FDs keep λ = 1).
4. **Permutation p + Benjamini–Hochberg** (q ≤ 0.05) over the view's screened
   family — what discovery ASSERTS. Seeded, so a redelivered run converges.
5. **Disagreement-set role check** (alias arm) — value-equality near-copies
   (0 < disagree ≤ 5%) are classified ROLE / VALUE-SYSTEMATIC / ABSTAIN / DIRT
   from the disagreement set; a ROLE pair (bill-to vs pay-to) is persisted as
   ``kind='role'`` and never merged; undecidable near-copies surface as
   ``needs_confirmation`` aliases instead of being silently merged.

Scan grain (the rel-hm lesson): guards and pair counts come from a FULL-view
scan (a row sample makes fold keys look near-key); row-level statistics run on
an aligned sample capped by ``MAX_SAMPLE_CELLS`` — inference is sample-honest,
guards are exact.

Edges become drill-down hierarchies (``zip → city → state``) after transitive
reduction; merged alias groups collapse to one canonical axis (the redundant-axis
dedup the DAT-545 driver tree consumes). Role pairs deliberately stay separate
axes — collapsing them was the BILLTO↔PAYER over-merge the role check reverses.
"""

from __future__ import annotations

import hashlib
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import numpy as np
import polars as pl
from sqlalchemy import delete, select

from dataraum.analysis.hierarchies import stats
from dataraum.analysis.hierarchies.db_models import (
    DimensionHierarchy,
    HierarchyMember,
    RoleEvidence,
)
from dataraum.analysis.hierarchies.overlay import hierarchy_overlay_specs
from dataraum.analysis.hierarchies.stats import RoleVerdict
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.semantic.utils import load_column_concepts
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

    from dataraum.analysis.hierarchies.judge import (
        AliasIdentityVerdict,
        DimensionIdentityJudge,
    )

logger = get_logger(__name__)

# Effect floor: g3 at or below this is an approximate FD (tolerates a small
# fraction of dirty-data violations). A true FD over clean data is g3 = 0.
FD_MAX_G3 = 0.01

# Goodman–Kruskal λ floor for asserted edges: the determinant must explain at
# least half the baseline (majority-vote) prediction error — the PRE midpoint,
# pre-registered in DAT-757 before the RelBench run (48 vacuous extras killed,
# 0 truth lost).
LAMBDA_MIN = 0.5

# BH false-discovery rate over one view's effect-screened candidate family.
# Must stay below stats.MAX_SOUND_Q: the permutation early-stop is conservative
# only while a stopped p can never be BH-rejected (see stats.py).
Q_FDR = 0.05
assert Q_FDR < stats.MAX_SOUND_Q, "early-stop soundness requires Q_FDR < MAX_SOUND_Q"

# A near-copy pair (value-equality disagreement in (0, this]) takes the gate-#2
# role check before it may merge. Above it, a bidirectional pair-g3 match is a
# relabeling bijection (code ↔ name), not a near-copy — merge semantics differ.
ROLE_MAX_DISAGREE = 0.05

# Eligibility (null-aware: NULL counts as a category — the null-policy lane).
# A 1-category column is not an axis; a 2-value column is a legitimate coarsest
# level. Determinants must distinguish ≥ 3 values (non-vacuous) and be below the
# near-key fraction (a near-unique column determines anything — spurious).
MIN_DISTINCT_DIMENSION = 2
MIN_DISTINCT_DETERMINANT = 3
NEAR_KEY_FRAC = 0.9
# The near-key determinant screen only fires on a table of at least this many
# rows: on a tiny table a genuine dimension's distinct/rows climbs toward 1.0
# (account_id is already 27/332 = 0.081 on a 332-row trial_balance), so screening
# by fraction alone would clip real dimensions on small tables (DAT-762 threshold
# pin, comment 16786). Below it, a near-key column is surfaced, not excluded.
MIN_ROWS_NEARKEY = 10

# Structures resting on fewer rows than this are surfaced for confirmation
# (``needs_confirmation``) rather than auto-asserted.
MIN_SUPPORT_ROWS = 100

# The within-view identity judge's operating point (DAT-762): a relabeling
# bijection is MERGED (axes collapsed) only when the judge's identity confidence
# is at least this high; below it the pair is surfaced as a needs_confirmation
# alias and NOT collapsed. Auto-merge is the irreversible, silently-corrupting
# action.
#
# The confidence is a DIRECTIONAL, evidence-anchored number (dimension_alias.yaml,
# modelled on the semantic agent's name-readability convention): how clearly the
# names and values show the pair to be ONE entity re-encoded, from a clear
# coincidence at 0.0 to a clear alias at 1.0, decoupled from the always-true 1:1.
#
# The floor mirrors the semantic relationship judge's REL_CONFIRM_MIN = 0.7
# (semantic/processor.py): that judge is also verdict-in-confidence and lands
# bimodally — coincidental low, aliases high, an empty dead zone between — where
# 0.7 sits IN the dead zone, the judge's own decision boundary, not a top-band
# cutoff that would discard the gradation and reduce the float to a boolean.
# Confirmed on held-out data (DAT-762 re-histogram): true aliases 0.95-0.98,
# coincidental bijections (held-out + constructed) 0.03-0.10 — a +0.85 gap with
# the whole 0.2-0.9 range empty, so 0.7 sits in it and no coincidental merges.
IDENTITY_MERGE_MIN = 0.7

# Sample values per column sent to the identity judge (evidence, not a decision
# surface). These columns were already sent to the LLM by the semantic phase, so
# this adds no new data exposure; kept small — names + cardinality carry most of
# the signal (the identity gate ran on ≤8).
_IDENTITY_SAMPLE_VALUES = 6

# Row-statistics working set: rows × candidate columns pulled into memory
# (the DAT-580 drivers precedent). Guards never depend on the sample.
MAX_SAMPLE_CELLS = 40_000_000
MIN_SAMPLE_ROWS = 50_000

# Fixed seed: the permutation null must be identical across Temporal
# success-redeliveries so the run-versioned upsert converges (the row sample is
# deterministic by construction — bottom-k-by-hash, see _pull_sample). Each
# candidate derives its OWN generator from this seed + a stable digest of the
# pair (see _pair_rng), so results are independent of iteration order and of
# the permutation pool's worker count.
_PERM_SEED = 20260714

# The screened candidates' permutation tests are pure numpy (no session, no
# DuckDB) and embarrassingly parallel; numpy's sort kernels release the GIL.
# Bounded modestly — the Temporal worker already runs phases concurrently.
_PERM_WORKERS = max(1, min(8, (os.cpu_count() or 2) - 1))


def _pair_rng(arm: str, a: str, b: str) -> np.random.Generator:
    """A per-candidate generator seeded from a stable digest of the pair.

    ``hash()`` is process-salted (PYTHONHASHSEED), so a keyed digest is used —
    the draw sequence for a candidate is a pure function of the module seed and
    the (arm, a, b) identity, never of scheduling.
    """
    digest = int.from_bytes(
        hashlib.blake2b(f"{arm}:{a}->{b}".encode(), digest_size=8).digest(), "big"
    )
    return np.random.default_rng((_PERM_SEED, digest))


# COUNT(DISTINCT (a, b)) aggregates per scan statement — wide views chunk the
# pair family across several single-scan queries instead of one giant SELECT.
_MAX_PAIR_AGGS = 500

# The permutation stage's own row ceiling, independent of the cells budget: a
# narrow-tall view (2-4 columns) would otherwise sample 10-20M rows and pay
# reps × O(n log n) per ACCEPTED candidate (true edges never early-stop). The
# test is sample-honest at any subsample, and 1M rows is ample power for
# candidates that already cleared the g3 + λ screens. The pulled frame is in
# hash order (bottom-k), so a prefix is itself a valid deterministic sample.
_MAX_PERM_ROWS = 1_000_000


def _quote(name: str) -> str:
    """Quote a catalog identifier for DuckDB SQL, doubling embedded quotes.

    View column names descend from source CSV headers (VARCHAR-first load), so
    an embedded ``"`` is possible — the drivers module's convention
    (``drivers/processor.py::quote``), adopted here.
    """
    return '"' + name.replace('"', '""') + '"'


@dataclass(frozen=True)
class _Candidate:
    """A resolved candidate dimension column on one enriched view."""

    column_name: str  # the enriched-view column (member identity)
    column_id: str  # the source column's catalog id (provenance; "" if unresolved)


@dataclass
class _ViewScan:
    """Full-view scan facts: everything the guards and the alias arm need."""

    n: int
    d2: dict[str, int]  # null-aware distinct counts (NULL = one category)
    d_sql: dict[str, int]  # SQL COUNT(DISTINCT) (null-blind; display metadata)
    # Row-literal pair distincts, (a < b) key order. Only ALIAS-SATISFIABLE pairs
    # are scanned: d_ab ≥ max(d2) always, so bidirectional pair-g3 ≤ 0.01 forces
    # the two distinct counts within 1% of each other — every other pair is
    # pruned from the O(k²) joint family without changing any decision.
    joints: dict[tuple[str, str], int]

    def pair_g3(self, a: str, b: str) -> tuple[float, float]:
        """Pair-count g3 for (a → b, b → a), null-aware numerators.

        A pair pruned from the joint scan cannot satisfy the alias screen —
        it reads as (1.0, 1.0), never an alias.
        """
        d_ab = self.joints.get((a, b) if (a, b) in self.joints else (b, a), 0)
        if d_ab == 0:
            return 1.0, 1.0
        return 1.0 - self.d2[a] / d_ab, 1.0 - self.d2[b] / d_ab


def _view_columns(duckdb_conn: duckdb.DuckDBPyConnection, view_name: str) -> list[str] | None:
    """The view's column names, or ``None`` if it is not queryable (logged)."""
    try:
        rows = duckdb_conn.execute(f"DESCRIBE {_quote(view_name)}").fetchall()
    except Exception as e:  # noqa: BLE001 — any DuckDB error → skip this view, logged
        logger.warning("hierarchy_view_describe_failed", view=view_name, error=str(e))
        return None
    return [str(r[0]) for r in rows]


def _resolve_candidates(
    session: Session, ev: EnrichedView, view_cols: list[str]
) -> dict[str, _Candidate]:
    """Resolve view columns to source-column provenance and drop measures.

    Fact-own columns resolve by name on the fact table; joined dim columns
    (``{fk}__{attr}``, builder.py convention) resolve to the dim table's ``attr``
    column via the exposed joins. An unresolvable column keeps ``column_id=""``
    (it still participates — provenance is metadata, not a gate).

    Measures are excluded BEFORE discovery (the additivity lane):
    ``semantic_role`` is object-grain, read by ``column_id`` without a run filter
    (the ``drivers/persistence.py`` convention).
    """
    fact_ids = {
        c.column_name: c.column_id
        for c in session.execute(
            select(Column).where(Column.table_id == ev.fact_table_id)
        ).scalars()
    }
    dim_table_by_fk: dict[str, str] = {
        str(j["fact_fk_column"]): str(j["dim_table_name"])
        for j in (ev.exposed_dimension_joins or [])
    }
    dim_ids: dict[tuple[str, str], str] = {}
    if ev.dimension_table_ids:
        name_by_id = {
            t.table_id: t.table_name
            for t in session.execute(
                select(Table).where(Table.table_id.in_(ev.dimension_table_ids))
            ).scalars()
        }
        for c in session.execute(
            select(Column).where(Column.table_id.in_(ev.dimension_table_ids))
        ).scalars():
            tname = name_by_id.get(c.table_id)
            if tname:
                dim_ids[(tname, c.column_name)] = c.column_id

    by_name: dict[str, _Candidate] = {}
    for name in view_cols:
        column_id = fact_ids.get(name, "")
        if not column_id and "__" in name:
            fk, attr = name.split("__", 1)
            column_id = dim_ids.get((dim_table_by_fk.get(fk, ""), attr), "")
        by_name[name] = _Candidate(column_name=name, column_id=column_id)

    resolved = [c.column_id for c in by_name.values() if c.column_id]
    measure_ids: set[str] = set()
    if resolved:
        measure_ids = set(
            session.execute(
                select(SemanticAnnotation.column_id).where(
                    SemanticAnnotation.column_id.in_(resolved),
                    SemanticAnnotation.semantic_role == "measure",
                )
            ).scalars()
        )
    for name in list(by_name):
        if by_name[name].column_id in measure_ids:
            logger.info("hierarchy_column_excluded", column=name, reason="measure")
            del by_name[name]
    return by_name


def _scan_view(
    duckdb_conn: duckdb.DuckDBPyConnection, view_name: str, cols: list[str]
) -> _ViewScan | None:
    """Full-view scan: row count, per-column distinct/null counts, pair distincts.

    Chunked — the pair family is O(k²) aggregates, split across single-scan
    queries of ≤ ``_MAX_PAIR_AGGS`` each. Returns ``None`` on failure (logged) —
    the view is then skipped, a visible abstention.
    """
    parts = ["COUNT(*) AS n"]
    parts += [f"COUNT(DISTINCT {_quote(c)}) AS d{i}" for i, c in enumerate(cols)]
    parts += [f"COUNT({_quote(c)}) AS c{i}" for i, c in enumerate(cols)]
    try:
        row = duckdb_conn.execute(
            f"SELECT {', '.join(parts)} FROM {_quote(view_name)}"  # noqa: S608 — catalog names
        ).fetchone()
        if row is None:
            return None
        n = int(row[0])
        d_sql = {c: int(row[1 + i]) for i, c in enumerate(cols)}
        non_null = {c: int(row[1 + len(cols) + i]) for i, c in enumerate(cols)}
        d2 = {c: d_sql[c] + (1 if non_null[c] < n else 0) for c in cols}

        # Joint distincts serve ONLY the alias screen, and d_ab ≥ max(d2_a, d2_b)
        # makes bidirectional g3 ≤ FD_MAX_G3 impossible unless the two distinct
        # counts sit within that tolerance of each other — prune the rest (on
        # wide/large views this cuts the O(k²) aggregate family by orders of
        # magnitude with zero semantic change).
        pairs = [
            (cols[i], cols[j])
            for i in range(len(cols))
            for j in range(i + 1, len(cols))
            if max(d2[cols[i]], d2[cols[j]]) <= min(d2[cols[i]], d2[cols[j]]) / (1.0 - FD_MAX_G3)
        ]
        joints: dict[tuple[str, str], int] = {}
        for start in range(0, len(pairs), _MAX_PAIR_AGGS):
            chunk = pairs[start : start + _MAX_PAIR_AGGS]
            sql = ", ".join(
                f"COUNT(DISTINCT ({_quote(a)}, {_quote(b)})) AS j{k}"
                for k, (a, b) in enumerate(chunk)
            )
            jrow = duckdb_conn.execute(f"SELECT {sql} FROM {_quote(view_name)}")  # noqa: S608
            fetched = jrow.fetchone()
            if fetched is None:
                return None
            for k, pair in enumerate(chunk):
                joints[pair] = int(fetched[k])
    except Exception as e:  # noqa: BLE001 — any DuckDB error → skip this view, logged
        logger.warning("hierarchy_scan_failed", view=view_name, error=str(e))
        return None
    return _ViewScan(n=n, d2=d2, d_sql=d_sql, joints=joints)


def _pull_sample(
    duckdb_conn: duckdb.DuckDBPyConnection, view_name: str, cols: list[str], n_rows: int
) -> pl.DataFrame | None:
    """An aligned VARCHAR sample of the candidate columns for the row statistics.

    Full view when it fits ``MAX_SAMPLE_CELLS``; else a bottom-k-by-hash sketch
    (the DAT-571 drivers idiom: ``ORDER BY hash(cols) LIMIT n`` stays
    deterministic on the multi-threaded worker connection, where ``USING SAMPLE
    … REPEATABLE`` only holds single-threaded — LIMIT ties are content-identical
    rows, so the cut is stable). Note the sample is tuple-clustered, not iid:
    all copies of a low-hash value-tuple enter together — first-order unbiased,
    but the variance structure differs from row-iid sampling. Row-level
    statistics are sample-honest — g3 exactness is subset-invariant, the
    permutation null is recomputed on the sample — while every guard reads the
    full-view scan, so a sampled fold key can never trip the near-key guard
    (the rel-hm lesson).
    """
    n_sample = max(MIN_SAMPLE_ROWS, MAX_SAMPLE_CELLS // max(len(cols), 1))
    sample = ""
    if n_rows > n_sample:
        hash_cols = ", ".join(_quote(c) for c in cols)
        sample = f" ORDER BY hash({hash_cols}) LIMIT {n_sample}"
        logger.info("hierarchy_view_sampled", view=view_name, full_n=n_rows, sample_n=n_sample)
    select_cols = ", ".join(f"CAST({_quote(c)} AS VARCHAR) AS {_quote(c)}" for c in cols)
    try:
        table = duckdb_conn.execute(
            f"SELECT {select_cols} FROM {_quote(view_name)}{sample}"  # noqa: S608 — catalog names
        ).arrow()
    except Exception as e:  # noqa: BLE001 — any DuckDB error → skip this view, logged
        logger.warning("hierarchy_sample_pull_failed", view=view_name, error=str(e))
        return None
    return cast("pl.DataFrame", pl.from_arrow(table))


def _break_cycles(edges: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Drop the edges inside directed cycles (Kahn peel), born-loud.

    The old pass's acyclicity premise (one global distinct count orients every
    edge) weakened when orientation moved to pairwise-complete row subsets: a
    rep-level cycle is constructible when cardinalities sit within the screen
    tolerances. A cycle is contradictory determination evidence — the honest
    output is NO chain through it, logged loudly, never a hang or a crash.
    Edges from acyclic nodes INTO the cycle survive (their target becomes a
    sink); edges within the cyclic core are dropped.
    """
    nodes = {n for e in edges for n in e}
    indeg = dict.fromkeys(nodes, 0)
    succ: dict[str, list[str]] = {n: [] for n in nodes}
    for a, b in edges:
        succ[a].append(b)
        indeg[b] += 1
    queue = [n for n in nodes if indeg[n] == 0]
    while queue:
        n = queue.pop()
        for m in succ[n]:
            indeg[m] -= 1
            if indeg[m] == 0:
                queue.append(m)
    cyclic = {n for n, d in indeg.items() if d > 0}
    if not cyclic:
        return edges
    kept = {(a, b) for a, b in edges if not (a in cyclic and b in cyclic)}
    logger.warning(
        "hierarchy_cycle_detected",
        cyclic_columns=sorted(cyclic),
        dropped_edges=sorted(edges - kept),
    )
    return kept


def _transitive_reduction(edges: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Remove edges implied by a longer path (DAG; ``a → b`` = a determines b).

    Drops ``a → c`` whenever ``a → … → c`` exists through an intermediate, so a
    chain ``zip → city → state`` keeps only the adjacent links. Input must be
    acyclic (``_break_cycles`` runs first), so a simple reachability test per
    edge suffices.
    """
    succ: dict[str, set[str]] = {}
    for a, b in edges:
        succ.setdefault(a, set()).add(b)

    def reaches(start: str, target: str, skip: tuple[str, str]) -> bool:
        stack = [start]
        seen = {start}
        while stack:
            node = stack.pop()
            for nxt in succ.get(node, ()):
                if (node, nxt) == skip:
                    continue
                if nxt == target:
                    return True
                if nxt not in seen:
                    seen.add(nxt)
                    stack.append(nxt)
        return False

    return {(a, b) for (a, b) in edges if not reaches(a, b, skip=(a, b))}


# Path enumeration is worst-case exponential on dense DAGs, and the widened
# candidate universe makes dense decided DAGs reachable — cap the emitted
# chains and say so, never hang.
_MAX_CHAINS_PER_VIEW = 500


def _maximal_chains(edges: set[tuple[str, str]]) -> list[list[str]]:
    """Every maximal path (length ≥ 2 nodes) through the reduced DAG, finest→coarsest.

    A node with no incoming reduced edge is a chain start (the finest level); a
    node with no outgoing edge is the end (coarsest). Branching yields multiple
    chains. Deterministic: starts and successors are sorted; the walk is
    iterative (no recursion limit) and capped born-loud at
    ``_MAX_CHAINS_PER_VIEW``. Input must be a DAG (``_break_cycles`` runs first).
    """
    succ: dict[str, list[str]] = {}
    has_incoming: set[str] = set()
    for a, b in sorted(edges):
        succ.setdefault(a, []).append(b)
        has_incoming.add(b)
    starts = sorted({a for a, _ in edges} - has_incoming)

    chains: list[list[str]] = []
    stack: list[list[str]] = [[s] for s in reversed(starts)]
    while stack:
        path = stack.pop()
        nexts = succ.get(path[-1])
        if not nexts:
            if len(path) >= 2:
                chains.append(path)
                if len(chains) >= _MAX_CHAINS_PER_VIEW:
                    logger.warning(
                        "hierarchy_chains_truncated",
                        cap=_MAX_CHAINS_PER_VIEW,
                        pending_paths=len(stack),
                    )
                    break
            continue
        for nxt in reversed(nexts):
            stack.append([*path, nxt])
    return chains


def discover_dimension_hierarchies(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str,
    judge: DimensionIdentityJudge,
) -> int:
    """Run the stack-v4 identity pass over each enriched view; persist run-versioned.

    Form-(a) writer (DAT-502): one row per ``(signature, run_id)``, UPSERTed. The
    STATISTICS are deterministic (fixed permutation/sample seeds), so a redelivered
    run converges; the ONE LLM touchpoint is the within-view identity judge on
    relabeling bijections (DAT-762 §5b) — a code↔name alias and a coincidental 1:1
    are statistically identical, so meaning decides whether the axes collapse. A
    failed judge call surfaces those pairs as needs_confirmation, never merges them.
    Returns the rows persisted.
    """
    enriched = (
        session.execute(
            select(EnrichedView).where(
                EnrichedView.fact_table_id.in_(table_ids),
                EnrichedView.is_grain_verified.is_(True),
            )
        )
        .scalars()
        .all()
    )
    table_by_id = {
        t.table_id: t
        for t in session.execute(select(Table).where(Table.table_id.in_(table_ids))).scalars()
    }

    rows: list[dict[str, object]] = []
    col_ids_by_table: dict[str, dict[str, str]] = {}
    for ev in enriched:
        view_cols = _view_columns(duckdb_conn, ev.view_name)
        if view_cols is None:
            continue
        by_name = _resolve_candidates(session, ev, view_cols)
        col_ids_by_table.setdefault(ev.fact_table_id, {}).update(
            {c.column_name: c.column_id for c in by_name.values()}
        )
        cand_names = sorted(by_name)
        if len(cand_names) < 2:
            continue
        logger.info(
            "hierarchy_candidates",
            table=table_by_id[ev.fact_table_id].table_name
            if ev.fact_table_id in table_by_id
            else ev.fact_table_id,
            view=ev.view_name,
            n_candidates=len(cand_names),
        )

        scan = _scan_view(duckdb_conn, ev.view_name, cand_names)
        if scan is None:
            continue
        frame = _pull_sample(duckdb_conn, ev.view_name, cand_names, scan.n)
        if frame is None:
            continue
        # Authored column meanings (DAT-769) for the identity judge — corroborating
        # evidence over the fact's own + folded dim columns. Keyed by the view's
        # column name via each candidate's resolved catalog id.
        concepts = load_column_concepts(
            session, [ev.fact_table_id, *(ev.dimension_table_ids or [])], run_id
        )
        meanings: dict[str, str] = {}
        for cand in by_name.values():
            concept = concepts.get(cand.column_id) if cand.column_id else None
            if concept is not None and concept.meaning:
                meanings[cand.column_name] = concept.meaning
        rows.extend(
            _view_structures(
                fact_table_id=ev.fact_table_id,
                table_name=table_by_id[ev.fact_table_id].table_name
                if ev.fact_table_id in table_by_id
                else ev.fact_table_id,
                view_name=ev.view_name,
                run_id=run_id,
                by_name=by_name,
                scan=scan,
                frame=frame,
                meanings=meanings,
                judge=judge,
            )
        )

    # Fold the user's durable hierarchy/alias teaches into this run (DAT-537),
    # mirroring relationship-overlay materialization minus keeper-lift-up + witness
    # (the stack is deterministic). reject suppresses a discovered structure;
    # add/alias assert one.
    rows = _apply_teaches(session, rows, col_ids_by_table=col_ids_by_table, run_id=run_id)

    # Delete-then-insert in ONE transaction (the derive_bus_matrix retry pattern).
    # The §5b identity judge makes an alias GROUP's signature depend on a
    # nondeterministic verdict, so a bare upsert keyed on the signature SET could
    # strand a prior delivery's group row when a verdict flip changes the group's
    # size — and drivers would collapse that stale needs_confirmation=False row,
    # dropping a real axis (the corruption this lane exists to prevent). Replacing
    # the run's rows wholesale makes the persisted set exactly the last delivery's;
    # the upsert + unique constraint stay as the in-batch backstop.
    session.execute(
        delete(DimensionHierarchy).where(
            DimensionHierarchy.run_id == run_id,
            DimensionHierarchy.table_id.in_(table_ids),
        )
    )
    upsert(session, DimensionHierarchy, rows, index_elements=["signature", "run_id"])
    return len(rows)


def _validated_members(members: list[HierarchyMember]) -> list[dict[str, object]]:
    """Validate the level set and dump the members to their JSON form (DAT-779).

    The write-side assertion of the direction contract: ``level`` is the sole
    carrier of order (see :class:`HierarchyMember`), so it must be a permutation of
    ``range(len)`` for a consumer to read a total order. Raises on a gap or a
    duplicate — a writer that mis-numbers the levels fails loud, never silently.
    """
    levels = sorted(m.level for m in members)
    if levels != list(range(len(members))):
        raise ValueError(
            f"hierarchy member levels must be a contiguous 0..{len(members) - 1}; got {levels}"
        )
    return [m.model_dump() for m in members]


def _hierarchy_row(
    *,
    run_id: str,
    table_id: str,
    kind: str,
    members: list[dict[str, object]],
    canonical_label: str,
    signature: str,
    detection_source: str,
    needs_confirmation: bool,
    g3: float | None = None,
    role: stats.RoleResult | None = None,
    disagree_rate: float | None = None,
    identity_confidence: float | None = None,
) -> dict[str, object]:
    """One dimension_hierarchies row dict with a HOMOGENEOUS key set (DAT-784).

    Every writer produces the same keys so the multi-values upsert renders one
    consistent INSERT. The nullable role/g3 columns default to None; when ``role``
    is given (a role-check-derived row) ``role_verdict`` + the validated
    ``role_evidence`` are filled and ``disagree_rate`` is required.
    """
    role_evidence: dict[str, object] | None = None
    role_verdict: str | None = None
    if role is not None:
        if disagree_rate is None:
            raise ValueError("disagree_rate is required when a role verdict is persisted")
        role_verdict = role.verdict.value
        role_evidence = RoleEvidence(
            t1_p=role.t1_p,
            t1_context=role.t1_context,
            t2_p=role.t2_p,
            k_disagree=role.k_disagree,
            alpha=role.alpha,
            disagree_rate=disagree_rate,
        ).model_dump()
    return {
        "run_id": run_id,
        "table_id": table_id,
        "kind": kind,
        "members": members,
        "canonical_label": canonical_label,
        "signature": signature,
        "g3": g3,
        "role_verdict": role_verdict,
        "role_evidence": role_evidence,
        "identity_confidence": identity_confidence,
        "detection_source": detection_source,
        "needs_confirmation": needs_confirmation,
    }


def _apply_teaches(
    session: Session,
    rows: list[dict[str, object]],
    *,
    col_ids_by_table: dict[str, dict[str, str]],
    run_id: str,
) -> list[dict[str, object]]:
    """Apply reject / add / alias hierarchy overlays to the discovered row set.

    reject drops the structure with a matching ``(table_id, member-set)``
    (kind-agnostic — a member-set is one structure, drilldown, alias or role);
    add asserts a ``manual`` drilldown, alias a ``manual`` alias. A manual assert
    overrides a same-signature discovered row (clears ``needs_confirmation``).
    Member column ids resolve through the candidate maps built during discovery;
    a member outside the candidate universe resolves to ``""`` rather than
    failing the teach.
    """
    by_sig: dict[str, dict[str, object]] = {str(r["signature"]): r for r in rows}

    def _row_member_names(r: dict[str, object]) -> frozenset[str]:
        members = cast("list[dict[str, object]]", r["members"])
        return frozenset(str(m["column_name"]) for m in members)

    # One read of the active hierarchy teaches, grouped by action (the parser
    # re-queries per action, so load each once).
    specs = {a: hierarchy_overlay_specs(session, a) for a in ("reject", "add", "alias")}

    # reject: drop any discovered structure whose table + member-set matches.
    rejected: set[tuple[str, frozenset[str]]] = {
        (spec.table_id, frozenset(spec.members)) for spec in specs["reject"]
    }
    if rejected:
        by_sig = {
            sig: r
            for sig, r in by_sig.items()
            if (str(r["table_id"]), _row_member_names(r)) not in rejected
        }

    # add → manual drilldown, alias → manual alias.
    for action, kind in (("add", "drilldown"), ("alias", "alias")):
        for spec in specs[action]:
            members = spec.members
            if kind == "drilldown" and len(members) < 2:
                logger.info("hierarchy_teach_skipped", reason="drilldown_needs_2_levels", spec=spec)
                continue
            names = col_ids_by_table.get(spec.table_id, {})
            sig = f"{kind}:{spec.table_id}:" + "|".join(sorted(members))
            # The teach INPUT is finest → coarsest for a drilldown (the user-facing
            # convention); STORAGE is coarse → fine with an explicit ``level`` so
            # array position never has to be trusted (DAT-779).
            ordered = list(reversed(members)) if kind == "drilldown" else list(members)
            by_sig[sig] = _hierarchy_row(
                run_id=run_id,
                table_id=spec.table_id,
                kind=kind,
                members=_validated_members(
                    [
                        HierarchyMember(
                            column_name=n,
                            column_id=names.get(n, ""),
                            distinct_count=None,
                            level=i,
                        )
                        for i, n in enumerate(ordered)
                    ]
                ),
                canonical_label=" → ".join(ordered) if kind == "drilldown" else ordered[0],
                signature=sig,
                # A manual assert asserts an exact FD / bijection — g3 = 0 (strongest).
                g3=0.0,
                detection_source="manual",
                needs_confirmation=False,
            )
            logger.info(
                "hierarchy_teach_applied", action=action, table_id=spec.table_id, members=members
            )

    return list(by_sig.values())


def _col_samples(frame: pl.DataFrame, col: str) -> list[str]:
    """Up to ``_IDENTITY_SAMPLE_VALUES`` most-common non-null values, as strings.

    Count ties are broken by value (ascending) so the evidence block is
    reproducible run-to-run — the judge input must not vary on undefined tie order
    (``value_counts(sort=True)`` alone leaves ties unordered).
    """
    s = frame.get_column(col).drop_nulls()
    if not len(s):
        return []
    vc = s.value_counts()
    vc = vc.sort(by=[vc.columns[1], vc.columns[0]], descending=[True, False])
    return [str(v) for v in vc[vc.columns[0]][:_IDENTITY_SAMPLE_VALUES].to_list()]


def _judge_alias_identity(
    judge: DimensionIdentityJudge,
    *,
    table_name: str,
    pairs: list[tuple[str, str]],
    scan: _ViewScan,
    frame: pl.DataFrame,
    meanings: dict[str, str],
) -> dict[tuple[str, str], AliasIdentityVerdict]:
    """Ask the identity judge which relabeling bijections are one dimension.

    A FAILED call is not a judgment (the research posture): it returns ``{}`` and
    every pair falls to the needs_confirmation path — an unjudged bijection is
    surfaced, never silently auto-merged (that collapse would corrupt two axes).
    """
    candidates: list[dict[str, object]] = []
    for i, (a, b) in enumerate(pairs):
        pair_meanings = {c: meanings[c] for c in (a, b) if c in meanings}
        candidates.append(
            {
                "ref": str(i),
                "table": table_name,
                "a": {"name": a, "distinct": scan.d2[a], "samples": _col_samples(frame, a)},
                "b": {"name": b, "distinct": scan.d2[b], "samples": _col_samples(frame, b)},
                "meanings": pair_meanings,
            }
        )
    result = judge.alias_identity(candidates=candidates)
    if not result.success:
        logger.warning(
            "hierarchy_alias_judge_failed", table=table_name, n=len(pairs), error=result.error
        )
        return {}
    by_ref = {v.pair_ref: v for v in result.unwrap()}
    return {pair: by_ref[str(i)] for i, pair in enumerate(pairs) if str(i) in by_ref}


def _view_structures(
    *,
    fact_table_id: str,
    table_name: str,
    view_name: str,
    run_id: str,
    by_name: dict[str, _Candidate],
    scan: _ViewScan,
    frame: pl.DataFrame,
    meanings: dict[str, str],
    judge: DimensionIdentityJudge | None,
) -> list[dict[str, object]]:
    """The drill-down + alias + role row dicts for one enriched view (stack v4).

    A module-level helper (not a closure in the per-view loop) so its inner
    functions bind these parameters, not loop variables.
    """
    cand_names = sorted(by_name)

    # -- 1. null policy + eligibility (born-loud on every drop) --------------
    eligible: list[str] = []
    for c in cand_names:
        if scan.d2[c] < MIN_DISTINCT_DIMENSION:
            logger.info(
                "hierarchy_column_excluded", column=c, reason="constant", distinct=scan.d2[c]
            )
            continue
        if scan.d_sql[c] <= 1 < scan.d2[c]:
            # Null-coded column ({value, NULL}): SQL sees a constant, the null
            # lane keeps it — NULL is a category for every row statistic below.
            logger.info("hierarchy_null_coded_column", column=c, distinct_sql=scan.d_sql[c])
        eligible.append(c)
    if len(eligible) < 2:
        return []

    codes = {c: stats.codes_of(frame.get_column(c)) for c in eligible}
    n_sample = frame.height

    # Null policy, edge arm (DAT-757 lane): a NULL is DATA when the column is
    # null-coded (its value domain is degenerate — nullness carries the signal)
    # and MISSINGNESS otherwise (join-miss / ragged rows) — edge statistics then
    # use pairwise deletion, which rescues true edges under partial nulls
    # without letting a sparse dependent assert from nothing (support floor).
    null_coded = {c for c in eligible if scan.d_sql[c] <= 1 < scan.d2[c]}
    null_mask = {
        c: frame.get_column(c).is_null().to_numpy() for c in eligible if c not in null_coded
    }

    pair_arrays_cache: dict[tuple[str, str], tuple[np.ndarray, np.ndarray, int, int]] = {}

    def edge_arrays(s: str, t: str) -> tuple[np.ndarray, np.ndarray, int, int]:
        """(codes_s, codes_t, d_s, d_t) over the pairwise-complete rows (edge arm).

        The distinct counts are taken on the SAME rows the statistics run on —
        full-view null-aware counts would let a null-degraded copy of a level
        read as strictly finer (its NULL category inflates the count by one) and
        interpose as a fake level between its base and the base's determinant.
        """
        key = (s, t)
        if key not in pair_arrays_cache:
            masks = [null_mask[c] for c in (s, t) if c in null_mask]
            if masks and (drop := np.logical_or.reduce(masks)).any():
                keep = ~drop
                cs, ct = codes[s][keep], codes[t][keep]
                d_s = len(np.unique(cs)) if len(cs) else 0
                d_t = len(np.unique(ct)) if len(ct) else 0
                pair_arrays_cache[key] = (cs, ct, d_s, d_t)
            else:
                pair_arrays_cache[key] = (codes[s], codes[t], scan.d2[s], scan.d2[t])
        return pair_arrays_cache[key]

    g3_cache: dict[tuple[str, str], float] = {}

    def edge_g3(a: str, b: str) -> float:
        if (a, b) not in g3_cache:
            ca, cb, _, _ = edge_arrays(a, b)
            g3_cache[(a, b)] = stats.row_g3(ca, cb) if len(ca) else 1.0
        return g3_cache[(a, b)]

    # Full-scan determinant guards (a column may still be a dependent/coarsest
    # level), born-loud once per column. Full-view distincts, never the sample —
    # a sampled fold key must not read as near-key (the rel-hm lesson).
    bad_det: set[str] = set()
    for c in eligible:
        d = scan.d2[c]
        if d < MIN_DISTINCT_DETERMINANT:
            logger.info("hierarchy_determinant_excluded", column=c, reason="too_coarse", distinct=d)
            bad_det.add(c)
        elif scan.n >= MIN_ROWS_NEARKEY and d >= NEAR_KEY_FRAC * scan.n:
            logger.info(
                "hierarchy_determinant_excluded",
                column=c,
                reason="near_key",
                distinct=d,
                rows=scan.n,
            )
            bad_det.add(c)

    # -- 2. effect screens ----------------------------------------------------
    cand_alias: list[tuple[str, str]] = []
    cand_edge: list[tuple[str, str]] = []
    # Edges whose pairwise-complete support fell below the floor are surfaced
    # (needs_confirmation on any chain that uses them), never silently dropped —
    # the same posture the tiny-view MIN_SUPPORT_ROWS flag takes.
    low_support: set[tuple[str, str]] = set()
    for i, a in enumerate(eligible):
        for b in eligible[i + 1 :]:
            fwd, bwd = scan.pair_g3(a, b)
            if fwd <= FD_MAX_G3 and bwd <= FD_MAX_G3:
                cand_alias.append((a, b))
            for s, t in ((a, b), (b, a)):
                if s in bad_det:
                    continue
                cs, ct, d_s, d_t = edge_arrays(s, t)
                if d_s <= d_t:  # finest → coarsest, on the rows the stats see
                    continue
                if len(cs) < MIN_SUPPORT_ROWS and len(cs) < n_sample:
                    logger.info(
                        "hierarchy_edge_low_support",
                        determinant=s,
                        dependent=t,
                        complete_rows=len(cs),
                    )
                    low_support.add((s, t))
                if edge_g3(s, t) > FD_MAX_G3:
                    continue
                # -- 3. λ floor: the vacuous-skew kill (edge arm only) --------
                lam = stats.gk_lambda(cs, ct)
                if lam < LAMBDA_MIN:
                    logger.info(
                        "hierarchy_edge_excluded",
                        determinant=s,
                        dependent=t,
                        reason="vacuous_skew",
                        gk_lambda=round(lam, 3),
                    )
                    continue
                cand_edge.append((s, t))

    # -- 4. permutation p + BH over the view's screened family ----------------
    # Edge tests run on the pairwise-complete rows their screen used; alias
    # tests on the full null-as-category codes (pair-count semantics). The
    # candidates fan across a bounded thread pool: each task derives its own
    # generator (_pair_rng) and only READS the pre-warmed caches (edge_arrays
    # was populated for every cand_edge pair during screening), so the results
    # are byte-identical at any worker count.
    tasks: list[tuple[str, str, str]] = [("e", a, b) for a, b in cand_edge]
    for a, b in cand_alias:
        tasks += [("a", a, b), ("a", b, a)]

    def perm_task(task: tuple[str, str, str]) -> tuple[tuple[str, str, str], float]:
        arm, a, b = task
        if arm == "e":
            ca, cb, _, _ = edge_arrays(a, b)
        else:
            ca, cb = codes[a], codes[b]
        if len(ca) > _MAX_PERM_ROWS:
            # Frame rows are in hash order — a prefix is a deterministic sample,
            # and the permutation test is sample-honest at any subsample.
            ca, cb = ca[:_MAX_PERM_ROWS], cb[:_MAX_PERM_ROWS]
        return task, stats.perm_pvalue(ca, cb, _pair_rng(arm, a, b))

    p_by_task: dict[tuple[str, str, str], float] = {}
    if tasks:
        with ThreadPoolExecutor(max_workers=_PERM_WORKERS) as pool:
            for task, p in pool.map(perm_task, tasks):
                p_by_task[task] = p

    pvals: dict[tuple[str, str, str], float] = {t: p_by_task[t] for t in p_by_task if t[0] == "e"}
    for a, b in cand_alias:
        # A 1:1 claim needs significant dependence in BOTH directions.
        pvals[("a", a, b)] = max(p_by_task[("a", a, b)], p_by_task[("a", b, a)])
    accepted = stats.bh_reject(pvals, m_family=max(1, len(pvals)), q=Q_FDR)
    acc_edges = {(a, b) for k, a, b in accepted if k == "e"}
    acc_alias = {(a, b) for k, a, b in accepted if k == "a"}

    # -- 5. disagreement-set role check on near-copies (alias arm) ------------
    needs_conf = scan.n < MIN_SUPPORT_ROWS
    out: list[dict[str, object]] = []
    merged: list[tuple[str, str]] = []
    # A near-copy pair that is NOT merged (role / undecidable) is the same domain
    # seen twice — never a level relationship. Its direct edge (a near-identity FD
    # that trivially passes the edge screen) is suppressed at assembly.
    same_domain: set[frozenset[str]] = set()

    def _member(col: str, level: int) -> HierarchyMember:
        c = by_name[col]
        return HierarchyMember(
            column_name=c.column_name,
            column_id=c.column_id,
            # Null-aware d2 (NULL = one category) — deliberately differs from the
            # pre-DAT-761 null-blind SQL count; the null lane made d2 the honest
            # "values this axis distinguishes".
            distinct_count=scan.d2[col],
            level=level,
        )

    # Disagreement vectors are built lazily per BH-accepted pair — an up-front
    # object-array for every eligible column is gigabytes at the cells budget,
    # for a handful of consumers.
    to_check: list[tuple[str, str, np.ndarray, float]] = []
    to_judge: list[tuple[str, str]] = []
    for a, b in sorted(acc_alias):
        dis = (
            (frame.get_column(a).fill_null("␀") != frame.get_column(b).fill_null("␀"))
            .to_numpy()
            .astype(np.int64)
        )
        rate = float(dis.mean())
        if rate == 0.0:
            # Exact copy: identical values, unambiguously one column seen twice.
            merged.append((a, b))
            continue
        if rate > ROLE_MAX_DISAGREE:
            # Relabeling bijection: values differ but 1:1. A true alias (code ↔
            # name) and a COINCIDENTAL bijection (an entity key that lines up 1:1
            # with a per-row timestamp) are statistically identical here — only
            # meaning separates them. The identity judge decides below (§5b);
            # merging on the statistic alone was the raceId↔date false identity.
            to_judge.append((a, b))
            continue
        to_check.append((a, b, dis, rate))

    def role_task(item: tuple[str, str, np.ndarray, float]) -> stats.RoleResult:
        a, b, dis, _rate = item
        contexts = {c: codes[c] for c in eligible if c not in (a, b)}
        return stats.role_verdict(dis, contexts, codes[b], _pair_rng("role", a, b))

    verdicts: list[stats.RoleResult] = []
    if to_check:
        # Same pool discipline as the perm stage: pure numpy, read-only shared
        # codes, per-pair generators — deterministic at any worker count.
        with ThreadPoolExecutor(max_workers=_PERM_WORKERS) as pool:
            verdicts = list(pool.map(role_task, to_check))

    for (a, b, _dis, rate), result in zip(to_check, verdicts, strict=True):
        logger.info(
            "hierarchy_role_check",
            a=a,
            b=b,
            verdict=result.verdict.value,
            disagree_rate=round(rate, 5),
            k=result.k_disagree,
            t1_p=result.t1_p,
            t1_context=result.t1_context,
            t2_p=result.t2_p,
        )
        if result.verdict is RoleVerdict.DIRT:
            merged.append((a, b))
            continue
        same_domain.add(frozenset((a, b)))
        # A near-copy pair is a peer set, not a hierarchy: ``level`` is the sorted
        # ordinal only (no coarse/fine axis). ``g3`` is None — the role check
        # measures disagreement, not a functional dependency; the rate + p-values
        # live in ``role_evidence`` (DAT-784), never overloaded into ``g3``.
        group = sorted((a, b))
        pair_members = _validated_members([_member(c, level=i) for i, c in enumerate(group)])
        if result.verdict is RoleVerdict.ROLE:
            out.append(
                _hierarchy_row(
                    run_id=run_id,
                    table_id=fact_table_id,
                    kind="role",
                    members=pair_members,
                    canonical_label=f"{group[0]} ⇄ {group[1]}",
                    signature=f"role:{fact_table_id}:" + "|".join(group),
                    role=result,
                    disagree_rate=rate,
                    detection_source="g3",
                    needs_confirmation=needs_conf,
                )
            )
            continue
        # VALUE_SYSTEMATIC / ABSTAIN: undecidable from data — surface as an alias to
        # confirm, but persist the verdict + evidence so the two stay DISTINGUISHABLE
        # (the DAT-784 bug: both used to collapse to a bare needs_confirmation alias).
        out.append(
            _hierarchy_row(
                run_id=run_id,
                table_id=fact_table_id,
                kind="alias",
                members=pair_members,
                canonical_label=group[0],
                signature=f"alias:{fact_table_id}:" + "|".join(group),
                role=result,
                disagree_rate=rate,
                detection_source="g3",
                needs_confirmation=True,
            )
        )

    # -- 5b. within-view identity judge on relabeling bijections -------------
    # The rate > ROLE_MAX_DISAGREE pairs are code↔name relabelings OR coincidental
    # 1:1s — statistically identical, only meaning tells them apart (DAT-762). A
    # confident same-dimension call merges (its axes collapse in the driver tree);
    # everything else — a coincidental pair, a grey call, or a judge that could
    # not answer — is surfaced as a needs_confirmation alias that is NOT collapsed
    # (drivers skip needs_confirmation aliases). ``merge_conf`` carries each merged
    # pair's confidence to its assembled group below.
    merge_conf: dict[frozenset[str], float] = {}
    id_verdicts = (
        _judge_alias_identity(
            judge, table_name=table_name, pairs=to_judge, scan=scan, frame=frame, meanings=meanings
        )
        if to_judge and judge is not None
        else {}
    )
    for a, b in to_judge:
        v = id_verdicts.get((a, b))
        conf = v.confidence if v is not None else None
        if v is not None and v.confidence >= IDENTITY_MERGE_MIN:
            merged.append((a, b))
            merge_conf[frozenset((a, b))] = v.confidence
            logger.info(
                "hierarchy_alias_judged",
                view=view_name,
                a=a,
                b=b,
                merged=True,
                confidence=round(v.confidence, 3),
            )
            continue
        # A non-merged bijection is a suspicious 1:1 pair, not a level relationship:
        # suppress its direct edge at assembly, exactly as the role-check non-merged
        # path does (an asymmetric-null pair could otherwise read finest→coarsest on
        # the null-dropped rows and assert a spurious drilldown the judge declined).
        same_domain.add(frozenset((a, b)))
        group = sorted((a, b))
        out.append(
            _hierarchy_row(
                run_id=run_id,
                table_id=fact_table_id,
                kind="alias",
                members=_validated_members([_member(c, level=i) for i, c in enumerate(group)]),
                canonical_label=group[0],
                signature=f"alias:{fact_table_id}:" + "|".join(group),
                detection_source="g3",
                needs_confirmation=True,
                identity_confidence=conf,
            )
        )
        logger.info(
            "hierarchy_alias_judged",
            view=view_name,
            a=a,
            b=b,
            merged=False,
            confidence=None if conf is None else round(conf, 3),
        )

    # -- 6. assemble: union-find → edges on reps → reduction → chains ---------
    parent = {c: c for c in eligible}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for a, b in merged:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[min(ra, rb)] = max(ra, rb)
    members: dict[str, list[str]] = {}
    for c in eligible:
        members.setdefault(find(c), []).append(c)
    groups = [sorted(g) for g in members.values() if len(g) >= 2]
    rep = {c: sorted(g)[0] for g in members.values() for c in g}

    edges: set[tuple[str, str]] = set()
    thin_edges: set[tuple[str, str]] = set()  # rep-level image of low_support
    for a, b in acc_edges:
        if frozenset((a, b)) in same_domain:
            continue
        ra, rb = rep[a], rep[b]
        if ra != rb:
            edges.add((ra, rb))
            if (a, b) in low_support:
                thin_edges.add((ra, rb))
    reduced = _transitive_reduction(_break_cycles(edges))

    for chain in _maximal_chains(reduced):
        hops = [(chain[k], chain[k + 1]) for k in range(len(chain) - 1)]
        score = max(edge_g3(x, y) for x, y in hops)
        # ``_maximal_chains`` yields finest → coarsest; store coarse → fine with
        # ``level`` = array index (see :class:`HierarchyMember`) so array, level and
        # label agree (DAT-779). ``signature`` sorts, so the flip never re-dedups.
        chain_ctf = list(reversed(chain))
        out.append(
            _hierarchy_row(
                run_id=run_id,
                table_id=fact_table_id,
                kind="drilldown",
                members=_validated_members([_member(c, level=i) for i, c in enumerate(chain_ctf)]),
                canonical_label=" → ".join(chain_ctf),
                signature=f"drilldown:{fact_table_id}:" + "|".join(sorted(chain)),
                g3=score,
                detection_source="g3",
                # Surface, don't decide: a chain resting on a sub-floor
                # pairwise-complete edge is flagged, same as a tiny view.
                needs_confirmation=needs_conf or any(h in thin_edges for h in hops),
            )
        )
        logger.info("hierarchy_drilldown", view=view_name, chain=chain_ctf, score=round(score, 4))

    for group in groups:
        pair_scores = [
            max(scan.pair_g3(group[i], group[j]))
            for i in range(len(group))
            for j in range(i + 1, len(group))
            if (group[i], group[j]) in scan.joints or (group[j], group[i]) in scan.joints
        ]
        # The group's identity confidence is its WEAKEST judged relabeling pair
        # (min over merge_conf of pairs inside the group). NULL when the group has
        # no judged pair — every merge was an exact copy (rate 0), unambiguous.
        gset = set(group)
        judged_conf = [c for fs, c in merge_conf.items() if fs <= gset]
        out.append(
            _hierarchy_row(
                run_id=run_id,
                table_id=fact_table_id,
                kind="alias",
                # A redundant-axis group is a peer set: ``level`` is the sorted
                # ordinal (canonical = group[0] = level 0), no coarse/fine meaning.
                members=_validated_members([_member(c, level=i) for i, c in enumerate(group)]),
                canonical_label=group[0],
                signature=f"alias:{fact_table_id}:" + "|".join(sorted(group)),
                g3=max(pair_scores) if pair_scores else 0.0,
                detection_source="g3",
                needs_confirmation=needs_conf,
                identity_confidence=min(judged_conf) if judged_conf else None,
            )
        )
        logger.info("hierarchy_alias", view=view_name, group=group)

    logger.info(
        "hierarchy_view_decided",
        view=view_name,
        n_rows=scan.n,
        n_sample=n_sample,
        eligible=len(eligible),
        screened_edges=len(cand_edge),
        screened_aliases=len(cand_alias),
        asserted_edges=len(acc_edges),
        merged_aliases=len(merged),
    )
    return out
