"""Dimension-identity discovery over the enriched views (DAT-761, stack v4 from DAT-757).

Deterministic, no LLM. For each fact's grain-verified enriched view, EVERY
dimension-like view column is a candidate (measures excluded by their
``semantic_role`` — the additivity lane: ``revenue → tier`` is reliably asserted
by every statistic, so measures never enter FD discovery). The upstream pipeline
``max_columns`` limit is the only width cap; all other exclusions are
data-grounded guards, each logged (born-loud).

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

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

import numpy as np
import polars as pl
from sqlalchemy import select

from dataraum.analysis.hierarchies import stats
from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.hierarchies.overlay import hierarchy_overlay_specs
from dataraum.analysis.hierarchies.stats import RoleVerdict
from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

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
Q_FDR = 0.05

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

# Structures resting on fewer rows than this are surfaced for confirmation
# (``needs_confirmation``) rather than auto-asserted.
MIN_SUPPORT_ROWS = 100

# Row-statistics working set: rows × candidate columns pulled into memory
# (the DAT-580 drivers precedent). Guards never depend on the sample.
MAX_SAMPLE_CELLS = 40_000_000
MIN_SAMPLE_ROWS = 50_000

# Fixed seeds: the permutation null and the row sample must be identical across
# Temporal success-redeliveries so the run-versioned upsert converges.
_PERM_SEED = 20260714
_SAMPLE_SEED = 757

# COUNT(DISTINCT (a, b)) aggregates per scan statement — wide views chunk the
# pair family across several single-scan queries instead of one giant SELECT.
_MAX_PAIR_AGGS = 500


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
    joints: dict[tuple[str, str], int]  # row-literal pair distincts, (a < b) key order

    def pair_g3(self, a: str, b: str) -> tuple[float, float]:
        """Pair-count g3 for (a → b, b → a), null-aware numerators."""
        d_ab = self.joints[(a, b) if (a, b) in self.joints else (b, a)]
        if d_ab == 0:
            return 1.0, 1.0
        return 1.0 - self.d2[a] / d_ab, 1.0 - self.d2[b] / d_ab


def _view_columns(duckdb_conn: duckdb.DuckDBPyConnection, view_name: str) -> list[str] | None:
    """The view's column names, or ``None`` if it is not queryable (logged)."""
    try:
        rows = duckdb_conn.execute(f'DESCRIBE "{view_name}"').fetchall()
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
    parts += [f'COUNT(DISTINCT "{c}") AS d{i}' for i, c in enumerate(cols)]
    parts += [f'COUNT("{c}") AS c{i}' for i, c in enumerate(cols)]
    try:
        row = duckdb_conn.execute(
            f'SELECT {", ".join(parts)} FROM "{view_name}"'  # noqa: S608 — catalog names
        ).fetchone()
        if row is None:
            return None
        n = int(row[0])
        d_sql = {c: int(row[1 + i]) for i, c in enumerate(cols)}
        non_null = {c: int(row[1 + len(cols) + i]) for i, c in enumerate(cols)}
        d2 = {c: d_sql[c] + (1 if non_null[c] < n else 0) for c in cols}

        pairs = [(cols[i], cols[j]) for i in range(len(cols)) for j in range(i + 1, len(cols))]
        joints: dict[tuple[str, str], int] = {}
        for start in range(0, len(pairs), _MAX_PAIR_AGGS):
            chunk = pairs[start : start + _MAX_PAIR_AGGS]
            sql = ", ".join(
                f'COUNT(DISTINCT ("{a}", "{b}")) AS j{k}' for k, (a, b) in enumerate(chunk)
            )
            jrow = duckdb_conn.execute(f'SELECT {sql} FROM "{view_name}"')  # noqa: S608
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

    Full view when it fits ``MAX_SAMPLE_CELLS``; else a seeded reservoir sample
    (row-level statistics are sample-honest — g3 exactness is subset-invariant —
    while every guard reads the full-view scan, so a sampled fold key can never
    trip the near-key guard: the rel-hm lesson).
    """
    n_sample = max(MIN_SAMPLE_ROWS, MAX_SAMPLE_CELLS // max(len(cols), 1))
    sample = (
        f" USING SAMPLE reservoir({n_sample} ROWS) REPEATABLE ({_SAMPLE_SEED})"
        if n_rows > n_sample
        else ""
    )
    select_cols = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in cols)
    try:
        table = duckdb_conn.execute(
            f'SELECT {select_cols} FROM "{view_name}"{sample}'  # noqa: S608 — catalog names
        ).arrow()
    except Exception as e:  # noqa: BLE001 — any DuckDB error → skip this view, logged
        logger.warning("hierarchy_sample_pull_failed", view=view_name, error=str(e))
        return None
    return cast("pl.DataFrame", pl.from_arrow(table))


def _transitive_reduction(edges: set[tuple[str, str]]) -> set[tuple[str, str]]:
    """Remove edges implied by a longer path (DAG; ``a → b`` = a determines b).

    Drops ``a → c`` whenever ``a → … → c`` exists through an intermediate, so a
    chain ``zip → city → state`` keeps only the adjacent links. Determination is a
    partial order (acyclic once aliases are collapsed), so a simple reachability
    test per edge suffices.
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


def _maximal_chains(edges: set[tuple[str, str]]) -> list[list[str]]:
    """Every maximal path (length ≥ 2 nodes) through the reduced DAG, finest→coarsest.

    A node with no incoming reduced edge is a chain start (the finest level); a
    node with no outgoing edge is the end (coarsest). Branching yields multiple
    chains. Deterministic: starts and successors are sorted.
    """
    succ: dict[str, list[str]] = {}
    has_incoming: set[str] = set()
    for a, b in sorted(edges):
        succ.setdefault(a, []).append(b)
        has_incoming.add(b)
    starts = sorted({a for a, _ in edges} - has_incoming)

    chains: list[list[str]] = []

    def walk(path: list[str]) -> None:
        tail = path[-1]
        nexts = succ.get(tail)
        if not nexts:
            if len(path) >= 2:
                chains.append(path)
            return
        for nxt in nexts:
            walk([*path, nxt])

    for start in starts:
        walk(path=[start])
    return chains


def discover_dimension_hierarchies(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str,
) -> int:
    """Run the stack-v4 identity pass over each enriched view; persist run-versioned.

    Form-(a) writer (DAT-502): one row per ``(signature, run_id)``, UPSERTed;
    deterministic (fixed permutation/sample seeds), so a redelivered run
    converges. Returns the rows persisted.
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
        rows.extend(
            _view_structures(
                fact_table_id=ev.fact_table_id,
                view_name=ev.view_name,
                run_id=run_id,
                by_name=by_name,
                scan=scan,
                frame=frame,
            )
        )

    # Fold the user's durable hierarchy/alias teaches into this run (DAT-537),
    # mirroring relationship-overlay materialization minus keeper-lift-up + witness
    # (the stack is deterministic). reject suppresses a discovered structure;
    # add/alias assert one.
    rows = _apply_teaches(session, rows, col_ids_by_table=col_ids_by_table, run_id=run_id)

    upsert(session, DimensionHierarchy, rows, index_elements=["signature", "run_id"])
    return len(rows)


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

    # add → manual drilldown, alias → manual alias (ordered members preserved).
    for action, kind in (("add", "drilldown"), ("alias", "alias")):
        for spec in specs[action]:
            members = spec.members
            if kind == "drilldown" and len(members) < 2:
                logger.info("hierarchy_teach_skipped", reason="drilldown_needs_2_levels", spec=spec)
                continue
            names = col_ids_by_table.get(spec.table_id, {})
            sig = f"{kind}:{spec.table_id}:" + "|".join(sorted(members))
            by_sig[sig] = {
                "run_id": run_id,
                "table_id": spec.table_id,
                "kind": kind,
                "members": [
                    {"column_name": n, "column_id": names.get(n, ""), "distinct_count": None}
                    for n in members
                ],
                "canonical_label": " → ".join(members) if kind == "drilldown" else members[0],
                "signature": sig,
                "score": 0.0,
                "detection_source": "manual",
                "needs_confirmation": False,
            }
            logger.info(
                "hierarchy_teach_applied", action=action, table_id=spec.table_id, members=members
            )

    return list(by_sig.values())


def _view_structures(
    *,
    fact_table_id: str,
    view_name: str,
    run_id: str,
    by_name: dict[str, _Candidate],
    scan: _ViewScan,
    frame: pl.DataFrame,
) -> list[dict[str, object]]:
    """The drill-down + alias + role row dicts for one enriched view (stack v4).

    A module-level helper (not a closure in the per-view loop) so its inner
    functions bind these parameters, not loop variables.
    """
    cand_names = sorted(by_name)
    rng = np.random.default_rng(_PERM_SEED)

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
    strs = {c: frame.get_column(c).fill_null("␀").to_numpy() for c in eligible}
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

    def row_g3(a: str, b: str) -> float:
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
        elif scan.n and d >= NEAR_KEY_FRAC * scan.n:
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
                        "hierarchy_edge_excluded",
                        determinant=s,
                        dependent=t,
                        reason="null_support",
                        complete_rows=len(cs),
                    )
                    continue
                if row_g3(s, t) > FD_MAX_G3:
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
    # tests on the full null-as-category codes (pair-count semantics).
    p_cache: dict[tuple[str, str, str], float] = {}

    def perm_p(arm: str, a: str, b: str) -> float:
        if (arm, a, b) not in p_cache:
            if arm == "e":
                ca, cb, _, _ = edge_arrays(a, b)
            else:
                ca, cb = codes[a], codes[b]
            p_cache[(arm, a, b)] = stats.perm_pvalue(ca, cb, rng)
        return p_cache[(arm, a, b)]

    pvals: dict[tuple[str, str, str], float] = {}
    for a, b in cand_edge:
        pvals[("e", a, b)] = perm_p("e", a, b)
    for a, b in cand_alias:
        # A 1:1 claim needs significant dependence in BOTH directions.
        pvals[("a", a, b)] = max(perm_p("a", a, b), perm_p("a", b, a))
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

    def _member(col: str) -> dict[str, object]:
        c = by_name[col]
        return {
            "column_name": c.column_name,
            "column_id": c.column_id,
            "distinct_count": scan.d2[col],
        }

    for a, b in sorted(acc_alias):
        dis = (strs[a] != strs[b]).astype(np.int64)
        rate = float(dis.mean())
        if rate == 0.0 or rate > ROLE_MAX_DISAGREE:
            # Exact copy, or a relabeling bijection (code ↔ name): a true alias.
            merged.append((a, b))
            continue
        contexts = {c: codes[c] for c in eligible if c not in (a, b)}
        result = stats.role_verdict(dis, contexts, codes[b], rng)
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
        if result.verdict is RoleVerdict.ROLE:
            group = sorted((a, b))
            out.append(
                {
                    "run_id": run_id,
                    "table_id": fact_table_id,
                    "kind": "role",
                    "members": [_member(c) for c in group],
                    "canonical_label": f"{group[0]} ⇄ {group[1]}",
                    "signature": f"role:{fact_table_id}:" + "|".join(group),
                    "score": rate,
                    "detection_source": "g3",
                    "needs_confirmation": needs_conf,
                }
            )
            continue
        # VALUE_SYSTEMATIC / ABSTAIN: undecidable from data — surface, don't merge.
        group = sorted((a, b))
        out.append(
            {
                "run_id": run_id,
                "table_id": fact_table_id,
                "kind": "alias",
                "members": [_member(c) for c in group],
                "canonical_label": group[0],
                "signature": f"alias:{fact_table_id}:" + "|".join(group),
                "score": rate,
                "detection_source": "g3",
                "needs_confirmation": True,
            }
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
    for a, b in acc_edges:
        if frozenset((a, b)) in same_domain:
            continue
        ra, rb = rep[a], rep[b]
        if ra != rb:
            edges.add((ra, rb))
    reduced = _transitive_reduction(edges)

    for chain in _maximal_chains(reduced):
        score = max(row_g3(chain[k], chain[k + 1]) for k in range(len(chain) - 1))
        out.append(
            {
                "run_id": run_id,
                "table_id": fact_table_id,
                "kind": "drilldown",
                "members": [_member(c) for c in chain],
                "canonical_label": " → ".join(chain),
                "signature": f"drilldown:{fact_table_id}:" + "|".join(sorted(chain)),
                "score": score,
                "detection_source": "g3",
                "needs_confirmation": needs_conf,
            }
        )
        logger.info("hierarchy_drilldown", view=view_name, chain=chain, score=round(score, 4))

    for group in groups:
        pair_scores = [
            max(scan.pair_g3(group[i], group[j]))
            for i in range(len(group))
            for j in range(i + 1, len(group))
            if (group[i], group[j]) in scan.joints or (group[j], group[i]) in scan.joints
        ]
        out.append(
            {
                "run_id": run_id,
                "table_id": fact_table_id,
                "kind": "alias",
                "members": [_member(c) for c in group],
                "canonical_label": group[0],
                "signature": f"alias:{fact_table_id}:" + "|".join(sorted(group)),
                "score": max(pair_scores) if pair_scores else 0.0,
                "detection_source": "g3",
                "needs_confirmation": needs_conf,
            }
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
