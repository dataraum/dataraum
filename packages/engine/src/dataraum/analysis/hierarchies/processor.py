"""g3 functional-dependency / hierarchy discovery over the enriched views (DAT-537).

Deterministic, no LLM. For each fact's grain-verified enriched view, the catalog's
grain-safe slice dimensions (DAT-536) are the candidate axes. One DuckDB scan per
view computes every column's distinct count and every unordered pair's joint
distinct count; from those the **approximate functional dependency** measure

    g3(A → B) = 1 − COUNT(DISTINCT A) / COUNT(DISTINCT (A, B))

is read for both directions of every pair (g3 = 0 ⇔ A determines B exactly). Edges
become drill-down hierarchies (``zip → city → state``) after transitive reduction;
bidirectional ``g3 ≈ 0`` pairs collapse into 1:1 alias groups (the redundant-axis
dedup the DAT-545 driver tree needs to de-confound its ranking).

Null semantics (documented bias): the per-column distinct counts ignore NULLs
(SQL ``COUNT(DISTINCT)``) while the joint distinct count over a row literal counts
``(a, NULL)`` as a present pair, so a column with NULLs inflates its joint count and
its g3 — biasing toward MISSED dependencies (false negatives), never spurious ones.
A missed edge is recoverable via teach; a spurious asserted hierarchy is not. Slice
dimensions are categorical and typically non-null, so for the common case g3 is exact.

Candidate set = ALL this-run grain-safe catalog dims (deterministic; no priority
cap — a cap would be arbitrary across runs and would drop axes DAT-545 must rank).
Pruning is only by data-grounded guards: a constant column (< ``MIN_DISTINCT_DIMENSION``)
is not an axis at all and is dropped from both roles; a too-coarse determinant
(≤ 2 distinct) determines anything vacuously and a near-key determinant
(≥ ``NEAR_KEY_FRAC`` of the rows distinct) manufactures a spurious FD, so both are
rejected as DETERMINANTS (still allowed as a coarsest level). The candidate count and
every guard exclusion are logged (born-loud): a pathologically wide view is a measured
signal, never a silent cut.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import select

from dataraum.analysis.hierarchies.db_models import DimensionHierarchy
from dataraum.analysis.hierarchies.overlay import hierarchy_overlay_specs
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.storage import Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# g3 at or below this is treated as an exact functional dependency (allows a
# small fraction of dirty-data violations). A true FD over clean data is g3 = 0.
FD_MAX_G3 = 0.01

# A constant column (1 distinct) is not a usable axis at all: useless as a level
# and trivially determined by everything (junk ``X → constant`` edges) — dropped
# from BOTH roles. A 2-value column is a legitimate low-cardinality level (e.g. a
# binary dimension, or the coarsest level when the data spans 2 states), so the
# floor for being a candidate is 2.
MIN_DISTINCT_DIMENSION = 2
# A DETERMINANT must distinguish enough values that the FD isn't trivial — a
# ≤2-distinct determinant "determines" anything coarser vacuously (ticket guard) —
# and at most ``NEAR_KEY_FRAC`` of the rows: a near-unique column is a key, every
# value maps to one of anything, a spurious FD.
MIN_DISTINCT_DETERMINANT = 3
NEAR_KEY_FRAC = 0.9

# Edges resting on fewer than this many rows are surfaced for confirmation
# (``needs_confirmation``) rather than auto-asserted — too little support to trust.
MIN_SUPPORT_ROWS = 100


@dataclass(frozen=True)
class _Pair:
    """The g3 evidence for one unordered column pair over the enriched view."""

    d_a: int  # distinct values of column a (NULLs ignored)
    d_b: int  # distinct values of column b
    d_ab: int  # distinct (a, b) pairs (row literal; NULL field counts as present)

    def g3(self, *, forward: bool) -> float:
        """The g3 of a → b (``forward``) or b → a. Empty pair → 1.0 (no FD)."""
        if self.d_ab == 0:
            return 1.0
        return 1.0 - (self.d_a if forward else self.d_b) / self.d_ab


def _g3_scan(
    duckdb_conn: duckdb.DuckDBPyConnection, view_name: str, cols: list[str]
) -> tuple[int, dict[str, int], dict[tuple[int, int], int]] | None:
    """One scan: row count, per-column distinct counts, per-pair joint distincts.

    Returns ``(n_rows, {col: d_col}, {(i, j): d_ij})`` for ``i < j``, or ``None``
    if the scan fails (logged) — the view is then skipped, a visible abstention.
    """
    parts = ["COUNT(*) AS n"]
    parts += [f'COUNT(DISTINCT "{c}") AS d{i}' for i, c in enumerate(cols)]
    pair_alias: dict[tuple[int, int], str] = {}
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            alias = f"j_{i}_{j}"
            pair_alias[(i, j)] = alias
            parts.append(f'COUNT(DISTINCT ("{cols[i]}", "{cols[j]}")) AS {alias}')
    sql = f'SELECT {", ".join(parts)} FROM "{view_name}"'  # noqa: S608 — names are catalog dims
    try:
        row = duckdb_conn.execute(sql).fetchone()
    except Exception as e:  # noqa: BLE001 — any DuckDB error → skip this view, logged
        logger.warning("hierarchy_g3_scan_failed", view=view_name, error=str(e))
        return None
    if row is None:
        return None
    n = int(row[0])
    singles = {c: int(row[1 + i]) for i, c in enumerate(cols)}
    # ``parts`` is n, then the k singles, then the joints in ``pair_alias`` order —
    # so the j-th joint sits at offset ``1 + k + j`` (pair_alias is insertion-ordered).
    offset = 1 + len(cols)
    joints = {pair: int(row[offset + k]) for k, pair in enumerate(pair_alias)}
    return n, singles, joints


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
        walk([start])
    return chains


@dataclass(frozen=True)
class _Candidate:
    """A resolved candidate dimension column on one enriched view."""

    column_name: str  # the enriched-view column the g3 pass measures (member identity)
    column_id: str  # the catalog SliceDefinition's underlying column (provenance)


def _alias_groups(
    names: list[str], pairs: dict[tuple[str, str], _Pair]
) -> tuple[list[list[str]], dict[str, str]]:
    """Union-find 1:1 aliases (bidirectional g3 ≈ 0) into groups.

    Returns ``(groups, representative)``: ``groups`` lists each alias set (size ≥ 2,
    sorted, canonical = first); ``representative`` maps every column to its canonical
    so hierarchy detection runs on collapsed axes (a redundant axis never appears as
    its own level).
    """
    parent = {n: n for n in names}

    def find(x: str) -> str:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x: str, y: str) -> None:
        rx, ry = find(x), find(y)
        if rx != ry:
            # Point the lexicographically smaller root at the larger; the group's
            # canonical is recomputed as ``sorted(group)[0]`` below, so the exact
            # link direction here is not load-bearing — only connectivity is.
            parent[min(rx, ry)] = max(rx, ry)

    for (a, b), pair in pairs.items():
        if pair.g3(forward=True) <= FD_MAX_G3 and pair.g3(forward=False) <= FD_MAX_G3:
            union(a, b)

    members: dict[str, list[str]] = {}
    for n in names:
        members.setdefault(find(n), []).append(n)
    groups = [sorted(g) for g in members.values() if len(g) >= 2]
    representative = {n: sorted(g)[0] for g in members.values() for n in g}
    return groups, representative


def discover_dimension_hierarchies(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str,
) -> int:
    """Compute g3 hierarchies + aliases over each enriched view; persist run-versioned.

    Form-(a) writer (DAT-502): one row per ``(signature, run_id)``, UPSERTed;
    deterministic, so a redelivered run converges. Returns the rows persisted.
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
    for ev in enriched:
        defs = (
            session.execute(
                select(SliceDefinition).where(
                    SliceDefinition.table_id == ev.fact_table_id,
                    SliceDefinition.run_id == run_id,
                    SliceDefinition.column_name.isnot(None),
                )
            )
            .scalars()
            .all()
        )
        # Dedup by enriched column name (the propagation pass can emit a dim twice).
        by_name: dict[str, _Candidate] = {}
        for sd in defs:
            name = sd.column_name or ""
            if name and name not in by_name:
                by_name[name] = _Candidate(column_name=name, column_id=sd.column_id)
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

        scan = _g3_scan(duckdb_conn, ev.view_name, cand_names)
        if scan is None:
            continue
        rows.extend(
            _view_structures(
                fact_table_id=ev.fact_table_id,
                view_name=ev.view_name,
                run_id=run_id,
                by_name=by_name,
                cand_names=cand_names,
                scan=scan,
            )
        )

    # Fold the user's durable hierarchy/alias teaches into this run (DAT-537),
    # mirroring relationship-overlay materialization minus keeper-lift-up + witness
    # (g3 is deterministic). reject suppresses a g3 structure; add/alias assert one.
    rows = _apply_teaches(session, rows, table_ids=table_ids, run_id=run_id)

    upsert(session, DimensionHierarchy, rows, index_elements=["signature", "run_id"])
    return len(rows)


def _member_column_ids(
    session: Session, table_ids: list[str], run_id: str
) -> dict[str, dict[str, str]]:
    """``table_id -> {column_name: column_id}`` from this run's slice catalog.

    Resolves a manual teach's member columns to their catalog column ids; a member
    the catalog doesn't carry (a forced edge on an excluded/unknown column) resolves
    to ``""`` rather than failing the teach.
    """
    out: dict[str, dict[str, str]] = {}
    for sd in (
        session.execute(
            select(SliceDefinition).where(
                SliceDefinition.table_id.in_(table_ids),
                SliceDefinition.run_id == run_id,
                SliceDefinition.column_name.isnot(None),
            )
        )
        .scalars()
        .all()
    ):
        if sd.column_name:
            out.setdefault(sd.table_id, {})[sd.column_name] = sd.column_id
    return out


def _apply_teaches(
    session: Session,
    rows: list[dict[str, object]],
    *,
    table_ids: list[str],
    run_id: str,
) -> list[dict[str, object]]:
    """Apply reject / add / alias hierarchy overlays to the g3 row set.

    reject drops the g3 structure with a matching ``(table_id, member-set)``
    (kind-agnostic — a member-set is one structure); add asserts a ``manual``
    drilldown, alias a ``manual`` alias. A manual assert overrides a same-signature
    g3 row (clears ``needs_confirmation``). Keyed by signature so the result stays
    one row per ``(signature, run_id)``.
    """
    by_sig: dict[str, dict[str, object]] = {str(r["signature"]): r for r in rows}

    def _row_member_names(r: dict[str, object]) -> frozenset[str]:
        members = cast("list[dict[str, object]]", r["members"])
        return frozenset(str(m["column_name"]) for m in members)

    # One read of the active hierarchy teaches, grouped by action (the parser
    # re-queries per action, so load each once).
    specs = {a: hierarchy_overlay_specs(session, a) for a in ("reject", "add", "alias")}

    # reject: drop any g3 structure whose table + member-set matches.
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
    col_ids = _member_column_ids(session, table_ids, run_id)
    for action, kind in (("add", "drilldown"), ("alias", "alias")):
        for spec in specs[action]:
            members = spec.members
            if kind == "drilldown" and len(members) < 2:
                logger.info("hierarchy_teach_skipped", reason="drilldown_needs_2_levels", spec=spec)
                continue
            names = col_ids.get(spec.table_id, {})
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
    cand_names: list[str],
    scan: tuple[int, dict[str, int], dict[tuple[int, int], int]],
) -> list[dict[str, object]]:
    """The drill-down + alias row dicts for one enriched view from its g3 scan.

    A module-level helper (not a closure in the per-view loop) so its inner
    functions bind these parameters, not loop variables.
    """
    n_rows, singles, joints = scan
    pairs: dict[tuple[str, str], _Pair] = {
        (cand_names[i], cand_names[j]): _Pair(
            d_a=singles[cand_names[i]], d_b=singles[cand_names[j]], d_ab=d_ij
        )
        for (i, j), d_ij in joints.items()
    }

    # Constant columns (< MIN_DISTINCT_DIMENSION distinct) are dropped from BOTH
    # roles: not a meaningful axis, and as a DEPENDENT a constant is trivially
    # determined by everything, manufacturing junk edges. Born-loud on each drop.
    eligible = [c for c in cand_names if singles[c] >= MIN_DISTINCT_DIMENSION]
    for c in cand_names:
        if singles[c] < MIN_DISTINCT_DIMENSION:
            logger.info(
                "hierarchy_column_excluded", column=c, reason="constant", distinct=singles[c]
            )
    if len(eligible) < 2:
        return []
    elig_pairs = {(a, b): p for (a, b), p in pairs.items() if a in eligible and b in eligible}

    # Collapse 1:1 aliases first, then detect hierarchies on canonical axes.
    groups, rep = _alias_groups(eligible, elig_pairs)

    # A column is rejected as a DETERMINANT (it may still be a dependent/coarsest
    # level) when it is too coarse to determine non-trivially (≤2 distinct) or
    # near-key (each value ~unique → spurious FD). Born-loud on every exclusion.
    def _bad_determinant(col: str) -> bool:
        d = singles[col]
        if d < MIN_DISTINCT_DETERMINANT:
            logger.info(
                "hierarchy_determinant_excluded", column=col, reason="too_coarse", distinct=d
            )
            return True
        if n_rows and d >= NEAR_KEY_FRAC * n_rows:
            logger.info(
                "hierarchy_determinant_excluded",
                column=col,
                reason="near_key",
                distinct=d,
                rows=n_rows,
            )
            return True
        return False

    # Directed FD edges on canonical reps: a → b when a determines b (g3 ≈ 0)
    # AND a is strictly finer (more distinct) — finest → coarsest drill direction.
    edges: set[tuple[str, str]] = set()
    for (a, b), pair in elig_pairs.items():
        ra, rb = rep[a], rep[b]
        if ra == rb:  # same alias group — not a level relationship
            continue
        fwd, bwd = pair.g3(forward=True), pair.g3(forward=False)
        if fwd <= FD_MAX_G3 and pair.d_a > pair.d_b and not _bad_determinant(a):
            edges.add((ra, rb))
        elif bwd <= FD_MAX_G3 and pair.d_b > pair.d_a and not _bad_determinant(b):
            edges.add((rb, ra))

    reduced = _transitive_reduction(edges)

    # Per-edge g3 (on the original measured columns behind each rep) for scoring.
    # ``pairs`` keys are always (lex-smaller, lex-larger) because
    # ``cand_names = sorted(by_name)`` — so ``forward=(a, b) in pairs`` resolves
    # which endpoint is ``d_a`` in the stored ``_Pair``. (If cand_names ever stops
    # being lexicographically sorted, this direction test must be revisited.)
    def _edge_g3(a: str, b: str) -> float:
        pair = pairs.get((a, b)) or pairs.get((b, a))
        if pair is None:
            return 0.0
        return pair.g3(forward=(a, b) in pairs)

    def _member(col: str) -> dict[str, object]:
        c = by_name[col]
        return {
            "column_name": c.column_name,
            "column_id": c.column_id,
            "distinct_count": singles[col],
        }

    needs_conf = n_rows < MIN_SUPPORT_ROWS
    out: list[dict[str, object]] = []

    for chain in _maximal_chains(reduced):
        score = max(_edge_g3(chain[k], chain[k + 1]) for k in range(len(chain) - 1))
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
        # Representative-pair score. For a 3+ member group every pair already passed
        # the alias threshold in union-find, so the first pair's g3 bounds the group
        # (≤ FD_MAX_G3) — a faithful, conservative score.
        gpair = pairs.get((group[0], group[1])) or pairs.get((group[1], group[0]))
        score = max(gpair.g3(forward=True), gpair.g3(forward=False)) if gpair else 0.0
        out.append(
            {
                "run_id": run_id,
                "table_id": fact_table_id,
                "kind": "alias",
                "members": [_member(c) for c in group],
                "canonical_label": group[0],
                "signature": f"alias:{fact_table_id}:" + "|".join(sorted(group)),
                "score": score,
                "detection_source": "g3",
                "needs_confirmation": needs_conf,
            }
        )
        logger.info("hierarchy_alias", view=view_name, group=group, score=round(score, 4))

    return out
