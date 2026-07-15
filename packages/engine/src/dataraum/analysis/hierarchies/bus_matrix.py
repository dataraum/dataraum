"""Bus-matrix derivation (DAT-762 Part 2) — fact × dimension exposure, per run.

Runs inside the ``dimension_hierarchies`` phase AFTER structure discovery, over
the same session scope. Three legs, one writer:

- **referenced** — purely structural: this run's ``SliceDefinition`` rows with a
  resolved ``dimension_table_id`` (DAT-756), grouped per (fact, dim table);
  ``roles`` carries the FK-role multiplicity and ``confirmation_source`` is the
  WEAKEST source across the roles' underlying relationships (the honest floor,
  read through ``EnrichedView.relationship_ids`` — the slicing-phase
  convention, never name-inferred).
- **folded** — the stats group, the judge decides identity (DAT-762 posture):
  fold components are connected sets of this run's discovered structures whose
  members are all fact-own, non-referenced columns; the conform judge decides
  CROSS-FACT identity over names + attribute sets + authored column meanings
  (``ColumnConcept.meaning``, DAT-769 — context evidence, never a bypass).
  Judge abstain → per-fact cells with ``needs_confirmation=True``, never an
  asserted shared concept. Vetoed (``needs_confirmation``) and ``kind='role'``
  structures never enter a component — role pairs are separate axes by design.
- **degenerate** — near-key, id-shaped fact columns (the ``NEAR_KEY_FRAC``
  guard's exclusions re-derived, shape-gated to idlike/code via the routing
  classifier): the fact-grain operational identifier recorded as its own cell
  so the abstention is visible.

The conform lane mirrors the veto lane's error posture: a failed judgment is
recorded (cells persist per-fact, unconformed), a PERMANENT provider error
skips conform for the run, a TRANSIENT one propagates to the Temporal boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import delete, select

from dataraum.analysis.hierarchies import routing
from dataraum.analysis.hierarchies.db_models import BusMatrixEntry, DimensionHierarchy
from dataraum.analysis.hierarchies.judge import ConformVerdict, DimensionIdentityJudge
from dataraum.analysis.hierarchies.processor import (
    NEAR_KEY_FRAC,
    column_evidence,
    pull_sample,
    quote_ident,
    resolve_candidates,
    view_columns,
)
from dataraum.analysis.semantic.utils import load_column_concepts
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.llm.providers.base import PermanentProviderError
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    from collections.abc import Sequence

    import duckdb
    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# Weakest-first provenance rank (DAT-776 vocabulary at cell grain): a
# referenced cell inherits the floor across its roles' relationships.
_SOURCE_RANK = {"unconfirmed": 0, "judge": 1, "keeper": 2, "user": 3}

# Degenerate cells are id-shaped only: a near-key prose or temporal column is
# an attribute/timestamp, not an operational identifier.
_DEGENERATE_SHAPES = frozenset({"idlike", "code"})

# Conform candidates per judgment call: pairs grow O(components²) with
# facts × fold groups, so the batch is chunked (the per-pair ref scheme keeps
# chunks independent) instead of one unbounded prompt.
_CONFORM_BATCH_MAX = 32


@dataclass
class BusMatrixStats:
    """The bus-matrix derivation's observable outcome — a first-class phase output.

    Same posture as ``VetoLaneStats``: the conform lane is advisory (cells exist
    with or without cross-fact judgment), so a failed conform call must never
    fail the phase — and must never die silently either.
    """

    status: str = "ran"  # ran | failed (the conform lane)
    referenced: int = 0
    folded: int = 0
    degenerate: int = 0
    conform_pairs: int = 0
    conformed: int = 0
    abstained: int = 0
    # Pairs the judge returned NO verdict for (a truncating model must be
    # visible in the phase outputs, not uphold-by-omission).
    unanswered: int = 0

    def as_output(self) -> dict[str, object]:
        return {
            "status": self.status,
            "referenced": self.referenced,
            "folded": self.folded,
            "degenerate": self.degenerate,
            "conform_pairs": self.conform_pairs,
            "conformed": self.conformed,
            "abstained": self.abstained,
            "unanswered": self.unanswered,
        }


@dataclass
class _FoldComponent:
    """One fact's folded-dimension group: connected structures over folded columns."""

    fact_table_id: str
    fact_table_name: str
    # column_name -> distinct_count (None on manual members), union over structures.
    members: dict[str, int | None]
    has_manual: bool
    # Assigned by the conform pass; None = unconformed (own label).
    concept_label: str | None = None
    # The conform-connected component's deterministic signature (DAT-800 group
    # key); None = no cross-fact identity asserted.
    conformed_group: str | None = None
    conformed: bool = False
    abstained: bool = False

    @property
    def fold_key(self) -> str:
        """The finest member (max distinct) — the fold's key column.

        A 1:1 alias group ties on distinct count (no statistical direction);
        the alphabetically-first name is then the deterministic canonical pick.
        """
        return sorted(self.members.items(), key=lambda kv: (-(kv[1] or 0), kv[0]))[0][0]

    @property
    def attributes(self) -> list[str]:
        key = self.fold_key
        return sorted(c for c in self.members if c != key)


def derive_bus_matrix(
    session: Session,
    *,
    duckdb_conn: duckdb.DuckDBPyConnection,
    table_ids: list[str],
    run_id: str,
    judge: DimensionIdentityJudge,
) -> tuple[int, BusMatrixStats]:
    """Derive and persist this run's bus-matrix cells; returns (cells, stats)."""
    stats = BusMatrixStats()
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
    if not enriched:
        return 0, stats
    fact_ids = sorted({ev.fact_table_id for ev in enriched})
    slices = (
        session.execute(
            select(SliceDefinition).where(
                SliceDefinition.run_id == run_id, SliceDefinition.table_id.in_(fact_ids)
            )
        )
        .scalars()
        .all()
    )
    # Facts AND the referenced dim targets — the only names any leg reports.
    named_ids = set(fact_ids) | {s.dimension_table_id for s in slices if s.dimension_table_id}
    name_of = {
        t.table_id: t.table_name
        for t in session.execute(select(Table).where(Table.table_id.in_(named_ids))).scalars()
    }

    rows: list[dict[str, object]] = []
    rows += _referenced_cells(session, enriched, slices, name_of, run_id=run_id, stats=stats)
    referenced_key_cols = {s.column_id for s in slices if s.dimension_table_id is not None}
    components = _fold_components(
        session, fact_ids, name_of, run_id=run_id, referenced_key_cols=referenced_key_cols
    )
    _conform_pass(session, components, run_id=run_id, judge=judge, stats=stats)
    rows += _folded_cells(components, run_id=run_id, stats=stats)
    fold_member_cols = {c for comp in components for c in comp.members}
    rows += _degenerate_cells(
        session,
        duckdb_conn,
        enriched,
        name_of,
        run_id=run_id,
        referenced_key_cols=referenced_key_cols,
        fold_member_cols=fold_member_cols,
        stats=stats,
    )

    # Retry stability: folded-cell identity depends on the LLM lanes (a veto
    # changes component membership → member_key → signature), so a
    # crash-after-commit redelivery with a flipped verdict would strand the
    # first attempt's cells under the same run_id — invisible to the upsert,
    # visible through ``current_bus_matrix``. Delete-then-insert in ONE
    # transaction is exactly idempotent when row identity is not retry-stable;
    # the upsert + unique constraint stay as the in-batch backstop.
    session.execute(
        delete(BusMatrixEntry).where(
            BusMatrixEntry.run_id == run_id, BusMatrixEntry.fact_table_id.in_(fact_ids)
        )
    )
    upsert(session, BusMatrixEntry, rows, index_elements=["signature", "run_id"])
    logger.info("bus_matrix_derived", **stats.as_output())
    return len(rows), stats


def _referenced_cells(
    session: Session,
    enriched: Sequence[EnrichedView],
    slices: Sequence[SliceDefinition],
    name_of: dict[str, str],
    *,
    run_id: str,
    stats: BusMatrixStats,
) -> list[dict[str, object]]:
    """One cell per (fact, referenced dimension table), roles = FK multiplicity."""
    # FK column -> the underlying relationship's confirmation_source, through the
    # views' relationship provenance (the slicing-phase convention).
    rel_ids = sorted({rid for ev in enriched for rid in (ev.relationship_ids or [])})
    source_by_fk_col: dict[str, str] = {}
    if rel_ids:
        from dataraum.analysis.relationships.db_models import Relationship

        for rel in session.execute(
            select(Relationship).where(Relationship.relationship_id.in_(rel_ids))
        ).scalars():
            source_by_fk_col[rel.from_column_id] = rel.confirmation_source

    grouped: dict[tuple[str, str], list[SliceDefinition]] = {}
    for s in slices:
        if s.dimension_table_id is not None:
            grouped.setdefault((s.table_id, s.dimension_table_id), []).append(s)

    out: list[dict[str, object]] = []
    for (fact_id, dim_id), group in sorted(grouped.items()):
        roles = sorted({s.fk_role or s.column_name or "" for s in group} - {""})
        attributes = sorted({s.dimension_attribute for s in group if s.dimension_attribute})
        source = min(
            (source_by_fk_col.get(s.column_id, "unconfirmed") for s in group),
            key=lambda v: _SOURCE_RANK.get(v, 0),
        )
        out.append(
            {
                "run_id": run_id,
                "fact_table_id": fact_id,
                "attachment": "referenced",
                "concept_label": name_of.get(dim_id, dim_id),
                "dimension_table_id": dim_id,
                "roles": roles,
                "attributes": attributes,
                "confirmation_source": source,
                "conformed_group": None,
                "needs_confirmation": False,
                "signature": f"bus:referenced:{fact_id}:{dim_id}",
            }
        )
    stats.referenced = len(out)
    return out


def _fold_components(
    session: Session,
    fact_ids: list[str],
    name_of: dict[str, str],
    *,
    run_id: str,
    referenced_key_cols: set[str],
) -> list[_FoldComponent]:
    """Connected fold groups from this run's discovered structures, per fact.

    A structure qualifies when every member is a FACT-OWN column (its
    ``column_id`` belongs to the fact — a joined ``fk__attr`` column resolves to
    the dim table's column and is the referenced leg's territory) and none is a
    referenced slice key. Vetoed/undecided (``needs_confirmation``) and
    ``kind='role'`` structures never enter — abstain over assert.
    """
    fact_col_ids: dict[str, set[str]] = {fid: set() for fid in fact_ids}
    for col in session.execute(select(Column).where(Column.table_id.in_(fact_ids))).scalars():
        fact_col_ids[col.table_id].add(col.column_id)

    structures = (
        session.execute(
            select(DimensionHierarchy).where(
                DimensionHierarchy.run_id == run_id,
                DimensionHierarchy.table_id.in_(fact_ids),
                DimensionHierarchy.kind.in_(["drilldown", "alias"]),
                DimensionHierarchy.needs_confirmation.is_(False),
            )
        )
        .scalars()
        .all()
    )

    # Union-find over member column names, per fact.
    parent: dict[tuple[str, str], tuple[str, str]] = {}

    def find(x: tuple[str, str]) -> tuple[str, str]:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    info: dict[tuple[str, str], tuple[int | None, bool]] = {}  # (distinct, manual) per node
    for st in sorted(structures, key=lambda s: s.signature):
        members = st.members
        own = fact_col_ids.get(st.table_id, set())
        col_ids = [str(m["column_id"]) for m in members]
        col_names = [str(m["column_name"]) for m in members]
        # An empty column_id is an unresolved member (manual teach), fact-own
        # by assumption — EXCEPT when the name carries the ``__`` join marker:
        # an unresolvable joined ``fk__attr`` column is referenced territory.
        if not all(
            (cid in own or (cid == "" and "__" not in name))
            for cid, name in zip(col_ids, col_names, strict=True)
        ):
            continue  # touches a joined dim column — referenced territory
        if any(cid in referenced_key_cols for cid in col_ids):
            continue  # rooted on a referenced FK key — already a referenced cell
        manual = st.detection_source == "manual"
        nodes = []
        for m in members:
            node = (st.table_id, str(m["column_name"]))
            parent.setdefault(node, node)
            d = m.get("distinct_count")
            prev = info.get(node, (None, False))
            info[node] = (
                cast("int | None", d) if d is not None else prev[0],
                prev[1] or manual,
            )
            nodes.append(node)
        for a, b in zip(nodes, nodes[1:], strict=False):
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb

    groups: dict[tuple[str, str], _FoldComponent] = {}
    for node in sorted(parent):
        root = find(node)
        fid = node[0]
        comp = groups.get(root)
        if comp is None:
            comp = groups[root] = _FoldComponent(
                fact_table_id=fid,
                fact_table_name=name_of.get(fid, fid),
                members={},
                has_manual=False,
            )
        d, manual = info[node]
        comp.members[node[1]] = d
        comp.has_manual = comp.has_manual or manual
    # Name-stable order: pair refs and prompt order must not depend on uuids.
    return sorted(groups.values(), key=lambda c: (c.fact_table_name, c.fold_key, c.fact_table_id))


def _conform_pass(
    session: Session,
    components: list[_FoldComponent],
    *,
    run_id: str,
    judge: DimensionIdentityJudge,
    stats: BusMatrixStats,
) -> None:
    """Cross-fact identity via the conform judge; verdicts land on the components.

    Candidates are all cross-fact component pairs, judged in chunks of
    ``_CONFORM_BATCH_MAX`` (components grow with facts × fold groups — one
    unbounded prompt would degrade exactly when the corpus is widest).
    Meanings are authored column context (``ColumnConcept.meaning``), served
    as corroborating evidence.

    Group identity is the CONFORM-CONNECTED COMPONENT, not the label: the
    judge's conform verdicts are union-found, each connected group gets one
    deterministic ``conformed_group`` signature (its members) and one label
    (the first conform verdict in deterministic pair order). Keying on the
    label instead would silently SPLIT a group whose verdicts drifted labels
    and silently MERGE two distinct groups sharing a generic label —
    discarding the judge's own DISTINCT verdict.
    """
    pairs = [
        (i, j)
        for i in range(len(components))
        for j in range(i + 1, len(components))
        if components[i].fact_table_id != components[j].fact_table_id
    ]
    stats.conform_pairs = len(pairs)
    if not pairs:
        return

    fact_ids = sorted({c.fact_table_id for c in components})
    concepts = load_column_concepts(session, fact_ids, run_id)
    meaning_by_col: dict[tuple[str, str], str] = {}
    for col in session.execute(select(Column).where(Column.table_id.in_(fact_ids))).scalars():
        concept = concepts.get(col.column_id)
        if concept is not None and concept.meaning:
            meaning_by_col[(col.table_id, col.column_name)] = concept.meaning

    def side(comp: _FoldComponent) -> dict[str, object]:
        meanings = {
            c: meaning_by_col[(comp.fact_table_id, c)]
            for c in sorted(comp.members)
            if (comp.fact_table_id, c) in meaning_by_col
        }
        return {
            "fact_table": comp.fact_table_name,
            "key": comp.fold_key,
            "attributes": comp.attributes,
            "meanings": meanings,
        }

    candidates = [
        {"ref": f"pair:{i}:{j}", "left": side(components[i]), "right": side(components[j])}
        for i, j in pairs
    ]
    verdicts: list[ConformVerdict] = []
    for start in range(0, len(candidates), _CONFORM_BATCH_MAX):
        chunk = candidates[start : start + _CONFORM_BATCH_MAX]
        try:
            result = judge.conform(candidates=chunk)
        except PermanentProviderError as e:
            # Permanent = a retry cannot help; transient errors deliberately
            # propagate to the Temporal boundary (the veto lane's contract).
            logger.warning("bus_matrix_conform_skipped", reason=str(e))
            stats.status = "failed"
            return
        if not result.success:
            logger.warning("bus_matrix_conform_skipped", reason=result.error)
            stats.status = "failed"
            return
        verdicts.extend(result.unwrap())

    by_ref = {f"pair:{i}:{j}": (i, j) for i, j in pairs}

    # Union-find over the components; conform verdicts are the edges.
    parent = list(range(len(components)))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    conform_edges: list[tuple[int, int, str]] = []  # (i, j, label) in verdict order
    non_conform: list[tuple[int, int, str]] = []  # (i, j, verdict) for consistency check
    abstain_touched: set[int] = set()
    seen_refs: set[str] = set()
    for verdict in verdicts:
        pair = by_ref.get(verdict.pair_ref)
        if pair is None:
            logger.warning("bus_matrix_conform_unknown_ref", ref=verdict.pair_ref)
            continue
        if verdict.pair_ref in seen_refs:
            continue  # first verdict per ref wins; duplicates never double-count
        seen_refs.add(verdict.pair_ref)
        i, j = pair
        left, right = components[i], components[j]
        logger.info(
            "bus_matrix_conform",
            left=f"{left.fact_table_name}.{left.fold_key}",
            right=f"{right.fact_table_name}.{right.fold_key}",
            verdict=verdict.verdict,
            concept=verdict.concept_label,
            reason=verdict.reason,
        )
        if verdict.verdict == "conform":
            stats.conformed += 1
            # concept_label is non-None here (ConformVerdict's validator).
            conform_edges.append((i, j, verdict.concept_label or left.fold_key))
            parent[find(i)] = find(j)
        elif verdict.verdict == "abstain":
            stats.abstained += 1
            abstain_touched.update(pair)
        else:
            # 'role' / 'distinct': separate axes — no flag (the judge answered;
            # the answer is "two dimensions"). Kept for the consistency check.
            non_conform.append((i, j, verdict.verdict))

    # A pair with no verdict is silently unjudged otherwise — make the
    # truncation observable (the DAT-536 inert-safeguard lesson).
    missing = sorted(set(by_ref) - seen_refs)
    if missing:
        stats.unanswered = len(missing)
        logger.warning("bus_matrix_conform_unanswered", refs=missing)

    # One label + one group signature per connected component: the FIRST
    # conform verdict on the component names it; later differing labels are
    # drift — reported, never applied (and never a split: the group key is
    # the component, not the label).
    canon: dict[int, tuple[str, str]] = {}
    for i, j, label in conform_edges:
        root = find(i)
        if root not in canon:
            canon[root] = (label, _group_key(components, parent, root))
        elif canon[root][0] != label:
            logger.warning(
                "bus_matrix_conform_label_drift",
                left=f"{components[i].fact_table_name}.{components[i].fold_key}",
                right=f"{components[j].fact_table_name}.{components[j].fold_key}",
                kept=canon[root][0],
                rejected=label,
            )
    for k, comp in enumerate(components):
        root = find(k)
        if root in canon:
            comp.conformed = True
            comp.concept_label, comp.conformed_group = canon[root]

    # The judge separating a pair that its own conform verdicts transitively
    # joined is instability — observable, never reconciled deterministically.
    for i, j, kind in non_conform:
        if find(i) == find(j):
            logger.warning(
                "bus_matrix_conform_inconsistent",
                left=f"{components[i].fact_table_name}.{components[i].fold_key}",
                right=f"{components[j].fact_table_name}.{components[j].fold_key}",
                separating_verdict=kind,
            )

    for k in abstain_touched:
        if not components[k].conformed:
            components[k].abstained = True


def _group_key(components: list[_FoldComponent], parent: list[int], root: int) -> str:
    """Deterministic conformed-group signature: the component's member set."""

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    members = sorted(
        f"{c.fact_table_id}:{c.fold_key}" for k, c in enumerate(components) if find(k) == root
    )
    return "conform:" + "|".join(members)


def _folded_cells(
    components: list[_FoldComponent], *, run_id: str, stats: BusMatrixStats
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    for comp in components:
        if comp.has_manual:
            source = "user"
        elif comp.conformed:
            source = "judge"
        else:
            source = "unconfirmed"
        member_key = "|".join(sorted(comp.members))
        out.append(
            {
                "run_id": run_id,
                "fact_table_id": comp.fact_table_id,
                "attachment": "folded",
                "concept_label": comp.concept_label or comp.fold_key,
                "dimension_table_id": None,
                "roles": [comp.fold_key],
                "attributes": comp.attributes,
                "confirmation_source": source,
                "conformed_group": comp.conformed_group,
                "needs_confirmation": comp.abstained and not comp.conformed,
                "signature": f"bus:folded:{comp.fact_table_id}:{member_key}",
            }
        )
    stats.folded = len(out)
    return out


def _degenerate_cells(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    enriched: Sequence[EnrichedView],
    name_of: dict[str, str],
    *,
    run_id: str,
    referenced_key_cols: set[str],
    fold_member_cols: set[str],
    stats: BusMatrixStats,
) -> list[dict[str, object]]:
    """Near-key, id-shaped fact columns → one degenerate cell each.

    Re-derives the discovery pass's ``NEAR_KEY_FRAC`` exclusion with a light
    per-view aggregate (no pair scan), then shape-gates on the routing
    classifier's value evidence — a unique prose/timestamp column is an
    attribute, not an operational identifier.
    """
    out: list[dict[str, object]] = []
    for ev in sorted(enriched, key=lambda e: e.view_name):
        view_cols = view_columns(duckdb_conn, ev.view_name)
        if view_cols is None:
            continue
        by_name = resolve_candidates(session, ev, view_cols)
        cand = sorted(
            c
            for c, meta in by_name.items()
            if meta.column_id not in referenced_key_cols and c not in fold_member_cols
        )
        if not cand:
            continue
        parts = ["COUNT(*)"] + [
            f"COUNT(DISTINCT {quote_ident(c)}), COUNT({quote_ident(c)})" for c in cand
        ]
        try:
            row = duckdb_conn.execute(
                f"SELECT {', '.join(parts)} FROM {quote_ident(ev.view_name)}"  # noqa: S608
            ).fetchone()
        except Exception as e:  # noqa: BLE001 — skip this view, logged (visible abstention)
            logger.warning("bus_matrix_degenerate_scan_failed", view=ev.view_name, error=str(e))
            continue
        if row is None or not row[0]:
            continue
        n = int(row[0])
        d_sql = {c: int(row[2 * i + 1]) for i, c in enumerate(cand)}
        # The discovery guard's statistic is the NULL-AWARE distinct count
        # (processor's d2: NULL is a groupable value) — match it exactly.
        d2 = {c: d_sql[c] + (1 if int(row[2 * i + 2]) < n else 0) for i, c in enumerate(cand)}
        near_keys = [c for c in cand if d2[c] >= NEAR_KEY_FRAC * n]
        if not near_keys:
            continue
        frame = pull_sample(duckdb_conn, ev.view_name, near_keys, n)
        if frame is None:
            continue
        for c in near_keys:
            shape = routing.classify_shape(column_evidence(frame, c, n_rows=n, d_sql=d_sql))
            if shape not in _DEGENERATE_SHAPES:
                continue
            out.append(
                {
                    "run_id": run_id,
                    "fact_table_id": ev.fact_table_id,
                    "attachment": "degenerate",
                    "concept_label": c,
                    "dimension_table_id": None,
                    "roles": [c],
                    "attributes": [],
                    "confirmation_source": "unconfirmed",
                    "conformed_group": None,
                    "needs_confirmation": False,
                    "signature": f"bus:degenerate:{ev.fact_table_id}:{c}",
                }
            )
    stats.degenerate = len(out)
    return out
