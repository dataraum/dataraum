"""Bus-matrix derivation (DAT-762 Part 2) — fact × dimension exposure, per run.

Runs inside the ``dimension_hierarchies`` phase AFTER structure discovery, over
the same session scope. Two legs, one writer:

- **referenced** — this run's ``SliceDefinition`` rows with a resolved
  ``dimension_table_id`` (DAT-756), grouped per (fact, dim table, ROLE identity).
  Role-playing FKs to one dim (bill-to vs ship-to) are SEPARATE cells (DAT-788)
  unless the conform judge merges their roles: same-named roles across facts
  auto-conform structurally (no LLM), differently-named cross-fact pairs are
  judged, and ``role`` / ``distinct`` / ``abstain`` / unjudged keep them apart.
  ``conformed_group`` carries the content-derived role identity, ``roles`` the
  fact's FK roles in that identity, and ``confirmation_source`` the WEAKEST source
  across their underlying relationships (the honest structural floor, read through
  ``EnrichedView.relationship_ids`` — the slicing-phase convention, never
  name-inferred; orthogonal to the conform decision).
- **folded** — the stats group, the judge decides identity (DAT-762 posture):
  fold components are connected sets of this run's discovered structures whose
  members are all fact-own, non-referenced columns; the conform judge decides
  CROSS-FACT identity over names + attribute sets + authored column meanings
  (``ColumnConcept.meaning``, DAT-769 — context evidence, never a bypass).
  Judge abstain → per-fact cells with ``needs_confirmation=True``, never an
  asserted shared concept. Undecided (``needs_confirmation``) and ``kind='role'``
  structures never enter a component — role pairs are separate axes by design.

Error posture: a failed conform judgment is recorded (cells persist per-fact,
unconformed), a PERMANENT provider error skips conform for the run, a TRANSIENT
one propagates to the Temporal boundary.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, cast

from sqlalchemy import delete, select

from dataraum.analysis.hierarchies.db_models import BusMatrixEntry, DimensionHierarchy
from dataraum.analysis.hierarchies.judge import ConformVerdict, DimensionIdentityJudge
from dataraum.analysis.semantic.utils import load_column_concepts
from dataraum.analysis.slicing.db_models import SliceDefinition
from dataraum.analysis.views.db_models import EnrichedView
from dataraum.core.logging import get_logger
from dataraum.llm.providers.base import PermanentProviderError
from dataraum.storage import Column, Table
from dataraum.storage.upsert import upsert

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = get_logger(__name__)

# Weakest-first provenance rank (DAT-776 vocabulary at cell grain): a
# referenced cell inherits the floor across its roles' relationships.
_SOURCE_RANK = {"unconfirmed": 0, "judge": 1, "keeper": 2, "user": 3}

# Conform candidates per judgment call: pairs grow O(components²) with
# facts × fold groups, so the batch is chunked (the per-pair ref scheme keeps
# chunks independent) instead of one unbounded prompt.
_CONFORM_BATCH_MAX = 32


@dataclass
class BusMatrixStats:
    """The bus-matrix derivation's observable outcome — a first-class phase output.

    The conform lane is advisory (cells exist with or without cross-fact
    judgment), so a failed conform call must never fail the phase — and must
    never die silently either.
    """

    status: str = "ran"  # ran | failed (the conform lane)
    referenced: int = 0
    folded: int = 0
    conform_pairs: int = 0
    conformed: int = 0
    abstained: int = 0
    # Pairs the judge returned NO verdict for (a truncating model must be
    # visible in the phase outputs, not uphold-by-omission).
    unanswered: int = 0
    # Referenced-role conform metrics (DAT-788), kept SEPARATE from the folded
    # counters above: the referenced leg judges differently-named cross-fact
    # FK-role pairs to decide whether role-playing FKs (bill-to vs ship-to)
    # conform to ONE identity or stay distinct axes. Same-named roles auto-conform
    # structurally (no LLM call), so these count only the genuinely ambiguous pairs.
    ref_conform_pairs: int = 0
    ref_conformed: int = 0
    ref_abstained: int = 0
    ref_unanswered: int = 0

    def as_output(self) -> dict[str, object]:
        return {
            "status": self.status,
            "referenced": self.referenced,
            "folded": self.folded,
            "conform_pairs": self.conform_pairs,
            "conformed": self.conformed,
            "abstained": self.abstained,
            "unanswered": self.unanswered,
            "ref_conform_pairs": self.ref_conform_pairs,
            "ref_conformed": self.ref_conformed,
            "ref_abstained": self.ref_abstained,
            "ref_unanswered": self.ref_unanswered,
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


@dataclass
class _RefExposure:
    """One fact's referenced exposure of a dimension table via ONE FK role (DAT-788).

    The role-conform unit: ``(fact, dim table, fk role)``. Several slices at
    different attributes (``account_id__type``, ``account_id__name``) collapse
    here — the role identity is attribute-invariant. The conform pass assigns
    ``conformed_group`` (the content-derived identity signature) and, when the
    judge abstained on a differently-named pair touching this role,
    ``needs_confirmation``. ``sources`` are the FK relationship confirmation
    sources across the role's slices (the referenced provenance floor, unchanged
    by the conform decision — who confirmed the STRUCTURE is orthogonal to whether
    the judge conformed the cross-role IDENTITY).
    """

    fact_table_id: str
    fact_table_name: str
    dim_table_id: str
    fk_role: str
    attributes: list[str]
    sources: list[str]
    conformed_group: str | None = None
    needs_confirmation: bool = False


def _ref_group_signature(dim_table_id: str, role_names: set[str]) -> str:
    """The referenced role-group's content-derived identity signature (DAT-788).

    Keyed on the dim table id + the sorted DISTINCT FK-role NAMES in the conform
    component — never a per-run uuid (the ``relationship_id`` bug class). Two
    exposures share an identity iff they land in the same component: same-named
    roles across facts (structural auto-conform) or a judge ``conform`` verdict
    over differently-named roles. Bill-to and ship-to (distinct names, no conform
    edge) get distinct signatures — separate axes, the DAT-788 fix.
    """
    return f"ref:{dim_table_id}:" + "|".join(sorted(role_names))


def derive_bus_matrix(
    session: Session,
    *,
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
    exposures = _referenced_exposures(session, enriched, slices, name_of)
    _referenced_conform_pass(session, exposures, run_id=run_id, judge=judge, stats=stats)
    rows += _referenced_cells(exposures, name_of, run_id=run_id, stats=stats)
    referenced_key_cols = {s.column_id for s in slices if s.dimension_table_id is not None}
    components = _fold_components(
        session, fact_ids, name_of, run_id=run_id, referenced_key_cols=referenced_key_cols
    )
    _conform_pass(session, components, run_id=run_id, judge=judge, stats=stats)
    rows += _folded_cells(components, run_id=run_id, stats=stats)

    # Retry stability: a folded cell's signature carries its component's member
    # set, which is derived from this run's discovered structures — and those
    # include the user's teach overlay, which can change BETWEEN activity
    # attempts (a teach landing after a crash shifts component membership →
    # member_key → signature). A redelivery would then strand the first
    # attempt's cells under the same run_id — invisible to the upsert, visible
    # through ``current_bus_matrix``. Delete-then-insert in ONE transaction
    # replaces the run's cell set wholesale, so no cell this derivation did not
    # emit can survive; the upsert + unique constraint stay as the in-batch
    # backstop.
    session.execute(
        delete(BusMatrixEntry).where(
            BusMatrixEntry.run_id == run_id, BusMatrixEntry.fact_table_id.in_(fact_ids)
        )
    )
    upsert(session, BusMatrixEntry, rows, index_elements=["signature", "run_id"])
    logger.info("bus_matrix_derived", **stats.as_output())
    return len(rows), stats


def _referenced_exposures(
    session: Session,
    enriched: Sequence[EnrichedView],
    slices: Sequence[SliceDefinition],
    name_of: dict[str, str],
) -> list[_RefExposure]:
    """This run's referenced exposures, one per (fact, dim table, FK role).

    Groups the run's dimension-resolved slices by their FK role (DAT-788): the
    role identity is attribute-invariant, so ``account_id__type`` and
    ``account_id__name`` are ONE exposure. Each carries the FK relationship
    confirmation sources (the referenced provenance floor). Name-stable order so
    the conform pair refs and prompt order never depend on uuids.
    """
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

    # (fact, dim, fk_role) -> the collapsing slices.
    grouped: dict[tuple[str, str, str], list[SliceDefinition]] = {}
    for s in slices:
        if s.dimension_table_id is None:
            continue
        role = s.fk_role or s.column_name or ""
        if not role:
            continue
        grouped.setdefault((s.table_id, s.dimension_table_id, role), []).append(s)

    exposures = [
        _RefExposure(
            fact_table_id=fact_id,
            fact_table_name=name_of.get(fact_id, fact_id),
            dim_table_id=dim_id,
            fk_role=role,
            attributes=sorted({s.dimension_attribute for s in group if s.dimension_attribute}),
            sources=[source_by_fk_col.get(s.column_id, "unconfirmed") for s in group],
        )
        for (fact_id, dim_id, role), group in grouped.items()
    ]
    # Name-stable order: the conform pair refs and prompt order must not depend on
    # DB row order or uuids (the _fold_components discipline).
    exposures.sort(
        key=lambda e: (
            e.fact_table_name,
            name_of.get(e.dim_table_id, e.dim_table_id),
            e.fk_role,
            e.fact_table_id,
        )
    )
    return exposures


def _referenced_conform_pass(
    session: Session,
    exposures: list[_RefExposure],
    *,
    run_id: str,
    judge: DimensionIdentityJudge,
    stats: BusMatrixStats,
) -> None:
    """Resolve role identity over the referenced exposures (DAT-788).

    A union-find whose edges come from two sources, in strictly asymmetric
    directions so conformance is never invented (absence-falls-loud):

    - **Structural floor** — same ``(dim table, FK-role NAME)`` across facts is
      auto-conformed with NO LLM call. This is the DAT-756 common case
      (``account_id`` ⇄ ``account_id``); it must never regress or cost a judgment.
    - **Judge overlay** — differently-named cross-fact pairs sharing a dim table
      are submitted to the conform judge (the cross-fact framing the prompt is
      built for; no prompt/logic change). Only ``conform`` adds an edge;
      ``role`` / ``distinct`` / ``abstain`` / unjudged add nothing — the safe
      default keeps the roles separate. An ``abstain`` on a pair marks its
      exposures ``needs_confirmation`` unless the judge conformed them elsewhere.

    Each exposure's ``conformed_group`` is then the component's content-derived
    signature. A judge failure keeps the STRUCTURAL floor (same-name groups still
    form) and is recorded — the referenced identity degrades to name-only, never
    to nothing.
    """
    n = len(exposures)
    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    # Structural floor: same (dim, role name) across facts — deterministic order.
    by_dim_role: dict[tuple[str, str], list[int]] = {}
    for k, e in enumerate(exposures):
        by_dim_role.setdefault((e.dim_table_id, e.fk_role), []).append(k)
    for members in by_dim_role.values():
        for a, b in zip(members, members[1:], strict=False):
            union(a, b)

    # Judge overlay: differently-named cross-fact pairs sharing a dim table.
    # Exposures are name-sorted, so ``pair:{i}:{j}`` refs are deterministic.
    pairs = [
        (i, j)
        for i in range(n)
        for j in range(i + 1, n)
        if exposures[i].dim_table_id == exposures[j].dim_table_id
        and exposures[i].fact_table_id != exposures[j].fact_table_id
        and exposures[i].fk_role != exposures[j].fk_role
    ]
    stats.ref_conform_pairs = len(pairs)
    judge_conformed: set[int] = set()
    abstain_touched: set[int] = set()
    if pairs:
        fact_ids = sorted({e.fact_table_id for e in exposures})
        concepts = load_column_concepts(session, fact_ids, run_id)
        meaning_by_col: dict[tuple[str, str], str] = {}
        for col in session.execute(select(Column).where(Column.table_id.in_(fact_ids))).scalars():
            concept = concepts.get(col.column_id)
            if concept is not None and concept.meaning:
                meaning_by_col[(col.table_id, col.column_name)] = concept.meaning

        def side(e: _RefExposure) -> dict[str, object]:
            meaning = meaning_by_col.get((e.fact_table_id, e.fk_role))
            return {
                "fact_table": e.fact_table_name,
                "key": e.fk_role,
                "attributes": e.attributes,
                "meanings": {e.fk_role: meaning} if meaning else {},
            }

        by_ref = {f"pair:{i}:{j}": (i, j) for i, j in pairs}
        candidates = [
            {"ref": f"pair:{i}:{j}", "left": side(exposures[i]), "right": side(exposures[j])}
            for i, j in pairs
        ]
        verdicts: list[ConformVerdict] = []
        for start in range(0, len(candidates), _CONFORM_BATCH_MAX):
            chunk = candidates[start : start + _CONFORM_BATCH_MAX]
            try:
                result = judge.conform(candidates=chunk)
            except PermanentProviderError as exc:
                logger.warning("bus_matrix_ref_conform_skipped", reason=str(exc))
                stats.status = "failed"
                verdicts = []
                break
            if not result.success:
                logger.warning("bus_matrix_ref_conform_skipped", reason=result.error)
                stats.status = "failed"
                verdicts = []
                break
            verdicts.extend(result.unwrap())

        seen_refs: set[str] = set()
        for verdict in verdicts:
            pair = by_ref.get(verdict.pair_ref)
            if pair is None:
                logger.warning("bus_matrix_ref_conform_unknown_ref", ref=verdict.pair_ref)
                continue
            if verdict.pair_ref in seen_refs:
                continue
            seen_refs.add(verdict.pair_ref)
            i, j = pair
            logger.info(
                "bus_matrix_ref_conform",
                left=f"{exposures[i].fact_table_name}.{exposures[i].fk_role}",
                right=f"{exposures[j].fact_table_name}.{exposures[j].fk_role}",
                dim=exposures[i].dim_table_id,
                verdict=verdict.verdict,
                reason=verdict.reason,
            )
            if verdict.verdict == "conform":
                stats.ref_conformed += 1
                judge_conformed.update(pair)
                union(i, j)
            elif verdict.verdict == "abstain":
                stats.ref_abstained += 1
                abstain_touched.update(pair)
            # 'role' / 'distinct': separate axes — no edge (the judge answered).

        if verdicts:
            missing = sorted(set(by_ref) - seen_refs)
            if missing:
                stats.ref_unanswered = len(missing)
                logger.warning("bus_matrix_ref_conform_unanswered", refs=missing)

    # Assign each exposure its component's content-derived signature + abstain flag.
    comp_roles: dict[int, set[str]] = {}
    for k, e in enumerate(exposures):
        comp_roles.setdefault(find(k), set()).add(e.fk_role)
    for k, e in enumerate(exposures):
        e.conformed_group = _ref_group_signature(e.dim_table_id, comp_roles[find(k)])
    for k in abstain_touched:
        if k not in judge_conformed:
            exposures[k].needs_confirmation = True


def _referenced_cells(
    exposures: list[_RefExposure],
    name_of: dict[str, str],
    *,
    run_id: str,
    stats: BusMatrixStats,
) -> list[dict[str, object]]:
    """One cell per (fact, dim table, role identity) — DAT-788 role-separated grain.

    Role-playing FKs to one dim (bill-to vs ship-to) are DISTINCT cells unless
    the conform pass merged their roles into one ``conformed_group``. ``roles``
    holds this fact's FK roles in the group (one, unless a fact's own roles were
    transitively conformed); ``confirmation_source`` is the weakest FK-relationship
    floor across them (structural provenance, unchanged by the conform decision).
    """
    # (fact, dim, group signature) -> the fact's exposures in that identity.
    grouped: dict[tuple[str, str, str], list[_RefExposure]] = {}
    for e in exposures:
        grouped.setdefault((e.fact_table_id, e.dim_table_id, e.conformed_group or ""), []).append(e)

    out: list[dict[str, object]] = []
    for (fact_id, dim_id, group), members in sorted(grouped.items()):
        roles = sorted({e.fk_role for e in members})
        attributes = sorted({a for e in members for a in e.attributes})
        source = min(
            (s for e in members for s in e.sources),
            key=lambda v: _SOURCE_RANK.get(v, 0),
            default="unconfirmed",
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
                "conformed_group": group,
                "needs_confirmation": any(e.needs_confirmation for e in members),
                "signature": f"bus:referenced:{fact_id}:{dim_id}:" + "|".join(roles),
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
    referenced slice key. Undecided (``needs_confirmation``) and
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
            # propagate to the Temporal boundary, where the phase retry re-runs
            # the deterministic stats identically and re-asks the judge.
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
            # concept_label is non-empty here (ConformVerdict's validator).
            conform_edges.append((i, j, verdict.concept_label))
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
