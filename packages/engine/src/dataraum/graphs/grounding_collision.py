"""Cross-concept grounding collision guard (DAT-709).

Grounding is ONE LLM call per concept (``GraphAgent._generate_sql``): nothing in
that call — or in persistence behind it — ever sees two concepts side by side.
So two concepts could ground to the SAME extract and every existing check waved
it through:

* the snippet semantic key INCLUDES ``standard_field``, so byte-identical
  statements for two concepts are two distinct, perfectly legal rows;
* ``normalized_expression`` matching is formula-only (it canonicalizes arithmetic
  over step ids, and would be garbage on a SELECT);
* the within-output uniqueness validator (``GraphProvenanceOutput``) only stops
  ONE concept appearing twice in ONE grounding's basis;
* disjointness was prompt text — an instruction, never an enforcement.

Two DISJOINT concepts grounded to the same extract is not a style issue: their
value-sets must not overlap, and identical parts select identical rows, which is
that overlap at its maximum. At most one of the two can be right and nothing in
the run can tell which — while every ratio between them silently computes 1.0.

The guard therefore runs where — and only where — every concept's grounding for
the run coexists: the end of the authoring pass (``metrics_phase._warm_shared_
nodes``), over the PERSISTED snippets, so it covers nodes freshly authored this
run and nodes assembled from a prior run's cache alike.

**It never picks a winner.** Deciding which concept keeps the extract is exactly
the LLM's judgment, and a deterministic re-pick here would hardcode a clean
schema shape real datasets violate. So the guard does three things and no more:

1. **Detects** — canonical-SQL equality (``core.sql_normalize``, DuckDB's own
   parse tree: whitespace/casing collapse, identifiers and clause order do not)
   between concepts carrying a ``disjoint_with`` edge. Scoped to disjoint pairs
   deliberately: two concepts in a ``part_of`` relation CAN legitimately share an
   extract (a whole with exactly one part), so an identity check over all
   concepts would fire on correct groundings.
2. **Re-grounds, symmetrically** — EVERY member of a collision is demoted to a
   retained failure and re-authored; the demotion is both what unblocks the LLM
   (a healthy snippet would be assembled from cache, no call) and what serves the
   disambiguation: the failed row's ``{failure_mode, failure_reason}`` names the
   partner concept and reaches the retry through its own ``prior_context`` (that
   exact prior SQL, "do NOT re-emit unchanged"). ONE round, mirroring the
   contract-repair turn: a model that cannot distinguish two concepts twice will
   not on a third pass.

   Members re-author SEQUENTIALLY, so what each one sees of its partner differs
   and that is deliberate: the FIRST still sees the partner's healthy
   ``grounded by:`` line in the served concept graph — the colliding statement,
   attributed, which is the sharper signal — while the second, whose partner has
   since been re-grounded or left flagged, sees whatever that produced. Both
   always carry the naming reason in their own prior_context, so neither depends
   on the concept-graph half.
3. **Abstains, typed** — anything still colliding is left flagged
   ``DISJOINT_COLLISION`` with its SQL retained, and its ``NodeDecision`` goes
   ungrounded so every metric built on it honest-fails born-loud. An abstained
   ratio is correct; a confident 1.0 is the failure we are preventing.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import select

from dataraum.core.logging import get_logger
from dataraum.core.sql_normalize import canonical_sql, canonical_sql_or_none
from dataraum.graphs.models import FailedSnippetProvenance, SnippetFailureMode
from dataraum.graphs.node_warming import NodeDecision

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.graphs.node_warming import NodeKey, WarmNode

logger = get_logger(__name__)


@dataclass(frozen=True)
class GroundedExtract:
    """One concept's persisted extract, as the guard compares it."""

    key: NodeKey
    concept: str
    sql: str


@dataclass(frozen=True)
class CollisionGroup:
    """Two or more disjoint concepts whose extracts canonicalize identically.

    ``members`` is sorted for determinism and holds only the concepts actually
    IMPLICATED — a third concept sharing the statement without a disjoint edge to
    any of them is legitimate and stays out of the group (and untouched).
    """

    members: tuple[GroundedExtract, ...]

    def partners_of(self, member: GroundedExtract) -> tuple[str, ...]:
        """The other concepts in this collision, sorted."""
        return tuple(sorted({m.concept for m in self.members if m.concept != member.concept}))


def disjoint_map(session: Session, vertical: str) -> dict[str, frozenset[str]]:
    """Active ``disjoint_with`` edges for ``vertical`` as ``concept → partners``.

    Read from the typed ``concept_edges`` table directly rather than through the
    property graph (``graphs.context_reads._read_concept_edges``): this needs one
    predicate, not the whole neighbourhood, and PGQ is Postgres-19-only while the
    guard must also run wherever the phase does. Symmetric edges are stored in
    BOTH directions, so accumulating each row under its from-side already
    populates both endpoints.
    """
    from dataraum.analysis.semantic.db_models import ConceptEdge, ConceptEdgePredicate

    rows = session.execute(
        select(ConceptEdge.from_concept, ConceptEdge.to_concept).where(
            ConceptEdge.vertical == vertical,
            ConceptEdge.predicate == ConceptEdgePredicate.DISJOINT_WITH.value,
            ConceptEdge.superseded_at.is_(None),
        )
    ).all()
    partners: dict[str, set[str]] = defaultdict(set)
    for from_concept, to_concept in rows:
        partners[from_concept].add(to_concept)
        partners[to_concept].add(from_concept)
    return {concept: frozenset(names) for concept, names in partners.items()}


def find_collisions(
    extracts: Iterable[GroundedExtract],
    disjoint: Mapping[str, frozenset[str]],
) -> list[CollisionGroup]:
    """Group extracts that canonicalize identically across DISJOINT concepts.

    Pure: no session, no LLM. Comparison is ``canonical_sql`` equality, so the
    same statement re-rendered with different whitespace or keyword casing still
    collides, while a different column, value list, or clause order does not.
    """
    buckets: dict[str, list[GroundedExtract]] = defaultdict(list)
    for extract in extracts:
        if not extract.sql:
            continue
        canonical = canonical_sql_or_none(extract.sql)
        if canonical is None:
            # DuckDB could not parse it, so this concept is compared byte-for-byte
            # from here: syntax-noise variants of the same statement stop
            # colliding. Not an error (the guard degrades to something weaker, not
            # to something wrong) but it is the reason a "missed" collision would
            # ever be missed, so leave a trail.
            logger.debug("grounding_collision_sql_unparsed", concept=extract.concept)
            canonical = canonical_sql(extract.sql)
        buckets[canonical].append(extract)

    groups: list[CollisionGroup] = []
    for _, members in sorted(buckets.items()):
        if len(members) < 2:
            continue
        implicated = [
            member
            for member in members
            if any(
                other.concept != member.concept
                and other.concept in disjoint.get(member.concept, frozenset())
                for other in members
            )
        ]
        if len(implicated) < 2:
            continue
        # NodeKey holds Nones, which do not order against str — sort on its repr.
        groups.append(
            CollisionGroup(members=tuple(sorted(implicated, key=lambda m: (m.concept, str(m.key)))))
        )
    return groups


def collision_reason(group: CollisionGroup, member: GroundedExtract, *, abstained: bool) -> str:
    """The retained-failure reason — what the re-grounding (or the user) reads.

    Names the disjoint partner(s) explicitly: the retry's job is to find the
    evidence that distinguishes THIS concept from THOSE, so a reason that only
    said "ambiguous" would send it back with nothing to work from.
    """
    partners = ", ".join(group.partners_of(member))
    collided = (
        f"'{member.concept}' grounded to the same extract as disjoint concept(s) {partners} — "
        "disjoint concepts cannot select the same rows, so at most one of these groundings "
        "can be right"
    )
    if abstained:
        # Deliberately says the COLLISION survived, not that this member was
        # re-grounded: the second round can pull in a concept the first never
        # touched (a repaired grounding landing on a third concept's statement).
        return (
            f"{collided}. The collision survived the re-grounding round, so the concept is "
            "left ungrounded rather than shipping a value indistinguishable from another "
            "concept's"
        )
    return collided


def resolve_grounding_collisions(
    bindings: dict[NodeKey, NodeDecision],
    nodes: Mapping[NodeKey, WarmNode],
    *,
    session: Session,
    workspace_id: str,
    schema_mapping_id: str,
    vertical: str,
    reauthor: Callable[[NodeKey, str], NodeDecision],
) -> dict[NodeKey, NodeDecision]:
    """Detect → re-ground once → abstain typed. Returns the updated binding map.

    ``reauthor(key, reason)`` must demote the node's snippet with ``reason`` and
    re-author it IN ONE unit of work — the phase owns that plumbing (an isolated
    session per node on the parallel path, the shared one on the serial path),
    and the demotion has to be visible to the re-authoring's own snippet lookup
    or the re-ground silently assembles the collided SQL from cache.

    Args:
        bindings: The authoring pass's run-scoped binding map (mutated + returned).
        nodes: The warm DAG's nodes, keyed as ``bindings`` is.
        session: The phase session — reads the persisted extracts, writes the
            final abstentions.
        workspace_id: Workspace id for the snippet library write path.
        schema_mapping_id: Schema mapping the snippets are keyed under.
        vertical: The active vertical, whose concept edges define disjointness.
        reauthor: Demote-and-re-author one node, returning its new decision.

    Returns:
        The binding map with every unresolved collision's node marked ungrounded.
    """
    disjoint = disjoint_map(session, vertical)
    if not disjoint:
        # No declared partition = no disjointness to enforce. Loud enough to find
        # if a vertical ships without concept_groups and expects this guard.
        logger.info("grounding_collision_no_disjointness", vertical=vertical)
        return bindings

    # FLUSH before the read. The authoring pass writes through ``save_snippet``,
    # whose heal branch mutates an existing row IN PLACE without flushing, and the
    # production session is ``autoflush=False`` (``core.connections``) — so on a
    # re-run where both concepts healed to the same statement, the pending SQL
    # never reaches the SELECT below and the guard compares the PREVIOUS run's
    # values. It sees no collision and calls nothing: blind, silently.
    session.flush()
    groups = find_collisions(
        _persisted_extracts(bindings, nodes, session, workspace_id, schema_mapping_id), disjoint
    )
    if not groups:
        return bindings

    for group in groups:
        for member in group.members:
            reason = collision_reason(group, member, abstained=False)
            logger.warning(
                "grounding_collision_detected",
                concept=member.concept,
                partners=group.partners_of(member),
            )
            try:
                bindings[member.key] = reauthor(member.key, reason)
            except Exception as exc:
                # Re-authoring calls the LLM and the DB, so it can raise where the
                # rest of this pass cannot. Both warm-pass drivers isolate their
                # nodes the same way, and for the same reason: an escape here
                # aborts the phase, discards every lifecycle write the run made,
                # and — on a deterministic error — makes the Temporal retry loop
                # on it forever.
                #
                # Abstain HERE rather than leaving it to round 2. An isolated-session
                # re-author rolls its own demotion back when it raises, so the
                # colliding row is healthy again — and round 2 reads only GROUNDED
                # nodes, which this one no longer is. Deferring would drop the pair
                # out of the check entirely and let two identical healthy snippets
                # persist. A transient failure costs one flagged row that the next
                # run re-authors from scratch, with this reason in its prior_context.
                failed_reason = f"{reason} (re-ground failed: {exc})"
                logger.warning(
                    "grounding_collision_reground_error",
                    concept=member.concept,
                    error=str(exc),
                )
                flag_collision(
                    session,
                    nodes[member.key],
                    workspace_id=workspace_id,
                    schema_mapping_id=schema_mapping_id,
                    reason=failed_reason,
                )
                bindings[member.key] = NodeDecision(grounded=False, reason=failed_reason)

    # Re-detect over the FULL set, not just the repaired groups: a re-grounding is
    # free to move onto a statement a third concept already holds.
    #
    # FLUSH before EXPIRE, in that order, and both are load-bearing:
    # * flush — ``save_snippet``'s heal path mutates in place without flushing, and
    #   ``expire_all`` DISCARDS pending changes on the instances it expires, so
    #   expiring first would roll every successful re-grounding back to its demoted
    #   state and make each repaired node look unresolved. The flush also persists
    #   the phase's own in-flight work (the metric ``LifecycleArtifact`` rows
    #   declared before warming) before we drop it from the identity map.
    # * expire — on the parallel path each re-authoring ran and COMMITTED in a
    #   different session, so this one's identity map holds pre-repair copies.
    # ``expire_all`` is the blunt instrument on purpose: the surgical alternative
    # (``populate_existing`` on the snippet read) would put a SQLAlchemy loader
    # option into ``SnippetLibrary``'s signature for this one caller, and it would
    # refresh ONLY snippets — while the isolated sessions also committed lifecycle
    # transitions this session may later read. The cost is re-selecting what the
    # phase touches next, once, and only on a run that actually collided.
    session.flush()
    session.expire_all()
    for group in find_collisions(
        _persisted_extracts(bindings, nodes, session, workspace_id, schema_mapping_id), disjoint
    ):
        for member in group.members:
            reason = collision_reason(group, member, abstained=True)
            flag_collision(
                session,
                nodes[member.key],
                workspace_id=workspace_id,
                schema_mapping_id=schema_mapping_id,
                reason=reason,
            )
            bindings[member.key] = NodeDecision(grounded=False, reason=reason)
            logger.warning(
                "grounding_collision_abstained",
                concept=member.concept,
                partners=group.partners_of(member),
            )
    return bindings


def _persisted_extracts(
    bindings: Mapping[NodeKey, NodeDecision],
    nodes: Mapping[NodeKey, WarmNode],
    session: Session,
    workspace_id: str,
    schema_mapping_id: str,
) -> list[GroundedExtract]:
    """The healthy snippet SQL behind every grounded node, as the guard sees it.

    Reads what actually PERSISTED rather than what the LLM returned: a node
    assembled from a prior run's cache never returned anything this run, and it
    can collide just as well as a freshly authored one.
    """
    from dataraum.query.snippet_library import SnippetLibrary

    library = SnippetLibrary(session, workspace_id=workspace_id)
    extracts: list[GroundedExtract] = []
    for key, decision in bindings.items():
        node = nodes.get(key)
        if not decision.grounded or node is None or node.step.source is None:
            continue
        # No standard_field = not a concept grounding, so no disjointness to
        # violate — the guard is defined over the vertical's concept vocabulary.
        concept = node.step.source.standard_field
        if concept is None:
            continue
        # The DECLARED predicate is part of the key (DAT-838). Without it a
        # restricted extract resolved to its unrestricted sibling's SQL, so the
        # guard compared the wrong statement — and a restricted node whose sibling
        # did not exist resolved to nothing at all, dropping it out of the
        # comparison set entirely and letting a real collision pass VACUOUSLY.
        match = library.find_by_key(
            snippet_type="extract",
            schema_mapping_id=schema_mapping_id,
            standard_field=concept,
            statement=node.step.source.statement,
            aggregation=node.step.aggregation,
            predicate=node.step.source.predicate,
        )
        if match is None:
            continue
        extracts.append(GroundedExtract(key=key, concept=concept, sql=match.snippet.sql))
    return extracts


def flag_collision(
    session: Session,
    node: WarmNode,
    *,
    workspace_id: str,
    schema_mapping_id: str,
    reason: str,
) -> None:
    """Flag the node's snippet ``DISJOINT_COLLISION`` — retained, not reusable.

    Both collision writes go through here — the demotion that precedes a
    re-grounding and the final abstention — so the mode, the retained SQL, and
    the feedback the next authoring reads are minted in exactly one place. The
    two differ only in their ``reason`` (see :func:`collision_reason`).

    The DECLARED predicate is part of the key (DAT-838), and this is the site
    where omitting it did the most damage: a WRITE. A restricted node's demotion
    resolved to its unrestricted sibling and flagged THAT row
    ``DISJOINT_COLLISION`` — a false verdict recorded against an innocent snippet,
    excluding a healthy grounding from all future reuse, while the row that
    actually collided stayed healthy and kept being served.
    """
    from dataraum.query.snippet_library import SnippetLibrary

    if node.step.source is None:
        return
    SnippetLibrary(session, workspace_id=workspace_id).demote_to_failure(
        snippet_type="extract",
        schema_mapping_id=schema_mapping_id,
        standard_field=node.step.source.standard_field,
        statement=node.step.source.statement,
        aggregation=node.step.aggregation,
        predicate=node.step.source.predicate,
        provenance=FailedSnippetProvenance(
            failure_mode=SnippetFailureMode.DISJOINT_COLLISION, failure_reason=reason
        ).model_dump(mode="json"),
    )


__all__ = [
    "CollisionGroup",
    "GroundedExtract",
    "collision_reason",
    "disjoint_map",
    "find_collisions",
    "flag_collision",
    "resolve_grounding_collisions",
]
