"""Concept edges — the operating-model graph's vocabulary relations (DAT-729).

The typed home for ``part_of`` / ``disjoint_with`` / ``reconciles_with`` concept
edges, seeded the same way concepts were (config→DB, DAT-728): the shipped vertical
YAML is the seed, the runtime reads the typed ``concept_edges`` table, the property
graph binds it as the ``concept_edge`` edge over the ``og_concepts`` vertex. Later
phases derive more edges (``reconciles_with`` from the aggregation-lineage witness)
and ``frame`` authors them for novel datasets (P13); this module owns the SEED
authoring from the vertical's own declarations.

``disjoint_with`` is read off convention ``concept_groups``: a convention partitions
concepts into named, mutually-exclusive sets (finance's ``sign_natural_balance`` splits
measures credit- vs debit-normal), and two concepts in DIFFERENT groups of one
convention cannot co-classify — an account is an asset xor a liability. That is exactly
``disjoint_with``.

``part_of`` is read off the vertical's ``compositions`` (DAT-729): each ``whole ←
parts`` declaration promotes ``part → whole`` mereological edges (``cash`` part_of
``current_assets``) — concept-grain composition, distinct from the account-instance
chart-of-accounts tree (that is the physical ``references`` topology). Both read off
declarations the engine already parses + lint-validates
(:class:`~dataraum.analysis.semantic.ontology.OntologyConvention` /
:class:`~dataraum.analysis.semantic.ontology.OntologyComposition`) — no new authoring
surface, the config→DB lever again.
"""

from __future__ import annotations

from itertools import combinations
from typing import Any

from sqlalchemy import text
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import ConceptEdge, ConceptEdgePredicate
from dataraum.analysis.semantic.ontology import OntologyDefinition, OntologyLoader
from dataraum.core.logging import get_logger
from dataraum.storage.upsert import insert_if_absent

logger = get_logger(__name__)

_EDGE_INDEX_ELEMENTS = ["vertical", "predicate", "from_concept", "to_concept"]


def ensure_concept_edges_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's concept edges as typed rows.

    Reads the vertical's YAML (the seed source) and inserts a :class:`ConceptEdge`
    row for every declared edge with no active row yet, via ``INSERT … ON CONFLICT
    DO NOTHING`` on the active-row partial-unique index — so a re-run is a no-op, a
    ``frame`` edit (which supersedes) is never clobbered, and a concurrent seed / write
    can no longer collide on the index (the same race-safety as
    :func:`~dataraum.analysis.semantic.concept_store.ensure_concepts_seeded`).

    A framed vertical (no on-disk YAML) seeds nothing here — its edges arrive through
    ``frame``'s typed writes, not the shipped seed. Returns the number of rows actually
    inserted (conflicts skipped).
    """
    definition = OntologyLoader().load(vertical)
    if definition is None:
        return 0
    rows = _disjoint_with_rows(vertical, definition) + _part_of_rows(vertical, definition)
    if not rows:
        return 0
    seeded = insert_if_absent(
        session,
        ConceptEdge,
        rows,
        index_elements=_EDGE_INDEX_ELEMENTS,
        index_where=text("superseded_at IS NULL"),
    )
    if seeded:
        logger.info("concept_edges_seeded", vertical=vertical, count=seeded)
    return seeded


def _disjoint_with_rows(vertical: str, definition: OntologyDefinition) -> list[dict[str, Any]]:
    """``disjoint_with`` edge rows from every convention's ``concept_groups`` partition.

    Two concepts in DIFFERENT groups of ONE convention cannot co-classify (groups are
    validated mutually-exclusive at load), so they are ``disjoint_with``. Emitted in
    BOTH directions — the predicate is symmetric, the property graph is directed — and
    de-duplicated across conventions, since the same pair can fall out of two
    partitions (finance's asset/liability split shows up in both the sign convention
    and the balance-sheet-composition convention).
    """
    seen: set[tuple[str, str]] = set()
    rows: list[dict[str, Any]] = []
    for conv in definition.conventions:
        groups = [members for members in conv.concept_groups.values() if members]
        for group_a, group_b in combinations(groups, 2):
            for a in group_a:
                for b in group_b:
                    for src, dst in ((a, b), (b, a)):
                        if (src, dst) in seen:
                            continue
                        seen.add((src, dst))
                        rows.append(
                            {
                                "vertical": vertical,
                                "predicate": ConceptEdgePredicate.DISJOINT_WITH.value,
                                "from_concept": src,
                                "to_concept": dst,
                                "source": "seed",
                            }
                        )
    return rows


def _part_of_rows(vertical: str, definition: OntologyDefinition) -> list[dict[str, Any]]:
    """``part_of`` edge rows from the vertical's concept ``compositions``.

    Each declared composition (``whole`` ← ``parts``) promotes one directed
    ``part → whole`` edge per part — mereological containment at the concept grain
    (``cash`` part_of ``current_assets``). Directed and single-row (unlike the
    symmetric ``disjoint_with``): the transitive ancestry is walked by the bounded
    recursive CTE, never materialized. Endpoints are declared concepts (lint-checked
    at load), so the ``og_concept_edges`` JOIN always resolves them.
    """
    return [
        {
            "vertical": vertical,
            "predicate": ConceptEdgePredicate.PART_OF.value,
            "from_concept": part,
            "to_concept": comp.whole,
            "source": "seed",
        }
        for comp in definition.compositions
        for part in comp.parts
    ]


__all__ = ["ensure_concept_edges_seeded"]
