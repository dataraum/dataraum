"""The vertical's table-entity taxonomy — typed rows, config→DB (DAT-724).

The table-grain sibling of :mod:`~dataraum.analysis.semantic.concept_store`. The
shipped vertical's ``ontology.yaml`` ``entities:`` block is the *seed*, normalized
into typed :class:`~dataraum.analysis.semantic.db_models.VerticalEntity` rows once
per workspace; runtime consumers read the typed rows, so a *framed* vertical — whose
taxonomy would exist only as rows — is served identically to a builtin.

**Why this exists.** The finance vertical declared a rich column-concept vocabulary
and nothing at all about what a *table* can be. ``table_entities.detected_entity_type``
was therefore unconstrained LLM free text, ``is_fact_table`` was answered against no
declared alternative, and the eval's roles oracle could only grade entity_type
report-only because there was nothing to ground against. These rows are the missing
vocabulary; :func:`load_workspace_entities` lifts them back onto the
:class:`~dataraum.analysis.semantic.ontology.OntologyDefinition` both table-grain
agents already read.

**The taxonomy PROPOSES, never suppresses.** It is served as prompt evidence and
nothing more — a table matching no declared entity keeps free-text detection, and
``derive_table_role`` keeps sole ownership of the structural ``periodic_snapshot``
refinement. No declared value is ever written straight through to a detection.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from dataraum.analysis.cycles.db_models import CycleType
from dataraum.analysis.semantic.db_models import TableRole, VerticalEntity
from dataraum.analysis.semantic.ontology import OntologyEntity, OntologyLoader
from dataraum.core.logging import get_logger
from dataraum.storage.upsert import insert_if_absent

logger = get_logger(__name__)

_VALID_ROLES: frozenset[str] = frozenset(r.value for r in TableRole)


def _declared_cycle_names(session: Session, vertical: str) -> set[str]:
    """The vertical's active cycle-type names — the vocabulary ``cycles`` resolves against.

    Reads the typed ``cycle_types`` home (DAT-881) rather than re-reading
    ``cycles.yaml``, so a taught cycle that reached the table is equally referenceable
    and there is one vocabulary, not two.
    """
    return set(
        session.execute(
            select(CycleType.name).where(
                CycleType.vertical == vertical, CycleType.superseded_at.is_(None)
            )
        )
        .scalars()
        .all()
    )


def ensure_entities_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's entity taxonomy as typed rows.

    ``INSERT … ON CONFLICT DO NOTHING`` on the active-row partial-unique index — the
    :func:`~dataraum.analysis.semantic.concept_store.ensure_concepts_seeded` pattern:
    a re-run is a no-op and it is race-safe against a concurrent seed. Returns the
    number of rows actually inserted (conflicts skipped).

    Two born-loud checks, both "the config is wrong, not the data":

    - **role** must be present and a valid :class:`TableRole` — an entity that declares
      no role declares nothing usable, and a role outside the vocabulary would be
      rejected by the CHECK anyway, far later and less legibly.
    - **cycles** must resolve against the typed ``cycle_types`` vocabulary. This
      requires :func:`~dataraum.analysis.cycles.cycle_type_store.ensure_cycle_types_seeded`
      to have run FIRST in the same transaction; the phase orders them that way and the
      error names the ordering so a mis-ordered caller is diagnosable rather than
      mysterious.

    Concept references are NOT re-checked here — ``OntologyDefinition``'s parse-time
    validator already linted them against the declared vocabulary, which is where an
    authoring error belongs.

    A framed vertical (no on-disk YAML) seeds nothing — it has no ``entities:`` block.
    """
    definition = OntologyLoader().load(vertical)
    if definition is None or not definition.entities:
        return 0

    known_cycles: set[str] | None = None
    rows: list[dict[str, Any]] = []
    for e in definition.entities:
        if not e.role or e.role not in _VALID_ROLES:
            raise ValueError(
                f"entity '{e.name}' in vertical '{vertical}' declares no valid role "
                f"(got {e.role!r}); one of {sorted(_VALID_ROLES)} is required to seed."
            )
        if e.cycles:
            # Resolved lazily: a taxonomy that names no cycle must not require the
            # cycle vocabulary to exist at all.
            if known_cycles is None:
                known_cycles = _declared_cycle_names(session, vertical)
            for cycle in e.cycles:
                if cycle not in known_cycles:
                    raise ValueError(
                        f"entity '{e.name}' in vertical '{vertical}' references cycle "
                        f"{cycle!r}, which is not a declared cycle type (known: "
                        f"{sorted(known_cycles)}). Declare it in the vertical's "
                        f"cycles.yaml, or seed the cycle types before the entities."
                    )
        rows.append(
            {
                "vertical": vertical,
                "name": e.name,
                "role": e.role,
                "description": e.description,
                "concepts": e.concepts or None,
                "cycles": e.cycles or None,
                "aliases": e.aliases or None,
                "source": "seed",
            }
        )

    seeded = insert_if_absent(
        session,
        VerticalEntity,
        rows,
        index_elements=["vertical", "name"],
        index_where=text("superseded_at IS NULL"),
    )
    if seeded:
        logger.info("vertical_entities_seeded", vertical=vertical, count=seeded)
    return seeded


def load_workspace_entities(session: Session, vertical: str) -> list[OntologyEntity]:
    """The workspace's declared entity taxonomy, as ``OntologyEntity`` objects.

    The reverse lift, mirroring
    :func:`~dataraum.analysis.semantic.concept_store.load_workspace_concepts`'s
    concept lift: typed rows back into the parse-shaped model the prompt formatter
    accepts. ``vertical`` is already the caller's resolved *effective* vertical (the
    ``workspace_settings`` binding wins there, DAT-848) — this reader does not
    re-resolve it, so the scoping decision keeps one home.

    Ordered by name for a stable prompt rendering: an unordered read reshuffles the
    served evidence between processes, the same determinism trap the graph-topology
    roles hit (DAT-725).
    """
    rows = list(
        session.execute(
            select(VerticalEntity)
            .where(VerticalEntity.vertical == vertical, VerticalEntity.superseded_at.is_(None))
            .order_by(VerticalEntity.name)
        ).scalars()
    )
    return [
        OntologyEntity(
            name=r.name,
            role=r.role,
            description=r.description,
            concepts=list(r.concepts or []),
            cycles=list(r.cycles or []),
            aliases=list(r.aliases or []),
        )
        for r in rows
    ]


__all__ = ["ensure_entities_seeded", "load_workspace_entities"]
