"""The workspace concept vocabulary — typed rows, config→DB (DAT-728).

The runtime home for a workspace's concepts. The shipped vertical YAML is the
*seed*, normalized into ``concepts`` rows once per workspace; runtime consumers
read the typed rows (never the YAML) so a *framed* vertical — whose concepts exist
only as rows — is served identically to a builtin. Conventions remain YAML
(:class:`~dataraum.analysis.semantic.ontology.OntologyLoader`); they are not
concepts and are out of config→DB's scope in this phase.

The seam replacing the retired ``config_overlay(type='concept')`` merge: the
engine no longer reads concept overlay rows — it reads this table, which the seed
(builtin YAML) and ``frame`` (declared/edited, via the cockpit's Drizzle mirror)
both write.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import Concept, ConceptKind
from dataraum.analysis.semantic.ontology import (
    OntologyConcept,
    OntologyDefinition,
    OntologyLoader,
)
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

_VALID_KINDS: frozenset[str] = frozenset(k.value for k in ConceptKind)


def ensure_concepts_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's concepts as typed rows.

    Reads the vertical's YAML definition (the seed source) and inserts a typed
    :class:`Concept` row for every concept that has no active row yet — so a
    re-run is a no-op and a ``frame`` edit (which supersedes) is never clobbered.
    Born-loud when a seeded concept declares no valid ``kind``: the config is
    wrong, not the data. Returns the number of rows seeded.

    A framed vertical (no on-disk YAML) seeds nothing here — its concepts arrive
    through ``frame``'s typed writes, not the shipped seed.
    """
    definition = OntologyLoader().load(vertical)
    if definition is None:
        return 0
    existing = {
        name
        for (name,) in session.execute(
            select(Concept.name).where(
                Concept.vertical == vertical, Concept.superseded_at.is_(None)
            )
        )
    }
    seeded = 0
    for c in definition.concepts:
        if c.name in existing:
            continue
        if not c.kind or c.kind not in _VALID_KINDS:
            raise ValueError(
                f"concept '{c.name}' in vertical '{vertical}' declares no valid kind "
                f"(got {c.kind!r}); one of {sorted(_VALID_KINDS)} is required to seed."
            )
        session.add(
            Concept(
                vertical=vertical,
                name=c.name,
                kind=c.kind,
                description=c.description,
                indicators=c.indicators or None,
                exclude_patterns=c.exclude_patterns or None,
                typical_role=c.typical_role,
                typical_values=c.typical_values or None,
                unit_from_concept=c.unit_from_concept,
                is_unit_dimension=c.is_unit_dimension,
                source="seed",
            )
        )
        seeded += 1
    session.flush()
    if seeded:
        logger.info("concepts_seeded", vertical=vertical, count=seeded)
    return seeded


def load_workspace_concepts(session: Session, vertical: str) -> OntologyDefinition:
    """The workspace's concept vocabulary as an ``OntologyDefinition``.

    Concepts come from the typed ``concepts`` table (the config→DB home); the
    vertical's conventions come from YAML (conventions are not config→DB in this
    phase). The returned definition is the same shape existing prompt/context
    consumers already accept — only the concept SOURCE moved off the YAML/overlay
    merge onto the typed table.
    """
    rows = list(
        session.execute(
            select(Concept)
            .where(Concept.vertical == vertical, Concept.superseded_at.is_(None))
            .order_by(Concept.name)
        ).scalars()
    )
    concepts = [
        OntologyConcept(
            name=r.name,
            kind=r.kind,
            description=r.description,
            indicators=list(r.indicators or []),
            exclude_patterns=list(r.exclude_patterns or []),
            typical_role=r.typical_role,
            typical_values=list(r.typical_values or []),
            unit_from_concept=r.unit_from_concept,
            is_unit_dimension=r.is_unit_dimension,
        )
        for r in rows
    ]
    yaml_def = OntologyLoader().load(vertical)
    # model_construct: the convention↔concept lint is a YAML-AUTHORING check that
    # already ran when yaml_def loaded. Re-linting here would be wrong — the active
    # concept set is a legitimate SUBSET (a superseded concept a convention still
    # names is stale text, not an authoring error), and re-validation would crash
    # the runtime read the moment a referenced concept is superseded.
    return OntologyDefinition.model_construct(
        name=yaml_def.name if yaml_def else vertical,
        version=yaml_def.version if yaml_def else "1.0.0",
        description=yaml_def.description if yaml_def else None,
        concepts=concepts,
        conventions=yaml_def.conventions if yaml_def else [],
    )


__all__ = ["ensure_concepts_seeded", "load_workspace_concepts"]
