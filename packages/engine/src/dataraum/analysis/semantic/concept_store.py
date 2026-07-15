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

from typing import Any

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import Concept, ConceptKind
from dataraum.analysis.semantic.ontology import (
    OntologyConcept,
    OntologyDefinition,
    OntologyLoader,
)
from dataraum.core.logging import get_logger
from dataraum.storage.upsert import insert_if_absent

logger = get_logger(__name__)

_VALID_KINDS: frozenset[str] = frozenset(k.value for k in ConceptKind)


def ensure_concepts_seeded(session: Session, vertical: str) -> int:
    """Idempotently seed the shipped vertical's concepts as typed rows.

    Reads the vertical's YAML definition (the seed source) and inserts a typed
    :class:`Concept` row for every concept with no active row yet, via
    ``INSERT … ON CONFLICT DO NOTHING`` on the active-row partial-unique index —
    so a re-run is a no-op, a ``frame`` edit (which supersedes) is never clobbered,
    AND it is race-safe: a concurrent seed (a Temporal at-least-once retry landing
    beside its still-running original) or a concurrent ``frame`` write can no longer
    collide on the index (the old read-then-insert had a TOCTOU window). Born-loud
    when a seeded concept declares no valid ``kind``: the config is wrong, not the
    data. Returns the number of rows actually inserted (conflicts skipped).

    A framed vertical (no on-disk YAML) seeds nothing here — its concepts arrive
    through ``frame``'s typed writes, not the shipped seed.
    """
    definition = OntologyLoader().load(vertical)
    if definition is None:
        return 0
    rows: list[dict[str, Any]] = []
    for c in definition.concepts:
        if not c.kind or c.kind not in _VALID_KINDS:
            raise ValueError(
                f"concept '{c.name}' in vertical '{vertical}' declares no valid kind "
                f"(got {c.kind!r}); one of {sorted(_VALID_KINDS)} is required to seed."
            )
        rows.append(
            {
                "vertical": vertical,
                "name": c.name,
                "kind": c.kind,
                "description": c.description,
                "indicators": c.indicators or None,
                "exclude_patterns": c.exclude_patterns or None,
                "unit_from_concept": c.unit_from_concept,
                "source": "seed",
            }
        )
    if not rows:
        return 0
    seeded = insert_if_absent(
        session,
        Concept,
        rows,
        index_elements=["vertical", "name"],
        index_where=text("superseded_at IS NULL"),
    )
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
            unit_from_concept=r.unit_from_concept,
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
