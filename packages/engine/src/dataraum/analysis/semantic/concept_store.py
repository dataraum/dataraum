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

from dataraum.analysis.semantic.db_models import Concept, ConceptKind, WorkspaceSettings
from dataraum.analysis.semantic.ontology import (
    OntologyConcept,
    OntologyDefinition,
    OntologyLoader,
)
from dataraum.core.logging import get_logger
from dataraum.core.vertical import VerticalKind, require_known_vertical
from dataraum.storage.upsert import insert_if_absent

logger = get_logger(__name__)

_VALID_KINDS: frozenset[str] = frozenset(k.value for k in ConceptKind)


def _active_vertical(session: Session) -> str | None:
    """The workspace's bound active vertical, or ``None`` if none is bound yet.

    Reads the single ``workspace_settings`` row (the ``pin`` CHECK keeps it at most
    one). ``None`` = unbound: no non-placeholder vertical has run yet.
    """
    return session.execute(select(WorkspaceSettings.active_vertical)).scalar_one_or_none()


def require_active_vertical(session: Session, name: str) -> VerticalKind:
    """Bind-or-check the workspace's active vertical; fail LOUD on mismatch (DAT-848).

    The resolve-time gate that gives "this workspace's vertical" one enforced home
    (:class:`~dataraum.analysis.semantic.db_models.WorkspaceSettings`). A workspace's
    concept vocabulary is bound to ONE vertical; without this, a run launched with
    the wrong ``--vertical`` seeded a second vertical's rows and every reader served
    the union (permanent cross-vertical contamination).

    Semantics:

    - First resolve :func:`~dataraum.core.vertical.require_known_vertical` — an
      unknown (typo'd / never-framed) name still fails loud exactly as before.
    - A **placeholder** (``_adhoc`` / leading-underscore) declares no domain: it
      never binds and is never checked — an ad-hoc workspace stays unbound.
    - Otherwise **bind-if-unset**: the first non-placeholder vertical writes the
      single ``workspace_settings`` row (race-safe ``ON CONFLICT (pin) DO NOTHING``
      against a concurrent Temporal-retried first run — re-read picks the winner).
    - A subsequent non-placeholder vertical that DIFFERS from the bound one raises
      ``RuntimeError``. Changing the workspace's vertical is a deliberate, explicit
      operation (re-run after the change), not a per-run override — the same
      permanent-failure shape ``require_known_vertical`` already uses, so Temporal's
      retry policy handles it identically.

    Called at the two seams where a vertical first commits to concepts: add_source's
    ``semantic_per_column`` (the sole seeder — binds) and operating_model's
    ``resolve_operating_model_scope`` (checks). Returns the resolved
    :class:`~dataraum.core.vertical.VerticalKind`.

    Note the bind commits with the surrounding phase transaction (it shares the
    phase's session), which is correct — a failed phase rolls the bind back with the
    seed — but means a concurrent first run's conflicting ``INSERT`` blocks until the
    winner's whole phase commits. That is once-per-workspace-lifetime and Temporal
    retries recover it; it is not a hot path.
    """
    kind = require_known_vertical(name)
    if kind is VerticalKind.PLACEHOLDER:
        return kind
    current = _active_vertical(session)
    if current is None:
        insert_if_absent(
            session,
            WorkspaceSettings,
            [{"pin": True, "active_vertical": name}],
            index_elements=["pin"],
        )
        # Re-read: under a concurrent first run the winner's row is what binds, so a
        # loser whose vertical differs must still see the mismatch below.
        current = session.execute(select(WorkspaceSettings.active_vertical)).scalar_one()
    if current != name:
        raise RuntimeError(
            f"Workspace vertical mismatch: this workspace is bound to {current!r}, but the "
            f"run requested {name!r}. A workspace's concept vocabulary is bound to one vertical "
            f"(DAT-848); a wrong --vertical must not seed beside it. Re-run with "
            f"--vertical {current!r}, or deliberately re-vertical the workspace first."
        )
    return kind


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

    **Scoped to the workspace's bound active vertical (DAT-848).** The read filters
    on the ``workspace_settings.active_vertical`` binding, NOT blindly on the
    caller's ``vertical`` argument — so a reader on an UN-gated path (the
    begin_session catalogue turn, the Q&A context assembler) that is threaded a
    mismatched vertical still serves the workspace's real vocabulary, never a Concept
    row left under a wrong ``--vertical`` (or the eval's wild-vertical stand-in). The
    ``vertical`` argument is the fallback for an UNBOUND workspace (no binding yet —
    a placeholder ``_adhoc`` run, or a cold-start read): a bound vertical always
    wins over it. This mirrors the parameter-free view readers (``__READ__.concepts``
    → ``og_concepts`` + the cockpit mirror), which fall back to ``_adhoc``.
    """
    effective = _active_vertical(session) or vertical
    rows = list(
        session.execute(
            select(Concept)
            .where(Concept.vertical == effective, Concept.superseded_at.is_(None))
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
    yaml_def = OntologyLoader().load(effective)
    # model_construct: the convention↔concept lint is a YAML-AUTHORING check that
    # already ran when yaml_def loaded. Re-linting here would be wrong — the active
    # concept set is a legitimate SUBSET (a superseded concept a convention still
    # names is stale text, not an authoring error), and re-validation would crash
    # the runtime read the moment a referenced concept is superseded.
    return OntologyDefinition.model_construct(
        name=yaml_def.name if yaml_def else effective,
        version=yaml_def.version if yaml_def else "1.0.0",
        description=yaml_def.description if yaml_def else None,
        concepts=concepts,
        conventions=yaml_def.conventions if yaml_def else [],
    )


__all__ = ["ensure_concepts_seeded", "load_workspace_concepts", "require_active_vertical"]
