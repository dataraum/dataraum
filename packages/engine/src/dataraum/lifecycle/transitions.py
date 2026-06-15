"""Typed lifecycle transitions + stage authorization (DAT-438).

Operations are typed by state transition (``architecture-future.md``): each
teach type exposes operations that move artifacts through
``declared`` → ``grounded`` → ``executed`` → ``canonical``, and each operation
is restricted to specific journey stages — the Goodhart firewall enforced at
the operation level. The maps below register three families —
``validation`` (DAT-438), ``cycle`` (DAT-455), and ``metric`` (DAT-456).
Validation and cycles ground via ``bind``; metrics ground via ``compose``
(the architecture-future name for a metric's declared → grounded move — its
graph's inputs resolve to real columns/concepts of the workspace). Both are
the same ``declared`` → ``grounded`` transition; the distinct verb keeps the
audit trail faithful to the operation vocabulary.

Authorization notes:

* ``<type>.declare`` is authorized for ``operating_model`` as the bootstrap:
  the engine materializes the *vertical's* declared set (validation specs /
  cycle vocabulary) as ``declared`` artifacts — the declare authority is the
  vertical, the stage just records it. frame-2 (DAT-441/DAT-457) takes over
  user declares.
* ``<type>.endorse`` (``executed`` → ``canonical``) is **defined with no
  authorized stage**: the transition exists in the state machine, but no
  endorsement workflow exists yet, so every caller is rejected.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final

from sqlalchemy import select

from dataraum.core.logging import get_logger

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from dataraum.lifecycle.db_models import LifecycleArtifact

logger = get_logger(__name__)


class ArtifactState(StrEnum):
    """Lifecycle states — three system states + one organizational state."""

    DECLARED = "declared"
    GROUNDED = "grounded"
    EXECUTED = "executed"
    CANONICAL = "canonical"


class LifecycleError(Exception):
    """Base for lifecycle contract violations."""


class StageNotAuthorizedError(LifecycleError):
    """The calling stage is not authorized for this (artifact_type, operation)."""


class IllegalTransitionError(LifecycleError):
    """The artifact is not in the operation's required from-state."""


# Each operation's (from_state, to_state). ``declare`` creates (no from-state)
# and is handled by :func:`declare_artifact`, never :func:`transition`.
_OPERATIONS: Final[dict[str, tuple[ArtifactState | None, ArtifactState]]] = {
    "declare": (None, ArtifactState.DECLARED),
    "bind": (ArtifactState.DECLARED, ArtifactState.GROUNDED),
    "compose": (ArtifactState.DECLARED, ArtifactState.GROUNDED),  # metrics' bind (DAT-456)
    "execute": (ArtifactState.GROUNDED, ArtifactState.EXECUTED),
    "endorse": (ArtifactState.EXECUTED, ArtifactState.CANONICAL),
}

# (artifact_type, operation) → stages authorized to invoke it. Absence of a
# key means no stage is authorized — fail closed.
_STAGE_AUTHORIZATIONS: Final[dict[tuple[str, str], frozenset[str]]] = {
    ("validation", "declare"): frozenset({"operating_model"}),
    ("validation", "bind"): frozenset({"operating_model"}),
    ("validation", "execute"): frozenset({"operating_model"}),
    ("validation", "endorse"): frozenset(),  # defined; no endorsement workflow yet
    # cycles — the second lifecycle family (DAT-455), mirroring validation 1:1:
    # the vertical's ``cycles.yaml`` cycle_types ⊕ ``cycle`` overlay teach rows
    # are the declared set, declare authority is the vertical (the stage just
    # records it; frame-2 takes over user declares), and ``endorse`` is defined
    # with no authorized stage (no endorsement workflow exists yet).
    ("cycle", "declare"): frozenset({"operating_model"}),
    ("cycle", "bind"): frozenset({"operating_model"}),
    ("cycle", "execute"): frozenset({"operating_model"}),
    ("cycle", "endorse"): frozenset(),  # defined; no endorsement workflow yet
    # metrics — the third lifecycle family (DAT-456), mirroring validation/cycles
    # 1:1 except the declared→grounded verb is ``compose`` (a metric grounds when
    # its graph's inputs resolve to real columns/concepts of the workspace). The
    # declared set is the vertical's ``metrics/`` graphs ⊕ ``metric`` overlay teach
    # rows; declare authority is the vertical (the stage records it; frame-2 takes
    # over user declares); ``endorse`` is defined with no authorized stage.
    ("metric", "declare"): frozenset({"operating_model"}),
    ("metric", "compose"): frozenset({"operating_model"}),
    ("metric", "execute"): frozenset({"operating_model"}),
    ("metric", "endorse"): frozenset(),  # defined; no endorsement workflow yet
}


def authorize(artifact_type: str, operation: str, stage: str) -> None:
    """Reject an operation the calling stage is not authorized for.

    Args:
        artifact_type: the teach type (e.g. ``"validation"``).
        operation: the lifecycle operation (``declare``/``bind``/``execute``/``endorse``).
        stage: the journey stage attempting the operation.

    Raises:
        StageNotAuthorizedError: unknown (type, operation) pair, or the stage
            is not in the pair's authorized set. Fail closed.
    """
    allowed = _STAGE_AUTHORIZATIONS.get((artifact_type, operation))
    if allowed is None:
        raise StageNotAuthorizedError(
            f"unknown lifecycle operation {artifact_type}.{operation} — no stage is authorized"
        )
    if stage not in allowed:
        raise StageNotAuthorizedError(
            f"stage {stage!r} is not authorized for {artifact_type}.{operation}"
            f" (authorized: {sorted(allowed) or 'none — no authority workflow exists'})"
        )


def declare_artifact(
    session: Session,
    *,
    artifact_type: str,
    artifact_key: str,
    run_id: str,
    stage: str,
    teaches: dict[str, Any] | None = None,
    strictness: float | None = None,
) -> LifecycleArtifact:
    """Declare-or-reuse: this run's ``declared`` artifact row (DAT-502).

    Temporal activities are at-least-once: a success-redelivery (committed
    rows, ack lost) re-declares the same ``(type, key, run)``
    identity. Instead of violating the identity UNIQUE, the existing row is
    RESET to ``declared`` — ``state_reason``/``grounded_against`` cleared,
    ``teaches``/``strictness`` refreshed — because :func:`transition` requires
    exact from-states: a leftover ``grounded``/``executed`` state from the
    first delivery would reject the redelivered bind. The redelivered run then
    re-flows the whole lifecycle on the same row. A NEW run still declares
    anew under its fresh ``run_id`` — supersession across runs, never
    mutation of a prior run's row.

    The row is added to ``session`` here (no caller-side ``session.add``).

    Raises:
        StageNotAuthorizedError: the stage may not declare this type.
    """
    from dataraum.lifecycle.db_models import LifecycleArtifact

    authorize(artifact_type, "declare", stage)

    existing = session.execute(
        select(LifecycleArtifact).where(
            LifecycleArtifact.artifact_type == artifact_type,
            LifecycleArtifact.artifact_key == artifact_key,
            LifecycleArtifact.run_id == run_id,
        )
    ).scalar_one_or_none()
    if existing is not None:
        existing.state = ArtifactState.DECLARED.value
        existing.state_reason = None
        existing.grounded_against = None
        existing.teaches = teaches
        existing.strictness = strictness
        existing.state_changed_at = datetime.now(UTC)
        logger.debug(
            "lifecycle_declare_reused",
            artifact_type=artifact_type,
            artifact_key=artifact_key,
            run_id=run_id,
        )
        return existing

    artifact = LifecycleArtifact(
        artifact_type=artifact_type,
        artifact_key=artifact_key,
        run_id=run_id,
        state=ArtifactState.DECLARED.value,
        stage=stage,
        teaches=teaches,
        strictness=strictness,
    )
    session.add(artifact)
    return artifact


def transition(
    artifact: LifecycleArtifact,
    *,
    operation: str,
    stage: str,
    grounded_against: dict[str, Any] | None = None,
    state_reason: str | None = None,
) -> LifecycleArtifact:
    """Advance an artifact through one lifecycle operation, in place.

    Mutates the active run's row (within-run advancement); prior runs' rows
    are never passed here — supersession across runs goes through
    :func:`declare_artifact` under the new ``run_id``.

    Args:
        artifact: the active run's row.
        operation: ``bind`` / ``execute`` / ``endorse``.
        stage: the journey stage invoking the operation.
        grounded_against: pinned base-run map snapshot, recorded on ``bind``.
        state_reason: optional context for the new state (replaces the old reason).

    Raises:
        StageNotAuthorizedError: stage not authorized for the operation.
        IllegalTransitionError: ``declare`` (creates, never transitions), an
            unknown operation, or the artifact not in the required from-state.
    """
    authorize(artifact.artifact_type, operation, stage)

    spec = _OPERATIONS.get(operation)
    if spec is None:
        raise IllegalTransitionError(f"{operation!r} is not a transition")
    from_state, to_state = spec
    if from_state is None:
        raise IllegalTransitionError(
            f"{operation!r} is not a transition — declare creates artifacts, "
            "it does not transition them"
        )

    if artifact.state != from_state.value:
        raise IllegalTransitionError(
            f"{artifact.artifact_type}.{operation} requires state {from_state.value!r}, "
            f"but {artifact.artifact_key!r} (run {artifact.run_id}) is {artifact.state!r}"
        )

    artifact.state = to_state.value
    artifact.state_changed_at = datetime.now(UTC)
    artifact.state_reason = state_reason
    if grounded_against is not None:
        artifact.grounded_against = grounded_against

    logger.debug(
        "lifecycle_transition",
        artifact_type=artifact.artifact_type,
        artifact_key=artifact.artifact_key,
        run_id=artifact.run_id,
        operation=operation,
        state=artifact.state,
    )
    return artifact
