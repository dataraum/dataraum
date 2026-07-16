"""Lifecycle artifact model — the typed artifact lifecycle substrate (DAT-438).

The journey's artifacts (validations in slice 1; cycles and metrics in later
operating_model slices) move through the typed lifecycle of
``architecture-future.md``: ``declared`` → ``grounded`` → ``executed`` are
*system* states, ``canonical`` is the *organizational* state reached only
through the (not yet built) endorsement workflow. This table is the general
substrate; the state machine and stage authorization live in
:mod:`dataraum.lifecycle.transitions`.

Versioning contract (DAT-408/413 + ADR-0008):

* **Append-only across runs** — a re-run supersedes by writing new rows under
  its fresh ``run_id``; a prior run's rows are never mutated. Within the
  *active* run the row's ``state`` advances in place (declared → grounded →
  executed happen during one ``OperatingModelWorkflow`` run on one row); the
  identity UNIQUE makes that one row per ``(type, key, run)``.
* **Born view-ready** — ``run_id``-stamped under the workspace catalog head
  (target ``catalog``, stage ``operating_model``), so the ADR-0008
  ``current_*`` view generator covers it with the standard head join; no
  reader needs hand-rolled head resolution.

Provenance lives ON the row (refine decision D2 — no edge table in slice 1):
``grounded_against`` snapshots the pinned base-run map the artifact was bound
against, ``teaches`` names what produced it (spec id, vertical, spec version),
``strictness`` records the journey parameter (nullable — frame does not write
it yet, and nothing gates on it; refine decision D3).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy import JSON, CheckConstraint, DateTime, Float, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.lifecycle.transitions import ArtifactState
from dataraum.storage.base import Base

# Closed-vocabulary CHECK values (DAT-802 enum-standard sweep), derived from
# ArtifactState, the single home (``lifecycle/transitions.py`` — every
# ``declare_artifact`` / ``transition`` write goes through ``.value`` on this
# enum). Sorted for a deterministic CHECK string in the offline DDL dump.
_ARTIFACT_STATE_VALUES: tuple[str, ...] = tuple(sorted(v.value for v in ArtifactState))


class LifecycleArtifact(Base):
    """One artifact's lifecycle state for one run.

    Columns:
        artifact_id: uuid4 primary key.
        artifact_type: the teach type — ``"validation"`` in slice 1.
        artifact_key: the artifact's identity within its type (e.g. ``validation_id``).
        run_id: the ``OperatingModelWorkflow`` run that wrote this row.
        state: ``declared`` / ``grounded`` / ``executed`` / ``canonical``
            (:class:`~dataraum.lifecycle.transitions.ArtifactState` values).
        state_reason: why the artifact sits in its current state — the
            "visibly impossible" home: an ungroundable artifact stays
            ``declared`` with the reason recorded here, never silently absent.
        stage: the journey stage that produced the row (``"operating_model"``).
        strictness: the journey's strictness parameter at build time; nullable.
        grounded_against: the pinned base-run map the bind read from (JSON).
        teaches: what declared/produced it — spec id, vertical, spec version (JSON).
        graph_definition: METRIC artifacts only — the effective (shipped ⊕ overlay)
            metric-graph the phase assembled from (the DAG dict from
            ``get_metric_definitions``). Persisted so the cockpit reads the EXACT
            structure it rendered (steps / formulas / extracts / constants) from one
            Postgres source, overlay-inclusive, zero divergence (DAT-591). Null for
            validation/cycle rows.
        created_at / state_changed_at: row birth / last in-run state advance.
    """

    __tablename__ = "lifecycle_artifacts"
    __table_args__ = (
        UniqueConstraint(
            "artifact_type",
            "artifact_key",
            "run_id",
            name="uq_lifecycle_artifact_identity",
        ),
        # Closed-vocabulary enforcement (DAT-802 enum-standard sweep): derived
        # from ArtifactState, the single home — the DB-enforced backstop for the
        # typed state machine ``lifecycle/transitions.py`` already governs app-side.
        CheckConstraint(
            "state IN (" + ", ".join(f"'{v}'" for v in _ARTIFACT_STATE_VALUES) + ")",
            name="state",
        ),
    )

    artifact_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    artifact_type: Mapped[str] = mapped_column(String, nullable=False)
    artifact_key: Mapped[str] = mapped_column(String, nullable=False)
    run_id: Mapped[str] = mapped_column(String, nullable=False)

    state: Mapped[str] = mapped_column(String, nullable=False)
    state_reason: Mapped[str | None] = mapped_column(Text)

    # Provenance (refine decision D2: on the row, no edge table in slice 1)
    stage: Mapped[str] = mapped_column(String, nullable=False)
    strictness: Mapped[float | None] = mapped_column(Float)
    grounded_against: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    teaches: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    # METRIC rows only: the effective (shipped ⊕ overlay) DAG the phase assembled from
    # (DAT-591). One Postgres source for the cockpit's step rendering; null otherwise.
    graph_definition: Mapped[dict[str, Any] | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    state_changed_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
