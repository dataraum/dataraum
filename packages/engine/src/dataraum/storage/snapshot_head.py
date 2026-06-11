"""Snapshot head-pointer model (DAT-413, generalized DAT-408).

The per-(target, stage) head of the snapshot version axis. Each run mints one
``run_id`` (``AddSourceWorkflow.run`` / ``BeginSessionWorkflow.run`` via
``workflow.uuid4``) and stamps it onto every metadata row it writes. This table
records, for each ``(target, stage)``, which ``run_id`` is the *current*
(promoted) snapshot — so a later promote step can flip the head from
delete-then-insert to insert-new-run-then-flip-head without widening any unique
constraint.

``target`` is the generic scope-string key (DAT-408): ``table:{id}`` for
add_source's per-table stages and detect column readiness, ``relationship:{from_col}::{to_col}``
for begin_session relationship readiness (and ``column:…`` / ``workspace:…`` when
later grains need it). It is a free string, not an FK — a relationship target has
no single owning table.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import DateTime, String, UniqueConstraint, select
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class MetadataSnapshotHead(Base):
    """Current promoted snapshot ``run_id`` for one ``(target, stage)`` grain.

    Columns:
        head_id: uuid4 primary key.
        target: the scope-string key — ``table:{id}`` / ``relationship:{from_col}::{to_col}``
            / ``column:…`` (DAT-408). Free string, not an FK.
        stage: the producing phase name (e.g. ``"statistics"``, ``"detect"``).
        run_id: the current (promoted) snapshot's run, minted by the workflow.
        promoted_at: when this head was last flipped.
    """

    __tablename__ = "metadata_snapshot_head"
    __table_args__ = (UniqueConstraint("target", "stage", name="uq_snapshot_head_target_stage"),)

    head_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    target: Mapped[str] = mapped_column(String, nullable=False)
    stage: Mapped[str] = mapped_column(String, nullable=False)
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    promoted_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )


def session_head_target(session_id: str) -> str:
    """The snapshot-head key sealing a begin_session run (DAT-408).

    begin_session re-runs the whole session atomically (no per-table partial
    replay), so it seals at **session grain** — one head per session, not per
    target. The head names the session's current (promoted) run; everything
    begin_session produces for the session reads at that run_id.
    """
    return f"session:{session_id}"


def head_run_id(session: Session, target: str, stage: str) -> str | None:
    """The promoted (current) ``run_id`` for one ``(target, stage)`` grain (DAT-408).

    The query-time resolver an external reader of run_id-stamped metadata uses to
    pick which run's rows are current. Under multi-run coexistence two runs' rows
    share a ``(target, stage)`` but carry distinct ``run_id``s; the head names the
    promoted one, so a reader filters its query by this value instead of assuming
    a single row per target.

    Returns ``None`` when no run has been promoted for this ``(target, stage)``
    yet — the caller treats that as "nothing current" and falls back to its
    no-data behaviour, never guessing a run.
    """
    return session.execute(
        select(MetadataSnapshotHead.run_id).where(
            MetadataSnapshotHead.target == target,
            MetadataSnapshotHead.stage == stage,
        )
    ).scalar_one_or_none()
