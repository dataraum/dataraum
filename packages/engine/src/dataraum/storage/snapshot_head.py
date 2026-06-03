"""Snapshot head-pointer model (DAT-413).

The per-(table, stage) head of the snapshot version axis. Each add_source run
mints one ``run_id`` (``AddSourceWorkflow.run`` via ``workflow.uuid4``) and stamps
it onto every metadata row it writes. This table records, for each
``(table_id, stage)``, which ``run_id`` is the *current* (promoted) snapshot — so
a later promote step can flip the head from delete-then-insert to
insert-new-run-then-flip-head without widening any unique constraint.

Phase 1 (behavior-preserving) defines the model ONLY so ``create_all`` makes the
table. Nothing reads or writes it yet — every phase still does delete-then-insert.
The grain is ``table_id`` for Slice A; a ``target``-string generalization
(``column:…`` / ``relationship:…``) is DAT-408.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from dataraum.storage.base import Base


class MetadataSnapshotHead(Base):
    """Current promoted snapshot ``run_id`` for one ``(table_id, stage)`` grain.

    Columns:
        head_id: uuid4 primary key.
        table_id: FK to ``tables`` — the per-table snapshot grain (Slice A).
        stage: the producing phase name (e.g. ``"statistics"``, ``"detect"``).
        run_id: the current (promoted) snapshot's run, minted by the workflow.
        promoted_at: when this head was last flipped.
        version: optimistic-concurrency counter; bumped on each promote.
    """

    __tablename__ = "metadata_snapshot_head"
    __table_args__ = (UniqueConstraint("table_id", "stage", name="uq_snapshot_head_table_stage"),)

    head_id: Mapped[str] = mapped_column(String, primary_key=True, default=lambda: str(uuid4()))
    table_id: Mapped[str] = mapped_column(
        ForeignKey("tables.table_id", ondelete="CASCADE"), nullable=False
    )
    stage: Mapped[str] = mapped_column(String, nullable=False)
    run_id: Mapped[str] = mapped_column(String, nullable=False)
    promoted_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=lambda: datetime.now(UTC)
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
