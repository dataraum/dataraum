"""Pinned base-run map — ADR-0008's in-run read mode (DAT-438).

An ``OperatingModelWorkflow`` run reads upstream promoted state (begin_session
relationships, add_source per-column semantics) through run_ids **pinned once
at run start** — the detached-HEAD mode. Re-deriving heads per reader is the
deprecated convention ADR-0008 retires: it tears under a concurrent re-promote
and re-implements the head join at every site. The map is also the artifact's
``grounded_against`` provenance (refine decision D2) — what the bind actually
read from, recorded verbatim.

:func:`resolve_base_runs` is the single implementation; its caller is the
``operating_model_resolve`` pre-flight activity, and the map travels with the
workflow's contracts from there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from dataraum.core.logging import get_logger
from dataraum.storage.snapshot_head import head_run_id, session_head_target

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


class BaseRunMap(BaseModel):
    """The run_ids an operating_model run reads upstream state at.

    Attributes:
        relationship_run_id: begin_session's promoted ``(session:{id}, detect)``
            head — scopes defined-relationship reads. ``None`` when the session
            has no promoted begin_session run yet: relationship context stays
            EMPTY (fail-closed, DAT-429), never a cross-run read.
        semantic_runs: per-table promoted ``(table:{id}, semantic_per_column)``
            heads — scope the per-column semantic-annotation reads. A table
            with no promoted head is absent: its annotations stay empty.
    """

    relationship_run_id: str | None = None
    semantic_runs: dict[str, str] = Field(default_factory=dict)


def resolve_base_runs(session: Session, session_id: str, table_ids: list[str]) -> BaseRunMap:
    """Resolve the promoted upstream heads ONCE for this run.

    Args:
        session: SQLAlchemy session.
        session_id: the journey session (same id begin_session ran under).
        table_ids: the session's typed tables.

    Returns:
        The pinned map. Unresolved heads are recorded as absent (fail-closed
        at the readers), and logged — a missing begin_session head on a
        session that is supposed to have one is worth seeing.
    """
    # "detect" is begin_session's promoted stage on the session target (see
    # promote_session_run) — distinct from operating_model's own head, which
    # this run WRITES at terminal promote and never reads from.
    relationship_run_id = head_run_id(session, session_head_target(session_id), "detect")
    if relationship_run_id is None:
        logger.warning(
            "base_run_unresolved",
            session_id=session_id,
            target="session",
            stage="detect",
            detail="no promoted begin_session run; relationship context will be empty",
        )

    semantic_runs: dict[str, str] = {}
    for table_id in table_ids:
        run = head_run_id(session, f"table:{table_id}", "semantic_per_column")
        if run is None:
            logger.warning(
                "base_run_unresolved",
                session_id=session_id,
                target=f"table:{table_id}",
                stage="semantic_per_column",
                detail="no promoted semantic run; this table's annotations will be empty",
            )
            continue
        semantic_runs[table_id] = run

    return BaseRunMap(relationship_run_id=relationship_run_id, semantic_runs=semantic_runs)
