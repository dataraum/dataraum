"""Pinned base-run map — ADR-0008's in-run read mode (DAT-438).

An ``OperatingModelWorkflow`` run reads upstream promoted state (begin_session
relationships, add_source per-column semantics) through run_ids **pinned once
at run start** — the detached-HEAD mode. Re-deriving heads per reader is the
deprecated convention ADR-0008 retires: it tears under a concurrent re-promote
and re-implements the head join at every site. The map is also the artifact's
``grounded_against`` provenance (refine decision D2) — what the bind actually
read from, recorded verbatim.

:func:`resolve_operating_model_base_runs` is the single implementation —
distinct from the detect-tier ``entropy.detectors.loaders.resolve_base_runs``
(per-table add_source stage pins for ONE detect pass); this map crosses the
workflow's contracts. Its caller is the
``operating_model_resolve`` pre-flight activity, and the map travels with the
workflow's contracts from there.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pydantic import BaseModel, Field

from dataraum.core.logging import get_logger
from dataraum.storage.snapshot_head import GENERATION_STAGE, catalog_head_target, head_run_id

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = get_logger(__name__)


class BaseRunMap(BaseModel):
    """The run_ids an operating_model run reads upstream state at.

    Attributes:
        relationship_run_id: begin_session's promoted ``(catalog, catalog)``
            head — scopes defined-relationship reads. ``None`` when the
            workspace has no promoted begin_session run yet: relationship context
            stays EMPTY (fail-closed, DAT-429), never a cross-run read.
        semantic_runs: per-table promoted generation heads (``table:{id}``) —
            scope the per-column semantic-annotation reads. A table with no
            promoted head is absent: its annotations stay empty.
    """

    relationship_run_id: str | None = None
    semantic_runs: dict[str, str] = Field(default_factory=dict)


def resolve_operating_model_base_runs(session: Session, table_ids: list[str]) -> BaseRunMap:
    """Resolve the promoted upstream heads ONCE for this run.

    Args:
        session: SQLAlchemy session.
        table_ids: the catalog's typed tables.

    Returns:
        The pinned map. An unresolved SEMANTIC head is recorded as absent
        (fail-closed at the readers: that table's annotations stay empty). An
        unresolved relationship head (``relationship_run_id=None``) is logged
        here but REFUSED by the only caller (``resolve_operating_model_scope``,
        DAT-511) — it is not a state downstream code handles.
    """
    # "catalog" is begin_session's promoted stage on the workspace catalog head
    # (see promote_session_run) — distinct from operating_model's own head, which
    # this run WRITES at terminal promote and never reads from.
    relationship_run_id = head_run_id(session, catalog_head_target(), "catalog")
    if relationship_run_id is None:
        logger.warning(
            "base_run_unresolved",
            target="catalog",
            stage="catalog",
            detail="no promoted begin_session run; relationship context will be empty",
        )

    semantic_runs: dict[str, str] = {}
    for table_id in table_ids:
        run = head_run_id(session, f"table:{table_id}", GENERATION_STAGE)
        if run is None:
            logger.warning(
                "base_run_unresolved",
                target=f"table:{table_id}",
                stage=GENERATION_STAGE,
                detail="no promoted semantic run; this table's annotations will be empty",
            )
            continue
        semantic_runs[table_id] = run

    return BaseRunMap(relationship_run_id=relationship_run_id, semantic_runs=semantic_runs)
