"""Materialize durable relationship overlays into a begin_session run (DAT-409).

The relationship catalog of a begin_session run is built in layers: ``relationships``
derives ephemeral ``candidate`` rows, ``semantic_per_table`` confirms a subset as
``llm``, and THIS step materializes the user's durable teaches —
``ConfigOverlay(type='relationship')`` with ``action`` ``add`` (→ ``manual``) or
``keep`` (→ ``keeper``, the silent-accept method, DAT-409 C3) — as run-stamped
``Relationship`` rows so they re-appear in every run without ever mutating derived
metadata.

Runs AFTER ``semantic_per_table`` (so the ``llm`` set exists) and before
``session_detect``: an overlay whose pair the current run already produced as
``llm`` is skipped, so the catalog never carries two rows for one pair. Rejected
pairs are skipped too (a reject wins over a stale add/keep). Re-running the same
``run_id`` (a Temporal retry) clears only this run's own ``manual``/``keeper`` rows
first, so it is idempotent and non-destructive to other runs.
"""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.utils import (
    _relationship_overlay_pairs,
    load_suppressed_relationship_pairs,
)
from dataraum.core.logging import get_logger
from dataraum.storage import Column

logger = get_logger(__name__)

# action → the durable detection_method it materializes as.
_ACTION_METHOD = {"add": "manual", "keep": "keeper"}


def materialize_relationship_overlays(
    session: Session,
    session_id: str,
    *,
    run_id: str | None,
    table_ids: list[str],
) -> int:
    """Write this run's durable (`manual`/`keeper`) relationships from overlays.

    Returns the number of rows materialized. Idempotent per ``run_id``.
    """
    # Retry-safe clear: drop only THIS run's prior durable rows (run_id-scoped, like
    # the candidate clear), never another run's. candidate/llm are owned by their
    # own steps and untouched.
    session.execute(
        delete(Relationship).where(
            Relationship.session_id == session_id,
            Relationship.run_id == run_id,
            Relationship.detection_method.in_(tuple(_ACTION_METHOD.values())),
        )
    )

    suppressed = load_suppressed_relationship_pairs(session)

    # The run's DEFINED catalog so far (llm; candidates are ephemeral and excluded).
    # A pair already defined this run must not be duplicated by an overlay.
    defined_stmt = select(Relationship.from_column_id, Relationship.to_column_id).where(
        Relationship.session_id == session_id,
        Relationship.detection_method != "candidate",
    )
    if run_id is not None:
        defined_stmt = defined_stmt.where(Relationship.run_id == run_id)
    existing: set[tuple[str, str]] = set(session.execute(defined_stmt).tuples())

    # Resolve column → owning table for every column an overlay references, bounded
    # to the session's tables — an overlay pointing outside the selection is skipped.
    table_by_column = _column_table_map(session, table_ids)

    count = 0
    for action, method in _ACTION_METHOD.items():
        for from_col, to_col in _relationship_overlay_pairs(session, action):
            pair = (from_col, to_col)
            if pair in suppressed or pair in existing:
                continue
            from_table = table_by_column.get(from_col)
            to_table = table_by_column.get(to_col)
            if from_table is None or to_table is None:
                # Endpoint outside the session's tables (or unknown) — nothing to
                # anchor the row to; skip rather than write a dangling relationship.
                continue
            session.add(
                Relationship(
                    relationship_id=str(uuid4()),
                    session_id=session_id,
                    run_id=run_id,
                    from_table_id=from_table,
                    from_column_id=from_col,
                    to_table_id=to_table,
                    to_column_id=to_col,
                    relationship_type="foreign_key",
                    cardinality=None,
                    confidence=1.0,
                    detection_method=method,
                    evidence={"source": "config_overlay", "action": action},
                    is_confirmed=True,
                )
            )
            existing.add(pair)
            count += 1

    logger.info(
        "relationship_overlays_materialized",
        session_id=session_id,
        run_id=run_id,
        count=count,
    )
    return count


def _column_table_map(session: Session, table_ids: list[str]) -> dict[str, str]:
    """``column_id -> table_id`` for every column in ``table_ids``."""
    rows = session.execute(
        select(Column.column_id, Column.table_id).where(Column.table_id.in_(table_ids))
    ).tuples()
    return dict(rows)
