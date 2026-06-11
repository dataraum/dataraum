"""Materialize durable relationship overlays into a begin_session run (DAT-409).

The relationship catalog of a begin_session run is built in layers: ``relationships``
derives ephemeral ``candidate`` rows, ``semantic_per_table`` confirms a subset as
``llm``, and THIS step materializes the user's durable teaches —
``ConfigOverlay(type='relationship')`` with ``action`` ``add`` or ``confirm``
(→ ``manual``, the explicit human assertion) or ``keep`` (→ ``keeper``, the
silent-accept method, DAT-409 C3) — as run-stamped ``Relationship`` rows so they
re-appear in every run without ever mutating derived metadata.

Runs AFTER ``semantic_per_table`` (so the ``llm`` set exists) and before
``session_detect``. Dedup is per (pair, METHOD), not per pair (DAT-447): the
relationship_discovery adjudication reads the per-method rows side by side, so a
human verdict must be able to coexist with the ``llm`` row it confirms — the old
skip-any-defined-pair rule made the ``manual_curation`` witness structurally
impossible on exactly the pairs the system asks the user to confirm, breaking the
teach circuit (system asks for the verdict → the verdict becomes a witness → the
witness's reliability gets measured). One row per (pair, method) is still
guaranteed (the ``uq_relationship_columns_method`` key); the catalog enumeration
de-duplicates pairs, and join-path counting collapses methods by column pair.
Rejected pairs are skipped (a reject wins over a stale add/confirm/keep).
Re-running the same ``run_id`` (a Temporal retry) clears only this run's own
``manual``/``keeper`` rows first, so it is idempotent and non-destructive to
other runs.
"""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from dataraum.analysis.relationships.db_models import Relationship
from dataraum.analysis.relationships.utils import (
    load_suppressed_relationship_pairs,
    relationship_overlay_pairs,
)
from dataraum.core.logging import get_logger
from dataraum.storage import Column

logger = get_logger(__name__)

# action → the durable detection_method it materializes as. ``add`` and
# ``confirm`` are both explicit human assertions (manual); ``add`` is listed
# first so it wins the per-(pair, method) dedup when both overlays exist.
_ACTION_METHOD = {"add": "manual", "confirm": "manual", "keep": "keeper"}


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

    # The run's DEFINED rows so far, keyed (pair, method) — candidates are
    # ephemeral and excluded. Dedup is per METHOD: an overlay row may join a
    # pair the llm already confirmed (that coexistence IS the witness pool),
    # but never duplicate a row of its own method.
    defined_stmt = select(
        Relationship.from_column_id,
        Relationship.to_column_id,
        Relationship.detection_method,
    ).where(
        Relationship.session_id == session_id,
        Relationship.detection_method != "candidate",
    )
    if run_id is not None:
        defined_stmt = defined_stmt.where(Relationship.run_id == run_id)
    written: set[tuple[str, str, str]] = {
        (f, t, str(m)) for f, t, m in session.execute(defined_stmt).tuples()
    }

    # Resolve column → owning table for every column an overlay references, bounded
    # to the session's tables — an overlay pointing outside the selection is skipped.
    table_by_column = _column_table_map(session, table_ids)

    count = 0
    for action, method in _ACTION_METHOD.items():
        for from_col, to_col in relationship_overlay_pairs(session, action):
            pair = (from_col, to_col)
            if pair in suppressed or (from_col, to_col, method) in written:
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
            written.add((from_col, to_col, method))
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
    # `.tuples().all()` materializes the rows — `dict()` on a bare Result takes the
    # mapping path (Result has .keys()) and raises "not subscriptable".
    rows = session.execute(
        select(Column.column_id, Column.table_id).where(Column.table_id.in_(table_ids))
    ).tuples()
    return dict(rows.all())


def write_relationship_keepers(
    session: Session,
    session_id: str,
    *,
    current_run_id: str | None,
) -> int:
    """Lift silently-accepted relationships into ``keep`` overlays (DAT-409 C3).

    The silent-accept rule: an ``llm`` relationship the **promoted prior run** found,
    that the **current run did not reproduce** and the user did **not reject**, was
    accepted by the user's silence. Make that explicit — write a
    ``ConfigOverlay(action='keep', …)`` so the next run materializes it as a durable
    ``keeper`` (the invisible "silence = acceptance" becomes an auditable record).

    Runs as a pre-promote step: the per-session head still points at the *prior*
    promoted run (``session_promote_to_latest`` flips it after this), which is the
    "promoted prior run" the rule compares against. Returns the number of keep
    overlays written. First run (no promoted prior) → no-op.

    The lifted relationship is absent from the run that detected its absence; the
    ``keep`` overlay materializes it as ``keeper`` from the NEXT run onward (the
    accepted one-run gap, by spec).
    """
    from dataraum.storage import ConfigOverlay
    from dataraum.storage.snapshot_head import head_run_id, session_head_target

    prior_run = head_run_id(session, session_head_target(session_id), "detect")
    if prior_run is None or prior_run == current_run_id:
        return 0

    prior_llm = _run_pairs(session, session_id, prior_run, method="llm")
    reproduced = _run_pairs(session, session_id, current_run_id)
    rejected = load_suppressed_relationship_pairs(session)
    already_kept = set(relationship_overlay_pairs(session, "keep"))

    count = 0
    for pair in prior_llm:
        if pair in reproduced or pair in rejected or pair in already_kept:
            continue
        session.add(
            ConfigOverlay(
                type="relationship",
                payload={
                    "action": "keep",
                    "from_column_id": pair[0],
                    "to_column_id": pair[1],
                },
            )
        )
        already_kept.add(pair)
        count += 1

    logger.info(
        "relationship_keepers_written",
        session_id=session_id,
        prior_run=prior_run,
        current_run=current_run_id,
        count=count,
    )
    return count


def _run_pairs(
    session: Session,
    session_id: str,
    run_id: str | None,
    *,
    method: str | None = None,
) -> set[tuple[str, str]]:
    """Directional ``(from_col, to_col)`` pairs of a run's catalog.

    ``method`` filters to one detection method; ``None`` = the whole defined catalog
    (``!= candidate``) — i.e. what the run actually reproduced.
    """
    stmt = select(Relationship.from_column_id, Relationship.to_column_id).where(
        Relationship.session_id == session_id,
        Relationship.run_id == run_id,
    )
    stmt = stmt.where(
        Relationship.detection_method == method
        if method is not None
        else Relationship.detection_method != "candidate"
    )
    return set(session.execute(stmt).tuples())
