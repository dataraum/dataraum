"""Persist per-column readiness — the terminal detect step's snapshot (DAT-394).

The readiness-v2 rollup (``entropy/views/readiness_context.py``) rolls the persisted
entropy objects through the per-intent loss tables into per-column readiness.
:func:`persist_readiness` writes that rollup to ``entropy_readiness`` (one row per
analyzed column) so the cockpit ``why`` / ``look`` tools read it via Drizzle with no
engine round-trip, and as agent context.

Self-refreshing: called from the terminal ``detect`` step (which re-runs on every
(re-)measure), it delete-before-inserts scoped to the session's table set, so a
teach->replay overwrites the rows with no stale leftovers.
"""

from __future__ import annotations

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from dataraum.core.logging import get_logger
from dataraum.entropy.db_models import EntropyReadinessRecord
from dataraum.entropy.views.readiness_context import ColumnReadinessResult, build_for_readiness
from dataraum.storage import Column, Table

logger = get_logger(__name__)


def persist_readiness(
    session: Session,
    session_id: str,
    table_ids: list[str],
    *,
    run_id: str | None = None,
) -> int:
    """Compute + persist per-column readiness for the session's typed tables.

    Delete-before-insert scoped to ``table_ids`` — the session's tables (DAT-410),
    not a source. Replay-safe: the prior rows for these tables are cleared even
    when the new rollup is empty, and a per-table replay clears only that table's
    rows (not the whole source's). For ``add_source`` the set is one source's
    typed tables, so the result is identical to the prior source-scoped behaviour.
    The caller owns the transaction (commit at its ``session_scope`` exit). Rows
    carry no ``source_id`` (DAT-408) — source is reachable via ``table_id``.
    ``run_id`` is the snapshot version axis (DAT-413), stamped on each row (``None``
    outside the workflow path); the head pointer is not consulted yet. Returns the
    rows written.
    """
    if not table_ids:
        return 0

    # Replay-safe refresh: clear THIS run's prior readiness rows first. ALWAYS
    # scoped to the run (``run_id ==``, i.e. ``IS NULL`` for the un-versioned test
    # path) — never an unscoped delete, which would wipe every run's readiness.
    # Two scopes (DAT-408): column rows carry ``table_id`` so they delete by the
    # table set; relationship rows carry no ``table_id`` (the identity is in
    # ``target``), so they delete by ``(session_id, relationship:%)``.
    col_del = delete(EntropyReadinessRecord).where(
        EntropyReadinessRecord.table_id.in_(table_ids),
        EntropyReadinessRecord.run_id == run_id,
    )
    # ``relationship:%`` rows are produced only by begin_session's detect — add_source
    # runs no relationship detectors — so this is a begin_session-only delete in
    # practice; the run_id scope keeps it harmless on the shared add_source path.
    rel_del = delete(EntropyReadinessRecord).where(
        EntropyReadinessRecord.session_id == session_id,
        EntropyReadinessRecord.target.like("relationship:%"),
        EntropyReadinessRecord.run_id == run_id,
    )
    session.execute(col_del)
    session.execute(rel_del)

    ctx = build_for_readiness(session, table_ids, current_run_id=run_id, session_id=session_id)
    if not ctx.columns:
        return 0

    target_to_ids = _column_id_map(session, table_ids)
    table_id_by_name = _table_id_by_name(session, table_ids)

    rows: list[EntropyReadinessRecord] = []
    for target, col in ctx.columns.items():
        if target.startswith("relationship:"):
            # Relationship readiness: the identity is the target; a relationship
            # spans two columns so it carries no single column FK.
            table_id: str | None = None
            column_id: str | None = None
        elif target.startswith("table:"):
            # Table-grain readiness (DAT-415): the fact table's dimension_coverage
            # rolled up. Carries the table FK (so it deletes with the table set on
            # replay) but no column FK — the identity is the table.
            table_id = table_id_by_name.get(target.split(":", 1)[1])
            column_id = None
            if table_id is None:
                logger.debug("readiness_table_target_unresolved", target=target)
                continue
        else:
            ids = target_to_ids.get(target)
            if ids is None:
                # A column target the rollup produced but we can't map back to a
                # Column row (e.g. a renamed/dropped column) — skip, don't guess.
                logger.debug("readiness_target_unresolved", target=target)
                continue
            table_id, column_id = ids
        rows.append(
            EntropyReadinessRecord(
                session_id=session_id,
                target=target,
                table_id=table_id,
                column_id=column_id,
                run_id=run_id,
                band=col.readiness,
                worst_intent_risk=round(col.worst_intent_risk, 4),
                intents=_intents_payload(col),
                top_drivers=_top_drivers_payload(col),
            )
        )

    session.add_all(rows)
    return len(rows)


def _column_id_map(session: Session, table_ids: list[str]) -> dict[str, tuple[str, str]]:
    """Map ``"column:{table_name}.{column_name}"`` -> ``(table_id, column_id)``.

    Mirrors the target string the detectors write (``engine.py`` builds it as
    ``f"column:{table.table_name}.{col.column_name}"``).
    """
    table_name_by_id: dict[str, str] = {}
    for table_id, table_name in session.execute(
        select(Table.table_id, Table.table_name).where(Table.table_id.in_(table_ids))
    ):
        table_name_by_id[table_id] = table_name
    out: dict[str, tuple[str, str]] = {}
    for table_id, column_name, column_id in session.execute(
        select(Column.table_id, Column.column_name, Column.column_id).where(
            Column.table_id.in_(table_ids)
        )
    ):
        table_name = table_name_by_id.get(table_id)
        if table_name is None:
            continue
        out[f"column:{table_name}.{column_name}"] = (table_id, column_id)
    return out


def _table_id_by_name(session: Session, table_ids: list[str]) -> dict[str, str]:
    """Map ``table_name`` -> ``table_id`` for the session's tables.

    Inverse of the ``table:{table_name}`` target the table-scoped detectors write
    (``engine.py`` builds it as ``f"table:{table.table_name}"``).
    """
    return {
        table_name: table_id
        for table_id, table_name in session.execute(
            select(Table.table_id, Table.table_name).where(Table.table_id.in_(table_ids))
        )
    }


def _intents_payload(col: ColumnReadinessResult) -> list[dict[str, object]]:
    """Per-intent breakdown: band + risk + self-describing ranked drivers."""
    return [
        {
            "intent": i.intent_name,
            "band": i.readiness,
            "risk": round(i.risk, 4),
            "drivers": [
                {
                    "node": d.node,
                    "dimension_path": d.dimension_path,
                    "label": d.label,
                    "state": d.state,
                    "impact_delta": round(d.impact_delta, 4),
                }
                for d in i.drivers
            ],
        }
        for i in col.intents
    ]


def _top_drivers_payload(col: ColumnReadinessResult) -> list[dict[str, object]]:
    """Column-level non-clean nodes ranked by collapsed impact_delta (self-describing)."""
    ranked = sorted(
        (ne for ne in col.node_evidence if ne.state != "low"),
        key=lambda ne: ne.impact_delta,
        reverse=True,
    )
    return [
        {
            "node": ne.node_name,
            "dimension_path": ne.dimension_path,
            "label": ne.label,
            "state": ne.state,
            "impact_delta": round(ne.impact_delta, 4),
        }
        for ne in ranked
    ]
