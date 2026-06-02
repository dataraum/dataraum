"""Persist per-column readiness — the terminal detect step's snapshot (DAT-394).

The readiness-v2 rollup (``entropy/views/readiness_context.py``) rolls the persisted
entropy objects up the network into per-column, per-intent readiness.
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


def persist_readiness(session: Session, session_id: str, table_ids: list[str]) -> int:
    """Compute + persist per-column readiness for the session's typed tables.

    Delete-before-insert scoped to ``table_ids`` — the session's tables (DAT-410),
    not a source. Replay-safe: the prior rows for these tables are cleared even
    when the new rollup is empty, and a per-table replay clears only that table's
    rows (not the whole source's). For ``add_source`` the set is one source's
    typed tables, so the result is identical to the prior source-scoped behaviour.
    The caller owns the transaction (commit at its ``session_scope`` exit). Each
    row's ``source_id`` is derived per-table so a multi-source session's rows stay
    well-formed. Returns the rows written.
    """
    # Replay-safe refresh: clear these tables' prior readiness rows first.
    session.execute(
        delete(EntropyReadinessRecord).where(EntropyReadinessRecord.table_id.in_(table_ids))
    )
    if not table_ids:
        return 0

    ctx = build_for_readiness(session, table_ids)
    if not ctx.columns:
        return 0

    target_to_ids = _column_id_map(session, table_ids)
    source_by_table = {
        table_id: source_id
        for table_id, source_id in session.execute(
            select(Table.table_id, Table.source_id).where(Table.table_id.in_(table_ids))
        )
    }

    rows: list[EntropyReadinessRecord] = []
    for target, col in ctx.columns.items():
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
                source_id=source_by_table[table_id],
                table_id=table_id,
                column_id=column_id,
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
