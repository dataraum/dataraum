"""persist_readiness scope key — the session's table set, not source_id (DAT-410).

A per-table replay (``persist_readiness`` over a single table) must clear only that
table's ``entropy_readiness`` rows; a sibling table's rows under the same source
survive. This is the isolation property the source-scoped delete could not give.
"""

from __future__ import annotations

from sqlalchemy.orm import Session

from dataraum.entropy.db_models import EntropyReadinessRecord
from dataraum.entropy.readiness import persist_readiness
from dataraum.storage import Source
from dataraum.storage.models import Table
from tests.conftest import baseline_session_id


def _readiness_row(session: Session, table_id: str, source_id: str) -> None:
    session.add(
        EntropyReadinessRecord(
            session_id=baseline_session_id(),
            target=f"table:{table_id}",
            source_id=source_id,
            table_id=table_id,
            column_id=None,
            band="ready",
            worst_intent_risk=0.0,
        )
    )


def test_per_table_replay_clears_only_its_own_rows(session: Session) -> None:
    """Re-persisting one table of a two-table source leaves the other's rows intact."""
    session.add(Source(source_id="src_x", name="src_x", source_type="csv"))
    for tid in ("tbl_a", "tbl_b"):
        session.add(Table(table_id=tid, source_id="src_x", table_name=tid, layer="typed"))
    session.flush()
    _readiness_row(session, "tbl_a", "src_x")
    _readiness_row(session, "tbl_b", "src_x")
    session.flush()

    # A per-table replay scoped to tbl_a only. No entropy objects exist, so the
    # rollup is empty and nothing is re-inserted — but the delete must touch only
    # tbl_a (DAT-410: delete-before-insert by table_id, not source_id).
    persist_readiness(session, baseline_session_id(), ["tbl_a"])
    session.flush()

    remaining = {r.table_id for r in session.query(EntropyReadinessRecord).all()}
    assert remaining == {"tbl_b"}, "sibling table's readiness must survive a per-table replay"


def _relationship_row(session: Session, target: str, run_id: str) -> None:
    """A relationship-granularity readiness row (DAT-408): identity in ``target``,
    no table_id/column_id/source_id."""
    session.add(
        EntropyReadinessRecord(
            session_id=baseline_session_id(),
            target=target,
            source_id=None,
            table_id=None,
            column_id=None,
            run_id=run_id,
            band="investigate",
            worst_intent_risk=0.5,
        )
    )


def test_relationship_rows_delete_is_run_scoped_and_session_scoped(session: Session) -> None:
    """A re-run clears only its OWN relationship readiness; a prior run survives.

    Relationship rows carry no ``table_id``, so the column delete (by table set)
    can't reach them — they're cleared by the separate ``(session_id, run_id,
    relationship:%)`` scope (DAT-408). A re-run under a fresh run_id must leave the
    earlier run's relationship rows intact (non-destructive, mirrors DAT-413).
    """
    session.add(Source(source_id="src_z", name="src_z", source_type="csv"))
    session.add(Table(table_id="tbl_z", source_id="src_z", table_name="tbl_z", layer="typed"))
    session.flush()
    rel = "relationship:tbl_z.fk-other.id"
    _relationship_row(session, rel, run_id="run-A")
    _relationship_row(session, rel, run_id="run-B")
    session.flush()

    # Re-persist run-A (no entropy objects → re-inserts nothing). Its relationship
    # row is cleared; run-B's survives.
    persist_readiness(session, baseline_session_id(), ["tbl_z"], run_id="run-A")
    session.flush()

    surviving = {
        (r.target, r.run_id)
        for r in session.query(EntropyReadinessRecord).all()
        if r.target.startswith("relationship:")
    }
    assert surviving == {(rel, "run-B")}, "only the re-run's own relationship row is cleared"


def test_empty_table_set_is_a_noop(session: Session) -> None:
    """An empty scope clears nothing (and never touches the DB)."""
    session.add(Source(source_id="src_y", name="src_y", source_type="csv"))
    session.add(Table(table_id="tbl_c", source_id="src_y", table_name="tbl_c", layer="typed"))
    session.flush()
    _readiness_row(session, "tbl_c", "src_y")
    session.flush()

    assert persist_readiness(session, baseline_session_id(), []) == 0
    assert session.query(EntropyReadinessRecord).filter_by(table_id="tbl_c").count() == 1
