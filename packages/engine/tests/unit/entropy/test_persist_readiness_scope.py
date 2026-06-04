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


def _readiness_row(session: Session, table_id: str) -> None:
    session.add(
        EntropyReadinessRecord(
            session_id=baseline_session_id(),
            target=f"table:{table_id}",
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
    _readiness_row(session, "tbl_a")
    _readiness_row(session, "tbl_b")
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
    no table_id/column_id."""
    session.add(
        EntropyReadinessRecord(
            session_id=baseline_session_id(),
            target=target,
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
    _readiness_row(session, "tbl_c")
    session.flush()

    assert persist_readiness(session, baseline_session_id(), []) == 0
    assert session.query(EntropyReadinessRecord).filter_by(table_id="tbl_c").count() == 1


def test_table_grain_readiness_round_trip(session: Session) -> None:
    """A table-scoped dimension_coverage object rolls up to a banded ``table:`` row (DAT-415).

    Proves the P4 round-trip: a ``table:`` entropy object rolls up the network
    (dimension_coverage → query/reporting intents), persists with the table FK and
    NO column FK, and ``load_table_readiness`` reads it back via the session head.
    """
    from datetime import UTC, datetime

    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.entropy.views.readiness_context import load_table_readiness
    from dataraum.storage import MetadataSnapshotHead, session_head_target

    session.add(Source(source_id="src_t", name="src_t", source_type="csv"))
    session.add(Table(table_id="fact_t", source_id="src_t", table_name="orders", layer="typed"))
    session.flush()
    # A high coverage-gap measurement at table grain (semantic.coverage.dimension_coverage
    # maps to the dimension_coverage network node → query/reporting intent risk).
    session.add(
        EntropyObjectRecord(
            session_id=baseline_session_id(),
            layer="semantic",
            dimension="coverage",
            sub_dimension="dimension_coverage",
            target="table:orders",
            table_id="fact_t",
            column_id=None,
            run_id="run-1",
            score=0.8,
            detector_id="dimension_coverage",
        )
    )
    session.flush()

    written = persist_readiness(session, baseline_session_id(), ["fact_t"], run_id="run-1")
    session.flush()
    assert written >= 1

    rows = [r for r in session.query(EntropyReadinessRecord).all() if r.target == "table:orders"]
    assert len(rows) == 1
    row = rows[0]
    assert (row.table_id, row.column_id, row.run_id) == ("fact_t", None, "run-1")
    assert row.band in ("investigate", "blocked"), "a 0.8 coverage gap is not 'ready'"

    # Reader resolves the current run via the session head (begin_session seals there).
    session.add(
        MetadataSnapshotHead(
            target=session_head_target(baseline_session_id()),
            stage="detect",
            run_id="run-1",
            promoted_at=datetime.now(UTC),
            version=0,
        )
    )
    session.flush()
    assert [r.target for r in load_table_readiness(session, baseline_session_id())] == [
        "table:orders"
    ]
