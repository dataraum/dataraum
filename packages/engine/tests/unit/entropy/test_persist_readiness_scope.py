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
from tests.conftest import baseline_run_id


def _readiness_row(session: Session, table_id: str) -> None:
    session.add(
        EntropyReadinessRecord(
            target=f"table:{table_id}",
            table_id=table_id,
            column_id=None,
            run_id=baseline_run_id(),
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
    persist_readiness(session, ["tbl_a"], run_id=baseline_run_id())
    session.flush()

    remaining = {r.table_id for r in session.query(EntropyReadinessRecord).all()}
    assert remaining == {"tbl_b"}, "sibling table's readiness must survive a per-table replay"


def _relationship_row(session: Session, target: str, run_id: str) -> None:
    """A relationship-granularity readiness row (DAT-408): identity in ``target``,
    no table_id/column_id."""
    session.add(
        EntropyReadinessRecord(
            target=target,
            table_id=None,
            column_id=None,
            run_id=run_id,
            band="investigate",
            worst_intent_risk=0.5,
        )
    )


def test_relationship_rows_delete_is_run_scoped(session: Session) -> None:
    """A re-run clears only its OWN relationship readiness; a prior run survives.

    Relationship rows carry no ``table_id``, so the column delete (by table set)
    can't reach them — they're cleared by the separate ``(run_id, relationship:%)``
    scope (DAT-408). A re-run under a fresh run_id must leave the earlier run's
    relationship rows intact (non-destructive, mirrors DAT-413).
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
    persist_readiness(session, ["tbl_z"], run_id="run-A")
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

    assert persist_readiness(session, []) == 0
    assert session.query(EntropyReadinessRecord).filter_by(table_id="tbl_c").count() == 1


def test_table_grain_readiness_round_trip(session: Session) -> None:
    """A table-scoped dimension_coverage object rolls up to a banded ``table:`` row (DAT-415).

    Proves the P4 round-trip: a ``table:`` entropy object rolls up the network
    (dimension_coverage → query/reporting intents), persists with the table FK and
    NO column FK, and ``load_table_readiness`` reads it back via the catalog head.
    """
    from datetime import UTC, datetime

    from dataraum.entropy.db_models import EntropyObjectRecord
    from dataraum.entropy.views.readiness_context import load_table_readiness
    from dataraum.storage import MetadataSnapshotHead, catalog_head_target

    session.add(Source(source_id="src_t", name="src_t", source_type="csv"))
    session.add(Table(table_id="fact_t", source_id="src_t", table_name="orders", layer="typed"))
    session.flush()
    # A high coverage-gap measurement at table grain (semantic.coverage.dimension_coverage
    # maps to the dimension_coverage network node → query/reporting intent risk).
    session.add(
        EntropyObjectRecord(
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

    written = persist_readiness(session, ["fact_t"], run_id="run-1")
    session.flush()
    assert written >= 1

    rows = [r for r in session.query(EntropyReadinessRecord).all() if r.target == "table:orders"]
    assert len(rows) == 1
    row = rows[0]
    assert (row.table_id, row.column_id, row.run_id) == ("fact_t", None, "run-1")
    assert row.band in ("investigate", "blocked"), "a 0.8 coverage gap is not 'ready'"

    # Reader resolves the current run via the catalog head (begin_session seals there).
    session.add(
        MetadataSnapshotHead(
            target=catalog_head_target(),
            stage="catalog",
            run_id="run-1",
            promoted_at=datetime.now(UTC),
        )
    )
    session.flush()
    assert [r.target for r in load_table_readiness(session)] == ["table:orders"]


class TestRunResolvedLoad:
    """Per (target, detector): current run > catalog head > table heads (review C2).

    temporal_behavior is the first detector on BOTH detect paths; a blind load
    let the stale add_source conflict outlive its session-detect re-adjudication
    via the max-score dedup — the resolution direction of the third witness.
    """

    @staticmethod
    def _record(session: Session, run_id: str | None, score: float) -> None:
        from dataraum.entropy.db_models import EntropyObjectRecord

        session.add(
            EntropyObjectRecord(
                layer="semantic",
                dimension="temporal",
                sub_dimension="temporal_behavior",
                target="column:t.debit_balance",
                table_id="fact_t",
                column_id=None,
                run_id=run_id,
                score=score,
                detector_id="temporal_behavior",
            )
        )

    def test_current_run_supersedes_table_head(self, session: Session) -> None:
        from dataraum.entropy.core.storage import EntropyRepository
        from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

        session.add(Source(source_id="src_x", name="src_x", source_type="csv"))
        session.add(Table(table_id="fact_t", source_id="src_x", table_name="t", layer="typed"))
        session.flush()
        # add_source run promoted as the table generation head: stale 2-witness C=0.9
        self._record(session, "addsource-run", 0.9)
        session.add(
            MetadataSnapshotHead(
                target="table:fact_t", stage=GENERATION_STAGE, run_id="addsource-run"
            )
        )
        # in-flight session detect: 3-witness re-adjudication resolved C=0.1
        self._record(session, "session-run", 0.1)
        session.flush()

        objects = EntropyRepository(session).load_for_tables(
            ["fact_t"], current_run_id="session-run"
        )
        assert [o.score for o in objects] == [0.1]

    def test_without_current_run_loads_blind(self, session: Session) -> None:
        from dataraum.entropy.core.storage import EntropyRepository

        session.add(Source(source_id="src_x", name="src_x", source_type="csv"))
        session.add(Table(table_id="fact_t", source_id="src_x", table_name="t", layer="typed"))
        session.flush()
        self._record(session, "a", 0.9)
        self._record(session, "b", 0.1)
        session.flush()
        objects = EntropyRepository(session).load_for_tables(["fact_t"])
        assert len(objects) == 2

    def test_catalog_head_resolves_without_current_run(self, session: Session) -> None:
        """Query time: no in-flight run, ``resolve_runs`` resolves to the catalog head."""
        from dataraum.entropy.core.storage import EntropyRepository
        from dataraum.storage import MetadataSnapshotHead, catalog_head_target
        from dataraum.storage.snapshot_head import GENERATION_STAGE

        session.add(Source(source_id="src_x", name="src_x", source_type="csv"))
        session.add(Table(table_id="fact_t", source_id="src_x", table_name="t", layer="typed"))
        session.flush()
        # add_source run promoted as the table generation head: stale 2-witness C=0.9
        self._record(session, "addsource-run", 0.9)
        session.add(
            MetadataSnapshotHead(
                target="table:fact_t", stage=GENERATION_STAGE, run_id="addsource-run"
            )
        )
        # begin_session re-adjudication, promoted as the catalog head
        self._record(session, "session-run", 0.1)
        session.add(
            MetadataSnapshotHead(
                target=catalog_head_target(),
                stage="catalog",
                run_id="session-run",
            )
        )
        session.flush()

        objects = EntropyRepository(session).load_for_tables(["fact_t"], resolve_runs=True)
        assert [o.score for o in objects] == [0.1]

    def test_legacy_unstamped_rows_rank_behind_catalog_head(self, session: Session) -> None:
        """A legacy row (run_id None) must not match the vacant in-flight slot.

        With ``current_run_id=None``, an unguarded ``record.run_id == current_run_id``
        ranks every unstamped legacy row 0 (``None == None``) and it outranks the
        catalog head — the inverse of the resolution this exists to provide.
        """
        from dataraum.entropy.core.storage import EntropyRepository
        from dataraum.storage import MetadataSnapshotHead, catalog_head_target

        session.add(Source(source_id="src_x", name="src_x", source_type="csv"))
        session.add(Table(table_id="fact_t", source_id="src_x", table_name="t", layer="typed"))
        session.flush()
        self._record(session, None, 0.9)  # legacy, pre-run-stamping
        self._record(session, "session-run", 0.1)
        session.add(
            MetadataSnapshotHead(
                target=catalog_head_target(),
                stage="catalog",
                run_id="session-run",
            )
        )
        session.flush()

        objects = EntropyRepository(session).load_for_tables(["fact_t"], resolve_runs=True)
        assert [o.score for o in objects] == [0.1]

    def test_build_for_readiness_forwards_resolution(self, session: Session) -> None:
        """``build_for_readiness`` threads the current run into the load (inert-fix regression).

        The detect-path fix added the kwargs but dropped them at the
        ``_load_entropy_objects`` call — signatures landed, resolution never ran,
        and the stale add_source score kept surfacing through the rollup.
        """
        from dataraum.entropy.views.readiness_context import build_for_readiness
        from dataraum.storage import MetadataSnapshotHead
        from dataraum.storage.snapshot_head import GENERATION_STAGE

        session.add(Source(source_id="src_x", name="src_x", source_type="csv"))
        session.add(Table(table_id="fact_t", source_id="src_x", table_name="t", layer="typed"))
        session.flush()
        self._record(session, "addsource-run", 0.9)
        session.add(
            MetadataSnapshotHead(
                target="table:fact_t", stage=GENERATION_STAGE, run_id="addsource-run"
            )
        )
        self._record(session, "session-run", 0.1)
        session.flush()

        ctx = build_for_readiness(session, ["fact_t"], current_run_id="session-run")
        scores = [ne.score for col in ctx.columns.values() for ne in col.node_evidence]
        scores += [ds.score for ds in ctx.direct_signals]
        assert scores, "the resolved load must still surface the current run's evidence"
        assert set(scores) == {0.1}, "stale add_source row must not outlive the re-adjudication"

    def test_build_column_evidence_resolves_with_resolve_runs(self, session: Session) -> None:
        """Query-time evidence resolves to the catalog head when ``resolve_runs`` is set."""
        from dataraum.entropy.views.readiness_context import build_column_evidence
        from dataraum.storage import MetadataSnapshotHead, catalog_head_target
        from dataraum.storage.snapshot_head import GENERATION_STAGE

        session.add(Source(source_id="src_x", name="src_x", source_type="csv"))
        session.add(Table(table_id="fact_t", source_id="src_x", table_name="t", layer="typed"))
        session.flush()
        self._record(session, "addsource-run", 0.9)
        session.add(
            MetadataSnapshotHead(
                target="table:fact_t", stage=GENERATION_STAGE, run_id="addsource-run"
            )
        )
        self._record(session, "session-run", 0.1)
        session.add(
            MetadataSnapshotHead(
                target=catalog_head_target(),
                stage="catalog",
                run_id="session-run",
            )
        )
        session.flush()

        ctx = build_column_evidence(session, ["fact_t"], resolve_runs=True)
        scores = [ne.score for col in ctx.columns.values() for ne in col.node_evidence]
        scores += [ds.score for ds in ctx.direct_signals]
        assert scores, "the resolved load must still surface the catalog head's evidence"
        assert set(scores) == {0.1}, "stale add_source row must not reach query-time consumers"
