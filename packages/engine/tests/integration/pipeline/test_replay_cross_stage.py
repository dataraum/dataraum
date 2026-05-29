"""DAT-373 — stable typed Column identity + cross-stage data survival on replay.

Two guarantees, exercised against a real DuckLake substrate:

1. **Stable identity** — re-running ``resolve_types`` on a raw table REUSES the
   existing typed ``Table`` + ``Column`` rows (ids unchanged) instead of minting
   fresh uuid4 ids. This is what lets a teach re-type avoid orphaning another
   stage's per-Column rows.

2. **Cross-stage survival** — a simulated ``begin_session`` / frame-ground
   per-Column finding (a ``SemanticAnnotation`` here) attached to a typed column
   survives a ``type_pattern``-style re-type: typing's ``replay_cleanup`` clears
   only its own rows in place, and the downstream analytics phases self-clean
   only their own rows.

These replace the pre-DAT-373 invariant (the integration smoke asserted every
typed_table_id CHANGED on replay — proof the typed Table was dropped). The new
invariant is the opposite: typed ids are STABLE and foreign per-Column rows live.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.statistics.db_models import StatisticalProfile
from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.core.models import SourceConfig
from dataraum.pipeline.base import PhaseStatus
from dataraum.pipeline.phases.statistics_phase import StatisticsPhase
from dataraum.pipeline.phases.typing_phase import TypingPhase
from dataraum.sources.csv import CSVLoader
from dataraum.storage import Column, Table
from tests.conftest import baseline_session_id


@pytest.fixture
def simple_csv(tmp_path):
    """A small CSV with a clean integer id column and a numeric amount."""
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "id,amount,note\n1,10.5,a\n2,20.0,b\n3,30.25,c\n4,40.0,d\n",
    )
    return csv_file


def _resolve_once(
    staged_table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
) -> str:
    """Infer + resolve types for a raw table, returning the typed table id."""
    raw_table = session.get(Table, staged_table_id)
    assert raw_table is not None
    infer = infer_type_candidates(
        raw_table, duckdb_conn, session, session_id=baseline_session_id()
    )
    assert infer.success, infer.error
    session.flush()
    resolve = resolve_types(
        staged_table_id,
        duckdb_conn,
        session,
        min_confidence=0.85,
        session_id=baseline_session_id(),
    )
    assert resolve.success, resolve.error
    return resolve.unwrap().typed_table_id


def _typed_column_ids(session: Session, typed_table_id: str) -> dict[str, str]:
    rows = session.execute(
        select(Column.column_name, Column.column_id).where(Column.table_id == typed_table_id)
    ).all()
    return dict(rows)


class TestStableTypedIdentity:
    """Re-typing reuses the typed Table + Column rows (ids unchanged)."""

    def test_second_resolve_keeps_table_and_column_ids(
        self, simple_csv, duckdb_conn, session
    ) -> None:
        loader = CSVLoader()
        config = SourceConfig(name="orders", source_type="csv", path=str(simple_csv))
        load_result = loader.load(config, duckdb_conn, session)
        assert load_result.success
        staging = load_result.unwrap()
        staged = staging.tables[0]

        first_typed_id = _resolve_once(staged.table_id, duckdb_conn, session)
        first_cols = _typed_column_ids(session, first_typed_id)
        assert first_cols  # sanity

        # Re-type the SAME raw table (a type_pattern teach re-runs typing).
        # Clear typing's own rows in place first, exactly as the replay does.
        TypingPhase().replay_cleanup(
            _CleanupCtx(session, duckdb_conn, staging.source_id),
            [staged.table_id],
        )
        second_typed_id = _resolve_once(staged.table_id, duckdb_conn, session)
        second_cols = _typed_column_ids(session, second_typed_id)

        # The typed Table id and every typed Column id are UNCHANGED.
        assert second_typed_id == first_typed_id
        assert second_cols == first_cols


class _CleanupCtx:
    """Minimal PhaseContext stand-in for invoking replay_cleanup directly.

    ``replay_cleanup`` only reads ``session`` / ``duckdb_conn`` / ``source_id``
    plus ``_typed_tables`` (which uses ``source_id`` + ``table_ids``); the rest
    of ``PhaseContext`` is unused by the cleanup path.
    """

    def __init__(self, session, duckdb_conn, source_id) -> None:
        self.session = session
        self.duckdb_conn = duckdb_conn
        self.source_id = source_id
        self.table_ids: list[str] = []
        self.config: dict = {}
        self.session_id = baseline_session_id()


class TestCrossStageSurvival:
    """A foreign per-Column row survives a re-type through the phase chain."""

    def test_semantic_annotation_survives_retype(self, harness, simple_csv) -> None:
        # Import + type the source through the harness (real substrate).
        result = harness.run_import(source_path=simple_csv, source_name="orders")
        assert result.status == PhaseStatus.COMPLETED, result.error
        result = harness.run_phase("typing")
        assert result.status == PhaseStatus.COMPLETED, result.error
        result = harness.run_phase("statistics")
        assert result.status == PhaseStatus.COMPLETED, result.error

        with harness.session_factory() as session:
            typed_table = session.execute(
                select(Table).where(Table.layer == "typed")
            ).scalar_one()
            typed_table_id = typed_table.table_id
            id_col = session.execute(
                select(Column).where(
                    Column.table_id == typed_table_id, Column.column_name == "id"
                )
            ).scalar_one()
            id_col_id = id_col.column_id
            # Simulate a begin_session / frame-ground finding attached to the
            # typed column — a stage NOT in the add_source chain.
            session.add(
                SemanticAnnotation(
                    annotation_id=str(uuid4()),
                    column_id=id_col_id,
                    semantic_role="identifier",
                    annotation_source="teach",
                    annotated_at=datetime.now(UTC),
                )
            )
            session.commit()

        # Re-type the table (type_pattern teach) + re-run statistics, mirroring
        # the workflow's replay: each phase self-cleans only its own rows.
        raw_table_id = None
        with harness.session_factory() as session:
            raw_table_id = session.execute(
                select(Table.table_id).where(Table.layer == "raw")
            ).scalar_one()

        with harness.session_factory() as session:
            TypingPhase().replay_cleanup(
                _CleanupCtx(session, harness.duckdb_conn, harness.source_id),
                [raw_table_id],
            )
            session.commit()
        result = harness.run_phase("typing", table_ids=[raw_table_id])
        assert result.status == PhaseStatus.COMPLETED, result.error

        with harness.session_factory() as session:
            StatisticsPhase().replay_cleanup(
                _CleanupCtx(session, harness.duckdb_conn, harness.source_id),
                [typed_table_id],
            )
            session.commit()
        result = harness.run_phase("statistics", table_ids=[typed_table_id])
        assert result.status == PhaseStatus.COMPLETED, result.error

        with harness.session_factory() as session:
            # The typed column id is stable AND the foreign annotation survives.
            surviving_col = session.execute(
                select(Column.column_id).where(
                    Column.table_id == typed_table_id, Column.column_name == "id"
                )
            ).scalar_one()
            assert surviving_col == id_col_id
            annotation = session.execute(
                select(SemanticAnnotation).where(SemanticAnnotation.column_id == id_col_id)
            ).first()
            assert annotation is not None, "re-type wiped a foreign per-Column row"
            # Statistics rebuilt its own rows (owner-scoped self-clean → re-run).
            profiles = session.execute(
                select(StatisticalProfile)
                .join(Column, Column.column_id == StatisticalProfile.column_id)
                .where(Column.table_id == typed_table_id)
            ).all()
            assert profiles, "statistics did not rebuild after re-type"
