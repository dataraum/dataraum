"""DAT-373 — stable typed Column identity + cross-stage data survival on re-type.

Two guarantees, exercised against a real DuckLake substrate:

1. **Stable identity** — re-running ``resolve_types`` on a raw table REUSES the
   existing typed ``Table`` + ``Column`` rows (ids unchanged) instead of minting
   fresh uuid4 ids. This is what lets a teach re-type avoid orphaning another
   stage's per-Column rows.

2. **Cross-stage survival** — a simulated ``begin_session`` / frame-ground
   per-Column finding (a ``SemanticAnnotation`` here) attached to a typed column
   survives a full re-type: the typed Column id is stable, so the re-type does
   NOT cascade-drop the foreign row.

These replace the pre-DAT-373 invariant (the integration smoke asserted every
typed_table_id CHANGED on replay — proof the typed Table was dropped). The new
invariant is the opposite: typed ids are STABLE and foreign per-Column rows live.

Post-DAT-413 a teach is a full re-run, not a scoped replay: ``resolve_types``
self-cleans its own run's ``TypeDecision``/``TypeCandidate`` rows before
re-inserting, so re-typing is idempotent without a separate cleanup hook.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.semantic.db_models import SemanticAnnotation
from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.pipeline.base import PhaseStatus
from dataraum.sources.csv import CSVLoader
from dataraum.sources.csv.null_values import load_null_value_config
from dataraum.storage import Column, Source, Table
from tests.conftest import baseline_run_id


@pytest.fixture
def simple_csv(tmp_path):
    """A small CSV with a clean integer id column and a numeric amount."""
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "id,amount,note\n1,10.5,a\n2,20.0,b\n3,30.25,c\n4,40.0,d\n",
    )
    return csv_file


def _seed_source(
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    csv_path,
) -> tuple[str, str]:
    """Seed a Source row and stage a CSV via the production loader path.

    Mirrors ``import_phase`` (create the Source row, then call the file loader's
    ``_load_single_file``); returns ``(source_id, staged_table_id)``.
    """
    source_uri = f"file://{csv_path}"
    source_id = "src-replay-seed"
    source = Source(
        source_id=source_id,
        name="seed_source",
        source_type="csv",
        connection_config={"path": source_uri},
    )
    session.add(source)
    session.flush()

    loader = CSVLoader()
    load_result = loader._load_single_file(
        source_uri=source_uri,
        source_id=source_id,
        duckdb_conn=duckdb_conn,
        session=session,
        null_config=load_null_value_config(),
    )
    assert load_result.success, load_result.error
    staged = load_result.unwrap()
    return source_id, staged.table_id


def _resolve_once(
    staged_table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    run_id: str | None = None,
) -> str:
    """Infer + resolve types for a raw table, returning the typed table id."""
    run_id = run_id or baseline_run_id()
    raw_table = session.get(Table, staged_table_id)
    assert raw_table is not None
    infer = infer_type_candidates(raw_table, duckdb_conn, session, run_id=run_id)
    assert infer.success, infer.error
    session.flush()
    resolve = resolve_types(
        staged_table_id,
        duckdb_conn,
        session,
        min_confidence=0.85,
        run_id=run_id,
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
        _source_id, staged_table_id = _seed_source(duckdb_conn, session, simple_csv)

        first_typed_id = _resolve_once(staged_table_id, duckdb_conn, session)
        first_cols = _typed_column_ids(session, first_typed_id)
        assert first_cols  # sanity

        # Re-type the SAME raw table (a type_pattern teach is a full re-run of
        # typing). ``resolve_types`` self-cleans this run's prior TypeDecision /
        # TypeCandidate rows before re-inserting, so no separate cleanup hook is
        # needed (DAT-413).
        second_typed_id = _resolve_once(staged_table_id, duckdb_conn, session)
        second_cols = _typed_column_ids(session, second_typed_id)

        # The typed Table id and every typed Column id are UNCHANGED.
        assert second_typed_id == first_typed_id
        assert second_cols == first_cols


class TestRetypeDoesNotFreezeOrDestroyDateColumns:
    """The replay-poison regression (German-dates user report, 2026-06-05).

    Run A persists an ``automatic`` TypeDecision for every column. The old
    selection honored that row as a human override on run B — re-applying the
    DATE type WITHOUT its standardization expr, so DD.MM.YYYY values plain-
    TRY_CAST to NULL: a 100%-NULL typed column whose rows all quarantine, which
    eligibility then drops as all_null. Run B must instead re-decide from its
    own candidates and keep parsing.
    """

    def test_second_run_still_parses_ddmmyyyy_dates(self, duckdb_conn, session, tmp_path) -> None:
        from dataraum.analysis.typing.db_models import TypeDecision
        from dataraum.core.duckdb_naming import schema_for_layer
        from dataraum.server.storage import LAKE_CATALOG_ALIAS

        # 5 parseable DD.MM.YYYY dates + 1 regex-matching but unparseable value
        # (no leap day): parse success 5/6 ≈ 0.83 clears the 0.8 gate, the bad
        # row quarantines instead of zeroing the pattern (TRY_STRPTIME).
        csv_file = tmp_path / "bookings.csv"
        csv_file.write_text(
            "id,tag_datum\n"
            "1,15.01.2024\n2,31.12.2023\n3,29.02.2023\n4,01.06.2026\n5,07.04.2025\n6,24.12.2024\n"
        )
        _source_id, staged_table_id = _seed_source(duckdb_conn, session, csv_file)

        _resolve_once(staged_table_id, duckdb_conn, session, run_id="run-A")
        typed_id = _resolve_once(staged_table_id, duckdb_conn, session, run_id="run-B")

        # Run B's decision is its own automatic DATE — not run A's row replayed
        # as a frozen "manual" override.
        raw_date_col = session.execute(
            select(Column).where(
                Column.table_id == staged_table_id, Column.column_name == "tag_datum"
            )
        ).scalar_one()
        run_b_decision = session.execute(
            select(TypeDecision).where(
                TypeDecision.column_id == raw_date_col.column_id,
                TypeDecision.run_id == "run-B",
            )
        ).scalar_one()
        assert run_b_decision.decided_type == "DATE"
        assert run_b_decision.decision_source == "automatic"

        # The typed column still resolves DATE and still PARSES: 5 real dates,
        # only the unparseable row NULL. (The destruction path produced 0.)
        typed_col = session.execute(
            select(Column).where(Column.table_id == typed_id, Column.column_name == "tag_datum")
        ).scalar_one()
        assert typed_col.resolved_type == "DATE"

        typed_table = session.get(Table, typed_id)
        assert typed_table is not None
        fqn = f'{LAKE_CATALOG_ALIAS}.{schema_for_layer("typed")}."{typed_table.duckdb_path}"'
        (parsed,) = duckdb_conn.execute(
            f"SELECT COUNT(*) FROM {fqn} WHERE tag_datum IS NOT NULL"
        ).fetchone()
        assert parsed == 5


class TestCrossStageSurvival:
    """A foreign per-Column row survives a full re-type (stable typed Column ids)."""

    def test_semantic_annotation_survives_retype(self, harness, simple_csv) -> None:
        # Import + type the source through the harness (real substrate).
        result = harness.run_import(source_path=simple_csv, source_name="orders")
        assert result.status == PhaseStatus.COMPLETED, result.error
        result = harness.run_phase("typing")
        assert result.status == PhaseStatus.COMPLETED, result.error

        with harness.session_factory() as session:
            typed_table = session.execute(select(Table).where(Table.layer == "typed")).scalar_one()
            typed_table_id = typed_table.table_id
            id_col = session.execute(
                select(Column).where(Column.table_id == typed_table_id, Column.column_name == "id")
            ).scalar_one()
            id_col_id = id_col.column_id
            # Simulate a PRIOR add_source run's finding attached to the typed
            # column — foreign to the re-type call below, which must not
            # destructively wipe it. (DAT-637 moved catalogue/begin_session-grain
            # findings to ColumnConcept — SemanticAnnotation is add_source-grain
            # only, so 'llm' is the real value here, not a hypothetical
            # cross-stage source.)
            session.add(
                SemanticAnnotation(
                    annotation_id=str(uuid4()),
                    column_id=id_col_id,
                    semantic_role="identifier",
                    annotation_source="llm",
                    annotated_at=datetime.now(UTC),
                )
            )
            session.commit()

        # Genuinely re-type the raw table (a type_pattern teach is a full re-run
        # of typing). ``resolve_types`` reuses the stable typed Column ids and
        # self-cleans only this run's own rows, so the re-type does not
        # cascade-drop the foreign annotation.
        with harness.session_factory() as session:
            raw_table_id = session.execute(
                select(Table.table_id).where(Table.layer == "raw")
            ).scalar_one()
            _resolve_once(raw_table_id, harness.duckdb_conn, session)
            session.commit()

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
