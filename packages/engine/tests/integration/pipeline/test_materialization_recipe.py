"""DAT-414 — versioned typed/quarantine materialization DDL (round-trip + reset).

Exercised against a real DuckLake substrate, mirroring ``test_replay_cross_stage``:

1. **Persist + stamp** (AC#1) — after typing, the typed AND quarantine
   ``CREATE TABLE`` DDL strings are stored as ``MaterializationRecipe`` rows
   stamped with the run's ``run_id``, keyed on the *typed* Table id.

2. **Round-trip** (AC#2) — DROPping the physical typed table then re-executing
   the stored DDL via ``rebuild_from_recipe`` recreates the DuckDB table
   identically (same rows, same types).

3. **Reset-to-prior-run** (AC#3) — after a second run re-types the table under a
   fresh ``run_id`` (different decided types → different DDL), ``reset_to_run``
   flips the typing snapshot head back to the first run and re-materializes the
   physical artifact from that run's stored DDL — WITHOUT re-running typing.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.analysis.typing.recipe import (
    current_typing_run,
    rebuild_from_recipe,
    reset_to_run,
)
from dataraum.sources.csv import CSVLoader
from dataraum.sources.csv.null_values import load_null_value_config
from dataraum.storage import Source, Table
from tests.conftest import baseline_session_id


@pytest.fixture
def simple_csv(tmp_path):
    """A CSV whose ``amount`` is overwhelmingly numeric with ONE cast-failing row.

    9/10 ``amount`` values parse as numeric, so the column resolves to a numeric
    type (above the 0.85 confidence threshold); the lone ``"oops"`` then fails
    the resolved-type TRY_CAST and lands in quarantine — so the quarantine table
    is non-empty and the round-trip proves BOTH artifacts rebuild.
    """
    csv_file = tmp_path / "orders.csv"
    rows = "\n".join(f"{i},{i * 10}.0" for i in range(1, 10))
    csv_file.write_text(f"id,amount\n{rows}\n10,oops\n")
    return csv_file


def _seed_source(
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    csv_path,
) -> str:
    """Seed a Source row + stage a CSV via the production loader; return raw table id."""
    source_uri = f"file://{csv_path}"
    source_id = f"src-recipe-{uuid4().hex[:8]}"
    session.add(
        Source(
            source_id=source_id,
            name=f"recipe_source_{uuid4().hex[:8]}",
            source_type="csv",
            connection_config={"path": source_uri},
        )
    )
    session.flush()

    loader = CSVLoader()
    load_result = loader._load_single_file(
        source_uri=source_uri,
        source_id=source_id,
        source_name=session.get(Source, source_id).name,
        duckdb_conn=duckdb_conn,
        session=session,
        null_config=load_null_value_config(),
    )
    assert load_result.success, load_result.error
    return load_result.unwrap().table_id


def _resolve(
    raw_table_id: str,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    run_id: str,
) -> str:
    """Infer + resolve types under ``run_id``; return the typed table id."""
    raw_table = session.get(Table, raw_table_id)
    assert raw_table is not None
    infer = infer_type_candidates(
        raw_table, duckdb_conn, session, session_id=baseline_session_id(), run_id=run_id
    )
    assert infer.success, infer.error
    session.flush()
    resolve = resolve_types(
        raw_table_id,
        duckdb_conn,
        session,
        min_confidence=0.85,
        session_id=baseline_session_id(),
        run_id=run_id,
    )
    assert resolve.success, resolve.error
    return resolve.unwrap().typed_table_id


def _recipes(
    session: Session, typed_table_id: str, run_id: str
) -> dict[str, MaterializationRecipe]:
    """The stored recipes for a typed table at a run, keyed by produced layer."""
    rows = session.execute(
        select(MaterializationRecipe).where(
            MaterializationRecipe.table_id == typed_table_id,
            MaterializationRecipe.run_id == run_id,
        )
    ).scalars()
    return {r.layer: r for r in rows}


def _quarantine_table_id(session: Session, raw_table_id: str) -> str:
    """The quarantine Table id sharing this raw table's (source, name)."""
    raw = session.get(Table, raw_table_id)
    assert raw is not None
    q = session.execute(
        select(Table).where(
            Table.source_id == raw.source_id,
            Table.table_name == raw.table_name,
            Table.layer == "quarantine",
        )
    ).scalar_one()
    return q.table_id


def _rows(duckdb_conn: duckdb.DuckDBPyConnection, fqn: str) -> list[tuple]:
    """Data rows of a table, cast to text so DuckDB never materializes a TIMESTAMP.

    ``SELECT *`` over a quarantine table (which carries ``_quarantined_at
    TIMESTAMP``) would pull a Python ``datetime`` through DuckDB's optional
    ``pytz`` path; casting every column to VARCHAR keeps the comparison a pure
    string round-trip and dependency-free.

    ``_quarantined_at`` is excluded: the quarantine DDL stamps it via
    ``CURRENT_TIMESTAMP``, so a faithful re-execution of the *same* recipe
    deliberately produces a fresh timestamp — the DATA round-trips identically,
    the audit clock advances.
    """
    cols = [
        c[0]
        for c in duckdb_conn.execute(f"DESCRIBE SELECT * FROM {fqn}").fetchall()
        if c[0] != "_quarantined_at"
    ]
    select_list = ", ".join(f'CAST("{c}" AS VARCHAR)' for c in cols)
    return sorted(duckdb_conn.execute(f"SELECT {select_list} FROM {fqn}").fetchall(), key=str)


def _column_types(duckdb_conn: duckdb.DuckDBPyConnection, fqn: str) -> dict[str, str]:
    """name -> DuckDB type for a materialized table (proves the cast survived)."""
    rows = duckdb_conn.execute(f"DESCRIBE SELECT * FROM {fqn}").fetchall()
    return {r[0]: r[1] for r in rows}


class TestRecipePersistedAndStamped:
    """AC#1 — typed + quarantine DDL persisted, stamped with run_id."""

    def test_typing_stores_run_stamped_recipes(self, simple_csv, duckdb_conn, session) -> None:
        raw_id = _seed_source(duckdb_conn, session, simple_csv)
        run_id = "run-A"
        typed_id = _resolve(raw_id, duckdb_conn, session, run_id)
        quarantine_id = _quarantine_table_id(session, raw_id)

        typed_recipes = _recipes(session, typed_id, run_id)
        assert "typed" in typed_recipes
        typed_recipe = typed_recipes["typed"]
        assert typed_recipe.run_id == run_id
        assert typed_recipe.ddl.startswith("CREATE OR REPLACE TABLE")
        assert "AS SELECT" in typed_recipe.ddl
        assert typed_recipe.depends_on  # reads the raw layer

        quarantine_recipes = _recipes(session, quarantine_id, run_id)
        assert "quarantine" in quarantine_recipes
        assert quarantine_recipes["quarantine"].run_id == run_id
        assert quarantine_recipes["quarantine"].ddl.startswith("CREATE OR REPLACE TABLE")


class TestRoundTrip:
    """AC#2 — re-executing stored DDL rebuilds the physical table identically."""

    def test_rebuild_recreates_typed_and_quarantine(self, simple_csv, duckdb_conn, session) -> None:
        raw_id = _seed_source(duckdb_conn, session, simple_csv)
        run_id = "run-A"
        typed_id = _resolve(raw_id, duckdb_conn, session, run_id)
        quarantine_id = _quarantine_table_id(session, raw_id)

        typed_fqn = _recipes(session, typed_id, run_id)["typed"].target_fqn
        quarantine_fqn = _recipes(session, quarantine_id, run_id)["quarantine"].target_fqn

        # Snapshot the freshly-materialized state.
        typed_rows_before = _rows(duckdb_conn, typed_fqn)
        typed_types_before = _column_types(duckdb_conn, typed_fqn)
        quarantine_rows_before = _rows(duckdb_conn, quarantine_fqn)

        # DROP both physical artifacts — the lake is now missing them.
        duckdb_conn.execute(f"DROP TABLE {typed_fqn}")
        duckdb_conn.execute(f"DROP TABLE {quarantine_fqn}")

        # Rebuild each from its stored DDL — no re-typing.
        rebuilt_typed = rebuild_from_recipe(session, duckdb_conn, table_id=typed_id, run_id=run_id)
        rebuilt_quar = rebuild_from_recipe(
            session, duckdb_conn, table_id=quarantine_id, run_id=run_id
        )
        assert typed_fqn in rebuilt_typed
        assert quarantine_fqn in rebuilt_quar

        # Identical rows AND types after the round-trip.
        assert _rows(duckdb_conn, typed_fqn) == typed_rows_before
        assert _column_types(duckdb_conn, typed_fqn) == typed_types_before
        assert _rows(duckdb_conn, quarantine_fqn) == quarantine_rows_before
        # The cast-failing row ("oops") landed in quarantine, so it is non-empty.
        assert len(quarantine_rows_before) >= 1


class TestResetToPriorRun:
    """AC#3 — reset rebuilds from a prior run's DDL without re-deriving typing."""

    def test_reset_flips_head_and_rematerializes(self, simple_csv, duckdb_conn, session) -> None:
        raw_id = _seed_source(duckdb_conn, session, simple_csv)

        # Run A produces the typed artifact + stores its recipe.
        typed_id = _resolve(raw_id, duckdb_conn, session, "run-A")
        typed_fqn = _recipes(session, typed_id, "run-A")["typed"].target_fqn
        rows_run_a = _rows(duckdb_conn, typed_fqn)
        types_run_a = _column_types(duckdb_conn, typed_fqn)
        assert rows_run_a  # sanity: the artifact has data

        # Diverge the PHYSICAL table from run A — as if a later run (or a manual
        # edit) replaced it. The lake is latest-only, so "reset" must restore run
        # A's artifact purely from its stored DDL.
        duckdb_conn.execute(f"DELETE FROM {typed_fqn}")
        assert _rows(duckdb_conn, typed_fqn) == []
        # Point the head elsewhere so we can prove reset flips it back.
        from dataraum.analysis.typing.recipe import _point_head

        _point_head(session, typed_id, "run-Z")
        session.flush()
        assert current_typing_run(session, typed_id) == "run-Z"

        # Reset to run A — re-executes run A's stored DDL + flips the typing head.
        # Crucially: NO infer/resolve call here, so this is NOT a re-derivation —
        # the versioned recipe is the only source of truth for the rebuild.
        rebuilt = reset_to_run(session, duckdb_conn, table_id=typed_id, run_id="run-A")
        assert typed_fqn in rebuilt

        # The physical artifact is restored to run A (rows + types) and the typing
        # head now names run A.
        assert _rows(duckdb_conn, typed_fqn) == rows_run_a
        assert _column_types(duckdb_conn, typed_fqn) == types_run_a
        assert current_typing_run(session, typed_id) == "run-A"

    def test_two_real_runs_coexist_then_reset(self, simple_csv, duckdb_conn, session) -> None:
        """A real second typing run coexists with the first; reset replays run A.

        Exercises the DAT-373 (stable typed id) × DAT-414 (run-versioned recipe)
        interaction end-to-end: re-typing the SAME raw table under a fresh run_id
        reuses the typed Table id but stores a SEPARATE recipe row — both runs'
        recipes coexist, and a reset re-executes the older run's stored DDL.
        """
        raw_id = _seed_source(duckdb_conn, session, simple_csv)

        typed_id = _resolve(raw_id, duckdb_conn, session, "run-A")
        typed_fqn = _recipes(session, typed_id, "run-A")["typed"].target_fqn
        rows_run_a = _rows(duckdb_conn, typed_fqn)

        # Re-type the SAME raw table under a new run — stable typed id (DAT-373),
        # but a coexisting recipe under run-B (DAT-414).
        typed_id_b = _resolve(raw_id, duckdb_conn, session, "run-B")
        assert typed_id_b == typed_id

        assert _recipes(session, typed_id, "run-A")["typed"].run_id == "run-A"
        assert _recipes(session, typed_id, "run-B")["typed"].run_id == "run-B"

        # Corrupt the live artifact, then reset to run A purely from its recipe.
        duckdb_conn.execute(f"DELETE FROM {typed_fqn}")
        reset_to_run(session, duckdb_conn, table_id=typed_id, run_id="run-A")
        assert _rows(duckdb_conn, typed_fqn) == rows_run_a
        assert current_typing_run(session, typed_id) == "run-A"
