"""DAT-414 — versioned typed/quarantine materialization DDL (persist + stamp).

Exercised against a real DuckLake substrate, mirroring ``test_replay_cross_stage``:

1. **Persist + stamp** (AC#1) — after typing, the typed AND quarantine
   ``CREATE TABLE`` DDL strings are stored as ``MaterializationRecipe`` rows
   stamped with the run's ``run_id``, keyed on the *typed* Table id.
"""

from __future__ import annotations

from uuid import uuid4

import duckdb
import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.analysis.typing import infer_type_candidates, resolve_types
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.pipeline.base import PhaseContext
from dataraum.pipeline.phases.typing_phase import TypingPhase
from dataraum.sources.csv import CSVLoader
from dataraum.sources.csv.null_values import load_null_value_config
from dataraum.storage import Source, Table


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
    infer = infer_type_candidates(raw_table, duckdb_conn, session, run_id=run_id)
    assert infer.success, infer.error
    session.flush()
    resolve = resolve_types(
        raw_table_id,
        duckdb_conn,
        session,
        min_confidence=0.85,
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


class TestStronglyTypedRecipe:
    """AC#1 (strongly-typed path) — the parquet/DB copy also stores a recipe."""

    def test_strongly_typed_promote_stores_recipe(self, simple_csv, duckdb_conn, session) -> None:
        """``_promote_strongly_typed`` versions its ``CREATE TABLE`` like the untyped path.

        The strongly-typed branch (parquet / typed DB sources) is a plain
        ``SELECT *`` copy with no quarantine. Parquet/DB loaders register real
        ``raw_type``s; the CSV loader stages VARCHAR, so flip one column to a
        non-VARCHAR type to model a strongly-typed source, then drive the branch
        directly and assert the recipe is persisted + run-stamped.
        """
        raw_id = _seed_source(duckdb_conn, session, simple_csv)
        raw_table = session.get(Table, raw_id)
        assert raw_table is not None
        raw_table.columns[0].raw_type = "BIGINT"
        session.flush()

        # Source-free ctx — typing runs in the source-free fan-out children
        # (DAT-422/426); the Table row's source_id is a DB field, not ctx identity.
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            source_id=None,
            table_ids=[raw_id],
            run_id="run-strong",
        )
        typed_id, _ = TypingPhase()._promote_strongly_typed(raw_table, ctx)

        recipes = _recipes(session, typed_id, "run-strong")
        assert "typed" in recipes
        assert recipes["typed"].run_id == "run-strong"
        assert recipes["typed"].ddl.startswith("CREATE OR REPLACE TABLE")
        assert "AS SELECT" in recipes["typed"].ddl
        # A strongly-typed copy can't fail a cast → no quarantine artifact/recipe.
        assert "quarantine" not in recipes
