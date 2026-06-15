"""Column eligibility phase against a real substrate.

Pins the deletion of the key-name hard abort (German-dates user report,
2026-06-05): the phase used to ``PhaseResult.failed`` the ENTIRE run when an
INELIGIBLE (100%-null) column's NAME matched ``_id$``/``^id$``/``_key$`` —
name-only key inference with no structural evidence and no teach escape
hatch. An all-null key-named column is now dropped + recorded INELIGIBLE
like any other, and the run continues; key-ness claims live downstream where
relationship evidence exists.

Also pins the DAT-504 lake-convergent drop sequence against the real DuckLake
fixture: rebuild-from-recipe → one-shot quarantine replace → column drop, plus
the DuckLake capability assumption it relies on (``ALTER TABLE … DROP COLUMN``
on a DuckLake table; fallback if it ever regresses: ``CREATE OR REPLACE …
EXCLUDE`` from the recipe-rebuilt table — see the DAT-504 refine notes).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from sqlalchemy import select

from dataraum.analysis.eligibility.db_models import ColumnEligibilityRecord
from dataraum.analysis.eligibility.evaluator import quarantine_and_drop_columns
from dataraum.analysis.typing.db_models import MaterializationRecipe
from dataraum.pipeline.base import PhaseStatus
from dataraum.storage import Column, Table
from tests.conftest import baseline_run_id


@pytest.fixture
def csv_with_all_null_id_column(tmp_path):
    """An optional FK column (``logikal30_id``) that is 100% empty in this extract."""
    csv_file = tmp_path / "positions.csv"
    csv_file.write_text(
        "id,logikal30_id,amount\n1,,10.5\n2,,20.0\n3,,30.25\n4,,40.0\n",
    )
    return csv_file


def test_all_null_key_named_column_drops_without_failing_the_run(
    harness, csv_with_all_null_id_column
) -> None:
    result = harness.run_import(source_path=csv_with_all_null_id_column, source_name="positions")
    assert result.status == PhaseStatus.COMPLETED, result.error
    result = harness.run_phase("typing")
    assert result.status == PhaseStatus.COMPLETED, result.error
    result = harness.run_phase("statistics")
    assert result.status == PhaseStatus.COMPLETED, result.error

    # The old behavior: PhaseResult.failed("Critical column 'logikal30_id' …
    # Cannot proceed with unusable key column.") — the whole run died.
    result = harness.run_phase("column_eligibility")
    assert result.status == PhaseStatus.COMPLETED, result.error
    assert result.outputs is not None
    assert result.outputs["dropped"] == 1

    with harness.session_factory() as session:
        # Recorded INELIGIBLE like any other all-null column…
        record = session.execute(
            select(ColumnEligibilityRecord).where(
                ColumnEligibilityRecord.column_name == "logikal30_id"
            )
        ).scalar_one()
        assert record.status == "INELIGIBLE"
        assert record.triggered_rule == "all_null"

        # …dropped from the typed table; the usable columns survive.
        typed_table = session.execute(select(Table).where(Table.layer == "typed")).scalar_one()
        typed_names = set(
            session.execute(
                select(Column.column_name).where(Column.table_id == typed_table.table_id)
            ).scalars()
        )
        assert "logikal30_id" not in typed_names
        assert {"id", "amount"} <= typed_names


def _typed_table_and_recipe(harness):
    """The typed table's (bare name, full-column recipe DDL, all-null Column)."""
    # expire_on_commit=False keeps .column_name accessible after session close
    with harness.session_factory() as session:
        typed_table = session.execute(select(Table).where(Table.layer == "typed")).scalar_one()
        recipe = session.execute(
            select(MaterializationRecipe).where(
                MaterializationRecipe.table_id == typed_table.table_id,
                MaterializationRecipe.layer == "typed",
            )
        ).scalar_one()
        column = session.execute(
            select(Column).where(
                Column.table_id == typed_table.table_id,
                Column.column_name == "logikal30_id",
            )
        ).scalar_one()
        return typed_table.duckdb_path, recipe.ddl, column


def _lake_state(harness, bare: str) -> tuple[list[str], list[tuple], list[str]]:
    """(typed columns, quarantine rows, quarantine-table placements) for ``bare``."""
    typed_cols = [
        r[0] for r in harness.duckdb_conn.execute(f'DESCRIBE lake.typed."{bare}"').fetchall()
    ]
    quarantine_rows = harness.duckdb_conn.execute(
        "SELECT _row_id, _column_name, _value, _quarantine_reason "
        f'FROM lake.quarantine."quarantine_columns_{bare}" ORDER BY _row_id'
    ).fetchall()
    placements = [
        r[0]
        for r in harness.duckdb_conn.execute(
            "SELECT schema_name FROM duckdb_tables() WHERE database_name = 'lake' "
            f"AND table_name = 'quarantine_columns_{bare}'"
        ).fetchall()
    ]
    return typed_cols, quarantine_rows, placements


class TestLakeConvergence:
    """DAT-504: the eligibility lake body converges under at-least-once redelivery."""

    def _import_type_profile(self, harness, csv_path) -> None:
        result = harness.run_import(source_path=csv_path, source_name="positions")
        assert result.status == PhaseStatus.COMPLETED, result.error
        for step in ("typing", "statistics"):
            result = harness.run_phase(step)
            assert result.status == PhaseStatus.COMPLETED, result.error

    def test_eligibility_body_executed_twice_converges(
        self, harness, csv_with_all_null_id_column
    ) -> None:
        """The lake body run twice under the same run grain lands on identical
        state — proving DuckLake's ALTER DROP COLUMN and the rebuild-replace-drop
        sequence (the old append+swallow path duplicated quarantine rows and
        errored on the second drop)."""
        self._import_type_profile(harness, csv_with_all_null_id_column)
        bare, recipe_ddl, column = _typed_table_and_recipe(harness)

        states = []
        for _ in range(2):
            quarantine_and_drop_columns(
                harness.duckdb_conn,
                bare,
                [(column, "Column has 100% null values")],
                typed_recipe_ddl=recipe_ddl,
            )
            states.append(_lake_state(harness, bare))

        first, second = states
        assert first == second
        typed_cols, quarantine_rows, placements = second
        assert "logikal30_id" not in typed_cols
        assert {"id", "amount"} <= set(typed_cols)
        # One row per source row for the single quarantined column — 4, not 8.
        assert len(quarantine_rows) == 4
        assert {r[1] for r in quarantine_rows} == {"logikal30_id"}
        # Relocated to lake.quarantine — the old unqualified CREATE landed in
        # whatever schema the connection USEd (lake.typed in production).
        assert placements == ["quarantine"]

    def test_phase_redelivery_after_lake_mutation_converges(
        self, harness, csv_with_all_null_id_column
    ) -> None:
        """The dangerous interleaving: the lake body already ran but the
        metadata commit was lost; the redelivered phase re-evaluates against
        full Column metadata and must converge — the old path swallowed the
        failing ALTER as a warning and left appended quarantine duplicates."""
        self._import_type_profile(harness, csv_with_all_null_id_column)
        bare, recipe_ddl, column = _typed_table_and_recipe(harness)

        # First delivery's lake mutations (metadata untouched).
        quarantine_and_drop_columns(
            harness.duckdb_conn,
            bare,
            [(column, "Column has 100% null values")],
            typed_recipe_ddl=recipe_ddl,
        )

        # Redelivery: the full phase against unchanged metadata.
        result = harness.run_phase("column_eligibility")
        assert result.status == PhaseStatus.COMPLETED, result.error
        assert result.outputs is not None
        assert result.outputs["dropped"] == 1
        assert not result.warnings, f"swallowed lake failure: {result.warnings}"

        typed_cols, quarantine_rows, placements = _lake_state(harness, bare)
        assert "logikal30_id" not in typed_cols
        assert len(quarantine_rows) == 4
        assert placements == ["quarantine"]

        # Metadata-side convergence: exactly ONE eligibility record for the
        # dropped column — guards the NULL-run_id NULLS-DISTINCT
        # duplicate-insert hazard on redelivery.
        with harness.session_factory() as session:
            records = (
                session.execute(
                    select(ColumnEligibilityRecord).where(
                        ColumnEligibilityRecord.column_name == "logikal30_id"
                    )
                )
                .scalars()
                .all()
            )
        assert len(records) == 1

    def test_quoted_column_name_survives_quarantine_and_drop(self, harness) -> None:
        """Column names can legitimately contain quotes (CSV headers, MSSQL);
        the quarantine SELECT's literal and the CAST/DROP identifiers must
        each be escaped for their context — and stay convergent on a re-run."""
        bare = "positions_quoted"
        quoted_col = 'it\'s "qty"'
        ident = quoted_col.replace('"', '""')
        recipe_ddl = (
            f'CREATE OR REPLACE TABLE lake.typed."{bare}" AS '
            f"SELECT * FROM (VALUES (1, 'a'), (2, 'b')) t(id, \"{ident}\")"
        )
        harness.duckdb_conn.execute(recipe_ddl)
        # The function only reads .column_name — a stand-in Column suffices.
        column = SimpleNamespace(column_name=quoted_col)

        for _ in range(2):
            quarantine_and_drop_columns(
                harness.duckdb_conn,
                bare,
                [(column, "Column has 100% null values")],
                typed_recipe_ddl=recipe_ddl,
            )

        typed_cols, quarantine_rows, placements = _lake_state(harness, bare)
        assert typed_cols == ["id"]
        assert len(quarantine_rows) == 2
        assert {r[1] for r in quarantine_rows} == {quoted_col}
        assert placements == ["quarantine"]

    def test_missing_recipe_fails_loud(self, harness, csv_with_all_null_id_column) -> None:
        """No stored typed recipe at the run grain = fail the phase, no
        fallback drop — convergence depends on the rebuild step."""
        self._import_type_profile(harness, csv_with_all_null_id_column)

        with harness.session_factory() as session:
            typed_table = session.execute(select(Table).where(Table.layer == "typed")).scalar_one()
            recipe = session.execute(
                select(MaterializationRecipe).where(
                    MaterializationRecipe.table_id == typed_table.table_id,
                    MaterializationRecipe.layer == "typed",
                    MaterializationRecipe.run_id == baseline_run_id(),
                )
            ).scalar_one()
            session.delete(recipe)
            session.commit()

        result = harness.run_phase("column_eligibility")
        assert result.status == PhaseStatus.FAILED
        assert result.error is not None
        assert "materialization recipe" in result.error
