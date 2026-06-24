"""Tests for shared SQL step execution function."""

from __future__ import annotations

import duckdb
import pytest

from dataraum.core.models.base import Result
from dataraum.query.execution import SQLStep, execute_sql_steps


@pytest.fixture
def duckdb_conn():
    """Create an in-memory DuckDB connection for testing."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE test_data (id INTEGER, name VARCHAR, value DOUBLE)")
    conn.execute("INSERT INTO test_data VALUES (1, 'Alice', 100.0)")
    conn.execute("INSERT INTO test_data VALUES (2, 'Bob', 200.0)")
    conn.execute("INSERT INTO test_data VALUES (3, 'Carol', 300.0)")
    yield conn
    conn.close()


class TestExecuteSqlSteps:
    """Tests for execute_sql_steps()."""

    def test_simple_single_step(self, duckdb_conn):
        """Single step with final SQL returns correct value."""
        steps = [
            SQLStep(
                step_id="total",
                sql="SELECT SUM(value) AS val FROM test_data",
                description="Sum all values",
            )
        ]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT val FROM total",
            duckdb_conn=duckdb_conn,
        )

        assert result.success
        assert result.value is not None
        assert result.value.final_value == 600.0
        assert len(result.value.step_results) == 1
        assert result.value.step_results[0].step_id == "total"
        assert result.value.step_results[0].value == 600.0

    def test_multiple_steps(self, duckdb_conn):
        """Multiple steps create chained views correctly."""
        steps = [
            SQLStep(
                step_id="step_count",
                sql="SELECT COUNT(*) AS cnt FROM test_data",
                description="Count rows",
            ),
            SQLStep(
                step_id="step_sum",
                sql="SELECT SUM(value) AS total FROM test_data",
                description="Sum values",
            ),
        ]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT (SELECT total FROM step_sum) / (SELECT cnt FROM step_count)",
            duckdb_conn=duckdb_conn,
        )

        assert result.success
        assert result.value is not None
        assert result.value.final_value == 200.0  # 600 / 3
        assert len(result.value.step_results) == 2

    def test_return_table_mode(self, duckdb_conn):
        """return_table=True returns columns and rows."""
        steps = []

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT id, name FROM test_data ORDER BY id",
            duckdb_conn=duckdb_conn,
            return_table=True,
        )

        assert result.success
        assert result.value is not None
        assert result.value.columns == ["id", "name"]
        assert result.value.rows is not None
        assert len(result.value.rows) == 3
        assert result.value.rows[0] == (1, "Alice")

    def test_step_failure_returns_error(self, duckdb_conn):
        """Invalid SQL in a step returns a failure result."""
        steps = [
            SQLStep(
                step_id="bad_step",
                sql="SELECT * FROM nonexistent_table",
                description="This should fail",
            )
        ]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT 1",
            duckdb_conn=duckdb_conn,
        )

        assert not result.success
        assert "bad_step" in result.error

    def test_final_sql_failure(self, duckdb_conn):
        """Invalid final SQL returns a failure result."""
        steps = [
            SQLStep(
                step_id="good_step",
                sql="SELECT 1 AS val",
                description="Good step",
            )
        ]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT * FROM nonexistent_final",
            duckdb_conn=duckdb_conn,
        )

        assert not result.success
        assert "Final SQL failed" in result.error

    def test_genuine_zero_final_passes(self, duckdb_conn):
        """A genuine 0 final value is returned, not rejected (DAT-616).

        The old guard `not final_result.value` conflated a real 0 with failure.
        A metric that legitimately computes 0 (e.g. revenue == cogs) must succeed.
        """
        steps = [SQLStep(step_id="z", sql="SELECT 0 AS val", description="zero")]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT 0 AS val",
            duckdb_conn=duckdb_conn,
        )

        assert result.success
        assert result.value is not None
        assert result.value.final_value == 0

    def test_empty_support_final_returns_null_ok_not_failure(self, duckdb_conn):
        """An empty-support final aggregates to NULL and returns ok(None) (DAT-616).

        The shared executor returns values faithfully — it no longer conflates an
        empty/NULL result with a thrown error. Degeneracy (no support) is judged
        downstream by the metric verifier, which has the catalogue + step context
        to produce a precise per-extract reason.
        """
        steps = [
            SQLStep(
                step_id="empty",
                sql="SELECT SUM(value) AS val FROM test_data WHERE name = 'nobody'",
                description="sum over an empty filter",
            )
        ]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT SUM(value) AS val FROM test_data WHERE name = 'nobody'",
            duckdb_conn=duckdb_conn,
        )

        assert result.success  # not a failure — a faithful NULL
        assert result.value is not None
        assert result.value.final_value is None
        # The per-step value is likewise None (no support) for the verifier to catch.
        assert result.value.step_results[0].value is None

    def test_no_temp_views_leak_and_composed_cte_returned(self, duckdb_conn):
        """DAT-616: execution composes ONE CTE — no temp view leaks onto the cursor,
        and the composed statement is returned as the executable artifact."""
        steps = [
            SQLStep(
                step_id="reuse_test",
                sql="SELECT 42 AS val",
                description="Test no view leak",
            )
        ]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT val FROM reuse_test",
            duckdb_conn=duckdb_conn,
        )

        assert result.success
        # No temp view is created anymore — the step name is not a queryable relation.
        with pytest.raises(Exception, match="reuse_test"):
            duckdb_conn.execute("SELECT * FROM reuse_test").fetchone()
        # The composed CTE (steps + final_sql, one statement) is the executable artifact.
        composed = result.value.composed_sql
        assert composed is not None
        assert "WITH reuse_test AS" in composed
        assert composed.rstrip().endswith("SELECT val FROM reuse_test")
        assert result.value.final_value == 42

    def test_repair_function_called_on_failure(self, duckdb_conn):
        """Repair function is called when a step fails."""
        repair_called = []

        def mock_repair(failed_sql: str, error_msg: str, description: str) -> Result[str]:
            repair_called.append({"sql": failed_sql, "error": error_msg})
            return Result.ok("SELECT 42 AS val")

        steps = [
            SQLStep(
                step_id="repair_test",
                sql="SELECT * FROM nonexistent_repair",
                description="Failing step",
            )
        ]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT val FROM repair_test",
            duckdb_conn=duckdb_conn,
            repair_fn=mock_repair,
            max_repair_attempts=1,
        )

        assert result.success
        assert len(repair_called) == 1
        assert "nonexistent_repair" in repair_called[0]["sql"]

    def test_no_steps_only_final(self, duckdb_conn):
        """Works with no steps, only final SQL."""
        result = execute_sql_steps(
            steps=[],
            final_sql="SELECT COUNT(*) FROM test_data",
            duckdb_conn=duckdb_conn,
        )

        assert result.success
        assert result.value is not None
        assert result.value.final_value == 3
        assert len(result.value.step_results) == 0

    def test_step_result_tracks_repair_attempts(self, duckdb_conn):
        """Step results track how many repair attempts were made."""
        call_count = 0

        def mock_repair(failed_sql: str, error_msg: str, description: str) -> Result[str]:
            nonlocal call_count
            call_count += 1
            return Result.ok("SELECT 99 AS val")

        steps = [
            SQLStep(
                step_id="attempt_test",
                sql="INVALID SQL HERE",
                description="Will be repaired",
            )
        ]

        result = execute_sql_steps(
            steps=steps,
            final_sql="SELECT val FROM attempt_test",
            duckdb_conn=duckdb_conn,
            repair_fn=mock_repair,
            max_repair_attempts=2,
        )

        assert result.success
        assert result.value.step_results[0].repair_attempts == 1
