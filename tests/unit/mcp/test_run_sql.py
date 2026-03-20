"""Tests for run_sql MCP tool."""

from __future__ import annotations

import duckdb
import pytest

from dataraum.mcp.formatters import format_run_sql_result
from dataraum.mcp.sql_executor import run_sql
from dataraum.query.execution import StepExecutionResult


@pytest.fixture
def cursor() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE TABLE orders (id INT, amount DOUBLE, region VARCHAR)")
    conn.execute(
        "INSERT INTO orders VALUES "
        "(1, 100.0, 'US'), (2, 200.0, 'EU'), (3, 150.0, 'US')"
    )
    return conn


# --- Phase 2a: Basic execution ---


class TestRawSqlExecutes:
    def test_simple_select(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT 42 AS x")
        assert "error" not in result
        assert result["columns"] == ["x"]
        assert result["row_count"] == 1
        assert result["rows"] == [{"x": 42}]
        assert result["truncated"] is False

    def test_select_from_table(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT * FROM orders WHERE region = 'US'")
        assert result["row_count"] == 2
        assert all(r["region"] == "US" for r in result["rows"])


class TestStructuredStepsExecuted:
    def test_two_steps_with_reference(self, cursor: duckdb.DuckDBPyConnection) -> None:
        steps = [
            {"step_id": "us_orders", "sql": "SELECT * FROM orders WHERE region = 'US'"},
            {
                "step_id": "us_total",
                "sql": "SELECT SUM(amount) AS total FROM us_orders",
                "description": "Sum US orders",
            },
        ]
        result = run_sql(cursor, steps=steps)
        assert "error" not in result
        assert result["row_count"] == 1
        assert result["rows"][0]["total"] == 250.0
        assert len(result["steps_executed"]) == 2
        assert result["steps_executed"][0]["step_id"] == "us_orders"
        assert result["steps_executed"][1]["step_id"] == "us_total"


class TestInputValidation:
    def test_rejects_both_steps_and_sql(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, steps=[{"step_id": "q", "sql": "SELECT 1"}], sql="SELECT 1")
        assert "error" in result
        assert "not both" in result["error"]

    def test_rejects_neither_steps_nor_sql(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor)
        assert "error" in result
        assert "Provide either" in result["error"]


class TestRowLimit:
    def test_default_limit_applied(self, cursor: duckdb.DuckDBPyConnection) -> None:
        # Insert enough rows to exceed default limit (100)
        cursor.execute(
            "CREATE TABLE big AS SELECT i AS id FROM generate_series(1, 200) t(i)"
        )
        result = run_sql(cursor, sql="SELECT * FROM big")
        assert result["row_count"] == 100
        assert result["truncated"] is True

    def test_custom_limit(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT * FROM orders", limit=2)
        assert result["row_count"] == 2
        assert result["truncated"] is True

    def test_max_limit_enforced(self, cursor: duckdb.DuckDBPyConnection) -> None:
        # Limit > 10000 is capped to 10000
        result = run_sql(cursor, sql="SELECT * FROM orders", limit=99999)
        # With only 3 rows, truncated should be False — the cap is 10000
        assert result["truncated"] is False
        assert result["row_count"] == 3


class TestSqlError:
    def test_bad_sql_returns_error(self, cursor: duckdb.DuckDBPyConnection) -> None:
        result = run_sql(cursor, sql="SELECT * FROM nonexistent_table")
        assert "error" in result


class TestFormatRunSqlResult:
    def test_basic_format(self) -> None:
        step_results = [
            StepExecutionResult(step_id="q", sql_executed="SELECT 1 AS x"),
        ]
        result = format_run_sql_result(
            columns=["x"],
            rows=[{"x": 1}],
            step_results=step_results,
            limit=100,
            total_rows=1,
        )
        assert result["columns"] == ["x"]
        assert result["row_count"] == 1
        assert result["truncated"] is False
        assert result["steps_executed"] == [{"step_id": "q", "sql": "SELECT 1 AS x"}]
        assert "column_quality" not in result
        assert "quality_caveat" not in result

    def test_truncated_flag(self) -> None:
        step_results = [
            StepExecutionResult(step_id="q", sql_executed="SELECT 1"),
        ]
        result = format_run_sql_result(
            columns=["x"],
            rows=[{"x": 1}] * 10,
            step_results=step_results,
            limit=10,
            total_rows=50,
        )
        assert result["truncated"] is True

    def test_quality_fields_included_when_provided(self) -> None:
        step_results = [
            StepExecutionResult(step_id="q", sql_executed="SELECT 1"),
        ]
        quality = {"revenue": {"quality_grade": "A"}}
        result = format_run_sql_result(
            columns=["revenue"],
            rows=[{"revenue": 100}],
            step_results=step_results,
            limit=100,
            total_rows=1,
            column_quality=quality,
            quality_caveat="entropy phase not run",
        )
        assert result["column_quality"] == quality
        assert result["quality_caveat"] == "entropy phase not run"
