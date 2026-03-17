"""Tests for the export layer."""

from __future__ import annotations

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dataraum.export import (
    export_query_result,
    export_sql,
)
from dataraum.query.models import QueryResult


def _make_query_result(
    data: list[dict] | None = None,
    columns: list[str] | None = None,
) -> QueryResult:
    """Create a minimal QueryResult for testing."""
    if data is None:
        data = [
            {"name": "Alice", "amount": 100},
            {"name": "Bob", "amount": 200},
        ]
    if columns is None:
        columns = ["name", "amount"]
    return QueryResult(
        execution_id="test-001",
        question="test query",
        executed_at=datetime(2025, 1, 1, tzinfo=UTC),
        answer="Test answer",
        sql="SELECT name, amount FROM test",
        data=data,
        columns=columns,
    )


class TestExportQueryResult:
    """Tests for export_query_result."""

    def test_csv_export(self, tmp_path: Path) -> None:
        result = _make_query_result()
        path = export_query_result(result, tmp_path / "out.csv", fmt="csv")

        assert path.exists()
        assert path.suffix == ".csv"

        with open(path) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[1]["amount"] == "200"

    def test_json_export(self, tmp_path: Path) -> None:
        result = _make_query_result()
        path = export_query_result(result, tmp_path / "out.json", fmt="json")

        assert path.exists()
        with open(path) as f:
            data = json.load(f)
        assert data["columns"] == ["name", "amount"]
        assert len(data["data"]) == 2

    def test_parquet_export(self, tmp_path: Path) -> None:
        result = _make_query_result()
        path = export_query_result(result, tmp_path / "out.parquet", fmt="parquet")

        assert path.exists()
        assert path.suffix == ".parquet"

        import duckdb

        df = duckdb.execute(f"SELECT * FROM '{path}'").fetchdf()
        assert len(df) == 2
        assert list(df.columns) == ["name", "amount"]

    def test_creates_metadata_sidecar(self, tmp_path: Path) -> None:
        result = _make_query_result()
        path = export_query_result(result, tmp_path / "out.csv", fmt="csv")

        sidecar = path.with_suffix(".csv.meta.json")
        assert sidecar.exists()

        with open(sidecar) as f:
            meta = json.load(f)
        assert meta["execution_id"] == "test-001"
        assert meta["question"] == "test query"
        assert meta["sql"] == "SELECT name, amount FROM test"
        assert meta["row_count"] == 2
        assert meta["column_count"] == 2
        assert "exported_at" in meta
        assert meta["confidence"]["level"] is not None

    def test_auto_corrects_extension(self, tmp_path: Path) -> None:
        result = _make_query_result()
        path = export_query_result(result, tmp_path / "out.txt", fmt="csv")
        assert path.suffix == ".csv"

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        result = _make_query_result()
        path = export_query_result(result, tmp_path / "sub" / "dir" / "out.csv", fmt="csv")
        assert path.exists()

    def test_raises_on_empty_data(self, tmp_path: Path) -> None:
        result = _make_query_result(data=None, columns=None)
        result.data = None
        result.columns = None
        with pytest.raises(ValueError, match="no tabular data"):
            export_query_result(result, tmp_path / "out.csv")

    def test_sidecar_includes_assumptions(self, tmp_path: Path) -> None:
        from dataraum.graphs.models import AssumptionBasis, QueryAssumption

        result = _make_query_result()
        result.assumptions = [
            QueryAssumption.create(
                execution_id="test-001",
                dimension="semantic.units",
                target="column:orders.amount",
                assumption="Currency is EUR",
                basis=AssumptionBasis.INFERRED,
                confidence=0.8,
            )
        ]
        path = export_query_result(result, tmp_path / "out.csv", fmt="csv")
        sidecar = path.with_suffix(".csv.meta.json")

        with open(sidecar) as f:
            meta = json.load(f)
        assert len(meta["assumptions"]) == 1
        assert meta["assumptions"][0]["assumption"] == "Currency is EUR"


class TestExportSql:
    """Tests for export_sql with a mock DuckDB connection."""

    def _mock_conn(self, columns: list[str], rows: list[tuple]) -> MagicMock:
        """Create a mock DuckDB connection."""
        conn = MagicMock()
        result = MagicMock()
        result.description = [(c,) for c in columns]
        result.fetchall.return_value = rows
        conn.execute.return_value = result

        # For count query
        count_result = MagicMock()
        count_result.fetchone.return_value = (len(rows),)

        def side_effect(sql: str) -> MagicMock:
            if sql.startswith("SELECT COUNT"):
                return count_result
            return result

        conn.execute.side_effect = side_effect
        return conn

    def test_json_export(self, tmp_path: Path) -> None:
        conn = self._mock_conn(["x", "y"], [(1, 2), (3, 4)])
        path = export_sql("SELECT x, y FROM t", conn, tmp_path / "out.json", fmt="json")

        assert path.exists()
        with open(path) as f:
            data = json.load(f)
        assert data["columns"] == ["x", "y"]
        assert len(data["data"]) == 2

    def test_json_sidecar(self, tmp_path: Path) -> None:
        conn = self._mock_conn(["x"], [(1,)])
        path = export_sql(
            "SELECT x FROM t",
            conn,
            tmp_path / "out.json",
            fmt="json",
            description="Test export",
        )

        sidecar = path.with_suffix(".json.meta.json")
        assert sidecar.exists()

        with open(sidecar) as f:
            meta = json.load(f)
        assert meta["sql"] == "SELECT x FROM t"
        assert meta["description"] == "Test export"
        assert meta["row_count"] == 1
