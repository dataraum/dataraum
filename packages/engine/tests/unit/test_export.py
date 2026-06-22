"""Tests for the export layer."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from dataraum.export import export_sql


class TestExportSql:
    """Tests for export_sql with DuckDB COPY path."""

    def test_csv_export(self, tmp_path: Path) -> None:
        """CSV export via DuckDB COPY creates file with header."""
        conn = duckdb.connect()
        conn.execute(
            "CREATE TABLE t AS SELECT 'Alice' AS name, 100 AS amount UNION ALL SELECT 'Bob', 200"
        )

        path = export_sql("SELECT * FROM t", conn, tmp_path / "out.csv", fmt="csv")

        assert path.exists()
        assert path.suffix == ".csv"
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 3  # header + 2 rows
        assert "name" in lines[0]

        conn.close()

    def test_parquet_export(self, tmp_path: Path) -> None:
        """Parquet export via DuckDB COPY creates readable file."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT 1 AS x, 2 AS y UNION ALL SELECT 3, 4")

        path = export_sql("SELECT * FROM t", conn, tmp_path / "out.parquet", fmt="parquet")

        assert path.exists()
        assert path.suffix == ".parquet"

        # Verify parquet is readable
        rows = duckdb.execute(f"SELECT * FROM '{path}'").fetchall()
        assert len(rows) == 2

        conn.close()

    def test_sidecar_written(self, tmp_path: Path) -> None:
        """Metadata sidecar is written alongside data file."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")

        path = export_sql("SELECT * FROM t", conn, tmp_path / "out.csv", fmt="csv")

        sidecar_path = path.with_suffix(".csv.meta.json")
        assert sidecar_path.exists()

        with open(sidecar_path) as f:
            meta = json.load(f)
        assert meta["row_count"] == 1
        assert meta["format"] == "csv"
        assert "exported_at" in meta

        conn.close()

    def test_caller_sidecar_merged(self, tmp_path: Path) -> None:
        """Caller-provided sidecar is merged into the metadata."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")

        sidecar = {
            "confidence": {"label": "GREEN"},
            "sql": "SELECT 1 AS x",
            "steps_executed": [{"step_id": "s1"}],
        }
        path = export_sql("SELECT * FROM t", conn, tmp_path / "out.csv", sidecar=sidecar)

        with open(path.with_suffix(".csv.meta.json")) as f:
            meta = json.load(f)
        assert meta["confidence"] == {"label": "GREEN"}
        assert meta["steps_executed"][0]["step_id"] == "s1"
        # Export metadata also present
        assert meta["row_count"] == 1
        assert "exported_at" in meta

        conn.close()

    def test_auto_corrects_extension(self, tmp_path: Path) -> None:
        """Wrong extension is corrected to match format."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")

        path = export_sql("SELECT * FROM t", conn, tmp_path / "out.txt", fmt="csv")
        assert path.suffix == ".csv"

        conn.close()

    def test_creates_parent_directories(self, tmp_path: Path) -> None:
        """Parent directories are created if they don't exist."""
        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")

        path = export_sql("SELECT * FROM t", conn, tmp_path / "deep" / "nested" / "out.csv")
        assert path.exists()

        conn.close()
