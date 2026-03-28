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
        df = duckdb.execute(f"SELECT * FROM '{path}'").fetchdf()
        assert len(df) == 2

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


class TestDoExport:
    """Tests for _do_export in server.py."""

    def test_exports_and_adds_path(self, tmp_path: Path) -> None:
        """_do_export adds export_path to result dict."""
        from dataraum.mcp.server import _do_export

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT 1 AS x, 2 AS y")

        result: dict = {"columns": ["x", "y"], "rows": [{"x": 1, "y": 2}]}
        _do_export(result, "SELECT * FROM t", conn, tmp_path, "csv", "my_export", "run_sql")

        assert "export_path" in result
        assert Path(result["export_path"]).exists()
        assert "exports" in result["export_path"]

        conn.close()

    def test_sidecar_excludes_rows(self, tmp_path: Path) -> None:
        """Sidecar does not contain rows/data from the MCP result."""
        from dataraum.mcp.server import _do_export

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")

        result: dict = {
            "columns": ["x"],
            "rows": [{"x": 1}],
            "row_count": 100,
            "rows_returned": 50,
            "truncated": True,
            "snippet_summary": {"reused": 1},
        }
        _do_export(result, "SELECT * FROM t", conn, tmp_path, "csv", None, "run_sql")

        sidecar_path = Path(result["export_path"]).with_suffix(".csv.meta.json")
        with open(sidecar_path) as f:
            meta = json.load(f)
        # Provenance present
        assert meta["snippet_summary"] == {"reused": 1}
        # Display artifacts stripped
        assert "rows" not in meta
        assert "data" not in meta
        assert "truncated" not in meta
        assert "rows_returned" not in meta

        conn.close()

    def test_sanitizes_path_traversal(self, tmp_path: Path) -> None:
        """Path traversal in export_name is sanitized."""
        from dataraum.mcp.server import _do_export

        conn = duckdb.connect()
        conn.execute("CREATE TABLE t AS SELECT 1 AS x")

        result: dict = {}
        _do_export(result, "SELECT * FROM t", conn, tmp_path, "csv", "../../../evil", "run_sql")

        assert "export_path" in result
        path = Path(result["export_path"])
        assert ".." not in str(path)
        assert "exports" in str(path)

        conn.close()

    def test_export_error_on_bad_sql(self, tmp_path: Path) -> None:
        """Bad SQL produces export_error, not exception."""
        from dataraum.mcp.server import _do_export

        conn = duckdb.connect()

        result: dict = {}
        _do_export(result, "SELECT * FROM nonexistent", conn, tmp_path, "csv", None, "run_sql")

        assert "export_error" in result
        assert "export_path" not in result

        conn.close()


class TestSafeExportPath:
    """Tests for _safe_export_path."""

    def test_generates_path(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _safe_export_path

        path = _safe_export_path(tmp_path, "my_file", "csv", "run_sql")
        assert isinstance(path, Path)
        assert path.name == "my_file.csv"
        assert "exports" in str(path)

    def test_auto_generates_name(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _safe_export_path

        path = _safe_export_path(tmp_path, None, "parquet", "query")
        assert isinstance(path, Path)
        assert "query_" in path.stem

    def test_sanitizes_special_chars(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _safe_export_path

        path = _safe_export_path(tmp_path, "my file (1)/test", "csv")
        assert isinstance(path, Path)
        assert "/" not in path.stem
        assert "(" not in path.stem
