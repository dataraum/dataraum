"""Tests for workspace file discovery."""

from __future__ import annotations

from pathlib import Path

from dataraum.sources.discovery import discover_sources


class TestDiscoverSources:
    def test_finds_csv_files(self, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("id,name,amount\n1,Alice,100\n2,Bob,200\n")

        result = discover_sources(tmp_path)

        assert len(result.files) == 1
        assert result.files[0].format == "csv"
        assert result.files[0].path == "data.csv"
        assert result.files[0].size_bytes > 0
        assert "id" in result.files[0].columns

    def test_finds_parquet_files(self, tmp_path: Path) -> None:
        import duckdb

        parquet = tmp_path / "data.parquet"
        conn = duckdb.connect()
        conn.execute(
            f"COPY (SELECT 1 AS id, 'test' AS name) TO '{parquet}' (FORMAT PARQUET)"
        )
        conn.close()

        result = discover_sources(tmp_path)

        assert len(result.files) == 1
        assert result.files[0].format == "parquet"
        assert "id" in result.files[0].columns

    def test_ignores_non_data_files(self, tmp_path: Path) -> None:
        (tmp_path / "readme.md").write_text("# Hello")
        (tmp_path / "script.py").write_text("print('hi')")
        (tmp_path / "data.csv").write_text("a,b\n1,2\n")

        result = discover_sources(tmp_path)
        assert len(result.files) == 1

    def test_recursive_scan(self, tmp_path: Path) -> None:
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "nested.csv").write_text("x,y\n1,2\n")

        result = discover_sources(tmp_path, recursive=True)
        assert len(result.files) == 1
        assert "subdir" in result.files[0].path

    def test_non_recursive_scan(self, tmp_path: Path) -> None:
        sub = tmp_path / "subdir"
        sub.mkdir()
        (sub / "nested.csv").write_text("x,y\n1,2\n")
        (tmp_path / "top.csv").write_text("a,b\n1,2\n")

        result = discover_sources(tmp_path, recursive=False)
        assert len(result.files) == 1
        assert result.files[0].path == "top.csv"

    def test_existing_sources_passed_through(self, tmp_path: Path) -> None:
        result = discover_sources(tmp_path, existing_sources=["bookings", "erp"])
        assert result.existing_sources == ["bookings", "erp"]

    def test_empty_directory(self, tmp_path: Path) -> None:
        result = discover_sources(tmp_path)
        assert result.files == []

    def test_nonexistent_directory(self, tmp_path: Path) -> None:
        result = discover_sources(tmp_path / "nonexistent")
        assert result.files == []

    def test_skips_empty_files(self, tmp_path: Path) -> None:
        (tmp_path / "empty.csv").write_text("")
        result = discover_sources(tmp_path)
        assert result.files == []

    def test_scan_root_is_set(self, tmp_path: Path) -> None:
        result = discover_sources(tmp_path)
        assert result.scan_root == str(tmp_path)

    def test_tsv_detected_as_csv_format(self, tmp_path: Path) -> None:
        (tmp_path / "data.tsv").write_text("a\tb\n1\t2\n")
        result = discover_sources(tmp_path)
        assert len(result.files) == 1
        assert result.files[0].format == "csv"
