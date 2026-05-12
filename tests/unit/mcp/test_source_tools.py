"""Tests for source management MCP tools."""

from __future__ import annotations

from pathlib import Path

from sqlalchemy.orm import Session

from dataraum.mcp.server import create_server

VALID_RECIPE = """\
backend: mssql
tables:
  invoices:
    sql: SELECT invoice_id FROM dbo.Invoices
"""


class TestToolRegistration:
    def test_handler_functions_importable(self) -> None:
        from dataraum.mcp.server import _add_source

        assert callable(_add_source)

    def test_server_creates_successfully(self) -> None:
        server = create_server(output_dir=Path("/tmp/test_output"))
        assert server is not None


class TestAddSourceTool:
    """add_source dispatches by file extension: .yaml/.yml → recipe loader;
    .csv/.tsv/.parquet/.json/.jsonl → file loader; directory → directory."""

    def test_add_csv_file(self, session: Session, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source

        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")

        result = _add_source(session, {"name": "test_src", "path": str(csv)})

        assert isinstance(result, dict)
        assert result["source"]["name"] == "test_src"
        assert result["source"]["status"] == "configured"
        assert result["source"]["type"] == "csv"

    def test_add_recipe_yaml(self, session: Session, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source

        recipe = tmp_path / "erp.yaml"
        recipe.write_text(VALID_RECIPE)

        result = _add_source(session, {"name": "erp", "path": str(recipe)})

        assert isinstance(result, dict)
        assert result["source"]["name"] == "erp"
        assert result["source"]["status"] == "configured"
        assert result["source"]["type"] == "db_recipe"
        assert result["source"]["backend"] == "mssql"
        assert result["source"]["recipe_tables"] == ["invoices"]

    def test_add_source_missing_path(self, session: Session) -> None:
        from dataraum.mcp.server import _add_source

        result = _add_source(session, {"name": "bad"})
        assert "error" in result

    def test_add_source_unknown_extension(self, session: Session, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source

        weird = tmp_path / "data.xlsx"
        weird.write_text("garbage")
        result = _add_source(session, {"name": "weird", "path": str(weird)})
        assert "error" in result
