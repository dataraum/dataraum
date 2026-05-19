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

    def test_add_source_duplicate_name_errors(self, session: Session, tmp_path: Path) -> None:
        """Registering the same source name twice is rejected (the registry is append-only)."""
        from dataraum.mcp.server import _add_source

        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")
        first = _add_source(session, {"name": "twin", "path": str(csv)})
        assert "error" not in first

        second = _add_source(session, {"name": "twin", "path": str(csv)})
        assert "error" in second
        assert "twin" in second["error"]
        assert "already exists" in second["error"].lower()


class TestListSourcesTool:
    """list_sources surfaces the workspace registry without secrets."""

    def test_empty_workspace(self, session: Session) -> None:
        from dataraum.mcp.server import _list_sources

        # The conftest `session` fixture seeds a baseline Source as the
        # InvestigationSession FK target. Filter it out for "empty workspace".
        result = _list_sources(session)
        result = {
            "sources": [s for s in result["sources"] if s["name"] != "test_baseline"],
            "count": result["count"] - 1 if result["count"] else 0,
        }
        assert result == {"sources": [], "count": 0}

    def test_lists_file_and_recipe_sources(self, session: Session, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source, _list_sources

        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")
        recipe = tmp_path / "erp.yaml"
        recipe.write_text(VALID_RECIPE)

        _add_source(session, {"name": "files", "path": str(csv)})
        _add_source(session, {"name": "erp", "path": str(recipe)})

        result = _list_sources(session)
        result["sources"] = [s for s in result["sources"] if s["name"] != "test_baseline"]
        result["count"] = len(result["sources"])

        assert result["count"] == 2
        by_name = {s["name"]: s for s in result["sources"]}
        assert by_name["files"]["type"] == "csv"
        assert by_name["files"]["status"] == "configured"
        assert by_name["erp"]["type"] == "db_recipe"
        assert by_name["erp"]["backend"] == "mssql"
        assert by_name["erp"]["recipe_tables"] == ["invoices"]
        # No URL/credential fields anywhere
        for entry in result["sources"]:
            assert "url" not in entry
            assert "credentials" not in entry
            assert "credential_ref" not in entry


class TestSourcesDirFallback:
    """add_source resolves bare names against SOURCES_DIR."""

    def test_bare_name_resolves_to_sources_dir(
        self, session: Session, tmp_path: Path, monkeypatch
    ) -> None:
        import dataraum.core.paths as paths_mod
        from dataraum.mcp.server import _add_source

        # Point SOURCES_DIR at a fake directory holding the recipe yaml directly
        (tmp_path / "warehouse.yaml").write_text(VALID_RECIPE)
        monkeypatch.setattr(paths_mod, "SOURCES_DIR", tmp_path)

        result = _add_source(session, {"name": "warehouse", "path": "warehouse"})

        assert isinstance(result, dict), result
        assert "error" not in result, result.get("error")
        assert result["source"]["type"] == "db_recipe"
        assert result["source"]["backend"] == "mssql"

    def test_filename_resolves_to_sources_dir(
        self, session: Session, tmp_path: Path, monkeypatch
    ) -> None:
        import dataraum.core.paths as paths_mod
        from dataraum.mcp.server import _add_source

        (tmp_path / "warehouse.yaml").write_text(VALID_RECIPE)
        monkeypatch.setattr(paths_mod, "SOURCES_DIR", tmp_path)

        result = _add_source(session, {"name": "warehouse", "path": "warehouse.yaml"})
        assert "error" not in result, result.get("error")
        assert result["source"]["type"] == "db_recipe"

    def test_missing_recipe_error_mentions_sources_dir(
        self, session: Session, tmp_path: Path, monkeypatch
    ) -> None:
        import dataraum.core.paths as paths_mod
        from dataraum.mcp.server import _add_source

        monkeypatch.setattr(paths_mod, "SOURCES_DIR", tmp_path)

        result = _add_source(session, {"name": "ghost", "path": "ghost"})
        assert "error" in result
        assert str(tmp_path) in result["error"]
