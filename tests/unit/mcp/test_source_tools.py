"""Tests for source management MCP tools."""

from __future__ import annotations

import json
from pathlib import Path

from dataraum.mcp.server import create_server


class TestToolRegistration:
    def test_handler_functions_importable(self) -> None:
        """Verify the new tool handler functions exist and are callable."""
        from dataraum.mcp.server import _add_source, _discover_sources, _remove_source

        assert callable(_discover_sources)
        assert callable(_add_source)
        assert callable(_remove_source)

    def test_server_creates_successfully(self) -> None:
        """Server creates without error with new tools registered."""
        server = create_server(output_dir=Path("/tmp/test_output"))
        assert server is not None


class TestDiscoverSourcesTool:
    def test_discover_files(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _discover_sources

        csv = tmp_path / "data.csv"
        csv.write_text("id,name\n1,Alice\n")

        result = _discover_sources(tmp_path / "output", str(tmp_path), True)
        parsed = json.loads(result)

        assert len(parsed["files"]) == 1
        assert parsed["files"][0]["format"] == "csv"
        assert "id" in parsed["files"][0]["columns"]

    def test_discover_empty(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _discover_sources

        result = _discover_sources(tmp_path / "output", str(tmp_path), True)
        assert "No data files found" in result

    def test_discover_nonexistent_dir(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _discover_sources

        result = _discover_sources(tmp_path, str(tmp_path / "nope"), True)
        assert "not found" in result.lower()


class TestAddSourceTool:
    def test_add_file_source(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source

        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")
        output_dir = tmp_path / "output"

        result = _add_source(output_dir, {"name": "test_src", "path": str(csv)})
        parsed = json.loads(result)

        assert parsed["source"]["name"] == "test_src"
        assert parsed["source"]["status"] == "configured"

    def test_add_source_no_path_or_backend(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source

        result = _add_source(tmp_path, {"name": "bad"})
        assert "Error" in result

    def test_add_source_both_path_and_backend(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source

        result = _add_source(tmp_path, {"name": "bad", "path": "/x", "backend": "postgres"})
        assert "Error" in result

    def test_add_db_source_needs_credentials(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source

        output_dir = tmp_path / "output"
        result = _add_source(output_dir, {"name": "mydb", "backend": "postgres"})
        parsed = json.loads(result)

        assert parsed["source"]["status"] == "needs_credentials"
        assert "credential_instructions" in parsed


class TestRemoveSourceTool:
    def test_remove_existing(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source, _remove_source

        csv = tmp_path / "data.csv"
        csv.write_text("a\n1\n")
        output_dir = tmp_path / "output"

        # Add then remove
        _add_source(output_dir, {"name": "to_remove", "path": str(csv)})
        result = _remove_source(output_dir, "to_remove", False)
        parsed = json.loads(result)

        assert parsed["removed"] == "to_remove"
        assert parsed["analysis_preserved"] is True

    def test_remove_nonexistent(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _remove_source

        output_dir = tmp_path / "output"
        # Create the output dir so the manager can be created
        output_dir.mkdir(parents=True)

        result = _remove_source(output_dir, "ghost", False)
        # Should be an error (either no database or source not found)
        assert "Error" in result or "No analyzed data" in result
