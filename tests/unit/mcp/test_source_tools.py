"""Tests for source management MCP tools."""

from __future__ import annotations

import json
from pathlib import Path

from dataraum.mcp.server import create_server


class TestToolRegistration:
    def test_handler_functions_importable(self) -> None:
        """Verify the tool handler functions exist and are callable."""
        from dataraum.mcp.server import _add_source, _discover_sources, _get_quality

        assert callable(_discover_sources)
        assert callable(_add_source)
        assert callable(_get_quality)

    def test_server_creates_successfully(self) -> None:
        """Server creates without error with tools registered."""
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


class TestGetQualityTool:
    def test_no_data_returns_error(self, tmp_path: Path) -> None:
        from dataraum.mcp.server import _get_quality

        result = _get_quality(tmp_path / "nonexistent")
        assert "No analyzed data" in result

    def test_include_filters_sections(self, tmp_path: Path) -> None:
        """Verify include parameter is accepted (integration tested separately)."""
        from dataraum.mcp.server import _get_quality

        # No database = early error, but proves the function accepts the param
        result = _get_quality(tmp_path / "nonexistent", include=["entropy"])
        assert "No analyzed data" in result
