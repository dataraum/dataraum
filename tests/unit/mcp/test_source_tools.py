"""Tests for source management MCP tools."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

from sqlalchemy.orm import Session

from dataraum.mcp.server import create_server


class TestToolRegistration:
    def test_handler_functions_importable(self) -> None:
        """Verify the tool handler functions exist and are callable."""
        from dataraum.mcp.server import _add_source

        assert callable(_add_source)

    def test_server_creates_successfully(self) -> None:
        """Server creates without error with tools registered."""
        server = create_server(output_dir=Path("/tmp/test_output"))
        assert server is not None


class TestAddSourceTool:
    def test_add_file_source(self, session: Session, duckdb_conn, tmp_path: Path) -> None:
        from dataraum.mcp.server import _add_source

        csv = tmp_path / "data.csv"
        csv.write_text("a,b\n1,2\n")

        result = _add_source(session, duckdb_conn, {"name": "test_src", "path": str(csv)})

        assert isinstance(result, dict)
        assert result["source"]["name"] == "test_src"
        assert result["source"]["status"] == "configured"

    def test_add_source_no_path_or_backend(self, session: Session) -> None:
        from dataraum.mcp.server import _add_source

        result = _add_source(session, MagicMock(), {"name": "bad"})
        assert "error" in result

    def test_add_source_both_path_and_backend(self, session: Session) -> None:
        from dataraum.mcp.server import _add_source

        result = _add_source(
            session, MagicMock(), {"name": "bad", "path": "/x", "backend": "postgres"}
        )
        assert "error" in result

    def test_add_db_source_needs_credentials(self, session: Session, duckdb_conn) -> None:
        from dataraum.mcp.server import _add_source

        result = _add_source(session, duckdb_conn, {"name": "mydb", "backend": "postgres"})

        assert isinstance(result, dict)
        assert result["source"]["status"] == "needs_credentials"
        assert "credential_instructions" in result
