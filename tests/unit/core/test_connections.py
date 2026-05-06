"""Tests for ConnectionConfig factories and ConnectionManager DuckDB optionality."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataraum.core.connections import ConnectionConfig, ConnectionManager


class TestForDirectory:
    """ConnectionConfig.for_directory creates a SQLite + DuckDB config."""

    def test_paths_set(self, tmp_path: Path) -> None:
        config = ConnectionConfig.for_directory(tmp_path)
        assert config.sqlite_path == tmp_path / "metadata.db"
        assert config.duckdb_path == tmp_path / "data.duckdb"


class TestForWorkspace:
    """ConnectionConfig.for_workspace creates a SQLite-only config (no DuckDB)."""

    def test_sqlite_only(self, tmp_path: Path) -> None:
        config = ConnectionConfig.for_workspace(tmp_path)
        assert config.sqlite_path == tmp_path / "workspace.db"
        assert config.duckdb_path is None

    def test_workspace_manager_initializes(self, tmp_path: Path) -> None:
        """A workspace ConnectionManager initializes SQLite without DuckDB."""
        config = ConnectionConfig.for_workspace(tmp_path)
        manager = ConnectionManager(config)
        manager.initialize()
        try:
            with manager.session_scope() as session:
                assert session is not None
        finally:
            manager.close()

    def test_workspace_manager_rejects_duckdb_cursor(self, tmp_path: Path) -> None:
        """duckdb_cursor() on a workspace manager raises with a clear message."""
        manager = ConnectionManager(ConnectionConfig.for_workspace(tmp_path))
        manager.initialize()
        try:
            with pytest.raises(RuntimeError, match="SQLite-only"):
                with manager.duckdb_cursor():
                    pass
        finally:
            manager.close()

    def test_active_session_table_created(self, tmp_path: Path) -> None:
        """Workspace manager creates the active_session pointer table."""
        from sqlalchemy import inspect

        manager = ConnectionManager(ConnectionConfig.for_workspace(tmp_path))
        manager.initialize()
        try:
            inspector = inspect(manager.engine)
            assert "active_session" in inspector.get_table_names()
        finally:
            manager.close()
