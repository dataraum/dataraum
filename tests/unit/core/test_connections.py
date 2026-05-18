"""Tests for ConnectionConfig factories and ConnectionManager.

Post-DAT-321: every SQLAlchemy engine binds to the workspace Postgres URL
read from ``DATABASE_URL``. Tests get a live Postgres via the session-scoped
``pg_url_clean`` fixture and a monkeypatched ``DATABASE_URL`` env var.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dataraum.core.connections import ConnectionConfig, ConnectionManager


@pytest.fixture(autouse=True)
def _database_url(monkeypatch: pytest.MonkeyPatch, pg_url_clean: str) -> None:
    """Wire DATABASE_URL to the per-test Postgres URL for every test in this module."""
    monkeypatch.setenv("DATABASE_URL", pg_url_clean)


class TestForDirectory:
    """ConnectionConfig.for_directory wires Postgres SQLAlchemy + per-session DuckDB."""

    def test_paths_and_url(self, tmp_path: Path, pg_url_clean: str) -> None:
        config = ConnectionConfig.for_directory(tmp_path)
        assert config.database_url == pg_url_clean
        assert config.duckdb_path == tmp_path / "data.duckdb"


class TestForWorkspace:
    """ConnectionConfig.for_workspace creates a Postgres-only config (no DuckDB)."""

    def test_postgres_only(self, pg_url_clean: str) -> None:
        config = ConnectionConfig.for_workspace()
        assert config.database_url == pg_url_clean
        assert config.duckdb_path is None

    def test_workspace_manager_initializes(self) -> None:
        """A workspace ConnectionManager initializes Postgres without DuckDB."""
        config = ConnectionConfig.for_workspace()
        manager = ConnectionManager(config)
        manager.initialize()
        try:
            with manager.session_scope() as session:
                assert session is not None
        finally:
            manager.close()

    def test_workspace_manager_rejects_duckdb_cursor(self) -> None:
        """duckdb_cursor() on a workspace manager raises with a clear message."""
        manager = ConnectionManager(ConnectionConfig.for_workspace())
        manager.initialize()
        try:
            with pytest.raises(RuntimeError, match="workspace-only"):
                with manager.duckdb_cursor():
                    pass
        finally:
            manager.close()

    def test_active_session_table_created(self) -> None:
        """Workspace manager creates the active_session pointer table."""
        from sqlalchemy import inspect

        manager = ConnectionManager(ConnectionConfig.for_workspace())
        manager.initialize()
        try:
            inspector = inspect(manager.engine)
            assert "active_session" in inspector.get_table_names()
        finally:
            manager.close()


class TestMissingDatabaseUrl:
    """for_workspace / for_directory fail loud when DATABASE_URL is unset."""

    def test_workspace_fails_without_env(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
            ConnectionConfig.for_workspace()

    def test_directory_fails_without_env(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
    ) -> None:
        monkeypatch.delenv("DATABASE_URL", raising=False)
        with pytest.raises(RuntimeError, match="DATABASE_URL is not set"):
            ConnectionConfig.for_directory(tmp_path)


class TestSessionId:
    """ConnectionManager carries an optional session_id for per-session row scoping."""

    def test_session_id_defaults_to_none(self) -> None:
        manager = ConnectionManager(ConnectionConfig.for_workspace())
        assert manager.session_id is None

    def test_session_id_is_settable(self) -> None:
        manager = ConnectionManager(ConnectionConfig.for_workspace(), session_id="sess-abc")
        assert manager.session_id == "sess-abc"
