"""Tests for ConnectionConfig factories and ConnectionManager.

Post-DAT-321: every SQLAlchemy engine binds to the workspace Postgres URL
read from ``DATABASE_URL``. Tests get a live Postgres via the session-scoped
``pg_url_clean`` fixture and a monkeypatched ``DATABASE_URL`` env var.

Post-DAT-323: per-session DuckDB is obtained from the DuckLake anchor; tests
that initialize a per-session ``ConnectionManager`` request the
``lake_anchor`` fixture so the bootstrap has run.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from dataraum.core.connections import (
    ConnectionConfig,
    ConnectionManager,
)


@pytest.fixture(autouse=True)
def _database_url(monkeypatch: pytest.MonkeyPatch, pg_url_clean: str) -> None:
    """Wire DATABASE_URL to the per-test Postgres URL for every test in this module."""
    monkeypatch.setenv("DATABASE_URL", pg_url_clean)


class TestForDirectory:
    """``for_directory`` is equivalent to ``for_workspace`` post-DAT-323.

    The ``output_dir`` parameter is retained for caller signature
    compatibility (engine code in ``pipeline/setup.py`` still passes a
    directory), but DuckDB-side state is driven entirely by the manager's
    ``session_id``.
    """

    def test_url_set(self, tmp_path: Path, pg_url_clean: str) -> None:
        config = ConnectionConfig.for_directory(tmp_path)
        assert config.database_url == pg_url_clean

    def test_no_duckdb_path_attribute(self, tmp_path: Path) -> None:
        config = ConnectionConfig.for_directory(tmp_path)
        assert not hasattr(config, "duckdb_path")


class TestForWorkspace:
    """``for_workspace`` creates a config with no DuckDB-side state."""

    def test_postgres_only(self, pg_url_clean: str) -> None:
        config = ConnectionConfig.for_workspace()
        assert config.database_url == pg_url_clean

    def test_workspace_manager_initializes(self) -> None:
        """A workspace ConnectionManager (no session_id) initializes Postgres without DuckDB."""
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


class TestWorkspaceTypedDuckLake:
    """Per-session managers open a DuckDB connection scoped to ``lake.typed``.

    Post-DAT-341 the substrate is workspace-stable — all session managers
    USE the same workspace schemas (``raw`` / ``typed`` / ``quarantine``)
    rather than per-session schemas keyed off ``session_id``. The
    ``session_id`` field still exists for non-DuckDB row scoping; it just
    no longer drives the DuckDB connection's USE state.
    """

    def test_bootstrap_creates_layer_schemas(self, lake_anchor, lake_clean) -> None:
        """raw / typed / quarantine schemas exist after bootstrap_lake."""
        from dataraum.server.storage import LAKE_CATALOG_ALIAS, LAKE_LAYER_SCHEMAS, get_anchor

        anchor = get_anchor()
        rows = anchor.execute(
            "SELECT schema_name FROM duckdb_schemas() "
            f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
            f"AND schema_name IN ({','.join(repr(s) for s in LAKE_LAYER_SCHEMAS)})"
        ).fetchall()
        assert sorted(r[0] for r in rows) == sorted(LAKE_LAYER_SCHEMAS)

    def test_initialize_uses_typed_schema(self, lake_anchor, lake_clean) -> None:
        """The manager's cursor lands unqualified CREATE TABLEs in lake.typed."""
        from dataraum.server.storage import LAKE_CATALOG_ALIAS, get_anchor

        manager = ConnectionManager(
            ConnectionConfig.for_workspace(),
            session_id="11111111-2222-3333-4444-555555555555",
        )
        manager.initialize()
        try:
            with manager.duckdb_cursor() as cursor:
                cursor.execute("CREATE TABLE marker (x INT)")
            anchor = get_anchor()
            tables = anchor.execute(
                "SELECT table_name FROM duckdb_tables() "
                f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
                "AND schema_name = 'typed'"
            ).fetchall()
            assert ("marker",) in tables
        finally:
            manager.close()

    def test_close_does_not_close_the_anchor(self, lake_anchor, lake_clean) -> None:
        from dataraum.server.storage import get_anchor, health_probe

        manager = ConnectionManager(
            ConnectionConfig.for_workspace(),
            session_id="abcdabcd-0000-0000-0000-000000000001",
        )
        manager.initialize()
        manager.close()

        # Anchor must still respond after the manager's connection is closed.
        assert get_anchor() is not None
        assert health_probe() == {"status": "ok"}

    def test_two_session_managers_share_typed_schema(self, lake_anchor, lake_clean) -> None:
        """Two per-session managers both USE lake.typed and see each other's tables.

        Post-DAT-341 there is no schema-level isolation between sessions —
        the substrate is workspace-stable. Row-level scoping (``workspace_id``
        on Table / EntropyObjectRecord) is the new isolation mechanism;
        slice 2's session overlays will live under reserved ``session_*``
        schemas, not in the ``typed`` schema itself.
        """
        a = ConnectionManager(
            ConnectionConfig.for_workspace(),
            session_id="aaaaaaaa-0000-0000-0000-000000000001",
        )
        b = ConnectionManager(
            ConnectionConfig.for_workspace(),
            session_id="bbbbbbbb-0000-0000-0000-000000000002",
        )
        a.initialize()
        b.initialize()
        try:
            with a.duckdb_cursor() as ca:
                ca.execute("CREATE TABLE shared_marker (x INT)")
            # Manager B's unqualified SELECT resolves against lake.typed too —
            # it sees what manager A just created.
            with b.duckdb_cursor() as cb:
                rows = cb.execute(
                    "SELECT table_name FROM duckdb_tables() "
                    "WHERE schema_name = current_schema() "
                    "AND database_name = 'lake'"
                ).fetchall()
            assert ("shared_marker",) in rows
        finally:
            a.close()
            b.close()


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
