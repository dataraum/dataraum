"""Lane smoke for DAT-323 — Per-session DuckDB files → DuckLake.

Scope: verify the L4 DuckLake substrate post-port.

* Process-wide DuckLake anchor bootstraps against Postgres catalog + local-FS
  DATA_PATH; ``/health``-style probe returns ``ok``.
* Per-session ``ConnectionManager`` opens a fresh DuckDB connection to the
  shared named in-memory DB and ``USE``s ``lake.session_<id>`` (the schema is
  auto-created).
* Direct ``connection.execute()`` and cursors derived from the manager both
  resolve unqualified table refs to the session's schema (the wrapper
  re-applies ``USE`` per cursor).
* Two concurrent per-session managers do not leak schema state across each
  other (the load-bearing reason for the wrapper).
* DDL patterns the pipeline uses (``CREATE TABLE``, ``ALTER TABLE RENAME``,
  ``CREATE VIEW``, ``DROP VIEW``) work against DuckLake.
* Parquet files land under DATA_PATH after a write (so the storage backend
  actually flushed, not just catalog state).
* Loader-style ephemeral ``duckdb.connect(':memory:')`` does NOT write to the
  lake (the carve-out documented in csv/json loaders).
* End-session leaves the lake schema in place (Option A from the design — no
  ``ALTER SCHEMA RENAME``, just a workspace-state flip).

The Postgres testcontainer is provided by ``tests/conftest.py``; this smoke
re-uses the shared ``lake_anchor`` so the per-test set-up cost is one
``lake_clean`` schema sweep.

Run:
    uv run pytest tests/platform/smoke_dat323.py -v
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from dataraum.core.connections import (
    ConnectionConfig,
    ConnectionManager,
    _session_id_to_schema,
)
from dataraum.server.storage import (
    LAKE_CATALOG_ALIAS,
    connect_session,
    get_anchor,
    health_probe,
)


@pytest.fixture(autouse=True)
def _wire_db(monkeypatch: pytest.MonkeyPatch, pg_url_clean: str, lake_anchor, lake_clean):
    """Every smoke needs DATABASE_URL + a clean lake schema. Bundled."""
    monkeypatch.setenv("DATABASE_URL", pg_url_clean)


# ---------------------------------------------------------------------------
# Anchor + health probe
# ---------------------------------------------------------------------------


class TestAnchorAndHealth:
    def test_anchor_alive_after_bootstrap(self):
        anchor = get_anchor()
        rows = anchor.execute(
            "SELECT database_name FROM duckdb_databases() "
            f"WHERE database_name = '{LAKE_CATALOG_ALIAS}'"
        ).fetchall()
        assert rows == [(LAKE_CATALOG_ALIAS,)]

    def test_health_probe_returns_ok(self):
        assert health_probe() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Per-session schema + USE propagation
# ---------------------------------------------------------------------------


class TestPerSessionSchemaScoping:
    def test_initialize_creates_schema(self):
        sid = "smoke-aaaa-0001"
        mgr = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr.initialize()
        try:
            schema = _session_id_to_schema(sid)
            seen = (
                get_anchor()
                .execute(
                    "SELECT schema_name FROM duckdb_schemas() "
                    f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
                    f"AND schema_name = '{schema}'"
                )
                .fetchall()
            )
            assert seen == [(schema,)]
        finally:
            mgr.close()

    def test_direct_execute_lands_in_session_schema(self):
        """``ctx.duckdb_conn.execute(...)`` is the dominant pipeline pattern."""
        sid = "smoke-bbbb-0002"
        mgr = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr.initialize()
        try:
            mgr._duckdb_conn.execute("CREATE TABLE direct_marker (x INT)")
            schema = _session_id_to_schema(sid)
            rows = (
                get_anchor()
                .execute(
                    "SELECT table_name FROM duckdb_tables() "
                    f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' AND schema_name = '{schema}'"
                )
                .fetchall()
            )
            assert ("direct_marker",) in rows
        finally:
            mgr.close()

    def test_cursor_inherits_use_via_wrapper(self):
        """``manager.duckdb_cursor()`` and analysis-style ``conn.cursor()`` both work."""
        sid = "smoke-cccc-0003"
        mgr = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr.initialize()
        try:
            # Path 1: manager.duckdb_cursor() (used by scheduler.py, mcp/server.py)
            with mgr.duckdb_cursor() as cursor:
                cursor.execute("CREATE TABLE marker_via_mgr_cursor (x INT)")

            # Path 2: ctx.duckdb_conn.cursor() (used by analysis/*)
            cursor_from_conn = mgr._duckdb_conn.cursor()
            try:
                cursor_from_conn.execute("CREATE TABLE marker_via_conn_cursor (x INT)")
            finally:
                cursor_from_conn.close()

            schema = _session_id_to_schema(sid)
            rows = (
                get_anchor()
                .execute(
                    "SELECT table_name FROM duckdb_tables() "
                    f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' AND schema_name = '{schema}' "
                    "ORDER BY table_name"
                )
                .fetchall()
            )
            names = {r[0] for r in rows}
            assert "marker_via_mgr_cursor" in names
            assert "marker_via_conn_cursor" in names
        finally:
            mgr.close()

    def test_cursor_of_cursor_inherits_use(self):
        """Cursor-of-cursor chains must preserve USE.

        Analysis modules in ``analysis/*`` open sub-cursors via
        ``duckdb_conn.cursor()`` where ``duckdb_conn`` is itself a cursor
        handed down from a phase. The wrapper returns ``_LakeScopedConnection``
        instances from ``cursor()`` so chained derivations stay in the
        session schema; without the recursive wrapping, a raw DuckDB cursor's
        ``.cursor()`` opens with ``current_database='memory', current_schema='main'``.
        """
        sid = "smoke-cursor-of-cursor"
        mgr = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr.initialize()
        try:
            with mgr.duckdb_cursor() as phase_cursor:
                with phase_cursor.cursor() as sub_cursor:
                    sub_cursor.execute("CREATE TABLE sub_marker (x INT)")

            schema = _session_id_to_schema(sid)
            rows = (
                get_anchor()
                .execute(
                    "SELECT table_name FROM duckdb_tables() "
                    f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' AND schema_name = '{schema}'"
                )
                .fetchall()
            )
            assert ("sub_marker",) in rows
        finally:
            mgr.close()

    def test_two_sessions_do_not_leak(self):
        a = ConnectionManager(ConnectionConfig.for_workspace(), session_id="smoke-aaa-leak")
        b = ConnectionManager(ConnectionConfig.for_workspace(), session_id="smoke-bbb-leak")
        a.initialize()
        b.initialize()
        try:
            with a.duckdb_cursor() as ca:
                ca.execute("CREATE TABLE leak_a (x INT)")
            with b.duckdb_cursor() as cb:
                cb.execute("CREATE TABLE leak_b (x INT)")

            with a.duckdb_cursor() as ca:
                rows_a = {
                    r[0]
                    for r in ca.execute(
                        "SELECT table_name FROM duckdb_tables() "
                        "WHERE schema_name = current_schema() "
                        f"AND database_name = '{LAKE_CATALOG_ALIAS}'"
                    ).fetchall()
                }
            with b.duckdb_cursor() as cb:
                rows_b = {
                    r[0]
                    for r in cb.execute(
                        "SELECT table_name FROM duckdb_tables() "
                        "WHERE schema_name = current_schema() "
                        f"AND database_name = '{LAKE_CATALOG_ALIAS}'"
                    ).fetchall()
                }
            assert "leak_a" in rows_a and "leak_b" not in rows_a
            assert "leak_b" in rows_b and "leak_a" not in rows_b
        finally:
            a.close()
            b.close()


# ---------------------------------------------------------------------------
# DDL patterns the pipeline depends on
# ---------------------------------------------------------------------------


class TestPipelineDDLPatterns:
    """DuckLake feature parity with the patterns the pipeline phases use today."""

    def test_alter_table_rename(self):
        sid = "smoke-ddl-rename"
        mgr = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr.initialize()
        try:
            with mgr.duckdb_cursor() as cur:
                cur.execute("CREATE TABLE before_rename (x INT)")
                cur.execute('ALTER TABLE "before_rename" RENAME TO "after_rename"')
                names = {
                    r[0]
                    for r in cur.execute(
                        "SELECT table_name FROM duckdb_tables() "
                        "WHERE schema_name = current_schema() "
                        f"AND database_name = '{LAKE_CATALOG_ALIAS}'"
                    ).fetchall()
                }
            assert "after_rename" in names
            assert "before_rename" not in names
        finally:
            mgr.close()

    def test_create_and_drop_view(self):
        sid = "smoke-ddl-view"
        mgr = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr.initialize()
        try:
            with mgr.duckdb_cursor() as cur:
                cur.execute("CREATE TABLE base_t (x INT)")
                cur.execute("INSERT INTO base_t VALUES (1), (2), (3)")
                cur.execute("CREATE VIEW v_double AS SELECT x * 2 AS y FROM base_t")
                vals = sorted(r[0] for r in cur.execute("SELECT y FROM v_double").fetchall())
                assert vals == [2, 4, 6]
                cur.execute("DROP VIEW v_double")
                views = cur.execute(
                    "SELECT view_name FROM duckdb_views() "
                    "WHERE schema_name = current_schema() "
                    f"AND database_name = '{LAKE_CATALOG_ALIAS}'"
                ).fetchall()
                assert ("v_double",) not in views
        finally:
            mgr.close()


# ---------------------------------------------------------------------------
# Storage backend: parquet files actually appear under DATA_PATH
# ---------------------------------------------------------------------------


class TestStorageBackend:
    def test_writes_produce_parquet_on_disk(self, lake_data_path: str):
        """DuckLake holds INSERT output in memory until ``CHECKPOINT``.

        Pipeline code paths that need the data on disk (export, hand-off, etc.)
        must call ``CHECKPOINT``; this smoke proves the disk path receives the
        parquet after that.
        """
        sid = "smoke-parquet-flush"
        mgr = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr.initialize()
        try:
            with mgr.duckdb_cursor() as cur:
                cur.execute("CREATE TABLE on_disk (x INT)")
                cur.execute("INSERT INTO on_disk VALUES (1), (2), (3)")
                cur.execute("CHECKPOINT")

            data_root = Path(lake_data_path)
            parquet_files = list(data_root.rglob("*.parquet"))
            assert parquet_files, (
                f"expected parquet files under {data_root} after CHECKPOINT, found none"
            )
        finally:
            mgr.close()


# ---------------------------------------------------------------------------
# Loader carve-out: ephemeral conns must NOT touch the lake
# ---------------------------------------------------------------------------


class TestLoaderCarveOut:
    def test_ephemeral_in_memory_conn_does_not_pollute_lake(self):
        """csv/json loaders' ``get_schema`` uses ``duckdb.connect(':memory:')``."""
        eph = duckdb.connect(":memory:")
        try:
            eph.execute("CREATE TABLE ephemeral_only (x INT)")
            eph.execute("INSERT INTO ephemeral_only VALUES (42)")
        finally:
            eph.close()

        seen = (
            get_anchor()
            .execute(
                "SELECT table_name FROM duckdb_tables() "
                f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
                "AND table_name = 'ephemeral_only'"
            )
            .fetchall()
        )
        assert seen == [], (
            "ephemeral in-memory conn leaked a table into the lake — loader carve-out broken"
        )


# ---------------------------------------------------------------------------
# Archive semantics (Option A): end-session leaves schema in place
# ---------------------------------------------------------------------------


class TestArchiveSemantics:
    def test_end_session_does_not_drop_lake_schema(self):
        """Option A: archive is a workspace-state flag; lake schema is untouched.

        ``DuckDB doesn't support ALTER SCHEMA RENAME``; the design intentionally
        leaves ``lake.session_<sid>`` in place after end_session so resume_session
        can rebind via ``bind_session_id(sid)`` without lake-side DDL.
        """
        sid = "smoke-archive-keepschema"
        mgr = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr.initialize()
        try:
            with mgr.duckdb_cursor() as cur:
                cur.execute("CREATE TABLE survives_archive (x INT)")
                cur.execute("INSERT INTO survives_archive VALUES (1)")
        finally:
            mgr.close()  # simulates end_session manager teardown

        # Schema + data still present from anchor's perspective.
        schema = _session_id_to_schema(sid)
        rows = (
            get_anchor()
            .execute(
                "SELECT table_name FROM duckdb_tables() "
                f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' AND schema_name = '{schema}'"
            )
            .fetchall()
        )
        assert ("survives_archive",) in rows

        # Resume: rebind via a fresh manager with the same sid. The cursor
        # should see the surviving data.
        mgr2 = ConnectionManager(ConnectionConfig.for_workspace(), session_id=sid)
        mgr2.initialize()
        try:
            with mgr2.duckdb_cursor() as cur:
                val = cur.execute("SELECT x FROM survives_archive").fetchone()
            assert val == (1,)
        finally:
            mgr2.close()


# ---------------------------------------------------------------------------
# connect_session smoke (direct, no manager)
# ---------------------------------------------------------------------------


def test_connect_session_returns_independent_connection():
    """Direct smoke of the storage primitive that ConnectionManager builds on."""
    a = connect_session()
    b = connect_session()
    try:
        assert a is not b
        # Catalog state is shared: both see the lake catalog.
        for c in (a, b):
            rows = c.execute(
                "SELECT database_name FROM duckdb_databases() "
                f"WHERE database_name = '{LAKE_CATALOG_ALIAS}'"
            ).fetchall()
            assert rows == [(LAKE_CATALOG_ALIAS,)]
    finally:
        a.close()
        b.close()
