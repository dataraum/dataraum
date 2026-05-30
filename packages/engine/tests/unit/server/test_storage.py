"""Tests for the DuckLake bootstrap and shared in-memory anchor."""

from __future__ import annotations

import pytest

from dataraum.server.storage import (
    LAKE_CATALOG_ALIAS,
    LAKE_DB_NAME,
    S3_SECRET_NAME,
    _build_s3_secret_sql,
    _pg_url_to_libpq,
    bootstrap_lake,
    connect_session,
    get_anchor,
    health_probe,
)


class TestPgUrlToLibpq:
    """Conversion from postgresql:// URL to libpq keyword-value form."""

    def test_full_url(self):
        result = _pg_url_to_libpq("postgresql://alice:s3cret@db.example.com:5432/mydb")
        assert "dbname=mydb" in result
        assert "host=db.example.com" in result
        assert "port=5432" in result
        assert "user=alice" in result
        assert "password=s3cret" in result

    def test_minimal_url(self):
        result = _pg_url_to_libpq("postgresql://localhost/justdb")
        assert "dbname=justdb" in result
        assert "host=localhost" in result
        assert "user=" not in result
        assert "password=" not in result

    def test_password_with_spaces_is_quoted(self):
        result = _pg_url_to_libpq("postgresql://alice:hello%20world@db/mydb")
        # urlparse already decoded %20 → " "
        assert "password='hello world'" in result

    def test_password_with_quote_is_escaped(self):
        result = _pg_url_to_libpq("postgresql://alice:o%27brien@db/mydb")
        # single quote present in password → quoted + backslash-escaped
        assert "password='o\\'brien'" in result


class TestBuildS3SecretSql:
    """The object-store ``CREATE OR REPLACE SECRET`` builder (DAT-388).

    Pure string surgery — no DuckDB connection, no network — so the SQL shape,
    escaping, and the ``USE_SSL`` toggle are verified offline (we don't test
    DuckLake-over-S3 itself; that's DuckLake's concern).
    """

    def test_well_formed_secret_for_clean_values(self):
        sql = _build_s3_secret_sql(
            access_key_id="dataraum",
            secret_access_key="dataraum-s3-secret",
            endpoint="seaweedfs:8333",
            region="us-east-1",
            use_ssl=False,
        )
        assert sql == (
            f"CREATE OR REPLACE SECRET {S3_SECRET_NAME} ("
            "TYPE s3, "
            "KEY_ID 'dataraum', "
            "SECRET 'dataraum-s3-secret', "
            "ENDPOINT 'seaweedfs:8333', "
            "REGION 'us-east-1', "
            "URL_STYLE 'path', "
            "USE_SSL false"
            ")"
        )

    def test_use_ssl_true_renders_true(self):
        sql = _build_s3_secret_sql(
            access_key_id="k",
            secret_access_key="s",
            endpoint="s3.example.com:443",
            region="eu-central-1",
            use_ssl=True,
        )
        assert "USE_SSL true" in sql

    def test_escapes_single_quote_in_secret(self):
        # A secret containing a single quote must not break out of the literal.
        sql = _build_s3_secret_sql(
            access_key_id="k",
            secret_access_key="pa'ss",
            endpoint="h:8333",
            region="us-east-1",
            use_ssl=False,
        )
        assert "SECRET 'pa\\'ss'" in sql


class TestHealthProbe:
    """The ``/health`` payload derived from the anchor state."""

    def test_returns_not_bootstrapped_before_bootstrap(self, no_anchor):
        assert health_probe() == {"status": "not_bootstrapped"}

    def test_returns_ok_after_bootstrap(self, lake_anchor):
        assert health_probe() == {"status": "ok"}


class TestBootstrap:
    """``bootstrap_lake`` opens an anchor and is idempotent; ``teardown_lake`` clears it."""

    def test_bootstrap_opens_anchor(self, lake_anchor):
        anchor = get_anchor()
        rows = anchor.execute(
            "SELECT database_name FROM duckdb_databases() "
            f"WHERE database_name = '{LAKE_CATALOG_ALIAS}'"
        ).fetchall()
        assert rows == [(LAKE_CATALOG_ALIAS,)]

    def test_bootstrap_is_idempotent(self, lake_anchor, lake_catalog_url, tmp_path):
        # Second call must not reopen, raise, or replace the anchor.
        first = get_anchor()
        bootstrap_lake(lake_catalog_url, str(tmp_path))
        second = get_anchor()
        assert first is second

    def test_bootstrap_fails_loud_on_bad_catalog(self, no_anchor, tmp_path):
        with pytest.raises(RuntimeError, match="DuckLake bootstrap failed"):
            bootstrap_lake(
                "postgresql://nobody:nothing@127.0.0.1:1/no_such_db",
                str(tmp_path),
            )

    def test_get_anchor_raises_before_bootstrap(self, no_anchor):
        with pytest.raises(RuntimeError, match="not bootstrapped"):
            get_anchor()

    def test_connect_session_raises_before_bootstrap(self, no_anchor):
        with pytest.raises(RuntimeError, match="not bootstrapped"):
            connect_session()


class TestConnectSession:
    """``connect_session`` returns fresh connections sharing catalog state."""

    def test_returns_fresh_connection_each_call(self, lake_anchor, lake_clean):
        a = connect_session()
        b = connect_session()
        try:
            assert a is not b
        finally:
            a.close()
            b.close()

    def test_shares_catalog_state_with_anchor(self, lake_anchor, lake_clean):
        anchor = get_anchor()
        # Schema created via a session connection must be visible to the anchor.
        with connect_session() as conn:
            conn.execute(f"CREATE SCHEMA {LAKE_CATALOG_ALIAS}.session_probe_share")
        seen = anchor.execute(
            "SELECT schema_name FROM duckdb_schemas() "
            f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
            "AND schema_name = 'session_probe_share'"
        ).fetchall()
        assert seen, "schema created via session connection not visible from anchor"

    def test_per_connection_use_isolation(self, lake_anchor, lake_clean):
        """Critical DuckLake assumption: ``USE`` on one connection does not
        leak into another connection to the same named in-memory DB.

        This is the load-bearing reason connect_session opens a *new connection*
        per ConnectionManager rather than reusing cursor() on the anchor.
        """
        a = connect_session()
        b = connect_session()
        try:
            a.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE_CATALOG_ALIAS}.session_a")
            b.execute(f"CREATE SCHEMA IF NOT EXISTS {LAKE_CATALOG_ALIAS}.session_b")

            a.execute(f"USE {LAKE_CATALOG_ALIAS}.session_a")
            b.execute(f"USE {LAKE_CATALOG_ALIAS}.session_b")

            a.execute("CREATE TABLE marker_a (x INT)")
            b.execute("CREATE TABLE marker_b (x INT)")

            a_tables = a.execute(
                "SELECT table_name FROM duckdb_tables() "
                f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
                "AND schema_name = 'session_a'"
            ).fetchall()
            b_tables = b.execute(
                "SELECT table_name FROM duckdb_tables() "
                f"WHERE database_name = '{LAKE_CATALOG_ALIAS}' "
                "AND schema_name = 'session_b'"
            ).fetchall()

            assert ("marker_a",) in a_tables
            assert ("marker_b",) in b_tables
        finally:
            a.close()
            b.close()

    def test_lake_db_name_constant(self):
        # Sanity: the constant is the named in-memory form documented in
        # DuckDB's dbapi page (":memory:<name>").
        assert LAKE_DB_NAME.startswith(":memory:")
        assert LAKE_DB_NAME != ":memory:"
