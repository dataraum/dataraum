"""Integration smoke against a live MS SQL Server.

Skipped unless DATARAUM_MSSQL_TEST_URL is set. To run locally:

  1. Stand up a SQL Server container:
     container run -d --name sql2025 \\
       -e ACCEPT_EULA=Y -e MSSQL_SA_PASSWORD='YourStrongP@ss1' \\
       -p 1433:1433 mcr.microsoft.com/mssql/server:2025-latest

  2. Create a tiny schema as `sa`:
     CREATE DATABASE Smoke;
     USE Smoke;
     CREATE TABLE dbo.Items (
       ItemID INT IDENTITY(1,1) PRIMARY KEY,
       ItemCode NVARCHAR(50) NOT NULL,
       Price DECIMAL(18,2),
       CreatedAt DATETIME2 DEFAULT SYSUTCDATETIME()
     );
     INSERT INTO dbo.Items (ItemCode, Price) VALUES
       ('A', 10.00), ('B', 22.50), ('C', 99.99);

  3. Create a read-only user (see docs/db-sources.md).

  4. Export the URL and run:
     export DATARAUM_MSSQL_TEST_URL="mssql://reader:pwd@host:1433/Smoke?TrustServerCertificate=yes"
     uv run pytest tests/integration/sources/test_db_recipe_mssql.py -v

This test exercises the real DuckDB community mssql extension against
a real server. It is intentionally lightweight (one recipe, three rows)
so it stays under a few seconds.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import pytest

from dataraum.sources.backends import extract_backend
from dataraum.sources.db_recipe import RecipeTable, parse_recipe

_TEST_URL_ENV = "DATARAUM_MSSQL_TEST_URL"

pytestmark = pytest.mark.skipif(
    not os.environ.get(_TEST_URL_ENV),
    reason=(
        f"Set {_TEST_URL_ENV} to a live MS SQL Server URL to run these tests. "
        "See the module docstring for setup steps."
    ),
)


@pytest.fixture
def mssql_url() -> str:
    return os.environ[_TEST_URL_ENV]


@pytest.fixture
def duckdb_conn():
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


class TestRealMSSQLExtraction:
    """End-to-end against a real MSSQL via the community DuckDB extension."""

    def test_extension_loads_and_extracts(self, mssql_url, duckdb_conn) -> None:
        """INSTALL FROM community + ATTACH READ_ONLY + CTAS the user's SQL."""
        result = extract_backend(
            backend="mssql",
            url=mssql_url,
            queries=[
                RecipeTable(
                    name="items",
                    sql="SELECT ItemID, ItemCode, Price FROM dbo.Items",
                ),
            ],
            duckdb_conn=duckdb_conn,
            raw_prefix="smoke_",
        )

        assert result.success, result.error
        payload = result.unwrap()
        assert len(payload.tables) == 1

        table = payload.tables[0]
        assert table.duckdb_table == "smoke_items"
        assert table.row_count >= 1, (
            "Expected at least one row in dbo.Items — did the smoke setup run?"
        )

        col_names = [c[0] for c in table.columns]
        assert col_names == ["ItemID", "ItemCode", "Price"]
        col_types = {c[0]: c[1] for c in table.columns}
        assert col_types["ItemID"].upper() in ("INTEGER", "BIGINT")
        # MSSQL DECIMAL(18,2) → DuckDB DECIMAL(18,2)
        assert col_types["Price"].upper().startswith("DECIMAL")

    def test_read_only_blocks_writes(self, mssql_url, duckdb_conn) -> None:
        """The (READ_ONLY) ATTACH flag must block writes from the extension layer.

        Even a SysAdmin connection cannot CREATE inside an attached
        read-only database — verified during the Phase 1 spike. This
        test pins that behavior so a future DuckDB-version bump that
        loosens the check would fail loudly.
        """
        duckdb_conn.execute("INSTALL mssql FROM community")
        duckdb_conn.execute("LOAD mssql")
        duckdb_conn.execute(f"ATTACH '{mssql_url}' AS smoke_ro (TYPE MSSQL, READ_ONLY)")
        try:
            with pytest.raises(duckdb.Error) as excinfo:
                duckdb_conn.execute("CREATE TABLE smoke_ro.dbo.dat286_write_probe (a INT)")
            assert "read-only" in str(excinfo.value).lower()
        finally:
            duckdb_conn.execute("DETACH smoke_ro")

    def test_unknown_column_in_recipe_fails_loud(self, mssql_url, duckdb_conn) -> None:
        """Bad SQL in a recipe surfaces with the offending table name."""
        result = extract_backend(
            backend="mssql",
            url=mssql_url,
            queries=[
                RecipeTable(
                    name="broken",
                    sql="SELECT NonExistentColumn FROM dbo.Items",
                ),
            ],
            duckdb_conn=duckdb_conn,
        )
        assert not result.success
        assert "broken" in result.error
        assert "SELECT failed" in result.error


class TestRealMSSQLViaRecipe:
    """End-to-end via the recipe parser → manager → extract path."""

    def test_recipe_parses_and_extracts(
        self, mssql_url, duckdb_conn, tmp_path: Path, monkeypatch
    ) -> None:
        """Recipe yaml parsed, persisted-config queries fed to extract_backend."""
        recipe_path = tmp_path / "smoke.yaml"
        recipe_path.write_text(
            "backend: mssql\n"
            "tables:\n"
            "  items:\n"
            "    sql: SELECT ItemID, ItemCode, Price FROM dbo.Items\n"
        )

        # 1. Parse the recipe — what the manager does at add_recipe_source.
        parsed = parse_recipe(recipe_path)
        assert parsed.success, parsed.error
        recipe = parsed.value
        assert recipe is not None
        assert recipe.backend == "mssql"
        assert [t.name for t in recipe.tables] == ["items"]

        # 2. Extract — what the pipeline does at import time.
        result = extract_backend(
            backend=recipe.backend,
            url=mssql_url,
            queries=recipe.tables,
            duckdb_conn=duckdb_conn,
            raw_prefix="smoke_",
        )
        assert result.success, result.error
        assert result.unwrap().tables[0].row_count >= 1
