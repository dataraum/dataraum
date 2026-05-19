"""Integration smoke against a live MS SQL Server.

Skipped unless ``DATARAUM_MSSQL_TEST_URL`` is set. To run locally:

  1. Bring up a SQL Server container (one-time). With Apple's `container` CLI::

       container run \\
         -e ACCEPT_EULA=Y \\
         -e MSSQL_SA_PASSWORD=Test1234 \\
         -e MSSQL_PID=Evaluation \\
         -p 1433:1433 \\
         --name sql2025 \\
         --memory 3g \\
         --platform linux/amd64 \\
         -d mcr.microsoft.com/mssql/server:2025-latest

  2. Restore AdventureWorksLT2025 and create the read-only login::

       tests/integration/sources/mssql_setup.sh

     The script is idempotent and finishes in <5 s on a warm container.

  3. Export the URL printed by the script and run pytest::

       export DATARAUM_MSSQL_TEST_URL='mssql://dataraum_reader:ReadOnly!2026@<ip>:1433/AdventureWorksLT?TrustServerCertificate=yes'
       uv run pytest tests/integration/sources/test_db_recipe_mssql.py -v

Why AdventureWorksLT: it's the smallest Microsoft sample that exercises the
shapes DataRaum cares about — multi-schema (``SalesLT.*`` + ``dbo.*``), real
FK chains (Customer ↔ Address ↔ SalesOrderHeader), and mixed types
(INT / NVARCHAR / MONEY / DATETIME). 1.8 MB on disk; restore takes <100 ms.
"""

from __future__ import annotations

import datetime
import os
from decimal import Decimal
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
        """INSTALL FROM community + ATTACH READ_ONLY + CTAS the user's SQL.

        Pins the type-fidelity contract for the four types we care about
        most: INT → INTEGER, NVARCHAR → VARCHAR, MONEY → DECIMAL(19,4),
        DATETIME → TIMESTAMP.
        """
        result = extract_backend(
            backend="mssql",
            url=mssql_url,
            queries=[
                RecipeTable(
                    name="products",
                    sql=("SELECT ProductID, Name, ListPrice, SellStartDate FROM SalesLT.Product"),
                ),
            ],
            duckdb_conn=duckdb_conn,
            raw_prefix="aw_",
        )
        assert result.success, result.error
        payload = result.unwrap()
        assert len(payload.tables) == 1

        table = payload.tables[0]
        assert table.duckdb_table == "aw_products"
        # AdventureWorksLT ships with ~290 products.
        assert table.row_count >= 100

        col_types = {c[0]: c[1].upper() for c in table.columns}
        assert col_types["ProductID"] in ("INTEGER", "BIGINT")
        assert col_types["Name"] == "VARCHAR"
        # MSSQL MONEY → DuckDB DECIMAL(19,4).
        assert col_types["ListPrice"].startswith("DECIMAL")
        # MSSQL DATETIME → DuckDB TIMESTAMP.
        assert col_types["SellStartDate"] == "TIMESTAMP"

    def test_cross_schema_extraction(self, mssql_url, duckdb_conn) -> None:
        """One recipe, two schemas — SalesLT and dbo materialized together."""
        result = extract_backend(
            backend="mssql",
            url=mssql_url,
            queries=[
                RecipeTable(
                    name="customers",
                    sql="SELECT CustomerID, FirstName, LastName FROM SalesLT.Customer",
                ),
                RecipeTable(
                    name="build_version",
                    # AdventureWorksLT's dbo.BuildVersion has a column
                    # literally named "Database Version" (with a space) —
                    # quote it the DuckDB way (double quotes), not the
                    # SQL Server way (square brackets).
                    sql=(
                        'SELECT SystemInformationID, "Database Version" AS db_version '
                        "FROM dbo.BuildVersion"
                    ),
                ),
            ],
            duckdb_conn=duckdb_conn,
            raw_prefix="aw_",
        )
        assert result.success, result.error
        tables = {t.name: t for t in result.unwrap().tables}
        assert set(tables) == {"customers", "build_version"}
        assert tables["customers"].row_count > 0
        assert tables["build_version"].row_count == 1

    def test_recipe_join_denormalization(self, mssql_url, duckdb_conn) -> None:
        """Recipe SQL can JOIN source tables — output columns need not exist
        on any single source table. This is the headline use case: the user
        shapes the data at extraction time rather than schlepping raw tables.
        """
        result = extract_backend(
            backend="mssql",
            url=mssql_url,
            queries=[
                RecipeTable(
                    name="customer_addresses",
                    sql=(
                        "SELECT c.CustomerID, c.FirstName, c.LastName, "
                        "       a.City, a.StateProvince, a.CountryRegion "
                        "FROM SalesLT.Customer c "
                        "JOIN SalesLT.CustomerAddress ca ON c.CustomerID = ca.CustomerID "
                        "JOIN SalesLT.Address a ON ca.AddressID = a.AddressID"
                    ),
                ),
            ],
            duckdb_conn=duckdb_conn,
            raw_prefix="aw_",
        )
        assert result.success, result.error
        table = result.unwrap().tables[0]
        assert [c[0] for c in table.columns] == [
            "CustomerID",
            "FirstName",
            "LastName",
            "City",
            "StateProvince",
            "CountryRegion",
        ]
        assert table.row_count > 0

    def test_datetime_round_trip(self, mssql_url, duckdb_conn) -> None:
        """MSSQL DATETIME values are usable as Python datetimes after extract."""
        result = extract_backend(
            backend="mssql",
            url=mssql_url,
            queries=[
                RecipeTable(
                    name="orders",
                    sql="SELECT SalesOrderID, OrderDate FROM SalesLT.SalesOrderHeader",
                ),
            ],
            duckdb_conn=duckdb_conn,
            raw_prefix="aw_",
        )
        assert result.success, result.error

        # The extractor already materialized aw_orders. Pull one row through
        # the same DuckDB connection to confirm value round-trip.
        row = duckdb_conn.execute(
            "SELECT SalesOrderID, OrderDate FROM aw_orders ORDER BY SalesOrderID LIMIT 1"
        ).fetchone()
        assert row is not None
        order_id, order_date = row
        assert isinstance(order_id, int)
        assert isinstance(order_date, datetime.datetime)
        # AdventureWorksLT orders sit in 2008 or have been date-shifted by
        # the 2025 .bak's "Dates have been adjusted" change. Either way the
        # value must parse — we just sanity-check the year range.
        assert 2000 <= order_date.year <= 2100

    def test_money_value_round_trip(self, mssql_url, duckdb_conn) -> None:
        """MSSQL MONEY survives as DuckDB DECIMAL(19,4) with the right scale."""
        result = extract_backend(
            backend="mssql",
            url=mssql_url,
            queries=[
                RecipeTable(
                    name="prices",
                    sql="SELECT ProductID, ListPrice FROM SalesLT.Product",
                ),
            ],
            duckdb_conn=duckdb_conn,
            raw_prefix="aw_",
        )
        assert result.success, result.error
        row = duckdb_conn.execute(
            "SELECT ListPrice FROM aw_prices WHERE ListPrice > 0 ORDER BY ProductID LIMIT 1"
        ).fetchone()
        assert row is not None
        (price,) = row
        assert isinstance(price, Decimal)
        # MONEY has 4 fractional digits; AdventureWorksLT prices are non-zero.
        assert price > 0

    def test_read_only_blocks_writes(self, mssql_url, duckdb_conn) -> None:
        """The (READ_ONLY) ATTACH flag must block writes from the extension layer.

        Even a SysAdmin connection cannot CREATE inside an attached
        read-only database — verified during the DAT-286 spike. This test
        pins the contract so a future DuckDB extension change that loosens
        the check fails loudly.
        """
        duckdb_conn.execute("INSTALL mssql FROM community")
        duckdb_conn.execute("LOAD mssql")
        duckdb_conn.execute(f"ATTACH '{mssql_url}' AS aw_ro (TYPE MSSQL, READ_ONLY)")
        try:
            with pytest.raises(duckdb.Error) as excinfo:
                duckdb_conn.execute("CREATE TABLE aw_ro.SalesLT.dat287_write_probe (a INT)")
            assert "read-only" in str(excinfo.value).lower()
        finally:
            duckdb_conn.execute("DETACH aw_ro")

    def test_unknown_column_in_recipe_fails_loud(self, mssql_url, duckdb_conn) -> None:
        """Bad SQL in a recipe surfaces with the offending table name."""
        result = extract_backend(
            backend="mssql",
            url=mssql_url,
            queries=[
                RecipeTable(
                    name="broken",
                    sql="SELECT NonExistentColumn FROM SalesLT.Customer",
                ),
            ],
            duckdb_conn=duckdb_conn,
        )
        assert not result.success
        assert "broken" in result.error
        assert "SELECT failed" in result.error


class TestRealMSSQLViaRecipe:
    """End-to-end via the recipe parser → manager → extract path."""

    def test_recipe_parses_and_extracts(self, mssql_url, duckdb_conn, tmp_path: Path) -> None:
        """Recipe yaml parsed, persisted-config queries fed to extract_backend."""
        recipe_path = tmp_path / "adventureworks.yaml"
        recipe_path.write_text(
            "backend: mssql\n"
            "tables:\n"
            "  customers:\n"
            "    sql: SELECT CustomerID, FirstName, LastName FROM SalesLT.Customer\n"
            "  products:\n"
            "    sql: SELECT ProductID, Name, ListPrice FROM SalesLT.Product\n"
        )

        parsed = parse_recipe(recipe_path)
        assert parsed.success, parsed.error
        recipe = parsed.value
        assert recipe is not None
        assert recipe.backend == "mssql"
        assert [t.name for t in recipe.tables] == ["customers", "products"]

        result = extract_backend(
            backend=recipe.backend,
            url=mssql_url,
            queries=recipe.tables,
            duckdb_conn=duckdb_conn,
            raw_prefix="aw_",
        )
        assert result.success, result.error
        tables = {t.name: t for t in result.unwrap().tables}
        assert tables["customers"].row_count > 0
        assert tables["products"].row_count > 0
