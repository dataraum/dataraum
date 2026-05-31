"""Tests for CSV loader.

Tests the sources.csv module which implements VARCHAR-first CSV loading.
Uses small_finance fixture data from tests/integration/fixtures/.
"""

from pathlib import Path

from dataraum.core.models import SourceConfig
from dataraum.sources.csv import CSVLoader

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures" / "small_finance"


class TestCSVLoader:
    """Tests for CSVLoader."""

    def test_get_schema(self, duckdb_conn):
        """Test getting schema from a CSV file."""
        loader = CSVLoader()
        config = SourceConfig(
            name="payment_methods",
            source_type="csv",
            path=str(FIXTURES_DIR / "payment_methods.csv"),
        )

        result = loader.get_schema(config)

        assert result.success
        columns = result.value
        assert columns
        assert len(columns) == 3  # Business Id, Payment method, Credit card
        assert columns[0].position == 0
        assert columns[0].source_type == "VARCHAR"
        assert columns[0].nullable is True
        assert len(columns[0].sample_values) > 0

    def test_get_schema_missing_file(self):
        """A missing URI surfaces DuckDB's read error via Result.fail (DAT-389).

        ``get_schema`` no longer stats the filesystem (the path is an opaque
        URI handed verbatim to ``read_csv_auto``); an unreadable path fails the
        Result with DuckDB's error rather than a pathlib pre-check.
        """
        loader = CSVLoader()
        config = SourceConfig(
            name="missing",
            source_type="csv",
            path="nonexistent.csv",
        )

        result = loader.get_schema(config)

        assert not result.success
        assert result.error
        assert "CSV schema" in result.error

    def test_get_schema_no_path(self):
        """Test error handling when path is not set."""
        loader = CSVLoader()
        config = SourceConfig(name="no_path", source_type="csv")

        result = loader.get_schema(config)

        assert not result.success
        assert "path" in result.error.lower()

    def test_load_single_file(self, duckdb_conn, session):
        """Test loading a single CSV file."""
        loader = CSVLoader()
        config = SourceConfig(
            name="payment_methods",
            source_type="csv",
            path=str(FIXTURES_DIR / "payment_methods.csv"),
        )

        result = loader.load(config, duckdb_conn, session)

        assert result.success, f"Load failed: {result.error}"

        staging_result = result.value
        assert staging_result.source_id is not None
        assert len(staging_result.tables) == 1

        table = staging_result.tables[0]
        # Post-DAT-341: bare name is ``<source>__<table>``
        assert table.table_name == "payment_methods__payment_methods"
        assert table.raw_table_name == "payment_methods__payment_methods"
        assert table.row_count > 0
        assert table.column_count == 3

        # Verify table exists in lake.raw
        tables = duckdb_conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE database_name = 'lake' AND schema_name = 'raw'"
        ).fetchall()
        table_names = [t[0] for t in tables]
        assert "payment_methods__payment_methods" in table_names

    def test_load_all_columns_varchar(self, duckdb_conn, session):
        """Verify all loaded columns are VARCHAR (VARCHAR-first approach)."""
        loader = CSVLoader()
        config = SourceConfig(
            name="customers",
            source_type="csv",
            path=str(FIXTURES_DIR / "customers.csv"),
        )

        result = loader.load(config, duckdb_conn, session)
        assert result.success

        schema = duckdb_conn.execute('DESCRIBE lake.raw."customers__customers"').fetchall()

        # DESCRIBE returns (column_name, column_type, null, key, default, extra)
        for row in schema:
            col_name, data_type = row[0], row[1]
            assert data_type == "VARCHAR", f"Column {col_name} is {data_type}, expected VARCHAR"

    def test_load_null_values_recognized(self, duckdb_conn, session):
        """Test that null values (--) are converted to NULL during loading."""
        loader = CSVLoader()
        config = SourceConfig(
            name="transactions",
            source_type="csv",
            path=str(FIXTURES_DIR / "transactions.csv"),
        )

        result = loader.load(config, duckdb_conn, session)
        assert result.success, f"Load failed: {result.error}"

        # Transactions has -- values in customer_name and vendor_name columns
        null_count = duckdb_conn.execute(
            'SELECT COUNT(*) FROM lake.raw."transactions__transactions" '
            'WHERE "customer_name" IS NULL'
        ).fetchone()[0]

        assert null_count > 0, "Expected some NULL values from -- conversion"

    def test_load_missing_file(self, duckdb_conn, session):
        """A missing URI fails the load via Result.fail (DuckDB error, DAT-389)."""
        loader = CSVLoader()
        config = SourceConfig(
            name="missing",
            source_type="csv",
            path="nonexistent.csv",
        )

        result = loader.load(config, duckdb_conn, session)
        assert not result.success
        assert result.error

    def test_sql_injection_in_path_is_inert(self, duckdb_conn, session):
        """A source URI crafted to break out of the SQL literal must stay inert.

        The loader f-string-interpolates the URI into ``read_csv_auto('<uri>')``,
        escaping ``'`` the DuckDB way (doubling, not backslash). A key carrying
        ``'); DROP TABLE ...; --`` must therefore be treated as one literal path
        — DuckDB looks for an object with that exact (nonexistent) name and the
        load fails cleanly — rather than the injected statement parsing and
        executing. We assert both: the load fails, and the canary table the
        injection tried to drop is still present afterward.
        """
        # Canary the injection payload tries to DROP via a lateral statement.
        duckdb_conn.execute("CREATE TABLE injection_canary (x INTEGER)")

        malicious_path = "evil.csv'); DROP TABLE injection_canary; SELECT * FROM read_csv_auto('x"
        loader = CSVLoader()
        config = SourceConfig(name="evil", source_type="csv", path=malicious_path)

        result = loader.load(config, duckdb_conn, session)

        # The whole string is one (missing-object) literal path → load fails,
        # no lateral DROP executes.
        assert not result.success
        assert result.error

        # The canary survives: the DROP never ran.
        survived = duckdb_conn.execute(
            "SELECT table_name FROM duckdb_tables() WHERE table_name = 'injection_canary'"
        ).fetchall()
        assert survived == [("injection_canary",)], "injection executed a lateral DROP"

    def test_single_quote_in_path_is_literal(self, duckdb_conn, session):
        """A legitimate key containing a single quote stays a literal path.

        S3 keys may legitimately contain ``'``; DuckDB's ``''`` escaping keeps
        such a key as one path (here a missing object) rather than a parse
        error or lateral execution — so the load fails with a read error, not a
        SQL syntax error.
        """
        loader = CSVLoader()
        config = SourceConfig(
            name="quoted",
            source_type="csv",
            path="s3://dataraum-lake/o'brien/orders.csv",
        )

        result = loader.load(config, duckdb_conn, session)

        assert not result.success
        assert result.error
        # A literal-treated missing object, not a SQL parse error.
        assert "syntax error" not in result.error.lower()
        assert "parser error" not in result.error.lower()
