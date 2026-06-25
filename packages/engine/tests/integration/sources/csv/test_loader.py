"""Integration tests for the CSV loader against a real DuckLake substrate."""

from __future__ import annotations

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.sources.csv.loader import CSVLoader
from dataraum.sources.csv.models import StagedTable
from dataraum.sources.csv.null_values import load_null_value_config
from dataraum.storage import Source


@pytest.fixture
def loader() -> CSVLoader:
    return CSVLoader()


def _write_csv(tmp_path, name: str, content: str) -> str:
    path = tmp_path / name
    path.write_text(content)
    return f"file://{path}"


def _stage_csv(
    loader: CSVLoader,
    duckdb_conn: duckdb.DuckDBPyConnection,
    session: Session,
    source_uri: str,
    *,
    source_name: str = "test_source",
) -> StagedTable:
    """Create a Source row and load a single CSV through ``_load_single_file``.

    Mirrors the production import path (``import_phase`` creates the Source row,
    then the loader stages the file), and unwraps the ``StagedTable``.
    """
    source_id = "src-csv-test"
    source = Source(
        source_id=source_id,
        name=source_name,
        source_type="csv",
        connection_config={"path": source_uri},
    )
    session.add(source)
    session.flush()

    result = loader._load_single_file(
        source_uri=source_uri,
        source_id=source_id,
        duckdb_conn=duckdb_conn,
        session=session,
        null_config=load_null_value_config(),
    )
    assert result.success, result.error
    return result.unwrap()


class TestCSVLoaderIntegration:
    """CSV loader behavior against a real DuckLake-backed connection."""

    def test_load_simple_csv(
        self,
        loader: CSVLoader,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        tmp_path,
    ) -> None:
        csv_path = _write_csv(tmp_path, "simple.csv", "id,name,value\n1,Alice,100\n2,Bob,200\n")

        staged = _stage_csv(loader, duckdb_conn, session, csv_path)

        assert staged.row_count == 2
        assert staged.column_count == 3

    def test_load_csv_with_nulls(
        self,
        loader: CSVLoader,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        tmp_path,
    ) -> None:
        csv_path = _write_csv(
            tmp_path,
            "nulls.csv",
            "id,name,value\n1,Alice,100\n2,,200\n3,Charlie,\n",
        )

        staged = _stage_csv(loader, duckdb_conn, session, csv_path)

        rows = duckdb_conn.execute(
            f'SELECT COUNT(*) FROM lake.raw."{staged.raw_table_name}" WHERE name IS NULL'
        ).fetchone()
        assert rows[0] == 1

    def test_load_csv_normalizes_columns(
        self,
        loader: CSVLoader,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        tmp_path,
    ) -> None:
        csv_path = _write_csv(
            tmp_path,
            "cols.csv",
            "User ID,Full-Name,VALUE\n1,Alice,100\n",
        )

        staged = _stage_csv(loader, duckdb_conn, session, csv_path)

        cols = duckdb_conn.execute(
            f'SELECT * FROM lake.raw."{staged.raw_table_name}" LIMIT 0'
        ).description
        col_names = [c[0] for c in cols]
        assert "user_id" in col_names
        assert "fullname" in col_names
        assert "value" in col_names

    def test_load_csv_duplicate_columns(
        self,
        loader: CSVLoader,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        tmp_path,
    ) -> None:
        csv_path = _write_csv(
            tmp_path,
            "dups.csv",
            "id,id,name\n1,2,Alice\n",
        )

        staged = _stage_csv(loader, duckdb_conn, session, csv_path)

        cols = duckdb_conn.execute(
            f'SELECT * FROM lake.raw."{staged.raw_table_name}" LIMIT 0'
        ).description
        col_names = [c[0] for c in cols]
        assert "id" in col_names
        assert "id_1" in col_names

    def test_load_csv_encoding_error(
        self,
        loader: CSVLoader,
        duckdb_conn: duckdb.DuckDBPyConnection,
        session: Session,
        tmp_path,
    ) -> None:
        # Write Latin-1 bytes that are not valid UTF-8
        path = tmp_path / "latin1.csv"
        path.write_bytes("naïve,café\n1,2\n".encode("latin-1"))
        source_uri = f"file://{path}"

        source = Source(
            source_id="src-csv-latin1",
            name="bad_encoding",
            source_type="csv",
            connection_config={"path": source_uri},
        )
        session.add(source)
        session.flush()

        result = loader._load_single_file(
            source_uri=source_uri,
            source_id="src-csv-latin1",
            duckdb_conn=duckdb_conn,
            session=session,
            null_config=load_null_value_config(),
        )

        assert not result.success
        assert "utf-8" in (result.error or "").lower()
