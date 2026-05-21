"""Tests for JSON/JSONL loader.

Post-DAT-341: loader writes to ``lake.raw.<source>__<table>`` via the
DuckLake-anchored connection. Tests under ``TestLoadSingleFile`` request
``lake_anchor`` + ``lake_clean`` and open ``connect_session()`` instead of
a plain ``:memory:`` DuckDB.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from sqlalchemy.orm import Session

from dataraum.core.models import SourceConfig
from dataraum.sources.json.loader import JsonLoader
from tests.conftest import _TEST_SOURCE_ID


@pytest.fixture
def loader() -> JsonLoader:
    return JsonLoader()


@pytest.fixture
def json_file(tmp_path: Path) -> Path:
    data = [
        {"id": 1, "name": "Alice", "amount": 100.5},
        {"id": 2, "name": "Bob", "amount": 200.0},
        {"id": 3, "name": "Carol", "amount": 300.75},
    ]
    path = tmp_path / "data.json"
    path.write_text(json.dumps(data))
    return path


@pytest.fixture
def jsonl_file(tmp_path: Path) -> Path:
    lines = [
        json.dumps({"id": 1, "city": "Berlin", "pop": 3645000}),
        json.dumps({"id": 2, "city": "Munich", "pop": 1472000}),
    ]
    path = tmp_path / "cities.jsonl"
    path.write_text("\n".join(lines))
    return path


class TestGetSchema:
    def test_json_columns(self, loader: JsonLoader, json_file: Path) -> None:
        config = SourceConfig(name="test", source_type="json", path=str(json_file))
        result = loader.get_schema(config)

        assert result.success
        columns = result.unwrap()
        names = [c.name for c in columns]
        assert "id" in names
        assert "name" in names
        assert "amount" in names
        assert all(c.source_type == "VARCHAR" for c in columns)

    def test_jsonl_columns(self, loader: JsonLoader, jsonl_file: Path) -> None:
        config = SourceConfig(name="test", source_type="json", path=str(jsonl_file))
        result = loader.get_schema(config)

        assert result.success
        columns = result.unwrap()
        names = [c.name for c in columns]
        assert "city" in names
        assert "pop" in names

    def test_missing_path(self, loader: JsonLoader) -> None:
        config = SourceConfig(name="test", source_type="json")
        result = loader.get_schema(config)
        assert not result.success

    def test_nonexistent_file(self, loader: JsonLoader) -> None:
        config = SourceConfig(name="test", source_type="json", path="/nonexistent.json")
        result = loader.get_schema(config)
        assert not result.success


_SOURCE_NAME = "test_baseline"  # matches the session fixture's seeded Source.name


def _fqn(bare: str) -> str:
    """Return ``lake.raw."<bare>"`` — convenience for assertions."""
    return f'lake.raw."{bare}"'


class TestLoadSingleFile:
    def test_loads_json_as_varchar(
        self,
        loader: JsonLoader,
        json_file: Path,
        session: Session,
        lake_anchor,
        lake_clean,
    ) -> None:
        from dataraum.server.storage import connect_session

        conn = connect_session()
        try:
            result = loader._load_single_file(
                file_path=json_file,
                source_id=_TEST_SOURCE_ID,
                source_name=_SOURCE_NAME,
                duckdb_conn=conn,
                session=session,
            )

            assert result.success
            staged = result.unwrap()
            assert staged.row_count == 3
            assert staged.column_count == 3
            assert staged.table_name == "test_baseline__data"
            assert staged.raw_table_name == "test_baseline__data"

            # Verify all columns are VARCHAR by reading back from lake.raw
            row = conn.execute(f"SELECT * FROM {_fqn(staged.raw_table_name)} LIMIT 1").fetchone()
            assert row is not None
            assert all(isinstance(v, str) for v in row)
        finally:
            conn.close()

    def test_loads_jsonl(
        self,
        loader: JsonLoader,
        jsonl_file: Path,
        session: Session,
        lake_anchor,
        lake_clean,
    ) -> None:
        from dataraum.server.storage import connect_session

        conn = connect_session()
        try:
            result = loader._load_single_file(
                file_path=jsonl_file,
                source_id=_TEST_SOURCE_ID,
                source_name=_SOURCE_NAME,
                duckdb_conn=conn,
                session=session,
            )

            assert result.success
            staged = result.unwrap()
            assert staged.row_count == 2
            assert staged.table_name == "test_baseline__cities"
        finally:
            conn.close()

    def test_normalizes_column_names(
        self,
        loader: JsonLoader,
        tmp_path: Path,
        session: Session,
        lake_anchor,
        lake_clean,
    ) -> None:
        from dataraum.server.storage import connect_session

        data = [{"First Name": "Alice", "Last-Name": "Smith", "123bad": "x"}]
        path = tmp_path / "weird_cols.json"
        path.write_text(json.dumps(data))

        conn = connect_session()
        try:
            result = loader._load_single_file(
                file_path=path,
                source_id=_TEST_SOURCE_ID,
                source_name=_SOURCE_NAME,
                duckdb_conn=conn,
                session=session,
            )

            assert result.success
            # Describe the materialized table directly via PRAGMA so we don't
            # depend on information_schema (DuckLake doesn't expose it cleanly).
            cols = conn.execute(
                f"SELECT column_name FROM (DESCRIBE {_fqn('test_baseline__weird_cols')}) "
                "ORDER BY column_name"
            ).fetchall()
            col_names = [c[0] for c in cols]
            assert "first_name" in col_names
            assert "lastname" in col_names
            assert "c_123bad" in col_names
        finally:
            conn.close()

    def test_nested_objects_become_varchar(
        self,
        loader: JsonLoader,
        tmp_path: Path,
        session: Session,
        lake_anchor,
        lake_clean,
    ) -> None:
        """Nested JSON objects and arrays must be serialized to VARCHAR, not fail."""
        from dataraum.server.storage import connect_session

        data = [
            {"id": 1, "address": {"city": "Berlin", "zip": "10115"}, "tags": ["a", "b"]},
            {"id": 2, "address": {"city": "Munich", "zip": "80331"}, "tags": ["c"]},
        ]
        path = tmp_path / "nested.json"
        path.write_text(json.dumps(data))

        conn = connect_session()
        try:
            result = loader._load_single_file(
                file_path=path,
                source_id=_TEST_SOURCE_ID,
                source_name=_SOURCE_NAME,
                duckdb_conn=conn,
                session=session,
            )

            assert result.success
            staged = result.unwrap()
            assert staged.column_count == 3

            # Nested object should be serialized as JSON string
            row = conn.execute(
                f"SELECT address FROM {_fqn(staged.raw_table_name)} LIMIT 1"
            ).fetchone()
            assert row is not None
            assert "Berlin" in row[0]
        finally:
            conn.close()

    def test_empty_json_array(
        self,
        loader: JsonLoader,
        tmp_path: Path,
        session: Session,
        lake_anchor,
        lake_clean,
    ) -> None:
        from dataraum.server.storage import connect_session

        path = tmp_path / "empty.json"
        path.write_text("[]")

        conn = connect_session()
        try:
            result = loader._load_single_file(
                file_path=path,
                source_id=_TEST_SOURCE_ID,
                source_name=_SOURCE_NAME,
                duckdb_conn=conn,
                session=session,
            )
            # DuckDB may fail on empty arrays — either fail or produce 0 rows
            if result.success:
                assert result.unwrap().row_count == 0
        finally:
            conn.close()
