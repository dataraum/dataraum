"""Tests for JSON/JSONL loader."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pytest
from sqlalchemy.orm import Session

from dataraum.core.models import SourceConfig
from dataraum.sources.json.loader import JsonLoader


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


class TestLoadSingleFile:
    def test_loads_json_as_varchar(
        self, loader: JsonLoader, json_file: Path, session: Session
    ) -> None:
        conn = duckdb.connect(":memory:")
        result = loader._load_single_file(
            file_path=json_file,
            source_id="src_1",
            duckdb_conn=conn,
            session=session,
        )

        assert result.success
        staged = result.unwrap()
        assert staged.row_count == 3
        assert staged.column_count == 3
        assert staged.table_name == "data"
        assert staged.raw_table_name == "raw_data"

        # Verify all columns are VARCHAR in DuckDB
        types = conn.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name = '{staged.raw_table_name}'"
        ).fetchall()
        for _col_name, data_type in types:
            assert data_type == "VARCHAR"

        conn.close()

    def test_loads_jsonl(self, loader: JsonLoader, jsonl_file: Path, session: Session) -> None:
        conn = duckdb.connect(":memory:")
        result = loader._load_single_file(
            file_path=jsonl_file,
            source_id="src_1",
            duckdb_conn=conn,
            session=session,
        )

        assert result.success
        staged = result.unwrap()
        assert staged.row_count == 2
        assert staged.table_name == "cities"
        conn.close()

    def test_normalizes_column_names(
        self, loader: JsonLoader, tmp_path: Path, session: Session
    ) -> None:
        data = [{"First Name": "Alice", "Last-Name": "Smith", "123bad": "x"}]
        path = tmp_path / "weird_cols.json"
        path.write_text(json.dumps(data))

        conn = duckdb.connect(":memory:")
        result = loader._load_single_file(
            file_path=path, source_id="src_1", duckdb_conn=conn, session=session
        )

        assert result.success
        cols = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'raw_weird_cols' ORDER BY column_name"
        ).fetchall()
        col_names = [c[0] for c in cols]
        assert "first_name" in col_names
        assert "lastname" in col_names
        assert "c_123bad" in col_names
        conn.close()

    def test_nested_objects_become_varchar(
        self, loader: JsonLoader, tmp_path: Path, session: Session
    ) -> None:
        """Nested JSON objects and arrays must be serialized to VARCHAR, not fail."""
        data = [
            {"id": 1, "address": {"city": "Berlin", "zip": "10115"}, "tags": ["a", "b"]},
            {"id": 2, "address": {"city": "Munich", "zip": "80331"}, "tags": ["c"]},
        ]
        path = tmp_path / "nested.json"
        path.write_text(json.dumps(data))

        conn = duckdb.connect(":memory:")
        result = loader._load_single_file(
            file_path=path, source_id="src_1", duckdb_conn=conn, session=session
        )

        assert result.success
        staged = result.unwrap()
        assert staged.column_count == 3

        # All columns should be VARCHAR, including nested ones
        types = conn.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name = '{staged.raw_table_name}'"
        ).fetchall()
        for _col_name, data_type in types:
            assert data_type == "VARCHAR"

        # Nested object should be serialized as JSON string
        rows = conn.execute(f'SELECT address FROM "{staged.raw_table_name}" LIMIT 1').fetchone()
        assert rows is not None
        assert "Berlin" in rows[0]  # JSON-serialized string
        conn.close()

    def test_empty_json_array(self, loader: JsonLoader, tmp_path: Path, session: Session) -> None:
        path = tmp_path / "empty.json"
        path.write_text("[]")

        conn = duckdb.connect(":memory:")
        result = loader._load_single_file(
            file_path=path, source_id="src_1", duckdb_conn=conn, session=session
        )
        # DuckDB may fail on empty arrays — either fail or produce 0 rows
        if result.success:
            assert result.unwrap().row_count == 0
        conn.close()
