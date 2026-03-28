"""Tests for SourceManager."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.credentials import CredentialChain
from dataraum.core.models import Result
from dataraum.sources.backends import BackendValidationResult, TablePreview
from dataraum.sources.manager import SourceManager
from dataraum.storage.models import Source


@pytest.fixture
def credential_chain(tmp_path: Path) -> CredentialChain:
    return CredentialChain(credentials_dir=tmp_path)


@pytest.fixture
def manager(session: Session, credential_chain: CredentialChain) -> SourceManager:
    return SourceManager(session=session, credential_chain=credential_chain)


class TestAddFileSource:
    def test_register_csv(self, manager: SourceManager, tmp_path: Path) -> None:
        csv = tmp_path / "bookings.csv"
        csv.write_text("id,name,amount\n1,Alice,100\n")

        result = manager.add_file_source("bookings", str(csv))

        assert result.success
        info = result.unwrap()
        assert info.name == "bookings"
        assert info.source_type == "csv"
        assert info.status == "configured"
        assert "id" in info.columns

    def test_register_parquet(self, manager: SourceManager, tmp_path: Path) -> None:
        import duckdb

        parquet = tmp_path / "data.parquet"
        conn = duckdb.connect()
        conn.execute(f"COPY (SELECT 1 AS id) TO '{parquet}' (FORMAT PARQUET)")
        conn.close()

        result = manager.add_file_source("mydata", str(parquet))

        assert result.success
        assert result.unwrap().source_type == "parquet"

    def test_invalid_name(self, manager: SourceManager, tmp_path: Path) -> None:
        csv = tmp_path / "x.csv"
        csv.write_text("a\n1\n")

        result = manager.add_file_source("Invalid-Name!", str(csv))
        assert not result.success
        assert "Invalid source name" in (result.error or "")

    def test_duplicate_name(self, manager: SourceManager, tmp_path: Path) -> None:
        csv = tmp_path / "x.csv"
        csv.write_text("a\n1\n")

        manager.add_file_source("src_dup", str(csv))
        result = manager.add_file_source("src_dup", str(csv))

        assert not result.success
        assert "already exists" in (result.error or "")

    def test_nonexistent_path(self, manager: SourceManager) -> None:
        result = manager.add_file_source("src_ne", "/nonexistent/data.csv")
        assert not result.success
        assert "not found" in (result.error or "").lower()

    def test_persists_to_db(self, manager: SourceManager, session: Session, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("a\n1\n")
        manager.add_file_source("persisted", str(csv))

        source = session.execute(select(Source).where(Source.name == "persisted")).scalar_one()
        assert source.source_type == "csv"
        assert source.status == "configured"


    def test_register_json(self, manager: SourceManager, tmp_path: Path) -> None:
        import json

        data = [{"id": 1, "name": "Alice"}]
        json_file = tmp_path / "records.json"
        json_file.write_text(json.dumps(data))

        result = manager.add_file_source("records", str(json_file))

        assert result.success
        info = result.unwrap()
        assert info.source_type == "json"
        assert info.status == "configured"
        assert "id" in info.columns

    def test_register_jsonl(self, manager: SourceManager, tmp_path: Path) -> None:
        import json

        lines = [json.dumps({"x": 1}), json.dumps({"x": 2})]
        jsonl_file = tmp_path / "data.jsonl"
        jsonl_file.write_text("\n".join(lines))

        result = manager.add_file_source("jsonl_src", str(jsonl_file))

        assert result.success
        info = result.unwrap()
        assert info.source_type == "json"
        assert "x" in info.columns

    def test_reject_unsupported_format(self, manager: SourceManager, tmp_path: Path) -> None:
        xlsx = tmp_path / "data.xlsx"
        xlsx.write_bytes(b"fake excel content")

        result = manager.add_file_source("bad_fmt", str(xlsx))

        assert not result.success
        assert "Unsupported file format" in (result.error or "")
        assert ".xlsx" in (result.error or "")
        assert ".csv" in (result.error or "")

    def test_register_directory(self, manager: SourceManager, tmp_path: Path) -> None:
        (tmp_path / "a.csv").write_text("id,name\n1,Alice\n")
        (tmp_path / "b.csv").write_text("id,name\n2,Bob\n")

        result = manager.add_file_source("dir_src", str(tmp_path))

        assert result.success
        info = result.unwrap()
        assert info.source_type == "csv"
        assert info.discovered_schema is not None
        assert info.discovered_schema["file_count"] == 2
        assert info.discovered_schema["formats"] == {"csv": 2}
        assert "2 csv" in info.discovered_schema["breakdown"]

    def test_reject_empty_directory(self, manager: SourceManager, tmp_path: Path) -> None:
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        result = manager.add_file_source("empty_dir", str(empty_dir))

        assert not result.success
        assert "No supported data files" in (result.error or "")

    def test_directory_ignores_unsupported_files(self, manager: SourceManager, tmp_path: Path) -> None:
        (tmp_path / "data.csv").write_text("id\n1\n")
        (tmp_path / "readme.txt").write_text("ignore me")
        (tmp_path / "image.png").write_bytes(b"\x89PNG")

        result = manager.add_file_source("mixed_dir", str(tmp_path))

        assert result.success
        info = result.unwrap()
        assert info.discovered_schema is not None
        assert info.discovered_schema["file_count"] == 1


class TestAddDatabaseSource:
    def test_needs_credentials(self, session: Session, credential_chain: CredentialChain) -> None:
        manager = SourceManager(session=session, credential_chain=credential_chain)
        result = manager.add_database_source("accounting", "postgres")

        assert result.success
        info = result.unwrap()
        assert info.status == "needs_credentials"
        assert info.credential_instructions is not None
        assert "postgres://" in info.credential_instructions["url_template"]

    def test_unsupported_backend(self, manager: SourceManager) -> None:
        result = manager.add_database_source("src_ub", "clickhouse")
        assert not result.success
        assert "Unsupported backend" in (result.error or "")

    def test_invalid_name(self, manager: SourceManager) -> None:
        result = manager.add_database_source("BAD NAME", "postgres")
        assert not result.success

    def test_validated_with_credentials(
        self, session: Session, credential_chain: CredentialChain, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DATARAUM_MYDB_URL", "postgres://u:p@host/db")

        mock_duckdb = MagicMock()
        tables = [
            TablePreview(name="orders", columns=["id", "amount"]),
            TablePreview(name="customers", columns=["id", "name"]),
        ]
        validation_result = Result.ok(BackendValidationResult(tables=tables))

        manager = SourceManager(
            session=session, credential_chain=credential_chain, duckdb_conn=mock_duckdb
        )

        with patch("dataraum.sources.manager.validate_backend", return_value=validation_result):
            result = manager.add_database_source("mydb", "postgres")

        assert result.success
        info = result.unwrap()
        assert info.status == "validated"
        assert info.credential_source == "env"
        assert info.discovered_schema is not None
        assert len(info.discovered_schema["tables"]) == 2

    def test_table_filter(
        self, session: Session, credential_chain: CredentialChain, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DATARAUM_FILTERED_URL", "postgres://u:p@host/db")
        mock_duckdb = MagicMock()
        tables = [
            TablePreview(name="orders", columns=["id"]),
            TablePreview(name="customers", columns=["id"]),
            TablePreview(name="logs", columns=["ts"]),
        ]
        validation_result = Result.ok(BackendValidationResult(tables=tables))

        manager = SourceManager(
            session=session, credential_chain=credential_chain, duckdb_conn=mock_duckdb
        )

        with patch("dataraum.sources.manager.validate_backend", return_value=validation_result):
            result = manager.add_database_source("filtered", "postgres", tables=["orders"])

        info = result.unwrap()
        assert info.discovered_schema is not None
        assert len(info.discovered_schema["tables"]) == 1
        assert info.discovered_schema["tables_excluded"] == 2

    def test_no_duckdb_conn(
        self, session: Session, credential_chain: CredentialChain, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("DATARAUM_NODB_URL", "postgres://u:p@host/db")
        manager = SourceManager(
            session=session, credential_chain=credential_chain, duckdb_conn=None
        )

        result = manager.add_database_source("nodb", "postgres")
        assert not result.success
        assert "DuckDB connection required" in (result.error or "")

    def test_needs_credentials_persists(
        self, session: Session, credential_chain: CredentialChain
    ) -> None:
        manager = SourceManager(session=session, credential_chain=credential_chain)
        manager.add_database_source("pending_src", "mysql")

        source = session.execute(select(Source).where(Source.name == "pending_src")).scalar_one()
        assert source.status == "needs_credentials"
        assert source.backend == "mysql"


class TestListSources:
    def test_list_empty(self, manager: SourceManager) -> None:
        assert manager.list_sources() == []

    def test_list_registered(self, manager: SourceManager, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("a\n1\n")
        manager.add_file_source("src_la", str(csv))
        manager.add_file_source("src_lb", str(csv))

        sources = manager.list_sources()
        assert len(sources) == 2
        names = [s.name for s in sources]
        assert "src_la" in names
        assert "src_lb" in names

    def test_filter_by_status(
        self, session: Session, credential_chain: CredentialChain, tmp_path: Path
    ) -> None:
        manager = SourceManager(session=session, credential_chain=credential_chain)

        csv = tmp_path / "data.csv"
        csv.write_text("a\n1\n")
        manager.add_file_source("configured_src", str(csv))
        manager.add_database_source("pending_src", "postgres")

        configured = manager.list_sources(status_filter="configured")
        assert len(configured) == 1
        assert configured[0].name == "configured_src"

    def test_excludes_archived(
        self, manager: SourceManager, session: Session, tmp_path: Path
    ) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("a\n1\n")
        manager.add_file_source("active", str(csv))
        manager.add_file_source("to_remove", str(csv))
        manager.remove_source("to_remove")

        sources = manager.list_sources()
        assert len(sources) == 1
        assert sources[0].name == "active"


class TestRemoveSource:
    def test_soft_delete(self, manager: SourceManager, session: Session, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("a\n1\n")
        manager.add_file_source("removeme", str(csv))

        result = manager.remove_source("removeme")
        assert result.success
        assert "archived" in result.unwrap()

        # Source still in DB but archived
        source = session.execute(select(Source).where(Source.name == "removeme")).scalar_one()
        assert source.archived_at is not None

    def test_hard_delete(self, manager: SourceManager, session: Session, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("a\n1\n")
        manager.add_file_source("purgeme", str(csv))

        result = manager.remove_source("purgeme", purge=True)
        assert result.success
        assert "deleted" in result.unwrap()

        source = session.execute(
            select(Source).where(Source.name == "purgeme")
        ).scalar_one_or_none()
        assert source is None

    def test_remove_nonexistent(self, manager: SourceManager) -> None:
        result = manager.remove_source("ghost")
        assert not result.success
        assert "not found" in (result.error or "").lower()

    def test_credential_hint(self, session: Session, credential_chain: CredentialChain) -> None:
        manager = SourceManager(session=session, credential_chain=credential_chain)
        manager.add_database_source("mydb_ch", "postgres")

        result = manager.remove_source("mydb_ch")
        assert result.success
        assert "credentials" in result.unwrap().lower()
