"""Tests for SourceManager."""

from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.core.credentials import CredentialChain
from dataraum.sources.manager import SourceManager
from dataraum.storage.models import Source

VALID_RECIPE = """\
backend: mssql
tables:
  invoices:
    sql: |
      SELECT invoice_id, total_amount FROM dbo.Invoices
  customers:
    sql: SELECT customer_id, name FROM dbo.Customers
"""


@pytest.fixture
def credential_chain() -> CredentialChain:
    return CredentialChain()


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

    def test_opaque_s3_uri_dispatches_by_suffix(
        self, manager: SourceManager, session: Session
    ) -> None:
        """An ``s3://`` URI registers by suffix and is stored verbatim (DAT-389).

        Registration never stats the filesystem: the URI is opaque and handed
        to DuckDB only at import time. The advisory preview can't reach the
        (non-existent in this unit test) object store, but that does not block
        registration.
        """
        uri = "s3://dataraum-lake/uploads/abc123/orders.csv"
        result = manager.add_file_source("s3_orders", uri)

        assert result.success, result.error
        info = result.unwrap()
        assert info.source_type == "csv"
        assert info.path == uri

        source = session.execute(select(Source).where(Source.name == "s3_orders")).scalar_one()
        assert source.connection_config == {"path": uri}

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


class TestAddRecipeSource:
    def test_registers_recipe(self, manager: SourceManager, tmp_path: Path) -> None:
        recipe = tmp_path / "erp.yaml"
        recipe.write_text(VALID_RECIPE)

        result = manager.add_recipe_source("erp", str(recipe))

        assert result.success, result.error
        info = result.unwrap()
        assert info.source_type == "db_recipe"
        assert info.status == "configured"
        assert info.backend == "mssql"
        assert info.recipe_tables == ["invoices", "customers"]
        assert info.path is not None

    def test_persists_recipe_in_connection_config(
        self, manager: SourceManager, session: Session, tmp_path: Path
    ) -> None:
        recipe = tmp_path / "erp.yaml"
        recipe.write_text(VALID_RECIPE)
        manager.add_recipe_source("erp", str(recipe))

        source = session.execute(select(Source).where(Source.name == "erp")).scalar_one()
        assert source.source_type == "db_recipe"
        assert source.backend == "mssql"
        assert source.connection_config is not None
        cfg = source.connection_config
        assert cfg["backend"] == "mssql"
        assert cfg["recipe_path"].endswith("erp.yaml")
        assert len(cfg["recipe_hash"]) == 64  # sha256 hex
        assert {t["name"] for t in cfg["tables"]} == {"invoices", "customers"}
        assert all("sql" in t for t in cfg["tables"])

    def test_no_credentials_persisted(
        self, manager: SourceManager, session: Session, tmp_path: Path
    ) -> None:
        recipe = tmp_path / "erp.yaml"
        recipe.write_text(VALID_RECIPE)
        manager.add_recipe_source("erp", str(recipe))

        source = session.execute(select(Source).where(Source.name == "erp")).scalar_one()
        cfg_str = str(source.connection_config)
        # No URL, password, user, host, etc. anywhere in the persisted config.
        for forbidden in ("password", "user@", "://", "mssql://", "://localhost"):
            assert forbidden not in cfg_str.lower(), f"Found '{forbidden}' in {cfg_str}"

    def test_invalid_recipe_fails_loud(self, manager: SourceManager, tmp_path: Path) -> None:
        recipe = tmp_path / "bad.yaml"
        recipe.write_text("backend: oracle\ntables:\n  t:\n    sql: SELECT 1\n")
        result = manager.add_recipe_source("badrecipe", str(recipe))
        assert not result.success
        assert "oracle" in (result.error or "").lower()

    def test_recipe_with_credentials_rejected(self, manager: SourceManager, tmp_path: Path) -> None:
        recipe = tmp_path / "leaky.yaml"
        recipe.write_text(
            "backend: mssql\n"
            "connection:\n"
            "  host: localhost\n"
            "  password: hunter2\n"
            "tables:\n  t:\n    sql: SELECT 1\n"
        )
        result = manager.add_recipe_source("leaky", str(recipe))
        assert not result.success
        assert "secret-free" in (result.error or "").lower()

    def test_duplicate_name_rejected(self, manager: SourceManager, tmp_path: Path) -> None:
        recipe = tmp_path / "erp.yaml"
        recipe.write_text(VALID_RECIPE)
        manager.add_recipe_source("dup_src", str(recipe))

        result = manager.add_recipe_source("dup_src", str(recipe))
        assert not result.success
        assert "already exists" in (result.error or "")

    def test_invalid_name(self, manager: SourceManager, tmp_path: Path) -> None:
        recipe = tmp_path / "erp.yaml"
        recipe.write_text(VALID_RECIPE)
        result = manager.add_recipe_source("BAD NAME", str(recipe))
        assert not result.success


class TestListSources:
    def test_list_empty(self, manager: SourceManager) -> None:
        # Conftest seeds a baseline Source as the InvestigationSession FK
        # target; filter it out to assert the workspace-as-registered view.
        sources = [s for s in manager.list_sources() if s.name != "test_baseline"]
        assert sources == []

    def test_list_registered(self, manager: SourceManager, tmp_path: Path) -> None:
        csv = tmp_path / "data.csv"
        csv.write_text("a\n1\n")
        manager.add_file_source("src_la", str(csv))
        manager.add_file_source("src_lb", str(csv))

        sources = [s for s in manager.list_sources() if s.name != "test_baseline"]
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
        # Manually mark one as a different status to confirm the filter.
        archived_source = Source(
            name="archived_src", source_type="csv", status="archived_pending", archived_at=None
        )
        session.add(archived_source)
        session.flush()

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

        sources = [s for s in manager.list_sources() if s.name != "test_baseline"]
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

    def test_recipe_remove_includes_credentials_hint(
        self, manager: SourceManager, tmp_path: Path
    ) -> None:
        recipe = tmp_path / "erp.yaml"
        recipe.write_text(VALID_RECIPE)
        manager.add_recipe_source("erp_ch", str(recipe))

        result = manager.remove_source("erp_ch")
        assert result.success
        assert "credentials" in result.unwrap().lower()
        assert "DATARAUM_ERP_CH_URL" in result.unwrap()
