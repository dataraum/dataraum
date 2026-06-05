"""Tests for the offline DDL dump (storage.dump_ddl)."""

import re

from dataraum.storage.base import Base, load_all_models
from dataraum.storage.dump_ddl import dump_ddl


def test_dump_is_deterministic_in_process() -> None:
    """Two dumps in one process are byte-identical.

    Cross-process byte-stability (what the CI drift gate actually relies on)
    is pinned by the `schema-drift` job regenerating against the checked-in
    file; it holds because sorted_tables sorts alphabetically before the
    topological pass and indexes are explicitly sorted by name.
    """
    assert dump_ddl() == dump_ddl()


def test_dump_covers_all_model_tables() -> None:
    """Every table in Base.metadata appears as a CREATE TABLE statement."""
    ddl = dump_ddl()
    load_all_models()
    assert Base.metadata.tables, "model import produced no tables"
    for name in Base.metadata.tables:
        assert f"CREATE TABLE {name} (" in ddl


def test_dump_is_schema_less() -> None:
    """No CREATE SCHEMA / qualified names — search_path decides the target ws_<id>."""
    ddl = dump_ddl()
    assert "CREATE SCHEMA" not in ddl
    assert re.search(r"CREATE TABLE \w+\.", ddl) is None
    # Index qualification appears after ON (CREATE [UNIQUE] INDEX i ON x.t).
    assert re.search(r" ON \w+\.", ddl) is None
