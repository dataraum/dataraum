"""Promoted-read surface generation (ADR-0008, DAT-453).

The statement generator is pure (no DB): these tests pin the registry's
coverage tripwire and the artifact's shape. The live properties — promoted-run
filtering, reader-role denial — are exercised against real Postgres by the
pull-metadata flow and were verified in the DAT-453 bring-up.
"""

from __future__ import annotations

import pytest

from dataraum.storage.base import Base, load_all_models
from dataraum.storage.read_views import (
    READ_TOKEN,
    WS_TOKEN,
    dump_read_ddl,
    read_schema_name_for,
    read_view_statements,
)


def test_every_run_stamped_table_gets_a_current_view() -> None:
    """The coverage tripwire: versioned tables cannot skip the read surface."""
    load_all_models()
    names = {name for name, _ in read_view_statements()}

    for table in Base.metadata.tables.values():
        if table.name == "metadata_snapshot_head":
            assert table.name in names  # pointer: pass-through, not current_*
            continue
        if "run_id" in {c.name for c in table.columns}:
            assert f"current_{table.name}" in names, table.name
        else:
            assert table.name in names, table.name


def test_statements_are_deterministic_and_tokenized() -> None:
    first = read_view_statements()
    second = read_view_statements()
    assert first == second
    for _, sql in first:
        assert READ_TOKEN in sql
        assert WS_TOKEN in sql
        assert sql.startswith("CREATE VIEW")
    # The checked-in artifact carries the apply contract in its header and a
    # DROP per view (CREATE OR REPLACE cannot drop/rename view columns).
    ddl = dump_read_ddl()
    assert "GENERATED" in ddl and WS_TOKEN in ddl and READ_TOKEN in ddl
    assert ddl.count("DROP VIEW IF EXISTS") == len(first)


def test_head_join_shape_for_column_grain() -> None:
    """Spot-check the hard join — written once, here, for everyone."""
    sql = dict(read_view_statements())["current_semantic_annotations"]
    assert "'table:' || c.table_id" in sql
    assert "h.stage = 'semantic_per_column'" in sql
    assert "h.run_id = r.run_id" in sql


def test_dual_grain_accepts_either_head_and_discriminates() -> None:
    """entropy objects/readiness: add_source seals per table, begin_session per
    session — and after both, a column has TWO current rows; the ``via_*``
    discriminators let consumers pin one grain (review finding, 2026-06-07)."""
    sql = dict(read_view_statements())["current_entropy_objects"]
    assert "'table:' || r.table_id" in sql
    assert "'session:' || r.session_id" in sql
    assert "AS via_table_head" in sql
    assert "AS via_session_head" in sql


def test_unclassified_versioned_table_fails_loud() -> None:
    """A new run-stamped table without a grain classification breaks generation."""
    from sqlalchemy import Column as SAColumn
    from sqlalchemy import String
    from sqlalchemy import Table as SATable

    # Register a rogue versioned table into the live metadata, then clean up.
    load_all_models()
    rogue = SATable(
        "rogue_versioned_artifacts",
        Base.metadata,
        SAColumn("id", String, primary_key=True),
        SAColumn("run_id", String, nullable=True),
    )
    try:
        with pytest.raises(RuntimeError, match="rogue_versioned_artifacts"):
            read_view_statements()
    finally:
        Base.metadata.remove(rogue)


def test_read_schema_name() -> None:
    assert read_schema_name_for("ws_abc") == "ws_abc_read"
