"""Promoted-read surface generation (ADR-0008, DAT-453).

The statement generator is pure (no DB): these tests pin the registry's
coverage tripwire and the artifact's shape. The live properties — promoted-run
filtering by the pull-metadata flow; role isolation by
``tests/integration/storage/test_workspace_roles.py`` (DAT-816) — run against
real Postgres.
"""

from __future__ import annotations

import pytest

from dataraum.storage.base import Base, load_all_models
from dataraum.storage.read_views import (
    _ALWAYS_PASSTHROUGH,
    _RUN_GRAIN_EXEMPT,
    READ_TOKEN,
    WS_TOKEN,
    dump_read_ddl,
    enforce_run_grain,
    read_schema_name_for,
    read_view_statements,
)


def test_every_run_stamped_table_gets_a_current_view() -> None:
    """The coverage tripwire: versioned tables cannot skip the read surface."""
    load_all_models()
    names = {name for name, _ in read_view_statements()}

    for table in Base.metadata.tables.values():
        if table.name in _ALWAYS_PASSTHROUGH:
            assert table.name in names  # run_id is the key, not a version axis: pass-through
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
    assert "h.stage = 'generation'" in sql
    assert "h.run_id = r.run_id" in sql


def test_dual_grain_accepts_either_head_and_discriminates() -> None:
    """entropy objects/readiness: add_source seals per table, begin_session per
    workspace catalog — and after both, a column has TWO current rows; the
    ``via_*`` discriminators let consumers pin one grain (DAT-506)."""
    sql = dict(read_view_statements())["current_entropy_objects"]
    assert "'table:' || r.table_id" in sql
    assert "h.target = 'catalog'" in sql
    assert "AS via_table_head" in sql
    assert "AS via_catalog_head" in sql


def test_claim_witnesses_is_dual_grain_witness_substrate() -> None:
    """ClaimWitnessRecord (ADR-0009, DAT-457) is written by both detect paths
    like entropy_objects, so its view joins either head and carries both
    discriminators — the witness provenance behind every pooled (C, U)."""
    sql = dict(read_view_statements())["current_claim_witnesses"]
    assert "'table:' || r.table_id" in sql
    assert "h.target = 'catalog'" in sql
    assert "AS via_table_head" in sql
    assert "AS via_catalog_head" in sql


def test_unclassified_versioned_table_fails_loud() -> None:
    """A new run-stamped table without a grain classification breaks generation."""
    from sqlalchemy import Column as SAColumn
    from sqlalchemy import String, UniqueConstraint
    from sqlalchemy import Table as SATable

    # Register a rogue versioned table into the live metadata, then clean up.
    # It carries a (key, run_id) UNIQUE so it passes the DAT-502 writer-grain
    # gate and exercises the READ-surface classification tripwire specifically.
    load_all_models()
    rogue = SATable(
        "rogue_versioned_artifacts",
        Base.metadata,
        SAColumn("id", String, primary_key=True),
        SAColumn("key", String),
        SAColumn("run_id", String, nullable=True),
        UniqueConstraint("key", "run_id", name="uq_rogue_key_run"),
    )
    try:
        with pytest.raises(RuntimeError, match="rogue_versioned_artifacts"):
            read_view_statements()
    finally:
        Base.metadata.remove(rogue)


class TestRunGrainGate:
    """The failure-contract gate (DAT-502): UNIQUE-or-exempt for run-stamped tables."""

    def test_live_metadata_passes(self) -> None:
        """Every shipped run-stamped table has a (key, run_id) UNIQUE or a
        sanctioned form-(b)/deferred listing — the 23-table sweep."""
        load_all_models()
        enforce_run_grain(Base.metadata.tables.values())  # must not raise

        versioned = {
            t.name
            for t in Base.metadata.tables.values()
            if t.name != "metadata_snapshot_head" and "run_id" in {c.name for c in t.columns}
        }
        # The exempt list is exactly the sanctioned non-grain writers; everything
        # else passes via its UNIQUE. Pinned so a new exemption is a conscious,
        # reviewed decision — not a drive-by.
        assert set(_RUN_GRAIN_EXEMPT) == {
            "entropy_readiness",
            "entropy_objects",
            "enriched_views",
            "derived_columns",
        }
        assert set(_RUN_GRAIN_EXEMPT) <= versioned

    def test_unlisted_run_stamped_table_without_unique_raises(self) -> None:
        """The negative gate: a synthetic run-stamped model with no grain fails loud."""
        from sqlalchemy import Column as SAColumn
        from sqlalchemy import MetaData, String
        from sqlalchemy import Table as SATable

        meta = MetaData()
        rogue = SATable(
            "rogue_ungrained",
            meta,
            SAColumn("id", String, primary_key=True),
            SAColumn("run_id", String, nullable=True),
        )
        with pytest.raises(RuntimeError, match="rogue_ungrained.*no \\(key, run_id\\) UNIQUE"):
            enforce_run_grain([rogue])

    def test_stale_exemption_with_unique_raises(self) -> None:
        """A listed table that gained its UNIQUE must be pruned from the list."""
        from sqlalchemy import Column as SAColumn
        from sqlalchemy import MetaData, String, UniqueConstraint
        from sqlalchemy import Table as SATable

        meta = MetaData()
        # Reuse a real exempt name so the listing check fires.
        graduated = SATable(
            "derived_columns",
            meta,
            SAColumn("id", String, primary_key=True),
            SAColumn("key", String),
            SAColumn("run_id", String, nullable=True),
            UniqueConstraint("key", "run_id", name="uq_graduated_key_run"),
        )
        with pytest.raises(RuntimeError, match="prune the stale listing"):
            enforce_run_grain([graduated])


def test_entropy_readiness_two_conflicting_bands_latest_promoted_wins() -> None:
    """The L3 precedence AC: when the SAME target has an ``entropy_readiness`` row
    promoted under the ``catalog`` head AND another under ``operating_model``,
    ``current_entropy_readiness`` returns EXACTLY ONE row — the latest-promoted.

    Without the catalog-grain precedence clause both rows surface as 'current'
    and an unpinned reader picks one nondeterministically (review wave-1 blocker).
    Executed live against in-memory SQLite (the generated DDL is pure SQL: ``||``,
    correlated ``EXISTS``, ``MAX`` — all SQLite-supported) by substituting the
    ``__WS__``/``__READ__`` schema tokens to the default schema.
    """
    import sqlite3
    from datetime import UTC, datetime, timedelta

    view_ddl = dict(read_view_statements())["current_entropy_readiness"]
    # Tokens → default schema; the view then references bare table names.
    view_ddl = view_ddl.replace(f"{READ_TOKEN}.", "").replace(f"{WS_TOKEN}.", "")

    conn = sqlite3.connect(":memory:")
    conn.executescript(
        "CREATE TABLE entropy_readiness ("
        "  readiness_id TEXT PRIMARY KEY, target TEXT, table_id TEXT, run_id TEXT, band TEXT"
        ");"
        "CREATE TABLE metadata_snapshot_head ("
        "  head_id TEXT PRIMARY KEY, target TEXT, stage TEXT, run_id TEXT, promoted_at TEXT"
        ");"
    )

    # Same target, two runs: an older begin_session catalog run + a newer
    # operating_model run. Both promote the SAME ``catalog`` target, distinct
    # stages — the conflict the precedence clause resolves.
    earlier = datetime(2026, 6, 15, 10, 0, tzinfo=UTC)
    later = earlier + timedelta(hours=1)
    conn.executemany(
        "INSERT INTO entropy_readiness VALUES (?, ?, ?, ?, ?)",
        [
            ("rd_catalog", "table:t1", "t1", "run_catalog", "ready"),
            ("rd_om", "table:t1", "t1", "run_om", "investigate"),
        ],
    )
    conn.executemany(
        "INSERT INTO metadata_snapshot_head VALUES (?, ?, ?, ?, ?)",
        [
            ("h_catalog", "catalog", "catalog", "run_catalog", earlier.isoformat()),
            ("h_om", "catalog", "operating_model", "run_om", later.isoformat()),
        ],
    )
    conn.execute(view_ddl)

    rows = conn.execute(
        "SELECT run_id, band FROM current_entropy_readiness WHERE target = 'table:t1'"
    ).fetchall()
    conn.close()

    # Exactly one current band for the target — the latest-promoted (operating_model).
    assert len(rows) == 1, rows
    assert rows[0] == ("run_om", "investigate")


def test_current_entity_views_shape() -> None:
    """DAT-655: the analyzed-representative views for the un-versioned anchors
    exist ALONGSIDE the plain pass-throughs (staging surfaces keep the raw
    layer axis; head-resolved consumers read the pre-scoped pick)."""
    statements = dict(read_view_statements())
    for name in ("tables", "columns", "current_tables", "current_columns"):
        assert name in statements, name
    assert "t.layer = 'typed'" in statements["current_tables"]
    assert "h.stage = 'generation'" in statements["current_tables"]
    assert "'table:' || t.table_id" in statements["current_columns"]


def test_current_groundings_shape() -> None:
    """DAT-727: the grounding surface over sql_snippets. Membership is the
    cross-lane contract — graph-authored extracts ONLY (the cockpit's query:%
    rows share the table and must never surface) — and so are the exposed
    ``concept`` / ``relation`` columns the eval oracle reads. Reads the BASE
    table (a read view may not depend on another read view — drop order)."""
    statements = dict(read_view_statements())
    sql = statements["current_groundings"]
    assert f"FROM {WS_TOKEN}.sql_snippets" in sql
    assert "snippet_type = 'extract'" in sql
    assert "source LIKE 'graph:%'" in sql
    assert "s.standard_field AS concept" in sql
    assert "s.parts->'from'->>0 AS relation" in sql
    assert "(s.failure_count > 0) AS failed" in sql


def test_current_tables_returns_promoted_typed_representative_only() -> None:
    """DAT-655 semantics, executed live: one logical table across three layers
    plus an unpromoted typed table → ``current_tables`` returns exactly the
    promoted typed row, ``current_columns`` exactly its columns."""
    import sqlite3

    statements = dict(read_view_statements())
    conn = sqlite3.connect(":memory:")
    conn.executescript(
        "CREATE TABLE tables (table_id TEXT PRIMARY KEY, table_name TEXT, layer TEXT);"
        "CREATE TABLE columns (column_id TEXT PRIMARY KEY, table_id TEXT, column_name TEXT);"
        "CREATE TABLE metadata_snapshot_head ("
        "  head_id TEXT PRIMARY KEY, target TEXT, stage TEXT, run_id TEXT"
        ");"
    )
    conn.executemany(
        "INSERT INTO tables VALUES (?, ?, ?)",
        [
            ("t_raw", "orders", "raw"),
            ("t_typed", "orders", "typed"),
            ("t_quar", "orders", "quarantine"),
            ("t_unpromoted", "drafts", "typed"),  # registered, never promoted
        ],
    )
    conn.executemany(
        "INSERT INTO columns VALUES (?, ?, ?)",
        [
            ("c_raw", "t_raw", "amount"),
            ("c_typed", "t_typed", "amount"),
            ("c_unpromoted", "t_unpromoted", "note"),
        ],
    )
    conn.execute(
        "INSERT INTO metadata_snapshot_head VALUES ('h1', 'table:t_typed', 'generation', 'run1')"
    )
    for name in ("current_tables", "current_columns"):
        conn.execute(statements[name].replace(f"{READ_TOKEN}.", "").replace(f"{WS_TOKEN}.", ""))

    tables = conn.execute("SELECT table_id, layer FROM current_tables").fetchall()
    columns = conn.execute("SELECT column_id FROM current_columns").fetchall()
    conn.close()

    assert tables == [("t_typed", "typed")]
    assert columns == [("c_typed",)]


def test_read_schema_name() -> None:
    assert read_schema_name_for("ws_abc") == "ws_abc_read"
