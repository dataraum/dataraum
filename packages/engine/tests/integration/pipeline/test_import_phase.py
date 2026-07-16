"""Tests for import phase.

The import phase runs once per source of an add_source run (DAT-422), against
a Source row already in the workspace DB. These tests pre-create the Source row
and populate ``ctx.config`` with the keys the worker's ``_build_phase_config``
would otherwise supply.

Per DAT-389 the import ingress (``_run``) gates the source URI through
``validate_source_uri`` — only ``s3://<lake-bucket>/<key>`` reaches a loader
(that gate is covered by the unit tests in ``tests/unit/pipeline``). These
integration tests exercise the *real* CSV read + table/column creation against
a live DuckDB connection, which requires a readable local file, so they drive
the post-validation loader entry point (``_load_file_source``) directly rather
than going through the ``s3://`` gate.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.storage import Column, Source, Table

if TYPE_CHECKING:
    import duckdb


def _seed_source(
    session: Session,
    source_id: str,
    name: str,
    path: Path,
    source_type: str = "csv",
) -> None:
    """Insert a Source row mimicking what the cockpit / select stage writes.

    Post-DAT-378 a file source carries its objects as an explicit ``file_uris``
    list under ``connection_config`` (the cockpit ``select`` stage enumerated the
    prefix into it).
    """
    session.add(
        Source(
            source_id=source_id,
            name=name,
            source_type=source_type,
            connection_config={"file_uris": [str(path)]},
        )
    )
    session.flush()


def _seed_db_source(
    session: Session,
    source_id: str,
    name: str,
    connection_config: dict[str, Any],
    *,
    with_raw_table: bool,
) -> None:
    """Insert a db_recipe Source row (mimicking the cockpit ``select``), optionally imported.

    ``with_raw_table=True`` adds one raw Table row — the state after a prior
    import — so the should_skip / staleness paths can be driven directly.
    """
    session.add(
        Source(
            source_id=source_id,
            name=name,
            source_type="db_recipe",
            connection_config=connection_config,
            backend="mssql",
        )
    )
    if with_raw_table:
        session.add(
            Table(
                table_id=str(uuid4()),
                source_id=source_id,
                table_name=f"{name}__t1",
                layer="raw",
                duckdb_path=f"{name}__t1",
                row_count=1,
            )
        )
    session.flush()


def _file_ctx(
    session: Session,
    duckdb_conn: duckdb.DuckDBPyConnection,
    source_id: str,
    name: str,
    path: Path,
    source_type: str = "csv",
    extra: dict[str, Any] | None = None,
) -> PhaseContext:
    """Build a PhaseContext for a file-source pipeline run (Source row pre-seeded).

    The ctx config carries the local readable URI directly under ``file_uris``:
    these tests drive ``_load_file_source`` (post-validation loader entry), which
    exercises the real DuckDB read. The ``s3://`` ingress gate on ``_run`` is
    covered by the unit tests; here we need a file DuckDB can actually read.
    """
    _seed_source(session, source_id, name, path, source_type)
    config: dict[str, Any] = {
        "source_id": source_id,
        "source_name": name,
        "source_type": source_type,
        "source_connection_config": {"file_uris": [str(path)]},
    }
    if extra:
        config.update(extra)
    return PhaseContext(
        session=session,
        duckdb_conn=duckdb_conn,
        config=config,
    )


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    """Create a simple CSV file for testing."""
    csv_path = tmp_path / "test_data.csv"
    csv_path.write_text(
        """id,name,value
1,Alice,100.5
2,Bob,200.3
3,Charlie,300.1
"""
    )
    return csv_path


class TestImportPhase:
    """Tests for ImportPhase."""

    def test_import_single_csv(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, csv_file: Path
    ):
        """Test importing a single CSV file (post-validation loader entry)."""
        phase = ImportPhase()
        source_id = str(uuid4())
        ctx = _file_ctx(session, duckdb_conn, source_id, "test_data", csv_file)
        source = session.get(Source, source_id)
        assert source is not None

        result = phase._load_file_source(ctx, source, "test_data", [str(csv_file)])

        assert result.status == PhaseStatus.COMPLETED
        assert "raw_tables" in result.outputs
        assert len(result.outputs["raw_tables"]) == 1
        assert result.records_processed == 3  # 3 rows
        assert result.records_created == 1  # 1 table

        # Source row was pre-seeded by the helper
        source = session.get(Source, source_id)
        assert source is not None
        assert source.source_type == "csv"

        # Verify Table was created
        stmt = select(Table).where(Table.source_id == source_id)
        result_tables = session.execute(stmt)
        tables = result_tables.scalars().all()
        assert len(tables) == 1
        assert tables[0].layer == "raw"
        assert tables[0].row_count == 3

        # Verify Columns were created
        stmt = select(Column).where(Column.table_id == tables[0].table_id)
        result_cols = session.execute(stmt)
        columns = result_cols.scalars().all()
        assert len(columns) == 3
        column_names = {c.column_name for c in columns}
        assert column_names == {"id", "name", "value"}

    def test_import_multiple_uris_one_table_each(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ):
        """The per-URI loop loads N distinct files into N raw tables (DAT-378).

        The cockpit ``select`` stage enumerates a prefix into an explicit URI
        list; ``_load_file_source`` loops it and yields one raw table per object.
        Each file gets a distinct ``<source_name>__<file_stem>`` table, so the
        names don't collide.
        """
        customers = tmp_path / "customers.csv"
        customers.write_text("id,name\n1,Alice\n2,Bob\n")
        orders = tmp_path / "orders.csv"
        orders.write_text("order_id,amount\n10,5.5\n11,6.5\n12,7.5\n")

        phase = ImportPhase()
        source_id = str(uuid4())
        source = Source(
            source_id=source_id,
            name="multi",
            source_type="csv",
            connection_config={"file_uris": [str(customers), str(orders)]},
        )
        session.add(source)
        session.flush()
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            config={"source_id": source_id, "source_name": "multi", "source_type": "csv"},
        )

        result = phase._load_file_source(ctx, source, "multi", [str(customers), str(orders)])

        assert result.status == PhaseStatus.COMPLETED, result.error
        assert len(result.outputs["raw_tables"]) == 2
        assert result.records_processed == 5  # 2 + 3 rows
        assert result.records_created == 2  # 2 tables

        stmt = select(Table).where(Table.source_id == source_id, Table.layer == "raw")
        names = {t.table_name for t in session.execute(stmt).scalars().all()}
        # DAT-639: narrow, workspace-unique names — the file stems, no source prefix.
        assert names == {"customers", "orders"}

    def test_import_missing_config(self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection):
        """Empty config: import phase reports the missing identity fields."""
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            config={"source_id": str(uuid4())},
        )

        result = phase.run(ctx)

        assert result.status == PhaseStatus.FAILED
        err = (result.error or "").lower()
        assert "source_name" in err
        assert "source_type" in err

    def test_import_unreadable_source_surfaces_read_error(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """A source DuckDB can't read surfaces the read error via Result.fail.

        DAT-389: the import phase never stats the filesystem (the URI is handed
        verbatim to DuckDB). A well-formed-but-unreadable source fails the loader
        with the DuckDB error rather than a pre-flight pathlib check. (The ingress
        ``s3://`` gate is covered by the unit tests; here the loader runs against
        a missing local file to assert the no-pre-check read-error path.)
        """
        phase = ImportPhase()
        source_id = str(uuid4())
        ghost_path = Path("/nonexistent/path.csv")
        _seed_source(session, source_id, "ghost", ghost_path)
        source = session.get(Source, source_id)
        assert source is not None
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            config={"source_id": source_id, "source_name": "ghost", "source_type": "csv"},
        )

        result = phase._load_file_source(ctx, source, "ghost", [str(ghost_path)])

        assert result.status == PhaseStatus.FAILED
        # The failure originates from DuckDB's read of the missing path,
        # surfaced through the loader's Result.fail (no pathlib pre-check).
        assert result.error

    def test_skip_if_tables_exist(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, csv_file: Path
    ):
        """An upload source with raw tables skips on presence alone.

        Upload sources are content-keyed (``src_<digest>``): changed bytes mint
        a NEW source, so existing raw tables are by construction the current
        content — no hash check needed (DAT-430 adds one only for the
        name-keyed db sources).
        """
        source_id = str(uuid4())

        # First, create a source with tables
        source = Source(
            source_id=source_id,
            name="existing_source",
            source_type="csv",
        )
        session.add(source)

        table = Table(
            table_id=str(uuid4()),
            source_id=source_id,
            table_name="existing_table",
            layer="raw",
            duckdb_path="raw_existing_table",
            row_count=10,
        )
        session.add(table)
        session.commit()

        # Now try to import
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session,
            duckdb_conn=duckdb_conn,
            config={"source_id": source_id},
        )

        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "already has" in skip_reason

    def test_db_recipe_should_skip_when_recipe_unchanged(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """A db source whose recipe_hash matches the import witness skips (DAT-430).

        The idempotent paths: a teach re-run (no re-select happened) and a
        re-select of the SAME pick (select recomputes the same hash and carries
        the witness forward) both see matching hashes → skip, like before.
        """
        source_id = str(uuid4())
        _seed_db_source(
            session,
            source_id,
            "warehouse",
            {
                "tables": [{"name": "t1", "sql": "SELECT 1"}],
                "recipe_hash": "hash-A",
                "imported_recipe_hash": "hash-A",
            },
            with_raw_table=True,
        )

        skip_reason = ImportPhase().should_skip(
            PhaseContext(session=session, duckdb_conn=duckdb_conn, config={"source_id": source_id})
        )
        assert skip_reason is not None
        assert "recipe unchanged" in skip_reason

    @pytest.mark.parametrize(
        ("recipe_hash", "imported_hash"),
        [
            ("hash-B", "hash-A"),  # re-pointed recipe (changed table pick)
            ("hash-A", None),  # raw tables predate recipe hashing / witness lost
            (None, None),  # hand-seeded row, never hashed
        ],
    )
    def test_db_recipe_changed_never_silently_skips(
        self,
        session: Session,
        duckdb_conn: duckdb.DuckDBPyConnection,
        recipe_hash: str | None,
        imported_hash: str | None,
    ):
        """A db source whose hashes don't BOTH match never skips (DAT-430).

        The DAT-430 staleness kill: re-selecting the same source name with a
        changed pick used to presence-skip forever, silently 'succeeding' over
        the old raw tables. ``should_skip`` must keep declining on any
        non-matching hash pair so the db load path runs the re-import-with-replace
        (DAT-596) instead of serving stale tables.
        """
        cc: dict[str, Any] = {"tables": [{"name": "t1", "sql": "SELECT 1"}]}
        if recipe_hash:
            cc["recipe_hash"] = recipe_hash
        if imported_hash:
            cc["imported_recipe_hash"] = imported_hash
        source_id = str(uuid4())
        _seed_db_source(session, source_id, "warehouse", cc, with_raw_table=True)
        source = session.get(Source, source_id)
        assert source is not None
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session, duckdb_conn=duckdb_conn, config={"source_id": source_id}
        )

        assert phase.should_skip(ctx) is None  # never a silent skip

    def test_db_recipe_repointed_recipe_replaces_in_place(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ) -> None:
        """A re-pointed recipe (changed pick, same name) replaces in place (DAT-596).

        Import recipe v1 (columns a, b) so a raw + typed table and their metadata
        children + a per-table snapshot head all exist; then re-import a re-pointed
        v2 (columns b, c). The v1 tables (raw + typed) and their Table/Column rows,
        the column-keyed metadata, and the per-table snapshot head must all be
        GONE; only the v2 shape survives; ``imported_recipe_hash`` is re-stamped to
        v2. Extraction + credentials are faked — the teardown+rematerialize
        lifecycle, not the backend, is under test.
        """
        from unittest.mock import MagicMock, patch

        from dataraum.analysis.statistics.db_models import StatisticalProfile
        from dataraum.core.models import Result
        from dataraum.sources.backends import BackendExtractionResult, ExtractedTable
        from dataraum.storage.snapshot_head import GENERATION_STAGE, MetadataSnapshotHead

        source_id = str(uuid4())
        _seed_db_source(
            session,
            source_id,
            "warehouse",
            {"tables": [{"name": "t1", "sql": "SELECT a, b FROM x"}], "recipe_hash": "hash-A"},
            with_raw_table=False,
        )
        source = session.get(Source, source_id)
        assert source is not None
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session, duckdb_conn=duckdb_conn, config={"source_id": source_id}
        )

        chain = MagicMock()
        chain.resolve.return_value = MagicMock(url="mssql://ignored")

        # --- v1 import: raw table warehouse__t1 with columns a, b ---
        v1 = BackendExtractionResult(
            tables=[
                ExtractedTable(
                    name="t1",
                    duckdb_table="warehouse__t1",
                    row_count=2,
                    columns=[("a", "VARCHAR"), ("b", "VARCHAR")],
                )
            ]
        )
        with (
            patch("dataraum.core.credentials.CredentialChain", return_value=chain),
            patch("dataraum.sources.backends.extract_backend", return_value=Result.ok(v1)),
        ):
            cc_v1 = {
                "tables": [{"name": "t1", "sql": "SELECT a, b FROM x"}],
                "recipe_hash": "hash-A",
            }
            r1 = phase._load_database_source(ctx, source, "warehouse", cc_v1, "mssql")
        assert r1.status == PhaseStatus.COMPLETED, r1.error
        v1_raw_id = r1.outputs["raw_tables"][0]

        # Add the surrounding state a real run would: a typed Table (sharing the
        # source_id + bare name), a statistical_profiles row on a v1 column, and
        # the per-table snapshot head add_source promotes.
        v1_cols = (
            session.execute(select(Column).where(Column.table_id == v1_raw_id)).scalars().all()
        )
        assert {c.column_name for c in v1_cols} == {"a", "b"}
        typed_id = str(uuid4())
        session.add(
            Table(
                table_id=typed_id,
                source_id=source_id,
                table_name="t1",
                layer="typed",
                duckdb_path="warehouse__t1",
                row_count=2,
            )
        )
        typed_col_id = str(uuid4())
        session.add(
            Column(
                table_id=typed_id,
                column_id=typed_col_id,
                column_name="a",
                column_position=0,
                raw_type="VARCHAR",
                resolved_type="VARCHAR",
            )
        )
        session.flush()
        session.add(
            StatisticalProfile(
                column_id=typed_col_id,
                run_id="run-v1",
                layer="typed",
                total_count=2,
                null_count=0,
                profile_data={},
            )
        )
        session.add(
            MetadataSnapshotHead(
                target=f"table:{v1_raw_id}", stage=GENERATION_STAGE, run_id="run-v1"
            )
        )
        session.add(
            MetadataSnapshotHead(
                target=f"table:{typed_id}", stage=GENERATION_STAGE, run_id="run-v1"
            )
        )
        session.flush()
        session.expire_all()

        # --- v2 re-import: re-pointed recipe, columns b, c ---
        source = session.get(Source, source_id)
        assert source is not None
        # The cockpit's re-select re-pointed the row's recipe; witness still hash-A.
        source.connection_config = {
            "tables": [{"name": "t1", "sql": "SELECT b, c FROM x"}],
            "recipe_hash": "hash-B",
            "imported_recipe_hash": "hash-A",
        }
        session.flush()
        assert phase.should_skip(ctx) is None  # mismatch → replace, never skip

        v2 = BackendExtractionResult(
            tables=[
                ExtractedTable(
                    name="t1",
                    duckdb_table="warehouse__t1",
                    row_count=3,
                    columns=[("b", "VARCHAR"), ("c", "VARCHAR")],
                )
            ]
        )
        cc_v2 = {
            "tables": [{"name": "t1", "sql": "SELECT b, c FROM x"}],
            "recipe_hash": "hash-B",
        }
        with (
            patch("dataraum.core.credentials.CredentialChain", return_value=chain),
            patch("dataraum.sources.backends.extract_backend", return_value=Result.ok(v2)),
        ):
            r2 = phase._load_database_source(ctx, source, "warehouse", cc_v2, "mssql")
        assert r2.status == PhaseStatus.COMPLETED, r2.error
        v2_raw_id = r2.outputs["raw_tables"][0]

        session.flush()
        session.expire_all()

        # The old raw + typed Table rows are gone; only the v2 raw table remains.
        remaining = (
            session.execute(select(Table).where(Table.source_id == source_id)).scalars().all()
        )
        assert [t.table_id for t in remaining] == [v2_raw_id]
        assert v1_raw_id != v2_raw_id

        # The v1 columns are gone; the v2 raw table has the new shape (b, c).
        assert session.get(Column, typed_col_id) is None
        v2_cols = (
            session.execute(select(Column).where(Column.table_id == v2_raw_id)).scalars().all()
        )
        assert {c.column_name for c in v2_cols} == {"b", "c"}

        # No orphaned metadata children, no dangling snapshot heads.
        assert (
            session.execute(
                select(StatisticalProfile).where(StatisticalProfile.column_id == typed_col_id)
            ).scalar_one_or_none()
            is None
        )
        heads = session.execute(select(MetadataSnapshotHead)).scalars().all()
        stale = {f"table:{v1_raw_id}", f"table:{typed_id}"}
        assert not ({h.target for h in heads} & stale)

        # The witness is re-stamped to the v2 recipe.
        source = session.get(Source, source_id)
        assert source is not None
        assert source.connection_config is not None
        assert source.connection_config["imported_recipe_hash"] == "hash-B"
        assert phase.should_skip(ctx) is not None  # now matches → clean skip

    def test_db_recipe_import_requires_recipe_hash(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """A fresh db import without a select-stamped recipe_hash fails loud.

        The hash pair is what makes every later run's skip decision sound, so an
        unhashed first import would re-open the silent-staleness hole — refuse it
        up front (the cockpit ``select`` always stamps the hash).
        """
        cc: dict[str, Any] = {"tables": [{"name": "t1", "sql": "SELECT 1"}]}
        source_id = str(uuid4())
        _seed_db_source(session, source_id, "warehouse", cc, with_raw_table=False)
        source = session.get(Source, source_id)
        assert source is not None

        result = ImportPhase()._load_database_source(
            PhaseContext(session=session, duckdb_conn=duckdb_conn, config={"source_id": source_id}),
            source,
            "warehouse",
            cc,
            "mssql",
        )
        assert result.status == PhaseStatus.FAILED
        assert "no recipe_hash" in (result.error or "")

    def test_db_recipe_import_stamps_witness(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """A successful db import copies recipe_hash to imported_recipe_hash.

        The witness completes the pair ``should_skip`` compares: right after a
        clean import the source skips (recipe unchanged), and a later re-pointed
        recipe (different recipe_hash) stops matching. Extraction + credentials
        are faked — the stamping contract, not the backend, is under test.
        """
        from unittest.mock import MagicMock, patch

        from dataraum.core.models import Result
        from dataraum.sources.backends import BackendExtractionResult, ExtractedTable

        cc: dict[str, Any] = {
            "tables": [{"name": "t1", "sql": "SELECT 1"}],
            "recipe_hash": "hash-A",
        }
        source_id = str(uuid4())
        _seed_db_source(session, source_id, "warehouse", cc, with_raw_table=False)
        source = session.get(Source, source_id)
        assert source is not None
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session, duckdb_conn=duckdb_conn, config={"source_id": source_id}
        )

        extracted = BackendExtractionResult(
            tables=[
                ExtractedTable(
                    name="t1",
                    duckdb_table="warehouse__t1",
                    row_count=2,
                    columns=[("a", "VARCHAR")],
                )
            ]
        )
        chain = MagicMock()
        chain.resolve.return_value = MagicMock(url="mssql://ignored")
        with (
            patch("dataraum.core.credentials.CredentialChain", return_value=chain),
            patch("dataraum.sources.backends.extract_backend", return_value=Result.ok(extracted)),
        ):
            result = phase._load_database_source(ctx, source, "warehouse", cc, "mssql")

        assert result.status == PhaseStatus.COMPLETED, result.error

        # Pin the PERSISTENCE, not the in-session attribute: flush, then expire,
        # so the assertions re-read the row from the DB. An in-place JSON
        # mutation (which SQLAlchemy would not change-track, so the flush would
        # persist nothing) fails here instead of passing on the live object.
        session.flush()
        session.expire_all()
        source = session.get(Source, source_id)
        assert source is not None
        assert source.connection_config is not None
        assert source.connection_config["imported_recipe_hash"] == "hash-A"
        assert source.connection_config["recipe_hash"] == "hash-A"

        # The pair now matches → the next run's should_skip is a clean skip.
        skip_reason = phase.should_skip(ctx)
        assert skip_reason is not None
        assert "recipe unchanged" in skip_reason

    def test_db_recipe_witness_stamp_merges_into_current_config(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection
    ):
        """The witness stamp merges into the ROW's current config, never the snapshot.

        The mid-import re-select wedge (DAT-430 review): a user re-selects while
        import runs, re-pointing ``connection_config`` to a NEW recipe; the
        engine's commit lands last. Stamping the phase-START snapshot would
        silently REVERT the user's new recipe. Instead the stamp merges into the
        row's current value: the new tables + recipe_hash survive, the witness
        records the recipe THIS import materialized, and the next run's hash
        compare fails loud (mismatch) rather than silently reverting.
        """
        from unittest.mock import MagicMock, patch

        from dataraum.core.models import Result
        from dataraum.sources.backends import BackendExtractionResult, ExtractedTable

        cc: dict[str, Any] = {
            "tables": [{"name": "t1", "sql": "SELECT 1"}],
            "recipe_hash": "hash-A",
        }
        source_id = str(uuid4())
        _seed_db_source(session, source_id, "warehouse", cc, with_raw_table=False)
        source = session.get(Source, source_id)
        assert source is not None
        phase = ImportPhase()
        ctx = PhaseContext(
            session=session, duckdb_conn=duckdb_conn, config={"source_id": source_id}
        )

        # Simulate the re-select landing mid-import: the row now carries a NEW
        # recipe while the phase still holds the OLD phase-start snapshot (cc).
        source.connection_config = {
            "tables": [{"name": "t2", "sql": "SELECT 2"}],
            "recipe_hash": "hash-B",
        }
        session.flush()

        extracted = BackendExtractionResult(
            tables=[
                ExtractedTable(
                    name="t1",
                    duckdb_table="warehouse__t1",
                    row_count=2,
                    columns=[("a", "VARCHAR")],
                )
            ]
        )
        chain = MagicMock()
        chain.resolve.return_value = MagicMock(url="mssql://ignored")
        with (
            patch("dataraum.core.credentials.CredentialChain", return_value=chain),
            patch("dataraum.sources.backends.extract_backend", return_value=Result.ok(extracted)),
        ):
            result = phase._load_database_source(ctx, source, "warehouse", cc, "mssql")

        assert result.status == PhaseStatus.COMPLETED, result.error
        session.flush()
        session.expire_all()
        source = session.get(Source, source_id)
        assert source is not None
        assert source.connection_config is not None
        # The user's re-pointed recipe survives the engine's commit …
        assert source.connection_config["tables"] == [{"name": "t2", "sql": "SELECT 2"}]
        assert source.connection_config["recipe_hash"] == "hash-B"
        # … and the witness names the recipe THIS import materialized, so the
        # next run mismatches (hash-B != hash-A) and fails loud, never skipping.
        assert source.connection_config["imported_recipe_hash"] == "hash-A"
        assert phase.should_skip(ctx) is None

    def test_drop_junk_columns(
        self, session: Session, duckdb_conn: duckdb.DuckDBPyConnection, tmp_path: Path
    ):
        """Test that junk columns are dropped."""
        # Create CSV with junk column
        csv_path = tmp_path / "with_junk.csv"
        csv_path.write_text(
            """id,name,Unnamed: 0
1,Alice,0
2,Bob,1
"""
        )

        phase = ImportPhase()
        source_id = str(uuid4())
        ctx = _file_ctx(
            session,
            duckdb_conn,
            source_id,
            "with_junk",
            csv_path,
            extra={"junk_columns": ["Unnamed: 0"]},
        )
        source = session.get(Source, source_id)
        assert source is not None

        result = phase._load_file_source(ctx, source, "with_junk", [str(csv_path)])

        assert result.status == PhaseStatus.COMPLETED

        # Verify junk column was removed from metadata
        table_id = result.outputs["raw_tables"][0]
        stmt = select(Column).where(Column.table_id == table_id)
        result_cols = session.execute(stmt)
        columns = result_cols.scalars().all()

        column_names = {c.column_name for c in columns}
        assert "Unnamed: 0" not in column_names
        assert column_names == {"id", "name"}
