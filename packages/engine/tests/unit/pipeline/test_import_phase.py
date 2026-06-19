"""Unit tests for the import phase.

The import phase loads ONE source per activity of an add_source run (DAT-422).
This module covers:

- TestImportDispatch: ``_run`` validates its config and dispatches on the bound
  source's type. (The column limit is no longer checked here — the run-scoped
  ``check_column_limit`` gate owns it, DAT-430; see
  ``tests/unit/worker/test_check_column_limit.py``.)
- TestSuffixDispatch: file-source loader selection is driven by the source
  URI's suffix alone (DAT-389), not the filesystem.
- TestMultiUriDispatch: the file loader loop is list-generic over
  ``connection_config['file_uris']`` — ``_run`` validates EVERY element (the
  engine never globs) then loads each in turn, and a single bad element fails
  the whole import. The cockpit ``select`` persists one-element lists today
  (one content-keyed source per file, DAT-422). Mid-list atomicity is owned by
  the phase runner's rollback-on-FAILED (DAT-502), not a phase-local helper.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dataraum.core.models import Result
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.sources.csv.models import StagedTable


class TestImportDispatch:
    """Tests for ``_run``'s config validation + dispatch on the bound source's type."""

    def _ctx(self, config: dict[str, Any]) -> PhaseContext:
        return PhaseContext(
            session=MagicMock(),
            duckdb_conn=MagicMock(),
            config=config,
        )

    def test_run_fails_when_no_source(self):
        """_run fails when ctx.config is missing source_name / source_type."""
        phase = ImportPhase()
        ctx = self._ctx({})

        result = phase._run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "source_name" in (result.error or "")
        assert "source_type" in (result.error or "")

    def test_run_fails_when_source_row_missing(self) -> None:
        """_run reports a missing Source row clearly rather than crashing."""
        phase = ImportPhase()
        # session.get(Source, ...) returns None — simulate by configuring the mock
        session = MagicMock()
        session.get.return_value = None
        ctx = PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            config={
                "source_id": "test-source",
                "source_name": "missing",
                "source_type": "csv",
                "source_connection_config": {"file_uris": ["s3://dataraum-lake/whatever.csv"]},
            },
        )
        result = phase._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "not found in the workspace DB" in (result.error or "")

    def test_run_fails_when_file_uris_missing_from_config(self) -> None:
        """File-source dispatch needs a non-empty file_uris list."""
        phase = ImportPhase()
        session = MagicMock()
        session.get.return_value = MagicMock()  # any non-None
        ctx = PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            config={
                "source_id": "test-source",
                "source_name": "noplace",
                "source_type": "csv",
                "source_connection_config": {},
            },
        )
        result = phase._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "no file uris" in (result.error or "").lower()

    def test_run_fails_for_db_recipe_without_backend(self) -> None:
        """db_recipe sources must declare a backend."""
        phase = ImportPhase()
        session = MagicMock()
        session.get.return_value = MagicMock()
        ctx = PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            config={
                "source_id": "test-source",
                "source_name": "broken_recipe",
                "source_type": "db_recipe",
                "source_connection_config": {"tables": [{"name": "t", "sql": "SELECT 1"}]},
                "source_backend": None,
            },
        )
        result = phase._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "backend" in (result.error or "").lower()

    def test_db_recipe_resolves_credentials_by_credential_source(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A db_recipe source reads through ``credential_source``, NOT its own name
        (DAT-592): a probed query imported as ``wwi_recent_orders`` resolves the
        ``wwi`` connection's ``DATARAUM_WWI_URL`` — never
        ``DATARAUM_WWI_RECENT_ORDERS_URL``. Both env keys are unset here, so the
        lookup fails; the message must name the CONNECTION's key, proving
        ``credential_source`` (not the source name) is the resolution key."""
        monkeypatch.delenv("DATARAUM_WWI_URL", raising=False)
        monkeypatch.delenv("DATARAUM_WWI_RECENT_ORDERS_URL", raising=False)
        phase = ImportPhase()
        session = MagicMock()
        session.get.return_value = MagicMock()  # Source row present
        # No existing raw tables for this source (the re-import guard reads []).
        session.execute.return_value.scalars.return_value.all.return_value = []
        ctx = PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            config={
                "source_id": "test-source",
                "source_name": "wwi_recent_orders",
                "source_type": "db_recipe",
                "source_connection_config": {
                    "tables": [{"name": "t", "sql": "SELECT 1"}],
                    "recipe_hash": "abc",
                    "credential_source": "wwi",
                },
                "source_backend": "mssql",
            },
        )
        result = phase._run(ctx)
        assert result.status == PhaseStatus.FAILED
        err = result.error or ""
        assert "DATARAUM_WWI_URL" in err
        assert "DATARAUM_WWI_RECENT_ORDERS_URL" not in err
        assert "connection 'wwi'" in err


class TestSuffixDispatch:
    """File-source loader selection is driven by the URI suffix alone (DAT-389).

    No filesystem stat: an ``s3://<lake-bucket>/<key>`` URI is routed to a loader
    purely by its extension, and the URI is handed to that loader verbatim. The
    URI is first gated through ``validate_source_uri`` — anything but the lake
    bucket fails before dispatch (see ``test_rejects_non_lake_bucket_uri``).
    These tests patch the loader classes so the dispatch can be asserted without
    an object store.
    """

    def _ctx(self, path: str) -> PhaseContext:
        session = MagicMock()
        session.get.return_value = MagicMock()  # Source row present
        return PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            config={
                "source_id": "test-source",
                "source_name": "src",
                "source_type": "file",
                "source_connection_config": {"file_uris": [path]},
            },
        )

    @pytest.mark.parametrize(
        ("uri", "expected_loader"),
        [
            ("s3://dataraum-lake/uploads/abc/orders.csv", "CSVLoader"),
            ("s3://dataraum-lake/data.parquet", "ParquetLoader"),
            ("s3://dataraum-lake/events.jsonl", "JsonLoader"),
            ("s3://dataraum-lake/legacy/orders.tsv", "CSVLoader"),
            # Cockpit-parity extensions (DAT-378): ndjson MUST be JSON, not CSV.
            ("s3://dataraum-lake/events.ndjson", "JsonLoader"),
            ("s3://dataraum-lake/notes.txt", "CSVLoader"),
            ("s3://dataraum-lake/data.pq", "ParquetLoader"),
        ],
    )
    def test_loader_chosen_by_suffix(self, uri: str, expected_loader: str) -> None:
        phase = ImportPhase()
        ctx = self._ctx(uri)

        staged = StagedTable(
            table_id="t1",
            table_name="src__orders",
            raw_table_name="src__orders",
            row_count=1,
            column_count=1,
        )
        loaders = {
            "CSVLoader": MagicMock(),
            "ParquetLoader": MagicMock(),
            "JsonLoader": MagicMock(),
        }
        for inst in loaders.values():
            inst.return_value._load_single_file.return_value = Result.ok(staged)

        with (
            patch("dataraum.pipeline.phases.import_phase.CSVLoader", loaders["CSVLoader"]),
            patch("dataraum.pipeline.phases.import_phase.ParquetLoader", loaders["ParquetLoader"]),
            patch("dataraum.pipeline.phases.import_phase.JsonLoader", loaders["JsonLoader"]),
            patch(
                "dataraum.pipeline.phases.import_phase.load_null_value_config",
                return_value=MagicMock(),
            ),
        ):
            result = phase._run(ctx)

        assert result.status == PhaseStatus.COMPLETED, result.error
        # The chosen loader was constructed + handed the URI verbatim; the others were not.
        for name, inst in loaders.items():
            if name == expected_loader:
                inst.return_value._load_single_file.assert_called_once()
                kwargs = inst.return_value._load_single_file.call_args.kwargs
                assert kwargs["source_uri"] == uri
            else:
                inst.return_value._load_single_file.assert_not_called()

    @pytest.mark.parametrize(
        "bad_uri",
        [
            "/etc/passwd",
            "/app/.env",
            "../foo.csv",
            "file:///etc/passwd",
            "orders.csv",
            "s3://other-bucket/orders.csv",
            "s3://key:secret@dataraum-lake/orders.csv",
        ],
    )
    def test_rejects_non_lake_bucket_uri(self, bad_uri: str) -> None:
        """A non-lake-bucket URI fails loudly before any loader runs (DAT-389).

        The URI is handed verbatim to DuckDB's ``read_*_auto``, so the import
        ingress must refuse it — never a silent arbitrary-file or foreign-bucket
        read. No loader is constructed.
        """
        phase = ImportPhase()
        ctx = self._ctx(bad_uri)

        loaders = {
            "CSVLoader": MagicMock(),
            "ParquetLoader": MagicMock(),
            "JsonLoader": MagicMock(),
        }
        with (
            patch("dataraum.pipeline.phases.import_phase.CSVLoader", loaders["CSVLoader"]),
            patch("dataraum.pipeline.phases.import_phase.ParquetLoader", loaders["ParquetLoader"]),
            patch("dataraum.pipeline.phases.import_phase.JsonLoader", loaders["JsonLoader"]),
            patch(
                "dataraum.pipeline.phases.import_phase.load_null_value_config",
                return_value=MagicMock(),
            ),
        ):
            result = phase._run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "Invalid source URI" in (result.error or "")
        for inst in loaders.values():
            inst.return_value._load_single_file.assert_not_called()


class TestMultiUriDispatch:
    """The file loader loop is list-generic over ``connection_config['file_uris']``.

    The cockpit ``select`` tool persists one-element lists today (one
    content-keyed source per file, DAT-422), but the loop is the load
    mechanism: ``_run`` validates EVERY element through ``validate_source_uri``
    (the engine never globs) then loads each in turn, and a single bad element
    fails the whole import before any loader runs.
    """

    def _ctx(self, file_uris: list[str]) -> PhaseContext:
        session = MagicMock()
        session.get.return_value = MagicMock()  # Source row present
        return PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            config={
                "source_id": "test-source",
                "source_name": "src",
                "source_type": "file",
                "source_connection_config": {"file_uris": file_uris},
            },
        )

    def _patched_loaders(self, staged: StagedTable) -> dict[str, MagicMock]:
        loaders = {
            "CSVLoader": MagicMock(),
            "ParquetLoader": MagicMock(),
            "JsonLoader": MagicMock(),
        }
        for inst in loaders.values():
            inst.return_value._load_single_file.return_value = Result.ok(staged)
        return loaders

    def test_loads_one_table_per_uri(self) -> None:
        """A 3-URI list drives three loader calls, one per object, in order."""
        phase = ImportPhase()
        uris = [
            "s3://dataraum-lake/sel/customers.csv",
            "s3://dataraum-lake/sel/orders.parquet",
            "s3://dataraum-lake/sel/events.jsonl",
        ]
        ctx = self._ctx(uris)

        staged = StagedTable(
            table_id="t",
            table_name="src__t",
            raw_table_name="src__t",
            row_count=1,
            column_count=1,
        )
        loaders = self._patched_loaders(staged)

        with (
            patch("dataraum.pipeline.phases.import_phase.CSVLoader", loaders["CSVLoader"]),
            patch("dataraum.pipeline.phases.import_phase.ParquetLoader", loaders["ParquetLoader"]),
            patch("dataraum.pipeline.phases.import_phase.JsonLoader", loaders["JsonLoader"]),
            patch(
                "dataraum.pipeline.phases.import_phase.load_null_value_config",
                return_value=MagicMock(),
            ),
        ):
            result = phase._run(ctx)

        assert result.status == PhaseStatus.COMPLETED, result.error
        # One loader call per URI — the CSV / Parquet / JSON URIs each hit their
        # own loader, dispatched per-element by suffix.
        loaders["CSVLoader"].return_value._load_single_file.assert_called_once()
        loaders["ParquetLoader"].return_value._load_single_file.assert_called_once()
        loaders["JsonLoader"].return_value._load_single_file.assert_called_once()
        # Three objects loaded → three raw tables aggregated into one result.
        assert result.outputs is not None
        assert len(result.outputs["raw_tables"]) == 3

    def test_one_bad_element_fails_whole_import_before_any_loader(self) -> None:
        """A single non-lake URI in the list fails the import; no loader runs.

        Per-element validation means the engine never partially-loads a list that
        contains an arbitrary-file / foreign-bucket read primitive.
        """
        phase = ImportPhase()
        uris = [
            "s3://dataraum-lake/sel/customers.csv",
            "/etc/passwd",  # smuggled local path
            "s3://dataraum-lake/sel/orders.parquet",
        ]
        ctx = self._ctx(uris)

        staged = StagedTable(
            table_id="t",
            table_name="src__t",
            raw_table_name="src__t",
            row_count=1,
            column_count=1,
        )
        loaders = self._patched_loaders(staged)

        with (
            patch("dataraum.pipeline.phases.import_phase.CSVLoader", loaders["CSVLoader"]),
            patch("dataraum.pipeline.phases.import_phase.ParquetLoader", loaders["ParquetLoader"]),
            patch("dataraum.pipeline.phases.import_phase.JsonLoader", loaders["JsonLoader"]),
            patch(
                "dataraum.pipeline.phases.import_phase.load_null_value_config",
                return_value=MagicMock(),
            ),
        ):
            result = phase._run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "Invalid source URI" in (result.error or "")
        # The list is fully validated before any load, so NOTHING loads.
        for inst in loaders.values():
            inst.return_value._load_single_file.assert_not_called()

    def test_mid_list_failure_commits_nothing(self) -> None:
        """No Postgres rows survive a mid-list loader failure (DAT-502).

        The import phase carries no rollback helper of its own anymore:
        within-attempt atomicity is owned by the phase runner — ``run_phase`` /
        ``run_session_phase`` roll the session back on a FAILED result
        (``9d262fde``), so the Table rows the URIs before the failure wrote
        never commit and a re-run's ``should_skip`` sees no raw tables.
        Leftover raw DuckDB tables are harmless (``CREATE OR REPLACE``).

        This drives the real writer over a real session: the first loader call
        persists a Table row exactly like a real loader, the second fails, and
        after the runner's rollback contract nothing is visible to a fresh
        session — the phase itself must never commit mid-list.
        """
        from sqlalchemy import create_engine, select
        from sqlalchemy.orm import sessionmaker
        from sqlalchemy.pool import StaticPool

        from dataraum.storage import Source, Table, init_database

        engine = create_engine(
            "sqlite:///:memory:",
            connect_args={"check_same_thread": False},
            poolclass=StaticPool,
        )
        init_database(engine)
        factory = sessionmaker(bind=engine)

        phase = ImportPhase()
        uris = [
            "s3://dataraum-lake/sel/customers.csv",
            "s3://dataraum-lake/sel/orders.csv",  # valid URI; the loader fails on it
        ]

        staged = StagedTable(
            table_id="t1",
            table_name="src__customers",
            raw_table_name="src__customers",
            row_count=1,
            column_count=1,
        )

        with factory() as session:
            session.add(Source(source_id="test-source", name="src", source_type="csv"))
            session.commit()

            ctx = PhaseContext(
                session=session,
                duckdb_conn=MagicMock(),
                config={
                    "source_id": "test-source",
                    "source_name": "src",
                    "source_type": "file",
                    "source_connection_config": {"file_uris": uris},
                },
            )

            calls: list[int] = []

            def _load_then_fail(**kwargs: Any) -> Result[StagedTable]:
                calls.append(1)
                if len(calls) == 1:
                    # A real loader writes the raw Table row into the session.
                    kwargs["session"].add(
                        Table(
                            table_id="t1",
                            source_id="test-source",
                            table_name="src__customers",
                            layer="raw",
                            duckdb_path="src__customers",
                        )
                    )
                    return Result.ok(staged)
                return Result.fail("boom")

            csv_loader = MagicMock()
            csv_loader.return_value._load_single_file.side_effect = _load_then_fail

            with (
                patch("dataraum.pipeline.phases.import_phase.CSVLoader", csv_loader),
                patch(
                    "dataraum.pipeline.phases.import_phase.load_null_value_config",
                    return_value=MagicMock(),
                ),
            ):
                result = phase._run(ctx)

            assert result.status == PhaseStatus.FAILED
            assert "boom" in (result.error or "")
            # The runner's contract (run_phase: FAILED → session.rollback()).
            session.rollback()

        # Nothing committed: a fresh session sees no raw tables, so the next
        # run's should_skip re-imports the whole list.
        with factory() as session:
            rows = list(session.execute(select(Table)).scalars())
        assert rows == []
