"""Unit tests for the import phase.

DAT-290 collapsed multi-source semantics into single-source-per-session.
This module covers:

- TestColumnLimit: enforcement of the max-columns guard (orthogonal to
  source model).
- TestImportDispatch: ``_run`` dispatches on the bound source's type
  without orchestrating across multiple sources.
- TestSuffixDispatch: file-source loader selection is driven by the source
  URI's suffix alone (DAT-389), not the filesystem.
- TestMultiUriDispatch: a file source carries a list of explicit ``s3://`` URIs
  under ``connection_config['file_uris']`` (DAT-378); ``_run`` validates EVERY
  element (the engine never globs) then loads each in turn, so one import yields
  one raw table per URI and a single bad element fails the whole import.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from dataraum.core.models import Result
from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.import_phase import ImportPhase
from dataraum.sources.csv.models import StagedTable


class TestColumnLimit:
    """Tests for the column limit enforcement."""

    def test_column_limit_check_under_limit(self):
        """_check_column_limit returns None when under limit."""
        phase = ImportPhase()
        mock_session = MagicMock()
        mock_session.execute.return_value.scalar_one.return_value = 50

        ctx = PhaseContext(
            session=mock_session,
            duckdb_conn=MagicMock(),
            source_id="test-source",
            config={},
        )

        with patch(
            "dataraum.pipeline.phases.import_phase.load_pipeline_config",
            return_value={"limits": {"max_columns": 500}},
        ):
            result = phase._check_column_limit(ctx)

        assert result is None

    def test_column_limit_check_over_limit(self):
        """_check_column_limit returns error when over limit."""
        phase = ImportPhase()
        mock_session = MagicMock()
        mock_session.execute.return_value.scalar_one.return_value = 600

        ctx = PhaseContext(
            session=mock_session,
            duckdb_conn=MagicMock(),
            source_id="test-source",
            config={},
        )

        with patch(
            "dataraum.pipeline.phases.import_phase.load_pipeline_config",
            return_value={"limits": {"max_columns": 500}},
        ):
            result = phase._check_column_limit(ctx)

        assert result is not None
        assert "600 > 500" in result
        assert "limits.max_columns" in result

    def test_column_limit_defaults_to_500(self):
        """When limits section missing, defaults to 500."""
        phase = ImportPhase()
        mock_session = MagicMock()
        mock_session.execute.return_value.scalar_one.return_value = 50

        ctx = PhaseContext(
            session=mock_session,
            duckdb_conn=MagicMock(),
            source_id="test-source",
            config={},
        )

        with patch(
            "dataraum.pipeline.phases.import_phase.load_pipeline_config",
            return_value={},
        ):
            result = phase._check_column_limit(ctx)

        assert result is None

    def test_run_fails_when_no_source(self):
        """_run fails when ctx.config is missing source_name / source_type."""
        phase = ImportPhase()
        ctx = PhaseContext(
            session=MagicMock(),
            duckdb_conn=MagicMock(),
            source_id="test-source",
            config={},
        )

        result = phase._run(ctx)

        assert result.status == PhaseStatus.FAILED
        assert "source_name" in (result.error or "")
        assert "source_type" in (result.error or "")


class TestImportDispatch:
    """Tests for ``_run``'s dispatch on single-source configuration.

    Per DAT-290 the import phase consumes a single source's identity from
    ``ctx.config``. The orchestration over multiple registered sources is
    gone — see git log for the old ``_load_registered_sources`` tests.
    """

    def _ctx(self, config: dict[str, Any]) -> PhaseContext:
        return PhaseContext(
            session=MagicMock(),
            duckdb_conn=MagicMock(),
            source_id="test-source",
            config=config,
        )

    def test_run_fails_when_source_row_missing(self) -> None:
        """_run reports a missing Source row clearly rather than crashing."""
        phase = ImportPhase()
        # session.get(Source, ...) returns None — simulate by configuring the mock
        session = MagicMock()
        session.get.return_value = None
        ctx = PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            source_id="test-source",
            config={
                "source_name": "missing",
                "source_type": "csv",
                "source_connection_config": {"file_uris": ["s3://dataraum-lake/whatever.csv"]},
            },
        )
        result = phase._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "not found in the session DB" in (result.error or "")

    def test_run_fails_when_file_uris_missing_from_config(self) -> None:
        """File-source dispatch needs a non-empty file_uris list."""
        phase = ImportPhase()
        session = MagicMock()
        session.get.return_value = MagicMock()  # any non-None
        ctx = PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            source_id="test-source",
            config={
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
            source_id="test-source",
            config={
                "source_name": "broken_recipe",
                "source_type": "db_recipe",
                "source_connection_config": {"tables": [{"name": "t", "sql": "SELECT 1"}]},
                "source_backend": None,
            },
        )
        result = phase._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "backend" in (result.error or "").lower()


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
            source_id="test-source",
            config={
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
            patch.object(ImportPhase, "_check_column_limit", return_value=None),
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
    """A file source loads a LIST of explicit ``s3://`` URIs (DAT-378).

    The cockpit ``select`` stage enumerates a prefix (ListObjectsV2) into an
    explicit, immutable ``connection_config['file_uris']`` list before the
    workflow triggers. ``_run`` validates EVERY element through
    ``validate_source_uri`` (the engine never globs) then loads each in turn, so
    one import activity yields one raw table per URI. A single bad element fails
    the whole import before any loader runs.
    """

    def _ctx(self, file_uris: list[str]) -> PhaseContext:
        session = MagicMock()
        session.get.return_value = MagicMock()  # Source row present
        return PhaseContext(
            session=session,
            duckdb_conn=MagicMock(),
            source_id="test-source",
            config={
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
            patch.object(ImportPhase, "_check_column_limit", return_value=None),
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

    def test_load_failure_mid_list_rolls_back_and_drops(self) -> None:
        """A loader failure mid-list undoes the partial load — nothing commits (DAT-378).

        URIs before the failure already created raw DuckDB tables + Table rows.
        Because ``PhaseResult.failed`` is a RETURN (``run_phase``'s ``session_scope``
        commits on clean exit), the phase must DROP this run's DuckDB tables and
        roll the session back — otherwise a re-run's ``should_skip`` would see the
        partial raw tables and silently skip the URIs past the failure.
        """
        phase = ImportPhase()
        uris = [
            "s3://dataraum-lake/sel/customers.csv",
            "s3://dataraum-lake/sel/orders.csv",  # valid URI; the loader fails on it
        ]
        ctx = self._ctx(uris)

        staged = StagedTable(
            table_id="t",
            table_name="src__customers",
            raw_table_name="src__customers",
            row_count=1,
            column_count=1,
        )
        csv_loader = MagicMock()
        # First URI loads; the second URI's loader fails.
        csv_loader.return_value._load_single_file.side_effect = [
            Result.ok(staged),
            Result.fail("boom"),
        ]

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
        # The partial load was undone: the session rolled back and the one DuckDB
        # table created before the failure was dropped.
        ctx.session.rollback.assert_called_once()
        drops = [
            str(c.args[0])
            for c in ctx.duckdb_conn.execute.call_args_list
            if c.args and "DROP TABLE IF EXISTS" in str(c.args[0])
        ]
        assert any("src__customers" in d for d in drops), drops

    def test_duplicate_basenames_fail_loud_before_loading(self) -> None:
        """Two URIs with the same basename fail the import up front (DAT-378).

        Both ``2024/data.csv`` and ``2025/data.csv`` map to raw table
        ``src__data``; the engine can't merge them onto one table, so ``_run``
        rejects the list BEFORE any loader runs rather than letting the second
        ``CREATE OR REPLACE`` clobber the first.
        """
        phase = ImportPhase()
        uris = [
            "s3://dataraum-lake/2024/data.csv",
            "s3://dataraum-lake/2025/data.csv",  # same basename -> same raw table
        ]
        ctx = self._ctx(uris)

        staged = StagedTable(
            table_id="t",
            table_name="src__data",
            raw_table_name="src__data",
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
        assert "same raw table" in (result.error or "")
        # Fail-loud happens before any load — no loader was constructed/called.
        for inst in loaders.values():
            inst.return_value._load_single_file.assert_not_called()
