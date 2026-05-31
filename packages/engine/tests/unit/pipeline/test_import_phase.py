"""Unit tests for the import phase.

DAT-290 collapsed multi-source semantics into single-source-per-session.
This module covers:

- TestColumnLimit: enforcement of the max-columns guard (orthogonal to
  source model).
- TestImportDispatch: ``_run`` dispatches on the bound source's type
  without orchestrating across multiple sources.
- TestSuffixDispatch: file-source loader selection is driven by the source
  URI's suffix alone (DAT-389), not the filesystem.
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
                "source_connection_config": {"path": "/whatever.csv"},
            },
        )
        result = phase._run(ctx)
        assert result.status == PhaseStatus.FAILED
        assert "not found in the session DB" in (result.error or "")

    def test_run_fails_when_file_path_missing_from_config(self) -> None:
        """File-source dispatch needs a path in source_connection_config (or source_path)."""
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
        assert "no path" in (result.error or "").lower()

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

    No filesystem stat: an opaque ``s3://`` URI (or any local path) is routed to
    a loader purely by its extension, and the URI is handed to that loader
    verbatim. These tests patch the loader classes so the dispatch can be
    asserted without an object store.
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
                "source_connection_config": {"path": path},
            },
        )

    @pytest.mark.parametrize(
        ("uri", "expected_loader"),
        [
            ("s3://bucket/uploads/abc/orders.csv", "CSVLoader"),
            ("s3://bucket/data.parquet", "ParquetLoader"),
            ("s3://bucket/events.jsonl", "JsonLoader"),
            ("/local/dev/orders.tsv", "CSVLoader"),
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
