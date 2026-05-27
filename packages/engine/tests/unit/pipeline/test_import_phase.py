"""Unit tests for the import phase.

DAT-290 collapsed multi-source semantics into single-source-per-session.
This module covers:

- TestColumnLimit: enforcement of the max-columns guard (orthogonal to
  source model).
- TestImportDispatch: ``_run`` dispatches on the bound source's type
  without orchestrating across multiple sources.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from dataraum.pipeline.base import PhaseContext, PhaseStatus
from dataraum.pipeline.phases.import_phase import ImportPhase


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
