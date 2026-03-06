"""Tests for the fix CLI command."""

from __future__ import annotations

from typer.testing import CliRunner

from dataraum.cli.main import app

runner = CliRunner()


class TestFixCommandRegistered:
    def test_help(self) -> None:
        result = runner.invoke(app, ["fix", "--help"])
        assert result.exit_code == 0
        assert "Review data quality actions" in result.output

    def test_listed_in_main_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "fix" in result.output

    def test_rerun_flag_in_help(self) -> None:
        """--rerun is listed as a valid option."""
        result = runner.invoke(app, ["fix", "--help"])
        assert result.exit_code == 0
        assert "--rerun" in result.output


class TestGetSourcePath:
    def test_returns_path_from_connection_config(self) -> None:
        """Extracts path from Source.connection_config."""
        from unittest.mock import MagicMock

        from dataraum.cli.commands.fix import _get_source_path

        source = MagicMock()
        source.connection_config = {"path": "/data/orders.csv"}
        result = _get_source_path(source)
        assert result is not None
        assert str(result) == "/data/orders.csv"

    def test_returns_none_when_no_config(self) -> None:
        """Returns None when connection_config is None."""
        from unittest.mock import MagicMock

        from dataraum.cli.commands.fix import _get_source_path

        source = MagicMock()
        source.connection_config = None
        result = _get_source_path(source)
        assert result is None

    def test_returns_none_when_no_path_key(self) -> None:
        """Returns None when connection_config has no 'path' key."""
        from unittest.mock import MagicMock

        from dataraum.cli.commands.fix import _get_source_path

        source = MagicMock()
        source.connection_config = {"host": "localhost"}
        result = _get_source_path(source)
        assert result is None


class TestSnapshotEntropy:
    def test_snapshot_returns_dimension_scores(self, session) -> None:
        """Snapshot returns average scores grouped by dimension path."""
        from dataraum.cli.commands.fix import _snapshot_entropy
        from dataraum.entropy.db_models import EntropyObjectRecord
        from dataraum.storage.models import Source

        source = Source(name="test_snap", source_type="csv")
        session.add(source)
        session.flush()

        # Add two entropy objects in the same dimension
        for score in [0.4, 0.6]:
            obj = EntropyObjectRecord(
                layer="semantic",
                dimension="units",
                sub_dimension="unit_declaration",
                target="column:orders.amount",
                source_id=source.source_id,
                score=score,
                detector_id="test",
            )
            session.add(obj)
        session.flush()

        result = _snapshot_entropy(session, source.source_id)
        assert "semantic.units.unit_declaration" in result
        assert abs(result["semantic.units.unit_declaration"] - 0.5) < 0.01

    def test_snapshot_empty_source(self, session) -> None:
        """Snapshot returns empty dict when no entropy objects exist."""
        from dataraum.cli.commands.fix import _snapshot_entropy

        result = _snapshot_entropy(session, "nonexistent")
        assert result == {}
