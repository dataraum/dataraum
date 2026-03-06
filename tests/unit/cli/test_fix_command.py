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
