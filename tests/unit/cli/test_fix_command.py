"""Tests for the fix CLI command."""

from __future__ import annotations

import re

from typer.testing import CliRunner

from dataraum.cli.main import app

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


class TestFixCommandRegistered:
    def test_help(self) -> None:
        result = runner.invoke(app, ["fix", "--help"])
        assert result.exit_code == 0
        assert "quality gates" in result.output.lower()

    def test_listed_in_main_help(self) -> None:
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "fix" in result.output

    def test_contract_flag_in_help(self) -> None:
        """--contract is listed as a valid option."""
        result = runner.invoke(app, ["fix", "--help"])
        assert result.exit_code == 0
        assert "--contract" in _strip_ansi(result.output)

    def test_name_flag_in_help(self) -> None:
        """--name is listed as a valid option."""
        result = runner.invoke(app, ["fix", "--help"])
        assert result.exit_code == 0
        assert "--name" in _strip_ansi(result.output)

    def test_source_argument_in_help(self) -> None:
        """Source path argument is documented."""
        result = runner.invoke(app, ["fix", "--help"])
        assert result.exit_code == 0
        assert "source" in result.output.lower()


class TestFixValidation:
    def test_nonexistent_source_path(self, tmp_path) -> None:
        """Fix command fails when source path doesn't exist."""
        result = runner.invoke(app, ["fix", str(tmp_path / "nope.csv")])
        assert result.exit_code != 0
        assert "does not exist" in result.output
