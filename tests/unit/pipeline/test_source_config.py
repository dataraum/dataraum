"""Tests for per-source config copy logic."""

from pathlib import Path

import yaml

from dataraum.core.config import _get_config_root, reset_config_root
from dataraum.pipeline.setup import _ensure_source_config


class TestEnsureSourceConfig:
    """Tests for _ensure_source_config()."""

    def teardown_method(self) -> None:
        """Reset config root after each test."""
        reset_config_root()

    def test_copies_config_on_first_run(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        _ensure_source_config(output_dir)

        source_config = output_dir / "config"
        assert source_config.is_dir()
        # Should have pipeline.yaml from global config
        assert (source_config / "pipeline.yaml").exists()
        # Should have phases/ directory
        assert (source_config / "phases").is_dir()

    def test_switches_config_root(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        _ensure_source_config(output_dir)

        assert _get_config_root() == output_dir / "config"

    def test_preserves_existing_config(self, tmp_path: Path) -> None:
        """Second run should not overwrite user modifications."""
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        # First run: copy
        _ensure_source_config(output_dir)
        reset_config_root()

        # Simulate user fix: modify a config file
        source_config = output_dir / "config"
        phases_dir = source_config / "phases"
        custom_file = phases_dir / "typing.yaml"
        original = yaml.safe_load(custom_file.read_text()) if custom_file.exists() else {}
        original["user_added_key"] = "custom_value"
        custom_file.write_text(yaml.dump(original))

        # Second run: should reuse, not overwrite
        _ensure_source_config(output_dir)

        data = yaml.safe_load(custom_file.read_text())
        assert data["user_added_key"] == "custom_value"

    def test_copies_phase_configs(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        _ensure_source_config(output_dir)

        source_config = output_dir / "config"
        # Check some known phase configs exist in the copy
        assert (source_config / "phases" / "import.yaml").exists()

    def test_copies_entropy_config(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        _ensure_source_config(output_dir)

        source_config = output_dir / "config"
        assert (source_config / "entropy").is_dir()

    def test_copies_verticals(self, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        output_dir.mkdir()

        _ensure_source_config(output_dir)

        source_config = output_dir / "config"
        assert (source_config / "verticals").is_dir()
