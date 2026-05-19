"""Tests for core/vertical.py - VerticalConfig."""

from __future__ import annotations

from pathlib import Path

import pytest

from dataraum.core.vertical import VerticalConfig


class TestVerticalConfig:
    """Tests for VerticalConfig path resolution."""

    def test_resolves_paths_for_finance(self) -> None:
        """Check all properties resolve to correct paths for 'finance'."""
        vc = VerticalConfig("finance")
        assert vc.name == "finance"
        assert vc.base_dir.name == "finance"
        assert vc.base_dir.parent.name == "verticals"
        assert vc.ontology_path == vc.base_dir / "ontology.yaml"
        assert vc.cycles_path == vc.base_dir / "cycles.yaml"
        assert vc.validations_dir == vc.base_dir / "validations"
        assert vc.metrics_dir == vc.base_dir / "metrics"

    def test_nonexistent_vertical_raises(self) -> None:
        """Non-existent vertical raises FileNotFoundError on construction."""
        with pytest.raises(FileNotFoundError, match="not found"):
            VerticalConfig("nonexistent_vertical_xyz")

    def test_error_message_lists_available(self) -> None:
        """Error message lists available verticals."""
        with pytest.raises(FileNotFoundError, match="finance"):
            VerticalConfig("bad_name")

    def test_custom_verticals_dir(self, tmp_path: Path) -> None:
        """Custom verticals_dir is respected."""
        (tmp_path / "custom_vert").mkdir()
        vc = VerticalConfig("custom_vert", verticals_dir=tmp_path)
        assert vc.base_dir == tmp_path / "custom_vert"

    def test_custom_dir_nonexistent_raises(self, tmp_path: Path) -> None:
        """Custom verticals_dir with nonexistent vertical raises."""
        with pytest.raises(FileNotFoundError, match="missing"):
            VerticalConfig("missing", verticals_dir=tmp_path)
