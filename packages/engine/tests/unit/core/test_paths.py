"""Tests for the container path constants."""

from __future__ import annotations

from pathlib import Path

from dataraum.core.paths import CONFIG_DIR


def test_config_dir_is_container_absolute() -> None:
    assert CONFIG_DIR == Path("/opt/dataraum/config")
    assert CONFIG_DIR.is_absolute()
