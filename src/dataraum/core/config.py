"""Configuration management.

Central config resolution for the entire application.
All modules load config through this module — never via Path(__file__) navigation.

Usage:
    from dataraum.core.config import get_config_file, load_yaml_config

    # Get a resolved path to a config file
    path = get_config_file("system/llm.yaml")

    # Load and parse a YAML config file
    data = load_yaml_config("system/entropy/thresholds.yaml")
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


def _find_config_dir() -> Path:
    """Find the config directory by walking up from the package location.

    This is the ONE place that does path-relative-to-file resolution.
    Everything else goes through get_config_file().
    """
    # src/dataraum/core/config.py -> 4 levels up -> project root
    package_dir = Path(__file__).resolve().parent.parent.parent.parent
    candidate = package_dir / "config"
    if candidate.is_dir():
        return candidate

    # Fallback: relative path (works when CWD is project root)
    return Path("config")


@lru_cache
def _get_config_root() -> Path:
    """Get the config root directory.

    Checks DATARAUM_CONFIG_PATH env var first, falls back to auto-detection.
    """
    env_path = os.environ.get("DATARAUM_CONFIG_PATH")
    if env_path:
        return Path(env_path)
    return _find_config_dir()


def get_config_file(relative_path: str) -> Path:
    """Resolve a config file path relative to the config root.

    This is the central entry point for all config file access.
    Modules should use this instead of constructing paths themselves.

    Args:
        relative_path: Path relative to config/, e.g. "system/llm.yaml"
                       or "verticals/finance/ontology.yaml"

    Returns:
        Resolved absolute Path to the config file.

    Raises:
        FileNotFoundError: If the resolved path does not exist.
    """
    config_root = _get_config_root()
    resolved = config_root / relative_path
    if not resolved.exists():
        raise FileNotFoundError(
            f"Config file not found: {resolved} "
            f"(config root: {config_root}, relative: {relative_path})"
        )
    return resolved


def get_config_dir(relative_path: str) -> Path:
    """Resolve a config directory path relative to the config root.

    Args:
        relative_path: Directory path relative to config/,
                       e.g. "system/prompts" or "verticals/finance/validations"

    Returns:
        Resolved absolute Path to the config directory.

    Raises:
        FileNotFoundError: If the resolved path does not exist or is not a directory.
    """
    config_root = _get_config_root()
    resolved = config_root / relative_path
    if not resolved.is_dir():
        raise FileNotFoundError(
            f"Config directory not found: {resolved} "
            f"(config root: {config_root}, relative: {relative_path})"
        )
    return resolved


def load_yaml_config(relative_path: str) -> dict[str, Any]:
    """Load and parse a YAML config file.

    Convenience function that combines get_config_file() + yaml.safe_load().

    Args:
        relative_path: Path relative to config/, e.g. "system/llm.yaml"

    Returns:
        Parsed YAML content as a dict.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the YAML is invalid.
    """
    path = get_config_file(relative_path)
    with open(path) as f:
        data = yaml.safe_load(f)
    if data is None:
        return {}
    result: dict[str, Any] = data
    return result
