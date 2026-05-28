"""Configuration management.

Central config resolution for the entire application.
All modules load config through this module — never via Path(__file__) navigation.

Usage:
    from dataraum.core.config import get_config_file, load_yaml_config

    # Get a resolved path to a config file
    path = get_config_file("llm/config.yaml")

    # Load and parse a YAML config file
    data = load_yaml_config("entropy/thresholds.yaml")

    # Load per-phase config by convention
    from dataraum.core.config import load_phase_config
    cfg = load_phase_config("statistics")  # -> config/phases/statistics.yaml
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

from dataraum.core.overlay import apply_overlay

# Module-level state. ``_config_root_override`` is the test/override slot
# that wins over everything. ``_active_workspace_config_dir`` is set by
# ``dataraum.server.workspace.bootstrap_workspace`` at FastAPI startup and
# points at the writable workspace overlay under DATARAUM_HOME.
_config_root_override: Path | None = None
_active_workspace_config_dir: Path | None = None


@lru_cache
def _find_config_dir() -> Path:
    """Locate the standalone ``dataraum-config`` package on the host.

    Config is a sibling package (``packages/dataraum-config/``), not part of
    the engine. In containers ``DATARAUM_CONFIG_PATH`` points at the
    bind-mounted copy and this function is never reached; this is the
    dev / CLI / test fallback that finds the sibling next to
    ``packages/engine/``.

    This is the ONE place that does path-relative-to-file resolution.
    Everything else goes through get_config_file().

    Cached because it does filesystem traversal with a stable result.
    """
    # src/dataraum/core/config.py -> 4 levels up -> packages/engine
    engine_dir = Path(__file__).resolve().parent.parent.parent.parent
    candidate = engine_dir.parent / "dataraum-config"
    if candidate.is_dir():
        return candidate

    # Fallback: relative path. Only works when CWD is the monorepo root;
    # in non-dev contexts prefer DATARAUM_CONFIG_PATH (priority 3 above).
    return Path("packages/dataraum-config")


def _get_config_root() -> Path:
    """Get the config root directory.

    Priority (highest first):
        1. ``set_config_root()`` override (tests).
        2. Active workspace ``config_dir`` set by
           ``bootstrap_workspace`` at FastAPI startup. Production reads
           land here once the server has booted.
        3. ``DATARAUM_CONFIG_PATH`` env var. Points at the
           ``dataraum-config`` package bind-mounted into the container
           (DAT-361 — config is mounted, not baked); also the bootstrap
           copy source for the workspace overlay and a fallback before a
           workspace exists (e.g. CLI tools, tests that haven't bootstrapped).
        4. Auto-detection of the sibling ``dataraum-config`` package
           (dev/CLI fallback).
    """
    if _config_root_override is not None:
        return _config_root_override
    if _active_workspace_config_dir is not None:
        return _active_workspace_config_dir
    env_path = os.environ.get("DATARAUM_CONFIG_PATH")
    if env_path:
        return Path(env_path)
    return _find_config_dir()


def set_config_root(path: Path) -> None:
    """Override the config root for the current process.

    Top-priority slot — wins over the active-workspace config_dir and the
    env var. Primarily used by tests that want to point the loader at a
    fixture config tree. Production code never calls this.

    Args:
        path: Absolute path to the config root directory.
    """
    global _config_root_override  # noqa: PLW0603
    _config_root_override = path


def reset_config_root() -> None:
    """Clear the config root override, reverting to default resolution.

    Primarily for testing.
    """
    global _config_root_override  # noqa: PLW0603
    _config_root_override = None


def set_active_workspace_config_dir(path: Path) -> None:
    """Register the active workspace's writable config overlay.

    Called once per process by ``bootstrap_workspace`` after the
    workspace row is loaded/created and its ``config_dir`` is
    populated. From that point on, ``_get_config_root()`` resolves to
    this path (unless a higher-priority override is set).

    Args:
        path: Absolute path to ``${DATARAUM_HOME}/workspaces/<id>/config/``.
    """
    global _active_workspace_config_dir  # noqa: PLW0603
    _active_workspace_config_dir = path


def reset_active_workspace_for_tests() -> None:
    """Clear the active workspace pointer. Tests only."""
    global _active_workspace_config_dir  # noqa: PLW0603
    _active_workspace_config_dir = None


def get_config_file(relative_path: str) -> Path:
    """Resolve a config file path relative to the config root.

    This is the central entry point for all config file access.
    Modules should use this instead of constructing paths themselves.

    Args:
        relative_path: Path relative to config/, e.g. "llm/config.yaml"
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
                       e.g. "llm/prompts" or "verticals/finance/validations"

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
    """Load and parse a YAML config file, layered with any active overlay rows.

    Convenience function that combines get_config_file() + yaml.safe_load(),
    then applies per-workspace teach edits via
    :func:`dataraum.core.overlay.apply_overlay` (no-op when no resolver is
    registered — e.g. CLI / tests that never bootstrap a workspace).

    Args:
        relative_path: Path relative to config/, e.g. "llm/config.yaml"

    Returns:
        Parsed YAML content as a dict, with any active workspace overlay rows
        for this file merged in.

    Raises:
        FileNotFoundError: If the config file does not exist.
        yaml.YAMLError: If the YAML is invalid.
    """
    path = get_config_file(relative_path)
    with open(path) as f:
        data = yaml.safe_load(f)
    if data is None:
        data = {}
    result: dict[str, Any] = data
    return apply_overlay(relative_path, result)


def load_phase_config(
    phase_name: str,
    config_root: Path | None = None,
) -> dict[str, Any]:
    """Load config for a pipeline phase by convention.

    Looks for config/phases/<phase_name>.yaml. Returns empty dict if the
    file doesn't exist (some phases have no config).

    When ``config_root`` is None (the production / worker path), the file
    is loaded via :func:`load_yaml_config`, so any registered teach
    overlay rows for ``phases/<phase_name>.yaml`` are merged in. With an
    explicit ``config_root`` (test fixtures pointing at a custom tree)
    the overlay is bypassed — fixtures are deterministic.

    Args:
        phase_name: Phase name, e.g. "statistics" -> config/phases/statistics.yaml
        config_root: Optional config root override. Uses default if None.

    Returns:
        Parsed YAML content as a dict, or empty dict if file doesn't exist.

    Raises:
        yaml.YAMLError: If the file exists but contains invalid YAML.
    """
    relative_path = f"phases/{phase_name}.yaml"
    if config_root is None:
        try:
            return load_yaml_config(relative_path)
        except FileNotFoundError:
            return {}
    path = config_root / "phases" / f"{phase_name}.yaml"
    if not path.exists():
        return {}
    with open(path) as f:
        data = yaml.safe_load(f)
    return data if data else {}


def load_pipeline_config() -> dict[str, Any]:
    """Load pipeline configuration.

    Loads config/pipeline.yaml which lists the pipeline phases (description,
    detectors) and run limits. Per-phase config lives in
    config/phases/<name>.yaml and is loaded via load_phase_config().

    Returns:
        Parsed pipeline config dict.

    Raises:
        FileNotFoundError: If config/pipeline.yaml is missing.
    """
    config_root = _get_config_root()
    path = config_root / "pipeline.yaml"
    if not path.exists():
        raise FileNotFoundError(f"Pipeline config not found: {path} (config root: {config_root})")
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return data
