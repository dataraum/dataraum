"""Entropy detection configuration loader.

Loads thresholds and parameters from config/entropy/thresholds.yaml.
Provides typed access to configuration values with sensible defaults.

Usage:
    from dataraum.entropy.config import get_entropy_config

    config = get_entropy_config()
    threshold = config.detector("null_ratio").multiplier
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dataraum.core.config import get_config_file
from dataraum.core.logging import get_logger

logger = get_logger(__name__)

ENTROPY_THRESHOLDS_CONFIG = "entropy/thresholds.yaml"


@dataclass
class DetectorConfig:
    """Configuration for a single detector."""

    name: str
    values: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a configuration value with optional default."""
        return self.values.get(key, default)

    def __getattr__(self, name: str) -> Any:
        """Allow attribute-style access to config values."""
        if name in ("name", "values"):
            return object.__getattribute__(self, name)
        return self.values.get(name)


@dataclass
class EntropyConfig:
    """Complete entropy configuration."""

    # Detector configurations
    detectors: dict[str, DetectorConfig] = field(default_factory=dict)

    # Human-readable dimension labels
    dimension_labels: dict[str, str] = field(default_factory=dict)

    def detector(self, detector_id: str) -> DetectorConfig:
        """Get configuration for a specific detector.

        Returns empty config if detector not found.
        """
        return self.detectors.get(detector_id, DetectorConfig(name=detector_id))


# Module-level cache for configuration
_config_cache: EntropyConfig | None = None
_config_path_cache: Path | None = None


def load_entropy_config(config_path: Path | None = None) -> EntropyConfig:
    """Load entropy configuration from YAML file.

    Args:
        config_path: Absolute path to thresholds.yaml, for testing.
                     If None, resolves via central config loader.

    Returns:
        EntropyConfig with loaded values.

    Raises:
        FileNotFoundError: If config file doesn't exist (fail-fast behavior).
        RuntimeError: If config file cannot be parsed.
    """
    if config_path is None:
        config_path = get_config_file(ENTROPY_THRESHOLDS_CONFIG)

    if not config_path.exists():
        raise FileNotFoundError(f"Required entropy config not found: {config_path}.")

    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

        return _parse_config(raw)

    except FileNotFoundError:
        raise
    except Exception as e:
        raise RuntimeError(f"Error loading entropy config from {config_path}: {e}") from e


def _parse_config(raw: dict[str, Any]) -> EntropyConfig:
    """Parse raw YAML config into EntropyConfig."""
    config = EntropyConfig()

    # Parse detector configurations
    if "detectors" in raw:
        for detector_id, values in raw["detectors"].items():
            config.detectors[detector_id] = DetectorConfig(
                name=detector_id,
                values=dict(values) if values else {},
            )

    # Parse dimension labels
    if "dimension_labels" in raw:
        config.dimension_labels = dict(raw["dimension_labels"])

    return config


def get_entropy_config(
    config_path: Path | None = None,
    config_dict: dict[str, Any] | None = None,
) -> EntropyConfig:
    """Get entropy configuration, using cache if available.

    Args:
        config_path: Optional absolute path to override default config location.
                    If different from cached path, reloads config.
        config_dict: Optional pre-loaded config dict (from ctx.config in pipeline).
                    If provided with required keys, parsed directly (no caching).

    Returns:
        Cached or newly loaded EntropyConfig.
    """
    global _config_cache, _config_path_cache

    # If a config dict is provided with entropy-specific keys, use it directly
    if config_dict is not None and "detectors" in config_dict:
        return _parse_config(config_dict)

    # Resolve path: explicit arg or central config
    if config_path is not None:
        path = config_path
    elif _config_path_cache is not None:
        path = _config_path_cache
    else:
        path = get_config_file(ENTROPY_THRESHOLDS_CONFIG)

    # Return cached config if path matches
    if _config_cache is not None and _config_path_cache == path:
        return _config_cache

    # Load and cache
    _config_cache = load_entropy_config(path)
    _config_path_cache = path
    return _config_cache


def get_dimension_label(dimension_path: str) -> str:
    """Get business-friendly label for a dimension path.

    Looks up labels from config with exact match first, then prefix match
    (first two segments), falling back to title-casing the last segment.

    Args:
        dimension_path: Dot-separated dimension path, e.g. "semantic.units.unit_declaration"

    Returns:
        Human-readable label string.
    """
    config = get_entropy_config()
    labels = config.dimension_labels

    # Try exact match
    if dimension_path in labels:
        return labels[dimension_path]

    # Try prefix match (first two segments)
    parts = dimension_path.split(".")
    if len(parts) >= 2:
        prefix = f"{parts[0]}.{parts[1]}"
        if prefix in labels:
            return labels[prefix]

    # Fallback: title-case the last segment
    return parts[-1].replace("_", " ").title()
