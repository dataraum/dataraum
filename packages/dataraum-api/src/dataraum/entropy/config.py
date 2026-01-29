"""Entropy detection configuration loader.

Loads thresholds and parameters from config/entropy/thresholds.yaml.
Provides typed access to configuration values with sensible defaults.

Usage:
    from dataraum.entropy.config import get_entropy_config

    config = get_entropy_config()
    weights = config.composite_weights
    threshold = config.detector("null_ratio").multiplier
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from dataraum.core.logging import get_logger

logger = get_logger(__name__)

# Default config path relative to project root
DEFAULT_CONFIG_PATH = (
    Path(__file__).parent.parent.parent.parent / "config" / "entropy" / "thresholds.yaml"
)


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
class CompoundRiskConfig:
    """Configuration for a compound risk pattern."""

    risk_type: str
    dimensions: list[str]
    threshold: float
    multiplier: float
    risk_level: str
    impact_template: str


@dataclass
class EntropyConfig:
    """Complete entropy configuration."""

    # Composite scoring weights
    composite_weights: dict[str, float] = field(
        default_factory=lambda: {
            "structural": 0.25,
            "semantic": 0.30,
            "value": 0.30,
            "computational": 0.15,
        }
    )

    # Readiness thresholds
    ready_threshold: float = 0.3
    blocked_threshold: float = 0.6

    # Entropy level thresholds
    high_entropy_threshold: float = 0.5
    critical_entropy_threshold: float = 0.8

    # Detector configurations
    detectors: dict[str, DetectorConfig] = field(default_factory=dict)

    # Compound risk definitions
    compound_risks: dict[str, CompoundRiskConfig] = field(default_factory=dict)

    # Effort factors for priority calculation
    effort_factors: dict[str, float] = field(
        default_factory=lambda: {
            "low": 1.0,
            "medium": 2.0,
            "high": 4.0,
        }
    )

    def detector(self, detector_id: str) -> DetectorConfig:
        """Get configuration for a specific detector.

        Returns empty config if detector not found.
        """
        return self.detectors.get(detector_id, DetectorConfig(name=detector_id))

    def get_readiness(self, score: float) -> str:
        """Classify readiness based on composite score."""
        if score < self.ready_threshold:
            return "ready"
        elif score < self.blocked_threshold:
            return "investigate"
        return "blocked"

    def is_high_entropy(self, score: float) -> bool:
        """Check if score exceeds high entropy threshold."""
        return score >= self.high_entropy_threshold

    def is_critical_entropy(self, score: float) -> bool:
        """Check if score exceeds critical entropy threshold."""
        return score >= self.critical_entropy_threshold

    def effort_factor(self, effort: str) -> float:
        """Get effort factor for priority calculation.

        Raises:
            KeyError: If effort level is not recognized (fail-fast behavior).
        """
        if effort not in self.effort_factors:
            raise KeyError(
                f"Unknown effort level '{effort}'. Valid levels: {list(self.effort_factors.keys())}"
            )
        return self.effort_factors[effort]


# Module-level cache for configuration
_config_cache: EntropyConfig | None = None
_config_path_cache: Path | None = None


def load_entropy_config(config_path: Path | None = None) -> EntropyConfig:
    """Load entropy configuration from YAML file.

    Args:
        config_path: Path to thresholds.yaml. Defaults to config/entropy/thresholds.yaml.

    Returns:
        EntropyConfig with loaded values.

    Raises:
        FileNotFoundError: If config file doesn't exist (fail-fast behavior).
        RuntimeError: If config file cannot be parsed.
    """
    config_path = config_path or DEFAULT_CONFIG_PATH

    if not config_path.exists():
        raise FileNotFoundError(
            f"Required entropy config not found: {config_path}. "
            "Create config/entropy/thresholds.yaml with entropy configuration."
        )

    try:
        with open(config_path) as f:
            raw = yaml.safe_load(f) or {}

        return _parse_config(raw)

    except FileNotFoundError:
        raise  # Re-raise FileNotFoundError
    except Exception as e:
        raise RuntimeError(f"Error loading entropy config from {config_path}: {e}") from e


def _parse_config(raw: dict[str, Any]) -> EntropyConfig:
    """Parse raw YAML config into EntropyConfig."""
    config = EntropyConfig()

    # Parse composite weights
    if "composite_weights" in raw:
        config.composite_weights = dict(raw["composite_weights"])

    # Parse readiness thresholds
    if "readiness" in raw:
        config.ready_threshold = raw["readiness"].get("ready_threshold", 0.3)
        config.blocked_threshold = raw["readiness"].get("blocked_threshold", 0.6)

    # Parse entropy level thresholds
    if "entropy_levels" in raw:
        config.high_entropy_threshold = raw["entropy_levels"].get("high_entropy", 0.5)
        config.critical_entropy_threshold = raw["entropy_levels"].get("critical_entropy", 0.8)

    # Parse detector configurations
    if "detectors" in raw:
        for detector_id, values in raw["detectors"].items():
            config.detectors[detector_id] = DetectorConfig(
                name=detector_id,
                values=dict(values) if values else {},
            )

    # Parse compound risk definitions
    if "compound_risks" in raw:
        for risk_type, definition in raw["compound_risks"].items():
            config.compound_risks[risk_type] = CompoundRiskConfig(
                risk_type=risk_type,
                dimensions=definition.get("dimensions", []),
                threshold=definition.get("threshold", 0.5),
                multiplier=definition.get("multiplier", 1.5),
                risk_level=definition.get("risk_level", "high"),
                impact_template=definition.get("impact_template", ""),
            )

    # Parse effort factors
    if "effort_factors" in raw:
        config.effort_factors = dict(raw["effort_factors"])

    return config


def get_entropy_config(config_path: Path | None = None) -> EntropyConfig:
    """Get entropy configuration, using cache if available.

    Args:
        config_path: Optional path to override default config location.
                    If different from cached path, reloads config.

    Returns:
        Cached or newly loaded EntropyConfig.
    """
    global _config_cache, _config_path_cache

    path = config_path or DEFAULT_CONFIG_PATH

    # Return cached config if path matches
    if _config_cache is not None and _config_path_cache == path:
        return _config_cache

    # Load and cache
    _config_cache = load_entropy_config(path)
    _config_path_cache = path
    return _config_cache


def clear_config_cache() -> None:
    """Clear the configuration cache.

    Useful for testing or when config file changes.
    """
    global _config_cache, _config_path_cache
    _config_cache = None
    _config_path_cache = None
