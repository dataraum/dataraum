"""Financial domain configuration loader.

Single source of truth for loading config/domains/financial.yaml.
"""

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

# Module-level cache
_FINANCIAL_CONFIG_CACHE: dict[str, Any] | None = None


def load_financial_config() -> dict[str, Any]:
    """Load financial domain configuration from YAML.

    Searches for config/domains/financial.yaml in:
    1. Current working directory
    2. Project root (relative to this file)

    Returns:
        Dictionary with cycle_patterns, quality_thresholds, expected_cycles, etc.
        Returns empty dict if config not found.
    """
    global _FINANCIAL_CONFIG_CACHE
    if _FINANCIAL_CONFIG_CACHE is not None:
        return _FINANCIAL_CONFIG_CACHE

    config_paths = [
        Path("config/domains/financial.yaml"),
        Path(__file__).parent.parent.parent.parent.parent / "config/domains/financial.yaml",
        Path.cwd() / "config/domains/financial.yaml",
    ]

    for path in config_paths:
        if path.exists():
            with open(path) as f:
                _FINANCIAL_CONFIG_CACHE = yaml.safe_load(f)
                logger.debug(f"Loaded financial config from {path}")
                return _FINANCIAL_CONFIG_CACHE

    logger.warning("Financial config not found, using empty config")
    _FINANCIAL_CONFIG_CACHE = {}
    return _FINANCIAL_CONFIG_CACHE


def clear_config_cache() -> None:
    """Clear the config cache.

    Useful for testing or when config files are updated.
    """
    global _FINANCIAL_CONFIG_CACHE
    _FINANCIAL_CONFIG_CACHE = None
