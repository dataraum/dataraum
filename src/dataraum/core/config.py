"""Configuration management.

Central config resolution for the entire application.
All modules load config through this module — never via Path(__file__) navigation.

Usage:
    from dataraum.core.config import get_config_file, load_yaml_config

    # Get a resolved path to a config file
    path = get_config_file("system/llm.yaml")

    # Load and parse a YAML config file
    data = load_yaml_config("system/entropy/thresholds.yaml")

    # Application settings (env vars, .env)
    settings = get_settings()
"""

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _find_config_dir() -> Path:
    """Find the config directory by walking up from the package location.

    This is the ONE place that does path-relative-to-file resolution.
    Everything else goes through get_config_file() or get_settings().config_path.
    """
    # src/dataraum/core/config.py -> 4 levels up -> project root
    package_dir = Path(__file__).resolve().parent.parent.parent.parent
    candidate = package_dir / "config"
    if candidate.is_dir():
        return candidate

    # Fallback: relative path (works when CWD is project root)
    return Path("config")


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
    config_root = get_settings().config_path
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
    config_root = get_settings().config_path
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


class Settings(BaseSettings):
    """Application settings.

    All settings can be overridden via environment variables.
    Prefix: DATARAUM_
    """

    model_config = SettingsConfigDict(
        env_prefix="DATARAUM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # Ignore extra env vars (like ANTHROPIC_API_KEY)
    )

    # Database (SQLAlchemy)
    # SQLite for local dev, PostgreSQL for production
    database_url: str = Field(
        default="sqlite+aiosqlite:///./dataraum.db",
        description="SQLAlchemy database URL. Use postgresql+asyncpg://... for production",
    )

    # DuckDB
    duckdb_path: str = Field(
        default=":memory:",
        description="Path to DuckDB database file, or :memory: for in-memory",
    )
    duckdb_memory_limit: str = Field(
        default="4GB",
        description="Memory limit for DuckDB",
    )
    duckdb_threads: int = Field(
        default=4,
        description="Number of threads for DuckDB",
    )

    # Configuration paths
    config_path: Path = Field(
        default_factory=_find_config_dir,
        description="Path to configuration files (ontologies, patterns, rules)",
    )

    # Profiling
    profile_sample_size: int = Field(
        default=100_000,
        description="Number of rows to sample for profiling (0 = all)",
    )
    profile_histogram_buckets: int = Field(
        default=20,
        description="Number of histogram buckets",
    )
    profile_top_k_values: int = Field(
        default=10,
        description="Number of top values to track",
    )

    # Type inference
    type_inference_min_confidence: float = Field(
        default=0.95,
        description="Minimum confidence for automatic type decisions",
    )

    # Quality
    quality_anomaly_threshold: float = Field(
        default=3.0,
        description="Standard deviations for anomaly detection",
    )

    # Default ontology
    default_ontology: str = Field(
        default="general",
        description="Default ontology to use",
    )

    # API
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_workers: int = Field(default=1)

    # Logging
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json")  # 'json' or 'console'


@lru_cache
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
